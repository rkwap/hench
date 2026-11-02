//! The unique part of the API's removals (`docs/design/unique.md` §2.11): a deleted or killed
//! unique job releases its lock in the same script that removes it (`REMOVE_UNIQUE`,
//! `KILL_UNIQUE`), and `kill_all` releases the locks of each moved chunk
//! (`UNIQUE_RELEASE_MANY`). Releases compare the holder: the lock of another job (a duplicate
//! pushed after this job's lock lapsed) is never released.
//!
//! Each function returns `None` when it has nothing to do (the job holds no lock that follows
//! it: not unique, or `expired`); the caller then removes the job as it always did. Like the
//! API's other removals, the scripts are sent once (a repeat after a dropped connection would
//! misreport the job), and a failed delete checks the job's presence.

use crate::error::Result;
use crate::keys;
use crate::payload::JobPayload;
use crate::redis::RedisPool;
use crate::unique::LockRef;
use crate::unique::lua::{KILL_UNIQUE, REMOVE_UNIQUE, UNIQUE_RELEASE_MANY, UNIQUE_TIDY};

/// Locks released per `UNIQUE_RELEASE_MANY` run.
pub(crate) const RELEASE_CHUNK: usize = 100;

/// The lock a removal of `job` releases, if any.
fn releasable(job: &JobPayload) -> Option<LockRef> {
    LockRef::from_payload_quietly(job).filter(LockRef::follows_the_job)
}

/// Removes `value` (the stored form of `job`) from the sorted set `key` and releases its lock
/// in one script. `Some(Ok(true))`: removed (or gone after an error); `Some(Ok(false))`: it was
/// not there.
pub(crate) async fn delete_from_set(
    redis: &RedisPool,
    key: &str,
    value: &str,
    job: &JobPayload,
) -> Option<Result<bool>> {
    let lock = releasable(job)?;
    Some(remove(redis, key, "zset", value, &lock).await)
}

/// Removes `value` (the stored form of `job`) from the queue list `key` and releases its lock
/// in one script.
pub(crate) async fn delete_from_queue(
    redis: &RedisPool,
    key: &str,
    value: &str,
    job: &JobPayload,
) -> Option<Result<bool>> {
    let lock = releasable(job)?;
    Some(remove(redis, key, "list", value, &lock).await)
}

/// `REMOVE_UNIQUE`, sent once; on an error, the job's presence decides (gone: it was removed,
/// or moved by someone else; still there, or unknown: the error).
async fn remove(
    redis: &RedisPool,
    key: &str,
    kind: &str,
    value: &str,
    lock: &LockRef,
) -> Result<bool> {
    let attempt: Result<(i64, i64)> = async {
        let mut conn = redis.conn().await?;
        Ok(REMOVE_UNIQUE
            .script()
            .key(key)
            .key(&lock.key)
            .key(keys::UNIQUE_LOCKS)
            .arg(kind)
            .arg(value)
            .arg(&lock.jid)
            .arg(&lock.digest)
            .invoke_async(&mut conn)
            .await?)
    }
    .await;
    let error = match attempt {
        Ok((removed, status)) => {
            if removed == 1 {
                tracing::debug!(
                    jid = %lock.jid,
                    digest = %lock.digest,
                    status,
                    "deleted a unique job and released its lock"
                );
            }
            return Ok(removed == 1);
        }
        Err(error) => error,
    };
    let present: Result<bool> = redis
        .with_retry(|mut conn| async move {
            if kind == "list" {
                ::redis::cmd("LPOS")
                    .arg(key)
                    .arg(value)
                    .query_async::<Option<i64>>(&mut conn)
                    .await
                    .map(|position| position.is_some())
            } else {
                ::redis::cmd("ZSCORE")
                    .arg(key)
                    .arg(value)
                    .query_async::<Option<f64>>(&mut conn)
                    .await
                    .map(|score| score.is_some())
            }
        })
        .await;
    match present {
        Ok(false) => Ok(true),
        Ok(true) | Err(_) => Err(error),
    }
}

/// The morgue's bounds for a kill: its key, the score of the killed job (now), the oldest score
/// kept and the rank bound of the size trim.
#[derive(Debug, Clone, Copy)]
pub(crate) struct MorgueBounds<'a> {
    /// The morgue's key.
    pub(crate) key: &'a str,
    /// The killed job's score.
    pub(crate) now: f64,
    /// Entries scored below this are removed.
    pub(crate) cutoff: f64,
    /// Upper bound of the rank trim.
    pub(crate) rank: i64,
}

/// Moves `value` (the stored form of `job`) from the sorted set `source` to the morgue and
/// releases its lock in one script; the answer is `MOVE_TO_MORGUE`'s (1 moved, 2 refreshed
/// in place, 0 not in `source`). Sent once: a repeat after a dropped connection would report a
/// moved job as missing.
pub(crate) async fn kill_from_set(
    redis: &RedisPool,
    source: &str,
    morgue: MorgueBounds<'_>,
    value: &str,
    job: &JobPayload,
) -> Option<Result<i64>> {
    let lock = releasable(job)?;
    let attempt: Result<(i64, i64)> = async {
        let mut conn = redis.conn().await?;
        Ok(KILL_UNIQUE
            .script()
            .key(source)
            .key(morgue.key)
            .key(&lock.key)
            .key(keys::UNIQUE_LOCKS)
            .arg(value)
            .arg(morgue.now)
            .arg(morgue.cutoff)
            .arg(morgue.rank)
            .arg(&lock.jid)
            .arg(&lock.digest)
            .invoke_async(&mut conn)
            .await?)
    }
    .await;
    Some(attempt.map(|(moved, status)| {
        if moved > 0 {
            tracing::debug!(
                jid = %lock.jid,
                digest = %lock.digest,
                status,
                "killed a unique job and released its lock"
            );
        }
        moved
    }))
}

/// Releases the locks of `jobs`, which `kill_all` just moved to the morgue (`UNIQUE_RELEASE_MANY`,
/// the holder compared). Errors are logged: the locks then expire with their time to live.
pub(crate) async fn release_many(redis: &RedisPool, jobs: &[JobPayload]) {
    let locks: Vec<LockRef> = jobs.iter().filter_map(releasable).collect();
    for chunk in locks.chunks(RELEASE_CHUNK) {
        let entries: Vec<(&str, &str, &str)> = chunk
            .iter()
            .map(|lock| (lock.jid.as_str(), lock.digest.as_str(), ""))
            .collect();
        if let Err(error) = release_entries(redis, &entries).await {
            tracing::warn!(
                %error,
                locks = chunk.len(),
                "could not release the unique locks of killed jobs; they expire with their time \
                 to live"
            );
        }
    }
}

/// `UNIQUE_RELEASE_MANY` for `entries` (jid, `''` for any holder; digest; class filter, `''` for
/// any): how many locks were released. Idempotent (a repeat releases nothing more).
pub(crate) async fn release_entries(
    redis: &RedisPool,
    entries: &[(&str, &str, &str)],
) -> Result<u64> {
    if entries.is_empty() {
        return Ok(0);
    }
    let script = UNIQUE_RELEASE_MANY.script();
    let mut invocation = script.prepare_invoke();
    invocation.key(keys::UNIQUE_LOCKS);
    for (jid, digest, class) in entries {
        invocation
            .key(keys::unique_lock(digest))
            .arg(*jid)
            .arg(*digest)
            .arg(*class);
    }
    let invocation = &invocation;
    redis
        .with_retry(|mut conn| async move { invocation.invoke_async(&mut conn).await })
        .await
}

/// `UNIQUE_TIDY` for `digests`: removes the index entries whose lock is gone (checked in the
/// script, so a lock taken again meanwhile keeps its entry). Returns how many were removed.
pub(crate) async fn tidy(redis: &RedisPool, digests: &[&str]) -> Result<u64> {
    if digests.is_empty() {
        return Ok(0);
    }
    let script = UNIQUE_TIDY.script();
    let mut invocation = script.prepare_invoke();
    invocation.key(keys::UNIQUE_LOCKS);
    for digest in digests {
        invocation.key(keys::unique_lock(digest)).arg(*digest);
    }
    let invocation = &invocation;
    redis
        .with_retry(|mut conn| async move { invocation.invoke_async(&mut conn).await })
        .await
}
