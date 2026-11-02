//! Delivery of jobs that carry a digest: `UNIQUE_PUSH` in chunks of at most 100 jobs, each
//! writing a job and its lock together (`docs/design/unique.md` §2.8.3).
//!
//! A chunk is one script run (a few milliseconds at most), sent through
//! [`RedisPool::with_retry`] like every push: a resend after a dropped connection finds the locks
//! of the jobs it already wrote held by their own jids and reports them as already pushed, so no
//! unique job is written twice. (A job without a lock in the same chunk can be, as with the
//! `MULTI` of other pushes.)

use chrono::Utc;

use crate::client::{PushResult, Stored};
use crate::error::{Error, Result};
use crate::keys;
use crate::payload::{JobPayload, now_millis};
use crate::redis::RedisPool;
use crate::unique::lua::UNIQUE_PUSH;
use crate::unique::types::{MAX_UNIQUE_FOR_SECS, valid_digest};
use crate::unique::{OnConflict, UniqueOption, UniqueUntil};

/// Most jobs one `UNIQUE_PUSH` run writes.
pub(crate) const CHUNK: usize = 100;

/// One job ready for the script: where it goes and the lock it takes.
struct Prepared {
    jid: String,
    class: String,
    /// The JSON as stored.
    json: String,
    /// `queue` or `schedule`.
    mode: &'static str,
    /// The queue's name, or the score.
    target: String,
    queue: String,
    destination: String,
    lock: Option<PushLock>,
}

/// The lock a job carrying a digest takes when it is pushed (by `UNIQUE_PUSH`, or by the
/// periodic scripts, which embed the same lock step).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PushLock {
    /// The lock's key (`unique:<digest>`).
    pub(crate) key: String,
    /// The digest.
    pub(crate) digest: String,
    /// When the lock is released.
    pub(crate) until: UniqueUntil,
    /// `unique_for`, in milliseconds (whole seconds).
    pub(crate) for_ms: u64,
    /// What a push does when another job holds the lock.
    pub(crate) on_conflict: OnConflict,
}

/// The lock of `job` as it is pushed, or `None` when it takes none. Leaves `job` as it is
/// stored: without the push-time fields of its `unique` option (`on_conflict`, `key`,
/// `across_queues`) and with its policy set; a job whose option is disabled, or has lost its
/// digest (client middleware may remove it), is stored without the option.
pub(crate) fn take_lock(job: &mut JobPayload) -> Result<Option<PushLock>> {
    let Some(UniqueOption::Enabled(mut unique)) = job.unique.take() else {
        return Ok(None);
    };
    let Some(digest) = unique.digest.clone() else {
        return Ok(None);
    };
    if !valid_digest(&digest) {
        return Err(Error::InvalidJob(format!(
            "the digest of a unique job must be 1 to 128 characters of A-Z, a-z, 0-9, `_` and \
             `-`, not {digest:?}"
        )));
    }
    let secs = unique.ttl.map_or(0, |ttl| ttl.as_secs());
    if secs == 0 || secs >= MAX_UNIQUE_FOR_SECS {
        return Err(Error::InvalidJob(
            "a unique job needs a `unique_for` of at least one second and below 1000000000 \
             seconds"
                .to_owned(),
        ));
    }
    let until = unique.until.unwrap_or_default();
    let on_conflict = unique.on_conflict.take().unwrap_or_default();
    unique.until = Some(until);
    unique.key = None;
    unique.across_queues = None;
    job.unique = Some(UniqueOption::Enabled(unique));
    Ok(Some(PushLock {
        key: keys::unique_lock(&digest),
        digest,
        until,
        for_ms: secs * 1000,
        on_conflict,
    }))
}

impl Prepared {
    /// Encodes `job` as `raw_push` stores it (`at` checked against `now`), without the
    /// push-time fields of its `unique` option. A job whose `unique` option is disabled, or has
    /// lost its digest (client middleware may remove it), is stored without the option and
    /// pushed without a lock.
    fn new(mut job: JobPayload, now: f64, enqueued_at: i64) -> Result<Self> {
        let lock = take_lock(&mut job)?;
        let jid = job.jid.clone();
        let class = job.class.clone();
        let queue = job.queue.clone();
        Ok(match Stored::encode(job, now, enqueued_at)? {
            Stored::Scheduled { at, json } => Prepared {
                jid,
                class,
                json,
                mode: "schedule",
                target: at.to_string(),
                queue,
                destination: keys::SCHEDULE.to_owned(),
                lock,
            },
            Stored::Queued { queue, json } => Prepared {
                jid,
                class,
                json,
                mode: "queue",
                target: queue.clone(),
                destination: keys::queue(&queue),
                queue,
                lock,
            },
        })
    }
}

/// Writes `jobs` (normalized, through client middleware, their `at` checked against `now`)
/// with their locks; one result per job, in order.
///
/// A job the script could not write (a key of the wrong type, Redis out of memory) fails the
/// call with the first such error, like the `MULTI` of other pushes; the jobs before it, and the
/// other jobs of its chunk, may have been written. Later chunks are not sent.
pub(crate) async fn deliver(
    redis: &RedisPool,
    jobs: Vec<JobPayload>,
    now: f64,
) -> Result<Vec<PushResult>> {
    let enqueued_at = now_millis();
    let prepared = jobs
        .into_iter()
        .map(|job| Prepared::new(job, now, enqueued_at))
        .collect::<Result<Vec<_>>>()?;
    let events = keys::unique_minute(Utc::now());
    let mut results = Vec::with_capacity(prepared.len());
    for chunk in prepared.chunks(CHUNK) {
        let replies = push_chunk(redis, &events, chunk).await?;
        if replies.len() != chunk.len() {
            return Err(Error::InvalidJob(format!(
                "UNIQUE_PUSH answered for {} jobs instead of {}",
                replies.len(),
                chunk.len()
            )));
        }
        for (job, (code, detail)) in chunk.iter().zip(replies) {
            results.push(result_of(job, code, detail)?);
        }
    }
    Ok(results)
}

/// One run of `UNIQUE_PUSH`: a `(code, detail)` pair per job.
async fn push_chunk(
    redis: &RedisPool,
    events: &str,
    chunk: &[Prepared],
) -> Result<Vec<(i64, String)>> {
    let script = UNIQUE_PUSH.script();
    let mut invocation = script.prepare_invoke();
    invocation
        .key(keys::QUEUES)
        .key(keys::SCHEDULE)
        .key(keys::UNIQUE_LOCKS)
        .key(events)
        .arg(chunk.len());
    for job in chunk {
        match &job.lock {
            Some(lock) => invocation.key(&lock.key),
            None => invocation.key(keys::UNIQUE_LOCKS),
        };
        invocation.key(&job.destination);
        let (until, for_ms, at, on_conflict, digest) = match &job.lock {
            Some(lock) => (
                lock.until.as_str(),
                lock.for_ms,
                if job.mode == "schedule" {
                    job.target.as_str()
                } else {
                    ""
                },
                lock.on_conflict.as_str(),
                lock.digest.as_str(),
            ),
            None => ("", 0, "", OnConflict::Drop.as_str(), ""),
        };
        invocation
            .arg(&job.jid)
            .arg(&job.json)
            .arg(job.mode)
            .arg(&job.target)
            .arg(&job.queue)
            .arg(until)
            .arg(for_ms)
            .arg(at)
            .arg(on_conflict)
            .arg(&job.class)
            .arg(digest);
    }
    let invocation = &invocation;
    redis
        .with_retry(|mut conn| async move { invocation.invoke_async(&mut conn).await })
        .await
}

/// What the script did with `job`.
fn result_of(job: &Prepared, code: i64, detail: String) -> Result<PushResult> {
    let digest = job.lock.as_ref().map_or("", |lock| lock.digest.as_str());
    Ok(match code {
        1 => PushResult::Pushed,
        2 => {
            tracing::debug!(jid = %job.jid, class = %job.class, digest, "unique job already pushed");
            PushResult::AlreadyPushed
        }
        0 => {
            tracing::debug!(
                class = %job.class,
                jid = %job.jid,
                holder = %detail,
                digest,
                "duplicate job not pushed"
            );
            PushResult::Duplicate { holder: detail }
        }
        3 => {
            tracing::debug!(
                class = %job.class,
                jid = %job.jid,
                replaced = %detail,
                digest,
                "unique job replaced the scheduled holder of its lock"
            );
            PushResult::Replaced { replaced: detail }
        }
        4 => PushResult::PushedWithoutLock { error: detail },
        _ => return Err(script_error(&detail)),
    })
}

/// The Redis error a script reported for one job (`WRONGTYPE ...`, `OOM ...`), with its code,
/// so that callers recognize Redis being unavailable as for any other command.
pub(crate) fn script_error(text: &str) -> Error {
    let text = text.trim();
    let (code, detail) = text.split_once(' ').unwrap_or((text, ""));
    if !code.is_empty()
        && code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte == b'_')
    {
        Error::Redis(::redis::make_extension_error(
            code.to_owned(),
            Some(detail.to_owned()),
        ))
    } else {
        Error::Redis(::redis::make_extension_error(
            "ERR".to_owned(),
            Some(text.to_owned()),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::unique::Unique;
    use serde_json::json;
    use std::time::Duration;

    fn unique_job() -> JobPayload {
        let mut job = JobPayload::new("Sync", vec![json!(1)]);
        job.jid = "0123456789abcdef01234567".to_owned();
        job.queue = "default".to_owned();
        let mut unique = Unique::new(Duration::from_secs(60)).on_conflict(OnConflict::Replace);
        unique.digest = Some("d1".to_owned());
        job.unique = Some(unique.into());
        job
    }

    #[test]
    fn jobs_are_stored_without_their_push_time_fields() {
        let prepared = Prepared::new(unique_job(), 100.0, 5).unwrap();
        let stored: serde_json::Value = serde_json::from_str(&prepared.json).unwrap();
        assert_eq!(
            stored["unique"],
            json!({"for": 60, "until": "success", "digest": "d1"})
        );
        assert_eq!(stored["enqueued_at"], json!(5));
        let lock = prepared.lock.unwrap();
        assert_eq!(lock.on_conflict, OnConflict::Replace);
        assert_eq!(lock.for_ms, 60_000);
        assert_eq!(lock.key, "unique:d1");
        assert_eq!(
            (prepared.mode, prepared.target.as_str()),
            ("queue", "default")
        );

        let mut scheduled = unique_job();
        scheduled.at = Some(1_800_000_000.25);
        let prepared = Prepared::new(scheduled, 100.0, 5).unwrap();
        assert_eq!(
            (prepared.mode, prepared.target.as_str()),
            ("schedule", "1800000000.25")
        );
        assert_eq!(prepared.destination, "schedule");
    }

    #[test]
    fn jobs_without_a_usable_digest_go_without_a_lock() {
        let mut job = unique_job();
        job.unique = Some(UniqueOption::Disabled);
        let prepared = Prepared::new(job, 100.0, 5).unwrap();
        assert!(prepared.lock.is_none());
        assert!(!prepared.json.contains("unique"));
        let mut job = unique_job();
        job.unique = Some(Unique::new(Duration::from_secs(60)).into());
        assert!(Prepared::new(job, 100.0, 5).unwrap().lock.is_none());
        let mut job = unique_job();
        let mut bad = Unique::new(Duration::from_secs(60));
        bad.digest = Some("no spaces".to_owned());
        job.unique = Some(bad.into());
        assert!(matches!(
            Prepared::new(job, 100.0, 5),
            Err(Error::InvalidJob(_))
        ));
        let mut job = unique_job();
        let mut short = Unique::new(Duration::from_millis(900));
        short.digest = Some("d".to_owned());
        job.unique = Some(short.into());
        assert!(matches!(
            Prepared::new(job, 100.0, 5),
            Err(Error::InvalidJob(_))
        ));
    }

    #[test]
    fn script_errors_keep_their_codes() {
        let Error::Redis(error) =
            script_error("OOM command not allowed when used memory > 'maxmemory'.")
        else {
            panic!("a Redis error");
        };
        assert_eq!(error.code(), Some("OOM"));
        let Error::Redis(error) = script_error("WRONGTYPE Operation against a key") else {
            panic!("a Redis error");
        };
        assert_eq!(error.code(), Some("WRONGTYPE"));
        let Error::Redis(error) = script_error("something odd") else {
            panic!("a Redis error");
        };
        assert_eq!(error.code(), Some("ERR"));
        assert!(error.to_string().contains("something odd"), "{error}");
    }
}
