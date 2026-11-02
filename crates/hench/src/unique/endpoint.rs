//! Unique jobs while they run: the release of a `start` lock just before `perform`
//! (`UNIQUE_RELEASE`, from the perform endpoint, after the batch cancellation check;
//! `docs/design/unique.md` §2.3, §2.9), and the methods a job uses to see and release its own
//! lock ([`JobContext::unique`], [`JobContext::release_unique_lock`]).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use chrono::Utc;

use crate::client::ClientMode;
use crate::error::Result;
use crate::job::JobContext;
use crate::keys;
use crate::redis::RedisPool;
use crate::unique::lua::UNIQUE_RELEASE;
use crate::unique::{LockRef, Unique, UniqueUntil};

/// Releases the lock of a job whose policy is `start`, read from `ctx.payload()` (the job as
/// fetched: server middleware passes on the context it received). A failure is logged and the
/// job runs anyway: its outcome releases the lock (the outcome scripts compare the holder).
/// A job that no longer holds its lock here counts a lapse.
pub(crate) async fn release_start(ctx: &JobContext) {
    let Some(lock) = LockRef::from_payload_quietly(ctx.payload()) else {
        return;
    };
    if lock.until != UniqueUntil::Start {
        return;
    }
    match release(ctx, &lock, true).await {
        Ok(true) => tracing::debug!(digest = %lock.digest, "released the lock of a start job"),
        Ok(false) => {}
        Err(error) => tracing::warn!(
            %error,
            jid = %lock.jid,
            digest = %lock.digest,
            "could not release the unique lock of a job when it started; it runs anyway (its \
             outcome releases the lock)"
        ),
    }
}

/// Releases `lock` on behalf of the job running with `ctx`: in Redis (`UNIQUE_RELEASE`,
/// counting a lapse when `count_lapse` and the job did not hold it), or in the in-memory locks
/// of a testing client. `true` when the job held the lock.
async fn release(ctx: &JobContext, lock: &LockRef, count_lapse: bool) -> Result<bool> {
    let client = ctx.client();
    match client.mode() {
        ClientMode::Fake(_) | ClientMode::Inline(_) => Ok(client.tables().unique.release(lock)),
        ClientMode::Redis | ClientMode::Collect(_) => {
            release_in_redis(client.redis(), lock, count_lapse).await
        }
    }
}

/// `UNIQUE_RELEASE` of `lock` by its job: `true` when it held the lock.
pub(crate) async fn release_in_redis(
    redis: &RedisPool,
    lock: &LockRef,
    count_lapse: bool,
) -> Result<bool> {
    let events = keys::unique_minute(Utc::now());
    let status: i64 = redis
        .with_retry(|mut conn| {
            let events = &events;
            async move {
                UNIQUE_RELEASE
                    .script()
                    .key(&lock.key)
                    .key(keys::UNIQUE_LOCKS)
                    .key(events)
                    .arg(&lock.jid)
                    .arg(&lock.digest)
                    .arg(&lock.class)
                    .arg(if count_lapse { "1" } else { "0" })
                    .invoke_async(&mut conn)
                    .await
            }
        })
        .await?;
    Ok(status == 1)
}

/// Whether a running job gave its lock up ([`JobContext::release_unique_lock`]): one per
/// execution, shared by the job's context (which sets it) and the processor's state of the
/// execution (`Fetched`), which the outcome's lock work, the heartbeat's renewal and the copies
/// continuing the execution read. It lives and dies with the execution: nothing to clean up,
/// nothing a later job reusing the jid could see.
#[derive(Debug, Clone, Default)]
pub(crate) struct EarlyRelease(Arc<AtomicBool>);

impl EarlyRelease {
    /// Notes that the job released its lock.
    pub(crate) fn note(&self) {
        self.0.store(true, Ordering::Release);
    }

    /// Whether the job released its lock.
    pub(crate) fn released(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

impl PartialEq for EarlyRelease {
    /// Two states are equal when they say the same thing (whichever execution they belong to).
    fn eq(&self, other: &Self) -> bool {
        self.released() == other.released()
    }
}

impl JobContext {
    /// The uniqueness of this job (see [`crate::unique`]): its settings as its payload carries
    /// them (`unique_for`, the policy and the lock's digest), or `None` when the job is not
    /// unique.
    pub fn unique(&self) -> Option<&Unique> {
        self.payload().unique.as_ref()?.settings()
    }

    /// Releases this job's unique lock now, whatever its policy (the holder is compared: a
    /// lock another job holds is never released), so that a duplicate can be pushed while the
    /// job still runs — for example once it has read the state it works on. `Ok(true)` when the
    /// job held its lock, `Ok(false)` when it held none (or the job is not unique). On a fake
    /// or inline testing client the in-memory lock is released.
    ///
    /// The job gives its lock up for the rest of this execution: its outcome does no lock work
    /// and counts no lapse, the heartbeat stops renewing the lock, and the copy that continues
    /// the execution (a retry, a job pushed back or rescheduled, the morgue's copy) is stored
    /// without uniqueness (`"unique": false`): it waits and runs without a lock.
    pub async fn release_unique_lock(&self) -> Result<bool> {
        let Some(lock) = LockRef::from_payload_quietly(self.payload()) else {
            return Ok(false);
        };
        let released = release(self, &lock, false).await?;
        if released && let Some(early) = self.early_release() {
            early.note();
        }
        Ok(released)
    }
}
