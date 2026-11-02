//! The engine side of unique jobs: the lock work of the outcome scripts (`ACK_UNIQUE`,
//! `SETTLE_UNIQUE`, `PUSH_BACK_UNIQUE`; `UNIQUE_LOCK_OP` after the write with basic fetch) and
//! the heartbeat's renewal of running jobs' locks (`UNIQUE_RENEW`; `docs/design/unique.md`
//! §2.8.4, §2.9).
//!
//! With reliable fetch the lock work happens in the script that writes the outcome, guarded by
//! the job's presence in the in-progress list: it happens once, with the outcome, and never for
//! a job no longer in progress (acknowledged by a reply that was lost, or recovered by another
//! process). Basic fetch has no such list: the lock work follows the write as a separate script,
//! idempotent (holds compare the jid and only extend, releases compare the jid).

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use chrono::Utc;

use crate::client::Stored;
use crate::config::FetchStrategy;
use crate::error::Result;
use crate::fetch::{Fetcher, Settlement, UnitOfWork};
use crate::keys;
use crate::launcher::Engine;
use crate::lua::ScriptCall;
use crate::payload::JobPayload;
use crate::pro::{Classified, Fetched};
use crate::redis::RedisPool;
use crate::retry::Verdict;
use crate::unique::lua::{
    ACK_UNIQUE, PUSH_BACK_UNIQUE, SETTLE_UNIQUE, UNIQUE_LOCK_OP, UNIQUE_RENEW,
};
use crate::unique::{LockAction, LockOp, LockRef, UniqueOption, UniqueUntil};

/// Locks renewed per `UNIQUE_RENEW` run.
const RENEW_CHUNK: usize = 200;

/// How often a process warns about the lapses of one class.
const LAPSE_NOTICE_INTERVAL: Duration = Duration::from_secs(60);

/// What the outcome write of a job does with its lock (`docs/design/pro-features.md` §3.2 step
/// 6): `None` when the job holds no lock, its lock follows no outcome (`expired`), the job gave
/// its lock up while it ran ([`JobContext::release_unique_lock`]), or the outcome leaves it
/// alone (a requeue at shutdown). `settlement` is the failure's write.
///
/// [`JobContext::release_unique_lock`]: crate::JobContext::release_unique_lock
///
/// | Outcome | Lock work |
/// |---|---|
/// | success, server middleware halting, skipped (batch cancelled) | release |
/// | interrupted (pushed back), over a limit or throttled (parked in the schedule) | hold |
/// | failure retried | hold (until the retry, plus `for`) |
/// | failure that will not run again (morgue, discarded, `retry: false`) | release |
/// | shutdown requeue | none |
pub(crate) fn lock_op(
    fetched: &Fetched,
    classified: &Classified,
    settlement: Option<&Settlement>,
) -> Option<LockOp> {
    let lock = fetched.lock.as_ref()?;
    if !lock.follows_the_job() || fetched.early_release.released() {
        return None;
    }
    let action = match classified {
        Classified::Success | Classified::Skipped => LockAction::Release,
        Classified::Interrupted | Classified::OverLimit(_) | Classified::Throttled(_) => {
            LockAction::Hold
        }
        Classified::Failure(verdict) => match settlement {
            Some(Settlement::Retry { .. }) => LockAction::Hold,
            Some(Settlement::Morgue { .. } | Settlement::Drop) => LockAction::Release,
            None => match verdict.as_ref() {
                Verdict::Retry { .. } => LockAction::Hold,
                Verdict::Kill { .. } | Verdict::Discard { .. } | Verdict::NoRetry { .. } => {
                    LockAction::Release
                }
            },
        },
    };
    Some(LockOp {
        lock: lock.clone(),
        action,
    })
}

/// The job as fetched, `job`, made into the copy that continues its execution (a retry, a job
/// pushed back or rescheduled, the morgue's copy): stored without uniqueness (`"unique":
/// false`) when the execution gave its lock up ([`JobContext::release_unique_lock`]), so that
/// the copy waits and runs as the job it now is, one without a lock: its outcome does no lock
/// work and counts no lapse, and the heartbeat renews nothing for it.
///
/// [`JobContext::release_unique_lock`]: crate::JobContext::release_unique_lock
pub(crate) fn continuation(fetched: &Fetched, mut job: JobPayload) -> JobPayload {
    if fetched.early_release.released() {
        job.unique = Some(UniqueOption::Disabled);
    }
    job
}

/// The events hash of the current minute.
fn events_key() -> String {
    keys::unique_minute(Utc::now())
}

/// Acknowledges `work` and does `op` in the same step (`ACK_UNIQUE`); with basic fetch, which
/// has nothing to acknowledge, `UNIQUE_LOCK_OP` alone.
pub(crate) async fn ack(fetcher: &Fetcher, work: &UnitOfWork, op: &LockOp) -> Result<()> {
    if fetcher.strategy() == FetchStrategy::Basic {
        lock_op_basic(fetcher.redis(), op, Next::Now, "acknowledged").await;
        return Ok(());
    }
    let lock = &op.lock;
    let (inprogress, events, fetched) = (fetcher.inprogress_key(), events_key(), work.job());
    let (_acked, status): (i64, i64) = fetcher
        .redis()
        .with_retry(|mut conn| {
            let events = &events;
            async move {
                ACK_UNIQUE
                    .script()
                    .key(inprogress)
                    .key(&lock.key)
                    .key(keys::UNIQUE_LOCKS)
                    .key(events)
                    .arg(fetched)
                    .arg(op.action.as_str())
                    .arg(&lock.jid)
                    .arg(&lock.digest)
                    .arg(&lock.class)
                    .arg(&lock.queue)
                    .arg(lock.until.as_str())
                    .arg(lock.ttl_ms())
                    .invoke_async(&mut conn)
                    .await
            }
        })
        .await?;
    note_status(lock, op.action, status, "acknowledged");
    Ok(())
}

/// Settles `work` and does `op` in the same step (`SETTLE_UNIQUE`); with basic fetch, the plain
/// write then `UNIQUE_LOCK_OP`. The return value is that of [`Fetcher::settle`]: `false` when the
/// job was no longer in progress (nothing written, no lock work).
pub(crate) async fn settle(
    fetcher: &Fetcher,
    work: &UnitOfWork,
    settlement: &Settlement,
    op: &LockOp,
) -> Result<bool> {
    let retry_at = match settlement {
        Settlement::Retry { at, .. } => Some(at.to_string()),
        Settlement::Morgue { .. } | Settlement::Drop => None,
    };
    if fetcher.strategy() == FetchStrategy::Basic {
        let next = retry_at.as_deref().map_or(Next::Now, Next::Retry);
        return around_basic_write(
            fetcher,
            op,
            next,
            "settled",
            fetcher.settle(work, settlement),
        )
        .await;
    }
    let (mode, key, member, score, cutoff, rank) = match settlement {
        Settlement::Retry { at, job } => ("retry", keys::RETRY, job.as_slice(), *at, 0.0, 0),
        Settlement::Morgue { job, bounds } => (
            "dead",
            keys::DEAD,
            job.as_slice(),
            bounds.now,
            bounds.cutoff,
            bounds.rank,
        ),
        Settlement::Drop => ("drop", keys::DEAD, &[][..], 0.0, 0.0, 0),
    };
    let lock = &op.lock;
    let (inprogress, events, fetched) = (fetcher.inprogress_key(), events_key(), work.job());
    let (settled, status): (i64, i64) = fetcher
        .redis()
        .with_retry(|mut conn| {
            let events = &events;
            async move {
                SETTLE_UNIQUE
                    .script()
                    .key(inprogress)
                    .key(key)
                    .key(&lock.key)
                    .key(keys::UNIQUE_LOCKS)
                    .key(events)
                    .arg(fetched)
                    .arg(mode)
                    .arg(member)
                    .arg(score)
                    .arg(cutoff)
                    .arg(rank)
                    .arg(op.action.as_str())
                    .arg(&lock.jid)
                    .arg(&lock.digest)
                    .arg(&lock.class)
                    .arg(&lock.queue)
                    .arg(lock.until.as_str())
                    .arg(lock.ttl_ms())
                    .invoke_async(&mut conn)
                    .await
            }
        })
        .await?;
    if settled == 1 {
        note_status(lock, op.action, status, "settled");
    }
    Ok(settled == 1)
}

/// Pushes `work` back (to the end of its queue, or into the schedule) and holds its lock in the
/// same step (`PUSH_BACK_UNIQUE`); with basic fetch, the plain write then `UNIQUE_LOCK_OP`. The
/// return value is [`Fetcher::push_back`]'s.
pub(crate) async fn push_back(
    fetcher: &Fetcher,
    work: &UnitOfWork,
    job: &Stored,
    op: &LockOp,
) -> Result<bool> {
    let (destination, member, score_or_queue, mode) = match job {
        Stored::Queued { queue, json } => (keys::queue(queue), json, queue.clone(), "queue"),
        Stored::Scheduled { at, json } => {
            (keys::SCHEDULE.to_owned(), json, at.to_string(), "schedule")
        }
    };
    if fetcher.strategy() == FetchStrategy::Basic {
        let next = if mode == "schedule" {
            Next::Scheduled(&score_or_queue, member)
        } else {
            Next::Now
        };
        return around_basic_write(
            fetcher,
            op,
            next,
            "pushed back",
            fetcher.push_back(work, job),
        )
        .await;
    }
    let lock = &op.lock;
    let (inprogress, events, fetched) = (fetcher.inprogress_key(), events_key(), work.job());
    let (destination, score_or_queue) = (&destination, &score_or_queue);
    let (written, status): (i64, i64) = fetcher
        .redis()
        .with_retry(|mut conn| {
            let events = &events;
            async move {
                PUSH_BACK_UNIQUE
                    .script()
                    .key(inprogress)
                    .key(keys::QUEUES)
                    .key(destination)
                    .key(&lock.key)
                    .key(keys::UNIQUE_LOCKS)
                    .key(events)
                    .arg(fetched)
                    .arg(member)
                    .arg(score_or_queue)
                    .arg(mode)
                    .arg(op.action.as_str())
                    .arg(&lock.jid)
                    .arg(&lock.digest)
                    .arg(&lock.class)
                    .arg(&lock.queue)
                    .arg(lock.until.as_str())
                    .arg(lock.ttl_ms())
                    .invoke_async(&mut conn)
                    .await
            }
        })
        .await?;
    if written == 1 {
        note_status(lock, op.action, status, "pushed back");
    }
    Ok(written == 1)
}

/// When a job whose outcome was written runs next, for the hold of its lock.
#[derive(Debug, Clone, Copy)]
enum Next<'a> {
    /// Now, or never: the hold lasts `for`.
    Now,
    /// At this score of the retry set: the hold lasts until then plus `for` (a debounce push
    /// never replaces a retry, so the lock records no `at`).
    Retry(&'a str),
    /// At this score of the schedule, as this member: the hold lasts until then plus `for`, and
    /// the lock records the score (`at`) and the member (`m`), so that a debounce push may
    /// replace the job.
    Scheduled(&'a str, &'a str),
}

/// An outcome write of basic fetch, which has no in-progress list to make it atomic with the
/// lock work: a hold comes first (the job may be fetched again as soon as it is written, and
/// must find its lock held, not take it after its next start), a release after the write (the
/// lock protects the job until its outcome is written). A hold whose write then fails leaves
/// the lock to its time to live.
async fn around_basic_write(
    fetcher: &Fetcher,
    op: &LockOp,
    next: Next<'_>,
    what: &str,
    write: impl Future<Output = Result<bool>>,
) -> Result<bool> {
    match op.action {
        LockAction::Hold => {
            lock_op_basic(fetcher.redis(), op, next, what).await;
            write.await
        }
        LockAction::Release => {
            let written = write.await?;
            lock_op_basic(fetcher.redis(), op, next, what).await;
            Ok(written)
        }
    }
}

/// The lock work of an outcome with basic fetch: `UNIQUE_LOCK_OP` (see
/// [`around_basic_write`]). A failure is logged, not returned: the outcome itself is written,
/// and the lock expires with its time to live.
async fn lock_op_basic(redis: &RedisPool, op: &LockOp, next: Next<'_>, what: &str) {
    let lock = &op.lock;
    let events = events_key();
    let (until_at, record, member) = match next {
        Next::Now => ("", "", ""),
        Next::Retry(at) => (at, "", ""),
        Next::Scheduled(at, member) => (at, at, member),
    };
    let result: Result<i64> = redis
        .with_retry(|mut conn| {
            let events = &events;
            async move {
                UNIQUE_LOCK_OP
                    .script()
                    .key(&lock.key)
                    .key(keys::UNIQUE_LOCKS)
                    .key(events)
                    .arg(op.action.as_str())
                    .arg(&lock.jid)
                    .arg(&lock.digest)
                    .arg(&lock.class)
                    .arg(&lock.queue)
                    .arg(lock.until.as_str())
                    .arg(lock.ttl_ms())
                    .arg(until_at)
                    .arg(record)
                    .arg(member)
                    .invoke_async(&mut conn)
                    .await
            }
        })
        .await;
    match result {
        Ok(status) => note_status(lock, op.action, status, what),
        Err(error) => tracing::warn!(
            %error,
            jid = %lock.jid,
            digest = %lock.digest,
            "could not update the lock of a unique job after its outcome; it expires with its \
             time to live"
        ),
    }
}

/// Reports what the lock work of an outcome found: a lapse (a `success` job that no longer held
/// its lock), or a lock key that could not be used.
fn note_status(lock: &LockRef, action: LockAction, status: i64, what: &str) {
    let held = match action {
        // 1: kept; 2: taken again (it had expired); 0: another job holds it.
        LockAction::Hold => status == 1,
        // 1: released; 0: no lock; -1: another job holds it.
        LockAction::Release => status == 1,
    };
    if status == -2 {
        tracing::warn!(
            jid = %lock.jid,
            class = %lock.class,
            digest = %lock.digest,
            "the lock of a unique job could not be used (a key of the wrong type?); the job was \
             {what} without its lock work"
        );
    } else if !held && lock.until == UniqueUntil::Success {
        lapse(lock, what);
    } else {
        tracing::trace!(jid = %lock.jid, status, action = action.as_str(), "unique lock work");
    }
}

/// Warns that a `success` job did not hold its lock when it should have (its `unique_for` is
/// shorter than the job's wait, retries or run, or the lock was released by hand): a duplicate
/// may have been pushed meanwhile. At most one warning per class and minute in a process; the
/// scripts count every lapse (`z`, see `UniqueLocks::events`).
pub(crate) fn lapse(lock: &LockRef, what: &str) {
    static LAST: Mutex<Option<HashMap<String, Instant>>> = Mutex::new(None);
    let now = Instant::now();
    {
        let mut last = LAST.lock().unwrap_or_else(PoisonError::into_inner);
        let last = last.get_or_insert_with(HashMap::new);
        if last.len() > 1_000 {
            last.retain(|_, at| now.duration_since(*at) < LAPSE_NOTICE_INTERVAL);
        }
        if last
            .get(&lock.class)
            .is_some_and(|at| now.duration_since(*at) < LAPSE_NOTICE_INTERVAL)
        {
            return;
        }
        last.insert(lock.class.clone(), now);
    }
    tracing::warn!(
        class = %lock.class,
        jid = %lock.jid,
        digest = %lock.digest,
        unique_for = lock.ttl.as_secs(),
        "a unique job no longer held its lock when it was {what}: its unique_for is shorter than \
         its wait, run or retries (or the lock was released by hand), so a duplicate may have \
         been pushed (one notice per class and minute)"
    );
}

/// The executions whose locks the heartbeat renews (`UNIQUE_RENEW`): the running jobs whose
/// policy is `success` and that did not give their lock up, so that each lock still held by its
/// job lasts at least `unique_for` more. The beat sends their calls ([`renew_calls`]) with the
/// throttle permits' in one pipeline (`pro::on_beat`), then hands the replies to [`renewed`].
pub(crate) fn running_locks(engine: &Engine) -> Vec<Arc<Fetched>> {
    engine
        .stats
        .featured()
        .into_iter()
        .map(|(_, fetched)| fetched)
        .filter(|fetched| {
            fetched
                .lock
                .as_ref()
                .is_some_and(|lock| lock.until == UniqueUntil::Success)
                && !fetched.early_release.released()
        })
        .collect()
}

/// The locks of `running` (each has one: see [`running_locks`]).
fn locks_of(running: &[Arc<Fetched>]) -> impl Iterator<Item = &LockRef> {
    running.iter().filter_map(|fetched| fetched.lock.as_ref())
}

/// The `UNIQUE_RENEW` calls renewing the locks of `running`, in chunks of [`RENEW_CHUNK`].
pub(crate) fn renew_calls(running: &[Arc<Fetched>]) -> Vec<ScriptCall> {
    running
        .chunks(RENEW_CHUNK)
        .map(|chunk| {
            let mut keys = Vec::with_capacity(chunk.len() + 1);
            let mut args = Vec::with_capacity(chunk.len() * 3);
            keys.push(keys::UNIQUE_LOCKS.to_owned());
            for lock in locks_of(chunk) {
                keys.push(lock.key.clone());
                args.push(lock.jid.clone());
                args.push(lock.digest.clone());
                args.push(lock.ttl_ms().to_string());
            }
            ScriptCall {
                script: &UNIQUE_RENEW,
                keys,
                args,
            }
        })
        .collect()
}

/// What the [`renew_calls`] of `running` answered (one reply per chunk, in order): warns about
/// the locks their jobs no longer held while they ran (each script answers their 1-based
/// positions). The renewal works from a snapshot taken before its round trip, so a job
/// acknowledged meanwhile (its outcome released the lock) or that gave its lock up meanwhile is
/// no lapse: only the executions still running once the reply came, and still claiming their
/// lock, are reported. A job that finished without its lock is counted and reported by its
/// outcome. A failed call is logged at debug level: the next beat tries again.
pub(crate) fn renewed(
    engine: &Engine,
    running: &[Arc<Fetched>],
    replies: &[::redis::RedisResult<::redis::Value>],
) {
    let lapsed = lapsed_executions(running, replies);
    if lapsed.is_empty() {
        return;
    }
    let still_running: HashSet<*const Fetched> = engine
        .stats
        .featured()
        .iter()
        .map(|(_, fetched)| Arc::as_ptr(fetched))
        .collect();
    for lock in still_lapsed(&lapsed, &still_running) {
        lapse(lock, "renewed while it ran");
    }
}

/// The executions of `running` whose lock the replies of their renewal say they no longer
/// held (one reply per chunk of [`RENEW_CHUNK`], each the 1-based positions in its chunk).
fn lapsed_executions<'a>(
    running: &'a [Arc<Fetched>],
    replies: &[::redis::RedisResult<::redis::Value>],
) -> Vec<&'a Arc<Fetched>> {
    let mut lapsed = Vec::new();
    for (chunk, reply) in running.chunks(RENEW_CHUNK).zip(replies) {
        let positions = reply
            .as_ref()
            .map_err(ToString::to_string)
            .and_then(|value| {
                ::redis::from_redis_value_ref::<Vec<usize>>(value)
                    .map_err(|error| error.to_string())
            });
        match positions {
            Ok(positions) => lapsed.extend(
                positions
                    .into_iter()
                    .filter_map(|position| chunk.get(position.checked_sub(1)?)),
            ),
            Err(error) => {
                tracing::debug!(%error, "could not renew the locks of running unique jobs");
            }
        }
    }
    lapsed
}

/// The locks of the `lapsed` executions that are lapses: executions still running after the
/// renewal's reply (`still_running`: their shared state, by address; the snapshot keeps each
/// one alive, so an address cannot be reused meanwhile) that did not give their lock up.
fn still_lapsed<'a>(
    lapsed: &[&'a Arc<Fetched>],
    still_running: &HashSet<*const Fetched>,
) -> Vec<&'a LockRef> {
    lapsed
        .iter()
        .filter(|fetched| {
            still_running.contains(&Arc::as_ptr(fetched)) && !fetched.early_release.released()
        })
        .filter_map(|fetched| fetched.lock.as_ref())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::limit::Reschedule;
    use crate::retry::MorgueBounds;
    use crate::unique::Unique;

    fn unique_job(until: UniqueUntil) -> JobPayload {
        let mut job = JobPayload::new("Sync", vec![]);
        job.jid = "0123456789abcdef01234567".to_owned();
        job.queue = "default".to_owned();
        let mut unique = Unique::new(Duration::from_secs(60)).until(until);
        unique.digest = Some("d1".to_owned());
        job.unique = Some(unique.into());
        job
    }

    fn fetched(until: UniqueUntil) -> Fetched {
        Fetched::from_payload(&unique_job(until))
    }

    fn reschedule() -> Reschedule {
        let job = JobPayload::new("Sync", vec![]);
        Reschedule::over_limit(&job, "w:stripe", Duration::from_secs(5), 1, None)
    }

    fn action(
        fetched: &Fetched,
        classified: &Classified,
        settlement: Option<&Settlement>,
    ) -> Option<LockAction> {
        lock_op(fetched, classified, settlement).map(|op| op.action)
    }

    #[test]
    fn outcomes_hold_or_release_the_lock() {
        let job = JobPayload::new("Sync", vec![]);
        let retry = Settlement::Retry {
            at: 1.0,
            job: Vec::new(),
        };
        let morgue = Settlement::Morgue {
            job: Vec::new(),
            bounds: MorgueBounds {
                now: 1.0,
                cutoff: 0.0,
                rank: -1,
            },
        };
        for until in [UniqueUntil::Success, UniqueUntil::Start] {
            let fetched = fetched(until);
            assert_eq!(
                action(&fetched, &Classified::Success, None),
                Some(LockAction::Release)
            );
            assert_eq!(
                action(&fetched, &Classified::Skipped, None),
                Some(LockAction::Release)
            );
            assert_eq!(
                action(&fetched, &Classified::Interrupted, None),
                Some(LockAction::Hold)
            );
            assert_eq!(
                action(&fetched, &Classified::OverLimit(reschedule()), None),
                Some(LockAction::Hold)
            );
            assert_eq!(
                action(&fetched, &Classified::Throttled(reschedule()), None),
                Some(LockAction::Hold)
            );
            let failure = Classified::Failure(Box::new(Verdict::NoRetry {
                job: job.clone(),
                error_class: "E".to_owned(),
                error_message: "e".to_owned(),
            }));
            assert_eq!(
                action(&fetched, &failure, Some(&retry)),
                Some(LockAction::Hold)
            );
            assert_eq!(
                action(&fetched, &failure, Some(&morgue)),
                Some(LockAction::Release)
            );
            assert_eq!(
                action(&fetched, &failure, Some(&Settlement::Drop)),
                Some(LockAction::Release)
            );
            let retried = Classified::Failure(Box::new(Verdict::Retry {
                at: 1.0,
                job: job.clone(),
            }));
            assert_eq!(action(&fetched, &retried, None), Some(LockAction::Hold));
            assert_eq!(action(&fetched, &failure, None), Some(LockAction::Release));
        }
        // `expired` locks ignore outcomes; plain jobs have none.
        let expired = fetched(UniqueUntil::Expired);
        assert_eq!(action(&expired, &Classified::Success, None), None);
        assert_eq!(
            action(&Fetched::default(), &Classified::Success, None),
            None
        );
    }

    #[test]
    fn a_job_that_gave_its_lock_up_does_no_lock_work_and_continues_without_it() {
        // unique-3, unique-4: the early release belongs to the execution (no process-wide
        // note to clean up or to mistake for another job's), whatever the policy.
        let retry = Settlement::Retry {
            at: 1.0,
            job: Vec::new(),
        };
        for until in [
            UniqueUntil::Success,
            UniqueUntil::Start,
            UniqueUntil::Expired,
        ] {
            let fetched = fetched(until);
            let job = unique_job(until);
            assert!(matches!(
                continuation(&fetched, job.clone()).unique,
                Some(UniqueOption::Enabled(_))
            ));
            fetched.early_release.clone().note();
            assert!(fetched.early_release.released());
            for classified in [
                Classified::Success,
                Classified::Interrupted,
                Classified::OverLimit(reschedule()),
            ] {
                assert_eq!(action(&fetched, &classified, None), None);
            }
            let failure = Classified::Failure(Box::new(Verdict::Retry {
                at: 1.0,
                job: job.clone(),
            }));
            assert_eq!(action(&fetched, &failure, Some(&retry)), None);
            // Its retry, push back or reschedule is stored without uniqueness.
            let continued = continuation(&fetched, job);
            assert_eq!(continued.unique, Some(UniqueOption::Disabled));
            assert!(continued.to_json().unwrap().contains("\"unique\":false"));
            assert!(LockRef::from_payload(&continued).is_none());
        }
        // A new execution of the same jid starts afresh.
        let again = fetched(UniqueUntil::Success);
        assert!(!again.early_release.released());
        assert_eq!(
            action(&again, &Classified::Success, None),
            Some(LockAction::Release)
        );
    }

    #[test]
    fn renewals_report_only_jobs_still_running_with_their_lock() {
        // unique-4: the beat's snapshot is taken before its round trip.
        let running: Vec<Arc<Fetched>> = (0..3)
            .map(|_| Arc::new(fetched(UniqueUntil::Success)))
            .collect();
        let replies = vec![Ok(::redis::Value::Array(vec![
            ::redis::Value::Int(1),
            ::redis::Value::Int(3),
        ]))];
        let lapsed = lapsed_executions(&running, &replies);
        assert_eq!(lapsed.len(), 2);
        assert!(Arc::ptr_eq(lapsed[0], &running[0]) && Arc::ptr_eq(lapsed[1], &running[2]));
        // The third job was acknowledged during the round trip: not a lapse.
        let mut still: HashSet<*const Fetched> =
            [Arc::as_ptr(&running[0]), Arc::as_ptr(&running[1])].into();
        assert_eq!(still_lapsed(&lapsed, &still).len(), 1);
        // The first gave its lock up meanwhile: not a lapse either.
        running[0].early_release.note();
        assert!(still_lapsed(&lapsed, &still).is_empty());
        // A failed renewal reports nothing; positions out of range are ignored.
        let failed: Vec<::redis::RedisResult<::redis::Value>> = vec![Err(
            ::redis::RedisError::from((::redis::ErrorKind::Io, "gone")),
        )];
        assert!(lapsed_executions(&running, &failed).is_empty());
        let odd = vec![Ok(::redis::Value::Array(vec![
            ::redis::Value::Int(0),
            ::redis::Value::Int(9),
        ]))];
        assert!(lapsed_executions(&running, &odd).is_empty());
        still.insert(Arc::as_ptr(&running[2]));
        assert_eq!(still_lapsed(&lapsed, &still).len(), 1);
        // Running jobs that gave their lock up are not renewed at all.
        assert_eq!(renew_calls(&running[1..2]).len(), 1);
    }
}
