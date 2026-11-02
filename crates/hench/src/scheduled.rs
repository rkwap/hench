//! The scheduler: moves due jobs from the `schedule` and `retry` sets to their queues and
//! prunes dead processes. See `docs/DESIGN.md` §5.
//!
//! Every process runs a poller. To spread the load without coordination, each one waits a
//! random interval between polls whose average is `poll_interval_average` or, by default, the
//! number of processes times `average_scheduled_poll_interval`, so that across the cluster
//! the sets are checked about once per `average_scheduled_poll_interval`.
//!
//! A poll takes due jobs one at a time, earliest first, and pushes each through the client, so
//! client middleware runs again. A job never leaves Redis before it is written elsewhere:
//!
//! 1. A script *leases* the earliest due member: it moves the member's score [`LEASE`] into
//!    the future (so that no other scheduler takes it meanwhile) and returns it with its
//!    original score.
//! 2. The job goes through the client's normalization and middleware (a panic there is a
//!    rejected push).
//! 3. One script ([`MOVE_OUT`]) pushes it to its queue (or the schedule, if middleware
//!    scheduled it) and only then removes it from its set, provided it is still there; so a
//!    script repeated after a lost reply, or racing another mover (the API), moves it once.
//!
//! A lost reply, a crash, an abort at the end of a shutdown or a panic at any point therefore
//! leaves the job in its set, due again when its lease ends at the latest. A job that cannot be
//! pushed keeps its original score when Redis is unavailable (the rest of the set waits for the
//! next poll) and is deferred a minute for any other error, so that it does not hold up the
//! jobs behind it (after ten such failures the set waits for the next poll). An invalid job
//! moves to the morgue ([`MOVE_TO_MORGUE`]).
//!
//! The jobs of the features go through the API's removals: a due job that client middleware
//! stops is deleted (a batch member counts dead in its batch, reason `deleted`; a unique job
//! releases its lock, `REMOVE_UNIQUE`), an invalid one is killed (`killed`, `KILL_UNIQUE`), and
//! a deferred unique job keeps its lock for the extra wait (`RESCORE_UNIQUE`).
//!
//! Jobs a limit parked in the schedule (throttled at the gate, or rescheduled over a limit)
//! come back at their slot in the limit's wait line, a few hundred milliseconds apart at most
//! for a busy limit: the process that parked a job asks its own scheduler to poll when the job
//! comes due ([`Wakeups`]), instead of leaving it to the next random poll (with N processes,
//! one every N × `average_scheduled_poll_interval` on average), which would bring the parked
//! jobs back in bursts that the limit refuses again but for a few.
//
// LANE: engine — this module is owned by the engine lane.

use std::collections::BTreeSet;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use ::redis::Script;
use futures::FutureExt;

use crate::api::{
    MOVE_OUT, MOVE_TO_MORGUE, cleanup_processes, live_process_count, lock_to_extend,
    rescore_holding,
};
use crate::batch::api::{Removal, after_removal, before_removal};
use crate::client::Stored;
use crate::config::{Config, ErrorContext};
use crate::error::{Error, JobError, Result};
use crate::keys;
use crate::launcher::{Engine, lock};
use crate::payload::{JobPayload, now_millis, now_secs_f64};
use crate::processor::is_unavailable;
use crate::redis::RedisPool;
use crate::retry::MorgueBounds;

/// Leases the earliest member of a sorted set whose score is at most `now`: moves its score
/// to the end of the lease, and returns it with its original score. KEYS: the sorted set.
/// ARGV: now, the end of the lease (epoch seconds). Returns `{member, score}` or nil.
const LEASE_DUE: &str = r"
local due = redis.call('ZRANGE', KEYS[1], '-inf', ARGV[1], 'BYSCORE', 'LIMIT', 0, 1, 'WITHSCORES')
if #due == 0 then
  return false
end
redis.call('ZADD', KEYS[1], 'XX', ARGV[2], due[1])
return due
";

pub(crate) static LEASE_DUE_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(LEASE_DUE));

/// The sorted sets polled, in order.
const SETS: [&str; 2] = [keys::RETRY, keys::SCHEDULE];

/// How long a due job taken by a scheduler stays out of the other schedulers' reach: if the
/// scheduler cannot finish (crash, lost reply, Redis unavailable), the job is due again then.
pub(crate) const LEASE: Duration = Duration::from_secs(60);

/// Pause after a failed poll-interval computation (Redis unreachable).
const ERROR_PAUSE: Duration = Duration::from_secs(5);

/// Pause after the poller itself panicked (a bug in hench), before it polls again.
const CRASH_PAUSE: Duration = Duration::from_secs(1);

/// Shortest wait between two polls.
const MIN_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// How long a due job whose push failed (other than because Redis is unavailable) waits
/// before it is tried again.
const PUSH_ERROR_DELAY: Duration = Duration::from_secs(60);

/// Local wake-ups of the scheduler: when jobs this process parked in the schedule come due
/// (see the module's documentation). A poll at a wake-up moves every due job, whoever parked it.
#[derive(Debug, Default)]
pub(crate) struct Wakeups {
    /// The pending wake-ups, earliest first.
    times: Mutex<BTreeSet<tokio::time::Instant>>,
    /// Notified when a wake-up earlier than every pending one is added.
    earlier: tokio::sync::Notify,
}

impl Wakeups {
    /// At most this many pending wake-ups: past that, the latest are dropped (their jobs come
    /// back at a regular poll, or at a later wake-up).
    const MAX: usize = 1_024;
    /// A wake-up is merged into a pending one at most this much later (a poll moves only the
    /// jobs due by then, so never into an earlier one).
    const MERGE: Duration = Duration::from_millis(20);
    /// Polls this long after a job comes due, so that it is due by the scheduler's clock.
    const MARGIN: Duration = Duration::from_millis(5);

    /// Asks for a poll when a job parked for `delay` comes due.
    pub(crate) fn after(&self, delay: Duration) {
        self.at(tokio::time::Instant::now() + delay + Self::MARGIN);
    }

    /// Asks for a poll at `at`.
    fn at(&self, at: tokio::time::Instant) {
        let mut times = lock(&self.times);
        // A pending wake-up a little later serves this one too.
        if times.range(at..=at + Self::MERGE).next().is_some() {
            return;
        }
        if times.len() >= Self::MAX {
            match times.last().copied() {
                Some(last) if last > at => {
                    times.remove(&last);
                }
                _ => return,
            }
        }
        let earliest = times.first().is_none_or(|first| at < *first);
        times.insert(at);
        drop(times);
        if earliest {
            // Wakes the scheduler if it waits (a `Notified` created before this call counts as
            // waiting); stores nothing otherwise: it reads the wake-ups before it waits again.
            self.earlier.notify_waiters();
        }
    }

    /// The earliest pending wake-up.
    fn next(&self) -> Option<tokio::time::Instant> {
        lock(&self.times).first().copied()
    }

    /// Forgets the wake-ups due by `now`: the poll about to start serves them.
    fn served(&self, now: tokio::time::Instant) {
        lock(&self.times).retain(|at| *at > now);
    }

    /// How many wake-ups are pending.
    #[cfg(test)]
    fn len(&self) -> usize {
        lock(&self.times).len()
    }
}

/// Sleeps `pause`, or until the earliest wake-up before its end (see [`Wakeups`]). `false`
/// once the process is quiet.
async fn sleep_until_poll(engine: &Engine, pause: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + pause;
    loop {
        // Registered before the wake-ups are read, so that one added meanwhile is noticed.
        let earlier = engine.wakeups.earlier.notified();
        let wakeup = engine.wakeups.next().filter(|at| *at < deadline);
        let until = wakeup.unwrap_or(deadline);
        tokio::select! {
            () = engine.quiet.cancelled() => return false,
            () = tokio::time::sleep_until(until) => {
                engine.wakeups.served(tokio::time::Instant::now());
                return true;
            }
            () = earlier => {}
        }
    }
}

/// Runs the poller until the process is quiet.
pub(crate) async fn run(engine: Arc<Engine>) {
    let config = &engine.config;
    let wait = if config.poll_interval_average.is_some() {
        Duration::ZERO
    } else {
        config.initial_scheduler_wait
    };
    let wait = wait.saturating_add(scale(config.initial_scheduler_jitter, fastrand::f64()));
    if !sleep_until_poll(&engine, wait).await {
        return;
    }
    while !engine.quiet.is_cancelled() {
        // Every poll, so that a process that died leaves the list about a minute after its
        // heartbeat expired (the cleanup's lock lets one process in the cluster do the work once
        // a minute, the others only send a `SET NX`).
        prune_dead_processes(&engine.redis).await;
        // Every job stays in Redis while it is handled, so a panic (a bug in hench: client
        // middleware panics are caught where it runs) loses nothing and the poller goes on;
        // and at the end of a shutdown a push still in progress (slow client middleware) can
        // simply be dropped: its job is due again when its lease ends.
        let polled = engine
            .background
            .run_until_cancelled(AssertUnwindSafe(poll(&engine)).catch_unwind())
            .await;
        let Some(polled) = polled else {
            tracing::debug!("the shutdown interrupted a scheduler poll");
            break;
        };
        let pause = match polled {
            Ok(Ok(Poll { enqueued, failed })) => {
                if enqueued > 0 {
                    tracing::debug!(enqueued, "enqueued due jobs");
                }
                if !failed {
                    engine.schedule_outage.succeeded();
                }
                None
            }
            Ok(Err(error)) => {
                engine.schedule_outage.failed(config, error);
                None
            }
            Err(panic) => {
                config.handle_error(
                    &JobError::panic(panic),
                    &ErrorContext::new("scheduler crashed; restarting it"),
                );
                Some(CRASH_PAUSE)
            }
        };
        let pause = match pause {
            Some(pause) => pause,
            None => match process_count(&engine.redis).await {
                Ok(count) => random_poll_interval(config, count),
                Err(error) => {
                    tracing::debug!(%error, "could not count processes");
                    ERROR_PAUSE
                }
            },
        };
        if !sleep_until_poll(&engine, pause).await {
            break;
        }
    }
    tracing::debug!("scheduler stopped");
}

/// At most this many due jobs of a set may fail to be pushed (for reasons other than Redis
/// being unavailable) in one poll: past that, the client middleware is presumably failing
/// for every job, and the rest of the set waits for the next poll.
const MAX_FAILURES_PER_SET: usize = 10;

/// The outcome of a poll.
#[derive(Debug, Default)]
struct Poll {
    /// Jobs enqueued.
    enqueued: usize,
    /// `true` if a push failed.
    failed: bool,
}

/// What happened to a leased job.
enum Pushed {
    /// Enqueued (or rescheduled by client middleware).
    Yes,
    /// Halted by client middleware, moved to the morgue, or already moved by someone else.
    Dropped,
    /// Pushing failed (not because of Redis): deferred, to be tried again later.
    Deferred,
    /// Pushing failed because of Redis: the job keeps its score (or its lease); the set waits
    /// for the next poll.
    PutBack,
}

/// Moves every due job of the retry and schedule sets to its queue, until the process goes
/// quiet. Fails if Redis fails (the rest waits for the next poll).
async fn poll(engine: &Engine) -> Result<Poll> {
    let mut poll = Poll::default();
    for set in SETS {
        let mut failures = 0;
        while !engine.quiet.is_cancelled() {
            let Some((member, score)) = lease_due(&engine.redis, set).await? else {
                break;
            };
            match push(engine, set, &member, score).await? {
                Pushed::Yes => poll.enqueued += 1,
                Pushed::Dropped => {}
                Pushed::Deferred => {
                    poll.failed = true;
                    failures += 1;
                    if failures >= MAX_FAILURES_PER_SET {
                        break;
                    }
                }
                Pushed::PutBack => {
                    poll.failed = true;
                    break;
                }
            }
        }
    }
    Ok(poll)
}

/// Pushes a leased job through the client and moves it out of `set`. Fails only if a job
/// that cannot be enqueued could not be moved to the morgue (it stays in `set`, leased).
async fn push(engine: &Engine, set: &'static str, member: &[u8], score: f64) -> Result<Pushed> {
    let job = match JobPayload::from_slice(member) {
        Ok(job) => job,
        Err(error) => {
            report(engine, error, "scheduled job is not valid JSON", member);
            bury(engine, set, member, None).await?;
            return Ok(Pushed::Dropped);
        }
    };
    // The job as stored, for the removals of the features (a batch member, a unique job).
    let featured = (job.bid.is_some() || job.unique.is_some()).then(|| job.clone());
    // A panicking client middleware is a rejected push, not a crashed scheduler.
    let prepared = AssertUnwindSafe(engine.client.prepare_push(job))
        .catch_unwind()
        .await
        .unwrap_or_else(|panic| Err(Error::Job(JobError::panic(panic))))
        .and_then(|prepared| {
            prepared
                .map(|job| Stored::encode(job, now_secs_f64(), now_millis()))
                .transpose()
        });
    let moved = match (prepared, &featured) {
        (Ok(None), Some(job)) => drop_featured(engine, set, member, job).await,
        (Ok(stored), _) => move_out(&engine.redis, set, member, stored.as_ref()).await,
        (Err(error), _) => Err(error),
    };
    match moved {
        Ok(Moved::Pushed) => {
            tracing::debug!(set, "enqueued due job");
            Ok(Pushed::Yes)
        }
        Ok(Moved::Halted) => {
            tracing::debug!(set, "client middleware stopped a due job; it was dropped");
            Ok(Pushed::Dropped)
        }
        Ok(Moved::Gone) => {
            tracing::debug!(set, "a due job was moved by someone else");
            Ok(Pushed::Dropped)
        }
        Err(error @ (Error::InvalidJob(_) | Error::Json(_))) => {
            report(engine, error, "scheduled job is invalid", member);
            bury(engine, set, member, featured.as_ref()).await?;
            Ok(Pushed::Dropped)
        }
        // Redis cannot serve anything right now (unreachable, out of memory, ...): give the
        // job its score back (or leave the lease to end) and the rest of the set to the next
        // poll. Whether or not a lost reply hides a move that happened, `XX` cannot bring a
        // moved job back.
        Err(error) if is_unavailable(&error) => {
            if let Err(restore) = rescore(&engine.redis, set, member, score).await {
                tracing::debug!(error = %restore, "could not give a due job its score back");
            }
            let jobstr = String::from_utf8_lossy(member);
            engine
                .schedule_outage
                .failed_with_job(&engine.config, error, Some(&jobstr));
            Ok(Pushed::PutBack)
        }
        // Anything else (client middleware failing or panicking, an error specific to this
        // job's keys) may well happen again: try the job again later, without holding up the
        // jobs behind it.
        Err(error) => {
            let later = now_secs_f64() + PUSH_ERROR_DELAY.as_secs_f64();
            // A unique job keeps its lock while it waits the extra minute (its time to live was
            // computed for its original run time).
            let deferred = match featured.as_ref().and_then(lock_to_extend) {
                Some(lock) => rescore_holding(&engine.redis, set, member, later, &lock)
                    .await
                    .map(|_| ()),
                None => rescore(&engine.redis, set, member, later).await,
            };
            if let Err(defer) = deferred {
                tracing::debug!(error = %defer, "could not defer a due job; its lease defers it");
            }
            let jobstr = String::from_utf8_lossy(member);
            engine
                .schedule_outage
                .failed_with_job(&engine.config, error, Some(&jobstr));
            Ok(Pushed::Deferred)
        }
    }
}

/// Reports a job that can never be enqueued.
fn report(engine: &Engine, error: Error, context: &str, member: &[u8]) {
    let jobstr = String::from_utf8_lossy(member);
    engine.config.handle_error(
        &JobError::from(error),
        &ErrorContext::new(context).with_jobstr(&jobstr),
    );
}

/// Leases the earliest due member of `set` (see [`LEASE_DUE`]), returning it with its
/// original score. Repeating the script after a lost reply leases the next one; the first is
/// due again when its lease ends.
async fn lease_due(redis: &RedisPool, set: &str) -> Result<Option<(Vec<u8>, f64)>> {
    let now = now_secs_f64();
    let until = now + LEASE.as_secs_f64();
    let leased: Option<(Vec<u8>, String)> = redis
        .with_retry(|mut conn| async move {
            LEASE_DUE_SCRIPT
                .key(set)
                .arg(now)
                .arg(until)
                .invoke_async(&mut conn)
                .await
        })
        .await?;
    Ok(leased.map(|(member, score)| (member, score.parse().unwrap_or(now))))
}

/// What [`move_out`] did.
enum Moved {
    /// The job went to its queue (or the schedule).
    Pushed,
    /// Client middleware stopped the push: the job was only removed from its set.
    Halted,
    /// The job was no longer in its set (moved by the API, or by this very script before a
    /// lost reply).
    Gone,
}

/// Moves `member` out of `set` with [`MOVE_OUT`]: to where `stored` says, or nowhere (`None`:
/// client middleware stopped the push). Nothing happens unless the member is still in the set,
/// so repeating the script after a dropped connection moves the job once.
async fn move_out(
    redis: &RedisPool,
    set: &str,
    member: &[u8],
    stored: Option<&Stored>,
) -> Result<Moved> {
    let (mode, target, job, extra) = match stored {
        Some(Stored::Queued { queue, json }) => {
            ("queue", keys::queue(queue), json.as_str(), queue.clone())
        }
        Some(Stored::Scheduled { at, json }) => (
            "schedule",
            keys::SCHEDULE.to_owned(),
            json.as_str(),
            at.to_string(),
        ),
        None => ("drop", set.to_owned(), "", String::new()),
    };
    let (target, extra) = (&target, &extra);
    let moved: i64 = redis
        .with_retry(|mut conn| async move {
            MOVE_OUT
                .key(set)
                .key(target)
                .key(keys::QUEUES)
                .arg(member)
                .arg(mode)
                .arg(job)
                .arg(extra)
                .invoke_async(&mut conn)
                .await
        })
        .await?;
    Ok(match moved {
        0 => Moved::Gone,
        _ if stored.is_none() => Moved::Halted,
        _ => Moved::Pushed,
    })
}

/// Removes `member`, a job of a feature (`job`) that client middleware stopped, from `set` as
/// the API deletes one: its batch counts it dead (reason `deleted`) before the removal, its
/// unique lock is released in the same script as the removal (`REMOVE_UNIQUE`, the holder
/// compared), and the batch's events are dispatched after. A job holding no lock that follows
/// it is removed with [`MOVE_OUT`].
async fn drop_featured(
    engine: &Engine,
    set: &str,
    member: &[u8],
    job: &JobPayload,
) -> Result<Moved> {
    let Ok(value) = std::str::from_utf8(member) else {
        return move_out(&engine.redis, set, member, None).await;
    };
    let reports = before_removal(&engine.redis, std::slice::from_ref(job), Removal::Deleted).await;
    let moved = match crate::unique::api::delete_from_set(&engine.redis, set, value, job).await {
        Some(Ok(true)) => Ok(Moved::Halted),
        Some(Ok(false)) => Ok(Moved::Gone),
        Some(Err(error)) => Err(error),
        None => move_out(&engine.redis, set, member, None).await,
    };
    after_removal(&engine.redis, reports).await;
    moved
}

/// Moves a job that can never be enqueued from `set` to the morgue (if it is still in `set`),
/// trimming the morgue. A job of a feature (`job`, the member parsed) is killed as the API kills
/// one: its batch counts it dead (reason `killed`) first, its unique lock is released in the
/// same script as the move (`KILL_UNIQUE`), and the batch's events are dispatched after.
async fn bury(engine: &Engine, set: &str, member: &[u8], job: Option<&JobPayload>) -> Result<()> {
    if let Some(job) = job
        && let Ok(value) = std::str::from_utf8(member)
    {
        let reports =
            before_removal(&engine.redis, std::slice::from_ref(job), Removal::Killed).await;
        let bounds = MorgueBounds::new(&engine.config);
        let morgue = crate::unique::api::MorgueBounds {
            key: keys::DEAD,
            now: bounds.now,
            cutoff: bounds.cutoff,
            rank: bounds.rank,
        };
        let killed =
            crate::unique::api::kill_from_set(&engine.redis, set, morgue, value, job).await;
        let result = match killed {
            Some(result) => result.map(|_| ()),
            None => move_to_morgue(engine, set, member).await,
        };
        after_removal(&engine.redis, reports).await;
        return result;
    }
    move_to_morgue(engine, set, member).await
}

/// [`MOVE_TO_MORGUE`] of `member` from `set`.
async fn move_to_morgue(engine: &Engine, set: &str, member: &[u8]) -> Result<()> {
    let MorgueBounds { now, cutoff, rank } = MorgueBounds::new(&engine.config);
    let _moved: i64 = engine
        .redis
        .with_retry(|mut conn| async move {
            MOVE_TO_MORGUE
                .key(set)
                .key(keys::DEAD)
                .arg(member)
                .arg(now)
                .arg(cutoff)
                .arg(rank)
                .invoke_async(&mut conn)
                .await
        })
        .await?;
    Ok(())
}

/// Sets the score of `member` in `set`, if it is still there (`ZADD XX`: never brings back a
/// job that was moved meanwhile).
async fn rescore(redis: &RedisPool, set: &str, member: &[u8], score: f64) -> Result<()> {
    redis
        .with_retry(|mut conn| async move {
            ::redis::cmd("ZADD")
                .arg(set)
                .arg("XX")
                .arg(score)
                .arg(member)
                .exec_async(&mut conn)
                .await
        })
        .await
}

/// Removes the processes whose heartbeat expired from the `processes` set (see
/// [`cleanup_processes`]: at most once a minute across the cluster).
async fn prune_dead_processes(redis: &RedisPool) {
    match cleanup_processes(redis).await {
        Ok(0) => {}
        Ok(pruned) => tracing::info!(pruned, "removed dead processes from the process list"),
        Err(error) => tracing::warn!(%error, "could not prune dead processes"),
    }
}

/// Number of live processes (at least 1): the members of `processes` with a heartbeat, not the
/// size of the set, which counts processes that died until they are pruned.
async fn process_count(redis: &RedisPool) -> Result<u64> {
    Ok(live_process_count(redis).await?.max(1))
}

/// The average interval between polls of this process, given the number of processes.
pub(crate) fn poll_interval_average(config: &Config, process_count: u64) -> Duration {
    config.poll_interval_average.unwrap_or_else(|| {
        config
            .average_scheduled_poll_interval
            .saturating_mul(u32::try_from(process_count).unwrap_or(u32::MAX))
    })
}

/// A random wait before the next poll. Below 10 processes it is within ±50% of the average
/// (so a small cluster stays close to its target); from 10 processes on it is uniform between
/// zero and twice the average.
pub(crate) fn random_poll_interval(config: &Config, process_count: u64) -> Duration {
    let average = poll_interval_average(config, process_count);
    let factor = if process_count < 10 {
        fastrand::f64() + 0.5
    } else {
        fastrand::f64() * 2.0
    };
    scale(average, factor).max(MIN_POLL_INTERVAL)
}

/// `duration × factor` (factor ≥ 0), saturating instead of panicking on overflow.
fn scale(duration: Duration, factor: f64) -> Duration {
    Duration::try_from_secs_f64(duration.as_secs_f64() * factor).unwrap_or(Duration::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(poll_interval_average: Option<Duration>, average: Duration) -> Config {
        Config {
            poll_interval_average,
            average_scheduled_poll_interval: average,
            ..Config::default()
        }
    }

    #[test]
    fn intervals_target_a_configured_average() {
        let config = config(Some(Duration::from_secs(10)), Duration::from_secs(5));
        let samples = 2_000;
        let intervals: Vec<f64> = (0..samples)
            .map(|_| random_poll_interval(&config, 1).as_secs_f64())
            .collect();
        assert!(intervals.iter().all(|x| (5.0..=15.0).contains(x)));
        let mean = intervals.iter().sum::<f64>() / f64::from(samples);
        assert!((mean - 10.0).abs() < 0.3, "{mean}");
    }

    #[test]
    fn intervals_scale_with_the_number_of_processes() {
        let config = config(None, Duration::from_secs(10));
        let samples = 2_000;
        let intervals: Vec<f64> = (0..samples)
            .map(|_| random_poll_interval(&config, 10).as_secs_f64())
            .collect();
        assert!(intervals.iter().all(|x| (0.0..=200.0).contains(x)));
        let mean = intervals.iter().sum::<f64>() / f64::from(samples);
        assert!((mean - 100.0).abs() < 5.0, "{mean}");

        let intervals: Vec<f64> = (0..samples)
            .map(|_| random_poll_interval(&config, 3).as_secs_f64())
            .collect();
        assert!(intervals.iter().all(|x| (15.0..=45.0).contains(x)));
        let mean = intervals.iter().sum::<f64>() / f64::from(samples);
        assert!((mean - 30.0).abs() < 1.5, "{mean}");
    }

    #[test]
    fn scaled_average() {
        let config = config(None, Duration::from_secs(10));
        assert_eq!(poll_interval_average(&config, 3), Duration::from_secs(30));
        let fixed = self::config(Some(Duration::from_secs(7)), Duration::from_secs(10));
        assert_eq!(poll_interval_average(&fixed, 3), Duration::from_secs(7));
        assert!(
            random_poll_interval(&self::config(Some(Duration::ZERO), Duration::ZERO), 1)
                >= MIN_POLL_INTERVAL
        );
        // Huge settings never panic; overflows saturate.
        let huge = self::config(None, Duration::MAX);
        for _ in 0..100 {
            assert!(random_poll_interval(&huge, 50) >= MIN_POLL_INTERVAL);
        }
        assert_eq!(scale(Duration::MAX, 1.5), Duration::MAX);
        assert!(scale(Duration::MAX, 0.999_999) > Duration::from_secs(1 << 60));
        assert_eq!(scale(Duration::from_secs(10), 0.5), Duration::from_secs(5));
    }

    #[tokio::test(start_paused = true)]
    async fn wakeups_keep_the_earliest_and_merge_only_into_later_ones() {
        let wakeups = Wakeups::default();
        let now = tokio::time::Instant::now();
        assert_eq!(wakeups.next(), None);
        wakeups.at(now + Duration::from_secs(2));
        // Within the merge window before a pending one: served by it.
        wakeups.at(now + Duration::from_secs(2) - Duration::from_millis(10));
        assert_eq!(wakeups.len(), 1);
        // After a pending one (never served by an earlier poll), or far before it: kept.
        wakeups.at(now + Duration::from_secs(2) + Duration::from_millis(10));
        wakeups.at(now + Duration::from_secs(1));
        assert_eq!(wakeups.len(), 3);
        assert_eq!(wakeups.next(), Some(now + Duration::from_secs(1)));
        // A poll serves what is due by its start.
        wakeups.served(now + Duration::from_secs(2));
        assert_eq!(
            wakeups.next(),
            Some(now + Duration::from_secs(2) + Duration::from_millis(10))
        );
        // `after` adds the margin that makes the job due by the scheduler's clock.
        let wakeups = Wakeups::default();
        wakeups.after(Duration::from_secs(1));
        assert_eq!(
            wakeups.next(),
            Some(now + Duration::from_secs(1) + Wakeups::MARGIN)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn wakeups_are_bounded_by_dropping_the_latest() {
        let wakeups = Wakeups::default();
        let now = tokio::time::Instant::now();
        for index in 0..Wakeups::MAX as u64 {
            wakeups.at(now + Duration::from_secs(10 + index));
        }
        assert_eq!(wakeups.len(), Wakeups::MAX);
        // Later than every pending one: dropped.
        wakeups.at(now + Duration::from_secs(100_000));
        assert_eq!(wakeups.len(), Wakeups::MAX);
        // Earlier: kept, in place of the latest.
        wakeups.at(now + Duration::from_secs(1));
        assert_eq!(wakeups.len(), Wakeups::MAX);
        assert_eq!(wakeups.next(), Some(now + Duration::from_secs(1)));
        let latest = lock(&wakeups.times).last().copied();
        assert_eq!(
            latest,
            Some(now + Duration::from_secs(10 + Wakeups::MAX as u64 - 2))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_earlier_wakeup_is_noticed_while_waiting() {
        let wakeups = Arc::new(Wakeups::default());
        let now = tokio::time::Instant::now();
        wakeups.at(now + Duration::from_secs(30));
        // A waiter in the loop of `sleep_until_poll`: registered (created, not yet polled),
        // then reads the wake-ups.
        let earlier = wakeups.earlier.notified();
        assert_eq!(wakeups.next(), Some(now + Duration::from_secs(30)));
        wakeups.at(now + Duration::from_secs(1));
        tokio::time::timeout(Duration::from_millis(1), earlier)
            .await
            .expect("notified of the earlier wake-up");
        // A later one does not disturb it.
        let later = wakeups.earlier.notified();
        wakeups.at(now + Duration::from_secs(60));
        assert!(
            tokio::time::timeout(Duration::from_millis(1), later)
                .await
                .is_err()
        );
    }
}
