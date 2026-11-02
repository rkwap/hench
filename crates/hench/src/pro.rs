//! Glue between the engine and the pro features (unique jobs, periodic jobs, batches, rate
//! limiting): the per-execution feature state, the gate and classification enums, and the
//! calls the launcher makes on every beat, at start and for the background tasks
//! (`docs/design/pro-features.md` §3, §4.3).
//!
//! This module only calls the feature modules' hook functions, in the fixed order of the
//! execution pipeline; each feature's logic stays in its module.

use std::sync::Arc;

use futures::future::BoxFuture;
use serde_json::{Map, Value, json};

use crate::batch::{self, BatchMember};
use crate::config::Config;
use crate::error::{Error, JobError, Result};
use crate::launcher::Engine;
use crate::limit::{self, Reschedule, ThrottleRef};
use crate::payload::JobPayload;
use crate::periodic::{self, PeriodicRef};
use crate::redis::RedisPool;
use crate::retry::Verdict;
use crate::unique::{self, LockRef};

/// The feature state of one execution, built once from the job **as fetched** (never as server
/// middleware changed it), by the processor or the testing executor.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct Fetched {
    /// The unique lock the job holds.
    pub(crate) lock: Option<LockRef>,
    /// The batch the job is a member of.
    pub(crate) member: Option<BatchMember>,
    /// The schedule that enqueued the job.
    pub(crate) periodic: Option<PeriodicRef>,
    /// The throttle the job names (resolved against the process's throttles by the gate).
    pub(crate) throttle: Option<ThrottleRef>,
    /// Whether the job released its unique lock while it ran (shared with its context).
    pub(crate) early_release: unique::EarlyRelease,
}

impl Fetched {
    /// The state of `job`, as fetched.
    pub(crate) fn from_payload(job: &JobPayload) -> Self {
        Self {
            lock: LockRef::from_payload(job),
            member: BatchMember::from_payload(job),
            periodic: PeriodicRef::from_payload(job),
            throttle: ThrottleRef::from_payload(job),
            early_release: unique::EarlyRelease::default(),
        }
    }

    /// `true` when the job uses no feature: the processor then does exactly what it did before
    /// the features existed.
    pub(crate) fn is_plain(&self) -> bool {
        self.lock.is_none()
            && self.member.is_none()
            && self.periodic.is_none()
            && self.throttle.is_none()
    }
}

/// What the throttle gate decided for a fetched job (`limit::engine::gate`). The processor
/// performs every write.
#[derive(Debug)]
pub(crate) enum Gate {
    /// Run the job (any permits are held by the engine under the processor's slot).
    Run,
    /// Refused: park the job in the schedule (or skip it when its batch was cancelled).
    Refused(Reschedule),
    /// Redis could not answer: requeue the job at the front of its queue and report a fetch
    /// outage.
    Unavailable(Error),
    /// The check failed for a reason specific to the job (a key of the wrong type): the job
    /// fails with this error.
    Failed(JobError),
}

/// How an execution ended, from the job as fetched, for every feature hook after `perform`
/// (`docs/design/pro-features.md` §3.2 step 4).
#[derive(Debug)]
pub(crate) enum Classified {
    /// `perform` succeeded, or server middleware halted the job.
    Success,
    /// The job's batch was cancelled (the endpoint's check, or a throttle refusal of a member of
    /// a cancelled batch): acknowledged without running again, counted as skipped. Batch
    /// accounting confirms it before the outcome write.
    Skipped,
    /// An iterable job saved its progress and continues later: pushed back to its queue.
    Interrupted,
    /// Over a limiter's limit while running, within its budget: rescheduled.
    OverLimit(Reschedule),
    /// Refused by the throttle gate before it ran: rescheduled.
    Throttled(Reschedule),
    /// Failed, with the retry handler's verdict (an over-limit refusal past its budget
    /// included). Boxed: the verdict holds the job.
    Failure(Box<Verdict>),
}

/// The heartbeat's feature work, after the work state: the renewals of the unique locks of
/// running jobs (`UNIQUE_RENEW`) and of the throttle permits the engine holds
/// (`LIMIT_RENEW`), sent in one pipeline: one round trip per beat whatever the features in use,
/// none when no running job uses them. The permits stay locked for the round trip, so that a
/// permit released meanwhile is released after its renewal. Errors are logged by the features;
/// a beat never fails because of them.
pub(crate) async fn on_beat(engine: &Engine) {
    let locks = unique::engine::running_locks(engine);
    let permits = engine.permits.renewing().await;
    let mut calls = unique::engine::renew_calls(&locks);
    let unique_calls = calls.len();
    calls.extend(permits.calls());
    if calls.is_empty() {
        return;
    }
    match crate::lua::run_pipelined(&engine.redis, &calls).await {
        Ok(mut replies) => {
            let limit_replies = replies.split_off(unique_calls.min(replies.len()));
            unique::engine::renewed(engine, &locks, &replies);
            permits.renewed(limit_replies);
        }
        Err(error) => {
            tracing::debug!(%error, "could not renew the locks and permits of running jobs");
        }
    }
}

/// The background tasks of the features: the periodic poller (it stops when the shutdown
/// starts) and batch maintenance (it stops when the process is quiet). Each task watches its
/// own stop signal.
pub(crate) fn background_tasks(engine: &Arc<Engine>) -> Vec<BoxFuture<'static, ()>> {
    [
        periodic::poller::task(engine),
        batch::maintenance::task(engine),
    ]
    .into_iter()
    .flatten()
    .collect()
}

/// The features' part of the heartbeat's `info` JSON: `features` (the stored-format version of
/// each feature this process understands; features not supported are absent), `periodic` (with
/// the `periodic` cargo feature: whether this process polls, the group whose crontab it
/// registers, the crontab's digest and its number of schedules) and the `throttles` definitions
/// when there are some.
pub(crate) fn info(engine: &Engine) -> Map<String, Value> {
    let mut info = Map::new();
    let features: Map<String, Value> = [
        ("unique", unique::FORMAT),
        ("periodic", periodic::FORMAT),
        ("batches", batch::FORMAT),
        ("limits", limit::FORMAT),
    ]
    .into_iter()
    .filter_map(|(name, format)| Some((name.to_owned(), json!(format?))))
    .collect();
    info.insert("features".to_owned(), Value::Object(features));
    if let Some(periodic) = periodic::info(&engine.config) {
        info.insert("periodic".to_owned(), periodic);
    }
    if !engine.throttles.is_empty() {
        info.insert(
            "throttles".to_owned(),
            Value::Object(engine.throttles.specs()),
        );
    }
    info
}

/// The features' checks of a configuration at `build`, before any I/O: the code-defined
/// periodic schedules, the throttles and the batch settings. (The throttles that registered
/// jobs define or name are checked once the registry exists: `limit::throttle::check_registry`.)
pub(crate) fn validate(config: &Config) -> Result<()> {
    periodic::validate_all(config)?;
    limit::validate(config)?;
    batch::validate(config)
}

/// The features' start-up checks: Redis 6.2 must replicate scripts by their effects, since
/// every feature script reads `TIME` before writing.
pub(crate) async fn start_checks(redis: &RedisPool) {
    crate::lua::check_effects_replication(redis).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::time::Duration;

    #[test]
    fn jobs_without_features_are_plain() {
        let job = JobPayload::new("Plain", vec![json!(1)]);
        let fetched = Fetched::from_payload(&job);
        assert!(fetched.is_plain());
        assert_eq!(fetched, Fetched::default());
    }

    #[test]
    fn the_state_comes_from_every_feature_field() {
        let mut job = JobPayload::new("Featured", vec![]);
        job.jid = "0123456789abcdef01234567".to_owned();
        job.queue = "default".to_owned();
        let mut unique = unique::Unique::new(Duration::from_secs(60));
        unique.digest = Some("d1".to_owned());
        job.unique = Some(unique.into());
        job.bid = Some("89abcdef0123456789abcdef".to_owned());
        job.periodic = Some(periodic::PeriodicMeta::new("nightly", 1_000, false));
        job.throttle = Some(limit::ThrottleOption::named("stripe"));
        let fetched = Fetched::from_payload(&job);
        assert!(!fetched.is_plain());
        assert_eq!(
            fetched.lock.map(|lock| lock.key),
            Some("unique:d1".to_owned())
        );
        assert_eq!(
            fetched.member.map(|member| member.bid),
            Some("89abcdef0123456789abcdef".to_owned())
        );
        assert_eq!(fetched.periodic.map(|tick| tick.tick), Some(1_000));
        assert_eq!(
            fetched.throttle.map(|throttle| throttle.name),
            Some("stripe".to_owned())
        );
    }
}
