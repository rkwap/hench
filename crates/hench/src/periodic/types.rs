//! The types of periodic jobs: the payload field, the tick a job was enqueued for, the
//! catch-up policies and the public enums of the API (`docs/design/periodic.md` §2.3).

use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

use crate::payload::{JobPayload, millis_to_system_time};

/// The `periodic` field of a job enqueued by a schedule: `{"name", "tick" (ms), "manual"?}`.
/// Set by the poller (and "enqueue now", with `manual`), kept through retries, recoveries and
/// interruptions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct PeriodicMeta {
    /// The schedule's name.
    pub name: String,
    /// The tick, in epoch milliseconds (for a manual enqueue: when it was requested).
    pub tick: i64,
    /// `true` for "enqueue now" (dashboard, CLI, API).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub manual: bool,
}

impl PeriodicMeta {
    /// The field of a job enqueued by schedule `name` for `tick` (epoch milliseconds).
    pub fn new(name: impl Into<String>, tick: i64, manual: bool) -> Self {
        Self {
            name: name.into(),
            tick,
            manual,
        }
    }
}

/// The tick a job was enqueued for ([`JobContext::periodic`](crate::JobContext)).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct PeriodicTick {
    /// The schedule's name.
    pub name: String,
    /// The tick (for a manual enqueue: when it was requested).
    pub tick: SystemTime,
    /// `true` for "enqueue now" (dashboard, CLI, API).
    pub manual: bool,
}

impl From<&PeriodicMeta> for PeriodicTick {
    fn from(meta: &PeriodicMeta) -> Self {
        Self {
            name: meta.name.clone(),
            tick: millis_to_system_time(meta.tick),
            manual: meta.manual,
        }
    }
}

/// What to do with ticks that could not be enqueued on time ([periodic.md] §2.2.5).
///
/// [periodic.md]: https://github.com/rkwap/hench/blob/v0.1.0/docs/design/periodic.md
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum CatchUp {
    /// Enqueue one job for the most recent missed tick (the default).
    #[default]
    Latest,
    /// Enqueue a tick only within its deadline (5 minutes unless set); never catch up.
    Skip,
    /// Enqueue every missed tick, oldest first, at most the `max` most recent ones (1 to
    /// 10 000).
    All {
        /// Most ticks enqueued by one catch-up.
        max: u32,
    },
}

impl CatchUp {
    /// The policy's name in definitions and TOML (`latest`, `skip`, `all`).
    pub fn as_str(&self) -> &'static str {
        match self {
            CatchUp::Latest => "latest",
            CatchUp::Skip => "skip",
            CatchUp::All { .. } => "all",
        }
    }
}

/// How a schedule's latest execution ended (`RECORD_RUN`, `LastRun::status`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum RunStatus {
    /// The job succeeded (or server middleware halted it).
    Succeeded,
    /// The job failed (whatever its verdict).
    Failed,
    /// An iterable job was interrupted and continues later.
    Interrupted,
    /// The job ran and returned over a limiter's limit: the rate limiter put it back in the
    /// schedule.
    Rescheduled,
}

impl RunStatus {
    /// The status as stored in the schedule's hash (`ok`, `failed`, `interrupted`,
    /// `rescheduled`).
    pub fn as_str(self) -> &'static str {
        match self {
            RunStatus::Succeeded => "ok",
            RunStatus::Failed => "failed",
            RunStatus::Interrupted => "interrupted",
            RunStatus::Rescheduled => "rescheduled",
        }
    }

    /// The status stored as `name` (`None` for a name this version does not know).
    pub fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "ok" => RunStatus::Succeeded,
            "failed" => RunStatus::Failed,
            "interrupted" => RunStatus::Interrupted,
            "rescheduled" => RunStatus::Rescheduled,
            _ => return None,
        })
    }
}

/// What the poller (or "enqueue now") did with a tick (a history entry's `result`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum FireResult {
    /// Pushed to its queue.
    Enqueued,
    /// Pushed to the schedule by client middleware setting `at`.
    Scheduled,
    /// Client middleware stopped the push.
    Halted,
    /// Missed beyond its deadline, or left out by the catch-up policy.
    Skipped,
    /// Not pushed: the job is unique and its lock is held (the previous tick's job is pending
    /// or running).
    Duplicate,
    /// Enqueued by hand ("enqueue now").
    Manual,
}

impl FireResult {
    /// The result as stored (`enqueued`, `scheduled`, `halted`, `skipped`, `duplicate`,
    /// `manual`).
    pub fn as_str(self) -> &'static str {
        match self {
            FireResult::Enqueued => "enqueued",
            FireResult::Scheduled => "scheduled",
            FireResult::Halted => "halted",
            FireResult::Skipped => "skipped",
            FireResult::Duplicate => "duplicate",
            FireResult::Manual => "manual",
        }
    }

    /// The result stored as `name` (`None` for a name this version does not know).
    pub fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "enqueued" => FireResult::Enqueued,
            "scheduled" => FireResult::Scheduled,
            "halted" => FireResult::Halted,
            "skipped" => FireResult::Skipped,
            "duplicate" => FireResult::Duplicate,
            "manual" => FireResult::Manual,
            _ => return None,
        })
    }
}

/// What an upsert of a schedule changed (`PeriodicSet::upsert`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Upserted {
    /// The schedule did not exist.
    Created,
    /// The definition changed, the timetable did not.
    Updated,
    /// The timetable changed: the next tick was computed again.
    Rescheduled,
    /// Nothing changed.
    Unchanged,
}

/// Who defines a schedule.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum PeriodicSource {
    /// Code ([`HenchBuilder::periodic`](crate::HenchBuilder)), registered by `group`'s
    /// processes.
    Code {
        /// The group whose crontab holds it.
        group: String,
    },
    /// The API, the dashboard or the CLI (dynamic schedules).
    Api,
}

/// When a schedule ticks.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Timetable {
    /// A cron expression in a time zone.
    Cron {
        /// The expression (5 or 6 fields, or an alias such as `@daily`).
        expression: String,
        /// The IANA time zone (`UTC` by default).
        tz: String,
    },
    /// Every `interval` (whole seconds), aligned to the Unix epoch plus `offset`.
    Every {
        /// The interval.
        interval: Duration,
        /// The offset of the ticks (below the interval).
        offset: Duration,
    },
}

/// The schedule and tick of a fetched job, for `RECORD_RUN` (crate-internal: read from the job
/// as fetched, never from what server middleware changed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PeriodicRef {
    /// The schedule's name.
    pub(crate) name: String,
    /// The tick, epoch milliseconds.
    pub(crate) tick: i64,
    /// Enqueued by hand.
    pub(crate) manual: bool,
    /// The job's jid (the run's record names it).
    pub(crate) jid: String,
}

impl PeriodicRef {
    /// The schedule of `job` (as fetched), if a schedule enqueued it.
    pub(crate) fn from_payload(job: &JobPayload) -> Option<Self> {
        let meta = job.periodic.as_ref()?;
        Some(Self {
            name: meta.name.clone(),
            tick: meta.tick,
            manual: meta.manual,
            jid: job.jid.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn the_payload_field_round_trips() {
        let meta = PeriodicMeta::new("nightly", 1_730_426_400_000, false);
        assert_eq!(
            serde_json::to_value(&meta).unwrap(),
            json!({"name": "nightly", "tick": 1_730_426_400_000_i64})
        );
        let manual = PeriodicMeta::new("nightly", 5, true);
        let value = serde_json::to_value(&manual).unwrap();
        assert_eq!(value, json!({"name": "nightly", "tick": 5, "manual": true}));
        assert_eq!(
            serde_json::from_value::<PeriodicMeta>(value).unwrap(),
            manual
        );
        let tick = PeriodicTick::from(&meta);
        assert_eq!(tick.name, "nightly");
        assert_eq!(
            tick.tick,
            SystemTime::UNIX_EPOCH + Duration::from_millis(1_730_426_400_000)
        );
        assert!(!tick.manual);
    }

    #[test]
    fn statuses_and_results_have_names() {
        for status in [
            RunStatus::Succeeded,
            RunStatus::Failed,
            RunStatus::Interrupted,
            RunStatus::Rescheduled,
        ] {
            assert_eq!(RunStatus::parse(status.as_str()), Some(status));
        }
        assert_eq!(RunStatus::parse("later"), None);
        for result in [
            FireResult::Enqueued,
            FireResult::Scheduled,
            FireResult::Halted,
            FireResult::Skipped,
            FireResult::Duplicate,
            FireResult::Manual,
        ] {
            assert_eq!(FireResult::parse(result.as_str()), Some(result));
        }
        assert_eq!(FireResult::parse("x"), None);
        assert_eq!(CatchUp::default(), CatchUp::Latest);
        assert_eq!(CatchUp::All { max: 3 }.as_str(), "all");
    }

    #[test]
    fn references_come_from_the_job_as_fetched() {
        let mut job = JobPayload::new("Report", vec![]);
        assert_eq!(PeriodicRef::from_payload(&job), None);
        job.periodic = Some(PeriodicMeta::new("nightly", 42, true));
        let reference = PeriodicRef::from_payload(&job).unwrap();
        assert_eq!(
            (reference.name.as_str(), reference.tick, reference.manual),
            ("nightly", 42, true)
        );
    }
}
