//! Periodic (cron) jobs: named timetables that enqueue a job at each tick, Sidekiq Enterprise's
//! periodic jobs (the design document: [periodic.md]).
//!
//! Schedules are defined in code with [`Periodic`] (cargo feature `periodic`, on by default) and
//! registered with [`HenchBuilder::periodic`](crate::HenchBuilder::periodic), or through the
//! API. A job enqueued by a schedule carries the tick it is for ([`PeriodicTick`]).
//!
//! Every worker process runs a poller (no leader): each tick's job is enqueued at most once,
//! by a compare-and-set in Redis, and at least once as long as some poller runs, with a
//! catch-up policy for ticks missed while none could ([`CatchUp`]). Ticks are decided with
//! Redis's clock, in the schedule's IANA time zone (UTC by default), with Vixie cron's
//! daylight saving rules. A process registers its crontab (the schedules given to its builder)
//! in its group at start: created, updated, and pruned when removed from the code.
//!
//! [periodic.md]: https://github.com/rkwap/hench/blob/v0.1.0/docs/design/periodic.md

#[path = "../api/periodic.rs"]
mod api;
#[cfg(feature = "periodic")]
mod builder;
#[cfg(feature = "periodic")]
mod clock;
#[cfg(feature = "periodic")]
pub(crate) mod definition;
pub(crate) mod lua;
#[cfg(feature = "periodic")]
pub(crate) mod plan;
pub(crate) mod poller;
pub(crate) mod record_run;
#[cfg(feature = "periodic")]
pub(crate) mod store;
#[cfg(feature = "periodic")]
pub(crate) mod timing;
#[cfg(feature = "cli")]
pub(crate) mod toml;
mod types;

#[cfg(feature = "periodic")]
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(feature = "periodic")]
pub use self::api::{
    CrontabGroup, HistoryEntry, LastEnqueue, LastRun, PeriodicEntry, PeriodicSet, PeriodicSummary,
};
#[cfg(feature = "periodic")]
pub use self::builder::Periodic;
#[cfg(all(feature = "periodic", feature = "test-util"))]
pub use self::clock::ManualClock;
pub(crate) use self::types::PeriodicRef;
pub use self::types::{
    CatchUp, FireResult, PeriodicMeta, PeriodicSource, PeriodicTick, RunStatus, Timetable, Upserted,
};

use crate::config::Config;
#[cfg(feature = "periodic")]
use crate::error::Error;
use crate::error::Result;
use crate::job::JobContext;

/// The version of the stored definition format (`spec.v`) this process understands, for the
/// heartbeat's `features` map; `None` without the `periodic` cargo feature (such a process
/// records the runs of periodic jobs but fires none).
#[cfg(feature = "periodic")]
pub(crate) const FORMAT: Option<u32> = Some(1);

/// The version of the stored definition format (`spec.v`) this process understands, for the
/// heartbeat's `features` map; `None` without the `periodic` cargo feature (such a process
/// records the runs of periodic jobs but fires none).
#[cfg(not(feature = "periodic"))]
pub(crate) const FORMAT: Option<u32> = None;

/// The group of a process that names none
/// ([`HenchBuilder::periodic_group`](crate::HenchBuilder::periodic_group)).
pub const DEFAULT_GROUP: &str = "default";

/// Checks the schedules of `config` at `build` (`docs/design/periodic.md` §2.3.2): each
/// definition ([`Periodic::validate`]), unique names, at most 10 000 schedules. The group, the
/// intervals and the lease are checked with the other settings. A schedule whose job type this
/// process does not know is not an error (it may be another service's job).
pub(crate) fn validate_all(config: &Config) -> Result<()> {
    #[cfg(feature = "periodic")]
    {
        use std::collections::HashSet;

        if config.periodic.len() > definition::MAX_CRONTAB {
            return Err(Error::Config(format!(
                "{} periodic jobs: a process registers at most {}",
                config.periodic.len(),
                definition::MAX_CRONTAB
            )));
        }
        let mut names = HashSet::new();
        for periodic in &config.periodic {
            periodic.validate().map_err(|error| match error {
                Error::InvalidSchedule(reason) => Error::Config(format!("periodic job {reason}")),
                other => other,
            })?;
            if !names.insert(periodic.name()) {
                return Err(Error::Config(format!(
                    "duplicate periodic job name `{}`",
                    periodic.name().escape_debug()
                )));
            }
        }
    }
    #[cfg(not(feature = "periodic"))]
    let _ = config;
    Ok(())
}

/// Applies to the schedules of the worker's file (`[[periodic]]` tables, which name their job
/// by class) the job type this process registers for their class, as `Periodic::job` does for
/// code: its options (queue, retries, uniqueness, throttle) under the table's keys, and its
/// typed unique and throttle keys. Called by `build` once the registry is complete; each
/// schedule is checked again. A class this process does not register keeps the table's keys
/// alone (the poller warns about it).
pub(crate) fn apply_registered_jobs(
    config: &mut Config,
    registry: &crate::job::Registry,
) -> Result<()> {
    #[cfg(feature = "periodic")]
    for periodic in &mut config.periodic {
        if !periodic.is_defined_in_file() {
            continue;
        }
        let Some(job) = registry.get(periodic.class_name()) else {
            continue;
        };
        periodic.apply_job_type(job.as_ref()).map_err(|reason| {
            Error::Config(format!(
                "periodic job `{}`: {reason}",
                periodic.name().escape_debug()
            ))
        })?;
        periodic.validate().map_err(|error| match error {
            Error::InvalidSchedule(reason) => Error::Config(format!(
                "periodic job {reason} (with the options of the job type `{}`)",
                periodic.class_name()
            )),
            other => other,
        })?;
    }
    #[cfg(not(feature = "periodic"))]
    let _ = (config, registry);
    Ok(())
}

/// The periodic part of a process's heartbeat `info` (`docs/design/periodic.md` §2.4):
/// `{"poller": bool, "group": name or null, "digest": crontab digest or null, "schedules":
/// count}`, from which the dashboard tells whether a live process polls and whether the
/// processes of a group declare the same crontab. `None` without the `periodic` cargo feature.
///
/// Computed by the heartbeat once per process start (a crontab's digest builds every
/// definition).
pub(crate) fn info(config: &Config) -> Option<serde_json::Value> {
    #[cfg(feature = "periodic")]
    {
        let registers = poller::registers(config);
        let schedules: Vec<(String, String)> = config
            .periodic
            .iter()
            .filter_map(|periodic| {
                let definition = periodic.definition().ok()?;
                Some((definition.name, definition.digest))
            })
            .collect();
        Some(serde_json::json!({
            "poller": config.periodic_poller,
            "group": registers.then(|| poller::group(config)),
            "digest": registers.then(|| {
                definition::crontab_digest(
                    schedules
                        .iter()
                        .map(|(name, digest)| (name.as_str(), digest.as_str())),
                )
            }),
            "schedules": config.periodic.len(),
        }))
    }
    #[cfg(not(feature = "periodic"))]
    {
        let _ = config;
        None
    }
}

/// Epoch milliseconds of `time` (negative before 1970, saturating).
#[cfg(feature = "periodic")]
pub(crate) fn epoch_millis(time: SystemTime) -> i64 {
    match time.duration_since(UNIX_EPOCH) {
        Ok(elapsed) => i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX),
        Err(before) => -i64::try_from(before.duration().as_millis()).unwrap_or(i64::MAX),
    }
}

/// Whole epoch seconds of `time`, rounded down.
#[cfg(feature = "periodic")]
pub(crate) fn epoch_secs(time: SystemTime) -> i64 {
    epoch_millis(time).div_euclid(1_000)
}

/// The instant of a tick (epoch seconds).
#[cfg(feature = "periodic")]
pub(crate) fn tick_time(tick: i64) -> SystemTime {
    let magnitude = Duration::from_secs(tick.unsigned_abs());
    if tick >= 0 {
        UNIX_EPOCH.checked_add(magnitude).unwrap_or(UNIX_EPOCH)
    } else {
        UNIX_EPOCH.checked_sub(magnitude).unwrap_or(UNIX_EPOCH)
    }
}

impl JobContext {
    /// The periodic schedule and tick that enqueued this job, if a schedule did (the job as
    /// fetched: retries, recoveries and interruptions keep it).
    ///
    /// ```no_run
    /// # use hench::prelude::*;
    /// # async fn perform(ctx: &JobContext) {
    /// if let Some(tick) = ctx.periodic() {
    ///     // Report up to the tick, not up to when the job happens to run.
    ///     let _covers_until = tick.tick;
    /// }
    /// # }
    /// ```
    pub fn periodic(&self) -> Option<PeriodicTick> {
        self.payload().periodic.as_ref().map(PeriodicTick::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "periodic")]
    #[test]
    fn instants_convert_both_ways() {
        let tick = 1_800_000_000;
        assert_eq!(epoch_secs(tick_time(tick)), tick);
        assert_eq!(epoch_millis(tick_time(tick)), tick * 1_000);
        let before = UNIX_EPOCH - Duration::from_millis(1_500);
        assert_eq!(epoch_millis(before), -1_500);
        assert_eq!(epoch_secs(before), -2);
        assert_eq!(epoch_secs(tick_time(-5)), -5);
    }

    #[cfg(feature = "periodic")]
    #[test]
    fn builds_refuse_invalid_and_duplicate_schedules() {
        use crate::Hench;

        let job = |name: &str| Periodic::cron(name, "0 * * * *").class("Hourly", vec![]);
        let invalid = Hench::builder()
            .periodic(Periodic::cron("broken", "0 0 30 2 *").class("Hourly", vec![]))
            .config_for_test();
        let error = invalid.unwrap_err().to_string();
        assert!(error.contains("periodic job `broken`"), "{error}");
        assert!(error.contains("never ticks"), "{error}");
        let duplicate = Hench::builder()
            .periodic(job("same"))
            .periodic(job("same"))
            .config_for_test();
        assert!(
            duplicate
                .unwrap_err()
                .to_string()
                .contains("duplicate periodic job name `same`")
        );
        assert!(
            Hench::builder()
                .periodic(job("a"))
                .periodic(job("b"))
                .config_for_test()
                .is_ok()
        );
    }

    #[cfg(feature = "periodic")]
    #[test]
    fn the_heartbeat_tells_the_poller_the_group_and_the_crontab() {
        use crate::Hench;
        use std::sync::Arc;

        let job = |name: &str| Periodic::cron(name, "0 * * * *").class("Hourly", vec![]);
        let config = Arc::new(
            Hench::builder()
                .periodic(job("a"))
                .periodic(job("b"))
                .config_for_test()
                .unwrap(),
        );
        let info = info(&config).unwrap();
        assert_eq!(info["poller"], true);
        assert_eq!(info["group"], "default");
        assert_eq!(info["schedules"], 2);
        let digest = info["digest"].as_str().unwrap().to_owned();
        assert_eq!(digest.len(), 43);
        // The same crontab in another order: the same digest.
        let reordered = Arc::new(
            Hench::builder()
                .periodic(job("b"))
                .periodic(job("a"))
                .config_for_test()
                .unwrap(),
        );
        assert_eq!(info_digest(&reordered), digest);
        // No crontab and no group: nothing registered.
        let none = Arc::new(
            Hench::builder()
                .periodic_poller(false)
                .config_for_test()
                .unwrap(),
        );
        let info = super::info(&none).unwrap();
        assert_eq!(info["poller"], false);
        assert!(info["group"].is_null() && info["digest"].is_null());
    }

    #[cfg(feature = "periodic")]
    fn info_digest(config: &std::sync::Arc<Config>) -> String {
        info(config).unwrap()["digest"].as_str().unwrap().to_owned()
    }
}
