//! Rate limiting and throttling (the design document: [ratelimit.md]): limiters used inside
//! jobs and anywhere with Redis, and throttles that workers apply to job starts. Every process
//! sharing a Redis shares the limits; decisions are made by Lua scripts on Redis's clock.
//!
//! **Limiters** ([`Limiter`]) protect a resource from every process: at most N holders at a
//! time ([`Limiter::concurrent`], leases renewed while held), at most N per sliding period
//! ([`Limiter::window`]), per calendar interval ([`Limiter::bucket`]), a leaky bucket
//! ([`Limiter::leaky`]: bursts, then a steady drip), a points budget with costs corrected
//! afterwards ([`Limiter::points`]), or several of them at once ([`Limiter::all`], all or
//! none). A refusal is [`Error::OverLimit`] with the time until the
//! caller's slot; returned from a job (`?`), it **reschedules** the job at that slot instead of
//! failing it, without using a retry, up to 20 times (then the retry system takes over).
//!
//! **Throttles** ([`Throttle`]) are limits on job starts, per job type and optionally per key
//! (tenant, account): a worker checks a job's throttle right after fetching it; a job over it
//! does not run and is parked in the schedule at its slot in the throttle's wait line, so that
//! it neither holds a processor nor blocks the jobs behind it.
//!
//! **Queue limits** (`HenchBuilder::queue_limits`, `Api::queue(..).set_limits`) apply to every
//! fetch of a queue, whatever its jobs: concurrency, window, bucket and leaky limits, checked
//! and charged by the fetch itself. A queue over its limits is skipped like a paused one (its
//! jobs stay in it, in order) while the other queues keep flowing. Their state lives under the
//! reserved name `@<queue>` (`c:@exports`), next to the limiters'.
//!
//! Limits are written in a spec grammar (`concurrency=10`, `window=100/1s`, `bucket=10000/day`,
//! `leaky=60/1m`, `points=1000/1m`, with `per key` for a throttle's per-key limits; see
//! [`Limit`]), used by the configuration file, the dashboard and `#[hench::job(throttle(..))]`.
//!
//! The state of one limit lives under its identity `<kind>:<name>[:<key>]` (kinds `c`
//! concurrency, `w` window, `b` bucket, `g` leaky and points) in `limit:<identity>`, with its
//! counters in `limit-stats:<identity>`; every key expires once it carries no information.
//! [`Limits`] (`Api::limits`) lists the identities in use with their counters, holders and wait
//! lines, and resets them.
//!
//! [ratelimit.md]: https://github.com/rkwap/hench/blob/v0.1.0/docs/design/ratelimit.md

// The `Limits` API, in the API's directory (`api/limits.rs`) but compiled here, with the
// feature it reads.
#[path = "../api/limits.rs"]
pub(crate) mod api;
pub(crate) mod engine;
mod limiter;
pub(crate) mod lua;
pub(crate) mod memory;
mod permit;
pub(crate) mod queue;
pub(crate) mod reschedule;
pub(crate) mod rule;
#[cfg(all(test, feature = "test-util"))]
mod script_tests;
pub(crate) mod scripts;
pub(crate) mod spec;
mod target;
#[cfg(test)]
pub(crate) mod test_logs;
pub mod testing;
pub(crate) mod throttle;
#[cfg(feature = "cli")]
pub(crate) mod toml;
mod types;
pub(crate) mod wakeups;

use crate::error::{Error, Result};
use crate::payload::JobPayload;

pub use self::api::{Holder, LimitEvents, LimitInfo, Limits};
pub(crate) use self::engine::HeldPermits;
pub use self::limiter::Limiter;
pub use self::permit::Permit;
pub use self::target::LimitTarget;
pub(crate) use self::throttle::Throttles;
pub use self::types::{
    Backoff, BackoffInput, Interval, Limit, LimitKind, LimitOrder, Limited, OverLimit, Scope,
    Throttle, ThrottleOption, ThrottleRef, Usage,
};
pub(crate) use self::types::{OverLimitDecision, Reschedule};

/// The version of the stored limiter format this process understands, for the heartbeat's
/// `features` map (older workers treat an [`OverLimit`] as a failure; the dashboard names them).
pub(crate) const FORMAT: Option<u32> = Some(1);

/// The version of queue limits this process enforces, for the heartbeat's `features` map under
/// `queue_limits`: hench 0.1 advertises `limits` but ignores queue limits, so the dashboard
/// names the processes without this key while some queue has limits.
pub(crate) const QUEUE_FORMAT: Option<u32> = Some(1);

/// The throttle step of `Client::normalize` (`docs/design/ratelimit.md` §2.4.4): the payload's
/// `throttle` from the merged options. A throttle without a name is refused (a key alone means
/// nothing), and so are invalid names; keys come from data and are never refused (one that
/// cannot be used as it is, empty or with NUL, gets a hashed stored form);
/// [`ThrottleOption::Disabled`] (an option layer turning a lower layer's throttle off) is not
/// stored.
pub(crate) fn normalize(job: &mut JobPayload) -> Result<()> {
    match job.throttle.take() {
        None | Some(ThrottleOption::Disabled) => Ok(()),
        Some(ThrottleOption::Enabled { name: None, .. }) => Err(Error::InvalidJob(
            "a throttle key needs a throttle: name it with `throttle(\"name\")`".to_owned(),
        )),
        Some(ThrottleOption::Enabled {
            name: Some(name),
            key,
        }) => {
            rule::check_name_of("throttle", &name).map_err(|error| match error {
                Error::Config(reason) => Error::InvalidJob(reason),
                other => other,
            })?;
            job.throttle = Some(ThrottleOption::Enabled {
                name: Some(name),
                key,
            });
            Ok(())
        }
    }
}

/// Checks the throttles and queue limits of a configuration (`Error::Config` naming the first
/// invalid one): for the validation of `HenchBuilder::build` (`pro::validate`).
pub(crate) fn validate(config: &crate::config::Config) -> Result<()> {
    throttle::validate_throttles(&config.throttles, config.heartbeat_interval)?;
    for (queue, limits) in &config.queue_limits {
        queue::check(queue, limits)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalization_keeps_named_throttles() {
        let mut job = JobPayload::new("Charge", vec![]);
        job.throttle = Some(ThrottleOption::Enabled {
            name: Some("stripe".to_owned()),
            key: Some("acme corp".to_owned()),
        });
        normalize(&mut job).unwrap();
        assert_eq!(
            job.throttle.as_ref().and_then(ThrottleOption::name),
            Some("stripe")
        );
        assert_eq!(
            job.throttle.as_ref().and_then(ThrottleOption::key),
            Some("acme corp")
        );
        // A layer turning the throttle off leaves no field.
        job.throttle = Some(ThrottleOption::Disabled);
        normalize(&mut job).unwrap();
        assert_eq!(job.throttle, None);
        normalize(&mut job).unwrap();
        assert_eq!(job.throttle, None);
    }

    #[test]
    fn normalization_refuses_what_no_worker_could_use() {
        for (option, reason) in [
            (
                ThrottleOption::Enabled {
                    name: None,
                    key: Some("42".to_owned()),
                },
                "needs a throttle",
            ),
            (ThrottleOption::named("a b"), "invalid throttle name `a b`"),
        ] {
            let mut job = JobPayload::new("Charge", vec![]);
            job.throttle = Some(option);
            let error = normalize(&mut job).unwrap_err();
            assert!(
                matches!(&error, Error::InvalidJob(message) if message.contains(reason)),
                "{error}"
            );
        }
    }

    #[test]
    fn keys_from_data_are_never_refused() {
        // An empty key, or one with NUL, computed from a job's arguments: the job is pushed
        // (its key is stored hashed by the throttle's identities).
        for key in ["", "a\0b"] {
            let mut job = JobPayload::new("Charge", vec![]);
            job.throttle = Some(ThrottleOption::Enabled {
                name: Some("stripe".to_owned()),
                key: Some(key.to_owned()),
            });
            normalize(&mut job).unwrap();
            assert_eq!(
                job.throttle.as_ref().and_then(ThrottleOption::key),
                Some(key)
            );
        }
    }

    #[test]
    fn the_format_is_advertised() {
        assert_eq!(FORMAT, Some(1));
    }
}
