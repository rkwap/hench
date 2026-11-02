//! `[throttles.<name>]` and `[queue_limits]` tables of the worker configuration file
//! (`docs/design/ratelimit.md` §2.17):
//!
//! ```toml
//! [throttles.stripe]
//! limits = ["concurrency=25", "window=5/1s per key"]
//! reschedules = 200   # optional; default unlimited
//! max_delay = 3600    # seconds; default 86400
//! max_wait = 0.5      # seconds; default 0
//! lease = 60          # seconds; default 60, at least three heartbeat intervals
//! fail_open = false
//! stats = true        # counters and wait line per identity; default true
//! stats_ttl = 86400   # seconds they stay after the last use; default 86400
//! ```
//!
//! [`parse_throttles`] reads and checks a `throttles` table; the file's settings carry the result
//! to the builder (throttles defined in the file replace same-named ones from code). Queue
//! limits come with phase 3: [`queue_limits`] refuses their table rather than accept limits
//! nothing would apply.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::Deserialize;

use crate::error::{Error, Result};
use crate::limit::spec::parse_specs;
use crate::limit::{Scope, Throttle};

/// Reads the `queue_limits` table of `path`.
pub(crate) fn queue_limits(path: &str, _value: &toml::Value) -> Result<()> {
    Err(Error::Config(format!(
        "{path}: `[queue_limits]` are not supported by this version of hench yet"
    )))
}

/// One `[throttles.<name>]` table.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileThrottle {
    /// Specs (`"concurrency=25"`, `"window=5/1s per key"`), one per entry or joined by `;`.
    limits: Vec<String>,
    reschedules: Option<i64>,
    max_delay: Option<f64>,
    max_wait: Option<f64>,
    lease: Option<f64>,
    fail_open: Option<bool>,
    stats: Option<bool>,
    stats_ttl: Option<f64>,
}

/// The throttles of a `throttles` table at `path` (the section's name, for errors), each
/// checked: unknown keys, malformed specs, invalid numbers and names are `Error::Config` naming
/// the throttle and the key.
pub(crate) fn parse_throttles(path: &str, value: &toml::Value) -> Result<Vec<Throttle>> {
    let tables: BTreeMap<String, FileThrottle> =
        value.clone().try_into().map_err(|error: toml::de::Error| {
            Error::Config(format!("{path}: {}", error.to_string().trim_end()))
        })?;
    tables
        .into_iter()
        .map(|(name, file)| read_throttle(&format!("{path}.{name}"), name, file))
        .collect()
}

/// The throttle `name` of the table at `at`.
fn read_throttle(at: &str, name: String, file: FileThrottle) -> Result<Throttle> {
    let invalid = |key: &str, reason: &str| Error::Config(format!("{at}.{key}: {reason}"));
    if file.limits.is_empty() {
        return Err(invalid("limits", "a throttle needs at least one limit"));
    }
    let mut throttle = Throttle::new(name);
    for text in &file.limits {
        let specs = parse_specs(text).map_err(|error| invalid("limits", &reason(&error)))?;
        for (limit, scope) in specs {
            throttle = match scope {
                Scope::Global => throttle.limit(limit),
                Scope::PerKey => throttle.limit_per_key(limit),
            };
        }
    }
    if let Some(reschedules) = file.reschedules {
        let reschedules = u32::try_from(reschedules)
            .map_err(|_| invalid("reschedules", "a number from 0 to 4294967295"))?;
        throttle = throttle.reschedules(Some(reschedules));
    }
    if let Some(seconds) = file.max_delay {
        let delay = seconds_of(seconds)
            .ok_or_else(|| invalid("max_delay", "a positive number of seconds"))?;
        throttle = throttle.max_delay(delay);
    }
    if let Some(seconds) = file.max_wait {
        let wait = if seconds == 0.0 {
            Some(Duration::ZERO)
        } else {
            seconds_of(seconds)
        };
        let wait =
            wait.ok_or_else(|| invalid("max_wait", "a number of seconds (0: never wait)"))?;
        throttle = throttle.max_wait(wait);
    }
    if let Some(seconds) = file.lease {
        let lease =
            seconds_of(seconds).ok_or_else(|| invalid("lease", "a positive number of seconds"))?;
        throttle = throttle.lease(lease);
    }
    if let Some(on) = file.fail_open {
        throttle = throttle.fail_open(on);
    }
    if let Some(on) = file.stats {
        throttle = throttle.stats(on);
    }
    if let Some(seconds) = file.stats_ttl {
        let ttl = seconds_of(seconds)
            .ok_or_else(|| invalid("stats_ttl", "a positive number of seconds"))?;
        throttle = throttle.stats_ttl(ttl);
    }
    throttle
        .validate()
        .map_err(|error| Error::Config(format!("{at}: {}", reason(&error))))?;
    Ok(throttle)
}

/// The reason of a configuration error, without the "configuration error:" its `Display`
/// starts with (it is wrapped in another one).
fn reason(error: &Error) -> String {
    match error {
        Error::Config(reason) => reason.clone(),
        other => other.to_string(),
    }
}

/// A positive, finite number of seconds, in whole milliseconds.
fn seconds_of(seconds: f64) -> Option<Duration> {
    if !seconds.is_finite() || seconds <= 0.0 || seconds > 1e9 {
        return None;
    }
    Some(Duration::from_millis((seconds * 1_000.0).round() as u64)).filter(|d| !d.is_zero())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::limit::Limit;

    fn table(text: &str) -> toml::Value {
        let parsed: toml::Table = text.parse().unwrap();
        parsed["throttles"].clone()
    }

    #[test]
    fn throttles_are_read_with_every_setting() {
        let throttles = parse_throttles(
            "throttles",
            &table(
                r#"
                [throttles.stripe]
                limits = ["concurrency=25", "window=5/1s per key"]
                reschedules = 200
                max_delay = 3600
                max_wait = 0.5
                lease = 90
                fail_open = true
                stats = false
                stats_ttl = 3600

                [throttles.erp]
                limits = ["c=3; leaky=60/1m per key"]
                "#,
            ),
        )
        .unwrap();
        assert_eq!(throttles.len(), 2);
        let erp = &throttles[0];
        assert_eq!(erp.name(), "erp");
        assert_eq!(
            erp.limits(),
            [
                (Limit::Concurrency { limit: 3 }, Scope::Global),
                (
                    Limit::Leaky {
                        size: 60,
                        drain: Duration::from_secs(60)
                    },
                    Scope::PerKey
                )
            ]
        );
        assert_eq!(erp.reschedule_budget(), None);
        assert!(erp.has_stats());
        assert_eq!(erp.stats_ttl_duration(), Throttle::STATS_TTL);
        let stripe = &throttles[1];
        assert_eq!(stripe.spec(), "c=25;w=5/1000 per key");
        assert_eq!(stripe.reschedule_budget(), Some(200));
        assert_eq!(stripe.max_delay_duration(), Duration::from_secs(3_600));
        assert_eq!(stripe.max_wait_duration(), Duration::from_millis(500));
        assert_eq!(stripe.lease_duration(), Duration::from_secs(90));
        assert!(stripe.is_fail_open());
        assert!(!stripe.has_stats());
        assert_eq!(stripe.stats_ttl_duration(), Duration::from_secs(3_600));
    }

    #[test]
    fn mistakes_name_the_throttle_and_the_key() {
        for (text, expected) in [
            ("[throttles.x]\nlimits = []", "throttles.x.limits"),
            ("[throttles.x]\nlimits = [\"c=0\"]", "throttles.x.limits"),
            ("[throttles.x]\nlimits = [\"rate=5\"]", "unknown kind"),
            (
                "[throttles.x]\nlimits = [\"c=1\"]\nreschedules = -1",
                "throttles.x.reschedules",
            ),
            (
                "[throttles.x]\nlimits = [\"c=1\"]\nmax_delay = 0",
                "throttles.x.max_delay",
            ),
            (
                "[throttles.x]\nlimits = [\"c=1\"]\nmax_wait = -1",
                "throttles.x.max_wait",
            ),
            ("[throttles.x]\nlimits = [\"c=1\"]\nlease = 0.1", "lease"),
            (
                "[throttles.x]\nlimits = [\"c=1\"]\nleese = 1",
                "unknown field",
            ),
            (
                "[throttles.\"a b\"]\nlimits = [\"c=1\"]",
                "invalid throttle name `a b`",
            ),
            (
                "[throttles.x]\nlimits = [\"c=1\"]\nstats_ttl = 0",
                "throttles.x.stats_ttl",
            ),
            ("[throttles.x]\nlimits = \"c=1\"", "throttles"),
        ] {
            let error = parse_throttles("throttles", &table(text))
                .unwrap_err()
                .to_string();
            assert!(error.contains(expected), "{text}: {error}");
            // One prefix, not one per layer that wrapped the reason.
            assert_eq!(
                error.matches("configuration error").count(),
                1,
                "{text}: {error}"
            );
        }
    }

    #[test]
    fn queue_limits_are_refused_until_they_land() {
        let parsed: toml::Table = "[queue_limits]\nreports = \"c=1\"".parse().unwrap();
        let error = queue_limits("queue_limits", &parsed["queue_limits"]).unwrap_err();
        assert!(error.to_string().contains("not supported"), "{error}");
    }
}
