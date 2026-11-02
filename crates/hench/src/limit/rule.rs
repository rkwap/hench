//! Limit identities and the rules the scripts take (`docs/design/ratelimit.md` §2.2, §2.8).
//!
//! The state of one limit lives under its identity `<kind>:<id>`: the kind's letter (`c`
//! concurrency, `w` window, `b` bucket, `g` leaky and points) and the limiter's id, its name and
//! `:key` for a keyed instance. Names are 1 to 100 characters of `[A-Za-z0-9_.-]` other than `.`
//! and `..` (`~` and `@` are reserved for generated names: a throttle's second limit of one kind
//! and scope is `<name>~2`, queue limits are `@<queue>`); keys are any text without NUL, those
//! longer than 128 bytes replaced by `#` and 22 characters of their SHA-256 (base64url).

use std::collections::HashSet;
use std::time::Duration;

use crate::error::{Error, Result};
use crate::keys;
use crate::limit::{Interval, Limit, LimitKind};

/// Longest limiter name, in characters.
pub(crate) const MAX_NAME_CHARS: usize = 100;

/// Longest key kept as is, in bytes; longer keys are hashed.
pub(crate) const MAX_KEY_BYTES: usize = 128;

/// Largest window limit: a window keeps one entry per unit (about 10 bytes each) and checks one
/// deep in its list, so larger limits belong to leaky or bucket limits (O(1) state).
pub(crate) const MAX_WINDOW_LIMIT: u32 = 100_000;

/// Longest period of a limit (1000 days): times stay exact in Lua's doubles.
pub(crate) const MAX_PERIOD: Duration = Duration::from_secs(1_000 * 86_400);

/// Shortest and longest lease of a concurrency permit.
pub(crate) const MIN_LEASE: Duration = Duration::from_secs(1);
pub(crate) const MAX_LEASE: Duration = Duration::from_secs(86_400);

/// Shortest and longest expiry of a limiter's counters after its last use.
pub(crate) const MIN_STATS_TTL: Duration = Duration::from_secs(1);
pub(crate) const MAX_STATS_TTL: Duration = Duration::from_secs(365 * 86_400);

/// Checks a limiter name.
pub(crate) fn check_name(name: &str) -> Result<()> {
    check_name_of("limiter", name)
}

/// Checks the name of a `what` (a limiter, a throttle): 1 to 100 letters, digits, `_`, `.` and
/// `-`, but not `.` or `..` (the dashboard and the JSON API name a limit in URL paths, where
/// those are dot segments that browsers and HTTP clients resolve away).
pub(crate) fn check_name_of(what: &str, name: &str) -> Result<()> {
    if matches!(name, "." | "..") {
        return Err(Error::Config(format!(
            "invalid {what} name `{name}`: `.` and `..` are dot segments in URL paths, where \
             the dashboard and the JSON API name limits"
        )));
    }
    let count = name.chars().count();
    if count == 0 || count > MAX_NAME_CHARS {
        return Err(Error::Config(format!(
            "invalid {what} name `{}`: 1 to {MAX_NAME_CHARS} characters",
            name.escape_debug()
        )));
    }
    if let Some(bad) = name
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-')))
    {
        return Err(Error::Config(format!(
            "invalid {what} name `{}`: `{}` is not allowed (letters, digits, `_`, `.` and `-`)",
            name.escape_debug(),
            bad.escape_debug()
        )));
    }
    Ok(())
}

/// The key (a tenant, an account) as identities hold it: as is when it is 1 to 128 bytes
/// without NUL, else (longer, empty, or with NUL) `#` and the first 22 characters of its SHA-256
/// (132 bits). Keys usually come from data (a job's arguments), so none is refused.
pub(crate) fn stored_key(key: &str) -> String {
    if key.is_empty() || key.len() > MAX_KEY_BYTES || key.contains('\0') {
        let digest = crate::canonical::sha256_b64url(key.as_bytes());
        format!("#{}", &digest[..22])
    } else {
        key.to_owned()
    }
}

/// The id of a limiter instance: its name, and `:key` when keyed.
pub(crate) fn id(name: &str, key: Option<&str>) -> String {
    match key {
        Some(key) => format!("{name}:{key}"),
        None => name.to_owned(),
    }
}

/// A period in whole milliseconds, or why it is not one.
fn period_ms(what: &str, period: Duration) -> Result<u64> {
    if period.is_zero() || period > MAX_PERIOD || !period.subsec_nanos().is_multiple_of(1_000_000) {
        return Err(Error::Config(format!(
            "invalid {what} {period:?}: a positive whole number of milliseconds, at most 1000 days"
        )));
    }
    u64::try_from(period.as_millis())
        .map_err(|_| Error::Config(format!("invalid {what} {period:?}")))
}

/// Checks the parameters of a limit.
pub(crate) fn check_limit(limit: &Limit) -> Result<()> {
    if limit.capacity() == 0 {
        return Err(Error::Config(format!(
            "invalid limit `{limit}`: its count must be at least 1 (pause a queue to stop its jobs)"
        )));
    }
    match *limit {
        Limit::Concurrency { .. } => {}
        Limit::Window {
            limit: count,
            period,
        } => {
            period_ms("window", period)?;
            if count > MAX_WINDOW_LIMIT {
                return Err(Error::Config(format!(
                    "invalid limit `{limit}`: a window keeps one entry per grant; above \
                     {MAX_WINDOW_LIMIT} use a leaky or bucket limit"
                )));
            }
        }
        Limit::Bucket { interval, .. } => {
            period_ms("bucket interval", interval.duration())?;
        }
        Limit::Leaky { drain, .. } => {
            period_ms("drain time", drain)?;
        }
        Limit::Points { refill, .. } => {
            period_ms("refill time", refill)?;
        }
    }
    Ok(())
}

/// Checks a concurrency lease.
pub(crate) fn check_lease(lease: Duration) -> Result<()> {
    if lease < MIN_LEASE || lease > MAX_LEASE {
        return Err(Error::Config(format!(
            "invalid lease {lease:?}: from 1 s to 1 day"
        )));
    }
    period_ms("lease", lease).map(|_| ())
}

/// Checks the expiry of the counters.
pub(crate) fn check_stats_ttl(ttl: Duration) -> Result<()> {
    if ttl < MIN_STATS_TTL || ttl > MAX_STATS_TTL {
        return Err(Error::Config(format!(
            "invalid stats TTL {ttl:?}: from 1 s to 365 days"
        )));
    }
    Ok(())
}

/// The period of a limit in ms (`lease` for concurrency).
pub(crate) fn period_of(limit: &Limit, lease: Duration) -> u64 {
    let period = match *limit {
        Limit::Concurrency { .. } => lease,
        Limit::Window { period, .. } => period,
        Limit::Bucket { interval, .. } => interval.duration(),
        Limit::Leaky { drain, .. } => drain,
        Limit::Points { refill, .. } => refill,
    };
    u64::try_from(period.as_millis()).unwrap_or(u64::MAX)
}

/// The signature of a limit, kept in its stats (`c|25`, `w|100|1000`, `b|10|86400000`,
/// `l|60|60000`, `p|1000|20000`): the spec letter, the count and the period in ms.
pub(crate) fn signature(limit: &Limit) -> String {
    let (letter, period) = match *limit {
        Limit::Concurrency { limit } => return format!("c|{limit}"),
        Limit::Window { period, .. } => ('w', period),
        Limit::Bucket { interval, .. } => ('b', interval.duration()),
        Limit::Leaky { drain, .. } => ('l', drain),
        Limit::Points { refill, .. } => ('p', refill),
    };
    format!("{letter}|{}|{}", limit.capacity(), period.as_millis())
}

/// The limit a signature describes (`None` for one this version does not know).
pub(crate) fn parse_signature(signature: &str) -> Option<Limit> {
    let mut parts = signature.split('|');
    let letter = parts.next()?;
    let count: u32 = parts.next()?.parse().ok()?;
    let period = match parts.next() {
        Some(ms) => Some(Duration::from_millis(ms.parse().ok()?)),
        None => None,
    };
    if parts.next().is_some() || count == 0 {
        return None;
    }
    let limit = match (letter, period) {
        ("c", None) => Limit::Concurrency { limit: count },
        ("w", Some(period)) => Limit::Window {
            limit: count,
            period,
        },
        ("b", Some(period)) => Limit::Bucket {
            limit: count,
            interval: match period.as_millis() {
                1_000 => Interval::Second,
                60_000 => Interval::Minute,
                3_600_000 => Interval::Hour,
                86_400_000 => Interval::Day,
                _ => Interval::Every(period),
            },
        },
        ("l", Some(drain)) => Limit::Leaky { size: count, drain },
        ("p", Some(refill)) => Limit::Points {
            capacity: count,
            refill,
        },
        _ => return None,
    };
    check_limit(&limit).ok()?;
    Some(limit)
}

/// One limit of a call, with its identity: what the scripts and the in-memory backend take.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Rule {
    /// The limit.
    pub(crate) limit: Limit,
    /// The id (`stripe:42`).
    pub(crate) id: String,
    /// The name its per-minute counters use (the limiter's or throttle's name, without key).
    pub(crate) counter: String,
    /// `limit:<kind>:<id>`.
    pub(crate) state: String,
    /// `limit-stats:<kind>:<id>`.
    pub(crate) stats: String,
    /// `<kind>:<id>`: the index member, and the label of refusals.
    pub(crate) member: String,
}

impl Rule {
    /// The rule of `limit` for the limiter `id`, counted under `counter`.
    pub(crate) fn new(limit: Limit, id: String, counter: String) -> Self {
        let letter = limit.kind().letter();
        Self {
            state: keys::limit_state(letter, &id),
            stats: keys::limit_stats(letter, &id),
            member: format!("{letter}:{id}"),
            limit,
            id,
            counter,
        }
    }

    /// The letter of its state's kind.
    pub(crate) fn letter(&self) -> char {
        self.limit.kind().letter()
    }

    /// Whether it is a concurrency limit.
    pub(crate) fn is_concurrency(&self) -> bool {
        self.limit.kind() == LimitKind::Concurrency
    }

    /// Whether it is a points limit (whose cost can be corrected).
    pub(crate) fn is_points(&self) -> bool {
        matches!(self.limit, Limit::Points { .. })
    }

    /// Its signature (see [`signature`]).
    pub(crate) fn signature(&self) -> String {
        signature(&self.limit)
    }

    /// The spacing a concurrency limit's wait line uses until its releases measure how long
    /// permits are held: `hold` (the expected hold time) shared by its holders, at least 1 ms.
    pub(crate) fn default_spacing_ms(&self, hold: Duration) -> u64 {
        let hold = u64::try_from(hold.as_millis()).unwrap_or(u64::MAX);
        (hold / u64::from(self.limit.capacity().max(1))).max(1)
    }
}

/// Checks that the identities of a call's rules differ (two limits sharing one state would count
/// each grant twice).
pub(crate) fn check_distinct(rules: &[Rule]) -> Result<()> {
    let mut seen = HashSet::new();
    for rule in rules {
        if !seen.insert(rule.member.as_str()) {
            return Err(Error::Config(format!(
                "two limits share the identity `{}`: give them different names (leaky and \
                 points limits share one kind of state)",
                rule.member
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_follow_the_rules() {
        for name in [
            "stripe",
            "a",
            "erp-v2",
            "crm.hourly",
            "A_1",
            &"n".repeat(100),
        ] {
            check_name(name).unwrap();
        }
        for name in [
            "",
            "stripe:42",
            "a b",
            "x~2",
            "@q",
            "é",
            "a/b",
            &"n".repeat(101),
            // URL dot segments (round 3, web-api-security-7).
            ".",
            "..",
        ] {
            let error = check_name(name).unwrap_err();
            assert!(error.to_string().contains("invalid limiter name"), "{name}");
        }
        for name in ["...", ".x", "x.."] {
            check_name(name).unwrap();
        }
    }

    #[test]
    fn keys_are_free_text_hashed_beyond_128_bytes() {
        assert_eq!(stored_key("acme corp: 42 / é"), "acme corp: 42 / é");
        // Keys come from data: an empty key, or one with NUL, is a key too, stored hashed.
        for odd in ["", "a\0b"] {
            let stored = stored_key(odd);
            assert_eq!(stored.len(), 23, "{odd:?}");
            assert_eq!(
                stored,
                format!(
                    "#{}",
                    &crate::canonical::sha256_b64url(odd.as_bytes())[..22]
                )
            );
        }
        assert_ne!(stored_key(""), stored_key("a\0b"));
        assert_eq!(stored_key("42"), "42");
        let exact = "k".repeat(128);
        assert_eq!(stored_key(&exact), exact);
        let long = "k".repeat(129);
        let stored = stored_key(&long);
        assert_eq!(stored.len(), 23);
        assert!(stored.starts_with('#'));
        // Golden vector: SHA-256 of 129 `k`, base64url, first 22 characters.
        assert_eq!(
            stored,
            format!(
                "#{}",
                &crate::canonical::sha256_b64url(long.as_bytes())[..22]
            )
        );
        assert_eq!(id("stripe", Some("42")), "stripe:42");
        assert_eq!(id("stripe", None), "stripe");
    }

    /// Round 4 (fresh-user-5): a limit is named in the spec grammar, as the file's and the
    /// macro's errors name it, not as a Rust value.
    #[test]
    fn invalid_limits_are_named_in_the_spec_grammar() {
        let zero = check_limit(&Limit::Concurrency { limit: 0 })
            .unwrap_err()
            .to_string();
        assert!(
            zero.contains("invalid limit `concurrency=0`: its count must be at least 1"),
            "{zero}"
        );
        let window = check_limit(&Limit::Window {
            limit: 0,
            period: Duration::from_secs(1),
        })
        .unwrap_err()
        .to_string();
        assert!(window.contains("invalid limit `window=0/1s`"), "{window}");
        assert!(!window.contains('{'), "{window}");
        let huge = check_limit(&Limit::Window {
            limit: MAX_WINDOW_LIMIT + 1,
            period: Duration::from_secs(1),
        })
        .unwrap_err()
        .to_string();
        assert!(huge.contains("invalid limit `window=100001/1s`"), "{huge}");
    }

    #[test]
    fn limits_are_checked() {
        let ok = [
            Limit::Concurrency { limit: 1 },
            Limit::Window {
                limit: 100,
                period: Duration::from_millis(1),
            },
            Limit::Bucket {
                limit: 5,
                interval: Interval::Every(Duration::from_millis(1_500)),
            },
            Limit::Leaky {
                size: 60,
                drain: Duration::from_secs(60),
            },
            Limit::Points {
                capacity: 1_000,
                refill: Duration::from_secs(20),
            },
        ];
        for limit in ok {
            check_limit(&limit).unwrap();
        }
        let bad = [
            Limit::Concurrency { limit: 0 },
            Limit::Window {
                limit: 1,
                period: Duration::ZERO,
            },
            Limit::Window {
                limit: 1,
                period: Duration::from_micros(1_500),
            },
            Limit::Window {
                limit: MAX_WINDOW_LIMIT + 1,
                period: Duration::from_secs(1),
            },
            Limit::Bucket {
                limit: 1,
                interval: Interval::Every(Duration::from_micros(500)),
            },
            Limit::Leaky {
                size: 1,
                drain: MAX_PERIOD + Duration::from_millis(1),
            },
            Limit::Points {
                capacity: 0,
                refill: Duration::from_secs(1),
            },
        ];
        for limit in bad {
            assert!(check_limit(&limit).is_err(), "{limit:?}");
        }
        assert!(check_lease(Duration::from_millis(999)).is_err());
        assert!(check_lease(Duration::from_millis(1_500)).is_ok());
        assert!(check_lease(Duration::from_secs(86_401)).is_err());
        assert!(check_stats_ttl(Duration::ZERO).is_err());
        assert!(check_stats_ttl(Duration::from_secs(86_400)).is_ok());
    }

    #[test]
    fn signatures_round_trip() {
        for spec in [
            "c=25",
            "w=100/1000",
            "b=10000/day",
            "b=3/second",
            "b=3/1500ms",
            "l=60/1m",
            "p=1000/20s",
        ] {
            let limit: Limit = spec.parse().unwrap();
            assert_eq!(parse_signature(&signature(&limit)), Some(limit), "{spec}");
        }
        assert_eq!(signature(&"w=100/1s".parse().unwrap()), "w|100|1000");
        assert_eq!(signature(&"c=25".parse().unwrap()), "c|25");
        for bad in [
            "", "c", "c|0", "w|5", "c|5|10", "x|5|10", "w|5|10|1", "w|a|1",
        ] {
            assert_eq!(parse_signature(bad), None, "{bad}");
        }
    }

    #[test]
    fn rules_know_their_keys() {
        let rule = Rule::new(
            "w=100/1s".parse().unwrap(),
            "stripe:42".to_owned(),
            "stripe".to_owned(),
        );
        assert_eq!(rule.state, "limit:w:stripe:42");
        assert_eq!(rule.stats, "limit-stats:w:stripe:42");
        assert_eq!(rule.member, "w:stripe:42");
        assert_eq!(rule.letter(), 'w');
        assert!(!rule.is_concurrency());
        let points = Rule::new("p=10/1s".parse().unwrap(), "gql".into(), "gql".into());
        assert_eq!(points.member, "g:gql");
        assert!(points.is_points());
        let concurrency = Rule::new("c=3".parse().unwrap(), "erp".into(), "erp".into());
        assert_eq!(
            concurrency.default_spacing_ms(Duration::from_millis(1_000)),
            333
        );
        assert_eq!(concurrency.default_spacing_ms(Duration::from_millis(1)), 1);
        assert_eq!(
            period_of(&concurrency.limit, Duration::from_secs(30)),
            30_000
        );
        let leaky = Rule::new("l=60/1m".parse().unwrap(), "x".into(), "x".into());
        assert!(check_distinct(&[concurrency.clone(), points.clone()]).is_ok());
        let same_state = Rule::new("p=5/1s".parse().unwrap(), "x".into(), "x".into());
        assert!(check_distinct(&[leaky, same_state]).is_err());
    }
}
