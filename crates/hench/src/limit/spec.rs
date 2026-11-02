//! The spec grammar of limits (`docs/design/ratelimit.md` §2.17), shared by `Limit`'s
//! `FromStr`/`Display`, the TOML file, the CLI and the dashboard:
//!
//! ```text
//! specs   = spec *(";" spec)
//! spec    = kind "=" count ["/" period] [" per key"]
//! kind    = "concurrency" | "c" | "window" | "w" | "bucket" | "b" | "leaky" | "l"
//!         | "points" | "p"
//! count   = 1 to 4294967295
//! period  = duration (a bare number of seconds, or numbers with units ms, s, m, h, d, in a row:
//!           "500ms", "1s", "1h30m"), or for buckets "second", "minute", "hour", "day"
//! ```
//!
//! Concurrency takes no period, every other kind needs one; periods are positive whole numbers
//! of milliseconds. `Display` writes the long form with the largest units (`window=90/1m30s`);
//! [`Limit::normalized`] writes the short form Redis stores (`w=90/90000`).
//!
//! `hench-macros` parses the same grammar at compile time with its own copy
//! (`crates/hench-macros/src/spec.rs`); `crates/hench/tests/spec_parity.rs` feeds both one
//! corpus.

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use crate::error::{Error, Result};
use crate::limit::{Interval, Limit, Scope};

/// The suffix of a per-key spec.
const PER_KEY: &str = "per key";

impl FromStr for Limit {
    type Err = Error;

    /// Parses one spec (`window=100/1s`). A `per key` suffix is refused: scopes belong to
    /// throttles ([`Throttle::limit_per_key`](crate::limit::Throttle::limit_per_key)).
    fn from_str(text: &str) -> Result<Self> {
        match parse_spec(text)? {
            (limit, Scope::Global) => Ok(limit),
            (_, _) => Err(invalid(text, "`per key` applies to throttles only")),
        }
    }
}

impl fmt::Display for Limit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Limit::Concurrency { limit } => write!(f, "concurrency={limit}"),
            Limit::Window { limit, period } => {
                write!(f, "window={limit}/{}", DisplayDuration(period))
            }
            Limit::Bucket { limit, interval } => match interval {
                Interval::Second => write!(f, "bucket={limit}/second"),
                Interval::Minute => write!(f, "bucket={limit}/minute"),
                Interval::Hour => write!(f, "bucket={limit}/hour"),
                Interval::Day => write!(f, "bucket={limit}/day"),
                Interval::Every(every) => {
                    write!(f, "bucket={limit}/{}", DisplayDuration(every))
                }
            },
            Limit::Leaky { size, drain } => write!(f, "leaky={size}/{}", DisplayDuration(drain)),
            Limit::Points { capacity, refill } => {
                write!(f, "points={capacity}/{}", DisplayDuration(refill))
            }
        }
    }
}

impl Limit {
    /// The short form stored in Redis and advertised by the heartbeat: `c=10`, `w=100/1000`,
    /// `b=10000/86400000`, `l=60/60000`, `p=1000/60000` (periods in milliseconds).
    pub fn normalized(&self) -> String {
        match *self {
            Limit::Concurrency { limit } => format!("c={limit}"),
            Limit::Window { limit, period } => format!("w={limit}/{}", period.as_millis()),
            Limit::Bucket { limit, interval } => {
                format!("b={limit}/{}", interval.duration().as_millis())
            }
            Limit::Leaky { size, drain } => format!("l={size}/{}", drain.as_millis()),
            Limit::Points { capacity, refill } => format!("p={capacity}/{}", refill.as_millis()),
        }
    }
}

/// Parses one spec with its optional `per key` suffix.
pub(crate) fn parse_spec(text: &str) -> Result<(Limit, Scope)> {
    let trimmed = text.trim();
    let (body, scope) = match trimmed.strip_suffix(PER_KEY) {
        Some(body) if body.ends_with(char::is_whitespace) => (body.trim_end(), Scope::PerKey),
        _ => (trimmed, Scope::Global),
    };
    let Some((kind, value)) = body.split_once('=') else {
        return Err(invalid(text, "expected `kind=count[/period]`"));
    };
    let (count, period) = match value.split_once('/') {
        Some((count, period)) => (count.trim(), Some(period.trim())),
        None => (value.trim(), None),
    };
    let count: u32 = count
        .parse()
        .map_err(|_| invalid(text, "the count must be a whole number"))?;
    if count == 0 {
        return Err(invalid(text, "the count must be at least 1"));
    }
    let needs_period = |period: Option<&str>| -> Result<Duration> {
        let period = period.ok_or_else(|| invalid(text, "this kind needs a period (`/1s`)"))?;
        parse_period(period).map_err(|reason| invalid(text, &reason))
    };
    let limit = match kind.trim() {
        "concurrency" | "c" => {
            if period.is_some() {
                return Err(invalid(text, "concurrency takes no period"));
            }
            Limit::Concurrency { limit: count }
        }
        "window" | "w" => Limit::Window {
            limit: count,
            period: needs_period(period)?,
        },
        "bucket" | "b" => {
            let interval = match period {
                Some("second") => Interval::Second,
                Some("minute") => Interval::Minute,
                Some("hour") => Interval::Hour,
                Some("day") => Interval::Day,
                other => Interval::Every(needs_period(other)?),
            };
            Limit::Bucket {
                limit: count,
                interval,
            }
        }
        "leaky" | "l" => Limit::Leaky {
            size: count,
            drain: needs_period(period)?,
        },
        "points" | "p" => Limit::Points {
            capacity: count,
            refill: needs_period(period)?,
        },
        other => {
            return Err(invalid(
                text,
                &format!("unknown kind `{other}` (concurrency, window, bucket, leaky or points)"),
            ));
        }
    };
    Ok((limit, scope))
}

/// Parses specs joined by `;`.
#[allow(dead_code, reason = "read by the TOML throttles and the dashboard")]
pub(crate) fn parse_specs(text: &str) -> Result<Vec<(Limit, Scope)>> {
    text.split(';').map(parse_spec).collect()
}

/// A period: a bare number of seconds, or numbers with units in a row; positive and a whole
/// number of milliseconds.
fn parse_period(text: &str) -> std::result::Result<Duration, String> {
    let millis = if let Ok(seconds) = text.parse::<f64>() {
        seconds * 1_000.0
    } else {
        let mut total = 0.0;
        let mut rest = text;
        if rest.is_empty() {
            return Err("empty period".to_owned());
        }
        while !rest.is_empty() {
            let digits = rest
                .find(|c: char| !c.is_ascii_digit() && c != '.')
                .unwrap_or(rest.len());
            let number: f64 = rest[..digits]
                .parse()
                .map_err(|_| format!("`{text}` is not a period (try 1s, 5m or 1h30m)"))?;
            rest = &rest[digits..];
            let unit_end = rest
                .find(|c: char| c.is_ascii_digit() || c == '.')
                .unwrap_or(rest.len());
            let unit = match &rest[..unit_end] {
                "ms" => 1.0,
                "s" => 1_000.0,
                "m" => 60_000.0,
                "h" => 3_600_000.0,
                "d" => 86_400_000.0,
                "" => return Err(format!("`{text}` needs a unit after {number} (s, m, h, d)")),
                unit => return Err(format!("unknown unit `{unit}` in `{text}`")),
            };
            rest = &rest[unit_end..];
            total += number * unit;
        }
        total
    };
    if !millis.is_finite() || millis <= 0.0 || millis.fract() != 0.0 || millis > 1e15 {
        return Err(format!(
            "`{text}` is not a positive whole number of milliseconds"
        ));
    }
    // A whole number of milliseconds below 1e15: exact in an f64 and in a u64.
    Ok(Duration::from_millis(millis as u64))
}

fn invalid(text: &str, reason: &str) -> Error {
    Error::Config(format!("invalid limit `{}`: {reason}", text.trim()))
}

/// A duration with the largest units: `1d`, `1h30m`, `1s500ms`.
struct DisplayDuration(Duration);

impl fmt::Display for DisplayDuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut millis = self.0.as_millis();
        if millis == 0 {
            return f.write_str("0ms");
        }
        for (unit, size) in [
            ("d", 86_400_000),
            ("h", 3_600_000),
            ("m", 60_000),
            ("s", 1_000),
            ("ms", 1),
        ] {
            if millis >= size {
                write!(f, "{}{unit}", millis / size)?;
                millis %= size;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limit(text: &str) -> Limit {
        text.parse().unwrap()
    }

    #[test]
    fn every_kind_parses_in_both_forms() {
        assert_eq!(limit("concurrency=10"), Limit::Concurrency { limit: 10 });
        assert_eq!(limit(" c=10 "), Limit::Concurrency { limit: 10 });
        assert_eq!(
            limit("window=100/1s"),
            Limit::Window {
                limit: 100,
                period: Duration::from_secs(1)
            }
        );
        assert_eq!(limit("w=100/500ms"), limit("window=100/0.5"));
        assert_eq!(
            limit("bucket=10000/day"),
            Limit::Bucket {
                limit: 10_000,
                interval: Interval::Day
            }
        );
        assert_eq!(
            limit("b=5/1h30m"),
            Limit::Bucket {
                limit: 5,
                interval: Interval::Every(Duration::from_secs(5_400))
            }
        );
        assert_eq!(
            limit("leaky=60/1m"),
            Limit::Leaky {
                size: 60,
                drain: Duration::from_secs(60)
            }
        );
        assert_eq!(
            limit("p=1000/1d"),
            Limit::Points {
                capacity: 1_000,
                refill: Duration::from_secs(86_400)
            }
        );
    }

    #[test]
    fn display_round_trips_and_normalizes() {
        for (text, display, normalized) in [
            ("c=10", "concurrency=10", "c=10"),
            ("window=100/1s", "window=100/1s", "w=100/1000"),
            ("w=90/90", "window=90/1m30s", "w=90/90000"),
            ("b=10000/day", "bucket=10000/day", "b=10000/86400000"),
            ("bucket=3/second", "bucket=3/second", "b=3/1000"),
            ("b=3/1500ms", "bucket=3/1s500ms", "b=3/1500"),
            ("leaky=60/1m", "leaky=60/1m", "l=60/60000"),
            ("points=1000/2h", "points=1000/2h", "p=1000/7200000"),
        ] {
            let parsed = limit(text);
            assert_eq!(parsed.to_string(), display, "{text}");
            assert_eq!(limit(display), parsed, "{display}");
            assert_eq!(parsed.normalized(), normalized, "{text}");
        }
    }

    #[test]
    fn scopes_and_lists() {
        let specs = parse_specs("concurrency=25; window=5/1s per key").unwrap();
        assert_eq!(
            specs,
            vec![
                (Limit::Concurrency { limit: 25 }, Scope::Global),
                (
                    Limit::Window {
                        limit: 5,
                        period: Duration::from_secs(1)
                    },
                    Scope::PerKey
                ),
            ]
        );
        assert!("window=5/1s per key".parse::<Limit>().is_err());
        assert!(parse_spec("window=5/1sper key").is_err());
    }

    #[test]
    fn mistakes_are_refused() {
        for text in [
            "",
            "c",
            "c=0",
            "c=-1",
            "c=1.5",
            "c=5/1s",
            "w=5",
            "w=5/0s",
            "w=5/1x",
            "w=5/s",
            "w=5/0.0001",
            "b=5/week",
            "q=5/1s",
            "c=99999999999",
            "c=1;",
        ] {
            let error = parse_specs(text).unwrap_err();
            assert!(
                error.to_string().contains("invalid limit"),
                "{text}: {error}"
            );
        }
    }
}
