//! The spec grammar of limits (`concurrency=10`, `window=100/1s per key`, ...), parsed at
//! compile time by `#[hench::job(throttle(..))]`.
//!
//! This is a copy of the grammar of `hench::limit` (`crates/hench/src/limit/spec.rs`): a
//! procedural macro crate cannot depend on the crate it generates code for. The two must accept
//! and print the same specs; `crates/hench/tests/spec_parity.rs` feeds both one corpus. This
//! file depends on `std` only, so that the parity test can include it.

use std::fmt;

/// A parsed spec: the limit's kind, its count, its period (milliseconds) or named bucket
/// interval, and whether it applies per key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Spec {
    pub(crate) kind: Kind,
    pub(crate) count: u32,
    pub(crate) period: Option<Period>,
    pub(crate) per_key: bool,
}

/// The kinds of limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    Concurrency,
    Window,
    Bucket,
    Leaky,
    Points,
}

/// A limit's period.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Period {
    /// A duration, in milliseconds.
    Millis(u64),
    /// A named bucket interval: `second`, `minute`, `hour` or `day`.
    Named(&'static str),
}

impl Kind {
    /// The kind's name, as limits are written (`concurrency`).
    pub(crate) fn name(self) -> &'static str {
        match self {
            Kind::Concurrency => "concurrency",
            Kind::Window => "window",
            Kind::Bucket => "bucket",
            Kind::Leaky => "leaky",
            Kind::Points => "points",
        }
    }

    /// The kind written `name` (long or one-letter form).
    pub(crate) fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "concurrency" | "c" => Kind::Concurrency,
            "window" | "w" => Kind::Window,
            "bucket" | "b" => Kind::Bucket,
            "leaky" | "l" => Kind::Leaky,
            "points" | "p" => Kind::Points,
            _ => return None,
        })
    }
}

/// Parses one spec, with its optional `per key` suffix.
pub(crate) fn parse(text: &str) -> Result<Spec, String> {
    let invalid = |reason: &str| format!("invalid limit `{}`: {reason}", text.trim());
    let trimmed = text.trim();
    let (body, per_key) = match trimmed.strip_suffix("per key") {
        Some(body) if body.ends_with(char::is_whitespace) => (body.trim_end(), true),
        _ => (trimmed, false),
    };
    let Some((kind, value)) = body.split_once('=') else {
        return Err(invalid("expected `kind=count[/period]`"));
    };
    let (count, period) = match value.split_once('/') {
        Some((count, period)) => (count.trim(), Some(period.trim())),
        None => (value.trim(), None),
    };
    let count: u32 = count
        .parse()
        .map_err(|_| invalid("the count must be a whole number"))?;
    if count == 0 {
        return Err(invalid("the count must be at least 1"));
    }
    let kind = Kind::parse(kind.trim()).ok_or_else(|| {
        invalid(&format!(
            "unknown kind `{}` (concurrency, window, bucket, leaky or points)",
            kind.trim()
        ))
    })?;
    let period = match (kind, period) {
        (Kind::Concurrency, None) => None,
        (Kind::Concurrency, Some(_)) => return Err(invalid("concurrency takes no period")),
        (_, None) => return Err(invalid("this kind needs a period (`/1s`)")),
        (Kind::Bucket, Some(name @ ("second" | "minute" | "hour" | "day"))) => {
            Some(Period::Named(match name {
                "second" => "second",
                "minute" => "minute",
                "hour" => "hour",
                _ => "day",
            }))
        }
        (_, Some(period)) => Some(Period::Millis(
            millis(period).map_err(|reason| invalid(&reason))?,
        )),
    };
    Ok(Spec {
        kind,
        count,
        period,
        per_key,
    })
}

/// Parses specs joined by `;`.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn parse_list(text: &str) -> Result<Vec<Spec>, String> {
    text.split(';').map(parse).collect()
}

/// A period in milliseconds: a bare number of seconds, or numbers with units (`ms`, `s`, `m`,
/// `h`, `d`) in a row; positive and whole.
fn millis(text: &str) -> Result<u64, String> {
    let total = if let Ok(seconds) = text.parse::<f64>() {
        seconds * 1_000.0
    } else {
        if text.is_empty() {
            return Err("empty period".to_owned());
        }
        let mut total = 0.0;
        let mut rest = text;
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
    if !total.is_finite() || total <= 0.0 || total.fract() != 0.0 || total > 1e15 {
        return Err(format!(
            "`{text}` is not a positive whole number of milliseconds"
        ));
    }
    // A whole number of milliseconds below 1e15: exact in an f64 and in a u64.
    Ok(total as u64)
}

/// Milliseconds with the largest units: `1d`, `1h30m`, `1s500ms`.
fn write_millis(f: &mut fmt::Formatter<'_>, mut millis: u64) -> fmt::Result {
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

impl fmt::Display for Spec {
    /// The long form `hench::limit::Limit` displays, with ` per key` for per-key specs.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}={}", self.kind.name(), self.count)?;
        match self.period {
            None => {}
            Some(Period::Named(name)) => write!(f, "/{name}")?,
            Some(Period::Millis(millis)) => {
                f.write_str("/")?;
                write_millis(f, millis)?;
            }
        }
        if self.per_key {
            f.write_str(" per key")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn specs_parse_and_print() {
        for (text, display) in [
            ("c=10", "concurrency=10"),
            ("window=100/1s per key", "window=100/1s per key"),
            ("w=90/90", "window=90/1m30s"),
            ("b=10000/day", "bucket=10000/day"),
            ("leaky=60/1m", "leaky=60/1m"),
            ("p=5/1500ms", "points=5/1s500ms"),
        ] {
            assert_eq!(parse(text).unwrap().to_string(), display, "{text}");
        }
        assert_eq!(parse_list("c=1; w=2/1s").unwrap().len(), 2);
        for text in ["", "c=0", "c=5/1s", "w=5", "w=5/0", "x=1", "w=1/1x", "c=1;"] {
            assert!(parse_list(text).is_err(), "{text}");
        }
    }
}
