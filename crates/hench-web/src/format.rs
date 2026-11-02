//! Formatting helpers for the pages: numbers, memory sizes, times, job arguments, and the
//! natural order of host names.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Datelike, Utc};
use serde_json::Value;

/// `1234567` → `"1,234,567"`.
pub(crate) fn delimit(number: u64) -> String {
    let digits = number.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            out.push(',');
        }
        out.push(digit);
    }
    out
}

/// A float with `decimals` decimals and thousands separators: `1234.5` → `"1,234.50"`.
/// Negative and non-finite values show as zero (latencies and runtimes are never negative).
pub(crate) fn decimal(value: f64, decimals: usize) -> String {
    let value = if value.is_finite() && value > 0.0 {
        value
    } else {
        0.0
    };
    let text = format!("{value:.decimals$}");
    let (whole, fraction) = match text.split_once('.') {
        Some((whole, fraction)) => (whole, Some(fraction)),
        None => (text.as_str(), None),
    };
    let whole = whole
        .parse::<u64>()
        .map(delimit)
        .unwrap_or_else(|_| whole.to_owned());
    match fraction {
        Some(fraction) => format!("{whole}.{fraction}"),
        None => whole,
    }
}

/// A memory size given in KB: `"0"` for nothing, KB below 100 000 KB, MB below
/// 10 000 000 KB, GB (one decimal) above.
pub(crate) fn memory(kb: u64) -> String {
    if kb == 0 {
        "0".to_owned()
    } else if kb < 100_000 {
        format!("{} KB", delimit(kb))
    } else if kb < 10_000_000 {
        format!("{} MB", delimit(kb / 1024))
    } else {
        format!("{} GB", decimal(kb as f64 / (1024.0 * 1024.0), 1))
    }
}

/// A percentage of `part` in `whole`, rounded: `0` when `whole` is zero.
pub(crate) fn percent(part: u64, whole: u64) -> u64 {
    if whole == 0 {
        return 0;
    }
    ((part as f64 / whole as f64) * 100.0).round() as u64
}

/// `time` as a UTC date and time, or `None` beyond what the calendar represents (about
/// 262,000 years either side of the epoch). Times come from Redis, so any value can show up:
/// a job scheduled with `at` in microseconds, an absurd retry delay, a corrupt heartbeat.
pub(crate) fn datetime(time: SystemTime) -> Option<DateTime<Utc>> {
    let (secs, nanos) = match time.duration_since(UNIX_EPOCH) {
        Ok(after) => (i64::try_from(after.as_secs()).ok()?, after.subsec_nanos()),
        Err(error) => {
            let before = error.duration();
            let secs = i64::try_from(before.as_secs()).ok()?.checked_neg()?;
            match before.subsec_nanos() {
                0 => (secs, 0),
                nanos => (secs.checked_sub(1)?, 1_000_000_000 - nanos),
            }
        }
    };
    DateTime::from_timestamp(secs, nanos)
}

/// Whole seconds since the Unix epoch (negative before it, saturating).
pub(crate) fn unix_seconds(time: SystemTime) -> i64 {
    match time.duration_since(UNIX_EPOCH) {
        Ok(after) => i64::try_from(after.as_secs()).unwrap_or(i64::MAX),
        Err(error) => i64::try_from(error.duration().as_secs()).map_or(i64::MIN, |secs| -secs),
    }
}

/// A time as UTC, e.g. `2026-11-01 12:34:56 UTC`; beyond the calendar, its Unix time.
pub(crate) fn utc(time: SystemTime) -> String {
    match datetime(time) {
        Some(time) => time.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
        None => crate::strings::unix_time(unix_seconds(time)),
    }
}

/// A time in RFC 3339 (UTC, seconds), e.g. `2026-11-01T12:34:56Z`, for `datetime`
/// attributes; empty for times RFC 3339 cannot write (years outside 0000–9999).
pub(crate) fn iso(time: SystemTime) -> String {
    match datetime(time) {
        Some(time) if (0..=9999).contains(&time.year()) => {
            time.format("%Y-%m-%dT%H:%M:%SZ").to_string()
        }
        _ => String::new(),
    }
}

/// The wall-clock time of day in UTC, e.g. `12:34:56 UTC`; beyond the calendar, the Unix
/// time.
pub(crate) fn clock(time: SystemTime) -> String {
    match datetime(time) {
        Some(time) => time.format("%H:%M:%S UTC").to_string(),
        None => crate::strings::unix_time(unix_seconds(time)),
    }
}

/// Milliseconds since the Unix epoch (0 before it).
pub(crate) fn epoch_millis(time: SystemTime) -> i64 {
    time.duration_since(UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// The units of relative times, largest first, with their length in seconds. `hench.js`
/// uses the same units and thresholds when it refreshes relative times in the browser.
const UNITS: [(u64, TimeUnit); 6] = [
    (365 * 24 * 3600, TimeUnit::Year),
    (30 * 24 * 3600, TimeUnit::Month),
    (24 * 3600, TimeUnit::Day),
    (3600, TimeUnit::Hour),
    (60, TimeUnit::Minute),
    (1, TimeUnit::Second),
];

/// Times closer than this to now are "just now".
const JUST_NOW_SECS: u64 = 10;

/// A unit of a relative time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TimeUnit {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}

/// `time` relative to `now`, in words: `just now`, `3 minutes ago`, `in 2 hours`, ...
pub(crate) fn relative(time: SystemTime, now: SystemTime) -> String {
    let (elapsed, past) = match now.duration_since(time) {
        Ok(elapsed) => (elapsed, true),
        Err(error) => (error.duration(), false),
    };
    let secs = elapsed.as_secs();
    if secs < JUST_NOW_SECS {
        return crate::strings::EN.just_now.to_owned();
    }
    let (size, unit) = UNITS
        .iter()
        .copied()
        .find(|(size, _)| secs >= *size)
        .unwrap_or((1, TimeUnit::Second));
    crate::strings::relative_time(secs / size, unit, past)
}

/// A time for display: relative text, with the absolute UTC time for the `title` (shown on
/// hover) and the machine-readable `datetime` attribute.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TimeView {
    /// RFC 3339, for `<time datetime>`.
    pub(crate) iso: String,
    /// Absolute UTC time, for the tooltip.
    pub(crate) utc: String,
    /// Relative time (`3 minutes ago`).
    pub(crate) text: String,
}

impl TimeView {
    /// `time` seen from `now`.
    pub(crate) fn new(time: SystemTime, now: SystemTime) -> Self {
        Self {
            iso: iso(time),
            utc: utc(time),
            text: relative(time, now),
        }
    }
}

/// `text` with its control characters spelled out (`\n`, `\u{1b}`), for names that come
/// from Redis (queue names): HTML would collapse a line break and hide an escape character.
pub(crate) fn visible(text: &str) -> Cow<'_, str> {
    if !text.chars().any(char::is_control) {
        return Cow::Borrowed(text);
    }
    let mut out = String::with_capacity(text.len() + 8);
    for c in text.chars() {
        if c.is_control() {
            out.extend(c.escape_default());
        } else {
            out.push(c);
        }
    }
    Cow::Owned(out)
}

/// Truncates `text` to at most `max` characters, adding an ellipsis when it was cut.
/// Returns the text and whether it was cut.
pub(crate) fn truncate(text: &str, max: usize) -> (String, bool) {
    match text.char_indices().nth(max) {
        None => (text.to_owned(), false),
        Some((cut, _)) => (format!("{}…", &text[..cut]), true),
    }
}

/// One job argument as shown in lists and detail pages: compact JSON.
pub(crate) fn arg(value: &Value) -> String {
    value.to_string()
}

/// Job arguments for display: each argument as compact JSON, joined by `, `.
pub(crate) fn args(values: &[Value]) -> String {
    values.iter().map(arg).collect::<Vec<_>>().join(", ")
}

/// Job arguments for a list: the full text when it is short, else a cut version (first) and
/// the full text up to `full_max` characters (second, `None` when nothing was cut).
pub(crate) fn args_summary(
    values: &[Value],
    short_max: usize,
    full_max: usize,
) -> (String, Option<String>) {
    let all = args(values);
    let (short, cut) = truncate(&all, short_max);
    if !cut {
        return (short, None);
    }
    let (full, _) = truncate(&all, full_max);
    (short, Some(full))
}

/// A whole number of milliseconds, in the unit that reads best: `"20 ms"`, `"1.1 s"`,
/// `"5.75 s"`, `"13 s"` (at most two decimals, trailing zeros dropped).
pub(crate) fn millis(ms: u64) -> String {
    if ms < 1_000 {
        return format!("{ms} ms");
    }
    let seconds = format!("{:.2}", ms as f64 / 1_000.0);
    let seconds = seconds.trim_end_matches('0').trim_end_matches('.');
    format!("{seconds} s")
}

/// A duration in milliseconds (fractional, e.g. an average): `"4.5 ms"` below a second,
/// else seconds with two decimals (`"1.25 s"`). Negative and non-finite values show as zero.
pub(crate) fn duration_ms(ms: f64) -> String {
    let ms = if ms.is_finite() && ms > 0.0 { ms } else { 0.0 };
    if ms < 1_000.0 {
        format!("{} ms", decimal(ms, 1))
    } else {
        format!("{} s", decimal(ms / 1_000.0, 2))
    }
}

/// One chunk of a host name for natural ordering: text, or a run of digits compared by value.
#[derive(Debug, PartialEq, Eq)]
enum Chunk<'a> {
    Text(&'a str),
    Number(&'a str),
}

impl Ord for Chunk<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Chunk::Text(a), Chunk::Text(b)) => a.cmp(b),
            (Chunk::Number(a), Chunk::Number(b)) => {
                let (a, b) = (a.trim_start_matches('0'), b.trim_start_matches('0'));
                a.len().cmp(&b.len()).then_with(|| a.cmp(b))
            }
            // Chunks alternate text/number from a text start, so mixed pairs only happen
            // when one name ends; order text first for totality.
            (Chunk::Text(_), Chunk::Number(_)) => Ordering::Less,
            (Chunk::Number(_), Chunk::Text(_)) => Ordering::Greater,
        }
    }
}

impl PartialOrd for Chunk<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Splits `text` into alternating text and digit runs, always starting with a (possibly
/// empty) text chunk, so that chunks at the same position have the same kind.
fn chunks(text: &str) -> Vec<Chunk<'_>> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut digits = false;
    for (index, c) in text.char_indices() {
        if c.is_ascii_digit() != digits {
            out.push(if digits {
                Chunk::Number(&text[start..index])
            } else {
                Chunk::Text(&text[start..index])
            });
            start = index;
            digits = !digits;
        }
    }
    out.push(if digits {
        Chunk::Number(&text[start..])
    } else {
        Chunk::Text(&text[start..])
    });
    out
}

/// Natural order of host names: digit runs compare by value (`a.2` before `a.10`).
pub(crate) fn natural_cmp(a: &str, b: &str) -> Ordering {
    chunks(a).cmp(&chunks(b)).then_with(|| a.cmp(b))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::time::Duration;

    #[test]
    fn durations_read_well() {
        assert_eq!(millis(0), "0 ms");
        assert_eq!(millis(20), "20 ms");
        assert_eq!(millis(1_000), "1 s");
        assert_eq!(millis(1_100), "1.1 s");
        assert_eq!(millis(5_750), "5.75 s");
        assert_eq!(millis(335_000), "335 s");
        assert_eq!(duration_ms(4.54), "4.5 ms");
        assert_eq!(duration_ms(999.94), "999.9 ms");
        assert_eq!(duration_ms(1_250.0), "1.25 s");
        assert_eq!(duration_ms(f64::NAN), "0.0 ms");
        assert_eq!(duration_ms(-3.0), "0.0 ms");
    }

    #[test]
    fn numbers_get_thousands_separators() {
        assert_eq!(delimit(0), "0");
        assert_eq!(delimit(999), "999");
        assert_eq!(delimit(1_000), "1,000");
        assert_eq!(delimit(1_234_567), "1,234,567");
        assert_eq!(delimit(u64::MAX), "18,446,744,073,709,551,615");
        assert_eq!(decimal(12.3456, 2), "12.35");
        assert_eq!(decimal(567.89999, 2), "567.90");
        assert_eq!(decimal(1_234_567.891, 2), "1,234,567.89");
        assert_eq!(decimal(-5.0, 2), "0.00");
        assert_eq!(decimal(f64::NAN, 1), "0.0");
        assert_eq!(decimal(65.0, 0), "65");
        assert_eq!(percent(1, 3), 33);
        assert_eq!(percent(5, 0), 0);
    }

    #[test]
    fn memory_sizes_pick_a_unit() {
        // Sidekiq's format_memory scenarios.
        assert_eq!(memory(0), "0");
        assert_eq!(memory(1), "1 KB");
        assert_eq!(memory(99_999), "99,999 KB");
        assert_eq!(memory(100_002), "97 MB");
        assert_eq!(memory(10_000_001), "9.5 GB");
    }

    #[test]
    fn relative_times_read_naturally() {
        let now = UNIX_EPOCH + Duration::from_secs(1_800_000_000);
        let ago = |secs| relative(now - Duration::from_secs(secs), now);
        assert_eq!(ago(0), "just now");
        assert_eq!(ago(9), "just now");
        assert_eq!(ago(30), "30 seconds ago");
        assert_eq!(ago(60), "1 minute ago");
        assert_eq!(ago(179), "2 minutes ago");
        assert_eq!(ago(3_600), "1 hour ago");
        assert_eq!(ago(3 * 3_600 + 5), "3 hours ago");
        assert_eq!(ago(86_400), "1 day ago");
        assert_eq!(ago(45 * 86_400), "1 month ago");
        assert_eq!(ago(800 * 86_400), "2 years ago");
        assert_eq!(
            relative(now + Duration::from_secs(300), now),
            "in 5 minutes"
        );
        let view = TimeView::new(UNIX_EPOCH + Duration::from_secs(1_700_000_000), now);
        assert_eq!(view.iso, "2023-11-14T22:13:20Z");
        assert_eq!(view.utc, "2023-11-14 22:13:20 UTC");
        assert_eq!(view.text, "3 years ago");
        assert_eq!(
            clock(UNIX_EPOCH + Duration::from_secs(3_661)),
            "01:01:01 UTC"
        );
        assert_eq!(
            epoch_millis(UNIX_EPOCH + Duration::from_millis(1_234)),
            1_234
        );
        assert_eq!(epoch_millis(UNIX_EPOCH - Duration::from_secs(1)), 0);
    }

    #[test]
    fn times_beyond_the_calendar_are_shown_as_unix_time() {
        let now = UNIX_EPOCH + Duration::from_secs(1_800_000_000);
        let far = UNIX_EPOCH + Duration::from_secs(10_000_000_000_000);
        assert_eq!(datetime(far), None);
        assert_eq!(utc(far), "Unix time 10000000000000");
        assert_eq!(clock(far), "Unix time 10000000000000");
        assert_eq!(iso(far), "");
        let view = TimeView::new(far, now);
        assert_eq!(view.text, "in 317,040 years");
        let ancient = UNIX_EPOCH - Duration::from_secs(9_000_000_000_000);
        assert_eq!(utc(ancient), "Unix time -9000000000000");
        assert_eq!(iso(ancient), "");
        assert_eq!(relative(ancient, now), "285,445 years ago");
        // The largest times a SystemTime holds do not overflow anything.
        let end = UNIX_EPOCH + Duration::from_secs(u64::MAX / 2);
        assert_eq!(utc(end), format!("Unix time {}", u64::MAX / 2));
        // Representable, but not in RFC 3339: shown, without a machine-readable form.
        let year_33658 = UNIX_EPOCH + Duration::from_secs(1_000_000_000_000);
        assert_eq!(utc(year_33658), "+33658-09-27 01:46:40 UTC");
        assert_eq!(iso(year_33658), "");
        // Before the epoch, fractions of a second round down like after it.
        let before = UNIX_EPOCH - Duration::from_millis(1_500);
        assert_eq!(iso(before), "1969-12-31T23:59:58Z");
        assert_eq!(unix_seconds(before), -1);
        assert_eq!(utc(UNIX_EPOCH), "1970-01-01 00:00:00 UTC");
    }

    #[test]
    fn long_text_is_cut_on_character_boundaries() {
        assert_eq!(truncate("short", 10), ("short".to_owned(), false));
        assert_eq!(truncate("exactly", 7), ("exactly".to_owned(), false));
        assert_eq!(truncate("héllo wörld", 5), ("héllo…".to_owned(), true));
        assert_eq!(truncate("", 0), (String::new(), false));
    }

    #[test]
    fn control_characters_are_spelled_out() {
        assert!(matches!(visible("default"), Cow::Borrowed("default")));
        assert_eq!(visible("a\u{1b}[31mb\nc\td\r"), "a\\u{1b}[31mb\\nc\\td\\r");
        assert_eq!(visible("é\u{7f}"), "é\\u{7f}");
    }

    #[test]
    fn arguments_are_shown_as_json() {
        // Sidekiq's display_args scenarios: values are shown as literals, strings quoted.
        assert_eq!(args(&[json!(1), json!(2), json!(3)]), "1, 2, 3");
        assert_eq!(args(&[json!("<html>"), json!(12)]), "\"<html>\", 12");
        assert_eq!(args(&[json!({"a": [1, null]})]), "{\"a\":[1,null]}");
        assert_eq!(args(&[]), "");
        let long = vec![json!("x".repeat(500))];
        let (short, full) = args_summary(&long, 100, 300);
        assert_eq!(short.chars().count(), 101);
        assert!(short.ends_with('…'));
        assert_eq!(full.as_deref().map(|full| full.chars().count()), Some(301));
        let (short, full) = args_summary(&[json!(1)], 100, 300);
        assert_eq!((short.as_str(), full), ("1", None));
    }

    #[test]
    fn host_names_sort_naturally() {
        // Sidekiq's sorted_processes scenarios.
        let mut hosts = vec![
            "a.10.2",
            "a.2",
            "busybee--10_1",
            "a.23",
            "a.10.1",
            "a.1",
            "192.168.0.10",
            "192.168.0.2",
            "2.1.1.1",
            "busybee-2__34",
        ];
        hosts.sort_by(|a, b| natural_cmp(a, b));
        assert_eq!(
            hosts,
            [
                "2.1.1.1",
                "192.168.0.2",
                "192.168.0.10",
                "a.1",
                "a.2",
                "a.10.1",
                "a.10.2",
                "a.23",
                "busybee-2__34",
                "busybee--10_1"
            ]
        );
        let mut workers = vec![
            "worker_critical.2",
            "worker_default.1",
            "worker_critical.1",
            "worker_default.2",
            "worker_critical.10",
        ];
        workers.sort_by(|a, b| natural_cmp(a, b));
        assert_eq!(
            workers,
            [
                "worker_critical.1",
                "worker_critical.2",
                "worker_critical.10",
                "worker_default.1",
                "worker_default.2"
            ]
        );
        // Huge digit runs and leading zeros compare by value without overflowing.
        assert_eq!(
            natural_cmp("h99999999999999999999999", "h100000000000000000000000"),
            Ordering::Less
        );
        assert_eq!(natural_cmp("h007", "h7"), Ordering::Less);
        assert_eq!(natural_cmp("h", "h1"), Ordering::Less);
        assert_eq!(natural_cmp("", ""), Ordering::Equal);
    }
}
