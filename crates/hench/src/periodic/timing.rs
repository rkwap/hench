//! When a schedule ticks: the private wrapper around `croner` (cron expressions) and `jiff`
//! (IANA time zones), and the arithmetic of fixed intervals (`docs/design/periodic.md` §2.2.2).
//!
//! Every function works on whole epoch seconds. `croner`'s types never leave this module, so
//! the parser can be replaced without touching the rest of hench.
//!
//! Daylight saving time follows Vixie cron: a fixed-time tick — of an expression whose seconds,
//! minutes and hours fields do not start with `*` (single values, lists, ranges: cronie's
//! `MIN_STAR`/`HR_STAR` rule) — that falls in a skipped interval runs at the first instant after
//! the gap, and once in a repeated interval (its first pass); ticks of fields starting with `*`
//! (`*`, `*/15`) follow real time (none in a gap, both passes of a fold). `croner` applies the
//! fixed-time rules to single values only: for lists and ranges this module applies them to
//! `croner`'s real-time results (it drops the second pass of a fold, and adds the instant after
//! a gap of up to two hours in which a skipped wall clock time matches). `croner` gives up on a
//! gap longer than two hours (the whole day Pacific/Apia skipped on 2011-12-30):
//! [`Timing::next_after`] then searches again from the end of the gap.

use std::fmt;

use croner::Cron;
use croner::errors::CronError;
use croner::parser::{CronParser, Seconds, Year};
use jiff::tz::TimeZone;
use jiff::{Timestamp, Zoned};

/// Missed ticks are counted up to this many for cron timetables (shown as "10 000+"); intervals
/// count exactly.
pub(crate) const MISSED_CAP: u64 = 10_000;

/// The longest interval of [`Timing::Every`]: 100 years, so that tick arithmetic stays far from
/// overflowing.
pub(crate) const MAX_INTERVAL_SECS: u64 = 100 * 366 * 24 * 3_600;

/// How far past an instant a search that `croner` gave up on is tried again (at the time zone's
/// transitions and every hour).
const RETRY_HOURS: i64 = 48;

/// Most candidates [`CronTiming::latest_at_or_before`] checks before giving up.
const MAX_ROUNDS: usize = 16;

/// The longest skipped interval after which a fixed-time tick of the interval still runs (as
/// `croner` does for single values; longer gaps are whole skipped days).
const MAX_GAP_SECS: i64 = 2 * 3_600;

/// Years of dates an expression's day and month fields are checked against before searching:
/// 28 years hold every weekday of every date, the 29th of February included.
const DATE_CHECK_YEARS: i32 = 29;

/// Why a tick could not be computed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TimingError(pub(crate) String);

impl fmt::Display for TimingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for TimingError {}

/// A parsed timetable.
#[derive(Clone)]
pub(crate) enum Timing {
    /// A cron expression in a time zone (boxed: the parsed pattern is large).
    Cron(Box<CronTiming>),
    /// Every `interval` seconds, at `k × interval + offset` epoch seconds.
    Every {
        /// Seconds between ticks (at least one).
        interval: i64,
        /// Seconds after the epoch-aligned instants (below `interval`).
        offset: i64,
    },
}

/// A cron expression and its zone.
#[derive(Clone)]
pub(crate) struct CronTiming {
    cron: Cron,
    tz: TimeZone,
    /// The expression as stored (trimmed, inner whitespace collapsed).
    expression: String,
    /// The zone's name as given.
    zone: String,
    /// Fixed-time in Vixie cron's sense (see [`vixie_fixed_time`]).
    fixed: bool,
    /// Fixed-time but not for `croner` (a list or a range): this wrapper applies the fixed-time
    /// rules to `croner`'s real-time results.
    emulated: bool,
}

impl fmt::Debug for Timing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Timing::Cron(cron) => f
                .debug_struct("Cron")
                .field("expression", &cron.expression)
                .field("zone", &cron.zone)
                .finish(),
            Timing::Every { interval, offset } => f
                .debug_struct("Every")
                .field("interval", interval)
                .field("offset", offset)
                .finish(),
        }
    }
}

/// The expression as stored: trimmed, inner whitespace collapsed to single spaces.
pub(crate) fn normalize_expression(expression: &str) -> String {
    expression.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The time zone named `name` (an IANA name such as `Europe/Paris`, or `UTC`), from the system
/// database when it has it, else from the copy bundled with hench (`jiff` reads only the system
/// database when there is one: a host whose tzdata predates a zone still knows it).
pub(crate) fn zone(name: &str) -> Result<TimeZone, TimingError> {
    zone_in(name, jiff::tz::db())
}

/// [`zone`] with `system` as the system database.
fn zone_in(name: &str, system: &jiff::tz::TimeZoneDatabase) -> Result<TimeZone, TimingError> {
    if name.is_empty() {
        return Err(TimingError("the time zone is empty".to_owned()));
    }
    if name.trim() != name || name.chars().any(char::is_control) {
        return Err(TimingError(format!(
            "`{}` is not a time zone name",
            name.escape_debug()
        )));
    }
    system
        .get(name)
        .or_else(|error| {
            jiff::tz::TimeZoneDatabase::bundled()
                .get(name)
                .map_err(|_| error)
        })
        .map_err(|error| {
            TimingError(format!(
                "unknown time zone `{name}` (use an IANA name such as `Europe/Paris`, or \
                 `UTC`): {error}"
            ))
        })
}

/// Whether `name` is written like a time zone name but is in neither the system database nor the
/// bundled copy: a zone newer than both (a definition from a host with newer tzdata).
pub(crate) fn unknown_zone(name: &str) -> bool {
    !name.is_empty()
        && name.trim() == name
        && !name.chars().any(char::is_control)
        && zone(name).is_err()
}

/// Whether `expression` (as stored) is fixed-time in Vixie cron's sense: none of its seconds,
/// minutes and hours fields starts with `*` (cronie sets `MIN_STAR` and `HR_STAR` only then), so
/// single values, lists, ranges and `a-b/n` steps are fixed-time. Aliases are, except `@hourly`
/// (`0 * * * *`). A 5-field expression's seconds are 0.
fn vixie_fixed_time(expression: &str) -> bool {
    if expression.starts_with('@') {
        return !expression.eq_ignore_ascii_case("@hourly");
    }
    let fields: Vec<&str> = expression.split(' ').collect();
    let time_fields = if fields.len() == 6 { 3 } else { 2 };
    fields
        .iter()
        .take(time_fields)
        .all(|field| !field.starts_with('*') && !field.starts_with('?'))
}

/// Whether some date of the [`DATE_CHECK_YEARS`] years from `today` matches the expression's
/// day and month fields (day of month, month, day of week, `L`, `W`, `#`, `+`): a few thousand
/// checks of bits, where `croner`'s search for an impossible date (`0 0 30 2 *`) gives up only
/// after walking a long way, and the retries past long gaps multiply that.
fn some_date_matches(cron: &Cron, today: jiff::civil::Date) -> bool {
    let mut date = today;
    let Ok(end) = today.checked_add(jiff::Span::new().years(DATE_CHECK_YEARS)) else {
        return true;
    };
    while date < end {
        let month = u32::try_from(date.month()).unwrap_or(0);
        let day = u32::try_from(date.day()).unwrap_or(0);
        let weekday = croner::Weekday::from_days_from_sunday(u32::from(
            date.weekday().to_sunday_zero_offset().unsigned_abs(),
        ));
        if cron.pattern.month_match(month).unwrap_or(false)
            && cron
                .pattern
                .day_match(i32::from(date.year()), month, day, weekday)
                .unwrap_or(false)
        {
            return true;
        }
        match date.tomorrow() {
            Ok(next) => date = next,
            Err(_) => return true,
        }
    }
    false
}

/// Parses a cron expression (see [`Timing::cron`] for the dialect).
fn parse_cron(expression: &str) -> Result<Cron, TimingError> {
    if expression.is_empty() {
        return Err(TimingError("the cron expression is empty".to_owned()));
    }
    let invalid =
        |reason: &str| TimingError(format!("invalid cron expression `{expression}`: {reason}"));
    let source = if expression.starts_with('@') {
        match expression.to_ascii_lowercase().as_str() {
            "@yearly" | "@annually" | "@monthly" | "@weekly" | "@daily" | "@hourly" => {
                expression.to_owned()
            }
            "@midnight" => "@daily".to_owned(),
            "@reboot" => {
                return Err(invalid(
                    "`@reboot` is not supported: a schedule ticks at instants, not when processes \
                     start",
                ));
            }
            alias if alias.starts_with("@every") => {
                return Err(invalid(
                    "`@every` is not supported: use `Periodic::every` for fixed intervals",
                ));
            }
            _ => {
                return Err(invalid(
                    "unknown alias (use @yearly, @annually, @monthly, @weekly, @daily, \
                     @midnight or @hourly)",
                ));
            }
        }
    } else {
        let fields = expression.split(' ').count();
        match fields {
            5 | 6 => expression.to_owned(),
            7 => {
                return Err(invalid(
                    "the year field is not supported (5 fields, or 6 with seconds first)",
                ));
            }
            _ => {
                return Err(invalid(&format!(
                    "{fields} fields; expected 5 (minute hour day-of-month month day-of-week) or \
                     6 (seconds first)"
                )));
            }
        }
    };
    let parser = CronParser::builder()
        .seconds(Seconds::Optional)
        .year(Year::Disallowed)
        .dom_and_dow(false)
        .alternative_weekdays(false)
        .sloppy_ranges(false)
        .build();
    parser.parse(&source).map_err(|error| {
        let reason = match error {
            CronError::InvalidPattern(message)
            | CronError::IllegalCharacters(message)
            | CronError::ComponentError(message) => message,
            other => other.to_string(),
        };
        invalid(reason.trim_end_matches('.'))
    })
}

/// `t` as an instant, or an error when it is out of the supported range.
fn timestamp(t: i64) -> Result<Timestamp, TimingError> {
    Timestamp::from_second(t)
        .map_err(|_| TimingError(format!("{t} is not a supported instant (epoch seconds)")))
}

impl Timing {
    /// A cron timetable: `expression` (5 fields, or 6 with seconds first, or an alias) in the
    /// zone `tz`.
    ///
    /// Accepted: lists, ranges, `*/n` and `a-b/n` steps, month and weekday names in any case,
    /// weekday 7 (Sunday), `?` as `*` in the day fields, `L`, `nW`, `n#k` and `nL`, the `+`
    /// prefix on the weekday field (day of month AND day of week; without it either matches, as
    /// in POSIX), and `@yearly`, `@annually`, `@monthly`, `@weekly`, `@daily`, `@midnight`,
    /// `@hourly`. Refused: the year field, `0/10` (write `*/10`), `@reboot`, `@every`, and an
    /// expression with no tick after now (`0 0 30 2 *`).
    pub(crate) fn cron(expression: &str, tz: &str) -> Result<Self, TimingError> {
        let expression = normalize_expression(expression);
        let cron = parse_cron(&expression)?;
        let fixed = vixie_fixed_time(&expression);
        let emulated = fixed && cron.determine_job_type() != croner::JobType::FixedTime;
        let time_zone = zone(tz)?;
        // An expression that never ticks (an impossible date) is refused now rather than when
        // the poller finds no next tick, and cheaply: no date matches its day fields.
        let now = Timestamp::now();
        if !some_date_matches(&cron, now.to_zoned(time_zone.clone()).date()) {
            return Err(TimingError(format!(
                "the cron expression `{expression}` never ticks: no date of the next \
                 {DATE_CHECK_YEARS} years matches its day and month fields"
            )));
        }
        let timing = Timing::Cron(Box::new(CronTiming {
            cron,
            tz: time_zone,
            expression,
            zone: tz.to_owned(),
            fixed,
            emulated,
        }));
        let now = now.as_second();
        timing.next_after(now).map_err(|error| {
            TimingError(format!(
                "the cron expression `{}` never ticks: {error}",
                timing.expression().unwrap_or_default()
            ))
        })?;
        Ok(timing)
    }

    /// An interval timetable: every `interval` seconds (1 to 100 years), at `offset` seconds
    /// (below the interval) after the instants aligned to the Unix epoch.
    pub(crate) fn every(interval: u64, offset: u64) -> Result<Self, TimingError> {
        if interval == 0 {
            return Err(TimingError(
                "the interval must be at least one second".to_owned(),
            ));
        }
        if interval > MAX_INTERVAL_SECS {
            return Err(TimingError(format!(
                "the interval must be at most {MAX_INTERVAL_SECS} seconds (100 years)"
            )));
        }
        if offset >= interval {
            return Err(TimingError(format!(
                "the offset ({offset} s) must be below the interval ({interval} s)"
            )));
        }
        // Both fit: at most 100 years of seconds.
        Ok(Timing::Every {
            interval: i64::try_from(interval).unwrap_or(i64::MAX),
            offset: i64::try_from(offset).unwrap_or(0),
        })
    }

    /// The cron expression, for cron timetables.
    pub(crate) fn expression(&self) -> Option<&str> {
        match self {
            Timing::Cron(cron) => Some(&cron.expression),
            Timing::Every { .. } => None,
        }
    }

    /// The time zone ticks are computed in (UTC for intervals).
    pub(crate) fn time_zone(&self) -> TimeZone {
        match self {
            Timing::Cron(cron) => cron.tz.clone(),
            Timing::Every { .. } => TimeZone::UTC,
        }
    }

    /// Whether a cron timetable is fixed-time, the class of expressions the DST rules treat
    /// specially: Vixie cron's rule, none of the seconds, minutes and hours fields starts with
    /// `*` (single values, lists and ranges are fixed-time). Intervals are not.
    pub(crate) fn is_fixed_time(&self) -> bool {
        match self {
            Timing::Cron(cron) => cron.fixed,
            Timing::Every { .. } => false,
        }
    }

    /// The first tick strictly after `t`.
    pub(crate) fn next_after(&self, t: i64) -> Result<i64, TimingError> {
        match self {
            Timing::Every { interval, offset } => {
                let k = (t - offset).div_euclid(*interval) + 1;
                k.checked_mul(*interval)
                    .and_then(|tick| tick.checked_add(*offset))
                    .ok_or_else(|| TimingError(format!("no tick after {t}: out of range")))
            }
            Timing::Cron(cron) => cron.next_after(t),
        }
    }

    /// The last tick at or before `t`.
    pub(crate) fn latest_at_or_before(&self, t: i64) -> Result<i64, TimingError> {
        match self {
            Timing::Every { interval, offset } => {
                let k = (t - offset).div_euclid(*interval);
                k.checked_mul(*interval)
                    .and_then(|tick| tick.checked_add(*offset))
                    .ok_or_else(|| TimingError(format!("no tick before {t}: out of range")))
            }
            Timing::Cron(cron) => cron.latest_at_or_before(t),
        }
    }

    /// The number of ticks in `[from, to)`, counted up to `cap` (intervals: exactly), and
    /// whether the count stopped at the cap.
    pub(crate) fn count(&self, from: i64, to: i64, cap: u64) -> Result<(u64, bool), TimingError> {
        if from >= to {
            return Ok((0, false));
        }
        match self {
            Timing::Every { interval, .. } => {
                let first = self.next_after(from - 1)?;
                if first >= to {
                    return Ok((0, false));
                }
                let count = (to - 1 - first) / interval + 1;
                Ok((u64::try_from(count).unwrap_or(0), false))
            }
            Timing::Cron(_) => {
                let mut count = 0;
                let mut tick = self.next_after(from - 1)?;
                while tick < to {
                    if count >= cap {
                        return Ok((count, true));
                    }
                    count += 1;
                    tick = self.next_after(tick)?;
                }
                Ok((count, false))
            }
        }
    }

    /// The `count` ticks strictly after `t`.
    pub(crate) fn upcoming(&self, t: i64, count: usize) -> Result<Vec<i64>, TimingError> {
        let mut ticks = Vec::with_capacity(count.min(1_000));
        let mut tick = t;
        for _ in 0..count {
            tick = self.next_after(tick)?;
            ticks.push(tick);
        }
        Ok(ticks)
    }

    /// The timetable in English: `At 02:00 (Europe/Paris)`, `Every 30 seconds`.
    pub(crate) fn describe(&self) -> String {
        match self {
            Timing::Cron(cron) => {
                let text = cron.cron.describe();
                let text = text.trim_end_matches('.');
                format!("{text} ({})", cron.zone)
            }
            Timing::Every { interval, offset } => {
                let every = format!("Every {}", spoken_duration(*interval));
                if *offset == 0 {
                    every
                } else {
                    format!(
                        "{every}, {} after the epoch-aligned instants",
                        spoken_duration(*offset)
                    )
                }
            }
        }
    }

    /// `t` on the timetable's clock: `2026-10-25 01:30:00 +01:00`.
    pub(crate) fn local(&self, t: i64) -> Result<String, TimingError> {
        let zoned = timestamp(t)?.to_zoned(self.time_zone());
        Ok(zoned.strftime("%Y-%m-%d %H:%M:%S %:z").to_string())
    }

    /// For a tick on a day the zone's offset changes (daylight saving time), what changes:
    /// `the clocks change from +01:00 to +00:00 at 2026-10-25 02:00:00 (local time before the
    /// change)`. `None` for intervals and quiet days.
    pub(crate) fn transition_note(&self, t: i64) -> Option<String> {
        let Timing::Cron(cron) = self else {
            return None;
        };
        let at = timestamp(t).ok()?;
        let window = 24 * 3_600;
        let near = |transition: Timestamp| (transition.as_second() - t).abs() <= window;
        let transition = cron
            .tz
            .preceding(at)
            .next()
            .filter(|transition| near(transition.timestamp()))
            .or_else(|| {
                cron.tz
                    .following(at)
                    .next()
                    .filter(|transition| near(transition.timestamp()))
            })?;
        let when = transition.timestamp();
        let before = cron
            .tz
            .to_offset(Timestamp::from_second(when.as_second() - 1).ok()?);
        let after = transition.offset();
        if before == after {
            return None;
        }
        let local_before = when.to_zoned(TimeZone::fixed(before));
        let rule = match (self.is_fixed_time(), after.seconds() > before.seconds()) {
            (true, true) => "a fixed-time tick in the skipped hour runs right after it",
            (true, false) => "a fixed-time tick in the repeated hour runs once",
            (false, true) => "ticks in the skipped hour do not happen",
            (false, false) => "ticks in the repeated hour happen in both passes",
        };
        Some(format!(
            "the clocks change from {} to {} at {} (local time before the change): {rule}",
            offset_text(before),
            offset_text(after),
            local_before.strftime("%Y-%m-%d %H:%M:%S")
        ))
    }
}

impl CronTiming {
    fn zoned(&self, t: i64) -> Result<Zoned, TimingError> {
        Ok(timestamp(t)?.to_zoned(self.tz.clone()))
    }

    /// The first tick strictly after `t`: `croner`'s, with the fixed-time rules applied to the
    /// lists and ranges it treats as real time (see the module's documentation).
    fn next_after(&self, t: i64) -> Result<i64, TimingError> {
        if !self.emulated {
            return self.next_by_croner(t);
        }
        let mut from = t;
        for _ in 0..MAX_ROUNDS {
            let candidate = self.next_by_croner(from)?;
            // A skipped interval before it in which the expression matches runs at its end.
            if let Some(after_gap) = self.skipped_tick(from, candidate)? {
                return Ok(after_gap);
            }
            // The second pass of a repeated interval: a fixed-time tick ran in the first.
            match self.second_pass_end(candidate)? {
                Some(end) => from = end - 1,
                None => return Ok(candidate),
            }
        }
        Err(no_tick(
            &self.expression,
            "after",
            t,
            &CronError::TimeSearchLimitExceeded,
        ))
    }

    /// `croner`'s first tick strictly after `t` (fixed-time rules for single values only), with
    /// the retries past long gaps.
    fn next_by_croner(&self, t: i64) -> Result<i64, TimingError> {
        let start = self.zoned(t)?;
        match self.cron.find_next_occurrence(&start, false) {
            Ok(next) => Ok(next.timestamp().as_second()),
            Err(CronError::TimeSearchLimitExceeded) => self.retry_forward(t),
            Err(error) => Err(no_tick(&self.expression, "after", t, &error)),
        }
    }

    /// The first instant after a skipped interval (clocks moved forward by at most
    /// [`MAX_GAP_SECS`]) in `(from, until]` whose skipped wall clock times include one the
    /// expression matches: where a fixed-time tick of that interval runs.
    fn skipped_tick(&self, from: i64, until: i64) -> Result<Option<i64>, TimingError> {
        for transition in self.tz.following(timestamp(from)?) {
            let at = transition.timestamp().as_second();
            if at > until {
                break;
            }
            if at <= from {
                continue;
            }
            let before = self.tz.to_offset(timestamp(at - 1)?);
            let skipped = i64::from(transition.offset().seconds() - before.seconds());
            if !(1..=MAX_GAP_SECS).contains(&skipped) {
                continue;
            }
            // The skipped wall clock times are [at, at + skipped) on the clock before the change.
            let clock = timestamp(at)?.to_zoned(TimeZone::fixed(before));
            let date_matches = |zoned: &Zoned| {
                let month = u32::try_from(zoned.month()).unwrap_or(0);
                let day = u32::try_from(zoned.day()).unwrap_or(0);
                let weekday = croner::Weekday::from_days_from_sunday(u32::from(
                    zoned.weekday().to_sunday_zero_offset().unsigned_abs(),
                ));
                self.cron.pattern.month_match(month).unwrap_or(false)
                    && self
                        .cron
                        .pattern
                        .day_match(i32::from(zoned.year()), month, day, weekday)
                        .unwrap_or(false)
            };
            let last = timestamp(at + skipped - 1)?.to_zoned(TimeZone::fixed(before));
            if !date_matches(&clock) && !date_matches(&last) {
                continue;
            }
            if let Ok(found) = self.cron.find_next_occurrence(&clock, true)
                && found.timestamp().as_second() < at + skipped
            {
                return Ok(Some(at));
            }
        }
        Ok(None)
    }

    /// When `t` lies in the second pass of a repeated interval (clocks moved back), the end of
    /// that pass (the first instant whose wall clock time was not seen before).
    fn second_pass_end(&self, t: i64) -> Result<Option<i64>, TimingError> {
        let Some(transition) = self.tz.preceding(timestamp(t + 1)?).next() else {
            return Ok(None);
        };
        let at = transition.timestamp().as_second();
        let before = self.tz.to_offset(timestamp(at - 1)?);
        let repeated = i64::from(before.seconds() - transition.offset().seconds());
        if repeated > 0 && t < at + repeated {
            Ok(Some(at + repeated))
        } else {
            Ok(None)
        }
    }

    /// `croner` stops searching a little past a gap; a gap longer than its search (a whole day)
    /// makes it give up. Search again from the zone's transitions after `t`, then from every hour,
    /// for two days, inclusively; the first tick found after `t` is the next one (a tick before
    /// a retry's start would have been found by the search that gave up, which walks the clock
    /// in order). Only a skipped interval explains such a failure: without a transition in those
    /// two days, the search gives up at once.
    fn retry_forward(&self, t: i64) -> Result<i64, TimingError> {
        let start = timestamp(t)?;
        let limit = t + RETRY_HOURS * 3_600;
        let mut starts: Vec<i64> = self
            .tz
            .following(start)
            .map(|transition| transition.timestamp().as_second())
            .take_while(|&at| at <= limit)
            .take(8)
            .collect();
        if starts.is_empty() {
            return Err(no_tick(
                &self.expression,
                "after",
                t,
                &CronError::TimeSearchLimitExceeded,
            ));
        }
        starts.extend((1..=RETRY_HOURS).map(|hours| t + hours * 3_600));
        starts.sort_unstable();
        starts.dedup();
        for from in starts {
            if let Ok(next) = self.cron.find_next_occurrence(&self.zoned(from)?, true) {
                let next = next.timestamp().as_second();
                if next > t {
                    return Ok(next);
                }
            }
        }
        Err(no_tick(
            &self.expression,
            "after",
            t,
            &CronError::TimeSearchLimitExceeded,
        ))
    }

    /// The last tick at or before `t`, as [`CronTiming::next_after`] would produce it: the
    /// backward search proposes a candidate, which counts only if the forward search from just
    /// before it returns it. Both searches follow the Vixie rules, but on days that defeat
    /// `croner` (a whole skipped day) they may disagree, and the forward sequence is the one the
    /// poller fires.
    fn latest_at_or_before(&self, t: i64) -> Result<i64, TimingError> {
        let mut probe = t;
        for _ in 0..MAX_ROUNDS {
            let candidate = self.backward(probe)?;
            let first = self.next_after(candidate - 1)?;
            // With the fixed-time rules applied here, the forward sequence may also hold a tick
            // after the candidate (the end of a skipped interval): walk it.
            if first == candidate && !self.emulated {
                return Ok(candidate);
            }
            if first <= t {
                // The forward sequence has ticks the backward search did not propose: the last
                // of them at or before `t` is the answer.
                let mut latest = first;
                for _ in 0..MAX_ROUNDS {
                    let next = self.next_after(latest)?;
                    if next > t {
                        return Ok(latest);
                    }
                    latest = next;
                }
                return Ok(latest);
            }
            probe = candidate - 1;
        }
        Err(no_tick(
            &self.expression,
            "at or before",
            t,
            &CronError::TimeSearchLimitExceeded,
        ))
    }

    /// `croner`'s backward search (inclusive), with retries past long gaps.
    fn backward(&self, t: i64) -> Result<i64, TimingError> {
        let start = self.zoned(t)?;
        match self.cron.find_previous_occurrence(&start, true) {
            Ok(previous) => Ok(previous.timestamp().as_second()),
            Err(CronError::TimeSearchLimitExceeded) => self.retry_backward(t),
            Err(error) => Err(no_tick(&self.expression, "at or before", t, &error)),
        }
    }

    /// The backward counterpart of [`CronTiming::retry_forward`]: from the instant before each
    /// of the zone's transitions before `t`, then from every hour, for two days (none without
    /// a transition).
    fn retry_backward(&self, t: i64) -> Result<i64, TimingError> {
        let start = timestamp(t)?;
        let limit = t - RETRY_HOURS * 3_600;
        let mut starts: Vec<i64> = self
            .tz
            .preceding(start)
            .map(|transition| transition.timestamp().as_second() - 1)
            .take_while(|&at| at >= limit)
            .take(8)
            .collect();
        if starts.is_empty() {
            return Err(no_tick(
                &self.expression,
                "at or before",
                t,
                &CronError::TimeSearchLimitExceeded,
            ));
        }
        starts.extend((1..=RETRY_HOURS).map(|hours| t - hours * 3_600));
        starts.sort_unstable_by(|a, b| b.cmp(a));
        starts.dedup();
        for from in starts {
            if let Ok(previous) = self.cron.find_previous_occurrence(&self.zoned(from)?, true) {
                let previous = previous.timestamp().as_second();
                if previous <= t {
                    return Ok(previous);
                }
            }
        }
        Err(no_tick(
            &self.expression,
            "at or before",
            t,
            &CronError::TimeSearchLimitExceeded,
        ))
    }
}

fn no_tick(expression: &str, relation: &str, t: i64, error: &CronError) -> TimingError {
    let reason = match error {
        CronError::TimeSearchLimitExceeded => "no matching time found".to_owned(),
        other => other.to_string(),
    };
    TimingError(format!(
        "`{expression}` has no tick {relation} {}: {reason}",
        Timestamp::from_second(t).map_or_else(|_| t.to_string(), |at| at.to_string())
    ))
}

/// An offset as `+01:00` (`-09:30`, `+05:45`).
fn offset_text(offset: jiff::tz::Offset) -> String {
    let seconds = offset.seconds();
    let sign = if seconds < 0 { '-' } else { '+' };
    let minutes = seconds.unsigned_abs() / 60;
    format!("{sign}{:02}:{:02}", minutes / 60, minutes % 60)
}

/// `seconds` in words: `30 seconds`, `5 minutes`, `1 hour`, `7 days`.
pub(crate) fn spoken_duration(seconds: i64) -> String {
    let units = [
        (86_400, "day"),
        (3_600, "hour"),
        (60, "minute"),
        (1, "second"),
    ];
    for (size, unit) in units {
        if seconds >= size && seconds % size == 0 {
            let count = seconds / size;
            let plural = if count == 1 { "" } else { "s" };
            return format!("{count} {unit}{plural}");
        }
    }
    format!("{seconds} seconds")
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// Epoch seconds of `text` (`2026-10-25T00:30:00Z`).
    fn at(text: &str) -> i64 {
        text.parse::<Timestamp>().unwrap().as_second()
    }

    /// `t` in the zone, as `2026-10-25T01:30:00+01:00`.
    fn local(timing: &Timing, t: i64) -> String {
        Timestamp::from_second(t)
            .unwrap()
            .to_zoned(timing.time_zone())
            .strftime("%Y-%m-%dT%H:%M:%S%:z")
            .to_string()
    }

    fn ticks(timing: &Timing, from: &str, count: usize) -> Vec<String> {
        timing
            .upcoming(at(from), count)
            .unwrap()
            .into_iter()
            .map(|t| local(timing, t))
            .collect()
    }

    fn cron(expression: &str, tz: &str) -> Timing {
        Timing::cron(expression, tz).unwrap_or_else(|error| panic!("{expression}: {error}"))
    }

    fn refused(expression: &str) -> String {
        match Timing::cron(expression, "UTC") {
            Ok(_) => panic!("{expression} was accepted"),
            Err(error) => error.0,
        }
    }

    #[test]
    fn the_dialect_accepts_five_and_six_fields_aliases_names_and_extensions() {
        for expression in [
            "* * * * *",
            "*/15 * * * * *",
            "0 2 * * *",
            "0,30 9-17 * * MON-FRI",
            "5 4 * jan,Jul sun",
            "0 0 * * 7",
            "0 0 ? * *",
            "0 0 L * *",
            "0 9 * * 5#L",
            "0 9 * * 5L",
            "0 9 15W * *",
            "0 9 * * 5#2",
            "0 0 13 * +FRI",
            "10-50/10 * * * *",
            "@yearly",
            "@annually",
            "@MONTHLY",
            "@weekly",
            "@daily",
            "@midnight",
            "@hourly",
            "  0   2  *  *  *  ",
        ] {
            cron(expression, "UTC");
        }
        assert_eq!(
            cron("  0   2  *  *  *  ", "UTC").expression(),
            Some("0 2 * * *")
        );
        // `@midnight` is `@daily`, stored as written.
        let midnight = cron("@midnight", "UTC");
        assert_eq!(midnight.expression(), Some("@midnight"));
        assert_eq!(
            ticks(&midnight, "2026-03-01T10:00:00Z", 2),
            ["2026-03-02T00:00:00+00:00", "2026-03-03T00:00:00+00:00"]
        );
    }

    #[test]
    fn the_dialect_refuses_what_it_does_not_support() {
        for (expression, reason) in [
            ("", "empty"),
            ("* * * *", "4 fields"),
            ("0 0 0 1 1 * 2030", "year field"),
            ("0 0 0 1 1 * 2030 x", "8 fields"),
            ("60 * * * *", "invalid cron expression"),
            ("0 25 * * *", "invalid cron expression"),
            ("0 0 * * 8", "invalid cron expression"),
            ("*/0 * * * *", "invalid cron expression"),
            ("0/10 * * * *", "invalid cron expression"),
            ("@reboot", "@reboot"),
            ("@every 5m", "Periodic::every"),
            ("@often", "unknown alias"),
            ("0 0 30 2 *", "never ticks"),
            ("0 0 31 4 *", "never ticks"),
        ] {
            let error = refused(expression);
            assert!(error.contains(reason), "{expression}: {error}");
        }
        assert!(
            Timing::cron("0 2 * * *", "Mars/Olympus")
                .unwrap_err()
                .0
                .contains("unknown time zone")
        );
        assert!(
            Timing::cron("0 2 * * *", "")
                .unwrap_err()
                .0
                .contains("empty")
        );
        assert!(Timing::cron("0 2 * * *", " UTC").is_err());
    }

    #[test]
    fn impossible_dates_are_refused_quickly() {
        // periodic-6: `croner` searched a long way before giving up on an impossible date, and
        // the retries past long gaps repeated that search 49 times (0.5 to 2 s per validation
        // in a release build, blocking the calling thread). The day and month fields are now
        // checked against the dates first.
        for (expression, zone) in [
            ("0 0 30 2 *", "UTC"),
            ("0 0 31 4 *", "UTC"),
            ("0 0 31 2,4,6,9,11 *", "UTC"),
            ("0 0 30 2 *", "America/New_York"),
            ("* * 30 2 *", "UTC"),
            ("*/5 * 31 2,4,6,9,11 *", "Europe/London"),
            ("0 0 0 30 2 *", "UTC"),
            ("0 0 30 2 +MON", "UTC"),
        ] {
            let started = std::time::Instant::now();
            let error = Timing::cron(expression, zone).unwrap_err();
            assert!(error.0.contains("never ticks"), "{expression}: {error}");
            assert!(
                started.elapsed() < std::time::Duration::from_millis(250),
                "{expression}: {:?}",
                started.elapsed()
            );
        }
        // Rare dates are found: the 29th of February on a Monday, the fifth Friday, the last
        // weekday.
        assert_eq!(
            ticks(&cron("0 0 29 2 +MON", "UTC"), "2026-11-02T00:00:00Z", 1),
            ["2044-02-29T00:00:00+00:00"]
        );
        assert_eq!(
            ticks(&cron("0 0 * 2 5#5", "UTC"), "2026-11-02T00:00:00Z", 1),
            ["2036-02-29T00:00:00+00:00"]
        );
        assert_eq!(
            ticks(&cron("0 0 LW * *", "UTC"), "2026-11-02T00:00:00Z", 1),
            ["2026-11-30T00:00:00+00:00"]
        );
        // A leap day, in a zone without transitions (where a failed search would not be tried
        // again).
        let utc = cron("0 0 29 2 *", "UTC");
        assert_eq!(
            ticks(&utc, "2026-11-02T00:00:00Z", 1),
            ["2028-02-29T00:00:00+00:00"]
        );
    }

    #[test]
    fn zones_missing_from_the_system_database_come_from_the_bundled_copy() {
        // periodic-3: `jiff` reads only the system database when there is one, so a host whose
        // tzdata predates a zone did not know it at all.
        let empty = jiff::tz::TimeZoneDatabase::none();
        let paris = zone_in("Europe/Paris", &empty).unwrap();
        assert_eq!(
            paris,
            jiff::tz::TimeZoneDatabase::bundled()
                .get("Europe/Paris")
                .unwrap()
        );
        assert!(
            zone_in("America/Brand_New_Zone", &empty)
                .unwrap_err()
                .0
                .contains("unknown time zone")
        );
        assert!(unknown_zone("America/Brand_New_Zone"));
        assert!(!unknown_zone("Europe/Paris"));
        // Malformed names are invalid, not unknown.
        assert!(!unknown_zone(" UTC"));
        assert!(!unknown_zone(""));
    }

    #[test]
    fn intervals_are_aligned_to_the_epoch() {
        let every = Timing::every(30, 5).unwrap();
        assert_eq!(every.next_after(0).unwrap(), 5);
        assert_eq!(every.next_after(5).unwrap(), 35);
        assert_eq!(every.next_after(34).unwrap(), 35);
        assert_eq!(every.latest_at_or_before(35).unwrap(), 35);
        assert_eq!(every.latest_at_or_before(64).unwrap(), 35);
        assert_eq!(every.latest_at_or_before(4).unwrap(), -25);
        assert_eq!(every.count(5, 65, 0).unwrap(), (2, false));
        assert_eq!(every.count(6, 66, 0).unwrap(), (2, false));
        assert_eq!(every.count(6, 6, 10).unwrap(), (0, false));
        // Every week ticks on Thursdays (the epoch was one).
        let weekly = Timing::every(7 * 86_400, 0).unwrap();
        let tick = weekly.next_after(at("2026-11-01T00:00:00Z")).unwrap();
        assert_eq!(local(&weekly, tick), "2026-11-05T00:00:00+00:00");
        assert!(
            Timing::every(0, 0)
                .unwrap_err()
                .0
                .contains("at least one second")
        );
        assert!(Timing::every(30, 30).unwrap_err().0.contains("offset"));
        assert!(Timing::every(MAX_INTERVAL_SECS + 1, 0).is_err());
        assert_eq!(
            every.describe(),
            "Every 30 seconds, 5 seconds after the epoch-aligned instants"
        );
        assert_eq!(Timing::every(3_600, 0).unwrap().describe(), "Every 1 hour");
        assert_eq!(Timing::every(90, 0).unwrap().describe(), "Every 90 seconds");
    }

    #[test]
    fn previous_and_next_ticks() {
        // Previous minute, hour and day (sidekiq-cron "cron test"; Oban's last_at/next_at).
        let minutely = cron("* * * * *", "UTC");
        let now = at("2026-05-04T10:20:30Z");
        assert_eq!(
            minutely.latest_at_or_before(now).unwrap(),
            at("2026-05-04T10:20:00Z")
        );
        assert_eq!(
            minutely.next_after(now).unwrap(),
            at("2026-05-04T10:21:00Z")
        );
        let hourly = cron("0 * * * *", "UTC");
        assert_eq!(
            hourly.latest_at_or_before(now).unwrap(),
            at("2026-05-04T10:00:00Z")
        );
        assert_eq!(hourly.next_after(now).unwrap(), at("2026-05-04T11:00:00Z"));
        let daily = cron("0 0 * * *", "UTC");
        assert_eq!(
            daily.latest_at_or_before(now).unwrap(),
            at("2026-05-04T00:00:00Z")
        );
        assert_eq!(daily.next_after(now).unwrap(), at("2026-05-05T00:00:00Z"));
        // A tick is its own latest tick, and not its own next one.
        let tick = at("2026-05-04T10:00:00Z");
        assert_eq!(hourly.latest_at_or_before(tick).unwrap(), tick);
        assert_eq!(hourly.next_after(tick).unwrap(), at("2026-05-04T11:00:00Z"));
        // Seconds.
        let fast = cron("*/15 * * * * *", "UTC");
        assert_eq!(fast.next_after(now).unwrap(), at("2026-05-04T10:20:45Z"));
        // Counting.
        assert_eq!(
            hourly
                .count(at("2026-05-04T10:00:00Z"), at("2026-05-04T13:00:00Z"), 100)
                .unwrap(),
            (3, false)
        );
        assert_eq!(
            minutely.count(0, at("2026-05-04T00:00:00Z"), 5).unwrap(),
            (5, true)
        );
    }

    #[test]
    fn day_rules_are_posix_or_and_plus_is_and() {
        // Day 13 or a Friday.
        let either = cron("0 0 13 * FRI", "UTC");
        assert_eq!(
            ticks(&either, "2026-02-01T00:00:00Z", 3),
            [
                "2026-02-06T00:00:00+00:00",
                "2026-02-13T00:00:00+00:00",
                "2026-02-20T00:00:00+00:00"
            ]
        );
        // Day 13 when it is a Friday.
        let both = cron("0 0 13 * +FRI", "UTC");
        assert_eq!(
            ticks(&both, "2026-01-01T00:00:00Z", 2),
            ["2026-02-13T00:00:00+00:00", "2026-03-13T00:00:00+00:00"]
        );
    }

    #[test]
    fn rare_timetables_tick_on_the_right_days() {
        assert_eq!(
            ticks(&cron("0 0 L * *", "UTC"), "2026-01-15T00:00:00Z", 3),
            [
                "2026-01-31T00:00:00+00:00",
                "2026-02-28T00:00:00+00:00",
                "2026-03-31T00:00:00+00:00"
            ]
        );
        assert_eq!(
            ticks(&cron("0 9 * * 5#L", "UTC"), "2026-01-01T00:00:00Z", 2),
            ["2026-01-30T09:00:00+00:00", "2026-02-27T09:00:00+00:00"]
        );
        // "Repeat the 7th day of every month at 9:25" (BullMQ).
        assert_eq!(
            ticks(&cron("25 9 7 * *", "UTC"), "2026-11-01T00:00:00Z", 3),
            [
                "2026-11-07T09:25:00+00:00",
                "2026-12-07T09:25:00+00:00",
                "2027-01-07T09:25:00+00:00"
            ]
        );
        assert_eq!(
            ticks(&cron("0 0 29 2 *", "UTC"), "2026-03-01T00:00:00Z", 1),
            ["2028-02-29T00:00:00+00:00"]
        );
    }

    #[test]
    fn dst_in_london() {
        // Fixed-time in the fall-back fold: once, in the first pass.
        let fixed = cron("30 1 * * *", "Europe/London");
        assert_eq!(
            ticks(&fixed, "2026-10-23T12:00:00Z", 3),
            [
                "2026-10-24T01:30:00+01:00",
                "2026-10-25T01:30:00+01:00",
                "2026-10-26T01:30:00+00:00"
            ]
        );
        // Wildcard in the fold: both passes.
        let wildcard = cron("*/30 1 * * *", "Europe/London");
        assert_eq!(
            ticks(&wildcard, "2026-10-24T12:00:00Z", 4),
            [
                "2026-10-25T01:00:00+01:00",
                "2026-10-25T01:30:00+01:00",
                "2026-10-25T01:00:00+00:00",
                "2026-10-25T01:30:00+00:00"
            ]
        );
        // Fixed-time in the spring-forward gap: the first instant after it.
        assert_eq!(
            ticks(&fixed, "2026-03-28T12:00:00Z", 2),
            ["2026-03-29T02:00:00+01:00", "2026-03-30T01:30:00+01:00"]
        );
        // Wildcard in the gap: no tick that day.
        let gap = cron("*/20 1 * * *", "Europe/London");
        assert_eq!(
            ticks(&gap, "2026-03-28T12:00:00Z", 4),
            [
                "2026-03-30T01:00:00+01:00",
                "2026-03-30T01:20:00+01:00",
                "2026-03-30T01:40:00+01:00",
                "2026-03-31T01:00:00+01:00"
            ]
        );
        // The latest tick across the fold agrees with the forward sequence.
        assert_eq!(
            local(
                &fixed,
                fixed
                    .latest_at_or_before(at("2026-10-25T01:40:00Z"))
                    .unwrap()
            ),
            "2026-10-25T01:30:00+01:00"
        );
        assert_eq!(
            local(
                &wildcard,
                wildcard
                    .latest_at_or_before(at("2026-10-25T01:40:00Z"))
                    .unwrap()
            ),
            "2026-10-25T01:30:00+00:00"
        );
        assert_eq!(
            local(
                &wildcard,
                wildcard
                    .latest_at_or_before(at("2026-10-25T00:59:00Z"))
                    .unwrap()
            ),
            "2026-10-25T01:30:00+01:00"
        );
        assert!(fixed.is_fixed_time());
        assert!(!wildcard.is_fixed_time());
    }

    #[test]
    fn fixed_time_is_vixie_crons_rule() {
        // A field starting with `*` follows real time (cronie's MIN_STAR/HR_STAR); single
        // values, lists, ranges and `a-b/n` steps are fixed-time, the seconds field too.
        for (expression, fixed) in [
            ("30 1 * * *", true),
            ("30 1,13 * * *", true),
            ("15 1-3 * * *", true),
            ("0-30/10 1 * * *", true),
            ("0,30 9-17 * * MON-FRI", true),
            ("0 30 1 * * *", true),
            ("0,30 30 1 * * *", true),
            ("@daily", true),
            ("@midnight", true),
            ("@weekly", true),
            ("*/30 1 * * *", false),
            ("30 * * * *", false),
            ("30 */2 * * *", false),
            ("* 30 1 * * *", false),
            ("*/15 * * * * *", false),
            ("@hourly", false),
        ] {
            assert_eq!(
                cron(expression, "UTC").is_fixed_time(),
                fixed,
                "{expression}"
            );
        }
    }

    #[test]
    fn lists_and_ranges_follow_the_fixed_time_rules_across_dst() {
        // periodic-1: `croner` treats only single values as fixed-time, so lists and ranges ran
        // twice in a repeated hour and not at all in a skipped one.
        // London falls back on 2026-10-25: once, in the first pass.
        let list = cron("30 1,13 * * *", "Europe/London");
        assert_eq!(
            ticks(&list, "2026-10-24T22:00:00Z", 3),
            [
                "2026-10-25T01:30:00+01:00",
                "2026-10-25T13:30:00+00:00",
                "2026-10-26T01:30:00+00:00"
            ]
        );
        let range = cron("15 1-3 * * *", "Europe/London");
        assert_eq!(
            ticks(&range, "2026-10-24T22:00:00Z", 4),
            [
                "2026-10-25T01:15:00+01:00",
                "2026-10-25T02:15:00+00:00",
                "2026-10-25T03:15:00+00:00",
                "2026-10-26T01:15:00+00:00"
            ]
        );
        let every_minute = cron("0-59 1 * * *", "Europe/London");
        assert_eq!(
            every_minute
                .count(
                    at("2026-10-24T23:00:00Z"),
                    at("2026-10-25T03:00:00Z"),
                    1_000
                )
                .unwrap(),
            (60, false),
            "each wall clock minute once"
        );
        // London springs forward on 2026-03-29: right after the gap, once.
        assert_eq!(
            ticks(&list, "2026-03-28T22:00:00Z", 3),
            [
                "2026-03-29T02:00:00+01:00",
                "2026-03-29T13:30:00+01:00",
                "2026-03-30T01:30:00+01:00"
            ]
        );
        let half_hours = cron("0,30 1 * * *", "Europe/London");
        assert_eq!(
            ticks(&half_hours, "2026-03-28T22:00:00Z", 3),
            [
                "2026-03-29T02:00:00+01:00",
                "2026-03-30T01:00:00+01:00",
                "2026-03-30T01:30:00+01:00"
            ]
        );
        // New York springs forward on 2026-03-08: 02:00 runs at 03:00.
        let new_york = cron("0 2,14 * * *", "America/New_York");
        assert_eq!(
            ticks(&new_york, "2026-03-08T00:00:00Z", 2),
            ["2026-03-08T03:00:00-04:00", "2026-03-08T14:00:00-04:00"]
        );
        // Santiago skips midnight on 2026-09-06: 00:00 runs at 01:00, like a single value.
        let santiago = cron("0 0,12 * * *", "America/Santiago");
        let single = cron("0 0 * * *", "America/Santiago");
        assert_eq!(
            local(
                &single,
                single.next_after(at("2026-09-05T17:00:00Z")).unwrap()
            ),
            "2026-09-06T01:00:00-03:00"
        );
        assert_eq!(
            ticks(&santiago, "2026-09-05T17:00:00Z", 2),
            ["2026-09-06T01:00:00-03:00", "2026-09-06T12:00:00-03:00"]
        );
        // The latest tick agrees with the forward sequence across both changes.
        assert_eq!(
            local(
                &list,
                list.latest_at_or_before(at("2026-10-25T01:40:00Z"))
                    .unwrap()
            ),
            "2026-10-25T01:30:00+01:00"
        );
        assert_eq!(
            local(
                &list,
                list.latest_at_or_before(at("2026-03-29T05:00:00Z"))
                    .unwrap()
            ),
            "2026-03-29T02:00:00+01:00"
        );
        assert_eq!(
            local(
                &new_york,
                new_york
                    .latest_at_or_before(at("2026-03-08T07:00:00Z"))
                    .unwrap()
            ),
            "2026-03-08T03:00:00-04:00"
        );
        // Counting counts each run once.
        assert_eq!(
            range
                .count(at("2026-10-24T22:00:00Z"), at("2026-10-25T04:00:00Z"), 100)
                .unwrap(),
            (3, false)
        );
        // The notes describe what happens.
        assert!(list.is_fixed_time() && range.is_fixed_time());
        let note = list.transition_note(at("2026-10-25T00:30:00Z")).unwrap();
        assert!(note.ends_with("runs once"), "{note}");
    }

    /// The runs of Vixie cron for a fixed-time expression whose seconds are 0, minute by minute
    /// over `[start, end]`: each instant whose wall clock `matches`, except in the second pass
    /// of a repeated interval, plus the first instant after a skipped interval (of at most two
    /// hours) in which a wall clock minute matches.
    fn vixie(tz: &TimeZone, matches: fn(i8, i8) -> bool, start: i64, end: i64) -> Vec<i64> {
        let mut runs = Vec::new();
        for t in (start..=end).step_by(60) {
            let zoned = Timestamp::from_second(t).unwrap().to_zoned(tz.clone());
            let later_pass = matches!(
                tz.to_ambiguous_timestamp(zoned.datetime()).offset(),
                jiff::tz::AmbiguousOffset::Fold { after, .. } if after == zoned.offset()
            );
            if !later_pass && matches(zoned.hour(), zoned.minute()) {
                runs.push(t);
            }
        }
        for transition in tz.following(Timestamp::from_second(start - 1).unwrap()) {
            let g = transition.timestamp().as_second();
            if g > end {
                break;
            }
            let before = tz.to_offset(Timestamp::from_second(g - 1).unwrap());
            let skipped = i64::from(transition.offset().seconds() - before.seconds());
            if !(1..=7_200).contains(&skipped) {
                continue;
            }
            let clock = TimeZone::fixed(before);
            let in_gap = (g..g + skipped).step_by(60).any(|x| {
                let wall = Timestamp::from_second(x).unwrap().to_zoned(clock.clone());
                matches(wall.hour(), wall.minute())
            });
            if in_gap {
                runs.push(g);
            }
        }
        runs.sort_unstable();
        runs.dedup();
        runs
    }

    /// Lists and ranges against a brute-force model of Vixie cron, minute by minute, around each
    /// DST change of 2026 in several zones.
    #[test]
    fn lists_and_ranges_match_vixie_cron_minute_by_minute() {
        type Case = (&'static str, &'static str, &'static str, fn(i8, i8) -> bool);
        let cases: [Case; 8] = [
            (
                "30 1,13 * * *",
                "Europe/London",
                "2026-03-28T00:00:00Z",
                |h, m| (h == 1 || h == 13) && m == 30,
            ),
            (
                "30 1,13 * * *",
                "Europe/London",
                "2026-10-24T00:00:00Z",
                |h, m| (h == 1 || h == 13) && m == 30,
            ),
            (
                "0,20,40 2-3 * * *",
                "Europe/Paris",
                "2026-03-28T00:00:00Z",
                |h, m| (2..=3).contains(&h) && m % 20 == 0,
            ),
            (
                "0,20,40 2-3 * * *",
                "Europe/Paris",
                "2026-10-24T00:00:00Z",
                |h, m| (2..=3).contains(&h) && m % 20 == 0,
            ),
            (
                "0 2,14 * * *",
                "America/New_York",
                "2026-03-07T00:00:00Z",
                |h, m| (h == 2 || h == 14) && m == 0,
            ),
            (
                "15 0-1 * * *",
                "America/New_York",
                "2026-10-31T00:00:00Z",
                |h, m| h <= 1 && m == 15,
            ),
            (
                "0,45 0 * * *",
                "America/Santiago",
                "2026-09-05T00:00:00Z",
                |h, m| h == 0 && (m == 0 || m == 45),
            ),
            (
                "10,50 2 * * *",
                "Australia/Lord_Howe",
                "2026-10-03T00:00:00Z",
                |h, m| h == 2 && (m == 10 || m == 50),
            ),
        ];
        for (expression, zone, from, matches) in cases {
            let timing = cron(expression, zone);
            let start = at(from);
            let end = start + 2 * 86_400;
            let expected = vixie(&timing.time_zone(), matches, start, end);
            assert_eq!(
                iterate(&timing, start, end),
                expected,
                "{expression} in {zone} from {from}"
            );
            for &run in &expected {
                assert_eq!(
                    timing.latest_at_or_before(run).unwrap(),
                    run,
                    "{expression}"
                );
                assert_eq!(
                    timing.latest_at_or_before(run + 59).unwrap(),
                    run,
                    "{expression}"
                );
            }
        }
    }

    #[test]
    fn dst_in_new_york() {
        let fixed = cron("30 2 * * *", "America/New_York");
        assert_eq!(
            ticks(&fixed, "2026-03-07T12:00:00Z", 2),
            ["2026-03-08T03:00:00-04:00", "2026-03-09T02:30:00-04:00"]
        );
        let fold = cron("30 1 * * *", "America/New_York");
        assert_eq!(
            ticks(&fold, "2026-10-31T12:00:00Z", 2),
            ["2026-11-01T01:30:00-04:00", "2026-11-02T01:30:00-05:00"]
        );
        let hourly = cron("0 * * * *", "America/New_York");
        assert_eq!(
            ticks(&hourly, "2026-11-01T04:30:00Z", 3),
            [
                "2026-11-01T01:00:00-04:00",
                "2026-11-01T01:00:00-05:00",
                "2026-11-01T02:00:00-05:00"
            ]
        );
    }

    #[test]
    fn dst_with_odd_offsets() {
        // Lord Howe moves its clocks by 30 minutes.
        let howe = cron("15 2 * * *", "Australia/Lord_Howe");
        assert_eq!(
            ticks(&howe, "2026-10-03T00:00:00Z", 1),
            ["2026-10-04T02:30:00+11:00"]
        );
        // Kathmandu is at +05:45.
        let kathmandu = cron("0 9 * * *", "Asia/Kathmandu");
        assert_eq!(
            ticks(&kathmandu, "2026-01-01T00:00:00Z", 1),
            ["2026-01-01T09:00:00+05:45"]
        );
    }

    #[test]
    fn a_skipped_day_is_searched_past() {
        // Pacific/Apia skipped 2011-12-30 entirely; croner gives up, the retry does not.
        let noon = cron("0 12 * * *", "Pacific/Apia");
        let from = at("2011-12-29T22:00:00Z"); // 12:00 on the 29th, at -10:00.
        assert_eq!(
            local(&noon, noon.next_after(from).unwrap()),
            "2011-12-31T12:00:00+14:00"
        );
        let midnight = cron("0 0 * * *", "Pacific/Apia");
        assert_eq!(
            local(&midnight, midnight.next_after(from).unwrap()),
            "2011-12-31T00:00:00+14:00"
        );
        let hourly = cron("0 * * * *", "Pacific/Apia");
        assert_eq!(
            local(
                &hourly,
                hourly.next_after(at("2011-12-30T09:30:00Z")).unwrap()
            ),
            "2011-12-31T00:00:00+14:00"
        );
        // Backwards too: from 11:59:59 on the 31st, the latest noon is the 29th's.
        let latest = noon
            .latest_at_or_before(at("2011-12-30T21:59:59Z"))
            .unwrap();
        assert_eq!(local(&noon, latest), "2011-12-29T12:00:00-10:00");
        let latest = noon
            .latest_at_or_before(at("2011-12-31T00:00:00Z"))
            .unwrap();
        assert_eq!(local(&noon, latest), "2011-12-31T12:00:00+14:00");
    }

    #[test]
    fn transitions_are_noted() {
        let london = cron("30 1 * * *", "Europe/London");
        let note = london.transition_note(at("2026-10-25T00:30:00Z")).unwrap();
        assert!(note.contains("+01:00 to +00:00"), "{note}");
        assert!(note.contains("2026-10-25 02:00:00"), "{note}");
        assert_eq!(london.transition_note(at("2026-07-01T00:30:00Z")), None);
        assert_eq!(Timing::every(60, 0).unwrap().transition_note(0), None);
        assert_eq!(
            london.local(at("2026-07-01T00:30:00Z")).unwrap(),
            "2026-07-01 01:30:00 +01:00"
        );
        let description = cron("0 2 * * *", "Europe/Paris").describe();
        assert!(description.ends_with("(Europe/Paris)"), "{description}");
    }

    /// The ticks in `[from, to]`, by iterating `next_after`.
    fn iterate(timing: &Timing, from: i64, to: i64) -> Vec<i64> {
        let mut found = Vec::new();
        let mut tick = timing.next_after(from - 1).unwrap();
        while tick <= to {
            found.push(tick);
            tick = timing.next_after(tick).unwrap();
        }
        found
    }

    fn expressions() -> impl Strategy<Value = String> {
        let minute = prop_oneof![
            Just("*".to_owned()),
            (0..60u8).prop_map(|m| m.to_string()),
            (1..30u8).prop_map(|s| format!("*/{s}")),
            (0..30u8).prop_map(|m| format!("{m},{}", m + 30)),
            Just("0-45/15".to_owned())
        ];
        let hour = prop_oneof![
            Just("*".to_owned()),
            (0..24u8).prop_map(|h| h.to_string()),
            (1..12u8).prop_map(|s| format!("*/{s}")),
            Just("1-3".to_owned()),
            (0..12u8).prop_map(|h| format!("{h},{}", h + 12)),
            Just("0-2,13".to_owned())
        ];
        let dom = prop_oneof![
            Just("*".to_owned()),
            (1..29u8).prop_map(|d| d.to_string()),
            Just("L".to_owned())
        ];
        let dow = prop_oneof![
            Just("*".to_owned()),
            (0..7u8).prop_map(|d| d.to_string()),
            Just("MON-FRI".to_owned())
        ];
        (minute, hour, dom, dow).prop_map(|(m, h, d, w)| format!("{m} {h} {d} * {w}"))
    }

    fn zones() -> impl Strategy<Value = &'static str> {
        prop_oneof![
            Just("UTC"),
            Just("Europe/London"),
            Just("America/New_York"),
            Just("Australia/Lord_Howe"),
            Just("Asia/Kathmandu"),
            Just("Europe/Paris"),
            Just("America/Santiago"),
        ]
    }

    /// Cases per property: 64, or `PERIODIC_PROPTEST_CASES` for longer runs.
    fn cases() -> u32 {
        std::env::var("PERIODIC_PROPTEST_CASES")
            .ok()
            .and_then(|cases| cases.parse().ok())
            .unwrap_or(64)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(cases()))]

        /// `next_after` moves forward and `latest_at_or_before` agrees with it: the latest tick
        /// at or before `t` is followed by the next tick after `t` (no tick hides between them,
        /// and the backward search never returns an instant the forward one skips, such as the
        /// second pass of a fixed-time tick in a fold).
        #[test]
        fn ticks_are_consistent(
            expression in expressions(),
            zone in zones(),
            // 2026, including both DST changes of the zones above.
            t in 1_767_225_600_i64..1_798_761_600,
        ) {
            let timing = Timing::cron(&expression, zone).unwrap();
            let next = timing.next_after(t).unwrap();
            prop_assert!(next > t);
            prop_assert_eq!(timing.latest_at_or_before(next).unwrap(), next);
            let latest = timing.latest_at_or_before(t).unwrap();
            prop_assert!(latest <= t);
            prop_assert_eq!(timing.next_after(latest).unwrap(), next);
            let following = timing.next_after(next).unwrap();
            prop_assert!(following > next);
            prop_assert_eq!(timing.latest_at_or_before(following - 1).unwrap(), next);
            // Counting matches iterating.
            let iterated = iterate(&timing, latest, following).len() as u64;
            prop_assert_eq!(timing.count(latest, following + 1, MISSED_CAP).unwrap(), (iterated, false));
        }

        /// Intervals: arithmetic against brute force over a small window.
        #[test]
        fn intervals_match_brute_force(interval in 1u64..500, offset_seed in 0u64..500, t in -10_000i64..10_000) {
            let offset = offset_seed % interval;
            let timing = Timing::every(interval, offset).unwrap();
            let is_tick = |x: i64| (x - offset as i64).rem_euclid(interval as i64) == 0;
            let next = timing.next_after(t).unwrap();
            prop_assert!(next > t && is_tick(next));
            prop_assert!((t + 1..next).all(|x| !is_tick(x)));
            let latest = timing.latest_at_or_before(t).unwrap();
            prop_assert!(latest <= t && is_tick(latest));
            prop_assert!((latest + 1..=t).all(|x| !is_tick(x)));
            let to = t + 1_000;
            let brute = (t..to).filter(|&x| is_tick(x)).count() as u64;
            prop_assert_eq!(timing.count(t, to, 0).unwrap(), (brute, false));
        }
    }

    /// An exhaustive check of a step expression around each DST change of 2026 in London and New
    /// York: over two days, the ticks `next_after` finds are exactly the instants (checked every
    /// minute) whose wall clock matches, so none in a gap and both passes of a fold.
    #[test]
    fn no_tick_hides_between_a_time_and_its_next_tick() {
        for (expression, zone, from) in [
            ("*/10 1-2 * * *", "Europe/London", "2026-03-28T00:00:00Z"),
            ("*/10 1-2 * * *", "Europe/London", "2026-10-24T00:00:00Z"),
            ("*/10 1-2 * * *", "America/New_York", "2026-03-07T00:00:00Z"),
            ("*/10 1-2 * * *", "America/New_York", "2026-10-31T00:00:00Z"),
        ] {
            let timing = cron(expression, zone);
            let start = at(from);
            let end = start + 2 * 86_400;
            let found = iterate(&timing, start, end);
            // Every minute of the window whose wall clock matches `*/10 1-2`.
            let tz = timing.time_zone();
            let expected: Vec<i64> = (start..=end)
                .step_by(60)
                .filter(|&t| {
                    let zoned = Timestamp::from_second(t).unwrap().to_zoned(tz.clone());
                    (1..=2).contains(&zoned.hour())
                        && zoned.minute() % 10 == 0
                        && zoned.second() == 0
                })
                .collect();
            assert_eq!(found, expected, "{expression} in {zone} from {from}");
        }
    }

    #[test]
    fn an_every_second_cron_counts_up_to_the_cap_quickly() {
        let timing = cron("* * * * * *", "UTC");
        let start = std::time::Instant::now();
        let week = 7 * 86_400;
        assert_eq!(
            timing.count(0, week, MISSED_CAP).unwrap(),
            (MISSED_CAP, true)
        );
        assert!(start.elapsed() < std::time::Duration::from_secs(5));
    }
}
