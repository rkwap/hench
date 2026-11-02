//! The texts of the Limits tab and pages.

use std::time::Duration;

use hench::limit::LimitKind;

use crate::flash::{Notice, NoticeKind};
use crate::format;

/// The tab's name.
pub(crate) const TAB: &str = "Limits";

/// The fixed texts of the Limits pages.
pub(crate) struct Texts {
    pub(crate) heading: &'static str,
    pub(crate) limit: &'static str,
    pub(crate) kind: &'static str,
    pub(crate) spec: &'static str,
    pub(crate) usage: &'static str,
    pub(crate) next_free: &'static str,
    pub(crate) granted: &'static str,
    pub(crate) refused: &'static str,
    pub(crate) wait_line: &'static str,
    pub(crate) last_used: &'static str,
    pub(crate) sort_by: &'static str,
    pub(crate) sort_last: &'static str,
    pub(crate) sort_name: &'static str,
    pub(crate) sort_refused: &'static str,
    pub(crate) empty: &'static str,
    pub(crate) no_matches: &'static str,
    pub(crate) unknown_spec: &'static str,
    pub(crate) most_refused: &'static str,
    pub(crate) throttles: &'static str,
    pub(crate) throttle: &'static str,
    pub(crate) processes: &'static str,
    pub(crate) differs: &'static str,
    pub(crate) reset: &'static str,
    pub(crate) reset_selected: &'static str,
    pub(crate) clear_line: &'static str,
    pub(crate) confirm_reset: &'static str,
    pub(crate) confirm_reset_selected: &'static str,
    pub(crate) select_limit: &'static str,
    pub(crate) name: &'static str,
    pub(crate) key: &'static str,
    pub(crate) queue: &'static str,
    pub(crate) identity: &'static str,
    pub(crate) waited: &'static str,
    pub(crate) average_wait: &'static str,
    pub(crate) releases: &'static str,
    pub(crate) average_hold: &'static str,
    pub(crate) reclaimed: &'static str,
    pub(crate) overages: &'static str,
    pub(crate) restored: &'static str,
    pub(crate) adjustments: &'static str,
    pub(crate) holders: &'static str,
    pub(crate) token: &'static str,
    pub(crate) lease_ends: &'static str,
    pub(crate) no_holders: &'static str,
    pub(crate) ended: &'static str,
    pub(crate) ended_help: &'static str,
    pub(crate) waiting_jobs: &'static str,
    pub(crate) no_waiting: &'static str,
    pub(crate) reschedules: &'static str,
    pub(crate) back: &'static str,
    pub(crate) never: &'static str,
    pub(crate) no_line: &'static str,
    pub(crate) unknown: &'static str,
    pub(crate) holders_help: &'static str,
    pub(crate) waiting_help: &'static str,
    pub(crate) sort_help: &'static str,
}

/// The English texts.
pub(crate) const EN: Texts = Texts {
    heading: "Limits",
    limit: "Limit",
    kind: "Kind",
    spec: "Spec",
    usage: "In use",
    next_free: "Next free",
    granted: "Granted",
    refused: "Refused",
    wait_line: "Wait line",
    last_used: "Last used",
    sort_by: "Sort by",
    sort_last: "Last used",
    sort_name: "Name",
    sort_refused: "Refusals",
    empty: "No limiter or throttle has been used in the last day.",
    no_matches: "No limit matches the filter.",
    unknown_spec: "unknown",
    most_refused: "Most refused in the last hour",
    throttles: "Throttles of live processes",
    throttle: "Throttle",
    processes: "Processes",
    differs: "differs between processes",
    reset: "Reset",
    reset_selected: "Reset selected",
    clear_line: "Clear wait line",
    confirm_reset: "Reset this limit? Every slot is freed at once; jobs holding one keep running.",
    confirm_reset_selected: "Reset the selected limits? Every slot is freed at once; jobs holding one keep running.",
    select_limit: "Select limit",
    name: "Name",
    key: "Key",
    queue: "Queue",
    identity: "Identity",
    waited: "Grants that waited",
    average_wait: "Average wait",
    releases: "Releases",
    average_hold: "Average hold",
    reclaimed: "Reclaimed leases",
    overages: "Overages",
    restored: "Restored leases",
    adjustments: "Cost corrections",
    holders: "Holders",
    token: "Token",
    lease_ends: "Lease ends",
    no_holders: "No one holds this limit.",
    ended: "lease ended",
    ended_help: "The holder did not renew its lease (a crash, or renewals that did not keep up): the next grant takes the slot back.",
    waiting_jobs: "Waiting jobs",
    no_waiting: "No scheduled job is waiting for this limit.",
    reschedules: "Reschedules",
    back: "All limits",
    never: "never",
    no_line: "none",
    unknown: "unknown",
    holders_help: "Concurrency permits: the jid of a throttled job, or a limiter's token.",
    waiting_help: "Scheduled jobs this limit rescheduled last (at most 100, earliest first).",
    sort_help: "Sorting by name or refusals, and filtering, look at the 10,000 most recently used limits.",
};

/// The text of a notice about limits.
pub(crate) fn notice(notice: &Notice, subject: &str) -> String {
    match notice.kind {
        NoticeKind::LimitsReset => format!(
            "Reset {}.",
            super::count_or_subject(notice, subject, "limit", "limits")
        ),
        _ => format!(
            "Cleared the wait line of {}.",
            super::count_or_subject(notice, subject, "limit", "limits")
        ),
    }
}

/// The name of a kind, as its chip shows it (points budgets share the leaky kind's state).
pub(crate) fn kind_name(kind: LimitKind, points: bool) -> &'static str {
    match kind {
        LimitKind::Concurrency => "concurrency",
        LimitKind::Window => "window",
        LimitKind::Bucket => "bucket",
        _ if points => "points",
        _ => "leaky",
    }
}

/// A use of a limit: `37 / 50`, `2.5 / 60` (leaky buckets and points have fractional
/// levels), `3` when the capacity is unknown.
pub(crate) fn usage(used: f64, capacity: Option<u32>) -> String {
    let decimals = if used.fract() == 0.0 { 0 } else { 1 };
    let used = format::decimal(used, decimals);
    match capacity {
        Some(capacity) => format!("{used} / {}", format::delimit(u64::from(capacity))),
        None => used,
    }
}

/// A wait in words: `now`, `in 250 ms`, `in 4 s`, `in 3 min`, `in 1 h 5 min`.
pub(crate) fn wait(wait: Duration) -> String {
    let secs = wait.as_secs();
    if wait.is_zero() {
        "now".to_owned()
    } else if secs == 0 {
        format!("in {} ms", wait.as_millis())
    } else if secs < 60 {
        format!("in {secs} s")
    } else if secs < 3_600 {
        format!("in {} min", secs / 60)
    } else if secs < 86_400 {
        format!("in {} h {} min", secs / 3_600, secs % 3_600 / 60)
    } else {
        format!("in {} d {} h", secs / 86_400, secs % 86_400 / 3_600)
    }
}

/// The wait line column: `line clears in 4 min`.
pub(crate) fn line_clears(wait: Duration) -> String {
    format!("line clears {}", self::wait(wait))
}

/// The summary under the heading.
pub(crate) fn summary(in_use: u64, granted: u64, refused: u64) -> String {
    let limits = if in_use == 1 { "limit" } else { "limits" };
    format!(
        "{} {limits} in use. Last hour: {} granted, {} refused.",
        format::delimit(in_use),
        format::delimit(granted),
        format::delimit(refused)
    )
}

/// The heading of an identity's page.
pub(crate) fn limit_heading(identity: &str) -> String {
    format!("Limit {}", format::visible(identity))
}

/// The chip of a job a limit rescheduled: `over limit: w:stripe:42 ×3`.
pub(crate) fn limited_chip(by: &str, n: u32) -> String {
    format!("over limit: {} ×{n}", format::visible(by))
}

/// The chip of a job that names a throttle: `throttle: stripe:42`.
pub(crate) fn throttle_chip(name: &str, key: Option<&str>) -> String {
    match key {
        Some(key) => format!(
            "throttle: {}:{}",
            format::visible(name),
            format::visible(key)
        ),
        None => format!("throttle: {}", format::visible(name)),
    }
}

/// The Limited row of a job's page: `by w:stripe:42, rescheduled 3 times`.
pub(crate) fn limited_row(by: &str, n: u32) -> String {
    let times = if n == 1 {
        "once".to_owned()
    } else {
        format!("{n} times")
    };
    format!("by {}, rescheduled {times}", format::visible(by))
}

/// The Throttle row of a job's page: `stripe, key 42`.
pub(crate) fn throttle_row(name: &str, key: Option<&str>) -> String {
    match key {
        Some(key) => format!("{}, key {}", format::visible(name), format::visible(key)),
        None => format::visible(name).into_owned(),
    }
}

/// The label of the Limited row.
pub(crate) const LIMITED_LABEL: &str = "Limited";

/// The label of the Throttle row.
pub(crate) const THROTTLE_LABEL: &str = "Throttle";

/// One of the most refused names: `stripe: 12 refused`.
pub(crate) fn refused_name(name: &str, refused: u64) -> String {
    format!(
        "{}: {} refused",
        format::visible(name),
        format::delimit(refused)
    )
}

/// How many processes declare a throttle spec.
pub(crate) fn process_count(count: usize) -> String {
    if count == 1 {
        "1 process".to_owned()
    } else {
        format!(
            "{} processes",
            format::delimit(u64::try_from(count).unwrap_or(u64::MAX))
        )
    }
}

/// The page title of the list.
pub(crate) fn list_title(in_use: u64) -> String {
    super::counted(EN.heading, in_use)
}

/// The "not found" text of an identity no longer in use.
pub(crate) fn not_in_use(identity: &str) -> String {
    format!(
        "The limit {} is not in use: its counters expired, or it was reset.",
        format::visible(identity)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn usages_and_waits_read_well() {
        assert_eq!(usage(37.0, Some(50)), "37 / 50");
        assert_eq!(usage(2.5, Some(60)), "2.5 / 60");
        assert_eq!(usage(3.0, None), "3");
        assert_eq!(usage(1.0, Some(10_000)), "1 / 10,000");
        assert_eq!(wait(Duration::ZERO), "now");
        assert_eq!(wait(Duration::from_millis(250)), "in 250 ms");
        assert_eq!(wait(Duration::from_secs(4)), "in 4 s");
        assert_eq!(wait(Duration::from_secs(185)), "in 3 min");
        assert_eq!(wait(Duration::from_secs(3_900)), "in 1 h 5 min");
        assert_eq!(wait(Duration::from_secs(90_000)), "in 1 d 1 h");
        assert_eq!(
            line_clears(Duration::from_secs(240)),
            "line clears in 4 min"
        );
        assert_eq!(
            summary(3, 1_200, 4),
            "3 limits in use. Last hour: 1,200 granted, 4 refused."
        );
        assert_eq!(
            summary(1, 0, 0),
            "1 limit in use. Last hour: 0 granted, 0 refused."
        );
    }

    #[test]
    fn chips_and_rows_spell_out_control_characters() {
        assert_eq!(limited_chip("w:stripe:42", 3), "over limit: w:stripe:42 ×3");
        assert_eq!(limited_chip("w:a\nb", 1), "over limit: w:a\\nb ×1");
        assert_eq!(throttle_chip("stripe", Some("42")), "throttle: stripe:42");
        assert_eq!(throttle_chip("stripe", None), "throttle: stripe");
        assert_eq!(limited_row("w:x", 1), "by w:x, rescheduled once");
        assert_eq!(limited_row("w:x", 4), "by w:x, rescheduled 4 times");
        assert_eq!(throttle_row("stripe", Some("4\t2")), "stripe, key 4\\t2");
        assert_eq!(kind_name(LimitKind::Leaky, true), "points");
        assert_eq!(kind_name(LimitKind::Leaky, false), "leaky");
        assert_eq!(process_count(1), "1 process");
        assert_eq!(process_count(2), "2 processes");
        assert_eq!(limit_heading("c:a\u{1b}b"), "Limit c:a\\u{1b}b");
    }
}
