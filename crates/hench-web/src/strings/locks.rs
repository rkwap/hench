//! The texts of the Locks tab and pages (the locks of unique jobs).

use std::time::Duration;

use hench::unique::{UniqueEventCounts, UniqueUntil};

use crate::flash::Notice;
use crate::format::delimit;
use crate::search::Outcome;

/// The tab's name.
pub(crate) const TAB: &str = "Locks";

/// The chip of a unique job in the job lists.
pub(crate) const UNIQUE_CHIP: &str = "unique";

/// The fixed labels of the Locks pages (templates read them as `t.<name>`).
pub(crate) struct LockStrings {
    pub(crate) heading: &'static str,
    pub(crate) policy: &'static str,
    pub(crate) holder: &'static str,
    pub(crate) locked: &'static str,
    pub(crate) expires: &'static str,
    pub(crate) unique_for: &'static str,
    pub(crate) digest: &'static str,
    pub(crate) holder_runs: &'static str,
    pub(crate) no_expiry: &'static str,
    pub(crate) empty: &'static str,
    pub(crate) no_matches: &'static str,
    pub(crate) no_matches_searched: &'static str,
    pub(crate) filter_placeholder: &'static str,
    pub(crate) order: &'static str,
    pub(crate) soonest_first: &'static str,
    pub(crate) latest_first: &'static str,
    pub(crate) last_hour: &'static str,
    pub(crate) last_hour_help: &'static str,
    pub(crate) duplicates: &'static str,
    pub(crate) replaced: &'static str,
    pub(crate) lapsed: &'static str,
    pub(crate) select_all: &'static str,
    pub(crate) select_lock: &'static str,
    pub(crate) selected: &'static str,
    pub(crate) everything: &'static str,
    pub(crate) release: &'static str,
    pub(crate) release_all: &'static str,
    pub(crate) release_lock: &'static str,
    pub(crate) confirm_release_selected: &'static str,
    pub(crate) confirm_release: &'static str,
    pub(crate) holder_heading: &'static str,
    pub(crate) show_job: &'static str,
    pub(crate) show_in_queue: &'static str,
    pub(crate) raw_hash: &'static str,
    pub(crate) release_help: &'static str,
}

/// The English labels.
pub(crate) static LOCKS: LockStrings = LockStrings {
    heading: "Unique locks",
    policy: "Released",
    holder: "Holder",
    locked: "Locked",
    expires: "Expires",
    unique_for: "Unique for",
    digest: "Digest",
    holder_runs: "Holder runs",
    no_expiry: "never (no time to live)",
    empty: "No unique job holds a lock.",
    no_matches: "No lock matches this filter.",
    no_matches_searched: "None of the locks searched match this filter.",
    filter_placeholder: "Job class, queue or JID",
    order: "Order",
    soonest_first: "Expiring soonest",
    latest_first: "Expiring latest",
    last_hour: "Last hour",
    last_hour_help: "Pushes not made because another job held the lock, holders replaced by a \
        newer push, and jobs that no longer held their lock when it was due for release (their \
        unique_for was shorter than their wait, run or retries).",
    duplicates: "Duplicates not pushed",
    replaced: "Replaced",
    lapsed: "Lapsed",
    select_all: "Select all locks on this page",
    select_lock: "Select the lock held by",
    selected: "Selected locks",
    everything: "Every lock",
    release: "Release",
    release_all: "Release all",
    release_lock: "Release this lock",
    confirm_release_selected: "Release the selected locks? Their duplicates can then be pushed.",
    confirm_release: "Release this lock? A duplicate of its job can then be pushed.",
    holder_heading: "Where the holder is",
    show_job: "Show the job",
    show_in_queue: "Show it in the queue",
    raw_hash: "The lock as stored",
    release_help: "Releasing a lock lets a duplicate be pushed at once; the holder itself is \
        not changed. Release the lock of a holder that is gone or stuck.",
};

/// The text of a notice about unique locks.
pub(crate) fn notice(notice: &Notice, subject: &str) -> String {
    format!(
        "Released {}.",
        super::count_or_subject(notice, subject, "lock", "locks")
    )
}

/// `"1 lock"`, `"3 locks"`.
pub(crate) fn locks(count: u64) -> String {
    format!(
        "{} {}",
        delimit(count),
        if count == 1 { "lock" } else { "locks" }
    )
}

/// What the list holds: `3 unique jobs hold a lock.`
pub(crate) fn summary(total: u64) -> String {
    match total {
        0 => "No unique job holds a lock.".to_owned(),
        1 => "1 unique job holds a lock.".to_owned(),
        _ => format!("{} unique jobs hold a lock.", delimit(total)),
    }
}

/// The last hour's counters in a sentence: `Last hour: 5 duplicates not pushed, 1 replaced, 0
/// lapsed.`
pub(crate) fn last_hour(counts: &UniqueEventCounts) -> String {
    let duplicates = match counts.duplicates {
        1 => "1 duplicate not pushed".to_owned(),
        count => format!("{} duplicates not pushed", delimit(count)),
    };
    format!(
        "Last hour: {duplicates}, {} replaced, {} lapsed.",
        delimit(counts.replaced),
        delimit(counts.lapsed)
    )
}

/// A lock's policy: `until success`.
pub(crate) fn policy(until: UniqueUntil) -> &'static str {
    match until {
        UniqueUntil::Start => "until start",
        UniqueUntil::Expired => "until expired",
        _ => "until success",
    }
}

/// What a policy means, for the lock's page.
pub(crate) fn policy_help(until: UniqueUntil) -> &'static str {
    match until {
        UniqueUntil::Start => "released when the job starts",
        UniqueUntil::Expired => "kept until it expires, even after the job ran",
        _ => "released when the job succeeds or dies",
    }
}

/// A time span in at most two adjacent units: `10 minutes`, `1 hour 30 minutes`, `45 seconds`.
pub(crate) fn span(duration: Duration) -> String {
    let secs = duration.as_secs();
    if secs == 0 {
        return "0 seconds".to_owned();
    }
    let units = [
        (86_400, "day", "days"),
        (3_600, "hour", "hours"),
        (60, "minute", "minutes"),
        (1, "second", "seconds"),
    ];
    let mut parts = Vec::new();
    let mut rest = secs;
    for (size, one, many) in units {
        let count = rest / size;
        if count > 0 {
            parts.push(format!(
                "{} {}",
                delimit(count),
                if count == 1 { one } else { many }
            ));
            rest %= size;
        } else if !parts.is_empty() {
            // Adjacent units only: `1 hour`, not `1 hour 5 seconds`.
            break;
        }
        if parts.len() == 2 {
            break;
        }
    }
    parts.join(" ")
}

/// The Unique row of a job's page: `until success, unique for 10 minutes`.
pub(crate) fn job_row(until: UniqueUntil, unique_for: Duration) -> String {
    format!("{}, unique for {}", policy(until), span(unique_for))
}

/// The summary of a filtered list: how many locks matched, and how much was searched.
pub(crate) fn filter_summary(count: usize, outcome: Outcome, filter: &str) -> String {
    let count = u64::try_from(count).unwrap_or(u64::MAX);
    match outcome {
        Outcome::Complete => format!("{} matching “{filter}”.", locks(count)),
        Outcome::MatchLimit => format!("Showing the first {} matching “{filter}”.", locks(count)),
        Outcome::ScanLimit { searched, total } => format!(
            "{} matching “{filter}” in the first {} of {}; the others were not searched.",
            locks(count),
            delimit(searched),
            locks(total)
        ),
    }
}

/// What a filtered list without matches says.
pub(crate) fn no_matches(outcome: Outcome) -> &'static str {
    match outcome {
        Outcome::ScanLimit { .. } => LOCKS.no_matches_searched,
        Outcome::Complete | Outcome::MatchLimit => LOCKS.no_matches,
    }
}

/// The confirmation of "Release all": `Release all 1,234 unique locks? ...`.
pub(crate) fn confirm_release_all(total: u64) -> String {
    if total == 1 {
        "Release the only unique lock? A duplicate of its job can then be pushed.".to_owned()
    } else {
        format!(
            "Release all {} unique locks? Duplicates of their jobs can then be pushed.",
            delimit(total)
        )
    }
}

/// The heading of a lock's page.
pub(crate) fn lock_heading(class: &str) -> String {
    format!("Lock of “{class}”")
}

/// Why a lock's page cannot be shown.
pub(crate) const LOCK_NOT_FOUND: &str = "No unique job holds this lock: it expired or was \
                                         released, so a duplicate of its job can be pushed.";

/// A bulk release naming too many locks.
pub(crate) fn too_many(max: usize) -> String {
    format!(
        "At most {} locks can be released at once.",
        delimit(u64::try_from(max).unwrap_or(u64::MAX))
    )
}

/// A release form without the holder it showed.
pub(crate) const NO_HOLDER: &str = "The form did not say which holder's lock to release.";

/// The lock page reached from the job holding it.
pub(crate) const HELD_BY_THIS_JOB: &str = "The job you came from holds this lock.";

/// The lock page reached from a job that does not hold it.
pub(crate) fn held_by_another(jid: &str) -> String {
    format!(
        "The job you came from does not hold this lock: job {jid} does (the lock was taken \
         again after the first one expired or was released)."
    )
}

/// Where the holder is: running (followed by when it started).
pub(crate) fn running(process: &str) -> String {
    format!("Running on {process}, started")
}

/// Where the holder is: in its queue.
pub(crate) fn queued(queue: &str, position: u64) -> String {
    let place = match position {
        1 => "next to run".to_owned(),
        _ => format!("number {} in line", delimit(position)),
    };
    format!("Waiting in queue “{queue}”, {place}.")
}

/// Where the holder is: in the schedule (followed by when it runs).
pub(crate) const SCHEDULED: &str = "Scheduled to run";

/// Where the holder is: in the retries (followed by when it is retried).
pub(crate) const RETRYING: &str = "Waiting for a retry,";

/// Where the holder is: dead (followed by when it died).
pub(crate) const DEAD: &str = "In the dead set since";

/// Where the holder is: nowhere. `expires` says when the lock expires (`in 5 minutes`).
pub(crate) fn not_found(until: UniqueUntil, complete: bool, expires: &str) -> String {
    let searched = if complete {
        "The holder is not running, nor in its queue, the schedule, the retries or the dead set"
    } else {
        "The holder was not found running, nor in the parts of its queue, the schedule, the \
         retries and the dead set searched (50,000 entries of each)"
    };
    let why = match until {
        UniqueUntil::Expired => {
            "a lock released when it expires stays after its job ran, which is how it keeps \
             duplicates out until then"
        }
        _ => {
            "it may have been removed while its lock lapsed and was taken again, or moved while \
             it was searched"
        }
    };
    format!("{searched}: {why}. The lock expires {expires}.")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flash::NoticeKind;

    #[test]
    fn spans_use_two_adjacent_units_at_most() {
        assert_eq!(span(Duration::ZERO), "0 seconds");
        assert_eq!(span(Duration::from_secs(1)), "1 second");
        assert_eq!(span(Duration::from_secs(45)), "45 seconds");
        assert_eq!(span(Duration::from_secs(600)), "10 minutes");
        assert_eq!(span(Duration::from_secs(90)), "1 minute 30 seconds");
        assert_eq!(span(Duration::from_secs(5_400)), "1 hour 30 minutes");
        assert_eq!(span(Duration::from_secs(3_605)), "1 hour");
        assert_eq!(
            span(Duration::from_secs(86_400 * 2 + 3_600)),
            "2 days 1 hour"
        );
        assert_eq!(span(Duration::from_secs(86_400 * 1_000)), "1,000 days");
    }

    #[test]
    fn sentences() {
        assert_eq!(summary(0), "No unique job holds a lock.");
        assert_eq!(summary(1), "1 unique job holds a lock.");
        assert_eq!(summary(1_234), "1,234 unique jobs hold a lock.");
        let mut counts = UniqueEventCounts::default();
        counts.duplicates = 1;
        assert_eq!(
            last_hour(&counts),
            "Last hour: 1 duplicate not pushed, 0 replaced, 0 lapsed."
        );
        counts.duplicates = 1_500;
        counts.lapsed = 2;
        assert_eq!(
            last_hour(&counts),
            "Last hour: 1,500 duplicates not pushed, 0 replaced, 2 lapsed."
        );
        assert_eq!(
            job_row(UniqueUntil::Start, Duration::from_secs(600)),
            "until start, unique for 10 minutes"
        );
        assert_eq!(
            confirm_release_all(1_234),
            "Release all 1,234 unique locks? Duplicates of their jobs can then be pushed."
        );
        assert!(confirm_release_all(1).starts_with("Release the only unique lock?"));
        assert_eq!(queued("q", 1), "Waiting in queue “q”, next to run.");
        assert_eq!(
            queued("q", 1_200),
            "Waiting in queue “q”, number 1,200 in line."
        );
        assert_eq!(
            filter_summary(2, Outcome::Complete, "Mail*"),
            "2 locks matching “Mail*”."
        );
        assert_eq!(
            filter_summary(
                1,
                Outcome::ScanLimit {
                    searched: 50_000,
                    total: 80_000
                },
                "x"
            ),
            "1 lock matching “x” in the first 50,000 of 80,000 locks; the others were not \
             searched."
        );
        assert_eq!(
            no_matches(Outcome::ScanLimit {
                searched: 1,
                total: 2
            }),
            LOCKS.no_matches_searched
        );
        assert!(not_found(UniqueUntil::Expired, true, "in 5 minutes").ends_with("in 5 minutes."));
        assert!(not_found(UniqueUntil::Success, false, "in 1 hour").contains("50,000"));
    }

    #[test]
    fn notices() {
        let mut released = Notice::new(NoticeKind::LocksReleased, 3, 0);
        assert_eq!(notice(&released, ""), "Released 3 locks.");
        released.count = 1;
        assert_eq!(notice(&released, ""), "Released 1 lock.");
        assert_eq!(notice(&released, "abc"), "Released lock “abc”.");
    }
}
