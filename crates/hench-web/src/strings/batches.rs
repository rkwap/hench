//! The texts of the Batches tab and pages.

use std::time::Duration;

use crate::flash::{Notice, NoticeKind};
use crate::format;

/// The tab's name.
pub(crate) const TAB: &str = "Batches";

/// The chip of a batch callback in the job lists.
pub(crate) const CALLBACK_CHIP: &str = "callback";

/// The fixed texts of the Batches pages.
pub(crate) struct Texts {
    pub(crate) heading: &'static str,
    pub(crate) states: &'static str,
    pub(crate) batch: &'static str,
    pub(crate) created: &'static str,
    pub(crate) progress: &'static str,
    pub(crate) total: &'static str,
    pub(crate) pending: &'static str,
    pub(crate) failures: &'static str,
    pub(crate) dead: &'static str,
    pub(crate) skipped: &'static str,
    pub(crate) succeeded: &'static str,
    pub(crate) children: &'static str,
    pub(crate) state: &'static str,
    pub(crate) expires: &'static str,
    pub(crate) empty: &'static str,
    pub(crate) empty_state: &'static str,
    pub(crate) no_matches: &'static str,
    pub(crate) back: &'static str,
    pub(crate) parent: &'static str,
    pub(crate) callbacks: &'static str,
    pub(crate) no_callbacks: &'static str,
    pub(crate) event: &'static str,
    pub(crate) pushed: &'static str,
    pub(crate) waiting: &'static str,
    pub(crate) not_fired: &'static str,
    pub(crate) no_failures: &'static str,
    pub(crate) failed_at: &'static str,
    pub(crate) no_children: &'static str,
    pub(crate) pending_members: &'static str,
    pub(crate) pending_help: &'static str,
    pub(crate) no_pending: &'static str,
    pub(crate) find_in: &'static str,
    pub(crate) search_failed: &'static str,
    pub(crate) skip: &'static str,
    pub(crate) confirm_skip: &'static str,
    pub(crate) cancel: &'static str,
    pub(crate) delete: &'static str,
    pub(crate) confirm_delete: &'static str,
    pub(crate) dispatch: &'static str,
    pub(crate) dispatch_help: &'static str,
    pub(crate) never: &'static str,
    pub(crate) callback_label: &'static str,
    pub(crate) member_label: &'static str,
}

/// The English texts.
pub(crate) const EN: Texts = Texts {
    heading: "Batches",
    states: "Show",
    batch: "Batch",
    created: "Created",
    progress: "Progress",
    total: "Jobs",
    pending: "Pending",
    failures: "Failures",
    dead: "Dead",
    skipped: "Skipped",
    succeeded: "Succeeded",
    children: "Children",
    state: "State",
    expires: "Expires",
    empty: "No batch exists, or every batch expired.",
    empty_state: "No batch is in this state.",
    no_matches: "No batch matches the filter.",
    back: "All batches",
    parent: "Parent batch",
    callbacks: "Callbacks",
    no_callbacks: "This batch has no callbacks.",
    event: "Event",
    pushed: "pushed",
    waiting: "waiting to be pushed",
    not_fired: "not fired",
    no_failures: "No job of this batch failed.",
    failed_at: "Failed",
    no_children: "This batch has no child batches.",
    pending_members: "Pending jobs",
    pending_help: "Jobs the batch still waits for (at most 25): queued, scheduled, retrying or dead. Find each by its jid.",
    no_pending: "No job is pending.",
    find_in: "Find in",
    search_failed: "Not found where its failure left it (it may be running again, or have moved): search the list for it.",
    skip: "Skip",
    confirm_skip: "Skip this job? The batch can then succeed without it.",
    cancel: "Cancel batch",
    delete: "Delete batch",
    confirm_delete: "Delete this batch and its child batches? Their jobs keep running, without a batch to report to.",
    dispatch: "Push callbacks now",
    dispatch_help: "An event fired and its callbacks wait to be pushed: maintenance pushes them, or push them now.",
    never: "–",
    callback_label: "Batch callback",
    member_label: "Batch",
};

/// The text of a notice about batches.
pub(crate) fn notice(notice: &Notice, subject: &str) -> String {
    match notice.kind {
        NoticeKind::BatchCancelled => format!(
            "Cancelled {}.",
            super::count_or_subject(notice, subject, "batch", "batches")
        ),
        NoticeKind::BatchDeleted => format!(
            "Deleted {}.",
            super::count_or_subject(notice, subject, "batch", "batches")
        ),
        NoticeKind::MemberSkipped => format!(
            "Skipped {}.",
            super::count_or_subject(notice, subject, "pending member", "pending members")
        ),
        _ => format!(
            "Dispatched {}.",
            super::count_or_subject(notice, subject, "batch event", "batch events")
        ),
    }
}

/// The name of a state filter, by its `state` parameter.
pub(crate) fn state_name(state: &str) -> &'static str {
    match state {
        "dead" => "Dead",
        "complete" => "Complete",
        "succeeded" => "Succeeded",
        "cancelled" => "Cancelled",
        "all" => "All",
        _ => "Active",
    }
}

/// The page title of the list.
pub(crate) fn list_title(count: u64) -> String {
    super::counted(EN.heading, count)
}

/// The summary under the list's heading: how many batches exist, and the last hour's events.
pub(crate) fn summary(
    batches: u64,
    created: u64,
    succeeded: u64,
    died: u64,
    cancelled: u64,
) -> String {
    let unit = if batches == 1 { "batch" } else { "batches" };
    format!(
        "{} {unit} in all. Last hour: {} created, {} succeeded, {} with a death, {} cancelled.",
        format::delimit(batches),
        format::delimit(created),
        format::delimit(succeeded),
        format::delimit(died),
        format::delimit(cancelled)
    )
}

/// The note of a filtered list, which searched part of the index.
pub(crate) fn searched(searched: u64, truncated: bool) -> String {
    let base = format!(
        "Searched the {} most recently active batches.",
        format::delimit(searched)
    );
    if truncated {
        format!("{base} Only the first 500 matches are listed.")
    } else {
        base
    }
}

/// The accessible label of a progress bar.
pub(crate) fn progress_label(
    total: u64,
    succeeded: u64,
    retrying: u64,
    dead: u64,
    skipped: u64,
    waiting: u64,
) -> String {
    format!(
        "{} of {} jobs succeeded, {} retrying, {} dead, {} skipped, {} not run yet",
        format::delimit(succeeded),
        format::delimit(total),
        format::delimit(retrying),
        format::delimit(dead),
        format::delimit(skipped),
        format::delimit(waiting)
    )
}

/// The counts shown next to a progress bar: `120 / 130`.
pub(crate) fn progress_counts(done: u64, total: u64) -> String {
    format!("{} / {}", format::delimit(done), format::delimit(total))
}

/// The heading of a batch's page.
pub(crate) fn batch_heading(bid: &str) -> String {
    format!("Batch {}", format::visible(bid))
}

/// The "not found" text of a batch.
pub(crate) fn not_found(bid: &str) -> String {
    format!(
        "Batch {} does not exist or its data has expired.",
        format::visible(bid)
    )
}

/// The confirmation of a cancellation, naming the children that go with it.
pub(crate) fn confirm_cancel(children: u64) -> String {
    match children {
        0 => "Cancel this batch? Jobs not yet run will be skipped, and it can never succeed."
            .to_owned(),
        1 => "Cancel this batch and its child batch? Jobs not yet run will be skipped.".to_owned(),
        n => format!(
            "Cancel this batch and its {} child batches? Jobs not yet run will be skipped.",
            format::delimit(n)
        ),
    }
}

/// The children fact: `3 (2 complete, 1 succeeded)`.
pub(crate) fn children_fact(children: u64, complete: u64, succeeded: u64) -> String {
    if children == 0 {
        return "0".to_owned();
    }
    format!(
        "{} ({} complete, {} succeeded)",
        format::delimit(children),
        format::delimit(complete),
        format::delimit(succeeded)
    )
}

/// An event's fact: when it fired, and when its callbacks were pushed.
pub(crate) fn event_fact(fired: &str, pushed: Option<&str>) -> String {
    match pushed {
        Some(pushed) => format!("{fired} (callbacks pushed {pushed})"),
        None => format!("{fired} (callbacks waiting to be pushed)"),
    }
}

/// A duration fact: `4 min 12 s`.
pub(crate) fn duration(duration: Duration) -> String {
    let secs = duration.as_secs();
    match secs {
        0 => format!("{} ms", duration.as_millis()),
        1..=59 => format!("{secs} s"),
        60..=3_599 => format!("{} min {} s", secs / 60, secs % 60),
        3_600..=86_399 => format!("{} h {} min", secs / 3_600, secs % 3_600 / 60),
        _ => format!("{} d {} h", secs / 86_400, secs % 86_400 / 3_600),
    }
}

/// The state column of a failure: `retrying` (`, next in 3 min`) or `dead: in the morgue`.
pub(crate) fn failure_state(dead: bool, reason: &str, next: Option<&str>) -> String {
    match (dead, next) {
        (true, _) => format!("dead: {}", reason_text(reason)),
        (false, Some(next)) => format!("retrying {next}"),
        (false, None) => "retrying".to_owned(),
    }
}

/// A failure's reason in words.
pub(crate) fn reason_text(reason: &str) -> String {
    match reason {
        "morgue" => "in the morgue".to_owned(),
        "no_retry" => "retries disabled".to_owned(),
        "discarded" => "discarded".to_owned(),
        "deleted" => "deleted".to_owned(),
        "killed" => "killed".to_owned(),
        "retry" => "retrying".to_owned(),
        other => format::visible(other).into_owned(),
    }
}

/// The Batch row of a member's page.
pub(crate) fn member_row(bid: &str) -> String {
    format!("member of batch {}", format::visible(bid))
}

/// The Batch callback row of a callback's page: `success callback of batch …`.
pub(crate) fn callback_row(event: &str, bid: &str) -> String {
    format!("{event} callback of batch {}", format::visible(bid))
}

/// What a delete or kill confirmation adds for a member: its batch counts it as dead.
pub(crate) fn removal_note(bid: &str) -> String {
    format!(
        "This job belongs to batch {}; it will count as dead there.",
        format::visible(bid)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summaries_read_well() {
        assert_eq!(
            summary(3, 10, 7, 1, 0),
            "3 batches in all. Last hour: 10 created, 7 succeeded, 1 with a death, 0 cancelled."
        );
        assert_eq!(
            summary(1, 0, 0, 0, 0),
            "1 batch in all. Last hour: 0 created, 0 succeeded, 0 with a death, 0 cancelled."
        );
        assert_eq!(
            searched(50_000, true),
            "Searched the 50,000 most recently active batches. Only the first 500 matches are \
             listed."
        );
        assert_eq!(state_name("dead"), "Dead");
        assert_eq!(state_name("anything"), "Active");
    }

    #[test]
    fn facts_and_rows_read_well() {
        assert_eq!(
            progress_label(130, 120, 3, 1, 0, 6),
            "120 of 130 jobs succeeded, 3 retrying, 1 dead, 0 skipped, 6 not run yet"
        );
        assert_eq!(progress_counts(1_200, 1_300), "1,200 / 1,300");
        assert_eq!(children_fact(0, 0, 0), "0");
        assert_eq!(children_fact(3, 2, 1), "3 (2 complete, 1 succeeded)");
        assert_eq!(
            event_fact("just now", None),
            "just now (callbacks waiting to be pushed)"
        );
        assert_eq!(
            event_fact("1 min ago", Some("1 min ago")),
            "1 min ago (callbacks pushed 1 min ago)"
        );
        assert_eq!(duration(Duration::from_millis(250)), "250 ms");
        assert_eq!(duration(Duration::from_secs(252)), "4 min 12 s");
        assert_eq!(duration(Duration::from_secs(90_000)), "1 d 1 h");
        assert_eq!(failure_state(true, "killed", None), "dead: killed");
        assert_eq!(
            failure_state(false, "retry", Some("in 3 min")),
            "retrying in 3 min"
        );
        assert_eq!(reason_text("x\ny"), "x\\ny");
        assert!(confirm_cancel(2).contains("its 2 child batches"));
        assert_eq!(batch_heading("a\u{1b}b"), "Batch a\\u{1b}b");
        assert_eq!(
            callback_row("success", "0123"),
            "success callback of batch 0123"
        );
        assert_eq!(member_row("0123"), "member of batch 0123");
    }
}
