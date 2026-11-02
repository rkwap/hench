//! The texts of the Periodic tab and pages (the periodic schedules).

use std::time::Duration;

use hench::periodic::{CatchUp, FireResult, RunStatus};

use crate::flash::{Notice, NoticeKind};
use crate::format::delimit;
use crate::search::Outcome;
use crate::strings::locks::span;

/// The tab's name.
pub(crate) const TAB: &str = "Periodic";

/// The fixed labels of the Periodic pages (templates read them as `t.<name>`).
pub(crate) struct PeriodicStrings {
    pub(crate) heading: &'static str,
    pub(crate) name: &'static str,
    pub(crate) timetable: &'static str,
    pub(crate) next_run: &'static str,
    pub(crate) last_enqueue: &'static str,
    pub(crate) last_run: &'static str,
    pub(crate) paused: &'static str,
    pub(crate) api: &'static str,
    pub(crate) error: &'static str,
    pub(crate) late: &'static str,
    pub(crate) empty: &'static str,
    pub(crate) no_matches: &'static str,
    pub(crate) no_matches_searched: &'static str,
    pub(crate) filter_placeholder: &'static str,
    pub(crate) select_all: &'static str,
    pub(crate) select_schedule: &'static str,
    pub(crate) selected: &'static str,
    pub(crate) enqueue_now: &'static str,
    pub(crate) pause: &'static str,
    pub(crate) resume: &'static str,
    pub(crate) delete: &'static str,
    pub(crate) confirm_enqueue_selected: &'static str,
    pub(crate) confirm_delete_selected: &'static str,
    pub(crate) confirm_enqueue: &'static str,
    pub(crate) confirm_pause: &'static str,
    pub(crate) confirm_delete: &'static str,
    pub(crate) confirm_delete_code: &'static str,
    pub(crate) never: &'static str,
    pub(crate) definition: &'static str,
    pub(crate) state: &'static str,
    pub(crate) source: &'static str,
    pub(crate) description: &'static str,
    pub(crate) upcoming: &'static str,
    pub(crate) catch_up: &'static str,
    pub(crate) job_template: &'static str,
    pub(crate) created: &'static str,
    pub(crate) updated: &'static str,
    pub(crate) digest: &'static str,
    pub(crate) paused_since: &'static str,
    pub(crate) last_manual: &'static str,
    pub(crate) counters: &'static str,
    pub(crate) parked: &'static str,
    pub(crate) history: &'static str,
    pub(crate) history_help: &'static str,
    pub(crate) no_history: &'static str,
    pub(crate) tick: &'static str,
    pub(crate) handled_at: &'static str,
    pub(crate) lateness: &'static str,
    pub(crate) result: &'static str,
    pub(crate) by: &'static str,
    pub(crate) missed: &'static str,
    pub(crate) raw_hash: &'static str,
    pub(crate) no_upcoming_paused: &'static str,
    pub(crate) actions_help: &'static str,
    pub(crate) delete_help_code: &'static str,
    pub(crate) unreadable: &'static str,
}

/// The English labels.
pub(crate) static PERIODIC: PeriodicStrings = PeriodicStrings {
    heading: "Periodic jobs",
    name: "Name",
    timetable: "Timetable",
    next_run: "Next run",
    last_enqueue: "Last enqueue",
    last_run: "Last run",
    paused: "paused",
    api: "api",
    error: "error",
    late: "overdue",
    empty: "No periodic job is defined.",
    no_matches: "No periodic job matches this filter.",
    no_matches_searched: "None of the periodic jobs searched match this filter.",
    filter_placeholder: "Name, job class or expression",
    select_all: "Select all periodic jobs on this page",
    select_schedule: "Select periodic job",
    selected: "Selected periodic jobs",
    enqueue_now: "Enqueue now",
    pause: "Pause",
    resume: "Resume",
    delete: "Delete",
    confirm_enqueue_selected: "Enqueue the jobs of the selected periodic jobs now? Their \
        timetables do not change.",
    confirm_delete_selected: "Delete the selected periodic jobs and their histories? Those \
        defined in the code of running processes are kept.",
    confirm_enqueue: "Enqueue this job now? The timetable does not change.",
    confirm_pause: "Pause this periodic job? Its ticks are not enqueued (nor counted as missed) \
        until it is resumed.",
    confirm_delete: "Delete this periodic job and its history?",
    confirm_delete_code: "Delete this periodic job and its history? No running process defines \
        it any more; a process whose code still defines it registers it again when it starts.",
    never: "never",
    definition: "Definition",
    state: "State",
    source: "Defined by",
    description: "Description",
    upcoming: "Next ticks",
    catch_up: "Missed ticks",
    job_template: "Job",
    created: "Created",
    updated: "Updated",
    digest: "Digest",
    paused_since: "Paused since",
    last_manual: "Last enqueued by hand",
    counters: "Counters",
    parked: "Error",
    history: "History",
    history_help: "What was done with the latest ticks, newest first (the processes keep as \
        many as their periodic_history setting says).",
    no_history: "No tick handled yet.",
    tick: "Tick",
    handled_at: "Handled",
    lateness: "Late by",
    result: "Result",
    by: "Process",
    missed: "Missed before",
    raw_hash: "The periodic job as stored",
    no_upcoming_paused: "None while it is paused: once resumed it ticks again from its first \
        tick after that moment (the ticks in between are not enqueued).",
    actions_help: "Enqueuing now pushes the job once and leaves the timetable alone; pausing \
        stops the ticks until it is resumed.",
    delete_help_code: "Defined in the code of running processes: remove it from their code to \
        delete it (they would register it again otherwise).",
    unreadable: "unreadable",
};

/// The text of a notice about periodic schedules. The subject names the schedule of a single
/// action; a single "enqueue now" that pushed nothing is a failure without a count.
pub(crate) fn notice(notice: &Notice, subject: &str) -> String {
    let what = super::count_or_subject(notice, subject, "schedule", "schedules");
    match notice.kind {
        NoticeKind::PeriodicEnqueued
            if !subject.is_empty() && notice.count == 0 && notice.failed > 0 =>
        {
            format!(
                "Did not enqueue the job of {what}: client middleware stopped it, or the job \
                 is unique and its lock is held."
            )
        }
        NoticeKind::PeriodicEnqueued if subject.is_empty() => {
            format!("Enqueued the jobs of {what} now.")
        }
        NoticeKind::PeriodicEnqueued => format!("Enqueued the job of {what} now."),
        NoticeKind::PeriodicPaused => format!("Paused {what}."),
        NoticeKind::PeriodicResumed => format!("Resumed {what}."),
        _ => format!("Deleted {what}."),
    }
}

/// `"1 periodic job"`, `"3 periodic jobs"`.
pub(crate) fn schedules(count: u64) -> String {
    let unit = if count == 1 {
        "periodic job"
    } else {
        "periodic jobs"
    };
    format!("{} {unit}", delimit(count))
}

/// What the list holds: `12 periodic jobs, 1 paused.`
pub(crate) fn summary(total: u64, paused: u64) -> String {
    match (total, paused) {
        (0, _) => PERIODIC.empty.to_owned(),
        (_, 0) => format!("{}.", schedules(total)),
        _ => format!("{}, {} paused.", schedules(total), delimit(paused)),
    }
}

/// The summary of a filtered list: how many schedules matched, and how much was searched.
pub(crate) fn filter_summary(count: usize, outcome: Outcome, filter: &str) -> String {
    let count = u64::try_from(count).unwrap_or(u64::MAX);
    match outcome {
        Outcome::Complete => format!("{} matching “{filter}”.", schedules(count)),
        Outcome::MatchLimit => {
            format!(
                "Showing the first {} matching “{filter}”.",
                schedules(count)
            )
        }
        Outcome::ScanLimit { searched, total } => format!(
            "{} matching “{filter}” in the first {} of {}; the others were not searched.",
            schedules(count),
            delimit(searched),
            schedules(total)
        ),
    }
}

/// What a filtered list without matches says.
pub(crate) fn no_matches(outcome: Outcome) -> &'static str {
    match outcome {
        Outcome::ScanLimit { .. } => PERIODIC.no_matches_searched,
        Outcome::Complete | Outcome::MatchLimit => PERIODIC.no_matches,
    }
}

/// The banner shown when no live process polls the schedules.
pub(crate) const NO_POLLER: &str = "No running process polls the periodic jobs, so none is \
    enqueued: start a worker (its poller is on unless its periodic_poller setting turns it off).";

/// The banner shown when the live processes of a group define different crontabs.
pub(crate) fn crontabs_differ(group: &str, count: usize) -> String {
    format!(
        "The running processes of group “{group}” define {count} different sets of periodic \
         jobs (a deploy is under way, or they run different code): each registers its own set \
         when it starts, so the last one started wins."
    )
}

/// The banner shown when schedules are overdue.
pub(crate) fn late_banner(count: u64) -> String {
    let (what, them) = if count == 1 {
        ("1 periodic job is".to_owned(), "it")
    } else {
        (format!("{} periodic jobs are", delimit(count)), "them")
    };
    format!(
        "{what} overdue by more than a minute: no poller enqueued {them} (none runs, or Redis \
         or client middleware keeps failing: see the workers' logs)."
    )
}

/// What was done with a tick, for a chip.
pub(crate) fn result(result: Option<FireResult>) -> &'static str {
    match result {
        Some(FireResult::Enqueued) => "enqueued",
        Some(FireResult::Scheduled) => "scheduled",
        Some(FireResult::Halted) => "stopped by middleware",
        Some(FireResult::Skipped) => "skipped",
        Some(FireResult::Duplicate) => "duplicate",
        Some(FireResult::Manual) => "enqueued by hand",
        _ => "unknown",
    }
}

/// Whether a tick's result deserves attention (a warning chip).
pub(crate) fn result_is_warning(result: Option<FireResult>) -> bool {
    !matches!(
        result,
        Some(FireResult::Enqueued | FireResult::Scheduled | FireResult::Manual)
    )
}

/// How a run ended, for a chip.
pub(crate) fn run_status(status: Option<RunStatus>) -> &'static str {
    match status {
        Some(RunStatus::Succeeded) => "succeeded",
        Some(RunStatus::Failed) => "failed",
        Some(RunStatus::Interrupted) => "interrupted",
        Some(RunStatus::Rescheduled) => "rescheduled",
        _ => "unknown",
    }
}

/// A short span for tables: `45 s`, `3 min`, `2 h 5 min`, `3 d`.
pub(crate) fn short_span(duration: Duration) -> String {
    let secs = duration.as_secs();
    match secs {
        0..60 => format!("{secs} s"),
        60..3_600 => format!("{} min", secs / 60),
        3_600..86_400 => match (secs % 3_600) / 60 {
            0 => format!("{} h", secs / 3_600),
            minutes => format!("{} h {minutes} min", secs / 3_600),
        },
        _ => format!("{} d", delimit(secs / 86_400)),
    }
}

/// How late a tick was handled, when it is worth saying (a second or more): `late 3 min`.
pub(crate) fn late(by: Duration) -> Option<String> {
    (by >= Duration::from_secs(1)).then(|| format!("late {}", short_span(by)))
}

/// A catch-up policy in words.
pub(crate) fn catch_up(policy: CatchUp, deadline: Option<Duration>) -> String {
    let deadline = deadline.map(span);
    match policy {
        CatchUp::Skip => format!(
            "skipped (a tick is enqueued only within {} of its time)",
            deadline.as_deref().unwrap_or("5 minutes")
        ),
        CatchUp::All { max } => {
            let max = format!("at most {} per catch-up", delimit(u64::from(max)));
            match deadline {
                Some(deadline) => format!(
                    "all enqueued, oldest first ({max}; ticks older than {deadline} are \
                     skipped)"
                ),
                None => format!("all enqueued, oldest first ({max})"),
            }
        }
        _ => match deadline {
            Some(deadline) => {
                format!("the latest one is enqueued, if within {deadline} of its time")
            }
            None => "the latest one is enqueued".to_owned(),
        },
    }
}

/// Who defines a schedule: `code (group “default”)` or the API.
pub(crate) fn source(group: Option<&str>) -> String {
    match group {
        Some(group) => format!("code (group “{group}”)"),
        None => "the API, the dashboard or the command line (a dynamic periodic job)".to_owned(),
    }
}

/// The counters in a sentence.
pub(crate) fn counters(fired: u64, missed: u64, skipped: u64, run_failures: u64) -> String {
    format!(
        "{} enqueued, {} missed, {} skipped, {} failed runs",
        delimit(fired),
        delimit(missed),
        delimit(skipped),
        delimit(run_failures)
    )
}

/// The Periodic row of a job's page: `nightly, tick 2026-11-01 02:00:00 UTC`.
pub(crate) fn job_row(name: &str, tick: &str, manual: bool) -> String {
    if manual {
        format!("{name}, enqueued by hand at {tick}")
    } else {
        format!("{name}, tick {tick}")
    }
}

/// The heading of a schedule's page.
pub(crate) fn heading(name: &str) -> String {
    format!("Periodic job “{name}”")
}

/// Why a schedule's page cannot be shown.
pub(crate) const NOT_FOUND: &str = "There is no periodic job with this name: it was deleted, \
    or its code no longer defines it.";

/// A bulk action naming too many schedules.
pub(crate) fn too_many(max: usize) -> String {
    format!(
        "At most {} periodic jobs can be changed at once.",
        delimit(u64::try_from(max).unwrap_or(u64::MAX))
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sentences() {
        assert_eq!(summary(0, 0), "No periodic job is defined.");
        assert_eq!(summary(1, 0), "1 periodic job.");
        assert_eq!(summary(12, 1), "12 periodic jobs, 1 paused.");
        assert_eq!(summary(1_234, 20), "1,234 periodic jobs, 20 paused.");
        assert_eq!(
            filter_summary(2, Outcome::Complete, "night*"),
            "2 periodic jobs matching “night*”."
        );
        assert_eq!(
            filter_summary(
                1,
                Outcome::ScanLimit {
                    searched: 50_000,
                    total: 60_000
                },
                "x"
            ),
            "1 periodic job matching “x” in the first 50,000 of 60,000 periodic jobs; the \
             others were not searched."
        );
        assert_eq!(no_matches(Outcome::Complete), PERIODIC.no_matches);
        assert_eq!(
            no_matches(Outcome::ScanLimit {
                searched: 1,
                total: 2
            }),
            PERIODIC.no_matches_searched
        );
        assert!(late_banner(1).starts_with("1 periodic job is overdue"));
        assert!(late_banner(1).contains("enqueued it"));
        assert!(late_banner(3).starts_with("3 periodic jobs are overdue"));
        assert!(crontabs_differ("default", 2).contains("group “default” define 2 different"));
        assert_eq!(job_row("nightly", "x", false), "nightly, tick x");
        assert_eq!(
            job_row("nightly", "x", true),
            "nightly, enqueued by hand at x"
        );
        assert_eq!(
            counters(5, 2, 1, 0),
            "5 enqueued, 2 missed, 1 skipped, 0 failed runs"
        );
        assert_eq!(source(Some("web")), "code (group “web”)");
        assert!(source(None).contains("dynamic"));
        assert_eq!(heading("a/b"), "Periodic job “a/b”");
        assert_eq!(
            too_many(1_000),
            "At most 1,000 periodic jobs can be changed at once."
        );
    }

    #[test]
    fn spans_and_lateness() {
        assert_eq!(short_span(Duration::from_secs(45)), "45 s");
        assert_eq!(short_span(Duration::from_secs(180)), "3 min");
        assert_eq!(short_span(Duration::from_secs(7_200)), "2 h");
        assert_eq!(short_span(Duration::from_secs(7_500)), "2 h 5 min");
        assert_eq!(short_span(Duration::from_secs(86_400 * 3)), "3 d");
        assert_eq!(late(Duration::from_millis(400)), None);
        assert_eq!(
            late(Duration::from_secs(180)).as_deref(),
            Some("late 3 min")
        );
    }

    #[test]
    fn policies_and_results() {
        assert_eq!(
            catch_up(CatchUp::Latest, None),
            "the latest one is enqueued"
        );
        assert_eq!(
            catch_up(CatchUp::Skip, Some(Duration::from_secs(300))),
            "skipped (a tick is enqueued only within 5 minutes of its time)"
        );
        assert_eq!(
            catch_up(CatchUp::All { max: 100 }, None),
            "all enqueued, oldest first (at most 100 per catch-up)"
        );
        assert_eq!(
            catch_up(CatchUp::Latest, Some(Duration::from_secs(3_600))),
            "the latest one is enqueued, if within 1 hour of its time"
        );
        assert_eq!(result(Some(FireResult::Halted)), "stopped by middleware");
        assert_eq!(result(None), "unknown");
        assert!(result_is_warning(Some(FireResult::Duplicate)));
        assert!(result_is_warning(None));
        assert!(!result_is_warning(Some(FireResult::Enqueued)));
        assert!(!result_is_warning(Some(FireResult::Manual)));
        assert_eq!(run_status(Some(RunStatus::Succeeded)), "succeeded");
        assert_eq!(run_status(None), "unknown");
    }

    #[test]
    fn notices() {
        let mut paused = Notice::new(NoticeKind::PeriodicPaused, 3, 0);
        assert_eq!(notice(&paused, ""), "Paused 3 schedules.");
        paused.count = 1;
        assert_eq!(notice(&paused, ""), "Paused 1 schedule.");
        assert_eq!(notice(&paused, "nightly"), "Paused schedule “nightly”.");
        let enqueued = Notice::new(NoticeKind::PeriodicEnqueued, 2, 0);
        assert_eq!(
            notice(&enqueued, ""),
            "Enqueued the jobs of 2 schedules now."
        );
        let one = Notice::about(NoticeKind::PeriodicEnqueued, "nightly");
        assert_eq!(
            notice(&one, "nightly"),
            "Enqueued the job of schedule “nightly” now."
        );
        let none = Notice {
            failed: 1,
            ..Notice::about(NoticeKind::PeriodicEnqueued, "nightly")
        };
        assert!(notice(&none, "nightly").starts_with("Did not enqueue the job of schedule"));
        let deleted = Notice::new(NoticeKind::PeriodicDeleted, 2, 0);
        assert_eq!(notice(&deleted, ""), "Deleted 2 schedules.");
        let resumed = Notice::about(NoticeKind::PeriodicResumed, "a/b");
        assert_eq!(notice(&resumed, "a/b"), "Resumed schedule “a/b”.");
    }
}
