//! Every piece of text the dashboard shows, in one place (English only for now). Templates
//! read the fixed labels from [`Strings`] (as `s.<name>`); messages that embed numbers or
//! names are built by the functions below.

use crate::flash::{Notice, NoticeKind};
use crate::search::Outcome;

/// The fixed labels of the dashboard.
pub(crate) struct Strings {
    // Navigation and page frame.
    pub(crate) dashboard: &'static str,
    pub(crate) busy: &'static str,
    pub(crate) queues: &'static str,
    pub(crate) retries: &'static str,
    pub(crate) scheduled: &'static str,
    pub(crate) dead: &'static str,
    pub(crate) metrics: &'static str,
    pub(crate) skip_to_content: &'static str,
    pub(crate) main_navigation: &'static str,
    pub(crate) back_to_app: &'static str,
    pub(crate) theme_toggle: &'static str,
    pub(crate) theme_auto: &'static str,
    pub(crate) theme_light: &'static str,
    pub(crate) theme_dark: &'static str,
    pub(crate) live: &'static str,
    pub(crate) live_help: &'static str,
    pub(crate) read_only_badge: &'static str,
    pub(crate) docs: &'static str,
    pub(crate) server_time: &'static str,
    pub(crate) just_now: &'static str,

    // Totals shown on every page.
    pub(crate) totals: &'static str,
    pub(crate) processed: &'static str,
    pub(crate) failed: &'static str,
    pub(crate) enqueued: &'static str,

    // Dashboard.
    pub(crate) live_chart: &'static str,
    pub(crate) live_chart_help: &'static str,
    pub(crate) poll_interval: &'static str,
    pub(crate) seconds_unit: &'static str,
    pub(crate) history: &'static str,
    pub(crate) history_range: &'static str,
    pub(crate) history_chart_help: &'static str,
    pub(crate) one_week: &'static str,
    pub(crate) one_month: &'static str,
    pub(crate) three_months: &'static str,
    pub(crate) six_months: &'static str,
    pub(crate) show_as_table: &'static str,
    pub(crate) date: &'static str,
    pub(crate) waiting_for_data: &'static str,
    pub(crate) live_status_template: &'static str,
    pub(crate) chart_unavailable: &'static str,
    pub(crate) version: &'static str,
    pub(crate) uptime_days: &'static str,
    pub(crate) connections: &'static str,
    pub(crate) memory_usage: &'static str,
    pub(crate) peak_memory: &'static str,
    pub(crate) redis_unavailable: &'static str,

    // Busy.
    pub(crate) status: &'static str,
    pub(crate) processes: &'static str,
    pub(crate) threads: &'static str,
    pub(crate) utilization: &'static str,
    pub(crate) rss: &'static str,
    pub(crate) process: &'static str,
    pub(crate) started: &'static str,
    pub(crate) last_beat: &'static str,
    pub(crate) quiet: &'static str,
    pub(crate) stop: &'static str,
    pub(crate) quiet_all: &'static str,
    pub(crate) stop_all: &'static str,
    pub(crate) confirm_quiet: &'static str,
    pub(crate) confirm_stop: &'static str,
    pub(crate) confirm_quiet_all: &'static str,
    pub(crate) confirm_stop_all: &'static str,
    pub(crate) all_skips_embedded: &'static str,
    pub(crate) embedded: &'static str,
    pub(crate) quiet_label: &'static str,
    /// The chip of a process that does not understand a feature in use.
    pub(crate) lacks: &'static str,
    /// Its explanation.
    pub(crate) lacks_help: &'static str,
    pub(crate) unknown: &'static str,
    pub(crate) no_processes: &'static str,
    pub(crate) jobs: &'static str,
    pub(crate) slot: &'static str,
    pub(crate) jid: &'static str,
    pub(crate) queue: &'static str,
    pub(crate) job: &'static str,
    pub(crate) arguments: &'static str,
    pub(crate) no_running_jobs: &'static str,
    pub(crate) running_since: &'static str,

    // Queues.
    pub(crate) size: &'static str,
    pub(crate) latency: &'static str,
    pub(crate) actions: &'static str,
    pub(crate) paused: &'static str,
    pub(crate) pause: &'static str,
    pub(crate) unpause: &'static str,
    pub(crate) delete: &'static str,
    pub(crate) delete_queue: &'static str,
    pub(crate) confirm_delete_queue: &'static str,
    pub(crate) no_queues: &'static str,
    pub(crate) position: &'static str,
    pub(crate) newest_first: &'static str,
    pub(crate) oldest_first: &'static str,
    pub(crate) show_all: &'static str,
    pub(crate) confirm_delete_job: &'static str,
    pub(crate) empty_queue: &'static str,
    pub(crate) waiting_since: &'static str,

    // Sorted sets (retries, scheduled, dead).
    pub(crate) retries_title: &'static str,
    pub(crate) scheduled_title: &'static str,
    pub(crate) dead_title: &'static str,
    pub(crate) next_retry: &'static str,
    pub(crate) retry_count: &'static str,
    pub(crate) when: &'static str,
    pub(crate) died: &'static str,
    pub(crate) error: &'static str,
    pub(crate) retry_now: &'static str,
    pub(crate) kill: &'static str,
    pub(crate) add_to_queue: &'static str,
    pub(crate) retry_all: &'static str,
    pub(crate) delete_all: &'static str,
    pub(crate) kill_all: &'static str,
    pub(crate) add_all_to_queue: &'static str,
    pub(crate) selected: &'static str,
    pub(crate) everything: &'static str,
    pub(crate) confirm_delete_selected: &'static str,
    pub(crate) confirm_kill_selected: &'static str,
    pub(crate) select_all: &'static str,
    pub(crate) select_job: &'static str,
    pub(crate) no_retries: &'static str,
    pub(crate) no_scheduled: &'static str,
    pub(crate) no_dead: &'static str,
    pub(crate) no_matches: &'static str,
    pub(crate) no_matches_searched: &'static str,
    pub(crate) filter: &'static str,
    pub(crate) filter_placeholder: &'static str,
    pub(crate) apply_filter: &'static str,
    pub(crate) clear_filter: &'static str,

    // Job details.
    pub(crate) tags: &'static str,
    pub(crate) created_at: &'static str,
    pub(crate) enqueued_at: &'static str,
    pub(crate) not_enqueued: &'static str,
    pub(crate) extras: &'static str,
    pub(crate) last_retry: &'static str,
    pub(crate) first_failed: &'static str,
    pub(crate) scheduled_for: &'static str,
    pub(crate) error_class: &'static str,
    pub(crate) error_message: &'static str,
    pub(crate) backtrace: &'static str,
    pub(crate) raw_json: &'static str,
    pub(crate) invalid_json: &'static str,
    pub(crate) go_back: &'static str,

    // Metrics.
    pub(crate) metrics_help: &'static str,
    pub(crate) metrics_period: &'static str,
    pub(crate) metrics_filter_placeholder: &'static str,
    pub(crate) job_class: &'static str,
    pub(crate) executions: &'static str,
    /// Executions rescheduled over a limiter's limit (shown when some class has some).
    pub(crate) over_limit: &'static str,
    /// Jobs a throttle parked before they ran (shown when some class has some).
    pub(crate) throttled: &'static str,
    /// Members of cancelled batches skipped (shown when some class has some).
    pub(crate) skipped: &'static str,
    pub(crate) total_time: &'static str,
    pub(crate) average_time: &'static str,
    pub(crate) sort_by: &'static str,
    pub(crate) chart_series_help: &'static str,
    pub(crate) time_per_minute: &'static str,
    pub(crate) time_per_ten_minutes: &'static str,
    pub(crate) executions_per_minute: &'static str,
    pub(crate) no_metrics: &'static str,
    pub(crate) no_metrics_match: &'static str,
    pub(crate) deploys: &'static str,
    pub(crate) deploy: &'static str,
    pub(crate) failure_rate: &'static str,
    pub(crate) total_time_fact: &'static str,
    pub(crate) average_fact: &'static str,
    pub(crate) median: &'static str,
    pub(crate) p95: &'static str,
    pub(crate) duration: &'static str,
    pub(crate) distribution: &'static str,
    pub(crate) distribution_help: &'static str,
    pub(crate) heatmap: &'static str,
    pub(crate) heatmap_help: &'static str,
    pub(crate) heatmap_fewer: &'static str,
    pub(crate) heatmap_more: &'static str,
    pub(crate) per_minute_table: &'static str,
    pub(crate) auto_refresh: &'static str,
    pub(crate) seconds_suffix: &'static str,
    pub(crate) jobs_suffix: &'static str,
    pub(crate) job_suffix: &'static str,
    pub(crate) time_utc: &'static str,
    pub(crate) iteration: &'static str,
    pub(crate) cancelled: &'static str,

    // Pagination.
    pub(crate) pagination: &'static str,
    pub(crate) first_page: &'static str,
    pub(crate) previous_page: &'static str,
    pub(crate) next_page: &'static str,
    pub(crate) last_page: &'static str,

    // Error pages.
    pub(crate) back_to_dashboard: &'static str,
}

/// The English strings.
pub(crate) static EN: Strings = Strings {
    dashboard: "Dashboard",
    busy: "Busy",
    queues: "Queues",
    retries: "Retries",
    scheduled: "Scheduled",
    dead: "Dead",
    metrics: "Metrics",
    skip_to_content: "Skip to content",
    main_navigation: "Main navigation",
    back_to_app: "Back to app",
    theme_toggle: "Color theme",
    theme_auto: "Theme: automatic",
    theme_light: "Theme: light",
    theme_dark: "Theme: dark",
    live: "Live",
    live_help: "Refresh this page automatically",
    read_only_badge: "read-only",
    docs: "Docs",
    server_time: "Server time",
    just_now: "just now",

    totals: "Totals",
    processed: "Processed",
    failed: "Failed",
    enqueued: "Enqueued",

    live_chart: "Jobs per refresh",
    live_chart_help: "Jobs processed and failed between two refreshes, newest on the right.",
    poll_interval: "Refresh every",
    seconds_unit: "s",
    history: "History",
    history_range: "History range",
    history_chart_help: "Jobs processed and failed per day (UTC), oldest on the left.",
    one_week: "1 week",
    one_month: "1 month",
    three_months: "3 months",
    six_months: "6 months",
    show_as_table: "Show as a table",
    date: "Date",
    waiting_for_data: "Waiting for the next refresh…",
    live_status_template: "Updated: {processed} processed, {failed} failed.",
    chart_unavailable: "Could not refresh the statistics; retrying.",
    version: "Version",
    uptime_days: "Uptime (days)",
    connections: "Connections",
    memory_usage: "Memory usage",
    peak_memory: "Peak memory usage",
    redis_unavailable: "The server did not answer INFO; its details are not available.",

    status: "Status",
    processes: "Processes",
    threads: "Threads",
    utilization: "Utilization",
    rss: "Memory (RSS)",
    process: "Process",
    started: "Started",
    last_beat: "Last heartbeat",
    quiet: "Quiet",
    stop: "Stop",
    quiet_all: "Quiet all",
    stop_all: "Stop all",
    confirm_quiet: "Stop this process from taking new jobs?",
    confirm_stop: "Shut this process down?",
    confirm_quiet_all: "Stop every process from taking new jobs?",
    confirm_stop_all: "Shut every process down?",
    all_skips_embedded: "Quiet all and Stop all leave embedded processes alone.",
    embedded: "embedded",
    quiet_label: "quiet",
    lacks: "lacks",
    lacks_help: "This process runs a version of hench that ignores these features, which are in use: upgrade it.",
    unknown: "unknown",
    no_processes: "No processes are running.",
    jobs: "Jobs",
    slot: "Slot",
    jid: "JID",
    queue: "Queue",
    job: "Job",
    arguments: "Arguments",
    no_running_jobs: "No jobs are running.",
    running_since: "Started",

    size: "Size",
    latency: "Latency (s)",
    actions: "Actions",
    paused: "paused",
    pause: "Pause",
    unpause: "Resume",
    delete: "Delete",
    delete_queue: "Delete queue",
    confirm_delete_queue: "Delete this queue and every job in it?",
    no_queues: "There are no queues yet.",
    position: "#",
    newest_first: "Newest first",
    oldest_first: "Oldest first",
    show_all: "Show all",
    confirm_delete_job: "Delete this job?",
    empty_queue: "This queue is empty.",
    waiting_since: "waiting since",

    retries_title: "Retries",
    scheduled_title: "Scheduled jobs",
    dead_title: "Dead jobs",
    next_retry: "Next retry",
    retry_count: "Retries",
    when: "When",
    died: "Died",
    error: "Error",
    retry_now: "Retry now",
    kill: "Kill",
    add_to_queue: "Enqueue now",
    retry_all: "Retry all",
    delete_all: "Delete all",
    kill_all: "Kill all",
    add_all_to_queue: "Enqueue all",
    selected: "Selected jobs",
    everything: "Every job",
    confirm_delete_selected: "Delete the selected jobs?",
    confirm_kill_selected: "Move the selected jobs to the dead set?",
    select_all: "Select all jobs on this page",
    select_job: "Select job",
    no_retries: "No retries found.",
    no_scheduled: "No scheduled jobs found.",
    no_dead: "No dead jobs found.",
    no_matches: "No jobs match this filter.",
    no_matches_searched: "None of the jobs searched match this filter.",
    filter: "Filter",
    filter_placeholder: "Any job content",
    apply_filter: "Search",
    clear_filter: "Clear",

    tags: "Tags",
    created_at: "Created",
    enqueued_at: "Enqueued",
    not_enqueued: "Not enqueued yet",
    extras: "Extras",
    last_retry: "Last retry",
    first_failed: "First failed",
    scheduled_for: "Scheduled for",
    error_class: "Error class",
    error_message: "Error message",
    backtrace: "Backtrace",
    raw_json: "Raw JSON",
    invalid_json: "This job is not valid JSON",
    go_back: "Back",

    metrics_help: "Executions, failures and time spent per job class, as recorded by the \
        workers on every heartbeat. Times are UTC.",
    metrics_period: "Period",
    metrics_filter_placeholder: "Job class",
    job_class: "Job class",
    executions: "Executions",
    over_limit: "Over limit",
    throttled: "Throttled",
    skipped: "Skipped",
    total_time: "Total time (s)",
    average_time: "Average (ms)",
    sort_by: "Sort by",
    chart_series_help: "Tick job classes in the table to compare them in the chart (8 at most).",
    time_per_minute: "Execution time per minute (s)",
    time_per_ten_minutes: "Execution time per 10 minutes (s)",
    executions_per_minute: "Executions per minute",
    no_metrics: "No job ran in this period.",
    no_metrics_match: "No job class matches this filter in this period.",
    deploys: "Deploys",
    deploy: "Deploy",
    failure_rate: "Failure rate",
    total_time_fact: "Total time",
    average_fact: "Average",
    median: "Median",
    p95: "95th percentile",
    duration: "Duration",
    distribution: "Execution time distribution",
    distribution_help: "Executions by how long they took, fastest on the left.",
    heatmap: "Execution times over time",
    heatmap_help: "Each cell counts the executions of one minute that took about that long; \
        the key shows which shades count more.",
    heatmap_fewer: "fewer",
    heatmap_more: "more",
    per_minute_table: "Per minute",
    auto_refresh: "This page refreshes every minute.",
    seconds_suffix: "s",
    jobs_suffix: "jobs",
    job_suffix: "job",
    time_utc: "Time (UTC)",
    iteration: "Iteration",
    cancelled: "cancelled",

    pagination: "Pages",
    first_page: "First",
    previous_page: "Previous",
    next_page: "Next",
    last_page: "Last",

    back_to_dashboard: "Back to the dashboard",
};

/// A relative time: `"3 minutes ago"`, `"in 1 hour"` (the wording of the browser's
/// `Intl.RelativeTimeFormat("en", {numeric: "always"})`, which refreshes these in place).
pub(crate) fn relative_time(count: u64, unit: crate::format::TimeUnit, past: bool) -> String {
    use crate::format::TimeUnit;
    let unit = match unit {
        TimeUnit::Year => "year",
        TimeUnit::Month => "month",
        TimeUnit::Day => "day",
        TimeUnit::Hour => "hour",
        TimeUnit::Minute => "minute",
        TimeUnit::Second => "second",
    };
    let phrase = format!(
        "{} {unit}{}",
        crate::format::delimit(count),
        if count == 1 { "" } else { "s" }
    );
    if past {
        format!("{phrase} ago")
    } else {
        format!("in {phrase}")
    }
}

/// A time the calendar cannot show, as its Unix time: `Unix time 10000000000000`.
pub(crate) fn unix_time(seconds: i64) -> String {
    format!("Unix time {seconds}")
}

/// `"1 job"`, `"3 jobs"`.
pub(crate) fn jobs(count: u64) -> String {
    plural(count, "job", "jobs")
}

/// `"1 process"`, `"3 processes"`.
pub(crate) fn processes(count: u64) -> String {
    plural(count, "process", "processes")
}

fn plural(count: u64, one: &str, many: &str) -> String {
    format!(
        "{} {}",
        crate::format::delimit(count),
        if count == 1 { one } else { many }
    )
}

/// The page title for a list with a count: `Retries (3)`.
pub(crate) fn counted(label: &str, count: u64) -> String {
    format!("{label} ({})", crate::format::delimit(count))
}

/// The summary of a filtered list: how many jobs matched, and how much was searched.
pub(crate) fn filter_summary(count: usize, outcome: Outcome, filter: &str) -> String {
    let count = u64::try_from(count).unwrap_or(u64::MAX);
    match outcome {
        Outcome::Complete => format!("{} matching “{filter}”.", jobs(count)),
        Outcome::MatchLimit => format!("Showing the first {} matching “{filter}”.", jobs(count)),
        Outcome::ScanLimit { searched, total } => format!(
            "{} matching “{filter}” in the first {} of {}; the others were not searched.",
            jobs(count),
            crate::format::delimit(searched),
            jobs(total)
        ),
    }
}

/// What a filtered list without matches says.
pub(crate) fn no_matches(outcome: Outcome) -> &'static str {
    match outcome {
        Outcome::ScanLimit { .. } => EN.no_matches_searched,
        Outcome::Complete | Outcome::MatchLimit => EN.no_matches,
    }
}

/// Every job of a set of `total`, for confirmations: `all 340 dead jobs`, `the only retry`.
pub(crate) fn all_of(total: u64, one: &str, many: &str) -> String {
    if total == 1 {
        format!("the only {one}")
    } else {
        format!("all {} {many}", crate::format::delimit(total))
    }
}

/// The confirmation of "Retry all": `Retry all 30 retries now?`.
pub(crate) fn confirm_retry_all(all: &str) -> String {
    format!("Retry {all} now?")
}

/// The confirmation of "Enqueue all": `Enqueue all 11 scheduled jobs now?`.
pub(crate) fn confirm_enqueue_all(all: &str) -> String {
    format!("Enqueue {all} now?")
}

/// The confirmation of "Delete all": `Delete all 340 dead jobs?`.
pub(crate) fn confirm_delete_all(all: &str) -> String {
    format!("Delete {all}?")
}

/// The confirmation of "Kill all": `Move all 30 retries to the dead set?`.
pub(crate) fn confirm_kill_all(all: &str) -> String {
    format!("Move {all} to the dead set?")
}

/// The title of the busy page: `Busy (3/10)`.
pub(crate) fn busy_title(busy: u64, threads: u64) -> String {
    format!(
        "{} ({}/{})",
        EN.busy,
        crate::format::delimit(busy),
        crate::format::delimit(threads)
    )
}

/// The page title shown in the browser: `Retries (3) · Hench`, or the product name alone.
pub(crate) fn page_title(page: Option<&str>, product: &str) -> String {
    match page {
        Some(page) if !page.is_empty() => format!("{page} · {product}"),
        _ => product.to_owned(),
    }
}

/// `Page 2 of 7`.
pub(crate) fn page_of(page: u64, pages: u64) -> String {
    format!(
        "Page {} of {}",
        crate::format::delimit(page),
        crate::format::delimit(pages)
    )
}

/// The heading of a queue's page.
pub(crate) fn queue_heading(name: &str) -> String {
    format!("Queue “{}”", crate::format::visible(name))
}

/// Footer text: `hench 0.1.0`.
pub(crate) fn product_version() -> String {
    format!("hench {}", env!("CARGO_PKG_VERSION"))
}

/// The text of a one-shot notice shown after an action.
pub(crate) fn notice(notice: &Notice) -> String {
    let jobs_done = jobs(notice.count);
    let subject = crate::format::visible(&notice.subject);
    let mut text = match notice.kind {
        NoticeKind::Retried => format!("Retrying {jobs_done}."),
        NoticeKind::Enqueued => format!("Enqueued {jobs_done}."),
        NoticeKind::Deleted => format!("Deleted {jobs_done}."),
        NoticeKind::Killed => format!("Moved {jobs_done} to the dead set."),
        NoticeKind::QueueCleared => format!("Deleted queue “{subject}”."),
        NoticeKind::QueuePaused => format!("Paused queue “{subject}”."),
        NoticeKind::QueueUnpaused => format!("Resumed queue “{subject}”."),
        NoticeKind::Quieted => format!("Asked {} to quiet down.", processes(notice.count)),
        NoticeKind::Stopped => format!("Asked {} to stop.", processes(notice.count)),
        NoticeKind::NothingSelected => "Select at least one job first.".to_owned(),
        NoticeKind::Gone => "That job is gone: it already ran, moved or was deleted.".to_owned(),
        NoticeKind::ProcessGone => "That process is no longer running.".to_owned(),
        NoticeKind::PeriodicEnqueued
        | NoticeKind::PeriodicPaused
        | NoticeKind::PeriodicResumed
        | NoticeKind::PeriodicDeleted => periodic::notice(notice, &subject),
        NoticeKind::BatchCancelled
        | NoticeKind::BatchDeleted
        | NoticeKind::MemberSkipped
        | NoticeKind::CallbacksDispatched => batches::notice(notice, &subject),
        NoticeKind::LimitsReset | NoticeKind::LineCleared => limits::notice(notice, &subject),
        NoticeKind::LocksReleased => locks::notice(notice, &subject),
    };
    if notice.failed > 0 {
        text.push_str(&format!(
            " {} could not be changed; see the server logs.",
            jobs(notice.failed)
        ));
    }
    text
}

/// What a feature's notice is about: the named item (`“nightly”`) when there is a subject,
/// else the count of items (`3 schedules`).
pub(crate) fn count_or_subject(notice: &Notice, subject: &str, one: &str, many: &str) -> String {
    if subject.is_empty() {
        let unit = if notice.count == 1 { one } else { many };
        format!("{} {unit}", crate::format::delimit(notice.count))
    } else {
        format!("{one} “{subject}”")
    }
}

/// A metrics period label in words: `"1h"` → `"the last hour"`, `"24h"` → `"the last 24
/// hours"` (labels are `<n>h` or `<n>m`).
pub(crate) fn period_phrase(label: &str) -> String {
    let (number, unit) = match label.strip_suffix('h') {
        Some(number) => (number, ("hour", "hours")),
        None => (label.trim_end_matches('m'), ("minute", "minutes")),
    };
    match number {
        "1" => format!("the last {}", unit.0),
        _ => format!("the last {number} {}", unit.1),
    }
}

/// The subtitle of the metrics page: what it shows and for how long.
pub(crate) fn metrics_summary(classes: usize, label: &str) -> String {
    let count = u64::try_from(classes).unwrap_or(u64::MAX);
    format!(
        "{} ran in {}.",
        plural(count, "job class", "job classes"),
        period_phrase(label)
    )
}

/// The subtitle of the metrics page when it is filtered.
pub(crate) fn metrics_matching(classes: usize, filter: &str, label: &str) -> String {
    let count = u64::try_from(classes).unwrap_or(u64::MAX);
    format!(
        "{} matching “{filter}” ran in {}.",
        plural(count, "job class", "job classes"),
        period_phrase(label)
    )
}

/// The accessible name of the checkbox that charts a job class.
pub(crate) fn chart_toggle(class: &str) -> String {
    format!("Show “{class}” in the chart")
}

/// The note under a metrics table cut to its busiest rows.
pub(crate) fn metrics_cut(shown: usize, total: usize) -> String {
    format!(
        "Showing the first {} of {} job classes; filter by name to find the others.",
        crate::format::delimit(u64::try_from(shown).unwrap_or(u64::MAX)),
        crate::format::delimit(u64::try_from(total).unwrap_or(u64::MAX))
    )
}

/// The time span a metrics page covers.
pub(crate) fn metrics_window(starts_at: &str, ends_at: &str) -> String {
    format!("Data from {starts_at} to {ends_at}.")
}

/// The heading of a job class's metrics page.
pub(crate) fn job_metrics_heading(class: &str) -> String {
    format!("Metrics of “{class}”")
}

/// Shown when a job class has no metrics in the period.
pub(crate) fn no_job_metrics(class: &str, label: &str) -> String {
    format!(
        "No execution of “{class}” was recorded in {}.",
        period_phrase(label)
    )
}

/// Why longer periods are not offered for one job class.
pub(crate) const JOB_PERIOD_NOTE: &str =
    "Execution-time histograms are kept for 8 hours, so one job class shows 8 hours at most.";

/// A job's iteration state: `cursor 500 · 7 executions · 12.345 s`.
pub(crate) fn iteration(cursor: Option<&str>, executions: u64, runtime_secs: f64) -> String {
    let cursor = match cursor {
        Some(cursor) => format!("cursor {cursor}"),
        None => "not started".to_owned(),
    };
    format!(
        "{cursor} · {} · {} s",
        plural(executions, "execution", "executions"),
        crate::format::decimal(runtime_secs, 3)
    )
}

/// Why an unknown queue cannot be shown.
pub(crate) fn queue_not_found(name: &str) -> String {
    format!(
        "There is no queue named “{}”.",
        crate::format::visible(name)
    )
}

/// Why an entry of a set cannot be shown.
pub(crate) fn entry_not_found(set: &str) -> String {
    format!("This job is no longer in {set}: it may have run, moved or been deleted.")
}

/// Error page headings and messages.
// The texts of the features' pages, one part each.
pub(crate) mod batches;
pub(crate) mod limits;
pub(crate) mod locks;
pub(crate) mod periodic;

pub(crate) mod errors {
    /// 404 heading.
    pub(crate) const NOT_FOUND: &str = "Not found";
    /// 404 for unknown paths.
    pub(crate) const NO_SUCH_PAGE: &str = "This page does not exist.";
    /// 400 heading.
    pub(crate) const BAD_REQUEST: &str = "Bad request";
    /// 403 heading.
    pub(crate) const FORBIDDEN: &str = "Forbidden";
    /// 415 heading.
    pub(crate) const UNSUPPORTED: &str = "Unsupported form";
    /// 500 heading.
    pub(crate) const UNAVAILABLE: &str = "Redis is not answering";
    /// 500 heading for other errors.
    pub(crate) const INTERNAL: &str = "Something went wrong";
    /// POST in read-only mode.
    pub(crate) const READ_ONLY: &str = "This dashboard is read-only: it cannot change anything.";
    /// POST from another site.
    pub(crate) const CROSS_SITE: &str =
        "This request came from another site; changes are only accepted from the dashboard itself.";
    /// POST without a matching CSRF token.
    pub(crate) const BAD_TOKEN: &str =
        "The form is outdated or was not sent by the dashboard. Reload the page and try again.";
    /// POST without a form body.
    pub(crate) const NOT_A_FORM: &str = "Changes must be sent as a form.";
    /// A POST naming no action.
    pub(crate) const NO_ACTION: &str = "The form did not say what to do.";
    /// Redis failure detail.
    pub(crate) const REDIS: &str = "Hench could not read or change its data in Redis";
    /// A malformed `only` parameter.
    pub(crate) const BAD_ONLY: &str = "`only` must be `jobs` or `processes`.";
    /// A malformed `days` parameter.
    pub(crate) const BAD_DAYS: &str = "`days` must be a whole number between 1 and 180.";
    /// A malformed `direction` parameter.
    pub(crate) const BAD_DIRECTION: &str = "`direction` must be `asc` or `desc`.";
    /// A malformed metrics `period` parameter.
    pub(crate) const BAD_PERIOD: &str = "`period` must be one of 1h, 2h, 4h, 8h, 24h, 48h and 72h.";
    /// A malformed metrics `sort` parameter.
    pub(crate) const BAD_SORT: &str = "`sort` must be `count`, `failures`, `time` or `average`.";
    /// A filter that is too long.
    pub(crate) const LONG_FILTER: &str = "The filter is too long (256 characters at most).";
    /// The random source failed.
    pub(crate) const NO_RANDOMNESS: &str = "The operating system's random source failed.";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn feature_notices() {
        let notice = |kind, count, subject: &str| Notice {
            kind,
            count,
            failed: 0,
            subject: subject.to_owned(),
        };
        assert_eq!(
            super::notice(&notice(NoticeKind::PeriodicPaused, 0, "nightly")),
            "Paused schedule “nightly”."
        );
        assert_eq!(
            super::notice(&notice(NoticeKind::PeriodicEnqueued, 0, "nightly")),
            "Enqueued the job of schedule “nightly” now."
        );
        assert_eq!(
            super::notice(&notice(NoticeKind::BatchCancelled, 3, "")),
            "Cancelled 3 batches."
        );
        assert_eq!(
            super::notice(&notice(NoticeKind::MemberSkipped, 1, "")),
            "Skipped 1 pending member."
        );
        assert_eq!(
            super::notice(&notice(NoticeKind::LineCleared, 0, "w:stripe")),
            "Cleared the wait line of limit “w:stripe”."
        );
        assert_eq!(
            super::notice(&notice(NoticeKind::LocksReleased, 2, "")),
            "Released 2 locks."
        );
    }

    #[test]
    fn counts_are_pluralized() {
        assert_eq!(jobs(1), "1 job");
        assert_eq!(jobs(0), "0 jobs");
        assert_eq!(jobs(1_500), "1,500 jobs");
        assert_eq!(processes(2), "2 processes");
        assert_eq!(counted("Retries", 3), "Retries (3)");
        assert_eq!(busy_title(4, 10), "Busy (4/10)");
        assert_eq!(
            page_title(Some("Queues (1)"), "Hench"),
            "Queues (1) · Hench"
        );
        assert_eq!(page_title(None, "Hench"), "Hench");
        assert_eq!(page_of(2, 7), "Page 2 of 7");
        assert_eq!(period_phrase("1h"), "the last hour");
        assert_eq!(period_phrase("72h"), "the last 72 hours");
        assert_eq!(period_phrase("90m"), "the last 90 minutes");
        assert_eq!(
            metrics_summary(1, "1h"),
            "1 job class ran in the last hour."
        );
        assert_eq!(
            metrics_summary(1_200, "8h"),
            "1,200 job classes ran in the last 8 hours."
        );
        assert!(metrics_cut(25, 40).starts_with("Showing the first 25 of 40 job classes"));
        assert_eq!(
            iteration(Some("{\"offset\":500}"), 7, 12.3456),
            "cursor {\"offset\":500} · 7 executions · 12.346 s"
        );
        assert_eq!(
            iteration(None, 1, 0.0),
            "not started · 1 execution · 0.000 s"
        );
        assert_eq!(
            filter_summary(3, Outcome::Complete, "x"),
            "3 jobs matching “x”."
        );
        assert_eq!(
            filter_summary(500, Outcome::MatchLimit, "x"),
            "Showing the first 500 jobs matching “x”."
        );
        assert_eq!(
            filter_summary(
                1,
                Outcome::ScanLimit {
                    searched: 50_000,
                    total: 1_000_000
                },
                "x"
            ),
            "1 job matching “x” in the first 50,000 of 1,000,000 jobs; the others were not \
             searched."
        );
        assert_eq!(all_of(1, "retry", "retries"), "the only retry");
        assert_eq!(all_of(1_200, "retry", "retries"), "all 1,200 retries");
        assert_eq!(
            confirm_delete_all(&all_of(340, "dead job", "dead jobs")),
            "Delete all 340 dead jobs?"
        );
    }

    #[test]
    fn notices_describe_what_happened() {
        let notice = |kind, count, failed, subject: &str| Notice {
            kind,
            count,
            failed,
            subject: subject.to_owned(),
        };
        assert_eq!(
            super::notice(&notice(NoticeKind::Retried, 2, 0, "")),
            "Retrying 2 jobs."
        );
        assert_eq!(
            super::notice(&notice(NoticeKind::Deleted, 1, 1, "")),
            "Deleted 1 job. 1 job could not be changed; see the server logs."
        );
        assert_eq!(
            super::notice(&notice(NoticeKind::QueuePaused, 0, 0, "mail")),
            "Paused queue “mail”."
        );
        assert_eq!(
            super::notice(&notice(NoticeKind::Stopped, 1, 0, "")),
            "Asked 1 process to stop."
        );
    }
}
