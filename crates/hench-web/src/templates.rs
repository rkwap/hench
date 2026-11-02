//! The askama templates (in `templates/`) and the data they render. Every value is escaped
//! by askama's HTML escaper; only `extension.html` inserts HTML as it is: the content of a
//! host application's page ([`crate::Page::render`]).

use askama::Template;

use crate::assets::AssetUrls;
use crate::format;
use crate::strings::Strings;
use crate::views::{
    BusyStatus, DayView, EntryView, FactView, JobRow, MarkView, MetricsRow, Pager, ProcessRow,
    QueueRow, RangeLink, RedisView, SortColumn, TableRow, Totals,
};

/// A link of the main navigation.
pub(crate) struct NavLink {
    pub(crate) name: String,
    pub(crate) href: String,
    pub(crate) active: bool,
}

/// A notice left by the previous action.
pub(crate) struct NoticeView {
    pub(crate) text: String,
    pub(crate) warning: bool,
}

/// The page footer.
pub(crate) struct Footer {
    pub(crate) version: String,
    pub(crate) redis: String,
    pub(crate) time: String,
    pub(crate) time_iso: String,
    pub(crate) docs: &'static str,
}

/// What the shared layout (`layout.html`) needs.
pub(crate) struct Layout {
    /// The product name (configurable title).
    pub(crate) product: String,
    /// The browser window title.
    pub(crate) window_title: String,
    /// The mount path (`""` or `/hench`).
    pub(crate) prefix: String,
    /// The dashboard's home URL.
    pub(crate) home: String,
    pub(crate) nav: Vec<NavLink>,
    /// The CSRF token for forms.
    pub(crate) csrf: String,
    pub(crate) notice: Option<NoticeView>,
    pub(crate) read_only: bool,
    pub(crate) app_url: Option<String>,
    /// The default refresh interval, in milliseconds.
    pub(crate) poll_ms: u128,
    /// The page can refresh itself ("Live").
    pub(crate) live: bool,
    pub(crate) totals: Option<Totals>,
    pub(crate) assets: AssetUrls,
    /// The server's clock, in milliseconds since the epoch (scripts correct relative times
    /// for clock skew with it).
    pub(crate) now_ms: i64,
    pub(crate) footer: Footer,
}

/// `dashboard.html`.
#[derive(Template)]
#[template(path = "dashboard.html")]
pub(crate) struct DashboardPage {
    pub(crate) layout: Layout,
    pub(crate) s: &'static Strings,
    pub(crate) poll_secs: u64,
    pub(crate) stats_url: String,
    pub(crate) ranges: Vec<RangeLink>,
    pub(crate) history_title: String,
    /// `{"dates": [...], "processed": [...], "failed": [...]}`, oldest first.
    pub(crate) history_json: String,
    /// The history table, newest first.
    pub(crate) history: Vec<DayView>,
    pub(crate) redis: RedisView,
}

/// `busy.html`.
#[derive(Template)]
#[template(path = "busy.html")]
pub(crate) struct BusyPage {
    pub(crate) layout: Layout,
    pub(crate) s: &'static Strings,
    pub(crate) busy_url: String,
    pub(crate) status: BusyStatus,
    pub(crate) show_processes: bool,
    pub(crate) show_jobs: bool,
    /// Some process is not embedded, so "Quiet all" and "Stop all" would reach one.
    pub(crate) any_standalone: bool,
    pub(crate) processes: Vec<ProcessRow>,
    pub(crate) works: Vec<JobRow>,
    pub(crate) pager: Option<Pager>,
}

/// `queues.html`.
#[derive(Template)]
#[template(path = "queues.html")]
pub(crate) struct QueuesPage {
    pub(crate) layout: Layout,
    pub(crate) s: &'static Strings,
    pub(crate) queues: Vec<QueueRow>,
}

/// `queue.html`.
#[derive(Template)]
#[template(path = "queue.html")]
pub(crate) struct QueuePage {
    pub(crate) layout: Layout,
    pub(crate) s: &'static Strings,
    pub(crate) heading: String,
    pub(crate) total: String,
    pub(crate) paused: bool,
    pub(crate) queue_url: String,
    pub(crate) delete_url: String,
    pub(crate) ascending: bool,
    pub(crate) newest_href: String,
    pub(crate) oldest_href: String,
    pub(crate) filter_action: String,
    pub(crate) filter_value: String,
    pub(crate) filter_summary: Option<String>,
    /// What the page says when it has no rows.
    pub(crate) empty_text: &'static str,
    pub(crate) rows: Vec<JobRow>,
    pub(crate) pager: Option<Pager>,
}

/// `set.html`: the retries, scheduled jobs or dead jobs.
#[derive(Template)]
#[template(path = "set.html")]
pub(crate) struct SetPage {
    pub(crate) layout: Layout,
    pub(crate) s: &'static Strings,
    pub(crate) heading: &'static str,
    pub(crate) total: String,
    pub(crate) is_retries: bool,
    pub(crate) is_scheduled: bool,
    pub(crate) time_label: &'static str,
    /// What the page says when it has no rows.
    pub(crate) empty_text: &'static str,
    pub(crate) list_url: String,
    pub(crate) filter_action: String,
    pub(crate) filter_value: String,
    pub(crate) filter_summary: Option<String>,
    pub(crate) rows: Vec<JobRow>,
    pub(crate) pager: Option<Pager>,
    /// The confirmations of the actions on every job of the set (which name its size), or
    /// `None` when they are not offered (a filtered list: they would not act on the matches).
    pub(crate) all_actions: Option<AllActions>,
}

/// The confirmations of the "Every job" buttons of a set page.
pub(crate) struct AllActions {
    /// Retry all (retries, dead jobs) or enqueue all (scheduled jobs).
    pub(crate) requeue: String,
    pub(crate) delete: String,
    /// Kill all (retries only).
    pub(crate) kill: String,
}

/// `entry.html`: one job of a sorted set.
#[derive(Template)]
#[template(path = "entry.html")]
pub(crate) struct EntryPage {
    pub(crate) layout: Layout,
    pub(crate) s: &'static Strings,
    pub(crate) heading: String,
    pub(crate) is_retries: bool,
    pub(crate) is_scheduled: bool,
    pub(crate) is_dead: bool,
    pub(crate) time_label: &'static str,
    pub(crate) entry: EntryView,
}

/// `metrics.html`: the busiest job classes of a period.
#[derive(Template)]
#[template(path = "metrics.html")]
pub(crate) struct MetricsPage {
    pub(crate) layout: Layout,
    pub(crate) s: &'static Strings,
    /// The period links (`1h` … `72h`).
    pub(crate) periods: Vec<RangeLink>,
    /// The selected period's label.
    pub(crate) period: &'static str,
    /// The selected order (`sort` parameter), kept by the filter form.
    pub(crate) sort: &'static str,
    /// The table is sorted in its default order (total time).
    pub(crate) default_sort: bool,
    pub(crate) filter_action: String,
    pub(crate) filter_value: String,
    /// The page without the filter.
    pub(crate) clear_href: String,
    /// What the page shows (`3 job classes ran in the last hour.`).
    pub(crate) summary: String,
    pub(crate) columns: Vec<SortColumn>,
    /// The headers of the columns of the features' outcomes (over limit, throttled, skipped),
    /// shown after the failures only when some class has a non-zero value in the period, so
    /// that deployments without these features see the usual table.
    pub(crate) feature_columns: Vec<&'static str>,
    pub(crate) rows: Vec<MetricsRow>,
    /// Set when the table shows only the busiest classes.
    pub(crate) cut_note: Option<String>,
    pub(crate) chart_title: &'static str,
    /// The chart's data (see `metrics.js`).
    pub(crate) chart_json: String,
    /// The table view of the chart: the charted classes per bucket, newest first.
    pub(crate) chart_head: Vec<String>,
    pub(crate) chart_rows: Vec<TableRow>,
    pub(crate) marks: Vec<MarkView>,
    /// `Data from … to ….`
    pub(crate) window: String,
    /// The page refreshes itself (the last hour).
    pub(crate) auto_refresh: bool,
}

/// `metrics_job.html`: one job class in detail.
#[derive(Template)]
#[template(path = "metrics_job.html")]
pub(crate) struct JobMetricsPage {
    pub(crate) layout: Layout,
    pub(crate) s: &'static Strings,
    pub(crate) class: String,
    /// The metrics page, same period.
    pub(crate) back_href: String,
    pub(crate) periods: Vec<RangeLink>,
    /// Why a longer period was cut to 8 hours.
    pub(crate) period_note: Option<&'static str>,
    /// No execution in the period: `empty_text` replaces the figures and charts.
    pub(crate) empty: bool,
    pub(crate) empty_text: String,
    pub(crate) facts: Vec<FactView>,
    pub(crate) series_title: &'static str,
    /// Executions and failures per bucket (see `metrics.js`).
    pub(crate) series_json: String,
    /// Executions per duration bucket.
    pub(crate) histogram_json: String,
    /// The table view of the histogram.
    pub(crate) histogram_rows: Vec<TableRow>,
    /// Executions per minute and duration bucket.
    pub(crate) heatmap_json: String,
    /// The table view of the series and the heat map: minutes with executions, newest first.
    pub(crate) minute_rows: Vec<TableRow>,
    pub(crate) marks: Vec<MarkView>,
    pub(crate) window: String,
    pub(crate) auto_refresh: bool,
}

/// `extension.html`: a page of the host application in the dashboard's layout
/// ([`crate::Page::render`]).
#[derive(Template)]
#[template(path = "extension.html")]
pub(crate) struct ExtensionPage<'a> {
    pub(crate) layout: Layout,
    pub(crate) s: &'static Strings,
    /// The host's HTML, inserted as it is (the host escapes what it did not write).
    pub(crate) content: &'a str,
}

/// `error.html`.
#[derive(Template)]
#[template(path = "error.html")]
pub(crate) struct ErrorPage {
    pub(crate) layout: Layout,
    pub(crate) s: &'static Strings,
    pub(crate) code: u16,
    pub(crate) heading: &'static str,
    pub(crate) message: String,
}

impl Footer {
    pub(crate) fn new(redis_url: String, now: std::time::SystemTime) -> Self {
        Self {
            version: crate::strings::product_version(),
            redis: redis_url,
            time: format::clock(now),
            time_iso: format::iso(now),
            docs: env!("CARGO_PKG_REPOSITORY"),
        }
    }
}
