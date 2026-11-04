//! View models: the API's data turned into ready-to-print strings for the templates (which
//! escape them). Nothing here talks to Redis.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::time::SystemTime;

use hench::api::{JobRecord, Page, Process, QueueSummary, SortedEntry, Stats, Work};
use hench::iterable::IterationState;
use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use serde_json::Value;

use crate::ctx::Ctx;
use crate::extension::JobInfoRow;
use crate::format::{self, TimeView};
use crate::params;
use crate::strings;

/// Characters escaped in a path segment (queue names and entry keys in URLs).
const SEGMENT: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'/')
    .add(b'<')
    .add(b'>')
    .add(b'?')
    .add(b'[')
    .add(b'\\')
    .add(b']')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}');

/// `text` escaped for use as one URL path segment.
pub(crate) fn segment(text: &str) -> String {
    utf8_percent_encode(text, SEGMENT).to_string()
}

/// Characters of arguments shown in list rows before they are cut.
const SHORT_ARGS: usize = 120;

/// Characters of arguments a list row can expand to (the detail page shows everything).
const FULL_ARGS: usize = 10_000;

/// Characters of an error shown in a list row.
const SHORT_ERROR: usize = 200;

/// Characters of an iteration cursor shown in lists.
pub(crate) const SHORT_CURSOR: usize = 100;

/// Characters of an iteration cursor shown on a job's page.
pub(crate) const FULL_CURSOR: usize = 10_000;

/// The progress of an iterable job: its cursor, executions and run time (Sidekiq shows it on
/// the busy page, the retries and the job pages).
pub(crate) struct IterationView {
    pub(crate) text: String,
    pub(crate) cancelled: bool,
}

impl IterationView {
    /// `state`, its cursor (compact JSON) cut to `max_cursor` characters.
    pub(crate) fn new(state: &IterationState, max_cursor: usize) -> Self {
        let cursor = state
            .cursor
            .as_ref()
            .map(|cursor| format::truncate(&cursor.to_string(), max_cursor).0);
        Self {
            text: strings::iteration(
                cursor.as_deref(),
                state.executions,
                state.runtime.as_secs_f64(),
            ),
            cancelled: state.cancelled.is_some(),
        }
    }
}

/// The totals strip shown on every page.
pub(crate) struct Totals {
    pub(crate) processed: String,
    pub(crate) failed: String,
    pub(crate) busy: String,
    pub(crate) enqueued: String,
    pub(crate) retries: String,
    pub(crate) scheduled: String,
    pub(crate) dead: String,
}

impl Totals {
    pub(crate) fn new(stats: &Stats) -> Self {
        Self {
            processed: format::delimit(stats.processed),
            failed: format::delimit(stats.failed),
            busy: format::delimit(stats.workers_size),
            enqueued: format::delimit(stats.enqueued),
            retries: format::delimit(stats.retry_size),
            scheduled: format::delimit(stats.scheduled_size),
            dead: format::delimit(stats.dead_size),
        }
    }
}

/// A job tag, linking to the filtered list when shown in a sorted set.
pub(crate) struct TagView {
    pub(crate) name: String,
    pub(crate) href: Option<String>,
}

fn tags(record: &JobRecord, filter_base: Option<&str>) -> Vec<TagView> {
    record
        .tags()
        .into_iter()
        .map(|tag| TagView {
            name: tag.to_owned(),
            href: filter_base
                .map(|base| format!("{base}{}", params::query_string(&[("substr", tag)]))),
        })
        .collect()
}

/// The class shown for a job (`display_class`, else `class`).
fn class_of(record: &JobRecord) -> String {
    record
        .display_class()
        .map_or_else(|| strings::EN.unknown.to_owned(), str::to_owned)
}

/// One job in a table: queues, sorted sets and the running jobs share it.
pub(crate) struct JobRow {
    /// In sorted sets, the entry's key (`score-jid`) as the value of the row's checkbox
    /// ([`crate::form::field`]: jids may hold any character); in queues, the key of the delete
    /// form ([`queue_row_key`]).
    pub(crate) key: String,
    /// The detail page (sorted sets), else empty.
    pub(crate) href: String,
    pub(crate) jid: String,
    pub(crate) queue: String,
    pub(crate) queue_href: String,
    pub(crate) class: String,
    pub(crate) tags: Vec<TagView>,
    /// The chips of the features the job uses (`callback`, `unique`, `throttle: stripe:42`,
    /// `over limit: w:stripe:42 ×3`), shown next to the class.
    pub(crate) chips: Vec<String>,
    /// The arguments, cut to a list-friendly length.
    pub(crate) args: String,
    /// The arguments up to a few thousand characters, when `args` was cut.
    pub(crate) args_full: Option<String>,
    /// `error_class: error_message`, cut.
    pub(crate) error: Option<String>,
    pub(crate) retry_count: Option<u64>,
    /// The time column: the score (sorted sets), when it was enqueued (queues) or started
    /// (running jobs).
    pub(crate) at: Option<TimeView>,
    /// The 1-based position in the queue.
    pub(crate) position: u64,
    /// The process running the job (busy page).
    pub(crate) process: String,
    /// The worker slot running the job (busy page).
    pub(crate) slot: String,
    /// The progress of an iterable job (busy page, retries).
    pub(crate) iteration: Option<IterationView>,
}

impl JobRow {
    fn from_record(ctx: &Ctx, record: &JobRecord, filter_base: Option<&str>) -> Self {
        let (args, args_full) = format::args_summary(&record.display_args(), SHORT_ARGS, FULL_ARGS);
        let error = match (record.error_class(), record.error_message()) {
            (None, None) => None,
            (class, message) => {
                let text = format!(
                    "{}: {}",
                    class.unwrap_or_default(),
                    message.unwrap_or_default()
                );
                Some(format::truncate(&text, SHORT_ERROR).0)
            }
        };
        let queue = record.queue();
        Self {
            key: String::new(),
            href: String::new(),
            jid: record.jid().unwrap_or_default().to_owned(),
            queue_href: ctx.url(&format!("/queues/{}", segment(queue))),
            queue: format::visible(queue).into_owned(),
            class: class_of(record),
            tags: tags(record, filter_base),
            chips: crate::pages::feature_chips(record),
            args,
            args_full,
            error,
            retry_count: record.retry_count(),
            at: None,
            position: 0,
            process: String::new(),
            slot: String::new(),
            iteration: None,
        }
    }

    /// A job of a queue of `total` jobs, at `position` in line (1: the next to run).
    pub(crate) fn queued(ctx: &Ctx, record: &JobRecord, position: u64, total: u64) -> Self {
        let mut row = Self::from_record(ctx, record, None);
        row.at = record.enqueued_at().map(|at| TimeView::new(at, ctx.now));
        row.key = queue_row_key(total.saturating_sub(position), record.value());
        row.position = position;
        row
    }

    /// An entry of the sorted set listed at `list_path` (`/retries`).
    pub(crate) fn entry(ctx: &Ctx, entry: &SortedEntry, list_path: &str) -> Self {
        let list = ctx.url(list_path);
        let mut row = Self::from_record(ctx, entry.record(), Some(&list));
        let key = entry.key();
        row.href = format!("{list}/{}", segment(&key));
        row.key = crate::form::field(&key);
        row.at = Some(TimeView::new(entry.at(), ctx.now));
        row
    }

    /// A running job.
    pub(crate) fn running(ctx: &Ctx, work: &Work) -> Self {
        let record = work.job();
        let mut row = Self::from_record(ctx, &record, None);
        row.at = Some(TimeView::new(work.run_at(), ctx.now));
        row.process = work.process_id().to_owned();
        row.slot = work.slot().to_owned();
        row
    }
}

/// The key of a queue row's delete form: the job's index in the queue when the page was
/// rendered (0: the newest job) and the SHA-1 of its JSON, `"{index}-{sha1}"`. The form
/// stays small whatever the job's size, and the server finds the exact JSON to remove with
/// it (browsers would turn line breaks in a raw JSON field into CRLF).
pub(crate) fn queue_row_key(index: u64, value: &str) -> String {
    format!("{index}-{}", job_digest(value))
}

/// The SHA-1 of a job's JSON, in hex.
pub(crate) fn job_digest(value: &str) -> String {
    sha1_smol::Sha1::from(value).digest().to_string()
}

/// The index and digest of a [`queue_row_key`].
pub(crate) fn parse_queue_row_key(key: &str) -> Option<(u64, &str)> {
    let (index, digest) = key.split_once('-')?;
    let valid = digest.len() == 40
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
    if !valid {
        return None;
    }
    Some((index.parse().ok()?, digest))
}

/// A row of the queues table.
pub(crate) struct QueueRow {
    pub(crate) name: String,
    pub(crate) href: String,
    pub(crate) size: String,
    pub(crate) latency: String,
    /// When the oldest job was enqueued, once it waited a minute or more.
    pub(crate) since: Option<TimeView>,
    pub(crate) paused: bool,
    /// Its limits as stored, control characters spelled out.
    pub(crate) limits: Option<String>,
}

impl QueueRow {
    pub(crate) fn new(ctx: &Ctx, summary: &QueueSummary) -> Self {
        let since = (summary.latency >= 60.0).then(|| {
            let waited =
                std::time::Duration::try_from_secs_f64(summary.latency).unwrap_or_default();
            TimeView::new(ctx.now.checked_sub(waited).unwrap_or(ctx.now), ctx.now)
        });
        Self {
            href: ctx.url(&format!("/queues/{}", segment(&summary.name))),
            name: format::visible(&summary.name).into_owned(),
            size: format::delimit(summary.size),
            latency: format::decimal(summary.latency, 2),
            since,
            paused: summary.paused,
            limits: summary
                .limits
                .as_deref()
                .map(|limits| format::visible(limits).into_owned()),
        }
    }
}

/// A row of the processes table.
pub(crate) struct ProcessRow {
    /// The identity, control characters spelled out.
    pub(crate) identity: String,
    /// The identity as the Quiet and Stop form's field ([`crate::form::field`]: identities
    /// come from Redis).
    pub(crate) identity_field: String,
    pub(crate) name: String,
    pub(crate) tag: Option<String>,
    pub(crate) labels: Vec<String>,
    pub(crate) embedded: bool,
    pub(crate) quiet: bool,
    /// The hench version the process runs, shown when it differs from the dashboard's.
    pub(crate) version: Option<String>,
    pub(crate) started: Option<TimeView>,
    pub(crate) beat: Option<TimeView>,
    pub(crate) rss: String,
    pub(crate) concurrency: String,
    pub(crate) busy: String,
    /// Each capsule's queues (`a, b; c`).
    pub(crate) queues: String,
    /// The features in use that the process does not understand (`batches, limits`): it runs
    /// an older version (see the rollout rule).
    pub(crate) lacks: Option<String>,
}

impl ProcessRow {
    pub(crate) fn new(process: &Process, now: SystemTime) -> Self {
        let name = match (process.hostname(), process.pid()) {
            (Some(host), Some(pid)) => format!("{host}:{pid}"),
            (Some(host), None) => host.to_owned(),
            _ => process.identity().to_owned(),
        };
        // One capsule: the process's own (ordered) queue list. Several: each capsule's queues
        // (capsule weights are reported as JSON objects, so in name order).
        let queues = if process.capsules().len() <= 1 {
            process.queues().join(", ")
        } else {
            process
                .capsules()
                .iter()
                .map(|capsule| {
                    capsule
                        .weights
                        .iter()
                        .map(|(queue, _)| queue.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .collect::<Vec<_>>()
                .join("; ")
        };
        let ours = env!("CARGO_PKG_VERSION");
        let version = match process.version() {
            Some(version) if version == ours => None,
            Some(version) => Some(version.to_owned()),
            None => Some(strings::EN.unknown.to_owned()),
        };
        Self {
            identity: format::visible(process.identity()).into_owned(),
            identity_field: crate::form::field(process.identity()),
            name,
            tag: process.tag().map(str::to_owned),
            labels: process.labels().into_iter().map(str::to_owned).collect(),
            embedded: process.is_embedded(),
            quiet: process.is_stopping(),
            version,
            started: process.started_at().map(|at| TimeView::new(at, now)),
            beat: process.beat().map(|at| TimeView::new(at, now)),
            rss: format::memory(process.rss_kb()),
            concurrency: format::delimit(process.concurrency()),
            busy: format::delimit(process.busy()),
            queues,
            lacks: None,
        }
    }

    /// Notes the features in use (`in_use`, by their names in the heartbeat's `features` map)
    /// that the process does not list: it runs a version that ignores them.
    pub(crate) fn with_lacks(mut self, process: &Process, in_use: &[&str]) -> Self {
        let listed = process.get("features").and_then(Value::as_object);
        let lacking: Vec<&str> = in_use
            .iter()
            .copied()
            .filter(|feature| !listed.is_some_and(|features| features.contains_key(*feature)))
            .collect();
        self.lacks = (!lacking.is_empty()).then(|| lacking.join(", "));
        self
    }
}

/// The error recorded on a job.
pub(crate) struct ErrorView {
    pub(crate) class: String,
    pub(crate) message: String,
    pub(crate) backtrace: Vec<String>,
}

/// The rows a feature adds to a job's detail page, and the payload fields they show (left out
/// of the page's extra fields).
#[derive(Default)]
pub(crate) struct FeatureRows {
    /// The rows, in order.
    pub(crate) rows: Vec<JobInfoRow>,
    /// The payload fields the rows show.
    pub(crate) shown: Vec<&'static str>,
}

impl FeatureRows {
    /// Appends `other`'s rows and fields.
    pub(crate) fn extend(&mut self, other: FeatureRows) {
        self.rows.extend(other.rows);
        self.shown.extend(other.shown);
    }
}

/// Everything the detail page of a sorted-set entry shows.
pub(crate) struct EntryView {
    pub(crate) list_href: String,
    pub(crate) action_href: String,
    pub(crate) queue: String,
    pub(crate) queue_href: String,
    pub(crate) class: String,
    pub(crate) tags: Vec<TagView>,
    /// Each argument, pretty-printed and complete.
    pub(crate) args: Vec<String>,
    pub(crate) jid: String,
    /// What deleting or killing a batch member does to its batch (it counts the member dead),
    /// added to the confirmations.
    pub(crate) removal_note: Option<String>,
    pub(crate) created: Option<TimeView>,
    pub(crate) enqueued: Option<TimeView>,
    pub(crate) retry_count: Option<u64>,
    pub(crate) last_retry: Option<TimeView>,
    pub(crate) first_failed: Option<TimeView>,
    /// The score.
    pub(crate) at: TimeView,
    /// Fields hench does not show elsewhere, as compact JSON.
    pub(crate) extras: Vec<(String, String)>,
    pub(crate) error: Option<ErrorView>,
    /// The job as stored, pretty-printed when it is valid JSON.
    pub(crate) raw: String,
    /// Why the job is not valid JSON.
    pub(crate) invalid: Option<String>,
    /// The progress of an iterable job.
    pub(crate) iteration: Option<IterationView>,
    /// The rows of the features (batch, periodic, unique, throttle, limited).
    pub(crate) features: Vec<JobInfoRow>,
    /// The host application's rows ([`crate::HenchWeb::job_info`]).
    pub(crate) custom: Vec<JobInfoRow>,
}

/// Fields shown in their own rows (everything else is an extra).
const SHOWN_FIELDS: [&str; 18] = [
    "queue",
    "class",
    "args",
    "retry_count",
    "retried_at",
    "failed_at",
    "jid",
    "bid",
    "error_message",
    "error_class",
    "backtrace",
    "error_backtrace",
    "enqueued_at",
    "retry",
    "created_at",
    "tags",
    "display_class",
    "wrapped",
];

impl EntryView {
    pub(crate) fn new(ctx: &Ctx, entry: &SortedEntry, list_path: &str) -> Self {
        let list = ctx.url(list_path);
        let record = entry.record();
        let queue = record.queue();
        let args = record
            .display_args()
            .iter()
            .map(|arg| serde_json::to_string_pretty(arg).unwrap_or_else(|_| format::arg(arg)))
            .collect();
        let features = crate::pages::feature_rows(ctx, record);
        let extras: BTreeMap<&String, &Value> = record
            .item()
            .iter()
            .filter(|(key, _)| {
                !SHOWN_FIELDS.contains(&key.as_str()) && !features.shown.contains(&key.as_str())
            })
            .collect();
        let error =
            (record.error_class().is_some() || record.error_message().is_some()).then(|| {
                let backtrace = match record.error_backtrace() {
                    Ok(lines) => lines.unwrap_or_default(),
                    Err(error) => vec![format!("(the backtrace cannot be decoded: {error})")],
                };
                ErrorView {
                    class: record.error_class().unwrap_or_default().to_owned(),
                    message: record.error_message().unwrap_or_default().to_owned(),
                    backtrace,
                }
            });
        let raw = if record.is_valid() {
            // The raw view hides what the argument list hides (the last argument of
            // `"encrypt": true` jobs).
            let mut item = record.item().clone();
            if let Cow::Owned(shown) = record.display_args() {
                item.insert("args".to_owned(), Value::Array(shown));
            }
            serde_json::to_string_pretty(&item).unwrap_or_else(|_| record.value().to_owned())
        } else {
            record.value().to_owned()
        };
        let time = |at: Option<SystemTime>| at.map(|at| TimeView::new(at, ctx.now));
        let retried = record.retry_count().is_some_and(|count| count > 0);
        Self {
            action_href: format!("{list}/{}", segment(&entry.key())),
            list_href: list.clone(),
            queue_href: ctx.url(&format!("/queues/{}", segment(queue))),
            queue: format::visible(queue).into_owned(),
            class: class_of(record),
            tags: tags(record, Some(&list)),
            args,
            jid: record.jid().unwrap_or_default().to_owned(),
            removal_note: record.bid().map(crate::strings::batches::removal_note),
            created: time(record.created_at()),
            enqueued: time(record.enqueued_at()),
            retry_count: record.retry_count(),
            last_retry: if retried {
                time(record.retried_at())
            } else {
                None
            },
            first_failed: time(record.failed_at()),
            at: TimeView::new(entry.at(), ctx.now),
            extras: extras
                .into_iter()
                .map(|(key, value)| (key.clone(), format::arg(value)))
                .collect(),
            error,
            raw,
            invalid: record.parse_error().map(str::to_owned),
            iteration: None,
            features: features.rows,
            custom: custom_rows(ctx, record),
        }
    }
}

/// The host application's rows for `record` ([`crate::HenchWeb::job_info`]). A callback that
/// panics loses its rows, not the page.
fn custom_rows(ctx: &Ctx, record: &JobRecord) -> Vec<JobInfoRow> {
    let mut rows = Vec::new();
    for job_info in &ctx.web.job_info {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| job_info(record))) {
            Ok(more) => rows.extend(more),
            Err(_) => tracing::error!(
                jid = record.jid(),
                "a job info callback (HenchWeb::job_info) panicked; its rows are left out"
            ),
        }
    }
    rows
}

/// One day of the dashboard's history table.
pub(crate) struct DayView {
    pub(crate) date: String,
    pub(crate) processed: String,
    pub(crate) failed: String,
}

/// A link of the history range selector.
pub(crate) struct RangeLink {
    pub(crate) label: &'static str,
    pub(crate) href: String,
    pub(crate) active: bool,
}

/// The Redis panel of the dashboard.
pub(crate) struct RedisView {
    pub(crate) available: bool,
    /// `Redis`, `Valkey` or `Dragonfly`.
    pub(crate) store: String,
    pub(crate) version: String,
    pub(crate) uptime_days: String,
    pub(crate) connections: String,
    pub(crate) memory: String,
    pub(crate) peak_memory: String,
}

/// The `INFO` fields the dashboard and `/stats` show.
pub(crate) const REDIS_FIELDS: [&str; 5] = [
    "redis_version",
    "uptime_in_days",
    "connected_clients",
    "used_memory_human",
    "used_memory_peak_human",
];

/// The server's product name and version from its `INFO` fields (Redis-compatible servers
/// announce themselves with their own version field).
pub(crate) fn store(info: &BTreeMap<String, String>) -> (&'static str, String) {
    if let Some(version) = info.get("dragonfly_version") {
        ("Dragonfly", version.clone())
    } else if let Some(version) = info.get("valkey_version") {
        ("Valkey", version.clone())
    } else {
        (
            "Redis",
            info.get("redis_version").cloned().unwrap_or_default(),
        )
    }
}

impl RedisView {
    pub(crate) fn new(info: Option<&BTreeMap<String, String>>) -> Self {
        let Some(info) = info else {
            return Self {
                available: false,
                store: "Redis".to_owned(),
                version: String::new(),
                uptime_days: String::new(),
                connections: String::new(),
                memory: String::new(),
                peak_memory: String::new(),
            };
        };
        let field = |name: &str| info.get(name).cloned().unwrap_or_else(|| "–".to_owned());
        let (store, version) = store(info);
        Self {
            available: true,
            store: store.to_owned(),
            version,
            uptime_days: field("uptime_in_days"),
            connections: field("connected_clients"),
            memory: field("used_memory_human"),
            peak_memory: field("used_memory_peak_human"),
        }
    }
}

/// A row of the metrics table: one job class over the period.
pub(crate) struct MetricsRow {
    pub(crate) class: String,
    /// The class's own metrics page.
    pub(crate) href: String,
    /// The accessible name of the checkbox that shows the class in the chart.
    pub(crate) toggle_label: String,
    pub(crate) executions: String,
    pub(crate) failed: String,
    /// The figures of the feature columns the page shows (see
    /// `MetricsPage::feature_columns`), in their order.
    pub(crate) features: Vec<String>,
    /// Total seconds.
    pub(crate) total: String,
    /// Average milliseconds per execution that did not fail.
    pub(crate) average: String,
    /// Charted when the page opens.
    pub(crate) charted: bool,
}

/// A sortable column of the metrics table.
pub(crate) struct SortColumn {
    pub(crate) label: &'static str,
    pub(crate) href: String,
    /// The table is sorted by this column.
    pub(crate) active: bool,
}

/// A deploy mark.
pub(crate) struct MarkView {
    /// RFC 3339, for `<time datetime>`.
    pub(crate) iso: String,
    /// The minute of the deploy, UTC.
    pub(crate) utc: String,
    pub(crate) label: String,
}

/// A labelled figure (the per-job metrics summary).
pub(crate) struct FactView {
    pub(crate) label: &'static str,
    pub(crate) value: String,
}

/// A row of the table that holds a chart's numbers: a label and its values.
pub(crate) struct TableRow {
    pub(crate) label: String,
    pub(crate) values: Vec<String>,
}

/// Status figures of the busy page.
pub(crate) struct BusyStatus {
    pub(crate) processes: String,
    pub(crate) threads: String,
    pub(crate) busy: String,
    pub(crate) utilization: u64,
    pub(crate) rss: String,
}

/// Pagination links of a list.
pub(crate) struct Pager {
    pub(crate) label: String,
    pub(crate) first: Option<String>,
    pub(crate) previous: Option<String>,
    pub(crate) next: Option<String>,
    pub(crate) last: Option<String>,
}

impl Pager {
    /// The pager of page `page` (of `size` rows, out of `total`) of the list at `base`
    /// (a full URL path); `None` when everything fits on one page. Links keep the list's
    /// other parameters.
    pub(crate) fn new(ctx: &Ctx, base: &str, page: usize, size: usize, total: u64) -> Option<Self> {
        let size = u64::try_from(size.max(1)).unwrap_or(1);
        let pages = total.div_ceil(size).max(1);
        if pages <= 1 {
            return None;
        }
        let page = u64::try_from(page).unwrap_or(1).clamp(1, pages);
        let link = |number: u64| {
            let number = number.to_string();
            format!("{base}{}", params::carry(&ctx.query, &[("page", &number)]))
        };
        Some(Self {
            label: strings::page_of(page, pages),
            first: (page > 1).then(|| link(1)),
            previous: (page > 1).then(|| link(page - 1)),
            next: (page < pages).then(|| link(page + 1)),
            last: (page < pages).then(|| link(pages)),
        })
    }
}

/// The number of the last page when `page` lies past the end of a list that is not empty
/// (its last rows were just removed): lists show that page instead of an empty one.
pub(crate) fn last_page<T>(page: &Page<T>) -> Option<usize> {
    if !page.items.is_empty() || page.total == 0 {
        return None;
    }
    let last = usize::try_from(page.total_pages()).unwrap_or(usize::MAX);
    (page.number > last).then_some(last)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segments_are_escaped() {
        assert_eq!(segment("default"), "default");
        assert_eq!(segment("foo_bar:123-wow."), "foo_bar:123-wow.");
        assert_eq!(segment("a/b c"), "a%2Fb%20c");
        assert_eq!(segment("<x>\"%"), "%3Cx%3E%22%25");
        assert_eq!(segment("é"), "%C3%A9");
        assert_eq!(segment("1700000000.5-abc"), "1700000000.5-abc");
    }

    #[test]
    fn stores_are_recognized() {
        let info = |pairs: &[(&str, &str)]| -> BTreeMap<String, String> {
            pairs
                .iter()
                .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                .collect()
        };
        assert_eq!(
            store(&info(&[("redis_version", "7.2.4")])),
            ("Redis", "7.2.4".to_owned())
        );
        assert_eq!(
            store(&info(&[
                ("redis_version", "7.2.4"),
                ("valkey_version", "8.0.1")
            ])),
            ("Valkey", "8.0.1".to_owned())
        );
        assert_eq!(
            store(&info(&[
                ("redis_version", "6.2"),
                ("dragonfly_version", "df-1.2")
            ])),
            ("Dragonfly", "df-1.2".to_owned())
        );
        let view = RedisView::new(Some(&info(&[("redis_version", "7.0.0")])));
        assert!(view.available);
        assert_eq!(view.memory, "–");
        assert!(!RedisView::new(None).available);
    }

    #[test]
    fn row_keys_name_a_place_and_a_digest() {
        let key = queue_row_key(7, "{\"class\":\"X\"}");
        let (index, digest) = parse_queue_row_key(&key).unwrap();
        assert_eq!(index, 7);
        assert_eq!(digest.len(), 40);
        assert_eq!(digest, job_digest("{\"class\":\"X\"}"));
        assert_ne!(digest, job_digest("{\"class\":\"X\"} "));
        assert_eq!(
            job_digest("abc"),
            "a9993e364706816aba3e25717850c26c9cd0d89d"
        );
        for bad in [
            "",
            "7",
            "-abc",
            "x-a9993e364706816aba3e25717850c26c9cd0d89d",
            "7-a9993e",
        ] {
            assert_eq!(parse_queue_row_key(bad), None, "{bad}");
        }
        assert_eq!(
            parse_queue_row_key("7-A9993E364706816ABA3E25717850C26C9CD0D89D"),
            None
        );
    }

    #[test]
    fn lists_past_their_end_show_the_last_page() {
        let page = |number, total| Page::<u8>::new(Vec::new(), number, 5, total);
        assert_eq!(last_page(&page(2, 5)), Some(1));
        assert_eq!(last_page(&page(9, 11)), Some(3));
        assert_eq!(last_page(&page(1, 0)), None);
        let full = Page::new(vec![1_u8], 2, 5, 6);
        assert_eq!(last_page(&full), None);
    }
}
