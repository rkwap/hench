//! The text and JSON reports of `hench stats`, `hench processes` and `hench queues`: an
//! overview of the cluster in the spirit of Sidekiq's `sidekiqmon`.
//!
//! Data is gathered from Redis into plain structs ([`Report`]) and rendered by pure
//! functions, so the formatting is testable without Redis.

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use hench::api::{Api, Process, Stats};
use hench::metrics::{Period, Query, SortBy};
use serde::{Serialize, Serializer};

/// Width of the left padding of the process detail lines (`"   Queues: "`).
const QUEUE_PAD: usize = 11;

/// Lines are wrapped before this many characters.
const LINE_WIDTH: usize = 80;

/// Spaces between table columns.
const COLUMN_PAD: usize = 2;

/// Job classes the metrics section lists at most.
pub const METRICS_ROWS: usize = 10;

/// Everything `hench stats` reports.
#[derive(Debug, Clone, Serialize)]
pub struct Report {
    /// The hench version of this binary.
    pub version: String,
    /// When the report was made (RFC 3339).
    pub time: String,
    /// The totals.
    pub overview: Overview,
    /// The live processes.
    pub processes: Vec<ProcessInfo>,
    /// The known queues, by name.
    pub queues: Vec<QueueInfo>,
    /// The busiest job classes of the last hour by total execution time (at most
    /// [`METRICS_ROWS`]), from the metrics the workers record.
    pub metrics: Vec<JobMetricsInfo>,
}

/// The totals of the overview.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Overview {
    /// Jobs processed.
    pub processed: u64,
    /// Jobs failed.
    pub failed: u64,
    /// Jobs running.
    pub busy: u64,
    /// Jobs waiting in queues.
    pub enqueued: u64,
    /// Jobs waiting to be retried.
    pub retries: u64,
    /// Jobs scheduled.
    pub scheduled: u64,
    /// Dead jobs.
    pub dead: u64,
}

impl From<&Stats> for Overview {
    fn from(stats: &Stats) -> Self {
        Self {
            processed: stats.processed,
            failed: stats.failed,
            busy: stats.workers_size,
            enqueued: stats.enqueued,
            retries: stats.retry_size,
            scheduled: stats.scheduled_size,
            dead: stats.dead_size,
        }
    }
}

/// One live process.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct ProcessInfo {
    /// `hostname:pid:nonce`.
    pub identity: String,
    /// The host name.
    pub hostname: Option<String>,
    /// The operating system's process id.
    pub pid: Option<u64>,
    /// The tag (application name).
    pub tag: Option<String>,
    /// The labels.
    pub labels: Vec<String>,
    /// When it started (RFC 3339), if known.
    pub started_at: Option<String>,
    /// Its last heartbeat (RFC 3339), if known.
    pub beat: Option<String>,
    /// Seconds since it started.
    #[serde(skip)]
    pub started_secs_ago: Option<u64>,
    /// Job slots.
    pub concurrency: u64,
    /// Jobs running.
    pub busy: u64,
    /// Quiet or stopping.
    pub quiet: bool,
    /// Embedded in another application.
    pub embedded: bool,
    /// Memory (RSS) in KB.
    pub rss_kb: u64,
    /// The hench version it runs.
    pub version: Option<String>,
    /// The names of the queues it works on, each once, in its configured order (with one
    /// capsule, the order it takes them in when strict).
    pub queues: Vec<String>,
    /// Its capsules by name, in the order it reports them (a JSON object keyed by name).
    #[serde(serialize_with = "as_object")]
    pub capsules: Vec<(String, CapsuleSettings)>,
    /// The stored-format version of each pro feature the process understands (its heartbeat's
    /// `features`: `unique`, `periodic`, `batches`, `limits`); empty for a process that predates
    /// them, which ignores those features' fields.
    pub features: BTreeMap<String, u64>,
}

/// One capsule of a process, as its heartbeat reports it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct CapsuleSettings {
    /// How many jobs it runs at a time.
    pub concurrency: u64,
    /// How it orders its queues: `strict`, `weighted` or `random`.
    pub mode: String,
    /// Its queues with their weights (`0` when strict), in the order the process reports them
    /// (a JSON object keyed by queue name).
    #[serde(serialize_with = "as_object")]
    pub weights: Vec<(String, u64)>,
}

/// Writes `(name, value)` pairs as a JSON object, in their order (the pairs hold each name
/// once: see [`each_name_once`]).
fn as_object<S: Serializer, V: Serialize>(
    pairs: &[(String, V)],
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.collect_map(pairs.iter().map(|(name, value)| (name, value)))
}

/// `pairs` with each name once: a name listed again keeps its first place and takes its last
/// value, as a JSON object with a repeated key reads. Heartbeats come from Redis and can hold
/// anything; the reports must not repeat a key of a JSON object.
fn each_name_once<V>(pairs: impl IntoIterator<Item = (String, V)>) -> Vec<(String, V)> {
    let mut unique: Vec<(String, V)> = Vec::new();
    let mut places: HashMap<String, usize> = HashMap::new();
    for (name, value) in pairs {
        match places.get(&name) {
            Some(&place) => {
                if let Some(entry) = unique.get_mut(place) {
                    entry.1 = value;
                }
            }
            None => {
                places.insert(name.clone(), unique.len());
                unique.push((name, value));
            }
        }
    }
    unique
}

/// `time` in RFC 3339, or `None` beyond what the calendar represents (process times come
/// from Redis and can hold any value).
pub fn rfc3339(time: SystemTime) -> Option<String> {
    datetime(time).map(|time| time.to_rfc3339())
}

/// `time` as a calendar time, or `None` beyond what the calendar represents (about ±262 000
/// years: times from Redis can hold any value, and `DateTime::from(SystemTime)` panics there).
pub fn datetime(time: SystemTime) -> Option<DateTime<Utc>> {
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
    DateTime::<Utc>::from_timestamp(secs, nanos)
}

impl ProcessInfo {
    /// The report of a process, as of `now`. Times the calendar cannot show are unknown.
    pub fn new(process: &Process, now: SystemTime) -> Self {
        let capsules = each_name_once(process.capsules().iter().map(|capsule| {
            let settings = CapsuleSettings {
                concurrency: capsule.concurrency,
                mode: capsule.mode.clone(),
                weights: each_name_once(capsule.weights.iter().cloned()),
            };
            (capsule.name.clone(), settings)
        }));
        Self {
            identity: process.identity().to_owned(),
            hostname: process.hostname().map(str::to_owned),
            pid: process.pid(),
            tag: process.tag().map(str::to_owned),
            labels: process.labels().into_iter().map(str::to_owned).collect(),
            started_at: process.started_at().and_then(rfc3339),
            beat: process.beat().and_then(rfc3339),
            started_secs_ago: process
                .started_at()
                .map(|started| now.duration_since(started).map_or(0, |ago| ago.as_secs())),
            concurrency: process.concurrency(),
            busy: process.busy(),
            quiet: process.is_stopping(),
            embedded: process.is_embedded(),
            rss_kb: process.rss_kb(),
            version: process.version().map(str::to_owned),
            queues: process.queues(),
            capsules,
            features: process
                .get("features")
                .and_then(serde_json::Value::as_object)
                .map(|features| {
                    features
                        .iter()
                        .filter_map(|(name, version)| Some((name.clone(), version.as_u64()?)))
                        .collect()
                })
                .unwrap_or_default(),
        }
    }
}

/// One queue.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct QueueInfo {
    /// Its name.
    pub name: String,
    /// Jobs waiting.
    pub size: u64,
    /// Seconds the oldest job has been waiting.
    pub latency: f64,
    /// Paused (workers skip it).
    pub paused: bool,
}

/// One job class's executions over the last hour.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct JobMetricsInfo {
    /// The job class.
    pub class: String,
    /// Executions (successful, failed or interrupted).
    pub executions: u64,
    /// Failed executions.
    pub failed: u64,
    /// Total execution time, in seconds.
    pub seconds: f64,
    /// Average milliseconds per execution that did not fail.
    pub average_ms: f64,
}

/// Reads everything `hench stats` reports.
pub async fn gather(api: &Api) -> hench::Result<Report> {
    let now = SystemTime::now();
    let stats = api.stats().await?;
    let processes = api.processes().list().await?;
    let mut queues = Stats::queue_summaries(api).await?;
    queues.sort_by(|a, b| a.name.cmp(&b.name));
    let metrics = Query::new(api.redis())
        .top_jobs(Period::Minutes(60), None)
        .await?;
    let metrics = metrics
        .sorted(SortBy::TotalTime)
        .into_iter()
        .take(METRICS_ROWS)
        .map(|(class, job)| JobMetricsInfo {
            class: class.to_owned(),
            executions: job.totals.processed,
            failed: job.totals.failed,
            seconds: job.totals.seconds(),
            average_ms: job.totals.average_ms(),
        })
        .collect();
    Ok(Report {
        version: env!("CARGO_PKG_VERSION").to_owned(),
        time: rfc3339(now).unwrap_or_default(),
        overview: Overview::from(&stats),
        processes: processes
            .iter()
            .map(|process| ProcessInfo::new(process, now))
            .collect(),
        queues: queues
            .into_iter()
            .map(|queue| QueueInfo {
                name: queue.name,
                size: queue.size,
                latency: queue.latency,
                paused: queue.paused,
            })
            .collect(),
        metrics,
    })
}

/// `text` with its control characters spelled out (`\n`, `\u{1b}`): names come from Redis,
/// and anyone who can push a job could otherwise send escape sequences (colors, window titles,
/// clipboard writes) or line breaks (forged rows) to the operator's terminal.
pub fn printable(text: &str) -> Cow<'_, str> {
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

/// `hench 0.1.0` and the time.
pub fn render_version(version: &str, now: DateTime<Utc>) -> String {
    format!("hench {version}\n{}\n", now.format("%Y-%m-%d %H:%M:%S UTC"))
}

/// The totals, aligned on the colon.
pub fn render_overview(overview: &Overview) -> String {
    let rows = [
        ("Processed", overview.processed),
        ("Failed", overview.failed),
        ("Busy", overview.busy),
        ("Enqueued", overview.enqueued),
        ("Retries", overview.retries),
        ("Scheduled", overview.scheduled),
        ("Dead", overview.dead),
    ];
    let mut out = String::from("---- Overview ----\n");
    for (label, value) in rows {
        out.push_str(&format!("{label:>11}: {}\n", delimit(value)));
    }
    out
}

/// The processes, one block each.
pub fn render_processes(processes: &[ProcessInfo]) -> String {
    let mut out = format!("---- Processes ({}) ----\n", processes.len());
    let ours = env!("CARGO_PKG_VERSION");
    for (index, process) in processes.iter().enumerate() {
        let identity = printable(&process.identity);
        match tags_for(process) {
            Some(tags) => out.push_str(&format!("{identity} {tags}\n")),
            None => out.push_str(&format!("{identity}\n")),
        }
        let started = match (&process.started_at, process.started_secs_ago) {
            (Some(at), Some(ago)) => format!("{at} ({})", time_ago(ago)),
            _ => "unknown".to_owned(),
        };
        out.push_str(&format!("  Started: {started}\n"));
        out.push_str(&format!(
            "  Threads: {} ({} busy)\n",
            process.concurrency, process.busy
        ));
        let queues: Vec<String> = queue_groups(process)
            .iter()
            .map(|queues| printable(queues).into_owned())
            .collect();
        out.push_str(&format!(
            "   Queues: {}\n",
            split_multiline(&queues, QUEUE_PAD, LINE_WIDTH - QUEUE_PAD)
        ));
        if process.version.as_deref() != Some(ours) {
            out.push_str(&format!(
                "  Version: {}\n",
                printable(process.version.as_deref().unwrap_or("unknown"))
            ));
        }
        if index + 1 < processes.len() {
            out.push('\n');
        }
    }
    out
}

/// The queues of a process as the text reports show them, one entry per capsule (`low,
/// default, high`): with one capsule (or none reported) its queue list, in its configured
/// order; with several, each capsule's queues in the order the process reports them (a
/// capsule's weights are a JSON object: by name, unless the process kept their order).
fn queue_groups(process: &ProcessInfo) -> Vec<String> {
    if process.capsules.len() <= 1 {
        if process.queues.is_empty() {
            return Vec::new();
        }
        return vec![process.queues.join(", ")];
    }
    process
        .capsules
        .iter()
        .map(|(_, capsule)| {
            capsule
                .weights
                .iter()
                .map(|(queue, _)| queue.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        })
        .collect()
}

/// The queues as a table: name (left-aligned), size and latency (right-aligned).
pub fn render_queues(queues: &[QueueInfo]) -> String {
    let rows: Vec<Vec<String>> = queues
        .iter()
        .map(|queue| {
            let name = printable(&queue.name);
            let name = if queue.paused {
                format!("{name} (paused)")
            } else {
                name.into_owned()
            };
            vec![
                name,
                queue.size.to_string(),
                format!("{:.2}", queue.latency),
            ]
        })
        .collect();
    let mut out = format!("---- Queues ({}) ----\n", queues.len());
    out.push_str(&table(&["NAME", "SIZE", "LATENCY"], &rows));
    out
}

/// The busiest job classes of the last hour as a table: class (left-aligned), executions,
/// failures, total seconds and average milliseconds (right-aligned).
pub fn render_metrics(metrics: &[JobMetricsInfo]) -> String {
    let mut out = String::from("---- Metrics (last hour) ----\n");
    if metrics.is_empty() {
        out.push_str("No job ran in the last hour.\n");
        return out;
    }
    let rows: Vec<Vec<String>> = metrics
        .iter()
        .map(|job| {
            vec![
                printable(&job.class).into_owned(),
                delimit(job.executions),
                delimit(job.failed),
                format!("{:.2}", job.seconds),
                format!("{:.1}", job.average_ms),
            ]
        })
        .collect();
    out.push_str(&table(
        &["CLASS", "EXECUTIONS", "FAILED", "TIME (S)", "AVG (MS)"],
        &rows,
    ));
    out
}

/// A text table: the first column left-aligned, the others right-aligned, each as wide as its
/// widest cell plus a two-space gutter.
fn table(headers: &[&str], rows: &[Vec<String>]) -> String {
    let widths: Vec<usize> = headers
        .iter()
        .enumerate()
        .map(|(column, header)| {
            rows.iter()
                .filter_map(|row| row.get(column))
                .map(|cell| cell.chars().count())
                .chain([header.chars().count()])
                .max()
                .unwrap_or(0)
                + COLUMN_PAD
        })
        .collect();
    let line = |cells: &mut dyn Iterator<Item = &str>| {
        let mut text = String::new();
        for (column, cell) in cells.enumerate() {
            let width = widths.get(column).copied().unwrap_or(0);
            if column == 0 {
                text.push_str(&format!("{cell:<width$}"));
            } else {
                text.push_str(&format!("{cell:>width$}"));
            }
        }
        text.push('\n');
        text
    };
    let mut out = line(&mut headers.iter().copied());
    for row in rows {
        out.push_str(&line(&mut row.iter().map(String::as_str)));
    }
    out
}

/// The whole report, sections separated by blank lines.
pub fn render_all(report: &Report, now: DateTime<Utc>) -> String {
    [
        render_version(&report.version, now),
        render_overview(&report.overview),
        render_processes(&report.processes),
        render_queues(&report.queues),
        render_metrics(&report.metrics),
    ]
    .join("\n")
}

/// `1234567` → `"1,234,567"`.
pub fn delimit(number: u64) -> String {
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

/// `values` joined with `"; "`, wrapped before `max_length` characters; continuation lines
/// start with `pad` spaces. `"none"` when there are no values.
pub fn split_multiline(values: &[String], pad: usize, max_length: usize) -> String {
    if values.is_empty() {
        return "none".to_owned();
    }
    let mut lines: Vec<String> = Vec::new();
    let mut line = String::new();
    for value in values {
        if !line.trim().is_empty() && line.chars().count() + value.chars().count() > max_length {
            lines.push(line.trim_end_matches("; ").to_owned());
            line = " ".repeat(pad);
        }
        line.push_str(value);
        line.push_str("; ");
    }
    lines.push(line.trim_end_matches("; ").to_owned());
    lines.join("\n")
}

/// `[tag] [label] … [quiet]`, or `None` without any.
pub fn tags_for(process: &ProcessInfo) -> Option<String> {
    let mut tags: Vec<Cow<'_, str>> = Vec::new();
    if let Some(tag) = process.tag.as_deref() {
        tags.push(printable(tag));
    }
    tags.extend(process.labels.iter().map(|label| printable(label)));
    if process.embedded {
        tags.push(Cow::Borrowed("embedded"));
    }
    if process.quiet {
        tags.push(Cow::Borrowed("quiet"));
    }
    (!tags.is_empty()).then(|| format!("[{}]", tags.join("] [")))
}

/// How long ago, in words: `just now`, `a minute ago`, `5 minutes ago`, `an hour ago`,
/// `3 hours ago`.
pub fn time_ago(seconds: u64) -> String {
    match seconds {
        0..60 => "just now".to_owned(),
        60..120 => "a minute ago".to_owned(),
        120..3_600 => format!("{} minutes ago", seconds / 60),
        3_600..7_200 => "an hour ago".to_owned(),
        _ => format!("{} hours ago", seconds / 3_600),
    }
}

/// `in 5 minutes`, `in an hour`, `in 3 days`, from a number of seconds to come.
pub fn time_until(seconds: u64) -> String {
    match seconds {
        0..60 => "in under a minute".to_owned(),
        60..120 => "in a minute".to_owned(),
        120..3_600 => format!("in {} minutes", seconds / 60),
        3_600..7_200 => "in an hour".to_owned(),
        7_200..172_800 => format!("in {} hours", seconds / 3_600),
        _ => format!("in {} days", seconds / 86_400),
    }
}

/// How long ago `at` was, or how long until it comes (seen from `now`): `5 minutes ago`,
/// `in 29 days`.
pub fn relative(at: SystemTime, now: SystemTime) -> String {
    match now.duration_since(at) {
        Ok(ago) => time_ago(ago.as_secs()),
        Err(ahead) => time_until(ahead.duration().as_secs()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    // Scenarios of Sidekiq's sidekiqmon_test.

    #[test]
    fn version_shows_the_version_and_the_time() {
        let at = Utc.timestamp_opt(0, 0).unwrap();
        let text = render_version("1.2.3", at);
        assert!(text.contains("hench 1.2.3"));
        assert!(text.contains("1970-01-01 00:00:00 UTC"));
    }

    #[test]
    fn overview_aligns_and_delimits_the_totals() {
        let text = render_overview(&Overview {
            processed: 420_710,
            failed: 12,
            busy: 34,
            enqueued: 56,
            retries: 78,
            scheduled: 90,
            dead: 666,
        });
        assert!(text.starts_with("---- Overview ----\n"));
        for line in [
            "  Processed: 420,710",
            "     Failed: 12",
            "       Busy: 34",
            "   Enqueued: 56",
            "    Retries: 78",
            "  Scheduled: 90",
            "       Dead: 666",
        ] {
            assert!(text.contains(line), "{line}\n{text}");
        }
    }

    /// `(name, settings)` of a capsule with these queues and weights.
    fn capsule(name: &str, mode: &str, weights: &[(&str, u64)]) -> (String, CapsuleSettings) {
        let settings = CapsuleSettings {
            concurrency: 1,
            mode: mode.to_owned(),
            weights: weights
                .iter()
                .map(|(queue, weight)| ((*queue).to_owned(), *weight))
                .collect(),
        };
        (name.to_owned(), settings)
    }

    fn names(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_owned()).collect()
    }

    #[test]
    fn processes_show_identity_tags_threads_and_queues() {
        assert_eq!(render_processes(&[]), "---- Processes (0) ----\n");
        let process = ProcessInfo {
            identity: "foobar".to_owned(),
            tag: Some("baz".to_owned()),
            started_at: Some("2026-11-01T00:00:00+00:00".to_owned()),
            started_secs_ago: Some(3),
            concurrency: 5,
            busy: 2,
            version: Some(env!("CARGO_PKG_VERSION").to_owned()),
            queues: names(&["low", "default", "high", "single"]),
            capsules: vec![
                capsule(
                    "default",
                    "weighted",
                    &[("low", 1), ("default", 2), ("high", 3)],
                ),
                capsule("single", "strict", &[("single", 0)]),
            ],
            ..ProcessInfo::default()
        };
        let text = render_processes(std::slice::from_ref(&process));
        assert!(text.contains("---- Processes (1) ----"));
        assert!(text.contains("foobar [baz]"));
        assert!(text.contains("Started: 2026-11-01T00:00:00+00:00 (just now)"));
        assert!(text.contains("Threads: 5 (2 busy)"));
        // Each capsule's queues, in the order the process reports them.
        assert!(
            text.contains("Queues: low, default, high; single"),
            "{text}"
        );
        assert!(!text.contains("Version"));
        let other = ProcessInfo {
            version: None,
            ..process.clone()
        };
        assert!(render_processes(&[other]).contains("  Version: unknown"));
        // One capsule: the process's queue list, in its configured order (a capsule's weights
        // are a JSON object, so usually in name order).
        let one = ProcessInfo {
            queues: names(&["low", "default", "high"]),
            capsules: vec![capsule(
                "default",
                "strict",
                &[("default", 0), ("high", 0), ("low", 0)],
            )],
            ..process.clone()
        };
        let text = render_processes(&[one]);
        assert!(text.contains("   Queues: low, default, high\n"), "{text}");
        // No queue reported.
        let none = ProcessInfo {
            queues: Vec::new(),
            capsules: Vec::new(),
            ..process
        };
        let text = render_processes(&[none]);
        assert!(text.contains("   Queues: none\n"), "{text}");
    }

    #[test]
    fn processes_serialize_queue_names_and_capsules_by_name() {
        // fresh-user-4: `queues` were display strings, one per capsule.
        let process = ProcessInfo {
            identity: "host:1:abc".to_owned(),
            queues: names(&["critical", "default", "low"]),
            capsules: vec![
                capsule("default", "strict", &[("critical", 0), ("default", 0)]),
                capsule("bulk", "weighted", &[("low", 3)]),
            ],
            ..ProcessInfo::default()
        };
        let json = serde_json::to_string(&process).unwrap();
        assert!(
            json.contains(
                "\"queues\":[\"critical\",\"default\",\"low\"],\"capsules\":{\"default\":\
                 {\"concurrency\":1,\"mode\":\"strict\",\"weights\":{\"critical\":0,\
                 \"default\":0}},\"bulk\":{\"concurrency\":1,\"mode\":\"weighted\",\
                 \"weights\":{\"low\":3}}}"
            ),
            "{json}"
        );
    }

    #[test]
    fn names_listed_twice_keep_their_first_place_and_last_value() {
        let pairs = [("a", 1), ("b", 2), ("a", 3), ("c", 4), ("b", 5)]
            .map(|(name, value)| (name.to_owned(), value));
        assert_eq!(
            each_name_once(pairs),
            [("a", 3), ("b", 5), ("c", 4)].map(|(name, value)| (name.to_owned(), value))
        );
        assert_eq!(each_name_once(Vec::<(String, u64)>::new()), []);
    }

    #[test]
    fn queues_are_a_table() {
        assert_eq!(
            render_queues(&[]),
            "---- Queues (0) ----\nNAME    SIZE  LATENCY\n"
        );
        let text = render_queues(&[
            QueueInfo {
                name: "foobar".to_owned(),
                size: 12,
                latency: 12.3456,
                paused: false,
            },
            QueueInfo {
                name: "a_long_queue_name".to_owned(),
                size: 234,
                latency: 567.89999,
                paused: false,
            },
        ]);
        assert!(
            text.contains("NAME                 SIZE  LATENCY\n"),
            "{text}"
        );
        assert!(
            text.contains("foobar                 12    12.35\n"),
            "{text}"
        );
        assert!(
            text.contains("a_long_queue_name     234   567.90\n"),
            "{text}"
        );
    }

    #[test]
    fn metrics_are_a_table() {
        assert_eq!(
            render_metrics(&[]),
            "---- Metrics (last hour) ----\nNo job ran in the last hour.\n"
        );
        let text = render_metrics(&[
            JobMetricsInfo {
                class: "ImportCsv".to_owned(),
                executions: 1_234,
                failed: 5,
                seconds: 1_500.256,
                average_ms: 1_221.6,
            },
            JobMetricsInfo {
                class: "A".to_owned(),
                executions: 3,
                failed: 0,
                seconds: 0.004,
                average_ms: 1.333,
            },
        ]);
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines[0], "---- Metrics (last hour) ----");
        assert_eq!(
            lines[1],
            "CLASS        EXECUTIONS  FAILED  TIME (S)  AVG (MS)"
        );
        assert_eq!(
            lines[2],
            "ImportCsv         1,234       5   1500.26    1221.6"
        );
        assert_eq!(
            lines[3],
            "A                     3       0      0.00       1.3"
        );
    }

    #[test]
    fn names_cannot_inject_terminal_escapes_or_rows() {
        // web-security-6: queue names, classes, identities, tags and labels come from Redis.
        let evil = "evil\u{1b}[1A\r  default  0  0.00\u{1b}[K\n\u{1b}]0;pwned\u{7}";
        let controls = |text: &str| -> Vec<char> {
            text.chars()
                .filter(|c| c.is_control() && *c != '\n')
                .collect()
        };
        let queues = [
            QueueInfo {
                name: "default".into(),
                size: 1_234,
                latency: 3600.0,
                paused: false,
            },
            QueueInfo {
                name: evil.into(),
                size: 1,
                latency: 0.0,
                paused: true,
            },
        ];
        let text = render_queues(&queues);
        assert!(controls(&text).is_empty(), "{text}");
        assert_eq!(text.lines().count(), 2 + queues.len(), "{text}");
        assert!(
            text.contains(
                "evil\\u{1b}[1A\\r  default  0  0.00\\u{1b}[K\\n\\u{1b}]0;pwned\\u{7} (paused)"
            ),
            "{text}"
        );

        let process = ProcessInfo {
            identity: format!("host:1:{evil}"),
            tag: Some(evil.to_owned()),
            labels: vec![evil.to_owned()],
            version: Some(evil.to_owned()),
            queues: vec![evil.to_owned()],
            ..ProcessInfo::default()
        };
        let text = render_processes(std::slice::from_ref(&process));
        assert!(controls(&text).is_empty(), "{text}");
        assert_eq!(text.lines().count(), 6, "{text}");
        // The same through the capsules (several capsules: each one's queue names).
        let capsules = ProcessInfo {
            capsules: vec![
                capsule(evil, "strict", &[(evil, 0)]),
                capsule("other", "strict", &[("default", 0), (evil, 0)]),
            ],
            ..process
        };
        let text = render_processes(&[capsules]);
        assert!(controls(&text).is_empty(), "{text}");
        // The queues wrap onto lines indented like them; no other line was added.
        let continued = " ".repeat(QUEUE_PAD);
        let rows = text.lines().filter(|line| !line.starts_with(&continued));
        assert_eq!(rows.count(), 6, "{text}");
        assert!(
            text.contains(&format!("\n{continued}default, evil\\u{{1b}}")),
            "{text}"
        );

        let text = render_metrics(&[JobMetricsInfo {
            class: evil.to_owned(),
            executions: 1,
            failed: 0,
            seconds: 0.5,
            average_ms: 500.0,
        }]);
        assert!(controls(&text).is_empty(), "{text}");
        assert_eq!(text.lines().count(), 3, "{text}");
        assert_eq!(printable("plain: é ü"), "plain: é ü");
    }

    #[test]
    fn times_beyond_the_calendar_have_no_rfc3339_form() {
        use std::time::Duration;
        assert_eq!(
            rfc3339(UNIX_EPOCH + Duration::from_secs(1_700_000_000)).as_deref(),
            Some("2023-11-14T22:13:20+00:00")
        );
        assert_eq!(
            rfc3339(UNIX_EPOCH - Duration::from_millis(1_500)).as_deref(),
            Some("1969-12-31T23:59:58.500+00:00")
        );
        assert_eq!(
            rfc3339(UNIX_EPOCH + Duration::from_secs(10_000_000_000_000)),
            None
        );
        assert_eq!(
            rfc3339(UNIX_EPOCH - Duration::from_secs(9_000_000_000_000)),
            None
        );
        assert_eq!(
            rfc3339(UNIX_EPOCH + Duration::from_secs(u64::MAX / 2)),
            None
        );
    }

    #[test]
    fn helpers() {
        assert_eq!(delimit(0), "0");
        assert_eq!(delimit(999), "999");
        assert_eq!(delimit(1_000), "1,000");
        assert_eq!(delimit(1_234_567), "1,234,567");

        assert_eq!(split_multiline(&[], 0, 80), "none");
        let values = |items: &[&str]| {
            items
                .iter()
                .map(|item| (*item).to_owned())
                .collect::<Vec<_>>()
        };
        assert_eq!(split_multiline(&values(&["a", "b", "c"]), 0, 80), "a; b; c");
        let wrapped = split_multiline(&values(&["aaaa", "bbbb", "cccc"]), 2, 10);
        let lines: Vec<&str> = wrapped.lines().collect();
        assert!(lines.len() >= 2, "{wrapped}");
        assert!(
            lines[1..].iter().all(|line| line.starts_with("  ")),
            "{wrapped}"
        );

        let mut process = ProcessInfo::default();
        assert_eq!(tags_for(&process), None);
        process.tag = Some("web".to_owned());
        process.labels = vec!["fast".to_owned(), "infra".to_owned()];
        process.quiet = true;
        assert_eq!(
            tags_for(&process).as_deref(),
            Some("[web] [fast] [infra] [quiet]")
        );
        process.labels.clear();
        process.quiet = false;
        assert_eq!(tags_for(&process).as_deref(), Some("[web]"));

        assert_eq!(time_ago(30), "just now");
        assert_eq!(time_ago(90), "a minute ago");
        assert_eq!(time_ago(600), "10 minutes ago");
        assert_eq!(time_ago(3_700), "an hour ago");
        assert_eq!(time_ago(3 * 3_600), "3 hours ago");
        let now = UNIX_EPOCH + std::time::Duration::from_secs(1_800_000_000);
        let day = std::time::Duration::from_secs(86_400);
        assert_eq!(relative(now - day, now), "24 hours ago");
        assert_eq!(
            relative(now + 30 * day - std::time::Duration::from_secs(5), now),
            "in 29 days"
        );
        assert_eq!(relative(now, now), "just now");
        assert!(datetime(UNIX_EPOCH + std::time::Duration::from_secs(u64::MAX / 2)).is_none());
    }
}
