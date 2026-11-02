//! `hench periodic`: the periodic schedules (`list`, the default; `show`; `enqueue`; `pause`;
//! `unpause`; `remove`; `add`; `repair`; `next`), and `hench stats periodic`.
//!
//! ```text
//! hench periodic [list] [--json]
//! hench periodic show <name> [--json]
//! hench periodic enqueue <name> [--json]
//! hench periodic pause <name>        hench periodic unpause <name>
//! hench periodic remove <name> [--force]
//! hench periodic add <name> (--cron EXPR [--tz ZONE] | --every DURATION [--offset DURATION])
//!     --class CLASS [--args JSON] [--queue Q] [--retry true|false|N]
//!     [--catch-up latest|skip|all:N] [--deadline DURATION] [--paused] [--description TEXT]
//!     [--unique-for DURATION [--unique-until success|start|expired] [--unique-key JSON]]
//!     [--throttle NAME[:KEY]]
//! hench periodic repair
//! hench periodic next <EXPR> [--tz ZONE] [--count 5] [--after RFC3339]
//! ```
//!
//! `next` previews a cron expression offline: [`offline`] answers it without Redis (the binary
//! calls it before connecting; [`run`] answers it too). The JSON shapes are shared with the JSON
//! API (`json_api::periodic`).

use std::io::Write;
use std::time::{Duration, SystemTime};

use anyhow::{Context, bail};
use chrono::{DateTime, Utc};
use clap::{Args, Subcommand};
use hench::api::Api;
use hench::periodic::{
    CatchUp, FireResult, HistoryEntry, Periodic, PeriodicEntry, PeriodicSource, RunStatus,
    Timetable, Upserted,
};
use hench::{Config, RetryOption, UniqueUntil};
use serde_json::{Value, json};

use crate::cli::{
    JobArgs, parse_args, parse_duration, parse_json, parse_retry, parse_throttle, parse_time,
};
use crate::commands::locks::{table, when};
use crate::monitor::{delimit, printable, relative, rfc3339};

/// Schedules `hench stats periodic` reads at most to count the parked ones.
pub(crate) const SCAN_LIMIT: u64 = 50_000;

/// History entries `show` prints at most.
pub(crate) const HISTORY: usize = 100;

/// Next ticks `show` prints.
pub(crate) const UPCOMING: usize = 5;

/// Schedules read per round trip by the listings.
const PAGE: usize = 1_000;

/// Ticks `next` previews at most.
const MAX_PREVIEW: u32 = 1_000;

/// How long past its time an active schedule counts as overdue: the default lease plus the
/// default poll interval (like the dashboard).
pub(crate) const LATE_AFTER: Duration = Duration::from_secs(
    Config::DEFAULT_PERIODIC_LEASE.as_secs() + Config::DEFAULT_PERIODIC_POLL_INTERVAL.as_secs(),
);

/// `hench periodic`.
#[derive(Debug, Clone, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct PeriodicArgs {
    /// What to do (default: list the schedules).
    #[command(subcommand)]
    pub command: Option<PeriodicCommand>,

    /// The options of `list`, when no subcommand is given.
    #[command(flatten)]
    pub list: ListArgs,
}

/// The subcommands of `hench periodic`.
#[derive(Debug, Clone, Subcommand)]
pub enum PeriodicCommand {
    /// List the schedules by name: timetable, zone, next run, last enqueue and status.
    List(ListArgs),
    /// Show one schedule: its definition, next ticks, state and history.
    Show(ShowArgs),
    /// Enqueue a schedule's job now (its timetable does not change); prints the jid.
    Enqueue(EnqueueNowArgs),
    /// Pause a schedule: its ticks are neither enqueued nor counted as missed.
    Pause(NameArgs),
    /// Resume a paused schedule at its first tick from now on.
    Unpause(NameArgs),
    /// Remove a schedule and its history (one defined in code needs --force).
    Remove(RemoveArgs),
    /// Create or update a dynamic schedule.
    Add(Box<AddArgs>),
    /// Repair the indexes of the schedules (after a partial restore, a manual edit).
    Repair,
    /// Preview a cron expression: its meaning and next ticks, with daylight saving changes.
    /// Needs no Redis.
    Next(NextArgs),
}

/// `hench periodic [list]`.
#[derive(Debug, Clone, Args)]
pub struct ListArgs {
    /// Print JSON instead of text.
    #[arg(long)]
    pub json: bool,
}

/// A command naming one schedule.
#[derive(Debug, Clone, Args)]
pub struct NameArgs {
    /// The schedule's name (as `hench periodic` prints it).
    #[arg(allow_hyphen_values = true)]
    pub name: String,
}

/// `hench periodic show`.
#[derive(Debug, Clone, Args)]
pub struct ShowArgs {
    /// The schedule's name (as `hench periodic` prints it).
    #[arg(allow_hyphen_values = true)]
    pub name: String,

    /// Print JSON instead of text.
    #[arg(long)]
    pub json: bool,
}

/// `hench periodic enqueue`.
#[derive(Debug, Clone, Args)]
pub struct EnqueueNowArgs {
    /// The schedule's name (as `hench periodic` prints it).
    #[arg(allow_hyphen_values = true)]
    pub name: String,

    /// Print JSON (`{"jid": ...}`) instead of the bare job id.
    #[arg(long)]
    pub json: bool,
}

/// `hench periodic remove`.
#[derive(Debug, Clone, Args)]
pub struct RemoveArgs {
    /// The schedule's name (as `hench periodic` prints it).
    #[arg(allow_hyphen_values = true)]
    pub name: String,

    /// Remove it even when it is defined in code (it comes back when a process whose code
    /// still defines it registers its schedules).
    #[arg(long)]
    pub force: bool,
}

/// `hench periodic add`.
#[derive(Debug, Clone, Args)]
pub struct AddArgs {
    /// The schedule's name (1 to 200 bytes, no control characters).
    #[arg(allow_hyphen_values = true)]
    pub name: String,

    /// A cron expression: 5 fields, 6 with seconds first, or an alias (@hourly, @daily,
    /// @weekly, @monthly, @yearly).
    #[arg(long, required_unless_present = "every", value_name = "EXPR")]
    pub cron: Option<String>,

    /// The time zone of the cron expression (an IANA name such as Europe/Paris; default UTC).
    // The conflicts are spelled out: clap lets `requires` pass when the required argument
    // conflicts with one given.
    #[arg(long, requires = "cron", conflicts_with = "every", value_name = "ZONE")]
    pub tz: Option<String>,

    /// An interval instead of a cron expression (e.g. 30s, 15m, 1h), aligned to the Unix epoch.
    #[arg(long, conflicts_with = "cron", value_parser = parse_duration, value_name = "DURATION")]
    pub every: Option<Duration>,

    /// Shifts the interval's ticks by this much (less than the interval).
    #[arg(
        long,
        requires = "every",
        conflicts_with = "cron",
        value_parser = parse_duration,
        value_name = "DURATION"
    )]
    pub offset: Option<Duration>,

    /// The job's class: the name its job type is registered under.
    #[arg(long)]
    pub class: String,

    /// The arguments, as a JSON array (a single other JSON value is one argument).
    #[arg(long, default_value = "[]", value_parser = parse_args)]
    pub args: JobArgs,

    /// The queue (default: "default"; the queue the job type declares is not known here).
    #[arg(long)]
    pub queue: Option<String>,

    /// Retries: `true`, `false` or a number.
    #[arg(long, value_parser = parse_retry)]
    pub retry: Option<Value>,

    /// What to do with ticks missed while no poller ran: latest (enqueue the most recent
    /// one, the default), skip, or all:N (each of them, at most N).
    #[arg(long, value_parser = parse_catch_up, value_name = "latest|skip|all:N")]
    pub catch_up: Option<CatchUp>,

    /// Ticks older than this are not enqueued (default for `skip`: 5m).
    #[arg(long, value_parser = parse_duration, value_name = "DURATION")]
    pub deadline: Option<Duration>,

    /// Create it paused (an update keeps the current state).
    #[arg(long)]
    pub paused: bool,

    /// What it is for (shown by the dashboard and `show`).
    #[arg(long, value_name = "TEXT")]
    pub description: Option<String>,

    /// Make the job unique for this long (e.g. 1h): a tick while the previous tick's job
    /// holds the lock is a duplicate, not enqueued.
    #[arg(long, value_parser = parse_duration, value_name = "DURATION")]
    pub unique_for: Option<Duration>,

    /// When the unique lock is released: success (the default), start or expired.
    #[arg(long, value_parser = ["success", "start", "expired"], requires = "unique_for")]
    pub unique_until: Option<String>,

    /// The value identifying duplicates instead of the arguments, as JSON.
    #[arg(long, value_parser = parse_json, requires = "unique_for", value_name = "JSON")]
    pub unique_key: Option<Value>,

    /// Subject the job's starts to a throttle: NAME, or NAME:KEY for its per-key limits.
    #[arg(long, value_parser = parse_throttle, value_name = "NAME[:KEY]")]
    pub throttle: Option<(String, Option<String>)>,
}

/// `hench periodic next`.
#[derive(Debug, Clone, Args)]
pub struct NextArgs {
    /// The cron expression (quote it: `"0 2 * * *"`).
    #[arg(allow_hyphen_values = true)]
    pub expression: String,

    /// The time zone (an IANA name such as Europe/Paris; default UTC).
    #[arg(long, value_name = "ZONE")]
    pub tz: Option<String>,

    /// How many ticks to list.
    #[arg(
        long,
        default_value_t = 5,
        value_parser = clap::value_parser!(u32).range(1..=i64::from(MAX_PREVIEW)),
        value_name = "N"
    )]
    pub count: u32,

    /// List the ticks after this time (RFC 3339; default: now).
    #[arg(long, value_parser = parse_time, value_name = "TIME")]
    pub after: Option<DateTime<Utc>>,
}

/// Parses `--catch-up`: `latest`, `skip` or `all:N` (N from 1 to 10 000).
pub fn parse_catch_up(text: &str) -> Result<CatchUp, String> {
    match text.trim() {
        "latest" => Ok(CatchUp::Latest),
        "skip" => Ok(CatchUp::Skip),
        "all" => Err("`all` needs the most ticks one catch-up enqueues: all:N".to_owned()),
        other => {
            let max = other
                .strip_prefix("all:")
                .ok_or_else(|| format!("`{text}` is not latest, skip or all:N"))?;
            match max.trim().parse::<u32>() {
                Ok(max @ 1..=10_000) => Ok(CatchUp::All { max }),
                _ => Err(format!("`{max}` is not a number from 1 to 10000")),
            }
        }
    }
}

/// Answers the commands that need no Redis (`next`) before the binary connects: `None` for
/// the others. The binary's dispatch calls it first, so that `next` works where Redis is not
/// reachable.
pub fn offline<W: Write>(args: &PeriodicArgs, out: &mut W) -> Option<anyhow::Result<()>> {
    match &args.command {
        Some(PeriodicCommand::Next(next_args)) => Some(next(next_args, out)),
        _ => None,
    }
}

/// Whether `hench stats all` prints this section: when some schedule exists.
pub async fn has_data(api: &Api) -> anyhow::Result<bool> {
    Ok(api.periodic().size().await? > 0)
}

/// Runs `hench periodic`.
pub async fn run<W: Write>(api: &Api, args: PeriodicArgs, out: &mut W) -> anyhow::Result<()> {
    match args.command {
        None => list(api, &args.list, out).await,
        Some(PeriodicCommand::List(list_args)) => list(api, &list_args, out).await,
        Some(PeriodicCommand::Show(show_args)) => show(api, &show_args, out).await,
        Some(PeriodicCommand::Enqueue(enqueue_args)) => enqueue(api, &enqueue_args, out).await,
        Some(PeriodicCommand::Pause(name)) => pause(api, &name.name, true, out).await,
        Some(PeriodicCommand::Unpause(name)) => pause(api, &name.name, false, out).await,
        Some(PeriodicCommand::Remove(remove_args)) => remove(api, &remove_args, out).await,
        Some(PeriodicCommand::Add(add_args)) => add(api, &add_args, out).await,
        Some(PeriodicCommand::Repair) => repair(api, out).await,
        Some(PeriodicCommand::Next(next_args)) => next(&next_args, out),
    }
}

/// Whether `entry` is active and overdue (see [`LATE_AFTER`]).
pub(crate) fn is_late(entry: &PeriodicEntry, now: SystemTime) -> bool {
    !entry.is_paused()
        && entry.next.is_some_and(|next| {
            now.duration_since(next)
                .is_ok_and(|overdue| overdue > LATE_AFTER)
        })
}

/// A duration in whole seconds and milliseconds as JSON numbers.
fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// A timetable as JSON: `{"cron", "tz"}` or `{"every", "offset"}` (seconds).
pub(crate) fn timetable_json(timetable: Option<&Timetable>) -> Value {
    match timetable {
        Some(Timetable::Cron { expression, tz }) => json!({"cron": expression, "tz": tz}),
        Some(Timetable::Every { interval, offset }) => {
            json!({"every": interval.as_secs(), "offset": offset.as_secs()})
        }
        _ => Value::Null,
    }
}

/// A history entry as JSON.
pub(crate) fn history_json(entry: &HistoryEntry) -> Value {
    json!({
        "tick": entry.tick.and_then(rfc3339),
        "at": rfc3339(entry.at),
        "result": entry.result.map(FireResult::as_str),
        "jid": entry.jid,
        "by": entry.by,
        "missed": entry.missed,
        "late_ms": entry.late.map(millis),
    })
}

/// A schedule as the JSON outputs show it (`hench periodic --json`, the JSON API).
pub(crate) fn schedule_json(entry: &PeriodicEntry, now: SystemTime) -> Value {
    let (source, group) = match &entry.source {
        PeriodicSource::Code { group } => ("code", Some(group.as_str())),
        _ => ("api", None),
    };
    let (catch_up_max, job) = (
        match entry.catch_up {
            CatchUp::All { max } => Some(max),
            _ => None,
        },
        entry.job.as_ref(),
    );
    json!({
        "name": entry.name,
        "source": source,
        "group": group,
        "timetable": timetable_json(entry.timetable.as_ref()),
        "description": entry.description,
        "class": job.map(|job| job.class.as_str()),
        "queue": job.map(|job| if job.queue.is_empty() { "default" } else { job.queue.as_str() }),
        "args": job.map(|job| &job.args),
        "catch_up": entry.catch_up.as_str(),
        "catch_up_max": catch_up_max,
        "deadline": entry.deadline.map(|deadline| deadline.as_secs()),
        "next": entry.next.and_then(rfc3339),
        "paused": entry.is_paused(),
        "paused_at": entry.paused_at.and_then(rfc3339),
        "overdue": is_late(entry, now),
        "last": entry.last.as_ref().map(|last| json!({
            "tick": rfc3339(last.tick),
            "at": rfc3339(last.at),
            "result": last.result.map(FireResult::as_str),
            "jid": last.jid,
            "by": last.by,
            "late_ms": millis(last.late()),
        })),
        "last_run": entry.last_run.as_ref().map(|run| json!({
            "tick": rfc3339(run.tick),
            "at": rfc3339(run.at),
            "status": run.status.map(RunStatus::as_str),
            "duration_ms": millis(run.duration),
            "error": run.error,
            "jid": run.jid,
        })),
        "manual": entry.manual_at.map(|at| json!({"at": rfc3339(at), "jid": entry.manual_jid})),
        "counters": {
            "fired": entry.fired,
            "missed": entry.missed,
            "skipped": entry.skipped,
            "run_failures": entry.run_failures,
        },
        "error": entry.error,
        "error_at": entry.error_at.and_then(rfc3339),
        "created_at": entry.created_at.and_then(rfc3339),
        "updated_at": entry.updated_at.and_then(rfc3339),
        "digest": entry.digest,
    })
}

/// A schedule with its next ticks, history and template, as JSON (`show --json`, the JSON
/// API's `GET /periodic/{name}`).
pub(crate) fn detail_json(
    entry: &PeriodicEntry,
    history: &[HistoryEntry],
    now: SystemTime,
) -> Value {
    let mut value = schedule_json(entry, now);
    let upcoming: Vec<Option<String>> = entry
        .upcoming(UPCOMING)
        .unwrap_or_default()
        .into_iter()
        .map(rfc3339)
        .collect();
    value["upcoming"] = json!(upcoming);
    value["describe"] = json!(entry.describe());
    value["template"] = entry
        .fields
        .get("job")
        .and_then(|job| serde_json::from_str(job).ok())
        .unwrap_or(Value::Null);
    value["history"] = Value::Array(history.iter().map(history_json).collect());
    value["fields"] = json!(entry.fields);
    value
}

/// A short span: `45s`, `3m`, `2h5m`, `3d`.
fn span(duration: Duration) -> String {
    let secs = duration.as_secs();
    match secs {
        0..60 => format!("{secs}s"),
        60..3_600 => format!("{}m", secs / 60),
        3_600..86_400 => match (secs % 3_600) / 60 {
            0 => format!("{}h", secs / 3_600),
            minutes => format!("{}h{minutes}m", secs / 3_600),
        },
        _ => format!("{}d", secs / 86_400),
    }
}

/// A timetable for a table cell: the expression, or `every 30s (+5s)`.
fn timetable_cell(timetable: Option<&Timetable>) -> (String, String) {
    match timetable {
        Some(Timetable::Cron { expression, tz }) => (
            printable(expression).into_owned(),
            printable(tz).into_owned(),
        ),
        Some(Timetable::Every { interval, offset }) => {
            let every = format!("every {}", span(*interval));
            let every = if offset.is_zero() {
                every
            } else {
                format!("{every} (+{})", span(*offset))
            };
            (every, "-".to_owned())
        }
        _ => ("?".to_owned(), "?".to_owned()),
    }
}

/// `at` on the schedule's clock: `2026-11-02 02:00:00 +01:00`.
fn local(entry: &PeriodicEntry, at: SystemTime) -> String {
    entry
        .timetable
        .as_ref()
        .and_then(|timetable| timetable.local_time(at))
        .or_else(|| rfc3339(at))
        .unwrap_or_else(|| "?".to_owned())
}

/// What was done with a tick, in words.
fn result_text(result: Option<FireResult>) -> &'static str {
    match result {
        Some(FireResult::Enqueued) => "enqueued",
        Some(FireResult::Scheduled) => "scheduled",
        Some(FireResult::Halted) => "stopped by middleware",
        Some(FireResult::Skipped) => "skipped",
        Some(FireResult::Duplicate) => "duplicate (unique lock held)",
        Some(FireResult::Manual) => "enqueued by hand",
        _ => "unknown",
    }
}

/// How a run ended, in words.
fn run_text(status: Option<RunStatus>) -> &'static str {
    match status {
        Some(RunStatus::Succeeded) => "succeeded",
        Some(RunStatus::Failed) => "failed",
        Some(RunStatus::Interrupted) => "interrupted",
        Some(RunStatus::Rescheduled) => "rescheduled",
        _ => "unknown",
    }
}

/// A schedule's status for the list: `paused`, `error`, `overdue` or `ok`, and whether its
/// last run failed.
fn status(entry: &PeriodicEntry, now: SystemTime) -> String {
    let state = if entry.is_paused() {
        "paused"
    } else if entry.error.is_some() {
        "error"
    } else if is_late(entry, now) {
        "overdue"
    } else {
        "ok"
    };
    let failed = entry
        .last_run
        .as_ref()
        .is_some_and(|run| run.status == Some(RunStatus::Failed));
    if failed {
        format!("{state}, last run failed")
    } else {
        state.to_owned()
    }
}

/// The text of `hench periodic [list]`.
pub(crate) fn render_list(entries: &[PeriodicEntry], now: SystemTime) -> String {
    let mut out = format!(
        "---- Periodic jobs ({}) ----\n",
        delimit(u64::try_from(entries.len()).unwrap_or(u64::MAX))
    );
    if entries.is_empty() {
        out.push_str("No periodic job is defined.\n");
        return out;
    }
    let rows: Vec<Vec<String>> = entries
        .iter()
        .map(|entry| {
            let (timetable, zone) = timetable_cell(entry.timetable.as_ref());
            let next = match entry.next {
                _ if entry.is_paused() => "paused".to_owned(),
                Some(next) => local(entry, next),
                None => "-".to_owned(),
            };
            let last = entry.last.as_ref().map_or_else(
                || "never".to_owned(),
                |last| format!("{} {}", result_text(last.result), relative(last.at, now)),
            );
            vec![
                printable(&entry.name).into_owned(),
                timetable,
                zone,
                next,
                last,
                status(entry, now),
            ]
        })
        .collect();
    out.push_str(&table(
        &["NAME", "TIMETABLE", "ZONE", "NEXT", "LAST", "STATUS"],
        &rows,
    ));
    out
}

/// The first `limit` schedules by name (a page of the index at a time), and how many exist.
pub(crate) async fn entries(api: &Api, limit: u64) -> hench::Result<(Vec<PeriodicEntry>, u64)> {
    let set = api.periodic();
    let mut found = Vec::new();
    let mut number = 1;
    loop {
        let page = set.page(number, PAGE).await?;
        let total = page.total;
        // Names this page covered (those whose hash is gone are left out).
        let read = u64::try_from(number.saturating_mul(PAGE))
            .unwrap_or(u64::MAX)
            .min(total);
        found.extend(page.items);
        if read >= total || read >= limit {
            return Ok((found, total));
        }
        number += 1;
    }
}

/// `hench periodic [list]`.
async fn list<W: Write>(api: &Api, args: &ListArgs, out: &mut W) -> anyhow::Result<()> {
    let (entries, _) = entries(api, u64::MAX).await?;
    let now = SystemTime::now();
    if args.json {
        let schedules: Vec<Value> = entries
            .iter()
            .map(|entry| schedule_json(entry, now))
            .collect();
        let value = json!({"size": entries.len(), "schedules": schedules});
        serde_json::to_writer_pretty(&mut *out, &value)?;
        writeln!(out)?;
    } else {
        out.write_all(render_list(&entries, now).as_bytes())?;
    }
    Ok(())
}

/// A catch-up policy in words: `latest`, `skip within 5m`, `all, at most 24`.
fn catch_up_text(policy: CatchUp, deadline: Option<Duration>) -> String {
    let within = deadline.map(|deadline| format!(" within {}", span(deadline)));
    let within = within.as_deref().unwrap_or("");
    match policy {
        CatchUp::Skip => format!("skip{within}"),
        CatchUp::All { max } => format!("all, at most {}{within}", delimit(u64::from(max))),
        _ => format!("latest{within}"),
    }
}

/// The text of `hench periodic show`.
pub(crate) fn render_show(
    entry: &PeriodicEntry,
    history: &[HistoryEntry],
    now: SystemTime,
) -> String {
    let mut rows: Vec<(&str, String)> = Vec::new();
    rows.push((
        "Defined by",
        match &entry.source {
            PeriodicSource::Code { group } => format!("code (group {})", printable(group)),
            _ => "the API, the dashboard or the command line".to_owned(),
        },
    ));
    if let Some(description) = entry.description.as_deref().filter(|text| !text.is_empty()) {
        rows.push(("Description", printable(description).into_owned()));
    }
    let (timetable, _) = timetable_cell(entry.timetable.as_ref());
    rows.push((
        "Timetable",
        format!("{timetable}: {}", printable(&entry.describe())),
    ));
    match entry.upcoming(UPCOMING) {
        Ok(ticks) if ticks.is_empty() => {
            rows.push(("Next ticks", "none while paused".to_owned()));
        }
        Ok(ticks) => {
            for (index, tick) in ticks.into_iter().enumerate() {
                let label = if index == 0 { "Next ticks" } else { "" };
                let mut text = format!("{} ({})", local(entry, tick), relative(tick, now));
                if let Some(note) = entry
                    .timetable
                    .as_ref()
                    .and_then(|timetable| timetable.transition_note(tick))
                {
                    text.push_str(&format!("; {note}"));
                }
                rows.push((label, text));
            }
        }
        Err(error) => rows.push(("Next ticks", printable(&error.to_string()).into_owned())),
    }
    rows.push(("Catch-up", catch_up_text(entry.catch_up, entry.deadline)));
    match &entry.job {
        Some(job) => {
            let queue = if job.queue.is_empty() {
                "default"
            } else {
                job.queue.as_str()
            };
            let args = serde_json::to_string(&job.args).unwrap_or_default();
            rows.push((
                "Job",
                format!(
                    "{} in queue {}, arguments {}",
                    printable(&job.class),
                    printable(queue),
                    printable(&args)
                ),
            ));
        }
        None => rows.push(("Job", "unreadable".to_owned())),
    }
    rows.push((
        "Next run",
        match entry.next {
            _ if entry.is_paused() => "paused".to_owned(),
            Some(next) => format!("{} ({})", local(entry, next), relative(next, now)),
            None => "unknown".to_owned(),
        },
    ));
    if let Some(at) = entry.paused_at {
        rows.push(("Paused", format!("since {}", when(at, now))));
    }
    rows.push((
        "Last enqueue",
        match &entry.last {
            Some(last) => {
                let mut text = format!(
                    "tick {}: {} {}",
                    local(entry, last.tick),
                    result_text(last.result),
                    relative(last.at, now)
                );
                if last.late() >= Duration::from_secs(1) {
                    text.push_str(&format!(" (late {})", span(last.late())));
                }
                if let Some(jid) = &last.jid {
                    text.push_str(&format!(", jid {}", printable(jid)));
                }
                if let Some(by) = &last.by {
                    text.push_str(&format!(", by {}", printable(by)));
                }
                text
            }
            None => "never".to_owned(),
        },
    ));
    if let Some(at) = entry.manual_at {
        let jid = entry
            .manual_jid
            .as_deref()
            .map(|jid| format!(", jid {}", printable(jid)))
            .unwrap_or_default();
        rows.push(("By hand", format!("{}{jid}", when(at, now))));
    }
    rows.push((
        "Last run",
        match &entry.last_run {
            Some(run) => {
                let mut text = format!(
                    "{} {} ({})",
                    run_text(run.status),
                    relative(run.at, now),
                    span(run.duration)
                );
                if let Some(jid) = &run.jid {
                    text.push_str(&format!(", jid {}", printable(jid)));
                }
                if let Some(error) = run.error.as_deref().filter(|error| !error.is_empty()) {
                    text.push_str(&format!(": {}", printable(error)));
                }
                text
            }
            None => "never".to_owned(),
        },
    ));
    rows.push((
        "Counters",
        format!(
            "{} enqueued, {} missed, {} skipped, {} failed runs",
            delimit(entry.fired),
            delimit(entry.missed),
            delimit(entry.skipped),
            delimit(entry.run_failures)
        ),
    ));
    if let Some(error) = &entry.error {
        let since = entry
            .error_at
            .map(|at| format!(" (since {})", when(at, now)))
            .unwrap_or_default();
        rows.push(("Error", format!("{}{since}", printable(error))));
    }
    if let Some(at) = entry.created_at {
        rows.push(("Created", when(at, now)));
    }
    if let Some(at) = entry.updated_at {
        rows.push(("Updated", when(at, now)));
    }
    rows.push(("Digest", printable(&entry.digest).into_owned()));
    let mut out = format!("---- Periodic job {} ----\n", printable(&entry.name));
    for (label, value) in rows {
        if label.is_empty() {
            // A continuation (the next ticks after the first).
            out.push_str(&format!("{:14}{value}\n", ""));
        } else {
            out.push_str(&format!("{label:>12}: {value}\n"));
        }
    }
    out.push('\n');
    if history.is_empty() {
        out.push_str("History: no tick handled yet.\n");
        return out;
    }
    out.push_str("History (newest first):\n");
    let rows: Vec<Vec<String>> = history
        .iter()
        .map(|item| {
            vec![
                item.tick
                    .map_or_else(|| "(by hand)".to_owned(), |tick| local(entry, tick)),
                relative(item.at, now),
                result_text(item.result).to_owned(),
                item.late
                    .filter(|late| *late >= Duration::from_secs(1))
                    .map(span)
                    .unwrap_or_default(),
                item.jid
                    .as_deref()
                    .map(|jid| printable(jid).into_owned())
                    .unwrap_or_default(),
                item.by
                    .as_deref()
                    .map(|by| printable(by).into_owned())
                    .unwrap_or_default(),
                if item.missed > 0 {
                    delimit(item.missed)
                } else {
                    String::new()
                },
            ]
        })
        .collect();
    out.push_str(&table(
        &[
            "TICK", "HANDLED", "RESULT", "LATE", "JID", "PROCESS", "MISSED",
        ],
        &rows,
    ));
    out
}

/// The schedule `name`, or an error saying there is none.
async fn entry(api: &Api, name: &str) -> anyhow::Result<PeriodicEntry> {
    match api.periodic().get(name).await? {
        Some(entry) => Ok(entry),
        None => bail!("there is no periodic job {}", printable(name)),
    }
}

/// `hench periodic show`.
async fn show<W: Write>(api: &Api, args: &ShowArgs, out: &mut W) -> anyhow::Result<()> {
    let entry = entry(api, &args.name).await?;
    let history = api.periodic().history(&args.name, HISTORY).await?;
    let now = SystemTime::now();
    if args.json {
        serde_json::to_writer_pretty(&mut *out, &detail_json(&entry, &history, now))?;
        writeln!(out)?;
    } else {
        out.write_all(render_show(&entry, &history, now).as_bytes())?;
    }
    Ok(())
}

/// `hench periodic enqueue`.
async fn enqueue<W: Write>(api: &Api, args: &EnqueueNowArgs, out: &mut W) -> anyhow::Result<()> {
    let shown = printable(&args.name);
    let Some(jid) = api.periodic().enqueue(&args.name).await? else {
        bail!(
            "the job of {shown} was not enqueued: client middleware stopped it, or the job is \
             unique and its lock is held"
        );
    };
    tracing::info!(schedule = args.name, jid, "enqueued a periodic job by hand");
    if args.json {
        writeln!(out, "{}", json!({"jid": jid}))?;
    } else {
        writeln!(out, "{jid}")?;
    }
    Ok(())
}

/// `hench periodic pause` (`pause`) and `unpause`.
async fn pause<W: Write>(api: &Api, name: &str, pause: bool, out: &mut W) -> anyhow::Result<()> {
    let shown = printable(name);
    let set = api.periodic();
    let changed = if pause {
        set.pause(name).await?
    } else {
        set.unpause(name).await?
    };
    if !changed {
        let entry = entry(api, name).await?;
        let state = if entry.is_paused() {
            "paused"
        } else {
            "running"
        };
        writeln!(out, "Periodic job {shown} was already {state}.")?;
        return Ok(());
    }
    tracing::info!(schedule = name, pause, "changed a periodic job");
    let done = if pause { "Paused" } else { "Resumed" };
    writeln!(out, "{done} periodic job {shown}.")?;
    Ok(())
}

/// `hench periodic remove`.
async fn remove<W: Write>(api: &Api, args: &RemoveArgs, out: &mut W) -> anyhow::Result<()> {
    let name = args.name.as_str();
    let shown = printable(name);
    let entry = entry(api, name).await?;
    let set = api.periodic();
    let removed = match &entry.source {
        PeriodicSource::Code { group } if !args.force => bail!(
            "periodic job {shown} is defined in the code of group {}: remove it from the code, \
             or remove it here with --force (a process whose code still defines it registers \
             it again when it starts)",
            printable(group)
        ),
        PeriodicSource::Code { .. } => set.remove_any(name).await?,
        _ => set.remove(name).await?,
    };
    if !removed {
        bail!("there is no periodic job {shown}");
    }
    tracing::info!(schedule = name, "removed a periodic job");
    writeln!(out, "Removed periodic job {shown}.")?;
    Ok(())
}

/// The dynamic schedule `add` describes.
pub(crate) fn periodic_of(args: &AddArgs) -> anyhow::Result<Periodic> {
    let mut periodic = match (&args.cron, args.every) {
        (Some(expression), None) => {
            let periodic = Periodic::cron(&args.name, expression);
            match &args.tz {
                Some(zone) => periodic.tz(zone),
                None => periodic,
            }
        }
        (None, Some(interval)) => {
            let periodic = Periodic::every(&args.name, interval);
            match args.offset {
                Some(offset) => periodic.offset(offset),
                None => periodic,
            }
        }
        _ => bail!("give either --cron or --every"),
    };
    periodic = periodic.class(&args.class, args.args.0.clone());
    if let Some(queue) = &args.queue {
        periodic = periodic.queue(queue);
    }
    if let Some(retry) = &args.retry {
        periodic = match retry {
            Value::Bool(retry) => periodic.retry(RetryOption::from(*retry)),
            other => {
                let count = other
                    .as_u64()
                    .and_then(|count| u32::try_from(count).ok())
                    .context("--retry takes true, false or a number")?;
                periodic.retry(RetryOption::from(count))
            }
        };
    }
    if let Some(policy) = args.catch_up {
        periodic = periodic.catch_up(policy);
    }
    if let Some(deadline) = args.deadline {
        periodic = periodic.deadline(deadline);
    }
    if args.paused {
        periodic = periodic.paused(true);
    }
    if let Some(description) = &args.description {
        periodic = periodic.description(description);
    }
    if let Some(unique_for) = args.unique_for {
        periodic = periodic.unique_for(unique_for);
        if let Some(until) = &args.unique_until {
            periodic = periodic.unique_until(match until.as_str() {
                "start" => UniqueUntil::Start,
                "expired" => UniqueUntil::Expired,
                _ => UniqueUntil::Success,
            });
        }
        if let Some(key) = &args.unique_key {
            periodic = periodic.unique_key(key.clone());
        }
    }
    if let Some((name, key)) = &args.throttle {
        periodic = periodic.throttle(name);
        if let Some(key) = key {
            periodic = periodic.throttle_key(key);
        }
    }
    Ok(periodic)
}

/// `hench periodic add`.
async fn add<W: Write>(api: &Api, args: &AddArgs, out: &mut W) -> anyhow::Result<()> {
    let periodic = periodic_of(args)?;
    let upserted = api.periodic().upsert(&periodic).await?;
    let done = match upserted {
        Upserted::Created => "created",
        Upserted::Updated => "updated",
        Upserted::Rescheduled => "rescheduled",
        _ => "unchanged",
    };
    tracing::info!(schedule = args.name, done, "upserted a periodic job");
    writeln!(out, "{done}")?;
    Ok(())
}

/// `hench periodic repair`.
async fn repair<W: Write>(api: &Api, out: &mut W) -> anyhow::Result<()> {
    let repairs = api.periodic().repair().await?;
    tracing::info!(repairs, "repaired the periodic indexes");
    writeln!(
        out,
        "{} {}.",
        delimit(repairs),
        if repairs == 1 { "repair" } else { "repairs" }
    )?;
    Ok(())
}

/// The text of `hench periodic next`.
pub(crate) fn render_next(timetable: &Timetable, ticks: &[SystemTime], now: SystemTime) -> String {
    let mut out = format!("{}\n", printable(&timetable.describe()));
    for tick in ticks {
        let local = timetable
            .local_time(*tick)
            .unwrap_or_else(|| "?".to_owned());
        let utc = rfc3339(*tick).unwrap_or_else(|| "?".to_owned());
        out.push_str(&format!("{local}  ({utc}, {})\n", relative(*tick, now)));
        if let Some(note) = timetable.transition_note(*tick) {
            out.push_str(&format!("    note: {note}\n"));
        }
    }
    out
}

/// `hench periodic next`: offline.
fn next<W: Write>(args: &NextArgs, out: &mut W) -> anyhow::Result<()> {
    let timetable = Timetable::Cron {
        expression: args.expression.clone(),
        tz: args.tz.clone().unwrap_or_else(|| "UTC".to_owned()),
    };
    timetable
        .validate()
        .with_context(|| format!("`{}` cannot be used", printable(&args.expression)))?;
    let now = SystemTime::now();
    let after = args.after.map_or(now, SystemTime::from);
    let count = usize::try_from(args.count).unwrap_or(usize::MAX);
    let ticks = timetable.upcoming(after, count)?;
    out.write_all(render_next(&timetable, &ticks, now).as_bytes())?;
    Ok(())
}

/// The numbers of `hench stats periodic`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PeriodicStats {
    pub(crate) size: u64,
    pub(crate) active: u64,
    pub(crate) paused: u64,
    /// Schedules with an error (parked, or a definition this version cannot read), among the
    /// first [`SCAN_LIMIT`].
    pub(crate) parked: u64,
    /// More schedules exist than were read for `parked`.
    pub(crate) truncated: bool,
    pub(crate) overdue: u64,
    pub(crate) next: Option<(String, SystemTime)>,
}

/// Gathers the numbers of `hench stats periodic`.
pub(crate) async fn gather_stats(api: &Api) -> hench::Result<PeriodicStats> {
    let summary = api.periodic().summary(LATE_AFTER).await?;
    let (read, total) = entries(api, SCAN_LIMIT).await?;
    let parked = read.iter().filter(|entry| entry.error.is_some()).count();
    Ok(PeriodicStats {
        size: summary.total,
        active: summary.active,
        paused: summary.paused(),
        parked: u64::try_from(parked).unwrap_or(u64::MAX),
        truncated: total > SCAN_LIMIT,
        overdue: summary.late,
        next: summary.next,
    })
}

/// The text of `hench stats periodic`.
pub(crate) fn render_stats(stats: &PeriodicStats, now: SystemTime) -> String {
    let mut out = String::from("---- Periodic ----\n");
    let parked = if stats.truncated {
        format!(
            "{} (among the first {})",
            delimit(stats.parked),
            delimit(SCAN_LIMIT)
        )
    } else {
        delimit(stats.parked)
    };
    let next = stats.next.as_ref().map_or_else(
        || "-".to_owned(),
        |(name, at)| format!("{} {}", printable(name), relative(*at, now)),
    );
    for (label, value) in [
        ("Defined", delimit(stats.size)),
        ("Paused", delimit(stats.paused)),
        ("Errors", parked),
        ("Overdue", delimit(stats.overdue)),
        ("Next due", next),
    ] {
        out.push_str(&format!("{label:>9}: {value}\n"));
    }
    out
}

/// Prints the `periodic` section of `hench stats` (`json`: as JSON).
pub async fn stats<W: Write>(api: &Api, json: bool, out: &mut W) -> anyhow::Result<()> {
    let stats = gather_stats(api).await?;
    if json {
        let value = json!({
            "size": stats.size,
            "active": stats.active,
            "paused": stats.paused,
            "errors": stats.parked,
            "errors_truncated": stats.truncated,
            "overdue": stats.overdue,
            "next": stats.next.as_ref().map(|(name, at)| json!({"name": name, "at": rfc3339(*at)})),
        });
        serde_json::to_writer_pretty(&mut *out, &value)?;
        writeln!(out)?;
    } else {
        out.write_all(render_stats(&stats, SystemTime::now()).as_bytes())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    //! `hench periodic` against a private Redis (`docs/design/periodic.md` §3.4 item 33), run
    //! past the dormancy switch.

    use clap::Parser;
    use hench::periodic::ManualClock;
    use hench::test_util::TestRedis;
    use hench::{keys, redis_rs};

    use super::*;
    use crate::cli::{Cli, Command};

    fn parse(words: &[&str]) -> Result<PeriodicArgs, clap::Error> {
        let mut argv = vec!["hench", "periodic"];
        argv.extend_from_slice(words);
        match Cli::try_parse_from(argv)?.command {
            Command::Periodic(args) => Ok(args),
            _ => panic!("not `hench periodic`"),
        }
    }

    fn args(words: &[&str]) -> PeriodicArgs {
        parse(words).unwrap()
    }

    async fn periodic(api: &Api, words: &[&str]) -> anyhow::Result<String> {
        let mut out = Vec::new();
        run(api, args(words), &mut out).await?;
        Ok(String::from_utf8(out).unwrap())
    }

    /// `nightly` (02:00 in Paris, with a description), `sync` (every 30 s, 5 s late) and
    /// `cleanup` (paused).
    async fn seeded(redis: &TestRedis) -> Api {
        let api = Api::new(redis.pool().await);
        for words in [
            &[
                "add",
                "nightly",
                "--cron",
                "0 2 * * *",
                "--tz",
                "Europe/Paris",
                "--class",
                "Report",
                "--args",
                r#"[42, "all"]"#,
                "--queue",
                "reports",
                "--description",
                "The nightly report.",
            ][..],
            &[
                "add", "sync", "--every", "30s", "--offset", "5s", "--class", "Sync", "--retry",
                "3",
            ],
            &[
                "add",
                "cleanup",
                "--cron",
                "*/5 * * * *",
                "--class",
                "Cleanup",
                "--paused",
                "--catch-up",
                "all:24",
            ],
        ] {
            assert_eq!(
                periodic(&api, words).await.unwrap(),
                "created\n",
                "{words:?}"
            );
        }
        api
    }

    #[tokio::test]
    async fn the_list_prints_a_table_or_json() {
        let redis = TestRedis::start().await;
        let api = Api::new(redis.pool().await);
        assert_eq!(
            periodic(&api, &[]).await.unwrap(),
            "---- Periodic jobs (0) ----\nNo periodic job is defined.\n"
        );
        let api = seeded(&redis).await;
        let text = periodic(&api, &["list"]).await.unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines[0], "---- Periodic jobs (3) ----");
        assert!(lines[1].starts_with("NAME"), "{text}");
        for header in ["TIMETABLE", "ZONE", "NEXT", "LAST", "STATUS"] {
            assert!(lines[1].contains(header), "{text}");
        }
        assert!(
            lines[2].starts_with("cleanup") && lines[2].contains("paused"),
            "{text}"
        );
        assert!(lines[3].starts_with("nightly"), "{text}");
        assert!(
            lines[3].contains("0 2 * * *") && lines[3].contains("Europe/Paris"),
            "{text}"
        );
        assert!(lines[3].contains(" 02:00:00 +0"), "{text}");
        assert!(
            lines[3].contains("never") && lines[3].ends_with("ok"),
            "{text}"
        );
        assert!(
            lines[4].starts_with("sync") && lines[4].contains("every 30s (+5s)"),
            "{text}"
        );
        let json: Value =
            serde_json::from_str(&periodic(&api, &["--json"]).await.unwrap()).unwrap();
        assert_eq!(json["size"], 3);
        let nightly = &json["schedules"][1];
        assert_eq!(nightly["name"], "nightly");
        assert_eq!(nightly["source"], "api");
        assert_eq!(nightly["group"], Value::Null);
        assert_eq!(
            nightly["timetable"],
            json!({"cron": "0 2 * * *", "tz": "Europe/Paris"})
        );
        assert_eq!(nightly["class"], "Report");
        assert_eq!(nightly["queue"], "reports");
        assert_eq!(nightly["args"], json!([42, "all"]));
        assert_eq!(nightly["paused"], false);
        assert_eq!(nightly["catch_up"], "latest");
        assert_eq!(nightly["counters"]["fired"], 0);
        assert!(nightly["next"].is_string());
        let sync = &json["schedules"][2];
        assert_eq!(sync["timetable"], json!({"every": 30, "offset": 5}));
        assert_eq!(sync["queue"], "default");
        let cleanup = &json["schedules"][0];
        assert_eq!(cleanup["paused"], true);
        assert_eq!(cleanup["catch_up"], "all");
        assert_eq!(cleanup["catch_up_max"], 24);
    }

    #[tokio::test]
    async fn show_prints_the_definition_state_and_history() {
        let redis = TestRedis::start().await;
        let api = seeded(&redis).await;
        let jid = periodic(&api, &["enqueue", "nightly"]).await.unwrap();
        let jid = jid.trim();
        assert_eq!(jid.len(), 24, "{jid}");
        let text = periodic(&api, &["show", "nightly"]).await.unwrap();
        assert!(
            text.starts_with("---- Periodic job nightly ----\n"),
            "{text}"
        );
        for expected in [
            "  Defined by: the API, the dashboard or the command line",
            " Description: The nightly report.",
            "   Timetable: 0 2 * * *: ",
            "  Next ticks: ",
            "    Catch-up: latest",
            "         Job: Report in queue reports, arguments [42,\"all\"]",
            "Last enqueue: never",
            "     By hand: ",
            "    Counters: 0 enqueued, 0 missed, 0 skipped, 0 failed runs",
            "History (newest first):",
            "enqueued by hand",
        ] {
            assert!(text.contains(expected), "{expected}\n{text}");
        }
        assert!(text.contains(jid), "{text}");
        // Five next ticks, at 02:00 in Paris.
        assert_eq!(text.matches(" 02:00:00 +0").count(), 6, "{text}");
        let json: Value = serde_json::from_str(
            &periodic(&api, &["show", "nightly", "--json"])
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(json["upcoming"].as_array().unwrap().len(), 5);
        assert_eq!(json["template"]["class"], "Report");
        assert_eq!(json["history"][0]["result"], "manual");
        assert_eq!(json["history"][0]["jid"], jid);
        assert_eq!(json["manual"]["jid"], jid);
        assert_eq!(json["fields"]["source"], "api");
        assert!(json["describe"].as_str().unwrap().contains("Europe/Paris"));
        // Paused: no next ticks.
        let text = periodic(&api, &["show", "cleanup"]).await.unwrap();
        assert!(text.contains("  Next ticks: none while paused"), "{text}");
        assert!(text.contains("    Next run: paused"), "{text}");
        assert!(text.contains("    Catch-up: all, at most 24"), "{text}");
        assert!(text.contains("History: no tick handled yet."), "{text}");
        let error = periodic(&api, &["show", "missing"]).await.unwrap_err();
        assert_eq!(error.to_string(), "there is no periodic job missing");
    }

    #[tokio::test]
    async fn enqueue_pause_unpause_and_remove() {
        let redis = TestRedis::start().await;
        let api = seeded(&redis).await;
        let json: Value = serde_json::from_str(
            &periodic(&api, &["enqueue", "sync", "--json"])
                .await
                .unwrap(),
        )
        .unwrap();
        assert!(json["jid"].is_string());
        assert_eq!(api.queue("default").size().await.unwrap(), 1);
        assert_eq!(
            periodic(&api, &["pause", "nightly"]).await.unwrap(),
            "Paused periodic job nightly.\n"
        );
        assert_eq!(
            periodic(&api, &["pause", "nightly"]).await.unwrap(),
            "Periodic job nightly was already paused.\n"
        );
        assert_eq!(
            periodic(&api, &["unpause", "nightly"]).await.unwrap(),
            "Resumed periodic job nightly.\n"
        );
        assert_eq!(
            periodic(&api, &["unpause", "nightly"]).await.unwrap(),
            "Periodic job nightly was already running.\n"
        );
        assert!(periodic(&api, &["pause", "missing"]).await.is_err());
        // Code schedules need --force.
        let () = {
            let mut conn = api.redis().conn().await.unwrap();
            redis_rs::cmd("HSET")
                .arg(keys::periodic("sync"))
                .arg("source")
                .arg("code")
                .arg("group")
                .arg("web")
                .query_async(&mut conn)
                .await
                .unwrap()
        };
        let error = periodic(&api, &["remove", "sync"]).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("defined in the code of group web"),
            "{error}"
        );
        assert_eq!(
            periodic(&api, &["remove", "sync", "--force"])
                .await
                .unwrap(),
            "Removed periodic job sync.\n"
        );
        assert_eq!(
            periodic(&api, &["remove", "nightly"]).await.unwrap(),
            "Removed periodic job nightly.\n"
        );
        let error = periodic(&api, &["remove", "nightly"]).await.unwrap_err();
        assert_eq!(error.to_string(), "there is no periodic job nightly");
        assert_eq!(api.periodic().size().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn an_enqueue_that_pushes_nothing_fails() {
        let redis = TestRedis::start().await;
        let api = Api::new(redis.pool().await);
        let created = periodic(
            &api,
            &[
                "add",
                "once",
                "--every",
                "1h",
                "--class",
                "Once",
                "--unique-for",
                "1h",
            ],
        )
        .await
        .unwrap();
        assert_eq!(created, "created\n");
        periodic(&api, &["enqueue", "once"]).await.unwrap();
        let error = periodic(&api, &["enqueue", "once"]).await.unwrap_err();
        assert!(error.to_string().contains("its lock is held"), "{error}");
    }

    #[tokio::test]
    async fn add_creates_updates_and_reschedules() {
        let redis = TestRedis::start().await;
        let api = seeded(&redis).await;
        let nightly = [
            "add",
            "nightly",
            "--cron",
            "0 2 * * *",
            "--tz",
            "Europe/Paris",
            "--class",
            "Report",
            "--args",
            r#"[42, "all"]"#,
            "--queue",
            "reports",
            "--description",
            "The nightly report.",
        ];
        assert_eq!(periodic(&api, &nightly).await.unwrap(), "unchanged\n");
        let mut updated = nightly.to_vec();
        updated.extend(["--retry", "false"]);
        assert_eq!(periodic(&api, &updated).await.unwrap(), "updated\n");
        let mut rescheduled = updated.clone();
        rescheduled[3] = "0 3 * * *";
        assert_eq!(periodic(&api, &rescheduled).await.unwrap(), "rescheduled\n");
        let entry = api.periodic().get("nightly").await.unwrap().unwrap();
        assert_eq!(
            entry.timetable,
            Some(Timetable::Cron {
                expression: "0 3 * * *".to_owned(),
                tz: "Europe/Paris".to_owned()
            })
        );
        assert_eq!(
            entry.job.unwrap().retry,
            Some(hench::RetryOption::Bool(false))
        );
        // Unique and throttled templates.
        let created = periodic(
            &api,
            &[
                "add",
                "digest",
                "--every",
                "1h",
                "--class",
                "Digest",
                "--unique-for",
                "1h",
                "--unique-until",
                "start",
                "--unique-key",
                r#""daily""#,
                "--throttle",
                "mail:acme",
                "--catch-up",
                "skip",
                "--deadline",
                "10m",
            ],
        )
        .await
        .unwrap();
        assert_eq!(created, "created\n");
        let entry = api.periodic().get("digest").await.unwrap().unwrap();
        let template: Value = serde_json::from_str(&entry.fields["job"]).unwrap();
        assert_eq!(template["unique"]["for"], 3_600);
        assert_eq!(template["unique"]["until"], "start");
        assert_eq!(template["unique"]["key"], "daily");
        assert_eq!(template["throttle"], json!({"name": "mail", "key": "acme"}));
        assert_eq!(entry.catch_up, CatchUp::Skip);
        assert_eq!(entry.deadline, Some(Duration::from_secs(600)));
        // Refused definitions say why.
        for (words, expected) in [
            (
                &["add", "bad", "--cron", "61 * * * *", "--class", "X"][..],
                "`bad`",
            ),
            (
                &[
                    "add",
                    "bad",
                    "--cron",
                    "* * * * *",
                    "--tz",
                    "Mars/Olympus",
                    "--class",
                    "X",
                ],
                "Mars/Olympus",
            ),
            (&["add", "bad", "--every", "0s", "--class", "X"], "`bad`"),
        ] {
            let error = periodic(&api, words).await.unwrap_err();
            assert!(
                format!("{error:#}").contains(expected),
                "{words:?}: {error:#}"
            );
        }
        // A code schedule cannot be changed this way.
        let () = {
            let mut conn = api.redis().conn().await.unwrap();
            redis_rs::cmd("HSET")
                .arg(keys::periodic("nightly"))
                .arg("source")
                .arg("code")
                .query_async(&mut conn)
                .await
                .unwrap()
        };
        let error = periodic(&api, &nightly).await.unwrap_err();
        assert!(error.to_string().contains("defined by code"), "{error}");
    }

    #[tokio::test]
    async fn repair_restores_the_indexes() {
        let redis = TestRedis::start().await;
        let api = seeded(&redis).await;
        assert_eq!(periodic(&api, &["repair"]).await.unwrap(), "0 repairs.\n");
        let () = {
            let mut conn = api.redis().conn().await.unwrap();
            redis_rs::cmd("ZREM")
                .arg(keys::PERIODIC)
                .arg("nightly")
                .query_async(&mut conn)
                .await
                .unwrap()
        };
        assert_eq!(periodic(&api, &["repair"]).await.unwrap(), "1 repair.\n");
    }

    #[test]
    fn next_previews_an_expression_with_daylight_saving_changes() {
        let mut out = Vec::new();
        let preview = args(&[
            "next",
            "30 1 * * *",
            "--tz",
            "Europe/London",
            "--after",
            "2027-03-26T12:00:00Z",
            "--count",
            "3",
        ]);
        let Some(PeriodicCommand::Next(next_args)) = preview.command else {
            panic!("not next");
        };
        next(&next_args, &mut out).unwrap();
        let text = String::from_utf8(out).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert!(lines[0].contains("Europe/London"), "{text}");
        assert!(
            lines[1].starts_with("2027-03-27 01:30:00 +00:00  (2027-03-27T01:30:00+00:00"),
            "{text}"
        );
        assert!(text.contains("note: the clocks change"), "{text}");
        assert!(text.contains("2027-03-29 01:30:00 +01:00"), "{text}");
        // Invalid expressions and zones say why.
        for words in [
            &["next", "not cron"][..],
            &["next", "* * * * *", "--tz", "Nowhere/Land"],
            &["next", "0 0 31 2 *"],
        ] {
            let Some(PeriodicCommand::Next(next_args)) = args(words).command else {
                panic!("not next");
            };
            let error = next(&next_args, &mut Vec::new()).unwrap_err();
            assert!(
                format!("{error:#}").contains("cannot be used"),
                "{words:?}: {error:#}"
            );
        }
    }

    #[tokio::test]
    async fn stats_print_the_counts() {
        let redis = TestRedis::start().await;
        let api = seeded(&redis).await;
        // An overdue schedule (defined two hours ago, never polled) and a parked one.
        let clock = ManualClock::new(SystemTime::now() - Duration::from_secs(7_200));
        api.periodic()
            .with_clock(clock)
            .upsert(&Periodic::cron("overdue", "*/10 * * * *").class("Late", vec![]))
            .await
            .unwrap();
        let () = {
            let mut conn = api.redis().conn().await.unwrap();
            redis_rs::cmd("HSET")
                .arg(keys::periodic("sync"))
                .arg("error")
                .arg("boom")
                .query_async(&mut conn)
                .await
                .unwrap()
        };
        let mut out = Vec::new();
        stats(&api, false, &mut out).await.unwrap();
        let text = String::from_utf8(out).unwrap();
        for expected in [
            "---- Periodic ----",
            "  Defined: 4",
            "   Paused: 1",
            "   Errors: 1",
            "  Overdue: 1",
            " Next due: overdue",
        ] {
            assert!(text.contains(expected), "{expected}\n{text}");
        }
        let mut out = Vec::new();
        stats(&api, true, &mut out).await.unwrap();
        let json: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(json["size"], 4);
        assert_eq!(json["active"], 3);
        assert_eq!(json["paused"], 1);
        assert_eq!(json["errors"], 1);
        assert_eq!(json["overdue"], 1);
        assert_eq!(json["next"]["name"], "overdue");
        // Overdue in the list too, and errors.
        let text = periodic(&api, &["list"]).await.unwrap();
        assert!(
            text.lines()
                .any(|line| line.starts_with("overdue") && line.ends_with("overdue")),
            "{text}"
        );
        assert!(
            text.lines()
                .any(|line| line.starts_with("sync") && line.ends_with("error")),
            "{text}"
        );
    }

    #[tokio::test]
    async fn next_runs_offline_and_stats_all_needs_a_schedule() {
        let redis = TestRedis::start().await;
        assert!(!has_data(&Api::new(redis.pool().await)).await.unwrap());
        let api = seeded(&redis).await;
        assert!(has_data(&api).await.unwrap());
        let mut out = Vec::new();
        assert!(offline(&args(&["next", "@daily"]), &mut out).is_some());
        assert!(offline(&args(&["list"]), &mut out).is_none());
    }

    #[test]
    fn the_command_line_is_parsed() {
        let list = args(&[]);
        assert!(list.command.is_none() && !list.list.json);
        assert!(args(&["--json"]).list.json);
        assert!(matches!(
            args(&["list", "--json"]).command,
            Some(PeriodicCommand::List(ListArgs { json: true }))
        ));
        // Names may start with `-`.
        let Some(PeriodicCommand::Show(show)) = args(&["show", "-nightly", "--json"]).command
        else {
            panic!("not show");
        };
        assert_eq!(show.name, "-nightly");
        assert!(show.json);
        let Some(PeriodicCommand::Remove(remove)) = args(&["remove", "a/b", "--force"]).command
        else {
            panic!("not remove");
        };
        assert!(remove.force && remove.name == "a/b");
        assert!(matches!(
            args(&["repair"]).command,
            Some(PeriodicCommand::Repair)
        ));
        let Some(PeriodicCommand::Add(add)) = args(&[
            "add",
            "n",
            "--every",
            "1h30m",
            "--offset",
            "5m",
            "--class",
            "C",
            "--catch-up",
            "all:5",
        ])
        .command
        else {
            panic!("not add");
        };
        assert_eq!(add.every, Some(Duration::from_secs(5_400)));
        assert_eq!(add.offset, Some(Duration::from_secs(300)));
        assert_eq!(add.catch_up, Some(CatchUp::All { max: 5 }));
        let Some(PeriodicCommand::Next(preview)) = args(&["next", "@hourly"]).command else {
            panic!("not next");
        };
        assert_eq!(preview.count, 5);
        assert!(preview.tz.is_none() && preview.after.is_none());
        for refused in [
            &["show"][..],
            &["add", "n", "--class", "C"],
            &["add", "n", "--cron", "* * * * *"],
            &[
                "add",
                "n",
                "--cron",
                "* * * * *",
                "--every",
                "1m",
                "--class",
                "C",
            ],
            &["add", "n", "--every", "1m", "--tz", "UTC", "--class", "C"],
            &[
                "add",
                "n",
                "--cron",
                "* * * * *",
                "--offset",
                "1s",
                "--class",
                "C",
            ],
            &[
                "add",
                "n",
                "--every",
                "1m",
                "--class",
                "C",
                "--catch-up",
                "all",
            ],
            &[
                "add",
                "n",
                "--every",
                "1m",
                "--class",
                "C",
                "--catch-up",
                "all:0",
            ],
            &[
                "add",
                "n",
                "--every",
                "1m",
                "--class",
                "C",
                "--unique-until",
                "start",
            ],
            &["next", "@daily", "--count", "0"],
            &["next", "@daily", "--after", "yesterday"],
            &["list", "extra"],
            &["explode"],
        ] {
            assert!(parse(refused).is_err(), "{refused:?}");
        }
        assert_eq!(parse_catch_up("latest"), Ok(CatchUp::Latest));
        assert_eq!(parse_catch_up(" skip "), Ok(CatchUp::Skip));
        assert_eq!(
            parse_catch_up("all:10000"),
            Ok(CatchUp::All { max: 10_000 })
        );
        assert!(parse_catch_up("all:10001").is_err());
        assert!(parse_catch_up("some").is_err());
    }

    #[test]
    fn spans_are_short() {
        assert_eq!(span(Duration::from_secs(45)), "45s");
        assert_eq!(span(Duration::from_secs(180)), "3m");
        assert_eq!(span(Duration::from_secs(7_500)), "2h5m");
        assert_eq!(span(Duration::from_secs(7_200)), "2h");
        assert_eq!(span(Duration::from_secs(3 * 86_400)), "3d");
        assert_eq!(
            catch_up_text(CatchUp::Skip, Some(Duration::from_secs(300))),
            "skip within 5m"
        );
    }
}
