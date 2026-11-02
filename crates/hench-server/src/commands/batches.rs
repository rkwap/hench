//! `hench batches`: the batches (list, show, cancel, delete, skip, dispatch), and `hench stats
//! batches` ([batches.md] §2.13).
//!
//! [batches.md]: https://github.com/rkwap/hench/blob/v0.1.0/docs/design/batches.md
//!
//! Descriptions, classes, errors and jids come from Redis (jids are whatever the pushing code
//! chose): they are printed with their control characters spelled out.
//!
//! ```text
//! hench batches [list] [--state active|dead|complete|succeeded|cancelled|all]
//!               [--substr TEXT] [--limit 50] [--json]
//! hench batches show <bid> [--json]
//! hench batches cancel <bid> --yes
//! hench batches delete <bid> [--force] --yes
//! hench batches skip <bid> <jid>
//! hench batches dispatch <bid>
//! hench stats batches [--json]
//! ```

use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::bail;
use clap::{Args, Subcommand, ValueEnum};
use hench::api::Api;
use hench::batch::{
    BatchError, BatchEvents, BatchFilter, BatchState, BatchStatus, BatchSummary, CallbackInfo,
    FailedMember,
};
use hench::metrics::Period;
use serde_json::{Value, json};

use crate::monitor::{datetime, printable, relative};

/// The most batches `hench batches` lists.
const MAX_LIST: u32 = 1_000;

/// Failures `hench batches show` lists.
const SHOWN_FAILURES: usize = 25;

/// Pending jobs `hench batches show` lists.
const SHOWN_PENDING: usize = 10;

/// Characters of a description in the list.
const DESCRIPTION_CHARS: usize = 60;

/// `hench batches`.
#[derive(Debug, Clone, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct BatchesArgs {
    /// What to do (default: list the batches not finished).
    #[command(subcommand)]
    pub command: Option<BatchesCommand>,
    /// The options of the list.
    #[command(flatten)]
    pub list: ListArgs,
}

/// The options of `hench batches [list]`.
#[derive(Debug, Clone, Args)]
pub struct ListArgs {
    /// Which batches: not finished (active), with a dead member, complete, succeeded,
    /// cancelled, or all of them.
    #[arg(long, value_enum, default_value_t = StateArg::Active)]
    pub state: StateArg,
    /// Only the batches whose bid or description contains TEXT; `*` stands for any run of
    /// characters.
    #[arg(long, value_name = "TEXT")]
    pub substr: Option<String>,
    /// How many batches to list (at most 1000).
    #[arg(long, default_value_t = 50, value_parser = clap::value_parser!(u32).range(1..=i64::from(MAX_LIST)))]
    pub limit: u32,
    /// Print JSON.
    #[arg(long)]
    pub json: bool,
}

/// The states `hench batches` lists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum StateArg {
    /// Not succeeded and not finished: still running, or waiting for an operator.
    Active,
    /// With a dead member.
    Dead,
    /// Whose `complete` fired.
    Complete,
    /// That succeeded.
    Succeeded,
    /// Cancelled.
    Cancelled,
    /// Every batch.
    All,
}

impl StateArg {
    fn filter(self) -> BatchFilter {
        match self {
            StateArg::Active => BatchFilter::Active,
            StateArg::Dead => BatchFilter::Dead,
            StateArg::Complete => BatchFilter::Complete,
            StateArg::Succeeded => BatchFilter::Succeeded,
            StateArg::Cancelled => BatchFilter::Cancelled,
            StateArg::All => BatchFilter::All,
        }
    }
}

/// The subcommands of `hench batches`.
#[derive(Debug, Clone, Subcommand)]
pub enum BatchesCommand {
    /// List batches (the default).
    List(ListArgs),
    /// Show one batch: its counts, events, callbacks, first failures and pending jobs.
    Show {
        /// The batch id (24 lowercase hexadecimal characters).
        bid: String,
        /// Print JSON.
        #[arg(long)]
        json: bool,
    },
    /// Cancel a batch and its child batches: jobs not yet run are skipped, `success` never
    /// fires, `complete` fires once every job left has run.
    Cancel {
        /// The batch id.
        bid: String,
        /// Confirm: without it, nothing is cancelled.
        #[arg(long)]
        yes: bool,
    },
    /// Delete the data of a batch and of its child batches (their jobs keep running).
    Delete {
        /// The batch id.
        bid: String,
        /// Delete an unfinished child of a live parent too (that parent can then never finish).
        #[arg(long)]
        force: bool,
        /// Confirm: without it, nothing is deleted.
        #[arg(long)]
        yes: bool,
    },
    /// Skip a pending job of a batch (it no longer blocks `success`), such as a dead one.
    Skip {
        /// The batch id.
        bid: String,
        /// The job's jid.
        jid: String,
    },
    /// Push the callbacks of the events a batch fired that wait to be pushed (maintenance does
    /// it after its grace period; this does it now).
    Dispatch {
        /// The batch id.
        bid: String,
    },
}

/// Runs `hench batches`.
pub async fn run<W: Write>(api: &Api, args: BatchesArgs, out: &mut W) -> anyhow::Result<()> {
    let batches = api.batches();
    match args.command {
        None => list(api, &args.list, out).await,
        Some(BatchesCommand::List(list_args)) => list(api, &list_args, out).await,
        Some(BatchesCommand::Show { bid, json }) => show(api, &bid, json, out).await,
        Some(BatchesCommand::Cancel { bid, yes }) => {
            check_bid(&bid)?;
            if !yes {
                bail!(
                    "cancelling batch {} skips its jobs not yet run and those of its child \
                     batches, for good: add --yes to confirm",
                    printable(&bid)
                );
            }
            let cancelled = batches.cancel(&bid).await.map_err(refusal)?;
            match cancelled {
                0 => writeln!(out, "Batch {bid} was already cancelled.")?,
                1 => writeln!(out, "Cancelled batch {bid}.")?,
                n => writeln!(out, "Cancelled batch {bid} and {} child batches.", n - 1)?,
            }
            Ok(())
        }
        Some(BatchesCommand::Delete { bid, force, yes }) => {
            check_bid(&bid)?;
            if !yes {
                bail!(
                    "deleting batch {} removes its data and that of its child batches (their \
                     jobs keep running): add --yes to confirm",
                    printable(&bid)
                );
            }
            if batches.delete(&bid, force).await.map_err(refusal)? {
                writeln!(out, "Deleted batch {bid}.")?;
            } else {
                writeln!(out, "Batch {bid} did not exist; nothing to delete.")?;
            }
            Ok(())
        }
        Some(BatchesCommand::Skip { bid, jid }) => {
            check_bid(&bid)?;
            if batches.skip(&bid, &jid).await.map_err(refusal)? {
                writeln!(out, "Skipped job {} of batch {bid}.", printable(&jid))?;
            } else {
                writeln!(
                    out,
                    "Job {} is not pending in batch {bid}; nothing to skip.",
                    printable(&jid)
                )?;
            }
            Ok(())
        }
        Some(BatchesCommand::Dispatch { bid }) => {
            check_bid(&bid)?;
            let dispatched = batches.dispatch(&bid).await.map_err(refusal)?;
            if dispatched == 0 {
                writeln!(out, "Batch {bid} had no callbacks waiting to be pushed.")?;
            } else {
                writeln!(out, "Pushed the waiting callbacks of {dispatched} batches.")?;
            }
            Ok(())
        }
    }
}

/// A refusal of the API in words (the other errors as they are).
fn refusal(error: hench::Error) -> anyhow::Error {
    match error {
        hench::Error::Batch(BatchError::NotFound { bid }) => anyhow::anyhow!(
            "batch {} does not exist (never did, expired, or was deleted)",
            printable(&bid)
        ),
        hench::Error::Batch(BatchError::Succeeded { bid }) => anyhow::anyhow!(
            "batch {} already succeeded: there is nothing left to cancel",
            printable(&bid)
        ),
        hench::Error::Batch(BatchError::DeleteChild { bid, parent }) => anyhow::anyhow!(
            "batch {} is an unfinished child of batch {}, which could then never finish: \
             delete or cancel the parent, or add --force",
            printable(&bid),
            printable(&parent)
        ),
        other => other.into(),
    }
}

/// Refuses a malformed batch id before asking Redis.
fn check_bid(bid: &str) -> anyhow::Result<()> {
    let valid = bid.len() == 24
        && bid
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'));
    if valid {
        Ok(())
    } else {
        bail!(
            "`{}` is not a batch id (24 lowercase hexadecimal characters, as `hench batches` \
             lists them)",
            printable(bid)
        )
    }
}

/// `hench batches [list]`.
async fn list<W: Write>(api: &Api, args: &ListArgs, out: &mut W) -> anyhow::Result<()> {
    let batches = api.batches();
    let size = usize::try_from(args.limit.min(MAX_LIST)).unwrap_or(50);
    let found = batches
        .search(&args.state.filter(), args.substr.as_deref(), 1, size)
        .await?;
    let events = batches.events(Period::Minutes(60)).await?;
    if args.json {
        let value = json!({
            "total": found.page.total,
            "searched": found.searched,
            "truncated": found.truncated,
            "batches": found.page.items.iter().map(summary_json).collect::<Vec<_>>(),
            "last_hour": events_json(&events),
        });
        serde_json::to_writer_pretty(&mut *out, &value)?;
        writeln!(out)?;
        return Ok(());
    }
    if found.page.items.is_empty() {
        let none = if args.substr.is_some() {
            "No batch matches the filter."
        } else if args.state == StateArg::All {
            "No batch exists, or every batch expired."
        } else {
            "No batch is in this state."
        };
        writeln!(out, "{none}")?;
    } else {
        let now = SystemTime::now();
        let rows: Vec<Vec<String>> = found
            .page
            .items
            .iter()
            .map(|summary| {
                vec![
                    summary.bid.clone(),
                    state_name(summary.state()).to_owned(),
                    summary.total.to_string(),
                    summary.pending.to_string(),
                    summary.failures.to_string(),
                    summary.dead.to_string(),
                    relative(summary.created_at, now),
                    cut(summary.description.as_deref().unwrap_or_default()),
                ]
            })
            .collect();
        out.write_all(
            table(
                &[
                    "BID",
                    "STATE",
                    "TOTAL",
                    "PENDING",
                    "FAILED",
                    "DEAD",
                    "CREATED",
                    "DESCRIPTION",
                ],
                &rows,
            )
            .as_bytes(),
        )?;
        let shown = u64::try_from(found.page.items.len()).unwrap_or(u64::MAX);
        if found.page.total > shown {
            writeln!(
                out,
                "({shown} of {}; --limit to see more)",
                found.page.total
            )?;
        }
        if found.truncated {
            writeln!(
                out,
                "(searched the {} most recently active batches; only the first 500 matches are \
                 listed)",
                found.searched
            )?;
        }
    }
    writeln!(out)?;
    writeln!(out, "{}", last_hour_line(&events))?;
    Ok(())
}

/// `hench batches show <bid>`.
async fn show<W: Write>(api: &Api, bid: &str, json: bool, out: &mut W) -> anyhow::Result<()> {
    check_bid(bid)?;
    let batches = api.batches();
    let Some(status) = batches.fetch(bid).await? else {
        bail!(
            "batch {} does not exist (never did, expired, or was deleted)",
            printable(bid)
        );
    };
    let failures = batches.failures(bid, 1, SHOWN_FAILURES).await?;
    let pending = batches.pending_jids(bid, SHOWN_PENDING).await?;
    if json {
        let mut value = status_json(&status);
        value["failures"] = failures.items.iter().map(failure_json).collect();
        value["failures_total"] = json!(failures.total);
        value["pending_jids"] = json!(pending);
        serde_json::to_writer_pretty(&mut *out, &value)?;
        writeln!(out)?;
        return Ok(());
    }
    let now = SystemTime::now();
    let when = |at: SystemTime| format!("{} ({})", rfc3339(at), relative(at, now));
    let mut rows: Vec<(&str, String)> = vec![
        ("Batch", status.bid.clone()),
        ("State", state_name(status.state()).to_owned()),
    ];
    if let Some(description) = &status.description {
        rows.push(("Description", printable(description).into_owned()));
    }
    if let Some(parent) = &status.parent {
        rows.push(("Parent", printable(parent).into_owned()));
    }
    rows.extend([
        ("Created", when(status.created_at)),
        (
            "Expires",
            status.expires_at.map_or_else(|| "-".to_owned(), when),
        ),
        ("Total", status.total.to_string()),
        ("Succeeded", status.succeeded().to_string()),
        ("Pending", status.pending.to_string()),
        ("Failures", status.failures.to_string()),
        ("Dead", status.dead.to_string()),
        ("Skipped", status.skipped.to_string()),
        (
            "Children",
            format!(
                "{} ({} complete, {} succeeded)",
                status.children, status.children_complete, status.children_succeeded
            ),
        ),
    ]);
    for (label, fired, pushed) in [
        (
            "Complete",
            status.complete_at,
            status.complete_dispatched_at,
        ),
        ("Success", status.success_at, status.success_dispatched_at),
        ("Death", status.death_at, status.death_dispatched_at),
    ] {
        if let Some(fired) = fired {
            let pushed = pushed.map_or_else(
                || " (callbacks not pushed yet)".to_owned(),
                |at| format!(" (callbacks pushed {})", relative(at, now)),
            );
            rows.push((label, format!("{}{pushed}", when(fired))));
        }
    }
    if let Some(at) = status.cancelled_at {
        rows.push(("Cancelled", when(at)));
    }
    if let Some(at) = status.finished_at {
        rows.push(("Finished", when(at)));
    }
    if let Some(stages) = status.workflow_stages {
        rows.push(("Workflow stages", stages.to_string()));
    }
    let width = rows.iter().map(|(label, _)| label.len()).max().unwrap_or(0) + 2;
    for (label, value) in &rows {
        writeln!(out, "{label:<width$}{value}")?;
    }
    if !status.callbacks.is_empty() {
        writeln!(out)?;
        let rows: Vec<Vec<String>> = status
            .callbacks
            .iter()
            .map(|callback| {
                vec![
                    callback.event.as_str().to_owned(),
                    printable(&callback.class).into_owned(),
                    printable(&callback.queue).into_owned(),
                    printable(&callback.jid).into_owned(),
                    if callback.dispatched { "pushed" } else { "-" }.to_owned(),
                ]
            })
            .collect();
        out.write_all(table(&["EVENT", "CALLBACK", "QUEUE", "JID", "PUSHED"], &rows).as_bytes())?;
    }
    if !failures.items.is_empty() {
        writeln!(out)?;
        let rows: Vec<Vec<String>> = failures
            .items
            .iter()
            .map(|failure| {
                vec![
                    printable(&failure.jid).into_owned(),
                    printable(&failure.class).into_owned(),
                    if failure.dead {
                        format!("dead ({})", printable(&failure.reason))
                    } else {
                        "retrying".to_owned()
                    },
                    cut(&format!(
                        "{}: {}",
                        failure.error_class.as_deref().unwrap_or("?"),
                        failure.error.as_deref().unwrap_or_default()
                    )),
                ]
            })
            .collect();
        out.write_all(table(&["FAILED", "CLASS", "STATE", "ERROR"], &rows).as_bytes())?;
        let shown = u64::try_from(failures.items.len()).unwrap_or(u64::MAX);
        if failures.total > shown {
            writeln!(out, "({shown} of {} failures)", failures.total)?;
        }
    }
    if !pending.is_empty() {
        writeln!(out)?;
        let pending: Vec<_> = pending.iter().map(|jid| printable(jid)).collect();
        writeln!(
            out,
            "Pending jobs (first {SHOWN_PENDING}): {}",
            pending.join(" ")
        )?;
    }
    Ok(())
}

/// Prints the `batches` section of `hench stats` (`json`: as JSON): the batches in the index,
/// those not finished and those with a dead member (counted among the 50 000 most recently
/// active), those whose callbacks wait to be pushed, and the last hour's events.
pub async fn stats<W: Write>(api: &Api, json: bool, out: &mut W) -> anyhow::Result<()> {
    let batches = api.batches();
    let size = batches.size().await?;
    let active = batches.search(&BatchFilter::Active, None, 1, 1).await?;
    let dead = batches.search(&BatchFilter::Dead, None, 1, 1).await?;
    let undispatched = batches.undispatched().await?;
    let events = batches.events(Period::Minutes(60)).await?;
    let count = |found: &hench::batch::BatchSearch| {
        if found.truncated {
            format!("{}+", found.page.total)
        } else {
            found.page.total.to_string()
        }
    };
    if json {
        let value = json!({
            "batches": size,
            "active": active.page.total,
            "active_truncated": active.truncated,
            "dead": dead.page.total,
            "dead_truncated": dead.truncated,
            "undispatched": undispatched,
            "last_hour": events_json(&events),
        });
        serde_json::to_writer_pretty(&mut *out, &value)?;
        writeln!(out)?;
        return Ok(());
    }
    writeln!(
        out,
        "Batches: {size} ({} active, {} with a dead job, {undispatched} with callbacks waiting \
         to be pushed)",
        count(&active),
        count(&dead)
    )?;
    writeln!(out, "{}", last_hour_line(&events))?;
    Ok(())
}

/// Whether `hench stats all` prints this section: when the index is not empty, or batches
/// were created in the last hour.
pub async fn has_data(api: &Api) -> anyhow::Result<bool> {
    let batches = api.batches();
    if batches.size().await? > 0 {
        return Ok(true);
    }
    let events = batches.events(Period::Minutes(60)).await?;
    Ok(events.created > 0)
}

/// `Last hour: 12 created, 9 complete, 8 succeeded, 1 with a death, 0 cancelled`.
fn last_hour_line(events: &BatchEvents) -> String {
    format!(
        "Last hour: {} created, {} complete, {} succeeded, {} with a death, {} cancelled",
        events.created, events.complete, events.success, events.death, events.cancelled
    )
}

/// The name of a state.
fn state_name(state: BatchState) -> &'static str {
    match state {
        BatchState::Succeeded => "succeeded",
        BatchState::Cancelled => "cancelled",
        BatchState::Dead => "dead",
        BatchState::Complete => "complete",
        _ => "pending",
    }
}

/// A batch of a list as JSON (the CLI's `--json` and the JSON API); times in seconds since the
/// Unix epoch.
pub(crate) fn summary_json(summary: &BatchSummary) -> Value {
    json!({
        "bid": summary.bid,
        "description": summary.description,
        "parent": summary.parent,
        "state": state_name(summary.state()),
        "total": summary.total,
        "succeeded": summary.succeeded(),
        "pending": summary.pending,
        "failures": summary.failures,
        "dead": summary.dead,
        "skipped": summary.skipped,
        "children": summary.children,
        "created_at": secs(summary.created_at),
        "complete_at": summary.complete_at.map(secs),
        "success_at": summary.success_at.map(secs),
        "death_at": summary.death_at.map(secs),
        "cancelled_at": summary.cancelled_at.map(secs),
        "finished_at": summary.finished_at.map(secs),
        "expires_at": summary.expires_at.map(secs),
        "undispatched": summary.undispatched,
    })
}

/// A batch's status as JSON (the CLI's `--json` and the JSON API).
pub(crate) fn status_json(status: &BatchStatus) -> Value {
    json!({
        "bid": status.bid,
        "description": status.description,
        "parent": status.parent,
        "state": state_name(status.state()),
        "total": status.total,
        "succeeded": status.succeeded(),
        "pending": status.pending,
        "failures": status.failures,
        "dead": status.dead,
        "skipped": status.skipped,
        "children": status.children,
        "children_complete": status.children_complete,
        "children_succeeded": status.children_succeeded,
        "created_at": secs(status.created_at),
        "complete_at": status.complete_at.map(secs),
        "success_at": status.success_at.map(secs),
        "death_at": status.death_at.map(secs),
        "complete_dispatched_at": status.complete_dispatched_at.map(secs),
        "success_dispatched_at": status.success_dispatched_at.map(secs),
        "death_dispatched_at": status.death_dispatched_at.map(secs),
        "cancelled_at": status.cancelled_at.map(secs),
        "finished_at": status.finished_at.map(secs),
        "expires_at": status.expires_at.map(secs),
        "ttl": status.ttl.as_secs(),
        "linger": status.linger.as_secs(),
        "duration": status.duration().map(|duration| duration.as_secs_f64()),
        "workflow_stages": status.workflow_stages,
        "undispatched": status.undispatched,
        "callbacks": status.callbacks.iter().map(callback_json).collect::<Vec<_>>(),
    })
}

fn callback_json(callback: &CallbackInfo) -> Value {
    json!({
        "event": callback.event.as_str(),
        "class": callback.class,
        "queue": callback.queue,
        "jid": callback.jid,
        "args": callback.args,
        "dispatched": callback.dispatched,
    })
}

/// A failed member as JSON.
pub(crate) fn failure_json(failure: &FailedMember) -> Value {
    json!({
        "jid": failure.jid,
        "class": failure.class,
        "error_class": failure.error_class,
        "error": failure.error,
        "dead": failure.dead,
        "reason": failure.reason,
        "retry_count": failure.retry_count,
        "failed_at": failure.failed_at.map(secs),
        "next_at": failure.next_at.map(secs),
    })
}

/// The last hour's events as JSON.
pub(crate) fn events_json(events: &BatchEvents) -> Value {
    json!({
        "created": events.created,
        "complete": events.complete,
        "success": events.success,
        "death": events.death,
        "cancelled": events.cancelled,
    })
}

/// Seconds since the Unix epoch (a float, like the rest of the JSON API).
fn secs(at: SystemTime) -> f64 {
    at.duration_since(UNIX_EPOCH)
        .map_or(0.0, |since| since.as_secs_f64())
}

/// `at` in RFC 3339 with milliseconds, or `?` beyond the calendar (times come from Redis).
fn rfc3339(at: SystemTime) -> String {
    datetime(at).map_or_else(
        || "?".to_owned(),
        |at| at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
    )
}

/// A text for a column: control characters spelled out, cut at 60 characters.
fn cut(text: &str) -> String {
    let text = printable(text);
    match text.char_indices().nth(DESCRIPTION_CHARS) {
        None => text.into_owned(),
        Some((at, _)) => format!("{}…", &text[..at]),
    }
}

/// A text table: the first column left-aligned, the last left-aligned too (free text), the
/// others right-aligned, each as wide as its widest cell plus two spaces.
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
                + 2
        })
        .collect();
    let last = headers.len().saturating_sub(1);
    let line = |cells: &mut dyn Iterator<Item = &str>| {
        let mut text = String::new();
        for (column, cell) in cells.enumerate() {
            let width = widths.get(column).copied().unwrap_or(0);
            if column == 0 {
                text.push_str(&format!("{cell:<width$}"));
            } else if column == last {
                text.push_str("  ");
                text.push_str(cell);
            } else {
                text.push_str(&format!("{cell:>width$}"));
            }
        }
        text.truncate(text.trim_end().len());
        text.push('\n');
        text
    };
    let mut out = line(&mut headers.iter().copied());
    for row in rows {
        out.push_str(&line(&mut row.iter().map(String::as_str)));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bids_are_checked_before_asking_redis() {
        assert!(check_bid("0123456789abcdef01234567").is_ok());
        let error = check_bid("nope\u{1b}").unwrap_err().to_string();
        assert!(error.contains("is not a batch id"), "{error}");
        assert!(error.contains("\\u{1b}"), "{error}");
    }

    #[test]
    fn texts_are_cut_and_spelled_out() {
        assert_eq!(cut("short"), "short");
        assert_eq!(cut("a\nb"), "a\\nb");
        let long = cut(&"x".repeat(100));
        assert_eq!(long.chars().count(), DESCRIPTION_CHARS + 1);
        assert!(long.ends_with('…'));
        let table = table(
            &["A", "B", "C"],
            &[vec!["x".into(), "10".into(), "free text".into()]],
        );
        assert_eq!(table, "A     B  C\nx    10  free text\n");
    }
}
