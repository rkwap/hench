//! `hench limits`: the limiters and throttles (list, show, reset, clear-line), and `hench stats
//! limits` ([ratelimit.md] §2.16).
//!
//! [ratelimit.md]: https://github.com/rkwap/hench/blob/v0.1.0/docs/design/ratelimit.md
//!
//! Names and keys come from Redis: they are printed with their control characters spelled out.
//!
//! ```text
//! hench limits [list] [--filter TEXT] [--sort last|name|refused] [--limit 50] [--json]
//! hench limits show <kind>:<id> [--json]
//! hench limits reset <kind>:<id> --yes
//! hench limits clear-line <kind>:<id>
//! hench stats limits [--json]
//! ```

use std::io::Write;
use std::time::{Duration, SystemTime};

use anyhow::{Context, bail};
use chrono::SecondsFormat;
use clap::{Args, Subcommand, ValueEnum};
use hench::api::Api;
use hench::limit::{Limit, LimitEvents, LimitInfo, LimitKind, LimitOrder};
use hench::metrics::Period;
use serde_json::{Value, json};

use crate::monitor::{datetime, printable, time_ago};

/// The most limits `hench limits` lists.
const MAX_LIST: u32 = 1_000;

/// Names `hench stats limits` lists.
const TOP_NAMES: usize = 10;

/// `hench limits`.
#[derive(Debug, Clone, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct LimitsArgs {
    /// What to do (default: list the limits in use).
    #[command(subcommand)]
    pub command: Option<LimitsCommand>,
    /// The options of the list.
    #[command(flatten)]
    pub list: ListArgs,
}

/// The options of `hench limits [list]`.
#[derive(Debug, Clone, Args)]
pub struct ListArgs {
    /// Only the limits whose id (`name[:key]`) contains TEXT; `*` stands for any run of
    /// characters.
    #[arg(long, value_name = "TEXT")]
    pub filter: Option<String>,
    /// The order: most recently used first, by name, or most refused first.
    #[arg(long, value_enum, default_value_t = SortArg::Last)]
    pub sort: SortArg,
    /// How many limits to list (at most 1000).
    #[arg(long, default_value_t = 50, value_parser = clap::value_parser!(u32).range(1..=i64::from(MAX_LIST)))]
    pub limit: u32,
    /// Print JSON.
    #[arg(long)]
    pub json: bool,
}

/// The orders of `hench limits`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum SortArg {
    /// Most recently used first.
    Last,
    /// By name and key.
    Name,
    /// Most refusals first.
    Refused,
}

/// The subcommands of `hench limits`.
#[derive(Debug, Clone, Subcommand)]
pub enum LimitsCommand {
    /// List the limits in use (the default).
    List(ListArgs),
    /// Show one limit: its use, counters, holders and wait line.
    Show {
        /// The limit: `<kind>:<id>`, kind `c` (concurrency), `w` (window), `b` (bucket) or `g`
        /// (leaky and points), id `name[:key]` (`w:stripe:42`).
        identity: String,
        /// Print JSON.
        #[arg(long)]
        json: bool,
    },
    /// Reset a limit: delete its state and counters (every slot is free at once; holders keep
    /// running).
    Reset {
        /// The limit (`<kind>:<id>`).
        identity: String,
        /// Confirm: without it, nothing is reset.
        #[arg(long)]
        yes: bool,
    },
    /// Clear the wait line of a limit (the next refused caller starts a new one).
    ClearLine {
        /// The limit (`<kind>:<id>`).
        identity: String,
    },
}

/// Runs `hench limits`.
pub async fn run<W: Write>(api: &Api, args: LimitsArgs, out: &mut W) -> anyhow::Result<()> {
    match args.command {
        None => list(api, &args.list, out).await,
        Some(LimitsCommand::List(list_args)) => list(api, &list_args, out).await,
        Some(LimitsCommand::Show { identity, json }) => show(api, &identity, json, out).await,
        Some(LimitsCommand::Reset { identity, yes }) => {
            let (kind, id) = parse_identity(&identity)?;
            if !yes {
                bail!(
                    "resetting `{}` frees every slot at once (holders keep running): add --yes to \
                     confirm",
                    printable(&identity)
                );
            }
            let existed = api.limits().reset(kind, id).await?;
            if existed {
                writeln!(out, "Reset {}.", printable(&identity))?;
            } else {
                writeln!(
                    out,
                    "{} was not in use; nothing to reset.",
                    printable(&identity)
                )?;
            }
            Ok(())
        }
        Some(LimitsCommand::ClearLine { identity }) => {
            let (kind, id) = parse_identity(&identity)?;
            if api.limits().clear_line(kind, id).await? {
                writeln!(out, "Cleared the wait line of {}.", printable(&identity))?;
            } else {
                writeln!(out, "{} had no wait line.", printable(&identity))?;
            }
            Ok(())
        }
    }
}

/// `hench limits [list]`.
async fn list<W: Write>(api: &Api, args: &ListArgs, out: &mut W) -> anyhow::Result<()> {
    let limits = api.limits();
    let order = match args.sort {
        SortArg::Last => LimitOrder::LastUsed,
        SortArg::Name => LimitOrder::Name,
        SortArg::Refused => LimitOrder::Refused,
    };
    let size = usize::try_from(args.limit.min(MAX_LIST)).unwrap_or(50);
    let page = limits
        .page_matching(1, size, order, args.filter.as_deref().unwrap_or_default())
        .await?;
    let events = limits.events(Period::Minutes(60)).await?;
    let now = SystemTime::now();
    if args.json {
        let value = json!({
            "total": page.total,
            "limits": page.items.iter().map(info_json).collect::<Vec<_>>(),
            "last_hour": events_json(&events),
        });
        serde_json::to_writer_pretty(&mut *out, &value)?;
        writeln!(out)?;
        return Ok(());
    }
    if page.items.is_empty() {
        let none = if args.filter.is_some() {
            "No limit matches the filter."
        } else {
            "No limiter or throttle has been used in the last day."
        };
        writeln!(out, "{none}")?;
    } else {
        let rows: Vec<Vec<String>> = page
            .items
            .iter()
            .map(|info| {
                vec![
                    printable(&info.identity()).into_owned(),
                    kind_name(info).to_owned(),
                    spec_text(info.spec.as_ref()),
                    used_text(info),
                    info.next_free_in.map_or_else(|| "?".to_owned(), wait_text),
                    info.granted.to_string(),
                    info.refused.to_string(),
                    info.last_used
                        .map_or_else(|| "-".to_owned(), |at| ago(at, now)),
                ]
            })
            .collect();
        out.write_all(
            table(
                &[
                    "LIMIT",
                    "KIND",
                    "SPEC",
                    "USED",
                    "NEXT-FREE",
                    "GRANTED",
                    "REFUSED",
                    "LAST-USED",
                ],
                &rows,
            )
            .as_bytes(),
        )?;
        if page.total > u64::try_from(page.items.len()).unwrap_or(u64::MAX) {
            writeln!(
                out,
                "({} of {}; --limit to see more)",
                page.items.len(),
                page.total
            )?;
        }
    }
    writeln!(out)?;
    write_last_hour(&events, usize::MAX, out)
}

/// `hench limits show <identity>`.
async fn show<W: Write>(api: &Api, identity: &str, json: bool, out: &mut W) -> anyhow::Result<()> {
    let (kind, id) = parse_identity(identity)?;
    let limits = api.limits();
    let Some(info) = limits.get(kind, id).await? else {
        bail!(
            "the limit `{}` is not in use: its counters expired, or it was reset (see `hench \
             limits`)",
            printable(identity)
        );
    };
    let holders = if kind == LimitKind::Concurrency {
        limits.holders(&info.id).await?
    } else {
        Vec::new()
    };
    if json {
        let mut value = info_json(&info);
        if kind == LimitKind::Concurrency {
            value["holders"] = holders
                .iter()
                .map(|holder| {
                    json!({
                        "token": holder.token,
                    "jid": holder.jid,
                        "lease_ends_at": rfc3339(holder.lease_ends_at),
                        "ended": holder.ended,
                    })
                })
                .collect();
        }
        serde_json::to_writer_pretty(&mut *out, &value)?;
        writeln!(out)?;
        return Ok(());
    }
    let now = SystemTime::now();
    let mut rows: Vec<(&str, String)> = vec![
        ("Identity", printable(&info.identity()).into_owned()),
        ("Kind", kind_name(&info).to_owned()),
        ("Spec", spec_text(info.spec.as_ref())),
        ("Used", used_text(&info)),
        (
            "Next free",
            info.next_free_in
                .map_or_else(|| "unknown".to_owned(), wait_text),
        ),
        ("Granted", info.granted.to_string()),
        ("Refused", info.refused.to_string()),
        (
            "Waited",
            format!("{} (average {})", info.waited, average(info.average_wait())),
        ),
    ];
    if kind == LimitKind::Concurrency {
        rows.extend([
            (
                "Releases",
                format!(
                    "{} (average hold {})",
                    info.releases,
                    average(info.average_hold())
                ),
            ),
            ("Reclaimed", info.reclaimed.to_string()),
            ("Overages", info.overages.to_string()),
            ("Restored", info.restored.to_string()),
        ]);
    }
    if matches!(info.spec, Some(Limit::Points { .. })) || info.adjustments > 0 {
        rows.push(("Corrections", info.adjustments.to_string()));
    }
    rows.push((
        "Wait line",
        info.line_ends_at.map_or_else(
            || "none".to_owned(),
            |ends| match ends.duration_since(now) {
                Ok(wait) if !wait.is_zero() => format!("clears in {}", wait_text(wait)),
                _ => "clears now".to_owned(),
            },
        ),
    ));
    rows.push((
        "Last used",
        info.last_used.map_or_else(
            || "never".to_owned(),
            |at| format!("{} ({})", rfc3339(at), ago(at, now)),
        ),
    ));
    let width = rows.iter().map(|(label, _)| label.len()).max().unwrap_or(0) + 2;
    for (label, value) in &rows {
        writeln!(out, "{label:<width$}{value}")?;
    }
    if kind == LimitKind::Concurrency {
        writeln!(out)?;
        if holders.is_empty() {
            writeln!(out, "No holders.")?;
        } else {
            let rows: Vec<Vec<String>> = holders
                .iter()
                .map(|holder| {
                    let ends = if holder.ended {
                        format!("{} (ended)", rfc3339(holder.lease_ends_at))
                    } else {
                        rfc3339(holder.lease_ends_at)
                    };
                    vec![printable(&holder.token).into_owned(), ends]
                })
                .collect();
            out.write_all(table(&["HOLDER", "LEASE-ENDS"], &rows).as_bytes())?;
        }
    }
    Ok(())
}

/// Prints the `limits` section of `hench stats` (`json`: as JSON): the last hour's grants and
/// refusals, the names with the most refusals first.
pub async fn stats<W: Write>(api: &Api, json: bool, out: &mut W) -> anyhow::Result<()> {
    let limits = api.limits();
    let in_use = limits.size().await?;
    let events = limits.events(Period::Minutes(60)).await?;
    if json {
        let mut value = events_json(&events);
        value["in_use"] = json!(in_use);
        serde_json::to_writer_pretty(&mut *out, &value)?;
        writeln!(out)?;
        return Ok(());
    }
    writeln!(out, "Limits: {in_use} in use")?;
    write_last_hour(&events, TOP_NAMES, out)
}

/// Whether `hench stats all` prints this section: when limits are in use or were in the last
/// hour.
pub async fn has_data(api: &Api) -> anyhow::Result<bool> {
    let limits = api.limits();
    if limits.size().await? > 0 {
        return Ok(true);
    }
    let events = limits.events(Period::Minutes(60)).await?;
    Ok(events.totals != (0, 0))
}

/// The last hour's totals, then up to `top` names, most refused first.
fn write_last_hour<W: Write>(events: &LimitEvents, top: usize, out: &mut W) -> anyhow::Result<()> {
    let (granted, refused) = events.totals;
    writeln!(out, "Last hour: {granted} granted, {refused} refused")?;
    let mut names: Vec<(String, u64, u64)> = events
        .by_name
        .keys()
        .map(|name| {
            let (granted, refused) = events.totals_of(name);
            (name.clone(), granted, refused)
        })
        .collect();
    names.sort_by(|a, b| {
        b.2.cmp(&a.2)
            .then(b.1.cmp(&a.1))
            .then_with(|| a.0.cmp(&b.0))
    });
    names.truncate(top);
    if !names.is_empty() {
        let rows: Vec<Vec<String>> = names
            .into_iter()
            .map(|(name, granted, refused)| {
                vec![
                    printable(&name).into_owned(),
                    granted.to_string(),
                    refused.to_string(),
                ]
            })
            .collect();
        out.write_all(table(&["NAME", "GRANTED", "REFUSED"], &rows).as_bytes())?;
    }
    Ok(())
}

/// The kind and id of `<kind>:<id>`.
pub(crate) fn parse_identity(identity: &str) -> anyhow::Result<(LimitKind, &str)> {
    let parsed = identity.split_once(':').and_then(|(letter, id)| {
        let mut letters = letter.chars();
        let kind = LimitKind::from_letter(letters.next()?)?;
        (letters.next().is_none() && !id.is_empty()).then_some((kind, id))
    });
    parsed.with_context(|| {
        format!(
            "`{}` is not a limit: write `<kind>:<id>`, kind c (concurrency), w (window), b \
             (bucket) or g (leaky and points), as `hench limits` lists them (`w:stripe:42`)",
            printable(identity)
        )
    })
}

/// A limit as JSON (the CLI's `--json` and the JSON API).
pub(crate) fn info_json(info: &LimitInfo) -> Value {
    json!({
        "identity": info.identity(),
        "kind": kind_name(info),
        "id": info.id,
        "name": info.name,
        "key": info.key,
        "queue": info.queue,
        "spec": info.spec.as_ref().map(ToString::to_string),
        "capacity": info.capacity(),
        "used": info.used,
        "next_free_in_ms": info.next_free_in.map(millis),
        "granted": info.granted,
        "refused": info.refused,
        "waited": info.waited,
        "wait_time_ms": millis(info.wait_time),
        "releases": info.releases,
        "hold_time_ms": millis(info.hold_time),
        "reclaimed": info.reclaimed,
        "overages": info.overages,
        "restored": info.restored,
        "adjustments": info.adjustments,
        "line_ends_at": info.line_ends_at.map(rfc3339),
        "last_used": info.last_used.map(rfc3339),
    })
}

/// The last hour's grants and refusals as JSON.
pub(crate) fn events_json(events: &LimitEvents) -> Value {
    let by_name: serde_json::Map<String, Value> = events
        .by_name
        .keys()
        .map(|name| {
            let (granted, refused) = events.totals_of(name);
            (
                name.clone(),
                json!({"granted": granted, "refused": refused}),
            )
        })
        .collect();
    json!({
        "granted": events.totals.0,
        "refused": events.totals.1,
        "by_name": by_name,
    })
}

/// The kind's name (`points` for the points budgets of the leaky kind).
fn kind_name(info: &LimitInfo) -> &'static str {
    match info.kind {
        LimitKind::Concurrency => "concurrency",
        LimitKind::Window => "window",
        LimitKind::Bucket => "bucket",
        _ if matches!(info.spec, Some(Limit::Points { .. })) => "points",
        _ => "leaky",
    }
}

fn spec_text(spec: Option<&Limit>) -> String {
    spec.map_or_else(|| "?".to_owned(), ToString::to_string)
}

/// `37/50`, `2.5/60`, `3` (unknown capacity).
fn used_text(info: &LimitInfo) -> String {
    let used = if info.used.fract() == 0.0 {
        format!("{:.0}", info.used)
    } else {
        format!("{:.1}", info.used)
    };
    match info.capacity() {
        Some(capacity) => format!("{used}/{capacity}"),
        None => used,
    }
}

/// `now`, `250ms`, `4s`, `3m`, `1h5m`.
fn wait_text(wait: Duration) -> String {
    let secs = wait.as_secs();
    if wait.is_zero() {
        "now".to_owned()
    } else if secs == 0 {
        format!("{}ms", wait.as_millis())
    } else if secs < 60 {
        format!("{secs}s")
    } else if secs < 3_600 {
        format!("{}m", secs / 60)
    } else {
        format!("{}h{}m", secs / 3_600, secs % 3_600 / 60)
    }
}

fn average(duration: Option<Duration>) -> String {
    duration.map_or_else(
        || "-".to_owned(),
        |duration| format!("{}ms", millis(duration)),
    )
}

fn ago(at: SystemTime, now: SystemTime) -> String {
    time_ago(now.duration_since(at).map_or(0, |ago| ago.as_secs()))
}

/// `at` in RFC 3339 with milliseconds, or `?` beyond the calendar (lease ends and last uses
/// come from Redis).
fn rfc3339(at: SystemTime) -> String {
    datetime(at).map_or_else(
        || "?".to_owned(),
        |at| at.to_rfc3339_opts(SecondsFormat::Millis, true),
    )
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// A text table: the first column left-aligned, the others right-aligned, each as wide as its
/// widest cell plus two spaces.
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
    fn identities_are_parsed() {
        assert_eq!(
            parse_identity("w:stripe:42").unwrap(),
            (LimitKind::Window, "stripe:42")
        );
        assert_eq!(
            parse_identity("c:erp").unwrap(),
            (LimitKind::Concurrency, "erp")
        );
        for bad in ["stripe", "x:erp", "cw:erp", "w:", ":erp", "w:\n"] {
            if bad == "w:\n" {
                // Any non-empty id is an id (keys may hold anything).
                assert!(parse_identity(bad).is_ok());
                continue;
            }
            let error = parse_identity(bad).unwrap_err().to_string();
            assert!(error.contains("is not a limit"), "{bad}: {error}");
        }
        let error = parse_identity("\u{1b}[31m").unwrap_err().to_string();
        assert!(error.contains("\\u{1b}"), "{error}");
    }

    #[test]
    fn values_read_well() {
        assert_eq!(wait_text(Duration::ZERO), "now");
        assert_eq!(wait_text(Duration::from_millis(40)), "40ms");
        assert_eq!(wait_text(Duration::from_secs(42)), "42s");
        assert_eq!(wait_text(Duration::from_secs(150)), "2m");
        assert_eq!(wait_text(Duration::from_secs(3_900)), "1h5m");
        assert_eq!(
            table(&["A", "B"], &[vec!["x".into(), "10".into()]]),
            "A     B\nx    10\n"
        );
    }
}
