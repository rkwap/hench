//! `hench locks`: the locks of unique jobs (`list`, the default; `show`; `release`;
//! `release-all`), and `hench stats locks`.
//!
//! ```text
//! hench locks [list] [--class C] [--limit 50] [--json]
//! hench locks show <digest> [--json]
//! hench locks release <digest> (--jid J | --force)
//! hench locks release-all [--class C] --yes      (also: release --all [--class C] --yes)
//! ```

use std::io::Write;
use std::time::SystemTime;

use anyhow::bail;
use clap::{Args, Subcommand};
use hench::api::{Api, SortOrder};
use hench::metrics::Period;
use hench::unique::{HolderPlace, UniqueEventCounts, UniqueEvents, UniqueLock, UniqueUntil};
use serde::Serialize;
use serde_json::{Value, json};

use crate::monitor::{delimit, printable, relative, rfc3339, time_ago, time_until};

/// Index entries a listing filtered by class reads at most (like the dashboard's searches).
pub(crate) const SCAN_LIMIT: u64 = 50_000;

/// Locks a listing prints at most.
pub(crate) const MAX_LIMIT: u32 = 10_000;

/// Index entries read per round trip by a listing.
const PAGE: usize = 1_000;

/// Classes `hench stats locks` lists at most.
const TOP_CLASSES: usize = 10;

/// `hench locks`.
#[derive(Debug, Clone, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct LocksArgs {
    /// What to do (default: list the locks).
    #[command(subcommand)]
    pub command: Option<LocksCommand>,

    /// The options of `list`, when no subcommand is given.
    #[command(flatten)]
    pub list: ListArgs,
}

/// The subcommands of `hench locks`.
#[derive(Debug, Clone, Subcommand)]
pub enum LocksCommand {
    /// List the locks, soonest expiry first, then the last hour's duplicates, replaced holders
    /// and lapses.
    List(ListArgs),
    /// Show one lock and where its holder is.
    Show(ShowArgs),
    /// Release one lock, if the job named with --jid still holds it (or whoever holds it, with
    /// --force), so that a duplicate can be pushed.
    Release(ReleaseArgs),
    /// Release every lock (of one job class with --class).
    ReleaseAll(ReleaseAllArgs),
}

/// `hench locks [list]`.
#[derive(Debug, Clone, Args)]
pub struct ListArgs {
    /// Only the locks of this job class.
    #[arg(long, value_name = "CLASS")]
    pub class: Option<String>,

    /// How many locks to print at most.
    #[arg(
        long,
        default_value_t = 50,
        value_parser = clap::value_parser!(u32).range(1..=i64::from(MAX_LIMIT)),
        value_name = "N"
    )]
    pub limit: u32,

    /// Print JSON instead of text.
    #[arg(long)]
    pub json: bool,
}

/// `hench locks show`.
#[derive(Debug, Clone, Args)]
pub struct ShowArgs {
    /// The lock's digest (as `hench locks` prints it; it may start with `-`).
    #[arg(allow_hyphen_values = true)]
    pub digest: String,

    /// Print JSON instead of text.
    #[arg(long)]
    pub json: bool,
}

/// `hench locks release`.
#[derive(Debug, Clone, Args)]
pub struct ReleaseArgs {
    /// The lock's digest (as `hench locks` prints it; it may start with `-`).
    #[arg(required_unless_present = "all", allow_hyphen_values = true)]
    pub digest: Option<String>,

    /// Release it only if this job holds it (the holder `hench locks` printed), so that a
    /// lock taken by another job meanwhile is left alone.
    #[arg(long, conflicts_with = "force", value_name = "JID")]
    pub jid: Option<String>,

    /// Release it whoever holds it.
    #[arg(long)]
    pub force: bool,

    /// Release every lock instead (like `release-all`).
    #[arg(long, conflicts_with_all = ["digest", "jid", "force"])]
    pub all: bool,

    /// With --all: only the locks of this job class.
    // The conflicts are spelled out: clap lets `requires = "all"` pass next to a digest,
    // because `--all` conflicts with the digest.
    #[arg(
        long,
        requires = "all",
        conflicts_with_all = ["digest", "jid", "force"],
        value_name = "CLASS"
    )]
    pub class: Option<String>,

    /// With --all: release them (without it, nothing is released).
    #[arg(long, requires = "all", conflicts_with_all = ["digest", "jid", "force"])]
    pub yes: bool,
}

/// `hench locks release-all`.
#[derive(Debug, Clone, Args)]
pub struct ReleaseAllArgs {
    /// Only the locks of this job class.
    #[arg(long, value_name = "CLASS")]
    pub class: Option<String>,

    /// Release them (without it, nothing is released).
    #[arg(long)]
    pub yes: bool,
}

/// Whether `hench stats all` prints this section: when some lock is held.
pub async fn has_data(api: &Api) -> anyhow::Result<bool> {
    Ok(api.unique_locks().size().await? > 0)
}

/// Runs `hench locks`.
pub async fn run<W: Write>(api: &Api, args: LocksArgs, out: &mut W) -> anyhow::Result<()> {
    match args.command {
        None => list(api, &args.list, out).await,
        Some(LocksCommand::List(list_args)) => list(api, &list_args, out).await,
        Some(LocksCommand::Show(show_args)) => show(api, &show_args, out).await,
        Some(LocksCommand::Release(release_args)) => match release_args.digest {
            Some(digest) if !release_args.all => {
                release(
                    api,
                    &digest,
                    release_args.jid.as_deref(),
                    release_args.force,
                    out,
                )
                .await
            }
            _ => {
                let all = ReleaseAllArgs {
                    class: release_args.class,
                    yes: release_args.yes,
                };
                release_all(api, &all, out).await
            }
        },
        Some(LocksCommand::ReleaseAll(all)) => release_all(api, &all, out).await,
    }
}

/// A lock as the JSON outputs show it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct LockInfo {
    /// The digest identifying the duplicates of the holder.
    pub(crate) digest: String,
    /// The holder's jid.
    pub(crate) jid: String,
    /// The holder's class.
    pub(crate) class: String,
    /// The holder's queue when it took the lock.
    pub(crate) queue: String,
    /// When the lock is released: `success`, `start` or `expired`.
    pub(crate) until: String,
    /// The holder's `unique_for`, in seconds.
    pub(crate) unique_for: u64,
    /// When the lock was taken (RFC 3339).
    pub(crate) locked_at: Option<String>,
    /// When the lock expires (RFC 3339).
    pub(crate) expires_at: Option<String>,
    /// When the holder runs, while it waits in the schedule (RFC 3339).
    pub(crate) scheduled_at: Option<String>,
}

impl From<&UniqueLock> for LockInfo {
    fn from(lock: &UniqueLock) -> Self {
        Self {
            digest: lock.digest.clone(),
            jid: lock.jid.clone(),
            class: lock.class.clone(),
            queue: lock.queue.clone(),
            until: lock.until.as_str().to_owned(),
            unique_for: lock.unique_for.as_secs(),
            locked_at: lock.locked_at.and_then(rfc3339),
            expires_at: lock.expires_at.and_then(rfc3339),
            scheduled_at: lock.scheduled_at.and_then(rfc3339),
        }
    }
}

/// The counters of unique jobs as the JSON outputs show them.
pub(crate) fn counts_json(counts: &UniqueEventCounts) -> Value {
    json!({
        "duplicates": counts.duplicates,
        "replaced": counts.replaced,
        "lapsed": counts.lapsed,
    })
}

/// A listing of locks: the first `limit` live locks (of `class`, if given), soonest expiry
/// first.
#[derive(Debug, Clone)]
pub(crate) struct Listing {
    /// The locks listed.
    pub(crate) locks: Vec<UniqueLock>,
    /// How many locks are live (whatever their class).
    pub(crate) size: u64,
    /// More locks (of the class) than listed may exist: the limit was reached, or a filtered
    /// listing stopped after [`SCAN_LIMIT`] index entries.
    pub(crate) truncated: bool,
}

/// The first `limit` live locks, soonest expiry first, of `class` if given (a filtered
/// listing reads at most [`SCAN_LIMIT`] index entries).
pub(crate) async fn listing(
    api: &Api,
    class: Option<&str>,
    limit: usize,
) -> hench::Result<Listing> {
    let locks = api.unique_locks();
    let mut found = Vec::new();
    let mut number = 1;
    loop {
        let size = if class.is_some() {
            PAGE
        } else {
            limit.min(PAGE)
        };
        let page = locks.page(number, size, SortOrder::Ascending).await?;
        let total = page.total;
        let read = u64::try_from(number.saturating_mul(size))
            .unwrap_or(u64::MAX)
            .min(total);
        for lock in page.items {
            if class.is_none_or(|class| lock.class == class) {
                if found.len() == limit {
                    return Ok(Listing {
                        locks: found,
                        size: total,
                        truncated: true,
                    });
                }
                found.push(lock);
            }
        }
        if read >= total {
            return Ok(Listing {
                locks: found,
                size: total,
                truncated: false,
            });
        }
        if found.len() == limit || read >= SCAN_LIMIT {
            return Ok(Listing {
                locks: found,
                size: total,
                truncated: true,
            });
        }
        number += 1;
    }
}

/// When a lock expires, relative to `now`.
fn expires(lock: &UniqueLock, now: SystemTime) -> String {
    match lock.expires_at {
        Some(at) => time_until(at.duration_since(now).unwrap_or_default().as_secs()),
        None => "never".to_owned(),
    }
}

/// A time with how long ago or until it is: `2026-11-01T12:00:00Z (5 minutes ago)`.
pub(crate) fn when(at: SystemTime, now: SystemTime) -> String {
    let stamp = rfc3339(at).unwrap_or_else(|| "?".to_owned());
    format!("{stamp} ({})", relative(at, now))
}

/// A lock's policy: `until success`.
fn policy(until: UniqueUntil) -> &'static str {
    match until {
        UniqueUntil::Start => "until start",
        UniqueUntil::Expired => "until expired",
        _ => "until success",
    }
}

/// The last hour's counters in a sentence.
fn last_hour(counts: &UniqueEventCounts) -> String {
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

/// A text table: every column left-aligned, as wide as its widest cell plus two spaces (the
/// last one unpadded).
pub(crate) fn table(headers: &[&str], rows: &[Vec<String>]) -> String {
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
        })
        .collect();
    let line = |cells: &mut dyn Iterator<Item = &str>| {
        let mut text = String::new();
        let cells: Vec<&str> = cells.collect();
        for (column, cell) in cells.iter().enumerate() {
            if column + 1 == cells.len() {
                text.push_str(cell);
            } else {
                let width = widths.get(column).copied().unwrap_or(0) + 2;
                text.push_str(&format!("{cell:<width$}"));
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

/// The text of a listing.
pub(crate) fn render_listing(
    listing: &Listing,
    class: Option<&str>,
    counts: &UniqueEventCounts,
    now: SystemTime,
) -> String {
    let mut out = match class {
        Some(class) => format!("---- Unique locks of {} ----\n", printable(class)),
        None => format!("---- Unique locks ({}) ----\n", delimit(listing.size)),
    };
    if listing.locks.is_empty() {
        out.push_str(match class {
            Some(_) if listing.truncated => {
                "No lock of this class among the first 50,000; the others were not searched.\n"
            }
            Some(_) => "No unique job of this class holds a lock.\n",
            None => "No unique job holds a lock.\n",
        });
    } else {
        let rows: Vec<Vec<String>> = listing
            .locks
            .iter()
            .map(|lock| {
                vec![
                    lock.digest.clone(),
                    printable(&lock.class).into_owned(),
                    printable(&lock.queue).into_owned(),
                    policy(lock.until).to_owned(),
                    printable(&lock.jid).into_owned(),
                    expires(lock, now),
                ]
            })
            .collect();
        out.push_str(&table(
            &["DIGEST", "CLASS", "QUEUE", "POLICY", "HOLDER", "EXPIRES"],
            &rows,
        ));
        if listing.truncated {
            out.push_str(&format!(
                "Showing the {} expiring soonest; more exist (use --limit or --class).\n",
                delimit(u64::try_from(listing.locks.len()).unwrap_or(u64::MAX))
            ));
        }
    }
    out.push('\n');
    out.push_str(&last_hour(counts));
    out.push('\n');
    out
}

/// The last hour's counters of `class`, or of every class.
fn counts_of(events: &UniqueEvents, class: Option<&str>) -> UniqueEventCounts {
    match class {
        Some(class) => events.by_class.get(class).copied().unwrap_or_default(),
        None => events.totals,
    }
}

/// `hench locks [list]`.
async fn list<W: Write>(api: &Api, args: &ListArgs, out: &mut W) -> anyhow::Result<()> {
    let class = args.class.as_deref();
    let limit = usize::try_from(args.limit).unwrap_or(usize::MAX);
    let listing = listing(api, class, limit).await?;
    let events = api.unique_locks().events(Period::Minutes(60)).await?;
    let counts = counts_of(&events, class);
    if args.json {
        let locks: Vec<LockInfo> = listing.locks.iter().map(LockInfo::from).collect();
        let value = json!({
            "size": listing.size,
            "locks": locks,
            "truncated": listing.truncated,
            "last_hour": counts_json(&counts),
        });
        serde_json::to_writer_pretty(&mut *out, &value)?;
        writeln!(out)?;
    } else {
        let text = render_listing(&listing, class, &counts, SystemTime::now());
        out.write_all(text.as_bytes())?;
    }
    Ok(())
}

/// Where the holder is, in words, with the place's JSON.
pub(crate) fn place(place: &HolderPlace, lock: &UniqueLock, now: SystemTime) -> (String, Value) {
    match place {
        HolderPlace::Running(work) => (
            format!(
                "Running on {}, started {}.",
                printable(work.process_id()),
                time_ago(
                    now.duration_since(work.run_at())
                        .unwrap_or_default()
                        .as_secs()
                )
            ),
            json!({"place": "running", "process": work.process_id(), "run_at": rfc3339(work.run_at())}),
        ),
        HolderPlace::Queued { record, position } => (
            format!(
                "Waiting in queue \"{}\", number {} in line.",
                printable(record.queue()),
                delimit(*position)
            ),
            json!({"place": "queued", "queue": record.queue(), "position": position}),
        ),
        HolderPlace::Scheduled(entry) => (
            format!("Scheduled to run {}.", when(entry.at(), now)),
            json!({"place": "scheduled", "at": rfc3339(entry.at())}),
        ),
        HolderPlace::Retrying(entry) => (
            format!("Waiting for a retry, {}.", when(entry.at(), now)),
            json!({"place": "retrying", "at": rfc3339(entry.at())}),
        ),
        HolderPlace::Dead(entry) => (
            format!("In the dead set since {}.", when(entry.at(), now)),
            json!({"place": "dead", "at": rfc3339(entry.at())}),
        ),
        HolderPlace::NotFound { complete, .. } => not_found(lock, *complete, now),
        _ => not_found(lock, false, now),
    }
}

fn not_found(lock: &UniqueLock, complete: bool, now: SystemTime) -> (String, Value) {
    let searched = if complete {
        "Not running, nor in its queue, the schedule, the retries or the dead set"
    } else {
        "Not found running, nor in the parts of its queue, the schedule, the retries and the \
         dead set searched (50,000 entries of each)"
    };
    let why = match lock.until {
        UniqueUntil::Expired => "a lock released when it expires stays after its job ran",
        _ => {
            "it may have been removed after its lock lapsed and was taken again, or moved meanwhile"
        }
    };
    (
        format!(
            "{searched}: {why}. The lock expires {}.",
            expires(lock, now)
        ),
        json!({"place": null, "complete": complete}),
    )
}

/// The text of `hench locks show`.
pub(crate) fn render_lock(lock: &UniqueLock, whereabouts: &str, now: SystemTime) -> String {
    let mut rows: Vec<(&str, String)> = vec![
        ("Class", printable(&lock.class).into_owned()),
        ("Queue", printable(&lock.queue).into_owned()),
        ("Holder", printable(&lock.jid).into_owned()),
        ("Policy", policy(lock.until).to_owned()),
        (
            "Unique for",
            format!("{} s", delimit(lock.unique_for.as_secs())),
        ),
    ];
    if let Some(at) = lock.locked_at {
        rows.push(("Locked", when(at, now)));
    }
    rows.push((
        "Expires",
        lock.expires_at
            .map_or_else(|| "never".to_owned(), |at| when(at, now)),
    ));
    if let Some(at) = lock.scheduled_at {
        rows.push(("Holder runs", when(at, now)));
    }
    rows.push(("Where", whereabouts.to_owned()));
    let mut out = format!("---- Lock {} ----\n", lock.digest);
    for (label, value) in rows {
        out.push_str(&format!("{label:>11}: {value}\n"));
    }
    out
}

/// `hench locks show`.
async fn show<W: Write>(api: &Api, args: &ShowArgs, out: &mut W) -> anyhow::Result<()> {
    let locks = api.unique_locks();
    let Some(lock) = locks.get(&args.digest).await? else {
        bail!(
            "no unique job holds the lock {} (it expired or was released)",
            printable(&args.digest)
        );
    };
    let found = locks.locate(&lock).await?;
    let now = SystemTime::now();
    let (whereabouts, place_json) = place(&found, &lock, now);
    if args.json {
        let mut value = serde_json::to_value(LockInfo::from(&lock))?;
        value["holder_place"] = place_json;
        value["fields"] = serde_json::to_value(&lock.fields)?;
        serde_json::to_writer_pretty(&mut *out, &value)?;
        writeln!(out)?;
    } else {
        out.write_all(render_lock(&lock, &whereabouts, now).as_bytes())?;
    }
    Ok(())
}

/// `hench locks release <digest> (--jid J | --force)`.
async fn release<W: Write>(
    api: &Api,
    digest: &str,
    jid: Option<&str>,
    force: bool,
    out: &mut W,
) -> anyhow::Result<()> {
    let locks = api.unique_locks();
    let shown = printable(digest);
    match (jid, force) {
        (Some(jid), _) => {
            if locks.release(digest, jid).await? {
                tracing::info!(digest, jid, "released a unique lock");
                writeln!(out, "Released the lock {shown} held by {}.", printable(jid))?;
                return Ok(());
            }
            match locks.get(digest).await? {
                Some(lock) => bail!(
                    "the lock {shown} is held by {}, not {}: nothing released",
                    printable(&lock.jid),
                    printable(jid)
                ),
                None => bail!("no unique job holds the lock {shown}"),
            }
        }
        (None, true) => {
            if locks.force_release(digest).await? {
                tracing::info!(digest, "released a unique lock (forced)");
                writeln!(out, "Released the lock {shown}.")?;
                Ok(())
            } else {
                bail!("no unique job holds the lock {shown}")
            }
        }
        (None, false) => bail!(
            "name the holder with --jid (as `hench locks` prints it), or release the lock \
             whoever holds it with --force"
        ),
    }
}

/// `hench locks release-all [--class C] --yes`.
async fn release_all<W: Write>(
    api: &Api,
    args: &ReleaseAllArgs,
    out: &mut W,
) -> anyhow::Result<()> {
    let class = args.class.as_deref();
    if !args.yes {
        let what = match class {
            Some(class) => format!("every lock of {}", printable(class)),
            None => "every unique lock".to_owned(),
        };
        bail!(
            "this releases {what} (duplicates of their jobs can then be pushed): run it again \
             with --yes"
        );
    }
    let released = api.unique_locks().release_all(class).await?;
    tracing::info!(released, class, "released unique locks");
    writeln!(
        out,
        "Released {} {}.",
        delimit(released),
        if released == 1 { "lock" } else { "locks" }
    )?;
    Ok(())
}

/// The text of `hench stats locks`.
pub(crate) fn render_stats(size: u64, events: &UniqueEvents) -> String {
    let mut out = String::from("---- Locks ----\n");
    out.push_str(&format!("{:>9}: {}\n", "Held", delimit(size)));
    out.push_str(&last_hour(&events.totals));
    out.push('\n');
    let mut classes: Vec<(&String, &UniqueEventCounts)> = events
        .by_class
        .iter()
        .filter(|(_, counts)| counts.duplicates + counts.replaced + counts.lapsed > 0)
        .collect();
    if classes.is_empty() {
        return out;
    }
    classes.sort_by(|(a_class, a), (b_class, b)| {
        b.duplicates
            .cmp(&a.duplicates)
            .then(b.lapsed.cmp(&a.lapsed))
            .then(b.replaced.cmp(&a.replaced))
            .then(a_class.cmp(b_class))
    });
    classes.truncate(TOP_CLASSES);
    let rows: Vec<Vec<String>> = classes
        .into_iter()
        .map(|(class, counts)| {
            vec![
                printable(class).into_owned(),
                delimit(counts.duplicates),
                delimit(counts.replaced),
                delimit(counts.lapsed),
            ]
        })
        .collect();
    out.push_str(&table(
        &["CLASS", "DUPLICATES", "REPLACED", "LAPSED"],
        &rows,
    ));
    out
}

/// Prints the `locks` section of `hench stats` (`json`: as JSON).
pub async fn stats<W: Write>(api: &Api, json: bool, out: &mut W) -> anyhow::Result<()> {
    let locks = api.unique_locks();
    let size = locks.size().await?;
    let events = locks.events(Period::Minutes(60)).await?;
    if json {
        let by_class: serde_json::Map<String, Value> = events
            .by_class
            .iter()
            .map(|(class, counts)| (class.clone(), counts_json(counts)))
            .collect();
        let value = json!({
            "size": size,
            "last_hour": counts_json(&events.totals),
            "last_hour_by_class": by_class,
        });
        serde_json::to_writer_pretty(&mut *out, &value)?;
        writeln!(out)?;
    } else {
        out.write_all(render_stats(size, &events).as_bytes())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    //! `hench locks` against a private Redis (`docs/design/unique.md` §3.5, S1), run past the
    //! dormancy switch.

    use std::time::Duration;

    use clap::Parser;
    use hench::test_util::TestRedis;
    use hench::{JobPayload, Unique};

    use super::*;
    use crate::cli::{Cli, Command};

    fn args(words: &[&str]) -> LocksArgs {
        let mut argv = vec!["hench", "locks"];
        argv.extend_from_slice(words);
        let Command::Locks(args) = Cli::try_parse_from(argv).unwrap().command else {
            panic!("not `hench locks`");
        };
        args
    }

    async fn locks(api: &Api, words: &[&str]) -> anyhow::Result<String> {
        let mut out = Vec::new();
        run(api, args(words), &mut out).await?;
        Ok(String::from_utf8(out).unwrap())
    }

    /// Pushes a unique job; its jid, or `None` for a duplicate.
    async fn push(
        api: &Api,
        class: &str,
        queue: &str,
        unique_for: u64,
        until: UniqueUntil,
    ) -> Option<String> {
        let mut job = JobPayload::new(class, vec![json!(1)]);
        job.queue = queue.to_owned();
        job.unique = Some(
            Unique::new(Duration::from_secs(unique_for))
                .until(until)
                .into(),
        );
        api.client().push(job).await.unwrap()
    }

    fn digest(class: &str, queue: &str) -> String {
        hench::unique::digest(class, Some(queue), &json!([1])).unwrap()
    }

    /// Three locks (expiring in 1, 10 and 20 minutes), and two duplicates of the first.
    async fn seeded(redis: &TestRedis) -> (Api, [String; 3]) {
        let api = Api::new(redis.pool().await);
        let sync = push(&api, "Sync", "default", 60, UniqueUntil::Success)
            .await
            .unwrap();
        let mailer = push(&api, "Mailer", "mail", 600, UniqueUntil::Start)
            .await
            .unwrap();
        let report = push(&api, "Report", "default", 1_200, UniqueUntil::Expired)
            .await
            .unwrap();
        for _ in 0..2 {
            assert_eq!(
                push(&api, "Sync", "default", 60, UniqueUntil::Success).await,
                None
            );
        }
        (api, [sync, mailer, report])
    }

    #[tokio::test]
    async fn the_list_prints_a_table_and_the_last_hour() {
        let redis = TestRedis::start().await;
        let (api, [sync, mailer, _]) = seeded(&redis).await;
        let text = locks(&api, &[]).await.unwrap();
        assert!(text.starts_with("---- Unique locks (3) ----\n"), "{text}");
        assert!(text.contains("DIGEST"), "{text}");
        let lines: Vec<&str> = text.lines().collect();
        let header = lines[1];
        for column in ["DIGEST", "CLASS", "QUEUE", "POLICY", "HOLDER", "EXPIRES"] {
            assert!(header.contains(column), "{header}");
        }
        assert!(lines[2].starts_with(&digest("Sync", "default")), "{text}");
        assert!(lines[2].contains(&sync) && lines[2].contains("until success"));
        assert!(lines[2].ends_with("in under a minute") || lines[2].ends_with("in a minute"));
        assert!(lines[3].contains("Mailer") && lines[3].contains("until start"));
        assert!(lines[3].contains(&mailer));
        assert!(lines[4].contains("Report") && lines[4].contains("until expired"));
        assert!(
            text.ends_with("\nLast hour: 2 duplicates not pushed, 0 replaced, 0 lapsed.\n"),
            "{text}"
        );
        // One class, a limit.
        let text = locks(&api, &["list", "--class", "Mailer"]).await.unwrap();
        assert!(
            text.starts_with("---- Unique locks of Mailer ----\n"),
            "{text}"
        );
        assert_eq!(
            text.lines().filter(|line| line.contains(&mailer)).count(),
            1
        );
        assert!(!text.contains("Sync"), "{text}");
        assert!(
            text.contains("Last hour: 0 duplicates not pushed"),
            "{text}"
        );
        let text = locks(&api, &["--limit", "2"]).await.unwrap();
        assert!(!text.contains("Report"), "{text}");
        assert!(
            text.contains("Showing the 2 expiring soonest; more exist"),
            "{text}"
        );
        // JSON.
        let text = locks(&api, &["--json"]).await.unwrap();
        let value: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value["size"], 3);
        assert_eq!(value["truncated"], false);
        assert_eq!(value["last_hour"]["duplicates"], 2);
        let first = &value["locks"][0];
        assert_eq!(first["class"], "Sync");
        assert_eq!(first["jid"], sync.as_str());
        assert_eq!(first["until"], "success");
        assert_eq!(first["unique_for"], 60);
        let expires = first["expires_at"].as_str().unwrap();
        assert!(
            chrono::DateTime::parse_from_rfc3339(expires).is_ok(),
            "{expires}"
        );
        let text = locks(&api, &["list", "--class", "Nope", "--json"])
            .await
            .unwrap();
        let value: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value["locks"], json!([]));
        let text = locks(&api, &["--class", "Nope"]).await.unwrap();
        assert!(
            text.contains("No unique job of this class holds a lock."),
            "{text}"
        );
    }

    #[tokio::test]
    async fn names_from_redis_are_spelled_out() {
        let redis = TestRedis::start().await;
        let api = Api::new(redis.pool().await);
        push(&api, "Evil\u{1b}[31m", "q\nx", 60, UniqueUntil::Success)
            .await
            .unwrap();
        let text = locks(&api, &[]).await.unwrap();
        assert!(!text.contains('\u{1b}'), "{text:?}");
        assert!(
            text.contains(r"Evil\u{1b}[31m") && text.contains(r"q\nx"),
            "{text}"
        );
        let text = locks(&api, &["show", &digest("Evil\u{1b}[31m", "q\nx")])
            .await
            .unwrap();
        assert!(!text.contains('\u{1b}'), "{text:?}");
    }

    #[tokio::test]
    async fn show_prints_every_field_and_where_the_holder_is() {
        let redis = TestRedis::start().await;
        let (api, [sync, ..]) = seeded(&redis).await;
        let sync_digest = digest("Sync", "default");
        let text = locks(&api, &["show", &sync_digest]).await.unwrap();
        assert!(
            text.starts_with(&format!("---- Lock {sync_digest} ----\n")),
            "{text}"
        );
        for line in [
            "      Class: Sync",
            "      Queue: default",
            &format!("     Holder: {sync}"),
            "     Policy: until success",
            " Unique for: 60 s",
            "      Where: Waiting in queue \"default\", number 1 in line.",
        ] {
            assert!(text.contains(line), "{line}: {text}");
        }
        let text = locks(&api, &["show", &sync_digest, "--json"])
            .await
            .unwrap();
        let value: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value["holder_place"]["place"], "queued");
        assert_eq!(value["holder_place"]["position"], 1);
        assert_eq!(value["fields"]["jid"], sync.as_str());
        // The holder gone: an `expired` lock stays after its job ran.
        let mut conn = api.redis().conn().await.unwrap();
        let () = hench::redis_rs::cmd("DEL")
            .arg(hench::keys::queue("default"))
            .query_async(&mut conn)
            .await
            .unwrap();
        let text = locks(&api, &["show", &digest("Report", "default")])
            .await
            .unwrap();
        assert!(
            text.contains("Where: Not running, nor in its queue, the schedule, the retries or the dead set: a lock released when it expires stays after its job ran."),
            "{text}"
        );
        let error = locks(&api, &["show", "unknown"]).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("no unique job holds the lock unknown"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn release_needs_the_holder_or_force() {
        let redis = TestRedis::start().await;
        let (api, [sync, mailer, _]) = seeded(&redis).await;
        let sync_digest = digest("Sync", "default");
        let error = locks(&api, &["release", &sync_digest]).await.unwrap_err();
        assert!(error.to_string().contains("--jid"), "{error}");
        let error = locks(
            &api,
            &["release", &sync_digest, "--jid", "0123456789abcdef01234567"],
        )
        .await
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains(&format!("is held by {sync}, not 0123456789abcdef01234567")),
            "{error}"
        );
        assert_eq!(api.unique_locks().size().await.unwrap(), 3);
        let text = locks(&api, &["release", &sync_digest, "--jid", &sync])
            .await
            .unwrap();
        assert_eq!(
            text,
            format!("Released the lock {sync_digest} held by {sync}.\n")
        );
        let error = locks(&api, &["release", &sync_digest, "--jid", &sync])
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("no unique job holds the lock"),
            "{error}"
        );
        let mailer_digest = digest("Mailer", "mail");
        let text = locks(&api, &["release", &mailer_digest, "--force"])
            .await
            .unwrap();
        assert_eq!(text, format!("Released the lock {mailer_digest}.\n"));
        let error = locks(&api, &["release", &mailer_digest, "--force"])
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("no unique job holds the lock"),
            "{error}"
        );
        assert_eq!(api.unique_locks().size().await.unwrap(), 1);
        drop(mailer);
    }

    #[tokio::test]
    async fn release_all_needs_yes() {
        let redis = TestRedis::start().await;
        let (api, _) = seeded(&redis).await;
        let error = locks(&api, &["release-all"]).await.unwrap_err();
        assert!(
            error.to_string().contains("run it again with --yes"),
            "{error}"
        );
        let error = locks(&api, &["release", "--all", "--class", "Mailer"])
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("every lock of Mailer"),
            "{error}"
        );
        assert_eq!(api.unique_locks().size().await.unwrap(), 3);
        let text = locks(&api, &["release-all", "--class", "Mailer", "--yes"])
            .await
            .unwrap();
        assert_eq!(text, "Released 1 lock.\n");
        let text = locks(&api, &["release", "--all", "--yes"]).await.unwrap();
        assert_eq!(text, "Released 2 locks.\n");
        assert_eq!(api.unique_locks().size().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn stats_prints_the_count_and_the_last_hour() {
        let redis = TestRedis::start().await;
        let (api, _) = seeded(&redis).await;
        let mut out = Vec::new();
        stats(&api, false, &mut out).await.unwrap();
        let text = String::from_utf8(out).unwrap();
        assert!(
            text.starts_with("---- Locks ----\n     Held: 3\n"),
            "{text}"
        );
        assert!(text.contains("Last hour: 2 duplicates not pushed, 0 replaced, 0 lapsed."));
        assert!(
            text.contains("CLASS  DUPLICATES  REPLACED  LAPSED\nSync   2           0         0\n"),
            "{text}"
        );
        let mut out = Vec::new();
        stats(&api, true, &mut out).await.unwrap();
        let value: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(value["size"], 3);
        assert_eq!(value["last_hour"]["duplicates"], 2);
        assert_eq!(value["last_hour_by_class"]["Sync"]["duplicates"], 2);
    }

    #[tokio::test]
    async fn stats_all_prints_the_section_while_a_lock_is_held() {
        let redis = TestRedis::start().await;
        assert!(!has_data(&Api::new(redis.pool().await)).await.unwrap());
        let (api, _) = seeded(&redis).await;
        assert!(has_data(&api).await.unwrap());
    }

    #[test]
    fn the_command_line_is_parsed() {
        let list = args(&[]);
        assert!(list.command.is_none());
        assert_eq!(list.list.limit, 50);
        let list = args(&["--class", "Sync", "--limit", "5", "--json"]);
        assert_eq!(list.list.class.as_deref(), Some("Sync"));
        assert!(list.list.json && list.list.limit == 5);
        assert!(matches!(
            args(&["list", "--json"]).command,
            Some(LocksCommand::List(ListArgs { json: true, .. }))
        ));
        let Some(LocksCommand::Release(release)) = args(&["release", "d", "--jid", "j"]).command
        else {
            panic!("not release");
        };
        assert_eq!(release.digest.as_deref(), Some("d"));
        assert_eq!(release.jid.as_deref(), Some("j"));
        let Some(LocksCommand::Release(all)) =
            args(&["release", "--all", "--class", "C", "--yes"]).command
        else {
            panic!("not release");
        };
        assert!(all.all && all.yes && all.digest.is_none());
        let Some(LocksCommand::ReleaseAll(all)) = args(&["release-all", "--yes"]).command else {
            panic!("not release-all");
        };
        assert!(all.yes && all.class.is_none());
        // Digests are base64url: they may start with `-` or `--`, before or after the options.
        let Some(LocksCommand::Show(show)) = args(&["show", "-Kx9", "--json"]).command else {
            panic!("not show");
        };
        assert_eq!(show.digest, "-Kx9");
        assert!(show.json);
        for words in [
            &["release", "--jid", "j", "--abc"][..],
            &["release", "--abc", "--jid", "j"],
        ] {
            let Some(LocksCommand::Release(release)) = args(words).command else {
                panic!("not release");
            };
            assert_eq!(release.digest.as_deref(), Some("--abc"), "{words:?}");
            assert_eq!(release.jid.as_deref(), Some("j"), "{words:?}");
        }
        let Some(LocksCommand::Release(forced)) = args(&["release", "-h4", "--force"]).command
        else {
            panic!("not release");
        };
        assert_eq!(forced.digest.as_deref(), Some("-h4"));
        assert!(forced.force);
        let parse = |words: &[&str]| {
            let mut argv = vec!["hench", "locks"];
            argv.extend_from_slice(words);
            Cli::try_parse_from(argv)
        };
        for refused in [
            &["release"][..],
            &["release", "d", "--jid", "j", "--force"],
            &["release", "d", "--all"],
            &["release", "d", "--yes"],
            &["release", "d", "--jid", "j", "--class", "C"],
            &["release", "--force", "--yes"],
            &["show"],
            &["--limit", "0"],
            &["--limit", "10001"],
            &["list", "extra"],
        ] {
            assert!(parse(refused).is_err(), "{refused:?}");
        }
    }

    #[test]
    fn times_to_come_read_naturally() {
        assert_eq!(time_until(0), "in under a minute");
        assert_eq!(time_until(90), "in a minute");
        assert_eq!(time_until(600), "in 10 minutes");
        assert_eq!(time_until(3_600), "in an hour");
        assert_eq!(time_until(7_200), "in 2 hours");
        assert_eq!(time_until(3 * 86_400), "in 3 days");
    }
}
