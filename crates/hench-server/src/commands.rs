//! The subcommands other than `web`: reports, signals and pushing jobs.

use std::io::Write;

use anyhow::{Context, bail};
use chrono::Utc;
use hench::api::Api;
use hench::limit::ThrottleOption;
use hench::{
    Client, JobPayload, OnConflict, PushOutcome, RedisConfig, RedisPool, RetryOption, Unique,
    UniqueUntil, now_secs_f64,
};
use serde_json::json;

use crate::cli::{Cli, Command, EnqueueArgs, Section, SignalArgs, StatsArgs};

// The commands of the features, one module each.
pub mod batches;
pub mod limits;
pub mod locks;
pub mod periodic;
use crate::connections;
use crate::json_api;
use crate::monitor::{self, Report};
use crate::server::{self, ServerConfig};

/// Runs a parsed command line, writing what the command prints to `out`.
pub async fn run<W: Write>(cli: Cli, out: &mut W) -> anyhow::Result<()> {
    let redis = redis_config(cli.redis_url.as_deref())?;
    match cli.command {
        Command::Web(args) => {
            let config = ServerConfig::from_args(&args)?;
            server::serve(config, redis, connections::shutdown_signals()).await
        }
        Command::Stats(args) => stats(&connect(redis).await?, &args, out).await,
        Command::Processes(args) => {
            let args = StatsArgs {
                section: Section::Processes,
                json: args.json,
            };
            stats(&connect(redis).await?, &args, out).await
        }
        Command::Queues(args) => {
            let args = StatsArgs {
                section: Section::Queues,
                json: args.json,
            };
            stats(&connect(redis).await?, &args, out).await
        }
        Command::Quiet(args) => signal(&connect(redis).await?, &args, false, out).await,
        Command::Stop(args) => signal(&connect(redis).await?, &args, true, out).await,
        Command::Enqueue(args) => enqueue(&connect(redis).await?, args, out).await,
        Command::Periodic(args) => match periodic::offline(&args, out) {
            // `hench periodic next` needs no Redis.
            Some(result) => result,
            None => periodic::run(&connect(redis).await?, args, out).await,
        },
        Command::Batches(args) => batches::run(&connect(redis).await?, args, out).await,
        Command::Limits(args) => limits::run(&connect(redis).await?, args, out).await,
        Command::Locks(args) => locks::run(&connect(redis).await?, args, out).await,
    }
}

/// The Redis configuration: `--redis-url` (or `HENCH_REDIS_URL`), else the environment like
/// workers (`REDIS_PROVIDER`, `REDIS_URL`), else the local default.
pub fn redis_config(url: Option<&str>) -> anyhow::Result<RedisConfig> {
    match url.map(str::trim).filter(|url| !url.is_empty()) {
        Some(url) => Ok(RedisConfig::new(url)),
        None => RedisConfig::from_env().context("invalid Redis environment"),
    }
}

/// Connects (and pings) for a one-shot command, which fails fast instead of waiting through
/// the reconnection attempts a long-running process makes.
async fn connect(redis: RedisConfig) -> anyhow::Result<Api> {
    let url = redis.redacted_url();
    let pool = RedisPool::from_config(redis.reconnect_retries(1))
        .await
        .with_context(|| format!("cannot reach Redis at {url}"))?;
    Ok(Api::new(pool))
}

/// `hench stats [SECTION] [--json]` (also `hench processes` and `hench queues`).
pub async fn stats<W: Write>(api: &Api, args: &StatsArgs, out: &mut W) -> anyhow::Result<()> {
    // The sections of the features come from their own commands.
    match args.section {
        Section::Periodic => return periodic::stats(api, args.json, out).await,
        Section::Batches => return batches::stats(api, args.json, out).await,
        Section::Limits => return limits::stats(api, args.json, out).await,
        Section::Locks => return locks::stats(api, args.json, out).await,
        _ => {}
    }
    let report = monitor::gather(api).await?;
    if args.json {
        let value = section_json(&report, args.section)?;
        serde_json::to_writer_pretty(&mut *out, &value)?;
        writeln!(out)?;
        return Ok(());
    }
    let now = Utc::now();
    let text = match args.section {
        Section::All => monitor::render_all(&report, now),
        Section::Version => monitor::render_version(&report.version, now),
        Section::Overview => monitor::render_overview(&report.overview),
        Section::Processes => monitor::render_processes(&report.processes),
        Section::Queues => monitor::render_queues(&report.queues),
        Section::Metrics => monitor::render_metrics(&report.metrics),
        // Printed by the features' commands (above).
        Section::Periodic | Section::Batches | Section::Limits | Section::Locks => String::new(),
    };
    out.write_all(text.as_bytes())?;
    if args.section == Section::All {
        // Then each feature's section, when the feature has data.
        if periodic::has_data(api).await? {
            periodic::stats(api, false, out).await?;
        }
        if batches::has_data(api).await? {
            batches::stats(api, false, out).await?;
        }
        if limits::has_data(api).await? {
            limits::stats(api, false, out).await?;
        }
        if locks::has_data(api).await? {
            locks::stats(api, false, out).await?;
        }
    }
    Ok(())
}

/// The JSON of one section of the report (the whole report for `all`).
fn section_json(report: &Report, section: Section) -> anyhow::Result<serde_json::Value> {
    Ok(match section {
        Section::All => serde_json::to_value(report)?,
        Section::Version => json!({"version": report.version, "time": report.time}),
        Section::Overview => serde_json::to_value(&report.overview)?,
        Section::Processes => serde_json::to_value(&report.processes)?,
        Section::Queues => serde_json::to_value(&report.queues)?,
        Section::Metrics => serde_json::to_value(&report.metrics)?,
        // Printed by the features' commands.
        Section::Periodic | Section::Batches | Section::Limits | Section::Locks => {
            serde_json::Value::Null
        }
    })
}

/// `hench quiet|stop <identity|all>`.
pub async fn signal<W: Write>(
    api: &Api,
    args: &SignalArgs,
    stop: bool,
    out: &mut W,
) -> anyhow::Result<()> {
    let verb = if stop { "stop" } else { "quiet down" };
    let processes = api.processes();
    if args.target == "all" {
        let mut signalled = Vec::new();
        let mut skipped = 0;
        for process in processes.list().await? {
            if process.is_embedded() {
                skipped += 1;
                continue;
            }
            if stop {
                process.stop().await?;
            } else {
                process.quiet().await?;
            }
            signalled.push(monitor::printable(process.identity()).into_owned());
        }
        if signalled.is_empty() {
            writeln!(out, "No process to {verb}.")?;
        } else {
            writeln!(
                out,
                "Asked {} to {verb}: {}",
                processes_word(signalled.len()),
                signalled.join(", ")
            )?;
        }
        if skipped > 0 {
            writeln!(
                out,
                "Left {} embedded in other applications alone; name them one by one.",
                processes_word(skipped)
            )?;
        }
        return Ok(());
    }
    let Some(process) = processes.get(&args.target).await? else {
        bail!(
            "no live process has the identity `{}` (see `hench processes`)",
            monitor::printable(&args.target)
        );
    };
    if stop {
        process.stop().await?;
    } else {
        process.quiet().await?;
    }
    writeln!(
        out,
        "Asked {} to {verb}.",
        monitor::printable(process.identity())
    )?;
    Ok(())
}

fn processes_word(count: usize) -> String {
    if count == 1 {
        "1 process".to_owned()
    } else {
        format!("{count} processes")
    }
}

/// `hench enqueue <class> [--args JSON] [--queue Q] [--in DELAY | --at TIME] [--retry R]`.
///
/// A raw push by class name: `hench` has no registry of the application's job types, so the
/// options a job type declares do not apply. Without `--queue` the job goes to `default`,
/// which is worth a warning: a job type declared for another queue would wait there for a
/// worker that never comes.
pub async fn enqueue<W: Write>(api: &Api, args: EnqueueArgs, out: &mut W) -> anyhow::Result<()> {
    let mut job = JobPayload::new(args.class, args.args.0);
    match args.queue {
        Some(queue) => job.queue = queue,
        None => tracing::warn!(
            class = %monitor::printable(&job.class),
            "no --queue: pushing to the `default` queue with the default options (hench does \
             not know the queue and options this job type declares; give --queue and --retry)"
        ),
    }
    if let Some(retry) = args.retry {
        job.retry = Some(serde_json::from_value::<RetryOption>(retry)?);
    }
    job.at = match (args.delay, args.at) {
        (Some(delay), _) => Some(now_secs_f64() + delay.as_secs_f64()),
        (None, Some(at)) => Some(at.timestamp_millis() as f64 / 1000.0),
        (None, None) => None,
    };
    if job.at.is_some_and(|at| at >= json_api::AT_LIMIT) {
        bail!("--in and --at must give a time before the year 10000");
    }
    if let Some(ttl) = args.unique_for {
        let mut unique = Unique::new(ttl);
        if let Some(until) = args.unique_until.as_deref().and_then(UniqueUntil::parse) {
            unique = unique.until(until);
        }
        if let Some(on_conflict) = args.on_conflict.as_deref().and_then(OnConflict::parse) {
            unique = unique.on_conflict(on_conflict);
        }
        if let Some(key) = args.unique_key {
            unique = unique.key(key);
        }
        if args.unique_across_queues {
            unique = unique.across_queues(true);
        }
        job.unique = Some(unique.into());
    }
    if let Some((name, key)) = args.throttle {
        job.throttle = Some(ThrottleOption::Enabled {
            name: Some(name),
            key,
        });
    }
    let at = job.at;
    let client = Client::new(api.redis().clone());
    let (jid, replaced) = match client.push_outcome(job).await? {
        PushOutcome::Pushed { jid } => (jid, None),
        PushOutcome::Replaced { jid, replaced } => (jid, Some(replaced)),
        // Not an error: a cron-style push expects duplicates while the previous job is pending
        // (`--on-conflict error` makes it one, failing the push).
        PushOutcome::Duplicate { holder, .. } => {
            if args.json {
                writeln!(out, "{}", json!({"jid": null, "duplicate_of": holder}))?;
            } else {
                writeln!(out, "duplicate of {holder}")?;
            }
            return Ok(());
        }
        _ => bail!("client middleware stopped the push"),
    };
    if args.json {
        let mut value = json!({"jid": jid, "at": at});
        if let Some(replaced) = replaced {
            value["replaced"] = json!(replaced);
        }
        writeln!(out, "{value}")?;
    } else {
        writeln!(out, "{jid}")?;
    }
    Ok(())
}
