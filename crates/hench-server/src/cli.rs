//! The command line: `hench <command>`, parsed with clap.

use std::net::SocketAddr;
use std::time::Duration;

use chrono::{DateTime, Utc};
use clap::{Args, Parser, Subcommand, ValueEnum};
use serde_json::Value;

/// The `hench` command line.
#[derive(Debug, Parser)]
#[command(
    name = "hench",
    version,
    about = "Dashboard, JSON API and command-line tools for hench background jobs",
    long_about = None,
)]
pub struct Cli {
    /// The Redis URL. Without it: $HENCH_REDIS_URL, else $REDIS_PROVIDER (the name of the
    /// variable holding the URL) or $REDIS_URL, else redis://127.0.0.1:6379.
    #[arg(
        long,
        global = true,
        env = "HENCH_REDIS_URL",
        hide_env_values = true,
        value_name = "URL"
    )]
    pub redis_url: Option<String>,

    /// What to do.
    #[command(subcommand)]
    pub command: Command,
}

/// The subcommands.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Serve the web dashboard (and, with --api, a JSON API).
    Web(WebArgs),
    /// Print an overview of the cluster: version, totals, processes, queues, busiest job
    /// classes, then periodic schedules, batches, limits and locks when in use.
    Stats(StatsArgs),
    /// List the live processes.
    Processes(OutputArgs),
    /// List the queues with their size, latency and limits; `hench queues limit` sets or
    /// clears the limits of a queue.
    Queues(QueuesArgs),
    /// Ask a process (by identity) or every process (`all`) to stop taking new jobs.
    Quiet(SignalArgs),
    /// Ask a process (by identity) or every process (`all`) to shut down.
    Stop(SignalArgs),
    /// Push a job by class name.
    ///
    /// `hench` does not know the job types of your application, so the options a job type
    /// declares (`#[hench::job(queue = .., retry = ..)]`, `Job::options`) do not apply: the job
    /// goes to the queue `default` with the default options (the workers' `max_retries`
    /// retries, 25 unless configured, then the dead set) unless --queue and --retry say
    /// otherwise. Give the queue and the retries the job needs.
    Enqueue(EnqueueArgs),
    /// The periodic schedules (list, show, enqueue, pause, unpause, remove, add, repair, next).
    Periodic(crate::commands::periodic::PeriodicArgs),
    /// The batches (list, show, cancel, delete, skip, dispatch).
    Batches(crate::commands::batches::BatchesArgs),
    /// The limiters and throttles (list, show, reset, clear-line).
    Limits(crate::commands::limits::LimitsArgs),
    /// The locks of unique jobs (list, show, release, release-all).
    Locks(crate::commands::locks::LocksArgs),
}

/// `hench web`.
#[derive(Debug, Clone, Args)]
pub struct WebArgs {
    /// The address to listen on.
    #[arg(long, env = "HENCH_WEB_BIND", default_value = "127.0.0.1:7420")]
    pub bind: SocketAddr,

    /// Require HTTP Basic authentication with this user name (with --password).
    #[arg(long, env = "HENCH_WEB_USERNAME")]
    pub username: Option<String>,

    /// Require HTTP Basic authentication with this password (with --username).
    #[arg(long, env = "HENCH_WEB_PASSWORD", hide_env_values = true)]
    pub password: Option<String>,

    /// Hide every action and refuse every change.
    #[arg(long, env = "HENCH_WEB_READ_ONLY")]
    pub read_only: bool,

    /// Serve everything under this path (for a reverse proxy that does not strip it),
    /// e.g. /hench.
    #[arg(long, env = "HENCH_WEB_PREFIX", default_value = "", value_parser = parse_prefix)]
    pub prefix: String,

    /// Also serve the JSON API under PREFIX/api/v1: stats, queues, processes, POST /jobs to
    /// push jobs, and the periodic schedules, batches, limits and locks, which it can change too
    /// (unless --read-only). Needs credentials unless listening on a loopback address.
    #[arg(long, env = "HENCH_WEB_API")]
    pub api: bool,

    /// Another host name to answer for (repeatable, or comma-separated): the name a proxy
    /// forwards in `Host`, `.example.com` for a domain and its subdomains, or `*` for every
    /// name. Without credentials only localhost and IP addresses are served otherwise (a
    /// defense against DNS rebinding); with credentials, listing names restricts the server to
    /// them.
    #[arg(
        long = "allowed-host",
        env = "HENCH_WEB_ALLOWED_HOSTS",
        value_delimiter = ',',
        value_parser = crate::hosts::parse_allowed_host,
        value_name = "HOST"
    )]
    pub allowed_hosts: Vec<String>,

    /// The product name in the dashboard's header.
    #[arg(long, default_value = "Hench")]
    pub title: String,

    /// How often live views refresh by default (e.g. 5s).
    #[arg(long, default_value = "5s", value_parser = parse_duration)]
    pub poll_interval: Duration,

    /// How many connections to serve at once; more clients wait until one closes. Keep it
    /// well below the limit of open files (`ulimit -n`, often 1024).
    #[arg(
        long,
        env = "HENCH_WEB_MAX_CONNECTIONS",
        default_value_t = crate::connections::Limits::DEFAULT.max_connections as u32,
        value_parser = clap::value_parser!(u32).range(1..=1_000_000),
        value_name = "N"
    )]
    pub max_connections: u32,
}

/// `hench stats`.
#[derive(Debug, Clone, Args)]
pub struct StatsArgs {
    /// Which part of the overview to print.
    #[arg(value_enum, default_value_t = Section::All)]
    pub section: Section,

    /// Print JSON instead of text.
    #[arg(long)]
    pub json: bool,
}

/// The parts of `hench stats`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Section {
    /// Everything.
    All,
    /// The hench version and the time.
    Version,
    /// The totals.
    Overview,
    /// The processes.
    Processes,
    /// The queues.
    Queues,
    /// The busiest job classes of the last hour (from the workers' metrics).
    Metrics,
    /// The periodic schedules.
    Periodic,
    /// The batches.
    Batches,
    /// The limiters and throttles.
    Limits,
    /// The locks of unique jobs.
    Locks,
}

/// Output options of the listing commands.
#[derive(Debug, Clone, Args)]
pub struct OutputArgs {
    /// Print JSON instead of text.
    #[arg(long)]
    pub json: bool,
}

/// `hench queues`.
#[derive(Debug, Clone, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct QueuesArgs {
    /// What to do (default: list the queues).
    #[command(subcommand)]
    pub command: Option<QueuesCommand>,
    /// The options of the list.
    #[command(flatten)]
    pub list: OutputArgs,
}

/// The subcommands of `hench queues`.
#[derive(Debug, Clone, Subcommand)]
pub enum QueuesCommand {
    /// Set or clear the limits on every fetch of a queue: a queue over its limits is skipped
    /// like a paused one, its jobs keep their order, and the other queues keep flowing. Every
    /// worker applies new limits within a heartbeat interval (10 s by default); a worker that
    /// starts with limits for the queue in its configuration writes its own back.
    Limit(QueueLimitArgs),
}

/// `hench queues limit`.
#[derive(Debug, Clone, Args)]
#[command(group(clap::ArgGroup::new("change").required(true).args(["set", "clear"])))]
pub struct QueueLimitArgs {
    /// The queue.
    pub queue: String,
    /// The limits, joined by `;`: concurrency, window, bucket and leaky limits, at most one of
    /// each kind ("concurrency=3; window=100/1m").
    #[arg(long, value_name = "SPECS")]
    pub set: Option<String>,
    /// Remove the queue's limits.
    #[arg(long)]
    pub clear: bool,
    /// Print JSON.
    #[arg(long)]
    pub json: bool,
}

/// `hench quiet` and `hench stop`.
#[derive(Debug, Clone, Args)]
pub struct SignalArgs {
    /// A process identity (`hostname:pid:nonce`, as `hench processes` lists them), or `all`
    /// for every process that is not embedded in another application.
    pub target: String,
}

/// `hench enqueue`.
#[derive(Debug, Clone, Args)]
pub struct EnqueueArgs {
    /// The job's class: the name its job type is registered under.
    pub class: String,

    /// The arguments, as a JSON array (a single other JSON value is one argument).
    #[arg(long, default_value = "[]", value_parser = parse_args)]
    pub args: JobArgs,

    /// The queue (default: "default"; the queue the job type declares is not known here).
    #[arg(long)]
    pub queue: Option<String>,

    /// Run it after this delay (e.g. 30s, 5m, 1h30m) instead of now.
    #[arg(long = "in", value_parser = parse_duration, conflicts_with = "at", value_name = "DELAY")]
    pub delay: Option<Duration>,

    /// Run it at this time (RFC 3339, e.g. 2030-01-01T09:00:00Z) instead of now.
    #[arg(long, value_parser = parse_time, value_name = "TIME")]
    pub at: Option<DateTime<Utc>>,

    /// Retries: `true` (the workers' `max_retries`, 25 unless configured), `false` (none, and
    /// no dead set) or a number (default: true; the job type's own setting is not known here).
    #[arg(long, value_parser = parse_retry)]
    pub retry: Option<Value>,

    /// Make the job unique for this long (e.g. 10m): a push while a job with the same class,
    /// queue and key holds the lock is a duplicate.
    #[arg(long, value_parser = parse_duration, value_name = "DURATION")]
    pub unique_for: Option<Duration>,

    /// When the unique lock is released: success (the default), start or expired.
    #[arg(long, value_parser = ["success", "start", "expired"], requires = "unique_for")]
    pub unique_until: Option<String>,

    /// The value identifying duplicates instead of the arguments, as JSON.
    #[arg(long, value_parser = parse_json, requires = "unique_for", value_name = "JSON")]
    pub unique_key: Option<Value>,

    /// Make the unique lock ignore the queue.
    #[arg(long, requires = "unique_for")]
    pub unique_across_queues: bool,

    /// What a duplicate push does: drop (the default), error or replace.
    #[arg(long, value_parser = ["drop", "error", "replace"], requires = "unique_for")]
    pub on_conflict: Option<String>,

    /// Subject the job's starts to a throttle: NAME, or NAME:KEY for its per-key limits.
    #[arg(long, value_parser = parse_throttle, value_name = "NAME[:KEY]")]
    pub throttle: Option<(String, Option<String>)>,

    /// Print JSON (`{"jid": ...}`) instead of the bare job id.
    #[arg(long)]
    pub json: bool,
}

/// Parses a JSON value (`--unique-key`).
pub fn parse_json(text: &str) -> Result<Value, String> {
    serde_json::from_str(text).map_err(|error| format!("not valid JSON: {error}"))
}

/// Parses `--throttle NAME[:KEY]` (the key may contain `:`).
pub fn parse_throttle(text: &str) -> Result<(String, Option<String>), String> {
    let (name, key) = match text.split_once(':') {
        Some((name, key)) => (name, Some(key.to_owned())),
        None => (text, None),
    };
    if name.trim().is_empty() {
        return Err("the throttle name must not be empty".to_owned());
    }
    Ok((name.to_owned(), key))
}

/// Job arguments parsed from JSON.
#[derive(Debug, Clone, PartialEq)]
pub struct JobArgs(pub Vec<Value>);

/// Parses `--args`: a JSON array, or any other JSON value as the only argument.
pub fn parse_args(text: &str) -> Result<JobArgs, String> {
    let value: Value =
        serde_json::from_str(text).map_err(|error| format!("not valid JSON: {error}"))?;
    Ok(JobArgs(match value {
        Value::Array(values) => values,
        other => vec![other],
    }))
}

/// Parses a duration: a number with a unit (`ms`, `s`, `m`, `h`, `d`), several in a row
/// (`1h30m`), or a bare number of seconds.
pub fn parse_duration(text: &str) -> Result<Duration, String> {
    let text = text.trim();
    if text.is_empty() {
        return Err("empty duration".to_owned());
    }
    if let Ok(seconds) = text.parse::<f64>() {
        return Duration::try_from_secs_f64(seconds)
            .map_err(|_| format!("`{text}` is not a valid number of seconds"));
    }
    let mut total = Duration::ZERO;
    let mut rest = text;
    while !rest.is_empty() {
        let digits = rest
            .find(|c: char| !c.is_ascii_digit() && c != '.')
            .unwrap_or(rest.len());
        if digits == 0 {
            return Err(format!("`{text}` is not a duration (try 30s, 5m or 1h30m)"));
        }
        let number: f64 = rest[..digits]
            .parse()
            .map_err(|_| format!("`{text}` is not a duration"))?;
        rest = &rest[digits..];
        let unit_end = rest
            .find(|c: char| c.is_ascii_digit() || c == '.')
            .unwrap_or(rest.len());
        let seconds_per_unit = match &rest[..unit_end] {
            "ms" => 0.001,
            "s" | "sec" | "secs" => 1.0,
            "m" | "min" | "mins" => 60.0,
            "h" | "hr" | "hrs" => 3_600.0,
            "d" | "day" | "days" => 86_400.0,
            "" => return Err(format!("`{text}` needs a unit after {number} (s, m, h, d)")),
            unit => return Err(format!("unknown unit `{unit}` in `{text}`")),
        };
        rest = &rest[unit_end..];
        let part = Duration::try_from_secs_f64(number * seconds_per_unit)
            .map_err(|_| format!("`{text}` is too long"))?;
        total = total
            .checked_add(part)
            .ok_or_else(|| format!("`{text}` is too long"))?;
    }
    Ok(total)
}

/// Parses an RFC 3339 time.
pub fn parse_time(text: &str) -> Result<DateTime<Utc>, String> {
    DateTime::parse_from_rfc3339(text.trim())
        .map(|time| time.with_timezone(&Utc))
        .map_err(|error| format!("`{text}` is not an RFC 3339 time ({error})"))
}

/// Parses `--retry`: `true`, `false` or a number of retries.
pub fn parse_retry(text: &str) -> Result<Value, String> {
    match text.trim() {
        "true" => Ok(Value::Bool(true)),
        "false" => Ok(Value::Bool(false)),
        number => number
            .parse::<u32>()
            .map(Value::from)
            .map_err(|_| format!("`{text}` is not true, false or a number of retries")),
    }
}

/// The path of the health check, which no prefix may take.
pub const HEALTH_PATH: &str = "/healthz";

/// Parses `--prefix`: `""` or `/path` (a trailing slash is dropped; `/` means the root), but
/// not the health check's path.
pub fn parse_prefix(text: &str) -> Result<String, String> {
    let trimmed = text.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return Ok(String::new());
    }
    if trimmed == HEALTH_PATH {
        return Err(format!(
            "`{text}` is the path of the health check ({HEALTH_PATH}); choose another prefix"
        ));
    }
    let valid = trimmed.starts_with('/')
        && !trimmed.contains("//")
        && trimmed
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '/' | '-' | '_' | '.' | '~'));
    if valid {
        Ok(trimmed.to_owned())
    } else {
        Err(format!(
            "`{text}` is not a path prefix like /hench (letters, digits, -, _, ., ~ and /)"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_are_parsed() {
        assert_eq!(parse_duration("90").unwrap(), Duration::from_secs(90));
        assert_eq!(parse_duration("1.5").unwrap(), Duration::from_millis(1_500));
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_duration("1h30m").unwrap(), Duration::from_secs(5_400));
        assert_eq!(parse_duration("2d").unwrap(), Duration::from_secs(172_800));
        assert_eq!(parse_duration("250ms").unwrap(), Duration::from_millis(250));
        assert_eq!(
            parse_duration(" 1.5h ").unwrap(),
            Duration::from_secs(5_400)
        );
        for bad in ["", "m", "5x", "5 m", "-5", "abc", "1h30", "1e400"] {
            assert!(parse_duration(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn other_values_are_parsed() {
        assert_eq!(
            parse_args("[1, \"a\"]").unwrap(),
            JobArgs(vec![Value::from(1), Value::from("a")])
        );
        assert_eq!(parse_args("{\"a\":1}").unwrap().0.len(), 1);
        assert!(parse_args("[1,").is_err());
        assert_eq!(
            parse_time("2030-01-01T09:00:00+02:00")
                .unwrap()
                .to_rfc3339(),
            "2030-01-01T07:00:00+00:00"
        );
        assert!(parse_time("tomorrow").is_err());
        assert_eq!(parse_retry("false").unwrap(), Value::Bool(false));
        assert_eq!(parse_retry("5").unwrap(), Value::from(5));
        assert!(parse_retry("-1").is_err());
        assert_eq!(parse_prefix("").unwrap(), "");
        assert_eq!(parse_prefix("/").unwrap(), "");
        assert_eq!(parse_prefix("/hench/").unwrap(), "/hench");
        assert_eq!(parse_prefix("/a/b-c").unwrap(), "/a/b-c");
        for bad in ["hench", "//x", "/a b", "/a?b", "/<x>"] {
            assert!(parse_prefix(bad).is_err(), "{bad}");
        }
        // The health check keeps its path (a dashboard nested there panicked the router).
        for taken in ["/healthz", "/healthz/", " /healthz// "] {
            let error = parse_prefix(taken).unwrap_err();
            assert!(error.contains("health check"), "{taken}: {error}");
        }
        assert_eq!(parse_prefix("/healthz/jobs").unwrap(), "/healthz/jobs");
        assert_eq!(parse_prefix("/HEALTHZ").unwrap(), "/HEALTHZ");
    }

    #[test]
    fn the_connection_limit_defaults_to_the_servers() {
        use clap::Parser;
        let Command::Web(web) = Cli::try_parse_from(["hench", "web"]).unwrap().command else {
            panic!("not `hench web`");
        };
        let limit = crate::connections::Limits::DEFAULT.max_connections;
        assert_eq!(usize::try_from(web.max_connections).unwrap(), limit);
        for bad in ["0", "-1", "lots"] {
            assert!(
                Cli::try_parse_from(["hench", "web", "--max-connections", bad]).is_err(),
                "{bad}"
            );
        }
    }
}
