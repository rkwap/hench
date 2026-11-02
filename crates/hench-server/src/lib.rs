//! # hench-server
//!
//! The `hench` command: the [hench](https://docs.rs/hench) dashboard as a standalone server,
//! a JSON API to inspect the cluster and push jobs over HTTP, and command-line tools to inspect
//! and manage a hench cluster: its processes and queues, periodic schedules, batches, rate
//! limits and the locks of unique jobs.
//!
//! ```text
//! hench web [--bind 127.0.0.1:7420] [--username U --password P] [--read-only]
//!           [--prefix /path] [--api] [--allowed-host HOST]... [--title T]
//!           [--poll-interval 5s] [--max-connections N]
//! hench stats [all|version|overview|processes|queues|metrics|periodic|batches|limits|locks]
//!             [--json]
//! hench processes [--json]
//! hench queues [--json]
//! hench quiet <identity|all>
//! hench stop <identity|all>
//! hench enqueue <class> [--args JSON] [--queue Q] [--in 5m | --at RFC3339] [--retry R]
//!               [--unique-for 10m [--unique-until success|start|expired] [--unique-key JSON]
//!                [--unique-across-queues] [--on-conflict drop|error|replace]]
//!               [--throttle NAME[:KEY]] [--json]
//! hench periodic [list|show|enqueue|pause|unpause|remove|add|repair|next] ...
//! hench batches [list|show|cancel|delete|skip|dispatch] ...
//! hench limits [list|show|reset|clear-line] ...
//! hench locks [list|show|release|release-all] ...
//! ```
//!
//! `hench <command> --help` describes each command and its options; the commands of the
//! features are also described in [`commands::periodic`], [`commands::batches`],
//! [`commands::limits`] and [`commands::locks`]. `hench stats` prints the version, the totals,
//! the processes, the queues and the busiest job classes of the last hour, then the section of
//! each feature in use; `hench stats <section>` prints one of them.
//!
//! Every command takes `--redis-url URL` (or `HENCH_REDIS_URL`); without it the environment
//! is read like workers do (`REDIS_PROVIDER`, then `REDIS_URL`), then
//! `redis://127.0.0.1:6379`.
//!
//! `hench web` serves the dashboard ([`hench_web`]) with HTTP Basic authentication when
//! `--username`/`--password` (or `HENCH_WEB_USERNAME`/`HENCH_WEB_PASSWORD`) are set, an
//! unauthenticated `/healthz` (also under `--prefix`), and with `--api` the JSON API of
//! [`json_api`] under `<prefix>/api/v1` (the totals, queues and processes, `POST /jobs`, and
//! the routes of the periodic schedules, batches, limits and locks). It refuses to serve the
//! API on a non-loopback address without credentials; without credentials it answers only
//! requests for `localhost` and IP addresses, plus the `--allowed-host` names (a defense
//! against DNS rebinding, see [`hosts`]). Every phase of a connection has a deadline and the
//! number of connections is capped ([`connections`]), so slow or idle clients cannot hold the
//! server. SIGINT or SIGTERM shut it down gracefully: requests in flight get 10 seconds to
//! finish, and a second signal stops it at once.
//!
//! This library is the implementation of the binary; its API is not meant for other uses and
//! is not covered by semantic versioning: any release may change it.

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]

pub mod auth;
pub mod cli;
pub mod commands;
pub mod connections;
pub mod hosts;
pub mod json_api;
pub mod monitor;
pub mod server;

use std::io::{IsTerminal, Write};

pub use crate::cli::Cli;

/// The examples of the repository's README, compiled (and, when they need no Redis, run) as
/// doc tests: this crate depends on everything they use (`hench` with the `cli` feature,
/// `hench-web`, axum, tokio, serde, clap, tracing).
// `README.md` in this crate's directory links to the repository's README, and the package
// carries it as a file, so the doc tests also build from the published crate.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
pub struct ReadmeDoctests;

/// Runs a parsed command line, writing what the command prints to `out` (logs go to
/// standard error).
pub async fn run<W: Write>(cli: Cli, out: &mut W) -> anyhow::Result<()> {
    commands::run(cli, out).await
}

/// Sets up logging to standard error: `HENCH_LOG` (or `RUST_LOG`) filters, defaulting to
/// `info` for `hench web` and `warn` for the other commands.
pub fn init_tracing(cli: &Cli) {
    let default = match cli.command {
        cli::Command::Web(_) => "info",
        _ => "warn",
    };
    let filter = std::env::var("HENCH_LOG")
        .ok()
        .and_then(|filter| tracing_subscriber::EnvFilter::try_new(filter).ok())
        .or_else(|| tracing_subscriber::EnvFilter::try_from_default_env().ok())
        .unwrap_or_else(|| tracing_subscriber::EnvFilter::new(default));
    // A second initialization (tests) keeps the first subscriber.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_ansi(std::io::stderr().is_terminal())
        .try_init();
}
