//! The command line of the harness.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use crate::{check, orchestrator, worker};

#[derive(Parser)]
#[command(about = "Soak and chaos harness for hench")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// One worker process (spawned by `run`).
    Worker {
        #[arg(long)]
        wid: String,
        #[arg(long)]
        url: String,
        #[arg(long)]
        log: PathBuf,
        #[arg(long, default_value_t = 8)]
        concurrency: usize,
        #[arg(long, default_value = "reliable")]
        fetch: String,
        #[arg(long, default_value_t = 5)]
        timeout: u64,
        #[arg(long, default_value_t = 20)]
        orphan_check: u64,
        #[arg(long, default_value_t = 10)]
        heartbeat: u64,
        /// The pro scenario's jobs, throttle and schedule.
        #[arg(long)]
        pro: bool,
    },
    /// Runs one scenario, then checks it.
    #[command(args_override_self = true)]
    Run {
        #[arg(long)]
        scenario: String,
        /// Scenario directory (default target/soak/<scenario>; emptied first).
        #[arg(long)]
        dir: Option<PathBuf>,
        /// Seconds of load and chaos (the drain comes after).
        #[arg(long, default_value_t = 120)]
        duration: u64,
        #[arg(long, default_value_t = 4)]
        workers: usize,
        #[arg(long, default_value_t = 8)]
        concurrency: usize,
        /// reliable | basic | mixed (odd slots use basic fetch)
        #[arg(long, default_value = "reliable")]
        fetch: String,
        /// aof | rdb | none
        #[arg(long, default_value = "aof")]
        persist: String,
        /// Workers reach Redis through the fault-injecting proxy.
        #[arg(long)]
        proxy: bool,
        /// Comma-separated chaos actions: kill9, term, dterm, tstp, remote_stop, remote_quiet,
        /// redis_restart, redis_kill9, pause_write, pause_all, long_pause, proxy_drop,
        /// proxy_delay, proxy_reset, queue_pause.
        #[arg(long, default_value = "")]
        actions: String,
        #[arg(long, default_value_t = 6)]
        chaos_min: u64,
        #[arg(long, default_value_t = 15)]
        chaos_max: u64,
        /// Chaos stops after this many seconds (default: the whole duration).
        #[arg(long)]
        chaos_until: Option<u64>,
        #[arg(long, default_value_t = 16379)]
        port: u16,
        #[arg(long, default_value_t = 16380)]
        proxy_port: u16,
        #[arg(long, default_value_t = 17420)]
        web_port: u16,
        /// Worker shutdown timeout (seconds).
        #[arg(long, default_value_t = 5)]
        timeout: u64,
        #[arg(long, default_value_t = 20)]
        orphan_check: u64,
        #[arg(long, default_value_t = 10)]
        heartbeat: u64,
        /// Longest drain (seconds).
        #[arg(long, default_value_t = 300)]
        drain: u64,
        /// Single pushes per second (approximate).
        #[arg(long, default_value_t = 60)]
        rate: u64,
        #[arg(long, default_value_t = 0)]
        nonce_base: u64,
        /// The `hench` binary (hench-server) for the dashboard's JSON API.
        #[arg(long)]
        hench_bin: Option<PathBuf>,
        /// Adds the pro features' load and checks: batches of throttled members with a limiter
        /// inside, cancellations, a unique periodic schedule (`pro.rs`, `check_pro.rs`).
        #[arg(long)]
        pro: bool,
    },
    /// Checks a scenario directory.
    Check { dir: PathBuf },
}

/// Prints the checker's summary; returns whether the scenario violated no invariant.
fn print_summary(report: &serde_json::Value) -> bool {
    let violations = report["violations"].as_array().map_or(0, Vec::len);
    println!("== {}", report["dir"]);
    println!(
        "pushes {} executions {} worker instances {}",
        report["pushes"], report["executions"], report["worker_instances"]
    );
    println!("counts: {}", report["counts"]);
    println!(
        "duplicate runs by cause: {}",
        report["duplicate_runs_by_cause"]
    );
    println!(
        "concurrent runs by cause: {}",
        report["concurrent_runs_by_cause"]
    );
    println!("stats: {}", report["stats"]);
    println!(
        "leftovers: inprogress entries {}, processes set {}, inprogress set {}, other {}",
        report["leftovers"]["inprogress_entries"],
        report["leftovers"]["processes_set_members"],
        report["leftovers"]["inprogress_set_members"],
        report["leftovers"]["other_keys"]
    );
    println!(
        "exits: unexpected {}, stuck {}, slow {}, max shutdown ms {}",
        report["process_exits"]["unexpected"]
            .as_array()
            .map_or(0, Vec::len),
        report["process_exits"]["stuck_terms"]
            .as_array()
            .map_or(0, Vec::len),
        report["process_exits"]["slow_shutdowns"]
            .as_array()
            .map_or(0, Vec::len),
        report["process_exits"]["shutdown_ms_max"]
    );
    println!("worker errors: {}", report["worker_errors_by_context"]);
    println!(
        "resources: clients max {} last {}, redis mem max {}",
        report["resources"]["redis_clients_max"],
        report["resources"]["redis_clients_last"],
        report["resources"]["redis_used_memory_max"]
    );
    println!(
        "dashboard: stale-process samples {} (max {}), web errors {}, processed decreases {}",
        report["dashboard"]["samples_with_stale_processes"],
        report["dashboard"]["stale_processes_max"],
        report["dashboard"]["web_errors"],
        report["dashboard"]["processed_decreases"]
            .as_array()
            .map_or(0, Vec::len)
    );
    if !report["pro"].is_null() {
        println!("pro: {}", report["pro"]["summary"]);
    }
    println!("VIOLATIONS: {violations}");
    if let Some(list) = report["violations"].as_array() {
        for violation in list.iter().take(15) {
            println!("  {violation}");
        }
    }
    violations == 0
}

/// Runs the command; exit status 1 when a checked scenario violated an invariant (or the
/// harness failed).
pub fn main() -> ExitCode {
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("soak: cannot start a runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(run(Cli::parse())) {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(error) => {
            eprintln!("soak: {error:#}");
            ExitCode::FAILURE
        }
    }
}

/// Runs the command; `false` when a checked scenario violated an invariant.
async fn run(cli: Cli) -> anyhow::Result<bool> {
    match cli.command {
        Command::Worker {
            wid,
            url,
            log,
            concurrency,
            fetch,
            timeout,
            orphan_check,
            heartbeat,
            pro,
        } => {
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn,hench=info")),
                )
                .with_ansi(false)
                .with_writer(std::io::stderr)
                .init();
            worker::run(worker::WorkerOptions {
                wid,
                url,
                log,
                concurrency,
                fetch,
                timeout,
                orphan_check,
                heartbeat,
                pro,
            })
            .await?;
            Ok(true)
        }
        Command::Run {
            scenario,
            dir,
            duration,
            workers,
            concurrency,
            fetch,
            persist,
            proxy,
            actions,
            chaos_min,
            chaos_max,
            chaos_until,
            port,
            proxy_port,
            web_port,
            timeout,
            orphan_check,
            heartbeat,
            drain,
            rate,
            nonce_base,
            hench_bin,
            pro,
        } => {
            let dir = dir.unwrap_or_else(|| PathBuf::from("target/soak").join(&scenario));
            // Absolute: redis-server opens its log file after moving into its own directory.
            let dir = std::path::absolute(&dir)?;
            if dir.exists() {
                std::fs::remove_dir_all(&dir)?;
            }
            let options = orchestrator::RunOptions {
                scenario,
                dir: dir.clone(),
                duration,
                workers,
                concurrency,
                fetch,
                persist,
                proxy,
                actions: actions
                    .split(',')
                    .map(str::trim)
                    .filter(|a| !a.is_empty())
                    .map(str::to_owned)
                    .collect(),
                chaos_min,
                chaos_max,
                chaos_until,
                port,
                proxy_port,
                web_port,
                timeout,
                orphan_check,
                heartbeat,
                drain,
                rate,
                nonce_base,
                hench_bin,
                pro,
                self_exe: std::env::current_exe()?,
            };
            orchestrator::run(options).await?;
            let report = check::check(&dir)?;
            Ok(print_summary(&report))
        }
        Command::Check { dir } => {
            let report = check::check(&dir)?;
            Ok(print_summary(&report))
        }
    }
}
