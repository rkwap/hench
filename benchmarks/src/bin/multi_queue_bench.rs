//! Hench's `bin/multi_queue_bench`: several processes fill several queues, then several worker
//! processes drain them, each its own queue, against one Redis.
//!
//! It follows Sidekiq 8.1's script, so that the two report comparable numbers:
//!
//! * `QUEUES` (8) queues, `q0`, `q1`...; one producer process per queue pushes `ITERATIONS`
//!   (1,000) bulk pushes of `ELEMENTS` (1,000) jobs, each with the arguments `[index]` and
//!   `retry: 1`: 8,000,000 jobs by default;
//! * then `PROCESSES` (8) worker processes with `THREADS` (30) slots each, process `i` working
//!   on the queue `q<i % QUEUES>`. Each stops once its queue is empty; process 0 also reads
//!   every queue's length every 200 ms and, when they are all empty, reports
//!   `Done, <jobs> jobs in <seconds> sec, <rate> jobs/sec`, timed from its own start.
//!
//! Redis is `IP`:`PORT` (127.0.0.1:6379), database 0, which the script does not empty. Hench
//! only: `FETCH=basic` replaces reliable fetch (Hench's default) with basic fetch (Sidekiq's
//! open-source one).
//!
//! ```text
//! cargo build --release -p hench-benchmarks --bin multi_queue_bench
//! THREADS=10 PROCESSES=8 QUEUES=8 target/release/multi_queue_bench
//! ```
//!
//! The processes are this binary started again with a role: `produce <queue>`, `consume
//! <queue>` or `consume <queue> all` (process 0).

use std::process::{Child, Command};
use std::time::{Duration, Instant};

use hench::api::{Api, Queue};
use hench::{
    Client, Hench, Job, JobContext, JobError, JobExt, JobOptions, JobPayload, RedisConfig,
    RedisPool, RetryIn,
};
use hench_benchmarks::{
    Error, MONITOR_INTERVAL, describe, env_or, fetch_name, fetch_strategy, init_logging, rss_kb,
};
use serde_json::{Value, json};

/// The script's job, which does nothing (but print its latency when given a push time).
struct LoadWorker;

impl Job for LoadWorker {
    const NAME: &'static str = "LoadWorker";
    /// `[index]`, or `[index, push time]`: Sidekiq's `perform(idx, ts = nil)`.
    type Args = Vec<Value>;

    fn options() -> JobOptions {
        JobOptions::new().retry(1)
    }

    async fn perform(&self, _ctx: &JobContext, args: Vec<Value>) -> Result<(), JobError> {
        if let Some(pushed) = args.get(1).and_then(Value::as_f64) {
            println!("{}", hench::now_secs_f64() - pushed);
        }
        Ok(())
    }

    fn retry_in(&self, _count: u32, _error: &JobError, _job: &JobPayload) -> RetryIn {
        RetryIn::After(Duration::from_secs(1))
    }
}

/// The script's settings, from the environment (every process reads them).
struct Settings {
    queues: usize,
    processes: usize,
    elements: u64,
    iterations: u64,
    threads: usize,
    url: String,
}

impl Settings {
    fn from_env() -> Result<Self, Error> {
        let ip: String = env_or("IP", "127.0.0.1".to_owned())?;
        let port: u16 = env_or("PORT", 6379)?;
        Ok(Self {
            queues: env_or("QUEUES", 8)?,
            processes: env_or("PROCESSES", 8)?,
            elements: env_or("ELEMENTS", 1_000)?,
            iterations: env_or("ITERATIONS", 1_000)?,
            threads: env_or("THREADS", 30)?,
            url: format!("redis://{ip}:{port}/0"),
        })
    }

    /// Every job of every queue.
    fn jobs(&self) -> u64 {
        self.elements * self.iterations * self.queues as u64
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let settings = Settings::from_env()?;
    let args: Vec<String> = std::env::args().skip(1).collect();
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    match args.as_slice() {
        [] => orchestrate(&settings),
        ["produce", queue] => produce(&settings, queue).await,
        ["consume", queue] => consume(&settings, queue, false).await,
        ["consume", queue, "all"] => consume(&settings, queue, true).await,
        _ => Err("usage: multi_queue_bench [produce QUEUE | consume QUEUE [all]]".into()),
    }
}

/// Runs the producers, then the consumers, each a process of its own.
fn orchestrate(settings: &Settings) -> Result<(), Error> {
    println!("{}", describe());
    println!(
        "QUEUES: {}, PROCESSES: {}, ELEMENTS: {}, ITERATIONS: {}, THREADS: {}, FETCH: {}",
        settings.queues,
        settings.processes,
        settings.elements,
        settings.iterations,
        settings.threads,
        fetch_name(fetch_strategy()?)
    );
    let names: Vec<String> = (0..settings.queues)
        .map(|index| format!("q{index}"))
        .collect();
    println!("Queues are: {names:?}");

    let producers = names
        .iter()
        .map(|queue| spawn(&["produce", queue]))
        .collect::<Result<Vec<_>, _>>()?;
    wait_all(producers)?;

    let consumers = (0..settings.processes)
        .map(|index| {
            let queue = &names[index % settings.queues];
            if index == 0 {
                spawn(&["consume", queue, "all"])
            } else {
                spawn(&["consume", queue])
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    wait_all(consumers)
}

/// This binary started again with `args`, sharing this process's environment and output.
fn spawn(args: &[&str]) -> Result<Child, Error> {
    Ok(Command::new(std::env::current_exe()?).args(args).spawn()?)
}

/// Waits for every child; fails if one did.
fn wait_all(children: Vec<Child>) -> Result<(), Error> {
    let mut failed = 0;
    for mut child in children {
        if !child.wait()?.success() {
            failed += 1;
        }
    }
    if failed > 0 {
        return Err(format!("{failed} processes failed").into());
    }
    Ok(())
}

/// Pushes the queue's jobs, `ELEMENTS` per bulk push.
async fn produce(settings: &Settings, queue: &str) -> Result<(), Error> {
    init_logging();
    tracing::warn!("Setup RSS: {}", rss_kb());
    tracing::warn!("Pushing work to queue: {queue}");
    let client = Client::new(RedisPool::from_config(RedisConfig::new(settings.url.clone())).await?);
    for _ in 0..settings.iterations {
        let jobs = (0..settings.elements)
            .map(|index| vec![json!(index)])
            .collect();
        LoadWorker::set()
            .queue(queue)
            .perform_bulk(&client, jobs)
            .await?;
    }
    println!("Done");
    Ok(())
}

/// Works on `queue` until it is empty (until every queue is, and then reports, with `all`).
async fn consume(settings: &Settings, queue: &str, all: bool) -> Result<(), Error> {
    init_logging();
    let hench = Hench::builder()
        .redis_config(RedisConfig::new(settings.url.clone()))
        .auto_register(false)
        .register(LoadWorker)
        .concurrency(settings.threads)
        .queues([queue])
        .fetch(fetch_strategy()?)
        .average_scheduled_poll_interval(Duration::from_secs(2))
        .build()
        .await?;
    tracing::warn!("Consuming from {queue}");
    let api = Api::from(&hench);
    let watched: Vec<Queue> = if all {
        (0..settings.queues)
            .map(|index| api.queue(format!("q{index}")))
            .collect()
    } else {
        vec![api.queue(queue)]
    };

    let started = Instant::now();
    let launcher = hench.start().await?;
    loop {
        tokio::time::sleep(MONITOR_INTERVAL).await;
        let mut left = 0;
        for queue in &watched {
            left += queue.size().await?;
        }
        if left == 0 {
            break;
        }
    }
    if all {
        let elapsed = started.elapsed().as_secs_f64();
        let jobs = settings.jobs();
        tracing::warn!(
            "Done, {jobs} jobs in {elapsed} sec, {} jobs/sec",
            (jobs as f64 / elapsed) as u64
        );
        tracing::warn!("Ending RSS: {}", rss_kb());
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    launcher.stop().await?;
    Ok(())
}
