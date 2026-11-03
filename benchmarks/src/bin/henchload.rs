//! Hench's `bin/sidekiqload`: pushes 500,000 no-op jobs, then times one process running them
//! as fast as it can.
//!
//! It follows Sidekiq 8.1's script, so that the two report comparable numbers:
//!
//! * 500 bulk pushes of `COUNT` (1,000) jobs into the queue `default` of database 13 (emptied
//!   first), each job with the arguments `["string", index, {"mike": "bob"}]` and `retry: 1`;
//! * one process with `THREADS` (10) worker slots runs them; the clock starts when the workers
//!   start and stops when a monitor, which reads the queue's length every 200 ms, finds it
//!   empty: `Done, <jobs> jobs in <seconds> sec, <rate> jobs/sec`;
//! * `LATENCY=1` delays every reply of Redis by 1 ms during the run, through toxiproxy (start
//!   `toxiproxy-server` first), to stand for a network between the process and Redis;
//! * then three more jobs print the time from their push to their execution, in seconds.
//!
//! `WARM=1` runs a first, untimed round. Redis is `IP`:`PORT` (127.0.0.1:6379), the proxy
//! listens on `PROXY_PORT` (6380). Hench only: `FETCH=basic` replaces reliable fetch (Hench's
//! default) with basic fetch (Sidekiq's open-source one); `ITERATIONS` (500) changes the number
//! of bulk pushes; and once the queue is empty the run also reports when the last job ended.
//! Sidekiq's `AJ` (ActiveJob), `GC` and `PROFILE` have no Hench counterpart.
//!
//! ```text
//! cargo run --release -p hench-benchmarks --bin henchload
//! LATENCY=1 THREADS=30 cargo run --release -p hench-benchmarks --bin henchload
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use hench::api::Api;
use hench::{
    Hench, Job, JobContext, JobError, JobExt, JobOptions, JobPayload, RedisConfig, RetryIn,
};
use hench_benchmarks::{
    Error, MONITOR_INTERVAL, Toxiproxy, describe, env_or, fetch_name, fetch_strategy, init_logging,
    rss_kb,
};
use serde_json::{Value, json};

/// Jobs run since the last setup.
static PERFORMED: AtomicU64 = AtomicU64::new(0);

/// The jobs the current round pushed.
static PUSHED: AtomicU64 = AtomicU64::new(0);

/// When the round's last pushed job ended.
static LAST_ENDED: Mutex<Option<Instant>> = Mutex::new(None);

/// The script's job: counts itself (logging every 100,000th), and when its fourth argument is
/// a push time, prints how long ago that was.
struct LoadWorker;

impl Job for LoadWorker {
    const NAME: &'static str = "LoadWorker";
    /// `["string", index, {"mike": "bob"}]`, and the push time for the latency probes: any
    /// array, as Sidekiq's `perform(string, idx, hash, ts = nil)`.
    type Args = Vec<Value>;

    fn options() -> JobOptions {
        JobOptions::new().retry(1)
    }

    async fn perform(&self, _ctx: &JobContext, args: Vec<Value>) -> Result<(), JobError> {
        let count = PERFORMED.fetch_add(1, Ordering::Relaxed) + 1;
        if count.is_multiple_of(100_000) {
            tracing::warn!("Done {count}");
        }
        if count == PUSHED.load(Ordering::Relaxed) {
            *LAST_ENDED.lock().unwrap_or_else(PoisonError::into_inner) = Some(Instant::now());
        }
        if let Some(pushed) = args.get(3).and_then(Value::as_f64) {
            println!("{}", hench::now_secs_f64() - pushed);
        }
        Ok(())
    }

    fn retry_in(&self, _count: u32, _error: &JobError, _job: &JobPayload) -> RetryIn {
        RetryIn::After(Duration::from_secs(1))
    }
}

/// The script's settings, from the environment.
struct Settings {
    threads: usize,
    latency: u64,
    count: u64,
    iterations: u64,
    ip: String,
    port: u16,
    proxy_port: u16,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    println!("{}", describe());
    let settings = Settings {
        threads: env_or("THREADS", 10)?,
        latency: env_or("LATENCY", 0)?,
        count: env_or("COUNT", 1_000)?,
        iterations: env_or("ITERATIONS", 500)?,
        ip: env_or("IP", "127.0.0.1".to_owned())?,
        port: env_or("PORT", 6379)?,
        proxy_port: env_or("PROXY_PORT", 6380)?,
    };
    let fetch = fetch_strategy()?;
    println!(
        "THREADS: {}, LATENCY: {}, FETCH: {}",
        settings.threads,
        settings.latency,
        fetch_name(fetch)
    );
    init_logging();

    // As the script: the process always talks to Redis through the proxy when there is
    // latency to add, but the latency only applies to the run, not to the pushes.
    let toxiproxy = if settings.latency > 0 {
        let toxiproxy = Toxiproxy::from_env();
        let listen = format!("{}:{}", settings.ip, settings.proxy_port);
        let upstream = format!("{}:{}", settings.ip, settings.port);
        toxiproxy.populate("redis", &listen, &upstream).await?;
        Some(toxiproxy)
    } else {
        None
    };
    let port = if toxiproxy.is_some() {
        settings.proxy_port
    } else {
        settings.port
    };
    let hench = Hench::builder()
        .redis_config(RedisConfig::new(format!(
            "redis://{}:{port}/13",
            settings.ip
        )))
        .auto_register(false)
        .register(LoadWorker)
        .concurrency(settings.threads)
        .queues(["default"])
        .fetch(fetch)
        .average_scheduled_poll_interval(Duration::from_secs(2))
        .build()
        .await?;

    if std::env::var_os("WARM").is_some() {
        setup(&hench, &settings).await?;
        run(&hench, "warmup", &settings, toxiproxy.as_ref()).await?;
    }
    setup(&hench, &settings).await?;
    run(&hench, "load", &settings, toxiproxy.as_ref()).await
}

/// Empties the database and pushes the round's jobs.
async fn setup(hench: &Hench, settings: &Settings) -> Result<(), Error> {
    tracing::warn!("Setup RSS: {}", rss_kb());
    let mut conn = hench.redis().conn().await?;
    hench::redis_rs::cmd("FLUSHDB")
        .query_async::<()>(&mut conn)
        .await?;
    let total = settings.count * settings.iterations;
    PERFORMED.store(0, Ordering::Relaxed);
    PUSHED.store(total, Ordering::Relaxed);
    *LAST_ENDED.lock().unwrap_or_else(PoisonError::into_inner) = None;

    let started = Instant::now();
    for _ in 0..settings.iterations {
        let jobs = (0..settings.count)
            .map(|index| vec![json!("string"), json!(index), json!({ "mike": "bob" })])
            .collect();
        LoadWorker::perform_bulk(hench.client(), jobs).await?;
    }
    tracing::warn!(
        "Created {total} jobs in {} sec",
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

/// Starts the workers and times them until the queue is empty, then measures the latency of
/// three jobs and stops.
async fn run(
    hench: &Hench,
    name: &str,
    settings: &Settings,
    toxiproxy: Option<&Toxiproxy>,
) -> Result<(), Error> {
    tracing::warn!("Starting {name}");
    tracing::warn!(
        "Simulating {}ms of latency between Hench and redis",
        settings.latency
    );
    if let Some(toxiproxy) = toxiproxy {
        toxiproxy.add_latency("redis", settings.latency).await?;
    }
    let queue = Api::from(hench).queue("default");
    let total = settings.count * settings.iterations;

    let started = Instant::now();
    let launcher = hench.start().await?;
    loop {
        tokio::time::sleep(MONITOR_INTERVAL).await;
        if queue.size().await? == 0 {
            break;
        }
    }
    let elapsed = started.elapsed().as_secs_f64();
    tracing::warn!(
        "Done, {total} jobs in {elapsed} sec, {} jobs/sec",
        (total as f64 / elapsed) as u64
    );
    tracing::warn!("Ending RSS: {}", rss_kb());

    // The queue empties when its last job is fetched; the jobs then still running end a
    // moment later. Without the monitor's 200 ms steps, the run took this long:
    while PERFORMED.load(Ordering::Relaxed) < total {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let ended = *LAST_ENDED.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(ended) = ended {
        let elapsed = ended.duration_since(started).as_secs_f64();
        tracing::warn!(
            "Last job ended after {elapsed} sec, {} jobs/sec",
            (total as f64 / elapsed) as u64
        );
    }

    tracing::warn!("Now here's the latency for three jobs");
    for index in 1..=3 {
        let probe = vec![
            json!(""),
            json!(index),
            json!({}),
            json!(hench::now_secs_f64()),
        ];
        LoadWorker::perform_async(hench.client(), probe).await?;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    launcher.stop().await?;
    if let Some(toxiproxy) = toxiproxy {
        toxiproxy.remove_latency("redis").await?;
    }
    Ok(())
}
