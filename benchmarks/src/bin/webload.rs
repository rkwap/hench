//! Hench's `bin/webload`: renders one page of the dashboard 10,000 times and prints how long
//! it took.
//!
//! It follows Sidekiq 8.1's script: the requests go to the dashboard's router in process, with
//! no HTTP server between (Sidekiq's goes through `Rack::Test`); 100 warm-up requests come
//! first, then `COUNT` (10,000) timed ones, each of which must answer 200 and is read whole:
//! `<page> in <seconds> sec`. `PAGE` is `/busy` by default, as in Sidekiq's script. Redis is
//! `REDIS_URL` (`redis://127.0.0.1:6379/0`).
//!
//! What a page costs depends on what Redis holds. `webload seed` gives both dashboards the same
//! data to show (Hench keeps Sidekiq's data model): it empties the database, fills the queues
//! `default`, `mailers` and `critical` with 100 jobs each, schedules 100 jobs, makes 100
//! retries and 100 dead jobs out of failing ones, then keeps 5 jobs busy in a live worker
//! process until it is stopped (SIGINT or SIGTERM). It prints `Seeded` once all that is in
//! place.
//!
//! ```text
//! cargo build --release -p hench-benchmarks --bin webload
//! target/release/webload seed &
//! PAGE=/busy target/release/webload
//! ```

use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use hench::api::Api;
use hench::{Client, Hench, Job, JobContext, JobError, JobExt, JobOptions, RedisConfig, RedisPool};
use hench_benchmarks::{Error, describe, env_or};
use hench_web::HenchWeb;
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

/// Requests before the timed ones.
const WARMUP: u32 = 100;

/// Jobs in each seeded queue, set and the busy worker.
const SEEDED: u64 = 100;

/// Busy jobs in the seeded worker.
const BUSY: u64 = 5;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let url: String = env_or("REDIS_URL", "redis://127.0.0.1:6379/0".to_owned())?;
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        None => load(&url).await,
        Some("seed") => seed(&url).await,
        Some(_) => Err("usage: webload [seed]".into()),
    }
}

/// Times `COUNT` requests to `PAGE`.
async fn load(url: &str) -> Result<(), Error> {
    println!("{}", describe());
    let page: String = env_or("PAGE", "/busy".to_owned())?;
    let count: u32 = env_or("COUNT", 10_000)?;
    let redis = RedisPool::from_config(RedisConfig::new(url)).await?;
    let router = hench_web::router(HenchWeb::new(&redis));

    for _ in 0..WARMUP {
        get(&router, &page).await?;
    }
    let started = Instant::now();
    for _ in 0..count {
        get(&router, &page).await?;
    }
    println!("{page} in {} sec", started.elapsed().as_secs_f64());
    Ok(())
}

/// One request, read to its end; anything but a 200 fails.
async fn get(router: &Router, page: &str) -> Result<(), Error> {
    let request = Request::get(page).body(Body::empty())?;
    let response = router.clone().oneshot(request).await?;
    let status = response.status();
    let body = response.into_body().collect().await?.to_bytes();
    if status != StatusCode::OK {
        let body = String::from_utf8_lossy(&body);
        return Err(format!("{page}: {status}: {body}").into());
    }
    std::hint::black_box(body);
    Ok(())
}

/// A job for the seeded queues and sets: never runs.
struct LoadWorker;

impl Job for LoadWorker {
    const NAME: &'static str = "LoadWorker";
    type Args = Vec<Value>;

    async fn perform(&self, _ctx: &JobContext, _args: Vec<Value>) -> Result<(), JobError> {
        Ok(())
    }
}

/// Fails, for the retries and the dead jobs.
struct Failer;

impl Job for Failer {
    const NAME: &'static str = "Failer";
    type Args = (u64,);

    fn options() -> JobOptions {
        JobOptions::new().queue("failing").retry(5)
    }

    async fn perform(&self, _ctx: &JobContext, (index,): (u64,)) -> Result<(), JobError> {
        Err(JobError::msg(format!("job {index} failed on purpose")))
    }
}

/// Stays busy until the process stops.
struct Sleeper;

impl Job for Sleeper {
    const NAME: &'static str = "Sleeper";
    type Args = (u64,);

    fn options() -> JobOptions {
        JobOptions::new().queue("slow")
    }

    async fn perform(&self, ctx: &JobContext, _args: (u64,)) -> Result<(), JobError> {
        ctx.stopping().await;
        Ok(())
    }
}

/// Fills Redis with what the pages show, then keeps a worker busy until a signal.
async fn seed(url: &str) -> Result<(), Error> {
    let redis = RedisPool::from_config(RedisConfig::new(url)).await?;
    let mut conn = redis.conn().await?;
    hench::redis_rs::cmd("FLUSHDB")
        .query_async::<()>(&mut conn)
        .await?;
    let client = Client::new(redis.clone());
    let args = |index: u64| vec![json!("string"), json!(index), json!({ "mike": "bob" })];
    for queue in ["default", "mailers", "critical"] {
        let jobs = (0..SEEDED).map(args).collect();
        LoadWorker::set()
            .queue(queue)
            .perform_bulk(&client, jobs)
            .await?;
    }
    let scheduled = (0..SEEDED).map(args).collect();
    LoadWorker::set()
        .delay(Duration::from_secs(3_600))
        .perform_bulk(&client, scheduled)
        .await?;
    Failer::perform_bulk(&client, (0..SEEDED).map(|index| (index,)).collect()).await?;
    Failer::set()
        .retry(0)
        .perform_bulk(&client, (0..SEEDED).map(|index| (index,)).collect())
        .await?;
    Sleeper::perform_bulk(&client, (0..BUSY).map(|index| (index,)).collect()).await?;

    let hench = Hench::builder()
        .redis(redis.clone())
        .auto_register(false)
        .register(Failer)
        .register(Sleeper)
        .concurrency(10)
        .queues(["failing", "slow"])
        // The retries stay retries while the pages are measured.
        .poll_interval_average(Duration::from_secs(3_600))
        .timeout(Duration::from_secs(1))
        .build()
        .await?;
    let launcher = hench.start().await?;
    let api = Api::new(redis);
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let retries = api.retries().size().await?;
        let dead = api.dead().size().await?;
        let busy = api.work().size().await?;
        if retries == SEEDED && dead == SEEDED && busy == BUSY {
            break;
        }
    }
    println!("Seeded");
    shutdown_signal().await;
    launcher.stop().await?;
    Ok(())
}

/// Ctrl-C or SIGTERM.
async fn shutdown_signal() {
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut terminate) => {
                terminate.recv().await;
            }
            Err(_) => std::future::pending().await,
        }
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        () = terminate => {}
    }
}
