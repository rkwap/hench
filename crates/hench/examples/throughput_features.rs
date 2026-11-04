//! Per-feature throughput: what each feature adds to a job's cost (`docs/DESIGN.md` §9k,
//! "Cost"). Like `throughput.rs`, it pushes `jobs` no-op jobs, then times one worker process
//! (reliable fetch, `concurrency` slots, the queue `default`) running them all, but each job
//! uses one feature, as `kind` says:
//!
//! * `plain`: none (the same as `throughput.rs`);
//! * `unique`: a unique job (`unique_for` 10 minutes, released at success);
//! * `unique-start`: a unique job released when it starts (one more script per execution);
//! * `throttled`: a job of a throttle whose limit (10,000 at once) never refuses, so that only
//!   the cost of its permits is measured;
//! * `per-key`: the same with a per-key limit, over 100 keys;
//! * `member`: a member of a batch (batches of 1,000);
//! * `queue-limited`: a plain job of a queue whose concurrency limit (10,000 at once) never
//!   refuses, so that only the cost of the limited fetch and of its permit is measured.
//!
//! ```text
//! cargo run --release --example throughput_features -- redis://127.0.0.1:6379/ <kind> [jobs] [concurrency]
//! ```
//!
//! `ci/throughput.sh --kind <kind>` builds it at the checkout and, with `--base`, at another
//! revision that has the features, and compares them like the plain-job gate.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use hench::limit::Limit;
use hench::{
    BulkPush, Client, Hench, Job, JobContext, JobError, RedisConfig, Throttle, UniqueUntil,
};
use serde_json::{Value, json};

/// Jobs run so far.
static DONE: AtomicU64 = AtomicU64::new(0);

/// Jobs pushed per bulk push (and members per batch).
const CHUNK: u64 = 1_000;

/// The throttles of `throttled` and `per-key`.
const THROTTLE: &str = "throughput";
const THROTTLE_PER_KEY: &str = "throughput-per-key";

/// A job that does nothing but count itself.
struct Noop;

impl Job for Noop {
    const NAME: &'static str = "Noop";
    type Args = (u64,);

    async fn perform(&self, _ctx: &JobContext, _args: (u64,)) -> Result<(), JobError> {
        DONE.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

/// The positional argument at `index`, or `default`.
fn argument<T: std::str::FromStr>(
    args: &[String],
    index: usize,
    default: T,
) -> Result<T, Box<dyn std::error::Error>>
where
    T::Err: std::error::Error + 'static,
{
    match args.get(index) {
        Some(value) => Ok(value.parse()?),
        None => Ok(default),
    }
}

/// Pushes one chunk of `kind`'s jobs (the `index`th).
async fn push(
    client: &Client,
    kind: &str,
    index: u64,
    chunk: Vec<Vec<Value>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let bulk = BulkPush::new(Noop::NAME, chunk);
    let bulk = match kind {
        "plain" | "member" | "queue-limited" => bulk,
        "unique" => bulk.unique_for(Duration::from_secs(600)),
        "unique-start" => bulk
            .unique_for(Duration::from_secs(600))
            .unique_until(UniqueUntil::Start),
        "throttled" => bulk.throttle(THROTTLE),
        "per-key" => bulk
            .throttle(THROTTLE_PER_KEY)
            .throttle_key((index % 100).to_string()),
        other => return Err(format!("unknown kind `{other}`").into()),
    };
    if kind == "member" {
        hench::batch::Batch::new(client)
            .jobs(|members| async move { members.push_bulk(bulk).await.map(|_| ()) })
            .await?;
    } else {
        client.push_bulk(bulk).await?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let usage = "usage: throughput_features <redis url> <kind> [jobs] [concurrency]";
    let url = args.first().cloned().ok_or(usage)?;
    let kind = args.get(1).cloned().ok_or(usage)?;
    let jobs: u64 = argument(&args, 2, 100_000)?;
    let concurrency: usize = argument(&args, 3, 25)?;

    let mut builder = Hench::builder()
        .redis_config(RedisConfig::new(url))
        .auto_register(false)
        .register(Noop)
        .throttle(Throttle::new(THROTTLE).concurrency(10_000))
        .throttle(Throttle::new(THROTTLE_PER_KEY).concurrency_per_key(10_000))
        .concurrency(concurrency)
        .queues(["default"]);
    if kind == "queue-limited" {
        builder = builder.queue_limits("default", [Limit::Concurrency { limit: 10_000 }]);
    }
    let hench = builder.build().await?;
    let mut next = 0;
    while next < jobs {
        let end = (next + CHUNK).min(jobs);
        let chunk = (next..end).map(|n| vec![json!(n)]).collect();
        push(hench.client(), &kind, next / CHUNK, chunk).await?;
        next = end;
    }

    let started = Instant::now();
    let launcher = hench.start().await?;
    while DONE.load(Ordering::Relaxed) < jobs {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let elapsed = started.elapsed().as_secs_f64();
    launcher.stop().await?;
    println!(
        "{jobs} {kind} jobs in {elapsed:.3} s = {:.0} jobs/s",
        jobs as f64 / elapsed
    );
    Ok(())
}
