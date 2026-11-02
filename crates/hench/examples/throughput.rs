//! Plain-job throughput: the performance gate X22 of `docs/design/pro-features.md`.
//!
//! Pushes `jobs` no-op jobs that use no feature, then times one worker process (reliable fetch,
//! `concurrency` slots, the queue `default`) running them all, and prints
//! `<jobs> jobs in <seconds> s = <rate> jobs/s`. Only the execution is timed: the pushes come
//! first. Point it at a Redis of its own (it pushes into `default` and leaves the statistics
//! behind).
//!
//! ```text
//! cargo run --release --example throughput -- redis://127.0.0.1:6379/ [jobs] [concurrency]
//! ```
//!
//! `ci/throughput.sh` builds it at the checkout and at an older revision, runs both in
//! alternation against a fresh `redis-server` each time and compares them. It only uses API
//! that existed before the pro features, so that it builds at those revisions too.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use hench::{BulkPush, Hench, Job, JobContext, JobError, RedisConfig};
use serde_json::json;

/// Jobs run so far.
static DONE: AtomicU64 = AtomicU64::new(0);

/// Jobs pushed per bulk push.
const CHUNK: u64 = 1_000;

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let url = args
        .first()
        .cloned()
        .ok_or("usage: throughput <redis url> [jobs] [concurrency]")?;
    let jobs: u64 = argument(&args, 1, 100_000)?;
    let concurrency: usize = argument(&args, 2, 25)?;

    let hench = Hench::builder()
        .redis_config(RedisConfig::new(url))
        .auto_register(false)
        .register(Noop)
        .concurrency(concurrency)
        .queues(["default"])
        .build()
        .await?;
    let mut next = 0;
    while next < jobs {
        let end = (next + CHUNK).min(jobs);
        let chunk = (next..end).map(|n| vec![json!(n)]).collect();
        hench
            .client()
            .push_bulk(BulkPush::new(Noop::NAME, chunk))
            .await?;
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
        "{jobs} jobs in {elapsed:.3} s = {:.0} jobs/s",
        jobs as f64 / elapsed
    );
    Ok(())
}
