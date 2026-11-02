//! A standalone worker process configured by its command line and an optional TOML file
//! (`hench::cli`, feature `cli`): processes jobs until SIGINT or SIGTERM (SIGTSTP stops it from
//! taking new jobs, like Sidekiq's quiet mode). `--enqueue N` enqueues sample jobs first.
//!
//! ```sh
//! REDIS_URL=redis://127.0.0.1:6379 cargo run -p hench --features cli --example worker -- \
//!     -c 10 -q critical,3 -q images,2 -q default -t 30 --enqueue 20
//! cargo run -p hench --features cli --example worker -- --help
//! ```
//!
//! Without `-q` it processes `critical`, `images` and `default` in that order; `-C` (or
//! `config/hench.toml`) can set everything `hench::cli` documents.

use std::time::Duration;

use clap::Parser;
use hench::cli::WorkerArgs;
use hench::iterable::{Enumerator, IterableJob, Iteration};
use hench::prelude::*;

/// The worker's options: hench's, plus one of this example.
#[derive(Debug, Parser)]
#[command(about = "Processes the example's background jobs")]
struct Cli {
    #[command(flatten)]
    worker: WorkerArgs,

    /// Enqueue this many sample jobs before starting
    #[arg(long, value_name = "N")]
    enqueue: Option<u32>,
}

/// Resizes an image (simulated).
#[derive(Default)]
struct ResizeImage;

impl Job for ResizeImage {
    const NAME: &'static str = "ResizeImage";
    type Args = (String, u32);

    fn options() -> JobOptions {
        JobOptions::new().queue("images").tags(["media"])
    }

    async fn perform(
        &self,
        ctx: &JobContext,
        (path, width): (String, u32),
    ) -> Result<(), JobError> {
        tracing::info!(path, width, jid = ctx.jid(), "resizing");
        // CPU-heavy work belongs on the blocking pool so it does not stall other jobs.
        tokio::task::spawn_blocking(move || std::thread::sleep(Duration::from_millis(20)))
            .await
            .map_err(|error| JobError::msg(error.to_string()))?;
        Ok(())
    }
}

/// Builds a nightly report; fails now and then to show retries.
#[derive(Default)]
struct NightlyReport;

impl Job for NightlyReport {
    const NAME: &'static str = "NightlyReport";
    type Args = (u32,);

    fn options() -> JobOptions {
        JobOptions::new().queue("serial").retry(3).backtrace(true)
    }

    async fn perform(&self, ctx: &JobContext, (day,): (u32,)) -> Result<(), JobError> {
        if day % 7 == 0 && ctx.retry_count().is_none() {
            return Err(JobError::msg(format!(
                "the data for day {day} is not ready yet"
            )));
        }
        tracing::info!(day, "report built");
        Ok(())
    }

    fn retry_in(&self, count: u32, _error: &JobError, _job: &JobPayload) -> RetryIn {
        RetryIn::After(Duration::from_secs(5 * u64::from(count + 1)))
    }
}

/// Re-indexes every product of the catalog, a page at a time; stopping the worker saves its
/// position, and the next worker continues from there.
#[derive(Default)]
struct ReindexCatalog;

impl IterableJob for ReindexCatalog {
    const NAME: &'static str = "ReindexCatalog";
    type Args = (u64,); // number of products
    type Item = Vec<u64>; // a page of product ids
    type Cursor = u64; // the last product id handled

    fn build_enumerator<'a>(
        &'a self,
        _ctx: &'a JobContext,
        (products,): &'a (u64,),
        cursor: Option<u64>,
    ) -> Result<Enumerator<'a, Vec<u64>, u64>, JobError> {
        // A page of 50 ids after `after`: a database query in a real application.
        let products = *products;
        Ok(Enumerator::batches(cursor, move |after| async move {
            let first = after.map_or(1, |id| id + 1);
            Ok((first..=products).take(50).map(|id| (id, id)).collect())
        }))
    }

    async fn each_iteration(
        &self,
        page: Vec<u64>,
        it: &Iteration<'_, Self>,
    ) -> Result<(), JobError> {
        tokio::time::sleep(Duration::from_millis(100)).await;
        tracing::info!(
            first = page.first(),
            last = page.last(),
            execution = it.executions(),
            "re-indexed a page of products"
        );
        Ok(())
    }

    async fn on_complete(&self, it: &Iteration<'_, Self>) -> Result<(), JobError> {
        tracing::info!(runtime = ?it.runtime(), "catalog re-indexed");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let settings = cli.worker.settings()?;
    // RUST_LOG wins; otherwise `-v` (or `verbose = true`) logs at debug level.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(settings.log_directives())),
        )
        .init();

    // The program's defaults; the configuration file and the command line override them.
    let builder = Hench::builder()
        // Strict priority: `critical` first, then `images`, then `default`.
        .queues(["critical", "images", "default"])
        .concurrency(10)
        // Reports run one at a time.
        .capsule("serial", |capsule| {
            capsule.concurrency(1).queues(["serial"])
        })
        .register(ResizeImage)
        .register(NightlyReport)
        .register(ReindexCatalog)
        // Long iterations take turns with the other jobs of their queue.
        .max_iteration_runtime(Duration::from_secs(30))
        .death_handler(|job, error| {
            tracing::error!(class = %job.class, jid = %job.jid, %error, "job died");
        })
        .on_startup(|| async { tracing::info!("worker starting") })
        .on_quiet(|| async { tracing::info!("worker quiet: finishing running jobs") })
        .on_shutdown(|| async { tracing::info!("worker shutting down") })
        .timeout(Duration::from_secs(25));
    // (`apply` logs the configuration file and the environment in use.)
    let hench = settings.apply(builder).build().await?;

    if let Some(count) = cli.enqueue {
        let client = hench.client();
        let images: Vec<(String, u32)> = (0..count)
            .map(|i| (format!("photos/{i}.jpg"), 640))
            .collect();
        ResizeImage::perform_bulk(client, images).await?;
        for day in 0..count.min(14) {
            NightlyReport::perform_in(client, Duration::from_secs(u64::from(day)), (day,)).await?;
        }
        ReindexCatalog::perform_async(client, (u64::from(count) * 25,)).await?;
        tracing::info!(count, "sample jobs enqueued");
    }

    hench.run().await?;
    Ok(())
}
