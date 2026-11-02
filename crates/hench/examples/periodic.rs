//! Periodic (cron) jobs: a worker that defines schedules in its code, manages a dynamic one
//! through the API, and processes their jobs until SIGINT or SIGTERM.
//!
//! ```sh
//! REDIS_URL=redis://127.0.0.1:6379 cargo run -p hench --example periodic
//! ```
//!
//! It registers `heartbeat` (every 10 seconds), `hourly-rollup` (five past every hour, catching
//! up to 24 missed hours after downtime) and `nightly-report` (02:00 in Paris), creates the
//! dynamic schedule `tenant-acme-sync` (every minute, never two runs at once) unless it exists,
//! enqueues the tenant's sync once now, prints the schedules, then runs. Start it twice: both
//! processes poll, and each tick is still enqueued once. `hench periodic` (or the dashboard's
//! Periodic tab) shows the schedules, their history and their last runs.

#[cfg(feature = "periodic")]
mod example {
    use std::time::Duration;

    use hench::api::Api;
    use hench::prelude::*;

    /// Logs that the workers are alive, every 10 seconds.
    #[hench::job(queue = "periodic", retry = false)]
    async fn heartbeat(ctx: &JobContext) {
        if let Some(tick) = ctx.periodic() {
            tracing::info!(schedule = tick.name, tick = ?tick.tick, "heartbeat");
        }
    }

    /// Rolls up the statistics of the last `hours` hours.
    #[hench::job(queue = "periodic")]
    async fn rollup(ctx: &JobContext, hours: u32) {
        // The tick the job is for, not when it runs: a late or retried job rolls up the right
        // hour.
        let tick = ctx.periodic().map(|tick| tick.tick);
        tracing::info!(hours, ?tick, "rolled up");
    }

    /// Builds the report of the previous day.
    #[hench::job(queue = "periodic", retry = 3)]
    async fn nightly_report(ctx: &JobContext) {
        let manual = ctx.periodic().is_some_and(|tick| tick.manual);
        tracing::info!(manual, "nightly report built");
    }

    /// Synchronizes one tenant's data with its CRM.
    #[hench::job(queue = "tenants")]
    async fn sync_tenant(tenant: String) -> Result<(), std::io::Error> {
        tokio::time::sleep(Duration::from_millis(200)).await;
        tracing::info!(tenant, "tenant synchronized");
        Ok(())
    }

    pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
            )
            .init();

        // Schedules defined in code: every process registers them when it starts (and deletes
        // those its code no longer defines).
        let hench = Hench::builder()
            .queues(["periodic", "tenants", "default"])
            .concurrency(5)
            .periodic(
                Periodic::every("heartbeat", Duration::from_secs(10))
                    .job::<heartbeat>(())
                    .catch_up(CatchUp::Skip),
            )
            .periodic(
                Periodic::cron("hourly-rollup", "5 * * * *")
                    .job::<rollup>((1,))
                    .catch_up(CatchUp::All { max: 24 })
                    .description("Hourly statistics; missed hours are rolled up after downtime"),
            )
            .periodic(
                Periodic::cron("nightly-report", "0 2 * * *")
                    .tz("Europe/Paris")
                    .job::<nightly_report>(())
                    .description("The report of the previous day"),
            )
            .build()
            .await?;

        // A dynamic schedule, as an application would create one per tenant.
        let schedules = Api::from(&hench).periodic();
        let tenant = Periodic::every("tenant-acme-sync", Duration::from_secs(60))
            .job::<sync_tenant>(("acme".to_owned(),))
            // A tick while the previous sync waits or runs is skipped (a duplicate).
            .unique_for(Duration::from_secs(120))
            .description("CRM synchronization of tenant acme");
        let upserted = schedules.upsert(&tenant).await?;
        tracing::info!(?upserted, "dynamic schedule");

        // Once now, by hand: the timetable does not change. (`None`: the previous sync still
        // holds the unique lock.)
        let jid = schedules.enqueue("tenant-acme-sync").await?;
        tracing::info!(?jid, "tenant sync enqueued by hand");

        // The code schedules appear once a process registered them (when it starts).
        let page = schedules.page(1, 100).await?;
        for entry in &page.items {
            println!(
                "{:<18} {:<40} next {:?}",
                entry.name,
                entry.describe(),
                entry.next
            );
        }

        hench.run().await?;
        Ok(())
    }
}

#[cfg(feature = "periodic")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    example::main().await
}

#[cfg(not(feature = "periodic"))]
fn main() {
    eprintln!("this example needs the `periodic` feature (on by default)");
}
