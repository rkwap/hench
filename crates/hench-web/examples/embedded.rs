//! An axum application with the hench dashboard nested at `/hench` and workers running in the
//! same process. Pass `--seed` to fill Redis with sample jobs first: queued, scheduled,
//! running, retried and dead ones.
//!
//! ```sh
//! REDIS_URL=redis://127.0.0.1:6379 cargo run -p hench-web --example embedded -- --seed
//! open http://127.0.0.1:3000/hench
//! ```

use std::time::Duration;

use axum::Router;
use axum::response::Html;
use axum::routing::get;
use hench::CancellationToken;
use hench::prelude::*;
use hench_web::HenchWeb;
use serde_json::json;

/// Takes a few seconds, so the busy page has something to show.
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
        (_path, seconds): (String, u32),
    ) -> Result<(), JobError> {
        tokio::select! {
            () = tokio::time::sleep(Duration::from_secs(u64::from(seconds))) => Ok(()),
            () = ctx.stopping() => Err(JobError::new(JobError::SHUTDOWN, "shutting down")),
        }
    }
}

/// Always fails, so the retries page has something to show.
#[derive(Default)]
struct ChargeCard;

impl Job for ChargeCard {
    const NAME: &'static str = "ChargeCard";
    type Args = (i64, String);

    fn options() -> JobOptions {
        JobOptions::new().queue("billing").retry(10).backtrace(true)
    }

    async fn perform(
        &self,
        _ctx: &JobContext,
        (customer, _): (i64, String),
    ) -> Result<(), JobError> {
        Err(JobError::msg(format!(
            "the payment gateway timed out for customer {customer}"
        )))
    }
}

async fn seed(hench: &Hench) -> hench::Result<()> {
    let client = hench.client();
    for index in 0..8 {
        ResizeImage::perform_async(client, (format!("/uploads/{index}.png"), 4 + index % 5))
            .await?;
    }
    for index in 0..40 {
        let mut job = JobPayload::new(
            "SendNewsletter",
            vec![json!(index), json!({"locale": "en"})],
        );
        job.queue = "mailers".to_owned();
        client.push(job).await?;
    }
    for index in 0..25 {
        let mut job = JobPayload::new("Cleanup", vec![json!(index)]);
        job.queue = "maintenance".to_owned();
        job.at = Some(hench::now_secs_f64() + 60.0 * f64::from(index + 1));
        client.push(job).await?;
    }
    for customer in 0..6 {
        ChargeCard::perform_async(client, (customer, "<b>premium</b>".to_owned())).await?;
    }
    let api = hench::api::Api::from_hench(hench);
    let dead = api.dead();
    for index in 0..4 {
        let job = json!({
            "class": "ImportCsv",
            "args": [format!("s3://bucket/import-{index}.csv")],
            "queue": "imports",
            "jid": hench::generate_jid(),
            "retry_count": 25,
            "failed_at": hench::now_millis() - 86_400_000,
            "retried_at": hench::now_millis() - 3_600_000,
            "error_class": "std::io::error::Error",
            "error_message": "connection reset by peer",
            "created_at": hench::now_millis() - 90_000_000,
        });
        dead.kill(&job.to_string(), false).await?;
    }
    api.queue("maintenance-paused").pause().await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let hench = Hench::builder()
        .queues(["images", "billing", "default"])
        .concurrency(4)
        .register(ResizeImage)
        .register(ChargeCard)
        .build()
        .await?;
    if std::env::args().any(|arg| arg == "--seed") {
        seed(&hench).await?;
    }

    // No authentication here: the dashboard answers only requests for localhost and IP
    // addresses (`HenchWeb::allowed_hosts` names others), so that a web page on another site
    // cannot reach it by pointing its own host name at this machine (DNS rebinding).
    let dashboard = HenchWeb::new(&hench).title("Acme jobs").app_url("/");
    let app = Router::new()
        .route(
            "/",
            get(|| async { Html("<p>The application. <a href=\"/hench\">Jobs dashboard</a></p>") }),
        )
        .nest_service("/hench", hench_web::router(dashboard));
    let address = std::env::var("BIND").unwrap_or_else(|_| "127.0.0.1:3000".to_owned());
    let listener = tokio::net::TcpListener::bind(&address).await?;
    tracing::info!(address = %listener.local_addr()?, "dashboard at /hench");

    let shutdown = CancellationToken::new();
    let server = {
        let shutdown = shutdown.clone();
        async move {
            let served = axum::serve(listener, app)
                .with_graceful_shutdown(shutdown.clone().cancelled_owned())
                .await;
            shutdown.cancel();
            served
        }
    };
    let workers = {
        let shutdown = shutdown.clone();
        async move {
            let ran = hench.run_until(shutdown.clone().cancelled_owned()).await;
            shutdown.cancel();
            ran
        }
    };
    tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                shutdown.cancel();
            }
        }
    });
    let (served, ran) = tokio::join!(server, workers);
    served?;
    ran?;
    Ok(())
}
