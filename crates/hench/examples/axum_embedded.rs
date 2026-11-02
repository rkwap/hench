//! An axum web application that enqueues jobs, with hench workers running in the same
//! process. One shutdown signal (Ctrl-C or SIGTERM) stops both gracefully: the server stops
//! accepting requests and finishes the ones in flight while the workers finish their jobs.
//! If either side stops on its own (an error, or a remote stop from the dashboard), the other
//! one is shut down too.
//!
//! The application also reads hench's statistics with the data API (`hench::api`), the same
//! numbers a dashboard shows.
//!
//! ```sh
//! REDIS_URL=redis://127.0.0.1:6379 cargo run -p hench --example axum_embedded
//! curl -X POST 'http://127.0.0.1:3000/welcome?user_id=42'
//! curl http://127.0.0.1:3000/stats
//! ```

use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use hench::CancellationToken;
use hench::api::Api;
use hench::prelude::*;
use serde::Deserialize;
use serde_json::{Value, json};

/// Shared state jobs read through `ctx.data::<Mailer>()`.
struct Mailer {
    from: String,
}

impl Mailer {
    async fn send_welcome(&self, user_id: i64) -> std::io::Result<()> {
        // A real mailer would talk to an SMTP server or an email API here.
        tokio::time::sleep(Duration::from_millis(50)).await;
        tracing::info!(from = %self.from, user_id, "welcome email sent");
        Ok(())
    }
}

/// Sends the welcome email of a new user.
#[derive(Default)]
struct SendWelcome;

impl Job for SendWelcome {
    const NAME: &'static str = "SendWelcome";
    type Args = (i64,);

    fn options() -> JobOptions {
        JobOptions::new().queue("mailers").retry(5)
    }

    async fn perform(&self, ctx: &JobContext, (user_id,): (i64,)) -> Result<(), JobError> {
        if ctx.is_stopping() {
            // Not started yet and the process is going away: let another process run it.
            return Err(JobError::new(JobError::SHUTDOWN, "shutting down"));
        }
        ctx.data::<Mailer>()?.send_welcome(user_id).await?;
        Ok(())
    }
}

/// What the request handlers share.
#[derive(Clone)]
struct AppState {
    /// Enqueues jobs.
    client: Client,
    /// Reads hench's data in Redis.
    api: Api,
}

#[derive(Deserialize)]
struct WelcomeParams {
    user_id: i64,
}

/// `POST /welcome?user_id=42`: enqueues the job and answers with its jid.
async fn welcome(
    State(state): State<AppState>,
    Query(params): Query<WelcomeParams>,
) -> Result<Json<Value>, (StatusCode, String)> {
    match SendWelcome::perform_async(&state.client, (params.user_id,)).await {
        Ok(jid) => Ok(Json(json!({ "jid": jid }))),
        Err(error) => Err((StatusCode::SERVICE_UNAVAILABLE, error.to_string())),
    }
}

/// `GET /stats`: job counts and the size of each queue.
async fn stats(State(state): State<AppState>) -> Result<Json<Value>, (StatusCode, String)> {
    let unavailable = |error: hench::Error| (StatusCode::SERVICE_UNAVAILABLE, error.to_string());
    let stats = state.api.stats().await.map_err(unavailable)?;
    let mut queues = serde_json::Map::new();
    for queue in state.api.queues().await.map_err(unavailable)? {
        let size = queue.size().await.map_err(unavailable)?;
        queues.insert(queue.name().to_owned(), json!(size));
    }
    Ok(Json(json!({
        "processed": stats.processed,
        "failed": stats.failed,
        "enqueued": stats.enqueued,
        "scheduled": stats.scheduled_size,
        "retries": stats.retry_size,
        "dead": stats.dead_size,
        "processes": stats.processes_size,
        "busy": stats.workers_size,
        "default_queue_latency": stats.default_queue_latency,
        "queues": queues,
    })))
}

/// Completes on Ctrl-C or, on Unix, SIGTERM.
async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "cannot listen for Ctrl-C");
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut terminate) => {
                terminate.recv().await;
            }
            Err(error) => {
                tracing::error!(%error, "cannot listen for SIGTERM");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // The Redis URL comes from REDIS_URL (or REDIS_PROVIDER), like Sidekiq.
    let hench = Hench::builder()
        .queues(["mailers", "default"])
        .concurrency(10)
        .register(SendWelcome)
        .data(Mailer {
            from: "hello@example.com".to_owned(),
        })
        .build()
        .await?;

    let state = AppState {
        client: hench.client().clone(),
        api: Api::from_hench(&hench),
    };
    let app = Router::new()
        .route("/welcome", post(welcome))
        .route("/stats", get(stats))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    tracing::info!(address = %listener.local_addr()?, "listening");

    let shutdown = CancellationToken::new();
    tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            shutdown_signal().await;
            tracing::info!("shutting down");
            shutdown.cancel();
        }
    });

    let server = {
        let shutdown = shutdown.clone();
        async move {
            let result = axum::serve(listener, app)
                .with_graceful_shutdown(shutdown.clone().cancelled_owned())
                .await;
            shutdown.cancel();
            result
        }
    };
    let workers = {
        let shutdown = shutdown.clone();
        async move {
            let result = hench.run_until(shutdown.clone().cancelled_owned()).await;
            shutdown.cancel();
            result
        }
    };
    let (served, worked) = tokio::join!(server, workers);
    served?;
    worked?;
    Ok(())
}
