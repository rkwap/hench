# Getting started

From an empty project to jobs that run: the dependencies, a job, pushes, a worker process, and
workers embedded next to an axum application with the dashboard.

You need Redis 6.2 or newer, or Valkey (the test suite passes against Valkey 9). Other
Redis-compatible servers that implement the same commands, such as Dragonfly, should work too,
though they are not tested yet: see the [roadmap](../ROADMAP.md).

## 1. Add the dependencies

The crates are published on [crates.io](https://crates.io/crates/hench): `hench = "0.1"` adds
the library (with `#[hench::job]`, from `hench-macros`), `hench-web = "0.1"` the dashboard, and
`cargo install hench-server --locked` installs the [`hench` command](cli.md). An
application that defines, enqueues and runs jobs, and serves the dashboard, starts from these
dependencies:

```toml
[dependencies]
hench = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "net", "signal"] }
serde = { version = "1", features = ["derive"] }
# Arguments as JSON values (raw pushes), and logging from jobs.
serde_json = "1"
tracing = "0.1"
# Showing hench's logs (`env-filter` for `EnvFilter`).
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
# Optional: reading the data API's streams, the dashboard, and the axum application that
# serves it or that the workers run next to.
futures = "0.3"
hench-web = "0.1"
axum = "0.8"
```

Features of `hench`: `macros` (on by default: `#[hench::job]`), `periodic` (on by default:
[periodic jobs](periodic-jobs.md), which bring a cron parser and a time-zone database), `tls`
(`rediss://` URLs), `cli` (worker command line and configuration file, see
[Configuration](configuration.md#command-line-and-configuration-file)), `test-util` (a private
`redis-server` per test). Tests that pause tokio's clock (`#[tokio::test(start_paused = true)]`,
for `TestLimits::InMemory`) also need tokio's own `test-util` feature in `[dev-dependencies]`.
Every example of this guide builds with these dependencies (plus the `cli` feature and `clap`
where noted).

## 2. Define a job

```rust
use hench::prelude::*;
use serde::{Deserialize, Serialize};

/// Arguments can be any serde types.
#[derive(Serialize, Deserialize)]
pub struct Invoice {
    pub id: u64,
    pub amount_cents: i64,
}

/// Charges an invoice. Runs on the `billing` queue; retried up to 10 times.
#[hench::job(queue = "billing", retry = 10, tags = ["payments"])]
pub async fn charge_invoice(
    ctx: &JobContext,
    invoice: Invoice,
    customer: String,
) -> Result<(), std::io::Error> {
    println!(
        "{}: charging {} cents to {customer} (attempt {:?})",
        ctx.jid(),
        invoice.amount_cents,
        ctx.retry_count()
    );
    Ok(())
}

fn main() {}
```

The macro turns the function into a unit struct named after it that implements `Job` and
registers itself, so workers find it without further setup. Its arguments become a tuple:
`(Invoice, String)` here.

Registration happens in the binaries that link the crate defining the job, and Rust links only
the crates a binary refers to. With the jobs in a library (shared by the web application and
the workers) and a worker binary whose `main` only builds and runs `Hench`, the library is left
out, and the worker knows no job: every job it takes fails with `hench::UnknownJob`. Refer to
the library from the worker, for example with `use my_jobs as _;`. Workers list the jobs they
know in their start-up log, and warn when there are none.

## 3. Enqueue jobs

```rust,no_run
use std::time::Duration;

use hench::prelude::*;

#[hench::job(queue = "reports")]
async fn monthly_report(account_id: u64, month: String) {
    println!("report of {month} for account {account_id}");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // A client is cheap to clone; keep one in your application state.
    let client = Client::new(RedisPool::connect("redis://127.0.0.1:6379").await?);

    // Now, in five minutes, or at a given time.
    monthly_report::perform_async(&client, (7, "2026-10".to_owned())).await?;
    monthly_report::perform_in(&client, Duration::from_secs(300), (7, "2026-09".to_owned()))
        .await?;
    let at = std::time::SystemTime::now() + Duration::from_secs(3_600);
    monthly_report::perform_at(&client, at, (8, "2026-10".to_owned())).await?;

    // With options for this push only.
    monthly_report::set()
        .queue("critical")
        .retry(false)
        .perform_async(&client, (9, "2026-10".to_owned()))
        .await?;

    // Many at once (one round trip per 1,000 jobs).
    let accounts = (1..=500).map(|id| (id, "2026-10".to_owned())).collect();
    monthly_report::perform_bulk(&client, accounts).await?;
    Ok(())
}
```

## 4. Run workers

A standalone worker process:

```rust,no_run
use hench::prelude::*;
use tracing_subscriber::EnvFilter;

#[hench::job(queue = "images")]
async fn make_thumbnail(path: String) {
    println!("thumbnail of {path}");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Hench logs through `tracing`: job starts, ends and failures at `info` and `warn`. (With
    // `env-filter`, `tracing_subscriber::fmt::init()` alone logs errors only unless RUST_LOG
    // is set, which would hide failing jobs.)
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let hench = Hench::builder()
        .redis_url("redis://127.0.0.1:6379")
        .concurrency(10)
        .queues(["critical", "images", "default"]) // checked in this order
        .build()
        .await?;
    // SIGINT/SIGTERM: stop (graceful, then requeue what is left); SIGTSTP: quiet;
    // SIGTTIN: log the running jobs.
    hench.run().await?;
    Ok(())
}
```

## 5. Or embed them next to your web application

Workers and the dashboard run in the same process as an axum server; a shutdown signal stops
both, each finishing its work in flight, and so does a stop sent to the workers from the
dashboard or `hench stop` (the application then exits, for its process manager to restart):

```rust,no_run
use axum::Router;
use axum::routing::get;
use hench::CancellationToken;
use hench::prelude::*;
use hench_web::HenchWeb;

#[hench::job]
async fn index_document(document_id: u64) {
    println!("indexing {document_id}");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let hench = Hench::builder()
        .redis_url("redis://127.0.0.1:6379")
        .build()
        .await?;
    let client = hench.client().clone();
    let app = Router::new()
        .route(
            "/index/{id}",
            get(move |axum::extract::Path(id): axum::extract::Path<u64>| {
                let client = client.clone();
                async move {
                    match index_document::perform_async(&client, (id,)).await {
                        Ok(_) => "queued",
                        Err(_) => "Redis is not available",
                    }
                }
            }),
        )
        // The dashboard, built from the `Hench` so that it uses its middleware and limits.
        // It has no authentication of its own: put it behind your application's before
        // serving it to anyone but yourself (see the guide's "Dashboard" page).
        .nest_service("/jobs", hench_web::router(HenchWeb::new(&hench)));

    // Cancelled by a signal, or when either side stops on its own.
    let shutdown = CancellationToken::new();
    tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            shutdown_signal().await;
            shutdown.cancel();
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
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
    let (served, ran) = tokio::join!(server, workers);
    served?;
    ran?;
    Ok(())
}

/// Ctrl-C, or SIGTERM (how systemd and Kubernetes stop a process).
async fn shutdown_signal() {
    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut terminate) => {
                terminate.recv().await;
            }
            Err(_) => std::future::pending().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        () = terminate => {}
    }
}
```

[`crates/hench/examples/axum_embedded.rs`][example-axum-embedded] and
[`crates/hench-web/examples/embedded.rs`][example-web-embedded] are complete versions;
[`crates/hench/examples/worker.rs`][example-worker] is a worker binary configured by its command
line. With the features (unique jobs, batches, throttles and schedules), see
[`crates/hench/examples/features.rs`][example-features].

---

[Guide](README.md) · [Defining and enqueuing jobs →](jobs.md)

[example-axum-embedded]: ../../crates/hench/examples/axum_embedded.rs
[example-web-embedded]: ../../crates/hench-web/examples/embedded.rs
[example-worker]: ../../crates/hench/examples/worker.rs
[example-features]: ../../crates/hench/examples/features.rs
