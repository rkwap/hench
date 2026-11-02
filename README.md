# Hench

Background jobs for Rust: typed jobs, Redis-backed queues, retries with backoff, scheduled
jobs, a web dashboard, metrics and a command-line tool. Hench runs on [tokio] and follows the
architecture, Redis data model and semantics of [Sidekiq] 8.1, the job system that has
carried some of the busiest Ruby applications for more than a decade.

```rust,no_run
use hench::prelude::*;
use tracing_subscriber::EnvFilter;

/// Sends the welcome email of a new user. Retried up to 5 times if it fails.
#[hench::job(queue = "mailers", retry = 5)]
async fn welcome_email(user_id: i64) -> Result<(), std::io::Error> {
    println!("welcome, user {user_id}!");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Logs at `info` unless RUST_LOG says otherwise: job starts, ends and failures.
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let hench = Hench::builder()
        .redis_url("redis://127.0.0.1:6379")
        .queues(["mailers", "default"])
        .build()
        .await?;
    // Enqueue from anywhere that has a client...
    welcome_email::perform_async(hench.client(), (42,)).await?;
    // ...and process jobs until SIGINT or SIGTERM.
    hench.run().await?;
    Ok(())
}
```

## Contents

- [Why Hench](#why-hench)
- [Features](#features)
- [Quick start](#quick-start)
- [Defining jobs](#defining-jobs)
- [Enqueuing jobs](#enqueuing-jobs)
- [Unique jobs](#unique-jobs)
- [Periodic jobs](#periodic-jobs)
- [Configuration](#configuration)
- [Queues and weights](#queues-and-weights)
- [Retries, the dead set and scheduled jobs](#retries-the-dead-set-and-scheduled-jobs)
- [Delivery guarantees: reliable and basic fetch](#delivery-guarantees-reliable-and-basic-fetch)
- [Middleware](#middleware)
- [Testing](#testing)
- [Metrics](#metrics)
- [Iterable jobs](#iterable-jobs)
- [Batches and workflows](#batches-and-workflows)
- [Rate limiting and throttling](#rate-limiting-and-throttling)
- [Using the features together](#using-the-features-together)
- [The dashboard](#the-dashboard)
- [The `hench` command](#the-hench-command)
- [Deployment](#deployment)
- [Differences from Sidekiq](#differences-from-sidekiq)
- [Minimum supported Rust version](#minimum-supported-rust-version)
- [Roadmap](#roadmap)
- [License and credits](#license-and-credits)

## Why Hench

- **A proven design.** Queues are Redis lists, retries, scheduled and dead jobs are sorted
  sets, processes report through heartbeats: the model Sidekiq has refined since 2012. Hench
  keeps its data layout, its retry formula, its quiet/stop lifecycle and its dashboard's
  pages, and its test suite ports Sidekiq's test scenarios one by one.
- **Typed jobs.** A job is a Rust type (or an `async fn` with `#[hench::job]`): arguments are
  serde types checked when you enqueue, not stringly typed hashes. Panics are caught and
  become ordinary failures, so a job never takes a worker down (with Rust's default
  `panic = "unwind"`, see [Deployment](#deployment)).
- **Safe by default.** Reliable fetch is the default: a job taken from a queue sits in its
  process's in-progress list until it is acknowledged, so a crashed process loses nothing.
  Outcome writes (acknowledgements, retries, the dead set) are retried through Redis outages,
  and API moves (retry, kill, enqueue now) are atomic Lua scripts.
- **Batteries included.** A dashboard you mount in your axum application, a standalone server
  and CLI, execution metrics with deploy marks, resumable iterable jobs, systemd
  notifications, a worker command line with a TOML configuration file, and testing modes that
  need no Redis.

## Features

- Jobs from `async fn`s (`#[hench::job]`) or `impl Job`, with options per job: queue, retries
  (count or time limit), backtraces, tags, dead-set opt-out, log level.
- Enqueue now, in a while, at a time, or in bulk (with spreading); custom payload fields;
  raw pushes of jobs other services consume.
- Workers with any concurrency, several *capsules* (groups of workers with their own queues
  and concurrency), strict, weighted or random queue order, and pausable queues.
- Retries with exponential backoff and jitter, `retry_in` and `retries_exhausted` hooks, a
  dead set ("morgue") trimmed by age and size, death and error handlers.
- Scheduled jobs and a scheduler that adapts its polling to the number of processes.
- Reliable fetch with orphan recovery, or basic fetch; graceful and hard shutdowns that
  requeue unfinished jobs.
- Client and server middleware chains.
- Lifecycle hooks (startup, quiet, shutdown, exit, heartbeat).
- A data API (`hench::api`) to inspect and change queues, retries, scheduled and dead jobs,
  processes and running jobs.
- Testing modes: capture jobs in memory (`Client::fake()`), run them as they are pushed
  (`Client::inline(..)`), or call one directly (`perform_inline`); a private `redis-server`
  per integration test (`hench::test_util`).
- Execution metrics per job class (counts, failures, time, histograms) and deploy marks.
- Iterable jobs that save their position and resume after an interruption, a failure or a
  deploy, and can be cancelled.
- Unique jobs (a lock per job and arguments, released when the job succeeds or starts, or when
  it expires; debouncing), periodic jobs (cron or intervals, time zones, exactly one push per
  tick across processes), batches and workflows (callbacks when a group of jobs completes or
  succeeds, nested batches, cancellation), and rate limiting (limiters inside jobs, throttles
  on job starts, shared by every process).
- A dashboard (`hench-web`): live and historical statistics, busy processes, queues, retries,
  scheduled and dead jobs with bulk actions, schedules, batches, limits and locks, metrics
  charts, dark mode, read-only mode, CSRF protection and a strict content security policy.
- The `hench` binary (`hench-server`): standalone dashboard with Basic authentication, a JSON
  API, and `stats`, `processes`, `queues`, `quiet`, `stop`, `enqueue`, `periodic`, `batches`,
  `limits` and `locks` commands.
- systemd `Type=notify` integration with watchdog keep-alives; TLS (`rediss://`), Unix
  socket and Redis Sentinel connections; per-job log levels through `tracing`.

The workspace has four crates:

| Crate | What it is |
|---|---|
| [`hench`][dir-hench] | The library: client, jobs, workers, API, metrics, iterable jobs, testing. |
| [`hench-macros`][dir-hench-macros] | The `#[hench::job]` attribute (re-exported by `hench`). |
| [`hench-web`][dir-hench-web] | The dashboard, as an `axum::Router`. |
| [`hench-server`][dir-hench-server] | The `hench` binary: standalone dashboard, JSON API and CLI. |

[dir-hench]: https://github.com/rkwap/hench/tree/v0.1.0/crates/hench
[dir-hench-macros]: https://github.com/rkwap/hench/tree/v0.1.0/crates/hench-macros
[dir-hench-web]: https://github.com/rkwap/hench/tree/v0.1.0/crates/hench-web
[dir-hench-server]: https://github.com/rkwap/hench/tree/v0.1.0/crates/hench-server

## Quick start

You need Redis 6.2 or newer (Redis-compatible servers that implement the same commands,
such as Valkey, should work too, though they are not tested yet: see the [roadmap](#roadmap)).

### 1. Add the dependencies

The crates are published on [crates.io](https://crates.io/crates/hench): `hench = "0.1"` adds
the library (with `#[hench::job]`, from `hench-macros`), `hench-web = "0.1"` the dashboard, and
`cargo install hench-server --locked` installs the [`hench` command](#the-hench-command). An
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
[periodic jobs](#periodic-jobs), which bring a cron parser and a time-zone database), `tls`
(`rediss://` URLs), `cli` (worker command line and configuration file, see
[below](#command-line-and-configuration-file)), `test-util` (a private `redis-server` per
test). Tests that pause tokio's clock (`#[tokio::test(start_paused = true)]`, for
`TestLimits::InMemory`) also need tokio's own `test-util` feature in `[dev-dependencies]`. Every example below builds with these dependencies (plus the `cli` feature and `clap`
where noted).

### 2. Define a job

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

### 3. Enqueue jobs

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

### 4. Run workers

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

### 5. Or embed them next to your web application

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
        // serving it to anyone but yourself (see "The dashboard" below).
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

[example-axum-embedded]: https://github.com/rkwap/hench/blob/v0.1.0/crates/hench/examples/axum_embedded.rs
[example-web-embedded]: https://github.com/rkwap/hench/blob/v0.1.0/crates/hench-web/examples/embedded.rs
[example-worker]: https://github.com/rkwap/hench/blob/v0.1.0/crates/hench/examples/worker.rs
[example-features]: https://github.com/rkwap/hench/blob/v0.1.0/crates/hench/examples/features.rs

## Defining jobs

### With `#[hench::job]`

```rust
use hench::prelude::*;

/// Every option, for reference.
#[hench::job(
    name = "billing::SyncLedger", // stored in the job's `class` (default: the function name)
    queue = "billing",            // default queue (default: "default")
    retry = 3,                    // true (25 retries), false (none) or a count
    backtrace = 20,               // store the error's backtrace: true, false or a line count
    dead = false,                 // do not keep it in the dead set once out of retries
    tags = ["ledger", "nightly"], // shown in the dashboard
    retry_for = 86_400,           // retry for a day instead of counting (seconds)
    retry_queue = "billing-retries",
    log_level = "warn",           // the job logger's level for this job
    // Unique jobs (see "Unique jobs"): one waiting or running copy per ledger for 10 minutes.
    unique_for = 600,             // seconds
    unique_until = "success",     // or "start", "expired"
    on_conflict = "drop",         // or "error", "replace"
    unique_across_queues = false,
    unique_by = [ledger_id],      // the arguments that identify a duplicate (default: all)
    // Throttles (see "Rate limiting and throttling"): a throttle defined in the builder or the
    // configuration file, keyed by ledger; `throttle(concurrency = 3, window_per_key = "5/1s")`
    // defines one with the job instead.
    throttle = "ledgers",
    throttle_by = [ledger_id],
)]
async fn sync_ledger(ctx: &JobContext, ledger_id: u64) -> Result<(), JobError> {
    if ctx.is_stopping() {
        // The process is shutting down: a job failing with the shutdown class is requeued
        // for another process instead of retried.
        return Err(JobError::new(JobError::SHUTDOWN, "not started"));
    }
    // `?` converts any error into a `JobError` that records the error's type.
    let entries: u32 = format!("{}", ledger_id % 100).parse()?;
    println!("syncing {entries} entries of ledger {ledger_id}");
    Ok(())
}

fn main() {}
```

- The optional first parameter `ctx: &JobContext` gives the job's id (`jid()`), queue,
  payload, retry count, tags, a client to enqueue more jobs, shared data
  (`ctx.data::<T>()`) and the shutdown signal (`ctx.stopping()`, `ctx.is_stopping()`).
- The other parameters are the arguments: owned types that serialize to JSON. The job's
  `Args` type is their tuple (`()` for none, `(A,)` for one).
- The function returns `()` or `Result<(), E>` for any error type `anyhow` accepts
  (`std::io::Error`, your own errors, `anyhow::Error`, `hench::JobError`). `String` and
  `Box<dyn Error>` are not error types: convert them with `.map_err(JobError::msg)` or
  `.map_err(JobError::from_boxed)` (the compiler's error says so).
- The name is what is stored in Redis: keep it stable while jobs of the old name may still be
  queued. Duplicate names are refused when the workers start, with both types named. A
  `#[hench::job]` registers itself: registering it with `register` too is refused as well
  (unless `auto_register(false)`).

### By hand

Implementing `Job` gives the full control the macro hides: per-job retry delays, what happens
when retries run out, and state in the job value itself.

```rust,no_run
use std::sync::Arc;
use std::time::Duration;

use hench::prelude::*;

/// Shared by every job through `ctx.data::<Database>()`.
struct Database {
    url: String,
}

struct ImportFeed {
    /// Jobs are shared by every execution: per-run state lives in locals.
    endpoint: Arc<str>,
}

impl Job for ImportFeed {
    const NAME: &'static str = "ImportFeed";
    type Args = (String, u32); // feed name, page

    fn options() -> JobOptions {
        JobOptions::new().queue("imports").retry(8).backtrace(true)
    }

    async fn perform(&self, ctx: &JobContext, (feed, page): (String, u32)) -> Result<(), JobError> {
        let database = ctx.data::<Database>()?;
        println!("{}: importing {feed} page {page} into {}", self.endpoint, database.url);
        Ok(())
    }

    /// The delay before retry `count` (the default is `count⁴ + 15` seconds plus jitter).
    fn retry_in(&self, count: u32, _error: &JobError, _job: &JobPayload) -> RetryIn {
        RetryIn::After(Duration::from_secs(30 * u64::from(count + 1)))
    }

    /// Out of retries: keep the job in the dead set (the default) or discard it.
    async fn retries_exhausted(&self, job: &JobPayload, error: &JobError) -> Exhausted {
        eprintln!("{} gave up: {error}", job.jid);
        Exhausted::Default
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let hench = Hench::builder()
        .data(Database { url: "postgres://db/feeds".to_owned() })
        .register(ImportFeed { endpoint: Arc::from("https://feeds.example") })
        .queues(["imports"])
        .build()
        .await?;
    ImportFeed::perform_async(hench.client(), ("news".to_owned(), 1)).await?;
    hench.run().await?;
    Ok(())
}
```

`RetryIn` can also `Discard` the job or `Kill` it (straight to the dead set). Errors carry a
class (the Rust type name of the error, `panic` for panics) that the dashboard and the error
handlers see.

## Enqueuing jobs

| Call | What it does |
|---|---|
| `J::perform_async(&client, args)` | enqueue now |
| `J::perform_in(&client, delay, args)` / `J::perform_at(&client, time, args)` | schedule |
| `J::perform_bulk(&client, vec_of_args)` | many jobs, few round trips |
| `J::set().spread_interval(d).perform_bulk(..)`, `J::set().perform_bulk_at(&client, vec_of_(args, time))` | many jobs spread over an interval, or each at its own time |
| `J::set().queue(..).retry(..).tags(..).custom(key, value)...perform_async(..)` | one-off options |
| `client.job::<J>().delay(..).perform_async(args)` | the same, from the client |
| `client.push(JobPayload::new("Class", vec![json!(1)]))` | a raw job, by class name |
| `client.push_bulk(BulkPush::new("Class", args).spread_interval(..))` | raw bulk push |
| `J::perform_inline(args)` | run the job here and now, without Redis (macro jobs) |

Every push returns the job's id (`jid`), or `None` when client middleware stopped it. The
client retries once on failovers and dropped connections; scheduled jobs live in the
`schedule` sorted set until they are due.

An error from a push does not always mean that the job was not pushed: when Redis does not
answer in time (a stall longer than the response timeout, 3 s: a slow fork, a failover) or the
connection drops after the push was sent, Redis may have run it, and the job then runs although
the call returned an error. So pushing again after an error can enqueue the job twice (as can
the client's own retry after a dropped connection, with the same jid). Make jobs idempotent,
which at-least-once delivery asks anyway, and give jobs you may push again a jid of your own
(`J::set().jid(..)`) so that they can tell a duplicate. `Client`'s documentation has the
details.

## Unique jobs

A unique job holds a **lock** while it waits and runs: pushing a job with the same class,
queue and arguments meanwhile is a **duplicate**, which is not pushed (`perform_async` and
`push` return `Ok(None)`, like Sidekiq Enterprise). The lock is taken by the Lua script that
writes the job and held or released by the scripts that write its outcome, so a push that fails
leaves no lock, a lock is never released before its job's outcome is written, and a push
resent after a lost reply is not pushed twice. Every lock expires: `unique_for` is mandatory.

```rust
use std::time::Duration;

use hench::prelude::*;

#[hench::job(queue = "sync")]
async fn sync_address(user_id: i64, reason: String) {
    println!("syncing the address of user {user_id} ({reason})");
}

#[tokio::main]
async fn main() -> hench::Result<()> {
    // Testing clients keep the locks in memory; a Redis client takes them in Redis.
    let client = Client::fake();
    let unique = || sync_address::set().unique_for(Duration::from_secs(600));

    let first = unique().perform_async(&client, (42, "form".to_owned())).await?;
    assert!(first.is_some());
    // The same arguments while the first job waits or runs: a duplicate, not pushed.
    assert_eq!(unique().perform_async(&client, (42, "form".to_owned())).await?, None);
    // `push_outcome` names the holder.
    let outcome = unique().push_outcome(&client, (42, "form".to_owned())).await?;
    assert!(matches!(outcome, hench::PushOutcome::Duplicate { holder, .. } if Some(&holder) == first.as_ref()));
    // Other arguments take another lock.
    assert!(unique().perform_async(&client, (7, "form".to_owned())).await?.is_some());

    // Once the job succeeded, its lock is released.
    client.drain::<sync_address>().await?;
    assert!(unique().perform_async(&client, (42, "form".to_owned())).await?.is_some());
    Ok(())
}
```

When the lock is released (`UniqueUntil`):

| Policy | Released | For |
|---|---|---|
| `Success` (default) | when the job succeeds, dies (or is discarded), or is deleted or killed from the dashboard or the API; kept and extended while a retry waits; renewed by the heartbeat while the job runs | at most one copy waiting or running |
| `Start` | when `perform` starts; taken back, if free, while a retry waits | at most one copy waiting: a change during a run triggers one follow-up job |
| `Expired` | only when `unique_for` ends, whatever the job did | at most one per period (not to be confused with [throttles](#rate-limiting-and-throttling), which limit how many start) |

Settings come from the client's `default_job_options`, the job type's options
(`JobOptions::unique_for` and the other `unique*` setters, or `#[hench::job(unique_for = 600,
unique_until = "start", on_conflict = "replace", unique_across_queues = true, unique_by =
[user_id])]`) and each push (`set()`, `client.job::<J>()`, `BulkPush`, raw
`JobPayload::unique`); each layer overrides the one before it field by field, and
`.not_unique()` turns uniqueness off for one push (the macro's other unique options need its
`unique_for`: without it they are a compile error). Duplicates are found by a digest (SHA-256)
of the class, the queue (left out with `unique_across_queues`) and the arguments, or the part
of them `Job::unique_key` returns (typed pushes, however the job became unique), or an explicit
`unique_key`:

```rust,no_run
use std::time::Duration;

use hench::api::Api;
use hench::prelude::*;
use serde_json::{Value, json};

/// Syncs a user's address with the CRM: one pending sync per user, whatever the reason.
#[derive(Default)]
struct SyncAddress;

impl Job for SyncAddress {
    const NAME: &'static str = "SyncAddress";
    type Args = (i64, String);

    fn options() -> JobOptions {
        JobOptions::new().queue("sync").unique_for(Duration::from_secs(600))
    }

    // The part of the arguments that makes a duplicate (all of them by default).
    fn unique_key((user_id, _reason): &Self::Args) -> Option<Value> {
        Some(json!([user_id]))
    }

    async fn perform(&self, _ctx: &JobContext, (user_id, reason): Self::Args) -> Result<(), JobError> {
        println!("syncing the address of user {user_id} ({reason})");
        Ok(())
    }
}

#[hench::job(queue = "search")]
async fn reindex(document_id: i64, version: u64) {
    println!("indexing version {version} of document {document_id}");
}

#[hench::job(queue = "mailers")]
async fn digest_email(user_id: i64) {
    println!("sending the daily digest to user {user_id}");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let redis = RedisPool::connect("redis://127.0.0.1:6379").await?;
    let client = Client::new(redis.clone());

    SyncAddress::perform_async(&client, (42, "form".to_owned())).await?;
    // The same user, another reason: a duplicate.
    assert_eq!(SyncAddress::perform_async(&client, (42, "retry".to_owned())).await?, None);

    // Debounce: index a document 30 s after its last change, with the last version. Each push
    // replaces the job still waiting in the schedule (a queued or running one is kept).
    reindex::set()
        .delay(Duration::from_secs(30))
        .unique_for(Duration::from_secs(60))
        .unique_key(json!([7]))
        .on_conflict(OnConflict::Replace)
        .perform_async(&client, (7, 12))
        .await?;

    // At most one per period: one digest per user and day, whatever happens to the job.
    let daily = Unique::new(Duration::from_secs(86_400)).until(UniqueUntil::Expired);
    digest_email::set().unique(daily).perform_async(&client, (42,)).await?;

    // Who holds the lock of user 42's sync? Release it by hand (the holder is compared).
    let locks = Api::new(redis).unique_locks();
    if let Some(lock) = locks.find::<SyncAddress>(&(42, String::new())).await? {
        println!("held by {} until {:?}", lock.jid, lock.expires_at);
        locks.release(&lock.digest, &lock.jid).await?;
    }
    Ok(())
}
```

- **Conflicts** (`on_conflict`): `Drop` (the default: `Ok(None)`), `Error` (the push fails with
  `Error::DuplicateJob`, naming the holder), `Replace` (the debounce above: the holder is
  replaced while it waits in the schedule, however many jobs share its run time — a bulk push
  with one delay, or every user's reminder at 09:00; anything but a holder waiting in the
  schedule makes the push a duplicate). `push_outcome` returns
  `PushOutcome::{Pushed, Replaced, Duplicate, Stopped}` whatever the strategy, but `Error`,
  whose duplicate fails the push with `Error::DuplicateJob` there too.
- **Choosing `unique_for`**: at least the longest time a job waits in its queue; at least
  three heartbeat intervals (30 s) for long jobs (the heartbeat renews the locks of running
  `Success` jobs); ten minutes or more where a duplicate after a crash matters (a crashed
  process's jobs are recovered after its heartbeat expires and the next orphan check). Retries
  extend a `Success` lock to the retry's time plus `unique_for`. A lock whose time runs out
  before its job lets a duplicate in: a **lapse**, counted per class and shown on the Locks
  tab, `hench locks` and `UniqueLocks::events`. Lapses mean `unique_for` is too short.
- **Uniqueness is not concurrency control**: with `Start`, a waiting copy can run next to a
  running one. For "at most one waiting and never two running", combine `Start` with a
  concurrency limit of one per key (`hench::limit`).
- **Where locks are not touched**: the scheduler, retries and moves from the dashboard keep the
  job's lock; a job moved to a later time (`SortedEntry::reschedule`, or the scheduler
  deferring a job it could not push) keeps it for the new wait, and one whose move client
  middleware stops is deleted, releasing it; `Queue::clear` and `JobSet::clear` leave locks to
  expire (`UniqueLocks::release_all` releases them); a job retried by hand from the dead set
  runs without a lock; a process that crashes keeps its jobs' locks until they expire or the
  recovered jobs finish (a job recovered more than `max_recoveries` times, a poison pill, goes
  to the morgue and releases its lock).
- **Seeing and releasing locks**: `Api::unique_locks()` (`page`, `find::<J>`, `release`,
  `force_release`, `release_all`, `events`), the dashboard's Locks tab (every lock, where its
  holder is, the last hour's duplicates and lapses, release one, a selection or all), `hench
  locks [list|show|release|release-all]`, `hench enqueue --unique-for 10m` and the JSON API
  (`GET <prefix>/api/v1/locks`, `DELETE <prefix>/api/v1/locks/{digest}?jid=`, `unique` in
  `POST <prefix>/api/v1/jobs`; `hench web --api`).
- **Testing**: `Client::fake()` and `Client::inline()` enforce locks in memory, per client
  (`Testing::unique_jobs(false)` turns that off); `client.unique_holder::<J>(&args)` tells who
  holds a lock. `HenchBuilder::unique_jobs(false)` (or `unique_jobs = false` in the
  configuration file) and `Client::with_unique_jobs(false)` make pushes ignore uniqueness.
- **Cost**: a unique push costs about three times the Redis CPU of a plain one; each lock
  takes about 440 bytes until it expires, plus a copy of the job while it waits in the schedule
  (what lets a debounce push find it at once). Jobs without uniqueness are pushed and
  acknowledged exactly as before.
- **Giving the lock up early**: `ctx.release_unique_lock()` frees the job's lock while it runs
  (for example once it has read the state it works on), whatever the policy, so that the next
  push is a job of its own. The job keeps no lock for the rest of its execution: a retry or a
  copy pushed back runs without one.
- **Upgrades**: deploy workers that understand unique jobs before clients push them (an older
  worker ignores the lock: it expires with its time to live).

## Periodic jobs

A **periodic job** (a *schedule*) enqueues a job at each **tick** of a timetable: a cron
expression in a time zone, or a fixed interval. Every worker process runs a small poller and no
process is the leader: a schedule's tick is enqueued **at most once**, whatever crashes, lost
replies or concurrent pollers do (the push and the schedule's advance are one compare-and-set
script), and **at least once** while some worker runs (a poller that dies mid-tick leaves a lease
that ends after a minute). Whether a tick is due is decided with Redis's clock, so clock skew
between hosts cannot fire a tick early or twice.

```rust
use std::time::{Duration, UNIX_EPOCH};

use hench::prelude::*;

#[hench::job(queue = "reports", retry = 3)]
async fn nightly_report(ctx: &JobContext) {
    // The tick the job is for (not when it runs): retries keep it.
    if let Some(tick) = ctx.periodic() {
        println!("report of the day before {:?}", tick.tick);
    }
}

#[tokio::main]
async fn main() -> hench::Result<()> {
    let nightly = Periodic::cron("nightly-report", "0 2 * * *")
        .tz("Europe/Paris")
        .job::<nightly_report>(())
        .description("The PDF report of the previous day");
    nightly.validate()?;
    assert_eq!(nightly.describe(), "At 02:00 (Europe/Paris)");
    // The next ticks after 2026-11-01 00:00 UTC: 02:00 in Paris is 01:00 UTC in winter.
    let after = UNIX_EPOCH + Duration::from_secs(1_793_491_200);
    let ticks = nightly.upcoming(after, 2)?;
    let at = |secs| UNIX_EPOCH + Duration::from_secs(secs);
    assert_eq!(ticks, [at(1_793_494_800), at(1_793_581_200)]);

    // Testing clients have no poller: a test pushes a tick's job the way the poller would.
    let client = Client::fake();
    nightly.enqueue(&client, ticks[0]).await?;
    let job = &client.jobs::<nightly_report>()[0];
    assert_eq!(job.periodic.as_ref().map(|meta| meta.name.as_str()), Some("nightly-report"));
    client.drain::<nightly_report>().await?;
    Ok(())
}
```

Schedules defined in code are registered by the workers when they start; schedules created at
runtime (one per tenant, say) go through the API (`Api::periodic().upsert`) or `hench periodic
add` (the dashboard lists every schedule and pauses, resumes, enqueues and deletes them, but
does not create or edit them yet: see the [roadmap](#roadmap)). Both kinds live in Redis and
fire the same way:

```rust,no_run
use std::time::Duration;

use hench::api::Api;
use hench::prelude::*;

#[hench::job(queue = "rollups")]
async fn rollup(hours: u32) {
    println!("rolling up the last {hours} hours");
}

#[hench::job(queue = "tenants")]
async fn sync_tenant(tenant: String) {
    println!("syncing {tenant}");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let hench = Hench::builder()
        .redis_url("redis://127.0.0.1:6379")
        .periodic(
            // Five minutes past every hour; after downtime, each missed hour, at most 24.
            Periodic::cron("hourly-rollup", "5 * * * *")
                .job::<rollup>((1,))
                .catch_up(CatchUp::All { max: 24 }),
        )
        .periodic(
            // A job of another service, by class name, every 30 seconds; never two waiting or
            // running at once (a tick whose previous job holds the lock is skipped).
            Periodic::every("refresh-rates", Duration::from_secs(30))
                .class("rates::Refresh", vec![])
                .queue("rates")
                .retry(false)
                .unique_for(Duration::from_secs(60))
                .catch_up(CatchUp::Skip),
        )
        .build()
        .await?;

    // Dynamic schedules.
    let schedules = Api::from(&hench).periodic();
    let tenant = Periodic::cron("tenant-acme-sync", "*/15 * * * *")
        .job::<sync_tenant>(("acme".to_owned(),));
    schedules.upsert(&tenant).await?; // created, updated, rescheduled or unchanged
    schedules.pause("tenant-acme-sync").await?; // its ticks are not enqueued nor missed
    schedules.unpause("tenant-acme-sync").await?; // from its next tick on: no backfill
    let jid = schedules.enqueue("tenant-acme-sync").await?; // once, now; same timetable
    println!("enqueued {jid:?}");
    // The code schedules appear once a worker registered them (when it starts).
    for entry in schedules.page(1, 100).await?.items {
        println!("{}: next {:?}, last {:?}", entry.name, entry.next, entry.last);
    }
    hench.run().await?;
    Ok(())
}
```

- **Timetables**: cron expressions of 5 fields, or 6 with seconds first; `@hourly`, `@daily`
  (`@midnight`), `@weekly`, `@monthly`, `@yearly` (`@annually`); lists, ranges, steps, names,
  `L` (last), `W` (nearest weekday), `#` (nth weekday). When both the day of the month and the
  day of the week are restricted, either matches (as in Vixie cron). `Periodic::every(name,
  interval)` ticks at whole multiples of the interval since the Unix epoch, shifted by
  `.offset(..)`: `every(1 h).offset(5 min)` ticks at five past every hour. `hench periodic next
  "0 2 * * *" --tz Europe/Paris` previews an expression in English with its next ticks.
- **Time zones and daylight saving time**: IANA zone names (`.tz("America/New_York")`; UTC by
  default) from the system's database, with the copy bundled with hench for a zone the system's
  lacks (older tzdata). Like Vixie cron, a tick at a fixed time — seconds, minutes and hours
  fields that do not start with `*`: `30 2 * * *`, but also lists and ranges such as
  `30 1,13 * * *` or `15 1-3 * * *` — that falls in the hour skipped in spring runs right after
  the change, and one in the hour repeated in autumn runs once; fields starting with `*`
  (`*/15 * * * *`, `0 * * * *`) follow real time. Use UTC for jobs that must run every 24 hours
  exactly. Give every host the same tzdata (and hench version): a process that computes another
  instant for a tick than the one stored treats the stored one as standing for its nearest tick
  (within three hours), so each period still runs once, and warns. A worker whose time-zone
  database lacks a schedule's zone leaves it to the workers that have it.
- **Missed ticks** (no worker running, Redis unreachable, everything paused for a deploy):
  `CatchUp::Latest` (the default) enqueues one job for the most recent missed tick;
  `CatchUp::Skip` enqueues a tick only within its deadline (5 minutes unless `.deadline(..)`
  says otherwise), so a missed tick never runs late; `CatchUp::All { max }` enqueues every missed
  tick, oldest first, at most `max`. `.deadline(..)` also bounds the other two. The number of
  ticks missed is counted on the schedule.
- **Slow jobs**: ticks keep coming while a job runs; to skip a tick while the previous job waits
  or runs, give the schedule `unique_for` about one period (unique jobs, above): the tick is then
  consumed as a `duplicate` and shows so in the history.
- **What a job knows**: `ctx.periodic()` gives the schedule's name, the tick and whether it was
  enqueued by hand; the job's payload carries them (`periodic: {"name", "tick", "manual"}`),
  through retries and recoveries. Client middleware runs for every tick's push, and may change,
  delay or stop it (a stopped push consumes the tick).
- **Deploys and groups**: when a worker starts, it registers its schedules in one script:
  new ones are created, changed ones updated (a changed timetable reschedules the next tick;
  other changes keep it), and those its code no longer defines are deleted; pauses, history and
  counters survive. Processes of a deploy in progress may disagree for a moment: the last one
  started wins, and the dashboard says so (it compares the crontabs the processes declare, names
  included). Applications sharing a Redis database keep their schedules apart with
  `.periodic_group("billing")` (a process registers and prunes only its group). A name a
  dynamic schedule already uses is a conflict, logged and left alone. Workers check their
  group's registration every minute and register again after Redis lost its data, or went back
  to older data (a restore from a backup, a failover to a replica that missed the
  registration).
- **Pollers**: every worker polls unless `.periodic_poller(false)`; idle, a poller costs one
  small script every 5 seconds at most (`.periodic_poll_interval(..)`), and each tick three round
  trips. `.periodic_lease(..)` (60 s) is how long a dying poller holds a tick back;
  `.periodic_history(..)` (10) how many handled ticks each schedule remembers.
- **Seeing and changing schedules**: `Api::periodic()` (`page`, `get`, `history`, `upsert`,
  `remove`, `pause`, `unpause`, `enqueue`, `summary`, `repair`), the dashboard's Periodic tab
  (each schedule's timetable, next run in its zone, last enqueue and last run, with banners when
  no worker polls, a schedule is overdue or a group's workers disagree; history; enqueue now,
  pause, resume, delete), `hench periodic [list|show|enqueue|pause|unpause|remove|add|repair|
  next]`, `hench stats periodic` and the JSON API (`GET <prefix>/api/v1/periodic`, `GET
  <prefix>/api/v1/periodic/{name}`, `POST <prefix>/api/v1/periodic/{name}/{enqueue|pause|
  unpause}`; `hench web --api`). Each schedule shows the outcome of its
  latest run (succeeded, failed with the error, interrupted, rescheduled) and its counters.
- **Configuration file**: `[[periodic]]` tables in the worker's TOML file (name, `cron` and
  `tz` or `every` and `offset`, `class`, `args`, `queue`, `retry`, `tags`, `catch_up` and
  `catch_up_max`, `deadline`, `paused`, `description`, `unique_for`, `unique_until`, `throttle`,
  `throttle_key`), next to the `periodic_*` settings. When the worker registers the job type the
  table names by `class`, the schedule takes that job type's options (queue, retries,
  uniqueness, throttle) and its `unique_key`/`throttle_key` of the table's `args`, under the
  table's own keys, as `Periodic::job` does (its `on_conflict` aside: a schedule's ticks drop
  duplicates); `build` refuses arguments that do not fit the job type. A class the worker does
  not register (another service's job) takes the table's keys alone: set its `queue`.
- **Testing**: `Periodic::validate`, `upcoming`, `describe` and `payload(tick)` check a
  definition; `enqueue(&client, tick)` pushes a tick's job through a fake or inline client (no
  poller runs in testing modes). Integration tests that cross hours, days or DST drive the
  workers' timetables with a `hench::periodic::ManualClock` (feature `test-util`) given to
  `HenchBuilder::data` and `PeriodicSet::with_clock`.
- **Upgrades**: schedules are stored with a format version; a worker that finds a newer one
  leaves it to the workers that read it, and the dashboard shows it as unreadable. A crontab's
  digest includes its schedules' names since this version: while the upgrade rolls out, the
  dashboard may show a group's old and new workers as disagreeing.

## Configuration

### The builder

`Hench::builder()` sets everything; the defaults are Sidekiq's.

| Method | Default | Meaning |
|---|---|---|
| `redis_url(url)`, `redis_config(cfg)`, `redis(pool)` | from the environment | where Redis is (see below) |
| `concurrency(n)` | 5 | jobs processed at once by the default capsule |
| `queues([..])` | `["default"]` | queues in priority order (`"name,weight"` for weights) |
| `weighted_queues([(name, weight)])` | | random order biased by weights |
| `capsule(name, \|c\| ..)` | | another group of workers with its own queues and concurrency |
| `timeout(d)` | 25 s | how long a shutdown waits for running jobs |
| `max_retries(n)` | 25 | retries of jobs with `retry: true` |
| `dead_max_jobs(n)`, `dead_timeout(d)` | 10,000, 180 days | dead-set limits |
| `fetch(FetchStrategy::Basic)` | reliable | see [delivery guarantees](#delivery-guarantees-reliable-and-basic-fetch) |
| `data(value)`, `data_arc(arc)` | | shared state for `ctx.data::<T>()` |
| `register(job)`, `register_default::<J>()`, `register_dyn(..)`, `auto_register(bool)` | macro jobs registered | jobs this process runs |
| `server_middleware(m)`, `client_middleware(m)` | | middleware |
| `error_handler(f)`, `clear_error_handlers()`, `death_handler(f)` | a logging handler | failure callbacks |
| `on_startup`, `on_quiet`, `on_shutdown`, `on_exit`, `on_heartbeat`, `on_beat` | | lifecycle callbacks (a startup callback that panics stops the start) |
| `on_startup_try(\|\| async { .. })` | | a check that stops the start when it returns an error: `start`/`run` return `Error::Startup` before any job is taken |
| `logged_job_attributes([..])` | none | payload keys (say a `request_id` a client middleware adds) shown on every log line of a job, as `attrs={"request_id":"..."}` |
| `backtrace_cleaner(\|lines\| ..)` | none | filters the backtraces stored with failed jobs (they already end where hench runs the job) |
| `tag(t)`, `label(l)`, `labels([..])` | | shown in the dashboard and `hench processes` |
| `default_job_options(opts)` | queue `default`, retries on | options of every job, merged over the defaults |
| `max_iteration_runtime(d)` | none | how long an iterable job runs before taking turns |
| `metrics(bool)`, `job_logging(bool)`, `systemd(bool)` | on, on, on (with `run()`, when `NOTIFY_SOCKET` is set) | features |
| `poll_interval_average(d)` | processes × 5 s | how often the scheduler looks for due jobs |
| `heartbeat_interval(d)`, `fetch_timeout(d)` | 10 s (at most 20 s), 2 s | internals, mostly for tests |
| `orphan_check_interval(d)` | 60 s | how often the cluster looks for the in-progress jobs of processes that died |
| `orphan_grace(d)` | 30 s | how long a process without a heartbeat must stay without one before its in-progress jobs are recovered |
| `max_recoveries(n)` | 3 | recoveries of a job from processes that died while running it before it is taken for a poison pill and moved to the morgue; a job recovered before comes back alone, one per orphan check (see [delivery guarantees](#delivery-guarantees-reliable-and-basic-fetch)) |
| `unique_jobs(bool)` | on | whether pushes honour uniqueness ([unique jobs](#unique-jobs)) |
| `periodic(p)`, `periodic_group(g)`, `periodic_poller(bool)` | none, `default`, on | schedules this process registers, their group, whether it fires due ones ([periodic jobs](#periodic-jobs)) |
| `periodic_poll_interval(d)`, `periodic_lease(d)`, `periodic_check_interval(d)`, `periodic_history(n)` | 5 s, 60 s, 60 s, 10 | the poller's timing and each schedule's history |
| `batch_ttl(d)`, `batch_linger(d)`, `batch_max_commit_jobs(n)`, `batch_commit_timeout(d)` | 30 days, 24 h, 10 000, 30 s | [batches](#batches-and-workflows): expiry, the jobs of one commit, its timeout |
| `batch_maintenance(bool)`, `batch_maintenance_interval(d)`, `batch_dispatch_grace(d)` | on, 30 s, 10 s | batch housekeeping |
| `throttle(t)` | none | a throttle jobs name ([rate limiting](#rate-limiting-and-throttling)) |
| `over_limit_reschedules(n)`, `over_limit_backoff(f)`, `over_limit_max_delay(d)`, `over_limit_when(f)` | 20, the refusal's slot, 24 h, none | jobs a limiter refuses while they run |

`build()` validates everything before connecting (empty queue names, duplicate jobs, zero
concurrency...) and returns `Error::Config` with an explanation.

### Redis

Without `redis_url`, workers read the URL from the variable named by `REDIS_PROVIDER`, else
`REDIS_URL`, else use `redis://127.0.0.1:6379`. URLs may be `redis://`, `rediss://` (feature
`tls`), `redis+unix://` or `unix://` (`?db=N&pass=...`). `RedisConfig` sets the timeouts
(3 s) and reconnection attempts, and `TlsConfig` a private CA or a client certificate for
mutual TLS. Passwords never appear in logs, nor in `RedisConfig`'s `Debug` output. Workers log
the URL they connect to and where it came from (`REDIS_URL`, the default...); an unreachable
server is tried again (six times, over about 7 s), while a wrong password or database fails at
once.

A server whose certificate cannot be verified, such as Heroku's Redis (self-signed, naming
another host), needs an explicit opt-out: `TlsConfig::new().danger_accept_invalid_certs(true)`
(Sidekiq's `verify_mode: VERIFY_NONE`) or a URL ending in `#insecure`
(`rediss://host:port/#insecure`, which works for the `hench` command too).
`danger_accept_invalid_hostnames(true)` still checks who signed the certificate. Either logs a
warning: anyone between the process and Redis can then pose as the server.

TLS goes through rustls with its `ring` crypto provider: when hench creates a `rediss://`
client and the process has no default rustls provider yet, it installs `ring` as the default,
so that rustls never has to pick between `ring` and another provider a dependency enabled
(such as aws-lc-rs, rustls's own default). To use another provider, install it first, e.g.
`rustls::crypto::aws_lc_rs::default_provider().install_default()` at the top of `main`.

With Redis Sentinel, give the sentinels and the name they monitor the primary under:
`redis+sentinel://:password@sentinel-1:26379,sentinel-2:26379/mymaster` (the password and an
optional `/db` after the name are the primary's; `?sentinel_password=...` is the sentinels').
Clients and workers ask the sentinels for the primary, and ask again after a failover (a
`READONLY` reply or a lost connection), so they follow it. TLS through Sentinel is not
supported yet (see the [roadmap](#roadmap)).

### Command line and configuration file

With the `cli` feature (`hench = { version = "0.1", features = ["cli"] }`, plus `clap = "4"`
for its `Parser` trait), `hench::cli::WorkerArgs` gives worker binaries Sidekiq's command line
(`-c`, `-q NAME[,WEIGHT]`, `-t`, `-C`, `-e`, `-g`, `-v`) and a TOML configuration file
(`-C path`, or `config/hench.toml` in the current directory when it exists). The command line
wins over the file, which wins over the builder:

```rust,no_run
use clap::Parser;
use hench::cli::WorkerArgs;
use hench::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let settings = WorkerArgs::parse().settings()?;
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(settings.log_directives()))
        .init();
    let builder = Hench::builder().queues(["critical", "default"]).concurrency(10);
    let hench = settings.apply(builder).build().await?;
    hench.run().await?;
    Ok(())
}
```

```toml
# config/hench.toml
concurrency = 10
queues = [["critical", 3], ["default", 2], "low,1"]
timeout = 25                   # seconds
tag = "billing"
labels = ["eu-west"]
max_retries = 25
fetch = "reliable"             # or "basic"
metrics = true
average_scheduled_poll_interval = 5 # seconds between scheduler polls, cluster-wide

[capsules.serial]              # a second capsule: one job at a time
concurrency = 1
queues = ["serial"]
# A [capsules.default] table would configure the capsule of the top-level keys (and win over
# them); -c and -q win over both.

[production]                   # used with `-e production` (or APP_ENV=production)
concurrency = 25
```

```sh
worker -c 20 -q critical,3 -q default,1 -e production -v
```

The whole file is checked when it is read, the sections of other environments included, and
`apply` logs which file and environment it uses. It warns when an environment chosen with `-e`
(or `APP_ENV`) selects nothing: no file was read (started from another directory, such as a
systemd unit without `WorkingDirectory=`, which starts in `/`) or the file has no such section.

## Queues and weights

- **Strict order** (`queues(["critical", "default", "low"])`): `critical` is always emptied
  first, then `default`, then `low`. A busy high-priority queue can starve the others.
- **Weighted** (`queues(["critical,3", "default,2", "low,1"])` or `weighted_queues`): each
  fetch draws a random order in which a queue comes first with probability
  `weight / total`; every queue gets its share.
- **Random**: all weights equal (`"a,1"`, `"b,1"`).
- **Capsules** separate concerns: `.capsule("serial", |c| c.concurrency(1).queues(["serial"]))`
  runs one `serial` job at a time next to the default capsule.
- **Pausing**: `api.queue("mailers").pause()` (or the dashboard) stops every process from
  fetching from the queue until it is resumed; jobs keep accumulating.

## Retries, the dead set and scheduled jobs

A failed job is retried with exponential backoff: retry `n` (from 0) waits `n⁴ + 15` seconds
plus a random jitter below `10 × (n + 1)` seconds, so 25 retries span about 20 days. Each
failure records `error_class`, `error_message` (and the backtrace when asked) in the job,
visible in the dashboard.

- `retry: false` drops the job at its first failure (death handlers still run); `retry: n`
  caps the retries; `retry_for` retries for a duration instead of a count; `retry_queue`
  sends retries elsewhere.
- Out of retries, `retries_exhausted` runs, then the job goes to the **dead set** (unless
  `dead: false`), where it stays for 180 days or until 10,000 newer dead jobs push it out.
  Death handlers (`.death_handler(..)`) run for every job that will not be retried.
- Error handlers (`.error_handler(..)`) see every failure with its context, for error
  reporting services.
- **Scheduled jobs** and **retries** sit in sorted sets scored by their due time; every
  process's scheduler polls them at a random interval averaging `processes × 5 s`, so the
  whole cluster checks about every 5 seconds, and enqueues due jobs through the client
  (client middleware runs again).
- **Timing precision.** A scheduled job or retry therefore reaches its queue a few seconds
  after it is due on average (up to about 10 s), like in Sidekiq. For tighter timing lower
  `average_scheduled_poll_interval` (for example to 1 s): every process then polls more
  often, which costs Redis a few more cheap commands per second. Jobs parked by a throttle or
  a limiter do not wait for that poll: the process that parked them wakes its scheduler when
  they are due.
- From the API or the dashboard, retries and dead jobs can be retried now, deleted or
  killed, and scheduled jobs enqueued early; all of these moves are atomic.

## Delivery guarantees: reliable and basic fetch

Hench delivers every job **at least once**: make jobs idempotent (a job can run again after
a crash, a lost reply or a hard shutdown, and a push that failed with a timeout may have
enqueued it, see [Enqueuing jobs](#enqueuing-jobs)).

| | Reliable fetch (default) | Basic fetch |
|---|---|---|
| Fetch | `LMOVE` from the queue into the process's `<identity>:inprogress` list | `RPOP` / `BRPOP` |
| Acknowledge | `LREM` from the in-progress list | nothing |
| Process crashes | its in-progress jobs are pushed back by another process once its heartbeat expires (60 s, checked every minute, and again after `orphan_grace`, 30 s: a live process re-creates a heartbeat Redis lost), about 1.5 to 2.5 minutes after the crash | the running jobs are lost |
| A job that crashes its process | recovered and run again up to `max_recoveries` times (3), alone after the first time, then moved to the morgue as a poison pill | the job is lost |
| Shutdown | unfinished jobs are requeued at the front of their queues | the same |
| Cost | one more list operation per job | the cheapest |

**Poison pills.** A job that kills the process running it (an out-of-memory kill, an abort, a
crash in foreign code) would otherwise be recovered and crash the next process, forever. Each
recovery from a dead process is counted per job (the key `recovered-<jid>`, kept 72 hours after
the last one); a job recovered more than `max_recoveries` times (3 by default; 0 never runs a job
again after its process died) goes to the morgue instead, with the error class
`hench::CrashLoop` and a message saying how often it was recovered and how many other jobs were
in flight at its last crash: its batch counts it dead, its unique lock is released, and the death
and error handlers of the process that found it run, once (a job with `dead: false` is dropped
instead). Fix the cause, then retry it from the morgue; a copy that still crashes its process
goes back there at its next recovery.

Every job running when a process dies is counted, the culprit and the jobs that merely ran
beside it. So that the next crash tells them apart, a job recovered for the first time goes back
to its queue at once (the usual case: a deploy, a lost machine), but a job recovered before — in
flight at an earlier crash too — comes back alone: such jobs wait in the `recovery-suspects`
sorted set, and an orphan check (every minute by default) releases one, the most recovered
first, once the one released before it is no longer waiting or running (or ten checks later);
the dashboard does not list that set. The poison pill then crashes its next process beside
first-time jobs only, and the jobs that ran beside it come back one by one and run: in a crash
loop they run minutes later rather than at once. When several jobs of one crash reach the limit
together, each gets one more recovery, alone, and the one that crashes its process again is the
poison pill. An iterable job forgets its recoveries when it saves progress, so a long iteration
that keeps making progress is never buried by unrelated crashes. Jobs pushed back at a shutdown
are never counted.

On `stop()` (or SIGTERM), a process quiets (fetches nothing more), waits up to `timeout` for
running jobs (which can watch `ctx.stopping()`), then interrupts the rest and requeues them;
a second signal interrupts immediately. Writing a job's outcome is retried for as long as
Redis is unreachable, so an outage delays acknowledgements and retries instead of losing
them. Paused queues are skipped by every fetch.

## Middleware

Middleware wraps pushes (client) and executions (server). Not calling `next` stops the job:
a client middleware returning `Ok(None)` cancels the push, a server middleware returning
early marks the job done.

```rust,no_run
use std::time::Instant;

use hench::prelude::*;

/// Logs how long every job takes.
struct Timing;

#[async_trait]
impl ServerMiddleware for Timing {
    async fn call(
        &self,
        ctx: &JobContext,
        job: &mut JobPayload,
        next: ServerNext<'_>,
    ) -> Result<(), JobError> {
        let started = Instant::now();
        let result = next.run(ctx, job).await;
        println!("{} took {:?}", job.class, started.elapsed());
        result
    }
}

/// Stamps every job with the tenant that enqueued it.
struct Tenant(&'static str);

#[async_trait]
impl ClientMiddleware for Tenant {
    async fn call(
        &self,
        mut job: JobPayload,
        _redis: &RedisPool,
        next: ClientNext<'_>,
    ) -> hench::Result<Option<JobPayload>> {
        job.extra.insert("tenant".to_owned(), self.0.into());
        next.run(job).await
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let hench = Hench::builder()
        .server_middleware(Timing)
        .client_middleware(Tenant("acme"))
        .build()
        .await?;
    hench.run().await?;
    Ok(())
}
```

Chains hold one middleware per type and support `add`, `prepend`, `insert_before`,
`insert_after`, `remove` and `exists` (`configure_server_middleware(|chain| ..)`).

## Testing

Testing clients run jobs without Redis. `Client::fake()` captures pushed jobs; `drain`
then runs them through the same execution path as the workers (middleware, the job span,
argument decoding):

```rust
use hench::prelude::*;

#[hench::job(queue = "mailers")]
async fn password_reset(email: String) -> Result<(), std::io::Error> {
    if email.contains('@') {
        Ok(())
    } else {
        Err(std::io::Error::other("not an email address"))
    }
}

#[tokio::main]
async fn main() -> hench::Result<()> {
    let client = Client::fake();
    password_reset::perform_async(&client, ("ada@example.com".to_owned(),)).await?;

    // What was enqueued, as stored.
    let jobs = client.jobs::<password_reset>();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].queue, "mailers");

    // Run them (jobs they enqueue are captured too).
    assert_eq!(client.drain::<password_reset>().await?, 1);
    assert!(client.jobs::<password_reset>().is_empty());

    // Or run one directly, errors included.
    assert!(password_reset::perform_inline(("nobody".to_owned(),)).await.is_err());
    Ok(())
}
```

- `hench::testing::Testing` builds testing clients with your registrations, shared data and
  middleware, in fake or inline mode (`Client::inline` runs each job as it is pushed).
  `Testing::from_builder(configure()).await?` takes them from the builder your program uses,
  without connecting to Redis (`Testing::from_hench(&hench)` copies a built `Hench`, but
  building one connects).
- With the `test-util` feature, `hench::test_util::TestRedis::start().await` runs a private
  `redis-server` for one test (on a Unix socket, killed when dropped), with `pool()`,
  `client()` and `builder()` (a `HenchBuilder` with short intervals) for end-to-end tests.
- The pro features in testing clients: what is decided at push time is enforced, what depends
  on time is not. Unique locks are held and released as workers would (a duplicate push
  returns `None`; `Testing::unique_jobs(false)` turns them off); a batch's commit captures its
  members, a drain counts their outcomes and then runs the callbacks the batch fired (an inline
  client runs the members at the commit). Limiters and throttles grant everything unless
  `Testing::limits(TestLimits::InMemory)` enforces them on tokio's clock (a refused job stays
  captured until its slot, with its `limited` field; `Client::limited_jobs` lists them).
  Schedules have no poller: `Periodic::payload(tick)` gives the job a tick would push, and
  `upcoming` the next ticks.

## Metrics

Workers count every execution per job class (executions, failures, milliseconds and a
26-bucket histogram of durations) and add the counts to Redis on every heartbeat: minute
buckets kept 8 hours and ten-minute buckets kept 3 days. `.metrics(false)` turns it off.

```rust,no_run
use hench::metrics::{Deploy, Period, Query, SortBy};
use hench::RedisPool;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let redis = RedisPool::connect("redis://127.0.0.1:6379").await?;

    // Mark deploys: the dashboard's charts show them.
    let label = Deploy::git_label().unwrap_or_else(|| "manual deploy".to_owned());
    Deploy::new(&redis).mark(&label, std::time::SystemTime::now()).await?;

    // The busiest job classes of the last 4 hours.
    let result = Query::new(&redis).top_jobs(Period::Minutes(240), None).await?;
    for (class, metrics) in result.sorted(SortBy::TotalTime).into_iter().take(5) {
        println!(
            "{class}: {} runs, {} failed, {:.1} s, {:.1} ms on average",
            metrics.totals.processed,
            metrics.totals.failed,
            metrics.totals.seconds(),
            metrics.totals.average_ms()
        );
    }

    // One class, with its execution-time histogram.
    let one = Query::new(&redis).for_job("ImportFeed", Period::Minutes(60)).await?;
    println!("{:?}", one.jobs["ImportFeed"].histogram());
    Ok(())
}
```

The dashboard's Metrics pages chart the same data (execution time per class over 1 hour to 3
days, and per class the executions, the distribution of durations and a heat map of
durations per minute, with deploy marks), and `hench stats metrics` prints the busiest
classes of the last hour.

## Iterable jobs

An iterable job works through a long sequence (rows, pages, files) one item at a time. It
saves its cursor regularly, so it resumes where it stopped after a shutdown, a failure or
when it used up its run time and went back to its queue to let other jobs run:

```rust,no_run
use std::time::Duration;

use hench::iterable::{Enumerator, IterableJob, Iteration};
use hench::prelude::*;

/// Re-indexes every product, a page of ids at a time.
#[derive(Default)]
struct ReindexProducts;

impl IterableJob for ReindexProducts {
    const NAME: &'static str = "ReindexProducts";
    type Args = (u64,); // how many products
    type Item = Vec<u64>; // a page of product ids
    type Cursor = u64; // the last product id handled

    fn build_enumerator<'a>(
        &'a self,
        _ctx: &'a JobContext,
        (count,): &'a (u64,),
        cursor: Option<u64>,
    ) -> Result<Enumerator<'a, Vec<u64>, u64>, JobError> {
        let count = *count;
        // Keyset pagination: each page starts after the cursor (a database query in practice).
        Ok(Enumerator::batches(cursor, move |after| async move {
            let first = after.map_or(1, |id| id + 1);
            Ok((first..=count).take(100).map(|id| (id, id)).collect())
        }))
    }

    async fn each_iteration(&self, page: Vec<u64>, it: &Iteration<'_, Self>) -> Result<(), JobError> {
        println!("execution {}: page from {:?}", it.executions(), page.first());
        Ok(())
    }

    /// Give other jobs a turn every 30 seconds.
    fn max_iteration_runtime(&self) -> Option<Duration> {
        Some(Duration::from_secs(30))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let hench = Hench::builder().register(ReindexProducts).build().await?;
    let jid = ReindexProducts::perform_async(hench.client(), (1_000_000,)).await?;
    if let Some(jid) = jid {
        // Changed our mind: the job stops at its next checkpoint.
        hench.client().cancel(&jid).await?;
    }
    hench.run().await?;
    Ok(())
}
```

Items run at least once (a crash resumes from the last saved cursor), so make
`each_iteration` idempotent. Hooks (`on_start`, `on_resume`, `on_stop`, `on_cancel`,
`on_complete`) and `Iteration::abort` complete the picture; the dashboard shows each running
or retrying iterable job's cursor and executions.

## Batches and workflows

A **batch** groups jobs and runs callback jobs when they are done (`hench::batch`): `complete`
once every job ran at least once, `success` once every job succeeded, `death` at the first job
that will not run again (the morgue, retries disabled, a discard, a delete or kill through the
API). Each job is counted once, however often it runs or is reported, and each event fires
once, even with several processes, lost replies, crashes and Redis failovers. A batch is
defined through a collecting client: every push is buffered, and the commit writes the batch,
its callbacks and every job in one atomic step (nothing is written when the definition fails).

```rust,no_run
use hench::prelude::*;

#[hench::job(queue = "imports")]
async fn import_row(import_id: i64, row: u32) -> Result<(), JobError> {
    println!("importing row {row} of import {import_id}");
    Ok(())
}

#[hench::job(queue = "critical")]
async fn import_finished(ctx: &JobContext, import_id: i64) -> Result<(), JobError> {
    let callback = ctx
        .batch_callback()
        .ok_or_else(|| JobError::msg("not a callback"))?;
    if let Some(status) = callback.status(ctx).await? {
        println!(
            "import {import_id}: {} of {} rows failed",
            status.failures, status.total
        );
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // `#[hench::job]` functions register themselves: no `register` call.
    let hench = Hench::builder()
        .queues(["imports", "critical"])
        .build()
        .await?;
    let committed = Batch::new(hench.client())
        .description("Import #7")
        .on_complete::<import_finished>((7,))
        .jobs(|jobs| async move {
            for row in 0..1_000 {
                import_row::perform_async(&jobs, (7, row)).await?;
            }
            Ok::<_, hench::Error>(())
        })
        .await?;
    println!("batch {} has {} jobs", committed.bid, committed.jids.len());
    hench.run().await?;
    Ok(())
}
```

- **Nesting**: a batch defined with the collecting client of another (`Batch::new(&jobs)`) is
  its child, committed with it; the child's callbacks are jobs of the parent, so the parent's
  `complete` and `success` wait for them, and a child's death is its parent's.
- **Adding jobs later**: `ctx.batch()` reopens a job's own batch (a job of a child batch's
  callback reopens the parent), `Batch::open(&client, bid)` any batch that has not succeeded or
  been cancelled. Give each loader its own `.commit_id(..)`: a loader that runs twice adds its
  jobs once. `Batch::with_bid(&client, ctx.jid())` with the same commit id creates a batch once
  from a job that may run twice: its commit ids stay recorded for `batch_ttl` (30 days) after
  the batch finished, so a re-run days later (a late retry, a job revived from the morgue)
  still writes nothing. One commit holds at most `batch_max_commit_jobs` jobs (10 000; at most
  100 000), each child batch counting as 10; larger batches use loader jobs (staged commits
  are planned for 0.2: see the [roadmap](#roadmap)). A commit is one
  Redis script, which every other client waits for: about 4 µs per job, so some 40 ms for
  10 000 jobs and 0.4 s for 100 000 (raise the limit only with response timeouts well above
  that).
- **Cancelling**: `client.cancel_batch(bid)` (or the dashboard) cancels a batch and its
  children: jobs not run yet are skipped (counted as processed), `success` never fires,
  `complete` fires once the jobs left have run; a long job can stop early with
  `ctx.check_batch().await?` (an iterable job checks at its checkpoints from 0.2: see the
  [roadmap](#roadmap)). A cancelled batch keeps its data until its last job left is
  skipped (a job waiting for a retry, or revived from the morgue, is skipped when it comes
  up), then lingers.
- **Workflows**: `hench::batch::Workflow` chains stages of parallel jobs, each stage starting
  once the previous one succeeded, with the batch callbacks of the whole (`on_complete` once
  every job of every stage ran, `on_success` once the last stage succeeded, `on_death` at the
  first job that will not run again, which stops the workflow at its stage). Between stages an
  internal job, `hench::WorkflowNext` (known to every worker, nothing to register), starts the
  next stage: it runs in the queue of the first job of the stage that just succeeded, or in
  the workflow's `callback_queue`. Each stage starts with one batch commit, so `start` refuses
  a stage whose jobs plus its batch (10) exceed `batch_max_commit_jobs`
  (`BatchError::StageTooLarge`; a loader job can push the rest of a large stage). A job that may
  run twice starts its workflow once with
  `Workflow::with_bid(ctx.client(), ctx.jid())?.commit_id(ctx.jid())`:

```rust,no_run
use hench::batch::Workflow;
use hench::prelude::*;

#[hench::job]
async fn reserve_stock(order: i64) -> Result<(), JobError> {
    println!("reserving the stock of order {order}");
    Ok(())
}

#[hench::job]
async fn charge_card(order: i64) -> Result<(), JobError> {
    println!("charging order {order}");
    Ok(())
}

#[hench::job]
async fn print_label(order: i64) -> Result<(), JobError> {
    println!("printing the label of order {order}");
    Ok(())
}

#[hench::job]
async fn order_shipped(order: i64) -> Result<(), JobError> {
    println!("order {order} shipped");
    Ok(())
}

async fn fulfil(client: &Client, order: i64) -> Result<(), hench::Error> {
    Workflow::new(client)
        .description(format!("Fulfil order {order}"))
        .stage(|s| s.job::<reserve_stock>((order,)))
        .stage(|s| s.job::<charge_card>((order,)).job::<print_label>((order,)))
        .on_success::<order_shipped>((order,))
        .start()
        .await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let hench = Hench::builder().build().await?;
    fulfil(hench.client(), 42).await?;
    hench.run().await?;
    Ok(())
}
```

- **Expiry**: a batch's data expires 30 days after its last activity (`batch_ttl`; activity
  extends it once a quarter of it, an hour at most, went by) and 24 hours after it finished
  (`batch_linger`); a parent lives at least as long as each of its children.
- **Tests**: fake clients capture a batch's jobs and its callbacks (`drain_all` runs them in
  order); inline clients run the jobs at the commit and the callbacks after them, and a
  workflow's stages one after the other, however many there are.
- **Rollout**: workers count a batch's jobs only if they understand batches; upgrade every
  worker before defining batches.
- **Operations**: the dashboard's **Batches** tab lists the batches by state with their
  progress, and shows each batch's events, callbacks, failures (linked to their retries or
  morgue entries), children and pending jobs, with Cancel, Delete, Skip and Push callbacks
  now; `hench batches` (`list`, `show`, `cancel --yes`, `delete --yes`, `skip`, `dispatch`),
  `hench stats batches` and the JSON API (`GET <prefix>/api/v1/batches`, `GET
  <prefix>/api/v1/batches/{bid}`, `POST <prefix>/api/v1/batches/{bid}/cancel`; `hench web
  --api`) do the same from scripts; `hench::api::Api::batches()` from code.

## Rate limiting and throttling

Limits are shared by every process that uses the same Redis, and decided by Lua scripts on
Redis's clock (`hench::limit`).

**Limiters** protect a resource from inside jobs, or from anywhere with Redis: at most N
holders at a time (`Limiter::concurrent`, leases renewed while held and given back when the
permit is dropped), N per sliding period (`window`), N per calendar interval (`bucket`,
aligned on UTC), a leaky bucket (`leaky`: a burst, then a steady drip), a points budget whose
cost is corrected once known (`points`), or several of them at once (`all`, granted all or
none). A key gives each tenant its own limit (any text: keys that cannot be stored as they
are, such as empty ones, are hashed). A refusal is `Error::OverLimit` with the time until the
caller's slot in the limit's wait line; returned from a job with `?`, it **reschedules** the
job at that slot instead of failing it, without using a retry, and the job keeps its place in
the line when it comes back. Up to 20 reschedules (`HenchBuilder::over_limit_reschedules`) are
allowed before the retry system takes over; an iterable job that processed items before a
refusal starts that count again, since it is being paced, not stuck. A caller that will not
wait (`try_acquire`, or a slot beyond its `max_wait`) is told when the line ends without
joining it; in a job, `acquire` always takes a place (the job returns the refusal and comes
back for it), so a job that handles a refusal itself (skips optional work, say) should call
`try_acquire`.

```rust,no_run
use std::time::Duration;

use hench::limit::Limiter;
use hench::prelude::*;

/// 5 calls per second to the payments API per account, and 25 calls at a time in all.
fn payments(account: i64) -> Limiter {
    Limiter::all([
        Limiter::window("payments", 5, Duration::from_secs(1)).key(account),
        Limiter::concurrent("payments-calls", 25),
    ])
}

#[hench::job(queue = "billing")]
async fn charge(ctx: &JobContext, account: i64, cents: i64) -> Result<(), JobError> {
    // Waits up to 5 s for a slot (`max_wait`); then `?` reschedules the job at its slot.
    let _permit = payments(account).acquire(ctx).await?;
    println!("charging {cents} cents to account {account}");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let hench = Hench::builder().queues(["billing"]).build().await?;
    charge::perform_async(hench.client(), (42, 990)).await?;
    hench.run().await?;
    Ok(())
}
```

**Throttles** limit job *starts*: a worker checks a job's throttle right after fetching it,
before any middleware. A job over its throttle does not run and holds no processor: it is
parked in the schedule at its slot in the throttle's wait line (spaced at the limit's rate)
and comes back then, keeping its place. Concurrency permits are held by the worker for the
job's whole execution (each execution its own, so a job requeued and fetched again at once
never shares or loses them), renewed by the heartbeat, and released once its outcome is
written (a crashed worker's permits end with their lease, which must last at least three
heartbeat intervals). The line is spaced by how long the throttle's jobs hold their permits
(a decaying average), so a backlog drains near the limit; jobs much shorter than the
scheduler's round trip (a few milliseconds) do better with `.max_wait(..)`, which waits for a
permit in the worker instead of parking.

```rust,no_run
use std::time::Duration;

use hench::prelude::*;

struct SyncAccount;

impl Job for SyncAccount {
    const NAME: &'static str = "SyncAccount";
    type Args = (i64,); // account

    fn options() -> JobOptions {
        JobOptions::new().queue("sync").throttle("erp")
    }

    /// Per-key limits apply to each account.
    fn throttle_key((account,): &(i64,)) -> Option<String> {
        Some(account.to_string())
    }

    async fn perform(&self, _ctx: &JobContext, (account,): (i64,)) -> Result<(), JobError> {
        println!("syncing account {account}");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let hench = Hench::builder()
        // 10 syncs at a time in all, one at a time per account, 60 per minute per account.
        .throttle(
            Throttle::new("erp")
                .concurrency(10)
                .concurrency_per_key(1)
                .window_per_key(60, Duration::from_secs(60)),
        )
        .register(SyncAccount)
        .queues(["sync"])
        .build()
        .await?;
    SyncAccount::perform_async(hench.client(), (42,)).await?;
    hench.run().await?;
    Ok(())
}
```

- With the macro: `#[hench::job(throttle = "erp", throttle_by = [account])]` names the
  throttle and derives the key from the arguments; `#[hench::job(throttle(concurrency = 3,
  window_per_key = "5/1s"), throttle_by = [account])]` defines a throttle with the job (named
  after it; per-key limits need `throttle_by`, else they would apply to nothing). Per-key
  limits apply to jobs that have a key (`throttle_by`, or `throttle_key` on a push): a job
  without one gets the global limits only. Pick a throttle per push with
  `charge::set().throttle("erp-vip")`.
- A throttle applies to the jobs whose payload names it, which a typed push of a job type that
  declares one does. A push **by class name** knows no job type and runs unthrottled unless it
  names the throttle itself: `hench enqueue sync_account --args '[33]' --throttle erp:33`,
  `"throttle": "erp", "throttle_key": "33"` in `POST /api/v1/jobs`, `hench periodic add ...
  --throttle erp:33` (or `Periodic::class(..).throttle(..)`) for a schedule created at runtime,
  and `throttle` on a `JobPayload` pushed with `Client::push` (another service's raw pushes).
  A worker file's `[[periodic]]` table takes the throttle of a job type the worker registers.
- Throttles and limiters share their names: a throttle `stripe` and a `Limiter` named
  `stripe` (with the same key) draw on one budget. Give them different names unless that is
  what you want; a limit used with two different definitions is logged at `warn`.
- In the worker's configuration file: `[throttles.erp]` with `limits = ["concurrency=10",
  "concurrency=1 per key", "window=60/1m per key"]` and optional `reschedules`, `max_delay`,
  `max_wait`, `lease`, `fail_open`, `stats` and `stats_ttl`. Specs use the same grammar
  everywhere (`concurrency=10`, `window=100/1s`, `bucket=10000/day`, `leaky=60/1m`,
  `points=1000/1m`).
- A throttle reschedules without limit by default (`.reschedules(n)` hands the job to the
  retry system after `n` parkings; they never count against a limiter's budget, nor the other
  way round). When Redis is unavailable, throttled jobs go back to their queue (or run
  unthrottled with `.fail_open(true)`). Each throttle identity (each key of a per-key limit)
  keeps about 330 B of counters and wait line for `stats_ttl` (24 h) after its last use;
  `.stats(false)` keeps none, for throttles with very many keys whose jobs are rarely refused.
- Tests: testing clients run limiters and throttles unlimited by default;
  `Testing::limits(TestLimits::InMemory)` enforces them on tokio's clock, and
  `hench::limit::testing::memory_target()` is a target whose limits live in memory, for testing
  code that uses limiters; `Client::limited_jobs::<J>()` lists the captured jobs a limit
  rescheduled.
- Queue limits (a concurrency or rate limit on a whole queue, applied when workers fetch from
  it) are planned for 0.2 (see the [roadmap](#roadmap)). Until then, a throttle on the queue's
  job types does the same per type; a `[queue_limits]` table in the worker's file is refused
  rather than ignored.
- Operations: the dashboard's **Limits** tab lists every limit in use (use, next free slot,
  grants, refusals, wait line), the throttles live processes declare, each limit's holders and
  waiting jobs, and resets limits or clears wait lines; `hench limits` (`list`, `show`, `reset
  --yes`, `clear-line`), `hench stats limits` and the JSON API (`GET <prefix>/api/v1/limits`,
  `GET` and `DELETE <prefix>/api/v1/limits/{kind}/{id}`; `hench web --api`) do the same from
  scripts; `hench::api::Api::limits()` from
  code. Job metrics count over-limit reschedules and throttled starts per class.

## Using the features together

Unique jobs, periodic jobs, batches and limits are designed to be combined: one job can be all of
them at once. A worker handles such a job in a fixed order: the throttle first (before any
middleware; a job over its throttle is parked without running), then server middleware, the
batch's cancellation check, the release of an `until: start` lock, the job itself, the batch's
accounting, the outcome write (with the lock work), the release of the throttle's permits, the
batch's callbacks and the schedule's record of the run.

[`crates/hench/examples/features.rs`][example-features] is a complete program
that uses the four (a unique job, a batch with a callback, a throttle with a key and a
schedule) against a private `redis-server` and checks what each did; CI runs it (`cargo run -p
hench --example features --features test-util`).

A nightly sync, as a periodic job that never overlaps itself, defines a batch of throttled jobs
that call a rate-limited API, and runs a callback when they all succeeded:

```rust,no_run
use std::time::Duration;

use hench::limit::Limiter;
use hench::prelude::*;

/// Syncs one account: never two at a time per account (the throttle), at most 20 calls a
/// second to the ERP in all (a limiter: a refusal reschedules the job at its slot).
#[hench::job(queue = "erp", retry = 5, throttle = "erp", throttle_by = [account])]
async fn sync_account(ctx: &JobContext, account: i64) -> Result<(), JobError> {
    let _call = Limiter::window("erp-api", 20, Duration::from_secs(1))
        .acquire(ctx)
        .await?;
    println!("syncing account {account}");
    Ok(())
}

/// Runs once every account of the batch synced.
#[hench::job(queue = "erp")]
async fn sync_done(accounts: usize) {
    println!("{accounts} accounts synced");
}

/// Defines the night's batch. The job may run twice (a crash after the commit, before its
/// acknowledgement): the batch takes the job's jid as its bid and a commit id, so a second
/// run commits nothing.
#[hench::job(queue = "erp")]
async fn nightly_sync(ctx: &JobContext) -> Result<(), JobError> {
    let accounts: Vec<i64> = (1..=500).collect();
    let committed = Batch::with_bid(ctx.client(), ctx.jid())?
        .commit_id("nightly")
        .description("Nightly ERP sync")
        .on_success::<sync_done>((accounts.len(),))
        .jobs(|jobs| async move {
            for account in accounts {
                sync_account::perform_async(&jobs, (account,)).await?;
            }
            Ok::<_, hench::Error>(())
        })
        .await?;
    if committed.already_applied {
        println!("batch {} was committed by an earlier run", committed.bid);
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let hench = Hench::builder()
        .queues(["erp"])
        // Ten syncs at a time in all, one at a time per account.
        .throttle(Throttle::new("erp").concurrency(10).concurrency_per_key(1))
        // At 02:00 UTC; a tick while the previous sync waits or runs is skipped.
        .periodic(
            Periodic::cron("nightly-sync", "0 2 * * *")
                .job::<nightly_sync>(())
                .unique_for(Duration::from_secs(6 * 3600)),
        )
        .build()
        .await?;
    hench.run().await?;
    Ok(())
}
```

Recipes:

| Want | Use |
|---|---|
| A periodic job that never overlaps itself | `unique_for` on the schedule, about the longest run: ticks meanwhile are skipped and shown as `duplicate` in its history |
| One job at a time per key, every push kept | a throttle with `concurrency_per_key(1)`: later jobs wait, parked at their slots |
| One job waiting per key, later pushes dropped | `unique_for` with `unique_by = [key]` (`on_conflict = "error"` to be told; a schedule's ticks can only drop duplicates: `Periodic::job` of such a job type needs `.options(&JobOptions::new().on_conflict(OnConflict::Drop))`, a `[[periodic]]` table drops them by itself) |
| Debounce: run once after the last of a burst of pushes | `unique_for`, `on_conflict = "replace"` and a delay on each push: the newest push replaces the waiting job (a batch member never replaces: push it with `J::set().on_conflict(OnConflict::Drop)`; schedules as above) |
| A batch created once by a job that may run twice | `Batch::with_bid(&client, ctx.jid())` and a `commit_id` (the commit is then idempotent) |
| A batch of jobs calling a rate-limited service | a throttle on the members' job type, or a limiter inside the job: refused members are parked and come back at their slots without using a retry (a limiter's refusals turn into failures after 20 reschedules, `over_limit_reschedules`); the batch waits for them |
| Stop a batch whose jobs are parked by a throttle | `client.cancel_batch(bid)`: parked members are skipped when they come back, not parked again |

**Rollout.** Upgrade every worker before any client uses one of these features. A worker that
predates a feature ignores its fields: a unique job's lock then lasts until it expires, batch
members are never counted (the batch never completes), a limiter's refusal is an ordinary
failure, and newer schedules are skipped. The dashboard's Busy page names a live process that
lacks a feature in use ("lacks: batches"); workers advertise the features they understand in
their heartbeat.

**Sizing.** A job that uses no feature costs what it did: its Redis commands are byte for byte
those of before, and the measured throughput of no-op jobs is unchanged within noise. Each
feature on a job adds, per execution: a unique job with a `success` or `expired` lock, no round
trip (the lock work is part of the push and outcome scripts), with a `start` lock one release
before `perform`, with basic fetch two lock scripts around the outcome, and while it runs its
share of a renewal per heartbeat; a batch member, one accounting script and one read; a
throttle, one acquire and one release (plus a renewal per heartbeat while it runs); a periodic
job, one record of its run. Redis CPU per job, measured with no-op jobs (one worker, 25
slots): plain 16 µs; unique (`success`) 31 µs, (`start`) 41 µs; batch member 45 µs; periodic
29 µs; inside a limiter 57 µs; throttled 82 µs (per key 88 µs); every feature at once 193 µs.
One Redis core thus runs about 63,000 plain jobs a second, 12,000 throttled ones. Memory in
Redis: about 440 B per unique lock (plus a copy of the job while it waits in the schedule);
200 B per batch, 72 B per pending and 300 B per failed member, kept 24 hours after the batch
finished; 1 KB per schedule plus 200 B per history entry; about 330 B per limit identity for
its statistics, index entry included (kept 24 hours after its last use, none with
`stats(false)`), plus 10 B per unit of a window, or per concurrency holder 30 B (a limiter's
permit) to 60 B (a throttled job), about 140 B beyond 128 holders.

## The dashboard

`hench-web` is the dashboard as an `axum::Router`. Mount it anywhere, behind your own
authentication:

```rust,no_run
use std::time::Duration;

use axum::extract::Request;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use hench_web::{AllowedHosts, HenchWeb};

async fn admins_only(request: Request, next: Next) -> Response {
    // Check your session cookie here, or a header your proxy sets (and strips from what
    // clients send).
    if request.headers().contains_key("x-admin") {
        next.run(request).await
    } else {
        axum::http::StatusCode::UNAUTHORIZED.into_response()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let redis = hench::RedisPool::connect("redis://127.0.0.1:6379").await?;
    let dashboard = HenchWeb::new(&redis)
        .title("Acme jobs")
        .app_url("/admin")              // a "Back to app" link
        .tab("Reports", "/admin/reports") // extra navigation tabs
        .poll_interval(Duration::from_secs(3))
        // The names it is reached under, besides localhost and IP addresses.
        .allowed_hosts(AllowedHosts::new(["jobs.example.com"]))
        .read_only(false);              // true: no buttons, every POST refused
    let app = axum::Router::new()
        .nest_service("/jobs", hench_web::router(dashboard).layer(middleware::from_fn(admins_only)));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    axum::serve(listener, app).await?;
    Ok(())
}
```

Pages: the dashboard (live chart, history, Redis), busy processes (quiet and stop them),
queues (pause, resume, delete, browse, filter), retries, scheduled and dead jobs (filter,
retry, enqueue now, delete, kill, in bulk or all at once), each job in detail (arguments,
error and backtrace, iteration progress, and its batch, schedule, lock and limits), and the
tabs of the features: periodic schedules (enqueue now, pause, resume, delete), batches
(cancel, delete, skip a job, push callbacks), limits (reset, clear a wait line) and the locks
of unique jobs (release one, a selection or all); then metrics. `.periodic(false)`,
`.batches(false)`, `.limits(false)`, `.locks(false)` and `.metrics(false)` turn a tab off: it
disappears and its pages answer 404. `hench-web`'s crate documentation lists every route,
with the ones that take actions (all `POST`, all refused by `.read_only(true)`), for a proxy
that filters paths. `HenchWeb::new(&hench)` uses the application's client middleware,
dead-set limits and death handlers for the actions. Pages of your own go inside it, like
Sidekiq's web extensions:
`.routes(router)` serves your routes behind the dashboard's protections, where
`hench_web::Page` renders them in its layout and `hench_web::Form` reads their forms with its
CSRF check; `.job_info(..)` adds rows, such as a link to a job's logs, to the job pages.

The dashboard answers only requests addressed to `localhost`, an IP address or a name given
to `allowed_hosts`: otherwise a web page on another site could point its own host name at
your machine (DNS rebinding) and use a dashboard listening on `127.0.0.1` as if it were its
own, CSRF token included. `AllowedHosts::any()` serves every name, for a dashboard whose every
request is authenticated with credentials such a page cannot use (a session cookie, HTTP
Basic); `hench_web::require_host` applies the same check to routes of your own. Every change
is a POST protected by a CSRF token and fetch-metadata checks; pages carry a
strict content security policy (no inline scripts or styles), `nosniff`, `same-origin`
referrers and frame protection; everything is escaped; the assets are embedded in the binary
and served with content hashes. It works at the root, nested several levels deep and under
paths with parameters, in light and dark themes, down to phone widths. Mount it with
`nest_service`, which serves both `/jobs` and `/jobs/` (axum's `nest` does not route
`/jobs/`).

## The `hench` command

`hench-server` builds the `hench` binary (`cargo install hench-server --locked`, which
builds it with the dependency versions it was tested with): a standalone
dashboard, a JSON API and maintenance commands. Redis comes from `--redis-url`,
`HENCH_REDIS_URL`, or the same variables as the workers. Give secrets (the dashboard's
password, a Redis URL with a password) through the environment: command-line arguments are
visible to every user of the machine (`ps`).

```sh
# The dashboard on port 7420, with Basic authentication and the JSON API; the password is
# read from HENCH_WEB_PASSWORD.
export HENCH_WEB_PASSWORD='a long random password'
hench web --bind 0.0.0.0:7420 --username admin --api
# Read-only, under /jobs, behind a proxy that forwards its own host name.
hench web --read-only --prefix /jobs --allowed-host jobs.example.com

# sidekiqmon-style reports (add --json for machine-readable output).
hench stats                 # version, overview, processes, queues, busiest job classes
hench stats overview
hench processes
hench queues

# Signals: quiet (finish running jobs, take no more) and stop.
hench quiet host-1:4242:0a1b2c3d4e5f
hench stop all              # every process that is not embedded in an application

# Push a job from a shell or a cron job. The class is the job's registered name: the
# function's for `#[hench::job]` (`monthly_report` from "Enqueue jobs" above), else `NAME`.
# `hench` does not know your job types: give the queue (else `default`), the retries (else the
# default) and the throttle (`--throttle NAME[:KEY]`, else none) the job needs.
hench enqueue monthly_report --args '[7, "2026-10"]' --queue reports --in 5m

# The features (see their sections above; `hench <command> --help` for every option).
hench periodic              # the schedules: show, enqueue, pause, unpause, remove, add, ...
hench batches --state dead  # the batches: show, cancel, delete, skip, dispatch
hench limits                # the limiters and throttles: show, reset, clear-line
hench locks                 # the locks of unique jobs: show, release, release-all
```

With `--api`, `GET <prefix>/api/v1/stats`, `/queues` and `/processes` return JSON and
`POST <prefix>/api/v1/jobs` enqueues `{"class": "...", "args": [...], "queue": "...", "in":
60}`. Each process of `/processes` (and of `hench processes --json`) lists the names of the
queues it works on in `queues`, and its capsules by name in `capsules`, with their
concurrency, mode and queue weights (`{"default": {"concurrency": 5, "mode": "weighted",
"weights": {"critical": 3, "default": 1}}}`). The features add their routes under the same
prefix (`/periodic`, `/batches`, `/limits`, `/locks`, described in their sections); a
read-only server refuses every change. Requests are read strictly (an unknown field, in the
body or in `unique`, or an unknown
`unique.until` is a `422`), and every error is JSON, `{"error": "..."}`, unknown routes (`404`)
and methods (`405`) included. `GET /healthz` answers `ok` while Redis does (`503` within a
second otherwise). The
server refuses to expose the API on a public address without credentials. Without
credentials it also answers only requests addressed to `localhost` or an IP address: a web
page on another site could otherwise point its own host name at your machine (DNS rebinding)
and drive a dashboard listening on `127.0.0.1`. Name the hosts a proxy forwards with
`--allowed-host` (or `HENCH_WEB_ALLOWED_HOSTS`, comma-separated; `.example.com` covers the
subdomains, `*` every name). Like `hench enqueue`, the JSON API pushes jobs by class name
without knowing their job types: a job without `"queue"` goes to `default`, and one without
`"throttle"` (and `"throttle_key"`) runs unthrottled, whatever its job type declares.

Slow or idle clients cannot hold the server: a request head must arrive within 10 seconds (an
idle connection is closed after as long), a body within 30 seconds, and a response the client
stops reading is abandoned after 30 seconds; at most 512 connections are served at once
(`--max-connections`, below the open-file limit), further clients wait to be accepted. SIGINT
or SIGTERM stops it gracefully: requests in flight get 10 seconds to finish, and a second
signal stops it at once. Expose it to the internet only behind a reverse proxy that limits
request rates (and terminates TLS).

## Deployment

**Redis.** Version 6.2 or newer (Hench uses `LMOVE`, `BLMOVE` and `ZRANGE BYSCORE`; workers
check at startup). Set `maxmemory-policy noeviction` (an evicting Redis silently drops jobs;
workers warn about it) and enable persistence (AOF) if jobs must survive a Redis restart.
Keys are not namespaced, exactly like Sidekiq: give each application its own Redis database
or instance, and do not share it with a cache.

**Signals.** `SIGTERM` or `SIGINT` stops a process gracefully: it stops fetching, waits up to
`timeout` (25 s) for running jobs, then interrupts the rest and requeues them; a second
signal interrupts at once. `SIGTSTP` quiets a process (it finishes its jobs and takes no new
ones), which is how a deploy drains old workers before stopping them. `SIGTTIN` logs what a
process is running (each busy slot's job and how long it has run), the first thing to look at
when a worker seems stuck; `api::Process::dump()` asks a process for the same from anywhere. Give your process
manager a stop timeout above `timeout` plus a few seconds (Kubernetes:
`terminationGracePeriodSeconds`). Embedded workers stop with your application
(`run_until`), and remote quiet/stop from the dashboard or `hench` reach every process (a
remote stop ends `run_until`: stop the rest of the application then, as in the
[embedding example](#5-or-embed-them-next-to-your-web-application)).

**Panics.** Workers catch a job's panic by unwinding, Rust's default panic strategy: keep
`panic = "unwind"` in the profile that builds a worker binary. Under `panic = "abort"` a
panicking job aborts the whole process instead of failing, taking down the jobs running beside
it; with reliable fetch it is recovered and aborts the processes that run it after it, up to
`max_recoveries` + 1 processes in all (4 by default), and then lands in the dead set as a
[poison pill](#delivery-guarantees-reliable-and-basic-fetch) (class `hench::CrashLoop`), never
in the retries. Workers log a warning at startup when built that way. (A stack overflow or an
allocation failure aborts the process whatever the strategy.)

**systemd.** Workers run with `Hench::run` notify systemd when started under `Type=notify`:
`READY=1` once the process is registered and its workers are running, a `STATUS=` line with
busy counts on every heartbeat, `STOPPING=1` when the shutdown begins, and watchdog keep-alives
(from a task of their own, twice per `WatchdogSec`, whatever Redis is doing) when `WatchdogSec`
is set. Workers embedded with `start` or `run_until` leave the notifications to the program
that embeds them (give them a notifier with `systemd_notifier` to send them anyway):

```ini
[Service]
Type=notify
# Where config/hench.toml is (or name the file: my-worker -C /etc/my-worker/hench.toml).
WorkingDirectory=/opt/my-app
ExecStart=/usr/local/bin/my-worker -e production
Environment=REDIS_URL=redis://10.0.0.5:6379/0
WatchdogSec=60
TimeoutStopSec=35
KillSignal=SIGTERM
Restart=always
```

**Logging.** Everything goes through `tracing`: each job runs in a `job{class, jid}` span,
with `start` and `done`/`fail` events and their duration (`info`), and failures reach the
default error handler (`warn`). Install a subscriber that shows `info`, as the examples do
(`RUST_LOG` overriding it): `EnvFilter`'s default without `RUST_LOG` is `error`, which hides
failing jobs. Set a job's `log_level` to quieten or detail it, and add
`hench::JOB_LOG_LEVEL_DIRECTIVES` to your `EnvFilter` to let a job log below the process
level.

**Health and capacity.** `HEAD /` on the dashboard and `GET /healthz` on `hench web` check
Redis. Processes appear on the busy page with their memory, concurrency and running jobs.
Run CPU-heavy or blocking work in `tokio::task::spawn_blocking` so it does not stall the other
jobs of the process, and scale with more processes or more concurrency. A thread of its own
keeps the process's heartbeat key alive, so a job that blocks the runtime does not make the
process look dead to the others (which would run its jobs again); but it does hold up the
heartbeat (remote signals, stats) and, under `WatchdogSec`, the keep-alives.

## Differences from Sidekiq

Hench is a Rust system with Sidekiq's model, not a Sidekiq client: it does not run Ruby jobs
or share queues with Ruby processes. The Redis layout mirrors Sidekiq's so that the concepts
and tools carry over. Notable differences:

- Jobs are typed; arguments are serde types, checked on push (no `NaN`, no non-array args).
- Reliable fetch (Sidekiq Pro's `super_fetch`) is the default, with orphan recovery.
- Queue pausing (a Sidekiq Pro feature) is built in, and so are the Pro and Enterprise
  features unique jobs, periodic jobs, batches and rate limiting. Their Redis work happens in
  the scripts that push, fetch and settle the job (a unique job's lock is taken with the push
  and released with the outcome write, atomically), not in middleware around them. Unique
  locks of failed jobs are held through their retries; periodic jobs are fired by every worker
  without a leader, each tick at most once; throttled jobs wait in the schedule for their slot
  in the limit's wait line, without using a retry.
- Outcome writes are retried through Redis outages; the scheduler leases due jobs instead of
  popping them, so a crash, a lost reply or a failing (even panicking) client middleware
  delays a job instead of losing it; API moves (retry, kill, add to queue) are atomic scripts.
- Middleware instances are shared by every job (Sidekiq instantiates them per job); chains
  hold one middleware per type.
- Panics are caught and handled as failures of class `panic` (with `panic = "unwind"`, the
  default).
- Testing modes are per client instead of a global switch.
- Metrics hash keys use a four-digit year; counts whose flush failed (metrics and `stat:*`
  counters) are sent again, and counted once even when an earlier attempt's reply was lost.
- The dashboard sorts the metrics table by any column, offers pause/resume, refuses
  cross-site POSTs by token *and* fetch metadata, and answers only the host names it knows
  (localhost and IP addresses unless told others); unknown queues and jobs are 404 pages.
- No ActiveJob, no Rails reloader, no `CurrentAttributes`, no profiling, no thread dumps:
  `SIGTTIN` and `api::Process::dump()` log each busy slot's job instead.

[`docs/DESIGN.md`][design] is the full contract (data model, semantics and every deviation);
[`docs/STATUS.md`][status] tracks what is done, and [`CHANGELOG.md`][changelog] what each
release changed.

[design]: https://github.com/rkwap/hench/blob/v0.1.0/docs/DESIGN.md
[status]: https://github.com/rkwap/hench/blob/v0.1.0/docs/STATUS.md

## Minimum supported Rust version

Rust 1.88, edition 2024.

## Roadmap

[CHANGELOG.md][changelog] lists what each release changed. Planned for 0.2:

- **Queue limits**: a concurrency or rate limit on a whole queue, enforced by the fetch, so that
  a queue over its limit is skipped like a paused one while its jobs keep their order and the
  other queues keep flowing. Until then, a [throttle](#rate-limiting-and-throttling) on the
  queue's job types limits each type, and a `[queue_limits]` table in the worker's file is
  refused rather than ignored.
- **Staged batch commits**: a batch of any size defined in one closure, written in chunks with
  bounded memory and still visible all at once. Until then one commit holds at most
  `batch_max_commit_jobs` jobs (10 000 by default, a child batch counting as 10), and a larger
  batch adds the rest from loader jobs (`Batch::open`, each with its own `commit_id`).
- **Iterable jobs that see their batch's cancellation**: an iterable job of a cancelled batch
  stops at its next checkpoint. Until then it runs to the end of its current execution, unless
  it calls `it.check_batch()` itself, and is skipped at its next one.
- **A process-wide cache of cancelled batches**: a batch's jobs learn of its cancellation
  without a Redis round trip each. Until then every execution of a batch's job reads its
  batch's cancellation from Redis (one `HGET`).
- **Batch tags**, on Redis 7.4 or newer: tag batches and find them by tag. Until then a batch is
  found by its bid or by the text of its description.
- **A lean lock mode for unique jobs**, if users ask for it: locks of about half the size, for
  applications that hold millions of them, which the Locks tab could not list. Until then every
  lock keeps the record (about 440 B) that the Locks tab shows and explains.

Later, without a date:

- the dashboard in other languages (it is in English only);
- creating and editing schedules in the dashboard (the API and `hench periodic add` create
  them);
- TLS through Redis Sentinel (`rediss+sentinel://` is refused), and options for a private
  certificate authority or a client certificate in the `hench` command;
- Valkey and Dragonfly in the tests (they should work, but nothing checks that they do);
- a `hench dump` command (`SIGTTIN` and `api::Process::dump()` log the busy jobs);
- a warning when a workflow's callback lands in a queue no process works on, and freed throttle
  permits handed straight to the jobs waiting for them.

[changelog]: https://github.com/rkwap/hench/blob/v0.1.0/CHANGELOG.md

## License and credits

Hench is licensed under the [MIT license][license].

[license]: https://github.com/rkwap/hench/blob/v0.1.0/LICENSE

Hench is inspired by [Sidekiq] by Mike Perham and Contributed Systems: its architecture, data
model, semantics and dashboard structure follow Sidekiq's, and its tests port the scenarios of
Sidekiq's test suite. Hench contains no Sidekiq code: Sidekiq is LGPL-3.0 licensed, and Hench
is an independent implementation written from its documented behaviour. Thanks also to
[rusty-sidekiq] and [Faktory] for ideas. Hench is not affiliated with or endorsed by
Contributed Systems.

[tokio]: https://tokio.rs
[Sidekiq]: https://github.com/sidekiq/sidekiq
[rusty-sidekiq]: https://github.com/film42/sidekiq-rs
[Faktory]: https://github.com/contribsys/faktory
