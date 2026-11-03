# Defining and enqueuing jobs

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

---

[← Getting started](getting-started.md) · [Guide](README.md) · [Configuration →](configuration.md)
