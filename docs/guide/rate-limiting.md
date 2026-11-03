# Rate limiting and throttling

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
  it) are planned for 0.2 (see the [roadmap](../ROADMAP.md)). Until then, a throttle on the queue's
  job types does the same per type; a `[queue_limits]` table in the worker's file is refused
  rather than ignored.
- Operations: the dashboard's **Limits** tab lists every limit in use (use, next free slot,
  grants, refusals, wait line), the throttles live processes declare, each limit's holders and
  waiting jobs, and resets limits or clears wait lines; `hench limits` (`list`, `show`, `reset
  --yes`, `clear-line`), `hench stats limits` and the JSON API (`GET <prefix>/api/v1/limits`,
  `GET` and `DELETE <prefix>/api/v1/limits/{kind}/{id}`; `hench web --api`) do the same from
  scripts; `hench::api::Api::limits()` from
  code. Job metrics count over-limit reschedules and throttled starts per class.

---

[← Batches and workflows](batches.md) · [Guide](README.md) · [Iterable jobs →](iterable-jobs.md)
