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
- Operations: the dashboard's **Limits** tab lists every limit in use (use, next free slot,
  grants, refusals, wait line), the throttles live processes declare, each limit's holders and
  waiting jobs, and resets limits or clears wait lines; `hench limits` (`list`, `show`, `reset
  --yes`, `clear-line`), `hench stats limits` and the JSON API (`GET <prefix>/api/v1/limits`,
  `GET` and `DELETE <prefix>/api/v1/limits/{kind}/{id}`; `hench web --api`) do the same from
  scripts; `hench::api::Api::limits()` from
  code. Job metrics count over-limit reschedules and throttled starts per class.

## Queue limits

A **queue limit** applies to every fetch of a queue, whatever its jobs: workers take a job
from a limited queue only when its limits grant one, and charge them in the same step. A
queue over its limits is skipped like a paused one: its jobs stay in it, in order (nothing is
parked in the schedule, so a large backlog costs nothing while it waits), and the worker
fetches from its other queues meanwhile. Use one to protect a resource that every job of a
queue uses, or to run a queue of heavy jobs a few at a time across all your workers.

```rust,no_run
use std::time::Duration;

use hench::limit::Limit;
use hench::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let hench = Hench::builder()
        .queues(["default", "exports"])
        // At most 3 exports at a time in the whole cluster, and 100 a minute.
        .queue_limits(
            "exports",
            [
                Limit::Concurrency { limit: 3 },
                Limit::Window { limit: 100, period: Duration::from_secs(60) },
            ],
        )
        .build()
        .await?;
    hench.run().await?;
    Ok(())
}
```

- A queue takes concurrency, window, bucket and leaky limits, at most one of each kind (a
  points budget needs a cost known after the job ran, which a fetch cannot know).
- The limits live in Redis and apply to every process fetching the queue. Change them at run
  time from the queue's page in the dashboard, with `hench queues limit exports --set
  "concurrency=5"` (or `--clear`), `PUT <prefix>/api/v1/queues/exports/limits` with
  `{"limits": "concurrency=5"}` (and `DELETE` to clear), or
  `api.queue("exports").set_limits(..)` from code: every worker applies them within a
  heartbeat interval (10 s by default).
- A worker that has limits for a queue in its configuration (`HenchBuilder::queue_limits`, or
  a `[queue_limits]` table in its file, `exports = "concurrency=3; window=100/1m"`) writes them
  at **every start** (deploys, restarts after a crash, scale-outs), logging any stored value it
  replaces: code wins over run-time edits at the next start, and two deployments configured
  differently overwrite each other. Removing a queue from the configuration leaves its stored
  limits in place; clear them with `hench queues limit <queue> --clear`. An environment's
  section of the file (`[production.queue_limits]`) replaces the top-level table as a whole.
- Upgrade every worker before setting queue limits: a worker of an earlier version fetches the
  queue without them (the dashboard's Busy page marks such processes "lacks: queue_limits").
- A job holds its queue's concurrency slot while it runs (renewed by the heartbeat, given back
  once its outcome is written; a crashed worker's slots end with their one-minute lease). A
  rate limit is charged once per job taken.
- `hench queues` and the dashboard's Queues page show each queue's limits; the use of each one
  is on the **Limits** tab, under the limiter name `@<queue>` (`c:@exports`, `w:@exports`).
- Queue limits or throttles? A queue limit counts every job of a queue together and costs
  nothing while jobs wait; a throttle limits job types (and keys, such as tenants), whichever
  queue they are in, and parks the jobs it refuses. A queue's jobs can have both: the queue
  limit is checked when the job is fetched, the throttle when it is about to start, so a job
  the throttle parks has already used a unit of the queue's rate limits (its concurrency slot
  is given back).
- Stored limits that do not parse (edited by hand in Redis) make every worker leave the queue
  alone, with an error in its logs, until they are set again or cleared: a broken limit never
  lifts the protection it was meant to give.
- A worker waits for new jobs on one queue at a time, and one worker per process watches each
  empty limited queue. When a burst arrives on a limited queue, the first job starts at once
  and the next ones as soon as sleeping workers are woken (in any capsule fetching the queue),
  or, if the other workers are waiting on unlimited queues, within the fetch timeout (2 s by
  default). Give a limited queue its own capsule when its bursts must start at once.

---

[← Batches and workflows](batches.md) · [Guide](README.md) · [Iterable jobs →](iterable-jobs.md)
