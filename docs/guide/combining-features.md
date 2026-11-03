# Using the features together

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

---

[← Metrics](metrics.md) · [Guide](README.md) · [The dashboard →](dashboard.md)

[example-features]: ../../crates/hench/examples/features.rs
