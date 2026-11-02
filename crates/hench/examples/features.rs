//! The four features in one program that checks itself: a unique job, a batch with a
//! callback, a throttled job and a periodic schedule, run by one worker against a private
//! `redis-server` (`hench::test_util::TestRedis`, so `redis-server` must be on the `PATH`). It
//! exits with an error unless every feature did what the README says; CI runs it.
//!
//! ```sh
//! cargo run -p hench --example features --features test-util
//! ```
//!
//! Each job is what an application would write; the counters of [`Seen`] only let the
//! program check what happened. Against your own Redis, build the workers with
//! `Hench::builder().redis_url(..)` instead of `TestRedis::builder`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use hench::PushOutcome;
use hench::prelude::*;
use hench::test_util::TestRedis;

/// What the jobs saw, shared with them through `HenchBuilder::data`.
#[derive(Default)]
struct Seen {
    /// Runs of `sync_user`, by user.
    syncs: HashMap<u64, u32>,
    /// Rows `import_row` imported.
    rows: u32,
    /// What the batch's `success` callback was told, once per call.
    imports_done: Vec<u32>,
    /// `call_partner` calls running now, in all and per account.
    calls_running: u32,
    calls_by_account: HashMap<u64, u32>,
    /// The most calls seen running at once, in all and for one account.
    calls_max: u32,
    calls_max_per_account: u32,
    /// Calls finished.
    calls: u32,
    /// Runs of the `heartbeat` schedule's job.
    ticks: u32,
}

type Shared = Arc<Mutex<Seen>>;

/// The counters (a job that panicked while holding them does not poison the others).
fn lock(shared: &Shared) -> MutexGuard<'_, Seen> {
    shared
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Unique: one waiting or running sync per user for a minute (`unique_by`: the reason does
/// not count); a push while one exists is a duplicate and is dropped.
#[hench::job(unique_for = 60, unique_by = [user])]
async fn sync_user(ctx: &JobContext, user: u64, reason: String) -> Result<(), JobError> {
    tracing::info!(user, reason, "syncing");
    tokio::time::sleep(Duration::from_millis(200)).await;
    *lock(ctx.data::<Shared>()?).syncs.entry(user).or_default() += 1;
    Ok(())
}

/// A member of the import batch.
#[hench::job]
async fn import_row(ctx: &JobContext, row: u32) -> Result<(), JobError> {
    tracing::debug!(row, "imported");
    lock(ctx.data::<Shared>()?).rows += 1;
    Ok(())
}

/// The import batch's `success` callback: runs once, after every member succeeded.
#[hench::job]
async fn import_done(ctx: &JobContext, rows: u32) -> Result<(), JobError> {
    lock(ctx.data::<Shared>()?).imports_done.push(rows);
    Ok(())
}

/// Throttled by `partner-api` (defined on the builder below: two calls at a time in all, one
/// at a time per account), with the account as the throttle key.
#[hench::job(throttle = "partner-api", throttle_by = [account])]
async fn call_partner(ctx: &JobContext, account: u64) -> Result<(), JobError> {
    let shared = ctx.data::<Shared>()?;
    {
        let mut seen = lock(shared);
        seen.calls_running += 1;
        seen.calls_max = seen.calls_max.max(seen.calls_running);
        let running = seen.calls_by_account.entry(account).or_default();
        *running += 1;
        let running = *running;
        seen.calls_max_per_account = seen.calls_max_per_account.max(running);
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut seen = lock(shared);
    seen.calls_running -= 1;
    seen.calls += 1;
    if let Some(running) = seen.calls_by_account.get_mut(&account) {
        *running -= 1;
    }
    Ok(())
}

/// The job of the `heartbeat` schedule.
#[hench::job]
async fn beat(ctx: &JobContext) -> Result<(), JobError> {
    lock(ctx.data::<Shared>()?).ticks += 1;
    Ok(())
}

/// Waits up to 30 s for `done` to hold of the counters.
async fn wait(shared: &Shared, what: &str, done: impl Fn(&Seen) -> bool) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if done(&lock(shared)) {
            println!("ok: {what}");
            return Ok(());
        }
        if Instant::now() > deadline {
            return Err(format!("timed out waiting for: {what}"));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();
    let redis = TestRedis::start().await;
    let shared: Shared = Arc::default();
    // The macro's jobs register themselves: no `.register(..)` for them.
    let hench = redis
        .builder()
        .await
        .data(shared.clone())
        .throttle(
            Throttle::new("partner-api")
                .concurrency(2)
                .concurrency_per_key(1),
        )
        .periodic(Periodic::every("heartbeat", Duration::from_secs(1)).job::<beat>(()))
        .build()
        .await?;
    let client = hench.client().clone();
    let launcher = hench.start().await?;

    // Unique jobs: while user 7's sync waits or runs, another push for user 7 is a duplicate.
    let first = sync_user::perform_async(&client, (7, "signup".into())).await?;
    let second = sync_user::set()
        .push_outcome(&client, (7, "profile edit".into()))
        .await?;
    if first.is_none() || !matches!(second, PushOutcome::Duplicate { .. }) {
        return Err(
            format!("unique: expected a push then a duplicate: {first:?}, {second:?}").into(),
        );
    }
    wait(&shared, "user 7's sync ran once", |seen| {
        seen.syncs.get(&7) == Some(&1)
    })
    .await?;
    // Its success released the lock (just after the job returned): a new push is accepted.
    let deadline = Instant::now() + Duration::from_secs(10);
    while sync_user::perform_async(&client, (7, "later".into()))
        .await?
        .is_none()
    {
        if Instant::now() > deadline {
            return Err("unique: the lock outlived its job".into());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    wait(&shared, "user 7's next sync ran", |seen| {
        seen.syncs.get(&7) == Some(&2)
    })
    .await?;

    // A batch: 20 rows, then its `success` callback, once.
    let committed = Batch::new(&client)
        .description("import #1")
        .on_success::<import_done>((20,))
        .jobs(|jobs| async move {
            for row in 0..20 {
                import_row::perform_async(&jobs, (row,)).await?;
            }
            Ok::<_, hench::Error>(())
        })
        .await?;
    println!(
        "batch {} committed with {} jobs",
        committed.bid,
        committed.jids.len()
    );
    wait(
        &shared,
        "the batch's callback ran once, after its 20 rows",
        |seen| seen.rows == 20 && seen.imports_done == [20],
    )
    .await?;

    // A throttle: 12 calls for 3 accounts, never more than 2 at once nor 2 for one account.
    for call in 0..12_u64 {
        call_partner::perform_async(&client, (call % 3,)).await?;
    }
    wait(&shared, "the 12 throttled calls ran", |seen| {
        seen.calls == 12
    })
    .await?;
    {
        let seen = lock(&shared);
        if seen.calls_max > 2 || seen.calls_max_per_account > 1 {
            return Err(format!(
                "throttle: {} calls at once ({} for one account)",
                seen.calls_max, seen.calls_max_per_account
            )
            .into());
        }
        println!(
            "ok: at most {} calls at once, {} for one account",
            seen.calls_max, seen.calls_max_per_account
        );
    }

    // A schedule: the heartbeat's job runs every second.
    wait(&shared, "the schedule's job ran twice", |seen| {
        seen.ticks >= 2
    })
    .await?;

    launcher.stop().await?;
    println!("every feature did its part");
    Ok(())
}
