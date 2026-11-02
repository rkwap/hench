//! Batches through real workers (`docs/design/batches.md` §3.4, E1–E21): events and their
//! callbacks, retries, deaths and revivals, the outcomes that count and those that do not,
//! cancellation, loaders, nesting, workflows, callback queues, recovery by maintenance, removals
//! through the API and paused queues.

mod batch_support;
mod support;

use std::sync::Arc;
use std::time::Duration;

use batch_support::*;
use hench::api::Api;
use hench::batch::{BatchEvent, BatchState, Workflow};
use hench::iterable::{Enumerator, IterableJob, Iteration};
use hench::prelude::*;
use hench::test_util::TestRedis;
use hench::{FetchStrategy, ServerMiddleware, ServerNext, keys};
use serde_json::json;
use support::*;

/// Pushes `n` members of mode `mode` (tags `<prefix><i>`) through `jobs`.
async fn members(jobs: &Client, prefix: &str, n: usize, mode: &str) -> hench::Result<Vec<String>> {
    let mut jids = Vec::new();
    for i in 0..n {
        let jid = Work::perform_async(jobs, work(&format!("{prefix}{i}"), mode)).await?;
        jids.extend(jid);
    }
    Ok(jids)
}

/// Waits a little, then checks that `entry` was recorded exactly `count` times (a callback
/// that ran twice would show up).
async fn settled_count(log: &Log, entry: &str, count: usize) {
    log.wait_count(entry, count).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(log.count(entry), count, "{entry}: {:?}", log.entries());
}

/// E1, E17: members that succeed fire `complete` then `success`, each callback once; the
/// counts are right and the keys keep only their linger.
#[tokio::test]
async fn members_that_succeed_fire_complete_then_success() {
    for fetch in [FetchStrategy::Reliable, FetchStrategy::Basic] {
        let redis = TestRedis::start().await;
        let log = Arc::new(Log::default());
        let (hench, launcher) = worker(&redis, &log, |builder| builder.fetch(fetch)).await;
        let client = hench.client().clone();
        let linger = Duration::from_secs(45);
        let committed = Batch::new(&client)
            .description("ten rows")
            .linger(linger)
            .on_complete::<Callback>(named("done"))
            .on_success::<Callback>(named("ok"))
            .on_death::<Callback>(named("dead"))
            .jobs(|jobs| async move { members(&jobs, "m", 10, "ok").await })
            .await
            .unwrap();
        settled_count(&log, "ok:success", 1).await;
        settled_count(&log, "done:complete", 1).await;
        assert_eq!(log.count_prefix("m"), 10);
        assert_eq!(log.count("dead:death"), 0);
        let status = wait_status(&client, &committed.bid, "finished", |status| {
            status.finished_at.is_some()
        })
        .await;
        assert_eq!(status.state(), BatchState::Succeeded);
        assert_eq!((status.total, status.succeeded()), (10, 10));
        assert_eq!(counts(&status), (0, 0, 0, 0));
        assert!(status.complete_at.unwrap() <= status.success_at.unwrap());
        assert_eq!(status.description.as_deref(), Some("ten rows"));
        assert!(!status.undispatched);
        // The success callback saw the batch succeed.
        let seen = log.status_seen("ok").unwrap();
        assert!(seen.is_success() && seen.pending == 0);
        // Every key of the batch keeps only its linger.
        let keys_left = batch_keys(&redis).await;
        assert!(!keys_left.is_empty());
        let pool = hench.redis().clone();
        for key in keys_left {
            let ttl = pttl(&pool, &key).await;
            assert!(
                ttl > 0 && ttl <= linger.as_millis() as i64,
                "{key}: {ttl} ms ({fetch:?})"
            );
        }
        assert_eq!(zcard(&pool, keys::BATCH_EVENTS).await, 0);
        launcher.stop().await.unwrap();
    }
}

/// E2, E17: a member failing once: `complete` fires after its first failure (the callback sees
/// it), `success` once the retry succeeded.
#[tokio::test]
async fn a_retried_member_completes_first_and_succeeds_later() {
    for fetch in [FetchStrategy::Reliable, FetchStrategy::Basic] {
        let redis = TestRedis::start().await;
        let log = Arc::new(Log::default());
        let (hench, launcher) = worker(&redis, &log, |builder| builder.fetch(fetch)).await;
        let client = hench.client().clone();
        let pool = hench.redis().clone();
        let committed = Batch::new(&client)
            .on_complete::<Callback>(named("done"))
            .on_success::<Callback>(named("ok"))
            .jobs(|jobs| async move {
                members(&jobs, "m", 3, "ok").await?;
                members(&jobs, "flaky", 1, "fail-once").await
            })
            .await
            .unwrap();
        log.wait_count("done:complete", 1).await;
        let seen = log.status_seen("done").unwrap();
        assert_eq!((seen.pending, seen.failures, seen.dead), (1, 1, 0));
        assert!(seen.success_at.is_none());
        assert_eq!(log.count("ok:success"), 0);
        let failures = batches(&client)
            .failures(&committed.bid, 1, 10)
            .await
            .unwrap()
            .items;
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].reason, "retry");
        assert!(!failures[0].dead && failures[0].next_at.is_some());
        assert_eq!(zcard(&pool, keys::RETRY).await, 1);
        // The retry, now.
        assert_eq!(
            Api::new(pool.clone()).retries().retry_all().await.unwrap(),
            1
        );
        settled_count(&log, "ok:success", 1).await;
        assert_eq!(log.count("done:complete"), 1, "complete fires once");
        let status = status_of(&client, &committed.bid).await;
        assert_eq!(counts(&status), (0, 0, 0, 0));
        assert_eq!(status.state(), BatchState::Succeeded);
        launcher.stop().await.unwrap();
    }
}

/// E3, E4: members in the morgue fire `death` once (the second death fires nothing),
/// `complete` fires, `success` does not; reviving the dead members through the API lets the
/// batch succeed.
#[tokio::test]
async fn deaths_fire_once_and_revivals_succeed() {
    let redis = TestRedis::start().await;
    let log = Arc::new(Log::default());
    let (hench, launcher) = worker(&redis, &log, |builder| builder).await;
    let client = hench.client().clone();
    let pool = hench.redis().clone();
    let committed = Batch::new(&client)
        .on_complete::<Callback>(named("done"))
        .on_success::<Callback>(named("ok"))
        .on_death::<Callback>(named("dead"))
        .jobs(|jobs| async move {
            members(&jobs, "m", 2, "ok").await?;
            for n in 0..2 {
                Work::set()
                    .retry(0u32)
                    .perform_async(&jobs, work(&format!("doomed{n}"), "fail-once"))
                    .await?;
            }
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap();
    log.wait_count("done:complete", 1).await;
    settled_count(&log, "dead:death", 1).await;
    assert_eq!(zcard(&pool, keys::DEAD).await, 2);
    let status = status_of(&client, &committed.bid).await;
    assert_eq!(counts(&status), (2, 2, 2, 0));
    assert_eq!(status.state(), BatchState::Dead);
    assert!(status.death_at.is_some() && status.success_at.is_none());
    for failure in batches(&client)
        .failures(&committed.bid, 1, 10)
        .await
        .unwrap()
        .items
    {
        assert!(failure.dead);
        assert_eq!(failure.reason, "morgue");
        assert_eq!(failure.class, "batch::Work");
    }
    assert_eq!(log.count("ok:success"), 0);
    // Revival: the morgue's jobs retried through the API succeed.
    assert_eq!(Api::new(pool.clone()).dead().retry_all().await.unwrap(), 2);
    settled_count(&log, "ok:success", 1).await;
    assert_eq!(log.count("dead:death"), 1);
    assert_eq!(log.count("done:complete"), 1);
    let revived = status_of(&client, &committed.bid).await;
    assert_eq!(counts(&revived), (0, 0, 0, 0));
    assert_eq!(revived.state(), BatchState::Succeeded);
    launcher.stop().await.unwrap();
}

/// E5: retries disabled, a discard from `retry_in`, and `dead: false` all count as deaths.
#[tokio::test]
async fn members_that_will_not_run_again_are_dead() {
    let redis = TestRedis::start().await;
    let log = Arc::new(Log::default());
    let (hench, launcher) = worker(&redis, &log, |builder| builder).await;
    let client = hench.client().clone();
    let cases = [
        ("no_retry", Work::set().retry(false), "fail"),
        ("discarded", Work::set(), "discard"),
        ("discarded", Work::set().dead(false).retry(0u32), "fail"),
    ];
    let mut bids = Vec::new();
    for (n, (reason, setter, mode)) in cases.into_iter().enumerate() {
        let name = format!("case{n}");
        let committed = Batch::new(&client)
            .on_death::<Callback>(named(&name))
            .jobs(|jobs| async move {
                setter.perform_async(&jobs, work("x", mode)).await?;
                Ok::<_, hench::Error>(())
            })
            .await
            .unwrap();
        bids.push((name, reason, committed.bid));
    }
    for (name, reason, bid) in bids {
        log.wait_count(&format!("{name}:death"), 1).await;
        let status = status_of(&client, &bid).await;
        assert_eq!(counts(&status), (1, 1, 1, 0), "{name}");
        let failures = batches(&client).failures(&bid, 1, 10).await.unwrap().items;
        assert_eq!(failures[0].reason, reason, "{name}");
        assert!(failures[0].dead);
    }
    assert_eq!(
        zcard(hench.redis(), keys::DEAD).await,
        0,
        "none went to the morgue"
    );
    launcher.stop().await.unwrap();
}

/// Does not run jobs whose first argument is `halt` (server middleware that does not call
/// `next`).
struct Halt;

#[async_trait]
impl ServerMiddleware for Halt {
    async fn call(
        &self,
        ctx: &JobContext,
        job: &mut JobPayload,
        next: ServerNext<'_>,
    ) -> Result<(), JobError> {
        if job.args.first() == Some(&json!("halt")) {
            return Ok(());
        }
        next.run(ctx, job).await
    }
}

/// E6: a member that server middleware does not run counts as succeeded.
#[tokio::test]
async fn members_halted_by_server_middleware_succeed() {
    let redis = TestRedis::start().await;
    let log = Arc::new(Log::default());
    let (hench, launcher) = worker(&redis, &log, |builder| builder.server_middleware(Halt)).await;
    let client = hench.client().clone();
    let committed = Batch::new(&client)
        .on_success::<Callback>(named("ok"))
        .jobs(|jobs| async move {
            Work::perform_async(&jobs, work("halt", "fail")).await?;
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap();
    settled_count(&log, "ok:success", 1).await;
    assert_eq!(log.count_prefix("halt"), 0, "never ran");
    assert_eq!(
        counts(&status_of(&client, &committed.bid).await),
        (0, 0, 0, 0)
    );
    launcher.stop().await.unwrap();
}

/// An iterable member: five items, each slower than the iteration budget.
#[derive(Default)]
struct Items;

impl IterableJob for Items {
    const NAME: &'static str = "batch::Items";
    type Args = ();
    type Item = i64;
    type Cursor = usize;

    fn build_enumerator<'a>(
        &'a self,
        _ctx: &'a JobContext,
        _args: &'a (),
        cursor: Option<usize>,
    ) -> Result<Enumerator<'a, i64, usize>, JobError> {
        Ok(Enumerator::vec((0..5).collect(), cursor))
    }

    async fn each_iteration(&self, item: i64, it: &Iteration<'_, Self>) -> Result<(), JobError> {
        tokio::time::sleep(Duration::from_millis(30)).await;
        it.data::<Log>()?.record(format!("item{item}"));
        Ok(())
    }
}

/// E7: an iterable member interrupted after each item (its runtime budget) is not an outcome:
/// the batch completes after the last item.
#[tokio::test]
async fn interrupted_iterable_members_are_not_outcomes() {
    let redis = TestRedis::start().await;
    let log = Arc::new(Log::default());
    let (hench, launcher) = worker(&redis, &log, |builder| {
        builder
            .register(Items)
            .max_iteration_runtime(Duration::from_millis(10))
    })
    .await;
    let client = hench.client().clone();
    let committed = Batch::new(&client)
        .on_complete::<Callback>(named("done"))
        .jobs(|jobs| async move {
            Items::perform_async(&jobs, ()).await?;
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap();
    settled_count(&log, "done:complete", 1).await;
    let entries = log.entries();
    assert_eq!(
        entries,
        ["item0", "item1", "item2", "item3", "item4", "done:complete"]
    );
    let status = status_of(&client, &committed.bid).await;
    assert_eq!(status.state(), BatchState::Succeeded);
    assert_eq!(counts(&status), (0, 0, 0, 0));
    launcher.stop().await.unwrap();
}

/// E8: a member stopping with `hench::Shutdown` at a shutdown is requeued, not counted; after
/// a restart it is counted once.
#[tokio::test]
async fn members_requeued_at_shutdown_are_counted_after_the_restart() {
    let redis = TestRedis::start().await;
    let log = Arc::new(Log::default());
    let (hench, launcher) = worker(&redis, &log, |builder| builder).await;
    let client = hench.client().clone();
    let pool = hench.redis().clone();
    let committed = Batch::new(&client)
        .on_complete::<Callback>(named("done"))
        .on_success::<Callback>(named("ok"))
        .jobs(|jobs| async move { members(&jobs, "safe", 1, "shutdown-once").await })
        .await
        .unwrap();
    log.wait_count("safe0", 1).await;
    launcher.stop().await.unwrap();
    assert_eq!(queue_size(&pool, "default").await, 1, "requeued");
    let paused = status_of(&client, &committed.bid).await;
    assert_eq!(counts(&paused), (1, 0, 0, 0), "not an outcome");
    assert!(paused.complete_at.is_none());
    let (_hench, restarted) = worker(&redis, &log, |builder| builder).await;
    settled_count(&log, "ok:success", 1).await;
    assert_eq!(log.count("done:complete"), 1);
    assert_eq!(log.count("safe0"), 2);
    restarted.stop().await.unwrap();
}

/// E9: a member interrupted by a hard shutdown is requeued and counted once, after it ran
/// again.
#[tokio::test]
async fn members_interrupted_by_a_hard_shutdown_are_counted_once() {
    let redis = TestRedis::start().await;
    let log = Arc::new(Log::default());
    let (hench, launcher) = worker(&redis, &log, |builder| {
        builder.timeout(Duration::from_millis(300))
    })
    .await;
    let client = hench.client().clone();
    let pool = hench.redis().clone();
    let committed = Batch::new(&client)
        .on_success::<Callback>(named("ok"))
        .jobs(|jobs| async move { members(&jobs, "stuck", 1, "hang-once").await })
        .await
        .unwrap();
    log.wait_count("stuck0", 1).await;
    launcher.stop().await.unwrap();
    assert_eq!(queue_size(&pool, "default").await, 1, "requeued");
    assert_eq!(
        counts(&status_of(&client, &committed.bid).await),
        (1, 0, 0, 0)
    );
    let (_hench, restarted) = worker(&redis, &log, |builder| builder).await;
    settled_count(&log, "ok:success", 1).await;
    assert_eq!(
        counts(&status_of(&client, &committed.bid).await),
        (0, 0, 0, 0)
    );
    restarted.stop().await.unwrap();
}

/// E10, E18: cancelled members are skipped before they run (counted as processed, not failed,
/// with the `s` metric), `complete` fires with `cancelled_at`, `success` never; a long member
/// stops at its next `check_batch`; `batch_cancelled()` from a member of a live batch is an
/// ordinary failure.
#[tokio::test]
async fn cancelled_batches_skip_their_members() {
    let redis = TestRedis::start().await;
    let log = Arc::new(Log::default());
    let pool = redis.pool().await;
    let client = redis.client().await;
    let committed = Batch::new(&client)
        .on_complete::<Callback>(named("done"))
        .on_success::<Callback>(named("ok"))
        .jobs(|jobs| async move { members(&jobs, "m", 3, "ok").await })
        .await
        .unwrap();
    assert_eq!(client.cancel_batch(&committed.bid).await.unwrap(), 1);
    let (hench, launcher) = worker(&redis, &log, |builder| builder).await;
    settled_count(&log, "done:complete", 1).await;
    assert_eq!(log.count_prefix("m"), 0, "perform never ran");
    assert_eq!(log.count("ok:success"), 0);
    let seen = log.status_seen("done").unwrap();
    assert!(seen.cancelled_at.is_some());
    let status = status_of(&client, &committed.bid).await;
    assert_eq!(counts(&status), (0, 0, 0, 3));
    assert_eq!(status.state(), BatchState::Cancelled);
    // Processed, not failed (E18), once the counters are flushed.
    wait_for("the processed counter", WAIT, || async {
        get_int(&pool, keys::STAT_PROCESSED).await >= 4
    })
    .await;
    assert_eq!(get_int(&pool, keys::STAT_FAILED).await, 0);
    let now = chrono::Utc::now();
    let mut skipped = 0;
    for minute in [now, now - chrono::Duration::minutes(1)] {
        let metrics = hgetall(&pool, &keys::metrics_minute(minute)).await;
        skipped += metrics
            .get("batch::Work|s")
            .map_or(0, |value| value.parse::<u64>().unwrap());
    }
    assert_eq!(skipped, 3);

    // A long member stops at its next check.
    let watched = Batch::new(hench.client())
        .on_complete::<Callback>(named("watched"))
        .jobs(|jobs| async move { members(&jobs, "watcher", 1, "watch").await })
        .await
        .unwrap();
    log.wait_count("watcher0", 1).await;
    hench.client().cancel_batch(&watched.bid).await.unwrap();
    settled_count(&log, "watched:complete", 1).await;
    assert_eq!(
        counts(&status_of(&client, &watched.bid).await),
        (0, 0, 0, 1)
    );
    assert_eq!(zcard(&pool, keys::RETRY).await, 0);

    // `batch_cancelled()` from a member of a live batch: a failure.
    let live = Batch::new(hench.client())
        .jobs(|jobs| async move { members(&jobs, "liar", 1, "cancelled").await })
        .await
        .unwrap();
    wait_for("the failure", WAIT, || async {
        zcard(&pool, keys::RETRY).await == 1
    })
    .await;
    let status = status_of(&client, &live.bid).await;
    assert_eq!(counts(&status), (1, 1, 0, 0));
    launcher.stop().await.unwrap();
}

/// A loader: adds 50 members to its own batch under its own commit id; with `fail`, its first
/// run fails after the commit.
#[derive(Default)]
struct Loader;

impl Job for Loader {
    const NAME: &'static str = "batch::Loader";
    type Args = (u32, bool);

    async fn perform(&self, ctx: &JobContext, (n, fail): Self::Args) -> Result<(), JobError> {
        let log = ctx.data::<Log>()?;
        let attempt = log.attempt(ctx.jid());
        let batch = ctx
            .batch()
            .ok_or_else(|| JobError::msg("a loader runs in a batch"))?;
        batch
            .commit_id(format!("load-{n}"))
            .jobs(|jobs| async move {
                let args = (0..50)
                    .map(|i| work(&format!("row{n}-{i}"), "ok"))
                    .collect();
                Work::perform_bulk(&jobs, args).await?;
                Ok::<_, hench::Error>(())
            })
            .await?;
        if fail && attempt == 1 {
            return Err(JobError::msg("the loader failed after its commit"));
        }
        Ok(())
    }
}

/// E11: 20 loaders × 50 members, one failing after its commit and retried: no duplicate
/// member, one `success`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn loaders_add_their_members_once() {
    let redis = TestRedis::start().await;
    let log = Arc::new(Log::default());
    let (hench, launcher) = worker(&redis, &log, |builder| {
        builder.register(Loader).concurrency(20)
    })
    .await;
    let client = hench.client().clone();
    let pool = hench.redis().clone();
    let committed = Batch::new(&client)
        .description("huge import")
        .on_success::<Callback>(named("imported"))
        .jobs(|jobs| async move {
            let args = (0..20).map(|n| (n, n == 7)).collect();
            Loader::perform_bulk(&jobs, args).await?;
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap();
    wait_for("the failed loader", WAIT, || async {
        zcard(&pool, keys::RETRY).await == 1
    })
    .await;
    assert_eq!(log.count("imported:success"), 0);
    Api::new(pool.clone()).retries().retry_all().await.unwrap();
    settled_count(&log, "imported:success", 1).await;
    let status = status_of(&client, &committed.bid).await;
    assert_eq!(status.total, 20 + 20 * 50);
    assert_eq!(counts(&status), (0, 0, 0, 0));
    for n in 0..20 {
        for i in 0..50 {
            assert_eq!(log.count(&format!("row{n}-{i}")), 1, "row{n}-{i}");
        }
    }
    launcher.stop().await.unwrap();
}

/// E12: a child's callbacks are members of its parent: the child's `complete` callback runs
/// before the parent's `complete`, its `success` callback before the parent's `success`; a
/// child's death is its parent's.
#[tokio::test]
async fn children_finish_before_their_parents() {
    let redis = TestRedis::start().await;
    let log = Arc::new(Log::default());
    let (hench, launcher) = worker(&redis, &log, |builder| builder).await;
    let client = hench.client().clone();
    let committed = Batch::new(&client)
        .on_complete::<Callback>(named("P"))
        .on_success::<Callback>(named("P"))
        .jobs(|jobs| async move {
            members(&jobs, "p", 2, "ok").await?;
            Batch::new(&jobs)
                .on_complete::<Callback>(named("C"))
                .on_success::<Callback>(named("C"))
                .jobs(|inner| async move { members(&inner, "c", 2, "ok").await })
                .await?;
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap();
    settled_count(&log, "P:success", 1).await;
    settled_count(&log, "P:complete", 1).await;
    let at = |entry: &str| log.position(entry).unwrap();
    assert!(at("C:complete") < at("P:complete"), "{:?}", log.entries());
    assert!(at("C:success") < at("P:success"), "{:?}", log.entries());
    assert_eq!(log.count("C:complete") + log.count("C:success"), 2);
    let parent = status_of(&client, &committed.bid).await;
    // The parent's two members and the child's two callbacks.
    assert_eq!(
        (
            parent.total,
            parent.children_complete,
            parent.children_succeeded
        ),
        (4, 1, 1)
    );

    // A child's death is its parent's.
    let doomed = Batch::new(&client)
        .on_death::<Callback>(named("P2"))
        .jobs(|jobs| async move {
            Batch::new(&jobs)
                .on_death::<Callback>(named("C2"))
                .jobs(|inner| async move {
                    Work::set()
                        .retry(0u32)
                        .perform_async(&inner, work("doomed", "fail"))
                        .await?;
                    Ok::<_, hench::Error>(())
                })
                .await?;
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap();
    settled_count(&log, "P2:death", 1).await;
    assert_eq!(log.count("C2:death"), 1);
    assert!(status_of(&client, &doomed.bid).await.death_at.is_some());
    launcher.stop().await.unwrap();
}

/// A step of a hand-written workflow: a callback of the previous step's batch (so a member of
/// the overall batch), which reopens the overall batch and defines its own step's batch.
#[derive(Default)]
struct Step;

/// The jobs of each step of the fulfilment workflow (steps run their jobs in parallel).
const STEPS: [&[&str]; 5] = [
    &["reserve"],
    &["charge", "label"],
    &["pack-1", "pack-2", "pack-3"],
    &["ship"],
    &["notify-email", "notify-sms"],
];

/// Defines step `n` (from 1) of the workflow through `jobs`.
async fn define_step(jobs: &Client, n: usize) -> hench::Result<()> {
    let mut step = Batch::new(jobs).description(format!("step {n}"));
    if n < STEPS.len() {
        step = step.on_success::<Step>((n + 1,));
    }
    step.jobs(|inner| async move {
        for tag in STEPS[n - 1] {
            Work::perform_async(&inner, work(tag, "ok")).await?;
        }
        Ok::<_, hench::Error>(())
    })
    .await?;
    Ok(())
}

impl Job for Step {
    const NAME: &'static str = "batch::Step";
    type Args = (usize,);

    async fn perform(&self, ctx: &JobContext, (n,): Self::Args) -> Result<(), JobError> {
        let overall = ctx
            .batch()
            .ok_or_else(|| JobError::msg("a step is a member of its workflow"))?;
        overall
            .commit_id(format!("step-{n}"))
            .jobs(|jobs| async move { define_step(&jobs, n).await })
            .await?;
        Ok(())
    }
}

/// E13: an order-fulfilment workflow written with callbacks that reopen the overall batch: the
/// steps run in order, their jobs in parallel, and `shipped` fires once at the end.
#[tokio::test]
async fn callbacks_reopening_their_parent_chain_steps() {
    let redis = TestRedis::start().await;
    let log = Arc::new(Log::default());
    let (hench, launcher) = worker(&redis, &log, |builder| builder.register(Step)).await;
    let client = hench.client().clone();
    let committed = Batch::new(&client)
        .description("order 42")
        .on_success::<Callback>(named("shipped"))
        .jobs(|jobs| async move { define_step(&jobs, 1).await })
        .await
        .unwrap();
    settled_count(&log, "shipped:success", 1).await;
    let mut last_of_previous = 0;
    for (n, step) in STEPS.iter().enumerate() {
        let positions: Vec<usize> = step
            .iter()
            .map(|tag| log.position(tag).unwrap_or_else(|| panic!("{tag} ran")))
            .collect();
        assert!(
            positions.iter().all(|&at| at >= last_of_previous),
            "step {} ran early: {:?}",
            n + 1,
            log.entries()
        );
        last_of_previous = positions.into_iter().max().unwrap();
        for tag in *step {
            assert_eq!(log.count(tag), 1, "{tag}");
        }
    }
    assert_eq!(
        log.position("shipped:success"),
        Some(log.entries().len() - 1)
    );
    let status = status_of(&client, &committed.bid).await;
    assert_eq!(status.children, 5);
    assert_eq!(status.state(), BatchState::Succeeded);
    launcher.stop().await.unwrap();
}

/// Runs every `hench::WorkflowNext` a second time, as a duplicate execution would (the same
/// job, same jid, pushed again once it ran).
#[derive(Default)]
struct Twice(std::sync::Mutex<std::collections::HashSet<String>>);

#[async_trait]
impl ServerMiddleware for Twice {
    async fn call(
        &self,
        ctx: &JobContext,
        job: &mut JobPayload,
        next: ServerNext<'_>,
    ) -> Result<(), JobError> {
        let copy = job.clone();
        next.run(ctx, job).await?;
        if copy.class == hench::batch::WORKFLOW_NEXT
            && self.0.lock().unwrap().insert(copy.jid.clone())
        {
            ctx.client().push(copy).await?;
            ctx.data::<Log>()?.record("duplicated");
        }
        Ok(())
    }
}

/// E14: the `Workflow` API: stages in order (a duplicate `WorkflowNext` adds nothing); a dead
/// member is the workflow's death, and skipping it lets the workflow continue.
#[tokio::test]
async fn workflows_run_their_stages_in_order() {
    let redis = TestRedis::start().await;
    let log = Arc::new(Log::default());
    let (hench, launcher) = worker(&redis, &log, |builder| builder).await;
    let client = hench.client().clone();
    let started = Workflow::new(&client)
        .description("three stages")
        .stage(|s| s.job::<Work>(work("a", "ok")))
        .stage(|s| {
            s.job::<Work>(work("b1", "ok"))
                .job::<Work>(work("b2", "ok"))
        })
        .stage(|s| s.job::<Work>(work("c", "ok")))
        .on_success::<Callback>(named("wf"))
        .start()
        .await
        .unwrap();
    settled_count(&log, "wf:success", 1).await;
    let at = |entry: &str| log.position(entry).unwrap();
    assert!(at("a") < at("b1") && at("a") < at("b2"));
    assert!(at("b1") < at("c") && at("b2") < at("c"));
    for tag in ["a", "b1", "b2", "c"] {
        assert_eq!(log.count(tag), 1, "{tag}");
    }
    let overall = status_of(&client, &started.bid).await;
    assert_eq!(overall.workflow_stages, Some(3));
    assert_eq!(overall.children, 3);
    launcher.stop().await.unwrap();

    // A dead member is the workflow's death; skipping it continues the workflow.
    let pool = hench.redis().clone();
    let (hench, launcher) = worker(&redis, &log, |builder| builder).await;
    let client = hench.client().clone();
    let started = Workflow::new(&client)
        .stage(|s| s.job_with::<Work>(Work::set().retry(0u32), work("x", "fail")))
        .stage(|s| s.job::<Work>(work("y", "ok")))
        .on_death::<Callback>(named("wf2"))
        .on_success::<Callback>(named("wf2"))
        .start()
        .await
        .unwrap();
    settled_count(&log, "wf2:death", 1).await;
    assert_eq!(log.count("y"), 0, "the next stage never started");
    let (dead, _) = sorted_jobs(&pool, keys::DEAD).await.remove(0);
    let stage = dead.bid.clone().unwrap();
    assert_ne!(stage, started.bid);
    assert!(batches(&client).skip(&stage, &dead.jid).await.unwrap());
    settled_count(&log, "wf2:success", 1).await;
    assert_eq!(log.count("y"), 1);
    launcher.stop().await.unwrap();
}

/// E14: a duplicate execution of `hench::WorkflowNext` (the same job run twice) adds nothing.
#[tokio::test]
async fn a_duplicate_workflow_step_adds_nothing() {
    let redis = TestRedis::start().await;
    let log = Arc::new(Log::default());
    let (hench, launcher) = worker(&redis, &log, |builder| {
        builder.server_middleware(Twice::default())
    })
    .await;
    let client = hench.client().clone();
    let started = Workflow::new(&client)
        .stage(|s| s.job::<Work>(work("a", "ok")))
        .stage(|s| s.job::<Work>(work("b", "ok")))
        .stage(|s| s.job::<Work>(work("c", "ok")))
        .on_success::<Callback>(named("wf"))
        .start()
        .await
        .unwrap();
    settled_count(&log, "wf:success", 1).await;
    // Both steps (to stages 2 and 3) ran twice; each stage's job ran once.
    settled_count(&log, "duplicated", 2).await;
    for tag in ["a", "b", "c"] {
        assert_eq!(log.count(tag), 1, "{tag}");
    }
    let overall = status_of(&client, &started.bid).await;
    assert_eq!(overall.children, 3);
    launcher.stop().await.unwrap();
}

/// E15: callbacks go to `callback_queue`, unless their setter names a queue.
#[tokio::test]
async fn callback_queues_are_honoured() {
    let redis = TestRedis::start().await;
    let log = Arc::new(Log::default());
    let (hench, launcher) = worker(&redis, &log, |builder| builder.queues(["default"])).await;
    let client = hench.client().clone();
    let pool = hench.redis().clone();
    Batch::new(&client)
        .callback_queue("low")
        .on_success::<Callback>(named("plain"))
        .on(
            BatchEvent::Success,
            Callback::set().queue("critical"),
            named("explicit"),
        )
        .jobs(|jobs| async move { members(&jobs, "m", 1, "ok").await })
        .await
        .unwrap();
    wait_for("the callbacks", WAIT, || async {
        queue_size(&pool, "low").await == 1 && queue_size(&pool, "critical").await == 1
    })
    .await;
    assert_eq!(queue_jobs(&pool, "low").await[0].args, [json!("plain")]);
    assert_eq!(
        queue_jobs(&pool, "critical").await[0].args,
        [json!("explicit")]
    );
    launcher.stop().await.unwrap();
}

/// E16: a process crashes after its accounting fired `success`, before the callbacks were
/// pushed (here its dispatch fails on a callback queue of the wrong type); another process's
/// maintenance dispatches them within its interval plus the grace, once.
#[tokio::test]
async fn maintenance_dispatches_what_a_crashed_process_left() {
    let redis = TestRedis::start().await;
    let log = Arc::new(Log::default());
    let pool = redis.pool().await;
    cmd::<()>(&pool, &redis_cmd!("SET", keys::queue("cbq"), "in the way")).await;
    let (hench, crashed) = worker(&redis, &log, |builder| builder).await;
    let client = hench.client().clone();
    let committed = Batch::new(&client)
        .on(
            BatchEvent::Success,
            Callback::set().queue("cbq"),
            named("ok"),
        )
        .jobs(|jobs| async move { members(&jobs, "m", 1, "ok").await })
        .await
        .unwrap();
    let fired = wait_status(&client, &committed.bid, "success fired", |status| {
        status.success_at.is_some()
    })
    .await;
    assert!(fired.undispatched && fired.success_dispatched_at.is_none());
    crashed.abandon();
    cmd::<()>(&pool, &redis_cmd!("DEL", keys::queue("cbq"))).await;
    let started = std::time::Instant::now();
    let (_hench, survivor) =
        worker(&redis, &log, |builder| builder.queues(["default", "cbq"])).await;
    settled_count(&log, "ok:success", 1).await;
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
    let status = status_of(&client, &committed.bid).await;
    assert!(!status.undispatched && status.success_dispatched_at.is_some());
    assert_eq!(zcard(&pool, keys::BATCH_EVENTS).await, 0);
    survivor.stop().await.unwrap();
}

/// A job that adds a child batch to its own batch, down to level `max`: each level's
/// `complete` callback is recorded as `L<level>`.
#[derive(Default)]
struct Nest;

impl Job for Nest {
    const NAME: &'static str = "batch::Nest";
    type Args = (u32, u32);

    async fn perform(&self, ctx: &JobContext, (level, max): Self::Args) -> Result<(), JobError> {
        ctx.data::<Log>()?.record(format!("nest{level}"));
        let own = ctx
            .batch()
            .ok_or_else(|| JobError::msg("a nest job runs in a batch"))?;
        own.commit_id(format!("nest-{level}"))
            .jobs(|jobs| async move {
                let next = level + 1;
                Batch::new(&jobs)
                    .on_complete::<Callback>(named(&format!("L{next}")))
                    .jobs(|inner| async move {
                        if next < max {
                            Nest::perform_async(&inner, (next, max)).await?;
                        } else {
                            Work::perform_async(&inner, work("leaf", "ok")).await?;
                        }
                        Ok::<_, hench::Error>(())
                    })
                    .await?;
                Ok::<_, hench::Error>(())
            })
            .await?;
        Ok(())
    }
}

/// E19: four levels of batches, each created by a job of the level above: every level's
/// callback once, the deepest first and the outermost last.
#[tokio::test]
async fn deep_nesting_finishes_inside_out() {
    let redis = TestRedis::start().await;
    let log = Arc::new(Log::default());
    let (hench, launcher) = worker(&redis, &log, |builder| builder.register(Nest)).await;
    let client = hench.client().clone();
    let committed = Batch::new(&client)
        .on_complete::<Callback>(named("L1"))
        .jobs(|jobs| async move {
            Nest::perform_async(&jobs, (1, 4)).await?;
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap();
    settled_count(&log, "L1:complete", 1).await;
    let order: Vec<String> = log
        .entries()
        .into_iter()
        .filter(|entry| entry.starts_with('L'))
        .collect();
    assert_eq!(
        order,
        ["L4:complete", "L3:complete", "L2:complete", "L1:complete"]
    );
    assert_eq!(log.count("leaf"), 1);
    let root = status_of(&client, &committed.bid).await;
    assert_eq!(root.state(), BatchState::Succeeded);
    assert_eq!(root.children, 1);
    launcher.stop().await.unwrap();
}

/// E20: a queued member deleted and a retrying member killed through the API count as dead
/// (`deleted`, `killed`): `death` fires, then `complete`.
#[tokio::test]
async fn members_removed_through_the_api_are_dead() {
    let redis = TestRedis::start().await;
    let log = Arc::new(Log::default());
    let (hench, launcher) = worker(&redis, &log, |builder| builder).await;
    let client = hench.client().clone();
    let pool = hench.redis().clone();
    let committed = Batch::new(&client)
        .on_complete::<Callback>(named("done"))
        .on_death::<Callback>(named("dead"))
        .jobs(|jobs| async move {
            let parked = Work::set()
                .queue("parked")
                .perform_async(&jobs, work("parked", "ok"))
                .await?
                .unwrap();
            let failing = Work::perform_async(&jobs, work("failing", "fail"))
                .await?
                .unwrap();
            Ok::<_, hench::Error>((parked, failing))
        })
        .await
        .unwrap();
    let (parked, failing) = committed.value.clone();
    wait_for("the failure", WAIT, || async {
        zcard(&pool, keys::RETRY).await == 1
    })
    .await;
    let api = Api::new(pool.clone());
    let record = api
        .queue("parked")
        .find_job(&parked)
        .await
        .unwrap()
        .unwrap();
    assert!(record.delete().await.unwrap());
    // Every member ran or died: `complete` fires with `death`.
    log.wait_count("dead:death", 1).await;
    log.wait_count("done:complete", 1).await;
    let entry = api.retries().find_job(&failing).await.unwrap().unwrap();
    assert!(entry.kill().await.unwrap());
    wait_status(&client, &committed.bid, "the kill counted", |status| {
        status.dead == 2
    })
    .await;
    // No event fires twice.
    settled_count(&log, "done:complete", 1).await;
    assert_eq!(log.count("dead:death"), 1);
    let status = status_of(&client, &committed.bid).await;
    assert_eq!(counts(&status), (2, 2, 2, 0));
    let mut reasons: Vec<(String, String)> = batches(&client)
        .failures(&committed.bid, 1, 10)
        .await
        .unwrap()
        .items
        .into_iter()
        .map(|failure| (failure.jid, failure.reason))
        .collect();
    reasons.sort();
    let mut expected = vec![
        (parked, "deleted".to_owned()),
        (failing, "killed".to_owned()),
    ];
    expected.sort();
    assert_eq!(reasons, expected);
    launcher.stop().await.unwrap();
}

/// E21: members in a paused queue keep their batch pending until it is unpaused; a paused
/// callback queue holds the callbacks without holding up the dispatch.
#[tokio::test]
async fn paused_queues_hold_members_and_callbacks() {
    let redis = TestRedis::start().await;
    let log = Arc::new(Log::default());
    let pool = redis.pool().await;
    let api = Api::new(pool.clone());
    api.queue("default").pause().await.unwrap();
    api.queue("critical").pause().await.unwrap();
    let (hench, launcher) = worker(&redis, &log, |builder| builder).await;
    let client = hench.client().clone();
    let committed = Batch::new(&client)
        .callback_queue("critical")
        .on_complete::<Callback>(named("done"))
        .on_success::<Callback>(named("ok"))
        .jobs(|jobs| async move { members(&jobs, "m", 2, "ok").await })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(log.entries(), Vec::<String>::new());
    assert_eq!(
        counts(&status_of(&client, &committed.bid).await),
        (2, 0, 0, 0)
    );
    api.queue("default").unpause().await.unwrap();
    let dispatched = wait_status(&client, &committed.bid, "dispatched", |status| {
        status.success_dispatched_at.is_some()
    })
    .await;
    assert!(!dispatched.undispatched);
    assert_eq!(queue_size(&pool, "critical").await, 2, "held by the pause");
    assert_eq!(log.count_prefix("done") + log.count_prefix("ok"), 0);
    api.queue("critical").unpause().await.unwrap();
    settled_count(&log, "ok:success", 1).await;
    settled_count(&log, "done:complete", 1).await;
    launcher.stop().await.unwrap();
}

/// batches-1: a stage member failing once completes its stage, but not the workflow: the
/// workflow's `complete` fires once, after the last stage ran (it fired at stage 1's first
/// failure, before the later stages existed, and never again).
#[tokio::test]
async fn a_workflow_completes_after_its_last_stage() {
    let redis = TestRedis::start().await;
    let log = Arc::new(Log::default());
    let (hench, launcher) = worker(&redis, &log, |builder| builder).await;
    let client = hench.client().clone();
    let pool = hench.redis().clone();
    let started = Workflow::new(&client)
        .stage(|s| s.job::<Work>(work("a", "fail-once")))
        .stage(|s| s.job::<Work>(work("b", "ok")))
        .stage(|s| s.job::<Work>(work("c", "ok")))
        .on_complete::<Callback>(named("wf"))
        .on_success::<Callback>(named("wf"))
        .start()
        .await
        .unwrap();
    wait_for("a's first failure", WAIT, || async {
        zcard(&pool, keys::RETRY).await == 1
    })
    .await;
    // Stage 1 completed (its member ran once); its `complete` reached the workflow.
    let held = wait_status(&client, &started.bid, "stage 1's complete", |status| {
        status.children_complete == 1
    })
    .await;
    assert!(held.complete_at.is_none());
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(log.count("wf:complete"), 0, "{:?}", log.entries());
    Api::new(pool.clone()).retries().retry_all().await.unwrap();
    settled_count(&log, "wf:success", 1).await;
    assert_eq!(log.count("wf:complete"), 1, "{:?}", log.entries());
    assert!(log.position("c").unwrap() < log.position("wf:complete").unwrap());
    let seen = log.status_seen("wf").unwrap();
    assert_eq!(seen.children, 3);
    launcher.stop().await.unwrap();
}

/// docs-usability-2: `hench::WorkflowNext` runs in the queue of the stage it continues, so a
/// workflow whose jobs and callbacks use queues other than `default` runs to its end on
/// workers that never poll `default` (it stalled after stage 1, without a word).
#[tokio::test]
async fn workflow_steps_run_in_the_queue_of_the_stage_they_continue() {
    let redis = TestRedis::start().await;
    let log = Arc::new(Log::default());
    let (hench, launcher) = worker(&redis, &log, |builder| builder.queues(["orders"])).await;
    let client = hench.client().clone();
    let pool = hench.redis().clone();
    let in_orders = || Work::set().queue("orders");
    let started = Workflow::new(&client)
        .stage(|s| s.job_with::<Work>(in_orders(), work("reserve", "ok")))
        .stage(|s| s.job_with::<Work>(in_orders(), work("charge", "ok")))
        .stage(|s| s.job_with::<Work>(in_orders(), work("ship", "ok")))
        .on(
            BatchEvent::Success,
            Callback::set().queue("orders"),
            named("shipped"),
        )
        .start()
        .await
        .unwrap();
    settled_count(&log, "shipped:success", 1).await;
    for tag in ["reserve", "charge", "ship"] {
        assert_eq!(log.count(tag), 1, "{tag}");
    }
    assert_eq!(queue_size(&pool, "default").await, 0);
    let status = status_of(&client, &started.bid).await;
    assert_eq!(status.state(), BatchState::Succeeded);
    launcher.stop().await.unwrap();

    // With a `callback_queue`, the steps go there.
    let (hench, launcher) =
        worker(&redis, &log, |builder| builder.queues(["orders", "steps"])).await;
    let client = hench.client().clone();
    Workflow::new(&client)
        .callback_queue("steps")
        .stage(|s| s.job_with::<Work>(in_orders(), work("one", "ok")))
        .stage(|s| s.job_with::<Work>(in_orders(), work("two", "ok")))
        .on_success::<Callback>(named("steps"))
        .start()
        .await
        .unwrap();
    settled_count(&log, "steps:success", 1).await;
    assert_eq!(log.count("two"), 1);
    launcher.stop().await.unwrap();
}

/// batches-2: members of a cancelled batch that failed before the cancel wait in the retry
/// set; the batch remembers its cancellation until each comes up and is skipped (its linger
/// started at `complete`, and a retry due after it ran the member).
#[tokio::test]
async fn a_cancelled_batch_skips_members_that_come_back_late() {
    let redis = TestRedis::start().await;
    let log = Arc::new(Log::default());
    let (hench, launcher) = worker(&redis, &log, |builder| builder).await;
    let client = hench.client().clone();
    let pool = hench.redis().clone();
    let committed = Batch::new(&client)
        .linger(Duration::from_secs(30))
        .on_complete::<Callback>(named("done"))
        .jobs(|jobs| async move {
            members(&jobs, "ok", 1, "ok").await?;
            members(&jobs, "flaky", 1, "fail-once").await
        })
        .await
        .unwrap();
    settled_count(&log, "done:complete", 1).await;
    assert_eq!(client.cancel_batch(&committed.bid).await.unwrap(), 1);
    let status = status_of(&client, &committed.bid).await;
    assert!(status.cancelled_at.is_some());
    assert!(status.finished_at.is_none(), "a member is still pending");
    assert!(pttl(&pool, &keys::batch(&committed.bid)).await > 30_000);
    // The retry comes due: skipped, and the batch's linger starts.
    assert_eq!(
        Api::new(pool.clone()).retries().retry_all().await.unwrap(),
        1
    );
    let status = wait_status(&client, &committed.bid, "the skip", |status| {
        status.skipped == 1
    })
    .await;
    assert!(status.finished_at.is_some());
    assert!(pttl(&pool, &keys::batch(&committed.bid)).await <= 30_000);
    assert_eq!(
        log.count("flaky0"),
        1,
        "the cancelled member did not run again"
    );
    launcher.stop().await.unwrap();
}

/// batches-7: a batch created with a chosen bid and commit id is not created again by a re-run
/// after its linger: its commit ids outlive the linger by its expiry.
#[tokio::test]
async fn a_chosen_bid_is_not_created_again_after_its_linger() {
    let redis = TestRedis::start().await;
    let log = Arc::new(Log::default());
    let (hench, launcher) = worker(&redis, &log, |builder| builder).await;
    let client = hench.client().clone();
    let pool = hench.redis().clone();
    let bid = "0000000000000000000000a7";
    let create = || async {
        Batch::with_bid(&client, bid)
            .unwrap()
            .commit_id("creator-jid")
            .linger(Duration::from_secs(30))
            .on_success::<Callback>(named("created"))
            .jobs(|jobs| async move {
                Work::perform_async(&jobs, work("side-effect", "ok")).await?;
                Ok::<_, hench::Error>(())
            })
            .await
            .unwrap()
    };
    assert!(!create().await.already_applied);
    settled_count(&log, "created:success", 1).await;
    wait_status(&client, bid, "the finish", |status| {
        status.finished_at.is_some()
    })
    .await;
    // The linger ends: every key whose expiry is at most the linger goes.
    for key in batch_keys(&redis).await {
        if pttl(&pool, &key).await <= 30_000 {
            del(&pool, &key).await;
        }
    }
    assert!(client.batch_status(bid).await.unwrap().is_none());
    assert!(create().await.already_applied);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(log.count("side-effect"), 1);
    launcher.stop().await.unwrap();
}
