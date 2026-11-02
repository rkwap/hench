//! Limiters against a private Redis (`docs/design/ratelimit.md` §3.3): waits in process,
//! concurrency permits (leases, renewals, releases, wake-ups), points corrections, failures, and
//! the in-memory target on tokio's clock.

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use hench::limit::testing::memory_target;
use hench::limit::{Interval, Limit, LimitTarget, Limiter, OverLimit};
use hench::prelude::*;
use hench::test_util::TestRedis;
use hench::{CancellationToken, Error, keys};
use support::*;

/// The refusal inside an `Error::OverLimit`.
#[track_caller]
fn over(result: Result<hench::limit::Permit, Error>) -> OverLimit {
    match result {
        Err(Error::OverLimit(over)) => over,
        Err(other) => panic!("expected a refusal, got the error {other}"),
        Ok(permit) => panic!("expected a refusal, got {permit:?}"),
    }
}

#[tokio::test]
async fn acquisitions_wait_for_a_slot_within_max_wait() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let limiter =
        Limiter::window("wait", 2, Duration::from_millis(400)).max_wait(Duration::from_secs(2));
    let _a = limiter.acquire(&pool).await.unwrap();
    let _b = limiter.acquire(&pool).await.unwrap();
    // `try_acquire` never waits, even with time to spare.
    let started = Instant::now();
    let _ = over(limiter.try_acquire(&pool).await);
    assert!(started.elapsed() < Duration::from_millis(300));
    // The third waits for the window to slide (about 400 ms, plus up to 10 % jitter).
    let started = Instant::now();
    let third = limiter.acquire(&pool).await.unwrap();
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_millis(300) && elapsed < Duration::from_millis(1_500),
        "{elapsed:?}"
    );
    assert!(third.waited() >= Duration::from_millis(300));
    // Beyond max_wait: refused at once, with the hint.
    let impatient =
        Limiter::window("wait-long", 1, Duration::from_secs(60)).max_wait(Duration::from_secs(1));
    let _held = impatient.acquire(&pool).await.unwrap();
    let started = Instant::now();
    let refused = over(impatient.acquire(&pool).await);
    assert!(started.elapsed() < Duration::from_millis(300));
    let after = refused.retry_after().unwrap();
    assert!(
        after > Duration::from_secs(55) && after <= Duration::from_secs(60),
        "{after:?}"
    );
    assert_eq!(refused.label(), "w:wait-long");
    assert!(refused.to_string().contains("window=1/1m"), "{refused}");
    // Counters: grants, refusals, and a grant after a wait.
    let stats = hgetall(&pool, &keys::limit_stats('w', "wait")).await;
    assert_eq!(stats["g"], "3");
    assert_eq!(stats["w"], "1");
    assert!(stats["r"].parse::<u32>().unwrap() >= 1);
}

#[tokio::test]
async fn concurrency_waiters_wake_on_local_releases_and_poll_for_others() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let erp = Limiter::concurrent("erp", 1).max_wait(Duration::from_secs(5));
    let held = erp.acquire(&pool).await.unwrap();
    let waiter = {
        let erp = erp.clone();
        let pool = pool.clone();
        tokio::spawn(async move {
            let permit = erp.acquire(&pool).await;
            (Instant::now(), permit)
        })
    };
    // Let the waiter start polling (its backoff grows), then release in this process.
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    let released_at = Instant::now();
    held.release().await.unwrap();
    let (granted_at, permit) = waiter.await.unwrap();
    let permit = permit.unwrap();
    assert!(
        granted_at.duration_since(released_at) < Duration::from_millis(150),
        "woken {:?} after the release",
        granted_at.duration_since(released_at)
    );
    // A holder in another process: its lease disappears without a local wake-up; the waiter
    // notices by polling (at most 400 ms apart).
    let waiter = {
        let erp = erp.clone();
        let pool = pool.clone();
        tokio::spawn(async move { (erp.acquire(&pool).await, Instant::now()) })
    };
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    let gone_at = Instant::now();
    del(&pool, &keys::limit_state('c', "erp")).await;
    let (second, granted_at) = waiter.await.unwrap();
    assert!(second.is_ok());
    assert!(
        granted_at.duration_since(gone_at) < Duration::from_millis(700),
        "{:?}",
        granted_at.duration_since(gone_at)
    );
    drop(permit);
}

#[tokio::test]
async fn run_releases_after_the_future_and_when_it_panics_or_is_dropped() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let mutex = Limiter::concurrent("mutex", 1).max_wait(Duration::ZERO);
    let state = keys::limit_state('c', "mutex");
    let output = mutex
        .run(&pool, async {
            assert_eq!(zcard(&pool, &state).await, 1);
            7
        })
        .await
        .unwrap();
    assert_eq!(output, 7);
    assert_eq!(zcard(&pool, &state).await, 0);
    // A panic inside: the permit is dropped and given back.
    let panicked = tokio::spawn({
        let mutex = mutex.clone();
        let pool = pool.clone();
        async move { mutex.run(&pool, async { panic!("boom") }).await }
    })
    .await;
    assert!(panicked.is_err());
    wait_for("the lease given back after a panic", WAIT, || async {
        zcard(&pool, &state).await == 0
    })
    .await;
    // Dropped while running (cancelled): given back too.
    let cancelled = tokio::time::timeout(
        Duration::from_millis(200),
        mutex.run(&pool, std::future::pending::<()>()),
    )
    .await;
    assert!(cancelled.is_err());
    wait_for(
        "the lease given back after a cancellation",
        WAIT,
        || async { zcard(&pool, &state).await == 0 },
    )
    .await;
    // Refused: the future does not run.
    let _held = mutex.acquire(&pool).await.unwrap();
    let ran = AtomicUsize::new(0);
    let refused = mutex
        .run(&pool, async {
            ran.fetch_add(1, Ordering::Relaxed);
        })
        .await;
    assert!(matches!(refused, Err(Error::OverLimit(_))));
    assert_eq!(ran.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn permits_give_their_slots_back_when_dropped() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let limiter = Limiter::concurrent("dropped", 2).max_wait(Duration::ZERO);
    let state = keys::limit_state('c', "dropped");
    {
        let _one = limiter.acquire(&pool).await.unwrap();
        let _two = limiter.acquire(&pool).await.unwrap();
        assert!(matches!(
            limiter.acquire(&pool).await,
            Err(Error::OverLimit(_))
        ));
    }
    wait_for("both leases given back", WAIT, || async {
        zcard(&pool, &state).await == 0
    })
    .await;
    // `let _ = ...` drops the permit at once (documented on `Permit`).
    let _ = limiter.acquire(&pool).await.unwrap();
    wait_for("the lease given back at once", WAIT, || async {
        zcard(&pool, &state).await == 0
    })
    .await;
    // Hold time is recorded.
    let stats = hgetall(&pool, &keys::limit_stats('c', "dropped")).await;
    assert_eq!(stats["h"], "3");
}

#[tokio::test]
async fn held_permits_are_renewed_and_never_reclaimed() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    // A one-second lease, renewed every third of a second, held for three leases.
    let limiter = Limiter::concurrent("renewed", 1)
        .lease(Duration::from_secs(1))
        .max_wait(Duration::ZERO);
    let permit = limiter.acquire(&pool).await.unwrap();
    for _ in 0..6 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(
            matches!(limiter.acquire(&pool).await, Err(Error::OverLimit(_))),
            "a live holder's slot was taken"
        );
        let ttl = pttl(&pool, &keys::limit_state('c', "renewed")).await;
        assert!(ttl > 0, "{ttl}");
    }
    assert!(!permit.is_lost());
    permit.release().await.unwrap();
    let stats = hgetall(&pool, &keys::limit_stats('c', "renewed")).await;
    assert!(!stats.contains_key("rc"), "{stats:?}");
    assert!(!stats.contains_key("ov"), "{stats:?}");
}

#[tokio::test]
async fn a_lease_taken_by_another_holder_is_lost() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let limiter = Limiter::concurrent("lost", 1)
        .lease(Duration::from_secs(1))
        .max_wait(Duration::ZERO);
    let permit = limiter.acquire(&pool).await.unwrap();
    // The state is lost (a reset, a Redis restart) and another holder takes the slot.
    del(&pool, &keys::limit_state('c', "lost")).await;
    let other = limiter.acquire(&pool).await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), permit.lost())
        .await
        .expect("the renewal reports the lease lost");
    assert!(permit.is_lost());
    assert!(!other.is_lost());
    let stats = hgetall(&pool, &keys::limit_stats('c', "lost")).await;
    assert_eq!(stats["ov"], "1");
    drop(permit);
    other.release().await.unwrap();
}

#[tokio::test]
async fn a_lost_key_is_restored_when_a_slot_is_free() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let limiter = Limiter::concurrent("restored", 2)
        .lease(Duration::from_secs(1))
        .max_wait(Duration::ZERO);
    let permit = limiter.acquire(&pool).await.unwrap();
    del(&pool, &keys::limit_state('c', "restored")).await;
    wait_for("the lease restored by its renewal", WAIT, || async {
        zcard(&pool, &keys::limit_state('c', "restored")).await == 1
    })
    .await;
    assert!(!permit.is_lost());
    let stats = hgetall(&pool, &keys::limit_stats('c', "restored")).await;
    assert_eq!(stats["restored"], "1");
}

#[tokio::test]
async fn points_corrections_are_visible_to_the_next_caller() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let shopify =
        Limiter::points("shopify", 1_000, Duration::from_secs(20)).max_wait(Duration::ZERO);
    let mut permits = Vec::new();
    for _ in 0..5 {
        permits.push(shopify.acquire_n(&pool, 200).await.unwrap());
    }
    let refused = over(shopify.acquire_n(&pool, 200).await);
    assert!(refused.retry_after().unwrap() > Duration::from_millis(3_900));
    // The first query cost 50, not 200: 150 points back, enough for a query of 100.
    permits[0].points_used(50).await.unwrap();
    let _small = shopify.acquire_n(&pool, 100).await.unwrap();
    // Correcting twice charges the difference to the last correction.
    permits[1].points_used(50).await.unwrap();
    permits[1].points_used(250).await.unwrap();
    // 1000, then -150, +100, -150 and +200.
    let level = shopify.usage(&pool).await.unwrap()[0].used;
    assert!((level - 1_000.0).abs() < 5.0, "{level}");
    // A cost above the budget is a configuration error, not a refusal.
    assert!(matches!(
        shopify.acquire_n(&pool, 1_001).await,
        Err(Error::Config(_))
    ));
}

#[tokio::test]
async fn composite_limits_charge_all_or_none() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let both = Limiter::all([
        Limiter::window("crm-h", 100, Duration::from_secs(3_600)),
        Limiter::window("crm-m", 2, Duration::from_secs(60)),
    ])
    .max_wait(Duration::ZERO);
    let _a = both.acquire(&pool).await.unwrap();
    let _b = both.acquire(&pool).await.unwrap();
    let refused = over(both.acquire(&pool).await);
    assert_eq!(refused.label(), "w:crm-m");
    // The hourly window was not charged by the refusal.
    let hourly = llen(&pool, &keys::limit_state('w', "crm-h")).await;
    assert_eq!(hourly, 2);
}

#[tokio::test]
async fn usage_and_reset() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let limiter = Limiter::all([
        Limiter::concurrent("usage-c", 3),
        Limiter::bucket("usage-b", 10, Interval::Minute),
    ])
    .key("acme");
    let _a = limiter.acquire(&pool).await.unwrap();
    let _b = limiter.acquire(&pool).await.unwrap();
    let usage = limiter.usage(&pool).await.unwrap();
    assert_eq!(usage.len(), 2);
    assert_eq!(usage[0].identity, "c:usage-c:acme");
    assert_eq!(usage[0].used, 2.0);
    assert_eq!(usage[0].next_free_in, Some(Duration::ZERO));
    assert_eq!(usage[1].identity, "b:usage-b:acme");
    assert_eq!(usage[1].used, 2.0);
    assert_eq!(
        usage[1].limit,
        Limit::Bucket {
            limit: 10,
            interval: Interval::Minute
        }
    );
    limiter.reset(&pool).await.unwrap();
    let usage = limiter.usage(&pool).await.unwrap();
    assert_eq!((usage[0].used, usage[1].used), (0.0, 0.0));
    assert!(!exists(&pool, &keys::limit_stats('c', "usage-c:acme")).await);
}

#[tokio::test]
async fn redis_errors_fail_closed_unless_fail_open() {
    let mut redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let closed = Limiter::window("closed", 10, Duration::from_secs(1));
    let open = Limiter::window("open", 10, Duration::from_secs(1)).fail_open(true);
    let open_concurrency = Limiter::concurrent("open-c", 10).fail_open(true);
    redis.stop();
    assert!(matches!(closed.acquire(&pool).await, Err(Error::Redis(_))));
    // Granted without limiting (a warning is logged).
    let permit = open.acquire(&pool).await.unwrap();
    permit.release().await.unwrap();
    // Concurrency limits never fail open.
    assert!(open_concurrency.acquire(&pool).await.is_err());
    redis.restart().await;
    assert!(closed.acquire(&pool).await.is_ok());
}

#[tokio::test]
async fn a_job_context_ends_waits_at_the_shutdown() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let stopping = CancellationToken::new();
    let ctx = JobContext::new(JobPayload::new("Waiter", vec![]), Client::new(pool.clone()))
        .with_stopping(stopping.clone());
    let limiter = Limiter::concurrent("stopping", 1).max_wait(Duration::from_secs(30));
    let _held = limiter.acquire(&pool).await.unwrap();
    let waiter = {
        let limiter = limiter.clone();
        tokio::spawn(async move { limiter.acquire(&ctx).await })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    let stopped_at = Instant::now();
    stopping.cancel();
    let refused = waiter.await.unwrap();
    assert!(stopped_at.elapsed() < Duration::from_millis(500));
    assert!(matches!(refused, Err(Error::OverLimit(_))));
}

#[tokio::test]
async fn testing_clients_grant_everything_by_default() {
    let limiter = Limiter::concurrent("fake", 1).max_wait(Duration::ZERO);
    let fake = Client::fake();
    let _one = limiter.acquire(&fake).await.unwrap();
    let _two = limiter.acquire(&fake).await.unwrap();
    assert!(limiter.usage(&fake).await.unwrap().is_empty());
}

/// A job its limiter refuses.
struct Refused;

impl Job for Refused {
    const NAME: &'static str = "limit::Refused";
    type Args = ();

    async fn perform(&self, _ctx: &JobContext, (): Self::Args) -> Result<(), JobError> {
        Err(JobError::over_limit(OverLimit::new("w:api")))
    }
}

#[tokio::test]
async fn inline_runs_return_a_refusal_as_a_job_error() {
    // An inline run happens once: a refusal is the caller's error (class `hench::OverLimit`),
    // nothing reschedules it.
    let error = Refused.perform_inline(()).await.unwrap_err();
    assert!(error.is_over_limit());
    assert_eq!(error.class(), JobError::OVER_LIMIT);
    assert_eq!(error.as_over_limit().map(OverLimit::label), Some("w:api"));
    // The same through an inline client: the push fails with the job's error.
    let client = hench::testing::Testing::new().register(Refused).inline();
    match Refused::perform_async(&client, ()).await {
        Err(Error::Job(error)) => assert_eq!(error.class(), JobError::OVER_LIMIT),
        other => panic!("expected the refusal, got {other:?}"),
    }
}

#[tokio::test(start_paused = true)]
async fn memory_targets_follow_tokio_time() {
    let target = memory_target();
    let limiter = Limiter::window("memory", 2, Duration::from_secs(1)).max_wait(Duration::ZERO);
    let _a = limiter.acquire(&target).await.unwrap();
    let _b = limiter.acquire(&target).await.unwrap();
    let refused = over(limiter.acquire(&target).await);
    assert_eq!(refused.retry_after(), Some(Duration::from_secs(1)));
    tokio::time::advance(Duration::from_secs(1)).await;
    let _c = limiter.acquire(&target).await.unwrap();
    // A wait in process sleeps on tokio's clock (the paused clock jumps ahead).
    let waiting = limiter.clone().max_wait(Duration::from_secs(5));
    let started = tokio::time::Instant::now();
    let _d = waiting.acquire(&target).await.unwrap();
    let _e = waiting.acquire(&target).await.unwrap();
    assert!(started.elapsed() >= Duration::from_secs(1));
    // Concurrency in memory, released on drop.
    let mutex = Limiter::concurrent("memory-c", 1).max_wait(Duration::ZERO);
    let held = mutex.acquire(&target).await.unwrap();
    assert!(mutex.try_acquire(&target).await.is_err());
    held.release().await.unwrap();
    assert!(mutex.try_acquire(&target).await.is_ok());
    // Separate targets do not share limits.
    let other = memory_target();
    assert!(limiter.try_acquire(&other).await.is_ok());
}

#[test]
#[allow(
    clippy::needless_borrows_for_generic_args,
    reason = "a reference to a reference is what `limiter.acquire(&ctx)` passes in a job"
)]
fn every_target_is_accepted() {
    fn takes(_: impl LimitTarget) {}
    let pool = RedisPool::detached();
    takes(&pool);
    takes(pool.clone());
    let client = Client::fake();
    takes(&client);
    takes(&&client);
    let ctx = JobContext::new(JobPayload::new("X", vec![]), client);
    takes(&ctx);
    takes(memory_target());
    let _ = Arc::new(());
}

/// ratelimit-6: callers waiting in process for a rate limit keep their place in its wait line.
/// Thirty callers ask a 5-per-second window at once, each ready to wait 6 s: the demand fits
/// in about 5 s, and every caller is served in turn, none refused because another one took
/// the unit of its slot a few milliseconds earlier.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn waiters_in_process_keep_their_place_in_the_line() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let limiter =
        Limiter::window("queue-up", 5, Duration::from_secs(1)).max_wait(Duration::from_secs(6));
    let started = Instant::now();
    let callers: Vec<_> = (0..30)
        .map(|_| {
            let limiter = limiter.clone();
            let pool = pool.clone();
            tokio::spawn(async move {
                limiter
                    .acquire(&pool)
                    .await
                    .map(|_| started.elapsed())
                    .map_err(|error| (started.elapsed(), error.to_string()))
            })
        })
        .collect();
    let mut refused = Vec::new();
    let mut last = Duration::ZERO;
    for caller in callers {
        match caller.await.unwrap() {
            Ok(at) => last = last.max(at),
            Err(refusal) => refused.push(refusal),
        }
    }
    assert!(refused.is_empty(), "{refused:?}");
    // Five per second: the last five at about 5 s.
    assert!(
        last >= Duration::from_millis(4_500) && last < Duration::from_secs(6),
        "{last:?}"
    );
}

/// ratelimit-5: callers that do not wait (`try_acquire`, an HTTP handler answering 429 with
/// `retry_after`) take no place in the wait line, so that however many of them are refused,
/// the hint stays when the limit really frees.
#[tokio::test]
async fn refusals_of_callers_that_do_not_wait_keep_hints_true() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let limiter = Limiter::window("hammered", 10, Duration::from_secs(1));
    for _ in 0..10 {
        drop(limiter.try_acquire(&pool).await.unwrap());
    }
    let mut longest = Duration::ZERO;
    for _ in 0..500 {
        let hint = over(limiter.try_acquire(&pool).await)
            .retry_after()
            .unwrap();
        longest = longest.max(hint);
    }
    assert!(longest <= Duration::from_secs(1), "{longest:?}");
    // No wait line was started: the stats have none.
    let stats = hgetall(&pool, &keys::limit_stats('w', "hammered")).await;
    assert!(!stats.contains_key("line"), "{stats:?}");
    assert_eq!(stats["r"], "500");
}

/// The end of the wait line (ms, Redis's clock) of the window limit `id`, if it has one.
async fn line_of(pool: &RedisPool, id: &str) -> Option<i64> {
    hgetall(pool, &keys::limit_stats('w', id))
        .await
        .get("line")
        .map(|value| value.parse().unwrap())
}

/// A context of a job whose payload carries `limited` (as the processor stores it when it
/// reschedules a refusal).
fn job_context(client: &Client, limited: Option<hench::limit::Limited>) -> JobContext {
    let mut job = JobPayload::new("limit::J", vec![]);
    job.limited = limited;
    JobContext::new(job, client.clone())
}

/// The `limited` field of a job rescheduled by `by` for its place at `slot`.
fn rescheduled_for(by: &str, slot: i64) -> hench::limit::Limited {
    let mut limited = hench::limit::Limited::new(1, by, None);
    limited.over_limit = 1;
    limited.slot = Some(slot);
    limited
}

/// round3-regressions-1: a job rescheduled by a limiter for its place in the wait line comes
/// back with it (`limited.s`) and keeps it, here coming back early (another process's scheduler
/// ahead, or "Add to queue"): the line does not move, and the job is told to wait for its own
/// slot, not for the end of the line behind the jobs refused after it.
#[tokio::test]
async fn a_job_rescheduled_by_a_limiter_keeps_its_place_when_it_comes_back() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let client = redis.client().await;
    let limiter = Limiter::window("rs", 1, Duration::from_secs(2)).max_wait(Duration::ZERO);
    drop(limiter.acquire(&pool).await.unwrap());
    // J is refused: its place is the first slot of the line.
    let first = over(limiter.acquire(&job_context(&client, None)).await);
    assert!(first.retry_after().unwrap() <= Duration::from_secs(2));
    let line = line_of(&pool, "rs").await.expect("a line");
    let slot = line - 2_000;
    // Three more jobs are refused after it.
    for _ in 0..3 {
        over(limiter.acquire(&job_context(&client, None)).await);
    }
    let behind = line_of(&pool, "rs").await.unwrap();
    assert_eq!(behind - line, 6_000);
    // J comes back early with its place: kept.
    let back = over(
        limiter
            .acquire(&job_context(&client, Some(rescheduled_for("w:rs", slot))))
            .await,
    );
    assert_eq!(
        line_of(&pool, "rs").await,
        Some(behind),
        "the line did not move"
    );
    let wait = back.retry_after().unwrap();
    assert!(
        wait <= Duration::from_secs(2),
        "told {wait:?}, not the end of the line"
    );
    // A ticket of another limit is not this limiter's.
    let other = over(
        limiter
            .acquire(&job_context(
                &client,
                Some(rescheduled_for("w:other", slot)),
            ))
            .await,
    );
    assert!(other.retry_after().unwrap() > Duration::from_secs(7));
}

/// round3-regressions-1: back a little late, after a caller that does not wait took the unit
/// of its slot, a rescheduled job gets the next unit that frees, still without a new place.
#[tokio::test]
async fn a_rescheduled_job_back_a_little_late_gets_the_next_unit() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let client = redis.client().await;
    let limiter = Limiter::window("rl", 1, Duration::from_secs(1)).max_wait(Duration::ZERO);
    drop(limiter.acquire(&pool).await.unwrap());
    over(limiter.acquire(&job_context(&client, None)).await);
    let line = line_of(&pool, "rl").await.unwrap();
    let slot = line - 1_000;
    for _ in 0..3 {
        over(limiter.acquire(&job_context(&client, None)).await);
    }
    let behind = line_of(&pool, "rl").await.unwrap();
    // At its slot, a caller that does not wait takes the unit first.
    let mut fresh = limiter.try_acquire(&pool).await;
    while fresh.is_err() {
        tokio::time::sleep(Duration::from_millis(5)).await;
        fresh = limiter.try_acquire(&pool).await;
    }
    let back = over(
        limiter
            .acquire(&job_context(&client, Some(rescheduled_for("w:rl", slot))))
            .await,
    );
    assert_eq!(
        line_of(&pool, "rl").await,
        Some(behind),
        "the line did not move"
    );
    assert!(back.retry_after().unwrap() <= Duration::from_secs(1));
}

/// round3-regressions-1: the place a rescheduled job comes back with is used once per execution:
/// a second acquisition of the same limit in that execution is a new caller.
#[tokio::test]
async fn a_jobs_place_in_the_line_is_used_once_per_execution() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let client = redis.client().await;
    let limiter = Limiter::window("once", 1, Duration::from_secs(2)).max_wait(Duration::ZERO);
    drop(limiter.acquire(&pool).await.unwrap());
    over(limiter.acquire(&job_context(&client, None)).await);
    let line = line_of(&pool, "once").await.unwrap();
    let ctx = job_context(&client, Some(rescheduled_for("w:once", line - 2_000)));
    over(limiter.acquire(&ctx).await);
    assert_eq!(line_of(&pool, "once").await, Some(line), "kept its place");
    let again = over(limiter.acquire(&ctx).await);
    assert_eq!(
        line_of(&pool, "once").await,
        Some(line + 2_000),
        "a new place"
    );
    assert!(again.retry_after().unwrap() > Duration::from_secs(2));
}

/// round3-regressions-1 with real workers: B, rescheduled by a limiter, is moved back to its
/// queue early ("Add to queue"); refused again, it keeps its place before C and D, refused after
/// it, and runs before them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rescheduled_job_moved_back_early_runs_at_its_place() {
    /// Takes a window limit of 1 per 2 s without waiting in process; records when it was
    /// granted.
    #[derive(Default)]
    struct Paced;

    impl Job for Paced {
        const NAME: &'static str = "limit::Paced";
        type Args = (u32,);

        async fn perform(&self, ctx: &JobContext, (marker,): (u32,)) -> Result<(), JobError> {
            let limiter =
                Limiter::window("paced", 1, Duration::from_secs(2)).max_wait(Duration::ZERO);
            let _permit = limiter.acquire(ctx).await?;
            ctx.data::<Runs>()?.record(marker.to_string());
            Ok(())
        }
    }

    let redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    let hench = redis
        .builder()
        .await
        .concurrency(1)
        .register(Paced)
        .data_arc(runs.clone())
        .build()
        .await
        .unwrap();
    let pool = hench.redis().clone();
    let mut jids = Vec::new();
    for marker in 0..4 {
        jids.push(
            Paced::perform_async(hench.client(), (marker,))
                .await
                .unwrap()
                .unwrap(),
        );
    }
    let launcher = hench.start().await.unwrap();
    wait_for("B, C and D rescheduled", WAIT, || async {
        zcard(&pool, keys::SCHEDULE).await == 3
    })
    .await;
    // B is moved back to its queue about 2 s before its slot.
    let b = hench::api::Api::new(pool.clone())
        .scheduled()
        .find_job(&jids[1])
        .await
        .unwrap()
        .unwrap();
    assert!(b.add_to_queue().await.unwrap());
    wait_for("all four granted", Duration::from_secs(20), || async {
        runs.len() == 4
    })
    .await;
    launcher.stop().await.unwrap();
    assert_eq!(runs.entries(), ["0", "1", "2", "3"], "B ran at its place");
}

/// round3-regressions-2: `try_acquire` never takes a place in the wait line, whatever calls it:
/// a job that tries a limiter and skips work when refused does not push the line away from the
/// limit's real availability.
#[tokio::test]
async fn try_acquire_in_a_job_takes_no_place_in_the_line() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let client = redis.client().await;
    let limiter = Limiter::window("tl", 2, Duration::from_secs(1));
    for _ in 0..2 {
        drop(limiter.try_acquire(&pool).await.unwrap());
    }
    for _ in 0..40 {
        over(limiter.try_acquire(&job_context(&client, None)).await);
    }
    assert_eq!(line_of(&pool, "tl").await, None, "no line was started");
    // A waiter is served when the window frees, about a second later.
    let started = Instant::now();
    let granted = limiter
        .clone()
        .max_wait(Duration::from_secs(3))
        .acquire(&pool)
        .await;
    assert!(granted.is_ok(), "{:?}", granted.err());
    assert!(started.elapsed() < Duration::from_secs(2));
    // `acquire` from a job still takes a place: its refusal reschedules the job for it.
    for _ in 0..2 {
        drop(limiter.try_acquire(&pool).await.ok());
    }
    over(
        limiter
            .clone()
            .max_wait(Duration::ZERO)
            .acquire(&job_context(&client, None))
            .await,
    );
    assert!(line_of(&pool, "tl").await.is_some());
}
