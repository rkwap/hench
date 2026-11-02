//! Unique jobs through real workers (`docs/design/unique.md` §3.3 E1-E12, §3.4 C1-C5): the lock
//! held while a job waits and runs, released or kept by each outcome in the script that writes
//! it, the `start` release, renewals by the heartbeat, lapses, basic fetch, crashes, hard
//! shutdowns, Redis restarts and concurrent pushers. Against private `redis-server`s.
#![cfg(feature = "test-util")]

mod support;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use hench::prelude::*;
use hench::test_util::TestRedis;
use hench::{FetchStrategy, Hench, Launcher, PushOutcome, keys};
use serde_json::{Value, json};
use support::*;

/// Shared state of the [`Probe`] jobs of a test.
#[derive(Default)]
struct Shared {
    runs: Runs,
    gate: Gate,
    /// Executions per key.
    executions: std::sync::Mutex<HashMap<i64, usize>>,
    /// What pushes made from inside the jobs returned.
    pushes: std::sync::Mutex<Vec<String>>,
}

impl Shared {
    fn execution(&self, key: i64) -> usize {
        let mut executions = self.executions.lock().unwrap();
        let count = executions.entry(key).or_default();
        *count += 1;
        *count
    }
}

/// A unique job keyed by its first argument, behaving as its second says.
#[derive(Default)]
struct Probe;

impl Job for Probe {
    const NAME: &'static str = "unique::Probe";
    type Args = (i64, String);

    fn options() -> JobOptions {
        JobOptions::new().unique_for(Duration::from_secs(60))
    }

    fn unique_key((key, _): &Self::Args) -> Option<Value> {
        Some(json!(key))
    }

    async fn perform(&self, ctx: &JobContext, (key, mode): Self::Args) -> Result<(), JobError> {
        let shared = ctx.data::<Shared>()?;
        let execution = shared.execution(key);
        shared.runs.record(format!("{key}:{mode}"));
        match mode.as_str() {
            "ok" => Ok(()),
            "fail" => Err(JobError::msg("boom")),
            "fail_once" if execution == 1 => Err(JobError::msg("boom")),
            "fail_once" => Ok(()),
            "interrupt_once" if execution == 1 => Err(JobError::interrupted()),
            "interrupt_once" => Ok(()),
            "gate" => {
                shared.gate.pass().await;
                Ok(())
            }
            "gate_then_fail_once" if execution == 1 => {
                shared.gate.pass().await;
                Err(JobError::msg("boom"))
            }
            "gate_then_fail_once" => Ok(()),
            "push_self" => {
                let until = ctx
                    .unique()
                    .and_then(|unique| unique.until)
                    .unwrap_or_default();
                let outcome = Probe::set()
                    .unique_until(until)
                    .push_outcome(ctx.client(), (key, "ok".to_owned()))
                    .await?;
                shared.pushes.lock().unwrap().push(match outcome {
                    PushOutcome::Pushed { .. } => "pushed".to_owned(),
                    PushOutcome::Duplicate { holder, .. } => format!("duplicate of {holder}"),
                    other => format!("{other:?}"),
                });
                Ok(())
            }
            other => match other.strip_prefix("sleep:") {
                Some(ms) => {
                    let ms: u64 = ms.parse().map_err(JobError::from)?;
                    tokio::time::sleep(Duration::from_millis(ms)).await;
                    Ok(())
                }
                None => Err(JobError::msg(format!("unknown mode {other}"))),
            },
        }
    }
}

/// The digest of the lock of `Probe` key `key` in queue `default`.
fn digest(key: i64) -> String {
    hench::unique::digest(Probe::NAME, Some("default"), &json!(key)).unwrap()
}

async fn holder(pool: &RedisPool, key: i64) -> Option<String> {
    cmd(
        pool,
        &redis_cmd!("HGET", &keys::unique_lock(&digest(key)), &"jid"),
    )
    .await
}

async fn lock_ttl(pool: &RedisPool, key: i64) -> i64 {
    pttl(pool, &keys::unique_lock(&digest(key))).await
}

/// The lapses of `Probe` counted in every minute: a test running across the turn of a minute
/// splits its counts over two hashes.
async fn lapses(pool: &RedisPool) -> i64 {
    let minutes: Vec<String> = cmd(pool, &redis_cmd!("KEYS", &"u|*")).await;
    let mut lapsed = 0;
    for key in minutes {
        let events = hgetall(pool, &key).await;
        lapsed += events
            .get(&format!("{}|z", Probe::NAME))
            .and_then(|count| count.parse::<i64>().ok())
            .unwrap_or(0);
    }
    lapsed
}

/// Pushes `Probe` (`key`, `mode`) with `until`.
async fn push(client: &Client, key: i64, mode: &str, until: UniqueUntil) -> Option<String> {
    Probe::set()
        .unique_until(until)
        .perform_async(client, (key, mode.to_owned()))
        .await
        .unwrap()
}

struct Workers {
    hench: Hench,
    launcher: Launcher,
    shared: Arc<Shared>,
}

async fn workers(
    redis: &TestRedis,
    configure: impl FnOnce(hench::HenchBuilder) -> hench::HenchBuilder,
) -> Workers {
    init_logging();
    let shared = Arc::new(Shared::default());
    let builder = redis
        .builder()
        .await
        .register(Probe)
        .data_arc(shared.clone());
    let hench = configure(builder).build().await.unwrap();
    let launcher = hench.start().await.unwrap();
    Workers {
        hench,
        launcher,
        shared,
    }
}

async fn acknowledged(pool: &RedisPool, launcher: &Launcher) {
    wait_for("the job acknowledged", WAIT, || async {
        llen(pool, &keys::inprogress(launcher.identity())).await == 0
    })
    .await;
}

#[tokio::test]
async fn success_locks_are_held_while_waiting_and_running_then_released() {
    // E1.
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let jid = push(&client, 1, "gate", UniqueUntil::Success)
        .await
        .unwrap();
    assert_eq!(holder(&pool, 1).await, Some(jid.clone()));
    let w = workers(&redis, |b| b).await;
    wait_for("the job running", WAIT, || async {
        w.shared.gate.entered() == 1
    })
    .await;
    // While it runs: still locked, a duplicate is refused.
    assert_eq!(holder(&pool, 1).await, Some(jid.clone()));
    assert_eq!(push(&client, 1, "ok", UniqueUntil::Success).await, None);
    w.shared.gate.open();
    acknowledged(&pool, &w.launcher).await;
    wait_for("the lock released", WAIT, || async {
        holder(&pool, 1).await.is_none()
    })
    .await;
    let indexed: Option<f64> = cmd(
        &pool,
        &redis_cmd!("ZSCORE", &keys::UNIQUE_LOCKS, &digest(1)),
    )
    .await;
    assert_eq!(indexed, None, "the index entry goes with the lock");
    // Free again: a new push takes it.
    assert!(push(&client, 1, "ok", UniqueUntil::Success).await.is_some());
    w.launcher.stop().await.unwrap();
    assert_eq!(lapses(&pool).await, 0);
}

#[tokio::test]
async fn success_locks_are_kept_through_retries() {
    // E2: kept and extended to the retry plus `for`, duplicates refused, released after the
    // retry succeeds.
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let w = workers(&redis, |b| b).await;
    let jid = push(&client, 2, "fail_once", UniqueUntil::Success)
        .await
        .unwrap();
    wait_for("the retry", WAIT, || async {
        zcard(&pool, keys::RETRY).await == 1
    })
    .await;
    let (_, retry_at) = sorted_jobs(&pool, keys::RETRY).await.remove(0);
    assert_eq!(holder(&pool, 2).await, Some(jid.clone()));
    let ttl = lock_ttl(&pool, 2).await as f64 / 1000.0;
    let until_retry = retry_at - hench::now_secs_f64();
    assert!(
        ttl >= until_retry + 55.0,
        "ttl {ttl} vs retry in {until_retry}"
    );
    assert_eq!(push(&client, 2, "ok", UniqueUntil::Success).await, None);
    // The retry runs (moved by hand rather than waiting for its delay) and releases the lock.
    let api = hench::api::Api::new(pool.clone());
    assert_eq!(api.retries().retry_all().await.unwrap(), 1);
    wait_for("the lock released", WAIT, || async {
        holder(&pool, 2).await.is_none()
    })
    .await;
    assert_eq!(w.shared.runs.count("2:fail_once"), 2);
    w.launcher.stop().await.unwrap();
    assert_eq!(lapses(&pool).await, 0);
}

/// A job type without retries.
#[derive(Default)]
struct NoRetry;

impl Job for NoRetry {
    const NAME: &'static str = "unique::NoRetry";
    type Args = (i64,);

    fn options() -> JobOptions {
        JobOptions::new()
            .retry(false)
            .unique_for(Duration::from_secs(60))
    }

    async fn perform(&self, _ctx: &JobContext, _args: Self::Args) -> Result<(), JobError> {
        Err(JobError::msg("no luck"))
    }
}

/// A job type whose retries are exhausted at once, discarded or sent to the morgue.
#[derive(Default)]
struct Exhausting;

impl Job for Exhausting {
    const NAME: &'static str = "unique::Exhausting";
    type Args = (i64, bool);

    fn options() -> JobOptions {
        JobOptions::new()
            .retry(0)
            .unique_for(Duration::from_secs(60))
    }

    async fn perform(&self, _ctx: &JobContext, _args: Self::Args) -> Result<(), JobError> {
        Err(JobError::msg("exhausted"))
    }

    async fn retries_exhausted(&self, job: &JobPayload, _err: &JobError) -> Exhausted {
        if job.args.get(1) == Some(&json!(true)) {
            Exhausted::Discard
        } else {
            Exhausted::Default
        }
    }
}

#[tokio::test]
async fn jobs_that_will_not_run_again_release_their_locks() {
    // E3: `retry: false`, exhausted to the morgue, discarded, `dead: false`.
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let w = workers(&redis, |b| b.register(NoRetry).register(Exhausting)).await;
    NoRetry::perform_async(&client, (1,))
        .await
        .unwrap()
        .unwrap();
    Exhausting::perform_async(&client, (1, false))
        .await
        .unwrap()
        .unwrap();
    Exhausting::perform_async(&client, (2, true))
        .await
        .unwrap()
        .unwrap();
    Exhausting::set()
        .dead(false)
        .perform_async(&client, (3, false))
        .await
        .unwrap()
        .unwrap();
    wait_for("every lock released", WAIT, || async {
        zcard(&pool, keys::UNIQUE_LOCKS).await == 0
    })
    .await;
    acknowledged(&pool, &w.launcher).await;
    assert_eq!(zcard(&pool, keys::DEAD).await, 1);
    // Free again.
    assert!(
        NoRetry::perform_async(&client, (1,))
            .await
            .unwrap()
            .is_some()
    );
    w.launcher.stop().await.unwrap();
}

#[tokio::test]
async fn start_locks_are_released_when_perform_starts() {
    // E4: a push from inside the job succeeds; a failure takes the lock back while the retry
    // waits.
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let w = workers(&redis, |b| b).await;
    push(&client, 4, "push_self", UniqueUntil::Start)
        .await
        .unwrap();
    wait_for("the follow-up pushed", WAIT, || async {
        !w.shared.pushes.lock().unwrap().is_empty()
    })
    .await;
    assert_eq!(w.shared.pushes.lock().unwrap()[0], "pushed");
    wait_for("both ran", WAIT, || async { w.shared.runs.len() == 2 }).await;
    wait_for("the follow-up's lock released", WAIT, || async {
        holder(&pool, 4).await.is_none()
    })
    .await;
    // A failing `start` job takes its lock back while its retry waits.
    let jid = push(&client, 5, "fail_once", UniqueUntil::Start)
        .await
        .unwrap();
    wait_for("the retry", WAIT, || async {
        zcard(&pool, keys::RETRY).await == 1
    })
    .await;
    assert_eq!(holder(&pool, 5).await, Some(jid));
    assert_eq!(push(&client, 5, "ok", UniqueUntil::Start).await, None);
    w.launcher.stop().await.unwrap();
    assert_eq!(
        lapses(&pool).await,
        0,
        "a start job holds no lock while it runs"
    );
}

#[tokio::test]
async fn start_jobs_whose_lock_was_taken_meanwhile_both_run() {
    // E4: the lock released at start is taken by a duplicate; the failed job's retry finds it
    // held and leaves it to the duplicate: both run.
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let api = hench::api::Api::new(pool.clone());
    let w = workers(&redis, |b| b.concurrency(1)).await;
    let first = push(&client, 6, "gate_then_fail_once", UniqueUntil::Start)
        .await
        .unwrap();
    wait_for("the first running", WAIT, || async {
        w.shared.gate.entered() == 1
    })
    .await;
    wait_for("its lock released", WAIT, || async {
        holder(&pool, 6).await.is_none()
    })
    .await;
    let duplicate = push(&client, 6, "ok", UniqueUntil::Start)
        .await
        .expect("a waiting copy");
    w.shared.gate.open();
    wait_for("the failure settled", WAIT, || async {
        zcard(&pool, keys::RETRY).await == 1
    })
    .await;
    wait_for("the duplicate ran", WAIT, || async {
        w.shared.runs.count("6:ok") == 1
    })
    .await;
    assert_ne!(
        holder(&pool, 6).await,
        Some(first.clone()),
        "the retry did not take it"
    );
    assert_eq!(api.retries().retry_all().await.unwrap(), 1);
    wait_for("the retry ran", WAIT, || async {
        w.shared.runs.count("6:gate_then_fail_once") == 2
    })
    .await;
    acknowledged(&pool, &w.launcher).await;
    let _ = duplicate;
    w.launcher.stop().await.unwrap();
    // The retry waited without its lock (the duplicate held it): one lapse, counted when the
    // retry started without holding it.
    assert_eq!(lapses(&pool).await, 1);
}

#[tokio::test]
async fn expired_locks_ignore_outcomes() {
    // E5.
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let w = workers(&redis, |b| b).await;
    let jid = Probe::set()
        .unique(Unique::new(Duration::from_secs(1)).until(UniqueUntil::Expired))
        .perform_async(&client, (7, "ok".to_owned()))
        .await
        .unwrap()
        .unwrap();
    w.shared.runs.wait_for(1).await;
    acknowledged(&pool, &w.launcher).await;
    assert_eq!(holder(&pool, 7).await, Some(jid), "kept after the success");
    assert_eq!(push(&client, 7, "ok", UniqueUntil::Expired).await, None);
    wait_for("the lock expired", WAIT, || async {
        holder(&pool, 7).await.is_none()
    })
    .await;
    assert!(push(&client, 7, "ok", UniqueUntil::Expired).await.is_some());
    w.launcher.stop().await.unwrap();
}

#[tokio::test]
async fn scheduled_unique_jobs_run_despite_their_own_lock() {
    // E6 (Sidekiq Ent #2499): the scheduler enqueues the due job without taking a lock.
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let w = workers(&redis, |b| b).await;
    let jid = Probe::set()
        .delay(Duration::from_millis(300))
        .perform_async(&client, (8, "ok".to_owned()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(holder(&pool, 8).await, Some(jid));
    w.shared.runs.wait_for(1).await;
    wait_for("the lock released", WAIT, || async {
        holder(&pool, 8).await.is_none()
    })
    .await;
    assert_eq!(w.shared.runs.count("8:ok"), 1);
    w.launcher.stop().await.unwrap();
    assert_eq!(lapses(&pool).await, 0);
}

#[tokio::test]
async fn interrupted_jobs_keep_or_take_back_their_lock() {
    // E7.
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let w = workers(&redis, |b| b).await;
    let success = push(&client, 9, "interrupt_once", UniqueUntil::Success)
        .await
        .unwrap();
    let start = push(&client, 10, "interrupt_once", UniqueUntil::Start)
        .await
        .unwrap();
    wait_for("both ran twice", WAIT, || async {
        w.shared.runs.len() == 4
    })
    .await;
    acknowledged(&pool, &w.launcher).await;
    wait_for("the locks released", WAIT, || async {
        holder(&pool, 9).await.is_none() && holder(&pool, 10).await.is_none()
    })
    .await;
    let _ = (success, start);
    w.launcher.stop().await.unwrap();
    assert_eq!(lapses(&pool).await, 0);
}

#[tokio::test]
async fn interrupted_jobs_hold_their_lock_while_they_wait_again() {
    // E7: between the push back and the next run, the lock is held (Success) or taken back
    // (Start). One processor: the interrupted job waits behind a gated job.
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let w = workers(&redis, |b| b.concurrency(1)).await;
    let api = hench::api::Api::new(pool.clone());
    api.queue("default").pause().await.unwrap();
    let success = push(&client, 11, "interrupt_once", UniqueUntil::Success)
        .await
        .unwrap();
    let start = push(&client, 12, "interrupt_once", UniqueUntil::Start)
        .await
        .unwrap();
    push(&client, 13, "gate", UniqueUntil::Success)
        .await
        .unwrap();
    api.queue("default").unpause().await.unwrap();
    wait_for("the gated job running", WAIT, || async {
        w.shared.gate.entered() == 1
    })
    .await;
    // Both interrupted jobs ran once and wait behind the gated one.
    assert_eq!(w.shared.runs.count("11:interrupt_once"), 1);
    assert_eq!(w.shared.runs.count("12:interrupt_once"), 1);
    assert_eq!(queue_size(&pool, "default").await, 2);
    assert_eq!(holder(&pool, 11).await, Some(success));
    assert_eq!(holder(&pool, 12).await, Some(start), "taken back");
    assert_eq!(push(&client, 11, "ok", UniqueUntil::Success).await, None);
    assert_eq!(push(&client, 12, "ok", UniqueUntil::Start).await, None);
    w.shared.gate.open();
    wait_for("every lock released", WAIT, || async {
        zcard(&pool, keys::UNIQUE_LOCKS).await == 0
    })
    .await;
    w.launcher.stop().await.unwrap();
    assert_eq!(lapses(&pool).await, 0);
}

/// Server middleware stopping every job without running it.
struct Halt;

#[async_trait]
impl ServerMiddleware for Halt {
    async fn call(
        &self,
        _ctx: &JobContext,
        _job: &mut JobPayload,
        _next: ServerNext<'_>,
    ) -> Result<(), JobError> {
        Ok(())
    }
}

#[tokio::test]
async fn jobs_skipped_by_server_middleware_release_their_lock() {
    // E8.
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let w = workers(&redis, |b| b.server_middleware(Halt)).await;
    push(&client, 30, "ok", UniqueUntil::Success).await.unwrap();
    wait_for("the lock released", WAIT, || async {
        holder(&pool, 30).await.is_none()
    })
    .await;
    assert_eq!(w.shared.runs.len(), 0);
    w.launcher.stop().await.unwrap();
}

#[tokio::test]
async fn unknown_jobs_keep_their_lock_through_retries() {
    // E9.
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let w = workers(&redis, |b| b).await;
    let mut job = JobPayload::new("unique::Nobody", vec![json!(1)]);
    job.unique = Some(Unique::new(Duration::from_secs(60)).into());
    let jid = client.push(job).await.unwrap().unwrap();
    wait_for("the retry", WAIT, || async {
        zcard(&pool, keys::RETRY).await == 1
    })
    .await;
    let digest = hench::unique::digest("unique::Nobody", Some("default"), &json!([1])).unwrap();
    let held: Option<String> = cmd(
        &pool,
        &redis_cmd!("HGET", &keys::unique_lock(&digest), &"jid"),
    )
    .await;
    assert_eq!(held, Some(jid));
    w.launcher.stop().await.unwrap();
}

#[tokio::test]
async fn running_jobs_renew_their_lock() {
    // E10: a job running three times its `unique_for` keeps its lock through heartbeats, and
    // the renewal never brings back a released lock.
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let w = workers(&redis, |b| b).await;
    let jid = Probe::set()
        .unique_for(Duration::from_secs(1))
        .perform_async(&client, (14, "gate".to_owned()))
        .await
        .unwrap()
        .unwrap();
    wait_for("the job running", WAIT, || async {
        w.shared.gate.entered() == 1
    })
    .await;
    let started = tokio::time::Instant::now();
    while started.elapsed() < Duration::from_secs(3) {
        assert_eq!(holder(&pool, 14).await.as_deref(), Some(jid.as_str()));
        let ttl = lock_ttl(&pool, 14).await;
        assert!(ttl > 0, "the lock of a running job never expires: {ttl}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    w.shared.gate.open();
    wait_for("the lock released", WAIT, || async {
        holder(&pool, 14).await.is_none()
    })
    .await;
    // Several beats later, still released.
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(holder(&pool, 14).await, None);
    w.launcher.stop().await.unwrap();
    assert_eq!(lapses(&pool).await, 0);
}

#[tokio::test]
async fn lapsed_locks_are_counted_and_never_release_a_duplicate() {
    // E11: `unique_for` shorter than the wait: the lock expires, a duplicate takes it, the
    // first job's success leaves the duplicate's lock and counts a lapse.
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let api = hench::api::Api::new(pool.clone());
    api.queue("default").pause().await.unwrap();
    let w = workers(&redis, |b| b).await;
    let first = Probe::set()
        .unique_for(Duration::from_secs(1))
        .perform_async(&client, (15, "ok".to_owned()))
        .await
        .unwrap()
        .unwrap();
    wait_for("the lock expired", WAIT, || async {
        holder(&pool, 15).await.is_none()
    })
    .await;
    let second = Probe::set()
        .unique_for(Duration::from_secs(60))
        .delay(Duration::from_secs(3_600))
        .perform_async(&client, (15, "ok".to_owned()))
        .await
        .unwrap()
        .expect("the lapsed lock lets a duplicate in");
    api.queue("default").unpause().await.unwrap();
    w.shared.runs.wait_for(1).await;
    acknowledged(&pool, &w.launcher).await;
    assert_eq!(holder(&pool, 15).await, Some(second));
    assert_eq!(lapses(&pool).await, 1);
    let _ = first;
    w.launcher.stop().await.unwrap();
}

#[tokio::test]
async fn basic_fetch_holds_and_releases_locks_after_the_write() {
    // E12.
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let w = workers(&redis, |b| b.fetch(FetchStrategy::Basic)).await;
    push(&client, 16, "ok", UniqueUntil::Success).await.unwrap();
    wait_for("the lock released", WAIT, || async {
        holder(&pool, 16).await.is_none()
    })
    .await;
    let jid = push(&client, 17, "fail_once", UniqueUntil::Success)
        .await
        .unwrap();
    wait_for("the retry", WAIT, || async {
        zcard(&pool, keys::RETRY).await == 1
    })
    .await;
    wait_for("the lock held for the retry", WAIT, || async {
        lock_ttl(&pool, 17).await > 60_000
    })
    .await;
    assert_eq!(holder(&pool, 17).await, Some(jid));
    let interrupted = push(&client, 18, "interrupt_once", UniqueUntil::Start)
        .await
        .unwrap();
    wait_for("both executions", WAIT, || async {
        w.shared.runs.count("18:interrupt_once") == 2
    })
    .await;
    wait_for("the lock released", WAIT, || async {
        holder(&pool, 18).await.is_none()
    })
    .await;
    let _ = interrupted;
    w.launcher.stop().await.unwrap();
    assert_eq!(lapses(&pool).await, 0);
}

#[tokio::test]
async fn a_crashed_job_is_recovered_with_its_lock() {
    // C1: the process dies mid-job (reliable fetch); duplicates are refused while the lock
    // lives; the job is recovered by another process and its success releases the lock.
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let first = workers(&redis, |b| b).await;
    let jid = push(&client, 19, "gate", UniqueUntil::Success)
        .await
        .unwrap();
    wait_for("the job running", WAIT, || async {
        first.shared.gate.entered() == 1
    })
    .await;
    first.launcher.abandon();
    assert_eq!(push(&client, 19, "ok", UniqueUntil::Success).await, None);
    // Another process recovers it (the dead one's heartbeat expires: shorten it).
    let () = cmd(
        &pool,
        &redis_cmd!("DEL", &keys::process(first.launcher.identity())),
    )
    .await;
    let second = workers(&redis, |b| b).await;
    second.shared.gate.open();
    wait_for("the recovered job ran", Duration::from_secs(20), || async {
        second.shared.runs.count("19:gate") == 1
    })
    .await;
    wait_for("the lock released", WAIT, || async {
        holder(&pool, 19).await.is_none()
    })
    .await;
    let _ = jid;
    second.launcher.stop().await.unwrap();
    let _ = first.hench;
}

#[tokio::test]
async fn a_hard_shutdown_requeues_the_job_with_its_lock() {
    // C2.
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let first = workers(&redis, |b| b.timeout(Duration::from_millis(200))).await;
    let jid = push(&client, 20, "gate", UniqueUntil::Success)
        .await
        .unwrap();
    wait_for("the job running", WAIT, || async {
        first.shared.gate.entered() == 1
    })
    .await;
    first.launcher.stop().await.unwrap();
    assert_eq!(queue_size(&pool, "default").await, 1, "requeued");
    assert_eq!(holder(&pool, 20).await, Some(jid.clone()), "with its lock");
    let second = workers(&redis, |b| b).await;
    second.shared.gate.open();
    wait_for("the lock released", WAIT, || async {
        holder(&pool, 20).await.is_none()
    })
    .await;
    assert_eq!(second.shared.runs.count("20:gate"), 1);
    second.launcher.stop().await.unwrap();
    assert_eq!(lapses(&pool).await, 0);
}

#[tokio::test]
async fn pushes_and_releases_resume_after_a_redis_restart() {
    // C3.
    let mut redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let w = workers(&redis, |b| b).await;
    push(&client, 21, "ok", UniqueUntil::Success).await.unwrap();
    wait_for("released", WAIT, || async {
        holder(&pool, 21).await.is_none()
    })
    .await;
    redis.restart().await;
    wait_for("a push after the restart", WAIT, || async {
        Probe::set()
            .perform_async(&client, (22, "ok".to_owned()))
            .await
            .is_ok_and(|jid| jid.is_some())
    })
    .await;
    let pool = redis.pool().await;
    wait_for("its lock released", WAIT, || async {
        let mut conn = match pool.conn().await {
            Ok(conn) => conn,
            Err(_) => return false,
        };
        let held: Result<Option<String>, _> =
            redis_cmd!("HGET", &keys::unique_lock(&digest(22)), &"jid")
                .query_async(&mut conn)
                .await;
        matches!(held, Ok(None))
    })
    .await;
    assert_eq!(w.shared.runs.count("22:ok"), 1);
    w.launcher.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_pushers_never_get_two_copies_of_a_digest() {
    // C4 (scaled down): pushers on several clients race on the same digests while two worker
    // processes run them; at no sampled instant are two copies of a digest waiting or running,
    // and every lock ends released.
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let first = workers(&redis, |b| b.concurrency(4)).await;
    let second = workers(&redis, |b| b.concurrency(4)).await;
    let pushed = Arc::new(AtomicUsize::new(0));
    let mut tasks = Vec::new();
    for task in 0..8 {
        let client = redis.client().await;
        let pushed = pushed.clone();
        tasks.push(tokio::spawn(async move {
            for round in 0..40 {
                let key = 100 + (task + round) % 10;
                if Probe::perform_async(&client, (key, "sleep:5".to_owned()))
                    .await
                    .unwrap()
                    .is_some()
                {
                    pushed.fetch_add(1, Ordering::SeqCst);
                }
            }
        }));
    }
    let sampler = {
        let pool = pool.clone();
        tokio::spawn(async move {
            let mut samples = 0;
            loop {
                let (queued, inprogress): (Vec<String>, Vec<String>) = {
                    let mut conn = pool.conn().await.unwrap();
                    let mut pipe = hench::redis_rs::pipe();
                    pipe.atomic()
                        .cmd("LRANGE")
                        .arg(keys::queue("default"))
                        .arg(0)
                        .arg(-1);
                    pipe.cmd("EVAL")
                        .arg("local all = {} for _, owner in ipairs(redis.call('SMEMBERS', KEYS[1])) do for _, job in ipairs(redis.call('LRANGE', owner .. ':inprogress', 0, -1)) do all[#all + 1] = job end end return all")
                        .arg(1)
                        .arg(keys::INPROGRESS);
                    pipe.query_async(&mut conn).await.unwrap()
                };
                let mut seen = HashSet::new();
                for raw in queued.iter().chain(&inprogress) {
                    let job: JobPayload = JobPayload::from_json(raw).unwrap();
                    let unique = job
                        .unique
                        .as_ref()
                        .and_then(hench::UniqueOption::settings)
                        .and_then(|unique| unique.digest.clone())
                        .unwrap();
                    assert!(
                        seen.insert(unique),
                        "two copies of one digest waiting or running"
                    );
                }
                samples += 1;
                if samples > 200 {
                    return samples;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };
    for task in tasks {
        task.await.unwrap();
    }
    let samples = sampler.await.unwrap();
    assert!(samples > 0);
    wait_for("every lock released", Duration::from_secs(30), || async {
        zcard(&pool, keys::UNIQUE_LOCKS).await == 0
    })
    .await;
    let ran = first.shared.runs.len() + second.shared.runs.len();
    assert_eq!(
        ran,
        pushed.load(Ordering::SeqCst),
        "every pushed job ran once"
    );
    first.launcher.stop().await.unwrap();
    second.launcher.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_replace_pushes_leave_one_scheduled_job() {
    // C5.
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let mut tasks = Vec::new();
    for task in 0..8 {
        let client = redis.client().await;
        tasks.push(tokio::spawn(async move {
            for version in 0..20 {
                Probe::set()
                    .delay(Duration::from_secs(60))
                    .on_conflict(OnConflict::Replace)
                    .perform_async(&client, (500, format!("v{task}-{version}")))
                    .await
                    .unwrap();
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    let scheduled = sorted_jobs(&pool, keys::SCHEDULE).await;
    assert_eq!(scheduled.len(), 1, "one waiting job");
    assert_eq!(holder(&pool, 500).await, Some(scheduled[0].0.jid.clone()));
}

/// A unique job (keyed by its first argument) that gives its lock up while it runs, then does
/// what its second argument says; retried after 300 ms.
#[derive(Default)]
struct Releaser;

impl Job for Releaser {
    const NAME: &'static str = "unique::Releaser";
    type Args = (i64, String);

    fn options() -> JobOptions {
        JobOptions::new().unique_for(Duration::from_secs(60))
    }

    fn unique_key((key, _): &Self::Args) -> Option<Value> {
        Some(json!(key))
    }

    fn retry_in(&self, _count: u32, _error: &JobError, _job: &JobPayload) -> RetryIn {
        RetryIn::After(Duration::from_millis(300))
    }

    async fn perform(&self, ctx: &JobContext, (key, mode): Self::Args) -> Result<(), JobError> {
        let shared = ctx.data::<Shared>()?;
        let execution = shared.execution(key);
        let released = ctx.release_unique_lock().await?;
        let unique = ctx.unique().is_some();
        shared.runs.record(format!(
            "{key}:{execution}:released={released}:unique={unique}"
        ));
        match mode.as_str() {
            "fail_once" if execution == 1 => Err(JobError::msg("boom after giving the lock up")),
            "interrupt_once" if execution == 1 => Err(JobError::interrupted()),
            _ => Ok(()),
        }
    }
}

fn releaser_digest(key: i64) -> String {
    hench::unique::digest(Releaser::NAME, Some("default"), &json!(key)).unwrap()
}

/// The `z` lapses of `class` counted in every minute.
async fn lapses_of(pool: &RedisPool, class: &str) -> i64 {
    let minutes: Vec<String> = cmd(pool, &redis_cmd!("KEYS", &"u|*")).await;
    let mut lapsed = 0;
    for key in minutes {
        lapsed += hgetall(pool, &key)
            .await
            .get(&format!("{class}|z"))
            .and_then(|count| count.parse::<i64>().ok())
            .unwrap_or(0);
    }
    lapsed
}

#[tokio::test]
async fn a_copy_continuing_a_job_that_gave_its_lock_up_runs_without_a_lock() {
    // unique-4: a retry, or a job pushed back, after `release_unique_lock` is stored with
    // `"unique": false`: it waits and runs without a lock, so its outcome counts no lapse and
    // a duplicate pushed meanwhile is a job of its own.
    for fetch in [FetchStrategy::Reliable, FetchStrategy::Basic] {
        let redis = TestRedis::start().await;
        let client = redis.client().await;
        let pool = redis.pool().await;
        let w = workers(&redis, |b| b.register(Releaser).fetch(fetch)).await;
        Releaser::perform_async(&client, (1, "fail_once".to_owned()))
            .await
            .unwrap()
            .expect("pushed");
        wait_for("the retry waiting", WAIT, || async {
            zcard(&pool, keys::RETRY).await == 1
        })
        .await;
        let (retry, _) = sorted_jobs(&pool, keys::RETRY).await.remove(0);
        assert_eq!(
            retry.unique,
            Some(hench::UniqueOption::Disabled),
            "{fetch:?}"
        );
        assert!(!exists(&pool, &keys::unique_lock(&releaser_digest(1))).await);
        // Due now (rather than after the retry's jitter of up to ten seconds).
        let (member, _) = zrange(&pool, keys::RETRY).await.remove(0);
        let _: i64 = cmd(&pool, &redis_cmd!("ZADD", &keys::RETRY, &"XX", &0, &member)).await;
        Releaser::perform_async(&client, (2, "interrupt_once".to_owned()))
            .await
            .unwrap()
            .expect("pushed");
        w.shared.runs.wait_for(4).await;
        acknowledged(&pool, &w.launcher).await;
        wait_for("both done", WAIT, || async {
            zcard(&pool, keys::RETRY).await == 0 && queue_size(&pool, "default").await == 0
        })
        .await;
        // The first execution gave its lock up; the copy continuing it held none (it is not
        // unique any more). No lock is left behind, and none was counted as lapsed.
        let mut runs = w.shared.runs.entries();
        runs.sort();
        assert_eq!(
            runs,
            [
                "1:1:released=true:unique=true",
                "1:2:released=false:unique=false",
                "2:1:released=true:unique=true",
                "2:2:released=false:unique=false",
            ],
            "{fetch:?}"
        );
        for key in [1, 2] {
            assert!(!exists(&pool, &keys::unique_lock(&releaser_digest(key))).await);
        }
        assert_eq!(lapses_of(&pool, Releaser::NAME).await, 0, "{fetch:?}");
        w.launcher.stop().await.unwrap();
    }
}

/// An `expired` job (a throttle window) that reopens its window early.
#[derive(Default)]
struct WindowReopener;

impl Job for WindowReopener {
    const NAME: &'static str = "unique::WindowReopener";
    type Args = (i64,);

    fn options() -> JobOptions {
        JobOptions::new()
            .unique_for(Duration::from_secs(60))
            .unique_until(UniqueUntil::Expired)
    }

    async fn perform(&self, ctx: &JobContext, _args: Self::Args) -> Result<(), JobError> {
        assert!(ctx.release_unique_lock().await?);
        ctx.data::<Shared>()?.runs.record("reopened");
        Ok(())
    }
}

#[tokio::test]
async fn an_early_release_belongs_to_its_execution() {
    // unique-3: an `expired` job releasing its lock early left a process-wide note that only
    // the outcomes of `success`/`start` jobs took: notes piled up (each insert past 10 000
    // scanned them all under a mutex every unique outcome took) and a later job reusing the
    // jid skipped its lock work. The early release now lives in the execution's own state.
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let w = workers(&redis, |b| b.register(WindowReopener).concurrency(5)).await;
    for n in 0..50 {
        WindowReopener::perform_async(&client, (n,))
            .await
            .unwrap()
            .expect("pushed");
    }
    w.shared.runs.wait_for(50).await;
    acknowledged(&pool, &w.launcher).await;
    // The window of each is open again.
    for n in 0..50 {
        assert!(
            WindowReopener::perform_async(&client, (n,))
                .await
                .unwrap()
                .is_some()
        );
    }
    w.shared.runs.wait_for(100).await;
    acknowledged(&pool, &w.launcher).await;
    // A `success` job reusing the jid of an `expired` job that released early: its outcome
    // releases its lock as usual.
    let jid = "00000000000000000000a3a3".to_owned();
    WindowReopener::set()
        .jid(jid.clone())
        .perform_async(&client, (999,))
        .await
        .unwrap()
        .expect("pushed");
    w.shared.runs.wait_for(101).await;
    acknowledged(&pool, &w.launcher).await;
    Probe::set()
        .jid(jid.clone())
        .perform_async(&client, (60, "ok".to_owned()))
        .await
        .unwrap()
        .expect("pushed");
    w.shared.runs.wait_for(102).await;
    acknowledged(&pool, &w.launcher).await;
    assert_eq!(holder(&pool, 60).await, None, "released at its success");
    assert!(matches!(
        Probe::set()
            .push_outcome(&client, (60, "ok".to_owned()))
            .await
            .unwrap(),
        PushOutcome::Pushed { .. }
    ));
    assert_eq!(lapses(&pool).await, 0);
    w.launcher.stop().await.unwrap();
}
