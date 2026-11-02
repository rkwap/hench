//! The pro features together (`docs/design/pro-features.md` §8): scenarios that need two or
//! more of unique jobs, periodic jobs, batches and rate limiting, through real workers on a
//! private `redis-server`. Each scenario sweeps the keys it leaves (X23): only the
//! configuration and index keys of the features live without an expiry.
//!
//! Elsewhere: X1 (a unique schedule's ticks are duplicates while its job holds the lock) in
//! `tests/periodic.rs` with real workers and `periodic/lua/redis_tests.rs` for the resent `FIRE`
//! and `OOM`; X16's dashboard columns, X17 and X19 in `hench-web/tests/features.rs` (X17's
//! `features` of `GET /processes` in `hench-server/tests/http.rs`); X20 in
//! `hench-server/tests/features.rs`; X21 is the soak scenario `pro` (`examples/soak`,
//! `ci/soak.sh`); X22 is the throughput comparison recorded in `docs/DESIGN.md` §9k and
//! `docs/STATUS.md`. Each feature's own scenarios are in
//! `tests/{unique,periodic,batch,limit,throttle}*.rs`.

mod support;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::StreamExt;
use hench::api::Api;
use hench::batch::WORKFLOW_NEXT;
use hench::iterable::{Enumerator, IterableJob, Iteration};
use hench::prelude::*;
use hench::test_util::TestRedis;
use hench::testing::{TestLimits, Testing};
use hench::{Hench, HenchBuilder, Launcher, PushOutcome, keys};
use serde_json::{Value, json};
use support::*;

/// What the jobs of a scenario saw.
#[derive(Default)]
struct Shared {
    /// `mode:jid` per execution, in order.
    runs: Runs,
    gate: Gate,
    /// Executions per jid.
    executions: Mutex<HashMap<String, usize>>,
    /// What the job and the server middleware saw (`what=value`).
    seen: Mutex<Vec<String>>,
}

impl Shared {
    fn execution(&self, jid: &str) -> usize {
        let mut executions = self.executions.lock().unwrap();
        let count = executions.entry(jid.to_owned()).or_default();
        *count += 1;
        *count
    }

    fn seen(&self) -> Vec<String> {
        self.seen.lock().unwrap().clone()
    }
}

/// A job behaving as its mode says (`ok`, `fail`, `gate`, `over`, `fail_until_4`, `limiter`,
/// `probe`); `key` makes its unique digest and its throttle key.
#[derive(Default)]
struct Pro;

impl Job for Pro {
    const NAME: &'static str = "pro::Job";
    type Args = (String, String);

    fn unique_key((_, key): &Self::Args) -> Option<Value> {
        Some(json!([key]))
    }

    fn throttle_key((_, key): &Self::Args) -> Option<String> {
        Some(key.clone())
    }

    fn retry_in(&self, _count: u32, _error: &JobError, _job: &JobPayload) -> RetryIn {
        RetryIn::After(Duration::from_millis(200))
    }

    async fn perform(&self, ctx: &JobContext, (mode, key): Self::Args) -> Result<(), JobError> {
        let shared = ctx.data::<Shared>()?;
        let execution = shared.execution(ctx.jid());
        shared.runs.record(format!("{mode}:{}", ctx.jid()));
        match mode.as_str() {
            "ok" => Ok(()),
            "fail" => Err(JobError::msg("boom")),
            "gate" => {
                shared.gate.pass().await;
                Ok(())
            }
            // A limiter's refusal, with an hour to wait: rescheduled once, not run again here.
            "over" => Err(JobError::over_limit(
                OverLimit::new("w:pro").with_retry_after(Duration::from_secs(3_600)),
            )),
            "over_once" if execution == 1 => Err(JobError::over_limit(
                OverLimit::new("w:pro").with_retry_after(Duration::from_millis(100)),
            )),
            "over_once" => Ok(()),
            "over_fast" => Err(JobError::over_limit(
                OverLimit::new("w:pro").with_retry_after(Duration::from_millis(100)),
            )),
            "fail_until_4" if execution < 4 => Err(JobError::msg("not yet")),
            "fail_until_4" => Ok(()),
            // X15: what the job sees, then a limiter of its own.
            "probe" => {
                let pool = ctx.client().redis().clone();
                let lock = exists(&pool, &keys::unique_lock(&digest(&key))).await;
                shared.seen.lock().unwrap().push(format!("job:lock={lock}"));
                let _permit = Limiter::concurrent("x15-inner", 5).acquire(ctx).await?;
                Ok(())
            }
            other => Err(JobError::msg(format!("unknown mode {other}"))),
        }
    }
}

/// The digest of `Pro` with `key` in the default queue.
fn digest(key: &str) -> String {
    hench::unique::digest(Pro::NAME, Some("default"), &json!([key])).unwrap()
}

fn args(mode: &str, key: &str) -> (String, String) {
    (mode.to_owned(), key.to_owned())
}

struct Worker {
    hench: Hench,
    launcher: Launcher,
    shared: Arc<Shared>,
}

/// A worker with `Pro` registered and `configure`'s settings.
async fn worker(redis: &TestRedis, configure: impl FnOnce(HenchBuilder) -> HenchBuilder) -> Worker {
    worker_sharing(redis, Arc::default(), configure).await
}

/// [`worker`], its jobs recording into `shared`.
async fn worker_sharing(
    redis: &TestRedis,
    shared: Arc<Shared>,
    configure: impl FnOnce(HenchBuilder) -> HenchBuilder,
) -> Worker {
    init_logging();
    let builder = redis
        .builder()
        .await
        .register(Pro)
        .concurrency(4)
        .data_arc(shared.clone());
    let hench = configure(builder).build().await.unwrap();
    let launcher = hench.start().await.unwrap();
    Worker {
        hench,
        launcher,
        shared,
    }
}

/// The unique counter `field` (`d`, `r`, `z`) of `Pro`, summed over the minutes.
async fn unique_count(pool: &RedisPool, field: &str) -> i64 {
    let minutes: Vec<String> = cmd(pool, &redis_cmd!("KEYS", "u|*")).await;
    let mut total = 0;
    for key in minutes {
        total += hgetall(pool, &key)
            .await
            .get(&format!("{}|{field}", Pro::NAME))
            .and_then(|count| count.parse::<i64>().ok())
            .unwrap_or(0);
    }
    total
}

/// X23: every key of the features expires, but the configuration and index keys.
async fn assert_feature_keys_expire(redis: &TestRedis) {
    let allowed = |key: &str| {
        key == "periodic"
            || key == "periodic-names"
            || key == "periodic-crontabs"
            || key.starts_with("periodic:")
            || key.starts_with("periodic-history:")
            || key.starts_with("periodic-group:")
            || key == "queue-limits"
            || key == keys::UNIQUE_LOCKS
            || key == "batches"
            || key == "batch-events"
            || key == "limits"
    };
    let lasting: Vec<String> = redis
        .keys_without_ttl(&[
            "unique*",
            "batch*",
            "limit*",
            "periodic*",
            "u|*",
            "b|*",
            "l|*",
            "j|*",
        ])
        .await
        .into_iter()
        .filter(|key| !allowed(key))
        .collect();
    assert!(lasting.is_empty(), "keys without expiry: {lasting:?}");
}

/// The status of batch `bid`.
async fn status(client: &Client, bid: &str) -> hench::batch::BatchStatus {
    client.batch_status(bid).await.unwrap().expect("the batch")
}

/// Holds the throttle `name`'s single concurrency slot from outside the workers (the limiter
/// of the same name shares its state), until the permit is dropped.
async fn hold_slot(pool: &RedisPool, name: &str) -> hench::limit::Permit {
    Limiter::concurrent(name, 1)
        .lease(Duration::from_secs(60))
        .max_wait(Duration::ZERO)
        .acquire(pool)
        .await
        .unwrap()
}

/// X3 and X1's complement: a periodic tick that a throttle parks keeps its unique lock, so the
/// later ticks are duplicates; a throttled tick records no run, a tick refused by a limiter
/// while it runs records `rescheduled`.
#[tokio::test]
async fn x3_periodic_ticks_parked_by_a_throttle_keep_their_lock_and_record_their_runs() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let slot = hold_slot(&pool, "x3").await;
    let w = worker(&redis, |builder| {
        builder
            .throttle(Throttle::new("x3").concurrency(1))
            .periodic(
                Periodic::every("x3-throttled", Duration::from_secs(1))
                    .job::<Pro>(args("ok", "t"))
                    .throttle("x3")
                    .unique_for(Duration::from_secs(60)),
            )
            .periodic(
                Periodic::every("x3-limited", Duration::from_secs(1))
                    .job::<Pro>(args("over", "l"))
                    .unique_for(Duration::from_secs(60)),
            )
    })
    .await;
    // The throttled tick is parked, holding its lock: the next ticks are duplicates.
    wait_for("the throttled tick parked", WAIT, || async {
        sorted_jobs(&pool, keys::SCHEDULE)
            .await
            .iter()
            .any(|(job, _)| job.limited.is_some() && job.args[0] == json!("ok"))
    })
    .await;
    wait_for("a later tick skipped as a duplicate", WAIT, || async {
        hgetall(&pool, &keys::periodic("x3-throttled"))
            .await
            .get("skipped")
            .is_some_and(|skipped| skipped != "0")
    })
    .await;
    let throttled = hgetall(&pool, &keys::periodic("x3-throttled")).await;
    assert_eq!(throttled.get("fired").map(String::as_str), Some("1"));
    assert!(!throttled.contains_key("run_status"), "{throttled:?}");
    assert!(exists(&pool, &keys::unique_lock(&digest("t"))).await);
    // The tick refused by a limiter while it ran records `rescheduled`, keeps its lock too.
    wait_for("the refused tick's run recorded", WAIT, || async {
        hgetall(&pool, &keys::periodic("x3-limited"))
            .await
            .get("run_status")
            .is_some_and(|status| status == "rescheduled")
    })
    .await;
    assert!(exists(&pool, &keys::unique_lock(&digest("l"))).await);
    w.launcher.stop().await.unwrap();
    drop(slot);
    assert_eq!(unique_count(&pool, "z").await, 0);
    assert_feature_keys_expire(&redis).await;
}

/// X2: a schedule of a typed job stores the job's own unique and throttle keys, so a process
/// that does not know the job type fires the digest and throttle key of a typed push.
#[tokio::test]
async fn x2_periodic_templates_carry_the_typed_keys() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    init_logging();
    // This process does not register `Pro`, and runs no jobs.
    let hench = redis
        .builder()
        .await
        .queues(["elsewhere"])
        .throttle(Throttle::new("x2").concurrency(1))
        .periodic(
            Periodic::every("x2", Duration::from_secs(1))
                .job::<Pro>(args("ok", "acme"))
                .unique_for(Duration::from_secs(60))
                .throttle("x2"),
        )
        .build()
        .await
        .unwrap();
    let launcher = hench.start().await.unwrap();
    wait_for("a tick", WAIT, || async {
        queue_size(&pool, "default").await == 1
    })
    .await;
    launcher.stop().await.unwrap();
    let fired = queue_jobs(&pool, "default").await.remove(0);
    let typed = Client::fake();
    Pro::set()
        .unique_for(Duration::from_secs(60))
        .throttle("x2")
        .perform_async(&typed, args("ok", "acme"))
        .await
        .unwrap();
    let pushed = typed.jobs::<Pro>().remove(0);
    let digest_of = |job: &JobPayload| {
        job.unique
            .as_ref()
            .and_then(|unique| unique.settings())
            .and_then(|unique| unique.digest.clone())
    };
    assert_eq!(digest_of(&fired), Some(digest("acme")));
    assert_eq!(digest_of(&fired), digest_of(&pushed));
    assert_eq!(fired.throttle, pushed.throttle);
    assert_eq!(
        fired.throttle.as_ref().and_then(|throttle| throttle.key()),
        Some("acme")
    );
    assert_feature_keys_expire(&redis).await;
}

/// A periodic job that defines a batch per tick, idempotently: `Batch::with_bid` with its jid.
#[derive(Default)]
struct Nightly;

impl Job for Nightly {
    const NAME: &'static str = "pro::Nightly";
    type Args = ();

    async fn perform(&self, ctx: &JobContext, (): ()) -> Result<(), JobError> {
        let shared = ctx.data::<Shared>()?;
        let committed = Batch::with_bid(ctx.client(), ctx.jid())?
            .commit_id(ctx.jid())
            .jobs(|jobs| async move {
                for key in ["a", "b"] {
                    Pro::perform_async(&jobs, args("ok", key)).await?;
                }
                Ok::<_, hench::Error>(())
            })
            .await?;
        shared.seen.lock().unwrap().push(format!(
            "commit:{}:{}",
            committed.bid, committed.already_applied
        ));
        // The first run stops here, as if its process died before the acknowledgement.
        if shared.execution(ctx.jid()) == 1 {
            shared.gate.pass().await;
        }
        Ok(())
    }
}

/// X4: a tick's job that commits a batch and dies before its acknowledgement runs again and
/// commits nothing new.
#[tokio::test]
async fn x4_a_periodic_job_re_run_after_a_crash_creates_its_batch_once() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let shared = Arc::new(Shared::default());
    let schedule = Periodic::cron("x4", "0 0 1 1 *").job::<Nightly>(());
    let first = worker_sharing(&redis, shared.clone(), |builder| {
        builder
            .register(Nightly)
            .queues(["nightly"])
            .periodic(schedule.clone().queue("nightly"))
    })
    .await;
    wait_for("the schedule registered", WAIT, || async {
        exists(&pool, &keys::periodic("x4")).await
    })
    .await;
    // Enqueued by hand, like a tick ("enqueue now").
    let jid = Api::new(pool.clone())
        .periodic()
        .enqueue("x4")
        .await
        .unwrap()
        .expect("enqueued");
    wait_for("the batch committed", WAIT, || async {
        shared.seen().len() == 1
    })
    .await;
    first.launcher.abandon();
    // Another process recovers the job and runs it again (the dead one's heartbeat expires:
    // shorten it).
    let () = cmd(
        &pool,
        &redis_cmd!("DEL", &keys::process(first.launcher.identity())),
    )
    .await;
    let second = worker_sharing(&redis, shared.clone(), |builder| {
        builder
            .register(Nightly)
            .queues(["nightly", "default"])
            .periodic(schedule.clone().queue("nightly"))
    })
    .await;
    wait_for("the job ran again", Duration::from_secs(30), || async {
        shared.seen().len() == 2
    })
    .await;
    let client = second.hench.client().clone();
    wait_for("the batch succeeded", WAIT, || async {
        status(&client, &jid).await.success_at.is_some()
    })
    .await;
    second.launcher.stop().await.unwrap();
    shared.gate.open();
    assert_eq!(
        shared.seen(),
        [format!("commit:{jid}:false"), format!("commit:{jid}:true")]
    );
    let batch = status(&client, &jid).await;
    assert_eq!(batch.total, 2);
    assert_eq!(cmd::<u64>(&pool, &redis_cmd!("ZCARD", "batches")).await, 1);
    assert_eq!(
        hgetall(&pool, &keys::periodic("x4"))
            .await
            .get("run_status")
            .map(String::as_str),
        Some("ok")
    );
    assert_feature_keys_expire(&redis).await;
}

/// X5: a `Success` lock is held across retries, released at death; a job revived by hand runs
/// without its lock, and its success counts a lapse without releasing a newer holder's lock.
#[tokio::test]
async fn x5_unique_locks_through_retries_death_and_revival() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let w = worker(&redis, |builder| builder).await;
    let client = w.hench.client().clone();
    let jid = Pro::set()
        .unique_for(Duration::from_secs(10))
        .retry(2)
        .perform_async(&client, args("fail_until_4", "k5"))
        .await
        .unwrap()
        .unwrap();
    let lock = keys::unique_lock(&digest("k5"));
    let api = Api::new(pool.clone());
    // Two retries, each made due at once (a retry's delay has a random part of seconds; a
    // manual retry would not count against the job's retries).
    for retry in 0..2_u32 {
        wait_for("a retry waits", WAIT, || async {
            sorted_jobs(&pool, keys::RETRY)
                .await
                .first()
                .is_some_and(|(job, _)| job.retry_count == Some(retry))
        })
        .await;
        // Held through the retry: at least `unique_for` from now, plus the retry's delay.
        let (_, at) = sorted_jobs(&pool, keys::RETRY).await.remove(0);
        let delay_ms = ((at - now_secs()) * 1_000.0).max(0.0);
        assert!(
            pttl(&pool, &lock).await as f64 > 9_000.0 + delay_ms - 1_000.0,
            "retry {retry}: ttl {} for a delay of {delay_ms} ms",
            pttl(&pool, &lock).await
        );
        let (member, _) = zrange(&pool, keys::RETRY).await.remove(0);
        let () = cmd(&pool, &redis_cmd!("ZADD", keys::RETRY, "XX", 0, member)).await;
    }
    wait_for("dead after its retries", WAIT, || async {
        zcard(&pool, keys::DEAD).await == 1
    })
    .await;
    wait_for("the lock released at death", WAIT, || async {
        !exists(&pool, &lock).await
    })
    .await;
    assert_eq!(w.shared.runs.len(), 3);
    // A duplicate pushed meanwhile takes the free lock, waiting in the schedule.
    let newer = Pro::set()
        .unique_for(Duration::from_secs(60))
        .delay(Duration::from_secs(3_600))
        .perform_async(&client, args("ok", "k5"))
        .await
        .unwrap()
        .expect("pushed: the lock was free");
    // Revived by hand, the dead job runs without a lock: its success counts a lapse and leaves
    // the newer job's lock alone.
    assert_eq!(api.dead().retry_all().await.unwrap(), 1);
    wait_for("the revived job succeeded", WAIT, || async {
        w.shared.runs.len() == 4 && unique_count(&pool, "z").await == 1
    })
    .await;
    w.launcher.stop().await.unwrap();
    assert_eq!(
        hgetall(&pool, &lock).await.get("jid").map(String::as_str),
        Some(newer.as_str())
    );
    assert_ne!(newer, jid);
    assert_feature_keys_expire(&redis).await;
}

/// X6: duplicates inside a batch commit, an explicit jid holding its lock, `error` and
/// `replace`, through Redis and the testing clients, which agree.
#[tokio::test]
async fn x6_unique_members_of_batch_commits_agree_across_clients() {
    let redis = TestRedis::start().await;
    let unique = || Pro::set().unique_for(Duration::from_secs(60));
    for client in [
        redis.client().await,
        Testing::new().register(Pro).fake(),
        Testing::new()
            .register(Pro)
            .data(Shared::default())
            .inline(),
    ] {
        let label = if client.is_fake() {
            "fake"
        } else if client.is_inline() {
            "inline"
        } else {
            "redis"
        };
        // A key already held outside the batch (inline clients run it at once: no lock left).
        let outside = unique()
            .perform_async(&client, args("ok", "held"))
            .await
            .unwrap();
        let committed = Batch::new(&client)
            .jobs(|jobs| {
                async move {
                    unique().perform_async(&jobs, args("ok", "x")).await?;
                    // The same key in the same commit: left out.
                    unique().perform_async(&jobs, args("ok", "x")).await?;
                    unique().perform_async(&jobs, args("ok", "held")).await?;
                    Ok::<_, hench::Error>(())
                }
            })
            .await
            .unwrap();
        let expected_duplicates = if client.is_inline() { 1 } else { 2 };
        assert_eq!(committed.duplicates.len(), expected_duplicates, "{label}");
        assert_eq!(
            committed.jids.len() + committed.duplicates.len(),
            3,
            "{label}"
        );
        assert!(outside.is_some(), "{label}");
        // `error`: the whole commit is refused, nothing written.
        let refused = Batch::new(&client)
            .jobs(|jobs| async move {
                unique().perform_async(&jobs, args("ok", "y")).await?;
                unique()
                    .on_conflict(OnConflict::Error)
                    .perform_async(&jobs, args("ok", "y"))
                    .await?;
                Ok::<_, hench::Error>(())
            })
            .await;
        assert!(
            matches!(refused, Err(hench::Error::DuplicateJob { .. })),
            "{label}: {refused:?}"
        );
        // `replace` is refused at the push.
        let replace = Batch::new(&client)
            .jobs(|jobs| async move {
                unique()
                    .on_conflict(OnConflict::Replace)
                    .perform_async(&jobs, args("ok", "z"))
                    .await?;
                Ok::<_, hench::Error>(())
            })
            .await;
        assert!(
            matches!(replace, Err(hench::Error::InvalidJob(_))),
            "{label}: {replace:?}"
        );
    }
    assert_feature_keys_expire(&redis).await;
}

/// X7: a member of a cancelled batch is skipped before `perform`, its `Start` or `Success` lock
/// released by its acknowledgement.
#[tokio::test]
async fn x7_skipped_members_release_their_locks() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let client = redis.client().await;
    let committed = Batch::new(&client)
        .jobs(|jobs| async move {
            for (key, until) in [("s7", UniqueUntil::Start), ("u7", UniqueUntil::Success)] {
                Pro::set()
                    .unique_for(Duration::from_secs(60))
                    .unique_until(until)
                    .perform_async(&jobs, args("ok", key))
                    .await?;
            }
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap();
    assert!(exists(&pool, &keys::unique_lock(&digest("s7"))).await);
    client.cancel_batch(&committed.bid).await.unwrap();
    let w = worker(&redis, |builder| builder).await;
    wait_for("both members skipped", WAIT, || async {
        status(&client, &committed.bid).await.skipped == 2
    })
    .await;
    w.launcher.stop().await.unwrap();
    assert_eq!(w.shared.runs.len(), 0, "perform never ran");
    for key in ["s7", "u7"] {
        assert!(
            !exists(&pool, &keys::unique_lock(&digest(key))).await,
            "{key}"
        );
    }
    assert_eq!(unique_count(&pool, "z").await, 0);
    let batch = status(&client, &committed.bid).await;
    assert_eq!((batch.pending, batch.skipped), (0, 2));
    assert!(batch.complete_at.is_some() && batch.success_at.is_none());
    assert_feature_keys_expire(&redis).await;
}

/// X8: a unique job parked by a throttle keeps its lock (TTL at least `unique_for` plus the
/// delay, `at` set), so a `Replace` push replaces it in the schedule; a `Start` job parked by
/// the gate keeps its lock too (released only when `perform` starts).
#[tokio::test]
async fn x8_jobs_parked_by_a_throttle_keep_their_locks() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let slot = hold_slot(&pool, "x8").await;
    let w = worker(&redis, |builder| {
        builder.throttle(
            Throttle::new("x8")
                .concurrency(1)
                .max_delay(Duration::from_secs(600)),
        )
    })
    .await;
    let client = w.hench.client().clone();
    let mut first = String::new();
    for (key, until) in [("p8", UniqueUntil::Success), ("s8", UniqueUntil::Start)] {
        let outcome = Pro::set()
            .unique_for(Duration::from_secs(30))
            .unique_until(until)
            .throttle("x8")
            .push_outcome(&client, args("ok", key))
            .await
            .unwrap();
        let PushOutcome::Pushed { jid } = outcome else {
            panic!("pushed: {outcome:?}");
        };
        if key == "p8" {
            first = jid;
        }
    }
    wait_for("both parked", WAIT, || async {
        zcard(&pool, keys::SCHEDULE).await == 2
    })
    .await;
    let parked = sorted_jobs(&pool, keys::SCHEDULE).await;
    let (_, score) = parked
        .iter()
        .find(|(job, _)| job.jid == first)
        .cloned()
        .unwrap();
    let delay_ms = (score - now_secs()) * 1_000.0;
    let lock = keys::unique_lock(&digest("p8"));
    assert!(
        pttl(&pool, &lock).await as f64 >= 29_000.0 + delay_ms - 2_000.0,
        "lock ttl {} for a delay of {delay_ms} ms",
        pttl(&pool, &lock).await
    );
    assert!(hgetall(&pool, &lock).await.contains_key("at"));
    assert!(exists(&pool, &keys::unique_lock(&digest("s8"))).await);
    // A debounce push replaces the parked job.
    let replaced = Pro::set()
        .unique_for(Duration::from_secs(30))
        .on_conflict(OnConflict::Replace)
        .throttle("x8")
        .push_outcome(&client, args("ok", "p8"))
        .await
        .unwrap();
    assert!(
        matches!(&replaced, PushOutcome::Replaced { replaced, .. } if *replaced == first),
        "{replaced:?}"
    );
    w.launcher.stop().await.unwrap();
    drop(slot);
    assert_eq!(w.shared.runs.len(), 0);
    assert_feature_keys_expire(&redis).await;
}

/// X9: a limiter refusal while a unique job runs reschedules it with its lock held (a `Start`
/// lock is taken back), and counts no lapse.
#[tokio::test]
async fn x9_jobs_over_a_limit_keep_or_take_back_their_locks() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let w = worker(&redis, |builder| builder).await;
    let client = w.hench.client().clone();
    for (key, until) in [("u9", UniqueUntil::Success), ("s9", UniqueUntil::Start)] {
        Pro::set()
            .unique_for(Duration::from_secs(30))
            .unique_until(until)
            .perform_async(&client, args("over", key))
            .await
            .unwrap();
    }
    wait_for("both rescheduled", WAIT, || async {
        zcard(&pool, keys::SCHEDULE).await == 2
    })
    .await;
    w.launcher.stop().await.unwrap();
    for (job, _) in sorted_jobs(&pool, keys::SCHEDULE).await {
        assert_eq!(job.limited.as_ref().map(|limited| limited.n), Some(1));
        let key = job.args[1].as_str().unwrap().to_owned();
        let lock = hgetall(&pool, &keys::unique_lock(&digest(&key))).await;
        assert_eq!(lock.get("jid"), Some(&job.jid), "{key}: {lock:?}");
        // Held for the hour the job waits, plus `unique_for`.
        assert!(pttl(&pool, &keys::unique_lock(&digest(&key))).await > 3_600_000);
    }
    assert_eq!(unique_count(&pool, "z").await, 0);
    assert_eq!(get_int(&pool, keys::STAT_FAILED).await, 0);
    assert_feature_keys_expire(&redis).await;
}

/// X10: members over a limit or throttled stay pending while rescheduled; a member past its
/// budget is a failure; members of a cancelled batch are skipped at their next refusal instead
/// of being parked again.
#[tokio::test]
async fn x10_batch_members_over_limits() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let slot = hold_slot(&pool, "x10").await;
    let w = worker(&redis, |builder| {
        builder
            .throttle(
                Throttle::new("x10")
                    .concurrency(1)
                    .max_delay(Duration::from_secs(1)),
            )
            .over_limit_reschedules(1)
    })
    .await;
    let client = w.hench.client().clone();
    let committed = Batch::new(&client)
        .jobs(|jobs| async move {
            // Over its limit, then past its budget of one reschedule: a death (no retries).
            Pro::set()
                .retry(false)
                .perform_async(&jobs, args("over_fast", "a"))
                .await?;
            Pro::set()
                .retry(false)
                .throttle("x10")
                .perform_async(&jobs, args("ok", "b"))
                .await?;
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap();
    let bid = committed.bid.clone();
    wait_for("the throttled member parked", WAIT, || async {
        sorted_jobs(&pool, keys::SCHEDULE)
            .await
            .iter()
            .any(|(job, _)| job.throttle.is_some())
    })
    .await;
    // Rescheduled once, still pending; then past its budget: a failure, counted dead.
    wait_for("the member over its limit dead", WAIT, || async {
        status(&client, &bid).await.dead == 1
    })
    .await;
    assert_eq!(
        w.shared
            .runs
            .count(&format!("over_fast:{}", committed.jids[0])),
        2
    );
    let batch = status(&client, &bid).await;
    assert_eq!((batch.pending, batch.failures), (2, 1));
    client.cancel_batch(&bid).await.unwrap();
    wait_for(
        "the throttled member skipped at its next refusal",
        WAIT,
        || async { status(&client, &bid).await.skipped == 1 },
    )
    .await;
    w.launcher.stop().await.unwrap();
    drop(slot);
    assert!(
        !sorted_jobs(&pool, keys::SCHEDULE)
            .await
            .iter()
            .any(|(job, _)| job.throttle.is_some()),
        "not parked again"
    );
    assert_eq!(w.shared.runs.len(), 2, "the throttled member never ran");
    let batch = status(&client, &bid).await;
    assert_eq!((batch.pending, batch.dead, batch.skipped), (1, 1, 1));
    assert!(batch.complete_at.is_some() && batch.success_at.is_none());
    assert_feature_keys_expire(&redis).await;
}

/// X11: a unique member of a crashed process (reliable fetch) is recovered by another process:
/// its batch counts it once, its lock is released once and never taken back by the dead
/// process's renewals or its stale copy.
#[tokio::test]
async fn x11_a_recovered_unique_member_is_counted_once() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let shared = Arc::new(Shared::default());
    let first = worker_sharing(&redis, shared.clone(), |builder| builder).await;
    let client = first.hench.client().clone();
    let committed = Batch::new(&client)
        .jobs(|jobs| async move {
            Pro::set()
                .unique_for(Duration::from_secs(60))
                .perform_async(&jobs, args("gate", "k11"))
                .await?;
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap();
    wait_for("the member running", WAIT, || async {
        shared.gate.entered() == 1
    })
    .await;
    first.launcher.abandon();
    let () = cmd(
        &pool,
        &redis_cmd!("DEL", &keys::process(first.launcher.identity())),
    )
    .await;
    shared.gate.open();
    let second = worker_sharing(&redis, shared.clone(), |builder| builder).await;
    wait_for("the batch succeeded", Duration::from_secs(20), || async {
        status(&client, &committed.bid).await.success_at.is_some()
    })
    .await;
    // The dead process's renewals are gone: the lock stays released.
    tokio::time::sleep(Duration::from_millis(300)).await;
    second.launcher.stop().await.unwrap();
    let lock = keys::unique_lock(&digest("k11"));
    assert!(!exists(&pool, &lock).await);
    let batch = status(&client, &committed.bid).await;
    assert_eq!((batch.total, batch.pending, batch.failures), (1, 0, 0));
    assert_eq!(unique_count(&pool, "z").await, 0);
    assert_eq!(
        llen(&pool, &keys::inprogress(first.launcher.identity())).await,
        0
    );
    assert_feature_keys_expire(&redis).await;
}

/// X13: callback templates of a unique callback job type are stored without uniqueness:
/// dispatched once per event, their acknowledgement counts no lapse.
#[tokio::test]
async fn x13_unique_callbacks_take_no_lock() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let w = worker(&redis, |builder| builder).await;
    let client = w.hench.client().clone();
    let committed = Batch::new(&client)
        .on(
            hench::batch::BatchEvent::Success,
            Pro::set().unique_for(Duration::from_secs(60)),
            args("ok", "callback"),
        )
        .jobs(|jobs| async move {
            Pro::perform_async(&jobs, args("ok", "member")).await?;
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap();
    let stored: HashMap<String, String> = hgetall(&pool, &keys::batch(&committed.bid)).await;
    let templates: Vec<&String> = stored
        .iter()
        .filter(|(field, _)| field.starts_with("cb"))
        .map(|(_, template)| template)
        .collect();
    assert!(!templates.is_empty(), "{stored:?}");
    for template in templates {
        assert!(!template.contains("\"unique\""), "{template}");
    }
    wait_for("the callback ran", WAIT, || async {
        w.shared.runs.len() == 2
    })
    .await;
    w.launcher.stop().await.unwrap();
    assert_eq!(unique_count(&pool, "z").await, 0);
    assert!(!exists(&pool, &keys::unique_lock(&digest("callback"))).await);
    assert_feature_keys_expire(&redis).await;
}

/// X14: deleting and killing unique batch members through the API counts them dead and
/// releases their locks; `kill_all` over 250 such members accounts each chunk.
#[tokio::test]
async fn x14_api_removals_of_unique_members() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let client = redis.client().await;
    let api = Api::new(pool.clone());
    // Waiting in the schedule: removed before any worker runs them.
    let committed = Batch::new(&client)
        .jobs(|jobs| async move {
            for key in 0..252 {
                Pro::set()
                    .unique_for(Duration::from_secs(600))
                    .delay(Duration::from_secs(3_600))
                    .perform_async(&jobs, args("ok", &format!("k{key}")))
                    .await?;
            }
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap();
    let bid = committed.bid.clone();
    assert_eq!(
        cmd::<u64>(&pool, &redis_cmd!("ZCARD", keys::UNIQUE_LOCKS)).await,
        252
    );
    let scheduled = api.scheduled();
    let mut entries = scheduled
        .page(1, 2, hench::api::SortOrder::Ascending)
        .await
        .unwrap()
        .items;
    let deleted = entries.remove(0);
    assert!(deleted.delete().await.unwrap());
    let killed = entries.remove(0);
    assert!(killed.kill().await.unwrap());
    // Dead members stay pending (not succeeded); `death` fired at the first.
    let batch = status(&client, &bid).await;
    assert_eq!((batch.pending, batch.failures, batch.dead), (252, 2, 2));
    assert!(batch.death_at.is_some() && batch.complete_at.is_none());
    // The rest at once.
    assert_eq!(scheduled.kill_all(false).await.unwrap(), 250);
    let batch = status(&client, &bid).await;
    assert_eq!((batch.pending, batch.failures, batch.dead), (252, 252, 252));
    assert!(batch.complete_at.is_some() && batch.success_at.is_none());
    assert_eq!(
        cmd::<u64>(&pool, &redis_cmd!("ZCARD", keys::UNIQUE_LOCKS)).await,
        0
    );
    let locks: Vec<String> = cmd(&pool, &redis_cmd!("KEYS", "unique:*")).await;
    assert!(locks.is_empty(), "{locks:?}");
    assert_feature_keys_expire(&redis).await;
}

/// Records what it sees before calling the next middleware (X15).
struct Snapshot;

#[hench::async_trait]
impl ServerMiddleware for Snapshot {
    async fn call(
        &self,
        ctx: &JobContext,
        job: &mut JobPayload,
        next: hench::ServerNext<'_>,
    ) -> Result<(), JobError> {
        let pool = ctx.client().redis().clone();
        let key = job.args[1].as_str().unwrap_or_default().to_owned();
        let lock = exists(&pool, &keys::unique_lock(&digest(&key))).await;
        let leases: u64 = cmd(&pool, &redis_cmd!("ZCARD", keys::limit_state('c', "x15"))).await;
        if let Ok(shared) = ctx.data::<Shared>() {
            shared
                .seen
                .lock()
                .unwrap()
                .push(format!("middleware:lock={lock},leases={leases}"));
        }
        next.run(ctx, job).await
    }
}

/// One `MONITOR` line: its arguments.
fn monitor_args(line: &str) -> Vec<String> {
    let Some(close) = line.find(']') else {
        return Vec::new();
    };
    line[close + 1..]
        .split("\" \"")
        .map(|arg| arg.trim().trim_matches('"').to_owned())
        .collect()
}

/// X15: one job using every feature (a periodic tick, a batch member with a `Start` lock, a
/// throttle, a limiter inside) goes through the pipeline in its fixed order: the throttle gate,
/// server middleware, the batch's cancellation check, the `Start` release, `perform`, batch
/// accounting, the outcome write, the permit release, the batch's dispatch, the periodic record.
#[tokio::test]
async fn x15_the_pipeline_order_of_a_job_using_every_feature() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let scripts: HashMap<String, &'static str> = hench::test_util::feature_scripts()
        .into_iter()
        .map(|(name, sha)| (sha, name))
        .collect();
    let monitor = hench::redis_rs::Client::open(redis.url())
        .unwrap()
        .get_async_monitor()
        .await
        .unwrap();
    let transcript: Arc<Mutex<Vec<Vec<String>>>> = Arc::default();
    let recorder = {
        let transcript = transcript.clone();
        tokio::spawn(async move {
            let mut lines = monitor.into_on_message::<String>();
            while let Some(line) = lines.next().await {
                let args = monitor_args(&line);
                let end = args.iter().any(|arg| arg == "x15-end");
                transcript.lock().unwrap().push(args);
                if end {
                    break;
                }
            }
        })
    };
    // A schedule that never fires by itself: the job carries its tick as if it had.
    Api::new(pool.clone())
        .periodic()
        .upsert(&Periodic::cron("x15", "0 0 1 1 *").job::<Pro>(args("probe", "e")))
        .await
        .unwrap();
    let w = worker(&redis, |builder| {
        builder
            .throttle(Throttle::new("x15").concurrency(2))
            .server_middleware(Snapshot)
    })
    .await;
    let client = w.hench.client().clone();
    let committed = Batch::new(&client)
        .jobs(|jobs| async move {
            let mut job = JobPayload::new(Pro::NAME, vec![json!("probe"), json!("e")]);
            job.queue = "default".to_owned();
            job.periodic = Some(hench::periodic::PeriodicMeta::new(
                "x15",
                1_700_000_000_000,
                false,
            ));
            job.unique = Some(
                Unique::new(Duration::from_secs(60))
                    .until(UniqueUntil::Start)
                    .key(json!(["e"]))
                    .into(),
            );
            job.throttle = Some(hench::limit::ThrottleOption::Enabled {
                name: Some("x15".to_owned()),
                key: Some("e".to_owned()),
            });
            jobs.push(job).await?;
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap();
    let bid = committed.bid.clone();
    wait_for("the run recorded", WAIT, || async {
        hgetall(&pool, &keys::periodic("x15"))
            .await
            .get("run_status")
            .is_some_and(|status| status == "ok")
    })
    .await;
    w.launcher.stop().await.unwrap();
    let _: () = cmd(&pool, &redis_cmd!("ECHO", "x15-end")).await;
    recorder.await.unwrap();
    // The snapshots: the gate ran before server middleware (the lease is held), the `Start`
    // lock is released after it, before `perform`.
    assert_eq!(
        w.shared.seen(),
        ["middleware:lock=true,leases=1", "job:lock=false"]
    );
    // The transcript: the steps of the job, in order.
    let jid = committed.jids[0].clone();
    let steps: Vec<String> = transcript
        .lock()
        .unwrap()
        .iter()
        .filter_map(|args| {
            let command = args.first()?.to_ascii_uppercase();
            if command == "HGET" && args.get(1)? == &keys::batch(&bid) {
                return Some("HGET cancelled_at".to_owned());
            }
            if command != "EVALSHA" {
                return None;
            }
            let name = scripts.get(args.get(1)?).copied()?;
            let mentions = |text: &str| args.iter().any(|arg| arg.contains(text));
            let step = match name {
                "LIMIT_ACQUIRE" | "LIMIT_RELEASE" if mentions("x15-inner") => {
                    format!("{name} inner")
                }
                "LIMIT_ACQUIRE" | "LIMIT_RELEASE" if mentions("x15") => format!("{name} gate"),
                "BATCH_COMMIT" | "LIMIT_RENEW" | "UNIQUE_RENEW" | "PERIODIC_LEASE"
                | "PERIODIC_SYNC" | "PERIODIC_UPSERT" | "BATCH_TOUCH" => return None,
                "PERIODIC_RECORD_RUN" | "BATCH_DISPATCH" => name.to_owned(),
                _ if mentions(&jid) || mentions(&bid) || mentions(&digest("e")) => name.to_owned(),
                _ => return None,
            };
            Some(step)
        })
        .collect();
    // (A script's first call in this Redis shows twice: `NOSCRIPT`, then again once loaded.
    // The job's own permit is released when it is dropped, in the background: it has no place
    // in the pipeline's order.)
    assert!(
        steps.iter().any(|step| step == "LIMIT_RELEASE inner"),
        "{steps:?}"
    );
    let order = [
        "LIMIT_ACQUIRE gate",
        "HGET cancelled_at",
        "UNIQUE_RELEASE",
        "LIMIT_ACQUIRE inner",
        "BATCH_SUCCEEDED",
        "ACK_UNIQUE",
        "LIMIT_RELEASE gate",
        "BATCH_DISPATCH",
        "PERIODIC_RECORD_RUN",
    ];
    let positions: Vec<usize> = order
        .iter()
        .map(|step| {
            steps
                .iter()
                .position(|seen| seen == step)
                .unwrap_or_else(|| panic!("no {step} in {steps:?}"))
        })
        .collect();
    assert!(
        positions.windows(2).all(|pair| pair[0] < pair[1]),
        "{steps:?}"
    );
    let batch = status(&client, &bid).await;
    assert!(batch.success_at.is_some());
    assert_feature_keys_expire(&redis).await;
}

/// X16: the metrics of one class for each outcome of the pipeline: success, failure, over a
/// limit (rescheduled), throttled (parked before it started), skipped (cancelled batch).
#[tokio::test]
async fn x16_metrics_of_every_outcome() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let client = redis.client().await;
    let slot = hold_slot(&pool, "x16").await;
    // Waiting before the worker starts: a member of a cancelled batch.
    let committed = Batch::new(&client)
        .jobs(|jobs| async move {
            Pro::perform_async(&jobs, args("ok", "skipped")).await?;
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap();
    client.cancel_batch(&committed.bid).await.unwrap();
    Pro::perform_async(&client, args("ok", "a")).await.unwrap();
    Pro::set()
        .retry(false)
        .perform_async(&client, args("fail", "b"))
        .await
        .unwrap();
    Pro::perform_async(&client, args("over", "c"))
        .await
        .unwrap();
    Pro::set()
        .throttle("x16")
        .perform_async(&client, args("ok", "d"))
        .await
        .unwrap();
    let w = worker(&redis, |builder| {
        builder.throttle(Throttle::new("x16").concurrency(1))
    })
    .await;
    wait_for("every outcome", WAIT, || async {
        zcard(&pool, keys::SCHEDULE).await == 2
            && status(&client, &committed.bid).await.skipped == 1
            && w.shared.runs.len() == 3
    })
    .await;
    w.launcher.stop().await.unwrap();
    drop(slot);
    let result = hench::metrics::Query::new(&pool)
        .for_job(Pro::NAME, hench::metrics::Period::Minutes(60))
        .await
        .unwrap();
    let totals = result.jobs[Pro::NAME].totals;
    // Executions: the success, the failure, the over-limit run and the skipped member.
    assert_eq!(totals.processed, 4, "{totals:?}");
    assert_eq!(totals.failed, 1);
    assert_eq!(totals.over_limit, 1);
    assert_eq!(totals.throttled, 1);
    assert_eq!(totals.skipped, 1);
    // Only the success is timed.
    assert_eq!(totals.timed(), 1);
    assert_eq!(get_int(&pool, keys::STAT_PROCESSED).await, 4);
    assert_eq!(get_int(&pool, keys::STAT_FAILED).await, 1);
    assert_feature_keys_expire(&redis).await;
}

/// An iterable member over a limit on one item (X12).
#[derive(Default)]
struct Paced {
    refused: Arc<Mutex<HashMap<String, usize>>>,
    log: Arc<Mutex<Vec<String>>>,
}

impl IterableJob for Paced {
    const NAME: &'static str = "pro::Paced";
    type Args = (i64,);
    type Item = i64;
    type Cursor = usize;

    fn build_enumerator<'a>(
        &'a self,
        _ctx: &'a JobContext,
        _args: &'a (i64,),
        cursor: Option<usize>,
    ) -> Result<Enumerator<'a, i64, usize>, JobError> {
        Ok(Enumerator::iter(0..4_i64, cursor))
    }

    async fn each_iteration(&self, item: i64, it: &Iteration<'_, Self>) -> Result<(), JobError> {
        let jid = it.ctx().jid().to_owned();
        if item == 2 {
            let mut refused = self.refused.lock().unwrap();
            let count = refused.entry(jid.clone()).or_default();
            *count += 1;
            if *count == 1 {
                return Err(JobError::over_limit(
                    OverLimit::new("w:items").with_retry_after(Duration::from_millis(100)),
                ));
            }
        }
        self.log.lock().unwrap().push(format!("{jid}:{item}"));
        Ok(())
    }
}

/// X12: iterable members refused on an item keep their cursor and resume; their batch
/// completes and succeeds only after the last item of each.
#[tokio::test]
async fn x12_iterable_members_over_a_limit_finish_their_batch() {
    let redis = TestRedis::start().await;
    init_logging();
    let job = Paced::default();
    let log = job.log.clone();
    let hench = redis.builder().await.register(job).build().await.unwrap();
    let launcher = hench.start().await.unwrap();
    let client = hench.client().clone();
    let committed = Batch::new(&client)
        .jobs(|jobs| async move {
            for n in 0..3 {
                <Paced as JobExt>::perform_async(&jobs, (n,)).await?;
            }
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap();
    wait_for("the batch succeeded", Duration::from_secs(20), || async {
        status(&client, &committed.bid).await.success_at.is_some()
    })
    .await;
    launcher.stop().await.unwrap();
    let log = log.lock().unwrap().clone();
    for jid in &committed.jids {
        let items: Vec<&String> = log.iter().filter(|entry| entry.starts_with(jid)).collect();
        // Each item once: the refused item ran again in the next execution, after the cursor.
        assert_eq!(items.len(), 4, "{jid}: {log:?}");
    }
    let batch = status(&client, &committed.bid).await;
    assert_eq!((batch.total, batch.pending, batch.failures), (3, 0, 0));
    assert_feature_keys_expire(&redis).await;
}

/// X18: the testing modes together: a fake client with locks, batches and in-memory limits on
/// tokio's paused clock, and an inline client.
#[tokio::test(start_paused = true)]
async fn x18_testing_modes_together() {
    let testing = Testing::new()
        .auto_register(false)
        .register(Pro)
        .data(Shared::default())
        .limits(TestLimits::InMemory)
        .throttle(Throttle::new("t18").concurrency(1));
    let fake = testing.fake();
    let unique = || Pro::set().unique_for(Duration::from_secs(60));
    // A batch of two unique members, one throttled.
    let committed = Batch::new(&fake)
        .jobs(|jobs| async move {
            unique().perform_async(&jobs, args("ok", "a")).await?;
            unique()
                .throttle("t18")
                .perform_async(&jobs, args("over", "b"))
                .await?;
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap();
    // Locked: duplicates are not captured.
    assert_eq!(
        unique()
            .perform_async(&fake, args("ok", "a"))
            .await
            .unwrap(),
        None
    );
    // The drain runs `a`, parks `b` (over its limit: captured again, not run again by this
    // drain), and the batch keeps `b` pending.
    assert_eq!(fake.drain::<Pro>().await.unwrap(), 2);
    let parked = fake.jobs::<Pro>();
    assert_eq!(parked.len(), 1);
    assert!(parked[0].limited.is_some() && parked[0].at.is_some());
    assert_eq!(fake.limited_jobs::<Pro>().len(), 1);
    let batch = status(&fake, &committed.bid).await;
    assert_eq!((batch.pending, batch.total), (1, 2));
    // The parked job keeps its lock.
    assert!(fake.unique_holder::<Pro>(&args("over", "b")).is_some());
    // Cancelled: the parked member is skipped by the next drain.
    fake.cancel_batch(&committed.bid).await.unwrap();
    fake.drain_all().await.unwrap();
    let batch = status(&fake, &committed.bid).await;
    assert_eq!((batch.pending, batch.skipped), (0, 1));
    assert_eq!(fake.unique_holder::<Pro>(&args("over", "b")), None);

    // Inline: an over-limit member is a failure, counted dead in its batch.
    let inline = testing.inline();
    let refused = Batch::new(&inline)
        .jobs(|jobs| async move {
            Pro::perform_async(&jobs, args("over", "c")).await?;
            Ok::<_, hench::Error>(())
        })
        .await;
    assert!(refused.is_err(), "{refused:?}");
    // Workflow steps resolve in a testing registry built by hand.
    let registry = Testing::new().auto_register(false).register(Pro);
    let client = registry.fake();
    let step = client
        .push(JobPayload::new(WORKFLOW_NEXT, vec![]))
        .await
        .unwrap();
    assert!(step.is_some());
    let error = client.drain_for(WORKFLOW_NEXT).await.unwrap_err();
    assert!(error.to_string().contains("invalid"), "{error}");
}

/// X24: jobs carrying values a newer version may write still parse and run: an unknown unique
/// policy, a throttle this process does not define.
#[tokio::test]
async fn x24_newer_feature_values_still_run() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let w = worker(&redis, |builder| builder).await;
    let job = json!({
        "class": Pro::NAME,
        "args": ["ok", "x24"],
        "jid": "0123456789abcdef0123abcd",
        "queue": "default",
        "retry": true,
        "created_at": 1_730_000_000_000_i64,
        "enqueued_at": 1_730_000_000_000_i64,
        "unique": {"for": 60, "until": "on-the-moon", "digest": "d24", "v": 2},
        "throttle": {"name": "from-the-future", "key": "k"},
        "periodic": {"name": "gone", "tick": 1_730_000_000_000_i64, "spec": {"v": 2}},
    })
    .to_string();
    push_raw(&pool, "default", job.as_bytes()).await;
    wait_for("the job ran", WAIT, || async { w.shared.runs.len() == 1 }).await;
    wait_for("acknowledged", WAIT, || async {
        llen(&pool, &keys::inprogress(w.launcher.identity())).await == 0
    })
    .await;
    w.launcher.stop().await.unwrap();
    assert_eq!(get_int(&pool, keys::STAT_FAILED).await, 0);
    assert_eq!(zcard(&pool, keys::DEAD).await, 0);
    assert_feature_keys_expire(&redis).await;
}

/// The heartbeat renews the locks of running unique jobs and the permits of running throttled
/// jobs in one pipeline (`docs/design/pro-features.md` §6.4): both outlive their leases while
/// the job runs, and nothing is renewed once it finished.
#[tokio::test]
async fn one_heartbeat_pipeline_renews_locks_and_permits() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let w = worker(&redis, |builder| {
        builder
            .heartbeat_interval(Duration::from_millis(200))
            .throttle(
                Throttle::new("beat")
                    .concurrency(1)
                    .lease(Duration::from_secs(1)),
            )
    })
    .await;
    let client = w.hench.client().clone();
    Pro::set()
        .unique_for(Duration::from_secs(1))
        .throttle("beat")
        .perform_async(&client, args("gate", "beat"))
        .await
        .unwrap();
    wait_for("running", WAIT, || async { w.shared.gate.entered() == 1 }).await;
    let lock = keys::unique_lock(&digest("beat"));
    // Three seconds: three times the lock's and the lease's lives.
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while std::time::Instant::now() < deadline {
        assert!(exists(&pool, &lock).await, "the lock lapsed");
        let leases: u64 = cmd(&pool, &redis_cmd!("ZCARD", keys::limit_state('c', "beat"))).await;
        assert_eq!(leases, 1, "the lease lapsed");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    w.shared.gate.open();
    wait_for("released", WAIT, || async {
        !exists(&pool, &lock).await && !exists(&pool, &keys::limit_state('c', "beat")).await
    })
    .await;
    w.launcher.stop().await.unwrap();
    assert_eq!(unique_count(&pool, "z").await, 0);
    assert_feature_keys_expire(&redis).await;
}

/// Seconds since the epoch.
fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

/// Round 3 (docs-usability-12): a throttle's reschedule budget is written like a limiter's
/// (`.reschedules(5)`), or as an `Option` (`None`: unlimited, the default).
#[test]
fn a_throttles_reschedules_are_written_like_a_limiters() {
    use hench::Throttle;
    assert_eq!(
        Throttle::new("stripe").concurrency(3).reschedules(5),
        Throttle::new("stripe").concurrency(3).reschedules(Some(5))
    );
    assert_eq!(
        Throttle::new("stripe").reschedules(None),
        Throttle::new("stripe")
    );
}
