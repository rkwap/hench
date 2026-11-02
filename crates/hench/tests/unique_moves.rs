//! Unique locks through moves (round 3: unique-2, unique-5, unique-6). A job a move drops
//! (client middleware stopping the re-push of a due job, or of an API move) or the scheduler
//! buries releases its lock, as a delete or a kill does; a move to a later time (the scheduler
//! deferring a due job whose push failed, `SortedEntry::reschedule`) extends the lock to cover
//! the new wait; typed pushes apply `Job::unique_key` whatever layer of options makes the job
//! unique. Against a private `redis-server`.
#![cfg(feature = "test-util")]

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use hench::api::Api;
use hench::batch::Batch;
use hench::prelude::*;
use hench::test_util::TestRedis;
use hench::{Hench, Launcher, PushOutcome, keys};
use serde_json::{Value, json};
use support::*;

/// Unique for a minute, keyed by its first argument; the second tells the gatekeeper what to
/// do with it.
#[derive(Default)]
struct Keyed;

impl Job for Keyed {
    const NAME: &'static str = "moves::Keyed";
    type Args = (i64, String);

    fn options() -> JobOptions {
        JobOptions::new().unique_for(Duration::from_secs(60))
    }

    fn unique_key((key, _): &Self::Args) -> Option<Value> {
        Some(json!([key]))
    }

    async fn perform(&self, ctx: &JobContext, (key, _): Self::Args) -> Result<(), JobError> {
        ctx.data::<Runs>()?.record(format!("ran:{key}"));
        Ok(())
    }
}

/// Client middleware: stops the push (`Ok(None)`) of a `Keyed` job whose mode is `halt`,
/// rejects as invalid one whose mode is `invalid`, and fails the others while `down` (a
/// dependency of the middleware is out).
#[derive(Clone, Default)]
struct Gatekeeper {
    down: Arc<AtomicBool>,
}

#[async_trait]
impl ClientMiddleware for Gatekeeper {
    async fn call(
        &self,
        job: JobPayload,
        _redis: &RedisPool,
        next: ClientNext<'_>,
    ) -> hench::Result<Option<JobPayload>> {
        if job.class == Keyed::NAME {
            match job.args.get(1).and_then(Value::as_str) {
                Some("halt") => return Ok(None),
                Some("invalid") => {
                    return Err(hench::Error::InvalidJob("tenant disabled".to_owned()));
                }
                _ if self.down.load(Ordering::SeqCst) => {
                    return Err(hench::Error::Config("key service unavailable".to_owned()));
                }
                _ => {}
            }
        }
        next.run(job).await
    }
}

fn digest(key: i64) -> String {
    hench::unique::digest(Keyed::NAME, Some("default"), &json!([key])).unwrap()
}

/// The lock of `key`: its fields and remaining time to live.
async fn lock_of(pool: &RedisPool, key: i64) -> (std::collections::HashMap<String, String>, i64) {
    let lock = keys::unique_lock(&digest(key));
    (hgetall(pool, &lock).await, pttl(pool, &lock).await)
}

fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

/// Workers whose client runs `gatekeeper` (the scheduler re-pushes through it).
async fn workers(redis: &TestRedis, gatekeeper: &Gatekeeper) -> (Hench, Launcher, Arc<Runs>) {
    let runs = Arc::new(Runs::default());
    let hench = redis
        .builder()
        .await
        .register(Keyed)
        .data_arc(runs.clone())
        .client_middleware(gatekeeper.clone())
        .build()
        .await
        .unwrap();
    let launcher = hench.start().await.unwrap();
    (hench, launcher, runs)
}

#[tokio::test]
async fn a_due_job_the_scheduler_drops_releases_its_lock() {
    let redis = TestRedis::start().await;
    let (client, pool) = (redis.client().await, redis.pool().await);
    let (_hench, launcher, runs) = workers(&redis, &Gatekeeper::default()).await;
    let jid = Keyed::set()
        .delay(Duration::from_millis(300))
        .perform_async(&client, (1, "halt".to_owned()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(lock_of(&pool, 1).await.0.get("jid"), Some(&jid));
    wait_for("the scheduler to drop the job", WAIT, || async {
        zcard(&pool, keys::SCHEDULE).await == 0
    })
    .await;
    // Dropped with its lock: the next push of the key is a job of its own.
    assert!(lock_of(&pool, 1).await.0.is_empty());
    let next = Keyed::set()
        .push_outcome(&client, (1, "ok".to_owned()))
        .await
        .unwrap();
    assert!(matches!(next, PushOutcome::Pushed { .. }), "{next:?}");
    runs.wait_for(1).await;
    launcher.stop().await.unwrap();
}

#[tokio::test]
async fn a_due_job_the_scheduler_buries_releases_its_lock() {
    let redis = TestRedis::start().await;
    let (client, pool) = (redis.client().await, redis.pool().await);
    let (_hench, launcher, _runs) = workers(&redis, &Gatekeeper::default()).await;
    Keyed::set()
        .delay(Duration::from_millis(300))
        .perform_async(&client, (2, "invalid".to_owned()))
        .await
        .unwrap()
        .unwrap();
    wait_for("the scheduler to bury the job", WAIT, || async {
        zcard(&pool, keys::DEAD).await == 1
    })
    .await;
    assert_eq!(zcard(&pool, keys::SCHEDULE).await, 0);
    assert!(lock_of(&pool, 2).await.0.is_empty(), "buried with its lock");
    launcher.stop().await.unwrap();
}

#[tokio::test]
async fn a_batch_member_the_scheduler_drops_counts_dead_in_its_batch() {
    let redis = TestRedis::start().await;
    let (client, pool) = (redis.client().await, redis.pool().await);
    let committed = Batch::new(&client)
        .jobs(|jobs| async move {
            Keyed::set()
                .delay(Duration::from_millis(300))
                .perform_async(&jobs, (3, "halt".to_owned()))
                .await
        })
        .await
        .unwrap();
    let (_hench, launcher, _runs) = workers(&redis, &Gatekeeper::default()).await;
    let batches = Api::new(pool.clone()).batches();
    wait_for("the member to count dead", WAIT, || async {
        batches
            .fetch(&committed.bid)
            .await
            .unwrap()
            .is_some_and(|status| status.dead == 1)
    })
    .await;
    let status = batches.fetch(&committed.bid).await.unwrap().unwrap();
    assert!(status.death_at.is_some());
    let failed = batches.failures(&committed.bid, 1, 10).await.unwrap().items;
    assert_eq!(failed[0].reason, "deleted");
    assert!(lock_of(&pool, 3).await.0.is_empty());
    launcher.stop().await.unwrap();
}

#[tokio::test]
async fn an_api_move_that_drops_a_job_releases_its_lock() {
    let redis = TestRedis::start().await;
    let (client, pool) = (redis.client().await, redis.pool().await);
    let gatekeeper = Gatekeeper::default();
    let api = Api::from_client(client.with_middleware(|chain| {
        chain.add(gatekeeper.clone());
    }));
    // One entry moved by itself, then two by `add_all_to_queue`.
    let mut jids = Vec::new();
    for key in [4, 5, 6] {
        jids.push(
            Keyed::set()
                .delay(Duration::from_secs(3_600))
                .perform_async(&client, (key, "halt".to_owned()))
                .await
                .unwrap()
                .unwrap(),
        );
    }
    let entry = api.scheduled().find_job(&jids[0]).await.unwrap().unwrap();
    assert!(entry.add_to_queue().await.unwrap());
    assert!(
        lock_of(&pool, 4).await.0.is_empty(),
        "dropped with its lock"
    );
    assert_eq!(api.scheduled().add_all_to_queue().await.unwrap(), 0);
    assert_eq!(zcard(&pool, keys::SCHEDULE).await, 0);
    for key in [5, 6] {
        assert!(lock_of(&pool, key).await.0.is_empty(), "key {key}");
    }
    assert_eq!(queue_size(&pool, "default").await, 0);
}

#[tokio::test]
async fn the_scheduler_extends_the_lock_of_a_due_job_it_defers() {
    let redis = TestRedis::start().await;
    let (client, pool) = (redis.client().await, redis.pool().await);
    let gatekeeper = Gatekeeper::default();
    gatekeeper.down.store(true, Ordering::SeqCst);
    let (_hench, launcher, runs) = workers(&redis, &gatekeeper).await;
    let jid = Keyed::set()
        .unique_for(Duration::from_secs(2))
        .delay(Duration::from_millis(300))
        .perform_async(&client, (7, "ok".to_owned()))
        .await
        .unwrap()
        .unwrap();
    // The push fails (not because of Redis): the job waits a minute, and its lock with it. The
    // scheduler's lease also moves the job a minute ahead, just before the deferral, so wait
    // for the lock to record the deferral rather than for the score alone to move.
    wait_for(
        "the scheduler to defer the job and its lock",
        WAIT,
        || async {
            let Some(score) = sorted_jobs(&pool, keys::SCHEDULE)
                .await
                .first()
                .map(|(_, score)| *score)
            else {
                return false;
            };
            let (fields, _) = lock_of(&pool, 7).await;
            score > now_secs() + 30.0
                && fields
                    .get("at")
                    .and_then(|at| at.parse::<f64>().ok())
                    .is_some_and(|at| (at - score).abs() < 0.001)
        },
    )
    .await;
    let (fields, ttl) = lock_of(&pool, 7).await;
    assert_eq!(fields.get("jid"), Some(&jid));
    assert!(ttl > 55_000, "the lock covers the deferral: {ttl} ms");
    let score = sorted_jobs(&pool, keys::SCHEDULE).await[0].1;
    let recorded: f64 = fields["at"].parse().unwrap();
    assert!((recorded - score).abs() < 0.001, "{recorded} vs {score}");
    // Long after `unique_for` (2 s), the waiting job still holds its lock.
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    let again = Keyed::set()
        .unique_for(Duration::from_secs(2))
        .push_outcome(&client, (7, "ok".to_owned()))
        .await
        .unwrap();
    assert!(matches!(again, PushOutcome::Duplicate { .. }), "{again:?}");
    assert!(runs.entries().is_empty());
    launcher.stop().await.unwrap();
}

#[tokio::test]
async fn reschedule_extends_the_lock_and_records_the_new_time() {
    let redis = TestRedis::start().await;
    let (client, pool) = (redis.client().await, redis.pool().await);
    let jid = Keyed::set()
        .unique_for(Duration::from_secs(4))
        .delay(Duration::from_secs(1))
        .perform_async(&client, (8, "ok".to_owned()))
        .await
        .unwrap()
        .unwrap();
    let api = Api::new(pool.clone());
    let mut entry = api.scheduled().find_job(&jid).await.unwrap().unwrap();
    let later = SystemTime::now() + Duration::from_secs(3_600);
    assert!(entry.reschedule(later).await.unwrap());
    let (fields, ttl) = lock_of(&pool, 8).await;
    assert_eq!(fields.get("jid"), Some(&jid));
    assert!(ttl > 3_600_000, "for + the new delay: {ttl} ms");
    let recorded: f64 = fields["at"].parse().unwrap();
    let score = sorted_jobs(&pool, keys::SCHEDULE).await[0].1;
    assert!((recorded - score).abs() < 0.001, "{recorded} vs {score}");
    // A debounce push finds the holder at its new time.
    let replaced = Keyed::set()
        .unique_for(Duration::from_secs(4))
        .delay(Duration::from_secs(3_600))
        .on_conflict(OnConflict::Replace)
        .push_outcome(&client, (8, "v2".to_owned()))
        .await
        .unwrap();
    assert!(
        matches!(replaced, PushOutcome::Replaced { .. }),
        "{replaced:?}"
    );
    // Moving a job earlier never shortens its lock.
    let holder = lock_of(&pool, 8).await.0["jid"].clone();
    let mut entry = api.scheduled().find_job(&holder).await.unwrap().unwrap();
    let before = lock_of(&pool, 8).await.1;
    assert!(
        entry
            .reschedule(SystemTime::now() + Duration::from_secs(60))
            .await
            .unwrap()
    );
    let (fields, after) = lock_of(&pool, 8).await;
    assert!(after > before - 5_000, "{after} after {before}");
    let recorded: f64 = fields["at"].parse().unwrap();
    assert!((recorded - (now_secs() + 60.0)).abs() < 5.0);
    // A job of another holder's lock (its lock lapsed and was taken) leaves that lock alone.
    let other = Keyed::set()
        .unique_for(Duration::from_secs(60))
        .delay(Duration::from_secs(600))
        .perform_async(&client, (9, "ok".to_owned()))
        .await
        .unwrap()
        .unwrap();
    let lock = keys::unique_lock(&digest(9));
    let _: i64 = cmd(&pool, &redis_cmd!("HSET", &lock, &"jid", &"someone-else")).await;
    let mut entry = api.scheduled().find_job(&other).await.unwrap().unwrap();
    assert!(
        entry
            .reschedule(SystemTime::now() + Duration::from_secs(7_200))
            .await
            .unwrap()
    );
    let (fields, ttl) = lock_of(&pool, 9).await;
    assert_eq!(fields["jid"], "someone-else");
    assert!(ttl <= 660_000, "untouched: {ttl} ms");
}

/// Keyed by the user (first argument), with no uniqueness of its own.
#[derive(Default)]
struct PerUser;

impl Job for PerUser {
    const NAME: &'static str = "moves::PerUser";
    type Args = (i64, String);

    fn unique_key((user, _): &Self::Args) -> Option<Value> {
        Some(json!([user]))
    }

    async fn perform(&self, _ctx: &JobContext, _args: Self::Args) -> Result<(), JobError> {
        Ok(())
    }
}

#[tokio::test]
async fn typed_pushes_apply_the_unique_key_when_the_client_defaults_make_jobs_unique() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let client = redis
        .client()
        .await
        .with_default_job_options(JobOptions::new().unique_for(Duration::from_secs(600)));
    let first = PerUser::perform_async(&client, (1, "a".to_owned()))
        .await
        .unwrap();
    assert!(first.is_some());
    // Same user, other arguments: a duplicate, by the job type's `unique_key`.
    let second = PerUser::perform_async(&client, (1, "b".to_owned()))
        .await
        .unwrap();
    assert!(second.is_none());
    assert_eq!(queue_size(&pool, "default").await, 1);
    let bulk = PerUser::perform_bulk(&client, vec![(1, "c".to_owned()), (2, "c".to_owned())])
        .await
        .unwrap();
    assert!(bulk[0].is_none() && bulk[1].is_some(), "{bulk:?}");
    // The lock is where `find::<J>` looks for it.
    let found = Api::from_client(client.clone())
        .unique_locks()
        .find::<PerUser>(&(1, "z".to_owned()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(Some(found.jid), first);

    // The same through a testing client.
    let fake = Client::fake()
        .with_default_job_options(JobOptions::new().unique_for(Duration::from_secs(600)));
    assert!(
        PerUser::perform_async(&fake, (1, "a".to_owned()))
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        PerUser::perform_async(&fake, (1, "b".to_owned()))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        fake.unique_holder::<PerUser>(&(1, "q".to_owned()))
            .is_some()
    );
}
