//! Unique jobs in the API (`docs/design/unique.md` §3.5, A1-A4): deletes and kills release the
//! lock of the job they remove (never another job's), moves and `clear` keep locks, and
//! `UniqueLocks` lists, finds, releases and trims locks and reads their counters. Against a
//! private `redis-server`.
#![cfg(feature = "test-util")]

mod support;

use std::time::{Duration, SystemTime};

use futures::TryStreamExt;
use hench::api::{Api, SortOrder};
use hench::metrics::Period;
use hench::prelude::*;
use hench::test_util::TestRedis;
use hench::unique::HolderPlace;
use hench::{UniqueOption, keys};
use serde_json::{Value, json};
use support::*;

/// A unique job keyed by its first argument (`success`, ten minutes).
#[derive(Default)]
struct Sync;

impl Job for Sync {
    const NAME: &'static str = "api::Sync";
    type Args = (i64, String);

    fn options() -> JobOptions {
        JobOptions::new()
            .queue("sync")
            .unique_for(Duration::from_secs(600))
    }

    fn unique_key((user, _): &Self::Args) -> Option<Value> {
        Some(json!([user]))
    }

    async fn perform(&self, _ctx: &JobContext, _args: Self::Args) -> Result<(), JobError> {
        Ok(())
    }
}

fn digest(user: i64) -> String {
    hench::unique::digest(Sync::NAME, Some("sync"), &json!([user])).unwrap()
}

async fn holder(pool: &RedisPool, user: i64) -> Option<String> {
    cmd(
        pool,
        &redis_cmd!("HGET", &keys::unique_lock(&digest(user)), &"jid"),
    )
    .await
}

/// Pushes `Sync` for `user`, scheduled an hour ahead.
async fn scheduled(client: &Client, user: i64) -> String {
    Sync::set()
        .delay(Duration::from_secs(3_600))
        .perform_async(client, (user, "x".to_owned()))
        .await
        .unwrap()
        .expect("pushed")
}

/// Moves the job `jid` from the schedule to the retry set, as a failure would (its lock
/// untouched).
async fn to_retry_set(pool: &RedisPool, jid: &str) {
    move_entry(pool, keys::SCHEDULE, keys::RETRY, jid).await;
}

/// Moves the job `jid` from the sorted set `from` to `to`, same score, its lock untouched.
async fn move_entry(pool: &RedisPool, from: &str, to: &str, jid: &str) {
    let (job, score) = sorted_jobs(pool, from)
        .await
        .into_iter()
        .find(|(job, _)| job.jid == jid)
        .unwrap();
    let raw = job.to_json().unwrap();
    let removed: i64 = cmd(pool, &redis_cmd!("ZREM", &from, &raw)).await;
    assert_eq!(removed, 1);
    let () = cmd(pool, &redis_cmd!("ZADD", &to, &score, &raw)).await;
}

#[tokio::test]
async fn deleting_a_job_releases_its_lock_and_only_its_own() {
    // A1.
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let api = Api::new(pool.clone());
    // Queued.
    let queued = Sync::perform_async(&client, (1, "x".into()))
        .await
        .unwrap()
        .unwrap();
    let record = api.queue("sync").find_job(&queued).await.unwrap().unwrap();
    assert!(record.delete().await.unwrap());
    assert_eq!(holder(&pool, 1).await, None);
    assert!(
        Sync::perform_async(&client, (1, "again".into()))
            .await
            .unwrap()
            .is_some()
    );
    // Scheduled.
    let jid = scheduled(&client, 2).await;
    let entry = api.scheduled().find_job(&jid).await.unwrap().unwrap();
    assert!(entry.delete().await.unwrap());
    assert_eq!(holder(&pool, 2).await, None);
    // Retrying, deleted by value.
    let jid = scheduled(&client, 3).await;
    to_retry_set(&pool, &jid).await;
    let entry = api.retries().find_job(&jid).await.unwrap().unwrap();
    assert!(api.retries().delete_by_value(entry.value()).await.unwrap());
    assert_eq!(holder(&pool, 3).await, None);
    // A job whose lock lapsed and was taken by a duplicate: deleting it leaves the duplicate's.
    let jid = scheduled(&client, 4).await;
    let () = cmd(&pool, &redis_cmd!("DEL", &keys::unique_lock(&digest(4)))).await;
    let duplicate = Sync::perform_async(&client, (4, "dup".into()))
        .await
        .unwrap()
        .unwrap();
    let entry = api.scheduled().find_job(&jid).await.unwrap().unwrap();
    assert!(entry.delete().await.unwrap());
    assert_eq!(holder(&pool, 4).await, Some(duplicate));
    // Deleting a job that is gone: nothing released.
    assert!(!entry.delete().await.unwrap());
}

#[tokio::test]
async fn kills_release_locks_and_moves_keep_them() {
    // A2, A3.
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let api = Api::new(pool.clone());
    let killed = scheduled(&client, 1).await;
    let entry = api.scheduled().find_job(&killed).await.unwrap().unwrap();
    assert!(entry.kill().await.unwrap());
    assert_eq!(holder(&pool, 1).await, None);
    assert_eq!(zcard(&pool, keys::DEAD).await, 1);
    // kill_all, over more than a chunk of 100.
    for user in 100..250 {
        scheduled(&client, user).await;
    }
    assert_eq!(api.scheduled().kill_all(false).await.unwrap(), 150);
    assert_eq!(zcard(&pool, keys::UNIQUE_LOCKS).await, 0);
    // Moves keep the lock: retry, add to queue, retry_all.
    let retried = scheduled(&client, 2).await;
    to_retry_set(&pool, &retried).await;
    let entry = api.retries().find_job(&retried).await.unwrap().unwrap();
    assert!(entry.retry().await.unwrap());
    assert_eq!(holder(&pool, 2).await, Some(retried));
    let added = scheduled(&client, 3).await;
    let entry = api.scheduled().find_job(&added).await.unwrap().unwrap();
    assert!(entry.add_to_queue().await.unwrap());
    assert_eq!(holder(&pool, 3).await, Some(added));
    let all = scheduled(&client, 4).await;
    to_retry_set(&pool, &all).await;
    assert_eq!(api.retries().retry_all().await.unwrap(), 1);
    assert_eq!(holder(&pool, 4).await, Some(all));
    assert_eq!(queue_size(&pool, "sync").await, 3);
    // `clear` keeps locks (they expire).
    let cleared = scheduled(&client, 5).await;
    api.scheduled().clear().await.unwrap();
    api.queue("sync").clear().await.unwrap();
    assert_eq!(holder(&pool, 5).await, Some(cleared));
    assert!(holder(&pool, 2).await.is_some());
}

#[tokio::test]
async fn expired_locks_are_not_released_by_removals() {
    // `expired` locks live their time to live whatever happens to the job.
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let api = Api::new(pool.clone());
    let jid = Sync::set()
        .unique_until(UniqueUntil::Expired)
        .delay(Duration::from_secs(60))
        .perform_async(&client, (1, "x".into()))
        .await
        .unwrap()
        .unwrap();
    let entry = api.scheduled().find_job(&jid).await.unwrap().unwrap();
    assert!(entry.delete().await.unwrap());
    assert_eq!(holder(&pool, 1).await, Some(jid));
}

#[tokio::test]
async fn unique_locks_lists_finds_and_releases_locks() {
    // A4.
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let api = Api::new(pool.clone());
    let locks = api.unique_locks();
    assert_eq!(locks.size().await.unwrap(), 0);
    let long = Sync::set()
        .unique_for(Duration::from_secs(300))
        .perform_async(&client, (1, "a".into()))
        .await
        .unwrap()
        .unwrap();
    let short = Sync::set()
        .unique_for(Duration::from_secs(60))
        .perform_async(&client, (2, "b".into()))
        .await
        .unwrap()
        .unwrap();
    let later = Sync::set()
        .unique_for(Duration::from_secs(60))
        .unique_until(UniqueUntil::Start)
        .delay(Duration::from_secs(3_600))
        .perform_async(&client, (3, "c".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(locks.size().await.unwrap(), 3);
    let page = locks.page(1, 25, SortOrder::Ascending).await.unwrap();
    let order: Vec<&str> = page.items.iter().map(|lock| lock.jid.as_str()).collect();
    assert_eq!(order, [short.as_str(), long.as_str(), later.as_str()]);
    assert_eq!(page.total, 3);
    let page = locks.page(1, 2, SortOrder::Descending).await.unwrap();
    let order: Vec<&str> = page.items.iter().map(|lock| lock.jid.as_str()).collect();
    assert_eq!(order, [later.as_str(), long.as_str()]);
    let second = locks.page(2, 2, SortOrder::Descending).await.unwrap();
    assert_eq!(second.items.len(), 1);
    assert_eq!(second.items[0].jid, short);
    // Every field of a lock.
    let lock = locks.get(&digest(3)).await.unwrap().unwrap();
    assert_eq!(lock.jid, later);
    assert_eq!(lock.class, Sync::NAME);
    assert_eq!(lock.queue, "sync");
    assert_eq!(lock.until, UniqueUntil::Start);
    assert_eq!(lock.unique_for, Duration::from_secs(60));
    let now = SystemTime::now();
    let expires = lock.expires_at.unwrap();
    assert!(expires > now + Duration::from_secs(3_600), "{expires:?}");
    assert!(lock.scheduled_at.is_some());
    assert!(
        lock.locked_at
            .is_some_and(|at| at <= now + Duration::from_secs(5))
    );
    assert_eq!(locks.get("no-such-digest").await.unwrap(), None);
    assert_eq!(locks.get("not a digest").await.unwrap(), None);
    // Find what a push would collide with.
    let found = locks
        .find::<Sync>(&(1, "anything".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.jid, long);
    assert_eq!(
        locks
            .find_in::<Sync>("other", &(1, "x".into()))
            .await
            .unwrap(),
        None
    );
    let found = locks
        .find_for(Sync::NAME, Some("sync"), &json!([2]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.jid, short);
    // The whole list as a stream.
    let all: Vec<_> = locks.entries().try_collect().await.unwrap();
    assert_eq!(all.len(), 3);
    // Release compares the holder; force release does not.
    assert!(!locks.release(&digest(1), &short).await.unwrap());
    assert!(locks.release(&digest(1), &long).await.unwrap());
    assert!(!locks.release(&digest(1), &long).await.unwrap());
    assert!(locks.force_release(&digest(2)).await.unwrap());
    assert!(!locks.force_release(&digest(2)).await.unwrap());
    assert_eq!(locks.size().await.unwrap(), 1);
}

#[tokio::test]
async fn release_all_trim_and_stale_entries() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let api = Api::new(pool.clone());
    let locks = api.unique_locks();
    for user in 0..150 {
        Sync::perform_async(&client, (user, "x".into()))
            .await
            .unwrap();
    }
    let mut other = JobPayload::new("api::Other", vec![json!(1)]);
    other.unique = Some(Unique::new(Duration::from_secs(60)).into());
    client.push(other).await.unwrap().unwrap();
    assert_eq!(locks.release_all(Some(Sync::NAME)).await.unwrap(), 150);
    assert_eq!(
        locks.size().await.unwrap(),
        1,
        "the other class is left alone"
    );
    assert_eq!(locks.release_all(None).await.unwrap(), 1);
    assert_eq!(zcard(&pool, keys::UNIQUE_LOCKS).await, 0);
    // Stale entries: an expired one, and a live-scored one whose lock is gone.
    Sync::perform_async(&client, (1, "x".into())).await.unwrap();
    let () = cmd(
        &pool,
        &redis_cmd!("ZADD", &keys::UNIQUE_LOCKS, &1, &"expired"),
    )
    .await;
    let () = cmd(
        &pool,
        &redis_cmd!("ZADD", &keys::UNIQUE_LOCKS, &9.0e10, &"vanished"),
    )
    .await;
    // Pages skip the entry whose lock is gone (and remove it).
    let page = locks.page(1, 25, SortOrder::Ascending).await.unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.total, 2, "counted until removed");
    let () = cmd(
        &pool,
        &redis_cmd!("ZADD", &keys::UNIQUE_LOCKS, &9.0e10, &"vanished"),
    )
    .await;
    assert_eq!(locks.trim().await.unwrap(), 2);
    assert_eq!(zcard(&pool, keys::UNIQUE_LOCKS).await, 1);
    assert_eq!(locks.trim().await.unwrap(), 0);
}

#[tokio::test]
async fn events_count_duplicates_per_class() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let api = Api::new(pool);
    Sync::perform_async(&client, (1, "x".into())).await.unwrap();
    for _ in 0..3 {
        Sync::perform_async(&client, (1, "y".into())).await.unwrap();
    }
    let events = api
        .unique_locks()
        .events(Period::Minutes(60))
        .await
        .unwrap();
    assert_eq!(events.totals.duplicates, 3);
    assert_eq!(events.by_class[Sync::NAME].duplicates, 3);
    assert_eq!(events.totals.lapsed, 0);
    assert!(matches!(
        api.unique_locks().events(Period::Hours(24)).await,
        Err(hench::Error::Config(_))
    ));
}

#[tokio::test]
async fn job_records_show_their_uniqueness() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let api = Api::new(pool);
    let jid = scheduled(&client, 1).await;
    let entry = api.scheduled().find_job(&jid).await.unwrap().unwrap();
    let unique = entry.unique().unwrap();
    assert_eq!(unique.digest, digest(1));
    assert_eq!(unique.until, UniqueUntil::Success);
    assert_eq!(unique.unique_for, Duration::from_secs(600));
    // Lenient: malformed or absent fields mean "not unique".
    for value in [
        json!({"class": "A", "unique": false}),
        json!({"class": "A", "unique": {"for": 60}}),
        json!({"class": "A", "unique": {"for": 60, "digest": "bad digest"}}),
        json!({"class": "A", "unique": "yes"}),
        json!({"class": "A"}),
    ] {
        let record = hench::api::JobRecord::new(&api, value.to_string(), None);
        assert_eq!(record.unique(), None, "{value}");
    }
    let payload = entry.payload().unwrap();
    assert!(matches!(payload.unique, Some(UniqueOption::Enabled(_))));
}

#[tokio::test]
async fn locate_finds_the_holder_wherever_it_waits() {
    // W2's lookups: running, queued (with its place in line), scheduled (at the time the lock
    // recorded, or by a scan), retrying, dead, or nowhere.
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let api = Api::new(pool.clone());
    let locks = api.unique_locks();
    let lock_of = |user: i64| {
        let locks = locks.clone();
        async move { locks.get(&digest(user)).await.unwrap().expect("a lock") }
    };
    // Queued, second in line.
    Sync::perform_async(&client, (1, "x".into()))
        .await
        .unwrap()
        .unwrap();
    let queued = Sync::perform_async(&client, (2, "x".into()))
        .await
        .unwrap()
        .unwrap();
    let lock = lock_of(2).await;
    assert_eq!(lock.fields["jid"], queued);
    assert_eq!(lock.fields["class"], Sync::NAME);
    assert_eq!(lock.fields["until"], "success");
    match locks.locate(&lock).await.unwrap() {
        HolderPlace::Queued { record, position } => {
            assert_eq!(record.jid(), Some(queued.as_str()));
            assert_eq!(position, 2);
        }
        other => panic!("not queued: {other:?}"),
    }
    // Scheduled: found at the time the lock recorded, and by a scan without it.
    let jid = scheduled(&client, 3).await;
    let lock = lock_of(3).await;
    assert!(lock.fields.contains_key("at"));
    assert!(matches!(
        locks.locate(&lock).await.unwrap(),
        HolderPlace::Scheduled(entry) if entry.jid() == Some(jid.as_str())
    ));
    let mut unrecorded = lock.clone();
    unrecorded.fields.remove("at");
    assert!(matches!(
        locks.locate(&unrecorded).await.unwrap(),
        HolderPlace::Scheduled(entry) if entry.jid() == Some(jid.as_str())
    ));
    // Retrying, then dead.
    to_retry_set(&pool, &jid).await;
    assert!(matches!(
        locks.locate(&lock).await.unwrap(),
        HolderPlace::Retrying(entry) if entry.jid() == Some(jid.as_str())
    ));
    move_entry(&pool, keys::RETRY, keys::DEAD, &jid).await;
    assert!(matches!(
        locks.locate(&lock).await.unwrap(),
        HolderPlace::Dead(entry) if entry.jid() == Some(jid.as_str())
    ));
    // Running: in a process's work hash.
    let job = queue_jobs(&pool, "sync")
        .await
        .into_iter()
        .find(|job| job.jid == queued)
        .unwrap();
    let () = cmd(&pool, &redis_cmd!("SADD", &keys::PROCESSES, &"host:1:abc")).await;
    let work = json!({"queue": "sync", "payload": job.to_json().unwrap(), "run_at": 1.7e9});
    let () = cmd(
        &pool,
        &redis_cmd!(
            "HSET",
            &keys::work("host:1:abc"),
            &"slot-1",
            &work.to_string()
        ),
    )
    .await;
    match locks.locate(&lock_of(2).await).await.unwrap() {
        HolderPlace::Running(work) => assert_eq!(work.process_id(), "host:1:abc"),
        other => panic!("not running: {other:?}"),
    }
    // Nowhere: a lock whose holder is gone (an `expired` lock after its job ran).
    let () = cmd(&pool, &redis_cmd!("DEL", &keys::work("host:1:abc"))).await;
    let () = cmd(&pool, &redis_cmd!("DEL", &keys::queue("sync"))).await;
    assert!(matches!(
        locks.locate(&lock_of(2).await).await.unwrap(),
        HolderPlace::NotFound { complete: true }
    ));
}

#[tokio::test]
async fn release_each_releases_the_holders_shown() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let api = Api::new(pool.clone());
    let mut jids = Vec::new();
    for user in 1..=3 {
        jids.push(
            Sync::perform_async(&client, (user, "x".into()))
                .await
                .unwrap()
                .unwrap(),
        );
    }
    let (one, two, three) = (digest(1), digest(2), digest(3));
    let released = api
        .unique_locks()
        .release_each(&[
            (one.as_str(), jids[0].as_str()),
            // Changed hands: left alone.
            (two.as_str(), "0123456789abcdef01234567"),
            (three.as_str(), jids[2].as_str()),
            // Skipped: not a digest, no jid.
            ("not a digest", jids[1].as_str()),
            (two.as_str(), ""),
        ])
        .await
        .unwrap();
    assert_eq!(released, 2);
    assert_eq!(holder(&pool, 1).await, None);
    assert_eq!(holder(&pool, 2).await, Some(jids[1].clone()));
    assert_eq!(holder(&pool, 3).await, None);
    assert_eq!(zcard(&pool, keys::UNIQUE_LOCKS).await, 1);
    // Again: nothing more.
    assert_eq!(
        api.unique_locks()
            .release_each(&[(one.as_str(), jids[0].as_str())])
            .await
            .unwrap(),
        0
    );
}

/// Round 3 (web-api-security-3): a lock whose times no clock can show (`at` written by a push
/// of a far-future job, before pushes refused them) is read without them; reading it used to
/// panic the Locks pages, the JSON API and `hench locks show`.
#[tokio::test]
async fn locks_with_times_out_of_range_are_read() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let api = Api::new(pool.clone());
    for (digest, at) in [
        ("far-1e20", "1e20"),
        ("far-max", "1.8446744073709552e19"),
        ("far-1e19", "1e19"),
        ("far-inf", "inf"),
        ("year-33658", "1e12"),
    ] {
        let _: () = cmd(
            &pool,
            &redis_cmd!(
                "HSET",
                keys::unique_lock(digest),
                "jid",
                "0123456789abcdef01234567",
                "class",
                "Far",
                "queue",
                "default",
                "until",
                "success",
                "for",
                "60",
                "t",
                at,
                "at",
                at
            ),
        )
        .await;
        let lock = api.unique_locks().get(digest).await.unwrap().unwrap();
        let readable = digest == "year-33658";
        assert_eq!(lock.scheduled_at.is_some(), readable, "{digest}: {lock:?}");
        assert_eq!(lock.fields.get("at").map(String::as_str), Some(at));
    }
}
