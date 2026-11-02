//! Unique jobs, push side (`docs/design/unique.md` §3.2, P1-P14, and D8-D11 through real
//! pushes): the lock taken with the job, duplicates, resends, conflicts, bulk pushes, client
//! middleware, the switch, out of memory, keys of the wrong type, the index trim and the
//! per-minute counters. Against a private `redis-server`.
#![cfg(feature = "test-util")]

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use hench::prelude::*;
use hench::test_util::TestRedis;
use hench::{Error, PushOutcome, UniqueOption, keys};
use serde_json::{Value, json};
use support::*;

/// A unique job (`success`, ten minutes), duplicates by its first argument.
#[derive(Default)]
struct Sync;

impl Job for Sync {
    const NAME: &'static str = "Sync";
    type Args = (i64, String);

    fn options() -> JobOptions {
        JobOptions::new()
            .queue("sync")
            .unique_for(Duration::from_secs(600))
            .unique_until(UniqueUntil::Start)
    }

    fn unique_key((user, _reason): &Self::Args) -> Option<Value> {
        Some(json!([user]))
    }

    async fn perform(&self, _ctx: &JobContext, _args: Self::Args) -> Result<(), JobError> {
        Ok(())
    }
}

/// A plain job.
#[derive(Default)]
struct Plain;

impl Job for Plain {
    const NAME: &'static str = "Plain";
    type Args = (i64,);

    async fn perform(&self, _ctx: &JobContext, _args: Self::Args) -> Result<(), JobError> {
        Ok(())
    }
}

/// A raw unique payload of class `class` with `args`, for `ttl` seconds.
fn unique_payload(class: &str, args: Vec<Value>, ttl: u64) -> JobPayload {
    let mut job = JobPayload::new(class, args);
    job.unique = Some(Unique::new(Duration::from_secs(ttl)).into());
    job
}

/// The digest of a lock.
fn digest_of(class: &str, queue: Option<&str>, key: Value) -> String {
    hench::unique::digest(class, queue, &key).unwrap()
}

async fn holder(pool: &RedisPool, digest: &str) -> Option<String> {
    cmd(
        pool,
        &redis_cmd!("HGET", &keys::unique_lock(digest), &"jid"),
    )
    .await
}

async fn index_score(pool: &RedisPool, digest: &str) -> Option<f64> {
    cmd(pool, &redis_cmd!("ZSCORE", &keys::UNIQUE_LOCKS, &digest)).await
}

/// The per-minute counter hashes of unique jobs in this test's private Redis.
async fn event_keys(pool: &RedisPool) -> Vec<String> {
    cmd(pool, &redis_cmd!("KEYS", &"u|*")).await
}

/// The counters of unique jobs, summed over every minute: a test may run across the turn of a
/// minute, which splits its counts over two hashes.
async fn events(pool: &RedisPool) -> std::collections::HashMap<String, String> {
    let mut totals: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    for key in event_keys(pool).await {
        for (field, count) in hgetall(pool, &key).await {
            *totals.entry(field).or_default() += count.parse::<i64>().unwrap_or(0);
        }
    }
    totals
        .into_iter()
        .map(|(field, count)| (field, count.to_string()))
        .collect()
}

#[tokio::test]
async fn the_first_push_takes_the_lock_and_duplicates_are_dropped() {
    // P1, P2, D11.
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let jid = client
        .push(unique_payload("Mail", vec![json!(42)], 600))
        .await
        .unwrap()
        .expect("pushed");
    let digest = digest_of("Mail", Some("default"), json!([42]));
    let lock = hgetall(&pool, &keys::unique_lock(&digest)).await;
    assert_eq!(lock["jid"], jid);
    assert_eq!(lock["class"], "Mail");
    assert_eq!(lock["queue"], "default");
    assert_eq!(lock["until"], "success");
    assert_eq!(lock["for"], "600");
    assert!(lock.contains_key("t"));
    assert!(!lock.contains_key("at"));
    let ttl = pttl(&pool, &keys::unique_lock(&digest)).await;
    assert!(ttl > 595_000 && ttl <= 600_000, "{ttl}");
    let expires = index_score(&pool, &digest).await.expect("indexed");
    let now = hench::now_secs_f64();
    assert!((expires - (now + 600.0)).abs() < 5.0, "{expires} vs {now}");
    // The stored job carries for, until and the digest only.
    let stored = queue_jobs(&pool, "default").await;
    assert_eq!(stored.len(), 1);
    let raw: Value =
        serde_json::from_slice(&lrange(&pool, &keys::queue("default")).await[0]).unwrap();
    assert_eq!(
        raw["unique"],
        json!({"for": 600, "until": "success", "digest": digest})
    );

    // The same push: a duplicate, not pushed.
    assert_eq!(
        client
            .push(unique_payload("Mail", vec![json!(42)], 600))
            .await
            .unwrap(),
        None
    );
    let outcome = client
        .push_outcome(unique_payload("Mail", vec![json!(42)], 600))
        .await
        .unwrap();
    let PushOutcome::Duplicate { holder: by, .. } = outcome else {
        panic!("not a duplicate: {outcome:?}");
    };
    assert_eq!(by, jid);
    assert_eq!(queue_size(&pool, "default").await, 1);
    // Other arguments, another queue: other locks.
    assert!(
        client
            .push(unique_payload("Mail", vec![json!(43)], 600))
            .await
            .unwrap()
            .is_some()
    );
    let mut elsewhere = unique_payload("Mail", vec![json!(42)], 600);
    elsewhere.queue = "other".to_owned();
    assert!(client.push(elsewhere).await.unwrap().is_some());
    // Across queues: one lock for every queue.
    let mut across = unique_payload("Mail", vec![json!(7)], 600);
    if let Some(UniqueOption::Enabled(unique)) = &mut across.unique {
        unique.across_queues = Some(true);
    }
    assert!(client.push(across.clone()).await.unwrap().is_some());
    across.queue = "other".to_owned();
    across.jid = String::new();
    assert_eq!(client.push(across).await.unwrap(), None);
    // `d` counted per class, kept 8 hours.
    let counted = events(&pool).await;
    assert_eq!(counted["Mail|d"], "3");
    let minutes = event_keys(&pool).await;
    assert!(!minutes.is_empty());
    for key in minutes {
        let ttl: i64 = cmd(&pool, &redis_cmd!("TTL", &key)).await;
        assert!(ttl > 28_000 && ttl <= 28_800, "{key}: {ttl}");
    }
}

#[tokio::test]
async fn typed_pushes_use_the_job_type_and_its_key() {
    // D5, D8, D9: the job type's options, Job::unique_key, per-push layers.
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let first = Sync::perform_async(&client, (42, "form".into()))
        .await
        .unwrap()
        .expect("pushed");
    // The same user, another reason: a duplicate.
    assert_eq!(
        Sync::perform_async(&client, (42, "retry".into()))
            .await
            .unwrap(),
        None
    );
    let digest = hench::unique::digest("Sync", Some("sync"), &json!([42])).unwrap();
    assert_eq!(
        holder(&pool, &digest).await.as_deref(),
        Some(first.as_str())
    );
    let lock = hgetall(&pool, &keys::unique_lock(&digest)).await;
    assert_eq!(lock["until"], "start");
    // A per-push `unique_for` keeps the job type's `until` (D8).
    let other = Sync::set()
        .unique_for(Duration::from_secs(30))
        .perform_async(&client, (7, "x".into()))
        .await
        .unwrap()
        .expect("pushed");
    let stored = queue_jobs(&pool, "sync").await;
    let job = stored.iter().find(|job| job.jid == other).unwrap();
    let unique = job
        .unique
        .as_ref()
        .and_then(UniqueOption::settings)
        .unwrap();
    assert_eq!(unique.ttl, Some(Duration::from_secs(30)));
    assert_eq!(unique.until, Some(UniqueUntil::Start));
    // `not_unique` bypasses (D9); an explicit key wins over Job::unique_key (D5).
    assert!(
        Sync::set()
            .not_unique()
            .perform_async(&client, (42, "bypass".into()))
            .await
            .unwrap()
            .is_some()
    );
    let keyed = Sync::set()
        .unique_key("user:42")
        .perform_async(&client, (42, "keyed".into()))
        .await
        .unwrap()
        .expect("another key");
    let keyed_digest = hench::unique::digest("Sync", Some("sync"), &json!("user:42")).unwrap();
    assert_eq!(holder(&pool, &keyed_digest).await, Some(keyed));
    // `push_outcome` says what happened.
    assert!(matches!(
        Sync::set()
            .push_outcome(&client, (42, "again".into()))
            .await
            .unwrap(),
        PushOutcome::Duplicate { .. }
    ));
    // A plain job type without uniqueness pushes as before.
    assert!(Plain::perform_async(&client, (1,)).await.unwrap().is_some());
    assert!(Plain::perform_async(&client, (1,)).await.unwrap().is_some());
}

#[tokio::test]
async fn scheduled_pushes_hold_the_lock_through_their_delay() {
    // P3.
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let mut job = unique_payload("Report", vec![json!("weekly")], 60);
    job.at = Some(hench::now_secs_f64() + 3_600.0);
    let jid = client.push(job).await.unwrap().expect("scheduled");
    let digest = hench::unique::digest("Report", Some("default"), &json!(["weekly"])).unwrap();
    let ttl = pttl(&pool, &keys::unique_lock(&digest)).await;
    assert!(ttl > 3_650_000 && ttl <= 3_660_000, "{ttl}");
    let (stored, score) = sorted_jobs(&pool, keys::SCHEDULE).await.remove(0);
    assert_eq!(stored.jid, jid);
    let at: f64 = cmd::<String>(
        &pool,
        &redis_cmd!("HGET", &keys::unique_lock(&digest), &"at"),
    )
    .await
    .parse()
    .unwrap();
    assert_eq!(at, score, "the lock records the job's score");
    // While it waits, an immediate push is a duplicate.
    assert_eq!(
        client
            .push(unique_payload("Report", vec![json!("weekly")], 60))
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn a_resent_push_is_not_pushed_twice() {
    // P4, G5: the same jid again (a lost reply retried): already pushed, one copy.
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let mut job = unique_payload("Mail", vec![json!(1)], 60);
    job.jid = "0123456789abcdef01234567".to_owned();
    let outcome = client.push_outcome(job.clone()).await.unwrap();
    assert_eq!(
        outcome,
        PushOutcome::Pushed {
            jid: job.jid.clone()
        }
    );
    let again = client.push_outcome(job.clone()).await.unwrap();
    assert_eq!(again, PushOutcome::Pushed { jid: job.jid });
    assert_eq!(queue_size(&pool, "default").await, 1);
    assert!(events(&pool).await.is_empty(), "no duplicate counted");
}

#[tokio::test]
async fn on_conflict_error_refuses_the_push() {
    // P6.
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let first = client
        .push(unique_payload("Mail", vec![json!(1)], 60))
        .await
        .unwrap()
        .expect("pushed");
    let mut job = unique_payload("Mail", vec![json!(1)], 60);
    if let Some(UniqueOption::Enabled(unique)) = &mut job.unique {
        unique.on_conflict = Some(OnConflict::Error);
    }
    let Err(Error::DuplicateJob {
        class,
        digest,
        holder,
    }) = client.push(job.clone()).await
    else {
        panic!("expected a duplicate-job error");
    };
    assert_eq!((class.as_str(), holder.as_str()), ("Mail", first.as_str()));
    assert_eq!(
        digest,
        hench::unique::digest("Mail", Some("default"), &json!([1])).unwrap()
    );
    // A typed push says the same through its setter.
    assert!(matches!(
        Sync::perform_async(&client, (5, "a".into())).await,
        Ok(Some(_))
    ));
    assert!(matches!(
        Sync::set()
            .on_conflict(OnConflict::Error)
            .perform_async(&client, (5, "b".into()))
            .await,
        Err(Error::DuplicateJob { .. })
    ));
}

#[tokio::test]
async fn replace_debounces_jobs_waiting_in_the_schedule() {
    // P7: a scheduled holder is replaced; queued, leased, retrying and batch-member holders
    // are not.
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let in_a_minute = || hench::now_secs_f64() + 60.0;
    let debounce = |doc: i64, version: i64| {
        let mut job = unique_payload("Reindex", vec![json!(doc), json!(version)], 30);
        if let Some(UniqueOption::Enabled(unique)) = &mut job.unique {
            unique.on_conflict = Some(OnConflict::Replace);
            unique.key = Some(json!(doc));
        }
        job.at = Some(in_a_minute());
        job
    };
    let first = client.push(debounce(1, 1)).await.unwrap().expect("pushed");
    let second = client.push_outcome(debounce(1, 2)).await.unwrap();
    let PushOutcome::Replaced { jid, replaced } = second else {
        panic!("not replaced: {second:?}");
    };
    assert_eq!(replaced, first);
    let scheduled = sorted_jobs(&pool, keys::SCHEDULE).await;
    assert_eq!(scheduled.len(), 1, "the holder is gone");
    assert_eq!(scheduled[0].0.jid, jid);
    assert_eq!(scheduled[0].0.args, [json!(1), json!(2)]);
    let digest = hench::unique::digest("Reindex", Some("default"), &json!(1)).unwrap();
    assert_eq!(holder(&pool, &digest).await, Some(jid.clone()));
    let ttl = pttl(&pool, &keys::unique_lock(&digest)).await;
    assert!(ttl > 85_000 && ttl <= 90_000, "{ttl}");
    assert_eq!(events(&pool).await["Reindex|r"], "1");

    // The scheduler leased the holder (its score moved ahead): a duplicate.
    let members: Vec<String> = cmd(&pool, &redis_cmd!("ZRANGE", &keys::SCHEDULE, &0, &0)).await;
    let member = &members[0];
    let leased = hench::now_secs_f64() + 3_600.0;
    let () = cmd(
        &pool,
        &redis_cmd!("ZADD", &keys::SCHEDULE, &"XX", &leased, member),
    )
    .await;
    assert!(matches!(
        client.push_outcome(debounce(1, 3)).await.unwrap(),
        PushOutcome::Duplicate { .. }
    ));

    // A queued holder is a duplicate too.
    let mut queued = debounce(2, 1);
    queued.at = None;
    client.push(queued).await.unwrap().expect("pushed");
    assert!(matches!(
        client.push_outcome(debounce(2, 2)).await.unwrap(),
        PushOutcome::Duplicate { .. }
    ));

    // A holder waiting in the retry set (no `at` recorded) is a duplicate.
    let retrying = client.push(debounce(3, 1)).await.unwrap().expect("pushed");
    let (job, score) = sorted_jobs(&pool, keys::SCHEDULE)
        .await
        .into_iter()
        .find(|(job, _)| job.jid == retrying)
        .unwrap();
    let raw = job.to_json().unwrap();
    let removed: i64 = cmd(&pool, &redis_cmd!("ZREM", &keys::SCHEDULE, &raw)).await;
    assert_eq!(removed, 1);
    let () = cmd(&pool, &redis_cmd!("ZADD", &keys::RETRY, &score, &raw)).await;
    let retry_digest = hench::unique::digest("Reindex", Some("default"), &json!(3)).unwrap();
    let _: i64 = cmd(
        &pool,
        &redis_cmd!("HDEL", &keys::unique_lock(&retry_digest), &"at"),
    )
    .await;
    assert!(matches!(
        client.push_outcome(debounce(3, 2)).await.unwrap(),
        PushOutcome::Duplicate { .. }
    ));

    // A holder that belongs to a batch: its batch would wait for it forever.
    let mut member = debounce(4, 1);
    member.bid = Some("89abcdef0123456789abcdef".to_owned());
    client.push(member).await.unwrap().expect("pushed");
    assert!(matches!(
        client.push_outcome(debounce(4, 2)).await.unwrap(),
        PushOutcome::Duplicate { .. }
    ));
}

/// A debounced reminder per user: the newest push replaces the one waiting in the schedule.
#[derive(Default)]
struct Remind;

impl Job for Remind {
    const NAME: &'static str = "Remind";
    type Args = (i64, String);

    fn options() -> JobOptions {
        JobOptions::new()
            .unique_for(Duration::from_secs(600))
            .on_conflict(OnConflict::Replace)
    }

    fn unique_key((user, _text): &Self::Args) -> Option<Value> {
        Some(json!(user))
    }

    async fn perform(&self, _ctx: &JobContext, _args: Self::Args) -> Result<(), JobError> {
        Ok(())
    }
}

/// The users whose reminder in the schedule still has the text `text`.
async fn scheduled_with(pool: &RedisPool, text: &str) -> Vec<i64> {
    let mut users: Vec<i64> = sorted_jobs(pool, keys::SCHEDULE)
        .await
        .into_iter()
        .filter(|(job, _)| job.class == Remind::NAME && job.args.get(1) == Some(&json!(text)))
        .filter_map(|(job, _)| job.args.first().and_then(Value::as_i64))
        .collect();
    users.sort_unstable();
    users
}

#[tokio::test]
async fn replace_finds_its_holder_among_many_jobs_at_one_score() {
    // unique-1: jobs at the same score are ordered by their JSON, so the holder of a lock is
    // not always among the first few members at its score; the lock records its member and a
    // debounce push finds it whatever the number of jobs sharing the score.
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    // A bulk push with one delay gives every job the same score.
    let first: Vec<(i64, String)> = (0..40).map(|user| (user, "v1".to_owned())).collect();
    let pushed = Remind::set()
        .delay(Duration::from_secs(60))
        .perform_bulk(&client, first)
        .await
        .unwrap();
    assert!(pushed.iter().all(Option::is_some));
    let scores: std::collections::HashSet<String> = sorted_jobs(&pool, keys::SCHEDULE)
        .await
        .iter()
        .map(|(_, score)| score.to_string())
        .collect();
    assert_eq!(scores.len(), 1, "one score for the whole bulk push");
    for user in 0..40 {
        let outcome = Remind::set()
            .delay(Duration::from_secs(60))
            .push_outcome(&client, (user, "v2".to_owned()))
            .await
            .unwrap();
        assert!(
            matches!(outcome, PushOutcome::Replaced { .. }),
            "user {user}: {outcome:?}"
        );
    }
    assert_eq!(scheduled_with(&pool, "v1").await, Vec::<i64>::new());
    assert_eq!(
        scheduled_with(&pool, "v2").await,
        (0..40).collect::<Vec<_>>()
    );
    assert_eq!(events(&pool).await["Remind|r"], "40");

    // A fixed run-at time (09:00 for every user), pushed one by one.
    let nine = std::time::UNIX_EPOCH + Duration::from_secs(4_102_477_200);
    for user in 100..130 {
        Remind::perform_at(&client, nine, (user, "w1".to_owned()))
            .await
            .unwrap()
            .expect("pushed");
    }
    for user in 100..130 {
        let outcome = Remind::set()
            .at(nine)
            .push_outcome(&client, (user, "w2".to_owned()))
            .await
            .unwrap();
        assert!(
            matches!(outcome, PushOutcome::Replaced { .. }),
            "user {user}: {outcome:?}"
        );
    }
    assert_eq!(scheduled_with(&pool, "w1").await, Vec::<i64>::new());

    // Jobs of another class at the holder's score, sorting before it.
    let holder = Remind::perform_at(&client, nine, (200, "x1".to_owned()))
        .await
        .unwrap()
        .expect("pushed");
    for n in 0..40 {
        let mut job = payload("A::Earlier", vec![json!(n)], "default");
        job.at = Some(4_102_477_200.0);
        client.push(job).await.unwrap().expect("pushed");
    }
    let outcome = Remind::set()
        .at(nine)
        .push_outcome(&client, (200, "x2".to_owned()))
        .await
        .unwrap();
    assert!(
        matches!(&outcome, PushOutcome::Replaced { replaced, .. } if *replaced == holder),
        "{outcome:?}"
    );
    let at_nine: usize = cmd(
        &pool,
        &redis_cmd!("ZCOUNT", &keys::SCHEDULE, &"4102477200", &"4102477200"),
    )
    .await;
    assert_eq!(at_nine, 30 + 1 + 40);

    // The lock records the holder's member; a holder that left the schedule (enqueued by the
    // scheduler) or was moved is not replaced.
    let digest = digest_of(Remind::NAME, Some("default"), json!(200));
    let lock = hgetall(&pool, &keys::unique_lock(&digest)).await;
    let member = lock.get("m").expect("the member is recorded").clone();
    let score: Option<f64> = cmd(&pool, &redis_cmd!("ZSCORE", &keys::SCHEDULE, &member)).await;
    assert_eq!(score, Some(4_102_477_200.0));
    let removed: i64 = cmd(&pool, &redis_cmd!("ZREM", &keys::SCHEDULE, &member)).await;
    assert_eq!(removed, 1);
    let () = cmd(&pool, &redis_cmd!("LPUSH", &"queue:default", &member)).await;
    assert!(matches!(
        Remind::set()
            .at(nine)
            .push_outcome(&client, (200, "x3".to_owned()))
            .await
            .unwrap(),
        PushOutcome::Duplicate { .. }
    ));
}

#[tokio::test]
async fn replace_outcomes_are_the_same_on_a_fake_client() {
    // unique-1: the testing mode and Redis agree, however many jobs share the holder's score;
    // neither replaces a batch member.
    async fn outcomes(client: &Client) -> Vec<&'static str> {
        let first: Vec<(i64, String)> = (0..40).map(|user| (user, "v1".to_owned())).collect();
        Remind::set()
            .delay(Duration::from_secs(60))
            .perform_bulk(client, first)
            .await
            .unwrap();
        let mut member = payload(Remind::NAME, vec![json!(99), json!("v1")], "default");
        member.unique = Some(
            Unique::new(Duration::from_secs(600))
                .key(json!(99))
                .on_conflict(OnConflict::Replace)
                .into(),
        );
        member.at = Some(hench::now_secs_f64() + 60.0);
        member.bid = Some("89abcdef0123456789abcdef".to_owned());
        client.push(member).await.unwrap().expect("pushed");
        let mut seen = Vec::new();
        for user in (0..40).chain([99]) {
            let outcome = Remind::set()
                .delay(Duration::from_secs(60))
                .push_outcome(client, (user, "v2".to_owned()))
                .await
                .unwrap();
            seen.push(match outcome {
                PushOutcome::Replaced { .. } => "replaced",
                PushOutcome::Duplicate { .. } => "duplicate",
                PushOutcome::Pushed { .. } => "pushed",
                _ => "other",
            });
        }
        seen
    }
    let redis = TestRedis::start().await;
    let in_redis = outcomes(&redis.client().await).await;
    let mut expected = vec!["replaced"; 40];
    expected.push("duplicate");
    assert_eq!(in_redis, expected);
    assert_eq!(outcomes(&Client::fake()).await, expected);
}

#[tokio::test]
async fn replace_looks_for_a_holder_without_a_recorded_member_at_its_score() {
    // A lock that does not record its holder's member (written by an older version): the
    // holder is looked for among the first members at its score, as text before decoding.
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let nine = std::time::UNIX_EPOCH + Duration::from_secs(4_102_477_200);
    for user in 0..20 {
        Remind::perform_at(&client, nine, (user, "v1".to_owned()))
            .await
            .unwrap()
            .expect("pushed");
        let digest = digest_of(Remind::NAME, Some("default"), json!(user));
        let _: i64 = cmd(
            &pool,
            &redis_cmd!("HDEL", &keys::unique_lock(&digest), &"m"),
        )
        .await;
    }
    for user in 0..20 {
        let outcome = Remind::set()
            .at(nine)
            .push_outcome(&client, (user, "v2".to_owned()))
            .await
            .unwrap();
        assert!(
            matches!(outcome, PushOutcome::Replaced { .. }),
            "user {user}: {outcome:?}"
        );
    }
    assert_eq!(scheduled_with(&pool, "v1").await, Vec::<i64>::new());
}

#[tokio::test]
async fn bulk_pushes_take_one_lock_per_digest() {
    // P5: duplicates within one push and across pushes; `Error` is `None` in bulk pushes; more
    // than 100 unique jobs span several script runs.
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let bulk = BulkPush::from_template(
        unique_payload("Mail", Vec::new(), 600),
        vec![vec![json!(1)], vec![json!(2)], vec![json!(1)]],
    );
    let jids = client.push_bulk(bulk).await.unwrap();
    assert!(jids[0].is_some() && jids[1].is_some());
    assert_eq!(jids[2], None, "a duplicate of the first job of the push");
    let mut template = unique_payload("Mail", Vec::new(), 600);
    if let Some(UniqueOption::Enabled(unique)) = &mut template.unique {
        unique.on_conflict = Some(OnConflict::Error);
    }
    let outcomes = client
        .push_bulk_outcomes(BulkPush::from_template(
            template,
            vec![vec![json!(2)], vec![json!(3)]],
        ))
        .await
        .unwrap();
    assert!(matches!(outcomes[0], PushOutcome::Duplicate { .. }));
    assert!(matches!(outcomes[1], PushOutcome::Pushed { .. }));
    // 250 jobs, 25 of them duplicates of the others, in chunks of 100.
    let args: Vec<Vec<Value>> = (0..250).map(|n| vec![json!(1_000 + n % 225)]).collect();
    let jids = client
        .push_bulk(BulkPush::from_template(
            unique_payload("Bulk", Vec::new(), 600),
            args,
        ))
        .await
        .unwrap();
    assert_eq!(jids.iter().filter(|jid| jid.is_some()).count(), 225);
    assert_eq!(jids[225..].iter().filter(|jid| jid.is_none()).count(), 25);
    assert_eq!(queue_size(&pool, "default").await, 3 + 225);
    let locks: u64 = cmd(&pool, &redis_cmd!("ZCARD", &keys::UNIQUE_LOCKS)).await;
    assert_eq!(locks, 3 + 225);
    // A typed bulk push computes each job's key.
    let jids = Sync::set()
        .perform_bulk(
            &client,
            vec![(9, "a".into()), (9, "b".into()), (10, "c".into())],
        )
        .await
        .unwrap();
    assert_eq!(jids.iter().filter(|jid| jid.is_some()).count(), 2);
    assert_eq!(jids[1], None);
}

/// Client middleware that does one thing to every job.
struct Middleware(fn(&mut JobPayload) -> Option<()>, Arc<AtomicUsize>);

#[async_trait]
impl ClientMiddleware for Middleware {
    async fn call(
        &self,
        mut job: JobPayload,
        _redis: &RedisPool,
        next: ClientNext<'_>,
    ) -> hench::Result<Option<JobPayload>> {
        self.1.fetch_add(1, Ordering::SeqCst);
        match (self.0)(&mut job) {
            Some(()) => next.run(job).await,
            None => Ok(None),
        }
    }
}

#[tokio::test]
async fn client_middleware_runs_before_the_lock_is_taken() {
    // P8, P9.
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let stopping = client.with_middleware(|chain| {
        chain.add(Middleware(|_| None, calls.clone()));
    });
    assert_eq!(
        stopping
            .push_outcome(unique_payload("Mail", vec![json!(1)], 60))
            .await
            .unwrap(),
        PushOutcome::Stopped
    );
    let digest = hench::unique::digest("Mail", Some("default"), &json!([1])).unwrap();
    assert_eq!(
        holder(&pool, &digest).await,
        None,
        "a stopped push takes no lock"
    );
    // Middleware rewriting the arguments (say, encrypting them) keeps the digest.
    let rewriting = client.with_middleware(|chain| {
        chain.add(Middleware(
            |job| {
                job.args = vec![json!("ciphertext")];
                Some(())
            },
            calls.clone(),
        ));
    });
    let jid = rewriting
        .push(unique_payload("Mail", vec![json!(1)], 60))
        .await
        .unwrap()
        .expect("pushed");
    assert_eq!(holder(&pool, &digest).await, Some(jid));
    assert_eq!(
        rewriting
            .push(unique_payload("Mail", vec![json!(1)], 60))
            .await
            .unwrap(),
        None
    );
    // Middleware removing uniqueness: pushed without a lock.
    let removing = client.with_middleware(|chain| {
        chain.add(Middleware(
            |job| {
                job.unique = None;
                Some(())
            },
            calls.clone(),
        ));
    });
    assert!(
        removing
            .push(unique_payload("Mail", vec![json!(1)], 60))
            .await
            .unwrap()
            .is_some()
    );
    // Middleware replacing the digest (a tenant's lock space): honoured, and checked.
    let tenant = client.with_middleware(|chain| {
        chain.add(Middleware(
            |job| {
                if let Some(UniqueOption::Enabled(unique)) = &mut job.unique {
                    unique.digest = Some("tenant-1-mail".to_owned());
                }
                Some(())
            },
            calls.clone(),
        ));
    });
    let jid = tenant
        .push(unique_payload("Mail", vec![json!(1)], 60))
        .await
        .unwrap()
        .expect("another lock");
    assert_eq!(holder(&pool, "tenant-1-mail").await, Some(jid));
    let invalid = client.with_middleware(|chain| {
        chain.add(Middleware(
            |job| {
                if let Some(UniqueOption::Enabled(unique)) = &mut job.unique {
                    unique.digest = Some("not a digest".to_owned());
                }
                Some(())
            },
            calls.clone(),
        ));
    });
    assert!(matches!(
        invalid
            .push(unique_payload("Mail", vec![json!(2)], 60))
            .await,
        Err(Error::InvalidJob(_))
    ));
    assert!(calls.load(Ordering::SeqCst) >= 6);
}

#[tokio::test]
async fn the_switch_turns_uniqueness_off_for_new_pushes() {
    // P10.
    let redis = TestRedis::start().await;
    let client = redis.client().await.with_unique_jobs(false);
    let pool = redis.pool().await;
    for _ in 0..2 {
        assert!(
            client
                .push(unique_payload("Mail", vec![json!(1)], 60))
                .await
                .unwrap()
                .is_some()
        );
    }
    let stored = lrange(&pool, &keys::queue("default")).await;
    assert_eq!(stored.len(), 2);
    for raw in stored {
        let job: Value = serde_json::from_slice(&raw).unwrap();
        assert!(job.get("unique").is_none(), "{job}");
    }
    let locks: u64 = cmd(&pool, &redis_cmd!("ZCARD", &keys::UNIQUE_LOCKS)).await;
    assert_eq!(locks, 0);
}

#[tokio::test]
async fn invalid_unique_settings_are_refused_before_anything_is_written() {
    // D10.
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let mut no_ttl = JobPayload::new("Mail", vec![json!(1)]);
    no_ttl.unique = Some(Unique::default().until(UniqueUntil::Start).into());
    assert!(matches!(
        client.push(no_ttl).await,
        Err(Error::InvalidJob(message)) if message.contains("unique_for")
    ));
    for ttl in [Duration::ZERO, Duration::from_secs(1_000_000_000)] {
        let mut job = JobPayload::new("Mail", vec![json!(1)]);
        job.unique = Some(Unique::new(ttl).into());
        assert!(matches!(client.push(job).await, Err(Error::InvalidJob(_))));
    }
    assert_eq!(queue_size(&pool, "default").await, 0);
}

#[tokio::test]
async fn out_of_memory_refuses_the_push_without_a_lock() {
    // P11.
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let () = cmd(&pool, &redis_cmd!("CONFIG", &"SET", &"maxmemory", &"1")).await;
    let mut job = unique_payload("Mail", vec![json!(1)], 60);
    job.jid = "0123456789abcdef01234567".to_owned();
    let error = client.push(job.clone()).await.unwrap_err();
    let Error::Redis(redis_error) = &error else {
        panic!("not a Redis error: {error}");
    };
    assert_eq!(redis_error.code(), Some("OOM"), "{error}");
    let () = cmd(&pool, &redis_cmd!("CONFIG", &"SET", &"maxmemory", &"0")).await;
    let digest = hench::unique::digest("Mail", Some("default"), &json!([1])).unwrap();
    assert_eq!(holder(&pool, &digest).await, None);
    assert_eq!(queue_size(&pool, "default").await, 0);
    // Once memory is available, the same push goes through (not "already pushed").
    assert_eq!(
        client.push_outcome(job.clone()).await.unwrap(),
        PushOutcome::Pushed {
            jid: job.jid.clone()
        }
    );
    assert_eq!(queue_size(&pool, "default").await, 1);
    assert_eq!(holder(&pool, &digest).await, Some(job.jid));
}

#[tokio::test]
async fn a_lock_key_of_the_wrong_type_fails_that_job_only() {
    // P12.
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let digest = hench::unique::digest("Mail", Some("default"), &json!([2])).unwrap();
    let () = cmd(&pool, &redis_cmd!("SET", &keys::unique_lock(&digest), &"x")).await;
    let bulk = BulkPush::from_template(
        unique_payload("Mail", Vec::new(), 60),
        vec![vec![json!(1)], vec![json!(2)], vec![json!(3)]],
    );
    let error = client.push_bulk(bulk).await.unwrap_err();
    let Error::Redis(redis_error) = &error else {
        panic!("not a Redis error: {error}");
    };
    assert_eq!(redis_error.code(), Some("WRONGTYPE"), "{error}");
    // The other jobs of the push were written with their locks.
    let pushed: Vec<Value> = queue_jobs(&pool, "default")
        .await
        .into_iter()
        .map(|job| job.args[0].clone())
        .collect();
    assert_eq!(pushed, [json!(1), json!(3)]);
}

#[tokio::test]
async fn pushes_trim_at_most_ten_expired_index_entries() {
    // P13.
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    for n in 0..25 {
        let () = cmd(
            &pool,
            &redis_cmd!(
                "ZADD",
                &keys::UNIQUE_LOCKS,
                &(1.0 + f64::from(n)),
                &format!("old{n}")
            ),
        )
        .await;
    }
    client
        .push(unique_payload("Mail", vec![json!(1)], 60))
        .await
        .unwrap();
    assert_eq!(zcard(&pool, keys::UNIQUE_LOCKS).await, 25 - 10 + 1);
    client
        .push(unique_payload("Mail", vec![json!(2)], 60))
        .await
        .unwrap();
    assert_eq!(zcard(&pool, keys::UNIQUE_LOCKS).await, 25 - 20 + 2);
    // Plain pushes leave the index alone.
    Plain::perform_async(&client, (1,)).await.unwrap();
    assert_eq!(zcard(&pool, keys::UNIQUE_LOCKS).await, 7);
}

#[tokio::test]
async fn jobs_without_uniqueness_keep_the_plain_push() {
    // A batch of plain jobs never runs the unique script (the MULTI of before, byte for byte,
    // is pinned by tests/featureless.rs); a mixed batch writes the plain jobs too.
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let mut plain = JobPayload::new("Plain", vec![json!(1)]);
    plain.jid = "aaaaaaaaaaaaaaaaaaaaaaaa".to_owned();
    let unique = unique_payload("Mail", vec![json!(1)], 60);
    client.push(plain.clone()).await.unwrap();
    client.push(unique).await.unwrap();
    let mut scheduled = JobPayload::new("Plain", vec![json!(2)]);
    scheduled.at = Some(hench::now_secs_f64() + 100.0);
    client.push(scheduled).await.unwrap();
    assert_eq!(queue_size(&pool, "default").await, 2);
    assert_eq!(zcard(&pool, keys::SCHEDULE).await, 1);
    let stored = queue_jobs(&pool, "default").await;
    assert!(stored[0].unique.is_none() && stored[0].enqueued_at.is_some());
}
