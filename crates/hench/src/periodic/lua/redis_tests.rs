//! The periodic scripts against a private Redis (`docs/design/periodic.md` Appendix B, ported):
//! each script's results and writes, resends, compare-and-sets, out of memory, keys of the wrong
//! type, and the lock step of unique jobs inside `PERIODIC_FIRE` and `PERIODIC_ENQUEUE_NOW`.

use std::collections::HashMap;
use std::time::Duration;

use crate::keys;
use crate::periodic::Periodic;
use crate::periodic::definition::Definition;
use crate::periodic::store::{
    self, EnqueueNow, Enqueued, Fire, Fired, Lock, SyncEntry, Synced, UpsertReply, Write,
};
use crate::redis::RedisPool;
use crate::test_util::TestRedis;
use crate::unique::push::PushLock;
use crate::unique::{OnConflict, UniqueUntil};

const JID: &str = "aaaaaaaaaaaaaaaaaaaaaaaa";
const OTHER: &str = "bbbbbbbbbbbbbbbbbbbbbbbb";
const EVENTS: &str = "u|20261101|9:30";

async fn command<T: ::redis::FromRedisValue>(pool: &RedisPool, command: &::redis::Cmd) -> T {
    let mut conn = pool.conn().await.unwrap();
    command.query_async(&mut conn).await.unwrap()
}

async fn hash(pool: &RedisPool, name: &str) -> HashMap<String, String> {
    command(pool, ::redis::cmd("HGETALL").arg(keys::periodic(name))).await
}

async fn score(pool: &RedisPool, name: &str) -> Option<f64> {
    command(pool, ::redis::cmd("ZSCORE").arg(keys::PERIODIC).arg(name)).await
}

async fn len(pool: &RedisPool, key: &str) -> usize {
    command(pool, ::redis::cmd("LLEN").arg(key)).await
}

/// An interval schedule of class `Tick` on queue `q`.
fn every(name: &str, seconds: u64) -> Definition {
    Periodic::every(name, Duration::from_secs(seconds))
        .class("Tick", vec![])
        .queue("q")
        .definition()
        .unwrap()
}

/// Creates `definition` (source API) with `next`.
async fn create(pool: &RedisPool, definition: &Definition, next: i64) {
    let reply = store::upsert(pool, definition, "", "api", next, next, 1_000)
        .await
        .unwrap();
    assert_eq!(reply, UpsertReply::Done(1));
}

fn queued(json: &str) -> Write {
    Write::Enqueued {
        queue: "q".to_owned(),
        json: json.to_owned(),
    }
}

/// A `FIRE` of tick `tick` expected at `next`, advancing to `next + 60`.
fn fire<'a>(definition: &'a Definition, expected: &'a str, tick: i64, write: Write) -> Fire<'a> {
    Fire {
        name: &definition.name,
        expected_next: expected,
        digest: &definition.digest,
        new_next: Some(tick + 60),
        tick: Some(tick),
        write,
        lock: None,
        missed: 0,
        entry: format!(r#"{{"tick":{tick},"at":1,"result":"enqueued"}}"#),
        duplicate_entry: format!(r#"{{"tick":{tick},"at":1,"result":"duplicate"}}"#),
        history: 10,
        jid: JID,
        identity: "host:1:abc",
        now_ms: tick * 1_000 + 5,
    }
}

fn lock(digest: &str, seconds: u64) -> Lock {
    Lock {
        lock: PushLock {
            key: keys::unique_lock(digest),
            digest: digest.to_owned(),
            until: UniqueUntil::Success,
            for_ms: seconds * 1_000,
            on_conflict: OnConflict::Drop,
        },
        class: "Tick".to_owned(),
        queue: "q".to_owned(),
    }
}

#[tokio::test]
async fn lease_takes_the_earliest_due_schedule_once() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let lease = Duration::from_secs(2);
    let empty = store::lease(&pool, lease, Some(250_000), &[])
        .await
        .unwrap();
    assert_eq!(
        (empty.now_ms, empty.due, empty.earliest),
        (250_000, None, None)
    );
    for (name, at) in [("a", 100), ("b", 200), ("c", 5_000)] {
        let _: () = command(
            &pool,
            ::redis::cmd("ZADD").arg(keys::PERIODIC).arg(at).arg(name),
        )
        .await;
    }
    let first = store::lease(&pool, lease, Some(250_000), &[])
        .await
        .unwrap();
    assert_eq!(first.due, Some(("a".to_owned(), 100.0)));
    assert_eq!(score(&pool, "a").await, Some(252.0));
    let second = store::lease(&pool, lease, Some(250_000), &[])
        .await
        .unwrap();
    assert_eq!(second.due, Some(("b".to_owned(), 200.0)));
    // Nothing due any more: the earliest score says when to look again.
    let none = store::lease(&pool, lease, Some(250_000), &[])
        .await
        .unwrap();
    assert_eq!((none.due, none.earliest), (None, Some(252.0)));
    // A skipped name is left alone: the next one is leased, and the earliest not skipped is
    // reported.
    let _: () = command(
        &pool,
        ::redis::cmd("ZADD").arg(keys::PERIODIC).arg(10).arg("a"),
    )
    .await;
    let _: () = command(
        &pool,
        ::redis::cmd("ZADD").arg(keys::PERIODIC).arg(20).arg("b"),
    )
    .await;
    let skipping = store::lease(&pool, lease, Some(250_000), &["a".to_owned()])
        .await
        .unwrap();
    assert_eq!(skipping.due, Some(("b".to_owned(), 20.0)));
    let idle = store::lease(&pool, lease, Some(250_000), &["a".to_owned()])
        .await
        .unwrap();
    assert_eq!((idle.due, idle.earliest), (None, Some(252.0)));
    assert_eq!(
        score(&pool, "a").await,
        Some(10.0),
        "a skipped name keeps its score"
    );
    // Without an override, Redis's clock.
    let now = store::lease(&pool, lease, None, &[]).await.unwrap();
    assert!((now.now_ms - crate::payload::now_millis()).abs() < 5_000);
    assert_eq!(now.due.map(|(name, _)| name), Some("a".to_owned()));
}

#[tokio::test]
async fn fire_enqueues_a_tick_once_and_advances_the_schedule() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let s = every("s", 60);
    create(&pool, &s, 960).await;
    let job = r#"{"class":"Tick","args":[],"queue":"q","jid":"aaaaaaaaaaaaaaaaaaaaaaaa"}"#;
    let fired = store::fire(&pool, &fire(&s, "960", 960, queued(job)), EVENTS)
        .await
        .unwrap();
    assert_eq!(fired, Fired::Done { without_lock: None });
    assert_eq!(len(&pool, "queue:q").await, 1);
    let queues: Vec<String> = command(&pool, ::redis::cmd("SMEMBERS").arg(keys::QUEUES)).await;
    assert_eq!(queues, ["q"]);
    let fields = hash(&pool, "s").await;
    assert_eq!(fields["next"], "1020");
    assert_eq!(fields["last_tick"], "960");
    assert_eq!(fields["last_jid"], JID);
    assert_eq!(fields["last_by"], "host:1:abc");
    assert_eq!(fields["last_result"], "enqueued");
    assert_eq!(fields["last_at"], "960005");
    assert_eq!(fields["fired"], "1");
    assert_eq!(score(&pool, "s").await, Some(1020.0));
    assert_eq!(len(&pool, &keys::periodic_history("s")).await, 1);
    // A resend (the reply was lost): `next` moved on, nothing more is written.
    let again = store::fire(&pool, &fire(&s, "960", 960, queued(job)), EVENTS)
        .await
        .unwrap();
    assert_eq!(again, Fired::Handled);
    assert_eq!(len(&pool, "queue:q").await, 1);
    assert_eq!(hash(&pool, "s").await["fired"], "1");
    // A changed definition, a paused schedule, a deleted one.
    let mut changed = fire(&s, "1020", 1_020, queued(job));
    changed.digest = "other";
    assert_eq!(
        store::fire(&pool, &changed, EVENTS).await.unwrap(),
        Fired::Changed
    );
    let _: () = command(
        &pool,
        ::redis::cmd("HSET")
            .arg(keys::periodic("s"))
            .arg("paused")
            .arg(1),
    )
    .await;
    assert_eq!(
        store::fire(&pool, &fire(&s, "1020", 1_020, queued(job)), EVENTS)
            .await
            .unwrap(),
        Fired::Paused
    );
    let _: () = command(&pool, ::redis::cmd("DEL").arg(keys::periodic("s"))).await;
    assert_eq!(
        store::fire(&pool, &fire(&s, "1020", 1_020, queued(job)), EVENTS)
            .await
            .unwrap(),
        Fired::Gone
    );
    assert_eq!(
        score(&pool, "s").await,
        None,
        "the stray index entry is removed"
    );
    assert_eq!(len(&pool, "queue:q").await, 1);
}

#[tokio::test]
async fn fire_never_goes_back_and_consumes_halted_or_skipped_ticks() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let s = every("s", 60);
    create(&pool, &s, 960).await;
    let key = keys::periodic("s");
    // The monotonic guard: a `next` at or before `last_tick` is never enqueued.
    let _: () = command(
        &pool,
        ::redis::cmd("HSET").arg(&key).arg("last_tick").arg(1_020),
    )
    .await;
    let guarded = store::fire(&pool, &fire(&s, "960", 960, queued("{}")), EVENTS)
        .await
        .unwrap();
    assert_eq!(guarded, Fired::Guard);
    let fields = hash(&pool, "s").await;
    assert_eq!(fields["last_tick"], "1020", "last_tick never moves back");
    assert_eq!(fields["skipped"], "1");
    assert_eq!(fields["next"], "1020");
    assert_eq!(len(&pool, "queue:q").await, 0);
    assert_eq!(len(&pool, &keys::periodic_history("s")).await, 0);
    // Halted by client middleware: the tick is consumed, no job, no jid.
    let _: () = command(
        &pool,
        ::redis::cmd("HSET").arg(&key).arg("last_jid").arg(OTHER),
    )
    .await;
    let mut halted = fire(&s, "1020", 1_080, Write::Halted);
    halted.entry = r#"{"result":"halted"}"#.to_owned();
    assert_eq!(
        store::fire(&pool, &halted, EVENTS).await.unwrap(),
        Fired::Done { without_lock: None }
    );
    let fields = hash(&pool, "s").await;
    assert_eq!(fields["last_tick"], "1080");
    assert_eq!(fields["last_result"], "halted");
    assert!(!fields.contains_key("last_jid"));
    assert_eq!(fields["skipped"], "2");
    assert_eq!(fields["next"], "1140");
    // A policy dropping ticks: only `missed` and `next` change, and the history says so.
    let mut skipped = fire(&s, "1140", 0, Write::Skipped);
    skipped.tick = None;
    skipped.missed = 3;
    skipped.new_next = Some(1_500);
    skipped.entry = r#"{"result":"skipped","missed":3}"#.to_owned();
    assert_eq!(
        store::fire(&pool, &skipped, EVENTS).await.unwrap(),
        Fired::Done { without_lock: None }
    );
    let fields = hash(&pool, "s").await;
    assert_eq!(fields["missed"], "3");
    assert_eq!(fields["last_tick"], "1080");
    assert_eq!(fields["next"], "1500");
    assert_eq!(len(&pool, &keys::periodic_history("s")).await, 2);
    // Scheduled by client middleware: to the schedule, at its score.
    let scheduled = Write::Scheduled {
        at: 99_999.5,
        json: "{\"jid\":\"x\"}".to_owned(),
    };
    store::fire(&pool, &fire(&s, "1500", 1_500, scheduled), EVENTS)
        .await
        .unwrap();
    let at: f64 = command(
        &pool,
        ::redis::cmd("ZSCORE")
            .arg(keys::SCHEDULE)
            .arg("{\"jid\":\"x\"}"),
    )
    .await;
    assert_eq!(at, 99_999.5);
    assert_eq!(hash(&pool, "s").await["last_result"], "scheduled");
    // A lost `next` is repaired with an empty expected value.
    let _: () = command(&pool, ::redis::cmd("HDEL").arg(&key).arg("next")).await;
    let _: () = command(&pool, ::redis::cmd("ZREM").arg(keys::PERIODIC).arg("s")).await;
    let mut repair = fire(&s, "", 0, Write::Skipped);
    repair.tick = None;
    repair.new_next = Some(2_000);
    repair.entry = String::new();
    store::fire(&pool, &repair, EVENTS).await.unwrap();
    assert_eq!(hash(&pool, "s").await["next"], "2000");
    assert_eq!(score(&pool, "s").await, Some(2_000.0));
    // No next tick: the schedule leaves the due index.
    let mut park = fire(&s, "2000", 0, Write::Skipped);
    park.tick = None;
    park.new_next = None;
    store::fire(&pool, &park, EVENTS).await.unwrap();
    assert!(!hash(&pool, "s").await.contains_key("next"));
    assert_eq!(score(&pool, "s").await, None);
}

#[tokio::test]
async fn the_history_is_capped_and_errors_are_cleared_by_a_fire() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let s = every("s", 60);
    create(&pool, &s, 60).await;
    store::set_error(&pool, "s", "the push failed", 5)
        .await
        .unwrap();
    assert_eq!(hash(&pool, "s").await["error"], "the push failed");
    for k in 1..=5 {
        let mut fired = fire(&s, "", 0, queued("{}"));
        let expected = (k * 60).to_string();
        fired.expected_next = &expected;
        fired.tick = Some(k * 60);
        fired.new_next = Some((k + 1) * 60);
        fired.entry = format!(r#"{{"tick":{}}}"#, k * 60);
        fired.history = 3;
        store::fire(&pool, &fired, EVENTS).await.unwrap();
    }
    let entries: Vec<String> = command(
        &pool,
        ::redis::cmd("LRANGE")
            .arg(keys::periodic_history("s"))
            .arg(0)
            .arg(-1),
    )
    .await;
    assert_eq!(entries.len(), 3);
    assert!(
        entries[0].contains("\"tick\":300"),
        "newest first: {entries:?}"
    );
    let fields = hash(&pool, "s").await;
    assert!(!fields.contains_key("error") && !fields.contains_key("error_at"));
    // A history of 0 keeps none.
    let mut fired = fire(&s, "360", 360, queued("{}"));
    fired.history = 0;
    store::fire(&pool, &fired, EVENTS).await.unwrap();
    assert_eq!(len(&pool, &keys::periodic_history("s")).await, 0);
    // An error is never recorded on a schedule that is gone.
    store::set_error(&pool, "gone", "x", 5).await.unwrap();
    assert!(hash(&pool, "gone").await.is_empty());
}

#[tokio::test]
async fn fire_takes_the_lock_of_a_unique_job_with_it() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let s = every("s", 60);
    create(&pool, &s, 960).await;
    let mut first = fire(&s, "960", 960, queued("job-1"));
    first.lock = Some(lock("d1", 60));
    assert_eq!(
        store::fire(&pool, &first, EVENTS).await.unwrap(),
        Fired::Done { without_lock: None }
    );
    let holder: Option<String> =
        command(&pool, ::redis::cmd("HGET").arg("unique:d1").arg("jid")).await;
    assert_eq!(holder.as_deref(), Some(JID));
    let ttl: i64 = command(&pool, ::redis::cmd("PTTL").arg("unique:d1")).await;
    assert!(ttl > 55_000 && ttl <= 60_000, "{ttl}");
    let indexed: Option<f64> = command(
        &pool,
        ::redis::cmd("ZSCORE").arg(keys::UNIQUE_LOCKS).arg("d1"),
    )
    .await;
    assert!(indexed.is_some());
    // A resend takes no second lock and writes no second job.
    assert_eq!(
        store::fire(&pool, &first, EVENTS).await.unwrap(),
        Fired::Handled
    );
    // The next tick while the job holds the lock: a duplicate. No job, the tick consumed,
    // `skipped` and the unique counter `d` counted, the lock untouched.
    let mut second = fire(&s, "1020", 1_020, queued("job-2"));
    second.jid = OTHER;
    second.lock = Some(lock("d1", 60));
    assert_eq!(
        store::fire(&pool, &second, EVENTS).await.unwrap(),
        Fired::Duplicate {
            holder: JID.to_owned()
        }
    );
    assert_eq!(len(&pool, "queue:q").await, 1);
    let fields = hash(&pool, "s").await;
    assert_eq!(fields["last_result"], "duplicate");
    assert_eq!(fields["last_tick"], "1020");
    assert_eq!(fields["skipped"], "1");
    assert_eq!(fields["fired"], "1");
    assert_eq!(fields["next"], "1080");
    assert!(!fields.contains_key("last_jid"));
    let history: Vec<String> = command(
        &pool,
        ::redis::cmd("LRANGE")
            .arg(keys::periodic_history("s"))
            .arg(0)
            .arg(0),
    )
    .await;
    assert!(history[0].contains("duplicate"), "{history:?}");
    let counted: Option<i64> = command(&pool, ::redis::cmd("HGET").arg(EVENTS).arg("Tick|d")).await;
    assert_eq!(counted, Some(1));
    let holder: Option<String> =
        command(&pool, ::redis::cmd("HGET").arg("unique:d1").arg("jid")).await;
    assert_eq!(holder.as_deref(), Some(JID));
    // A scheduled unique job's lock also covers its delay.
    let at = crate::payload::now_secs_f64() + 120.0;
    let mut scheduled = fire(
        &s,
        "1080",
        1_080,
        Write::Scheduled {
            at,
            json: "job-3".to_owned(),
        },
    );
    scheduled.jid = "cccccccccccccccccccccccc";
    scheduled.lock = Some(lock("d3", 60));
    store::fire(&pool, &scheduled, EVENTS).await.unwrap();
    let ttl: i64 = command(&pool, ::redis::cmd("PTTL").arg("unique:d3")).await;
    assert!(ttl > 170_000 && ttl <= 180_000, "{ttl}");
    let recorded: Option<String> =
        command(&pool, ::redis::cmd("HGET").arg("unique:d3").arg("at")).await;
    assert!(recorded.is_some());
    // With its member there, so that a debounce push finds it (unique-1).
    let member: Option<String> =
        command(&pool, ::redis::cmd("HGET").arg("unique:d3").arg("m")).await;
    assert_eq!(member.as_deref(), Some("job-3"));
    // A lock key of the wrong type: refused before any write.
    let _: () = command(&pool, ::redis::cmd("SET").arg("unique:d4").arg("x")).await;
    let mut wrong = fire(&s, "1140", 1_140, queued("job-4"));
    wrong.lock = Some(lock("d4", 60));
    let error = store::fire(&pool, &wrong, EVENTS).await.unwrap_err();
    assert!(error.to_string().contains("WRONGTYPE"), "{error}");
    assert_eq!(hash(&pool, "s").await["next"], "1140");
    assert_eq!(len(&pool, "queue:q").await, 1);
}

#[tokio::test]
async fn out_of_memory_refuses_a_fire_before_any_write() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let s = every("s", 60);
    create(&pool, &s, 960).await;
    let mut held = fire(&s, "960", 960, queued("job-1"));
    held.lock = Some(lock("d1", 60));
    store::fire(&pool, &held, EVENTS).await.unwrap();
    let before = hash(&pool, "s").await;
    let _: () = command(
        &pool,
        ::redis::cmd("CONFIG").arg("SET").arg("maxmemory").arg("1"),
    )
    .await;
    // A plain tick: the job's write is the first, refused.
    let error = store::fire(&pool, &fire(&s, "1020", 1_020, queued("job-2")), EVENTS)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("OOM"), "{error}");
    // A duplicate tick: nothing written either.
    let mut duplicate = fire(&s, "1020", 1_020, queued("job-2"));
    duplicate.jid = OTHER;
    duplicate.lock = Some(lock("d1", 60));
    assert!(store::fire(&pool, &duplicate, EVENTS).await.is_err());
    assert_eq!(hash(&pool, "s").await, before);
    assert_eq!(len(&pool, "queue:q").await, 1);
    let _: () = command(
        &pool,
        ::redis::cmd("CONFIG").arg("SET").arg("maxmemory").arg("0"),
    )
    .await;
    // Memory free again: the tick fires, once.
    assert_eq!(
        store::fire(&pool, &fire(&s, "1020", 1_020, queued("job-2")), EVENTS)
            .await
            .unwrap(),
        Fired::Done { without_lock: None }
    );
    assert_eq!(len(&pool, "queue:q").await, 2);
}

/// Registers `crontab` in group `group`, reading its members first, at `now_ms`.
async fn register(pool: &RedisPool, group: &str, crontab: &[&Definition], now_ms: i64) -> Synced {
    let mut members: Vec<String> = command(
        pool,
        ::redis::cmd("SMEMBERS").arg(keys::periodic_group(group)),
    )
    .await;
    members.sort();
    let names: Vec<&str> = crontab
        .iter()
        .map(|definition| definition.name.as_str())
        .collect();
    let stale: Vec<String> = members
        .iter()
        .filter(|member| !names.contains(&member.as_str()))
        .cloned()
        .collect();
    let now = now_ms / 1_000;
    let entries: Vec<SyncEntry<'_>> = crontab
        .iter()
        .map(|definition| SyncEntry {
            definition,
            next_new: definition.timing.next_after(now).unwrap(),
            next_changed: definition.timing.next_after(now - 60).unwrap(),
        })
        .collect();
    store::sync(
        pool,
        group,
        r#"{"digest":"x"}"#,
        &members,
        &stale,
        &entries,
        now_ms,
    )
    .await
    .unwrap()
    .expect("the group did not change")
}

fn hourly(name: &str, expression: &str) -> Definition {
    Periodic::cron(name, expression)
        .class("Report", vec![])
        .definition()
        .unwrap()
}

#[tokio::test]
async fn sync_creates_keeps_and_reschedules_schedules() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    // 2026-11-02 10:00:30 UTC.
    let now_ms = 1_793_613_630_000;
    let a = hourly("a", "0 * * * *");
    let b = hourly("tenant:42/b c", "30 * * * *");
    let synced = register(&pool, "g", &[&a, &b], now_ms).await;
    assert_eq!(
        (synced.created, synced.unchanged, synced.removed),
        (2, 0, 0)
    );
    let fields = hash(&pool, "a").await;
    assert_eq!(fields["source"], "code");
    assert_eq!(fields["group"], "g");
    assert_eq!(fields["digest"], a.digest);
    assert_eq!(fields["next"], "1793617200", "strictly after now: 11:00");
    assert_eq!(fields["created_at"], now_ms.to_string());
    assert_eq!(hash(&pool, "tenant:42/b c").await["next"], "1793615400");
    let names: Vec<String> = command(
        &pool,
        ::redis::cmd("ZRANGE")
            .arg(keys::PERIODIC_NAMES)
            .arg(0)
            .arg(-1),
    )
    .await;
    assert_eq!(names, ["a", "tenant:42/b c"]);
    let record: Option<String> = command(
        &pool,
        ::redis::cmd("HGET").arg(keys::PERIODIC_CRONTABS).arg("g"),
    )
    .await;
    assert_eq!(record.as_deref(), Some(r#"{"digest":"x"}"#));
    // The operators' state survives an identical registration.
    let _: () = command(
        &pool,
        ::redis::cmd("HSET")
            .arg(keys::periodic("a"))
            .arg("last_tick")
            .arg(42)
            .arg("fired")
            .arg(7)
            .arg("next")
            .arg(1_793_620_800),
    )
    .await;
    let synced = register(&pool, "g", &[&a, &b], now_ms + 1_000).await;
    assert_eq!((synced.created, synced.unchanged), (0, 2));
    let fields = hash(&pool, "a").await;
    assert_eq!(fields["last_tick"], "42");
    assert_eq!(fields["fired"], "7");
    assert_eq!(fields["next"], "1793620800");
    // A template change keeps `next`; a timetable change moves it (the 60 s grace: a tick of
    // the new timetable in the last minute still fires).
    let a2 = Periodic::cron("a", "0 * * * *")
        .class("Report", vec![serde_json::json!(1)])
        .definition()
        .unwrap();
    let b2 = hourly("tenant:42/b c", "0 * * * *");
    let synced = register(&pool, "g", &[&a2, &b2], now_ms).await;
    assert_eq!((synced.updated, synced.rescheduled), (1, 1));
    assert_eq!(hash(&pool, "a").await["next"], "1793620800");
    assert_eq!(
        hash(&pool, "tenant:42/b c").await["next"],
        "1793613600",
        "10:00, within the minute before now"
    );
    assert_eq!(score(&pool, "tenant:42/b c").await, Some(1_793_613_600.0));
}

#[tokio::test]
async fn sync_prunes_removed_schedules_and_respects_others() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let now_ms = 1_793_613_630_000;
    let a = hourly("a", "0 * * * *");
    let b = hourly("b:with:colons", "0 * * * *");
    register(&pool, "g", &[&a, &b], now_ms).await;
    let _: () = command(
        &pool,
        ::redis::cmd("RPUSH")
            .arg(keys::periodic_history("b:with:colons"))
            .arg("{}"),
    )
    .await;
    // Another group's schedule and an API schedule.
    let other = hourly("other", "0 * * * *");
    register(&pool, "h", &[&other], now_ms).await;
    let api = every("api", 60);
    create(&pool, &api, 60).await;
    // A leaked member whose hash is gone.
    let _: () = command(
        &pool,
        ::redis::cmd("SADD")
            .arg(keys::periodic_group("g"))
            .arg("ghost"),
    )
    .await;
    let _: () = command(
        &pool,
        ::redis::cmd("ZADD").arg(keys::PERIODIC).arg(1).arg("ghost"),
    )
    .await;
    // The crontab of g now has a, plus names the API and h use.
    let conflict_api = hourly("api", "0 * * * *");
    let conflict_other = hourly("other", "5 * * * *");
    let synced = register(&pool, "g", &[&a, &conflict_api, &conflict_other], now_ms).await;
    assert_eq!(synced.removed, 1);
    assert_eq!(synced.conflicts, ["api", "other"]);
    assert!(hash(&pool, "b:with:colons").await.is_empty());
    assert_eq!(
        len(&pool, &keys::periodic_history("b:with:colons")).await,
        0
    );
    assert_eq!(score(&pool, "b:with:colons").await, None);
    assert_eq!(score(&pool, "ghost").await, None);
    let members: Vec<String> = command(
        &pool,
        ::redis::cmd("SMEMBERS").arg(keys::periodic_group("g")),
    )
    .await;
    assert_eq!(members, ["a"]);
    // The conflicting schedules are untouched.
    assert_eq!(hash(&pool, "api").await["source"], "api");
    assert_eq!(hash(&pool, "other").await["group"], "h");
    assert_eq!(hash(&pool, "other").await["digest"], other.digest);
    // A group read before another registration changed it: refused, read again.
    let entries = [SyncEntry {
        definition: &a,
        next_new: 1,
        next_changed: 1,
    }];
    let stale = store::sync(
        &pool,
        "g",
        "{}",
        &["a".to_owned(), "zzz".to_owned()],
        &[],
        &entries,
        now_ms,
    )
    .await
    .unwrap();
    assert_eq!(stale, None);
    // An empty crontab prunes the group.
    let synced = register(&pool, "g", &[], now_ms).await;
    assert_eq!(synced.removed, 1);
    assert!(hash(&pool, "a").await.is_empty());
    assert_eq!(hash(&pool, "other").await["group"], "h");
}

#[tokio::test]
async fn sync_repairs_the_index_and_keeps_paused_schedules_paused() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let now_ms = 1_793_613_630_000;
    let a = hourly("a", "0 * * * *");
    let b = hourly("b", "0 * * * *");
    let paused = Periodic::cron("p", "0 * * * *")
        .class("Report", vec![])
        .paused(true)
        .definition()
        .unwrap();
    register(&pool, "g", &[&a, &b, &paused], now_ms).await;
    // Created paused: no `next`, not due.
    let fields = hash(&pool, "p").await;
    assert!(fields.contains_key("paused") && !fields.contains_key("next"));
    assert_eq!(score(&pool, "p").await, None);
    // A lost index entry comes back at `next`; a lease in progress is kept (NX).
    let _: () = command(&pool, ::redis::cmd("ZREM").arg(keys::PERIODIC).arg("a")).await;
    let _: () = command(
        &pool,
        ::redis::cmd("ZADD")
            .arg(keys::PERIODIC)
            .arg(9_999_999_999_u64)
            .arg("b"),
    )
    .await;
    // A lost `next` gets the first tick after now.
    let _: () = command(
        &pool,
        ::redis::cmd("HDEL").arg(keys::periodic("b")).arg("next"),
    )
    .await;
    let _: () = command(
        &pool,
        ::redis::cmd("HDEL").arg(keys::periodic("a")).arg("x"),
    )
    .await;
    register(&pool, "g", &[&a, &b, &paused], now_ms).await;
    assert_eq!(score(&pool, "a").await, Some(1_793_617_200.0));
    assert_eq!(hash(&pool, "b").await["next"], "1793617200");
    // Paused stays paused, even when its timetable changes.
    let changed = Periodic::cron("p", "30 * * * *")
        .class("Report", vec![])
        .definition()
        .unwrap();
    register(&pool, "g", &[&a, &b, &changed], now_ms).await;
    let fields = hash(&pool, "p").await;
    assert!(fields.contains_key("paused") && !fields.contains_key("next"));
    assert_eq!(fields["digest"], changed.digest);
}

#[tokio::test]
async fn upsert_creates_updates_and_reschedules() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let s = every("s", 60);
    assert_eq!(
        store::upsert(&pool, &s, "", "api", 120, 60, 5)
            .await
            .unwrap(),
        UpsertReply::Done(1)
    );
    let fields = hash(&pool, "s").await;
    assert_eq!(
        (fields["next"].as_str(), fields["source"].as_str()),
        ("120", "api")
    );
    assert_eq!(fields["created_at"], "5");
    // Identical: nothing changes.
    assert_eq!(
        store::upsert(&pool, &s, &s.digest, "api", 999, 999, 6)
            .await
            .unwrap(),
        UpsertReply::Done(0)
    );
    assert_eq!(hash(&pool, "s").await["next"], "120");
    // Expected absent, or another digest: the current digest comes back.
    assert_eq!(
        store::upsert(&pool, &s, "", "api", 1, 1, 6).await.unwrap(),
        UpsertReply::Conflict(s.digest.clone())
    );
    assert_eq!(
        store::upsert(&pool, &s, "stale", "api", 1, 1, 6)
            .await
            .unwrap(),
        UpsertReply::Conflict(s.digest.clone())
    );
    // Owned by the other source.
    assert_eq!(
        store::upsert(&pool, &s, "*", "code", 1, 1, 6)
            .await
            .unwrap(),
        UpsertReply::OtherSource("api".to_owned())
    );
    // A template change keeps `next`; a timetable change takes the second candidate.
    let template = Periodic::every("s", Duration::from_secs(60))
        .class("Tick", vec![serde_json::json!(2)])
        .queue("q")
        .definition()
        .unwrap();
    assert_eq!(
        store::upsert(&pool, &template, &s.digest, "api", 999, 999, 7)
            .await
            .unwrap(),
        UpsertReply::Done(2)
    );
    assert_eq!(hash(&pool, "s").await["next"], "120");
    let timetable = every("s", 30);
    assert_eq!(
        store::upsert(&pool, &timetable, &template.digest, "api", 999, 90, 8)
            .await
            .unwrap(),
        UpsertReply::Done(3)
    );
    assert_eq!(hash(&pool, "s").await["next"], "90");
    assert_eq!(score(&pool, "s").await, Some(90.0));
    // Paused stays paused.
    store::pause(&pool, "s", 9).await.unwrap();
    assert_eq!(
        store::upsert(&pool, &s, &timetable.digest, "api", 999, 999, 10)
            .await
            .unwrap(),
        UpsertReply::Done(2)
    );
    assert!(!hash(&pool, "s").await.contains_key("next"));
    assert_eq!(score(&pool, "s").await, None);
}

#[tokio::test]
async fn pause_unpause_and_remove() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let s = every("s", 60);
    create(&pool, &s, 120).await;
    assert_eq!(store::pause(&pool, "s", 50).await.unwrap(), 1);
    assert_eq!(store::pause(&pool, "s", 51).await.unwrap(), 2);
    assert_eq!(store::pause(&pool, "missing", 51).await.unwrap(), 0);
    let fields = hash(&pool, "s").await;
    assert_eq!(fields["paused"], "50");
    assert!(!fields.contains_key("next"));
    assert_eq!(score(&pool, "s").await, None);
    // A resume computed for another definition is refused.
    assert_eq!(store::unpause(&pool, "s", 180, "stale").await.unwrap(), -1);
    assert_eq!(store::unpause(&pool, "s", 180, &s.digest).await.unwrap(), 1);
    assert_eq!(store::unpause(&pool, "s", 180, &s.digest).await.unwrap(), 2);
    assert_eq!(
        store::unpause(&pool, "missing", 180, &s.digest)
            .await
            .unwrap(),
        0
    );
    let fields = hash(&pool, "s").await;
    assert!(!fields.contains_key("paused"));
    assert_eq!(fields["next"], "180");
    assert_eq!(score(&pool, "s").await, Some(180.0));
    // Removal: refused for another source, then everything goes.
    let code = hourly("c", "0 * * * *");
    register(&pool, "g", &[&code], 1_793_613_630_000).await;
    let _: () = command(
        &pool,
        ::redis::cmd("RPUSH")
            .arg(keys::periodic_history("c"))
            .arg("{}"),
    )
    .await;
    assert_eq!(
        store::remove(&pool, "c", Some("api"), Some("g"))
            .await
            .unwrap(),
        -2
    );
    assert_eq!(
        store::remove(&pool, "c", None, Some("other"))
            .await
            .unwrap(),
        -3
    );
    assert_eq!(store::remove(&pool, "c", None, Some("g")).await.unwrap(), 1);
    assert!(hash(&pool, "c").await.is_empty());
    assert_eq!(len(&pool, &keys::periodic_history("c")).await, 0);
    let members: Vec<String> = command(
        &pool,
        ::redis::cmd("SMEMBERS").arg(keys::periodic_group("g")),
    )
    .await;
    assert!(members.is_empty());
    assert_eq!(
        store::remove(&pool, "s", Some("api"), None).await.unwrap(),
        1
    );
    let names: Vec<String> = command(
        &pool,
        ::redis::cmd("ZRANGE")
            .arg(keys::PERIODIC_NAMES)
            .arg(0)
            .arg(-1),
    )
    .await;
    assert!(names.is_empty());
    // A stray index entry of a missing schedule is tidied.
    let _: () = command(
        &pool,
        ::redis::cmd("ZADD").arg(keys::PERIODIC).arg(1).arg("stray"),
    )
    .await;
    assert_eq!(store::remove(&pool, "stray", None, None).await.unwrap(), 0);
    assert_eq!(score(&pool, "stray").await, None);
}

#[tokio::test]
async fn unindex_removes_only_a_schedule_still_paused_or_deleted() {
    // periodic-7: the poller found a schedule paused (or deleted) and removed its index entry
    // with a plain ZREM in a later round trip: a resume (or a re-creation) landing in between
    // lost its entry, and the schedule never fired again.
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    for name in ["p", "d"] {
        create(&pool, &every(name, 60), 120).await;
    }
    // Paused, then resumed before the removal: kept.
    let _: () = command(
        &pool,
        ::redis::cmd("HSET")
            .arg(keys::periodic("p"))
            .arg("paused")
            .arg("1"),
    )
    .await;
    let _: () = command(
        &pool,
        ::redis::cmd("HDEL").arg(keys::periodic("p")).arg("paused"),
    )
    .await;
    assert!(!store::unindex(&pool, "p").await.unwrap());
    assert_eq!(score(&pool, "p").await, Some(120.0));
    // Still paused: removed.
    let _: () = command(
        &pool,
        ::redis::cmd("HSET")
            .arg(keys::periodic("p"))
            .arg("paused")
            .arg("1"),
    )
    .await;
    assert!(store::unindex(&pool, "p").await.unwrap());
    assert_eq!(score(&pool, "p").await, None);
    // Deleted, then created again before the removal: kept; still deleted: removed.
    assert!(!store::unindex(&pool, "d").await.unwrap());
    assert_eq!(score(&pool, "d").await, Some(120.0));
    let _: () = command(&pool, ::redis::cmd("DEL").arg(keys::periodic("d"))).await;
    assert!(store::unindex(&pool, "d").await.unwrap());
    assert_eq!(score(&pool, "d").await, None);
    // Nothing to remove.
    assert!(!store::unindex(&pool, "d").await.unwrap());
}

#[tokio::test]
async fn enqueue_now_pushes_without_touching_the_timetable() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let s = every("s", 60);
    create(&pool, &s, 120).await;
    let now = EnqueueNow {
        name: "s",
        write: queued("job-1"),
        lock: None,
        entry: r#"{"result":"manual"}"#.to_owned(),
        duplicate_entry: r#"{"result":"duplicate"}"#.to_owned(),
        history: 10,
        now_ms: 77,
        jid: JID,
    };
    assert_eq!(
        store::enqueue_now(&pool, &now, EVENTS).await.unwrap(),
        Enqueued::Done { without_lock: None }
    );
    let fields = hash(&pool, "s").await;
    assert_eq!(
        (fields["manual_at"].as_str(), fields["manual_jid"].as_str()),
        ("77", JID)
    );
    assert_eq!(fields["next"], "120", "the timetable is untouched");
    assert_eq!(len(&pool, "queue:q").await, 1);
    assert_eq!(len(&pool, &keys::periodic_history("s")).await, 1);
    // Unique: the lock is taken; a second request is a duplicate; a resend of the first is
    // recognized.
    let mut unique = now.clone();
    unique.jid = OTHER;
    unique.write = queued("job-2");
    unique.lock = Some(lock("d1", 60));
    assert_eq!(
        store::enqueue_now(&pool, &unique, EVENTS).await.unwrap(),
        Enqueued::Done { without_lock: None }
    );
    assert_eq!(
        store::enqueue_now(&pool, &unique, EVENTS).await.unwrap(),
        Enqueued::Already
    );
    let mut duplicate = unique.clone();
    duplicate.jid = "cccccccccccccccccccccccc";
    assert_eq!(
        store::enqueue_now(&pool, &duplicate, EVENTS).await.unwrap(),
        Enqueued::Duplicate {
            holder: OTHER.to_owned()
        }
    );
    assert_eq!(len(&pool, "queue:q").await, 2);
    // Scheduled (by client middleware): the lock records the job's score and member there, so
    // that a debounce push finds it (unique-1).
    let mut scheduled = unique.clone();
    scheduled.jid = "dddddddddddddddddddddddd";
    scheduled.write = Write::Scheduled {
        at: crate::payload::now_secs_f64() + 120.0,
        json: "job-3".to_owned(),
    };
    scheduled.lock = Some(lock("d2", 60));
    assert_eq!(
        store::enqueue_now(&pool, &scheduled, EVENTS).await.unwrap(),
        Enqueued::Done { without_lock: None }
    );
    let recorded: Vec<Option<String>> = command(
        &pool,
        ::redis::cmd("HMGET").arg("unique:d2").arg("at").arg("m"),
    )
    .await;
    assert!(recorded[0].is_some(), "{recorded:?}");
    assert_eq!(recorded[1].as_deref(), Some("job-3"));
    // Halted: the request is recorded, nothing written.
    let mut halted = now.clone();
    halted.write = Write::Halted;
    halted.jid = "";
    store::enqueue_now(&pool, &halted, EVENTS).await.unwrap();
    assert!(!hash(&pool, "s").await.contains_key("manual_jid"));
    // A missing schedule.
    let mut missing = now.clone();
    missing.name = "missing";
    assert_eq!(
        store::enqueue_now(&pool, &missing, EVENTS).await.unwrap(),
        Enqueued::Missing
    );
    assert_eq!(len(&pool, "queue:q").await, 2);
}

#[tokio::test]
async fn record_run_keeps_the_newest_outcome_and_counts_failures_once() {
    use crate::periodic::RunStatus;
    use crate::periodic::record_run::record;

    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let s = every("s", 60);
    create(&pool, &s, 120).await;
    assert!(
        record(&pool, "s", 60_000, RunStatus::Failed, "Boom: no", 12, JID)
            .await
            .unwrap()
    );
    let fields = hash(&pool, "s").await;
    assert_eq!(fields["run_status"], "failed");
    assert_eq!(fields["run_error"], "Boom: no");
    assert_eq!(fields["run_ms"], "12");
    assert_eq!(fields["run_jid"], JID);
    assert_eq!(fields["run_failures"], "1");
    // A newer tick wins; an older one is ignored.
    assert!(
        record(&pool, "s", 120_000, RunStatus::Succeeded, "", 3, OTHER)
            .await
            .unwrap()
    );
    assert!(
        !record(&pool, "s", 60_000, RunStatus::Failed, "late", 5, JID)
            .await
            .unwrap()
    );
    let fields = hash(&pool, "s").await;
    assert_eq!(fields["run_status"], "ok");
    assert!(!fields.contains_key("run_error"));
    assert_eq!(fields["run_failures"], "1");
    // A deleted schedule is never recreated.
    let _: () = command(&pool, ::redis::cmd("DEL").arg(keys::periodic("s"))).await;
    assert!(
        !record(&pool, "s", 180_000, RunStatus::Succeeded, "", 1, JID)
            .await
            .unwrap()
    );
    assert!(hash(&pool, "s").await.is_empty());
    // A resend of the same record (same token) changes nothing.
    create(&pool, &s, 120).await;
    let key = keys::periodic("s");
    let script = crate::periodic::lua::PERIODIC_RECORD_RUN.script();
    for _ in 0..2 {
        let _: i64 = {
            let mut conn = pool.conn().await.unwrap();
            script
                .key(&key)
                .arg(240_000)
                .arg("failed")
                .arg("x")
                .arg(1)
                .arg(JID)
                .arg("token")
                .invoke_async(&mut conn)
                .await
                .unwrap()
        };
    }
    assert_eq!(hash(&pool, "s").await["run_failures"], "1");
}

#[tokio::test]
async fn concurrent_fires_of_one_tick_push_once() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let s = every("s", 60);
    create(&pool, &s, 960).await;
    let mut tasks = Vec::new();
    for i in 0..16 {
        let pool = pool.clone();
        let s = s.clone();
        tasks.push(tokio::spawn(async move {
            let jid = format!("{i:024}");
            let mut attempt = fire(&s, "960", 960, queued(&format!("job-{i}")));
            attempt.jid = &jid;
            store::fire(&pool, &attempt, EVENTS).await.unwrap()
        }));
    }
    let mut done = 0;
    for task in tasks {
        if let Fired::Done { .. } = task.await.unwrap() {
            done += 1;
        }
    }
    assert_eq!(done, 1);
    assert_eq!(len(&pool, "queue:q").await, 1);
    assert_eq!(hash(&pool, "s").await["fired"], "1");
}
