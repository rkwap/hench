//! The unique library and scripts against a private Redis.

use super::*;
use crate::redis::RedisPool;
use crate::test_util::TestRedis;
use ::redis::Value;

const LOCK: &str = "unique:d1";
const INDEX: &str = "unique-locks";
const EVENTS: &str = "u|20261101|9:30";
const JID: &str = "aaaaaaaaaaaaaaaaaaaaaaaa";
const OTHER: &str = "bbbbbbbbbbbbbbbbbbbbbbbb";

/// Runs the test script `body` after the prelude and the library, with the three keys of a
/// lock and `args`.
async fn call(pool: &RedisPool, body: &str, args: &[&str]) -> Value {
    let script = ::redis::Script::new(&format!("{}\n{LIB}\n{body}", crate::lua::PRELUDE));
    let mut invocation = script.prepare_invoke();
    invocation.key(LOCK).key(INDEX).key(EVENTS);
    for arg in args {
        invocation.arg(*arg);
    }
    let mut conn = pool.conn().await.unwrap();
    invocation.invoke_async(&mut conn).await.unwrap()
}

async fn command<T: ::redis::FromRedisValue>(pool: &RedisPool, command: &::redis::Cmd) -> T {
    let mut conn = pool.conn().await.unwrap();
    command.query_async(&mut conn).await.unwrap()
}

const FIELDS: &str = "{'class', 'C', 'queue', 'q', 'until', 'success', 'for', '60'}";

#[tokio::test]
async fn take_hold_release_and_check() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let check = "return hu_check(KEYS[1], ARGV[1])";
    assert_eq!(
        call(&pool, check, &[JID]).await,
        Value::BulkString(b"free".to_vec())
    );
    // Take: every field, an expiry and an index entry scored by the expiry.
    let take = format!(
        "local now = h_now_ms() hu_take(KEYS[1], KEYS[2], 'd1', ARGV[1], 60000, now, {FIELDS}, ARGV[2]) return now"
    );
    let Value::Int(now) = call(&pool, &take, &[JID, "1800000000.5"]).await else {
        panic!("expected the time");
    };
    let fields: std::collections::HashMap<String, String> =
        command(&pool, ::redis::cmd("HGETALL").arg(LOCK)).await;
    assert_eq!(fields["jid"], JID);
    assert_eq!(fields["class"], "C");
    assert_eq!(fields["until"], "success");
    assert_eq!(fields["for"], "60");
    assert_eq!(fields["at"], "1800000000.5");
    let ttl: i64 = command(&pool, ::redis::cmd("PTTL").arg(LOCK)).await;
    assert!(ttl > 59_000 && ttl <= 60_000, "{ttl}");
    let score: f64 = command(&pool, ::redis::cmd("ZSCORE").arg(INDEX).arg("d1")).await;
    assert!(
        (score * 1000.0 - (now as f64 + 60_000.0)).abs() < 2.0,
        "{score}"
    );
    assert_eq!(
        call(&pool, check, &[JID]).await,
        Value::BulkString(b"self".to_vec())
    );
    assert_eq!(
        call(&pool, check, &[OTHER]).await,
        Value::BulkString(JID.as_bytes().to_vec())
    );

    // Hold by the holder never shortens, records or clears `at`.
    let hold = format!(
        "return hu_hold(KEYS[1], KEYS[2], 'd1', ARGV[1], tonumber(ARGV[2]), h_now_ms(), {FIELDS}, ARGV[3])"
    );
    assert_eq!(call(&pool, &hold, &[JID, "1000", ""]).await, Value::Int(1));
    let ttl: i64 = command(&pool, ::redis::cmd("PTTL").arg(LOCK)).await;
    assert!(ttl > 50_000, "a hold must not shorten the lock: {ttl}");
    let at: Option<String> = command(&pool, ::redis::cmd("HGET").arg(LOCK).arg("at")).await;
    assert_eq!(at, None);
    assert_eq!(
        call(&pool, &hold, &[JID, "120000", "7.5"]).await,
        Value::Int(1)
    );
    let ttl: i64 = command(&pool, ::redis::cmd("PTTL").arg(LOCK)).await;
    assert!(ttl > 100_000, "{ttl}");
    // Another job's hold leaves the lock alone.
    assert_eq!(
        call(&pool, &hold, &[OTHER, "999999", ""]).await,
        Value::Int(0)
    );
    let holder: String = command(&pool, ::redis::cmd("HGET").arg(LOCK).arg("jid")).await;
    assert_eq!(holder, JID);

    // Release: by another job, refused; by the holder, done; again, nothing to release.
    let release = "return hu_release(KEYS[1], KEYS[2], 'd1', ARGV[1])";
    assert_eq!(call(&pool, release, &[OTHER]).await, Value::Int(-1));
    assert_eq!(call(&pool, release, &[JID]).await, Value::Int(1));
    let exists: i64 = command(&pool, ::redis::cmd("EXISTS").arg(LOCK)).await;
    assert_eq!(exists, 0);
    let indexed: Option<f64> = command(&pool, ::redis::cmd("ZSCORE").arg(INDEX).arg("d1")).await;
    assert_eq!(indexed, None);
    assert_eq!(call(&pool, release, &[JID]).await, Value::Int(0));
    // A hold of a free lock takes it.
    assert_eq!(
        call(&pool, &hold, &[OTHER, "5000", ""]).await,
        Value::Int(2)
    );
    let holder: String = command(&pool, ::redis::cmd("HGET").arg(LOCK).arg("jid")).await;
    assert_eq!(holder, OTHER);
}

#[tokio::test]
async fn push_one_writes_the_job_before_its_lock() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    // `write` records whether it ran (and whether the lock existed then).
    let push = format!(
        "local now = h_now_ms() \
         local function write() redis.call('SET', 'written', redis.call('EXISTS', KEYS[1])) end \
         return hu_push_one(KEYS[1], KEYS[2], KEYS[3], 'd1', ARGV[1], 'C', 60000, now, {FIELDS}, '', write)"
    );
    assert_eq!(
        call(&pool, &push, &[JID]).await,
        Value::Array(vec![Value::Int(1)])
    );
    let written: String = command(&pool, ::redis::cmd("GET").arg("written")).await;
    assert_eq!(written, "0", "the job is written before the lock is taken");
    // Same jid: already pushed, nothing written.
    let _: () = command(&pool, ::redis::cmd("DEL").arg("written")).await;
    assert_eq!(
        call(&pool, &push, &[JID]).await,
        Value::Array(vec![
            Value::Int(2),
            Value::BulkString(JID.as_bytes().to_vec())
        ])
    );
    // Another jid: a duplicate, nothing written, `d` counted for the class (kept 8 hours).
    assert_eq!(
        call(&pool, &push, &[OTHER]).await,
        Value::Array(vec![
            Value::Int(0),
            Value::BulkString(JID.as_bytes().to_vec())
        ])
    );
    let written: Option<String> = command(&pool, ::redis::cmd("GET").arg("written")).await;
    assert_eq!(written, None);
    let duplicates: i64 = command(&pool, ::redis::cmd("HGET").arg(EVENTS).arg("C|d")).await;
    assert_eq!(duplicates, 1);
    let ttl: i64 = command(&pool, ::redis::cmd("TTL").arg(EVENTS)).await;
    assert!(ttl > 28_700 && ttl <= 28_800, "{ttl}");
    // A lock key of the wrong type: refused before anything is written.
    let _: () = command(&pool, ::redis::cmd("DEL").arg(LOCK)).await;
    let _: () = command(&pool, ::redis::cmd("SET").arg(LOCK).arg("x")).await;
    let Value::Array(reply) = call(&pool, &push, &[OTHER]).await else {
        panic!("expected an array");
    };
    assert_eq!(reply[0], Value::Int(-1));
    let written: Option<String> = command(&pool, ::redis::cmd("GET").arg("written")).await;
    assert_eq!(written, None);
    // The lock cannot be taken (the index has the wrong type): written without its lock.
    let _: () = command(&pool, ::redis::cmd("DEL").arg(LOCK).arg(INDEX)).await;
    let _: () = command(&pool, ::redis::cmd("SET").arg(INDEX).arg("x")).await;
    let Value::Array(reply) = call(&pool, &push, &[OTHER]).await else {
        panic!("expected an array");
    };
    assert_eq!(reply[0], Value::Int(4));
    let written: String = command(&pool, ::redis::cmd("GET").arg("written")).await;
    assert_eq!(written, "0");
}

#[tokio::test]
async fn trims_remove_at_most_the_limit_of_expired_entries() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    for index in 0..12 {
        let _: () = command(
            &pool,
            ::redis::cmd("ZADD")
                .arg(INDEX)
                .arg(1.0 + f64::from(index))
                .arg(format!("old{index}")),
        )
        .await;
    }
    let _: () = command(
        &pool,
        ::redis::cmd("ZADD").arg(INDEX).arg(9.0e12).arg("live"),
    )
    .await;
    let trim = "hu_trim(KEYS[2], h_now_ms(), 10) return redis.call('ZCARD', KEYS[2])";
    assert_eq!(call(&pool, trim, &[]).await, Value::Int(3));
    assert_eq!(call(&pool, trim, &[]).await, Value::Int(1));
}

// The scripts, one by one: their effects, the guards that make a resend (a lost reply, a
// `persist` retry) harmless, the holder comparisons, and concurrent callers.

const INPROGRESS: &str = "host:1:abc:inprogress";
const DIGEST: &str = "d1";
const FETCHED: &str = r#"{"class":"C","jid":"aaaaaaaaaaaaaaaaaaaaaaaa","queue":"q"}"#;

/// Runs one of the unique scripts with `keys` and `args`.
async fn run<T: ::redis::FromRedisValue>(
    pool: &RedisPool,
    script: &LuaScript,
    keys: &[&str],
    args: &[&str],
) -> T {
    let script = script.script();
    let mut invocation = script.prepare_invoke();
    for key in keys {
        invocation.key(*key);
    }
    for arg in args {
        invocation.arg(*arg);
    }
    let mut conn = pool.conn().await.unwrap();
    invocation.invoke_async(&mut conn).await.unwrap()
}

/// Takes the lock of `jid` (60 s) as a push would.
async fn take(pool: &RedisPool, jid: &str) {
    let take = format!(
        "hu_take(KEYS[1], KEYS[2], 'd1', ARGV[1], 60000, h_now_ms(), {FIELDS}, '') return 1"
    );
    let _: Value = call(pool, &take, &[jid]).await;
}

async fn holder_of(pool: &RedisPool) -> Option<String> {
    command(pool, ::redis::cmd("HGET").arg(LOCK).arg("jid")).await
}

async fn lapses(pool: &RedisPool) -> i64 {
    let count: Option<i64> = command(pool, ::redis::cmd("HGET").arg(EVENTS).arg("C|z")).await;
    count.unwrap_or(0)
}

async fn put_in_progress(pool: &RedisPool) {
    let _: () = command(pool, ::redis::cmd("LPUSH").arg(INPROGRESS).arg(FETCHED)).await;
}

/// The arguments of the lock work after an outcome's own (op, jid, digest, class, queue,
/// until, for).
fn lock_args<'a>(op: &'a str, jid: &'a str, until: &'a str) -> [&'a str; 7] {
    [op, jid, DIGEST, "C", "q", until, "60000"]
}

#[tokio::test]
async fn ack_releases_the_lock_once_and_only_its_own() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    take(&pool, JID).await;
    put_in_progress(&pool).await;
    let keys = [INPROGRESS, LOCK, INDEX, EVENTS];
    let mut args = vec![FETCHED];
    args.extend(lock_args("release", JID, "success"));
    let reply: (i64, i64) = run(&pool, &ACK_UNIQUE, &keys, &args).await;
    assert_eq!(reply, (1, 1));
    assert_eq!(holder_of(&pool).await, None);
    let indexed: Option<f64> = command(&pool, ::redis::cmd("ZSCORE").arg(INDEX).arg(DIGEST)).await;
    assert_eq!(indexed, None);
    // A resend (the reply was lost): nothing done, no lapse counted.
    let reply: (i64, i64) = run(&pool, &ACK_UNIQUE, &keys, &args).await;
    assert_eq!(reply, (0, 0));
    assert_eq!(lapses(&pool).await, 0);
    // The job's copy acknowledged while a duplicate holds the lock: left to it, lapse counted.
    take(&pool, OTHER).await;
    put_in_progress(&pool).await;
    let reply: (i64, i64) = run(&pool, &ACK_UNIQUE, &keys, &args).await;
    assert_eq!(reply, (1, -1));
    assert_eq!(holder_of(&pool).await.as_deref(), Some(OTHER));
    assert_eq!(lapses(&pool).await, 1);
    // A `start` job finds its lock released at its start: no lapse.
    let _: () = command(&pool, ::redis::cmd("DEL").arg(LOCK)).await;
    put_in_progress(&pool).await;
    let mut start = vec![FETCHED];
    start.extend(lock_args("release", JID, "start"));
    let reply: (i64, i64) = run(&pool, &ACK_UNIQUE, &keys, &start).await;
    assert_eq!(reply, (1, 0));
    assert_eq!(lapses(&pool).await, 1);
    // A lock key of the wrong type never aborts the acknowledgement.
    let _: () = command(&pool, ::redis::cmd("SET").arg(LOCK).arg("x")).await;
    put_in_progress(&pool).await;
    let reply: (i64, i64) = run(&pool, &ACK_UNIQUE, &keys, &args).await;
    assert_eq!(reply, (1, -2));
    let left: i64 = command(&pool, ::redis::cmd("LLEN").arg(INPROGRESS)).await;
    assert_eq!(left, 0);
}

#[tokio::test]
async fn settle_holds_through_a_retry_and_releases_at_death() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    take(&pool, JID).await;
    put_in_progress(&pool).await;
    let now: f64 = crate::payload::now_secs_f64();
    let retry_at = format!("{}", now + 600.0);
    let keys = [INPROGRESS, "retry", LOCK, INDEX, EVENTS];
    let mut args = vec![FETCHED, "retry", "{\"retried\":1}", &retry_at, "0", "0"];
    args.extend(lock_args("hold", JID, "success"));
    let reply: (i64, i64) = run(&pool, &SETTLE_UNIQUE, &keys, &args).await;
    assert_eq!(reply, (1, 1));
    // Held until the retry plus `for`, never shortened, without `at` (not replaceable).
    let ttl: i64 = command(&pool, ::redis::cmd("PTTL").arg(LOCK)).await;
    assert!(ttl > 655_000 && ttl <= 660_000, "{ttl}");
    let at: Option<String> = command(&pool, ::redis::cmd("HGET").arg(LOCK).arg("at")).await;
    assert_eq!(at, None);
    let retries: i64 = command(&pool, ::redis::cmd("ZCARD").arg("retry")).await;
    assert_eq!(retries, 1);
    let left: i64 = command(&pool, ::redis::cmd("LLEN").arg(INPROGRESS)).await;
    assert_eq!(left, 0, "removed from the in-progress list after the write");
    // A resend writes nothing more.
    let reply: (i64, i64) = run(&pool, &SETTLE_UNIQUE, &keys, &args).await;
    assert_eq!(reply, (0, 0));
    // To the morgue: written, trimmed, the lock released.
    put_in_progress(&pool).await;
    let dead = ["dead", LOCK, INDEX, EVENTS];
    let now_text = format!("{now}");
    let mut args = vec![FETCHED, "dead", "{\"died\":1}", &now_text, "0", "-10000"];
    args.extend(lock_args("release", JID, "success"));
    let keys = [INPROGRESS, dead[0], dead[1], dead[2], dead[3]];
    let reply: (i64, i64) = run(&pool, &SETTLE_UNIQUE, &keys, &args).await;
    assert_eq!(reply, (1, 1));
    assert_eq!(holder_of(&pool).await, None);
    let dead_jobs: i64 = command(&pool, ::redis::cmd("ZCARD").arg("dead")).await;
    assert_eq!(dead_jobs, 1);
    // A retry of a job whose lock expired takes it back and counts the lapse.
    put_in_progress(&pool).await;
    let keys = [INPROGRESS, "retry", LOCK, INDEX, EVENTS];
    let mut args = vec![FETCHED, "retry", "{\"retried\":2}", &retry_at, "0", "0"];
    args.extend(lock_args("hold", JID, "success"));
    let reply: (i64, i64) = run(&pool, &SETTLE_UNIQUE, &keys, &args).await;
    assert_eq!(reply, (1, 2));
    assert_eq!(holder_of(&pool).await.as_deref(), Some(JID));
    assert_eq!(lapses(&pool).await, 1);
}

#[tokio::test]
async fn out_of_memory_refuses_an_outcome_before_any_write() {
    // The outcome's write is the script's first: refused under OOM, the job stays in
    // progress with its lock, and the outcome can be written once memory frees.
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    take(&pool, JID).await;
    put_in_progress(&pool).await;
    let _: () = command(
        &pool,
        ::redis::cmd("CONFIG").arg("SET").arg("maxmemory").arg("1"),
    )
    .await;
    let retry_at = format!("{}", crate::payload::now_secs_f64() + 60.0);
    let keys = [INPROGRESS, "retry", LOCK, INDEX, EVENTS];
    let mut args = vec![FETCHED, "retry", "{}", &retry_at, "0", "0"];
    args.extend(lock_args("hold", JID, "success"));
    let script = SETTLE_UNIQUE.script();
    let mut invocation = script.prepare_invoke();
    for key in keys {
        invocation.key(key);
    }
    for arg in &args {
        invocation.arg(*arg);
    }
    let mut conn = pool.conn().await.unwrap();
    let refused: Result<Value, _> = invocation.invoke_async(&mut conn).await;
    let error = refused.unwrap_err();
    assert_eq!(error.code(), Some("OOM"), "{error}");
    let _: () = command(
        &pool,
        ::redis::cmd("CONFIG").arg("SET").arg("maxmemory").arg("0"),
    )
    .await;
    let left: i64 = command(&pool, ::redis::cmd("LLEN").arg(INPROGRESS)).await;
    assert_eq!(left, 1, "still in progress");
    assert_eq!(holder_of(&pool).await.as_deref(), Some(JID));
    let reply: (i64, i64) = run(&pool, &SETTLE_UNIQUE, &keys, &args).await;
    assert_eq!(reply, (1, 1));
}

#[tokio::test]
async fn push_back_holds_and_records_the_schedule_score() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    take(&pool, JID).await;
    put_in_progress(&pool).await;
    let at = format!("{}", crate::payload::now_secs_f64() + 300.0);
    let keys = [INPROGRESS, "queues", "schedule", LOCK, INDEX, EVENTS];
    let mut args = vec![FETCHED, "{\"copy\":1}", &at, "schedule"];
    args.extend(lock_args("hold", JID, "success"));
    let reply: (i64, i64) = run(&pool, &PUSH_BACK_UNIQUE, &keys, &args).await;
    assert_eq!(reply, (1, 1));
    let recorded: String = command(&pool, ::redis::cmd("HGET").arg(LOCK).arg("at")).await;
    assert_eq!(recorded, at, "a debounce push may replace the parked job");
    let member: String = command(&pool, ::redis::cmd("HGET").arg(LOCK).arg("m")).await;
    assert_eq!(
        member, "{\"copy\":1}",
        "the lock records the parked job's member"
    );
    let ttl: i64 = command(&pool, ::redis::cmd("PTTL").arg(LOCK)).await;
    assert!(ttl > 355_000 && ttl <= 360_000, "{ttl}");
    let reply: (i64, i64) = run(&pool, &PUSH_BACK_UNIQUE, &keys, &args).await;
    assert_eq!(reply, (0, 0), "a resend writes nothing");
    let scheduled: i64 = command(&pool, ::redis::cmd("ZCARD").arg("schedule")).await;
    assert_eq!(scheduled, 1);
    // Back to a queue: the recorded score is cleared.
    put_in_progress(&pool).await;
    let keys = [INPROGRESS, "queues", "queue:q", LOCK, INDEX, EVENTS];
    let mut args = vec![FETCHED, "{\"copy\":2}", "q", "queue"];
    args.extend(lock_args("hold", JID, "success"));
    let reply: (i64, i64) = run(&pool, &PUSH_BACK_UNIQUE, &keys, &args).await;
    assert_eq!(reply, (1, 1));
    let recorded: Option<String> = command(&pool, ::redis::cmd("HGET").arg(LOCK).arg("at")).await;
    assert_eq!(recorded, None);
    let member: Option<String> = command(&pool, ::redis::cmd("HGET").arg(LOCK).arg("m")).await;
    assert_eq!(member, None, "the member is cleared with the score");
    // A `start` job takes its released lock back.
    let _: () = command(&pool, ::redis::cmd("DEL").arg(LOCK)).await;
    put_in_progress(&pool).await;
    let mut args = vec![FETCHED, "{\"copy\":3}", "q", "queue"];
    args.extend(lock_args("hold", JID, "start"));
    let reply: (i64, i64) = run(&pool, &PUSH_BACK_UNIQUE, &keys, &args).await;
    assert_eq!(reply, (1, 2));
    assert_eq!(holder_of(&pool).await.as_deref(), Some(JID));
    assert_eq!(lapses(&pool).await, 0);
}

#[tokio::test]
async fn lock_op_and_release_compare_the_holder() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    take(&pool, OTHER).await;
    let keys = [LOCK, INDEX, EVENTS];
    let hold: i64 = run(
        &pool,
        &UNIQUE_LOCK_OP,
        &keys,
        &["hold", JID, DIGEST, "C", "q", "success", "60000", "", ""],
    )
    .await;
    assert_eq!(hold, 0, "another holder's lock is left alone");
    let release: i64 = run(&pool, &UNIQUE_RELEASE, &keys, &[JID, DIGEST, "C", "1"]).await;
    assert_eq!(release, -1);
    assert_eq!(holder_of(&pool).await.as_deref(), Some(OTHER));
    assert_eq!(lapses(&pool).await, 2);
    let release: i64 = run(&pool, &UNIQUE_RELEASE, &keys, &[OTHER, DIGEST, "C", "0"]).await;
    assert_eq!(release, 1);
    let again: i64 = run(&pool, &UNIQUE_RELEASE, &keys, &[OTHER, DIGEST, "C", "0"]).await;
    assert_eq!(again, 0, "a repeated release is a no-op");
}

#[tokio::test]
async fn lock_op_records_the_member_of_a_job_waiting_in_the_schedule() {
    // unique-1, basic fetch: the hold before a job is parked records its score and member, as
    // PUSH_BACK_UNIQUE does; a hold for a queue or a retry clears both; a lock taken back
    // records them too.
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    take(&pool, JID).await;
    let keys = [LOCK, INDEX, EVENTS];
    let at = format!("{}", crate::payload::now_secs_f64() + 300.0);
    let parked = r#"{"class":"C","jid":"aaaaaaaaaaaaaaaaaaaaaaaa","parked":1}"#;
    let mut args = lock_args("hold", JID, "success").to_vec();
    args.extend([at.as_str(), at.as_str(), parked]);
    let hold: i64 = run(&pool, &UNIQUE_LOCK_OP, &keys, &args).await;
    assert_eq!(hold, 1);
    let fields: Vec<Option<String>> =
        command(&pool, ::redis::cmd("HMGET").arg(LOCK).arg("at").arg("m")).await;
    assert_eq!(fields, [Some(at.clone()), Some(parked.to_owned())]);
    // Held for a retry: neither is kept (a retry is never replaced).
    let mut args = lock_args("hold", JID, "success").to_vec();
    args.extend([at.as_str(), "", ""]);
    let hold: i64 = run(&pool, &UNIQUE_LOCK_OP, &keys, &args).await;
    assert_eq!(hold, 1);
    let fields: Vec<Option<String>> =
        command(&pool, ::redis::cmd("HMGET").arg(LOCK).arg("at").arg("m")).await;
    assert_eq!(fields, [None, None]);
    // A free lock taken back by a parked job records them.
    let _: () = command(&pool, ::redis::cmd("DEL").arg(LOCK)).await;
    let mut args = lock_args("hold", JID, "start").to_vec();
    args.extend([at.as_str(), at.as_str(), parked]);
    let hold: i64 = run(&pool, &UNIQUE_LOCK_OP, &keys, &args).await;
    assert_eq!(hold, 2);
    let fields: Vec<Option<String>> =
        command(&pool, ::redis::cmd("HMGET").arg(LOCK).arg("at").arg("m")).await;
    assert_eq!(fields, [Some(at.clone()), Some(parked.to_owned())]);
    // Recorded without a member (an older caller): the score alone, no stale member.
    let mut args = lock_args("hold", JID, "start").to_vec();
    args.extend([at.as_str(), at.as_str()]);
    let hold: i64 = run(&pool, &UNIQUE_LOCK_OP, &keys, &args).await;
    assert_eq!(hold, 1);
    let fields: Vec<Option<String>> =
        command(&pool, ::redis::cmd("HMGET").arg(LOCK).arg("at").arg("m")).await;
    assert_eq!(fields, [Some(at), None]);
}

#[tokio::test]
async fn replace_finds_the_recorded_member_among_many_at_its_score() {
    // unique-1 at the script level: 300 jobs share the holder's score and sort before it.
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let at = "4102477200";
    for n in 0..300 {
        let _: i64 = command(
            &pool,
            ::redis::cmd("ZADD")
                .arg("schedule")
                .arg(at)
                .arg(format!("{{\"class\":\"A\",\"n\":{n}}}")),
        )
        .await;
    }
    let holder = format!("{{\"class\":\"C\",\"jid\":\"{JID}\",\"v\":1}}");
    let scheduled = |jid: &str, json: &str, conflict: &str| {
        let mut args = vec!["1".to_owned()];
        args.extend(
            [
                jid, json, "schedule", at, "q", "success", "60000", at, conflict, "C", DIGEST,
            ]
            .iter()
            .map(|arg| (*arg).to_owned()),
        );
        args
    };
    let keys = ["queues", "schedule", INDEX, EVENTS, LOCK, "schedule"];
    let args = scheduled(JID, &holder, "drop");
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    let first: Vec<(i64, String)> = run(&pool, &UNIQUE_PUSH, &keys, &args).await;
    assert_eq!(first, [(1, String::new())]);
    let member: String = command(&pool, ::redis::cmd("HGET").arg(LOCK).arg("m")).await;
    assert_eq!(member, holder);
    let newer = format!("{{\"class\":\"C\",\"jid\":\"{OTHER}\",\"v\":2}}");
    let args = scheduled(OTHER, &newer, "replace");
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    let replaced: Vec<(i64, String)> = run(&pool, &UNIQUE_PUSH, &keys, &args).await;
    assert_eq!(replaced, [(3, JID.to_owned())]);
    let gone: Option<f64> =
        command(&pool, ::redis::cmd("ZSCORE").arg("schedule").arg(&holder)).await;
    assert_eq!(gone, None);
    let member: String = command(&pool, ::redis::cmd("HGET").arg(LOCK).arg("m")).await;
    assert_eq!(member, newer, "the new holder's member is recorded");
    assert_eq!(holder_of(&pool).await.as_deref(), Some(OTHER));
    // Without a recorded member, the holder is looked for among the first 100 members at its
    // score only: here it sorts after 300 others.
    let _: i64 = command(&pool, ::redis::cmd("HDEL").arg(LOCK).arg("m")).await;
    let third = format!("{{\"class\":\"C\",\"jid\":\"{}\",\"v\":3}}", "c".repeat(24));
    let args = scheduled(&"c".repeat(24), &third, "replace");
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    let refused: Vec<(i64, String)> = run(&pool, &UNIQUE_PUSH, &keys, &args).await;
    assert_eq!(refused, [(0, OTHER.to_owned())]);
}

#[tokio::test]
async fn renewals_extend_held_locks_and_never_create_one() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let short = format!(
        "hu_take(KEYS[1], KEYS[2], 'd1', ARGV[1], 1000, h_now_ms(), {FIELDS}, '') return 1"
    );
    let _: Value = call(&pool, &short, &[JID]).await;
    let lapsed: Vec<i64> = run(
        &pool,
        &UNIQUE_RENEW,
        &[INDEX, LOCK, "unique:gone"],
        &[JID, DIGEST, "60000", JID, "gone", "60000"],
    )
    .await;
    assert_eq!(lapsed, [2], "the second job no longer holds its lock");
    let ttl: i64 = command(&pool, ::redis::cmd("PTTL").arg(LOCK)).await;
    assert!(ttl > 59_000, "{ttl}");
    let exists: i64 = command(&pool, ::redis::cmd("EXISTS").arg("unique:gone")).await;
    assert_eq!(exists, 0, "never created");
    // Another job's lock is not extended.
    let lapsed: Vec<i64> = run(
        &pool,
        &UNIQUE_RENEW,
        &[INDEX, LOCK],
        &[OTHER, DIGEST, "900000"],
    )
    .await;
    assert_eq!(lapsed, [1]);
    let ttl: i64 = command(&pool, ::redis::cmd("PTTL").arg(LOCK)).await;
    assert!(ttl <= 60_000, "{ttl}");
}

#[tokio::test]
async fn removals_release_the_lock_with_the_job() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    take(&pool, JID).await;
    let _: () = command(
        &pool,
        ::redis::cmd("ZADD").arg("schedule").arg(5).arg(FETCHED),
    )
    .await;
    let reply: (i64, i64) = run(
        &pool,
        &REMOVE_UNIQUE,
        &["schedule", LOCK, INDEX],
        &["zset", FETCHED, JID, DIGEST],
    )
    .await;
    assert_eq!(reply, (1, 1));
    // Not there: nothing done.
    take(&pool, JID).await;
    let reply: (i64, i64) = run(
        &pool,
        &REMOVE_UNIQUE,
        &["schedule", LOCK, INDEX],
        &["zset", FETCHED, JID, DIGEST],
    )
    .await;
    assert_eq!(reply, (0, 0));
    assert_eq!(holder_of(&pool).await.as_deref(), Some(JID));
    // From a queue, while another job holds the lock: removed, the lock left alone.
    let _: () = command(&pool, ::redis::cmd("DEL").arg(LOCK)).await;
    take(&pool, OTHER).await;
    let _: () = command(&pool, ::redis::cmd("LPUSH").arg("queue:q").arg(FETCHED)).await;
    let reply: (i64, i64) = run(
        &pool,
        &REMOVE_UNIQUE,
        &["queue:q", LOCK, INDEX],
        &["list", FETCHED, JID, DIGEST],
    )
    .await;
    assert_eq!(reply, (1, -1));
    assert_eq!(holder_of(&pool).await.as_deref(), Some(OTHER));
    // Killing: moved to the morgue (trimmed), then released.
    let _: () = command(&pool, ::redis::cmd("DEL").arg(LOCK)).await;
    take(&pool, JID).await;
    let _: () = command(&pool, ::redis::cmd("ZADD").arg("retry").arg(5).arg(FETCHED)).await;
    let reply: (i64, i64) = run(
        &pool,
        &KILL_UNIQUE,
        &["retry", "dead", LOCK, INDEX],
        &[FETCHED, "100", "0", "-10000", JID, DIGEST],
    )
    .await;
    assert_eq!(reply, (1, 1));
    let dead: i64 = command(&pool, ::redis::cmd("ZCARD").arg("dead")).await;
    assert_eq!(dead, 1);
    let reply: (i64, i64) = run(
        &pool,
        &KILL_UNIQUE,
        &["retry", "dead", LOCK, INDEX],
        &[FETCHED, "100", "0", "-10000", JID, DIGEST],
    )
    .await;
    assert_eq!(reply, (0, 0), "a resend finds nothing to move");
}

#[tokio::test]
async fn release_many_and_tidy_respect_holders_and_live_locks() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    take(&pool, JID).await;
    let _: () = command(
        &pool,
        ::redis::cmd("ZADD").arg(INDEX).arg(9.0e12).arg("stale"),
    )
    .await;
    // Wrong holder, wrong class: kept. Missing lock: its entry removed.
    let released: i64 = run(
        &pool,
        &UNIQUE_RELEASE_MANY,
        &[INDEX, LOCK, "unique:stale"],
        &[OTHER, DIGEST, "", "", "stale", ""],
    )
    .await;
    assert_eq!(released, 0);
    assert_eq!(holder_of(&pool).await.as_deref(), Some(JID));
    let stale: Option<f64> = command(&pool, ::redis::cmd("ZSCORE").arg(INDEX).arg("stale")).await;
    assert_eq!(stale, None);
    let released: i64 = run(
        &pool,
        &UNIQUE_RELEASE_MANY,
        &[INDEX, LOCK],
        &["", DIGEST, "Other"],
    )
    .await;
    assert_eq!(released, 0, "class filter");
    let released: i64 = run(
        &pool,
        &UNIQUE_RELEASE_MANY,
        &[INDEX, LOCK],
        &["", DIGEST, "C"],
    )
    .await;
    assert_eq!(released, 1);
    // Tidy removes only the entries whose lock is gone.
    take(&pool, JID).await;
    let _: () = command(&pool, ::redis::cmd("ZADD").arg(INDEX).arg(1).arg("gone")).await;
    let removed: i64 = run(
        &pool,
        &UNIQUE_TIDY,
        &[INDEX, LOCK, "unique:gone"],
        &[DIGEST, "gone"],
    )
    .await;
    assert_eq!(removed, 1);
    let live: Option<f64> = command(&pool, ::redis::cmd("ZSCORE").arg(INDEX).arg(DIGEST)).await;
    assert!(live.is_some());
}

/// The arguments of one job of `UNIQUE_PUSH`.
fn push_args(jid: &str, json: &str, conflict: &str) -> Vec<String> {
    [
        jid, json, "queue", "q", "q", "success", "60000", "", conflict, "C", DIGEST,
    ]
    .iter()
    .map(|arg| (*arg).to_owned())
    .collect()
}

#[tokio::test]
async fn concurrent_pushes_of_one_digest_take_one_lock() {
    // G1: pushes of the same digest from many connections at once: exactly one is written.
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let mut tasks = Vec::new();
    for n in 0..32 {
        let pool = redis.pool().await;
        tasks.push(tokio::spawn(async move {
            let jid = format!("{n:024x}");
            let json = format!("{{\"jid\":\"{jid}\"}}");
            let mut args = vec!["1".to_owned()];
            args.extend(push_args(&jid, &json, "drop"));
            let args: Vec<&str> = args.iter().map(String::as_str).collect();
            let reply: Vec<(i64, String)> = run(
                &pool,
                &UNIQUE_PUSH,
                &["queues", "schedule", INDEX, EVENTS, LOCK, "queue:q"],
                &args,
            )
            .await;
            reply[0].0
        }));
    }
    let mut pushed = 0;
    for task in tasks {
        if task.await.unwrap() == 1 {
            pushed += 1;
        }
    }
    assert_eq!(pushed, 1);
    let queued: i64 = command(&pool, ::redis::cmd("LLEN").arg("queue:q")).await;
    assert_eq!(queued, 1);
    let duplicates: i64 = command(&pool, ::redis::cmd("HGET").arg(EVENTS).arg("C|d")).await;
    assert_eq!(duplicates, 31);
}

#[tokio::test]
async fn a_resent_push_chunk_is_already_pushed() {
    // G5 at the script level: the same chunk twice writes each job once.
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let mut args = vec!["1".to_owned()];
    args.extend(push_args(JID, FETCHED, "drop"));
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    let keys = ["queues", "schedule", INDEX, EVENTS, LOCK, "queue:q"];
    let first: Vec<(i64, String)> = run(&pool, &UNIQUE_PUSH, &keys, &args).await;
    let again: Vec<(i64, String)> = run(&pool, &UNIQUE_PUSH, &keys, &args).await;
    assert_eq!(first, [(1, String::new())]);
    assert_eq!(again, [(2, JID.to_owned())]);
    let queued: i64 = command(&pool, ::redis::cmd("LLEN").arg("queue:q")).await;
    assert_eq!(queued, 1);
}
