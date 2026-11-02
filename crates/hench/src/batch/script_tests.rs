//! The batch scripts against a private `redis-server` (`docs/design/batches.md` §3.2): the
//! scenarios of the design's lab (S1 to S18), `OOM`, the expiry of every key, a callback queue
//! of the wrong type, deletes, the per-minute counters, the dispatch's `enqueued_at` surgery,
//! and the unique section of `BATCH_COMMIT` against the unique jobs' frozen library.

use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use serde_json::json;

use crate::batch::dispatch::{dispatch_chain, refresh_ancestors};
use crate::batch::lua::BATCH_COMMIT;
use crate::batch::maintenance::Round;
use crate::batch::scripts::{
    self, Accounted, CommitBatch, CommitJob, CommitMode, CommitReply, Outcome, Template,
    UniqueMember, commit_refusal, parse_commit,
};
use crate::batch::{BatchCallback, BatchError, BatchEvent};
use crate::client::Stored;
use crate::error::{Error, Result};
use crate::keys;
use crate::payload::JobPayload;
use crate::redis::RedisPool;
use crate::test_util::TestRedis;
use crate::unique::{OnConflict, UniqueUntil};

fn bid(n: u64) -> String {
    format!("{n:024x}")
}

fn jid(n: u64) -> String {
    format!("j{n:023x}")
}

fn batch(mode: CommitMode, bid: &str, parent: usize) -> CommitBatch {
    CommitBatch {
        mode,
        bid: bid.to_owned(),
        parent,
        ttl_secs: 2_592_000,
        linger_secs: 86_400,
        description: String::new(),
        callbacks: Default::default(),
        extra: Vec::new(),
    }
}

fn new_batch(bid: &str) -> CommitBatch {
    batch(CommitMode::New, bid, 0)
}

fn open_batch(bid: &str) -> CommitBatch {
    batch(CommitMode::Open, bid, 0)
}

/// A member of batch `index` of the commit, queued in `queue`.
fn queued(index: usize, jid: &str, queue: &str, bid: &str) -> CommitJob {
    CommitJob {
        batch: index,
        jid: jid.to_owned(),
        stored: Stored::Queued {
            queue: queue.to_owned(),
            json: json!({"class": "W", "args": [], "jid": jid, "queue": queue, "bid": bid})
                .to_string(),
        },
        unique: None,
    }
}

/// A member of batch `index` of the commit, scheduled at `at`.
fn scheduled(index: usize, jid: &str, at: f64, bid: &str) -> CommitJob {
    CommitJob {
        batch: index,
        jid: jid.to_owned(),
        stored: Stored::Scheduled {
            at,
            json: json!({"class": "W", "args": [], "jid": jid, "queue": "q1", "bid": bid})
                .to_string(),
        },
        unique: None,
    }
}

/// The callback template of job `class` with `jid`, for `event` of batch `of`, member of
/// `parent`.
fn template(
    class: &str,
    jid: &str,
    queue: &str,
    parent: Option<&str>,
    of: &str,
    event: &str,
) -> Template {
    let mut job = json!({
        "class": class, "args": [42], "jid": jid, "queue": queue, "retry": true,
        "created_at": 1_730_000_000_000_i64,
        "batch_callback": {"bid": of, "event": event},
    });
    if let Some(parent) = parent {
        job["bid"] = json!(parent);
    }
    Template {
        q: queue.to_owned(),
        jid: jid.to_owned(),
        j: job.to_string(),
    }
}

fn templates(templates: &[Template]) -> String {
    serde_json::to_string(templates).unwrap()
}

/// Sends a commit (once).
async fn commit(
    pool: &RedisPool,
    cid: &str,
    batches: &[CommitBatch],
    jobs: &[CommitJob],
) -> Result<CommitReply> {
    let (keys, argv) = scripts::commit_call(cid, batches, jobs, Utc::now());
    match scripts::run(pool, &BATCH_COMMIT, &keys, &argv).await {
        Ok(reply) => parse_commit(reply),
        Err(Error::Redis(error)) => {
            Err(commit_refusal(&error, jobs).unwrap_or(Error::Redis(error)))
        }
        Err(error) => Err(error),
    }
}

/// Commits `root` with `n` members `jid(1..=n)` in `queue:q1`.
async fn new_with_members(pool: &RedisPool, cid: &str, root: CommitBatch, n: u64) -> CommitReply {
    let jobs: Vec<CommitJob> = (1..=n)
        .map(|index| queued(1, &jid(index), "q1", &root.bid))
        .collect();
    commit(pool, cid, &[root], &jobs).await.unwrap()
}

fn applied(fired: &[&str]) -> CommitReply {
    CommitReply::Applied {
        fired: fired.iter().map(|bid| (*bid).to_owned()).collect(),
        duplicates: Vec::new(),
        unlocked: Vec::new(),
    }
}

async fn succeeded(pool: &RedisPool, bid: &str, jid: &str) -> Accounted {
    scripts::account(pool, bid, jid, &Outcome::Succeeded)
        .await
        .unwrap()
}

async fn failed(pool: &RedisPool, bid: &str, jid: &str, retry: bool) -> Accounted {
    let state = if retry { "retry" } else { "dead" };
    let outcome = Outcome::Failed {
        retry,
        summary: json!({"class": "W", "state": state}).to_string(),
    };
    scripts::account(pool, bid, jid, &outcome).await.unwrap()
}

async fn skipped(pool: &RedisPool, bid: &str, jid: &str, cancelled: bool) -> Accounted {
    scripts::account(pool, bid, jid, &Outcome::Skipped { cancelled })
        .await
        .unwrap()
}

/// `(applied, dispatch, fired)` of an accounting.
fn short(accounted: &Accounted) -> (bool, bool, Vec<&str>) {
    (
        accounted.applied,
        accounted.dispatch,
        accounted.fired.iter().map(String::as_str).collect(),
    )
}

async fn cmd<T: ::redis::FromRedisValue>(pool: &RedisPool, command: &::redis::Cmd) -> T {
    let mut conn = pool.conn().await.unwrap();
    command.query_async(&mut conn).await.unwrap()
}

async fn hget(pool: &RedisPool, bid: &str, field: &str) -> Option<String> {
    cmd(pool, ::redis::cmd("HGET").arg(keys::batch(bid)).arg(field)).await
}

/// `(pending, failed, dead, total)` of a batch.
async fn counts(pool: &RedisPool, bid: &str) -> (u64, u64, u64, u64) {
    let pending: u64 = cmd(pool, ::redis::cmd("SCARD").arg(keys::batch_pending(bid))).await;
    let failed: u64 = cmd(pool, ::redis::cmd("HLEN").arg(keys::batch_failed(bid))).await;
    let dead: u64 = cmd(pool, ::redis::cmd("SCARD").arg(keys::batch_dead(bid))).await;
    let total = hget(pool, bid, "total")
        .await
        .and_then(|total| total.parse().ok())
        .unwrap_or(0);
    (pending, failed, dead, total)
}

async fn pttl(pool: &RedisPool, key: &str) -> i64 {
    cmd(pool, ::redis::cmd("PTTL").arg(key)).await
}

async fn redis_now(pool: &RedisPool) -> u64 {
    let (secs, _): (u64, u64) = cmd(pool, &::redis::cmd("TIME")).await;
    secs
}

async fn in_outbox(pool: &RedisPool, bid: &str) -> bool {
    let score: Option<f64> = cmd(
        pool,
        ::redis::cmd("ZSCORE").arg(keys::BATCH_EVENTS).arg(bid),
    )
    .await;
    score.is_some()
}

/// The jids of the jobs of `queue`, oldest first.
async fn queue_jids(pool: &RedisPool, queue: &str) -> Vec<String> {
    let jobs: Vec<String> = cmd(
        pool,
        ::redis::cmd("LRANGE")
            .arg(keys::queue(queue))
            .arg(0)
            .arg(-1),
    )
    .await;
    jobs.iter()
        .rev()
        .map(|job| {
            serde_json::from_str::<serde_json::Value>(job).unwrap()["jid"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect()
}

/// Every key of the batches has an expiry, and each batch's index score matches its hash's
/// expiry (± 1.5 s): the TTL rule of every script (Pro #1057, #2889).
async fn assert_every_key_expires(redis: &TestRedis, pool: &RedisPool) {
    let without = redis.keys_without_ttl(&["batch:*", "b|*"]).await;
    assert!(without.is_empty(), "keys without expiry: {without:?}");
    let index: Vec<(String, f64)> = cmd(
        pool,
        ::redis::cmd("ZRANGE")
            .arg(keys::BATCHES)
            .arg(0)
            .arg(-1)
            .arg("WITHSCORES"),
    )
    .await;
    #[allow(clippy::cast_precision_loss)]
    let now = redis_now(pool).await as f64;
    for (bid, score) in index {
        let ttl = pttl(pool, &keys::batch(&bid)).await;
        if ttl < 0 {
            continue;
        }
        #[allow(clippy::cast_precision_loss)]
        let expiry = now + ttl as f64 / 1_000.0;
        assert!(
            (expiry - score).abs() < 1.5,
            "index score {score} of {bid} vs expiry {expiry}"
        );
    }
}

#[tokio::test]
async fn s1_success_dispatch_and_linger() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let b = bid(1);
    let mut root = new_batch(&b);
    root.callbacks[1] = templates(&[template("Done", "cb1", "cbq", None, &b, "success")]);
    assert_eq!(new_with_members(&pool, "c1", root, 3).await, applied(&[]));
    assert_eq!(queue_jids(&pool, "q1").await.len(), 3);
    assert_eq!(counts(&pool, &b).await, (3, 0, 0, 3));
    assert!(pttl(&pool, &keys::batch(&b)).await > 2_591_000_000);
    assert_every_key_expires(&redis, &pool).await;
    assert_eq!(
        short(&succeeded(&pool, &b, &jid(1)).await),
        (true, false, vec![])
    );
    assert_eq!(
        short(&succeeded(&pool, &b, &jid(2)).await),
        (true, false, vec![])
    );
    assert_eq!(
        short(&succeeded(&pool, &b, &jid(3)).await),
        (true, true, vec!["complete", "success"])
    );
    // A repeat is a no-op, and says that a dispatch is pending.
    assert_eq!(
        short(&succeeded(&pool, &b, &jid(3)).await),
        (false, true, vec![])
    );
    assert_eq!(dispatch_chain(&pool, &b).await.unwrap(), 1);
    let pushed: Vec<String> = cmd(
        &pool,
        ::redis::cmd("LRANGE").arg("queue:cbq").arg(0).arg(-1),
    )
    .await;
    assert_eq!(pushed.len(), 1);
    let callback: serde_json::Value = serde_json::from_str(&pushed[0]).unwrap();
    assert_eq!(callback["jid"], "cb1");
    assert!(callback["enqueued_at"].as_i64().unwrap() > 1_700_000_000_000);
    assert_eq!(
        callback["batch_callback"],
        json!({"bid": b, "event": "success"})
    );
    assert!(callback.get("bid").is_none());
    assert!(!in_outbox(&pool, &b).await);
    // A second dispatch is a no-op.
    assert_eq!(dispatch_chain(&pool, &b).await.unwrap(), 0);
    assert_eq!(queue_jids(&pool, "cbq").await.len(), 1);
    assert!(pttl(&pool, &keys::batch(&b)).await <= 86_400_000);
    assert!(hget(&pool, &b, "success_dt").await.is_some());
    assert!(hget(&pool, &b, "finished_at").await.is_some());
    assert_every_key_expires(&redis, &pool).await;
}

#[tokio::test]
async fn s2_complete_with_a_retrying_member_then_success() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let b = bid(2);
    new_with_members(&pool, "c1", new_batch(&b), 3).await;
    assert_eq!(
        short(&failed(&pool, &b, &jid(1), true).await),
        (true, false, vec![])
    );
    assert_eq!(counts(&pool, &b).await.1, 1);
    succeeded(&pool, &b, &jid(2)).await;
    assert_eq!(
        short(&succeeded(&pool, &b, &jid(3)).await),
        (true, true, vec!["complete"])
    );
    dispatch_chain(&pool, &b).await.unwrap();
    assert_eq!(
        short(&succeeded(&pool, &b, &jid(1)).await),
        (true, true, vec!["success"])
    );
    assert_eq!(counts(&pool, &b).await.1, 0);
    assert_every_key_expires(&redis, &pool).await;
}

#[tokio::test]
async fn s3_death_fires_once_and_revival_succeeds() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let b = bid(3);
    new_with_members(&pool, "c1", new_batch(&b), 3).await;
    failed(&pool, &b, &jid(1), true).await;
    assert_eq!(
        short(&failed(&pool, &b, &jid(1), false).await),
        (true, true, vec!["death"])
    );
    dispatch_chain(&pool, &b).await.unwrap();
    assert_eq!(
        short(&failed(&pool, &b, &jid(2), false).await),
        (true, false, vec![])
    );
    assert_eq!(
        short(&succeeded(&pool, &b, &jid(3)).await),
        (true, true, vec!["complete"])
    );
    dispatch_chain(&pool, &b).await.unwrap();
    assert_eq!(counts(&pool, &b).await, (2, 2, 2, 3));
    assert_eq!(
        short(&succeeded(&pool, &b, &jid(1)).await),
        (true, false, vec![])
    );
    assert_eq!(
        short(&succeeded(&pool, &b, &jid(2)).await),
        (true, true, vec!["success"])
    );
    assert_eq!(counts(&pool, &b).await.2, 0);
}

#[tokio::test]
async fn s4_stale_duplicates_are_ignored() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let b = bid(4);
    new_with_members(&pool, "c1", new_batch(&b), 3).await;
    succeeded(&pool, &b, &jid(1)).await;
    assert!(!failed(&pool, &b, &jid(1), true).await.applied);
    assert!(!failed(&pool, &b, &jid(1), false).await.applied);
    assert_eq!(hget(&pool, &b, "death_at").await, None);
    assert!(!succeeded(&pool, &b, "unknown").await.applied);
}

#[tokio::test]
async fn s5_commits_apply_once_per_commit_id() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let b = bid(5);
    assert_eq!(
        new_with_members(&pool, "same", new_batch(&b), 3).await,
        applied(&[])
    );
    assert_eq!(
        new_with_members(&pool, "same", new_batch(&b), 3).await,
        CommitReply::Already {
            duplicates: Vec::new()
        }
    );
    assert_eq!(queue_jids(&pool, "q1").await.len(), 3);
    // Another commit id on the bid: the batch exists.
    assert!(matches!(
        commit(&pool, "other", &[new_batch(&b)], &[]).await,
        Err(Error::Batch(BatchError::Exists { .. }))
    ));
}

#[tokio::test]
async fn s6_reopen_rules() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let b = bid(6);
    new_with_members(&pool, "c1", new_batch(&b), 1).await;
    let extra = [queued(1, "extra", "q2", &b)];
    assert_eq!(
        commit(&pool, "c2", &[open_batch(&b)], &extra)
            .await
            .unwrap(),
        applied(&[])
    );
    assert_eq!(counts(&pool, &b).await.3, 2);
    assert!(succeeded(&pool, &b, &jid(1)).await.fired.is_empty());
    assert_eq!(
        succeeded(&pool, &b, "extra").await.fired,
        ["complete", "success"]
    );
    assert!(matches!(
        commit(&pool, "c3", &[open_batch(&b)], &[]).await,
        Err(Error::Batch(BatchError::Succeeded { bid })) if bid == b
    ));
    assert!(matches!(
        commit(&pool, "c4", &[open_batch(&bid(999))], &[]).await,
        Err(Error::Batch(BatchError::NotFound { .. }))
    ));
    let b2 = bid(66);
    new_with_members(&pool, "c5", new_batch(&b2), 1).await;
    assert_eq!(
        commit(&pool, "c6", &[open_batch(&b2)], &[]).await.unwrap(),
        applied(&[])
    );
    assert_eq!(hget(&pool, &b2, "complete_at").await, None);
}

#[tokio::test]
async fn s7_an_empty_batch_fires_at_its_commit() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let b = bid(7);
    let mut root = new_batch(&b);
    root.callbacks[1] = templates(&[template("Done", "cb7", "cbq", None, &b, "success")]);
    assert_eq!(
        commit(&pool, "c1", &[root], &[]).await.unwrap(),
        applied(&[&b])
    );
    dispatch_chain(&pool, &b).await.unwrap();
    assert_eq!(queue_jids(&pool, "cbq").await, ["cb7"]);
    assert_every_key_expires(&redis, &pool).await;
}

#[tokio::test]
async fn s8_child_callbacks_are_members_of_the_parent() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let (p, c) = (bid(80), bid(81));
    let mut parent = new_batch(&p);
    parent.callbacks[1] = templates(&[template("PDone", "pcb", "cbq", None, &p, "success")]);
    let mut child = batch(CommitMode::New, &c, 1);
    child.callbacks[1] = templates(&[template("CDone", "ccb-s", "cbq", Some(&p), &c, "success")]);
    child.callbacks[0] = templates(&[template(
        "CComplete",
        "ccb-c",
        "cbq",
        Some(&p),
        &c,
        "complete",
    )]);
    let jobs = [queued(1, "a", "q1", &p), queued(2, "b", "q1", &c)];
    assert_eq!(
        commit(&pool, "c1", &[parent, child], &jobs).await.unwrap(),
        applied(&[])
    );
    assert_eq!(hget(&pool, &p, "kids").await.as_deref(), Some("1"));
    let kids: Vec<String> = cmd(&pool, ::redis::cmd("SMEMBERS").arg(keys::batch_kids(&p))).await;
    assert_eq!(kids, std::slice::from_ref(&c));
    assert_eq!(hget(&pool, &c, "parent").await, Some(p.clone()));
    assert!(succeeded(&pool, &p, "a").await.fired.is_empty());
    assert_eq!(
        short(&succeeded(&pool, &c, "b").await),
        (true, true, vec!["complete", "success"])
    );
    assert_eq!(dispatch_chain(&pool, &c).await.unwrap(), 1);
    // The child's callbacks are pending members of the parent, which waits for them.
    assert_eq!(counts(&pool, &p).await.0, 2);
    assert_eq!(counts(&pool, &p).await.3, 3);
    assert_eq!(hget(&pool, &p, "kc").await.as_deref(), Some("1"));
    assert_eq!(hget(&pool, &p, "ks").await.as_deref(), Some("1"));
    assert_eq!(hget(&pool, &p, "complete_at").await, None);
    assert!(succeeded(&pool, &p, "ccb-c").await.fired.is_empty());
    assert_eq!(
        succeeded(&pool, &p, "ccb-s").await.fired,
        ["complete", "success"]
    );
    dispatch_chain(&pool, &p).await.unwrap();
    assert_eq!(queue_jids(&pool, "cbq").await.len(), 3);
    assert_every_key_expires(&redis, &pool).await;
}

#[tokio::test]
async fn s9_a_child_without_callbacks_finishes_its_parent_at_dispatch() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let (p, c) = (bid(90), bid(91));
    let jobs = [queued(2, "b", "q1", &c)];
    commit(
        &pool,
        "c1",
        &[new_batch(&p), batch(CommitMode::New, &c, 1)],
        &jobs,
    )
    .await
    .unwrap();
    succeeded(&pool, &c, "b").await;
    // One chain dispatches both: the child, then the parent it finished.
    assert_eq!(dispatch_chain(&pool, &c).await.unwrap(), 2);
    assert!(hget(&pool, &p, "success_at").await.is_some());
    assert!(hget(&pool, &p, "finished_at").await.is_some());
    assert!(!in_outbox(&pool, &p).await);
}

#[tokio::test]
async fn s10_death_propagates_to_the_parent() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let (p, c) = (bid(100), bid(101));
    let mut parent = new_batch(&p);
    parent.callbacks[2] = templates(&[template("PDied", "pdied", "cbq", None, &p, "death")]);
    let jobs = [queued(1, "a", "q1", &p), queued(2, "b", "q1", &c)];
    commit(&pool, "c1", &[parent, batch(CommitMode::New, &c, 1)], &jobs)
        .await
        .unwrap();
    assert_eq!(
        short(&failed(&pool, &c, "b", false).await),
        (true, true, vec!["death", "complete"])
    );
    assert_eq!(dispatch_chain(&pool, &c).await.unwrap(), 2);
    assert!(hget(&pool, &p, "death_at").await.is_some());
    assert_eq!(queue_jids(&pool, "cbq").await, ["pdied"]);
    assert_eq!(hget(&pool, &p, "kc").await.as_deref(), Some("1"));
    assert_eq!(hget(&pool, &p, "ks").await.as_deref(), Some("0"));
}

#[tokio::test]
async fn s11_cancellation_skips_completes_never_succeeds() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let b = bid(110);
    new_with_members(&pool, "c1", new_batch(&b), 3).await;
    let refused = skipped(&pool, &b, &jid(1), true).await;
    assert!(refused.not_cancelled && !refused.applied);
    let cancelled = scripts::cancel(&pool, &b).await.unwrap();
    assert!(cancelled.first);
    assert!(!scripts::cancel(&pool, &b).await.unwrap().first);
    assert!(succeeded(&pool, &b, &jid(1)).await.applied);
    assert_eq!(
        short(&skipped(&pool, &b, &jid(2), true).await),
        (true, false, vec![])
    );
    assert_eq!(
        short(&skipped(&pool, &b, &jid(3), true).await),
        (true, true, vec!["complete"])
    );
    assert_eq!(hget(&pool, &b, "skipped").await.as_deref(), Some("2"));
    assert_eq!(hget(&pool, &b, "success_at").await, None);
    assert!(matches!(
        commit(&pool, "c9", &[open_batch(&b)], &[]).await,
        Err(Error::Batch(BatchError::Cancelled { .. }))
    ));
    // Cancelling a succeeded batch is refused; a missing one is not found.
    let s = bid(111);
    commit(&pool, "c1", &[new_batch(&s)], &[]).await.unwrap();
    assert!(matches!(
        scripts::cancel(&pool, &s).await,
        Err(Error::Batch(BatchError::Succeeded { .. }))
    ));
    assert!(matches!(
        scripts::cancel(&pool, &bid(112)).await,
        Err(Error::Batch(BatchError::NotFound { .. }))
    ));
}

#[tokio::test]
async fn s12_a_destination_of_the_wrong_type_refuses_the_commit_before_any_write() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let b = bid(120);
    cmd::<()>(&pool, ::redis::cmd("SET").arg("queue:q1").arg("oops")).await;
    let error = commit(&pool, "c1", &[new_batch(&b)], &[queued(1, "a", "q1", &b)])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("WRONGTYPE"), "{error}");
    let exists: u64 = cmd(&pool, ::redis::cmd("EXISTS").arg(keys::batch(&b))).await;
    assert_eq!(exists, 0);
}

#[tokio::test]
async fn s13_an_operator_skip_lets_a_dead_batch_succeed() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let b = bid(130);
    new_with_members(&pool, "c1", new_batch(&b), 2).await;
    failed(&pool, &b, &jid(1), false).await;
    dispatch_chain(&pool, &b).await.unwrap();
    succeeded(&pool, &b, &jid(2)).await;
    dispatch_chain(&pool, &b).await.unwrap();
    assert_eq!(
        short(&skipped(&pool, &b, &jid(1), false).await),
        (true, true, vec!["success"])
    );
}

#[tokio::test]
async fn s14_scheduled_members_extend_the_expiry_of_their_batch_and_its_parent() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let (p, c) = (bid(140), bid(141));
    #[allow(clippy::cast_precision_loss)]
    let at = (redis_now(&pool).await + 7 * 86_400) as f64;
    let mut parent = new_batch(&p);
    parent.ttl_secs = 3_600;
    let mut child = batch(CommitMode::New, &c, 1);
    child.ttl_secs = 3_600;
    commit(&pool, "c1", &[parent, child], &[scheduled(2, "s1", at, &c)])
        .await
        .unwrap();
    let week = 7 * 86_400 * 1_000;
    assert!(pttl(&pool, &keys::batch(&c)).await > week);
    // The parent lives at least as long as its child.
    assert!(pttl(&pool, &keys::batch(&p)).await > week);
    let scheduled: u64 = cmd(&pool, ::redis::cmd("ZCARD").arg(keys::SCHEDULE)).await;
    assert_eq!(scheduled, 1);
    assert_every_key_expires(&redis, &pool).await;
}

#[tokio::test]
async fn s15_maintenance_finds_events_left_in_the_outbox() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let b = bid(150);
    let mut root = new_batch(&b);
    root.callbacks[1] = templates(&[template("Done", "cb15", "cbq", None, &b, "success")]);
    new_with_members(&pool, "c1", root, 1).await;
    succeeded(&pool, &b, &jid(1)).await;
    // The process crashed before dispatching: a round of maintenance dispatches it.
    let round = Round {
        redis: &pool,
        identity: "test:1:abc",
        interval: Duration::from_millis(500),
        grace: Duration::ZERO,
    };
    let report = round.run().await.unwrap();
    assert_eq!(report.dispatched, 1);
    assert_eq!(queue_jids(&pool, "cbq").await, ["cb15"]);
    // Another process skips its round while the lock is held; the holder does not.
    let other = Round {
        identity: "test:2:def",
        ..round
    };
    assert!(other.run().await.unwrap().skipped);
    assert!(!round.run().await.unwrap().skipped);
}

#[tokio::test]
async fn maintenance_trims_the_index_of_expired_batches() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    cmd::<()>(
        &pool,
        ::redis::cmd("ZADD")
            .arg(keys::BATCHES)
            .arg(1)
            .arg("gone")
            .arg("+inf")
            .arg("live"),
    )
    .await;
    let round = Round {
        redis: &pool,
        identity: "test:1:abc",
        interval: Duration::from_millis(500),
        grace: Duration::from_secs(10),
    };
    assert_eq!(round.run().await.unwrap().trimmed, 1);
    let left: Vec<String> = cmd(
        &pool,
        ::redis::cmd("ZRANGE").arg(keys::BATCHES).arg(0).arg(-1),
    )
    .await;
    assert_eq!(left, ["live"]);
}

#[tokio::test]
async fn s16_a_finished_cancelled_batch_keeps_its_linger() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let b = bid(160);
    let mut root = new_batch(&b);
    root.linger_secs = 600;
    new_with_members(&pool, "c1", root, 1).await;
    scripts::cancel(&pool, &b).await.unwrap();
    assert_eq!(skipped(&pool, &b, &jid(1), true).await.fired, ["complete"]);
    dispatch_chain(&pool, &b).await.unwrap();
    assert!(pttl(&pool, &keys::batch(&b)).await <= 600_000);
    // A member waiting in the retry set comes due later: its skip keeps the linger.
    cmd::<()>(
        &pool,
        ::redis::cmd("HSET")
            .arg(keys::batch(&b))
            .arg("touched_at")
            .arg(0),
    )
    .await;
    cmd::<()>(
        &pool,
        ::redis::cmd("SADD")
            .arg(keys::batch_pending(&b))
            .arg("late"),
    )
    .await;
    skipped(&pool, &b, "late", true).await;
    assert!(pttl(&pool, &keys::batch(&b)).await <= 600_000);
    assert!(hget(&pool, &b, "finished_at").await.is_some());
}

#[tokio::test]
async fn a_cancel_after_complete_was_dispatched_finishes_the_batch() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let b = bid(161);
    let mut root = new_batch(&b);
    root.linger_secs = 600;
    new_with_members(&pool, "c1", root, 2).await;
    failed(&pool, &b, &jid(1), true).await;
    assert_eq!(succeeded(&pool, &b, &jid(2)).await.fired, ["complete"]);
    dispatch_chain(&pool, &b).await.unwrap();
    assert_eq!(hget(&pool, &b, "finished_at").await, None);
    scripts::cancel(&pool, &b).await.unwrap();
    // Its failed member waits for its retry: the batch keeps its expiry, so that the member is
    // skipped when it comes due, however late (batches-2).
    assert_eq!(hget(&pool, &b, "finished_at").await, None);
    assert!(pttl(&pool, &keys::batch(&b)).await > 2_000_000_000);
    assert_eq!(
        skipped(&pool, &b, &jid(1), true).await.fired,
        Vec::<String>::new()
    );
    assert!(hget(&pool, &b, "finished_at").await.is_some());
    assert!(pttl(&pool, &keys::batch(&b)).await <= 600_000);
    assert_every_key_expires(&redis, &pool).await;

    // A batch with nothing pending (its child completed with a dead member, so it cannot
    // succeed) finishes at its cancel.
    let (p, c) = (bid(162), bid(163));
    let mut parent = new_batch(&p);
    parent.linger_secs = 600;
    let jobs = [queued(2, "c1", "q1", &c)];
    commit(&pool, "c1", &[parent, batch(CommitMode::New, &c, 1)], &jobs)
        .await
        .unwrap();
    assert_eq!(
        failed(&pool, &c, "c1", false).await.fired,
        ["death", "complete"]
    );
    dispatch_chain(&pool, &c).await.unwrap();
    assert!(hget(&pool, &p, "complete_dt").await.is_some());
    assert_eq!(hget(&pool, &p, "finished_at").await, None);
    scripts::cancel(&pool, &p).await.unwrap();
    assert!(hget(&pool, &p, "finished_at").await.is_some());
    assert!(pttl(&pool, &keys::batch(&p)).await <= 600_000);
}

/// batches-2: a member that failed before its batch was cancelled is skipped when its retry
/// comes due, even after the batch's `complete` was dispatched: the batch finishes (and its
/// linger starts) only once nothing is left pending.
#[tokio::test]
async fn a_cancelled_batch_lives_until_its_last_member_is_skipped() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let b = bid(164);
    let mut root = new_batch(&b);
    root.linger_secs = 30;
    new_with_members(&pool, "c1", root, 3).await;
    scripts::cancel(&pool, &b).await.unwrap();
    succeeded(&pool, &b, &jid(1)).await;
    failed(&pool, &b, &jid(2), true).await;
    assert_eq!(
        failed(&pool, &b, &jid(3), false).await.fired,
        ["death", "complete"]
    );
    dispatch_chain(&pool, &b).await.unwrap();
    assert!(hget(&pool, &b, "complete_dt").await.is_some());
    assert_eq!(hget(&pool, &b, "finished_at").await, None);
    assert!(pttl(&pool, &keys::batch(&b)).await > 2_000_000_000);
    // The retry comes due: skipped; the dead member is still pending.
    assert!(skipped(&pool, &b, &jid(2), true).await.applied);
    assert_eq!(hget(&pool, &b, "finished_at").await, None);
    // The dead member revived from the morgue: skipped too, and the batch is finished.
    assert!(skipped(&pool, &b, &jid(3), true).await.applied);
    assert!(hget(&pool, &b, "finished_at").await.is_some());
    assert!(pttl(&pool, &keys::batch(&b)).await <= 30_000);
    assert_every_key_expires(&redis, &pool).await;
}

/// batches-1: a batch held open (`open`: a workflow whose later stages are not defined yet)
/// fires neither `complete` nor `success` when its children did; the reopen that releases the
/// hold fires what holds then, and a cancel releases it too.
#[tokio::test]
async fn a_held_batch_fires_once_its_hold_is_released() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let (w, s1, s2) = (bid(165), bid(166), bid(167));
    let mut overall = new_batch(&w);
    overall.extra = vec![("open".to_owned(), "1".to_owned())];
    commit(
        &pool,
        "c1",
        &[overall, batch(CommitMode::New, &s1, 1)],
        &[queued(2, "a", "q1", &s1)],
    )
    .await
    .unwrap();
    // Stage 1's member fails once: the stage completes, the held workflow does not.
    assert_eq!(failed(&pool, &s1, "a", true).await.fired, ["complete"]);
    dispatch_chain(&pool, &s1).await.unwrap();
    assert_eq!(hget(&pool, &w, "kc").await.as_deref(), Some("1"));
    assert_eq!(hget(&pool, &w, "complete_at").await, None);
    // The retry succeeds: still held.
    succeeded(&pool, &s1, "a").await;
    dispatch_chain(&pool, &s1).await.unwrap();
    assert_eq!(hget(&pool, &w, "ks").await.as_deref(), Some("1"));
    assert_eq!(hget(&pool, &w, "success_at").await, None);
    assert!(!in_outbox(&pool, &w).await);
    // The last stage is defined by a reopen releasing the hold.
    let mut reopen = open_batch(&w);
    reopen.extra = vec![("open".to_owned(), "0".to_owned())];
    let stage2 = [reopen, batch(CommitMode::New, &s2, 1)];
    assert_eq!(
        commit(&pool, "wf-2", &stage2, &[queued(2, "b", "q1", &s2)])
            .await
            .unwrap(),
        applied(&[])
    );
    assert_eq!(hget(&pool, &w, "open").await.as_deref(), Some("0"));
    succeeded(&pool, &s2, "b").await;
    assert_eq!(dispatch_chain(&pool, &s2).await.unwrap(), 2);
    assert!(hget(&pool, &w, "complete_at").await.is_some());
    assert!(hget(&pool, &w, "success_at").await.is_some());

    // A release finding the conditions already met fires at the commit.
    let (v, t1) = (bid(168), bid(169));
    let mut held = new_batch(&v);
    held.extra = vec![("open".to_owned(), "1".to_owned())];
    commit(
        &pool,
        "c1",
        &[held, batch(CommitMode::New, &t1, 1)],
        &[queued(2, "x", "q1", &t1)],
    )
    .await
    .unwrap();
    succeeded(&pool, &t1, "x").await;
    dispatch_chain(&pool, &t1).await.unwrap();
    assert_eq!(hget(&pool, &v, "success_at").await, None);
    let mut release = open_batch(&v);
    release.extra = vec![("open".to_owned(), "0".to_owned())];
    assert_eq!(
        commit(&pool, "release", &[release], &[]).await.unwrap(),
        applied(&[&v])
    );
    assert!(hget(&pool, &v, "success_at").await.is_some());
    assert!(in_outbox(&pool, &v).await);

    // A cancel releases the hold: the stages defined so far completed, so the workflow does.
    let (u, r1) = (bid(170), bid(171));
    let mut held = new_batch(&u);
    held.extra = vec![("open".to_owned(), "1".to_owned())];
    held.linger_secs = 600;
    commit(
        &pool,
        "c1",
        &[held, batch(CommitMode::New, &r1, 1)],
        &[queued(2, "y", "q1", &r1)],
    )
    .await
    .unwrap();
    failed(&pool, &r1, "y", true).await;
    dispatch_chain(&pool, &r1).await.unwrap();
    assert_eq!(hget(&pool, &u, "complete_at").await, None);
    let cancelled = scripts::cancel(&pool, &u).await.unwrap();
    assert!(cancelled.dispatch);
    assert_eq!(hget(&pool, &u, "open").await, None);
    assert!(hget(&pool, &u, "complete_at").await.is_some());
    dispatch_chain(&pool, &u).await.unwrap();
    assert!(hget(&pool, &u, "finished_at").await.is_some());
    assert_every_key_expires(&redis, &pool).await;
}

/// batches-3: activity refreshes a batch once a quarter of its expiry (an hour at most) went by,
/// so that a one-hour batch, the shortest, lives on while it is active.
#[tokio::test]
async fn activity_refreshes_short_expiries() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let b = bid(172);
    let mut root = new_batch(&b);
    root.ttl_secs = 3_600;
    new_with_members(&pool, "c1", root, 3).await;
    let expire_all = |left: i64| {
        let pool = pool.clone();
        let b = b.clone();
        async move {
            for key in scripts::batch_keys(&b) {
                cmd::<()>(&pool, ::redis::cmd("PEXPIRE").arg(key).arg(left)).await;
            }
        }
    };
    // 10 minutes after the commit: not refreshed yet.
    expire_all(3_000_000).await;
    assert!(!succeeded(&pool, &b, &jid(1)).await.refreshed);
    assert!(pttl(&pool, &keys::batch(&b)).await <= 3_000_000);
    // 59 minutes and 58.5 s after it (the finding's case): refreshed to a full hour.
    expire_all(1_500).await;
    assert!(succeeded(&pool, &b, &jid(2)).await.refreshed);
    for key in scripts::batch_keys(&b) {
        let left = pttl(&pool, &key).await;
        assert!(left > 3_590_000 || left == -2, "{key}: {left}");
    }
    assert_every_key_expires(&redis, &pool).await;

    // The default 30 days: refreshed only once an hour went by.
    let d = bid(173);
    new_with_members(&pool, "c1", new_batch(&d), 2).await;
    for key in scripts::batch_keys(&d) {
        cmd::<()>(
            &pool,
            ::redis::cmd("PEXPIRE")
                .arg(key)
                .arg(2_592_000_000_u64 - 3_500_000),
        )
        .await;
    }
    assert!(!succeeded(&pool, &d, &jid(1)).await.refreshed);
    for key in scripts::batch_keys(&d) {
        cmd::<()>(
            &pool,
            ::redis::cmd("PEXPIRE")
                .arg(key)
                .arg(2_592_000_000_u64 - 3_700_000),
        )
        .await;
    }
    assert!(failed(&pool, &d, &jid(2), true).await.refreshed);
    assert!(pttl(&pool, &keys::batch(&d)).await > 2_591_990_000);
}

/// batches-7: the commit ids of a root created with a chosen bid (`xbid`) outlive its linger by
/// its `ttl`: a re-run of its creating job after the linger finds its commit applied; a batch
/// with a random bid keeps nothing.
#[tokio::test]
async fn commit_ids_of_a_chosen_bid_outlive_the_linger() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    for (n, chosen) in [(174, true), (175, false)] {
        let b = bid(n);
        let mut root = new_batch(&b);
        root.ttl_secs = 7_200;
        root.linger_secs = 30;
        if chosen {
            root.extra = vec![("xbid".to_owned(), "1".to_owned())];
        }
        let jobs = [queued(1, "m1", "q1", &b)];
        commit(&pool, "creator", std::slice::from_ref(&root), &jobs)
            .await
            .unwrap();
        succeeded(&pool, &b, "m1").await;
        dispatch_chain(&pool, &b).await.unwrap();
        assert!(hget(&pool, &b, "finished_at").await.is_some());
        assert!(pttl(&pool, &keys::batch(&b)).await <= 30_000);
        let kept = pttl(&pool, &keys::batch_commits(&b)).await;
        if chosen {
            assert!(kept > 7_190_000, "{kept}");
        } else {
            assert!(kept <= 30_000, "{kept}");
        }
        // The linger ends: every key whose expiry is at most the linger goes.
        for key in scripts::batch_keys(&b) {
            if pttl(&pool, &key).await <= 30_000 {
                cmd::<()>(&pool, ::redis::cmd("DEL").arg(key)).await;
            }
        }
        let again = commit(&pool, "creator", std::slice::from_ref(&root), &jobs).await;
        if chosen {
            assert!(
                matches!(again, Ok(CommitReply::Already { .. })),
                "{again:?}"
            );
            assert_eq!(hget(&pool, &b, "v").await, None, "not created again");
            // Another commit id cannot take the bid either.
            assert!(matches!(
                commit(&pool, "other", std::slice::from_ref(&root), &jobs).await,
                Err(Error::Batch(BatchError::Exists { .. }))
            ));
        } else {
            assert_eq!(again.unwrap(), applied(&[]));
        }
    }
    assert_eq!(queue_jids(&pool, "q1").await, ["m1", "m1", "m1"]);
}

/// batches-10: a member counted dead (killed through the API) that runs again and fails with a
/// retry is no longer dead.
#[tokio::test]
async fn a_revived_member_failing_with_a_retry_is_not_dead() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let b = bid(176);
    new_with_members(&pool, "c1", new_batch(&b), 2).await;
    failed(&pool, &b, &jid(1), true).await;
    assert_eq!(counts(&pool, &b).await, (2, 1, 0, 2));
    failed(&pool, &b, &jid(1), false).await;
    assert_eq!(counts(&pool, &b).await, (2, 1, 1, 2));
    failed(&pool, &b, &jid(1), true).await;
    assert_eq!(counts(&pool, &b).await, (2, 1, 0, 2));
    assert!(
        hget(&pool, &b, "death_at").await.is_some(),
        "death fired once, for good"
    );
}

/// batches-11: a reopened child gaining a member scheduled beyond its parent's expiry extends
/// its ancestors too (the refresh after a reopen floors them with the child's expiry).
#[tokio::test]
async fn a_reopened_child_extends_its_ancestors() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let (g, p, c) = (bid(177), bid(178), bid(179));
    let batches = [
        new_batch(&g),
        batch(CommitMode::New, &p, 1),
        batch(CommitMode::New, &c, 2),
    ];
    commit(&pool, "c1", &batches, &[queued(3, "c0", "q1", &c)])
        .await
        .unwrap();
    #[allow(clippy::cast_precision_loss)]
    let at = (redis_now(&pool).await + 40 * 86_400) as f64;
    commit(
        &pool,
        "far",
        &[open_batch(&c)],
        &[scheduled(1, "late", at, &c)],
    )
    .await
    .unwrap();
    let child = pttl(&pool, &keys::batch(&c)).await;
    assert!(child > 69 * 86_400_000, "{child}");
    refresh_ancestors(&pool, &c).await;
    for ancestor in [&p, &g] {
        for key in scripts::batch_keys(ancestor) {
            let left = pttl(&pool, &key).await;
            assert!(
                left == -2 || left >= child - 5_000,
                "{key}: {left} vs {child}"
            );
        }
    }
    assert_every_key_expires(&redis, &pool).await;
}

/// batches-9: outbox entries that cannot be dispatched go behind the others: 100 of them (a
/// full round) no longer keep maintenance from a batch fired after them.
#[tokio::test]
async fn maintenance_puts_failing_entries_behind_the_others() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    cmd::<()>(&pool, ::redis::cmd("SET").arg("queue:bad").arg("oops")).await;
    for n in 0..100 {
        let b = bid(1_000 + n);
        let mut root = new_batch(&b);
        root.callbacks[1] = templates(&[template(
            "Bad",
            &format!("bad{n}"),
            "bad",
            None,
            &b,
            "success",
        )]);
        assert_eq!(
            commit(&pool, "c1", &[root], &[]).await.unwrap(),
            applied(&[&b])
        );
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    let good = bid(1_200);
    let mut root = new_batch(&good);
    root.callbacks[1] = templates(&[template("Good", "good", "good", None, &good, "success")]);
    commit(&pool, "c1", &[root], &[]).await.unwrap();
    let round = Round {
        redis: &pool,
        identity: "test:1:abc",
        interval: Duration::from_millis(500),
        grace: Duration::ZERO,
    };
    let first = round.run().await.unwrap();
    assert_eq!((first.dispatched, first.postponed), (0, 100));
    assert!(queue_jids(&pool, "good").await.is_empty());
    // The next round reaches the batch behind them.
    let second = round.run().await.unwrap();
    assert_eq!((second.dispatched, second.postponed), (1, 0));
    assert_eq!(queue_jids(&pool, "good").await, ["good"]);
    // The failing entries wait a minute (Redis time) before they are tried again.
    let (secs, _): (u64, u64) = cmd(&pool, &::redis::cmd("TIME")).await;
    #[allow(clippy::cast_precision_loss)]
    let later = secs as f64 + 50.0;
    let due: Vec<String> = cmd(
        &pool,
        ::redis::cmd("ZRANGEBYSCORE")
            .arg(keys::BATCH_EVENTS)
            .arg("-inf")
            .arg(later),
    )
    .await;
    assert!(due.is_empty(), "{due:?}");
    assert_eq!(
        cmd::<u64>(&pool, ::redis::cmd("ZCARD").arg(keys::BATCH_EVENTS)).await,
        100
    );
}

#[tokio::test]
async fn s17_workflow_stages_reopen_the_overall_batch_once() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let (w, s1, s2) = (bid(170), bid(171), bid(172));
    let mut overall = new_batch(&w);
    overall.callbacks[1] = templates(&[template("Shipped", "shipped", "cbq", None, &w, "success")]);
    let mut stage1 = batch(CommitMode::New, &s1, 1);
    stage1.callbacks[1] = templates(&[template(
        "hench::WorkflowNext",
        "next1",
        "default",
        Some(&w),
        &s1,
        "success",
    )]);
    commit(
        &pool,
        "c1",
        &[overall, stage1],
        &[queued(2, "a", "q1", &s1)],
    )
    .await
    .unwrap();
    succeeded(&pool, &s1, "a").await;
    dispatch_chain(&pool, &s1).await.unwrap();
    let pending: bool = cmd(
        &pool,
        ::redis::cmd("SISMEMBER")
            .arg(keys::batch_pending(&w))
            .arg("next1"),
    )
    .await;
    assert!(pending, "WorkflowNext is a member of the overall batch");
    let jobs = [queued(2, "b", "q1", &s2)];
    let stage2 = [open_batch(&w), batch(CommitMode::New, &s2, 1)];
    assert_eq!(
        commit(&pool, "wf-2", &stage2, &jobs).await.unwrap(),
        applied(&[])
    );
    // A re-run of WorkflowNext does not add stage 2 twice.
    let again = [open_batch(&w), batch(CommitMode::New, &bid(173), 1)];
    assert!(matches!(
        commit(&pool, "wf-2", &again, &jobs).await.unwrap(),
        CommitReply::Already { .. }
    ));
    assert!(succeeded(&pool, &w, "next1").await.fired.is_empty());
    succeeded(&pool, &s2, "b").await;
    assert_eq!(dispatch_chain(&pool, &s2).await.unwrap(), 2);
    assert_eq!(queue_jids(&pool, "cbq").await, ["shipped"]);
    assert!(hget(&pool, &w, "success_at").await.is_some());
}

#[tokio::test]
async fn s18_a_refresh_refreshes_the_ancestors() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let (g, p, c) = (bid(180), bid(181), bid(182));
    let mut batches = [
        new_batch(&g),
        batch(CommitMode::New, &p, 1),
        batch(CommitMode::New, &c, 2),
    ];
    for batch in &mut batches {
        batch.ttl_secs = 7_200;
    }
    let jobs = [queued(3, "c0", "q1", &c), queued(3, "c1", "q1", &c)];
    commit(&pool, "c1", &batches, &jobs).await.unwrap();
    for key in [keys::batch(&g), keys::batch(&p)] {
        cmd::<()>(&pool, ::redis::cmd("PEXPIRE").arg(key).arg(60_000)).await;
    }
    // Half an hour later (a quarter of the 2 hours): the child's next activity refreshes it.
    for key in scripts::batch_keys(&c) {
        cmd::<()>(&pool, ::redis::cmd("PEXPIRE").arg(key).arg(5_300_000)).await;
    }
    assert!(succeeded(&pool, &c, "c0").await.refreshed);
    refresh_ancestors(&pool, &c).await;
    assert!(pttl(&pool, &keys::batch(&p)).await > 7_000_000);
    assert!(pttl(&pool, &keys::batch(&g)).await > 7_000_000);
    assert!(pttl(&pool, &keys::batch_kids(&g)).await > 7_000_000);
    // A second refresh right after is skipped.
    assert!(!succeeded(&pool, &c, "c1").await.refreshed);
}

#[tokio::test]
async fn a_reopen_extends_every_key_of_the_batch() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let b = bid(190);
    let mut root = new_batch(&b);
    root.ttl_secs = 3_600;
    new_with_members(&pool, "c1", root, 2).await;
    failed(&pool, &b, &jid(1), false).await;
    #[allow(clippy::cast_precision_loss)]
    let at = (redis_now(&pool).await + 86_400) as f64;
    commit(&pool, "c2", &[open_batch(&b)], &[scheduled(1, "s", at, &b)])
        .await
        .unwrap();
    for key in [
        keys::batch(&b),
        keys::batch_failed(&b),
        keys::batch_dead(&b),
    ] {
        assert!(pttl(&pool, &key).await > 86_400_000, "{key}");
    }
    assert_every_key_expires(&redis, &pool).await;
}

#[tokio::test]
async fn out_of_memory_refuses_commits_and_failures_but_lets_successes_finish() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let b = bid(200);
    let mut root = new_batch(&b);
    root.callbacks[1] = templates(&[template("Done", "cb", "cbq", None, &b, "success")]);
    new_with_members(&pool, "c1", root, 3).await;
    cmd::<()>(
        &pool,
        ::redis::cmd("CONFIG")
            .arg("SET")
            .arg("maxmemory-policy")
            .arg("noeviction"),
    )
    .await;
    cmd::<()>(
        &pool,
        ::redis::cmd("CONFIG").arg("SET").arg("maxmemory").arg(1),
    )
    .await;
    // A commit: refused, nothing written.
    let other = bid(201);
    let jobs = [queued(1, "x", "q2", &other)];
    let error = commit(&pool, "c2", &[new_batch(&other)], &jobs)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("OOM"), "{error}");
    let exists: u64 = cmd(&pool, ::redis::cmd("EXISTS").arg(keys::batch(&other))).await;
    assert_eq!(exists, 0);
    assert!(queue_jids(&pool, "q2").await.is_empty());
    // A failure: refused, nothing written.
    let refused = scripts::account(
        &pool,
        &b,
        &jid(1),
        &Outcome::Failed {
            retry: true,
            summary: "{}".to_owned(),
        },
    )
    .await;
    assert!(refused.unwrap_err().to_string().contains("OOM"));
    assert_eq!(counts(&pool, &b).await.1, 0);
    // Successes: counted, and the last one fires.
    succeeded(&pool, &b, &jid(1)).await;
    succeeded(&pool, &b, &jid(2)).await;
    assert_eq!(
        succeeded(&pool, &b, &jid(3)).await.fired,
        ["complete", "success"]
    );
    cmd::<()>(
        &pool,
        ::redis::cmd("CONFIG").arg("SET").arg("maxmemory").arg(0),
    )
    .await;
    // Memory freed: the refused commit applies.
    assert_eq!(
        commit(&pool, "c2", &[new_batch(&other)], &jobs)
            .await
            .unwrap(),
        applied(&[])
    );
    dispatch_chain(&pool, &b).await.unwrap();
    assert_eq!(queue_jids(&pool, "cbq").await, ["cb"]);
}

#[tokio::test]
async fn dispatch_refuses_a_callback_queue_of_the_wrong_type_before_any_write() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let b = bid(210);
    let mut root = new_batch(&b);
    root.callbacks[1] = templates(&[
        template("A", "cb-a", "good", None, &b, "success"),
        template("B", "cb-b", "bad", None, &b, "success"),
    ]);
    new_with_members(&pool, "c1", root, 1).await;
    succeeded(&pool, &b, &jid(1)).await;
    cmd::<()>(&pool, ::redis::cmd("SET").arg("queue:bad").arg("oops")).await;
    let error = dispatch_chain(&pool, &b).await.unwrap_err();
    assert!(error.to_string().contains("WRONGTYPE"), "{error}");
    assert!(in_outbox(&pool, &b).await);
    assert!(queue_jids(&pool, "good").await.is_empty());
    assert_eq!(hget(&pool, &b, "success_dt").await, None);
    cmd::<()>(&pool, ::redis::cmd("DEL").arg("queue:bad")).await;
    assert_eq!(dispatch_chain(&pool, &b).await.unwrap(), 1);
    assert_eq!(queue_jids(&pool, "good").await, ["cb-a"]);
    assert_eq!(queue_jids(&pool, "bad").await, ["cb-b"]);
}

#[tokio::test]
async fn deletes_refuse_unfinished_children_of_live_parents() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let (p, c) = (bid(220), bid(221));
    let jobs = [queued(1, "a", "q1", &p), queued(2, "b", "q1", &c)];
    commit(
        &pool,
        "c1",
        &[new_batch(&p), batch(CommitMode::New, &c, 1)],
        &jobs,
    )
    .await
    .unwrap();
    assert!(matches!(
        scripts::delete(&pool, &c, Some(&p), false).await,
        Err(Error::Batch(BatchError::DeleteChild { parent, .. })) if parent == p
    ));
    assert!(scripts::delete(&pool, &c, Some(&p), true).await.unwrap() > 0);
    // Members of a deleted batch are no-ops.
    assert!(!succeeded(&pool, &c, "b").await.applied);
    assert!(scripts::delete(&pool, &p, None, false).await.unwrap() > 0);
    let left: Vec<String> = cmd(&pool, ::redis::cmd("KEYS").arg("batch*")).await;
    assert!(left.is_empty(), "{left:?}");
}

fn unique(index: usize, jid: &str, digest: &str, on_conflict: OnConflict, bid: &str) -> CommitJob {
    let mut job = queued(index, jid, "q1", bid);
    job.unique = Some(UniqueMember {
        digest: digest.to_owned(),
        class: "U".to_owned(),
        queue: "q1".to_owned(),
        until: UniqueUntil::Success,
        for_ms: 60_000,
        on_conflict,
    });
    job
}

/// The unique duplicates counted for `class` this minute.
async fn unique_duplicates(pool: &RedisPool, class: &str) -> u64 {
    let hash: HashMap<String, String> = cmd(
        pool,
        ::redis::cmd("HGETALL").arg(keys::unique_minute(Utc::now())),
    )
    .await;
    hash.get(&format!("{class}|d"))
        .and_then(|count| count.parse().ok())
        .unwrap_or(0)
}

#[tokio::test]
async fn commits_take_unique_locks_with_the_frozen_library() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let b = bid(230);
    // Another job holds d2's lock.
    cmd::<()>(
        &pool,
        ::redis::cmd("HSET")
            .arg(keys::unique_lock("d2"))
            .arg("jid")
            .arg("holder"),
    )
    .await;
    cmd::<()>(
        &pool,
        ::redis::cmd("PEXPIRE")
            .arg(keys::unique_lock("d2"))
            .arg(60_000),
    )
    .await;
    let jobs = [
        unique(1, "u1", "d1", OnConflict::Drop, &b),
        // A duplicate of u1 within the commit.
        unique(1, "u2", "d1", OnConflict::Drop, &b),
        unique(1, "u3", "d2", OnConflict::Drop, &b),
        queued(1, "plain", "q1", &b),
    ];
    let duplicates = vec![
        ("u2".to_owned(), "u1".to_owned()),
        ("u3".to_owned(), "holder".to_owned()),
    ];
    assert_eq!(
        commit(&pool, "c1", &[new_batch(&b)], &jobs).await.unwrap(),
        CommitReply::Applied {
            fired: Vec::new(),
            duplicates: duplicates.clone(),
            unlocked: Vec::new(),
        }
    );
    assert_eq!(queue_jids(&pool, "q1").await, ["u1", "plain"]);
    assert_eq!(counts(&pool, &b).await, (2, 0, 0, 2));
    let lock: HashMap<String, String> =
        cmd(&pool, ::redis::cmd("HGETALL").arg(keys::unique_lock("d1"))).await;
    assert_eq!(lock.get("jid").map(String::as_str), Some("u1"));
    assert_eq!(lock.get("class").map(String::as_str), Some("U"));
    assert_eq!(lock.get("for").map(String::as_str), Some("60"));
    assert_eq!(lock.get("until").map(String::as_str), Some("success"));
    let ttl = pttl(&pool, &keys::unique_lock("d1")).await;
    assert!((59_000..=60_000).contains(&ttl), "{ttl}");
    let indexed: Option<f64> = cmd(
        &pool,
        ::redis::cmd("ZSCORE").arg(keys::UNIQUE_LOCKS).arg("d1"),
    )
    .await;
    assert!(indexed.is_some());
    assert_eq!(unique_duplicates(&pool, "U").await, 2);
    // A resend reports the same duplicates and writes nothing.
    assert_eq!(
        commit(&pool, "c1", &[new_batch(&b)], &jobs).await.unwrap(),
        CommitReply::Already { duplicates }
    );
    assert_eq!(queue_jids(&pool, "q1").await.len(), 2);
    // A member whose own jid holds the lock is in Redis outside the batch: left out.
    let b2 = bid(231);
    let reply = commit(
        &pool,
        "c2",
        &[new_batch(&b2)],
        &[unique(1, "u1", "d1", OnConflict::Drop, &b2)],
    )
    .await
    .unwrap();
    assert_eq!(
        reply,
        CommitReply::Applied {
            fired: vec![b2.clone()],
            duplicates: vec![("u1".to_owned(), "u1".to_owned())],
            unlocked: Vec::new(),
        }
    );
    assert_every_key_expires(&redis, &pool).await;
}

#[tokio::test]
async fn an_error_conflict_refuses_the_whole_commit_before_any_write() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let b = bid(240);
    cmd::<()>(
        &pool,
        ::redis::cmd("HSET")
            .arg(keys::unique_lock("d9"))
            .arg("jid")
            .arg("holder"),
    )
    .await;
    let jobs = [
        unique(1, "first", "d1", OnConflict::Drop, &b),
        unique(1, "second", "d9", OnConflict::Error, &b),
    ];
    let error = commit(&pool, "c1", &[new_batch(&b)], &jobs)
        .await
        .unwrap_err();
    assert!(
        matches!(&error, Error::DuplicateJob { class, digest, holder }
            if class == "U" && digest == "d9" && holder == "holder"),
        "{error}"
    );
    let written: u64 = cmd(
        &pool,
        ::redis::cmd("EXISTS")
            .arg(keys::batch(&b))
            .arg(keys::unique_lock("d1"))
            .arg("queue:q1"),
    )
    .await;
    assert_eq!(written, 0);
    // A lock key of the wrong type refuses too, before any write.
    cmd::<()>(
        &pool,
        ::redis::cmd("SET").arg(keys::unique_lock("dx")).arg("oops"),
    )
    .await;
    let error = commit(
        &pool,
        "c2",
        &[new_batch(&b)],
        &[unique(1, "x", "dx", OnConflict::Drop, &b)],
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("WRONGTYPE"), "{error}");
    let exists: u64 = cmd(&pool, ::redis::cmd("EXISTS").arg(keys::batch(&b))).await;
    assert_eq!(exists, 0);
}

#[tokio::test]
async fn scheduled_unique_members_lock_for_their_delay_too() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let b = bid(250);
    #[allow(clippy::cast_precision_loss)]
    let at = (redis_now(&pool).await + 3_600) as f64;
    let mut job = scheduled(1, "s", at, &b);
    job.unique = Some(UniqueMember {
        digest: "ds".to_owned(),
        class: "U".to_owned(),
        queue: "q1".to_owned(),
        until: UniqueUntil::Start,
        for_ms: 60_000,
        on_conflict: OnConflict::Drop,
    });
    commit(&pool, "c1", &[new_batch(&b)], &[job]).await.unwrap();
    let ttl = pttl(&pool, &keys::unique_lock("ds")).await;
    assert!(ttl > 3_600_000 && ttl <= 3_661_000, "{ttl}");
    let at_field: Option<String> = cmd(
        &pool,
        ::redis::cmd("HGET").arg(keys::unique_lock("ds")).arg("at"),
    )
    .await;
    assert_eq!(at_field, Some(at.to_string()));
}

#[tokio::test]
async fn counters_of_the_minute_record_the_events() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let b = bid(260);
    new_with_members(&pool, "c1", new_batch(&b), 2).await;
    failed(&pool, &b, &jid(1), false).await;
    succeeded(&pool, &b, &jid(2)).await;
    let c = bid(261);
    new_with_members(&pool, "c2", new_batch(&c), 1).await;
    scripts::cancel(&pool, &c).await.unwrap();
    // Several minutes may be involved: sum them.
    let now = Utc::now();
    let mut counters: HashMap<String, u64> = HashMap::new();
    for minute in [now - chrono::Duration::minutes(1), now] {
        let hash: HashMap<String, u64> = cmd(
            &pool,
            ::redis::cmd("HGETALL").arg(keys::batch_minute(minute)),
        )
        .await;
        for (field, count) in hash {
            *counters.entry(field).or_default() += count;
        }
    }
    assert_eq!(counters.get("created"), Some(&2));
    assert_eq!(counters.get("death"), Some(&1));
    assert_eq!(counters.get("complete"), Some(&1));
    assert_eq!(counters.get("cancelled"), Some(&1));
    assert_eq!(counters.get("success"), None);
    let ttl = pttl(&pool, &keys::batch_minute(Utc::now())).await;
    assert!(ttl > 0 && ttl <= 28_800_000, "{ttl}");
}

#[tokio::test]
async fn dispatch_surgery_keeps_the_job_intact() {
    // A template with extreme and float arguments, extra keys and unicode parses back into the
    // same job, plus `enqueued_at`.
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let b = bid(270);
    let mut job = JobPayload::new(
        "Done",
        vec![
            json!(i64::MIN),
            json!(i64::MAX),
            json!(0.1),
            json!(1e300),
            json!("ünïcødé ✓ \"quoted\" \\ / \n"),
        ],
    );
    job.jid = "cbx".to_owned();
    job.queue = "cbq".to_owned();
    job.extra
        .insert("custom".to_owned(), json!({"nested": [1, 2.5, null]}));
    job.batch_callback = Some(BatchCallback::new(&b, BatchEvent::Success));
    let mut root = new_batch(&b);
    root.callbacks[1] = templates(&[Template {
        q: "cbq".to_owned(),
        jid: "cbx".to_owned(),
        j: job.to_json().unwrap(),
    }]);
    commit(&pool, "c1", &[root], &[]).await.unwrap();
    dispatch_chain(&pool, &b).await.unwrap();
    let pushed: Vec<String> = cmd(
        &pool,
        ::redis::cmd("LRANGE").arg("queue:cbq").arg(0).arg(-1),
    )
    .await;
    let mut parsed = JobPayload::from_json(&pushed[0]).unwrap();
    assert!(parsed.enqueued_at.take().is_some());
    assert_eq!(parsed, job);
}
