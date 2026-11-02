//! The in-memory model of the testing modes against the batch scripts (`docs/design/batches.md`
//! §2.11, R2): random sequences of commits (with nesting, reopens, repeated commit ids and bid
//! collisions), outcomes in random order with repeats, cancels, dispatches and deletes, applied
//! to both a [`Model`] and a private `redis-server`; after every step their replies and their
//! states (counts, sets, event times as present or absent, the outbox, the members and callbacks
//! pushed, `finished_at`) must agree.

use std::collections::{BTreeSet, HashMap};

use chrono::Utc;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};
use serde_json::json;

use crate::batch::lua::BATCH_COMMIT;
use crate::batch::model::{Model, ModelCommit, ModelJob, event_index};
use crate::batch::scripts::{
    self, Accounted, CommitBatch, CommitJob, CommitMode, CommitReply, Outcome, Template,
    commit_refusal, parse_commit,
};
use crate::batch::{BatchError, BatchEvent};
use crate::client::Stored;
use crate::error::{Error, Result};
use crate::keys;
use crate::redis::RedisPool;
use crate::test_util::TestRedis;

/// Batches the operations draw from (few, so that reopens, collisions and parents happen).
const BIDS: u8 = 6;

/// Member jids the operations draw from.
const JIDS: u8 = 10;

fn bid(n: u8) -> String {
    format!("{n:024x}")
}

fn jid(n: u8) -> String {
    format!("m{n}")
}

/// One batch of a generated commit.
#[derive(Debug, Clone)]
struct GenBatch {
    bid: u8,
    /// The parent among the commit's earlier batches (1-based; children only).
    parent: usize,
    /// Which events (complete, success, death) have a callback.
    callbacks: [bool; 3],
    /// A new batch held open (`open` = 1, a workflow's overall batch).
    hold: bool,
}

#[derive(Debug, Clone)]
enum Op {
    Commit {
        cid: u8,
        reopen: bool,
        /// A reopened root releases its hold (`open` = 0, a workflow's last stage).
        release: bool,
        batches: Vec<GenBatch>,
        /// `(batch index in the commit, jid)`.
        members: Vec<(usize, u8)>,
    },
    Succeeded(u8, u8),
    Failed(u8, u8, bool),
    Skipped(u8, u8, bool),
    Cancel(u8),
    Dispatch(u8),
    Delete(u8, bool),
}

fn gen_batches() -> impl Strategy<Value = Vec<GenBatch>> {
    (
        0..BIDS,
        any::<[bool; 3]>(),
        prop::bool::weighted(0.2),
        proptest::collection::vec(
            (
                0..BIDS,
                any::<prop::sample::Index>(),
                any::<[bool; 3]>(),
                prop::bool::weighted(0.2),
            ),
            0..3,
        ),
    )
        .prop_map(|(root, callbacks, hold, children)| {
            let mut batches = vec![GenBatch {
                bid: root,
                parent: 0,
                callbacks,
                hold,
            }];
            for (bid, parent, callbacks, hold) in children {
                if batches.iter().any(|batch| batch.bid == bid) {
                    continue;
                }
                let parent = parent.index(batches.len()) + 1;
                batches.push(GenBatch {
                    bid,
                    parent,
                    callbacks,
                    hold,
                });
            }
            batches
        })
}

fn gen_commit() -> impl Strategy<Value = Op> {
    (
        0..4_u8,
        any::<bool>(),
        any::<bool>(),
        gen_batches(),
        proptest::collection::vec((any::<prop::sample::Index>(), 0..JIDS), 0..5),
    )
        .prop_map(|(cid, reopen, release, batches, members)| {
            let mut seen = BTreeSet::new();
            let members = members
                .into_iter()
                .filter(|(_, jid)| seen.insert(*jid))
                .map(|(index, jid)| (index.index(batches.len()), jid))
                .collect();
            Op::Commit {
                cid,
                reopen,
                release,
                batches,
                members,
            }
        })
}

fn gen_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => gen_commit(),
        4 => (0..BIDS, 0..JIDS + 4).prop_map(|(b, j)| Op::Succeeded(b, j)),
        2 => (0..BIDS, 0..JIDS + 4, any::<bool>()).prop_map(|(b, j, retry)| Op::Failed(b, j, retry)),
        1 => (0..BIDS, 0..JIDS + 4, any::<bool>())
            .prop_map(|(b, j, cancelled)| Op::Skipped(b, j, cancelled)),
        1 => (0..BIDS).prop_map(Op::Cancel),
        3 => (0..BIDS).prop_map(Op::Dispatch),
        1 => (0..BIDS, any::<bool>()).prop_map(|(b, force)| Op::Delete(b, force)),
    ]
}

/// The callback jid of `event` of batch `bid` (fixed, so that both sides push the same).
fn callback_jid(bid: u8, event: BatchEvent) -> String {
    format!("cb-{bid}-{event}")
}

/// The commit batches and jobs of a generated commit.
fn commit_of(
    reopen: bool,
    release: bool,
    batches: &[GenBatch],
    members: &[(usize, u8)],
) -> (Vec<CommitBatch>, Vec<CommitJob>) {
    let commit_batches: Vec<CommitBatch> = batches
        .iter()
        .enumerate()
        .map(|(index, batch)| {
            let mode = if index == 0 && reopen {
                CommitMode::Open
            } else {
                CommitMode::New
            };
            let parent = if index == 0 { 0 } else { batch.parent };
            let member_of = parent
                .checked_sub(1)
                .and_then(|index| batches.get(index))
                .map(|parent| bid(parent.bid));
            let mut callbacks: [String; 3] = Default::default();
            if mode == CommitMode::New {
                for event in [BatchEvent::Complete, BatchEvent::Success, BatchEvent::Death] {
                    if batch.callbacks[event_index(event)] {
                        let mut job = json!({
                            "class": "Cb", "args": [], "jid": callback_jid(batch.bid, event),
                            "queue": "cb", "batch_callback": {"bid": bid(batch.bid), "event": event},
                        });
                        if let Some(parent) = &member_of {
                            job["bid"] = json!(parent);
                        }
                        let template = Template {
                            q: "cb".to_owned(),
                            jid: callback_jid(batch.bid, event),
                            j: job.to_string(),
                        };
                        callbacks[event_index(event)] =
                            serde_json::to_string(&[template]).unwrap();
                    }
                }
            }
            let extra = match mode {
                CommitMode::New if batch.hold => vec![("open".to_owned(), "1".to_owned())],
                CommitMode::Open if release => vec![("open".to_owned(), "0".to_owned())],
                _ => Vec::new(),
            };
            CommitBatch {
                mode,
                bid: bid(batch.bid),
                parent,
                ttl_secs: 3_600,
                linger_secs: 60,
                description: String::new(),
                callbacks,
                extra,
            }
        })
        .collect();
    let jobs = members
        .iter()
        .map(|(index, member)| CommitJob {
            batch: index + 1,
            jid: jid(*member),
            stored: Stored::Queued {
                queue: "q1".to_owned(),
                json: json!({"class": "M", "args": [], "jid": jid(*member), "queue": "q1"})
                    .to_string(),
            },
            unique: None,
        })
        .collect();
    (commit_batches, jobs)
}

async fn cmd<T: ::redis::FromRedisValue>(pool: &RedisPool, command: &::redis::Cmd) -> T {
    let mut conn = pool.conn().await.unwrap();
    command.query_async(&mut conn).await.unwrap()
}

/// What a batch's keys hold, comparable on both sides.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Snapshot {
    total: u64,
    skipped: u64,
    kids: u64,
    kc: u64,
    ks: u64,
    parent: Option<String>,
    pending: BTreeSet<String>,
    failed: BTreeSet<String>,
    dead: BTreeSet<String>,
    children: BTreeSet<String>,
    commits: BTreeSet<String>,
    /// Held open (`open` > 0).
    held: bool,
    /// Which of complete_at, success_at, death_at, complete_dt, success_dt, death_dt,
    /// cancelled_at, finished_at are set.
    times: [bool; 8],
}

async fn redis_snapshot(pool: &RedisPool, bid: &str) -> Option<Snapshot> {
    let hash: HashMap<String, String> =
        cmd(pool, ::redis::cmd("HGETALL").arg(keys::batch(bid))).await;
    if hash.is_empty() {
        return None;
    }
    let number = |field: &str| {
        hash.get(field)
            .and_then(|value| value.parse().ok())
            .unwrap_or(0)
    };
    let set = |members: Vec<String>| members.into_iter().collect::<BTreeSet<_>>();
    let failed: HashMap<String, String> =
        cmd(pool, ::redis::cmd("HGETALL").arg(keys::batch_failed(bid))).await;
    let commits: HashMap<String, String> =
        cmd(pool, ::redis::cmd("HGETALL").arg(keys::batch_commits(bid))).await;
    let times = [
        "complete_at",
        "success_at",
        "death_at",
        "complete_dt",
        "success_dt",
        "death_dt",
        "cancelled_at",
        "finished_at",
    ]
    .map(|field| hash.contains_key(field));
    Some(Snapshot {
        total: number("total"),
        skipped: number("skipped"),
        kids: number("kids"),
        kc: number("kc"),
        ks: number("ks"),
        parent: hash.get("parent").cloned(),
        pending: set(cmd(pool, ::redis::cmd("SMEMBERS").arg(keys::batch_pending(bid))).await),
        failed: failed.into_keys().collect(),
        dead: set(cmd(pool, ::redis::cmd("SMEMBERS").arg(keys::batch_dead(bid))).await),
        children: set(cmd(pool, ::redis::cmd("SMEMBERS").arg(keys::batch_kids(bid))).await),
        commits: commits.into_keys().collect(),
        held: hash
            .get("open")
            .and_then(|open| open.parse::<f64>().ok())
            .is_some_and(|open| open > 0.0),
        times,
    })
}

fn model_snapshot(model: &Model, bid: &str) -> Option<Snapshot> {
    let batch = model.batches.get(bid)?;
    Some(Snapshot {
        total: batch.total,
        skipped: batch.skipped,
        kids: batch.kids,
        kc: batch.kc,
        ks: batch.ks,
        parent: batch.parent.clone(),
        pending: batch.pending.clone(),
        failed: batch.failed.keys().cloned().collect(),
        dead: batch.dead.clone(),
        children: batch.children.clone(),
        commits: batch.commits.keys().cloned().collect(),
        held: batch.held(),
        times: [
            batch.complete_at.is_some(),
            batch.success_at.is_some(),
            batch.death_at.is_some(),
            batch.complete_dt.is_some(),
            batch.success_dt.is_some(),
            batch.death_dt.is_some(),
            batch.cancelled_at.is_some(),
            batch.finished_at.is_some(),
        ],
    })
}

/// The jids of a queue, oldest first.
async fn queue(pool: &RedisPool, name: &str) -> Vec<String> {
    let jobs: Vec<String> = cmd(
        pool,
        ::redis::cmd("LRANGE").arg(keys::queue(name)).arg(0).arg(-1),
    )
    .await;
    jobs.iter()
        .rev()
        .map(|job| {
            serde_json::from_str::<serde_json::Value>(job).unwrap()["jid"]
                .as_str()
                .unwrap_or("")
                .to_owned()
        })
        .collect()
}

/// The comparable part of an accounting's reply (the model has no expiries: no `refreshed`).
fn accounted(reply: &Accounted) -> (bool, bool, bool, Vec<String>) {
    (
        reply.applied,
        reply.not_cancelled,
        reply.dispatch,
        reply.fired.clone(),
    )
}

/// The name of an error, to compare refusals.
fn error_name(error: &Error) -> String {
    match error {
        Error::Batch(BatchError::NotFound { .. }) => "not found".to_owned(),
        Error::Batch(BatchError::Succeeded { .. }) => "succeeded".to_owned(),
        Error::Batch(BatchError::Cancelled { .. }) => "cancelled".to_owned(),
        Error::Batch(BatchError::Exists { .. }) => "exists".to_owned(),
        Error::Batch(BatchError::DeleteChild { .. }) => "delete child".to_owned(),
        Error::InvalidJob(_) => "invalid".to_owned(),
        Error::Redis(error) if error.to_string().contains("parent") => "invalid".to_owned(),
        other => format!("{other}"),
    }
}

/// The expected state, carried along a sequence.
#[derive(Default)]
struct Expected {
    model: Model,
    members: Vec<String>,
    callbacks: Vec<String>,
    now: i64,
    /// The event times (`complete`, `success`, `death`) each batch had after the last step.
    events: HashMap<String, [Option<i64>; 3]>,
}

/// The model's invariants after every step (U6): dead ⊆ failed ⊆ pending; `pending + skipped
/// ≤ total` (`succeeded ≥ 0`); `success` only once `complete` fired, and never with a
/// cancellation; an event's time, once written, never changes (each event fires once).
fn invariants(
    model: &Model,
    seen: &mut HashMap<String, [Option<i64>; 3]>,
) -> std::result::Result<(), String> {
    seen.retain(|bid, _| model.batches.contains_key(bid));
    for (bid, batch) in &model.batches {
        if let Some(jid) = batch
            .dead
            .iter()
            .find(|jid| !batch.failed.contains_key(*jid))
        {
            return Err(format!("{bid}: dead member {jid} has no failure"));
        }
        if let Some(jid) = batch
            .failed
            .keys()
            .find(|jid| !batch.pending.contains(*jid))
        {
            return Err(format!("{bid}: failed member {jid} is not pending"));
        }
        let left = u64::try_from(batch.pending.len()).unwrap_or(u64::MAX) + batch.skipped;
        if left > batch.total {
            return Err(format!(
                "{bid}: {} pending and {} skipped of {} members",
                batch.pending.len(),
                batch.skipped,
                batch.total
            ));
        }
        if batch.success_at.is_some() && batch.complete_at.is_none() {
            return Err(format!("{bid}: success without complete"));
        }
        if batch.success_at.is_some() && batch.cancelled_at.is_some() {
            return Err(format!("{bid}: success and cancelled"));
        }
        let events = [batch.complete_at, batch.success_at, batch.death_at];
        if let Some(before) = seen.get(bid)
            && before
                .iter()
                .zip(events)
                .any(|(before, now)| before.is_some() && *before != now)
        {
            return Err(format!(
                "{bid}: an event time changed: {before:?} → {events:?}"
            ));
        }
        seen.insert(bid.clone(), events);
    }
    Ok(())
}

async fn redis_commit(
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

/// Applies `op` to both sides and checks that they agree.
async fn step(
    pool: &RedisPool,
    expected: &mut Expected,
    op: &Op,
) -> std::result::Result<(), String> {
    expected.now += 1;
    let now = expected.now;
    match op {
        Op::Commit {
            cid,
            reopen,
            release,
            batches,
            members,
        } => {
            let (commit_batches, jobs) = commit_of(*reopen, *release, batches, members);
            let cid = format!("c{cid}");
            let redis = redis_commit(pool, &cid, &commit_batches, &jobs).await;
            let model_jobs: Vec<ModelJob> = jobs
                .iter()
                .map(|job| ModelJob {
                    batch: job.batch,
                    jid: job.jid.clone(),
                })
                .collect();
            let model = expected
                .model
                .commit(&cid, &commit_batches, &model_jobs, &[], now);
            match (redis, model) {
                (
                    Ok(CommitReply::Applied {
                        fired, duplicates, ..
                    }),
                    Ok(ModelCommit::Applied {
                        fired: expected_fired,
                    }),
                ) => {
                    if fired != expected_fired || !duplicates.is_empty() {
                        return Err(format!("commit fired {fired:?} vs {expected_fired:?}"));
                    }
                    expected
                        .members
                        .extend(jobs.iter().map(|job| job.jid.clone()));
                }
                (Ok(CommitReply::Already { .. }), Ok(ModelCommit::Already { .. })) => {}
                (Err(error), Err(expected_error))
                    if error_name(&error) == error_name(&expected_error) => {}
                (redis, model) => {
                    return Err(format!("commit: redis {redis:?} vs model {model:?}"));
                }
            }
        }
        Op::Succeeded(b, j) | Op::Failed(b, j, _) | Op::Skipped(b, j, _) => {
            let outcome = match op {
                Op::Failed(_, _, retry) => Outcome::Failed {
                    retry: *retry,
                    summary: "{}".to_owned(),
                },
                Op::Skipped(_, _, cancelled) => Outcome::Skipped {
                    cancelled: *cancelled,
                },
                _ => Outcome::Succeeded,
            };
            let (b, j) = (bid(*b), jid(*j));
            let redis = scripts::account(pool, &b, &j, &outcome)
                .await
                .map_err(|error| error.to_string())?;
            let model = expected.model.account(&b, &j, &outcome, now);
            if accounted(&redis) != accounted(&model) {
                return Err(format!("{op:?}: redis {redis:?} vs model {model:?}"));
            }
        }
        Op::Cancel(b) => {
            let b = bid(*b);
            let redis = scripts::cancel(pool, &b).await;
            let model = expected.model.cancel(&b, now);
            match (redis, model) {
                (Ok(redis), Ok(model)) => {
                    let mut children = redis.children.clone();
                    children.sort();
                    let mut expected_children = model.children.clone();
                    expected_children.sort();
                    if (redis.first, redis.dispatch, children)
                        != (model.first, model.dispatch, expected_children)
                    {
                        return Err(format!("cancel: redis {redis:?} vs model {model:?}"));
                    }
                }
                (Err(error), Err(expected_error))
                    if error_name(&error) == error_name(&expected_error) => {}
                (redis, model) => {
                    return Err(format!("cancel: redis {redis:?} vs model {model:?}"));
                }
            }
        }
        Op::Dispatch(b) => {
            let b = bid(*b);
            let target = scripts::dispatch_target(pool, &b)
                .await
                .map_err(|error| error.to_string())?;
            let redis = scripts::dispatch(pool, &b, &target)
                .await
                .map_err(|error| error.to_string())?;
            let model = expected.model.dispatch(&b, now);
            if (redis.dispatched, &redis.parent_fired, redis.parent_dispatch)
                != (model.dispatched, &model.parent_fired, model.parent_dispatch)
            {
                return Err(format!("dispatch: redis {redis:?} vs model {model:?}"));
            }
            expected.callbacks.extend(
                model
                    .pushed
                    .iter()
                    .map(|pushed| pushed.template.jid.clone()),
            );
        }
        Op::Delete(b, force) => {
            let b = bid(*b);
            let parent: Option<String> = cmd(
                pool,
                ::redis::cmd("HGET").arg(keys::batch(&b)).arg("parent"),
            )
            .await;
            let redis = scripts::delete(pool, &b, parent.as_deref(), *force).await;
            let model = expected.model.delete(&b, *force);
            match (redis, model) {
                (Ok(redis), Ok(model)) if redis == model => {}
                (Err(error), Err(expected_error))
                    if error_name(&error) == error_name(&expected_error) => {}
                (redis, model) => {
                    return Err(format!("delete: redis {redis:?} vs model {model:?}"));
                }
            }
        }
    }
    // The states.
    for n in 0..BIDS {
        let b = bid(n);
        let redis = redis_snapshot(pool, &b).await;
        let model = model_snapshot(&expected.model, &b);
        if redis != model {
            return Err(format!(
                "after {op:?}, batch {b}: redis {redis:?} vs model {model:?}"
            ));
        }
    }
    let outbox: Vec<String> = cmd(
        pool,
        ::redis::cmd("ZRANGE")
            .arg(keys::BATCH_EVENTS)
            .arg(0)
            .arg(-1),
    )
    .await;
    let outbox: BTreeSet<String> = outbox.into_iter().collect();
    let expected_outbox: BTreeSet<String> = expected.model.outbox.keys().cloned().collect();
    if outbox != expected_outbox {
        return Err(format!(
            "after {op:?}: outbox {outbox:?} vs {expected_outbox:?}"
        ));
    }
    let members = queue(pool, "q1").await;
    if members != expected.members {
        return Err(format!(
            "after {op:?}: members {members:?} vs {:?}",
            expected.members
        ));
    }
    let callbacks = queue(pool, "cb").await;
    if callbacks != expected.callbacks {
        return Err(format!(
            "after {op:?}: callbacks {callbacks:?} vs {:?}",
            expected.callbacks
        ));
    }
    invariants(&expected.model, &mut expected.events)
        .map_err(|message| format!("after {op:?}: {message}"))
}

#[test]
fn the_model_and_the_scripts_agree() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let redis = runtime.block_on(TestRedis::start());
    let pool = runtime.block_on(redis.pool());
    let cases = std::env::var("HENCH_BATCH_PARITY_CASES")
        .ok()
        .and_then(|cases| cases.parse().ok())
        .unwrap_or(256);
    let mut runner = TestRunner::new(Config {
        cases,
        failure_persistence: None,
        ..Config::default()
    });
    let result = runner.run(&proptest::collection::vec(gen_op(), 1..60), |ops| {
        runtime.block_on(async {
            cmd::<()>(&pool, &::redis::cmd("FLUSHALL")).await;
            let mut expected = Expected::default();
            for op in &ops {
                if let Err(message) = step(&pool, &mut expected, op).await {
                    return Err(TestCaseError::fail(message));
                }
            }
            Ok(())
        })
    });
    if let Err(error) = result {
        panic!("{error}");
    }
}

#[test]
fn the_parity_harness_catches_a_divergence() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let redis = TestRedis::start().await;
        let pool = redis.pool().await;
        let mut expected = Expected::default();
        let op = Op::Commit {
            cid: 0,
            reopen: false,
            release: false,
            batches: vec![GenBatch {
                bid: 1,
                parent: 0,
                callbacks: [false, true, false],
                hold: false,
            }],
            members: vec![(0, 1)],
        };
        step(&pool, &mut expected, &op).await.unwrap();
        step(&pool, &mut expected, &Op::Succeeded(1, 1))
            .await
            .unwrap();
        step(&pool, &mut expected, &Op::Dispatch(1)).await.unwrap();
        assert_eq!(expected.callbacks, ["cb-1-success"]);
        // A model that misses a write is caught by the next comparison.
        if let Some(batch) = expected.model.batches.get_mut(&bid(1)) {
            batch.skipped += 1;
        }
        assert!(step(&pool, &mut expected, &Op::Dispatch(1)).await.is_err());
    });
}

/// The round-3 rules, step by step on both sides (each also drawn by the random sequences): a
/// held batch fires nothing until a reopen or a cancel releases it (batches-1); a cancelled
/// batch finishes only once nothing is pending (batches-2); a dead member failing again with a
/// retry is no longer dead (batches-10).
#[test]
fn the_model_follows_holds_cancels_and_revivals() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let redis = TestRedis::start().await;
        let pool = redis.pool().await;
        let held = |bid, hold| GenBatch {
            bid,
            parent: 0,
            callbacks: [true, true, true],
            hold,
        };
        // Without callbacks: a child's callbacks are members of its parent, which the harness
        // never runs.
        let child = |bid| GenBatch {
            bid,
            parent: 1,
            callbacks: [false; 3],
            hold: false,
        };
        let commit = |cid, reopen, release, batches, members| Op::Commit {
            cid,
            reopen,
            release,
            batches,
            members,
        };
        let mut expected = Expected::default();
        let ops = [
            // Batch 1 held, with child 2 holding member 1.
            commit(0, false, false, vec![held(1, true), child(2)], vec![(1, 1)]),
            Op::Failed(2, 1, true),
            Op::Dispatch(2),
            Op::Succeeded(2, 1),
            Op::Dispatch(2),
            // Released by a reopen adding nothing: it fires at the commit.
            commit(1, true, true, vec![held(1, false)], vec![]),
            Op::Dispatch(1),
            // A dead member revived and failing with a retry.
            commit(0, false, false, vec![held(3, false)], vec![(0, 3), (0, 4)]),
            Op::Failed(3, 3, false),
            Op::Failed(3, 3, true),
            // A cancel while member 4 waits: complete is dispatched, the batch is not finished
            // until member 3 and member 4 are skipped.
            Op::Cancel(3),
            Op::Failed(3, 4, true),
            Op::Dispatch(3),
            Op::Skipped(3, 3, true),
            Op::Skipped(3, 4, true),
            // A cancel releases a hold.
            commit(0, false, false, vec![held(4, true), child(5)], vec![(1, 5)]),
            Op::Failed(5, 5, true),
            Op::Dispatch(5),
            Op::Cancel(4),
            Op::Dispatch(4),
        ];
        for op in &ops {
            step(&pool, &mut expected, op)
                .await
                .unwrap_or_else(|message| panic!("{message}"));
        }
        let snapshot = |bid| model_snapshot(&expected.model, &self::bid(bid)).unwrap();
        // Batch 1 fired once released; batch 3 finished with its last skip; batch 4 completed
        // at its cancel.
        assert_eq!(snapshot(1).times[..2], [true, true]);
        assert!(!snapshot(1).held);
        assert!(snapshot(3).dead.is_empty());
        assert!(snapshot(3).times[7], "batch 3 finished");
        assert!(!snapshot(4).held);
        assert!(snapshot(4).times[0], "batch 4 completed");
    });
}
