//! The `pro` scenario (`--pro`): unique jobs, periodic jobs, rate limiting and batches together,
//! under the same chaos as the other scenarios (DESIGN §10; X21 of
//! `docs/design/pro-features.md`).
//!
//! * **Workers** register three more jobs. `SoakPro` is a batch member throttled by `soak-pro`
//!   (3 at a time across every process, permits leased for 30 s and renewed by the heartbeat);
//!   it takes a slot of the window limiter `soak-pro-w` (8 a second; a refusal reschedules it),
//!   then sleeps and succeeds or fails as planned: `ok`, `errk` (fails its first attempt) or
//!   `aerr` (always fails; pushed without retries, so it dies at once). `SoakTick` is the job of
//!   the schedule `soak-tick` (every second, `unique_for` 10 s; runs of up to 2.5 s, so the
//!   ticks meanwhile are duplicates). `SoakProCb` is the batches' callback job. `SoakUniq` is a
//!   unique job pushed by the client (one lock per key among six, `unique_for` 20 s, half
//!   `success` and half `start`, a quarter of the pushes debounced with `replace`, a tenth
//!   failing their first attempt so that a retry holds the lock), throttled one run at a time
//!   per key by `soak-uniq` (a per-key throttle whose keys come and go).
//! * **Load** (`run`): every 2 to 4 s a batch of 5 to 20 members with `complete`, `success` and
//!   `death` callbacks (`batches.jsonl`); a fifth of the batches are cancelled 0.1 to 3 s after
//!   their commit. Members are logged in `pushes.jsonl` like the other jobs (kind `member`), so
//!   the usual checks apply to them. Once, 40% into the load, a burst batch of 150 short
//!   members (20 to 50 ms) makes a backlog of fifty times the throttle's slots; its drain is
//!   measured. The window limiter paces it, not the throttle: every member takes one of its 8
//!   grants a second, so the throttle's slots stay mostly free while the burst drains (20 to
//!   26% busy in the round-3 runs, before and after the throttle's drain fix, which
//!   `tests/throttle.rs` asserts). Every 0.2 to 0.6 s a unique job is pushed (`unique.jsonl`,
//!   with what the push did: pushed, duplicate, replaced). When the load ends the schedule is
//!   paused, and the drain also waits for the batch outbox to empty.
//! * **Snapshot** (`pro.json`, once the workers stopped): every batch's status with its pending
//!   and dead members, the outbox, the unique locks with their expiry, the limits' counters,
//!   the schedule and its history, and the expiry of every feature key.
//! * **Checks**: `check_pro.rs`.

use std::time::Duration;

use hench::api::Api;
use hench::prelude::*;
use hench::{HenchBuilder, OnConflict, PushOutcome, UniqueUntil, redis_rs};
use serde_json::{Value, json};

use crate::jsonlog::{JsonLog, now_ms};
use crate::worker::Recorder;

/// The throttle of the batch members.
pub const THROTTLE: &str = "soak-pro";
/// Its concurrency limit.
pub const THROTTLE_LIMIT: u32 = 3;
/// The window limiter the members take a slot of.
pub const WINDOW: &str = "soak-pro-w";
/// Its grants per second.
pub const WINDOW_LIMIT: u32 = 8;
/// The schedule.
pub const SCHEDULE: &str = "soak-tick";
/// The schedule's `unique_for`.
pub const UNIQUE_FOR: Duration = Duration::from_secs(10);
/// The workers' `periodic_lease` (shorter than the default, 60 s): a fault may stall the
/// schedule this long, and the checker requires ticks once the faults stop.
pub const PERIODIC_LEASE: Duration = Duration::from_secs(15);
/// The workers' `periodic_check_interval` (also shorter than the default): how long a schedule
/// erased with Redis's data waits to be registered again.
pub const PERIODIC_CHECK: Duration = Duration::from_secs(15);
/// The throttle of the unique jobs: one run at a time per key.
pub const UNIQUE_THROTTLE: &str = "soak-uniq";
/// The unique jobs' `unique_for`.
pub const UNIQUE_JOB_FOR: Duration = Duration::from_secs(20);
/// The keys the unique jobs spread over (few, so that many pushes are duplicates).
const UNIQUE_KEYS: u64 = 6;
/// Members of the burst batch.
pub const BURST_MEMBERS: u64 = 150;

/// A batch member. Args: (plan, param, nonce): `ok` sleeps `param` ms and succeeds, `errk`
/// fails while its attempt is below `param`, `aerr` always fails.
pub struct SoakPro {
    pub rec: Recorder,
}

impl Job for SoakPro {
    const NAME: &'static str = "SoakPro";
    type Args = (String, u64, u64);

    fn options() -> JobOptions {
        JobOptions::new()
            .queue("default")
            .retry(2_u32)
            .throttle(THROTTLE)
    }

    async fn perform(
        &self,
        ctx: &JobContext,
        (plan, param, nonce): (String, u64, u64),
    ) -> Result<(), JobError> {
        // Never waits: a refusal reschedules the job at its slot.
        let _permit = Limiter::window(WINDOW, WINDOW_LIMIT, Duration::from_secs(1))
            .max_wait(Duration::ZERO)
            .acquire(ctx)
            .await?;
        self.rec.write(json!({"e": "lg", "n": nonce}));
        let attempt = u64::from(ctx.retry_count().map_or(0, |count| count + 1));
        match plan.as_str() {
            "ok" => {
                tokio::time::sleep(Duration::from_millis(param)).await;
                Ok(())
            }
            "errk" => {
                tokio::time::sleep(Duration::from_millis(30)).await;
                if attempt < param {
                    Err(JobError::new(
                        "soak::PlannedFailure",
                        format!("planned failure (attempt {attempt})"),
                    ))
                } else {
                    Ok(())
                }
            }
            "aerr" => {
                tokio::time::sleep(Duration::from_millis(30)).await;
                Err(JobError::new(
                    "soak::PlannedFailure",
                    format!("planned permanent failure (attempt {attempt})"),
                ))
            }
            other => Err(JobError::new("soak::UnknownPlan", other.to_owned())),
        }
    }

    fn retry_in(&self, _count: u32, _error: &JobError, _job: &JobPayload) -> RetryIn {
        RetryIn::After(Duration::from_secs(1))
    }
}

/// The job of the schedule [`SCHEDULE`]: runs for up to 2.5 s.
pub struct SoakTick;

impl Job for SoakTick {
    const NAME: &'static str = "SoakTick";
    type Args = ();

    async fn perform(&self, _ctx: &JobContext, (): ()) -> Result<(), JobError> {
        tokio::time::sleep(Duration::from_millis(fastrand::u64(0..2_500))).await;
        Ok(())
    }
}

/// A unique job pushed by the client: one lock per key ([`Job::unique_key`]), one run at a time
/// per key ([`UNIQUE_THROTTLE`], [`Job::throttle_key`]). Args: (key, id, plan): `ok` succeeds
/// after 50 to 400 ms, `retry` fails its first attempt.
pub struct SoakUniq;

impl Job for SoakUniq {
    const NAME: &'static str = "SoakUniq";
    type Args = (u64, u64, String);

    fn options() -> JobOptions {
        JobOptions::new()
            .queue("default")
            .retry(2_u32)
            .unique_for(UNIQUE_JOB_FOR)
            .throttle(UNIQUE_THROTTLE)
    }

    fn unique_key((key, _, _): &Self::Args) -> Option<Value> {
        Some(json!([key]))
    }

    fn throttle_key((key, _, _): &Self::Args) -> Option<String> {
        Some(key.to_string())
    }

    async fn perform(
        &self,
        ctx: &JobContext,
        (_key, _id, plan): (u64, u64, String),
    ) -> Result<(), JobError> {
        tokio::time::sleep(Duration::from_millis(fastrand::u64(50..400))).await;
        if plan == "retry" && ctx.retry_count().is_none() {
            return Err(JobError::new(
                "soak::PlannedFailure",
                "planned failure of a unique job (its retry holds the lock)",
            ));
        }
        Ok(())
    }

    fn retry_in(&self, _count: u32, _error: &JobError, _job: &JobPayload) -> RetryIn {
        RetryIn::After(Duration::from_secs(1))
    }
}

/// The batches' callback job. Args: the batch's nonce.
pub struct SoakProCb;

impl Job for SoakProCb {
    const NAME: &'static str = "SoakProCb";
    type Args = (u64,);

    async fn perform(&self, _ctx: &JobContext, _: (u64,)) -> Result<(), JobError> {
        tokio::time::sleep(Duration::from_millis(5)).await;
        Ok(())
    }
}

/// A worker's builder with the pro jobs, the throttle and the schedule.
pub fn configure(builder: HenchBuilder, rec: &Recorder) -> HenchBuilder {
    builder
        .register(SoakPro { rec: rec.clone() })
        .register(SoakTick)
        .register(SoakProCb)
        .register(SoakUniq)
        .throttle(
            Throttle::new(UNIQUE_THROTTLE)
                .concurrency_per_key(1)
                .lease(Duration::from_secs(30)),
        )
        .throttle(
            Throttle::new(THROTTLE)
                .concurrency(THROTTLE_LIMIT)
                .lease(Duration::from_secs(30)),
        )
        .periodic(
            Periodic::every(SCHEDULE, Duration::from_secs(1))
                .job::<SoakTick>(())
                .unique_for(UNIQUE_FOR),
        )
        .periodic_lease(PERIODIC_LEASE)
        .periodic_check_interval(PERIODIC_CHECK)
        // Refusals of the window limiter reschedule; never let them reach the failure path
        // (the checker counts them apart from failures).
        .over_limit_reschedules(1_000)
}

/// One member's plan: (plan, param, retry).
fn pick_member() -> (&'static str, u64, u32) {
    match fastrand::u32(0..100) {
        0..85 => ("ok", fastrand::u64(20..300), 2),
        85..97 => ("errk", 1, 2),
        _ => ("aerr", 0, 0),
    }
}

/// Defines and commits one batch of `count` members (nonces `first_nonce..first_nonce +
/// count`; the batch's own nonce, given to its callbacks, is `first_nonce + count`). Returns its
/// bid once committed. The members of a `burst` all succeed after 20 to 50 ms.
pub async fn push_batch(
    client: &Client,
    pushes: &JsonLog,
    batches: &JsonLog,
    first_nonce: u64,
    count: u64,
    burst: bool,
) -> Option<String> {
    let batch_nonce = first_nonce + count;
    let members: Vec<(u64, String, &'static str, u64, u32)> = (0..count)
        .map(|offset| {
            let (plan, param, retry) = if burst {
                ("ok", fastrand::u64(20..50), 2)
            } else {
                pick_member()
            };
            (
                first_nonce + offset,
                hench::generate_jid(),
                plan,
                param,
                retry,
            )
        })
        .collect();
    let batch = Batch::new(client)
        .description(format!("soak {batch_nonce}"))
        .commit_id(format!("soak-{batch_nonce}"))
        .on_complete::<SoakProCb>((batch_nonce,))
        .on_success::<SoakProCb>((batch_nonce,))
        .on_death::<SoakProCb>((batch_nonce,));
    let bid = batch.bid().to_owned();
    let started = now_ms();
    let definition = members.clone();
    let result = batch
        .jobs(|jobs| async move {
            for (nonce, jid, plan, param, retry) in definition {
                jobs.job::<SoakPro>()
                    .jid(jid)
                    .retry(retry)
                    .perform_async((plan.to_owned(), param, nonce))
                    .await?;
            }
            Ok::<_, hench::Error>(())
        })
        .await;
    let ok = result.is_ok();
    let error = result.as_ref().err().map(ToString::to_string);
    for (nonce, jid, plan, param, retry) in &members {
        pushes.write(&json!({
            "e": "push", "k": "member", "n": nonce, "j": jid, "plan": plan, "param": param,
            "q": "default", "retry": retry, "dead": Value::Null, "at": Value::Null, "b": bid,
            "ok": ok, "err": error, "t0": started, "t": now_ms(),
        }));
    }
    batches.write(&json!({
        "e": "batch", "bid": bid, "n": batch_nonce,
        "members": members.iter().map(|member| member.0).collect::<Vec<_>>(),
        "jids": members.iter().map(|member| member.1.clone()).collect::<Vec<_>>(),
        "committed": result.as_ref().map(|committed| committed.jids.len()).ok(),
        "ok": ok, "err": error, "t0": started, "t": now_ms(), "burst": burst,
    }));
    ok.then_some(bid)
}

/// Pushes unique job `id` and logs what the push did (`unique.jsonl`): a key among a few,
/// `success` or `start`, and one push in four a debounce (`replace`, 1 to 3 s ahead).
pub async fn push_unique(client: &Client, log: &JsonLog, id: u64) {
    let key = fastrand::u64(0..UNIQUE_KEYS);
    let start = fastrand::bool();
    let replace = fastrand::u32(0..4) == 0;
    let plan = if fastrand::u32(0..10) == 0 {
        "retry"
    } else {
        "ok"
    };
    let mut setter = SoakUniq::set().unique_until(if start {
        UniqueUntil::Start
    } else {
        UniqueUntil::Success
    });
    let delay_ms = if replace {
        fastrand::u64(1_000..3_000)
    } else {
        0
    };
    if replace {
        setter = setter
            .on_conflict(OnConflict::Replace)
            .delay(Duration::from_millis(delay_ms));
    }
    let started = now_ms();
    let outcome = setter
        .push_outcome(client, (key, id, plan.to_owned()))
        .await;
    let (what, jid, other, error) = match outcome {
        Ok(PushOutcome::Pushed { jid }) => ("pushed", Some(jid), None, None),
        Ok(PushOutcome::Replaced { jid, replaced }) => {
            ("replaced", Some(jid), Some(replaced), None)
        }
        Ok(PushOutcome::Duplicate { holder, .. }) => ("duplicate", None, Some(holder), None),
        Ok(_) => ("stopped", None, None, None),
        Err(error) => ("error", None, None, Some(error.to_string())),
    };
    log.write(&json!({
        "e": "u", "id": id, "key": key, "until": if start { "start" } else { "success" },
        "replace": replace, "delay": delay_ms, "plan": plan, "outcome": what, "j": jid,
        "other": other, "err": error, "t0": started, "t": now_ms(),
    }));
}

/// Cancels batch `bid` after `delay`.
pub async fn cancel_later(client: Client, batches: &JsonLog, bid: String, delay: Duration) {
    tokio::time::sleep(delay).await;
    let started = now_ms();
    let result = client.cancel_batch(&bid).await;
    batches.write(&json!({
        "e": "cancel", "bid": bid, "ok": result.is_ok(),
        "cancelled": result.as_ref().ok(),
        "err": result.as_ref().err().map(ToString::to_string),
        "t0": started, "t": now_ms(),
    }));
}

/// Pauses the schedule (the drain needs its ticks to stop), retried while Redis is away.
pub async fn pause_schedule(pool: RedisPool) -> Value {
    let api = Api::new(pool);
    let mut last = String::new();
    for _ in 0..30 {
        match api.periodic().pause(SCHEDULE).await {
            Ok(paused) => return json!({"ok": true, "paused": paused}),
            Err(error) => last = error.to_string(),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    json!({"ok": false, "err": last})
}

/// Milliseconds since the epoch of an optional time.
fn ms(time: Option<std::time::SystemTime>) -> Value {
    time.and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map_or(Value::Null, |since| json!(since.as_millis() as u64))
}

/// Every key matching `pattern`.
async fn scan(
    conn: &mut redis_rs::aio::MultiplexedConnection,
    pattern: &str,
) -> anyhow::Result<Vec<String>> {
    let mut keys = Vec::new();
    let mut cursor: u64 = 0;
    loop {
        let (next, page): (u64, Vec<String>) = redis_rs::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(pattern)
            .arg("COUNT")
            .arg(1_000)
            .query_async(conn)
            .await?;
        keys.extend(page);
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    keys.sort();
    keys.dedup();
    Ok(keys)
}

/// The feature state after the run (`pro.json`): `bids` are the batches the load committed.
pub async fn snapshot(port: u16, bids: &[String]) -> anyhow::Result<Value> {
    let pool = RedisPool::connect(&format!("redis://127.0.0.1:{port}/")).await?;
    let api = Api::new(pool);
    let mut conn = crate::orchestrator::admin(port).await?;

    let mut batches = serde_json::Map::new();
    for bid in bids {
        let status = api.batches().fetch(bid).await?;
        let (pending, dead): (Vec<String>, Vec<String>) = redis_rs::pipe()
            .cmd("SMEMBERS")
            .arg(hench::keys::batch_pending(bid))
            .cmd("SMEMBERS")
            .arg(hench::keys::batch_dead(bid))
            .query_async(&mut conn)
            .await?;
        let value = match status {
            None => Value::Null,
            Some(status) => json!({
                "total": status.total, "pending": status.pending, "failures": status.failures,
                "dead": status.dead, "skipped": status.skipped,
                "complete_at": ms(status.complete_at), "success_at": ms(status.success_at),
                "death_at": ms(status.death_at),
                "complete_dispatched_at": ms(status.complete_dispatched_at),
                "success_dispatched_at": ms(status.success_dispatched_at),
                "death_dispatched_at": ms(status.death_dispatched_at),
                "cancelled_at": ms(status.cancelled_at), "finished_at": ms(status.finished_at),
                "undispatched": status.undispatched,
                "pending_jids": pending, "dead_jids": dead,
            }),
        };
        batches.insert(bid.clone(), value);
    }
    let outbox: u64 = redis_rs::cmd("ZCARD")
        .arg(hench::keys::BATCH_EVENTS)
        .query_async(&mut conn)
        .await?;

    // Every feature key with its expiry (-1: none), and the content of the locks and limits.
    let mut ttls = serde_json::Map::new();
    let mut locks = Vec::new();
    let mut limit_stats = serde_json::Map::new();
    for pattern in [
        "unique*",
        "batch*",
        "limit*",
        "periodic*",
        "queue-limits",
        "u|*",
        "b|*",
        "l|*",
        "j|*",
    ] {
        for key in scan(&mut conn, pattern).await? {
            let ttl: i64 = redis_rs::cmd("PTTL")
                .arg(&key)
                .query_async(&mut conn)
                .await?;
            if key.starts_with(hench::keys::UNIQUE_LOCK_PREFIX) {
                let fields: std::collections::BTreeMap<String, String> = redis_rs::cmd("HGETALL")
                    .arg(&key)
                    .query_async(&mut conn)
                    .await?;
                locks.push(json!({"key": key, "pttl": ttl, "fields": fields}));
            } else if key.starts_with("limit-stats:") {
                let fields: std::collections::BTreeMap<String, String> = redis_rs::cmd("HGETALL")
                    .arg(&key)
                    .query_async(&mut conn)
                    .await?;
                limit_stats.insert(key.clone(), json!(fields));
            }
            ttls.insert(key, json!(ttl));
        }
    }
    let schedule: std::collections::BTreeMap<String, String> = redis_rs::cmd("HGETALL")
        .arg(hench::keys::periodic(SCHEDULE))
        .query_async(&mut conn)
        .await?;
    let history: Vec<String> = redis_rs::cmd("LRANGE")
        .arg(hench::keys::periodic_history(SCHEDULE))
        .arg(0)
        .arg(-1)
        .query_async(&mut conn)
        .await?;
    Ok(json!({
        "t": now_ms(), "batches": batches, "outbox": outbox, "ttls": ttls, "locks": locks,
        "limit_stats": limit_stats, "schedule": schedule, "history": history,
    }))
}
