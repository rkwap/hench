//! The `pro` scenario's checks, from `pro.json`, `batches.jsonl`, `chaos.jsonl` and the
//! workers' execution logs (X21 of `docs/design/pro-features.md`):
//!
//! * **Ticks**: each tick of the schedule was enqueued at most once (one jid per tick), and two
//!   ticks' jobs never ran at the same time while the first one's lock could hold (ticks less
//!   than `unique_for` apart); and ticks kept coming: no stretch of the load without one longer
//!   than the poll interval, `unique_for` and a tick's run explain, plus, for each fault in the
//!   stretch, its duration and the lease a lost reply or a dead poller leaves (and the
//!   registration check after a restart that lost Redis's data).
//! * **Batches**: every committed batch exists with all its members (but those a documented data
//!   loss erased: committed before a restart without persistence, or within the second before
//!   a `kill -9` of Redis with AOF `everysec`); no member is left pending
//!   but the dead ones; `complete` fired; `success` fired exactly when nothing died and the
//!   batch was not cancelled; `death` exactly when a member died; every event that fired was
//!   dispatched and its callback ran, no other callback ran, and a callback that ran twice is
//!   explained by a crash or a fault; the members counted as succeeded are those seen
//!   succeeding; the outbox is empty.
//! * **Locks**: every unique lock left expires, within its `for`.
//! * **Throttle**: at most 3 members ran at any moment, plus the overages the limit counted.
//!   The burst batch's drain is measured (`burst_*`: how long its 150 members took, and how
//!   busy the throttle's slots were meanwhile), not asserted: the window limiter's 8 grants a
//!   second pace it, so the slots' use says how busy the limiter kept them, not how well the
//!   throttle drains (`tests/throttle.rs` asserts that).
//! * **Unique jobs** (`unique.jsonl`): a push is accepted only once the job holding the key's
//!   lock released it (`start`: it started; `success`: it succeeded) or its lock lapsed, but
//!   for a debounce that replaced it; a replaced job never runs; two jobs of one key never run
//!   at once (the per-key throttle); every accepted job ran to success. A fault near the
//!   moment explains a break (locks of killed jobs lapse, leases of killed processes end).
//! * **Keys**: every feature key expires, but the configuration and index keys.
//!
//! The usual checks apply to the members too (`check.rs`: no member lost, extra runs and
//! concurrent runs explained, `stat:processed` and `stat:failed`): a member with no successful
//! run counts as skipped when its batch was cancelled and the member is neither pending nor
//! dead there ([`Facts::skipped`]).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;

use serde_json::{Value, json};

use crate::jsonlog::read_all;
use crate::pro::{
    PERIODIC_CHECK, PERIODIC_LEASE, THROTTLE, THROTTLE_LIMIT, UNIQUE_FOR, UNIQUE_JOB_FOR,
};

/// The workers' `periodic_lease`: how long a schedule may wait after its poller died or lost
/// the reply of the lease.
const PERIODIC_LEASE_MS: i64 = PERIODIC_LEASE.as_millis() as i64;
/// The workers' `periodic_check_interval`: how long a schedule erased with Redis's data may wait
/// to be registered again.
const PERIODIC_CHECK_MS: i64 = PERIODIC_CHECK.as_millis() as i64;
/// The longest stretch without a tick with no fault in it: the poll interval (5 s), `unique_for`
/// (a tick whose job still waits or runs is a duplicate), a tick job's run (2.5 s) and a margin.
const TICK_GAP_MS: i64 = 5_000 + UNIQUE_FOR.as_millis() as i64 + 2_500 + 12_500;

/// What the generic checks need to know of the pro run.
pub struct Facts {
    /// The batches' state after the run (`pro.json`), by bid.
    batches: HashMap<String, Value>,
}

impl Facts {
    /// The facts of a run with the pro load (`None` without `pro.json`).
    pub fn load(dir: &Path) -> anyhow::Result<Option<Facts>> {
        let path = dir.join("pro.json");
        if !path.exists() {
            return Ok(None);
        }
        let pro: Value = serde_json::from_slice(&std::fs::read(path)?)?;
        let batches = pro["batches"]
            .as_object()
            .map(|batches| {
                batches
                    .iter()
                    .map(|(bid, status)| (bid.clone(), status.clone()))
                    .collect()
            })
            .unwrap_or_default();
        Ok(Some(Facts { batches }))
    }

    /// Whether a member with no successful run was skipped: its batch was cancelled, and the
    /// member is neither pending nor dead in it.
    pub fn skipped(&self, bid: Option<&str>, jid: Option<&str>) -> bool {
        let (Some(bid), Some(jid)) = (bid, jid) else {
            return false;
        };
        let Some(status) = self.batches.get(bid) else {
            return false;
        };
        let listed = |field: &str| {
            status[field]
                .as_array()
                .is_some_and(|jids| jids.iter().any(|listed| listed == jid))
        };
        !status["cancelled_at"].is_null() && !listed("pending_jids") && !listed("dead_jids")
    }
}

/// One execution of a pro job.
#[derive(Debug, Clone)]
struct Run {
    wid: String,
    class: String,
    jid: String,
    start: i64,
    /// `None`: it never ended and its process has no recorded exit.
    end: Option<i64>,
    outcome: String,
    tick: Option<i64>,
    callback: Option<(String, String)>,
}

/// The executions of the pro jobs (`SoakPro`, `SoakTick`, `SoakProCb`), from every worker's
/// log; an execution its process never ended ends at that process's exit (`killed` after a
/// `kill -9`, else `vanished`).
fn runs(dir: &Path, exits: &HashMap<String, (i64, String)>) -> anyhow::Result<Vec<Run>> {
    let mut runs = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if !(name.starts_with("exec-") && name.ends_with(".jsonl")) {
            continue;
        }
        let wid = name
            .trim_start_matches("exec-")
            .trim_end_matches(".jsonl")
            .to_owned();
        let mut open: HashMap<(String, u64), Vec<Run>> = HashMap::new();
        for record in read_all(&path)? {
            let jid = record["j"].as_str().unwrap_or("").to_owned();
            let attempt = record["a"].as_u64().unwrap_or(0);
            let t = record["t"].as_i64().unwrap_or(0);
            match record["e"].as_str().unwrap_or("") {
                "s" => {
                    let class = record["c"].as_str().unwrap_or("");
                    if !matches!(class, "SoakPro" | "SoakTick" | "SoakProCb" | "SoakUniq") {
                        continue;
                    }
                    let callback = record["cb"].as_array().and_then(|pair| {
                        Some((
                            pair.first()?.as_str()?.to_owned(),
                            pair.get(1)?.as_str()?.to_owned(),
                        ))
                    });
                    open.entry((jid.clone(), attempt)).or_default().push(Run {
                        wid: wid.clone(),
                        class: class.to_owned(),
                        jid,
                        start: t,
                        end: None,
                        outcome: String::new(),
                        tick: record["pt"].as_i64(),
                        callback,
                    });
                }
                "e" | "x" => {
                    if let Some(mut run) = open.get_mut(&(jid, attempt)).and_then(Vec::pop) {
                        run.end = Some(t);
                        run.outcome = record["o"].as_str().unwrap_or("").to_owned();
                        runs.push(run);
                    }
                }
                _ => {}
            }
        }
        for (_, list) in open {
            for mut run in list {
                if let Some((t, how)) = exits.get(&wid) {
                    run.end = Some(*t);
                    run.outcome = if how == "kill9" || how == "kill_after_stuck" {
                        "killed".into()
                    } else {
                        "vanished".into()
                    };
                } else {
                    run.outcome = "vanished".into();
                }
                runs.push(run);
            }
        }
    }
    runs.sort_by_key(|run| run.start);
    Ok(runs)
}

/// Whether two runs overlap in time (a run without an end is ignored).
fn overlap(a: &Run, b: &Run) -> bool {
    match (a.end, b.end) {
        (Some(a_end), Some(b_end)) => a.start < b_end && b.start < a_end,
        _ => false,
    }
}

/// Runs the checks; returns the report's `pro` section and the violations found.
pub fn check(dir: &Path) -> anyhow::Result<(Value, Vec<Value>)> {
    let path = dir.join("pro.json");
    if !path.exists() {
        return Ok((Value::Null, Vec::new()));
    }
    let pro: Value = serde_json::from_slice(&std::fs::read(path)?)?;
    let mut violations = Vec::new();
    if let Some(error) = pro["error"].as_str() {
        violations.push(json!({"type": "PRO_SNAPSHOT_FAILED", "error": error}));
        return Ok((json!({"summary": "no snapshot"}), violations));
    }

    // ---- the chaos log: worker exits, kills and fault windows (to explain repeated callbacks)
    let chaos = read_all(&dir.join("chaos.jsonl"))?;
    let mut exits: HashMap<String, (i64, String)> = HashMap::new();
    let mut disruptions: Vec<(i64, i64, String)> = Vec::new();
    let mut redis_down: Option<(i64, String)> = None;
    // Restarts that lost Redis's data (`--persist none`), and the stretches before a `kill -9`
    // of Redis with AOF `everysec` whose writes may be lost: what was committed then may be
    // gone, as the generic checks allow.
    let mut data_loss: Vec<i64> = Vec::new();
    let mut lossy: Vec<(i64, i64)> = Vec::new();
    let mut stopped: HashMap<String, i64> = HashMap::new();
    let mut queue_paused: Option<i64> = None;
    let (mut load_start, mut load_end) = (None, None);
    for event in &chaos {
        let t = event["t"].as_i64().unwrap_or(0);
        let wid = event["w"].as_str().unwrap_or("").to_owned();
        match event["e"].as_str().unwrap_or("") {
            "scenario" => load_start = Some(t),
            "load_end" => load_end = Some(t),
            "exit" => {
                let how = event["how"].as_str().unwrap_or("").to_owned();
                if let Some(since) = stopped.remove(&wid) {
                    disruptions.push((since, t, format!("stopped {wid}")));
                }
                if how == "kill9" || how == "kill_after_stuck" || how == "term" {
                    disruptions.push((t - 1_000, t, format!("{how} {wid}")));
                }
                exits.insert(wid, (t, how));
            }
            // A worker stopped (SIGTSTP) or told to stop or quiet holds what it held until it
            // exits.
            "tstp" | "dterm" => {
                stopped.entry(wid).or_insert(t);
            }
            "remote_stop" | "remote_quiet" => disruptions.push((t, t + 6_000, "remote".into())),
            "queue_pause" => queue_paused = Some(t),
            "queue_unpause" => {
                if let Some(since) = queue_paused.take() {
                    disruptions.push((since, t, "queue paused".into()));
                }
            }
            "redis_down" => {
                redis_down = Some((t, event["how"].as_str().unwrap_or("").to_owned()));
            }
            "redis_up" => {
                if let Some((down, how)) = redis_down.take() {
                    disruptions.push((down, t, "redis restart".into()));
                    if event["persist"] == "none" {
                        data_loss.push(down);
                    } else if how == "redis_kill9" {
                        lossy.push((down - 2_000, down));
                    }
                }
            }
            "proxy_blackhole" | "proxy_delay" | "client_pause" => {
                let ms = event["ms"].as_i64().unwrap_or(0);
                disruptions.push((t, t + ms, event["e"].as_str().unwrap_or("").to_owned()));
            }
            "proxy_reset" => disruptions.push((t, t + 100, "proxy reset".into())),
            _ => {}
        }
    }
    let runs = runs(dir, &exits)?;

    // ---- ticks: one jid per tick; no two ticks' jobs at once while a lock could hold
    let ticks: Vec<&Run> = runs.iter().filter(|run| run.class == "SoakTick").collect();
    let mut jids_of_tick: BTreeMap<i64, BTreeSet<String>> = BTreeMap::new();
    for run in &ticks {
        if let Some(tick) = run.tick {
            jids_of_tick
                .entry(tick)
                .or_default()
                .insert(run.jid.clone());
        }
    }
    for (tick, jids) in &jids_of_tick {
        if jids.len() > 1 {
            violations.push(json!({"type": "TICK_ENQUEUED_TWICE", "tick": tick, "jids": jids}));
        }
    }
    let unique_for_ms = UNIQUE_FOR.as_millis() as i64;
    let mut tick_overlaps_after_lapse = 0;
    for (index, a) in ticks.iter().enumerate() {
        for b in &ticks[index + 1..] {
            if a.jid == b.jid || !overlap(a, b) {
                continue;
            }
            let apart = (a.tick.unwrap_or(0) - b.tick.unwrap_or(0)).abs();
            if apart < unique_for_ms {
                violations.push(json!({"type": "TICK_RUNS_OVERLAP", "ticks_apart_ms": apart,
                    "a": {"jid": a.jid, "tick": a.tick, "w": a.wid, "start": a.start, "end": a.end},
                    "b": {"jid": b.jid, "tick": b.tick, "w": b.wid, "start": b.start, "end": b.end}}));
            } else {
                // The older job's lock had lapsed (it waited longer than `unique_for`, its
                // process crashed): unique jobs promise nothing more then.
                tick_overlaps_after_lapse += 1;
            }
        }
    }

    // ---- ticks keep coming
    let mut tick_gap_max = 0;
    if let (Some(start), Some(end)) = (load_start, load_end) {
        let (longest, stopped) = ticks_stopped(
            jids_of_tick.keys().copied(),
            (start, end),
            &disruptions,
            &data_loss,
        );
        tick_gap_max = longest;
        violations.extend(stopped);
    }

    // ---- batches
    let batch_log = read_all(&dir.join("batches.jsonl")).unwrap_or_default();
    let statuses = pro["batches"].as_object().cloned().unwrap_or_default();
    let outbox = pro["outbox"].as_u64().unwrap_or(0);
    if outbox > 0 {
        violations.push(json!({"type": "OUTBOX_NOT_EMPTY", "batches": outbox}));
    }
    let mut ok_members: HashMap<&str, bool> = HashMap::new();
    for run in runs.iter().filter(|run| run.class == "SoakPro") {
        let ok = ok_members.entry(run.jid.as_str()).or_default();
        *ok |= run.outcome == "ok";
    }
    let mut callback_runs: HashMap<(String, String), Vec<&Run>> = HashMap::new();
    for run in runs.iter().filter(|run| run.class == "SoakProCb") {
        if let Some(callback) = &run.callback {
            callback_runs.entry(callback.clone()).or_default().push(run);
        }
    }
    let mut counts: BTreeMap<&str, u64> = BTreeMap::new();
    let mut seen_bids = BTreeSet::new();
    for record in batch_log.iter().filter(|record| record["e"] == "batch") {
        let bid = record["bid"].as_str().unwrap_or("").to_owned();
        if record["ok"] != true {
            *counts.entry("commits_failed").or_default() += 1;
            continue;
        }
        seen_bids.insert(bid.clone());
        *counts.entry("batches").or_default() += 1;
        let jids: Vec<String> = record["jids"]
            .as_array()
            .map(|jids| {
                jids.iter()
                    .filter_map(|jid| jid.as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default();
        let Some(status) = statuses.get(&bid).filter(|status| !status.is_null()) else {
            // Committed before a restart that lost Redis's data, or in the second of writes
            // a `kill -9` of Redis with AOF `everysec` may lose: a documented loss.
            let committed = record["t"].as_i64().unwrap_or(0);
            if lost_with_redis_data(committed, &data_loss, &lossy) {
                *counts.entry("batches_lost_with_redis_data").or_default() += 1;
            } else {
                violations.push(json!({"type": "BATCH_MISSING", "bid": bid}));
            }
            continue;
        };
        let number = |field: &str| status[field].as_u64().unwrap_or(0);
        let set = |field: &str| !status[field].is_null();
        let (total, pending, failures, dead, skipped) = (
            number("total"),
            number("pending"),
            number("failures"),
            number("dead"),
            number("skipped"),
        );
        let cancelled = set("cancelled_at");
        if cancelled {
            *counts.entry("batches_cancelled").or_default() += 1;
        }
        if total != jids.len() as u64 {
            violations.push(json!({"type": "BATCH_TOTAL", "bid": bid, "total": total,
                "members": jids.len()}));
        }
        let dead_jids: BTreeSet<&str> = status["dead_jids"]
            .as_array()
            .map(|jids| jids.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default();
        let left: Vec<&str> = status["pending_jids"]
            .as_array()
            .map(|jids| {
                jids.iter()
                    .filter_map(Value::as_str)
                    .filter(|jid| !dead_jids.contains(jid))
                    .collect()
            })
            .unwrap_or_default();
        if !left.is_empty() {
            violations.push(json!({"type": "BATCH_MEMBERS_LEFT_PENDING", "bid": bid,
                "jids": left}));
        }
        if failures != dead {
            violations.push(json!({"type": "BATCH_FAILURES_NOT_DEAD", "bid": bid,
                "failures": failures, "dead": dead}));
        }
        if !set("complete_at") {
            violations.push(json!({"type": "BATCH_NOT_COMPLETE", "bid": bid,
                "pending": pending, "failures": failures}));
        }
        let should_succeed = !cancelled && dead == 0;
        if set("success_at") != should_succeed {
            violations.push(json!({"type": "BATCH_SUCCESS_WRONG", "bid": bid,
                "success": set("success_at"), "cancelled": cancelled, "dead": dead}));
        }
        if set("death_at") != (dead > 0) {
            violations.push(json!({"type": "BATCH_DEATH_WRONG", "bid": bid,
                "death": set("death_at"), "dead": dead}));
        }
        if status["undispatched"] == true {
            violations.push(json!({"type": "BATCH_UNDISPATCHED", "bid": bid}));
        }
        // Members counted as succeeded against members seen succeeding.
        let succeeded = total.saturating_sub(pending).saturating_sub(skipped);
        let observed = jids
            .iter()
            .filter(|jid| ok_members.get(jid.as_str()).copied().unwrap_or(false))
            .count() as u64;
        if succeeded > observed || (succeeded < observed && !cancelled) {
            violations.push(json!({"type": "BATCH_SUCCEEDED_MISMATCH", "bid": bid,
                "counted": succeeded, "observed": observed, "cancelled": cancelled}));
        } else if succeeded < observed {
            // Ran successfully, then skipped: its outcome was lost to a crash and the re-run
            // found the batch cancelled.
            *counts.entry("members_succeeded_then_skipped").or_default() += observed - succeeded;
        }
        *counts.entry("members").or_default() += total;
        *counts.entry("members_skipped").or_default() += skipped;
        *counts.entry("members_dead").or_default() += dead;
        // Each event fired was dispatched and its callback ran; no other callback ran.
        for event in ["complete", "success", "death"] {
            let fired = set(&format!("{event}_at"));
            let dispatched = set(&format!("{event}_dispatched_at"));
            let list = callback_runs
                .get(&(bid.clone(), event.to_owned()))
                .cloned()
                .unwrap_or_default();
            let ok_runs: Vec<&&Run> = list.iter().filter(|run| run.outcome == "ok").collect();
            if fired {
                *counts.entry("events").or_default() += 1;
                if !dispatched {
                    violations.push(json!({"type": "EVENT_NOT_DISPATCHED", "bid": bid,
                        "event": event}));
                }
                if ok_runs.is_empty() {
                    violations.push(json!({"type": "CALLBACK_NOT_RUN", "bid": bid,
                        "event": event, "runs": list.len()}));
                }
            } else if !list.is_empty() {
                violations.push(json!({"type": "CALLBACK_WITHOUT_EVENT", "bid": bid,
                    "event": event, "runs": list.len()}));
            }
            // A callback that succeeded twice: its outcome write was lost to a crash or a
            // fault after the first success, or it was dispatched twice (a violation).
            for pair in ok_runs.windows(2) {
                let end = pair[0].end.unwrap_or(pair[0].start);
                let explained = disruptions
                    .iter()
                    .any(|(from, to, _)| *to + 15_000 >= end && *from - 5_000 <= end);
                if explained {
                    *counts.entry("callbacks_rerun_after_a_fault").or_default() += 1;
                } else {
                    violations.push(json!({"type": "CALLBACK_RAN_TWICE", "bid": bid,
                        "event": event, "first": {"w": pair[0].wid, "start": pair[0].start,
                        "end": pair[0].end}, "second": {"w": pair[1].wid,
                        "start": pair[1].start, "end": pair[1].end}}));
                }
            }
        }
    }
    let foreign_callbacks = callback_runs
        .keys()
        .filter(|(bid, _)| !seen_bids.contains(bid))
        .count();
    if foreign_callbacks > 0 {
        violations.push(json!({"type": "CALLBACK_OF_AN_UNKNOWN_BATCH",
            "callbacks": foreign_callbacks}));
    }

    // ---- unique locks: each one left expires within its `for`
    let locks = pro["locks"].as_array().cloned().unwrap_or_default();
    for lock in &locks {
        let ttl = lock["pttl"].as_i64().unwrap_or(-1);
        let for_ms = lock["fields"]["for"]
            .as_str()
            .and_then(|secs| secs.parse::<i64>().ok())
            .unwrap_or(0)
            * 1_000;
        if ttl == -1 {
            violations.push(json!({"type": "LOCK_WITHOUT_TTL", "lock": lock}));
        } else if ttl > for_ms + 2_000 {
            violations.push(json!({"type": "LOCK_OUTLIVES_ITS_FOR", "lock": lock}));
        }
    }

    // ---- the throttle: members running at once, against the limit and the overages
    let mut points: Vec<(i64, i32)> = Vec::new();
    for run in runs.iter().filter(|run| run.class == "SoakPro") {
        if let Some(end) = run.end {
            points.push((run.start, 1));
            points.push((end, -1));
        }
    }
    // Ends before starts at the same millisecond: a slot handed over is not an overlap.
    points.sort_unstable();
    let (mut running, mut max_running, mut above_since, mut above_ms) = (0, 0, None, 0_i64);
    for (t, delta) in points {
        running += delta;
        max_running = max_running.max(running);
        if running > THROTTLE_LIMIT as i32 {
            above_since.get_or_insert(t);
        } else if let Some(since) = above_since.take() {
            above_ms += t - since;
        }
    }
    let overages: u64 = pro["limit_stats"]
        .as_object()
        .map(|stats| {
            stats
                .iter()
                .filter(|(key, _)| key.as_str() == format!("limit-stats:c:{THROTTLE}"))
                .filter_map(|(_, fields)| fields["ov"].as_str()?.parse::<u64>().ok())
                .sum()
        })
        .unwrap_or(0);
    if max_running > THROTTLE_LIMIT as i32 + overages as i32 {
        violations.push(
            json!({"type": "THROTTLE_LIMIT_EXCEEDED", "max_running": max_running,
            "limit": THROTTLE_LIMIT, "overages": overages, "ms_above_limit": above_ms}),
        );
    }

    // ---- unique jobs pushed by the client
    let unique_log = read_all(&dir.join("unique.jsonl")).unwrap_or_default();
    let near_fault = |from: i64, to: i64| {
        disruptions
            .iter()
            .any(|(a, b, _)| *b + 15_000 >= from && *a - 15_000 <= to)
    };
    let mut unique_runs: HashMap<&str, Vec<&Run>> = HashMap::new();
    for run in runs.iter().filter(|run| run.class == "SoakUniq") {
        unique_runs.entry(run.jid.as_str()).or_default().push(run);
    }
    let mut unique_counts: BTreeMap<String, u64> = BTreeMap::new();
    let mut accepted_by_key: BTreeMap<u64, Vec<&Value>> = BTreeMap::new();
    let replaced: BTreeSet<&str> = unique_log
        .iter()
        .filter(|push| push["outcome"] == "replaced")
        .filter_map(|push| push["other"].as_str())
        .collect();
    for push in &unique_log {
        let outcome = push["outcome"].as_str().unwrap_or("").to_owned();
        *unique_counts.entry(outcome.clone()).or_default() += 1;
        if outcome == "pushed" || outcome == "replaced" {
            accepted_by_key
                .entry(push["key"].as_u64().unwrap_or(0))
                .or_default()
                .push(push);
        }
    }
    let unique_for_ms = UNIQUE_JOB_FOR.as_millis() as i64;
    let lost_with_data = |t: i64| {
        data_loss.iter().any(|down| t <= *down)
            || lossy.iter().any(|(from, to)| (*from..=*to).contains(&t))
    };
    for (key, pushes) in &accepted_by_key {
        for pair in pushes.windows(2) {
            let (a, b) = (pair[0], pair[1]);
            let a_jid = a["j"].as_str().unwrap_or("");
            let (a_done, b_start, b_done) = (
                a["t"].as_i64().unwrap_or(0),
                b["t0"].as_i64().unwrap_or(0),
                b["t"].as_i64().unwrap_or(0),
            );
            // A debounce replaced the waiting job.
            if b["outcome"] == "replaced" && b["other"].as_str() == Some(a_jid) {
                continue;
            }
            // The lock lapsed: `unique_for` past the push (and the delay of a scheduled job).
            if b_start >= a_done + unique_for_ms + a["delay"].as_i64().unwrap_or(0) {
                continue;
            }
            let runs = unique_runs.get(a_jid).cloned().unwrap_or_default();
            let released = if a["until"] == "start" {
                runs.iter().map(|run| run.start).min()
            } else {
                runs.iter()
                    .filter(|run| run.outcome == "ok")
                    .filter_map(|run| run.end)
                    .min()
            };
            let held_too_long = match released {
                Some(at) => at > b_done,
                None => true,
            };
            if held_too_long && !near_fault(a["t0"].as_i64().unwrap_or(0), b_done) {
                violations.push(json!({"type": "UNIQUE_ACCEPTED_WHILE_HELD", "key": key,
                    "holder": a, "accepted": b, "released": released}));
            }
        }
        for push in pushes {
            let jid = push["j"].as_str().unwrap_or("");
            let runs = unique_runs.get(jid).cloned().unwrap_or_default();
            if replaced.contains(jid) {
                if !runs.is_empty() {
                    violations.push(json!({"type": "UNIQUE_REPLACED_RAN", "push": push,
                        "runs": runs.len()}));
                }
                continue;
            }
            let ok = runs.iter().any(|run| run.outcome == "ok");
            let killed = runs.iter().filter(|run| run.outcome == "killed").count();
            if !ok
                && killed < crate::worker::MAX_RECOVERIES as usize
                && !lost_with_data(push["t"].as_i64().unwrap_or(0))
            {
                violations.push(json!({"type": "UNIQUE_JOB_LOST", "push": push,
                    "runs": runs.len()}));
            }
        }
    }
    // Two jobs of one key never run at once: the per-key throttle (plus its overages after a
    // fault).
    let mut unique_overlaps = 0;
    for pushes in accepted_by_key.values() {
        let mut key_runs: Vec<&Run> = pushes
            .iter()
            .filter_map(|push| push["j"].as_str())
            .flat_map(|jid| unique_runs.get(jid).cloned().unwrap_or_default())
            .collect();
        key_runs.sort_by_key(|run| run.start);
        for (index, a) in key_runs.iter().enumerate() {
            for b in &key_runs[index + 1..] {
                if a.jid == b.jid || !overlap(a, b) {
                    continue;
                }
                unique_overlaps += 1;
                if !near_fault(a.start, b.end.unwrap_or(b.start)) {
                    violations.push(json!({"type": "UNIQUE_RUNS_OVERLAP",
                        "a": {"jid": a.jid, "w": a.wid, "start": a.start, "end": a.end},
                        "b": {"jid": b.jid, "w": b.wid, "start": b.start, "end": b.end}}));
                }
            }
        }
    }

    // ---- the burst: how fast a backlog of 150 members drained (measured only: how busy the
    // throttle's slots were from the commit until the last member started; the members' window
    // limiter, 8 grants a second, paces the drain)
    let mut burst = Value::Null;
    if let Some(record) = batch_log
        .iter()
        .find(|record| record["e"] == "batch" && record["burst"] == true && record["ok"] == true)
    {
        let committed = record["t"].as_i64().unwrap_or(0);
        let jids: BTreeSet<&str> = record["jids"]
            .as_array()
            .map(|jids| jids.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default();
        let members: Vec<&Run> = runs
            .iter()
            .filter(|run| run.class == "SoakPro" && jids.contains(run.jid.as_str()))
            .collect();
        let last_start = members
            .iter()
            .map(|run| run.start)
            .max()
            .unwrap_or(committed);
        let last_end = members
            .iter()
            .filter_map(|run| run.end)
            .max()
            .unwrap_or(committed);
        let window = (last_start - committed).max(1);
        let busy: i64 = runs
            .iter()
            .filter(|run| run.class == "SoakPro")
            .filter_map(|run| {
                let end = run.end?;
                let (from, to) = (run.start.max(committed), end.min(last_start));
                (to > from).then_some(to - from)
            })
            .sum();
        burst = json!({
            "members": jids.len(), "runs": members.len(), "drain_ms": last_end - committed,
            "utilization": busy as f64 / (THROTTLE_LIMIT as f64 * window as f64),
            "faults_during": near_fault(committed, last_end),
        });
    }

    // ---- keys: every feature key expires, but the configuration and index keys
    let allowed = |key: &str| {
        matches!(
            key,
            "periodic"
                | "periodic-names"
                | "periodic-crontabs"
                | "queue-limits"
                | "unique-locks"
                | "batches"
                | "batch-events"
                | "limits"
        ) || key.starts_with("periodic:")
            || key.starts_with("periodic-history:")
            || key.starts_with("periodic-group:")
    };
    let lasting: Vec<&String> = pro["ttls"]
        .as_object()
        .map(|ttls| {
            ttls.iter()
                .filter(|(key, ttl)| ttl.as_i64() == Some(-1) && !allowed(key))
                .map(|(key, _)| key)
                .collect()
        })
        .unwrap_or_default();
    if !lasting.is_empty() {
        violations.push(json!({"type": "KEYS_WITHOUT_TTL", "keys": lasting}));
    }

    // ---- the window limiter's grants, by the time they were logged (Redis decides on its
    // own clock: informative only)
    let mut grants: Vec<i64> = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if name.starts_with("exec-") && name.ends_with(".jsonl") {
            grants.extend(
                read_all(&path)?
                    .iter()
                    .filter(|record| record["e"] == "lg")
                    .filter_map(|record| record["t"].as_i64()),
            );
        }
    }
    grants.sort_unstable();
    let mut window_max = 0;
    let mut first = 0;
    for (index, t) in grants.iter().enumerate() {
        while grants[first] <= t - 1_000 {
            first += 1;
        }
        window_max = window_max.max(index + 1 - first);
    }

    let tick_runs = ticks.len();
    let summary = json!({
        "ticks": jids_of_tick.len(), "tick_runs": tick_runs, "tick_gap_max_ms": tick_gap_max,
        "tick_overlaps_after_a_lapse": tick_overlaps_after_lapse,
        "batches": counts, "outbox": outbox, "locks_left": locks.len(),
        "members_max_running": max_running, "throttle_overages": overages,
        "ms_above_limit": above_ms, "window_grants": grants.len(),
        "unique_pushes": unique_counts, "unique_runs_overlapping": unique_overlaps,
        "burst": burst,
        "window_grants_max_per_logged_second": window_max,
    });
    let section = json!({
        "summary": summary,
        "schedule": pro["schedule"].clone(),
        "history": pro["history"].clone(),
        "limit_stats": pro["limit_stats"].clone(),
        "violations": violations.len(),
    });
    Ok((section, violations))
}

/// The stretches of the load (`start..end`) without a tick (`ticks`, in epoch milliseconds)
/// that the faults do not explain. A stretch may last until [`TICK_GAP_MS`] after the latest of
/// its start and of the recoveries of the faults in it: a fault may cost the schedule a lease (a
/// lost reply of `PERIODIC_LEASE` or `FIRE`, a poller killed or stopped while it held the
/// lease), and a restart that lost Redis's data the registration check too. So ticks must come
/// back once the faults stop (the scenarios end with a calm stretch for that). Returns the
/// longest stretch and a `TICKS_STOPPED` violation per stretch too long.
fn ticks_stopped(
    ticks: impl Iterator<Item = i64>,
    (start, end): (i64, i64),
    disruptions: &[(i64, i64, String)],
    data_loss: &[i64],
) -> (i64, Vec<Value>) {
    let mut points = vec![start];
    points.extend(ticks.filter(|tick| *tick > start && *tick < end));
    points.push(end);
    points.sort_unstable();
    let mut longest = 0;
    let mut violations = Vec::new();
    for pair in points.windows(2) {
        let (from, to) = (pair[0], pair[1]);
        longest = longest.max(to - from);
        let mut resumed = from;
        let mut faults = Vec::new();
        for (a, b, what) in disruptions {
            let lost_data = data_loss.iter().any(|down| (*a..=*b).contains(down));
            let recovered = b + PERIODIC_LEASE_MS + if lost_data { PERIODIC_CHECK_MS } else { 0 };
            if recovered >= from && *a <= to {
                resumed = resumed.max(recovered);
                faults.push(what);
            }
        }
        if to > resumed + TICK_GAP_MS {
            violations.push(json!({"type": "TICKS_STOPPED", "from": from, "to": to,
                "gap_ms": to - from, "allowed_ms": resumed + TICK_GAP_MS - from,
                "faults": faults}));
        }
    }
    (longest, violations)
}

/// Whether something written at `t` may be gone with Redis's data, as documented: written
/// before a restart without persistence (`data_loss`), or in the stretch before a `kill -9` of
/// Redis with AOF `everysec` whose writes may be lost (`lossy`).
fn lost_with_redis_data(t: i64, data_loss: &[i64], lossy: &[(i64, i64)]) -> bool {
    data_loss.iter().any(|down| t <= *down)
        || lossy.iter().any(|(from, to)| (*from..=*to).contains(&t))
}

#[cfg(test)]
mod tests {
    use super::*;

    const LOAD: (i64, i64) = (0, 180_000);

    fn every_second(from: i64, to: i64) -> impl Iterator<Item = i64> {
        (from..to).step_by(1_000)
    }

    /// Round 3 (cross-soak-perf-3): a schedule that stopped for most of the load fails.
    #[test]
    fn a_schedule_that_stopped_ticking_is_reported() {
        let (longest, violations) = ticks_stopped(every_second(1_000, 65_000), LOAD, &[], &[]);
        assert_eq!(longest, 180_000 - 64_000);
        assert_eq!(violations.len(), 1, "{violations:?}");
        assert_eq!(violations[0]["type"], "TICKS_STOPPED");
        // No tick at all: one stretch, the whole load.
        let (longest, violations) = ticks_stopped(std::iter::empty(), LOAD, &[], &[]);
        assert_eq!((longest, violations.len()), (180_000, 1));
        // Ticks all along: nothing to report.
        let (longest, violations) = ticks_stopped(every_second(1_000, 180_000), LOAD, &[], &[]);
        assert_eq!((longest, violations.len()), (1_000, 0));
    }

    /// After a fault, ticks come back within a lease and the gap; after a restart that lost the
    /// data, within the registration check too.
    #[test]
    fn ticks_come_back_after_the_faults() {
        let lease = PERIODIC_LEASE_MS;
        // A 5 s CLIENT PAUSE at 50 s: ticks may stop from 39 s until its end, a lease and the
        // gap later.
        let pause = vec![(50_000, 55_000, "client_pause".to_owned())];
        let back = 55_000 + lease + TICK_GAP_MS;
        let ticks = |at: i64| every_second(1_000, 40_000).chain(every_second(at, 180_000));
        let (longest, violations) = ticks_stopped(ticks(back), LOAD, &pause, &[]);
        assert_eq!(
            (longest, violations.len()),
            (back - 39_000, 0),
            "{violations:?}"
        );
        assert_eq!(
            ticks_stopped(ticks(back + 1_000), LOAD, &pause, &[])
                .1
                .len(),
            1
        );
        // Without it, the same stretch is too long.
        assert_eq!(ticks_stopped(ticks(back), LOAD, &[], &[]).1.len(), 1);
        // A fault that recovered before the stretch explains nothing.
        let early = vec![(10_000, 15_000, "client_pause".to_owned())];
        assert_eq!(ticks_stopped(ticks(back), LOAD, &early, &[]).1.len(), 1);
        // The reviewer's run: ticks stopped at 64 s, faults until 100 s, a calm end.
        let faults = vec![
            (60_000, 65_000, "client_pause".to_owned()),
            (95_000, 100_000, "client_pause".to_owned()),
        ];
        let (_, violations) = ticks_stopped(every_second(1_000, 65_000), LOAD, &faults, &[]);
        assert_eq!(violations.len(), 1, "{violations:?}");
        // A restart that lost the data: its window, a lease and the registration check.
        let restart = vec![(60_000, 62_000, "redis restart".to_owned())];
        let back = 62_000 + lease + PERIODIC_CHECK_MS + TICK_GAP_MS;
        let (_, violations) = ticks_stopped(ticks(back), LOAD, &restart, &[60_000]);
        assert!(violations.is_empty(), "{violations:?}");
        let (_, violations) = ticks_stopped(ticks(back), LOAD, &restart, &[]);
        assert_eq!(
            violations.len(),
            1,
            "a restart that kept the data needs no registration"
        );
    }

    /// Round 3 (cross-soak-perf-5): what a documented data loss erased is excused, nothing else.
    #[test]
    fn writes_before_a_restart_without_persistence_may_be_lost() {
        let (data_loss, lossy) = (vec![100_000], vec![(148_000, 150_000)]);
        assert!(lost_with_redis_data(99_000, &data_loss, &lossy));
        assert!(lost_with_redis_data(100_000, &data_loss, &lossy));
        assert!(!lost_with_redis_data(120_000, &data_loss, &lossy));
        assert!(lost_with_redis_data(149_000, &data_loss, &lossy));
        assert!(!lost_with_redis_data(151_000, &data_loss, &lossy));
        assert!(!lost_with_redis_data(99_000, &[], &[]));
    }
}
