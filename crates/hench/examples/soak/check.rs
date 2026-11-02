//! `soak check DIR`: checks a scenario's logs and final Redis state against the invariants.
//!
//! * Every pushed job ends as its plan says: success (exactly one successful run, ideally),
//!   the dead set after exactly `retry + 1` failed attempts, or dropped/discarded (death
//!   handler, not in the dead set).
//! * Extra runs of the same attempt are classified by cause: crash recovery (a killed
//!   process), hard-shutdown requeue, cooperative shutdown, a fault window (Redis restart,
//!   CLIENT PAUSE, proxy fault: lost replies), or unexplained.
//! * No job runs concurrently in two processes except in the documented cases.
//! * Nothing stays in any in-progress list; the `processes` and `inprogress` sets are clean.
//! * `stat:processed` / `stat:failed` match the executions observed, within the counts a
//!   `kill -9` may lose (one heartbeat interval) and never above them.
//! * Worker memory, threads, fds and Redis connections stay bounded; the dashboard API agrees
//!   with Redis.
//! * With the pro load, the features' invariants (`check_pro.rs`).

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use serde_json::{Value, json};

use crate::jsonlog::read_all;

#[derive(Debug, Clone)]
struct Push {
    nonce: u64,
    plan: String,
    param: u64,
    retry: Value,
    ok: bool,
    t0: i64,
    at: Option<i64>,
    kind: String,
    jid: Option<String>,
    /// The batch of a member (`--pro`).
    bid: Option<String>,
}

#[derive(Debug, Clone)]
struct Exec {
    nonce: u64,
    wid: String,
    attempt: u64,
    start: i64,
    end: Option<i64>,
    /// ok | err | panic | int | shutdown | drop | killed | vanished
    outcome: String,
    enqueued_at: Option<i64>,
}

#[derive(Debug, Clone)]
struct Exit {
    t: i64,
    how: String,
    shutdown_ms: Option<u64>,
}

/// Time windows of faults that can lose replies (start, end, what).
#[derive(Debug, Clone)]
struct Fault {
    start: i64,
    end: i64,
    what: String,
}

fn expected_terminal(plan: &str) -> &'static str {
    match plan {
        "aerr" | "apanic" => "dead",
        "noretry" => "dropped",
        "deadfalse" => "discarded",
        _ => "success",
    }
}

fn retry_max(retry: &Value) -> Option<u64> {
    match retry {
        Value::Bool(false) => None,
        Value::Bool(true) => Some(25),
        other => other.as_u64(),
    }
}

/// The nonce of a job's JSON (third argument).
fn nonce_of_json(json: &str) -> Option<(u64, Value)> {
    let value: Value = serde_json::from_str(json).ok()?;
    let nonce = value["args"].get(2)?.as_u64()?;
    Some((nonce, value))
}

pub fn check(dir: &Path) -> anyhow::Result<Value> {
    // ---- inputs
    let mut pushes: BTreeMap<u64, Push> = BTreeMap::new();
    let mut push_dups = 0;
    for record in read_all(&dir.join("pushes.jsonl"))? {
        let push = Push {
            nonce: record["n"].as_u64().unwrap_or(0),
            plan: record["plan"].as_str().unwrap_or("").to_owned(),
            param: record["param"].as_u64().unwrap_or(0),
            retry: record["retry"].clone(),
            ok: record["ok"].as_bool().unwrap_or(false),
            t0: record["t0"].as_i64().unwrap_or(0),
            at: record["at"].as_i64(),
            kind: record["k"].as_str().unwrap_or("").to_owned(),
            jid: record["j"].as_str().map(str::to_owned),
            bid: record["b"].as_str().map(str::to_owned),
        };
        if pushes.insert(push.nonce, push).is_some() {
            push_dups += 1;
        }
    }

    let chaos = read_all(&dir.join("chaos.jsonl"))?;
    let mut exits: HashMap<String, Exit> = HashMap::new();
    let mut faults: Vec<Fault> = Vec::new();
    let mut kills: Vec<(String, i64)> = Vec::new();
    let mut unexpected_exits = Vec::new();
    let mut stuck_terms = Vec::new();
    let mut slow_shutdowns = Vec::new();
    let mut shutdown_ms_all = Vec::new();
    let mut redis_restarts = Vec::new();
    let mut data_loss_restarts: Vec<i64> = Vec::new();
    // `redis_kill9` with AOF everysec: writes of the last second or so before the kill.
    let mut kill9_windows: Vec<(i64, i64)> = Vec::new();
    let mut fetch_of: HashMap<String, String> = HashMap::new();
    let mut open_pause: Option<(i64, String)> = None;
    let mut redis_down_at: Option<(i64, String)> = None;
    let mut timeout_s = 5;
    if let Ok(options) = std::fs::read_to_string(dir.join("options.txt")) {
        for line in options.lines() {
            if let Some(rest) = line.trim().strip_prefix("timeout: ") {
                timeout_s = rest.trim_end_matches(',').parse().unwrap_or(5);
            }
        }
    }
    let mut scenario_persist = String::new();
    for event in &chaos {
        let t = event["t"].as_i64().unwrap_or(0);
        let wid = event["w"].as_str().unwrap_or("").to_owned();
        match event["e"].as_str().unwrap_or("") {
            "scenario" => {
                scenario_persist = event["persist"].as_str().unwrap_or("").to_owned();
            }
            "spawn" => {
                fetch_of.insert(wid, event["fetch"].as_str().unwrap_or("").to_owned());
            }
            "exit" => {
                let how = event["how"].as_str().unwrap_or("").to_owned();
                let expected = event["expected"].as_bool().unwrap_or(false);
                let shutdown_ms = event["shutdown_ms"].as_u64();
                if let Some(ms) = shutdown_ms {
                    shutdown_ms_all.push(ms);
                    // A graceful stop waits `timeout` for jobs, then up to 3 s + fetch timeout
                    // for interrupted processors, then bounded cleanup steps.
                    if ms > (timeout_s + 12) * 1000 {
                        slow_shutdowns.push(json!({"w": wid, "shutdown_ms": ms, "how": how}));
                    }
                }
                if !expected {
                    unexpected_exits.push(event.clone());
                }
                exits.insert(
                    wid,
                    Exit {
                        t,
                        how,
                        shutdown_ms,
                    },
                );
            }
            "kill9" => kills.push((wid, t)),
            "term_stuck" => stuck_terms.push(event.clone()),
            "redis_down" => {
                redis_down_at = Some((t, event["how"].as_str().unwrap_or("").to_owned()));
            }
            "redis_up" => {
                if let Some((down, how)) = redis_down_at.take() {
                    faults.push(Fault {
                        start: down,
                        end: t,
                        what: format!("redis {how}"),
                    });
                    let persist = event["persist"].as_str().unwrap_or("");
                    if persist == "none" {
                        data_loss_restarts.push(down);
                    } else if how == "redis_kill9" {
                        kill9_windows.push((down - 2_000, down));
                    }
                    redis_restarts
                        .push(json!({"down": down, "up": t, "how": how, "persist": persist}));
                }
            }
            "client_pause" => {
                let ms = event["ms"].as_i64().unwrap_or(0);
                faults.push(Fault {
                    start: t,
                    end: t + ms,
                    what: format!(
                        "client pause {} {ms} ms",
                        event["mode"].as_str().unwrap_or("")
                    ),
                });
                open_pause = Some((t, String::new()));
            }
            "proxy_blackhole" => {
                let ms = event["ms"].as_i64().unwrap_or(0);
                faults.push(Fault {
                    start: t,
                    end: t + ms,
                    what: "proxy blackhole".into(),
                });
            }
            "proxy_delay" => {
                let ms = event["ms"].as_i64().unwrap_or(0);
                faults.push(Fault {
                    start: t,
                    end: t + ms,
                    what: format!("proxy delay {} ms", event["delay_ms"]),
                });
            }
            "proxy_reset" => faults.push(Fault {
                start: t,
                end: t + 100,
                what: "proxy reset".into(),
            }),
            _ => {}
        }
    }
    let _ = open_pause;

    // ---- orphan recoveries (from the workers' own logs): which process was recovered, and
    // was it still alive then?
    let mut wid_of_pid: HashMap<u64, String> = HashMap::new();
    for event in &chaos {
        if event["e"] == "spawn"
            && let (Some(pid), Some(wid)) = (event["p"].as_u64(), event["w"].as_str())
        {
            wid_of_pid.insert(pid, wid.to_owned());
        }
    }
    let mut recoveries: Vec<(i64, String, String, u64)> = Vec::new(); // (t, recovered wid, by wid, jobs)
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_owned();
        if !(name.starts_with("worker-") && name.ends_with(".log")) {
            continue;
        }
        let by = name
            .trim_start_matches("worker-")
            .trim_end_matches(".log")
            .to_owned();
        let text = std::fs::read_to_string(&path).unwrap_or_default();
        for line in text.lines() {
            if !line.contains("recovered jobs held by a process that died") {
                continue;
            }
            let Some(stamp) = line.split_whitespace().next() else {
                continue;
            };
            let Ok(time) = chrono::DateTime::parse_from_rfc3339(stamp) else {
                continue;
            };
            let pid = line
                .split("process=")
                .nth(1)
                .and_then(|rest| rest.split_whitespace().next())
                .and_then(|identity| identity.split(':').nth(1))
                .and_then(|pid| pid.parse::<u64>().ok());
            let jobs = line
                .split("jobs=")
                .nth(1)
                .and_then(|rest| rest.split_whitespace().next())
                .and_then(|n| n.parse().ok())
                .unwrap_or(0);
            if let Some(wid) = pid.and_then(|pid| wid_of_pid.get(&pid)) {
                recoveries.push((time.timestamp_millis(), wid.clone(), by.clone(), jobs));
            }
        }
    }
    let mut live_recoveries: Vec<Value> = Vec::new();
    for (t, wid, by, jobs) in &recoveries {
        let alive = exits.get(wid).is_none_or(|exit| exit.t > *t);
        if alive {
            live_recoveries.push(json!({"t": t, "recovered": wid, "by": by, "jobs": jobs}));
        }
    }

    // ---- executions
    let mut execs: Vec<Exec> = Vec::new();
    let mut items: HashMap<u64, BTreeMap<u64, u64>> = HashMap::new();
    let mut item_times: HashMap<u64, Vec<(u64, String, i64)>> = HashMap::new();
    let mut deaths: HashMap<u64, Vec<Value>> = HashMap::new();
    let mut errors: BTreeMap<String, u64> = BTreeMap::new();
    let mut worker_files = 0;
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if !(name.starts_with("exec-") && name.ends_with(".jsonl")) {
            continue;
        }
        worker_files += 1;
        let wid = name
            .trim_start_matches("exec-")
            .trim_end_matches(".jsonl")
            .to_owned();
        let mut open: HashMap<(u64, u64), Vec<(Exec, bool)>> = HashMap::new();
        for record in read_all(&path)? {
            let nonce = record["n"].as_u64().unwrap_or(0);
            let attempt = record["a"].as_u64().unwrap_or(0);
            let t = record["t"].as_i64().unwrap_or(0);
            match record["e"].as_str().unwrap_or("") {
                "s" => open.entry((nonce, attempt)).or_default().push((
                    Exec {
                        nonce,
                        wid: wid.clone(),
                        attempt,
                        start: t,
                        end: None,
                        outcome: String::new(),
                        enqueued_at: record["eq"].as_i64(),
                    },
                    false,
                )),
                "pp" => {
                    if let Some(list) = open.get_mut(&(nonce, attempt))
                        && let Some(last) = list.last_mut()
                    {
                        last.1 = true;
                    }
                }
                "e" | "x" => {
                    if let Some(list) = open.get_mut(&(nonce, attempt))
                        && let Some((mut exec, panicked)) = list.pop()
                    {
                        exec.end = Some(t);
                        let outcome = record["o"].as_str().unwrap_or("");
                        exec.outcome = if record["e"] == "x" && panicked {
                            "panic".into()
                        } else {
                            outcome.to_owned()
                        };
                        execs.push(exec);
                    }
                }
                "i" => {
                    item_times.entry(nonce).or_default().push((
                        record["k"].as_u64().unwrap_or(0),
                        wid.clone(),
                        t,
                    ));
                    *items
                        .entry(nonce)
                        .or_default()
                        .entry(record["k"].as_u64().unwrap_or(0))
                        .or_default() += 1;
                }
                "death" => deaths.entry(nonce).or_default().push(record.clone()),
                "err" => {
                    *errors
                        .entry(record["ctx"].as_str().unwrap_or("").to_owned())
                        .or_default() += 1;
                }
                _ => {}
            }
        }
        // Executions that never ended: their process died (or stopped without ending them).
        let exit = exits.get(&wid);
        for (_, list) in open {
            for (mut exec, _) in list {
                match exit {
                    Some(exit) => {
                        exec.end = Some(exit.t);
                        exec.outcome = if exit.how == "kill9" || exit.how == "kill_after_stuck" {
                            "killed".into()
                        } else {
                            "vanished".into()
                        };
                    }
                    None => exec.outcome = "vanished".into(),
                }
                execs.push(exec);
            }
        }
    }
    let mut by_nonce: HashMap<u64, Vec<Exec>> = HashMap::new();
    for exec in &execs {
        by_nonce.entry(exec.nonce).or_default().push(exec.clone());
    }
    for list in by_nonce.values_mut() {
        list.sort_by_key(|exec| exec.start);
    }

    // ---- final Redis state
    let final_state: Value = serde_json::from_slice(&std::fs::read(dir.join("final.json"))?)?;
    let redis = &final_state["redis"];
    let mut location: HashMap<u64, Vec<String>> = HashMap::new();
    let mut dead_members: HashMap<u64, u64> = HashMap::new();
    // Jobs in the dead set as poison pills (`hench::CrashLoop`): recovered from killed
    // processes more than `max_recoveries` times.
    let mut crash_loop_dead: std::collections::HashSet<u64> = std::collections::HashSet::new();
    let mut leftover_inprogress = 0;
    let mut leftover_lists = Vec::new();
    if let Some(lists) = redis["lists"].as_object() {
        for (key, items) in lists {
            let items = items.as_array().cloned().unwrap_or_default();
            if key.ends_with(":inprogress") && !items.is_empty() {
                leftover_inprogress += items.len();
                leftover_lists.push(json!({"key": key, "len": items.len()}));
            }
            for item in &items {
                if let Some((nonce, _)) = item.as_str().and_then(nonce_of_json) {
                    location.entry(nonce).or_default().push(key.clone());
                }
            }
        }
    }
    if let Some(sets) = redis["zsets"].as_object() {
        for (key, entries) in sets {
            for entry in entries.as_array().cloned().unwrap_or_default() {
                if let Some((nonce, job)) = entry[0].as_str().and_then(nonce_of_json) {
                    location.entry(nonce).or_default().push(key.clone());
                    if key == "dead" {
                        *dead_members.entry(nonce).or_default() += 1;
                        if job["error_class"] == "hench::CrashLoop" {
                            crash_loop_dead.insert(nonce);
                        }
                    }
                }
            }
        }
    }

    // ---- per-job verdicts
    let near = |t: i64, before: i64, after: i64| -> Vec<String> {
        faults
            .iter()
            .filter(|fault| fault.end + after >= t && fault.start - before <= t)
            .map(|fault| fault.what.clone())
            .collect()
    };
    let lost_redis_data = |push: &Push| -> bool {
        // Pushed before a restart without persistence and not finished before it, or pushed
        // in the last moments before a kill -9 of an AOF-everysec Redis.
        data_loss_restarts.iter().any(|restart| push.t0 <= *restart)
            || kill9_windows
                .iter()
                .any(|(from, to)| push.t0 >= *from && push.t0 <= *to)
    };
    // A run whose outcome may have been written in the moments a kill -9 of Redis lost.
    let in_kill9_window = |t: i64| -> bool {
        kill9_windows
            .iter()
            .any(|(from, to)| t >= *from && t <= *to + 500)
    };
    let mut violations: Vec<Value> = Vec::new();
    let mut counts: BTreeMap<String, u64> = BTreeMap::new();
    let mut dup_causes: BTreeMap<String, u64> = BTreeMap::new();
    let mut dup_examples: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    let mut overlaps: Vec<Value> = Vec::new();
    let mut overlap_causes: BTreeMap<String, u64> = BTreeMap::new();
    let bump = |counts: &mut BTreeMap<String, u64>, key: &str| {
        *counts.entry(key.to_owned()).or_default() += 1;
    };

    let pro_facts = crate::check_pro::Facts::load(dir)?;
    for push in pushes.values() {
        let runs = by_nonce.get(&push.nonce).cloned().unwrap_or_default();
        // A member of a cancelled batch that never succeeded is skipped, whatever its plan.
        let skipped_member = push.kind == "member"
            && !runs.iter().any(|exec| exec.outcome == "ok")
            && !deaths.contains_key(&push.nonce)
            && pro_facts
                .as_ref()
                .is_some_and(|facts| facts.skipped(push.bid.as_deref(), push.jid.as_deref()));
        // A poison pill: its death handler (or its copy in the dead set) says it was recovered
        // from killed processes more than `max_recoveries` times.
        let crash_loop = crash_loop_dead.contains(&push.nonce)
            || deaths.get(&push.nonce).is_some_and(|records| {
                records
                    .iter()
                    .any(|record| record["x"] == "hench::CrashLoop")
            });
        let terminal = if crash_loop {
            "crash_loop"
        } else if skipped_member {
            "skipped"
        } else if push.kind == "iter" {
            "success"
        } else {
            expected_terminal(&push.plan)
        };
        // Where the job still is in Redis, the dead set aside (a terminal place).
        let where_now: Vec<String> = location
            .get(&push.nonce)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|key| key != "dead")
            .collect();
        bump(&mut counts, &format!("pushed_{}", push.kind));
        if !push.ok {
            bump(&mut counts, "push_errors");
            if runs.is_empty() && where_now.is_empty() && !dead_members.contains_key(&push.nonce) {
                bump(&mut counts, "push_errors_not_pushed");
                continue;
            }
            bump(&mut counts, "push_errors_but_pushed");
        }
        let ok_runs = runs.iter().filter(|exec| exec.outcome == "ok").count();
        let killed_basic = runs.iter().any(|exec| {
            exec.outcome == "killed" && fetch_of.get(&exec.wid).map(String::as_str) == Some("basic")
        });
        let basic_any = runs
            .iter()
            .any(|exec| fetch_of.get(&exec.wid).map(String::as_str) == Some("basic"));

        // Final state.
        match terminal {
            "success" => {
                let last_end = runs.iter().filter_map(|exec| exec.end).max();
                if ok_runs == 0 {
                    let kind = if !where_now.is_empty() {
                        "STUCK"
                    } else if lost_redis_data(push) || last_end.is_some_and(in_kill9_window) {
                        "lost_with_redis_data (restart without persistence)"
                    } else if killed_basic {
                        "lost_basic_fetch_crash (documented)"
                    } else {
                        "LOST"
                    };
                    bump(&mut counts, kind);
                    if kind == "LOST" || kind == "STUCK" {
                        violations.push(json!({
                            "type": kind, "nonce": push.nonce, "plan": push.plan,
                            "push_kind": push.kind, "pushed_at": push.t0, "at": push.at,
                            "where": where_now, "basic": basic_any,
                            "runs": runs.iter().map(|e| json!({"w": e.wid, "a": e.attempt,
                                "start": e.start, "end": e.end, "o": e.outcome})).collect::<Vec<_>>(),
                        }));
                    }
                } else {
                    bump(&mut counts, "succeeded");
                }
                if ok_runs >= 2 {
                    bump(&mut counts, "succeeded_more_than_once");
                }
                if dead_members.contains_key(&push.nonce) {
                    bump(&mut counts, "WRONG_DEAD");
                    violations.push(json!({"type": "WRONG_DEAD", "nonce": push.nonce,
                        "plan": push.plan}));
                }
                if push.kind == "iter" {
                    // Per run: the items it processed (by worker and time window).
                    let last = push.param.saturating_sub(1);
                    for (index, run) in runs.iter().enumerate() {
                        let end = run.end.unwrap_or(i64::MAX);
                        let did_last = item_times.get(&push.nonce).is_some_and(|times| {
                            times.iter().any(|(item, wid, t)| {
                                *item == last && *wid == run.wid && *t >= run.start && *t <= end
                            })
                        });
                        if !did_last {
                            continue;
                        }
                        match run.outcome.as_str() {
                            "err" | "panic" => {
                                bump(&mut counts, "ITER_COMPLETED_THEN_FAILED");
                                let restarted = runs.get(index + 1).is_some_and(|next| {
                                    item_times.get(&push.nonce).is_some_and(|times| {
                                        times.iter().any(|(item, wid, t)| {
                                            *item == 0 && *wid == next.wid && *t >= next.start
                                        })
                                    })
                                });
                                violations.push(json!({"type": "ITER_COMPLETED_THEN_FAILED", "nonce": push.nonce,
                                    "run": {"w": run.wid, "start": run.start, "end": run.end, "o": run.outcome},
                                    "retry_started_over_from_item_0": restarted}));
                            }
                            "int" => bump(&mut counts, "iter_interrupted_after_its_last_item"),
                            _ => {}
                        }
                    }
                }
                if push.kind == "iter" && ok_runs > 0 {
                    let seen = items.get(&push.nonce).cloned().unwrap_or_default();
                    let missing: Vec<u64> = (0..push.param)
                        .filter(|item| !seen.contains_key(item))
                        .collect();
                    let repeated: u64 = seen.values().map(|count| count.saturating_sub(1)).sum();
                    if !missing.is_empty() {
                        bump(&mut counts, "ITER_ITEMS_MISSING");
                        violations.push(json!({"type": "ITER_ITEMS_MISSING", "nonce": push.nonce,
                            "missing": missing}));
                    }
                    *counts.entry("iter_items_repeated".into()).or_default() += repeated;
                    let executions = runs.len() as u64;
                    *counts.entry("iter_executions".into()).or_default() += executions;
                }
            }
            "dead" | "dropped" | "discarded" => {
                let max = retry_max(&push.retry).unwrap_or(0);
                let attempts_expected = if terminal == "dropped" { 1 } else { max + 1 };
                let failing: Vec<&Exec> = runs
                    .iter()
                    .filter(|exec| matches!(exec.outcome.as_str(), "err" | "panic"))
                    .collect();
                let mut attempts: Vec<u64> = failing.iter().map(|exec| exec.attempt).collect();
                attempts.sort_unstable();
                attempts.dedup();
                let done = deaths.contains_key(&push.nonce);
                let complete = attempts.len() as u64 == attempts_expected
                    && attempts.iter().copied().eq(0..attempts_expected);
                let last_end = runs.iter().filter_map(|exec| exec.end).max();
                // Death handlers run in the worker after the outcome write: a `kill -9` of the
                // worker in between skips them. The outcome was written when the job left the
                // system without another run, in the morgue exactly when it should be there;
                // had the kill come before the write, orphan recovery would have run it again.
                let handler_lost_to_kill = !done
                    && where_now.is_empty()
                    && (terminal == "dead") == dead_members.contains_key(&push.nonce)
                    && runs.last().is_some_and(|last| {
                        matches!(last.outcome.as_str(), "err" | "panic")
                            && last.end.is_some_and(|end| {
                                exits.get(&last.wid).is_some_and(|exit| {
                                    (exit.how == "kill9" || exit.how == "kill_after_stuck")
                                        && exit.t >= end
                                        && exit.t <= end + 2_000
                                })
                            })
                    });
                if handler_lost_to_kill {
                    bump(&mut counts, &format!("ended_{terminal}"));
                    bump(&mut counts, "death_handler_lost_to_a_kill");
                    if !complete {
                        bump(&mut counts, "WRONG_ATTEMPTS");
                        violations.push(json!({"type": "WRONG_ATTEMPTS", "nonce": push.nonce,
                            "plan": push.plan, "expected": attempts_expected, "attempts": attempts}));
                    }
                } else if !done {
                    let kind = if !where_now.is_empty() {
                        "STUCK"
                    } else if lost_redis_data(push) || last_end.is_some_and(in_kill9_window) {
                        "lost_with_redis_data (restart without persistence)"
                    } else if killed_basic {
                        "lost_basic_fetch_crash (documented)"
                    } else {
                        "LOST"
                    };
                    bump(&mut counts, kind);
                    if kind == "LOST" || kind == "STUCK" {
                        violations.push(json!({
                            "type": kind, "nonce": push.nonce, "plan": push.plan,
                            "expected": terminal, "attempts": attempts, "where": where_now,
                            "runs": runs.iter().map(|e| json!({"w": e.wid, "a": e.attempt,
                                "start": e.start, "end": e.end, "o": e.outcome})).collect::<Vec<_>>(),
                        }));
                    }
                } else {
                    bump(&mut counts, &format!("ended_{terminal}"));
                    if !complete {
                        bump(&mut counts, "WRONG_ATTEMPTS");
                        violations.push(json!({"type": "WRONG_ATTEMPTS", "nonce": push.nonce,
                            "plan": push.plan, "expected": attempts_expected, "attempts": attempts}));
                    }
                    let in_dead = dead_members.get(&push.nonce).copied().unwrap_or(0);
                    match terminal {
                        "dead" if in_dead == 0 && !lost_redis_data(push) => {
                            bump(&mut counts, "DEAD_MISSING");
                            violations.push(json!({"type": "DEAD_MISSING", "nonce": push.nonce,
                                "plan": push.plan}));
                        }
                        "dead" if in_dead > 1 => {
                            bump(&mut counts, "dead_set_duplicates");
                        }
                        "dropped" | "discarded" if in_dead > 0 => {
                            bump(&mut counts, "WRONG_DEAD");
                            violations.push(json!({"type": "WRONG_DEAD", "nonce": push.nonce,
                                "plan": push.plan}));
                        }
                        _ => {}
                    }
                    if deaths[&push.nonce].len() > 1 {
                        bump(&mut counts, "death_handler_more_than_once");
                    }
                }
                if !where_now.is_empty() && done {
                    bump(&mut counts, "terminal_but_still_in_redis");
                    violations.push(json!({"type": "TERMINAL_BUT_IN_REDIS", "nonce": push.nonce,
                        "where": where_now}));
                }
            }
            "crash_loop" => {
                // Justified only when the job really was in flight in the processes killed:
                // `max_recoveries + 1` recoveries, one of which may have caught it between its
                // fetch and its start (no execution logged).
                let killed = runs.iter().filter(|exec| exec.outcome == "killed").count() as u64;
                let needed = u64::from(crate::worker::MAX_RECOVERIES);
                if killed >= needed && ok_runs == 0 {
                    bump(&mut counts, "poison_pill_after_kills");
                } else {
                    bump(&mut counts, "CRASH_LOOP_UNJUSTIFIED");
                    violations.push(
                        json!({"type": "CRASH_LOOP_UNJUSTIFIED", "nonce": push.nonce,
                        "plan": push.plan, "killed_runs": killed, "ok_runs": ok_runs,
                        "runs": runs.iter().map(|e| json!({"w": e.wid, "a": e.attempt,
                            "start": e.start, "end": e.end, "o": e.outcome})).collect::<Vec<_>>()}),
                    );
                }
                if !where_now.is_empty() {
                    bump(&mut counts, "terminal_but_still_in_redis");
                    violations.push(json!({"type": "TERMINAL_BUT_IN_REDIS", "nonce": push.nonce,
                        "where": where_now}));
                }
            }
            "skipped" => {
                bump(&mut counts, "member_skipped");
                if !runs.iter().any(|exec| exec.outcome == "skip") {
                    // Skipped by the throttle gate, before any middleware: counted as processed
                    // by hench, never seen by the execution log.
                    bump(&mut counts, "member_skipped_at_the_gate");
                }
                if !where_now.is_empty() {
                    bump(&mut counts, "STUCK");
                    violations.push(json!({"type": "STUCK", "nonce": push.nonce,
                        "plan": push.plan, "where": where_now}));
                }
            }
            _ => {}
        }

        // Extra runs of one attempt (iterable continuations are not extra runs).
        let mut by_attempt: BTreeMap<u64, Vec<&Exec>> = BTreeMap::new();
        for exec in &runs {
            by_attempt.entry(exec.attempt).or_default().push(exec);
        }
        for (attempt, list) in &by_attempt {
            for pair in list.windows(2) {
                let (previous, next) = (pair[0], pair[1]);
                if push.kind == "iter" && previous.outcome == "int" {
                    continue;
                }
                let cause = match previous.outcome.as_str() {
                    "drop" => "hard_shutdown_requeue".to_owned(),
                    // A limiter refused it: rescheduled, the same attempt runs again.
                    "ol" => "over_limit_reschedule".to_owned(),
                    "shutdown" => "cooperative_shutdown_requeue".to_owned(),
                    "killed" => "crash_recovery".to_owned(),
                    "int" => "iterable_continuation".to_owned(),
                    "vanished" => "VANISHED_PREVIOUS_RUN".to_owned(),
                    _ => {
                        // The previous run finished (ok/err/panic) yet the same attempt ran
                        // again: what could have lost its outcome?
                        let end = previous.end.unwrap_or(previous.start);
                        let exit = exits.get(&previous.wid);
                        let killed_soon = exit.is_some_and(|exit| {
                            (exit.how == "kill9" || exit.how == "kill_after_stuck")
                                && exit.t >= end
                                && exit.t - end <= 15_000
                        });
                        let hard_soon = exit.is_some_and(|exit| {
                            exit.t >= end
                                && exit.t - end <= 15_000
                                && exit.shutdown_ms.is_some_and(|ms| ms >= timeout_s * 1000)
                        });
                        let faults_near = near(end, 5_000, 15_000);
                        let push_faults = near(push.t0, 1_000, 1_000);
                        let recovered_live = recoveries.iter().any(|(t, wid, _, _)| {
                            *wid == previous.wid
                                && *t >= previous.start
                                && *t <= next.start
                                && exits.get(wid).is_none_or(|exit| exit.t > *t)
                        });
                        if recovered_live {
                            "ORPHAN_RECOVERY_OF_A_LIVE_PROCESS".to_owned()
                        } else if killed_soon {
                            "killed_before_outcome_written".to_owned()
                        } else if hard_soon {
                            "hard_shutdown_race".to_owned()
                        } else if *attempt == 0 && !push_faults.is_empty() {
                            "possible_duplicate_push".to_owned()
                        } else if !faults_near.is_empty() {
                            format!(
                                "fault_window ({})",
                                faults_near[0]
                                    .split(' ')
                                    .take(2)
                                    .collect::<Vec<_>>()
                                    .join(" ")
                            )
                        } else {
                            "UNEXPLAINED".to_owned()
                        }
                    }
                };
                *dup_causes.entry(cause.clone()).or_default() += 1;
                let examples = dup_examples.entry(cause.clone()).or_default();
                if examples.len() < 5 {
                    examples.push(json!({
                        "nonce": push.nonce, "plan": push.plan, "attempt": attempt,
                        "previous": {"w": previous.wid, "start": previous.start, "end": previous.end, "o": previous.outcome},
                        "next": {"w": next.wid, "start": next.start, "end": next.end, "o": next.outcome},
                    }));
                }
                if cause == "UNEXPLAINED"
                    || cause == "VANISHED_PREVIOUS_RUN"
                    || cause == "ORPHAN_RECOVERY_OF_A_LIVE_PROCESS"
                {
                    violations.push(json!({"type": format!("DUPLICATE_{cause}"), "nonce": push.nonce,
                        "plan": push.plan, "attempt": attempt,
                        "previous": {"w": previous.wid, "start": previous.start, "end": previous.end, "o": previous.outcome},
                        "next": {"w": next.wid, "start": next.start, "end": next.end, "o": next.outcome}}));
                }
            }
        }

        // Concurrent runs in two processes.
        for (i, a) in runs.iter().enumerate() {
            for b in &runs[i + 1..] {
                if a.wid == b.wid {
                    continue;
                }
                let a_end = a.end.unwrap_or(i64::MAX);
                let b_end = b.end.unwrap_or(i64::MAX);
                if a.start < b_end && b.start < a_end {
                    let long_pause = faults.iter().any(|fault| {
                        fault.end - fault.start >= 55_000
                            && fault.start <= b.start
                            && fault.end + 60_000 >= b.start
                    });
                    let lost_fetch = kill9_windows.iter().any(|(_, down)| {
                        *down >= a.start.min(b.start) - 2_000 && *down <= a.start.max(b.start)
                    });
                    // The first run's process was "recovered" (its jobs pushed back) by another
                    // process while it was alive and running this job.
                    let (first, second) = if a.start <= b.start { (a, b) } else { (b, a) };
                    let recovered_live = recoveries.iter().any(|(t, wid, _, _)| {
                        *wid == first.wid
                            && *t >= first.start
                            && *t <= second.start
                            && first.end.is_none_or(|end| end >= *t)
                    });
                    let cause = if recovered_live {
                        "ORPHAN_RECOVERY_OF_A_LIVE_PROCESS"
                    } else if a.attempt == 0
                        && b.attempt == 0
                        && !near(push.t0, 1_000, 1_000).is_empty()
                    {
                        "possible_duplicate_push"
                    } else if lost_fetch {
                        "redis_kill9_lost_the_fetch (AOF everysec)"
                    } else if long_pause {
                        "heartbeat_expired_during_long_pause (documented)"
                    } else {
                        "UNEXPLAINED_CONCURRENT_RUN"
                    };
                    *overlap_causes.entry(cause.to_owned()).or_default() += 1;
                    let record = json!({"nonce": push.nonce, "plan": push.plan, "cause": cause,
                        "a": {"w": a.wid, "attempt": a.attempt, "start": a.start, "end": a.end, "o": a.outcome},
                        "b": {"w": b.wid, "attempt": b.attempt, "start": b.start, "end": b.end, "o": b.outcome}});
                    if cause == "UNEXPLAINED_CONCURRENT_RUN"
                        || cause == "ORPHAN_RECOVERY_OF_A_LIVE_PROCESS"
                    {
                        violations.push(json!({"type": cause, "detail": record.clone()}));
                    }
                    if overlaps.len() < 20 {
                        overlaps.push(record);
                    }
                }
            }
        }
    }
    // Lateness of scheduled jobs (first run − due time), by minute of the scenario.
    let first_push = pushes.values().map(|push| push.t0).min().unwrap_or(0);
    let mut lateness_by_minute: BTreeMap<i64, Vec<i64>> = BTreeMap::new();
    for push in pushes.values() {
        let Some(at) = push.at else { continue };
        let Some(first) = by_nonce.get(&push.nonce).and_then(|runs| runs.first()) else {
            continue;
        };
        // When the scheduler enqueued it (the run may then wait behind a backlog).
        let enqueued = first.enqueued_at.unwrap_or(first.start);
        lateness_by_minute
            .entry((at - first_push) / 60_000)
            .or_default()
            .push(enqueued - at);
    }
    let lateness: BTreeMap<String, Value> = lateness_by_minute
        .into_iter()
        .map(|(minute, mut values)| {
            values.sort_unstable();
            let p50 = values[values.len() / 2];
            let p90 = values[values.len() * 9 / 10];
            let max = values[values.len() - 1];
            (
                format!("minute {minute}"),
                json!({"jobs": values.len(), "p50_ms": p50, "p90_ms": p90, "max_ms": max}),
            )
        })
        .collect();
    let executions_of_unknown_jobs = by_nonce
        .keys()
        .filter(|nonce| !pushes.contains_key(nonce))
        .count();

    // ---- stats
    let ended: Vec<&Exec> = execs
        .iter()
        .filter(|exec| !matches!(exec.outcome.as_str(), "killed" | "vanished"))
        .collect();
    let processed_observed = ended.len() as i64;
    let failed_observed = ended
        .iter()
        .filter(|exec| matches!(exec.outcome.as_str(), "err" | "panic" | "drop" | "shutdown"))
        .count() as i64;
    // Counts a kill -9 may lose: executions that ended in the killed process within the last
    // heartbeat interval (plus a margin) before the kill.
    let heartbeat_ms = 10_000 + 2_000;
    let mut processed_unflushable = 0_i64;
    let mut failed_unflushable = 0_i64;
    for exec in &ended {
        if let Some(exit) = exits.get(&exec.wid)
            && (exit.how == "kill9" || exit.how == "kill_after_stuck")
            && let Some(end) = exec.end
            && exit.t - end <= heartbeat_ms
        {
            processed_unflushable += 1;
            if matches!(exec.outcome.as_str(), "err" | "panic" | "drop" | "shutdown") {
                failed_unflushable += 1;
            }
        }
    }
    // Members skipped by the throttle gate are processed without an execution to observe.
    let processed_observed = processed_observed
        + counts
            .get("member_skipped_at_the_gate")
            .copied()
            .unwrap_or(0) as i64;
    let processed_redis = redis["processed"].as_i64().unwrap_or(0);
    let failed_redis = redis["failed"].as_i64().unwrap_or(0);
    let stats_comparable = data_loss_restarts.is_empty();
    let mut stats_violations = Vec::new();
    if stats_comparable {
        if processed_redis > processed_observed {
            stats_violations.push(format!(
                "stat:processed {processed_redis} > {processed_observed} executions observed (double counting)"
            ));
        }
        if processed_redis < processed_observed - processed_unflushable {
            stats_violations.push(format!(
                "stat:processed {processed_redis} < {} (observed {processed_observed} minus {processed_unflushable} a kill -9 may lose)",
                processed_observed - processed_unflushable
            ));
        }
        if failed_redis > failed_observed {
            stats_violations.push(format!(
                "stat:failed {failed_redis} > {failed_observed} failed executions observed"
            ));
        }
        if failed_redis < failed_observed - failed_unflushable {
            stats_violations.push(format!(
                "stat:failed {failed_redis} < {} (observed {failed_observed} minus {failed_unflushable})",
                failed_observed - failed_unflushable
            ));
        }
    }
    for message in &stats_violations {
        violations.push(json!({"type": "STATS_MISMATCH", "detail": message}));
    }

    // ---- leftovers
    for recovery in &live_recoveries {
        violations.push(json!({"type": "ORPHAN_RECOVERY_OF_A_LIVE_PROCESS", "detail": recovery}));
    }
    let processes_left = redis["processes"].as_array().map_or(0, Vec::len);
    if processes_left > 0 {
        // Every worker was stopped gracefully (SREM), after the drain waited for the cleanup to
        // prune the processes that died (`kill -9`): what is left was never pruned, or did not
        // remove itself when it stopped. Each member with the worker it was and how it ended.
        let members: Vec<Value> = redis["processes"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|member| {
                let wid = member
                    .as_str()
                    .and_then(|identity| identity.rsplit(':').nth(1))
                    .and_then(|pid| pid.parse::<u64>().ok())
                    .and_then(|pid| wid_of_pid.get(&pid));
                let exit = wid.and_then(|wid| exits.get(wid));
                json!({"member": member, "w": wid, "how": exit.map(|exit| exit.how.as_str()),
                    "exit_t": exit.map(|exit| exit.t)})
            })
            .collect();
        violations.push(json!({"type": "STALE_PROCESSES_LEFT", "members": members}));
    }
    let inprogress_owners_left = redis["inprogress"].as_array().map_or(0, Vec::len);
    if leftover_inprogress > 0 {
        violations.push(json!({"type": "INPROGRESS_LEFTOVER", "lists": leftover_lists}));
    }
    let mut other_keys: BTreeMap<String, u64> = BTreeMap::new();
    for key in redis["other"].as_array().cloned().unwrap_or_default() {
        let name = key["key"].as_str().unwrap_or("");
        let class = if name.starts_with("flush:") {
            "flush:*".to_owned()
        } else if name.starts_with("batch:") {
            "batch:* (finished batches linger)".to_owned()
        } else if name.starts_with("u|") || name.starts_with("b|") || name.starts_with("l|") {
            "feature minute counters".to_owned()
        } else if name.starts_with("it-") {
            "it-* (iteration state)".to_owned()
        } else if name.ends_with(":work") {
            "<identity>:work".to_owned()
        } else if name.starts_with("stat:") {
            "stat:*".to_owned()
        } else if name.starts_with("j|") || name.starts_with("h|") {
            "metrics".to_owned()
        } else if name.matches(':').count() >= 2 {
            "<identity> heartbeat".to_owned()
        } else {
            name.to_owned()
        };
        *other_keys.entry(class).or_default() += 1;
    }

    // ---- samples: resources and dashboard consistency
    let samples = read_all(&dir.join("samples.jsonl")).unwrap_or_default();
    let mut rss: BTreeMap<String, Vec<(i64, u64)>> = BTreeMap::new();
    let mut fds_max: BTreeMap<String, u64> = BTreeMap::new();
    let mut threads_max: BTreeMap<String, u64> = BTreeMap::new();
    let mut clients: Vec<(i64, u64)> = Vec::new();
    let mut used_memory: Vec<(i64, u64)> = Vec::new();
    let mut stale_processes_samples = 0;
    let mut stale_max = 0_i64;
    let mut stale_last: Option<Value> = None;
    let mut web_errors = 0;
    let mut web_mismatch = Vec::new();
    let mut processed_decreases = Vec::new();
    let mut last_processed: Option<(i64, u64)> = None;
    let mut samples_count = 0;
    for sample in &samples {
        samples_count += 1;
        let t = sample["t"].as_i64().unwrap_or(0);
        for worker in sample["workers"].as_array().cloned().unwrap_or_default() {
            let wid = worker["w"].as_str().unwrap_or("").to_owned();
            if let Some(kb) = worker["rss_kb"].as_u64() {
                rss.entry(wid.clone()).or_default().push((t, kb));
            }
            if let Some(fds) = worker["fds"].as_u64() {
                let entry = fds_max.entry(wid.clone()).or_default();
                *entry = (*entry).max(fds);
            }
            if let Some(threads) = worker["threads"].as_u64() {
                let entry = threads_max.entry(wid).or_default();
                *entry = (*entry).max(threads);
            }
        }
        let direct = &sample["direct"];
        if let Some(count) = direct["connected_clients"].as_u64() {
            clients.push((t, count));
        }
        if let Some(bytes) = direct["used_memory"].as_u64() {
            used_memory.push((t, bytes));
        }
        if let (Some(processes), Some(live)) =
            (direct["processes"].as_i64(), direct["live"].as_i64())
            && processes > live
        {
            stale_processes_samples += 1;
            stale_max = stale_max.max(processes - live);
            stale_last = Some(json!({"t": t, "processes": processes, "live": live}));
        }
        if let Some(processed) = direct["processed"].as_u64() {
            if let Some((_, previous)) = last_processed
                && processed < previous
            {
                processed_decreases.push(json!({"t": t, "from": previous, "to": processed}));
            }
            last_processed = Some((t, processed));
        }
        let web = &sample["web_stats"];
        if web.is_null() {
            continue;
        }
        if web["status"] != "200" {
            web_errors += 1;
            continue;
        }
        let body = &web["body"];
        let web_processes = &sample["web_processes"];
        // The dashboard's process count against the processes it lists.
        if let (Some(count), Some(listed)) =
            (body["processes"].as_u64(), web_processes["count"].as_u64())
            && count != listed
            && web_mismatch.len() < 10
        {
            web_mismatch.push(
                json!({"t": t, "stats.processes": count, "processes listed": listed,
                "direct.live": direct["live"], "direct.processes": direct["processes"]}),
            );
        }
        if let (Some(failed), Some(processed)) =
            (body["failed"].as_u64(), body["processed"].as_u64())
            && failed > processed
        {
            violations.push(json!({"type": "DASHBOARD_FAILED_ABOVE_PROCESSED", "t": t}));
        }
    }
    let mut rss_report = BTreeMap::new();
    for (wid, points) in &rss {
        if points.len() < 2 {
            continue;
        }
        let first = points[0];
        let last = points[points.len() - 1];
        let max = points.iter().map(|(_, kb)| *kb).max().unwrap_or(0);
        rss_report.insert(
            wid.clone(),
            json!({"first_kb": first.1, "last_kb": last.1, "max_kb": max,
                "life_s": (last.0 - first.0) / 1000}),
        );
    }
    let clients_max = clients.iter().map(|(_, c)| *c).max().unwrap_or(0);
    let clients_last = clients.last().map(|(_, c)| *c).unwrap_or(0);
    let memory_max = used_memory.iter().map(|(_, m)| *m).max().unwrap_or(0);

    let (pro, pro_violations) = crate::check_pro::check(dir)?;
    violations.extend(pro_violations);

    let report = json!({
        "dir": dir.display().to_string(),
        "pushes": pushes.len(),
        "push_records_duplicated": push_dups,
        "executions": execs.len(),
        "worker_instances": worker_files,
        "counts": counts,
        "duplicate_runs_by_cause": dup_causes,
        "duplicate_examples": dup_examples,
        "concurrent_runs_by_cause": overlap_causes,
        "concurrent_run_examples": overlaps,
        "executions_of_unknown_jobs": executions_of_unknown_jobs,
        "scheduled_lateness": lateness,
        "stats": {
            "comparable": stats_comparable,
            "processed_redis": processed_redis, "processed_observed": processed_observed,
            "processed_a_kill_may_lose": processed_unflushable,
            "failed_redis": failed_redis, "failed_observed": failed_observed,
            "failed_a_kill_may_lose": failed_unflushable,
            "violations": stats_violations,
        },
        "leftovers": {
            "inprogress_entries": leftover_inprogress,
            "processes_set_members": processes_left,
            "inprogress_set_members": inprogress_owners_left,
            "other_keys": other_keys,
            "after_stop": final_state["after_stop"].clone(),
            "web_after": final_state["web_after"].clone(),
        },
        "process_exits": {
            "unexpected": unexpected_exits,
            "stuck_terms": stuck_terms,
            "slow_shutdowns": slow_shutdowns,
            "shutdown_ms_max": shutdown_ms_all.iter().max(),
            "kills": kills.len(),
        },
        "redis_restarts": redis_restarts,
        "orphan_recoveries": recoveries.len(),
        "orphan_recoveries_of_live_processes": live_recoveries,
        "scenario_persist": scenario_persist,
        "worker_errors_by_context": errors,
        "resources": {
            "samples": samples_count,
            "rss": rss_report,
            "fds_max": fds_max,
            "threads_max": threads_max,
            "redis_clients_max": clients_max,
            "redis_clients_last": clients_last,
            "redis_used_memory_max": memory_max,
        },
        "dashboard": {
            "samples_with_stale_processes": stale_processes_samples,
            "stale_processes_max": stale_max,
            "stale_processes_last": stale_last,
            "web_errors": web_errors,
            "stats_vs_listed_mismatch_examples": web_mismatch,
            "processed_decreases": processed_decreases,
        },
        "pro": pro,
        "violations": violations,
    });
    std::fs::write(dir.join("report.json"), serde_json::to_vec_pretty(&report)?)?;
    Ok(report)
}
