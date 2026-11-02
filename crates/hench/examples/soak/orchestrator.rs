//! `soak run`: one scenario. Starts a private `redis-server`, optionally the reply-dropping
//! proxy, `hench web --api` (the dashboard's JSON API), N worker OS processes (each slot
//! restarted when its process ends), a pusher, a chaos controller and a sampler; then drains
//! (the jobs, then the processes that died from the `processes` set), stops the workers with
//! SIGTERM, snapshots Redis and runs the checker.
//!
//! Files in the scenario directory:
//! * `pushes.jsonl`: every push (nonce, jid, plan, options, whether Redis confirmed it);
//! * `exec-<wid>.jsonl`: each worker instance's executions (see `worker.rs`);
//! * `chaos.jsonl`: spawns, exits, signals, Redis restarts, pauses, proxy faults, drain;
//! * `samples.jsonl`: every 2 s, per-worker RSS/threads/fds, Redis clients and memory, direct
//!   Redis counts and the dashboard API's numbers;
//! * `batches.jsonl` (`--pro`): every batch commit and cancellation;
//! * `unique.jsonl` (`--pro`): every push of a unique job and what it did;
//! * `final.json`: Redis after the run; `pro.json` (`--pro`): the features' state after the
//!   run; `report.json`: the checker's verdicts.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use hench::redis_rs;
use hench::{BulkPush, Client, RedisConfig, RedisPool};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, Command};
use tokio::sync::{Mutex, mpsc, oneshot};

use crate::jsonlog::{JsonLog, now_ms};
use crate::proxy::{Mode, Proxy};
use crate::worker::{SoakIter, SoakJob};

#[derive(Debug, Clone)]
pub struct RunOptions {
    pub scenario: String,
    pub dir: PathBuf,
    pub duration: u64,
    pub workers: usize,
    pub concurrency: usize,
    /// reliable | basic | mixed (odd slots basic)
    pub fetch: String,
    /// aof | rdb | none
    pub persist: String,
    pub proxy: bool,
    pub actions: Vec<String>,
    pub chaos_min: u64,
    pub chaos_max: u64,
    /// Chaos stops after this many seconds (default: the whole duration).
    pub chaos_until: Option<u64>,
    pub port: u16,
    pub proxy_port: u16,
    pub web_port: u16,
    pub timeout: u64,
    pub orphan_check: u64,
    pub heartbeat: u64,
    pub drain: u64,
    /// Single pushes per second (approximate).
    pub rate: u64,
    pub nonce_base: u64,
    pub hench_bin: Option<PathBuf>,
    /// The pro features' load and checks (`pro.rs`).
    pub pro: bool,
    pub self_exe: PathBuf,
}

/// Commands for a worker slot.
enum SlotCmd {
    Kill9 {
        restart_after: Duration,
    },
    Term {
        restart_after: Duration,
    },
    DoubleTerm {
        restart_after: Duration,
    },
    Tstp {
        term_after: Duration,
        restart_after: Duration,
    },
    Final(oneshot::Sender<()>),
}

struct Ctx {
    opts: RunOptions,
    chaos: Arc<JsonLog>,
    worker_url: String,
    /// wid -> pid of live worker processes.
    live: std::sync::Mutex<HashMap<String, u32>>,
    /// pids whose exit is expected (remote stop).
    expect_exit: std::sync::Mutex<HashSet<u32>>,
    redis: Mutex<RedisServer>,
    proxy: Option<Arc<Proxy>>,
    stop_chaos: AtomicBool,
}

impl Ctx {
    fn log(&self, mut value: Value) {
        if let Some(object) = value.as_object_mut() {
            object.insert("t".into(), Value::from(now_ms()));
        }
        self.chaos.write(&value);
    }
}

/// Sends `sig` to the process `pid` (never to a process group: pid 0 is refused).
fn signal(pid: u32, sig: i32) -> bool {
    let Ok(pid) = libc::pid_t::try_from(pid) else {
        return false;
    };
    if pid <= 0 {
        return false;
    }
    // SAFETY: kill(2) takes plain integers and has no memory effects; the pid is positive, so
    // it addresses one process (a child of this harness), never a group.
    unsafe { libc::kill(pid, sig) == 0 }
}

// ---------------------------------------------------------------------------------------------
// Redis server management

struct RedisServer {
    child: Option<Child>,
    port: u16,
    dir: PathBuf,
    persist: String,
}

impl RedisServer {
    async fn start(&mut self) -> anyhow::Result<()> {
        let mut command = Command::new("redis-server");
        command
            .arg("--port")
            .arg(self.port.to_string())
            .arg("--bind")
            .arg("127.0.0.1")
            .arg("--dir")
            .arg(&self.dir)
            .arg("--logfile")
            .arg(self.dir.join("redis.log"))
            .arg("--maxmemory-policy")
            .arg("noeviction")
            .arg("--save")
            .arg("")
            .arg("--daemonize")
            .arg("no");
        match self.persist.as_str() {
            "aof" => {
                command.args(["--appendonly", "yes", "--appendfsync", "everysec"]);
            }
            _ => {
                command.args(["--appendonly", "no"]);
            }
        }
        command
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        self.child = Some(command.spawn()?);
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Ok(mut conn) = admin(self.port).await {
                let pong: redis_rs::RedisResult<String> =
                    redis_rs::cmd("PING").query_async(&mut conn).await;
                if pong.is_ok() {
                    return Ok(());
                }
            }
            if Instant::now() > deadline {
                anyhow::bail!("redis-server did not start");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// `SHUTDOWN SAVE|NOSAVE` (rdb: SAVE; aof: the AOF is fsynced; none: NOSAVE).
    async fn shutdown(&mut self) {
        let mode = if self.persist == "rdb" {
            "SAVE"
        } else {
            "NOSAVE"
        };
        if let Ok(mut conn) = admin(self.port).await {
            let _: redis_rs::RedisResult<()> = redis_rs::cmd("SHUTDOWN")
                .arg(mode)
                .query_async(&mut conn)
                .await;
        }
        self.wait_exit().await;
    }

    async fn kill9(&mut self) {
        if let Some(child) = &self.child
            && let Some(pid) = child.id()
        {
            signal(pid, libc::SIGKILL);
        }
        self.wait_exit().await;
    }

    async fn wait_exit(&mut self) {
        if let Some(mut child) = self.child.take()
            && tokio::time::timeout(Duration::from_secs(15), child.wait())
                .await
                .is_err()
        {
            let _ = child.kill().await;
        }
    }
}

/// A fresh admin connection to Redis (10 s response timeout).
pub(crate) async fn admin(port: u16) -> anyhow::Result<redis_rs::aio::MultiplexedConnection> {
    let client = redis_rs::Client::open(format!("redis://127.0.0.1:{port}/"))?;
    let config = redis_rs::AsyncConnectionConfig::new()
        .set_connection_timeout(Some(Duration::from_secs(2)))
        .set_response_timeout(Some(Duration::from_secs(10)));
    Ok(client
        .get_multiplexed_async_connection_with_config(&config)
        .await?)
}

// ---------------------------------------------------------------------------------------------
// Worker slots

fn slot_fetch(opts: &RunOptions, slot: usize) -> &'static str {
    match opts.fetch.as_str() {
        "basic" => "basic",
        "mixed" if slot % 2 == 1 => "basic",
        _ => "reliable",
    }
}

fn spawn_worker(ctx: &Ctx, slot: usize, wid: &str) -> anyhow::Result<Child> {
    let opts = &ctx.opts;
    let stderr = std::fs::File::create(opts.dir.join(format!("worker-{wid}.log")))?;
    let child = Command::new(&opts.self_exe)
        .arg("worker")
        .arg("--wid")
        .arg(wid)
        .arg("--url")
        .arg(&ctx.worker_url)
        .arg("--log")
        .arg(opts.dir.join(format!("exec-{wid}.jsonl")))
        .arg("--concurrency")
        .arg(opts.concurrency.to_string())
        .arg("--fetch")
        .arg(slot_fetch(opts, slot))
        .arg("--timeout")
        .arg(opts.timeout.to_string())
        .arg("--orphan-check")
        .arg(opts.orphan_check.to_string())
        .arg("--heartbeat")
        .arg(opts.heartbeat.to_string())
        .args(opts.pro.then_some("--pro"))
        .env("RUST_LOG", "warn,hench=info")
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr))
        .kill_on_drop(true)
        .spawn()?;
    Ok(child)
}

fn describe(status: &std::process::ExitStatus) -> Value {
    use std::os::unix::process::ExitStatusExt;
    json!({"code": status.code(), "signal": status.signal()})
}

/// How long a SIGTERM may take before the slot gives up and kills the process.
fn term_limit(opts: &RunOptions) -> Duration {
    Duration::from_secs(opts.timeout + 40)
}

async fn term_and_wait(ctx: &Ctx, child: &mut Child, wid: &str, pid: u32, double: bool) {
    let started = Instant::now();
    signal(pid, libc::SIGTERM);
    if double {
        tokio::time::sleep(Duration::from_millis(300)).await;
        signal(pid, libc::SIGTERM);
    }
    match tokio::time::timeout(term_limit(&ctx.opts), child.wait()).await {
        Ok(Ok(status)) => ctx.log(json!({
            "e": "exit", "w": wid, "p": pid, "status": describe(&status), "expected": true,
            "shutdown_ms": started.elapsed().as_millis() as u64, "how": if double {"dterm"} else {"term"},
        })),
        _ => {
            ctx.log(json!({"e": "term_stuck", "w": wid, "p": pid,
                "waited_ms": started.elapsed().as_millis() as u64}));
            signal(pid, libc::SIGKILL);
            let _ = child.wait().await;
            ctx.log(json!({"e": "exit", "w": wid, "p": pid, "expected": true, "how": "kill_after_stuck"}));
        }
    }
}

async fn slot_task(ctx: Arc<Ctx>, slot: usize, mut commands: mpsc::Receiver<SlotCmd>) {
    let mut launch = 0;
    loop {
        launch += 1;
        let wid = format!("w{slot}.{launch}");
        let mut child = match spawn_worker(&ctx, slot, &wid) {
            Ok(child) => child,
            Err(error) => {
                ctx.log(json!({"e": "spawn_failed", "w": wid, "error": error.to_string()}));
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        let pid = child.id().unwrap_or(0);
        ctx.live.lock().unwrap().insert(wid.clone(), pid);
        ctx.log(json!({"e": "spawn", "w": wid, "p": pid, "fetch": slot_fetch(&ctx.opts, slot)}));
        let mut restart_after = Duration::from_secs(1);
        let mut final_ack: Option<oneshot::Sender<()>> = None;
        let mut closed = false;
        tokio::select! {
            status = child.wait() => {
                let expected = ctx.expect_exit.lock().unwrap().remove(&pid);
                let status = status.map(|s| describe(&s)).unwrap_or(Value::Null);
                ctx.log(json!({"e": "exit", "w": wid, "p": pid, "status": status,
                    "expected": expected, "how": if expected {"remote_stop"} else {"unexpected"}}));
            }
            command = commands.recv() => match command {
                Some(SlotCmd::Kill9 { restart_after: after }) => {
                    signal(pid, libc::SIGKILL);
                    ctx.log(json!({"e": "kill9", "w": wid, "p": pid}));
                    let _ = child.wait().await;
                    ctx.log(json!({"e": "exit", "w": wid, "p": pid, "expected": true, "how": "kill9"}));
                    restart_after = after;
                }
                Some(SlotCmd::Term { restart_after: after }) => {
                    ctx.log(json!({"e": "term", "w": wid, "p": pid}));
                    term_and_wait(&ctx, &mut child, &wid, pid, false).await;
                    restart_after = after;
                }
                Some(SlotCmd::DoubleTerm { restart_after: after }) => {
                    ctx.log(json!({"e": "dterm", "w": wid, "p": pid}));
                    term_and_wait(&ctx, &mut child, &wid, pid, true).await;
                    restart_after = after;
                }
                Some(SlotCmd::Tstp { term_after, restart_after: after }) => {
                    signal(pid, libc::SIGTSTP);
                    ctx.log(json!({"e": "tstp", "w": wid, "p": pid}));
                    tokio::select! {
                        _ = tokio::time::sleep(term_after) => {
                            ctx.log(json!({"e": "term", "w": wid, "p": pid, "after": "tstp"}));
                            term_and_wait(&ctx, &mut child, &wid, pid, false).await;
                        }
                        status = child.wait() => {
                            let status = status.map(|s| describe(&s)).unwrap_or(Value::Null);
                            ctx.log(json!({"e": "exit", "w": wid, "p": pid, "status": status,
                                "expected": false, "how": "exited_while_quiet"}));
                        }
                    }
                    restart_after = after;
                }
                Some(SlotCmd::Final(ack)) => {
                    ctx.log(json!({"e": "term", "w": wid, "p": pid, "final": true}));
                    term_and_wait(&ctx, &mut child, &wid, pid, false).await;
                    final_ack = Some(ack);
                }
                None => {
                    term_and_wait(&ctx, &mut child, &wid, pid, false).await;
                    closed = true;
                }
            }
        }
        ctx.live.lock().unwrap().remove(&wid);
        if let Some(ack) = final_ack {
            let _ = ack.send(());
            return;
        }
        if closed {
            return;
        }
        tokio::time::sleep(restart_after).await;
    }
}

// ---------------------------------------------------------------------------------------------
// Pusher

struct Plan {
    plan: &'static str,
    param: u64,
    retry: Value,
    dead: Option<bool>,
}

fn pick_plan() -> Plan {
    let roll = fastrand::u32(0..1000);
    let (plan, param, retry, dead) = match roll {
        0..550 => ("ok", fastrand::u64(0..20), json!(3), None),
        550..670 => ("slow", fastrand::u64(200..2500), json!(3), None),
        670..680 => ("vslow", fastrand::u64(12_000..25_000), json!(3), None),
        680..710 => ("coop", fastrand::u64(2_000..8_000), json!(3), None),
        710..810 => ("errk", fastrand::u64(1..3), json!(3), None),
        810..860 => ("panick", 1, json!(3), None),
        860..900 => ("aerr", 0, json!(2), None),
        900..920 => ("apanic", 0, json!(1), None),
        920..960 => ("noretry", 0, json!(false), None),
        _ => ("deadfalse", 0, json!(1), Some(false)),
    };
    Plan {
        plan,
        param,
        retry,
        dead,
    }
}

fn pick_queue() -> &'static str {
    match fastrand::u32(0..10) {
        0..2 => "critical",
        2..7 => "default",
        _ => "low",
    }
}

async fn push_single(client: &Client, pushes: &JsonLog, nonce: u64) {
    let plan = pick_plan();
    let queue = pick_queue();
    let jid = hench::generate_jid();
    let scheduled = (fastrand::u32(0..100) < 15).then(|| fastrand::u64(1_000..20_000));
    let mut builder = client.job::<SoakJob>().queue(queue).jid(jid.clone());
    builder = match &plan.retry {
        Value::Bool(retry) => builder.retry(*retry),
        other => builder.retry(other.as_u64().unwrap_or(3) as u32),
    };
    if let Some(dead) = plan.dead {
        builder = builder.dead(dead);
    }
    if let Some(ms) = scheduled {
        builder = builder.delay(Duration::from_millis(ms));
    }
    let started = now_ms();
    let result = builder
        .perform_async((plan.plan.to_owned(), plan.param, nonce))
        .await;
    pushes.write(&json!({
        "e": "push", "k": "single", "n": nonce, "j": jid, "plan": plan.plan, "param": plan.param,
        "q": queue, "retry": plan.retry, "dead": plan.dead,
        "at": scheduled.map(|ms| started + ms as i64),
        "ok": result.is_ok(), "err": result.as_ref().err().map(|e| e.to_string()),
        "t0": started, "t": now_ms(),
    }));
}

async fn push_bulk(client: &Client, pushes: &JsonLog, first_nonce: u64, count: u64) {
    let queue = pick_queue();
    let mut args = Vec::new();
    let mut plans = Vec::new();
    for offset in 0..count {
        let (plan, param) = if fastrand::u32(0..10) == 0 {
            ("errk", 1)
        } else {
            ("ok", fastrand::u64(0..10))
        };
        plans.push((plan, param));
        args.push(vec![json!(plan), json!(param), json!(first_nonce + offset)]);
    }
    let started = now_ms();
    let result = client
        .push_bulk(BulkPush::new("SoakJob", args).queue(queue).retry(3_u32))
        .await;
    let jids: Vec<Option<String>> = match &result {
        Ok(jids) => jids.clone(),
        Err(_) => vec![None; count as usize],
    };
    for (offset, ((plan, param), jid)) in plans.iter().zip(jids).enumerate() {
        pushes.write(&json!({
            "e": "push", "k": "bulk", "n": first_nonce + offset as u64, "j": jid, "plan": plan,
            "param": param, "q": queue, "retry": 3, "dead": Value::Null, "at": Value::Null,
            "ok": result.is_ok(), "err": result.as_ref().err().map(|e| e.to_string()),
            "t0": started, "t": now_ms(),
        }));
    }
}

async fn push_iterable(client: &Client, pushes: &JsonLog, nonce: u64) {
    let queue = pick_queue();
    let jid = hench::generate_jid();
    let items = fastrand::u64(20..60);
    let ms = fastrand::u64(40..160);
    let started = now_ms();
    let result = client
        .job::<SoakIter>()
        .queue(queue)
        .retry(3_u32)
        .jid(jid.clone())
        .perform_async((items, ms, nonce))
        .await;
    pushes.write(&json!({
        "e": "push", "k": "iter", "n": nonce, "j": jid, "plan": "iter", "param": items,
        "ms": ms, "q": queue, "retry": 3, "dead": Value::Null, "at": Value::Null,
        "ok": result.is_ok(), "err": result.as_ref().err().map(|e| e.to_string()),
        "t0": started, "t": now_ms(),
    }));
}

async fn pusher(
    ctx: Arc<Ctx>,
    client: Client,
    pushes: Arc<JsonLog>,
    batches: Arc<JsonLog>,
    uniques: Arc<JsonLog>,
    deadline: Instant,
) -> (u64, Vec<String>) {
    let mut nonce = ctx.opts.nonce_base;
    let mut ticker = tokio::time::interval(Duration::from_millis(100));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let per_tick = (ctx.opts.rate / 10).max(1);
    let mut last_bulk = Instant::now();
    let mut last_iter = Instant::now();
    let mut last_batch = Instant::now();
    // The pro load: committed batches, and the cancellations still waiting; the unique pushes;
    // the burst batch, 40% into the load.
    let mut bids = Vec::new();
    let mut cancels = Vec::new();
    let mut last_unique = Instant::now();
    let mut unique_id = 0_u64;
    let burst_at = Instant::now()
        + deadline
            .saturating_duration_since(Instant::now())
            .mul_f64(0.4);
    let mut burst_task = None;
    while Instant::now() < deadline {
        ticker.tick().await;
        let pushes_now = fastrand::u64(0..=per_tick * 2);
        let mut tasks = Vec::new();
        for _ in 0..pushes_now {
            nonce += 1;
            let (client, pushes) = (client.clone(), pushes.clone());
            let n = nonce;
            tasks.push(tokio::spawn(async move {
                push_single(&client, &pushes, n).await
            }));
        }
        if last_bulk.elapsed() > Duration::from_secs(fastrand::u64(6..14)) {
            last_bulk = Instant::now();
            let count = fastrand::u64(100..600);
            let first = nonce + 1;
            nonce += count;
            let (client, pushes) = (client.clone(), pushes.clone());
            tasks.push(tokio::spawn(async move {
                push_bulk(&client, &pushes, first, count).await
            }));
        }
        if last_iter.elapsed() > Duration::from_secs(fastrand::u64(2..6)) {
            last_iter = Instant::now();
            nonce += 1;
            let n = nonce;
            let (client, pushes) = (client.clone(), pushes.clone());
            tasks.push(tokio::spawn(async move {
                push_iterable(&client, &pushes, n).await
            }));
        }
        if ctx.opts.pro && last_unique.elapsed() > Duration::from_millis(fastrand::u64(200..600)) {
            last_unique = Instant::now();
            unique_id += 1;
            let (client, uniques, id) = (client.clone(), uniques.clone(), unique_id);
            tasks.push(tokio::spawn(async move {
                crate::pro::push_unique(&client, &uniques, id).await
            }));
        }
        if ctx.opts.pro && burst_task.is_none() && Instant::now() >= burst_at {
            let count = crate::pro::BURST_MEMBERS;
            let first = nonce + 1;
            nonce += count + 1;
            let (client, pushes, batches) = (client.clone(), pushes.clone(), batches.clone());
            burst_task = Some(tokio::spawn(async move {
                crate::pro::push_batch(&client, &pushes, &batches, first, count, true).await
            }));
        }
        let mut batch_task = None;
        if ctx.opts.pro && last_batch.elapsed() > Duration::from_secs(fastrand::u64(2..5)) {
            last_batch = Instant::now();
            let count = fastrand::u64(5..=20);
            let first = nonce + 1;
            // The members' nonces, then the batch's own (its callbacks').
            nonce += count + 1;
            let (client, pushes, batches) = (client.clone(), pushes.clone(), batches.clone());
            batch_task = Some(tokio::spawn(async move {
                crate::pro::push_batch(&client, &pushes, &batches, first, count, false).await
            }));
        }
        for task in tasks {
            let _ = task.await;
        }
        if let Some(task) = batch_task
            && let Ok(Some(bid)) = task.await
        {
            if fastrand::u32(0..5) == 0 {
                let delay = Duration::from_millis(fastrand::u64(100..3_000));
                let (client, batches, bid) = (client.clone(), batches.clone(), bid.clone());
                cancels.push(tokio::spawn(async move {
                    crate::pro::cancel_later(client, &batches, bid, delay).await
                }));
            }
            bids.push(bid);
        }
    }
    for cancel in cancels {
        let _ = cancel.await;
    }
    if let Some(task) = burst_task
        && let Ok(Some(bid)) = task.await
    {
        bids.push(bid);
    }
    (nonce - ctx.opts.nonce_base, bids)
}

// ---------------------------------------------------------------------------------------------
// Chaos

async fn live_identities(port: u16) -> Vec<(String, Value)> {
    let Ok(mut conn) = admin(port).await else {
        return Vec::new();
    };
    let members: Vec<String> = redis_rs::cmd("SMEMBERS")
        .arg("processes")
        .query_async(&mut conn)
        .await
        .unwrap_or_default();
    let mut out = Vec::new();
    for identity in members {
        let info: Option<String> = redis_rs::cmd("HGET")
            .arg(&identity)
            .arg("info")
            .query_async(&mut conn)
            .await
            .unwrap_or(None);
        if let Some(info) = info {
            out.push((identity, serde_json::from_str(&info).unwrap_or(Value::Null)));
        }
    }
    out
}

async fn chaos(ctx: Arc<Ctx>, slots: Vec<mpsc::Sender<SlotCmd>>, deadline: Instant) {
    let opts = ctx.opts.clone();
    if opts.actions.is_empty() {
        return;
    }
    while Instant::now() < deadline && !ctx.stop_chaos.load(Ordering::Relaxed) {
        let pause = fastrand::u64(opts.chaos_min * 1000..=opts.chaos_max * 1000);
        tokio::time::sleep(Duration::from_millis(pause)).await;
        if Instant::now() >= deadline {
            break;
        }
        let action = opts.actions[fastrand::usize(0..opts.actions.len())].as_str();
        let slot = fastrand::usize(0..slots.len());
        let started = Instant::now();
        match action {
            "kill9" => {
                let restart_after = Duration::from_millis(fastrand::u64(0..3000));
                let _ = slots[slot].send(SlotCmd::Kill9 { restart_after }).await;
            }
            "term" => {
                let restart_after = Duration::from_millis(fastrand::u64(0..2000));
                let _ = slots[slot].send(SlotCmd::Term { restart_after }).await;
            }
            "dterm" => {
                let restart_after = Duration::from_millis(fastrand::u64(0..2000));
                let _ = slots[slot]
                    .send(SlotCmd::DoubleTerm { restart_after })
                    .await;
            }
            "tstp" => {
                let term_after = Duration::from_millis(fastrand::u64(3000..12000));
                let restart_after = Duration::from_millis(fastrand::u64(0..2000));
                let _ = slots[slot]
                    .send(SlotCmd::Tstp {
                        term_after,
                        restart_after,
                    })
                    .await;
            }
            "remote_stop" | "remote_quiet" => {
                let processes = live_identities(opts.port).await;
                if processes.is_empty() {
                    continue;
                }
                let (identity, info) = &processes[fastrand::usize(0..processes.len())];
                let pid = info.get("pid").and_then(Value::as_u64).unwrap_or(0) as u32;
                let pool = match RedisPool::lazy(RedisConfig::new(format!(
                    "redis://127.0.0.1:{}/",
                    opts.port
                ))) {
                    Ok(pool) => pool,
                    Err(_) => continue,
                };
                let api = hench::api::Api::new(pool.clone());
                let Ok(Some(process)) = api.processes().get(identity).await else {
                    continue;
                };
                if action == "remote_stop" {
                    ctx.expect_exit.lock().unwrap().insert(pid);
                    let result = process.stop().await;
                    ctx.log(json!({"e": "remote_stop", "identity": identity, "p": pid,
                        "ok": result.is_ok()}));
                } else {
                    let result = process.quiet().await;
                    ctx.log(json!({"e": "remote_quiet", "identity": identity, "p": pid,
                        "ok": result.is_ok()}));
                    // Then stop it locally a little later, as a deploy would.
                    tokio::time::sleep(Duration::from_millis(fastrand::u64(2000..6000))).await;
                    let wid = ctx
                        .live
                        .lock()
                        .unwrap()
                        .iter()
                        .find(|(_, p)| **p == pid)
                        .map(|(wid, _)| wid.clone());
                    if let Some(wid) = wid {
                        let index: usize = wid[1..wid.find('.').unwrap_or(1)].parse().unwrap_or(0);
                        if let Some(sender) = slots.get(index) {
                            let _ = sender
                                .send(SlotCmd::Term {
                                    restart_after: Duration::from_millis(500),
                                })
                                .await;
                        }
                    }
                }
            }
            "redis_restart" | "redis_kill9" => {
                let mut redis = ctx.redis.lock().await;
                let down = Duration::from_millis(fastrand::u64(200..4000));
                ctx.log(json!({"e": "redis_down", "how": action, "persist": redis.persist}));
                if action == "redis_kill9" {
                    redis.kill9().await;
                } else {
                    redis.shutdown().await;
                }
                tokio::time::sleep(down).await;
                let started_redis = redis.start().await;
                ctx.log(json!({"e": "redis_up", "ok": started_redis.is_ok(),
                    "down_ms": started.elapsed().as_millis() as u64, "persist": redis.persist}));
            }
            "pause_write" | "pause_all" => {
                let ms = if action == "pause_write" {
                    fastrand::u64(1000..12_000)
                } else {
                    fastrand::u64(1000..8_000)
                };
                let mode = if action == "pause_write" {
                    "WRITE"
                } else {
                    "ALL"
                };
                let result = match admin(opts.port).await {
                    Ok(mut conn) => redis_rs::cmd("CLIENT")
                        .arg("PAUSE")
                        .arg(ms)
                        .arg(mode)
                        .query_async::<()>(&mut conn)
                        .await
                        .map_err(|e| e.to_string()),
                    Err(error) => Err(error.to_string()),
                };
                ctx.log(json!({"e": "client_pause", "mode": mode, "ms": ms, "ok": result.is_ok()}));
                tokio::time::sleep(Duration::from_millis(ms)).await;
            }
            "long_pause" => {
                let ms = 70_000;
                let result = match admin(opts.port).await {
                    Ok(mut conn) => redis_rs::cmd("CLIENT")
                        .arg("PAUSE")
                        .arg(ms)
                        .arg("WRITE")
                        .query_async::<()>(&mut conn)
                        .await
                        .map_err(|e| e.to_string()),
                    Err(error) => Err(error.to_string()),
                };
                ctx.log(
                    json!({"e": "client_pause", "mode": "WRITE", "ms": ms, "ok": result.is_ok()}),
                );
                tokio::time::sleep(Duration::from_millis(ms)).await;
            }
            "proxy_drop" => {
                if let Some(proxy) = &ctx.proxy {
                    let ms = fastrand::u64(1000..8000);
                    proxy.set(Mode::Blackhole);
                    ctx.log(json!({"e": "proxy_blackhole", "ms": ms}));
                    tokio::time::sleep(Duration::from_millis(ms)).await;
                    proxy.set(Mode::Pass);
                    ctx.log(json!({"e": "proxy_pass"}));
                }
            }
            "proxy_delay" => {
                if let Some(proxy) = &ctx.proxy {
                    let delay = fastrand::u64(200..4500);
                    let ms = fastrand::u64(4000..15_000);
                    proxy.set(Mode::Delay(delay));
                    ctx.log(json!({"e": "proxy_delay", "delay_ms": delay, "ms": ms}));
                    tokio::time::sleep(Duration::from_millis(ms)).await;
                    proxy.set(Mode::Pass);
                    ctx.log(json!({"e": "proxy_pass"}));
                }
            }
            "proxy_reset" => {
                if let Some(proxy) = &ctx.proxy {
                    proxy.reset();
                    ctx.log(json!({"e": "proxy_reset"}));
                }
            }
            "queue_pause" => {
                if let Ok(pool) = RedisPool::lazy(RedisConfig::new(format!(
                    "redis://127.0.0.1:{}/",
                    opts.port
                ))) {
                    let api = hench::api::Api::new(pool.clone());
                    let queue = api.queue(pick_queue());
                    let paused = queue.pause().await;
                    ctx.log(json!({"e": "queue_pause", "q": queue.name(), "ok": paused.is_ok()}));
                    tokio::time::sleep(Duration::from_millis(fastrand::u64(2000..8000))).await;
                    let resumed = queue.unpause().await;
                    ctx.log(
                        json!({"e": "queue_unpause", "q": queue.name(), "ok": resumed.is_ok()}),
                    );
                }
            }
            other => ctx.log(json!({"e": "unknown_action", "action": other})),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Sampling

fn proc_stats(pid: u32) -> Value {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap_or_default();
    let field = |name: &str| -> Option<u64> {
        status
            .lines()
            .find_map(|line| line.strip_prefix(name))
            .and_then(|rest| rest.split_whitespace().next())
            .and_then(|value| value.parse().ok())
    };
    let fds = std::fs::read_dir(format!("/proc/{pid}/fd"))
        .map(|dir| dir.count())
        .ok();
    json!({"rss_kb": field("VmRSS:"), "threads": field("Threads:"), "fds": fds})
}

fn info_field(info: &str, name: &str) -> Option<u64> {
    info.lines()
        .find_map(|line| line.strip_prefix(&format!("{name}:")))
        .and_then(|value| value.trim().parse().ok())
}

/// Counts read straight from Redis.
pub async fn direct_counts(port: u16) -> anyhow::Result<Value> {
    let mut conn = admin(port).await?;
    let info: String = redis_rs::cmd("INFO").query_async(&mut conn).await?;
    let processes: Vec<String> = redis_rs::cmd("SMEMBERS")
        .arg("processes")
        .query_async(&mut conn)
        .await?;
    let mut live = 0;
    let mut busy = 0;
    let mut identities = Vec::new();
    for identity in &processes {
        let (exists, busy_field, info_json): (bool, Option<u64>, Option<String>) = redis_rs::pipe()
            .cmd("EXISTS")
            .arg(identity)
            .cmd("HGET")
            .arg(identity)
            .arg("busy")
            .cmd("HGET")
            .arg(identity)
            .arg("info")
            .query_async(&mut conn)
            .await?;
        if exists {
            live += 1;
        }
        busy += busy_field.unwrap_or(0);
        let pid = info_json
            .and_then(|info| serde_json::from_str::<Value>(&info).ok())
            .and_then(|info| info.get("pid").cloned());
        identities.push(json!({"id": identity, "alive": exists, "pid": pid}));
    }
    let queues: Vec<String> = redis_rs::cmd("SMEMBERS")
        .arg("queues")
        .query_async(&mut conn)
        .await?;
    let mut enqueued = 0_u64;
    for queue in &queues {
        let length: u64 = redis_rs::cmd("LLEN")
            .arg(format!("queue:{queue}"))
            .query_async(&mut conn)
            .await?;
        enqueued += length;
    }
    let mut inprogress_keys: Vec<String> = Vec::new();
    let mut cursor: u64 = 0;
    loop {
        let (next, keys): (u64, Vec<String>) = redis_rs::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg("*:inprogress")
            .arg("COUNT")
            .arg(1000)
            .query_async(&mut conn)
            .await?;
        inprogress_keys.extend(keys);
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    let mut inprogress = 0_u64;
    for key in &inprogress_keys {
        let length: u64 = redis_rs::cmd("LLEN")
            .arg(key)
            .query_async(&mut conn)
            .await?;
        inprogress += length;
    }
    let inprogress_owners: Vec<String> = redis_rs::cmd("SMEMBERS")
        .arg("inprogress")
        .query_async(&mut conn)
        .await?;
    let (schedule, retry, dead, processed, failed, outbox, suspects): (
        u64,
        u64,
        u64,
        Option<u64>,
        Option<u64>,
        u64,
        u64,
    ) = redis_rs::pipe()
        .cmd("ZCARD")
        .arg("schedule")
        .cmd("ZCARD")
        .arg("retry")
        .cmd("ZCARD")
        .arg("dead")
        .cmd("GET")
        .arg("stat:processed")
        .cmd("GET")
        .arg("stat:failed")
        .cmd("ZCARD")
        .arg(hench::keys::BATCH_EVENTS)
        // Jobs in flight at two crashes, waiting to come back one at a time (crash loops).
        .cmd("ZCARD")
        .arg(hench::keys::RECOVERY_SUSPECTS)
        .query_async(&mut conn)
        .await?;
    // Jobs already due but not yet enqueued by any scheduler, and how overdue the oldest is.
    let now = now_ms() as f64 / 1000.0;
    let mut due = serde_json::Map::new();
    for set in ["schedule", "retry"] {
        let (count, oldest): (u64, Vec<(String, f64)>) = redis_rs::pipe()
            .cmd("ZCOUNT")
            .arg(set)
            .arg("-inf")
            .arg(now)
            .cmd("ZRANGE")
            .arg(set)
            .arg(0)
            .arg(0)
            .arg("WITHSCORES")
            .query_async(&mut conn)
            .await?;
        let overdue = oldest
            .first()
            .map(|(_, score)| ((now - score) * 1000.0) as i64)
            .filter(|ms| *ms > 0)
            .unwrap_or(0);
        due.insert(
            set.to_owned(),
            json!({"due": count, "oldest_overdue_ms": overdue}),
        );
    }
    Ok(json!({
        "processes": processes.len(), "live": live, "busy": busy, "identities": identities,
        "enqueued": enqueued, "schedule": schedule, "retry": retry, "dead": dead,
        "inprogress": inprogress, "inprogress_keys": inprogress_keys.len(),
        "inprogress_owners": inprogress_owners.len(),
        "processed": processed.unwrap_or(0), "failed": failed.unwrap_or(0),
        "batch_outbox": outbox, "recovery_suspects": suspects, "due": due,
        "connected_clients": info_field(&info, "connected_clients"),
        "blocked_clients": info_field(&info, "blocked_clients"),
        "used_memory": info_field(&info, "used_memory"),
        "dbsize_keys": info.lines().find(|l| l.starts_with("db0:")).map(str::to_owned),
    }))
}

/// GET on the dashboard server; the parsed JSON body.
async fn http_get_json(port: u16, path: &str) -> anyhow::Result<Value> {
    let mut stream = tokio::time::timeout(
        Duration::from_secs(3),
        tokio::net::TcpStream::connect(("127.0.0.1", port)),
    )
    .await??;
    let request = format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await?;
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut response)).await??;
    let text = String::from_utf8_lossy(&response);
    let (head, body) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
    let status = head.split_whitespace().nth(1).unwrap_or("0").to_owned();
    // The server may answer with chunked encoding.
    let body = if head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        let mut out = String::new();
        let mut rest = body;
        while let Some((size, after)) = rest.split_once("\r\n") {
            let size = usize::from_str_radix(size.trim(), 16).unwrap_or(0);
            if size == 0 || after.len() < size {
                break;
            }
            out.push_str(&after[..size]);
            rest = after[size..].trim_start_matches("\r\n");
        }
        out
    } else {
        body.to_owned()
    };
    let parsed: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
    Ok(json!({"status": status, "body": parsed}))
}

async fn sampler(ctx: Arc<Ctx>, samples: Arc<JsonLog>, stop: Arc<AtomicBool>) {
    while !stop.load(Ordering::Relaxed) {
        let started = Instant::now();
        let workers: Vec<Value> = {
            let live = ctx.live.lock().unwrap().clone();
            live.iter()
                .map(|(wid, pid)| {
                    let mut stats = proc_stats(*pid);
                    stats["w"] = json!(wid);
                    stats["p"] = json!(pid);
                    stats
                })
                .collect()
        };
        let direct = direct_counts(ctx.opts.port)
            .await
            .unwrap_or_else(|e| json!({"error": e.to_string()}));
        let (web_stats, web_processes) = if ctx.opts.hench_bin.is_some() {
            let stats = http_get_json(ctx.opts.web_port, "/api/v1/stats")
                .await
                .unwrap_or_else(|e| json!({"error": e.to_string()}));
            let processes = http_get_json(ctx.opts.web_port, "/api/v1/processes")
                .await
                .map(|value| {
                    let list = value["body"].as_array().cloned().unwrap_or_default();
                    json!({"status": value["status"], "count": list.len(),
                        "busy": list.iter().map(|p| p["busy"].as_u64().unwrap_or(0)).sum::<u64>(),
                        "pids": list.iter().map(|p| p["pid"].clone()).collect::<Vec<_>>()})
                })
                .unwrap_or_else(|e| json!({"error": e.to_string()}));
            (stats, processes)
        } else {
            (Value::Null, Value::Null)
        };
        let proxy = ctx.proxy.as_ref().map(|proxy| {
            json!({"connections": proxy.connections.load(Ordering::Relaxed),
                "accepted": proxy.accepted.load(Ordering::Relaxed)})
        });
        samples.write(&json!({
            "t": now_ms(), "workers": workers, "direct": direct, "web_stats": web_stats,
            "web_processes": web_processes, "proxy": proxy,
            "sample_ms": started.elapsed().as_millis() as u64,
        }));
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

// ---------------------------------------------------------------------------------------------
// Final snapshot

async fn snapshot(port: u16) -> anyhow::Result<Value> {
    let mut conn = admin(port).await?;
    let mut keys: Vec<String> = Vec::new();
    let mut cursor: u64 = 0;
    loop {
        let (next, batch): (u64, Vec<String>) = redis_rs::cmd("SCAN")
            .arg(cursor)
            .arg("COUNT")
            .arg(1000)
            .query_async(&mut conn)
            .await?;
        keys.extend(batch);
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    keys.sort();
    let mut lists = serde_json::Map::new();
    let mut sets = serde_json::Map::new();
    let mut other = Vec::new();
    for key in &keys {
        let kind: String = redis_rs::cmd("TYPE")
            .arg(key)
            .query_async(&mut conn)
            .await?;
        match kind.as_str() {
            "list" => {
                let items: Vec<String> = redis_rs::cmd("LRANGE")
                    .arg(key)
                    .arg(0)
                    .arg(-1)
                    .query_async(&mut conn)
                    .await?;
                lists.insert(key.clone(), json!(items));
            }
            "zset" => {
                let items: Vec<(String, f64)> = redis_rs::cmd("ZRANGE")
                    .arg(key)
                    .arg(0)
                    .arg(-1)
                    .arg("WITHSCORES")
                    .query_async(&mut conn)
                    .await?;
                sets.insert(key.clone(), json!(items));
            }
            _ => other.push(json!({"key": key, "type": kind})),
        }
    }
    let processes: Vec<String> = redis_rs::cmd("SMEMBERS")
        .arg("processes")
        .query_async(&mut conn)
        .await?;
    let inprogress: Vec<String> = redis_rs::cmd("SMEMBERS")
        .arg("inprogress")
        .query_async(&mut conn)
        .await?;
    let (processed, failed): (Option<u64>, Option<u64>) = redis_rs::pipe()
        .cmd("GET")
        .arg("stat:processed")
        .cmd("GET")
        .arg("stat:failed")
        .query_async(&mut conn)
        .await?;
    Ok(json!({
        "keys": keys.len(), "lists": lists, "zsets": sets, "other": other,
        "processes": processes, "inprogress": inprogress,
        "processed": processed.unwrap_or(0), "failed": failed.unwrap_or(0),
    }))
}

/// How long the drain waits for the cleanup to prune processes that died: up to 60 s for the
/// heartbeat key to expire (`hench::keys::PROCESS_TTL`), up to 60 s more for the cleanup's lock
/// (the same TTL), then a scheduler's next poll, whose wait was drawn from the processes it
/// counted at its previous poll (a killed one counts until its key expires): from 10 processes
/// on, up to 2 × 5 s each, so 120 s for the 12 that the crash loop of `stale-sched` can reach.
/// It only matters when hench fails to prune: otherwise the wait ends at the prune.
const PRUNE_WAIT_MAX: Duration = Duration::from_secs(4 * 60);

/// Members of the `processes` set that are not running workers (by the pid in the identity,
/// `host:pid:nonce`): processes that died without removing themselves. `None` if Redis could
/// not be read.
async fn dead_members(ctx: &Ctx, port: u16) -> Option<Vec<String>> {
    let mut conn = admin(port).await.ok()?;
    let members: Vec<String> = redis_rs::cmd("SMEMBERS")
        .arg("processes")
        .query_async(&mut conn)
        .await
        .ok()?;
    let running: HashSet<u32> = ctx.live.lock().unwrap().values().copied().collect();
    Some(
        members
            .into_iter()
            .filter(|identity| {
                identity
                    .rsplit(':')
                    .nth(1)
                    .and_then(|pid| pid.parse::<u32>().ok())
                    .is_none_or(|pid| !running.contains(&pid))
            })
            .collect(),
    )
}

// ---------------------------------------------------------------------------------------------
// The scenario

pub async fn run(opts: RunOptions) -> anyhow::Result<()> {
    std::fs::create_dir_all(&opts.dir)?;
    let redis_dir = opts.dir.join("redis");
    std::fs::create_dir_all(&redis_dir)?;
    std::fs::write(opts.dir.join("options.txt"), format!("{opts:#?}\n"))?;
    let chaos_log = Arc::new(JsonLog::open(&opts.dir.join("chaos.jsonl"))?);
    let pushes = Arc::new(JsonLog::open(&opts.dir.join("pushes.jsonl"))?);
    let batches = Arc::new(JsonLog::open(&opts.dir.join("batches.jsonl"))?);
    let uniques = Arc::new(JsonLog::open(&opts.dir.join("unique.jsonl"))?);
    let samples = Arc::new(JsonLog::open(&opts.dir.join("samples.jsonl"))?);

    let mut redis = RedisServer {
        child: None,
        port: opts.port,
        dir: redis_dir,
        persist: opts.persist.clone(),
    };
    redis.start().await?;
    let proxy = if opts.proxy {
        Some(Proxy::start(opts.proxy_port, opts.port).await?)
    } else {
        None
    };
    let worker_port = if opts.proxy {
        opts.proxy_port
    } else {
        opts.port
    };
    let ctx = Arc::new(Ctx {
        worker_url: format!("redis://127.0.0.1:{worker_port}/"),
        opts: opts.clone(),
        chaos: chaos_log,
        live: std::sync::Mutex::new(HashMap::new()),
        expect_exit: std::sync::Mutex::new(HashSet::new()),
        redis: Mutex::new(redis),
        proxy,
        stop_chaos: AtomicBool::new(false),
    });
    ctx.log(
        json!({"e": "scenario", "name": opts.scenario, "duration": opts.duration,
        "workers": opts.workers, "fetch": opts.fetch, "persist": opts.persist,
        "proxy": opts.proxy, "actions": opts.actions}),
    );

    // The dashboard's JSON API (hench web --api), straight on Redis.
    let mut web = match &opts.hench_bin {
        Some(bin) => {
            let log = std::fs::File::create(opts.dir.join("web.log"))?;
            Some(
                Command::new(bin)
                    .arg("--redis-url")
                    .arg(format!("redis://127.0.0.1:{}/", opts.port))
                    .arg("web")
                    .arg("--bind")
                    .arg(format!("127.0.0.1:{}", opts.web_port))
                    .arg("--api")
                    .env("HENCH_LOG", "warn")
                    .stdout(Stdio::null())
                    .stderr(Stdio::from(log))
                    .kill_on_drop(true)
                    .spawn()?,
            )
        }
        None => None,
    };

    if opts.hench_bin.is_some() {
        // Wait for the dashboard server before sampling it.
        for _ in 0..100 {
            if http_get_json(opts.web_port, "/healthz").await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    // Workers.
    let mut senders = Vec::new();
    let mut slot_tasks = Vec::new();
    for slot in 0..opts.workers {
        let (sender, receiver) = mpsc::channel(4);
        senders.push(sender);
        slot_tasks.push(tokio::spawn(slot_task(ctx.clone(), slot, receiver)));
    }

    let stop_sampling = Arc::new(AtomicBool::new(false));
    let sampling = tokio::spawn(sampler(ctx.clone(), samples.clone(), stop_sampling.clone()));

    // Pusher: straight on Redis (its pushes are the bookkeeping), default timeouts.
    let push_pool = RedisPool::lazy(RedisConfig::new(format!(
        "redis://127.0.0.1:{}/",
        opts.port
    )))?;
    let client = Client::new(push_pool.clone());
    let deadline = Instant::now() + Duration::from_secs(opts.duration);
    let pushing = tokio::spawn(pusher(
        ctx.clone(),
        client,
        pushes.clone(),
        batches.clone(),
        uniques.clone(),
        deadline,
    ));
    let chaos_deadline = match opts.chaos_until {
        Some(secs) => Instant::now() + Duration::from_secs(secs.min(opts.duration)),
        None => deadline,
    };
    let chaos_task = tokio::spawn(chaos(ctx.clone(), senders.clone(), chaos_deadline));

    let (pushed, bids) = pushing.await.unwrap_or_default();
    ctx.stop_chaos.store(true, Ordering::Relaxed);
    let _ = chaos_task.await;
    ctx.log(json!({"e": "load_end", "nonces": pushed, "batches": bids.len()}));

    // Make sure nothing is left faulty.
    if let Some(proxy) = &ctx.proxy {
        proxy.set(Mode::Pass);
    }
    {
        let mut redis = ctx.redis.lock().await;
        if redis.child.is_none() {
            let _ = redis.start().await;
        }
    }
    if let Ok(mut conn) = admin(opts.port).await {
        let _: redis_rs::RedisResult<()> = redis_rs::cmd("CLIENT")
            .arg("UNPAUSE")
            .query_async(&mut conn)
            .await;
    }

    // The schedule stops ticking, so that the drain can end.
    if opts.pro {
        let paused = crate::pro::pause_schedule(push_pool.clone()).await;
        ctx.log(json!({"e": "periodic_pause", "result": paused}));
    }

    // Drain: wait until Redis holds no job anywhere (suspects of crash loops included: they come
    // back one per orphan check) and nobody is busy (and, with the pro load, no batch event
    // waits in the outbox), for 3 checks in a row.
    let drain_started = Instant::now();
    let mut quiet_checks = 0;
    let mut last = Value::Null;
    while drain_started.elapsed() < Duration::from_secs(opts.drain) {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let Ok(counts) = direct_counts(opts.port).await else {
            continue;
        };
        let idle = [
            "enqueued",
            "schedule",
            "retry",
            "inprogress",
            "busy",
            "recovery_suspects",
        ]
        .iter()
        .all(|field| counts[*field].as_u64() == Some(0))
            && (!opts.pro || counts["batch_outbox"].as_u64() == Some(0));
        last = counts;
        if idle {
            quiet_checks += 1;
            if quiet_checks >= 3 {
                break;
            }
        } else {
            quiet_checks = 0;
        }
    }
    let drained = quiet_checks >= 3;
    let drain_ms = drain_started.elapsed().as_millis() as u64;

    // Then wait until the `processes` set lists the running workers only. A worker killed with
    // `kill -9` stays listed until a scheduler's cleanup prunes it, which hench bounds but does
    // not hurry: its heartbeat key expires up to 60 s after the kill, and the cleanup runs once
    // per 60 s lock across the cluster, on a scheduler's next poll. Stopping the workers before
    // that left a dead member that the check reported as never pruned, when the kill came less
    // than about two minutes before the drain ended. What is still listed when the wait gives up
    // is reported by the check.
    let prune_started = Instant::now();
    let mut dead_listed = dead_members(&ctx, opts.port).await;
    while dead_listed.as_ref().is_none_or(|dead| !dead.is_empty())
        && prune_started.elapsed() < PRUNE_WAIT_MAX
    {
        tokio::time::sleep(Duration::from_secs(2)).await;
        dead_listed = dead_members(&ctx, opts.port).await;
    }
    let mut drain_record = json!({"e": "drain_end", "drained": drained, "drain_ms": drain_ms,
        "prune_wait_ms": prune_started.elapsed().as_millis() as u64,
        "dead_listed": dead_listed});
    for field in [
        "enqueued",
        "schedule",
        "retry",
        "inprogress",
        "busy",
        "dead",
        "batch_outbox",
        "recovery_suspects",
    ] {
        drain_record[field] = last[field].clone();
    }
    ctx.log(drain_record);

    // Stop every worker gracefully.
    let mut acks = Vec::new();
    for sender in &senders {
        let (ack, done) = oneshot::channel();
        if sender.send(SlotCmd::Final(ack)).await.is_ok() {
            acks.push(done);
        }
    }
    for done in acks {
        let _ = done.await;
    }
    for task in slot_tasks {
        let _ = task.await;
    }
    // Let the last heartbeat keys and process entries settle, then sample once more.
    tokio::time::sleep(Duration::from_secs(1)).await;
    stop_sampling.store(true, Ordering::Relaxed);
    let _ = sampling.await;
    let after_stop = direct_counts(opts.port)
        .await
        .unwrap_or_else(|e| json!({"error": e.to_string()}));
    let web_after = if opts.hench_bin.is_some() {
        json!({
            "stats": http_get_json(opts.web_port, "/api/v1/stats").await.unwrap_or(Value::Null),
            "processes": http_get_json(opts.web_port, "/api/v1/processes").await.unwrap_or(Value::Null),
            "queues": http_get_json(opts.web_port, "/api/v1/queues").await.unwrap_or(Value::Null),
        })
    } else {
        Value::Null
    };
    let final_state = snapshot(opts.port)
        .await
        .unwrap_or_else(|e| json!({"error": e.to_string()}));
    if opts.pro {
        let pro = crate::pro::snapshot(opts.port, &bids)
            .await
            .unwrap_or_else(|e| json!({"error": e.to_string()}));
        std::fs::write(opts.dir.join("pro.json"), serde_json::to_vec_pretty(&pro)?)?;
    }
    std::fs::write(
        opts.dir.join("final.json"),
        serde_json::to_vec_pretty(&json!({
            "after_stop": after_stop, "web_after": web_after, "redis": final_state,
            "drained": drained,
        }))?,
    )?;
    ctx.log(json!({"e": "scenario_end"}));

    if let Some(web) = web.as_mut() {
        signal(web.id().unwrap_or(0), libc::SIGTERM);
        let _ = tokio::time::timeout(Duration::from_secs(5), web.wait()).await;
    }
    {
        let mut redis = ctx.redis.lock().await;
        if let Ok(mut conn) = admin(opts.port).await {
            let _: redis_rs::RedisResult<()> = redis_rs::cmd("SHUTDOWN")
                .arg("NOSAVE")
                .query_async(&mut conn)
                .await;
        }
        redis.wait_exit().await;
    }
    Ok(())
}
