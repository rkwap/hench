//! Helpers shared by the worker-runtime integration tests (fetch, processor, retry,
//! scheduled, launcher, e2e, chaos).
#![allow(dead_code)]

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use hench::prelude::*;
use hench::redis_rs;
use serde_json::Value;
use tokio::sync::Notify;

/// Default wait for asynchronous effects in tests.
pub const WAIT: Duration = Duration::from_secs(10);

/// Polls `condition` every 10 ms until it holds; panics with `what` after `timeout`.
pub async fn wait_for<F, Fut>(what: &str, timeout: Duration, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    while !condition().await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out after {timeout:?} waiting for: {what}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Runs a Redis command on the shared connection.
pub async fn cmd<T: redis_rs::FromRedisValue>(pool: &RedisPool, command: &redis_rs::Cmd) -> T {
    let mut conn = pool.conn().await.expect("redis connection");
    command.query_async(&mut conn).await.expect("redis command")
}

/// A script that keeps Redis busy, serving no other client, for `ARGV[1]` milliseconds: a
/// stalled server (a slow fork, a long script). Commands sent meanwhile wait, and run once it
/// ends. The loop ends on `TIME`, or on the server's CPU time where scripts can read it
/// (`os.clock`, Redis 7 and Valkey): Valkey keeps a script's `TIME` still, so `TIME` alone would
/// end it only at the busy threshold (5 s), and every other client would get `BUSY` replies.
pub const BUSY_LOOP: &str = r"
local ms = tonumber(ARGV[1])
local ok, clock = pcall(function() return os.clock end)
if not ok or type(clock) ~= 'function' then clock = nil end
local cpu = clock and clock()
local t = redis.call('TIME')
local start = tonumber(t[1]) * 1000000 + tonumber(t[2])
while true do
  t = redis.call('TIME')
  if tonumber(t[1]) * 1000000 + tonumber(t[2]) - start >= ms * 1000 then return 1 end
  if clock and (clock() - cpu) * 1000 >= ms then return 1 end
end
";

/// Lets a script run for 30 s before Redis answers other clients `BUSY` (5 s by default), so
/// that a [`BUSY_LOOP`] slowed down by a loaded machine (its CPU time lags behind the clock)
/// still holds the commands sent meanwhile, which then run, rather than refusing them.
pub async fn allow_long_scripts(pool: &RedisPool) {
    let _: () = cmd(
        pool,
        redis_rs::cmd("CONFIG")
            .arg("SET")
            .arg("lua-time-limit")
            .arg(30_000),
    )
    .await;
}

/// Builds a Redis command: `redis_cmd!("LLEN", key)`.
#[macro_export]
macro_rules! redis_cmd {
    ($name:expr $(, $arg:expr)* $(,)?) => {{
        #[allow(unused_mut)]
        let mut command = ::hench::redis_rs::cmd($name);
        $( command.arg($arg); )*
        command
    }};
}

/// `LLEN` of a key.
pub async fn llen(pool: &RedisPool, key: &str) -> usize {
    cmd(pool, &redis_cmd!("LLEN", &key)).await
}

/// Number of jobs in a queue.
pub async fn queue_size(pool: &RedisPool, queue: &str) -> usize {
    llen(pool, &hench::keys::queue(queue)).await
}

/// Raw jobs of a list, head (newest) first.
pub async fn lrange(pool: &RedisPool, key: &str) -> Vec<Vec<u8>> {
    cmd(pool, &redis_cmd!("LRANGE", &key, &0, &-1)).await
}

/// Jobs of a queue, next to run first.
pub async fn queue_jobs(pool: &RedisPool, queue: &str) -> Vec<JobPayload> {
    let mut jobs: Vec<JobPayload> = lrange(pool, &hench::keys::queue(queue))
        .await
        .iter()
        .map(|raw| JobPayload::from_slice(raw).expect("valid job"))
        .collect();
    jobs.reverse();
    jobs
}

/// `ZCARD` of a key.
pub async fn zcard(pool: &RedisPool, key: &str) -> usize {
    cmd(pool, &redis_cmd!("ZCARD", &key)).await
}

/// Members of a sorted set with their scores, lowest score first.
pub async fn zrange(pool: &RedisPool, key: &str) -> Vec<(Vec<u8>, f64)> {
    cmd(pool, &redis_cmd!("ZRANGE", &key, &0, &-1, &"WITHSCORES")).await
}

/// Jobs of a sorted set (retry, schedule, dead) with their scores, lowest score first.
pub async fn sorted_jobs(pool: &RedisPool, key: &str) -> Vec<(JobPayload, f64)> {
    zrange(pool, key)
        .await
        .into_iter()
        .map(|(raw, score)| (JobPayload::from_slice(&raw).expect("valid job"), score))
        .collect()
}

/// Adds a job to a sorted set.
pub async fn zadd(pool: &RedisPool, key: &str, score: f64, job: &JobPayload) {
    let json = job.to_json().expect("job JSON");
    let () = cmd(pool, &redis_cmd!("ZADD", &key, &score, &json)).await;
}

/// `SMEMBERS` of a key, sorted.
pub async fn smembers(pool: &RedisPool, key: &str) -> Vec<String> {
    let mut members: Vec<String> = cmd(pool, &redis_cmd!("SMEMBERS", &key)).await;
    members.sort();
    members
}

/// `HGETALL` of a key.
pub async fn hgetall(pool: &RedisPool, key: &str) -> HashMap<String, String> {
    cmd(pool, &redis_cmd!("HGETALL", &key)).await
}

/// `EXISTS` of a key.
pub async fn exists(pool: &RedisPool, key: &str) -> bool {
    cmd(pool, &redis_cmd!("EXISTS", &key)).await
}

/// `PTTL` of a key in milliseconds.
pub async fn pttl(pool: &RedisPool, key: &str) -> i64 {
    cmd(pool, &redis_cmd!("PTTL", &key)).await
}

/// `GET` of an integer key (0 if missing).
pub async fn get_int(pool: &RedisPool, key: &str) -> i64 {
    let value: Option<i64> = cmd(pool, &redis_cmd!("GET", &key)).await;
    value.unwrap_or(0)
}

/// Pushes a raw value to the front (next to run) of a queue, like a foreign client would.
pub async fn push_raw(pool: &RedisPool, queue: &str, job: &[u8]) {
    let () = cmd(pool, &redis_cmd!("LPUSH", &hench::keys::queue(queue), &job)).await;
}

/// Deletes a key.
pub async fn del(pool: &RedisPool, key: &str) {
    let _: i64 = cmd(pool, &redis_cmd!("DEL", &key)).await;
}

/// A payload for `class` with `args` in `queue`.
pub fn payload(class: &str, args: Vec<Value>, queue: &str) -> JobPayload {
    let mut job = JobPayload::new(class, args);
    job.queue = queue.to_owned();
    job
}

/// Records the jobs that ran, by name.
#[derive(Default)]
pub struct Runs {
    log: Mutex<Vec<String>>,
    notify: Notify,
}

impl Runs {
    /// Records a run.
    pub fn record(&self, entry: impl Into<String>) {
        self.log.lock().unwrap().push(entry.into());
        self.notify.notify_waiters();
    }

    /// Every recorded run.
    pub fn entries(&self) -> Vec<String> {
        self.log.lock().unwrap().clone()
    }

    /// Number of recorded runs.
    pub fn len(&self) -> usize {
        self.log.lock().unwrap().len()
    }

    /// Number of runs recorded as `entry`.
    pub fn count(&self, entry: &str) -> usize {
        self.log
            .lock()
            .unwrap()
            .iter()
            .filter(|e| *e == entry)
            .count()
    }

    /// Waits until `count` runs are recorded.
    pub async fn wait_for(&self, count: usize) {
        wait_for(&format!("{count} job runs"), WAIT, || async {
            self.len() >= count
        })
        .await;
    }
}

/// A gate jobs wait on until the test opens it.
#[derive(Default)]
pub struct Gate {
    open: std::sync::atomic::AtomicBool,
    notify: Notify,
    entered: AtomicUsize,
}

impl Gate {
    /// Lets every waiting (and future) job through.
    pub fn open(&self) {
        self.open.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    /// Waits until the gate opens.
    pub async fn pass(&self) {
        self.entered.fetch_add(1, Ordering::SeqCst);
        loop {
            let notified = self.notify.notified();
            if self.open.load(Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }

    /// How many jobs reached the gate.
    pub fn entered(&self) -> usize {
        self.entered.load(Ordering::SeqCst)
    }
}

/// `(context, error class, jid)` of an error reported to the error handlers.
pub type Reported = (String, String, Option<String>);

/// Records every error reported to the error handlers.
#[derive(Clone, Default)]
pub struct Errors(Arc<Mutex<Vec<Reported>>>);

impl Errors {
    /// An error handler recording into this list.
    pub fn handler(&self) -> impl Fn(&JobError, &hench::ErrorContext<'_>) + Send + Sync + 'static {
        let errors = self.0.clone();
        move |error, ctx| {
            errors.lock().unwrap().push((
                ctx.context.to_owned(),
                error.class().to_owned(),
                ctx.job.map(|job| job.jid.clone()),
            ));
        }
    }

    /// The recorded errors.
    pub fn all(&self) -> Vec<Reported> {
        self.0.lock().unwrap().clone()
    }

    /// Errors reported with `context`.
    pub fn with_context(&self, context: &str) -> Vec<Reported> {
        self.all()
            .into_iter()
            .filter(|(c, _, _)| c == context)
            .collect()
    }
}

/// Records the jobs given to the death handlers.
#[derive(Clone, Default)]
pub struct Deaths(Arc<Mutex<Vec<(JobPayload, String)>>>);

impl Deaths {
    /// A death handler recording into this list.
    pub fn handler(&self) -> impl Fn(&JobPayload, &JobError) + Send + Sync + 'static {
        let deaths = self.0.clone();
        move |job, error| {
            deaths.lock().unwrap().push((job.clone(), error.message()));
        }
    }

    /// The recorded deaths.
    pub fn all(&self) -> Vec<(JobPayload, String)> {
        self.0.lock().unwrap().clone()
    }
}

/// Records lifecycle events in order.
#[derive(Clone, Default)]
pub struct Events(Arc<Mutex<Vec<&'static str>>>);

impl Events {
    /// Registers a callback for every lifecycle event on `builder`.
    pub fn register(&self, mut builder: hench::HenchBuilder) -> hench::HenchBuilder {
        for event in hench::LifecycleEvent::ALL {
            let events = self.0.clone();
            builder = builder.on(event, move || {
                let events = events.clone();
                async move { events.lock().unwrap().push(event.name()) }
            });
        }
        builder
    }

    /// The recorded events.
    pub fn all(&self) -> Vec<&'static str> {
        self.0.lock().unwrap().clone()
    }

    /// How many times `event` fired.
    pub fn count(&self, event: &str) -> usize {
        self.all().iter().filter(|e| **e == event).count()
    }

    /// The recorded events without the heartbeat ones.
    pub fn milestones(&self) -> Vec<&'static str> {
        self.all()
            .into_iter()
            .filter(|e| *e != "beat" && *e != "heartbeat")
            .collect()
    }
}

/// Records its first argument in the `Runs` data.
#[derive(Default)]
pub struct Echo;

impl Job for Echo {
    const NAME: &'static str = "Echo";
    type Args = (String,);

    async fn perform(&self, ctx: &JobContext, (value,): (String,)) -> Result<(), JobError> {
        ctx.data::<Runs>()?.record(value);
        Ok(())
    }
}

/// Always fails with class `RuntimeError` and message `kerblammo!`.
#[derive(Default)]
pub struct Fail;

impl Job for Fail {
    const NAME: &'static str = "Fail";
    type Args = Vec<Value>;

    async fn perform(&self, _ctx: &JobContext, _args: Vec<Value>) -> Result<(), JobError> {
        Err(JobError::new("RuntimeError", "kerblammo!"))
    }
}

/// Waits at the `Gate` (shared data), records `entered:<arg>` and `done:<arg>` in `Runs`.
#[derive(Default)]
pub struct Blocked;

impl Job for Blocked {
    const NAME: &'static str = "Blocked";
    type Args = (String,);

    async fn perform(&self, ctx: &JobContext, (value,): (String,)) -> Result<(), JobError> {
        let runs = ctx.data_arc::<Runs>()?;
        runs.record(format!("entered:{value}"));
        ctx.data::<Gate>()?.pass().await;
        runs.record(format!("done:{value}"));
        Ok(())
    }
}

/// Sleeps far longer than any test, ignoring the shutdown signal.
#[derive(Default)]
pub struct Stubborn;

impl Job for Stubborn {
    const NAME: &'static str = "Stubborn";
    type Args = (String,);

    async fn perform(&self, ctx: &JobContext, (value,): (String,)) -> Result<(), JobError> {
        ctx.data::<Runs>()?.record(format!("entered:{value}"));
        tokio::time::sleep(Duration::from_secs(3600)).await;
        Ok(())
    }
}

/// Initializes logging once per test binary when `HENCH_TEST_LOG` is set.
pub fn init_logging() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        if let Ok(filter) = std::env::var("HENCH_TEST_LOG") {
            let _ = tracing_subscriber::fmt()
                .with_env_filter(tracing_subscriber::EnvFilter::new(filter))
                .with_test_writer()
                .try_init();
        }
    });
}

/// A TCP proxy in front of a test Redis (Unix socket) that can simulate a network partition:
/// while partitioned it stops forwarding (bytes wait in the socket buffers, as with a real
/// partition, and flow again once it heals); new connections are accepted but stay silent.
pub struct Proxy {
    addr: std::net::SocketAddr,
    blackhole: Arc<std::sync::atomic::AtomicBool>,
    accept: tokio::task::JoinHandle<()>,
}

/// Writes all of `data` to a socket using only readiness-based I/O.
macro_rules! write_all {
    ($stream:expr, $data:expr) => {{
        let mut data: &[u8] = $data;
        let mut result = Ok(());
        while !data.is_empty() {
            if let Err(error) = $stream.writable().await {
                result = Err(error);
                break;
            }
            match $stream.try_write(data) {
                Ok(written) => data = &data[written..],
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => {
                    result = Err(error);
                    break;
                }
            }
        }
        result
    }};
}

impl Proxy {
    /// Starts a proxy to the Unix socket `target`.
    pub async fn start(target: &std::path::Path) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let blackhole = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let target = target.to_owned();
        let flag = blackhole.clone();
        let accept = tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let Ok(server) = tokio::net::UnixStream::connect(&target).await else {
                    continue;
                };
                let flag = flag.clone();
                tokio::spawn(async move {
                    let mut buffer = vec![0_u8; 16 * 1024];
                    loop {
                        while flag.load(Ordering::SeqCst) {
                            tokio::time::sleep(Duration::from_millis(5)).await;
                        }
                        tokio::select! {
                            ready = client.readable() => {
                                if ready.is_err() {
                                    return;
                                }
                                match client.try_read(&mut buffer) {
                                    Ok(0) => return,
                                    Ok(n) => {
                                        if write_all!(server, &buffer[..n]).is_err() {
                                            return;
                                        }
                                    }
                                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                                    Err(_) => return,
                                }
                            }
                            ready = server.readable() => {
                                if ready.is_err() {
                                    return;
                                }
                                match server.try_read(&mut buffer) {
                                    Ok(0) => return,
                                    Ok(n) => {
                                        if write_all!(client, &buffer[..n]).is_err() {
                                            return;
                                        }
                                    }
                                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                                    Err(_) => return,
                                }
                            }
                        }
                    }
                });
            }
        });
        Self {
            addr,
            blackhole,
            accept,
        }
    }

    /// The proxy's URL.
    pub fn url(&self) -> String {
        format!("redis://{}", self.addr)
    }

    /// Starts (`true`) or heals (`false`) a partition.
    pub fn set_partitioned(&self, blackhole: bool) {
        self.blackhole.store(blackhole, Ordering::SeqCst);
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

/// A TCP proxy in front of a test Redis (Unix socket) that loses one reply: once armed, it
/// closes the client's connection instead of delivering the first reply containing `marker`.
/// Redis ran the command; the client sees a reset connection (and `with_retry` may send the
/// command again).
pub struct ReplyDropper {
    addr: std::net::SocketAddr,
    armed: Arc<std::sync::atomic::AtomicBool>,
    dropped: Arc<AtomicUsize>,
    accept: tokio::task::JoinHandle<()>,
}

impl ReplyDropper {
    /// Starts a proxy to the Unix socket `target`, armed.
    pub async fn start(target: &std::path::Path, marker: &'static [u8]) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let armed = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let dropped = Arc::new(AtomicUsize::new(0));
        let target = target.to_owned();
        let (flag, count) = (armed.clone(), dropped.clone());
        let accept = tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let Ok(server) = tokio::net::UnixStream::connect(&target).await else {
                    continue;
                };
                let (flag, count) = (flag.clone(), count.clone());
                tokio::spawn(async move {
                    let mut buffer = vec![0_u8; 64 * 1024];
                    loop {
                        tokio::select! {
                            ready = client.readable() => {
                                if ready.is_err() {
                                    return;
                                }
                                match client.try_read(&mut buffer) {
                                    Ok(0) => return,
                                    Ok(n) => {
                                        if write_all!(server, &buffer[..n]).is_err() {
                                            return;
                                        }
                                    }
                                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                                    Err(_) => return,
                                }
                            }
                            ready = server.readable() => {
                                if ready.is_err() {
                                    return;
                                }
                                match server.try_read(&mut buffer) {
                                    Ok(0) => return,
                                    Ok(n) => {
                                        let chunk = &buffer[..n];
                                        if chunk.windows(marker.len()).any(|w| w == marker)
                                            && flag.swap(false, Ordering::SeqCst)
                                        {
                                            count.fetch_add(1, Ordering::SeqCst);
                                            // Both sockets are dropped: the client sees a reset.
                                            return;
                                        }
                                        if write_all!(client, chunk).is_err() {
                                            return;
                                        }
                                    }
                                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                                    Err(_) => return,
                                }
                            }
                        }
                    }
                });
            }
        });
        Self {
            addr,
            armed,
            dropped,
            accept,
        }
    }

    /// The proxy's URL.
    pub fn url(&self) -> String {
        format!("redis://{}", self.addr)
    }

    /// Replies dropped so far.
    pub fn dropped(&self) -> usize {
        self.dropped.load(Ordering::SeqCst)
    }

    /// Arms the proxy again: the next reply containing the marker is dropped.
    pub fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }
}

impl Drop for ReplyDropper {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

/// A TCP proxy in front of test Redis servers (Unix sockets) whose target can be switched, like
/// a DNS name or a virtual IP moved to a new primary after a failover: new connections go to
/// the new target, existing ones stay connected to the server they reached.
pub struct SwitchProxy {
    addr: std::net::SocketAddr,
    target: Arc<Mutex<std::path::PathBuf>>,
    accept: tokio::task::JoinHandle<()>,
}

impl SwitchProxy {
    /// Starts a proxy to the Unix socket `target`.
    pub async fn start(target: &std::path::Path) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let target = Arc::new(Mutex::new(target.to_owned()));
        let current = target.clone();
        let accept = tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let path = current.lock().unwrap().clone();
                let Ok(server) = tokio::net::UnixStream::connect(&path).await else {
                    continue;
                };
                tokio::spawn(async move {
                    let mut buffer = vec![0_u8; 64 * 1024];
                    loop {
                        tokio::select! {
                            ready = client.readable() => {
                                if ready.is_err() {
                                    return;
                                }
                                match client.try_read(&mut buffer) {
                                    Ok(0) => return,
                                    Ok(n) => {
                                        if write_all!(server, &buffer[..n]).is_err() {
                                            return;
                                        }
                                    }
                                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                                    Err(_) => return,
                                }
                            }
                            ready = server.readable() => {
                                if ready.is_err() {
                                    return;
                                }
                                match server.try_read(&mut buffer) {
                                    Ok(0) => return,
                                    Ok(n) => {
                                        if write_all!(client, &buffer[..n]).is_err() {
                                            return;
                                        }
                                    }
                                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                                    Err(_) => return,
                                }
                            }
                        }
                    }
                });
            }
        });
        Self {
            addr,
            target,
            accept,
        }
    }

    /// The proxy's URL.
    pub fn url(&self) -> String {
        format!("redis://{}", self.addr)
    }

    /// Sends the connections opened from now on to `target`.
    pub fn switch(&self, target: &std::path::Path) {
        *self.target.lock().unwrap() = target.to_owned();
    }
}

impl Drop for SwitchProxy {
    fn drop(&mut self) {
        self.accept.abort();
    }
}
