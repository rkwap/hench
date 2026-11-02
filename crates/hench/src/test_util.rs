//! Test support (feature `test-util`): [`TestRedis`] runs a private `redis-server` for one
//! test, so tests are isolated and can run in parallel.
//!
//! ```no_run
//! use hench::test_util::TestRedis;
//!
//! # async fn demo() {
//! let redis = TestRedis::start().await;          // panics with a clear message on failure
//! let client = redis.client().await;              // a Client connected to it
//! let hench = redis.builder().await.build().await.unwrap(); // short intervals for tests
//! # let _ = (client, hench);
//! # }
//! ```
//!
//! The server listens only on a Unix socket in a temporary directory, persists nothing, and
//! is killed when the `TestRedis` is dropped. A small watchdog (`sh`) kills it, and removes
//! its directory, if the test process dies without dropping it (killed, aborted): it waits
//! for a word on a pipe from the test process, whose end the system closes when the process
//! dies. The `redis-server` binary is taken from the `HENCH_REDIS_SERVER` environment
//! variable, then `PATH`, then common install locations.

use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use crate::client::Client;
use crate::config::{Hench, HenchBuilder};
use crate::error::{Error, Result};
use crate::redis::{RedisConfig, RedisPool};

/// Environment variable that names the `redis-server` binary to use.
pub const REDIS_SERVER_ENV: &str = "HENCH_REDIS_SERVER";

/// How long to wait for a freshly spawned server to answer `PING`.
const STARTUP_TIMEOUT: Duration = Duration::from_secs(10);

/// A private `redis-server` for one test. Killed (and its files removed) on drop.
pub struct TestRedis {
    server: PathBuf,
    dir: tempfile::TempDir,
    socket: PathBuf,
    /// Extra `redis-server` arguments, see [`TestRedis::start_with_args`].
    args: Vec<String>,
    child: Option<Child>,
    /// Kills the server and removes its directory if this process dies first.
    watchdog: Option<Child>,
}

impl TestRedis {
    /// Starts a server and waits until it answers.
    ///
    /// # Panics
    ///
    /// If `redis-server` cannot be found or does not start; the message explains why.
    pub async fn start() -> Self {
        match Self::try_start().await {
            Ok(redis) => redis,
            Err(error) => panic!("could not start a test redis-server: {error}"),
        }
    }

    /// Starts a server and waits until it answers.
    pub async fn try_start() -> Result<Self> {
        Self::try_start_with_args(&[]).await
    }

    /// Starts a server with extra `redis-server` arguments (for example
    /// `["--rename-command", "INFO", ""]`, as some managed services do), kept on restarts.
    ///
    /// # Panics
    ///
    /// If `redis-server` cannot be found or does not start; the message explains why.
    pub async fn start_with_args(args: &[&str]) -> Self {
        match Self::try_start_with_args(args).await {
            Ok(redis) => redis,
            Err(error) => panic!("could not start a test redis-server: {error}"),
        }
    }

    /// Starts a server with extra `redis-server` arguments and waits until it answers.
    pub async fn try_start_with_args(args: &[&str]) -> Result<Self> {
        let server = find_redis_server()?;
        let dir = socket_dir()?;
        let socket = dir.path().join("redis.sock");
        let mut redis = Self {
            server,
            dir,
            socket,
            args: args.iter().map(|arg| (*arg).to_owned()).collect(),
            child: None,
            watchdog: None,
        };
        redis.spawn().await?;
        Ok(redis)
    }

    /// The server's URL (`redis+unix://<dir>/redis.sock`).
    pub fn url(&self) -> String {
        format!("redis+unix://{}", self.socket.display())
    }

    /// Path of the server's Unix socket.
    pub fn socket_path(&self) -> &Path {
        &self.socket
    }

    /// A Redis configuration for this server with test-friendly timeouts (quick
    /// reconnection after [`TestRedis::restart`]).
    pub fn config(&self) -> RedisConfig {
        RedisConfig::new(self.url())
            .connection_timeout(Duration::from_secs(1))
            .response_timeout(Duration::from_secs(5))
            .reconnect_retries(5)
            .reconnect_delays(Duration::from_millis(10), Duration::from_millis(200))
    }

    /// A pool connected to this server.
    pub async fn try_pool(&self) -> Result<RedisPool> {
        RedisPool::from_config(self.config()).await
    }

    /// A pool connected to this server.
    ///
    /// # Panics
    ///
    /// If the server cannot be reached.
    pub async fn pool(&self) -> RedisPool {
        match self.try_pool().await {
            Ok(pool) => pool,
            Err(error) => panic!("could not connect to the test redis-server: {error}"),
        }
    }

    /// A client connected to this server.
    ///
    /// # Panics
    ///
    /// If the server cannot be reached.
    pub async fn client(&self) -> Client {
        Client::new(self.pool().await)
    }

    /// A builder connected to this server, with short intervals suitable for tests:
    /// heartbeat, scheduler poll and fetch timeout of 100 ms, no initial scheduler wait,
    /// orphan checks every 500 ms with a 1 s orphan grace, a 5 s shutdown timeout and no
    /// systemd notifications; for the features, a periodic poll of 100 ms with a 2 s lease
    /// and a registration check every 200 ms, and batch maintenance every 100 ms with a 100 ms
    /// dispatch grace. Override as needed.
    ///
    /// # Panics
    ///
    /// If the server cannot be reached.
    pub async fn builder(&self) -> HenchBuilder {
        Hench::builder()
            .redis(self.pool().await)
            .heartbeat_interval(Duration::from_millis(100))
            .poll_interval_average(Duration::from_millis(100))
            .average_scheduled_poll_interval(Duration::from_millis(100))
            .initial_scheduler_wait(Duration::ZERO)
            .initial_scheduler_jitter(Duration::ZERO)
            .fetch_timeout(Duration::from_millis(100))
            .orphan_check_interval(Duration::from_millis(500))
            .orphan_grace(Duration::from_secs(1))
            .timeout(Duration::from_secs(5))
            .periodic_poll_interval(Duration::from_millis(100))
            .periodic_lease(Duration::from_secs(2))
            .periodic_check_interval(Duration::from_millis(200))
            .batch_maintenance_interval(Duration::from_millis(100))
            .batch_dispatch_grace(Duration::from_millis(100))
            // Tests never notify a service manager the test runner may run under.
            .systemd(false)
    }

    /// The keys matching any of `patterns` (`SCAN` `MATCH` globs, such as `"*"` or
    /// `"batch:*"`) that have no expiry, sorted: what a test checks against the features' TTL
    /// policy (every per-entity key expires; only configuration and index keys may not).
    ///
    /// # Panics
    ///
    /// If the server cannot be reached.
    pub async fn keys_without_ttl(&self, patterns: &[&str]) -> Vec<String> {
        let pool = self.pool().await;
        let mut conn = match pool.conn().await {
            Ok(conn) => conn,
            Err(error) => panic!("could not connect to the test redis-server: {error}"),
        };
        let mut found = std::collections::BTreeSet::new();
        for pattern in patterns {
            let mut cursor: u64 = 0;
            loop {
                let (next, keys): (u64, Vec<String>) = match ::redis::cmd("SCAN")
                    .arg(cursor)
                    .arg("MATCH")
                    .arg(*pattern)
                    .arg("COUNT")
                    .arg(1_000)
                    .query_async(&mut conn)
                    .await
                {
                    Ok(page) => page,
                    Err(error) => panic!("could not scan the test redis-server: {error}"),
                };
                for key in keys {
                    let ttl: i64 = match ::redis::cmd("PTTL").arg(&key).query_async(&mut conn).await
                    {
                        Ok(ttl) => ttl,
                        Err(error) => panic!("could not read the expiry of `{key}`: {error}"),
                    };
                    // -1: no expiry (-2: the key is gone since the scan).
                    if ttl == -1 {
                        found.insert(key);
                    }
                }
                cursor = next;
                if cursor == 0 {
                    break;
                }
            }
        }
        found.into_iter().collect()
    }

    /// `true` while the server process is alive.
    pub fn is_running(&mut self) -> bool {
        match self.child.as_mut() {
            Some(child) => matches!(child.try_wait(), Ok(None)),
            None => false,
        }
    }

    /// Kills the server (simulating a crash or outage). Data is lost. Use
    /// [`TestRedis::restart`] to bring it back on the same socket.
    pub fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            if let Err(error) = child.kill() {
                tracing::debug!(%error, "test redis-server was already gone");
            }
            if let Err(error) = child.wait() {
                tracing::debug!(%error, "could not reap the test redis-server");
            }
        }
        self.dismiss_watchdog();
    }

    /// Tells the watchdog that the server was stopped on purpose, so that it leaves without
    /// doing anything, and reaps it.
    fn dismiss_watchdog(&mut self) {
        if let Some(mut watchdog) = self.watchdog.take() {
            if let Some(mut stdin) = watchdog.stdin.take()
                && let Err(error) = stdin.write_all(b"stop\n")
            {
                tracing::debug!(%error, "could not dismiss the test redis-server's watchdog");
            }
            if let Err(error) = watchdog.wait() {
                tracing::debug!(%error, "could not reap the test redis-server's watchdog");
            }
        }
    }

    /// Stops the server if it is running and starts a fresh one on the same socket, so
    /// existing pools reconnect to it.
    ///
    /// # Panics
    ///
    /// If the server does not come back.
    pub async fn restart(&mut self) {
        if let Err(error) = self.try_restart().await {
            panic!("could not restart the test redis-server: {error}");
        }
    }

    /// Stops the server if it is running and starts a fresh one on the same socket.
    pub async fn try_restart(&mut self) -> Result<()> {
        self.stop();
        self.spawn().await
    }

    async fn spawn(&mut self) -> Result<()> {
        if self.socket.exists() {
            std::fs::remove_file(&self.socket)?;
        }
        let child = Command::new(&self.server)
            .arg("--port")
            .arg("0")
            .arg("--unixsocket")
            .arg(&self.socket)
            .arg("--unixsocketperm")
            .arg("700")
            .arg("--save")
            .arg("")
            .arg("--appendonly")
            .arg("no")
            .arg("--dir")
            .arg(self.dir.path())
            .arg("--logfile")
            .arg(self.log_path())
            .args(&self.args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| {
                Error::Io(io::Error::new(
                    error.kind(),
                    format!("could not run `{}`: {error}", self.server.display()),
                ))
            })?;
        self.watchdog = watchdog(child.id(), self.dir.path());
        self.child = Some(child);
        self.wait_until_ready().await
    }

    async fn wait_until_ready(&mut self) -> Result<()> {
        let deadline = Instant::now() + STARTUP_TIMEOUT;
        loop {
            if let Some(child) = self.child.as_mut()
                && let Some(status) = child.try_wait()?
            {
                self.child = None;
                return Err(Error::Io(io::Error::other(format!(
                    "redis-server exited during startup ({status}); log:\n{}",
                    self.log_tail()
                ))));
            }
            if self.socket.exists() && ping(&self.url()).await.is_ok() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                self.stop();
                return Err(Error::Io(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "redis-server did not answer within {STARTUP_TIMEOUT:?}; log:\n{}",
                        self.log_tail()
                    ),
                )));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn log_path(&self) -> PathBuf {
        self.dir.path().join("redis.log")
    }

    fn log_tail(&self) -> String {
        match std::fs::read_to_string(self.log_path()) {
            Ok(log) => {
                let lines: Vec<&str> = log.lines().collect();
                lines[lines.len().saturating_sub(20)..].join("\n")
            }
            Err(error) => format!("(no log: {error})"),
        }
    }
}

/// Every Lua script of the pro features, by name (`UNIQUE_PUSH`, `BATCH_COMMIT`, ...) with
/// the SHA-1 Redis knows it by: for tests that read a `MONITOR` transcript (`EVALSHA <sha1>`)
/// to check which scripts ran, and in which order.
pub fn feature_scripts() -> Vec<(&'static str, String)> {
    crate::lua::all_scripts()
        .into_iter()
        .map(|script| (script.name(), script.script().get_hash().to_owned()))
        .collect()
}

impl Drop for TestRedis {
    fn drop(&mut self) {
        self.stop();
    }
}

impl std::fmt::Debug for TestRedis {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestRedis")
            .field("socket", &self.socket)
            .field("running", &self.child.is_some())
            .finish()
    }
}

/// Starts a watchdog for the server `pid` in `dir`: it reads a line from a pipe this process
/// writes `stop` to before stopping the server; if the pipe closes first (this process died:
/// a `kill -9`, an abort, the OOM killer), it kills the server and removes its directory.
/// Best effort: without `sh`, test servers are only stopped by `Drop`.
fn watchdog(pid: u32, dir: &Path) -> Option<Child> {
    const SCRIPT: &str =
        r#"read -r word; [ "$word" = stop ] || { kill -9 "$1" 2>/dev/null; rm -rf "$2"; }"#;
    match Command::new("sh")
        .arg("-c")
        .arg(SCRIPT)
        .arg("hench-redis-watchdog")
        .arg(pid.to_string())
        .arg(dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(watchdog) => Some(watchdog),
        Err(error) => {
            tracing::debug!(%error, "could not start a watchdog for the test redis-server");
            None
        }
    }
}

/// One `PING` over a fresh connection, without retries.
async fn ping(url: &str) -> Result<()> {
    let client = ::redis::Client::open(url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;
    let _: String = ::redis::cmd("PING").query_async(&mut conn).await?;
    Ok(())
}

fn find_redis_server() -> Result<PathBuf> {
    locate_redis_server(
        std::env::var_os(REDIS_SERVER_ENV),
        std::env::var_os("PATH"),
        &[
            "/usr/bin/redis-server",
            "/usr/local/bin/redis-server",
            "/opt/homebrew/bin/redis-server",
        ],
    )
}

/// Finds `redis-server`: the override, then `PATH`, then the well-known locations.
fn locate_redis_server(
    override_path: Option<std::ffi::OsString>,
    search_path: Option<std::ffi::OsString>,
    well_known: &[&str],
) -> Result<PathBuf> {
    if let Some(path) = override_path {
        let path = PathBuf::from(path);
        if path.is_file() {
            return Ok(path);
        }
        return Err(Error::Io(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "{REDIS_SERVER_ENV} is set to `{}`, which is not a file",
                path.display()
            ),
        )));
    }
    if let Some(paths) = search_path {
        for dir in std::env::split_paths(&paths) {
            let candidate = dir.join("redis-server");
            if candidate.is_file() {
                return Ok(candidate);
            }
        }
    }
    for candidate in well_known {
        if Path::new(candidate).is_file() {
            return Ok(PathBuf::from(candidate));
        }
    }
    Err(Error::Io(io::Error::new(
        io::ErrorKind::NotFound,
        format!(
            "redis-server not found: install Redis (for example `apt-get install redis-server` \
             or `brew install redis`) or set {REDIS_SERVER_ENV} to the binary's path"
        ),
    )))
}

/// A temporary directory short enough for a Unix socket path (about 100 bytes at most).
fn socket_dir() -> Result<tempfile::TempDir> {
    let mut base = std::env::temp_dir();
    if base.as_os_str().len() > 60 && Path::new("/tmp").is_dir() {
        base = PathBuf::from("/tmp");
    }
    Ok(tempfile::Builder::new()
        .prefix("hench-redis-")
        .tempdir_in(base)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn locates_redis_server() {
        let dir = tempfile::tempdir().unwrap();
        let fake = dir.path().join("redis-server");
        std::fs::write(&fake, b"").unwrap();
        let missing = dir.path().join("missing");

        let found = locate_redis_server(Some(fake.clone().into()), None, &[]).unwrap();
        assert_eq!(found, fake);

        let error = locate_redis_server(Some(missing.into()), None, &[]).unwrap_err();
        assert!(error.to_string().contains(REDIS_SERVER_ENV), "{error}");

        let found = locate_redis_server(None, Some(dir.path().into()), &[]).unwrap();
        assert_eq!(found, fake);

        let well_known = fake.to_str().unwrap();
        let found = locate_redis_server(None, None, &[well_known]).unwrap();
        assert_eq!(found, fake);

        let error = locate_redis_server(None, None, &[]).unwrap_err();
        assert!(error.to_string().contains("install Redis"), "{error}");
    }

    #[tokio::test]
    async fn keys_without_ttl_lists_keys_that_never_expire() {
        let redis = TestRedis::start().await;
        let pool = redis.pool().await;
        let mut conn = pool.conn().await.unwrap();
        let _: () = ::redis::pipe()
            .cmd("SET")
            .arg("forever")
            .arg(1)
            .cmd("SET")
            .arg("brief")
            .arg(1)
            .arg("PX")
            .arg(60_000)
            .cmd("ZADD")
            .arg("unique-locks")
            .arg(1)
            .arg("d")
            .query_async(&mut conn)
            .await
            .unwrap();
        assert_eq!(
            redis.keys_without_ttl(&["*"]).await,
            ["forever", "unique-locks"]
        );
        assert_eq!(
            redis.keys_without_ttl(&["unique*", "brief"]).await,
            ["unique-locks"]
        );
        let config = redis.builder().await.config_for_test().unwrap();
        assert_eq!(config.periodic_lease, Duration::from_secs(2));
        assert_eq!(config.batch_dispatch_grace, Duration::from_millis(100));
    }

    #[test]
    fn socket_directories_are_short() {
        let dir = socket_dir().unwrap();
        assert!(dir.path().join("redis.sock").as_os_str().len() < 100);
    }
}
