//! What hench logs: the job logger's events from real workers (Sidekiq's processor_test
//! "custom job logger class" checks that wiring) and the start-up checks of Redis (Sidekiq's
//! cli_test "checking maxmemory policy", plus a Redis without `INFO` and one too old). In a
//! binary of its own because it installs a global `tracing` subscriber (`tracing` caches
//! callsite interest process-wide, see `tests/testing_spans.rs`).

mod support;

use std::io::Write;
use std::sync::{Arc, Mutex, OnceLock};

use hench::prelude::*;
use hench::test_util::TestRedis;
use hench::{Error, redis_rs};
use serde_json::json;
use support::*;

/// A `tracing` writer appending to a shared buffer.
#[derive(Clone, Default)]
struct Buffer(Arc<Mutex<Vec<u8>>>);

impl Write for Buffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Everything hench logged at `info` and above in this process (the subscriber is installed by
/// the first test that asks).
fn logs() -> String {
    static LOGS: OnceLock<Buffer> = OnceLock::new();
    let buffer = LOGS.get_or_init(|| {
        let buffer = Buffer::default();
        let writer = buffer.clone();
        tracing::subscriber::set_global_default(
            tracing_subscriber::fmt()
                .with_writer(move || writer.clone())
                .with_ansi(false)
                .with_env_filter("hench=info")
                .finish(),
        )
        .unwrap();
        buffer
    });
    String::from_utf8_lossy(&buffer.0.lock().unwrap()).into_owned()
}

/// Real workers log `start`, then `done` or `fail` with the elapsed time, inside the job's
/// span (`job{class, jid}`).
#[tokio::test]
async fn workers_log_start_done_and_fail_with_the_job_context() {
    logs();
    let redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    let errors = Errors::default();
    let hench = redis
        .builder()
        .await
        .register(Echo)
        .register(Fail)
        .data_arc(runs.clone())
        .error_handler(errors.handler())
        .build()
        .await
        .unwrap();
    let launcher = hench.start().await.unwrap();
    let ok = Echo::perform_async(hench.client(), ("logged".into(),))
        .await
        .unwrap()
        .unwrap();
    let failed = Fail::perform_async(hench.client(), vec![json!(1)])
        .await
        .unwrap()
        .unwrap();
    runs.wait_for(1).await;
    wait_for("the failure", WAIT, || async {
        errors.with_context("job failed").len() == 1
    })
    .await;
    launcher.stop().await.unwrap();
    let output = logs();
    let lines_of = |jid: &str| -> Vec<String> {
        output
            .lines()
            .filter(|line| line.contains(&format!("jid={jid}")))
            .map(str::to_owned)
            .collect()
    };
    let succeeded = lines_of(&ok);
    assert!(
        succeeded
            .iter()
            .any(|line| line.contains("class=Echo") && line.ends_with("start")),
        "{output}"
    );
    assert!(
        succeeded
            .iter()
            .any(|line| line.contains("done") && line.contains("elapsed=")),
        "{output}"
    );
    assert!(
        !succeeded.iter().any(|line| line.contains(" fail")),
        "{output}"
    );
    let failures = lines_of(&failed);
    assert!(
        failures
            .iter()
            .any(|line| line.contains("class=Fail") && line.ends_with("start")),
        "{output}"
    );
    assert!(
        failures
            .iter()
            .any(|line| line.contains("fail") && line.contains("elapsed=")),
        "{output}"
    );
    assert!(
        !failures.iter().any(|line| line.contains("done")),
        "{output}"
    );
}

/// The `starting` line lists the registered jobs, and a process without any warns that every
/// job it takes will fail and why that usually happens (a `#[hench::job]` registers itself only
/// in binaries that link the crate defining it).
#[tokio::test]
async fn the_start_up_lists_the_jobs_and_warns_when_there_are_none() {
    logs();
    let redis = TestRedis::start().await;
    let with_jobs = redis
        .builder()
        .await
        .auto_register(false)
        .register(Echo)
        .build()
        .await
        .unwrap()
        .start()
        .await
        .unwrap();
    let without_jobs = redis
        .builder()
        .await
        .auto_register(false)
        .build()
        .await
        .unwrap()
        .start()
        .await
        .unwrap();
    with_jobs.stop().await.unwrap();
    without_jobs.stop().await.unwrap();
    let output = logs();
    let starting = |identity: &str| -> String {
        output
            .lines()
            .find(|line| {
                line.contains(&format!("identity={identity}")) && line.contains(": starting ")
            })
            .unwrap_or_else(|| panic!("no start-up line for {identity}: {output}"))
            .to_owned()
    };
    assert!(
        starting(with_jobs.identity()).contains(r#"jobs=["Echo"]"#),
        "{output}"
    );
    assert!(
        starting(without_jobs.identity()).contains("jobs=[]"),
        "{output}"
    );
    assert!(
        output.contains("no job is registered") && output.contains("use my_jobs as _;"),
        "{output}"
    );
}

/// `logged_job_attributes` adds payload keys to every line logged in a job's span, `start` and
/// `done` included (Sidekiq's `logged_job_attributes`): here a request id, as a client
/// middleware would stamp it. Fields of the span cannot be added once it exists, so they used
/// to reach no line at all.
#[tokio::test]
async fn logged_job_attributes_reach_every_line_of_the_job() {
    logs();
    let redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    let hench = redis
        .builder()
        .await
        .auto_register(false)
        .register(Echo)
        .data_arc(runs.clone())
        .logged_job_attributes(["request_id", "tenant", "absent"])
        .build()
        .await
        .unwrap();
    let launcher = hench.start().await.unwrap();
    let jid = Echo::set()
        .custom("request_id", "req-4242")
        .custom("tenant", 7)
        .perform_async(hench.client(), ("attributes".into(),))
        .await
        .unwrap()
        .unwrap();
    runs.wait_for(1).await;
    launcher.stop().await.unwrap();
    let output = logs();
    let lines: Vec<&str> = output
        .lines()
        .filter(|line| line.contains(&format!("jid={jid}")))
        .collect();
    let attributes = r#"attrs={"request_id":"req-4242","tenant":7}"#;
    assert!(
        lines
            .iter()
            .any(|line| line.ends_with("start") && line.contains(attributes)),
        "{output}"
    );
    assert!(
        lines
            .iter()
            .any(|line| line.contains(" done ") && line.contains(attributes)),
        "{output}"
    );
}

/// A unique job that gives its lock up as soon as it starts (`release_unique_lock`), then works
/// across several heartbeats.
#[derive(Default)]
struct GivesItsLockUp;

impl Job for GivesItsLockUp {
    const NAME: &'static str = "logging::GivesItsLockUp";
    type Args = (String,);

    fn options() -> JobOptions {
        JobOptions::new().unique_for(std::time::Duration::from_secs(60))
    }

    async fn perform(&self, ctx: &JobContext, (label,): Self::Args) -> Result<(), JobError> {
        let runs = ctx.data::<Runs>()?;
        let released = ctx.release_unique_lock().await?;
        runs.record(format!("{label}:released={released}"));
        tokio::time::sleep(std::time::Duration::from_millis(800)).await;
        runs.record(format!("{label}:done"));
        Ok(())
    }
}

/// The heartbeat renews the locks of running unique jobs and warns about a job that no longer
/// holds its lock (a lapse: a duplicate may have been pushed). A job that gave its lock up on
/// purpose is no lapse: neither its renewals (every 100 ms here, while it works for 800 ms) nor
/// its outcome report it. (The renewals used to report it at every beat.)
#[tokio::test]
async fn a_job_that_gave_its_lock_up_is_not_reported_as_a_lapse() {
    logs();
    let redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    let hench = redis
        .builder()
        .await
        .auto_register(false)
        .register(GivesItsLockUp)
        .data_arc(runs.clone())
        .build()
        .await
        .unwrap();
    let launcher = hench.start().await.unwrap();
    GivesItsLockUp::perform_async(hench.client(), ("first".into(),))
        .await
        .unwrap()
        .unwrap();
    runs.wait_for(1).await;
    // While it still runs, its lock is free: a duplicate is a job of its own.
    assert!(
        GivesItsLockUp::perform_async(hench.client(), ("second".into(),))
            .await
            .unwrap()
            .is_some()
    );
    runs.wait_for(4).await;
    launcher.stop().await.unwrap();
    let mut entries = runs.entries();
    entries.sort();
    assert_eq!(
        entries,
        [
            "first:done",
            "first:released=true",
            "second:done",
            "second:released=true"
        ]
    );
    let output = logs();
    let lapses: Vec<&str> = output
        .lines()
        .filter(|line| line.contains(GivesItsLockUp::NAME) && line.contains("no longer held"))
        .collect();
    assert!(lapses.is_empty(), "{lapses:#?}");
}

/// A remote `TTIN` (`api::Process::dump`, Sidekiq's `dump_threads`) makes the process log what
/// each busy slot runs; the signal is consumed and the process goes on. (Hench used to log it as
/// an unknown signal.)
#[tokio::test]
async fn a_remote_dump_logs_the_running_jobs() {
    logs();
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let runs = Arc::new(Runs::default());
    let gate = Arc::new(Gate::default());
    let hench = redis
        .builder()
        .await
        .auto_register(false)
        .register(Blocked)
        .data_arc(runs.clone())
        .data_arc(gate.clone())
        .build()
        .await
        .unwrap();
    let launcher = hench.start().await.unwrap();
    let jid = Blocked::perform_async(hench.client(), ("dumped".into(),))
        .await
        .unwrap()
        .unwrap();
    wait_for("the job to start", WAIT, || async { gate.entered() == 1 }).await;
    let process = hench::api::Api::new(pool.clone())
        .processes()
        .get(launcher.identity())
        .await
        .unwrap()
        .unwrap();
    process.dump().await.unwrap();
    wait_for("the dump", WAIT, || async {
        logs()
            .lines()
            .any(|line| line.contains(&format!("jid={jid}")) && line.contains("running job"))
    })
    .await;
    let output = logs();
    let line = output
        .lines()
        .find(|line| line.contains(&format!("jid={jid}")) && line.contains("running job"))
        .unwrap();
    assert!(
        line.contains("class=Blocked") && line.contains("queue=default"),
        "{line}"
    );
    assert!(
        output.contains(&format!("identity={}", launcher.identity())) && output.contains("busy=1"),
        "{output}"
    );
    wait_for("the signal to be consumed", WAIT, || async {
        llen(&pool, &hench::keys::signals(launcher.identity())).await == 0
    })
    .await;
    assert!(!launcher.is_quiet() && !launcher.is_stopping());
    gate.open();
    launcher.stop().await.unwrap();
    assert_eq!(runs.entries(), ["entered:dumped", "done:dumped"]);
}

/// How many times the start-up check warned about Redis's eviction policy.
fn eviction_warnings() -> usize {
    logs().matches("maxmemory-policy to noeviction").count()
}

/// The start-up check warns when Redis may evict keys (`maxmemory-policy` other than
/// `noeviction`), and is silent otherwise.
#[tokio::test]
async fn the_eviction_policy_is_warned_about_unless_noeviction() {
    logs();
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let before = eviction_warnings();
    // noeviction (the default): silent.
    let launcher = redis
        .builder()
        .await
        .build()
        .await
        .unwrap()
        .start()
        .await
        .unwrap();
    launcher.stop().await.unwrap();
    assert_eq!(eviction_warnings(), before);
    // allkeys-lru: starts, with a warning.
    let () = cmd(
        &pool,
        &redis_cmd!("CONFIG", "SET", "maxmemory-policy", "allkeys-lru"),
    )
    .await;
    let launcher = redis
        .builder()
        .await
        .build()
        .await
        .unwrap()
        .start()
        .await
        .unwrap();
    launcher.stop().await.unwrap();
    assert_eq!(eviction_warnings(), before + 1);
}

/// Managed services may rename or disable `INFO`: the version check is skipped and the
/// process starts.
#[tokio::test]
async fn a_redis_without_info_still_starts() {
    logs();
    let redis = TestRedis::start_with_args(&["--rename-command", "INFO", ""]).await;
    let pool = redis.pool().await;
    let refused: redis_rs::RedisResult<String> = {
        let mut conn = pool.conn().await.unwrap();
        redis_rs::cmd("INFO").query_async(&mut conn).await
    };
    assert!(refused.is_err());
    let launcher = redis
        .builder()
        .await
        .build()
        .await
        .unwrap()
        .start()
        .await
        .expect("start without INFO");
    launcher.stop().await.unwrap();
}

/// A fake Redis answering `INFO` as Redis 6.0 would, `PING` with `PONG` and anything else with
/// `OK`; returns its port.
fn fake_old_redis() -> u16 {
    use std::io::{BufRead, BufReader, Read};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { return };
            std::thread::spawn(move || {
                let mut write = stream.try_clone().unwrap();
                let mut read = BufReader::new(stream);
                loop {
                    let mut header = String::new();
                    if read.read_line(&mut header).unwrap_or(0) == 0 {
                        return;
                    }
                    let count: usize = header.trim()[1..].parse().unwrap_or(0);
                    let mut args = Vec::new();
                    for _ in 0..count {
                        let mut len = String::new();
                        if read.read_line(&mut len).unwrap_or(0) == 0 {
                            return;
                        }
                        let len: usize = len.trim()[1..].parse().unwrap_or(0);
                        let mut data = vec![0; len + 2];
                        if read.read_exact(&mut data).is_err() {
                            return;
                        }
                        data.truncate(len);
                        args.push(String::from_utf8_lossy(&data).to_uppercase());
                    }
                    let reply = match args.first().map(String::as_str) {
                        Some("INFO") => {
                            let info = "# Server\r\nredis_version:6.0.16\r\n\r\n# Memory\r\n\
                                        maxmemory_policy:noeviction\r\n";
                            format!("${}\r\n{info}\r\n", info.len())
                        }
                        Some("PING") => "+PONG\r\n".to_owned(),
                        _ => "+OK\r\n".to_owned(),
                    };
                    if write.write_all(reply.as_bytes()).is_err() {
                        return;
                    }
                }
            });
        }
    });
    port
}

/// Redis older than 6.2 (no `LMOVE`, `BLMOVE` or `ZRANGE BYSCORE`) is refused at start.
#[tokio::test]
async fn redis_older_than_6_2_is_refused() {
    logs();
    let port = fake_old_redis();
    let hench = Hench::builder()
        .redis_url(format!("redis://127.0.0.1:{port}/"))
        .systemd(false)
        .build()
        .await
        .unwrap();
    match hench.start().await {
        Err(Error::Config(message)) => assert!(message.contains("too old"), "{message}"),
        Err(other) => panic!("expected a configuration error, got {other}"),
        Ok(_) => panic!("started against Redis 6.0"),
    }
}

/// A worker built from `hench::cli` says where its settings come from, and warns about an
/// environment chosen with `-e` that selects nothing: here there is no configuration file in
/// the current directory (the tests run in the crate's), as for a systemd unit without
/// `WorkingDirectory=`, which starts in `/`.
#[cfg(feature = "cli")]
#[test]
fn worker_settings_and_their_warnings_are_logged() {
    use clap::Parser;

    logs();
    let settings = hench::cli::WorkerArgs::try_parse_from(["worker", "-e", "production"])
        .unwrap()
        .settings()
        .unwrap();
    let _builder = settings.apply(Hench::builder());
    let output = logs();
    let line = output
        .lines()
        .find(|line| line.contains("worker settings without a configuration file"))
        .unwrap_or_else(|| panic!("no settings line in {output}"));
    assert!(
        line.contains("INFO") && line.contains("environment=production"),
        "{line}"
    );
    let warning = output
        .lines()
        .find(|line| line.contains("no configuration file was read"))
        .unwrap_or_else(|| panic!("no warning in {output}"));
    assert!(
        warning.contains("WARN") && warning.contains("-e"),
        "{warning}"
    );
}
