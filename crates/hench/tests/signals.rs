//! `Hench::run` and process signals: SIGTSTP quiets, SIGTERM stops; and what depends on the
//! process environment (systemd's `NOTIFY_SOCKET`). In a test binary of its own because
//! signals and the environment are process-wide; its tests run one at a time.
#![cfg(unix)]

mod support;

use std::time::Duration;

use hench::keys;
use hench::test_util::TestRedis;
use support::*;
use tokio::net::UnixDatagram;

/// Held by every test of this binary, so that they run one at a time.
static PROCESS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Sends `signal` (e.g. `-TSTP`) to this test process.
fn signal_self(signal: &str) {
    let status = std::process::Command::new("kill")
        .arg(signal)
        .arg(std::process::id().to_string())
        .status()
        .expect("run kill");
    assert!(status.success());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_quiets_on_sigtstp_and_stops_on_sigterm() {
    let _process = PROCESS.lock().await;
    init_logging();
    let redis = TestRedis::start().await;
    let events = Events::default();
    let hench = events
        .register(redis.builder().await)
        .heartbeat_interval(hench::Config::MAX_HEARTBEAT_INTERVAL)
        .build()
        .await
        .unwrap();
    let pool = hench.redis().clone();
    let running = tokio::spawn(hench.run());
    // `run` listens to signals before it registers the process.
    wait_for("the process to register", WAIT, || async {
        smembers(&pool, keys::PROCESSES).await.len() == 1
    })
    .await;
    let identity = smembers(&pool, keys::PROCESSES).await.remove(0);
    assert_eq!(hgetall(&pool, &identity).await["quiet"], "false");

    signal_self("-TSTP");
    wait_for("the quiet flag", WAIT, || async {
        hgetall(&pool, &identity).await["quiet"] == "true"
    })
    .await;
    assert!(!running.is_finished());

    signal_self("-TERM");
    tokio::time::timeout(WAIT, running)
        .await
        .expect("run returns after SIGTERM")
        .unwrap()
        .unwrap();
    assert!(smembers(&pool, keys::PROCESSES).await.is_empty());
    assert_eq!(
        events.milestones(),
        ["startup", "quiet", "shutdown", "exit"]
    );
}

/// The notifications received within `wait` of each other.
async fn notifications(socket: &UnixDatagram, wait: Duration) -> Vec<String> {
    let mut buffer = vec![0_u8; 512];
    let mut seen = Vec::new();
    while let Ok(Ok(size)) = tokio::time::timeout(wait, socket.recv(&mut buffer)).await {
        seen.push(String::from_utf8_lossy(&buffer[..size]).into_owned());
    }
    seen
}

/// By default (`NOTIFY_SOCKET` set by systemd) only `Hench::run` notifies systemd: workers
/// embedded with `start` or `run_until` leave the service's notifications to the program that
/// embeds them (their `STOPPING=1` after a remote stop used to make systemd stop the whole
/// service).
#[test]
fn only_run_notifies_systemd_by_default() {
    let _process = PROCESS.blocking_lock();
    init_logging();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("notify.sock");
    // SAFETY: no other thread of this process uses the environment meanwhile: the tests of
    // this binary run one at a time and the runtime is not started yet.
    unsafe { std::env::set_var(hench::systemd::NOTIFY_SOCKET, &path) };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let socket = UnixDatagram::bind(&path).unwrap();
        let redis = TestRedis::start().await;
        let pool = redis.pool().await;
        let builder = || async { redis.builder().await.systemd(true) };

        // Embedded with `start`, stopped remotely (the dashboard's Stop).
        let launcher = builder()
            .await
            .build()
            .await
            .unwrap()
            .start()
            .await
            .unwrap();
        let process = hench::api::Api::new(pool.clone())
            .processes()
            .get(launcher.identity())
            .await
            .unwrap()
            .unwrap();
        process.quiet().await.unwrap();
        process.stop().await.unwrap();
        tokio::time::timeout(WAIT, launcher.wait()).await.unwrap();
        assert_eq!(
            notifications(&socket, Duration::from_millis(300)).await,
            Vec::<String>::new()
        );

        // Embedded with `run_until`.
        let token = hench::CancellationToken::new();
        let workers = tokio::spawn(
            builder()
                .await
                .build()
                .await
                .unwrap()
                .run_until(token.clone().cancelled_owned()),
        );
        wait_for("the process to register", WAIT, || async {
            smembers(&pool, keys::PROCESSES).await.len() == 1
        })
        .await;
        token.cancel();
        workers.await.unwrap().unwrap();
        assert_eq!(
            notifications(&socket, Duration::from_millis(300)).await,
            Vec::<String>::new()
        );

        // The process's own workers, with `run`.
        let running = tokio::spawn(builder().await.build().await.unwrap().run());
        let mut buffer = vec![0_u8; 512];
        let size = tokio::time::timeout(WAIT, socket.recv(&mut buffer))
            .await
            .unwrap()
            .unwrap();
        let ready = String::from_utf8_lossy(&buffer[..size]).into_owned();
        assert!(ready.starts_with("READY=1"), "{ready}");
        signal_self("-TERM");
        tokio::time::timeout(WAIT, running)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let stopping = notifications(&socket, Duration::from_millis(300)).await;
        assert!(
            stopping
                .iter()
                .any(|message| message.starts_with("STOPPING=1")),
            "{stopping:?}"
        );
    });
    drop(runtime);
    // SAFETY: as above; the runtime and its threads are gone.
    unsafe { std::env::remove_var(hench::systemd::NOTIFY_SOCKET) };
}

/// A second SIGTERM during `Hench::run`'s graceful shutdown interrupts the running jobs right
/// away (DESIGN §5) instead of waiting for `timeout`; the interrupted job goes back to its
/// queue.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_sigterm_interrupts_running_jobs_at_once() {
    let _process = PROCESS.lock().await;
    init_logging();
    let redis = TestRedis::start().await;
    let runs = std::sync::Arc::new(Runs::default());
    let hench = redis
        .builder()
        .await
        .register(Stubborn)
        .data_arc(runs.clone())
        // Far longer than the test may take: only a forced shutdown ends it in time.
        .timeout(Duration::from_secs(60))
        .build()
        .await
        .unwrap();
    let pool = hench.redis().clone();
    let client = hench.client().clone();
    let running = tokio::spawn(hench.run());
    wait_for("the process to register", WAIT, || async {
        smembers(&pool, keys::PROCESSES).await.len() == 1
    })
    .await;
    let identity = smembers(&pool, keys::PROCESSES).await.remove(0);
    client.enqueue::<Stubborn>(("a".into(),)).await.unwrap();
    runs.wait_for(1).await;
    signal_self("-TERM");
    // The graceful shutdown waits for the job (up to 60 s).
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!running.is_finished());
    let second = std::time::Instant::now();
    signal_self("-TERM");
    tokio::time::timeout(Duration::from_secs(15), running)
        .await
        .expect("run returns soon after the second SIGTERM")
        .unwrap()
        .unwrap();
    assert!(second.elapsed() < Duration::from_secs(15));
    // The interrupted job is back in its queue; nothing is left in progress.
    assert_eq!(queue_size(&pool, "default").await, 1);
    assert_eq!(llen(&pool, &keys::inprogress(&identity)).await, 0);
    assert!(smembers(&pool, keys::PROCESSES).await.is_empty());
}

/// Whether this process catches `signal`: its bit in the `SigCgt` mask of `/proc/self/status`.
#[cfg(target_os = "linux")]
fn catches(signal: libc::c_int) -> bool {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    let mask = status
        .lines()
        .find_map(|line| line.strip_prefix("SigCgt:"))
        .unwrap()
        .trim();
    let mask = u64::from_str_radix(mask, 16).unwrap();
    mask & (1 << (signal - 1)) != 0
}

/// SIGTTIN, Sidekiq's thread-dump signal, logs what each busy slot runs (see
/// `tests/logging.rs` for the dump itself). Left to its default action it stopped the whole
/// process, liveness thread included, whose jobs another process then recovered and ran a
/// second time.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_logs_a_dump_on_sigttin_and_keeps_running() {
    let _process = PROCESS.lock().await;
    init_logging();
    let redis = TestRedis::start().await;
    let hench = redis.builder().await.build().await.unwrap();
    let pool = hench.redis().clone();
    let running = tokio::spawn(hench.run());
    wait_for("the process to register", WAIT, || async {
        smembers(&pool, keys::PROCESSES).await.len() == 1
    })
    .await;
    let identity = smembers(&pool, keys::PROCESSES).await.remove(0);
    // Checked before sending it: a process that does not catch it would stop here.
    #[cfg(target_os = "linux")]
    assert!(catches(libc::SIGTTIN), "SIGTTIN keeps its default action");
    signal_self("-TTIN");
    // Still running (a stopped process does not beat), and not quieted.
    let beat = |fields: std::collections::HashMap<String, String>| -> f64 {
        fields["beat"].parse().unwrap()
    };
    let first = beat(hgetall(&pool, &identity).await);
    wait_for("another heartbeat", WAIT, || async {
        beat(hgetall(&pool, &identity).await) > first
    })
    .await;
    assert_eq!(hgetall(&pool, &identity).await["quiet"], "false");
    assert!(!running.is_finished());
    signal_self("-TERM");
    tokio::time::timeout(WAIT, running)
        .await
        .expect("run returns after SIGTERM")
        .unwrap()
        .unwrap();
}
