//! systemd notifications sent by running workers (Sidekiq's `systemd_test` scenarios cover the
//! notifier itself, in `src/systemd.rs`).
#![cfg(unix)]

use std::time::Duration;

use hench::systemd::Notifier;
use hench::test_util::TestRedis;
use tokio::net::UnixDatagram;

/// A notification socket in a temporary directory.
fn socket() -> (tempfile::TempDir, std::path::PathBuf, UnixDatagram) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("notify.sock");
    let socket = UnixDatagram::bind(&path).unwrap();
    (directory, path, socket)
}

/// The next notification, or `None` after `wait` without one.
async fn next(socket: &UnixDatagram, wait: Duration) -> Option<String> {
    let mut buffer = vec![0_u8; 512];
    let size = tokio::time::timeout(wait, socket.recv(&mut buffer))
        .await
        .ok()?
        .unwrap();
    Some(String::from_utf8_lossy(&buffer[..size]).into_owned())
}

/// Receives notifications until one satisfies `done`; returns them all.
async fn until(socket: &UnixDatagram, done: impl Fn(&str) -> bool) -> Vec<String> {
    let mut seen = Vec::new();
    loop {
        let message = next(socket, Duration::from_secs(10))
            .await
            .unwrap_or_else(|| panic!("no notification after {seen:?}"));
        let finished = done(&message);
        seen.push(message);
        if finished {
            return seen;
        }
    }
}

#[tokio::test]
async fn workers_notify_readiness_status_watchdog_and_shutdown() {
    let (_directory, path, socket) = socket();
    let redis = TestRedis::start().await;
    let hench = redis
        .builder()
        .await
        .concurrency(3)
        // Keep-alives every 30 ms: more often than the 100 ms heartbeat.
        .systemd_notifier(Notifier::new(&path).with_watchdog(Duration::from_millis(60)))
        .build()
        .await
        .unwrap();
    let launcher = hench.start().await.unwrap();

    let ready = next(&socket, Duration::from_secs(10)).await.unwrap();
    assert_eq!(ready, "READY=1\nSTATUS=processing jobs: 0 of 3 busy");

    // Keep-alives between beats, and with the status on every beat.
    let seen = until(&socket, |message| {
        message.starts_with("WATCHDOG=1\nSTATUS=")
    })
    .await;
    assert!(
        seen.iter().any(|message| message == "WATCHDOG=1"),
        "{seen:?}"
    );

    launcher.quiet().await;
    until(&socket, |message| {
        message.contains("STATUS=quiet: 0 of 3 busy")
    })
    .await;

    let stop = tokio::spawn({
        let launcher = launcher.clone();
        async move { launcher.stop().await }
    });
    let seen = until(&socket, |message| message.starts_with("STOPPING=1")).await;
    assert_eq!(
        seen.last().map(String::as_str),
        Some("STOPPING=1\nSTATUS=stopping: waiting up to 5s for running jobs")
    );
    stop.await.unwrap().unwrap();
}

#[tokio::test]
async fn without_a_watchdog_beats_only_update_the_status() {
    let (_directory, path, socket) = socket();
    let redis = TestRedis::start().await;
    let hench = redis
        .builder()
        .await
        .systemd_notifier(Notifier::new(&path))
        .build()
        .await
        .unwrap();
    let launcher = hench.start().await.unwrap();
    assert!(
        next(&socket, Duration::from_secs(10))
            .await
            .unwrap()
            .starts_with("READY=1")
    );
    for _ in 0..3 {
        let message = next(&socket, Duration::from_secs(10)).await.unwrap();
        assert_eq!(message, "STATUS=processing jobs: 0 of 5 busy");
    }
    launcher.stop().await.unwrap();
}

#[tokio::test]
async fn notifications_can_be_turned_off() {
    let (_directory, path, socket) = socket();
    let redis = TestRedis::start().await;
    let hench = redis
        .builder()
        .await
        .systemd_notifier(Notifier::new(&path))
        .systemd(false)
        .build()
        .await
        .unwrap();
    let launcher = hench.start().await.unwrap();
    // Three heartbeats' worth of silence.
    assert_eq!(next(&socket, Duration::from_millis(350)).await, None);
    launcher.stop().await.unwrap();
    assert_eq!(next(&socket, Duration::from_millis(50)).await, None);
}

#[tokio::test]
async fn a_missing_socket_does_not_stop_the_workers() {
    let redis = TestRedis::start().await;
    let hench = redis
        .builder()
        .await
        .systemd_notifier(
            Notifier::new("/nonexistent/hench/notify.sock")
                .with_watchdog(Duration::from_millis(50)),
        )
        .build()
        .await
        .unwrap();
    let launcher = hench.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(!launcher.is_stopped());
    launcher.stop().await.unwrap();
}

/// `process_id` of the test redis-server, from `INFO server`.
async fn redis_pid(pool: &hench::RedisPool) -> u32 {
    let mut conn = pool.conn().await.unwrap();
    let info: String = hench::redis_rs::cmd("INFO")
        .arg("server")
        .query_async(&mut conn)
        .await
        .unwrap();
    info.lines()
        .find_map(|line| line.strip_prefix("process_id:"))
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

/// Sends `signal` (`STOP`, `CONT`) to the process `pid`.
fn signal(pid: u32, signal: &str) {
    let status = std::process::Command::new("kill")
        .arg(format!("-{signal}"))
        .arg(pid.to_string())
        .status()
        .unwrap();
    assert!(status.success());
}

/// Watchdog keep-alives do not wait for the heartbeat: while Redis does not answer, every beat
/// waits for its commands to time out (5 s each here), yet a keep-alive still goes out every
/// half watchdog period. (They used to come from the heartbeat's own loop, so a Redis stall
/// longer than the watchdog got the process killed by systemd.)
#[tokio::test]
async fn watchdog_keep_alives_continue_while_redis_stalls() {
    let (_directory, path, socket) = socket();
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let hench = redis
        .builder()
        .await
        .systemd_notifier(Notifier::new(&path).with_watchdog(Duration::from_secs(1)))
        .build()
        .await
        .unwrap();
    let launcher = hench.start().await.unwrap();
    assert!(
        next(&socket, Duration::from_secs(10))
            .await
            .unwrap()
            .starts_with("READY=1")
    );
    let pid = redis_pid(&pool).await;
    signal(pid, "STOP");
    let stalled = std::time::Instant::now();
    let mut last = stalled;
    let mut longest = Duration::ZERO;
    let mut keep_alives = 0;
    while stalled.elapsed() < Duration::from_secs(3) {
        match next(&socket, Duration::from_secs(2)).await {
            Some(message) if message.contains("WATCHDOG=1") => {
                longest = longest.max(last.elapsed());
                last = std::time::Instant::now();
                keep_alives += 1;
            }
            Some(_) => {}
            None => {
                longest = longest.max(last.elapsed());
                break;
            }
        }
    }
    signal(pid, "CONT");
    launcher.stop().await.unwrap();
    assert!(keep_alives >= 4, "{keep_alives} keep-alives in 3 s");
    assert!(
        longest < Duration::from_millis(900),
        "{longest:?} without a keep-alive (watchdog: 1 s)"
    );
}
