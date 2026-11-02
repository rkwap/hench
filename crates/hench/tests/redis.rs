//! `RedisPool` and `TestRedis` behaviour against a real server.

mod common;

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use common::*;
use hench::redis_rs::{self, make_extension_error};
use hench::test_util::TestRedis;
use hench::{Error, RedisConfig, RedisPool};

#[tokio::test]
async fn connects_and_pings() {
    let redis = TestRedis::start().await;
    assert!(redis.url().starts_with("redis+unix://"));
    assert!(redis.socket_path().exists());
    let pool = redis.pool().await;
    pool.ping().await.unwrap();
    let pool = RedisPool::connect(&redis.url()).await.unwrap();
    pool.ping().await.unwrap();
    assert!(format!("{pool:?}").contains("redis+unix"));
}

#[tokio::test]
async fn servers_are_isolated() {
    let first = TestRedis::start().await;
    let second = TestRedis::start().await;
    let a = first.pool().await;
    let b = second.pool().await;
    let () = cmd(&a, redis_rs::cmd("SET").arg("k").arg("first").clone()).await;
    let missing: Option<String> = cmd(&b, redis_rs::cmd("GET").arg("k").clone()).await;
    assert_eq!(missing, None);
}

#[tokio::test]
async fn database_numbers_in_unix_urls() {
    let redis = TestRedis::start().await;
    let db3 = RedisPool::connect(&format!("{}?db=3", redis.url()))
        .await
        .unwrap();
    let db0 = redis.pool().await;
    let () = cmd(&db3, redis_rs::cmd("SET").arg("k").arg("in db 3").clone()).await;
    let in_db0: Option<String> = cmd(&db0, redis_rs::cmd("GET").arg("k").clone()).await;
    assert_eq!(in_db0, None);
    let in_db3: Option<String> = cmd(&db3, redis_rs::cmd("GET").arg("k").clone()).await;
    assert_eq!(in_db3.as_deref(), Some("in db 3"));
}

#[tokio::test]
async fn the_shared_connection_is_reused_until_reconnect() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let id = || async {
        let id: i64 = cmd(&pool, redis_rs::cmd("CLIENT").arg("ID").clone()).await;
        id
    };
    let first = id().await;
    assert_eq!(id().await, first);
    pool.reconnect();
    let second = id().await;
    assert_ne!(second, first);
    assert_eq!(id().await, second);
}

#[tokio::test]
async fn several_shared_connections_are_handed_out_in_turn() {
    let redis = TestRedis::start().await;
    let pool = RedisPool::from_config(redis.config().shared_connections(3))
        .await
        .unwrap();
    let mut ids = Vec::new();
    for _ in 0..6 {
        let id: i64 = cmd(&pool, redis_rs::cmd("CLIENT").arg("ID").clone()).await;
        ids.push(id);
    }
    // Three connections, each used every third time.
    assert_eq!(&ids[..3], &ids[3..]);
    let mut distinct = ids.clone();
    distinct.sort_unstable();
    distinct.dedup();
    assert_eq!(distinct.len(), 3, "{ids:?}");
    // A reconnection replaces them all.
    pool.reconnect();
    let mut after = Vec::new();
    for _ in 0..3 {
        let id: i64 = cmd(&pool, redis_rs::cmd("CLIENT").arg("ID").clone()).await;
        after.push(id);
    }
    assert!(
        after.iter().all(|id| !ids.contains(id)),
        "{ids:?} then {after:?}"
    );
}

#[tokio::test]
async fn concurrent_first_use_creates_one_connection() {
    let redis = TestRedis::start().await;
    let pool = RedisPool::lazy(redis.config()).unwrap();
    let tasks: Vec<_> = (0..20)
        .map(|_| {
            let pool = pool.clone();
            tokio::spawn(async move {
                let id: i64 = cmd(&pool, redis_rs::cmd("CLIENT").arg("ID").clone()).await;
                id
            })
        })
        .collect();
    let mut ids = Vec::new();
    for task in tasks {
        ids.push(task.await.unwrap());
    }
    ids.dedup();
    assert_eq!(ids.len(), 1, "{ids:?}");
}

#[tokio::test]
async fn blocking_connections_outlive_the_response_timeout() {
    let redis = TestRedis::start().await;
    let config = redis.config().response_timeout(Duration::from_millis(200));
    let pool = RedisPool::from_config(config).await.unwrap();

    // On the shared connection a long BRPOP trips the response timeout...
    let mut shared = pool.conn().await.unwrap();
    let result: Result<Option<(String, String)>, _> = redis_rs::cmd("BRPOP")
        .arg("nothing")
        .arg(1)
        .query_async(&mut shared)
        .await;
    assert!(result.is_err(), "{result:?}");

    // ...while a dedicated blocking connection waits for the whole block time.
    let mut blocking = pool.blocking_conn(Duration::from_secs(1)).await.unwrap();
    let started = Instant::now();
    let popped: Option<(String, String)> = redis_rs::cmd("BRPOP")
        .arg("nothing")
        .arg(1)
        .query_async(&mut blocking)
        .await
        .unwrap();
    assert_eq!(popped, None);
    assert!(started.elapsed() >= Duration::from_millis(900));
}

#[tokio::test]
async fn blocking_connections_receive_pushed_values() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let mut blocking = pool.blocking_conn(Duration::from_secs(5)).await.unwrap();
    let waiter = tokio::spawn(async move {
        let popped: Option<(String, String)> = redis_rs::cmd("BRPOP")
            .arg("list")
            .arg(5)
            .query_async(&mut blocking)
            .await
            .unwrap();
        popped
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let _: i64 = cmd(
        &pool,
        redis_rs::cmd("LPUSH").arg("list").arg("value").clone(),
    )
    .await;
    let popped = tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(popped, Some(("list".to_owned(), "value".to_owned())));
}

#[tokio::test]
async fn with_retry_reconnects_once_after_a_failover_error() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let calls = AtomicUsize::new(0);
    let ids = Mutex::new(Vec::new());
    let value = pool
        .with_retry(|mut conn| {
            let (calls, ids) = (&calls, &ids);
            async move {
                let id: i64 = redis_rs::cmd("CLIENT")
                    .arg("ID")
                    .query_async(&mut conn)
                    .await?;
                ids.lock().unwrap().push(id);
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    Err(make_extension_error(
                        "READONLY".into(),
                        Some("replica".into()),
                    ))
                } else {
                    Ok("done")
                }
            }
        })
        .await
        .unwrap();
    assert_eq!(value, "done");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let ids = ids.into_inner().unwrap();
    assert_ne!(ids[0], ids[1], "the retry must use a fresh connection");
}

#[tokio::test]
async fn with_retry_gives_up_after_one_retry() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let calls = AtomicUsize::new(0);
    let result: hench::Result<()> = pool
        .with_retry(|_conn| {
            let calls = &calls;
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(make_extension_error("NOREPLICAS".into(), None))
            }
        })
        .await;
    assert!(matches!(result, Err(Error::Redis(_))));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn with_retry_does_not_retry_other_errors() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let calls = AtomicUsize::new(0);
    let result: hench::Result<()> = pool
        .with_retry(|mut conn| {
            let calls = &calls;
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                redis_rs::cmd("NOSUCHCOMMAND").query_async(&mut conn).await
            }
        })
        .await;
    assert!(matches!(result, Err(Error::Redis(_))));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn the_pool_reconnects_after_a_restart() {
    let mut redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let () = cmd(&pool, redis_rs::cmd("SET").arg("k").arg("v").clone()).await;
    redis.restart().await;
    let value: Option<String> = pool
        .with_retry(
            |mut conn| async move { redis_rs::cmd("GET").arg("k").query_async(&mut conn).await },
        )
        .await
        .unwrap();
    assert_eq!(value, None, "the restarted server is empty");
    pool.ping().await.unwrap();
}

#[tokio::test]
async fn stopped_servers_refuse_connections() {
    let mut redis = TestRedis::start().await;
    assert!(redis.is_running());
    redis.stop();
    assert!(!redis.is_running());
    let config = redis.config().reconnect_retries(0);
    assert!(matches!(
        RedisPool::from_config(config).await,
        Err(Error::Redis(_))
    ));
    redis.restart().await;
    assert!(redis.is_running());
    redis.pool().await.ping().await.unwrap();
}

#[tokio::test]
async fn handing_out_connections_never_waits_for_redis() {
    let mut redis = TestRedis::start().await;
    let pool = RedisPool::lazy(redis.config().reconnect_retries(1)).unwrap();
    redis.stop();
    // Many callers during an outage: none of them blocks on a connection attempt.
    let started = Instant::now();
    let connections = futures::future::join_all((0..50).map(|_| pool.conn())).await;
    assert!(connections.iter().all(Result::is_ok));
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "{:?}",
        started.elapsed()
    );
    // Commands fail while the server is down and succeed once it is back.
    assert!(pool.ping().await.is_err());
    redis.restart().await;
    wait_until(Duration::from_secs(10), || async {
        pool.ping().await.is_ok()
    })
    .await;
}

#[tokio::test]
async fn invalid_urls_fail_before_connecting() {
    let result = RedisPool::from_config(RedisConfig::new("mysql://localhost")).await;
    assert!(matches!(result, Err(Error::Config(_))));
}

/// Where `a_helper_holding_a_server` writes its server's pid and socket.
const HELPER_OUT: &str = "HENCH_TEST_HELPER_OUT";

/// Not a test of its own: run in a child process by
/// `test_servers_die_with_their_test_process`, which kills it.
#[tokio::test]
#[ignore = "run by test_servers_die_with_their_test_process"]
async fn a_helper_holding_a_server() {
    let Ok(out) = std::env::var(HELPER_OUT) else {
        return;
    };
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let info: String = cmd(&pool, redis_rs::cmd("INFO").arg("server").clone()).await;
    let pid = info
        .lines()
        .find_map(|line| line.strip_prefix("process_id:"))
        .unwrap()
        .trim()
        .to_owned();
    std::fs::write(&out, format!("{pid}\n{}", redis.socket_path().display())).unwrap();
    tokio::time::sleep(Duration::from_secs(120)).await;
}

/// A test process that dies without dropping its `TestRedis` (killed by an IDE's stop button,
/// the OOM killer, ...) leaves no `redis-server` behind, nor its directory.
#[cfg(unix)]
#[test]
fn test_servers_die_with_their_test_process() {
    let scratch = tempfile::tempdir().unwrap();
    let out = scratch.path().join("helper");
    let mut helper = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "a_helper_holding_a_server", "--ignored"])
        .env(HELPER_OUT, &out)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    let written = loop {
        match std::fs::read_to_string(&out) {
            Ok(written) if written.contains('\n') => break written,
            _ if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(20)),
            _ => panic!("the helper did not start its server"),
        }
    };
    let (pid, socket) = written.split_once('\n').unwrap();
    let directory = std::path::Path::new(socket).parent().unwrap().to_owned();
    let alive = |pid: &str| {
        std::process::Command::new("kill")
            .args(["-0", pid])
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap()
            .success()
    };
    assert!(alive(pid));
    assert!(directory.exists());
    // SIGKILL: no destructor runs in the helper.
    helper.kill().unwrap();
    helper.wait().unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while (alive(pid) || directory.exists()) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    let leaked = alive(pid);
    if leaked {
        let _ = std::process::Command::new("kill")
            .args(["-9", pid])
            .status();
    }
    assert!(!leaked, "redis-server {pid} outlived its test process");
    assert!(
        !directory.exists(),
        "{} was left behind",
        directory.display()
    );
}

/// Errors that no reconnection can fix fail at once (they used to go through the six default
/// reconnection attempts, 7.5 s): a wrong password, a database that does not exist.
#[tokio::test]
async fn permanent_connection_errors_fail_at_once() {
    let redis = TestRedis::start().await;
    let _: () = cmd(
        &redis.pool().await,
        redis_rs::cmd("CONFIG")
            .arg("SET")
            .arg("requirepass")
            .arg("sekret")
            .clone(),
    )
    .await;
    for url in [
        format!("{}?pass=wrong", redis.url()),
        format!("{}?pass=sekret&db=99", redis.url()),
    ] {
        let started = Instant::now();
        // Default reconnection settings: 6 attempts, 100 ms to 2 s apart.
        let result = RedisPool::from_config(RedisConfig::new(&url)).await;
        assert!(matches!(result, Err(Error::Redis(_))), "{url}: {result:?}");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "{url}: {:?}",
            started.elapsed()
        );
    }
    RedisPool::from_config(RedisConfig::new(format!("{}?pass=sekret", redis.url())))
        .await
        .unwrap()
        .ping()
        .await
        .unwrap();
}

/// An unreachable server is still tried again as configured.
#[tokio::test]
async fn unreachable_servers_are_tried_again() {
    let mut redis = TestRedis::start().await;
    let config = redis
        .config()
        .reconnect_retries(3)
        .reconnect_delays(Duration::from_millis(100), Duration::from_millis(100));
    redis.stop();
    let started = Instant::now();
    let result = RedisPool::from_config(config).await;
    assert!(matches!(result, Err(Error::Redis(_))), "{result:?}");
    assert!(started.elapsed() >= Duration::from_millis(300));
}
