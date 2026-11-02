//! Redis Sentinel (`redis+sentinel://` URLs, Sidekiq's `sentinels:` configuration), against a
//! real primary, replica and sentinel (`redis-server --sentinel`): clients and workers find
//! the primary through the sentinel, and follow it when a failover moves it.

mod support;

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use hench::prelude::*;
use hench::{Error, Hench, RedisConfig, redis_rs};
use support::*;

/// How long a sentinel may take to see a replica or to fail over.
const SENTINEL_WAIT: Duration = Duration::from_secs(30);

/// A `redis-server` process, killed when dropped.
struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn redis_server() -> PathBuf {
    std::env::var_os("HENCH_REDIS_SERVER").map_or_else(|| "redis-server".into(), PathBuf::from)
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn spawn(dir: &Path, args: &[&str]) -> Server {
    Server(
        Command::new(redis_server())
            .args(args)
            .current_dir(dir)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("redis-server"),
    )
}

/// A plain Redis server on `port` (`--replicaof` arguments may follow).
fn redis_on(dir: &Path, port: u16, extra: &[&str]) -> Server {
    let port = port.to_string();
    let mut args = vec![
        "--port",
        port.as_str(),
        "--bind",
        "127.0.0.1",
        "--save",
        "",
        "--appendonly",
        "no",
    ];
    args.extend_from_slice(extra);
    spawn(dir, &args)
}

async fn query<T: redis_rs::FromRedisValue>(
    port: u16,
    command: &redis_rs::Cmd,
) -> redis_rs::RedisResult<T> {
    let client = redis_rs::Client::open(format!("redis://127.0.0.1:{port}"))?;
    let mut conn = client.get_multiplexed_async_connection().await?;
    command.query_async(&mut conn).await
}

/// A field of the server's `INFO replication`.
async fn replication(port: u16, field: &str) -> Option<String> {
    let info: String = query(port, redis_rs::cmd("INFO").arg("replication"))
        .await
        .ok()?;
    info.lines()
        .find_map(|line| line.strip_prefix(&format!("{field}:")))
        .map(|value| value.trim().to_owned())
}

/// The port of the primary the sentinel on `port` names.
async fn named_primary(port: u16) -> Option<u16> {
    let addr: Vec<String> = query(
        port,
        redis_rs::cmd("SENTINEL")
            .arg("GET-MASTER-ADDR-BY-NAME")
            .arg("mymaster"),
    )
    .await
    .ok()?;
    addr.get(1)?.parse().ok()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clients_and_workers_follow_the_primary_through_a_failover() {
    init_logging();
    let dir = tempfile::tempdir().unwrap();
    let (first, second, watcher) = (free_port(), free_port(), free_port());
    let first_port = first.to_string();
    let _first = redis_on(dir.path(), first, &[]);
    let _second = redis_on(
        dir.path(),
        second,
        &["--replicaof", "127.0.0.1", first_port.as_str()],
    );
    wait_for("the replication", WAIT, || async {
        replication(second, "master_link_status").await.as_deref() == Some("up")
    })
    .await;
    std::fs::write(
        dir.path().join("sentinel.conf"),
        format!(
            "port {watcher}\nbind 127.0.0.1\ndir {}\n\
             sentinel monitor mymaster 127.0.0.1 {first} 1\n\
             sentinel down-after-milliseconds mymaster 1000\n\
             sentinel failover-timeout mymaster 5000\n",
            dir.path().display()
        ),
    )
    .unwrap();
    let _sentinel = spawn(dir.path(), &["sentinel.conf", "--sentinel"]);
    wait_for("the sentinel to see the replica", SENTINEL_WAIT, || async {
        let replicas: redis_rs::RedisResult<Vec<redis_rs::Value>> = query(
            watcher,
            redis_rs::cmd("SENTINEL").arg("REPLICAS").arg("mymaster"),
        )
        .await;
        replicas.is_ok_and(|replicas| replicas.len() == 1)
    })
    .await;

    let config = RedisConfig::new(format!(
        "redis+sentinel://127.0.0.1:{watcher},127.0.0.1:{}/mymaster",
        free_port()
    ))
    .connection_timeout(Duration::from_secs(2))
    .reconnect_retries(2);
    // A wrong service name fails at once, saying so.
    let unknown = RedisConfig::new(format!("redis+sentinel://127.0.0.1:{watcher}/nosuch"))
        .reconnect_retries(2);
    let result = RedisPool::from_config(unknown).await;
    assert!(
        matches!(&result, Err(Error::Redis(error))
            if error.to_string().contains("no sentinel monitors") && error.to_string().contains("nosuch")),
        "{result:?}"
    );

    let runs = Arc::new(Runs::default());
    let hench = Hench::builder()
        .redis(RedisPool::from_config(config.clone()).await.unwrap())
        .heartbeat_interval(Duration::from_millis(100))
        .poll_interval_average(Duration::from_millis(100))
        .fetch_timeout(Duration::from_millis(100))
        .systemd(false)
        .auto_register(false)
        .register(Echo)
        .data_arc(runs.clone())
        .build()
        .await
        .unwrap();
    let launcher = hench.start().await.unwrap();
    let client = Client::new(RedisPool::from_config(config).await.unwrap());
    Echo::perform_async(&client, ("before".into(),))
        .await
        .unwrap();
    runs.wait_for(1).await;

    // The sentinel promotes the replica (once it has heard from it: until then it has no
    // replica it trusts, NOGOODSLAVE). It reconfigures the old primary as the new one's
    // replica soon after; the test does it at once.
    wait_for(
        "the sentinel to start a failover",
        SENTINEL_WAIT,
        || async {
            let started: redis_rs::RedisResult<String> = query(
                watcher,
                redis_rs::cmd("SENTINEL").arg("FAILOVER").arg("mymaster"),
            )
            .await;
            if started.is_err() {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            started.is_ok()
        },
    )
    .await;
    wait_for(
        "the sentinel to name the new primary",
        SENTINEL_WAIT,
        || async { named_primary(watcher).await == Some(second) },
    )
    .await;
    let _: String = query(
        first,
        redis_rs::cmd("REPLICAOF").arg("127.0.0.1").arg(second),
    )
    .await
    .unwrap();

    // The old primary refuses the client's next push (READONLY): the client asks the
    // sentinel again and pushes to the new primary.
    for i in 0..3 {
        Echo::perform_async(&client, (format!("after {i}"),))
            .await
            .unwrap();
    }
    // The workers follow too: they run the jobs pushed to the new primary...
    wait_for(
        "the jobs pushed after the failover",
        SENTINEL_WAIT,
        || async { (0..3).all(|i| runs.count(&format!("after {i}")) == 1) },
    )
    .await;
    // ...and beat there.
    let beat = || async {
        let beat: Option<String> = query(
            second,
            redis_rs::cmd("HGET").arg(launcher.identity()).arg("beat"),
        )
        .await
        .ok()
        .flatten();
        beat.and_then(|beat| beat.parse::<f64>().ok())
    };
    let seen = beat().await.unwrap_or_default();
    wait_for("a heartbeat on the new primary", WAIT, || async {
        beat().await.is_some_and(|beat| beat > seen)
    })
    .await;
    launcher.stop().await.unwrap();
    assert_eq!(runs.len(), 4);
}
