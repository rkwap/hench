//! Limiters when replies are lost or the primary moves (`docs/design/ratelimit.md` §3.3.9,
//! §3.5.3, §2.11): an acquisition whose reply never arrives is sent again once over a new
//! connection; a concurrency limiter then grants the same lease again (its token), and a rate
//! limiter may charge the unit twice (under-admission) but never grants more than its limit.
//! After a failover, calls reconnect to the new primary, where the limits carried over.

mod support;

use std::time::Duration;

use hench::limit::Limiter;
use hench::test_util::TestRedis;
use hench::{Error, RedisConfig, RedisPool, keys};
use support::*;

/// The reply of a grant by `LIMIT_ACQUIRE` (`{1, reclaimed, held, mismatch, now}`).
const GRANT_REPLY: &[u8] = b"*5\r\n:1\r\n";

/// A pool whose connections go through `proxy`.
async fn through(proxy: &ReplyDropper) -> RedisPool {
    RedisPool::from_config(RedisConfig::new(proxy.url()))
        .await
        .unwrap()
}

#[tokio::test]
async fn a_lost_concurrency_grant_is_granted_again_on_the_same_slot() {
    init_logging();
    let redis = TestRedis::start().await;
    let direct = redis.pool().await;
    let proxy = ReplyDropper::start(redis.socket_path(), GRANT_REPLY).await;
    let pool = through(&proxy).await;
    let erp = Limiter::concurrent("erp", 2).max_wait(Duration::ZERO);
    // The grant's reply is lost: the call is sent again, with the same token.
    let first = erp.acquire(&pool).await.unwrap();
    assert_eq!(proxy.dropped(), 1);
    let state = keys::limit_state('c', "erp");
    assert_eq!(zcard(&direct, &state).await, 1, "one slot for one permit");
    // The second slot is still free; then the limit is reached.
    let second = erp.acquire(&direct).await.unwrap();
    assert!(matches!(
        erp.acquire(&direct).await,
        Err(Error::OverLimit(_))
    ));
    first.release().await.unwrap();
    second.release().await.unwrap();
    assert!(!exists(&direct, &state).await);
}

#[tokio::test]
async fn a_lost_rate_grant_never_over_admits() {
    init_logging();
    let redis = TestRedis::start().await;
    let direct = redis.pool().await;
    let proxy = ReplyDropper::start(redis.socket_path(), GRANT_REPLY).await;
    let pool = through(&proxy).await;
    let api = Limiter::window("api", 3, Duration::from_secs(60)).max_wait(Duration::ZERO);
    // The first grant's reply is lost and the call sent again: the unit is charged twice.
    let _first = api.acquire(&pool).await.unwrap();
    assert_eq!(proxy.dropped(), 1);
    let grants = keys::limit_state('w', "api");
    assert_eq!(
        llen(&direct, &grants).await,
        2,
        "the resent call charged again"
    );
    // One unit is left (under-admission), never more than the limit.
    let _second = api.acquire(&direct).await.unwrap();
    assert!(matches!(
        api.acquire(&direct).await,
        Err(Error::OverLimit(_))
    ));
    assert_eq!(llen(&direct, &grants).await, 3);
}

/// A free TCP port on the loopback interface.
fn free_port() -> String {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
        .to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn limits_carry_over_a_failover() {
    init_logging();
    let (port_a, port_b) = (free_port(), free_port());
    let a = TestRedis::start_with_args(&["--port", &port_a, "--bind", "127.0.0.1"]).await;
    let b = TestRedis::start_with_args(&["--port", &port_b, "--bind", "127.0.0.1"]).await;
    let (pool_a, pool_b) = (a.pool().await, b.pool().await);
    let replication =
        async |pool: &RedisPool| -> String { cmd(pool, &redis_cmd!("INFO", "replication")).await };
    let _: String = cmd(&pool_b, &redis_cmd!("REPLICAOF", "127.0.0.1", &port_a)).await;
    wait_for("the replica to sync", WAIT, || async {
        replication(&pool_b).await.contains("master_link_status:up")
    })
    .await;
    // The limiters reach the primary through a proxy that a failover moves (a virtual IP).
    let proxy = SwitchProxy::start(a.socket_path()).await;
    let pool = RedisPool::from_config(RedisConfig::new(proxy.url()))
        .await
        .unwrap();
    let api = Limiter::window("api", 3, Duration::from_secs(60)).max_wait(Duration::ZERO);
    let erp = Limiter::concurrent("erp", 1).max_wait(Duration::ZERO);
    let _first = api.acquire(&pool).await.unwrap();
    let held = erp.acquire(&pool).await.unwrap();
    // A coordinated failover: the replica is promoted, the old primary demoted (its clients
    // stay connected and get READONLY), the proxy moved to the new primary.
    let _: String = cmd(&pool_a, &redis_cmd!("FAILOVER")).await;
    wait_for("the failover", WAIT, || async {
        replication(&pool_a).await.contains("role:slave")
            && replication(&pool_b).await.contains("role:master")
    })
    .await;
    proxy.switch(b.socket_path());
    // The next calls reconnect to the new primary, where the state was replicated: the window
    // keeps its grant and the concurrency limit its holder.
    let _second = api.acquire(&pool).await.unwrap();
    let _third = api.acquire(&pool).await.unwrap();
    assert!(matches!(api.acquire(&pool).await, Err(Error::OverLimit(_))));
    assert!(matches!(erp.acquire(&pool).await, Err(Error::OverLimit(_))));
    assert_eq!(llen(&pool_b, &keys::limit_state('w', "api")).await, 3);
    // The permit taken before the failover is given back on the new primary.
    held.release().await.unwrap();
    assert!(!exists(&pool_b, &keys::limit_state('c', "erp")).await);
    let _again = erp.acquire(&pool).await.unwrap();
}
