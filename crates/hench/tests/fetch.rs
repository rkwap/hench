//! Fetching jobs (Sidekiq's `fetch_test.rb` scenarios, plus reliable fetch, paused queues,
//! queue ordering and orphan recovery).

mod support;

use std::collections::HashMap;
use std::time::{Duration, Instant};

use hench::fetch::{Fetcher, UnitOfWork};
use hench::test_util::TestRedis;
use hench::{CapsuleConfig, Config, FetchStrategy, HenchBuilder, RedisPool, keys};
use support::*;

/// The default capsule of a configuration built by `configure`.
async fn capsule(
    redis: &TestRedis,
    configure: impl FnOnce(HenchBuilder) -> HenchBuilder,
) -> CapsuleConfig {
    let hench = configure(redis.builder().await).build().await.unwrap();
    hench.config().capsules[0].clone()
}

fn fetcher(
    strategy: FetchStrategy,
    pool: &RedisPool,
    identity: &str,
    capsule: &CapsuleConfig,
) -> Fetcher {
    Fetcher::new(strategy, pool.clone(), identity, capsule).with_timeout(Duration::from_millis(200))
}

async fn rpush(pool: &RedisPool, key: &str, values: &[&str]) {
    for value in values {
        let _: i64 = cmd(pool, &redis_cmd!("RPUSH", &key, value)).await;
    }
}

fn job_json(queue: &str, marker: &str) -> String {
    let mut job = payload("Echo", vec![marker.into()], queue);
    job.jid = format!("{marker:0>24}");
    job.to_json().unwrap()
}

#[tokio::test]
async fn basic_fetch_retrieves_requeues_and_acknowledges() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    rpush(&pool, "queue:basic", &["msg"]).await;
    let capsule = capsule(&redis, |b| b.queues(["basic", "bar,3"])).await;
    let fetcher = fetcher(FetchStrategy::Basic, &pool, "me:1:a", &capsule);

    let work = fetcher.worker(0).retrieve_work().await.unwrap().unwrap();
    assert_eq!(work.queue_name(), "basic");
    assert_eq!(work.queue(), "queue:basic");
    assert_eq!(work.job(), b"msg");
    assert_eq!(queue_size(&pool, "basic").await, 0);
    fetcher.requeue(&work).await.unwrap();
    assert_eq!(queue_size(&pool, "basic").await, 1);
    fetcher.acknowledge(&work).await.unwrap();
    assert_eq!(queue_size(&pool, "basic").await, 1);
}

#[tokio::test]
async fn strict_ordering_checks_queues_in_the_configured_order() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let capsule = capsule(&redis, |b| b.queues(["basic", "bar"])).await;
    let fetcher = fetcher(FetchStrategy::Reliable, &pool, "me:1:a", &capsule);
    assert_eq!(fetcher.queue_order(), ["queue:basic", "queue:bar"]);

    for strategy in [FetchStrategy::Basic, FetchStrategy::Reliable] {
        let fetcher = self::fetcher(strategy, &pool, "me:1:a", &capsule);
        rpush(&pool, "queue:bar", &["bar-1", "bar-2"]).await;
        rpush(&pool, "queue:basic", &["basic-1"]).await;
        let mut worker = fetcher.worker(0);
        let mut fetched = Vec::new();
        while let Some(work) = worker.retrieve_work().await.unwrap() {
            fetched.push(work.job_str().into_owned());
            fetcher.acknowledge(&work).await.unwrap();
        }
        // `basic` first; each queue in FIFO order (RPUSH here means oldest first).
        assert_eq!(fetched, ["basic-1", "bar-2", "bar-1"], "{strategy:?}");
    }
}

#[tokio::test]
async fn weighted_ordering_follows_the_weights() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let capsule = capsule(&redis, |b| b.queues(["high,3", "mid,2", "low,1"])).await;
    for queue in ["high", "mid", "low"] {
        let jobs: Vec<String> = (0..300).map(|i| format!("{queue}-{i}")).collect();
        let jobs: Vec<&str> = jobs.iter().map(String::as_str).collect();
        rpush(&pool, &keys::queue(queue), &jobs).await;
    }
    let fetcher = fetcher(FetchStrategy::Reliable, &pool, "me:1:a", &capsule);
    let mut worker = fetcher.worker(0);
    let mut counts: HashMap<String, usize> = HashMap::new();
    for _ in 0..300 {
        let work = worker.retrieve_work().await.unwrap().unwrap();
        *counts.entry(work.queue_name().to_owned()).or_default() += 1;
        fetcher.acknowledge(&work).await.unwrap();
    }
    // Every queue stays non-empty, so each fetch takes from the first queue of a weighted
    // random order: about 150, 100 and 50 jobs.
    let high = counts["high"];
    let mid = counts["mid"];
    let low = counts["low"];
    assert!((110..=190).contains(&high), "{counts:?}");
    assert!((65..=135).contains(&mid), "{counts:?}");
    assert!((20..=80).contains(&low), "{counts:?}");
    assert!(high > low, "{counts:?}");
}

#[tokio::test]
async fn bulk_requeue_puts_jobs_back() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let capsule = capsule(&redis, |b| b.queues(["foo", "bar"])).await;
    for strategy in [FetchStrategy::Basic, FetchStrategy::Reliable] {
        rpush(&pool, "queue:foo", &["bob", "bar"]).await;
        rpush(&pool, "queue:bar", &["widget"]).await;
        let fetcher = fetcher(strategy, &pool, "me:1:a", &capsule);
        let mut worker = fetcher.worker(0);
        let mut works = Vec::new();
        for _ in 0..3 {
            works.push(worker.retrieve_work().await.unwrap().unwrap());
        }
        assert_eq!(queue_size(&pool, "foo").await, 0);
        assert_eq!(queue_size(&pool, "bar").await, 0);

        assert_eq!(fetcher.bulk_requeue(&works).await.unwrap(), 3);
        assert_eq!(queue_size(&pool, "foo").await, 2, "{strategy:?}");
        assert_eq!(queue_size(&pool, "bar").await, 1, "{strategy:?}");
        assert_eq!(llen(&pool, &keys::inprogress("me:1:a")).await, 0);
        assert_eq!(fetcher.bulk_requeue(&[]).await.unwrap(), 0);
        del(&pool, "queue:foo").await;
        del(&pool, "queue:bar").await;
    }
}

#[tokio::test]
async fn reliable_fetch_keeps_jobs_in_progress_until_acknowledged() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let capsule = capsule(&redis, |b| b).await;
    let fetcher = fetcher(FetchStrategy::Reliable, &pool, "me:1:a", &capsule);
    let inprogress = keys::inprogress("me:1:a");
    let job = job_json("default", "1");
    push_raw(&pool, "default", job.as_bytes()).await;

    let work = fetcher.worker(0).retrieve_work().await.unwrap().unwrap();
    assert_eq!(work.job(), job.as_bytes());
    assert_eq!(queue_size(&pool, "default").await, 0);
    assert_eq!(lrange(&pool, &inprogress).await, [job.as_bytes().to_vec()]);
    assert_eq!(smembers(&pool, keys::INPROGRESS).await, ["me:1:a"]);

    fetcher.acknowledge(&work).await.unwrap();
    assert_eq!(llen(&pool, &inprogress).await, 0);
    assert_eq!(queue_size(&pool, "default").await, 0);
}

#[tokio::test]
async fn reliable_requeue_moves_jobs_to_the_front_of_their_queue() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let capsule = capsule(&redis, |b| b).await;
    let fetcher = fetcher(FetchStrategy::Reliable, &pool, "me:1:a", &capsule);
    let inprogress = keys::inprogress("me:1:a");
    let first = job_json("default", "1");
    let second = job_json("default", "2");
    push_raw(&pool, "default", first.as_bytes()).await;
    push_raw(&pool, "default", second.as_bytes()).await;

    let mut worker = fetcher.worker(0);
    let work = worker.retrieve_work().await.unwrap().unwrap();
    assert_eq!(work.job(), first.as_bytes());
    fetcher.requeue(&work).await.unwrap();
    assert_eq!(llen(&pool, &inprogress).await, 0);
    // The requeued job is next again.
    let again = worker.retrieve_work().await.unwrap().unwrap();
    assert_eq!(again.job(), first.as_bytes());

    // Requeueing a job that is no longer in progress pushes nothing.
    fetcher.acknowledge(&again).await.unwrap();
    fetcher.requeue(&again).await.unwrap();
    assert_eq!(queue_size(&pool, "default").await, 1);
}

#[tokio::test]
async fn jobs_are_acknowledged_byte_for_byte() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let capsule = capsule(&redis, |b| b).await;
    let fetcher = fetcher(FetchStrategy::Reliable, &pool, "me:1:a", &capsule);
    let binary: Vec<u8> = vec![0xff, 0xfe, b'{', 0x00, b'}'];
    push_raw(&pool, "default", &binary).await;
    let work = fetcher.worker(0).retrieve_work().await.unwrap().unwrap();
    assert_eq!(work.job(), binary.as_slice());
    assert!(work.parse().is_err());
    fetcher.acknowledge(&work).await.unwrap();
    assert_eq!(llen(&pool, &keys::inprogress("me:1:a")).await, 0);
}

#[tokio::test]
async fn empty_queues_time_out() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let capsule = capsule(&redis, |b| b.queues(["a", "b"])).await;
    for strategy in [FetchStrategy::Basic, FetchStrategy::Reliable] {
        let fetcher = fetcher(strategy, &pool, "me:1:a", &capsule);
        let started = Instant::now();
        assert!(fetcher.worker(0).retrieve_work().await.unwrap().is_none());
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_millis(150) && elapsed < Duration::from_secs(3),
            "{strategy:?}: {elapsed:?}"
        );
    }
}

#[tokio::test]
async fn blocked_fetches_wake_up_for_new_jobs() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let capsule = capsule(&redis, |b| b.queues(["a", "b"])).await;
    for strategy in [FetchStrategy::Basic, FetchStrategy::Reliable] {
        let fetcher = Fetcher::new(strategy, pool.clone(), "me:1:a", &capsule)
            .with_timeout(Duration::from_secs(5));
        // Two workers block on the two queues (reliable fetch spreads them).
        let mut tasks = Vec::new();
        for index in 0..2 {
            let mut worker = fetcher.worker(index);
            tasks.push(tokio::spawn(async move { worker.retrieve_work().await }));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        let started = Instant::now();
        let job = job_json("b", "1");
        push_raw(&pool, "b", job.as_bytes()).await;
        let mut found = None;
        while found.is_none() {
            for task in &mut tasks {
                if task.is_finished() {
                    found = task.await.unwrap().unwrap();
                    break;
                }
            }
            assert!(
                started.elapsed() < Duration::from_secs(3),
                "{strategy:?}: no blocked worker woke up"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let work = found.unwrap();
        assert_eq!(work.queue_name(), "b");
        assert_eq!(work.job(), job.as_bytes());
        for task in tasks {
            task.abort();
        }
        fetcher.acknowledge(&work).await.unwrap();
    }
}

#[tokio::test]
async fn paused_queues_are_skipped() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let capsule = capsule(&redis, |b| b.queues(["a", "b"])).await;
    let _: i64 = cmd(&pool, &redis_cmd!("SADD", &keys::PAUSED, &"a")).await;
    for strategy in [FetchStrategy::Basic, FetchStrategy::Reliable] {
        push_raw(&pool, "a", job_json("a", "1").as_bytes()).await;
        push_raw(&pool, "b", job_json("b", "2").as_bytes()).await;
        let fetcher = fetcher(strategy, &pool, "me:1:a", &capsule);
        let mut worker = fetcher.worker(0);
        let work = worker.retrieve_work().await.unwrap().unwrap();
        assert_eq!(work.queue_name(), "b", "{strategy:?}");
        fetcher.acknowledge(&work).await.unwrap();
        assert!(
            worker.retrieve_work().await.unwrap().is_none(),
            "{strategy:?}"
        );
        assert_eq!(queue_size(&pool, "a").await, 1);
        del(&pool, "queue:a").await;
    }
}

#[tokio::test]
async fn fetching_waits_when_every_queue_is_paused() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let capsule = capsule(&redis, |b| b.queues(["a"])).await;
    let _: i64 = cmd(&pool, &redis_cmd!("SADD", &keys::PAUSED, &"a")).await;
    push_raw(&pool, "a", job_json("a", "1").as_bytes()).await;
    for strategy in [FetchStrategy::Basic, FetchStrategy::Reliable] {
        let fetcher = fetcher(strategy, &pool, "me:1:a", &capsule);
        let started = Instant::now();
        assert!(fetcher.worker(0).retrieve_work().await.unwrap().is_none());
        assert!(started.elapsed() >= Duration::from_millis(150));
        assert_eq!(queue_size(&pool, "a").await, 1);
    }
    // Unpausing makes the queue available again.
    let _: i64 = cmd(&pool, &redis_cmd!("SREM", &keys::PAUSED, &"a")).await;
    let fetcher = fetcher(FetchStrategy::Reliable, &pool, "me:1:a", &capsule);
    assert!(fetcher.worker(0).retrieve_work().await.unwrap().is_some());
}

/// Puts `jobs` in the in-progress list of `identity` and registers it as an owner.
async fn hold(pool: &RedisPool, identity: &str, jobs: &[&[u8]]) {
    for job in jobs {
        let _: i64 = cmd(pool, &redis_cmd!("LPUSH", &keys::inprogress(identity), job)).await;
    }
    let _: i64 = cmd(pool, &redis_cmd!("SADD", &keys::INPROGRESS, &identity)).await;
}

#[tokio::test]
async fn orphaned_jobs_are_recovered() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let capsule = capsule(&redis, |b| b).await;
    let config = Config::default();

    let a = job_json("a", "1");
    let b = job_json("b", "2");
    let garbage = b"{not json".to_vec();
    hold(&pool, "dead:1:x", &[a.as_bytes(), b.as_bytes(), &garbage]).await;
    let alive = job_json("c", "3");
    hold(&pool, "alive:2:y", &[alive.as_bytes()]).await;
    let _: i64 = cmd(&pool, &redis_cmd!("HSET", &"alive:2:y", &"beat", &1)).await;

    let fetcher = fetcher(FetchStrategy::Reliable, &pool, "me:3:z", &capsule);
    let recovered = fetcher
        .recover_orphans(Duration::from_secs(1), &config)
        .await
        .unwrap();
    assert_eq!(recovered, 3);
    assert_eq!(lrange(&pool, "queue:a").await, [a.as_bytes().to_vec()]);
    assert_eq!(lrange(&pool, "queue:b").await, [b.as_bytes().to_vec()]);
    let dead = zrange(&pool, keys::DEAD).await;
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].0, garbage);
    assert!(!exists(&pool, &keys::inprogress("dead:1:x")).await);
    // The live process is untouched.
    assert_eq!(smembers(&pool, keys::INPROGRESS).await, ["alive:2:y"]);
    assert_eq!(llen(&pool, &keys::inprogress("alive:2:y")).await, 1);
    // One recovery per lock period.
    let pttl = pttl(&pool, keys::INPROGRESS_RECOVERY).await;
    assert!(pttl > 0 && pttl <= 1_000, "{pttl}");
}

#[tokio::test]
async fn recovery_preserves_queue_order() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let capsule = capsule(&redis, |b| b).await;
    // The dead process fetched 1 then 2 (its list holds the newest first).
    let first = job_json("default", "1");
    let second = job_json("default", "2");
    hold(&pool, "dead:1:x", &[first.as_bytes(), second.as_bytes()]).await;
    let waiting = job_json("default", "3");
    push_raw(&pool, "default", waiting.as_bytes()).await;
    let fetcher = fetcher(FetchStrategy::Reliable, &pool, "me:3:z", &capsule);
    fetcher
        .recover_orphans(Duration::from_secs(1), &Config::default())
        .await
        .unwrap();
    let mut worker = fetcher.worker(0);
    let mut order = Vec::new();
    while let Some(work) = worker.retrieve_work().await.unwrap() {
        order.push(work.parse().unwrap().args[0].as_str().unwrap().to_owned());
        fetcher.acknowledge(&work).await.unwrap();
    }
    assert_eq!(order, ["1", "2", "3"]);
}

#[tokio::test]
async fn orphan_recovery_is_locked() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let capsule = capsule(&redis, |b| b).await;
    let config = Config::default();
    let fetcher = fetcher(FetchStrategy::Reliable, &pool, "me:3:z", &capsule);
    assert_eq!(
        fetcher
            .recover_orphans(Duration::from_millis(300), &config)
            .await
            .unwrap(),
        0
    );
    hold(&pool, "dead:1:x", &[job_json("a", "1").as_bytes()]).await;
    // The lock taken by the first check is still held.
    assert_eq!(
        fetcher
            .recover_orphans(Duration::from_millis(300), &config)
            .await
            .unwrap(),
        0
    );
    assert_eq!(queue_size(&pool, "a").await, 0);
    wait_for("the recovery lock to expire", WAIT, || async {
        !exists(&pool, keys::INPROGRESS_RECOVERY).await
    })
    .await;
    assert_eq!(
        fetcher
            .recover_orphans(Duration::from_millis(300), &config)
            .await
            .unwrap(),
        1
    );
    assert_eq!(queue_size(&pool, "a").await, 1);
}

#[tokio::test]
async fn basic_fetch_has_nothing_to_recover() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let capsule = capsule(&redis, |b| b).await;
    hold(&pool, "dead:1:x", &[job_json("a", "1").as_bytes()]).await;
    let fetcher = fetcher(FetchStrategy::Basic, &pool, "me:3:z", &capsule);
    assert_eq!(
        fetcher
            .recover_orphans(Duration::from_secs(1), &Config::default())
            .await
            .unwrap(),
        0
    );
    assert_eq!(llen(&pool, &keys::inprogress("dead:1:x")).await, 1);
}

#[tokio::test]
async fn units_of_work_can_be_built_by_hand() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let capsule = capsule(&redis, |b| b).await;
    let fetcher = fetcher(FetchStrategy::Basic, &pool, "me:1:a", &capsule);
    let work = UnitOfWork::new("queue:default", job_json("default", "1"));
    fetcher.requeue(&work).await.unwrap();
    assert_eq!(queue_size(&pool, "default").await, 1);
    assert_eq!(fetcher.strategy(), FetchStrategy::Basic);
    assert_eq!(fetcher.identity(), "me:1:a");
}

#[tokio::test]
async fn blocking_reliable_fetches_register_the_owner() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let capsule = capsule(&redis, |b| b).await;
    let fetcher = Fetcher::new(FetchStrategy::Reliable, pool.clone(), "me:1:a", &capsule)
        .with_timeout(Duration::from_secs(5));
    let mut worker = fetcher.worker(0);
    let task = tokio::spawn(async move { worker.retrieve_work().await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    // Nothing was found by the non-blocking step, so the owner is not registered yet.
    assert!(smembers(&pool, keys::INPROGRESS).await.is_empty());
    push_raw(&pool, "default", job_json("default", "1").as_bytes()).await;
    let work = task.await.unwrap().unwrap().unwrap();
    assert_eq!(smembers(&pool, keys::INPROGRESS).await, ["me:1:a"]);
    fetcher.acknowledge(&work).await.unwrap();
}

#[tokio::test]
async fn a_failing_owner_registration_never_drops_a_fetched_job() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let capsule = capsule(&redis, |b| b).await;
    // The owners set is broken: SADD fails with WRONGTYPE.
    let () = cmd(&pool, &redis_cmd!("SET", keys::INPROGRESS, "oops")).await;
    let fetcher = Fetcher::new(FetchStrategy::Reliable, pool.clone(), "me:1:a", &capsule)
        .with_timeout(Duration::from_secs(5));
    let mut worker = fetcher.worker(0);
    let task = tokio::spawn(async move { worker.retrieve_work().await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    let job = job_json("default", "1");
    push_raw(&pool, "default", job.as_bytes()).await;
    let work = task.await.unwrap().unwrap().unwrap();
    assert_eq!(work.job(), job.as_bytes());
    fetcher.acknowledge(&work).await.unwrap();
}

/// Basic fetch requeues a job to the *front* of its queue (`RPUSH`, DESIGN §2), ahead of jobs
/// pushed in the meantime, for one job and in bulk.
#[tokio::test]
async fn basic_requeue_puts_jobs_ahead_of_newer_ones() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let hench = redis.builder().await.build().await.unwrap();
    let capsule = hench.config().capsules[0].clone();
    let fetcher = Fetcher::new(FetchStrategy::Basic, pool.clone(), "me:1:a", &capsule)
        .with_timeout(Duration::from_millis(200));
    let mut worker = fetcher.worker(0);
    // LPUSH, like the client: `first` is the oldest.
    for job in ["first", "second"] {
        push_raw(&pool, "default", job.as_bytes()).await;
    }
    let work = worker.retrieve_work().await.unwrap().unwrap();
    assert_eq!(work.job(), b"first");
    push_raw(&pool, "default", b"third").await;
    fetcher.requeue(&work).await.unwrap();
    let again = worker.retrieve_work().await.unwrap().unwrap();
    assert_eq!(again.job_str(), "first", "a requeued job is fetched next");
    // In bulk: both requeued jobs come before a job pushed in the meantime.
    let other = worker.retrieve_work().await.unwrap().unwrap();
    assert_eq!(other.job_str(), "second");
    push_raw(&pool, "default", b"fourth").await;
    assert_eq!(fetcher.bulk_requeue(&[again, other]).await.unwrap(), 2);
    let mut next = Vec::new();
    for _ in 0..2 {
        next.push(
            worker
                .retrieve_work()
                .await
                .unwrap()
                .unwrap()
                .job_str()
                .into_owned(),
        );
    }
    next.sort();
    assert_eq!(next, ["first", "second"]);
    assert_eq!(llen(&pool, &keys::queue("default")).await, 2);
}

/// With an orphan grace, a process found without a heartbeat key is only taken for dead if it
/// still has none at the end of the grace: Redis may have lost the key of a live process, which
/// re-creates it within a heartbeat interval.
#[tokio::test]
async fn owners_without_a_heartbeat_get_the_orphan_grace() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let capsule = capsule(&redis, |b| b).await;
    let revived = job_json("a", "1");
    let dead = job_json("b", "2");
    hold(&pool, "revived:1:x", &[revived.as_bytes()]).await;
    hold(&pool, "dead:2:y", &[dead.as_bytes()]).await;
    let fetcher = fetcher(FetchStrategy::Reliable, &pool, "me:3:z", &capsule)
        .with_orphan_grace(Duration::from_millis(800));
    let started = Instant::now();
    let check = {
        let fetcher = fetcher.clone();
        tokio::spawn(async move {
            fetcher
                .recover_orphans(Duration::from_secs(5), &Config::default())
                .await
        })
    };
    // The first process re-creates its heartbeat key during the grace.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(queue_size(&pool, "b").await, 0, "nothing recovered yet");
    let _: i64 = cmd(&pool, &redis_cmd!("HSET", &"revived:1:x", &"liveness", &1)).await;
    assert_eq!(check.await.unwrap().unwrap(), 1);
    assert!(started.elapsed() >= Duration::from_millis(800));
    assert_eq!(lrange(&pool, "queue:b").await, [dead.as_bytes().to_vec()]);
    assert_eq!(queue_size(&pool, "a").await, 0);
    assert_eq!(
        lrange(&pool, &keys::inprogress("revived:1:x")).await,
        [revived.as_bytes().to_vec()]
    );
    assert_eq!(smembers(&pool, keys::INPROGRESS).await, ["revived:1:x"]);
}
