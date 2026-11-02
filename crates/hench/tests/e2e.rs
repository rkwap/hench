//! End-to-end throughput: thousands of jobs through running processes, with both fetch
//! strategies; every job must run exactly once and nothing may be left behind.

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::time::Duration;

use hench::prelude::*;
use hench::test_util::TestRedis;
use hench::{FetchStrategy, Hench, keys};
use support::*;

/// How many times each job ran.
struct Tally {
    counts: Vec<AtomicU32>,
    total: AtomicUsize,
}

impl Tally {
    fn new(jobs: usize) -> Arc<Self> {
        Arc::new(Self {
            counts: (0..jobs).map(|_| AtomicU32::new(0)).collect(),
            total: AtomicUsize::new(0),
        })
    }

    fn total(&self) -> usize {
        self.total.load(Ordering::SeqCst)
    }

    fn runs(&self) -> Vec<u32> {
        self.counts
            .iter()
            .map(|c| c.load(Ordering::SeqCst))
            .collect()
    }
}

/// Counts its run; fails the first attempt of every job whose index is a multiple of the
/// `Failing` data (if registered).
#[derive(Default)]
struct Count;

struct Failing(usize);

impl Job for Count {
    const NAME: &'static str = "Count";
    type Args = (usize,);

    async fn perform(&self, ctx: &JobContext, (index,): (usize,)) -> Result<(), JobError> {
        if let Ok(Failing(every)) = ctx.data::<Failing>()
            && index % every == 0
            && ctx.retry_count().is_none()
        {
            return Err(JobError::msg("first attempt fails"));
        }
        let tally = ctx.data::<Tally>()?;
        tally.counts[index].fetch_add(1, Ordering::SeqCst);
        tally.total.fetch_add(1, Ordering::SeqCst);
        // Yield so that the processors interleave.
        tokio::task::yield_now().await;
        Ok(())
    }
}

async fn push_all(hench: &Hench, range: std::ops::Range<usize>) {
    let args: Vec<(usize,)> = range.map(|index| (index,)).collect();
    for chunk in args.chunks(1_000) {
        let jids = Count::perform_bulk(hench.client(), chunk.to_vec())
            .await
            .unwrap();
        assert!(jids.iter().all(Option::is_some));
    }
}

async fn assert_clean(pool: &RedisPool, identities: &[String]) {
    assert_eq!(queue_size(pool, "default").await, 0);
    for identity in identities {
        assert!(!exists(pool, &keys::inprogress(identity)).await);
        assert!(!exists(pool, &keys::work(identity)).await);
    }
    assert!(smembers(pool, keys::INPROGRESS).await.is_empty());
    assert!(smembers(pool, keys::PROCESSES).await.is_empty());
    assert_eq!(zcard(pool, keys::DEAD).await, 0);
}

async fn every_job_runs_exactly_once(strategy: FetchStrategy) {
    const JOBS: usize = 5_000;
    init_logging();
    let redis = TestRedis::start().await;
    let tally = Tally::new(JOBS);
    let hench = redis
        .builder()
        .await
        .fetch(strategy)
        .concurrency(25)
        .job_logging(false)
        .register(Count)
        .data_arc(tally.clone())
        .build()
        .await
        .unwrap();
    let launcher = hench.start().await.unwrap();
    // Push while the processors are already fetching.
    push_all(&hench, 0..JOBS).await;
    wait_for("every job to run", Duration::from_secs(120), || async {
        tally.total() >= JOBS
    })
    .await;
    launcher.stop().await.unwrap();

    let runs = tally.runs();
    let wrong: Vec<(usize, u32)> = runs
        .iter()
        .enumerate()
        .filter(|(_, count)| **count != 1)
        .map(|(index, count)| (index, *count))
        .collect();
    assert!(
        wrong.is_empty(),
        "{strategy:?}: jobs not run exactly once: {wrong:?}"
    );
    let pool = hench.redis();
    assert_clean(pool, &[launcher.identity().to_owned()]).await;
    assert_eq!(zcard(pool, keys::RETRY).await, 0);
    assert_eq!(
        get_int(pool, keys::STAT_PROCESSED).await,
        i64::try_from(JOBS).unwrap()
    );
    assert_eq!(get_int(pool, keys::STAT_FAILED).await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reliable_fetch_runs_every_job_exactly_once() {
    every_job_runs_exactly_once(FetchStrategy::Reliable).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn basic_fetch_runs_every_job_exactly_once() {
    every_job_runs_exactly_once(FetchStrategy::Basic).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn several_processes_share_the_queues() {
    const JOBS: usize = 3_000;
    init_logging();
    let redis = TestRedis::start().await;
    let tally = Tally::new(JOBS);
    let mut processes = Vec::new();
    for strategy in [
        FetchStrategy::Reliable,
        FetchStrategy::Reliable,
        FetchStrategy::Basic,
    ] {
        let hench = redis
            .builder()
            .await
            .fetch(strategy)
            .concurrency(10)
            .job_logging(false)
            .register(Count)
            .data_arc(tally.clone())
            .build()
            .await
            .unwrap();
        let launcher = hench.start().await.unwrap();
        processes.push((hench, launcher));
    }
    push_all(&processes[0].0, 0..JOBS).await;
    wait_for("every job to run", Duration::from_secs(120), || async {
        tally.total() >= JOBS
    })
    .await;
    let mut identities = Vec::new();
    for (_, launcher) in &processes {
        identities.push(launcher.identity().to_owned());
        launcher.stop().await.unwrap();
    }
    assert!(tally.runs().iter().all(|count| *count == 1));
    let pool = processes[0].0.redis();
    assert_clean(pool, &identities).await;
    assert_eq!(
        get_int(pool, keys::STAT_PROCESSED).await,
        i64::try_from(JOBS).unwrap()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_jobs_are_retried_until_they_succeed() {
    const JOBS: usize = 500;
    init_logging();
    let redis = TestRedis::start().await;
    let tally = Tally::new(JOBS);
    let hench = redis
        .builder()
        .await
        .concurrency(10)
        .job_logging(false)
        .register(Count)
        .data(Failing(10))
        .data_arc(tally.clone())
        .build()
        .await
        .unwrap();
    let pool = hench.redis().clone();
    let launcher = hench.start().await.unwrap();
    // Retries are at least 15 s away; make them due right away.
    let rescheduler = tokio::spawn({
        let pool = pool.clone();
        async move {
            loop {
                for (member, _) in zrange(&pool, keys::RETRY).await {
                    let _: i64 =
                        cmd(&pool, &redis_cmd!("ZADD", keys::RETRY, "XX", 0, member)).await;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    });
    push_all(&hench, 0..JOBS).await;
    wait_for("every job to succeed", Duration::from_secs(60), || async {
        tally.total() >= JOBS
    })
    .await;
    rescheduler.abort();
    launcher.stop().await.unwrap();
    assert!(tally.runs().iter().all(|count| *count == 1));
    // 50 jobs failed once.
    assert_eq!(get_int(&pool, keys::STAT_FAILED).await, 50);
    assert_eq!(get_int(&pool, keys::STAT_PROCESSED).await, 550);
    assert_eq!(zcard(&pool, keys::RETRY).await, 0);
    assert_clean(&pool, &[launcher.identity().to_owned()]).await;
}
