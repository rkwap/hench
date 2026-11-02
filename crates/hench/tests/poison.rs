//! Poison pills (reliable fetch): a job recovered from processes that died while they held it
//! more than `max_recoveries` times is moved to the morgue (`hench::CrashLoop`) instead of being
//! pushed back to kill another process; its batch counts it dead, its unique lock is released,
//! the death and error handlers of the recovering process run. Recoveries are counted per jid
//! for 72 hours after the last one; requeues at a shutdown are not recoveries. Against a private
//! `redis-server`.
#![cfg(feature = "test-util")]

mod support;

use std::sync::Arc;
use std::time::Duration;

use hench::api::Api;
use hench::batch::Batch;
use hench::fetch::{Fetcher, RECOVERY_WINDOW};
use hench::prelude::*;
use hench::test_util::TestRedis;
use hench::{Config, FetchStrategy, Hench, HenchBuilder, JobError, keys};
use support::*;

/// Hangs forever once it started, as a job that kills its process would: the test then kills
/// (abandons) the process. Unique for a minute, keyed by its argument.
#[derive(Default)]
struct Pill;

impl Job for Pill {
    const NAME: &'static str = "Pill";
    type Args = (u32,);

    fn options() -> JobOptions {
        JobOptions::new().unique_for(Duration::from_secs(60))
    }

    async fn perform(&self, ctx: &JobContext, (marker,): (u32,)) -> Result<(), JobError> {
        ctx.data::<Runs>()?.record(format!("start:{marker}"));
        std::future::pending::<()>().await;
        Ok(())
    }
}

/// The settings of a worker built for these tests (`configure` adds to them), with the death and
/// error handlers recording into `deaths` and `errors`.
async fn worker_config(
    redis: &TestRedis,
    runs: &Arc<Runs>,
    deaths: &Deaths,
    errors: &Errors,
    configure: impl FnOnce(HenchBuilder) -> HenchBuilder,
) -> Hench {
    configure(
        redis
            .builder()
            .await
            .register(Pill)
            .data_arc(runs.clone())
            .death_handler(deaths.handler())
            .error_handler(errors.handler()),
    )
    .build()
    .await
    .unwrap()
}

/// A fetcher of the process `me:1:a` (alive for the checks: it is not in `inprogress`).
fn recovering_fetcher(pool: &RedisPool, config: &Config) -> Fetcher {
    Fetcher::new(
        FetchStrategy::Reliable,
        pool.clone(),
        "me:1:a",
        &config.capsules[0],
    )
}

/// A process `owner` fetched the job at the front of `queue` and died while it held it.
async fn crash_with(pool: &RedisPool, queue: &str, owner: &str) -> Vec<u8> {
    let job: Option<Vec<u8>> = cmd(
        pool,
        &redis_cmd!(
            "LMOVE",
            &keys::queue(queue),
            &keys::inprogress(owner),
            &"RIGHT",
            &"LEFT"
        ),
    )
    .await;
    let _: i64 = cmd(pool, &redis_cmd!("SADD", &keys::INPROGRESS, &owner)).await;
    job.expect("a job to crash with")
}

/// One orphan check (whatever the lock of the previous one): how many jobs it recovered.
async fn check(pool: &RedisPool, fetcher: &Fetcher, config: &Config) -> usize {
    del(pool, keys::INPROGRESS_RECOVERY).await;
    fetcher
        .recover_orphans(Duration::from_secs(60), config)
        .await
        .unwrap()
}

/// The recovery counter of `jid`: its value and remaining time to live.
async fn counter(pool: &RedisPool, jid: &str) -> (Option<String>, i64) {
    let key = keys::recoveries(jid);
    let value: Option<String> = cmd(pool, &redis_cmd!("GET", &key)).await;
    (value, pttl(pool, &key).await)
}

#[tokio::test]
async fn a_job_recovered_more_than_max_recoveries_times_goes_to_the_morgue() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let (deaths, errors) = (Deaths::default(), Errors::default());
    let runs = Arc::new(Runs::default());
    let hench = worker_config(&redis, &runs, &deaths, &errors, |b| b.max_recoveries(2)).await;
    let config = hench.config();
    assert_eq!(config.max_recoveries, 2);
    let fetcher = recovering_fetcher(&pool, config);
    let mut job = payload("Echo", vec!["x".into()], "default");
    job.jid = "0123456789abcdef01234567".to_owned();
    let jid = hench.client().push(job).await.unwrap().unwrap();

    // Two crashes: the job goes back to its queue, byte for byte, its recoveries counted.
    for recovery in 1..=2_u32 {
        let fetched = crash_with(&pool, "default", &format!("dead:{recovery}:x")).await;
        assert_eq!(check(&pool, &fetcher, config).await, 1);
        assert_eq!(lrange(&pool, &keys::queue("default")).await, [fetched]);
        let (count, ttl) = counter(&pool, &jid).await;
        assert_eq!(count, Some(recovery.to_string()));
        let window = i64::try_from(RECOVERY_WINDOW.as_millis()).unwrap();
        assert!(ttl > window - 60_000 && ttl <= window, "{ttl}");
    }
    assert!(deaths.all().is_empty());

    // The third: a poison pill, moved to the morgue instead of crashing another process.
    crash_with(&pool, "default", "dead:3:x").await;
    assert_eq!(check(&pool, &fetcher, config).await, 1);
    assert_eq!(queue_size(&pool, "default").await, 0);
    assert!(!exists(&pool, &keys::inprogress("dead:3:x")).await);
    assert!(smembers(&pool, keys::INPROGRESS).await.is_empty());
    let dead = sorted_jobs(&pool, keys::DEAD).await;
    assert_eq!(dead.len(), 1);
    let (buried, _) = &dead[0];
    assert_eq!(buried.jid, jid);
    assert_eq!(buried.error_class.as_deref(), Some(JobError::CRASH_LOOP));
    let message = buried.error_message.as_deref().unwrap_or_default();
    assert!(message.contains("recovered 3 times"), "{message}");
    assert!(message.contains("max_recoveries is 2"), "{message}");
    assert!(buried.failed_at.is_some());
    // The recovering process ran its death and error handlers.
    let deaths = deaths.all();
    assert_eq!(deaths.len(), 1);
    assert_eq!(deaths[0].0.jid, jid);
    assert_eq!(
        deaths[0].0.error_class.as_deref(),
        Some(JobError::CRASH_LOOP)
    );
    let reported = errors.with_context("job recovered too many times");
    assert_eq!(
        reported,
        [(
            "job recovered too many times".to_owned(),
            JobError::CRASH_LOOP.to_owned(),
            Some(jid.clone())
        )]
    );
    // The counter (the recoveries that pushed the job back) stays until its window ends: a copy
    // retried from the morgue that still crashes its process goes back there at its next
    // recovery.
    assert_eq!(counter(&pool, &jid).await.0, Some("2".to_owned()));
}

#[tokio::test]
async fn max_recoveries_zero_never_runs_a_job_again_after_its_process_died() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let (deaths, errors) = (Deaths::default(), Errors::default());
    let runs = Arc::new(Runs::default());
    let hench = worker_config(&redis, &runs, &deaths, &errors, |b| b.max_recoveries(0)).await;
    let config = hench.config();
    let fetcher = recovering_fetcher(&pool, config);
    hench
        .client()
        .push(payload("Echo", vec!["x".into()], "default"))
        .await
        .unwrap();
    crash_with(&pool, "default", "dead:1:x").await;
    assert_eq!(check(&pool, &fetcher, config).await, 1);
    assert_eq!(queue_size(&pool, "default").await, 0);
    assert_eq!(zcard(&pool, keys::DEAD).await, 1);
    assert_eq!(deaths.all().len(), 1);
}

#[tokio::test]
async fn a_dead_false_poison_pill_is_dropped_and_its_death_handlers_run() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let (deaths, errors) = (Deaths::default(), Errors::default());
    let runs = Arc::new(Runs::default());
    let hench = worker_config(&redis, &runs, &deaths, &errors, |b| b.max_recoveries(0)).await;
    let config = hench.config();
    let fetcher = recovering_fetcher(&pool, config);
    let mut job = payload("Echo", vec!["x".into()], "default");
    job.dead = Some(false);
    let jid = hench.client().push(job).await.unwrap().unwrap();
    crash_with(&pool, "default", "dead:1:x").await;
    assert_eq!(check(&pool, &fetcher, config).await, 1);
    assert_eq!(queue_size(&pool, "default").await, 0);
    assert_eq!(
        zcard(&pool, keys::DEAD).await,
        0,
        "dead: false keeps it out"
    );
    assert!(!exists(&pool, &keys::inprogress("dead:1:x")).await);
    let deaths = deaths.all();
    assert_eq!(deaths.len(), 1);
    assert_eq!(deaths[0].0.jid, jid);
    assert!(deaths[0].0.discarded_at.is_some());
}

#[tokio::test]
async fn a_poison_pill_releases_its_unique_lock_and_counts_dead_in_its_batch() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let (deaths, errors) = (Deaths::default(), Errors::default());
    let runs = Arc::new(Runs::default());
    let hench = worker_config(&redis, &runs, &deaths, &errors, |b| b.max_recoveries(0)).await;
    let config = hench.config();
    let client = hench.client();
    let fetcher = recovering_fetcher(&pool, config);

    // A unique job: its lock goes with it.
    let jid = Pill::perform_async(client, (1,)).await.unwrap().unwrap();
    let digest =
        hench::unique::digest(Pill::NAME, Some("default"), &serde_json::json!([1])).unwrap();
    let lock = keys::unique_lock(&digest);
    assert_eq!(hgetall(&pool, &lock).await.get("jid"), Some(&jid));
    crash_with(&pool, "default", "dead:1:x").await;
    assert_eq!(check(&pool, &fetcher, config).await, 1);
    assert!(
        !exists(&pool, &lock).await,
        "the poison pill released its lock"
    );
    assert!(Pill::perform_async(client, (1,)).await.unwrap().is_some());
    cmd::<i64>(&pool, &redis_cmd!("DEL", &keys::queue("default"))).await;

    // A batch member: its batch counts it dead and fires `death`.
    let committed = Batch::new(client)
        .jobs(|jobs| async move { Pill::perform_async(&jobs, (2,)).await })
        .await
        .unwrap();
    crash_with(&pool, "default", "dead:2:x").await;
    assert_eq!(check(&pool, &fetcher, config).await, 1);
    let status = Api::new(pool.clone())
        .batches()
        .fetch(&committed.bid)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((status.total, status.pending, status.dead), (1, 1, 1));
    assert!(status.death_at.is_some());
    let failed = Api::new(pool.clone())
        .batches()
        .failures(&committed.bid, 1, 10)
        .await
        .unwrap()
        .items;
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].error_class.as_deref(), Some(JobError::CRASH_LOOP));
    assert_eq!(failed[0].reason, "morgue");
    assert!(failed[0].dead);
    assert_eq!(deaths.all().len(), 2);
}

#[tokio::test]
async fn jobs_pushed_back_at_a_shutdown_are_not_counted() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let (deaths, errors) = (Deaths::default(), Errors::default());
    let runs = Arc::new(Runs::default());
    let hench = worker_config(&redis, &runs, &deaths, &errors, |b| {
        b.max_recoveries(0).timeout(Duration::from_millis(300))
    })
    .await;
    let jid = Pill::perform_async(hench.client(), (1,))
        .await
        .unwrap()
        .unwrap();
    let launcher = hench.start().await.unwrap();
    runs.wait_for(1).await;
    launcher.stop().await.unwrap();
    // The hard shutdown pushed the job back to its queue: not a recovery from a dead process.
    assert_eq!(queue_jobs(&pool, "default").await[0].jid, jid);
    assert_eq!(counter(&pool, &jid).await.0, None);
    assert!(deaths.all().is_empty());
}

/// The job kills every worker that runs it (the test abandons the process as soon as the job
/// starts, as an out-of-memory kill would): after `max_recoveries` (2) recoveries by the next
/// workers' orphan checks, the next one moves it to the morgue instead of running it again.
#[tokio::test]
async fn a_job_that_kills_its_workers_ends_in_the_morgue() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let (deaths, errors) = (Deaths::default(), Errors::default());
    let jid = Pill::perform_async(&redis.client().await, (7,))
        .await
        .unwrap()
        .unwrap();
    // Workers 1 to 3 run it and die; worker 4's orphan check finds it recovered twice already.
    for worker in 1..=4 {
        let runs = Arc::new(Runs::default());
        let hench = worker_config(&redis, &runs, &deaths, &errors, |b| b.max_recoveries(2)).await;
        let launcher = hench.start().await.unwrap();
        if worker < 4 {
            // Fetched (for workers 2 and 3, once their orphan check recovered it), then run.
            runs.wait_for(1).await;
            // The job "kills" its worker: no cleanup, and its heartbeat key expires.
            launcher.abandon();
            del(&pool, launcher.identity()).await;
            continue;
        }
        wait_for(
            "the poison pill in the morgue",
            Duration::from_secs(20),
            || async { zcard(&pool, keys::DEAD).await == 1 },
        )
        .await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        launcher.stop().await.unwrap();
        assert!(
            runs.entries().is_empty(),
            "the poison pill did not run again"
        );
    }
    let dead = sorted_jobs(&pool, keys::DEAD).await;
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].0.jid, jid);
    assert_eq!(dead[0].0.error_class.as_deref(), Some(JobError::CRASH_LOOP));
    assert_eq!(deaths.all().len(), 1);
    assert_eq!(queue_size(&pool, "default").await, 0);
    assert_eq!(counter(&pool, &jid).await.0, Some("2".to_owned()));
    assert!(smembers(&pool, keys::INPROGRESS).await.is_empty());
}

// ---------------------------------------------------------------------------------------------
// Review round 4 (release): suspects isolated (crashloop-1), progress forgets recoveries
// (crashloop-2), each poison pill settled once (crashloop-4).
// ---------------------------------------------------------------------------------------------

/// The jids of the jobs now in `queue`, in the order they are fetched.
async fn queued_jids(pool: &RedisPool, queue: &str) -> Vec<String> {
    queue_jobs(pool, queue)
        .await
        .into_iter()
        .map(|job| job.jid)
        .collect()
}

/// Pushes `count` jobs to `default` with recognizable jids (`…a1`, `…a2`, …), fetched in that
/// order.
async fn push_numbered(client: &Client, count: usize) -> Vec<String> {
    let mut jids = Vec::new();
    for n in 1..=count {
        let mut job = payload("Echo", vec![n.to_string().into()], "default");
        job.jid = format!("{:0>22}a{n}", "");
        jids.push(client.push(job).await.unwrap().unwrap());
    }
    jids
}

/// crashloop-1: jobs that were in flight at an earlier crash too (suspects) come back one per
/// orphan check, the most recently fetched first, so that the next crash tells the job that
/// kills processes from the jobs that merely ran beside it. Jobs at their first crash come back
/// together, at once, as before.
#[tokio::test]
async fn suspects_come_back_one_per_orphan_check() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let (deaths, errors) = (Deaths::default(), Errors::default());
    let runs = Arc::new(Runs::default());
    let hench = worker_config(&redis, &runs, &deaths, &errors, |b| b.max_recoveries(3)).await;
    let config = hench.config();
    let fetcher = recovering_fetcher(&pool, config);
    let jids = push_numbered(hench.client(), 5).await;

    // The first crash: all five come back at once, in the order they were fetched.
    for _ in 0..5 {
        crash_with(&pool, "default", "dead:1:x").await;
    }
    assert_eq!(check(&pool, &fetcher, config).await, 5);
    assert_eq!(queued_jids(&pool, "default").await, jids);
    for jid in &jids {
        assert_eq!(counter(&pool, jid).await.0.as_deref(), Some("1"));
    }

    // The process that fetched them again dies too: they are suspects now. They leave the dead
    // process's list for the suspects, and one comes back per check (the last one fetched
    // first), its recovery counted then.
    for _ in 0..5 {
        crash_with(&pool, "default", "dead:2:x").await;
    }
    let mut released = Vec::new();
    for round in 1..=5_usize {
        assert_eq!(check(&pool, &fetcher, config).await, 1, "round {round}");
        assert!(!exists(&pool, &keys::inprogress("dead:2:x")).await);
        assert!(smembers(&pool, keys::INPROGRESS).await.is_empty());
        let queued = queued_jids(&pool, "default").await;
        assert_eq!(queued.len(), 1, "round {round}: {queued:?}");
        assert_eq!(counter(&pool, &queued[0]).await.0.as_deref(), Some("2"));
        assert_eq!(zcard(&pool, keys::RECOVERY_SUSPECTS).await, 5 - round);
        released.push(queued[0].clone());
        // It runs and finishes before the next check.
        cmd::<i64>(&pool, &redis_cmd!("DEL", &keys::queue("default"))).await;
    }
    let mut newest_first = jids.clone();
    newest_first.reverse();
    assert_eq!(released, newest_first);
    for jid in &jids[..4] {
        assert_eq!(counter(&pool, jid).await.0.as_deref(), Some("2"));
    }
    assert_eq!(check(&pool, &fetcher, config).await, 0);
    assert_eq!(zcard(&pool, keys::DEAD).await, 0);
    assert!(deaths.all().is_empty());
}

/// crashloop-1: when several jobs of one crash reach the limit together, hench cannot tell
/// which one kills processes: each gets one more recovery, alone. The one that crashes its
/// process again, alone, is the poison pill; the others run.
#[tokio::test]
async fn jobs_that_reach_the_limit_together_get_one_more_recovery_alone() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let (deaths, errors) = (Deaths::default(), Errors::default());
    let runs = Arc::new(Runs::default());
    let hench = worker_config(&redis, &runs, &deaths, &errors, |b| b.max_recoveries(1)).await;
    let config = hench.config();
    let fetcher = recovering_fetcher(&pool, config);
    let jids = push_numbered(hench.client(), 3).await;
    for _ in 0..3 {
        crash_with(&pool, "default", "dead:1:x").await;
    }
    assert_eq!(check(&pool, &fetcher, config).await, 3);
    // Crashed together a second time: all three are past `max_recoveries` (1), together.
    for _ in 0..3 {
        crash_with(&pool, "default", "dead:2:x").await;
    }
    assert_eq!(check(&pool, &fetcher, config).await, 1);
    assert_eq!(zcard(&pool, keys::DEAD).await, 0, "none buried yet");
    let suspect = queued_jids(&pool, "default").await;
    assert_eq!(suspect, [jids[2].clone()]);
    // It kills the next process too, alone this time: the poison pill.
    crash_with(&pool, "default", "dead:3:x").await;
    assert_eq!(
        check(&pool, &fetcher, config).await,
        2,
        "the pill and the next suspect"
    );
    let dead = sorted_jobs(&pool, keys::DEAD).await;
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].0.jid, jids[2]);
    assert_eq!(dead[0].0.error_class.as_deref(), Some(JobError::CRASH_LOOP));
    let message = dead[0].0.error_message.as_deref().unwrap_or_default();
    assert!(message.contains("recovered 3 times"), "{message}");
    assert!(
        message.contains("no other job was in flight at its last crash"),
        "{message}"
    );
    // The other two come back, one per check, and run.
    assert_eq!(queued_jids(&pool, "default").await, [jids[1].clone()]);
    cmd::<i64>(&pool, &redis_cmd!("DEL", &keys::queue("default"))).await;
    assert_eq!(check(&pool, &fetcher, config).await, 1);
    assert_eq!(queued_jids(&pool, "default").await, [jids[0].clone()]);
    assert_eq!(deaths.all().len(), 1);
    assert!(smembers(&pool, keys::INPROGRESS).await.is_empty());
}

/// crashloop-1: the error of a poison pill says how many other jobs were in flight when it
/// crashed its process the last time, so that the jobs buried beside a pill can be told apart.
#[tokio::test]
async fn the_crash_loop_error_counts_the_jobs_in_flight_beside_the_pill() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let (deaths, errors) = (Deaths::default(), Errors::default());
    let runs = Arc::new(Runs::default());
    let hench = worker_config(&redis, &runs, &deaths, &errors, |b| b.max_recoveries(1)).await;
    let config = hench.config();
    let fetcher = recovering_fetcher(&pool, config);
    let jids = push_numbered(hench.client(), 1).await;
    crash_with(&pool, "default", "dead:1:x").await;
    assert_eq!(check(&pool, &fetcher, config).await, 1);
    // Its second crash, beside two jobs at their first.
    let client = hench.client();
    for n in 7..=8 {
        client
            .push(payload("Echo", vec![n.to_string().into()], "default"))
            .await
            .unwrap();
    }
    for _ in 0..3 {
        crash_with(&pool, "default", "dead:2:x").await;
    }
    assert_eq!(check(&pool, &fetcher, config).await, 3);
    let dead = sorted_jobs(&pool, keys::DEAD).await;
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].0.jid, jids[0]);
    let message = dead[0].0.error_message.as_deref().unwrap_or_default();
    assert!(
        message.contains("2 other jobs were in flight at its last crash"),
        "{message}"
    );
    // The two others are back in their queue.
    assert_eq!(queue_size(&pool, "default").await, 2);
}

/// Sleeps `ms`, then records `done:<marker>` (and `start:<marker>` before).
#[derive(Default)]
struct Slow;

impl Job for Slow {
    const NAME: &'static str = "Slow";
    type Args = (u32, u64);

    async fn perform(&self, ctx: &JobContext, (marker, ms): (u32, u64)) -> Result<(), JobError> {
        ctx.data::<Runs>()?.record(format!("start:{marker}"));
        tokio::time::sleep(Duration::from_millis(ms)).await;
        ctx.data::<Runs>()?.record(format!("done:{marker}"));
        Ok(())
    }
}

/// crashloop-1 with real workers: a single process at a time (a small deployment), the poison
/// pill and four jobs that take longer than the pill takes to kill their process, fetched
/// together. Each process is killed as soon as the pill starts in it. Only the pill ends in the
/// morgue; the four others run to completion (they used to be buried with it).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jobs_that_ran_beside_a_poison_pill_are_not_buried_with_it() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let (deaths, errors) = (Deaths::default(), Errors::default());
    let client = redis.client().await;
    for marker in 1..=2 {
        Slow::perform_async(&client, (marker, 1_500)).await.unwrap();
    }
    Pill::perform_async(&client, (0,)).await.unwrap();
    for marker in 3..=4 {
        Slow::perform_async(&client, (marker, 1_500)).await.unwrap();
    }
    let done = Arc::new(Runs::default());
    let mut killed = 0;
    loop {
        let runs = Arc::new(Runs::default());
        let hench = redis
            .builder()
            .await
            .register(Pill)
            .register(Slow)
            .data_arc(runs.clone())
            .death_handler(deaths.handler())
            .error_handler(errors.handler())
            .max_recoveries(2)
            .concurrency(5)
            .build()
            .await
            .unwrap();
        let launcher = hench.start().await.unwrap();
        // The pill starts in this process (and kills it), or is buried by its orphan check.
        wait_for(
            "the pill to start or be buried",
            Duration::from_secs(60),
            || async { runs.count("start:0") > 0 || zcard(&pool, keys::DEAD).await > 0 },
        )
        .await;
        if runs.count("start:0") > 0 {
            launcher.abandon();
            del(&pool, launcher.identity()).await;
            killed += 1;
            for entry in runs.entries() {
                done.record(entry);
            }
            assert!(killed <= 6, "the pill was not buried: {:?}", done.entries());
            continue;
        }
        wait_for(
            "the four others to finish",
            Duration::from_secs(60),
            || async {
                (1..=4).all(|marker| {
                    runs.count(&format!("done:{marker}")) + done.count(&format!("done:{marker}"))
                        > 0
                })
            },
        )
        .await;
        launcher.stop().await.unwrap();
        break;
    }
    let dead = sorted_jobs(&pool, keys::DEAD).await;
    let buried: Vec<&str> = dead.iter().map(|(job, _)| job.class.as_str()).collect();
    assert_eq!(buried, ["Pill"], "only the pill is buried");
    assert_eq!(deaths.all().len(), 1);
    assert_eq!(dead[0].0.error_class.as_deref(), Some(JobError::CRASH_LOOP));
}

/// Walks 0..10 000, an item every 20 ms, saving its cursor every 100 ms.
#[derive(Default)]
struct Walk;

impl hench::iterable::IterableJob for Walk {
    const NAME: &'static str = "Walk";
    type Args = (u32,);
    type Item = u64;
    type Cursor = u64;

    fn build_enumerator<'a>(
        &'a self,
        _ctx: &'a JobContext,
        _args: &'a (u32,),
        cursor: Option<u64>,
    ) -> Result<hench::iterable::Enumerator<'a, u64, u64>, JobError> {
        Ok(hench::iterable::Enumerator::range(0..10_000, cursor))
    }

    async fn each_iteration(
        &self,
        item: u64,
        it: &hench::iterable::Iteration<'_, Self>,
    ) -> Result<(), JobError> {
        it.ctx().data::<Runs>()?.record(format!("item:{item}"));
        tokio::time::sleep(Duration::from_millis(20)).await;
        Ok(())
    }

    fn checkpoint_interval(&self) -> Duration {
        Duration::from_millis(100)
    }
}

/// Hangs at its first item: an iterable job that kills its process before it makes progress.
#[derive(Default)]
struct StuckWalk;

impl hench::iterable::IterableJob for StuckWalk {
    const NAME: &'static str = "StuckWalk";
    type Args = (u32,);
    type Item = u64;
    type Cursor = u64;

    fn build_enumerator<'a>(
        &'a self,
        _ctx: &'a JobContext,
        _args: &'a (u32,),
        cursor: Option<u64>,
    ) -> Result<hench::iterable::Enumerator<'a, u64, u64>, JobError> {
        Ok(hench::iterable::Enumerator::range(0..10, cursor))
    }

    async fn each_iteration(
        &self,
        item: u64,
        it: &hench::iterable::Iteration<'_, Self>,
    ) -> Result<(), JobError> {
        it.ctx().data::<Runs>()?.record(format!("stuck:{item}"));
        std::future::pending::<()>().await;
        Ok(())
    }

    fn checkpoint_interval(&self) -> Duration {
        Duration::from_millis(100)
    }
}

/// A worker for the iterable jobs (`max_recoveries` 1).
async fn iterable_worker(redis: &TestRedis, runs: &Arc<Runs>, deaths: &Deaths) -> Hench {
    redis
        .builder()
        .await
        .register(Walk)
        .register(StuckWalk)
        .data_arc(runs.clone())
        .death_handler(deaths.handler())
        .max_recoveries(1)
        .build()
        .await
        .unwrap()
}

/// crashloop-2: an iterable job that makes progress between crashes of its processes (each
/// execution saves a cursor beyond the one it started from) is not taking them down: its
/// recoveries are forgotten with its progress, and unrelated crashes never bury it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_iterable_job_that_makes_progress_between_crashes_is_not_a_poison_pill() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let deaths = Deaths::default();
    let jid = Walk::perform_async(&redis.client().await, (1,))
        .await
        .unwrap()
        .unwrap();
    // Four processes in a row die while running it (max_recoveries is 1), each after the job
    // processed (and saved) items.
    for worker in 1..=4 {
        let runs = Arc::new(Runs::default());
        let hench = iterable_worker(&redis, &runs, &deaths).await;
        let launcher = hench.start().await.unwrap();
        wait_for("progress", Duration::from_secs(30), || async {
            runs.len() >= 15
        })
        .await;
        // Let a checkpoint save the cursor, then crash.
        tokio::time::sleep(Duration::from_millis(250)).await;
        launcher.abandon();
        del(&pool, launcher.identity()).await;
        assert_eq!(zcard(&pool, keys::DEAD).await, 0, "worker {worker}");
    }
    // The next process recovers it once more, and it goes on from its cursor.
    let runs = Arc::new(Runs::default());
    let hench = iterable_worker(&redis, &runs, &deaths).await;
    let launcher = hench.start().await.unwrap();
    wait_for(
        "progress after the last crash",
        Duration::from_secs(30),
        || async { runs.len() >= 5 },
    )
    .await;
    launcher.stop().await.unwrap();
    assert_eq!(zcard(&pool, keys::DEAD).await, 0);
    assert!(deaths.all().is_empty());
    assert!(
        counter(&pool, &jid).await.0.is_none(),
        "the progress since its last recovery forgot the count"
    );
}

/// crashloop-2: an iterable job that kills its processes before it saves any progress is a
/// poison pill like any other job.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_iterable_job_that_crashes_before_any_progress_is_a_poison_pill() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let deaths = Deaths::default();
    let jid = StuckWalk::perform_async(&redis.client().await, (1,))
        .await
        .unwrap()
        .unwrap();
    for worker in 1..=3 {
        let runs = Arc::new(Runs::default());
        let hench = iterable_worker(&redis, &runs, &deaths).await;
        let launcher = hench.start().await.unwrap();
        if worker < 3 {
            wait_for("the stuck item", Duration::from_secs(30), || async {
                runs.count("stuck:0") > 0
            })
            .await;
            tokio::time::sleep(Duration::from_millis(250)).await;
            launcher.abandon();
            del(&pool, launcher.identity()).await;
            continue;
        }
        wait_for(
            "the pill in the morgue",
            Duration::from_secs(30),
            || async { zcard(&pool, keys::DEAD).await == 1 },
        )
        .await;
        launcher.stop().await.unwrap();
    }
    let dead = sorted_jobs(&pool, keys::DEAD).await;
    assert_eq!(dead[0].0.jid, jid);
    assert_eq!(dead[0].0.error_class.as_deref(), Some(JobError::CRASH_LOOP));
    assert_eq!(deaths.all().len(), 1);
}

/// crashloop-4: two orphan checks of one dead list overlap (the first one's lock expired while
/// it recovered): each poison pill reaches the morgue once, its batch counts it dead once, and
/// its death and error handlers run once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overlapping_recoveries_settle_each_poison_pill_once() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let (deaths, errors) = (Deaths::default(), Errors::default());
    let runs = Arc::new(Runs::default());
    let hench = worker_config(&redis, &runs, &deaths, &errors, |b| b).await;
    let config = hench.config();
    let client = hench.client();
    for marker in 0..20 {
        Pill::perform_async(client, (marker,)).await.unwrap();
    }
    let committed = Batch::new(client)
        .jobs(|jobs| async move {
            for marker in 100..120 {
                Pill::perform_async(&jobs, (marker,)).await?;
            }
            Ok::<(), hench::Error>(())
        })
        .await
        .unwrap();
    let queued = queue_jobs(&pool, "default").await;
    assert_eq!(queued.len(), 40);
    // Each one recovered beyond max_recoveries (3) already (it had its recovery more, alone):
    // the next recovery buries it, whatever crashed with it.
    for job in &queued {
        cmd::<()>(&pool, &redis_cmd!("SET", &keys::recoveries(&job.jid), &"4")).await;
    }
    for _ in 0..40 {
        crash_with(&pool, "default", "dead:1:x").await;
    }
    let a = Fetcher::new(
        FetchStrategy::Reliable,
        pool.clone(),
        "me:1:a",
        &config.capsules[0],
    );
    let b = Fetcher::new(
        FetchStrategy::Reliable,
        pool.clone(),
        "me:2:b",
        &config.capsules[0],
    );
    // A takes the lock for 1 ms only: B takes it while A is still recovering.
    let (ra, rb) = tokio::join!(a.recover_orphans(Duration::from_millis(1), config), async {
        tokio::time::sleep(Duration::from_millis(3)).await;
        b.recover_orphans(Duration::from_millis(1), config).await
    });
    let recovered = ra.unwrap() + rb.unwrap();
    // Whatever either left behind (it stopped once the other held the lock) is recovered by the
    // next check.
    if recovered < 40 {
        del(&pool, keys::INPROGRESS_RECOVERY).await;
        a.recover_orphans(Duration::from_secs(60), config)
            .await
            .unwrap();
    }
    assert_eq!(
        zcard(&pool, keys::DEAD).await,
        40,
        "one morgue entry per pill"
    );
    let mut per_jid = std::collections::HashMap::<String, usize>::new();
    for (job, _) in deaths.all() {
        *per_jid.entry(job.jid).or_default() += 1;
    }
    assert_eq!(per_jid.len(), 40);
    assert!(
        per_jid.values().all(|calls| *calls == 1),
        "death handlers once per pill: {per_jid:?}"
    );
    assert_eq!(
        errors.with_context("job recovered too many times").len(),
        40
    );
    let status = Api::new(pool.clone())
        .batches()
        .fetch(&committed.bid)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.dead, 20, "the batch counts each member dead once");
}

/// crashloop-4: the reply to a poison pill's claim (the first step of its settlement) is lost
/// and the claim sent again: the pill is buried once and its handlers run once.
#[tokio::test]
async fn a_lost_claim_reply_buries_the_poison_pill_once() {
    let redis = TestRedis::start().await;
    let direct = redis.pool().await;
    let (deaths, errors) = (Deaths::default(), Errors::default());
    let runs = Arc::new(Runs::default());
    let hench = worker_config(&redis, &runs, &deaths, &errors, |b| b).await;
    let config = hench.config().clone();
    let mut job = payload("Echo", vec!["x".into()], "default");
    job.jid = "0123456789abcdef01234567".to_owned();
    let jid = hench.client().push(job).await.unwrap().unwrap();
    cmd::<()>(&direct, &redis_cmd!("SET", &keys::recoveries(&jid), &"3")).await;
    crash_with(&direct, "default", "dead:1:x").await;
    // A fresh claim answers 2 (a claim renewed by its holder 1, a refusal 0).
    let proxy = ReplyDropper::start(redis.socket_path(), b":2\r\n").await;
    let pool = RedisPool::connect(&proxy.url()).await.unwrap();
    let fetcher = recovering_fetcher(&pool, &config);
    assert_eq!(
        fetcher
            .recover_orphans(Duration::from_secs(60), &config)
            .await
            .unwrap(),
        1
    );
    assert_eq!(proxy.dropped(), 1, "the claim's reply was lost");
    assert_eq!(zcard(&direct, keys::DEAD).await, 1);
    assert_eq!(deaths.all().len(), 1);
    assert_eq!(errors.with_context("job recovered too many times").len(), 1);
}

/// crashloop-4: a poison pill another live process is settling (it claimed it) is left to that
/// process; a claim of the same job in another dead process's list (a pill revived from the
/// morgue that crashed again), or one this process holds already, does not hold it back.
#[tokio::test]
async fn a_poison_pill_claimed_by_another_process_is_left_to_it() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let (deaths, errors) = (Deaths::default(), Errors::default());
    let runs = Arc::new(Runs::default());
    let hench = worker_config(&redis, &runs, &deaths, &errors, |b| b.max_recoveries(0)).await;
    let config = hench.config();
    let fetcher = recovering_fetcher(&pool, config);
    let jid = push_numbered(hench.client(), 1).await.remove(0);
    crash_with(&pool, "default", "dead:1:x").await;
    let claim = keys::recovery_claim(&jid);
    cmd::<()>(
        &pool,
        &redis_cmd!("HSET", &claim, &"of", &"dead:1:x", &"by", &"other:2:y"),
    )
    .await;
    cmd::<()>(&pool, &redis_cmd!("PEXPIRE", &claim, &60_000)).await;
    assert_eq!(check(&pool, &fetcher, config).await, 0);
    assert_eq!(zcard(&pool, keys::DEAD).await, 0);
    assert!(deaths.all().is_empty());
    assert_eq!(llen(&pool, &keys::inprogress("dead:1:x")).await, 1);
    // A claim of an earlier burial (another dead list) does not hold it back.
    cmd::<()>(
        &pool,
        &redis_cmd!("HSET", &claim, &"of", &"dead:0:w", &"by", &"other:2:y"),
    )
    .await;
    assert_eq!(check(&pool, &fetcher, config).await, 1);
    assert_eq!(zcard(&pool, keys::DEAD).await, 1);
    assert_eq!(deaths.all().len(), 1);
    let recorded = hgetall(&pool, &claim).await;
    assert_eq!(recorded.get("of").map(String::as_str), Some("dead:1:x"));
    assert_eq!(recorded.get("by").map(String::as_str), Some("me:1:a"));
    let ttl = pttl(&pool, &claim).await;
    assert!(ttl > 0, "the claim expires: {ttl}");
}
