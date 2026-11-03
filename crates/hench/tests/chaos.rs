//! Failure scenarios: crashed processes (reliable fetch recovers their jobs), Redis restarts
//! and outages while jobs run.

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::time::Duration;

use hench::prelude::*;
use hench::test_util::TestRedis;
use hench::{FetchStrategy, Hench, Launcher, keys};
use support::*;

/// How a `Crashy` job behaves in a process.
#[derive(Clone, Copy, PartialEq)]
enum Behaviour {
    /// Never finishes (the process will "crash" while running it).
    Hang,
    /// Finishes quickly.
    Finish,
}

/// Counts runs per index.
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

#[derive(Default)]
struct Crashy;

impl Job for Crashy {
    const NAME: &'static str = "Crashy";
    type Args = (usize,);

    async fn perform(&self, ctx: &JobContext, (index,): (usize,)) -> Result<(), JobError> {
        let runs = ctx.data::<Runs>()?;
        runs.record(format!("start:{index}"));
        if *ctx.data::<Behaviour>()? == Behaviour::Hang {
            std::future::pending::<()>().await;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
        ctx.data::<Tally>()?.counts[index].fetch_add(1, Ordering::SeqCst);
        ctx.data::<Tally>()?.total.fetch_add(1, Ordering::SeqCst);
        runs.record(format!("done:{index}"));
        Ok(())
    }
}

async fn process(
    redis: &TestRedis,
    behaviour: Behaviour,
    runs: &Arc<Runs>,
    tally: &Arc<Tally>,
    concurrency: usize,
) -> (Hench, Launcher) {
    init_logging();
    let hench = redis
        .builder()
        .await
        .concurrency(concurrency)
        .register(Crashy)
        .data(behaviour)
        .data_arc(runs.clone())
        .data_arc(tally.clone())
        .build()
        .await
        .unwrap();
    let launcher = hench.start().await.unwrap();
    (hench, launcher)
}

#[tokio::test]
async fn reliable_fetch_recovers_the_jobs_of_a_crashed_process() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let runs = Arc::new(Runs::default());
    let tally = Tally::new(1);
    let (hench, crashed) = process(&redis, Behaviour::Hang, &runs, &tally, 2).await;
    let jid = Crashy::perform_async(hench.client(), (0,))
        .await
        .unwrap()
        .unwrap();
    runs.wait_for(1).await;
    let dead = crashed.identity().to_owned();
    assert_eq!(llen(&pool, &keys::inprogress(&dead)).await, 1);

    // The process dies without any cleanup...
    crashed.abandon();
    assert!(crashed.is_stopped());
    assert_eq!(llen(&pool, &keys::inprogress(&dead)).await, 1);
    assert!(smembers(&pool, keys::INPROGRESS).await.contains(&dead));
    // ...and its heartbeat expires.
    del(&pool, &dead).await;

    let (_hench, survivor) = process(&redis, Behaviour::Finish, &runs, &tally, 2).await;
    wait_for("the recovered job to run", WAIT, || async {
        tally.total() == 1
    })
    .await;
    assert_eq!(runs.entries(), ["start:0", "start:0", "done:0"]);
    assert!(!exists(&pool, &keys::inprogress(&dead)).await);
    assert!(!smembers(&pool, keys::INPROGRESS).await.contains(&dead));
    survivor.stop().await.unwrap();
    assert_eq!(queue_size(&pool, "default").await, 0);
    let _ = jid;
}

#[tokio::test]
async fn live_processes_keep_their_jobs() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let runs = Arc::new(Runs::default());
    let tally = Tally::new(1);
    let (hench, busy) = process(&redis, Behaviour::Hang, &runs, &tally, 1).await;
    Crashy::perform_async(hench.client(), (0,)).await.unwrap();
    runs.wait_for(1).await;
    // Another process checks for orphans several times: the busy process is alive.
    let (_hench, other) = process(&redis, Behaviour::Finish, &runs, &tally, 1).await;
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    assert_eq!(llen(&pool, &keys::inprogress(busy.identity())).await, 1);
    assert_eq!(runs.len(), 1);
    other.stop().await.unwrap();
    busy.abandon();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_job_is_lost_when_a_process_crashes_mid_stream() {
    const JOBS: usize = 600;
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let runs = Arc::new(Runs::default());
    let tally = Tally::new(JOBS);
    // Both processes finish jobs; one of them will crash while running some.
    let (hench, crashing) = process(&redis, Behaviour::Finish, &runs, &tally, 5).await;
    let (_hench, survivor) = process(&redis, Behaviour::Finish, &runs, &tally, 5).await;
    let args: Vec<(usize,)> = (0..JOBS).map(|index| (index,)).collect();
    Crashy::perform_bulk(hench.client(), args).await.unwrap();
    wait_for("some progress", WAIT, || async { tally.total() > 100 }).await;
    crashing.abandon();
    del(&pool, crashing.identity()).await;
    wait_for("every job to run", Duration::from_secs(60), || async {
        tally.runs().iter().all(|count| *count >= 1)
    })
    .await;
    survivor.stop().await.unwrap();
    let total: u32 = tally.runs().iter().sum();
    // At least once: jobs finished but not yet acknowledged by the crashed process may run
    // twice, at most one per processor.
    let extra = usize::try_from(total).unwrap() - JOBS;
    assert!(extra <= 5, "{extra} duplicate runs");
    assert_eq!(queue_size(&pool, "default").await, 0);
    assert!(!exists(&pool, &keys::inprogress(crashing.identity())).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn processing_resumes_after_redis_restarts() {
    for strategy in [FetchStrategy::Reliable, FetchStrategy::Basic] {
        const JOBS: usize = 200;
        init_logging();
        let mut redis = TestRedis::start().await;
        let runs = Arc::new(Runs::default());
        let tally = Tally::new(JOBS);
        let events = Events::default();
        let hench = events
            .register(redis.builder().await)
            .fetch(strategy)
            .concurrency(5)
            .register(Crashy)
            .data(Behaviour::Finish)
            .data_arc(runs.clone())
            .data_arc(tally.clone())
            .build()
            .await
            .unwrap();
        let launcher = hench.start().await.unwrap();
        let pool = hench.redis().clone();
        push_range(&hench, 0..100).await;
        wait_for("the first half", WAIT, || async { tally.total() >= 100 }).await;

        // Redis restarts: everything it held is gone.
        redis.restart().await;
        // Jobs pushed after the restart all run.
        push_range(&hench, 100..JOBS).await;
        wait_for("the second half", Duration::from_secs(30), || async {
            tally.runs()[100..].iter().all(|count| *count == 1)
        })
        .await;
        // The process registered itself again.
        wait_for("the heartbeat to come back", WAIT, || async {
            exists(&pool, launcher.identity()).await
        })
        .await;
        wait_for("a second heartbeat event", WAIT, || async {
            events.count("heartbeat") >= 2
        })
        .await;
        launcher.stop().await.unwrap();
        assert!(
            tally.runs()[..100].iter().all(|count| *count == 1),
            "{strategy:?}"
        );
        assert_eq!(queue_size(&pool, "default").await, 0);
    }
}

/// Pushes `Crashy` jobs, retrying while the connection recovers from a restart.
async fn push_range(hench: &Hench, range: std::ops::Range<usize>) {
    for index in range {
        let mut attempts = 0;
        while let Err(error) = Crashy::perform_async(hench.client(), (index,)).await {
            attempts += 1;
            assert!(attempts < 100, "push keeps failing: {error}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outcomes_and_stats_survive_an_outage() {
    let mut redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    let gate = Arc::new(Gate::default());
    let hench = redis
        .builder()
        .await
        .register(Blocked)
        .register(Echo)
        .data_arc(runs.clone())
        .data_arc(gate.clone())
        .build()
        .await
        .unwrap();
    let launcher = hench.start().await.unwrap();
    let pool = hench.redis().clone();
    Blocked::perform_async(hench.client(), ("x".into(),))
        .await
        .unwrap();
    wait_for("the job to start", WAIT, || async { gate.entered() == 1 }).await;

    // Redis goes away while the job runs; the job finishes during the outage.
    redis.stop();
    gate.open();
    wait_for("the job to finish", WAIT, || async {
        runs.count("done:x") == 1
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    redis.restart().await;

    // The processors recover and the count of the job finished during the outage is
    // flushed by a later heartbeat.
    let mut attempts = 0;
    while Echo::perform_async(hench.client(), ("after".into(),))
        .await
        .is_err()
    {
        attempts += 1;
        assert!(attempts < 100);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    wait_for(
        "a job after the outage",
        Duration::from_secs(30),
        || async { runs.count("after") == 1 },
    )
    .await;
    wait_for("the stats to be flushed", WAIT, || async {
        get_int(&pool, keys::STAT_PROCESSED).await >= 2
    })
    .await;
    launcher.stop().await.unwrap();
    assert_eq!(get_int(&pool, keys::STAT_PROCESSED).await, 2);
}

#[tokio::test]
async fn shutdown_completes_while_redis_is_down() {
    let mut redis = TestRedis::start().await;
    let events = Events::default();
    let hench = events
        .register(redis.builder().await)
        .build()
        .await
        .unwrap();
    let launcher = hench.start().await.unwrap();
    redis.stop();
    tokio::time::timeout(Duration::from_secs(30), launcher.stop())
        .await
        .expect("the shutdown finishes")
        .unwrap();
    assert_eq!(
        events.milestones(),
        ["startup", "quiet", "shutdown", "exit"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_is_bounded_when_redis_stops_answering() {
    init_logging();
    let redis = TestRedis::start().await;
    let proxy = Proxy::start(redis.socket_path()).await;
    let runs = Arc::new(Runs::default());
    let gate = Arc::new(Gate::default());
    let events = Events::default();
    let config = hench::RedisConfig::new(proxy.url())
        .connection_timeout(Duration::from_millis(500))
        .response_timeout(Duration::from_millis(500))
        .reconnect_retries(1);
    let hench = events
        .register(Hench::builder())
        .redis_config(config)
        .heartbeat_interval(Duration::from_millis(100))
        .poll_interval_average(Duration::from_millis(100))
        .initial_scheduler_wait(Duration::ZERO)
        .initial_scheduler_jitter(Duration::ZERO)
        .fetch_timeout(Duration::from_millis(200))
        .timeout(Duration::from_millis(500))
        .register(Blocked)
        .data_arc(runs.clone())
        .data_arc(gate.clone())
        .build()
        .await
        .unwrap();
    let launcher = hench.start().await.unwrap();
    Blocked::perform_async(hench.client(), ("x".into(),))
        .await
        .unwrap();
    wait_for("the job to start", WAIT, || async { gate.entered() == 1 }).await;

    // Redis stops answering (a partition): the job completes, but nothing can be written.
    proxy.set_partitioned(true);
    gate.open();
    let started = std::time::Instant::now();
    tokio::time::timeout(Duration::from_secs(40), launcher.stop())
        .await
        .expect("the shutdown is bounded")
        .unwrap();
    let elapsed = started.elapsed();
    assert!(elapsed < Duration::from_secs(25), "{elapsed:?}");
    assert_eq!(
        events.milestones(),
        ["startup", "quiet", "shutdown", "exit"]
    );
    assert_eq!(runs.count("done:x"), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_job_is_lost_across_a_network_partition() {
    const JOBS: usize = 60;
    init_logging();
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let proxy = Proxy::start(redis.socket_path()).await;
    let runs = Arc::new(Runs::default());
    let tally = Tally::new(JOBS);
    let config = hench::RedisConfig::new(proxy.url())
        .connection_timeout(Duration::from_millis(500))
        .response_timeout(Duration::from_millis(500))
        .reconnect_retries(2);
    let hench = Hench::builder()
        .redis_config(config)
        .heartbeat_interval(Duration::from_millis(100))
        .poll_interval_average(Duration::from_millis(100))
        .initial_scheduler_wait(Duration::ZERO)
        .fetch_timeout(Duration::from_millis(200))
        .timeout(Duration::from_secs(2))
        .concurrency(4)
        .register(Crashy)
        .data(Behaviour::Finish)
        .data_arc(runs.clone())
        .data_arc(tally.clone())
        .build()
        .await
        .unwrap();
    let launcher = hench.start().await.unwrap();
    // Jobs are pushed directly (not through the partitioned proxy).
    let client = hench.client().with_redis(pool.clone());
    for index in 0..JOBS / 2 {
        Crashy::perform_async(&client, (index,)).await.unwrap();
    }
    wait_for("the first jobs", WAIT, || async {
        tally.total() >= JOBS / 2
    })
    .await;

    proxy.set_partitioned(true);
    for index in JOBS / 2..JOBS {
        Crashy::perform_async(&client, (index,)).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    proxy.set_partitioned(false);

    // Processing resumes after the partition heals.
    wait_for(
        "progress after the partition",
        Duration::from_secs(30),
        || async { tally.total() > JOBS / 2 },
    )
    .await;
    // Give stragglers a moment, then stop: replies lost during the partition can strand
    // fetched jobs in the in-progress list, which the shutdown hands back, so every job
    // either ran or is back in its queue.
    tokio::time::sleep(Duration::from_secs(2)).await;
    launcher.stop().await.unwrap();
    let ran: Vec<bool> = tally.runs().iter().map(|count| *count >= 1).collect();
    let queued: Vec<usize> = queue_jobs(&pool, "default")
        .await
        .iter()
        .map(|job| usize::try_from(job.args[0].as_u64().unwrap()).unwrap())
        .collect();
    for (index, ran) in ran.iter().enumerate() {
        assert!(
            *ran || queued.contains(&index),
            "job {index} was lost (queued: {queued:?})"
        );
    }
    assert!(!exists(&pool, &keys::inprogress(launcher.identity())).await);
}

/// Counts each run of job `index` in the `Tally`.
#[derive(Default)]
struct Counted;

impl Job for Counted {
    const NAME: &'static str = "Counted";
    type Args = (usize,);

    async fn perform(&self, ctx: &JobContext, (index,): (usize,)) -> Result<(), JobError> {
        let tally = ctx.data::<Tally>()?;
        tally.counts[index].fetch_add(1, Ordering::SeqCst);
        tally.total.fetch_add(1, Ordering::SeqCst);
        tokio::task::yield_now().await;
        Ok(())
    }
}

/// Redis stalls longer than the response timeout while processors fetch from a backlog: the
/// fetch scripts sent meanwhile time out on the client but still move a job each into the
/// process's in-progress list once Redis resumes. Those jobs are pushed back while the process
/// lives (they used to wait there until it stopped), and every job runs exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jobs_stranded_by_lost_fetch_replies_run_while_the_process_lives() {
    const JOBS: usize = 3_000;
    init_logging();
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    allow_long_scripts(&pool).await;
    let engine_pool =
        RedisPool::from_config(redis.config().response_timeout(Duration::from_millis(300)))
            .await
            .unwrap();
    let tally = Tally::new(JOBS);
    let hench = redis
        .builder()
        .await
        .redis(engine_pool)
        .concurrency(10)
        .job_logging(false)
        .register(Counted)
        .data_arc(tally.clone())
        .build()
        .await
        .unwrap();
    let args: Vec<(usize,)> = (0..JOBS).map(|index| (index,)).collect();
    for chunk in args.chunks(1_000) {
        Counted::perform_bulk(&redis.client().await, chunk.to_vec())
            .await
            .unwrap();
    }
    let launcher = hench.start().await.unwrap();
    let inprogress = keys::inprogress(launcher.identity());
    wait_for("the first jobs", WAIT, || async { tally.total() >= 300 }).await;
    let stall_pool =
        RedisPool::from_config(redis.config().response_timeout(Duration::from_secs(10)))
            .await
            .unwrap();
    let mut conn = stall_pool.conn().await.unwrap();
    let _: i64 = hench::redis_rs::cmd("EVAL")
        .arg(BUSY_LOOP)
        .arg(0)
        .arg(1_500)
        .query_async(&mut conn)
        .await
        .unwrap();
    wait_for("every job to run", Duration::from_secs(30), || async {
        tally.total() >= JOBS
    })
    .await;
    // Nothing is left in progress while the process is still running.
    wait_for("an empty in-progress list", WAIT, || async {
        llen(&pool, &inprogress).await == 0
    })
    .await;
    assert!(!launcher.is_stopping());
    launcher.stop().await.unwrap();
    let runs = tally.runs();
    assert!(runs.iter().all(|count| *count == 1), "{:?}", {
        let mut uneven: Vec<(usize, u32)> = runs
            .iter()
            .enumerate()
            .filter(|(_, count)| **count != 1)
            .map(|(index, count)| (index, *count))
            .collect();
        uneven.truncate(10);
        uneven
    });
    assert_eq!(queue_size(&pool, "default").await, 0);
}

/// Records which process ran a [`Slow`] job, and when.
#[derive(Default)]
struct Timeline(std::sync::Mutex<Vec<(&'static str, &'static str, std::time::Instant)>>);

impl Timeline {
    fn record(&self, who: &'static str, what: &'static str) {
        self.0
            .lock()
            .unwrap()
            .push((who, what, std::time::Instant::now()));
    }

    fn count(&self, what: &str) -> usize {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.1 == what)
            .count()
    }

    fn entries(&self) -> Vec<(&'static str, &'static str)> {
        self.0.lock().unwrap().iter().map(|e| (e.0, e.1)).collect()
    }
}

/// The name of the process running a job.
#[derive(Clone, Copy)]
struct Who(&'static str);

/// Runs for the given number of milliseconds.
#[derive(Default)]
struct Slow;

impl Job for Slow {
    const NAME: &'static str = "Slow";
    type Args = (u64,);

    async fn perform(&self, ctx: &JobContext, (millis,): (u64,)) -> Result<(), JobError> {
        let (who, timeline) = (ctx.data::<Who>()?.0, ctx.data::<Timeline>()?);
        timeline.record(who, "start");
        tokio::time::sleep(Duration::from_millis(millis)).await;
        timeline.record(who, "end");
        Ok(())
    }
}

/// After Redis loses its keys (a restart without persistence, an old snapshot, `FLUSHALL`), a
/// live process registers again as the owner of its in-progress jobs with its next fetch, but
/// its heartbeat key only comes back with its next beat (or liveness tick). An orphan check in
/// between (here: the start-up check of a process starting then) used to take it for dead
/// and push its running job back to the queue, where the other process ran it a second time,
/// at the same time. Owners without a heartbeat now get the orphan grace to re-create theirs.
/// A worker running [`Slow`] jobs, with a 1 s heartbeat and a 3 s orphan grace.
async fn slow_worker(redis: &TestRedis, who: &'static str, timeline: &Arc<Timeline>) -> Hench {
    redis
        .builder()
        .await
        .heartbeat_interval(Duration::from_secs(1))
        .orphan_grace(Duration::from_secs(3))
        .concurrency(1)
        .register(Slow)
        .auto_register(false)
        .data(Who(who))
        .data_arc(timeline.clone())
        .build()
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_live_process_keeps_its_jobs_after_redis_loses_its_keys() {
    init_logging();
    let mut redis = TestRedis::start().await;
    let timeline = Arc::new(Timeline::default());
    let a = slow_worker(&redis, "A", &timeline).await;
    let launcher_a = a.start().await.unwrap();
    let pool = a.redis().clone();
    let key = keys::process(launcher_a.identity());
    wait_for("A's heartbeat", WAIT, || async {
        exists(&pool, &key).await
    })
    .await;

    // Redis restarts without persistence: A's heartbeat key is gone with everything else.
    redis.restart().await;
    // A job pushed after the restart, taken by A.
    wait_for("Redis to accept pushes", WAIT, || async {
        Slow::perform_async(a.client(), (4_000,)).await.is_ok()
    })
    .await;
    wait_for("A to start the job", WAIT, || async {
        timeline.count("start") == 1
    })
    .await;
    assert!(
        smembers(&pool, keys::INPROGRESS)
            .await
            .contains(&launcher_a.identity().to_owned())
    );
    // B starts (a deploy, an autoscaler, a crashed worker restarted). Whether or not A's
    // heartbeat came back already, B's start-up check finds none: within the grace, A
    // re-creates it.
    let b = slow_worker(&redis, "B", &timeline).await;
    del(&pool, &key).await;
    let launcher_b = b.start().await.unwrap();
    wait_for("A to finish the job", WAIT, || async {
        timeline.count("end") == 1
    })
    .await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        timeline.entries(),
        [("A", "start"), ("A", "end")],
        "the job ran once, in A"
    );
    assert!(exists(&pool, &key).await);
    launcher_b.stop().await.unwrap();
    launcher_a.stop().await.unwrap();
}
