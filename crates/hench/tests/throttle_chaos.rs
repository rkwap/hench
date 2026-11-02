//! Throttles and limiters under concurrency and failures (`docs/design/ratelimit.md` §3.4.10,
//! §3.5): several processes sharing one throttle and one limiter, a holder whose renewals stop
//! (an overage), a Redis restart without persistence, Redis unreachable during the gate
//! (fail closed, or open with `fail_open`), and a reschedule whose reply is lost.

mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use hench::limit::{Limiter, OverLimit, Throttle};
use hench::prelude::*;
use hench::test_util::TestRedis;
use hench::{Hench, HenchBuilder, Launcher, RedisConfig, keys};
use support::*;

/// The window limiter the jobs of [`Shared`] call: 20 calls per second, shared by every process.
static API: LazyLock<Limiter> = LazyLock::new(|| {
    Limiter::window("chaos-api", 20, Duration::from_secs(1)).max_wait(Duration::from_secs(5))
});

/// What the jobs of the several processes saw.
#[derive(Default)]
struct Probe {
    running: AtomicUsize,
    most: AtomicUsize,
    grants: Mutex<Vec<Instant>>,
    done: Mutex<Vec<u64>>,
}

/// One running execution, counted until dropped.
struct Running<'a>(&'a Probe);

impl Drop for Running<'_> {
    fn drop(&mut self) {
        self.0.running.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Probe {
    fn enter(&self) -> Running<'_> {
        let running = self.running.fetch_add(1, Ordering::SeqCst) + 1;
        self.most.fetch_max(running, Ordering::SeqCst);
        Running(self)
    }

    fn done(&self) -> usize {
        self.done.lock().unwrap().len()
    }
}

/// A job of the throttle `shared` (3 at a time across every process) calling the window
/// limiter [`API`].
struct Shared;

impl Job for Shared {
    const NAME: &'static str = "chaos::Shared";
    type Args = (u64,);

    fn options() -> JobOptions {
        JobOptions::new().throttle("shared")
    }

    async fn perform(&self, ctx: &JobContext, (index,): Self::Args) -> Result<(), JobError> {
        let probe = ctx.data::<Probe>()?;
        let _running = probe.enter();
        let _permit = API.acquire(ctx).await?;
        probe.grants.lock().unwrap().push(Instant::now());
        tokio::time::sleep(Duration::from_millis(20)).await;
        probe.done.lock().unwrap().push(index);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn processes_share_their_throttles_and_limiters() {
    const PROCESSES: usize = 4;
    const JOBS: u64 = 40;
    init_logging();
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let probe = Arc::new(Probe::default());
    let mut launchers: Vec<Launcher> = Vec::new();
    for _ in 0..PROCESSES {
        let hench = redis
            .builder()
            .await
            .concurrency(4)
            .register(Shared)
            .throttle(
                Throttle::new("shared")
                    .concurrency(3)
                    .max_wait(Duration::from_secs(5)),
            )
            .data_arc(probe.clone())
            .build()
            .await
            .unwrap();
        launchers.push(hench.start().await.unwrap());
    }
    let client = Client::new(pool.clone());
    for index in 0..JOBS {
        Shared::perform_async(&client, (index,)).await.unwrap();
    }
    wait_for("every job", Duration::from_secs(40), || async {
        probe.done() == JOBS as usize
    })
    .await;
    for launcher in launchers {
        launcher.stop().await.unwrap();
    }
    // Each job ran to its end once.
    let mut done = probe.done.lock().unwrap().clone();
    done.sort_unstable();
    done.dedup();
    assert_eq!(done.len(), JOBS as usize);
    // Never more than three at a time, across the four processes.
    let most = probe.most.load(Ordering::SeqCst);
    assert!((1..=3).contains(&most), "{most} at once");
    // The limiter: never more than 20 grants in a second (the grants are timed by this
    // process, a little after Redis decided them: a margin for that).
    let mut grants = probe.grants.lock().unwrap().clone();
    grants.sort_unstable();
    assert!(grants.len() >= JOBS as usize);
    for window in grants.windows(21) {
        let apart = window[20].duration_since(window[0]);
        assert!(
            apart >= Duration::from_millis(800),
            "21 grants in {apart:?}"
        );
    }
    // Every slot was given back, none was lost or reclaimed.
    assert!(!exists(&pool, &keys::limit_state('c', "shared")).await);
    let stats = hgetall(&pool, &keys::limit_stats('c', "shared")).await;
    assert_eq!(stats["g"], JOBS.to_string(), "{stats:?}");
    assert_eq!(stats["h"], JOBS.to_string(), "{stats:?}");
    assert!(
        !stats.contains_key("ov") && !stats.contains_key("rc"),
        "{stats:?}"
    );
    assert!(redis.keys_without_ttl(&["limit*"]).await == ["limits"]);
}

/// The gate of [`Hold`].
#[derive(Default)]
struct HoldGate(Gate);

/// The gate of [`Quick`].
#[derive(Default)]
struct QuickGate(Gate);

/// A job of the throttle `stall`, running until the test opens [`HoldGate`].
struct Hold;

impl Job for Hold {
    const NAME: &'static str = "chaos::Hold";
    type Args = ();

    fn options() -> JobOptions {
        JobOptions::new().throttle("stall")
    }

    async fn perform(&self, ctx: &JobContext, (): Self::Args) -> Result<(), JobError> {
        ctx.data::<HoldGate>()?.0.pass().await;
        ctx.data::<Runs>()?.record("hold");
        Ok(())
    }
}

/// Another job of the throttle `stall`, running until the test opens [`QuickGate`].
struct Quick;

impl Job for Quick {
    const NAME: &'static str = "chaos::Quick";
    type Args = ();

    fn options() -> JobOptions {
        JobOptions::new().throttle("stall")
    }

    async fn perform(&self, ctx: &JobContext, (): Self::Args) -> Result<(), JobError> {
        ctx.data::<QuickGate>()?.0.pass().await;
        ctx.data::<Runs>()?.record("quick");
        Ok(())
    }
}

/// The lease end of the execution of job `jid` holding a slot of the concurrency state `state`
/// (its token is the jid, `~` and a suffix naming the execution).
async fn lease_of(pool: &RedisPool, state: &str, jid: &str) -> Option<f64> {
    let prefix = format!("{jid}~");
    zrange(pool, state)
        .await
        .into_iter()
        .find(|(token, _)| token.starts_with(prefix.as_bytes()))
        .map(|(_, score)| score)
}

/// A process running `queue` with the throttle `stall` (one at a time, one-second leases).
async fn stall_process(
    redis: &TestRedis,
    queue: &str,
    runs: &Arc<Runs>,
    gates: &(Arc<HoldGate>, Arc<QuickGate>),
    configure: impl FnOnce(HenchBuilder) -> HenchBuilder,
) -> (Hench, Launcher) {
    let builder = redis
        .builder()
        .await
        .concurrency(1)
        .queues([queue])
        .register(Hold)
        .register(Quick)
        .throttle(
            Throttle::new("stall")
                .concurrency(1)
                .lease(Duration::from_secs(1)),
        )
        .data_arc(runs.clone())
        .data_arc(gates.0.clone())
        .data_arc(gates.1.clone());
    let hench = configure(builder).build().await.unwrap();
    let launcher = hench.start().await.unwrap();
    (hench, launcher)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_holder_whose_renewals_stop_loses_its_slot_to_an_overage() {
    init_logging();
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let runs = Arc::new(Runs::default());
    let gates = (
        Arc::new(HoldGate::default()),
        Arc::new(QuickGate::default()),
    );
    // Process A reaches Redis through a proxy, which the test partitions for longer than A's
    // one-second lease while A's job runs: like a network partition (or a runtime stalled)
    // for longer than a lease. (A heartbeat slower than the lease would do the same, but
    // `build` refuses a lease shorter than three heartbeats.)
    let proxy = Proxy::start(redis.socket_path()).await;
    let config = RedisConfig::new(proxy.url())
        .connection_timeout(Duration::from_millis(500))
        .response_timeout(Duration::from_millis(500))
        .reconnect_retries(1);
    let (_a, stalled) = stall_process(&redis, "a", &runs, &gates, |builder| {
        builder.redis_config(config)
    })
    .await;
    let (_b, healthy) = stall_process(&redis, "b", &runs, &gates, |builder| builder).await;
    let client = Client::new(pool.clone());
    Hold::set()
        .queue("a")
        .perform_async(&client, ())
        .await
        .unwrap();
    wait_for("the first job holding the slot", WAIT, || async {
        gates.0.0.entered() == 1
    })
    .await;
    proxy.set_partitioned(true);
    // B's job is parked while A's lease is live, and granted once it ended unrenewed: two jobs
    // of a one-at-a-time throttle now run together.
    Quick::set()
        .queue("b")
        .perform_async(&client, ())
        .await
        .unwrap();
    wait_for(
        "the second job granted",
        Duration::from_secs(15),
        || async { gates.1.0.entered() == 1 },
    )
    .await;
    assert_eq!(runs.len(), 0, "the first job still runs");
    proxy.set_partitioned(false);
    let stats_key = keys::limit_stats('c', "stall");
    // A's next renewal finds its slot taken: the permit is lost, an overage is counted.
    wait_for("the overage", Duration::from_secs(10), || async {
        hgetall(&pool, &stats_key)
            .await
            .get("ov")
            .map(String::as_str)
            == Some("1")
    })
    .await;
    // (Whether B's grant counted the ended lease as reclaimed depends on whether the state
    // key outlived it: with a single holder, its expiry is the lease's end.)
    // Both jobs end well: the first kept running without its slot.
    gates.0.0.open();
    gates.1.0.open();
    runs.wait_for(2).await;
    wait_for("the slot given back", WAIT, || async {
        !exists(&pool, &keys::limit_state('c', "stall")).await
    })
    .await;
    stalled.stop().await.unwrap();
    healthy.stop().await.unwrap();
    assert_eq!(zcard(&pool, keys::RETRY).await, 0);
    assert_eq!(get_int(&pool, keys::STAT_FAILED).await, 0);
    let stats = hgetall(&pool, &stats_key).await;
    // The lost permit is neither renewed nor released again: one overage, one release.
    assert_eq!(stats.get("ov").map(String::as_str), Some("1"), "{stats:?}");
    assert_eq!(stats.get("h").map(String::as_str), Some("1"), "{stats:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_redis_restart_without_persistence_restores_the_leases_of_running_jobs() {
    init_logging();
    let mut redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    let gates = (
        Arc::new(HoldGate::default()),
        Arc::new(QuickGate::default()),
    );
    let (hench, launcher) = stall_process(&redis, "default", &runs, &gates, |b| b).await;
    let jid = Hold::perform_async(hench.client(), ())
        .await
        .unwrap()
        .unwrap();
    wait_for("the job holding the slot", WAIT, || async {
        gates.0.0.entered() == 1
    })
    .await;
    let state = keys::limit_state('c', "stall");
    let stats_key = keys::limit_stats('c', "stall");
    let pool = redis.pool().await;
    assert!(lease_of(&pool, &state, &jid).await.is_some());
    // Every key is lost; the next renewal takes the lease back (the slot is free).
    redis.restart().await;
    let pool = redis.pool().await;
    wait_for("the lease restored", Duration::from_secs(10), || async {
        lease_of(&pool, &state, &jid).await.is_some()
            && hgetall(&pool, &stats_key)
                .await
                .get("restored")
                .map(String::as_str)
                == Some("1")
    })
    .await;
    gates.0.0.open();
    runs.wait_for(1).await;
    wait_for("the slot given back", WAIT, || async {
        !exists(&pool, &state).await
    })
    .await;
    launcher.stop().await.unwrap();
    assert_eq!(runs.count("hold"), 1);
    let stats = hgetall(&pool, &stats_key).await;
    assert_eq!(stats.get("h").map(String::as_str), Some("1"), "{stats:?}");
    assert!(!stats.contains_key("ov"), "{stats:?}");
}

/// A job of the throttle `outage`.
struct Outage;

impl Job for Outage {
    const NAME: &'static str = "chaos::Outage";
    type Args = ();

    fn options() -> JobOptions {
        JobOptions::new().throttle("outage")
    }

    async fn perform(&self, ctx: &JobContext, (): Self::Args) -> Result<(), JobError> {
        ctx.data::<Runs>()?.record("ran");
        Ok(())
    }
}

/// Workers behind `proxy`, with short timeouts, running [`Outage`] under a one-at-a-time
/// throttle that waits up to 20 s for its slot in process.
async fn behind_proxy(proxy: &Proxy, runs: &Arc<Runs>, errors: &Errors, fail_open: bool) -> Hench {
    let config = RedisConfig::new(proxy.url())
        .connection_timeout(Duration::from_millis(500))
        .response_timeout(Duration::from_millis(500))
        .reconnect_retries(1);
    Hench::builder()
        .redis_config(config)
        .heartbeat_interval(Duration::from_millis(100))
        .poll_interval_average(Duration::from_millis(100))
        .initial_scheduler_wait(Duration::ZERO)
        .initial_scheduler_jitter(Duration::ZERO)
        .fetch_timeout(Duration::from_millis(200))
        .timeout(Duration::from_secs(2))
        .systemd(false)
        .concurrency(1)
        .register(Outage)
        .throttle(
            Throttle::new("outage")
                .concurrency(1)
                .max_wait(Duration::from_secs(20))
                .fail_open(fail_open),
        )
        .error_handler(errors.handler())
        .data_arc(runs.clone())
        .build()
        .await
        .unwrap()
}

/// A job waiting at the gate (its slot held by another process) when Redis stops answering.
async fn gate_outage(fail_open: bool) {
    init_logging();
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let proxy = Proxy::start(redis.socket_path()).await;
    let runs = Arc::new(Runs::default());
    let errors = Errors::default();
    let hench = behind_proxy(&proxy, &runs, &errors, fail_open).await;
    let launcher = hench.start().await.unwrap();
    // The slot is held by a permit of another process (the same limit identity).
    let holder = Limiter::concurrent("outage", 1)
        .acquire(&pool)
        .await
        .unwrap();
    Outage::perform_async(&Client::new(pool.clone()), ())
        .await
        .unwrap();
    wait_for("the job fetched", WAIT, || async {
        queue_size(&pool, "default").await == 0
    })
    .await;
    // It waits at the gate, polling for the slot; Redis then stops answering.
    tokio::time::sleep(Duration::from_millis(300)).await;
    proxy.set_partitioned(true);
    if fail_open {
        // The gate cannot answer: the job runs unthrottled (the slot is still held).
        runs.wait_for(1).await;
        proxy.set_partitioned(false);
        holder.release().await.unwrap();
    } else {
        tokio::time::sleep(Duration::from_millis(1_500)).await;
        assert_eq!(
            runs.len(),
            0,
            "a throttled job ran while Redis could not answer"
        );
        proxy.set_partitioned(false);
        // Back in its queue, then at the gate again: still nothing runs while the slot is held.
        tokio::time::sleep(Duration::from_millis(1_000)).await;
        assert_eq!(runs.len(), 0, "a throttled job ran without its slot");
        holder.release().await.unwrap();
        runs.wait_for(1).await;
    }
    wait_for("the outcome written", WAIT, || async {
        exists(&pool, keys::STAT_PROCESSED).await
    })
    .await;
    launcher.stop().await.unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(queue_size(&pool, "default").await, 0);
    assert_eq!(zcard(&pool, keys::RETRY).await, 0);
    if !fail_open {
        // The gate's failure was reported like a failed fetch.
        assert!(
            !errors.with_context("could not fetch jobs").is_empty(),
            "{:?}",
            errors.all()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_gate_fails_closed_while_redis_cannot_answer() {
    gate_outage(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fail_open_throttles_run_their_jobs_while_redis_cannot_answer() {
    gate_outage(true).await;
}

/// A job refused by a limiter, which starts a network partition as it returns: the reschedule
/// is written while Redis cannot answer.
struct Partitioning;

impl Job for Partitioning {
    const NAME: &'static str = "chaos::Partitioning";
    type Args = ();

    async fn perform(&self, ctx: &JobContext, (): Self::Args) -> Result<(), JobError> {
        ctx.data::<Runs>()?.record("refused");
        ctx.data::<Proxy>()?.set_partitioned(true);
        Err(JobError::over_limit(
            OverLimit::new("w:api").with_retry_after(Duration::from_secs(600)),
        ))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reschedule_written_during_a_partition_is_written_once() {
    init_logging();
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let proxy = Arc::new(Proxy::start(redis.socket_path()).await);
    let runs = Arc::new(Runs::default());
    let config = RedisConfig::new(proxy.url())
        .connection_timeout(Duration::from_millis(500))
        .response_timeout(Duration::from_millis(500))
        .reconnect_retries(1);
    let hench = Hench::builder()
        .redis_config(config)
        .heartbeat_interval(Duration::from_millis(100))
        .poll_interval_average(Duration::from_millis(100))
        .initial_scheduler_wait(Duration::ZERO)
        .initial_scheduler_jitter(Duration::ZERO)
        .fetch_timeout(Duration::from_millis(200))
        .timeout(Duration::from_secs(2))
        .systemd(false)
        .register(Partitioning)
        .data_arc(runs.clone())
        .data_arc(proxy.clone())
        .build()
        .await
        .unwrap();
    let launcher = hench.start().await.unwrap();
    let jid = Partitioning::perform_async(&Client::new(pool.clone()), ())
        .await
        .unwrap()
        .unwrap();
    runs.wait_for(1).await;
    // The reschedule's first attempts time out; the partition heals; they arrive late, with
    // the retries.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    proxy.set_partitioned(false);
    wait_for("the job rescheduled", Duration::from_secs(15), || async {
        zcard(&pool, keys::SCHEDULE).await == 1
    })
    .await;
    // Give late duplicates every chance to land.
    tokio::time::sleep(Duration::from_millis(500)).await;
    launcher.stop().await.unwrap();
    let scheduled = sorted_jobs(&pool, keys::SCHEDULE).await;
    assert_eq!(scheduled.len(), 1, "written once");
    let (job, _) = &scheduled[0];
    assert_eq!(job.jid, jid);
    assert_eq!(job.limited.as_ref().map(|limited| limited.n), Some(1));
    assert_eq!(runs.len(), 1, "it ran once");
    assert_eq!(zcard(&pool, keys::RETRY).await, 0);
    assert_eq!(queue_size(&pool, "default").await, 0);
    assert_eq!(get_int(&pool, keys::STAT_FAILED).await, 0);
}
