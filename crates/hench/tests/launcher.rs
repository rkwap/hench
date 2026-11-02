//! The launcher and managers (Sidekiq's `launcher_test.rb`, `manager_test.rb`,
//! `capsule_test.rb` and `actors_test.rb` scenarios): heartbeat, lifecycle events, quiet,
//! stop, hard shutdown, remote signals, capsules and concurrency.

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use hench::prelude::*;
use hench::test_util::TestRedis;
use hench::{FetchStrategy, Hench, Launcher, keys};
use serde_json::Value;
use support::*;

async fn running(builder: hench::HenchBuilder) -> (Hench, Launcher) {
    init_logging();
    let hench = builder.build().await.unwrap();
    let launcher = hench.start().await.unwrap();
    (hench, launcher)
}

#[tokio::test]
async fn starts_and_stops() {
    let redis = TestRedis::start().await;
    let (hench, launcher) = running(redis.builder().await).await;
    let identity = launcher.identity().to_owned();
    assert!(!launcher.is_quiet() && !launcher.is_stopping() && !launcher.is_stopped());
    let pool = hench.redis().clone();
    assert!(smembers(&pool, keys::PROCESSES).await.contains(&identity));
    launcher.stop().await.unwrap();
    assert!(launcher.is_quiet() && launcher.is_stopping() && launcher.is_stopped());
    assert!(!smembers(&pool, keys::PROCESSES).await.contains(&identity));
    assert!(!exists(&pool, &keys::work(&identity)).await);
    assert!(!smembers(&pool, keys::INPROGRESS).await.contains(&identity));
    // Stopping again (or waiting) returns immediately.
    launcher.stop().await.unwrap();
    launcher.wait().await;
    assert!(format!("{launcher:?}").contains(&identity));
}

#[tokio::test]
async fn heartbeat_stores_process_info() {
    let redis = TestRedis::start().await;
    let (hench, launcher) = running(
        redis
            .builder()
            .await
            .concurrency(3)
            .tag("myapp")
            .label("blue")
            .capsule("serial", |c| c.concurrency(1).queues(["single"]))
            .weighted_queues([("a", 2), ("b", 1)]),
    )
    .await;
    let pool = hench.redis().clone();
    let identity = launcher.identity().to_owned();
    let hostname = identity.split(':').next().unwrap().to_owned();
    let parts: Vec<&str> = identity.split(':').collect();
    assert_eq!(parts[parts.len() - 2], std::process::id().to_string());
    assert_eq!(parts[parts.len() - 1].len(), 12);

    let process = hgetall(&pool, &identity).await;
    assert_eq!(process["busy"], "0");
    assert_eq!(process["concurrency"], "4");
    assert_eq!(process["quiet"], "false");
    let rtt: u64 = process["rtt_us"].parse().unwrap();
    assert!(rtt < 1_000_000, "{rtt}");
    let beat: f64 = process["beat"].parse().unwrap();
    assert!((beat - hench::now_secs_f64()).abs() < 10.0);
    let rss: u64 = process["rss"].parse().unwrap();
    if cfg!(target_os = "linux") {
        assert!(rss > 0);
    }
    let ttl = pttl(&pool, &identity).await;
    assert!(ttl > 55_000 && ttl <= 60_000, "{ttl}");

    let info: Value = serde_json::from_str(&process["info"]).unwrap();
    assert_eq!(info["hostname"], hostname.as_str());
    assert_eq!(info["pid"], std::process::id());
    assert_eq!(info["tag"], "myapp");
    assert_eq!(info["labels"], serde_json::json!(["blue"]));
    assert_eq!(info["identity"], identity.as_str());
    assert_eq!(info["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(info["embedded"], true);
    assert!(info["started_at"].as_f64().unwrap() > 1.0e9);
    assert!(info.get("concurrency").is_none());
    assert_eq!(info["queues"], serde_json::json!(["a", "b", "single"]));
    assert_eq!(
        info["capsules"]["default"],
        serde_json::json!({"concurrency": 3, "mode": "weighted", "weights": {"a": 2, "b": 1}})
    );
    assert_eq!(
        info["capsules"]["serial"],
        serde_json::json!({"concurrency": 1, "mode": "strict", "weights": {"single": 0}})
    );
    assert_eq!(info["weights"][1], serde_json::json!({"single": 0}));
    launcher.stop().await.unwrap();
}

#[tokio::test]
async fn heartbeat_event_fires_once_and_beat_on_every_heartbeat() {
    let redis = TestRedis::start().await;
    let events = Events::default();
    let (hench, launcher) = running(events.register(redis.builder().await)).await;
    wait_for("three beats", WAIT, || async { events.count("beat") >= 3 }).await;
    assert_eq!(events.count("heartbeat"), 1);
    assert_eq!(events.count("startup"), 1);

    // When Redis loses the process (an outage, a restart), the next beat fires `heartbeat`
    // again.
    del(hench.redis(), launcher.identity()).await;
    wait_for("a second heartbeat event", WAIT, || async {
        events.count("heartbeat") == 2
    })
    .await;
    launcher.stop().await.unwrap();
}

#[tokio::test]
async fn lifecycle_events_fire_in_order() {
    let redis = TestRedis::start().await;
    let events = Events::default();
    let (_hench, launcher) = running(events.register(redis.builder().await)).await;
    launcher.quiet().await;
    launcher.quiet().await;
    launcher.stop().await.unwrap();
    assert_eq!(
        events.milestones(),
        ["startup", "quiet", "shutdown", "exit"]
    );
    assert!(events.count("heartbeat") >= 1);
}

#[tokio::test]
async fn quiet_stops_fetching_but_finishes_running_jobs() {
    let redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    let gate = Arc::new(Gate::default());
    let (hench, launcher) = running(
        redis
            .builder()
            .await
            .register(Blocked)
            .register(Echo)
            .data_arc(runs.clone())
            .data_arc(gate.clone()),
    )
    .await;
    let pool = hench.redis().clone();
    Blocked::perform_async(hench.client(), ("first".into(),))
        .await
        .unwrap();
    wait_for("the first job to start", WAIT, || async {
        gate.entered() == 1
    })
    .await;

    launcher.quiet().await;
    assert!(launcher.is_quiet());
    wait_for("the heartbeat to report quiet", WAIT, || async {
        hgetall(&pool, launcher.identity()).await["quiet"] == "true"
    })
    .await;
    // Jobs pushed now stay in the queue...
    Echo::perform_async(hench.client(), ("later".into(),))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(queue_size(&pool, "default").await, 1);
    assert_eq!(runs.count("later"), 0);
    // ...while the running one finishes normally.
    gate.open();
    wait_for("the first job to finish", WAIT, || async {
        runs.count("done:first") == 1
    })
    .await;
    launcher.stop().await.unwrap();
    assert_eq!(queue_size(&pool, "default").await, 1);
    assert_eq!(llen(&pool, &keys::inprogress(launcher.identity())).await, 0);
}

#[tokio::test]
async fn the_work_state_shows_running_jobs() {
    let redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    let gate = Arc::new(Gate::default());
    let (hench, launcher) = running(
        redis
            .builder()
            .await
            .register(Blocked)
            .data_arc(runs.clone())
            .data_arc(gate.clone()),
    )
    .await;
    let pool = hench.redis().clone();
    let jid = Blocked::perform_async(hench.client(), ("x".into(),))
        .await
        .unwrap()
        .unwrap();
    let work_key = keys::work(launcher.identity());
    wait_for("the job in the work state", WAIT, || async {
        hgetall(&pool, &work_key).await.len() == 1
    })
    .await;
    let work = hgetall(&pool, &work_key).await;
    let (tid, entry) = work.into_iter().next().unwrap();
    assert!(!tid.is_empty());
    let entry: Value = serde_json::from_str(&entry).unwrap();
    assert_eq!(entry["queue"], "default");
    let payload = JobPayload::from_json(entry["payload"].as_str().unwrap()).unwrap();
    assert_eq!(payload.jid, jid);
    assert!((entry["run_at"].as_f64().unwrap() - hench::now_secs_f64()).abs() < 10.0);
    let ttl = pttl(&pool, &work_key).await;
    assert!(ttl > 0 && ttl <= 60_000);
    wait_for("busy = 1", WAIT, || async {
        hgetall(&pool, launcher.identity()).await["busy"] == "1"
    })
    .await;

    gate.open();
    wait_for("the work state to empty", WAIT, || async {
        !exists(&pool, &work_key).await
    })
    .await;
    launcher.stop().await.unwrap();
}

/// Sleeps until the process stops, then returns `Ok`.
#[derive(Default)]
struct Patient;

impl Job for Patient {
    const NAME: &'static str = "Patient";
    type Args = ();

    async fn perform(&self, ctx: &JobContext, (): ()) -> Result<(), JobError> {
        ctx.data::<Runs>()?.record("waiting");
        ctx.stopping().await;
        ctx.data::<Runs>()?.record("stopping seen");
        Ok(())
    }
}

#[tokio::test]
async fn jobs_observe_the_shutdown() {
    let redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    let (hench, launcher) = running(
        redis
            .builder()
            .await
            .register(Patient)
            .data_arc(runs.clone())
            .timeout(Duration::from_secs(30)),
    )
    .await;
    Patient::perform_async(hench.client(), ()).await.unwrap();
    runs.wait_for(1).await;
    let started = Instant::now();
    launcher.stop().await.unwrap();
    assert!(started.elapsed() < Duration::from_secs(5));
    assert_eq!(runs.entries(), ["waiting", "stopping seen"]);
    // It finished, so it was acknowledged, not requeued.
    let pool = hench.redis();
    assert_eq!(queue_size(pool, "default").await, 0);
    assert_eq!(get_int(pool, keys::STAT_PROCESSED).await, 1);
}

/// Stops early when the process shuts down, reporting it with the `hench::Shutdown` class.
#[derive(Default)]
struct Interruptible;

impl Job for Interruptible {
    const NAME: &'static str = "Interruptible";
    type Args = ();

    async fn perform(&self, ctx: &JobContext, (): ()) -> Result<(), JobError> {
        ctx.data::<Runs>()?.record("started");
        ctx.stopping().await;
        Err(JobError::new(
            JobError::SHUTDOWN,
            "interrupted at a safe point",
        ))
    }
}

#[tokio::test]
async fn jobs_interrupted_by_the_shutdown_are_requeued() {
    let redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    let errors = Errors::default();
    let (hench, launcher) = running(
        redis
            .builder()
            .await
            .register(Interruptible)
            .data_arc(runs.clone())
            .error_handler(errors.handler()),
    )
    .await;
    let pool = hench.redis().clone();
    let jid = Interruptible::perform_async(hench.client(), ())
        .await
        .unwrap()
        .unwrap();
    runs.wait_for(1).await;
    launcher.stop().await.unwrap();
    let queued = queue_jobs(&pool, "default").await;
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].jid, jid);
    assert_eq!(queued[0].retry_count, None);
    assert_eq!(zcard(&pool, keys::RETRY).await, 0);
    assert!(errors.with_context("job failed").is_empty());
}

#[tokio::test]
async fn hard_shutdown_requeues_unfinished_jobs() {
    for strategy in [FetchStrategy::Reliable, FetchStrategy::Basic] {
        let redis = TestRedis::start().await;
        let runs = Arc::new(Runs::default());
        let events = Events::default();
        let (hench, launcher) = running(
            events.register(
                redis
                    .builder()
                    .await
                    .fetch(strategy)
                    .register(Stubborn)
                    .data_arc(runs.clone())
                    .timeout(Duration::from_millis(300)),
            ),
        )
        .await;
        let pool = hench.redis().clone();
        Stubborn::perform_async(hench.client(), ("a".into(),))
            .await
            .unwrap();
        runs.wait_for(1).await;
        let fetched = queue_size(&pool, "default").await;
        assert_eq!(fetched, 0);
        // The job, byte for byte, as the worker holds it.
        let held = if strategy == FetchStrategy::Reliable {
            lrange(&pool, &keys::inprogress(launcher.identity())).await
        } else {
            Vec::new()
        };

        let started = Instant::now();
        launcher.stop().await.unwrap();
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_millis(250) && elapsed < Duration::from_secs(5),
            "{strategy:?}: {elapsed:?}"
        );
        let requeued = lrange(&pool, &keys::queue("default")).await;
        assert_eq!(requeued.len(), 1, "{strategy:?}");
        if strategy == FetchStrategy::Reliable {
            assert_eq!(requeued, held);
        }
        assert_eq!(llen(&pool, &keys::inprogress(launcher.identity())).await, 0);
        assert_eq!(zcard(&pool, keys::RETRY).await, 0);
        assert_eq!(zcard(&pool, keys::DEAD).await, 0);
        assert_eq!(
            events.milestones(),
            ["startup", "quiet", "shutdown", "exit"]
        );
        // An interrupted job counts as processed and failed, like in Sidekiq.
        assert_eq!(get_int(&pool, keys::STAT_PROCESSED).await, 1);
        assert_eq!(get_int(&pool, keys::STAT_FAILED).await, 1);
    }
}

#[tokio::test]
async fn interrupted_jobs_run_again_on_the_next_start() {
    let redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    let builder = || async {
        redis
            .builder()
            .await
            .register(Stubborn)
            .data_arc(runs.clone())
            .timeout(Duration::from_millis(200))
    };
    let (hench, launcher) = running(builder().await).await;
    let jid = Stubborn::perform_async(hench.client(), ("a".into(),))
        .await
        .unwrap();
    runs.wait_for(1).await;
    launcher.stop().await.unwrap();
    let (_hench, launcher) = running(builder().await).await;
    runs.wait_for(2).await;
    assert_eq!(runs.count("entered:a"), 2);
    launcher.stop().await.unwrap();
    assert_eq!(
        queue_jobs(hench.redis(), "default").await[0].jid,
        jid.unwrap()
    );
}

#[tokio::test]
async fn remote_signals_quiet_and_stop_the_process() {
    let redis = TestRedis::start().await;
    let events = Events::default();
    let (hench, launcher) = running(events.register(redis.builder().await)).await;
    let pool = hench.redis().clone();
    let signals = keys::signals(launcher.identity());
    let _: i64 = cmd(&pool, &redis_cmd!("LPUSH", &signals, keys::SIGNAL_QUIET)).await;
    wait_for("the remote quiet", WAIT, || async { launcher.is_quiet() }).await;
    assert!(!launcher.is_stopping());
    let _: i64 = cmd(&pool, &redis_cmd!("LPUSH", &signals, keys::SIGNAL_STOP)).await;
    tokio::time::timeout(WAIT, launcher.wait())
        .await
        .expect("remote stop");
    assert!(launcher.is_stopped());
    assert_eq!(
        events.milestones(),
        ["startup", "quiet", "shutdown", "exit"]
    );
    assert!(
        !smembers(&pool, keys::PROCESSES)
            .await
            .contains(&launcher.identity().to_owned())
    );
}

#[tokio::test]
async fn run_until_stops_when_its_future_completes() {
    let redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    let hench = redis
        .builder()
        .await
        .register(Echo)
        .data_arc(runs.clone())
        .build()
        .await
        .unwrap();
    let client = hench.client().clone();
    let pool = hench.redis().clone();
    let token = hench::CancellationToken::new();
    let workers = tokio::spawn(hench.run_until(token.clone().cancelled_owned()));
    Echo::perform_async(&client, ("x".into(),)).await.unwrap();
    runs.wait_for(1).await;
    token.cancel();
    tokio::time::timeout(WAIT, workers)
        .await
        .expect("run_until returns")
        .unwrap()
        .unwrap();
    assert!(smembers(&pool, keys::PROCESSES).await.is_empty());
}

#[tokio::test]
async fn run_until_returns_after_a_remote_stop() {
    let redis = TestRedis::start().await;
    let hench = redis.builder().await.build().await.unwrap();
    let pool = hench.redis().clone();
    let workers = tokio::spawn(hench.run_until(std::future::pending()));
    wait_for("the process to register", WAIT, || async {
        !smembers(&pool, keys::PROCESSES).await.is_empty()
    })
    .await;
    let identity = smembers(&pool, keys::PROCESSES).await.remove(0);
    let _: i64 = cmd(
        &pool,
        &redis_cmd!("LPUSH", keys::signals(&identity), keys::SIGNAL_STOP),
    )
    .await;
    tokio::time::timeout(WAIT, workers)
        .await
        .expect("run_until returns")
        .unwrap()
        .unwrap();
}

/// Tracks how many instances run at the same time.
#[derive(Default)]
struct Concurrency {
    running: AtomicUsize,
    max: AtomicUsize,
    done: AtomicUsize,
}

#[derive(Default)]
struct Busy;

impl Job for Busy {
    const NAME: &'static str = "Busy";
    type Args = ();

    async fn perform(&self, ctx: &JobContext, (): ()) -> Result<(), JobError> {
        let state = ctx.data::<Concurrency>()?;
        let now = state.running.fetch_add(1, Ordering::SeqCst) + 1;
        state.max.fetch_max(now, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(30)).await;
        state.running.fetch_sub(1, Ordering::SeqCst);
        state.done.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrency_is_respected() {
    let redis = TestRedis::start().await;
    let state = Arc::new(Concurrency::default());
    let (hench, launcher) = running(
        redis
            .builder()
            .await
            .concurrency(3)
            .register(Busy)
            .data_arc(state.clone()),
    )
    .await;
    Busy::perform_bulk(hench.client(), vec![(); 30])
        .await
        .unwrap();
    wait_for("30 jobs", WAIT, || async {
        state.done.load(Ordering::SeqCst) == 30
    })
    .await;
    assert_eq!(state.max.load(Ordering::SeqCst), 3);
    launcher.stop().await.unwrap();
}

#[derive(Default)]
struct SerialBusy;

impl Job for SerialBusy {
    const NAME: &'static str = "SerialBusy";
    type Args = ();

    fn options() -> JobOptions {
        JobOptions::new().queue("single")
    }

    async fn perform(&self, ctx: &JobContext, args: ()) -> Result<(), JobError> {
        assert_eq!(ctx.queue(), "single");
        Busy.perform(ctx, args).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn capsules_have_their_own_concurrency_and_queues() {
    let redis = TestRedis::start().await;
    let state = Arc::new(Concurrency::default());
    let (hench, launcher) = running(
        redis
            .builder()
            .await
            .concurrency(5)
            .capsule("single-threaded", |c| c.concurrency(1).queues(["single"]))
            .register(SerialBusy)
            .data_arc(state.clone()),
    )
    .await;
    SerialBusy::perform_bulk(hench.client(), vec![(); 8])
        .await
        .unwrap();
    wait_for("8 jobs", WAIT, || async {
        state.done.load(Ordering::SeqCst) == 8
    })
    .await;
    // Only the single-threaded capsule processes `single`.
    assert_eq!(state.max.load(Ordering::SeqCst), 1);
    assert_eq!(hench.config().total_concurrency(), 6);
    launcher.stop().await.unwrap();
}

#[tokio::test]
async fn several_launchers_can_run_in_one_process() {
    let redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    let hench = redis
        .builder()
        .await
        .register(Echo)
        .data_arc(runs.clone())
        .build()
        .await
        .unwrap();
    let first = hench.start().await.unwrap();
    let second = hench.start().await.unwrap();
    assert_ne!(first.identity(), second.identity());
    let pool = hench.redis().clone();
    assert_eq!(smembers(&pool, keys::PROCESSES).await.len(), 2);
    for i in 0..20 {
        Echo::perform_async(hench.client(), (i.to_string(),))
            .await
            .unwrap();
    }
    runs.wait_for(20).await;
    first.stop().await.unwrap();
    assert_eq!(
        smembers(&pool, keys::PROCESSES).await,
        [second.identity().to_owned()]
    );
    second.stop().await.unwrap();
    assert_eq!(runs.len(), 20);
}

#[tokio::test]
async fn start_fails_when_redis_is_unreachable() {
    let mut redis = TestRedis::start().await;
    let hench = Hench::builder()
        .redis(hench::RedisPool::lazy(redis.config().reconnect_retries(0)).unwrap())
        .build()
        .await
        .unwrap();
    redis.stop();
    let error = hench.start().await.unwrap_err();
    assert!(matches!(error, hench::Error::Redis(_)), "{error}");
}

#[tokio::test]
async fn stats_are_flushed_on_shutdown() {
    let redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    let (hench, launcher) = running(
        redis
            .builder()
            .await
            .register(Echo)
            .register(Fail)
            .data_arc(runs.clone())
            // No heartbeat during the test: only the shutdown flushes.
            .heartbeat_interval(hench::Config::MAX_HEARTBEAT_INTERVAL),
    )
    .await;
    let pool = hench.redis().clone();
    Echo::perform_async(hench.client(), ("a".into(),))
        .await
        .unwrap();
    Echo::perform_async(hench.client(), ("b".into(),))
        .await
        .unwrap();
    Fail::perform_async(hench.client(), vec![]).await.unwrap();
    runs.wait_for(2).await;
    wait_for("the failure to be recorded", WAIT, || async {
        zcard(&pool, keys::RETRY).await == 1
    })
    .await;
    assert_eq!(get_int(&pool, keys::STAT_PROCESSED).await, 0);
    launcher.stop().await.unwrap();
    assert_eq!(get_int(&pool, keys::STAT_PROCESSED).await, 3);
    assert_eq!(get_int(&pool, keys::STAT_FAILED).await, 1);
    let today = chrono_today();
    let daily = keys::stat_processed_on(today);
    assert_eq!(get_int(&pool, &daily).await, 3);
    assert_eq!(get_int(&pool, &keys::stat_failed_on(today)).await, 1);
    let ttl = pttl(&pool, &daily).await;
    let five_years_ms = i64::try_from(keys::STATS_TTL.as_millis()).unwrap();
    assert!(
        ttl > five_years_ms - 60_000 && ttl <= five_years_ms,
        "{ttl}"
    );
}

fn chrono_today() -> chrono::NaiveDate {
    chrono::Utc::now().date_naive()
}

#[tokio::test]
async fn stats_are_flushed_by_the_heartbeat() {
    let redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    let (hench, launcher) =
        running(redis.builder().await.register(Echo).data_arc(runs.clone())).await;
    let pool = hench.redis().clone();
    Echo::perform_async(hench.client(), ("a".into(),))
        .await
        .unwrap();
    wait_for("the heartbeat to flush the stats", WAIT, || async {
        get_int(&pool, keys::STAT_PROCESSED).await == 1
    })
    .await;
    launcher.stop().await.unwrap();
    assert_eq!(get_int(&pool, keys::STAT_PROCESSED).await, 1);
}

#[tokio::test]
async fn quiet_is_published_without_waiting_for_the_next_heartbeat() {
    let redis = TestRedis::start().await;
    let (hench, launcher) = running(
        redis
            .builder()
            .await
            .heartbeat_interval(hench::Config::MAX_HEARTBEAT_INTERVAL),
    )
    .await;
    let pool = hench.redis().clone();
    assert_eq!(hgetall(&pool, launcher.identity()).await["quiet"], "false");
    launcher.quiet().await;
    wait_for("the quiet flag", Duration::from_secs(2), || async {
        hgetall(&pool, launcher.identity()).await["quiet"] == "true"
    })
    .await;
    launcher.stop().await.unwrap();
}

#[tokio::test]
async fn remote_signals_are_consumed_and_unknown_ones_skipped() {
    let redis = TestRedis::start().await;
    let (hench, launcher) = running(redis.builder().await).await;
    let pool = hench.redis().clone();
    let signals = keys::signals(launcher.identity());
    // Pushed like the dashboard does: the oldest is handled first.
    let _: i64 = cmd(&pool, &redis_cmd!("LPUSH", &signals, "USR9")).await;
    let _: i64 = cmd(&pool, &redis_cmd!("LPUSH", &signals, keys::SIGNAL_QUIET)).await;
    wait_for("the remote quiet", WAIT, || async { launcher.is_quiet() }).await;
    wait_for("the signals to be consumed", WAIT, || async {
        llen(&pool, &signals).await == 0
    })
    .await;
    assert!(!launcher.is_stopping());
    launcher.stop().await.unwrap();
}

#[tokio::test]
async fn unusual_queue_names_work() {
    let redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    let queues = ["emails:high", "отчёты", "with space", "low"];
    let (hench, launcher) = running(
        redis
            .builder()
            .await
            .queues(queues)
            .register(Echo)
            .data_arc(runs.clone()),
    )
    .await;
    for queue in queues {
        Echo::set()
            .queue(queue)
            .perform_async(hench.client(), (queue.to_owned(),))
            .await
            .unwrap();
    }
    runs.wait_for(queues.len()).await;
    launcher.stop().await.unwrap();
    let mut ran = runs.entries();
    ran.sort();
    let mut expected: Vec<String> = queues.iter().map(|q| (*q).to_owned()).collect();
    expected.sort();
    assert_eq!(ran, expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn capsules_can_share_a_queue() {
    let redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    let (hench, launcher) = running(
        redis
            .builder()
            .await
            .queues(["shared"])
            .capsule("more", |c| c.concurrency(2).queues(["shared", "other"]))
            .register(Echo)
            .data_arc(runs.clone()),
    )
    .await;
    let args: Vec<(String,)> = (0..200).map(|i| (i.to_string(),)).collect();
    Echo::set()
        .queue("shared")
        .perform_bulk(hench.client(), args)
        .await
        .unwrap();
    runs.wait_for(200).await;
    launcher.stop().await.unwrap();
    let mut ran = runs.entries();
    ran.sort();
    ran.dedup();
    assert_eq!(ran.len(), 200, "every job ran exactly once");
    assert_eq!(runs.len(), 200);
}

/// Blocks its thread for the given number of milliseconds, like a job calling a synchronous
/// library would.
#[derive(Default)]
struct Blocking;

impl Job for Blocking {
    const NAME: &'static str = "Blocking";
    type Args = (u64,);

    async fn perform(&self, ctx: &JobContext, (millis,): (u64,)) -> Result<(), JobError> {
        let runs = ctx.data::<Runs>()?;
        runs.record("start");
        std::thread::sleep(Duration::from_millis(millis));
        runs.record("end");
        Ok(())
    }
}

/// A job blocking the only thread of the workers' runtime stops the heartbeat, but not the
/// refreshing of the process's heartbeat key: other processes, which treat a process whose key
/// expired as dead and run its jobs again, never see it expire while the job runs.
#[tokio::test]
async fn the_heartbeat_key_stays_fresh_while_a_job_blocks_the_runtime() {
    init_logging();
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let runs = Arc::new(Runs::default());
    let builder = redis
        .builder()
        .await
        .register(Blocking)
        .auto_register(false)
        .data_arc(runs.clone());
    let (started, identity) = std::sync::mpsc::channel();
    // The workers on a runtime of their own, with a single thread that the job blocks.
    let workers = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let hench = builder.build().await.unwrap();
            let launcher = hench.start().await.unwrap();
            Blocking::perform_async(hench.client(), (3_000,))
                .await
                .unwrap();
            started.send(launcher.identity().to_owned()).unwrap();
            launcher.wait().await;
        });
    });
    let identity = tokio::task::spawn_blocking(move || identity.recv().unwrap())
        .await
        .unwrap();
    let key = keys::process(&identity);
    wait_for("the job to start", WAIT, || async {
        runs.count("start") == 1
    })
    .await;
    // While the runtime is blocked, the key's expiry keeps being refreshed (every 100 ms):
    // without that it would count down from 60 s.
    tokio::time::sleep(Duration::from_millis(1_000)).await;
    let mut lowest = i64::MAX;
    while runs.count("end") == 0 {
        lowest = lowest.min(pttl(&pool, &key).await);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        lowest > 59_000,
        "the key's expiry was not refreshed: {lowest} ms left"
    );
    // Stop the workers remotely, then wait for their thread.
    let _: i64 = cmd(
        &pool,
        &redis_cmd!("LPUSH", keys::signals(&identity), keys::SIGNAL_STOP),
    )
    .await;
    tokio::task::spawn_blocking(move || workers.join().unwrap())
        .await
        .unwrap();
    assert_eq!(runs.entries(), ["start", "end"]);
    // The liveness thread stopped with the process: the key is no longer refreshed.
    let left = pttl(&pool, &key).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let later = pttl(&pool, &key).await;
    assert!(later < left || later < 0, "{left} then {later}");
}

/// A remote quiet (dashboard, `hench quiet`) runs the quiet callbacks without holding up the
/// heartbeat, and a stop that follows lets them finish before the shutdown callbacks (it used
/// to run them in the heartbeat task, which stopped beating, and abort them).
#[tokio::test]
async fn a_remote_quiet_neither_stops_the_heartbeat_nor_is_cut_short_by_a_stop() {
    init_logging();
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let log = Arc::new(Runs::default());
    let beats = Arc::new(AtomicUsize::new(0));
    let (quiet_log, shutdown_log, exit_log, beat_count) =
        (log.clone(), log.clone(), log.clone(), beats.clone());
    let hench = redis
        .builder()
        .await
        .on_quiet(move || {
            let log = quiet_log.clone();
            async move {
                log.record("quiet-start");
                tokio::time::sleep(Duration::from_secs(2)).await;
                log.record("quiet-done");
            }
        })
        .on_shutdown(move || {
            let log = shutdown_log.clone();
            async move { log.record("shutdown") }
        })
        .on_exit(move || {
            let log = exit_log.clone();
            async move { log.record("exit") }
        })
        .on_beat(move || {
            let beats = beat_count.clone();
            async move {
                beats.fetch_add(1, Ordering::SeqCst);
            }
        })
        .build()
        .await
        .unwrap();
    let launcher = hench.start().await.unwrap();
    let process = hench::api::Api::new(pool.clone())
        .processes()
        .get(launcher.identity())
        .await
        .unwrap()
        .unwrap();
    process.quiet().await.unwrap();
    wait_for("the quiet callback", WAIT, || async {
        log.count("quiet-start") == 1
    })
    .await;
    let before = beats.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        beats.load(Ordering::SeqCst) > before,
        "no heartbeat while the quiet callback runs"
    );
    launcher.stop().await.unwrap();
    assert_eq!(
        log.entries(),
        ["quiet-start", "quiet-done", "shutdown", "exit"]
    );
}

/// Dropping the `run_until` future (the losing branch of a `select!`) shuts the workers down
/// gracefully instead of leaving them running out of reach.
#[tokio::test]
async fn dropping_run_until_shuts_the_workers_down() {
    init_logging();
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let runs = Arc::new(Runs::default());
    let hench = redis
        .builder()
        .await
        .register(Echo)
        .auto_register(false)
        .data_arc(runs.clone())
        .build()
        .await
        .unwrap();
    let client = hench.client().clone();
    tokio::select! {
        _ = hench.run_until(std::future::pending()) => unreachable!(),
        () = async {
            wait_for("the process to register", WAIT, || async {
                smembers(&pool, keys::PROCESSES).await.len() == 1
            })
            .await;
        } => {}
    }
    wait_for("the process to unregister", WAIT, || async {
        smembers(&pool, keys::PROCESSES).await.is_empty()
    })
    .await;
    Echo::perform_async(&client, ("after-drop".into(),))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(runs.len(), 0);
    assert_eq!(queue_size(&pool, "default").await, 1);
}

/// Workers behind `url` (a proxy) on a current-thread runtime of their own, which a
/// [`Blocking`] job blocks. Returns the thread and the process identity.
async fn blockable_workers(
    url: String,
    runs: Arc<Runs>,
    events: Option<Events>,
) -> (std::thread::JoinHandle<()>, String) {
    let mut builder = Hench::builder()
        .redis_config(
            hench::RedisConfig::new(url)
                .connection_timeout(Duration::from_secs(1))
                .response_timeout(Duration::from_secs(5))
                .reconnect_retries(5)
                .reconnect_delays(Duration::from_millis(10), Duration::from_millis(200)),
        )
        .heartbeat_interval(Duration::from_millis(100))
        .poll_interval_average(Duration::from_millis(100))
        .initial_scheduler_wait(Duration::ZERO)
        .initial_scheduler_jitter(Duration::ZERO)
        .fetch_timeout(Duration::from_millis(100))
        .orphan_check_interval(Duration::from_millis(500))
        .timeout(Duration::from_secs(5))
        .systemd(false)
        .register(Blocking)
        .auto_register(false)
        .data_arc(runs);
    if let Some(events) = events {
        builder = events.register(builder);
    }
    let (started, identity) = std::sync::mpsc::channel();
    let workers = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let hench = builder.build().await.unwrap();
            let launcher = hench.start().await.unwrap();
            started.send(launcher.identity().to_owned()).unwrap();
            launcher.wait().await;
        });
    });
    let identity = tokio::task::spawn_blocking(move || identity.recv().unwrap())
        .await
        .unwrap();
    (workers, identity)
}

/// Stops workers started by [`blockable_workers`] remotely, through `pool`, and waits for them.
async fn stop_blockable_workers(
    pool: &RedisPool,
    identity: &str,
    workers: std::thread::JoinHandle<()>,
) {
    let _: i64 = cmd(
        pool,
        &redis_cmd!("LPUSH", keys::signals(identity), keys::SIGNAL_STOP),
    )
    .await;
    tokio::task::spawn_blocking(move || workers.join().unwrap())
        .await
        .unwrap();
}

/// Fails over from `old` to `new` with `failover` while the workers run behind a proxy (which
/// it switches to `new`), then blocks the workers' runtime with a 3 s job: returns the lowest
/// time to live of the process's heartbeat key on `new` meanwhile.
async fn lowest_pttl_after_failover(
    old: &TestRedis,
    new: &TestRedis,
    failover: impl AsyncFnOnce(&SwitchProxy, &RedisPool, &RedisPool),
) -> i64 {
    let (pool_old, pool_new) = (old.pool().await, new.pool().await);
    let proxy = SwitchProxy::start(old.socket_path()).await;
    let runs = Arc::new(Runs::default());
    let (workers, identity) = blockable_workers(proxy.url(), runs.clone(), None).await;
    let key = keys::process(&identity);
    wait_for("the heartbeat on the old primary", WAIT, || async {
        exists(&pool_old, &key).await
    })
    .await;
    // The liveness thread connects on its first tick, one heartbeat interval after the start:
    // let it connect to the old primary.
    tokio::time::sleep(Duration::from_millis(500)).await;
    failover(&proxy, &pool_old, &pool_new).await;
    wait_for("the heartbeat on the new primary", WAIT, || async {
        exists(&pool_new, &key).await && pttl(&pool_new, &key).await > 59_000
    })
    .await;
    // The fetchers move too, after their next error.
    tokio::time::sleep(Duration::from_millis(500)).await;
    Blocking::perform_async(&new.client().await, (3_000,))
        .await
        .unwrap();
    wait_for("the job to start", WAIT, || async {
        runs.count("start") == 1
    })
    .await;
    tokio::time::sleep(Duration::from_millis(1_000)).await;
    let mut lowest = i64::MAX;
    while runs.count("end") == 0 {
        lowest = lowest.min(pttl(&pool_new, &key).await);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    stop_blockable_workers(&pool_new, &identity, workers).await;
    lowest
}

/// A failover that demotes the old primary without closing its clients' connections (an
/// operator's `REPLICAOF`; some orchestrators): every write of the liveness thread's
/// connection then answers `READONLY`. The thread must reconnect, through the address that now
/// leads to the new primary, or the heartbeat key there would only be refreshed by the
/// heartbeat, which a job blocking the runtime stops (and the job would run twice).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_liveness_thread_follows_a_failover_that_keeps_clients_connected() {
    init_logging();
    let (a, b) = (TestRedis::start().await, TestRedis::start().await);
    let lowest = lowest_pttl_after_failover(&a, &b, async |proxy, pool_a, _pool_b| {
        proxy.switch(b.socket_path());
        let _: String = cmd(pool_a, &redis_cmd!("REPLICAOF", "127.0.0.1", "1")).await;
    })
    .await;
    assert!(
        lowest > 59_000,
        "the key was not refreshed on the new primary: {lowest} ms left"
    );
}

/// The same with Redis's own coordinated `FAILOVER` (the replica is promoted, the old primary
/// demoted, clients stay connected), the proxy then switched to the new primary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_liveness_thread_follows_a_coordinated_failover() {
    init_logging();
    let free_port = || {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
            .to_string()
    };
    let (port_a, port_b) = (free_port(), free_port());
    let a = TestRedis::start_with_args(&["--port", &port_a, "--bind", "127.0.0.1"]).await;
    let b = TestRedis::start_with_args(&["--port", &port_b, "--bind", "127.0.0.1"]).await;
    let replication =
        async |pool: &RedisPool| -> String { cmd(pool, &redis_cmd!("INFO", "replication")).await };
    {
        let pool_b = b.pool().await;
        let _: String = cmd(&pool_b, &redis_cmd!("REPLICAOF", "127.0.0.1", &port_a)).await;
        wait_for("the replica to sync", WAIT, || async {
            replication(&pool_b).await.contains("master_link_status:up")
        })
        .await;
    }
    let lowest = lowest_pttl_after_failover(&a, &b, async |proxy, pool_a, pool_b| {
        let _: String = cmd(pool_a, &redis_cmd!("FAILOVER")).await;
        wait_for("the failover", WAIT, || async {
            replication(pool_a).await.contains("role:slave")
                && replication(pool_b).await.contains("role:master")
        })
        .await;
        proxy.switch(b.socket_path());
    })
    .await;
    assert!(
        lowest > 59_000,
        "the key was not refreshed on the new primary: {lowest} ms left"
    );
}

/// When Redis loses the heartbeat key (a restart without persistence, `FLUSHALL`) while a job
/// blocks the runtime, the liveness thread re-creates it: an orphan check of another process
/// would otherwise take this live process for dead. The heartbeat, once it runs again, writes
/// the process's description and fires the `heartbeat` event.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_liveness_thread_recreates_a_lost_heartbeat_key() {
    init_logging();
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let proxy = SwitchProxy::start(redis.socket_path()).await;
    let runs = Arc::new(Runs::default());
    let events = Events::default();
    let (workers, identity) =
        blockable_workers(proxy.url(), runs.clone(), Some(events.clone())).await;
    let key = keys::process(&identity);
    wait_for("the first heartbeat", WAIT, || async {
        exists(&pool, &key).await
    })
    .await;
    Blocking::perform_async(&redis.client().await, (3_000,))
        .await
        .unwrap();
    wait_for("the job to start", WAIT, || async {
        runs.count("start") == 1
    })
    .await;
    // Redis loses the key while the runtime is blocked.
    del(&pool, &key).await;
    wait_for("the key to come back", Duration::from_secs(2), || async {
        exists(&pool, &key).await
    })
    .await;
    assert_eq!(runs.count("end"), 0, "the job still blocks the runtime");
    let fields = hgetall(&pool, &key).await;
    assert!(fields.contains_key("liveness"), "{fields:?}");
    assert!(!fields.contains_key("info"), "{fields:?}");
    assert!(pttl(&pool, &key).await > 59_000);
    // Once the runtime runs again, the heartbeat describes the process again.
    wait_for("the heartbeat event", WAIT, || async {
        events.count("heartbeat") == 2
    })
    .await;
    assert!(hgetall(&pool, &key).await.contains_key("info"));
    stop_blockable_workers(&pool, &identity, workers).await;
}

/// A `Hench` built before Redis restarts starts after it: its pool's connection predates the
/// restart, and the first command of the start (the version check) is retried once on a new
/// connection instead of failing the start with a broken pipe.
#[tokio::test]
async fn start_survives_a_redis_restart_after_the_build() {
    let mut redis = TestRedis::start().await;
    let hench = redis.builder().await.build().await.unwrap();
    redis.restart().await;
    let launcher = hench.start().await.unwrap();
    launcher.stop().await.unwrap();
}

/// A fallible startup callback (`on_startup_try`) that fails, or a startup callback that panics,
/// stops the start before the process registers or fetches anything (Sidekiq fires `startup`
/// with `reraise: true`); the start used to go on and the process took jobs anyway.
#[tokio::test]
async fn a_failing_startup_callback_stops_the_start() {
    init_logging();
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let runs = Arc::new(Runs::default());
    let client = redis.client().await;
    Echo::perform_async(&client, ("waiting".into(),))
        .await
        .unwrap();
    let order = Arc::new(Runs::default());
    let (first, second) = (order.clone(), order.clone());
    let failing = redis
        .builder()
        .await
        .auto_register(false)
        .register(Echo)
        .data_arc(runs.clone())
        .on_startup(move || {
            let order = first.clone();
            async move { order.record("startup") }
        })
        .on_startup_try(move || {
            let order = second.clone();
            async move {
                order.record("check");
                Err::<(), _>(std::io::Error::other("the database is unreachable"))
            }
        })
        .build()
        .await
        .unwrap();
    let error = failing.start().await.unwrap_err();
    assert!(matches!(error, hench::Error::Startup(_)), "{error:?}");
    assert!(
        error.to_string().contains("the database is unreachable"),
        "{error}"
    );
    // The checks run first; nothing else ran or started.
    assert_eq!(order.entries(), ["check"]);
    assert!(smembers(&pool, keys::PROCESSES).await.is_empty());
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(runs.len(), 0);
    assert_eq!(queue_size(&pool, "default").await, 1);

    // A panicking startup callback stops it too, after the callbacks before it.
    let panicking = redis
        .builder()
        .await
        .auto_register(false)
        .register(Echo)
        .data_arc(runs.clone())
        .on_startup(|| async { panic!("no model to load") })
        .build()
        .await
        .unwrap();
    let error = panicking.start().await.unwrap_err();
    assert!(
        matches!(&error, hench::Error::Startup(job) if job.is_panic()),
        "{error:?}"
    );
    assert!(error.to_string().contains("no model to load"), "{error}");
    assert!(smembers(&pool, keys::PROCESSES).await.is_empty());

    // Checks that pass let it start.
    let passing = redis
        .builder()
        .await
        .auto_register(false)
        .register(Echo)
        .data_arc(runs.clone())
        .on_startup_try(|| async { Ok::<(), std::io::Error>(()) })
        .build()
        .await
        .unwrap();
    let launcher = passing.start().await.unwrap();
    runs.wait_for(1).await;
    launcher.stop().await.unwrap();
}
