//! The data API (`hench::api`) against running workers: what the engine writes (heartbeats,
//! work state, stats, retries, the morgue, the schedule) is exactly what the API reads, and
//! what the API writes (remote signals, moves between sets and queues, paused queues) is
//! what the engine acts on. Also checks that testing clients built from a `Hench` run jobs
//! the way its workers do.

mod support;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::Utc;
use hench::api::{Api, StatsHistory};
use hench::prelude::*;
use hench::test_util::TestRedis;
use hench::testing::Testing;
use hench::{Error, Hench, HenchBuilder, Launcher, keys};
use serde_json::json;
use support::*;

async fn start(builder: HenchBuilder) -> (Hench, Launcher, Api) {
    init_logging();
    let hench = builder.build().await.unwrap();
    let launcher = hench.start().await.unwrap();
    let api = Api::from_hench(&hench);
    (hench, launcher, api)
}

fn secs_since_epoch(time: SystemTime) -> f64 {
    time.duration_since(UNIX_EPOCH).unwrap().as_secs_f64()
}

/// Fails on its first attempt and succeeds on every later one (a manual retry included).
/// Retries are parked an hour away so that only the API brings them back.
#[derive(Default)]
struct Flaky;

impl Job for Flaky {
    const NAME: &'static str = "Flaky";
    type Args = (String,);

    fn retry_in(&self, _count: u32, _error: &JobError, _job: &JobPayload) -> RetryIn {
        RetryIn::After(Duration::from_secs(3600))
    }

    async fn perform(&self, ctx: &JobContext, (value,): (String,)) -> Result<(), JobError> {
        let runs = ctx.data::<Runs>()?;
        match ctx.retry_count() {
            None => {
                runs.record(format!("failed:{value}"));
                Err(JobError::new("FlakyError", "the first attempt fails"))
            }
            Some(count) => {
                runs.record(format!("done:{value}:{count}"));
                Ok(())
            }
        }
    }
}

/// Fails with a chain of causes, keeping its backtrace; its retry is parked an hour away.
#[derive(Default)]
struct Wrapped;

impl Job for Wrapped {
    const NAME: &'static str = "Wrapped";
    type Args = ();

    fn options() -> JobOptions {
        JobOptions::new().backtrace(true)
    }

    fn retry_in(&self, _count: u32, _error: &JobError, _job: &JobPayload) -> RetryIn {
        RetryIn::After(Duration::from_secs(3600))
    }

    async fn perform(&self, _ctx: &JobContext, (): ()) -> Result<(), JobError> {
        let error = anyhow::anyhow!("disk full").context("could not save the report");
        Err(error.into())
    }
}

/// Panics.
#[derive(Default)]
struct Panicky;

impl Job for Panicky {
    const NAME: &'static str = "Panicky";
    type Args = ();

    async fn perform(&self, _ctx: &JobContext, (): ()) -> Result<(), JobError> {
        panic!("boom")
    }
}

/// Server middleware recording `class:queue` for every job it sees.
#[derive(Clone, Default)]
struct Recorder(Arc<Mutex<Vec<String>>>);

impl Recorder {
    fn seen(&self) -> Vec<String> {
        self.0.lock().unwrap().clone()
    }
}

#[async_trait]
impl ServerMiddleware for Recorder {
    async fn call(
        &self,
        ctx: &JobContext,
        job: &mut JobPayload,
        next: ServerNext<'_>,
    ) -> Result<(), JobError> {
        self.0
            .lock()
            .unwrap()
            .push(format!("{}:{}", ctx.class(), ctx.queue()));
        next.run(ctx, job).await
    }
}

#[tokio::test]
async fn the_api_sees_live_processes_and_the_jobs_they_run() {
    let redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    let gate = Arc::new(Gate::default());
    let (hench, launcher, api) = start(
        redis
            .builder()
            .await
            .concurrency(2)
            .queues(["critical", "default"])
            .capsule("serial", |c| c.concurrency(1).queues(["serial"]))
            .tag("billing")
            .label("canary")
            .register(Blocked)
            .data_arc(runs.clone())
            .data_arc(gate.clone()),
    )
    .await;
    let client = hench.client();
    let started = SystemTime::now();
    let first = Blocked::perform_async(client, ("one".into(),))
        .await
        .unwrap()
        .unwrap();
    let second = Blocked::set()
        .queue("serial")
        .perform_async(client, ("two".into(),))
        .await
        .unwrap()
        .unwrap();
    runs.wait_for(2).await;

    // The work state the heartbeat publishes.
    let work = api.work();
    wait_for("both jobs in the work set", WAIT, || async {
        work.list().await.unwrap().len() == 2 && work.size().await.unwrap() == 2
    })
    .await;
    let running = work.list().await.unwrap();
    let by_jid: HashMap<String, &hench::api::Work> = running
        .iter()
        .map(|work| (work.job().jid().unwrap().to_owned(), work))
        .collect();
    assert_eq!(by_jid.len(), 2);
    assert_eq!(by_jid[&first].queue(), "default");
    assert_eq!(by_jid[&second].queue(), "serial");
    assert_eq!(by_jid[&first].job().class(), Some("Blocked"));
    assert_eq!(by_jid[&first].job().args(), [json!("one")]);
    assert_eq!(by_jid[&second].job().args(), [json!("two")]);
    assert_ne!(by_jid[&first].slot(), by_jid[&second].slot());
    for work in &running {
        assert_eq!(work.process_id(), launcher.identity());
        let run_at = secs_since_epoch(work.run_at());
        assert!(
            run_at >= secs_since_epoch(started).floor() && run_at <= hench::now_secs_f64(),
            "{run_at}"
        );
    }
    let found = work.find_work(&first).await.unwrap().unwrap();
    assert_eq!(found.slot(), by_jid[&first].slot());
    assert!(work.find_work("missing").await.unwrap().is_none());

    // The heartbeat hash and the process description.
    let processes = api.processes();
    let listed = processes.list().await.unwrap();
    assert_eq!(listed.len(), 1);
    let process = &listed[0];
    assert_eq!(process.identity(), launcher.identity());
    assert!(
        launcher
            .identity()
            .starts_with(&format!("{}:", process.hostname().unwrap()))
    );
    assert_eq!(process.pid(), Some(u64::from(std::process::id())));
    assert_eq!(process.tag(), Some("billing"));
    assert_eq!(process.labels(), ["canary"]);
    assert_eq!(process.version(), Some(env!("CARGO_PKG_VERSION")));
    assert!(process.is_embedded(), "started with Hench::start");
    assert_eq!(process.concurrency(), 3);
    assert_eq!(process.busy(), 2);
    assert!(!process.is_stopping());
    assert_eq!(process.queues(), ["critical", "default", "serial"]);
    let capsules: Vec<(&str, u64, &str)> = process
        .capsules()
        .iter()
        .map(|capsule| {
            (
                capsule.name.as_str(),
                capsule.concurrency,
                capsule.mode.as_str(),
            )
        })
        .collect();
    assert_eq!(
        capsules,
        [("default", 2, "strict"), ("serial", 1, "strict")]
    );
    assert_eq!(
        process.capsules()[0].weights,
        [("critical".to_owned(), 0), ("default".to_owned(), 0)]
    );
    let started_at = secs_since_epoch(process.started_at().unwrap());
    assert!(started_at <= hench::now_secs_f64());
    let beat = secs_since_epoch(process.beat().unwrap());
    assert!((hench::now_secs_f64() - beat).abs() < 10.0, "{beat}");
    if cfg!(target_os = "linux") {
        assert!(process.rss_kb() > 0);
    }
    assert!(process.rtt_us() < 1_000_000);
    assert_eq!(processes.size().await.unwrap(), 1);
    assert_eq!(processes.total_concurrency().await.unwrap(), 3);
    assert_eq!(
        processes
            .get(launcher.identity())
            .await
            .unwrap()
            .unwrap()
            .identity(),
        launcher.identity()
    );

    let stats = api.stats().await.unwrap();
    assert_eq!(stats.processes_size, 1);
    assert_eq!(stats.workers_size, 2);

    gate.open();
    wait_for("the work set to empty", WAIT, || async {
        work.list().await.unwrap().is_empty() && work.size().await.unwrap() == 0
    })
    .await;
    launcher.stop().await.unwrap();
    assert!(processes.list().await.unwrap().is_empty());
    assert_eq!(processes.size().await.unwrap(), 0);
    assert!(processes.get(launcher.identity()).await.unwrap().is_none());
}

#[tokio::test]
async fn processes_obey_quiet_and_stop_sent_through_the_api() {
    let redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    let (hench, launcher, api) =
        start(redis.builder().await.register(Echo).data_arc(runs.clone())).await;
    let identity = launcher.identity().to_owned();
    let process = api.processes().get(&identity).await.unwrap().unwrap();
    assert!(!process.is_stopping());

    process.quiet().await.unwrap();
    wait_for("the process to go quiet", WAIT, || async {
        launcher.is_quiet()
    })
    .await;
    assert!(!launcher.is_stopping());
    wait_for("the API to see the process as quiet", WAIT, || async {
        api.processes()
            .get(&identity)
            .await
            .unwrap()
            .is_some_and(|process| process.is_stopping())
    })
    .await;

    // A quiet process fetches nothing: a job pushed now waits in its queue.
    Echo::perform_async(hench.client(), ("late".into(),))
        .await
        .unwrap();

    process.stop().await.unwrap();
    tokio::time::timeout(WAIT, launcher.wait())
        .await
        .expect("the remote stop signal stops the process");
    assert!(launcher.is_stopped());
    assert!(api.processes().list().await.unwrap().is_empty());
    assert_eq!(api.processes().size().await.unwrap(), 0);
    assert_eq!(runs.len(), 0);
    assert_eq!(api.queue("default").size().await.unwrap(), 1);
    // Handled signals are removed.
    assert_eq!(llen(hench.redis(), &keys::signals(&identity)).await, 0);
}

#[tokio::test]
async fn stats_and_history_count_what_workers_processed() {
    let redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    let errors = Errors::default();
    let (hench, launcher, api) = start(
        redis
            .builder()
            .await
            .register(Echo)
            .register(Fail)
            .data_arc(runs.clone())
            .error_handler(errors.handler()),
    )
    .await;
    let client = hench.client();
    for value in ["a", "b", "c"] {
        Echo::perform_async(client, (value.into(),)).await.unwrap();
    }
    for _ in 0..2 {
        Fail::set()
            .retry(false)
            .perform_async(client, vec![])
            .await
            .unwrap();
    }
    runs.wait_for(3).await;
    wait_for("both failures to be reported", WAIT, || async {
        errors.with_context("job failed").len() == 2
    })
    .await;
    wait_for("the counters to be flushed", WAIT, || async {
        let stats = api.stats().await.unwrap();
        stats.processed == 5 && stats.failed == 2
    })
    .await;
    let stats = api.stats().await.unwrap();
    assert_eq!(stats.enqueued, 0);
    assert_eq!(stats.retry_size, 0, "retry: false skips the retry set");
    assert_eq!(stats.dead_size, 0, "retry: false skips the morgue");

    // The daily counters (two days, in case the test runs across midnight UTC).
    let before = Utc::now().date_naive();
    let history = StatsHistory::new(&api, 2).unwrap();
    let after = Utc::now().date_naive();
    let processed: u64 = history
        .processed()
        .await
        .unwrap()
        .iter()
        .map(|(_, count)| count)
        .sum();
    let failed: u64 = history
        .failed()
        .await
        .unwrap()
        .iter()
        .map(|(_, count)| count)
        .sum();
    assert_eq!((processed, failed), (5, 2));
    let newest = history.dates()[0];
    assert!(
        newest == before || newest == after,
        "newest day first: {newest}"
    );
    launcher.stop().await.unwrap();
}

#[tokio::test]
async fn retries_written_by_workers_can_be_retried_and_killed_through_the_api() {
    let redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    // `(jid, error class, error message)` of every job given to the death handlers.
    let deaths: Arc<Mutex<Vec<(String, String, String)>>> = Arc::default();
    let recorded = deaths.clone();
    let (hench, launcher, api) = start(
        redis
            .builder()
            .await
            .register(Flaky)
            .data_arc(runs.clone())
            .death_handler(move |job, error| {
                recorded.lock().unwrap().push((
                    job.jid.clone(),
                    error.class().to_owned(),
                    error.message(),
                ));
            }),
    )
    .await;
    let client = hench.client();
    let retries = api.retries();
    let pushed_at = hench::now_secs_f64();
    let first = Flaky::perform_async(client, ("x".into(),))
        .await
        .unwrap()
        .unwrap();
    let second = Flaky::perform_async(client, ("y".into(),))
        .await
        .unwrap()
        .unwrap();
    // A job no worker knows fails like any other job.
    let unknown = client
        .push(JobPayload::new("NoSuchJob", vec![json!(1)]))
        .await
        .unwrap()
        .unwrap();
    wait_for("three jobs in the retry set", WAIT, || async {
        retries.size().await.unwrap() == 3
    })
    .await;

    let entry = retries.find_job(&first).await.unwrap().unwrap();
    assert_eq!(entry.class(), Some("Flaky"));
    assert_eq!(entry.queue(), "default");
    assert_eq!(entry.args(), [json!("x")]);
    assert_eq!(entry.error_class(), Some("FlakyError"));
    assert_eq!(entry.error_message(), Some("the first attempt fails"));
    assert_eq!(entry.retry_count(), Some(0));
    let failed_at = secs_since_epoch(entry.failed_at().unwrap());
    assert!(failed_at >= pushed_at.floor() && failed_at <= hench::now_secs_f64());
    assert!(entry.retried_at().is_none());
    // `retry_in` parked it an hour away, plus Sidekiq's jitter (below 10 s on a first retry).
    assert!(
        entry.score() >= pushed_at + 3600.0 && entry.score() < hench::now_secs_f64() + 3610.0,
        "{}",
        entry.score()
    );
    let missing = retries.find_job(&unknown).await.unwrap().unwrap();
    assert_eq!(missing.error_class(), Some(JobError::UNKNOWN_JOB));
    assert_eq!(missing.class(), Some("NoSuchJob"));

    // A manual retry does not count against the limit: the job runs again with retry count
    // 0 (Sidekiq would store -1; hench's count saturates at 0).
    assert!(entry.retry().await.unwrap());
    wait_for("the retried job to run", WAIT, || async {
        runs.count("done:x:0") == 1
    })
    .await;
    assert!(retries.find_job(&first).await.unwrap().is_none());

    // Killing a retry moves it to the morgue and tells the death handlers.
    let entry = retries.find_job(&second).await.unwrap().unwrap();
    assert!(entry.kill().await.unwrap());
    assert!(!entry.kill().await.unwrap(), "already moved");
    let dead = api.dead();
    let corpse = dead.find_job(&second).await.unwrap().unwrap();
    assert_eq!(corpse.error_class(), Some("FlakyError"));
    assert_eq!(
        *deaths.lock().unwrap(),
        [(
            second.clone(),
            JobError::KILLED.to_owned(),
            "the job was killed through the API".to_owned()
        )]
    );

    // Reviving the morgue sends the job back to a worker, which now succeeds.
    assert_eq!(dead.retry_all().await.unwrap(), 1);
    wait_for("the revived job to run", WAIT, || async {
        runs.count("done:y:0") == 1
    })
    .await;
    assert_eq!(dead.size().await.unwrap(), 0);
    assert_eq!(
        retries.size().await.unwrap(),
        1,
        "only the unknown job is left"
    );
    launcher.stop().await.unwrap();
}

#[tokio::test]
async fn error_details_written_by_workers_decode_through_the_api() {
    let redis = TestRedis::start().await;
    let (hench, launcher, api) = start(redis.builder().await.register(Wrapped)).await;
    let jid = Wrapped::perform_async(hench.client(), ())
        .await
        .unwrap()
        .unwrap();
    let retries = api.retries();
    wait_for("the failed job in the retry set", WAIT, || async {
        retries.size().await.unwrap() == 1
    })
    .await;
    let entry = retries.find_job(&jid).await.unwrap().unwrap();
    assert!(entry.has_error());
    assert_eq!(entry.error_class(), Some("anyhow::Error"));
    assert_eq!(
        entry.error_message(),
        Some("could not save the report: disk full")
    );
    // Compressed by the worker (base64 of zlib'd JSON lines), decoded by the API. Frames
    // follow when the test runs with RUST_BACKTRACE set.
    let lines = entry.error_backtrace().unwrap().unwrap();
    assert_eq!(
        lines.first().map(String::as_str),
        Some("caused by: disk full")
    );
    launcher.stop().await.unwrap();
}

#[tokio::test]
async fn exhausted_jobs_reach_the_morgue_and_can_be_retried_from_it() {
    let redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    let deaths = Deaths::default();
    let (hench, launcher, api) = start(
        redis
            .builder()
            .await
            .register(Flaky)
            .register(Fail)
            .data_arc(runs.clone())
            .death_handler(deaths.handler()),
    )
    .await;
    let client = hench.client();
    let dead = api.dead();
    // `retry: 0`: no retries, straight to the morgue (unlike `retry: false`).
    let flaky = Flaky::set()
        .retry(0)
        .perform_async(client, ("z".into(),))
        .await
        .unwrap()
        .unwrap();
    let doomed = Fail::set()
        .retry(0)
        .perform_async(client, vec![json!("doomed")])
        .await
        .unwrap()
        .unwrap();
    wait_for("both jobs in the morgue", WAIT, || async {
        dead.size().await.unwrap() == 2
    })
    .await;
    // Death handlers run right after the morgue is written.
    wait_for("both deaths reported", WAIT, || async {
        deaths.all().len() == 2
    })
    .await;
    assert_eq!(api.retries().size().await.unwrap(), 0);
    let corpse = dead.find_job(&doomed).await.unwrap().unwrap();
    assert_eq!(corpse.error_class(), Some("RuntimeError"));
    assert_eq!(corpse.error_message(), Some("kerblammo!"));
    assert_eq!(corpse.retry_count(), Some(0));

    // Retried from the morgue: the flaky job succeeds, the doomed one dies again (after one
    // more failure, so with a retry time this time).
    let corpse = dead.find_job(&flaky).await.unwrap().unwrap();
    assert!(corpse.retry().await.unwrap());
    let corpse = dead.find_job(&doomed).await.unwrap().unwrap();
    assert!(corpse.retry().await.unwrap());
    wait_for("the flaky job to succeed", WAIT, || async {
        runs.count("done:z:0") == 1
    })
    .await;
    wait_for("the doomed job to die again", WAIT, || async {
        dead.find_job(&doomed)
            .await
            .unwrap()
            .is_some_and(|corpse| corpse.retried_at().is_some())
    })
    .await;
    assert!(dead.find_job(&flaky).await.unwrap().is_none());
    assert_eq!(dead.size().await.unwrap(), 1);
    wait_for("the second death reported", WAIT, || async {
        deaths.all().len() == 3
    })
    .await;
    launcher.stop().await.unwrap();
}

#[tokio::test]
async fn scheduled_jobs_can_be_run_early_or_rescheduled_through_the_api() {
    let redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    let (hench, launcher, api) =
        start(redis.builder().await.register(Echo).data_arc(runs.clone())).await;
    let client = hench.client();
    let scheduled = api.scheduled();
    let now = hench::now_secs_f64();
    let early = Echo::perform_in(client, Duration::from_secs(3600), ("early".into(),))
        .await
        .unwrap()
        .unwrap();
    let moved = Echo::perform_in(client, Duration::from_secs(7200), ("moved".into(),))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(scheduled.size().await.unwrap(), 2);
    let entry = scheduled.find_job(&early).await.unwrap().unwrap();
    let at = secs_since_epoch(entry.at());
    assert!((at - (now + 3600.0)).abs() < 5.0, "{at}");
    assert!(entry.get("at").is_none() && entry.enqueued_at().is_none());

    // Straight to the queue: a worker runs it right away.
    assert!(entry.add_to_queue().await.unwrap());
    runs.wait_for(1).await;
    assert_eq!(runs.entries(), ["early"]);

    // Rescheduled to now: the workers' scheduler enqueues it at its next poll.
    let mut entry = scheduled.find_job(&moved).await.unwrap().unwrap();
    assert!(entry.reschedule(SystemTime::now()).await.unwrap());
    runs.wait_for(2).await;
    assert_eq!(runs.entries(), ["early", "moved"]);
    assert_eq!(scheduled.size().await.unwrap(), 0);
    launcher.stop().await.unwrap();
}

#[tokio::test]
async fn workers_skip_paused_queues_until_they_are_unpaused() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let api = Api::new(pool.clone());
    let client = Client::new(pool.clone());
    let low = api.queue("low");
    assert!(low.pause().await.unwrap());
    assert!(!low.pause().await.unwrap(), "already paused");
    assert!(low.is_paused().await.unwrap());
    // Both jobs wait before the worker starts, so its first fetch sees both queues: in strict
    // order it would take `low` first if pausing had no effect.
    Echo::set()
        .queue("low")
        .perform_async(&client, ("low".into(),))
        .await
        .unwrap();
    Echo::set()
        .queue("high")
        .perform_async(&client, ("high".into(),))
        .await
        .unwrap();

    let runs = Arc::new(Runs::default());
    let (_hench, launcher, _) = start(
        redis
            .builder()
            .await
            .concurrency(1)
            .queues(["low", "high"])
            .register(Echo)
            .data_arc(runs.clone()),
    )
    .await;
    runs.wait_for(1).await;
    assert_eq!(runs.entries(), ["high"]);
    assert_eq!(low.size().await.unwrap(), 1);
    let summaries = api.stats().await.unwrap();
    assert_eq!(summaries.enqueued, 1);
    let paused: Vec<(String, bool)> = hench::api::Stats::queue_summaries(&api)
        .await
        .unwrap()
        .into_iter()
        .map(|summary| (summary.name, summary.paused))
        .collect();
    assert!(paused.contains(&("low".to_owned(), true)), "{paused:?}");
    assert!(paused.contains(&("high".to_owned(), false)), "{paused:?}");

    assert!(low.unpause().await.unwrap());
    assert!(!low.unpause().await.unwrap(), "no longer paused");
    runs.wait_for(2).await;
    assert_eq!(runs.entries(), ["high", "low"]);
    assert_eq!(low.size().await.unwrap(), 0);
    launcher.stop().await.unwrap();
}

#[tokio::test]
async fn crashed_processes_linger_until_their_heartbeat_expires_and_are_cleaned_up() {
    let redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    let (hench, launcher, api) =
        start(redis.builder().await.register(Echo).data_arc(runs.clone())).await;
    let pool = hench.redis().clone();
    let identity = launcher.identity().to_owned();
    // The process dies without cleaning up.
    launcher.abandon();
    let processes = api.processes();
    assert_eq!(
        processes.list().await.unwrap().len(),
        1,
        "heartbeat still alive"
    );
    // Nothing to prune while its heartbeat lives; whoever cleaned up first (its scheduler at
    // startup, or this call) holds the minute-long lock.
    assert_eq!(processes.cleanup().await.unwrap(), 0);
    assert!(exists(&pool, keys::PROCESS_CLEANUP).await);

    // A minute later its heartbeat has expired, and so has the lock.
    del(&pool, &identity).await;
    del(&pool, keys::PROCESS_CLEANUP).await;
    assert!(processes.list().await.unwrap().is_empty());
    assert_eq!(processes.size().await.unwrap(), 1, "still in the set");
    assert_eq!(processes.cleanup().await.unwrap(), 1);
    assert_eq!(processes.size().await.unwrap(), 0);
    assert_eq!(processes.cleanup().await.unwrap(), 0, "once a minute");
}

#[tokio::test]
async fn testing_clients_from_a_hench_execute_jobs_like_its_workers() {
    let redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    let recorder = Recorder::default();
    let (hench, launcher, api) = start(
        redis
            .builder()
            .await
            .register(Echo)
            .register(Panicky)
            .server_middleware(recorder.clone())
            .data_arc(runs.clone()),
    )
    .await;

    // The workers.
    Echo::perform_async(hench.client(), ("worker".into(),))
        .await
        .unwrap();
    let panicked = Panicky::perform_async(hench.client(), ())
        .await
        .unwrap()
        .unwrap();
    runs.wait_for(1).await;
    let retries = api.retries();
    wait_for("the panicked job in the retry set", WAIT, || async {
        retries.find_job(&panicked).await.unwrap().is_some()
    })
    .await;
    let entry = retries.find_job(&panicked).await.unwrap().unwrap();
    assert_eq!(entry.error_class(), Some(JobError::PANIC));
    assert_eq!(entry.error_message(), Some("boom"));
    launcher.stop().await.unwrap();
    // Several processors: the two jobs may have run in either order.
    let mut seen = recorder.seen();
    seen.sort();
    assert_eq!(seen, ["Echo:default", "Panicky:default"]);

    // An inline testing client built from the same configuration: same middleware, same
    // data, same panic handling, but no Redis.
    let inline = Testing::from_hench(&hench).inline();
    Echo::perform_async(&inline, ("inline".into(),))
        .await
        .unwrap();
    assert_eq!(runs.entries(), ["worker", "inline"]);
    match Panicky::perform_async(&inline, ()).await {
        Err(Error::Job(error)) => {
            assert_eq!(error.class(), JobError::PANIC);
            assert_eq!(error.message(), "boom");
        }
        other => panic!("expected the panic as a job error, got {other:?}"),
    }
    assert_eq!(recorder.seen()[2..], ["Echo:default", "Panicky:default"]);

    // Unknown classes differ on purpose: workers run the middleware and fail the job with
    // `hench::UnknownJob` (it is retried, as above); testing clients report the class before
    // running anything, like Sidekiq's testing modes.
    match inline.push(JobPayload::new("NoSuchJob", vec![])).await {
        Err(Error::UnknownJob(class)) => assert_eq!(class, "NoSuchJob"),
        other => panic!("expected an unknown job error, got {other:?}"),
    }
    assert_eq!(recorder.seen().len(), 4);
}
