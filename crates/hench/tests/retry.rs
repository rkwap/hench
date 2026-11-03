//! Retries and the morgue (Sidekiq's `retry_test.rb` and `retry_exhausted_test.rb`
//! scenarios), end to end: jobs fail in a running process and the tests look at the retry
//! set, the morgue and the handlers.

mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use hench::prelude::*;
use hench::test_util::TestRedis;
use hench::{Exhausted, Hench, Launcher, RetryIn, RetryOption, keys};
use serde_json::json;
use support::*;

/// Fails with a two-level cause chain and a class chosen by its argument; records its
/// `retries_exhausted` calls.
struct Flaky {
    exhausted: ExhaustedLog,
}

#[derive(Debug, thiserror::Error)]
#[error("kerblammo!")]
struct Kerblammo {
    #[source]
    source: std::io::Error,
}

impl Job for Flaky {
    const NAME: &'static str = "Flaky";
    type Args = Vec<serde_json::Value>;

    async fn perform(&self, _: &JobContext, args: Self::Args) -> Result<(), JobError> {
        let class = args
            .first()
            .and_then(|class| class.as_str())
            .unwrap_or("RuntimeError")
            .to_owned();
        let error = Kerblammo {
            source: std::io::Error::other("disk on fire"),
        };
        Err(JobError::from(error).with_class(class))
    }

    fn retry_in(&self, count: u32, error: &JobError, _: &JobPayload) -> RetryIn {
        match error.class() {
            "Kill" => RetryIn::Kill,
            "Discard" => RetryIn::Discard,
            "Slow" => RetryIn::After(Duration::from_secs(u64::from(count + 1) * 1000)),
            "Forever" => RetryIn::After(Duration::MAX),
            _ => RetryIn::Default,
        }
    }

    async fn retries_exhausted(&self, job: &JobPayload, error: &JobError) -> Exhausted {
        self.exhausted.lock().unwrap().push((
            job.jid.clone(),
            job.error_message.clone(),
            error.message(),
        ));
        if job.args.get(1) == Some(&json!("discard")) {
            Exhausted::Discard
        } else {
            Exhausted::Default
        }
    }
}

type ExhaustedLog = Arc<Mutex<Vec<(String, Option<String>, String)>>>;

struct Fixture {
    _redis: TestRedis,
    hench: Hench,
    launcher: Launcher,
    deaths: Deaths,
    errors: Errors,
    exhausted: ExhaustedLog,
}

impl Fixture {
    async fn new(configure: impl FnOnce(hench::HenchBuilder) -> hench::HenchBuilder) -> Self {
        init_logging();
        let exhausted = ExhaustedLog::default();
        let redis = TestRedis::start().await;
        let deaths = Deaths::default();
        let errors = Errors::default();
        let builder = redis
            .builder()
            .await
            .register(Flaky {
                exhausted: exhausted.clone(),
            })
            .death_handler(deaths.handler())
            .error_handler(errors.handler());
        let hench = configure(builder).build().await.unwrap();
        let launcher = hench.start().await.unwrap();
        Self {
            _redis: redis,
            hench,
            launcher,
            deaths,
            errors,
            exhausted,
        }
    }

    fn pool(&self) -> &RedisPool {
        self.hench.redis()
    }

    /// Pushes a failing job built from `job` and waits until it has been handled.
    async fn fail(&self, mut job: JobPayload) -> String {
        if job.class.is_empty() {
            job.class = "Flaky".into();
        }
        let failures = self.errors.with_context("job failed").len();
        let jid = self.hench.client().push(job).await.unwrap().unwrap();
        wait_for("the failure to be handled", WAIT, || async {
            self.errors.with_context("job failed").len() > failures
        })
        .await;
        jid
    }

    async fn retries(&self) -> Vec<(JobPayload, f64)> {
        sorted_jobs(self.pool(), keys::RETRY).await
    }

    async fn dead(&self) -> Vec<(JobPayload, f64)> {
        sorted_jobs(self.pool(), keys::DEAD).await
    }

    async fn stop(self) {
        self.launcher.stop().await.unwrap();
    }
}

fn job(retry: impl Into<RetryOption>) -> JobPayload {
    let mut job = JobPayload::new("Flaky", vec![]);
    job.retry = Some(retry.into());
    job
}

#[tokio::test]
async fn a_new_failure_records_the_error() {
    let fixture = Fixture::new(|b| b).await;
    let before = hench::now_millis();
    let jid = fixture.fail(job(true)).await;
    let retries = fixture.retries().await;
    assert_eq!(retries.len(), 1);
    let (retried, score) = &retries[0];
    assert_eq!(retried.jid, jid);
    assert_eq!(retried.queue, "default");
    assert_eq!(
        retried.error_message.as_deref(),
        Some("kerblammo!: disk on fire")
    );
    assert_eq!(retried.error_class.as_deref(), Some("RuntimeError"));
    assert_eq!(retried.retry_count, Some(0));
    assert!(retried.failed_at.unwrap() >= before);
    assert_eq!(retried.retried_at, None);
    assert_eq!(retried.error_backtrace, None);
    // 15 s plus a jitter below 10 s.
    let delay = score - hench::now_secs_f64();
    assert!((13.0..26.0).contains(&delay), "{delay}");
    assert!(fixture.dead().await.is_empty());
    assert!(fixture.deaths.all().is_empty());
    fixture.stop().await;
}

#[tokio::test]
async fn unknown_classes_are_retried_like_any_job() {
    let fixture = Fixture::new(|b| b).await;
    let mut unknown = job(true);
    unknown.class = "Bob".into();
    fixture.fail(unknown).await;
    let retries = fixture.retries().await;
    assert_eq!(retries.len(), 1);
    assert_eq!(
        retries[0].0.error_class.as_deref(),
        Some(JobError::UNKNOWN_JOB)
    );
    fixture.stop().await;
}

#[tokio::test]
async fn retries_can_be_disabled() {
    let fixture = Fixture::new(|b| b).await;
    fixture.fail(job(false)).await;
    assert!(fixture.retries().await.is_empty());
    assert!(fixture.dead().await.is_empty());
    assert_eq!(fixture.deaths.all().len(), 1);
    fixture.stop().await;
}

#[tokio::test]
async fn numeric_retry_limits() {
    let fixture = Fixture::new(|b| b).await;
    fixture.fail(job(2_u32)).await;
    assert_eq!(fixture.retries().await.len(), 1);
    assert!(fixture.dead().await.is_empty());
    // `retry: 0` goes straight to the morgue.
    fixture.fail(job(0_u32)).await;
    assert_eq!(fixture.retries().await.len(), 1);
    assert_eq!(fixture.dead().await.len(), 1);
    assert_eq!(fixture.deaths.all().len(), 1);
    fixture.stop().await;
}

#[tokio::test]
async fn retry_for_limits_by_time() {
    let fixture = Fixture::new(|b| b).await;
    let mut within = job(true);
    within.retry_for = Some(60);
    fixture.fail(within).await;
    assert_eq!(fixture.retries().await.len(), 1);
    assert!(fixture.dead().await.is_empty());

    let mut expired = job(true);
    expired.retry_for = Some(60);
    expired.retry_count = Some(0);
    expired.failed_at = Some(hench::now_millis() - 61_000);
    fixture.fail(expired).await;
    assert_eq!(fixture.retries().await.len(), 1);
    assert_eq!(fixture.dead().await.len(), 1);
    fixture.stop().await;
}

#[tokio::test]
async fn retry_for_ignores_max_retries() {
    let fixture = Fixture::new(|b| b.max_retries(2)).await;
    let mut job = job(true);
    job.retry_for = Some(3600);
    job.retry_count = Some(5);
    job.failed_at = Some(hench::now_millis());
    fixture.fail(job).await;
    assert_eq!(fixture.retries().await.len(), 1);
    assert!(fixture.dead().await.is_empty());
    fixture.stop().await;
}

#[tokio::test]
async fn max_retries_is_configurable() {
    let fixture = Fixture::new(|b| b.max_retries(7)).await;
    for attempt in 1..=8_u32 {
        let mut job = job(true);
        job.retry_count = attempt.checked_sub(2);
        fixture.fail(job).await;
    }
    assert_eq!(fixture.retries().await.len(), 7);
    assert_eq!(fixture.dead().await.len(), 1);
    fixture.stop().await;
}

#[tokio::test]
async fn the_default_limit_is_25_retries() {
    let fixture = Fixture::new(|b| b).await;
    let mut job = job(true);
    job.retry_count = Some(25);
    job.failed_at = Some(hench::now_millis());
    job.error_message = Some("kerblammo!".into());
    fixture.fail(job).await;
    assert!(fixture.retries().await.is_empty());
    assert_eq!(queue_size(fixture.pool(), "default").await, 0);
    let dead = fixture.dead().await;
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].0.retry_count, Some(26));
    fixture.stop().await;
}

#[tokio::test]
async fn recurring_failures_increment_the_retry_count() {
    let fixture = Fixture::new(|b| b).await;
    let failed_at = hench::now_millis() - 5_000;
    let mut job = job(true);
    job.retry_count = Some(10);
    job.failed_at = Some(failed_at);
    job.error_message = Some("old".into());
    job.error_class = Some("OldError".into());
    fixture.fail(job).await;
    let (retried, score) = fixture.retries().await.remove(0);
    assert_eq!(retried.retry_count, Some(11));
    assert_eq!(retried.failed_at, Some(failed_at));
    assert!(retried.retried_at.unwrap() > failed_at);
    assert_eq!(
        retried.error_message.as_deref(),
        Some("kerblammo!: disk on fire")
    );
    assert_eq!(retried.error_class.as_deref(), Some("RuntimeError"));
    // 11⁴ + 15 seconds plus a jitter below 120 s.
    let delay = score - hench::now_secs_f64();
    assert!(delay > 14_640.0 && delay < 14_776.0, "{delay}");
    fixture.stop().await;
}

#[tokio::test]
async fn retries_can_go_to_another_queue() {
    let fixture = Fixture::new(|b| b).await;
    let mut job = job(true);
    job.retry_queue = Some("retryx".into());
    fixture.fail(job).await;
    let (retried, _) = fixture.retries().await.remove(0);
    assert_eq!(retried.queue, "retryx");
    fixture.stop().await;
}

#[tokio::test]
async fn backtraces_are_saved_when_enabled() {
    let fixture = Fixture::new(|b| b).await;
    let mut all = job(true);
    all.backtrace = Some(true.into());
    all.args = vec![json!("Full")];
    fixture.fail(all).await;
    let mut partial = job(true);
    partial.backtrace = Some(1_u32.into());
    partial.args = vec![json!("Partial")];
    fixture.fail(partial).await;
    let retries = fixture.retries().await;
    let by_class = |class: &str| {
        retries
            .iter()
            .find(|(job, _)| job.error_class.as_deref() == Some(class))
            .map(|(job, _)| job.clone())
            .unwrap()
    };
    let full =
        hench::decompress_backtrace(by_class("Full").error_backtrace.as_ref().unwrap()).unwrap();
    assert_eq!(
        full.first().map(String::as_str),
        Some("caused by: disk on fire")
    );
    let partial =
        hench::decompress_backtrace(by_class("Partial").error_backtrace.as_ref().unwrap()).unwrap();
    assert_eq!(partial, ["caused by: disk on fire"]);
    fixture.stop().await;
}

#[tokio::test]
async fn retry_in_can_kill_discard_or_delay() {
    let fixture = Fixture::new(|b| b).await;
    let mut kill = job(true);
    kill.args = vec![json!("Kill")];
    fixture.fail(kill).await;
    assert_eq!(fixture.dead().await.len(), 1);

    let mut discard = job(true);
    discard.args = vec![json!("Discard")];
    let jid = fixture.fail(discard).await;
    assert_eq!(fixture.dead().await.len(), 1);
    assert!(fixture.retries().await.is_empty());
    let deaths = fixture.deaths.all();
    let discarded = deaths.iter().find(|(job, _)| job.jid == jid).unwrap();
    assert!(discarded.0.discarded_at.is_some());

    let mut slow = job(true);
    slow.args = vec![json!("Slow")];
    fixture.fail(slow).await;
    let (_, score) = fixture.retries().await.remove(0);
    // count 0: 1000 s plus a jitter below 10 s.
    let delay = score - hench::now_secs_f64();
    assert!(delay > 998.0 && delay < 1_011.0, "{delay}");
    // Kill and Discard both ended the job's life.
    assert_eq!(fixture.deaths.all().len(), 2);
    // Round 3 (web-api-security-3): a delay beyond reason retries at the end of the year 9999,
    // the latest time a job may have (it used to store a score of 1.8e19 seconds, which the
    // batch pages and `hench batches show` panicked on).
    let mut forever = job(true);
    forever.args = vec![json!("Forever")];
    let jid = fixture.fail(forever).await;
    let (_, score) = fixture
        .retries()
        .await
        .into_iter()
        .find(|(job, _)| job.jid == jid)
        .unwrap();
    assert_eq!(score, 253_402_300_799.0);
    fixture.stop().await;
}

#[tokio::test]
async fn retries_exhausted_runs_only_when_retries_are_exhausted() {
    let fixture = Fixture::new(|b| b).await;
    // Retries remain: not called.
    fixture.fail(job(1_u32)).await;
    assert!(fixture.exhausted.lock().unwrap().is_empty());
    // Exhausted: called with the job (error fields set) and the error.
    let mut last = job(1_u32);
    last.retry_count = Some(0);
    let jid = fixture.fail(last).await;
    let calls = fixture.exhausted.lock().unwrap().clone();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0, jid);
    assert_eq!(calls[0].1.as_deref(), Some("kerblammo!: disk on fire"));
    assert_eq!(calls[0].2, "kerblammo!: disk on fire");
    assert_eq!(fixture.dead().await.len(), 1);
    // The global death handlers ran as well.
    let deaths = fixture.deaths.all();
    assert_eq!(deaths.len(), 1);
    assert_eq!(deaths[0].0.jid, jid);
    assert_eq!(deaths[0].1, "kerblammo!: disk on fire");
    fixture.stop().await;
}

#[tokio::test]
async fn retries_exhausted_can_discard_the_job() {
    let fixture = Fixture::new(|b| b).await;
    let mut job = job(0_u32);
    job.args = vec![json!("RuntimeError"), json!("discard")];
    fixture.fail(job).await;
    assert_eq!(fixture.exhausted.lock().unwrap().len(), 1);
    assert!(fixture.dead().await.is_empty());
    let deaths = fixture.deaths.all();
    assert_eq!(deaths.len(), 1);
    assert!(deaths[0].0.discarded_at.is_some());
    fixture.stop().await;
}

#[tokio::test]
async fn dead_false_keeps_jobs_out_of_the_morgue() {
    let fixture = Fixture::new(|b| b).await;
    let mut job = job(0_u32);
    job.dead = Some(false);
    fixture.fail(job).await;
    assert!(fixture.dead().await.is_empty());
    let deaths = fixture.deaths.all();
    assert_eq!(deaths.len(), 1);
    assert!(deaths[0].0.discarded_at.is_some());
    fixture.stop().await;
}

#[tokio::test]
async fn the_morgue_is_trimmed_by_size_and_age() {
    let fixture = Fixture::new(|b| b.dead_max_jobs(3).dead_timeout(Duration::from_secs(10))).await;
    // An entry older than dead_timeout disappears with the next death.
    let mut ancient = JobPayload::new("Old", vec![]);
    ancient.jid = "000000000000000000000001".into();
    zadd(
        fixture.pool(),
        keys::DEAD,
        hench::now_secs_f64() - 11.0,
        &ancient,
    )
    .await;
    let mut jids = Vec::new();
    for _ in 0..3 {
        jids.push(fixture.fail(job(0_u32)).await);
    }
    let dead: Vec<String> = fixture
        .dead()
        .await
        .into_iter()
        .map(|(job, _)| job.jid)
        .collect();
    // Like Sidekiq, the size trim keeps the newest `dead_max_jobs - 1` jobs.
    assert_eq!(dead, jids[1..]);
    fixture.stop().await;
}

#[tokio::test]
async fn every_failure_reaches_the_error_handlers() {
    let fixture = Fixture::new(|b| b).await;
    fixture.fail(job(true)).await;
    fixture.fail(job(false)).await;
    fixture.fail(job(0_u32)).await;
    let reported = fixture.errors.with_context("job failed");
    assert_eq!(reported.len(), 3);
    assert!(
        reported
            .iter()
            .all(|(_, class, jid)| class == "RuntimeError" && jid.is_some())
    );
    fixture.stop().await;
}

#[tokio::test]
async fn long_error_messages_are_truncated() {
    #[derive(Default)]
    struct Verbose;
    impl Job for Verbose {
        const NAME: &'static str = "Verbose";
        type Args = ();
        async fn perform(&self, _: &JobContext, (): ()) -> Result<(), JobError> {
            Err(JobError::msg("é".repeat(20_000)))
        }
    }
    let fixture = Fixture::new(|b| b.register(Verbose)).await;
    fixture.fail(JobPayload::new("Verbose", vec![])).await;
    let (retried, _) = fixture.retries().await.remove(0);
    assert_eq!(
        retried.error_message.unwrap().chars().count(),
        hench::MAX_ERROR_MESSAGE_CHARS
    );
    fixture.stop().await;
}

/// Fails at once; `retry: 0` sends it straight to the morgue.
#[derive(Default)]
struct DiesAtOnce;

impl Job for DiesAtOnce {
    const NAME: &'static str = "DiesAtOnce";
    type Args = ();
    fn options() -> JobOptions {
        JobOptions::new().retry(0)
    }
    async fn perform(&self, _ctx: &JobContext, (): ()) -> Result<(), JobError> {
        Err(JobError::msg("boom"))
    }
}

/// With reliable fetch a failed job's outcome (here the morgue) and its acknowledgement are
/// written in one step, before the death handlers run: a hard shutdown (or a crash) while a
/// slow death handler runs no longer finds the job still in progress and pushes it back to its
/// queue, where it would run again although it is dead.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dead_job_is_acknowledged_before_the_death_handlers_run() {
    init_logging();
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let entered = Arc::new(Mutex::new(None));
    let seen = entered.clone();
    let identity = Arc::new(Mutex::new(String::new()));
    let me = identity.clone();
    let probe = redis.pool().await;
    let hench = redis
        .builder()
        .await
        .concurrency(1)
        .timeout(Duration::from_secs(1))
        .auto_register(false)
        .register(DiesAtOnce)
        // A slow synchronous call (an alerting service, say), made the well-behaved way:
        // `block_in_place` keeps the rest of the runtime running.
        .death_handler(move |_job, _error| {
            let key = keys::inprogress(&me.lock().unwrap());
            let probe = probe.clone();
            tokio::task::block_in_place(|| {
                let in_progress =
                    tokio::runtime::Handle::current().block_on(async { llen(&probe, &key).await });
                *seen.lock().unwrap() = Some(in_progress);
                std::thread::sleep(Duration::from_secs(4));
            });
        })
        .build()
        .await
        .unwrap();
    let launcher = hench.start().await.unwrap();
    *identity.lock().unwrap() = launcher.identity().to_owned();
    DiesAtOnce::perform_async(hench.client(), ()).await.unwrap();
    wait_for("the death handler", WAIT, || async {
        entered.lock().unwrap().is_some()
    })
    .await;
    // Already out of the in-progress list when the death handlers run.
    assert_eq!(*entered.lock().unwrap(), Some(0));
    assert_eq!(zcard(&pool, keys::DEAD).await, 1);
    // The shutdown reaches its deadline while the handler runs.
    launcher.stop().await.unwrap();
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert_eq!(zcard(&pool, keys::DEAD).await, 1);
    assert_eq!(
        queue_size(&pool, "default").await,
        0,
        "the dead job was queued again"
    );
    assert_eq!(llen(&pool, &keys::inprogress(launcher.identity())).await, 0);
}

/// Line of the panic in `PanicsHere::perform`.
const PANIC_LINE: u32 = line!() + 14;

/// Panics (index out of bounds); keeps its backtrace.
#[derive(Default)]
struct PanicsHere;

impl Job for PanicsHere {
    const NAME: &'static str = "PanicsHere";
    type Args = ();
    fn options() -> JobOptions {
        JobOptions::new().backtrace(true)
    }
    async fn perform(&self, _ctx: &JobContext, (): ()) -> Result<(), JobError> {
        let empty: Vec<u8> = std::hint::black_box(Vec::new());
        let _ = empty[3];
        Ok(())
    }
}

/// Line of the failing `?` in `FailsHere::perform`.
const ERROR_LINE: u32 = line!() + 13;

/// Fails parsing a number; keeps its backtrace.
#[derive(Default)]
struct FailsHere;

impl Job for FailsHere {
    const NAME: &'static str = "FailsHere";
    type Args = ();
    fn options() -> JobOptions {
        JobOptions::new().backtrace(true)
    }
    async fn perform(&self, _ctx: &JobContext, (): ()) -> Result<(), JobError> {
        let parsed: u32 = std::hint::black_box("not a number").parse()?;
        let _ = parsed;
        Ok(())
    }
}

/// The stored backtrace of a job that enables `backtrace` tells where it failed, whatever the
/// environment (it used to be empty without `RUST_BACKTRACE`/`RUST_LIB_BACKTRACE`, and a panic's
/// never reached the panicking line).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stored_backtraces_show_where_jobs_failed() {
    init_logging();
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let hench = redis
        .builder()
        .await
        .auto_register(false)
        .register(PanicsHere)
        .register(FailsHere)
        .build()
        .await
        .unwrap();
    let launcher = hench.start().await.unwrap();
    PanicsHere::perform_async(hench.client(), ()).await.unwrap();
    FailsHere::perform_async(hench.client(), ()).await.unwrap();
    wait_for("two retries", WAIT, || async {
        zcard(&pool, keys::RETRY).await == 2
    })
    .await;
    launcher.stop().await.unwrap();
    let file = std::path::Path::new(file!())
        .file_name()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    for (job, _) in sorted_jobs(&pool, keys::RETRY).await {
        let lines = hench::decompress_backtrace(job.error_backtrace.as_deref().unwrap()).unwrap();
        // Frames are known by their locations: their names depend on the platform's
        // symbolization (`<retry::FailsHere as hench::job::Job>::perform::{closure#0}`, or a
        // bare `{async_fn#0}` for a frame inlined on macOS).
        if job.class == "PanicsHere" {
            assert_eq!(job.error_class.as_deref(), Some(JobError::PANIC));
            let location = format!("panicked at {}:{PANIC_LINE}:", file!());
            assert!(lines[0].starts_with(&location), "{lines:?}");
            // The job's own frame, at the panicking line.
            let site = format!("{file}:{PANIC_LINE}:");
            assert!(
                lines[1..].iter().any(|line| line.contains(&site)),
                "{lines:?}"
            );
        } else {
            // From the job's own frame, at the failing line.
            let site = format!("{file}:{ERROR_LINE}:");
            assert!(lines[0].contains(&site), "{lines:?}");
        }
    }
}

/// The jids `Killable::retries_exhausted` saw.
type KilledLog = Arc<Mutex<Vec<String>>>;

/// Fails with the class named by its first argument; `Kill` asks to be killed; its
/// `retries_exhausted` records the jid and discards the job when the second argument is
/// `"discard"`.
struct Killable {
    exhausted: KilledLog,
}

impl Job for Killable {
    const NAME: &'static str = "Killable";
    type Args = Vec<serde_json::Value>;

    async fn perform(&self, _: &JobContext, args: Self::Args) -> Result<(), JobError> {
        let class = args
            .first()
            .and_then(|class| class.as_str())
            .unwrap_or("RuntimeError")
            .to_owned();
        Err(JobError::new(class, "kerblammo!"))
    }

    fn retry_in(&self, _count: u32, error: &JobError, _: &JobPayload) -> RetryIn {
        match error.class() {
            "Kill" => RetryIn::Kill,
            _ => RetryIn::Default,
        }
    }

    async fn retries_exhausted(&self, job: &JobPayload, _error: &JobError) -> Exhausted {
        self.exhausted.lock().unwrap().push(job.jid.clone());
        if job.args.get(1) == Some(&json!("discard")) {
            Exhausted::Discard
        } else {
            Exhausted::Default
        }
    }
}

/// `RetryIn::Kill` goes through `retries_exhausted` like Sidekiq's `:kill` (Sidekiq's "supports
/// kill" with "runs exhausted block when retries exhausted"): the hook runs and may discard the
/// job, and `dead: false` keeps the job out of the morgue.
#[tokio::test]
async fn retry_in_kill_runs_the_retries_exhausted_hook() {
    init_logging();
    let exhausted = KilledLog::default();
    let redis = TestRedis::start().await;
    let deaths = Deaths::default();
    let errors = Errors::default();
    let hench = redis
        .builder()
        .await
        .register(Killable {
            exhausted: exhausted.clone(),
        })
        .death_handler(deaths.handler())
        .error_handler(errors.handler())
        .build()
        .await
        .unwrap();
    let launcher = hench.start().await.unwrap();
    let fail = |args: Vec<serde_json::Value>, dead: Option<bool>| {
        let (hench, errors) = (&hench, &errors);
        async move {
            let mut job = JobPayload::new("Killable", args);
            job.retry = Some(RetryOption::Bool(true));
            job.dead = dead;
            let failures = errors.with_context("job failed").len();
            let jid = hench.client().push(job).await.unwrap().unwrap();
            wait_for("the failure to be handled", WAIT, || async {
                errors.with_context("job failed").len() > failures
            })
            .await;
            jid
        }
    };
    let dead = || async {
        sorted_jobs(hench.redis(), keys::DEAD)
            .await
            .into_iter()
            .map(|(job, _)| job.jid)
            .collect::<Vec<_>>()
    };
    // Killed by retry_in: the hook runs, then the job goes to the morgue.
    let killed = fail(vec![json!("Kill")], None).await;
    assert_eq!(*exhausted.lock().unwrap(), std::slice::from_ref(&killed));
    assert_eq!(dead().await, std::slice::from_ref(&killed));
    // The hook can still discard a job killed by retry_in.
    let discarded = fail(vec![json!("Kill"), json!("discard")], None).await;
    assert_eq!(exhausted.lock().unwrap().len(), 2);
    assert_eq!(dead().await, std::slice::from_ref(&killed));
    let all = deaths.all();
    let death = all.iter().find(|(job, _)| job.jid == discarded).unwrap();
    assert!(death.0.discarded_at.is_some());
    // `dead: false` keeps a job killed by retry_in out of the morgue too.
    let kept_out = fail(vec![json!("Kill")], Some(false)).await;
    assert_eq!(dead().await, [killed]);
    let all = deaths.all();
    let death = all.iter().find(|(job, _)| job.jid == kept_out).unwrap();
    assert!(death.0.discarded_at.is_some());
    assert_eq!(deaths.all().len(), 3);
    assert!(sorted_jobs(hench.redis(), keys::RETRY).await.is_empty());
    launcher.stop().await.unwrap();
}

const MORGUE_TIMEOUT: Duration = Duration::from_secs(60);

/// Plants one morgue entry older than `MORGUE_TIMEOUT` and one younger; returns their jids.
async fn plant(pool: &RedisPool) -> (String, String) {
    let now = hench::now_secs_f64();
    let mut old = JobPayload::new("Old", vec![]);
    old.jid = "00000000000000000000old1".into();
    zadd(pool, keys::DEAD, now - 120.0, &old).await;
    let mut young = JobPayload::new("Young", vec![]);
    young.jid = "000000000000000000young1".into();
    zadd(pool, keys::DEAD, now - 30.0, &young).await;
    (old.jid, young.jid)
}

/// A death trims the morgue by age on its own (`dead_max_jobs` is far above the number of
/// entries, so the size trim cannot hide a missing age trim), with either fetch strategy.
#[tokio::test]
async fn a_death_trims_the_morgue_by_age_alone() {
    for strategy in [hench::FetchStrategy::Reliable, hench::FetchStrategy::Basic] {
        init_logging();
        let redis = TestRedis::start().await;
        let pool = redis.pool().await;
        let (old, young) = plant(&pool).await;
        let errors = Errors::default();
        let hench = redis
            .builder()
            .await
            .fetch(strategy)
            .register(Fail)
            .error_handler(errors.handler())
            .dead_max_jobs(100)
            .dead_timeout(MORGUE_TIMEOUT)
            .build()
            .await
            .unwrap();
        let launcher = hench.start().await.unwrap();
        let mut job = JobPayload::new("Fail", vec![]);
        job.retry = Some(RetryOption::Count(0));
        let jid = hench.client().push(job).await.unwrap().unwrap();
        wait_for("the failure to be handled", WAIT, || async {
            errors.with_context("job failed").len() == 1
        })
        .await;
        let dead: Vec<String> = sorted_jobs(&pool, keys::DEAD)
            .await
            .into_iter()
            .map(|(job, _)| job.jid)
            .collect();
        assert_eq!(
            dead,
            [young, jid],
            "{strategy:?}: the entry older than dead_timeout ({old}) must go"
        );
        launcher.stop().await.unwrap();
    }
}

/// Orphan recovery burying a job that cannot be parsed trims the morgue by age too.
#[tokio::test]
async fn orphan_recovery_trims_the_morgue_by_age() {
    init_logging();
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let (old, young) = plant(&pool).await;
    // A crashed process (no heartbeat key) holding a job that cannot be parsed.
    let owner = "crashed:1:0123456789ab";
    let _: i64 = cmd(
        &pool,
        &redis_cmd!("LPUSH", keys::inprogress(owner), "not json"),
    )
    .await;
    let _: i64 = cmd(&pool, &redis_cmd!("SADD", keys::INPROGRESS, owner)).await;
    let hench = redis
        .builder()
        .await
        .dead_max_jobs(100)
        .dead_timeout(MORGUE_TIMEOUT)
        .build()
        .await
        .unwrap();
    let launcher = hench.start().await.unwrap();
    let members = || async {
        zrange(&pool, keys::DEAD)
            .await
            .into_iter()
            .map(|(member, _)| String::from_utf8_lossy(&member).into_owned())
            .collect::<Vec<_>>()
    };
    wait_for("the orphan in the morgue", WAIT, || async {
        members().await.iter().any(|member| member == "not json")
    })
    .await;
    let members = members().await;
    assert!(
        !members.iter().any(|member| member.contains(&old)),
        "the entry older than dead_timeout must go: {members:?}"
    );
    assert!(
        members.iter().any(|member| member.contains(&young)),
        "{members:?}"
    );
    assert_eq!(members.len(), 2, "{members:?}");
    launcher.stop().await.unwrap();
}

/// Lines of `FailsDeep`'s frames, innermost first: the failing `?` in `parse`, the call of
/// `parse` in `step`, the call of `step` in `perform`.
const DEEP_LINES: [u32; 3] = [line!() + 8, line!() + 12, line!() + 23];

/// Fails two calls deep; keeps its backtrace.
#[derive(Default)]
struct FailsDeep;

impl FailsDeep {
    fn parse(text: &str) -> Result<u32, JobError> {
        Ok(text.parse::<u32>()?)
    }

    fn step(text: &str) -> Result<u32, JobError> {
        Self::parse(text)
    }
}

impl Job for FailsDeep {
    const NAME: &'static str = "FailsDeep";
    type Args = ();
    fn options() -> JobOptions {
        JobOptions::new().backtrace(true)
    }
    async fn perform(&self, _ctx: &JobContext, (): ()) -> Result<(), JobError> {
        Self::step(std::hint::black_box("not a number"))?;
        Ok(())
    }
}

/// A stored backtrace ends where hench called the job: its middleware chain, its panic
/// catching, the processor and tokio below them made up about 95% of it. A
/// `backtrace_cleaner` (Sidekiq's) then filters what is stored; one that panics is reported
/// and the backtrace is stored as it was.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stored_backtraces_end_with_the_job_and_can_be_cleaned() {
    init_logging();
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    for cleaner in ["none", "cleans", "panics"] {
        let errors = Errors::default();
        let mut builder = redis
            .builder()
            .await
            .auto_register(false)
            .register(FailsDeep)
            .error_handler(errors.handler());
        builder = match cleaner {
            // Keeps the application's frames, by their file, as backtrace cleaners do.
            "cleans" => builder.backtrace_cleaner(|lines| {
                lines
                    .into_iter()
                    .filter(|line| line.contains("retry.rs:"))
                    .map(|line| format!("app: {line}"))
                    .collect()
            }),
            "panics" => builder.backtrace_cleaner(|_| panic!("the cleaner has a bug")),
            _ => builder,
        };
        let hench = builder.build().await.unwrap();
        let launcher = hench.start().await.unwrap();
        FailsDeep::perform_async(hench.client(), ()).await.unwrap();
        wait_for("the retry", WAIT, || async {
            zcard(&pool, keys::RETRY).await == 1
        })
        .await;
        launcher.stop().await.unwrap();
        let (job, _) = sorted_jobs(&pool, keys::RETRY).await.remove(0);
        del(&pool, keys::RETRY).await;
        let lines = hench::decompress_backtrace(job.error_backtrace.as_deref().unwrap()).unwrap();
        // The job's three frames (`parse`, `step`, `perform`), at their lines, and nothing below
        // them. Frames are known by their locations: their names depend on the platform's
        // symbolization.
        assert_eq!(lines.len(), 3, "{cleaner}: {lines:?}");
        for (line, number) in lines.iter().zip(DEEP_LINES) {
            let site = format!("retry.rs:{number}:");
            assert!(line.contains(&site), "{cleaner}, {site}: {lines:?}");
        }
        for machinery in [
            "src/middleware.rs",
            "src/processor.rs",
            "src/job.rs",
            "src/error.rs",
            "hench::middleware",
            "hench::processor",
            "tokio",
            "futures_util",
        ] {
            assert!(
                lines.iter().all(|line| !line.contains(machinery)),
                "{cleaner}, {machinery}: {lines:?}"
            );
        }
        assert_eq!(
            lines.iter().all(|line| line.starts_with("app: ")),
            cleaner == "cleans",
            "{lines:?}"
        );
        let reports =
            errors.with_context("backtrace_cleaner panicked; storing the backtrace uncleaned");
        assert_eq!(
            reports.len(),
            usize::from(cleaner == "panics"),
            "{reports:?}"
        );
    }
}
