//! Over-limit refusals through real workers (`docs/design/ratelimit.md` §3.4.1–6): a job
//! refused by a limiter is rescheduled (same jid, as fetched, without using a retry), up to its
//! budget, then handed to the retry system; custom errors through `over_limit_when`; iterable
//! jobs refused on an item resume after it.

mod support;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use hench::iterable::{Enumerator, IterableJob, Iteration};
use hench::limit::{BackoffInput, OverLimit};
use hench::prelude::*;
use hench::test_util::TestRedis;
use hench::{
    ClientMiddleware, ClientNext, Hench, JobExt, Launcher, ServerMiddleware, ServerNext, keys,
    now_secs_f64,
};
use serde_json::{Value, json};
use support::*;

/// What the over-limit tests saw.
#[derive(Default)]
struct Seen {
    runs: Mutex<Vec<(String, Vec<Value>)>>,
}

impl Seen {
    fn record(&self, jid: &str, args: Vec<Value>) {
        self.runs.lock().unwrap().push((jid.to_owned(), args));
    }

    fn runs(&self) -> usize {
        self.runs.lock().unwrap().len()
    }
}

/// A custom error a client library might return: an HTTP 429 with `Retry-After`.
#[derive(Debug)]
struct TooManyRequests {
    retry_after: Duration,
}

impl std::fmt::Display for TooManyRequests {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HTTP 429 Too Many Requests")
    }
}

impl std::error::Error for TooManyRequests {}

/// Refuses as its argument says.
struct Refuser;

impl Job for Refuser {
    const NAME: &'static str = "limit::Refuser";
    type Args = (String,);

    async fn perform(&self, ctx: &JobContext, (mode,): Self::Args) -> Result<(), JobError> {
        let seen = ctx.data::<Seen>()?;
        seen.record(ctx.jid(), ctx.payload().args.clone());
        match mode.as_str() {
            "over" => Err(JobError::over_limit(
                OverLimit::new("w:api").with_retry_after(Duration::from_secs(30)),
            )),
            // Through `?` on a hench::Error, with context of the job's own.
            "over-error" => {
                let refused: Result<(), hench::Error> = Err(hench::Error::OverLimit(
                    OverLimit::new("w:api").with_retry_after(Duration::from_secs(30)),
                ));
                Ok(refused.map_err(|error| JobError::from(error).context("calling the API"))?)
            }
            "budget-2" => Err(JobError::over_limit(
                OverLimit::new("w:api")
                    .with_retry_after(Duration::from_secs(30))
                    .with_reschedules(2),
            )),
            "budget-0" => Err(JobError::over_limit(
                OverLimit::new("w:api").with_reschedules(0),
            )),
            "429" => Err(JobError::from(TooManyRequests {
                retry_after: Duration::from_secs(7),
            })),
            "classifier-panics" => Err(JobError::msg("the classifier panics on this")),
            "no-hint" => Err(JobError::over_limit(OverLimit::new("custom"))),
            _ => Ok(()),
        }
    }

    fn retry_in(&self, _count: u32, error: &JobError, job: &JobPayload) -> RetryIn {
        RETRY_IN
            .lock()
            .unwrap()
            .entry(job.jid.clone())
            .or_default()
            .push(error.is_over_limit());
        RetryIn::Default
    }
}

/// What `retry_in` saw (whether the error was an over-limit refusal), by jid: `retry_in` has no
/// context, and the tests of this binary run at the same time, each with jobs of its own.
static RETRY_IN: Mutex<BTreeMap<String, Vec<bool>>> = Mutex::new(BTreeMap::new());

/// What `retry_in` saw for the job `jid`.
fn retry_in_of(jid: &str) -> Vec<bool> {
    RETRY_IN
        .lock()
        .unwrap()
        .get(jid)
        .cloned()
        .unwrap_or_default()
}

/// Changes the job's arguments and adds a key before it runs (none of which may leak into the
/// rescheduled job).
struct Mutator;

#[async_trait]
impl ServerMiddleware for Mutator {
    async fn call(
        &self,
        ctx: &JobContext,
        job: &mut JobPayload,
        next: ServerNext<'_>,
    ) -> Result<(), JobError> {
        job.args = vec![json!("over")];
        job.extra.insert("mutated".to_owned(), json!(true));
        next.run(ctx, job).await
    }
}

/// Counts the pushes that go through client middleware.
#[derive(Default, Clone)]
struct Pushes(Arc<AtomicUsize>);

#[async_trait]
impl ClientMiddleware for Pushes {
    async fn call(
        &self,
        job: JobPayload,
        redis: &RedisPool,
        next: ClientNext<'_>,
    ) -> hench::Result<Option<JobPayload>> {
        let _ = redis;
        self.0.fetch_add(1, Ordering::SeqCst);
        next.run(job).await
    }
}

async fn start(
    redis: &TestRedis,
    seen: &Arc<Seen>,
    configure: impl FnOnce(hench::HenchBuilder) -> hench::HenchBuilder,
) -> (Hench, Launcher) {
    init_logging();
    let builder = redis
        .builder()
        .await
        .register(Refuser)
        .data_arc(seen.clone());
    let hench = configure(builder).build().await.unwrap();
    let launcher = hench.start().await.unwrap();
    (hench, launcher)
}

async fn push(client: &Client, mode: &str) -> String {
    Refuser::perform_async(client, (mode.to_owned(),))
        .await
        .unwrap()
        .unwrap()
}

/// The jobs in the schedule with their scores.
async fn scheduled(pool: &RedisPool) -> Vec<(JobPayload, f64)> {
    sorted_jobs(pool, keys::SCHEDULE).await
}

#[tokio::test]
async fn over_limit_jobs_are_rescheduled_not_failed() {
    let redis = TestRedis::start().await;
    let seen = Arc::new(Seen::default());
    let errors = Errors::default();
    let handler = errors.handler();
    let (hench, launcher) = start(&redis, &seen, |builder| builder.error_handler(handler)).await;
    let pool = hench.redis().clone();
    let jid = push(hench.client(), "over").await;
    let error_jid = push(hench.client(), "over-error").await;
    wait_for("both jobs in the schedule", WAIT, || async {
        zcard(&pool, keys::SCHEDULE).await == 2
    })
    .await;
    wait_for("the in-progress list empty", WAIT, || async {
        llen(&pool, &keys::inprogress(launcher.identity())).await == 0
    })
    .await;
    // Counted as processed, with the over-limit metric, once the heartbeat flushed.
    wait_for("the processed counter flushed", WAIT, || async {
        get_int(&pool, keys::STAT_PROCESSED).await == 2
    })
    .await;
    launcher.stop().await.unwrap();
    for (job, score) in scheduled(&pool).await {
        assert!([&jid, &error_jid].contains(&&job.jid), "{}", job.jid);
        let limited = job.limited.expect("limited");
        assert_eq!((limited.n, limited.by.as_str()), (1, "w:api"));
        // At the hint (30 s) after the refusal `limited.at` records, plus up to 10 % jitter.
        let delay = score - limited.at.expect("refused at") as f64 / 1000.0;
        assert!((29.99..=33.01).contains(&delay), "{delay}");
        assert_eq!(job.retry_count, None);
        assert_eq!(job.error_class, None);
        assert_eq!(job.at, None);
    }
    assert_eq!(get_int(&pool, keys::STAT_FAILED).await, 0);
    assert_eq!(zcard(&pool, keys::RETRY).await, 0);
    assert!(errors.all().is_empty(), "{:?}", errors.all());
    let metrics: std::collections::HashMap<String, String> =
        hgetall(&pool, &keys::metrics_minute(chrono::Utc::now())).await;
    let over: u64 = metrics
        .iter()
        .filter(|(field, _)| field.as_str() == "limit::Refuser|o")
        .map(|(_, value)| value.parse::<u64>().unwrap())
        .sum();
    let previous_minute: std::collections::HashMap<String, String> = hgetall(
        &pool,
        &keys::metrics_minute(chrono::Utc::now() - chrono::Duration::minutes(1)),
    )
    .await;
    let over = over
        + previous_minute
            .get("limit::Refuser|o")
            .map_or(0, |value| value.parse::<u64>().unwrap());
    assert_eq!(over, 2);
}

#[tokio::test]
async fn the_rescheduled_job_is_the_job_as_fetched() {
    let redis = TestRedis::start().await;
    let seen = Arc::new(Seen::default());
    let pushes = Pushes::default();
    let counter = pushes.clone();
    let (hench, launcher) = start(&redis, &seen, |builder| {
        builder
            .server_middleware(Mutator)
            .client_middleware(counter)
    })
    .await;
    let pool = hench.redis().clone();
    // Fetched with the argument "clean"; the middleware makes the job over its limit.
    let jid = push(hench.client(), "clean").await;
    assert_eq!(pushes.0.load(Ordering::SeqCst), 1);
    wait_for("the job in the schedule", WAIT, || async {
        zcard(&pool, keys::SCHEDULE).await == 1
    })
    .await;
    launcher.stop().await.unwrap();
    let (job, _) = scheduled(&pool).await.remove(0);
    assert_eq!(job.jid, jid);
    assert_eq!(job.args, [json!("clean")]);
    assert!(!job.extra.contains_key("mutated"));
    // The reschedule itself does not run client middleware (the scheduler will).
    assert_eq!(pushes.0.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn the_scheduler_brings_rescheduled_jobs_back_through_client_middleware() {
    let redis = TestRedis::start().await;
    let seen = Arc::new(Seen::default());
    let pushes = Pushes::default();
    let counter = pushes.clone();
    let (hench, launcher) =
        start(&redis, &seen, |builder| builder.client_middleware(counter)).await;
    let pool = hench.redis().clone();
    // A refusal without a hint: rescheduled in a minute; move it to now by hand.
    let jid = push(hench.client(), "no-hint").await;
    wait_for("the job in the schedule", WAIT, || async {
        zcard(&pool, keys::SCHEDULE).await == 1
    })
    .await;
    let (job, score) = scheduled(&pool).await.remove(0);
    // After the refusal `limited.at` records, plus up to 10 % jitter.
    let refused_at = job.limited.as_ref().and_then(|l| l.at).expect("refused at");
    let delay = score - refused_at as f64 / 1000.0;
    assert!((59.99..=66.01).contains(&delay), "60 s × 1: {delay}");
    let raw: Vec<Vec<u8>> = cmd(&pool, &redis_cmd!("ZRANGE", keys::SCHEDULE, 0, 0)).await;
    let () = cmd(
        &pool,
        &redis_cmd!("ZADD", keys::SCHEDULE, now_secs_f64() - 1.0, &raw[0]),
    )
    .await;
    // It runs again (refused again): `limited.n` counts on, the jid stays.
    wait_for("the second run", WAIT, || async { seen.runs() >= 2 }).await;
    wait_for("the job rescheduled again", WAIT, || async {
        scheduled(&pool)
            .await
            .first()
            .is_some_and(|(job, _)| job.limited.as_ref().is_some_and(|l| l.n == 2))
    })
    .await;
    launcher.stop().await.unwrap();
    assert_eq!(job.jid, jid);
    // One push by the test, one move by the scheduler.
    assert_eq!(pushes.0.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn spent_budgets_hand_the_job_to_retries() {
    let redis = TestRedis::start().await;
    let seen = Arc::new(Seen::default());
    let (hench, launcher) = start(&redis, &seen, |builder| builder).await;
    let pool = hench.redis().clone();
    // A job already rescheduled twice by limiters (its count of reschedules without progress
    // is `limited.o`), whose refusal allows two reschedules.
    let mut job = JobPayload::new("limit::Refuser", vec![json!("budget-2")]);
    job.queue = "default".to_owned();
    let mut limited = hench::limit::Limited::new(2, "w:api", Some(1));
    limited.over_limit = 2;
    job.limited = Some(limited);
    let spent = hench.client().push(job).await.unwrap().unwrap();
    // `reschedules(0)`: fails at the first refusal.
    let none = push(hench.client(), "budget-0").await;
    wait_for("both jobs in the retry set", WAIT, || async {
        zcard(&pool, keys::RETRY).await == 2
    })
    .await;
    launcher.stop().await.unwrap();
    for (job, _) in sorted_jobs(&pool, keys::RETRY).await {
        assert_eq!(job.error_class.as_deref(), Some(JobError::OVER_LIMIT));
        assert_eq!(job.retry_count, Some(0));
        let message = job.error_message.unwrap();
        assert!(message.contains("handing over to retries"), "{message}");
    }
    assert_eq!(zcard(&pool, keys::SCHEDULE).await, 0);
    assert_eq!(get_int(&pool, keys::STAT_FAILED).await, 2);
    // `retry_in` sees the refusal.
    assert_eq!(retry_in_of(&spent), [true]);
    assert_eq!(retry_in_of(&none), [true]);
}

#[tokio::test]
async fn custom_errors_are_refusals_through_over_limit_when() {
    let redis = TestRedis::start().await;
    let seen = Arc::new(Seen::default());
    let errors = Errors::default();
    let handler = errors.handler();
    let (hench, launcher) = start(&redis, &seen, |builder| {
        builder.error_handler(handler).over_limit_when(|error| {
            if error.message().contains("panics") {
                panic!("a broken classifier");
            }
            let too_many = error.downcast_ref::<TooManyRequests>()?;
            Some(OverLimit::new("http:api").with_retry_after(too_many.retry_after))
        })
    })
    .await;
    let pool = hench.redis().clone();
    push(hench.client(), "429").await;
    push(hench.client(), "classifier-panics").await;
    wait_for("one job rescheduled, one failed", WAIT, || async {
        zcard(&pool, keys::SCHEDULE).await == 1 && zcard(&pool, keys::RETRY).await == 1
    })
    .await;
    launcher.stop().await.unwrap();
    let (job, score) = scheduled(&pool).await.remove(0);
    let limited = job.limited.unwrap();
    // Measured from the refusal, which `limited.at` records, not from the time the test looks:
    // the wait above and the shutdown, slow on a busy machine, must not count. The delay is the
    // hint (7 s) plus a jitter of up to a tenth.
    let delay = score - limited.at.unwrap() as f64 / 1000.0;
    assert!((6.99..=7.71).contains(&delay), "{delay}");
    assert_eq!(limited.by, "http:api");
    // The classifier's panic is reported, and the job failed like any other.
    let reported: Vec<String> = errors
        .all()
        .into_iter()
        .map(|(context, ..)| context)
        .collect();
    assert!(
        reported.contains(&"over_limit_when panicked".to_owned()),
        "{reported:?}"
    );
    assert!(reported.contains(&"job failed".to_owned()), "{reported:?}");
}

#[tokio::test]
async fn a_panicking_backoff_falls_back_on_the_hint() {
    let redis = TestRedis::start().await;
    let seen = Arc::new(Seen::default());
    let errors = Errors::default();
    let handler = errors.handler();
    let (hench, launcher) = start(&redis, &seen, |builder| {
        builder
            .error_handler(handler)
            .over_limit_backoff(|input: &BackoffInput<'_>| {
                assert_eq!(input.reschedules, 1);
                panic!("no delay")
            })
    })
    .await;
    let pool = hench.redis().clone();
    push(hench.client(), "over").await;
    wait_for("the job rescheduled", WAIT, || async {
        zcard(&pool, keys::SCHEDULE).await == 1
    })
    .await;
    launcher.stop().await.unwrap();
    let (job, score) = scheduled(&pool).await.remove(0);
    // The hint (30 s) after the refusal `limited.at` records, plus up to 10 % jitter.
    let refused_at = job.limited.and_then(|l| l.at).expect("refused at");
    let delay = score - refused_at as f64 / 1000.0;
    assert!((29.99..=33.01).contains(&delay), "{delay}");
    let reported: Vec<String> = errors
        .all()
        .into_iter()
        .map(|(context, ..)| context)
        .collect();
    assert_eq!(reported, ["over-limit backoff panicked; using the default"]);
}

/// An iterable job over 0..10 whose limiter refuses item 5 in its first execution.
#[derive(Default)]
struct Paced {
    log: Arc<Mutex<Vec<String>>>,
    refused: Arc<AtomicUsize>,
}

impl IterableJob for Paced {
    const NAME: &'static str = "limit::Paced";
    type Args = ();
    type Item = i64;
    type Cursor = usize;

    fn build_enumerator<'a>(
        &'a self,
        _ctx: &'a JobContext,
        _args: &'a (),
        cursor: Option<usize>,
    ) -> Result<Enumerator<'a, i64, usize>, JobError> {
        Ok(Enumerator::iter(0..10_i64, cursor))
    }

    async fn each_iteration(&self, item: i64, it: &Iteration<'_, Self>) -> Result<(), JobError> {
        if item == 5 && self.refused.fetch_add(1, Ordering::SeqCst) == 0 {
            self.log.lock().unwrap().push(format!("refused {item}"));
            return Err(JobError::over_limit(
                OverLimit::new("w:items").with_retry_after(Duration::from_millis(100)),
            ));
        }
        self.log
            .lock()
            .unwrap()
            .push(format!("item {item} (execution {})", it.executions()));
        Ok(())
    }

    async fn on_resume(&self, it: &Iteration<'_, Self>) -> Result<(), JobError> {
        self.log
            .lock()
            .unwrap()
            .push(format!("resume at {:?}", it.cursor()));
        Ok(())
    }

    async fn on_stop(&self, _it: &Iteration<'_, Self>) -> Result<(), JobError> {
        self.log.lock().unwrap().push("stop".to_owned());
        Ok(())
    }
}

#[tokio::test]
async fn iterable_jobs_refused_on_an_item_resume_after_the_last_one_done() {
    let redis = TestRedis::start().await;
    init_logging();
    let job = Paced::default();
    let log = job.log.clone();
    let hench = redis.builder().await.register(job).build().await.unwrap();
    let launcher = hench.start().await.unwrap();
    let pool = hench.redis().clone();
    let jid = <Paced as JobExt>::perform_async(hench.client(), ())
        .await
        .unwrap()
        .unwrap();
    // Rescheduled (at least a second), brought back by the scheduler, then finished.
    wait_for(
        "the iteration finished",
        Duration::from_secs(15),
        || async {
            log.lock()
                .unwrap()
                .iter()
                .any(|entry| entry == "item 9 (execution 2)")
        },
    )
    .await;
    wait_for("the state deleted", WAIT, || async {
        !exists(&pool, &keys::iteration_state(&jid)).await
    })
    .await;
    launcher.stop().await.unwrap();
    let log = log.lock().unwrap().clone();
    let expected: Vec<String> = (0..5)
        .map(|item| format!("item {item} (execution 1)"))
        .chain([
            "refused 5".to_owned(),
            "stop".to_owned(),
            "resume at Some(5)".to_owned(),
        ])
        .chain((5..10).map(|item| format!("item {item} (execution 2)")))
        .chain(["stop".to_owned()])
        .collect();
    assert_eq!(log, expected);
    assert_eq!(get_int(&pool, keys::STAT_FAILED).await, 0);
    assert_eq!(zcard(&pool, keys::RETRY).await, 0);
}

/// ratelimit-2: waiting for a throttle never spends a limiter's budget. A job parked 25 times by
/// its throttle (more than the default budget of 20) is refused by a limiter while it runs: it
/// is rescheduled (its first limiter reschedule), not failed.
#[tokio::test]
async fn throttle_parkings_do_not_spend_the_limiters_budget() {
    let redis = TestRedis::start().await;
    let seen = Arc::new(Seen::default());
    let (hench, launcher) = start(&redis, &seen, |builder| builder).await;
    let pool = hench.redis().clone();
    let mut job = JobPayload::new("limit::Refuser", vec![json!("over")]);
    job.queue = "default".to_owned();
    let mut limited = hench::limit::Limited::new(25, "c:erp", Some(1));
    limited.throttled = 25;
    job.limited = Some(limited);
    let jid = hench.client().push(job).await.unwrap().unwrap();
    wait_for("the job rescheduled", WAIT, || async {
        zcard(&pool, keys::SCHEDULE).await == 1
    })
    .await;
    launcher.stop().await.unwrap();
    let (rescheduled, _) = scheduled(&pool).await.remove(0);
    assert_eq!(rescheduled.jid, jid);
    let limited = rescheduled.limited.unwrap();
    assert_eq!(
        (limited.n, limited.throttled, limited.over_limit),
        (26, 25, 1)
    );
    assert_eq!(limited.by, "w:api");
    assert_eq!(zcard(&pool, keys::RETRY).await, 0);
    assert_eq!(get_int(&pool, keys::STAT_FAILED).await, 0);
}

/// An iterable job over 0..25 that takes a unit of a 5-per-second window per item, never
/// waiting in process: each execution processes about five items, then is rescheduled.
#[derive(Default)]
struct Metered {
    done: Arc<AtomicUsize>,
}

impl IterableJob for Metered {
    const NAME: &'static str = "limit::Metered";
    type Args = ();
    type Item = i64;
    type Cursor = usize;

    fn build_enumerator<'a>(
        &'a self,
        _ctx: &'a JobContext,
        _args: &'a (),
        cursor: Option<usize>,
    ) -> Result<Enumerator<'a, i64, usize>, JobError> {
        Ok(Enumerator::iter(0..25_i64, cursor))
    }

    async fn each_iteration(&self, _item: i64, it: &Iteration<'_, Self>) -> Result<(), JobError> {
        static ITEMS: std::sync::LazyLock<hench::limit::Limiter> = std::sync::LazyLock::new(|| {
            hench::limit::Limiter::window("metered-items", 5, Duration::from_secs(1))
                .max_wait(Duration::ZERO)
        });
        let _permit = ITEMS.acquire(it.ctx()).await?;
        self.done.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// ratelimit-3: an iterable job paced by a per-item limiter is rescheduled once per window of
/// items. Each of those executions made progress, so none of them spends the limiter's budget
/// (here 3 reschedules): the job runs to its end, without a failure.
#[tokio::test]
async fn iterable_jobs_paced_by_a_limiter_are_never_failed_for_it() {
    let redis = TestRedis::start().await;
    init_logging();
    let job = Metered::default();
    let done = job.done.clone();
    let hench = redis
        .builder()
        .await
        .over_limit_reschedules(3)
        .register(job)
        .build()
        .await
        .unwrap();
    let launcher = hench.start().await.unwrap();
    let pool = hench.redis().clone();
    let jid = <Metered as JobExt>::perform_async(hench.client(), ())
        .await
        .unwrap()
        .unwrap();
    wait_for("every item done", Duration::from_secs(30), || async {
        done.load(Ordering::SeqCst) == 25
    })
    .await;
    wait_for("the state deleted", WAIT, || async {
        !exists(&pool, &keys::iteration_state(&jid)).await
    })
    .await;
    launcher.stop().await.unwrap();
    assert_eq!(done.load(Ordering::SeqCst), 25);
    assert_eq!(zcard(&pool, keys::RETRY).await, 0);
    assert_eq!(get_int(&pool, keys::STAT_FAILED).await, 0);
    // Rescheduled more often than its budget allows in a row, each time after progress.
    let stats = hgetall(&pool, &keys::limit_stats('w', "metered-items")).await;
    let refused: u64 = stats["r"].parse().unwrap();
    assert!(refused > 3, "{stats:?}");
}
