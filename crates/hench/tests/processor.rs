//! Processing jobs (Sidekiq's `processor_test.rb`, `exception_handler_test.rb` and
//! `job_logger_test.rb` scenarios): execution, acknowledgement, invalid jobs, panics, server
//! middleware and error handlers.

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use hench::prelude::*;
use hench::test_util::TestRedis;
use hench::{FetchStrategy, Hench, Launcher, keys};
use serde_json::{Value, json};
use support::*;

async fn running(builder: hench::HenchBuilder) -> (Hench, Launcher) {
    init_logging();
    let hench = builder.build().await.unwrap();
    let launcher = hench.start().await.unwrap();
    (hench, launcher)
}

/// Waits until a job is in the retry set and returns it.
async fn retried(pool: &RedisPool) -> JobPayload {
    wait_for("a job in the retry set", WAIT, || async {
        zcard(pool, keys::RETRY).await == 1
    })
    .await;
    sorted_jobs(pool, keys::RETRY).await.remove(0).0
}

#[tokio::test]
async fn processes_jobs() {
    for strategy in [FetchStrategy::Reliable, FetchStrategy::Basic] {
        let redis = TestRedis::start().await;
        let runs = Arc::new(Runs::default());
        let (hench, launcher) = running(
            redis
                .builder()
                .await
                .fetch(strategy)
                .register(Echo)
                .data_arc(runs.clone()),
        )
        .await;
        let pool = hench.redis().clone();
        Echo::perform_async(hench.client(), ("myarg".into(),))
            .await
            .unwrap();
        runs.wait_for(1).await;
        assert_eq!(runs.entries(), ["myarg"]);
        launcher.stop().await.unwrap();
        assert_eq!(queue_size(&pool, "default").await, 0);
        assert_eq!(llen(&pool, &keys::inprogress(launcher.identity())).await, 0);
        assert_eq!(get_int(&pool, keys::STAT_PROCESSED).await, 1);
        assert_eq!(get_int(&pool, keys::STAT_FAILED).await, 0);
    }
}

#[tokio::test]
async fn failures_are_retried_and_reported() {
    let redis = TestRedis::start().await;
    let errors = Errors::default();
    let (hench, launcher) = running(
        redis
            .builder()
            .await
            .register(Fail)
            .error_handler(errors.handler()),
    )
    .await;
    let pool = hench.redis().clone();
    let jid = Fail::perform_async(hench.client(), vec![json!("boom")])
        .await
        .unwrap()
        .unwrap();
    let job = retried(&pool).await;
    assert_eq!(job.jid, jid);
    assert_eq!(job.error_message.as_deref(), Some("kerblammo!"));
    assert_eq!(job.error_class.as_deref(), Some("RuntimeError"));
    assert_eq!(job.retry_count, Some(0));
    wait_for("the error handler", WAIT, || async {
        !errors.with_context("job failed").is_empty()
    })
    .await;
    let reported = errors.with_context("job failed");
    assert_eq!(reported.len(), 1);
    assert_eq!(reported[0].1, "RuntimeError");
    assert_eq!(reported[0].2.as_deref(), Some(jid.as_str()));
    launcher.stop().await.unwrap();
    // The failed job was acknowledged.
    assert_eq!(llen(&pool, &keys::inprogress(launcher.identity())).await, 0);
    assert_eq!(queue_size(&pool, "default").await, 0);
    assert_eq!(get_int(&pool, keys::STAT_PROCESSED).await, 1);
    assert_eq!(get_int(&pool, keys::STAT_FAILED).await, 1);
}

/// Panics on `"boom"`, records anything else.
#[derive(Default)]
struct Fragile;

impl Job for Fragile {
    const NAME: &'static str = "Fragile";
    type Args = (String,);

    async fn perform(&self, ctx: &JobContext, (value,): (String,)) -> Result<(), JobError> {
        assert!(value != "boom", "the job panicked on {value}");
        ctx.data::<Runs>()?.record(value);
        Ok(())
    }
}

#[tokio::test]
async fn panics_are_caught_and_the_processor_keeps_going() {
    let redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    let (hench, launcher) = running(
        redis
            .builder()
            .await
            .concurrency(1)
            .register(Fragile)
            .data_arc(runs.clone()),
    )
    .await;
    let pool = hench.redis().clone();
    Fragile::perform_async(hench.client(), ("boom".into(),))
        .await
        .unwrap();
    let job = retried(&pool).await;
    assert_eq!(job.error_class.as_deref(), Some("panic"));
    assert!(
        job.error_message
            .as_deref()
            .unwrap()
            .contains("the job panicked on boom"),
        "{job:?}"
    );
    // The single processor survived.
    Fragile::perform_async(hench.client(), ("fine".into(),))
        .await
        .unwrap();
    runs.wait_for(1).await;
    launcher.stop().await.unwrap();
}

#[tokio::test]
async fn invalid_json_goes_to_the_morgue() {
    for strategy in [FetchStrategy::Reliable, FetchStrategy::Basic] {
        let redis = TestRedis::start().await;
        let errors = Errors::default();
        let (hench, launcher) = running(
            redis
                .builder()
                .await
                .fetch(strategy)
                .error_handler(errors.handler()),
        )
        .await;
        let pool = hench.redis().clone();
        let broken = br#"{"class":"Echo","args":["boom"#;
        push_raw(&pool, "default", broken).await;
        wait_for("the morgue", WAIT, || async {
            zcard(&pool, keys::DEAD).await == 1
        })
        .await;
        assert_eq!(zrange(&pool, keys::DEAD).await[0].0, broken.to_vec());
        wait_for("the error report", WAIT, || async {
            errors.with_context("job is not valid JSON").len() == 1
        })
        .await;
        launcher.stop().await.unwrap();
        assert_eq!(queue_size(&pool, "default").await, 0);
        assert_eq!(llen(&pool, &keys::inprogress(launcher.identity())).await, 0);
        // Like Sidekiq, unreadable jobs are not counted as processed.
        assert_eq!(get_int(&pool, keys::STAT_PROCESSED).await, 0);
    }
}

#[tokio::test]
async fn unknown_classes_take_the_retry_path() {
    let redis = TestRedis::start().await;
    let (hench, launcher) = running(redis.builder().await).await;
    let pool = hench.redis().clone();
    hench
        .client()
        .push(JobPayload::new("NoSuchJob", vec![json!(1)]))
        .await
        .unwrap();
    let job = retried(&pool).await;
    assert_eq!(job.error_class.as_deref(), Some(JobError::UNKNOWN_JOB));
    assert!(job.error_message.unwrap().contains("NoSuchJob"));
    launcher.stop().await.unwrap();
}

/// The error of an unknown class names the jobs the process knows and the usual cause (a
/// worker binary that does not link the crate defining the job), so that a worker started with
/// an empty registry is easy to diagnose from any failed job.
#[tokio::test]
async fn unknown_class_errors_name_the_known_jobs_and_the_likely_cause() {
    let redis = TestRedis::start().await;
    for (builder, known) in [
        (
            redis.builder().await.auto_register(false).register(Echo),
            "which knows `Echo`",
        ),
        (
            redis.builder().await.auto_register(false),
            "which knows no job",
        ),
    ] {
        let (hench, launcher) = running(builder).await;
        let pool = hench.redis().clone();
        hench
            .client()
            .push(JobPayload::new("NoSuchJob", vec![json!(1)]))
            .await
            .unwrap();
        let job = retried(&pool).await;
        let message = job.error_message.unwrap();
        assert!(
            message.contains("no job is registered under the name `NoSuchJob`")
                && message.contains(known)
                && message.contains("use my_jobs as _;"),
            "{message}"
        );
        launcher.stop().await.unwrap();
        del(&pool, keys::RETRY).await;
    }
}

#[tokio::test]
async fn invalid_arguments_take_the_retry_path() {
    let redis = TestRedis::start().await;
    let (hench, launcher) = running(redis.builder().await.register(Echo)).await;
    let pool = hench.redis().clone();
    hench
        .client()
        .push(JobPayload::new("Echo", vec![json!(1), json!(2)]))
        .await
        .unwrap();
    let job = retried(&pool).await;
    assert_eq!(job.error_class.as_deref(), Some(JobError::INVALID_ARGS));
    launcher.stop().await.unwrap();
}

/// Where a test middleware fails or stops the chain.
#[derive(Clone, Copy, PartialEq)]
enum Mode {
    FailBefore,
    FailAfter,
    Skip,
    MutateArgs,
}

struct Middleware(Mode);

#[async_trait]
impl ServerMiddleware for Middleware {
    async fn call(
        &self,
        ctx: &JobContext,
        job: &mut JobPayload,
        next: ServerNext<'_>,
    ) -> Result<(), JobError> {
        match self.0 {
            Mode::FailBefore => Err(JobError::new("MiddlewareError", "before")),
            Mode::FailAfter => {
                next.run(ctx, job).await?;
                Err(JobError::new("MiddlewareError", "after"))
            }
            Mode::Skip => Ok(()),
            Mode::MutateArgs => {
                job.args = vec![json!("mutated")];
                job.extra.insert("seen_by".into(), json!("middleware"));
                next.run(ctx, job).await
            }
        }
    }
}

/// Records its argument and the `seen_by` key of its payload, then fails unless the argument
/// is `"x"`.
#[derive(Default)]
struct Inspect;

impl Job for Inspect {
    const NAME: &'static str = "Inspect";
    type Args = (String,);

    async fn perform(&self, ctx: &JobContext, (value,): (String,)) -> Result<(), JobError> {
        let seen_by = ctx.payload().extra.get("seen_by").cloned();
        ctx.data::<Runs>()?
            .record(format!("{value}/{}", seen_by.unwrap_or(Value::Null)));
        if value != "x" {
            return Err(JobError::msg("inspect failed"));
        }
        Ok(())
    }
}

async fn with_middleware(mode: Mode, arg: &str) -> (Arc<Runs>, Vec<JobPayload>, usize) {
    let redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    let (hench, launcher) = running(
        redis
            .builder()
            .await
            .register(Inspect)
            .data_arc(runs.clone())
            .server_middleware(Middleware(mode)),
    )
    .await;
    let pool = hench.redis().clone();
    Inspect::perform_async(hench.client(), (arg.to_owned(),))
        .await
        .unwrap();
    wait_for("the job to be handled", WAIT, || async {
        queue_size(&pool, "default").await == 0
            && llen(&pool, &keys::inprogress(launcher.identity())).await == 0
    })
    .await;
    launcher.stop().await.unwrap();
    assert_eq!(llen(&pool, &keys::inprogress(launcher.identity())).await, 0);
    let retries = sorted_jobs(&pool, keys::RETRY)
        .await
        .into_iter()
        .map(|(job, _)| job)
        .collect();
    let processed = get_int(&pool, keys::STAT_PROCESSED).await;
    (runs, retries, usize::try_from(processed).unwrap())
}

#[tokio::test]
async fn middleware_failing_before_the_job_acks_and_retries() {
    let (runs, retries, processed) = with_middleware(Mode::FailBefore, "x").await;
    assert_eq!(runs.len(), 0);
    assert_eq!(retries.len(), 1);
    assert_eq!(retries[0].error_message.as_deref(), Some("before"));
    assert_eq!(processed, 1);
}

#[tokio::test]
async fn middleware_failing_after_the_job_acks_and_retries() {
    let (runs, retries, _) = with_middleware(Mode::FailAfter, "x").await;
    assert_eq!(runs.entries(), ["x/null"]);
    assert_eq!(retries.len(), 1);
    assert_eq!(retries[0].error_message.as_deref(), Some("after"));
}

#[tokio::test]
async fn middleware_skipping_the_job_acks_it() {
    let (runs, retries, processed) = with_middleware(Mode::Skip, "x").await;
    assert_eq!(runs.len(), 0);
    assert!(retries.is_empty());
    assert_eq!(processed, 1);
}

#[tokio::test]
async fn retries_use_the_original_arguments() {
    let (runs, retries, _) = with_middleware(Mode::MutateArgs, "boom").await;
    // The job saw the middleware's changes...
    assert_eq!(runs.entries(), ["mutated/\"middleware\""]);
    // ...but it is retried as it was fetched.
    assert_eq!(retries.len(), 1);
    assert_eq!(retries[0].args, [json!("boom")]);
    assert!(!retries[0].extra.contains_key("seen_by"));
}

/// Checks what the context exposes.
#[derive(Default)]
struct Introspect;

struct Secret(&'static str);

impl Job for Introspect {
    const NAME: &'static str = "Introspect";
    type Args = ();

    fn options() -> JobOptions {
        JobOptions::new().queue("special").tags(["a", "b"])
    }

    async fn perform(&self, ctx: &JobContext, (): ()) -> Result<(), JobError> {
        let entry = format!(
            "{}|{}|{:?}|{}|{}|{}",
            ctx.queue(),
            ctx.class(),
            ctx.retry_count(),
            ctx.tags().join(","),
            ctx.data::<Secret>()?.0,
            ctx.is_stopping()
        );
        ctx.data::<Runs>()?.record(entry);
        ctx.client()
            .push(JobPayload::new("Followup", vec![]))
            .await?;
        Ok(())
    }
}

#[tokio::test]
async fn jobs_see_their_context() {
    let redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    let (hench, launcher) = running(
        redis
            .builder()
            .await
            .queues(["special"])
            .register(Introspect)
            .data(Secret("s3cret"))
            .data_arc(runs.clone()),
    )
    .await;
    Introspect::perform_async(hench.client(), ()).await.unwrap();
    runs.wait_for(1).await;
    assert_eq!(runs.entries(), ["special|Introspect|None|a,b|s3cret|false"]);
    launcher.stop().await.unwrap();
    // The job enqueued a follow-up through its context's client.
    assert_eq!(
        queue_jobs(hench.redis(), "default").await[0].class,
        "Followup"
    );
}

#[tokio::test]
async fn panicking_error_handlers_do_not_stop_processing() {
    let redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let (hench, launcher) = running(
        redis
            .builder()
            .await
            .concurrency(1)
            .register(Fail)
            .register(Echo)
            .data_arc(runs.clone())
            .error_handler(|_, _| panic!("error handler bug"))
            .error_handler(move |_, _| {
                counter.fetch_add(1, Ordering::SeqCst);
            }),
    )
    .await;
    Fail::perform_async(hench.client(), vec![]).await.unwrap();
    Echo::perform_async(hench.client(), ("after".into(),))
        .await
        .unwrap();
    runs.wait_for(1).await;
    wait_for("the second handler", WAIT, || async {
        calls.load(Ordering::SeqCst) == 1
    })
    .await;
    launcher.stop().await.unwrap();
}

#[tokio::test]
async fn disabled_retries_call_the_death_handlers() {
    let redis = TestRedis::start().await;
    let deaths = Deaths::default();
    let errors = Errors::default();
    let (hench, launcher) = running(
        redis
            .builder()
            .await
            .register(Fail)
            .death_handler(deaths.handler())
            .error_handler(errors.handler()),
    )
    .await;
    let pool = hench.redis().clone();
    let jid = Fail::set()
        .retry(false)
        .perform_async(hench.client(), vec![])
        .await
        .unwrap()
        .unwrap();
    wait_for("the death handler", WAIT, || async {
        deaths.all().len() == 1
    })
    .await;
    let (job, message) = deaths.all().remove(0);
    assert_eq!(job.jid, jid);
    assert_eq!(message, "kerblammo!");
    // The job is reported as it was pushed: no retry fields.
    assert_eq!(job.error_message, None);
    assert_eq!(job.retry_count, None);
    wait_for("the error handler", WAIT, || async {
        errors.with_context("job failed").len() == 1
    })
    .await;
    launcher.stop().await.unwrap();
    assert_eq!(zcard(&pool, keys::RETRY).await, 0);
    assert_eq!(zcard(&pool, keys::DEAD).await, 0);
}

#[tokio::test]
async fn jobs_are_logged_with_their_context() {
    // The job logger itself is unit tested; here: disabling it does not affect processing.
    let redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    let (hench, launcher) = running(
        redis
            .builder()
            .await
            .job_logging(false)
            .register(Echo)
            .data_arc(runs.clone()),
    )
    .await;
    Echo::perform_async(hench.client(), ("quiet".into(),))
        .await
        .unwrap();
    runs.wait_for(1).await;
    launcher.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_shutdown_signal_reaches_jobs_through_their_context() {
    #[derive(Default)]
    struct Loop;
    impl Job for Loop {
        const NAME: &'static str = "Loop";
        type Args = ();
        async fn perform(&self, ctx: &JobContext, (): ()) -> Result<(), JobError> {
            let runs = ctx.data::<Runs>()?;
            runs.record("started");
            while !ctx.is_stopping() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            runs.record("stopped");
            Ok(())
        }
    }
    let redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    let (hench, launcher) =
        running(redis.builder().await.register(Loop).data_arc(runs.clone())).await;
    Loop::perform_async(hench.client(), ()).await.unwrap();
    runs.wait_for(1).await;
    launcher.stop().await.unwrap();
    assert_eq!(runs.entries(), ["started", "stopped"]);
}

/// Forwards a shutdown it observed elsewhere (say a dependency's cancellation) while its own
/// process keeps running.
#[derive(Default)]
struct ForwardsShutdown;

impl Job for ForwardsShutdown {
    const NAME: &'static str = "ForwardsShutdown";
    type Args = ();

    async fn perform(&self, ctx: &JobContext, (): ()) -> Result<(), JobError> {
        ctx.data::<Runs>()?.record("ran");
        Err(JobError::new(JobError::SHUTDOWN, "a dependency shut down"))
    }
}

/// A job failing with class `hench::Shutdown` while its process is *not* stopping is an
/// ordinary failure (retry set, `job failed` report), not a requeue to the front of its queue
/// (which only a real shutdown does: Sidekiq's "shuts down without retrying work-in-progress").
#[tokio::test]
async fn a_shutdown_class_failure_outside_a_shutdown_is_retried_and_reported() {
    let redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    let errors = Errors::default();
    let (hench, launcher) = running(
        redis
            .builder()
            .await
            .register(ForwardsShutdown)
            .data_arc(runs.clone())
            .error_handler(errors.handler()),
    )
    .await;
    let pool = hench.redis().clone();
    let jid = ForwardsShutdown::perform_async(hench.client(), ())
        .await
        .unwrap()
        .unwrap();
    wait_for("the job in the retry set", WAIT, || async {
        zcard(&pool, keys::RETRY).await == 1
    })
    .await;
    wait_for("the failure report", WAIT, || async {
        errors.with_context("job failed").len() == 1
    })
    .await;
    let (job, _) = sorted_jobs(&pool, keys::RETRY).await.remove(0);
    assert_eq!(job.jid, jid);
    assert_eq!(job.retry_count, Some(0));
    assert_eq!(job.error_class.as_deref(), Some(JobError::SHUTDOWN));
    // It ran once and was not pushed back to its queue.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(runs.len(), 1, "{:?}", runs.entries());
    assert_eq!(queue_size(&pool, "default").await, 0);
    let reported = errors.with_context("job failed");
    assert_eq!(reported.len(), 1);
    assert_eq!(reported[0].1, JobError::SHUTDOWN);
    assert_eq!(reported[0].2.as_deref(), Some(jid.as_str()));
    launcher.stop().await.unwrap();
    assert_eq!(get_int(&pool, keys::STAT_FAILED).await, 1);
}

/// A fetch failing inside running processors reaches the error handlers (context `could not
/// fetch jobs`), once per episode however many processors keep failing, and processing resumes
/// once the cause is gone (Sidekiq's "handles exceptions raised during fetch").
#[tokio::test]
async fn fetch_failures_reach_the_error_handlers_once_per_episode() {
    for strategy in [FetchStrategy::Reliable, FetchStrategy::Basic] {
        let redis = TestRedis::start().await;
        let pool = redis.pool().await;
        // A queue key of the wrong type makes every fetch of the capsule fail (WRONGTYPE).
        let () = cmd(&pool, &redis_cmd!("SET", "queue:default", "oops")).await;
        let runs = Arc::new(Runs::default());
        let errors = Errors::default();
        let (hench, launcher) = running(
            redis
                .builder()
                .await
                .fetch(strategy)
                .register(Echo)
                .data_arc(runs.clone())
                .error_handler(errors.handler()),
        )
        .await;
        wait_for("a `could not fetch jobs` report", WAIT, || async {
            !errors.with_context("could not fetch jobs").is_empty()
        })
        .await;
        // Five processors keep failing every second: still a single report.
        tokio::time::sleep(Duration::from_millis(2_500)).await;
        assert_eq!(
            errors.with_context("could not fetch jobs").len(),
            1,
            "{strategy:?}: {:?}",
            errors.all()
        );
        // The end of the episode: jobs run again.
        del(&pool, "queue:default").await;
        Echo::perform_async(hench.client(), ("after".into(),))
            .await
            .unwrap();
        runs.wait_for(1).await;
        assert_eq!(runs.entries(), ["after"]);
        launcher.stop().await.unwrap();
        assert_eq!(errors.with_context("could not fetch jobs").len(), 1);
    }
}
