//! Testing modes: fake clients capture jobs, inline clients run them on push (scenarios of
//! Sidekiq's testing_test, testing_fake_test and testing_inline_test, plus hench specifics).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hench::prelude::*;
use hench::test_util::TestRedis;
use hench::testing::Testing;
use hench::{BulkPush, Error, Registry, now_millis, now_secs_f64};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// Counts executions and records the arguments it saw.
#[derive(Default)]
struct Recorded {
    runs: AtomicUsize,
    seen: Mutex<Vec<Vec<Value>>>,
}

impl Recorded {
    fn runs(&self) -> usize {
        self.runs.load(Ordering::SeqCst)
    }
}

/// `perform(a, b)`: records the call.
struct DirectJob(Arc<Recorded>);

impl Job for DirectJob {
    const NAME: &'static str = "DirectJob";
    type Args = (i64, i64);

    async fn perform(&self, ctx: &JobContext, (a, b): (i64, i64)) -> Result<(), JobError> {
        assert!(!ctx.jid().is_empty());
        self.0.runs.fetch_add(1, Ordering::SeqCst);
        self.0.seen.lock().unwrap().push(vec![json!(a), json!(b)]);
        Ok(())
    }
}

/// An application error.
#[derive(Debug)]
struct PerformError;

impl std::fmt::Display for PerformError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("perform failed")
    }
}

impl std::error::Error for PerformError {}

/// `perform(error)`: fails when asked to.
#[derive(Default)]
struct StoredJob(Arc<Recorded>);

impl Job for StoredJob {
    const NAME: &'static str = "StoredJob";
    type Args = (bool,);

    async fn perform(&self, _ctx: &JobContext, (error,): (bool,)) -> Result<(), JobError> {
        self.0.runs.fetch_add(1, Ordering::SeqCst);
        if error {
            Err(PerformError)?;
        }
        Ok(())
    }
}

/// A job routed to the `alt` queue by its options.
#[derive(Default)]
struct AltQueueJob;

impl Job for AltQueueJob {
    const NAME: &'static str = "AltQueueJob";
    type Args = (i64, i64);

    fn options() -> JobOptions {
        JobOptions::new().queue("alt")
    }

    async fn perform(&self, _ctx: &JobContext, _: (i64, i64)) -> Result<(), JobError> {
        Ok(())
    }
}

/// Runs of `FirstJob` and `SecondJob`, shared through the context's data.
#[derive(Default)]
struct Counters {
    first: AtomicUsize,
    second: AtomicUsize,
}

/// Increments `Counters::first`.
#[derive(Default)]
struct FirstJob;

impl Job for FirstJob {
    const NAME: &'static str = "FirstJob";
    type Args = ();

    async fn perform(&self, ctx: &JobContext, (): ()) -> Result<(), JobError> {
        ctx.data::<Counters>()?.first.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// Increments `Counters::second`.
#[derive(Default)]
struct SecondJob;

impl Job for SecondJob {
    const NAME: &'static str = "SecondJob";
    type Args = ();

    async fn perform(&self, ctx: &JobContext, (): ()) -> Result<(), JobError> {
        ctx.data::<Counters>()?
            .second
            .fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// Enqueues a `FirstJob` and a `SecondJob`.
#[derive(Default)]
struct ThirdJob;

impl Job for ThirdJob {
    const NAME: &'static str = "ThirdJob";
    type Args = ();

    async fn perform(&self, ctx: &JobContext, (): ()) -> Result<(), JobError> {
        FirstJob::perform_async(ctx.client(), ()).await?;
        SecondJob::perform_async(ctx.client(), ()).await?;
        Ok(())
    }
}

/// Succeeds only when its argument is its own jid.
struct SpecificJidJob(Arc<Recorded>);

impl Job for SpecificJidJob {
    const NAME: &'static str = "SpecificJidJob";
    type Args = (String,);

    async fn perform(&self, ctx: &JobContext, (jid,): (String,)) -> Result<(), JobError> {
        if jid == ctx.jid() {
            self.0.runs.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }
}

fn testing() -> Testing {
    Testing::new()
        .register_default::<AltQueueJob>()
        .register_default::<FirstJob>()
        .register_default::<SecondJob>()
        .register_default::<ThirdJob>()
        .data(Counters::default())
}

/// Runs of `FirstJob` and `SecondJob`.
fn counters(counters: &Counters) -> (usize, usize) {
    (
        counters.first.load(Ordering::SeqCst),
        counters.second.load(Ordering::SeqCst),
    )
}

#[tokio::test]
async fn fake_stubs_the_async_calls() {
    let recorded = Arc::new(Recorded::default());
    let client = Testing::new().register(DirectJob(recorded.clone())).fake();
    assert!(client.jobs::<DirectJob>().is_empty());

    let jid = DirectJob::perform_async(&client, (1, 2)).await.unwrap();
    assert!(jid.is_some());
    let jobs = client.jobs::<DirectJob>();
    assert_eq!(jobs.len(), 1);
    let enqueued_at = jobs[0].enqueued_at.expect("immediate jobs get enqueued_at");
    assert!((now_millis() - enqueued_at).abs() < 1_000);
    assert!(jobs[0].at.is_none());
    assert_eq!(jobs[0].jid, jid.unwrap());
    assert_eq!(jobs[0].queue, "default");

    DirectJob::perform_in(&client, Duration::from_secs(10), (1, 2))
        .await
        .unwrap();
    let jobs = client.jobs::<DirectJob>();
    assert_eq!(jobs.len(), 2);
    assert!(jobs[1].enqueued_at.is_none());

    let soon = SystemTime::now() + Duration::from_secs(10);
    DirectJob::perform_at(&client, soon, (1, 2)).await.unwrap();
    let jobs = client.jobs::<DirectJob>();
    assert_eq!(jobs.len(), 3);
    let at = jobs[2].at.expect("scheduled jobs keep their time");
    let soon = soon.duration_since(UNIX_EPOCH).unwrap().as_secs_f64();
    assert!((at - soon).abs() < 0.1, "{at} vs {soon}");
    // Nothing ran.
    assert_eq!(recorded.runs(), 0);
}

#[tokio::test]
async fn fake_stubs_enqueue_and_raw_pushes() {
    let client = Client::fake();
    client.enqueue::<AltQueueJob>((1, 2)).await.unwrap();
    assert_eq!(client.jobs::<AltQueueJob>().len(), 1);

    // Pushing to another queue (Sidekiq's enqueue_to).
    client
        .job::<AltQueueJob>()
        .queue("someq")
        .perform_async((1, 2))
        .await
        .unwrap();
    assert_eq!(client.jobs_in_queue("someq").len(), 1);
    assert_eq!(client.jobs_in_queue("alt").len(), 1);

    // Jobs of unknown classes are captured too.
    let mut payload = JobPayload::new("NonExistentJob", vec![json!(1)]);
    payload.queue = "missing".into();
    client.push(payload).await.unwrap();
    assert_eq!(client.jobs_in_queue("missing").len(), 1);
    assert_eq!(client.jobs_for("NonExistentJob").len(), 1);
    assert_eq!(client.all_jobs().len(), 3);
}

#[tokio::test]
async fn fake_drain_executes_every_stored_job_and_stops_at_a_failure() {
    let recorded = Arc::new(Recorded::default());
    let client = Testing::new().register(StoredJob(recorded.clone())).fake();
    StoredJob::perform_async(&client, (false,)).await.unwrap();
    StoredJob::perform_async(&client, (true,)).await.unwrap();
    StoredJob::perform_async(&client, (false,)).await.unwrap();
    assert_eq!(client.jobs::<StoredJob>().len(), 3);

    let error = client.drain::<StoredJob>().await.unwrap_err();
    let Error::Job(error) = error else {
        panic!("expected a job error, got {error:?}");
    };
    assert_eq!(error.message(), "perform failed");
    assert!(error.is::<PerformError>());
    // The failing job was removed before it ran; the one after it is still there.
    assert_eq!(recorded.runs(), 2);
    assert_eq!(client.jobs::<StoredJob>().len(), 1);
    assert_eq!(client.drain::<StoredJob>().await.unwrap(), 1);
    assert!(client.jobs::<StoredJob>().is_empty());
}

#[tokio::test]
async fn fake_executes_jobs_with_their_assigned_jid() {
    let recorded = Arc::new(Recorded::default());
    let client = Testing::new()
        .register(SpecificJidJob(recorded.clone()))
        .fake();
    for i in 0..4 {
        let jid = hench::generate_jid();
        let arg = if i % 2 == 0 {
            "wrong_jid".to_owned()
        } else {
            jid.clone()
        };
        SpecificJidJob::set()
            .jid(jid)
            .perform_async(&client, (arg,))
            .await
            .unwrap();
    }
    assert!(
        client
            .perform_one::<SpecificJidJob>()
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(recorded.runs(), 0);
    client.perform_one::<SpecificJidJob>().await.unwrap();
    assert_eq!(recorded.runs(), 1);
    assert_eq!(client.drain::<SpecificJidJob>().await.unwrap(), 2);
    assert_eq!(recorded.runs(), 2);
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Point {
    x: f64,
    label: String,
}

/// Records the argument it received.
struct RoundTrip(Arc<Mutex<Vec<(Point, f64)>>>);

impl Job for RoundTrip {
    const NAME: &'static str = "RoundTrip";
    type Args = (Point, f64);

    async fn perform(&self, _ctx: &JobContext, args: (Point, f64)) -> Result<(), JobError> {
        self.0.lock().unwrap().push(args);
        Ok(())
    }
}

#[tokio::test]
async fn fake_round_trips_the_job_arguments_through_json() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let client = Testing::new().register(RoundTrip(seen.clone())).fake();
    let point = Point {
        x: 0.1 + 0.2,
        label: "ünïcode".into(),
    };
    let now = now_secs_f64();
    RoundTrip::perform_async(&client, (point.clone(), now))
        .await
        .unwrap();
    let job = &client.jobs::<RoundTrip>()[0];
    assert_eq!(job.args[0], json!({"x": 0.1 + 0.2, "label": "ünïcode"}));
    client.drain::<RoundTrip>().await.unwrap();
    assert_eq!(*seen.lock().unwrap(), [(point, now)]);

    // Arguments JSON cannot represent are rejected when pushing.
    let error = RoundTrip::perform_async(
        &client,
        (
            Point {
                x: f64::NAN,
                label: String::new(),
            },
            1.0,
        ),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, Error::InvalidJob(_)), "{error:?}");
    assert_eq!(client.jobs::<RoundTrip>().len(), 0);
}

#[tokio::test]
async fn fake_perform_one_runs_only_one_job() {
    let recorded = Arc::new(Recorded::default());
    let client = Testing::new().register(DirectJob(recorded.clone())).fake();
    DirectJob::perform_async(&client, (1, 2)).await.unwrap();
    DirectJob::perform_async(&client, (3, 4)).await.unwrap();
    assert_eq!(client.jobs::<DirectJob>().len(), 2);

    let job = client.perform_one::<DirectJob>().await.unwrap().unwrap();
    assert_eq!(job.args, [json!(1), json!(2)]);
    assert_eq!(client.jobs::<DirectJob>().len(), 1);
    assert_eq!(*recorded.seen.lock().unwrap(), [vec![json!(1), json!(2)]]);

    client.clear::<DirectJob>();
    assert!(client.jobs::<DirectJob>().is_empty());
}

#[tokio::test]
async fn fake_perform_one_reports_an_empty_queue() {
    let client = testing().fake();
    assert!(client.perform_one::<FirstJob>().await.unwrap().is_none());
    assert!(client.perform_one_for("Nothing").await.unwrap().is_none());
}

#[tokio::test]
async fn fake_clears_jobs_across_all_job_types() {
    let counters_data = Arc::new(Counters::default());
    let client = testing().data_arc(counters_data.clone()).fake();
    FirstJob::perform_async(&client, ()).await.unwrap();
    SecondJob::perform_async(&client, ()).await.unwrap();
    assert_eq!(client.jobs::<FirstJob>().len(), 1);
    assert_eq!(client.jobs::<SecondJob>().len(), 1);

    client.clear_all();
    assert!(client.jobs::<FirstJob>().is_empty());
    assert!(client.jobs::<SecondJob>().is_empty());
    assert!(client.all_jobs().is_empty());
    assert_eq!(counters(&counters_data), (0, 0));
}

#[tokio::test]
async fn fake_drains_jobs_across_all_job_types() {
    let counters_data = Arc::new(Counters::default());
    let client = testing().data_arc(counters_data.clone()).fake();
    FirstJob::perform_async(&client, ()).await.unwrap();
    SecondJob::perform_async(&client, ()).await.unwrap();

    assert_eq!(client.drain_all().await.unwrap(), 2);
    assert!(client.jobs::<FirstJob>().is_empty());
    assert!(client.jobs::<SecondJob>().is_empty());
    assert_eq!(counters(&counters_data), (1, 1));
}

#[tokio::test]
async fn fake_drains_jobs_created_while_draining() {
    let counters_data = Arc::new(Counters::default());
    let client = testing().data_arc(counters_data.clone()).fake();
    ThirdJob::perform_async(&client, ()).await.unwrap();
    assert_eq!(client.jobs::<ThirdJob>().len(), 1);

    assert_eq!(client.drain_all().await.unwrap(), 3);
    assert!(client.all_jobs().is_empty());
    assert_eq!(counters(&counters_data), (1, 1));

    // Draining one class also drains jobs of that class pushed by its own jobs.
    ThirdJob::perform_async(&client, ()).await.unwrap();
    assert_eq!(client.drain::<ThirdJob>().await.unwrap(), 1);
    assert_eq!(client.jobs::<FirstJob>().len(), 1);
    assert_eq!(client.jobs::<SecondJob>().len(), 1);
}

#[tokio::test]
async fn fake_clears_and_drains_jobs_in_custom_queues() {
    let client = testing().fake();
    AltQueueJob::perform_async(&client, (5, 6)).await.unwrap();
    assert_eq!(client.jobs::<AltQueueJob>().len(), 1);
    assert_eq!(client.jobs_in_queue("alt").len(), 1);
    client.clear::<AltQueueJob>();
    assert!(client.jobs::<AltQueueJob>().is_empty());
    assert!(client.jobs_in_queue("alt").is_empty());

    AltQueueJob::perform_async(&client, (5, 6)).await.unwrap();
    assert_eq!(client.drain_all().await.unwrap(), 1);
    assert!(client.jobs::<AltQueueJob>().is_empty());
}

#[tokio::test]
async fn fake_queues_find_and_clear_enqueued_jobs() {
    let client = Testing::new()
        .register(DirectJob(Arc::default()))
        .register_default::<AltQueueJob>()
        .fake();
    assert!(client.jobs_in_queue("default").is_empty());
    DirectJob::perform_async(&client, (1, 2)).await.unwrap();
    DirectJob::perform_async(&client, (1, 2)).await.unwrap();
    AltQueueJob::perform_async(&client, (1, 2)).await.unwrap();

    let default = client.jobs_in_queue("default");
    assert_eq!(default.len(), 2);
    assert_eq!(default[0].args, [json!(1), json!(2)]);
    assert_eq!(client.jobs_in_queue("alt").len(), 1);

    client.clear_all();
    assert!(client.jobs_in_queue("default").is_empty());
    assert!(client.jobs::<DirectJob>().is_empty());
    assert!(client.jobs_in_queue("alt").is_empty());
}

#[tokio::test]
async fn fake_clones_share_jobs_but_fake_clients_are_independent() {
    let client = testing().fake();
    let clone = client.clone();
    FirstJob::perform_async(&clone, ()).await.unwrap();
    assert_eq!(client.jobs::<FirstJob>().len(), 1);
    // Derived clients (middleware, options) share the captured jobs too.
    let derived = client.with_default_job_options(JobOptions::defaults().queue("other"));
    FirstJob::perform_async(&derived, ()).await.unwrap();
    assert_eq!(client.jobs_in_queue("other").len(), 1);

    let other = testing().fake();
    assert!(other.jobs::<FirstJob>().is_empty());
}

#[tokio::test]
async fn fake_pushes_are_safe_from_many_tasks() {
    let client = testing().fake();
    let tasks: Vec<_> = (0..20)
        .map(|_| {
            let client = client.clone();
            tokio::spawn(async move {
                for _ in 0..50 {
                    FirstJob::perform_async(&client, ()).await.unwrap();
                }
            })
        })
        .collect();
    for task in tasks {
        task.await.unwrap();
    }
    let jobs = client.jobs::<FirstJob>();
    assert_eq!(jobs.len(), 1_000);
    let jids: std::collections::HashSet<_> = jobs.iter().map(|job| job.jid.clone()).collect();
    assert_eq!(jids.len(), 1_000);
}

/// Two jobs registered under one name make every run of a testing client fail with the
/// builder's error, said once ("configuration error: configuration error: ..." before).
#[tokio::test]
async fn testing_registry_errors_are_reported_once() {
    #[derive(Default)]
    struct First;
    impl Job for First {
        const NAME: &'static str = "Twice";
        type Args = ();
        async fn perform(&self, _: &JobContext, (): ()) -> Result<(), JobError> {
            Ok(())
        }
    }
    #[derive(Default)]
    struct Second;
    impl Job for Second {
        const NAME: &'static str = "Twice";
        type Args = ();
        async fn perform(&self, _: &JobContext, (): ()) -> Result<(), JobError> {
            Ok(())
        }
    }
    let client = Testing::new().register(First).register(Second).fake();
    First::perform_async(&client, ()).await.unwrap();
    let error = client.drain_all().await.unwrap_err().to_string();
    assert!(
        error.starts_with("configuration error: duplicate job name `Twice`")
            && error.matches("configuration error").count() == 1,
        "{error}"
    );
}
#[tokio::test]
async fn fake_bulk_pushes_are_captured() {
    let client = Testing::new().register(DirectJob(Arc::default())).fake();
    let jids = DirectJob::perform_bulk(&client, vec![(1, 1), (2, 2), (3, 3)])
        .await
        .unwrap();
    assert_eq!(jids.len(), 3);
    let jobs = client.jobs::<DirectJob>();
    assert_eq!(
        jobs.iter()
            .map(|job| Some(job.jid.clone()))
            .collect::<Vec<_>>(),
        jids
    );
    assert_eq!(jobs[2].args, [json!(3), json!(3)]);
}

#[tokio::test]
async fn fake_drain_reports_unknown_jobs_and_configuration_errors() {
    let client = Testing::new().auto_register(false).fake();
    client.push(JobPayload::new("Ghost", vec![])).await.unwrap();
    let error = client.drain_all().await.unwrap_err();
    assert!(
        matches!(error, Error::UnknownJob(ref name) if name == "Ghost"),
        "{error:?}"
    );
    // Nothing was removed: the job can still be inspected (and run once registered).
    assert_eq!(client.jobs_for("Ghost").len(), 1);
    assert!(client.perform_one_for("Ghost").await.is_err());
    assert_eq!(client.jobs_for("Ghost").len(), 1);

    // Duplicate names are a configuration error, reported when a job runs.
    let broken = Testing::new()
        .auto_register(false)
        .register_default::<FirstJob>()
        .register_default::<FirstJob>()
        .fake();
    FirstJob::perform_async(&broken, ()).await.unwrap();
    let error = broken.drain::<FirstJob>().await.unwrap_err();
    assert!(
        matches!(error, Error::Config(ref m) if m.contains("FirstJob")),
        "{error:?}"
    );
    assert_eq!(broken.jobs::<FirstJob>().len(), 1);

    // Draining is only possible on fake clients.
    let inline = Client::inline(Registry::new());
    assert!(matches!(inline.drain_all().await, Err(Error::Config(_))));
    assert!(matches!(
        inline.drain::<FirstJob>().await,
        Err(Error::Config(_))
    ));
    assert!(matches!(
        inline.perform_one::<FirstJob>().await,
        Err(Error::Config(_))
    ));
}

#[tokio::test]
#[should_panic(expected = "only available on fake clients")]
async fn fake_accessors_panic_on_redis_clients() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let _ = client.jobs::<FirstJob>();
}

/// Sets `foo` on every job it wraps.
struct AttributeMiddleware;

#[async_trait]
impl ServerMiddleware for AttributeMiddleware {
    async fn call(
        &self,
        ctx: &JobContext,
        job: &mut JobPayload,
        next: ServerNext<'_>,
    ) -> Result<(), JobError> {
        job.extra.insert("foo".into(), json!("bar"));
        next.run(ctx, job).await
    }
}

/// Counts runs where the middleware set `foo`.
struct AttributeJob(Arc<Recorded>);

impl Job for AttributeJob {
    const NAME: &'static str = "AttributeJob";
    type Args = ();

    async fn perform(&self, ctx: &JobContext, (): ()) -> Result<(), JobError> {
        if ctx.payload().extra.get("foo") == Some(&json!("bar")) {
            self.0.runs.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }
}

#[tokio::test]
async fn testing_clients_wrap_jobs_with_server_middleware() {
    let recorded = Arc::new(Recorded::default());
    let testing = Testing::new()
        .register(AttributeJob(recorded.clone()))
        .server_middleware(AttributeMiddleware);

    let fake = testing.fake();
    AttributeJob::perform_async(&fake, ()).await.unwrap();
    assert_eq!(recorded.runs(), 0);
    fake.perform_one::<AttributeJob>().await.unwrap();
    assert_eq!(recorded.runs(), 1);

    let inline = testing.inline();
    AttributeJob::perform_async(&inline, ()).await.unwrap();
    assert_eq!(recorded.runs(), 2);

    // Without the middleware the attribute is missing.
    let plain = Testing::new()
        .register(AttributeJob(recorded.clone()))
        .inline();
    AttributeJob::perform_async(&plain, ()).await.unwrap();
    assert_eq!(recorded.runs(), 2);
}

/// Fails when its argument is `false`; checks that it has a jid.
#[derive(Default)]
struct InlineJob;

#[derive(Debug)]
struct InlineError;

impl std::fmt::Display for InlineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("inline error")
    }
}

impl std::error::Error for InlineError {}

impl Job for InlineJob {
    const NAME: &'static str = "InlineJob";
    type Args = (bool,);

    async fn perform(&self, ctx: &JobContext, (pass,): (bool,)) -> Result<(), JobError> {
        if ctx.jid().is_empty() {
            return Err(JobError::msg("no jid"));
        }
        if pass {
            Ok(())
        } else {
            Err(InlineError.into())
        }
    }
}

fn inline_client() -> Client {
    let mut registry = Registry::new();
    registry.register_default::<InlineJob>().unwrap();
    Client::inline(registry)
}

fn assert_inline_error(error: Error) {
    let Error::Job(error) = error else {
        panic!("expected a job error, got {error:?}");
    };
    assert!(error.is::<InlineError>(), "{error:?}");
}

#[tokio::test]
async fn inline_stubs_the_async_call() {
    let client = inline_client();
    assert!(
        InlineJob::perform_async(&client, (true,))
            .await
            .unwrap()
            .is_some()
    );
    assert_inline_error(
        InlineJob::perform_async(&client, (false,))
            .await
            .unwrap_err(),
    );
}

#[tokio::test]
async fn inline_stubs_the_enqueue_call() {
    let client = inline_client();
    client.enqueue::<InlineJob>((true,)).await.unwrap();
    assert_inline_error(client.enqueue::<InlineJob>((false,)).await.unwrap_err());
    // Raw pushes run too.
    client
        .push(JobPayload::new("InlineJob", vec![json!(true)]))
        .await
        .unwrap();
    assert_inline_error(
        client
            .push(JobPayload::new("InlineJob", vec![json!(false)]))
            .await
            .unwrap_err(),
    );
}

#[tokio::test]
async fn inline_stubs_bulk_pushes_and_stops_at_the_first_failure() {
    let client = inline_client();
    client
        .push_bulk(BulkPush::new(
            "InlineJob",
            vec![vec![json!(true)], vec![json!(true)]],
        ))
        .await
        .unwrap();
    assert_inline_error(
        client
            .push_bulk(BulkPush::new(
                "InlineJob",
                vec![vec![json!(true)], vec![json!(false)]],
            ))
            .await
            .unwrap_err(),
    );
    assert_inline_error(
        InlineJob::perform_bulk(&client, vec![(true,), (false,)])
            .await
            .unwrap_err(),
    );
}

#[tokio::test]
async fn inline_relays_parameters_through_json() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let client = Testing::new().register(RoundTrip(seen.clone())).inline();
    let point = Point {
        x: now_secs_f64(),
        label: "time".into(),
    };
    RoundTrip::perform_async(&client, (point.clone(), 1.5))
        .await
        .unwrap();
    assert_eq!(*seen.lock().unwrap(), [(point, 1.5)]);
}

/// The `at` and `enqueued_at` of each run.
type Timings = Arc<Mutex<Vec<(Option<f64>, Option<i64>)>>>;

/// Records the timing fields of the payload it runs with.
struct Timing(Timings);

impl Job for Timing {
    const NAME: &'static str = "Timing";
    type Args = ();

    async fn perform(&self, ctx: &JobContext, (): ()) -> Result<(), JobError> {
        let payload = ctx.payload();
        self.0
            .lock()
            .unwrap()
            .push((payload.at, payload.enqueued_at));
        Ok(())
    }
}

#[tokio::test]
async fn scheduled_jobs_run_as_if_the_scheduler_had_enqueued_them() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let testing = Testing::new().register(Timing(seen.clone()));
    let fake = testing.fake();
    Timing::perform_in(&fake, Duration::from_secs(60), ())
        .await
        .unwrap();
    // Captured with its time...
    assert!(fake.jobs::<Timing>()[0].at.is_some());
    assert!(fake.jobs::<Timing>()[0].enqueued_at.is_none());
    // ...and run like a job the scheduler enqueued.
    fake.drain::<Timing>().await.unwrap();
    let inline = testing.inline();
    Timing::perform_in(&inline, Duration::from_secs(60), ())
        .await
        .unwrap();
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    for (at, enqueued_at) in seen.iter() {
        assert!(at.is_none());
        assert!((now_millis() - enqueued_at.unwrap()).abs() < 5_000);
    }
}

#[tokio::test]
async fn inline_runs_scheduled_jobs_immediately() {
    let recorded = Arc::new(Recorded::default());
    let client = Testing::new()
        .register(DirectJob(recorded.clone()))
        .inline();
    DirectJob::perform_in(&client, Duration::from_secs(3_600), (1, 2))
        .await
        .unwrap();
    DirectJob::perform_at(&client, SystemTime::now() + Duration::from_secs(60), (3, 4))
        .await
        .unwrap();
    assert_eq!(recorded.runs(), 2);
}

#[tokio::test]
async fn inline_runs_jobs_enqueued_by_jobs() {
    let counters_data = Arc::new(Counters::default());
    let client = testing().data_arc(counters_data.clone()).inline();
    ThirdJob::perform_async(&client, ()).await.unwrap();
    assert_eq!(counters(&counters_data), (1, 1));
}

#[tokio::test]
async fn inline_reports_unknown_jobs_and_panics() {
    let client = Client::inline(Registry::new());
    let error = client
        .push(JobPayload::new("Ghost", vec![]))
        .await
        .unwrap_err();
    assert!(
        matches!(error, Error::UnknownJob(ref name) if name == "Ghost"),
        "{error:?}"
    );

    /// Panics.
    struct Panics;
    impl Job for Panics {
        const NAME: &'static str = "Panics";
        type Args = (u8,);
        async fn perform(&self, _: &JobContext, (n,): (u8,)) -> Result<(), JobError> {
            panic!("boom {n}");
        }
    }
    let client = Testing::new().register(Panics).inline();
    let error = Panics::perform_async(&client, (7,)).await.unwrap_err();
    let Error::Job(error) = error else {
        panic!("expected a job error, got {error:?}");
    };
    assert!(error.is_panic());
    assert_eq!(error.message(), "boom 7");
}

/// Adds a custom key, or halts pushes of jobs tagged `halt`.
struct Stamp;

#[async_trait]
impl ClientMiddleware for Stamp {
    async fn call(
        &self,
        mut job: JobPayload,
        _redis: &RedisPool,
        next: ClientNext<'_>,
    ) -> hench::Result<Option<JobPayload>> {
        if job.tags.iter().any(|tag| tag == "halt") {
            return Ok(None);
        }
        job.extra.insert("stamped".into(), json!(true));
        next.run(job).await
    }
}

#[tokio::test]
async fn testing_clients_run_client_middleware() {
    let recorded = Arc::new(Recorded::default());
    let testing = Testing::new()
        .register(AttributeJob(recorded.clone()))
        .client_middleware(Stamp);

    let fake = testing.fake();
    AttributeJob::perform_async(&fake, ()).await.unwrap();
    assert_eq!(fake.jobs::<AttributeJob>()[0].extra["stamped"], json!(true));
    let halted = AttributeJob::set()
        .tags(["halt"])
        .perform_async(&fake, ())
        .await
        .unwrap();
    assert!(halted.is_none());
    assert_eq!(fake.jobs::<AttributeJob>().len(), 1);

    let inline = testing.inline();
    AttributeJob::set()
        .tags(["halt"])
        .perform_async(&inline, ())
        .await
        .unwrap();
    assert_eq!(recorded.runs(), 0);
}

#[tokio::test]
async fn testing_options_apply_to_pushes() {
    let client = Testing::new()
        .default_job_options(JobOptions::new().queue("tests").retry(3))
        .fake();
    client
        .push(JobPayload::new("Anything", vec![]))
        .await
        .unwrap();
    let job = &client.all_jobs()[0];
    assert_eq!(job.queue, "tests");
    assert_eq!(job.retry, Some(hench::RetryOption::Count(3)));
}

#[tokio::test]
async fn registered_jobs_replace_jobs_of_the_same_name() {
    /// A stand-in for `FirstJob`.
    struct FirstJobStub(Arc<Recorded>);
    impl Job for FirstJobStub {
        const NAME: &'static str = "FirstJob";
        type Args = ();
        async fn perform(&self, _: &JobContext, (): ()) -> Result<(), JobError> {
            self.0.runs.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    let recorded = Arc::new(Recorded::default());
    let mut registry = Registry::new();
    registry.register_default::<FirstJob>().unwrap();
    let client = Testing::new()
        .registry(registry)
        .register(FirstJobStub(recorded.clone()))
        .inline();
    // The stub runs (the real job would fail: no `Counters` data).
    FirstJob::perform_async(&client, ()).await.unwrap();
    assert_eq!(recorded.runs(), 1);
}

#[tokio::test]
async fn testing_from_hench_uses_its_jobs_data_and_middleware() {
    let redis = TestRedis::start().await;
    let counters_data = Arc::new(Counters::default());
    let hench = redis
        .builder()
        .await
        .auto_register(false)
        .register_default::<FirstJob>()
        .register_default::<ThirdJob>()
        .register_default::<SecondJob>()
        .data_arc(counters_data.clone())
        .default_job_options(JobOptions::new().queue("from-hench"))
        .build()
        .await
        .unwrap();
    let client = Testing::from_hench(&hench).fake();
    ThirdJob::perform_async(&client, ()).await.unwrap();
    assert_eq!(client.jobs_in_queue("from-hench").len(), 1);
    assert_eq!(client.drain_all().await.unwrap(), 3);
    assert_eq!(counters(&counters_data), (1, 1));
    // Nothing reached Redis.
    let stats: Option<String> = {
        let mut conn = redis.pool().await.conn().await.unwrap();
        hench::redis_rs::cmd("RANDOMKEY")
            .query_async(&mut conn)
            .await
            .unwrap()
    };
    assert_eq!(stats, None);
}

/// Tests that share the program's builder (`fn configure() -> HenchBuilder`) get its jobs, data
/// and middleware without a Redis: `Testing::from_hench` needs a built `Hench`, and `build`
/// connects (6 reconnection attempts, then an error, where no Redis runs).
#[tokio::test]
async fn testing_from_a_builder_needs_no_redis() {
    let counters_data = Arc::new(Counters::default());
    let configure = || {
        hench::Hench::builder()
            // Nothing listens there.
            .redis_url("redis://127.0.0.1:1")
            .auto_register(false)
            .register_default::<FirstJob>()
            .register_default::<ThirdJob>()
            .register_default::<SecondJob>()
            .data_arc(counters_data.clone())
            .default_job_options(JobOptions::new().queue("from-builder"))
    };
    let started = std::time::Instant::now();
    let client = Testing::from_builder(configure()).await.unwrap().fake();
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "{:?}",
        started.elapsed()
    );
    ThirdJob::perform_async(&client, ()).await.unwrap();
    assert_eq!(client.jobs_in_queue("from-builder").len(), 1);
    assert_eq!(client.drain_all().await.unwrap(), 3);
    assert_eq!(counters(&counters_data), (1, 1));

    // An invalid configuration fails as `build` would.
    let invalid = configure().register_default::<FirstJob>();
    assert!(matches!(
        Testing::from_builder(invalid).await,
        Err(Error::Config(_))
    ));
}

#[tokio::test]
async fn jobs_see_the_testing_client_in_their_context() {
    /// Checks the context it runs with.
    struct Inspect;
    impl Job for Inspect {
        const NAME: &'static str = "Inspect";
        type Args = ();
        async fn perform(&self, ctx: &JobContext, (): ()) -> Result<(), JobError> {
            assert!(ctx.client().is_fake());
            assert_eq!(ctx.queue(), "default");
            assert_eq!(ctx.retry_count(), None);
            assert!(ctx.payload().enqueued_at.is_some());
            assert!(!ctx.is_stopping());
            Ok(())
        }
    }
    let client = Testing::new().register(Inspect).fake();
    Inspect::perform_async(&client, ()).await.unwrap();
    client.drain::<Inspect>().await.unwrap();
}

/// The testing helpers can be awaited from multi-threaded runtimes (compile-time check).
#[test]
fn testing_futures_are_send() {
    fn send<T: Send>(_: &T) {}
    fn check(client: Client) {
        send(&client.drain::<FirstJob>());
        send(&client.drain_for("FirstJob"));
        send(&client.drain_all());
        send(&client.perform_one::<FirstJob>());
        send(&FirstJob::perform_async(&client, ()));
        send(&Testing::from_builder(hench::Hench::builder()));
    }
    let _ = check;
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Testing>();
}
