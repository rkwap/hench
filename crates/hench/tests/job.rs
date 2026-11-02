//! Jobs implemented by hand and used through `JobExt`, the registry (including `inventory`
//! registration exactly as `#[hench::job]` will generate it), contexts and dynamic jobs.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};

use futures::future::BoxFuture;
use hench::prelude::*;
use hench::test_util::TestRedis;
use hench::{Config, Data, ErasedJob, Error, Registry, RetryOption, args_to_values};
use serde_json::{Value, json};

/// Registered through inventory below, like a `#[hench::job]` function.
#[derive(Default)]
#[allow(non_camel_case_types)]
struct inventory_job;

impl Job for inventory_job {
    const NAME: &'static str = "tests::inventory_job";
    type Args = (String,);

    fn options() -> JobOptions {
        JobOptions::new().queue("inventory").retry(2)
    }

    async fn perform(&self, ctx: &JobContext, (name,): (String,)) -> Result<(), JobError> {
        let counter = ctx.data::<AtomicUsize>()?;
        counter.fetch_add(name.len(), Ordering::SeqCst);
        Ok(())
    }
}

hench::__private::inventory::submit! {
    hench::__private::JobRegistration::of::<inventory_job>()
}

#[test]
fn inventory_jobs_are_discovered() {
    let registry = Registry::from_inventory().unwrap();
    let job = registry.get("tests::inventory_job").expect("registered");
    assert_eq!(job.name(), "tests::inventory_job");
    assert_eq!(job.job_options().queue.as_deref(), Some("inventory"));
}

#[tokio::test]
async fn build_registers_inventory_jobs_unless_disabled() {
    let redis = TestRedis::start().await;
    let hench = redis.builder().await.build().await.unwrap();
    assert!(hench.registry().contains("tests::inventory_job"));

    let hench = redis
        .builder()
        .await
        .auto_register(false)
        .build()
        .await
        .unwrap();
    assert!(!hench.registry().contains("tests::inventory_job"));
}

#[tokio::test]
async fn build_connects_from_a_url_or_config() {
    let redis = TestRedis::start().await;
    let hench = Hench::builder()
        .redis_url(redis.url())
        .build()
        .await
        .unwrap();
    hench.redis().ping().await.unwrap();
    inventory_job::perform_async(hench.client(), ("url".into(),))
        .await
        .unwrap();
    assert_eq!(common::queue_size(hench.redis(), "inventory").await, 1);

    let hench = Hench::builder()
        .redis_config(redis.config())
        .build()
        .await
        .unwrap();
    assert_eq!(common::queue_size(hench.redis(), "inventory").await, 1);
}

#[tokio::test]
async fn build_fails_when_redis_is_unreachable() {
    let mut redis = TestRedis::start().await;
    let config = redis.config().reconnect_retries(0);
    redis.stop();
    let result = Hench::builder().redis_config(config).build().await;
    assert!(matches!(result, Err(Error::Redis(_))), "{result:?}");
}

#[tokio::test]
async fn manual_and_inventory_jobs_must_not_share_a_name() {
    let redis = TestRedis::start().await;
    let result = redis.builder().await.register(inventory_job).build().await;
    assert!(matches!(result, Err(Error::Config(ref m)) if m.contains("inventory_job")));
    // Registering it manually is fine when automatic registration is off.
    let hench = redis
        .builder()
        .await
        .auto_register(false)
        .register_default::<inventory_job>()
        .build()
        .await
        .unwrap();
    assert_eq!(hench.registry().names(), ["tests::inventory_job"]);
}

#[tokio::test]
async fn jobs_read_shared_data_from_their_context() {
    let redis = TestRedis::start().await;
    let hench = redis
        .builder()
        .await
        .data(AtomicUsize::new(0))
        .build()
        .await
        .unwrap();
    // What a processor does: look the job up and run it with the process's data.
    let payload = JobPayload::new("tests::inventory_job", vec![json!("abcd")]);
    let ctx =
        JobContext::new(payload.clone(), hench.client().clone()).with_data(hench.data().clone());
    let job = hench.registry().get(&payload.class).unwrap();
    job.perform_json(&ctx, payload.args.clone()).await.unwrap();
    job.perform_json(&ctx, vec![json!("xy")]).await.unwrap();
    assert_eq!(
        hench
            .data()
            .get::<AtomicUsize>()
            .unwrap()
            .load(Ordering::SeqCst),
        6
    );

    let without_data = JobContext::new(payload.clone(), hench.client().clone());
    let error = job
        .perform_json(&without_data, payload.args)
        .await
        .unwrap_err();
    assert_eq!(error.class(), JobError::MISSING_DATA);
}

#[tokio::test]
async fn typed_pushes_use_the_job_options() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let jid = inventory_job::perform_async(&client, ("x".into(),))
        .await
        .unwrap()
        .unwrap();
    let jobs = common::queue_jobs(client.redis(), "inventory").await;
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].jid, jid);
    assert_eq!(jobs[0].class, "tests::inventory_job");
    assert_eq!(jobs[0].retry, Some(RetryOption::Count(2)));
    assert_eq!(jobs[0].args, [json!("x")]);
}

/// A job with custom retry behaviour.
struct Flaky {
    attempts: AtomicUsize,
}

impl Job for Flaky {
    const NAME: &'static str = "Flaky";
    type Args = (u32,);

    async fn perform(&self, ctx: &JobContext, (fail_until,): (u32,)) -> Result<(), JobError> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
        if (attempt as u32) < fail_until {
            Err(JobError::new(
                "Flaky::NotYet",
                format!("attempt {attempt} of {}", ctx.jid()),
            ))
        } else {
            Ok(())
        }
    }

    fn retry_in(&self, count: u32, error: &JobError, _job: &JobPayload) -> RetryIn {
        match (count, error.class()) {
            (0, _) => RetryIn::After(Duration::from_secs(1)),
            (_, "Flaky::NotYet") if count > 3 => RetryIn::Kill,
            _ => RetryIn::Default,
        }
    }

    async fn retries_exhausted(&self, job: &JobPayload, _error: &JobError) -> Exhausted {
        if job.tags.iter().any(|tag| tag == "disposable") {
            Exhausted::Discard
        } else {
            Exhausted::Default
        }
    }
}

#[tokio::test]
async fn retry_hooks_are_reachable_through_erased_jobs() {
    let job: Arc<dyn ErasedJob> = Arc::new(Flaky {
        attempts: AtomicUsize::new(0),
    });
    let ctx = JobContext::new(JobPayload::new("Flaky", vec![]), Client::fake());
    let error = job.perform_json(&ctx, vec![json!(1)]).await.unwrap_err();
    assert_eq!(error.class(), "Flaky::NotYet");
    job.perform_json(&ctx, vec![json!(1)]).await.unwrap();

    let payload = JobPayload::new("Flaky", vec![]);
    assert_eq!(
        job.dyn_retry_in(0, &error, &payload),
        RetryIn::After(Duration::from_secs(1))
    );
    assert_eq!(job.dyn_retry_in(5, &error, &payload), RetryIn::Kill);
    assert_eq!(job.dyn_retry_in(2, &error, &payload), RetryIn::Default);
    assert_eq!(
        job.dyn_retries_exhausted(&payload, &error).await,
        Exhausted::Default
    );
    let mut disposable = payload.clone();
    disposable.tags.push("disposable".into());
    assert_eq!(
        job.dyn_retries_exhausted(&disposable, &error).await,
        Exhausted::Discard
    );
}

#[tokio::test]
async fn perform_inline_runs_a_job_in_process() {
    let job = Flaky {
        attempts: AtomicUsize::new(0),
    };
    let error = job.perform_inline((1,)).await.unwrap_err();
    assert_eq!(error.class(), "Flaky::NotYet");
    job.perform_inline((1,)).await.unwrap();
    assert_eq!(job.attempts.load(Ordering::SeqCst), 2);

    // The payload seen by the job reflects the options, like a real push.
    struct Inspect;
    impl Job for Inspect {
        const NAME: &'static str = "Inspect";
        type Args = ();
        fn options() -> JobOptions {
            JobOptions::new().queue("inspected")
        }
        async fn perform(&self, ctx: &JobContext, (): ()) -> Result<(), JobError> {
            assert_eq!(ctx.queue(), "inspected");
            assert_eq!(ctx.tags(), ["t"]);
            assert_eq!(ctx.jid().len(), 24);
            assert!(ctx.payload().created_at.is_some());
            assert!(ctx.client().is_fake());
            Ok(())
        }
    }
    Inspect::set()
        .tags(["t"])
        .perform_inline(&Inspect, ())
        .await
        .unwrap();
}

/// A job whose name is only known at runtime.
struct Dynamic {
    name: String,
    runs: Arc<AtomicUsize>,
}

impl ErasedJob for Dynamic {
    fn name(&self) -> &str {
        &self.name
    }

    fn job_options(&self) -> JobOptions {
        JobOptions::new()
    }

    fn perform_json<'a>(
        &'a self,
        _ctx: &'a JobContext,
        args: Vec<Value>,
    ) -> BoxFuture<'a, Result<(), JobError>> {
        Box::pin(async move {
            self.runs.fetch_add(args.len(), Ordering::SeqCst);
            Ok(())
        })
    }

    fn dyn_retry_in(&self, _count: u32, _error: &JobError, _job: &JobPayload) -> RetryIn {
        RetryIn::Discard
    }

    fn dyn_retries_exhausted<'a>(
        &'a self,
        _job: &'a JobPayload,
        _error: &'a JobError,
    ) -> BoxFuture<'a, Exhausted> {
        Box::pin(async { Exhausted::Discard })
    }
}

#[tokio::test]
async fn dynamic_jobs_can_be_registered() {
    let runs = Arc::new(AtomicUsize::new(0));
    let redis = TestRedis::start().await;
    let hench = redis
        .builder()
        .await
        .auto_register(false)
        .register_dyn(Arc::new(Dynamic {
            name: format!("Dynamic{}", 42),
            runs: runs.clone(),
        }))
        .build()
        .await
        .unwrap();
    let job = hench.registry().get("Dynamic42").unwrap();
    let ctx = JobContext::new(JobPayload::new("Dynamic42", vec![]), hench.client().clone());
    job.perform_json(&ctx, vec![json!(1), json!(2)])
        .await
        .unwrap();
    assert_eq!(runs.load(Ordering::SeqCst), 2);
}

#[test]
fn args_must_be_arrays() {
    #[derive(serde::Serialize, serde::Deserialize)]
    struct Named {
        id: u64,
    }
    assert!(matches!(
        args_to_values("J", &Named { id: 1 }),
        Err(Error::InvalidJob(_))
    ));
    assert_eq!(
        args_to_values("J", &(Named { id: 1 },)).unwrap(),
        [json!({"id": 1})]
    );
}

#[test]
fn core_types_are_thread_safe() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Client>();
    assert_send_sync::<Hench>();
    assert_send_sync::<Config>();
    assert_send_sync::<RedisPool>();
    assert_send_sync::<Registry>();
    assert_send_sync::<JobContext>();
    assert_send_sync::<JobPayload>();
    assert_send_sync::<JobError>();
    assert_send_sync::<Error>();
    assert_send_sync::<Data>();
    assert_send_sync::<hench::Launcher>();
}

// ---------------------------------------------------------------------------------------
// Jobs defined with `#[hench::job]` (Sidekiq's job_test scenarios, plus macro specifics).
// ---------------------------------------------------------------------------------------

/// Takes no arguments; queue `foo` with 12 retries (Sidekiq's `MySetJob`).
#[hench::job(queue = "foo", retry = 12)]
async fn my_set_job() {}

/// Takes one argument.
#[hench::job(queue = "foo", retry = 12)]
async fn my_set_job_with_arg(_value: i64) {}

/// Uses every option.
#[hench::job(
    name = "Billing::Charge",
    queue = "billing",
    retry = 3,
    backtrace = 10,
    dead = false,
    tags = ["billing", "money"],
    retry_for = 7200,
    retry_queue = "billing_retries"
)]
async fn charge(_amount: u64) {}

#[hench::job(retry = false, backtrace = true)]
async fn no_retries() {}

#[hench::job(retry = 0)]
async fn zero_retries() {}

#[hench::job(retry = true)]
async fn default_retries() {}

/// Records its arguments in the data registered for it.
#[hench::job]
async fn record_args(
    ctx: &JobContext,
    number: i64,
    text: String,
    list: Vec<u8>,
) -> Result<(), JobError> {
    let seen = ctx.data::<std::sync::Mutex<Vec<String>>>()?;
    seen.lock()
        .unwrap()
        .push(format!("{}|{number}|{text}|{list:?}", ctx.jid().len()));
    Ok(())
}

/// Destructures its argument with a pattern.
#[hench::job]
async fn sum_pair((a, b): (i64, i64), mut factor: i64) -> Result<(), JobError> {
    factor *= 2;
    if (a + b) * factor == 20 {
        Ok(())
    } else {
        Err(JobError::msg(format!("({a} + {b}) * {factor} != 20")))
    }
}

/// A custom application error.
#[derive(Debug)]
struct ChargeDeclined {
    code: u16,
}

impl std::fmt::Display for ChargeDeclined {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "charge declined with code {}", self.code)
    }
}

impl std::error::Error for ChargeDeclined {}

#[hench::job]
async fn custom_error(code: u16) -> Result<(), ChargeDeclined> {
    Err(ChargeDeclined { code })
}

#[hench::job]
async fn anyhow_error(fail: bool) -> anyhow::Result<()> {
    if fail {
        anyhow::bail!("anyhow says no");
    }
    Ok(())
}

#[hench::job]
async fn question_mark(path: String) -> Result<(), JobError> {
    let _ = std::fs::read(path)?;
    Ok(())
}

#[hench::job]
async fn panics(message: String) {
    panic!("{message}");
}

#[hench::job]
async fn unit_with_explicit_return_type(_x: u8) -> () {}

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Order {
    id: u64,
    items: Vec<String>,
}

#[hench::job]
async fn struct_argument(order: Order, expected: u64) -> Result<(), JobError> {
    if order.id == expected && order.items == ["book"] {
        Ok(())
    } else {
        Err(JobError::msg(format!("unexpected {order:?}")))
    }
}

/// Enqueues `my_set_job` from inside a job.
#[hench::job]
async fn enqueues_another(ctx: &JobContext) -> Result<(), JobError> {
    my_set_job::perform_async(ctx.client(), ()).await?;
    Ok(())
}

mod nested {
    /// Visible to the parent module.
    #[hench::job(queue = "nested")]
    pub(crate) async fn nested_job(value: String) -> Result<(), hench::JobError> {
        if value.is_empty() {
            return Err(hench::JobError::msg("empty"));
        }
        Ok(())
    }
}

/// Clippy's `unused_async` (pedantic) does not fire on jobs without `.await`: the macro needs an
/// `async fn`, so the lint's advice cannot be followed. (`cargo clippy --all-targets -- -D
/// warnings`, part of the checks before every commit, checks this module.)
#[warn(clippy::unused_async)]
mod synchronous_bodies {
    /// Does nothing asynchronous.
    #[hench::job]
    pub(crate) async fn no_await(n: u32) -> Result<(), hench::JobError> {
        if n > 0 {
            Ok(())
        } else {
            Err(hench::JobError::msg("zero"))
        }
    }
}

/// A deprecated job: the generated code names it through an alias, so that it can allow the
/// deprecation of the job without allowing it in the job's body.
#[deprecated(note = "use my_set_job")]
#[hench::job(queue = "retired")]
async fn retired_job(n: u32) -> Result<(), JobError> {
    if n > 0 {
        Ok(())
    } else {
        Err(JobError::msg("zero"))
    }
}

#[tokio::test]
#[allow(deprecated)]
async fn deprecated_and_synchronous_jobs_work_like_any_job() {
    assert_eq!(<retired_job as Job>::NAME, "retired_job");
    assert_eq!(retired_job::options().queue.as_deref(), Some("retired"));
    retired_job::perform_inline((1,)).await.unwrap();
    assert!(retired_job::perform_inline((0,)).await.is_err());
    let client = Client::fake();
    retired_job::perform_async(&client, (2,)).await.unwrap();
    synchronous_bodies::no_await::perform_async(&client, (3,))
        .await
        .unwrap();
    assert_eq!(client.drain_all().await.unwrap(), 2);
    let registry = Registry::from_inventory().unwrap();
    assert!(registry.contains("retired_job"));
    assert!(registry.contains("no_await"));
    synchronous_bodies::no_await::perform_inline((1,))
        .await
        .unwrap();
    assert!(
        synchronous_bodies::no_await::perform_inline((0,))
            .await
            .is_err()
    );
}

#[test]
fn macro_jobs_are_unit_structs_implementing_job() {
    fn assert_traits<J: Job + Clone + Copy + std::fmt::Debug + Default + Send + Sync>() {}
    assert_traits::<my_set_job>();
    assert_traits::<record_args>();
    assert_traits::<nested::nested_job>();
    assert_eq!(format!("{:?}", my_set_job), "my_set_job");
    let copied = charge;
    let _also_usable = copied;
    let _value_too = copied;

    assert_eq!(<my_set_job as Job>::NAME, "my_set_job");
    assert_eq!(<charge as Job>::NAME, "Billing::Charge");
    assert_eq!(<nested::nested_job as Job>::NAME, "nested_job");

    // Argument tuples follow the parameter list (the context is not an argument).
    fn args_of<J: Job>(args: J::Args) -> J::Args {
        args
    }
    let () = args_of::<my_set_job>(());
    let (_,): (i64,) = args_of::<my_set_job_with_arg>((1,));
    let (_, _, _): (i64, String, Vec<u8>) = args_of::<record_args>((1, String::new(), vec![]));
    let ((_, _), _): ((i64, i64), i64) = args_of::<sum_pair>(((1, 2), 3));
}

#[test]
fn macro_options_become_job_options() {
    let options = charge::options();
    assert_eq!(options.queue.as_deref(), Some("billing"));
    assert_eq!(options.retry, Some(RetryOption::Count(3)));
    assert_eq!(options.backtrace, Some(hench::BacktraceOption::Lines(10)));
    assert_eq!(options.dead, Some(false));
    assert_eq!(
        options.tags,
        Some(vec!["billing".to_owned(), "money".to_owned()])
    );
    assert_eq!(options.retry_for, Some(Duration::from_secs(7_200)));
    assert_eq!(options.retry_queue.as_deref(), Some("billing_retries"));

    let options = no_retries::options();
    assert_eq!(options.retry, Some(RetryOption::Bool(false)));
    assert_eq!(options.backtrace, Some(hench::BacktraceOption::Bool(true)));
    assert_eq!(zero_retries::options().retry, Some(RetryOption::Count(0)));
    assert_eq!(
        default_retries::options().retry,
        Some(RetryOption::Bool(true))
    );
    // Without options nothing is overridden.
    assert_eq!(record_args::options(), JobOptions::default());
}

#[test]
fn macro_jobs_are_discovered_through_inventory() {
    let registry = Registry::from_inventory().unwrap();
    for name in [
        "my_set_job",
        "Billing::Charge",
        "record_args",
        "nested_job",
        "tests::inventory_job",
    ] {
        assert!(registry.contains(name), "{name} is not registered");
    }
    // (`charge` itself names the job's unit struct, so it cannot be a variable name.)
    let erased = registry.get("Billing::Charge").unwrap();
    assert_eq!(erased.job_options().queue.as_deref(), Some("billing"));
}

#[tokio::test]
async fn macro_jobs_set_provides_scheduling_and_queue_overrides() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = client.redis();

    assert_eq!(common::scheduled_size(pool).await, 0);
    let jid = my_set_job::set()
        .at(SystemTime::now() + Duration::from_secs(3_600))
        .perform_async(&client, ())
        .await
        .unwrap();
    assert!(jid.is_some());
    assert_eq!(common::scheduled_size(pool).await, 1);
    my_set_job::set()
        .delay(Duration::from_secs(3_600))
        .perform_async(&client, ())
        .await
        .unwrap();
    assert_eq!(common::scheduled_size(pool).await, 2);

    assert_eq!(common::queue_size(pool, "foo").await, 0);
    my_set_job::perform_async(&client, ()).await.unwrap();
    my_set_job::perform_inline(()).await.unwrap();
    assert_eq!(common::queue_size(pool, "foo").await, 1);

    my_set_job::set()
        .queue("xyz")
        .perform_async(&client, ())
        .await
        .unwrap();
    assert_eq!(common::queue_size(pool, "xyz").await, 1);
}

#[tokio::test]
async fn macro_job_setters_can_be_memoized() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let setter = my_set_job_with_arg::set()
        .queue("bar")
        .custom("foo", "qaaz");
    for _ in 0..4 {
        setter.perform_async(&client, (1,)).await.unwrap();
    }
    let jobs = common::queue_jobs(client.redis(), "bar").await;
    assert_eq!(jobs.len(), 4);
    let jids: std::collections::HashSet<_> = jobs.iter().map(|job| job.jid.clone()).collect();
    assert_eq!(jids.len(), 4);
    assert!(jobs.iter().all(|job| job.extra["foo"] == json!("qaaz")));
    setter
        .perform_in(&client, Duration::from_secs(10), (1,))
        .await
        .unwrap();
    assert_eq!(common::scheduled_size(client.redis()).await, 1);
}

#[tokio::test]
async fn macro_job_setters_override_some_options_and_keep_the_others() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    my_set_job_with_arg::set()
        .queue("bar")
        .perform_async(&client, (1,))
        .await
        .unwrap();
    let job = &common::queue_jobs(client.redis(), "bar").await[0];
    assert_eq!(job.queue, "bar");
    assert_eq!(job.retry, Some(RetryOption::Count(12)));

    my_set_job_with_arg::set()
        .queue("baz")
        .retry(11)
        .perform_async(&client, (1,))
        .await
        .unwrap();
    let job = &common::queue_jobs(client.redis(), "baz").await[0];
    assert_eq!(job.retry, Some(RetryOption::Count(11)));

    // Without overrides the job's own options apply.
    my_set_job_with_arg::perform_async(&client, (1,))
        .await
        .unwrap();
    let job = &common::queue_jobs(client.redis(), "foo").await[0];
    assert_eq!(
        (job.queue.as_str(), job.retry),
        ("foo", Some(RetryOption::Count(12)))
    );

    // Chained calls accumulate, including scheduling.
    my_set_job_with_arg::set()
        .queue("bar")
        .custom("bar", "xyz")
        .delay(Duration::from_secs(3_600))
        .perform_async(&client, (1,))
        .await
        .unwrap();
    let scheduled = common::scheduled(client.redis()).await;
    assert_eq!(scheduled.len(), 1);
    assert_eq!(scheduled[0].0.queue, "bar");
    assert_eq!(scheduled[0].0.args, [json!(1)]);
    assert_eq!(scheduled[0].0.extra["bar"], json!("xyz"));
}

#[tokio::test]
async fn macro_jobs_perform_bulk() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let args: Vec<(i64,)> = (1..=1_001).map(|i| (i,)).collect();
    let jids = my_set_job_with_arg::set()
        .queue("bar")
        .perform_bulk(&client, args.clone())
        .await
        .unwrap();
    assert_eq!(jids.len(), 1_001);
    assert_eq!(common::queue_size(client.redis(), "bar").await, 1_001);

    let jids = my_set_job_with_arg::perform_bulk(&client, args)
        .await
        .unwrap();
    assert_eq!(jids.iter().flatten().count(), 1_001);
    assert_eq!(common::queue_size(client.redis(), "foo").await, 1_001);
}

#[tokio::test]
async fn macro_jobs_perform_in_and_at_normalize_times() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = client.redis();

    my_set_job::perform_in(&client, Duration::from_secs(60), ())
        .await
        .unwrap();
    assert_eq!(common::scheduled_size(pool).await, 1);
    assert_eq!(common::queue_size(pool, "foo").await, 0);

    // 1e9 seconds or more is an absolute timestamp.
    my_set_job::perform_in(&client, Duration::from_secs(2_000_000_000), ())
        .await
        .unwrap();
    let scheduled = common::scheduled(pool).await;
    assert_eq!(scheduled.len(), 2);
    assert_eq!(scheduled[1].1, 2_000_000_000.0);

    // A time in the past enqueues immediately.
    my_set_job::perform_at(&client, SystemTime::now() - Duration::from_secs(60), ())
        .await
        .unwrap();
    my_set_job::perform_in(&client, Duration::ZERO, ())
        .await
        .unwrap();
    assert_eq!(common::scheduled_size(pool).await, 2);
    assert_eq!(common::queue_size(pool, "foo").await, 2);
}

#[tokio::test]
async fn macro_jobs_store_their_options_in_payloads() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    charge::perform_async(&client, (100,)).await.unwrap();
    let job = &common::queue_jobs(client.redis(), "billing").await[0];
    assert_eq!(job.class, "Billing::Charge");
    assert_eq!(job.args, [json!(100)]);
    assert_eq!(job.retry, Some(RetryOption::Count(3)));
    assert_eq!(job.backtrace, Some(hench::BacktraceOption::Lines(10)));
    assert_eq!(job.dead, Some(false));
    assert_eq!(job.tags, ["billing", "money"]);
    assert_eq!(job.retry_for, Some(7_200));
    assert_eq!(job.retry_queue.as_deref(), Some("billing_retries"));

    no_retries::perform_async(&client, ()).await.unwrap();
    let job = &common::queue_jobs(client.redis(), "default").await[0];
    assert_eq!(job.retry, Some(RetryOption::Bool(false)));
    assert_eq!(job.args, Vec::<Value>::new());
}

#[tokio::test]
async fn macro_jobs_run_inline_with_typed_arguments() {
    sum_pair::perform_inline(((4, 1), 2)).await.unwrap();
    let error = sum_pair::perform_inline(((1, 1), 1)).await.unwrap_err();
    assert_eq!(error.message(), "(1 + 1) * 2 != 20");

    let order = Order {
        id: 7,
        items: vec!["book".into()],
    };
    struct_argument::perform_inline((order, 7)).await.unwrap();
    unit_with_explicit_return_type::perform_inline((1,))
        .await
        .unwrap();
    nested::nested_job::perform_inline(("x".into(),))
        .await
        .unwrap();
    assert!(
        nested::nested_job::perform_inline((String::new(),))
            .await
            .is_err()
    );
    // Method syntax through `JobExt` works too.
    my_set_job.perform_inline(()).await.unwrap();
}

#[tokio::test]
async fn macro_job_errors_keep_their_class_and_message() {
    let error = custom_error::perform_inline((402,)).await.unwrap_err();
    assert_eq!(error.class(), std::any::type_name::<ChargeDeclined>());
    assert_eq!(error.message(), "charge declined with code 402");
    assert_eq!(error.downcast_ref::<ChargeDeclined>().unwrap().code, 402);

    anyhow_error::perform_inline((false,)).await.unwrap();
    let error = anyhow_error::perform_inline((true,)).await.unwrap_err();
    assert_eq!(error.class(), "anyhow::Error");
    assert_eq!(error.message(), "anyhow says no");

    let error = question_mark::perform_inline(("/definitely/not/here".into(),))
        .await
        .unwrap_err();
    assert_eq!(error.class(), std::any::type_name::<std::io::Error>());

    let error = panics::perform_inline(("kaboom".into(),))
        .await
        .unwrap_err();
    assert!(error.is_panic());
    assert_eq!(error.message(), "kaboom");
}

#[tokio::test]
async fn macro_jobs_receive_the_context() {
    let seen = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let client = hench::testing::Testing::new().data_arc(seen.clone()).fake();
    record_args::perform_async(&client, (5, "five".into(), vec![1, 2]))
        .await
        .unwrap();
    // Registered automatically: no `register` needed to run it.
    assert_eq!(client.drain::<record_args>().await.unwrap(), 1);
    assert_eq!(*seen.lock().unwrap(), ["24|5|five|[1, 2]"]);

    // Without the data the job fails with a `MissingData` error.
    let error = record_args::perform_inline((1, String::new(), vec![]))
        .await
        .unwrap_err();
    assert_eq!(error.class(), JobError::MISSING_DATA);
}

#[tokio::test]
async fn macro_jobs_enqueue_more_jobs_through_the_context() {
    let client = Client::fake();
    enqueues_another::perform_async(&client, ()).await.unwrap();
    assert_eq!(client.drain_all().await.unwrap(), 2);
    assert!(client.all_jobs().is_empty());

    let client = Client::inline(Registry::from_inventory().unwrap());
    enqueues_another::perform_async(&client, ()).await.unwrap();
}

#[tokio::test]
async fn macro_jobs_are_registered_by_the_builder() {
    let redis = TestRedis::start().await;
    let hench = redis.builder().await.build().await.unwrap();
    for name in ["my_set_job", "Billing::Charge", "nested_job"] {
        assert!(hench.registry().contains(name), "{name}");
    }
    // What a processor does with a fetched payload.
    let payload = JobPayload::new("sum_pair", vec![json!([3, 2]), json!(2)]);
    let job = hench.registry().get(&payload.class).unwrap();
    let ctx = JobContext::new(payload.clone(), hench.client().clone());
    job.perform_json(&ctx, payload.args.clone()).await.unwrap();
    let error = job
        .perform_json(&ctx, vec![json!("not a pair")])
        .await
        .unwrap_err();
    assert_eq!(error.class(), JobError::INVALID_ARGS);
}

/// Adds `amount` to the shared counter (run by real workers below).
#[hench::job(queue = "macro_workers")]
async fn add_to_counter(ctx: &JobContext, amount: usize) -> Result<(), JobError> {
    ctx.data::<AtomicUsize>()?
        .fetch_add(amount, Ordering::SeqCst);
    Ok(())
}

#[tokio::test]
async fn macro_jobs_run_on_real_workers() {
    let redis = TestRedis::start().await;
    let counter = Arc::new(AtomicUsize::new(0));
    let hench = redis
        .builder()
        .await
        .queues(["macro_workers", "default"])
        .data_arc(counter.clone())
        .build()
        .await
        .unwrap();
    let launcher = hench.start().await.unwrap();
    let client = hench.client();

    // Registered through the inventory, routed to the queue named in the attribute, run by a
    // processor; a scheduled one is enqueued by the scheduler.
    add_to_counter::perform_async(client, (2,)).await.unwrap();
    add_to_counter::perform_in(client, Duration::from_millis(50), (3,))
        .await
        .unwrap();
    // Jobs enqueued from inside a macro job go through the process client: `my_set_job`
    // (queue `foo`, which nobody works on) waits in its queue.
    enqueues_another::perform_async(client, ()).await.unwrap();
    common::wait_until(Duration::from_secs(10), || async {
        counter.load(Ordering::SeqCst) == 5 && common::queue_size(hench.redis(), "foo").await == 1
    })
    .await;

    // A failure is recorded with the error type of the function's own error.
    let jid = custom_error::perform_async(client, (402,))
        .await
        .unwrap()
        .unwrap();
    let retries = hench::api::Api::from_hench(&hench).retries();
    common::wait_until(Duration::from_secs(10), || async {
        retries.find_job(&jid).await.unwrap().is_some()
    })
    .await;
    let entry = retries.find_job(&jid).await.unwrap().unwrap();
    assert_eq!(entry.class(), Some("custom_error"));
    assert_eq!(entry.error_class(), Some("job::ChargeDeclined"));
    assert_eq!(entry.error_message(), Some("charge declined with code 402"));
    launcher.stop().await.unwrap();
    let waiting = common::queue_jobs(hench.redis(), "foo").await;
    assert_eq!(waiting.len(), 1);
    assert_eq!(
        (waiting[0].class.as_str(), waiting[0].retry),
        ("my_set_job", Some(RetryOption::Count(12)))
    );
}

/// Instrumented: its span must carry the job's name.
#[hench::job]
#[tracing::instrument(skip(ctx), fields(jid = ctx.jid()))]
async fn instrumented(ctx: &JobContext, value: u32) -> Result<(), JobError> {
    tracing::info!(value, "running");
    Ok(())
}

#[tokio::test]
async fn macro_jobs_keep_their_name_in_tracing_spans() {
    use tracing_subscriber::layer::SubscriberExt;

    /// Records the names of new spans.
    struct SpanNames(Arc<std::sync::Mutex<Vec<String>>>);

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for SpanNames {
        fn on_new_span(
            &self,
            attrs: &tracing::span::Attributes<'_>,
            _id: &tracing::span::Id,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            self.0
                .lock()
                .unwrap()
                .push(attrs.metadata().name().to_owned());
        }
    }

    let names = Arc::new(std::sync::Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(SpanNames(names.clone()));
    let _guard = tracing::subscriber::set_default(subscriber);
    instrumented::perform_inline((7,)).await.unwrap();
    assert!(
        names
            .lock()
            .unwrap()
            .iter()
            .any(|name| name == "instrumented"),
        "{:?}",
        names.lock().unwrap()
    );
}

/// Registering a `#[hench::job]` by hand as well fails with an error that says so (it used to
/// read as if two different jobs shared a name), and real name clashes name both types.
#[tokio::test]
async fn registration_clashes_say_what_clashed() {
    let builder = || Hench::builder().redis(hench::RedisPool::detached());
    let error = builder()
        .register_default::<record_args>()
        .build()
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("job `record_args` (`job::record_args`) is registered twice")
            && error.contains("drop the `register` call")
            && error.contains("auto_register(false)"),
        "{error}"
    );

    /// Another job under the name of the macro job.
    #[derive(Default)]
    struct Imposter;
    impl Job for Imposter {
        const NAME: &'static str = "record_args";
        type Args = ();
        async fn perform(&self, _: &JobContext, (): ()) -> Result<(), JobError> {
            Ok(())
        }
    }
    let error = builder()
        .register(Imposter)
        .build()
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("`#[hench::job]` registered `job::record_args`")
            && error.contains("::Imposter` is registered with the same name")
            && error.contains("name = \"...\""),
        "{error}"
    );

    /// A third job, by hand, under the name of `Imposter`.
    #[derive(Default)]
    struct Twin;
    impl Job for Twin {
        const NAME: &'static str = "record_args";
        type Args = ();
        async fn perform(&self, _: &JobContext, (): ()) -> Result<(), JobError> {
            Ok(())
        }
    }
    let error = builder()
        .auto_register(false)
        .register(Imposter)
        .register(Twin)
        .build()
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("duplicate job name `record_args`")
            && error.contains("Imposter` and `")
            && error.contains("Twin` both have it"),
        "{error}"
    );
    let error = builder()
        .auto_register(false)
        .register(Twin)
        .register(Twin)
        .build()
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("Twin` is registered twice"), "{error}");
}

/// An iterable job whose run time is limited, run by `perform_inline`.
#[derive(Default)]
struct Chunked {
    done: AtomicUsize,
}

impl hench::iterable::IterableJob for Chunked {
    const NAME: &'static str = "Chunked";
    type Args = (u32,);
    type Item = u32;
    type Cursor = usize;

    fn build_enumerator<'a>(
        &'a self,
        _ctx: &'a JobContext,
        (count,): &'a (u32,),
        cursor: Option<usize>,
    ) -> Result<hench::iterable::Enumerator<'a, u32, usize>, JobError> {
        Ok(hench::iterable::Enumerator::vec(
            (0..*count).collect(),
            cursor,
        ))
    }

    async fn each_iteration(
        &self,
        _item: u32,
        _it: &hench::iterable::Iteration<'_, Self>,
    ) -> Result<(), JobError> {
        tokio::time::sleep(Duration::from_millis(30)).await;
        self.done.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn max_iteration_runtime(&self) -> Option<Duration> {
        Some(Duration::from_millis(50))
    }
}

/// `perform_inline` is one execution (as Sidekiq's): an interrupted iterable job returns the
/// interruption, whose message no longer claims the job "will continue" (nothing runs it again)
/// but names the inline testing client, which does run it to the end.
#[tokio::test]
async fn perform_inline_says_where_interrupted_jobs_run_to_the_end() {
    let job = Chunked::default();
    let error = job.perform_inline((10,)).await.unwrap_err();
    assert!(error.is_interrupted(), "{error:?}");
    assert!(
        error
            .message()
            .contains("perform_inline runs one execution")
            && error
                .message()
                .contains("Testing::new().register(job).inline()"),
        "{error:?}"
    );
    let done = job.done.load(Ordering::SeqCst);
    assert!((1..10).contains(&done), "{done}");

    let client = hench::testing::Testing::new()
        .register(Chunked::default())
        .inline();
    Chunked::perform_async(&client, (10,)).await.unwrap();
}
