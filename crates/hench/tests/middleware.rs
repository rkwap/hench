//! Middleware chains, ported scenario by scenario from Sidekiq's `middleware_test.rb`, plus
//! the hench-specific pieces (typed chain operations, `PerformEndpoint`, client chains).

mod common;

use std::sync::{Arc, Mutex};

use hench::prelude::*;
use hench::test_util::TestRedis;
use hench::{
    ClientChain, ErasedJob, Error, PerformEndpoint, ServerChain, ServerEndpoint, redis_rs,
};
use serde_json::json;

type Recorder = Arc<Mutex<Vec<String>>>;

fn log(recorder: &Recorder, entry: impl Into<String>) {
    recorder.lock().unwrap().push(entry.into());
}

fn entries(recorder: &Recorder) -> Vec<String> {
    recorder.lock().unwrap().clone()
}

macro_rules! recording_middleware {
    ($name:ident) => {
        struct $name(&'static str, Recorder);

        #[async_trait]
        impl ServerMiddleware for $name {
            async fn call(
                &self,
                ctx: &JobContext,
                job: &mut JobPayload,
                next: ServerNext<'_>,
            ) -> Result<(), JobError> {
                log(&self.1, format!("{} before", self.0));
                let result = next.run(ctx, job).await;
                log(&self.1, format!("{} after", self.0));
                result
            }
        }
    };
}

recording_middleware!(CustomMiddleware);
recording_middleware!(AnotherCustomMiddleware);
recording_middleware!(YetAnotherCustomMiddleware);

/// Never calls `next`.
struct NonYieldingMiddleware(#[allow(dead_code)] u32);

#[async_trait]
impl ServerMiddleware for NonYieldingMiddleware {
    async fn call(
        &self,
        _ctx: &JobContext,
        _job: &mut JobPayload,
        _next: ServerNext<'_>,
    ) -> Result<(), JobError> {
        Ok(())
    }
}

/// The job used by the ordering scenario: records that it ran.
struct CustomJob(Recorder);

impl Job for CustomJob {
    const NAME: &'static str = "CustomJob";
    type Args = ();

    async fn perform(&self, _ctx: &JobContext, (): ()) -> Result<(), JobError> {
        log(&self.0, "job_performed");
        Ok(())
    }
}

fn context(class: &str) -> JobContext {
    JobContext::new(JobPayload::new(class, vec![]), Client::fake())
}

async fn run(chain: &ServerChain, job: Arc<dyn ErasedJob>) -> Result<(), JobError> {
    let mut payload = JobPayload::new(job.name(), vec![]);
    let endpoint = PerformEndpoint::new(job);
    chain
        .invoke(&context(&payload.class.clone()), &mut payload, &endpoint)
        .await
}

#[test]
fn supports_custom_middleware() {
    let recorder = Recorder::default();
    let mut chain = ServerChain::new();
    chain.add(CustomMiddleware("1", recorder));
    assert!(chain.names().last().unwrap().ends_with("CustomMiddleware"));
    assert_eq!(chain.len(), 1);
}

#[tokio::test]
async fn executes_middleware_in_the_proper_order() {
    let recorder = Recorder::default();
    let mut chain = ServerChain::new();
    // Adding the same type twice keeps only the second one.
    for name in ["0", "1"] {
        chain.add(CustomMiddleware(name, recorder.clone()));
    }
    chain.insert_before::<CustomMiddleware>(AnotherCustomMiddleware("2", recorder.clone()));
    chain
        .insert_after::<AnotherCustomMiddleware>(YetAnotherCustomMiddleware("3", recorder.clone()));

    run(&chain, Arc::new(CustomJob(recorder.clone())))
        .await
        .unwrap();
    assert_eq!(
        entries(&recorder),
        [
            "2 before",
            "3 before",
            "1 before",
            "job_performed",
            "1 after",
            "3 after",
            "2 after"
        ]
    );
}

#[test]
fn replaces_middleware_configured_differently() {
    let mut chain = ServerChain::new();
    chain.add(NonYieldingMiddleware(1));
    chain.add(NonYieldingMiddleware(5));
    assert_eq!(chain.len(), 1);
}

#[test]
fn prepends_middleware() {
    let recorder = Recorder::default();
    let mut chain = ServerChain::new();
    chain.add(CustomMiddleware("c", recorder.clone()));
    chain.prepend(YetAnotherCustomMiddleware("y", recorder));
    let names = chain.names();
    assert!(
        names
            .first()
            .unwrap()
            .ends_with("YetAnotherCustomMiddleware")
    );
    assert!(names.last().unwrap().ends_with("::CustomMiddleware"));
}

#[tokio::test]
async fn middleware_can_stop_the_rest_of_the_chain() {
    let recorder = Recorder::default();
    let mut chain = ServerChain::new();
    chain.add(NonYieldingMiddleware(0));
    chain.add(CustomMiddleware("1", recorder.clone()));
    run(&chain, Arc::new(CustomJob(recorder.clone())))
        .await
        .unwrap();
    assert!(entries(&recorder).is_empty(), "{:?}", entries(&recorder));
}

#[tokio::test]
async fn an_empty_chain_runs_the_job() {
    let recorder = Recorder::default();
    run(&ServerChain::new(), Arc::new(CustomJob(recorder.clone())))
        .await
        .unwrap();
    assert_eq!(entries(&recorder), ["job_performed"]);
}

#[test]
fn removes_and_finds_middleware_by_type() {
    let recorder = Recorder::default();
    let mut chain = ServerChain::new();
    chain
        .add(CustomMiddleware("1", recorder.clone()))
        .add(AnotherCustomMiddleware("2", recorder.clone()))
        .add(YetAnotherCustomMiddleware("3", recorder));
    assert!(chain.exists::<AnotherCustomMiddleware>());
    chain.remove::<AnotherCustomMiddleware>();
    assert!(!chain.exists::<AnotherCustomMiddleware>());
    assert_eq!(chain.len(), 2);
    chain.remove::<NonYieldingMiddleware>();
    assert_eq!(chain.len(), 2);
    let copy = chain.clone();
    chain.clear();
    assert!(chain.is_empty());
    assert_eq!(copy.len(), 2);
}

#[test]
fn inserting_relative_to_a_missing_middleware() {
    let recorder = Recorder::default();
    let mut chain = ServerChain::new();
    chain.add(CustomMiddleware("1", recorder.clone()));
    chain.insert_before::<NonYieldingMiddleware>(AnotherCustomMiddleware("2", recorder.clone()));
    chain.insert_after::<NonYieldingMiddleware>(YetAnotherCustomMiddleware("3", recorder));
    let names = chain.names();
    assert!(names[0].ends_with("AnotherCustomMiddleware"));
    assert!(names[1].ends_with("::CustomMiddleware"));
    assert!(names[2].ends_with("YetAnotherCustomMiddleware"));
}

/// Changes the job's arguments and adds a key before running it.
struct Rewrite;

#[async_trait]
impl ServerMiddleware for Rewrite {
    async fn call(
        &self,
        ctx: &JobContext,
        job: &mut JobPayload,
        next: ServerNext<'_>,
    ) -> Result<(), JobError> {
        job.args = vec![json!("rewritten")];
        job.extra.insert("touched".into(), json!(true));
        next.run(ctx, job).await
    }
}

struct Echo(Recorder);

impl Job for Echo {
    const NAME: &'static str = "Echo";
    type Args = (String,);

    async fn perform(&self, ctx: &JobContext, (value,): (String,)) -> Result<(), JobError> {
        log(&self.0, value);
        log(
            &self.0,
            format!("touched={}", ctx.payload().extra.contains_key("touched")),
        );
        Ok(())
    }
}

#[tokio::test]
async fn jobs_run_with_the_payload_as_modified_by_middleware() {
    let recorder = Recorder::default();
    let mut chain = ServerChain::new();
    chain.add(Rewrite);
    let mut payload = JobPayload::new("Echo", vec![json!("original")]);
    let endpoint = PerformEndpoint::new(Arc::new(Echo(recorder.clone())));
    chain
        .invoke(&context("Echo"), &mut payload, &endpoint)
        .await
        .unwrap();
    assert_eq!(entries(&recorder), ["rewritten", "touched=true"]);
    assert_eq!(payload.args, [json!("rewritten")]);
}

#[tokio::test]
async fn invalid_arguments_fail_through_the_chain() {
    let recorder = Recorder::default();
    let mut chain = ServerChain::new();
    chain.add(CustomMiddleware("1", recorder.clone()));
    let mut payload = JobPayload::new("Echo", vec![json!(42)]);
    let endpoint = PerformEndpoint::new(Arc::new(Echo(recorder.clone())));
    let error = chain
        .invoke(&context("Echo"), &mut payload, &endpoint)
        .await
        .unwrap_err();
    assert_eq!(error.class(), JobError::INVALID_ARGS);
    assert_eq!(entries(&recorder), ["1 before", "1 after"]);
}

/// Fails before running the job.
struct Guard;

#[async_trait]
impl ServerMiddleware for Guard {
    async fn call(
        &self,
        _ctx: &JobContext,
        _job: &mut JobPayload,
        _next: ServerNext<'_>,
    ) -> Result<(), JobError> {
        Err(JobError::new("Guard::Denied", "not allowed"))
    }
}

#[tokio::test]
async fn middleware_errors_propagate() {
    let recorder = Recorder::default();
    let mut chain = ServerChain::new();
    chain.add(CustomMiddleware("outer", recorder.clone()));
    chain.add(Guard);
    let error = run(&chain, Arc::new(CustomJob(recorder.clone())))
        .await
        .unwrap_err();
    assert_eq!(error.class(), "Guard::Denied");
    assert_eq!(entries(&recorder), ["outer before", "outer after"]);
}

/// A custom endpoint: jobs need not be performed through `PerformEndpoint`.
struct CountingEndpoint(Recorder);

#[async_trait]
impl ServerEndpoint for CountingEndpoint {
    async fn call(&self, ctx: &JobContext, job: &mut JobPayload) -> Result<(), JobError> {
        log(&self.0, format!("endpoint {} {}", ctx.class(), job.class));
        Ok(())
    }
}

#[tokio::test]
async fn custom_endpoints() {
    let recorder = Recorder::default();
    let mut payload = JobPayload::new("Anything", vec![]);
    ServerChain::new()
        .invoke(
            &context("Anything"),
            &mut payload,
            &CountingEndpoint(recorder.clone()),
        )
        .await
        .unwrap();
    assert_eq!(entries(&recorder), ["endpoint Anything Anything"]);
}

// ---------------------------------------------------------------------------------------
// Client chains
// ---------------------------------------------------------------------------------------

struct Tag(&'static str, Recorder);

#[async_trait]
impl ClientMiddleware for Tag {
    async fn call(
        &self,
        mut job: JobPayload,
        _redis: &RedisPool,
        next: ClientNext<'_>,
    ) -> hench::Result<Option<JobPayload>> {
        log(&self.1, format!("{} before", self.0));
        job.tags.push(self.0.to_owned());
        let result = next.run(job).await;
        log(&self.1, format!("{} after", self.0));
        result
    }
}

struct Counter(Recorder);

#[async_trait]
impl ClientMiddleware for Counter {
    async fn call(
        &self,
        job: JobPayload,
        redis: &RedisPool,
        next: ClientNext<'_>,
    ) -> hench::Result<Option<JobPayload>> {
        // Middleware gets a pool to the Redis the job is pushed to.
        let mut conn = redis.conn().await?;
        let count: i64 = redis_rs::cmd("INCR")
            .arg("Counter")
            .query_async(&mut conn)
            .await?;
        log(&self.0, format!("count {count}"));
        next.run(job).await
    }
}

#[tokio::test]
async fn client_chains_run_in_order_and_modify_jobs() {
    let recorder = Recorder::default();
    let mut chain = ClientChain::new();
    chain.add(Tag("b", recorder.clone()));
    chain.prepend(Tag2("a", recorder.clone()));
    let job = chain
        .invoke(JobPayload::new("J", vec![]), &RedisPool::detached())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.tags, ["a", "b"]);
    assert_eq!(
        entries(&recorder),
        ["a before", "b before", "b after", "a after"]
    );
}

/// Same behaviour as `Tag`, distinct type (a chain holds one middleware per type).
struct Tag2(&'static str, Recorder);

#[async_trait]
impl ClientMiddleware for Tag2 {
    async fn call(
        &self,
        job: JobPayload,
        redis: &RedisPool,
        next: ClientNext<'_>,
    ) -> hench::Result<Option<JobPayload>> {
        Tag(self.0, self.1.clone()).call(job, redis, next).await
    }
}

#[tokio::test]
async fn client_middleware_can_use_redis() {
    let redis = TestRedis::start().await;
    let recorder = Recorder::default();
    let client = redis.client().await.with_middleware(|chain| {
        chain.add(Counter(recorder.clone()));
    });
    client.push(JobPayload::new("J", vec![])).await.unwrap();
    client.push(JobPayload::new("J", vec![])).await.unwrap();
    assert_eq!(entries(&recorder), ["count 1", "count 2"]);
}

#[tokio::test]
async fn client_middleware_errors_surface_from_detached_pools() {
    let recorder = Recorder::default();
    let mut chain = ClientChain::new();
    chain.add(Counter(recorder));
    let result = chain
        .invoke(JobPayload::new("J", vec![]), &RedisPool::detached())
        .await;
    assert!(matches!(result, Err(Error::Config(_))), "{result:?}");
}

#[tokio::test]
async fn builder_middleware_is_used_by_the_built_client() {
    let redis = TestRedis::start().await;
    let recorder = Recorder::default();
    let hench = Hench::builder()
        .redis(redis.pool().await)
        .client_middleware(Tag("built", recorder.clone()))
        .configure_client_middleware(|chain| {
            chain.prepend(Counter(recorder.clone()));
        })
        .server_middleware(NonYieldingMiddleware(0))
        .configure_server_middleware(|chain| {
            chain.insert_before::<NonYieldingMiddleware>(Rewrite);
        })
        .default_job_options(JobOptions::new().queue("low").retry(3))
        .build()
        .await
        .unwrap();
    assert!(hench.config().server_middleware.names()[0].ends_with("Rewrite"));
    hench
        .client()
        .push(JobPayload::new("J", vec![]))
        .await
        .unwrap();
    assert_eq!(
        entries(&recorder),
        ["count 1", "built before", "built after"]
    );
    let jobs = common::queue_jobs(hench.redis(), "low").await;
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].tags, ["built"]);
    assert_eq!(jobs[0].retry, Some(hench::RetryOption::Count(3)));
}
