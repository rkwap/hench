//! Defining jobs: the [`Job`] trait, the per-execution [`JobContext`], the shared [`Data`]
//! type map, type-erased jobs ([`ErasedJob`]), the [`Registry`] of jobs a worker can run and
//! the [`JobExt`] enqueueing helpers.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};

use futures::FutureExt;
use futures::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::batch::WORKFLOW_NEXT;
use crate::client::{BulkAt, BulkPush, Client, TypedKeys};
use crate::error::{Error, JobError, Result};
use crate::limit::{Throttle, ThrottleOption};
use crate::options::JobOptions;
use crate::payload::{BacktraceOption, JobPayload, RetryOption, now_secs_f64, system_time_to_secs};
use crate::unique::{OnConflict, PushOutcome, UniqueOption, UniqueUntil};

/// A background job.
///
/// Implement it by hand, or let `#[hench::job]` generate the implementation from an `async fn`.
///
/// ```
/// use hench::prelude::*;
///
/// #[derive(Default)]
/// struct SendWelcome;
///
/// impl Job for SendWelcome {
///     const NAME: &'static str = "SendWelcome";
///     type Args = (i64, String);
///
///     fn options() -> JobOptions {
///         JobOptions::new().queue("mailers").retry(5)
///     }
///
///     async fn perform(&self, ctx: &JobContext, (user_id, template): Self::Args) -> Result<(), JobError> {
///         tracing::info!(jid = ctx.jid(), user_id, template, "sending");
///         Ok(())
///     }
/// }
/// ```
///
/// Instances are shared (behind an `Arc`) by every concurrent execution, so per-run state
/// belongs in local variables; shared state belongs in [`Data`] (see [`JobContext::data`]).
pub trait Job: Send + Sync + 'static {
    /// The job's name, stored in the payload's `class` field. Must be unique in a process.
    const NAME: &'static str;

    /// The job's arguments. They must serialize to a JSON array, so use a tuple (`(A,)`,
    /// `(A, B)`, ...) or `()` for no arguments (stored as `[]`).
    type Args: Serialize + DeserializeOwned + Send + 'static;

    /// Options for this job type (Sidekiq's `sidekiq_options`). Explicit push options win
    /// over these, and these win over the process `default_job_options`.
    fn options() -> JobOptions {
        JobOptions::default()
    }

    /// Runs the job.
    fn perform(
        &self,
        ctx: &JobContext,
        args: Self::Args,
    ) -> impl Future<Output = Result<(), JobError>> + Send;

    /// Decides when (or whether) to retry after a failure (Sidekiq's `sidekiq_retry_in`).
    /// `count` is the job's `retry_count` after this failure: 0 after the first failure, 1
    /// after the second, and so on. The default keeps the standard backoff.
    fn retry_in(&self, count: u32, error: &JobError, job: &JobPayload) -> RetryIn {
        let _ = (count, error, job);
        RetryIn::Default
    }

    /// Called once the job's retries are exhausted, before it is moved to the morgue
    /// (Sidekiq's `sidekiq_retries_exhausted`). Return [`Exhausted::Discard`] to drop the job
    /// instead.
    fn retries_exhausted(
        &self,
        job: &JobPayload,
        error: &JobError,
    ) -> impl Future<Output = Exhausted> + Send {
        let _ = (job, error);
        std::future::ready(Exhausted::Default)
    }

    /// The part of the arguments that identifies a duplicate of a unique job (all of them when
    /// `None`, the default). Typed pushes call it when the merged options (the client's
    /// `default_job_options`, the job type's [`options`](Job::options), the push's) make the job
    /// unique and set no explicit key ([`JobSetter::unique_key`] wins over it).
    fn unique_key(args: &Self::Args) -> Option<Value> {
        let _ = args;
        None
    }

    /// The throttle key of these arguments (tenant, account): the throttle's per-key limits
    /// apply to it. `None` (the default): only the global limits apply. Typed pushes call it
    /// when the merged options (as for [`unique_key`](Job::unique_key)) name a throttle and set
    /// no explicit key.
    fn throttle_key(args: &Self::Args) -> Option<String> {
        let _ = args;
        None
    }

    /// A throttle defined with the job type, registered under its name when the worker is built
    /// (see [`Throttle`]). Default none.
    fn throttle() -> Option<Throttle> {
        None
    }
}

/// What to do after a failure, returned by [`Job::retry_in`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum RetryIn {
    /// Use the default backoff: `count⁴ + 15` seconds plus random jitter.
    #[default]
    Default,
    /// Retry after this delay (plus random jitter), and no later than the end of the year
    /// 9999, the latest time a job may have.
    After(Duration),
    /// Drop the job: no retry, no morgue (`discarded_at` is set; death handlers run).
    Discard,
    /// Treat the retries as exhausted now: the job goes to the morgue.
    Kill,
}

/// What to do with a job whose retries are exhausted, returned by
/// [`Job::retries_exhausted`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum Exhausted {
    /// Move the job to the morgue (unless its `dead` option is `false`).
    #[default]
    Default,
    /// Drop the job instead (`discarded_at` is set).
    Discard,
}

/// Information about the job being executed and access to process resources. Passed to
/// [`Job::perform`] and to server middleware.
#[derive(Clone)]
pub struct JobContext {
    payload: Arc<JobPayload>,
    queue: String,
    client: Client,
    data: Arc<Data>,
    stopping: CancellationToken,
    max_iteration_runtime: Option<Duration>,
    shutdown_timeout: Option<Duration>,
    /// The execution's early release of its unique lock (workers set it; see
    /// [`JobContext::release_unique_lock`]).
    early_release: Option<crate::unique::EarlyRelease>,
    /// Whether the execution handed its place in a limiter's wait line (`limited.s`, the slot a
    /// refusal rescheduled the job for) to an acquisition already: it is used once.
    limit_ticket_taken: Arc<AtomicBool>,
}

impl JobContext {
    /// Creates a context for `payload`. The queue is taken from the payload; the data map is
    /// empty and the context is never stopping unless configured with the `with_*` methods.
    /// Workers create contexts themselves; this is public so jobs can be unit tested.
    pub fn new(payload: JobPayload, client: Client) -> Self {
        let queue = payload.queue.clone();
        Self {
            payload: Arc::new(payload),
            queue,
            client,
            data: Arc::new(Data::new()),
            stopping: CancellationToken::new(),
            max_iteration_runtime: None,
            shutdown_timeout: None,
            early_release: None,
            limit_ticket_taken: Arc::new(AtomicBool::new(false)),
        }
    }

    /// The place this job holds in the wait line of one of `members` (the slot, in ms, of the
    /// refusal that rescheduled it, `limited.s`), for the first acquisition of that limit in this
    /// execution only: later ones are new callers.
    pub(crate) fn take_limit_ticket(&self, members: &[&str]) -> Option<i64> {
        let ticket = crate::limit::Limited::ticket_for(&self.payload, members)?;
        (!self.limit_ticket_taken.swap(true, Ordering::Relaxed)).then_some(ticket)
    }

    /// Shares the execution's early release of its unique lock with the job (the processor's
    /// state of the execution holds the same flag).
    #[must_use]
    pub(crate) fn with_early_release(mut self, early: crate::unique::EarlyRelease) -> Self {
        self.early_release = Some(early);
        self
    }

    /// The execution's early release of its unique lock, when a worker runs the job.
    pub(crate) fn early_release(&self) -> Option<&crate::unique::EarlyRelease> {
        self.early_release.as_ref()
    }

    /// Replaces the queue the job was fetched from.
    #[must_use]
    pub fn with_queue(mut self, queue: impl Into<String>) -> Self {
        self.queue = queue.into();
        self
    }

    /// Replaces the shared data map.
    #[must_use]
    pub fn with_data(mut self, data: Arc<Data>) -> Self {
        self.data = data;
        self
    }

    /// Replaces the shutdown signal.
    #[must_use]
    pub fn with_stopping(mut self, stopping: CancellationToken) -> Self {
        self.stopping = stopping;
        self
    }

    /// Replaces the payload (used to hand the job, as modified by server middleware, to
    /// `perform`). The queue is kept.
    #[must_use]
    pub fn with_payload(mut self, payload: JobPayload) -> Self {
        self.payload = Arc::new(payload);
        self
    }

    /// Sets how long an [iterable job](crate::iterable) may iterate before it saves its cursor
    /// and goes back to its queue (workers use
    /// [`HenchBuilder::max_iteration_runtime`](crate::HenchBuilder::max_iteration_runtime)).
    #[must_use]
    pub fn with_max_iteration_runtime(mut self, limit: Option<Duration>) -> Self {
        self.max_iteration_runtime = limit;
        self
    }

    /// Sets the process's shutdown timeout (workers use
    /// [`HenchBuilder::timeout`](crate::HenchBuilder::timeout)); iterable jobs warn when one
    /// iteration takes longer.
    #[must_use]
    pub fn with_shutdown_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    /// The job id.
    pub fn jid(&self) -> &str {
        &self.payload.jid
    }

    /// The job's class (its registered name).
    pub fn class(&self) -> &str {
        &self.payload.class
    }

    /// The queue the job was fetched from.
    pub fn queue(&self) -> &str {
        &self.queue
    }

    /// `None` on the first attempt; `Some(n)` when this execution is retry number `n + 1`.
    pub fn retry_count(&self) -> Option<u32> {
        self.payload.retry_count
    }

    /// The batch id, if any.
    pub fn bid(&self) -> Option<&str> {
        self.payload.bid.as_deref()
    }

    /// The job's tags.
    pub fn tags(&self) -> &[String] {
        &self.payload.tags
    }

    /// The full payload.
    pub fn payload(&self) -> &JobPayload {
        &self.payload
    }

    /// A client to enqueue more jobs (configured like the process's client).
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Shared state of type `T` registered with
    /// [`HenchBuilder::data`](crate::HenchBuilder::data). Fails with a [`JobError`] of class
    /// [`JobError::MISSING_DATA`] if nothing of that type was registered, so `?` works in jobs.
    pub fn data<T: Send + Sync + 'static>(&self) -> Result<&T, JobError> {
        self.data.get::<T>().ok_or_else(missing_data::<T>)
    }

    /// Like [`JobContext::data`] but returns an `Arc`, for moving into spawned tasks.
    pub fn data_arc<T: Send + Sync + 'static>(&self) -> Result<Arc<T>, JobError> {
        self.data.get_arc::<T>().ok_or_else(missing_data::<T>)
    }

    /// The whole data map.
    pub fn data_map(&self) -> &Arc<Data> {
        &self.data
    }

    /// `true` once the process has started shutting down. Long-running jobs should check it
    /// and return early (they are re-run later if they return an error or are interrupted).
    pub fn is_stopping(&self) -> bool {
        self.stopping.is_cancelled()
    }

    /// Completes when the process starts shutting down. Useful with `tokio::select!`.
    pub async fn stopping(&self) {
        self.stopping.cancelled().await;
    }

    /// The shutdown signal itself.
    pub fn stopping_token(&self) -> &CancellationToken {
        &self.stopping
    }

    /// How long an [iterable job](crate::iterable) may iterate in one execution before it
    /// saves its cursor and goes back to its queue (`None`: no limit).
    pub fn max_iteration_runtime(&self) -> Option<Duration> {
        self.max_iteration_runtime
    }

    /// How long the process waits for running jobs when it stops, if known.
    pub fn shutdown_timeout(&self) -> Option<Duration> {
        self.shutdown_timeout
    }
}

impl fmt::Debug for JobContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JobContext")
            .field("class", &self.class())
            .field("jid", &self.jid())
            .field("queue", &self.queue)
            .field("stopping", &self.is_stopping())
            .finish_non_exhaustive()
    }
}

fn missing_data<T>() -> JobError {
    JobError::new(
        JobError::MISSING_DATA,
        format!(
            "no data of type `{}` was registered (use HenchBuilder::data)",
            std::any::type_name::<T>()
        ),
    )
}

/// A map from type to value holding shared application state (database pools, HTTP clients,
/// configuration) that jobs read through [`JobContext::data`].
#[derive(Clone, Default)]
pub struct Data {
    values: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
}

impl Data {
    /// An empty map.
    pub fn new() -> Self {
        Self::default()
    }

    /// Stores `value`, replacing (and returning) any previous value of the same type.
    pub fn insert<T: Send + Sync + 'static>(&mut self, value: T) -> Option<Arc<T>> {
        self.insert_arc(Arc::new(value))
    }

    /// Stores an `Arc`, replacing (and returning) any previous value of the same type.
    pub fn insert_arc<T: Send + Sync + 'static>(&mut self, value: Arc<T>) -> Option<Arc<T>> {
        self.values
            .insert(TypeId::of::<T>(), value)
            .and_then(|previous| previous.downcast::<T>().ok())
    }

    /// The value of type `T`, if any.
    pub fn get<T: Send + Sync + 'static>(&self) -> Option<&T> {
        self.values
            .get(&TypeId::of::<T>())
            .and_then(|value| value.downcast_ref::<T>())
    }

    /// The value of type `T` as an `Arc`, if any.
    pub fn get_arc<T: Send + Sync + 'static>(&self) -> Option<Arc<T>> {
        self.values
            .get(&TypeId::of::<T>())
            .and_then(|value| Arc::clone(value).downcast::<T>().ok())
    }

    /// `true` if a value of type `T` is stored.
    pub fn contains<T: Send + Sync + 'static>(&self) -> bool {
        self.values.contains_key(&TypeId::of::<T>())
    }

    /// Number of stored values.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// `true` if nothing is stored.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

impl fmt::Debug for Data {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Data")
            .field("len", &self.values.len())
            .finish()
    }
}

/// An object-safe, type-erased [`Job`], as stored in the [`Registry`].
///
/// Every `Job` implements it. Implement it directly only for jobs whose name is known at
/// runtime; the methods carry a `dyn_`/`job_` prefix so that they never clash with [`Job`]'s.
pub trait ErasedJob: Send + Sync + 'static {
    /// The job's name (`Job::NAME`).
    fn name(&self) -> &str;

    /// The job type's options (`Job::options`).
    fn job_options(&self) -> JobOptions;

    /// Deserializes `args` and runs the job. Arguments that do not deserialize produce a
    /// [`JobError`] of class [`JobError::INVALID_ARGS`].
    fn perform_json<'a>(
        &'a self,
        ctx: &'a JobContext,
        args: Vec<Value>,
    ) -> BoxFuture<'a, Result<(), JobError>>;

    /// `Job::retry_in`.
    fn dyn_retry_in(&self, count: u32, error: &JobError, job: &JobPayload) -> RetryIn;

    /// `Job::retries_exhausted`.
    fn dyn_retries_exhausted<'a>(
        &'a self,
        job: &'a JobPayload,
        error: &'a JobError,
    ) -> BoxFuture<'a, Exhausted>;

    /// The Rust type implementing the job (its [`std::any::type_name`], such as
    /// `my_app::billing::sync` for a `#[hench::job] fn sync` in module `billing`): diagnostics
    /// name it, for example when two jobs share a name.
    fn dyn_type_name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    /// `Job::throttle`: the throttle defined with the job type.
    fn dyn_throttle(&self) -> Option<Throttle> {
        None
    }

    /// `Job::unique_key` and `Job::throttle_key` of `args` (JSON arguments, deserialized into
    /// the job's arguments), for definitions that name the job by its class: a schedule of the
    /// worker's configuration file takes them as `Periodic::job` does. `Err` with the reason
    /// when the arguments do not deserialize.
    fn dyn_keys(
        &self,
        args: &[Value],
    ) -> std::result::Result<(Option<Value>, Option<String>), String> {
        let _ = args;
        Ok((None, None))
    }
}

impl<J: Job> ErasedJob for J {
    fn name(&self) -> &str {
        J::NAME
    }

    fn job_options(&self) -> JobOptions {
        J::options()
    }

    fn perform_json<'a>(
        &'a self,
        ctx: &'a JobContext,
        args: Vec<Value>,
    ) -> BoxFuture<'a, Result<(), JobError>> {
        match args_from_values::<J::Args>(args) {
            Ok(args) => Box::pin(self.perform(ctx, args)),
            Err(error) => Box::pin(std::future::ready(Err(JobError::invalid_args(
                J::NAME,
                error,
            )))),
        }
    }

    fn dyn_retry_in(&self, count: u32, error: &JobError, job: &JobPayload) -> RetryIn {
        self.retry_in(count, error, job)
    }

    fn dyn_retries_exhausted<'a>(
        &'a self,
        job: &'a JobPayload,
        error: &'a JobError,
    ) -> BoxFuture<'a, Exhausted> {
        Box::pin(self.retries_exhausted(job, error))
    }

    fn dyn_throttle(&self) -> Option<Throttle> {
        J::throttle()
    }

    fn dyn_keys(
        &self,
        args: &[Value],
    ) -> std::result::Result<(Option<Value>, Option<String>), String> {
        let args = args_from_values::<J::Args>(args.to_vec()).map_err(|error| error.to_string())?;
        Ok((J::unique_key(&args), J::throttle_key(&args)))
    }
}

/// Serializes typed job arguments into the payload's `args` array.
///
/// `()` (and anything else serializing to `null`) becomes `[]`. Values that do not
/// serialize to an array are rejected with [`Error::InvalidJob`], and so are arguments that
/// do not deserialize back into `A` (for example non-finite floats, which JSON cannot
/// represent and which would otherwise silently become `null`).
pub fn args_to_values<A>(job: &str, args: &A) -> Result<Vec<Value>>
where
    A: Serialize + DeserializeOwned,
{
    let value = serde_json::to_value(args)?;
    if let Err(error) = A::deserialize(&value) {
        return Err(Error::InvalidJob(format!(
            "arguments of job `{job}` must be native JSON values that deserialize back into \
             the job's arguments (non-finite floats are not valid JSON): {error}"
        )));
    }
    match value {
        Value::Null => Ok(Vec::new()),
        Value::Array(values) => Ok(values),
        other => Err(Error::InvalidJob(format!(
            "arguments of job `{job}` must serialize to a JSON array (use a tuple such as \
             `(value,)`), not {}",
            json_kind(&other)
        ))),
    }
}

/// Deserializes the payload's `args` array into typed arguments. An empty array also
/// deserializes into `()` and unit structs.
pub fn args_from_values<A: DeserializeOwned>(
    values: Vec<Value>,
) -> std::result::Result<A, serde_json::Error> {
    let empty = values.is_empty();
    match serde_json::from_value(Value::Array(values)) {
        Ok(args) => Ok(args),
        Err(error) if empty => A::deserialize(&Value::Null).map_err(|_| error),
        Err(error) => Err(error),
    }
}

fn json_kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

/// Runs a job future, turning a panic into a [`JobError`] of class [`JobError::PANIC`].
pub(crate) async fn catch_panic<F>(future: F) -> Result<(), JobError>
where
    F: Future<Output = Result<(), JobError>>,
{
    match AssertUnwindSafe(future).catch_unwind().await {
        Ok(result) => result,
        // On the thread that panicked: what the panic hook recorded is this panic's.
        Err(payload) => Err(crate::error::with_panic_origin(JobError::panic(payload))),
    }
}

/// The jobs a worker process can run, by name.
#[derive(Clone, Default)]
pub struct Registry {
    jobs: HashMap<String, Arc<dyn ErasedJob>>,
}

impl Registry {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// A registry containing every job registered through `inventory`: the `#[hench::job]`
    /// items of the crates linked into the binary. Fails if two of them share a name.
    ///
    /// Linked means used: rustc leaves out a dependency that the binary never refers to, and
    /// its jobs with it, even though it is in `Cargo.toml`. A worker binary whose jobs live in
    /// a library it does not otherwise use must name that library, for example with
    /// `use my_jobs as _;`, or register the jobs explicitly.
    pub fn from_inventory() -> Result<Self> {
        let mut registry = Self::new();
        for registration in inventory::iter::<JobRegistration> {
            registry.register_dyn(registration.instantiate())?;
        }
        Ok(registry)
    }

    /// Registers a job instance.
    pub fn register<J: Job>(&mut self, job: J) -> Result<&mut Self> {
        self.register_dyn(Arc::new(job))
    }

    /// Registers `J::default()`.
    pub fn register_default<J: Job + Default>(&mut self) -> Result<&mut Self> {
        self.register(J::default())
    }

    /// Registers a type-erased job. Fails with [`Error::Config`] if the name is empty or
    /// already registered (the message names the types of both jobs).
    pub fn register_dyn(&mut self, job: Arc<dyn ErasedJob>) -> Result<&mut Self> {
        let name = job.name().to_owned();
        if name == WORKFLOW_NEXT {
            return Err(Error::Config(format!(
                "the job name `{name}` is reserved for hench's workflows (`{}`)",
                job.dyn_type_name()
            )));
        }
        if name.is_empty() {
            return Err(Error::Config(format!(
                "job names must not be empty (`{}`)",
                job.dyn_type_name()
            )));
        }
        if let Some(registered) = self.jobs.get(&name) {
            let (first, second) = (registered.dyn_type_name(), job.dyn_type_name());
            let which = if first == second {
                format!("`{first}` is registered twice")
            } else {
                format!("`{first}` and `{second}` both have it")
            };
            return Err(Error::Config(format!(
                "duplicate job name `{name}`: {which}; every job must have a unique name (rename \
                 one with `#[hench::job(name = \"...\")]` or its `Job::NAME`)"
            )));
        }
        self.jobs.insert(name, job);
        Ok(self)
    }

    /// Adds every job of `other`. Fails on the first duplicate name.
    pub fn merge(&mut self, other: Registry) -> Result<&mut Self> {
        let mut jobs: Vec<_> = other.jobs.into_iter().collect();
        jobs.sort_by(|(a, _), (b, _)| a.cmp(b));
        for (_, job) in jobs {
            self.register_dyn(job)?;
        }
        Ok(self)
    }

    /// The job registered as `name`.
    pub fn get(&self, name: &str) -> Option<&Arc<dyn ErasedJob>> {
        self.jobs.get(name)
    }

    /// The job that runs a fetched job of class `name`: the registered one, or hench's own job
    /// for the reserved [`WORKFLOW_NEXT`] name (resolved in every process without being listed
    /// among the registered names).
    pub(crate) fn resolve(&self, name: &str) -> Option<Arc<dyn ErasedJob>> {
        match self.jobs.get(name) {
            Some(job) => Some(job.clone()),
            None if name == WORKFLOW_NEXT => Some(crate::batch::workflow::job()),
            None => None,
        }
    }

    /// `true` if a job is registered as `name`.
    pub fn contains(&self, name: &str) -> bool {
        self.jobs.contains_key(name)
    }

    /// Registered names, sorted.
    pub fn names(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self.jobs.keys().map(String::as_str).collect();
        names.sort_unstable();
        names
    }

    /// Number of registered jobs.
    pub fn len(&self) -> usize {
        self.jobs.len()
    }

    /// `true` if no job is registered.
    pub fn is_empty(&self) -> bool {
        self.jobs.is_empty()
    }
}

impl fmt::Debug for Registry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Registry").field(&self.names()).finish()
    }
}

/// A job registered through `inventory` (generated by `#[hench::job]`).
#[doc(hidden)]
pub struct JobRegistration {
    name: &'static str,
    make: fn() -> Arc<dyn ErasedJob>,
}

impl JobRegistration {
    /// Registration of `J::default()`.
    pub const fn of<J: Job + Default>() -> Self {
        Self {
            name: J::NAME,
            make: make_default::<J>,
        }
    }

    /// Registration with an explicit constructor.
    pub const fn new(name: &'static str, make: fn() -> Arc<dyn ErasedJob>) -> Self {
        Self { name, make }
    }

    /// The registered job's name.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Creates the job instance.
    pub fn instantiate(&self) -> Arc<dyn ErasedJob> {
        (self.make)()
    }
}

fn make_default<J: Job + Default>() -> Arc<dyn ErasedJob> {
    Arc::new(J::default())
}

inventory::collect!(JobRegistration);

/// Enqueueing helpers available on every [`Job`] (in the prelude).
///
/// ```no_run
/// # use hench::prelude::*;
/// # #[derive(Default)] struct Resize;
/// # impl Job for Resize {
/// #     const NAME: &'static str = "Resize";
/// #     type Args = (u64,);
/// #     async fn perform(&self, _: &JobContext, _: (u64,)) -> Result<(), JobError> { Ok(()) }
/// # }
/// # async fn demo(client: &Client) -> hench::Result<()> {
/// use std::time::Duration;
///
/// Resize::perform_async(client, (42,)).await?;
/// Resize::perform_in(client, Duration::from_secs(60), (42,)).await?;
/// Resize::set().queue("low").retry(false).perform_async(client, (42,)).await?;
/// Resize::perform_bulk(client, vec![(1,), (2,), (3,)]).await?;
/// # Ok(()) }
/// ```
pub trait JobExt: Job + Sized {
    /// Pushes the job for immediate execution. Returns its jid, or `None` if client
    /// middleware stopped the push. An error after a timeout may come with the job pushed
    /// all the same: see [when a push fails](Client#when-a-push-fails) (the same holds for
    /// every push).
    fn perform_async(
        client: &Client,
        args: Self::Args,
    ) -> impl Future<Output = Result<Option<String>>> + Send {
        client.enqueue::<Self>(args)
    }

    /// Schedules the job to run after `delay`. Like Sidekiq, a delay of 1e9 seconds or more
    /// is taken as an absolute Unix timestamp, and a zero delay pushes immediately.
    fn perform_in(
        client: &Client,
        delay: Duration,
        args: Self::Args,
    ) -> impl Future<Output = Result<Option<String>>> + Send {
        client.enqueue_in::<Self>(delay, args)
    }

    /// Schedules the job to run at `at`; a time in the past pushes immediately.
    fn perform_at(
        client: &Client,
        at: SystemTime,
        args: Self::Args,
    ) -> impl Future<Output = Result<Option<String>>> + Send {
        client.enqueue_at::<Self>(at, args)
    }

    /// Pushes one job per element of `args` in as few round trips as possible (batches of
    /// 1000). Returns one entry per job: its jid, or `None` if client middleware stopped it.
    fn perform_bulk(
        client: &Client,
        args: Vec<Self::Args>,
    ) -> impl Future<Output = Result<Vec<Option<String>>>> + Send {
        async move { JobSetter::<Self>::new().perform_bulk(client, args).await }
    }

    /// Options for a single push: `J::set().queue("low").perform_async(&client, args)`.
    fn set() -> JobSetter<Self> {
        JobSetter::new()
    }

    /// Runs this job instance right now, in this process, without Redis.
    ///
    /// The arguments make a JSON round trip exactly like a real push, and the job sees a
    /// context whose client is a fake one ([`Client::fake`]): jobs it enqueues are captured
    /// in memory, not pushed. The data map is empty and middleware does not run (use
    /// [`Client::inline`] for a full in-process pipeline). Panics are caught and returned
    /// as a [`JobError`] of class [`JobError::PANIC`].
    ///
    /// This is one execution, as Sidekiq's `perform_inline`: a job that asks to continue later
    /// ([`JobError::interrupted`], such as an [iterable job](crate::iterable) whose
    /// `max_iteration_runtime` is up) returns that error, whose message says so, and its saved
    /// cursor goes with the call's fake client. To run such a job to the end, as workers do,
    /// push it to an inline testing client
    /// ([`Testing::inline`](crate::testing::Testing::inline)), which runs it again until it
    /// finishes.
    fn perform_inline(
        &self,
        args: Self::Args,
    ) -> impl Future<Output = Result<(), JobError>> + Send {
        async move { JobSetter::<Self>::new().perform_inline(self, args).await }
    }
}

impl<J: Job> JobExt for J {}

/// When a typed push should run.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Schedule {
    /// Absolute time, epoch seconds.
    At(f64),
    /// Seconds from now; values of 1e9 or more are absolute timestamps (Sidekiq semantics).
    In(f64),
}

/// Intervals at least this large are absolute Unix timestamps (Sidekiq's `perform_in`).
const TIMESTAMP_THRESHOLD_SECS: f64 = 1_000_000_000.0;

impl Schedule {
    /// The `at` value for a push made at `now`: `None` when the job should run immediately.
    fn resolve(self, now: f64) -> Option<f64> {
        let at = match self {
            Self::At(at) => at,
            Self::In(interval) if interval < TIMESTAMP_THRESHOLD_SECS => now + interval,
            Self::In(timestamp) => timestamp,
        };
        (at > now).then_some(at)
    }
}

/// Per-push options for a typed job (Sidekiq's `Job.set(...)`), created by [`JobExt::set`]
/// or [`Client::job`]. A setter can be reused for many pushes; each push gets a fresh jid
/// unless one was set explicitly.
pub struct JobSetter<J: Job> {
    options: JobOptions,
    jid: Option<String>,
    schedule: Option<Schedule>,
    batch_size: Option<usize>,
    spread_interval: Option<Duration>,
    _job: PhantomData<fn() -> J>,
}

impl<J: Job> JobSetter<J> {
    /// A setter with no options.
    pub fn new() -> Self {
        Self {
            options: JobOptions::new(),
            jid: None,
            schedule: None,
            batch_size: None,
            spread_interval: None,
            _job: PhantomData,
        }
    }

    /// Pushes to this queue.
    #[must_use]
    pub fn queue(mut self, queue: impl Into<String>) -> Self {
        self.options.queue = Some(queue.into());
        self
    }

    /// Sets the retry policy: `true`, `false` or a maximum number of retries.
    #[must_use]
    pub fn retry(mut self, retry: impl Into<RetryOption>) -> Self {
        self.options.retry = Some(retry.into());
        self
    }

    /// Sets whether to store error backtraces: `true`, `false` or a number of lines.
    #[must_use]
    pub fn backtrace(mut self, backtrace: impl Into<BacktraceOption>) -> Self {
        self.options.backtrace = Some(backtrace.into());
        self
    }

    /// `false` keeps the job out of the morgue when its retries are exhausted.
    #[must_use]
    pub fn dead(mut self, dead: bool) -> Self {
        self.options.dead = Some(dead);
        self
    }

    /// Sets the tags.
    #[must_use]
    pub fn tags<I, S>(mut self, tags: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.options.tags = Some(tags.into_iter().map(Into::into).collect());
        self
    }

    /// Retries for this long after the first failure instead of a number of times.
    #[must_use]
    pub fn retry_for(mut self, retry_for: Duration) -> Self {
        self.options.retry_for = Some(retry_for);
        self
    }

    /// Sends retries to this queue.
    #[must_use]
    pub fn retry_queue(mut self, queue: impl Into<String>) -> Self {
        self.options.retry_queue = Some(queue.into());
        self
    }

    /// Adds a custom payload key.
    #[must_use]
    pub fn custom(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.options = self.options.custom(key, value);
        self
    }

    /// Sets the log level of this execution, see [`JobOptions::log_level`].
    #[must_use]
    pub fn log_level(mut self, level: tracing::Level) -> Self {
        self.options = self.options.log_level(level);
        self
    }

    /// Overlays `options` on the options set so far.
    #[must_use]
    pub fn options(mut self, options: &JobOptions) -> Self {
        self.options = self.options.merge(options);
        self
    }

    /// Uses this jid instead of generating one. Only valid for pushes of a single job.
    #[must_use]
    pub fn jid(mut self, jid: impl Into<String>) -> Self {
        self.jid = Some(jid.into());
        self
    }

    /// Schedules the job at `at` (a time in the past pushes immediately).
    #[must_use]
    pub fn at(mut self, at: SystemTime) -> Self {
        self.schedule = Some(Schedule::At(system_time_to_secs(at)));
        self
    }

    /// Schedules the job `delay` from the time of the push (see [`JobExt::perform_in`]).
    #[must_use]
    pub fn delay(mut self, delay: Duration) -> Self {
        self.schedule = Some(Schedule::In(delay.as_secs_f64()));
        self
    }

    /// Jobs per round trip for [`perform_bulk`](Self::perform_bulk) (default 1000, or 100
    /// when scheduled).
    #[must_use]
    pub fn batch_size(mut self, size: usize) -> Self {
        self.batch_size = Some(size);
        self
    }

    /// Makes [`perform_bulk`](Self::perform_bulk) schedule each job at a random time within
    /// `interval` from now (at least 5 s), so that a large fan-out does not run all at once
    /// (Sidekiq's `perform_bulk(args, spread_interval:)`). Not with [`at`](Self::at) or
    /// [`delay`](Self::delay).
    #[must_use]
    pub fn spread_interval(mut self, interval: Duration) -> Self {
        self.spread_interval = Some(interval);
        self
    }

    /// Sets the whole uniqueness option (see [`JobOptions::unique`]).
    #[must_use]
    pub fn unique(mut self, unique: impl Into<UniqueOption>) -> Self {
        self.options = self.options.unique(unique);
        self
    }

    /// Makes the job unique for `ttl` (see [`JobOptions::unique_for`]).
    #[must_use]
    pub fn unique_for(mut self, ttl: Duration) -> Self {
        self.options = self.options.unique_for(ttl);
        self
    }

    /// When the job's lock is released (see [`JobOptions::unique_until`]).
    #[must_use]
    pub fn unique_until(mut self, until: UniqueUntil) -> Self {
        self.options = self.options.unique_until(until);
        self
    }

    /// What the push does when another job holds the lock (see [`JobOptions::on_conflict`]).
    #[must_use]
    pub fn on_conflict(mut self, on_conflict: OnConflict) -> Self {
        self.options = self.options.on_conflict(on_conflict);
        self
    }

    /// Whether the lock ignores the queue (see [`JobOptions::unique_across_queues`]).
    #[must_use]
    pub fn unique_across_queues(mut self, across: bool) -> Self {
        self.options = self.options.unique_across_queues(across);
        self
    }

    /// The value identifying duplicates (see [`JobOptions::unique_key`]); wins over
    /// [`Job::unique_key`].
    #[must_use]
    pub fn unique_key(mut self, key: impl Into<Value>) -> Self {
        self.options = self.options.unique_key(key);
        self
    }

    /// Not unique, whatever the job type's options say.
    #[doc(alias = "no_unique")]
    #[must_use]
    pub fn not_unique(mut self) -> Self {
        self.options = self.options.not_unique();
        self
    }

    /// The throttle of the job's starts (see [`JobOptions::throttle`]).
    #[must_use]
    pub fn throttle(mut self, name: impl Into<String>) -> Self {
        self.options = self.options.throttle(name);
        self
    }

    /// The throttle key (see [`JobOptions::throttle_key`]); wins over [`Job::throttle_key`].
    #[must_use]
    pub fn throttle_key(mut self, key: impl Into<String>) -> Self {
        self.options = self.options.throttle_key(key);
        self
    }

    /// Not throttled, whatever the job type's options say.
    #[doc(alias = "not_throttled")]
    #[must_use]
    pub fn no_throttle(mut self) -> Self {
        self.options = self.options.no_throttle();
        self
    }

    /// The explicit options set so far.
    pub fn explicit_options(&self) -> &JobOptions {
        &self.options
    }

    /// Pushes the job (scheduled if `at`/`delay` was set). Returns its jid, or `None` if
    /// client middleware stopped the push. An error after a timeout may come with the job
    /// pushed all the same: see [when a push fails](Client#when-a-push-fails) (the same holds
    /// for the other pushes).
    pub async fn perform_async(&self, client: &Client, args: J::Args) -> Result<Option<String>> {
        let values = args_to_values(J::NAME, &args)?;
        let mut payload = self.payload(values, now_secs_f64())?;
        self.typed_keys(client, &args).apply(&mut payload);
        client.push_typed(payload, &J::options()).await
    }

    /// Pushes the job like [`perform_async`](Self::perform_async) and says what happened: pushed,
    /// a duplicate of a unique job (with the jid holding the lock), a replacement, or stopped by
    /// client middleware. A duplicate of a job with [`OnConflict::Error`] is the error
    /// [`Error::DuplicateJob`](crate::Error::DuplicateJob) here too, not
    /// [`PushOutcome::Duplicate`].
    pub async fn push_outcome(&self, client: &Client, args: J::Args) -> Result<PushOutcome> {
        let values = args_to_values(J::NAME, &args)?;
        let mut payload = self.payload(values, now_secs_f64())?;
        self.typed_keys(client, &args).apply(&mut payload);
        client.push_typed_outcome(payload, &J::options()).await
    }

    /// Pushes one job per element of `args` like [`perform_bulk`](Self::perform_bulk), with
    /// the outcome of each.
    pub async fn push_bulk_outcomes(
        &self,
        client: &Client,
        args: Vec<J::Args>,
    ) -> Result<Vec<PushOutcome>> {
        let bulk = self.bulk(client, &args)?;
        client.push_bulk_typed_outcomes(bulk, &J::options()).await
    }

    /// The keys [`Job::unique_key`] and [`Job::throttle_key`] give `args`, when the merged
    /// options make the job unique or throttled without an explicit key: the client's
    /// `default_job_options`, under the job type's, under this setter's, as the push's
    /// normalization merges them (and as `UniqueLocks::find` computes a digest), so that a job
    /// type made unique by the client's defaults alone still gets its digest from its
    /// `unique_key`.
    fn typed_keys(&self, client: &Client, args: &J::Args) -> TypedKeys {
        let merged = client
            .default_job_options()
            .merge(&J::options())
            .merge(&self.options);
        let unique = match &merged.unique {
            Some(UniqueOption::Enabled(unique)) if unique.key.is_none() => J::unique_key(args),
            _ => None,
        };
        let throttle = match &merged.throttle {
            Some(ThrottleOption::Enabled { key: None, .. }) => J::throttle_key(args),
            _ => None,
        };
        TypedKeys { unique, throttle }
    }

    /// The bulk push of `args` through `client`, with the per-job typed keys.
    fn bulk(&self, client: &Client, args: &[J::Args]) -> Result<BulkPush> {
        let values = args
            .iter()
            .map(|args| args_to_values(J::NAME, args))
            .collect::<Result<Vec<_>>>()?;
        let template = self.payload(Vec::new(), now_secs_f64())?;
        let mut bulk = BulkPush::from_template(template, values);
        if let Some(at) = bulk.template.at.take() {
            bulk.at = Some(BulkAt::All(at));
        }
        bulk.spread_interval = self.spread_interval;
        bulk.batch_size = self.batch_size;
        let keys: Vec<TypedKeys> = args
            .iter()
            .map(|args| self.typed_keys(client, args))
            .collect();
        if keys.iter().any(|keys| !keys.is_empty()) {
            bulk.keys = keys;
        }
        Ok(bulk)
    }

    /// Pushes the job to run after `delay` (overriding any `at`/`delay` set before).
    pub async fn perform_in(
        &self,
        client: &Client,
        delay: Duration,
        args: J::Args,
    ) -> Result<Option<String>> {
        self.clone().delay(delay).perform_async(client, args).await
    }

    /// Pushes the job to run at `at` (overriding any `at`/`delay` set before).
    pub async fn perform_at(
        &self,
        client: &Client,
        at: SystemTime,
        args: J::Args,
    ) -> Result<Option<String>> {
        self.clone().at(at).perform_async(client, args).await
    }

    /// Pushes one job per element of `args` (see [`Client::push_bulk`]). With `at`/`delay`
    /// set, every job is scheduled at the same time; with
    /// [`spread_interval`](Self::spread_interval), at random times within it.
    pub async fn perform_bulk(
        &self,
        client: &Client,
        args: Vec<J::Args>,
    ) -> Result<Vec<Option<String>>> {
        let bulk = self.bulk(client, &args)?;
        client.push_bulk_typed(bulk, &J::options()).await
    }

    /// Pushes one job per `(args, time)` pair, each scheduled at its own time (a time that is
    /// not in the future pushes that job immediately), in as few round trips as possible
    /// (Sidekiq's `perform_bulk(args, at: [...])`). The job type's options apply as for every
    /// typed push. Not with [`at`](Self::at), [`delay`](Self::delay) or
    /// [`spread_interval`](Self::spread_interval): each job has its time.
    pub async fn perform_bulk_at(
        &self,
        client: &Client,
        jobs: Vec<(J::Args, SystemTime)>,
    ) -> Result<Vec<Option<String>>> {
        if self.schedule.is_some() || self.spread_interval.is_some() {
            return Err(Error::InvalidJob(
                "perform_bulk_at schedules each job at its own time: do not also set at, delay \
                 or spread_interval"
                    .to_owned(),
            ));
        }
        let mut values = Vec::with_capacity(jobs.len());
        let mut times = Vec::with_capacity(jobs.len());
        let mut keys = Vec::with_capacity(jobs.len());
        for (args, at) in &jobs {
            values.push(args_to_values(J::NAME, args)?);
            times.push(system_time_to_secs(*at));
            keys.push(self.typed_keys(client, args));
        }
        let template = self.payload(Vec::new(), now_secs_f64())?;
        let mut bulk = BulkPush::from_template(template, values);
        bulk.at = Some(BulkAt::Each(times));
        bulk.batch_size = self.batch_size;
        if keys.iter().any(|keys| !keys.is_empty()) {
            bulk.keys = keys;
        }
        client.push_bulk_typed(bulk, &J::options()).await
    }

    /// Runs `job` in this process, see [`JobExt::perform_inline`]. The payload the job sees
    /// reflects this setter's options.
    pub async fn perform_inline(&self, job: &J, args: J::Args) -> Result<(), JobError> {
        let client = Client::fake();
        let values = args_to_values(J::NAME, &args)?;
        let payload = self.payload(values, now_secs_f64())?;
        let payload = client.normalize(payload, Some(&J::options()))?;
        let payload = JobPayload::from_json(&payload.to_json()?)?;
        let args = payload.args.clone();
        let ctx = JobContext::new(payload, client);
        match catch_panic(job.perform_json(&ctx, args)).await {
            // The interruption's own message ("the job will continue from where it stopped")
            // is not true here: nothing runs it again.
            Err(error) if error.is_interrupted() => Err(JobError::new(
                JobError::INTERRUPTED,
                "interrupted: perform_inline runs one execution, and the job asked to continue \
                 later (an iterable job whose run time is up); run it through an inline testing \
                 client (`Testing::new().register(job).inline()`), which runs it to the end",
            )),
            result => result,
        }
    }

    /// Builds the payload for a push made at `now` (explicit options only; the client adds
    /// the job type's options and the defaults).
    fn payload(&self, args: Vec<Value>, now: f64) -> Result<JobPayload> {
        if self.options.queue.as_deref() == Some("") {
            return Err(Error::InvalidJob(
                "Job must include a valid queue name".to_owned(),
            ));
        }
        let mut payload = JobPayload::new(J::NAME, args);
        self.options.apply_defaults(&mut payload);
        if let Some(jid) = &self.jid {
            payload.jid.clone_from(jid);
        }
        payload.at = self.schedule.and_then(|schedule| schedule.resolve(now));
        Ok(payload)
    }
}

impl<J: Job> Default for JobSetter<J> {
    fn default() -> Self {
        Self::new()
    }
}

impl<J: Job> Clone for JobSetter<J> {
    fn clone(&self) -> Self {
        Self {
            options: self.options.clone(),
            jid: self.jid.clone(),
            schedule: self.schedule,
            batch_size: self.batch_size,
            spread_interval: self.spread_interval,
            _job: PhantomData,
        }
    }
}

impl<J: Job> fmt::Debug for JobSetter<J> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JobSetter")
            .field("job", &J::NAME)
            .field("options", &self.options)
            .field("jid", &self.jid)
            .field("schedule", &self.schedule)
            .field("batch_size", &self.batch_size)
            .field("spread_interval", &self.spread_interval)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use serde_json::json;

    #[derive(Default)]
    struct Noop;

    impl Job for Noop {
        const NAME: &'static str = "Noop";
        type Args = ();

        async fn perform(&self, _ctx: &JobContext, (): ()) -> Result<(), JobError> {
            Ok(())
        }
    }

    struct Named(&'static str);

    impl Job for Named {
        const NAME: &'static str = "Named";
        type Args = (String,);

        async fn perform(&self, _ctx: &JobContext, (value,): (String,)) -> Result<(), JobError> {
            if value == self.0 {
                Ok(())
            } else {
                Err(JobError::msg(format!("expected {}, got {value}", self.0)))
            }
        }
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Unit;

    #[test]
    fn args_round_trip() {
        assert_eq!(args_to_values("J", &()).unwrap(), Vec::<Value>::new());
        args_from_values::<()>(vec![]).unwrap();
        assert_eq!(args_to_values("J", &Unit).unwrap(), Vec::<Value>::new());
        assert_eq!(args_from_values::<Unit>(vec![]).unwrap(), Unit);
        assert_eq!(
            args_to_values("J", &(1, "a".to_owned())).unwrap(),
            vec![json!(1), json!("a")]
        );
        assert_eq!(
            args_from_values::<(i32, String)>(vec![json!(1), json!("a")]).unwrap(),
            (1, "a".to_owned())
        );
        assert_eq!(
            args_from_values::<Vec<i32>>(vec![]).unwrap(),
            Vec::<i32>::new()
        );
        assert_eq!(
            args_to_values("J", &vec![1, 2]).unwrap(),
            vec![json!(1), json!(2)]
        );
        assert!(args_from_values::<(i32,)>(vec![]).is_err());
        assert!(args_from_values::<(i32,)>(vec![json!("x")]).is_err());
    }

    #[test]
    fn non_array_args_are_rejected() {
        #[derive(Serialize, Deserialize)]
        struct Object {
            a: i32,
        }
        let error = args_to_values("J", &Object { a: 1 }).unwrap_err();
        assert!(
            matches!(error, Error::InvalidJob(ref m) if m.contains("an object")),
            "{error}"
        );
        assert!(matches!(args_to_values("J", &5), Err(Error::InvalidJob(_))));
        // Wrapping in a 1-tuple works.
        assert_eq!(
            args_to_values("J", &(Object { a: 1 },)).unwrap(),
            vec![json!({"a": 1})]
        );
    }

    #[test]
    fn non_finite_floats_are_rejected() {
        let error = args_to_values("J", &(f64::NAN,)).unwrap_err();
        assert!(
            matches!(error, Error::InvalidJob(ref m) if m.contains("`J`")),
            "{error}"
        );
        assert!(matches!(
            args_to_values("J", &(f64::INFINITY,)),
            Err(Error::InvalidJob(_))
        ));
        assert_eq!(args_to_values("J", &(1.5,)).unwrap(), vec![json!(1.5)]);
    }

    #[test]
    fn non_string_map_keys_fail_to_serialize() {
        let mut map = std::collections::BTreeMap::new();
        map.insert((1, 2), "tuple key".to_owned());
        assert!(matches!(args_to_values("J", &(map,)), Err(Error::Json(_))));
    }

    #[test]
    fn data_type_map() {
        let mut data = Data::new();
        assert!(data.is_empty());
        assert!(data.insert(5_u32).is_none());
        assert_eq!(data.insert(6_u32).as_deref(), Some(&5));
        data.insert_arc(Arc::new("text".to_owned()));
        assert_eq!(data.get::<u32>(), Some(&6));
        assert_eq!(data.get::<String>().map(String::as_str), Some("text"));
        assert_eq!(data.get::<i64>(), None);
        assert_eq!(data.get_arc::<u32>().as_deref(), Some(&6));
        assert!(data.contains::<String>());
        assert!(!data.contains::<i64>());
        assert_eq!(data.len(), 2);
        assert_eq!(format!("{data:?}"), "Data { len: 2 }");
    }

    #[test]
    fn context_accessors() {
        let mut payload = JobPayload::new("Noop", vec![]);
        payload.jid = "abc".into();
        payload.queue = "q".into();
        payload.retry_count = Some(2);
        payload.tags = vec!["t".into()];
        payload.bid = Some("b".into());
        let mut data = Data::new();
        data.insert(41_u64);
        let token = CancellationToken::new();
        let ctx = JobContext::new(payload, Client::fake())
            .with_data(Arc::new(data))
            .with_stopping(token.clone());
        assert_eq!(ctx.jid(), "abc");
        assert_eq!(ctx.class(), "Noop");
        assert_eq!(ctx.queue(), "q");
        assert_eq!(ctx.retry_count(), Some(2));
        assert_eq!(ctx.tags(), ["t"]);
        assert_eq!(ctx.bid(), Some("b"));
        assert_eq!(ctx.data::<u64>().copied().unwrap(), 41);
        assert_eq!(*ctx.data_arc::<u64>().unwrap(), 41);
        let missing = ctx.data::<String>().unwrap_err();
        assert_eq!(missing.class(), JobError::MISSING_DATA);
        assert!(missing.message().contains("String"));
        assert!(!ctx.is_stopping());
        token.cancel();
        assert!(ctx.is_stopping());
        assert!(ctx.client().is_fake());
        let moved = ctx
            .clone()
            .with_queue("other")
            .with_payload(JobPayload::new("X", vec![]));
        assert_eq!((moved.queue(), moved.class()), ("other", "X"));
        assert!(format!("{ctx:?}").contains("abc"));
    }

    #[tokio::test]
    async fn stopping_future_completes_on_cancel() {
        let token = CancellationToken::new();
        let ctx = JobContext::new(JobPayload::new("Noop", vec![]), Client::fake())
            .with_stopping(token.clone());
        let waiter = tokio::spawn(async move { ctx.stopping().await });
        token.cancel();
        tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .unwrap()
            .unwrap();
    }

    #[test]
    fn registry_detects_duplicates() {
        let mut registry = Registry::new();
        registry.register_default::<Noop>().unwrap();
        registry.register(Named("x")).unwrap();
        let error = registry.register(Noop).map(|_| ()).unwrap_err();
        assert!(
            matches!(error, Error::Config(ref m) if m.contains("Noop")),
            "{error}"
        );
        assert_eq!(registry.names(), ["Named", "Noop"]);
        assert!(registry.contains("Noop"));
        assert_eq!(registry.get("Named").map(|job| job.name()), Some("Named"));
        assert!(registry.get("Missing").is_none());
        assert_eq!(registry.len(), 2);
        assert!(format!("{registry:?}").contains("Named"));

        let mut other = Registry::new();
        other.register_default::<Noop>().unwrap();
        assert!(registry.merge(other).is_err());
    }

    #[test]
    fn registry_rejects_empty_names() {
        struct Nameless;
        impl Job for Nameless {
            const NAME: &'static str = "";
            type Args = ();
            async fn perform(&self, _: &JobContext, (): ()) -> Result<(), JobError> {
                Ok(())
            }
        }
        assert!(matches!(
            Registry::new().register(Nameless).map(|_| ()),
            Err(Error::Config(_))
        ));
    }

    #[test]
    fn registrations_instantiate_jobs() {
        const REGISTRATION: JobRegistration = JobRegistration::of::<Noop>();
        assert_eq!(REGISTRATION.name(), "Noop");
        assert_eq!(REGISTRATION.instantiate().name(), "Noop");
        let custom = JobRegistration::new("Named", || Arc::new(Named("x")));
        assert_eq!(custom.instantiate().name(), "Named");
    }

    #[tokio::test]
    async fn erased_jobs_deserialize_arguments() {
        let job: Arc<dyn ErasedJob> = Arc::new(Named("ok"));
        let ctx = JobContext::new(JobPayload::new("Named", vec![]), Client::fake());
        job.perform_json(&ctx, vec![json!("ok")]).await.unwrap();
        let failed = job
            .perform_json(&ctx, vec![json!("nope")])
            .await
            .unwrap_err();
        assert_eq!(failed.class(), JobError::MESSAGE);
        let invalid = job.perform_json(&ctx, vec![json!(1)]).await.unwrap_err();
        assert_eq!(invalid.class(), JobError::INVALID_ARGS);
        assert!(job.job_options().queue.is_none());
        assert_eq!(
            job.dyn_retry_in(1, &failed, ctx.payload()),
            RetryIn::Default
        );
        assert_eq!(
            job.dyn_retries_exhausted(ctx.payload(), &failed).await,
            Exhausted::Default
        );
    }

    #[tokio::test]
    async fn catch_panic_converts_panics() {
        let result = catch_panic(async { panic!("kaboom") }).await;
        let error = result.unwrap_err();
        assert!(error.is_panic());
        assert_eq!(error.message(), "kaboom");
        catch_panic(async { Ok(()) }).await.unwrap();
    }

    #[test]
    fn schedules_resolve_like_sidekiq() {
        let now = 1_700_000_000.0;
        assert_eq!(Schedule::In(10.0).resolve(now), Some(now + 10.0));
        assert_eq!(Schedule::In(0.0).resolve(now), None);
        assert_eq!(
            Schedule::In(2_000_000_000.0).resolve(now),
            Some(2_000_000_000.0)
        );
        assert_eq!(Schedule::In(1_000_000_000.0).resolve(now), None);
        assert_eq!(Schedule::At(now + 1.0).resolve(now), Some(now + 1.0));
        assert_eq!(Schedule::At(now - 1.0).resolve(now), None);
        assert_eq!(Schedule::At(now).resolve(now), None);
    }

    #[test]
    fn setters_build_payloads() {
        let setter = Noop::set()
            .queue("q")
            .retry(3)
            .backtrace(true)
            .dead(false)
            .tags(["a"])
            .retry_for(Duration::from_secs(60))
            .retry_queue("rq")
            .custom("locale", "fr")
            .jid("0123456789abcdef01234567")
            .delay(Duration::from_secs(30));
        let payload = setter.payload(vec![], 100.0).unwrap();
        assert_eq!(payload.class, "Noop");
        assert_eq!(payload.queue, "q");
        assert_eq!(payload.retry, Some(RetryOption::Count(3)));
        assert_eq!(payload.backtrace, Some(BacktraceOption::Bool(true)));
        assert_eq!(payload.dead, Some(false));
        assert_eq!(payload.tags, ["a"]);
        assert_eq!(payload.retry_for, Some(60));
        assert_eq!(payload.retry_queue.as_deref(), Some("rq"));
        assert_eq!(payload.extra["locale"], json!("fr"));
        assert_eq!(payload.jid, "0123456789abcdef01234567");
        assert_eq!(payload.at, Some(130.0));
        assert!(format!("{setter:?}").contains("Noop"));

        let error = Noop::set().queue("").payload(vec![], 0.0).unwrap_err();
        assert!(matches!(error, Error::InvalidJob(_)));

        let merged = Noop::set()
            .queue("a")
            .options(&JobOptions::new().queue("b").retry(false));
        assert_eq!(merged.explicit_options().queue.as_deref(), Some("b"));
    }

    #[tokio::test]
    async fn perform_inline_runs_the_job() {
        Named("hi")
            .perform_inline(("hi".to_owned(),))
            .await
            .unwrap();
        let error = Named("hi")
            .perform_inline(("bye".to_owned(),))
            .await
            .unwrap_err();
        assert_eq!(error.message(), "expected hi, got bye");
        Noop.perform_inline(()).await.unwrap();
    }

    #[tokio::test]
    async fn perform_inline_catches_panics() {
        struct Panics;
        impl Job for Panics {
            const NAME: &'static str = "Panics";
            type Args = (u8,);
            async fn perform(&self, _: &JobContext, (n,): (u8,)) -> Result<(), JobError> {
                panic!("job panicked with {n}");
            }
        }
        let error = Panics.perform_inline((7,)).await.unwrap_err();
        assert!(error.is_panic());
        assert_eq!(error.message(), "job panicked with 7");
    }

    #[tokio::test]
    async fn perform_inline_validates_like_a_push() {
        let error = Noop::set()
            .queue("")
            .perform_inline(&Noop, ())
            .await
            .unwrap_err();
        assert!(error.message().contains("queue"), "{error:?}");
    }
}
