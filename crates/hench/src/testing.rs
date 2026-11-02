//! Running jobs in tests without Redis (Sidekiq's `Sidekiq::Testing`).
//!
//! Sidekiq switches a global testing mode; hench attaches the mode to a [`Client`] instead,
//! so tests stay independent and can run in parallel:
//!
//! * A **fake** client ([`Client::fake`], [`Testing::fake`]) captures pushed jobs in memory.
//!   Tests assert on them ([`Client::jobs`], [`Client::jobs_for`], [`Client::jobs_in_queue`])
//!   and run them on demand ([`Client::drain`], [`Client::perform_one`],
//!   [`Client::drain_all`]). Jobs pushed while draining (for example by the jobs being run)
//!   are captured too and drained in the same call.
//! * An **inline** client ([`Client::inline`], [`Testing::inline`]) runs every pushed job
//!   immediately, in process; a failing job fails the push with [`Error::Job`].
//!
//! Jobs make a JSON round trip exactly as they would through Redis, run through the testing
//! server middleware and see a [`JobContext`] whose client is the testing client itself, so
//! jobs they enqueue stay in the same mode. Jobs are looked up by name in a [`Registry`],
//! like in a worker: every `#[hench::job]` function is registered automatically; register
//! other jobs with [`Testing::register`].
//!
//! ```
//! use hench::prelude::*;
//!
//! /// Sends the welcome email.
//! #[hench::job(queue = "mailers")]
//! async fn welcome(ctx: &JobContext, user_id: u64) -> Result<(), JobError> {
//!     tracing::info!(jid = ctx.jid(), user_id, "welcome!");
//!     Ok(())
//! }
//!
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! let client = Client::fake();
//! welcome::perform_async(&client, (42,)).await?;
//!
//! let jobs = client.jobs::<welcome>();
//! assert_eq!(jobs.len(), 1);
//! assert_eq!(jobs[0].queue, "mailers");
//! assert_eq!(jobs[0].args, [serde_json::json!(42)]);
//!
//! assert_eq!(client.drain::<welcome>().await?, 1);
//! assert!(client.jobs::<welcome>().is_empty());
//! # Ok::<(), hench::Error>(()) }).unwrap();
//! ```
//!
//! Jobs implemented by hand are registered explicitly, together with the data and middleware
//! they need:
//!
//! ```
//! use hench::prelude::*;
//! use hench::testing::Testing;
//! use std::sync::atomic::{AtomicU64, Ordering};
//!
//! #[derive(Default)]
//! struct Credit;
//!
//! impl Job for Credit {
//!     const NAME: &'static str = "Credit";
//!     type Args = (u64,);
//!
//!     async fn perform(&self, ctx: &JobContext, (amount,): (u64,)) -> Result<(), JobError> {
//!         ctx.data::<AtomicU64>()?.fetch_add(amount, Ordering::SeqCst);
//!         Ok(())
//!     }
//! }
//!
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! let balance = std::sync::Arc::new(AtomicU64::new(0));
//! let client = Testing::new().register(Credit).data_arc(balance.clone()).inline();
//! Credit::perform_async(&client, (25,)).await?; // runs right away
//! assert_eq!(balance.load(Ordering::SeqCst), 25);
//! # Ok::<(), hench::Error>(()) }).unwrap();
//! ```

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::Duration;

use tracing::Instrument;

use crate::batch::BatchDefaults;
use crate::client::{Client, ClientMode};
use crate::config::Config;
use crate::config::{Hench, HenchBuilder};
use crate::error::{Error, JobError, Result};
use crate::job::{Data, ErasedJob, Job, JobContext, JobRegistration, Registry};
use crate::job_logger::job_span;
use crate::limit::memory::Parked;
use crate::limit::{OverLimit, OverLimitDecision, Reschedule, Throttle};
use crate::middleware::{
    ClientChain, ClientMiddleware, ServerChain, ServerMiddleware, execute_job,
};
use crate::options::JobOptions;
use crate::payload::{JobPayload, now_millis, now_secs_f64};
use crate::pro::{Classified, Fetched, Gate};
use crate::redis::RedisPool;
use crate::retry::Verdict;

/// Configuration of the testing clients: which jobs they can run, the server middleware
/// and data jobs see, and the client middleware and default options of pushes.
///
/// [`Testing::fake`] and [`Testing::inline`] create clients from it. [`Client::fake`] is
/// `Testing::new().fake()` and [`Client::inline`] uses exactly the given registry.
///
/// The jobs come from:
///
/// 1. jobs registered with [`register`](Testing::register) (and friends), which take
///    precedence, so a test can replace a job with a stub of the same name;
/// 2. the registry given to [`registry`](Testing::registry), or else every `#[hench::job]`
///    function (`inventory`), unless [`auto_register(false)`](Testing::auto_register).
///
/// An invalid configuration (for example two different inventory jobs with the same name)
/// is reported by the first job execution, as [`Error::Config`].
///
/// ```
/// use hench::prelude::*;
/// use hench::testing::Testing;
///
/// struct Db { url: String }
///
/// let client = Testing::new()
///     .data(Db { url: "postgres://test".into() })
///     .fake();
/// assert!(client.is_fake());
/// ```
#[derive(Clone)]
pub struct Testing {
    jobs: Vec<Arc<dyn ErasedJob>>,
    registry: Option<Registry>,
    auto_register: bool,
    data: Data,
    server_middleware: ServerChain,
    client_middleware: ClientChain,
    default_job_options: JobOptions,
    max_iteration_runtime: Option<Duration>,
    unique_jobs: bool,
    limits: TestLimits,
    throttles: Vec<Throttle>,
    batch_defaults: BatchDefaults,
    /// The over-limit settings (`over_limit_when`, the backoff, the reschedule budget) and the
    /// error handlers they report to: the program's with [`Testing::from_hench`], else the
    /// defaults.
    config: Arc<Config>,
}

/// How the testing clients apply rate limits ([`Testing::limits`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum TestLimits {
    /// Every limiter and throttle grants at once (the default: limits depend on time, which
    /// tests rarely want).
    #[default]
    Unlimited,
    /// Limits are enforced in memory, on tokio's clock (pause it to step through windows): a
    /// fake client's job refused by its throttle (or returning a limiter's refusal) stays
    /// captured, parked at its slot with `at` and `limited` set, and is not run again by the
    /// drain that parked it; an inline client returns the refusal as a failure.
    InMemory,
}

/// The feature tables of the testing modes (unique locks, batches, limits), shared by a testing
/// client, its clones and its collecting clients.
#[derive(Debug, Default)]
pub(crate) struct Tables {
    /// In-memory unique locks.
    pub(crate) unique: crate::unique::fake::UniqueTable,
    /// In-memory batches.
    pub(crate) batches: crate::batch::model::BatchTable,
    /// In-memory limiter state.
    pub(crate) limits: crate::limit::memory::LimitTable,
}

impl Testing {
    /// Every `#[hench::job]` function, no middleware, no data and the built-in default job
    /// options (queue `default`, `retry: true`).
    pub fn new() -> Self {
        Self {
            jobs: Vec::new(),
            registry: None,
            auto_register: true,
            data: Data::new(),
            server_middleware: ServerChain::new(),
            client_middleware: ClientChain::new(),
            default_job_options: JobOptions::defaults(),
            max_iteration_runtime: None,
            unique_jobs: true,
            limits: TestLimits::default(),
            throttles: Vec::new(),
            batch_defaults: BatchDefaults::default(),
            config: Arc::new(Config::default()),
        }
    }

    /// The jobs, data, middleware and default job options of a configured [`Hench`], to run
    /// its jobs in tests as its workers would: the testing clients never use Redis.
    ///
    /// Getting the `Hench` does: [`HenchBuilder::build`] connects to Redis (unless given a
    /// pool such as [`RedisPool::detached`]). For tests that run where no Redis is reachable,
    /// start from the builder with [`Testing::from_builder`].
    pub fn from_hench(hench: &Hench) -> Self {
        let config = hench.config();
        Self {
            jobs: Vec::new(),
            registry: Some(Registry::clone(hench.registry())),
            auto_register: false,
            data: Data::clone(&config.data),
            server_middleware: config.server_middleware.clone(),
            client_middleware: config.client_middleware.clone(),
            default_job_options: config.default_job_options.clone(),
            max_iteration_runtime: config.max_iteration_runtime,
            unique_jobs: config.unique_jobs,
            // The limits mode is the test's choice, not the program's.
            limits: TestLimits::default(),
            throttles: config.throttles.clone(),
            batch_defaults: config.batch_defaults(),
            config: Arc::clone(config),
        }
    }

    /// [`Testing::from_hench`] for the `Hench` that `builder` would build, without connecting
    /// to Redis (the builder's Redis settings are ignored): for tests that share the program's
    /// builder (`fn configure() -> HenchBuilder`) and run without Redis.
    ///
    /// ```
    /// use hench::HenchBuilder;
    /// use hench::prelude::*;
    /// use hench::testing::Testing;
    ///
    /// fn configure() -> HenchBuilder {
    ///     Hench::builder()
    ///         .redis_url("redis://redis.internal:6379")
    ///         .default_job_options(JobOptions::new().queue("app"))
    /// }
    ///
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let client = Testing::from_builder(configure()).await?.fake();
    /// assert!(client.is_fake());
    /// # Ok::<(), hench::Error>(()) }).unwrap();
    /// ```
    ///
    /// Fails like [`HenchBuilder::build`] when the configuration is invalid (two jobs with the
    /// same name, empty queue names, ...).
    pub async fn from_builder(builder: HenchBuilder) -> Result<Self> {
        let hench = builder.redis(RedisPool::detached()).build().await?;
        Ok(Self::from_hench(&hench))
    }

    /// Registers a job instance. It takes precedence over a job of the same name from the
    /// inventory or from [`registry`](Testing::registry).
    #[must_use]
    pub fn register<J: Job>(self, job: J) -> Self {
        self.register_dyn(Arc::new(job))
    }

    /// Registers `J::default()`, see [`register`](Testing::register).
    #[must_use]
    pub fn register_default<J: Job + Default>(self) -> Self {
        self.register(J::default())
    }

    /// Registers a type-erased job, see [`register`](Testing::register).
    #[must_use]
    pub fn register_dyn(mut self, job: Arc<dyn ErasedJob>) -> Self {
        self.jobs.push(job);
        self
    }

    /// Uses the jobs of `registry` instead of the `#[hench::job]` inventory (jobs added with
    /// [`register`](Testing::register) still take precedence).
    #[must_use]
    pub fn registry(mut self, registry: Registry) -> Self {
        self.registry = Some(registry);
        self
    }

    /// Whether every `#[hench::job]` function is available (default `true`). Ignored when a
    /// [`registry`](Testing::registry) is given.
    #[must_use]
    pub fn auto_register(mut self, enabled: bool) -> Self {
        self.auto_register = enabled;
        self
    }

    /// Makes `value` available to jobs through [`JobContext::data`].
    #[must_use]
    pub fn data<T: Send + Sync + 'static>(mut self, value: T) -> Self {
        self.data.insert(value);
        self
    }

    /// Makes a shared value available to jobs through [`JobContext::data`].
    #[must_use]
    pub fn data_arc<T: Send + Sync + 'static>(mut self, value: Arc<T>) -> Self {
        self.data.insert_arc(value);
        self
    }

    /// Adds server middleware run around every job execution (Sidekiq's
    /// `Sidekiq::Testing.server_middleware`).
    #[must_use]
    pub fn server_middleware(mut self, middleware: impl ServerMiddleware) -> Self {
        self.server_middleware.add(middleware);
        self
    }

    /// Modifies the server middleware chain.
    #[must_use]
    pub fn configure_server_middleware(mut self, configure: impl FnOnce(&mut ServerChain)) -> Self {
        configure(&mut self.server_middleware);
        self
    }

    /// Adds client middleware run on every push of the testing clients.
    #[must_use]
    pub fn client_middleware(mut self, middleware: impl ClientMiddleware) -> Self {
        self.client_middleware.add(middleware);
        self
    }

    /// Sets options applied to pushed jobs that do not set them, merged over the current
    /// defaults like [`HenchBuilder::default_job_options`](crate::HenchBuilder::default_job_options).
    #[must_use]
    pub fn default_job_options(mut self, options: JobOptions) -> Self {
        self.default_job_options = self.default_job_options.merge(&options);
        self
    }

    /// Limits how long [iterable jobs](crate::iterable) iterate per execution, like
    /// [`HenchBuilder::max_iteration_runtime`](crate::HenchBuilder::max_iteration_runtime):
    /// an interrupted job goes back to the end of its fake queue (or runs again right away on
    /// an inline client) and continues where it stopped.
    #[must_use]
    pub fn max_iteration_runtime(mut self, limit: Duration) -> Self {
        self.max_iteration_runtime = Some(limit);
        self
    }

    /// Whether the testing clients enforce unique jobs' locks in memory (default `true`, as
    /// pushes are deterministic; see [`crate::unique`]): a duplicate push returns `None`, and
    /// locks are released as workers release them.
    #[must_use]
    pub fn unique_jobs(mut self, enabled: bool) -> Self {
        self.unique_jobs = enabled;
        self
    }

    /// How the testing clients apply rate limits (default [`TestLimits::Unlimited`]).
    #[must_use]
    pub fn limits(mut self, limits: TestLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Defines a throttle for the testing clients, like
    /// [`HenchBuilder::throttle`](crate::HenchBuilder::throttle).
    #[must_use]
    pub fn throttle(mut self, throttle: Throttle) -> Self {
        self.throttles.push(throttle);
        self
    }

    /// A client that captures pushed jobs in memory (Sidekiq's fake mode); see the
    /// [module documentation](self). Clones of the client share the captured jobs.
    pub fn fake(&self) -> Client {
        self.client(ClientMode::Fake(FakeStore::new(self.executor())))
    }

    /// A client that runs every pushed job immediately, in process (Sidekiq's inline mode).
    /// A push fails with [`Error::Job`] if the job fails, [`Error::UnknownJob`] if no job of
    /// that name is registered.
    pub fn inline(&self) -> Client {
        self.client(ClientMode::Inline(InlineExecutor {
            executor: Arc::new(self.executor()),
        }))
    }

    fn client(&self, mode: ClientMode) -> Client {
        let client = Client::with_mode(RedisPool::detached(), mode)
            .with_middleware(|chain| *chain = self.client_middleware.clone())
            .with_default_job_options(self.default_job_options.clone())
            .with_unique_jobs(self.unique_jobs)
            .with_batch_defaults(self.batch_defaults);
        // The throttles the registered jobs define, then the configured ones (which win, as in
        // a worker). An invalid registry is reported by the first execution.
        let from_jobs: Vec<Throttle> = self
            .build_registry()
            .map(|registry| {
                registry
                    .names()
                    .iter()
                    .filter_map(|name| registry.get(name)?.dyn_throttle())
                    .collect()
            })
            .unwrap_or_default();
        client.tables().limits.configure(
            self.limits,
            from_jobs.into_iter().chain(self.throttles.iter().cloned()),
        );
        client
    }

    fn executor(&self) -> Executor {
        Executor {
            jobs: self.clone(),
            registry: OnceLock::new(),
            server_middleware: self.server_middleware.clone(),
            data: Arc::new(self.data.clone()),
            max_iteration_runtime: self.max_iteration_runtime,
        }
    }

    /// Explicit jobs first, then the base registry's jobs whose names are still free.
    ///
    /// An explicit job shadows every inventory job of its name, so a test can stub a name that
    /// the inventory has twice; other duplicate inventory names are an error, as in a worker.
    fn build_registry(&self) -> Result<Registry> {
        self.build_registry_with(::inventory::iter::<JobRegistration>)
    }

    /// [`Testing::build_registry`] with the given inventory.
    fn build_registry_with<'a>(
        &self,
        inventory: impl IntoIterator<Item = &'a JobRegistration>,
    ) -> Result<Registry> {
        let mut registry = Registry::new();
        for job in &self.jobs {
            registry.register_dyn(Arc::clone(job))?;
        }
        match &self.registry {
            Some(base) => {
                for name in base.names() {
                    if !registry.contains(name)
                        && let Some(job) = base.get(name)
                    {
                        registry.register_dyn(Arc::clone(job))?;
                    }
                }
            }
            None if self.auto_register => {
                let mut discovered = Registry::new();
                for registration in inventory {
                    if !registry.contains(registration.name()) {
                        discovered.register_dyn(registration.instantiate())?;
                    }
                }
                registry.merge(discovered)?;
            }
            None => {}
        }
        Ok(registry)
    }
}

impl Default for Testing {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Testing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Testing")
            .field(
                "jobs",
                &self.jobs.iter().map(|job| job.name()).collect::<Vec<_>>(),
            )
            .field("registry", &self.registry)
            .field("auto_register", &self.auto_register)
            .field("data", &self.data)
            .field("server_middleware", &self.server_middleware)
            .field("client_middleware", &self.client_middleware)
            .field("default_job_options", &self.default_job_options)
            .field("max_iteration_runtime", &self.max_iteration_runtime)
            .field("unique_jobs", &self.unique_jobs)
            .field("limits", &self.limits)
            .field("throttles", &self.throttles)
            .field("batch_defaults", &self.batch_defaults)
            .finish_non_exhaustive()
    }
}

/// Runs jobs in process for the testing clients.
struct Executor {
    /// Where the jobs come from.
    jobs: Testing,
    /// The jobs, or why the configuration is invalid, built when the first job runs (most
    /// fake clients, such as the one behind `perform_inline`, never run a job).
    registry: OnceLock<std::result::Result<Registry, String>>,
    server_middleware: ServerChain,
    data: Arc<Data>,
    max_iteration_runtime: Option<Duration>,
}

impl Executor {
    /// The job registered as `class`: [`Error::UnknownJob`] if there is none,
    /// [`Error::Config`] if the registry is invalid.
    fn job(&self, class: &str) -> Result<Arc<dyn ErasedJob>> {
        let registry = self
            .registry
            .get_or_init(|| {
                // The message without its `Display` prefix: it is wrapped again below.
                self.jobs.build_registry().map_err(|error| match error {
                    Error::Config(message) => message,
                    other => other.to_string(),
                })
            })
            .as_ref()
            .map_err(|message| Error::Config(message.clone()))?;
        registry
            .resolve(class)
            .ok_or_else(|| Error::UnknownJob(class.to_owned()))
    }

    /// Looks the job up and runs it, see [`Executor::perform_with`].
    async fn perform(&self, client: &Client, job: JobPayload) -> Result<Ran> {
        let erased = self.job(&job.class)?;
        self.perform_with(client, erased, job).await
    }

    /// Runs `job` through the server middleware, with `client` as the context's client, inside
    /// the same `job{class, jid}` tracing span as in a worker. A scheduled job is run as the
    /// scheduler would enqueue it: without `at`, with `enqueued_at` set.
    ///
    /// An interrupted job ([`JobError::interrupted`](crate::JobError::interrupted)) is pushed
    /// back through `client` (client middleware included), as a worker pushes it back to the
    /// end of its queue: a fake client captures it, an inline client runs it again at once. The
    /// inline re-runs happen in a loop here, not through [`Client::push`], whose inline delivery
    /// would run the job from within this call: every interruption would add a level of nesting,
    /// and an iterable job interrupted a few dozen times would overflow the stack.
    ///
    /// The features follow the processor's pipeline (`docs/design/pro-features.md` §2.12): the
    /// throttle gate first (it refuses only with [`TestLimits::InMemory`]); a member of a
    /// cancelled batch refused by it, or stopped by the endpoint's check, is skipped (`Ok`);
    /// a job over a limit (refused by its throttle, or returning a limiter's refusal) is captured
    /// again by a fake client, parked at its slot with `limited` counted, and a fake client's
    /// drain does not run it again ([`Ran::Parked`]); an inline client returns the refusal as a
    /// failure. Outcomes update the batch table, then the unique table, then the limits table.
    async fn perform_with(
        &self,
        client: &Client,
        mut erased: Arc<dyn ErasedJob>,
        mut job: JobPayload,
    ) -> Result<Ran> {
        loop {
            if job.at.take().is_some() || job.enqueued_at.is_none() {
                job.enqueued_at = Some(now_millis());
            }
            let fetched = job.clone();
            let state = Fetched::from_payload(&fetched);
            // The throttle gate, as in a worker (before server middleware).
            match crate::limit::memory::gate(client, &fetched, &state) {
                Gate::Run => {}
                Gate::Refused(reschedule) => {
                    return self.refused(client, &fetched, &state, reschedule);
                }
                Gate::Unavailable(error) => return Err(error),
                Gate::Failed(error) => {
                    // A failure specific to the job (a throttle key it cannot have), as in a
                    // worker.
                    outcome(client, &state, &no_retry(&fetched, &error));
                    return Err(Error::Job(error));
                }
            }
            let ctx = JobContext::new(job.clone(), client.clone())
                .with_data(Arc::clone(&self.data))
                .with_max_iteration_runtime(self.max_iteration_runtime);
            let span = job_span(&job);
            // The workers' execution path: server middleware, then the job, panics caught.
            let result = execute_job(&self.server_middleware, &ctx, &mut job, Some(erased))
                .instrument(span)
                .await;
            let error = match result {
                Ok(()) => {
                    outcome(client, &state, &Classified::Success);
                    return Ok(Ran::Done);
                }
                Err(error) => error,
            };
            if error.is_batch_cancelled()
                && let Some(member) = &state.member
            {
                // Skipped once its batch confirms the cancellation (the table's accounting turns
                // an unconfirmed one into a failure, as in workers).
                let cancelled = crate::batch::model::is_cancelled(client, &member.bid);
                outcome(client, &state, &Classified::Skipped);
                if cancelled {
                    return Ok(Ran::Done);
                }
                return Err(Error::Job(error));
            }
            if !error.is_interrupted() {
                if !client.is_inline()
                    && let Some(decision) =
                        crate::limit::engine::decide(&self.jobs.config, &error, &fetched)
                {
                    return match decision {
                        OverLimitDecision::Reschedule(reschedule) => {
                            self.park(client, &fetched, &state, reschedule, None, false)
                        }
                        OverLimitDecision::Exhausted(exhausted) => {
                            outcome(client, &state, &no_retry(&fetched, &exhausted));
                            Err(Error::Job(exhausted))
                        }
                    };
                }
                outcome(client, &state, &no_retry(&fetched, &error));
                return Err(Error::Job(error));
            }
            outcome(client, &state, &Classified::Interrupted);
            if !client.is_inline() {
                // Boxed: `push` can lead back here (through an inline delivery), so the
                // future's type is recursive.
                return Box::pin(client.push(fetched)).await.map(|_| Ran::Done);
            }
            // What the push would deliver (`None`: client middleware stopped it), run by the
            // next turn of the loop as freshly enqueued.
            let Some(pushed) = client.prepare_push(fetched).await? else {
                return Ok(Ran::Done);
            };
            job = round_trip(&pushed)?;
            job.enqueued_at = None;
            erased = self.job(&job.class)?;
        }
    }

    /// A job the throttle gate refused (with [`TestLimits::InMemory`]): a member of a cancelled
    /// batch is skipped; an inline client returns the refusal as a failure (nothing would run the
    /// job again: its batch counts it dead); a fake client parks it.
    fn refused(
        &self,
        client: &Client,
        fetched: &JobPayload,
        state: &Fetched,
        reschedule: Reschedule,
    ) -> Result<Ran> {
        if state
            .member
            .as_ref()
            .is_some_and(|member| crate::batch::model::is_cancelled(client, &member.bid))
        {
            outcome(client, state, &Classified::Skipped);
            return Ok(Ran::Done);
        }
        if client.is_inline() {
            let error = JobError::over_limit(OverLimit::new(reschedule.label));
            outcome(client, state, &no_retry(fetched, &error));
            return Err(Error::Job(error));
        }
        let budget = crate::limit::memory::throttle_budget(client, state);
        self.park(client, fetched, state, reschedule, budget, true)
    }

    /// Captures `fetched` again, parked at its slot with `limited` counted (`throttled`: refused
    /// at the gate, else over a limit while it ran), as a worker reschedules it: not an outcome
    /// (its batch keeps it pending, its unique lock stays). Past `budget` it fails.
    fn park(
        &self,
        client: &Client,
        fetched: &JobPayload,
        state: &Fetched,
        reschedule: Reschedule,
        budget: Option<u32>,
        throttled: bool,
    ) -> Result<Ran> {
        let parked =
            crate::limit::memory::park(fetched, &reschedule, budget, now_secs_f64(), now_millis());
        match parked {
            Parked::Again(job) => {
                let classified = if throttled {
                    Classified::Throttled(reschedule)
                } else {
                    Classified::OverLimit(reschedule)
                };
                outcome(client, state, &classified);
                match client.mode() {
                    ClientMode::Fake(store) => Ok(Ran::Parked(store.lock().push(*job))),
                    // Only fake clients park jobs (inline clients fail them).
                    _ => Ok(Ran::Done),
                }
            }
            Parked::Exhausted(error) => {
                outcome(client, state, &no_retry(fetched, &error));
                Err(Error::Job(error))
            }
        }
    }
}

/// What became of a job the testing executor took.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ran {
    /// It ran (or was skipped, as a member of a cancelled batch).
    Done,
    /// A fake client captured it again (this sequence number), parked over a limit: the drain
    /// that took it does not run it again.
    Parked(u64),
}

/// The outcome hooks of the features for a job the testing executor took, in the processor's
/// order: batches, then unique locks, then limits. Jobs without any feature skip them.
fn outcome(client: &Client, state: &Fetched, classified: &Classified) {
    if state.is_plain() {
        return;
    }
    crate::batch::model::account(client, state, classified);
    crate::unique::fake::after(client, state, classified);
    crate::limit::memory::after(client, state, classified);
}

/// The failure of a job the testing clients do not retry: it is dropped as fetched, with its
/// error.
fn no_retry(fetched: &JobPayload, error: &JobError) -> Classified {
    Classified::Failure(Box::new(Verdict::NoRetry {
        job: fetched.clone(),
        error_class: error.class().to_owned(),
        error_message: error.message(),
    }))
}

/// Jobs captured by a fake client, shared by its clones.
#[derive(Clone)]
pub(crate) struct FakeStore {
    jobs: Arc<Mutex<FakeJobs>>,
    executor: Arc<Executor>,
}

impl FakeStore {
    fn new(executor: Executor) -> Self {
        Self {
            jobs: Arc::default(),
            executor: Arc::new(executor),
        }
    }

    fn lock(&self) -> MutexGuard<'_, FakeJobs> {
        self.jobs.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Default for FakeStore {
    fn default() -> Self {
        Self::new(Testing::new().executor())
    }
}

/// The captured jobs, per class, in push order.
#[derive(Default)]
struct FakeJobs {
    /// Sequence number of the next pushed job (orders jobs across classes).
    next: u64,
    by_class: HashMap<String, VecDeque<(u64, JobPayload)>>,
}

impl FakeJobs {
    /// Captures `job`; returns its sequence number.
    fn push(&mut self, job: JobPayload) -> u64 {
        let sequence = self.next;
        self.next += 1;
        self.by_class
            .entry(job.class.clone())
            .or_default()
            .push_back((sequence, job));
        sequence
    }

    fn of_class(&self, class: &str) -> Vec<JobPayload> {
        self.by_class
            .get(class)
            .map(|jobs| jobs.iter().map(|(_, job)| job.clone()).collect())
            .unwrap_or_default()
    }

    /// Every job matching `filter`, in push order.
    fn all_matching(&self, filter: impl Fn(&JobPayload) -> bool) -> Vec<JobPayload> {
        let mut jobs: Vec<&(u64, JobPayload)> = self
            .by_class
            .values()
            .flatten()
            .filter(|(_, job)| filter(job))
            .collect();
        jobs.sort_unstable_by_key(|(sequence, _)| *sequence);
        jobs.into_iter().map(|(_, job)| job.clone()).collect()
    }

    /// Whether `class` has a captured job a drain may run (one not in `parked`).
    fn has_jobs(&self, class: &str, parked: &HashSet<u64>) -> bool {
        self.by_class
            .get(class)
            .is_some_and(|jobs| jobs.iter().any(|(sequence, _)| !parked.contains(sequence)))
    }

    /// Takes the oldest job of `class` not in `parked` (the jobs parked during the current
    /// drain).
    fn pop_front(&mut self, class: &str, parked: &HashSet<u64>) -> Option<JobPayload> {
        let jobs = self.by_class.get_mut(class)?;
        let position = jobs
            .iter()
            .position(|(sequence, _)| !parked.contains(sequence))?;
        let job = jobs.remove(position).map(|(_, job)| job);
        if jobs.is_empty() {
            self.by_class.remove(class);
        }
        job
    }

    /// Classes with captured jobs a drain may run (not in `parked`), ordered by their oldest
    /// such job.
    fn classes(&self, parked: &HashSet<u64>) -> Vec<String> {
        let mut classes: Vec<(u64, &String)> = self
            .by_class
            .iter()
            .filter_map(|(class, jobs)| {
                jobs.iter()
                    .map(|(sequence, _)| *sequence)
                    .find(|sequence| !parked.contains(sequence))
                    .map(|sequence| (sequence, class))
            })
            .collect();
        classes.sort_unstable();
        classes
            .into_iter()
            .map(|(_, class)| class.clone())
            .collect()
    }
}

/// Runs jobs pushed to an inline client.
#[derive(Clone)]
pub(crate) struct InlineExecutor {
    executor: Arc<Executor>,
}

impl InlineExecutor {
    /// An executor running exactly the jobs of `registry`.
    pub(crate) fn new(registry: Registry) -> Self {
        Self {
            executor: Arc::new(Testing::new().registry(registry).executor()),
        }
    }
}

/// A copy of `payload` as it would come back from Redis.
fn round_trip(payload: &JobPayload) -> Result<JobPayload> {
    JobPayload::from_json(&payload.to_json()?)
}

/// Stores jobs pushed to a fake client. Jobs arrive after client middleware; `at` is set
/// only for jobs scheduled in the future, the others get `enqueued_at`.
pub(crate) async fn fake_push(store: &FakeStore, payloads: Vec<JobPayload>) -> Result<()> {
    let enqueued_at = now_millis();
    let mut jobs = Vec::with_capacity(payloads.len());
    for payload in &payloads {
        let mut job = round_trip(payload)?;
        if job.at.is_none() {
            job.enqueued_at = Some(enqueued_at);
        }
        jobs.push(job);
    }
    let mut store = store.lock();
    for job in jobs {
        store.push(job);
    }
    Ok(())
}

/// Runs jobs pushed to an inline client, in order, scheduled ones included. Jobs arrive after
/// client middleware; the first failing job fails the push (the remaining jobs do not run).
///
/// Jobs pushed by a running job run nested inside it (its push returns once they ran, as in
/// Sidekiq's inline mode). Each level takes a good deal of stack (tens of kilobytes in debug
/// builds), so a chain of a few dozen jobs each enqueueing the next can overflow the 2 MiB stack
/// of a test thread: run such chains on a fake client, whose `drain` runs them one after the
/// other. Interrupted iterable jobs do not nest: they run again in a loop
/// ([`Executor::perform_with`]).
pub(crate) async fn inline_push(
    executor: &InlineExecutor,
    client: &Client,
    payloads: Vec<JobPayload>,
) -> Result<()> {
    for payload in &payloads {
        executor
            .executor
            .perform(client, round_trip(payload)?)
            .await?;
    }
    Ok(())
}

/// Helpers for clients created by [`Client::fake`] or [`Testing::fake`].
///
/// The accessors panic when called on another kind of client, since that is a mistake in
/// the test; the `async` methods return [`Error::Config`] instead.
impl Client {
    fn fake_store(&self, method: &str) -> &FakeStore {
        match self.mode() {
            ClientMode::Fake(store) => store,
            _ => panic!(
                "Client::{method} is only available on fake clients (created by Client::fake \
                 or Testing::fake), not on {self:?}"
            ),
        }
    }

    fn try_fake_store(&self, method: &str) -> Result<&FakeStore> {
        match self.mode() {
            ClientMode::Fake(store) => Ok(store),
            _ => Err(Error::Config(format!(
                "Client::{method} is only available on fake clients (created by Client::fake \
                 or Testing::fake)"
            ))),
        }
    }

    /// The captured jobs of type `J`, oldest first (Sidekiq's `HardJob.jobs`).
    ///
    /// # Panics
    ///
    /// If this is not a fake client.
    pub fn jobs<J: Job>(&self) -> Vec<JobPayload> {
        self.jobs_for(J::NAME)
    }

    /// The captured jobs whose class is `class`, oldest first.
    ///
    /// # Panics
    ///
    /// If this is not a fake client.
    pub fn jobs_for(&self, class: &str) -> Vec<JobPayload> {
        self.fake_store("jobs_for").lock().of_class(class)
    }

    /// Every captured job, oldest first (Sidekiq's `Sidekiq::Job.jobs`).
    ///
    /// # Panics
    ///
    /// If this is not a fake client.
    pub fn all_jobs(&self) -> Vec<JobPayload> {
        self.fake_store("all_jobs").lock().all_matching(|_| true)
    }

    /// The captured jobs pushed to `queue`, oldest first (Sidekiq's
    /// `Sidekiq::Queues["name"]`).
    ///
    /// # Panics
    ///
    /// If this is not a fake client.
    pub fn jobs_in_queue(&self, queue: &str) -> Vec<JobPayload> {
        self.fake_store("jobs_in_queue")
            .lock()
            .all_matching(|job| job.queue == queue)
    }

    /// Drops the captured jobs of type `J`.
    ///
    /// # Panics
    ///
    /// If this is not a fake client.
    pub fn clear<J: Job>(&self) {
        self.clear_for(J::NAME);
    }

    /// Drops the captured jobs whose class is `class`.
    ///
    /// # Panics
    ///
    /// If this is not a fake client.
    pub fn clear_for(&self, class: &str) {
        self.fake_store("clear_for").lock().by_class.remove(class);
    }

    /// Drops every captured job.
    ///
    /// # Panics
    ///
    /// If this is not a fake client.
    pub fn clear_all(&self) {
        self.fake_store("clear_all").lock().by_class.clear();
    }

    /// Runs the captured jobs of type `J`, oldest first, until there are none left: jobs of
    /// type `J` pushed by the jobs being run are run too. Returns how many jobs ran.
    ///
    /// Each job is removed before it runs. The first failure stops draining and is returned
    /// ([`Error::Job`]); the remaining jobs stay captured. If jobs are captured for a class no
    /// job is registered for, draining fails with [`Error::UnknownJob`] before removing
    /// anything ([`Error::Config`] if the registry is invalid); with nothing captured for the
    /// class, it returns `Ok(0)`.
    ///
    /// A job over a limit (see [`TestLimits::InMemory`]) is captured again, parked at its slot
    /// with `at` and `limited` set, and is not run again by the same call (the next call tries
    /// it again); it counts as run.
    pub async fn drain<J: Job>(&self) -> Result<usize> {
        self.drain_for(J::NAME).await
    }

    /// [`drain`](Client::drain) for the jobs whose class is `class`.
    pub async fn drain_for(&self, class: &str) -> Result<usize> {
        let mut parked = HashSet::new();
        self.drain_class(class, &mut parked).await
    }

    /// [`drain_for`](Client::drain_for), skipping the jobs `parked` during the current drain
    /// (and adding those it parks).
    async fn drain_class(&self, class: &str, parked: &mut HashSet<u64>) -> Result<usize> {
        let store = self.try_fake_store("drain_for")?;
        let mut performed = 0;
        while store.lock().has_jobs(class, parked) {
            // Resolve the job before removing it: an unknown class keeps its jobs.
            let erased = store.executor.job(class)?;
            let next = store.lock().pop_front(class, parked);
            let Some(job) = next else { break };
            if let Ran::Parked(sequence) = store.executor.perform_with(self, erased, job).await? {
                parked.insert(sequence);
            }
            performed += 1;
        }
        Ok(performed)
    }

    /// Runs the oldest captured job of type `J`. Returns it, or `None` if there was none
    /// (Sidekiq raises `EmptyQueueError` instead). A failure of the job is returned as
    /// [`Error::Job`].
    pub async fn perform_one<J: Job>(&self) -> Result<Option<JobPayload>> {
        self.perform_one_for(J::NAME).await
    }

    /// [`perform_one`](Client::perform_one) for the jobs whose class is `class`.
    pub async fn perform_one_for(&self, class: &str) -> Result<Option<JobPayload>> {
        let store = self.try_fake_store("perform_one_for")?;
        let none = HashSet::new();
        if !store.lock().has_jobs(class, &none) {
            return Ok(None);
        }
        // Resolve the job before removing it: an unknown class keeps its jobs.
        let erased = store.executor.job(class)?;
        let next = store.lock().pop_front(class, &none);
        let Some(job) = next else {
            return Ok(None);
        };
        store
            .executor
            .perform_with(self, erased, job.clone())
            .await?;
        Ok(Some(job))
    }

    /// Runs every captured job until none are left, including jobs pushed by the jobs being
    /// run. Classes are drained in the order of their oldest job. Returns how many jobs ran.
    /// Jobs parked over a limit during the call stay captured (see [`Client::drain`]).
    ///
    /// A job that always enqueues another job makes this loop forever, as in Sidekiq.
    pub async fn drain_all(&self) -> Result<usize> {
        let store = self.try_fake_store("drain_all")?;
        let mut parked = HashSet::new();
        let mut performed = 0;
        loop {
            let classes = store.lock().classes(&parked);
            if classes.is_empty() {
                return Ok(performed);
            }
            for class in classes {
                performed += self.drain_class(&class, &mut parked).await?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn job(class: &str, queue: &str, arg: i64) -> JobPayload {
        let mut job = JobPayload::new(class, vec![json!(arg)]);
        job.queue = queue.to_owned();
        job
    }

    #[test]
    fn fake_jobs_keep_push_order_across_classes() {
        let mut jobs = FakeJobs::default();
        jobs.push(job("B", "q1", 1));
        jobs.push(job("A", "q2", 2));
        jobs.push(job("B", "q2", 3));
        let none = HashSet::new();
        assert_eq!(jobs.classes(&none), ["B", "A"]);
        let args = |jobs: Vec<JobPayload>| -> Vec<i64> {
            jobs.iter()
                .map(|job| job.args[0].as_i64().unwrap())
                .collect()
        };
        assert_eq!(args(jobs.all_matching(|_| true)), [1, 2, 3]);
        assert_eq!(args(jobs.all_matching(|job| job.queue == "q2")), [2, 3]);
        assert_eq!(args(jobs.of_class("B")), [1, 3]);
        assert_eq!(
            jobs.pop_front("B", &none).map(|job| job.args[0].clone()),
            Some(json!(1))
        );
        assert_eq!(jobs.classes(&none), ["A", "B"]);
        // Jobs parked during a drain are skipped by it.
        let parked = HashSet::from([2]);
        assert_eq!(jobs.classes(&parked), ["A"]);
        assert!(!jobs.has_jobs("B", &parked));
        assert!(jobs.pop_front("B", &parked).is_none());
        assert!(jobs.pop_front("A", &none).is_some());
        assert!(jobs.pop_front("A", &none).is_none());
        assert!(!jobs.by_class.contains_key("A"));
        assert!(jobs.of_class("A").is_empty());
    }

    #[tokio::test]
    async fn registries_prefer_explicit_jobs() {
        /// Fails with its label, to tell instances apart.
        struct Stub(&'static str);
        impl Job for Stub {
            const NAME: &'static str = "Stub";
            type Args = ();
            async fn perform(
                &self,
                _: &JobContext,
                (): (),
            ) -> std::result::Result<(), crate::JobError> {
                Err(crate::JobError::msg(self.0))
            }
        }
        let mut base = Registry::new();
        base.register(Stub("base")).unwrap();
        let testing = Testing::new().registry(base).register(Stub("explicit"));
        let registry = testing.build_registry().unwrap();
        assert_eq!(registry.names(), ["Stub"]);
        let ctx = JobContext::new(JobPayload::new("Stub", vec![]), Client::fake());
        let error = registry
            .get("Stub")
            .unwrap()
            .perform_json(&ctx, vec![])
            .await
            .unwrap_err();
        assert_eq!(error.message(), "explicit");

        let duplicate = Testing::new()
            .auto_register(false)
            .register(Stub("a"))
            .register(Stub("b"));
        assert!(matches!(duplicate.build_registry(), Err(Error::Config(_))));
        assert!(
            Testing::new()
                .auto_register(false)
                .build_registry()
                .unwrap()
                .is_empty()
        );
        assert!(format!("{testing:?}").contains("Stub"));
    }

    #[test]
    fn explicit_jobs_shadow_duplicated_inventory_names() {
        struct Named(&'static str);
        impl Job for Named {
            const NAME: &'static str = "Named";
            type Args = ();
            async fn perform(
                &self,
                _: &JobContext,
                (): (),
            ) -> std::result::Result<(), crate::JobError> {
                Err(crate::JobError::msg(self.0))
            }
        }
        #[derive(Default)]
        struct Other;
        impl Job for Other {
            const NAME: &'static str = "Other";
            type Args = ();
            async fn perform(
                &self,
                _: &JobContext,
                (): (),
            ) -> std::result::Result<(), crate::JobError> {
                Ok(())
            }
        }
        let duplicated = [
            JobRegistration::new("Named", || Arc::new(Named("first"))),
            JobRegistration::new("Named", || Arc::new(Named("second"))),
            JobRegistration::of::<Other>(),
        ];
        // As in a worker, a duplicated name is an error...
        let error = Testing::new().build_registry_with(&duplicated).unwrap_err();
        assert!(
            matches!(error, Error::Config(ref m) if m.contains("Named")),
            "{error}"
        );
        // ...unless the test registers its own job under that name.
        let registry = Testing::new()
            .register(Named("stub"))
            .build_registry_with(&duplicated)
            .unwrap();
        assert_eq!(registry.names(), ["Named", "Other"]);
        // Without automatic registration the inventory is not read.
        let registry = Testing::new()
            .auto_register(false)
            .build_registry_with(&duplicated)
            .unwrap();
        assert!(registry.is_empty());
    }

    #[test]
    #[should_panic(expected = "only available on fake clients")]
    fn accessors_panic_on_other_clients() {
        let _ = Client::inline(Registry::new()).jobs_for("A");
    }
}
