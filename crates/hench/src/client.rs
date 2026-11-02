//! Pushing jobs: [`Client`], bulk pushes ([`BulkPush`]) and the typed builder
//! ([`JobBuilder`]).

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use serde_json::Value;

use crate::batch::BatchDefaults;
use crate::batch::collect::Collector;
use crate::error::{Error, Result};
use crate::iterable::LocalStates;
use crate::job::{Job, JobSetter, Registry};
use crate::keys;
use crate::limit::ThrottleOption;
use crate::middleware::ClientChain;
use crate::options::JobOptions;
use crate::payload::{
    BacktraceOption, JobPayload, RetryOption, generate_jid, now_millis, now_secs_f64,
    system_time_to_secs,
};
use crate::redis::RedisPool;
use crate::testing::{FakeStore, InlineExecutor, Tables};
use crate::unique::{OnConflict, PushOutcome, Unique, UniqueOption, UniqueUntil};

/// Largest accepted `retry_for`, in seconds (larger values look like timestamps).
const MAX_RETRY_FOR_SECS: u64 = 1_000_000_000;

/// Smallest `spread_interval` used by [`Client::push_bulk`] (shorter spreads are pointless
/// given the scheduler's poll interval).
const MIN_SPREAD_INTERVAL: Duration = Duration::from_secs(5);

/// Default bulk batch size for immediate jobs.
const DEFAULT_BATCH_SIZE: usize = 1_000;

/// Default bulk batch size for scheduled jobs (sorted-set inserts are more expensive).
const DEFAULT_SCHEDULED_BATCH_SIZE: usize = 100;

/// Pushes jobs. Cheap to clone; clones share the connection pool and configuration.
///
/// Create one with [`Client::new`] (or get the configured one from
/// [`Hench::client`](crate::Hench::client)), then push with the typed helpers
/// ([`JobExt`](crate::JobExt), [`Client::enqueue`], [`Client::job`]) or with raw payloads
/// ([`Client::push`], [`Client::push_bulk`]).
///
/// [`Client::fake`] and [`Client::inline`] create clients for tests that never touch Redis.
///
/// A push goes through four steps (Sidekiq's `Client#push`):
///
/// 1. **normalize**: fill unset fields from the job type's options and the client's
///    `default_job_options`, generate the jid, set `created_at`, and validate;
/// 2. **client middleware**, which may modify the job or stop the push;
/// 3. **raw push**: a job whose `at` is in the future is added to the `schedule` sorted set
///    (without its `at` and `enqueued_at` fields); any other job is pushed to its queue with
///    `enqueued_at` set, together with registering the queue name, in one `MULTI`;
/// 4. on a failover error (`READONLY`, `NOREPLICAS`, `UNBLOCKED`) or a dropped connection the
///    raw push is retried once.
///
/// # When a push fails
///
/// An error does not always mean that the job was not pushed. Errors before anything is sent
/// (an invalid job, [`Error::InvalidJob`](crate::Error::InvalidJob); a client middleware's
/// error) and refusals from Redis (out of memory, a read-only replica after the retry) do. But
/// a reply that never comes leaves the outcome unknown: Redis may have run the push, and the
/// job then runs although the call failed. That is the case of a response timeout (Redis
/// stalled by a slow fork, a long script, a paused VM or a failover; the error is
/// [`Error::Redis`](crate::Error::Redis) with
/// [`is_timeout()`](crate::redis_rs::RedisError::is_timeout)), which is never retried for that
/// reason, and of a connection dropped after the request left, whose retry (step 4) can push
/// the job twice, with the same jid.
///
/// So pushing again after an error may enqueue a job twice. Jobs run at least once anyway, so
/// make them idempotent; to recognize duplicates, give the jobs a jid of your own
/// ([`JobSetter::jid`](crate::JobSetter::jid), [`JobBuilder::jid`], [`JobPayload::jid`]),
/// keep it when pushing again, and let the job (or a server middleware) skip a jid it has
/// already handled.
#[derive(Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
}

#[derive(Clone)]
struct ClientInner {
    redis: RedisPool,
    middleware: ClientChain,
    default_options: JobOptions,
    mode: ClientMode,
    /// Iteration states of iterable jobs, for the testing modes (which have no Redis).
    local_states: Arc<LocalStates>,
    /// Whether pushes honour uniqueness ([`Client::with_unique_jobs`]).
    unique_jobs: bool,
    /// The settings of the batches this client defines ([`Client::with_batch_defaults`]).
    batch_defaults: BatchDefaults,
    /// The feature tables of the testing modes (locks, batches, limits), shared by a client, its
    /// clones and its collecting clients.
    tables: Arc<Tables>,
}

/// Where pushed jobs go.
#[derive(Clone)]
pub(crate) enum ClientMode {
    /// To Redis.
    Redis,
    /// Into an in-memory store (Sidekiq's fake testing mode).
    Fake(FakeStore),
    /// Executed immediately, in process (Sidekiq's inline testing mode).
    Inline(InlineExecutor),
    /// Buffered for the commit of a batch definition (the collecting client of a batch).
    Collect(Collector),
}

/// What [`Client::raw_push`] did with one job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PushResult {
    /// Written (with its lock, for a unique job).
    Pushed,
    /// A unique job whose lock its own jid holds: written by an earlier attempt (a resend).
    AlreadyPushed,
    /// A unique job whose lock another job holds: not written.
    Duplicate {
        /// The jid holding the lock.
        holder: String,
    },
    /// A unique job written in place of the scheduled holder of its lock.
    Replaced {
        /// The jid of the job it replaced.
        replaced: String,
    },
    /// Written, but its lock could not be taken (the error is logged).
    PushedWithoutLock {
        /// Why the lock could not be taken.
        error: String,
    },
}

impl PushResult {
    /// Whether the job was written.
    pub(crate) fn written(&self) -> bool {
        !matches!(
            self,
            PushResult::Duplicate { .. } | PushResult::AlreadyPushed
        )
    }
}

/// The keys of a typed push computed by [`Job::unique_key`] and [`Job::throttle_key`] (see
/// [`JobSetter::typed_keys`]), set on the job before normalization.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct TypedKeys {
    /// The unique key.
    pub(crate) unique: Option<Value>,
    /// The throttle key.
    pub(crate) throttle: Option<String>,
}

impl TypedKeys {
    /// `true` when there is no key.
    pub(crate) fn is_empty(&self) -> bool {
        self.unique.is_none() && self.throttle.is_none()
    }

    /// Sets the keys on `job` (under its explicit options, which already exclude them).
    pub(crate) fn apply(&self, job: &mut JobPayload) {
        if let Some(key) = &self.unique {
            let keyed = UniqueOption::Enabled(Unique::default().key(key.clone()));
            job.unique = Some(match job.unique.take() {
                Some(explicit) => explicit.merge(&keyed),
                None => keyed,
            });
        }
        if let Some(key) = &self.throttle {
            let keyed = ThrottleOption::Enabled {
                name: None,
                key: Some(key.clone()),
            };
            job.throttle = Some(match job.throttle.take() {
                Some(explicit) => explicit.merge(&keyed),
                None => keyed,
            });
        }
    }
}

/// A job going through `raw_push`, and how its result maps to a [`PushOutcome`].
struct Pushing {
    jid: String,
    class: String,
    digest: Option<String>,
    on_conflict: OnConflict,
}

impl Pushing {
    fn of(job: &JobPayload) -> Self {
        let unique = job.unique.as_ref().and_then(UniqueOption::settings);
        Self {
            jid: job.jid.clone(),
            class: job.class.clone(),
            digest: unique.and_then(|unique| unique.digest.clone()),
            on_conflict: unique
                .and_then(|unique| unique.on_conflict)
                .unwrap_or_default(),
        }
    }

    /// The outcome of `result`. A duplicate is an error with [`OnConflict::Error`], except in
    /// bulk pushes, which cannot fail for some jobs only.
    fn outcome(self, result: PushResult, bulk: bool) -> Result<PushOutcome> {
        let Pushing {
            jid,
            class,
            digest,
            on_conflict,
        } = self;
        Ok(match result {
            PushResult::Pushed | PushResult::AlreadyPushed => PushOutcome::Pushed { jid },
            PushResult::PushedWithoutLock { error } => {
                tracing::warn!(%jid, %class, %error, "a unique job was pushed without its lock");
                PushOutcome::Pushed { jid }
            }
            PushResult::Replaced { replaced } => PushOutcome::Replaced { jid, replaced },
            PushResult::Duplicate { holder } => {
                if on_conflict == OnConflict::Error && !bulk {
                    return Err(Error::DuplicateJob {
                        class,
                        digest: digest.unwrap_or_default(),
                        holder,
                    });
                }
                PushOutcome::Duplicate { jid, holder }
            }
        })
    }
}

/// The only result of a one-job `raw_push`.
fn single(results: Vec<PushResult>) -> Result<PushResult> {
    results
        .into_iter()
        .next()
        .ok_or_else(|| Error::InvalidJob("the push of one job reported no result".to_owned()))
}

/// `true` when `job` carries the digest of a unique lock (it goes through the push script of
/// unique jobs).
fn has_digest(job: &JobPayload) -> bool {
    job.unique
        .as_ref()
        .and_then(UniqueOption::settings)
        .is_some_and(|unique| unique.digest.is_some())
}

impl Client {
    /// A client pushing to `redis`, with no middleware and the built-in `default_job_options`
    /// (queue `default`, `retry: true`).
    pub fn new(redis: RedisPool) -> Self {
        Self::with_mode(redis, ClientMode::Redis)
    }

    /// A Redis client with the given middleware, default job options, uniqueness switch and
    /// batch settings (used by [`HenchBuilder::build`](crate::HenchBuilder::build), from the
    /// configuration).
    pub(crate) fn from_parts(
        redis: RedisPool,
        middleware: ClientChain,
        default_options: JobOptions,
        unique_jobs: bool,
        batch_defaults: BatchDefaults,
    ) -> Self {
        Self {
            inner: Arc::new(ClientInner {
                redis,
                middleware,
                default_options,
                mode: ClientMode::Redis,
                local_states: Arc::default(),
                unique_jobs,
                batch_defaults,
                tables: Arc::default(),
            }),
        }
    }

    pub(crate) fn with_mode(redis: RedisPool, mode: ClientMode) -> Self {
        Self {
            inner: Arc::new(ClientInner {
                redis,
                middleware: ClientChain::new(),
                default_options: JobOptions::defaults(),
                mode,
                local_states: Arc::default(),
                unique_jobs: true,
                batch_defaults: BatchDefaults::default(),
                tables: Arc::default(),
            }),
        }
    }

    /// A client that captures pushed jobs in memory instead of pushing them to Redis
    /// (Sidekiq's fake testing mode). See [`crate::testing`].
    pub fn fake() -> Self {
        Self::with_mode(
            RedisPool::detached(),
            ClientMode::Fake(FakeStore::default()),
        )
    }

    /// A client that runs pushed jobs immediately, in process, using the jobs in `registry`
    /// (Sidekiq's inline testing mode). See [`crate::testing`].
    pub fn inline(registry: Registry) -> Self {
        Self::with_mode(
            RedisPool::detached(),
            ClientMode::Inline(InlineExecutor::new(registry)),
        )
    }

    /// A copy of this client whose middleware chain was modified by `configure` (this client
    /// is unchanged).
    #[must_use]
    pub fn with_middleware(&self, configure: impl FnOnce(&mut ClientChain)) -> Client {
        let mut inner = ClientInner::clone(&self.inner);
        configure(&mut inner.middleware);
        Self {
            inner: Arc::new(inner),
        }
    }

    /// A copy of this client whose `default_job_options` are this client's merged with
    /// `options`: what `options` sets replaces the current value, the rest is kept (like
    /// [`HenchBuilder::default_job_options`](crate::HenchBuilder::default_job_options)).
    #[must_use]
    pub fn with_default_job_options(&self, options: JobOptions) -> Client {
        let mut inner = ClientInner::clone(&self.inner);
        inner.default_options = inner.default_options.merge(&options);
        Self {
            inner: Arc::new(inner),
        }
    }

    /// A copy of this client pushing to `redis` instead (sharding: one client per Redis).
    #[must_use]
    pub fn with_redis(&self, redis: RedisPool) -> Client {
        let mut inner = ClientInner::clone(&self.inner);
        inner.redis = redis;
        Self {
            inner: Arc::new(inner),
        }
    }

    /// A copy of this client that honours uniqueness (`true`, the default) or ignores it
    /// (`false`: pushes drop the `unique` option and take no lock; see [`crate::unique`]).
    #[must_use]
    pub fn with_unique_jobs(&self, enabled: bool) -> Client {
        let mut inner = ClientInner::clone(&self.inner);
        inner.unique_jobs = enabled;
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Whether pushes honour uniqueness (see [`Client::with_unique_jobs`]).
    pub fn unique_jobs(&self) -> bool {
        self.inner.unique_jobs
    }

    /// A copy of this client defining batches with these settings (see [`crate::batch`]).
    #[must_use]
    pub fn with_batch_defaults(&self, defaults: BatchDefaults) -> Client {
        let mut inner = ClientInner::clone(&self.inner);
        inner.batch_defaults = defaults;
        Self {
            inner: Arc::new(inner),
        }
    }

    /// The settings of the batches this client defines.
    pub fn batch_defaults(&self) -> BatchDefaults {
        self.inner.batch_defaults
    }

    /// The feature tables of the testing modes, shared by this client, its clones and its
    /// collecting clients.
    pub(crate) fn tables(&self) -> &Arc<Tables> {
        &self.inner.tables
    }

    /// A copy of this client that buffers its pushes for `collector`'s batch (the collecting
    /// client of a batch definition): everything else is this client's (pool, middleware,
    /// default job options, unique switch, batch settings and the testing tables).
    pub(crate) fn collecting(&self, collector: crate::batch::collect::Collector) -> Client {
        let mut inner = ClientInner::clone(&self.inner);
        inner.mode = ClientMode::Collect(collector);
        Self {
            inner: Arc::new(inner),
        }
    }

    /// The Redis pool jobs are pushed to (detached for fake and inline clients).
    pub fn redis(&self) -> &RedisPool {
        &self.inner.redis
    }

    /// The client middleware chain.
    pub fn middleware(&self) -> &ClientChain {
        &self.inner.middleware
    }

    /// The options applied to every job that does not set them.
    pub fn default_job_options(&self) -> &JobOptions {
        &self.inner.default_options
    }

    /// `true` for clients created by [`Client::fake`].
    pub fn is_fake(&self) -> bool {
        matches!(self.inner.mode, ClientMode::Fake(_))
    }

    /// `true` for clients created by [`Client::inline`].
    pub fn is_inline(&self) -> bool {
        matches!(self.inner.mode, ClientMode::Inline(_))
    }

    /// Where this client's jobs go (used by the testing modes).
    pub(crate) fn mode(&self) -> &ClientMode {
        &self.inner.mode
    }

    /// Where the testing modes keep the state of iterable jobs (`None` for Redis clients, whose
    /// jobs keep it in Redis).
    pub(crate) fn local_states(&self) -> Option<&LocalStates> {
        match self.inner.mode {
            ClientMode::Redis | ClientMode::Collect(_) => None,
            ClientMode::Fake(_) | ClientMode::Inline(_) => Some(&self.inner.local_states),
        }
    }

    /// Pushes a job given as a raw payload. Only `default_job_options` apply (as in Sidekiq,
    /// pushing by class name does not apply the job type's own options). An empty `jid` is
    /// generated and an empty `queue` means the default queue.
    ///
    /// Returns the job's jid, or `None` if client middleware stopped the push. An error after
    /// a timeout may come with the job pushed all the same: see
    /// [when a push fails](Client#when-a-push-fails).
    pub async fn push(&self, payload: JobPayload) -> Result<Option<String>> {
        let payload = self.normalize(payload, None)?;
        self.push_normalized(payload).await
    }

    /// Pushes many jobs of the same class in as few round trips as possible; see
    /// [`BulkPush`] for the rules. Returns one entry per job: its jid, or `None` if client
    /// middleware stopped that job. On an error some batches may have been pushed (those
    /// before the failing one, and that one too after a timeout: see
    /// [when a push fails](Client#when-a-push-fails)).
    pub async fn push_bulk(&self, bulk: BulkPush) -> Result<Vec<Option<String>>> {
        Ok(jids(self.push_bulk_with(bulk, None).await?))
    }

    /// Pushes a job given as a raw payload like [`Client::push`], and says what happened:
    /// pushed, a duplicate of a unique job (with the jid holding its lock; an
    /// [`Error::DuplicateJob`] with [`OnConflict::Error`]), a replacement, or stopped by client
    /// middleware.
    pub async fn push_outcome(&self, payload: JobPayload) -> Result<PushOutcome> {
        let payload = self.normalize(payload, None)?;
        self.push_normalized_outcome(payload).await
    }

    /// Pushes many jobs like [`Client::push_bulk`], with the outcome of each (a duplicate is
    /// never an error in a bulk push).
    pub async fn push_bulk_outcomes(&self, bulk: BulkPush) -> Result<Vec<PushOutcome>> {
        self.push_bulk_with(bulk, None).await
    }

    /// Pushes job `J` with `args` for immediate execution. Returns its jid, or `None` if client
    /// middleware stopped the push; an error after a timeout may come with the job pushed (see
    /// [when a push fails](Client#when-a-push-fails)).
    pub async fn enqueue<J: Job>(&self, args: J::Args) -> Result<Option<String>> {
        JobSetter::<J>::new().perform_async(self, args).await
    }

    /// Schedules job `J` to run after `delay`; see [`JobExt::perform_in`](crate::JobExt::perform_in)
    /// and [when a push fails](Client#when-a-push-fails).
    pub async fn enqueue_in<J: Job>(
        &self,
        delay: Duration,
        args: J::Args,
    ) -> Result<Option<String>> {
        JobSetter::<J>::new()
            .delay(delay)
            .perform_async(self, args)
            .await
    }

    /// Schedules job `J` to run at `at` (immediately if `at` is not in the future); see
    /// [when a push fails](Client#when-a-push-fails).
    pub async fn enqueue_at<J: Job>(
        &self,
        at: SystemTime,
        args: J::Args,
    ) -> Result<Option<String>> {
        JobSetter::<J>::new().at(at).perform_async(self, args).await
    }

    /// A builder for pushing job `J` with per-push options:
    /// `client.job::<J>().queue("low").delay(d).perform_async(args)`.
    pub fn job<J: Job>(&self) -> JobBuilder<'_, J> {
        JobBuilder {
            client: self,
            setter: JobSetter::new(),
        }
    }

    /// Pushes a payload built by a typed setter: applies `class_options` (the job type's
    /// options) under the payload's explicit fields, then pushes.
    pub(crate) async fn push_typed(
        &self,
        payload: JobPayload,
        class_options: &JobOptions,
    ) -> Result<Option<String>> {
        let payload = self.normalize(payload, Some(class_options))?;
        self.push_normalized(payload).await
    }

    /// [`Client::push_typed`] with the outcome.
    pub(crate) async fn push_typed_outcome(
        &self,
        payload: JobPayload,
        class_options: &JobOptions,
    ) -> Result<PushOutcome> {
        let payload = self.normalize(payload, Some(class_options))?;
        self.push_normalized_outcome(payload).await
    }

    /// [`Client::push_bulk`] for typed setters (with the job type's options).
    pub(crate) async fn push_bulk_typed(
        &self,
        bulk: BulkPush,
        class_options: &JobOptions,
    ) -> Result<Vec<Option<String>>> {
        Ok(jids(self.push_bulk_with(bulk, Some(class_options)).await?))
    }

    /// [`Client::push_bulk_typed`] with the outcome of each job.
    pub(crate) async fn push_bulk_typed_outcomes(
        &self,
        bulk: BulkPush,
        class_options: &JobOptions,
    ) -> Result<Vec<PushOutcome>> {
        self.push_bulk_with(bulk, Some(class_options)).await
    }

    /// Normalizes and validates a payload before middleware (Sidekiq's `normalize_item`):
    /// fills unset fields from `class_options` and then the default job options, generates
    /// the jid if empty and sets `created_at` if unset.
    pub(crate) fn normalize(
        &self,
        payload: JobPayload,
        class_options: Option<&JobOptions>,
    ) -> Result<JobPayload> {
        let mut payload = self.normalize_common(payload, class_options)?;
        self.normalize_features(&mut payload)?;
        Ok(payload)
    }

    /// The part of [`Client::normalize`] that does not depend on the job's arguments (shared
    /// by the jobs of a bulk push).
    fn normalize_common(
        &self,
        mut payload: JobPayload,
        class_options: Option<&JobOptions>,
    ) -> Result<JobPayload> {
        if payload.class.is_empty() {
            return Err(invalid("Job class must not be empty"));
        }
        match class_options {
            Some(class_options) => self
                .inner
                .default_options
                .merge(class_options)
                .apply_defaults(&mut payload),
            None => self.inner.default_options.apply_defaults(&mut payload),
        }
        if payload.queue.is_empty() {
            return Err(invalid("Job must include a valid queue name"));
        }
        validate_retry_for(payload.retry_for)?;
        validate_at(payload.at)?;
        if payload.jid.is_empty() {
            payload.jid = generate_jid();
        }
        if payload.created_at.is_none() {
            payload.created_at = Some(now_millis());
        }
        strip_reserved_extra(&mut payload);
        Ok(payload)
    }

    /// The features' steps of normalization, once the job has its arguments
    /// (`docs/design/pro-features.md` §3.1): uniqueness (validation and digest), the throttle,
    /// and the batch of a collecting client.
    fn normalize_features(&self, payload: &mut JobPayload) -> Result<()> {
        crate::unique::normalize(payload, self.inner.unique_jobs)?;
        crate::limit::normalize(payload)?;
        if let ClientMode::Collect(collector) = &self.inner.mode {
            crate::batch::collect::normalize(collector, payload)?;
        }
        Ok(())
    }

    /// Runs client middleware on a normalized payload and pushes the result.
    async fn push_normalized(&self, payload: JobPayload) -> Result<Option<String>> {
        let outcome = self.push_normalized_outcome(payload).await?;
        Ok(outcome.jid().map(str::to_owned))
    }

    /// [`Client::push_normalized`] with the outcome.
    async fn push_normalized_outcome(&self, payload: JobPayload) -> Result<PushOutcome> {
        let Some(payload) = self.run_middleware(payload).await? else {
            return Ok(PushOutcome::Stopped);
        };
        let pushing = Pushing::of(&payload);
        let result = single(self.raw_push(vec![payload]).await?)?;
        pushing.outcome(result, false)
    }

    /// What [`Client::push`] does before delivering a job: normalizes it and runs client
    /// middleware, returning the job as [`Client::raw_push`] would receive it, or `None` if
    /// middleware stopped the push. Lets [`crate::api`] move a job out of a sorted set and into
    /// its queue in one atomic step.
    pub(crate) async fn prepare_push(&self, payload: JobPayload) -> Result<Option<JobPayload>> {
        let payload = self.normalize(payload, None)?;
        self.run_middleware(payload).await
    }

    /// Runs client middleware on a normalized payload and validates its output; `None` if
    /// middleware stopped the push.
    async fn run_middleware(&self, payload: JobPayload) -> Result<Option<JobPayload>> {
        match self
            .inner
            .middleware
            .invoke(payload, &self.inner.redis)
            .await?
        {
            Some(payload) => revalidate(payload).map(Some),
            None => Ok(None),
        }
    }

    async fn push_bulk_with(
        &self,
        bulk: BulkPush,
        class_options: Option<&JobOptions>,
    ) -> Result<Vec<PushOutcome>> {
        let BulkPush {
            mut template,
            args,
            at,
            spread_interval,
            batch_size,
            keys,
        } = bulk;

        if template.at.is_some() {
            return Err(invalid(
                "set the time of bulk jobs with BulkPush::at, not the template's `at`",
            ));
        }
        match &at {
            Some(BulkAt::All(at)) => validate_at(Some(*at))?,
            Some(BulkAt::Each(times)) => {
                if times.is_empty() {
                    return Err(invalid(
                        "Job 'at' must be a timestamp or a non-empty list of timestamps",
                    ));
                }
                if times.len() != args.len() {
                    return Err(invalid(
                        "Job 'at' list must have the same size as the 'args' list",
                    ));
                }
                for time in times {
                    validate_at(Some(*time))?;
                }
            }
            None => {}
        }
        let explicit_jid = std::mem::take(&mut template.jid);
        if !explicit_jid.is_empty() && args.len() > 1 {
            return Err(invalid(
                "Explicitly passing 'jid' when pushing more than one job is not supported",
            ));
        }
        let at = match spread_interval {
            Some(spread) => {
                if spread.is_zero() {
                    return Err(invalid("Jobs 'spread_interval' must be positive"));
                }
                if at.is_some() {
                    return Err(invalid(
                        "Only one of 'at' or 'spread_interval' can be provided",
                    ));
                }
                let spread = spread.max(MIN_SPREAD_INTERVAL).as_secs_f64();
                let now = now_secs_f64();
                if now + spread > MAX_AT {
                    return Err(invalid(
                        "Jobs 'spread_interval' must end before the year 10000",
                    ));
                }
                Some(BulkAt::Each(
                    args.iter()
                        .map(|_| now + fastrand::f64() * spread)
                        .collect(),
                ))
            }
            None => at,
        };
        let batch_size = batch_size.unwrap_or(if at.is_some() {
            DEFAULT_SCHEDULED_BATCH_SIZE
        } else {
            DEFAULT_BATCH_SIZE
        });
        if batch_size == 0 {
            return Err(invalid("batch_size must be positive"));
        }

        if !keys.is_empty() && keys.len() != args.len() {
            return Err(invalid(
                "the typed keys of a bulk push must match its argument lists",
            ));
        }
        template.args = Vec::new();
        let normalized = self.normalize_common(template, class_options)?;
        if args.is_empty() {
            return Ok(Vec::new());
        }

        let mut outcomes = Vec::with_capacity(args.len());
        let mut args = args.into_iter();
        let mut index = 0_usize;
        loop {
            let batch: Vec<Vec<Value>> = args.by_ref().take(batch_size).collect();
            if batch.is_empty() {
                break;
            }
            let mut to_push = Vec::with_capacity(batch.len());
            // One entry per job of the batch: `None` when middleware stopped it.
            let mut pushing = Vec::with_capacity(batch.len());
            for job_args in batch {
                let mut job = normalized.clone();
                job.args = job_args;
                job.jid = if explicit_jid.is_empty() {
                    generate_jid()
                } else {
                    explicit_jid.clone()
                };
                job.at = match &at {
                    None => None,
                    Some(BulkAt::All(at)) => Some(*at),
                    Some(BulkAt::Each(times)) => times.get(index).copied(),
                };
                if let Some(keys) = keys.get(index) {
                    keys.apply(&mut job);
                }
                index += 1;
                self.normalize_features(&mut job)?;
                match self.inner.middleware.invoke(job, &self.inner.redis).await? {
                    Some(job) => {
                        let job = revalidate(job)?;
                        pushing.push(Some(Pushing::of(&job)));
                        to_push.push(job);
                    }
                    None => pushing.push(None),
                }
            }
            let mut results = self.raw_push(to_push).await?.into_iter();
            for job in pushing {
                outcomes.push(match job {
                    None => PushOutcome::Stopped,
                    Some(job) => {
                        let result = results.next().ok_or_else(|| {
                            Error::InvalidJob("a bulk push reported too few results".to_owned())
                        })?;
                        job.outcome(result, true)?
                    }
                });
            }
        }
        Ok(outcomes)
    }

    /// Delivers jobs that went through middleware: to Redis, or to the testing store.
    /// Jobs whose `at` is not in the future lose their `at` and are pushed immediately.
    /// Returns one result per job, in order.
    pub(crate) async fn raw_push(&self, mut payloads: Vec<JobPayload>) -> Result<Vec<PushResult>> {
        if payloads.is_empty() {
            return Ok(Vec::new());
        }
        let now = now_secs_f64();
        for payload in &mut payloads {
            if payload.at.is_some_and(|at| at <= now) {
                payload.at = None;
            }
        }
        match &self.inner.mode {
            ClientMode::Redis if payloads.iter().any(has_digest) => {
                crate::unique::push::deliver(&self.inner.redis, payloads, now).await
            }
            ClientMode::Redis => {
                let pushed = vec![PushResult::Pushed; payloads.len()];
                self.redis_push(payloads, now).await?;
                Ok(pushed)
            }
            ClientMode::Fake(store) => {
                let (results, admitted) = self.admit(payloads);
                crate::testing::fake_push(store, admitted).await?;
                Ok(results)
            }
            ClientMode::Inline(executor) => {
                let (results, admitted) = self.admit(payloads);
                crate::testing::inline_push(executor, self, admitted).await?;
                Ok(results)
            }
            ClientMode::Collect(collector) => crate::batch::collect::append(collector, payloads),
        }
    }

    /// The jobs a testing client stores or runs, after the in-memory locks of unique jobs: the
    /// result of each job, and the jobs written.
    fn admit(&self, payloads: Vec<JobPayload>) -> (Vec<PushResult>, Vec<JobPayload>) {
        let results = crate::unique::fake::admit(self, &payloads);
        let admitted = payloads
            .into_iter()
            .zip(&results)
            .filter(|(_, result)| result.written())
            .map(|(payload, _)| payload)
            .collect();
        (results, admitted)
    }

    /// Writes jobs to Redis in one transaction; `now` is the time `raw_push` checked their
    /// `at` against.
    async fn redis_push(&self, payloads: Vec<JobPayload>, now: f64) -> Result<()> {
        let enqueued_at = now_millis();
        let mut scheduled: Vec<(f64, String)> = Vec::new();
        let mut queued: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for payload in payloads {
            match Stored::encode(payload, now, enqueued_at)? {
                Stored::Scheduled { at, json } => scheduled.push((at, json)),
                Stored::Queued { queue, json } => queued.entry(queue).or_default().push(json),
            }
        }

        let mut pipe = ::redis::pipe();
        pipe.atomic();
        if !scheduled.is_empty() {
            let command = pipe.cmd("ZADD").arg(keys::SCHEDULE);
            for (score, json) in &scheduled {
                command.arg(*score).arg(json);
            }
            command.ignore();
        }
        if !queued.is_empty() {
            pipe.cmd("SADD")
                .arg(keys::QUEUES)
                .arg(queued.keys().collect::<Vec<_>>())
                .ignore();
            for (queue, jobs) in &queued {
                pipe.cmd("LPUSH").arg(keys::queue(queue)).arg(jobs).ignore();
            }
        }
        let pipe = &pipe;
        self.inner
            .redis
            .with_retry(|mut conn| async move { pipe.exec_async(&mut conn).await })
            .await
    }
}

/// How a job leaving client middleware is stored in Redis: the encoding of
/// [`Client::raw_push`], shared with [`crate::api`]'s atomic moves so that both write the
/// same JSON.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Stored {
    /// `ZADD schedule <at> <json>`: `at` is in the future. Stored without `at` and
    /// `enqueued_at`.
    Scheduled {
        /// Run-at time, epoch seconds.
        at: f64,
        /// The job's JSON.
        json: String,
    },
    /// `LPUSH queue:<queue> <json>` (with `SADD queues <queue>`), `enqueued_at` set.
    Queued {
        /// The queue's name.
        queue: String,
        /// The job's JSON.
        json: String,
    },
}

impl Stored {
    /// Encodes `job`: scheduled if its `at` is after `now` (epoch seconds), else queued with
    /// `enqueued_at` (epoch milliseconds).
    pub(crate) fn encode(mut job: JobPayload, now: f64, enqueued_at: i64) -> Result<Self> {
        match job.at.take() {
            Some(at) if at > now => {
                job.enqueued_at = None;
                Ok(Self::Scheduled {
                    at,
                    json: job.to_json()?,
                })
            }
            _ => {
                job.enqueued_at = Some(enqueued_at);
                let json = job.to_json()?;
                Ok(Self::Queued {
                    queue: job.queue,
                    json,
                })
            }
        }
    }
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mode = match self.inner.mode {
            ClientMode::Redis => "redis",
            ClientMode::Fake(_) => "fake",
            ClientMode::Inline(_) => "inline",
            ClientMode::Collect(_) => "collect",
        };
        f.debug_struct("Client")
            .field("mode", &mode)
            .field("redis", &self.inner.redis)
            .field("middleware", &self.inner.middleware)
            .finish_non_exhaustive()
    }
}

fn invalid(message: &str) -> Error {
    Error::InvalidJob(message.to_owned())
}

/// The jids of pushes, `None` for jobs not pushed (duplicates, stopped by middleware).
fn jids(outcomes: Vec<PushOutcome>) -> Vec<Option<String>> {
    outcomes
        .into_iter()
        .map(|outcome| outcome.jid().map(str::to_owned))
        .collect()
}

fn validate_retry_for(retry_for: Option<u64>) -> Result<()> {
    match retry_for {
        Some(secs) if secs > MAX_RETRY_FOR_SECS => Err(invalid(
            "retry_for must be a relative amount of time (in seconds, below 1e9), e.g. 48 hours",
        )),
        _ => Ok(()),
    }
}

/// The latest time a job may run at: 9999-12-31T23:59:59Z, in seconds since the Unix epoch.
/// Later times cannot be shown as dates (by the dashboard, the JSON API, the CLI), and a unique
/// job's lock would outlive any expiry Redis accepts. Retries are scheduled no later either.
pub(crate) const MAX_AT: f64 = 253_402_300_799.0;

fn validate_at(at: Option<f64>) -> Result<()> {
    match at {
        Some(at) if !at.is_finite() => Err(invalid("Job 'at' must be a finite Unix timestamp")),
        Some(at) if at > MAX_AT => Err(invalid(
            "Job 'at' must be a time before the year 10000 (a Unix timestamp of at most \
             253402300799)",
        )),
        _ => Ok(()),
    }
}

fn strip_reserved_extra(payload: &mut JobPayload) {
    let removed = payload.remove_reserved_extra();
    if removed > 0 {
        tracing::warn!(
            class = %payload.class,
            removed,
            "dropped extra payload keys that name payload fields"
        );
    }
}

/// Validates a job again after client middleware, which may have changed it.
fn revalidate(mut payload: JobPayload) -> Result<JobPayload> {
    if payload.class.is_empty() {
        return Err(invalid("client middleware left the job without a class"));
    }
    if payload.queue.is_empty() {
        return Err(invalid("client middleware left the job without a queue"));
    }
    validate_retry_for(payload.retry_for)?;
    validate_at(payload.at)?;
    if payload.jid.is_empty() {
        payload.jid = generate_jid();
    }
    strip_reserved_extra(&mut payload);
    Ok(payload)
}

/// When the jobs of a [`BulkPush`] run: epoch seconds for all of them, or one per job.
#[derive(Debug, Clone, PartialEq)]
pub enum BulkAt {
    /// Every job runs at this time.
    All(f64),
    /// Job `i` runs at element `i` (the list must be as long as the arguments).
    Each(Vec<f64>),
}

/// Many jobs of one class pushed by [`Client::push_bulk`] (Sidekiq's `push_bulk`).
///
/// * `template` supplies every field except the arguments: class, queue, retry, tags, ...
///   An explicit `jid` is only allowed when pushing a single job.
/// * `args` holds one argument list per job.
/// * `at` schedules the jobs (one time for all, or one per job); a time that is not in the
///   future pushes immediately. `spread_interval` instead schedules each job at a random time
///   within that interval from now (at least 5 s); the two are mutually exclusive.
/// * Jobs are pushed in batches of `batch_size` (default 1000, or 100 when scheduled), one
///   round trip per batch. Client middleware runs for every job.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct BulkPush {
    /// Fields shared by every job. Its `args` are ignored and its `at` must be unset (use
    /// [`BulkPush::at`] and friends).
    pub template: JobPayload,
    /// One argument list per job.
    pub args: Vec<Vec<Value>>,
    /// When the jobs run.
    pub at: Option<BulkAt>,
    /// Spread the jobs randomly over this interval from now.
    pub spread_interval: Option<Duration>,
    /// Jobs per round trip.
    pub batch_size: Option<usize>,
    /// The keys typed pushes computed per job ([`Job::unique_key`], [`Job::throttle_key`]):
    /// empty, or one entry per argument list.
    pub(crate) keys: Vec<TypedKeys>,
}

impl BulkPush {
    /// Jobs of `class`, one per argument list.
    pub fn new(class: impl Into<String>, args: Vec<Vec<Value>>) -> Self {
        Self::from_template(JobPayload::new(class, Vec::new()), args)
    }

    /// Jobs copying every field of `template` except `args`.
    pub fn from_template(template: JobPayload, args: Vec<Vec<Value>>) -> Self {
        Self {
            template,
            args,
            at: None,
            spread_interval: None,
            batch_size: None,
            keys: Vec::new(),
        }
    }

    /// Pushes the jobs to `queue`.
    #[must_use]
    pub fn queue(mut self, queue: impl Into<String>) -> Self {
        self.template.queue = queue.into();
        self
    }

    /// Sets the retry policy of every job.
    #[must_use]
    pub fn retry(mut self, retry: impl Into<RetryOption>) -> Self {
        self.template.retry = Some(retry.into());
        self
    }

    /// Sets the backtrace option of every job.
    #[must_use]
    pub fn backtrace(mut self, backtrace: impl Into<BacktraceOption>) -> Self {
        self.template.backtrace = Some(backtrace.into());
        self
    }

    /// Overlays `options` on the template: every option they set replaces the template's value
    /// (custom keys are added or replaced), as [`JobSetter::options`](crate::JobSetter::options)
    /// and [`JobBuilder::options`] do; setters called afterwards win in turn. To fill only the
    /// fields the template leaves unset, use [`BulkPush::default_options`].
    #[must_use]
    pub fn options(mut self, options: &JobOptions) -> Self {
        options.apply_overrides(&mut self.template);
        self
    }

    /// Fills the fields the template leaves unset from `options`: what is already set stays
    /// (for example, a job type's options under the template's explicit fields).
    #[must_use]
    pub fn default_options(mut self, options: &JobOptions) -> Self {
        options.apply_defaults(&mut self.template);
        self
    }

    /// Uses this jid (only valid when pushing a single job).
    #[must_use]
    pub fn jid(mut self, jid: impl Into<String>) -> Self {
        self.template.jid = jid.into();
        self
    }

    /// Runs every job at `at`.
    #[must_use]
    pub fn at(mut self, at: SystemTime) -> Self {
        self.at = Some(BulkAt::All(system_time_to_secs(at)));
        self
    }

    /// Runs every job at `at` (Unix timestamp in seconds).
    #[must_use]
    pub fn at_timestamp(mut self, at: f64) -> Self {
        self.at = Some(BulkAt::All(at));
        self
    }

    /// Runs every job `delay` from now.
    #[must_use]
    pub fn delay(mut self, delay: Duration) -> Self {
        self.at = Some(BulkAt::All(now_secs_f64() + delay.as_secs_f64()));
        self
    }

    /// Runs job `i` at `times[i]`.
    #[must_use]
    pub fn at_each(mut self, times: impl IntoIterator<Item = SystemTime>) -> Self {
        self.at = Some(BulkAt::Each(
            times.into_iter().map(system_time_to_secs).collect(),
        ));
        self
    }

    /// Runs job `i` at `times[i]` (Unix timestamps in seconds).
    #[must_use]
    pub fn at_each_timestamp(mut self, times: Vec<f64>) -> Self {
        self.at = Some(BulkAt::Each(times));
        self
    }

    /// Spreads the jobs randomly over `interval` from now.
    #[must_use]
    pub fn spread_interval(mut self, interval: Duration) -> Self {
        self.spread_interval = Some(interval);
        self
    }

    /// Pushes `size` jobs per round trip.
    #[must_use]
    pub fn batch_size(mut self, size: usize) -> Self {
        self.batch_size = Some(size);
        self
    }

    /// Applies `options` over the template (what `options` sets wins).
    fn with_options(mut self, options: JobOptions) -> Self {
        options.apply_overrides(&mut self.template);
        self
    }

    /// Sets the whole uniqueness option of every job (see [`JobOptions::unique`]).
    #[must_use]
    pub fn unique(self, unique: impl Into<UniqueOption>) -> Self {
        self.with_options(JobOptions::new().unique(unique))
    }

    /// Makes every job unique for `ttl` (see [`JobOptions::unique_for`]).
    #[must_use]
    pub fn unique_for(self, ttl: Duration) -> Self {
        self.with_options(JobOptions::new().unique_for(ttl))
    }

    /// When the locks are released (see [`JobOptions::unique_until`]).
    #[must_use]
    pub fn unique_until(self, until: UniqueUntil) -> Self {
        self.with_options(JobOptions::new().unique_until(until))
    }

    /// What a push does when another job holds a lock (see [`JobOptions::on_conflict`]); a
    /// duplicate is never an error in a bulk push.
    #[must_use]
    pub fn on_conflict(self, on_conflict: OnConflict) -> Self {
        self.with_options(JobOptions::new().on_conflict(on_conflict))
    }

    /// Whether the locks ignore the queue (see [`JobOptions::unique_across_queues`]).
    #[must_use]
    pub fn unique_across_queues(self, across: bool) -> Self {
        self.with_options(JobOptions::new().unique_across_queues(across))
    }

    /// The value identifying duplicates, the same for every job (see
    /// [`JobOptions::unique_key`]).
    #[must_use]
    pub fn unique_key(self, key: impl Into<Value>) -> Self {
        self.with_options(JobOptions::new().unique_key(key))
    }

    /// Not unique, whatever lower layers say.
    #[doc(alias = "no_unique")]
    #[must_use]
    pub fn not_unique(self) -> Self {
        self.with_options(JobOptions::new().not_unique())
    }

    /// The throttle of every job (see [`JobOptions::throttle`]).
    #[must_use]
    pub fn throttle(self, name: impl Into<String>) -> Self {
        self.with_options(JobOptions::new().throttle(name))
    }

    /// The throttle key of every job (see [`JobOptions::throttle_key`]).
    #[must_use]
    pub fn throttle_key(self, key: impl Into<String>) -> Self {
        self.with_options(JobOptions::new().throttle_key(key))
    }

    /// Not throttled, whatever lower layers say.
    #[doc(alias = "not_throttled")]
    #[must_use]
    pub fn no_throttle(self) -> Self {
        self.with_options(JobOptions::new().no_throttle())
    }
}

/// A typed push builder bound to a client, created by [`Client::job`].
pub struct JobBuilder<'c, J: Job> {
    client: &'c Client,
    setter: JobSetter<J>,
}

impl<'c, J: Job> JobBuilder<'c, J> {
    fn map(mut self, update: impl FnOnce(JobSetter<J>) -> JobSetter<J>) -> Self {
        self.setter = update(self.setter);
        self
    }

    /// Pushes to this queue.
    #[must_use]
    pub fn queue(self, queue: impl Into<String>) -> Self {
        self.map(|setter| setter.queue(queue))
    }

    /// Sets the retry policy: `true`, `false` or a maximum number of retries.
    #[must_use]
    pub fn retry(self, retry: impl Into<RetryOption>) -> Self {
        self.map(|setter| setter.retry(retry))
    }

    /// Sets whether to store error backtraces: `true`, `false` or a number of lines.
    #[must_use]
    pub fn backtrace(self, backtrace: impl Into<BacktraceOption>) -> Self {
        self.map(|setter| setter.backtrace(backtrace))
    }

    /// `false` keeps the job out of the morgue when its retries are exhausted.
    #[must_use]
    pub fn dead(self, dead: bool) -> Self {
        self.map(|setter| setter.dead(dead))
    }

    /// Sets the tags.
    #[must_use]
    pub fn tags<I, S>(self, tags: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.map(|setter| setter.tags(tags))
    }

    /// Retries for this long after the first failure.
    #[must_use]
    pub fn retry_for(self, retry_for: Duration) -> Self {
        self.map(|setter| setter.retry_for(retry_for))
    }

    /// Sends retries to this queue.
    #[must_use]
    pub fn retry_queue(self, queue: impl Into<String>) -> Self {
        self.map(|setter| setter.retry_queue(queue))
    }

    /// Adds a custom payload key.
    #[must_use]
    pub fn custom(self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.map(|setter| setter.custom(key, value))
    }

    /// Overlays `options`.
    #[must_use]
    pub fn options(self, options: &JobOptions) -> Self {
        self.map(|setter| setter.options(options))
    }

    /// Uses this jid instead of generating one.
    #[must_use]
    pub fn jid(self, jid: impl Into<String>) -> Self {
        self.map(|setter| setter.jid(jid))
    }

    /// Schedules the job at `at`.
    #[must_use]
    pub fn at(self, at: SystemTime) -> Self {
        self.map(|setter| setter.at(at))
    }

    /// Schedules the job `delay` from the time of the push.
    #[must_use]
    pub fn delay(self, delay: Duration) -> Self {
        self.map(|setter| setter.delay(delay))
    }

    /// Jobs per round trip for [`perform_bulk`](Self::perform_bulk).
    #[must_use]
    pub fn batch_size(self, size: usize) -> Self {
        self.map(|setter| setter.batch_size(size))
    }

    /// Spreads the jobs of [`perform_bulk`](Self::perform_bulk) over `interval`, see
    /// [`JobSetter::spread_interval`].
    #[must_use]
    pub fn spread_interval(self, interval: Duration) -> Self {
        self.map(|setter| setter.spread_interval(interval))
    }

    /// Sets the whole uniqueness option (see [`JobOptions::unique`]).
    #[must_use]
    pub fn unique(self, unique: impl Into<UniqueOption>) -> Self {
        self.map(|setter| setter.unique(unique))
    }

    /// Makes the job unique for `ttl` (see [`JobOptions::unique_for`]).
    #[must_use]
    pub fn unique_for(self, ttl: Duration) -> Self {
        self.map(|setter| setter.unique_for(ttl))
    }

    /// When the job's lock is released (see [`JobOptions::unique_until`]).
    #[must_use]
    pub fn unique_until(self, until: UniqueUntil) -> Self {
        self.map(|setter| setter.unique_until(until))
    }

    /// What the push does when another job holds the lock (see [`JobOptions::on_conflict`]).
    #[must_use]
    pub fn on_conflict(self, on_conflict: OnConflict) -> Self {
        self.map(|setter| setter.on_conflict(on_conflict))
    }

    /// Whether the lock ignores the queue (see [`JobOptions::unique_across_queues`]).
    #[must_use]
    pub fn unique_across_queues(self, across: bool) -> Self {
        self.map(|setter| setter.unique_across_queues(across))
    }

    /// The value identifying duplicates (see [`JobOptions::unique_key`]).
    #[must_use]
    pub fn unique_key(self, key: impl Into<Value>) -> Self {
        self.map(|setter| setter.unique_key(key))
    }

    /// Not unique, whatever the job type's options say.
    #[doc(alias = "no_unique")]
    #[must_use]
    pub fn not_unique(self) -> Self {
        self.map(JobSetter::not_unique)
    }

    /// The throttle of the job's starts (see [`JobOptions::throttle`]).
    #[must_use]
    pub fn throttle(self, name: impl Into<String>) -> Self {
        self.map(|setter| setter.throttle(name))
    }

    /// The throttle key (see [`JobOptions::throttle_key`]).
    #[must_use]
    pub fn throttle_key(self, key: impl Into<String>) -> Self {
        self.map(|setter| setter.throttle_key(key))
    }

    /// Not throttled, whatever the job type's options say.
    #[doc(alias = "not_throttled")]
    #[must_use]
    pub fn no_throttle(self) -> Self {
        self.map(JobSetter::no_throttle)
    }

    /// Pushes the job and says what happened (see [`JobSetter::push_outcome`]; a duplicate of a
    /// job with [`OnConflict::Error`] is the error [`Error::DuplicateJob`]).
    pub async fn push_outcome(&self, args: J::Args) -> Result<PushOutcome> {
        self.setter.push_outcome(self.client, args).await
    }

    /// Pushes one job per element of `args`, with the outcome of each (see
    /// [`JobSetter::push_bulk_outcomes`]).
    pub async fn push_bulk_outcomes(&self, args: Vec<J::Args>) -> Result<Vec<PushOutcome>> {
        self.setter.push_bulk_outcomes(self.client, args).await
    }

    /// Pushes the job. Returns its jid, or `None` if client middleware stopped the push. An
    /// error after a timeout may come with the job pushed all the same: see
    /// [when a push fails](Client#when-a-push-fails).
    pub async fn perform_async(&self, args: J::Args) -> Result<Option<String>> {
        self.setter.perform_async(self.client, args).await
    }

    /// Pushes the job to run after `delay` (see [`JobBuilder::perform_async`]).
    pub async fn perform_in(&self, delay: Duration, args: J::Args) -> Result<Option<String>> {
        self.setter.perform_in(self.client, delay, args).await
    }

    /// Pushes the job to run at `at` (see [`JobBuilder::perform_async`]).
    pub async fn perform_at(&self, at: SystemTime, args: J::Args) -> Result<Option<String>> {
        self.setter.perform_at(self.client, at, args).await
    }

    /// Pushes one job per element of `args` (see [`Client::push_bulk`]).
    pub async fn perform_bulk(&self, args: Vec<J::Args>) -> Result<Vec<Option<String>>> {
        self.setter.perform_bulk(self.client, args).await
    }

    /// Pushes one job per `(args, time)` pair, each at its own time, see
    /// [`JobSetter::perform_bulk_at`].
    pub async fn perform_bulk_at(
        &self,
        jobs: Vec<(J::Args, SystemTime)>,
    ) -> Result<Vec<Option<String>>> {
        self.setter.perform_bulk_at(self.client, jobs).await
    }

    /// The underlying setter.
    pub fn setter(&self) -> &JobSetter<J> {
        &self.setter
    }
}

impl<J: Job> fmt::Debug for JobBuilder<'_, J> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JobBuilder")
            .field("setter", &self.setter)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::payload::RetryOption;

    fn client() -> Client {
        Client::with_mode(RedisPool::detached(), ClientMode::Redis)
    }

    #[test]
    fn normalize_fills_defaults() {
        let payload = client()
            .normalize(JobPayload::new("A", vec![]), None)
            .unwrap();
        assert_eq!(payload.queue, "default");
        assert_eq!(payload.retry, Some(RetryOption::Bool(true)));
        assert_eq!(payload.jid.len(), 24);
        assert!(payload.created_at.is_some());
    }

    #[test]
    fn normalize_precedence() {
        let class = JobOptions::new().queue("class").retry(12);
        let mut payload = JobPayload::new("A", vec![]);
        payload.queue = "explicit".into();
        let payload = client().normalize(payload, Some(&class)).unwrap();
        assert_eq!(payload.queue, "explicit");
        assert_eq!(payload.retry, Some(RetryOption::Count(12)));
    }

    #[test]
    fn normalize_preserves_jid_and_created_at() {
        let mut payload = JobPayload::new("A", vec![]);
        payload.jid = "abc123".into();
        payload.created_at = Some(123);
        let payload = client().normalize(payload, None).unwrap();
        assert_eq!(payload.jid, "abc123");
        assert_eq!(payload.created_at, Some(123));
    }

    #[test]
    fn normalize_rejects_invalid_jobs() {
        let c = client();
        assert!(matches!(
            c.normalize(JobPayload::new("", vec![]), None),
            Err(Error::InvalidJob(_))
        ));

        let empty_queue = JobOptions::new().queue("");
        assert!(matches!(
            c.normalize(JobPayload::new("A", vec![]), Some(&empty_queue)),
            Err(Error::InvalidJob(ref m)) if m.contains("queue")
        ));
        // Default options are merged: only an explicitly empty default queue leaves a job
        // without a queue.
        let no_default_queue = c.with_default_job_options(JobOptions::new().queue(""));
        assert!(matches!(
            no_default_queue.normalize(JobPayload::new("A", vec![]), None),
            Err(Error::InvalidJob(_))
        ));
        let merged = c.with_default_job_options(JobOptions::new().backtrace(true));
        let job = merged
            .normalize(JobPayload::new("A", vec![]), None)
            .unwrap();
        assert_eq!(job.queue, "default");
        assert_eq!(job.backtrace, Some(true.into()));

        let mut payload = JobPayload::new("A", vec![]);
        payload.retry_for = Some(2_000_000_000);
        assert!(
            matches!(c.normalize(payload, None), Err(Error::InvalidJob(ref m)) if m.contains("retry_for"))
        );
        let mut payload = JobPayload::new("A", vec![]);
        payload.retry_for = Some(1_000);
        assert!(c.normalize(payload, None).is_ok());

        for at in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let mut payload = JobPayload::new("A", vec![]);
            payload.at = Some(at);
            assert!(matches!(
                c.normalize(payload, None),
                Err(Error::InvalidJob(_))
            ));
        }
    }

    #[test]
    fn normalize_strips_reserved_extra_keys() {
        let mut payload = JobPayload::new("A", vec![]);
        payload.extra.insert("jid".into(), Value::from("shadow"));
        payload.extra.insert("locale".into(), Value::from("fr"));
        let payload = client().normalize(payload, None).unwrap();
        assert!(!payload.extra.contains_key("jid"));
        assert!(payload.extra.contains_key("locale"));
    }

    #[test]
    fn revalidate_checks_middleware_output() {
        let mut payload = JobPayload::new("A", vec![]);
        payload.queue = "q".into();
        let fixed = revalidate(payload.clone()).unwrap();
        assert_eq!(fixed.jid.len(), 24);
        payload.queue.clear();
        assert!(revalidate(payload).is_err());
    }

    #[test]
    fn bulk_builder() {
        let bulk = BulkPush::new("A", vec![vec![]])
            .queue("q")
            .retry(false)
            .backtrace(2_u32)
            .jid("j")
            .default_options(&JobOptions::new().queue("ignored").dead(false))
            .at_timestamp(5.0)
            .batch_size(7)
            .spread_interval(Duration::from_secs(9));
        assert_eq!(bulk.template.queue, "q");
        assert_eq!(bulk.template.retry, Some(RetryOption::Bool(false)));
        assert_eq!(bulk.template.backtrace, Some(BacktraceOption::Lines(2)));
        assert_eq!(bulk.template.dead, Some(false));
        assert_eq!(bulk.template.jid, "j");
        assert_eq!(bulk.at, Some(BulkAt::All(5.0)));
        assert_eq!(bulk.batch_size, Some(7));
        assert_eq!(bulk.spread_interval, Some(Duration::from_secs(9)));
        let each = BulkPush::new("A", vec![]).at_each([SystemTime::UNIX_EPOCH]);
        assert_eq!(each.at, Some(BulkAt::Each(vec![0.0])));

        // `options` overlays: what it sets wins, the rest stays.
        let overlaid = bulk.options(&JobOptions::new().queue("wins").custom("k", 1));
        assert_eq!(overlaid.template.queue, "wins");
        assert_eq!(overlaid.template.retry, Some(RetryOption::Bool(false)));
        assert_eq!(overlaid.template.extra["k"], 1);
    }

    #[test]
    fn stored_jobs_are_scheduled_or_queued() {
        let mut job = JobPayload::new("A", vec![Value::from(1)]);
        job.queue = "q".to_owned();
        job.jid = "j".to_owned();
        job.enqueued_at = Some(1);

        // In the future: scheduled, without `at` and `enqueued_at` in the stored JSON.
        let mut scheduled = job.clone();
        scheduled.at = Some(100.5);
        let Stored::Scheduled { at, json } = Stored::encode(scheduled, 100.0, 7).unwrap() else {
            panic!("expected a scheduled job");
        };
        assert_eq!(at, 100.5);
        let stored = JobPayload::from_json(&json).unwrap();
        assert_eq!((stored.at, stored.enqueued_at), (None, None));
        assert_eq!(stored.jid, "j");

        // Due (or without `at`): queued now, with `enqueued_at`.
        for at in [Some(100.0), Some(5.0), None] {
            let mut due = job.clone();
            due.at = at;
            let Stored::Queued { queue, json } = Stored::encode(due, 100.0, 7).unwrap() else {
                panic!("expected a queued job");
            };
            assert_eq!(queue, "q");
            let stored = JobPayload::from_json(&json).unwrap();
            assert_eq!((stored.at, stored.enqueued_at), (None, Some(7)));
        }
    }

    #[test]
    fn client_variants() {
        let fake = Client::fake();
        assert!(fake.is_fake() && !fake.is_inline());
        assert!(fake.redis().is_detached());
        let inline = Client::inline(Registry::new());
        assert!(inline.is_inline());
        assert!(format!("{inline:?}").contains("inline"));
        let options = JobOptions::new().queue("x");
        let custom = fake.with_default_job_options(options.clone());
        assert_eq!(
            custom.default_job_options(),
            &JobOptions::defaults().merge(&options)
        );
        assert_eq!(custom.default_job_options().queue.as_deref(), Some("x"));
        assert_eq!(fake.default_job_options(), &JobOptions::defaults());
        let other = fake.with_redis(RedisPool::detached());
        assert!(other.is_fake());
    }
}
