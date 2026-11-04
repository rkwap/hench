//! Process configuration: [`Hench::builder`] returns a [`HenchBuilder`]; its
//! [`build`](HenchBuilder::build) validates everything, connects to Redis and returns a
//! [`Hench`] holding the immutable [`Config`], the Redis pool, the job [`Registry`] and a
//! configured [`Client`].

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use futures::future::BoxFuture;

use crate::batch::BatchDefaults;
use crate::client::Client;
use crate::error::{Error, JobError, Result, panic_message};
use crate::job::{Data, ErasedJob, Job, Registry};
use crate::limit::{Backoff, Limit, OverLimit, Throttle};
use crate::middleware::{ClientChain, ClientMiddleware, ServerChain, ServerMiddleware};
use crate::options::JobOptions;
use crate::payload::JobPayload;
use crate::redis::{RedisConfig, RedisPool};
use crate::systemd::{Notifications, Notifier};

/// Called for every error hench reports: job failures, fetch and heartbeat errors, panicking
/// handlers, ... (Sidekiq's `error_handlers`).
pub type ErrorHandler = Arc<dyn Fn(&JobError, &ErrorContext<'_>) + Send + Sync>;

/// Called when a job dies: its retries are exhausted, it was killed or discarded, or it
/// failed with retries disabled (Sidekiq's `death_handlers`).
pub type DeathHandler = Arc<dyn Fn(&JobPayload, &JobError) + Send + Sync>;

/// Recognizes job errors that are a limiter's refusal in disguise (a client library's
/// HTTP 429 error carrying `Retry-After`): see [`HenchBuilder::over_limit_when`].
pub type OverLimitClassifier = Arc<dyn Fn(&JobError) -> Option<OverLimit> + Send + Sync>;

/// An asynchronous lifecycle callback, see [`LifecycleEvent`].
pub type LifecycleCallback = Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;

/// A fallible startup callback ([`HenchBuilder::on_startup_try`]): an error stops the start.
pub type StartupCheck = Arc<dyn Fn() -> BoxFuture<'static, Result<(), JobError>> + Send + Sync>;

/// Filters the backtrace lines stored with a failed job ([`HenchBuilder::backtrace_cleaner`],
/// Sidekiq's `backtrace_cleaner`).
pub type BacktraceCleaner = Arc<dyn Fn(Vec<String>) -> Vec<String> + Send + Sync>;

/// Points in a process's life at which callbacks run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[non_exhaustive]
pub enum LifecycleEvent {
    /// The process is starting, before it registers or fetches anything. A startup callback
    /// that panics stops the start (see [`HenchBuilder::on_startup`]).
    Startup,
    /// The process stopped fetching new jobs.
    Quiet,
    /// The process is shutting down (waiting for running jobs).
    Shutdown,
    /// The process finished shutting down.
    Exit,
    /// A heartbeat found the process's heartbeat key missing: the first heartbeat, or one after
    /// the key expired or Redis lost it (a restart without persistence, a failover). An outage
    /// shorter than the key's 60 s expiry, after which Redis still has the key, does not fire
    /// it.
    Heartbeat,
    /// Every heartbeat.
    Beat,
}

impl LifecycleEvent {
    /// Every event.
    pub const ALL: [LifecycleEvent; 6] = [
        Self::Startup,
        Self::Quiet,
        Self::Shutdown,
        Self::Exit,
        Self::Heartbeat,
        Self::Beat,
    ];

    /// Lowercase name of the event.
    pub fn name(self) -> &'static str {
        match self {
            Self::Startup => "startup",
            Self::Quiet => "quiet",
            Self::Shutdown => "shutdown",
            Self::Exit => "exit",
            Self::Heartbeat => "heartbeat",
            Self::Beat => "beat",
        }
    }
}

impl fmt::Display for LifecycleEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// Describes where an error reported to the error handlers happened.
#[derive(Debug, Clone, Copy, Default)]
#[non_exhaustive]
pub struct ErrorContext<'a> {
    /// What hench was doing, e.g. `"job failed"` (a job's failure), `"could not fetch jobs"`,
    /// `"could not enqueue scheduled jobs"`; [DESIGN.md] §5 lists them all.
    ///
    /// [DESIGN.md]: https://github.com/rkwap/hench/blob/v0.1.0/docs/DESIGN.md
    pub context: &'a str,
    /// The job involved, if any.
    pub job: Option<&'a JobPayload>,
    /// The raw job JSON, when the job could not be parsed.
    pub jobstr: Option<&'a str>,
    /// The queue involved, if any.
    pub queue: Option<&'a str>,
    /// The lifecycle event whose callback failed, if any.
    pub event: Option<LifecycleEvent>,
}

impl<'a> ErrorContext<'a> {
    /// A context described by `context`.
    pub fn new(context: &'a str) -> Self {
        Self {
            context,
            ..Self::default()
        }
    }

    /// Adds the job.
    #[must_use]
    pub fn with_job(mut self, job: &'a JobPayload) -> Self {
        self.job = Some(job);
        self
    }

    /// Adds the raw job JSON.
    #[must_use]
    pub fn with_jobstr(mut self, jobstr: &'a str) -> Self {
        self.jobstr = Some(jobstr);
        self
    }

    /// Adds the queue.
    #[must_use]
    pub fn with_queue(mut self, queue: &'a str) -> Self {
        self.queue = Some(queue);
        self
    }

    /// Adds the lifecycle event.
    #[must_use]
    pub fn with_event(mut self, event: LifecycleEvent) -> Self {
        self.event = Some(event);
        self
    }
}

/// How workers take jobs from Redis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum FetchStrategy {
    /// `BRPOP`: a job held by a process that crashes is lost.
    Basic,
    /// Jobs move atomically to a per-process in-progress list and are recovered if the
    /// process dies (the default).
    #[default]
    Reliable,
}

/// How a capsule orders its queues.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum QueueMode {
    /// Always in the configured order (every weight is 0).
    Strict,
    /// Random order on every fetch, biased by weight.
    Weighted,
    /// Uniformly random order on every fetch (every weight is 1).
    Random,
}

impl QueueMode {
    /// Lowercase name (as reported in the heartbeat `info`).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Strict => "strict",
            Self::Weighted => "weighted",
            Self::Random => "random",
        }
    }
}

/// A set of workers processing some queues with a given concurrency (Sidekiq's `Capsule`).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct CapsuleConfig {
    /// Unique name; the default capsule is called `default`.
    pub name: String,
    /// Number of jobs processed at once.
    pub concurrency: usize,
    /// Queue names in configured (priority) order, without duplicates.
    pub queues: Vec<String>,
    /// Weight of each queue, in the same order (0 for strictly ordered queues).
    pub weights: Vec<(String, u32)>,
    /// Ordering mode derived from the weights.
    pub mode: QueueMode,
}

impl CapsuleConfig {
    /// Name of the capsule configured by [`HenchBuilder::concurrency`] and
    /// [`HenchBuilder::queues`].
    pub const DEFAULT_NAME: &'static str = "default";

    /// Largest accepted queue weight. Weighted ordering expands each queue `weight` times, so
    /// huge weights only cost memory and time; ratios between small weights work as well.
    pub const MAX_WEIGHT: u32 = 1_000;

    /// The queue list used for weighted ordering: every queue repeated `max(weight, 1)`
    /// times. Shuffling it and removing duplicates yields a weighted random order.
    pub fn expanded_queues(&self) -> Vec<&str> {
        self.weights
            .iter()
            .flat_map(|(name, weight)| {
                std::iter::repeat_n(name.as_str(), (*weight).max(1) as usize)
            })
            .collect()
    }
}

/// Configures one capsule, see [`HenchBuilder::capsule`].
#[derive(Debug, Clone)]
pub struct CapsuleBuilder {
    name: String,
    concurrency: Option<usize>,
    queues: Option<Vec<(String, u32)>>,
    errors: Vec<String>,
}

impl CapsuleBuilder {
    fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            concurrency: None,
            queues: None,
            errors: Vec::new(),
        }
    }

    /// The capsule's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Number of jobs processed at once (default 5).
    #[must_use]
    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = Some(concurrency);
        self
    }

    /// Queues to process. Plain names are processed in strict priority order; a name may
    /// carry a weight after a comma (`"critical,3"`), as in Sidekiq's configuration files.
    #[must_use]
    pub fn queues<I, S>(mut self, queues: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut parsed = Vec::new();
        for queue in queues {
            let queue: String = queue.into();
            match queue.split_once(',') {
                None => parsed.push((queue, 0)),
                Some((name, weight)) => match weight.trim().parse::<u32>() {
                    Ok(weight) => parsed.push((name.trim().to_owned(), weight)),
                    Err(_) => self.errors.push(format!(
                        "invalid queue weight in `{queue}` (capsule `{}`)",
                        self.name
                    )),
                },
            }
        }
        self.queues = Some(parsed);
        self
    }

    /// Queues with weights: on each fetch a queue is checked first with probability
    /// `weight / total`. Weights of 1 everywhere give a uniformly random order; weights of 0
    /// everywhere give strict order.
    #[must_use]
    pub fn weighted_queues<I, S>(mut self, queues: I) -> Self
    where
        I: IntoIterator<Item = (S, u32)>,
        S: Into<String>,
    {
        self.queues = Some(
            queues
                .into_iter()
                .map(|(name, weight)| (name.into(), weight))
                .collect(),
        );
        self
    }

    fn build(&self) -> Result<CapsuleConfig> {
        if self.name.is_empty() {
            return Err(Error::Config("capsule names must not be empty".to_owned()));
        }
        if let Some(error) = self.errors.first() {
            return Err(Error::Config(error.clone()));
        }
        let concurrency = self.concurrency.unwrap_or(Config::DEFAULT_CONCURRENCY);
        if concurrency == 0 {
            return Err(Error::Config(format!(
                "the concurrency of capsule `{}` must be at least 1",
                self.name
            )));
        }
        let weights = self
            .queues
            .clone()
            .unwrap_or_else(|| vec![("default".to_owned(), 0)]);
        if weights.is_empty() {
            return Err(Error::Config(format!(
                "capsule `{}` must process at least one queue",
                self.name
            )));
        }
        let mut seen = HashSet::new();
        for (queue, _) in &weights {
            if queue.is_empty() {
                return Err(Error::Config(format!(
                    "queue names must not be empty (capsule `{}`)",
                    self.name
                )));
            }
            if !seen.insert(queue.as_str()) {
                return Err(Error::Config(format!(
                    "queue `{queue}` is listed twice in capsule `{}`",
                    self.name
                )));
            }
        }
        if let Some((queue, weight)) = weights
            .iter()
            .find(|(_, weight)| *weight > CapsuleConfig::MAX_WEIGHT)
        {
            return Err(Error::Config(format!(
                "queue `{queue}` in capsule `{}` has weight {weight}; weights above {} are not \
                 supported (only their ratios matter)",
                self.name,
                CapsuleConfig::MAX_WEIGHT
            )));
        }
        let mode = if weights.iter().all(|(_, weight)| *weight == 0) {
            QueueMode::Strict
        } else if weights.iter().all(|(_, weight)| *weight == 1) {
            QueueMode::Random
        } else {
            QueueMode::Weighted
        };
        Ok(CapsuleConfig {
            name: self.name.clone(),
            concurrency,
            queues: weights.iter().map(|(queue, _)| queue.clone()).collect(),
            weights,
            mode,
        })
    }
}

/// The configuration of a hench process. Built by [`HenchBuilder`]; immutable once built.
///
/// Every duration is configurable so that tests can use short intervals.
#[non_exhaustive]
pub struct Config {
    /// Capsules, the default one first.
    pub capsules: Vec<CapsuleConfig>,
    /// Error handlers (initially one that logs through `tracing`).
    pub error_handlers: Vec<ErrorHandler>,
    /// Death handlers.
    pub death_handlers: Vec<DeathHandler>,
    /// Lifecycle callbacks by event, in registration order.
    pub lifecycle: HashMap<LifecycleEvent, Vec<LifecycleCallback>>,
    /// Labels shown in the dashboard.
    pub labels: Vec<String>,
    /// Tag shown in the dashboard (Sidekiq uses the application directory name).
    pub tag: Option<String>,
    /// Whether each job execution is logged (start/done/fail).
    pub job_logging: bool,
    /// Payload keys recorded, as a JSON object, in the `attrs` field of every job's span, so
    /// that every line logged while the job runs carries them (Sidekiq's
    /// `logged_job_attributes`; `bid` and `tags` have fields of their own). Default none.
    pub logged_job_attributes: Vec<String>,
    /// Filters the backtrace lines stored with a failed job whose `backtrace` option is on,
    /// before the option's line limit applies. Default none.
    pub backtrace_cleaner: Option<BacktraceCleaner>,
    /// Fallible startup callbacks ([`HenchBuilder::on_startup_try`]), run before the `startup`
    /// callbacks.
    pub startup_checks: Vec<StartupCheck>,
    /// Whether workers record execution metrics ([`crate::metrics`]). Default `true`.
    pub metrics: bool,
    /// Whether the process notifies systemd ([`crate::systemd`]). Default: when systemd set
    /// `NOTIFY_SOCKET`.
    pub systemd: Notifications,
    /// How jobs are fetched.
    pub fetch: FetchStrategy,
    /// Options applied to jobs that set neither them nor their type's options.
    pub default_job_options: JobOptions,
    /// Retries of jobs with `retry: true`. Default 25.
    pub max_retries: u32,
    /// Size limit of the morgue. Default 10 000.
    pub dead_max_jobs: usize,
    /// Age limit of morgue entries. Default 180 days.
    pub dead_timeout: Duration,
    /// How long shutdown waits for running jobs before requeueing them. Default 25 s.
    pub timeout: Duration,
    /// How long an [iterable job](crate::iterable) may iterate in one execution before it
    /// saves its cursor and goes back to the end of its queue. Default `None` (no limit).
    pub max_iteration_runtime: Option<Duration>,
    /// Fixed average scheduler poll interval. `None` (the default) scales
    /// [`average_scheduled_poll_interval`](Self::average_scheduled_poll_interval) by the
    /// number of processes.
    pub poll_interval_average: Option<Duration>,
    /// Target interval between scheduler polls across all processes. Default 5 s.
    pub average_scheduled_poll_interval: Duration,
    /// Interval between heartbeats. Default 10 s, at most
    /// [`Config::MAX_HEARTBEAT_INTERVAL`].
    pub heartbeat_interval: Duration,
    /// Wait before the first scheduler poll (skipped when `poll_interval_average` is set).
    /// Default 10 s.
    pub initial_scheduler_wait: Duration,
    /// Random extra wait (up to this long) before the first scheduler poll. Default 5 s.
    pub initial_scheduler_jitter: Duration,
    /// How long a fetch blocks waiting for a job (`BRPOP`/`BLMOVE` timeout). Default 2 s.
    pub fetch_timeout: Duration,
    /// Interval between checks for jobs orphaned by dead processes (reliable fetch). Default
    /// 60 s ([`Config::DEFAULT_ORPHAN_CHECK_INTERVAL`]): the jobs of a process that died are
    /// back in their queues about 1.5 to 2.5 minutes later (its heartbeat key expires within
    /// 60 s, the next check finds it gone, then waits the [`orphan_grace`](Self::orphan_grace)).
    /// The cluster checks once per interval whatever the number of processes (one `SET NX` per
    /// process and interval; the process that takes the lock reads `inprogress` and the
    /// heartbeats of its members).
    pub orphan_check_interval: Duration,
    /// How long a process found without a heartbeat key by an orphan check must stay without
    /// one before its in-progress jobs are pushed back to their queues (reliable fetch).
    /// Default 30 s ([`Config::DEFAULT_ORPHAN_GRACE`]); never less than twice
    /// [`heartbeat_interval`](Self::heartbeat_interval).
    ///
    /// A key that expired means the process died, but Redis can also lose the keys of live
    /// processes (a restart without persistence, an old snapshot, `FLUSHALL`): they re-create
    /// theirs within a heartbeat interval, so the grace must exceed the heartbeat interval of
    /// every process sharing the Redis (at most [`Config::MAX_HEARTBEAT_INTERVAL`], which the
    /// default covers). A shorter grace recovers the jobs of crashed processes sooner.
    pub orphan_grace: Duration,
    /// How many times a job may be recovered from processes that died while they held it
    /// (reliable fetch) before it is taken for a *poison pill*, a job that kills the processes
    /// running it: past this many recoveries within [`RECOVERY_WINDOW`](crate::fetch::RECOVERY_WINDOW)
    /// of each other, the next recovery moves the job to the morgue (or drops it, for `dead:
    /// false`) with the error class [`JobError::CRASH_LOOP`](crate::JobError::CRASH_LOOP),
    /// instead of pushing it back to crash another process. A job recovered before comes back
    /// alone (one per orphan check), so that the jobs running beside a poison pill are told
    /// apart from it; several jobs of one crash reaching the limit together each get one
    /// recovery more, alone. Default 3 ([`Config::DEFAULT_MAX_RECOVERIES`]); 0 never runs a job
    /// again after its process died; `u32::MAX` never gives up. See [`crate::fetch`].
    pub max_recoveries: u32,
    /// Server middleware.
    pub server_middleware: ServerChain,
    /// Client middleware (also used by the scheduler when it re-pushes jobs).
    pub client_middleware: ClientChain,
    /// Shared state available to jobs through [`JobContext::data`](crate::JobContext::data).
    pub data: Arc<Data>,
    /// Whether pushes honour uniqueness ([`crate::unique`]). Default `true`. The engine always
    /// releases and holds the locks of jobs that carry one, whatever this says.
    pub unique_jobs: bool,
    /// Code-defined periodic schedules ([`crate::periodic`]).
    #[cfg(feature = "periodic")]
    pub periodic: Vec<crate::periodic::Periodic>,
    /// The group whose crontab this process registers and prunes. Default `None` (the group
    /// `default`, an empty crontab pruning nothing).
    pub periodic_group: Option<String>,
    /// Whether this process runs the periodic poller. Default `true`.
    pub periodic_poller: bool,
    /// Longest wait between two looks at the periodic schedules. Default 5 s.
    pub periodic_poll_interval: Duration,
    /// Enqueues remembered per periodic schedule. Default 10.
    pub periodic_history: usize,
    /// Lease of a due periodic schedule while a poller fires it. Default 60 s, at least twice
    /// [`periodic_poll_interval`](Self::periodic_poll_interval).
    pub periodic_lease: Duration,
    /// Interval between checks that this process's crontab is registered. Default 60 s.
    pub periodic_check_interval: Duration,
    /// Expiry of an unfinished batch after its last activity. Default 30 days, at least 1 hour.
    pub batch_ttl: Duration,
    /// How long a finished batch's data stays. Default 24 hours, at least 30 s.
    pub batch_linger: Duration,
    /// Most members of one batch commit, each child batch counting as 10. Default 10 000, from
    /// 1 to 100 000.
    pub batch_max_commit_jobs: usize,
    /// How long a batch commit whose outcome is unknown is sent again. Default 30 s.
    pub batch_commit_timeout: Duration,
    /// Whether this process runs batch maintenance. Default `true`.
    pub batch_maintenance: bool,
    /// Period of batch maintenance (with ±20 % jitter). Default 30 s.
    pub batch_maintenance_interval: Duration,
    /// Age of an undispatched batch event before maintenance dispatches it. Default 10 s.
    pub batch_dispatch_grace: Duration,
    /// Throttles applied to the jobs that name them ([`crate::limit`]), in definition order (a
    /// later definition of a name replaces an earlier one).
    pub throttles: Vec<Throttle>,
    /// Limits on every fetch of a queue, by queue ([`HenchBuilder::queue_limits`]): written to
    /// Redis when the process starts, replacing the queue's stored limits (code wins over
    /// run-time edits at the next start). Default none.
    pub queue_limits: Vec<(String, Vec<Limit>)>,
    /// Recognizes job errors that are a limiter's refusal in disguise. Default none.
    pub over_limit_when: Option<OverLimitClassifier>,
    /// Reschedule delay policy of limiters without their own. Default: the limit's next slot,
    /// else 60 s times the reschedule count.
    pub over_limit_backoff: Option<Backoff>,
    /// Times a job may be rescheduled by limiters before the failure path takes over. Default
    /// 20.
    pub over_limit_reschedules: u32,
    /// Longest reschedule delay. Default 24 hours.
    pub over_limit_max_delay: Duration,
}

impl Config {
    /// Default concurrency of a capsule.
    pub const DEFAULT_CONCURRENCY: usize = 5;
    /// Default `max_retries`.
    pub const DEFAULT_MAX_RETRIES: u32 = 25;
    /// Default `dead_max_jobs`.
    pub const DEFAULT_DEAD_MAX_JOBS: usize = 10_000;
    /// Default `dead_timeout` (180 days).
    pub const DEFAULT_DEAD_TIMEOUT: Duration = Duration::from_secs(180 * 24 * 60 * 60);
    /// Default shutdown `timeout`.
    pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(25);
    /// Longest `heartbeat_interval` accepted: a third of the heartbeat key's expiry
    /// ([`keys::PROCESS_TTL`](crate::keys::PROCESS_TTL), 60 s), so that a beat slowed down by
    /// Redis can be late without the process looking dead to the others (they would push its
    /// running jobs back to their queues) and remote signals, which expire after 60 s, are
    /// always read.
    pub const MAX_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(20);
    /// Default [`orphan_grace`](Self::orphan_grace): the longest heartbeat interval, plus 10 s.
    pub const DEFAULT_ORPHAN_GRACE: Duration = Duration::from_secs(30);
    /// Default [`orphan_check_interval`](Self::orphan_check_interval).
    pub const DEFAULT_ORPHAN_CHECK_INTERVAL: Duration = Duration::from_secs(60);
    /// Default [`max_recoveries`](Self::max_recoveries).
    pub const DEFAULT_MAX_RECOVERIES: u32 = 3;
    /// Default [`periodic_poll_interval`](Self::periodic_poll_interval).
    pub const DEFAULT_PERIODIC_POLL_INTERVAL: Duration = Duration::from_secs(5);
    /// Default [`periodic_history`](Self::periodic_history).
    pub const DEFAULT_PERIODIC_HISTORY: usize = 10;
    /// Default [`periodic_lease`](Self::periodic_lease).
    pub const DEFAULT_PERIODIC_LEASE: Duration = Duration::from_secs(60);
    /// Default [`periodic_check_interval`](Self::periodic_check_interval).
    pub const DEFAULT_PERIODIC_CHECK_INTERVAL: Duration = Duration::from_secs(60);
    /// Default [`batch_maintenance_interval`](Self::batch_maintenance_interval).
    pub const DEFAULT_BATCH_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(30);
    /// Default [`batch_dispatch_grace`](Self::batch_dispatch_grace).
    pub const DEFAULT_BATCH_DISPATCH_GRACE: Duration = Duration::from_secs(10);
    /// Default [`over_limit_reschedules`](Self::over_limit_reschedules).
    pub const DEFAULT_OVER_LIMIT_RESCHEDULES: u32 = 20;

    /// The batch settings of the clients this configuration builds.
    pub fn batch_defaults(&self) -> BatchDefaults {
        BatchDefaults {
            ttl: self.batch_ttl,
            linger: self.batch_linger,
            max_commit_jobs: self.batch_max_commit_jobs,
            commit_timeout: self.batch_commit_timeout,
        }
    }

    /// The [`orphan_grace`](Self::orphan_grace) in effect: at least twice the heartbeat
    /// interval, so that a live process always has a chance to re-create its key in time.
    pub fn effective_orphan_grace(&self) -> Duration {
        self.orphan_grace
            .max(self.heartbeat_interval.saturating_mul(2))
    }

    /// Sum of the capsules' concurrency.
    pub fn total_concurrency(&self) -> usize {
        self.capsules
            .iter()
            .map(|capsule| capsule.concurrency)
            .sum()
    }

    /// The capsule called `name`.
    pub fn capsule(&self, name: &str) -> Option<&CapsuleConfig> {
        self.capsules.iter().find(|capsule| capsule.name == name)
    }

    /// Every queue processed by any capsule, without duplicates, in capsule order.
    pub fn queue_names(&self) -> Vec<&str> {
        let mut seen = HashSet::new();
        self.capsules
            .iter()
            .flat_map(|capsule| capsule.queues.iter().map(String::as_str))
            .filter(|queue| seen.insert(*queue))
            .collect()
    }

    /// Callbacks registered for `event`.
    pub fn callbacks(&self, event: LifecycleEvent) -> &[LifecycleCallback] {
        self.lifecycle.get(&event).map_or(&[], Vec::as_slice)
    }

    /// Reports an error to every error handler. A panicking handler is logged and does not
    /// prevent the other handlers from running. With no handler configured, the error is
    /// logged.
    pub fn handle_error(&self, error: &JobError, ctx: &ErrorContext<'_>) {
        if self.error_handlers.is_empty() {
            log_error(error, ctx);
            return;
        }
        for handler in &self.error_handlers {
            if let Err(panic) = std::panic::catch_unwind(AssertUnwindSafe(|| handler(error, ctx))) {
                tracing::error!(
                    context = ctx.context,
                    error = %error,
                    panic = %panic_message(panic.as_ref()),
                    "an error handler panicked"
                );
            }
        }
    }

    /// Runs every death handler for `job`. A panicking handler is reported to the error
    /// handlers and does not prevent the others from running.
    pub fn handle_death(&self, job: &JobPayload, error: &JobError) {
        for handler in &self.death_handlers {
            if let Err(panic) = std::panic::catch_unwind(AssertUnwindSafe(|| handler(job, error))) {
                self.handle_error(
                    &JobError::panic(panic),
                    &ErrorContext::new("Error calling death handler").with_job(job),
                );
            }
        }
    }

    /// Runs the callbacks of `event` one after the other, in registration order (or reverse
    /// order). A panicking callback is reported to the error handlers and the next one runs.
    pub async fn fire_event(&self, event: LifecycleEvent, reverse: bool) {
        let callbacks = self.callbacks(event);
        let ordered: Vec<&LifecycleCallback> = if reverse {
            callbacks.iter().rev().collect()
        } else {
            callbacks.iter().collect()
        };
        tracing::debug!(%event, callbacks = ordered.len(), "firing lifecycle event");
        for callback in ordered {
            let outcome = match std::panic::catch_unwind(AssertUnwindSafe(|| callback())) {
                Ok(future) => AssertUnwindSafe(future).catch_unwind().await,
                Err(panic) => Err(panic),
            };
            if let Err(panic) = outcome {
                self.handle_error(
                    &JobError::panic(panic),
                    &ErrorContext::new("Exception during lifecycle event").with_event(event),
                );
            }
        }
    }

    /// Runs the startup checks ([`HenchBuilder::on_startup_try`]) and then the `startup`
    /// callbacks, in registration order, stopping at the first that fails or panics: its error
    /// is reported to the error handlers and returned (Sidekiq re-raises the exceptions of its
    /// startup event, which stops the boot).
    pub async fn fire_startup(&self) -> Result<(), JobError> {
        let callbacks = self.callbacks(LifecycleEvent::Startup);
        tracing::debug!(
            checks = self.startup_checks.len(),
            callbacks = callbacks.len(),
            "firing lifecycle event startup"
        );
        for check in &self.startup_checks {
            let outcome = match std::panic::catch_unwind(AssertUnwindSafe(|| check())) {
                Ok(future) => AssertUnwindSafe(future)
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|panic| Err(JobError::panic(panic))),
                Err(panic) => Err(JobError::panic(panic)),
            };
            self.startup_outcome(outcome)?;
        }
        for callback in callbacks {
            let outcome = match std::panic::catch_unwind(AssertUnwindSafe(|| callback())) {
                Ok(future) => AssertUnwindSafe(future)
                    .catch_unwind()
                    .await
                    .map_err(JobError::panic),
                Err(panic) => Err(JobError::panic(panic)),
            };
            self.startup_outcome(outcome)?;
        }
        Ok(())
    }

    /// Reports the error of a startup callback, which stops the start.
    fn startup_outcome(&self, outcome: Result<(), JobError>) -> Result<(), JobError> {
        if let Err(error) = &outcome {
            self.handle_error(
                error,
                &ErrorContext::new("Exception during lifecycle event")
                    .with_event(LifecycleEvent::Startup),
            );
        }
        outcome
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            capsules: vec![CapsuleConfig {
                name: CapsuleConfig::DEFAULT_NAME.to_owned(),
                concurrency: Self::DEFAULT_CONCURRENCY,
                queues: vec!["default".to_owned()],
                weights: vec![("default".to_owned(), 0)],
                mode: QueueMode::Strict,
            }],
            error_handlers: vec![Arc::new(log_error)],
            death_handlers: Vec::new(),
            lifecycle: HashMap::new(),
            labels: Vec::new(),
            tag: None,
            job_logging: true,
            logged_job_attributes: Vec::new(),
            backtrace_cleaner: None,
            startup_checks: Vec::new(),
            metrics: true,
            systemd: Notifications::default(),
            fetch: FetchStrategy::default(),
            default_job_options: JobOptions::defaults(),
            max_retries: Self::DEFAULT_MAX_RETRIES,
            dead_max_jobs: Self::DEFAULT_DEAD_MAX_JOBS,
            dead_timeout: Self::DEFAULT_DEAD_TIMEOUT,
            timeout: Self::DEFAULT_TIMEOUT,
            max_iteration_runtime: None,
            poll_interval_average: None,
            average_scheduled_poll_interval: Duration::from_secs(5),
            heartbeat_interval: Duration::from_secs(10),
            initial_scheduler_wait: Duration::from_secs(10),
            initial_scheduler_jitter: Duration::from_secs(5),
            fetch_timeout: Duration::from_secs(2),
            orphan_check_interval: Self::DEFAULT_ORPHAN_CHECK_INTERVAL,
            orphan_grace: Self::DEFAULT_ORPHAN_GRACE,
            max_recoveries: Self::DEFAULT_MAX_RECOVERIES,
            server_middleware: ServerChain::new(),
            client_middleware: ClientChain::new(),
            data: Arc::new(Data::new()),
            unique_jobs: true,
            #[cfg(feature = "periodic")]
            periodic: Vec::new(),
            periodic_group: None,
            periodic_poller: true,
            periodic_poll_interval: Self::DEFAULT_PERIODIC_POLL_INTERVAL,
            periodic_history: Self::DEFAULT_PERIODIC_HISTORY,
            periodic_lease: Self::DEFAULT_PERIODIC_LEASE,
            periodic_check_interval: Self::DEFAULT_PERIODIC_CHECK_INTERVAL,
            batch_ttl: BatchDefaults::TTL,
            batch_linger: BatchDefaults::LINGER,
            batch_max_commit_jobs: BatchDefaults::MAX_COMMIT_JOBS,
            batch_commit_timeout: BatchDefaults::COMMIT_TIMEOUT,
            batch_maintenance: true,
            batch_maintenance_interval: Self::DEFAULT_BATCH_MAINTENANCE_INTERVAL,
            batch_dispatch_grace: Self::DEFAULT_BATCH_DISPATCH_GRACE,
            throttles: Vec::new(),
            queue_limits: Vec::new(),
            over_limit_when: None,
            over_limit_backoff: None,
            over_limit_reschedules: Self::DEFAULT_OVER_LIMIT_RESCHEDULES,
            over_limit_max_delay: Throttle::MAX_DELAY,
        }
    }
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let lifecycle: HashMap<&str, usize> = self
            .lifecycle
            .iter()
            .map(|(event, callbacks)| (event.name(), callbacks.len()))
            .collect();
        let mut debug = f.debug_struct("Config");
        debug
            .field("capsules", &self.capsules)
            .field("error_handlers", &self.error_handlers.len())
            .field("death_handlers", &self.death_handlers.len())
            .field("lifecycle", &lifecycle)
            .field("labels", &self.labels)
            .field("tag", &self.tag)
            .field("job_logging", &self.job_logging)
            .field("logged_job_attributes", &self.logged_job_attributes)
            .field("backtrace_cleaner", &self.backtrace_cleaner.is_some())
            .field("startup_checks", &self.startup_checks.len())
            .field("metrics", &self.metrics)
            .field("systemd", &self.systemd)
            .field("fetch", &self.fetch)
            .field("default_job_options", &self.default_job_options)
            .field("max_retries", &self.max_retries)
            .field("dead_max_jobs", &self.dead_max_jobs)
            .field("dead_timeout", &self.dead_timeout)
            .field("timeout", &self.timeout)
            .field("max_iteration_runtime", &self.max_iteration_runtime)
            .field("poll_interval_average", &self.poll_interval_average)
            .field(
                "average_scheduled_poll_interval",
                &self.average_scheduled_poll_interval,
            )
            .field("heartbeat_interval", &self.heartbeat_interval)
            .field("initial_scheduler_wait", &self.initial_scheduler_wait)
            .field("initial_scheduler_jitter", &self.initial_scheduler_jitter)
            .field("fetch_timeout", &self.fetch_timeout)
            .field("orphan_check_interval", &self.orphan_check_interval)
            .field("orphan_grace", &self.orphan_grace)
            .field("max_recoveries", &self.max_recoveries)
            .field("server_middleware", &self.server_middleware)
            .field("client_middleware", &self.client_middleware)
            .field("data", &self.data)
            .field("unique_jobs", &self.unique_jobs);
        #[cfg(feature = "periodic")]
        debug.field("periodic", &self.periodic);
        debug
            .field("periodic_group", &self.periodic_group)
            .field("periodic_poller", &self.periodic_poller)
            .field("periodic_poll_interval", &self.periodic_poll_interval)
            .field("periodic_history", &self.periodic_history)
            .field("periodic_lease", &self.periodic_lease)
            .field("periodic_check_interval", &self.periodic_check_interval)
            .field("batch_ttl", &self.batch_ttl)
            .field("batch_linger", &self.batch_linger)
            .field("batch_max_commit_jobs", &self.batch_max_commit_jobs)
            .field("batch_commit_timeout", &self.batch_commit_timeout)
            .field("batch_maintenance", &self.batch_maintenance)
            .field(
                "batch_maintenance_interval",
                &self.batch_maintenance_interval,
            )
            .field("batch_dispatch_grace", &self.batch_dispatch_grace)
            .field("throttles", &self.throttles)
            .field("queue_limits", &self.queue_limits)
            .field("over_limit_when", &self.over_limit_when.is_some())
            .field("over_limit_backoff", &self.over_limit_backoff.is_some())
            .field("over_limit_reschedules", &self.over_limit_reschedules)
            .field("over_limit_max_delay", &self.over_limit_max_delay)
            .finish()
    }
}

/// The default error handler: logs the error through `tracing`.
fn log_error(error: &JobError, ctx: &ErrorContext<'_>) {
    tracing::warn!(
        error_class = error.class(),
        error = %error,
        class = ctx.job.map(|job| job.class.as_str()),
        jid = ctx.job.map(|job| job.jid.as_str()),
        queue = ctx.queue,
        event = ctx.event.map(LifecycleEvent::name),
        "{}",
        ctx.context
    );
}

/// Where the builder gets its Redis connection.
enum RedisSource {
    Env,
    Config(RedisConfig),
    Pool(RedisPool),
}

/// Builds a [`Hench`]. Created by [`Hench::builder`].
///
/// [`build`](HenchBuilder::build) validates the configuration (capsules, queues, intervals,
/// unique job names), connects to Redis and returns the [`Hench`].
///
/// ```no_run
/// use hench::prelude::*;
/// use std::time::Duration;
///
/// # async fn demo() -> hench::Result<()> {
/// let hench = Hench::builder()
///     .redis_url("redis://127.0.0.1/")
///     .concurrency(10)
///     .queues(["critical", "default", "low"])
///     .capsule("serial", |c| c.concurrency(1).queues(["serial"]))
///     .timeout(Duration::from_secs(25))
///     .error_handler(|error, ctx| tracing::error!(%error, context = ctx.context, "job error"))
///     .on_startup(|| async { tracing::info!("starting") })
///     .build()
///     .await?;
/// let client = hench.client().clone();
/// # let _ = client; Ok(()) }
/// ```
pub struct HenchBuilder {
    config: Config,
    redis: RedisSource,
    capsules: Vec<CapsuleBuilder>,
    jobs: Vec<Arc<dyn ErasedJob>>,
    auto_register: bool,
    data: Data,
    /// The names of the schedules the worker's configuration file added (`[[periodic]]`), for
    /// the error that names where a duplicate comes from.
    #[cfg(feature = "periodic")]
    file_schedules: Vec<String>,
}

impl HenchBuilder {
    fn new() -> Self {
        Self {
            config: Config::default(),
            redis: RedisSource::Env,
            capsules: vec![CapsuleBuilder::new(CapsuleConfig::DEFAULT_NAME)],
            jobs: Vec::new(),
            auto_register: true,
            data: Data::new(),
            #[cfg(feature = "periodic")]
            file_schedules: Vec::new(),
        }
    }

    /// Connects to `url` (default: `REDIS_PROVIDER`/`REDIS_URL`, else
    /// `redis://127.0.0.1:6379`).
    #[must_use]
    pub fn redis_url(mut self, url: impl Into<String>) -> Self {
        self.redis = RedisSource::Config(RedisConfig::new(url));
        self
    }

    /// Connects with `config`.
    #[must_use]
    pub fn redis_config(mut self, config: RedisConfig) -> Self {
        self.redis = RedisSource::Config(config);
        self
    }

    /// Uses an existing pool.
    #[must_use]
    pub fn redis(mut self, pool: RedisPool) -> Self {
        self.redis = RedisSource::Pool(pool);
        self
    }

    fn capsule_mut(&mut self, name: &str) -> &mut CapsuleBuilder {
        let index = match self
            .capsules
            .iter()
            .position(|capsule| capsule.name == name)
        {
            Some(index) => index,
            None => {
                self.capsules.push(CapsuleBuilder::new(name));
                self.capsules.len() - 1
            }
        };
        &mut self.capsules[index]
    }

    fn update_capsule(
        mut self,
        name: &str,
        update: impl FnOnce(CapsuleBuilder) -> CapsuleBuilder,
    ) -> Self {
        let capsule = self.capsule_mut(name);
        let updated = update(capsule.clone());
        *capsule = CapsuleBuilder {
            name: capsule.name.clone(),
            ..updated
        };
        self
    }

    /// Concurrency of the default capsule (default 5).
    #[must_use]
    pub fn concurrency(self, concurrency: usize) -> Self {
        self.update_capsule(CapsuleConfig::DEFAULT_NAME, |capsule| {
            capsule.concurrency(concurrency)
        })
    }

    /// Queues of the default capsule, in strict priority order (default `["default"]`).
    /// `"name,weight"` entries set weights, see [`CapsuleBuilder::queues`].
    #[must_use]
    pub fn queues<I, S>(self, queues: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.update_capsule(CapsuleConfig::DEFAULT_NAME, |capsule| {
            capsule.queues(queues)
        })
    }

    /// Weighted queues of the default capsule, see [`CapsuleBuilder::weighted_queues`].
    #[must_use]
    pub fn weighted_queues<I, S>(self, queues: I) -> Self
    where
        I: IntoIterator<Item = (S, u32)>,
        S: Into<String>,
    {
        self.update_capsule(CapsuleConfig::DEFAULT_NAME, |capsule| {
            capsule.weighted_queues(queues)
        })
    }

    /// Configures the capsule called `name`, creating it if needed (the default capsule is
    /// called `default`). Capsules have their own concurrency and queues:
    /// `.capsule("serial", |c| c.concurrency(1).queues(["serial"]))`.
    #[must_use]
    pub fn capsule(
        self,
        name: impl Into<String>,
        configure: impl FnOnce(CapsuleBuilder) -> CapsuleBuilder,
    ) -> Self {
        let name = name.into();
        self.update_capsule(&name, configure)
    }

    /// Registers shared state for jobs ([`JobContext::data`](crate::JobContext::data)),
    /// replacing any previous value of the same type.
    #[must_use]
    pub fn data<T: Send + Sync + 'static>(mut self, value: T) -> Self {
        self.data.insert(value);
        self
    }

    /// Registers shared state that is already in an `Arc`.
    #[must_use]
    pub fn data_arc<T: Send + Sync + 'static>(mut self, value: Arc<T>) -> Self {
        self.data.insert_arc(value);
        self
    }

    /// Registers a job instance. Jobs defined with `#[hench::job]` register themselves, in
    /// every binary that links the crate defining them (see [`Registry::from_inventory`]):
    /// registering one of them here as well makes [`build`](Self::build) fail with an error
    /// saying so, unless automatic registration is off ([`auto_register`](Self::auto_register)).
    /// Two jobs with one name are an error too.
    #[must_use]
    pub fn register<J: Job>(mut self, job: J) -> Self {
        self.jobs.push(Arc::new(job));
        self
    }

    /// Registers `J::default()`.
    #[must_use]
    pub fn register_default<J: Job + Default>(self) -> Self {
        self.register(J::default())
    }

    /// Registers a type-erased job.
    #[must_use]
    pub fn register_dyn(mut self, job: Arc<dyn ErasedJob>) -> Self {
        self.jobs.push(job);
        self
    }

    /// Whether jobs defined with `#[hench::job]` are registered automatically (default
    /// `true`): those of every crate linked into the binary, which a crate the binary never
    /// refers to is not (see [`Registry::from_inventory`]).
    #[must_use]
    pub fn auto_register(mut self, enabled: bool) -> Self {
        self.auto_register = enabled;
        self
    }

    /// Appends a server middleware (replacing one of the same type).
    #[must_use]
    pub fn server_middleware(mut self, middleware: impl ServerMiddleware) -> Self {
        self.config.server_middleware.add(middleware);
        self
    }

    /// Appends a client middleware (replacing one of the same type).
    #[must_use]
    pub fn client_middleware(mut self, middleware: impl ClientMiddleware) -> Self {
        self.config.client_middleware.add(middleware);
        self
    }

    /// Edits the server middleware chain (`insert_before`, `remove`, ...).
    #[must_use]
    pub fn configure_server_middleware(mut self, configure: impl FnOnce(&mut ServerChain)) -> Self {
        configure(&mut self.config.server_middleware);
        self
    }

    /// Edits the client middleware chain.
    #[must_use]
    pub fn configure_client_middleware(mut self, configure: impl FnOnce(&mut ClientChain)) -> Self {
        configure(&mut self.config.client_middleware);
        self
    }

    /// Adds an error handler. The default handler (logging through `tracing`) stays unless
    /// [`clear_error_handlers`](Self::clear_error_handlers) is called first.
    #[must_use]
    pub fn error_handler<F>(mut self, handler: F) -> Self
    where
        F: Fn(&JobError, &ErrorContext<'_>) + Send + Sync + 'static,
    {
        self.config.error_handlers.push(Arc::new(handler));
        self
    }

    /// Removes every error handler, including the default logging one.
    #[must_use]
    pub fn clear_error_handlers(mut self) -> Self {
        self.config.error_handlers.clear();
        self
    }

    /// Adds a death handler.
    #[must_use]
    pub fn death_handler<F>(mut self, handler: F) -> Self
    where
        F: Fn(&JobPayload, &JobError) + Send + Sync + 'static,
    {
        self.config.death_handlers.push(Arc::new(handler));
        self
    }

    /// Adds a callback for `event`. Callbacks run in registration order (those of `quiet`,
    /// `shutdown` and `exit` in reverse order); one that panics is reported to the error handlers and the
    /// next one runs, except at startup, where it stops the start
    /// ([`on_startup`](Self::on_startup)).
    #[must_use]
    pub fn on<F, Fut>(mut self, event: LifecycleEvent, callback: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let callback: LifecycleCallback = Arc::new(move || Box::pin(callback()));
        self.config
            .lifecycle
            .entry(event)
            .or_default()
            .push(callback);
        self
    }

    /// Adds a callback run when the process starts, before it registers or fetches anything.
    /// A callback that panics stops the start: [`Hench::start`] (and `run`, `run_until`)
    /// returns [`Error::Startup`], as Sidekiq's startup event re-raises.
    #[must_use]
    pub fn on_startup<F, Fut>(self, callback: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.on(LifecycleEvent::Startup, callback)
    }

    /// Adds a fallible startup callback: a check of something the jobs need (a database, a
    /// model, migrations) that stops the start when it fails, so that the process exits (and
    /// its supervisor restarts it, or alerts) instead of taking jobs that would all fail.
    /// [`Hench::start`] (and `run`, `run_until`) then returns [`Error::Startup`], before the
    /// process registers or fetches anything. These callbacks run in the order they were
    /// added, before the `startup` callbacks ([`on_startup`](Self::on_startup)).
    ///
    /// ```no_run
    /// # async fn demo() -> hench::Result<()> {
    /// let hench = hench::Hench::builder()
    ///     .on_startup_try(|| async {
    ///         // e.g. ping the database the jobs use
    ///         Ok::<(), std::io::Error>(())
    ///     })
    ///     .build()
    ///     .await?;
    /// hench.run().await?;
    /// # Ok(()) }
    /// ```
    #[must_use]
    pub fn on_startup_try<F, Fut, E>(mut self, callback: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<(), E>> + Send + 'static,
        JobError: From<E>,
    {
        let check: StartupCheck = Arc::new(move || {
            let checked = callback();
            Box::pin(async move { checked.await.map_err(JobError::from) })
        });
        self.config.startup_checks.push(check);
        self
    }

    /// Adds a callback run when the process stops fetching jobs. Quiet callbacks run in a task
    /// of their own, and a shutdown lets them finish (until its deadline) before it runs the
    /// shutdown callbacks.
    #[must_use]
    pub fn on_quiet<F, Fut>(self, callback: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.on(LifecycleEvent::Quiet, callback)
    }

    /// Adds a callback run when the process starts shutting down.
    #[must_use]
    pub fn on_shutdown<F, Fut>(self, callback: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.on(LifecycleEvent::Shutdown, callback)
    }

    /// Adds a callback run when the process has shut down.
    #[must_use]
    pub fn on_exit<F, Fut>(self, callback: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.on(LifecycleEvent::Exit, callback)
    }

    /// Adds a callback run when a heartbeat finds the process's heartbeat key missing: after the
    /// first heartbeat, and after one that found the key expired or lost by Redis (see
    /// [`LifecycleEvent::Heartbeat`]).
    ///
    /// Heartbeat callbacks run inside the heartbeat, as in Sidekiq: keep them quick (spawn
    /// long work). A slow one delays the heartbeat's other work (remote signals, stats, the
    /// dashboard's view of the process), though not the process's liveness, which a thread of
    /// its own maintains.
    #[must_use]
    pub fn on_heartbeat<F, Fut>(self, callback: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.on(LifecycleEvent::Heartbeat, callback)
    }

    /// Adds a callback run on every heartbeat. Like [`HenchBuilder::on_heartbeat`]'s, it runs
    /// inside the heartbeat: keep it quick.
    #[must_use]
    pub fn on_beat<F, Fut>(self, callback: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.on(LifecycleEvent::Beat, callback)
    }

    /// Adds a label shown in the dashboard.
    #[must_use]
    pub fn label(mut self, label: impl Into<String>) -> Self {
        let label = label.into();
        if !self.config.labels.contains(&label) {
            self.config.labels.push(label);
        }
        self
    }

    /// Adds labels shown in the dashboard.
    #[must_use]
    pub fn labels<I, S>(self, labels: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        labels
            .into_iter()
            .fold(self, |builder, label| builder.label(label))
    }

    /// Sets the tag shown in the dashboard.
    #[must_use]
    pub fn tag(mut self, tag: impl Into<String>) -> Self {
        self.config.tag = Some(tag.into());
        self
    }

    /// Enables or disables logging of every job execution (default enabled).
    #[must_use]
    pub fn job_logging(mut self, enabled: bool) -> Self {
        self.config.job_logging = enabled;
        self
    }

    /// Payload keys to add to every line logged while a job runs, `start` and `done`/`fail`
    /// included (Sidekiq's `logged_job_attributes`): the span's `attrs` field holds those the
    /// job has, as a JSON object (`attrs={"request_id":"req-4242"}`). For example a request id
    /// that a client middleware stamps on every job, to follow a request into its jobs.
    /// Replaces the list; `bid` and `tags` have fields of their own.
    #[must_use]
    pub fn logged_job_attributes<I, S>(mut self, keys: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.config.logged_job_attributes = keys.into_iter().map(Into::into).collect();
        self
    }

    /// Filters the backtrace lines stored with failed jobs that enable `backtrace` (Sidekiq's
    /// `backtrace_cleaner`), for example to keep only your own crates' frames. It gets every
    /// line ([`JobError::backtrace_lines`]: the causes, then the frames from the job's code
    /// down to where hench runs it) and returns those to store, before the option's line limit
    /// applies. A cleaner that panics is reported and the lines are stored as they were.
    #[must_use]
    pub fn backtrace_cleaner<F>(mut self, cleaner: F) -> Self
    where
        F: Fn(Vec<String>) -> Vec<String> + Send + Sync + 'static,
    {
        self.config.backtrace_cleaner = Some(Arc::new(cleaner));
        self
    }

    /// Enables or disables execution metrics ([`crate::metrics`]): per-class counts, failures,
    /// execution times and histograms, written to Redis on every heartbeat (default enabled).
    #[must_use]
    pub fn metrics(mut self, enabled: bool) -> Self {
        self.config.metrics = enabled;
        self
    }

    /// Whether the process notifies systemd of its state ([`crate::systemd`]): readiness,
    /// status, shutdown and watchdog keep-alives. Enabled by default, in which case it happens
    /// only when systemd set `NOTIFY_SOCKET` (a `Type=notify` service) and the process runs
    /// with [`Hench::run`]: workers embedded with [`Hench::start`] or [`Hench::run_until`]
    /// leave the service's notifications to the program that embeds them (give them a
    /// notifier with [`HenchBuilder::systemd_notifier`] to send them anyway).
    #[must_use]
    pub fn systemd(mut self, enabled: bool) -> Self {
        self.config.systemd = if enabled {
            Notifications::FromEnv
        } else {
            Notifications::Disabled
        };
        self
    }

    /// Sends the systemd notifications through `notifier` instead of the socket named by
    /// `NOTIFY_SOCKET`, in every mode: embedded workers ([`Hench::start`],
    /// [`Hench::run_until`]) send them too, including `STOPPING=1` when they stop (which makes
    /// systemd stop the whole service, even after a remote stop from the dashboard).
    #[must_use]
    pub fn systemd_notifier(mut self, notifier: Notifier) -> Self {
        self.config.systemd = Notifications::To(notifier);
        self
    }

    /// Selects the fetch strategy (default [`FetchStrategy::Reliable`]).
    #[must_use]
    pub fn fetch(mut self, fetch: FetchStrategy) -> Self {
        self.config.fetch = fetch;
        self
    }

    /// Sets options of jobs that set neither them nor their type's options. They are merged
    /// over the current defaults (initially queue `default` and `retry: true`), like Sidekiq's
    /// `default_job_options=`: what `options` sets replaces the current value, the rest is
    /// kept, so `JobOptions::new().backtrace(true)` keeps the default queue and retries.
    #[must_use]
    pub fn default_job_options(mut self, options: JobOptions) -> Self {
        self.config.default_job_options = self.config.default_job_options.merge(&options);
        self
    }

    /// Number of retries of jobs with `retry: true` (default 25).
    #[must_use]
    pub fn max_retries(mut self, retries: u32) -> Self {
        self.config.max_retries = retries;
        self
    }

    /// Maximum number of jobs kept in the morgue (default 10 000).
    #[must_use]
    pub fn dead_max_jobs(mut self, jobs: usize) -> Self {
        self.config.dead_max_jobs = jobs;
        self
    }

    /// How long dead jobs are kept (default 180 days).
    #[must_use]
    pub fn dead_timeout(mut self, timeout: Duration) -> Self {
        self.config.dead_timeout = timeout;
        self
    }

    /// How long shutdown waits for running jobs before requeueing them (default 25 s).
    #[must_use]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.config.timeout = timeout;
        self
    }

    /// Lets [iterable jobs](crate::iterable) iterate for at most this long per execution: after
    /// the item that exceeds it, the job saves its cursor and goes back to the end of its queue,
    /// so that long iterations take turns with other jobs (default: no limit). An execution
    /// always processes at least one item.
    #[must_use]
    pub fn max_iteration_runtime(mut self, limit: Duration) -> Self {
        self.config.max_iteration_runtime = Some(limit);
        self
    }

    /// Fixes the average scheduler poll interval instead of scaling it with the number of
    /// processes.
    #[must_use]
    pub fn poll_interval_average(mut self, interval: Duration) -> Self {
        self.config.poll_interval_average = Some(interval);
        self
    }

    /// Target interval between scheduler polls across all processes (default 5 s).
    #[must_use]
    pub fn average_scheduled_poll_interval(mut self, interval: Duration) -> Self {
        self.config.average_scheduled_poll_interval = interval;
        self
    }

    /// Interval between heartbeats (default 10 s, like Sidekiq's). At most
    /// [`Config::MAX_HEARTBEAT_INTERVAL`] (20 s): other processes treat a process whose
    /// heartbeat key is a minute old as dead. Shorter intervals are mostly for tests.
    #[must_use]
    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.config.heartbeat_interval = interval;
        self
    }

    /// Wait before the first scheduler poll (default 10 s; skipped when
    /// `poll_interval_average` is set).
    #[must_use]
    pub fn initial_scheduler_wait(mut self, wait: Duration) -> Self {
        self.config.initial_scheduler_wait = wait;
        self
    }

    /// Random extra wait before the first scheduler poll (default up to 5 s).
    #[must_use]
    pub fn initial_scheduler_jitter(mut self, jitter: Duration) -> Self {
        self.config.initial_scheduler_jitter = jitter;
        self
    }

    /// How long a fetch blocks waiting for a job (default 2 s).
    #[must_use]
    pub fn fetch_timeout(mut self, timeout: Duration) -> Self {
        self.config.fetch_timeout = timeout;
        self
    }

    /// Interval between checks for the in-progress jobs of processes that died (default 60 s;
    /// see [`Config::orphan_check_interval`]).
    #[must_use]
    pub fn orphan_check_interval(mut self, interval: Duration) -> Self {
        self.config.orphan_check_interval = interval;
        self
    }

    /// How long a process without a heartbeat key must stay without one before an orphan check
    /// pushes its in-progress jobs back to their queues (default 30 s, at least twice the
    /// heartbeat interval; see [`Config::orphan_grace`]). Redis can lose the keys of live
    /// processes (a restart without persistence), which re-create them within a heartbeat
    /// interval: keep the grace above the heartbeat interval of every process.
    #[must_use]
    pub fn orphan_grace(mut self, grace: Duration) -> Self {
        self.config.orphan_grace = grace;
        self
    }

    /// How many times a job may be recovered from processes that died while they held it
    /// before it is taken for a poison pill and moved to the morgue (default 3; see
    /// [`Config::max_recoveries`]).
    #[must_use]
    pub fn max_recoveries(mut self, recoveries: u32) -> Self {
        self.config.max_recoveries = recoveries;
        self
    }

    /// Whether pushes honour uniqueness (default `true`; see [`crate::unique`]). Off, pushes
    /// ignore the `unique` option and take no lock; the engine still releases and holds the
    /// locks of jobs that carry one.
    #[must_use]
    pub fn unique_jobs(mut self, enabled: bool) -> Self {
        self.config.unique_jobs = enabled;
        self
    }

    /// Registers a code-defined periodic schedule (repeatable; names must be unique; see
    /// [`crate::periodic`]).
    #[cfg(feature = "periodic")]
    #[must_use]
    pub fn periodic(mut self, periodic: crate::periodic::Periodic) -> Self {
        self.config.periodic.push(periodic);
        self
    }

    /// A schedule of the worker's configuration file (`[[periodic]]`): as
    /// [`periodic`](Self::periodic), remembered as the file's for the error a duplicate name
    /// gives.
    #[cfg(all(feature = "periodic", feature = "cli"))]
    pub(crate) fn periodic_from_file(mut self, periodic: crate::periodic::Periodic) -> Self {
        self.file_schedules.push(periodic.name().to_owned());
        self.periodic(periodic)
    }

    /// The group whose crontab this process registers and prunes (default `default`). Setting
    /// it explicitly also makes an empty crontab authoritative (it prunes the group).
    #[must_use]
    pub fn periodic_group(mut self, group: impl Into<String>) -> Self {
        self.config.periodic_group = Some(group.into());
        self
    }

    /// Runs the periodic poller in this process (default `true`).
    #[must_use]
    pub fn periodic_poller(mut self, enabled: bool) -> Self {
        self.config.periodic_poller = enabled;
        self
    }

    /// Longest wait between two looks at the periodic schedules (default 5 s).
    #[must_use]
    pub fn periodic_poll_interval(mut self, interval: Duration) -> Self {
        self.config.periodic_poll_interval = interval;
        self
    }

    /// Enqueues remembered per periodic schedule (default 10; 0 keeps none).
    #[must_use]
    pub fn periodic_history(mut self, entries: usize) -> Self {
        self.config.periodic_history = entries;
        self
    }

    /// Lease of a due periodic schedule while a poller fires it (default 60 s; at least twice
    /// [`periodic_poll_interval`](Self::periodic_poll_interval), checked by `build`): how long a
    /// tick waits when the poller firing it dies, or loses Redis's reply, before another poller
    /// takes it. A schedule ticking every few seconds may want a shorter lease (a tick held
    /// back that long is then caught up as its `catch_up` policy says). Also `periodic_lease`
    /// in the worker's configuration file (seconds).
    #[must_use]
    pub fn periodic_lease(mut self, lease: Duration) -> Self {
        self.config.periodic_lease = lease;
        self
    }

    /// Interval between checks that this process's crontab is registered (default 60 s).
    #[doc(hidden)]
    #[must_use]
    pub fn periodic_check_interval(mut self, every: Duration) -> Self {
        self.config.periodic_check_interval = every;
        self
    }

    /// Expiry of an unfinished batch after its last activity (default 30 days; at least 1 hour).
    #[must_use]
    pub fn batch_ttl(mut self, ttl: Duration) -> Self {
        self.config.batch_ttl = ttl;
        self
    }

    /// How long a finished batch's data stays (default 24 hours; at least 30 s).
    #[must_use]
    pub fn batch_linger(mut self, linger: Duration) -> Self {
        self.config.batch_linger = linger;
        self
    }

    /// Most members of one batch commit, each child batch counting as 10 (default 10 000; 1 to
    /// 100 000). A commit is one Redis script, which every other client of the Redis waits for:
    /// about 4 µs per member, so some 40 ms at 10 000 and 0.4 s at 100 000 (keep the response
    /// timeouts of every client well above that, or define large batches with loader jobs
    /// instead).
    #[must_use]
    pub fn batch_max_commit_jobs(mut self, jobs: usize) -> Self {
        self.config.batch_max_commit_jobs = jobs;
        self
    }

    /// How long a batch commit whose outcome is unknown is sent again (default 30 s).
    #[must_use]
    pub fn batch_commit_timeout(mut self, timeout: Duration) -> Self {
        self.config.batch_commit_timeout = timeout;
        self
    }

    /// Runs batch maintenance in this process (default `true`).
    #[must_use]
    pub fn batch_maintenance(mut self, enabled: bool) -> Self {
        self.config.batch_maintenance = enabled;
        self
    }

    /// Period of batch maintenance (default 30 s, with ±20 % jitter).
    #[must_use]
    pub fn batch_maintenance_interval(mut self, interval: Duration) -> Self {
        self.config.batch_maintenance_interval = interval;
        self
    }

    /// Age of an undispatched batch event before maintenance dispatches it (default 10 s).
    #[must_use]
    pub fn batch_dispatch_grace(mut self, grace: Duration) -> Self {
        self.config.batch_dispatch_grace = grace;
        self
    }

    /// Defines a throttle (see [`crate::limit`]); a later definition with the same name
    /// replaces an earlier one.
    #[must_use]
    pub fn throttle(mut self, throttle: Throttle) -> Self {
        self.config.throttles.push(throttle);
        self
    }

    /// Limits every fetch of `queue` (concurrency, window, bucket or leaky limits, at most one
    /// of each kind): a queue over its limits is skipped like a paused one, its jobs keep their
    /// order, and the other queues keep flowing. The limits apply to every process fetching the
    /// queue (they live in Redis): this process writes them at every start (a deploy, a restart,
    /// a scale-out), replacing the queue's stored limits and logging a value it replaces, so
    /// that code wins over run-time edits (`Api::queue(..).set_limits`, the dashboard, `hench
    /// queues limit`) at the next start, and two deployments configured differently overwrite
    /// each other. Removing the call leaves the stored limits in place: clear them with
    /// `hench queues limit <queue> --clear` or `Api::queue(..).clear_limits`. Workers of an
    /// earlier hench version ignore queue limits, so upgrade every worker first. A later call
    /// for the same queue replaces an earlier one. See [`crate::limit`] for the kinds of limits.
    ///
    /// ```
    /// use std::time::Duration;
    /// use hench::limit::Limit;
    ///
    /// let builder = hench::Hench::builder().queue_limits(
    ///     "exports",
    ///     [
    ///         Limit::Concurrency { limit: 3 },
    ///         Limit::Window { limit: 100, period: Duration::from_secs(60) },
    ///     ],
    /// );
    /// ```
    #[must_use]
    pub fn queue_limits(
        mut self,
        queue: impl Into<String>,
        limits: impl IntoIterator<Item = Limit>,
    ) -> Self {
        let queue = queue.into();
        let limits: Vec<Limit> = limits.into_iter().collect();
        self.config.queue_limits.retain(|(name, _)| *name != queue);
        self.config.queue_limits.push((queue, limits));
        self
    }

    /// Recognizes job errors that are a limiter's refusal in disguise (a client library's
    /// HTTP 429 error carrying `Retry-After`), for errors that are not already an
    /// [`OverLimit`]. A panicking classifier is reported and ignored.
    #[must_use]
    pub fn over_limit_when<F>(mut self, classify: F) -> Self
    where
        F: Fn(&JobError) -> Option<OverLimit> + Send + Sync + 'static,
    {
        self.config.over_limit_when = Some(Arc::new(classify));
        self
    }

    /// Reschedule delay policy of limiters without their own.
    #[must_use]
    pub fn over_limit_backoff<F>(mut self, backoff: F) -> Self
    where
        F: Fn(&crate::limit::BackoffInput<'_>) -> Duration + Send + Sync + 'static,
    {
        self.config.over_limit_backoff = Some(Arc::new(backoff));
        self
    }

    /// Times a job may be rescheduled by limiters before the failure path takes over
    /// (default 20; 0 fails an over-limit job at once).
    #[must_use]
    pub fn over_limit_reschedules(mut self, max: u32) -> Self {
        self.config.over_limit_reschedules = max;
        self
    }

    /// Longest reschedule delay (default 24 hours; at least 1 s).
    #[must_use]
    pub fn over_limit_max_delay(mut self, delay: Duration) -> Self {
        self.config.over_limit_max_delay = delay;
        self
    }

    /// Validates the configuration without connecting to Redis or touching the registry.
    fn validated_config(&mut self) -> Result<()> {
        let mut capsules = Vec::with_capacity(self.capsules.len());
        let mut names = HashSet::new();
        for builder in &self.capsules {
            let capsule = builder.build()?;
            if !names.insert(capsule.name.clone()) {
                return Err(Error::Config(format!(
                    "duplicate capsule name `{}`",
                    capsule.name
                )));
            }
            capsules.push(capsule);
        }
        let config = &mut self.config;
        config.capsules = capsules;
        let positive = [
            ("heartbeat_interval", config.heartbeat_interval),
            (
                "average_scheduled_poll_interval",
                config.average_scheduled_poll_interval,
            ),
            ("fetch_timeout", config.fetch_timeout),
            ("orphan_check_interval", config.orphan_check_interval),
            ("periodic_poll_interval", config.periodic_poll_interval),
            ("periodic_check_interval", config.periodic_check_interval),
            ("batch_commit_timeout", config.batch_commit_timeout),
            (
                "batch_maintenance_interval",
                config.batch_maintenance_interval,
            ),
        ];
        for (name, value) in positive {
            if value.is_zero() {
                return Err(Error::Config(format!("{name} must be greater than zero")));
            }
        }
        if config.heartbeat_interval > Config::MAX_HEARTBEAT_INTERVAL {
            return Err(Error::Config(format!(
                "heartbeat_interval must be at most {} s: other processes treat a process whose \
                 heartbeat is a minute old as dead and run its jobs again",
                Config::MAX_HEARTBEAT_INTERVAL.as_secs()
            )));
        }
        if config
            .poll_interval_average
            .is_some_and(|interval| interval.is_zero())
        {
            return Err(Error::Config(
                "poll_interval_average must be greater than zero".to_owned(),
            ));
        }
        if config
            .max_iteration_runtime
            .is_some_and(|limit| limit.is_zero())
        {
            return Err(Error::Config(
                "max_iteration_runtime must be greater than zero".to_owned(),
            ));
        }
        if config.dead_max_jobs == 0 {
            return Err(Error::Config("dead_max_jobs must be at least 1".to_owned()));
        }
        if config.default_job_options.queue.as_deref() == Some("") {
            return Err(Error::Config(
                "the default job options name an empty queue".to_owned(),
            ));
        }
        validate_features(config)?;
        #[cfg(feature = "periodic")]
        check_schedule_origins(&config.periodic, &self.file_schedules)?;
        crate::pro::validate(config)
    }

    /// The validated configuration, without building anything (tests of other modules).
    #[cfg(test)]
    pub(crate) fn config_for_test(mut self) -> Result<Config> {
        self.validated_config()?;
        Ok(self.config)
    }

    /// Validates the configuration, builds the job registry (detecting duplicate names),
    /// connects to Redis and returns the [`Hench`].
    pub async fn build(mut self) -> Result<Hench> {
        self.validated_config()?;
        let mut registry = if self.auto_register {
            Registry::from_inventory()?
        } else {
            Registry::new()
        };
        let mut explicit = HashSet::new();
        for job in std::mem::take(&mut self.jobs) {
            let name = job.name().to_owned();
            if !explicit.contains(&name)
                && let Some(automatic) = registry.get(&name)
            {
                return Err(Error::Config(automatic_clash(
                    &name,
                    automatic.dyn_type_name(),
                    job.dyn_type_name(),
                )));
            }
            registry.register_dyn(job)?;
            explicit.insert(name);
        }
        // Every throttle a registered job defines must be valid, and every throttle a job names
        // must be defined (by a job, the builder or the configuration file).
        crate::limit::throttle::check_registry(&self.config, &registry)?;
        // The worker file's schedules take the options of the job types they name.
        crate::periodic::apply_registered_jobs(&mut self.config, &registry)?;
        let redis = match std::mem::replace(&mut self.redis, RedisSource::Env) {
            RedisSource::Pool(pool) => pool,
            RedisSource::Config(config) => RedisPool::from_config(config).await?,
            RedisSource::Env => RedisPool::from_config(RedisConfig::from_env()?).await?,
        };
        let mut config = self.config;
        config.data = Arc::new(self.data);
        let client = Client::from_parts(
            redis.clone(),
            config.client_middleware.clone(),
            config.default_job_options.clone(),
            config.unique_jobs,
            config.batch_defaults(),
        );
        Ok(Hench {
            config: Arc::new(config),
            redis,
            registry: Arc::new(registry),
            client,
        })
    }
}

/// A schedule of the worker's configuration file named like one defined in code, or like
/// another of the file: a configuration error that says where both come from (a file schedule
/// does not replace a code schedule, unlike a file throttle). Two code schedules of one name
/// are left to `periodic::validate_all`.
#[cfg(feature = "periodic")]
fn check_schedule_origins(
    schedules: &[crate::periodic::Periodic],
    from_file: &[String],
) -> Result<()> {
    let mut seen: HashSet<&str> = HashSet::new();
    for periodic in schedules {
        let name = periodic.name();
        if seen.insert(name) {
            continue;
        }
        let origins = match from_file
            .iter()
            .filter(|file| file.as_str() == name)
            .count()
        {
            0 => continue,
            1 => {
                "in code (`HenchBuilder::periodic`) and in the configuration file (`[[periodic]]`)"
            }
            _ => "twice in the configuration file (`[[periodic]]`)",
        };
        return Err(Error::Config(format!(
            "periodic job `{}` is defined {origins}: a schedule of the file does not replace one \
             of the code; rename one of them",
            name.escape_debug()
        )));
    }
    Ok(())
}

/// The bounds of the features' settings (`docs/design/pro-features.md` §2.10).
fn validate_features(config: &Config) -> Result<()> {
    if config.periodic_lease < config.periodic_poll_interval.saturating_mul(2) {
        return Err(Error::Config(
            "periodic_lease must be at least twice periodic_poll_interval".to_owned(),
        ));
    }
    if let Some(group) = &config.periodic_group
        && !valid_name(group, 100)
    {
        return Err(Error::Config(format!(
            "invalid periodic_group `{group}`: 1 to 100 bytes, no control characters, no leading \
             or trailing whitespace"
        )));
    }
    if config.batch_ttl < BatchDefaults::MIN_TTL {
        return Err(Error::Config(
            "batch_ttl must be at least 1 hour".to_owned(),
        ));
    }
    if config.batch_linger < BatchDefaults::MIN_LINGER {
        return Err(Error::Config(
            "batch_linger must be at least 30 s".to_owned(),
        ));
    }
    if !(1..=BatchDefaults::MAX_COMMIT_JOBS_LIMIT).contains(&config.batch_max_commit_jobs) {
        return Err(Error::Config(format!(
            "batch_max_commit_jobs must be between 1 and {}",
            BatchDefaults::MAX_COMMIT_JOBS_LIMIT
        )));
    }
    if config.over_limit_max_delay < Duration::from_secs(1) {
        return Err(Error::Config(
            "over_limit_max_delay must be at least 1 s".to_owned(),
        ));
    }
    Ok(())
}

/// A free-text name of the features (periodic schedules and groups): 1 to `max` bytes, no
/// control characters, no leading or trailing whitespace.
pub(crate) fn valid_name(name: &str, max: usize) -> bool {
    (1..=max).contains(&name.len()) && !name.chars().any(char::is_control) && name.trim() == name
}

/// The error of a job registered explicitly under the name of a job `#[hench::job]` registered
/// automatically (types `automatic` and `explicit`).
fn automatic_clash(name: &str, automatic: &str, explicit: &str) -> String {
    if automatic == explicit {
        format!(
            "job `{name}` (`{explicit}`) is registered twice: `#[hench::job]` registers it by \
             itself, in every binary that links the crate defining it (naming the job, as \
             `register` does, links that crate), so drop the `register` call, or register every \
             job by hand after `auto_register(false)`"
        )
    } else {
        format!(
            "duplicate job name `{name}`: `#[hench::job]` registered `{automatic}` under it, and \
             `{explicit}` is registered with the same name; rename one (`#[hench::job(name = \
             \"...\")]`, or its `Job::NAME`), or register every job by hand after \
             `auto_register(false)`"
        )
    }
}

impl fmt::Debug for HenchBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HenchBuilder")
            .field("config", &self.config)
            .field("capsules", &self.capsules)
            .field(
                "jobs",
                &self.jobs.iter().map(|job| job.name()).collect::<Vec<_>>(),
            )
            .field("auto_register", &self.auto_register)
            .finish_non_exhaustive()
    }
}

/// A configured hench instance: configuration, Redis pool, job registry and client.
///
/// Start processing jobs with [`Hench::run`], [`Hench::run_until`] or [`Hench::start`].
/// Cheap to clone.
#[derive(Clone)]
pub struct Hench {
    pub(crate) config: Arc<Config>,
    pub(crate) redis: RedisPool,
    pub(crate) registry: Arc<Registry>,
    pub(crate) client: Client,
}

impl Hench {
    /// Starts configuring an instance.
    pub fn builder() -> HenchBuilder {
        HenchBuilder::new()
    }

    /// The client, configured with the client middleware and default job options.
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// The Redis pool.
    pub fn redis(&self) -> &RedisPool {
        &self.redis
    }

    /// The configuration.
    pub fn config(&self) -> &Arc<Config> {
        &self.config
    }

    /// The registered jobs.
    pub fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }

    /// The shared data map.
    pub fn data(&self) -> &Arc<Data> {
        &self.config.data
    }
}

impl fmt::Debug for Hench {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Hench")
            .field("redis", &self.redis)
            .field("registry", &self.registry)
            .field("config", &self.config)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn built(builder: HenchBuilder) -> Result<Config> {
        let mut builder = builder;
        builder.validated_config()?;
        Ok(builder.config)
    }

    #[test]
    fn feature_settings_have_their_defaults_and_bounds() {
        let config = built(Hench::builder()).unwrap();
        assert!(config.unique_jobs);
        assert!(config.periodic_poller);
        assert_eq!(config.periodic_group, None);
        assert_eq!(config.periodic_poll_interval, Duration::from_secs(5));
        assert_eq!(config.periodic_history, 10);
        assert_eq!(config.periodic_lease, Duration::from_secs(60));
        assert_eq!(config.periodic_check_interval, Duration::from_secs(60));
        assert_eq!(config.batch_ttl, Duration::from_secs(30 * 86_400));
        assert_eq!(config.batch_linger, Duration::from_secs(86_400));
        assert_eq!(config.batch_max_commit_jobs, 10_000);
        assert_eq!(config.batch_commit_timeout, Duration::from_secs(30));
        assert!(config.batch_maintenance);
        assert_eq!(config.batch_maintenance_interval, Duration::from_secs(30));
        assert_eq!(config.batch_dispatch_grace, Duration::from_secs(10));
        assert!(config.throttles.is_empty());
        assert_eq!(config.over_limit_reschedules, 20);
        assert_eq!(config.over_limit_max_delay, Duration::from_secs(86_400));
        assert_eq!(
            config.batch_defaults(),
            crate::batch::BatchDefaults::default()
        );

        let config = built(
            Hench::builder()
                .unique_jobs(false)
                .periodic_group("billing")
                .periodic_poller(false)
                .periodic_poll_interval(Duration::from_millis(100))
                .periodic_lease(Duration::from_secs(2))
                .periodic_check_interval(Duration::from_millis(200))
                .periodic_history(0)
                .batch_ttl(Duration::from_secs(3_600))
                .batch_linger(Duration::from_secs(30))
                .batch_max_commit_jobs(100_000)
                .batch_maintenance(false)
                .throttle(crate::Throttle::new("stripe").concurrency(2))
                .over_limit_when(|_| None)
                .over_limit_reschedules(0)
                .over_limit_max_delay(Duration::from_secs(1)),
        )
        .unwrap();
        assert!(!config.unique_jobs);
        assert_eq!(config.periodic_group.as_deref(), Some("billing"));
        assert_eq!(config.throttles.len(), 1);
        assert!(config.over_limit_when.is_some());

        for (builder, expected) in [
            (
                Hench::builder()
                    .periodic_poll_interval(Duration::from_secs(40))
                    .periodic_lease(Duration::from_secs(60)),
                "periodic_lease",
            ),
            (
                Hench::builder().periodic_poll_interval(Duration::ZERO),
                "periodic_poll_interval",
            ),
            (Hench::builder().periodic_group(""), "periodic_group"),
            (
                Hench::builder().batch_ttl(Duration::from_secs(60)),
                "batch_ttl",
            ),
            (
                Hench::builder().batch_linger(Duration::from_secs(1)),
                "batch_linger",
            ),
            (
                Hench::builder().batch_max_commit_jobs(0),
                "batch_max_commit_jobs",
            ),
            (
                Hench::builder().batch_max_commit_jobs(100_001),
                "batch_max_commit_jobs",
            ),
            (
                Hench::builder().batch_maintenance_interval(Duration::ZERO),
                "batch_maintenance_interval",
            ),
            (
                Hench::builder().over_limit_max_delay(Duration::ZERO),
                "over_limit_max_delay",
            ),
        ] {
            let error = built(builder).unwrap_err().to_string();
            assert!(error.contains(expected), "{expected}: {error}");
        }
    }

    #[test]
    fn defaults_match_sidekiq() {
        let config = built(Hench::builder()).unwrap();
        assert_eq!(config.capsules.len(), 1);
        let capsule = &config.capsules[0];
        assert_eq!(capsule.name, "default");
        assert_eq!(capsule.concurrency, 5);
        assert_eq!(capsule.queues, ["default"]);
        assert_eq!(capsule.mode, QueueMode::Strict);
        assert_eq!(config.timeout, Duration::from_secs(25));
        assert_eq!(config.max_iteration_runtime, None);
        assert_eq!(config.max_retries, 25);
        assert_eq!(config.dead_max_jobs, 10_000);
        assert_eq!(config.dead_timeout, Duration::from_secs(180 * 86_400));
        assert_eq!(config.poll_interval_average, None);
        assert_eq!(
            config.average_scheduled_poll_interval,
            Duration::from_secs(5)
        );
        assert_eq!(config.heartbeat_interval, Duration::from_secs(10));
        assert_eq!(config.fetch, FetchStrategy::Reliable);
        assert_eq!(config.default_job_options, JobOptions::defaults());
        assert_eq!(config.error_handlers.len(), 1);
        assert!(config.job_logging);
        assert!(config.metrics);
        assert_eq!(config.systemd, Notifications::FromEnv);
        assert_eq!(config.total_concurrency(), 5);
    }

    #[test]
    fn capsules_and_queues() {
        let config = built(
            Hench::builder()
                .concurrency(10)
                .queues(["critical", "default", "low"])
                .capsule("serial", |c| c.concurrency(1).queues(["serial"]))
                .capsule("weighted", |c| c.queues(["a,3", "b, 1"]))
                .capsule("random", |c| c.weighted_queues([("x", 1), ("y", 1)])),
        )
        .unwrap();
        assert_eq!(config.capsules.len(), 4);
        assert_eq!(config.capsules[0].queues, ["critical", "default", "low"]);
        assert_eq!(config.capsules[0].concurrency, 10);
        let serial = config.capsule("serial").unwrap();
        assert_eq!((serial.concurrency, serial.mode), (1, QueueMode::Strict));
        let weighted = config.capsule("weighted").unwrap();
        assert_eq!(weighted.mode, QueueMode::Weighted);
        assert_eq!(weighted.weights, [("a".to_owned(), 3), ("b".to_owned(), 1)]);
        assert_eq!(weighted.expanded_queues(), ["a", "a", "a", "b"]);
        assert_eq!(config.capsule("random").unwrap().mode, QueueMode::Random);
        let max = built(Hench::builder().weighted_queues([("a", CapsuleConfig::MAX_WEIGHT)]));
        assert!(max.is_ok());
        assert_eq!(config.total_concurrency(), 21);
        assert_eq!(
            config.queue_names(),
            ["critical", "default", "low", "serial", "a", "b", "x", "y"]
        );
    }

    #[test]
    fn capsule_called_twice_is_updated() {
        let config = built(
            Hench::builder()
                .capsule("x", |c| c.concurrency(2))
                .capsule("x", |c| c.queues(["q"]))
                .capsule("default", |c| c.concurrency(3)),
        )
        .unwrap();
        assert_eq!(config.capsules.len(), 2);
        let x = config.capsule("x").unwrap();
        assert_eq!(
            (x.concurrency, x.queues.as_slice()),
            (2, ["q".to_owned()].as_slice())
        );
        assert_eq!(config.capsules[0].concurrency, 3);
    }

    #[test]
    fn invalid_configurations_are_rejected() {
        let cases: Vec<(&str, HenchBuilder)> = vec![
            ("concurrency", Hench::builder().concurrency(0)),
            ("no queues", Hench::builder().queues(Vec::<String>::new())),
            ("empty queue", Hench::builder().queues([""])),
            ("duplicate queue", Hench::builder().queues(["a", "a"])),
            ("bad weight", Hench::builder().queues(["a,x"])),
            (
                "huge weight",
                Hench::builder().weighted_queues([("a", 1_001)]),
            ),
            (
                "huge parsed weight",
                Hench::builder().queues(["a,5000", "b,1"]),
            ),
            ("capsule name", Hench::builder().capsule("", |c| c)),
            (
                "capsule concurrency",
                Hench::builder().capsule("c", |c| c.concurrency(0)),
            ),
            (
                "heartbeat",
                Hench::builder().heartbeat_interval(Duration::ZERO),
            ),
            // Other processes would see the process as dead between beats.
            (
                "slow heartbeat",
                Hench::builder().heartbeat_interval(Duration::from_secs(21)),
            ),
            (
                "very slow heartbeat",
                Hench::builder().heartbeat_interval(Duration::from_secs(90)),
            ),
            (
                "poll average",
                Hench::builder().poll_interval_average(Duration::ZERO),
            ),
            (
                "scheduled poll",
                Hench::builder().average_scheduled_poll_interval(Duration::ZERO),
            ),
            (
                "fetch timeout",
                Hench::builder().fetch_timeout(Duration::ZERO),
            ),
            (
                "orphan interval",
                Hench::builder().orphan_check_interval(Duration::ZERO),
            ),
            ("dead max", Hench::builder().dead_max_jobs(0)),
            (
                "iteration runtime",
                Hench::builder().max_iteration_runtime(Duration::ZERO),
            ),
            (
                "default queue",
                Hench::builder().default_job_options(JobOptions::new().queue("")),
            ),
        ];
        for (name, builder) in cases {
            assert!(
                matches!(built(builder), Err(Error::Config(_))),
                "{name} should be rejected"
            );
        }
        // The longest heartbeat interval accepted.
        let config =
            built(Hench::builder().heartbeat_interval(Config::MAX_HEARTBEAT_INTERVAL)).unwrap();
        assert_eq!(config.heartbeat_interval, Duration::from_secs(20));
        assert!(Config::MAX_HEARTBEAT_INTERVAL * 3 <= crate::keys::PROCESS_TTL);
    }

    #[test]
    fn duplicate_capsule_names_are_rejected() {
        let mut builder = Hench::builder();
        builder.capsules.push(CapsuleBuilder::new("default"));
        assert!(
            matches!(built(builder), Err(Error::Config(ref m)) if m.contains("duplicate capsule"))
        );
    }

    #[test]
    fn builder_settings() {
        let config = built(
            Hench::builder()
                .labels(["a", "b", "a"])
                .label("c")
                .tag("app")
                .job_logging(false)
                .metrics(false)
                .systemd(false)
                .fetch(FetchStrategy::Basic)
                .max_retries(3)
                .dead_max_jobs(7)
                .dead_timeout(Duration::from_secs(9))
                .timeout(Duration::from_secs(1))
                .max_iteration_runtime(Duration::from_secs(2))
                .poll_interval_average(Duration::from_millis(100))
                .average_scheduled_poll_interval(Duration::from_millis(50))
                .heartbeat_interval(Duration::from_millis(20))
                .initial_scheduler_wait(Duration::ZERO)
                .initial_scheduler_jitter(Duration::ZERO)
                .fetch_timeout(Duration::from_millis(30))
                .orphan_check_interval(Duration::from_millis(40))
                .default_job_options(JobOptions::new().queue("q")),
        )
        .unwrap();
        assert_eq!(config.labels, ["a", "b", "c"]);
        assert_eq!(config.tag.as_deref(), Some("app"));
        assert!(!config.job_logging);
        assert!(!config.metrics);
        assert_eq!(config.systemd, Notifications::Disabled);
        assert_eq!(config.fetch, FetchStrategy::Basic);
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.dead_max_jobs, 7);
        assert_eq!(config.dead_timeout, Duration::from_secs(9));
        assert_eq!(config.timeout, Duration::from_secs(1));
        assert_eq!(config.max_iteration_runtime, Some(Duration::from_secs(2)));
        assert_eq!(
            config.poll_interval_average,
            Some(Duration::from_millis(100))
        );
        assert_eq!(
            config.average_scheduled_poll_interval,
            Duration::from_millis(50)
        );
        assert_eq!(config.heartbeat_interval, Duration::from_millis(20));
        assert_eq!(config.initial_scheduler_wait, Duration::ZERO);
        assert_eq!(config.initial_scheduler_jitter, Duration::ZERO);
        assert_eq!(config.fetch_timeout, Duration::from_millis(30));
        assert_eq!(config.orphan_check_interval, Duration::from_millis(40));
        assert_eq!(config.default_job_options.queue.as_deref(), Some("q"));
        assert!(format!("{config:?}").contains("Basic"));
    }

    #[test]
    fn error_handlers_run_and_survive_panics() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let recorder = seen.clone();
        let config = built(
            Hench::builder()
                .clear_error_handlers()
                .error_handler(|_, _| panic!("handler bug"))
                .error_handler(move |error, ctx| {
                    recorder
                        .lock()
                        .unwrap()
                        .push(format!("{}: {}", ctx.context, error.message()));
                }),
        )
        .unwrap();
        assert_eq!(config.error_handlers.len(), 2);
        config.handle_error(&JobError::msg("boom"), &ErrorContext::new("testing"));
        assert_eq!(*seen.lock().unwrap(), ["testing: boom"]);

        let empty = built(Hench::builder().clear_error_handlers()).unwrap();
        empty.handle_error(&JobError::msg("logged"), &ErrorContext::new("no handlers"));
    }

    #[test]
    fn death_handlers_report_panics_as_errors() {
        let deaths = Arc::new(AtomicUsize::new(0));
        let errors = Arc::new(Mutex::new(Vec::new()));
        let (d, e) = (deaths.clone(), errors.clone());
        let config = built(
            Hench::builder()
                .death_handler(|_, _| panic!("death handler bug"))
                .death_handler(move |job, error| {
                    assert_eq!(job.class, "J");
                    assert_eq!(error.message(), "died");
                    d.fetch_add(1, Ordering::SeqCst);
                })
                .error_handler(move |error, ctx| {
                    e.lock()
                        .unwrap()
                        .push((ctx.context.to_owned(), error.class().to_owned()));
                }),
        )
        .unwrap();
        config.handle_death(&JobPayload::new("J", vec![]), &JobError::msg("died"));
        assert_eq!(deaths.load(Ordering::SeqCst), 1);
        assert_eq!(
            *errors.lock().unwrap(),
            [("Error calling death handler".to_owned(), "panic".to_owned())]
        );
    }

    #[tokio::test]
    async fn lifecycle_events_fire_in_order() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let (a, b) = (log.clone(), log.clone());
        let errors = Arc::new(AtomicUsize::new(0));
        let e = errors.clone();
        let config = built(
            Hench::builder()
                .on_startup(move || {
                    let a = a.clone();
                    async move { a.lock().unwrap().push("first") }
                })
                .on_startup(|| async { panic!("callback bug") })
                .on_startup(move || {
                    let b = b.clone();
                    async move { b.lock().unwrap().push("second") }
                })
                .on_quiet(|| async {})
                .on_shutdown(|| async {})
                .on_exit(|| async {})
                .on_heartbeat(|| async {})
                .on_beat(|| async {})
                .error_handler(move |error, ctx| {
                    assert_eq!(ctx.event, Some(LifecycleEvent::Startup));
                    assert!(error.is_panic());
                    e.fetch_add(1, Ordering::SeqCst);
                }),
        )
        .unwrap();
        config.fire_event(LifecycleEvent::Startup, false).await;
        assert_eq!(*log.lock().unwrap(), ["first", "second"]);
        config.fire_event(LifecycleEvent::Startup, true).await;
        assert_eq!(*log.lock().unwrap(), ["first", "second", "second", "first"]);
        assert_eq!(errors.load(Ordering::SeqCst), 2);
        for event in LifecycleEvent::ALL {
            assert!(!config.callbacks(event).is_empty(), "{event}");
        }
        config.fire_event(LifecycleEvent::Beat, false).await;
    }

    #[test]
    fn lifecycle_event_names() {
        let names: Vec<&str> = LifecycleEvent::ALL.iter().map(|e| e.name()).collect();
        assert_eq!(
            names,
            ["startup", "quiet", "shutdown", "exit", "heartbeat", "beat"]
        );
        assert_eq!(LifecycleEvent::Beat.to_string(), "beat");
        assert_eq!(QueueMode::Weighted.as_str(), "weighted");
    }

    #[tokio::test]
    async fn build_detects_duplicate_jobs() {
        #[derive(Default)]
        struct Dup;
        impl Job for Dup {
            const NAME: &'static str = "Dup";
            type Args = ();
            async fn perform(&self, _: &crate::JobContext, (): ()) -> Result<(), JobError> {
                Ok(())
            }
        }
        let result = Hench::builder()
            .redis(RedisPool::detached())
            .register(Dup)
            .register_default::<Dup>()
            .build()
            .await;
        assert!(matches!(result, Err(Error::Config(ref m)) if m.contains("Dup")));

        let hench = Hench::builder()
            .redis(RedisPool::detached())
            .register(Dup)
            .data(5_u8)
            .build()
            .await
            .unwrap();
        assert!(hench.registry().contains("Dup"));
        assert_eq!(hench.data().get::<u8>(), Some(&5));
        assert!(hench.redis().is_detached());
        assert!(!hench.client().is_fake());
        assert!(format!("{hench:?}").contains("Dup"));
    }

    #[tokio::test]
    async fn build_reports_bad_redis_urls() {
        let result = Hench::builder().redis_url("http://nope").build().await;
        assert!(matches!(result, Err(Error::Config(_))));
    }
}
