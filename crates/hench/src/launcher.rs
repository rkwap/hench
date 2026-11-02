//! The launcher: starts the managers, the scheduler and the heartbeat, handles remote
//! signals and lifecycle events, and shuts everything down. See `docs/DESIGN.md` §5.
//!
//! [`Hench::start`] registers the process (a first heartbeat) and spawns, all driven by
//! cancellation tokens:
//!
//! * one manager per capsule, each with `concurrency` processors;
//! * the scheduler, which enqueues due scheduled jobs and retries;
//! * the heartbeat, which every `heartbeat_interval` flushes the stats, publishes the work
//!   state and the process info, and executes remote signals (`TSTP` quiets the process,
//!   `TERM` stops it);
//! * with reliable fetch, the orphan check, which recovers the jobs of dead processes;
//! * a supervisor, which performs the shutdown when one is requested.
//!
//! Shutdown ([`Launcher::stop`], a remote `TERM`, a signal in [`Hench::run`] or the future
//! given to [`Hench::run_until`]): quiet the process, signal running jobs
//! ([`JobContext::stopping`](crate::JobContext::stopping)), wait for them up to `timeout`,
//! interrupt and requeue the rest, push back anything left in the in-progress list, remove
//! the process from Redis and flush the stats. Lifecycle events fire along the way.
//
// LANE: engine — this module (including the `impl Hench` block below) is owned by the engine
// lane.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError, Weak};
use std::time::Duration;

use ::redis::Script;
use futures::FutureExt;
use serde_json::{Map, Value, json};
use tokio::sync::Notify;
use tokio::task::AbortHandle;
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::client::Client;
use crate::config::{CapsuleConfig, Config, ErrorContext, FetchStrategy, Hench, LifecycleEvent};
use crate::error::{Error, JobError, Result};
use crate::fetch::{Fetcher, Held, Stranded};
use crate::job::Registry;
use crate::job_logger::JobLogger;
use crate::keys;
use crate::limit::{HeldPermits, Throttles};
use crate::manager::Manager;
use crate::metrics::Tracker;
use crate::payload::{now_secs_f64, random_hex};
use crate::redis::{RedisConnection, RedisPool};
use crate::scheduled;
use crate::stats::{Stats, rss_kb};
use crate::systemd::{Notifications, Notifier};

/// Oldest Redis version hench works with (it needs `LMOVE`, `BLMOVE` and `ZRANGE ... BYSCORE`).
const MIN_REDIS_VERSION: (u64, u64, u64) = (6, 2, 0);

/// How long a shutdown waits for the heartbeat and the other background tasks to stop
/// before aborting them.
const BACKGROUND_STOP_TIMEOUT: Duration = Duration::from_secs(5);

/// Time allowed to each Redis cleanup step at the end of a shutdown, so that an unreachable
/// Redis (connection attempts can take several seconds each) cannot hold the process.
pub(crate) const CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);

/// Number of round-trip readings kept; a warning is logged when all are slow.
const RTT_READINGS: usize = 5;

/// Round trips above this many microseconds are slow.
const RTT_WARNING_US: u64 = 50_000;

/// The state shared by the tasks of a running process.
pub(crate) struct Engine {
    pub(crate) config: Arc<Config>,
    pub(crate) redis: RedisPool,
    pub(crate) registry: Arc<Registry>,
    pub(crate) client: Client,
    pub(crate) identity: String,
    pub(crate) stats: Stats,
    /// Execution metrics, unless disabled.
    pub(crate) metrics: Option<Tracker>,
    /// Notifies systemd, when the process runs as a notify service.
    notifier: Option<Notifier>,
    pub(crate) logger: JobLogger,
    /// Cancelled when the process stops fetching new jobs.
    pub(crate) quiet: CancellationToken,
    /// Cancelled when the shutdown starts; jobs see it through `JobContext::stopping`.
    pub(crate) stopping: CancellationToken,
    /// Cancelled at the shutdown deadline: running jobs are interrupted.
    pub(crate) hard: CancellationToken,
    /// Cancelled to stop waiting for running jobs during a shutdown.
    pub(crate) force: CancellationToken,
    /// Cancelled to request a shutdown.
    stop_requested: CancellationToken,
    /// Cancelled at the end of the shutdown to stop the heartbeat.
    pub(crate) background: CancellationToken,
    /// Cancelled once the process has stopped.
    done: CancellationToken,
    /// Asks the heartbeat to beat now.
    beat_now: Notify,
    quieted: AtomicBool,
    /// Cancelled once the `quiet` callbacks have run (or their task ended).
    quiet_done: CancellationToken,
    hostname: String,
    started_at: f64,
    embedded: bool,
    /// Failures of the processors' fetches.
    pub(crate) fetch_outage: Outage,
    /// Failures of the scheduler.
    pub(crate) schedule_outage: Outage,
    /// The jobs the processors hold (reliable fetch): the rest of the in-progress list is
    /// pushed back by [`reconcile_in_progress`].
    pub(crate) held: Arc<Held>,
    /// Failures of the periodic poller.
    #[cfg(feature = "periodic")]
    pub(crate) periodic_outage: Outage,
    /// Failures of batch maintenance.
    pub(crate) batch_maintenance_outage: Outage,
    /// The throttles of this process, by name (the jobs' own, then the builder's).
    pub(crate) throttles: Throttles,
    /// The throttle permits the engine holds for running jobs, by worker slot.
    pub(crate) permits: HeldPermits,
    /// When the jobs this process parked in the schedule come due, for the scheduler.
    pub(crate) wakeups: crate::scheduled::Wakeups,
    tasks: Mutex<Vec<AbortHandle>>,
}

impl Engine {
    fn new(hench: &Hench, hostname: String, embedded: bool) -> Self {
        let identity = keys::identity(&hostname, std::process::id(), &random_hex(6));
        Self {
            config: hench.config.clone(),
            redis: hench.redis.clone(),
            registry: hench.registry.clone(),
            client: hench.client.clone(),
            identity,
            stats: Stats::new(),
            metrics: hench.config.metrics.then(Tracker::new),
            notifier: notifier(&hench.config.systemd, embedded),
            logger: JobLogger::new(hench.config.job_logging),
            quiet: CancellationToken::new(),
            stopping: CancellationToken::new(),
            hard: CancellationToken::new(),
            force: CancellationToken::new(),
            stop_requested: CancellationToken::new(),
            background: CancellationToken::new(),
            done: CancellationToken::new(),
            beat_now: Notify::new(),
            quieted: AtomicBool::new(false),
            quiet_done: CancellationToken::new(),
            hostname,
            started_at: now_secs_f64(),
            embedded,
            fetch_outage: Outage::new("could not fetch jobs", "fetching jobs"),
            schedule_outage: Outage::new(
                "could not enqueue scheduled jobs",
                "enqueueing scheduled jobs",
            ),
            held: Arc::default(),
            #[cfg(feature = "periodic")]
            periodic_outage: Outage::new(
                "could not enqueue periodic jobs",
                "enqueueing periodic jobs",
            ),
            batch_maintenance_outage: Outage::new(
                "could not maintain batches",
                "maintaining batches",
            ),
            throttles: process_throttles(hench),
            permits: HeldPermits::default(),
            wakeups: crate::scheduled::Wakeups::default(),
            tasks: Mutex::new(Vec::new()),
        }
    }

    /// Remembers a spawned task (so that a test can simulate a crash by aborting them all).
    pub(crate) fn track(&self, task: AbortHandle) {
        lock(&self.tasks).push(task);
    }

    /// Stops fetching new jobs and fires the `quiet` event in a task of its own, once; returns
    /// at once. Used where waiting for the callbacks would hold up something else (the
    /// heartbeat handling a remote quiet, the signal loop of [`Hench::run`]).
    pub(crate) fn begin_quiet(self: &Arc<Self>) {
        if self.quieted.swap(true, Ordering::SeqCst) {
            return;
        }
        tracing::info!(identity = %self.identity, "quiet: no longer fetching new jobs");
        self.quiet.cancel();
        self.beat_now.notify_one();
        self.notify(&format!("STATUS={}", self.status()));
        let engine = self.clone();
        let callbacks = tokio::spawn(async move {
            // Releases the waiters however the task ends (aborted by a simulated crash too).
            let _done = engine.quiet_done.clone().drop_guard();
            engine.config.fire_event(LifecycleEvent::Quiet, true).await;
        });
        self.track(callbacks.abort_handle());
    }

    /// Stops fetching new jobs (once) and waits until the `quiet` callbacks have run, whoever
    /// started them.
    pub(crate) async fn quiet(self: &Arc<Self>) {
        self.begin_quiet();
        self.quiet_done.cancelled().await;
    }

    /// Sends `state` to systemd, if the process runs as a notify service.
    fn notify(&self, state: &str) {
        if let Some(notifier) = &self.notifier {
            notifier.send_quietly(state);
        }
    }

    /// Logs what the process is doing, at `WARN` so that the default level shows it (Sidekiq
    /// dumps its threads' backtraces on `TTIN`): a summary, then one line per busy slot with
    /// its job (jid, class, queue) and how long it has run. `source` names the signal.
    pub(crate) fn dump(&self, source: &str) {
        let running = self.stats.running();
        tracing::warn!(
            signal = %source,
            identity = %self.identity,
            busy = running.len(),
            concurrency = self.config.total_concurrency(),
            quiet = self.quiet.is_cancelled(),
            stopping = self.stop_requested.is_cancelled(),
            "dump of the running jobs"
        );
        let now = now_secs_f64();
        for (slot, work) in running {
            /// The fields of a job the dump shows.
            #[derive(Default, serde::Deserialize)]
            struct Head {
                class: Option<String>,
                jid: Option<String>,
            }
            let head: Head = serde_json::from_slice(&work.payload).unwrap_or_default();
            tracing::warn!(
                slot = %slot,
                jid = %head.jid.as_deref().unwrap_or("?"),
                class = %head.class.as_deref().unwrap_or("?"),
                queue = %work.queue,
                running_secs = ((now - work.run_at).max(0.0) * 1000.0).round() / 1000.0,
                "running job"
            );
        }
    }

    /// A one-line description of the process for systemd's `STATUS`.
    fn status(&self) -> String {
        let busy = self.stats.busy();
        let concurrency = self.config.total_concurrency();
        if self.stop_requested.is_cancelled() {
            format!("stopping: {busy} jobs still running")
        } else if self.quiet.is_cancelled() {
            format!("quiet: {busy} of {concurrency} busy, not fetching new jobs")
        } else {
            format!("processing jobs: {busy} of {concurrency} busy")
        }
    }
}

/// How often a failure that keeps happening is logged again.
const OUTAGE_LOG_INTERVAL: Duration = Duration::from_secs(60);

/// Reports the failures of a background activity (fetching jobs, enqueueing scheduled jobs)
/// without flooding: at most one notice a minute, the first of an episode of failures going
/// to the error handlers and the following ones to the log; the end of a reported episode is
/// logged. Episodes that start within a minute of a notice (an activity that keeps failing
/// and recovering) are only logged at debug level until a minute has passed.
pub(crate) struct Outage {
    /// Context given to the error handlers.
    context: &'static str,
    /// What is failing, for the logs.
    activity: &'static str,
    /// `true` during an episode (a fast path for `succeeded`; written under the lock).
    failing: AtomicBool,
    state: Mutex<OutageState>,
}

#[derive(Default)]
struct OutageState {
    /// Start of the current episode.
    since: Option<std::time::Instant>,
    /// Whether the current episode was reported to the error handlers.
    reported: bool,
    /// Last notice (report or log), across episodes.
    noticed: Option<std::time::Instant>,
    failures: u64,
}

impl Outage {
    fn new(context: &'static str, activity: &'static str) -> Self {
        Self {
            context,
            activity,
            failing: AtomicBool::new(false),
            state: Mutex::new(OutageState::default()),
        }
    }

    /// Records a failure.
    pub(crate) fn failed(&self, config: &Config, error: Error) {
        self.failed_with_job(config, error, None);
    }

    /// Records a failure concerning a job (its JSON is given to the error handlers and logs).
    pub(crate) fn failed_with_job(&self, config: &Config, error: Error, jobstr: Option<&str>) {
        let now = std::time::Instant::now();
        let mut state = lock(&self.state);
        let since = *state.since.get_or_insert(now);
        self.failing.store(true, Ordering::Release);
        state.failures += 1;
        let due = state
            .noticed
            .is_none_or(|noticed| now.duration_since(noticed) >= OUTAGE_LOG_INTERVAL);
        if !due {
            drop(state);
            tracing::debug!(%error, job = jobstr, "{}", self.context);
            return;
        }
        state.noticed = Some(now);
        let first = !state.reported;
        state.reported = true;
        let failures = state.failures;
        drop(state);
        if first {
            let mut ctx = ErrorContext::new(self.context);
            if let Some(jobstr) = jobstr {
                ctx = ctx.with_jobstr(jobstr);
            }
            config.handle_error(&error.into_job_error(), &ctx);
        } else {
            tracing::warn!(
                %error,
                failures,
                failing_secs = now.duration_since(since).as_secs(),
                job = jobstr,
                "{} keeps failing",
                self.activity
            );
        }
    }

    /// Records a success, ending an episode of failures.
    pub(crate) fn succeeded(&self) {
        if !self.failing.load(Ordering::Acquire) {
            return;
        }
        let mut state = lock(&self.state);
        if let Some(since) = state.since.take()
            && state.reported
        {
            tracing::info!(
                downtime_secs = since.elapsed().as_secs_f64(),
                failures = state.failures,
                "{} works again",
                self.activity
            );
        }
        state.failures = 0;
        state.reported = false;
        self.failing.store(false, Ordering::Release);
    }
}

/// The instant `duration` from now, or a far future one if that overflows.
fn later(duration: Duration) -> Instant {
    let now = Instant::now();
    now.checked_add(duration)
        .unwrap_or_else(|| now + Duration::from_secs(100 * 365 * 24 * 60 * 60))
}

/// Locks a mutex that only holds plain data (a poisoned lock is still usable).
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A running hench process, returned by [`Hench::start`]. Cheap to clone; every clone controls
/// the same process.
///
/// Dropping it does not stop the process: call [`Launcher::stop`].
#[derive(Clone)]
#[must_use = "dropping a Launcher does not stop it; call `stop`"]
pub struct Launcher {
    engine: Arc<Engine>,
}

impl Launcher {
    /// The process identity (`hostname:pid:nonce`), as shown in the dashboard and used for
    /// remote signals.
    pub fn identity(&self) -> &str {
        &self.engine.identity
    }

    /// Stops fetching new jobs; running jobs continue and the heartbeat reports the process
    /// as quiet. Fires the `quiet` event the first time, and returns once its callbacks have
    /// run (also when a remote quiet started them).
    pub async fn quiet(&self) {
        self.engine.quiet().await;
    }

    /// Shuts down: quiets, waits up to the configured timeout for running jobs, interrupts and
    /// requeues the rest and removes the process from Redis. Returns once the process has
    /// stopped (immediately if it already has). Errors talking to Redis during the shutdown
    /// are logged, not returned.
    pub async fn stop(&self) -> Result<()> {
        self.engine.stop_requested.cancel();
        self.engine.done.cancelled().await;
        Ok(())
    }

    /// Waits until the process has stopped, whatever the reason: [`Launcher::stop`], a
    /// remote stop signal (`TERM` from the dashboard) or a signal handled by [`Hench::run`].
    pub async fn wait(&self) {
        self.engine.done.cancelled().await;
    }

    /// `true` once the process stopped fetching new jobs.
    pub fn is_quiet(&self) -> bool {
        self.engine.quiet.is_cancelled()
    }

    /// `true` once a shutdown was requested.
    pub fn is_stopping(&self) -> bool {
        self.engine.stop_requested.is_cancelled()
    }

    /// `true` once the process has stopped.
    pub fn is_stopped(&self) -> bool {
        self.engine.done.is_cancelled()
    }

    /// Requests a shutdown that does not wait for running jobs: they are interrupted and
    /// requeued right away.
    pub(crate) fn force_stop(&self) {
        self.engine.force.cancel();
        self.engine.stop_requested.cancel();
    }

    /// Simulates a crash: aborts every task of the process without any cleanup, leaving its
    /// heartbeat, work state and in-progress list in Redis. For tests only.
    #[cfg(feature = "test-util")]
    #[doc(hidden)]
    pub fn abandon(&self) {
        let tasks = std::mem::take(&mut *lock(&self.engine.tasks));
        for task in tasks {
            task.abort();
        }
        // Also stops the liveness thread, so that the heartbeat key expires as after a crash.
        self.engine.background.cancel();
        self.engine.quiet.cancel();
        self.engine.done.cancel();
    }
}

impl fmt::Debug for Launcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Launcher")
            .field("identity", &self.engine.identity)
            .field("quiet", &self.is_quiet())
            .field("stopping", &self.is_stopping())
            .field("stopped", &self.is_stopped())
            .finish()
    }
}

impl Hench {
    /// Starts processing jobs in background tasks and returns a handle to stop them.
    ///
    /// Checks that Redis is reachable and recent enough (6.2+), fires the `startup` event,
    /// registers the process with a first heartbeat, then starts the processors, the
    /// scheduler and the heartbeat. The process is not stopped when the returned
    /// [`Launcher`] is dropped: call [`Launcher::stop`]. Remote signals from the dashboard
    /// (quiet, stop) work in this mode too.
    pub async fn start(&self) -> Result<Launcher> {
        self.launch(true).await
    }

    /// Processes jobs until SIGINT or SIGTERM (stop) is received; SIGTSTP quiets the process,
    /// and SIGTTIN logs the jobs it is running (see [`api::Process::dump`](crate::api::Process::dump)).
    /// A second SIGINT or SIGTERM during the shutdown interrupts the running jobs right away.
    /// Also returns when the process is stopped remotely (`TERM` from the dashboard).
    ///
    /// Await the returned future to completion: it returns once the shutdown is over. If it is
    /// dropped instead, the process shuts down gracefully in the background (as long as the
    /// tokio runtime keeps running).
    pub async fn run(self) -> Result<()> {
        // Listen before starting, so that no signal is missed.
        let mut signals = OsSignals::new()?;
        let launcher = self.launch(false).await?;
        let _stop_when_dropped = launcher.engine.stop_requested.clone().drop_guard();
        loop {
            tokio::select! {
                signal = signals.recv() => match signal {
                    OsSignal::Quiet(name) => {
                        tracing::info!(signal = name, "no longer accepting new jobs");
                        // The quiet callbacks run in a task of their own: a stop signal is
                        // still handled meanwhile (the shutdown waits for them).
                        launcher.engine.begin_quiet();
                    }
                    OsSignal::Stop(name) => {
                        tracing::info!(signal = name, "shutting down");
                        let stop = launcher.stop();
                        tokio::pin!(stop);
                        loop {
                            tokio::select! {
                                result = &mut stop => return result,
                                signal = signals.recv() => match signal {
                                    OsSignal::Stop(name) => {
                                        tracing::warn!(
                                            signal = name,
                                            "second stop signal: interrupting running jobs now"
                                        );
                                        launcher.force_stop();
                                    }
                                    // What a shutdown waits for.
                                    OsSignal::Dump(name) => launcher.engine.dump(name),
                                    OsSignal::Quiet(_) => {}
                                }
                            }
                        }
                    }
                    OsSignal::Dump(name) => launcher.engine.dump(name),
                },
                () = launcher.wait() => return Ok(()),
            }
        }
    }

    /// Processes jobs until `shutdown` completes, then shuts down gracefully. Use it to
    /// embed hench in another server:
    ///
    /// ```no_run
    /// # async fn demo(hench: hench::Hench) -> hench::Result<()> {
    /// let token = hench::CancellationToken::new();
    /// let workers = hench.run_until(token.clone().cancelled_owned());
    /// // ... serve requests, then `token.cancel()` to stop the workers ...
    /// workers.await
    /// # }
    /// ```
    ///
    /// Also returns when the process is stopped remotely (`TERM` from the dashboard).
    ///
    /// Await the returned future to completion: it returns once the shutdown is over. If it is
    /// dropped instead (the losing branch of a `select!`, a timeout), the workers shut down
    /// gracefully in the background, as long as the tokio runtime keeps running.
    pub async fn run_until<F>(self, shutdown: F) -> Result<()>
    where
        F: Future<Output = ()> + Send,
    {
        let launcher = self.launch(true).await?;
        let _stop_when_dropped = launcher.engine.stop_requested.clone().drop_guard();
        tokio::select! {
            () = shutdown => launcher.stop().await,
            () = launcher.wait() => Ok(()),
        }
    }

    async fn launch(&self, embedded: bool) -> Result<Launcher> {
        let config = &self.config;
        let Some(first_capsule) = config.capsules.first() else {
            return Err(Error::Config("no capsule is configured".to_owned()));
        };
        check_redis(&self.redis).await?;
        crate::pro::start_checks(&self.redis).await;
        warn_if_panics_abort();
        let engine = Arc::new(Engine::new(self, hostname(), embedded));
        tracing::info!(
            identity = %engine.identity,
            concurrency = config.total_concurrency(),
            queues = ?config.queue_names(),
            fetch = ?config.fetch,
            jobs = ?self.registry.names(),
            "starting"
        );
        if self.registry.is_empty() {
            tracing::warn!(
                "no job is registered: every job this process takes will fail with \
                 hench::UnknownJob. A #[hench::job] registers itself only in binaries that link \
                 the crate defining it: refer to that crate from this binary (`use my_jobs as \
                 _;`), or register the jobs with HenchBuilder::register"
            );
        }
        // A failing startup callback stops the start here, before anything is registered.
        config.fire_startup().await.map_err(Error::Startup)?;

        // Register the process before fetching anything: the orphan check of other processes
        // treats in-progress jobs of processes without a heartbeat as abandoned.
        let mut heartbeat = Heartbeat::new(engine.clone());
        heartbeat.beat().await?;

        let fetcher = Fetcher::new(
            config.fetch,
            self.redis.clone(),
            engine.identity.clone(),
            first_capsule,
        )
        .with_timeout(config.fetch_timeout)
        .with_orphan_grace(config.effective_orphan_grace());
        spawn_liveness(&engine);
        let mut background = Background::default();
        background.spawn(&engine, heartbeat.run());
        if let Some(notifier) = engine.notifier.clone()
            && let Some(interval) = notifier.keep_alive_interval()
        {
            background.spawn(
                &engine,
                keep_watchdog_alive(engine.background.clone(), notifier, interval),
            );
        }
        if config.fetch == FetchStrategy::Reliable {
            background.spawn(&engine, recover_orphans(engine.clone(), fetcher.clone()));
            background.spawn(
                &engine,
                reconcile_in_progress(engine.clone(), fetcher.clone()),
            );
        }
        background.spawn(&engine, scheduled::run(engine.clone()));
        for task in crate::pro::background_tasks(&engine) {
            background.spawn(&engine, task);
        }
        background.tracker.close();
        let managers: Vec<Manager> = config
            .capsules
            .iter()
            .map(|capsule| Manager::start(&engine, capsule))
            .collect();
        let supervisor = tokio::spawn(supervise(engine.clone(), managers, background, fetcher));
        engine.track(supervisor.abort_handle());
        engine.notify(&format!("READY=1\nSTATUS={}", engine.status()));
        Ok(Launcher { engine })
    }
}

/// The background tasks of a process: heartbeat, scheduler, orphan check.
#[derive(Default)]
struct Background {
    tracker: TaskTracker,
    tasks: Vec<AbortHandle>,
}

impl Background {
    fn spawn<F>(&mut self, engine: &Engine, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let handle = self.tracker.spawn(task).abort_handle();
        engine.track(handle.clone());
        self.tasks.push(handle);
    }

    /// Waits for the tasks to finish; aborts them if they take too long.
    async fn stop(&self) {
        if tokio::time::timeout(BACKGROUND_STOP_TIMEOUT, self.tracker.wait())
            .await
            .is_err()
        {
            tracing::warn!("background tasks did not stop in time; aborting them");
            for task in &self.tasks {
                task.abort();
            }
        }
    }
}

/// Waits for a shutdown request and performs it.
async fn supervise(
    engine: Arc<Engine>,
    managers: Vec<Manager>,
    background: Background,
    fetcher: Fetcher,
) {
    // Whatever happens, waiters are released at the end.
    let _done = engine.done.clone().drop_guard();
    engine.stop_requested.cancelled().await;
    let shutdown = shutdown(&engine, &managers, &background, &fetcher);
    if let Err(panic) = AssertUnwindSafe(shutdown).catch_unwind().await {
        engine.config.handle_error(
            &JobError::panic(panic),
            &ErrorContext::new("the shutdown panicked"),
        );
    }
}

async fn shutdown(
    engine: &Arc<Engine>,
    managers: &[Manager],
    background: &Background,
    fetcher: &Fetcher,
) {
    let config = &engine.config;
    let deadline = later(config.timeout);
    let running: usize = managers.iter().map(Manager::running).sum();
    tracing::info!(
        identity = %engine.identity,
        timeout = ?config.timeout,
        processors = running,
        "shutting down"
    );
    engine.notify(&format!(
        "STOPPING=1\nSTATUS=stopping: waiting up to {}s for running jobs",
        config.timeout.as_secs()
    ));
    // The quiet callbacks (already running after a remote or SIGTSTP quiet, or started now)
    // finish before the shutdown ones fire, within the shutdown's deadline.
    tokio::select! {
        () = engine.quiet() => {}
        () = tokio::time::sleep_until(deadline) => tracing::warn!(
            "the quiet callbacks are still running at the shutdown deadline; shutting down anyway"
        ),
        () = engine.force.cancelled() => tracing::warn!(
            "the shutdown was forced while the quiet callbacks are still running"
        ),
    }
    engine.stopping.cancel();
    let stop_managers = futures::future::join_all(
        managers
            .iter()
            .map(|manager| manager.stop(engine, deadline)),
    );
    tokio::join!(
        stop_managers,
        config.fire_event(LifecycleEvent::Shutdown, true)
    );

    engine.background.cancel();
    background.stop().await;
    match tokio::time::timeout(CLEANUP_TIMEOUT, fetcher.release(config)).await {
        Ok(Ok(0)) => {}
        Ok(Ok(jobs)) => {
            tracing::warn!(jobs, "pushed jobs left in progress back to their queues");
        }
        Ok(Err(error)) => tracing::warn!(
            %error,
            "could not push jobs left in progress back to their queues; another process will \
             recover them"
        ),
        Err(_) => tracing::warn!(
            "Redis did not answer; jobs left in progress will be recovered by another process"
        ),
    }
    // The process disappears from the dashboard first, then the last counts are flushed.
    if tokio::time::timeout(CLEANUP_TIMEOUT, unregister(engine))
        .await
        .is_err()
    {
        tracing::warn!("Redis did not answer; could not remove the process from Redis");
    }
    match tokio::time::timeout(CLEANUP_TIMEOUT, engine.stats.flush(&engine.redis)).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::warn!(%error, "could not flush stats"),
        Err(_) => tracing::warn!("Redis did not answer; could not flush stats"),
    }
    if let Some(metrics) = &engine.metrics {
        match tokio::time::timeout(CLEANUP_TIMEOUT, metrics.flush(&engine.redis)).await {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => tracing::warn!(%error, "could not flush job metrics"),
            Err(_) => tracing::warn!("Redis did not answer; could not flush job metrics"),
        }
    }
    config.fire_event(LifecycleEvent::Exit, true).await;
    tracing::info!(identity = %engine.identity, "stopped");
}

/// Removes the process from Redis (best effort).
async fn unregister(engine: &Engine) {
    let mut pipe = ::redis::pipe();
    pipe.cmd("SREM")
        .arg(keys::PROCESSES)
        .arg(&engine.identity)
        .ignore()
        .cmd("UNLINK")
        .arg(keys::work(&engine.identity))
        .ignore();
    let result = async {
        let mut conn = engine.redis.conn().await?;
        pipe.exec_async(&mut conn).await?;
        Ok::<(), Error>(())
    }
    .await;
    if let Err(error) = result {
        tracing::warn!(%error, "could not remove the process from Redis");
    }
}

/// Recovers the jobs of dead processes at startup and then periodically, until quiet. A process
/// found without a heartbeat key is only taken for dead if it still has none after the orphan
/// grace (see [`Fetcher::with_orphan_grace`]).
async fn recover_orphans(engine: Arc<Engine>, fetcher: Fetcher) {
    let interval = engine.config.orphan_check_interval;
    loop {
        let recovered = fetcher
            .recover_orphans_until(interval, &engine.config, Some(&engine.quiet))
            .await;
        match recovered {
            Ok(0) => {}
            Ok(jobs) => tracing::info!(jobs, "recovered jobs held by dead processes"),
            Err(error) => tracing::warn!(%error, "could not check for jobs held by dead processes"),
        }
        if engine
            .quiet
            .run_until_cancelled(tokio::time::sleep(interval))
            .await
            .is_none()
        {
            return;
        }
    }
}

/// How often (at most) a process looks for jobs stranded in its own in-progress list; more
/// often when `orphan_check_interval` is shorter.
const RECONCILE_INTERVAL: Duration = Duration::from_secs(30);

/// A reconciliation check this much later than planned means the runtime was stalled.
const RECONCILE_STALL: Duration = Duration::from_secs(1);

/// Pushes back the jobs stranded in this process's own in-progress list (see
/// [`Fetcher::reconcile`]), until quiet: a job whose fetch reply was lost (a timeout, a dropped
/// connection) would otherwise wait there until the process stops. A job counts as stranded
/// once no processor has held it for longer than a fetch can wait for its reply (the fetch
/// timeout plus the blocking connection's margin, plus a second), measured on checks that ran
/// on time: after a stall of the runtime (a check much later than planned), a reply may be
/// waiting for its processor, so the clocks start again.
async fn reconcile_in_progress(engine: Arc<Engine>, fetcher: Fetcher) {
    let config = &engine.config;
    let interval = config.orphan_check_interval.min(RECONCILE_INTERVAL);
    let margin = engine
        .redis
        .config()
        .response_timeout
        .max(Duration::from_secs(1));
    let grace = config
        .fetch_timeout
        .saturating_add(margin)
        .saturating_add(Duration::from_secs(1));
    let mut stranded = Stranded::default();
    let mut planned = later(interval);
    while engine
        .quiet
        .run_until_cancelled(tokio::time::sleep_until(planned))
        .await
        .is_some()
    {
        let now = Instant::now();
        if now.saturating_duration_since(planned) > RECONCILE_STALL {
            stranded.forget();
        }
        planned = later(interval);
        if let Err(error) = fetcher
            .reconcile(&engine.held, &mut stranded, grace, config)
            .await
        {
            tracing::debug!(%error, "could not check this process's in-progress list");
        }
    }
}

/// Publishes the process state to Redis and executes remote signals.
struct Heartbeat {
    engine: Arc<Engine>,
    info: String,
    rtt: VecDeque<u64>,
}

impl Heartbeat {
    fn new(engine: Arc<Engine>) -> Self {
        let info = process_info(&engine).to_string();
        Self {
            engine,
            info,
            rtt: VecDeque::with_capacity(RTT_READINGS),
        }
    }

    /// Beats every `heartbeat_interval` until the end of the shutdown, telling systemd what
    /// the process is doing after each beat. (The heartbeat key's expiry is also refreshed by
    /// [`spawn_liveness`], and watchdog keep-alives come from [`keep_watchdog_alive`], so that
    /// neither depends on how long a beat takes.)
    async fn run(mut self) {
        let period = self.engine.config.heartbeat_interval;
        let mut ticker = tokio::time::interval_at(later(period), period);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                () = self.engine.background.cancelled() => return,
                // Publish a state change (quiet) right away.
                () = self.engine.beat_now.notified() => ticker.reset(),
                _ = ticker.tick() => {}
            }
            if let Err(error) = self.beat().await {
                tracing::error!(%error, "heartbeat failed");
            }
            self.keep_alive();
        }
    }

    /// Tells systemd what the process is doing (and that it is alive, under a watchdog).
    fn keep_alive(&self) {
        let engine = &self.engine;
        let Some(notifier) = &engine.notifier else {
            return;
        };
        let status = engine.status();
        if notifier.watchdog().is_some() {
            notifier.send_quietly(&format!("WATCHDOG=1\nSTATUS={status}"));
        } else {
            notifier.send_quietly(&format!("STATUS={status}"));
        }
    }

    /// One heartbeat.
    async fn beat(&mut self) -> Result<()> {
        let engine = self.engine.clone();
        let redis = &engine.redis;
        let identity = engine.identity.as_str();
        if let Err(error) = engine.stats.flush(redis).await {
            tracing::warn!(%error, "could not flush stats");
        }
        if let Some(metrics) = &engine.metrics
            && let Err(error) = metrics.flush(redis).await
        {
            tracing::warn!(%error, "could not flush job metrics");
        }

        let work = engine.stats.work_snapshot();
        let busy = work.len();
        let work_key = keys::work(identity);
        let mut pipe = ::redis::pipe();
        pipe.atomic().cmd("UNLINK").arg(&work_key).ignore();
        if !work.is_empty() {
            pipe.cmd("HSET").arg(&work_key).arg(&work).ignore();
            pipe.cmd("EXPIRE")
                .arg(&work_key)
                .arg(keys::PROCESS_TTL.as_secs())
                .ignore();
        }
        let pipe = &pipe;
        redis
            .with_retry(|mut conn| async move { pipe.exec_async(&mut conn).await })
            .await?;
        crate::pro::on_beat(&engine).await;

        let rtt = self.measure_rtt().await?;

        let process_key = keys::process(identity);
        let mut pipe = ::redis::pipe();
        pipe.atomic()
            .cmd("SADD")
            .arg(keys::PROCESSES)
            .arg(identity)
            .ignore()
            // Whether the process's description is there: the key may be missing, or only
            // re-created by the liveness thread after Redis lost it.
            .cmd("HEXISTS")
            .arg(&process_key)
            .arg("info")
            .cmd("HSET")
            .arg(&process_key)
            .arg("info")
            .arg(&self.info)
            .arg("concurrency")
            .arg(engine.config.total_concurrency())
            .arg("busy")
            .arg(busy)
            .arg("beat")
            .arg(now_secs_f64())
            .arg("rtt_us")
            .arg(rtt)
            .arg("quiet")
            .arg(if engine.quiet.is_cancelled() {
                "true"
            } else {
                "false"
            })
            .arg("rss")
            .arg(rss_kb())
            .ignore()
            .cmd("EXPIRE")
            .arg(&process_key)
            .arg(keys::PROCESS_TTL.as_secs())
            .ignore()
            // Peek at the oldest remote signal; it is removed once handled, so a lost reply
            // never loses a signal (handling one twice is harmless).
            .cmd("LINDEX")
            .arg(keys::signals(identity))
            .arg(-1);
        if engine.config.fetch == FetchStrategy::Reliable {
            // Keep the process registered as the owner of its in-progress list, even if
            // another process unregistered it while it could not reach Redis.
            pipe.cmd("SADD")
                .arg(keys::INPROGRESS)
                .arg(identity)
                .ignore();
        }
        let pipe = &pipe;
        let (existed, signal): (bool, Option<String>) = redis
            .with_retry(|mut conn| async move { pipe.query_async(&mut conn).await })
            .await?;

        if !existed {
            // First heartbeat, or Redis lost the process (outage, restart).
            engine
                .config
                .fire_event(LifecycleEvent::Heartbeat, false)
                .await;
        }
        engine.config.fire_event(LifecycleEvent::Beat, false).await;
        if let Some(signal) = signal {
            self.handle_signal(&signal);
            let signals = keys::signals(identity);
            let (signals, signal) = (&signals, &signal);
            let removed = redis
                .with_retry(|mut conn| async move {
                    ::redis::cmd("LREM")
                        .arg(signals)
                        .arg(-1)
                        .arg(signal)
                        .exec_async(&mut conn)
                        .await
                })
                .await;
            if let Err(error) = removed {
                // The signal is handled again at the next beat, which is harmless.
                tracing::warn!(%error, "could not remove a handled remote signal");
            }
        }
        Ok(())
    }

    /// Times a `PING`, in microseconds, warning when Redis is consistently slow.
    async fn measure_rtt(&mut self) -> Result<u64> {
        let mut conn = self.engine.redis.conn().await?;
        let started = std::time::Instant::now();
        let _: String = ::redis::cmd("PING").query_async(&mut conn).await?;
        let rtt = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
        if self.rtt.len() == RTT_READINGS {
            self.rtt.pop_front();
        }
        self.rtt.push_back(rtt);
        if self.rtt.len() == RTT_READINGS && self.rtt.iter().all(|&rtt| rtt > RTT_WARNING_US) {
            tracing::warn!(
                readings_us = ?self.rtt,
                "the round trip to Redis is slow (it should be well under 10 ms): run Redis \
                 close to this process and make sure the process is not short of CPU"
            );
            self.rtt.clear();
        }
        Ok(rtt)
    }

    fn handle_signal(&self, signal: &str) {
        match signal {
            keys::SIGNAL_QUIET => {
                tracing::info!("received a remote quiet signal");
                // The quiet callbacks run in a task of their own: they must not hold up the
                // heartbeat.
                self.engine.begin_quiet();
            }
            keys::SIGNAL_STOP => {
                tracing::info!("received a remote stop signal");
                self.engine.stop_requested.cancel();
            }
            keys::SIGNAL_DUMP => self.engine.dump("remote TTIN"),
            other => tracing::warn!(signal = other, "ignoring an unknown remote signal"),
        }
    }
}

/// The systemd notifier of a process: none for embedded workers ([`Hench::start`],
/// [`Hench::run_until`]) unless one was given explicitly, since the program that embeds them
/// owns its service's notifications (hench's `READY=1` could come before the program is ready,
/// and its `STOPPING=1` after a remote stop would make systemd stop the whole service).
fn notifier(systemd: &Notifications, embedded: bool) -> Option<Notifier> {
    match systemd {
        Notifications::FromEnv if embedded => None,
        other => other.notifier(),
    }
}

/// Refreshes the expiry of a heartbeat key, re-creating the key if Redis lost it (a restart
/// without persistence, `FLUSHALL`), with a `liveness` field only: the heartbeat writes the
/// process's description, and fires the `heartbeat` event, when it finds no `info`.
///
/// KEYS: the heartbeat key. ARGV: its time to live (seconds), now (epoch seconds). Returns 1
/// if the key was there, 0 if it was re-created.
const REFRESH_LIVENESS: &str = r"
if redis.call('EXPIRE', KEYS[1], ARGV[1]) == 1 then
  return 1
end
redis.call('HSET', KEYS[1], 'liveness', ARGV[2])
redis.call('EXPIRE', KEYS[1], ARGV[1])
return 0
";

pub(crate) static REFRESH_LIVENESS_SCRIPT: LazyLock<Script> =
    LazyLock::new(|| Script::new(REFRESH_LIVENESS));

/// How long the liveness thread keeps failing before it warns (then at most once a minute).
const LIVENESS_WARNING_AFTER: Duration = Duration::from_secs(20);

/// Keeps the process's heartbeat key (`<identity>`) from expiring, from a thread of its own
/// with its own Redis connection. Other processes treat a process whose key has expired as
/// dead and push its in-progress jobs back to their queues, so the key must not depend on the
/// tokio runtime that runs the jobs: jobs blocking every worker thread, or a slow lifecycle
/// callback, would otherwise let it expire while those jobs still run, and they would run
/// twice at the same time. The heartbeat proper (stats, work state, process info, remote
/// signals) still runs on the runtime; this refreshes the key's expiry every
/// `heartbeat_interval`, and re-creates the key if Redis lost it (see [`REFRESH_LIVENESS`]),
/// until the end of the shutdown (or until every handle on the process is gone, if its runtime
/// was dropped).
///
/// A failed refresh replaces the connection and is tried again at once: a primary demoted by a
/// failover keeps its clients connected and refuses their writes (`READONLY`), and only a new
/// connection reaches the new primary.
fn spawn_liveness(engine: &Arc<Engine>) {
    let redis = engine.redis.clone();
    let key = keys::process(&engine.identity);
    let interval = engine.config.heartbeat_interval;
    let stop = engine.background.clone();
    let alive = Arc::downgrade(engine);
    let spawned = std::thread::Builder::new()
        .name("hench-liveness".to_owned())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build();
            match runtime {
                Ok(runtime) => runtime.block_on(refresh_liveness(redis, key, interval, stop, alive)),
                Err(error) => tracing::warn!(
                    %error,
                    "could not start the liveness runtime; only the heartbeat keeps the process alive"
                ),
            }
        });
    if let Err(error) = spawned {
        tracing::warn!(
            %error,
            "could not start the liveness thread; only the heartbeat keeps the process alive"
        );
    }
}

/// The loop of [`spawn_liveness`]'s thread.
async fn refresh_liveness(
    redis: RedisPool,
    key: String,
    interval: Duration,
    stop: CancellationToken,
    alive: Weak<Engine>,
) {
    let mut conn: Option<RedisConnection> = None;
    // Since when refreshing fails, and when that was last reported as a warning.
    let mut failing: Option<std::time::Instant> = None;
    let mut warned: Option<std::time::Instant> = None;
    loop {
        let slept = stop.run_until_cancelled(tokio::time::sleep(interval)).await;
        if slept.is_none() || alive.strong_count() == 0 {
            return;
        }
        // The connection in use, then a new one if that fails.
        let mut outcome = None;
        for _ in 0..2 {
            let mut current = match &conn {
                Some(current) => current.clone(),
                // A dedicated connection, driven by this thread's runtime.
                None => match stop
                    .run_until_cancelled(redis.blocking_conn(Duration::ZERO))
                    .await
                {
                    None => return,
                    Some(Ok(opened)) => opened,
                    Some(Err(error)) => {
                        outcome = Some(Err(error));
                        break;
                    }
                },
            };
            let refreshed: ::redis::RedisResult<i64> = REFRESH_LIVENESS_SCRIPT
                .key(&key)
                .arg(keys::PROCESS_TTL.as_secs())
                .arg(now_secs_f64())
                .invoke_async(&mut current)
                .await;
            match refreshed {
                Ok(existed) => {
                    conn = Some(current);
                    outcome = Some(Ok(existed == 1));
                    break;
                }
                Err(error) => {
                    // Replaced: a demoted primary keeps its clients (`READONLY`), and the
                    // connection manager only reconnects after the connection drops.
                    conn = None;
                    outcome = Some(Err(error.into()));
                }
            }
        }
        match outcome {
            Some(Ok(existed)) => {
                if !existed {
                    tracing::info!("liveness: Redis had lost the heartbeat key; re-created it");
                }
                if failing.take().is_some() && warned.take().is_some() {
                    tracing::info!("liveness: the heartbeat key is refreshed again");
                }
            }
            Some(Err(error)) => {
                let now = std::time::Instant::now();
                let since = *failing.get_or_insert(now);
                let due = now.duration_since(since) >= LIVENESS_WARNING_AFTER
                    && warned.is_none_or(|last| now.duration_since(last) >= OUTAGE_LOG_INTERVAL);
                if due {
                    warned = Some(now);
                    tracing::warn!(
                        %error,
                        failing_secs = now.duration_since(since).as_secs(),
                        "liveness: cannot refresh the heartbeat key; while jobs block the \
                         runtime (or the heartbeat is slow), other processes may take this one \
                         for dead and run its jobs again"
                    );
                } else {
                    tracing::debug!(%error, "liveness: could not refresh the heartbeat key");
                }
            }
            None => {}
        }
    }
}

/// Sends systemd watchdog keep-alives every `interval` (half the watchdog timeout) until the
/// end of the shutdown, whatever the heartbeat is doing: a beat can wait on Redis or on
/// lifecycle callbacks, but as long as the runtime runs tasks the process is alive, and if it
/// stops running them systemd restarts the process.
async fn keep_watchdog_alive(stop: CancellationToken, notifier: Notifier, interval: Duration) {
    let mut ticker = tokio::time::interval_at(later(interval), interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = stop.cancelled() => return,
            _ = ticker.tick() => notifier.send_quietly("WATCHDOG=1"),
        }
    }
}

/// The throttles of a process: those defined with registered job types, then the builder's
/// (a later definition of a name replaces an earlier one).
fn process_throttles(hench: &Hench) -> Throttles {
    let mut names = hench.registry.names();
    names.sort_unstable();
    let from_jobs = names
        .into_iter()
        .filter_map(|name| hench.registry.get(name))
        .filter_map(|job| job.dyn_throttle());
    Throttles::new(from_jobs.chain(hench.config.throttles.iter().cloned()))
}

/// The `info` field of the heartbeat: a JSON description of the process for the dashboard.
fn process_info(engine: &Engine) -> Value {
    let config = &engine.config;
    let capsules: Map<String, Value> = config
        .capsules
        .iter()
        .map(|capsule| {
            (
                capsule.name.clone(),
                json!({
                    "concurrency": capsule.concurrency,
                    "mode": capsule.mode.as_str(),
                    "weights": weights(capsule),
                }),
            )
        })
        .collect();
    let mut info = json!({
        "hostname": engine.hostname,
        "started_at": engine.started_at,
        "pid": std::process::id(),
        "tag": config.tag.clone().unwrap_or_default(),
        "capsules": capsules,
        "queues": config.queue_names(),
        "weights": config.capsules.iter().map(weights).collect::<Vec<_>>(),
        "labels": config.labels,
        "identity": engine.identity,
        "version": env!("CARGO_PKG_VERSION"),
        "embedded": engine.embedded,
    });
    if let Value::Object(fields) = &mut info {
        fields.extend(crate::pro::info(engine));
    }
    info
}

fn weights(capsule: &CapsuleConfig) -> Map<String, Value> {
    capsule
        .weights
        .iter()
        .map(|(queue, weight)| (queue.clone(), Value::from(*weight)))
        .collect()
}

/// The host name used in the process identity: `DYNO` (set by Heroku) if present, else the
/// system host name.
fn hostname() -> String {
    match std::env::var("DYNO") {
        Ok(dyno) if !dyno.is_empty() => dyno,
        _ => hostname::get()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|_| "localhost".to_owned()),
    }
}

/// Warns when this binary is built with `panic = "abort"`: job panics are caught by unwinding,
/// so there a panicking job aborts the process instead of failing, with the jobs running beside
/// it, and, with reliable fetch, the processes that recover it after it, until it is recovered
/// past `max_recoveries` and moved to the morgue as a poison pill (`hench::CrashLoop`).
fn warn_if_panics_abort() {
    if cfg!(panic = "abort") {
        tracing::warn!(
            "built with panic = \"abort\": a job that panics will abort this process instead of \
             failing, taking down the jobs running beside it; with reliable fetch it then aborts \
             the processes that recover it, up to max_recoveries + 1 processes in all, before it \
             goes to the morgue as hench::CrashLoop; build workers with panic = \"unwind\", \
             Rust's default"
        );
    }
}

/// Checks that Redis answers and is recent enough, and warns when it may evict keys.
async fn check_redis(redis: &RedisPool) -> Result<()> {
    // Retried once after a dropped connection: the pool's connection may predate a restart of
    // Redis (a `Hench` built before it, started after).
    let info = redis
        .with_retry(|mut conn| async move {
            ::redis::cmd("INFO").query_async::<String>(&mut conn).await
        })
        .await;
    let info = match info {
        Ok(info) => info,
        // INFO may be renamed or disabled on managed services.
        Err(Error::Redis(error)) if error.code().is_some() => {
            tracing::debug!(%error, "INFO is not available; skipping the Redis version check");
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    let fields = parse_info(&info);
    if let Some(version) = fields.get("redis_version")
        && let Some(parsed) = parse_version(version)
        && parsed < MIN_REDIS_VERSION
    {
        return Err(Error::Config(format!(
            "Redis {version} is too old: hench needs Redis {}.{}.{} or later",
            MIN_REDIS_VERSION.0, MIN_REDIS_VERSION.1, MIN_REDIS_VERSION.2
        )));
    }
    if let Some(policy) = fields.get("maxmemory_policy")
        && *policy != "noeviction"
    {
        tracing::warn!(
            policy,
            "Redis may evict hench's data when it runs out of memory; set its \
             maxmemory-policy to noeviction"
        );
    }
    Ok(())
}

/// The `key:value` lines of an `INFO` reply.
fn parse_info(info: &str) -> HashMap<&str, &str> {
    info.lines()
        .filter_map(|line| line.trim_end().split_once(':'))
        .collect()
}

/// `major.minor.patch` (missing parts are 0).
fn parse_version(version: &str) -> Option<(u64, u64, u64)> {
    let mut parts = version.trim().split('.').map(str::parse::<u64>);
    let major = parts.next()?.ok()?;
    let minor = parts.next().and_then(|part| part.ok()).unwrap_or(0);
    let patch = parts.next().and_then(|part| part.ok()).unwrap_or(0);
    Some((major, minor, patch))
}

/// A signal handled by [`Hench::run`].
enum OsSignal {
    Quiet(&'static str),
    Stop(&'static str),
    /// Log the running jobs ([`Engine::dump`]).
    Dump(&'static str),
}

/// The process signals [`Hench::run`] listens to.
#[cfg(unix)]
struct OsSignals {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
    quiet: tokio::signal::unix::Signal,
    /// SIGTTIN, whose default action would stop the whole process (its liveness thread
    /// included, so that other processes would recover and run its jobs again).
    dump: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl OsSignals {
    fn new() -> Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};
        Ok(Self {
            interrupt: signal(SignalKind::interrupt())?,
            terminate: signal(SignalKind::terminate())?,
            quiet: signal(SignalKind::from_raw(libc::SIGTSTP))?,
            dump: signal(SignalKind::from_raw(libc::SIGTTIN))?,
        })
    }

    async fn recv(&mut self) -> OsSignal {
        tokio::select! {
            () = next_signal(&mut self.interrupt) => OsSignal::Stop("SIGINT"),
            () = next_signal(&mut self.terminate) => OsSignal::Stop("SIGTERM"),
            () = next_signal(&mut self.quiet) => OsSignal::Quiet("SIGTSTP"),
            () = next_signal(&mut self.dump) => OsSignal::Dump("SIGTTIN"),
        }
    }
}

/// Completes when `signal` is received (never, if the signal stream ended).
#[cfg(unix)]
async fn next_signal(signal: &mut tokio::signal::unix::Signal) {
    if signal.recv().await.is_none() {
        std::future::pending::<()>().await;
    }
}

#[cfg(not(unix))]
struct OsSignals;

#[cfg(not(unix))]
impl OsSignals {
    fn new() -> Result<Self> {
        Ok(Self)
    }

    async fn recv(&mut self) -> OsSignal {
        match tokio::signal::ctrl_c().await {
            Ok(()) => OsSignal::Stop("Ctrl-C"),
            Err(error) => {
                tracing::warn!(%error, "cannot listen for Ctrl-C");
                std::future::pending().await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions() {
        assert_eq!(parse_version("7.0.15"), Some((7, 0, 15)));
        assert_eq!(parse_version("6.2"), Some((6, 2, 0)));
        assert_eq!(parse_version("8"), Some((8, 0, 0)));
        assert_eq!(parse_version("x.1"), None);
        assert!(parse_version("6.0.9").unwrap() < MIN_REDIS_VERSION);
        assert!(parse_version("6.2.0").unwrap() >= MIN_REDIS_VERSION);
    }

    #[test]
    fn info_fields() {
        let info = "# Server\r\nredis_version:7.0.15\r\nredis_mode:standalone\r\n\r\n# Memory\r\nmaxmemory_policy:noeviction\r\n";
        let fields = parse_info(info);
        assert_eq!(fields.get("redis_version"), Some(&"7.0.15"));
        assert_eq!(fields.get("maxmemory_policy"), Some(&"noeviction"));
        assert_eq!(fields.get("# Server"), None);
    }

    #[test]
    fn outages_are_reported_once_per_episode() {
        use std::sync::atomic::AtomicUsize;
        let reports = Arc::new(AtomicUsize::new(0));
        let seen = reports.clone();
        let mut config = Config::default();
        config.error_handlers.clear();
        config.error_handlers.push(Arc::new(move |_, ctx| {
            assert_eq!(ctx.context, "could not fetch jobs");
            seen.fetch_add(1, Ordering::SeqCst);
        }));
        let outage = Outage::new("could not fetch jobs", "fetching jobs");
        let error = || Error::InvalidJob("boom".into());
        outage.succeeded();
        outage.failed(&config, error());
        outage.failed(&config, error());
        outage.failed(&config, error());
        assert_eq!(reports.load(Ordering::SeqCst), 1);
        assert!(outage.failing.load(Ordering::SeqCst));
        outage.succeeded();
        assert!(!outage.failing.load(Ordering::SeqCst));
        assert_eq!(lock(&outage.state).since, None);
        assert_eq!(lock(&outage.state).failures, 0);
        // An activity that keeps failing and recovering is not reported again within a
        // minute of the last notice...
        for _ in 0..10 {
            outage.failed(&config, error());
            outage.succeeded();
        }
        assert_eq!(reports.load(Ordering::SeqCst), 1);
        // ...but a new episode after that is.
        lock(&outage.state).noticed = None;
        outage.failed(&config, error());
        assert_eq!(reports.load(Ordering::SeqCst), 2);

        // Concurrent failures and successes leave a consistent state.
        let outage = Arc::new(Outage::new("could not fetch jobs", "fetching jobs"));
        let config = Arc::new(config);
        let threads: Vec<_> = (0..8)
            .map(|i| {
                let (outage, config) = (outage.clone(), config.clone());
                std::thread::spawn(move || {
                    for _ in 0..1_000 {
                        if i % 2 == 0 {
                            outage.failed(&config, Error::InvalidJob("x".into()));
                        } else {
                            outage.succeeded();
                        }
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        let state = lock(&outage.state);
        assert_eq!(outage.failing.load(Ordering::SeqCst), state.since.is_some());
    }

    #[test]
    fn later_never_overflows() {
        assert!(later(Duration::MAX) > Instant::now());
        assert!(later(Duration::from_secs(1)) > Instant::now());
    }

    #[test]
    fn hostnames() {
        assert!(!hostname().is_empty());
    }
}
