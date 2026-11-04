//! Processors: fetch a job, run it through server middleware and `perform`, record the
//! outcome. See `docs/DESIGN.md` §5.
//!
//! A processor loops until its process is quiet: fetch a unit of work, parse it (invalid
//! JSON goes straight to the morgue), record it in the work state, run the server middleware
//! chain and the job inside the job's `tracing` span (panics are caught), count it, then
//! acknowledge it, after recording a failure through the retry handler.
//!
//! Only a hard shutdown interrupts a running job: the job's future is dropped and its unit of
//! work is left in the processor's [`Slot`] for the manager to requeue. Writing a job's
//! outcome to Redis (acknowledgement, retry, morgue) is retried while Redis is unreachable,
//! so a completed job's outcome survives an outage.
//
// LANE: engine — this module is owned by the engine lane.

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::FutureExt;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::batch::{self, Accounting, Report};
use crate::client::Stored;
use crate::config::{ErrorContext, FetchStrategy};
use crate::error::{Error, JobError, Result};
use crate::fetch::{FetchWorker, Fetcher, Settlement, UnitOfWork};
use crate::job::{ErasedJob, JobContext, Registry};
use crate::job_logger::{JobLogger, job_span_with};
use crate::launcher::{Engine, lock};
use crate::limit::{self, OverLimitDecision, Reschedule};
use crate::metrics::Outcome;
use crate::middleware::execute_job_or;
use crate::payload::{JobPayload, now_millis, now_secs_f64};
use crate::periodic;
use crate::pro::{Classified, Fetched, Gate};
use crate::retry;
use crate::stats::Work;
use crate::unique::{self, LockOp};

/// The unit of work a processor is running, shared with its manager so that a hard shutdown
/// can requeue it.
pub(crate) type Slot = Arc<Mutex<Option<UnitOfWork>>>;

/// Pause after a failed fetch before trying again.
const FETCH_ERROR_PAUSE: Duration = Duration::from_secs(1);

/// Pause before restarting a processor that crashed.
const CRASH_PAUSE: Duration = Duration::from_secs(1);

/// First and largest pause between attempts to write a job's outcome to Redis.
const PERSIST_MIN_BACKOFF: Duration = Duration::from_millis(100);
const PERSIST_MAX_BACKOFF: Duration = Duration::from_secs(5);

/// Attempts to write a job's outcome when Redis answers with an error (rather than being
/// unreachable, which is retried until it comes back or the process is hard-stopped).
const PERSIST_ATTEMPTS: u32 = 3;

/// Whether the processor keeps going after a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Flow {
    Continue,
    /// A hard shutdown interrupted the job.
    Exit,
}

/// What a failed execution needs on its way to the outcome write.
struct Failure<'a> {
    work: &'a UnitOfWork,
    job: &'a JobPayload,
    queue: &'a str,
    job_impl: Option<&'a dyn ErasedJob>,
    fetched: &'a Fetched,
    class: &'a str,
    logger: &'a JobLogger,
    elapsed: Duration,
}

/// The metrics outcome of an execution's result, before the features classify it.
fn metric_of(result: &Result<(), JobError>) -> Outcome {
    match result {
        Ok(()) => Outcome::Success,
        Err(error) if error.is_interrupted() => Outcome::Interrupted,
        Err(_) => Outcome::Failure,
    }
}

/// Runs jobs for one worker slot of a capsule.
pub(crate) struct Processor {
    engine: Arc<Engine>,
    fetcher: Fetcher,
    worker: FetchWorker,
    index: usize,
    tid: String,
    slot: Slot,
}

impl Processor {
    /// Processor number `index` of a capsule, using `fetcher`. `tid` identifies its slot in
    /// the work state.
    pub(crate) fn new(
        engine: Arc<Engine>,
        fetcher: Fetcher,
        index: usize,
        tid: String,
        slot: Slot,
    ) -> Self {
        Self {
            worker: fetcher.worker(index),
            engine,
            fetcher,
            index,
            tid,
            slot,
        }
    }

    /// Runs until the process is quiet, restarting the loop if it ever panics (jobs' panics
    /// are caught before they get here; this guards against bugs in hench itself).
    pub(crate) async fn run(mut self) {
        loop {
            match AssertUnwindSafe(self.work()).catch_unwind().await {
                Ok(()) => return,
                Err(panic) => {
                    self.engine.config.handle_error(
                        &JobError::panic(panic),
                        &ErrorContext::new("processor crashed; restarting it"),
                    );
                    self.engine.stats.finish_work(&self.tid);
                    self.engine.stats.finish_featured(&self.tid);
                    self.worker = self.fetcher.worker(self.index);
                    let orphan = lock(&self.slot).take();
                    if let Some(work) = orphan {
                        self.requeue(&work).await;
                    }
                    limit::engine::release(&self.engine, &self.tid).await;
                    // Pause, so that a bug triggered by a job cannot spin.
                    let pause = tokio::time::sleep(CRASH_PAUSE);
                    if self.engine.quiet.run_until_cancelled(pause).await.is_none() {
                        return;
                    }
                }
            }
        }
    }

    /// The fetch loop.
    async fn work(&mut self) {
        let engine = self.engine.clone();
        while !engine.quiet.is_cancelled() {
            match self.worker.retrieve_work_until(&engine.quiet).await {
                Ok(fetched) => {
                    engine.fetch_outage.succeeded();
                    let Some(work) = fetched else { continue };
                    // Held until the job's outcome is written (or the processor gives the
                    // job up), so that the reconciliation of the in-progress list leaves it
                    // alone.
                    let _held = (self.fetcher.strategy() == FetchStrategy::Reliable)
                        .then(|| engine.held.hold(&work));
                    // The slots of its queue's concurrency limit, held for the job's run
                    // (renewed by the heartbeat; released with the slot's other permits).
                    let queue = self.hold_queue_permit(&work).await;
                    if engine.quiet.is_cancelled() {
                        // Fetched while the process was going quiet: hand it back.
                        self.requeue(&work).await;
                        self.release_queue_permit(queue.as_deref()).await;
                        return;
                    }
                    let flow = self.process(work).await;
                    self.release_queue_permit(queue.as_deref()).await;
                    if flow == Flow::Exit {
                        return;
                    }
                }
                Err(error) => {
                    engine.fetch_outage.failed(&engine.config, error);
                    let _ = engine
                        .quiet
                        .run_until_cancelled(tokio::time::sleep(FETCH_ERROR_PAUSE))
                        .await;
                }
            }
        }
    }

    /// Handles one fetched unit of work.
    async fn process(&mut self, work: UnitOfWork) -> Flow {
        match work.parse() {
            Ok(job) => {
                let span = job_span_with(&job, &self.engine.config.logged_job_attributes);
                self.run_job(work, job).instrument(span).await
            }
            Err(error) => {
                self.reject(&work, error).await;
                Flow::Continue
            }
        }
    }

    /// Moves a job that cannot be parsed to the morgue, acknowledging it in the same step.
    async fn reject(&self, work: &UnitOfWork, error: Error) {
        let engine = &self.engine;
        let jobstr = work.job_str();
        let settlement = Settlement::Morgue {
            job: work.job().to_vec(),
            bounds: retry::MorgueBounds::new(&engine.config),
        };
        let stored = self
            .persist(|| self.fetcher.settle(work, &settlement))
            .await;
        engine.config.handle_error(
            &JobError::from(error),
            &ErrorContext::new("job is not valid JSON")
                .with_jobstr(&jobstr)
                .with_queue(work.queue_name()),
        );
        if let Err(error) = stored {
            self.lost(
                work,
                &error,
                "could not move an unreadable job to the morgue",
            );
        }
    }

    /// Runs a parsed job and records its outcome. Runs inside the job's span.
    ///
    /// The pipeline of `docs/design/pro-features.md` §3.2: the throttle gate; the execution
    /// (server middleware, then the endpoint's feature steps and `perform`); the classification
    /// of the outcome from the job as fetched; batch accounting; the outcome write, with the
    /// unique lock work in the same script; then the release of the permits the engine holds,
    /// the death handlers, the dispatch of batch events and the periodic record. Each feature
    /// step runs only for the jobs that use the feature: a job without any takes exactly the
    /// path (and sends exactly the commands) it did before the features existed.
    async fn run_job(&mut self, work: UnitOfWork, mut job: JobPayload) -> Flow {
        let engine = self.engine.clone();
        let queue = work.queue_name().to_owned();
        // The features' state comes from the job as fetched, never as middleware changes it.
        let fetched = Arc::new(Fetched::from_payload(&job));
        if fetched.throttle.is_some() {
            match limit::engine::gate(&engine, &self.tid, &work, &job, &fetched).await {
                Gate::Run => {}
                Gate::Refused(reschedule) => {
                    self.refused(&work, &job, &fetched, reschedule).await;
                    return Flow::Continue;
                }
                Gate::Unavailable(error) => {
                    // Like a failed fetch: the job goes back to the front of its queue.
                    engine.fetch_outage.failed(&engine.config, error);
                    self.requeue(&work).await;
                    let pause = tokio::time::sleep(FETCH_ERROR_PAUSE);
                    let _ = engine.quiet.run_until_cancelled(pause).await;
                    return Flow::Continue;
                }
                Gate::Failed(error) => {
                    // A failure specific to the job (not an outage): the failure path, so that
                    // a poisoned throttle key can never block the front of a queue.
                    engine.stats.record_processed();
                    engine.stats.record_failed();
                    let job_impl = engine.registry.resolve(&job.class);
                    self.track(&job.class, Duration::ZERO, Outcome::Failure);
                    self.fail_and_report(
                        &work,
                        &job,
                        job.clone(),
                        &queue,
                        job_impl.as_deref(),
                        &fetched,
                        &error,
                        Duration::ZERO,
                    )
                    .await;
                    return Flow::Continue;
                }
            }
        }
        engine.stats.start_work(
            &self.tid,
            Work {
                queue: queue.clone(),
                payload: work.shared_job().clone(),
                run_at: now_secs_f64(),
            },
        );
        if !fetched.is_plain() {
            engine.stats.start_featured(&self.tid, fetched.clone());
        }
        *lock(&self.slot) = Some(work);
        let job_impl = engine.registry.resolve(&job.class);
        let logger = engine.logger.for_job(&job);
        let started = Instant::now();
        logger.start();
        let outcome = tokio::select! {
            biased;
            () = engine.hard.cancelled() => None,
            result = execute(&engine, job_impl.clone(), &mut job, &queue, &fetched) => Some(result),
        };
        let elapsed = started.elapsed();
        engine.stats.finish_work(&self.tid);
        if !fetched.is_plain() {
            engine.stats.finish_featured(&self.tid);
        }
        engine.stats.record_processed();
        let job_impl = job_impl.as_deref();
        // The job that ran: the registered one, or the class of an unknown one.
        let class = job_impl
            .map_or(job.class.as_str(), |job| job.name())
            .to_owned();
        let Some(result) = outcome else {
            self.track(&class, elapsed, Outcome::Failure);
            engine.stats.record_failed();
            logger.fail(elapsed);
            tracing::warn!("interrupted by the shutdown; the job will be pushed back to its queue");
            // The unit of work stays in the slot: the manager requeues it (and releases the
            // permits the engine holds for it).
            return Flow::Exit;
        };
        // Take the unit of work back: from now on recording the outcome is up to us.
        let Some(work) = lock(&self.slot).take() else {
            // The manager gave up waiting for this job and already requeued it.
            self.track(&class, elapsed, metric_of(&result));
            return Flow::Exit;
        };
        match result {
            Ok(()) => {
                self.track(&class, elapsed, Outcome::Success);
                logger.done(elapsed);
                self.succeeded(&work, &fetched, elapsed).await;
            }
            Err(error) if error.is_interrupted() => {
                self.track(&class, elapsed, Outcome::Interrupted);
                logger.interrupted(elapsed);
                self.push_back(&work, &job, &queue, job_impl, &fetched, elapsed)
                    .await;
            }
            Err(error) if error.class() == JobError::SHUTDOWN && engine.stopping.is_cancelled() => {
                self.track(&class, elapsed, Outcome::Failure);
                engine.stats.record_failed();
                logger.fail(elapsed);
                tracing::info!("stopped early for the shutdown; pushing it back to its queue");
                // The lock of a unique job stays as it is: the job runs again.
                self.requeue(&work).await;
                self.release_permits(&fetched).await;
            }
            Err(error) => {
                let failure = Failure {
                    work: &work,
                    job: &job,
                    queue: &queue,
                    job_impl,
                    fetched: &fetched,
                    class: &class,
                    logger: &logger,
                    elapsed,
                };
                self.failed(failure, error).await;
            }
        }
        Flow::Continue
    }

    /// Counts an execution in the job metrics.
    fn track(&self, class: &str, elapsed: Duration, outcome: Outcome) {
        if let Some(metrics) = &self.engine.metrics {
            metrics.track(class, elapsed, outcome);
        }
    }

    /// Records a successful execution (or a halt by server middleware): batch accounting, the
    /// acknowledgement with its lock work, then the feature steps after the write.
    async fn succeeded(&self, work: &UnitOfWork, fetched: &Fetched, elapsed: Duration) {
        let classified = Classified::Success;
        let report = self.account(fetched, &classified).await;
        let lock = self.lock_op(fetched, &classified, None);
        self.acknowledge_with(work, lock.as_ref()).await;
        self.release_permits(fetched).await;
        self.after_outcome(fetched, &classified, report, elapsed)
            .await;
    }

    /// Handles a job that returned an error: a member of a cancelled batch is skipped, a
    /// limiter's refusal within its budget is rescheduled, anything else is a failure.
    async fn failed(&self, failure: Failure<'_>, error: JobError) {
        let engine = &self.engine;
        // Like a retry, the job continues as it was fetched, not as middleware changed it.
        let original = failure.work.parse().unwrap_or_else(|_| failure.job.clone());
        let original = unique::engine::continuation(failure.fetched, original);
        if error.is_batch_cancelled() && failure.fetched.member.is_some() {
            // Skipped, once the batch confirms its cancellation (else a failure, below).
            let skipped = Classified::Skipped;
            match batch::account::before_outcome(engine, failure.fetched, &skipped).await {
                Accounting::NotCancelled => {}
                accounting => {
                    self.track(failure.class, failure.elapsed, Outcome::Skipped);
                    failure.logger.skipped(failure.elapsed);
                    self.skipped(failure.work, failure.fetched, accounting, failure.elapsed)
                        .await;
                    return;
                }
            }
        }
        let error = match limit::engine::over_limit(engine, &error, &original) {
            Some(OverLimitDecision::Reschedule(reschedule)) => {
                self.track(failure.class, failure.elapsed, Outcome::OverLimit);
                failure.logger.over_limit(
                    failure.elapsed,
                    &reschedule.label,
                    reschedule.delay,
                    reschedule.n,
                );
                let classified = Classified::OverLimit(reschedule);
                self.reschedule(
                    failure.work,
                    original,
                    failure.fetched,
                    classified,
                    failure.elapsed,
                )
                .await;
                return;
            }
            Some(OverLimitDecision::Exhausted(exhausted)) => exhausted,
            None => error,
        };
        self.track(failure.class, failure.elapsed, Outcome::Failure);
        engine.stats.record_failed();
        failure.logger.fail(failure.elapsed);
        self.fail_and_report(
            failure.work,
            failure.job,
            original,
            failure.queue,
            failure.job_impl,
            failure.fetched,
            &error,
            failure.elapsed,
        )
        .await;
    }

    /// Acknowledges a member of a cancelled batch (whose accounting was recorded) with its lock
    /// work, then dispatches the batch's events.
    async fn skipped(
        &self,
        work: &UnitOfWork,
        fetched: &Fetched,
        accounting: Accounting,
        elapsed: Duration,
    ) {
        let classified = Classified::Skipped;
        let report = match accounting {
            Accounting::Recorded(report) => Some(report),
            Accounting::Nothing | Accounting::NotCancelled => None,
        };
        let lock = self.lock_op(fetched, &classified, None);
        self.acknowledge_with(work, lock.as_ref()).await;
        self.release_permits(fetched).await;
        self.after_outcome(fetched, &classified, report, elapsed)
            .await;
    }

    /// A job the throttle gate refused before it started: a member of a cancelled batch is
    /// skipped; any other job is parked in the schedule until its slot.
    async fn refused(
        &self,
        work: &UnitOfWork,
        job: &JobPayload,
        fetched: &Fetched,
        reschedule: Reschedule,
    ) {
        let engine = &self.engine;
        let logger = engine.logger.for_job(job);
        if fetched.member.is_some() && batch::endpoint::member_cancelled(engine, fetched).await {
            let skipped = Classified::Skipped;
            match batch::account::before_outcome(engine, fetched, &skipped).await {
                Accounting::NotCancelled => {}
                accounting => {
                    engine.stats.record_processed();
                    self.track(&job.class, Duration::ZERO, Outcome::Skipped);
                    logger.skipped(Duration::ZERO);
                    self.skipped(work, fetched, accounting, Duration::ZERO)
                        .await;
                    return;
                }
            }
        }
        self.track(&job.class, Duration::ZERO, Outcome::Throttled);
        logger.throttled(&reschedule.label, reschedule.delay);
        // The job as fetched (the gate runs before any middleware).
        let classified = Classified::Throttled(reschedule);
        self.reschedule(work, job.clone(), fetched, classified, Duration::ZERO)
            .await;
    }

    /// Parks a job over a limit (or throttled) in the schedule, acknowledging it in the same
    /// step (`PUSH_BACK` in its schedule mode, with the unique lock held): not an outcome, so
    /// batches count nothing and no handler runs. `original` is the job as fetched; client
    /// middleware runs when the scheduler moves it back.
    async fn reschedule(
        &self,
        work: &UnitOfWork,
        original: JobPayload,
        fetched: &Fetched,
        classified: Classified,
        elapsed: Duration,
    ) {
        let (Classified::OverLimit(reschedule) | Classified::Throttled(reschedule)) = &classified
        else {
            return;
        };
        match reschedule.stored(original, now_secs_f64(), now_millis()) {
            Ok(stored) => {
                let lock = self.lock_op(fetched, &classified, None);
                let written = self
                    .persist(|| self.fetcher.push_back_with(work, &stored, lock.as_ref()))
                    .await;
                match written {
                    Ok(true) => {
                        tracing::debug!(
                            delay = ?reschedule.delay,
                            reschedules = reschedule.n,
                            "rescheduled over its limit"
                        );
                        // Back at its slot, not at the scheduler's next random poll.
                        self.engine.wakeups.after(reschedule.delay);
                    }
                    Ok(false) => tracing::debug!(
                        "a job over its limit was no longer in progress; it was not rescheduled \
                         again"
                    ),
                    Err(error) => {
                        self.lost(work, &error, "could not reschedule a job over its limit")
                    }
                }
            }
            Err(error) => self.lost(work, &error, "could not encode a job over its limit"),
        }
        self.release_permits(fetched).await;
        self.after_outcome(fetched, &classified, None, elapsed)
            .await;
    }

    /// Records a failure (see [`Processor::fail`]) and reports it to the error handlers.
    #[allow(clippy::too_many_arguments)]
    async fn fail_and_report(
        &self,
        work: &UnitOfWork,
        job: &JobPayload,
        original: JobPayload,
        queue: &str,
        job_impl: Option<&dyn ErasedJob>,
        fetched: &Fetched,
        error: &JobError,
        elapsed: Duration,
    ) {
        self.fail(
            work, job, original, queue, job_impl, fetched, error, elapsed,
        )
        .await;
        self.engine.config.handle_error(
            error,
            &ErrorContext::new("job failed")
                .with_job(job)
                .with_queue(queue),
        );
    }

    /// Pushes an interrupted job (see [`JobError::interrupted`]) to the back of its queue and
    /// acknowledges it: the job as fetched goes through client middleware again, like a new
    /// push, and replaces the fetched one in one step with reliable fetch. If the push is
    /// rejected (invalid job, failing middleware), the job is handled as a failure instead.
    async fn push_back(
        &self,
        work: &UnitOfWork,
        job: &JobPayload,
        queue: &str,
        job_impl: Option<&dyn ErasedJob>,
        fetched: &Fetched,
        elapsed: Duration,
    ) {
        let engine = &self.engine;
        // Like a retry, the job continues as it was fetched, not as middleware changed it.
        let original = work.parse().unwrap_or_else(|_| job.clone());
        let original = unique::engine::continuation(fetched, original);
        // A panicking client middleware is a rejected push, not a crashed processor.
        let prepared = AssertUnwindSafe(engine.client.prepare_push(original))
            .catch_unwind()
            .await
            .unwrap_or_else(|panic| Err(Error::Job(JobError::panic(panic))))
            .and_then(|prepared| {
                prepared
                    .map(|job| Stored::encode(job, now_secs_f64(), now_millis()))
                    .transpose()
            });
        let classified = Classified::Interrupted;
        let stored = match prepared {
            Ok(Some(stored)) => stored,
            Ok(None) => {
                tracing::warn!(
                    "client middleware stopped pushing an interrupted job back to its queue; the \
                     job is dropped"
                );
                let lock = self.lock_op(fetched, &Classified::Success, None);
                self.acknowledge_with(work, lock.as_ref()).await;
                self.release_permits(fetched).await;
                return;
            }
            Err(error) => {
                engine.stats.record_failed();
                let error = error
                    .into_job_error()
                    .context("could not push an interrupted job back to its queue");
                let original = work.parse().unwrap_or_else(|_| job.clone());
                let original = unique::engine::continuation(fetched, original);
                self.fail_and_report(
                    work, job, original, queue, job_impl, fetched, &error, elapsed,
                )
                .await;
                return;
            }
        };
        let lock = self.lock_op(fetched, &classified, None);
        match self
            .persist(|| self.fetcher.push_back_with(work, &stored, lock.as_ref()))
            .await
        {
            Ok(true) => tracing::debug!("pushed back to the end of its queue"),
            Ok(false) => tracing::warn!(
                "an interrupted job was no longer in progress; it was not pushed back again"
            ),
            Err(error) => self.lost(
                work,
                &error,
                "could not push an interrupted job back to its queue",
            ),
        }
        self.release_permits(fetched).await;
        self.after_outcome(fetched, &classified, None, elapsed)
            .await;
    }

    /// Records a failure through the retry handler: writes the verdict (retry set, morgue or
    /// nothing) and acknowledges the job, in one step with reliable fetch, then runs the death
    /// handlers. `original` is the job as fetched. A batch member's failure is accounted before
    /// the write; the unique lock work happens in the same script as the write.
    #[allow(clippy::too_many_arguments)]
    async fn fail(
        &self,
        work: &UnitOfWork,
        job: &JobPayload,
        original: JobPayload,
        queue: &str,
        job_impl: Option<&dyn ErasedJob>,
        fetched: &Fetched,
        error: &JobError,
        elapsed: Duration,
    ) {
        let engine = &self.engine;
        let verdict = retry::decide(&engine.config, job_impl, original, queue, error).await;
        let classified = Classified::Failure(Box::new(verdict));
        let report = self.account(fetched, &classified).await;
        let Classified::Failure(verdict) = &classified else {
            return;
        };
        let verdict: &retry::Verdict = verdict;
        let stored = match retry::settlement(&engine.config, verdict) {
            Ok(settlement) => {
                let lock = self.lock_op(fetched, &classified, Some(&settlement));
                self.persist(|| self.fetcher.settle_with(work, &settlement, lock.as_ref()))
                    .await
            }
            Err(error) => Err(error),
        };
        match stored {
            Ok(settled) => {
                if !settled {
                    // Settled by an attempt whose reply was lost, or recovered by another
                    // process; the death handlers run either way (at least once).
                    tracing::debug!("a failed job was no longer in progress");
                }
                self.release_permits(fetched).await;
                retry::finish(&engine.config, verdict, error);
                self.after_outcome(fetched, &classified, report, elapsed)
                    .await;
            }
            Err(persist_error) => {
                self.release_permits(fetched).await;
                let jobstr = work.job_str();
                engine.config.handle_error(
                    &JobError::from(persist_error),
                    &ErrorContext::new("could not record a failed job")
                        .with_job(job)
                        .with_jobstr(&jobstr)
                        .with_queue(queue),
                );
                if self.fetcher.strategy() == FetchStrategy::Basic {
                    tracing::error!(job = %jobstr, "a failed job could not be saved for a retry and is lost");
                } else {
                    tracing::warn!(
                        "a failed job could not be saved for a retry; it stays in progress and \
                         will run again"
                    );
                }
            }
        }
    }

    /// Batch accounting of an outcome, before its write (members of batches only).
    async fn account(&self, fetched: &Fetched, classified: &Classified) -> Option<Report> {
        fetched.member.as_ref()?;
        match batch::account::before_outcome(&self.engine, fetched, classified).await {
            Accounting::Recorded(report) => Some(report),
            Accounting::Nothing | Accounting::NotCancelled => None,
        }
    }

    /// What the outcome write does with the job's unique lock (jobs holding one only).
    fn lock_op(
        &self,
        fetched: &Fetched,
        classified: &Classified,
        settlement: Option<&Settlement>,
    ) -> Option<LockOp> {
        fetched.lock.as_ref()?;
        unique::engine::lock_op(fetched, classified, settlement)
    }

    /// Registers the slots `work` holds in its queue's concurrency limit under this processor's
    /// slot, so that the heartbeat renews them. Returns the queue when the job came with
    /// permits of its queue's limits (released by [`Processor::release_queue_permit`]).
    async fn hold_queue_permit(&self, work: &UnitOfWork) -> Option<String> {
        let permit = work.queue_permit()?;
        if !permit.leases.is_empty() {
            let owner = crate::limit::queue::id(&permit.queue);
            self.engine
                .permits
                .hold(&self.tid, permit.leases.clone(), &owner)
                .await;
        }
        Some(permit.queue.clone())
    }

    /// Releases the slots this processor's job held in its queue's concurrency limit (if the
    /// outcome write did not already, with the slot's other permits), and ends the queue's
    /// cooldown in this process: a slot is free.
    async fn release_queue_permit(&self, queue: Option<&str>) {
        let Some(queue) = queue else { return };
        limit::engine::release(&self.engine, &self.tid).await;
        self.fetcher.slot_freed(queue);
    }

    /// Releases the permits the engine holds for this slot's job (throttled jobs only), after
    /// its outcome write.
    async fn release_permits(&self, fetched: &Fetched) {
        if fetched.throttle.is_some() {
            limit::engine::release(&self.engine, &self.tid).await;
        }
    }

    /// The feature steps after an outcome write: the dispatch of the batch events the
    /// accounting recorded, and the periodic record of the execution (spawned).
    async fn after_outcome(
        &self,
        fetched: &Fetched,
        classified: &Classified,
        report: Option<Report>,
        elapsed: Duration,
    ) {
        if report.is_some() {
            batch::dispatch::after_outcome(&self.engine, report).await;
        }
        if fetched.periodic.is_some() {
            periodic::record_run::spawn(&self.engine, fetched, classified, elapsed);
        }
    }

    /// Acknowledges a handled job, with the lock work of its outcome.
    async fn acknowledge_with(&self, work: &UnitOfWork, lock: Option<&LockOp>) {
        if let Err(error) = self
            .persist(|| self.fetcher.acknowledge_with(work, lock))
            .await
        {
            tracing::error!(
                %error,
                "could not acknowledge a job; it stays in progress and will run again"
            );
        }
    }

    /// Pushes a job back to the front of its queue.
    async fn requeue(&self, work: &UnitOfWork) {
        if let Err(error) = self.persist(|| self.fetcher.requeue(work)).await {
            self.lost(work, &error, "could not push a job back to its queue");
        }
    }

    /// Reports a job whose state could not be written to Redis.
    fn lost(&self, work: &UnitOfWork, error: &Error, what: &str) {
        match self.fetcher.strategy() {
            FetchStrategy::Basic => {
                tracing::error!(%error, job = %work.job_str(), "{what}; the job is lost");
            }
            FetchStrategy::Reliable => {
                tracing::warn!(
                    %error,
                    "{what}; it stays in progress and is recovered later"
                );
            }
        }
    }

    /// Writes a job's outcome to Redis, see [`write_with_backoff`] (until a hard shutdown).
    async fn persist<T, F, Fut>(&self, operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        write_with_backoff(&self.engine.hard, operation).await
    }
}

/// Runs a Redis write until it succeeds. While Redis is unavailable (see [`is_unavailable`]) it
/// is retried with backoff (100 ms, doubling up to 5 s) until it comes back or `stop` is
/// cancelled (one more attempt is made after that); other errors are retried a few times.
async fn write_with_backoff<T, F, Fut>(stop: &CancellationToken, mut operation: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut pause = PERSIST_MIN_BACKOFF;
    let mut failures = 0;
    loop {
        let error = match operation().await {
            Ok(value) => return Ok(value),
            Err(error) => error,
        };
        failures += 1;
        if stop.is_cancelled() || (!is_unavailable(&error) && failures >= PERSIST_ATTEMPTS) {
            return Err(error);
        }
        tracing::warn!(%error, failures, "could not write to Redis; retrying");
        let _ = stop.run_until_cancelled(tokio::time::sleep(pause)).await;
        pause = (pause * 2).min(PERSIST_MAX_BACKOFF);
    }
}

/// Runs the server middleware chain and the job (panics become [`JobError`]s) through
/// [`execute_job_or`], the execution path the testing modes share. An unknown class fails with
/// [`JobError::UNKNOWN_JOB`] once the chain reaches the job (see [`unknown_job`]).
async fn execute(
    engine: &Engine,
    job_impl: Option<Arc<dyn ErasedJob>>,
    job: &mut JobPayload,
    queue: &str,
    fetched: &Fetched,
) -> Result<(), JobError> {
    let ctx = JobContext::new(job.clone(), engine.client.clone())
        .with_queue(queue)
        .with_data(engine.config.data.clone())
        .with_stopping(engine.stopping.clone())
        .with_max_iteration_runtime(engine.config.max_iteration_runtime)
        .with_shutdown_timeout(Some(engine.config.timeout))
        .with_early_release(fetched.early_release.clone());
    let unknown = |class: &str| unknown_job(class, &engine.registry);
    execute_job_or(
        &engine.config.server_middleware,
        &ctx,
        job,
        job_impl,
        &unknown,
    )
    .await
}

/// Registered names a worker's unknown-job error lists, at most.
const LISTED_JOBS: usize = 10;

/// The error of a job whose class this process does not know ([`JobError::UNKNOWN_JOB`]): it
/// names the jobs the process knows and the usual cause, a binary that does not link the crate
/// defining the job.
pub(crate) fn unknown_job(class: &str, registry: &Registry) -> JobError {
    let names = registry.names();
    let known = if names.is_empty() {
        "knows no job".to_owned()
    } else {
        let listed: Vec<String> = names
            .iter()
            .take(LISTED_JOBS)
            .map(|name| format!("`{name}`"))
            .collect();
        match names.len().saturating_sub(LISTED_JOBS) {
            0 => format!("knows {}", listed.join(", ")),
            more => format!("knows {} and {more} more", listed.join(", ")),
        }
    };
    JobError::new(
        JobError::UNKNOWN_JOB,
        format!(
            "no job is registered under the name `{class}` in this process, which {known} (a \
             #[hench::job] registers itself only in binaries that link the crate defining it: \
             refer to that crate from the worker, such as with `use my_jobs as _;`)"
        ),
    )
}

/// `true` for errors meaning Redis could not be reached (or is failing over).
pub(crate) fn is_unreachable(error: &Error) -> bool {
    match error {
        Error::Redis(error) => {
            error.is_io_error()
                || error.is_connection_dropped()
                || error.is_connection_refusal()
                || error.is_timeout()
                || crate::redis::retry_action(error).is_some()
        }
        _ => false,
    }
}

/// Error codes with which Redis refuses commands whatever the keys: out of memory, loading a
/// dataset, busy running a script, unable to persist, primary down, cluster unavailable.
const UNAVAILABLE_CODES: [&str; 7] = [
    "OOM",
    "LOADING",
    "BUSY",
    "MISCONF",
    "MASTERDOWN",
    "TRYAGAIN",
    "CLUSTERDOWN",
];

/// `true` when Redis as a whole cannot serve the command right now (unreachable, failing
/// over, or one of [`UNAVAILABLE_CODES`], also inside a transaction or pipeline), as opposed
/// to an error specific to the command or its keys. Such errors are worth retrying for as long
/// as it takes.
pub(crate) fn is_unavailable(error: &Error) -> bool {
    if is_unreachable(error) {
        return true;
    }
    let Error::Redis(error) = error else {
        return false;
    };
    error
        .code()
        .is_some_and(|code| UNAVAILABLE_CODES.contains(&code))
        || error.clone().into_server_errors().is_some_and(|errors| {
            errors
                .iter()
                .any(|(_, error)| UNAVAILABLE_CODES.contains(&error.code()))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::redis::{ErrorKind, RedisError};

    #[test]
    fn unreachable_errors() {
        let refused = RedisError::from(std::io::Error::from(std::io::ErrorKind::ConnectionRefused));
        assert!(is_unreachable(&Error::Redis(refused)));
        let timeout = RedisError::from(std::io::Error::from(std::io::ErrorKind::TimedOut));
        assert!(is_unreachable(&Error::Redis(timeout)));
        let readonly = ::redis::make_extension_error("READONLY".into(), None);
        assert!(is_unreachable(&Error::Redis(readonly)));
        let wrong_type = RedisError::from((ErrorKind::Client, "nope"));
        assert!(!is_unreachable(&Error::Redis(wrong_type)));
        assert!(!is_unreachable(&Error::InvalidJob("x".into())));
    }

    #[test]
    fn unavailable_errors() {
        for code in UNAVAILABLE_CODES {
            let error = ::redis::make_extension_error(code.into(), Some("detail".into()));
            assert!(is_unavailable(&Error::Redis(error)), "{code}");
        }
        let refused = RedisError::from(std::io::Error::from(std::io::ErrorKind::ConnectionRefused));
        assert!(is_unavailable(&Error::Redis(refused)));
        let wrong_type = ::redis::make_extension_error("WRONGTYPE".into(), None);
        assert!(!is_unavailable(&Error::Redis(wrong_type)));
        assert!(!is_unavailable(&Error::Config("x".into())));
    }
}
