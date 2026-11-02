//! Iterable jobs: long jobs that walk through a sequence of items one at a time, remember how
//! far they got, and continue from there after an interruption.
//!
//! Implement [`IterableJob`] instead of [`Job`]: [`IterableJob::build_enumerator`] returns the
//! items still to process after a cursor ([`Enumerator`]), [`IterableJob::each_iteration`]
//! processes one. Every iterable job is a [`Job`] (enqueue it with
//! [`JobExt::perform_async`](crate::JobExt::perform_async) and friends, register it like any
//! job), whose `perform` runs the iteration:
//!
//! 1. It loads the job's state (hash [`keys::iteration_state`](crate::keys::iteration_state),
//!    `it-<jid>`): the cursor of the last item processed, the number of executions, the run
//!    time, and whether the job was cancelled ([`Client::cancel`](crate::Client::cancel)).
//! 2. It builds the enumerator from the cursor and calls [`IterableJob::on_start`] (first
//!    execution) or [`IterableJob::on_resume`].
//! 3. It processes the items one by one. It stops if the job was cancelled; otherwise it takes
//!    the next item (none left: the iteration is complete, whatever the time or a shutdown) and
//!    stops before it if the process is shutting down, or if the execution has run for longer
//!    than its limit
//!    ([`HenchBuilder::max_iteration_runtime`](crate::HenchBuilder::max_iteration_runtime) or
//!    [`IterableJob::max_iteration_runtime`]; at least one item is processed per execution).
//!    Every [`IterableJob::checkpoint_interval`] (5 s by default) it saves the cursor and checks
//!    whether the job was cancelled.
//! 4. Interrupted (shutdown or time limit): it saves the cursor, calls
//!    [`IterableJob::on_stop`], and returns [`JobError::interrupted`]: workers push the job back
//!    to the end of its queue (keeping its jid) and acknowledge it, without counting a failure
//!    or using a retry; it continues from the cursor when it runs again (with the item it
//!    stopped before).
//! 5. Finished (no more items, cancelled, or [`Iteration::abort`] called, by an item or by
//!    `on_start`/`on_resume` before the first one): it records in the state that the
//!    iteration is over, calls [`IterableJob::on_cancel`] (when cancelled),
//!    [`IterableJob::on_stop`] and [`IterableJob::on_complete`], then deletes the state. If a
//!    hook fails (or the process stops before the job is acknowledged), the job's next
//!    execution only runs these hooks again. Once they have run the job is complete: a
//!    failure to delete the state is logged (the state expires).
//! 6. An error from an item (or from the enumerator) saves the cursor of the last item
//!    processed and fails the job: its retry continues from there, starting with the item that
//!    failed. A limiter's refusal ([`JobError::is_over_limit`], such as
//!    `limiter.acquire(it.ctx()).await?` in an item) is not a failure: the cursor is saved,
//!    [`IterableJob::on_stop`] runs, and workers reschedule the job at the slot the limiter gave
//!    it, without using a retry; it continues from there (with the refused item). Such a
//!    reschedule counts against the limiter's budget of reschedules (20 by default) only while
//!    the job makes no progress: an execution that processed items before its refusal starts
//!    the count again, so a long iteration paced by a limiter never fails because of it. A
//!    panic there does the same before it goes on (workers turn it into a failure of class
//!    `panic`). Errors from the other hooks and from `build_enumerator` fail the job as well.
//!
//! Items are processed at least once: a process that crashes, or a hard shutdown that drops an
//! item halfway, makes the job continue from the last saved cursor, so make
//! `each_iteration` idempotent. The recoveries of a job from crashed processes count toward
//! [`max_recoveries`](crate::Config::max_recoveries) (the poison-pill limit) only until it saves
//! progress again: the first save of an execution beyond the cursor it started from forgets
//! them, so a long iteration that keeps making progress is never taken for a job that kills its
//! processes, whatever crashes them; one that crashes before any progress is.
//!
//! The state expires 30 days after its last write. Testing clients
//! ([`Client::fake`](crate::Client::fake), [`Client::inline`](crate::Client::inline)) keep it
//! in memory: interrupted jobs go back to their fake queue (or run
//! again inline) and continue where they stopped.
//!
//! ```
//! use hench::iterable::{Enumerator, IterableJob, Iteration};
//! use hench::prelude::*;
//!
//! /// Sends a newsletter to every subscriber of a list, a page of subscribers at a time.
//! #[derive(Default)]
//! struct SendNewsletter;
//!
//! impl IterableJob for SendNewsletter {
//!     const NAME: &'static str = "SendNewsletter";
//!     type Args = (u64,); // the list
//!     type Item = u64; // a subscriber
//!     type Cursor = u64; // the last subscriber handled
//!
//!     fn build_enumerator<'a>(
//!         &'a self,
//!         _ctx: &'a JobContext,
//!         (list,): &'a (u64,),
//!         cursor: Option<u64>,
//!     ) -> Result<Enumerator<'a, u64, u64>, JobError> {
//!         Ok(Enumerator::paginate(cursor, move |after| async move {
//!             let page = subscribers(*list, after, 100).await?;
//!             Ok(page.into_iter().map(|id| (id, id)).collect())
//!         }))
//!     }
//!
//!     async fn each_iteration(&self, subscriber: u64, it: &Iteration<'_, Self>) -> Result<(), JobError> {
//!         tracing::info!(subscriber, list = it.args().0, jid = it.jid(), "sending");
//!         Ok(())
//!     }
//! }
//!
//! /// The subscribers of `list` after `after`, at most `limit` of them.
//! async fn subscribers(list: u64, after: Option<u64>, limit: u64) -> Result<Vec<u64>, JobError> {
//!     let first = after.map_or(1, |id| id + 1);
//!     Ok((first..first + limit).take_while(|id| *id <= 250 + list).collect())
//! }
//!
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! let client = hench::testing::Testing::new().register(SendNewsletter).inline();
//! SendNewsletter::perform_async(&client, (7,)).await?;
//! # Ok::<(), hench::Error>(()) }).unwrap();
//! ```

mod enumerator;
mod state;

use std::fmt;
use std::future::Future;
use std::ops::Deref;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use futures::{FutureExt, StreamExt};
use serde::Serialize;
use serde::de::DeserializeOwned;

pub use enumerator::{Enumerator, RangeCursor};
pub use state::IterationState;
pub(crate) use state::LocalStates;

use serde_json::Value;

use crate::error::{CaughtPanic, JobError};
use crate::job::{Exhausted, Job, JobContext, RetryIn};
use crate::limit::Throttle;
use crate::options::JobOptions;
use crate::payload::JobPayload;
use state::{CURSOR, DONE, EXECUTIONS, Fields, RUNTIME, Store};

/// Class of the error with which an iterable job fails when its saved cursor cannot be read
/// back into its [`IterableJob::Cursor`] type (for example after the type changed). Delete the
/// job's state ([`keys::iteration_state`](crate::keys::iteration_state)) to start it over.
pub const INVALID_CURSOR: &str = "hench::InvalidCursor";

/// Default [`IterableJob::checkpoint_interval`].
pub const DEFAULT_CHECKPOINT_INTERVAL: Duration = Duration::from_secs(5);

/// A job that processes a sequence of items one at a time and can stop and resume between
/// items (Sidekiq's iterable jobs); see the [module documentation](self).
///
/// Every `IterableJob` is a [`Job`] with the same name, arguments and options, so it is
/// enqueued, registered and tested like any job. (With both traits in scope, `J::NAME` is
/// ambiguous: write `<J as Job>::NAME`.)
pub trait IterableJob: Send + Sync + Sized + 'static {
    /// The job's name, stored in the payload's `class` field. Must be unique in a process.
    const NAME: &'static str;

    /// The job's arguments, as for [`Job::Args`]: a tuple (or `()`), serialized as a JSON array.
    type Args: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// What is processed in one iteration: a record, a batch of records, a line, ...
    type Item: Send;

    /// Where the iteration stands after an item: an offset, the key of the last record, ... It
    /// is saved as JSON; it must not serialize to `null` (that reads back as "no cursor").
    type Cursor: Serialize + DeserializeOwned + Clone + Send + Sync + 'static;

    /// Options for this job type, as for [`Job::options`].
    fn options() -> JobOptions {
        JobOptions::default()
    }

    /// The items to process after `cursor` (`None` when starting), each with the cursor reached
    /// once it is processed. See [`Enumerator`].
    fn build_enumerator<'a>(
        &'a self,
        ctx: &'a JobContext,
        args: &'a Self::Args,
        cursor: Option<Self::Cursor>,
    ) -> Result<Enumerator<'a, Self::Item, Self::Cursor>, JobError>;

    /// Processes one item. Returning an error (or panicking) saves the cursor of the previous
    /// item and fails the job (its retry starts with this item again); [`Iteration::abort`]
    /// ends the iteration after this item as if it were the last.
    fn each_iteration(
        &self,
        item: Self::Item,
        it: &Iteration<'_, Self>,
    ) -> impl Future<Output = Result<(), JobError>> + Send;

    /// Called before the first item of the first execution.
    fn on_start(
        &self,
        it: &Iteration<'_, Self>,
    ) -> impl Future<Output = Result<(), JobError>> + Send {
        let _ = it;
        std::future::ready(Ok(()))
    }

    /// Called before the first item of every later execution (after an interruption or a
    /// failure; not when the iteration is over and only the finishing hooks run again).
    fn on_resume(
        &self,
        it: &Iteration<'_, Self>,
    ) -> impl Future<Output = Result<(), JobError>> + Send {
        let _ = it;
        std::future::ready(Ok(()))
    }

    /// Called whenever an execution stops iterating without failing: interrupted, cancelled
    /// or finished.
    fn on_stop(
        &self,
        it: &Iteration<'_, Self>,
    ) -> impl Future<Output = Result<(), JobError>> + Send {
        let _ = it;
        std::future::ready(Ok(()))
    }

    /// Called when the job stops because it was cancelled ([`Client::cancel`](crate::Client::cancel)),
    /// before [`IterableJob::on_stop`] and [`IterableJob::on_complete`].
    fn on_cancel(
        &self,
        it: &Iteration<'_, Self>,
    ) -> impl Future<Output = Result<(), JobError>> + Send {
        let _ = it;
        std::future::ready(Ok(()))
    }

    /// Called once the iteration is over: every item processed, the job cancelled or
    /// [aborted](Iteration::abort). The state is deleted afterwards. If this hook (or
    /// `on_cancel` or `on_stop` before it) fails, the job's retry runs these hooks again, not
    /// the items.
    fn on_complete(
        &self,
        it: &Iteration<'_, Self>,
    ) -> impl Future<Output = Result<(), JobError>> + Send {
        let _ = it;
        std::future::ready(Ok(()))
    }

    /// How long one execution may iterate before the job goes back to its queue, overriding
    /// the process setting ([`HenchBuilder::max_iteration_runtime`](crate::HenchBuilder::max_iteration_runtime))
    /// when `Some`.
    fn max_iteration_runtime(&self) -> Option<Duration> {
        None
    }

    /// How often the cursor is saved and cancellation checked while iterating (default
    /// [`DEFAULT_CHECKPOINT_INTERVAL`]). Each checkpoint is a Redis round trip; with
    /// [`Duration::ZERO`] it happens before every item.
    fn checkpoint_interval(&self) -> Duration {
        DEFAULT_CHECKPOINT_INTERVAL
    }

    /// When (or whether) to retry after a failure, as for [`Job::retry_in`].
    fn retry_in(&self, count: u32, error: &JobError, job: &JobPayload) -> RetryIn {
        let _ = (count, error, job);
        RetryIn::Default
    }

    /// Called once the job's retries are exhausted, as for [`Job::retries_exhausted`].
    fn retries_exhausted(
        &self,
        job: &JobPayload,
        error: &JobError,
    ) -> impl Future<Output = Exhausted> + Send {
        let _ = (job, error);
        std::future::ready(Exhausted::Default)
    }

    /// The part of the arguments that identifies a duplicate of a unique job, as for
    /// [`Job::unique_key`].
    fn unique_key(args: &Self::Args) -> Option<Value> {
        let _ = args;
        None
    }

    /// The throttle key of these arguments, as for [`Job::throttle_key`]. Throttles apply per
    /// execution (each time the job starts), not per item: to limit items, use a
    /// [`Limiter`](crate::limit::Limiter) in [`IterableJob::each_iteration`].
    fn throttle_key(args: &Self::Args) -> Option<String> {
        let _ = args;
        None
    }

    /// A throttle defined with the job type, as for [`Job::throttle`].
    fn throttle() -> Option<Throttle> {
        None
    }
}

impl<J: IterableJob> Job for J {
    const NAME: &'static str = <J as IterableJob>::NAME;
    type Args = <J as IterableJob>::Args;

    fn options() -> JobOptions {
        <J as IterableJob>::options()
    }

    fn perform(
        &self,
        ctx: &JobContext,
        args: Self::Args,
    ) -> impl Future<Output = Result<(), JobError>> + Send {
        iterate(self, ctx, args)
    }

    fn retry_in(&self, count: u32, error: &JobError, job: &JobPayload) -> RetryIn {
        <J as IterableJob>::retry_in(self, count, error, job)
    }

    fn retries_exhausted(
        &self,
        job: &JobPayload,
        error: &JobError,
    ) -> impl Future<Output = Exhausted> + Send {
        <J as IterableJob>::retries_exhausted(self, job, error)
    }

    fn unique_key(args: &Self::Args) -> Option<Value> {
        <J as IterableJob>::unique_key(args)
    }

    fn throttle_key(args: &Self::Args) -> Option<String> {
        <J as IterableJob>::throttle_key(args)
    }

    fn throttle() -> Option<Throttle> {
        <J as IterableJob>::throttle()
    }
}

/// What the hooks and [`IterableJob::each_iteration`] see of the iteration: the job context
/// (it derefs to [`JobContext`]), the arguments and the progress.
pub struct Iteration<'a, J: IterableJob> {
    ctx: &'a JobContext,
    args: &'a J::Args,
    cursor: Option<&'a J::Cursor>,
    executions: u64,
    runtime: Duration,
    aborted: &'a AtomicBool,
}

impl<'a, J: IterableJob> Iteration<'a, J> {
    /// The job context.
    pub fn ctx(&self) -> &'a JobContext {
        self.ctx
    }

    /// The job's arguments.
    pub fn args(&self) -> &'a J::Args {
        self.args
    }

    /// The cursor of the last item processed (`None` before the first one); while processing
    /// an item, the position of that item.
    pub fn cursor(&self) -> Option<&'a J::Cursor> {
        self.cursor
    }

    /// This execution's number: 1 for the first one, then one more after each interruption or
    /// failure.
    pub fn executions(&self) -> u64 {
        self.executions
    }

    /// Total time spent iterating, previous executions included.
    pub fn runtime(&self) -> Duration {
        self.runtime
    }

    /// Ends the iteration after the current item, as if it were the last one: `on_stop` and
    /// `on_complete` run and the state is deleted.
    ///
    /// Called from [`IterableJob::on_start`] or [`IterableJob::on_resume`], it ends the
    /// iteration before the first item of the execution (for example when a precondition no
    /// longer holds). From `on_stop`, `on_cancel` and `on_complete`, which run once the
    /// execution stopped iterating, it has no effect.
    pub fn abort(&self) {
        self.aborted.store(true, Ordering::Relaxed);
    }

    /// `true` once [`Iteration::abort`] was called.
    pub fn is_aborted(&self) -> bool {
        self.aborted.load(Ordering::Relaxed)
    }
}

impl<J: IterableJob> Deref for Iteration<'_, J> {
    type Target = JobContext;

    fn deref(&self) -> &JobContext {
        self.ctx
    }
}

impl<J: IterableJob> fmt::Debug for Iteration<'_, J> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Iteration")
            .field("job", &<J as IterableJob>::NAME)
            .field("jid", &self.ctx.jid())
            .field("executions", &self.executions)
            .field("runtime", &self.runtime)
            .finish_non_exhaustive()
    }
}

/// Why an execution stopped iterating.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stop {
    /// No more items.
    Completed,
    /// [`Iteration::abort`] was called.
    Aborted,
    /// [`crate::Client::cancel`] was called.
    Cancelled,
    /// The process is stopping or the run time is up.
    Interrupted,
}

impl Stop {
    /// How an iteration that is over ended, as recorded in its state (`done`; an interrupted
    /// iteration is not over, and records nothing).
    fn as_str(self) -> &'static str {
        match self {
            Stop::Completed => "completed",
            Stop::Aborted => "aborted",
            Stop::Cancelled => "cancelled",
            Stop::Interrupted => "interrupted",
        }
    }

    /// Reads [`Stop::as_str`] back (an unknown value is taken as completed).
    fn from_done(value: &str) -> Self {
        match value {
            "aborted" => Stop::Aborted,
            "cancelled" => Stop::Cancelled,
            _ => Stop::Completed,
        }
    }
}

/// The progress of an iteration, as saved in its state.
struct Progress<'s, C> {
    store: Store<'s>,
    jid: &'s str,
    executions: u64,
    cursor: Option<C>,
    /// The cursor this execution started from, as saved (JSON): a save beyond it is progress.
    started_from: String,
    /// Whether this execution forgot the job's recoveries already (with its first progress).
    recoveries_forgotten: AtomicBool,
    /// Run time of the previous executions.
    earlier: Duration,
    started: Instant,
    cancelled: bool,
    /// How the iteration ended, when an earlier execution saw it end (only its finishing hooks
    /// remain).
    done: Option<Stop>,
}

impl<'s, C: Serialize + DeserializeOwned> Progress<'s, C> {
    /// Reads a saved state (empty when the job starts).
    fn restore(store: Store<'s>, jid: &'s str, fields: &Fields) -> Result<Self, JobError> {
        let cursor = match fields.get(CURSOR) {
            None => None,
            Some(json) => serde_json::from_str::<Option<C>>(json).map_err(|error| {
                JobError::new(
                    INVALID_CURSOR,
                    format!("the saved cursor `{json}` cannot be read: {error}"),
                )
            })?,
        };
        let started_from = serde_json::to_string(&cursor)?;
        Ok(Self {
            store,
            jid,
            executions: state::parse_executions(fields),
            cursor,
            started_from,
            recoveries_forgotten: AtomicBool::new(false),
            earlier: state::parse_runtime(fields),
            started: Instant::now(),
            cancelled: fields.contains_key(state::CANCELLED),
            done: fields.get(DONE).map(|value| Stop::from_done(value)),
        })
    }

    /// Total run time so far.
    fn runtime(&self) -> Duration {
        self.earlier.saturating_add(self.started.elapsed())
    }

    /// Saves the cursor, the executions and the run time; returns whether the job was
    /// cancelled.
    async fn save(&self) -> Result<bool, JobError> {
        self.save_with(None).await
    }

    /// [`Progress::save`], plus the `done` field once the iteration is over. The first save of
    /// an execution beyond the cursor it started from also forgets the job's recoveries from
    /// processes that died while they held it: a job that makes progress is not what kills them
    /// (only crashes without progress in between count toward `max_recoveries`).
    async fn save_with(&self, done: Option<Stop>) -> Result<bool, JobError> {
        let cursor = serde_json::to_string(&self.cursor)?;
        let forget =
            cursor != self.started_from && !self.recoveries_forgotten.load(Ordering::Relaxed);
        let mut fields = vec![
            (EXECUTIONS, self.executions.to_string()),
            (CURSOR, cursor),
            (RUNTIME, self.runtime().as_secs_f64().to_string()),
        ];
        if let Some(stop) = done {
            fields.push((DONE, stop.as_str().to_owned()));
        }
        let cancelled = self.store.save(self.jid, &fields, forget).await?;
        if forget {
            self.recoveries_forgotten.store(true, Ordering::Relaxed);
        }
        Ok(cancelled)
    }

    /// Records that the iteration is over (`stop`), before the finishing hooks run: if one of
    /// them fails, or the process stops before the job is acknowledged, the job's next
    /// execution runs them again instead of the items since the last checkpoint. Best effort:
    /// a job whose every item is done is not failed for want of this record (logged).
    async fn save_done(&self, stop: Stop) {
        if let Err(error) = self.save_with(Some(stop)).await {
            tracing::warn!(
                %error,
                "could not record that an iterable job finished iterating; if it runs again, \
                 it continues from its last saved cursor"
            );
        }
    }

    /// Saves the progress before the job fails with `error`.
    async fn fail(&self, error: JobError) -> JobError {
        self.save_before_failing().await;
        error
    }

    /// Saves the progress of a job about to fail (a failure to save is logged: the job's error,
    /// or panic, is the one that matters).
    async fn save_before_failing(&self) {
        if let Err(save_error) = self.save().await {
            tracing::warn!(
                error = %save_error,
                "could not save the cursor of a failing iterable job; its retry continues from \
                 the last cursor saved"
            );
        }
    }

    /// Runs `future` (the enumerator's next item, or `each_iteration`); if it panics, saves the
    /// progress before letting the panic go on, as [`Progress::fail`] does for an error, so that
    /// the retry resumes after the last item processed rather than at the last checkpoint.
    async fn saving_on_panic<T>(&self, future: impl Future<Output = T>) -> T {
        // Unwind safety: after a panic only `self` is read (its cursor is that of the last item
        // completed, set after each successful item), then the panic resumes.
        match AssertUnwindSafe(future).catch_unwind().await {
            Ok(output) => output,
            Err(payload) => {
                // With where it happened (for jobs that store backtraces), taken before the save
                // awaits: meanwhile this task may move to another thread, and other jobs panic
                // on this one.
                let panic = CaughtPanic::new(payload);
                self.save_before_failing().await;
                panic.resume()
            }
        }
    }
}

/// Runs an iterable job: the `perform` of every [`IterableJob`].
async fn iterate<J: IterableJob>(job: &J, ctx: &JobContext, args: J::Args) -> Result<(), JobError> {
    let jid = ctx.jid();
    let store = Store::of(ctx.client());
    let fields = store.load(jid).await?;
    let mut progress = Progress::<J::Cursor>::restore(store, jid, &fields)?;
    progress.executions += 1;
    let aborted = AtomicBool::new(false);
    if let Some(stop) = progress.done {
        tracing::debug!(
            executions = progress.executions,
            "the iteration is over: only its finishing hooks remain"
        );
        return finish(job, ctx, &args, &progress, &aborted, stop).await;
    }
    let limit = job.max_iteration_runtime().or(ctx.max_iteration_runtime());
    let checkpoint_interval = job.checkpoint_interval();

    let mut items = job.build_enumerator(ctx, &args, progress.cursor.clone())?;
    {
        let it = view(ctx, &args, &progress, &aborted);
        if progress.executions == 1 {
            job.on_start(&it).await?;
        } else {
            job.on_resume(&it).await?;
        }
    }

    let mut processed: u64 = 0;
    let mut checkpoint = Instant::now();
    let mut stop = loop {
        if progress.cancelled {
            break Stop::Cancelled;
        }
        // After an item that called `abort()`, or before the first item when `on_start` or
        // `on_resume` did.
        if aborted.load(Ordering::Relaxed) {
            break Stop::Aborted;
        }
        // The next item first: an execution that processed the last one completes, even when
        // the process is stopping or its time is up (Sidekiq checks for both once its
        // enumerator yields another item too). Going back to the queue would cost a whole
        // execution, after every job queued meanwhile, only to find nothing left to do.
        let (item, next_cursor) = match progress.saving_on_panic(items.next()).await {
            None => break Stop::Completed,
            Some(Ok(next)) => next,
            Some(Err(error)) => {
                drop(items);
                return Err(
                    item_error(job, ctx, &args, &progress, &aborted, error, processed).await,
                );
            }
        };
        // Then stop before this item when the process is stopping, or when the time is up
        // (after at least one item per execution): the item is dropped, and the enumerator
        // yields it again after the saved cursor (that of the last item processed).
        if ctx.is_stopping()
            || (processed > 0 && limit.is_some_and(|limit| progress.started.elapsed() >= limit))
        {
            break Stop::Interrupted;
        }
        // Now and then, save the cursor, which also tells whether the job was cancelled.
        if processed > 0 && checkpoint.elapsed() >= checkpoint_interval {
            progress.cancelled = progress.save().await?;
            checkpoint = Instant::now();
            if progress.cancelled {
                break Stop::Cancelled;
            }
        }
        let item_started = Instant::now();
        let result = {
            let it = view(ctx, &args, &progress, &aborted);
            progress
                .saving_on_panic(job.each_iteration(item, &it))
                .await
        };
        warn_if_slow(ctx, item_started.elapsed());
        if let Err(error) = result {
            drop(items);
            return Err(item_error(job, ctx, &args, &progress, &aborted, error, processed).await);
        }
        progress.cursor = Some(next_cursor);
        processed += 1;
    };
    drop(items);

    if stop == Stop::Interrupted {
        if progress.save().await? {
            // Cancelled meanwhile: finish now rather than come back for nothing.
            stop = Stop::Cancelled;
        } else {
            job.on_stop(&view(ctx, &args, &progress, &aborted)).await?;
            tracing::debug!(
                executions = progress.executions,
                processed,
                "interrupted after saving its cursor; it continues later"
            );
            return Err(JobError::interrupted());
        }
    }
    if processed == 0 {
        match stop {
            Stop::Completed => tracing::debug!("the enumerator had nothing to iterate"),
            Stop::Aborted => tracing::debug!("aborted before the first item"),
            Stop::Cancelled | Stop::Interrupted => {}
        }
    }
    progress.save_done(stop).await;
    finish(job, ctx, &args, &progress, &aborted, stop).await
}

/// The error of an item (or of the enumerator): the cursor of the last item processed is saved,
/// then the job fails with it, unless it is a limiter's refusal ([`JobError::is_over_limit`]):
/// that one stops the execution like an interruption (`on_stop` runs) and is returned, so that
/// the worker reschedules the job at its slot (without using a retry) and the next execution
/// resumes after the last item processed (`docs/design/ratelimit.md` §2.5). When this execution
/// processed items (`processed`) before the refusal, the refusal says so: the job is being
/// paced, not stuck, so its reschedule starts the limiters' budget of reschedules again.
async fn item_error<J: IterableJob>(
    job: &J,
    ctx: &JobContext,
    args: &J::Args,
    progress: &Progress<'_, J::Cursor>,
    aborted: &AtomicBool,
    error: JobError,
    processed: u64,
) -> JobError {
    let mut error = progress.fail(error).await;
    if !error.is_over_limit() {
        return error;
    }
    tracing::debug!(
        executions = progress.executions,
        processed,
        "a limiter refused an item: the job stops here and continues later from its cursor"
    );
    if processed > 0 {
        error = error.context(crate::limit::engine::MadeProgress { items: processed });
    }
    match job.on_stop(&view(ctx, args, progress, aborted)).await {
        Ok(()) => error,
        Err(stop_error) => stop_error,
    }
}

/// The end of an iteration that is over (`stop` is not [`Stop::Interrupted`]): the finishing
/// hooks, then the deletion of the state.
async fn finish<J: IterableJob>(
    job: &J,
    ctx: &JobContext,
    args: &J::Args,
    progress: &Progress<'_, J::Cursor>,
    aborted: &AtomicBool,
    stop: Stop,
) -> Result<(), JobError> {
    let it = view(ctx, args, progress, aborted);
    if stop == Stop::Cancelled {
        tracing::info!("iterable job cancelled");
        job.on_cancel(&it).await?;
    }
    job.on_stop(&it).await?;
    job.on_complete(&it).await?;
    // The job is complete whatever happens to its state now. Failing it would make it run
    // again for nothing, and from its first item if Redis deleted the state after all (a reply
    // that times out does not mean the command did not run). A state left behind expires, and
    // says that the iteration is over.
    if let Err(error) = progress.store.delete(progress.jid).await {
        tracing::warn!(
            %error,
            "could not delete the state of a completed iterable job; it expires in 30 days"
        );
    }
    tracing::debug!(
        executions = progress.executions,
        runtime_secs = progress.runtime().as_secs_f64(),
        "completed iteration"
    );
    Ok(())
}

/// What the hooks see of `progress`.
fn view<'a, J: IterableJob>(
    ctx: &'a JobContext,
    args: &'a J::Args,
    progress: &'a Progress<'_, J::Cursor>,
    aborted: &'a AtomicBool,
) -> Iteration<'a, J> {
    Iteration {
        ctx,
        args,
        cursor: progress.cursor.as_ref(),
        executions: progress.executions,
        runtime: progress.runtime(),
        aborted,
    }
}

/// Warns when one iteration takes longer than the shutdown timeout: such iterations are
/// interrupted by every shutdown and redone.
fn warn_if_slow(ctx: &JobContext, elapsed: Duration) {
    if let Some(timeout) = ctx.shutdown_timeout()
        && elapsed > timeout
    {
        tracing::warn!(
            elapsed_secs = elapsed.as_secs_f64(),
            timeout_secs = timeout.as_secs_f64(),
            "an iteration took longer than the shutdown timeout: shutdowns will interrupt it \
             and it will run again; process smaller items"
        );
    }
}
