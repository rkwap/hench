//! Error types.
//!
//! * [`Error`] is returned by hench's own operations (connecting to Redis, pushing jobs,
//!   validating configuration, ...).
//! * [`JobError`] is what a job's `perform` returns when it fails. It wraps an
//!   [`anyhow::Error`] and records an error *class* (the Rust type name of the original
//!   error, or `panic`) which is stored in the job payload's `error_class` field and shown
//!   in the dashboard.

use std::any::Any;
use std::backtrace::{Backtrace, BacktraceStatus};
use std::borrow::Cow;
use std::cell::{Cell, RefCell};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Once;
use std::task::{Context, Poll};

/// Result alias used throughout hench. The error type defaults to [`Error`].
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors returned by hench operations.
///
/// The variants wrapping another error (`Redis`, `Json`, `Io`) include its message in their
/// own and do not report it as their [`source`](std::error::Error::source): error reports that
/// print a chain of sources (`anyhow`'s `{:#}`, `Caused by:` lists) would otherwise repeat it.
/// Match the variant to reach the wrapped error.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A Redis command or connection failed.
    #[error("redis error: {0}")]
    Redis(::redis::RedisError),

    /// JSON serialization or deserialization failed.
    #[error("JSON error: {0}")]
    Json(serde_json::Error),

    /// The job is malformed: an empty class or queue, a non-finite `at`, an oversized
    /// `retry_for`, arguments that do not serialize to a JSON array, ... These are the
    /// situations in which Sidekiq raises `ArgumentError`.
    #[error("invalid job: {}", .0.trim_end())]
    InvalidJob(String),

    /// The configuration is invalid (duplicate job names, empty queue names, bad Redis URL, ...).
    /// (Displayed without trailing whitespace: parsers' messages, such as TOML's, can end with
    /// a line break.)
    #[error("configuration error: {}", .0.trim_end())]
    Config(String),

    /// No job is registered under this name.
    #[error("unknown job class `{0}`")]
    UnknownJob(String),

    /// The operation was abandoned because hench is shutting down.
    #[error("hench is shutting down")]
    Shutdown,

    /// A job executed in-process (for example by an inline testing client) failed.
    #[error("job failed: {0}")]
    Job(JobError),

    /// A startup callback failed or panicked, which stops the start
    /// ([`HenchBuilder::on_startup_try`](crate::HenchBuilder::on_startup_try),
    /// [`HenchBuilder::on_startup`](crate::HenchBuilder::on_startup)).
    #[error("a startup callback failed: {0}")]
    Startup(JobError),

    /// An I/O error outside of Redis (for example while spawning a test `redis-server`).
    #[error("I/O error: {0}")]
    Io(std::io::Error),

    /// A push of a unique job whose lock another job holds, with
    /// [`OnConflict::Error`](crate::OnConflict::Error).
    #[error(
        "duplicate job: a {class} job with the same unique key is held by {holder} (digest \
         {digest})"
    )]
    DuplicateJob {
        /// The job's class.
        class: String,
        /// The lock's digest.
        digest: String,
        /// The jid of the job holding the lock.
        holder: String,
    },

    /// An invalid periodic schedule (definition, API call or CLI).
    #[error("invalid periodic job: {}", .0.trim_end())]
    InvalidSchedule(String),

    /// A batch operation was refused.
    #[error("batch error: {0}")]
    Batch(crate::batch::BatchError),

    /// A limiter refused (see [`OverLimit`](crate::OverLimit)). Returned from a job, it
    /// reschedules the job instead of failing it.
    #[error("{0}")]
    OverLimit(crate::limit::OverLimit),
}

impl From<::redis::RedisError> for Error {
    fn from(error: ::redis::RedisError) -> Self {
        Error::Redis(error)
    }
}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Error::Json(error)
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Error::Io(error)
    }
}

impl From<JobError> for Error {
    fn from(error: JobError) -> Self {
        Error::Job(error)
    }
}

impl Error {
    /// This error as a [`JobError`] for the error handlers: the job error itself for
    /// [`Error::Job`] (so that its class, `panic` for example, is kept), otherwise a
    /// `JobError` wrapping it.
    pub(crate) fn into_job_error(self) -> JobError {
        match self {
            Error::Job(error) => error,
            other => JobError::from(other),
        }
    }
}

/// The error type returned by jobs.
///
/// A `JobError` wraps an [`anyhow::Error`] together with an error *class*: a short name
/// identifying the kind of failure, stored in the job's `error_class` field when it is
/// retried or killed (the Rust equivalent of a Ruby exception class).
///
/// Any error that converts into [`anyhow::Error`] converts into `JobError`, so `?` works
/// for standard library errors, your own error types and `anyhow::Error` alike. The class
/// is the [`std::any::type_name`] of the converted value (for example
/// `std::io::error::Error`; the exact text is whatever the compiler reports for the type).
///
/// ```
/// use hench::JobError;
///
/// fn read_config() -> Result<Vec<u8>, JobError> {
///     let bytes = std::fs::read("/definitely/not/here")?;
///     Ok(bytes)
/// }
///
/// let err = read_config().unwrap_err();
/// assert!(err.class().contains("io"));
/// assert!(!err.message().is_empty());
/// ```
///
/// `JobError` deliberately does not implement [`std::error::Error`] (like `anyhow::Error`):
/// that is what makes the blanket `From` conversion possible. Use [`JobError::as_anyhow`]
/// or [`JobError::into_anyhow`] when a `std::error::Error` is needed.
pub struct JobError {
    class: Cow<'static, str>,
    error: anyhow::Error,
    /// Where the error came from, when it was created while its job captured origins (see
    /// [`capturing_origins`]).
    origin: Option<Box<Origin>>,
}

impl JobError {
    /// Class of errors created from a caught panic.
    pub const PANIC: &'static str = "panic";
    /// Class of errors created by [`JobError::msg`].
    pub const MESSAGE: &'static str = "hench::JobError";
    /// Class used when a job's `class` is not registered in the worker process.
    pub const UNKNOWN_JOB: &'static str = "hench::UnknownJob";
    /// Class used when a job's arguments cannot be deserialized into the job's `Args`.
    pub const INVALID_ARGS: &'static str = "hench::InvalidArgs";
    /// Class used when [`JobContext::data`](crate::JobContext::data) finds no value of the
    /// requested type.
    pub const MISSING_DATA: &'static str = "hench::MissingData";
    /// Class used for jobs interrupted by a hard shutdown.
    pub const SHUTDOWN: &'static str = "hench::Shutdown";
    /// Class of the error given to death handlers when a job is killed through the API
    /// ([`DeadSet::kill`](crate::api::DeadSet::kill) and friends, with notification).
    pub const KILLED: &'static str = "hench::Killed";
    /// Class of the error with which a job asks to be pushed again to the back of its queue
    /// instead of being retried ([`JobError::interrupted`]); iterable jobs
    /// ([`crate::iterable`]) return it when they save their progress before the process stops
    /// or when their run time is up.
    pub const INTERRUPTED: &'static str = "hench::Interrupted";
    /// Class of the error with which the endpoint stops a member of a cancelled batch
    /// ([`JobError::batch_cancelled`]): workers count the member as skipped and acknowledge it,
    /// without retrying it or reporting an error.
    pub const BATCH_CANCELLED: &'static str = "hench::BatchCancelled";
    /// Class of a limiter's refusal ([`JobError::over_limit`]): workers reschedule the job
    /// instead of failing it, within its reschedule budget.
    pub const OVER_LIMIT: &'static str = "hench::OverLimit";
    /// Class of the error of a *poison pill*: a job recovered from processes that died while
    /// they held it more than [`max_recoveries`](crate::Config::max_recoveries) times, which the
    /// orphan check moved to the morgue instead of pushing it back to kill another process
    /// (reliable fetch; see [`crate::fetch`]). Its death handlers and the error handlers get
    /// it.
    pub const CRASH_LOOP: &'static str = "hench::CrashLoop";

    /// Creates an error from a message, with class [`JobError::MESSAGE`].
    pub fn msg<M>(message: M) -> Self
    where
        M: fmt::Display + fmt::Debug + Send + Sync + 'static,
    {
        Self {
            class: Cow::Borrowed(Self::MESSAGE),
            error: anyhow::Error::msg(message),
            origin: Origin::here(),
        }
    }

    /// Creates an error with an explicit class and message.
    pub fn new<C, M>(class: C, message: M) -> Self
    where
        C: Into<Cow<'static, str>>,
        M: fmt::Display + fmt::Debug + Send + Sync + 'static,
    {
        Self {
            class: class.into(),
            error: anyhow::Error::msg(message),
            origin: Origin::here(),
        }
    }

    /// Wraps an [`anyhow::Error`] with an explicit class.
    pub fn with_anyhow<C>(class: C, error: anyhow::Error) -> Self
    where
        C: Into<Cow<'static, str>>,
    {
        Self {
            class: class.into(),
            error,
            origin: Origin::here(),
        }
    }

    /// Wraps a boxed standard error. The class is `Box<dyn std::error::Error>` because the
    /// concrete type has been erased.
    pub fn from_boxed(error: Box<dyn std::error::Error + Send + Sync + 'static>) -> Self {
        Self {
            class: Cow::Borrowed("Box<dyn std::error::Error>"),
            error: anyhow::Error::from_boxed(error),
            origin: Origin::here(),
        }
    }

    /// Creates an error from a panic payload (as returned by
    /// [`std::panic::catch_unwind`]), with class [`JobError::PANIC`].
    pub fn panic(payload: Box<dyn Any + Send + 'static>) -> Self {
        let message = panic_message(payload.as_ref());
        Self {
            class: Cow::Borrowed(Self::PANIC),
            error: anyhow::Error::msg(message),
            // Where it panicked is known to the panic hook, see `catch_panic`.
            origin: None,
        }
    }

    /// The error recorded when a job's class is not registered in this process.
    pub fn unknown_job(class: &str) -> Self {
        Self::new(
            Self::UNKNOWN_JOB,
            format!("no job is registered under the name `{class}`"),
        )
    }

    /// The error recorded when a job's arguments do not deserialize into its `Args` type.
    pub fn invalid_args(job: &str, error: serde_json::Error) -> Self {
        Self::new(
            Self::INVALID_ARGS,
            format!("invalid arguments for job `{job}`: {error}"),
        )
    }

    /// The error with which a job asks to run again later: workers push it back to the end of
    /// its queue (through client middleware) and acknowledge it, without counting a failure,
    /// retrying it or reporting an error. Interrupted [iterable jobs](crate::iterable) return
    /// it after saving their cursor.
    pub fn interrupted() -> Self {
        Self::new(
            Self::INTERRUPTED,
            "interrupted: the job will continue from where it stopped",
        )
    }

    /// `true` for [`JobError::interrupted`] errors (class [`JobError::INTERRUPTED`]).
    pub fn is_interrupted(&self) -> bool {
        self.class == Self::INTERRUPTED
    }

    /// The error with which a member of a cancelled batch stops (between the steps of a long
    /// job: `ctx.check_batch().await?`). Workers count the member as skipped.
    pub fn batch_cancelled() -> Self {
        Self::new(
            Self::BATCH_CANCELLED,
            "the job's batch was cancelled: the job is skipped",
        )
    }

    /// `true` for [`JobError::batch_cancelled`] errors (class [`JobError::BATCH_CANCELLED`]).
    pub fn is_batch_cancelled(&self) -> bool {
        self.class == Self::BATCH_CANCELLED
    }

    /// A limiter's refusal as a job error (class [`JobError::OVER_LIMIT`], the refusal as its
    /// source): workers reschedule the job instead of failing it.
    pub fn over_limit(over: crate::limit::OverLimit) -> Self {
        Self::with_anyhow(Self::OVER_LIMIT, anyhow::Error::new(over))
    }

    /// The limiter's refusal this error is (or wraps, through context): an
    /// [`OverLimit`](crate::OverLimit), or a [`hench::Error::OverLimit`](Error::OverLimit).
    pub fn as_over_limit(&self) -> Option<&crate::limit::OverLimit> {
        if let Some(over) = self.error.downcast_ref::<crate::limit::OverLimit>() {
            return Some(over);
        }
        match self.error.downcast_ref::<Error>() {
            Some(Error::OverLimit(over)) => Some(over),
            _ => None,
        }
    }

    /// `true` when the error is a limiter's refusal (see [`JobError::as_over_limit`]).
    pub fn is_over_limit(&self) -> bool {
        self.as_over_limit().is_some()
    }

    /// Replaces the class of this error.
    #[must_use]
    pub fn with_class<C>(mut self, class: C) -> Self
    where
        C: Into<Cow<'static, str>>,
    {
        self.class = class.into();
        self
    }

    /// Wraps the error with additional context (see [`anyhow::Error::context`]). The class
    /// is kept.
    #[must_use]
    pub fn context<C>(self, context: C) -> Self
    where
        C: fmt::Display + Send + Sync + 'static,
    {
        Self {
            class: self.class,
            error: self.error.context(context),
            origin: self.origin,
        }
    }

    /// The error class, e.g. `std::io::error::Error`, `panic` or `hench::UnknownJob`.
    pub fn class(&self) -> &str {
        &self.class
    }

    /// `true` if this error was created from a caught panic.
    pub fn is_panic(&self) -> bool {
        self.class == Self::PANIC
    }

    /// The error message: the error followed by its causes, separated by `": "`. A cause
    /// whose text already ends the message (a common pattern for wrapper errors that
    /// include their source in their own `Display`) is not repeated.
    pub fn message(&self) -> String {
        let mut message = String::new();
        for (index, cause) in self.error.chain().enumerate() {
            let text = cause.to_string();
            if index == 0 {
                message = text;
            } else if !text.is_empty() && !message.ends_with(&text) {
                message.push_str(": ");
                message.push_str(&text);
            }
        }
        message
    }

    /// Lines describing where the error came from, stored (compressed) in `error_backtrace`
    /// when a job enables `backtrace`: one `caused by: ...` line per source error, then, for a
    /// panic, `panicked at <file>:<line>:<column>`, then the frames of the stack where the
    /// error was created (or the panic happened), starting with the job's own code.
    ///
    /// Workers and testing clients capture that stack for the jobs that enable `backtrace`,
    /// whatever the environment (a panic hook, installed when the first such job runs and
    /// calling the hook installed before it, records where panics happen). Other errors carry
    /// the backtrace anyhow captured, if any (when `RUST_BACKTRACE` or `RUST_LIB_BACKTRACE` is
    /// set).
    pub fn backtrace_lines(&self) -> Vec<String> {
        let mut lines: Vec<String> = self
            .error
            .chain()
            .skip(1)
            .map(|cause| format!("caused by: {cause}"))
            .collect();
        match &self.origin {
            Some(origin) => {
                if let Some(location) = &origin.location {
                    lines.push(format!("panicked at {location}"));
                }
                if let Some(backtrace) = &origin.backtrace {
                    let frames = backtrace_frames(&backtrace.to_string());
                    lines.extend(own_frames(frames, origin.location.as_deref()));
                }
            }
            None => {
                let backtrace = self.error.backtrace();
                if backtrace.status() == BacktraceStatus::Captured {
                    lines.extend(backtrace_frames(&backtrace.to_string()));
                }
            }
        }
        lines
    }

    /// Records where this error (a panic) came from, as the panic hook saw it.
    fn with_origin(mut self, origin: Option<Box<Origin>>) -> Self {
        if origin.is_some() {
            self.origin = origin;
        }
        self
    }

    /// Returns the wrapped error if it is (or was converted from) an `E`, see
    /// [`anyhow::Error::downcast_ref`].
    pub fn downcast_ref<E>(&self) -> Option<&E>
    where
        E: fmt::Display + fmt::Debug + Send + Sync + 'static,
    {
        self.error.downcast_ref::<E>()
    }

    /// `true` if the wrapped error is an `E`, see [`anyhow::Error::is`].
    pub fn is<E>(&self) -> bool
    where
        E: fmt::Display + fmt::Debug + Send + Sync + 'static,
    {
        self.error.is::<E>()
    }

    /// The wrapped [`anyhow::Error`].
    pub fn as_anyhow(&self) -> &anyhow::Error {
        &self.error
    }

    /// Unwraps into the inner [`anyhow::Error`], dropping the class.
    pub fn into_anyhow(self) -> anyhow::Error {
        self.error
    }
}

/// Where a job's error came from (see [`JobError::backtrace_lines`]).
struct Origin {
    /// Where a panic happened (`file:line:column`).
    location: Option<String>,
    /// The stack where the error was created or the panic happened.
    backtrace: Option<Backtrace>,
}

thread_local! {
    /// Whether the code running on this thread captures the origin of its errors and panics:
    /// set while a job that enables `backtrace` is polled (see [`capturing_origins`]).
    static CAPTURING: Cell<bool> = const { Cell::new(false) };
    /// What the panic hook recorded about the last panic on this thread.
    static PANIC_ORIGIN: RefCell<Option<Box<Origin>>> = const { RefCell::new(None) };
}

impl Origin {
    /// The stack here, if the current code captures origins.
    fn here() -> Option<Box<Self>> {
        if !capturing() {
            return None;
        }
        Some(Box::new(Self {
            location: None,
            backtrace: Some(Backtrace::force_capture()),
        }))
    }
}

/// `true` while the code on this thread captures the origin of its errors and panics.
fn capturing() -> bool {
    CAPTURING.try_with(Cell::get).unwrap_or(false)
}

/// Sets [`CAPTURING`] for its lifetime, restoring the previous value when dropped (also when a
/// panic unwinds through it).
struct CaptureScope {
    previous: bool,
}

impl CaptureScope {
    fn enter() -> Self {
        Self {
            previous: CAPTURING
                .try_with(|capturing| capturing.replace(true))
                .unwrap_or(false),
        }
    }
}

impl Drop for CaptureScope {
    fn drop(&mut self) {
        let previous = self.previous;
        let _ = CAPTURING.try_with(|capturing| capturing.set(previous));
    }
}

/// Runs `future` capturing, if `capture`, the origin of the errors it creates and of its
/// panics: on whatever thread polls it, while it is polled. Used around jobs that enable
/// `backtrace`, so that [`JobError::backtrace_lines`] can tell where they failed without
/// `RUST_BACKTRACE`.
pub(crate) async fn capturing_origins<F: Future>(capture: bool, future: F) -> F::Output {
    if !capture {
        return future.await;
    }
    install_panic_hook();
    let mut future = std::pin::pin!(future);
    std::future::poll_fn(move |cx| {
        let _scope = CaptureScope::enter();
        future.as_mut().poll(cx)
    })
    .await
}

/// Polls `future` (a job's `perform`, or a server middleware) through
/// [`hench_execution_boundary`], whose frame marks, in the stacks captured below it, where hench
/// called the code that failed: the backtrace stored with a job stops there (see
/// [`own_frames`]), leaving out hench's middleware chain, the processor and tokio.
pub(crate) async fn across_boundary<F>(future: F) -> F::Output
where
    F: Future<Output = Result<(), JobError>>,
{
    let mut future = std::pin::pin!(future);
    std::future::poll_fn(move |cx| hench_execution_boundary(future.as_mut(), cx)).await
}

/// See [`across_boundary`]. Never inlined, and the poll is not its last instruction (which
/// could become a jump), so that its frame is in the stack in optimized builds too.
#[inline(never)]
fn hench_execution_boundary(
    future: Pin<&mut dyn Future<Output = Result<(), JobError>>>,
    cx: &mut Context<'_>,
) -> Poll<Result<(), JobError>> {
    let poll = future.poll(cx);
    std::hint::black_box(&poll);
    poll
}

/// Installs, once, a panic hook recording where a panic happens (and the stack there) for code
/// that captures origins, before calling the hook installed before it.
fn install_panic_hook() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            // Every panic replaces the record, so that a stale one is never attached to a
            // later panic.
            let origin = capturing().then(|| {
                Box::new(Origin {
                    location: info.location().map(ToString::to_string),
                    backtrace: Some(Backtrace::force_capture()),
                })
            });
            let _ = PANIC_ORIGIN.try_with(|last| *last.borrow_mut() = origin);
            previous(info);
        }));
    });
}

/// Attaches to an error made from a panic that was just caught on this thread what the panic
/// hook recorded about it, if anything.
pub(crate) fn with_panic_origin(error: JobError) -> JobError {
    error.with_origin(take_panic_origin())
}

/// Takes what the panic hook recorded on this thread about its last panic.
fn take_panic_origin() -> Option<Box<Origin>> {
    PANIC_ORIGIN
        .try_with(|last| last.borrow_mut().take())
        .ok()
        .flatten()
}

/// A panic caught on the thread that panicked, held with what the panic hook recorded about it
/// so that it can be resumed later — after an `.await`, maybe on another thread — and still be
/// reported with its origin.
///
/// The record lives in a thread-local, which [`with_panic_origin`] reads where the unwinding
/// stops: on the thread that panicked when nothing awaits in between. Code that catches a panic,
/// awaits, then resumes it (an iterable job saving its cursor first) would otherwise leave the
/// record behind, where another job's panic replaces or takes it, while the resumed panic finds
/// nothing (or a stale record) on the thread it resumes on: [`CaughtPanic::new`] takes the
/// record right after the catch, [`CaughtPanic::resume`] puts it back right before the panic
/// unwinds again, within the poll that reaches [`with_panic_origin`].
pub(crate) struct CaughtPanic {
    payload: Box<dyn Any + Send>,
    origin: Option<Box<Origin>>,
}

impl CaughtPanic {
    /// Holds `payload`, a panic just caught on this thread, with the hook's record of it.
    pub(crate) fn new(payload: Box<dyn Any + Send>) -> Self {
        Self {
            payload,
            origin: take_panic_origin(),
        }
    }

    /// Resumes the panic on this thread, with its record.
    pub(crate) fn resume(self) -> ! {
        let origin = self.origin;
        let _ = PANIC_ORIGIN.try_with(|last| *last.borrow_mut() = origin);
        std::panic::resume_unwind(self.payload)
    }
}

/// The frames that say something about the job. They start, for a panic at `location`
/// (`file:line:col`), at the frame at that line; otherwise at the first frame that is neither
/// the standard library's nor hench's error handling (the frames of the capture itself come
/// first). They end where hench called the code that failed, the job or a server middleware
/// (the first [`hench_execution_boundary`] frame below the start), without the standard
/// library's frames just above it. All of them if no frame qualifies.
fn own_frames(frames: Vec<String>, location: Option<&str>) -> Vec<String> {
    let at_location = location.and_then(site_of).and_then(|site| {
        frames
            .iter()
            .position(|frame| frame.contains(site.as_str()))
    });
    let start = at_location.or_else(|| frames.iter().position(|frame| !is_machinery(frame)));
    let Some(start) = start else {
        return frames;
    };
    let end = frames
        .iter()
        .skip(start)
        .position(|frame| is_boundary(frame))
        .map_or(frames.len(), |offset| start + offset);
    let mut own: Vec<String> = frames.into_iter().take(end).skip(start).collect();
    while own.len() > 1 && own.last().is_some_and(|frame| is_standard_library(frame)) {
        own.pop();
    }
    own
}

/// `true` for the frame of [`hench_execution_boundary`].
fn is_boundary(frame: &str) -> bool {
    let name = frame.split(" at ").next().unwrap_or(frame);
    name.contains("hench_execution_boundary")
}

/// `file:line:` (the file's name only) of a `path:line:col` location, as backtrace frames show
/// it (their paths may be relative to another directory).
fn site_of(location: &str) -> Option<String> {
    let mut parts = location.rsplitn(3, ':');
    let _column = parts.next()?;
    let line = parts.next()?;
    let path = parts.next()?;
    let file = path.rsplit(['/', '\\']).next()?;
    Some(format!("{file}:{line}:"))
}

/// `true` for frames of the standard library, of the panic machinery or of hench's capture of
/// an error's origin (also when inlined, which shortens their names: hence the check of their
/// file, this one; a job's own helper in a file of the same name, building the error, would be
/// skipped too, which only starts the frames one call later).
fn is_machinery(frame: &str) -> bool {
    is_standard_library(frame)
        || frame.contains("hench::error::")
        || frame.contains("src/error.rs:")
        || frame.contains("src\\error.rs:")
}

/// `true` for frames of the standard library (its source paths start with `/rustc/`) and of
/// the panic machinery.
fn is_standard_library(frame: &str) -> bool {
    const PREFIXES: [&str; 10] = [
        "std::",
        "<std::",
        "core::",
        "<core::",
        "alloc::",
        "<alloc::",
        "rust_begin_unwind",
        "__rustc::",
        "__rust_",
        "_rust_",
    ];
    frame.contains("/rustc/") || PREFIXES.iter().any(|prefix| frame.starts_with(prefix))
}

impl<E> From<E> for JobError
where
    E: Into<anyhow::Error> + 'static,
{
    fn from(error: E) -> Self {
        Self {
            class: Cow::Borrowed(std::any::type_name::<E>()),
            error: error.into(),
            origin: Origin::here(),
        }
    }
}

impl fmt::Display for JobError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            write!(f, "{}: {}", self.class, self.message())
        } else {
            f.write_str(&self.message())
        }
    }
}

impl fmt::Debug for JobError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JobError")
            .field("class", &self.class)
            .field("message", &self.message())
            .finish()
    }
}

/// Extracts a readable message from a panic payload.
pub(crate) fn panic_message(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else if let Some(error) = payload.downcast_ref::<JobError>() {
        error.message()
    } else {
        "panic with a non-string payload".to_owned()
    }
}

/// Turns the `Display` output of a captured [`std::backtrace::Backtrace`] into one line per
/// frame: `function at file:line:column`.
fn backtrace_frames(rendered: &str) -> Vec<String> {
    let mut frames: Vec<String> = Vec::new();
    for line in rendered.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Some(location) = trimmed.strip_prefix("at ")
            && let Some(last) = frames.last_mut()
        {
            last.push_str(" at ");
            last.push_str(location);
            continue;
        }
        let frame = match trimmed.split_once(": ") {
            Some((index, function)) if index.chars().all(|c| c.is_ascii_digit()) => function,
            _ => trimmed,
        };
        frames.push(frame.to_owned());
    }
    frames
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn feature_errors() {
        let cancelled = JobError::batch_cancelled();
        assert!(cancelled.is_batch_cancelled());
        assert_eq!(cancelled.class(), JobError::BATCH_CANCELLED);
        assert!(!JobError::msg("x").is_batch_cancelled());

        let over = crate::limit::OverLimit::new("w:stripe:42");
        let error = JobError::over_limit(over.clone());
        assert_eq!(error.class(), JobError::OVER_LIMIT);
        assert_eq!(
            error.as_over_limit().map(crate::limit::OverLimit::label),
            Some("w:stripe:42")
        );
        assert!(error.is_over_limit());
        // Through `?` from the refusal itself, from `hench::Error`, and with context.
        let from_refusal = JobError::from(over.clone());
        assert!(from_refusal.is_over_limit());
        let from_error = JobError::from(Error::OverLimit(over)).context("calling stripe");
        assert_eq!(
            from_error
                .as_over_limit()
                .map(crate::limit::OverLimit::label),
            Some("w:stripe:42")
        );
        assert!(!JobError::msg("x").is_over_limit());

        let duplicate = Error::DuplicateJob {
            class: "Sync".into(),
            digest: "d".into(),
            holder: "h".into(),
        };
        assert_eq!(
            duplicate.to_string(),
            "duplicate job: a Sync job with the same unique key is held by h (digest d)"
        );
        assert_eq!(
            Error::InvalidSchedule("bad cron ".into()).to_string(),
            "invalid periodic job: bad cron"
        );
        assert_eq!(
            Error::Batch(crate::batch::BatchError::Closed).to_string(),
            "batch error: this batch was already committed; push through a new definition"
        );
    }

    #[derive(Debug, thiserror::Error)]
    #[error("outer failure")]
    struct Outer {
        #[source]
        source: std::io::Error,
    }

    #[derive(Debug, thiserror::Error)]
    #[error("wrapped: {source}")]
    struct Repeating {
        #[source]
        source: std::io::Error,
    }

    fn io_error() -> std::io::Error {
        std::io::Error::new(std::io::ErrorKind::NotFound, "no such thing")
    }

    #[test]
    fn question_mark_records_the_source_type_name() {
        fn fails() -> Result<(), JobError> {
            Err(io_error())?;
            Ok(())
        }
        let err = fails().unwrap_err();
        assert_eq!(err.class(), std::any::type_name::<std::io::Error>());
        assert_eq!(err.message(), "no such thing");
        assert!(err.is::<std::io::Error>());
        assert_eq!(
            err.downcast_ref::<std::io::Error>()
                .map(std::io::Error::kind),
            Some(std::io::ErrorKind::NotFound)
        );
    }

    #[test]
    fn anyhow_errors_convert() {
        fn fails() -> Result<(), JobError> {
            Err(anyhow::anyhow!("plain anyhow"))?;
            Ok(())
        }
        let err = fails().unwrap_err();
        assert_eq!(err.class(), "anyhow::Error");
        assert_eq!(err.message(), "plain anyhow");
    }

    #[test]
    fn hench_errors_convert_both_ways() {
        let err = JobError::from(Error::InvalidJob("bad".into()));
        assert_eq!(err.class(), std::any::type_name::<Error>());
        assert_eq!(err.message(), "invalid job: bad");
        let back: Error = JobError::msg("boom").into();
        assert!(matches!(back, Error::Job(ref e) if e.message() == "boom"));
        assert_eq!(back.to_string(), "job failed: boom");
    }

    #[test]
    fn job_error_passes_through_question_mark() {
        fn inner() -> Result<(), JobError> {
            Err(JobError::new("Custom", "kept"))
        }
        fn outer() -> Result<(), JobError> {
            inner()?;
            Ok(())
        }
        let err = outer().unwrap_err();
        assert_eq!(err.class(), "Custom");
        assert_eq!(err.message(), "kept");
    }

    #[test]
    fn message_includes_causes_without_repeating_them() {
        let err = JobError::from(Outer { source: io_error() });
        assert_eq!(err.message(), "outer failure: no such thing");

        let err = JobError::from(Repeating { source: io_error() });
        assert_eq!(err.message(), "wrapped: no such thing");

        let err = JobError::from(anyhow::Error::new(io_error()).context("loading user"));
        assert_eq!(err.message(), "loading user: no such thing");
    }

    #[test]
    fn context_keeps_class() {
        let err = JobError::from(io_error()).context("while syncing");
        assert_eq!(err.class(), std::any::type_name::<std::io::Error>());
        assert_eq!(err.message(), "while syncing: no such thing");
    }

    #[test]
    fn explicit_constructors() {
        let err = JobError::msg("just a message");
        assert_eq!(err.class(), JobError::MESSAGE);
        assert_eq!(err.to_string(), "just a message");
        assert_eq!(format!("{err:#}"), "hench::JobError: just a message");

        let err = JobError::new("billing::Declined", "card declined").with_class("Other");
        assert_eq!(err.class(), "Other");

        let err = JobError::unknown_job("NoSuchJob");
        assert_eq!(err.class(), JobError::UNKNOWN_JOB);
        assert!(err.message().contains("NoSuchJob"));

        let json_err = serde_json::from_str::<u8>("\"x\"").unwrap_err();
        let err = JobError::invalid_args("MyJob", json_err);
        assert_eq!(err.class(), JobError::INVALID_ARGS);
        assert!(err.message().contains("MyJob"));

        let boxed: Box<dyn std::error::Error + Send + Sync> = Box::new(io_error());
        let err = JobError::from_boxed(boxed);
        assert_eq!(err.message(), "no such thing");
        assert!(!err.class().is_empty());

        let err = JobError::with_anyhow("X", anyhow::anyhow!("y"));
        assert_eq!((err.class(), err.message().as_str()), ("X", "y"));
        assert_eq!(err.into_anyhow().to_string(), "y");
    }

    #[test]
    fn panics_become_job_errors() {
        let payload = std::panic::catch_unwind(|| panic!("static str panic")).unwrap_err();
        let err = JobError::panic(payload);
        assert!(err.is_panic());
        assert_eq!(err.class(), "panic");
        assert_eq!(err.message(), "static str panic");

        let value = 42;
        let payload = std::panic::catch_unwind(|| panic!("formatted {value}")).unwrap_err();
        assert_eq!(JobError::panic(payload).message(), "formatted 42");

        let payload = std::panic::catch_unwind(|| std::panic::panic_any(7_u8)).unwrap_err();
        assert_eq!(
            JobError::panic(payload).message(),
            "panic with a non-string payload"
        );
    }

    #[test]
    fn backtrace_lines_list_the_cause_chain() {
        let err = JobError::from(Outer { source: io_error() });
        let lines = err.backtrace_lines();
        assert_eq!(
            lines.first().map(String::as_str),
            Some("caused by: no such thing")
        );
        // Without causes only frames of a captured backtrace (RUST_LIB_BACKTRACE=1) appear.
        assert!(
            JobError::msg("no causes")
                .backtrace_lines()
                .iter()
                .all(|line| !line.starts_with("caused by"))
        );
    }

    #[test]
    fn backtrace_frames_are_joined_with_their_location() {
        let rendered = "   0: my_crate::job::perform\n             at ./src/job.rs:10:5\n   1: tokio::runtime::task\n   2: main\n             at ./src/main.rs:3:1\n";
        assert_eq!(
            backtrace_frames(rendered),
            vec![
                "my_crate::job::perform at ./src/job.rs:10:5".to_owned(),
                "tokio::runtime::task".to_owned(),
                "main at ./src/main.rs:3:1".to_owned(),
            ]
        );
    }

    #[tokio::test]
    async fn errors_created_while_capturing_record_their_origin() {
        let error = capturing_origins(true, async { JobError::msg("boom") }).await;
        let lines = error.backtrace_lines();
        // The stack where the error was created, without the capture's own frames (this test
        // lives in `src/error.rs` too, so its frames are skipped as well: see
        // `own_frames_skip_the_capture_and_the_panic_machinery` for the trimming itself).
        assert!(!lines.is_empty());
        assert!(
            lines.iter().all(|frame| !frame.contains("force_capture")),
            "{lines:?}"
        );
        // Nothing is captured outside a capturing job.
        let error = capturing_origins(false, async { JobError::msg("boom") }).await;
        assert!(error.origin.is_none());
        assert!(JobError::msg("boom").origin.is_none());
        // `context` keeps the origin.
        let error = capturing_origins(true, async { JobError::msg("boom") })
            .await
            .context("while doing something");
        assert!(error.origin.is_some());
    }

    #[tokio::test]
    async fn panics_record_where_they_happened() {
        let line = line!() + 2;
        let error = crate::job::catch_panic(capturing_origins(true, async {
            panic!("kaboom");
        }))
        .await
        .unwrap_err();
        assert!(error.is_panic());
        assert_eq!(error.message(), "kaboom");
        let lines = error.backtrace_lines();
        let expected = format!("panicked at {}:{line}:", file!());
        assert!(lines[0].starts_with(&expected), "{lines:?}");
        // The frames start at the panic.
        let site = format!("error.rs:{line}:");
        assert!(lines[1].contains(&site), "{lines:?}");
        // A panic outside a capturing job records nothing (and leaves nothing behind).
        let error = crate::job::catch_panic(async { panic!("quiet") })
            .await
            .unwrap_err();
        assert!(error.origin.is_none());
    }

    #[test]
    fn own_frames_skip_the_capture_and_the_panic_machinery() {
        let frames = |names: &[&str]| -> Vec<String> {
            names.iter().map(|name| (*name).to_owned()).collect()
        };
        // An error: past the capture and the standard library's conversion frames.
        assert_eq!(
            own_frames(
                frames(&[
                    "std::backtrace::Backtrace::force_capture",
                    "<hench::error::Origin>::here at ./src/error.rs:1:1",
                    "<hench::error::JobError as core::convert::From<E>>::from",
                    "<core::result::Result<T, E> as FromResidual>::from_residual at /rustc/x/core/src/result.rs:1:1",
                    "<my_app::Send as hench::job::Job>::perform::{closure#0} at ./src/jobs.rs:3:5",
                    "{closure#0}<hench::error::capturing_origins> at ./src/error.rs:2:2",
                ]),
                None
            ),
            frames(&[
                "<my_app::Send as hench::job::Job>::perform::{closure#0} at ./src/jobs.rs:3:5",
                "{closure#0}<hench::error::capturing_origins> at ./src/error.rs:2:2",
            ])
        );
        // A panic: from the frame at its location (whose path may be written differently).
        assert_eq!(
            own_frames(
                frames(&[
                    "{closure#0} at ./src/error.rs:9:9",
                    "<alloc::boxed::Box<F> as core::ops::function::Fn<A>>::call",
                    "core::panicking::panic_bounds_check",
                    "<usize as core::slice::index::SliceIndex<[T]>>::index at /rustc/x/core/src/slice.rs:1:1",
                    "{async_block#0} at ./src/jobs.rs:42:13",
                    "main at ./src/main.rs:1:1",
                ]),
                Some("crates/app/src/jobs.rs:42:13")
            ),
            frames(&[
                "{async_block#0} at ./src/jobs.rs:42:13",
                "main at ./src/main.rs:1:1"
            ])
        );
        // Only internal frames: kept as they are.
        let internal = frames(&["std::rt::lang_start", "core::ops::function"]);
        assert_eq!(own_frames(internal.clone(), None), internal);
        assert_eq!(site_of("a/b/c.rs:10:5").as_deref(), Some("c.rs:10:"));
        assert_eq!(site_of("c.rs"), None);
    }

    #[test]
    fn own_frames_end_where_hench_called_the_code() {
        let frames = |names: &[&str]| -> Vec<String> {
            names.iter().map(|name| (*name).to_owned()).collect()
        };
        // Up to the boundary, without the standard library's frames just above it; the
        // standard library's frames between the job's own are kept.
        assert_eq!(
            own_frames(
                frames(&[
                    "std::backtrace::Backtrace::force_capture",
                    "<hench::error::Origin>::here at ./src/error.rs:1:1",
                    "my_app::parse at ./src/jobs.rs:20:9",
                    "core::iter::adapters::map::map_try_fold at /rustc/x/core/src/iter/map.rs:1:1",
                    "<my_app::Send as hench::job::Job>::perform::{closure#0} at ./src/jobs.rs:3:5",
                    "poll<alloc::boxed::Box<dyn Future>> at /rustc/x/core/src/future/future.rs:1:1",
                    "hench_execution_boundary at /registry/hench-0.1.0/src/error.rs:470:5",
                    "{async_block#0} at /registry/hench-0.1.0/src/middleware.rs:108:55",
                    "tokio::runtime::task::harness::poll_future",
                    "start_thread",
                ]),
                None
            ),
            frames(&[
                "my_app::parse at ./src/jobs.rs:20:9",
                "core::iter::adapters::map::map_try_fold at /rustc/x/core/src/iter/map.rs:1:1",
                "<my_app::Send as hench::job::Job>::perform::{closure#0} at ./src/jobs.rs:3:5",
            ])
        );
        // A panic, from its location; optimized builds without line tables name the boundary
        // by its path. The first boundary below the start ends the frames (a middleware's
        // comes after the job's).
        assert_eq!(
            own_frames(
                frames(&[
                    "core::panicking::panic_bounds_check",
                    "my_app::index at ./src/jobs.rs:42:13",
                    "<core::pin::Pin<P> as core::future::future::Future>::poll",
                    "hench::error::hench_execution_boundary",
                    "my_app::Timing::call::{closure#0}",
                    "hench::error::hench_execution_boundary",
                    "main",
                ]),
                Some("src/jobs.rs:42:13")
            ),
            frames(&["my_app::index at ./src/jobs.rs:42:13"])
        );
        // An error made by a middleware ends at the middleware's boundary.
        assert_eq!(
            own_frames(
                frames(&[
                    "my_app::Timing::call::{closure#0} at ./src/timing.rs:7:5",
                    "hench::error::hench_execution_boundary",
                    "hench::middleware::ServerNext::run",
                ]),
                None
            ),
            frames(&["my_app::Timing::call::{closure#0} at ./src/timing.rs:7:5"])
        );
        // The name decides, not the file.
        assert!(!is_boundary(
            "my_app::run at ./src/hench_execution_boundary.rs:1:1"
        ));
    }

    #[test]
    fn debug_output_shows_class_and_message() {
        let rendered = format!("{:?}", JobError::new("K", "m"));
        assert_eq!(rendered, "JobError { class: \"K\", message: \"m\" }");
    }

    /// Wrapped errors are in the message, not also the source: reports printing the chain of
    /// sources (anyhow's `{:#}`) said "Connection refused (os error 111)" three times.
    #[test]
    fn wrapped_errors_are_said_once() {
        let refused = ::redis::RedisError::from(std::io::Error::from_raw_os_error(111));
        let error = Error::from(refused);
        assert!(std::error::Error::source(&error).is_none());
        let report = format!(
            "{:#}",
            anyhow::Error::new(error).context("cannot reach Redis")
        );
        assert_eq!(report.matches("os error 111").count(), 1, "{report}");
        let json = Error::from(serde_json::from_str::<u8>("x").unwrap_err());
        assert!(std::error::Error::source(&json).is_none());
        let io = Error::from(std::io::Error::other("disk"));
        assert!(std::error::Error::source(&io).is_none());
        assert_eq!(io.to_string(), "I/O error: disk");
        // A configuration message ending with a line break (TOML's do) is shown without it.
        let config = Error::Config("bad.toml: expected i64\nin `timeout`\n".to_owned());
        assert_eq!(
            config.to_string(),
            "configuration error: bad.toml: expected i64\nin `timeout`"
        );
    }

    #[test]
    fn error_display_messages() {
        assert_eq!(
            Error::Config("x".into()).to_string(),
            "configuration error: x"
        );
        assert_eq!(
            Error::UnknownJob("J".into()).to_string(),
            "unknown job class `J`"
        );
        assert_eq!(Error::Shutdown.to_string(), "hench is shutting down");
    }
}
