//! # hench
//!
//! Simple, efficient, Redis-backed background job processing for Rust (tokio), inspired by
//! Sidekiq: same Redis data model and semantics (queues, scheduled jobs, retries with
//! exponential backoff, the morgue, heartbeats, middleware), with typed jobs and reliable
//! fetch by default.
//!
//! ```no_run
//! use hench::prelude::*;
//! use std::time::Duration;
//!
//! #[derive(Default)]
//! struct SendWelcome;
//!
//! impl Job for SendWelcome {
//!     const NAME: &'static str = "SendWelcome";
//!     type Args = (i64,);
//!
//!     fn options() -> JobOptions {
//!         JobOptions::new().queue("mailers").retry(5)
//!     }
//!
//!     async fn perform(&self, ctx: &JobContext, (user_id,): (i64,)) -> Result<(), JobError> {
//!         tracing::info!(jid = ctx.jid(), user_id, "sending welcome email");
//!         Ok(())
//!     }
//! }
//!
//! # async fn demo() -> hench::Result<()> {
//! let hench = Hench::builder()
//!     .redis_url("redis://127.0.0.1/")
//!     .queues(["mailers", "default"])
//!     .register(SendWelcome)
//!     .build()
//!     .await?;
//!
//! // Enqueue (from anywhere that has a client):
//! SendWelcome::perform_async(hench.client(), (42,)).await?;
//! SendWelcome::perform_in(hench.client(), Duration::from_secs(300), (42,)).await?;
//!
//! // Process jobs until SIGINT/SIGTERM:
//! hench.run().await?;
//! # Ok(()) }
//! ```
//!
//! Beyond jobs and workers: [`api`] reads and changes what is in Redis (queues, retries, the
//! morgue, processes) like the dashboard does; [`metrics`] holds per-class execution metrics
//! and deploy marks; [`iterable`] jobs work through long sequences and resume where they
//! stopped; [`testing`] runs jobs in tests without Redis; [`systemd`] integrates with
//! `Type=notify` services; the `cli` feature adds a worker command line and configuration
//! file (`hench::cli`).
//!
//! Four features go further, each optional per job and costing nothing to jobs that do not use
//! it:
//!
//! * [`unique`] jobs: at most one waiting or running copy of a job per digest of its class,
//!   queue and arguments ([`JobOptions::unique_for`], `#[hench::job(unique_for = ..)]`).
//! * [`periodic`] jobs (feature `periodic`, on by default): cron and interval schedules
//!   ([`Periodic`], [`HenchBuilder::periodic`]), each tick enqueued once however many workers
//!   poll.
//! * [`batch`]es and workflows: groups of jobs with `complete`, `success` and `death`
//!   callbacks ([`Batch`](batch::Batch), [`Workflow`](batch::Workflow)).
//! * [`limit`]: rate limiters used inside jobs ([`Limiter`]) and throttles on job starts
//!   ([`Throttle`], [`HenchBuilder::throttle`]).
//!
//! Each has its section in the API ([`api::Api::unique_locks`], [`api::Api::periodic`],
//! [`api::Api::batches`], [`api::Api::limits`]), the testing clients ([`testing`]), the
//! dashboard and the `hench` command.
//!
//! The repository's [DESIGN.md] (as of this release) describes the data model and the full
//! semantics.
//!
//! [DESIGN.md]: https://github.com/rkwap/hench/blob/v0.1.0/docs/DESIGN.md
#![cfg_attr(docsrs, feature(doc_cfg))]
#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]

pub mod api;
pub mod batch;
mod canonical;
#[cfg(feature = "cli")]
pub mod cli;
mod client;
mod config;
mod error;
mod flush;
pub mod iterable;
mod job;
pub mod keys;
pub mod limit;
mod lua;
pub mod metrics;
mod middleware;
mod options;
mod payload;
pub mod periodic;
mod pro;
mod redis;
#[cfg(feature = "test-util")]
pub mod test_util;
pub mod testing;
pub mod unique;

// LANE: engine — runtime modules (bodies owned by the engine lane).
pub mod fetch;
mod job_logger;
mod launcher;
mod manager;
mod processor;
mod retry;
mod scheduled;
mod stats;
pub mod systemd;

pub use crate::client::{BulkAt, BulkPush, Client, JobBuilder};
pub use crate::config::{
    BacktraceCleaner, CapsuleBuilder, CapsuleConfig, Config, DeathHandler, ErrorContext,
    ErrorHandler, FetchStrategy, Hench, HenchBuilder, LifecycleCallback, LifecycleEvent, QueueMode,
    StartupCheck,
};
pub use crate::error::{Error, JobError, Result};
pub use crate::job::{
    Data, ErasedJob, Exhausted, Job, JobContext, JobExt, JobSetter, Registry, RetryIn,
    args_from_values, args_to_values,
};
pub use crate::launcher::Launcher;
pub use crate::limit::{Limiter, OverLimit, Throttle};
pub use crate::middleware::{
    Chain, ClientChain, ClientMiddleware, ClientNext, PerformEndpoint, ServerChain, ServerEndpoint,
    ServerMiddleware, ServerNext,
};
pub use crate::options::JobOptions;
pub use crate::payload::{
    BacktraceOption, JobPayload, MAX_ERROR_MESSAGE_CHARS, RetryOption, compress_backtrace,
    decompress_backtrace, generate_jid, millis_to_system_time, now_millis, now_secs_f64,
    random_hex, secs_to_system_time, system_time_to_secs, truncate_error_message,
};
#[cfg(feature = "periodic")]
pub use crate::periodic::Periodic;
#[cfg(feature = "tls")]
pub use crate::redis::TlsConfig;
pub use crate::redis::{RedisConfig, RedisConnection, RedisPool};
pub use crate::unique::{OnConflict, PushOutcome, Unique, UniqueOption, UniqueUntil};

/// The `redis` crate hench is built on, for writing commands in middleware and jobs with the
/// same version.
pub use ::redis as redis_rs;
/// The `anyhow` crate whose error [`JobError`] wraps ([`JobError::with_anyhow`],
/// [`JobError::as_anyhow`], [`JobError::into_anyhow`]), at the version hench uses.
pub use anyhow;
/// `#[async_trait]`, needed to implement [`ServerMiddleware`] and [`ClientMiddleware`].
pub use async_trait::async_trait;
/// The `#[hench::job]` attribute: turns an `async fn` into a [`Job`].
///
/// The jobs it defines register themselves in every binary that links the crate defining them
/// ([`Registry::from_inventory`]). A worker binary that never refers to the crate holding its
/// jobs leaves it out: refer to it, for example with `use my_jobs as _;`.
#[cfg(feature = "macros")]
pub use hench_macros::job;
/// The cancellation token type used for shutdown signals ([`JobContext::stopping_token`]).
pub use tokio_util::sync::CancellationToken;

/// `tracing-subscriber` `EnvFilter` directives that honour the log level of jobs pushed with
/// [`JobOptions::log_level`]: a job at `TRACE`, `DEBUG` or `INFO` gets its events (and those of
/// middleware while it runs) logged down to that level whatever the process's level. Append
/// them to the process's filter:
///
/// ```ignore
/// let filter = EnvFilter::new(format!("warn,{}", hench::JOB_LOG_LEVEL_DIRECTIVES));
/// ```
///
/// (Levels above the process's are honoured by hench's own job events only: a filter cannot
/// silence what its other directives enable.)
pub const JOB_LOG_LEVEL_DIRECTIVES: &str =
    "[job{log_level=trace}]=trace,[job{log_level=debug}]=debug,[job{log_level=info}]=info";

/// The items most programs need: `use hench::prelude::*;`.
pub mod prelude {
    pub use crate::batch::Batch;
    pub use crate::periodic::CatchUp;
    #[cfg(feature = "periodic")]
    pub use crate::periodic::Periodic;
    pub use crate::{
        BulkPush, Client, ClientMiddleware, ClientNext, Exhausted, Hench, Job, JobContext,
        JobError, JobExt, JobOptions, JobPayload, Limiter, OnConflict, OverLimit, RedisPool,
        RetryIn, ServerMiddleware, ServerNext, Throttle, Unique, UniqueUntil, async_trait,
    };
    #[cfg(feature = "macros")]
    pub use hench_macros::job;
}

/// Implementation details used by code generated by `#[hench::job]`. Not public API.
#[doc(hidden)]
pub mod __private {
    pub use crate::job::{ErasedJob, JobRegistration};
    pub use futures;
    pub use inventory;
    pub use serde;
    pub use serde_json;

    use crate::error::JobError;

    /// The return types a `#[hench::job]` function may have: `()` or `Result<(), E>` where
    /// `E` converts into [`JobError`].
    #[diagnostic::on_unimplemented(
        message = "`{Self}` is not a valid return type for a `#[hench::job]` function",
        label = "return `()` or `Result<(), E>` where `E` converts into `hench::JobError`",
        note = "`E` can be `hench::JobError`, `anyhow::Error` (`anyhow::Result<()>`) or any \
                type implementing `std::error::Error + Send + Sync + 'static`; `String` and \
                `Box<dyn Error>` are not: convert them with `.map_err(JobError::msg)` or \
                `.map_err(JobError::from_boxed)`"
    )]
    pub trait JobReturn {
        /// Converts the function's result into a job result.
        fn into_job_result(self) -> Result<(), JobError>;
    }

    impl JobReturn for () {
        fn into_job_result(self) -> Result<(), JobError> {
            Ok(())
        }
    }

    // Not recommended: for an error type that does not convert, the compiler then reports that
    // the return type is not a `JobReturn` (with the message and note above) instead of the
    // nested `JobError: From<E>` bound, whose diagnostics wander through anyhow's conversions
    // (`String: std::error::Error`, or `dyn Error + Send + Sync` having no size).
    #[diagnostic::do_not_recommend]
    impl<E> JobReturn for Result<(), E>
    where
        JobError: From<E>,
    {
        fn into_job_result(self) -> Result<(), JobError> {
            self.map_err(JobError::from)
        }
    }

    /// Converts the result of a `#[hench::job]` function, checking its type `R`.
    pub fn into_job_result<R: JobReturn>(result: R) -> Result<(), JobError> {
        result.into_job_result()
    }
}
