//! Batches: groups of jobs tracked together, with callbacks when they complete, succeed or
//! die (Sidekiq Pro's batches), nesting, cancellation, and simple workflows
//! (the design document: [batches.md]).
//!
//! A [`Batch`] is defined through a collecting client ([`Batch::client`], or the client a
//! [`Batch::jobs`] closure receives): every push is buffered, and the **commit** writes the
//! batch, the registration of every member and the members themselves in one atomic script,
//! at most once per commit id (a commit whose reply was lost is sent again safely).
//!
//! Each member's outcome is counted once, however many times it runs or is reported: a success,
//! a failure that will be retried, a death (the morgue, retries disabled, a discard, a delete or
//! kill through the API), or a skip (its batch cancelled, or an operator's decision). An
//! interrupted iterable job, a shutdown requeue or a rate-limit reschedule is not an outcome.
//! Three **events** fire, each at most once: `complete` (every member ran at least once),
//! `success` (every member succeeded) and `death` (the first member that will not run again).
//! Each event pushes the callback jobs registered for it ([`Batch::on_success`], ...).
//!
//! A batch defined inside another's definition is its **child**: the child's callbacks are
//! members of the parent, so the parent's `complete` waits for the children's `complete`
//! callbacks to have run, and its `success` for theirs to have succeeded. A job can add members
//! to its own batch ([`JobContext::batch`](crate::JobContext::batch)); a callback of a child
//! reopens the parent. [`Workflow`] chains stages of parallel jobs on top of nested batches.
//!
//! Cancelling a batch ([`Batches::cancel`], [`Client::cancel_batch`](crate::Client::cancel_batch))
//! marks it and its descendants: members are skipped right before they would run, `success`
//! never fires, `complete` fires once every member left has run.
//!
//! Every key of a batch expires: 30 days after its last activity (`batch_ttl`), 24 hours once it
//! finished (`batch_linger`). [`Batches`] (`Api::batches`) lists them, with their failures,
//! children and pending members, and cancels, deletes or skips; the dashboard's Batches tab and
//! the `hench batches` command use it.
//!
//! **Rollout**: workers count members only if they understand batches (the heartbeat
//! advertises `features.batches = 1`); upgrade every worker before defining batches.
//!
//! [batches.md]: https://github.com/rkwap/hench/blob/v0.1.0/docs/design/batches.md

pub(crate) mod account;
pub(crate) mod api;
// The `Batches` API, in the API's directory (`api/batch.rs`) but compiled here, with the
// feature it reads.
#[path = "../api/batch.rs"]
pub(crate) mod batches;
pub(crate) mod collect;
pub(crate) mod commit;
mod context;
pub(crate) mod define;
pub(crate) mod dispatch;
pub(crate) mod endpoint;
pub(crate) mod lua;
pub(crate) mod maintenance;
pub(crate) mod model;
#[cfg(all(test, feature = "test-util"))]
mod parity_tests;
#[cfg(all(test, feature = "test-util"))]
mod script_tests;
pub(crate) mod scripts;
mod types;
pub(crate) mod workflow;

use crate::config::Config;
use crate::error::{Error, Result};

pub use self::batches::{
    BatchEvents, BatchSearch, BatchStatus, BatchSummary, Batches, CallbackInfo, FailedMember,
};
pub use self::define::{Batch, Committed};
pub(crate) use self::types::{Accounting, BatchMember, Report, new_bid, valid_bid};
pub use self::types::{
    BatchCallback, BatchDefaults, BatchError, BatchEvent, BatchFilter, BatchState, WORKFLOW_NEXT,
};
pub use self::workflow::{Stage, Workflow};

/// The version of the stored batch format this process understands, for the heartbeat's
/// `features` map (older workers never count batch members; the dashboard names them).
pub(crate) const FORMAT: Option<u32> = Some(1);

/// The bounds of the batch settings of a configuration (`docs/design/batches.md` §2.14):
/// `batch_ttl` from 1 hour to 365 days, `batch_linger` from 30 s to 30 days,
/// `batch_max_commit_jobs` from 1 to 100 000, `batch_commit_timeout` from 1 s to 10 minutes,
/// `batch_maintenance_interval` from 1 ms to 10 minutes, `batch_dispatch_grace` from 1 ms to 1
/// hour (short intervals for tests). `HenchBuilder::build` calls it through `pro::validate`.
pub(crate) fn validate(config: &Config) -> Result<()> {
    use std::time::Duration;
    let bounded = [
        (
            "batch_ttl",
            config.batch_ttl,
            BatchDefaults::MIN_TTL,
            define::MAX_TTL,
        ),
        (
            "batch_linger",
            config.batch_linger,
            BatchDefaults::MIN_LINGER,
            define::MAX_LINGER,
        ),
        (
            "batch_commit_timeout",
            config.batch_commit_timeout,
            Duration::from_secs(1),
            Duration::from_secs(600),
        ),
        (
            "batch_maintenance_interval",
            config.batch_maintenance_interval,
            Duration::from_millis(1),
            Duration::from_secs(600),
        ),
        (
            "batch_dispatch_grace",
            config.batch_dispatch_grace,
            Duration::from_millis(1),
            Duration::from_secs(3_600),
        ),
    ];
    for (name, value, min, max) in bounded {
        if !(min..=max).contains(&value) {
            return Err(Error::Config(format!(
                "{name} must be from {min:?} to {max:?} (got {value:?})"
            )));
        }
    }
    if !(1..=BatchDefaults::MAX_COMMIT_JOBS_LIMIT).contains(&config.batch_max_commit_jobs) {
        return Err(Error::Config(format!(
            "batch_max_commit_jobs must be from 1 to {}",
            BatchDefaults::MAX_COMMIT_JOBS_LIMIT
        )));
    }
    Ok(())
}
