//! The types of batches: events, the `batch_callback` payload field, the errors, the client
//! defaults, the API's state enums, and the crate-internal member and accounting handles
//! (`docs/design/batches.md` §2.3, §2.10).

use std::fmt;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::payload::{JobPayload, random_hex};

/// The internal job that runs the next step of a workflow (`hench::batch::Workflow`). The
/// name is reserved: registering a job under it is an
/// [`Error::Config`](crate::Error::Config).
pub const WORKFLOW_NEXT: &str = "hench::WorkflowNext";

/// A batch event, for which callbacks run ([batches.md] §2.2.4).
///
/// [batches.md]: https://github.com/rkwap/hench/blob/v0.1.0/docs/design/batches.md
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum BatchEvent {
    /// Every member ran once (succeeded, or failed at least once), and every child completed.
    Complete,
    /// Every member succeeded (or was skipped), and every child succeeded.
    Success,
    /// A member died (first death only).
    Death,
}

impl BatchEvent {
    /// The event's name (`complete`, `success`, `death`).
    pub fn as_str(self) -> &'static str {
        match self {
            BatchEvent::Complete => "complete",
            BatchEvent::Success => "success",
            BatchEvent::Death => "death",
        }
    }

    /// The event named `name`.
    pub fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "complete" => BatchEvent::Complete,
            "success" => BatchEvent::Success,
            "death" => BatchEvent::Death,
            _ => return None,
        })
    }
}

impl fmt::Display for BatchEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The `batch_callback` field of a callback job: the batch and event it runs for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchCallback {
    bid: String,
    event: BatchEvent,
}

impl BatchCallback {
    /// The callback of `event` for batch `bid`.
    pub(crate) fn new(bid: impl Into<String>, event: BatchEvent) -> Self {
        Self {
            bid: bid.into(),
            event,
        }
    }

    /// The batch whose event it is.
    pub fn bid(&self) -> &str {
        &self.bid
    }

    /// The event.
    pub fn event(&self) -> BatchEvent {
        self.event
    }
}

/// Why a batch operation was refused (`hench::Error::Batch`).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum BatchError {
    /// The batch does not exist (never did, expired or deleted).
    #[error("batch {bid} does not exist")]
    NotFound {
        /// The batch.
        bid: String,
    },
    /// The batch succeeded: it cannot be reopened or cancelled.
    #[error("batch {bid} already succeeded")]
    Succeeded {
        /// The batch.
        bid: String,
    },
    /// The batch was cancelled: it cannot be reopened.
    #[error("batch {bid} was cancelled")]
    Cancelled {
        /// The batch.
        bid: String,
    },
    /// `Batch::with_bid` of a bid another commit already used.
    #[error("batch {bid} already exists")]
    Exists {
        /// The batch.
        bid: String,
    },
    /// Not a batch id (24 lowercase hexadecimal characters).
    #[error("`{bid}` is not a batch id (24 lowercase hexadecimal characters)")]
    InvalidBid {
        /// The text given.
        bid: String,
    },
    /// More jobs than `batch_max_commit_jobs` in one commit, each child batch counting as 10
    /// jobs.
    #[error(
        "a batch commit of {jobs} jobs is over the limit of {limit} (each child batch counts as \
         10 jobs; use loader jobs)"
    )]
    TooLarge {
        /// Jobs in the commit, each child batch counting as 10.
        jobs: usize,
        /// The limit.
        limit: usize,
    },
    /// A stage of a workflow too large for the commit that starts it: its jobs plus its batch,
    /// which counts as 10, over `batch_max_commit_jobs` (checked for every stage by
    /// [`Workflow::start`](crate::batch::Workflow::start)).
    #[error(
        "stage {stage} of the workflow has {jobs} jobs: with its batch, which counts as 10 jobs, \
         that is over the limit of {limit} jobs per batch commit (batch_max_commit_jobs); give \
         the stage a loader job that pushes the rest, or raise the limit"
    )]
    StageTooLarge {
        /// The stage (1 for the first).
        stage: usize,
        /// Its jobs.
        jobs: usize,
        /// The limit.
        limit: usize,
    },
    /// A push through a collecting client after its commit.
    #[error("this batch was already committed; push through a new definition")]
    Closed,
    /// The same jid twice in one definition.
    #[error("jid {jid} appears twice in one batch definition")]
    DuplicateJid {
        /// The jid.
        jid: String,
    },
    /// Deleting an unfinished child of a live parent (without `force`).
    #[error("batch {bid} is an unfinished child of {parent}, which could never finish")]
    DeleteChild {
        /// The child.
        bid: String,
        /// Its parent.
        parent: String,
    },
    /// The batch was written by a newer version of hench, whose format this one does not read.
    #[error("batch {bid} was written by a newer hench (format {version}); upgrade to read it")]
    Unsupported {
        /// The batch.
        bid: String,
        /// The format it was written in.
        version: u32,
    },
    /// No definite reply to a commit within `batch_commit_timeout`.
    #[error("the commit {commit_id} of batch {bid} is unconfirmed: Redis did not answer in time")]
    Unconfirmed {
        /// The batch.
        bid: String,
        /// The commit id (commit again with it to find out).
        commit_id: String,
    },
}

/// The batch settings a client applies to the batches it defines (`HenchBuilder::batch_*`,
/// `Client::with_batch_defaults`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct BatchDefaults {
    /// Expiry of an unfinished batch after its last activity (default 30 days; at least 1 hour).
    pub ttl: Duration,
    /// How long a finished batch's data stays (default 24 hours; at least 30 seconds).
    pub linger: Duration,
    /// Most members of one commit, each child batch counting as 10 (default 10 000; 1 to
    /// 100 000): the bound of how long one commit blocks Redis.
    pub max_commit_jobs: usize,
    /// How long a commit whose outcome is unknown is sent again (default 30 seconds).
    pub commit_timeout: Duration,
}

impl BatchDefaults {
    /// Default [`ttl`](Self::ttl).
    pub const TTL: Duration = Duration::from_secs(30 * 24 * 60 * 60);
    /// Shortest [`ttl`](Self::ttl).
    pub const MIN_TTL: Duration = Duration::from_secs(60 * 60);
    /// Default [`linger`](Self::linger).
    pub const LINGER: Duration = Duration::from_secs(24 * 60 * 60);
    /// Shortest [`linger`](Self::linger).
    pub const MIN_LINGER: Duration = Duration::from_secs(30);
    /// Default [`max_commit_jobs`](Self::max_commit_jobs).
    pub const MAX_COMMIT_JOBS: usize = 10_000;
    /// Largest [`max_commit_jobs`](Self::max_commit_jobs).
    pub const MAX_COMMIT_JOBS_LIMIT: usize = 100_000;
    /// Default [`commit_timeout`](Self::commit_timeout).
    pub const COMMIT_TIMEOUT: Duration = Duration::from_secs(30);
}

impl Default for BatchDefaults {
    fn default() -> Self {
        Self {
            ttl: Self::TTL,
            linger: Self::LINGER,
            max_commit_jobs: Self::MAX_COMMIT_JOBS,
            commit_timeout: Self::COMMIT_TIMEOUT,
        }
    }
}

/// A batch's state for display: the first that applies of `Succeeded`, `Cancelled`, `Dead`
/// (a member died and the batch did not succeed), `Complete`, `Pending`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum BatchState {
    /// Members are still pending.
    Pending,
    /// `complete` fired.
    Complete,
    /// A member died.
    Dead,
    /// Cancelled.
    Cancelled,
    /// `success` fired.
    Succeeded,
}

/// Which batches a listing shows (`Batches::page`).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum BatchFilter {
    /// Every batch.
    All,
    /// Batches still pending.
    Active,
    /// Batches with a dead member.
    Dead,
    /// Batches whose `complete` fired.
    Complete,
    /// Batches that succeeded.
    Succeeded,
    /// Cancelled batches.
    Cancelled,
    /// Batches whose bid or description contains the text (`*` matches anything).
    Text(String),
}

/// `true` for a batch id: 24 lowercase hexadecimal characters.
pub(crate) fn valid_bid(bid: &str) -> bool {
    bid.len() == 24
        && bid
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

/// A new batch id (24 random lowercase hexadecimal characters, like a jid).
pub(crate) fn new_bid() -> String {
    random_hex(12)
}

/// The batch a fetched job is a member of, from the job as fetched (crate-internal; the
/// processor passes it on without looking inside).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BatchMember {
    /// The batch.
    pub(crate) bid: String,
    /// The member's jid.
    pub(crate) jid: String,
}

impl BatchMember {
    /// The membership of `job` (as fetched): `None` without a `bid`, or with one that is not a
    /// batch id (another system's payload: logged, never accounted).
    pub(crate) fn from_payload(job: &JobPayload) -> Option<Self> {
        let bid = job.bid.as_deref()?;
        if !valid_bid(bid) {
            warn_invalid_bid(bid, &job.jid);
            return None;
        }
        Some(Self {
            bid: bid.to_owned(),
            jid: job.jid.clone(),
        })
    }
}

/// Most invalid bids remembered, to warn once per bid (beyond, at debug).
const WARNED_BIDS: usize = 1_000;

/// Logs a job whose `bid` is not a batch id (another system's payload, or a corrupted one): a
/// warning once per bid and process, then at debug.
fn warn_invalid_bid(bid: &str, jid: &str) {
    static WARNED: std::sync::Mutex<Option<std::collections::HashSet<String>>> =
        std::sync::Mutex::new(None);
    let first = {
        let mut warned = WARNED
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let warned = warned.get_or_insert_with(std::collections::HashSet::new);
        warned.len() < WARNED_BIDS && warned.insert(bid.to_owned())
    };
    if first {
        tracing::warn!(
            jid,
            bid,
            "a job's `bid` is not a batch id (24 lowercase hexadecimal characters); it is not \
             counted in any batch"
        );
    } else {
        tracing::debug!(jid, "a job's `bid` is not a batch id; not a batch member");
    }
}

/// What an accounting script recorded, for the dispatch after the outcome write
/// (crate-internal).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Report {
    /// The batch accounted.
    pub(crate) bid: String,
    /// Whether events wait in the outbox (dispatch them now).
    pub(crate) dispatch: bool,
    /// Whether the batch's expiry was refreshed (refresh its ancestors' too).
    pub(crate) refreshed: bool,
}

/// The result of `batch::account::before_outcome`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Accounting {
    /// Nothing to account (not a member, or an outcome that is not a batch outcome: interrupted,
    /// shutdown, over limit, throttled).
    Nothing,
    /// Recorded; dispatch after the outcome write.
    Recorded(Report),
    /// The job said its batch was cancelled, but the batch is not: the processor handles the
    /// error as a failure instead (and accounts that).
    NotCancelled,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn events_and_callbacks_round_trip() {
        for event in [BatchEvent::Complete, BatchEvent::Success, BatchEvent::Death] {
            assert_eq!(BatchEvent::parse(event.as_str()), Some(event));
            assert_eq!(serde_json::to_value(event).unwrap(), json!(event.as_str()));
        }
        let callback = BatchCallback::new("0123456789abcdef01234567", BatchEvent::Success);
        let value = serde_json::to_value(&callback).unwrap();
        assert_eq!(
            value,
            json!({"bid": "0123456789abcdef01234567", "event": "success"})
        );
        assert_eq!(
            serde_json::from_value::<BatchCallback>(value).unwrap(),
            callback
        );
        assert_eq!(callback.bid(), "0123456789abcdef01234567");
        assert_eq!(callback.event(), BatchEvent::Success);
    }

    #[test]
    fn bids_are_24_lowercase_hex_characters() {
        let bid = new_bid();
        assert!(valid_bid(&bid), "{bid}");
        assert!(!valid_bid("0123456789ABCDEF01234567"));
        assert!(!valid_bid("0123456789abcdef0123456"));
        assert!(!valid_bid(""));
        let mut job = JobPayload::new("Member", vec![]);
        job.jid = "j".repeat(24);
        assert_eq!(BatchMember::from_payload(&job), None);
        job.bid = Some("not-a-bid".to_owned());
        assert_eq!(BatchMember::from_payload(&job), None);
        job.bid = Some(bid.clone());
        let member = BatchMember::from_payload(&job).unwrap();
        assert_eq!((member.bid.as_str(), member.jid.len()), (bid.as_str(), 24));
    }

    #[test]
    fn errors_and_defaults() {
        assert_eq!(
            BatchError::TooLarge {
                jobs: 12,
                limit: 10
            }
            .to_string(),
            "a batch commit of 12 jobs is over the limit of 10 (each child batch counts as 10 \
             jobs; use loader jobs)"
        );
        let defaults = BatchDefaults::default();
        assert_eq!(defaults.ttl, Duration::from_secs(2_592_000));
        assert_eq!(defaults.linger, Duration::from_secs(86_400));
        assert_eq!(defaults.max_commit_jobs, 10_000);
        assert_eq!(defaults.commit_timeout, Duration::from_secs(30));
        assert_eq!(WORKFLOW_NEXT, "hench::WorkflowNext");
    }
}
