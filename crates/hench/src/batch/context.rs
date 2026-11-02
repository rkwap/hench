//! The batch methods of [`Client`], [`JobContext`] and [`BatchCallback`]
//! (`docs/design/batches.md` §2.3.2, §2.7): they work with every client, reading Redis or the
//! in-memory table of a testing client.

use std::collections::{HashSet, VecDeque};

use crate::batch::batches::{BatchStatus, Batches, status_in_model};
use crate::batch::commit::settle_inline;
use crate::batch::endpoint::is_cancelled;
use crate::batch::model::capture_callbacks;
use crate::batch::{Batch, BatchCallback, BatchError, BatchMember};
use crate::client::{Client, ClientMode};
use crate::error::{Error, JobError, Result};
use crate::job::JobContext;
use crate::payload::now_millis;

impl Client {
    /// A new batch committed through this client: [`Batch::new`].
    pub fn batch(&self) -> Batch {
        Batch::new(self)
    }

    /// The status of batch `bid` (`None` when it never existed, expired or was deleted): from
    /// Redis, or from the in-memory table of a testing client.
    pub async fn batch_status(&self, bid: &str) -> Result<Option<BatchStatus>> {
        match self.mode() {
            ClientMode::Fake(_) | ClientMode::Inline(_) => {
                Ok(status_in_model(&self.tables().batches.lock(), bid))
            }
            ClientMode::Redis | ClientMode::Collect(_) => {
                Batches::new(self.clone()).fetch(bid).await
            }
        }
    }

    /// Cancels batch `bid` and its descendants (see
    /// [`Batches::cancel`](crate::batch::Batches::cancel)): in Redis, or in the in-memory table
    /// of a testing client (whose callbacks of the events this fires are captured or run at
    /// once). Returns how many batches were newly cancelled.
    pub async fn cancel_batch(&self, bid: &str) -> Result<usize> {
        match self.mode() {
            ClientMode::Fake(_) | ClientMode::Inline(_) => cancel_in_table(self, bid).await,
            ClientMode::Redis | ClientMode::Collect(_) => {
                Batches::new(self.clone()).cancel(bid).await
            }
        }
    }
}

/// Cancels `bid` and its descendants in a testing client's table.
async fn cancel_in_table(client: &Client, bid: &str) -> Result<usize> {
    let cancelled = {
        let mut model = client.tables().batches.lock();
        let now = now_millis();
        let mut queue = VecDeque::from([bid.to_owned()]);
        let mut seen = HashSet::from([bid.to_owned()]);
        let mut cancelled = 0;
        while let Some(next) = queue.pop_front() {
            let done = match model.cancel(&next, now) {
                Ok(done) => done,
                Err(Error::Batch(BatchError::NotFound { .. } | BatchError::Succeeded { .. }))
                    if next != bid =>
                {
                    continue;
                }
                Err(error) => return Err(error),
            };
            cancelled += usize::from(done.first);
            for child in done.children {
                if seen.insert(child.clone()) {
                    queue.push_back(child);
                }
            }
        }
        cancelled
    };
    match client.mode() {
        ClientMode::Inline(executor) => settle_inline(client, executor).await?,
        _ => capture_callbacks(client),
    }
    Ok(cancelled)
}

impl JobContext {
    /// A handle reopening this job's batch (no I/O; checked at its commit), to add members and
    /// child batches. `None` outside batches. A callback of a child batch is a member of the
    /// parent, so this reopens the parent: the way a step of a workflow defines the next one.
    pub fn batch(&self) -> Option<Batch> {
        let member = BatchMember::from_payload(self.payload())?;
        Batch::open(self.client(), member.bid).ok()
    }

    /// The batch and event this callback job runs for (`None` for other jobs).
    pub fn batch_callback(&self) -> Option<BatchCallback> {
        self.payload().batch_callback.clone()
    }

    /// The status of this job's batch (`None` outside batches, or once its data is gone).
    pub async fn batch_status(&self) -> std::result::Result<Option<BatchStatus>, JobError> {
        let Some(member) = BatchMember::from_payload(self.payload()) else {
            return Ok(None);
        };
        Ok(self.client().batch_status(&member.bid).await?)
    }

    /// Whether this job's batch was cancelled (one `HGET`; `false` outside batches).
    pub async fn is_batch_cancelled(&self) -> std::result::Result<bool, JobError> {
        let Some(member) = BatchMember::from_payload(self.payload()) else {
            return Ok(false);
        };
        Ok(is_cancelled(self.client(), &member.bid).await?)
    }

    /// `Err(JobError::batch_cancelled())` when this job's batch was cancelled: a long job stops
    /// early with `ctx.check_batch().await?` between its steps, and is counted as skipped.
    pub async fn check_batch(&self) -> std::result::Result<(), JobError> {
        if self.is_batch_cancelled().await? {
            Err(JobError::batch_cancelled())
        } else {
            Ok(())
        }
    }
}

impl BatchCallback {
    /// The status of the batch this callback runs for (`None` once its data is gone).
    pub async fn status(&self, ctx: &JobContext) -> Result<Option<BatchStatus>> {
        ctx.client().batch_status(self.bid()).await
    }
}
