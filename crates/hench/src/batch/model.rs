//! Batches in memory: the state machine of the batch scripts, transition for transition
//! (`docs/design/batches.md` §2.11), used by the testing modes (a [`BatchTable`] per testing
//! client, shared by its clones and its collecting clients) and checked against the scripts by
//! a property test.
//!
//! The model keeps what the scripts keep, except expiries: the testing modes have no time-driven
//! behaviour, so a batch lives as long as its table (its `finished_at` still marks the end of
//! its life in Redis).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, MutexGuard, PoisonError};

use crate::batch::account::{Reason, failure, summary};
use crate::batch::scripts::{Accounted, CommitBatch, CommitMode, Outcome, Template};
use crate::batch::{BatchError, BatchEvent};
use crate::client::{Client, ClientMode};
use crate::error::{Error, JobError, Result};
use crate::payload::{JobPayload, now_millis};
use crate::pro::{Classified, Fetched};

/// One batch, as its keys hold it.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct ModelBatch {
    /// `created_at` (ms).
    pub(crate) created_at: i64,
    /// `desc`.
    pub(crate) description: Option<String>,
    /// `parent`.
    pub(crate) parent: Option<String>,
    /// `ttl` (s).
    pub(crate) ttl_secs: u64,
    /// `linger` (s).
    pub(crate) linger_secs: u64,
    /// `total`.
    pub(crate) total: u64,
    /// `skipped`.
    pub(crate) skipped: u64,
    /// `kids`: children.
    pub(crate) kids: u64,
    /// `kc`: children whose `complete` was dispatched.
    pub(crate) kc: u64,
    /// `ks`: children whose `success` was dispatched.
    pub(crate) ks: u64,
    /// `:pending`.
    pub(crate) pending: BTreeSet<String>,
    /// `:failed`.
    pub(crate) failed: BTreeMap<String, String>,
    /// `:dead`.
    pub(crate) dead: BTreeSet<String>,
    /// `:kids`.
    pub(crate) children: BTreeSet<String>,
    /// `:commits`: commit id → the duplicates it left out.
    pub(crate) commits: BTreeMap<String, Vec<(String, String)>>,
    /// `complete_at` (ms).
    pub(crate) complete_at: Option<i64>,
    /// `success_at` (ms).
    pub(crate) success_at: Option<i64>,
    /// `death_at` (ms).
    pub(crate) death_at: Option<i64>,
    /// `complete_dt` (ms).
    pub(crate) complete_dt: Option<i64>,
    /// `success_dt` (ms).
    pub(crate) success_dt: Option<i64>,
    /// `death_dt` (ms).
    pub(crate) death_dt: Option<i64>,
    /// `cancelled_at` (ms).
    pub(crate) cancelled_at: Option<i64>,
    /// `finished_at` (ms).
    pub(crate) finished_at: Option<i64>,
    /// The callback templates of `complete`, `success` and `death`.
    pub(crate) callbacks: [Vec<Template>; 3],
    /// More fields (the workflow's `wf`).
    pub(crate) extra: BTreeMap<String, String>,
}

/// The field of a batch held open (`open` > 0: a workflow whose later stages are not defined
/// yet): its `complete` and `success` cannot fire.
pub(crate) const HOLD: &str = "open";

impl ModelBatch {
    /// Whether the batch is held open (`hb_evaluate` reads `open` as a number).
    pub(crate) fn held(&self) -> bool {
        self.extra
            .get(HOLD)
            .and_then(|value| value.parse::<f64>().ok())
            .is_some_and(|value| value > 0.0)
    }
}

/// The index of an event's templates in [`ModelBatch::callbacks`] (and in
/// [`CommitBatch::callbacks`]).
pub(crate) fn event_index(event: BatchEvent) -> usize {
    match event {
        BatchEvent::Complete => 0,
        BatchEvent::Success => 1,
        BatchEvent::Death => 2,
    }
}

/// Every batch of a store, and the outbox.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct Model {
    /// The batches, by bid.
    pub(crate) batches: BTreeMap<String, ModelBatch>,
    /// The outbox: bid → when its oldest undispatched event fired (ms).
    pub(crate) outbox: BTreeMap<String, i64>,
}

/// What a commit did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ModelCommit {
    /// Written now: the new batches that fired at once, deepest first.
    Applied {
        /// Their bids.
        fired: Vec<String>,
    },
    /// The commit id had been applied, with these duplicates.
    Already {
        /// `(jid, holder)`.
        duplicates: Vec<(String, String)>,
    },
}

/// One member of a commit, for the model: its 1-based batch index and jid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ModelJob {
    /// The batch.
    pub(crate) batch: usize,
    /// The jid.
    pub(crate) jid: String,
}

/// A callback pushed by a dispatch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Pushed {
    /// The event.
    pub(crate) event: BatchEvent,
    /// The template.
    pub(crate) template: Template,
}

/// What a dispatch did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ModelDispatch {
    /// The batch was in the outbox.
    pub(crate) dispatched: bool,
    /// The callbacks pushed, in order.
    pub(crate) pushed: Vec<Pushed>,
    /// The parent, when it exists.
    pub(crate) parent: Option<String>,
    /// The events fired in the parent.
    pub(crate) parent_fired: Vec<String>,
    /// The parent has events waiting in the outbox.
    pub(crate) parent_dispatch: bool,
}

/// What a cancel did to one batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ModelCancel {
    /// The first cancel.
    pub(crate) first: bool,
    /// The batch has events waiting in the outbox.
    pub(crate) dispatch: bool,
    /// Its children.
    pub(crate) children: Vec<String>,
}

impl Model {
    /// `BATCH_COMMIT`: `batches` (the root first) and the members `jobs`, minus `duplicates`
    /// (unique members left out: `(jid, holder)`), as commit `cid` at `now`.
    pub(crate) fn commit(
        &mut self,
        cid: &str,
        batches: &[CommitBatch],
        jobs: &[ModelJob],
        duplicates: &[(String, String)],
        now: i64,
    ) -> Result<ModelCommit> {
        let Some(root) = batches.first() else {
            return Err(Error::InvalidJob("a batch commit needs a batch".to_owned()));
        };
        if let Some(recorded) = self
            .batches
            .get(&root.bid)
            .and_then(|batch| batch.commits.get(cid))
        {
            return Ok(ModelCommit::Already {
                duplicates: recorded.clone(),
            });
        }
        self.check_commit(batches)?;
        // Writes: new batches (a reopened one takes its extra fields), then the children joining
        // their parents.
        for batch in batches {
            if batch.mode != CommitMode::New {
                if let Some(existing) = self.batches.get_mut(&batch.bid) {
                    existing.extra.extend(batch.extra.iter().cloned());
                }
                continue;
            }
            let parent = batch
                .parent
                .checked_sub(1)
                .and_then(|index| batches.get(index))
                .map(|parent| parent.bid.clone());
            let mut callbacks: [Vec<Template>; 3] = Default::default();
            for (slot, raw) in callbacks.iter_mut().zip(&batch.callbacks) {
                *slot = crate::batch::scripts::parse_templates(Some(raw))?;
            }
            self.batches.insert(
                batch.bid.clone(),
                ModelBatch {
                    created_at: now,
                    description: (!batch.description.is_empty()).then(|| batch.description.clone()),
                    parent,
                    ttl_secs: batch.ttl_secs,
                    linger_secs: batch.linger_secs,
                    callbacks,
                    extra: batch.extra.iter().cloned().collect(),
                    ..ModelBatch::default()
                },
            );
        }
        for batch in batches {
            if batch.mode == CommitMode::New
                && let Some(parent_bid) = batch
                    .parent
                    .checked_sub(1)
                    .and_then(|index| batches.get(index))
                    .map(|parent| parent.bid.clone())
                && let Some(parent) = self.batches.get_mut(&parent_bid)
            {
                parent.children.insert(batch.bid.clone());
                parent.kids += 1;
            }
        }
        // The members.
        let excluded: BTreeSet<&str> = duplicates.iter().map(|(jid, _)| jid.as_str()).collect();
        for job in jobs {
            if excluded.contains(job.jid.as_str()) {
                continue;
            }
            let Some(batch) = job
                .batch
                .checked_sub(1)
                .and_then(|index| batches.get(index))
                .and_then(|batch| self.batches.get_mut(&batch.bid))
            else {
                continue;
            };
            if batch.pending.insert(job.jid.clone()) {
                batch.total += 1;
            }
        }
        if let Some(root) = self.batches.get_mut(&root.bid) {
            root.commits.insert(cid.to_owned(), duplicates.to_vec());
        }
        // New batches without member or child fire at once, deepest first; a reopened batch may
        // fire when the commit released its hold.
        let mut fired = Vec::new();
        for batch in batches.iter().rev() {
            if !self.evaluate(&batch.bid, now).is_empty() {
                fired.push(batch.bid.clone());
            }
        }
        Ok(ModelCommit::Applied { fired })
    }

    /// The checks of a commit, before any write.
    pub(crate) fn check_commit(&self, batches: &[CommitBatch]) -> Result<()> {
        for (index, batch) in batches.iter().enumerate() {
            match batch.mode {
                CommitMode::Open => {
                    let Some(existing) = self.batches.get(&batch.bid) else {
                        return Err(Error::Batch(BatchError::NotFound {
                            bid: batch.bid.clone(),
                        }));
                    };
                    if existing.success_at.is_some() {
                        return Err(Error::Batch(BatchError::Succeeded {
                            bid: batch.bid.clone(),
                        }));
                    }
                    if existing.cancelled_at.is_some() {
                        return Err(Error::Batch(BatchError::Cancelled {
                            bid: batch.bid.clone(),
                        }));
                    }
                    if batch.parent != 0 {
                        return Err(Error::InvalidJob(
                            "a reopened batch has no parent index".to_owned(),
                        ));
                    }
                }
                CommitMode::New => {
                    if self.batches.contains_key(&batch.bid) {
                        return Err(Error::Batch(BatchError::Exists {
                            bid: batch.bid.clone(),
                        }));
                    }
                    if batch.parent > index {
                        return Err(Error::InvalidJob(
                            "the parent of a batch must come before it".to_owned(),
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// `hb_evaluate`: fires `complete`, then `success`, when their conditions hold (neither
    /// while the batch is held open).
    pub(crate) fn evaluate(&mut self, bid: &str, now: i64) -> Vec<String> {
        let Some(batch) = self.batches.get_mut(bid) else {
            return Vec::new();
        };
        if batch.held() {
            return Vec::new();
        }
        let mut fired = Vec::new();
        let pending = batch.pending.len();
        if batch.complete_at.is_none() && pending == batch.failed.len() && batch.kc >= batch.kids {
            batch.complete_at = Some(now);
            fired.push("complete".to_owned());
        }
        if batch.success_at.is_none()
            && batch.cancelled_at.is_none()
            && pending == 0
            && batch.ks >= batch.kids
        {
            batch.success_at = Some(now);
            fired.push("success".to_owned());
        }
        if !fired.is_empty() {
            self.outbox.entry(bid.to_owned()).or_insert(now);
        }
        fired
    }

    /// The accounting scripts: records `outcome` for member `jid` of batch `bid` at `now`.
    pub(crate) fn account(
        &mut self,
        bid: &str,
        jid: &str,
        outcome: &Outcome,
        now: i64,
    ) -> Accounted {
        let mut accounted = Accounted::default();
        match outcome {
            Outcome::Succeeded => {
                if let Some(batch) = self.batches.get_mut(bid)
                    && batch.pending.remove(jid)
                {
                    batch.failed.remove(jid);
                    batch.dead.remove(jid);
                    accounted.applied = true;
                    accounted.fired = self.evaluate(bid, now);
                    self.finish_cancelled(bid, now);
                }
            }
            Outcome::Failed { retry, summary } => {
                if let Some(batch) = self.batches.get_mut(bid)
                    && batch.pending.contains(jid)
                {
                    batch.failed.insert(jid.to_owned(), summary.clone());
                    let mut died = false;
                    if *retry {
                        batch.dead.remove(jid);
                    } else {
                        batch.dead.insert(jid.to_owned());
                        if batch.death_at.is_none() {
                            batch.death_at = Some(now);
                            died = true;
                        }
                    }
                    if died {
                        self.outbox.entry(bid.to_owned()).or_insert(now);
                        accounted.fired.push("death".to_owned());
                    }
                    accounted.applied = true;
                    accounted.fired.extend(self.evaluate(bid, now));
                }
            }
            Outcome::Skipped { cancelled } => {
                let batch_cancelled = self
                    .batches
                    .get(bid)
                    .is_some_and(|batch| batch.cancelled_at.is_some());
                if *cancelled && !batch_cancelled {
                    accounted.not_cancelled = true;
                    return accounted;
                }
                if let Some(batch) = self.batches.get_mut(bid)
                    && batch.pending.remove(jid)
                {
                    batch.failed.remove(jid);
                    batch.dead.remove(jid);
                    batch.skipped += 1;
                    accounted.applied = true;
                    accounted.fired = self.evaluate(bid, now);
                    self.finish_cancelled(bid, now);
                }
            }
        }
        accounted.dispatch = self.outbox.contains_key(bid);
        accounted
    }

    /// `BATCH_DISPATCH`: pushes the callbacks of the events `bid` fired and did not dispatch
    /// yet (death, complete, success), registers them in the parent and updates it.
    pub(crate) fn dispatch(&mut self, bid: &str, now: i64) -> ModelDispatch {
        let mut result = ModelDispatch::default();
        if self.outbox.remove(bid).is_none() {
            return result;
        }
        result.dispatched = true;
        let Some(batch) = self.batches.get(bid).cloned() else {
            return result;
        };
        let parent = batch
            .parent
            .clone()
            .filter(|parent| self.batches.contains_key(parent));
        let due = [BatchEvent::Death, BatchEvent::Complete, BatchEvent::Success]
            .into_iter()
            .filter(|event| match event {
                BatchEvent::Death => batch.death_at.is_some() && batch.death_dt.is_none(),
                BatchEvent::Complete => batch.complete_at.is_some() && batch.complete_dt.is_none(),
                _ => batch.success_at.is_some() && batch.success_dt.is_none(),
            });
        let mut registered = 0;
        for event in due {
            for template in &batch.callbacks[event_index(event)] {
                result.pushed.push(Pushed {
                    event,
                    template: template.clone(),
                });
                if let Some(parent) = parent
                    .as_ref()
                    .and_then(|parent| self.batches.get_mut(parent))
                    && parent.pending.insert(template.jid.clone())
                {
                    registered += 1;
                }
            }
            if let Some(parent_bid) = &parent {
                let mut died = false;
                if let Some(parent) = self.batches.get_mut(parent_bid) {
                    match event {
                        BatchEvent::Complete => parent.kc += 1,
                        BatchEvent::Success => parent.ks += 1,
                        _ => {
                            if parent.death_at.is_none() {
                                parent.death_at = Some(now);
                                died = true;
                            }
                        }
                    }
                }
                if died {
                    self.outbox.entry(parent_bid.clone()).or_insert(now);
                    result.parent_fired.push("death".to_owned());
                }
            }
            if let Some(batch) = self.batches.get_mut(bid) {
                match event {
                    BatchEvent::Complete => batch.complete_dt = Some(now),
                    BatchEvent::Success => batch.success_dt = Some(now),
                    _ => batch.death_dt = Some(now),
                }
                if event == BatchEvent::Success {
                    batch.finished_at = Some(now);
                }
            }
            if event == BatchEvent::Complete {
                self.finish_cancelled(bid, now);
            }
        }
        if let Some(parent_bid) = &parent {
            if let Some(parent) = self.batches.get_mut(parent_bid) {
                parent.total += registered;
            }
            result.parent_fired.extend(self.evaluate(parent_bid, now));
            result.parent_dispatch = self.outbox.contains_key(parent_bid);
        }
        result.parent = parent;
        result
    }

    /// `BATCH_CANCEL` of one batch.
    pub(crate) fn cancel(&mut self, bid: &str, now: i64) -> Result<ModelCancel> {
        let Some(batch) = self.batches.get_mut(bid) else {
            return Err(Error::Batch(BatchError::NotFound {
                bid: bid.to_owned(),
            }));
        };
        if batch.success_at.is_some() {
            return Err(Error::Batch(BatchError::Succeeded {
                bid: bid.to_owned(),
            }));
        }
        let first = batch.cancelled_at.is_none();
        if first {
            batch.cancelled_at = Some(now);
        }
        batch.extra.remove(HOLD);
        let children = batch.children.iter().cloned().collect();
        self.evaluate(bid, now);
        self.finish_cancelled(bid, now);
        Ok(ModelCancel {
            first,
            dispatch: self.outbox.contains_key(bid),
            children,
        })
    }

    /// `hb_finish_cancelled`: a cancelled batch whose `complete` was dispatched finishes once
    /// no member is left pending.
    fn finish_cancelled(&mut self, bid: &str, now: i64) {
        if let Some(batch) = self.batches.get_mut(bid)
            && batch.cancelled_at.is_some()
            && batch.complete_dt.is_some()
            && batch.finished_at.is_none()
            && batch.pending.is_empty()
        {
            batch.finished_at = Some(now);
        }
    }

    /// `BATCH_DELETE` of one batch: the number of keys removed.
    #[allow(
        dead_code,
        reason = "the model of BATCH_DELETE, checked by the parity tests"
    )]
    pub(crate) fn delete(&mut self, bid: &str, force: bool) -> Result<u64> {
        let Some(batch) = self.batches.get(bid) else {
            self.outbox.remove(bid);
            return Ok(0);
        };
        if !force
            && batch.finished_at.is_none()
            && let Some(parent) = &batch.parent
            && self
                .batches
                .get(parent)
                .is_some_and(|parent| parent.finished_at.is_none())
        {
            return Err(Error::Batch(BatchError::DeleteChild {
                bid: bid.to_owned(),
                parent: parent.clone(),
            }));
        }
        let removed = 1
            + u64::from(!batch.pending.is_empty())
            + u64::from(!batch.failed.is_empty())
            + u64::from(!batch.dead.is_empty())
            + u64::from(!batch.children.is_empty())
            + u64::from(!batch.commits.is_empty());
        self.batches.remove(bid);
        self.outbox.remove(bid);
        Ok(removed)
    }

    /// Dispatches every event waiting in the outbox, until none is left (a dispatch may fire
    /// events in the parent); returns the callback jobs pushed, in order, with `enqueued_at`.
    pub(crate) fn dispatch_all(&mut self, now: i64) -> Vec<JobPayload> {
        let mut jobs = Vec::new();
        while let Some(bid) = self.outbox.keys().next().cloned() {
            for pushed in self.dispatch(&bid, now).pushed {
                match JobPayload::from_json(&pushed.template.j) {
                    Ok(mut job) => {
                        job.enqueued_at = Some(now);
                        jobs.push(job);
                    }
                    Err(error) => tracing::warn!(
                        %error,
                        bid = %bid,
                        "a batch callback template is not a job; it is not pushed"
                    ),
                }
            }
        }
        jobs
    }
}

/// The in-memory batches of a testing client, its clones and its collecting clients.
#[derive(Debug, Default)]
pub(crate) struct BatchTable {
    model: Mutex<Model>,
}

impl BatchTable {
    /// The model, locked (never across an `.await`).
    pub(crate) fn lock(&self) -> MutexGuard<'_, Model> {
        self.model.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Accounts the outcome of a member the testing executor ran, before the unique table releases
/// its lock (the processor's order). Testing clients do not retry, so a failure is a death. A
/// `batch_cancelled` error from a member whose batch is not cancelled is a failure, as in
/// workers. A fake client captures the callbacks of the events this fires right away; an
/// inline client's commit runs them.
pub(crate) fn account(client: &Client, fetched: &Fetched, classified: &Classified) {
    let Some(member) = &fetched.member else {
        return;
    };
    let outcome = match classified {
        Classified::Success => Outcome::Succeeded,
        Classified::Skipped => Outcome::Skipped { cancelled: true },
        Classified::Failure(verdict) => failure(verdict, now_millis()),
        _ => return,
    };
    let now = now_millis();
    {
        let mut model = client.tables().batches.lock();
        let accounted = model.account(&member.bid, &member.jid, &outcome, now);
        if accounted.not_cancelled {
            // The class is unknown here: the summary names the error alone.
            let error = JobError::batch_cancelled();
            let summary = summary(
                &JobPayload::new("", Vec::new()),
                Reason::NoRetry,
                Some(&error),
                None,
                now,
            );
            model.account(
                &member.bid,
                &member.jid,
                &Outcome::Failed {
                    retry: false,
                    summary,
                },
                now,
            );
        }
    }
    capture_callbacks(client);
}

/// A fake client captures the callbacks of the events waiting in its table (an inline client's
/// commit runs them itself).
pub(crate) fn capture_callbacks(client: &Client) {
    let ClientMode::Fake(store) = client.mode() else {
        return;
    };
    let jobs = client.tables().batches.lock().dispatch_all(now_millis());
    if jobs.is_empty() {
        return;
    }
    // `fake_push` never waits: it completes on its first poll.
    if let Err(error) = futures::executor::block_on(crate::testing::fake_push(store, jobs)) {
        tracing::warn!(%error, "could not capture the callbacks of a batch");
    }
}

/// Whether batch `bid` of a testing client's table was cancelled.
pub(crate) fn is_cancelled(client: &Client, bid: &str) -> bool {
    client
        .tables()
        .batches
        .lock()
        .batches
        .get(bid)
        .is_some_and(|batch| batch.cancelled_at.is_some())
}
