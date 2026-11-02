//! Collecting clients: the clients of a batch definition, whose pushes are buffered until the
//! commit (`ClientMode::Collect`; `docs/design/batches.md` §2.3.1, §2.7).
//!
//! A definition is a root batch and the child batches defined inside it, all written by one
//! commit. Its [`Definition`] holds what they collected; each batch's collecting client pushes
//! into it with the batch's index ([`Collector`]). A push runs the client's whole pipeline
//! (normalization, `bid`, client middleware) and returns its jid at once; nothing is written
//! before the root's commit.

use std::collections::HashSet;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crate::batch::BatchError;
use crate::batch::define::Staged;
use crate::client::{Client, PushResult};
use crate::error::{Error, Result};
use crate::payload::JobPayload;
use crate::unique::{OnConflict, UniqueOption};

/// What a child batch counts for against `batch_max_commit_jobs`: its share of the commit
/// (about 25 commands, its callback templates) and, when it has no member, its dispatch right
/// after the commit cost about as much as ten members. The cap bounds how long one commit blocks
/// Redis.
pub(crate) const CHILD_WEIGHT: usize = 10;

/// The definition of a root batch and its children, shared by their handles and collecting
/// clients.
pub(crate) struct Definition {
    /// The client the commit goes through (Redis or a testing client; never a collecting one).
    pub(crate) client: Client,
    state: Mutex<DefinitionState>,
}

/// What a definition collected.
#[derive(Debug, Default)]
pub(crate) struct DefinitionState {
    /// The root's commit started (or the root was dropped): no more pushes or children.
    pub(crate) closed: bool,
    /// The batches: the root first, then the children in the order they were defined (a
    /// parent always comes before its children).
    pub(crate) slots: Vec<Slot>,
    /// The members, in push order.
    pub(crate) members: Vec<Member>,
    /// The jids pushed (unique within a definition).
    jids: HashSet<String>,
}

/// One batch of a definition.
#[derive(Debug)]
pub(crate) struct Slot {
    /// Its bid.
    pub(crate) bid: String,
    /// The index of its parent's slot (`None` for the root).
    pub(crate) parent: Option<usize>,
    /// Where its definition stands.
    pub(crate) state: SlotState,
}

/// Where the definition of one batch stands.
#[derive(Debug)]
pub(crate) enum SlotState {
    /// Being defined: its collecting client takes pushes.
    Open,
    /// A child committed into the root's commit (its settings and callback templates).
    Staged(Box<Staged>),
    /// Dropped without a commit (or its closure failed): written with nothing.
    Discarded,
}

/// One buffered member.
#[derive(Debug, Clone)]
pub(crate) struct Member {
    /// The index of its batch's slot.
    pub(crate) slot: usize,
    /// The job as it left client middleware (`at` set only when scheduled in the future).
    pub(crate) job: JobPayload,
    /// Its conflict strategy, for a unique job (the stored JSON drops it).
    pub(crate) on_conflict: OnConflict,
}

impl Definition {
    /// A definition committed through `client`, whose root is `bid`.
    pub(crate) fn new(client: Client, bid: String) -> Arc<Self> {
        let state = DefinitionState {
            slots: vec![Slot {
                bid,
                parent: None,
                state: SlotState::Open,
            }],
            ..DefinitionState::default()
        };
        Arc::new(Self {
            client,
            state: Mutex::new(state),
        })
    }

    /// The state, locked (never across an `.await`).
    pub(crate) fn lock(&self) -> MutexGuard<'_, DefinitionState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The most members one commit of this definition may hold, each child batch counting as
    /// [`CHILD_WEIGHT`] members.
    pub(crate) fn max_jobs(&self) -> usize {
        self.client.batch_defaults().max_commit_jobs
    }
}

impl fmt::Debug for Definition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Definition")
            .field("client", &self.client)
            .finish_non_exhaustive()
    }
}

impl DefinitionState {
    /// Adds a child of slot `parent` with bid `bid`; its index, or `None` once the definition
    /// is closed or its parent is no longer being defined.
    pub(crate) fn add_child(&mut self, parent: usize, bid: String) -> Option<usize> {
        let open = !self.closed
            && self
                .slots
                .get(parent)
                .is_some_and(|slot| matches!(slot.state, SlotState::Open));
        open.then(|| {
            self.slots.push(Slot {
                bid,
                parent: Some(parent),
                state: SlotState::Open,
            });
            self.slots.len() - 1
        })
    }

    /// The size of the commit so far, in members: the members, plus [`CHILD_WEIGHT`] for each
    /// child batch not discarded.
    pub(crate) fn weight(&self) -> usize {
        let children = self
            .slots
            .iter()
            .skip(1)
            .filter(|slot| !matches!(slot.state, SlotState::Discarded))
            .count();
        self.members
            .len()
            .saturating_add(children.saturating_mul(CHILD_WEIGHT))
    }

    /// [`BatchError::TooLarge`] when the commit so far, plus `more` members, is over `limit`.
    pub(crate) fn check_size(&self, more: usize, limit: usize) -> Result<()> {
        let total = self.weight().saturating_add(more);
        if total > limit {
            return Err(Error::Batch(BatchError::TooLarge { jobs: total, limit }));
        }
        Ok(())
    }

    /// Whether slot `slot` still takes pushes and children.
    pub(crate) fn is_open(&self, slot: usize) -> bool {
        !self.closed
            && self
                .slots
                .get(slot)
                .is_some_and(|slot| matches!(slot.state, SlotState::Open))
    }

    /// Discards slot `slot` and its descendants: they and their members are not written.
    pub(crate) fn discard(&mut self, slot: usize) {
        let mut discarded = HashSet::from([slot]);
        // Children come after their parents: one pass finds every descendant.
        for (index, child) in self.slots.iter().enumerate() {
            if child
                .parent
                .is_some_and(|parent| discarded.contains(&parent))
            {
                discarded.insert(index);
            }
        }
        for index in &discarded {
            if let Some(slot) = self.slots.get_mut(*index) {
                slot.state = SlotState::Discarded;
            }
        }
        let jids = &mut self.jids;
        self.members.retain(|member| {
            let keep = !discarded.contains(&member.slot);
            if !keep {
                jids.remove(&member.job.jid);
            }
            keep
        });
    }

    /// The slots below `slot` (its descendants), in definition order.
    pub(crate) fn descendants(&self, slot: usize) -> Vec<usize> {
        let mut below = HashSet::from([slot]);
        let mut found = Vec::new();
        for (index, child) in self.slots.iter().enumerate() {
            if child.parent.is_some_and(|parent| below.contains(&parent)) {
                below.insert(index);
                found.push(index);
            }
        }
        found
    }
}

/// The collector of one batch of a definition: what a collecting client pushes into.
#[derive(Clone)]
pub(crate) struct Collector {
    /// The definition.
    pub(crate) definition: Arc<Definition>,
    /// The index of the batch's slot.
    pub(crate) slot: usize,
    /// The batch the pushes join.
    pub(crate) bid: Arc<str>,
}

impl fmt::Debug for Collector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Collector")
            .field("bid", &self.bid)
            .field("slot", &self.slot)
            .finish_non_exhaustive()
    }
}

/// The collecting client of slot `slot` of `definition`: it shares the committing client's
/// pool, middleware, default job options, unique switch, batch settings and testing tables, and
/// buffers its pushes.
pub(crate) fn collecting_client(definition: &Arc<Definition>, slot: usize, bid: &str) -> Client {
    definition.client.collecting(Collector {
        definition: Arc::clone(definition),
        slot,
        bid: Arc::from(bid),
    })
}

/// The batch step of normalization for a collecting client: sets `bid` (refusing a job that
/// names another batch) and refuses `OnConflict::Replace` (replacing a holder that belongs to
/// another batch would strand that batch).
pub(crate) fn normalize(collector: &Collector, job: &mut JobPayload) -> Result<()> {
    if let Some(bid) = &job.bid
        && bid.as_str() != &*collector.bid
    {
        return Err(Error::InvalidJob(format!(
            "job {} names batch {bid}, but it is pushed in the definition of batch {}: a job \
             joins the batch whose client pushes it",
            job.jid, collector.bid
        )));
    }
    if on_conflict(job) == OnConflict::Replace {
        return Err(Error::InvalidJob(
            "a batch member cannot replace another job (OnConflict::Replace): the job it \
             replaces may belong to another batch, which would then wait for it forever; when \
             the job type declares `on_conflict = \"replace\"`, push the member with \
             `J::set().on_conflict(OnConflict::Drop)` (or `.on_conflict(OnConflict::Error)`)"
                .to_owned(),
        ));
    }
    job.bid = Some(collector.bid.to_string());
    Ok(())
}

/// The conflict strategy of a unique job (`Drop` for other jobs).
fn on_conflict(job: &JobPayload) -> OnConflict {
    job.unique
        .as_ref()
        .and_then(UniqueOption::settings)
        .and_then(|unique| unique.on_conflict)
        .unwrap_or_default()
}

/// Buffers `jobs` (out of client middleware) for the commit: each is reported pushed, since
/// duplicates of unique jobs are only known at the commit (`Committed::duplicates`). Fails with
/// [`BatchError::Closed`] once the batch is committed or dropped, [`BatchError::DuplicateJid`]
/// for a jid pushed twice in one definition and [`BatchError::TooLarge`] beyond
/// `batch_max_commit_jobs` (each child batch counting as [`CHILD_WEIGHT`] members); a failing
/// push buffers none of `jobs`.
pub(crate) fn append(collector: &Collector, jobs: Vec<JobPayload>) -> Result<Vec<PushResult>> {
    let limit = collector.definition.max_jobs();
    let mut state = collector.definition.lock();
    if !state.is_open(collector.slot) {
        return Err(Error::Batch(BatchError::Closed));
    }
    state.check_size(jobs.len(), limit)?;
    let mut seen = HashSet::new();
    for job in &jobs {
        if state.jids.contains(&job.jid) || !seen.insert(job.jid.as_str()) {
            return Err(Error::Batch(BatchError::DuplicateJid {
                jid: job.jid.clone(),
            }));
        }
    }
    let results = vec![PushResult::Pushed; jobs.len()];
    for job in jobs {
        state.jids.insert(job.jid.clone());
        let on_conflict = on_conflict(&job);
        state.members.push(Member {
            slot: collector.slot,
            job,
            on_conflict,
        });
    }
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(slot: usize, jid: &str) -> Member {
        let mut job = JobPayload::new("A", vec![]);
        job.jid = jid.to_owned();
        Member {
            slot,
            job,
            on_conflict: OnConflict::Drop,
        }
    }

    #[test]
    fn discarding_a_child_drops_its_subtree() {
        let definition = Definition::new(Client::fake(), "r".to_owned());
        let mut state = definition.lock();
        let child = state.add_child(0, "c".to_owned()).unwrap();
        let grandchild = state.add_child(child, "g".to_owned()).unwrap();
        let sibling = state.add_child(0, "s".to_owned()).unwrap();
        for (slot, jid) in [(0, "a"), (child, "b"), (grandchild, "c"), (sibling, "d")] {
            state.jids.insert(jid.to_owned());
            state.members.push(member(slot, jid));
        }
        assert_eq!(state.descendants(0), [child, grandchild, sibling]);
        assert_eq!(state.descendants(child), [grandchild]);
        state.discard(child);
        let left: Vec<&str> = state.members.iter().map(|m| m.job.jid.as_str()).collect();
        assert_eq!(left, ["a", "d"]);
        assert!(!state.jids.contains("b") && state.jids.contains("d"));
        assert!(!state.is_open(grandchild));
        assert!(state.is_open(sibling));
        // A closed definition takes no child.
        state.closed = true;
        assert_eq!(state.add_child(0, "x".to_owned()), None);
        assert!(!state.is_open(0));
    }
}
