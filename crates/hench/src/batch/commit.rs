//! The commit of a batch definition (`docs/design/batches.md` §2.5.3): the plan of what a
//! definition holds, its encoding for `BATCH_COMMIT` and the sending loop (resent until a
//! definite reply), and the same commit for the testing clients, applied to their in-memory
//! table (`docs/design/batches.md` §2.11).

use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use futures::StreamExt;
use tokio::time::Instant;

use crate::batch::BatchError;
use crate::batch::Committed;
use crate::batch::collect::{Definition, DefinitionState, SlotState};
use crate::batch::dispatch::{dispatch_chain, refresh_ancestors};
use crate::batch::model::{ModelCommit, ModelJob, capture_callbacks};
use crate::batch::scripts::{
    self, CommitBatch, CommitJob, CommitMode, CommitReply, UniqueMember, commit_refusal,
};
use crate::client::{Client, ClientMode, PushResult, Stored};
use crate::error::{Error, Result};
use crate::payload::{JobPayload, now_millis, now_secs_f64};
use crate::pro::{Classified, Fetched};
use crate::redis::{RedisPool, RetryAction, retry_action};
use crate::testing::InlineExecutor;
use crate::unique::{OnConflict, UniqueOption};

/// Longest `unique_for` of a unique member, in seconds (as for every unique push).
const MAX_UNIQUE_FOR_SECS: u64 = 1_000_000_000;

/// First and largest pause between two sends of a commit whose outcome is unknown.
const MIN_RESEND_PAUSE: Duration = Duration::from_millis(100);
const MAX_RESEND_PAUSE: Duration = Duration::from_secs(2);

/// Most dispatches of the batches one commit fired that run at the same time.
const DISPATCH_CONCURRENCY: usize = 16;

/// The root batch of a commit, as its handle resolved it.
pub(crate) struct Root {
    /// Created or reopened.
    pub(crate) mode: CommitMode,
    /// Its bid.
    pub(crate) bid: String,
    /// Expiry after the last activity, seconds.
    pub(crate) ttl_secs: u64,
    /// Linger once finished, seconds.
    pub(crate) linger_secs: u64,
    /// Description (`""`: none).
    pub(crate) description: String,
    /// The callback templates of `complete`, `success` and `death`.
    pub(crate) callbacks: [String; 3],
    /// More fields of the hash.
    pub(crate) extra: Vec<(String, String)>,
}

/// What a commit writes.
struct Plan {
    /// The batches, the root first (a parent before its children).
    batches: Vec<CommitBatch>,
    /// The members, in push order.
    members: Vec<Planned>,
    /// The new children.
    children: Vec<String>,
}

/// One member of a plan.
struct Planned {
    /// The 1-based index of its batch.
    batch: usize,
    /// The job as it left client middleware, with its stored `unique` field.
    job: JobPayload,
    /// Its lock, for a unique member.
    unique: Option<UniqueMember>,
}

impl Plan {
    /// The plan of a closed definition whose root is `root`. A child neither committed nor
    /// dropped is an error.
    fn of(state: &DefinitionState, root: Root) -> Result<Self> {
        let mut batches = vec![CommitBatch {
            mode: root.mode,
            bid: root.bid,
            parent: 0,
            ttl_secs: root.ttl_secs,
            linger_secs: root.linger_secs,
            description: root.description,
            callbacks: root.callbacks,
            extra: root.extra,
        }];
        let mut position: HashMap<usize, usize> = HashMap::from([(0, 1)]);
        let mut children = Vec::new();
        for (index, slot) in state.slots.iter().enumerate().skip(1) {
            match &slot.state {
                SlotState::Discarded => {}
                SlotState::Open => {
                    return Err(Error::InvalidJob(format!(
                        "child batch {} was defined but neither committed nor dropped",
                        slot.bid
                    )));
                }
                SlotState::Staged(staged) => {
                    let parent = slot
                        .parent
                        .and_then(|parent| position.get(&parent))
                        .copied()
                        .ok_or_else(|| {
                            Error::InvalidJob(format!(
                                "child batch {} lost its parent's definition",
                                slot.bid
                            ))
                        })?;
                    batches.push(CommitBatch {
                        mode: CommitMode::New,
                        bid: slot.bid.clone(),
                        parent,
                        ttl_secs: staged.ttl_secs,
                        linger_secs: staged.linger_secs,
                        description: staged.description.clone(),
                        callbacks: staged.callbacks.clone(),
                        extra: staged.extra.clone(),
                    });
                    position.insert(index, batches.len());
                    children.push(slot.bid.clone());
                }
            }
        }
        let mut bids = HashSet::new();
        if let Some(repeated) = batches
            .iter()
            .find(|batch| !bids.insert(batch.bid.as_str()))
        {
            return Err(Error::InvalidJob(format!(
                "batch {} is defined twice in one definition",
                repeated.bid
            )));
        }
        let mut members = Vec::with_capacity(state.members.len());
        for member in &state.members {
            let Some(batch) = position.get(&member.slot).copied() else {
                continue;
            };
            let mut job = member.job.clone();
            let unique = stored_unique(&mut job, member.on_conflict)?;
            members.push(Planned { batch, job, unique });
        }
        Ok(Self {
            batches,
            members,
            children,
        })
    }

    fn root(&self) -> &CommitBatch {
        &self.batches[0]
    }
}

/// Turns the `unique` field of a member into its stored form (`for`, `until`, `digest`; the
/// push-time `on_conflict`, `key` and `across_queues` dropped), and returns the lock it takes. A
/// member whose uniqueness is disabled, or whose digest middleware removed, is stored without
/// the field and takes no lock (as `UNIQUE_PUSH` does).
fn stored_unique(job: &mut JobPayload, on_conflict: OnConflict) -> Result<Option<UniqueMember>> {
    let Some(UniqueOption::Enabled(mut unique)) = job.unique.take() else {
        return Ok(None);
    };
    let Some(digest) = unique.digest.clone() else {
        return Ok(None);
    };
    if !valid_digest(&digest) {
        return Err(Error::InvalidJob(format!(
            "the digest of a unique job must be 1 to 128 characters of A-Z, a-z, 0-9, `_` and \
             `-`, not {digest:?}"
        )));
    }
    let secs = unique.ttl.map_or(0, |ttl| ttl.as_secs());
    if secs == 0 || secs >= MAX_UNIQUE_FOR_SECS {
        return Err(Error::InvalidJob(
            "a unique job needs a `unique_for` of at least one second and below 1000000000 \
             seconds"
                .to_owned(),
        ));
    }
    let until = unique.until.unwrap_or_default();
    unique.until = Some(until);
    unique.on_conflict = None;
    unique.key = None;
    unique.across_queues = None;
    job.unique = Some(UniqueOption::Enabled(unique));
    Ok(Some(UniqueMember {
        digest,
        class: job.class.clone(),
        queue: job.queue.clone(),
        until,
        for_ms: secs.saturating_mul(1000),
        on_conflict,
    }))
}

/// The characters a unique digest may hold, and its bounds (as for every unique push).
fn valid_digest(digest: &str) -> bool {
    (1..=128).contains(&digest.len())
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

/// Commits the definition whose root is `root` through `client` (Redis, or a testing client's
/// table), as commit `commit_id`. Closes the definition first: later pushes fail with
/// [`BatchError::Closed`].
pub(crate) async fn commit(
    client: &Client,
    definition: &Arc<Definition>,
    root: Root,
    commit_id: &str,
) -> Result<Committed> {
    let limit = definition.max_jobs();
    let plan = {
        let mut state = definition.lock();
        state.closed = true;
        state.check_size(0, limit)?;
        Plan::of(&state, root)?
    };
    let reopen = plan.root().mode == CommitMode::Open;
    let committed = match client.mode() {
        ClientMode::Redis => commit_redis(client, plan, commit_id).await?,
        ClientMode::Fake(_) | ClientMode::Inline(_) => {
            commit_testing(client, plan, commit_id).await?
        }
        ClientMode::Collect(_) => {
            return Err(Error::InvalidJob(
                "a root batch commits through a client that is not collecting".to_owned(),
            ));
        }
    };
    tracing::info!(
        bid = %committed.bid,
        members = committed.jids.len(),
        duplicates = committed.duplicates.len(),
        children = committed.children.len(),
        reopen,
        already_applied = committed.already_applied,
        "batch committed"
    );
    Ok(committed)
}

/// The commit through Redis: `BATCH_COMMIT`, resent until a definite reply, then the dispatch
/// of the new batches that fired at once (empty ones).
async fn commit_redis(client: &Client, plan: Plan, commit_id: &str) -> Result<Committed> {
    let now = now_secs_f64();
    let now_ms = now_millis();
    let jobs = plan
        .members
        .iter()
        .map(|member| {
            Ok(CommitJob {
                batch: member.batch,
                jid: member.job.jid.clone(),
                stored: Stored::encode(member.job.clone(), now, now_ms)?,
                unique: member.unique.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let (keys, argv) = scripts::commit_call(commit_id, &plan.batches, &jobs, Utc::now());
    let redis = client.redis();
    let root = plan.root();
    let timeout = client.batch_defaults().commit_timeout;
    let (reply, resent) = send(redis, &keys, &argv, &jobs, timeout, &root.bid, commit_id).await?;
    let (duplicates, fired) = match reply {
        CommitReply::Applied {
            fired,
            duplicates,
            unlocked,
        } => {
            for (jid, error) in unlocked {
                tracing::warn!(
                    bid = %root.bid,
                    jid = %jid,
                    %error,
                    "a unique batch member was pushed without its lock"
                );
            }
            (duplicates, fired)
        }
        CommitReply::Already { duplicates } if resent => {
            // An earlier send of this commit applied it: which batches fired is not known, so
            // each one is dispatched if it waits in the outbox (deepest first).
            let fired = plan
                .batches
                .iter()
                .rev()
                .map(|batch| batch.bid.clone())
                .collect();
            (duplicates, fired)
        }
        CommitReply::Already { .. } => {
            return Ok(Committed {
                bid: root.bid.clone(),
                jids: Vec::new(),
                duplicates: Vec::new(),
                children: Vec::new(),
                already_applied: true,
                value: (),
            });
        }
    };
    dispatch_fired(redis, fired).await;
    if root.mode == CommitMode::Open {
        refresh_ancestors(redis, &root.bid).await;
    }
    Ok(committed(&plan, duplicates))
}

/// Dispatches the batches a commit fired (empty new batches, a reopened batch whose hold it
/// released), a few at a time: siblings update their parent through atomic scripts, in any
/// order. Failures are logged; maintenance dispatches what is left after the grace period.
async fn dispatch_fired(redis: &RedisPool, fired: Vec<String>) {
    futures::stream::iter(fired)
        .for_each_concurrent(DISPATCH_CONCURRENCY, |bid| async move {
            if let Err(error) = dispatch_chain(redis, &bid).await {
                tracing::warn!(
                    %error,
                    bid = %bid,
                    "could not dispatch the callbacks of a new batch; maintenance will"
                );
            }
        })
        .await;
}

/// What a commit that wrote `plan` reports, `duplicates` left out.
fn committed(plan: &Plan, duplicates: Vec<(String, String)>) -> Committed {
    let left_out: HashSet<&str> = duplicates.iter().map(|(jid, _)| jid.as_str()).collect();
    Committed {
        bid: plan.root().bid.clone(),
        jids: plan
            .members
            .iter()
            .map(|member| member.job.jid.as_str())
            .filter(|jid| !left_out.contains(jid))
            .map(str::to_owned)
            .collect(),
        duplicates: duplicates.into_iter().map(|(jid, _)| jid).collect(),
        children: plan.children.clone(),
        already_applied: false,
        value: (),
    }
}

/// Sends `BATCH_COMMIT` until a definite reply. A reply, a refusal (nothing was written for
/// it) and an error before anything could reach Redis are definite; a timeout, or a connection
/// lost after the request left, may have applied the commit, so it is sent again (the commit id
/// makes the repeat a no-op), with backoff, until `timeout` passes
/// ([`BatchError::Unconfirmed`]). A failover's refusal (`READONLY`) is sent again on the new
/// primary. Returns the reply, and whether an earlier send may have applied the commit (an
/// `already` reply then means that it did).
async fn send(
    redis: &RedisPool,
    keys: &[String],
    argv: &[String],
    jobs: &[CommitJob],
    timeout: Duration,
    bid: &str,
    commit_id: &str,
) -> Result<(CommitReply, bool)> {
    let deadline = Instant::now() + timeout;
    let mut pause = MIN_RESEND_PAUSE;
    let mut maybe_applied = false;
    loop {
        let error = match redis.conn().await {
            Ok(mut conn) => match scripts::commit_once(&mut conn, keys, argv).await {
                Ok(reply) => return Ok((scripts::parse_commit(reply)?, maybe_applied)),
                Err(error) => {
                    if let Some(refusal) = commit_refusal(&error, jobs) {
                        return Err(refusal);
                    }
                    match sent(&error) {
                        Sent::Definite => return Err(Error::Redis(error)),
                        Sent::Unknown => maybe_applied = true,
                        Sent::Failover => redis.reconnect(),
                        Sent::Never if !maybe_applied => return Err(Error::Redis(error)),
                        Sent::Never => {}
                    }
                    Error::Redis(error)
                }
            },
            // No connection: nothing was sent this time.
            Err(error) if !maybe_applied => return Err(error),
            Err(error) => error,
        };
        let now = Instant::now();
        if now >= deadline {
            return Err(if maybe_applied {
                Error::Batch(BatchError::Unconfirmed {
                    bid: bid.to_owned(),
                    commit_id: commit_id.to_owned(),
                })
            } else {
                error
            });
        }
        tracing::warn!(
            %error,
            bid,
            commit_id,
            "a batch commit did not get a definite reply; sending it again"
        );
        tokio::time::sleep(pause.min(deadline - now)).await;
        pause = (pause * 2).min(MAX_RESEND_PAUSE);
    }
}

/// What an error of one send says about the commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Sent {
    /// Refused by Redis (out of memory, a key of the wrong type, ...): not applied.
    Definite,
    /// Lost after the request left (a timeout, a dropped connection): maybe applied.
    Unknown,
    /// A replica refused it during a failover: not applied; worth sending to the new primary.
    Failover,
    /// Never sent (the connection was refused).
    Never,
}

fn sent(error: &redis::RedisError) -> Sent {
    if retry_action(error) == Some(RetryAction::Reconnect) {
        Sent::Failover
    } else if error.is_connection_refusal() {
        Sent::Never
    } else if error.is_timeout() || error.is_connection_dropped() || error.is_io_error() {
        Sent::Unknown
    } else {
        Sent::Definite
    }
}

/// The commit through a testing client: the model's commit applied to its table (with the
/// unique table's admission of unique members), then the members captured (fake) or run
/// (inline, with the callbacks of the events they fire).
async fn commit_testing(client: &Client, plan: Plan, commit_id: &str) -> Result<Committed> {
    let root = plan.root().bid.clone();
    let now = now_millis();
    let duplicates = {
        let mut model = client.tables().batches.lock();
        if let Some(batch) = model.batches.get(&root)
            && batch.commits.contains_key(commit_id)
        {
            return Ok(Committed {
                bid: root,
                jids: Vec::new(),
                duplicates: Vec::new(),
                children: Vec::new(),
                already_applied: true,
                value: (),
            });
        }
        model.check_commit(&plan.batches)?;
        let duplicates = admit(client, &plan.members)?;
        let jobs: Vec<ModelJob> = plan
            .members
            .iter()
            .map(|member| ModelJob {
                batch: member.batch,
                jid: member.job.jid.clone(),
            })
            .collect();
        match model.commit(commit_id, &plan.batches, &jobs, &duplicates, now)? {
            ModelCommit::Applied { .. } => duplicates,
            ModelCommit::Already { duplicates } => duplicates,
        }
    };
    let left_out: HashSet<&str> = duplicates.iter().map(|(jid, _)| jid.as_str()).collect();
    let written: Vec<JobPayload> = plan
        .members
        .iter()
        .filter(|member| !left_out.contains(member.job.jid.as_str()))
        .map(|member| member.job.clone())
        .collect();
    match client.mode() {
        ClientMode::Fake(store) => {
            crate::testing::fake_push(store, written).await?;
            capture_callbacks(client);
        }
        ClientMode::Inline(executor) => run_inline(client, executor, written).await?,
        ClientMode::Redis | ClientMode::Collect(_) => {}
    }
    Ok(committed(&plan, duplicates))
}

/// The unique table's admission of the unique members of a testing commit (the rules of
/// `BATCH_COMMIT`: a member whose lock another job holds, whose own jid holds it, or whose
/// digest an earlier member took, is left out; with `OnConflict::Error` the commit is refused
/// and the locks it took are given back). Returns the duplicates, `(jid, holder)`.
fn admit(client: &Client, members: &[Planned]) -> Result<Vec<(String, String)>> {
    let unique: Vec<&Planned> = members.iter().filter(|m| m.unique.is_some()).collect();
    if unique.is_empty() {
        return Ok(Vec::new());
    }
    let payloads: Vec<JobPayload> = unique.iter().map(|member| member.job.clone()).collect();
    let results = crate::unique::fake::admit(client, &payloads);
    let mut duplicates = Vec::new();
    let mut taken = Vec::new();
    let mut refused = None;
    for (member, result) in unique.iter().zip(results) {
        let holder = match result {
            PushResult::AlreadyPushed => member.job.jid.clone(),
            PushResult::Duplicate { holder } => holder,
            _ => {
                taken.push(*member);
                continue;
            }
        };
        if let Some(lock) = &member.unique
            && lock.on_conflict == OnConflict::Error
            && refused.is_none()
        {
            refused = Some(Error::DuplicateJob {
                class: lock.class.clone(),
                digest: lock.digest.clone(),
                holder: holder.clone(),
            });
        }
        duplicates.push((member.job.jid.clone(), holder));
    }
    if let Some(error) = refused {
        for member in taken {
            crate::unique::fake::after(
                client,
                &Fetched::from_payload(&member.job),
                &Classified::Skipped,
            );
        }
        return Err(error);
    }
    Ok(duplicates)
}

tokio::task_local! {
    /// The members of the inline commits made by jobs that an outer inline commit of this task
    /// runs (a workflow's next stage, a step adding to its batch): that outer commit runs them,
    /// right after the job that committed them, instead of the commit running them inside the
    /// job. Running them inside nested every stage of a workflow in the one before it, and a
    /// few stages were enough to overflow the stack.
    static DEFERRED: RefCell<VecDeque<(Client, JobPayload)>>;
}

/// Runs the members of an inline commit, one after the other, each followed by the callbacks
/// of the events it fired (`settle_inline`). A failing member stops the commit: it was counted
/// dead, its batch's `death` callbacks run, and its error is returned. A skipped member (its
/// batch cancelled) is not a failure.
///
/// A commit made by a job that an inline commit of this task is running only queues its
/// members: the outer commit runs them next (before its own remaining members), in a loop, so
/// that the depth of the stack does not grow with the number of stages or steps.
async fn run_inline(
    client: &Client,
    executor: &InlineExecutor,
    members: Vec<JobPayload>,
) -> Result<()> {
    let mut members = Some(members);
    let queued = DEFERRED.try_with(|deferred| {
        let mut deferred = deferred.borrow_mut();
        for job in members.take().into_iter().flatten() {
            deferred.push_back((client.clone(), job));
        }
    });
    match (queued, members) {
        (Ok(()), _) | (Err(_), None) => Ok(()),
        (Err(_), Some(members)) => {
            DEFERRED
                .scope(
                    RefCell::new(VecDeque::new()),
                    drain_inline(client, executor, members),
                )
                .await
        }
    }
}

/// The loop of the outermost inline commit of a task: its members, and those of the commits
/// they make, each followed by the callbacks it fired.
async fn drain_inline(
    client: &Client,
    executor: &InlineExecutor,
    members: Vec<JobPayload>,
) -> Result<()> {
    settle_inline(client, executor).await?;
    let mut queue: VecDeque<(Client, JobPayload)> = taken_deferred();
    queue.extend(members.into_iter().map(|job| (client.clone(), job)));
    while let Some((owner, job)) = queue.pop_front() {
        let ClientMode::Inline(executor) = owner.mode() else {
            continue;
        };
        let (bid, jid) = (job.bid.clone(), job.jid.clone());
        match crate::testing::inline_push(executor, &owner, vec![job]).await {
            Ok(()) => {}
            Err(Error::Job(error))
                if error.is_batch_cancelled() && skipped(&owner, bid.as_deref(), &jid) => {}
            Err(error) => {
                if let Err(callback) = settle_inline(&owner, executor).await {
                    tracing::warn!(error = %callback, "a death callback of an inline batch failed");
                }
                return Err(error);
            }
        }
        settle_inline(&owner, executor).await?;
        // The members this one (or the callbacks it fired) committed run next, in order.
        let mut deferred = taken_deferred();
        deferred.append(&mut queue);
        queue = deferred;
    }
    Ok(())
}

/// The members queued for the current inline commit's loop (none outside one).
fn taken_deferred() -> VecDeque<(Client, JobPayload)> {
    DEFERRED
        .try_with(|deferred| std::mem::take(&mut *deferred.borrow_mut()))
        .unwrap_or_default()
}

/// Runs the callbacks of every event waiting in an inline client's table, until none is left
/// (a callback may fire its parent's events).
pub(crate) async fn settle_inline(client: &Client, executor: &InlineExecutor) -> Result<()> {
    loop {
        let callbacks = client.tables().batches.lock().dispatch_all(now_millis());
        if callbacks.is_empty() {
            return Ok(());
        }
        for job in callbacks {
            let (bid, jid) = (job.bid.clone(), job.jid.clone());
            match crate::testing::inline_push(executor, client, vec![job]).await {
                Ok(()) => {}
                Err(Error::Job(error))
                    if error.is_batch_cancelled() && skipped(client, bid.as_deref(), &jid) => {}
                Err(error) => return Err(error),
            }
        }
    }
}

/// Whether member `jid` of batch `bid` was skipped (its batch cancelled, the member no longer
/// pending) in a testing client's table.
fn skipped(client: &Client, bid: Option<&str>, jid: &str) -> bool {
    let Some(bid) = bid else {
        return false;
    };
    client
        .tables()
        .batches
        .lock()
        .batches
        .get(bid)
        .is_some_and(|batch| batch.cancelled_at.is_some() && !batch.pending.contains(jid))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::collect::{Member, Slot};
    use crate::batch::define::Staged;
    use crate::unique::Unique;

    fn root() -> Root {
        Root {
            mode: CommitMode::New,
            bid: "r".to_owned(),
            ttl_secs: 3_600,
            linger_secs: 60,
            description: String::new(),
            callbacks: Default::default(),
            extra: Vec::new(),
        }
    }

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
    fn plans_keep_staged_children_and_refuse_open_ones() {
        let staged = |bid: &str, parent| Slot {
            bid: bid.to_owned(),
            parent: Some(parent),
            state: SlotState::Staged(Box::new(Staged {
                description: String::new(),
                ttl_secs: 3_600,
                linger_secs: 60,
                callbacks: Default::default(),
                extra: Vec::new(),
            })),
        };
        let mut state = DefinitionState::default();
        state.slots = vec![
            Slot {
                bid: "r".to_owned(),
                parent: None,
                state: SlotState::Open,
            },
            Slot {
                bid: "gone".to_owned(),
                parent: Some(0),
                state: SlotState::Discarded,
            },
            staged("c", 0),
            staged("g", 2),
        ];
        state.members = vec![member(0, "a"), member(2, "b"), member(3, "c")];
        let plan = Plan::of(&state, root()).unwrap();
        let bids: Vec<&str> = plan.batches.iter().map(|b| b.bid.as_str()).collect();
        assert_eq!(bids, ["r", "c", "g"]);
        // Parents are 1-based positions in the plan.
        assert_eq!(plan.batches[2].parent, 2);
        assert_eq!(plan.children, ["c", "g"]);
        let batches: Vec<usize> = plan.members.iter().map(|m| m.batch).collect();
        assert_eq!(batches, [1, 2, 3]);
        state.slots.push(Slot {
            bid: "open".to_owned(),
            parent: Some(0),
            state: SlotState::Open,
        });
        assert!(matches!(
            Plan::of(&state, root()),
            Err(Error::InvalidJob(_))
        ));
    }

    #[test]
    fn unique_members_are_stored_without_their_push_time_fields() {
        let mut job = JobPayload::new("A", vec![]);
        let mut unique = Unique::new(Duration::from_secs(60))
            .on_conflict(OnConflict::Error)
            .key(serde_json::json!(1));
        unique.digest = Some("d1".to_owned());
        job.unique = Some(unique.into());
        let lock = stored_unique(&mut job, OnConflict::Error).unwrap().unwrap();
        assert_eq!(lock.for_ms, 60_000);
        assert_eq!(lock.on_conflict, OnConflict::Error);
        let stored = serde_json::to_value(&job).unwrap();
        assert_eq!(
            stored["unique"],
            serde_json::json!({"for": 60, "until": "success", "digest": "d1"})
        );
        // Without a digest: stored without the field, no lock.
        let mut plain = JobPayload::new("A", vec![]);
        plain.unique = Some(Unique::new(Duration::from_secs(60)).into());
        assert_eq!(stored_unique(&mut plain, OnConflict::Drop).unwrap(), None);
        assert_eq!(plain.unique, None);
    }
}
