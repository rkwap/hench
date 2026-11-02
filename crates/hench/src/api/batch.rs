//! [`Batches`]: the batches of a Redis, for dashboards and admin tools
//! (`docs/design/batches.md` §2.10): their status, failures, children and pending members, and
//! the operator's actions (cancel, delete, skip a member, dispatch now, wait).
//!
//! Batches are listed from the `batches` index, scored by the expiry of their keys (the most
//! recently active first); an expired batch stays in the index until maintenance or a reader
//! trims it, and is never listed.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::time::{Duration, SystemTime};

use chrono::Utc;
use serde::Deserialize;
use serde_json::Value;

use crate::api::events::{self, EventKind};
use crate::api::{Api, JobRecord, Page};
use crate::batch::dispatch::{dispatch_chain, settle};
use crate::batch::model::{Model, ModelBatch};
use crate::batch::scripts::{self, Outcome, Template, parse_templates};
use crate::batch::{BatchCallback, BatchError, BatchEvent, BatchFilter, BatchState, Report};
use crate::error::{Error, Result};
use crate::keys;
use crate::metrics::Period;
use crate::payload::{JobPayload, millis_to_system_time};
use crate::redis::RedisPool;

/// The stored format this version reads (`v`); a batch written by a newer one is refused.
const FORMAT: u32 = 1;

/// Index entries a filtered listing looks at, at most.
const SEARCH_WINDOW: usize = 50_000;

/// Matches a filtered listing keeps, at most.
const MAX_MATCHES: usize = 500;

/// Index entries read per round trip.
const INDEX_CHUNK: usize = 1_000;

/// The largest page served.
const MAX_PAGE_SIZE: usize = 1_000;

/// Failure summaries read for one listing, at most.
const MAX_FAILURES: usize = 1_000;

/// Children listed, at most.
const MAX_CHILDREN: usize = 1_000;

/// Batches of a tree cancelled, deleted or read per round trip (one script or command each,
/// pipelined).
const TREE_PIPELINE: usize = 500;

/// The hash fields a listing reads.
const SUMMARY_FIELDS: [&str; 16] = [
    "v",
    "created_at",
    "desc",
    "parent",
    "total",
    "skipped",
    "kids",
    "kc",
    "ks",
    "complete_at",
    "success_at",
    "death_at",
    "cancelled_at",
    "finished_at",
    "ttl",
    "linger",
];

/// The batches of a Redis ([`Api::batches`]). Cheap to clone. Redis only: the testing clients
/// read their batches with [`Client::batch_status`](crate::Client::batch_status).
#[derive(Clone)]
pub struct Batches {
    api: Api,
}

impl fmt::Debug for Batches {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Batches").finish_non_exhaustive()
    }
}

impl Api {
    /// The batches of this Redis.
    pub fn batches(&self) -> Batches {
        Batches { api: self.clone() }
    }
}

impl JobRecord {
    /// The `batch_callback` of a callback job: the batch and event it runs for (`None` for
    /// other jobs, and when the field is malformed).
    pub fn batch_callback(&self) -> Option<BatchCallback> {
        serde_json::from_value(self.get("batch_callback")?.clone()).ok()
    }
}

/// A batch's counts and times, as a listing shows them.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct BatchSummary {
    /// The batch id.
    pub bid: String,
    /// Its description.
    pub description: Option<String>,
    /// Its parent, for a child batch.
    pub parent: Option<String>,
    /// When it was created.
    pub created_at: SystemTime,
    /// When its keys expire (`None`: no expiry, which should not happen).
    pub expires_at: Option<SystemTime>,
    /// Members registered.
    pub total: u64,
    /// Members not yet succeeded or skipped.
    pub pending: u64,
    /// Members that failed and have not succeeded since.
    pub failures: u64,
    /// Members that will not run again on their own.
    pub dead: u64,
    /// Members skipped (their batch cancelled, or an operator's decision).
    pub skipped: u64,
    /// Child batches.
    pub children: u64,
    /// When `complete` fired.
    pub complete_at: Option<SystemTime>,
    /// When `success` fired.
    pub success_at: Option<SystemTime>,
    /// When `death` fired (the first death).
    pub death_at: Option<SystemTime>,
    /// When it was cancelled (the first time).
    pub cancelled_at: Option<SystemTime>,
    /// When it finished (its linger started).
    pub finished_at: Option<SystemTime>,
    /// Events wait in the outbox, not dispatched yet.
    pub undispatched: bool,
}

impl BatchSummary {
    /// The state to show: the first that applies of succeeded, cancelled, dead (a member died
    /// and the batch did not succeed), complete, pending.
    pub fn state(&self) -> BatchState {
        state_of(
            self.success_at.is_some(),
            self.cancelled_at.is_some(),
            self.death_at.is_some(),
            self.complete_at.is_some(),
        )
    }

    /// Members that succeeded: `total − pending − skipped`.
    pub fn succeeded(&self) -> u64 {
        self.total
            .saturating_sub(self.pending)
            .saturating_sub(self.skipped)
    }
}

/// A batch's full status ([`Batches::fetch`], [`Client::batch_status`]).
///
/// [`Client::batch_status`]: crate::Client::batch_status
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct BatchStatus {
    /// The batch id.
    pub bid: String,
    /// Its description.
    pub description: Option<String>,
    /// Its parent, for a child batch.
    pub parent: Option<String>,
    /// When it was created.
    pub created_at: SystemTime,
    /// When its keys expire (`None` for the testing clients, whose batches do not expire).
    pub expires_at: Option<SystemTime>,
    /// Members registered.
    pub total: u64,
    /// Members not yet succeeded or skipped.
    pub pending: u64,
    /// Members that failed and have not succeeded since.
    pub failures: u64,
    /// Members that will not run again on their own.
    pub dead: u64,
    /// Members skipped.
    pub skipped: u64,
    /// Child batches.
    pub children: u64,
    /// Children whose `complete` was dispatched.
    pub children_complete: u64,
    /// Children whose `success` was dispatched.
    pub children_succeeded: u64,
    /// When `complete` fired.
    pub complete_at: Option<SystemTime>,
    /// When `success` fired.
    pub success_at: Option<SystemTime>,
    /// When `death` fired.
    pub death_at: Option<SystemTime>,
    /// When `complete` was dispatched.
    pub complete_dispatched_at: Option<SystemTime>,
    /// When `success` was dispatched.
    pub success_dispatched_at: Option<SystemTime>,
    /// When `death` was dispatched.
    pub death_dispatched_at: Option<SystemTime>,
    /// When it was cancelled (the first time).
    pub cancelled_at: Option<SystemTime>,
    /// When it finished (its linger started).
    pub finished_at: Option<SystemTime>,
    /// Expiry after the last activity of the unfinished batch.
    pub ttl: Duration,
    /// How long the data stays once the batch finished.
    pub linger: Duration,
    /// Its callbacks.
    pub callbacks: Vec<CallbackInfo>,
    /// For a workflow's overall batch, its number of stages.
    pub workflow_stages: Option<u64>,
    /// Events wait in the outbox, not dispatched yet.
    pub undispatched: bool,
}

impl BatchStatus {
    /// The state to show: the first that applies of succeeded, cancelled, dead (a member died
    /// and the batch did not succeed), complete, pending.
    pub fn state(&self) -> BatchState {
        state_of(
            self.success_at.is_some(),
            self.cancelled_at.is_some(),
            self.death_at.is_some(),
            self.complete_at.is_some(),
        )
    }

    /// Members that succeeded: `total − pending − skipped`.
    pub fn succeeded(&self) -> u64 {
        self.total
            .saturating_sub(self.pending)
            .saturating_sub(self.skipped)
    }

    /// Whether `complete` fired.
    pub fn is_complete(&self) -> bool {
        self.complete_at.is_some()
    }

    /// Whether `success` fired.
    pub fn is_success(&self) -> bool {
        self.success_at.is_some()
    }

    /// How long the batch took to succeed (`success_at − created_at`).
    pub fn duration(&self) -> Option<Duration> {
        self.success_at?.duration_since(self.created_at).ok()
    }
}

fn state_of(succeeded: bool, cancelled: bool, dead: bool, complete: bool) -> BatchState {
    if succeeded {
        BatchState::Succeeded
    } else if cancelled {
        BatchState::Cancelled
    } else if dead {
        BatchState::Dead
    } else if complete {
        BatchState::Complete
    } else {
        BatchState::Pending
    }
}

/// One callback of a batch.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct CallbackInfo {
    /// The event it runs for.
    pub event: BatchEvent,
    /// Its job class.
    pub class: String,
    /// Its queue.
    pub queue: String,
    /// Its jid (fixed when the batch was defined).
    pub jid: String,
    /// Its arguments.
    pub args: Vec<Value>,
    /// Its event was dispatched: it was pushed.
    pub dispatched: bool,
}

/// One failed member of a batch ([`Batches::failures`]).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct FailedMember {
    /// Its jid.
    pub jid: String,
    /// Its job class.
    pub class: String,
    /// The class of its error.
    pub error_class: Option<String>,
    /// Its error message (the first 200 characters).
    pub error: Option<String>,
    /// It will not run again on its own.
    pub dead: bool,
    /// Why: `retry`, `morgue`, `no_retry`, `discarded`, `deleted` or `killed`.
    pub reason: String,
    /// Its retry count.
    pub retry_count: Option<u32>,
    /// When it failed.
    pub failed_at: Option<SystemTime>,
    /// When it runs again, for a retry.
    pub next_at: Option<SystemTime>,
}

/// The batch counters of a period ([`Batches::events`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct BatchEvents {
    /// Batches created.
    pub created: u64,
    /// `complete` events fired.
    pub complete: u64,
    /// `success` events fired.
    pub success: u64,
    /// `death` events fired.
    pub death: u64,
    /// Batches cancelled.
    pub cancelled: u64,
}

/// A listing of [`Batches::search`]: the page, and what was searched.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct BatchSearch {
    /// The page of matches (its `total`: the matches found).
    pub page: Page<BatchSummary>,
    /// Index entries looked at.
    pub searched: u64,
    /// The search stopped before the end of the index (50 000 entries looked at, or 500
    /// matches found).
    pub truncated: bool,
}

/// One summary of the stored failure (the JSON in `:failed`).
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct StoredFailure {
    class: String,
    error_class: Option<String>,
    error: Option<String>,
    state: String,
    reason: String,
    retry_count: Option<u32>,
    at: Option<i64>,
    next_at: Option<f64>,
}

impl Batches {
    /// The batches of `api`'s Redis.
    pub fn new(api: impl Into<Api>) -> Self {
        Self { api: api.into() }
    }

    fn redis(&self) -> &RedisPool {
        self.api.redis()
    }

    /// Entries of the index (expired batches included until trimmed: an upper bound).
    pub async fn size(&self) -> Result<u64> {
        self.redis()
            .with_retry(|mut conn| async move {
                redis::cmd("ZCARD")
                    .arg(keys::BATCHES)
                    .query_async(&mut conn)
                    .await
            })
            .await
    }

    /// Batches whose fired events wait to be dispatched (their callbacks pushed): the outbox.
    /// Entries older than `batch_dispatch_grace` mean that no process dispatches them (no
    /// worker runs batch maintenance, or every attempt fails).
    pub async fn undispatched(&self) -> Result<u64> {
        self.redis()
            .with_retry(|mut conn| async move {
                redis::cmd("ZCARD")
                    .arg(keys::BATCH_EVENTS)
                    .query_async(&mut conn)
                    .await
            })
            .await
    }

    /// One page (from 1) of the batches `filter` selects, most recently active first; see
    /// [`Batches::search`] for the bounds of filtered listings.
    pub async fn page(
        &self,
        filter: &BatchFilter,
        page: usize,
        size: usize,
    ) -> Result<Page<BatchSummary>> {
        let (state, text) = match filter {
            BatchFilter::Text(text) => (BatchFilter::All, Some(text.as_str())),
            other => (other.clone(), None),
        };
        Ok(self.search(&state, text, page, size).await?.page)
    }

    /// One page (from 1) of the batches in state `state` whose bid or description contains
    /// `text` (`*` standing for any run of characters), most recently active first. Without a
    /// filter (`All`, no text) the index is paged directly; otherwise its 50 000 most recently
    /// active entries are searched and at most 500 matches kept ([`BatchSearch`] says so).
    pub async fn search(
        &self,
        state: &BatchFilter,
        text: Option<&str>,
        page: usize,
        size: usize,
    ) -> Result<BatchSearch> {
        let page = page.max(1);
        let size = size.clamp(1, MAX_PAGE_SIZE);
        let text = text.filter(|text| !text.is_empty());
        let filtered = text.is_some() || *state != BatchFilter::All;
        if !filtered {
            let total = self.size().await?;
            let start = (page - 1).saturating_mul(size);
            let bids = self.index_slice(start, size).await?;
            let summaries = self.summaries(&bids).await?;
            return Ok(BatchSearch {
                page: Page {
                    items: summaries.into_iter().flatten().collect(),
                    number: page,
                    size,
                    total,
                },
                searched: total,
                truncated: false,
            });
        }
        let pattern = text.map(Pattern::new);
        let mut matches = Vec::new();
        let mut searched = 0_u64;
        let mut truncated = false;
        let mut start = 0;
        while start < SEARCH_WINDOW {
            let bids = self.index_slice(start, INDEX_CHUNK).await?;
            if bids.is_empty() {
                break;
            }
            start += bids.len();
            let fields = self.state_fields(&bids).await?;
            for (bid, fields) in bids.into_iter().zip(fields) {
                searched += 1;
                let Some(fields) = fields else {
                    continue;
                };
                if matches_state(state, &fields)
                    && pattern.as_ref().is_none_or(|pattern| {
                        pattern.matches(&bid)
                            || fields.get("desc").is_some_and(|desc| pattern.matches(desc))
                    })
                {
                    matches.push(bid);
                    if matches.len() >= MAX_MATCHES {
                        truncated = true;
                        break;
                    }
                }
            }
            if truncated {
                break;
            }
        }
        if start >= SEARCH_WINDOW && self.size().await? > u64::try_from(start).unwrap_or(0) {
            truncated = true;
        }
        let total = u64::try_from(matches.len()).unwrap_or(u64::MAX);
        let skip = (page - 1).saturating_mul(size);
        let chosen: Vec<String> = matches.into_iter().skip(skip).take(size).collect();
        let summaries = self.summaries(&chosen).await?;
        Ok(BatchSearch {
            page: Page {
                items: summaries.into_iter().flatten().collect(),
                number: page,
                size,
                total,
            },
            searched,
            truncated,
        })
    }

    /// Bids of the index from position `start` (most recently active first).
    async fn index_slice(&self, start: usize, count: usize) -> Result<Vec<String>> {
        let stop = start.saturating_add(count).saturating_sub(1);
        self.redis()
            .with_retry(|mut conn| async move {
                redis::cmd("ZREVRANGE")
                    .arg(keys::BATCHES)
                    .arg(start)
                    .arg(stop)
                    .query_async(&mut conn)
                    .await
            })
            .await
    }

    /// The fields a state filter needs, per bid (`None` for a batch that is gone).
    async fn state_fields(&self, bids: &[String]) -> Result<Vec<Option<HashMap<String, String>>>> {
        const FIELDS: [&str; 7] = [
            "v",
            "desc",
            "complete_at",
            "success_at",
            "death_at",
            "cancelled_at",
            "finished_at",
        ];
        let mut pipe = redis::pipe();
        for bid in bids {
            pipe.cmd("HMGET").arg(keys::batch(bid)).arg(&FIELDS);
        }
        let pipe = &pipe;
        let rows: Vec<Vec<Option<String>>> = self
            .redis()
            .with_retry(|mut conn| async move { pipe.query_async(&mut conn).await })
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                let fields: HashMap<String, String> = FIELDS
                    .iter()
                    .zip(row)
                    .filter_map(|(field, value)| Some(((*field).to_owned(), value?)))
                    .collect();
                fields.contains_key("v").then_some(fields)
            })
            .collect())
    }

    /// The summaries of `bids`, in order (`None` for a batch that is gone).
    async fn summaries(&self, bids: &[String]) -> Result<Vec<Option<BatchSummary>>> {
        if bids.is_empty() {
            return Ok(Vec::new());
        }
        let mut pipe = redis::pipe();
        for bid in bids {
            pipe.cmd("HMGET").arg(keys::batch(bid)).arg(&SUMMARY_FIELDS);
            pipe.cmd("SCARD").arg(keys::batch_pending(bid));
            pipe.cmd("HLEN").arg(keys::batch_failed(bid));
            pipe.cmd("SCARD").arg(keys::batch_dead(bid));
            pipe.cmd("PTTL").arg(keys::batch(bid));
            pipe.cmd("ZSCORE").arg(keys::BATCH_EVENTS).arg(bid);
        }
        let pipe = &pipe;
        let replies: Vec<redis::Value> = self
            .redis()
            .with_retry(|mut conn| async move { pipe.query_async(&mut conn).await })
            .await?;
        let now = SystemTime::now();
        let mut summaries = Vec::with_capacity(bids.len());
        let mut replies = replies.into_iter();
        for bid in bids {
            let row: Vec<redis::Value> = replies.by_ref().take(6).collect();
            summaries.push(summary_of(bid, &row, now)?);
        }
        Ok(summaries)
    }

    /// The full status of batch `bid`, or `None` when it never existed, expired or was deleted.
    /// A batch written by a newer hench is [`BatchError::Unsupported`].
    pub async fn fetch(&self, bid: &str) -> Result<Option<BatchStatus>> {
        let mut pipe = redis::pipe();
        pipe.cmd("HGETALL").arg(keys::batch(bid));
        pipe.cmd("SCARD").arg(keys::batch_pending(bid));
        pipe.cmd("HLEN").arg(keys::batch_failed(bid));
        pipe.cmd("SCARD").arg(keys::batch_dead(bid));
        pipe.cmd("PTTL").arg(keys::batch(bid));
        pipe.cmd("ZSCORE").arg(keys::BATCH_EVENTS).arg(bid);
        let pipe = &pipe;
        #[allow(clippy::type_complexity)]
        let (hash, pending, failures, dead, pttl, outbox): (
            HashMap<String, String>,
            u64,
            u64,
            u64,
            i64,
            Option<f64>,
        ) = self
            .redis()
            .with_retry(|mut conn| async move { pipe.query_async(&mut conn).await })
            .await?;
        if hash.is_empty() {
            return Ok(None);
        }
        check_format(bid, &hash)?;
        let expires_at =
            (pttl > 0).then(|| SystemTime::now() + Duration::from_millis(pttl.unsigned_abs()));
        Ok(Some(status_of(
            bid,
            &hash,
            Counts {
                pending,
                failures,
                dead,
            },
            expires_at,
            outbox.is_some(),
        )))
    }

    /// The failed members of batch `bid`, most recent failure first (at most 1 000 read).
    pub async fn failures(
        &self,
        bid: &str,
        page: usize,
        size: usize,
    ) -> Result<Page<FailedMember>> {
        let key = keys::batch_failed(bid);
        let entries: Vec<(String, String)> = self.scan_hash(&key, MAX_FAILURES).await?;
        let mut members: Vec<FailedMember> = entries
            .into_iter()
            .map(|(jid, summary)| failed_member(jid, &summary))
            .collect();
        members.sort_by(|a, b| b.failed_at.cmp(&a.failed_at).then(a.jid.cmp(&b.jid)));
        Ok(crate::api::page_items(
            members,
            page,
            size.clamp(1, MAX_PAGE_SIZE),
        ))
    }

    /// Up to `limit` jids of the members of batch `bid` still pending (for finding stuck
    /// members), in no particular order.
    pub async fn pending_jids(&self, bid: &str, limit: usize) -> Result<Vec<String>> {
        let key = keys::batch_pending(bid);
        let key = &key;
        let mut jids = Vec::new();
        let mut cursor = 0_u64;
        loop {
            let (next, found): (u64, Vec<String>) = self
                .redis()
                .with_retry(|mut conn| async move {
                    redis::cmd("SSCAN")
                        .arg(key)
                        .arg(cursor)
                        .arg("COUNT")
                        .arg(200)
                        .query_async(&mut conn)
                        .await
                })
                .await?;
            jids.extend(found);
            cursor = next;
            if cursor == 0 || jids.len() >= limit {
                break;
            }
        }
        jids.sort_unstable();
        jids.dedup();
        jids.truncate(limit);
        Ok(jids)
    }

    /// The child batches of batch `bid` (at most 1 000), oldest first.
    pub async fn children(&self, bid: &str) -> Result<Vec<BatchSummary>> {
        let mut kids = self.kids(bid).await?;
        kids.truncate(MAX_CHILDREN);
        let mut children: Vec<BatchSummary> =
            self.summaries(&kids).await?.into_iter().flatten().collect();
        children.sort_by(|a, b| a.created_at.cmp(&b.created_at).then(a.bid.cmp(&b.bid)));
        Ok(children)
    }

    async fn kids(&self, bid: &str) -> Result<Vec<String>> {
        let key = keys::batch_kids(bid);
        let key = &key;
        self.redis()
            .with_retry(|mut conn| async move {
                redis::cmd("SMEMBERS").arg(key).query_async(&mut conn).await
            })
            .await
    }

    /// Cancels batch `bid` and its descendants ([batches.md] §2.2.6): members not
    /// run yet are skipped when they come up, `success` never fires, `complete` fires once every
    /// member left has run. Returns how many batches were newly cancelled. A missing batch is
    /// [`BatchError::NotFound`], a succeeded one [`BatchError::Succeeded`].
    ///
    /// The whole tree is cancelled by this call, level by level, 500 batches per round trip
    /// (each batch by its own script); descendants that are gone or have succeeded are left as
    /// they are. If Redis refuses to cancel a descendant, the rest of the tree is cancelled
    /// anyway (but for that descendant's own subtree), the events of the batches cancelled are
    /// dispatched, and the first refusal is returned: cancelling again is harmless.
    ///
    /// [batches.md]: https://github.com/rkwap/hench/blob/v0.1.0/docs/design/batches.md
    pub async fn cancel(&self, bid: &str) -> Result<usize> {
        let root = scripts::cancel(self.redis(), bid).await?;
        let mut cancelled = usize::from(root.first);
        let mut to_dispatch = Vec::new();
        if root.dispatch {
            to_dispatch.push(bid.to_owned());
        }
        let mut seen = HashSet::from([bid.to_owned()]);
        let mut level: Vec<String> = root
            .children
            .into_iter()
            .filter(|child| seen.insert(child.clone()))
            .collect();
        let mut descendants = 0_usize;
        let mut failure = None;
        'tree: while !level.is_empty() {
            let mut next = Vec::new();
            for chunk in level.chunks(TREE_PIPELINE) {
                let replies = match scripts::cancel_each(self.redis(), chunk).await {
                    Ok(replies) => replies,
                    Err(error) => {
                        failure = Some(error);
                        break 'tree;
                    }
                };
                for (child, reply) in chunk.iter().zip(replies) {
                    let done = match reply {
                        Ok(done) => done,
                        Err(error) if is_gone_or_succeeded(&error) => continue,
                        Err(error) => {
                            tracing::warn!(%error, bid = %child, "could not cancel a batch of a tree");
                            failure.get_or_insert(error);
                            continue;
                        }
                    };
                    descendants += 1;
                    cancelled += usize::from(done.first);
                    if done.dispatch {
                        to_dispatch.push(child.clone());
                    }
                    next.extend(
                        done.children
                            .into_iter()
                            .filter(|grandchild| seen.insert(grandchild.clone())),
                    );
                }
            }
            level = next;
        }
        tracing::info!(bid, descendants, cancelled, "batch cancelled");
        for bid in to_dispatch {
            if let Err(error) = dispatch_chain(self.redis(), &bid).await {
                tracing::warn!(%error, bid = %bid, "could not dispatch a cancelled batch's callbacks");
            }
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(cancelled),
        }
    }

    /// Deletes the data of batch `bid` and of its descendants. An unfinished child of a parent
    /// that still exists and has not finished is refused unless `force`
    /// ([`BatchError::DeleteChild`]: that parent could then never finish). Members still in
    /// queues and sets run normally; their batch is gone. Returns `false` when there was nothing
    /// to delete.
    ///
    /// The whole tree is deleted by this call: it is read level by level, then deleted from its
    /// deepest level up, 500 batches per round trip, `bid` last, so that a call that fails
    /// midway leaves a tree that a repeat finds and finishes.
    pub async fn delete(&self, bid: &str, force: bool) -> Result<bool> {
        let hash = keys::batch(bid);
        let hash = &hash;
        let (parent, finished): (Option<String>, Option<String>) = self
            .redis()
            .with_retry(|mut conn| async move {
                redis::cmd("HMGET")
                    .arg(hash)
                    .arg(&["parent", "finished_at"])
                    .query_async(&mut conn)
                    .await
            })
            .await?;
        if !force
            && finished.is_none()
            && let Some(parent) = &parent
            && self.unfinished(parent).await?
        {
            return Err(Error::Batch(BatchError::DeleteChild {
                bid: bid.to_owned(),
                parent: parent.clone(),
            }));
        }
        // The descendants, level by level.
        let mut levels: Vec<Vec<String>> = Vec::new();
        let mut seen = HashSet::from([bid.to_owned()]);
        let mut level = vec![bid.to_owned()];
        loop {
            let mut next = Vec::new();
            for chunk in level.chunks(TREE_PIPELINE) {
                for kids in self.kids_of(chunk).await? {
                    next.extend(kids.into_iter().filter(|kid| seen.insert(kid.clone())));
                }
            }
            if next.is_empty() {
                break;
            }
            levels.push(next.clone());
            level = next;
        }
        let mut removed = 0;
        for level in levels.iter().rev() {
            for chunk in level.chunks(TREE_PIPELINE) {
                removed += scripts::delete_each(self.redis(), chunk).await?;
            }
        }
        removed += scripts::delete(self.redis(), bid, parent.as_deref(), force).await?;
        tracing::info!(
            bid,
            batches = 1 + levels.iter().map(Vec::len).sum::<usize>(),
            keys = removed,
            "batch deleted"
        );
        Ok(removed > 0)
    }

    /// The `kids` sets of `bids`, in order, read in one round trip.
    async fn kids_of(&self, bids: &[String]) -> Result<Vec<Vec<String>>> {
        let mut pipe = redis::pipe();
        for bid in bids {
            pipe.cmd("SMEMBERS").arg(keys::batch_kids(bid));
        }
        let pipe = &pipe;
        self.redis()
            .with_retry(|mut conn| async move { pipe.query_async(&mut conn).await })
            .await
    }

    /// Whether batch `bid` exists and has not finished.
    async fn unfinished(&self, bid: &str) -> Result<bool> {
        let hash = keys::batch(bid);
        let hash = &hash;
        let (version, finished): (Option<String>, Option<String>) = self
            .redis()
            .with_retry(|mut conn| async move {
                redis::cmd("HMGET")
                    .arg(hash)
                    .arg(&["v", "finished_at"])
                    .query_async(&mut conn)
                    .await
            })
            .await?;
        Ok(version.is_some() && finished.is_none())
    }

    /// Skips pending member `jid` of batch `bid` (an operator's decision: it no longer blocks
    /// `success`), then dispatches the events this fired. `false` when it was not pending; a
    /// missing batch is [`BatchError::NotFound`].
    pub async fn skip(&self, bid: &str, jid: &str) -> Result<bool> {
        if !self.exists(bid).await? {
            return Err(Error::Batch(BatchError::NotFound {
                bid: bid.to_owned(),
            }));
        }
        let accounted = scripts::account(
            self.redis(),
            bid,
            jid,
            &Outcome::Skipped { cancelled: false },
        )
        .await?;
        if accounted.applied {
            tracing::info!(bid, jid, "batch member skipped by an operator");
        }
        settle(
            self.redis(),
            &Report {
                bid: bid.to_owned(),
                dispatch: accounted.dispatch,
                refreshed: accounted.refreshed,
            },
        )
        .await;
        Ok(accounted.applied)
    }

    async fn exists(&self, bid: &str) -> Result<bool> {
        let hash = keys::batch(bid);
        let hash = &hash;
        self.redis()
            .with_retry(|mut conn| async move {
                redis::cmd("HEXISTS")
                    .arg(hash)
                    .arg("v")
                    .query_async(&mut conn)
                    .await
            })
            .await
    }

    /// Dispatches the events batch `bid` left waiting in the outbox now, rather than waiting
    /// for maintenance; returns how many batches had events dispatched (its ancestors may).
    pub async fn dispatch(&self, bid: &str) -> Result<usize> {
        dispatch_chain(self.redis(), bid).await
    }

    /// Polls batch `bid` (every 50 ms at first, then up to every second) until `complete` fired
    /// or `timeout` passed, and returns its status (Sidekiq Pro's `join`). A batch that does not
    /// exist, or disappears, is [`BatchError::NotFound`].
    pub async fn wait(&self, bid: &str, timeout: Duration) -> Result<BatchStatus> {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut pause = Duration::from_millis(50);
        loop {
            let Some(status) = self.fetch(bid).await? else {
                return Err(Error::Batch(BatchError::NotFound {
                    bid: bid.to_owned(),
                }));
            };
            let now = tokio::time::Instant::now();
            if status.is_complete() || now >= deadline {
                return Ok(status);
            }
            tokio::time::sleep(pause.min(deadline - now)).await;
            pause = (pause * 2).min(Duration::from_secs(1));
        }
    }

    /// The batch counters of `period` (minute periods of up to 8 hours): batches created,
    /// events fired, batches cancelled.
    pub async fn events(&self, period: Period) -> Result<BatchEvents> {
        let read = events::read(self.redis(), EventKind::Batches, period, Utc::now()).await?;
        let mut counts = BatchEvents::default();
        for ((_, event), count) in read.totals {
            match event.as_str() {
                "created" => counts.created += count,
                "complete" => counts.complete += count,
                "success" => counts.success += count,
                "death" => counts.death += count,
                "cancelled" => counts.cancelled += count,
                _ => {}
            }
        }
        Ok(counts)
    }

    /// Reads up to `limit` fields of hash `key`.
    async fn scan_hash(&self, key: &str, limit: usize) -> Result<Vec<(String, String)>> {
        let mut entries = Vec::new();
        let mut cursor = 0_u64;
        loop {
            let (next, found): (u64, Vec<(String, String)>) = self
                .redis()
                .with_retry(|mut conn| async move {
                    redis::cmd("HSCAN")
                        .arg(key)
                        .arg(cursor)
                        .arg("COUNT")
                        .arg(200)
                        .query_async(&mut conn)
                        .await
                })
                .await?;
            entries.extend(found);
            cursor = next;
            if cursor == 0 || entries.len() >= limit {
                break;
            }
        }
        entries.sort_unstable();
        entries.dedup_by(|a, b| a.0 == b.0);
        entries.truncate(limit);
        Ok(entries)
    }
}

/// Whether a cancel's error means the batch is gone or succeeded (skipped for descendants).
fn is_gone_or_succeeded(error: &Error) -> bool {
    matches!(
        error,
        Error::Batch(BatchError::NotFound { .. } | BatchError::Succeeded { .. })
    )
}

/// Whether a batch with the fields of a filter is in state `state`.
fn matches_state(state: &BatchFilter, fields: &HashMap<String, String>) -> bool {
    let has = |field: &str| fields.contains_key(field);
    match state {
        BatchFilter::Active => !has("success_at") && !has("finished_at"),
        BatchFilter::Dead => has("death_at") && !has("success_at"),
        BatchFilter::Complete => has("complete_at"),
        BatchFilter::Succeeded => has("success_at"),
        BatchFilter::Cancelled => has("cancelled_at"),
        _ => true,
    }
}

/// Refuses a batch written by a newer hench.
fn check_format(bid: &str, hash: &HashMap<String, String>) -> Result<()> {
    let version = hash
        .get("v")
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(0);
    if version > FORMAT {
        return Err(Error::Batch(BatchError::Unsupported {
            bid: bid.to_owned(),
            version,
        }));
    }
    Ok(())
}

/// The counts of a batch's sets.
struct Counts {
    pending: u64,
    failures: u64,
    dead: u64,
}

fn number(fields: &HashMap<String, String>, field: &str) -> u64 {
    fields
        .get(field)
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
}

fn time(fields: &HashMap<String, String>, field: &str) -> Option<SystemTime> {
    fields
        .get(field)
        .and_then(|value| value.parse::<i64>().ok())
        .map(millis_to_system_time)
}

fn text_field(fields: &HashMap<String, String>, field: &str) -> Option<String> {
    fields.get(field).filter(|value| !value.is_empty()).cloned()
}

/// The summary of `bid` from a pipeline row (HMGET of [`SUMMARY_FIELDS`], SCARD, HLEN, SCARD,
/// PTTL, ZSCORE).
fn summary_of(bid: &str, row: &[redis::Value], now: SystemTime) -> Result<Option<BatchSummary>> {
    let values: Vec<Option<String>> = match row.first() {
        Some(value) => parse(value)?,
        None => return Ok(None),
    };
    let fields: HashMap<String, String> = SUMMARY_FIELDS
        .iter()
        .zip(values)
        .filter_map(|(field, value)| Some(((*field).to_owned(), value?)))
        .collect();
    if !fields.contains_key("v") {
        return Ok(None);
    }
    let count = |index: usize| -> Result<u64> { row.get(index).map_or(Ok(0), parse::<u64>) };
    let pttl: i64 = row.get(4).map_or(Ok(-2), parse)?;
    let outbox: Option<f64> = row.get(5).map_or(Ok(None), parse)?;
    Ok(Some(BatchSummary {
        bid: bid.to_owned(),
        description: text_field(&fields, "desc"),
        parent: text_field(&fields, "parent"),
        created_at: time(&fields, "created_at").unwrap_or(SystemTime::UNIX_EPOCH),
        expires_at: (pttl > 0).then(|| now + Duration::from_millis(pttl.unsigned_abs())),
        total: number(&fields, "total"),
        pending: count(1)?,
        failures: count(2)?,
        dead: count(3)?,
        skipped: number(&fields, "skipped"),
        children: number(&fields, "kids"),
        complete_at: time(&fields, "complete_at"),
        success_at: time(&fields, "success_at"),
        death_at: time(&fields, "death_at"),
        cancelled_at: time(&fields, "cancelled_at"),
        finished_at: time(&fields, "finished_at"),
        undispatched: outbox.is_some(),
    }))
}

/// A value of a pipeline's reply.
fn parse<T: redis::FromRedisValue>(value: &redis::Value) -> Result<T> {
    redis::from_redis_value_ref(value).map_err(|error| Error::Redis(error.into()))
}

/// The status of `bid` from its hash.
fn status_of(
    bid: &str,
    fields: &HashMap<String, String>,
    counts: Counts,
    expires_at: Option<SystemTime>,
    undispatched: bool,
) -> BatchStatus {
    let dispatched = |event: BatchEvent| fields.contains_key(&format!("{event}_dt"));
    let mut callbacks = Vec::new();
    for event in [BatchEvent::Complete, BatchEvent::Success, BatchEvent::Death] {
        let raw = fields.get(&format!("cb_{event}"));
        for template in parse_templates(raw.map(String::as_str)).unwrap_or_default() {
            callbacks.push(callback_info(event, &template, dispatched(event)));
        }
    }
    let workflow_stages = fields
        .get("wf")
        .and_then(|wf| serde_json::from_str::<Value>(wf).ok())
        .and_then(|wf| wf.get("n").and_then(Value::as_u64));
    BatchStatus {
        bid: bid.to_owned(),
        description: text_field(fields, "desc"),
        parent: text_field(fields, "parent"),
        created_at: time(fields, "created_at").unwrap_or(SystemTime::UNIX_EPOCH),
        expires_at,
        total: number(fields, "total"),
        pending: counts.pending,
        failures: counts.failures,
        dead: counts.dead,
        skipped: number(fields, "skipped"),
        children: number(fields, "kids"),
        children_complete: number(fields, "kc"),
        children_succeeded: number(fields, "ks"),
        complete_at: time(fields, "complete_at"),
        success_at: time(fields, "success_at"),
        death_at: time(fields, "death_at"),
        complete_dispatched_at: time(fields, "complete_dt"),
        success_dispatched_at: time(fields, "success_dt"),
        death_dispatched_at: time(fields, "death_dt"),
        cancelled_at: time(fields, "cancelled_at"),
        finished_at: time(fields, "finished_at"),
        ttl: Duration::from_secs(number(fields, "ttl")),
        linger: Duration::from_secs(number(fields, "linger")),
        callbacks,
        workflow_stages,
        undispatched,
    }
}

/// The description of one callback template.
fn callback_info(event: BatchEvent, template: &Template, dispatched: bool) -> CallbackInfo {
    let job = JobPayload::from_json(&template.j).ok();
    CallbackInfo {
        event,
        class: job
            .as_ref()
            .map(|job| job.class.clone())
            .unwrap_or_default(),
        queue: template.q.clone(),
        jid: template.jid.clone(),
        args: job.map(|job| job.args).unwrap_or_default(),
        dispatched,
    }
}

/// A failed member from its stored summary (lenient: an unreadable summary keeps the jid).
fn failed_member(jid: String, summary: &str) -> FailedMember {
    let stored: StoredFailure = serde_json::from_str(summary).unwrap_or_default();
    FailedMember {
        jid,
        class: stored.class,
        error_class: stored.error_class,
        error: stored.error,
        dead: stored.state != "retry",
        reason: stored.reason,
        retry_count: stored.retry_count,
        failed_at: stored.at.map(millis_to_system_time),
        // A time no clock can show (a `retry_in` beyond reason, from before retry delays were
        // bounded) is left out rather than shown wrong.
        next_at: stored
            .next_at
            .filter(|at| at.is_finite() && *at > 0.0)
            .and_then(|at| Duration::try_from_secs_f64(at).ok())
            .and_then(|since_epoch| SystemTime::UNIX_EPOCH.checked_add(since_epoch)),
    }
}

/// The status of a batch of a testing client's table.
pub(crate) fn status_in_model(model: &Model, bid: &str) -> Option<BatchStatus> {
    let batch: &ModelBatch = model.batches.get(bid)?;
    let at = |time: Option<i64>| time.map(millis_to_system_time);
    let mut callbacks = Vec::new();
    for (event, dispatched) in [
        (BatchEvent::Complete, batch.complete_dt.is_some()),
        (BatchEvent::Success, batch.success_dt.is_some()),
        (BatchEvent::Death, batch.death_dt.is_some()),
    ] {
        for template in &batch.callbacks[crate::batch::model::event_index(event)] {
            callbacks.push(callback_info(event, template, dispatched));
        }
    }
    let workflow_stages = batch
        .extra
        .get("wf")
        .and_then(|wf| serde_json::from_str::<Value>(wf).ok())
        .and_then(|wf| wf.get("n").and_then(Value::as_u64));
    let len = |n: usize| u64::try_from(n).unwrap_or(u64::MAX);
    Some(BatchStatus {
        bid: bid.to_owned(),
        description: batch.description.clone(),
        parent: batch.parent.clone(),
        created_at: millis_to_system_time(batch.created_at),
        expires_at: None,
        total: batch.total,
        pending: len(batch.pending.len()),
        failures: len(batch.failed.len()),
        dead: len(batch.dead.len()),
        skipped: batch.skipped,
        children: batch.kids,
        children_complete: batch.kc,
        children_succeeded: batch.ks,
        complete_at: at(batch.complete_at),
        success_at: at(batch.success_at),
        death_at: at(batch.death_at),
        complete_dispatched_at: at(batch.complete_dt),
        success_dispatched_at: at(batch.success_dt),
        death_dispatched_at: at(batch.death_dt),
        cancelled_at: at(batch.cancelled_at),
        finished_at: at(batch.finished_at),
        ttl: Duration::from_secs(batch.ttl_secs),
        linger: Duration::from_secs(batch.linger_secs),
        callbacks,
        workflow_stages,
        undispatched: model.outbox.contains_key(bid),
    })
}

/// A filter: text an id must contain, `*` standing for any run of characters (the dashboard's
/// rule).
struct Pattern {
    parts: Vec<String>,
}

impl Pattern {
    fn new(filter: &str) -> Self {
        Self {
            parts: filter
                .split('*')
                .filter(|part| !part.is_empty())
                .map(str::to_owned)
                .collect(),
        }
    }

    fn matches(&self, text: &str) -> bool {
        let mut rest = text;
        for part in &self.parts {
            match rest.find(part.as_str()) {
                Some(start) => rest = &rest[start + part.len()..],
                None => return false,
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn states_come_from_the_event_times() {
        assert_eq!(state_of(true, true, true, true), BatchState::Succeeded);
        assert_eq!(state_of(false, true, true, true), BatchState::Cancelled);
        assert_eq!(state_of(false, false, true, true), BatchState::Dead);
        assert_eq!(state_of(false, false, false, true), BatchState::Complete);
        assert_eq!(state_of(false, false, false, false), BatchState::Pending);
        let fields: HashMap<String, String> = [("death_at", "1"), ("complete_at", "1")]
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect();
        assert!(matches_state(&BatchFilter::Dead, &fields));
        assert!(matches_state(&BatchFilter::Active, &fields));
        assert!(matches_state(&BatchFilter::Complete, &fields));
        assert!(!matches_state(&BatchFilter::Succeeded, &fields));
        assert!(!matches_state(&BatchFilter::Cancelled, &fields));
    }

    #[test]
    fn failures_are_read_leniently() {
        let member = failed_member(
            "j1".to_owned(),
            r#"{"class":"A","error":"boom","state":"retry","reason":"retry","retry_count":2,"at":1000,"next_at":1.5}"#,
        );
        assert_eq!(member.class, "A");
        assert!(!member.dead);
        assert_eq!(member.retry_count, Some(2));
        assert_eq!(member.failed_at, Some(millis_to_system_time(1_000)));
        assert!(member.next_at.is_some());
        let unreadable = failed_member("j2".to_owned(), "not json");
        assert_eq!(unreadable.jid, "j2");
        assert!(unreadable.dead);
    }

    #[test]
    fn patterns_match_like_the_dashboard() {
        assert!(Pattern::new("imp*7").matches("import #7"));
        assert!(Pattern::new("").matches("anything"));
        assert!(!Pattern::new("x*y").matches("yx"));
    }

    #[test]
    fn newer_formats_are_refused() {
        let hash: HashMap<String, String> = [("v".to_owned(), "2".to_owned())].into();
        assert!(matches!(
            check_format("b", &hash),
            Err(Error::Batch(BatchError::Unsupported { version: 2, .. }))
        ));
        let hash: HashMap<String, String> = [("v".to_owned(), "1".to_owned())].into();
        assert!(check_format("b", &hash).is_ok());
    }
}
