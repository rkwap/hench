//! The locks of unique jobs ([`UniqueLocks`], from [`Api::unique_locks`]): listing, finding and
//! releasing them, trimming their index, and the per-minute counters of duplicates, replaced
//! holders and lapses (`docs/design/unique.md` §2.11). Also [`JobRecord::unique`].
//!
//! A lock is the hash `unique:<digest>`; the sorted set `unique-locks` indexes them by expiry.
//! The index can hold entries whose lock is gone (expired or released by a script that could
//! not reach the index): every read checks the lock itself and never shows a stale entry, and
//! pushes, reads and [`UniqueLocks::trim`] remove such entries.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::Utc;
use futures::Stream;
use serde_json::Value;

use crate::api::events::{EventKind, read as read_events};
use crate::api::{
    Api, JobRecord, JobSet, Page, SortOrder, SortedEntry, Work, batched, json_escaped,
    literal_pattern,
};
use crate::error::Result;
use crate::job::Job;
use crate::keys;
use crate::metrics::Period;
use crate::payload::now_secs_f64;
use crate::unique::endpoint::release_in_redis;
use crate::unique::types::valid_digest;
use crate::unique::{LockRef, UniqueOption, UniqueRef, UniqueUntil};

/// The largest page served (like the API's other pages).
const MAX_PAGE_SIZE: usize = 1_000;

/// Index entries read per round trip by [`UniqueLocks::entries`], [`UniqueLocks::release_all`]
/// and [`UniqueLocks::trim`].
const BATCH: usize = 100;

/// Stale index entries a page removes at most.
const PAGE_TRIM: usize = 100;

/// Index entries checked per round trip by [`UniqueLocks::trim`].
const TRIM_BATCH: usize = 1_000;

/// Entries [`UniqueLocks::locate`] reads at most in each place it searches (a queue, a sorted
/// set), like the dashboard's searches.
pub const LOCATE_LIMIT: u64 = 50_000;

/// Jobs or set entries [`UniqueLocks::locate`] reads per round trip.
const LOCATE_PAGE: usize = 1_000;

/// A unique job's lock (`unique:<digest>`), as [`UniqueLocks`] reads it.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct UniqueLock {
    /// The digest identifying the duplicates of the holder.
    pub digest: String,
    /// The jid of the job holding the lock.
    pub jid: String,
    /// The holder's class.
    pub class: String,
    /// The holder's queue when it took the lock (a lock across queues covers every queue).
    pub queue: String,
    /// When the lock is released.
    pub until: UniqueUntil,
    /// The holder's `unique_for`.
    pub unique_for: Duration,
    /// When the lock was taken (Redis's clock).
    pub locked_at: Option<SystemTime>,
    /// When the lock expires, from its remaining time to live when it was read (`None`: no
    /// expiry, which hench never sets).
    pub expires_at: Option<SystemTime>,
    /// When the holder runs, while it waits in the schedule (a debounce push may replace it).
    pub scheduled_at: Option<SystemTime>,
    /// The lock's hash as stored, every field (for inspection).
    pub fields: BTreeMap<String, String>,
}

impl UniqueLock {
    /// The lock read from its hash (`fields`, from `HGETALL`) and remaining time to live
    /// (`pttl`, milliseconds; negative: none, or gone), at `now`. `None` when the hash is empty
    /// (the lock is gone).
    fn from_fields(
        digest: &str,
        fields: &HashMap<String, String>,
        pttl: i64,
        now: SystemTime,
    ) -> Option<Self> {
        let jid = fields.get("jid")?.clone();
        let seconds = |field: &str| {
            fields
                .get(field)
                .and_then(|value| value.parse::<f64>().ok())
                .filter(|secs| secs.is_finite() && *secs >= 0.0)
                // A time no clock can show (a push of a far-future job wrote one before pushes
                // refused them) is left out.
                .and_then(|secs| Duration::try_from_secs_f64(secs).ok())
                .and_then(|since_epoch| UNIX_EPOCH.checked_add(since_epoch))
        };
        Some(Self {
            digest: digest.to_owned(),
            jid,
            class: fields.get("class").cloned().unwrap_or_default(),
            queue: fields.get("queue").cloned().unwrap_or_default(),
            until: fields
                .get("until")
                .and_then(|until| UniqueUntil::parse(until))
                .unwrap_or_default(),
            unique_for: Duration::from_secs(
                fields
                    .get("for")
                    .and_then(|secs| secs.parse::<u64>().ok())
                    .unwrap_or_default(),
            ),
            locked_at: seconds("t"),
            expires_at: u64::try_from(pttl)
                .ok()
                .and_then(|ms| now.checked_add(Duration::from_millis(ms))),
            scheduled_at: seconds("at"),
            fields: fields
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
        })
    }
}

/// Where the holder of a lock is ([`UniqueLocks::locate`]).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum HolderPlace {
    /// Running: the entry of the process running it.
    Running(Work),
    /// Waiting in its queue, `position` jobs from the front (1: the next to run).
    Queued {
        /// The job as queued.
        record: JobRecord,
        /// Its place in line.
        position: u64,
    },
    /// Waiting in the schedule.
    Scheduled(SortedEntry),
    /// Waiting for a retry.
    Retrying(SortedEntry),
    /// In the dead set.
    Dead(SortedEntry),
    /// In none of those places: it ran (an `expired` lock stays after its holder ran), its lock
    /// lapsed or was taken by a job that is gone, or it moved while the places were read.
    NotFound {
        /// Every place was searched to the end (else some stopped after
        /// [`LOCATE_LIMIT`] entries).
        complete: bool,
    },
}

/// The outcome of a bounded search.
enum Search<T> {
    Found(T),
    /// Not found; `true` when the whole place was read.
    Missing(bool),
}

/// The per-minute counters of unique jobs over a period ([`UniqueLocks::events`]).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct UniqueEvents {
    /// The sums over every class.
    pub totals: UniqueEventCounts,
    /// The sums per class.
    pub by_class: BTreeMap<String, UniqueEventCounts>,
}

/// Counts of the events of unique jobs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct UniqueEventCounts {
    /// Pushes that were duplicates (dropped, or refused with `OnConflict::Error`).
    pub duplicates: u64,
    /// Holders replaced by a debounce push (`OnConflict::Replace`).
    pub replaced: u64,
    /// Lapses: a `success` job that no longer held its lock when it succeeded, failed, was
    /// pushed back or released it, or a `start` job when it started (its `unique_for` was
    /// shorter than its wait, run or retries, or the lock was released by hand).
    pub lapsed: u64,
}

impl UniqueEventCounts {
    fn add(&mut self, event: &str, count: u64) {
        match event {
            "d" => self.duplicates += count,
            "r" => self.replaced += count,
            "z" => self.lapsed += count,
            _ => {}
        }
    }
}

/// The locks of unique jobs: [`Api::unique_locks`].
///
/// ```no_run
/// # async fn demo(api: hench::api::Api) -> hench::Result<()> {
/// use hench::api::SortOrder;
///
/// let locks = api.unique_locks();
/// for lock in locks.page(1, 25, SortOrder::Ascending).await?.items {
///     println!("{} held by {} until {:?}", lock.class, lock.jid, lock.expires_at);
/// }
/// // Who holds the lock a job with these arguments would take?
/// let holder = locks
///     .find_for("SyncAddress", Some("default"), &serde_json::json!([42]))
///     .await?;
/// # Ok(()) }
/// ```
#[derive(Clone)]
pub struct UniqueLocks {
    api: Api,
}

impl Api {
    /// The locks of unique jobs (see [`crate::unique`]).
    pub fn unique_locks(&self) -> UniqueLocks {
        UniqueLocks::new(self)
    }
}

impl std::fmt::Debug for UniqueLocks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UniqueLocks").finish_non_exhaustive()
    }
}

/// The score bound just after `secs` (exclusive), for `ZRANGE ... BYSCORE`.
fn after(secs: f64) -> String {
    format!("({secs}")
}

impl UniqueLocks {
    /// The locks of the Redis of `api`.
    pub fn new(api: impl Into<Api>) -> Self {
        Self { api: api.into() }
    }

    /// How many locks the index lists as live (expiring after now). Stale entries of locks
    /// released without reaching the index may be counted until they are trimmed.
    pub async fn size(&self) -> Result<u64> {
        let now = after(now_secs_f64());
        let now = &now;
        self.api
            .redis()
            .with_retry(|mut conn| async move {
                ::redis::cmd("ZCOUNT")
                    .arg(keys::UNIQUE_LOCKS)
                    .arg(now)
                    .arg("+inf")
                    .query_async(&mut conn)
                    .await
            })
            .await
    }

    /// One page of the live locks, soonest expiry first ([`SortOrder::Ascending`]) or latest
    /// first. Page numbers below 1 mean the first page; sizes are capped at 1000. An index
    /// entry whose lock is gone is skipped (and removed), so a page may be short; `total` is
    /// [`UniqueLocks::size`].
    pub async fn page(
        &self,
        page: usize,
        size: usize,
        order: SortOrder,
    ) -> Result<Page<UniqueLock>> {
        let (number, size) = (page.max(1), size.min(MAX_PAGE_SIZE));
        let total = self.size().await?;
        if size == 0 {
            return Ok(Page {
                items: Vec::new(),
                number,
                size,
                total,
            });
        }
        let offset = (number - 1).saturating_mul(size);
        let now = after(now_secs_f64());
        let (now, order) = (&now, order);
        let digests: Vec<String> = self
            .api
            .redis()
            .with_retry(|mut conn| async move {
                let mut command = ::redis::cmd("ZRANGE");
                command.arg(keys::UNIQUE_LOCKS);
                match order {
                    SortOrder::Ascending => command.arg(now).arg("+inf").arg("BYSCORE"),
                    SortOrder::Descending => command.arg("+inf").arg(now).arg("BYSCORE").arg("REV"),
                };
                command
                    .arg("LIMIT")
                    .arg(offset)
                    .arg(size)
                    .query_async(&mut conn)
                    .await
            })
            .await?;
        let items = self.read(&digests).await?;
        Ok(Page {
            items,
            number,
            size,
            total,
        })
    }

    /// Every live lock, soonest expiry first, read 100 at a time (a lock whose expiry is
    /// extended meanwhile may be seen twice or not at all).
    pub fn entries(&self) -> impl Stream<Item = Result<UniqueLock>> + Send + 'static {
        let this = self.clone();
        batched(Some(Cursor::live()), move |cursor| {
            let this = this.clone();
            async move {
                let Some(cursor) = cursor else {
                    return Ok(None);
                };
                let (digests, next) = this.next_batch(cursor, BATCH).await?;
                let locks = this.read(&digests).await?;
                Ok(Some((locks, next)))
            }
        })
    }

    /// The next `batch` index entries after `cursor`, and the cursor after them (`None` at the
    /// end).
    async fn next_batch(
        &self,
        cursor: Cursor,
        batch: usize,
    ) -> Result<(Vec<String>, Option<Cursor>)> {
        let from = match cursor.score {
            Some(score) => score.to_string(),
            None if cursor.all => "-inf".to_owned(),
            None => after(now_secs_f64()),
        };
        let skip = cursor.seen.len();
        let from = &from;
        let entries: Vec<(String, f64)> = self
            .api
            .redis()
            .with_retry(|mut conn| async move {
                ::redis::cmd("ZRANGE")
                    .arg(keys::UNIQUE_LOCKS)
                    .arg(from)
                    .arg("+inf")
                    .arg("BYSCORE")
                    .arg("LIMIT")
                    .arg(0)
                    .arg(batch + skip)
                    .arg("WITHSCORES")
                    .query_async(&mut conn)
                    .await
            })
            .await?;
        let exhausted = entries.len() < batch + skip;
        let fresh: Vec<(String, f64)> = entries
            .into_iter()
            .filter(|(digest, score)| {
                !(cursor.score == Some(*score) && cursor.seen.contains(digest))
            })
            .collect();
        let next = match fresh.last() {
            Some((_, last)) if !exhausted => {
                let mut seen: HashSet<String> = fresh
                    .iter()
                    .filter(|(_, score)| score == last)
                    .map(|(digest, _)| digest.clone())
                    .collect();
                if cursor.score == Some(*last) {
                    seen.extend(cursor.seen.iter().cloned());
                }
                Some(Cursor {
                    score: Some(*last),
                    seen,
                    all: cursor.all,
                })
            }
            _ => None,
        };
        Ok((fresh.into_iter().map(|(digest, _)| digest).collect(), next))
    }

    /// The locks of `digests`, in order, skipping (and removing from the index, at most
    /// [`PAGE_TRIM`] per call) the entries whose lock is gone.
    async fn read(&self, digests: &[String]) -> Result<Vec<UniqueLock>> {
        if digests.is_empty() {
            return Ok(Vec::new());
        }
        let mut pipe = ::redis::pipe();
        for digest in digests {
            let key = keys::unique_lock(digest);
            pipe.cmd("HGETALL").arg(&key).cmd("PTTL").arg(&key);
        }
        let pipe = &pipe;
        let replies: Vec<(HashMap<String, String>, i64)> = self
            .api
            .redis()
            .with_retry(|mut conn| async move { pipe.query_async(&mut conn).await })
            .await?;
        let now = SystemTime::now();
        let mut locks = Vec::with_capacity(digests.len());
        let mut stale = Vec::new();
        for (digest, (fields, pttl)) in digests.iter().zip(replies) {
            match UniqueLock::from_fields(digest, &fields, pttl, now) {
                Some(lock) => locks.push(lock),
                None => stale.push(digest.as_str()),
            }
        }
        stale.truncate(PAGE_TRIM);
        if let Err(error) = crate::unique::api::tidy(self.api.redis(), &stale).await {
            tracing::debug!(%error, "could not remove stale entries of the unique-lock index");
        }
        Ok(locks)
    }

    /// The lock of `digest`, if it exists.
    pub async fn get(&self, digest: &str) -> Result<Option<UniqueLock>> {
        if !valid_digest(digest) {
            return Ok(None);
        }
        let key = keys::unique_lock(digest);
        let key = &key;
        let (fields, pttl): (HashMap<String, String>, i64) = self
            .api
            .redis()
            .with_retry(|mut conn| async move {
                ::redis::pipe()
                    .cmd("HGETALL")
                    .arg(key)
                    .cmd("PTTL")
                    .arg(key)
                    .query_async(&mut conn)
                    .await
            })
            .await?;
        Ok(UniqueLock::from_fields(
            digest,
            &fields,
            pttl,
            SystemTime::now(),
        ))
    }

    /// The lock a typed push of `J` with `args` would take, if another job holds it (Sidekiq
    /// Enterprise's `locked?`): the digest is computed as such a push computes it, with the job
    /// type's queue and options over the default options of the API's client, then
    /// [`Job::unique_key`] or the arguments.
    pub async fn find<J: Job>(&self, args: &J::Args) -> Result<Option<UniqueLock>> {
        let defaults = self.api.client().default_job_options();
        let digest = crate::unique::digest_of::<J>(defaults, None, args)?;
        self.get(&digest).await
    }

    /// [`UniqueLocks::find`] for a push of `J` to `queue`.
    pub async fn find_in<J: Job>(&self, queue: &str, args: &J::Args) -> Result<Option<UniqueLock>> {
        let defaults = self.api.client().default_job_options();
        let digest = crate::unique::digest_of::<J>(defaults, Some(queue), args)?;
        self.get(&digest).await
    }

    /// The lock of a job of class `class` in `queue` (`None`: a lock across queues) whose key
    /// is `key` (its arguments as a JSON array, unless the job has a key of its own).
    pub async fn find_for(
        &self,
        class: &str,
        queue: Option<&str>,
        key: &Value,
    ) -> Result<Option<UniqueLock>> {
        let digest = crate::unique::digest(class, queue, key)?;
        self.get(&digest).await
    }

    /// Releases the lock of `digest` if `jid` holds it (an operator's release: the jid shown
    /// when the decision was made, so that a lock that changed hands meanwhile is left alone).
    /// `true` when it was released.
    pub async fn release(&self, digest: &str, jid: &str) -> Result<bool> {
        if !valid_digest(digest) || jid.is_empty() {
            return Ok(false);
        }
        let lock = LockRef {
            digest: digest.to_owned(),
            key: keys::unique_lock(digest),
            jid: jid.to_owned(),
            class: String::new(),
            queue: String::new(),
            until: UniqueUntil::Success,
            ttl: Duration::ZERO,
        };
        release_in_redis(self.api.redis(), &lock, false).await
    }

    /// Releases the lock of `digest` whoever holds it. `true` when there was one.
    pub async fn force_release(&self, digest: &str) -> Result<bool> {
        if !valid_digest(digest) {
            return Ok(false);
        }
        let released =
            crate::unique::api::release_entries(self.api.redis(), &[("", digest, "")]).await?;
        Ok(released > 0)
    }

    /// Releases each lock `(digest, jid)` that `jid` still holds (an operator's release of the
    /// holders shown when the decision was made), 100 per round trip. Entries with an invalid
    /// digest or an empty jid are skipped. Returns how many locks were released.
    pub async fn release_each(&self, locks: &[(&str, &str)]) -> Result<u64> {
        let entries: Vec<(&str, &str, &str)> = locks
            .iter()
            .filter(|(digest, jid)| valid_digest(digest) && !jid.is_empty())
            .map(|(digest, jid)| (*jid, *digest, ""))
            .collect();
        let mut released = 0;
        for chunk in entries.chunks(BATCH) {
            released += crate::unique::api::release_entries(self.api.redis(), chunk).await?;
        }
        Ok(released)
    }

    /// Releases every lock (of class `class`, if given), 100 at a time, whoever holds them.
    /// Returns how many were released. Locks taken meanwhile may be released or not.
    pub async fn release_all(&self, class: Option<&str>) -> Result<u64> {
        let class = class.unwrap_or_default();
        let mut released = 0;
        let mut cursor = Some(Cursor::everything());
        while let Some(current) = cursor {
            let (digests, next) = self.next_batch(current, BATCH).await?;
            let entries: Vec<(&str, &str, &str)> = digests
                .iter()
                .map(|digest| ("", digest.as_str(), class))
                .collect();
            released += crate::unique::api::release_entries(self.api.redis(), &entries).await?;
            cursor = next;
        }
        Ok(released)
    }

    /// Removes every index entry whose lock is gone (expired, or released by a script that
    /// could not reach the index), reading the whole index 1000 entries at a time; each lock is
    /// checked in the script that removes its entry, so a lock taken again meanwhile keeps it.
    /// Returns how many entries were removed. Pushes remove at most 10 expired entries each:
    /// this is for operators and tests after bursts.
    pub async fn trim(&self) -> Result<u64> {
        let mut removed = 0;
        let mut cursor = Some(Cursor::everything());
        while let Some(current) = cursor {
            let (digests, next) = self.next_batch(current, TRIM_BATCH).await?;
            let digests: Vec<&str> = digests.iter().map(String::as_str).collect();
            removed += crate::unique::api::tidy(self.api.redis(), &digests).await?;
            cursor = next;
        }
        Ok(removed)
    }

    /// Where the holder of `lock` is: running, in the schedule (found at once where the lock
    /// recorded its time), waiting for a retry, dead, or waiting in the lock's queue; each
    /// sorted set and the queue are searched for the jid up to [`LOCATE_LIMIT`] entries. An
    /// admin tool: a miss reads up to a few hundred pages. A job moving meanwhile may be missed.
    pub async fn locate(&self, lock: &UniqueLock) -> Result<HolderPlace> {
        let jid = lock.jid.as_str();
        if let Some(work) = self.api.work().find_work(jid).await? {
            return Ok(HolderPlace::Running(work));
        }
        let mut complete = true;
        let scheduled = self.api.scheduled().into_inner();
        let recorded = match lock.fields.get("at") {
            Some(at) => self.scheduled_at(&scheduled, at, jid).await?,
            None => None,
        };
        if let Some(entry) = recorded {
            return Ok(HolderPlace::Scheduled(entry));
        }
        let sets = [
            (self.api.retries().into_inner(), Place::Retrying),
            (self.api.dead().into_inner(), Place::Dead),
            (scheduled, Place::Scheduled),
        ];
        for (set, place) in sets {
            match self.search_set(&set, jid).await? {
                Search::Found(entry) => {
                    return Ok(match place {
                        Place::Retrying => HolderPlace::Retrying(entry),
                        Place::Dead => HolderPlace::Dead(entry),
                        Place::Scheduled => HolderPlace::Scheduled(entry),
                    });
                }
                Search::Missing(whole) => complete &= whole,
            }
        }
        if !lock.queue.is_empty() {
            match self.search_queue(&lock.queue, jid).await? {
                Search::Found((record, position)) => {
                    return Ok(HolderPlace::Queued { record, position });
                }
                Search::Missing(whole) => complete &= whole,
            }
        }
        Ok(HolderPlace::NotFound { complete })
    }

    /// The entry of `jid` in the schedule at the score `at` the lock recorded (as stored, so
    /// that it names the score exactly).
    async fn scheduled_at(&self, set: &JobSet, at: &str, jid: &str) -> Result<Option<SortedEntry>> {
        let Ok(score) = at.parse::<f64>() else {
            return Ok(None);
        };
        if !score.is_finite() {
            return Ok(None);
        }
        Ok(set.fetch(score, Some(jid)).await?.into_iter().next())
    }

    /// The entry of `jid` in `set`, scanning for its jid as written in JSON (`ZSCAN ... MATCH`)
    /// up to [`LOCATE_LIMIT`] entries.
    async fn search_set(&self, set: &JobSet, jid: &str) -> Result<Search<SortedEntry>> {
        let pattern = literal_pattern(&json_escaped(jid));
        let (key, pattern) = (set.name(), pattern.as_str());
        let mut cursor: u64 = 0;
        let mut scanned: u64 = 0;
        loop {
            let (next, members): (u64, Vec<(String, f64)>) = self
                .api
                .redis()
                .with_retry(|mut conn| async move {
                    ::redis::cmd("ZSCAN")
                        .arg(key)
                        .arg(cursor)
                        .arg("MATCH")
                        .arg(pattern)
                        .arg("COUNT")
                        .arg(LOCATE_PAGE)
                        .query_async(&mut conn)
                        .await
                })
                .await?;
            for (_, score) in members {
                // The member names the jid somewhere; the entry must be the job itself.
                if let Some(entry) = set.fetch(score, Some(jid)).await?.into_iter().next() {
                    return Ok(Search::Found(entry));
                }
            }
            scanned += LOCATE_PAGE as u64;
            if next == 0 {
                return Ok(Search::Missing(true));
            }
            if scanned >= LOCATE_LIMIT {
                return Ok(Search::Missing(false));
            }
            cursor = next;
        }
    }

    /// The job `jid` in `queue` with its place in line, reading the queue from the next job to
    /// run, up to [`LOCATE_LIMIT`] jobs.
    async fn search_queue(&self, queue: &str, jid: &str) -> Result<Search<(JobRecord, u64)>> {
        let queue = self.api.queue(queue);
        let needle = json_escaped(jid);
        let mut number = 1;
        loop {
            let page = queue
                .page(number, LOCATE_PAGE, SortOrder::Ascending)
                .await?;
            let start = (number as u64 - 1) * LOCATE_PAGE as u64;
            let read = page.items.len();
            for (index, record) in page.items.into_iter().enumerate() {
                if record.value().contains(&needle) && record.jid() == Some(jid) {
                    return Ok(Search::Found((record, start + index as u64 + 1)));
                }
            }
            let end = start + read as u64;
            if read < LOCATE_PAGE || end >= page.total {
                return Ok(Search::Missing(true));
            }
            if end >= LOCATE_LIMIT {
                return Ok(Search::Missing(false));
            }
            number += 1;
        }
    }

    /// The duplicates, replaced holders and lapses counted over `period`, per class: minute
    /// periods of at most 8 hours (`Period::Minutes(n)`, `n` ≤ 480), the life of the
    /// per-minute counters; any other period is an [`Error::Config`](crate::Error::Config).
    pub async fn events(&self, period: Period) -> Result<UniqueEvents> {
        let events = read_events(self.api.redis(), EventKind::Unique, period, Utc::now()).await?;
        let mut summary = UniqueEvents::default();
        for ((class, event), count) in events.totals {
            summary.totals.add(&event, count);
            summary
                .by_class
                .entry(class)
                .or_default()
                .add(&event, count);
        }
        Ok(summary)
    }
}

/// The sorted sets [`UniqueLocks::locate`] searches.
#[derive(Clone, Copy)]
enum Place {
    Retrying,
    Dead,
    Scheduled,
}

/// Where an iteration of the index is: after the entries `seen` at `score` (`None`: from the
/// start: the first live entry, or the first entry of all with `all`).
#[derive(Debug, Clone, Default)]
struct Cursor {
    score: Option<f64>,
    seen: HashSet<String>,
    all: bool,
}

impl Cursor {
    /// From the first entry expiring after now.
    fn live() -> Self {
        Self::default()
    }

    /// From the first entry, expired ones included.
    fn everything() -> Self {
        Self {
            all: true,
            ..Self::default()
        }
    }
}

impl JobRecord {
    /// The uniqueness of this job (its lock's digest, policy and `unique_for`), read leniently
    /// from its `unique` field: `None` when the job is not unique or the field is malformed.
    pub fn unique(&self) -> Option<UniqueRef> {
        let value = self.get("unique")?;
        let unique = serde_json::from_value::<UniqueOption>(value.clone()).ok()?;
        let unique = unique.settings()?;
        let digest = unique
            .digest
            .clone()
            .filter(|digest| valid_digest(digest))?;
        Some(UniqueRef {
            digest,
            until: unique.until.unwrap_or_default(),
            unique_for: unique.ttl?,
        })
    }
}
