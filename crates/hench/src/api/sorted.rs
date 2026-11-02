//! Sorted sets of jobs: [`JobSet`] and its entries ([`SortedEntry`]), and the three sets
//! hench maintains: [`ScheduledSet`], [`RetrySet`] and [`DeadSet`] (the morgue).

use std::collections::HashSet;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::SystemTime;

use ::redis::Script;
use futures::FutureExt;
use futures::stream::{FuturesUnordered, Stream, StreamExt, TryStreamExt};

use crate::api::page::{normalize, sorted_set_page};
use crate::api::{
    Api, JobRecord, Page, SortOrder, batched, json_escaped, literal_pattern, match_pattern,
};
use crate::batch::api::Removal as BatchRemoval;
use crate::client::Stored;
use crate::error::{Error, JobError, Result, panic_message};
use crate::keys;
use crate::lua::{LuaScript, PRELUDE};
use crate::payload::{
    JobPayload, now_millis, now_secs_f64, secs_to_system_time, system_time_to_secs,
};
use crate::redis::RedisPool;
use crate::unique::{self, LockRef};

/// Entries fetched per round trip while iterating a set.
const ITERATION_PAGE: i64 = 50;

/// Entries read per round trip by [`JobSet::fetch_range`].
const FETCH_PAGE: u64 = 500;

/// Jobs [`JobSet::retry_all`] moves at the same time.
const RETRY_ALL_CONCURRENCY: usize = 16;

/// Entries examined per `ZSCAN` call.
const SCAN_COUNT: usize = 100;

/// Jobs moved per round trip by [`JobSet::retry_all`] and [`JobSet::kill_all`].
const BULK_BATCH: u64 = 100;

/// Moves one member from a sorted set to the morgue, atomically: nothing happens unless the
/// member is in the source set (so a job is never both run and dead). Then trims the morgue by
/// age and size. The write that can fail (when Redis is out of memory) comes before the
/// removal, and Redis does not roll scripts back, so a failure leaves the job where it was.
///
/// KEYS: source set, morgue. ARGV: member, now, oldest score kept, rank bound of the size trim.
/// Returns 1 if the job moved, 2 if it was refreshed in place (the source is the morgue), 0 if
/// it was not there. Also used by the scheduler for jobs that cannot be enqueued.
pub(crate) static MOVE_TO_MORGUE: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
if not redis.call('ZSCORE', KEYS[1], ARGV[1]) then
  return 0
end
redis.call('ZADD', KEYS[2], ARGV[2], ARGV[1])
local moved = 2
if KEYS[1] ~= KEYS[2] then
  redis.call('ZREM', KEYS[1], ARGV[1])
  moved = 1
end
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', ARGV[3])
redis.call('ZREMRANGEBYRANK', KEYS[2], 0, ARGV[4])
return moved
",
    )
});

/// Moves a job out of a sorted set, atomically, if it is still there: pushes it to its queue,
/// schedules it, or only removes it. The writes that can fail (for example when Redis is out of
/// memory) come before the removal, so a failure leaves the job where it was.
///
/// KEYS: the source set, the target (queue list, or schedule set), the `queues` set. ARGV: the
/// member, the mode (`queue`, `schedule` or `drop`), the job as pushed, and the queue name
/// (`queue`) or score (`schedule`). Returns 1 if the job was moved out of the set, 2 if it was
/// rescheduled within it (the source is the schedule), 0 if it was not there. Also used by the
/// scheduler to enqueue due jobs.
pub(crate) static MOVE_OUT: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
if not redis.call('ZSCORE', KEYS[1], ARGV[1]) then
  return 0
end
if ARGV[2] == 'queue' then
  redis.call('SADD', KEYS[3], ARGV[4])
  redis.call('LPUSH', KEYS[2], ARGV[3])
elseif ARGV[2] == 'schedule' then
  redis.call('ZADD', KEYS[2], ARGV[4], ARGV[3])
  if KEYS[2] == KEYS[1] then
    if ARGV[3] ~= ARGV[1] then
      redis.call('ZREM', KEYS[1], ARGV[1])
    end
    return 2
  end
end
redis.call('ZREM', KEYS[1], ARGV[1])
return 1
",
    )
});

/// RESCORE_UNIQUE: moves a member of a sorted set (the schedule or the retry set) to a new
/// score, if it is still there (`ZADD XX`), and extends its job's unique lock to cover the wait:
/// only while the job holds it (a lapsed lock is not taken back, another job's is left alone),
/// never shortened (`for` plus the new delay), with the job's place in the schedule recorded
/// (`at`, and its member there) for a debounce push to find it, or cleared in the retry set (a
/// retry is never replaced). Assembled after the prelude and the unique library.
///
/// KEYS: [1] the sorted set, [2] the lock, [3] unique-locks. ARGV: [1] the member, [2] the new
/// score, [3] jid, [4] digest, [5] `for` (ms), [6] `1` when the set is the schedule.
/// Returns {1 moved, or 0 not in the set (nothing done); the lock: 1 held (extended or kept),
/// 0 not held by this job, -2 the lock key could not be used}.
const RESCORE_UNIQUE_BODY: &str = r"
local now_ms = h_now_ms()
if not redis.call('ZSCORE', KEYS[1], ARGV[1]) then
  return {0, 0}
end
redis.call('ZADD', KEYS[1], 'XX', ARGV[2], ARGV[1])
local ok, state = pcall(hu_check, KEYS[2], ARGV[3])
if not ok then return {1, -2} end
if state ~= 'self' then return {1, 0} end
local ttl = tonumber(ARGV[5])
local delay = math.floor(tonumber(ARGV[2]) * 1000) - now_ms
if delay > 0 then ttl = ttl + delay end
local record, fields = '', {}
if ARGV[6] == '1' then
  record = ARGV[2]
  fields = {'m', ARGV[1]}
end
local held, status = pcall(hu_hold, KEYS[2], KEYS[3], ARGV[4], ARGV[3], ttl, now_ms, fields, record)
if not held then status = -2 end
return {1, status}
";

/// [`RESCORE_UNIQUE_BODY`], assembled: the scheduler's deferral of a due job whose push failed
/// and [`SortedEntry::reschedule`] use it for jobs holding a lock that follows them.
pub(crate) static RESCORE_UNIQUE: LuaScript = LuaScript::new(
    "RESCORE_UNIQUE",
    &[PRELUDE, unique::lua::LIB, RESCORE_UNIQUE_BODY],
);

/// The lock of `job` that a move to a later time extends: one that follows the job (every
/// policy but `expired`).
pub(crate) fn lock_to_extend(job: &JobPayload) -> Option<LockRef> {
    LockRef::from_payload_quietly(job).filter(LockRef::follows_the_job)
}

/// Moves `member` of `set` (the schedule or the retry set) to `score` and extends its job's
/// lock `lock` to cover the new wait while the job holds it ([`RESCORE_UNIQUE`]): idempotent, so
/// a resend after a dropped connection is harmless. Returns whether the member was in the set.
pub(crate) async fn rescore_holding(
    redis: &RedisPool,
    set: &str,
    member: &[u8],
    score: f64,
    lock: &LockRef,
) -> Result<bool> {
    let schedule = if set == keys::SCHEDULE { "1" } else { "" };
    let (moved, status): (i64, i64) = redis
        .with_retry(|mut conn| async move {
            RESCORE_UNIQUE
                .script()
                .key(set)
                .key(&lock.key)
                .key(keys::UNIQUE_LOCKS)
                .arg(member)
                .arg(score)
                .arg(&lock.jid)
                .arg(&lock.digest)
                .arg(lock.ttl_ms())
                .arg(schedule)
                .invoke_async(&mut conn)
                .await
        })
        .await?;
    if moved == 1 && status != 1 {
        tracing::debug!(
            jid = %lock.jid,
            digest = %lock.digest,
            status,
            "moved a job whose unique lock it no longer holds; the lock was left alone"
        );
    }
    Ok(moved == 1)
}

/// SCAN_CONTAINING (read only): the members of a sorted set from rank ARGV[1] to rank ARGV[2]
/// (by score) that contain the text ARGV[3] (a plain substring, no pattern), each followed by
/// its score; and how many members those ranks held. Redis does the matching, so only the
/// matches travel.
///
/// KEYS: [1] the sorted set. Returns {members read, {member, score, member, score, ...}}.
const SCAN_CONTAINING_BODY: &str = r"
local members = redis.call('ZRANGE', KEYS[1], ARGV[1], ARGV[2], 'WITHSCORES')
local found = {}
for i = 1, #members, 2 do
  if string.find(members[i], ARGV[3], 1, true) then
    found[#found + 1] = members[i]
    found[#found + 1] = members[i + 1]
  end
end
return {math.floor(#members / 2), found}
";

/// [`SCAN_CONTAINING_BODY`], assembled.
pub(crate) static SCAN_CONTAINING: LuaScript =
    LuaScript::new("SCAN_CONTAINING", &[PRELUDE, SCAN_CONTAINING_BODY]);

/// Members [`JobSet::first_containing`] reads per call of [`SCAN_CONTAINING`]: each call keeps
/// Redis busy for about 1.7 ms with jobs of a kilobyte (about 3.4 µs of Redis CPU per job read,
/// some three times a plain `ZRANGE` of the job; nothing but the matches is sent).
const SCAN_CHUNK: usize = 500;

/// Moves up to `ARGV[1]` of the lowest-scored members of `KEYS[1]` into `KEYS[2]` with score
/// `ARGV[2]`, atomically, and returns them. As above, the members are added before they are
/// removed, so a failed write leaves them where they were.
pub(crate) static MOVE_LOWEST: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
local members = redis.call('ZRANGE', KEYS[1], 0, tonumber(ARGV[1]) - 1)
for _, member in ipairs(members) do
  redis.call('ZADD', KEYS[2], ARGV[2], member)
end
if KEYS[1] ~= KEYS[2] and #members > 0 then
  redis.call('ZREM', KEYS[1], unpack(members))
end
return members
",
    )
});

/// A sorted set of jobs scored by time (Sidekiq's `JobSet`): the scheduled jobs, the retries,
/// the morgue, or any other set holding job JSON.
///
/// Operations that take a job out of the set and push it to its queue
/// ([`SortedEntry::retry`], [`SortedEntry::add_to_queue`], [`JobSet::retry_all`]) first run
/// the client's validation and middleware, then move the job in one atomic step: a job is never
/// lost or pushed twice by them, whatever fails and even if the caller stops waiting.
#[derive(Clone)]
pub struct JobSet {
    api: Api,
    name: Arc<str>,
    /// Entries removed through this handle and the entries it produced. Only read by the
    /// iteration that created the handle (see [`JobSet::entries`]).
    removed: Arc<AtomicU64>,
}

impl JobSet {
    /// The set stored under the key `name`.
    pub fn new(api: impl Into<Api>, name: impl Into<String>) -> Self {
        Self {
            api: api.into(),
            name: Arc::from(name.into()),
            removed: Arc::default(),
        }
    }

    /// The set's Redis key.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The API handle.
    pub fn api(&self) -> &Api {
        &self.api
    }

    /// The number of jobs in the set.
    pub async fn size(&self) -> Result<u64> {
        let key = self.name();
        self.api
            .redis()
            .with_retry(|mut conn| async move {
                ::redis::cmd("ZCARD").arg(key).query_async(&mut conn).await
            })
            .await
    }

    fn entry(&self, value: String, score: f64) -> SortedEntry {
        SortedEntry {
            record: JobRecord::new(&self.api, value, None),
            score,
            set: self.clone(),
        }
    }

    fn entries_from(&self, members: Vec<(String, f64)>) -> Vec<SortedEntry> {
        members
            .into_iter()
            .map(|(value, score)| self.entry(value, score))
            .collect()
    }

    fn note_removed(&self, count: u64) {
        self.removed.fetch_add(count, Ordering::Relaxed);
    }

    /// The entries among the `scan` earliest (by score) whose stored JSON contains `text` (a
    /// plain substring) and that `keep` accepts, earliest first, at most `max`. Redis does the
    /// matching ([`SCAN_CONTAINING`], [`SCAN_CHUNK`] entries per call), so only the candidates
    /// travel; `keep` checks each one parsed. Entries moved meanwhile may be missed or seen
    /// once more (each is returned once), as when paging.
    pub(crate) async fn first_containing(
        &self,
        text: &str,
        scan: usize,
        max: usize,
        keep: impl Fn(&SortedEntry) -> bool,
    ) -> Result<Vec<SortedEntry>> {
        let mut found: Vec<SortedEntry> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let mut start = 0;
        while start < scan && found.len() < max {
            let stop = (start + SCAN_CHUNK).min(scan) - 1;
            let key = self.name();
            let (read, matches): (usize, Vec<String>) = self
                .api
                .redis()
                .with_retry(|mut conn| async move {
                    SCAN_CONTAINING
                        .script()
                        .key(key)
                        .arg(start)
                        .arg(stop)
                        .arg(text)
                        .invoke_async(&mut conn)
                        .await
                })
                .await?;
            for pair in matches.as_chunks::<2>().0 {
                let (value, score) = (&pair[0], &pair[1]);
                let Ok(score) = score.parse::<f64>() else {
                    continue;
                };
                if !seen.insert(value.clone()) {
                    continue;
                }
                let entry = self.entry(value.clone(), score);
                if keep(&entry) {
                    found.push(entry);
                    if found.len() >= max {
                        break;
                    }
                }
            }
            if read <= stop - start {
                // The set ended before `stop`.
                break;
            }
            start = stop + 1;
        }
        Ok(found)
    }

    /// One page of entries (numbered from 1; at most 1000 per page), by score:
    /// [`SortOrder::Ascending`] lists the earliest first.
    pub async fn page(
        &self,
        page: usize,
        page_size: usize,
        order: SortOrder,
    ) -> Result<Page<SortedEntry>> {
        let (number, size) = normalize(page, page_size);
        let (total, members) = sorted_set_page(&self.api, self.name(), number, size, order).await?;
        Ok(Page {
            items: self.entries_from(members),
            number,
            size,
            total,
        })
    }

    /// Every entry, latest first, fetched 50 at a time (Sidekiq's `JobSet#each`). The stream is
    /// not `Unpin`: pin it to read it (`std::pin::pin!`, see the [module docs](super)).
    ///
    /// Removing the yielded entries while iterating ([`SortedEntry::delete`],
    /// [`SortedEntry::add_to_queue`], ...) does not make the iteration skip others: each page
    /// is offset by the number of yielded entries removed so far. Other changes, including
    /// [`SortedEntry::reschedule`] of a yielded entry to an earlier time, can make an entry
    /// appear twice or be missed, as with any live data: collect the entries first to
    /// reschedule them. The iteration reads at
    /// most as many pages as the set had when it started, so it ends even if the set keeps
    /// growing.
    pub fn entries(&self) -> impl Stream<Item = Result<SortedEntry>> + Send + 'static {
        struct State {
            /// A handle of its own, so that only removals of yielded entries are counted.
            set: JobSet,
            page: i64,
            pages_left: Option<i64>,
        }
        let state = State {
            set: JobSet {
                api: self.api.clone(),
                name: Arc::clone(&self.name),
                removed: Arc::default(),
            },
            page: -1,
            pages_left: None,
        };
        batched(state, |mut state| async move {
            let set = &state.set;
            let pages_left = match state.pages_left {
                Some(pages) => pages,
                None => {
                    let pages = set.size().await?.div_ceil(ITERATION_PAGE.unsigned_abs());
                    i64::try_from(pages).unwrap_or(i64::MAX)
                }
            };
            if pages_left <= 0 {
                return Ok(None);
            }
            let removed = set.removed.load(Ordering::Relaxed);
            let offset = i64::try_from(removed).unwrap_or(i64::MAX);
            let end = state
                .page
                .saturating_mul(ITERATION_PAGE)
                .saturating_add(ITERATION_PAGE - 1)
                .saturating_add(offset)
                .min(-1);
            let start = end.saturating_sub(ITERATION_PAGE - 1);
            let key = set.name();
            let mut members: Vec<(String, f64)> = set
                .api
                .redis()
                .with_retry(|mut conn| async move {
                    ::redis::cmd("ZRANGE")
                        .arg(key)
                        .arg(start)
                        .arg(end)
                        .arg("WITHSCORES")
                        .query_async(&mut conn)
                        .await
                })
                .await?;
            if members.is_empty() {
                return Ok(None);
            }
            members.reverse();
            let entries = set.entries_from(members);
            state.page -= 1;
            state.pages_left = Some(pages_left - 1);
            Ok(Some((entries, state)))
        })
    }

    /// The entries whose JSON matches `pattern`, read with `ZSCAN … MATCH` (in no particular
    /// order). A pattern containing `*` is a Redis glob; anything else matches entries
    /// containing it as a substring. Each entry is yielded once. Pin the stream to read it
    /// (see the [module docs](super)).
    pub fn scan(&self, pattern: &str) -> impl Stream<Item = Result<SortedEntry>> + Send + 'static {
        self.scan_matching(match_pattern(pattern))
    }

    /// [`JobSet::scan`] with a ready `MATCH` pattern.
    fn scan_matching(
        &self,
        pattern: String,
    ) -> impl Stream<Item = Result<SortedEntry>> + Send + 'static {
        struct State {
            set: JobSet,
            pattern: String,
            cursor: Option<u64>,
            /// Hashes of the entries yielded so far (`ZSCAN` may return an entry twice).
            seen: HashSet<u64>,
        }
        let state = State {
            set: self.clone(),
            pattern,
            cursor: None,
            seen: HashSet::new(),
        };
        batched(state, |mut state| async move {
            let cursor = match state.cursor {
                Some(0) => return Ok(None),
                Some(cursor) => cursor,
                None => 0,
            };
            let (key, pattern) = (state.set.name(), state.pattern.as_str());
            let (next, members): (u64, Vec<(String, f64)>) = state
                .set
                .api
                .redis()
                .with_retry(|mut conn| async move {
                    ::redis::cmd("ZSCAN")
                        .arg(key)
                        .arg(cursor)
                        .arg("MATCH")
                        .arg(pattern)
                        .arg("COUNT")
                        .arg(SCAN_COUNT)
                        .query_async(&mut conn)
                        .await
                })
                .await?;
            let fresh: Vec<(String, f64)> = members
                .into_iter()
                .filter(|(value, _)| state.seen.insert(hash_of(value)))
                .collect();
            let entries = state.set.entries_from(fresh);
            state.cursor = Some(next);
            Ok(Some((entries, state)))
        })
    }

    /// The entries scored exactly `score` (with this jid, if given).
    pub async fn fetch(&self, score: f64, jid: Option<&str>) -> Result<Vec<SortedEntry>> {
        self.fetch_range(score, score, jid).await
    }

    /// The entries scored between `min` and `max` inclusive (with this jid, if given), lowest
    /// first, read 500 at a time. A wide range reads that many entries: use pages or
    /// iteration to browse a set.
    pub async fn fetch_range(
        &self,
        min: f64,
        max: f64,
        jid: Option<&str>,
    ) -> Result<Vec<SortedEntry>> {
        let mut found = Vec::new();
        // Pages continue from the last score read, skipping the entries already read at
        // that score: removals elsewhere in the set cannot shift the pages.
        let (mut from, mut read_at_from) = (ScoreBound::Inclusive(min), 0);
        loop {
            let page = self
                .range_by_score(from, ScoreBound::Inclusive(max), read_at_from, FETCH_PAGE)
                .await?;
            let Some(&(_, last)) = page.last() else {
                return Ok(found);
            };
            let full = page.len() as u64 == FETCH_PAGE;
            let at_last = page
                .iter()
                .rev()
                .take_while(|(_, score)| *score == last)
                .count() as u64;
            read_at_from = if from == ScoreBound::Inclusive(last) {
                read_at_from + at_last
            } else {
                at_last
            };
            from = ScoreBound::Inclusive(last);
            found.extend(
                self.entries_from(page)
                    .into_iter()
                    .filter(|entry| jid.is_none_or(|jid| entry.jid() == Some(jid))),
            );
            if !full {
                return Ok(found);
            }
        }
    }

    /// `ZRANGE key <from> <to> BYSCORE LIMIT <skip> <count> WITHSCORES`.
    async fn range_by_score(
        &self,
        from: ScoreBound,
        to: ScoreBound,
        skip: u64,
        count: u64,
    ) -> Result<Vec<(String, f64)>> {
        let (key, from, to) = (self.name(), from.to_string(), to.to_string());
        let (from, to) = (from.as_str(), to.as_str());
        self.api
            .redis()
            .with_retry(|mut conn| async move {
                ::redis::cmd("ZRANGE")
                    .arg(key)
                    .arg(from)
                    .arg(to)
                    .arg("BYSCORE")
                    .arg("LIMIT")
                    .arg(skip)
                    .arg(count)
                    .arg("WITHSCORES")
                    .query_async(&mut conn)
                    .await
            })
            .await
    }

    /// The entry with this jid, if any. Scans the set (`ZSCAN` matching the jid): an O(n)
    /// operation for admin tools, not for application logic.
    pub async fn find_job(&self, jid: &str) -> Result<Option<SortedEntry>> {
        if jid.is_empty() {
            return Ok(None);
        }
        // Members are JSON: look for the jid as it is written there, literally.
        let mut matches = std::pin::pin!(self.scan_matching(literal_pattern(&json_escaped(jid))));
        while let Some(entry) = matches.try_next().await? {
            if entry.jid() == Some(jid) {
                return Ok(Some(entry));
            }
        }
        Ok(None)
    }

    /// Removes the entry whose JSON is exactly `value`. Returns `false` if it was not there.
    ///
    /// A batch member counts as dead (deleted) in its batch first; a unique job's lock is
    /// released by the script that removes it.
    pub async fn delete_by_value(&self, value: &str) -> Result<bool> {
        let featured = super::featured_job(value);
        let jobs = featured.as_slice();
        let reports = super::before_removal(&self.api, jobs, BatchRemoval::Deleted).await;
        let unique = match &featured {
            Some(job) => {
                unique::api::delete_from_set(self.api.redis(), self.name(), value, job).await
            }
            None => None,
        };
        let removed = match unique {
            Some(removed) => removed?,
            None => match self.remove_once(value).await {
                Removal::Removed | Removal::Gone => true,
                Removal::Absent => false,
                Removal::Failed(error) | Removal::Unknown(error) => return Err(error),
            },
        };
        if removed {
            self.note_removed(1);
        }
        super::after_removal(&self.api, reports).await;
        Ok(removed)
    }

    /// Removes the entry scored `score` whose jid is `jid`. Returns `false` if there is none.
    pub async fn delete_by_jid(&self, score: f64, jid: &str) -> Result<bool> {
        for entry in self.fetch(score, Some(jid)).await? {
            if self.delete_by_value(entry.value()).await? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Deletes every entry.
    pub async fn clear(&self) -> Result<()> {
        let key = self.name();
        self.api
            .redis()
            .with_retry(|mut conn| async move {
                ::redis::cmd("UNLINK").arg(key).exec_async(&mut conn).await
            })
            .await
    }

    /// Adds `job` to the set, scored `at` (Sidekiq's `JobSet#schedule`). Like the client does
    /// for scheduled jobs, its `at` and `enqueued_at` fields are not stored: the score is the
    /// time, and the job is not enqueued.
    pub async fn schedule(&self, at: SystemTime, job: &JobPayload) -> Result<()> {
        let mut job = job.clone();
        job.at = None;
        job.enqueued_at = None;
        let (key, json, score) = (self.name(), job.to_json()?, system_time_to_secs(at));
        let json = json.as_str();
        self.api
            .redis()
            .with_retry(|mut conn| async move {
                ::redis::cmd("ZADD")
                    .arg(key)
                    .arg(score)
                    .arg(json)
                    .exec_async(&mut conn)
                    .await
            })
            .await
    }

    /// Pushes every job of the set back to its queue through the client (so client middleware
    /// runs), and returns how many were pushed. Like a manual [`SortedEntry::retry`], this
    /// gives back the retry it uses (`retry_count` is decremented), with the same caveat: a job
    /// whose `retry_count` is 0 (one failure so far) keeps 0, so it gets one automatic retry
    /// fewer than if it had not been retried by hand.
    ///
    /// Jobs are taken in score order, 16 at a time, so jobs of the same batch can reach their
    /// queues in any order. Each job moves atomically (see [`JobSet`]): concurrent calls never
    /// push a job twice and no job is ever lost. A job that cannot be parsed, whose push is
    /// rejected (client validation, an `Err` from client middleware, a Redis error about that
    /// job) or whose client middleware panics stays in the set, with a warning, and the call
    /// goes on. A job whose push client middleware stops (`Ok(None)`) is dropped, as in Sidekiq
    /// (with a warning; a batch member or a unique job as a delete removes it: counted dead in
    /// its batch, its lock released), and not counted. If Redis itself fails (connection, failover, out of
    /// memory, ...) the call stops, once the moves in flight are done, and returns the error.
    /// At most as many jobs as the set held when the call started are examined.
    pub async fn retry_all(&self) -> Result<u64> {
        self.requeue_all(Requeue::Retry).await
    }

    /// Pushes every job of the set to its queue now, through the client, and returns how many
    /// were pushed (Sidekiq's "add all to queue" of the scheduled set): like
    /// [`retry_all`](Self::retry_all), with the same guarantees, but retry counts are left as
    /// they are, as [`SortedEntry::add_to_queue`] does for one job.
    pub async fn add_all_to_queue(&self) -> Result<u64> {
        self.requeue_all(Requeue::Enqueue).await
    }

    /// [`retry_all`](Self::retry_all) and [`add_all_to_queue`](Self::add_all_to_queue).
    async fn requeue_all(&self, how: Requeue) -> Result<u64> {
        let mut budget = self.size().await?;
        let mut pushed = 0;
        // Jobs below `from` were handled (and the ones left in place are skipped this way).
        let mut from = ScoreBound::Inclusive(f64::NEG_INFINITY);
        while budget > 0 {
            let count = budget.min(BULK_BATCH);
            let batch = self
                .range_by_score(from, ScoreBound::Inclusive(f64::INFINITY), 0, count)
                .await?;
            let Some(&(_, last)) = batch.last() else {
                break;
            };
            let full = batch.len() as u64 == count;
            if full && batch.iter().all(|(_, score)| *score == last) {
                // A single score fills the batch: go through its jobs on their own.
                pushed += self.retry_score(last, &mut budget, how).await?;
            } else {
                // Unless this is the end of the set, the jobs at the last score may continue
                // past the batch: leave them for the next round.
                let batch: Vec<(String, f64)> = if full {
                    batch
                        .into_iter()
                        .filter(|(_, score)| *score < last)
                        .collect()
                } else {
                    batch
                };
                budget = budget.saturating_sub(batch.len() as u64);
                pushed += self.retry_entries(batch, how).await?.pushed;
                if !full {
                    // The end of the set.
                    break;
                }
                from = ScoreBound::Inclusive(last);
                continue;
            }
            from = ScoreBound::Exclusive(last);
        }
        Ok(pushed)
    }

    /// [`retry_all`](Self::retry_all) for the jobs scored exactly `score`, by rank: the jobs
    /// left in place come first, so each page starts after them. (If one of those leaves the
    /// set meanwhile, a job of this score may be passed over; it stays in the set.)
    async fn retry_score(&self, score: f64, budget: &mut u64, how: Requeue) -> Result<u64> {
        let (mut pushed, mut left) = (0, 0);
        while *budget > 0 {
            let count = (*budget).min(BULK_BATCH);
            let page = self
                .range_by_score(
                    ScoreBound::Inclusive(score),
                    ScoreBound::Inclusive(score),
                    left,
                    count,
                )
                .await?;
            let short = (page.len() as u64) < count;
            *budget = budget.saturating_sub(page.len() as u64);
            let outcome = self.retry_entries(page, how).await?;
            pushed += outcome.pushed;
            left += outcome.left;
            if short {
                break;
            }
        }
        Ok(pushed)
    }

    /// Moves `entries` back to their queues, 16 at a time. A failure of Redis itself stops
    /// the moves (the ones in flight finish first) and is returned; anything else only
    /// concerns that job, which stays in the set.
    async fn retry_entries(
        &self,
        entries: Vec<(String, f64)>,
        how: Requeue,
    ) -> Result<RetryOutcome> {
        let mut outcome = RetryOutcome::default();
        let mut moves = Vec::with_capacity(entries.len());
        for (value, _) in entries {
            match how.payload(&value) {
                Ok(payload) => moves.push(self.move_job(value, payload)),
                Err(error) => {
                    tracing::warn!(set = self.name(), %error, "a job cannot be parsed; it stays in the set");
                    outcome.left += 1;
                }
            }
        }
        let mut pending = moves.into_iter();
        let mut in_flight = FuturesUnordered::new();
        let mut failure = None;
        loop {
            while failure.is_none() && in_flight.len() < RETRY_ALL_CONCURRENCY {
                let Some(next) = pending.next() else { break };
                in_flight.push(next);
            }
            let Some(moved) = in_flight.next().await else {
                break;
            };
            match moved {
                Ok(Moved::Pushed) => outcome.pushed += 1,
                Ok(Moved::Dropped | Moved::Missing | Moved::Gone) => {}
                Ok(Moved::Rejected(error)) | Err(error) if is_unavailable(&error) => {
                    failure.get_or_insert(error);
                }
                Ok(Moved::Panicked(panic)) => {
                    tracing::error!(
                        set = self.name(),
                        panic = %panic_message(panic.as_ref()),
                        "client middleware panicked while pushing a job; it stays in the set"
                    );
                    outcome.left += 1;
                }
                Ok(Moved::Rejected(error)) | Err(error) => {
                    tracing::warn!(set = self.name(), %error, "could not move a job; it stays in the set");
                    outcome.left += 1;
                }
            }
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(outcome),
        }
    }

    /// `ZREM` of one job, sent once: repeated after a dropped connection it would answer 0
    /// for a job it had removed. If it fails, the job's presence tells what happened.
    async fn remove_once(&self, value: &str) -> Removal {
        let key = self.name();
        let attempt: Result<u64> = async {
            let mut conn = self.api.redis().conn().await?;
            Ok(::redis::cmd("ZREM")
                .arg(key)
                .arg(value)
                .query_async(&mut conn)
                .await?)
        }
        .await;
        let error = match attempt {
            Ok(removed) if removed > 0 => return Removal::Removed,
            Ok(_) => return Removal::Absent,
            Err(error) => error,
        };
        match self.contains(value).await {
            Some(true) => Removal::Failed(error),
            Some(false) => Removal::Gone,
            None => Removal::Unknown(error),
        }
    }

    /// Moves the job `value` out of the set and pushes `payload` (its parsed form): first the
    /// client's validation and middleware run (nothing changes in Redis if they reject, stop
    /// or panic), then one script moves the job to its queue (or the schedule, if middleware
    /// scheduled it) if it is still in the set.
    ///
    /// The script is sent once: repeated after a dropped connection, it would find the job
    /// gone and report it missing. If it fails, the job's presence tells whether it moved.
    async fn move_job(&self, value: String, payload: JobPayload) -> Result<Moved> {
        let (class, jid) = (payload.class.clone(), payload.jid.clone());
        let prepared = AssertUnwindSafe(self.api.client().prepare_push(payload))
            .catch_unwind()
            .await;
        let target = match prepared {
            Ok(Ok(Some(job))) => Target::of(job)?,
            Ok(Ok(None)) => Target::Drop,
            Ok(Err(error)) => return Ok(Moved::Rejected(error)),
            Err(panic) => return Ok(Moved::Panicked(panic)),
        };
        if let Target::Drop = target
            && super::featured_job(&value).is_some()
        {
            // A job of a feature dropped by client middleware is deleted as the API deletes
            // one: a batch member counts dead in its batch, a unique job releases its lock.
            return Ok(match self.delete_by_value(&value).await {
                Ok(true) => {
                    tracing::warn!(
                        set = self.name(),
                        class,
                        jid,
                        "client middleware stopped the push; the job was dropped"
                    );
                    Moved::Dropped
                }
                Ok(false) => Moved::Missing,
                Err(error) => Moved::Rejected(error),
            });
        }
        let (mode, job, extra) = match &target {
            Target::Queue { name, json } => ("queue", json.as_str(), name.clone()),
            Target::Schedule { at, json } => ("schedule", json.as_str(), at.to_string()),
            Target::Drop => ("drop", "", String::new()),
        };
        let target_key = match &target {
            Target::Queue { name, .. } => keys::queue(name),
            Target::Schedule { .. } => keys::SCHEDULE.to_owned(),
            Target::Drop => self.name().to_owned(),
        };
        let attempt: Result<i64> = async {
            let mut conn = self.api.redis().conn().await?;
            Ok(MOVE_OUT
                .key(self.name())
                .key(&target_key)
                .key(keys::QUEUES)
                .arg(&value)
                .arg(mode)
                .arg(job)
                .arg(&extra)
                .invoke_async(&mut conn)
                .await?)
        }
        .await;
        match attempt {
            Ok(0) => Ok(Moved::Missing),
            // Rescheduled by client middleware within this (scheduled) set.
            Ok(2) => Ok(Moved::Pushed),
            Ok(_) => {
                self.note_removed(1);
                if let Target::Drop = target {
                    tracing::warn!(
                        set = self.name(),
                        class,
                        jid,
                        "client middleware stopped the push; the job was dropped"
                    );
                    return Ok(Moved::Dropped);
                }
                Ok(Moved::Pushed)
            }
            // Did the script run before the reply was lost? The job's presence tells.
            Err(error) => match self.contains(&value).await {
                Some(true) => Ok(Moved::Rejected(error)),
                Some(false) => {
                    // Moved, by this call or a concurrent one.
                    self.note_removed(1);
                    Ok(Moved::Gone)
                }
                None => Err(error),
            },
        }
    }

    /// `Some(true)` if `value` is in the set, `Some(false)` if not, `None` if Redis cannot
    /// tell.
    async fn contains(&self, value: &str) -> Option<bool> {
        let key = self.name();
        self.api
            .redis()
            .with_retry(|mut conn| async move {
                ::redis::cmd("ZSCORE")
                    .arg(key)
                    .arg(value)
                    .query_async::<Option<f64>>(&mut conn)
                    .await
            })
            .await
            .ok()
            .map(|score| score.is_some())
    }

    /// Moves every job of the set to the morgue (atomically, 100 at a time), then trims the
    /// morgue, and returns how many jobs were moved. With `notify`, the death handlers run for
    /// each job (see [`DeadSet::kill`]). Only as many jobs as the set held when the call
    /// started are moved. Jobs are never lost: after an error each job is in one of the two
    /// sets.
    pub async fn kill_all(&self, notify: bool) -> Result<u64> {
        let morgue = DeadSet::new(&self.api);
        let mut moved = 0;
        let result = self.kill_batches(&morgue, notify, &mut moved).await;
        // Account for (and trim after) the batches moved, even if a later batch failed. Jobs
        // killed in the morgue itself stay in it.
        if self.name() != morgue.name() {
            self.note_removed(moved);
        }
        let trimmed = morgue.trim().await;
        result?;
        trimmed?;
        Ok(moved)
    }

    async fn kill_batches(&self, morgue: &DeadSet, notify: bool, moved: &mut u64) -> Result<()> {
        let mut remaining = self.size().await?;
        while remaining > 0 {
            let count = remaining.min(BULK_BATCH);
            // Sent once: a repeat after a dropped connection would move another batch (and the
            // first batch's jobs could not be reported to the death handlers).
            let mut conn = self.api.redis().conn().await?;
            let batch: Vec<String> = MOVE_LOWEST
                .key(self.name())
                .key(morgue.name())
                .arg(count)
                .arg(now_secs_f64())
                .invoke_async(&mut conn)
                .await?;
            if batch.is_empty() {
                break;
            }
            let batch_size = batch.len() as u64;
            remaining = remaining.saturating_sub(batch_size);
            *moved += batch_size;
            // The batch members of the chunk count as dead (killed), and the locks of its
            // unique jobs are released. The chunk is chosen by the move itself, so this follows
            // it.
            let featured: Vec<JobPayload> = batch
                .iter()
                .filter_map(|value| super::featured_job(value))
                .collect();
            let reports = super::before_removal(&self.api, &featured, BatchRemoval::Killed).await;
            if !featured.is_empty() {
                unique::api::release_many(self.api.redis(), &featured).await;
            }
            if notify {
                for value in &batch {
                    morgue.notify(value, None);
                }
            }
            super::after_removal(&self.api, reports).await;
        }
        Ok(())
    }
}

impl fmt::Debug for JobSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("JobSet").field(&self.name()).finish()
    }
}

/// What happened to a `ZREM` sent once ([`JobSet::remove_once`]).
enum Removal {
    /// This call removed the job.
    Removed,
    /// The job was not in the set.
    Absent,
    /// The command failed and the job is still in the set.
    Failed(Error),
    /// The command failed and the job is no longer in the set (this call may have removed it).
    Gone,
    /// The command failed and whether the job is still in the set is unknown.
    Unknown(Error),
}

/// What [`JobSet::move_job`] did.
enum Moved {
    /// The job was pushed.
    Pushed,
    /// Client middleware stopped the push: the job was removed from the set (as in Sidekiq;
    /// a job of a feature as a delete removes it).
    Dropped,
    /// The job was no longer in the set.
    Missing,
    /// The move failed in transit and the job is no longer in the set: this call or a
    /// concurrent one moved it.
    Gone,
    /// The push was rejected (validation, middleware, Redis); the job stays in the set.
    Rejected(Error),
    /// Client middleware panicked; the job stays in the set.
    Panicked(Box<dyn std::any::Any + Send>),
}

/// Where [`JobSet::move_job`] sends a job, encoded exactly as `Client::raw_push` stores it
/// (`client::Stored`).
enum Target {
    /// Pushed to the queue `name` (with `enqueued_at` set).
    Queue { name: String, json: String },
    /// Added to the schedule at `at` (without `at` and `enqueued_at`).
    Schedule { at: f64, json: String },
    /// Only removed: client middleware stopped the push.
    Drop,
}

impl Target {
    /// The target of a job as it leaves client middleware: scheduled if its `at` is in the
    /// future, else pushed to its queue now.
    fn of(job: JobPayload) -> Result<Self> {
        Ok(match Stored::encode(job, now_secs_f64(), now_millis())? {
            Stored::Scheduled { at, json } => Self::Schedule { at, json },
            Stored::Queued { queue, json } => Self::Queue { name: queue, json },
        })
    }
}

/// What [`JobSet::retry_entries`] did.
#[derive(Default)]
struct RetryOutcome {
    /// Jobs pushed.
    pushed: u64,
    /// Jobs left in the set (unparseable, rejected or panicking).
    left: u64,
}

/// One end of a score range (`ZRANGE ... BYSCORE`).
#[derive(Debug, Clone, Copy, PartialEq)]
enum ScoreBound {
    /// This score, included.
    Inclusive(f64),
    /// This score, excluded.
    Exclusive(f64),
}

impl fmt::Display for ScoreBound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Rust prints finite floats in their shortest exact form, which Redis parses back to
        // the same score; infinite scores need Redis's spelling.
        let score = |f: &mut fmt::Formatter<'_>, score: f64| {
            if score == f64::INFINITY {
                f.write_str("+inf")
            } else if score == f64::NEG_INFINITY {
                f.write_str("-inf")
            } else {
                write!(f, "{score}")
            }
        };
        match *self {
            Self::Inclusive(value) => score(f, value),
            Self::Exclusive(value) => {
                f.write_str("(")?;
                score(f, value)
            }
        }
    }
}

/// `true` for errors about Redis itself (unreachable, failing over, out of memory, ...),
/// after which other jobs would fail too, as opposed to errors about one job.
fn is_unavailable(error: &Error) -> bool {
    const SERVER_WIDE: [&str; 9] = [
        "READONLY",
        "NOREPLICAS",
        "LOADING",
        "BUSY",
        "MASTERDOWN",
        "OOM",
        "MISCONF",
        "NOAUTH",
        "CLUSTERDOWN",
    ];
    match error {
        Error::Redis(error) => {
            error.is_io_error()
                || error.is_timeout()
                || error.is_connection_dropped()
                || error.is_connection_refusal()
                || error.is_unrecoverable_error()
                || error.is_cluster_error()
                || error.code().is_some_and(|code| SERVER_WIDE.contains(&code))
                // A transaction aborted at queue time (`EXECABORT`) or a pipeline: the errors
                // of the commands inside it.
                || error.clone().into_server_errors().is_some_and(|errors| {
                    errors
                        .iter()
                        .any(|(_, error)| SERVER_WIDE.contains(&error.code()))
                })
        }
        Error::Shutdown | Error::Io(_) => true,
        _ => false,
    }
}

/// A 64-bit hash of an entry, to deduplicate scans without keeping every entry around.
fn hash_of(value: &str) -> u64 {
    let mut hasher = std::hash::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

/// Parses a job for a manual retry: its retry count is decremented so that the retry does
/// not count against its limit, and any `at` is dropped so that it is enqueued now.
fn retry_payload(value: &str) -> Result<JobPayload> {
    let mut payload = JobPayload::from_json(value)?;
    payload.at = None;
    payload.retry_count = payload.retry_count.map(|count| count.saturating_sub(1));
    Ok(payload)
}

/// How [`JobSet::requeue_all`] pushes jobs back to their queues.
#[derive(Debug, Clone, Copy)]
enum Requeue {
    /// A manual retry: the retry count is decremented ([`retry_payload`]).
    Retry,
    /// Enqueued as is (only `at` is dropped), like [`SortedEntry::add_to_queue`].
    Enqueue,
}

impl Requeue {
    /// The payload to push for the job `value`.
    fn payload(self, value: &str) -> Result<JobPayload> {
        match self {
            Requeue::Retry => retry_payload(value),
            Requeue::Enqueue => {
                let mut payload = JobPayload::from_json(value)?;
                payload.at = None;
                Ok(payload)
            }
        }
    }
}

/// One job of a [`JobSet`] with its score (Sidekiq's `SortedEntry`). Dereferences to its
/// [`JobRecord`] for the job's fields.
#[derive(Clone)]
pub struct SortedEntry {
    record: JobRecord,
    score: f64,
    set: JobSet,
}

impl SortedEntry {
    /// The score: when the job runs (scheduled), will be retried (retries) or died (morgue),
    /// in seconds since the Unix epoch.
    pub fn score(&self) -> f64 {
        self.score
    }

    /// The score as a time.
    pub fn at(&self) -> SystemTime {
        secs_to_system_time(self.score)
    }

    /// The job as a record.
    pub fn record(&self) -> &JobRecord {
        &self.record
    }

    /// The set the entry was read from.
    pub fn set(&self) -> &JobSet {
        &self.set
    }

    /// A key identifying the entry, `"<score>-<jid>"`, for URLs; see [`parse_key`].
    pub fn key(&self) -> String {
        format!("{}-{}", self.score, self.jid().unwrap_or_default())
    }

    /// `true` if the job records an error (it has an `error_class`).
    pub fn has_error(&self) -> bool {
        self.error_class().is_some()
    }

    /// Removes the entry from its set. Returns `false` if it was no longer there.
    pub async fn delete(&self) -> Result<bool> {
        self.set.delete_by_value(self.value()).await
    }

    /// Moves the entry to time `at`. Returns `false` (and changes nothing) if the entry is no
    /// longer in its set.
    ///
    /// A unique job of the scheduled or retry set that holds its lock keeps it for the new wait:
    /// the lock is extended to `unique_for` beyond `at` (never shortened, so a job moved earlier
    /// keeps its lock as long as before) and, in the scheduled set, records the job's new time,
    /// where a debounce push (`OnConflict::Replace`) finds it. A lock that lapsed meanwhile, or
    /// that another job holds, is left alone.
    pub async fn reschedule(&mut self, at: SystemTime) -> Result<bool> {
        let (key, value, score) = (self.set.name(), self.value(), system_time_to_secs(at));
        let lock = if key == keys::SCHEDULE || key == keys::RETRY {
            super::featured_job(value).and_then(|job| lock_to_extend(&job))
        } else {
            None
        };
        if let Some(lock) = lock {
            let moved =
                rescore_holding(self.set.api.redis(), key, value.as_bytes(), score, &lock).await?;
            if moved {
                self.score = score;
            }
            return Ok(moved);
        }
        let mut pipe = ::redis::pipe();
        pipe.atomic()
            .cmd("ZSCORE")
            .arg(key)
            .arg(value)
            .cmd("ZADD")
            .arg(key)
            .arg("XX")
            .arg(score)
            .arg(value)
            .ignore();
        let pipe = &pipe;
        let (existing,): (Option<f64>,) = self
            .set
            .api
            .redis()
            .with_retry(|mut conn| async move { pipe.query_async(&mut conn).await })
            .await?;
        if existing.is_some() {
            self.score = score;
        }
        Ok(existing.is_some())
    }

    /// Pushes the job to its queue now, through the client, and removes it from the set.
    /// Returns `false` if the entry was no longer in the set (someone else moved it).
    ///
    /// Client validation and middleware run first; then the job moves to its queue in one
    /// atomic step, only if it is still in the set: two concurrent calls never push it twice,
    /// and a failure (an error, a panic of client middleware, Redis being unavailable or out of
    /// memory, the caller giving up) leaves it in the set. The failure is returned (a panic is
    /// resumed). A job that cannot be parsed is left in the set and an error returned. If
    /// client middleware stops the push, the job is dropped as in Sidekiq (a warning is
    /// logged; a batch member or a unique job is removed as [`delete`](Self::delete) removes
    /// it: counted dead in its batch, its lock released) and this still returns `true`. If
    /// middleware schedules the job in the future, it moves to the scheduled set instead.
    pub async fn add_to_queue(&self) -> Result<bool> {
        let mut payload = JobPayload::from_json(self.value())?;
        payload.at = None;
        self.move_to_queue(payload).await
    }

    /// Retries the job now (Sidekiq's `SortedEntry#retry`): like
    /// [`add_to_queue`](Self::add_to_queue), but the retry count is decremented first so the
    /// manual retry does not count against the job's limit. (A count of 0 stays 0: Sidekiq
    /// stores -1, which hench's unsigned field cannot represent.)
    pub async fn retry(&self) -> Result<bool> {
        let payload = retry_payload(self.value())?;
        self.move_to_queue(payload).await
    }

    async fn move_to_queue(&self, payload: JobPayload) -> Result<bool> {
        // A stale entry (already moved) does not need client middleware to run.
        if self.set.contains(self.value()).await == Some(false) {
            return Ok(false);
        }
        match self.set.move_job(self.value().to_owned(), payload).await? {
            Moved::Pushed | Moved::Dropped | Moved::Gone => Ok(true),
            Moved::Missing => Ok(false),
            Moved::Rejected(error) => Err(error),
            // The job is still in the set; the panic belongs to the caller.
            Moved::Panicked(panic) => std::panic::resume_unwind(panic),
        }
    }

    /// Moves the job to the morgue now and runs the death handlers (Sidekiq's
    /// `SortedEntry#kill`). The move is atomic; returns `false` if the entry was no longer in
    /// its set.
    pub async fn kill(&self) -> Result<bool> {
        let morgue = DeadSet::new(&self.set.api);
        let featured = super::featured_job(self.value());
        let jobs = featured.as_slice();
        let reports = super::before_removal(&self.set.api, jobs, BatchRemoval::Killed).await;
        let moved = morgue
            .move_from(self.set.name(), self.value(), featured.as_ref())
            .await?;
        if moved == 1 {
            self.set.note_removed(1);
        }
        if moved > 0 {
            morgue.notify(self.value(), None);
        }
        super::after_removal(&self.set.api, reports).await;
        Ok(moved > 0)
    }
}

impl Deref for SortedEntry {
    type Target = JobRecord;

    fn deref(&self) -> &JobRecord {
        &self.record
    }
}

impl fmt::Debug for SortedEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SortedEntry")
            .field("set", &self.set.name())
            .field("score", &self.score)
            .field("value", &self.value())
            .finish()
    }
}

/// Splits a [`SortedEntry::key`] into its score and jid (`None` when the job has no jid).
pub fn parse_key(key: &str) -> Option<(f64, Option<&str>)> {
    // The score may be negative (times before 1970): its separator is the first `-` after
    // the first character. Scores are formatted without exponents.
    let separator = key
        .char_indices()
        .skip(1)
        .find_map(|(index, c)| (c == '-').then_some(index))?;
    let (score, jid) = (&key[..separator], &key[separator + 1..]);
    let score = score
        .parse::<f64>()
        .ok()
        .filter(|score| score.is_finite())?;
    Some((score, Some(jid).filter(|jid| !jid.is_empty())))
}

macro_rules! named_set {
    ($(#[$doc:meta])* $name:ident, $key:expr) => {
        $(#[$doc])*
        #[derive(Clone)]
        pub struct $name(JobSet);

        impl $name {
            /// The set, read and modified through `api`.
            pub fn new(api: impl Into<Api>) -> Self {
                Self(JobSet::new(api, $key))
            }

            /// The set as a plain [`JobSet`].
            pub fn into_inner(self) -> JobSet {
                self.0
            }
        }

        impl Deref for $name {
            type Target = JobSet;

            fn deref(&self) -> &JobSet {
                &self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_tuple(stringify!($name)).field(&self.0.name()).finish()
            }
        }
    };
}

named_set!(
    /// Jobs scheduled to run later (`schedule`), scored by run-at time. The scheduler moves
    /// them to their queues when they are due. Dereferences to [`JobSet`].
    ScheduledSet,
    keys::SCHEDULE
);

named_set!(
    /// Jobs waiting to be retried (`retry`), scored by retry-at time. The scheduler moves
    /// them to their queues when they are due. Dereferences to [`JobSet`].
    RetrySet,
    keys::RETRY
);

named_set!(
    /// The morgue (`dead`): jobs whose retries were exhausted, scored by time of death.
    /// Entries older than the API's dead timeout or beyond its size limit are trimmed when
    /// jobs are added. Dereferences to [`JobSet`].
    DeadSet,
    keys::DEAD
);

impl DeadSet {
    /// Adds the raw job `job` to the morgue now and trims it (Sidekiq's `DeadSet#kill`). With
    /// `notify`, the death handlers of the API's configuration run with a [`JobError`] of
    /// class `hench::Killed` (a job that cannot be parsed is still added, but handlers are
    /// skipped with a warning).
    pub async fn kill(&self, job: &str, notify: bool) -> Result<()> {
        let featured = super::featured_job(job);
        let reports =
            super::before_removal(&self.api, featured.as_slice(), BatchRemoval::Killed).await;
        self.add(job).await?;
        if notify {
            self.notify(job, None);
        }
        super::after_removal(&self.api, reports).await;
        Ok(())
    }

    /// [`kill`](Self::kill) with notification, passing `error` to the death handlers.
    pub async fn kill_with_error(&self, job: &str, error: &JobError) -> Result<()> {
        let featured = super::featured_job(job);
        let reports =
            super::before_removal(&self.api, featured.as_slice(), BatchRemoval::Killed).await;
        self.add(job).await?;
        self.notify(job, Some(error));
        super::after_removal(&self.api, reports).await;
        Ok(())
    }

    /// Removes jobs older than the API's dead timeout and the oldest jobs beyond its size
    /// limit. Like Sidekiq, the size trim keeps the newest `dead_max_jobs - 1` jobs.
    pub async fn trim(&self) -> Result<()> {
        let (key, cutoff, rank) = self.trim_bounds();
        let mut pipe = ::redis::pipe();
        pipe.atomic()
            .cmd("ZREMRANGEBYSCORE")
            .arg(key)
            .arg("-inf")
            .arg(cutoff)
            .ignore()
            .cmd("ZREMRANGEBYRANK")
            .arg(key)
            .arg(0)
            .arg(rank)
            .ignore();
        let pipe = &pipe;
        self.api
            .redis()
            .with_retry(|mut conn| async move { pipe.exec_async(&mut conn).await })
            .await
    }

    /// The key, the oldest score kept and the rank bound of the size trim.
    fn trim_bounds(&self) -> (&str, f64, i64) {
        let cutoff = now_secs_f64() - self.api.dead_timeout().as_secs_f64();
        let rank = -i64::try_from(self.api.dead_max_jobs()).unwrap_or(i64::MAX);
        (self.name(), cutoff, rank)
    }

    /// `ZADD` now plus the trim, in one transaction.
    async fn add(&self, job: &str) -> Result<()> {
        let (key, cutoff, rank) = self.trim_bounds();
        let mut pipe = ::redis::pipe();
        pipe.atomic()
            .cmd("ZADD")
            .arg(key)
            .arg(now_secs_f64())
            .arg(job)
            .ignore()
            .cmd("ZREMRANGEBYSCORE")
            .arg(key)
            .arg("-inf")
            .arg(cutoff)
            .ignore()
            .cmd("ZREMRANGEBYRANK")
            .arg(key)
            .arg(0)
            .arg(rank)
            .ignore();
        let pipe = &pipe;
        self.api
            .redis()
            .with_retry(|mut conn| async move { pipe.exec_async(&mut conn).await })
            .await
    }

    /// Atomically moves `job` from the set `source` to the morgue and trims it. Returns the
    /// script's answer: 1 (moved), 2 (refreshed in place) or 0 (not in `source`). A unique job
    /// (`featured`) releases its lock in the same script.
    async fn move_from(
        &self,
        source: &str,
        job: &str,
        featured: Option<&JobPayload>,
    ) -> Result<i64> {
        let (key, cutoff, rank) = self.trim_bounds();
        if let Some(payload) = featured {
            let bounds = unique::api::MorgueBounds {
                key,
                now: now_secs_f64(),
                cutoff,
                rank,
            };
            if let Some(moved) =
                unique::api::kill_from_set(self.api.redis(), source, bounds, job, payload).await
            {
                return moved;
            }
        }
        // Sent once: a repeat after a dropped connection would report a moved job as missing.
        let mut conn = self.api.redis().conn().await?;
        let moved: i64 = MOVE_TO_MORGUE
            .key(source)
            .key(key)
            .arg(job)
            .arg(now_secs_f64())
            .arg(cutoff)
            .arg(rank)
            .invoke_async(&mut conn)
            .await?;
        Ok(moved)
    }

    /// Runs the death handlers for a killed job.
    fn notify(&self, job: &str, error: Option<&JobError>) {
        match JobPayload::from_json(job) {
            Ok(payload) => match error {
                Some(error) => self.api.notify_death(&payload, error),
                None => self.api.notify_death(
                    &payload,
                    &JobError::new(JobError::KILLED, "the job was killed through the API"),
                ),
            },
            Err(error) => tracing::warn!(
                %error,
                "not running death handlers for a killed job that cannot be parsed"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_round_trip() {
        assert_eq!(
            parse_key("1700000000.123456-0123abc"),
            Some((1_700_000_000.123_456, Some("0123abc")))
        );
        assert_eq!(parse_key("12.5-"), Some((12.5, None)));
        assert_eq!(parse_key("12.5-a-b"), Some((12.5, Some("a-b"))));
        assert_eq!(parse_key("nope"), None);
        assert_eq!(parse_key("x-jid"), None);
        assert_eq!(parse_key("inf-jid"), None);
        assert_eq!(parse_key("-5-jid"), Some((-5.0, Some("jid"))));
        assert_eq!(parse_key("-0.25-"), Some((-0.25, None)));
        assert_eq!(parse_key("-"), None);
        assert_eq!(parse_key(""), None);
    }

    #[test]
    fn score_bounds_use_redis_syntax() {
        assert_eq!(ScoreBound::Inclusive(f64::INFINITY).to_string(), "+inf");
        assert_eq!(ScoreBound::Inclusive(1.5).to_string(), "1.5");
        assert_eq!(
            ScoreBound::Exclusive(1_700_000_000.123_456).to_string(),
            "(1700000000.123456"
        );
        assert_eq!(ScoreBound::Exclusive(f64::INFINITY).to_string(), "(+inf");
        assert_eq!(ScoreBound::Inclusive(f64::NEG_INFINITY).to_string(), "-inf");
        assert_eq!(ScoreBound::Inclusive(1e-7).to_string(), "0.0000001");
    }

    #[test]
    fn redis_outages_are_told_apart_from_job_errors() {
        let io = Error::Redis(::redis::RedisError::from(std::io::Error::from(
            std::io::ErrorKind::ConnectionReset,
        )));
        assert!(is_unavailable(&io));
        let readonly = Error::Redis(::redis::make_extension_error("READONLY".to_owned(), None));
        assert!(is_unavailable(&readonly));
        let wrongtype = Error::Redis(::redis::make_extension_error("WRONGTYPE".to_owned(), None));
        assert!(!is_unavailable(&wrongtype));
        assert!(!is_unavailable(&Error::InvalidJob("bad".into())));
        assert!(is_unavailable(&Error::Shutdown));
    }

    #[test]
    fn manual_retries_do_not_count() {
        let payload = retry_payload(r#"{"class":"A","retry_count":3,"at":5.0}"#).unwrap();
        assert_eq!(payload.retry_count, Some(2));
        assert_eq!(payload.at, None);
        let payload = retry_payload(r#"{"class":"A","retry_count":0}"#).unwrap();
        assert_eq!(payload.retry_count, Some(0));
        let payload = retry_payload(r#"{"class":"A"}"#).unwrap();
        assert_eq!(payload.retry_count, None);
        assert!(retry_payload("garbage").is_err());
    }
}
