//! Poison pills: jobs that kill the processes running them (an out-of-memory kill, an abort, a
//! crash in foreign code). The orphan check recovers the in-progress jobs of a dead process and
//! counts each recovery per job (`recovered-<jid>`, kept [`RECOVERY_WINDOW`] after the last
//! one). Every job in flight when a process dies is counted: the one that killed it, if a job
//! did, and the ones that merely ran beside it. So that the next crash tells them apart, the jobs
//! of a dead process's list are judged together, once, when the list is recovered:
//!
//! - a job at its first recovery (the usual case: a deploy, a lost node, an out-of-memory kill of
//!   a busy process) goes back to the front of its queue at once, with the others;
//! - a job recovered before (a *suspect*: in flight at an earlier crash too) waits in
//!   [`keys::RECOVERY_SUSPECTS`] instead, and the suspects come back alone: an orphan check
//!   releases one (counting its recovery) once the one released before it
//!   ([`keys::RELEASED_SUSPECT`]) is no longer waiting in its queue or running (or after ten
//!   check intervals), the most recovered first, then the first parked, and of one crash the
//!   most recently fetched first (a job that kills its process soon after it starts was fetched
//!   after the jobs running beside it). A process that runs a suspect and dies holds it beside
//!   jobs at their first recovery only, which are not suspects yet;
//! - a job whose recovery would be beyond [`Config::max_recoveries`] is not pushed back to crash
//!   another process: it is settled like a job whose retries are exhausted, in the dead process's
//!   list, with the error class [`JobError::CRASH_LOOP`] and a message saying how many other jobs
//!   were in flight at its last crash. When several suspects of one crash reach the limit
//!   together, the crash cannot be pinned on one of them: each gets one recovery more, alone (it
//!   waits with the suspects), and the one that crashes its process again goes to the morgue.
//!
//! Settling a pill:
//!
//! 1. it is claimed ([`keys::recovery_claim`]): an orphan check that overlaps the one settling it
//!    (whose lock lapsed while it ran) leaves it alone, so its handlers run once;
//! 2. its batch counts it dead (before the write, so that a crash in between repeats both);
//! 3. it moves to the morgue (or, with `dead: false`, is dropped) in one script that also
//!    releases its unique lock (`SETTLE`, `SETTLE_UNIQUE`), guarded by its presence in the list;
//! 4. then the recovering process runs its death and error handlers, dispatches the batch's
//!    events and records the run in the job's schedule (periodic jobs).
//!
//! An iterable job that saves progress (a cursor beyond the one its execution started from)
//! forgets its recoveries: it is not what kills its processes, and unrelated crashes of a long
//! iteration never bury it (`crate::iterable`). Other jobs restart from scratch at each crash, so
//! their recoveries count until the window ends, whatever ran in between.

use std::sync::LazyLock;
use std::time::Duration;

use ::redis::Script;

use crate::batch::{self, BatchMember, Report};
use crate::config::{Config, ErrorContext};
use crate::error::{JobError, Result};
use crate::keys;
use crate::payload::{JobPayload, now_millis, truncate_error_message};
use crate::periodic::{self, PeriodicRef, RunStatus};
use crate::pro::{Classified, Fetched};
use crate::redis::RedisPool;
use crate::retry::{self, Verdict};
use crate::unique;

use super::{
    BATCH, Fetcher, RECOVER_SCRIPT, RECOVERY_CLAIM_TTL, RECOVERY_WINDOW, RecoveryLock, UnitOfWork,
    read_list, release_owner, requeue_entries,
};

/// Characters of the error a schedule's record of the run keeps (as `RECORD_RUN` does).
const RECORDED_ERROR_CHARS: usize = 500;

/// Jids of the other jobs of a pill's last crash named in the log.
const LOGGED_NEIGHBOURS: usize = 10;

/// Scores of the suspects: [`SUSPECT_RANK`] for each recovery short of
/// [`SUSPECT_MAX_RANKED`], plus the time parked (ms) and the place in the dead list.
const SUSPECT_RANK: f64 = 1e13;
const SUSPECT_MAX_RANKED: u32 = 100;

/// Claims the settlement of a poison pill for this process (see [`keys::recovery_claim`]).
///
/// KEYS: the claim. ARGV: the dead process whose list holds the pill, this process, the claim's
/// life (ms). Returns 2 when this process claimed it now, 1 when it held the claim already (a
/// resend, or an earlier check of its own), 0 when another process is settling it. A claim of
/// another dead process's list (an earlier burial of the same job) is replaced.
const CLAIM: &str = r"
local claim = redis.call('HMGET', KEYS[1], 'of', 'by')
if claim[1] == ARGV[1] and claim[2] then
  if claim[2] ~= ARGV[2] then
    return 0
  end
  return 1
end
redis.call('DEL', KEYS[1])
redis.call('HSET', KEYS[1], 'of', ARGV[1], 'by', ARGV[2])
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return 2
";

/// Moves suspects from a dead process's in-progress list to [`keys::RECOVERY_SUSPECTS`]
/// (written before the entry leaves the list; an entry no longer in the list is left alone).
///
/// KEYS: the in-progress list, the suspects. ARGV: pairs of score and job. Returns how many
/// jobs were moved.
const PARK: &str = r"
local moved = 0
for i = 1, #ARGV, 2 do
  local job = ARGV[i + 1]
  if redis.call('LPOS', KEYS[1], job, 'RANK', -1) then
    redis.call('ZADD', KEYS[2], ARGV[i], job)
    redis.call('LREM', KEYS[1], -1, job)
    moved = moved + 1
  end
end
return moved
";

/// Moves a suspect from [`keys::RECOVERY_SUSPECTS`] to the front of its queue and counts its
/// recovery, like `RECOVER` (the counter under `pcall`; nothing written when the job is no longer
/// a suspect), and records it as the suspect last released ([`keys::RELEASED_SUSPECT`]).
///
/// KEYS: the suspects, the job's queue, its counter, the record of the last release. ARGV: the
/// job, the counter's expiry (ms), the queue's name, now (ms), the record's expiry (ms). Returns
/// 1 when the job was moved, 0 otherwise.
const RELEASE_SUSPECT: &str = r"
if not redis.call('ZSCORE', KEYS[1], ARGV[1]) then
  return 0
end
redis.call('RPUSH', KEYS[2], ARGV[1])
pcall(function()
  redis.call('INCR', KEYS[3])
  redis.call('PEXPIRE', KEYS[3], ARGV[2])
end)
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('DEL', KEYS[4])
redis.call('HSET', KEYS[4], 'job', ARGV[1], 'queue', ARGV[3], 'at', ARGV[4])
redis.call('PEXPIRE', KEYS[4], ARGV[5])
return 1
";

/// How far from the front of its queue a released suspect is looked for (it was pushed to the
/// front: only jobs recovered after it come before it).
const RELEASED_SEARCH: usize = 1_000;

/// How many orphan-check intervals the suspect last released may hold the next one back while
/// it waits in its queue or runs (a job that runs that long without killing its process is not
/// what kills them quickly).
const RELEASED_HOLD_INTERVALS: u32 = 10;

static CLAIM_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(CLAIM));
static PARK_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(PARK));
static RELEASE_SUSPECT_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(RELEASE_SUSPECT));

/// What a recovery does with a counted job of a dead process's list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fate {
    /// Its first recovery: back to the front of its queue at once.
    Release,
    /// Recovered before (a suspect): it waits with the suspects, which come back alone.
    Suspect,
    /// Past `max_recoveries`: settled as a poison pill.
    Pill,
}

/// The fate of each counted job of one dead process's list, from the recoveries counted before
/// (`previous`, in the list's order) and the limit (`max`). See the module documentation.
fn fates(previous: &[u32], max: u32) -> Vec<Fate> {
    let past = |count: u32| count.saturating_add(1) > max;
    // Suspects that reach the limit at this recovery: more than one, and the crash cannot be
    // pinned on one of them.
    let at_limit = previous
        .iter()
        .filter(|count| **count >= 1 && past(**count))
        .count();
    previous
        .iter()
        .map(|&count| {
            if !past(count) {
                if count == 0 {
                    Fate::Release
                } else {
                    Fate::Suspect
                }
            } else if count == 0 || at_limit <= 1 || count > max {
                // A first recovery past the limit (`max_recoveries(0)`), the only suspect at the
                // limit, or one that had its recovery more already.
                Fate::Pill
            } else {
                Fate::Suspect
            }
        })
        .collect()
}

/// A counted job of a dead process's list.
struct Entry {
    /// The job as stored in the list.
    job: Vec<u8>,
    payload: JobPayload,
    /// Its place in the list: 0 for the job fetched last.
    position: usize,
    /// The recoveries counted before this one.
    previous: u32,
    fate: Fate,
}

/// A dead process's in-progress list, read and judged.
struct DeadList {
    /// A fetcher working on the list (the outcome scripts settle pills there).
    fetcher: Fetcher,
    /// Jobs that cannot be counted (no jid, or unreadable): back to their queue, or the morgue.
    uncounted: Vec<Vec<u8>>,
    entries: Vec<Entry>,
    /// Every job the list held: the jobs in flight when the process died.
    in_flight: usize,
}

/// Recovers the in-progress lists of the dead processes `owners` (see the
/// [module documentation](self)), then releases one suspect, as `fetcher`'s process, which holds
/// the orphan check's `lock` (the check stops where it finds another process holding it).
/// Returns how many jobs went back to their queues or to the morgue (a suspect counts when it is
/// released).
pub(super) async fn recover_dead(
    fetcher: &Fetcher,
    owners: &[String],
    config: &Config,
    lock: &RecoveryLock<'_>,
) -> Result<usize> {
    let mut moved = 0;
    for owner in owners {
        if !lock.keep().await? {
            tracing::debug!("another process took over the orphan check");
            return Ok(moved);
        }
        let list = read(fetcher.for_owner(owner), config).await?;
        let (count, finished) = recover_list(fetcher, &list, config, lock).await?;
        moved += count;
        if !finished {
            tracing::debug!("another process took over the orphan check");
            return Ok(moved);
        }
    }
    if owners.is_empty() || lock.keep().await? {
        let hold = config
            .orphan_check_interval
            .saturating_mul(RELEASED_HOLD_INTERVALS);
        moved += release_suspect(fetcher.redis(), hold).await?;
    }
    Ok(moved)
}

/// Recovers the judged list `list`: returns how many jobs went back to their queues or to the
/// morgue, and whether the list was done (`false`: the check stopped, another process holds the
/// lock).
async fn recover_list(
    recovering: &Fetcher,
    list: &DeadList,
    config: &Config,
    lock: &RecoveryLock<'_>,
) -> Result<(usize, bool)> {
    let dead = &list.fetcher;
    let redis = dead.redis();
    let (owner, key) = (dead.identity(), dead.inprogress_key());
    let uncounted: Vec<&[u8]> = list.uncounted.iter().map(Vec::as_slice).collect();
    let mut count = requeue_entries(redis, key, owner, &uncounted, config).await?;
    let released: Vec<&Entry> = list.entries_of(Fate::Release).collect();
    count += recover_counted(redis, key, &released).await?;
    let suspects: Vec<&Entry> = list.entries_of(Fate::Suspect).collect();
    let parked = park(redis, key, &suspects).await?;
    if parked > 0 {
        tracing::warn!(
            process = %owner,
            jobs = parked,
            "jobs of a process that died were in flight at an earlier crash too: they come back \
             one per orphan check, alone, so that a job that kills its processes is told apart \
             from the jobs that ran beside it"
        );
    }
    for entry in list.entries_of(Fate::Pill) {
        if !lock.keep().await? {
            return Ok((count, false));
        }
        if settle_poison_pill(recovering, list, entry, config).await? {
            count += 1;
        }
    }
    if count > 0 {
        tracing::warn!(
            process = %owner,
            jobs = count,
            "recovered jobs held by a process that died"
        );
    }
    release_owner(redis, key, owner).await?;
    Ok((count, true))
}

impl DeadList {
    /// The entries with fate `fate`, in the list's order.
    fn entries_of(&self, fate: Fate) -> impl Iterator<Item = &Entry> {
        self.entries.iter().filter(move |entry| entry.fate == fate)
    }
}

/// Reads the list of the dead process `dead` works for and judges each job.
async fn read(dead: Fetcher, config: &Config) -> Result<DeadList> {
    let jobs = read_list(dead.redis(), dead.inprogress_key()).await?;
    let in_flight = jobs.len();
    let mut uncounted = Vec::new();
    let mut counted = Vec::new();
    for (position, job) in jobs.into_iter().enumerate() {
        match JobPayload::from_slice(&job) {
            Ok(payload) if !payload.queue.is_empty() && !payload.jid.is_empty() => {
                counted.push((job, payload, position));
            }
            _ => uncounted.push(job),
        }
    }
    let previous = recoveries(dead.redis(), &counted).await?;
    let fates = fates(&previous, config.max_recoveries);
    let entries = counted
        .into_iter()
        .zip(previous)
        .zip(fates)
        .map(|(((job, payload, position), previous), fate)| Entry {
            job,
            payload,
            position,
            previous,
            fate,
        })
        .collect();
    Ok(DeadList {
        fetcher: dead,
        uncounted,
        entries,
        in_flight,
    })
}

/// The score of a suspect parked at `now_ms`: the most recovered first, then the first parked,
/// and of one list the most recently fetched first.
fn suspect_score(entry: &Entry, now_ms: i64) -> f64 {
    let rank = SUSPECT_MAX_RANKED.saturating_sub(entry.previous.min(SUSPECT_MAX_RANKED));
    let position = u32::try_from(entry.position.min(999)).unwrap_or(999);
    #[allow(clippy::cast_precision_loss)] // ms since 1970 are far below 2^53
    let time = now_ms as f64;
    f64::from(rank) * SUSPECT_RANK + time + f64::from(position)
}

/// Moves `suspects` (entries of the in-progress list `list`) to the suspects. Returns how many
/// were moved.
async fn park(redis: &RedisPool, list: &str, suspects: &[&Entry]) -> Result<usize> {
    let now = now_millis();
    let mut moved = 0;
    for chunk in suspects.chunks(BATCH) {
        let mut invocation = PARK_SCRIPT.prepare_invoke();
        invocation.key(list).key(keys::RECOVERY_SUSPECTS);
        for entry in chunk {
            invocation
                .arg(suspect_score(entry, now))
                .arg(entry.job.as_slice());
        }
        let invocation = &invocation;
        let count: usize = redis
            .with_retry(|mut conn| async move { invocation.invoke_async(&mut conn).await })
            .await?;
        moved += count;
    }
    Ok(moved)
}

/// Releases the first suspect: back to the front of its queue, its recovery counted, unless the
/// suspect last released is still waiting in its queue or running (for less than `hold`): then
/// the next one waits for a later check. Returns 1 when one was released. A suspect that cannot
/// be read (or names no queue) is taken out.
async fn release_suspect(redis: &RedisPool, hold: Duration) -> Result<usize> {
    let first: Vec<Vec<u8>> = redis
        .with_retry(|mut conn| async move {
            ::redis::cmd("ZRANGE")
                .arg(keys::RECOVERY_SUSPECTS)
                .arg(0)
                .arg(0)
                .query_async(&mut conn)
                .await
        })
        .await?;
    let Some(job) = first.into_iter().next() else {
        return Ok(0);
    };
    if released_in_flight(redis, hold).await? {
        tracing::debug!(
            "the suspect last released is still waiting or running: the next one waits"
        );
        return Ok(0);
    }
    let payload = match JobPayload::from_slice(&job) {
        Ok(payload) if !payload.queue.is_empty() => payload,
        _ => {
            // Never written so (only counted jobs are parked); taken out rather than block the
            // others.
            tracing::warn!("an unreadable job among the recovery suspects: removed");
            let job = &job;
            let _removed: i64 = redis
                .with_retry(|mut conn| async move {
                    ::redis::cmd("ZREM")
                        .arg(keys::RECOVERY_SUSPECTS)
                        .arg(job)
                        .query_async(&mut conn)
                        .await
                })
                .await?;
            return Ok(0);
        }
    };
    let window_ms = u64::try_from(RECOVERY_WINDOW.as_millis()).unwrap_or(u64::MAX);
    let hold_ms = u64::try_from(hold.as_millis()).unwrap_or(u64::MAX).max(1);
    let (queue, counter) = (keys::queue(&payload.queue), keys::recoveries(&payload.jid));
    let (job_ref, queue_ref, counter_ref, name) = (&job, &queue, &counter, &payload.queue);
    let now = now_millis();
    let released: i64 = redis
        .with_retry(|mut conn| async move {
            RELEASE_SUSPECT_SCRIPT
                .key(keys::RECOVERY_SUSPECTS)
                .key(queue_ref)
                .key(counter_ref)
                .key(keys::RELEASED_SUSPECT)
                .arg(job_ref)
                .arg(window_ms)
                .arg(name)
                .arg(now)
                .arg(hold_ms)
                .invoke_async(&mut conn)
                .await
        })
        .await?;
    if released == 1 {
        tracing::warn!(
            class = %payload.class,
            jid = %payload.jid,
            queue = %payload.queue,
            "a job recovered from processes that died while it was running comes back alone, so \
             that a crash it causes is told apart from the jobs beside it"
        );
    }
    Ok(usize::try_from(released).unwrap_or(0))
}

/// Whether the suspect last released (if any, released less than `hold` ago) is still waiting
/// near the front of its queue (not paused) or in a process's in-progress list.
async fn released_in_flight(redis: &RedisPool, hold: Duration) -> Result<bool> {
    let record: std::collections::HashMap<String, Vec<u8>> = redis
        .with_retry(|mut conn| async move {
            ::redis::cmd("HGETALL")
                .arg(keys::RELEASED_SUSPECT)
                .query_async(&mut conn)
                .await
        })
        .await?;
    let (Some(job), Some(queue), Some(at)) = (
        record.get("job"),
        record
            .get("queue")
            .and_then(|queue| String::from_utf8(queue.clone()).ok()),
        record
            .get("at")
            .and_then(|at| std::str::from_utf8(at).ok()?.parse::<i64>().ok()),
    ) else {
        return Ok(false);
    };
    let hold_ms = i64::try_from(hold.as_millis()).unwrap_or(i64::MAX);
    if now_millis().saturating_sub(at) >= hold_ms {
        return Ok(false);
    }
    let owners: Vec<String> = redis
        .with_retry(|mut conn| async move {
            ::redis::cmd("SMEMBERS")
                .arg(keys::INPROGRESS)
                .query_async(&mut conn)
                .await
        })
        .await?;
    let mut pipe = ::redis::pipe();
    pipe.cmd("SISMEMBER").arg(keys::PAUSED).arg(&queue);
    pipe.cmd("LPOS")
        .arg(keys::queue(&queue))
        .arg(job)
        .arg("RANK")
        .arg(-1)
        .arg("MAXLEN")
        .arg(RELEASED_SEARCH);
    for owner in &owners {
        pipe.cmd("LPOS")
            .arg(keys::inprogress(owner))
            .arg(job)
            .arg("RANK")
            .arg(-1);
    }
    let pipe = &pipe;
    let found: Vec<::redis::Value> = redis
        .with_retry(|mut conn| async move { pipe.query_async(&mut conn).await })
        .await?;
    let present = |value: &::redis::Value| !matches!(value, ::redis::Value::Nil);
    let paused = matches!(found.first(), Some(::redis::Value::Int(1)));
    let waiting = !paused && found.get(1).is_some_and(present);
    let running = found.iter().skip(2).any(present);
    Ok(waiting || running)
}

/// How many times each job was recovered so far (its counter; 0 when it has none or it is not
/// a number).
async fn recoveries(redis: &RedisPool, jobs: &[(Vec<u8>, JobPayload, usize)]) -> Result<Vec<u32>> {
    let mut counts = Vec::with_capacity(jobs.len());
    for chunk in jobs.chunks(BATCH) {
        let counters: Vec<String> = chunk
            .iter()
            .map(|(_, payload, _)| keys::recoveries(&payload.jid))
            .collect();
        let counters = &counters;
        let values: Vec<Option<String>> = redis
            .with_retry(|mut conn| async move {
                ::redis::cmd("MGET")
                    .arg(counters)
                    .query_async(&mut conn)
                    .await
            })
            .await?;
        counts.extend(values.into_iter().map(|value| {
            value
                .and_then(|value| value.parse::<u32>().ok())
                .unwrap_or(0)
        }));
    }
    Ok(counts)
}

/// Pushes `jobs` (entries of the in-progress list `list`) back to the front of their queues
/// with the `RECOVER` script, counting their recovery. Returns how many were moved.
async fn recover_counted(redis: &RedisPool, list: &str, jobs: &[&Entry]) -> Result<usize> {
    let window_ms = u64::try_from(RECOVERY_WINDOW.as_millis()).unwrap_or(u64::MAX);
    let mut moved = 0;
    for chunk in jobs.chunks(BATCH) {
        let mut invocation = RECOVER_SCRIPT.prepare_invoke();
        invocation.key(list).arg(window_ms);
        for entry in chunk {
            invocation
                .key(keys::queue(&entry.payload.queue))
                .key(keys::recoveries(&entry.payload.jid))
                .arg(entry.job.as_slice());
        }
        let invocation = &invocation;
        let count: usize = redis
            .with_retry(|mut conn| async move { invocation.invoke_async(&mut conn).await })
            .await?;
        moved += count;
    }
    Ok(moved)
}

/// Claims the settlement of the poison pill `jid` of the dead process `owner`'s list for the
/// process `me`: whether this process settles it (another one is otherwise).
async fn claim(redis: &RedisPool, jid: &str, owner: &str, me: &str) -> Result<bool> {
    let key = keys::recovery_claim(jid);
    let ttl_ms = u64::try_from(RECOVERY_CLAIM_TTL.as_millis()).unwrap_or(u64::MAX);
    let key = &key;
    let claimed: i64 = redis
        .with_retry(|mut conn| async move {
            CLAIM_SCRIPT
                .key(key)
                .arg(owner)
                .arg(me)
                .arg(ttl_ms)
                .invoke_async(&mut conn)
                .await
        })
        .await?;
    Ok(claimed > 0)
}

/// The error of a poison pill recovered for the `recovery`th time, `others` other jobs in
/// flight beside it at its last crash.
fn crash_loop(recovery: u32, others: usize, config: &Config) -> JobError {
    let beside = match others {
        0 => "no other job was in flight at its last crash".to_owned(),
        1 => "1 other job was in flight at its last crash".to_owned(),
        n => format!("{n} other jobs were in flight at its last crash"),
    };
    JobError::new(
        JobError::CRASH_LOOP,
        format!(
            "recovered {recovery} times from processes that died while they held it \
             (max_recoveries is {}; {beside}): the job may be what kills them; fix the cause, \
             then retry it from the morgue",
            config.max_recoveries
        ),
    )
}

/// What a poison pill's death is: the morgue, with its error fields, or (`dead: false`)
/// dropped, as for a job whose retries are exhausted.
fn verdict(mut job: JobPayload, error: &JobError) -> Verdict {
    let now = now_millis();
    job.error_class = Some(error.class().to_owned());
    job.error_message = Some(truncate_error_message(&error.message()));
    // A backtrace stored by an earlier failure would describe another error.
    job.error_backtrace = None;
    if job.failed_at.is_none() {
        job.failed_at = Some(now);
    }
    if job.dead == Some(false) {
        job.discarded_at = Some(now);
        Verdict::Discard { job }
    } else {
        Verdict::Kill { job }
    }
}

/// Settles the poison pill `entry` of the dead process's list `list` as `recovering`'s process,
/// as the [module documentation](self) describes. Returns whether the entry left the list now:
/// `false` when another process claimed it (that process settles it), or when it was gone
/// already (its handlers still ran, as for a failure whose write was repeated).
async fn settle_poison_pill(
    recovering: &Fetcher,
    list: &DeadList,
    entry: &Entry,
    config: &Config,
) -> Result<bool> {
    let dead = &list.fetcher;
    let redis = dead.redis();
    let payload = &entry.payload;
    let (class, jid, queue) = (&payload.class, &payload.jid, &payload.queue);
    if !claim(redis, jid, dead.identity(), recovering.identity()).await? {
        tracing::debug!(
            class = %class,
            jid = %jid,
            process = %dead.identity(),
            "another process is settling this poison pill"
        );
        return Ok(false);
    }
    let recovery = entry.previous.saturating_add(1);
    let others = list.in_flight.saturating_sub(1);
    let error = crash_loop(recovery, others, config);
    let fetched = Fetched::from_payload(payload);
    let verdict = verdict(payload.clone(), &error);
    let report = match &fetched.member {
        Some(member) => account(redis, member, &verdict).await?,
        None => None,
    };
    let settlement = retry::settlement(config, &verdict)?;
    let classified = Classified::Failure(Box::new(verdict));
    let lock = fetched
        .lock
        .as_ref()
        .and_then(|_| unique::engine::lock_op(&fetched, &classified, Some(&settlement)));
    let work = UnitOfWork::new(keys::queue(queue), entry.job.clone());
    let settled = dead.settle_with(&work, &settlement, lock.as_ref()).await?;
    let Classified::Failure(verdict) = &classified else {
        return Ok(settled);
    };
    let (stored, discarded) = match verdict.as_ref() {
        Verdict::Discard { job } => (job, true),
        Verdict::Kill { job } | Verdict::Retry { job, .. } | Verdict::NoRetry { job, .. } => {
            (job, false)
        }
    };
    let neighbours: Vec<&str> = list
        .entries
        .iter()
        .filter(|other| other.position != entry.position)
        .take(LOGGED_NEIGHBOURS)
        .map(|other| other.payload.jid.as_str())
        .collect();
    tracing::warn!(
        class = %class,
        jid = %jid,
        recoveries = recovery,
        max_recoveries = config.max_recoveries,
        process = %dead.identity(),
        others_in_flight = others,
        ?neighbours,
        "a job was recovered from processes that died while they held it too many times: it is \
         taken for a poison pill and {}",
        if discarded {
            "dropped (dead: false)"
        } else {
            "moved to the morgue"
        }
    );
    retry::finish(config, verdict, &error);
    config.handle_error(
        &error,
        &ErrorContext::new("job recovered too many times")
            .with_job(stored)
            .with_queue(queue),
    );
    if let Some(report) = report {
        batch::dispatch::settle(redis, &report).await;
    }
    if let Some(schedule) = &fetched.periodic {
        record_run(redis, schedule, &error).await;
    }
    Ok(settled)
}

/// Counts a poison pill dead in its batch (`member`), before its write. Errors stop the
/// recovery of the list (retried at the next check: the accounting is idempotent).
async fn account(
    redis: &RedisPool,
    member: &BatchMember,
    verdict: &Verdict,
) -> Result<Option<Report>> {
    let outcome = batch::account::failure(verdict, now_millis());
    let accounted = batch::scripts::account(redis, &member.bid, &member.jid, &outcome).await?;
    if accounted.applied {
        tracing::info!(
            bid = %member.bid,
            jid = %member.jid,
            "a poison pill of a batch counted dead in its batch"
        );
    }
    Ok((accounted.dispatch || accounted.refreshed).then(|| Report {
        bid: member.bid.clone(),
        dispatch: accounted.dispatch,
        refreshed: accounted.refreshed,
    }))
}

/// Records the poison pill's run as failed in its schedule (informational: errors are logged).
async fn record_run(redis: &RedisPool, schedule: &PeriodicRef, error: &JobError) {
    let text: String = format!("{}: {}", error.class(), error.message())
        .chars()
        .take(RECORDED_ERROR_CHARS)
        .collect();
    if let Err(error) = periodic::record_run::record(
        redis,
        &schedule.name,
        schedule.tick,
        RunStatus::Failed,
        &text,
        0,
        &schedule.jid,
    )
    .await
    {
        tracing::debug!(
            schedule = %schedule.name,
            jid = %schedule.jid,
            %error,
            "could not record the run of a periodic job taken for a poison pill"
        );
    }
}
