//! The Rust side of the periodic scripts (`docs/design/periodic.md` §2.5): each function builds
//! the keys and arguments of one script and reads its reply. The poller and the API decide;
//! these functions only talk to Redis.
//!
//! Retries: `LEASE`, `FIRE`, `SYNC`, `UPSERT`, `REMOVE`, `PAUSE`, `UNPAUSE` and `ERROR` are
//! idempotent or guarded (a compare-and-set, a token), so they go through
//! [`RedisPool::with_retry`]. `ENQUEUE_NOW` without a lock would push twice if resent: it is
//! sent once, and its caller reads `manual_jid` after an error to learn whether it ran.

use std::collections::HashMap;
use std::time::Duration;

use crate::error::{Error, Result};
use crate::keys;
use crate::periodic::definition::Definition;
use crate::periodic::lua::{
    PERIODIC_ENQUEUE_NOW, PERIODIC_ERROR, PERIODIC_FIRE, PERIODIC_LEASE, PERIODIC_PAUSE,
    PERIODIC_REMOVE, PERIODIC_SYNC, PERIODIC_UNINDEX, PERIODIC_UNPAUSE, PERIODIC_UPSERT,
};
use crate::redis::RedisPool;
use crate::unique::push::{PushLock, script_error};

/// A schedule's hash, as `HGETALL` returns it.
pub(crate) type Fields = HashMap<String, String>;

/// Sends `invocation` through [`RedisPool::with_retry`].
async fn send<T: ::redis::FromRedisValue + Send>(
    redis: &RedisPool,
    invocation: &::redis::ScriptInvocation<'_>,
) -> Result<T> {
    redis
        .with_retry(|mut conn| async move { invocation.invoke_async(&mut conn).await })
        .await
}

/// What [`lease`] answered.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Leased {
    /// The time the script used (Redis's clock, or the test clock), epoch milliseconds.
    pub(crate) now_ms: i64,
    /// The schedule leased, with its score before the lease.
    pub(crate) due: Option<(String, f64)>,
    /// When nothing is due: the earliest score of a schedule this poller can handle.
    pub(crate) earliest: Option<f64>,
}

/// Leases the earliest due schedule for `lease` (`lease.lua`). `clock`: the time to use instead
/// of Redis's (tests). `skip`: names this poller cannot handle.
pub(crate) async fn lease(
    redis: &RedisPool,
    lease: Duration,
    clock: Option<i64>,
    skip: &[String],
) -> Result<Leased> {
    let mut invocation = PERIODIC_LEASE.script().prepare_invoke();
    invocation
        .key(keys::PERIODIC)
        .arg(u64::try_from(lease.as_millis()).unwrap_or(u64::MAX))
        .arg(clock.map(|now| now.to_string()).unwrap_or_default());
    for name in skip {
        invocation.arg(name);
    }
    let (now_ms, name, score): (i64, Option<String>, Option<String>) =
        send(redis, &invocation).await?;
    let score = score.and_then(|score| score.parse::<f64>().ok());
    Ok(match name {
        Some(name) => Leased {
            now_ms,
            due: Some((name, score.unwrap_or(0.0))),
            earliest: None,
        },
        None => Leased {
            now_ms,
            due: None,
            earliest: score,
        },
    })
}

/// The hash of schedule `name` (empty when there is none).
pub(crate) async fn read(redis: &RedisPool, name: &str) -> Result<Fields> {
    let key = keys::periodic(name);
    let key = key.as_str();
    redis
        .with_retry(|mut conn| async move {
            ::redis::cmd("HGETALL")
                .arg(key)
                .query_async::<Fields>(&mut conn)
                .await
        })
        .await
}

/// Sets the score of `name` in the due index, only if it is there (`ZADD XX`): gives a leased
/// schedule its score back, or defers it.
pub(crate) async fn rescore(redis: &RedisPool, name: &str, score: f64) -> Result<()> {
    redis
        .with_retry(|mut conn| async move {
            ::redis::cmd("ZADD")
                .arg(keys::PERIODIC)
                .arg("XX")
                .arg(score)
                .arg(name)
                .query_async::<()>(&mut conn)
                .await
        })
        .await
}

/// Removes `name` from the due index if it is still deleted or paused (`PERIODIC_UNINDEX`: the
/// check and the removal in one step, so that a resume or a re-creation since the poller read the
/// hash keeps the entry). `true` when it was removed.
pub(crate) async fn unindex(redis: &RedisPool, name: &str) -> Result<bool> {
    let key = keys::periodic(name);
    let mut invocation = PERIODIC_UNINDEX.script().prepare_invoke();
    invocation.key(keys::PERIODIC).key(&key).arg(name);
    send::<i64>(redis, &invocation)
        .await
        .map(|removed| removed == 1)
}

/// Records why schedule `name` cannot fire (`error`, `error_at`), if it still exists
/// (`error.lua`); `FIRE` clears it once the schedule fires again.
pub(crate) async fn set_error(
    redis: &RedisPool,
    name: &str,
    error: &str,
    at_ms: i64,
) -> Result<()> {
    let key = keys::periodic(name);
    let mut invocation = PERIODIC_ERROR.script().prepare_invoke();
    invocation.key(&key).arg(truncate(error, 2_000)).arg(at_ms);
    send::<i64>(redis, &invocation).await.map(|_| ())
}

/// At most `max` bytes of `text`, cut at a character boundary.
pub(crate) fn truncate(text: &str, max: usize) -> &str {
    if text.len() <= max {
        return text;
    }
    let mut end = max;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

/// How a job is written by `FIRE` or `ENQUEUE_NOW`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Write {
    /// `LPUSH queue:<queue>`, with `SADD queues <queue>`.
    Enqueued {
        /// The queue.
        queue: String,
        /// The job's JSON.
        json: String,
    },
    /// `ZADD schedule <at>` (client middleware scheduled it).
    Scheduled {
        /// The run-at time, epoch seconds.
        at: f64,
        /// The job's JSON.
        json: String,
    },
    /// Nothing: client middleware stopped the push.
    Halted,
    /// Nothing: the catch-up policy enqueues no tick now (or a repair).
    Skipped,
}

impl Write {
    fn mode(&self) -> &'static str {
        match self {
            Write::Enqueued { .. } => "enqueued",
            Write::Scheduled { .. } => "scheduled",
            Write::Halted => "halted",
            Write::Skipped => "skipped",
        }
    }

    /// The destination key, the job's JSON and the queue name or score.
    fn parts(&self, any_key: &str) -> (String, String, String) {
        match self {
            Write::Enqueued { queue, json } => (keys::queue(queue), json.clone(), queue.clone()),
            Write::Scheduled { at, json } => {
                (keys::SCHEDULE.to_owned(), json.clone(), at.to_string())
            }
            Write::Halted | Write::Skipped => (any_key.to_owned(), String::new(), String::new()),
        }
    }

    /// The lock's `at`: the score of a scheduled job.
    fn at(&self) -> String {
        match self {
            Write::Scheduled { at, .. } => at.to_string(),
            _ => String::new(),
        }
    }
}

/// The lock of a unique job written by `FIRE` or `ENQUEUE_NOW`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Lock {
    /// The lock as the push scripts take it.
    pub(crate) lock: PushLock,
    /// The job's class.
    pub(crate) class: String,
    /// The job's queue.
    pub(crate) queue: String,
}

/// The lock's keys (or placeholders) and arguments.
fn lock_parts(lock: Option<&Lock>, any_key: &str) -> (String, Vec<String>) {
    match lock {
        Some(lock) => (
            lock.lock.key.clone(),
            vec![
                lock.lock.digest.clone(),
                lock.lock.until.as_str().to_owned(),
                lock.lock.for_ms.to_string(),
            ],
        ),
        None => (
            any_key.to_owned(),
            vec![String::new(), String::new(), "0".to_owned()],
        ),
    }
}

/// One `FIRE`.
#[derive(Debug, Clone)]
pub(crate) struct Fire<'a> {
    /// The schedule.
    pub(crate) name: &'a str,
    /// Its `next` as read ('' when it had none).
    pub(crate) expected_next: &'a str,
    /// Its digest as read.
    pub(crate) digest: &'a str,
    /// Its `next` once fired (`None`: parked, out of the due index).
    pub(crate) new_next: Option<i64>,
    /// The tick handled (`None`: none, the policy enqueues nothing now).
    pub(crate) tick: Option<i64>,
    /// What to write.
    pub(crate) write: Write,
    /// The job's lock, for a unique job.
    pub(crate) lock: Option<Lock>,
    /// Ticks dropped by the catch-up policy.
    pub(crate) missed: u64,
    /// The history entry ('' = none).
    pub(crate) entry: String,
    /// The history entry if the job turns out to be a duplicate.
    pub(crate) duplicate_entry: String,
    /// Entries kept in the history.
    pub(crate) history: usize,
    /// The job's jid.
    pub(crate) jid: &'a str,
    /// The process identity.
    pub(crate) identity: &'a str,
    /// The poller's time, epoch milliseconds.
    pub(crate) now_ms: i64,
}

/// What `FIRE` did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Fired {
    /// Done as asked; `without_lock`: the job was written but its lock could not be taken.
    Done {
        /// Why the lock could not be taken.
        without_lock: Option<String>,
    },
    /// The tick was not after `last_tick`: advanced, nothing enqueued.
    Guard,
    /// A duplicate: the tick is consumed without a job; `holder` holds the lock.
    Duplicate {
        /// The jid holding the lock.
        holder: String,
    },
    /// The tick was handled already (`next` changed).
    Handled,
    /// The definition changed.
    Changed,
    /// Paused.
    Paused,
    /// Gone (its index entry was removed).
    Gone,
}

/// Sends `fire` (`fire.lua`). The lock key of the wrong type is an error (nothing written).
pub(crate) async fn fire(redis: &RedisPool, fire: &Fire<'_>, events: &str) -> Result<Fired> {
    let hash = keys::periodic(fire.name);
    let (destination, json, place) = fire.write.parts(&hash);
    let (lock_key, lock_args) = lock_parts(fire.lock.as_ref(), &hash);
    let mut invocation = PERIODIC_FIRE.script().prepare_invoke();
    invocation
        .key(keys::PERIODIC)
        .key(&hash)
        .key(keys::periodic_history(fire.name))
        .key(keys::QUEUES)
        .key(&destination)
        .key(&lock_key)
        .key(keys::UNIQUE_LOCKS)
        .key(events)
        .arg(fire.name)
        .arg(fire.expected_next)
        .arg(fire.digest)
        .arg(
            fire.new_next
                .map(|next| next.to_string())
                .unwrap_or_default(),
        )
        .arg(fire.tick.map(|tick| tick.to_string()).unwrap_or_default())
        .arg(fire.write.mode())
        .arg(&json)
        .arg(&place)
        .arg(fire.missed)
        .arg(&fire.entry)
        .arg(&fire.duplicate_entry)
        .arg(fire.history)
        .arg(fire.jid)
        .arg(fire.identity)
        .arg(fire.now_ms);
    for arg in &lock_args {
        invocation.arg(arg);
    }
    let (class, queue) = fire
        .lock
        .as_ref()
        .map(|lock| (lock.class.as_str(), lock.queue.as_str()))
        .unwrap_or_default();
    invocation.arg(fire.write.at()).arg(class).arg(queue);
    let (code, detail): (i64, String) = send(redis, &invocation).await?;
    Ok(match code {
        1 => Fired::Done {
            without_lock: (!detail.is_empty()).then_some(detail),
        },
        2 => Fired::Guard,
        3 => Fired::Duplicate { holder: detail },
        0 => Fired::Handled,
        -1 => Fired::Changed,
        -2 => Fired::Paused,
        -3 => Fired::Gone,
        _ => return Err(script_error(&detail)),
    })
}

/// One schedule of a `SYNC`.
#[derive(Debug, Clone)]
pub(crate) struct SyncEntry<'a> {
    /// The definition.
    pub(crate) definition: &'a Definition,
    /// Its first tick if it is new.
    pub(crate) next_new: i64,
    /// Its first tick if its timetable changed.
    pub(crate) next_changed: i64,
}

/// What `SYNC` did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Synced {
    /// Schedules created.
    pub(crate) created: u64,
    /// Schedules whose definition changed, not their timetable.
    pub(crate) updated: u64,
    /// Schedules whose timetable changed.
    pub(crate) rescheduled: u64,
    /// Schedules unchanged.
    pub(crate) unchanged: u64,
    /// The group's schedules deleted (no longer in the crontab).
    pub(crate) removed: u64,
    /// Names an API schedule or another group uses (not registered).
    pub(crate) conflicts: Vec<String>,
}

/// Registers a group's crontab (`sync.lua`). `members`: the group's members as read; `stale`:
/// those no longer in the crontab. `None` when the members changed meanwhile (read again).
pub(crate) async fn sync(
    redis: &RedisPool,
    group: &str,
    record: &str,
    members: &[String],
    stale: &[String],
    entries: &[SyncEntry<'_>],
    now_ms: i64,
) -> Result<Option<Synced>> {
    let mut invocation = PERIODIC_SYNC.script().prepare_invoke();
    invocation
        .key(keys::PERIODIC)
        .key(keys::PERIODIC_NAMES)
        .key(keys::PERIODIC_CRONTABS)
        .key(keys::periodic_group(group));
    for entry in entries {
        invocation.key(keys::periodic(&entry.definition.name));
    }
    for name in stale {
        invocation
            .key(keys::periodic(name))
            .key(keys::periodic_history(name));
    }
    invocation
        .arg(group)
        .arg(record)
        .arg(members.join("\n"))
        .arg(entries.len())
        .arg(now_ms);
    for entry in entries {
        let definition = entry.definition;
        invocation
            .arg(&definition.name)
            .arg(&definition.digest)
            .arg(&definition.spec)
            .arg(&definition.timing_key)
            .arg(&definition.policy)
            .arg(&definition.job)
            .arg(definition.description.as_deref().unwrap_or_default())
            .arg(entry.next_new)
            .arg(entry.next_changed)
            .arg(if definition.paused { "1" } else { "0" });
    }
    let reply: Vec<::redis::Value> = send(redis, &invocation).await?;
    let number = |index: usize| -> u64 {
        match reply.get(index) {
            Some(::redis::Value::Int(n)) => u64::try_from(*n).unwrap_or(0),
            _ => 0,
        }
    };
    if number(0) != 1 {
        return Ok(None);
    }
    let conflicts = reply
        .iter()
        .skip(6)
        .filter_map(|value| match value {
            ::redis::Value::BulkString(bytes) => Some(String::from_utf8_lossy(bytes).into_owned()),
            ::redis::Value::SimpleString(text) => Some(text.clone()),
            _ => None,
        })
        .collect();
    Ok(Some(Synced {
        created: number(1),
        updated: number(2),
        rescheduled: number(3),
        unchanged: number(4),
        removed: number(5),
        conflicts,
    }))
}

/// What `UPSERT` answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum UpsertReply {
    /// Created (1), updated (2), rescheduled (3) or unchanged (0).
    Done(i64),
    /// The digest was not the one expected: the current one.
    Conflict(String),
    /// The schedule belongs to the other source.
    OtherSource(String),
}

/// One `UPSERT` (`upsert.lua`): `expected` is the digest read ('' = must not exist).
pub(crate) async fn upsert(
    redis: &RedisPool,
    definition: &Definition,
    expected: &str,
    source: &str,
    next_new: i64,
    next_changed: i64,
    now_ms: i64,
) -> Result<UpsertReply> {
    let mut invocation = PERIODIC_UPSERT.script().prepare_invoke();
    invocation
        .key(keys::PERIODIC)
        .key(keys::periodic(&definition.name))
        .key(keys::PERIODIC_NAMES)
        .arg(&definition.name)
        .arg(expected)
        .arg(&definition.digest)
        .arg(source)
        .arg(&definition.spec)
        .arg(&definition.timing_key)
        .arg(&definition.policy)
        .arg(&definition.job)
        .arg(definition.description.as_deref().unwrap_or_default())
        .arg(next_new)
        .arg(next_changed)
        .arg(if definition.paused { "1" } else { "0" })
        .arg(now_ms);
    let reply: Vec<::redis::Value> = send(redis, &invocation).await?;
    let code = match reply.first() {
        Some(::redis::Value::Int(code)) => *code,
        _ => return Err(script_error("ERR PERIODIC_UPSERT answered without a code")),
    };
    let detail = reply
        .get(1)
        .and_then(|value| ::redis::from_redis_value_ref::<String>(value).ok())
        .unwrap_or_default();
    Ok(match code {
        -1 => UpsertReply::Conflict(detail),
        -2 => UpsertReply::OtherSource(detail),
        code => UpsertReply::Done(code),
    })
}

/// One `REMOVE` (`remove.lua`): 1 removed, 0 none, -2 another source, -3 the group changed.
pub(crate) async fn remove(
    redis: &RedisPool,
    name: &str,
    source: Option<&str>,
    group: Option<&str>,
) -> Result<i64> {
    let group_key = match group {
        Some(group) => keys::periodic_group(group),
        None => keys::periodic(name),
    };
    let mut invocation = PERIODIC_REMOVE.script().prepare_invoke();
    invocation
        .key(keys::PERIODIC)
        .key(keys::PERIODIC_NAMES)
        .key(keys::periodic(name))
        .key(keys::periodic_history(name))
        .key(&group_key)
        .arg(name)
        .arg(source.unwrap_or_default())
        .arg(group.unwrap_or_default());
    send(redis, &invocation).await
}

/// One `PAUSE` (`pause.lua`): 1 paused, 2 already, 0 none.
pub(crate) async fn pause(redis: &RedisPool, name: &str, now_ms: i64) -> Result<i64> {
    let mut invocation = PERIODIC_PAUSE.script().prepare_invoke();
    invocation
        .key(keys::PERIODIC)
        .key(keys::periodic(name))
        .arg(name)
        .arg(now_ms);
    send(redis, &invocation).await
}

/// One `UNPAUSE` (`unpause.lua`): 1 resumed, 2 not paused, 0 none, -1 definition changed.
pub(crate) async fn unpause(redis: &RedisPool, name: &str, next: i64, digest: &str) -> Result<i64> {
    let mut invocation = PERIODIC_UNPAUSE.script().prepare_invoke();
    invocation
        .key(keys::PERIODIC)
        .key(keys::periodic(name))
        .arg(name)
        .arg(next)
        .arg(digest);
    send(redis, &invocation).await
}

/// One "enqueue now".
#[derive(Debug, Clone)]
pub(crate) struct EnqueueNow<'a> {
    /// The schedule.
    pub(crate) name: &'a str,
    /// What to write (`Enqueued`, `Scheduled` or `Halted`).
    pub(crate) write: Write,
    /// The job's lock, for a unique job.
    pub(crate) lock: Option<Lock>,
    /// The history entry.
    pub(crate) entry: String,
    /// The history entry if the job turns out to be a duplicate.
    pub(crate) duplicate_entry: String,
    /// Entries kept in the history.
    pub(crate) history: usize,
    /// The time, epoch milliseconds.
    pub(crate) now_ms: i64,
    /// The job's jid.
    pub(crate) jid: &'a str,
}

/// What `ENQUEUE_NOW` did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Enqueued {
    /// Written (or halted); `without_lock` as for [`Fired::Done`].
    Done {
        /// Why the lock could not be taken.
        without_lock: Option<String>,
    },
    /// Written already by an earlier attempt of this jid.
    Already,
    /// A duplicate: nothing written.
    Duplicate {
        /// The jid holding the lock.
        holder: String,
    },
    /// No such schedule.
    Missing,
}

/// Sends `enqueue` once (`enqueue_now.lua`). Not retried: without a lock a resend would push
/// twice.
pub(crate) async fn enqueue_now(
    redis: &RedisPool,
    enqueue: &EnqueueNow<'_>,
    events: &str,
) -> Result<Enqueued> {
    let hash = keys::periodic(enqueue.name);
    let (destination, json, place) = enqueue.write.parts(&hash);
    let (lock_key, lock_args) = lock_parts(enqueue.lock.as_ref(), &hash);
    let mut invocation = PERIODIC_ENQUEUE_NOW.script().prepare_invoke();
    invocation
        .key(&hash)
        .key(keys::periodic_history(enqueue.name))
        .key(keys::QUEUES)
        .key(&destination)
        .key(&lock_key)
        .key(keys::UNIQUE_LOCKS)
        .key(events)
        .arg(enqueue.write.mode())
        .arg(&json)
        .arg(&place)
        .arg(&enqueue.entry)
        .arg(&enqueue.duplicate_entry)
        .arg(enqueue.history)
        .arg(enqueue.now_ms)
        .arg(enqueue.jid);
    for arg in &lock_args {
        invocation.arg(arg);
    }
    let (class, queue) = enqueue
        .lock
        .as_ref()
        .map(|lock| (lock.class.as_str(), lock.queue.as_str()))
        .unwrap_or_default();
    invocation.arg(enqueue.write.at()).arg(class).arg(queue);
    let mut conn = redis.conn().await?;
    let (code, detail): (i64, String) = invocation
        .invoke_async(&mut conn)
        .await
        .map_err(Error::from)?;
    Ok(match code {
        1 => Enqueued::Done {
            without_lock: (!detail.is_empty()).then_some(detail),
        },
        2 => Enqueued::Already,
        3 => Enqueued::Duplicate { holder: detail },
        0 => Enqueued::Missing,
        _ => return Err(script_error(&detail)),
    })
}
