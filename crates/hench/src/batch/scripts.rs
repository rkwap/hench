//! Calls of the batch scripts (`batch/lua/*.lua`): their keys and arguments, and their replies
//! (`docs/design/batches.md` §2.5).
//!
//! The commit has its own sending loop (`batch::commit`), since its outcome must be known; the
//! other scripts go through [`RedisPool::with_retry`], which resends once after a dropped
//! connection or a failover: each is idempotent (keyed by jid, outbox entry or batch state).

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use redis::{ErrorKind, RedisError, Value};

use crate::batch::BatchError;
use crate::batch::lua::{
    BATCH_CANCEL, BATCH_COMMIT, BATCH_DELETE, BATCH_DISPATCH, BATCH_FAILED, BATCH_SKIPPED,
    BATCH_SUCCEEDED, BATCH_TOUCH,
};
use crate::client::Stored;
use crate::error::{Error, Result};
use crate::keys;
use crate::lua::{LuaScript, ScriptCall};
use crate::redis::RedisPool;
use crate::unique::{OnConflict, UniqueUntil};

/// The six keys of a batch, in the order every script lists them: hash, pending, failed, dead,
/// kids, commits.
pub(crate) fn batch_keys(bid: &str) -> [String; 6] {
    [
        keys::batch(bid),
        keys::batch_pending(bid),
        keys::batch_failed(bid),
        keys::batch_dead(bid),
        keys::batch_kids(bid),
        keys::batch_commits(bid),
    ]
}

/// Whether a batch of a commit is created or reopened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommitMode {
    /// A new batch: it must not exist.
    New,
    /// An existing batch gaining members (and children): it must exist, not have succeeded and
    /// not be cancelled.
    Open,
}

impl CommitMode {
    fn as_str(self) -> &'static str {
        match self {
            CommitMode::New => "new",
            CommitMode::Open => "open",
        }
    }
}

/// One batch of a commit.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CommitBatch {
    /// Created or reopened.
    pub(crate) mode: CommitMode,
    /// The batch.
    pub(crate) bid: String,
    /// The 1-based index of its parent among the commit's batches (0: none); only new batches
    /// have one, and it comes before them.
    pub(crate) parent: usize,
    /// Expiry after the last activity, seconds (new batches).
    pub(crate) ttl_secs: u64,
    /// Linger once finished, seconds (new batches).
    pub(crate) linger_secs: u64,
    /// Description (`""`: none).
    pub(crate) description: String,
    /// The callback templates of `complete`, `success` and `death` (JSON arrays, `""`: none).
    pub(crate) callbacks: [String; 3],
    /// More fields of the hash (new batches: the workflow's `wf`).
    pub(crate) extra: Vec<(String, String)>,
}

/// One member of a commit.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CommitJob {
    /// The 1-based index of its batch among the commit's batches.
    pub(crate) batch: usize,
    /// Its jid.
    pub(crate) jid: String,
    /// Where it goes and its JSON.
    pub(crate) stored: Stored,
    /// Its lock, for a unique member.
    pub(crate) unique: Option<UniqueMember>,
}

/// The lock of a unique member of a commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UniqueMember {
    /// The digest.
    pub(crate) digest: String,
    /// The job's class.
    pub(crate) class: String,
    /// The job's queue.
    pub(crate) queue: String,
    /// When the lock is released.
    pub(crate) until: UniqueUntil,
    /// The lock's time to live, ms (`for`).
    pub(crate) for_ms: u64,
    /// `Drop` or `Error` (`Replace` is refused when the job is pushed to the collector).
    pub(crate) on_conflict: OnConflict,
}

/// The keys and arguments of [`BATCH_COMMIT`] for commit `cid` of `batches` (the root first)
/// and `jobs`, at `now` (the minute of the counters).
pub(crate) fn commit_call(
    cid: &str,
    batches: &[CommitBatch],
    jobs: &[CommitJob],
    now: DateTime<Utc>,
) -> (Vec<String>, Vec<String>) {
    let mut keys = vec![
        keys::QUEUES.to_owned(),
        keys::SCHEDULE.to_owned(),
        keys::BATCHES.to_owned(),
        keys::BATCH_EVENTS.to_owned(),
        keys::batch_minute(now),
    ];
    let mut argv = vec![cid.to_owned(), batches.len().to_string()];
    for batch in batches {
        keys.extend(batch_keys(&batch.bid));
        argv.push(batch.mode.as_str().to_owned());
        argv.push(batch.bid.clone());
        argv.push(batch.parent.to_string());
        argv.push(batch.ttl_secs.to_string());
        argv.push(batch.linger_secs.to_string());
        argv.push(batch.description.clone());
        argv.extend(batch.callbacks.iter().cloned());
        argv.push(batch.extra.len().to_string());
        for (field, value) in &batch.extra {
            argv.push(field.clone());
            argv.push(value.clone());
        }
    }
    // The destination lists, once each (the schedule is KEYS[2]), by their 1-based index.
    let mut destinations: HashMap<String, usize> = HashMap::new();
    let mut job_args = Vec::with_capacity(jobs.len() * 6);
    for job in jobs {
        let (index, kind, target, json) = match &job.stored {
            Stored::Queued { queue, json } => {
                let key = keys::queue(queue);
                let index = match destinations.get(&key) {
                    Some(index) => *index,
                    None => {
                        keys.push(key.clone());
                        destinations.insert(key, keys.len());
                        keys.len()
                    }
                };
                (index, "q", queue.clone(), json)
            }
            Stored::Scheduled { at, json } => (2, "s", at.to_string(), json),
        };
        job_args.push(job.batch.to_string());
        job_args.push(job.jid.clone());
        job_args.push(index.to_string());
        job_args.push(kind.to_owned());
        job_args.push(target);
        job_args.push(json.clone());
    }
    argv.push(jobs.len().to_string());
    argv.extend(job_args);
    let unique: Vec<(usize, &CommitJob, &UniqueMember)> = jobs
        .iter()
        .enumerate()
        .filter_map(|(index, job)| job.unique.as_ref().map(|unique| (index + 1, job, unique)))
        .collect();
    argv.push(unique.len().to_string());
    if !unique.is_empty() {
        keys.push(keys::UNIQUE_LOCKS.to_owned());
        argv.push(keys.len().to_string());
        keys.push(keys::unique_minute(now));
        let mut locks: HashMap<String, usize> = HashMap::new();
        for (index, _, member) in unique {
            let lock = keys::unique_lock(&member.digest);
            let lock_index = match locks.get(&lock) {
                Some(lock_index) => *lock_index,
                None => {
                    keys.push(lock.clone());
                    locks.insert(lock, keys.len());
                    keys.len()
                }
            };
            argv.push(index.to_string());
            argv.push(lock_index.to_string());
            argv.push(member.digest.clone());
            argv.push(member.class.clone());
            argv.push(member.queue.clone());
            argv.push(member.until.as_str().to_owned());
            argv.push(member.for_ms.to_string());
            argv.push(member.on_conflict.as_str().to_owned());
        }
    }
    (keys, argv)
}

/// What [`BATCH_COMMIT`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CommitReply {
    /// Written now.
    Applied {
        /// The new batches that fired at once (no member, no child), deepest first.
        fired: Vec<String>,
        /// Unique members left out: `(jid, holder)`.
        duplicates: Vec<(String, String)>,
        /// Unique members written without their lock: `(jid, error)`.
        unlocked: Vec<(String, String)>,
    },
    /// The commit id had been applied: nothing written now.
    Already {
        /// The duplicates that commit left out: `(jid, holder)`.
        duplicates: Vec<(String, String)>,
    },
}

/// Runs [`BATCH_COMMIT`] once on `conn` (the caller decides about resends).
pub(crate) async fn commit_once(
    conn: &mut crate::redis::RedisConnection,
    keys: &[String],
    argv: &[String],
) -> redis::RedisResult<Value> {
    let mut invocation = BATCH_COMMIT.script().prepare_invoke();
    for key in keys {
        invocation.key(key);
    }
    for arg in argv {
        invocation.arg(arg);
    }
    invocation.invoke_async(conn).await
}

/// The reply of [`BATCH_COMMIT`].
pub(crate) fn parse_commit(reply: Value) -> Result<CommitReply> {
    let items = array(reply, "BATCH_COMMIT")?;
    let mut items = items.into_iter();
    let status = text(&items.next().unwrap_or(Value::Nil))?;
    match status.as_str() {
        "ok" => {
            let fired = strings(items.next().unwrap_or(Value::Nil), "BATCH_COMMIT")?;
            let duplicates = pairs(strings(items.next().unwrap_or(Value::Nil), "BATCH_COMMIT")?);
            let unlocked = pairs(strings(items.next().unwrap_or(Value::Nil), "BATCH_COMMIT")?);
            Ok(CommitReply::Applied {
                fired,
                duplicates,
                unlocked,
            })
        }
        "already" => {
            let record = text(&items.next().unwrap_or(Value::Nil))?;
            let duplicates = if record.is_empty() {
                Vec::new()
            } else {
                let flat: Vec<String> = serde_json::from_str(&record)
                    .map_err(|_| unexpected("BATCH_COMMIT", "an unreadable commit record"))?;
                pairs(flat)
            };
            Ok(CommitReply::Already { duplicates })
        }
        other => Err(unexpected(
            "BATCH_COMMIT",
            &format!("an unknown status `{other}`"),
        )),
    }
}

/// The error a refusal of [`BATCH_COMMIT`] stands for: a [`BatchError`], or
/// [`Error::DuplicateJob`] for a unique member with `OnConflict::Error` (`jobs` are the
/// commit's, to name it); `None` for other errors (kept as [`Error::Redis`]).
pub(crate) fn commit_refusal(error: &RedisError, jobs: &[CommitJob]) -> Option<Error> {
    let code = error.code()?;
    let detail = error.detail().unwrap_or_default().trim().to_owned();
    let batch = |error: BatchError| Some(Error::Batch(error));
    match code {
        "NOBATCH" => batch(BatchError::NotFound { bid: detail }),
        "SUCCEEDED" => batch(BatchError::Succeeded { bid: detail }),
        "CANCELLED" => batch(BatchError::Cancelled { bid: detail }),
        "EXISTS" => batch(BatchError::Exists { bid: detail }),
        "DUPLICATE" => {
            let (index, holder) = detail.split_once(' ')?;
            let job = jobs.get(index.parse::<usize>().ok()?.checked_sub(1)?)?;
            let member = job.unique.as_ref()?;
            Some(Error::DuplicateJob {
                class: member.class.clone(),
                digest: member.digest.clone(),
                holder: holder.to_owned(),
            })
        }
        _ => None,
    }
}

/// How a member's outcome is recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// `BATCH_SUCCEEDED`.
    Succeeded,
    /// `BATCH_FAILED` with the kind (`retry` or `dead`) and the failure summary (JSON).
    Failed {
        /// The member will run again (`false`: it is dead).
        retry: bool,
        /// The summary kept in `:failed`.
        summary: String,
    },
    /// `BATCH_SKIPPED`: its batch was cancelled (`true`; verified by the script) or an
    /// operator skipped it (`false`).
    Skipped {
        /// Skipped because the batch was cancelled.
        cancelled: bool,
    },
}

/// What an accounting script did.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct Accounted {
    /// The member was pending and its outcome was recorded now.
    pub(crate) applied: bool,
    /// `BATCH_SKIPPED` with a cancellation, for a batch that is not cancelled: nothing was
    /// recorded.
    pub(crate) not_cancelled: bool,
    /// The batch has events waiting in the outbox.
    pub(crate) dispatch: bool,
    /// The batch's expiry was refreshed (refresh its ancestors).
    pub(crate) refreshed: bool,
    /// The events fired now.
    pub(crate) fired: Vec<String>,
}

/// The keys and arguments of an accounting script for member `jid` of batch `bid`.
pub(crate) fn account_call(
    bid: &str,
    jid: &str,
    outcome: &Outcome,
    now: DateTime<Utc>,
) -> (&'static LuaScript, Vec<String>, Vec<String>) {
    let mut keys = batch_keys(bid).to_vec();
    keys.push(keys::BATCHES.to_owned());
    keys.push(keys::BATCH_EVENTS.to_owned());
    keys.push(keys::batch_minute(now));
    let mut argv = vec![jid.to_owned(), bid.to_owned()];
    let script = match outcome {
        Outcome::Succeeded => &BATCH_SUCCEEDED,
        Outcome::Failed { retry, summary } => {
            argv.push(if *retry { "retry" } else { "dead" }.to_owned());
            argv.push(summary.clone());
            &BATCH_FAILED
        }
        Outcome::Skipped { cancelled } => {
            argv.push(if *cancelled { "cancel" } else { "operator" }.to_owned());
            &BATCH_SKIPPED
        }
    };
    (script, keys, argv)
}

/// The reply of an accounting script.
pub(crate) fn parse_accounted(reply: Value) -> Result<Accounted> {
    let items = array(reply, "batch accounting")?;
    let applied = int(items.first().unwrap_or(&Value::Nil))?;
    let dispatch = int(items.get(1).unwrap_or(&Value::Nil))? == 1;
    let refreshed = int(items.get(2).unwrap_or(&Value::Nil))? == 1;
    let fired = items.iter().skip(3).map(text).collect::<Result<Vec<_>>>()?;
    Ok(Accounted {
        applied: applied == 1,
        not_cancelled: applied == -1,
        dispatch,
        refreshed,
        fired,
    })
}

/// Records the outcome of member `jid` of batch `bid` (one script, through `with_retry`).
pub(crate) async fn account(
    redis: &RedisPool,
    bid: &str,
    jid: &str,
    outcome: &Outcome,
) -> Result<Accounted> {
    let (script, keys, argv) = account_call(bid, jid, outcome, Utc::now());
    parse_accounted(run(redis, script, &keys, &argv).await?)
}

/// What a dispatch did.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct Dispatched {
    /// The batch was in the outbox and its events were dispatched now.
    pub(crate) dispatched: bool,
    /// Its parent (when it has one).
    pub(crate) parent: Option<String>,
    /// The parent's expiry was refreshed (refresh its ancestors).
    pub(crate) parent_refreshed: bool,
    /// The parent has events waiting in the outbox (dispatch it next).
    pub(crate) parent_dispatch: bool,
    /// The events the dispatch fired in the parent.
    pub(crate) parent_fired: Vec<String>,
    /// The callbacks pushed.
    pub(crate) callbacks: u64,
    /// The events dispatched.
    pub(crate) events: Vec<String>,
}

/// The fields of a batch the dispatch reads first, to declare its keys: the parent and the
/// callback templates' queues.
pub(crate) struct DispatchTarget {
    /// The parent's bid (`""`: none).
    pub(crate) parent: String,
    /// The callbacks' queues, sorted, once each.
    pub(crate) queues: Vec<String>,
}

/// One callback template as stored (`cb_<event>`: a JSON array of them).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct Template {
    /// The queue.
    pub(crate) q: String,
    /// The jid.
    pub(crate) jid: String,
    /// The job's JSON, without `enqueued_at` (the dispatch adds it).
    pub(crate) j: String,
}

/// The templates of a stored `cb_<event>` field (an empty list for none).
pub(crate) fn parse_templates(raw: Option<&str>) -> Result<Vec<Template>> {
    match raw {
        None | Some("") => Ok(Vec::new()),
        Some(raw) => serde_json::from_str(raw).map_err(Error::Json),
    }
}

/// Reads the parent and the callback queues of batch `bid`.
pub(crate) async fn dispatch_target(redis: &RedisPool, bid: &str) -> Result<DispatchTarget> {
    let hash = keys::batch(bid);
    let fields: Vec<Option<String>> = redis
        .with_retry(|mut conn| {
            let hash = hash.clone();
            async move {
                redis::cmd("HMGET")
                    .arg(hash)
                    .arg(&["parent", "cb_death", "cb_complete", "cb_success"])
                    .query_async(&mut conn)
                    .await
            }
        })
        .await?;
    let mut fields = fields.into_iter();
    let parent = fields.next().flatten().unwrap_or_default();
    let mut queues = Vec::new();
    for raw in fields {
        for template in parse_templates(raw.as_deref())? {
            queues.push(template.q);
        }
    }
    queues.sort_unstable();
    queues.dedup();
    Ok(DispatchTarget { parent, queues })
}

/// Runs [`BATCH_DISPATCH`] for batch `bid`, whose parent and callback queues are `target`.
pub(crate) async fn dispatch(
    redis: &RedisPool,
    bid: &str,
    target: &DispatchTarget,
) -> Result<Dispatched> {
    let mut keys = vec![
        keys::BATCH_EVENTS.to_owned(),
        keys::BATCHES.to_owned(),
        keys::QUEUES.to_owned(),
    ];
    keys.extend(batch_keys(bid));
    if target.parent.is_empty() {
        keys.extend(batch_keys(bid));
    } else {
        keys.extend(batch_keys(&target.parent));
    }
    keys.push(keys::batch_minute(Utc::now()));
    let mut argv = vec![
        bid.to_owned(),
        target.parent.clone(),
        target.queues.len().to_string(),
    ];
    for queue in &target.queues {
        keys.push(keys::queue(queue));
        argv.push(queue.clone());
    }
    let items = array(
        run(redis, &BATCH_DISPATCH, &keys, &argv).await?,
        "BATCH_DISPATCH",
    )?;
    let dispatched = int(items.first().unwrap_or(&Value::Nil))? == 1;
    let parent_refreshed = int(items.get(1).unwrap_or(&Value::Nil))? == 1;
    let parent_dispatch = int(items.get(2).unwrap_or(&Value::Nil))? == 1;
    let callbacks = u64::try_from(int(items.get(3).unwrap_or(&Value::Int(0)))?).unwrap_or(0);
    let events = text(items.get(4).unwrap_or(&Value::Nil))?
        .split(',')
        .filter(|event| !event.is_empty())
        .map(str::to_owned)
        .collect();
    let parent_fired = items.iter().skip(5).map(text).collect::<Result<Vec<_>>>()?;
    Ok(Dispatched {
        dispatched,
        parent: (!target.parent.is_empty()).then(|| target.parent.clone()),
        parent_refreshed,
        parent_dispatch,
        parent_fired,
        callbacks,
        events,
    })
}

/// What [`BATCH_TOUCH`] did to one batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Touched {
    /// Its parent (`None` for a root, a batch that is gone or a finished one).
    pub(crate) parent: Option<String>,
    /// Its lifetime now, ms (0 when it was not refreshed: gone or finished).
    pub(crate) ttl_ms: u64,
}

/// Runs [`BATCH_TOUCH`] for batch `bid`, giving it at least `floor_ms` of life (the lifetime of
/// the child the walk comes from).
pub(crate) async fn touch(redis: &RedisPool, bid: &str, floor_ms: u64) -> Result<Touched> {
    let mut keys = batch_keys(bid).to_vec();
    keys.push(keys::BATCHES.to_owned());
    let argv = [bid.to_owned(), floor_ms.to_string()];
    let items = array(run(redis, &BATCH_TOUCH, &keys, &argv).await?, "BATCH_TOUCH")?;
    let parent = text(items.first().unwrap_or(&Value::Nil))?;
    let ttl_ms = u64::try_from(int(items.get(1).unwrap_or(&Value::Int(0)))?).unwrap_or(0);
    Ok(Touched {
        parent: (!parent.is_empty()).then_some(parent),
        ttl_ms,
    })
}

/// What a cancel did to one batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Cancelled {
    /// The first cancel of this batch.
    pub(crate) first: bool,
    /// The batch has events waiting in the outbox.
    pub(crate) dispatch: bool,
    /// Its children, to cancel next.
    pub(crate) children: Vec<String>,
}

/// Runs [`BATCH_CANCEL`] for batch `bid`. A missing batch is [`BatchError::NotFound`], a
/// succeeded one [`BatchError::Succeeded`].
pub(crate) async fn cancel(redis: &RedisPool, bid: &str) -> Result<Cancelled> {
    let call = cancel_call(bid, Utc::now());
    let reply = run(redis, call.script, &call.keys, &call.args)
        .await
        .map_err(batch_refusal)?;
    parse_cancelled(reply)
}

/// Runs [`BATCH_CANCEL`] for each of `bids` in one pipeline (each script runs on its own, as
/// [`cancel`] runs it): one result per batch, in order, with the same refusals. The error of
/// the call itself is the pipeline's (Redis unavailable).
pub(crate) async fn cancel_each(
    redis: &RedisPool,
    bids: &[String],
) -> Result<Vec<Result<Cancelled>>> {
    let now = Utc::now();
    let calls: Vec<ScriptCall> = bids.iter().map(|bid| cancel_call(bid, now)).collect();
    let replies = crate::lua::run_pipelined(redis, &calls).await?;
    Ok(replies
        .into_iter()
        .map(|reply| {
            reply
                .map_err(|error| batch_refusal(Error::Redis(error)))
                .and_then(parse_cancelled)
        })
        .collect())
}

/// The call of [`BATCH_CANCEL`] for batch `bid`, counting in the minute of `now`.
fn cancel_call(bid: &str, now: DateTime<Utc>) -> ScriptCall {
    let mut keys = batch_keys(bid).to_vec();
    keys.push(keys::BATCHES.to_owned());
    keys.push(keys::BATCH_EVENTS.to_owned());
    keys.push(keys::batch_minute(now));
    ScriptCall {
        script: &BATCH_CANCEL,
        keys,
        args: vec![bid.to_owned()],
    }
}

/// The reply of [`BATCH_CANCEL`].
fn parse_cancelled(reply: Value) -> Result<Cancelled> {
    let items = array(reply, "BATCH_CANCEL")?;
    Ok(Cancelled {
        first: int(items.first().unwrap_or(&Value::Nil))? == 1,
        dispatch: int(items.get(1).unwrap_or(&Value::Nil))? == 1,
        children: items.iter().skip(2).map(text).collect::<Result<Vec<_>>>()?,
    })
}

/// Runs [`BATCH_DELETE`] for batch `bid`, whose parent is `parent`; returns the number of keys
/// removed. An unfinished child of a live parent is [`BatchError::DeleteChild`] unless
/// `force`.
pub(crate) async fn delete(
    redis: &RedisPool,
    bid: &str,
    parent: Option<&str>,
    force: bool,
) -> Result<u64> {
    let mut keys = batch_keys(bid).to_vec();
    keys.push(keys::BATCHES.to_owned());
    keys.push(keys::BATCH_EVENTS.to_owned());
    keys.push(keys::batch(parent.unwrap_or(bid)));
    let argv = [bid.to_owned(), if force { "1" } else { "0" }.to_owned()];
    let reply = run(redis, &BATCH_DELETE, &keys, &argv)
        .await
        .map_err(|error| match &error {
            Error::Redis(redis_error) if redis_error.code() == Some("DELETE_CHILD") => {
                Error::Batch(BatchError::DeleteChild {
                    bid: bid.to_owned(),
                    parent: redis_error.detail().unwrap_or_default().trim().to_owned(),
                })
            }
            _ => error,
        })?;
    Ok(u64::try_from(int(&reply)?).unwrap_or(0))
}

/// Runs [`BATCH_DELETE`] for each of `bids`, forced (the descendants of a batch being deleted:
/// their parents go too), in one pipeline; returns the number of keys removed. Every delete is
/// sent; the first that failed is the error (a repeat is harmless: deleting is idempotent).
pub(crate) async fn delete_each(redis: &RedisPool, bids: &[String]) -> Result<u64> {
    let calls: Vec<ScriptCall> = bids
        .iter()
        .map(|bid| {
            let mut keys = batch_keys(bid).to_vec();
            keys.push(keys::BATCHES.to_owned());
            keys.push(keys::BATCH_EVENTS.to_owned());
            keys.push(keys::batch(bid));
            ScriptCall {
                script: &BATCH_DELETE,
                keys,
                args: vec![bid.clone(), "1".to_owned()],
            }
        })
        .collect();
    let mut removed = 0;
    for reply in crate::lua::run_pipelined(redis, &calls).await? {
        removed += u64::try_from(int(&reply?)?).unwrap_or(0);
    }
    Ok(removed)
}

/// Maps the `NOBATCH` and `SUCCEEDED` refusals of a script to their [`BatchError`].
fn batch_refusal(error: Error) -> Error {
    let Error::Redis(redis_error) = &error else {
        return error;
    };
    let bid = redis_error.detail().unwrap_or_default().trim().to_owned();
    match redis_error.code() {
        Some("NOBATCH") => Error::Batch(BatchError::NotFound { bid }),
        Some("SUCCEEDED") => Error::Batch(BatchError::Succeeded { bid }),
        _ => error,
    }
}

/// Runs one of the batch scripts with `keys` and `args`, through `with_retry`.
pub(crate) async fn run(
    redis: &RedisPool,
    script: &'static LuaScript,
    keys: &[String],
    args: &[String],
) -> Result<Value> {
    redis
        .with_retry(|mut conn| async move {
            let mut invocation = script.script().prepare_invoke();
            for key in keys {
                invocation.key(key);
            }
            for arg in args {
                invocation.arg(arg);
            }
            invocation.invoke_async(&mut conn).await
        })
        .await
}

fn unexpected(script: &str, what: &str) -> Error {
    Error::Redis(RedisError::from((
        ErrorKind::UnexpectedReturnType,
        "unexpected reply from a batch script",
        format!("{script}: {what}"),
    )))
}

fn array(value: Value, script: &str) -> Result<Vec<Value>> {
    match value {
        Value::Array(items) | Value::Set(items) => Ok(items),
        Value::Nil => Ok(Vec::new()),
        _ => Err(unexpected(script, "not an array")),
    }
}

fn strings(value: Value, script: &str) -> Result<Vec<String>> {
    array(value, script)?.iter().map(text).collect()
}

/// `[a, b, c, d]` as `[(a, b), (c, d)]` (an odd last element is dropped).
fn pairs(flat: Vec<String>) -> Vec<(String, String)> {
    let mut flat = flat.into_iter();
    let mut pairs = Vec::new();
    while let (Some(first), Some(second)) = (flat.next(), flat.next()) {
        pairs.push((first, second));
    }
    pairs
}

fn int(value: &Value) -> Result<i64> {
    match value {
        Value::Int(number) => Ok(*number),
        Value::BulkString(bytes) => std::str::from_utf8(bytes)
            .ok()
            .and_then(|text| text.parse().ok())
            .ok_or_else(|| unexpected("batch script", "not an integer")),
        Value::SimpleString(text) => text
            .parse()
            .map_err(|_| unexpected("batch script", "not an integer")),
        _ => Err(unexpected("batch script", "not an integer")),
    }
}

fn text(value: &Value) -> Result<String> {
    match value {
        Value::BulkString(bytes) => Ok(String::from_utf8_lossy(bytes).into_owned()),
        Value::SimpleString(text) => Ok(text.clone()),
        Value::Int(number) => Ok(number.to_string()),
        Value::Nil => Ok(String::new()),
        Value::Okay => Ok("OK".to_owned()),
        _ => Err(unexpected("batch script", "not a string")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn queued(batch: usize, jid: &str, queue: &str) -> CommitJob {
        CommitJob {
            batch,
            jid: jid.to_owned(),
            stored: Stored::Queued {
                queue: queue.to_owned(),
                json: format!("{{\"jid\":\"{jid}\"}}"),
            },
            unique: None,
        }
    }

    fn batch(mode: CommitMode, bid: &str, parent: usize) -> CommitBatch {
        CommitBatch {
            mode,
            bid: bid.to_owned(),
            parent,
            ttl_secs: 3_600,
            linger_secs: 60,
            description: String::new(),
            callbacks: Default::default(),
            extra: Vec::new(),
        }
    }

    #[test]
    fn commit_calls_declare_every_key_once() {
        let mut unique = queued(1, "c", "a");
        unique.unique = Some(UniqueMember {
            digest: "d1".to_owned(),
            class: "C".to_owned(),
            queue: "a".to_owned(),
            until: UniqueUntil::Success,
            for_ms: 60_000,
            on_conflict: OnConflict::Drop,
        });
        let mut same_lock = queued(2, "e", "a");
        same_lock.unique = unique.unique.clone();
        let jobs = vec![
            queued(1, "a", "a"),
            queued(2, "b", "b"),
            unique,
            CommitJob {
                batch: 1,
                jid: "d".to_owned(),
                stored: Stored::Scheduled {
                    at: 1_800_000_000.5,
                    json: "{}".to_owned(),
                },
                unique: None,
            },
            same_lock,
        ];
        let batches = [
            batch(CommitMode::New, "root", 0),
            batch(CommitMode::New, "kid", 1),
        ];
        let now = Utc::now();
        let (keys, argv) = commit_call("cid", &batches, &jobs, now);
        // 5 fixed keys, 6 per batch, the two queues, then unique-locks, the minute, one lock.
        assert_eq!(keys.len(), 5 + 12 + 2 + 3);
        assert_eq!(keys[17], "queue:a");
        assert_eq!(keys[18], "queue:b");
        assert_eq!(keys[19], keys::UNIQUE_LOCKS);
        assert_eq!(keys[21], "unique:d1");
        // Batches: 10 values each without extras.
        assert_eq!(&argv[..4], ["cid", "2", "new", "root"]);
        let jobs_at = 2 + 20;
        assert_eq!(argv[jobs_at], "5");
        // Job 1 goes to KEYS[18] (queue:a), job 2 to KEYS[19] (queue:b), job 4 to the schedule.
        assert_eq!(argv[jobs_at + 3], "18");
        assert_eq!(argv[jobs_at + 6 + 3], "19");
        assert_eq!(argv[jobs_at + 18 + 3], "2");
        assert_eq!(argv[jobs_at + 18 + 5], "1800000000.5");
        let unique_at = jobs_at + 1 + 30;
        assert_eq!(argv[unique_at], "2");
        assert_eq!(argv[unique_at + 1], "20");
        // Both unique members use the one lock key, KEYS[22].
        assert_eq!(argv[unique_at + 2], "3");
        assert_eq!(argv[unique_at + 3], "22");
        assert_eq!(argv[unique_at + 10], "5");
        assert_eq!(argv[unique_at + 11], "22");
        assert_eq!(argv.len(), unique_at + 2 + 16);
    }

    #[test]
    fn replies_and_refusals_are_read() {
        let reply = Value::Array(vec![
            Value::BulkString(b"ok".to_vec()),
            Value::Array(vec![Value::BulkString(b"b1".to_vec())]),
            Value::Array(vec![
                Value::BulkString(b"j1".to_vec()),
                Value::BulkString(b"h1".to_vec()),
            ]),
            Value::Array(vec![]),
        ]);
        assert_eq!(
            parse_commit(reply).unwrap(),
            CommitReply::Applied {
                fired: vec!["b1".to_owned()],
                duplicates: vec![("j1".to_owned(), "h1".to_owned())],
                unlocked: Vec::new(),
            }
        );
        let already = Value::Array(vec![
            Value::BulkString(b"already".to_vec()),
            Value::BulkString(br#"["j1","h1"]"#.to_vec()),
        ]);
        assert_eq!(
            parse_commit(already).unwrap(),
            CommitReply::Already {
                duplicates: vec![("j1".to_owned(), "h1".to_owned())]
            }
        );
        let error = redis::make_extension_error("EXISTS".into(), Some("abc".into()));
        assert!(matches!(
            commit_refusal(&error, &[]),
            Some(Error::Batch(BatchError::Exists { bid })) if bid == "abc"
        ));
        let other = redis::make_extension_error("WRONGTYPE".into(), Some("queue:a".into()));
        assert!(commit_refusal(&other, &[]).is_none());
        assert_eq!(
            pairs(vec!["a".into(), "b".into(), "c".into()]),
            [("a".to_owned(), "b".to_owned())]
        );
    }
}
