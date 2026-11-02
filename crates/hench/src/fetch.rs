//! Fetching jobs from Redis: basic (`BRPOP`) and reliable (atomic move to a per-process
//! in-progress list, with orphan recovery). See [DESIGN.md] §5.
//!
//! A [`Fetcher`] takes jobs from the queues of one capsule; each processor uses its own
//! [`FetchWorker`] (it owns a dedicated connection for blocking commands). Fetched jobs are
//! [`UnitOfWork`]s, which are acknowledged once handled or requeued if they could not be run.
//!
//! * [`FetchStrategy::Basic`]: a fetched job is removed from its queue, so a process that
//!   crashes while running it loses it. Acknowledging is a no-op.
//! * [`FetchStrategy::Reliable`] (the default): a fetched job moves atomically from its queue
//!   to the process's in-progress list (`<identity>:inprogress`) and is removed from there
//!   when acknowledged. Jobs left behind by processes that died are moved back to their queues
//!   by [`Fetcher::recover_orphans`].
//!
//! **Poison pills.** A job that kills the process running it (an out-of-memory kill, an abort,
//! a crash in foreign code) would be recovered and crash the next process, forever. Each
//! recovery from a dead process is therefore counted per job ([`keys::recoveries`], forgotten
//! [`RECOVERY_WINDOW`] after the last one, or when an iterable job saves progress): past
//! [`max_recoveries`](crate::Config::max_recoveries) (3 by default), the next recovery moves the
//! job to the morgue with the error class [`JobError::CRASH_LOOP`](crate::JobError::CRASH_LOOP)
//! (dropped instead for `dead: false`), counts it dead in its batch, releases its unique lock and
//! runs the death and error handlers of the recovering process. So that the jobs that merely ran
//! beside a poison pill do not share its fate, a job recovered before (a suspect) comes back
//! alone: the suspects wait in [`keys::RECOVERY_SUSPECTS`] and each orphan check releases one,
//! the most recovered first. Jobs pushed back at a shutdown, or by a process from its own list,
//! are not counted.
//!
//! Every fetch first tries all queues without blocking, in the capsule's order (strict, or a
//! fresh weighted random order each time), skipping queues listed in the
//! [`paused`](crate::keys::PAUSED) set. When they are all empty it blocks for up to the fetch
//! timeout: on every open queue (`BRPOP`) for basic fetch, on one queue (`BLMOVE`) for
//! reliable fetch, rotating the queue each processor waits on so that every queue has waiters.
//!
//! [DESIGN.md]: https://github.com/rkwap/hench/blob/v0.1.0/docs/DESIGN.md
//
// LANE: engine — this module is owned by the engine lane.

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use ::redis::Script;
use tokio_util::sync::CancellationToken;

mod poison;

use crate::client::Stored;
use crate::config::{CapsuleConfig, Config, FetchStrategy, QueueMode};
use crate::error::{Error, Result};
use crate::keys;
use crate::payload::JobPayload;
use crate::redis::{RedisConnection, RedisPool};
use crate::retry::MorgueBounds;
use crate::unique::LockOp;

/// The default time a fetch blocks waiting for a job.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);

/// Jobs per script call when requeueing or recovering many jobs.
const BATCH: usize = 100;

/// How long the recoveries of a job are remembered after the last one (72 hours, as Sidekiq
/// Pro's super_fetch counts them): a job recovered more than
/// [`max_recoveries`](crate::Config::max_recoveries) times, each within this window of the
/// previous one, is a poison pill (see the [module documentation](self)).
pub const RECOVERY_WINDOW: Duration = Duration::from_secs(72 * 60 * 60);

/// How long the claim on a poison pill's settlement ([`keys::recovery_claim`]) lasts: longer than
/// any orphan check that could overlap the one settling it, which then leaves the pill alone.
pub const RECOVERY_CLAIM_TTL: Duration = Duration::from_secs(10 * 60);

/// Reliable fetch, non-blocking step. Moves the next job of the first open (not paused),
/// non-empty queue to the in-progress list and registers the process as an owner of an
/// in-progress list.
///
/// KEYS: in-progress list, in-progress owners set, paused set, then the queue lists in fetch
/// order. ARGV: the process identity, then the queue names (`ARGV[i]` names `KEYS[i + 2]`).
/// Returns `{1, {queue_key, job}}`, or `{0, {open queue keys}}` when every queue is empty.
const RELIABLE_FETCH: &str = r"
local inprogress, owners, paused = KEYS[1], KEYS[2], KEYS[3]
local open = {}
for i = 4, #KEYS do
  if redis.call('SISMEMBER', paused, ARGV[i - 2]) == 0 then
    local job = redis.call('LMOVE', KEYS[i], inprogress, 'RIGHT', 'LEFT')
    if job then
      redis.call('SADD', owners, ARGV[1])
      return {1, {KEYS[i], job}}
    end
    open[#open + 1] = KEYS[i]
  end
end
return {0, open}
";

/// Basic fetch, non-blocking step: pops the next job of the first open, non-empty queue.
///
/// KEYS: paused set, then the queue lists in fetch order. ARGV: the queue names (`ARGV[i]`
/// names `KEYS[i + 1]`). Returns like [`RELIABLE_FETCH`].
const BASIC_FETCH: &str = r"
local paused = KEYS[1]
local open = {}
for i = 2, #KEYS do
  if redis.call('SISMEMBER', paused, ARGV[i - 1]) == 0 then
    local job = redis.call('RPOP', KEYS[i])
    if job then
      return {1, {KEYS[i], job}}
    end
    open[#open + 1] = KEYS[i]
  end
end
return {0, open}
";

/// Moves jobs from an in-progress list back to the front of their queues. A job that is no
/// longer in the list (already acknowledged or recovered) is not pushed again.
///
/// In-progress lists are scanned from the tail, where the oldest jobs are (fetches push to the
/// head), since jobs mostly finish in the order they were fetched.
///
/// KEYS: the in-progress list, then the destination queue of each job. ARGV: the jobs.
/// Returns how many jobs were moved.
const REQUEUE: &str = r"
local moved = 0
for i = 1, #ARGV do
  if redis.call('LREM', KEYS[1], -1, ARGV[i]) > 0 then
    redis.call('RPUSH', KEYS[i + 1], ARGV[i])
    moved = moved + 1
  end
end
return moved
";

/// Moves jobs that cannot be requeued (unparseable, or without a queue) from an in-progress
/// list to the dead set, trimming it by age and size afterwards.
///
/// KEYS: the in-progress list, the dead set. ARGV: score (now), oldest score to keep, rank
/// bound for the size trim, then the jobs. Returns how many jobs were moved.
const BURY: &str = r"
local moved = 0
for i = 4, #ARGV do
  if redis.call('LREM', KEYS[1], -1, ARGV[i]) > 0 then
    redis.call('ZADD', KEYS[2], ARGV[1], ARGV[i])
    moved = moved + 1
  end
end
if moved > 0 then
  redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', ARGV[2])
  redis.call('ZREMRANGEBYRANK', KEYS[2], 0, ARGV[3])
end
return moved
";

/// Moves jobs from the in-progress list of a dead process back to the front of their queues,
/// like [`REQUEUE`], and counts each one's recovery: the counter of its jid is incremented and
/// expires [`RECOVERY_WINDOW`] later. The copy is written before the job leaves the list (a
/// failing write loses nothing; a job no longer in the list is left alone), and the counter is
/// written under `pcall`: a counter that cannot be written never stops a recovery.
///
/// KEYS: the in-progress list, then for each job its queue and its counter. ARGV: the
/// counters' expiry (milliseconds), then the jobs. Returns how many jobs were moved.
const RECOVER: &str = r"
local moved = 0
for i = 2, #ARGV do
  local job = ARGV[i]
  if redis.call('LPOS', KEYS[1], job, 'RANK', -1) then
    local at = 2 * (i - 1)
    redis.call('RPUSH', KEYS[at], job)
    local counter = KEYS[at + 1]
    pcall(function()
      redis.call('INCR', counter)
      redis.call('PEXPIRE', counter, ARGV[1])
    end)
    redis.call('LREM', KEYS[1], -1, job)
    moved = moved + 1
  end
end
return moved
";

/// Keeps the orphan check's lock ([`keys::INPROGRESS_RECOVERY`]) while a check runs: fails when
/// another process holds it (this check then stops), takes it back when it lapsed, and otherwise
/// makes it last at least the given time.
///
/// KEYS: the lock. ARGV: this process's identity, the least time left on the lock (ms). Returns 1
/// while this process holds the lock, 0 once another one does.
const KEEP_RECOVERY_LOCK: &str = r"
local holder = redis.call('GET', KEYS[1])
if holder and holder ~= ARGV[1] then
  return 0
end
if not holder then
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
elseif redis.call('PTTL', KEYS[1]) < tonumber(ARGV[2]) then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 1
";

/// Unregisters a process as an in-progress list owner once its list is empty.
///
/// KEYS: the in-progress list, the owners set. ARGV: the identity. Returns 1 if the owner was
/// removed, 0 if its list still holds jobs.
const RELEASE: &str = r"
if redis.call('LLEN', KEYS[1]) == 0 then
  redis.call('SREM', KEYS[2], ARGV[1])
  return 1
end
return 0
";

/// Reliable fetch: replaces a job of the in-progress list by a new copy at the back of a queue
/// (or in the schedule), in one step. Nothing is written if the job is no longer in the list
/// (acknowledged or recovered already), so running it twice is harmless; the new copy is
/// written before the job leaves the list, so a failing write (out of memory) loses nothing.
///
/// KEYS: the in-progress list, the queues set, the destination (queue list or schedule).
/// ARGV: the job as fetched, the new copy, the queue name (or the run-at score), then `queue`
/// or `schedule`. Returns 1 if the copy was written, 0 otherwise.
const PUSH_BACK: &str = r"
if not redis.call('LPOS', KEYS[1], ARGV[1], 'RANK', -1) then
  return 0
end
if ARGV[4] == 'queue' then
  redis.call('SADD', KEYS[2], ARGV[3])
  redis.call('LPUSH', KEYS[3], ARGV[2])
else
  redis.call('ZADD', KEYS[3], ARGV[3], ARGV[2])
end
redis.call('LREM', KEYS[1], -1, ARGV[1])
return 1
";

/// Reliable fetch: settles a fetched job that will not run again from its queue, in one step:
/// writes it to the retry set or to the morgue (trimming the morgue by age and size), or writes
/// nothing, then removes it from the in-progress list. As with [`PUSH_BACK`], nothing is written
/// if the job is no longer in the list (so running it twice is harmless), and the write comes
/// before the removal (so a failing write loses nothing): a crash can no longer leave a failed
/// job both in the retry set (or the morgue) and in progress, to run again.
///
/// KEYS: the in-progress list, the set written (retry or dead; any key for `drop`). ARGV: the job
/// as fetched, the mode (`retry`, `dead` or `drop`), the job to write, its score, then for
/// `dead` the oldest score kept and the rank bound of the size trim. Returns 1 if the job was
/// settled, 0 if it was no longer in progress.
const SETTLE: &str = r"
if not redis.call('LPOS', KEYS[1], ARGV[1], 'RANK', -1) then
  return 0
end
if ARGV[2] ~= 'drop' then
  redis.call('ZADD', KEYS[2], ARGV[4], ARGV[3])
  if ARGV[2] == 'dead' then
    redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', ARGV[5])
    redis.call('ZREMRANGEBYRANK', KEYS[2], 0, ARGV[6])
  end
end
redis.call('LREM', KEYS[1], -1, ARGV[1])
return 1
";

pub(crate) static RELIABLE_FETCH_SCRIPT: LazyLock<Script> =
    LazyLock::new(|| Script::new(RELIABLE_FETCH));
pub(crate) static PUSH_BACK_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(PUSH_BACK));
pub(crate) static SETTLE_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(SETTLE));
pub(crate) static BASIC_FETCH_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(BASIC_FETCH));
pub(crate) static REQUEUE_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(REQUEUE));
pub(crate) static BURY_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(BURY));
pub(crate) static RELEASE_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(RELEASE));
static RECOVER_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(RECOVER));
static KEEP_RECOVERY_LOCK_SCRIPT: LazyLock<Script> =
    LazyLock::new(|| Script::new(KEEP_RECOVERY_LOCK));

/// A fetched job: the queue it came from and its JSON, byte for byte as stored in Redis
/// (acknowledging and requeueing need the exact bytes). Cheap to clone: the bytes are shared.
#[derive(Clone, PartialEq, Eq)]
pub struct UnitOfWork {
    queue: String,
    job: Arc<Vec<u8>>,
}

impl UnitOfWork {
    /// A unit of work for `job` fetched from the queue list `queue_key` (`queue:<name>`).
    pub fn new(queue_key: impl Into<String>, job: impl Into<Vec<u8>>) -> Self {
        Self {
            queue: queue_key.into(),
            job: Arc::new(job.into()),
        }
    }

    /// The job's bytes, shared (for the work state and the jobs held by a process).
    pub(crate) fn shared_job(&self) -> &Arc<Vec<u8>> {
        &self.job
    }

    /// Key of the queue list the job came from, e.g. `queue:default`.
    pub fn queue(&self) -> &str {
        &self.queue
    }

    /// Name of the queue the job came from, e.g. `default`.
    pub fn queue_name(&self) -> &str {
        keys::queue_name(&self.queue)
    }

    /// The job's JSON, exactly as stored.
    pub fn job(&self) -> &[u8] {
        &self.job
    }

    /// The job's JSON as text (invalid UTF-8 is replaced, see [`String::from_utf8_lossy`]).
    pub fn job_str(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(&self.job)
    }

    /// Parses the job.
    pub fn parse(&self) -> Result<JobPayload> {
        JobPayload::from_slice(&self.job)
    }
}

impl fmt::Debug for UnitOfWork {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnitOfWork")
            .field("queue", &self.queue)
            .field("job", &self.job_str())
            .finish()
    }
}

/// What becomes of a fetched job that will not run again from its queue (see
/// [`Fetcher::settle`]).
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Settlement {
    /// Added to the retry set, to run at `at` (epoch seconds).
    Retry {
        /// When the job runs again.
        at: f64,
        /// The job to store (with its error fields).
        job: Vec<u8>,
    },
    /// Added to the morgue, which is then trimmed.
    Morgue {
        /// The job to store.
        job: Vec<u8>,
        /// Its score and the trims.
        bounds: MorgueBounds,
    },
    /// Nothing is written: the job is only acknowledged.
    Drop,
}

/// The jobs a process's processors hold: fetched and not yet acknowledged, settled or pushed
/// back. With reliable fetch, every other job in the process's in-progress list was left there
/// by a fetch whose reply was lost (or an outcome that could not be written), and
/// [`Fetcher::reconcile`] pushes it back.
#[derive(Debug, Default)]
pub(crate) struct Held {
    jobs: Mutex<Vec<Arc<Vec<u8>>>>,
}

impl Held {
    /// Records that a processor holds `work` until the returned guard is dropped.
    pub(crate) fn hold(self: &Arc<Self>, work: &UnitOfWork) -> HeldGuard {
        let job = work.shared_job().clone();
        crate::launcher::lock(&self.jobs).push(job.clone());
        HeldGuard {
            held: self.clone(),
            job,
        }
    }

    /// The `entries` (of an in-progress list) that no held job accounts for, each held job
    /// accounting for one entry with the same bytes.
    fn unheld(&self, entries: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
        let jobs = crate::launcher::lock(&self.jobs);
        let mut used = vec![false; jobs.len()];
        entries
            .into_iter()
            .filter(|entry| {
                let found = jobs.iter().zip(used.iter_mut()).find(|(job, used)| {
                    !**used && job.len() == entry.len() && job.as_slice() == entry.as_slice()
                });
                match found {
                    Some((_, used)) => {
                        *used = true;
                        false
                    }
                    None => true,
                }
            })
            .collect()
    }
}

/// Releases a held job when dropped (see [`Held::hold`]).
#[derive(Debug)]
pub(crate) struct HeldGuard {
    held: Arc<Held>,
    job: Arc<Vec<u8>>,
}

impl Drop for HeldGuard {
    fn drop(&mut self) {
        let mut jobs = crate::launcher::lock(&self.held.jobs);
        if let Some(index) = jobs.iter().position(|job| Arc::ptr_eq(job, &self.job)) {
            jobs.swap_remove(index);
        }
    }
}

/// Since when each unheld entry of the in-progress list has been seen unheld, from one
/// [`Fetcher::reconcile`] to the next.
#[derive(Debug, Default)]
pub(crate) struct Stranded {
    since: HashMap<Vec<u8>, Instant>,
}

impl Stranded {
    /// Records the entries seen unheld `now` (forgetting the others) and returns those unheld
    /// for at least `grace`, one per copy in the list.
    fn update(&mut self, unheld: Vec<Vec<u8>>, now: Instant, grace: Duration) -> Vec<Vec<u8>> {
        let mut since = HashMap::with_capacity(unheld.len());
        let mut due = Vec::new();
        for entry in unheld {
            let first = since
                .get(&entry)
                .or_else(|| self.since.get(&entry))
                .copied()
                .unwrap_or(now);
            if now.saturating_duration_since(first) >= grace {
                due.push(entry);
            } else {
                since.insert(entry, first);
            }
        }
        self.since = since;
        due
    }

    /// Forgets when entries were first seen unheld. Used after the checks were held up (a
    /// stalled runtime): a fetch's reply may then be waiting for its processor to run, and the
    /// time an entry spent unheld meanwhile says nothing.
    pub(crate) fn forget(&mut self) {
        self.since.clear();
    }
}

/// Takes jobs from the queues of one capsule. Cheap to clone.
///
/// With [`FetchStrategy::Reliable`], `identity` must belong to a live process: one whose
/// heartbeat key (`<identity>`, see [`keys::process`]) is refreshed at least every minute, as
/// a running hench process does. The orphan check of every hench process treats the
/// in-progress jobs of an identity without a heartbeat key as abandoned and pushes them back
/// to their queues. Tools fetching jobs outside a running process should use
/// [`FetchStrategy::Basic`] or keep such a key alive.
///
/// ```no_run
/// use hench::fetch::Fetcher;
/// use hench::{FetchStrategy, Hench};
///
/// # async fn demo(hench: &Hench) -> hench::Result<()> {
/// let capsule = &hench.config().capsules[0];
/// let fetcher = Fetcher::new(FetchStrategy::Basic, hench.redis().clone(), "tool:1:abc", capsule);
/// let mut worker = fetcher.worker(0);
/// if let Some(work) = worker.retrieve_work().await? {
///     // ... handle the job ...
///     fetcher.acknowledge(&work).await?;
/// }
/// # Ok(()) }
/// ```
#[derive(Clone)]
pub struct Fetcher {
    inner: Arc<FetcherInner>,
}

#[derive(Clone)]
struct FetcherInner {
    strategy: FetchStrategy,
    redis: RedisPool,
    identity: String,
    inprogress: String,
    capsule: String,
    mode: QueueMode,
    names: Vec<String>,
    keys: Vec<String>,
    weights: Vec<u64>,
    timeout: Duration,
    /// How long an owner without a heartbeat key must stay without one before its jobs are
    /// recovered (see [`Fetcher::with_orphan_grace`]).
    orphan_grace: Duration,
}

impl Fetcher {
    /// A fetcher for the queues of `capsule`, fetching on behalf of the process `identity`
    /// (whose in-progress list reliable fetch uses). The fetch timeout is
    /// [`DEFAULT_TIMEOUT`]; see [`Fetcher::with_timeout`].
    pub fn new(
        strategy: FetchStrategy,
        redis: RedisPool,
        identity: impl Into<String>,
        capsule: &CapsuleConfig,
    ) -> Self {
        let identity = identity.into();
        let names: Vec<String> = capsule.queues.clone();
        let keys = names.iter().map(|name| keys::queue(name)).collect();
        let weights = names
            .iter()
            .map(|name| {
                capsule
                    .weights
                    .iter()
                    .find(|(queue, _)| queue == name)
                    .map_or(1, |(_, weight)| u64::from((*weight).max(1)))
            })
            .collect();
        Self {
            inner: Arc::new(FetcherInner {
                strategy,
                redis,
                inprogress: keys::inprogress(&identity),
                identity,
                capsule: capsule.name.clone(),
                mode: capsule.mode,
                names,
                keys,
                weights,
                timeout: DEFAULT_TIMEOUT,
                orphan_grace: Duration::ZERO,
            }),
        }
    }

    /// Sets how long a fetch blocks waiting for a job when every queue is empty (at least one
    /// millisecond).
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        Arc::make_mut(&mut self.inner).timeout = timeout.max(Duration::from_millis(1));
        self
    }

    /// Makes [`Fetcher::recover_orphans`] look twice before it takes a process for dead: an
    /// owner of in-progress jobs found without a heartbeat key is recovered only if it still
    /// has none `grace` later (default zero: at once). Redis can lose the keys of live processes
    /// (a restart without persistence, an old snapshot, `FLUSHALL`), which re-create theirs
    /// within a heartbeat interval, so workers use [`Config::orphan_grace`] (at least twice the
    /// heartbeat interval).
    #[must_use]
    pub fn with_orphan_grace(mut self, grace: Duration) -> Self {
        Arc::make_mut(&mut self.inner).orphan_grace = grace;
        self
    }

    /// The fetch strategy.
    pub fn strategy(&self) -> FetchStrategy {
        self.inner.strategy
    }

    /// The identity of the process this fetcher works for.
    pub fn identity(&self) -> &str {
        &self.inner.identity
    }

    /// The fetch timeout.
    pub fn timeout(&self) -> Duration {
        self.inner.timeout
    }

    /// The queue list keys in the order the next fetch checks them: the configured order in
    /// strict mode, otherwise a fresh random order in which each queue comes first with a
    /// probability proportional to its weight (Sidekiq's weighted ordering).
    pub fn queue_order(&self) -> Vec<&str> {
        self.order()
            .into_iter()
            .map(|index| self.inner.keys[index].as_str())
            .collect()
    }

    /// Indexes of the queues in fetch order.
    fn order(&self) -> Vec<usize> {
        let inner = &self.inner;
        match inner.mode {
            QueueMode::Strict => (0..inner.names.len()).collect(),
            QueueMode::Weighted | QueueMode::Random => weighted_order(&inner.weights),
        }
    }

    /// A worker that fetches jobs; each processor needs its own (it owns a dedicated
    /// connection for blocking commands). `index` spreads the workers of a capsule over its
    /// queues when they block waiting for jobs.
    pub fn worker(&self, index: usize) -> FetchWorker {
        FetchWorker {
            fetcher: self.clone(),
            index,
            turn: 0,
            conn: None,
        }
    }

    /// Marks a job as handled. Reliable fetch removes it from the in-progress list (searching
    /// from the tail, where the oldest jobs are); basic fetch has nothing to do.
    pub async fn acknowledge(&self, work: &UnitOfWork) -> Result<()> {
        if self.inner.strategy == FetchStrategy::Basic {
            return Ok(());
        }
        let inprogress = self.inner.inprogress.as_str();
        let job = work.job();
        self.inner
            .redis
            .with_retry(|mut conn| async move {
                ::redis::cmd("LREM")
                    .arg(inprogress)
                    .arg(-1)
                    .arg(job)
                    .exec_async(&mut conn)
                    .await
            })
            .await
    }

    /// [`Fetcher::acknowledge`], with the unique lock work of the outcome (`lock`) done in the
    /// same step: `None` is exactly [`Fetcher::acknowledge`] (the jobs of every feature but
    /// unique jobs, and every job of a version without them).
    pub(crate) async fn acknowledge_with(
        &self,
        work: &UnitOfWork,
        lock: Option<&LockOp>,
    ) -> Result<()> {
        match lock {
            None => self.acknowledge(work).await,
            Some(op) => crate::unique::engine::ack(self, work, op).await,
        }
    }

    /// [`Fetcher::push_back`], with the unique lock work of the outcome (`lock`, a hold) done in
    /// the same step; `None` is exactly [`Fetcher::push_back`].
    pub(crate) async fn push_back_with(
        &self,
        work: &UnitOfWork,
        job: &Stored,
        lock: Option<&LockOp>,
    ) -> Result<bool> {
        match lock {
            None => self.push_back(work, job).await,
            Some(op) => crate::unique::engine::push_back(self, work, job, op).await,
        }
    }

    /// [`Fetcher::settle`], with the unique lock work of the outcome (`lock`) done in the same
    /// step; `None` is exactly [`Fetcher::settle`].
    pub(crate) async fn settle_with(
        &self,
        work: &UnitOfWork,
        settlement: &Settlement,
        lock: Option<&LockOp>,
    ) -> Result<bool> {
        match lock {
            None => self.settle(work, settlement).await,
            Some(op) => crate::unique::engine::settle(self, work, settlement, op).await,
        }
    }

    /// This fetcher, working on the in-progress list of the process `owner` (the orphan check
    /// settles a dead process's poison pills in its list with the outcome scripts).
    fn for_owner(&self, owner: &str) -> Fetcher {
        let mut inner = (*self.inner).clone();
        inner.inprogress = keys::inprogress(owner);
        inner.identity = owner.to_owned();
        Fetcher {
            inner: Arc::new(inner),
        }
    }

    /// The Redis pool the fetcher writes outcomes to (for the outcome scripts of the features).
    pub(crate) fn redis(&self) -> &RedisPool {
        &self.inner.redis
    }

    /// The in-progress list of the process (reliable fetch), for the outcome scripts of the
    /// features.
    pub(crate) fn inprogress_key(&self) -> &str {
        &self.inner.inprogress
    }

    /// Puts a job back at the front of its queue (it is the next one fetched). With reliable
    /// fetch the job moves atomically out of the in-progress list, and nothing is pushed if
    /// it is no longer there.
    pub async fn requeue(&self, work: &UnitOfWork) -> Result<()> {
        self.bulk_requeue(std::slice::from_ref(work))
            .await
            .map(|_| ())
    }

    /// Puts jobs back at the front of their queues (see [`Fetcher::requeue`]). Returns how
    /// many jobs were pushed back.
    pub async fn bulk_requeue(&self, work: &[UnitOfWork]) -> Result<usize> {
        if work.is_empty() {
            return Ok(0);
        }
        let requeued = match self.inner.strategy {
            FetchStrategy::Basic => self.basic_requeue(work).await?,
            FetchStrategy::Reliable => {
                let jobs: Vec<(&str, &[u8])> =
                    work.iter().map(|work| (work.queue(), work.job())).collect();
                requeue_from(&self.inner.redis, &self.inner.inprogress, &jobs).await?
            }
        };
        tracing::info!(
            capsule = %self.inner.capsule,
            jobs = requeued,
            "pushed unfinished jobs back to their queues"
        );
        Ok(requeued)
    }

    /// Pushes `job` (an encoded copy of a fetched job, see [`Stored`]) to the back of its
    /// queue, or into the schedule, and acknowledges the unit of work it replaces. With
    /// reliable fetch both happen in one step, and nothing is pushed if the unit of work is no
    /// longer in progress (then `false` is returned); with basic fetch a reply lost to a
    /// dropped connection can push the copy twice. Used for interrupted jobs, which continue
    /// later.
    pub(crate) async fn push_back(&self, work: &UnitOfWork, job: &Stored) -> Result<bool> {
        let (destination, member, score_or_queue, mode) = match job {
            Stored::Queued { queue, json } => (keys::queue(queue), json, queue.clone(), "queue"),
            Stored::Scheduled { at, json } => {
                (keys::SCHEDULE.to_owned(), json, at.to_string(), "schedule")
            }
        };
        let (destination, member, score_or_queue) = (&destination, member, &score_or_queue);
        match self.inner.strategy {
            FetchStrategy::Reliable => {
                let inprogress = self.inner.inprogress.as_str();
                let fetched = work.job();
                let pushed: i64 = self
                    .inner
                    .redis
                    .with_retry(|mut conn| async move {
                        PUSH_BACK_SCRIPT
                            .key(inprogress)
                            .key(keys::QUEUES)
                            .key(destination)
                            .arg(fetched)
                            .arg(member)
                            .arg(score_or_queue)
                            .arg(mode)
                            .invoke_async(&mut conn)
                            .await
                    })
                    .await?;
                Ok(pushed == 1)
            }
            FetchStrategy::Basic => {
                let mut pipe = ::redis::pipe();
                pipe.atomic();
                match job {
                    Stored::Queued { queue, json } => {
                        pipe.cmd("SADD").arg(keys::QUEUES).arg(queue).ignore();
                        pipe.cmd("LPUSH").arg(destination).arg(json).ignore();
                    }
                    Stored::Scheduled { at, json } => {
                        pipe.cmd("ZADD")
                            .arg(destination)
                            .arg(*at)
                            .arg(json)
                            .ignore();
                    }
                }
                let pipe = &pipe;
                self.inner
                    .redis
                    .with_retry(|mut conn| async move { pipe.exec_async(&mut conn).await })
                    .await?;
                Ok(true)
            }
        }
    }

    /// Settles a job that will not run again from its queue (see [`Settlement`]): with reliable
    /// fetch, the write and the acknowledgement happen in one step, and nothing is written if
    /// the unit of work is no longer in progress (then `false` is returned); with basic fetch
    /// only the write happens (acknowledging is a no-op).
    pub(crate) async fn settle(&self, work: &UnitOfWork, settlement: &Settlement) -> Result<bool> {
        let redis = &self.inner.redis;
        match self.inner.strategy {
            FetchStrategy::Reliable => {
                let (mode, key, member, score, cutoff, rank) = match settlement {
                    Settlement::Retry { at, job } => {
                        ("retry", keys::RETRY, job.as_slice(), *at, 0.0, 0)
                    }
                    Settlement::Morgue { job, bounds } => (
                        "dead",
                        keys::DEAD,
                        job.as_slice(),
                        bounds.now,
                        bounds.cutoff,
                        bounds.rank,
                    ),
                    Settlement::Drop => ("drop", keys::DEAD, &[][..], 0.0, 0.0, 0),
                };
                let (inprogress, fetched) = (self.inner.inprogress.as_str(), work.job());
                let settled: i64 = redis
                    .with_retry(|mut conn| async move {
                        SETTLE_SCRIPT
                            .key(inprogress)
                            .key(key)
                            .arg(fetched)
                            .arg(mode)
                            .arg(member)
                            .arg(score)
                            .arg(cutoff)
                            .arg(rank)
                            .invoke_async(&mut conn)
                            .await
                    })
                    .await?;
                Ok(settled == 1)
            }
            FetchStrategy::Basic => {
                match settlement {
                    Settlement::Retry { at, job } => {
                        let (at, job) = (*at, job.as_slice());
                        redis
                            .with_retry(|mut conn| async move {
                                ::redis::cmd("ZADD")
                                    .arg(keys::RETRY)
                                    .arg(at)
                                    .arg(job)
                                    .exec_async(&mut conn)
                                    .await
                            })
                            .await?;
                    }
                    Settlement::Morgue { job, bounds } => {
                        let mut pipe = ::redis::pipe();
                        pipe.atomic()
                            .cmd("ZADD")
                            .arg(keys::DEAD)
                            .arg(bounds.now)
                            .arg(job)
                            .ignore()
                            .cmd("ZREMRANGEBYSCORE")
                            .arg(keys::DEAD)
                            .arg("-inf")
                            .arg(bounds.cutoff)
                            .ignore()
                            .cmd("ZREMRANGEBYRANK")
                            .arg(keys::DEAD)
                            .arg(0)
                            .arg(bounds.rank)
                            .ignore();
                        let pipe = &pipe;
                        redis
                            .with_retry(|mut conn| async move { pipe.exec_async(&mut conn).await })
                            .await?;
                    }
                    Settlement::Drop => {}
                }
                Ok(true)
            }
        }
    }

    async fn basic_requeue(&self, work: &[UnitOfWork]) -> Result<usize> {
        let mut pipe = ::redis::pipe();
        for unit in work {
            pipe.cmd("RPUSH").arg(unit.queue()).arg(unit.job()).ignore();
        }
        let pipe = &pipe;
        self.inner
            .redis
            .with_retry(|mut conn| async move { pipe.exec_async(&mut conn).await })
            .await?;
        Ok(work.len())
    }

    /// Moves the jobs of dead processes back to their queues (reliable fetch only; returns 0
    /// for basic fetch).
    ///
    /// A process is dead when its identity is in the [`INPROGRESS`](keys::INPROGRESS) set but
    /// it has no heartbeat key, and still has none after the
    /// [orphan grace](Fetcher::with_orphan_grace), if any: this call then waits that long
    /// before it recovers anything. Each of a dead process's in-progress jobs goes back to the
    /// front of the queue named in the job, and its recovery is counted. A job that was in flight
    /// at an earlier crash too (a suspect, see the [module documentation](self)) waits in
    /// [`RECOVERY_SUSPECTS`](keys::RECOVERY_SUSPECTS) instead, and the suspects come back alone:
    /// one per call, the most recovered first. A job recovered more than [`max_recoveries`](Config::max_recoveries) times (each
    /// within [`RECOVERY_WINDOW`] of the previous one) is a poison pill and goes to the morgue
    /// instead (`config`'s death and error handlers run for it); jobs that cannot be parsed (or
    /// name no queue) go to the morgue.
    ///
    /// One process recovers at a time, and a check starts at most once per `lock_ttl`: the
    /// [`INPROGRESS_RECOVERY`](keys::INPROGRESS_RECOVERY) lock is taken for that long and kept
    /// while the check runs (a check that finds another process holding it stops), and a poison
    /// pill is claimed ([`keys::recovery_claim`]) before it is settled, so that a check that
    /// overlaps another one (its lock lapsed during a stall) never settles it twice. Returns
    /// how many jobs went back to their queues or to the morgue (a suspect counts when it comes
    /// back).
    pub async fn recover_orphans(&self, lock_ttl: Duration, config: &Config) -> Result<usize> {
        self.recover_orphans_until(lock_ttl, config, None).await
    }

    /// [`Fetcher::recover_orphans`], giving up (returning 0, nothing recovered) if `stop` is
    /// cancelled during the grace period.
    pub(crate) async fn recover_orphans_until(
        &self,
        lock_ttl: Duration,
        config: &Config,
        stop: Option<&CancellationToken>,
    ) -> Result<usize> {
        if self.inner.strategy != FetchStrategy::Reliable {
            return Ok(0);
        }
        let redis = &self.inner.redis;
        let identity = self.inner.identity.as_str();
        let ttl_ms = u64::try_from(lock_ttl.as_millis())
            .unwrap_or(u64::MAX)
            .max(1);
        let mut conn = redis.conn().await?;
        let locked: Option<String> = ::redis::cmd("SET")
            .arg(keys::INPROGRESS_RECOVERY)
            .arg(identity)
            .arg("NX")
            .arg("PX")
            .arg(ttl_ms)
            .query_async(&mut conn)
            .await?;
        if locked.is_none() {
            tracing::debug!("another process is recovering orphaned jobs");
            return Ok(0);
        }
        let lock = RecoveryLock::new(redis, identity, lock_ttl);
        let owners: Vec<String> = ::redis::cmd("SMEMBERS")
            .arg(keys::INPROGRESS)
            .query_async(&mut conn)
            .await?;
        let others: Vec<String> = owners
            .into_iter()
            .filter(|owner| owner.as_str() != identity)
            .collect();
        let mut dead = without_heartbeat(redis, others).await?;
        let grace = self.inner.orphan_grace;
        if !dead.is_empty() && !grace.is_zero() {
            // A key that expired means the process died, but Redis may also have lost the keys of
            // live processes, which re-create them within a heartbeat interval: look again after
            // the grace before taking any of them for dead.
            tracing::debug!(
                processes = ?dead,
                ?grace,
                "processes without a heartbeat; checking again after the grace period"
            );
            let held = match stop {
                Some(stop) => match stop.run_until_cancelled(lock.wait(grace)).await {
                    Some(held) => held?,
                    None => return Ok(0),
                },
                None => lock.wait(grace).await?,
            };
            if !held {
                tracing::debug!("another process took over the orphan check");
                return Ok(0);
            }
            dead = without_heartbeat(redis, dead).await?;
        }
        // The suspects of crash loops (jobs recovered before) come back one per check, dead
        // processes or not.
        poison::recover_dead(self, &dead, config, &lock).await
    }

    /// Pushes back to their queues the jobs of this process's own in-progress list that no
    /// processor holds (see [`Held`]) and that nothing held for at least `grace` (checks made
    /// by `stranded`): jobs moved there by a fetch whose reply was lost (a timeout, a dropped
    /// connection), or left there by an outcome write that failed. `grace` must exceed the
    /// longest time a fetch can wait for its reply, so that a job still on its way to a
    /// processor is never taken for one. Reliable fetch only (returns 0 for basic fetch).
    /// Returns how many jobs were pushed back (or moved to the morgue, if unreadable).
    pub(crate) async fn reconcile(
        &self,
        held: &Held,
        stranded: &mut Stranded,
        grace: Duration,
        config: &Config,
    ) -> Result<usize> {
        if self.inner.strategy != FetchStrategy::Reliable {
            return Ok(0);
        }
        let redis = &self.inner.redis;
        let list = self.inner.inprogress.as_str();
        let entries: Vec<Vec<u8>> = redis
            .with_retry(|mut conn| async move {
                ::redis::cmd("LRANGE")
                    .arg(list)
                    .arg(0)
                    .arg(-1)
                    .query_async(&mut conn)
                    .await
            })
            .await?;
        // What the processors hold now, after the list was read: a job acknowledged meanwhile
        // looks unheld this time and is gone from the list at the next check.
        let unheld = held.unheld(entries);
        let due = stranded.update(unheld, Instant::now(), grace);
        if due.is_empty() {
            return Ok(0);
        }
        let jobs: Vec<&[u8]> = due.iter().map(Vec::as_slice).collect();
        let moved = requeue_entries(redis, list, &self.inner.identity, &jobs, config).await?;
        if moved > 0 {
            tracing::warn!(
                jobs = moved,
                "pushed back jobs left in this process's in-progress list (a fetch whose reply \
                 was lost, or an outcome that could not be written)"
            );
        }
        Ok(moved)
    }

    /// Moves every job still in this process's in-progress list back to its queue and, once
    /// the list is empty, unregisters the process as an owner. Used at the end of a shutdown
    /// (reliable fetch only). Returns how many jobs were moved.
    pub(crate) async fn release(&self, config: &Config) -> Result<usize> {
        if self.inner.strategy != FetchStrategy::Reliable {
            return Ok(0);
        }
        recover_list(self, config).await
    }
}

impl fmt::Debug for Fetcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Fetcher")
            .field("strategy", &self.inner.strategy)
            .field("identity", &self.inner.identity)
            .field("capsule", &self.inner.capsule)
            .field("mode", &self.inner.mode)
            .field("queues", &self.inner.names)
            .field("timeout", &self.inner.timeout)
            .finish()
    }
}

/// Fetches jobs for one processor. Created by [`Fetcher::worker`].
pub struct FetchWorker {
    fetcher: Fetcher,
    index: usize,
    turn: usize,
    /// Dedicated connection for `BRPOP`/`BLMOVE`, opened on first use.
    conn: Option<RedisConnection>,
}

/// What the non-blocking step of a fetch found.
enum Attempt {
    Found(UnitOfWork),
    /// Every queue was empty; these queue keys are not paused.
    Empty(Vec<String>),
}

impl FetchWorker {
    /// Fetches the next job, waiting up to the fetch timeout when every queue is empty.
    /// Returns `None` if no job arrived in time (or every queue is paused).
    pub async fn retrieve_work(&mut self) -> Result<Option<UnitOfWork>> {
        self.retrieve(None).await
    }

    /// Like [`FetchWorker::retrieve_work`], but gives up early (returning `None`) once
    /// `cancel` is cancelled while no command is in flight: waiting for a connection or
    /// sleeping because every queue is paused. A command that was sent always completes, so
    /// a job taken from Redis is never dropped.
    pub(crate) async fn retrieve_work_until(
        &mut self,
        cancel: &CancellationToken,
    ) -> Result<Option<UnitOfWork>> {
        self.retrieve(Some(cancel)).await
    }

    async fn retrieve(&mut self, cancel: Option<&CancellationToken>) -> Result<Option<UnitOfWork>> {
        let order = self.fetcher.order();
        let open = match self.attempt(&order).await? {
            Attempt::Found(work) => return Ok(Some(work)),
            Attempt::Empty(open) => open,
        };
        let timeout = self.fetcher.inner.timeout;
        if open.is_empty() {
            // Every queue is paused: wait before checking again.
            match cancel {
                Some(cancel) => {
                    let _ = cancel
                        .run_until_cancelled(tokio::time::sleep(timeout))
                        .await;
                }
                None => tokio::time::sleep(timeout).await,
            }
            return Ok(None);
        }
        let Some(mut conn) = self.blocking_conn(cancel).await? else {
            return Ok(None);
        };
        let block = timeout.as_secs_f64();
        let result = match self.fetcher.inner.strategy {
            FetchStrategy::Basic => {
                let popped: ::redis::RedisResult<Option<(String, Vec<u8>)>> = ::redis::cmd("BRPOP")
                    .arg(&open)
                    .arg(block)
                    .query_async(&mut conn)
                    .await;
                popped.map(|popped| popped.map(|(queue, job)| UnitOfWork::new(queue, job)))
            }
            FetchStrategy::Reliable => {
                // Spread the processors over the open queues, and move each one to the next
                // queue every time it blocks, so that every queue has waiters.
                let queue = &open[(self.index.wrapping_add(self.turn)) % open.len()];
                self.turn = self.turn.wrapping_add(1);
                // Registering the owner (which the script does atomically) rides in the same
                // round trip: Redis runs the `SADD` once `BLMOVE` returns. Its errors are
                // ignored, so that a job `BLMOVE` moved is never dropped because of it.
                let replies: ::redis::RedisResult<Vec<::redis::RedisResult<Option<Vec<u8>>>>> =
                    ::redis::pipe()
                        .ignore_errors()
                        .cmd("BLMOVE")
                        .arg(queue)
                        .arg(&self.fetcher.inner.inprogress)
                        .arg("RIGHT")
                        .arg("LEFT")
                        .arg(block)
                        .cmd("SADD")
                        .arg(keys::INPROGRESS)
                        .arg(&self.fetcher.inner.identity)
                        .ignore()
                        .query_async(&mut conn)
                        .await;
                let moved = replies.and_then(|replies| {
                    replies.into_iter().next().unwrap_or_else(|| {
                        Err(::redis::RedisError::from((
                            ::redis::ErrorKind::UnexpectedReturnType,
                            "no reply to BLMOVE",
                        )))
                    })
                });
                moved.map(|moved| moved.map(|job| UnitOfWork::new(queue.clone(), job)))
            }
        };
        match result {
            Ok(work) => Ok(work),
            Err(error) => {
                // Start over with a fresh connection (after a failover the old one may point
                // at a replica).
                self.conn = None;
                Err(error.into())
            }
        }
    }

    /// The non-blocking step: one script call that takes the first available job.
    async fn attempt(&self, order: &[usize]) -> Result<Attempt> {
        let inner = &self.fetcher.inner;
        let mut invocation = match inner.strategy {
            FetchStrategy::Basic => {
                let mut invocation = BASIC_FETCH_SCRIPT.prepare_invoke();
                invocation.key(keys::PAUSED);
                invocation
            }
            FetchStrategy::Reliable => {
                let mut invocation = RELIABLE_FETCH_SCRIPT.prepare_invoke();
                invocation
                    .key(&inner.inprogress)
                    .key(keys::INPROGRESS)
                    .key(keys::PAUSED)
                    .arg(&inner.identity);
                invocation
            }
        };
        for &index in order {
            invocation.key(&inner.keys[index]).arg(&inner.names[index]);
        }
        let invocation = &invocation;
        let (found, values): (i64, Vec<Vec<u8>>) = inner
            .redis
            .with_retry(|mut conn| async move { invocation.invoke_async(&mut conn).await })
            .await?;
        if found == 1 {
            let mut values = values.into_iter();
            let (Some(queue), Some(job)) = (values.next(), values.next()) else {
                return Err(Error::Redis(::redis::RedisError::from((
                    ::redis::ErrorKind::UnexpectedReturnType,
                    "the fetch script returned a malformed job",
                ))));
            };
            Ok(Attempt::Found(UnitOfWork::new(
                String::from_utf8_lossy(&queue).into_owned(),
                job,
            )))
        } else {
            Ok(Attempt::Empty(
                values
                    .into_iter()
                    .map(|key| String::from_utf8_lossy(&key).into_owned())
                    .collect(),
            ))
        }
    }

    /// The dedicated blocking connection, opened if needed. `None` if `cancel` fired first.
    async fn blocking_conn(
        &mut self,
        cancel: Option<&CancellationToken>,
    ) -> Result<Option<RedisConnection>> {
        if let Some(conn) = &self.conn {
            return Ok(Some(conn.clone()));
        }
        let redis = &self.fetcher.inner.redis;
        let connect = redis.blocking_conn(self.fetcher.inner.timeout);
        let conn = match cancel {
            Some(cancel) => match cancel.run_until_cancelled(connect).await {
                Some(conn) => conn?,
                None => return Ok(None),
            },
            None => connect.await?,
        };
        self.conn = Some(conn.clone());
        Ok(Some(conn))
    }
}

impl fmt::Debug for FetchWorker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FetchWorker")
            .field("fetcher", &self.fetcher)
            .field("index", &self.index)
            .field("connected", &self.conn.is_some())
            .finish()
    }
}

/// A random order of the indexes of `weights` in which, at every position, each remaining
/// index comes next with a probability proportional to its weight. This is the distribution
/// Sidekiq gets by shuffling the queue list with each queue repeated `weight` times and
/// removing duplicates, without materializing that list.
fn weighted_order(weights: &[u64]) -> Vec<usize> {
    let mut remaining: Vec<usize> = (0..weights.len()).collect();
    let mut total: u64 = weights.iter().sum();
    let mut order = Vec::with_capacity(weights.len());
    while !remaining.is_empty() {
        let mut pick = if total == 0 {
            0
        } else {
            fastrand::u64(0..total)
        };
        let mut chosen = remaining.len() - 1;
        for (position, &index) in remaining.iter().enumerate() {
            let weight = weights[index];
            if pick < weight {
                chosen = position;
                break;
            }
            pick -= weight;
        }
        let index = remaining.swap_remove(chosen);
        total = total.saturating_sub(weights[index]);
        order.push(index);
    }
    order
}

/// Moves `(queue key, job)` pairs out of the in-progress list `inprogress` back to the front
/// of their queues. Returns how many were moved.
async fn requeue_from(
    redis: &RedisPool,
    inprogress: &str,
    jobs: &[(&str, &[u8])],
) -> Result<usize> {
    let mut moved = 0;
    for batch in jobs.chunks(BATCH) {
        let mut invocation = REQUEUE_SCRIPT.prepare_invoke();
        invocation.key(inprogress);
        for (queue, job) in batch {
            invocation.key(*queue).arg(*job);
        }
        let invocation = &invocation;
        let count: usize = redis
            .with_retry(|mut conn| async move { invocation.invoke_async(&mut conn).await })
            .await?;
        moved += count;
    }
    Ok(moved)
}

/// The processes of `owners` without a heartbeat key (one pipelined `EXISTS` each).
async fn without_heartbeat(redis: &RedisPool, owners: Vec<String>) -> Result<Vec<String>> {
    if owners.is_empty() {
        return Ok(owners);
    }
    let mut pipe = ::redis::pipe();
    for owner in &owners {
        pipe.cmd("EXISTS").arg(keys::process(owner));
    }
    let pipe = &pipe;
    let alive: Vec<bool> = redis
        .with_retry(|mut conn| async move { pipe.query_async(&mut conn).await })
        .await?;
    Ok(owners
        .into_iter()
        .zip(alive)
        .filter_map(|(owner, alive)| (!alive).then_some(owner))
        .collect())
}

/// The orphan check's lock ([`keys::INPROGRESS_RECOVERY`]) while this process holds it for a
/// check: kept from lapsing as long as the check runs.
pub(super) struct RecoveryLock<'a> {
    redis: &'a RedisPool,
    identity: &'a str,
    /// The least time the lock keeps while the check runs (half the lock's time, at most 5 s):
    /// longer than the time between two keeps, short enough that the next check (which waits
    /// for the lock to end) is barely delayed.
    margin: Duration,
}

impl<'a> RecoveryLock<'a> {
    /// The lock, taken for `ttl` by the process `identity`.
    fn new(redis: &'a RedisPool, identity: &'a str, ttl: Duration) -> Self {
        let margin = (ttl / 2).clamp(Duration::from_millis(1), Duration::from_secs(5));
        Self {
            redis,
            identity,
            margin,
        }
    }

    /// Whether this process still holds the lock (taken back if it lapsed), which then lasts at
    /// least the margin. `false`: another process holds it, and this check must stop.
    pub(super) async fn keep(&self) -> Result<bool> {
        let margin_ms = u64::try_from(self.margin.as_millis()).unwrap_or(u64::MAX);
        let (identity, lock) = (self.identity, keys::INPROGRESS_RECOVERY);
        let held: i64 = self
            .redis
            .with_retry(|mut conn| async move {
                KEEP_RECOVERY_LOCK_SCRIPT
                    .key(lock)
                    .arg(identity)
                    .arg(margin_ms)
                    .invoke_async(&mut conn)
                    .await
            })
            .await?;
        Ok(held == 1)
    }

    /// Waits `time`, keeping the lock; `false` once another process holds it.
    async fn wait(&self, time: Duration) -> Result<bool> {
        let until = tokio::time::Instant::now() + time;
        let step = (self.margin / 2).max(Duration::from_millis(1));
        loop {
            let left = until.saturating_duration_since(tokio::time::Instant::now());
            if left.is_zero() {
                return Ok(true);
            }
            tokio::time::sleep(left.min(step)).await;
            if !self.keep().await? {
                return Ok(false);
            }
        }
    }
}

/// Empties this process's own in-progress list at the end of its shutdown: its jobs go back to
/// their queues as they are (not counted as recoveries; the morgue for those that cannot be
/// parsed), then the process is unregistered as an owner once the list is empty.
async fn recover_list(fetcher: &Fetcher, config: &Config) -> Result<usize> {
    let redis = &fetcher.inner.redis;
    let (owner, list) = (fetcher.identity(), fetcher.inprogress_key());
    let jobs = read_list(redis, list).await?;
    let jobs: Vec<&[u8]> = jobs.iter().map(Vec::as_slice).collect();
    let moved = requeue_entries(redis, list, owner, &jobs, config).await?;
    release_owner(redis, list, owner).await?;
    Ok(moved)
}

/// Every entry of the in-progress list `list`, the most recently fetched first.
async fn read_list(redis: &RedisPool, list: &str) -> Result<Vec<Vec<u8>>> {
    redis
        .with_retry(|mut conn| async move {
            ::redis::cmd("LRANGE")
                .arg(list)
                .arg(0)
                .arg(-1)
                .query_async(&mut conn)
                .await
        })
        .await
}

/// Unregisters `owner` as the owner of the in-progress list `list` once the list is empty.
async fn release_owner(redis: &RedisPool, list: &str, owner: &str) -> Result<()> {
    let mut invocation = RELEASE_SCRIPT.prepare_invoke();
    invocation.key(list).key(keys::INPROGRESS).arg(owner);
    let invocation = &invocation;
    let _released: i64 = redis
        .with_retry(|mut conn| async move { invocation.invoke_async(&mut conn).await })
        .await?;
    Ok(())
}

/// Moves `jobs` (entries of the in-progress list `list` of process `owner`) back to the front
/// of the queue named in each, or to the morgue for those that cannot be parsed or name no
/// queue. Entries no longer in the list are left alone. Returns how many were moved.
async fn requeue_entries(
    redis: &RedisPool,
    list: &str,
    owner: &str,
    jobs: &[&[u8]],
    config: &Config,
) -> Result<usize> {
    let mut requeue: Vec<(String, &[u8])> = Vec::new();
    let mut bury: Vec<&[u8]> = Vec::new();
    for job in jobs {
        match JobPayload::from_slice(job) {
            Ok(payload) if !payload.queue.is_empty() => {
                requeue.push((keys::queue(&payload.queue), *job));
            }
            _ => bury.push(*job),
        }
    }
    let pairs: Vec<(&str, &[u8])> = requeue
        .iter()
        .map(|(queue, job)| (queue.as_str(), *job))
        .collect();
    let mut moved = requeue_from(redis, list, &pairs).await?;
    if !bury.is_empty() {
        tracing::warn!(
            process = %owner,
            jobs = bury.len(),
            "moving unreadable in-progress jobs to the morgue"
        );
        moved += bury_from(redis, list, &bury, config).await?;
    }
    Ok(moved)
}

/// Moves jobs out of an in-progress list into the morgue.
async fn bury_from(
    redis: &RedisPool,
    list: &str,
    jobs: &[&[u8]],
    config: &Config,
) -> Result<usize> {
    let MorgueBounds { now, cutoff, rank } = MorgueBounds::new(config);
    let mut moved = 0;
    for batch in jobs.chunks(BATCH) {
        let mut invocation = BURY_SCRIPT.prepare_invoke();
        invocation
            .key(list)
            .key(keys::DEAD)
            .arg(now)
            .arg(cutoff)
            .arg(rank);
        for job in batch {
            invocation.arg(*job);
        }
        let invocation = &invocation;
        let count: usize = redis
            .with_retry(|mut conn| async move { invocation.invoke_async(&mut conn).await })
            .await?;
        moved += count;
    }
    Ok(moved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Hench;

    fn capsule(builder: crate::config::HenchBuilder) -> CapsuleConfig {
        // `build` needs Redis; validate through a detached pool instead.
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let hench = rt
            .block_on(builder.redis(RedisPool::detached()).build())
            .unwrap();
        hench.config().capsules[0].clone()
    }

    fn fetcher(capsule: &CapsuleConfig) -> Fetcher {
        Fetcher::new(
            FetchStrategy::Reliable,
            RedisPool::detached(),
            "host:1:abc",
            capsule,
        )
    }

    #[test]
    fn strict_order_is_the_configured_order() {
        let capsule = capsule(Hench::builder().queues(["basic", "bar"]));
        let fetcher = fetcher(&capsule);
        for _ in 0..10 {
            assert_eq!(fetcher.queue_order(), ["queue:basic", "queue:bar"]);
        }
    }

    #[test]
    fn weighted_order_lists_every_queue_once() {
        let capsule = capsule(Hench::builder().queues(["basic", "bar,3"]));
        assert_eq!(capsule.mode, QueueMode::Weighted);
        let fetcher = fetcher(&capsule);
        let mut firsts = std::collections::HashMap::new();
        for _ in 0..2_000 {
            let mut order = fetcher.queue_order();
            *firsts.entry(order[0]).or_insert(0) += 1;
            order.sort_unstable();
            assert_eq!(order, ["queue:bar", "queue:basic"]);
        }
        // bar comes first ~3/4 of the time (weight 3 vs. 1, zero weights count as 1).
        let bar = firsts["queue:bar"];
        assert!((1_300..1_700).contains(&bar), "{firsts:?}");
    }

    #[test]
    fn weighted_order_distribution() {
        let weights = [3_u64, 2, 1];
        let samples = 60_000;
        let mut first = [0_u32; 3];
        let mut second_given_first_0 = [0_u32; 3];
        for _ in 0..samples {
            let order = weighted_order(&weights);
            assert_eq!(order.len(), 3);
            first[order[0]] += 1;
            if order[0] == 0 {
                second_given_first_0[order[1]] += 1;
            }
        }
        let share = |count: u32, total: u32| f64::from(count) / f64::from(total);
        assert!((share(first[0], samples) - 0.5).abs() < 0.02, "{first:?}");
        assert!(
            (share(first[1], samples) - 1.0 / 3.0).abs() < 0.02,
            "{first:?}"
        );
        assert!(
            (share(first[2], samples) - 1.0 / 6.0).abs() < 0.02,
            "{first:?}"
        );
        // After queue 0, queue 1 (weight 2) follows twice as often as queue 2 (weight 1).
        let rest = second_given_first_0[1] + second_given_first_0[2];
        assert!(
            (share(second_given_first_0[1], rest) - 2.0 / 3.0).abs() < 0.03,
            "{second_given_first_0:?}"
        );
    }

    #[test]
    fn random_order_produces_every_permutation() {
        let capsule = capsule(Hench::builder().weighted_queues([("a", 1), ("b", 1), ("c", 1)]));
        assert_eq!(capsule.mode, QueueMode::Random);
        let fetcher = fetcher(&capsule);
        let mut seen = std::collections::HashSet::new();
        for _ in 0..1_000 {
            seen.insert(fetcher.queue_order().join(" "));
        }
        assert_eq!(seen.len(), 6, "{seen:?}");
    }

    #[test]
    fn weighted_order_edge_cases() {
        assert!(weighted_order(&[]).is_empty());
        assert_eq!(weighted_order(&[5]), [0]);
        let mut order = weighted_order(&[0, 0]);
        order.sort_unstable();
        assert_eq!(order, [0, 1]);
    }

    proptest::proptest! {
        /// DESIGN §10: every weighted order lists each queue exactly once, and a queue of
        /// weight 0 never comes before one with a positive weight.
        #[test]
        fn weighted_orders_are_permutations(
            weights in proptest::collection::vec(0_u64..=1_000, 0..24)
        ) {
            let order = weighted_order(&weights);
            let mut sorted = order.clone();
            sorted.sort_unstable();
            proptest::prop_assert_eq!(sorted, (0..weights.len()).collect::<Vec<_>>());
            let first_zero = order.iter().position(|&index| weights[index] == 0);
            if let Some(first_zero) = first_zero {
                proptest::prop_assert!(order[first_zero..].iter().all(|&index| weights[index] == 0));
            }
        }
    }

    #[test]
    fn units_of_work() {
        let work = UnitOfWork::new("queue:default", b"{\"class\":\"A\"}".to_vec());
        assert_eq!(work.queue(), "queue:default");
        assert_eq!(work.queue_name(), "default");
        assert_eq!(work.job_str(), "{\"class\":\"A\"}");
        assert_eq!(work.parse().unwrap().class, "A");
        let binary = UnitOfWork::new("queue:x", vec![0xff, b'{']);
        assert_eq!(binary.job(), [0xff, b'{']);
        assert!(binary.parse().is_err());
        assert!(format!("{binary:?}").contains("queue:x"));
    }

    #[test]
    fn stranded_entries_are_due_after_the_grace() {
        let grace = Duration::from_secs(5);
        let start = Instant::now();
        let mut stranded = Stranded::default();
        let entries = |names: &[&str]| -> Vec<Vec<u8>> {
            names.iter().map(|name| name.as_bytes().to_vec()).collect()
        };
        // First seen: never due yet.
        assert!(
            stranded
                .update(entries(&["a", "b"]), start, grace)
                .is_empty()
        );
        // Still unheld, but not for long enough.
        let soon = start + Duration::from_secs(2);
        assert!(
            stranded
                .update(entries(&["a", "b"]), soon, grace)
                .is_empty()
        );
        // `b` was held meanwhile (gone from the unheld entries): forgotten.
        let later = start + Duration::from_secs(6);
        assert_eq!(
            stranded.update(entries(&["a"]), later, grace),
            entries(&["a"])
        );
        // `b` unheld again: its clock starts again.
        assert!(stranded.update(entries(&["b"]), later, grace).is_empty());
        // Every copy of a due entry is returned.
        let much_later = later + grace;
        assert_eq!(
            stranded.update(entries(&["b", "b"]), much_later, grace),
            entries(&["b", "b"])
        );
        // After a stall the clocks start again.
        assert!(
            stranded
                .update(entries(&["c"]), much_later, grace)
                .is_empty()
        );
        stranded.forget();
        let after_stall = much_later + grace * 2;
        assert!(
            stranded
                .update(entries(&["c"]), after_stall, grace)
                .is_empty()
        );
    }

    #[test]
    fn held_jobs_account_for_one_entry_each() {
        let held = Arc::new(Held::default());
        let a = UnitOfWork::new("queue:q", b"a".to_vec());
        let a_again = UnitOfWork::new("queue:q", b"a".to_vec());
        let entries = || vec![b"a".to_vec(), b"a".to_vec(), b"b".to_vec()];
        assert_eq!(held.unheld(entries()).len(), 3);
        let first = held.hold(&a);
        assert_eq!(held.unheld(entries()), vec![b"a".to_vec(), b"b".to_vec()]);
        let second = held.hold(&a_again);
        assert_eq!(held.unheld(entries()), vec![b"b".to_vec()]);
        drop(first);
        assert_eq!(held.unheld(entries()), vec![b"a".to_vec(), b"b".to_vec()]);
        drop(second);
        assert_eq!(held.unheld(entries()).len(), 3);
    }

    #[test]
    fn fetcher_settings() {
        let capsule = capsule(Hench::builder());
        let fetcher = fetcher(&capsule).with_timeout(Duration::ZERO);
        assert_eq!(fetcher.timeout(), Duration::from_millis(1));
        assert_eq!(fetcher.strategy(), FetchStrategy::Reliable);
        assert_eq!(fetcher.identity(), "host:1:abc");
        assert!(format!("{fetcher:?}").contains("host:1:abc"));
        assert!(format!("{:?}", fetcher.worker(3)).contains("index: 3"));
    }
}

/// Tests of crate-internal operations against a private Redis.
#[cfg(all(test, feature = "test-util"))]
mod redis_tests {
    use super::*;
    use crate::client::Stored;
    use crate::payload::{now_millis, now_secs_f64};
    use crate::test_util::TestRedis;

    async fn query<T: ::redis::FromRedisValue>(pool: &RedisPool, command: &::redis::Cmd) -> T {
        let mut conn = pool.conn().await.unwrap();
        command.query_async(&mut conn).await.unwrap()
    }

    /// A job taken from `queue:default` by a fetcher of `strategy`.
    async fn fetched(strategy: FetchStrategy) -> (TestRedis, RedisPool, Fetcher, UnitOfWork) {
        let redis = TestRedis::start().await;
        let pool = redis.pool().await;
        let mut job = JobPayload::new("Iterable", vec![]);
        job.queue = "default".into();
        job.jid = "0123456789abcdef01234567".into();
        let () = query(
            &pool,
            ::redis::cmd("LPUSH")
                .arg(keys::queue("default"))
                .arg(job.to_json().unwrap()),
        )
        .await;
        let capsule = Config::default().capsules[0].clone();
        let fetcher = Fetcher::new(strategy, pool.clone(), "host:1:abc", &capsule)
            .with_timeout(Duration::from_millis(50));
        let work = fetcher.worker(0).retrieve_work().await.unwrap().unwrap();
        (redis, pool, fetcher, work)
    }

    #[tokio::test]
    async fn push_back_replaces_an_in_progress_job_once() {
        let (_redis, pool, fetcher, work) = fetched(FetchStrategy::Reliable).await;
        let inprogress = keys::inprogress("host:1:abc");
        assert_eq!(
            query::<usize>(&pool, ::redis::cmd("LLEN").arg(&inprogress)).await,
            1
        );
        let job = work.parse().unwrap();
        let stored = Stored::encode(job, now_secs_f64(), now_millis()).unwrap();
        assert!(fetcher.push_back(&work, &stored).await.unwrap());
        // Running it again (a retried write) pushes nothing more.
        assert!(!fetcher.push_back(&work, &stored).await.unwrap());
        assert_eq!(
            query::<usize>(&pool, ::redis::cmd("LLEN").arg(&inprogress)).await,
            0
        );
        let queued: Vec<String> = query(
            &pool,
            ::redis::cmd("LRANGE")
                .arg(keys::queue("default"))
                .arg(0)
                .arg(-1),
        )
        .await;
        assert_eq!(queued.len(), 1);
        let pushed = JobPayload::from_json(&queued[0]).unwrap();
        assert_eq!(pushed.jid, "0123456789abcdef01234567");
        assert!(pushed.enqueued_at.is_some());
        let queues: Vec<String> = query(&pool, ::redis::cmd("SMEMBERS").arg(keys::QUEUES)).await;
        assert_eq!(queues, ["default"]);
    }

    #[tokio::test]
    async fn push_back_can_schedule() {
        let (_redis, pool, fetcher, work) = fetched(FetchStrategy::Reliable).await;
        let mut job = work.parse().unwrap();
        let at = now_secs_f64() + 60.0;
        job.at = Some(at);
        let stored = Stored::encode(job, now_secs_f64(), now_millis()).unwrap();
        assert!(fetcher.push_back(&work, &stored).await.unwrap());
        let scheduled: Vec<(String, f64)> = query(
            &pool,
            ::redis::cmd("ZRANGE")
                .arg(keys::SCHEDULE)
                .arg(0)
                .arg(-1)
                .arg("WITHSCORES"),
        )
        .await;
        assert_eq!(scheduled.len(), 1);
        assert!((scheduled[0].1 - at).abs() < 0.001);
        let inprogress = keys::inprogress("host:1:abc");
        assert_eq!(
            query::<usize>(&pool, ::redis::cmd("LLEN").arg(&inprogress)).await,
            0
        );
    }

    #[tokio::test]
    async fn basic_push_back_pushes() {
        let (_redis, pool, fetcher, work) = fetched(FetchStrategy::Basic).await;
        let stored = Stored::encode(work.parse().unwrap(), now_secs_f64(), now_millis()).unwrap();
        assert!(fetcher.push_back(&work, &stored).await.unwrap());
        assert_eq!(
            query::<usize>(&pool, ::redis::cmd("LLEN").arg(keys::queue("default"))).await,
            1
        );
    }
}
