//! Queue limits (`docs/design/ratelimit.md` §2.7): limits on every fetch of a queue, enforced by
//! the fetch itself, so that a queue over its limits is skipped like a paused one while its jobs
//! keep their order and the other queues keep flowing.
//!
//! * **Storage.** `queue-limits` (a hash: queue name → its limits in their stored form,
//!   `concurrency=3;window=100/1m`) is configuration, like `paused`: the API, the dashboard and
//!   the `hench` command edit it at run time, and a worker writes the limits of its builder and
//!   file at every start (code wins at the next deploy). The state of a queue's limits lives
//!   under the reserved limiter name `@<queue>` (`limit:c:@exports`, `limit-stats:c:@exports`).
//! * **Kinds.** Concurrency, window, bucket and leaky limits, at most one of each per queue
//!   (points need a cost known after the job ran, which a fetch cannot know).
//! * **Cache.** Each process reads the hash at start, at every heartbeat and whenever a fetch
//!   finds that a queue's stored limits are not the ones it believes ([`QueueLimits`]). A capsule
//!   with no limited queue keeps the plain fetch scripts, byte for byte.
//! * **Fetch.** `LIMIT_FETCH_GATE` checks a limited queue's limits before taking its next job
//!   and charges them in the same step: a job leaves a limited queue only through it, so the
//!   queue keeps its order. A queue over its limits (or left full by a grant) is skipped and
//!   cools down in the process ([`Cooldowns`]): until its rate limits free a unit, or for
//!   [`BUSY_COOLDOWN`] when a concurrency limit is full (ended early when this process frees a
//!   slot of the queue). Then a single fetch of the process checks it again. The blocking step
//!   watches an empty limited queue (`BLMOVE` from the list to its own tail, which moves
//!   nothing), one fetch per process, and the next fetch goes through the gate.
//! * **Permits.** The concurrency leases of a fetched job ([`QueuePermit`]) are engine-held
//!   permits, like a throttle's: renewed by the heartbeat, released once the job's outcome is
//!   written.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::{Duration, Instant};

use ::redis::Value;
use chrono::Utc;
use tokio::sync::Notify;

use crate::error::{Error, Result};
use crate::keys;
use crate::limit::lua::LIMIT_FETCH_GATE;
use crate::limit::rule::{self, Rule, period_of};
use crate::limit::scripts::{
    self, AcquireArgs, Acquired, LeaseRef, array, int, millis, now_arg, text, unexpected,
};
use crate::limit::{Limit, LimitKind, Throttle};
use crate::redis::RedisPool;

/// How long a queue whose concurrency limit is full is left out of a process's fetches (ended
/// early when the process frees a slot of the queue).
pub(crate) const BUSY_COOLDOWN: Duration = Duration::from_millis(250);

/// The longest a queue over its rate limits is left out of a process's fetches, whatever its
/// wait: a limit raised meanwhile, or a unit freed by a reset, is noticed within this time.
pub(crate) const MAX_COOLDOWN: Duration = Duration::from_secs(5);

/// How often a fetcher created outside a running process ([`crate::fetch::Fetcher::new`])
/// re-reads the queue limits (a running process re-reads them at every heartbeat).
pub(crate) const STANDALONE_REFRESH: Duration = Duration::from_secs(10);

/// How long a counter of a queue limit lives after its last use.
pub(crate) const STATS_TTL: Duration = Throttle::STATS_TTL;

/// The prefix of the reserved limiter name of a queue's limits (`@<queue>`).
pub(crate) const PREFIX: char = '@';

/// The limiter id of `queue`'s limits: `@<queue>`.
pub(crate) fn id(queue: &str) -> String {
    format!("{PREFIX}{queue}")
}

/// Checks the limits of `queue`: at least one, of the kinds a fetch can enforce (concurrency,
/// window, bucket, leaky), at most one of each kind, each valid.
pub(crate) fn check(queue: &str, limits: &[Limit]) -> Result<()> {
    if queue.is_empty() {
        return Err(Error::Config(
            "queue limits need a queue name (it must not be empty)".to_owned(),
        ));
    }
    if limits.is_empty() {
        return Err(Error::Config(format!(
            "the queue `{}` is given no limit (clear its limits instead)",
            queue.escape_debug()
        )));
    }
    let mut seen: Vec<LimitKind> = Vec::with_capacity(limits.len());
    for limit in limits {
        if matches!(limit, Limit::Points { .. }) {
            return Err(Error::Config(format!(
                "invalid limit `{limit}` for the queue `{}`: points limits need a cost known \
                 after the job ran; a queue takes concurrency, window, bucket and leaky limits",
                queue.escape_debug()
            )));
        }
        rule::check_limit(limit).map_err(|error| match error {
            Error::Config(reason) => Error::Config(format!(
                "{reason} (a limit of the queue `{}`)",
                queue.escape_debug()
            )),
            other => other,
        })?;
        let kind = limit.kind();
        if seen.contains(&kind) {
            return Err(Error::Config(format!(
                "the queue `{}` has two {} limits: a queue takes at most one limit of each kind",
                queue.escape_debug(),
                kind_name(kind)
            )));
        }
        seen.push(kind);
    }
    Ok(())
}

/// The name of a kind of limit, for messages.
fn kind_name(kind: LimitKind) -> &'static str {
    match kind {
        LimitKind::Concurrency => "concurrency",
        LimitKind::Window => "window",
        LimitKind::Bucket => "bucket",
        LimitKind::Leaky => "leaky",
    }
}

/// The order of the kinds in the stored form.
fn kind_rank(limit: &Limit) -> u8 {
    match limit.kind() {
        LimitKind::Concurrency => 0,
        LimitKind::Window => 1,
        LimitKind::Bucket => 2,
        LimitKind::Leaky => 3,
    }
}

/// The stored form of a queue's limits (checked with [`check`] first): each limit's spec in the
/// long form, in the order concurrency, window, bucket, leaky, joined by `;`
/// (`concurrency=3;window=100/1m`).
pub(crate) fn stored_form(limits: &[Limit]) -> String {
    let mut sorted: Vec<&Limit> = limits.iter().collect();
    sorted.sort_by_key(|limit| kind_rank(limit));
    sorted
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(";")
}

/// Parses limits in the spec grammar, joined by `;` (`concurrency=3; window=100/1m`), and checks
/// them as limits of `queue`. `per key` is refused: a queue has no keys.
pub(crate) fn parse(queue: &str, text: &str) -> Result<Vec<Limit>> {
    let limits = text
        .split(';')
        .filter(|spec| !spec.trim().is_empty())
        .map(|spec| {
            spec.parse::<Limit>().map_err(|error| match error {
                Error::Config(reason) => {
                    // Two limits run together (`c=3, w=100/1m`, `c=3 w=100/1m`): say how to
                    // separate them.
                    let hint = if spec.matches('=').count() > 1 {
                        "; separate limits with `;`"
                    } else {
                        ""
                    };
                    Error::Config(format!("{reason}{hint} (a limit of the queue `{queue}`)"))
                }
                other => other,
            })
        })
        .collect::<Result<Vec<Limit>>>()?;
    check(queue, &limits)?;
    Ok(limits)
}

/// The rules of `queue`'s limits (limiter id `@<queue>`).
pub(crate) fn rules(queue: &str, limits: &[Limit]) -> Vec<Rule> {
    let id = id(queue);
    limits
        .iter()
        .map(|limit| Rule::new(*limit, id.clone(), id.clone()))
        .collect()
}

/// The lease of a queue's concurrency permits, renewed by the heartbeat: [`Throttle::LEASE`],
/// longer when the heartbeat is so slow that a lease would not outlast three of its intervals.
pub(crate) fn lease_for(heartbeat_interval: Duration) -> Duration {
    Throttle::LEASE.max(heartbeat_interval.saturating_mul(4))
}

/// Writes the queue limits of a configuration (checked with [`check`] at build) to Redis,
/// replacing the stored limits of those queues, in one step. A stored limit that differs (a
/// run-time edit, or another deployment's configuration) is logged as it is replaced.
pub(crate) async fn seed(redis: &RedisPool, limits: &[(String, Vec<Limit>)]) -> Result<()> {
    if limits.is_empty() {
        return Ok(());
    }
    let wanted: Vec<(&str, String)> = limits
        .iter()
        .map(|(queue, limits)| (queue.as_str(), stored_form(limits)))
        .collect();
    let mut read = ::redis::cmd("HMGET");
    read.arg(keys::QUEUE_LIMITS);
    for (queue, _) in &wanted {
        read.arg(*queue);
    }
    let read = &read;
    let stored: Vec<Option<String>> = redis
        .with_retry(|mut conn| async move { read.query_async(&mut conn).await })
        .await?;
    for ((queue, limits), before) in wanted.iter().zip(&stored) {
        if let Some(before) = before.as_deref().filter(|before| before != limits) {
            tracing::info!(
                queue = %queue.escape_debug(),
                before = %before.escape_debug(),
                limits = %limits,
                "replacing the stored limits of a queue with the configuration's"
            );
        }
    }
    let mut write = ::redis::cmd("HSET");
    write.arg(keys::QUEUE_LIMITS);
    for (queue, limits) in &wanted {
        write.arg(*queue).arg(limits);
    }
    let write = &write;
    redis
        .with_retry(|mut conn| async move { write.exec_async(&mut conn).await })
        .await
}

/// One queue's entry in a process's view of the queue limits.
#[derive(Debug, Clone)]
pub(crate) struct QueueEntry {
    /// The stored form, exactly as read (the gate script compares it with the stored one).
    pub(crate) stored: String,
    /// The rules, or `None` when the stored form is not valid: the queue is then not fetched
    /// (a broken limit must not lift the protection it was meant to give).
    pub(crate) rules: Option<Arc<[Rule]>>,
}

/// A process's view of the queue limits at one time.
#[derive(Debug, Default)]
pub(crate) struct Snapshot {
    queues: HashMap<String, QueueEntry>,
    loaded: Option<Instant>,
}

impl Snapshot {
    /// The limits of `queue`, if it has some.
    pub(crate) fn get(&self, queue: &str) -> Option<&QueueEntry> {
        self.queues.get(queue)
    }

    /// Whether no queue has limits.
    pub(crate) fn is_empty(&self) -> bool {
        self.queues.is_empty()
    }

    /// The queues with limits.
    #[allow(dead_code, reason = "read by the tests")]
    pub(crate) fn queues(&self) -> impl Iterator<Item = &str> {
        self.queues.keys().map(String::as_str)
    }
}

/// What a process knows of the queue limits stored in Redis: read at start, refreshed at every
/// heartbeat (or, for a fetcher without a process, when older than [`STANDALONE_REFRESH`]), and
/// at once when a fetch finds a queue's stored limits changed. Shared by the fetchers of a
/// process's capsules.
///
/// It also holds the process's [`Cooldowns`]: a queue over its limits is over them for every
/// capsule fetching it.
#[derive(Debug)]
pub(crate) struct QueueLimits {
    snapshot: RwLock<Arc<Snapshot>>,
    refreshing: tokio::sync::Mutex<()>,
    /// A snapshot older than this is refreshed by the next fetch.
    max_age: Duration,
    /// The lease of the concurrency permits of fetched jobs.
    lease: Duration,
    cooldowns: Cooldowns,
}

impl QueueLimits {
    /// A view not read yet, refreshed by fetches once older than `max_age`; fetched jobs'
    /// concurrency permits get `lease`.
    pub(crate) fn new(max_age: Duration, lease: Duration) -> Self {
        Self {
            snapshot: RwLock::new(Arc::new(Snapshot::default())),
            refreshing: tokio::sync::Mutex::new(()),
            max_age,
            lease,
            cooldowns: Cooldowns::default(),
        }
    }

    /// The lease of the concurrency permits of fetched jobs.
    pub(crate) fn lease(&self) -> Duration {
        self.lease
    }

    /// The queues left out of this process's fetches for a while.
    pub(crate) fn cooldowns(&self) -> &Cooldowns {
        &self.cooldowns
    }

    /// The current view.
    pub(crate) fn current(&self) -> Arc<Snapshot> {
        self.snapshot
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Reads the queue limits from Redis and replaces the view. Returns the queues whose stored
    /// limits changed (added, edited or removed); their cooldowns end, since they no longer say
    /// anything. Refreshes run one at a time, so that an older read never replaces a newer one.
    pub(crate) async fn refresh(&self, redis: &RedisPool) -> Result<Vec<String>> {
        let _refreshing = self.refreshing.lock().await;
        self.load(redis).await
    }

    /// [`QueueLimits::refresh`], for a caller holding `refreshing`.
    async fn load(&self, redis: &RedisPool) -> Result<Vec<String>> {
        let stored: Vec<(Vec<u8>, Vec<u8>)> = redis
            .with_retry(|mut conn| async move {
                ::redis::cmd("HGETALL")
                    .arg(keys::QUEUE_LIMITS)
                    .query_async(&mut conn)
                    .await
            })
            .await?;
        let mut queues = HashMap::with_capacity(stored.len());
        for (queue, value) in stored {
            let queue = String::from_utf8_lossy(&queue).into_owned();
            let value = String::from_utf8_lossy(&value).into_owned();
            let rules = match parse(&queue, &value) {
                Ok(limits) => Some(Arc::from(rules(&queue, &limits))),
                Err(error) => {
                    if crate::limit::throttle::warn_now(&format!("bad-queue-limits:{queue}")) {
                        tracing::error!(
                            queue = %queue,
                            stored = %value,
                            %error,
                            "the stored limits of a queue are not valid: its jobs are not \
                             fetched until they are fixed (set them again, or clear them)"
                        );
                    }
                    None
                }
            };
            queues.insert(
                queue,
                QueueEntry {
                    stored: value,
                    rules,
                },
            );
        }
        let fresh = Arc::new(Snapshot {
            queues,
            loaded: Some(Instant::now()),
        });
        let mut snapshot = self
            .snapshot
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        let mut changed: Vec<String> = fresh
            .queues
            .iter()
            .filter(|(queue, entry)| {
                snapshot
                    .queues
                    .get(*queue)
                    .is_none_or(|old| old.stored != entry.stored)
            })
            .map(|(queue, _)| queue.clone())
            .collect();
        changed.extend(
            snapshot
                .queues
                .keys()
                .filter(|queue| !fresh.queues.contains_key(*queue))
                .cloned(),
        );
        *snapshot = fresh;
        drop(snapshot);
        self.cooldowns.end(&changed);
        Ok(changed)
    }

    /// Makes sure the view was read and is not older than its maximum age: the first call reads
    /// it (and fails if Redis does); later ones refresh an old view at most once at a time,
    /// keeping the old one if Redis fails (the heartbeat tries again).
    pub(crate) async fn ensure_fresh(&self, redis: &RedisPool) -> Result<Vec<String>> {
        let loaded = self.current().loaded;
        match loaded {
            None => {
                let _refreshing = self.refreshing.lock().await;
                if self.current().loaded.is_some() {
                    return Ok(Vec::new());
                }
                self.load(redis).await
            }
            Some(at) if at.elapsed() >= self.max_age => {
                let Ok(_refreshing) = self.refreshing.try_lock() else {
                    return Ok(Vec::new());
                };
                if self
                    .current()
                    .loaded
                    .is_some_and(|at| at.elapsed() < self.max_age)
                {
                    return Ok(Vec::new());
                }
                match self.load(redis).await {
                    Ok(changed) => Ok(changed),
                    Err(error) => {
                        tracing::debug!(%error, "could not refresh the queue limits");
                        Ok(Vec::new())
                    }
                }
            }
            Some(_) => Ok(Vec::new()),
        }
    }
}

/// How a fetch may treat a limited queue, by this process's [`Cooldowns`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Admission {
    /// Not cooling: any fetch may check it.
    Open,
    /// Its cooldown ended and this fetch checks it first: the other fetches of the process
    /// leave it out until the fetch reports what it found ([`Probes`]).
    Probe,
    /// Cooling, or being checked by another fetch: left out.
    Out,
}

/// How this process's fetches share its limited queues, so that their load on Redis does not
/// grow with the number of idle processors:
///
/// * a queue over its limits is left out of every fetch of the process for a while (it
///   **cools down**); one fetch of the process **times** the cooldown (the first one that
///   would wait for it), and once it ends a single fetch checks the queue again (the
///   **probe**) while the others keep leaving it out, until the probe reports;
/// * a single fetch **watches** an empty limited queue in its blocking step (without taking
///   its jobs: they leave a limited queue only through `LIMIT_FETCH_GATE`), and the other
///   fetches leave the queue to it.
///
/// Nothing here wakes every idle fetch at once: the fetch that frees a slot, is refused or
/// takes a job checks the queue next itself, and wakes at most one sleeping fetch of each
/// capsule that fetches the queue ([`Cooldowns::wake`]). Only changed limits wake them all
/// ([`Cooldowns::notified`]).
#[derive(Debug, Default)]
pub(crate) struct Cooldowns {
    state: Mutex<CoolState>,
    /// Whether any queue may be cooling or watched (a fast path for fetches that never see
    /// limits).
    any: AtomicBool,
    /// Bumped before every wake-up, so that a fetch about to sleep notices one it missed.
    generation: AtomicU64,
    changed: Notify,
    /// The last fetch id handed out ([`Cooldowns::fetch_id`]).
    fetches: AtomicU64,
    /// Per queue, the wakers of the capsules fetching it ([`Cooldowns::register`]).
    wakers: Mutex<HashMap<String, Vec<Arc<Notify>>>>,
}

#[derive(Debug, Default)]
struct CoolState {
    cooling: HashMap<String, Cooling>,
    /// The fetch watching each limited queue in its blocking step.
    watchers: HashMap<String, u64>,
}

/// One cooling queue.
#[derive(Debug, Clone, Copy)]
struct Cooling {
    /// When it comes back (in the past: it is back, waiting for a probe).
    until: Instant,
    /// A concurrency limit was full (a slot freed in this process ends the cooldown).
    busy: bool,
    /// The fetch checking the queue again since its cooldown ended.
    probe: Option<u64>,
    /// The fetch that wakes when the cooldown ends.
    timer: Option<u64>,
    /// A slot was freed here while the queue was probed: a refusal for a full concurrency limit
    /// may predate it, so it does not cool the queue again.
    freed: bool,
}

impl Cooldowns {
    fn lock(&self) -> std::sync::MutexGuard<'_, CoolState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Keeps the fast path's flag in step with `state`.
    fn sync(&self, state: &CoolState) {
        let any = !state.cooling.is_empty() || !state.watchers.is_empty();
        self.any.store(any, Ordering::Release);
    }

    /// A new fetch id (each fetch worker has its own).
    pub(crate) fn fetch_id(&self) -> u64 {
        self.fetches.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Registers a capsule fetching `queues`: `waker` wakes one of its sleeping workers.
    pub(crate) fn register<'q>(
        &self,
        queues: impl IntoIterator<Item = &'q str>,
        waker: &Arc<Notify>,
    ) {
        let mut wakers = self.wakers.lock().unwrap_or_else(PoisonError::into_inner);
        for queue in queues {
            let list = wakers.entry(queue.to_owned()).or_default();
            if !list.iter().any(|known| Arc::ptr_eq(known, waker)) {
                list.push(Arc::clone(waker));
            }
        }
    }

    /// Wakes one sleeping worker of each capsule that fetches `queue` (it may have a job, or a
    /// cooldown nobody times): capsules sharing a queue share its cooldowns, so they share the
    /// wake-ups too.
    pub(crate) fn wake(&self, queue: &str) {
        let wakers = self.wakers.lock().unwrap_or_else(PoisonError::into_inner);
        for waker in wakers.get(queue).into_iter().flatten() {
            waker.notify_one();
        }
    }

    /// How the fetch `fetch` may treat the limited queue `queue` at `now`: a queue watched by
    /// another fetch is left to it; a queue whose cooldown ended is handed to one fetch
    /// ([`Admission::Probe`]) and left out of the others.
    pub(crate) fn admit(&self, queue: &str, fetch: u64, now: Instant) -> Admission {
        if !self.any.load(Ordering::Acquire) {
            return Admission::Open;
        }
        let mut state = self.lock();
        if state
            .watchers
            .get(queue)
            .is_some_and(|&watcher| watcher != fetch)
        {
            return Admission::Out;
        }
        let Some(cooling) = state.cooling.get_mut(queue) else {
            return Admission::Open;
        };
        if cooling.until > now {
            return Admission::Out;
        }
        match cooling.probe {
            None => {
                cooling.probe = Some(fetch);
                Admission::Probe
            }
            Some(probe) if probe == fetch => Admission::Probe,
            Some(_) => Admission::Out,
        }
    }

    /// Leaves `queue` out until `wait` from now (at least 1 ms, at most [`MAX_COOLDOWN`]; at
    /// least [`BUSY_COOLDOWN`] when a concurrency limit is full). It replaces the queue's
    /// cooldown and ends a probe of it; a full concurrency limit reported by a probe during
    /// which this process freed a slot does not cool the queue (it is checked again at once).
    pub(crate) fn cool(&self, queue: &str, wait: Duration, busy: bool) {
        let mut wait = wait.clamp(Duration::from_millis(1), MAX_COOLDOWN);
        if busy {
            wait = wait.max(BUSY_COOLDOWN);
        }
        let now = Instant::now();
        let mut state = self.lock();
        let freed = busy
            && state
                .cooling
                .get(queue)
                .is_some_and(|cooling| cooling.probe.is_some() && cooling.freed);
        state.cooling.insert(
            queue.to_owned(),
            Cooling {
                until: if freed { now } else { now + wait },
                busy,
                probe: None,
                timer: None,
                freed: false,
            },
        );
        self.sync(&state);
    }

    /// The probe of `queue` by `fetch` found the queue open (a job was granted, or it is empty
    /// or paused): it is no longer cooling.
    fn settle(&self, queue: &str, fetch: u64) {
        let mut state = self.lock();
        if state
            .cooling
            .get(queue)
            .is_some_and(|cooling| cooling.probe == Some(fetch))
        {
            state.cooling.remove(queue);
            self.sync(&state);
        }
    }

    /// The probe of `queue` by `fetch` ended without checking it (a job was found in an
    /// earlier queue, or the fetch failed): the next fetch checks it. Whether it was released.
    fn unchecked(&self, queue: &str, fetch: u64) -> bool {
        let mut state = self.lock();
        match state.cooling.get_mut(queue) {
            Some(cooling) if cooling.probe == Some(fetch) => {
                cooling.probe = None;
                true
            }
            _ => false,
        }
    }

    /// When the first of `queues` (a capsule's) comes back, for the fetch `fetch` about to wait
    /// (`None` when none cools): a queue whose cooldown ended comes back now. Each cooldown is
    /// timed by one fetch, the first to ask (the others do not wake for it). A queue being probed
    /// by another fetch comes back when the probe reports (which wakes a worker), or at the
    /// latest [`BUSY_COOLDOWN`] from now; a queue another fetch watches is left to it.
    pub(crate) fn next_end<'q>(
        &self,
        fetch: u64,
        queues: impl IntoIterator<Item = &'q str>,
        now: Instant,
    ) -> Option<Instant> {
        if !self.any.load(Ordering::Acquire) {
            return None;
        }
        let mut state = self.lock();
        if state.cooling.is_empty() {
            return None;
        }
        let CoolState { cooling, watchers } = &mut *state;
        let mut first: Option<Instant> = None;
        for queue in queues {
            let Some(cooling) = cooling.get_mut(queue) else {
                continue;
            };
            if cooling.timer.is_some_and(|timer| timer != fetch)
                || watchers.get(queue).is_some_and(|&watcher| watcher != fetch)
            {
                continue;
            }
            cooling.timer = Some(fetch);
            let at = match cooling.probe {
                Some(probe) if probe != fetch => now + BUSY_COOLDOWN,
                _ => cooling.until.max(now),
            };
            first = Some(first.map_or(at, |first| first.min(at)));
        }
        first
    }

    /// The fetch `fetch` stops waiting (it took a job, failed or ended): the cooldowns it timed
    /// are timed by the next fetch to wait. Returns those of `queues` (its capsule's) that now
    /// cool with nobody timing or probing them, for the caller to wake a worker for each.
    pub(crate) fn stop_timing<'q>(
        &self,
        fetch: u64,
        queues: impl IntoIterator<Item = &'q str>,
    ) -> Vec<String> {
        if !self.any.load(Ordering::Acquire) {
            return Vec::new();
        }
        let mut state = self.lock();
        if state.cooling.is_empty() {
            return Vec::new();
        }
        for cooling in state.cooling.values_mut() {
            if cooling.timer == Some(fetch) {
                cooling.timer = None;
            }
        }
        queues
            .into_iter()
            .filter(|queue| {
                state
                    .cooling
                    .get(*queue)
                    .is_some_and(|cooling| cooling.probe.is_none() && cooling.timer.is_none())
            })
            .map(str::to_owned)
            .collect()
    }

    /// This process freed a slot of `queue`: a cooldown for its full concurrency limit ends
    /// (the fetch that freed it checks the queue next), and a probe under way is not trusted to
    /// cool it again.
    pub(crate) fn freed(&self, queue: &str) {
        if !self.any.load(Ordering::Acquire) {
            return;
        }
        let mut state = self.lock();
        if let Some(cooling) = state.cooling.get_mut(queue)
            && cooling.busy
        {
            if cooling.probe.is_some() {
                cooling.freed = true;
            } else {
                cooling.until = Instant::now();
            }
        }
    }

    /// Ends the cooldowns of `queues` (their limits changed), waking every waiting fetch.
    pub(crate) fn end(&self, queues: &[String]) {
        if queues.is_empty() || !self.any.load(Ordering::Acquire) {
            return;
        }
        let ended = {
            let mut state = self.lock();
            let mut ended = false;
            for queue in queues {
                ended |= state.cooling.remove(queue).is_some();
            }
            self.sync(&state);
            ended
        };
        if ended {
            self.generation.fetch_add(1, Ordering::AcqRel);
            self.changed.notify_waiters();
        }
    }

    /// Makes `fetch` the watcher of the empty limited queue `queue` in its blocking step, unless
    /// another fetch of the process watches it, or the queue cools (a grant filled it meanwhile:
    /// it is checked again when its cooldown ends). The watch ends when the returned guard drops.
    pub(crate) fn watch(&self, queue: &str, fetch: u64) -> Option<Watch<'_>> {
        let mut state = self.lock();
        if state.cooling.contains_key(queue) {
            return None;
        }
        match state.watchers.get(queue) {
            Some(&watcher) if watcher != fetch => None,
            _ => {
                state.watchers.insert(queue.to_owned(), fetch);
                self.sync(&state);
                Some(Watch {
                    cooldowns: self,
                    queue: queue.to_owned(),
                    fetch,
                })
            }
        }
    }

    /// The probes of the fetch `fetch`: each queue [`Cooldowns::admit`] handed it is reported
    /// through them (or, if dropped unreported, handed to the next fetch).
    pub(crate) fn probes(&self, fetch: u64) -> Probes<'_> {
        Probes {
            cooldowns: self,
            fetch,
            queues: Vec::new(),
        }
    }

    /// Changes whenever the waiting fetches are woken: a fetch reads it before looking at the
    /// queues and, once its [`Cooldowns::notified`] future exists, sleeps only if it has not
    /// changed.
    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Resolves when limits changed (it receives the wake-ups from the moment it is created).
    pub(crate) fn notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.changed.notified()
    }
}

/// The watch of an empty limited queue by one fetch ([`Cooldowns::watch`]); ends when dropped.
#[derive(Debug)]
pub(crate) struct Watch<'a> {
    cooldowns: &'a Cooldowns,
    queue: String,
    fetch: u64,
}

impl Drop for Watch<'_> {
    fn drop(&mut self) {
        let mut state = self.cooldowns.lock();
        if state.watchers.get(&self.queue) == Some(&self.fetch) {
            state.watchers.remove(&self.queue);
            self.cooldowns.sync(&state);
        }
    }
}

/// The queues one fetch probes ([`Admission::Probe`]). Each is reported once the gate has
/// answered; one still unreported when this drops (the fetch failed or was dropped) is handed to
/// the next fetch.
#[derive(Debug)]
pub(crate) struct Probes<'a> {
    cooldowns: &'a Cooldowns,
    fetch: u64,
    queues: Vec<String>,
}

impl Probes<'_> {
    /// Notes that this fetch probes `queue`.
    pub(crate) fn add(&mut self, queue: &str) {
        if !self.queues.iter().any(|probed| probed == queue) {
            self.queues.push(queue.to_owned());
        }
    }

    /// The queue `queue` was refused again: it cools down for `wait` (see [`Cooldowns::cool`]).
    /// Also for a queue this fetch did not probe.
    pub(crate) fn cool(&mut self, queue: &str, wait: Duration, busy: bool) {
        self.queues.retain(|probed| probed != queue);
        self.cooldowns.cool(queue, wait, busy);
    }

    /// The gate took a job from `taken` (a queue name; `None`: none) and found the other
    /// queues it reached open (empty or paused) unless reported with [`Probes::cool`]: a probed
    /// queue the gate reached comes back for every fetch; one after `taken` was not checked and
    /// goes to the next fetch. Returns the queues handed on so (a worker should come for each).
    pub(crate) fn report(
        mut self,
        taken: Option<&str>,
        reached: impl Fn(&str) -> bool,
    ) -> Vec<String> {
        let mut handed_on = Vec::new();
        for queue in std::mem::take(&mut self.queues) {
            if taken == Some(queue.as_str()) || reached(&queue) {
                self.cooldowns.settle(&queue, self.fetch);
            } else if self.cooldowns.unchecked(&queue, self.fetch) {
                handed_on.push(queue);
            }
        }
        handed_on
    }
}

impl Drop for Probes<'_> {
    fn drop(&mut self) {
        for queue in &self.queues {
            self.cooldowns.unchecked(queue, self.fetch);
        }
    }
}

/// The permits of a job fetched from a limited queue: the leases of the queue's concurrency
/// limit (rate limits are charged once and hold nothing). Held by the engine while the job runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QueuePermit {
    /// The queue's name.
    pub(crate) queue: String,
    /// The leases to renew and release (empty without a concurrency limit).
    pub(crate) leases: Vec<LeaseRef>,
}

impl QueuePermit {
    /// The permit granted with `token` at `now_ms` for the limits `rules` of `queue`.
    pub(crate) fn granted(
        queue: &str,
        rules: &[Rule],
        token: &str,
        lease: Duration,
        now_ms: i64,
    ) -> Self {
        Self {
            queue: queue.to_owned(),
            leases: rules
                .iter()
                .filter(|rule| rule.is_concurrency())
                .map(|rule| LeaseRef::of(rule, token, lease, Some(STATS_TTL), Some(now_ms)))
                .collect(),
        }
    }
}

/// The permit token of one fetch: 16 random hex digits (a queue permit names no job).
pub(crate) fn token() -> String {
    crate::payload::random_hex(8)
}

/// One queue of a [`gate`] call, in fetch order.
#[derive(Debug, Clone, Copy)]
pub(crate) struct GateQueue<'a> {
    /// Its list (`queue:<name>`).
    pub(crate) key: &'a str,
    /// Its name.
    pub(crate) name: &'a str,
    /// Its limits as the process believes them (`None`: none).
    pub(crate) entry: Option<&'a QueueEntry>,
}

/// A [`gate`] call.
#[derive(Debug, Clone)]
pub(crate) struct GateArgs<'a> {
    /// The in-progress list (reliable fetch; ignored by basic fetch).
    pub(crate) inprogress: &'a str,
    /// The process identity (registered as an owner of an in-progress list).
    pub(crate) identity: &'a str,
    /// Reliable fetch (`LMOVE` to the in-progress list) or basic (`RPOP`).
    pub(crate) reliable: bool,
    /// The permit token of the fetch.
    pub(crate) token: &'a str,
    /// The lease of concurrency grants.
    pub(crate) lease: Duration,
    /// The queues, in fetch order.
    pub(crate) queues: Vec<GateQueue<'a>>,
    /// A fixed time, for tests (`None`: Redis's clock).
    pub(crate) now: Option<i64>,
}

/// A queue a [`gate`] call skipped: over its limits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Skipped {
    /// The queue's name.
    pub(crate) queue: String,
    /// The time until its rate limits free a unit (zero when only a concurrency limit is full).
    pub(crate) wait: Duration,
    /// A concurrency limit is full.
    pub(crate) busy: bool,
    /// The state of one of its limits cannot be read (a key of the wrong type): the queue is
    /// closed, and the other queues keep flowing.
    pub(crate) broken: bool,
}

/// The reply of a [`gate`] call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Gated {
    /// A job was taken from `queue_key`; `permit` is the token its limits were charged with
    /// (`None` for an unlimited queue), at `now_ms`.
    Found {
        /// The queue list the job came from.
        queue_key: String,
        /// The job, byte for byte.
        job: Vec<u8>,
        /// The permit token (limited queues only).
        token: Option<String>,
        /// The time of the grant (ms, Redis's clock; 0 for an unlimited queue).
        now_ms: i64,
        /// The queues over their limits the script skipped before this one.
        skipped: Vec<Skipped>,
        /// The job's queue as the grant left it, when its limits would refuse another job now
        /// (so that the process leaves it out without asking).
        full: Option<Skipped>,
    },
    /// No job was taken.
    Empty {
        /// The queues neither paused nor skipped, for the blocking step.
        open: Vec<String>,
        /// The queues over their limits.
        skipped: Vec<Skipped>,
    },
    /// The stored limits of this queue differ from the process's: re-read them. With the
    /// queues over their limits the script skipped before it.
    Stale(String, Vec<Skipped>),
}

/// The keys and arguments of [`LIMIT_FETCH_GATE`].
pub(crate) fn gate_call(args: &GateArgs<'_>) -> (Vec<String>, Vec<String>) {
    let mut keys = vec![
        args.inprogress.to_owned(),
        keys::INPROGRESS.to_owned(),
        keys::PAUSED.to_owned(),
        keys::QUEUE_LIMITS.to_owned(),
        keys::LIMITS.to_owned(),
        keys::limit_minute(Utc::now()),
    ];
    let mut argv = vec![
        now_arg(args.now),
        args.identity.to_owned(),
        args.token.to_owned(),
        if args.reliable { "R" } else { "B" }.to_owned(),
        millis(STATS_TTL).to_string(),
        args.queues.len().to_string(),
    ];
    for queue in &args.queues {
        keys.push(queue.key.to_owned());
        argv.push(queue.name.to_owned());
        let (stored, rules): (&str, &[Rule]) = match queue.entry {
            Some(entry) => (
                entry.stored.as_str(),
                entry.rules.as_deref().unwrap_or_default(),
            ),
            None => ("", &[]),
        };
        argv.push(stored.to_owned());
        argv.push(rules.len().to_string());
        for rule in rules {
            keys.push(rule.state.clone());
            keys.push(rule.stats.clone());
            argv.push(rule.letter().to_string());
            argv.push(rule.limit.capacity().to_string());
            argv.push(period_of(&rule.limit, args.lease).to_string());
            argv.push(rule.member.clone());
            argv.push(rule.signature());
        }
    }
    (keys, argv)
}

/// [`LIMIT_FETCH_GATE`]: the non-blocking step of a fetch from a capsule with limited queues.
///
/// A grant whose reply was lost leaves a concurrency lease held by the fetch's token, which
/// nothing would release before the lease ends. So after a failed call, or one sent more than
/// once, the token's leases are released (best effort) on every queue but the one a job came
/// from: a release of a lease never granted does nothing.
pub(crate) async fn gate(pool: &RedisPool, args: &GateArgs<'_>) -> Result<Gated> {
    let (keys, argv) = gate_call(args);
    let sent = AtomicUsize::new(0);
    let reply = pool
        .with_retry(|mut conn| {
            sent.fetch_add(1, Ordering::Relaxed);
            let (keys, argv) = (&keys, &argv);
            async move {
                let mut invocation = LIMIT_FETCH_GATE.script().prepare_invoke();
                for key in keys {
                    invocation.key(key);
                }
                for arg in argv {
                    invocation.arg(arg);
                }
                invocation.invoke_async(&mut conn).await
            }
        })
        .await
        .and_then(parse_gated);
    let resent = sent.load(Ordering::Relaxed) > 1;
    match &reply {
        Ok(Gated::Found { queue_key, .. }) if resent => {
            release_unused(pool, args, Some(queue_key)).await;
        }
        Ok(_) if !resent => {}
        _ => release_unused(pool, args, None).await,
    }
    reply
}

/// Releases the concurrency leases the token of `args` may hold on its queues, but `kept` (a
/// queue key). Best effort: a lease not released ends with its lease time.
pub(crate) async fn release_unused(pool: &RedisPool, args: &GateArgs<'_>, kept: Option<&str>) {
    let leases: Vec<LeaseRef> = args
        .queues
        .iter()
        .filter(|queue| kept != Some(queue.key))
        .filter_map(|queue| queue.entry.and_then(|entry| entry.rules.as_deref()))
        .flatten()
        .filter(|rule| rule.is_concurrency())
        .map(|rule| LeaseRef::of(rule, args.token, args.lease, None, None))
        .collect();
    release_token(pool, &leases).await;
}

/// Releases `leases` (no bookkeeping), logging a failure at debug level.
async fn release_token(pool: &RedisPool, leases: &[LeaseRef]) {
    if leases.is_empty() {
        return;
    }
    if let Err(error) = scripts::release(pool, leases, None).await {
        tracing::debug!(
            %error,
            "could not release the slots of a fetch that failed; they end with their lease"
        );
    }
}

/// The reply of [`LIMIT_FETCH_GATE`].
pub(crate) fn parse_gated(reply: Value) -> Result<Gated> {
    const SCRIPT: &str = "LIMIT_FETCH_GATE";
    let mut items = array(reply, SCRIPT)?.into_iter();
    let short = || unexpected(SCRIPT, "a short reply");
    let found = int(&items.next().ok_or_else(short)?)?;
    match found {
        1 => {
            let queue_key = text(&items.next().ok_or_else(short)?)?;
            let job = match items.next().ok_or_else(short)? {
                Value::BulkString(bytes) => bytes,
                Value::SimpleString(text) => text.into_bytes(),
                _ => return Err(unexpected(SCRIPT, "a job that is not a string")),
            };
            let token = text(&items.next().ok_or_else(short)?)?;
            let now_ms = int(&items.next().ok_or_else(short)?)?;
            let skipped = parse_skipped(items.next().unwrap_or(Value::Nil))?;
            let mut rest = items.map(|value| int(&value).unwrap_or(0));
            let full = match (rest.next(), rest.next(), rest.next()) {
                (Some(0), Some(wait), Some(busy)) => Some(Skipped {
                    queue: keys::queue_name(&queue_key).to_owned(),
                    wait: Duration::from_millis(u64::try_from(wait).unwrap_or(0)),
                    busy: busy == 1,
                    broken: false,
                }),
                _ => None,
            };
            Ok(Gated::Found {
                queue_key,
                job,
                token: (!token.is_empty()).then_some(token),
                now_ms,
                skipped,
                full,
            })
        }
        2 => {
            let queue = text(&items.next().ok_or_else(short)?)?;
            let skipped = parse_skipped(items.next().unwrap_or(Value::Nil))?;
            Ok(Gated::Stale(queue, skipped))
        }
        _ => {
            let open = array(items.next().unwrap_or(Value::Nil), SCRIPT)?
                .iter()
                .map(text)
                .collect::<Result<Vec<_>>>()?;
            let skipped = parse_skipped(items.next().unwrap_or(Value::Nil))?;
            Ok(Gated::Empty { open, skipped })
        }
    }
}

/// The skipped queues of a [`LIMIT_FETCH_GATE`] reply (name, wait, busy, flattened).
fn parse_skipped(flat: Value) -> Result<Vec<Skipped>> {
    const SCRIPT: &str = "LIMIT_FETCH_GATE";
    let flat = array(flat, SCRIPT)?;
    let mut skipped = Vec::with_capacity(flat.len() / 3);
    for triple in flat.chunks(3) {
        let [queue, wait, busy] = triple else {
            return Err(unexpected(SCRIPT, "a skipped queue without its wait"));
        };
        let busy = int(busy)?;
        skipped.push(Skipped {
            queue: text(queue)?,
            wait: Duration::from_millis(u64::try_from(int(wait)?).unwrap_or(0)),
            busy: busy == 1,
            broken: busy == 2,
        });
    }
    Ok(skipped)
}

/// The check of a job a blocking wait took from a limited queue: [`LIMIT_ACQUIRE`] of the
/// queue's limits, with the fetch's token, no wait line (the queue is the line) and the
/// counters kept.
///
/// [`LIMIT_ACQUIRE`]: crate::limit::lua::LIMIT_ACQUIRE
pub(crate) async fn check_taken(
    pool: &RedisPool,
    rules: &[Rule],
    token: &str,
    lease: Duration,
) -> Result<Acquired> {
    let args = AcquireArgs {
        rules,
        token,
        cost: 1,
        lease,
        ticket: None,
        line: false,
        place_within: None,
        expected_hold: None,
        stats: true,
        stats_ttl: STATS_TTL,
        waited: Duration::ZERO,
        now: None,
    };
    let acquired = scripts::acquire(pool, &args).await;
    if acquired.is_err() {
        // A grant whose reply was lost holds slots nothing else releases.
        let leases: Vec<LeaseRef> = rules
            .iter()
            .filter(|rule| rule.is_concurrency())
            .map(|rule| LeaseRef::of(rule, token, lease, None, None))
            .collect();
        release_token(pool, &leases).await;
    }
    acquired
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits(text: &str) -> Vec<Limit> {
        parse("exports", text).unwrap()
    }

    #[test]
    fn queues_take_one_limit_of_each_kind_a_fetch_can_enforce() {
        let all = limits("leaky=60/1m; bucket=10000/day;window=100/1m ;concurrency=3");
        assert_eq!(all.len(), 4);
        // The stored form: long specs, in kind order, whatever the order given.
        assert_eq!(
            stored_form(&all),
            "concurrency=3;window=100/1m;bucket=10000/day;leaky=60/1m"
        );
        assert_eq!(limits(&stored_form(&all)), {
            let mut sorted = all.clone();
            sorted.sort_by_key(kind_rank);
            sorted
        });
        // Short forms parse too.
        assert_eq!(stored_form(&limits("c=5")), "concurrency=5");
        for (text, reason) in [
            ("", "is given no limit"),
            (" ; ", "is given no limit"),
            ("points=100/1m", "points limits need a cost"),
            ("c=2;c=3", "two concurrency limits"),
            ("leaky=1/1s;l=2/1s", "two leaky limits"),
            ("c=0", "at least 1"),
            ("window=5/1s per key", "`per key` applies to throttles only"),
            ("speed=5", "unknown kind"),
            ("window=200000/1s", "a window keeps one entry per grant"),
            ("c=3, w=100/1m", "separate limits with `;`"),
            ("concurrency=3 window=100/1m", "separate limits with `;`"),
            ("c=0", "(a limit of the queue `exports`)"),
        ] {
            let error = parse("exports", text).unwrap_err().to_string();
            assert!(error.contains(reason), "{text}: {error}");
        }
        let error = check("", &limits("c=1")).unwrap_err().to_string();
        assert!(error.contains("need a queue name"), "{error}");
    }

    #[test]
    fn queue_limits_use_the_reserved_name() {
        let rules = rules("exports", &limits("c=3;w=10/1s"));
        assert_eq!(rules[0].state, "limit:c:@exports");
        assert_eq!(rules[0].stats, "limit-stats:c:@exports");
        assert_eq!(rules[0].member, "c:@exports");
        assert_eq!(rules[0].counter, "@exports");
        assert_eq!(rules[1].state, "limit:w:@exports");
        // Queue names keep every character (a `:` is not a key separator here).
        assert_eq!(
            super::rules("low:prio", &limits("c=1"))[0].state,
            "limit:c:@low:prio"
        );
    }

    #[test]
    fn leases_outlast_three_heartbeats() {
        assert_eq!(lease_for(Duration::from_secs(10)), Duration::from_secs(60));
        assert_eq!(lease_for(Duration::from_secs(30)), Duration::from_secs(120));
    }

    #[test]
    fn cooldowns_end_on_time_or_when_a_slot_frees() {
        let cooldowns = Cooldowns::default();
        let (one, two) = (cooldowns.fetch_id(), cooldowns.fetch_id());
        assert_ne!(one, two);
        let queues = ["a", "b"];
        let now = Instant::now();
        assert_eq!(cooldowns.admit("a", one, now), Admission::Open);
        assert_eq!(cooldowns.next_end(one, queues, now), None);
        cooldowns.cool("a", Duration::ZERO, true);
        cooldowns.cool("b", Duration::from_secs(60), false);
        let now = Instant::now();
        assert_eq!(cooldowns.admit("a", one, now), Admission::Out);
        assert_eq!(cooldowns.admit("b", one, now), Admission::Out);
        // A full concurrency limit cools for at least 250 ms; a rate wait for at most 5 s.
        let first = cooldowns.next_end(one, queues, now).unwrap();
        assert!(first >= now + Duration::from_millis(200), "{first:?}");
        let later = now + Duration::from_secs(6);
        assert_eq!(cooldowns.admit("b", one, later), Admission::Probe);
        // A slot freed in the process ends a busy queue's cooldown, not a rate wait.
        cooldowns.cool("b", Duration::from_secs(1), false);
        cooldowns.freed("a");
        cooldowns.freed("b");
        let now = Instant::now();
        assert_eq!(cooldowns.admit("a", one, now), Admission::Probe);
        assert_eq!(cooldowns.admit("b", one, now), Admission::Out);
        // Changed limits end any cooldown, waking the waiting fetches.
        let generation = cooldowns.generation();
        cooldowns.end(&["a".to_owned(), "b".to_owned()]);
        assert_ne!(cooldowns.generation(), generation);
        assert_eq!(cooldowns.admit("b", two, Instant::now()), Admission::Open);
        assert_eq!(cooldowns.next_end(two, queues, Instant::now()), None);
    }

    #[test]
    fn one_fetch_times_a_cooldown_and_only_its_capsules_queues_count() {
        let cooldowns = Cooldowns::default();
        let (one, two) = (cooldowns.fetch_id(), cooldowns.fetch_id());
        cooldowns.cool("a", Duration::from_secs(1), false);
        let now = Instant::now();
        // Another capsule's queues do not shorten a wait.
        assert_eq!(cooldowns.next_end(one, ["other"], now), None);
        // The first fetch to wait times it; the others do not wake for it.
        assert!(cooldowns.next_end(one, ["a"], now).is_some());
        assert!(cooldowns.next_end(one, ["a"], now).is_some());
        assert_eq!(cooldowns.next_end(two, ["a"], now), None);
        // Once it takes a job, the next fetch to wait times it.
        assert_eq!(cooldowns.stop_timing(one, ["a"]), ["a"]);
        assert!(cooldowns.next_end(two, ["a"], now).is_some());
        assert!(cooldowns.stop_timing(one, ["a"]).is_empty());
        // A queue back from its cooldown but not checked yet comes back now.
        cooldowns.cool("b", Duration::from_millis(1), false);
        let later = Instant::now() + Duration::from_millis(10);
        assert_eq!(cooldowns.next_end(one, ["b"], later), Some(later));
    }

    #[test]
    fn one_fetch_probes_a_queue_whose_cooldown_ended() {
        let cooldowns = Cooldowns::default();
        let (one, two) = (cooldowns.fetch_id(), cooldowns.fetch_id());
        cooldowns.cool("a", Duration::from_millis(1), false);
        let later = Instant::now() + Duration::from_millis(10);
        // The first fetch to look probes it; the others leave it out until the probe reports
        // (or, at the latest, a busy cooldown later).
        let mut probes = cooldowns.probes(one);
        assert_eq!(cooldowns.admit("a", one, later), Admission::Probe);
        probes.add("a");
        assert_eq!(cooldowns.admit("a", one, later), Admission::Probe);
        assert_eq!(cooldowns.admit("a", two, later), Admission::Out);
        assert_eq!(
            cooldowns.next_end(two, ["a"], later),
            Some(later + BUSY_COOLDOWN)
        );
        // Refused again: it cools again, without waking anyone.
        let generation = cooldowns.generation();
        probes.cool("a", Duration::from_millis(1), false);
        assert_eq!(cooldowns.generation(), generation);
        assert!(cooldowns.next_end(two, ["a"], Instant::now()).is_some());
        assert!(probes.report(None, |_| true).is_empty());
        let later = Instant::now() + Duration::from_millis(10);
        assert_eq!(cooldowns.admit("a", two, later), Admission::Probe);
        // A probe dropped unreported (the fetch failed) hands the queue to the next fetch.
        drop(cooldowns.probes(two));
        let mut probes = cooldowns.probes(two);
        probes.add("a");
        drop(probes);
        assert_eq!(cooldowns.admit("a", one, later), Admission::Probe);
        // A probe that found the queue open ends its cooldown for every fetch.
        let mut probes = cooldowns.probes(one);
        probes.add("a");
        assert!(probes.report(None, |_| true).is_empty());
        assert_eq!(cooldowns.admit("a", two, later), Admission::Open);
        // A probe that took a job from an earlier queue hands the queue on (the caller wakes
        // another fetch).
        cooldowns.cool("a", Duration::from_millis(1), false);
        let later = Instant::now() + Duration::from_millis(10);
        let mut probes = cooldowns.probes(one);
        assert_eq!(cooldowns.admit("a", one, later), Admission::Probe);
        probes.add("a");
        assert_eq!(probes.report(Some("first"), |_| false), ["a"]);
        assert_eq!(cooldowns.admit("a", two, later), Admission::Probe);
    }

    #[test]
    fn a_queue_probed_elsewhere_comes_back_within_a_busy_cooldown() {
        let cooldowns = Cooldowns::default();
        let (one, two) = (cooldowns.fetch_id(), cooldowns.fetch_id());
        cooldowns.cool("a", Duration::from_millis(1), false);
        let later = Instant::now() + Duration::from_millis(10);
        assert_eq!(cooldowns.admit("a", one, later), Admission::Probe);
        // If the probe's hand-on never reaches this fetch, it still looks again soon.
        assert_eq!(
            cooldowns.next_end(two, ["a"], later),
            Some(later + BUSY_COOLDOWN)
        );
        // A watched queue cannot be watched while it cools, and a cooling queue watched by
        // another fetch is left to it.
        let _probe = cooldowns.probes(one);
        assert!(cooldowns.watch("a", two).is_none());
        let watch = cooldowns.watch("b", one).unwrap();
        cooldowns.cool("b", Duration::from_millis(1), false);
        assert_eq!(cooldowns.next_end(two, ["b"], later), None);
        drop(watch);
    }

    #[tokio::test]
    async fn wake_ups_reach_every_capsule_fetching_the_queue() {
        let cooldowns = Cooldowns::default();
        let (first, second, other) = (
            Arc::new(Notify::new()),
            Arc::new(Notify::new()),
            Arc::new(Notify::new()),
        );
        cooldowns.register(["q", "a"], &first);
        cooldowns.register(["q"], &second);
        cooldowns.register(["q"], &second);
        cooldowns.register(["b"], &other);
        cooldowns.wake("q");
        let soon = Duration::from_millis(100);
        assert!(tokio::time::timeout(soon, first.notified()).await.is_ok());
        assert!(tokio::time::timeout(soon, second.notified()).await.is_ok());
        assert!(tokio::time::timeout(soon, other.notified()).await.is_err());
    }

    #[test]
    fn a_slot_freed_during_a_probe_overrides_its_full_report() {
        let cooldowns = Cooldowns::default();
        let one = cooldowns.fetch_id();
        cooldowns.cool("a", Duration::ZERO, true);
        let later = Instant::now() + Duration::from_millis(300);
        let mut probes = cooldowns.probes(one);
        assert_eq!(cooldowns.admit("a", one, later), Admission::Probe);
        probes.add("a");
        // The slot frees while the probe's refusal is on its way.
        cooldowns.freed("a");
        probes.cool("a", Duration::ZERO, true);
        assert_eq!(
            cooldowns.admit("a", one, Instant::now()),
            Admission::Probe,
            "checked again at once"
        );
    }

    #[test]
    fn one_fetch_watches_a_limited_queue() {
        let cooldowns = Cooldowns::default();
        let (one, two) = (cooldowns.fetch_id(), cooldowns.fetch_id());
        let watch = cooldowns.watch("a", one).unwrap();
        assert!(cooldowns.watch("a", two).is_none());
        assert!(cooldowns.watch("b", two).is_some());
        // The other fetches leave a watched queue to its watcher.
        let now = Instant::now();
        assert_eq!(cooldowns.admit("a", two, now), Admission::Out);
        assert_eq!(cooldowns.admit("a", one, now), Admission::Open);
        drop(watch);
        assert_eq!(cooldowns.admit("a", two, now), Admission::Open);
        assert!(cooldowns.watch("a", two).is_some());
    }

    #[test]
    fn gate_calls_carry_each_queues_limits() {
        let entry = QueueEntry {
            stored: "concurrency=2;window=5/1s".to_owned(),
            rules: Some(Arc::from(rules("exports", &limits("c=2;w=5/1s")))),
        };
        let args = GateArgs {
            inprogress: "host:1:abc:inprogress",
            identity: "host:1:abc",
            reliable: true,
            token: "0123456789abcdef",
            lease: Duration::from_secs(60),
            queues: vec![
                GateQueue {
                    key: "queue:default",
                    name: "default",
                    entry: None,
                },
                GateQueue {
                    key: "queue:exports",
                    name: "exports",
                    entry: Some(&entry),
                },
            ],
            now: Some(1_000),
        };
        let (keys, argv) = gate_call(&args);
        assert_eq!(
            keys[..4],
            [
                "host:1:abc:inprogress",
                "inprogress",
                "paused",
                "queue-limits"
            ]
        );
        assert_eq!(keys[4], "limits");
        assert!(keys[5].starts_with("l|"));
        assert_eq!(
            keys[6..],
            [
                "queue:default",
                "queue:exports",
                "limit:c:@exports",
                "limit-stats:c:@exports",
                "limit:w:@exports",
                "limit-stats:w:@exports",
            ]
        );
        assert_eq!(
            argv,
            [
                "1000",
                "host:1:abc",
                "0123456789abcdef",
                "R",
                "86400000",
                "2",
                "default",
                "",
                "0",
                "exports",
                "concurrency=2;window=5/1s",
                "2",
                "c",
                "2",
                "60000",
                "c:@exports",
                "c|2",
                "w",
                "5",
                "1000",
                "w:@exports",
                "w|5|1000",
            ]
        );
    }

    #[test]
    fn gate_replies_are_parsed() {
        let bulk = |text: &str| Value::BulkString(text.as_bytes().to_vec());
        let found = Value::Array(vec![
            Value::Int(1),
            bulk("queue:exports"),
            bulk("{\"jid\":\"a\"}"),
            bulk("0123456789abcdef"),
            Value::Int(1_000),
            Value::Array(vec![bulk("first"), Value::Int(0), Value::Int(2)]),
            Value::Int(0),
            Value::Int(250),
            Value::Int(0),
        ]);
        assert_eq!(
            parse_gated(found).unwrap(),
            Gated::Found {
                queue_key: "queue:exports".to_owned(),
                job: b"{\"jid\":\"a\"}".to_vec(),
                token: Some("0123456789abcdef".to_owned()),
                now_ms: 1_000,
                skipped: vec![Skipped {
                    queue: "first".to_owned(),
                    wait: Duration::ZERO,
                    busy: false,
                    broken: true,
                }],
                full: Some(Skipped {
                    queue: "exports".to_owned(),
                    wait: Duration::from_millis(250),
                    busy: false,
                    broken: false,
                }),
            }
        );
        let unlimited = Value::Array(vec![
            Value::Int(1),
            bulk("queue:default"),
            bulk("{}"),
            bulk(""),
            Value::Int(0),
        ]);
        assert!(matches!(
            parse_gated(unlimited).unwrap(),
            Gated::Found { token: None, .. }
        ));
        let empty = Value::Array(vec![
            Value::Int(0),
            Value::Array(vec![bulk("queue:default")]),
            Value::Array(vec![bulk("exports"), Value::Int(120), Value::Int(1)]),
        ]);
        assert_eq!(
            parse_gated(empty).unwrap(),
            Gated::Empty {
                open: vec!["queue:default".to_owned()],
                skipped: vec![Skipped {
                    queue: "exports".to_owned(),
                    wait: Duration::from_millis(120),
                    busy: true,
                    broken: false,
                }],
            }
        );
        // Lua turns empty tables into empty arrays.
        let none = Value::Array(vec![
            Value::Int(0),
            Value::Array(vec![]),
            Value::Array(vec![]),
        ]);
        assert_eq!(
            parse_gated(none).unwrap(),
            Gated::Empty {
                open: vec![],
                skipped: vec![]
            }
        );
        let stale = Value::Array(vec![Value::Int(2), bulk("exports")]);
        assert_eq!(
            parse_gated(stale).unwrap(),
            Gated::Stale("exports".to_owned(), vec![])
        );
        assert!(parse_gated(Value::Array(vec![Value::Int(1)])).is_err());
    }
}
