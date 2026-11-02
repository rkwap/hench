//! [`Limiter`]: a named limiter, used inside jobs and anywhere with Redis
//! (`docs/design/ratelimit.md` §2.4.1), and the acquisition loop it shares with the throttle
//! gate: ask, and while the slot given comes within the time allowed, wait for it (sleeping
//! toward the slot for rate limits; polling, and waking on releases in this process, for
//! concurrency), never with a blocking Redis command.

use std::collections::HashSet;
use std::fmt;
use std::future::Future;
use std::sync::{Arc, LazyLock, Mutex, PoisonError};
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::error::{Error, Result};
use crate::limit::permit::Permit;
use crate::limit::rule::{self, Rule};
use crate::limit::scripts::{self, AcquireArgs, Acquired, LeaseRef, Place, UsageArgs};
use crate::limit::target::{LimitTarget, Store};
use crate::limit::throttle::warn_now;
use crate::limit::{Backoff, BackoffInput, Interval, Limit, OverLimit, Usage, wakeups};
use crate::payload::random_hex;
use crate::processor::is_unavailable;

/// A named limiter: cheap to build and to clone (an `Arc` inside); no I/O until used. Every
/// process sharing the Redis shares its limits.
///
/// A limiter's identities are also those of the [`Throttle`](crate::Throttle) of the same
/// name: the two share their state (one budget for jobs and other code). Give them different
/// names unless that is what you mean; a limit used with two definitions is logged at `warn`.
///
/// ```
/// use std::time::Duration;
/// use hench::limit::Limiter;
///
/// let stripe = Limiter::window("stripe-api", 100, Duration::from_secs(1));
/// assert_eq!(stripe.key(42).id(), "stripe-api:42");
/// ```
///
/// Inside a job, a refusal returned with `?` reschedules the job (see [`OverLimit`]):
///
/// ```ignore
/// static STRIPE: LazyLock<Limiter> =
///     LazyLock::new(|| Limiter::window("stripe-api", 100, Duration::from_secs(1)));
///
/// #[hench::job(queue = "billing")]
/// async fn charge(ctx: &JobContext, account: i64, cents: i64) -> Result<(), JobError> {
///     let _permit = STRIPE.key(account).acquire(ctx).await?;
///     call_stripe(account, cents).await?;
///     Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct Limiter {
    inner: Arc<LimiterInner>,
}

#[derive(Clone)]
struct LimiterInner {
    /// The limits, and for each the limiter it came from (name and key).
    parts: Vec<Part>,
    /// The same limits, for [`Limiter::limits`].
    limits: Vec<Limit>,
    max_wait: Duration,
    lease: Duration,
    reschedules: Option<u32>,
    max_delay: Option<Duration>,
    backoff: Option<Backoff>,
    spread: bool,
    stats: bool,
    stats_ttl: Duration,
    fail_open: bool,
    unlimited: bool,
}

/// The identity a limit belongs to: the name of its limiter and the key of its instance.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Part {
    name: String,
    key: Option<String>,
}

impl Limiter {
    /// Default in-process wait for a slot.
    pub const MAX_WAIT: Duration = Duration::from_secs(5);
    /// Default lease of a concurrency permit.
    pub const LEASE: Duration = Duration::from_secs(30);
    /// Default expiry of a limiter's counters after its last use.
    pub const STATS_TTL: Duration = Duration::from_secs(24 * 60 * 60);

    /// A limiter named `name` with `limit`. Names are 1 to 100 characters of letters, digits,
    /// `_`, `.` and `-`.
    pub fn new(name: impl Into<String>, limit: Limit) -> Limiter {
        let name = name.into();
        Self::with_parts(vec![(Part { name, key: None }, limit)])
    }

    fn with_parts(parts: Vec<(Part, Limit)>) -> Limiter {
        let (parts, limits) = parts.into_iter().unzip();
        Limiter {
            inner: Arc::new(LimiterInner {
                parts,
                limits,
                max_wait: Self::MAX_WAIT,
                lease: Self::LEASE,
                reschedules: None,
                max_delay: None,
                backoff: None,
                spread: true,
                stats: true,
                stats_ttl: Self::STATS_TTL,
                fail_open: false,
                unlimited: false,
            }),
        }
    }

    /// At most `limit` holders at a time.
    pub fn concurrent(name: impl Into<String>, limit: u32) -> Limiter {
        Self::new(name, Limit::Concurrency { limit })
    }

    /// At most `limit` grants in any window of `period`.
    pub fn window(name: impl Into<String>, limit: u32, period: Duration) -> Limiter {
        Self::new(name, Limit::Window { limit, period })
    }

    /// At most `limit` grants per `interval` bucket (UTC-aligned).
    pub fn bucket(name: impl Into<String>, limit: u32, interval: Interval) -> Limiter {
        Self::new(name, Limit::Bucket { limit, interval })
    }

    /// A leaky bucket of `size` drained in `drain`: bursts of `size`, then one every
    /// `drain / size`.
    pub fn leaky(name: impl Into<String>, size: u32, drain: Duration) -> Limiter {
        Self::new(name, Limit::Leaky { size, drain })
    }

    /// A points budget of `capacity` refilled in `refill`: each call takes its cost
    /// ([`Limiter::acquire_n`]), corrected afterwards with [`Permit::points_used`].
    pub fn points(name: impl Into<String>, capacity: u32, refill: Duration) -> Limiter {
        Self::new(name, Limit::Points { capacity, refill })
    }

    /// Always grants, without Redis (tests, exempt callers).
    pub fn unlimited() -> Limiter {
        let mut limiter = Self::with_parts(Vec::new());
        Arc::make_mut(&mut limiter.inner).unlimited = true;
        limiter
    }

    /// Every limit of every limiter given, checked and charged together: all or none (an
    /// improvement over limiters nested in each other, which charge the outer limit when the
    /// inner one refuses). Each limit keeps its limiter's name and key; the options are the
    /// first limiter's.
    pub fn all(limiters: impl IntoIterator<Item = Limiter>) -> Limiter {
        let limiters: Vec<Limiter> = limiters.into_iter().collect();
        let Some(first) = limiters.first() else {
            return Self::with_parts(Vec::new());
        };
        let mut inner = LimiterInner::clone(&first.inner);
        inner.parts = Vec::new();
        inner.limits = Vec::new();
        inner.unlimited = false;
        for limiter in &limiters {
            inner.parts.extend(limiter.inner.parts.iter().cloned());
            inner.limits.extend(limiter.inner.limits.iter().copied());
        }
        inner.unlimited = inner.parts.is_empty() && limiters.iter().all(Limiter::is_unlimited);
        Limiter {
            inner: Arc::new(inner),
        }
    }

    fn update(&self, change: impl FnOnce(&mut LimiterInner)) -> Limiter {
        let mut inner = (*self.inner).clone();
        change(&mut inner);
        Limiter {
            inner: Arc::new(inner),
        }
    }

    /// The instance of this limiter for one key (a tenant, an account): `stripe` and `42` give
    /// `stripe:42`, with limits of its own. Keys are any text; those of more than 128 bytes, and
    /// empty ones or ones with NUL, are replaced by `#` and the first 22 characters of their
    /// SHA-256 in base64url. For [`Limiter::all`], every limit gets the key.
    #[must_use]
    pub fn key(&self, key: impl fmt::Display) -> Limiter {
        let key = key.to_string();
        self.update(|inner| {
            for part in &mut inner.parts {
                part.key = Some(key.clone());
            }
        })
    }

    /// How long an acquisition may wait in process for a slot (default 5 s; zero never waits).
    /// It waits only when the slot it is given comes within the time left: a refusal whose
    /// slot is further away returns at once (from a job: rescheduled at that slot).
    #[must_use]
    pub fn max_wait(self, wait: Duration) -> Limiter {
        self.update(|inner| inner.max_wait = wait)
    }

    /// Concurrency: the lease of a permit (default 30 s, from 1 s to 1 day), renewed every
    /// third of it while the permit is held. A process that crashes frees its slots within one
    /// lease.
    #[must_use]
    pub fn lease(self, lease: Duration) -> Limiter {
        self.update(|inner| inner.lease = lease)
    }

    /// Times a job may be rescheduled by this limiter before the failure path takes over
    /// (default: the process's, 20; 0 fails the job at the first refusal).
    #[must_use]
    pub fn reschedules(self, max: u32) -> Limiter {
        self.update(|inner| inner.reschedules = Some(max))
    }

    /// Longest reschedule delay (default: the process's, 24 h); delays are at least 1 s.
    #[must_use]
    pub fn max_delay(self, delay: Duration) -> Limiter {
        self.update(|inner| inner.max_delay = Some(delay))
    }

    /// Reschedule delay policy of this limiter's refusals (default: the process's, else the
    /// slot given by the wait line, else 60 s × the reschedule's number), clamped to [1 s, max
    /// delay], plus up to 10 % jitter. A closure that panics is reported and the default used.
    #[must_use]
    pub fn backoff(
        self,
        f: impl Fn(&BackoffInput<'_>) -> Duration + Send + Sync + 'static,
    ) -> Limiter {
        let backoff: Backoff = Arc::new(f);
        self.update(|inner| inner.backoff = Some(backoff))
    }

    /// Keep a wait line (default `true`): each refusal gets the next slot, spaced at the
    /// limit's rate, so that callers coming back are spread out. Off, a refusal gets the
    /// limit's own wait only.
    #[must_use]
    pub fn spread(self, on: bool) -> Limiter {
        self.update(|inner| inner.spread = on)
    }

    /// Per-limiter counters and index entry (default `true`). Off saves about 40 % of the cost
    /// of a call, and turns the wait line off; the limiter is then not listed by the API.
    #[must_use]
    pub fn stats(self, on: bool) -> Limiter {
        self.update(|inner| inner.stats = on)
    }

    /// Expiry of the counters after the last use (default 24 h).
    #[must_use]
    pub fn stats_ttl(self, ttl: Duration) -> Limiter {
        self.update(|inner| inner.stats_ttl = ttl)
    }

    /// When Redis cannot be reached (or is out of memory, loading, failing over): refuse with
    /// the error (default), or grant without limiting, with a warning. Concurrency limits always
    /// refuse: granting a slot nobody counts could break the limit.
    #[must_use]
    pub fn fail_open(self, on: bool) -> Limiter {
        self.update(|inner| inner.fail_open = on)
    }

    /// The limiter's name (the first limit's, for [`Limiter::all`]).
    pub fn name(&self) -> &str {
        self.inner
            .parts
            .first()
            .map_or("unlimited", |part| part.name.as_str())
    }

    /// The identity of this instance: the name, and `:key` for a keyed instance (its key as
    /// stored: hashed when longer than 128 bytes).
    pub fn id(&self) -> String {
        match self.inner.parts.first() {
            Some(part) => rule::id(
                &part.name,
                part.key.as_deref().map(rule::stored_key).as_deref(),
            ),
            None => self.name().to_owned(),
        }
    }

    /// The limits.
    pub fn limits(&self) -> &[Limit] {
        &self.inner.limits
    }

    /// Whether this limiter always grants ([`Limiter::unlimited`]).
    pub fn is_unlimited(&self) -> bool {
        self.inner.unlimited
    }

    /// Checks names, keys and parameters (every call does too, returning `Error::Config`).
    pub fn validate(&self) -> Result<()> {
        self.rules().map(|_| ())
    }

    /// The rules of a call, checked.
    fn rules(&self) -> Result<Vec<Rule>> {
        let inner = &self.inner;
        rule::check_lease(inner.lease)?;
        rule::check_stats_ttl(inner.stats_ttl)?;
        if inner
            .max_delay
            .is_some_and(|delay| delay < Duration::from_secs(1))
        {
            return Err(Error::Config(
                "a limiter's max_delay must be at least 1 s".to_owned(),
            ));
        }
        let mut rules = Vec::with_capacity(inner.parts.len());
        for (part, limit) in inner.parts.iter().zip(&inner.limits) {
            rule::check_name(&part.name)?;
            rule::check_limit(limit)?;
            let key = part.key.as_deref().map(rule::stored_key);
            rules.push(Rule::new(
                *limit,
                rule::id(&part.name, key.as_deref()),
                part.name.clone(),
            ));
        }
        rule::check_distinct(&rules)?;
        Ok(rules)
    }

    /// Takes one unit from every limit (and a slot from every concurrency limit), waiting up to
    /// [`max_wait`](Limiter::max_wait). Refused: [`Error::OverLimit`], with the time until the
    /// caller's place in the wait line.
    ///
    /// From a job (a [`JobContext`](crate::JobContext) target), a refusal always takes a place
    /// in the line, since the job returns it (`?`) and is rescheduled for that place; a job
    /// rescheduled so comes back with it (`limited.s`) and keeps it, its first acquisition of
    /// the limit in that execution. A job that handles a refusal itself (skips the work, tries
    /// again later) should call [`try_acquire`](Limiter::try_acquire), which never takes one.
    pub async fn acquire(&self, on: impl LimitTarget) -> Result<Permit> {
        self.acquire_with(&on, 1, self.inner.max_wait, Call::Acquire)
            .await
    }

    /// Takes `cost` units from every rate limit (window, bucket, leaky, points; concurrency limits
    /// take one slot whatever the cost). A cost above a limit can never be granted:
    /// `Error::Config`. Places in the wait line as [`acquire`](Limiter::acquire).
    pub async fn acquire_n(&self, on: impl LimitTarget, cost: u32) -> Result<Permit> {
        self.acquire_with(&on, cost, self.inner.max_wait, Call::Acquire)
            .await
    }

    /// Takes one unit without waiting. A refusal says when the wait line ends but takes no place
    /// in it, whatever the target (a job too): callers that do not come back for a slot must
    /// not push the line away from the limit's real availability.
    pub async fn try_acquire(&self, on: impl LimitTarget) -> Result<Permit> {
        self.acquire_with(&on, 1, Duration::ZERO, Call::Try).await
    }

    /// Acquires, runs `f`, then gives the permit back (also when `f` panics or the returned
    /// future is dropped): Sidekiq's `within_limit`. Refused: [`Error::OverLimit`], and `f`
    /// does not run.
    pub async fn run<F, T>(&self, on: impl LimitTarget, f: F) -> Result<T>
    where
        F: Future<Output = T> + Send,
    {
        let permit = self.acquire(on).await?;
        let output = f.await;
        if let Err(error) = permit.release().await {
            tracing::debug!(
                %error,
                limiter = %self.id(),
                "could not give a permit back; its lease ends by itself"
            );
        }
        Ok(output)
    }

    /// The current use of each of this limiter's limits (read only). Empty for unlimited
    /// limiters and targets.
    pub async fn usage(&self, on: impl LimitTarget) -> Result<Vec<Usage>> {
        if self.is_unlimited() {
            return Ok(Vec::new());
        }
        let rules = self.rules()?;
        let Some(store) = on.backend().0 else {
            return Ok(Vec::new());
        };
        let reads: Vec<UsageArgs> = rules.iter().map(UsageArgs::of).collect();
        let replies = store.usage(&reads).await?;
        Ok(rules
            .iter()
            .zip(replies)
            .map(|(rule, reply)| Usage {
                identity: rule.member.clone(),
                limit: rule.limit,
                used: reply.used.unwrap_or(0.0),
                next_free_in: reply.free_in,
            })
            .collect())
    }

    /// Deletes the state (every slot freed) and the counters of this limiter's identities.
    /// Holders keep running; their releases find nothing.
    pub async fn reset(&self, on: impl LimitTarget) -> Result<()> {
        if self.is_unlimited() {
            return Ok(());
        }
        let rules = self.rules()?;
        if let Some(store) = on.backend().0 {
            store.reset(&rules).await?;
            for rule in rules.iter().filter(|rule| rule.is_concurrency()) {
                wakeups::notify(&rule.state);
            }
        }
        Ok(())
    }

    async fn acquire_with(
        &self,
        on: &impl LimitTarget,
        cost: u32,
        max_wait: Duration,
        call: Call,
    ) -> Result<Permit> {
        if self.is_unlimited() {
            return Ok(Permit::unlimited());
        }
        if cost == 0 {
            return Err(Error::Config(
                "the cost of an acquisition must be at least 1".to_owned(),
            ));
        }
        let rules = self.rules()?;
        if let Some(rule) = rules
            .iter()
            .find(|rule| !rule.is_concurrency() && cost > rule.limit.capacity())
        {
            return Err(impossible(rule, cost));
        }
        let Some(store) = on.backend().0 else {
            return Ok(Permit::unlimited());
        };
        let stopping = on.stopping();
        let inner = &self.inner;
        let token = if rules.iter().any(Rule::is_concurrency) {
            random_hex(8)
        } else {
            String::new()
        };
        // A job rescheduled for its place in the line of one of these limits comes back with it.
        let members: Vec<&str> = rules.iter().map(|rule| rule.member.as_str()).collect();
        let request = Request {
            rules: &rules,
            token: &token,
            cost,
            lease: inner.lease,
            max_wait,
            line: inner.spread && inner.stats,
            stats: inner.stats,
            stats_ttl: inner.stats_ttl,
            ticket: on.ticket(&members),
            comes_back: call == Call::Acquire && on.comes_back(),
            expected_hold: None,
        };
        match obtain(&store, &request, stopping.as_ref()).await {
            Ok(Obtained::Granted { now_ms, waited }) => Ok(Permit::granted(
                store,
                &request,
                inner.stats.then_some(inner.stats_ttl),
                now_ms,
                waited,
            )),
            Ok(Obtained::Refused {
                limiting,
                wait,
                place,
            }) => {
                let mut over = self.refusal(&rules[limiting], wait);
                if let Some(slot) = place {
                    over = over.with_place(slot);
                }
                Err(Error::OverLimit(over))
            }
            Ok(Obtained::Impossible { limiting }) => Err(impossible(&rules[limiting], cost)),
            Err(error) => {
                if inner.fail_open
                    && !rules.iter().any(Rule::is_concurrency)
                    && is_unavailable(&error)
                {
                    if warn_now(&format!("fail-open:{}", self.id())) {
                        tracing::warn!(
                            %error,
                            limiter = %self.id(),
                            "Redis is unavailable: granting without limiting (fail_open)"
                        );
                    }
                    return Ok(Permit::unlimited());
                }
                Err(error)
            }
        }
    }

    /// The refusal of `rule` with `wait` until the caller's slot, with this limiter's options.
    fn refusal(&self, rule: &Rule, wait: Duration) -> OverLimit {
        let inner = &self.inner;
        let mut over = OverLimit::new(rule.member.clone())
            .with_limit(rule.limit)
            .with_retry_after(wait);
        if let Some(max) = inner.reschedules {
            over = over.with_reschedules(max);
        }
        if let Some(delay) = inner.max_delay {
            over = over.with_max_delay(delay);
        }
        if let Some(backoff) = &inner.backoff {
            over = over.with_backoff(Arc::clone(backoff));
        }
        over
    }
}

/// Which method acquires: `acquire`/`acquire_n` (a job's refusal takes a place in the wait
/// line, for its reschedule) or `try_acquire` (never takes one).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Call {
    Acquire,
    Try,
}

/// The error of a cost above a limit.
fn impossible(rule: &Rule, cost: u32) -> Error {
    Error::Config(format!(
        "a cost of {cost} can never be granted by `{}` ({}): it is above the limit",
        rule.member, rule.limit
    ))
}

impl fmt::Debug for Limiter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = &self.inner;
        f.debug_struct("Limiter")
            .field("id", &self.id())
            .field("limits", &inner.limits)
            .field("max_wait", &inner.max_wait)
            .field("lease", &inner.lease)
            .field("reschedules", &inner.reschedules)
            .field("max_delay", &inner.max_delay)
            .field("backoff", &inner.backoff.is_some())
            .field("spread", &inner.spread)
            .field("stats", &inner.stats)
            .field("stats_ttl", &inner.stats_ttl)
            .field("fail_open", &inner.fail_open)
            .field("unlimited", &inner.unlimited)
            .finish()
    }
}

/// An acquisition: what [`obtain`] asks for.
#[derive(Debug, Clone)]
pub(crate) struct Request<'a> {
    /// The limits, granted all or none.
    pub(crate) rules: &'a [Rule],
    /// The token of the concurrency leases.
    pub(crate) token: &'a str,
    /// Units taken from every rate limit.
    pub(crate) cost: u32,
    /// The lease of concurrency grants.
    pub(crate) lease: Duration,
    /// How long to wait in process for a slot.
    pub(crate) max_wait: Duration,
    /// Keep a wait line.
    pub(crate) line: bool,
    /// Keep the counters.
    pub(crate) stats: bool,
    /// The counters' expiry.
    pub(crate) stats_ttl: Duration,
    /// The place the caller already holds in the line (the slot an earlier refusal gave a job
    /// that was parked or rescheduled for it).
    pub(crate) ticket: Option<i64>,
    /// Whether a refusal brings the caller back at its slot whatever it is (a job parked or
    /// rescheduled for it): such a caller always takes a place in the line, another one only
    /// for a slot it waits for in process.
    pub(crate) comes_back: bool,
    /// How long a concurrency permit is expected to be held, until the limit's own releases
    /// tell (`None`: the default).
    pub(crate) expected_hold: Option<Duration>,
}

impl Request<'_> {
    /// The concurrency leases of a grant at `acquired_ms`.
    pub(crate) fn leases(&self, acquired_ms: i64) -> Vec<LeaseRef> {
        self.rules
            .iter()
            .filter(|rule| rule.is_concurrency())
            .map(|rule| {
                LeaseRef::of(
                    rule,
                    self.token,
                    self.lease,
                    self.stats.then_some(self.stats_ttl),
                    Some(acquired_ms),
                )
            })
            .collect()
    }
}

/// How an acquisition ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Obtained {
    /// Granted at `now_ms` (the store's clock), after waiting `waited`.
    Granted {
        /// When.
        now_ms: i64,
        /// How long it waited.
        waited: Duration,
    },
    /// Refused by `rules[limiting]`: the caller's slot comes in `wait`.
    Refused {
        /// The rule that decided the wait.
        limiting: usize,
        /// Until the caller's slot.
        wait: Duration,
        /// The slot (ms, the store's clock) of the place the caller holds in the line, if it
        /// holds one: the ticket to come back with.
        place: Option<i64>,
    },
    /// The cost is above `rules[limiting]`'s limit: never grantable.
    Impossible {
        /// The rule.
        limiting: usize,
    },
}

/// Shortest sleep toward a slot.
const MIN_SLEEP: Duration = Duration::from_millis(10);

/// First and longest pause between two polls of a concurrency limit.
const POLL_MIN: Duration = Duration::from_millis(25);
const POLL_MAX: Duration = Duration::from_millis(400);

/// Mismatches logged (once per process each): the limit's kind and name, without the key (a
/// keyed limiter changed in a deploy is one mismatch, not one per key), with the two
/// signatures.
static MISMATCHES: LazyLock<Mutex<HashSet<String>>> = LazyLock::new(|| Mutex::new(HashSet::new()));

/// Most mismatches remembered; past it, each is logged at most once a minute.
const MAX_MISMATCHES: usize = 1_024;

/// Asks `store` for `request`'s limits; while refused with a slot within the time left (and
/// `stopping` not cancelled), waits for it and asks again, keeping its place in the line.
///
/// A caller that does not come back on its own takes a place in the line only for a slot it
/// waits for: a refusal it will not wait for (such as `try_acquire`'s) only tells it when the
/// end of the line is, so that callers who never return cannot push the line away from the
/// limit's real availability.
pub(crate) async fn obtain(
    store: &Store,
    request: &Request<'_>,
    stopping: Option<&CancellationToken>,
) -> Result<Obtained> {
    let started = tokio::time::Instant::now();
    let mut ticket = request.ticket;
    let mut poll = POLL_MIN;
    loop {
        let place_within =
            (!request.comes_back).then(|| request.max_wait.saturating_sub(started.elapsed()));
        let args = AcquireArgs {
            rules: request.rules,
            token: request.token,
            cost: request.cost,
            lease: request.lease,
            ticket,
            line: request.line,
            place_within,
            expected_hold: request.expected_hold,
            stats: request.stats,
            stats_ttl: request.stats_ttl,
            waited: started.elapsed(),
            now: None,
        };
        let reply = match store.acquire(&args).await {
            Ok(reply) => reply,
            Err(error) => {
                give_back_after_timeout(store, request, &error);
                return Err(error);
            }
        };
        let (wait, limiting) = match reply {
            Acquired::Granted {
                now_ms, mismatch, ..
            } => {
                if let Some(previous) = mismatch {
                    warn_mismatch(request.rules, &previous);
                }
                let waited = started.elapsed();
                if !ticket.is_none() {
                    tracing::debug!(
                        waited_ms = u64::try_from(waited.as_millis()).unwrap_or(u64::MAX),
                        "granted after waiting"
                    );
                }
                return Ok(Obtained::Granted { now_ms, waited });
            }
            Acquired::Refused {
                impossible: true,
                limiting,
                ..
            } => return Ok(Obtained::Impossible { limiting }),
            Acquired::Refused {
                wait_ms,
                limiting,
                slot_ms,
                place,
                mismatch,
                ..
            } => {
                if let Some(previous) = mismatch {
                    warn_mismatch(request.rules, &previous);
                }
                // A new place replaces the ticket; a kept one leaves it as it was (its slot is
                // still the caller's place, however often the caller is told to wait more).
                match place {
                    Place::New => ticket = Some(slot_ms),
                    Place::Kept | Place::None => {}
                }
                (Duration::from_millis(wait_ms), limiting)
            }
        };
        let limiting = limiting.min(request.rules.len().saturating_sub(1));
        let remaining = request.max_wait.saturating_sub(started.elapsed());
        let stopped = stopping.is_some_and(CancellationToken::is_cancelled);
        if stopped || request.max_wait.is_zero() || wait > remaining {
            return Ok(Obtained::Refused {
                limiting,
                wait,
                place: ticket,
            });
        }
        let slot_at = tokio::time::Instant::now() + wait;
        let rule = &request.rules[limiting];
        let pause = if rule.is_concurrency() {
            // Poll before the slot: a holder may finish early (here or in another process).
            let pause = poll
                .mul_f64(0.5 + fastrand::f64() * 0.5)
                .min(wait.max(MIN_SLEEP));
            poll = (poll * 2).min(POLL_MAX);
            pause
        } else if request.line {
            // Rate limits with a wait line: the slot is exact, and every waiter has its own.
            wait.max(MIN_SLEEP)
        } else {
            // Without a line, callers refused together are told the same wait: a little
            // jitter spreads them.
            let jitter = (wait / 10)
                .mul_f64(fastrand::f64())
                .min(remaining.saturating_sub(wait));
            (wait + jitter).max(MIN_SLEEP)
        };
        let woken = rule.is_concurrency().then(|| wakeups::listen(&rule.state));
        let notified = async {
            match &woken {
                Some(notify) => notify.notified().await,
                None => std::future::pending().await,
            }
        };
        let stop = async {
            match stopping {
                Some(stopping) => stopping.cancelled().await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            () = tokio::time::sleep(pause) => {}
            () = notified => {}
            () = stop => {
                let wait = slot_at.saturating_duration_since(tokio::time::Instant::now());
                return Ok(Obtained::Refused { limiting, wait, place: ticket });
            }
        }
    }
}

/// After a timeout (the request may have run without its reply), gives back the token's leases
/// in the background, without bookkeeping: the lease ends anyway, sooner is better.
fn give_back_after_timeout(store: &Store, request: &Request<'_>, error: &Error) {
    let Error::Redis(redis) = error else {
        return;
    };
    if !redis.is_timeout() || request.token.is_empty() {
        return;
    }
    let leases: Vec<LeaseRef> = request
        .rules
        .iter()
        .filter(|rule| rule.is_concurrency())
        .map(|rule| LeaseRef::of(rule, request.token, request.lease, None, None))
        .collect();
    if let (Ok(handle), Store::Redis(pool)) = (tokio::runtime::Handle::try_current(), store) {
        let pool = pool.clone();
        handle.spawn(async move {
            if let Err(error) = scripts::release(&pool, &leases, None).await {
                tracing::debug!(%error, "could not give back the leases of a timed-out acquisition");
            }
        });
    }
}

/// Logs that a limit is used with another definition than the one its state was last used
/// with: once per process for each limit name (whatever the key) and pair of definitions.
fn warn_mismatch(rules: &[Rule], previous: &str) {
    let Some(rule) = rules.iter().find(|rule| rule.signature() != previous) else {
        return;
    };
    // `<kind>:<name>` without the key: the id's name ends at its first `:`.
    let name = rule.id.split(':').next().unwrap_or(&rule.id);
    let subject = format!("{}:{name}|{previous}|{}", rule.letter(), rule.signature());
    let first = {
        let mut seen = MISMATCHES.lock().unwrap_or_else(PoisonError::into_inner);
        if seen.contains(&subject) {
            false
        } else if seen.len() < MAX_MISMATCHES {
            seen.insert(subject)
        } else {
            warn_now(&format!("mismatch:{subject}"))
        }
    };
    if first {
        tracing::warn!(
            limit = %rule.member,
            stored = previous,
            used = %rule.signature(),
            "a limit is used with another definition than the one its state was last used with \
             (normal while a deploy changes it; other identities of the same limiter, such as \
             other keys, are not reported again): both share its state. If they are two \
             different limits (a limiter and a throttle, say), give them different names"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limiters_have_identities() {
        let stripe = Limiter::window("stripe", 100, Duration::from_secs(1));
        assert_eq!(stripe.id(), "stripe");
        assert_eq!(stripe.key(42).id(), "stripe:42");
        assert_eq!(stripe.key("acme").name(), "stripe");
        let long = stripe.key("k".repeat(129));
        let id = long.id();
        assert!(id.starts_with("stripe:#"), "{id}");
        assert_eq!(id.len(), "stripe:#".len() + 22);
        assert_eq!(
            stripe.key("k".repeat(128)).id().len(),
            "stripe:".len() + 128
        );
        let both = Limiter::all([stripe.clone(), Limiter::concurrent("erp", 5)]);
        assert_eq!(both.limits().len(), 2);
        assert_eq!(both.name(), "stripe");
        assert!(Limiter::unlimited().is_unlimited());
        assert!(!stripe.clone().max_wait(Duration::ZERO).is_unlimited());
        assert!(Limiter::all([]).limits().is_empty());
    }

    #[test]
    fn composites_keep_each_limiters_identity() {
        let hourly = Limiter::window("crm-h", 100, Duration::from_secs(3_600));
        let minutely = Limiter::window("crm-m", 5, Duration::from_secs(60));
        let both = Limiter::all([hourly, minutely]).key(7);
        let members: Vec<String> = both
            .rules()
            .unwrap()
            .into_iter()
            .map(|rule| rule.member)
            .collect();
        assert_eq!(members, ["w:crm-h:7", "w:crm-m:7"]);
        // Two limits sharing one state are refused.
        let clash = Limiter::all([
            Limiter::leaky("x", 5, Duration::from_secs(1)),
            Limiter::points("x", 5, Duration::from_secs(1)),
        ]);
        assert!(clash.validate().is_err());
    }

    #[test]
    fn definitions_are_checked() {
        assert!(
            Limiter::window("ok", 1, Duration::from_secs(1))
                .validate()
                .is_ok()
        );
        for limiter in [
            Limiter::window("bad name", 1, Duration::from_secs(1)),
            Limiter::window("zero", 0, Duration::from_secs(1)),
            Limiter::concurrent("lease", 1).lease(Duration::from_millis(1)),
            Limiter::window("ttl", 1, Duration::from_secs(1)).stats_ttl(Duration::ZERO),
            Limiter::window("delay", 1, Duration::from_secs(1)).max_delay(Duration::ZERO),
        ] {
            let error = limiter.validate().unwrap_err();
            assert!(matches!(error, Error::Config(_)), "{limiter:?}: {error}");
        }
        // Keys come from data: any text is a key (stored hashed when it cannot be kept as is).
        for key in ["", "a\0b"] {
            let keyed = Limiter::window("nul", 1, Duration::from_secs(1)).key(key);
            keyed.validate().unwrap();
            assert!(keyed.id().starts_with("nul:#"), "{}", keyed.id());
        }
    }

    #[tokio::test]
    async fn unlimited_limiters_make_no_call() {
        let detached = crate::redis::RedisPool::detached();
        let permit = Limiter::unlimited().acquire(&detached).await.unwrap();
        assert!(!permit.is_lost());
        permit.release().await.unwrap();
        assert!(
            Limiter::unlimited()
                .usage(&detached)
                .await
                .unwrap()
                .is_empty()
        );
        let output = Limiter::unlimited()
            .run(&detached, async { 42 })
            .await
            .unwrap();
        assert_eq!(output, 42);
        // A real limiter needs the store.
        let error = Limiter::window("x", 1, Duration::from_secs(1))
            .acquire(&detached)
            .await
            .unwrap_err();
        assert!(!matches!(error, Error::OverLimit(_)), "{error}");
    }

    #[tokio::test]
    async fn costs_above_a_limit_are_configuration_errors() {
        let detached = crate::redis::RedisPool::detached();
        let error = Limiter::window("x", 3, Duration::from_secs(1))
            .acquire_n(&detached, 4)
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Config(_)), "{error}");
        let error = Limiter::window("x", 3, Duration::from_secs(1))
            .acquire_n(&detached, 0)
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Config(_)), "{error}");
    }

    use crate::limit::test_logs::warnings;

    #[test]
    fn a_second_definition_of_an_identity_is_logged_once() {
        let rule = Rule::new(
            Limit::Window {
                limit: 5,
                period: Duration::from_secs(1),
            },
            "mismatch-test".to_owned(),
            "mismatch-test".to_owned(),
        );
        let logs = warnings(|| {
            warn_mismatch(std::slice::from_ref(&rule), "w|10|1000");
            warn_mismatch(std::slice::from_ref(&rule), "w|10|1000");
        });
        assert_eq!(logs.matches("another definition").count(), 1, "{logs}");
        assert!(
            logs.contains("w:mismatch-test")
                && logs.contains("w|10|1000")
                && logs.contains("w|5|1000"),
            "{logs}"
        );
        // A definition that agrees with the stored one says nothing.
        let other = Rule::new(
            Limit::Concurrency { limit: 2 },
            "agreeing-test".to_owned(),
            "agreeing-test".to_owned(),
        );
        let logs = warnings(|| warn_mismatch(std::slice::from_ref(&other), "c|2"));
        assert!(logs.is_empty(), "{logs}");
        // A keyed limiter whose definition changed: one warning, not one per key.
        let logs = warnings(|| {
            for key in 0..200 {
                let keyed = Rule::new(
                    Limit::Window {
                        limit: 10,
                        period: Duration::from_secs(1),
                    },
                    format!("keyed-test:{key}"),
                    "keyed-test".to_owned(),
                );
                warn_mismatch(std::slice::from_ref(&keyed), "w|5|1000");
            }
        });
        assert_eq!(logs.matches("another definition").count(), 1, "{logs}");
        assert!(logs.contains("w:keyed-test:0"), "{logs}");
    }
}
