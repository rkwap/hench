//! The types of rate limiting: limits and their kinds, throttles, the over-limit refusal, the
//! payload fields `throttle` and `limited`, and the crate-internal reschedule handles
//! (`docs/design/ratelimit.md` §2.4).

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use crate::client::Stored;
use crate::error::{JobError, Result};
use crate::payload::JobPayload;

/// One rule of a limiter or throttle. [`FromStr`](std::str::FromStr) and [`Display`] use the
/// spec grammar (`concurrency=10`, `window=100/1s`, `bucket=10000/day`, `leaky=60/1m`,
/// `points=1000/1m`; see [`crate::limit`]).
///
/// [`Display`]: fmt::Display
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Limit {
    /// At most `limit` holders at a time (leases).
    Concurrency {
        /// Most holders.
        limit: u32,
    },
    /// At most `limit` grants in any sliding window of `period`.
    Window {
        /// Most grants per window.
        limit: u32,
        /// The window.
        period: Duration,
    },
    /// At most `limit` grants per fixed `interval` (UTC-aligned buckets).
    Bucket {
        /// Most grants per bucket.
        limit: u32,
        /// The bucket.
        interval: Interval,
    },
    /// A leaky bucket of `size` units that drains completely in `drain` (bursts of `size`,
    /// then one unit every `drain / size`).
    Leaky {
        /// The bucket's size.
        size: u32,
        /// Time to drain a full bucket.
        drain: Duration,
    },
    /// A points budget of `capacity` refilled in `refill` (costs per call, corrected after).
    Points {
        /// The budget.
        capacity: u32,
        /// Time to refill a spent budget.
        refill: Duration,
    },
}

impl Limit {
    /// The shape of the limit's state in Redis.
    pub fn kind(&self) -> LimitKind {
        match self {
            Limit::Concurrency { .. } => LimitKind::Concurrency,
            Limit::Window { .. } => LimitKind::Window,
            Limit::Bucket { .. } => LimitKind::Bucket,
            Limit::Leaky { .. } | Limit::Points { .. } => LimitKind::Leaky,
        }
    }

    /// The number of units the limit allows (its `N`).
    pub fn capacity(&self) -> u32 {
        match *self {
            Limit::Concurrency { limit }
            | Limit::Window { limit, .. }
            | Limit::Bucket { limit, .. } => limit,
            Limit::Leaky { size, .. } => size,
            Limit::Points { capacity, .. } => capacity,
        }
    }
}

/// The interval of a [`Limit::Bucket`]: buckets start at UTC multiples of it since the epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Interval {
    /// One second.
    Second,
    /// One minute.
    Minute,
    /// One hour.
    Hour,
    /// One day (resets at 00:00 UTC).
    Day,
    /// Any whole number of milliseconds.
    Every(Duration),
}

impl Interval {
    /// The interval's length.
    pub fn duration(&self) -> Duration {
        match *self {
            Interval::Second => Duration::from_secs(1),
            Interval::Minute => Duration::from_secs(60),
            Interval::Hour => Duration::from_secs(3_600),
            Interval::Day => Duration::from_secs(86_400),
            Interval::Every(every) => every,
        }
    }
}

/// Whether a throttle's limit applies to all its jobs together or to each throttle key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Scope {
    /// One limit shared by every job of the throttle.
    Global,
    /// One limit per throttle key (`name:key`).
    PerKey,
}

/// The shape of a limit's state in Redis, and its letter in keys (`limit:<letter>:<id>`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[non_exhaustive]
pub enum LimitKind {
    /// Leases (ZSET).
    Concurrency,
    /// Grant times (LIST).
    Window,
    /// Fixed buckets (HASH).
    Bucket,
    /// Leaky buckets and points budgets (STRING, a theoretical arrival time).
    Leaky,
}

impl LimitKind {
    /// The kind's letter in Redis keys: `c`, `w`, `b`, `g`.
    pub fn letter(self) -> char {
        match self {
            LimitKind::Concurrency => 'c',
            LimitKind::Window => 'w',
            LimitKind::Bucket => 'b',
            LimitKind::Leaky => 'g',
        }
    }

    /// The kind whose letter is `letter`.
    pub fn from_letter(letter: char) -> Option<Self> {
        Some(match letter {
            'c' => LimitKind::Concurrency,
            'w' => LimitKind::Window,
            'b' => LimitKind::Bucket,
            'g' => LimitKind::Leaky,
            _ => return None,
        })
    }
}

/// The order of a page of limits (`Limits::page`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum LimitOrder {
    /// Most recently used first.
    #[default]
    LastUsed,
    /// By identity.
    Name,
    /// Most refusals first.
    Refused,
}

/// Limits on job starts, applied by workers to the jobs whose payload names the throttle
/// ([ratelimit.md] §2.4.3): define it on the builder
/// ([`HenchBuilder::throttle`](crate::HenchBuilder::throttle)) or with the job type
/// ([`Job::throttle`](crate::Job::throttle)), and name it in the job's options
/// ([`JobOptions::throttle`](crate::JobOptions::throttle), with a key per tenant through
/// [`Job::throttle_key`](crate::Job::throttle_key) or
/// [`JobOptions::throttle_key`](crate::JobOptions::throttle_key)).
///
/// Workers check a job's throttle right after fetching it, before any middleware. A job over
/// it does not run: it is parked in the schedule until its slot in the throttle's wait line
/// (spaced at the limit's rate), without using a retry, and comes back then. Concurrency
/// permits are held by the worker for the job's whole execution, renewed by the heartbeat and
/// released once its outcome is written.
///
/// Throttles and [`Limiter`](crate::limit::Limiter)s share one namespace: a throttle's limits
/// are the limit identities a limiter of the same name (and key) uses, so the two share their
/// state. That is the way to make jobs and other code draw on one budget; otherwise give them
/// different names (the throttle `stripe`, the limiter `stripe-api`).
///
/// ```
/// use std::time::Duration;
/// use hench::Throttle;
///
/// // At most 25 Stripe jobs at a time, and 5 per second per account.
/// let stripe = Throttle::new("stripe")
///     .concurrency(25)
///     .window_per_key(5, Duration::from_secs(1));
/// assert_eq!(stripe.spec(), "c=25;w=5/1000 per key");
/// ```
///
/// [ratelimit.md]: https://github.com/rkwap/hench/blob/v0.1.0/docs/design/ratelimit.md
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Throttle {
    name: String,
    limits: Vec<(Limit, Scope)>,
    lease: Duration,
    reschedules: Option<u32>,
    max_delay: Duration,
    max_wait: Duration,
    fail_open: bool,
    stats: bool,
    stats_ttl: Duration,
}

impl Throttle {
    /// Default lease of the permits a worker holds for a running throttled job.
    pub const LEASE: Duration = Duration::from_secs(60);
    /// Default longest reschedule delay.
    pub const MAX_DELAY: Duration = Duration::from_secs(24 * 60 * 60);
    /// Default expiry of the counters and wait line of each of the throttle's identities after
    /// its last use.
    pub const STATS_TTL: Duration = Duration::from_secs(24 * 60 * 60);

    /// A throttle named `name` with no limit yet.
    pub fn new(name: impl Into<String>) -> Throttle {
        Throttle {
            name: name.into(),
            limits: Vec::new(),
            lease: Self::LEASE,
            reschedules: None,
            max_delay: Self::MAX_DELAY,
            max_wait: Duration::ZERO,
            fail_open: false,
            stats: true,
            stats_ttl: Self::STATS_TTL,
        }
    }

    /// Adds a limit shared by every job of the throttle.
    #[must_use]
    pub fn limit(mut self, limit: Limit) -> Throttle {
        self.limits.push((limit, Scope::Global));
        self
    }

    /// Adds a limit per throttle key.
    #[must_use]
    pub fn limit_per_key(mut self, limit: Limit) -> Throttle {
        self.limits.push((limit, Scope::PerKey));
        self
    }

    /// At most `n` jobs running at a time.
    #[must_use]
    pub fn concurrency(self, n: u32) -> Throttle {
        self.limit(Limit::Concurrency { limit: n })
    }

    /// At most `n` jobs running at a time per key.
    #[must_use]
    pub fn concurrency_per_key(self, n: u32) -> Throttle {
        self.limit_per_key(Limit::Concurrency { limit: n })
    }

    /// At most `n` starts in any window of `period`.
    #[must_use]
    pub fn window(self, n: u32, period: Duration) -> Throttle {
        self.limit(Limit::Window { limit: n, period })
    }

    /// At most `n` starts per key in any window of `period`.
    #[must_use]
    pub fn window_per_key(self, n: u32, period: Duration) -> Throttle {
        self.limit_per_key(Limit::Window { limit: n, period })
    }

    /// A leaky bucket of `size` starts drained in `drain`.
    #[must_use]
    pub fn leaky(self, size: u32, drain: Duration) -> Throttle {
        self.limit(Limit::Leaky { size, drain })
    }

    /// A leaky bucket of `size` starts drained in `drain`, per key.
    #[must_use]
    pub fn leaky_per_key(self, size: u32, drain: Duration) -> Throttle {
        self.limit_per_key(Limit::Leaky { size, drain })
    }

    /// At most `n` starts per `interval` bucket.
    #[must_use]
    pub fn bucket(self, n: u32, interval: Interval) -> Throttle {
        self.limit(Limit::Bucket { limit: n, interval })
    }

    /// At most `n` starts per `interval` bucket, per key.
    #[must_use]
    pub fn bucket_per_key(self, n: u32, interval: Interval) -> Throttle {
        self.limit_per_key(Limit::Bucket { limit: n, interval })
    }

    /// Lease of the concurrency permits a worker holds for a running job (default 60 s). The
    /// worker renews them on every heartbeat for as long as the job runs, so the lease is not the
    /// expected duration of a job: it is how long a crashed worker's slots stay taken. It must
    /// be at least three heartbeat intervals (`HenchBuilder::heartbeat_interval`, 10 s by
    /// default), else `build` refuses it: a lease that ends between two renewals lets other jobs
    /// take the slots of jobs still running.
    #[must_use]
    pub fn lease(mut self, lease: Duration) -> Throttle {
        self.lease = lease;
        self
    }

    /// How many times a job may be parked before the failure path takes over (default `None`:
    /// unlimited, a throttled job waits as long as it takes): a number (`.reschedules(5)`, like
    /// [`Limiter::reschedules`](crate::limit::Limiter::reschedules)) or an `Option`.
    #[must_use]
    pub fn reschedules(mut self, max: impl Into<Option<u32>>) -> Throttle {
        self.reschedules = max.into();
        self
    }

    /// Longest delay a job is parked for (default 24 h).
    #[must_use]
    pub fn max_delay(mut self, delay: Duration) -> Throttle {
        self.max_delay = delay;
        self
    }

    /// In-process wait for a slot before parking the job (default zero).
    #[must_use]
    pub fn max_wait(mut self, wait: Duration) -> Throttle {
        self.max_wait = wait;
        self
    }

    /// Run jobs unthrottled when Redis fails the check (default `false`: requeue them).
    #[must_use]
    pub fn fail_open(mut self, on: bool) -> Throttle {
        self.fail_open = on;
        self
    }

    /// Keep the counters and the wait line of each of the throttle's identities (default
    /// `true`). They cost about 330 bytes per identity (a key of a per-key limit) for
    /// [`stats_ttl`](Throttle::stats_ttl) after its last use, index entry included. Off, the
    /// identities are not listed by the API and the dashboard, and refused jobs get no place in
    /// a wait line: each is parked for its own wait, with a jitter, so a backlog comes back in
    /// bursts. Turn it off only for throttles with very many keys whose jobs are rarely refused.
    #[must_use]
    pub fn stats(mut self, on: bool) -> Throttle {
        self.stats = on;
        self
    }

    /// Expiry of the counters and wait line of each identity after its last use (default 24 h,
    /// from 1 s to 365 days). A per-key throttle with many keys can keep them for less: the line
    /// only matters while jobs are parked.
    #[must_use]
    pub fn stats_ttl(mut self, ttl: Duration) -> Throttle {
        self.stats_ttl = ttl;
        self
    }

    /// The throttle's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The limits and their scopes, in the order given.
    pub fn limits(&self) -> &[(Limit, Scope)] {
        &self.limits
    }

    /// The lease of engine-held permits.
    pub fn lease_duration(&self) -> Duration {
        self.lease
    }

    /// The reschedule budget (`None`: unlimited).
    pub fn reschedule_budget(&self) -> Option<u32> {
        self.reschedules
    }

    /// The longest delay a job is parked for.
    pub fn max_delay_duration(&self) -> Duration {
        self.max_delay
    }

    /// The in-process wait before parking.
    pub fn max_wait_duration(&self) -> Duration {
        self.max_wait
    }

    /// Whether jobs run unthrottled when Redis fails the check.
    pub fn is_fail_open(&self) -> bool {
        self.fail_open
    }

    /// Whether the counters and wait lines of the throttle's identities are kept.
    pub fn has_stats(&self) -> bool {
        self.stats
    }

    /// The expiry of each identity's counters and wait line after its last use.
    pub fn stats_ttl_duration(&self) -> Duration {
        self.stats_ttl
    }

    /// The definition as specs joined by `;` (`c=25;w=5/1000 per key`), as the heartbeat
    /// advertises it.
    pub fn spec(&self) -> String {
        self.limits
            .iter()
            .map(|(limit, scope)| match scope {
                Scope::Global => limit.normalized(),
                Scope::PerKey => format!("{} per key", limit.normalized()),
            })
            .collect::<Vec<_>>()
            .join(";")
    }
}

/// A reschedule delay policy: given the job, the refusal and the reschedule count, the delay.
pub type Backoff = Arc<dyn Fn(&BackoffInput<'_>) -> Duration + Send + Sync>;

/// What a backoff closure sees.
#[non_exhaustive]
pub struct BackoffInput<'a> {
    /// The job, as fetched.
    pub job: &'a JobPayload,
    /// The refusal.
    pub over: &'a OverLimit,
    /// This reschedule's number in the limiters' budget: 1 the first time, and 1 again after
    /// an execution that made progress before its refusal (an iterable job that processed
    /// items).
    pub reschedules: u32,
}

impl fmt::Debug for BackoffInput<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BackoffInput")
            .field("jid", &self.job.jid)
            .field("over", &self.over)
            .field("reschedules", &self.reschedules)
            .finish()
    }
}

/// The refusal of a limiter (or a refusal of your own: an HTTP 429, a quota error). A job
/// returning it (with `?`, or [`JobError::over_limit`]) is rescheduled instead of failed,
/// within its reschedule budget.
///
/// Its `Display` names the limit: "over the limit `w:stripe:42` (window=100/1s); next slot in
/// 1.234 s".
#[derive(Clone)]
#[non_exhaustive]
pub struct OverLimit {
    label: String,
    limit: Option<Limit>,
    retry_after: Option<Duration>,
    reschedules: Option<u32>,
    max_delay: Option<Duration>,
    backoff: Option<Backoff>,
    place: Option<i64>,
}

impl OverLimit {
    /// A refusal of your own: `label` names the resource.
    pub fn new(label: impl Into<String>) -> OverLimit {
        OverLimit {
            label: label.into(),
            limit: None,
            retry_after: None,
            reschedules: None,
            max_delay: None,
            backoff: None,
            place: None,
        }
    }

    /// When the next slot is expected (the reschedule delay, unless a backoff says otherwise).
    #[must_use]
    pub fn with_retry_after(mut self, after: Duration) -> OverLimit {
        self.retry_after = Some(after);
        self
    }

    /// The reschedule budget for this refusal (default: the process's, 20).
    #[must_use]
    pub fn with_reschedules(mut self, max: u32) -> OverLimit {
        self.reschedules = Some(max);
        self
    }

    /// The limit that refused (for its `Display`).
    #[must_use]
    pub(crate) fn with_limit(mut self, limit: Limit) -> OverLimit {
        self.limit = Some(limit);
        self
    }

    /// The longest reschedule delay for this refusal.
    #[must_use]
    pub(crate) fn with_max_delay(mut self, delay: Duration) -> OverLimit {
        self.max_delay = Some(delay);
        self
    }

    /// The delay policy of the limiter that refused.
    #[must_use]
    pub(crate) fn with_backoff(mut self, backoff: Backoff) -> OverLimit {
        self.backoff = Some(backoff);
        self
    }

    /// The slot (ms, Redis's clock) of the place the refused caller holds in the limit's wait
    /// line: its `retry_after` leads there, and a job rescheduled for it comes back then.
    #[must_use]
    pub(crate) fn with_place(mut self, slot: i64) -> OverLimit {
        self.place = Some(slot);
        self
    }

    /// The limit's label (`w:stripe:42`), or the resource named by [`OverLimit::new`].
    pub fn label(&self) -> &str {
        &self.label
    }

    /// When the next slot is expected.
    pub fn retry_after(&self) -> Option<Duration> {
        self.retry_after
    }

    /// The reschedule budget of this refusal, when it has its own.
    pub fn reschedules(&self) -> Option<u32> {
        self.reschedules
    }

    /// The longest reschedule delay of this refusal, when it has its own.
    pub(crate) fn max_delay(&self) -> Option<Duration> {
        self.max_delay
    }

    /// The delay policy of the limiter that refused, if it has one.
    pub(crate) fn backoff(&self) -> Option<&Backoff> {
        self.backoff.as_ref()
    }

    /// The slot of the refused caller's place in the wait line, when it holds one.
    pub(crate) fn place(&self) -> Option<i64> {
        self.place
    }
}

impl fmt::Debug for OverLimit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OverLimit")
            .field("label", &self.label)
            .field("limit", &self.limit)
            .field("retry_after", &self.retry_after)
            .field("reschedules", &self.reschedules)
            .field("max_delay", &self.max_delay)
            .field("backoff", &self.backoff.is_some())
            .field("place", &self.place)
            .finish()
    }
}

impl fmt::Display for OverLimit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "over the limit `{}`", self.label)?;
        if let Some(limit) = &self.limit {
            write!(f, " ({limit})")?;
        }
        if let Some(after) = self.retry_after {
            write!(f, "; next slot in {:.3} s", after.as_secs_f64())?;
        }
        Ok(())
    }
}

impl std::error::Error for OverLimit {}

/// Current use of one limit identity (`Limiter::usage`, `api::Limits`).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Usage {
    /// The identity (`w:stripe:42`).
    pub identity: String,
    /// The limit.
    pub limit: Limit,
    /// Units in use (leaky and points: the fractional level).
    pub used: f64,
    /// Concurrency: the earliest lease end (a worst case).
    pub next_free_in: Option<Duration>,
}

/// The `throttle` option of a job: the same type in [`JobOptions`](crate::JobOptions) and in
/// [`JobPayload`] (stored as `{"name", "key"}`).
///
/// Layers merge like [`UniqueOption`](crate::unique::UniqueOption): a higher
/// [`Disabled`](Self::Disabled) wins; two [`Enabled`](Self::Enabled) merge per field; a higher
/// `Enabled` over a lower `Disabled` is the higher one alone.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ThrottleOption {
    /// Not throttled, whatever lower layers say. JSON `false`.
    Disabled,
    /// Throttled by `name` (required by the time of the push), per `key` when set.
    Enabled {
        /// The throttle's name.
        name: Option<String>,
        /// The throttle key (tenant, account): per-key limits apply to it.
        key: Option<String>,
    },
}

impl ThrottleOption {
    /// Throttled by `name`.
    pub fn named(name: impl Into<String>) -> Self {
        ThrottleOption::Enabled {
            name: Some(name.into()),
            key: None,
        }
    }

    /// `upper` over `self` (see the merge rules above).
    #[must_use]
    pub fn merge(&self, upper: &ThrottleOption) -> ThrottleOption {
        match (self, upper) {
            (_, ThrottleOption::Disabled) => ThrottleOption::Disabled,
            (
                ThrottleOption::Enabled { name, key },
                ThrottleOption::Enabled {
                    name: upper_name,
                    key: upper_key,
                },
            ) => ThrottleOption::Enabled {
                name: upper_name.clone().or_else(|| name.clone()),
                key: upper_key.clone().or_else(|| key.clone()),
            },
            (ThrottleOption::Disabled, enabled) => enabled.clone(),
        }
    }

    /// The throttle's name, when enabled with one.
    pub fn name(&self) -> Option<&str> {
        match self {
            ThrottleOption::Enabled { name, .. } => name.as_deref(),
            ThrottleOption::Disabled => None,
        }
    }

    /// The throttle key, when enabled with one.
    pub fn key(&self) -> Option<&str> {
        match self {
            ThrottleOption::Enabled { key, .. } => key.as_deref(),
            ThrottleOption::Disabled => None,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct ThrottleRepr {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    key: Option<String>,
}

impl Serialize for ThrottleOption {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        match self {
            ThrottleOption::Disabled => serializer.serialize_bool(false),
            ThrottleOption::Enabled { name, key } => ThrottleRepr {
                name: name.clone(),
                key: key.clone(),
            }
            .serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for ThrottleOption {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        match Value::deserialize(deserializer)? {
            Value::Bool(false) => Ok(ThrottleOption::Disabled),
            Value::Object(object) => {
                let repr =
                    ThrottleRepr::deserialize(Value::Object(object)).map_err(D::Error::custom)?;
                Ok(ThrottleOption::Enabled {
                    name: repr.name,
                    key: repr.key,
                })
            }
            other => Err(D::Error::custom(format!(
                "`throttle` must be `false` or an object, not {other}"
            ))),
        }
    }
}

/// The throttle a job names, resolved: its name and key (`JobRecord::throttle`, the gate).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ThrottleRef {
    /// The throttle's name.
    pub name: String,
    /// The throttle key.
    pub key: Option<String>,
}

impl ThrottleRef {
    /// The throttle of `job` (as fetched): `None` unless its `throttle` field names one.
    pub(crate) fn from_payload(job: &JobPayload) -> Option<Self> {
        match job.throttle.as_ref()? {
            ThrottleOption::Enabled {
                name: Some(name),
                key,
            } => Some(Self {
                name: name.clone(),
                key: key.clone(),
            }),
            _ => None,
        }
    }
}

/// The `limited` field of a job the rate limiter rescheduled (`{"n", "by", "at", "t", "o",
/// "s"}`): `n` reschedules so far (never reset), the label of the limit that refused last, and
/// when (epoch milliseconds, the worker's clock); then the two budgets' counters, kept apart so
/// that waiting for a throttle never spends a limiter's budget and the other way round:
/// `t` parkings by throttles (never reset) and `o` limiter reschedules since the job last made
/// progress; and `s`, the job's place in the wait line of `by`, passed back when it returns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Limited {
    /// Reschedules so far: parkings by throttles and limiter reschedules together. Never
    /// reset.
    pub n: u32,
    /// The label of the limit that refused last (`w:stripe:42`).
    pub by: String,
    /// When, in epoch milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at: Option<i64>,
    /// Parkings by throttles so far, which a throttle's
    /// [`reschedules`](Throttle::reschedules) budget counts (JSON `t`). Never reset.
    #[serde(default, rename = "t", skip_serializing_if = "is_zero")]
    pub throttled: u32,
    /// Limiter reschedules since the job last made progress, which a limiter's budget counts
    /// ([`OverLimit::with_reschedules`], `HenchBuilder::over_limit_reschedules`; JSON `o`): an
    /// execution that made progress before its refusal (an iterable job that processed items)
    /// starts it again from 1.
    #[serde(default, rename = "o", skip_serializing_if = "is_zero")]
    pub over_limit: u32,
    /// The slot (epoch milliseconds, Redis's clock) of the job's place in the wait line of the
    /// limit `by` (JSON `s`): when the job comes back for it, it keeps its place instead of
    /// queueing again behind the callers that came meanwhile.
    #[serde(default, rename = "s", skip_serializing_if = "Option::is_none")]
    pub slot: Option<i64>,
}

/// For `skip_serializing_if`.
#[allow(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde passes a reference"
)]
fn is_zero(value: &u32) -> bool {
    *value == 0
}

impl Limited {
    /// The field after reschedule number `n`, by `by`, at `at` (epoch milliseconds).
    pub fn new(n: u32, by: impl Into<String>, at: Option<i64>) -> Self {
        Self {
            n,
            by: by.into(),
            at,
            throttled: 0,
            over_limit: 0,
            slot: None,
        }
    }

    /// The place `job` holds in the wait line of one of `members` (limit identities such as
    /// `c:stripe:42`): its slot, when the limit that refused it last is one of them.
    pub(crate) fn ticket_for(job: &JobPayload, members: &[&str]) -> Option<i64> {
        let limited = job.limited.as_ref()?;
        members
            .contains(&limited.by.as_str())
            .then_some(limited.slot)
            .flatten()
    }
}

/// A decided reschedule: the job goes to the schedule in `delay`, with `limited` set to
/// `limited` (crate-internal; `docs/design/ratelimit.md` §2.5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Reschedule {
    /// How long the job waits.
    pub(crate) delay: Duration,
    /// The label of the limit that refused.
    pub(crate) label: String,
    /// The reschedule's number (the job's `limited.n` after it).
    pub(crate) n: u32,
    /// The job's `limited` after it (`at` is set when it is stored).
    pub(crate) limited: Limited,
}

impl Reschedule {
    /// A parking of `job` (as fetched) by its throttle, refused by `label`, for `delay`, its
    /// place in the line at `slot`: `n` and `t` count it, `o` is kept.
    pub(crate) fn throttled(
        job: &JobPayload,
        label: impl Into<String>,
        delay: Duration,
        slot: Option<i64>,
    ) -> Self {
        let before = job.limited.as_ref();
        let label = label.into();
        let mut limited = Limited::new(
            before.map_or(0, |limited| limited.n).saturating_add(1),
            label.clone(),
            None,
        );
        limited.throttled = before
            .map_or(0, |limited| limited.throttled)
            .saturating_add(1);
        limited.over_limit = before.map_or(0, |limited| limited.over_limit);
        limited.slot = slot;
        Self {
            delay,
            label,
            n: limited.n,
            limited,
        }
    }

    /// A reschedule of `job` (as fetched) refused by a limiter while it ran, number `run` in the
    /// limiter's budget (see [`Limited::over_limit`]), for `delay`, its place in the line at
    /// `slot`: `n` counts it, `t` is kept.
    pub(crate) fn over_limit(
        job: &JobPayload,
        label: impl Into<String>,
        delay: Duration,
        run: u32,
        slot: Option<i64>,
    ) -> Self {
        let before = job.limited.as_ref();
        let label = label.into();
        let mut limited = Limited::new(
            before.map_or(0, |limited| limited.n).saturating_add(1),
            label.clone(),
            None,
        );
        limited.throttled = before.map_or(0, |limited| limited.throttled);
        limited.over_limit = run;
        limited.slot = slot;
        Self {
            delay,
            label,
            n: limited.n,
            limited,
        }
    }

    /// `job` (as fetched) stored for the reschedule: `limited` set, scored `now + delay`
    /// (`now` in epoch seconds, `now_ms` in milliseconds).
    pub(crate) fn stored(&self, mut job: JobPayload, now: f64, now_ms: i64) -> Result<Stored> {
        let mut limited = self.limited.clone();
        limited.at = Some(now_ms);
        job.limited = Some(limited);
        // At least a millisecond ahead, so that the job is scheduled rather than queued.
        job.at = Some(now + self.delay.as_secs_f64().max(0.001));
        Stored::encode(job, now, now_ms)
    }
}

/// What the processor does with a job error that is a limiter's refusal (`limit::engine::over_limit`).
#[derive(Debug)]
pub(crate) enum OverLimitDecision {
    /// Within the budget: reschedule.
    Reschedule(Reschedule),
    /// Past the budget: the failure path, with this error (class `hench::OverLimit`).
    Exhausted(JobError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn kinds_and_letters() {
        assert_eq!(
            Limit::Concurrency { limit: 3 }.kind(),
            LimitKind::Concurrency
        );
        let points = Limit::Points {
            capacity: 1_000,
            refill: Duration::from_secs(60),
        };
        assert_eq!(points.kind(), LimitKind::Leaky);
        assert_eq!(points.capacity(), 1_000);
        for kind in [
            LimitKind::Concurrency,
            LimitKind::Window,
            LimitKind::Bucket,
            LimitKind::Leaky,
        ] {
            assert_eq!(LimitKind::from_letter(kind.letter()), Some(kind));
        }
        assert_eq!(LimitKind::from_letter('l'), None);
        assert_eq!(Interval::Day.duration(), Duration::from_secs(86_400));
    }

    #[test]
    fn over_limit_refusals_describe_themselves() {
        let over = OverLimit::new("w:stripe:42")
            .with_limit(Limit::Window {
                limit: 100,
                period: Duration::from_secs(1),
            })
            .with_retry_after(Duration::from_millis(1_234))
            .with_reschedules(3);
        assert_eq!(
            over.to_string(),
            "over the limit `w:stripe:42` (window=100/1s); next slot in 1.234 s"
        );
        assert_eq!(over.label(), "w:stripe:42");
        assert_eq!(over.reschedules(), Some(3));
        assert_eq!(
            OverLimit::new("quota").to_string(),
            "over the limit `quota`"
        );
    }

    #[test]
    fn throttle_options_merge_and_round_trip() {
        let lower = ThrottleOption::named("stripe");
        let upper = ThrottleOption::Enabled {
            name: None,
            key: Some("42".to_owned()),
        };
        let merged = lower.merge(&upper);
        assert_eq!((merged.name(), merged.key()), (Some("stripe"), Some("42")));
        assert_eq!(
            merged.merge(&ThrottleOption::Disabled),
            ThrottleOption::Disabled
        );
        assert_eq!(ThrottleOption::Disabled.merge(&upper), upper);
        let value = serde_json::to_value(&merged).unwrap();
        assert_eq!(value, json!({"name": "stripe", "key": "42"}));
        assert_eq!(
            serde_json::from_value::<ThrottleOption>(value).unwrap(),
            merged
        );
        assert_eq!(
            serde_json::from_value::<ThrottleOption>(json!(false)).unwrap(),
            ThrottleOption::Disabled
        );
        assert!(serde_json::from_value::<ThrottleOption>(json!(true)).is_err());
        let mut job = JobPayload::new("Charge", vec![]);
        job.throttle = Some(merged);
        assert_eq!(
            ThrottleRef::from_payload(&job),
            Some(ThrottleRef {
                name: "stripe".to_owned(),
                key: Some("42".to_owned())
            })
        );
        job.throttle = Some(ThrottleOption::Enabled {
            name: None,
            key: None,
        });
        assert_eq!(ThrottleRef::from_payload(&job), None);
    }

    #[test]
    fn reschedules_store_the_job_in_the_schedule() {
        let mut job = JobPayload::new("Charge", vec![json!(1)]);
        job.jid = "a".repeat(24);
        job.queue = "billing".to_owned();
        job.limited = Some(Limited::new(2, "c:stripe", None));
        let reschedule =
            Reschedule::over_limit(&job, "w:stripe:42", Duration::from_secs(12), 1, None);
        assert_eq!(reschedule.n, 3);
        let Stored::Scheduled { at, json } = reschedule.stored(job, 1_000.0, 1_000_000).unwrap()
        else {
            panic!("a reschedule goes to the schedule");
        };
        assert!((at - 1_012.0).abs() < 1e-9);
        let stored: JobPayload = serde_json::from_str(&json).unwrap();
        let mut expected = Limited::new(3, "w:stripe:42", Some(1_000_000));
        expected.over_limit = 1;
        assert_eq!(stored.limited, Some(expected));
        assert_eq!(stored.at, None);
        let value: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(
            value["limited"],
            json!({"n": 3, "by": "w:stripe:42", "at": 1_000_000, "o": 1})
        );
    }

    #[test]
    fn throttle_parkings_and_limiter_reschedules_count_apart() {
        let mut job = JobPayload::new("Charge", vec![]);
        // Parked twice by its throttle, at its place in the line.
        for _ in 0..2 {
            let parked =
                Reschedule::throttled(&job, "c:stripe:42", Duration::from_secs(1), Some(5_000));
            job.limited = Some(parked.limited);
        }
        let limited = job.limited.clone().unwrap();
        assert_eq!(
            (
                limited.n,
                limited.throttled,
                limited.over_limit,
                limited.slot
            ),
            (2, 2, 0, Some(5_000))
        );
        assert_eq!(Limited::ticket_for(&job, &["c:stripe:42"]), Some(5_000));
        assert_eq!(Limited::ticket_for(&job, &["c:other"]), None);
        // Then rescheduled by a limiter: its own count, the throttle's kept.
        let rescheduled =
            Reschedule::over_limit(&job, "w:api", Duration::from_secs(1), 1, Some(9_000));
        let limited = rescheduled.limited;
        assert_eq!(
            (
                limited.n,
                limited.throttled,
                limited.over_limit,
                limited.slot
            ),
            (3, 2, 1, Some(9_000))
        );
        // The JSON keeps the short keys; old payloads read as zero.
        let value = serde_json::to_value(&limited).unwrap();
        assert_eq!(
            value,
            json!({"n": 3, "by": "w:api", "t": 2, "o": 1, "s": 9_000})
        );
        let old: Limited = serde_json::from_value(json!({"n": 7, "by": "w:x"})).unwrap();
        assert_eq!((old.throttled, old.over_limit, old.slot), (0, 0, None));
    }
}
