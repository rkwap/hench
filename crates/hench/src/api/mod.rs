//! Read and manage the data hench keeps in Redis (Sidekiq's `api.rb`): statistics, queues,
//! the scheduled, retry and dead sorted sets, processes and the jobs they are running, and the
//! state of the features: unique locks ([`Api::unique_locks`]), periodic schedules
//! ([`Api::periodic`], feature `periodic`), batches ([`Api::batches`]) and limits
//! ([`Api::limits`]). This is what dashboards and admin tools are built on; workers never need
//! it.
//!
//! Everything starts from an [`Api`] handle, created from a [`RedisPool`], a [`Client`] or a
//! [`Hench`]:
//!
//! ```no_run
//! use hench::api::{Api, SortOrder};
//! use hench::RedisPool;
//!
//! # async fn demo(redis: RedisPool) -> hench::Result<()> {
//! let api = Api::new(redis);
//!
//! let stats = api.stats().await?;
//! println!("{} processed, {} failed, {} enqueued", stats.processed, stats.failed, stats.enqueued);
//!
//! for queue in api.queues().await? {
//!     println!("{}: {} jobs, {:.1}s latency", queue.name(), queue.size().await?, queue.latency().await?);
//! }
//!
//! // The first page of retries, earliest first; retry the ones that failed with a timeout.
//! let page = api.retries().page(1, 25, SortOrder::Ascending).await?;
//! for entry in page.items {
//!     if entry.error_class() == Some("timeout") {
//!         entry.retry().await?;
//!     }
//! }
//!
//! // Quiet every process (they stop fetching new jobs).
//! for process in api.processes().list().await? {
//!     process.quiet().await?;
//! }
//! # Ok(()) }
//! ```
//!
//! Iterating over a whole collection ([`Queue::jobs`], [`JobSet::entries`], [`JobSet::scan`])
//! gives a [`Stream`]: pin it, then read it with `StreamExt::next` from the
//! [`futures`](https://docs.rs/futures) crate (`futures = "0.3"` in your dependencies):
//!
//! ```no_run
//! use futures::StreamExt;
//! use hench::api::Api;
//!
//! # async fn demo(api: Api) -> hench::Result<()> {
//! let mut jobs = std::pin::pin!(api.queue("default").jobs());
//! while let Some(job) = jobs.next().await {
//!     let job = job?;
//!     println!("{:?} {:?}", job.jid(), job.display_class());
//! }
//! let mut dead = std::pin::pin!(api.dead().scan("ImportFeed"));
//! while let Some(entry) = dead.next().await {
//!     entry?.retry().await?;
//! }
//! # Ok(()) }
//! ```
//!
//! The data is live: it changes between (and during) calls. Pages, iteration and searches
//! read at most 1000 entries per round trip and use the `SCAN` family, never `KEYS`;
//! summaries of processes and queues pipeline one command per process or queue. Searches
//! (`find_job`, `scan`) and bulk operations (`retry_all`, `kill_all`, `clear`) touch every
//! entry of their collection, so they are for admin tools, not application logic.

pub(crate) mod events;
mod page;
mod process;
mod queue;
mod record;
mod sorted;
mod stats;
mod work;

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::{self, Stream, TryStreamExt};
use serde_json::Value;

use crate::client::Client;
use crate::config::{Config, Hench};
use crate::error::{JobError, Result};
use crate::payload::{JobPayload, millis_to_system_time, now_millis};
use crate::redis::RedisPool;

pub use self::page::{Page, RawEntry, SortOrder, page_items, page_key};
pub use self::process::{CapsuleInfo, Process, ProcessSet};
pub(crate) use self::process::{cleanup_processes, live_process_count};
pub use self::queue::Queue;
pub use self::record::JobRecord;
#[cfg(test)]
pub(crate) use self::sorted::MOVE_LOWEST;
pub use self::sorted::{DeadSet, JobSet, RetrySet, ScheduledSet, SortedEntry, parse_key};
pub(crate) use self::sorted::{MOVE_OUT, MOVE_TO_MORGUE, lock_to_extend, rescore_holding};
#[cfg(any(test, feature = "test-util"))]
pub(crate) use self::sorted::{RESCORE_UNIQUE, SCAN_CONTAINING};
pub use self::stats::{QueueSummary, StatCounter, Stats, StatsHistory};
pub use self::work::{Work, WorkSet};
// The API types of the features (each also in its feature's module).
pub use crate::batch::{
    BatchEvents, BatchSearch, BatchStatus, BatchSummary, Batches, CallbackInfo, FailedMember,
};
pub use crate::limit::{Holder, LimitEvents, LimitInfo, Limits};
#[cfg(feature = "periodic")]
pub use crate::periodic::{
    CrontabGroup, HistoryEntry, LastEnqueue, LastRun, PeriodicEntry, PeriodicSet, PeriodicSummary,
};
pub use crate::unique::{HolderPlace, UniqueEventCounts, UniqueEvents, UniqueLock, UniqueLocks};

/// A handle to the hench data in one Redis, shared by every API type. Cheap to clone.
///
/// Besides the connection it holds what operations that move jobs need:
///
/// * the [`Client`] used to push jobs back to their queues ([`SortedEntry::retry`],
///   [`SortedEntry::add_to_queue`], [`JobSet::retry_all`]), so client middleware runs as for
///   any other push;
/// * the size and age limits of the morgue ([`DeadSet::trim`]), by default the
///   configuration defaults (10 000 jobs, 180 days);
/// * optionally a process [`Config`] whose death handlers run when a job is killed with
///   notification ([`DeadSet::kill`]).
///
/// [`Api::from_hench`] takes all of them from a configured [`Hench`].
#[derive(Clone)]
pub struct Api {
    inner: Arc<ApiInner>,
}

#[derive(Clone)]
struct ApiInner {
    client: Client,
    dead_max_jobs: usize,
    dead_timeout: Duration,
    config: Option<Arc<Config>>,
}

impl Api {
    /// An API on `redis`, pushing jobs with a plain client (no middleware) and using the
    /// default morgue limits.
    pub fn new(redis: RedisPool) -> Self {
        Self::from_client(Client::new(redis))
    }

    /// An API on the client's Redis, pushing jobs with `client` (and its middleware).
    pub fn from_client(client: Client) -> Self {
        Self {
            inner: Arc::new(ApiInner {
                client,
                dead_max_jobs: Config::DEFAULT_DEAD_MAX_JOBS,
                dead_timeout: Config::DEFAULT_DEAD_TIMEOUT,
                config: None,
            }),
        }
    }

    /// An API using the client, morgue limits and death handlers of a configured [`Hench`].
    pub fn from_hench(hench: &Hench) -> Self {
        Self::from_client(hench.client().clone()).with_config(Arc::clone(hench.config()))
    }

    /// A copy using the morgue limits and death handlers of `config`.
    #[must_use]
    pub fn with_config(&self, config: Arc<Config>) -> Self {
        let mut inner = ApiInner::clone(&self.inner);
        inner.dead_max_jobs = config.dead_max_jobs.max(1);
        inner.dead_timeout = config.dead_timeout;
        inner.config = Some(config);
        Self {
            inner: Arc::new(inner),
        }
    }

    /// A copy with other morgue limits, applied by [`DeadSet::trim`] whenever jobs are killed:
    /// none older than `timeout`, and, like Sidekiq (and the workers' morgue), only the newest
    /// `max_jobs - 1` jobs are kept. `max_jobs` is at least 1 (which keeps no job).
    #[must_use]
    pub fn with_dead_limits(&self, max_jobs: usize, timeout: Duration) -> Self {
        let mut inner = ApiInner::clone(&self.inner);
        inner.dead_max_jobs = max_jobs.max(1);
        inner.dead_timeout = timeout;
        Self {
            inner: Arc::new(inner),
        }
    }

    /// The Redis pool.
    pub fn redis(&self) -> &RedisPool {
        self.inner.client.redis()
    }

    /// The client used to push jobs.
    pub fn client(&self) -> &Client {
        &self.inner.client
    }

    /// The morgue's size limit.
    pub fn dead_max_jobs(&self) -> usize {
        self.inner.dead_max_jobs
    }

    /// The morgue's age limit.
    pub fn dead_timeout(&self) -> Duration {
        self.inner.dead_timeout
    }

    /// Current statistics, see [`Stats::fetch`].
    pub async fn stats(&self) -> Result<Stats> {
        Stats::fetch(self).await
    }

    /// The queue called `name` (which may not exist yet).
    pub fn queue(&self, name: impl Into<String>) -> Queue {
        Queue::new(self, name)
    }

    /// Every known queue, sorted by name, see [`Queue::all`].
    pub async fn queues(&self) -> Result<Vec<Queue>> {
        Queue::all(self).await
    }

    /// Jobs scheduled to run later.
    pub fn scheduled(&self) -> ScheduledSet {
        ScheduledSet::new(self)
    }

    /// Jobs waiting to be retried.
    pub fn retries(&self) -> RetrySet {
        RetrySet::new(self)
    }

    /// The morgue: jobs whose retries were exhausted.
    pub fn dead(&self) -> DeadSet {
        DeadSet::new(self)
    }

    /// The live processes.
    pub fn processes(&self) -> ProcessSet {
        ProcessSet::new(self)
    }

    /// The jobs being run right now.
    pub fn work(&self) -> WorkSet {
        WorkSet::new(self)
    }

    /// The fields of Redis's `INFO` reply (`redis_version`, `uptime_in_days`,
    /// `connected_clients`, `used_memory_human`, ...), for display. Section headers and
    /// comments are skipped. Fails if the server does not allow `INFO`.
    pub async fn redis_info(&self) -> Result<BTreeMap<String, String>> {
        let info: String = self
            .redis()
            .with_retry(|mut conn| async move { ::redis::cmd("INFO").query_async(&mut conn).await })
            .await?;
        Ok(parse_info(&info))
    }

    /// The configuration this API was given ([`Api::with_config`], [`Api::from_hench`]).
    #[cfg(feature = "periodic")]
    pub(crate) fn config(&self) -> Option<&Arc<Config>> {
        self.inner.config.as_ref()
    }

    /// Runs the death handlers of the configuration, if any, for a killed job.
    fn notify_death(&self, job: &JobPayload, error: &JobError) {
        if let Some(config) = &self.inner.config {
            config.handle_death(job, error);
        }
    }
}

impl fmt::Debug for Api {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Api")
            .field("redis", self.redis())
            .field("dead_max_jobs", &self.inner.dead_max_jobs)
            .field("dead_timeout", &self.inner.dead_timeout)
            .field("death_handlers", &self.inner.config.is_some())
            .finish_non_exhaustive()
    }
}

impl From<RedisPool> for Api {
    fn from(redis: RedisPool) -> Self {
        Self::new(redis)
    }
}

impl From<&RedisPool> for Api {
    fn from(redis: &RedisPool) -> Self {
        Self::new(redis.clone())
    }
}

impl From<Client> for Api {
    fn from(client: Client) -> Self {
        Self::from_client(client)
    }
}

impl From<&Client> for Api {
    fn from(client: &Client) -> Self {
        Self::from_client(client.clone())
    }
}

impl From<&Hench> for Api {
    fn from(hench: &Hench) -> Self {
        Self::from_hench(hench)
    }
}

impl From<&Api> for Api {
    fn from(api: &Api) -> Self {
        api.clone()
    }
}

/// Parses the `field:value` lines of an `INFO` reply.
fn parse_info(info: &str) -> BTreeMap<String, String> {
    info.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .filter_map(|line| line.split_once(':'))
        .map(|(field, value)| (field.to_owned(), value.to_owned()))
        .collect()
}

/// Floats below this are seconds since the epoch (older Sidekiq payloads), larger numbers are
/// milliseconds; the same rule as the payload parser.
const FLOAT_SECONDS_LIMIT: f64 = 1e11;

/// Epoch milliseconds of a payload timestamp: an integer (milliseconds) or a float (seconds
/// when below 1e11, as written by older Sidekiq versions, else milliseconds).
pub(crate) fn timestamp_millis(value: &Value) -> Option<i64> {
    if let Some(millis) = value.as_i64() {
        return Some(millis);
    }
    if let Some(millis) = value.as_u64() {
        return Some(i64::try_from(millis).unwrap_or(i64::MAX));
    }
    let float = value.as_f64().filter(|float| float.is_finite())?;
    let millis = if float.abs() < FLOAT_SECONDS_LIMIT {
        (float * 1000.0).round()
    } else {
        float.trunc()
    };
    // `as` saturates for out-of-range floats.
    Some(millis as i64)
}

/// Epoch seconds of a time written by a process (heartbeat `started_at`, work `run_at`):
/// seconds as an integer or a float, or milliseconds for values of 1e11 and more.
pub(crate) fn process_time_secs(value: &Value) -> Option<f64> {
    let secs = value.as_f64().filter(|secs| secs.is_finite())?;
    Some(if secs.abs() >= FLOAT_SECONDS_LIMIT {
        secs / 1000.0
    } else {
        secs
    })
}

/// Seconds since the job was enqueued (or created, if it never was): Sidekiq's
/// `calculate_latency`. `0.0` without a timestamp; never negative (clock skew).
pub(crate) fn latency_of(item: &serde_json::Map<String, Value>) -> f64 {
    let timestamp = item
        .get("enqueued_at")
        .and_then(timestamp_millis)
        .or_else(|| item.get("created_at").and_then(timestamp_millis));
    match timestamp {
        Some(millis) => (now_millis().saturating_sub(millis) as f64 / 1000.0).max(0.0),
        None => 0.0,
    }
}

/// [`latency_of`] for a raw job (`0.0` if it is not a JSON object).
pub(crate) fn latency_of_raw(json: &str) -> f64 {
    match serde_json::from_str::<serde_json::Map<String, Value>>(json) {
        Ok(item) => latency_of(&item),
        Err(_) => 0.0,
    }
}

/// A payload timestamp as a [`std::time::SystemTime`].
pub(crate) fn timestamp(value: Option<&Value>) -> Option<std::time::SystemTime> {
    value.and_then(timestamp_millis).map(millis_to_system_time)
}

/// Lenient integer parsing of a Redis reply that should hold a counter (Ruby's `to_i`
/// leniency: missing or malformed values count as zero, negative values as zero).
pub(crate) fn parse_count(value: Option<&str>) -> u64 {
    let Some(value) = value.map(str::trim) else {
        return 0;
    };
    if let Ok(count) = value.parse::<u64>() {
        return count;
    }
    match value.parse::<f64>() {
        // `as` saturates; NaN becomes zero.
        Ok(float) if float > 0.0 => float as u64,
        _ => 0,
    }
}

/// `text` as it appears inside a JSON string (`"` and `\\` escaped, ...), to search job JSON
/// for it.
pub(crate) fn json_escaped(text: &str) -> String {
    let quoted = Value::String(text.to_owned()).to_string();
    quoted[1..quoted.len() - 1].to_owned()
}

/// A `SCAN`-family `MATCH` pattern: a pattern containing `*` is used as is (a glob);
/// anything else matches values containing it, with glob metacharacters escaped.
pub(crate) fn match_pattern(pattern: &str) -> String {
    if pattern.contains('*') {
        return pattern.to_owned();
    }
    literal_pattern(pattern)
}

/// A `SCAN`-family `MATCH` pattern matching values that contain `text`, with every glob
/// metacharacter escaped.
pub(crate) fn literal_pattern(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len() + 2);
    escaped.push('*');
    for c in text.chars() {
        if matches!(c, '*' | '?' | '[' | ']' | '\\') {
            escaped.push('\\');
        }
        escaped.push(c);
    }
    escaped.push('*');
    escaped
}

/// A stream that fetches items in batches: `next` returns the next batch and state, or
/// `None` when done.
pub(crate) fn batched<S, T, F, Fut>(state: S, next: F) -> impl Stream<Item = Result<T>> + Send
where
    S: Send,
    T: Send,
    F: FnMut(S) -> Fut + Send,
    Fut: Future<Output = Result<Option<(Vec<T>, S)>>> + Send,
{
    stream::try_unfold(state, next)
        .map_ok(|batch| stream::iter(batch.into_iter().map(Ok)))
        .try_flatten()
}

/// The job behind a stored value that the API removes, when its removal concerns a feature: a
/// batch member (it counts as dead in its batch) or a unique job (its lock is released). `None`
/// for every other job, which is not even parsed: its removal stays exactly what it was.
pub(crate) fn featured_job(value: &str) -> Option<JobPayload> {
    if !(value.contains("\"bid\"") || value.contains("\"unique\"")) {
        return None;
    }
    let job = JobPayload::from_json(value).ok()?;
    (job.bid.is_some() || job.unique.is_some()).then_some(job)
}

/// The batch accounting of jobs the API removes (`featured_job`s), before their removal.
pub(crate) async fn before_removal(
    api: &Api,
    jobs: &[JobPayload],
    removal: crate::batch::api::Removal,
) -> Vec<crate::batch::Report> {
    if jobs.is_empty() {
        return Vec::new();
    }
    crate::batch::api::before_removal(api.redis(), jobs, removal).await
}

/// The dispatch of the batch events the accounting of removed jobs recorded.
pub(crate) async fn after_removal(api: &Api, reports: Vec<crate::batch::Report>) {
    if !reports.is_empty() {
        crate::batch::api::after_removal(api.redis(), reports).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn timestamps_accept_every_representation() {
        assert_eq!(
            timestamp_millis(&json!(1_700_000_000_123_i64)),
            Some(1_700_000_000_123)
        );
        assert_eq!(
            timestamp_millis(&json!(1_700_000_000.5)),
            Some(1_700_000_000_500)
        );
        assert_eq!(
            timestamp_millis(&json!(1_700_000_000_123.0)),
            Some(1_700_000_000_123)
        );
        assert_eq!(timestamp_millis(&json!(u64::MAX)), Some(i64::MAX));
        assert_eq!(timestamp_millis(&json!("1700000000")), None);
        assert_eq!(timestamp_millis(&Value::Null), None);
        assert!(timestamp(Some(&json!(0))).is_some());
        assert!(timestamp(None).is_none());
    }

    #[test]
    fn process_times_are_seconds_or_milliseconds() {
        assert_eq!(
            process_time_secs(&json!(1_700_000_000)),
            Some(1_700_000_000.0)
        );
        assert_eq!(
            process_time_secs(&json!(1_700_000_000.5)),
            Some(1_700_000_000.5)
        );
        assert_eq!(
            process_time_secs(&json!(1_700_000_000_500_i64)),
            Some(1_700_000_000.5)
        );
        assert_eq!(process_time_secs(&json!("soon")), None);
    }

    #[test]
    fn latency_prefers_enqueued_at_then_created_at() {
        let now = now_millis();
        let item = |value: Value| value.as_object().cloned().unwrap();
        assert_eq!(latency_of(&item(json!({}))), 0.0);
        let latency = latency_of(&item(json!({"enqueued_at": now - 5_000})));
        assert!((latency - 5.0).abs() < 0.5, "{latency}");
        let seconds = now as f64 / 1000.0 - 5.0;
        let latency = latency_of(&item(json!({"enqueued_at": seconds})));
        assert!((latency - 5.0).abs() < 0.5, "{latency}");
        let latency = latency_of(&item(
            json!({"enqueued_at": now - 2_000, "created_at": now - 9_000}),
        ));
        assert!((latency - 2.0).abs() < 0.5, "{latency}");
        let latency = latency_of(&item(json!({"created_at": now - 3_000})));
        assert!((latency - 3.0).abs() < 0.5, "{latency}");
        let latency = latency_of(&item(
            json!({"enqueued_at": null, "created_at": now - 3_000}),
        ));
        assert!((latency - 3.0).abs() < 0.5, "{latency}");
        let latency = latency_of(&item(
            json!({"enqueued_at": "2024-01-01", "created_at": now - 3_000}),
        ));
        assert!((latency - 3.0).abs() < 0.5, "{latency}");
        // Clock skew never yields a negative latency.
        assert_eq!(latency_of(&item(json!({"enqueued_at": now + 60_000}))), 0.0);
        assert_eq!(latency_of_raw("not json"), 0.0);
        assert_eq!(latency_of_raw("[1]"), 0.0);
    }

    #[test]
    fn counts_are_lenient() {
        assert_eq!(parse_count(Some("42")), 42);
        assert_eq!(parse_count(Some(" 7 ")), 7);
        assert_eq!(parse_count(Some("3.9")), 3);
        assert_eq!(parse_count(Some("-4")), 0);
        assert_eq!(parse_count(Some("garbage")), 0);
        assert_eq!(parse_count(None), 0);
    }

    #[test]
    fn info_replies_are_parsed() {
        let info = "# Server\r\nredis_version:7.2.4\r\nredis_mode:standalone\r\n\r\n# Memory\r\nused_memory_human:1.05M\r\nmaxmemory_policy:noeviction\r\nweird line\r\n";
        let parsed = parse_info(info);
        assert_eq!(parsed["redis_version"], "7.2.4");
        assert_eq!(parsed["used_memory_human"], "1.05M");
        assert_eq!(parsed.len(), 4);
    }

    #[test]
    fn jids_are_searched_as_written_in_json() {
        assert_eq!(json_escaped("abc"), "abc");
        assert_eq!(json_escaped("a\\b\"c"), "a\\\\b\\\"c");
        assert_eq!(json_escaped(""), "");
    }

    #[test]
    fn match_patterns_escape_substrings() {
        assert_eq!(match_pattern("mike"), "*mike*");
        assert_eq!(match_pattern("*test*"), "*test*");
        assert_eq!(match_pattern("a?b[c]\\"), "*a\\?b\\[c\\]\\\\*");
        assert_eq!(match_pattern(""), "**");
        assert_eq!(literal_pattern("a*b"), "*a\\*b*");
    }

    #[test]
    fn handles_convert_from_pools_and_clients() {
        let redis = RedisPool::detached();
        let api = Api::from(&redis);
        assert!(api.redis().is_detached());
        assert_eq!(api.dead_max_jobs(), Config::DEFAULT_DEAD_MAX_JOBS);
        assert_eq!(api.dead_timeout(), Config::DEFAULT_DEAD_TIMEOUT);
        let limited = api.with_dead_limits(0, Duration::from_secs(5));
        assert_eq!(limited.dead_max_jobs(), 1);
        assert_eq!(limited.dead_timeout(), Duration::from_secs(5));
        assert_eq!(api.dead_max_jobs(), Config::DEFAULT_DEAD_MAX_JOBS);
        let config = Config {
            dead_max_jobs: 3,
            ..Config::default()
        };
        let configured = Api::from(Client::fake()).with_config(Arc::new(config));
        assert_eq!(configured.dead_max_jobs(), 3);
        assert!(configured.client().is_fake());
        assert!(format!("{configured:?}").contains("death_handlers: true"));
        let _ = Api::from(&Api::from(redis.clone()));
        let _ = Api::from(&Client::new(redis));
    }
}
