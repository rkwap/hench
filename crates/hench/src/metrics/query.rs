//! Reading execution metrics back from Redis.

use std::collections::BTreeMap;
use std::time::Duration;

use chrono::{DateTime, NaiveDate, TimeDelta, Utc};

use super::deploy::{DeployMark, fetch_marks};
use super::histogram::{BUCKETS, Histogram};
use crate::error::Result;
use crate::keys;
use crate::redis::RedisPool;

/// Most minute buckets a query reads (eight hours: minute data is kept that long).
pub const MAX_MINUTES: u32 = 8 * 60;

/// Most hours of ten-minute buckets a query reads (three days: that data is kept that long).
pub const MAX_HOURS: u32 = 72;

/// The resolution of a query's series: the two resolutions hench stores metrics at. A new one
/// would change the data kept in Redis, so this enum is exhaustive: matching on it needs no
/// catch-all arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Granularity {
    /// One bucket per minute (periods up to eight hours).
    Minute,
    /// One bucket per ten minutes (longer periods).
    TenMinutes,
}

impl Granularity {
    /// The length of one bucket.
    pub fn stride(self) -> Duration {
        match self {
            Self::Minute => Duration::from_secs(60),
            Self::TenMinutes => Duration::from_secs(600),
        }
    }

    /// The start of the bucket containing `time`.
    pub fn truncate(self, time: DateTime<Utc>) -> DateTime<Utc> {
        let stride = self.stride().as_secs().cast_signed();
        let secs = time.timestamp();
        DateTime::from_timestamp(secs - secs.rem_euclid(stride), 0).unwrap_or(time)
    }

    /// The metrics hash of the bucket containing `time`.
    fn key(self, time: DateTime<Utc>) -> String {
        match self {
            Self::Minute => keys::metrics_minute(time),
            Self::TenMinutes => keys::metrics_ten_minutes(time),
        }
    }

    fn step(self) -> TimeDelta {
        match self {
            Self::Minute => TimeDelta::minutes(1),
            Self::TenMinutes => TimeDelta::minutes(10),
        }
    }
}

/// How far back a query looks. Periods of up to eight hours read minute buckets; longer ones
/// read ten-minute buckets (up to three days).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Period {
    /// The last `n` minutes, one bucket per minute (at most [`MAX_MINUTES`]).
    Minutes(u32),
    /// The last `n` hours, one bucket per ten minutes (at most [`MAX_HOURS`]).
    Hours(u32),
}

impl Period {
    /// The periods offered by the dashboard, with their labels.
    pub const PRESETS: [(&'static str, Period); 7] = [
        ("1h", Period::Minutes(60)),
        ("2h", Period::Minutes(120)),
        ("4h", Period::Minutes(240)),
        ("8h", Period::Minutes(480)),
        ("24h", Period::Hours(24)),
        ("48h", Period::Hours(48)),
        ("72h", Period::Hours(72)),
    ];

    /// The preset labelled `label` (`"1h"`, `"2h"`, `"4h"`, `"8h"`, `"24h"`, `"48h"`, `"72h"`).
    pub fn from_label(label: &str) -> Option<Self> {
        Self::PRESETS
            .iter()
            .find(|(name, _)| *name == label)
            .map(|(_, period)| *period)
    }

    /// A short label: `"1h"` for 60 minutes, `"90m"`, `"24h"`.
    pub fn label(self) -> String {
        match self {
            Self::Minutes(minutes) if minutes % 60 == 0 => format!("{}h", minutes / 60),
            Self::Minutes(minutes) => format!("{minutes}m"),
            Self::Hours(hours) => format!("{hours}h"),
        }
    }

    /// The resolution of the series of this period.
    pub fn granularity(self) -> Granularity {
        match self {
            Self::Minutes(_) => Granularity::Minute,
            Self::Hours(_) => Granularity::TenMinutes,
        }
    }

    /// Number of buckets read, within the limits (at least one).
    pub fn buckets(self) -> u32 {
        match self {
            Self::Minutes(minutes) => minutes.clamp(1, MAX_MINUTES),
            Self::Hours(hours) => hours.clamp(1, MAX_HOURS) * 6,
        }
    }
}

impl Default for Period {
    /// One hour of minute buckets.
    fn default() -> Self {
        Self::Minutes(60)
    }
}

/// Counters of one job class over a bucket or a whole period.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct Counts {
    /// Executions (successful, failed, interrupted, over limit or skipped).
    pub processed: u64,
    /// Failed executions.
    pub failed: u64,
    /// Total milliseconds of the timed executions (successful and interrupted).
    pub ms: u64,
    /// Executions that went over a limiter's limit and were rescheduled (not failures, not
    /// timed).
    pub over_limit: u64,
    /// Jobs a throttle parked before they started (not executions).
    pub throttled: u64,
    /// Members of cancelled batches that were skipped (not timed).
    pub skipped: u64,
}

impl Counts {
    /// Executions that did not fail.
    pub fn succeeded(&self) -> u64 {
        self.processed.saturating_sub(self.failed)
    }

    /// Executions whose duration is recorded: those that did not fail, minus the ones over a
    /// limit and the skipped ones. When those exceed the executions that did not fail (counters
    /// from different sources), the executions that did not fail.
    pub fn timed(&self) -> u64 {
        let succeeded = self.succeeded();
        self.over_limit
            .checked_add(self.skipped)
            .and_then(|untimed| succeeded.checked_sub(untimed))
            .filter(|timed| *timed > 0)
            .unwrap_or(succeeded)
    }

    /// Total execution time in seconds.
    pub fn seconds(&self) -> f64 {
        self.ms as f64 / 1_000.0
    }

    /// Average milliseconds per timed execution (0 when there was none).
    pub fn average_ms(&self) -> f64 {
        match self.timed() {
            0 => 0.0,
            timed => self.ms as f64 / timed as f64,
        }
    }

    fn add(&mut self, metric: &str, value: u64) {
        let counter = match metric {
            keys::METRIC_PROCESSED => &mut self.processed,
            keys::METRIC_FAILED => &mut self.failed,
            keys::METRIC_MS => &mut self.ms,
            keys::METRIC_OVER_LIMIT => &mut self.over_limit,
            keys::METRIC_THROTTLED => &mut self.throttled,
            keys::METRIC_SKIPPED => &mut self.skipped,
            _ => return,
        };
        *counter = counter.saturating_add(value);
    }
}

/// The metrics of one job class over a period.
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct JobMetrics {
    /// Sums over the whole period.
    pub totals: Counts,
    /// Counters per bucket (by bucket start); buckets without data are absent (see
    /// [`QueryResult::buckets`] for every bucket of the period).
    pub series: BTreeMap<DateTime<Utc>, Counts>,
    /// Execution-time histogram per minute bucket. Only [`Query::for_job`] with a period of
    /// minutes fills it; minutes without executions are absent.
    pub histograms: BTreeMap<DateTime<Utc>, Histogram>,
}

impl JobMetrics {
    /// The histogram of the whole period (the sum of [`JobMetrics::histograms`]).
    pub fn histogram(&self) -> Histogram {
        let mut total = Histogram::new();
        for histogram in self.histograms.values() {
            total.merge(histogram);
        }
        total
    }

    fn add(&mut self, bucket: DateTime<Utc>, metric: &str, value: u64) {
        self.totals.add(metric, value);
        self.series.entry(bucket).or_default().add(metric, value);
    }
}

/// How [`QueryResult::sorted`] orders job classes (largest first).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum SortBy {
    /// Total execution time (the dashboard's order).
    #[default]
    TotalTime,
    /// Number of executions.
    Processed,
    /// Number of failures.
    Failed,
    /// Average execution time.
    AverageTime,
}

/// The answer to a metrics [`Query`].
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct QueryResult {
    /// The resolution of the series.
    pub granularity: Granularity,
    /// Start of the period (`ends_at` minus the period).
    pub starts_at: DateTime<Utc>,
    /// End of the period (the time of the query).
    pub ends_at: DateTime<Utc>,
    /// Metrics per job class.
    pub jobs: BTreeMap<String, JobMetrics>,
    /// Deploy marks within the period, oldest first.
    pub marks: Vec<DeployMark>,
}

impl QueryResult {
    /// The start of every bucket of the period, oldest first: the x axis of a chart.
    pub fn buckets(&self) -> Vec<DateTime<Utc>> {
        let step = self.granularity.step();
        let last = self.granularity.truncate(self.ends_at);
        let mut buckets = Vec::new();
        let mut bucket = last;
        while bucket > self.starts_at {
            buckets.push(bucket);
            bucket -= step;
        }
        buckets.reverse();
        buckets
    }

    /// The job classes ordered by `by`, largest first (ties by name).
    pub fn sorted(&self, by: SortBy) -> Vec<(&str, &JobMetrics)> {
        let mut jobs: Vec<(&str, &JobMetrics)> = self
            .jobs
            .iter()
            .map(|(class, metrics)| (class.as_str(), metrics))
            .collect();
        let key = |metrics: &JobMetrics| -> f64 {
            let totals = &metrics.totals;
            match by {
                SortBy::TotalTime => totals.ms as f64,
                SortBy::Processed => totals.processed as f64,
                SortBy::Failed => totals.failed as f64,
                SortBy::AverageTime => totals.average_ms(),
            }
        };
        jobs.sort_by(|(a_class, a), (b_class, b)| {
            key(b).total_cmp(&key(a)).then_with(|| a_class.cmp(b_class))
        });
        jobs
    }
}

/// Reads the execution metrics written by the workers (see
/// [`Tracker`](super::Tracker)): the busiest job classes of a period, or one class in detail.
/// All times are UTC.
///
/// ```no_run
/// use hench::metrics::{Period, Query, SortBy};
///
/// # async fn demo(redis: hench::RedisPool) -> hench::Result<()> {
/// let result = Query::new(&redis).top_jobs(Period::Minutes(60), None).await?;
/// for (class, metrics) in result.sorted(SortBy::TotalTime).into_iter().take(5) {
///     println!("{class}: {} runs, {:.1} ms on average", metrics.totals.processed, metrics.totals.average_ms());
/// }
/// # Ok(()) }
/// ```
#[derive(Debug, Clone)]
pub struct Query {
    redis: RedisPool,
    now: Option<DateTime<Utc>>,
}

impl Query {
    /// A query on `redis`, as of now.
    pub fn new(redis: &RedisPool) -> Self {
        Self {
            redis: redis.clone(),
            now: None,
        }
    }

    /// Queries as of `now` instead of the current time.
    #[must_use]
    pub fn at(mut self, now: impl Into<DateTime<Utc>>) -> Self {
        self.now = Some(now.into());
        self
    }

    fn now(&self) -> DateTime<Utc> {
        self.now.unwrap_or_else(Utc::now)
    }

    /// The bucket times of `period` ending at the query time, newest first, and the start of
    /// the period.
    fn window(&self, period: Period) -> (Vec<DateTime<Utc>>, DateTime<Utc>, DateTime<Utc>) {
        let now = self.now();
        let step = period.granularity().step();
        let times: Vec<DateTime<Utc>> = (0..period.buckets())
            .map(|index| now - step * index.cast_signed())
            .collect();
        let starts_at = now - step * period.buckets().cast_signed();
        (times, starts_at, now)
    }

    /// The metrics of every job class over `period`. With a `class_filter`, only the classes
    /// whose name contains it (ignoring case).
    pub async fn top_jobs(
        &self,
        period: Period,
        class_filter: Option<&str>,
    ) -> Result<QueryResult> {
        let granularity = period.granularity();
        let (times, starts_at, ends_at) = self.window(period);
        let mut pipe = ::redis::pipe();
        for time in &times {
            pipe.cmd("HGETALL").arg(granularity.key(*time));
        }
        let pipe = &pipe;
        let hashes: Vec<Vec<(String, String)>> = self
            .redis
            .with_retry(|mut conn| async move { pipe.query_async(&mut conn).await })
            .await?;
        let filter = class_filter
            .filter(|filter| !filter.is_empty())
            .map(str::to_lowercase);
        let mut jobs: BTreeMap<String, JobMetrics> = BTreeMap::new();
        for (time, fields) in times.iter().zip(hashes) {
            let bucket = granularity.truncate(*time);
            for (field, value) in fields {
                let Some((class, metric)) = keys::parse_metric_field(&field) else {
                    continue;
                };
                if let Some(filter) = &filter
                    && !class.to_lowercase().contains(filter.as_str())
                {
                    continue;
                }
                let Some(value) = parse_counter(&value) else {
                    continue;
                };
                jobs.entry(class.to_owned())
                    .or_default()
                    .add(bucket, metric, value);
            }
        }
        let marks = fetch_marks(&self.redis, starts_at, ends_at, granularity).await?;
        Ok(QueryResult {
            granularity,
            starts_at,
            ends_at,
            jobs,
            marks,
        })
    }

    /// The metrics of job class `class` over `period`, with per-minute execution-time
    /// histograms when the period is in minutes (histograms are only kept per minute). The
    /// result always has an entry for `class`, empty if it did not run.
    pub async fn for_job(&self, class: &str, period: Period) -> Result<QueryResult> {
        let granularity = period.granularity();
        let (times, starts_at, ends_at) = self.window(period);
        let metric_names = [
            keys::METRIC_MS,
            keys::METRIC_PROCESSED,
            keys::METRIC_FAILED,
            keys::METRIC_OVER_LIMIT,
            keys::METRIC_THROTTLED,
            keys::METRIC_SKIPPED,
        ];
        let fields = metric_names.map(|metric| keys::metric_field(class, metric));
        let mut pipe = ::redis::pipe();
        for time in &times {
            pipe.cmd("HMGET").arg(granularity.key(*time)).arg(&fields);
        }
        let pipe = &pipe;
        let counters: Vec<Vec<Option<String>>> = self
            .redis
            .with_retry(|mut conn| async move { pipe.query_async(&mut conn).await })
            .await?;
        let mut metrics = JobMetrics::default();
        for (time, values) in times.iter().zip(counters) {
            let bucket = granularity.truncate(*time);
            for (metric, value) in metric_names.into_iter().zip(values) {
                if let Some(value) = value.as_deref().and_then(parse_counter) {
                    metrics.add(bucket, metric, value);
                }
            }
        }
        if granularity == Granularity::Minute {
            let mut pipe = ::redis::pipe();
            for time in &times {
                let command = pipe.cmd("BITFIELD_RO").arg(keys::histogram(class, *time));
                for index in 0..BUCKETS {
                    command.arg("GET").arg("u16").arg(format!("#{index}"));
                }
            }
            let pipe = &pipe;
            let histograms: Vec<Vec<u64>> = self
                .redis
                .with_retry(|mut conn| async move { pipe.query_async(&mut conn).await })
                .await?;
            for (time, counts) in times.iter().zip(histograms) {
                let mut histogram = Histogram::new();
                for (index, count) in counts.into_iter().enumerate() {
                    histogram.add_to_bucket(index, count);
                }
                if !histogram.is_empty() {
                    metrics
                        .histograms
                        .insert(granularity.truncate(*time), histogram);
                }
            }
        }
        let marks = fetch_marks(&self.redis, starts_at, ends_at, granularity).await?;
        let mut jobs = BTreeMap::new();
        jobs.insert(class.to_owned(), metrics);
        Ok(QueryResult {
            granularity,
            starts_at,
            ends_at,
            jobs,
            marks,
        })
    }
}

/// A counter stored in a metrics hash (negative or unreadable values are skipped).
fn parse_counter(value: &str) -> Option<u64> {
    value.trim().parse::<u64>().ok()
}

/// The UTC days a period spans, first to last.
pub(super) fn days(starts_at: DateTime<Utc>, ends_at: DateTime<Utc>) -> Vec<NaiveDate> {
    let mut days = Vec::new();
    let mut day = starts_at.date_naive();
    let last = ends_at.date_naive();
    while day <= last {
        days.push(day);
        match day.succ_opt() {
            Some(next) => day = next,
            None => break,
        }
    }
    days
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn feature_counts_are_not_timed() {
        let mut counts = Counts::default();
        for (metric, value) in [
            ("p", 10),
            ("f", 1),
            ("o", 2),
            ("s", 1),
            ("t", 7),
            ("ms", 6_000),
        ] {
            counts.add(metric, value);
        }
        assert_eq!(
            (counts.over_limit, counts.skipped, counts.throttled),
            (2, 1, 7)
        );
        assert_eq!(counts.timed(), 6);
        assert_eq!(counts.average_ms(), 1_000.0);
    }
    use chrono::TimeZone;

    fn time(h: u32, m: u32, s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2022, 7, 22, h, m, s).unwrap()
    }

    #[test]
    fn periods() {
        assert_eq!(Period::from_label("1h"), Some(Period::Minutes(60)));
        assert_eq!(Period::from_label("72h"), Some(Period::Hours(72)));
        assert_eq!(Period::from_label("3h"), None);
        for (label, period) in Period::PRESETS {
            assert_eq!(period.label(), label);
        }
        assert_eq!(Period::Minutes(90).label(), "90m");
        assert_eq!(Period::Minutes(60).buckets(), 60);
        assert_eq!(Period::Minutes(10_000).buckets(), MAX_MINUTES);
        assert_eq!(Period::Minutes(0).buckets(), 1);
        assert_eq!(Period::Hours(24).buckets(), 144);
        assert_eq!(Period::Hours(1_000).buckets(), MAX_HOURS * 6);
        assert_eq!(Period::default().granularity(), Granularity::Minute);
        assert_eq!(Period::Hours(2).granularity(), Granularity::TenMinutes);
    }

    #[test]
    fn granularity_truncates() {
        assert_eq!(
            Granularity::Minute.truncate(time(22, 3, 59)),
            time(22, 3, 0)
        );
        assert_eq!(
            Granularity::TenMinutes.truncate(time(22, 3, 59)),
            time(22, 0, 0)
        );
        assert_eq!(
            Granularity::TenMinutes.truncate(time(21, 58, 0)),
            time(21, 50, 0)
        );
        assert_eq!(Granularity::Minute.key(time(8, 4, 0)), "j|20220722|8:04");
        assert_eq!(
            Granularity::TenMinutes.key(time(8, 43, 0)),
            "j|20220722|8:4"
        );
    }

    #[test]
    fn counts() {
        let mut counts = Counts::default();
        assert_eq!(counts.average_ms(), 0.0);
        counts.add("p", 4);
        counts.add("f", 1);
        counts.add("ms", 3_000);
        counts.add("s", 99);
        assert_eq!(counts.succeeded(), 3);
        assert_eq!(counts.seconds(), 3.0);
        assert_eq!(counts.average_ms(), 1_000.0);
        assert_eq!(parse_counter("12"), Some(12));
        assert_eq!(parse_counter("-1"), None);
        assert_eq!(parse_counter("x"), None);
    }

    #[test]
    fn result_buckets_and_sorting() {
        let mut jobs = BTreeMap::new();
        let mut a = JobMetrics::default();
        a.add(time(22, 3, 0), "p", 10);
        a.add(time(22, 3, 0), "ms", 100);
        let mut b = JobMetrics::default();
        b.add(time(22, 2, 0), "p", 2);
        b.add(time(22, 2, 0), "f", 2);
        let mut c = JobMetrics::default();
        c.add(time(22, 2, 0), "p", 1);
        c.add(time(22, 2, 0), "ms", 500);
        jobs.insert("A".to_owned(), a);
        jobs.insert("B".to_owned(), b);
        jobs.insert("C".to_owned(), c);
        let result = QueryResult {
            granularity: Granularity::Minute,
            starts_at: time(21, 3, 0),
            ends_at: time(22, 3, 0),
            jobs,
            marks: Vec::new(),
        };
        let buckets = result.buckets();
        assert_eq!(buckets.len(), 60);
        assert_eq!(buckets.first(), Some(&time(21, 4, 0)));
        assert_eq!(buckets.last(), Some(&time(22, 3, 0)));
        let order = |by| -> Vec<&str> {
            result
                .sorted(by)
                .into_iter()
                .map(|(class, _)| class)
                .collect()
        };
        assert_eq!(order(SortBy::TotalTime), ["C", "A", "B"]);
        assert_eq!(order(SortBy::Processed), ["A", "B", "C"]);
        assert_eq!(order(SortBy::Failed), ["B", "A", "C"]);
        assert_eq!(order(SortBy::AverageTime), ["C", "A", "B"]);
    }

    #[test]
    fn days_of_a_period() {
        let end = time(1, 0, 0);
        assert_eq!(days(end - TimeDelta::hours(72), end).len(), 4);
        assert_eq!(days(end - TimeDelta::minutes(30), end).len(), 1);
    }
}
