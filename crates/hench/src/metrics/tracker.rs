//! In-memory aggregation of job executions, flushed to Redis by the heartbeat.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use chrono::{DateTime, Utc};

use super::histogram::{BUCKETS, Histogram, millis};
use crate::error::Result;
use crate::flush::{Batch, Flusher};
use crate::keys;
use crate::launcher::lock;
use crate::redis::RedisPool;

/// How an execution ended, for [`Tracker::track`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Outcome {
    /// The job succeeded: counted, and its duration recorded.
    Success,
    /// The job failed (an error, a panic, or a hard shutdown): counted as processed and
    /// failed. Its duration is not recorded, since failures say little about performance.
    Failure,
    /// An iterable job saved its progress and asked to continue later: counted and timed like a
    /// success, not as a failure.
    Interrupted,
    /// The job went over a limiter's limit while running and was rescheduled: counted as
    /// processed and over limit (`o`), not as a failure, not timed.
    OverLimit,
    /// A throttle parked the job before it started: counted as throttled (`t`) only (it did not
    /// run).
    Throttled,
    /// The job's batch was cancelled and it was skipped: counted as processed and skipped (`s`),
    /// not timed.
    Skipped,
}

/// What one job class did since the last flush.
#[derive(Debug, Clone, Default, PartialEq)]
struct ClassStats {
    processed: u64,
    failed: u64,
    over_limit: u64,
    throttled: u64,
    skipped: u64,
    /// Total milliseconds of the timed executions.
    ms: u64,
    /// Durations of the timed executions.
    histogram: Histogram,
}

impl ClassStats {
    fn record(&mut self, elapsed: Duration, outcome: Outcome) {
        if outcome == Outcome::Throttled {
            // Parked before it started: not an execution.
            self.throttled = self.throttled.saturating_add(1);
            return;
        }
        self.processed = self.processed.saturating_add(1);
        match outcome {
            Outcome::Failure => self.failed = self.failed.saturating_add(1),
            Outcome::Success | Outcome::Interrupted => {
                let ms = millis(elapsed);
                self.ms = self.ms.saturating_add(ms);
                self.histogram.record_ms(ms);
            }
            Outcome::OverLimit => self.over_limit = self.over_limit.saturating_add(1),
            Outcome::Skipped => self.skipped = self.skipped.saturating_add(1),
            Outcome::Throttled => {}
        }
    }
}

/// Aggregates job executions per class in memory (a lock and a hash lookup per job) and
/// writes them to Redis in one atomic step per flush.
///
/// Workers create one per process (unless [`HenchBuilder::metrics`](crate::HenchBuilder::metrics)
/// is off), track every job they run and flush on every heartbeat and when they stop. A flush
/// adds the counts to the hashes of the current minute ([`keys::metrics_minute`], kept 8 hours)
/// and of the current ten minutes ([`keys::metrics_ten_minutes`], kept 3 days), and the
/// durations to the class's histogram of the minute ([`keys::histogram`], kept 8 hours). Read
/// them with [`Query`](super::Query).
///
/// ```no_run
/// use hench::metrics::{Outcome, Tracker};
/// use std::time::Duration;
///
/// # async fn demo(redis: hench::RedisPool) -> hench::Result<()> {
/// let tracker = Tracker::new();
/// tracker.track("ImportJob", Duration::from_millis(120), Outcome::Success);
/// tracker.track("ImportJob", Duration::from_millis(3), Outcome::Failure);
/// tracker.flush(&redis).await?;
/// # Ok(()) }
/// ```
#[derive(Debug, Default)]
pub struct Tracker {
    classes: Mutex<HashMap<String, ClassStats>>,
    /// Writes each flush's counts once, even when replies are lost.
    flusher: Flusher,
}

impl Tracker {
    /// An empty tracker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records one execution of a job of `class` that took `elapsed`.
    pub fn track(&self, class: &str, elapsed: Duration, outcome: Outcome) {
        let mut classes = lock(&self.classes);
        // Allocates the class name only the first time it is seen since the last flush.
        if let Some(stats) = classes.get_mut(class) {
            stats.record(elapsed, outcome);
        } else {
            let mut stats = ClassStats::default();
            stats.record(elapsed, outcome);
            classes.insert(class.to_owned(), stats);
        }
    }

    /// `true` when there is nothing to write: nothing was tracked since the last flush, and
    /// Redis confirmed every flush.
    pub fn is_empty(&self) -> bool {
        lock(&self.classes).is_empty() && !self.flusher.has_unconfirmed()
    }

    /// Writes what was tracked since the last flush to the metrics of the current minute. See
    /// [`Tracker::flush_at`].
    pub async fn flush(&self, redis: &RedisPool) -> Result<usize> {
        self.flush_at(redis, Utc::now()).await
    }

    /// Writes what was tracked since the last flush to the metrics of the minute of `at`, in one
    /// atomic step, and returns the number of counters incremented in the minute and
    /// ten-minute hashes.
    ///
    /// When Redis does not confirm a flush (an error, a timeout), its counts are kept as they
    /// were, for the minute of their `at`, and the next flush writes them first, then what was
    /// tracked meanwhile. A flush that timed out may have been applied all the same (Redis runs
    /// the requests it received, a stalled server once it continues): the counts are then not
    /// applied again, so every execution is counted once. The returned number includes the
    /// counters of such an earlier flush.
    pub async fn flush_at(&self, redis: &RedisPool, at: DateTime<Utc>) -> Result<usize> {
        let counters = self
            .flusher
            .flush(redis, || {
                let classes = std::mem::take(&mut *lock(&self.classes));
                (!classes.is_empty()).then(|| flush_batch(&classes, at))
            })
            .await?;
        if counters > 0 {
            tracing::debug!(counters, "flushed job metrics");
        }
        Ok(counters)
    }
}

/// The writes adding `classes` to the metrics of the minute of `at`; its weight is the number
/// of counters it increments.
fn flush_batch(classes: &HashMap<String, ClassStats>, at: DateTime<Utc>) -> Batch {
    let mut batch = Batch::new();
    let histogram_ttl = keys::HISTOGRAM_TTL.as_secs();
    for (class, stats) in classes {
        if stats.histogram.is_empty() {
            continue;
        }
        let key = keys::histogram(class, at);
        let mut args = vec!["OVERFLOW".to_owned(), "SAT".to_owned()];
        for index in 0..BUCKETS {
            let count = stats.histogram.count(index);
            if count > 0 {
                // The counters saturate at u16::MAX anyway.
                let count = count.min(u64::from(u16::MAX));
                args.extend([
                    "INCRBY".to_owned(),
                    "u16".to_owned(),
                    format!("#{index}"),
                    count.to_string(),
                ]);
            }
        }
        batch.write("BITFIELD", &key, args);
        batch.write("EXPIRE", &key, [histogram_ttl]);
    }
    let mut counters = 0;
    let rollups = [
        (keys::metrics_minute(at), keys::METRICS_MINUTE_TTL),
        (keys::metrics_ten_minutes(at), keys::METRICS_TEN_MINUTES_TTL),
    ];
    for (key, ttl) in &rollups {
        for (class, stats) in classes {
            let timed = !stats.histogram.is_empty();
            let values = [
                (keys::METRIC_PROCESSED, stats.processed, stats.processed > 0),
                (keys::METRIC_FAILED, stats.failed, stats.failed > 0),
                (keys::METRIC_MS, stats.ms, timed),
                (
                    keys::METRIC_OVER_LIMIT,
                    stats.over_limit,
                    stats.over_limit > 0,
                ),
                (keys::METRIC_THROTTLED, stats.throttled, stats.throttled > 0),
                (keys::METRIC_SKIPPED, stats.skipped, stats.skipped > 0),
            ];
            for (metric, value, present) in values {
                if present {
                    batch.write(
                        "HINCRBY",
                        key,
                        [keys::metric_field(class, metric), value.to_string()],
                    );
                    counters += 1;
                }
            }
        }
        batch.write("EXPIRE", key, [ttl.as_secs()]);
    }
    batch.set_weight(counters);
    batch
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn feature_outcomes_have_their_own_counters() {
        let tracker = Tracker::new();
        tracker.track("A", Duration::from_millis(5), Outcome::Success);
        tracker.track("A", Duration::from_millis(9), Outcome::OverLimit);
        tracker.track("A", Duration::from_millis(9), Outcome::Skipped);
        tracker.track("A", Duration::from_millis(9), Outcome::Throttled);
        tracker.track("B", Duration::ZERO, Outcome::Throttled);
        let classes = lock(&tracker.classes).clone();
        let a = &classes["A"];
        assert_eq!((a.processed, a.failed, a.ms), (3, 0, 5));
        assert_eq!((a.over_limit, a.throttled, a.skipped), (1, 1, 1));
        let b = &classes["B"];
        assert_eq!((b.processed, b.throttled), (0, 1));
        assert!(b.histogram.is_empty());
        // Only the counters that moved are written.
        let at = chrono::Utc::now();
        let batch = flush_batch(&classes, at);
        assert_eq!(batch.weight(), 2 * (5 + 1));
    }

    #[test]
    fn tracks_per_class() {
        let tracker = Tracker::new();
        assert!(tracker.is_empty());
        tracker.track("A", Duration::from_millis(5), Outcome::Success);
        tracker.track("A", Duration::from_millis(25), Outcome::Interrupted);
        tracker.track("A", Duration::from_millis(1_000), Outcome::Failure);
        tracker.track("B", Duration::from_millis(1), Outcome::Failure);
        let classes = lock(&tracker.classes).clone();
        let a = &classes["A"];
        assert_eq!((a.processed, a.failed, a.ms), (3, 1, 30));
        assert_eq!(a.histogram.total(), 2);
        assert_eq!((a.histogram.count(0), a.histogram.count(1)), (1, 1));
        let b = &classes["B"];
        assert_eq!((b.processed, b.failed, b.ms), (1, 1, 0));
        assert!(b.histogram.is_empty());
    }

    #[tokio::test]
    async fn failed_flushes_keep_their_counts_apart_from_new_ones() {
        let tracker = Tracker::new();
        tracker.track("A", Duration::from_millis(5), Outcome::Success);
        assert!(tracker.flush(&RedisPool::detached()).await.is_err());
        // The failed flush keeps its counts (sent again as they were by the next flush); what
        // is tracked afterwards waits for the flush after it.
        assert!(lock(&tracker.classes).is_empty());
        assert!(!tracker.is_empty());
        tracker.track("A", Duration::from_millis(7), Outcome::Failure);
        assert!(tracker.flush(&RedisPool::detached()).await.is_err());
        let classes = lock(&tracker.classes).clone();
        assert_eq!(
            (classes["A"].processed, classes["A"].failed, classes["A"].ms),
            (1, 1, 0)
        );
    }

    #[test]
    fn counters_skip_absent_metrics() {
        let tracker = Tracker::new();
        tracker.track("A", Duration::from_millis(5), Outcome::Success);
        tracker.track("B", Duration::from_millis(5), Outcome::Failure);
        let classes = lock(&tracker.classes).clone();
        // A: p and ms; B: p and f; twice (minute and ten minutes).
        assert_eq!(flush_batch(&classes, Utc::now()).weight(), 8);
    }

    #[tokio::test]
    async fn failed_flushes_keep_the_data() {
        let tracker = Tracker::new();
        tracker.track("A", Duration::from_millis(5), Outcome::Success);
        assert!(tracker.flush(&RedisPool::detached()).await.is_err());
        assert!(!tracker.is_empty());
        let empty = Tracker::new();
        assert_eq!(empty.flush(&RedisPool::detached()).await.unwrap(), 0);
    }
}
