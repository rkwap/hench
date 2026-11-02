//! Execution-time histograms: a fixed set of buckets whose upper bounds grow roughly by half
//! each step, from 20 ms to about five and a half minutes, plus a last bucket for anything
//! slower. Counting executions per bucket keeps the storage constant (one 16-bit counter per
//! bucket per class per minute in Redis) however many jobs run.

use std::fmt;
use std::time::Duration;

/// Number of buckets in a [`Histogram`].
pub const BUCKETS: usize = 26;

/// Exclusive upper bound of each bucket but the last, in milliseconds: an execution of `ms`
/// milliseconds is counted in the first bucket whose bound is greater than `ms`. The last
/// bucket counts everything from 335 s on.
pub const BUCKET_BOUNDS_MS: [u64; BUCKETS - 1] = [
    20, 30, 45, 65, 100, 150, 225, 335, 500, 750, 1_100, 1_700, 2_500, 3_800, 5_750, 8_500, 13_000,
    20_000, 30_000, 45_000, 65_000, 100_000, 150_000, 225_000, 335_000,
];

/// Execution counts per duration bucket (see [`BUCKET_BOUNDS_MS`]). Bucket 0 holds the fastest
/// executions.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub struct Histogram {
    counts: [u64; BUCKETS],
}

impl Histogram {
    /// An empty histogram.
    pub fn new() -> Self {
        Self::default()
    }

    /// A histogram with these counts (bucket 0 first).
    pub fn from_counts(counts: [u64; BUCKETS]) -> Self {
        Self { counts }
    }

    /// The bucket an execution of `ms` milliseconds falls in.
    pub fn bucket_index(ms: u64) -> usize {
        BUCKET_BOUNDS_MS.partition_point(|&bound| bound <= ms)
    }

    /// The bucket an execution taking `elapsed` falls in.
    pub fn bucket_of(elapsed: Duration) -> usize {
        Self::bucket_index(millis(elapsed))
    }

    /// Counts one execution of `ms` milliseconds.
    pub fn record_ms(&mut self, ms: u64) {
        let bucket = &mut self.counts[Self::bucket_index(ms)];
        *bucket = bucket.saturating_add(1);
    }

    /// Counts one execution taking `elapsed`.
    pub fn record(&mut self, elapsed: Duration) {
        self.record_ms(millis(elapsed));
    }

    /// Adds `count` executions to bucket `index` (ignored if out of range).
    pub fn add_to_bucket(&mut self, index: usize, count: u64) {
        if let Some(bucket) = self.counts.get_mut(index) {
            *bucket = bucket.saturating_add(count);
        }
    }

    /// Adds every count of `other`.
    pub fn merge(&mut self, other: &Histogram) {
        for (bucket, count) in self.counts.iter_mut().zip(other.counts) {
            *bucket = bucket.saturating_add(count);
        }
    }

    /// The counts, bucket 0 (fastest) first.
    pub fn counts(&self) -> &[u64; BUCKETS] {
        &self.counts
    }

    /// The count of bucket `index` (0 when out of range).
    pub fn count(&self, index: usize) -> u64 {
        self.counts.get(index).copied().unwrap_or(0)
    }

    /// Total number of executions counted.
    pub fn total(&self) -> u64 {
        self.counts
            .iter()
            .fold(0_u64, |total, count| total.saturating_add(*count))
    }

    /// `true` when nothing was counted.
    pub fn is_empty(&self) -> bool {
        self.counts.iter().all(|count| *count == 0)
    }

    /// The exclusive upper bound of bucket `index`, or `None` for the last (unbounded) bucket
    /// and indexes out of range.
    pub fn upper_bound(index: usize) -> Option<Duration> {
        BUCKET_BOUNDS_MS
            .get(index)
            .map(|ms| Duration::from_millis(*ms))
    }

    /// A short label for bucket `index`: its upper bound (`"20ms"`, `"1.1s"`, `"335s"`), or
    /// `">335s"` for the last bucket. Empty for indexes out of range.
    pub fn label(index: usize) -> String {
        match BUCKET_BOUNDS_MS.get(index) {
            Some(bound) => format_millis(*bound),
            None if index == BUCKETS - 1 => {
                format!(">{}", format_millis(BUCKET_BOUNDS_MS[BUCKETS - 2]))
            }
            None => String::new(),
        }
    }
}

impl fmt::Debug for Histogram {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Only the non-empty buckets, which is what one wants to see.
        let mut map = f.debug_map();
        for (index, count) in self.counts.iter().enumerate() {
            if *count > 0 {
                map.entry(&Self::label(index), count);
            }
        }
        map.finish()
    }
}

/// Whole milliseconds of `elapsed`, saturating.
pub(crate) fn millis(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
}

/// `"750ms"`, `"1.1s"`, `"5.75s"`, `"13s"`.
fn format_millis(ms: u64) -> String {
    if ms < 1_000 {
        return format!("{ms}ms");
    }
    let (secs, rest) = (ms / 1_000, ms % 1_000);
    if rest == 0 {
        return format!("{secs}s");
    }
    let fraction = format!("{rest:03}");
    format!("{secs}.{}s", fraction.trim_end_matches('0'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_are_increasing() {
        assert!(BUCKET_BOUNDS_MS.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn executions_land_in_the_right_bucket() {
        assert_eq!(Histogram::bucket_index(0), 0);
        assert_eq!(Histogram::bucket_index(19), 0);
        assert_eq!(Histogram::bucket_index(20), 1);
        assert_eq!(Histogram::bucket_index(29), 1);
        assert_eq!(Histogram::bucket_index(46), 3);
        assert_eq!(Histogram::bucket_index(300), 7);
        assert_eq!(Histogram::bucket_index(334_999), 24);
        assert_eq!(Histogram::bucket_index(335_000), 25);
        assert_eq!(Histogram::bucket_index(u64::MAX), 25);
        assert_eq!(Histogram::bucket_of(Duration::from_micros(20_999)), 1);
    }

    #[test]
    fn counts_executions() {
        // Sidekiq's histogram scenario: 10, 46, 47, 48, 300, 301, 302 and a huge duration.
        let mut histogram = Histogram::new();
        assert!(histogram.is_empty());
        for ms in [10, 46, 47, 48, 300, 301, 302, 300_000_000] {
            histogram.record_ms(ms);
        }
        assert_eq!(histogram.total(), 8);
        assert_eq!(histogram.count(0), 1);
        assert_eq!(histogram.count(3), 3);
        assert_eq!(histogram.count(7), 3);
        assert_eq!(histogram.count(25), 1);
        assert_eq!(histogram.count(99), 0);

        let mut sum = Histogram::new();
        sum.merge(&histogram);
        sum.merge(&histogram);
        sum.add_to_bucket(0, 2);
        sum.add_to_bucket(BUCKETS, 2);
        assert_eq!(sum.total(), 18);
        assert_eq!(sum.count(0), 4);
        assert_eq!(Histogram::from_counts(*sum.counts()), sum);
    }

    #[test]
    fn labels() {
        let labels: Vec<String> = (0..BUCKETS).map(Histogram::label).collect();
        assert_eq!(labels[0], "20ms");
        assert_eq!(labels[9], "750ms");
        assert_eq!(labels[10], "1.1s");
        assert_eq!(labels[14], "5.75s");
        assert_eq!(labels[16], "13s");
        assert_eq!(labels[24], "335s");
        assert_eq!(labels[25], ">335s");
        assert_eq!(Histogram::label(BUCKETS), "");
        assert_eq!(Histogram::upper_bound(0), Some(Duration::from_millis(20)));
        assert_eq!(Histogram::upper_bound(25), None);
        let mut histogram = Histogram::new();
        histogram.record_ms(25);
        assert_eq!(format!("{histogram:?}"), r#"{"30ms": 1}"#);
    }
}
