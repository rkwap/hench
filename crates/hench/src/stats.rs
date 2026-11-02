//! Local processed/failed counters and the shared work state flushed by the heartbeat.
//! See `docs/DESIGN.md` §5.
//
// LANE: engine — this module is owned by the engine lane.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Serialize, Serializer};

use crate::error::Result;
use crate::flush::{Batch, Flusher};
use crate::keys;
use crate::launcher::lock;
use crate::pro::Fetched;
use crate::redis::RedisPool;

/// What one worker slot is running, as stored in the `<identity>:work` hash.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct Work {
    /// Name of the queue the job was fetched from.
    pub(crate) queue: String,
    /// The job's JSON, exactly as fetched (shared with the unit of work, not copied: it is
    /// only decoded, lossily if it is not UTF-8, when the heartbeat writes the work state).
    #[serde(serialize_with = "as_text")]
    pub(crate) payload: Arc<Vec<u8>>,
    /// When the job started, in epoch seconds.
    pub(crate) run_at: f64,
}

/// Serializes job bytes as text (replacing invalid UTF-8, see [`String::from_utf8_lossy`]).
fn as_text<S: Serializer>(bytes: &Arc<Vec<u8>>, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&String::from_utf8_lossy(bytes))
}

/// Counters and work state of one running process (one launcher).
///
/// Processors update it for every job; the heartbeat flushes the counters to the `stat:*`
/// keys and rewrites the `<identity>:work` hash from the work state.
#[derive(Debug, Default)]
pub(crate) struct Stats {
    processed: AtomicU64,
    failed: AtomicU64,
    work: Mutex<HashMap<String, Work>>,
    /// The feature state of the running jobs that use a feature (unique lock, batch, schedule,
    /// throttle), by worker slot: what the heartbeat's renewals read. Jobs without a feature
    /// never touch it.
    featured: Mutex<HashMap<String, Arc<Fetched>>>,
    /// Writes each flush's counts once, even when replies are lost.
    flusher: Flusher,
}

impl Stats {
    /// Empty counters and work state.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Counts one executed job (successful or not).
    pub(crate) fn record_processed(&self) {
        self.processed.fetch_add(1, Ordering::Relaxed);
    }

    /// Counts one failed job.
    pub(crate) fn record_failed(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Counts not yet flushed: `(processed, failed)`.
    #[cfg(test)]
    pub(crate) fn pending(&self) -> (u64, u64) {
        (
            self.processed.load(Ordering::Relaxed),
            self.failed.load(Ordering::Relaxed),
        )
    }

    /// Takes the counts, resetting them to zero.
    fn take(&self) -> (u64, u64) {
        (
            self.processed.swap(0, Ordering::Relaxed),
            self.failed.swap(0, Ordering::Relaxed),
        )
    }

    /// Records that worker slot `tid` started running a job.
    pub(crate) fn start_work(&self, tid: &str, work: Work) {
        self.lock_work().insert(tid.to_owned(), work);
    }

    /// Records that worker slot `tid` finished its job.
    pub(crate) fn finish_work(&self, tid: &str) {
        self.lock_work().remove(tid);
    }

    /// Records the feature state of the job worker slot `tid` started (for jobs that use a
    /// feature only).
    pub(crate) fn start_featured(&self, tid: &str, fetched: Arc<Fetched>) {
        lock(&self.featured).insert(tid.to_owned(), fetched);
    }

    /// Forgets the feature state of worker slot `tid`'s job.
    pub(crate) fn finish_featured(&self, tid: &str) {
        lock(&self.featured).remove(tid);
    }

    /// The feature state of the running jobs that use a feature, by worker slot, sorted by
    /// slot.
    pub(crate) fn featured(&self) -> Vec<(String, Arc<Fetched>)> {
        let mut featured: Vec<(String, Arc<Fetched>)> = lock(&self.featured)
            .iter()
            .map(|(tid, fetched)| (tid.clone(), fetched.clone()))
            .collect();
        featured.sort_by(|a, b| a.0.cmp(&b.0));
        featured
    }

    /// Number of jobs running right now.
    pub(crate) fn busy(&self) -> usize {
        self.lock_work().len()
    }

    /// The running jobs by worker slot, sorted by slot.
    pub(crate) fn running(&self) -> Vec<(String, Work)> {
        let mut running: Vec<(String, Work)> = self.lock_work().clone().into_iter().collect();
        running.sort_by(|a, b| a.0.cmp(&b.0));
        running
    }

    /// The running jobs as `(tid, JSON)` pairs, ready for `HSET <identity>:work`.
    pub(crate) fn work_snapshot(&self) -> Vec<(String, String)> {
        let work = self.lock_work().clone();
        work.into_iter()
            .filter_map(|(tid, work)| match serde_json::to_string(&work) {
                Ok(json) => Some((tid, json)),
                Err(error) => {
                    tracing::warn!(%error, tid, "could not serialize the work state");
                    None
                }
            })
            .collect()
    }

    fn lock_work(&self) -> std::sync::MutexGuard<'_, HashMap<String, Work>> {
        crate::launcher::lock(&self.work)
    }

    /// Adds the local counts to the lifetime and daily (UTC) `stat:*` counters in one atomic
    /// step. When Redis does not confirm it (an error, a timeout), the counts are kept as they
    /// were and the next flush writes them first, applied only if this attempt was not (Redis
    /// may run a request whose reply timed out), so that every job is counted once.
    pub(crate) async fn flush(&self, redis: &RedisPool) -> Result<()> {
        self.flusher
            .flush(redis, || {
                let (processed, failed) = self.take();
                (processed > 0 || failed > 0).then(|| stats_batch(processed, failed))
            })
            .await?;
        Ok(())
    }
}

/// The writes adding `processed` and `failed` jobs to the lifetime counters and to those of
/// the current day (UTC).
fn stats_batch(processed: u64, failed: u64) -> Batch {
    let today = chrono::Utc::now().date_naive();
    let ttl = keys::STATS_TTL.as_secs();
    let mut batch = Batch::new();
    for (lifetime, daily, count) in [
        (
            keys::STAT_PROCESSED,
            keys::stat_processed_on(today),
            processed,
        ),
        (keys::STAT_FAILED, keys::stat_failed_on(today), failed),
    ] {
        batch
            .write("INCRBY", lifetime, [count])
            .write("INCRBY", &daily, [count])
            .write("EXPIRE", &daily, [ttl]);
    }
    batch
}

/// Resident set size of this process in kilobytes (`VmRSS` from `/proc/self/status`), or 0
/// where that is not available.
pub(crate) fn rss_kb() -> u64 {
    #[cfg(target_os = "linux")]
    {
        match std::fs::read_to_string("/proc/self/status") {
            Ok(status) => parse_vm_rss(&status).unwrap_or(0),
            Err(_) => 0,
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        0
    }
}

/// Extracts the `VmRSS` value (in kB) from the contents of a `/proc/<pid>/status` file.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_vm_rss(status: &str) -> Option<u64> {
    status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .and_then(|value| value.split_whitespace().next())
        .and_then(|kb| kb.parse().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_take() {
        let stats = Stats::new();
        stats.record_processed();
        stats.record_processed();
        stats.record_failed();
        assert_eq!(stats.pending(), (2, 1));
        assert_eq!(stats.take(), (2, 1));
        assert_eq!(stats.pending(), (0, 0));
        stats.record_processed();
        assert_eq!(stats.pending(), (1, 0));
    }

    #[tokio::test]
    async fn failed_flushes_keep_their_counts_apart_from_new_ones() {
        let stats = Stats::new();
        stats.record_processed();
        stats.record_failed();
        assert!(stats.flush(&RedisPool::detached()).await.is_err());
        // Kept for the next flush, which sends them again as they were; jobs counted
        // meanwhile wait for the flush after it.
        assert_eq!(stats.pending(), (0, 0));
        assert!(stats.flusher.has_unconfirmed());
        stats.record_processed();
        assert!(stats.flush(&RedisPool::detached()).await.is_err());
        assert_eq!(stats.pending(), (1, 0));
    }

    #[test]
    fn work_state() {
        let stats = Stats::new();
        assert_eq!(stats.busy(), 0);
        stats.start_work(
            "abc",
            Work {
                queue: "default".into(),
                payload: Arc::new(br#"{"class":"A"}"#.to_vec()),
                run_at: 1_700_000_000.5,
            },
        );
        // A job that is not valid UTF-8 is decoded lossily.
        stats.start_work(
            "def",
            Work {
                queue: "default".into(),
                payload: Arc::new(vec![b'{', 0xff, b'}']),
                run_at: 1_700_000_000.5,
            },
        );
        assert_eq!(stats.busy(), 2);
        let mut snapshot = stats.work_snapshot();
        snapshot.sort();
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot[0].0, "abc");
        let value: serde_json::Value = serde_json::from_str(&snapshot[0].1).unwrap();
        assert_eq!(value["queue"], "default");
        assert_eq!(value["payload"], r#"{"class":"A"}"#);
        assert_eq!(value["run_at"], 1_700_000_000.5);
        let value: serde_json::Value = serde_json::from_str(&snapshot[1].1).unwrap();
        assert_eq!(value["payload"], "{\u{fffd}}");
        stats.finish_work("def");
        stats.finish_work("abc");
        stats.finish_work("missing");
        assert_eq!(stats.busy(), 0);
        assert!(stats.work_snapshot().is_empty());
    }

    #[test]
    fn vm_rss_parsing() {
        let status = "Name:\thench\nVmPeak:\t  2000 kB\nVmRSS:\t  1234 kB\nThreads:\t4\n";
        assert_eq!(parse_vm_rss(status), Some(1234));
        assert_eq!(parse_vm_rss("Name:\tx\n"), None);
        assert_eq!(parse_vm_rss("VmRSS:\tlots kB\n"), None);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn rss_is_reported_on_linux() {
        assert!(rss_kb() > 0);
    }
}

/// Flushes against a private Redis.
#[cfg(all(test, unix, feature = "test-util"))]
mod redis_tests {
    use std::time::Duration;

    use super::*;
    use crate::test_util::TestRedis;

    async fn get(redis: &RedisPool, key: &str) -> Option<u64> {
        let mut conn = redis.conn().await.unwrap();
        ::redis::cmd("GET")
            .arg(key)
            .query_async(&mut conn)
            .await
            .unwrap()
    }

    /// `redis-server` stopped with SIGSTOP until dropped: it receives requests but runs them
    /// only once it continues.
    struct Stalled(String);

    impl Stalled {
        async fn start(redis: &RedisPool) -> Self {
            let mut conn = redis.conn().await.unwrap();
            let info: String = ::redis::cmd("INFO")
                .arg("server")
                .query_async(&mut conn)
                .await
                .unwrap();
            let pid = info
                .lines()
                .find_map(|line| line.strip_prefix("process_id:"))
                .unwrap()
                .trim()
                .to_owned();
            let status = std::process::Command::new("kill")
                .args(["-STOP", &pid])
                .status()
                .unwrap();
            assert!(status.success());
            Self(pid)
        }
    }

    impl Drop for Stalled {
        fn drop(&mut self) {
            let _ = std::process::Command::new("kill")
                .args(["-CONT", &self.0])
                .status();
        }
    }

    /// A flush that times out may still be run by Redis (here once it continues after a
    /// stall): the flushes after it must not count its jobs again.
    #[tokio::test]
    async fn flushes_that_time_out_are_counted_once() {
        let redis = TestRedis::start().await;
        let reader = redis.pool().await;
        let pool =
            RedisPool::from_config(redis.config().response_timeout(Duration::from_millis(250)))
                .await
                .unwrap();
        let stats = Stats::new();
        stats.record_processed();
        // Also loads the script, so that Redis can run the attempts it receives during the
        // stall.
        stats.flush(&pool).await.unwrap();
        for _ in 0..10 {
            stats.record_processed();
        }
        stats.record_failed();
        {
            let _stalled = Stalled::start(&reader).await;
            for _ in 0..3 {
                assert!(stats.flush(&pool).await.is_err());
            }
            stats.record_processed();
        }
        stats.flush(&pool).await.unwrap();
        assert!(!stats.flusher.has_unconfirmed());
        assert_eq!(stats.pending(), (0, 0));
        assert_eq!(get(&reader, keys::STAT_PROCESSED).await, Some(12));
        assert_eq!(get(&reader, keys::STAT_FAILED).await, Some(1));
        let today = chrono::Utc::now().date_naive();
        assert_eq!(
            get(&reader, &keys::stat_processed_on(today)).await,
            Some(12)
        );
    }
}
