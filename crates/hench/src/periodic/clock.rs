//! The clock periodic jobs read: Redis's (`TIME`), so that the skew of worker hosts never fires
//! a tick early or twice (`docs/design/periodic.md` §2.1, D3), or, in tests, a [`ManualClock`].

#[cfg(feature = "test-util")]
use std::sync::Arc;
#[cfg(feature = "test-util")]
use std::sync::atomic::{AtomicI64, Ordering};
#[cfg(feature = "test-util")]
use std::time::{Duration, SystemTime};

use crate::config::Config;
use crate::error::Result;
use crate::redis::RedisPool;

/// A clock tests set by hand, read by the periodic poller and
/// [`PeriodicSet`](crate::periodic::PeriodicSet) instead of Redis's: tests then cross hours,
/// days and daylight saving changes without waiting.
///
/// Install it on a worker with [`HenchBuilder::data`](crate::HenchBuilder::data) (the poller
/// looks for one there), and on the API with `PeriodicSet::with_clock`. Clones share the time.
/// Only the timetables read it: job payloads, logs and unique locks keep the real time.
///
/// ```
/// use std::time::{Duration, SystemTime};
/// use hench::periodic::ManualClock;
///
/// let clock = ManualClock::new(SystemTime::UNIX_EPOCH + Duration::from_secs(1_800_000_000));
/// clock.advance(Duration::from_secs(3_600));
/// assert_eq!(
///     clock.now(),
///     SystemTime::UNIX_EPOCH + Duration::from_secs(1_800_003_600)
/// );
/// ```
#[cfg(feature = "test-util")]
#[derive(Debug, Clone)]
pub struct ManualClock {
    now_ms: Arc<AtomicI64>,
}

#[cfg(feature = "test-util")]
impl ManualClock {
    /// A clock showing `now`.
    pub fn new(now: SystemTime) -> Self {
        Self {
            now_ms: Arc::new(AtomicI64::new(super::epoch_millis(now))),
        }
    }

    /// The time it shows.
    pub fn now(&self) -> SystemTime {
        crate::payload::millis_to_system_time(self.now_ms.load(Ordering::Acquire))
    }

    /// Sets the time.
    pub fn set(&self, now: SystemTime) {
        self.now_ms
            .store(super::epoch_millis(now), Ordering::Release);
    }

    /// Moves the time forward by `by`.
    pub fn advance(&self, by: Duration) {
        let by = i64::try_from(by.as_millis()).unwrap_or(i64::MAX);
        self.now_ms.fetch_add(by, Ordering::AcqRel);
    }

    /// The time, epoch milliseconds.
    pub(crate) fn now_ms(&self) -> i64 {
        self.now_ms.load(Ordering::Acquire)
    }
}

/// The clock of the periodic poller and API: Redis's, unless a test installed a
/// [`ManualClock`].
#[derive(Debug, Clone, Default)]
pub(crate) struct Clock {
    #[cfg(feature = "test-util")]
    manual: Option<ManualClock>,
}

impl Clock {
    /// Redis's clock.
    pub(crate) fn redis() -> Self {
        Self::default()
    }

    /// The clock a worker uses: a [`ManualClock`] stored in its data, else Redis's.
    pub(crate) fn of(config: &Config) -> Self {
        #[cfg(feature = "test-util")]
        {
            Self {
                manual: config.data.get::<ManualClock>().cloned(),
            }
        }
        #[cfg(not(feature = "test-util"))]
        {
            let _ = config;
            Self::redis()
        }
    }

    /// A manual clock.
    #[cfg(feature = "test-util")]
    pub(crate) fn manual(clock: ManualClock) -> Self {
        Self {
            manual: Some(clock),
        }
    }

    /// The time to pass to the scripts that read the clock themselves (`LEASE`), when it is not
    /// Redis's.
    pub(crate) fn override_ms(&self) -> Option<i64> {
        #[cfg(feature = "test-util")]
        {
            self.manual.as_ref().map(ManualClock::now_ms)
        }
        #[cfg(not(feature = "test-util"))]
        {
            None
        }
    }

    /// The time, epoch milliseconds: Redis's `TIME` (one round trip), or the manual clock's.
    pub(crate) async fn now_ms(&self, redis: &RedisPool) -> Result<i64> {
        if let Some(now) = self.override_ms() {
            return Ok(now);
        }
        let (seconds, micros): (i64, i64) = redis
            .with_retry(|mut conn| async move {
                ::redis::cmd("TIME")
                    .query_async::<(i64, i64)>(&mut conn)
                    .await
            })
            .await?;
        Ok(seconds * 1_000 + micros / 1_000)
    }
}

#[cfg(all(test, feature = "test-util"))]
mod tests {
    use super::*;
    use crate::test_util::TestRedis;

    #[tokio::test]
    async fn the_clock_is_redis_unless_a_manual_one_is_installed() {
        let redis = TestRedis::start().await;
        let pool = redis.pool().await;
        let clock = Clock::redis();
        assert_eq!(clock.override_ms(), None);
        let now = clock.now_ms(&pool).await.unwrap();
        assert!((now - crate::payload::now_millis()).abs() < 5_000);
        let manual = ManualClock::new(SystemTime::UNIX_EPOCH + Duration::from_secs(42));
        let clock = Clock::manual(manual.clone());
        assert_eq!(clock.now_ms(&pool).await.unwrap(), 42_000);
        manual.advance(Duration::from_millis(1_500));
        assert_eq!(clock.override_ms(), Some(43_500));
        manual.set(SystemTime::UNIX_EPOCH);
        assert_eq!(clock.now_ms(&pool).await.unwrap(), 0);
    }
}
