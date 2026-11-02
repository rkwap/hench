//! Limiters in unit tests of your own code ([ratelimit.md] §2.14): a
//! [`LimitTarget`] whose limits live in memory, on tokio's clock.
//!
//! For jobs, use the testing clients instead: they run limiters unlimited by default, or in
//! memory with [`TestLimits::InMemory`]; [`Client::limited_jobs`] lists the captured jobs a
//! limiter or throttle rescheduled.
//!
//! ```
//! use std::time::Duration;
//! use hench::limit::Limiter;
//! use hench::limit::testing::memory_target;
//!
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! let target = memory_target();
//! let api = Limiter::window("api", 2, Duration::from_secs(1)).max_wait(Duration::ZERO);
//! let _first = api.acquire(&target).await?;
//! let _second = api.acquire(&target).await?;
//! assert!(matches!(api.acquire(&target).await, Err(hench::Error::OverLimit(_))));
//! # Ok::<(), hench::Error>(()) }).unwrap();
//! ```
//!
//! [ratelimit.md]: https://github.com/rkwap/hench/blob/v0.1.0/docs/design/ratelimit.md

use std::fmt;
use std::sync::Arc;

use crate::client::Client;
use crate::job::Job;
use crate::limit::target::{Backend, LimitTarget, Store, sealed};
use crate::payload::JobPayload;
use crate::testing::{Tables, TestLimits};

/// A target whose limits live in memory, on tokio's clock (pause it with
/// `tokio::time::pause()` and step with `advance` to test windows deterministically). Clones
/// share their limits; separate targets do not.
#[derive(Clone)]
pub struct MemoryTarget {
    tables: Arc<Tables>,
}

impl fmt::Debug for MemoryTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryTarget").finish_non_exhaustive()
    }
}

/// A fresh in-memory target.
pub fn memory_target() -> MemoryTarget {
    let tables = Arc::new(Tables::default());
    tables.limits.configure(TestLimits::InMemory, Vec::new());
    MemoryTarget { tables }
}

impl LimitTarget for MemoryTarget {}

impl sealed::Sealed for MemoryTarget {
    fn backend(&self) -> Backend {
        Backend::store(Store::Memory(Arc::clone(&self.tables)))
    }
}

impl Client {
    /// The captured jobs of type `J` that a limiter or a throttle rescheduled (their
    /// [`JobPayload::limited`] is set), oldest first.
    ///
    /// # Panics
    ///
    /// If this is not a fake client.
    pub fn limited_jobs<J: Job>(&self) -> Vec<JobPayload> {
        self.limited_jobs_for(J::NAME)
    }

    /// The captured jobs of class `class` that a limiter or a throttle rescheduled, oldest
    /// first.
    ///
    /// # Panics
    ///
    /// If this is not a fake client.
    pub fn limited_jobs_for(&self, class: &str) -> Vec<JobPayload> {
        let mut jobs = self.jobs_for(class);
        jobs.retain(|job| job.limited.is_some());
        jobs
    }
}

#[cfg(test)]
mod tests {
    use crate::client::Client;
    use crate::limit::Limited;
    use crate::payload::JobPayload;

    #[tokio::test]
    async fn limited_jobs_are_the_rescheduled_ones() {
        let client = Client::fake();
        let mut parked = JobPayload::new("Charge", vec![1.into()]);
        parked.limited = Some(Limited::new(2, "w:stripe:42", Some(1_800_000_000_000)));
        client.push(parked).await.unwrap();
        client
            .push(JobPayload::new("Charge", vec![2.into()]))
            .await
            .unwrap();
        client.push(JobPayload::new("Other", vec![])).await.unwrap();
        let limited = client.limited_jobs_for("Charge");
        assert_eq!(limited.len(), 1);
        assert_eq!(limited[0].args, vec![serde_json::Value::from(1)]);
        assert_eq!(
            limited[0].limited.as_ref().map(|limited| limited.n),
            Some(2)
        );
        assert!(client.limited_jobs_for("Other").is_empty());
    }
}
