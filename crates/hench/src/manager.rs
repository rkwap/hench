//! Managers: one per capsule, owning its processors, quiet/stop and hard shutdown.
//! See `docs/DESIGN.md` §5.
//!
//! A manager starts `concurrency` processors sharing the capsule's [`Fetcher`]. Quieting is
//! process-wide (the engine's quiet token): processors finish their current job and exit. On
//! stop the manager waits for them until the shutdown deadline, then interrupts the jobs still
//! running (hard shutdown) and pushes them back to their queues: jobs run at least once, and
//! it is better to run one twice than to lose it.
//
// LANE: engine — this module is owned by the engine lane.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::time::Instant;
use tokio_util::task::TaskTracker;

use crate::config::CapsuleConfig;
use crate::fetch::{Fetcher, UnitOfWork};
use crate::launcher::{CLEANUP_TIMEOUT, Engine, lock};
use crate::processor::{Processor, Slot};

/// How long a hard shutdown waits for interrupted processors to exit, on top of the fetch
/// timeout (a processor blocked in a fetch finishes it first, so that no job is dropped).
const HARD_SHUTDOWN_GRACE: Duration = Duration::from_secs(3);

/// Interrupted jobs requeued per call during a hard shutdown.
const REQUEUE_CHUNK: usize = 100;

/// The processors of one capsule.
pub(crate) struct Manager {
    capsule: String,
    fetcher: Fetcher,
    tracker: TaskTracker,
    slots: Vec<Slot>,
    /// The work-state id of each slot's processor (the permits the engine holds for a job are
    /// keyed by it).
    tids: Vec<String>,
}

impl Manager {
    /// Starts the processors of `capsule`.
    pub(crate) fn start(engine: &Arc<Engine>, capsule: &CapsuleConfig) -> Self {
        let fetcher = Fetcher::new(
            engine.config.fetch,
            engine.redis.clone(),
            engine.identity.clone(),
            capsule,
        )
        .with_timeout(engine.config.fetch_timeout)
        .with_queue_limits(engine.queue_limits.clone());
        let tracker = TaskTracker::new();
        let slots: Vec<Slot> = (0..capsule.concurrency).map(|_| Slot::default()).collect();
        let tids: Vec<String> = slots.iter().map(|_| next_tid()).collect();
        for (index, (slot, tid)) in slots.iter().zip(&tids).enumerate() {
            let processor = Processor::new(
                engine.clone(),
                fetcher.clone(),
                index,
                tid.clone(),
                slot.clone(),
            );
            let handle = tracker.spawn(processor.run());
            engine.track(handle.abort_handle());
        }
        tracker.close();
        tracing::debug!(
            capsule = %capsule.name,
            concurrency = capsule.concurrency,
            queues = ?capsule.queues,
            "started processors"
        );
        Self {
            capsule: capsule.name.clone(),
            fetcher,
            tracker,
            slots,
            tids,
        }
    }

    /// Number of processors still running.
    pub(crate) fn running(&self) -> usize {
        self.tracker.len()
    }

    /// Stops the capsule. The engine must already be quiet. Waits for the processors to finish
    /// their jobs until `deadline` (or until a forced shutdown); then interrupts the remaining
    /// jobs and requeues them.
    pub(crate) async fn stop(&self, engine: &Engine, deadline: Instant) {
        if self.tracker.is_empty() {
            return;
        }
        let busy = self.busy();
        if busy > 0 {
            tracing::info!(capsule = %self.capsule, busy, "waiting for running jobs to finish");
        }
        let finished = tokio::select! {
            () = self.tracker.wait() => true,
            () = tokio::time::sleep_until(deadline) => false,
            () = engine.force.cancelled() => false,
        };
        if finished {
            return;
        }
        self.hard_shutdown(engine).await;
    }

    /// Interrupts the running jobs and pushes them back to their queues.
    async fn hard_shutdown(&self, engine: &Engine) {
        let busy = self.busy();
        if busy > 0 {
            tracing::warn!(
                capsule = %self.capsule,
                busy,
                "jobs are still running at the shutdown deadline; interrupting them"
            );
        }
        engine.hard.cancel();
        let grace = HARD_SHUTDOWN_GRACE.saturating_add(engine.config.fetch_timeout);
        let exited = tokio::time::timeout(grace, self.tracker.wait())
            .await
            .is_ok();
        // Whatever is left in a slot did not finish.
        let mut interrupted = Vec::new();
        let unfinished: Vec<UnitOfWork> = self
            .slots
            .iter()
            .zip(&self.tids)
            .filter_map(|(slot, tid)| {
                let work = lock(slot).take()?;
                interrupted.push(tid.clone());
                Some(work)
            })
            .collect();
        // In chunks, so that a failure is reported for exactly the jobs it concerns.
        for chunk in unfinished.chunks(REQUEUE_CHUNK) {
            let requeue = self.fetcher.bulk_requeue(chunk);
            match tokio::time::timeout(CLEANUP_TIMEOUT, requeue).await {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => self.report_unrequeued(chunk, &error.to_string(), true),
                // The commands may still run: the jobs may or may not be back.
                Err(_) => self.report_unrequeued(chunk, "Redis did not answer", false),
            }
        }
        // The permits the engine holds for the interrupted jobs (their leases end anyway).
        if !interrupted.is_empty() {
            let release = crate::limit::engine::release_slots(engine, &interrupted);
            if tokio::time::timeout(CLEANUP_TIMEOUT, release)
                .await
                .is_err()
            {
                tracing::warn!(
                    capsule = %self.capsule,
                    "could not release the throttle permits of interrupted jobs in time; their \
                     leases end by themselves"
                );
            }
        }
        if !exited {
            tracing::warn!(
                capsule = %self.capsule,
                processors = self.tracker.len(),
                "some jobs ignore cancellation (blocking code?) and are still running"
            );
        }
    }

    /// Reports interrupted jobs that could not be requeued (`certain`: the requeue failed;
    /// otherwise it timed out and may still have happened).
    fn report_unrequeued(&self, unfinished: &[UnitOfWork], error: &str, certain: bool) {
        tracing::error!(
            capsule = %self.capsule,
            error,
            jobs = unfinished.len(),
            "could not confirm that interrupted jobs are back in their queues"
        );
        match self.fetcher.strategy() {
            crate::FetchStrategy::Basic => {
                let state = if certain { "lost" } else { "possibly lost" };
                for work in unfinished {
                    tracing::error!(job = %work.job_str(), state, "interrupted job");
                }
            }
            crate::FetchStrategy::Reliable => tracing::warn!(
                "jobs that are not back in their queues stay in the in-progress list and are \
                 recovered later"
            ),
        }
    }

    /// Number of processors running a job.
    fn busy(&self) -> usize {
        self.slots
            .iter()
            .filter(|slot| lock(slot).is_some())
            .count()
    }
}

/// A new worker slot id, unique in this process: base-36 text like Sidekiq's thread ids.
fn next_tid() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let seed = (u64::from(std::process::id()) << 24) ^ 0x5bd1_e995;
    to_base36(seed.wrapping_add(NEXT.fetch_add(1, Ordering::Relaxed)))
}

fn to_base36(mut value: u64) -> String {
    const DIGITS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut out = Vec::new();
    loop {
        // The index is below 36.
        out.push(DIGITS[(value % 36) as usize]);
        value /= 36;
        if value == 0 {
            break;
        }
    }
    out.reverse();
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tids_are_unique() {
        let tids: std::collections::HashSet<String> = (0..1_000).map(|_| next_tid()).collect();
        assert_eq!(tids.len(), 1_000);
        assert!(
            tids.iter()
                .all(|tid| tid.chars().all(|c| c.is_ascii_alphanumeric()))
        );
    }

    #[test]
    fn base36() {
        assert_eq!(to_base36(0), "0");
        assert_eq!(to_base36(35), "z");
        assert_eq!(to_base36(36), "10");
        assert_eq!(to_base36(u64::MAX), "3w5e11264sgsf");
    }
}
