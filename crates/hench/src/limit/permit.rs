//! [`Permit`]: the proof of a grant (`docs/design/ratelimit.md` §2.4.2).
//!
//! A permit of concurrency limits holds leases: a task renews them every third of their length
//! while the permit lives, and they are given back when it is released or dropped (a crashed
//! process's leases end by themselves). Permits of rate limits are receipts; a points permit can
//! correct its cost.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::{Mutex as AsyncMutex, Notify};
use tokio_util::sync::CancellationToken;

use crate::error::Result;
use crate::limit::limiter::Request;
use crate::limit::rule::Rule;
use crate::limit::scripts::{LeaseRef, ReleaseOutcome, RenewOutcome};
use crate::limit::target::Store;
use crate::limit::throttle::warn_now;
use crate::limit::wakeups;

/// Proof of a grant. Bind it for as long as the resource is used (`let _permit = ...`):
/// `let _ = ...` drops it at once, which gives its concurrency slots back immediately (permits
/// of rate limits are receipts: dropping them changes nothing).
///
/// Dropping a permit gives its concurrency slots back in a spawned task (when a tokio runtime
/// is running; otherwise their leases end by themselves); [`Permit::release`] does it now and
/// reports errors.
#[must_use = "a permit gives its concurrency slots back when dropped; bind it with `let _permit = ...`"]
pub struct Permit {
    held: Option<Held>,
    waited: Duration,
    lost: Arc<LostSignal>,
}

/// What a permit holds.
struct Held {
    store: Store,
    /// The concurrency leases.
    leases: Vec<LeaseRef>,
    /// The points limits and the cost charged to each.
    points: Vec<(Rule, u32)>,
    /// The counters' expiry (`None`: no bookkeeping).
    stats_ttl: Option<Duration>,
    /// Stops the renewal task.
    renewal: Option<CancellationToken>,
    /// Held by a renewal in flight: a release waits for it, so that a renewal cannot take back a
    /// lease just released.
    turn: Arc<AsyncMutex<()>>,
}

/// Whether the leases were lost, and the waiters of [`Permit::lost`].
#[derive(Default)]
struct LostSignal {
    lost: AtomicBool,
    notify: Notify,
}

impl LostSignal {
    fn set(&self) {
        self.lost.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }
}

impl Permit {
    /// A permit that holds nothing (an unlimited limiter, or a grant without limiting).
    pub(crate) fn unlimited() -> Permit {
        Permit {
            held: None,
            waited: Duration::ZERO,
            lost: Arc::default(),
        }
    }

    /// A permit for the grant of `request` at `acquired_ms` (the store's clock): it renews the
    /// concurrency leases until it is released.
    pub(crate) fn granted(
        store: Store,
        request: &Request<'_>,
        stats_ttl: Option<Duration>,
        acquired_ms: i64,
        waited: Duration,
    ) -> Permit {
        let leases = request.leases(acquired_ms);
        let lease = request.lease;
        let points: Vec<(Rule, u32)> = request
            .rules
            .iter()
            .filter(|rule| rule.is_points())
            .map(|rule| (rule.clone(), request.cost))
            .collect();
        let lost: Arc<LostSignal> = Arc::default();
        if leases.is_empty() && points.is_empty() {
            return Permit {
                held: None,
                waited,
                lost,
            };
        }
        let turn = Arc::new(AsyncMutex::new(()));
        let renewal = if leases.is_empty() {
            None
        } else {
            spawn_renewal(
                store.clone(),
                leases.clone(),
                lease,
                Arc::clone(&lost),
                Arc::clone(&turn),
            )
        };
        Permit {
            held: Some(Held {
                store,
                leases,
                points,
                stats_ttl,
                renewal,
                turn,
            }),
            waited,
            lost,
        }
    }

    /// Gives the concurrency slots back now, reporting Redis errors (the leases then end by
    /// themselves). Permits of rate limits have nothing to give back.
    pub async fn release(mut self) -> Result<()> {
        match self.held.take() {
            Some(held) => held.release().await,
            None => Ok(()),
        }
    }

    /// Points limits: replaces the cost charged (the estimate given to
    /// [`Limiter::acquire_n`](crate::limit::Limiter::acquire_n)) by the actual cost, refunding
    /// or charging the difference. A refund never takes the budget above full; an extra charge
    /// may take it below empty (debt that later callers wait out). Does nothing for other
    /// limits.
    pub async fn points_used(&mut self, actual: u32) -> Result<()> {
        let Some(held) = self.held.as_mut() else {
            return Ok(());
        };
        for (rule, charged) in &mut held.points {
            let delta = i64::from(actual) - i64::from(*charged);
            if delta != 0 {
                held.store.adjust(rule, delta, held.stats_ttl).await?;
                *charged = actual;
            }
        }
        Ok(())
    }

    /// Concurrency: whether the leases were lost (not renewable, and no free slot to take them
    /// back: another holder may have the slot). The holder keeps running; a long holder may want
    /// to stop.
    pub fn is_lost(&self) -> bool {
        self.lost.lost.load(Ordering::SeqCst)
    }

    /// Resolves once the leases are lost (see [`Permit::is_lost`]); never for permits without
    /// concurrency limits.
    pub async fn lost(&self) {
        loop {
            let notified = self.lost.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_lost() {
                return;
            }
            notified.await;
        }
    }

    /// How long the acquisition waited.
    pub fn waited(&self) -> Duration {
        self.waited
    }
}

impl Held {
    /// Stops the renewals, then gives the leases back.
    async fn release(self) -> Result<()> {
        if let Some(renewal) = &self.renewal {
            renewal.cancel();
        }
        if self.leases.is_empty() {
            return Ok(());
        }
        let _turn = self.turn.lock().await;
        let released = self.store.release(&self.leases).await;
        for lease in &self.leases {
            wakeups::notify(&lease.state);
        }
        let released = released?;
        for (lease, outcome) in self.leases.iter().zip(released) {
            if outcome != ReleaseOutcome::Released && warn_now(&format!("late:{}", lease.state)) {
                tracing::warn!(
                    limit = %lease.state,
                    "a concurrency permit was given back after its lease ended: another holder \
                     may have had its slot (an overage); renewals did not keep up"
                );
            }
        }
        Ok(())
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        let Some(held) = self.held.take() else {
            return;
        };
        if let Some(renewal) = &held.renewal {
            renewal.cancel();
        }
        if held.leases.is_empty() {
            return;
        }
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(async move {
                    if let Err(error) = held.release().await {
                        tracing::debug!(
                            %error,
                            "could not give a concurrency permit back; its lease ends by itself"
                        );
                    }
                });
            }
            Err(_) => tracing::debug!(
                "a concurrency permit was dropped outside a tokio runtime; its lease ends by itself"
            ),
        }
    }
}

impl fmt::Debug for Permit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let leases: Vec<&str> = self
            .held
            .iter()
            .flat_map(|held| held.leases.iter().map(|lease| lease.state.as_str()))
            .collect();
        f.debug_struct("Permit")
            .field("leases", &leases)
            .field("waited", &self.waited)
            .field("lost", &self.is_lost())
            .finish_non_exhaustive()
    }
}

/// Renews `leases` every third of `lease` until `stop` is cancelled (the permit is released or
/// dropped). A lease that cannot be renewed and finds no free slot is lost; so are the leases of
/// a permit that could not reach Redis for a whole lease.
fn spawn_renewal(
    store: Store,
    leases: Vec<LeaseRef>,
    lease: Duration,
    lost: Arc<LostSignal>,
    turn: Arc<AsyncMutex<()>>,
) -> Option<CancellationToken> {
    let handle = tokio::runtime::Handle::try_current().ok()?;
    let stop = CancellationToken::new();
    let task_stop = stop.clone();
    handle.spawn(async move {
        let every = (lease / 3).max(Duration::from_millis(100));
        let mut live = leases;
        let mut renewed_at = tokio::time::Instant::now();
        loop {
            if task_stop
                .run_until_cancelled(tokio::time::sleep(every))
                .await
                .is_none()
            {
                return;
            }
            let _turn = turn.lock().await;
            if task_stop.is_cancelled() {
                return;
            }
            match store.renew(&live).await {
                Ok(results) => {
                    renewed_at = tokio::time::Instant::now();
                    let mut kept = Vec::with_capacity(live.len());
                    for (held, result) in live.into_iter().zip(results) {
                        match result {
                            RenewOutcome::Renewed => kept.push(held),
                            RenewOutcome::Restored => {
                                if warn_now(&format!("restored:{}", held.state)) {
                                    tracing::warn!(
                                        limit = %held.state,
                                        "a concurrency lease had ended and was taken back"
                                    );
                                }
                                kept.push(held);
                            }
                            RenewOutcome::Lost => {
                                if warn_now(&format!("lost:{}", held.state)) {
                                    tracing::warn!(
                                        limit = %held.state,
                                        "a concurrency lease was lost: its slot went to another \
                                         holder while this one still runs (an overage)"
                                    );
                                }
                                lost.set();
                            }
                        }
                    }
                    live = kept;
                    if live.is_empty() {
                        return;
                    }
                }
                Err(error) => {
                    tracing::debug!(%error, "could not renew a concurrency permit; retrying");
                    if renewed_at.elapsed() >= lease {
                        tracing::warn!(
                            %error,
                            "a concurrency permit could not be renewed for a whole lease: its \
                             slot is lost"
                        );
                        lost.set();
                        return;
                    }
                }
            }
        }
    });
    Some(stop)
}
