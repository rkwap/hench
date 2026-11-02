//! Batch maintenance (`docs/design/batches.md` §2.6.4): dispatches the events left in the
//! outbox (a process that fired an event and died before dispatching it), and trims the index
//! of expired batches. Every process runs the loop; a lock lets one of them work per interval,
//! the others only send a `SET NX`. Runs until the process is quiet.

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use futures::future::BoxFuture;

use crate::batch::dispatch::dispatch_chain;
use crate::config::ErrorContext;
use crate::error::{JobError, Result};
use crate::keys;
use crate::launcher::Engine;
use crate::processor::is_unavailable;
use crate::redis::RedisPool;

/// Outbox entries dispatched per round, at most.
const DISPATCH_PER_ROUND: usize = 100;

/// How far back an outbox entry whose dispatch failed goes (Redis time): it is tried again that
/// much later (plus the grace period), behind the entries that are due, so that batches that
/// cannot be dispatched never keep maintenance from the others.
const RETRY_FAILED_AFTER: Duration = Duration::from_secs(60);

/// Pause after the loop itself panicked (a bug in hench), before it goes on.
const CRASH_PAUSE: Duration = Duration::from_secs(1);

/// The maintenance task, or `None` when this process runs none (`batch_maintenance(false)`).
pub(crate) fn task(engine: &Arc<Engine>) -> Option<BoxFuture<'static, ()>> {
    engine
        .config
        .batch_maintenance
        .then(|| run(Arc::clone(engine)).boxed())
}

/// Runs a round every `batch_maintenance_interval` (± 20 %) until the process is quiet.
async fn run(engine: Arc<Engine>) {
    let config = &engine.config;
    loop {
        let jitter = 0.8 + 0.4 * fastrand::f64();
        let pause = config.batch_maintenance_interval.mul_f64(jitter);
        if engine
            .quiet
            .run_until_cancelled(tokio::time::sleep(pause))
            .await
            .is_none()
        {
            return;
        }
        let round = Round {
            redis: &engine.redis,
            identity: &engine.identity,
            interval: config.batch_maintenance_interval,
            grace: config.batch_dispatch_grace,
        };
        let done = engine
            .background
            .run_until_cancelled(AssertUnwindSafe(round.run()).catch_unwind())
            .await;
        match done {
            None => return,
            Some(Ok(Ok(report))) => {
                engine.batch_maintenance_outage.succeeded();
                if report.dispatched > 0 || report.trimmed > 0 || report.postponed > 0 {
                    tracing::debug!(
                        dispatched = report.dispatched,
                        trimmed = report.trimmed,
                        postponed = report.postponed,
                        "batch maintenance"
                    );
                }
            }
            Some(Ok(Err(error))) => engine.batch_maintenance_outage.failed(config, error),
            Some(Err(panic)) => {
                config.handle_error(
                    &JobError::panic(panic),
                    &ErrorContext::new("batch maintenance crashed; restarting it"),
                );
                let pause = tokio::time::sleep(CRASH_PAUSE);
                if engine.quiet.run_until_cancelled(pause).await.is_none() {
                    return;
                }
            }
        }
    }
}

/// One round of maintenance.
pub(crate) struct Round<'a> {
    /// The Redis of the batches.
    pub(crate) redis: &'a RedisPool,
    /// This process (the holder of the lock).
    pub(crate) identity: &'a str,
    /// The period of the rounds.
    pub(crate) interval: Duration,
    /// The age of an undispatched event before maintenance dispatches it.
    pub(crate) grace: Duration,
}

/// What a round did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct RoundReport {
    /// Another process held the lock: nothing was done.
    pub(crate) skipped: bool,
    /// Batches whose events were dispatched.
    pub(crate) dispatched: usize,
    /// Expired index entries removed.
    pub(crate) trimmed: u64,
    /// Outbox entries whose dispatch failed, put back a minute later.
    pub(crate) postponed: usize,
}

impl Round<'_> {
    /// Takes the lock (skips the round when another process holds it), dispatches the outbox
    /// entries older than the grace period (at most 100), then trims the index of the batches
    /// whose keys expired. Times are Redis's.
    pub(crate) async fn run(&self) -> Result<RoundReport> {
        if !self.lock().await? {
            return Ok(RoundReport {
                skipped: true,
                ..RoundReport::default()
            });
        }
        let now = self.redis_time().await?;
        let due_before = now - self.grace.as_secs_f64();
        let due: Vec<String> = self
            .redis
            .with_retry(|mut conn| async move {
                redis::cmd("ZRANGEBYSCORE")
                    .arg(keys::BATCH_EVENTS)
                    .arg("-inf")
                    .arg(format!("({due_before}"))
                    .arg("LIMIT")
                    .arg(0)
                    .arg(DISPATCH_PER_ROUND)
                    .query_async(&mut conn)
                    .await
            })
            .await?;
        let mut report = RoundReport::default();
        for bid in due {
            match dispatch_chain(self.redis, &bid).await {
                Ok(dispatched) => report.dispatched += dispatched,
                // Redis is down: the round ends; the next one tries again.
                Err(error) if is_unavailable(&error) => return Err(error),
                // A batch that cannot be dispatched (a callback queue of the wrong type,
                // unreadable templates) is reported and goes to the back of the outbox, tried
                // again a minute later: the next rounds reach the entries behind it.
                Err(error) => {
                    tracing::warn!(
                        %error,
                        bid = %bid,
                        "could not dispatch batch callbacks; trying again in a minute"
                    );
                    report.postponed += 1;
                    self.postpone(&bid, now).await?;
                }
            }
        }
        report.trimmed = self
            .redis
            .with_retry(|mut conn| async move {
                redis::cmd("ZREMRANGEBYSCORE")
                    .arg(keys::BATCHES)
                    .arg("-inf")
                    .arg(format!("({now}"))
                    .query_async(&mut conn)
                    .await
            })
            .await?;
        Ok(report)
    }

    /// Puts the outbox entry of batch `bid` back to [`RETRY_FAILED_AFTER`] after `now` (Redis
    /// time, seconds), if it is still there (`XX`: a dispatch that went through meanwhile
    /// removed it).
    async fn postpone(&self, bid: &str, now: f64) -> Result<()> {
        let score = now + RETRY_FAILED_AFTER.as_secs_f64();
        self.redis
            .with_retry(|mut conn| async move {
                redis::cmd("ZADD")
                    .arg(keys::BATCH_EVENTS)
                    .arg("XX")
                    .arg(score)
                    .arg(bid)
                    .query_async::<()>(&mut conn)
                    .await
            })
            .await
    }

    /// Takes the maintenance lock for 0.8 × the interval; `true` when this process holds it
    /// (also when a resent `SET NX` finds the lock it took itself).
    async fn lock(&self) -> Result<bool> {
        let ttl_ms = u64::try_from(self.interval.mul_f64(0.8).as_millis())
            .unwrap_or(u64::MAX)
            .max(1);
        let identity = self.identity;
        let taken: Option<String> = self
            .redis
            .with_retry(|mut conn| async move {
                redis::cmd("SET")
                    .arg(keys::BATCH_MAINTENANCE)
                    .arg(identity)
                    .arg("NX")
                    .arg("PX")
                    .arg(ttl_ms)
                    .query_async(&mut conn)
                    .await
            })
            .await?;
        if taken.is_some() {
            return Ok(true);
        }
        let holder: Option<String> = self
            .redis
            .with_retry(|mut conn| async move {
                redis::cmd("GET")
                    .arg(keys::BATCH_MAINTENANCE)
                    .query_async(&mut conn)
                    .await
            })
            .await?;
        Ok(holder.as_deref() == Some(identity))
    }

    /// Redis's clock, in epoch seconds (the scores of the index and the outbox are).
    async fn redis_time(&self) -> Result<f64> {
        let (secs, micros): (u64, u64) = self
            .redis
            .with_retry(|mut conn| async move { redis::cmd("TIME").query_async(&mut conn).await })
            .await?;
        // Epoch seconds fit an f64 exactly at this precision.
        #[allow(clippy::cast_precision_loss)]
        let now = secs as f64 + micros as f64 / 1e6;
        Ok(now)
    }
}
