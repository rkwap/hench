//! Dispatch of batch events (`BATCH_DISPATCH`) and refresh of ancestors' expiries
//! (`BATCH_TOUCH`; `docs/design/batches.md` §2.5.5, §2.5.6, §2.6.1). After a member's outcome
//! write the processor dispatches the events its accounting found waiting; the API does the
//! same after a removal, a commit after creating an empty batch, and maintenance for whatever
//! was left behind. Every dispatch is guarded by the outbox: any number of processes may try
//! at the same time, the callbacks are pushed once.

use std::time::Duration;

use crate::batch::Report;
use crate::batch::scripts;
use crate::error::Result;
use crate::keys;
use crate::launcher::Engine;
use crate::redis::RedisPool;

/// How long the processor's dispatch after an outcome may take; maintenance finishes a
/// dispatch left undone after the grace period.
const DISPATCH_TIMEOUT: Duration = Duration::from_secs(5);

/// Most levels of a tree walked up by one dispatch or refresh (a guard against a cycle in
/// corrupted data: parents are always created before their children).
const MAX_DEPTH: usize = 100;

/// Dispatches the events `report` found waiting in the outbox, and refreshes the expiry of the
/// batch's ancestors when its own was refreshed. Informational for the job: failures are
/// logged and maintenance dispatches the events after the grace period.
pub(crate) async fn after_outcome(engine: &Engine, report: Option<Report>) {
    if let Some(report) = report {
        settle(&engine.redis, &report).await;
    }
}

/// The work after an accounting (processor, API removals): dispatch (bounded), then the
/// refresh of the ancestors, best effort.
pub(crate) async fn settle(redis: &RedisPool, report: &Report) {
    if report.dispatch {
        match tokio::time::timeout(DISPATCH_TIMEOUT, dispatch_chain(redis, &report.bid)).await {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => tracing::warn!(
                %error,
                bid = %report.bid,
                "could not dispatch batch callbacks; maintenance will"
            ),
            Err(_) => tracing::warn!(
                bid = %report.bid,
                "dispatching batch callbacks took too long; maintenance will finish"
            ),
        }
    }
    if report.refreshed {
        refresh_ancestors(redis, &report.bid).await;
    }
}

/// Dispatches the events of batch `bid`, then those of its ancestors while the dispatch leaves
/// them waiting in the outbox (a child's events update its parent). Returns how many batches
/// had events dispatched.
pub(crate) async fn dispatch_chain(redis: &RedisPool, bid: &str) -> Result<usize> {
    let mut bid = bid.to_owned();
    let mut dispatched = 0;
    for _ in 0..MAX_DEPTH {
        let target = scripts::dispatch_target(redis, &bid).await?;
        let done = scripts::dispatch(redis, &bid, &target).await?;
        if done.dispatched {
            dispatched += 1;
            tracing::info!(
                bid = %bid,
                events = %done.events.join(","),
                callbacks = done.callbacks,
                parent = done.parent.as_deref().unwrap_or(""),
                "batch callbacks dispatched"
            );
        }
        if let Some(parent) = &done.parent {
            for event in &done.parent_fired {
                tracing::info!(bid = %parent, event = %event, "batch event fired");
            }
            if done.parent_refreshed {
                refresh_ancestors(redis, parent).await;
            }
        }
        match done.parent {
            Some(parent) if done.parent_dispatch => bid = parent,
            _ => break,
        }
    }
    Ok(dispatched)
}

/// Refreshes the expiries of the ancestors of batch `bid` (`BATCH_TOUCH` up the tree, one call
/// per level), so that a parent never expires while a child is still active: each ancestor gets
/// at least the lifetime of the level below it (a reopened child may have gained a member
/// scheduled beyond its parent's expiry). Best effort: errors are logged at debug (the next
/// refresh tries again).
pub(crate) async fn refresh_ancestors(redis: &RedisPool, bid: &str) {
    let (mut next, mut floor) = match parent_and_lifetime(redis, bid).await {
        Ok(found) => found,
        Err(error) => {
            tracing::debug!(%error, bid, "could not read a batch's parent to refresh it");
            return;
        }
    };
    for _ in 0..MAX_DEPTH {
        let Some(bid) = next else {
            return;
        };
        match scripts::touch(redis, &bid, floor).await {
            Ok(touched) => {
                next = touched.parent;
                floor = touched.ttl_ms;
            }
            Err(error) => {
                tracing::debug!(%error, bid = %bid, "could not refresh a batch's expiry");
                return;
            }
        }
    }
}

/// The parent of batch `bid`, if it has one, and its remaining lifetime in ms (0 when it has
/// none: gone, or without expiry).
async fn parent_and_lifetime(redis: &RedisPool, bid: &str) -> Result<(Option<String>, u64)> {
    let hash = keys::batch(bid);
    let hash = &hash;
    let (parent, pttl): (Option<String>, i64) = redis
        .with_retry(|mut conn| async move {
            redis::pipe()
                .cmd("HGET")
                .arg(hash)
                .arg("parent")
                .cmd("PTTL")
                .arg(hash)
                .query_async(&mut conn)
                .await
        })
        .await?;
    Ok((parent, u64::try_from(pttl).unwrap_or(0)))
}
