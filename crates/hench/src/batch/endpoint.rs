//! The batch steps of the processor before a member runs: the endpoint's cancellation check
//! (one `HGET` per member execution) and the check of a throttled member's batch
//! (`docs/design/batches.md` §2.6.2; `docs/design/pro-features.md` §3.2).

use crate::batch::{BatchMember, model};
use crate::client::{Client, ClientMode};
use crate::error::{JobError, Result};
use crate::job::JobContext;
use crate::keys;
use crate::launcher::Engine;
use crate::pro::Fetched;

/// `Err(JobError::batch_cancelled())` when the job (as fetched: `ctx.payload()`) is a member
/// of a cancelled batch; the processor then counts it as skipped (once its batch confirms the
/// cancellation) and acknowledges it. Runs before the release of a `start` unique lock, so that
/// a skipped member's lock is released by its acknowledgement. A failed read is logged and the
/// job runs: only a positive answer skips a job.
pub(crate) async fn check(ctx: &JobContext) -> std::result::Result<(), JobError> {
    let Some(member) = BatchMember::from_payload(ctx.payload()) else {
        return Ok(());
    };
    match is_cancelled(ctx.client(), &member.bid).await {
        Ok(true) => {
            tracing::debug!(bid = %member.bid, "the job's batch was cancelled; skipping it");
            Err(JobError::batch_cancelled())
        }
        Ok(false) => Ok(()),
        Err(error) => {
            tracing::warn!(
                %error,
                bid = %member.bid,
                "could not check whether the job's batch was cancelled; running it"
            );
            Ok(())
        }
    }
}

/// Whether the batch of a member refused by the throttle gate was cancelled (it is then
/// skipped instead of parked). Errors are logged and answer `false` (the job is parked and
/// checked again at its next refusal or run).
pub(crate) async fn member_cancelled(engine: &Engine, fetched: &Fetched) -> bool {
    let Some(member) = &fetched.member else {
        return false;
    };
    match is_cancelled(&engine.client, &member.bid).await {
        Ok(cancelled) => cancelled,
        Err(error) => {
            tracing::warn!(
                %error,
                bid = %member.bid,
                "could not check whether a throttled job's batch was cancelled"
            );
            false
        }
    }
}

/// Whether batch `bid` was cancelled: `HGET batch:<bid> cancelled_at` for a Redis client, the
/// in-memory table of a testing client.
pub(crate) async fn is_cancelled(client: &Client, bid: &str) -> Result<bool> {
    match client.mode() {
        ClientMode::Fake(_) | ClientMode::Inline(_) => Ok(model::is_cancelled(client, bid)),
        ClientMode::Redis | ClientMode::Collect(_) => {
            let hash = keys::batch(bid);
            let hash = &hash;
            let cancelled: Option<String> = client
                .redis()
                .with_retry(|mut conn| async move {
                    redis::cmd("HGET")
                        .arg(hash)
                        .arg("cancelled_at")
                        .query_async(&mut conn)
                        .await
                })
                .await?;
            Ok(cancelled.is_some())
        }
    }
}
