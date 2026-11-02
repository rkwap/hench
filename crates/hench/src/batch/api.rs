//! The batch part of the API's removals: a deleted or killed member counts as dead in its
//! batch (`BATCH_FAILED` with reason `deleted`/`killed`) before it is removed, and the events
//! are dispatched after (`docs/design/batches.md` §2.9, §2.10; `pro-features.md` §3.3).
//!
//! Accounting first means that a crash between the two steps cannot strand the batch. The price
//! is a rare race: a delete that loses to a concurrent move of the same entry (the scheduler
//! enqueued it meanwhile) reports a death for a job that then runs, and its success repairs the
//! counts.

use crate::batch::account::{Reason, summary};
use crate::batch::dispatch::settle;
use crate::batch::scripts::{self, Outcome};
use crate::batch::{BatchMember, Report};
use crate::payload::{JobPayload, now_millis};
use crate::redis::RedisPool;

/// Why a member leaves the system through the API.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Removal {
    /// Deleted.
    Deleted,
    /// Killed (moved to the morgue).
    Killed,
}

/// Accounts the removal of `jobs` (members are counted dead) before the removal script runs.
/// Errors are logged: a removal never fails because of its batch.
pub(crate) async fn before_removal(
    redis: &RedisPool,
    jobs: &[JobPayload],
    removal: Removal,
) -> Vec<Report> {
    let reason = match removal {
        Removal::Deleted => Reason::Deleted,
        Removal::Killed => Reason::Killed,
    };
    let now = now_millis();
    let mut reports = Vec::new();
    for job in jobs {
        let Some(member) = BatchMember::from_payload(job) else {
            continue;
        };
        let outcome = Outcome::Failed {
            retry: false,
            summary: summary(job, reason, None, None, now),
        };
        match scripts::account(redis, &member.bid, &member.jid, &outcome).await {
            Ok(accounted) => {
                if accounted.applied {
                    tracing::info!(
                        bid = %member.bid,
                        jid = %member.jid,
                        reason = reason.as_str(),
                        "batch member removed; counted dead"
                    );
                }
                if accounted.dispatch || accounted.refreshed {
                    reports.push(Report {
                        bid: member.bid,
                        dispatch: accounted.dispatch,
                        refreshed: accounted.refreshed,
                    });
                }
            }
            Err(error) => tracing::warn!(
                %error,
                bid = %member.bid,
                jid = %member.jid,
                "could not count a removed job as dead in its batch; its batch shows it as \
                 pending"
            ),
        }
    }
    reports
}

/// Dispatches the events of `reports` after the removal.
pub(crate) async fn after_removal(redis: &RedisPool, reports: Vec<Report>) {
    for report in reports {
        settle(redis, &report).await;
    }
}
