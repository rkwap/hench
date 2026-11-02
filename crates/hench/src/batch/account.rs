//! Batch accounting of a member's outcome, before the outcome write (`BATCH_SUCCEEDED`,
//! `BATCH_SKIPPED`, `BATCH_FAILED`; `docs/design/batches.md` §2.5.4, §2.6.1), and the failure
//! summaries kept in a batch's `:failed`.
//!
//! The accounting runs before the outcome is written (acknowledgement, retry set, morgue): if
//! the process dies between the two, the job runs again and its accounting is then a no-op,
//! whereas the reverse order could acknowledge a job its batch never hears of. Each script is
//! keyed by jid and guarded by membership in `:pending`, so retries, recovered copies, lost
//! replies and basic fetch are all counted once.

use std::time::Duration;

use serde::Serialize;

use crate::batch::scripts::{self, Accounted, Outcome};
use crate::batch::{Accounting, BatchMember, Report};
use crate::config::ErrorContext;
use crate::error::JobError;
use crate::launcher::Engine;
use crate::payload::{JobPayload, now_millis};
use crate::pro::{Classified, Fetched};
use crate::processor::is_unavailable;
use crate::retry::Verdict;

/// Characters of an error message a failure summary keeps.
const SUMMARY_MESSAGE_CHARS: usize = 200;

/// Attempts of an accounting script that fails with an error other than Redis being
/// unavailable (which is retried until it comes back, or until a hard shutdown).
const ATTEMPTS: u32 = 3;

/// First and largest pause between attempts.
const MIN_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(5);

/// Why a member failed: the `reason` of its failure summary (`state` is `retry` for
/// [`Reason::Retry`], `dead` for every other reason).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Reason {
    /// It will be retried.
    Retry,
    /// Retries exhausted (or `RetryIn::Kill`): in the morgue.
    Morgue,
    /// Retries disabled (`retry: false`).
    NoRetry,
    /// Discarded (`dead: false`, `RetryIn::Discard`, `Exhausted::Discard`).
    Discarded,
    /// Deleted through the API.
    Deleted,
    /// Killed through the API.
    Killed,
}

impl Reason {
    /// The name stored in summaries.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Reason::Retry => "retry",
            Reason::Morgue => "morgue",
            Reason::NoRetry => "no_retry",
            Reason::Discarded => "discarded",
            Reason::Deleted => "deleted",
            Reason::Killed => "killed",
        }
    }
}

/// The summary of a failure kept in a batch's `:failed` (no backtrace: the job in the retry
/// set or the morgue has the details).
#[derive(Serialize)]
struct Summary<'a> {
    class: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_class: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    state: &'static str,
    reason: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    retry_count: Option<u32>,
    at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_at: Option<f64>,
}

/// The failure summary of `job` (JSON): its class, the error (`error`, or the job's error
/// fields; the message cut at 200 characters), the state and reason, the retry count, when
/// (`now`, ms) and, for a retry, when it runs again (`next_at`, epoch seconds).
pub(crate) fn summary(
    job: &JobPayload,
    reason: Reason,
    error: Option<&JobError>,
    next_at: Option<f64>,
    now: i64,
) -> String {
    let (error_class, message) = match error {
        Some(error) => (Some(error.class()), Some(error.message())),
        None => (job.error_class.as_deref(), job.error_message.clone()),
    };
    let summary = Summary {
        class: &job.class,
        error_class,
        error: message.map(|message| message.chars().take(SUMMARY_MESSAGE_CHARS).collect()),
        state: if reason == Reason::Retry {
            "retry"
        } else {
            "dead"
        },
        reason: reason.as_str(),
        retry_count: job.retry_count,
        at: now,
        next_at: next_at.filter(|_| reason == Reason::Retry),
    };
    serde_json::to_string(&summary).unwrap_or_else(|_| "{}".to_owned())
}

/// The outcome of a failed member, from the retry handler's verdict, at `now` (ms). A job that
/// is not retried (`retry: false`) is dropped as fetched, without error fields: its summary
/// names the error the verdict carries.
pub(crate) fn failure(verdict: &Verdict, now: i64) -> Outcome {
    let (job, reason, next_at, error) = match verdict {
        Verdict::Retry { at, job } => (job, Reason::Retry, Some(*at), None),
        Verdict::Kill { job } => (job, Reason::Morgue, None, None),
        Verdict::Discard { job } => (job, Reason::Discarded, None, None),
        Verdict::NoRetry {
            job,
            error_class,
            error_message,
        } => (
            job,
            Reason::NoRetry,
            None,
            Some(JobError::new(error_class.clone(), error_message.clone())),
        ),
    };
    Outcome::Failed {
        retry: reason == Reason::Retry,
        summary: summary(job, reason, error.as_ref(), next_at, now),
    }
}

/// Records the outcome of a batch member (`fetched.member`) in its batch, before the outcome
/// write. Errors are retried while Redis is unavailable (until a hard shutdown) and a few times
/// otherwise, then reported to the error handlers (`could not record a job in its batch`) and
/// never returned: the outcome is written anyway, so that a broken batch never makes a job run
/// forever (its batch then shows the member as pending, and an operator can skip it).
///
/// For [`Classified::Skipped`] the script confirms the cancellation;
/// [`Accounting::NotCancelled`] tells the processor to handle the job's `batch_cancelled` error
/// as a failure.
pub(crate) async fn before_outcome(
    engine: &Engine,
    fetched: &Fetched,
    classified: &Classified,
) -> Accounting {
    let Some(member) = &fetched.member else {
        return Accounting::Nothing;
    };
    let (outcome, job) = match classified {
        Classified::Success => (Outcome::Succeeded, None),
        Classified::Skipped => (Outcome::Skipped { cancelled: true }, None),
        Classified::Failure(verdict) => {
            (failure(verdict, now_millis()), Some(verdict_job(verdict)))
        }
        _ => return Accounting::Nothing,
    };
    match record(engine, member, &outcome, job).await {
        Some(accounted) if accounted.not_cancelled => Accounting::NotCancelled,
        Some(accounted) => {
            if accounted.applied {
                for event in &accounted.fired {
                    tracing::info!(bid = %member.bid, event = %event, "batch event fired");
                }
            }
            Accounting::Recorded(Report {
                bid: member.bid.clone(),
                dispatch: accounted.dispatch,
                refreshed: accounted.refreshed,
            })
        }
        None => Accounting::Nothing,
    }
}

/// The job a verdict holds.
fn verdict_job(verdict: &Verdict) -> &JobPayload {
    match verdict {
        Verdict::Retry { job, .. }
        | Verdict::Kill { job }
        | Verdict::Discard { job }
        | Verdict::NoRetry { job, .. } => job,
    }
}

/// Runs the accounting script of `outcome` until it answers (see [`before_outcome`]); `None`
/// when it gave up (reported).
async fn record(
    engine: &Engine,
    member: &BatchMember,
    outcome: &Outcome,
    job: Option<&JobPayload>,
) -> Option<Accounted> {
    let mut pause = MIN_BACKOFF;
    let mut failures = 0;
    loop {
        let error = match scripts::account(&engine.redis, &member.bid, &member.jid, outcome).await {
            Ok(accounted) => return Some(accounted),
            Err(error) => error,
        };
        failures += 1;
        if engine.hard.is_cancelled() || (!is_unavailable(&error) && failures >= ATTEMPTS) {
            let error = error
                .into_job_error()
                .context(format!("batch {}, member {}", member.bid, member.jid));
            let mut context = ErrorContext::new("could not record a job in its batch");
            if let Some(job) = job {
                context = context.with_job(job);
            }
            engine.config.handle_error(&error, &context);
            tracing::warn!(
                bid = %member.bid,
                jid = %member.jid,
                "gave up recording a job in its batch; its batch shows it as pending"
            );
            return None;
        }
        tracing::warn!(
            %error,
            failures,
            bid = %member.bid,
            "could not record a job in its batch; retrying"
        );
        let _ = engine
            .hard
            .run_until_cancelled(tokio::time::sleep(pause))
            .await;
        pause = (pause * 2).min(MAX_BACKOFF);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    #[test]
    fn summaries_cut_messages_on_a_character_boundary() {
        let mut job = JobPayload::new("ImportRow", vec![]);
        job.retry_count = Some(3);
        let long = "é".repeat(300);
        let error = JobError::msg(long);
        let retried: Value =
            serde_json::from_str(&summary(&job, Reason::Retry, Some(&error), Some(12.5), 7))
                .unwrap();
        assert_eq!(retried["class"], "ImportRow");
        assert_eq!(retried["error"].as_str().unwrap().chars().count(), 200);
        assert_eq!(retried["state"], "retry");
        assert_eq!(retried["reason"], "retry");
        assert_eq!(retried["retry_count"], 3);
        assert_eq!(retried["at"], 7);
        assert_eq!(retried["next_at"], 12.5);
        // `next_at` only for retries; the job's own error fields without an error.
        job.error_class = Some("std::io::error::Error".to_owned());
        job.error_message = Some("disk full".to_owned());
        let dead: Value =
            serde_json::from_str(&summary(&job, Reason::Killed, None, Some(12.5), 7)).unwrap();
        assert_eq!(
            dead,
            json!({
                "class": "ImportRow", "error_class": "std::io::error::Error",
                "error": "disk full", "state": "dead", "reason": "killed", "retry_count": 3,
                "at": 7
            })
        );
    }

    #[test]
    fn verdicts_give_their_reasons() {
        let job = JobPayload::new("A", vec![]);
        let cases = [
            (
                Verdict::Retry {
                    at: 10.0,
                    job: job.clone(),
                },
                true,
                "retry",
            ),
            (Verdict::Kill { job: job.clone() }, false, "morgue"),
            (Verdict::Discard { job: job.clone() }, false, "discarded"),
            (
                Verdict::NoRetry {
                    job,
                    error_class: "Timeout".to_owned(),
                    error_message: "too slow".to_owned(),
                },
                false,
                "no_retry",
            ),
        ];
        for (verdict, expected_retry, reason) in cases {
            let Outcome::Failed { retry, summary } = failure(&verdict, 1) else {
                panic!("a failure");
            };
            assert_eq!(retry, expected_retry);
            let summary: Value = serde_json::from_str(&summary).unwrap();
            assert_eq!(summary["reason"], reason);
            assert_eq!(summary.get("next_at").is_some(), expected_retry);
            if reason == "no_retry" {
                // Dropped as fetched: the summary names the verdict's error.
                assert_eq!(summary["error_class"], "Timeout");
                assert_eq!(summary["error"], "too slow");
            }
        }
    }
}
