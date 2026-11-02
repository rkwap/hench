//! The outcome of a periodic job's execution, recorded in its schedule (`PERIODIC_RECORD_RUN`,
//! spawned after the outcome write: `docs/design/periodic.md` §2.10). Informational: the
//! processor never waits for it, and an error is only debug-logged.

use std::time::Duration;

use crate::error::Result;
use crate::keys;
use crate::launcher::Engine;
use crate::payload::random_hex;
use crate::periodic::RunStatus;
use crate::periodic::lua::PERIODIC_RECORD_RUN;
use crate::pro::{Classified, Fetched};
use crate::redis::RedisPool;
use crate::retry::Verdict;

/// The longest error text recorded, in characters.
const MAX_ERROR_CHARS: usize = 500;

/// Records the execution of a job enqueued by a schedule (`fetched.periodic`, the job as
/// fetched): `ok`, `failed` (with the error), `interrupted` (an iterable job continues later)
/// or `rescheduled` (it ran and returned over a limiter's limit). Nothing is recorded for jobs
/// that did not run to an outcome: parked by a throttle before they started, skipped because
/// their batch was cancelled, requeued by a shutdown.
pub(crate) fn spawn(
    engine: &Engine,
    fetched: &Fetched,
    classified: &Classified,
    elapsed: Duration,
) {
    let Some(periodic) = &fetched.periodic else {
        return;
    };
    let (status, error) = match classified {
        Classified::Success => (RunStatus::Succeeded, String::new()),
        Classified::Interrupted => (RunStatus::Interrupted, String::new()),
        Classified::OverLimit(_) => (RunStatus::Rescheduled, String::new()),
        Classified::Failure(verdict) => (RunStatus::Failed, failure(verdict)),
        Classified::Skipped | Classified::Throttled(_) => return,
    };
    let redis = engine.redis.clone();
    let name = periodic.name.clone();
    let tick = periodic.tick;
    let jid = periodic.jid.clone();
    let duration = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
    tokio::spawn(async move {
        if let Err(error) = record(&redis, &name, tick, status, &error, duration, &jid).await {
            tracing::debug!(
                schedule = %name,
                %jid,
                %error,
                "could not record the run of a periodic job"
            );
        }
    });
}

/// The error of a failed run: `class: message`, at most 500 characters.
fn failure(verdict: &Verdict) -> String {
    let (class, message) = match verdict {
        Verdict::Retry { job, .. } | Verdict::Kill { job } | Verdict::Discard { job } => {
            (job.error_class.as_deref(), job.error_message.as_deref())
        }
        // `retry: false`: the job is dropped as fetched; the verdict carries its error.
        Verdict::NoRetry {
            error_class,
            error_message,
            ..
        } => (Some(error_class.as_str()), Some(error_message.as_str())),
    };
    let text = match (class, message) {
        (Some(class), Some(message)) => format!("{class}: {message}"),
        (Some(class), None) => class.to_owned(),
        (None, Some(message)) => message.to_owned(),
        (None, None) => "the job failed (its error went to the error handlers)".to_owned(),
    };
    text.chars().take(MAX_ERROR_CHARS).collect()
}

/// Sends `PERIODIC_RECORD_RUN` (`record_run.lua`): `true` when recorded. A token makes a resend
/// after a lost reply harmless (a failure is counted once).
pub(crate) async fn record(
    redis: &RedisPool,
    name: &str,
    tick_ms: i64,
    status: RunStatus,
    error: &str,
    duration_ms: u64,
    jid: &str,
) -> Result<bool> {
    let key = keys::periodic(name);
    let token = random_hex(8);
    let mut invocation = PERIODIC_RECORD_RUN.script().prepare_invoke();
    invocation
        .key(&key)
        .arg(tick_ms)
        .arg(status.as_str())
        .arg(error)
        .arg(duration_ms)
        .arg(jid)
        .arg(&token);
    let invocation = &invocation;
    let recorded: i64 = redis
        .with_retry(|mut conn| async move { invocation.invoke_async(&mut conn).await })
        .await?;
    Ok(recorded == 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::payload::JobPayload;

    #[test]
    fn failures_name_the_error() {
        let mut job = JobPayload::new("Report", vec![]);
        job.error_class = Some("Timeout".to_owned());
        job.error_message = Some("x".repeat(1_000));
        let text = failure(&Verdict::Kill { job: job.clone() });
        assert!(text.starts_with("Timeout: xxx"));
        assert_eq!(text.chars().count(), MAX_ERROR_CHARS);
        job.error_message = None;
        assert_eq!(failure(&Verdict::Discard { job: job.clone() }), "Timeout");
        let fetched = JobPayload::new("Report", vec![]);
        assert!(
            failure(&Verdict::Discard {
                job: fetched.clone()
            })
            .contains("error handlers")
        );
        let dropped = Verdict::NoRetry {
            job: fetched,
            error_class: "Timeout".to_owned(),
            error_message: "too slow".to_owned(),
        };
        assert_eq!(failure(&dropped), "Timeout: too slow");
    }
}
