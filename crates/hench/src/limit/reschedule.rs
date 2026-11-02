//! The delay of a reschedule and the error past a reschedule budget
//! (`docs/design/ratelimit.md` §2.5, §2.6).
//!
//! Over-limit reschedules (a limiter refused while the job ran) take the limiter's `backoff`,
//! else the process's `over_limit_backoff`, else the refusal's `retry_after` (the wait-line
//! slot), else 60 s × n, clamped to [1 s, max delay]; throttled jobs take the slot their
//! throttle gave them, clamped to [50 ms, max delay]. A delay that leads to the caller's own
//! place in a wait line is exact (every waiter has its own slot, and one coming back late may
//! find its unit taken); any other gets a uniform jitter of up to a tenth (at most 30 s), which
//! spreads callers told the same wait.

use std::panic::AssertUnwindSafe;
use std::time::Duration;

use crate::error::JobError;
use crate::limit::{Backoff, BackoffInput, OverLimit};
use crate::payload::JobPayload;

/// Shortest reschedule delay of a job a limiter refused while it ran (a reschedule runs the
/// job again from its start).
pub(crate) const MIN_DELAY: Duration = Duration::from_secs(1);

/// Shortest parking of a job its throttle refused: the slot of its place in the line, but never
/// a busy loop (the scheduler brings a parked job back within a few milliseconds of its slot).
pub(crate) const MIN_PARK: Duration = Duration::from_millis(50);

/// The step of the default backoff: reschedule `n` waits `n` steps.
const DEFAULT_STEP: Duration = Duration::from_secs(60);

/// Largest jitter added to a delay.
const MAX_JITTER: Duration = Duration::from_secs(30);

/// The delay of over-limit reschedule number `n` of `job` (as fetched), refused by `over`.
/// `process_backoff` and `process_max` are the process's settings; a backoff closure that
/// panics is reported through `report` and the default applies.
pub(crate) fn over_limit_delay(
    over: &OverLimit,
    job: &JobPayload,
    n: u32,
    process_backoff: Option<&Backoff>,
    process_max: Duration,
    report: &dyn Fn(JobError),
) -> Duration {
    let input = BackoffInput {
        job,
        over,
        reschedules: n,
    };
    let chosen = over.backoff().or(process_backoff).and_then(|backoff| {
        match std::panic::catch_unwind(AssertUnwindSafe(|| backoff(&input))) {
            Ok(delay) => Some(delay),
            Err(panic) => {
                report(JobError::panic(panic));
                None
            }
        }
    });
    // The slot of the job's own place in the line is exact; any other delay is jittered.
    let (base, exact) = match (chosen, over.retry_after()) {
        (Some(delay), _) => (delay, false),
        (None, Some(after)) => (after, over.place().is_some()),
        (None, None) => (DEFAULT_STEP.saturating_mul(n.max(1)), false),
    };
    let max = over.max_delay().unwrap_or(process_max).max(MIN_DELAY);
    let delay = base.clamp(MIN_DELAY, max);
    if exact { delay } else { with_jitter(delay) }
}

/// The delay of a job its throttle refused: until its slot (`wait`), clamped to [50 ms,
/// `max_delay`]. With a wait line (`line`) the slot is the job's own place, so it comes back
/// exactly then (a jitter would only make it late for it); without one, refused jobs are told
/// the same wait and a jitter spreads them.
pub(crate) fn throttle_delay(wait: Duration, max_delay: Duration, line: bool) -> Duration {
    let delay = wait.clamp(MIN_PARK, max_delay.max(MIN_PARK));
    if line { delay } else { with_jitter(delay) }
}

/// `delay` plus a uniform jitter in [0, min(delay / 10, 30 s)].
pub(crate) fn with_jitter(delay: Duration) -> Duration {
    let cap = (delay / 10).min(MAX_JITTER);
    delay.saturating_add(cap.mul_f64(fastrand::f64()))
}

/// The error of a job whose budget of `max` reschedules is spent: a refusal of class
/// `hench::OverLimit` (so `JobError::is_over_limit` holds for it), which goes through the
/// failure path; `cause` is the job's own error when it was not a limiter's refusal (an error
/// the `over_limit_when` classifier recognized).
pub(crate) fn exhausted_with(over: &OverLimit, max: u32, cause: Option<&JobError>) -> JobError {
    let mut error = JobError::over_limit(over.clone());
    if let Some(cause) = cause {
        error = error.context(cause.message());
    }
    error.context(format!(
        "rescheduled {max} times by `{}`; handing over to retries",
        over.label()
    ))
}

/// [`exhausted_with`] without a cause of its own.
pub(crate) fn exhausted(over: &OverLimit, max: u32) -> JobError {
    exhausted_with(over, max, None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn job() -> JobPayload {
        JobPayload::new("Charge", vec![])
    }

    fn no_report(_: JobError) {}

    #[test]
    fn hints_are_clamped_and_jittered() {
        let max = Duration::from_secs(86_400);
        // The hint, within a tenth.
        let over = OverLimit::new("w:x").with_retry_after(Duration::from_secs(12));
        let delay = over_limit_delay(&over, &job(), 1, None, max, &no_report);
        assert!(delay >= Duration::from_secs(12) && delay <= Duration::from_millis(13_200));
        // At least a second.
        let over = OverLimit::new("w:x").with_retry_after(Duration::from_millis(10));
        let delay = over_limit_delay(&over, &job(), 1, None, max, &no_report);
        assert!(
            delay >= MIN_DELAY && delay <= Duration::from_millis(1_100),
            "{delay:?}"
        );
        // At most the refusal's own maximum, else the process's.
        let over = OverLimit::new("w:x")
            .with_retry_after(Duration::from_secs(7_200))
            .with_max_delay(Duration::from_secs(60));
        let delay = over_limit_delay(&over, &job(), 1, None, max, &no_report);
        assert!(delay >= Duration::from_secs(60) && delay <= Duration::from_secs(66));
        let over = OverLimit::new("w:x").with_retry_after(Duration::from_secs(7_200));
        let delay = over_limit_delay(&over, &job(), 1, None, Duration::from_secs(10), &no_report);
        assert!(delay >= Duration::from_secs(10) && delay <= Duration::from_secs(11));
        // Without a hint: a minute per reschedule.
        let delay = over_limit_delay(&OverLimit::new("api"), &job(), 3, None, max, &no_report);
        assert!(delay >= Duration::from_secs(180) && delay <= Duration::from_secs(198));
    }

    #[test]
    fn the_limiter_backoff_wins_over_the_process_one() {
        let max = Duration::from_secs(86_400);
        let process: Backoff = Arc::new(|input: &BackoffInput<'_>| {
            Duration::from_secs(100 * u64::from(input.reschedules))
        });
        let own: Backoff = Arc::new(|_: &BackoffInput<'_>| Duration::from_secs(5));
        let over = OverLimit::new("w:x").with_retry_after(Duration::from_secs(1_000));
        let delay = over_limit_delay(&over, &job(), 2, Some(&process), max, &no_report);
        assert!(delay >= Duration::from_secs(200) && delay <= Duration::from_secs(220));
        let over = over.with_backoff(own);
        let delay = over_limit_delay(&over, &job(), 2, Some(&process), max, &no_report);
        assert!(delay >= Duration::from_secs(5) && delay <= Duration::from_millis(5_500));
    }

    #[test]
    fn a_panicking_backoff_is_reported_and_the_default_used() {
        let reported = AtomicUsize::new(0);
        let report = |error: JobError| {
            assert_eq!(error.class(), "panic");
            reported.fetch_add(1, Ordering::Relaxed);
        };
        let broken: Backoff = Arc::new(|_: &BackoffInput<'_>| panic!("no delay for you"));
        let over = OverLimit::new("w:x")
            .with_retry_after(Duration::from_secs(4))
            .with_backoff(broken);
        let delay = over_limit_delay(&over, &job(), 1, None, Duration::from_secs(60), &report);
        assert!(delay >= Duration::from_secs(4) && delay <= Duration::from_millis(4_400));
        assert_eq!(reported.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn throttles_wait_for_their_slot() {
        // Their own place in the line: exactly its slot.
        let delay = throttle_delay(Duration::from_millis(4_200), Duration::from_secs(60), true);
        assert_eq!(delay, Duration::from_millis(4_200));
        let delay = throttle_delay(Duration::from_millis(80), Duration::from_secs(60), true);
        assert_eq!(delay, Duration::from_millis(80));
        let delay = throttle_delay(Duration::ZERO, Duration::from_secs(60), true);
        assert_eq!(delay, MIN_PARK);
        let delay = throttle_delay(Duration::from_secs(600), Duration::from_secs(60), true);
        assert_eq!(delay, Duration::from_secs(60));
        // Without a line, a jitter spreads the jobs told the same wait.
        let delay = throttle_delay(Duration::from_millis(4_200), Duration::from_secs(60), false);
        assert!(delay >= Duration::from_millis(4_200) && delay <= Duration::from_millis(4_620));
        let delay = throttle_delay(Duration::from_secs(600), Duration::from_secs(60), false);
        assert!(delay >= Duration::from_secs(60) && delay <= Duration::from_secs(66));
    }

    #[test]
    fn a_place_in_the_line_is_kept_exactly() {
        let max = Duration::from_secs(86_400);
        let placed = OverLimit::new("w:x")
            .with_retry_after(Duration::from_millis(12_345))
            .with_place(1_000);
        let delay = over_limit_delay(&placed, &job(), 1, None, max, &no_report);
        assert_eq!(delay, Duration::from_millis(12_345));
        // Still at least a second: a reschedule runs the job again from its start.
        let soon = OverLimit::new("w:x")
            .with_retry_after(Duration::from_millis(10))
            .with_place(1_000);
        assert_eq!(
            over_limit_delay(&soon, &job(), 1, None, max, &no_report),
            MIN_DELAY
        );
    }

    #[test]
    fn budgets_end_in_an_over_limit_failure() {
        let over = OverLimit::new("w:stripe:42");
        let error = exhausted(&over, 20);
        assert_eq!(error.class(), JobError::OVER_LIMIT);
        assert!(error.is_over_limit());
        assert!(
            error
                .message()
                .starts_with("rescheduled 20 times by `w:stripe:42`; handing over to retries"),
            "{}",
            error.message()
        );
        let cause = JobError::msg("HTTP 429 Too Many Requests");
        let error = exhausted_with(&OverLimit::new("api"), 3, Some(&cause));
        assert!(error.message().contains("HTTP 429"), "{}", error.message());
        assert!(error.is_over_limit());
    }

    proptest! {
        #[test]
        fn jitter_stays_within_a_tenth(millis in 0_u64..10_000_000) {
            let delay = Duration::from_millis(millis);
            let jittered = with_jitter(delay);
            prop_assert!(jittered >= delay);
            prop_assert!(jittered <= delay + (delay / 10).min(MAX_JITTER));
        }
    }
}
