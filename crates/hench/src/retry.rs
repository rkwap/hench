//! Retries, death handling and the morgue (Sidekiq's `JobRetry`). See `docs/DESIGN.md` §5.
//!
//! When a job fails, [`decide`] works out what happens to it (scheduling a retry, the
//! morgue, or dropping it), running the job's [`retry_in`](crate::Job::retry_in) and
//! [`retries_exhausted`](crate::Job::retries_exhausted) hooks; [`settlement`] turns that
//! verdict into what the fetcher writes to Redis together with the job's acknowledgement (in
//! one idempotent step with reliable fetch, so the processor can retry it during an outage),
//! and [`finish`] then runs the death handlers.
//!
//! The rules, as in Sidekiq:
//!
//! * The job that is retried is the one that was fetched, not the copy server middleware may
//!   have modified.
//! * `retry: false` (in the payload, else in the job type's options) disables retries: no
//!   retry set, no morgue, but the death handlers run (with the job as fetched).
//! * Otherwise the job goes to `retry_queue` (else the queue it came from) and gets
//!   `error_message`, `error_class`, `retry_count` (0 after the first failure), `failed_at`
//!   (first failure) or `retried_at`, and `error_backtrace` when `backtrace` is enabled.
//! * Retries are exhausted when `retry_for` has elapsed since `failed_at` (if set), else when
//!   `retry_count` reaches the maximum (`retry: n`, or `max_retries` for `retry: true`).
//! * `retry_in` may pick the delay (`After`), drop the job (`Discard`: `discarded_at` is set)
//!   or exhaust its retries now (`Kill`); the default delay is `count⁴ + 15` seconds. A
//!   random jitter of up to `10 × (count + 1)` seconds is always added.
//! * Exhausted jobs go through `retries_exhausted` and then to the morgue, unless the job has
//!   `dead: false` or the hook returns `Discard` (then `discarded_at` is set instead).
//! * The death handlers run for every job that will not be retried.
//
// LANE: engine — this module is owned by the engine lane.

use std::panic::AssertUnwindSafe;

use futures::FutureExt;

use crate::client::MAX_AT;
use crate::config::{Config, ErrorContext};
use crate::error::{JobError, Result};
use crate::fetch::Settlement;
use crate::job::{ErasedJob, Exhausted, RetryIn};
use crate::payload::{
    BacktraceOption, JobPayload, RetryOption, compress_backtrace, now_millis, now_secs_f64,
    truncate_error_message,
};

/// What happens to a failed job.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Verdict {
    /// Retried at `at` (epoch seconds): added to the retry set.
    Retry {
        /// When the job runs again.
        at: f64,
        /// The job with its error fields.
        job: JobPayload,
    },
    /// Retries exhausted (or `Kill`): moved to the morgue.
    Kill {
        /// The job with its error fields.
        job: JobPayload,
    },
    /// Dropped (`dead: false`, or `Discard` from a hook): `discarded_at` is set.
    Discard {
        /// The job with its error fields.
        job: JobPayload,
    },
    /// Retries are disabled (`retry: false`): dropped as fetched.
    NoRetry {
        /// The job as it was fetched.
        job: JobPayload,
        /// The class of the error (stored nowhere: for the batch summary and the periodic
        /// record of the run).
        error_class: String,
        /// The error's message, truncated like a stored job's.
        error_message: String,
    },
}

impl Verdict {
    /// The job as it will be stored or reported.
    #[cfg(test)]
    pub(crate) fn job(&self) -> &JobPayload {
        match self {
            Self::Retry { job, .. }
            | Self::Kill { job }
            | Self::Discard { job }
            | Self::NoRetry { job, .. } => job,
        }
    }
}

/// The default delay before retry number `count + 1`, in seconds: `count⁴ + 15`.
pub(crate) fn default_delay(count: u32) -> f64 {
    f64::from(count).powi(4) + 15.0
}

/// A random extra delay in whole seconds, below `10 × (count + 1)`.
pub(crate) fn jitter(count: u32) -> f64 {
    let bound = 10 * (u64::from(count) + 1);
    // `as` is exact: the bound is far below 2^53.
    fastrand::u64(0..bound) as f64
}

/// Decides what happens to a job that failed with `error`.
///
/// `job` is the job type (`None` when the class is not registered: no hooks run and only the
/// payload's own `retry` setting counts); `original` is the job as fetched; `queue` is the
/// queue it was fetched from. Panicking hooks are reported to the error handlers and the
/// default behaviour is used.
pub(crate) async fn decide(
    config: &Config,
    job: Option<&dyn ErasedJob>,
    mut original: JobPayload,
    queue: &str,
    error: &JobError,
) -> Verdict {
    if original.retry.is_none()
        && let Some(job) = job
    {
        original.retry = job.job_options().retry;
    }
    let retry = original.retry.unwrap_or(RetryOption::Bool(true));
    let Some(max_retries) = retry.max_retries(config.max_retries) else {
        return Verdict::NoRetry {
            job: original,
            error_class: error.class().to_owned(),
            error_message: truncate_error_message(&error.message()),
        };
    };

    let mut msg = original;
    let now_ms = now_millis();
    msg.queue = match msg.retry_queue.as_deref() {
        Some(retry_queue) if !retry_queue.is_empty() => retry_queue.to_owned(),
        _ => queue.to_owned(),
    };
    msg.error_message = Some(truncate_error_message(&error.message()));
    msg.error_class = Some(error.class().to_owned());
    let count = match msg.retry_count {
        Some(previous) => {
            msg.retried_at = Some(now_ms);
            previous.saturating_add(1)
        }
        None => {
            msg.failed_at = Some(now_ms);
            0
        }
    };
    msg.retry_count = Some(count);
    if let Some(option) = msg
        .backtrace
        .filter(|option| *option != BacktraceOption::Bool(false))
        && let Some(lines) = option.select(cleaned_backtrace(config, error, &msg))
    {
        msg.error_backtrace = Some(compress_backtrace(&lines));
    }

    // `retry_for` replaces the count-based limit entirely.
    let exhausted = match msg.retry_for {
        Some(retry_for) => {
            let failed_at = msg.failed_at.unwrap_or(now_ms);
            let window_ms = i64::try_from(retry_for)
                .unwrap_or(i64::MAX)
                .saturating_mul(1000);
            failed_at.saturating_add(window_ms) <= now_ms
        }
        None => count >= max_retries,
    };
    if exhausted {
        return exhaust(config, job, msg, error).await;
    }

    let decision = match job {
        None => RetryIn::Default,
        Some(job) => match std::panic::catch_unwind(AssertUnwindSafe(|| {
            job.dyn_retry_in(count, error, &msg)
        })) {
            Ok(decision) => decision,
            Err(panic) => {
                config.handle_error(
                    &JobError::panic(panic),
                    &ErrorContext::new("retry_in panicked; using the default retry delay")
                        .with_job(&msg),
                );
                RetryIn::Default
            }
        },
    };
    let delay = match decision {
        RetryIn::Discard => {
            msg.discarded_at = Some(now_millis());
            return Verdict::Discard { job: msg };
        }
        RetryIn::Kill => return exhaust(config, job, msg, error).await,
        RetryIn::After(delay) if !delay.is_zero() => delay.as_secs_f64(),
        RetryIn::After(_) | RetryIn::Default => default_delay(count),
    };
    let mut at = now_secs_f64() + delay + jitter(count);
    if at > MAX_AT {
        // A `retry_in` beyond reason (`Duration::MAX`): no job time is later than the year
        // 9999, which every page can still show and a unique job's lock can still expire at.
        tracing::warn!(
            class = %msg.class,
            jid = %msg.jid,
            delay_secs = delay,
            "retry_in asked for a retry after the year 9999; retrying at its end instead"
        );
        at = MAX_AT;
    }
    Verdict::Retry { at, job: msg }
}

/// The backtrace lines of `error` to store with `job`, as the configured cleaner
/// (`HenchBuilder::backtrace_cleaner`) leaves them. A cleaner that panics is reported and the
/// lines are stored as they were.
fn cleaned_backtrace(config: &Config, error: &JobError, job: &JobPayload) -> Vec<String> {
    let lines = error.backtrace_lines();
    let Some(cleaner) = &config.backtrace_cleaner else {
        return lines;
    };
    match std::panic::catch_unwind(AssertUnwindSafe(|| cleaner(lines.clone()))) {
        Ok(cleaned) => cleaned,
        Err(panic) => {
            config.handle_error(
                &JobError::panic(panic),
                &ErrorContext::new("backtrace_cleaner panicked; storing the backtrace uncleaned")
                    .with_job(job),
            );
            lines
        }
    }
}

/// The verdict for a job whose retries are exhausted: runs `retries_exhausted`, then sends the
/// job to the morgue unless it is discarded.
async fn exhaust(
    config: &Config,
    job: Option<&dyn ErasedJob>,
    mut msg: JobPayload,
    error: &JobError,
) -> Verdict {
    let outcome = match job {
        None => Exhausted::Default,
        Some(job) => {
            let hook = match std::panic::catch_unwind(AssertUnwindSafe(|| {
                job.dyn_retries_exhausted(&msg, error)
            })) {
                Ok(future) => AssertUnwindSafe(future).catch_unwind().await,
                Err(panic) => Err(panic),
            };
            match hook {
                Ok(outcome) => outcome,
                Err(panic) => {
                    config.handle_error(
                        &JobError::panic(panic),
                        &ErrorContext::new("retries_exhausted panicked").with_job(&msg),
                    );
                    Exhausted::Default
                }
            }
        }
    };
    if msg.dead == Some(false) || outcome == Exhausted::Discard {
        msg.discarded_at = Some(now_millis());
        Verdict::Discard { job: msg }
    } else {
        Verdict::Kill { job: msg }
    }
}

/// What a verdict writes to Redis (see [`Settlement`]): the retry set for `Retry`, the morgue
/// for `Kill` (trimmed by age and size, see [`MorgueBounds`]), nothing otherwise.
pub(crate) fn settlement(config: &Config, verdict: &Verdict) -> Result<Settlement> {
    Ok(match verdict {
        Verdict::Retry { at, job } => Settlement::Retry {
            at: *at,
            job: job.to_json()?.into_bytes(),
        },
        Verdict::Kill { job } => {
            tracing::info!("adding dead job to the morgue");
            Settlement::Morgue {
                job: job.to_json()?.into_bytes(),
                bounds: MorgueBounds::new(config),
            }
        }
        Verdict::Discard { .. } | Verdict::NoRetry { .. } => Settlement::Drop,
    })
}

/// Runs the death handlers for verdicts that end the job's life.
pub(crate) fn finish(config: &Config, verdict: &Verdict, error: &JobError) {
    match verdict {
        Verdict::Retry { .. } => {}
        Verdict::Kill { job } | Verdict::Discard { job } | Verdict::NoRetry { job, .. } => {
            config.handle_death(job, error);
        }
    }
}

/// Where a job added to the morgue now goes and how the morgue is trimmed afterwards.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct MorgueBounds {
    /// The score of a job added now (epoch seconds).
    pub(crate) now: f64,
    /// Entries scored below this are older than `dead_timeout` and removed.
    pub(crate) cutoff: f64,
    /// Upper bound of `ZREMRANGEBYRANK dead 0 <rank>`: `-dead_max_jobs`. Like Sidekiq, this
    /// keeps the newest `dead_max_jobs - 1` entries.
    pub(crate) rank: i64,
}

impl MorgueBounds {
    /// The bounds for an insertion now.
    pub(crate) fn new(config: &Config) -> Self {
        let now = now_secs_f64();
        Self {
            now,
            cutoff: now - config.dead_timeout.as_secs_f64(),
            rank: -i64::try_from(config.dead_max_jobs).unwrap_or(i64::MAX),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::{Job, JobContext};
    use crate::payload::{BacktraceOption, decompress_backtrace};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[derive(Default)]
    struct Plain;

    impl Job for Plain {
        const NAME: &'static str = "Plain";
        type Args = ();
        async fn perform(&self, _: &JobContext, (): ()) -> Result<(), JobError> {
            Ok(())
        }
    }

    /// `retry_in` by error class, like Sidekiq's `CustomJobWithException`.
    #[derive(Default)]
    struct Custom;

    impl Job for Custom {
        const NAME: &'static str = "Custom";
        type Args = ();
        async fn perform(&self, _: &JobContext, (): ()) -> Result<(), JobError> {
            Ok(())
        }
        fn retry_in(&self, count: u32, error: &JobError, _job: &JobPayload) -> RetryIn {
            match error.class() {
                "kill" => RetryIn::Kill,
                "discard" => RetryIn::Discard,
                "default" => RetryIn::Default,
                "zero" => RetryIn::After(Duration::ZERO),
                "panic" => panic!("retry_in bug"),
                _ => RetryIn::After(Duration::from_secs(u64::from(count) * 4)),
            }
        }
    }

    /// Records `retries_exhausted` calls and returns a configured outcome.
    struct Exhaust {
        calls: Arc<AtomicUsize>,
        outcome: Exhausted,
        panics: bool,
    }

    impl Job for Exhaust {
        const NAME: &'static str = "Exhaust";
        type Args = ();
        async fn perform(&self, _: &JobContext, (): ()) -> Result<(), JobError> {
            Ok(())
        }
        async fn retries_exhausted(&self, job: &JobPayload, error: &JobError) -> Exhausted {
            assert_eq!(job.error_message.as_deref(), Some(error.message().as_str()));
            self.calls.fetch_add(1, Ordering::SeqCst);
            assert!(!self.panics, "exhausted bug");
            self.outcome
        }
    }

    /// A job with `retry: false` in its options.
    #[derive(Default)]
    struct NoRetries;

    impl Job for NoRetries {
        const NAME: &'static str = "NoRetries";
        type Args = ();
        fn options() -> crate::JobOptions {
            crate::JobOptions::new().retry(false)
        }
        async fn perform(&self, _: &JobContext, (): ()) -> Result<(), JobError> {
            Ok(())
        }
    }

    fn config() -> Config {
        let mut config = Config::default();
        config.error_handlers.clear();
        config
    }

    fn job() -> JobPayload {
        let mut job = JobPayload::new("Bob", vec![1.into(), 2.into(), "foo".into()]);
        job.jid = "0123456789abcdef01234567".into();
        job.queue = "default".into();
        job.retry = Some(RetryOption::Bool(true));
        job
    }

    fn boom() -> JobError {
        JobError::new("RuntimeError", "kerblammo!")
    }

    async fn run_decide(
        job: Option<&dyn ErasedJob>,
        payload: JobPayload,
        error: &JobError,
    ) -> Verdict {
        decide(&config(), job, payload, "default", error).await
    }

    #[tokio::test]
    async fn new_failures_get_error_fields() {
        let before = now_millis();
        let Verdict::Retry { at, job } = run_decide(Some(&Plain), job(), &boom()).await else {
            panic!("expected a retry");
        };
        assert_eq!(job.queue, "default");
        assert_eq!(job.error_message.as_deref(), Some("kerblammo!"));
        assert_eq!(job.error_class.as_deref(), Some("RuntimeError"));
        assert_eq!(job.retry_count, Some(0));
        assert!(job.failed_at.unwrap() >= before);
        assert_eq!(job.retried_at, None);
        assert_eq!(job.error_backtrace, None);
        // count 0: 15 s plus a jitter below 10 s.
        let delay = at - now_secs_f64();
        assert!((14.0..25.5).contains(&delay), "{delay}");
    }

    #[tokio::test]
    async fn recurring_failures_increment_the_count() {
        let mut payload = job();
        payload.retry_count = Some(10);
        payload.failed_at = Some(1_700_000_000_000);
        payload.error_message = Some("old".into());
        let Verdict::Retry { at, job } = run_decide(None, payload, &boom()).await else {
            panic!("expected a retry");
        };
        assert_eq!(job.retry_count, Some(11));
        assert_eq!(job.failed_at, Some(1_700_000_000_000));
        assert!(job.retried_at.is_some());
        assert_eq!(job.error_message.as_deref(), Some("kerblammo!"));
        let delay = at - now_secs_f64();
        let base = default_delay(11);
        assert!(delay > base - 1.0 && delay < base + 120.0, "{delay}");
    }

    #[tokio::test]
    async fn retry_queue_overrides_the_queue() {
        let mut payload = job();
        payload.retry_queue = Some("retryx".into());
        let verdict = run_decide(Some(&Plain), payload, &boom()).await;
        assert_eq!(verdict.job().queue, "retryx");
        // The queue the job was fetched from wins over the payload's queue.
        let verdict = decide(&config(), None, job(), "fetched", &boom()).await;
        assert_eq!(verdict.job().queue, "fetched");
    }

    #[tokio::test]
    async fn disabled_retries() {
        let mut payload = job();
        payload.retry = Some(RetryOption::Bool(false));
        let verdict = run_decide(Some(&Plain), payload.clone(), &boom()).await;
        assert_eq!(
            verdict,
            Verdict::NoRetry {
                job: payload,
                error_class: boom().class().to_owned(),
                error_message: boom().message(),
            }
        );
        // The job type's options apply when the payload has no `retry`.
        let mut payload = job();
        payload.retry = None;
        let verdict = run_decide(Some(&NoRetries), payload, &boom()).await;
        assert!(matches!(verdict, Verdict::NoRetry { ref job, .. } if job.error_message.is_none()));
        // Without any setting, retries are enabled.
        let mut payload = job();
        payload.retry = None;
        assert!(matches!(
            run_decide(None, payload, &boom()).await,
            Verdict::Retry { .. }
        ));
    }

    #[tokio::test]
    async fn numeric_retries() {
        let mut payload = job();
        payload.retry = Some(RetryOption::Count(2));
        assert!(matches!(
            run_decide(Some(&Plain), payload.clone(), &boom()).await,
            Verdict::Retry { .. }
        ));
        payload.retry_count = Some(1);
        assert!(matches!(
            run_decide(Some(&Plain), payload, &boom()).await,
            Verdict::Kill { .. }
        ));
        let mut payload = job();
        payload.retry = Some(RetryOption::Count(0));
        assert!(matches!(
            run_decide(Some(&Plain), payload, &boom()).await,
            Verdict::Kill { .. }
        ));
    }

    #[tokio::test]
    async fn max_retries_applies_to_retry_true() {
        let mut config = config();
        config.max_retries = 7;
        let mut retried = 0;
        let mut killed = 0;
        for attempt in 0..8_u32 {
            let mut payload = job();
            payload.retry_count = attempt.checked_sub(1);
            match decide(&config, Some(&Plain), payload, "default", &boom()).await {
                Verdict::Retry { .. } => retried += 1,
                Verdict::Kill { .. } => killed += 1,
                other => panic!("unexpected {other:?}"),
            }
        }
        assert_eq!((retried, killed), (7, 1));
        // The default is 25.
        let mut payload = job();
        payload.retry_count = Some(25);
        assert!(matches!(
            run_decide(None, payload, &boom()).await,
            Verdict::Kill { .. }
        ));
    }

    #[tokio::test]
    async fn retry_for_replaces_the_count() {
        let mut payload = job();
        payload.retry_for = Some(60);
        assert!(matches!(
            run_decide(Some(&Plain), payload.clone(), &boom()).await,
            Verdict::Retry { .. }
        ));
        payload.retry_count = Some(0);
        payload.failed_at = Some(now_millis() - 61_000);
        assert!(matches!(
            run_decide(Some(&Plain), payload, &boom()).await,
            Verdict::Kill { .. }
        ));
        // retry_for ignores max_retries.
        let mut config = config();
        config.max_retries = 2;
        let mut payload = job();
        payload.retry_for = Some(3600);
        payload.retry_count = Some(5);
        payload.failed_at = Some(now_millis());
        assert!(matches!(
            decide(&config, Some(&Plain), payload, "default", &boom()).await,
            Verdict::Retry { .. }
        ));
        // retry_for 0 exhausts at the first failure.
        let mut payload = job();
        payload.retry_for = Some(0);
        assert!(matches!(
            run_decide(Some(&Plain), payload, &boom()).await,
            Verdict::Kill { .. }
        ));
    }

    #[tokio::test]
    async fn backtraces() {
        #[derive(Debug, thiserror::Error)]
        #[error("outer")]
        struct Outer(#[source] std::io::Error);
        let error = JobError::from(Outer(std::io::Error::other("inner")));

        let mut payload = job();
        payload.backtrace = Some(BacktraceOption::Bool(true));
        let verdict = run_decide(Some(&Plain), payload, &error).await;
        let lines = decompress_backtrace(verdict.job().error_backtrace.as_ref().unwrap()).unwrap();
        assert_eq!(lines.first().map(String::as_str), Some("caused by: inner"));

        let mut payload = job();
        payload.backtrace = Some(BacktraceOption::Lines(0));
        let verdict = run_decide(Some(&Plain), payload, &error).await;
        let lines = decompress_backtrace(verdict.job().error_backtrace.as_ref().unwrap()).unwrap();
        assert!(lines.is_empty());

        let mut payload = job();
        payload.backtrace = Some(BacktraceOption::Bool(false));
        assert_eq!(
            run_decide(Some(&Plain), payload, &error)
                .await
                .job()
                .error_backtrace,
            None
        );
    }

    #[tokio::test]
    async fn long_messages_are_truncated() {
        let error = JobError::msg("x".repeat(20_000));
        let verdict = run_decide(None, job(), &error).await;
        assert_eq!(
            verdict.job().error_message.as_ref().map(String::len),
            Some(crate::MAX_ERROR_MESSAGE_CHARS)
        );
    }

    #[tokio::test]
    async fn retry_in_decisions() {
        let Verdict::Retry { at, .. } =
            run_decide(Some(&Custom), job(), &JobError::new("x", "m")).await
        else {
            panic!("expected a retry");
        };
        // count 0 → After(0) means the default delay (15 s + jitter).
        assert!(at - now_secs_f64() > 14.0);

        let mut payload = job();
        payload.retry_count = Some(1);
        let Verdict::Retry { at, .. } =
            run_decide(Some(&Custom), payload, &JobError::new("x", "m")).await
        else {
            panic!("expected a retry");
        };
        // count 2 → 8 s plus a jitter below 30 s.
        let delay = at - now_secs_f64();
        assert!((7.0..38.5).contains(&delay), "{delay}");

        let discarded = run_decide(Some(&Custom), job(), &JobError::new("discard", "m")).await;
        assert!(matches!(discarded, Verdict::Discard { ref job } if job.discarded_at.is_some()));
        let killed = run_decide(Some(&Custom), job(), &JobError::new("kill", "m")).await;
        assert!(matches!(killed, Verdict::Kill { .. }));
        let zero = run_decide(Some(&Custom), job(), &JobError::new("zero", "m")).await;
        assert!(matches!(zero, Verdict::Retry { at, .. } if at - now_secs_f64() > 14.0));
    }

    #[tokio::test]
    async fn panicking_retry_in_falls_back_to_the_default() {
        let errors = Arc::new(AtomicUsize::new(0));
        let seen = errors.clone();
        let mut config = config();
        config.error_handlers.push(Arc::new(move |error, ctx| {
            assert!(error.is_panic());
            assert!(ctx.context.contains("retry_in"));
            seen.fetch_add(1, Ordering::SeqCst);
        }));
        let verdict = decide(
            &config,
            Some(&Custom),
            job(),
            "default",
            &JobError::new("panic", "m"),
        )
        .await;
        assert!(matches!(verdict, Verdict::Retry { at, .. } if at - now_secs_f64() > 14.0));
        assert_eq!(errors.load(Ordering::SeqCst), 1);
    }

    fn exhaust_job(outcome: Exhausted, panics: bool) -> (Exhaust, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        (
            Exhaust {
                calls: calls.clone(),
                outcome,
                panics,
            },
            calls,
        )
    }

    #[tokio::test]
    async fn retries_exhausted_hook() {
        // Not called while retries remain.
        let (hook, calls) = exhaust_job(Exhausted::Default, false);
        let mut payload = job();
        payload.retry = Some(RetryOption::Count(1));
        assert!(matches!(
            run_decide(Some(&hook), payload.clone(), &boom()).await,
            Verdict::Retry { .. }
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        // Called once they are exhausted.
        payload.retry_count = Some(0);
        assert!(matches!(
            run_decide(Some(&hook), payload.clone(), &boom()).await,
            Verdict::Kill { .. }
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        // `Discard` keeps the job out of the morgue.
        let (hook, calls) = exhaust_job(Exhausted::Discard, false);
        let discarded = run_decide(Some(&hook), payload.clone(), &boom()).await;
        assert!(matches!(discarded, Verdict::Discard { ref job } if job.discarded_at.is_some()));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        // A panicking hook is reported and the job still dies.
        let (hook, calls) = exhaust_job(Exhausted::Discard, true);
        assert!(matches!(
            run_decide(Some(&hook), payload, &boom()).await,
            Verdict::Kill { .. }
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn dead_false_discards_exhausted_jobs() {
        let mut payload = job();
        payload.retry = Some(RetryOption::Count(0));
        payload.dead = Some(false);
        let verdict = run_decide(Some(&Plain), payload, &boom()).await;
        assert!(matches!(verdict, Verdict::Discard { ref job } if job.discarded_at.is_some()));
    }

    #[test]
    fn morgue_bounds() {
        let mut config = config();
        config.dead_max_jobs = 10_000;
        config.dead_timeout = Duration::from_secs(10);
        let bounds = MorgueBounds::new(&config);
        assert_eq!(bounds.rank, -10_000);
        assert!((bounds.now - bounds.cutoff - 10.0).abs() < 1e-6);
        config.dead_max_jobs = usize::MAX;
        assert_eq!(MorgueBounds::new(&config).rank, -i64::MAX);
    }

    #[test]
    fn delays() {
        assert_eq!(default_delay(0), 15.0);
        assert_eq!(default_delay(2), 31.0);
        assert_eq!(default_delay(25), 390_640.0);
        assert!(default_delay(u32::MAX).is_finite());
        for count in [0, 1, 5, 24] {
            for _ in 0..200 {
                let jitter = jitter(count);
                assert!(jitter >= 0.0 && jitter < f64::from(10 * (count + 1)));
                assert_eq!(jitter.fract(), 0.0);
            }
        }
        assert!(jitter(u32::MAX) >= 0.0);
    }

    proptest::proptest! {
        /// DESIGN §10: retry delays stay within Sidekiq's bounds for every retry count.
        #[test]
        fn retry_delays_stay_in_bounds(count in proptest::num::u32::ANY) {
            let delay = default_delay(count);
            proptest::prop_assert!(delay.is_finite() && delay >= 15.0);
            proptest::prop_assert_eq!(delay, f64::from(count).powi(4) + 15.0);
            if let Some(next) = count.checked_add(1) {
                proptest::prop_assert!(default_delay(next) > delay);
            }
            let jitter = jitter(count);
            proptest::prop_assert!(jitter >= 0.0 && jitter < 10.0 * (f64::from(count) + 1.0));
            proptest::prop_assert_eq!(jitter.fract(), 0.0);
        }
    }

    #[tokio::test]
    async fn death_handlers_run_for_final_verdicts() {
        let deaths = Arc::new(AtomicUsize::new(0));
        let seen = deaths.clone();
        let mut config = config();
        config.death_handlers.push(Arc::new(move |_, _| {
            seen.fetch_add(1, Ordering::SeqCst);
        }));
        let error = boom();
        let job = job();
        finish(
            &config,
            &Verdict::Retry {
                at: 0.0,
                job: job.clone(),
            },
            &error,
        );
        assert_eq!(deaths.load(Ordering::SeqCst), 0);
        for verdict in [
            Verdict::Kill { job: job.clone() },
            Verdict::Discard { job: job.clone() },
            Verdict::NoRetry {
                job,
                error_class: error.class().to_owned(),
                error_message: error.message(),
            },
        ] {
            finish(&config, &verdict, &error);
        }
        assert_eq!(deaths.load(Ordering::SeqCst), 3);
    }
}
