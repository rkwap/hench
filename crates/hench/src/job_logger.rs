//! Per-job `tracing` spans and start/done/fail events. See `docs/DESIGN.md` §5.
//
// LANE: engine — this module is owned by the engine lane.

use std::time::Duration;

use serde_json::{Map, Value};
use tracing::Level;
use tracing::field::Empty;

use crate::payload::JobPayload;

/// The payload key holding a job's log level (Sidekiq's `log_level` option).
pub(crate) const LOG_LEVEL_KEY: &str = "log_level";

/// The lowercase name of a level, as stored in payloads and recorded on spans.
pub(crate) fn level_name(level: Level) -> &'static str {
    match level {
        Level::TRACE => "trace",
        Level::DEBUG => "debug",
        Level::INFO => "info",
        Level::WARN => "warn",
        Level::ERROR => "error",
    }
}

/// The log level a job asks for (`"log_level": "debug"`, any case; `warning` and `fatal` are
/// accepted as `warn` and `error`), if any.
pub(crate) fn job_log_level(job: &JobPayload) -> Option<Level> {
    let name = job.extra.get(LOG_LEVEL_KEY)?.as_str()?;
    match name.trim().to_ascii_lowercase().as_str() {
        "trace" => Some(Level::TRACE),
        "debug" => Some(Level::DEBUG),
        "info" => Some(Level::INFO),
        "warn" | "warning" => Some(Level::WARN),
        "error" | "fatal" => Some(Level::ERROR),
        _ => None,
    }
}

/// The payload keys with span fields of their own, left out of `attrs`.
const OWN_FIELDS: [&str; 5] = ["class", "jid", "bid", "tags", LOG_LEVEL_KEY];

/// The span every job runs in: `job{class, jid, bid, tags, log_level, attrs}`, where `bid`,
/// `tags`, `log_level` and `attrs` are only recorded when the job has them. Everything logged
/// while the job runs (by the job itself, middleware, the retry handler and the error
/// handlers) carries these fields; filters can match `log_level` (see
/// [`JOB_LOG_LEVEL_DIRECTIVES`](crate::JOB_LOG_LEVEL_DIRECTIVES)).
pub(crate) fn job_span(job: &JobPayload) -> tracing::Span {
    job_span_with(job, &[])
}

/// [`job_span`], recording in `attrs` the payload keys of `attributes` (Sidekiq's
/// `logged_job_attributes`, see `HenchBuilder::logged_job_attributes`) that the job has, as a
/// JSON object: `attrs={"request_id":"req-4242"}`.
pub(crate) fn job_span_with(job: &JobPayload, attributes: &[String]) -> tracing::Span {
    let span = tracing::info_span!(
        "job",
        class = %job.class,
        jid = %job.jid,
        bid = Empty,
        batch_event = Empty,
        tags = Empty,
        log_level = Empty,
        attrs = Empty,
    );
    if let Some(bid) = &job.bid {
        span.record("bid", tracing::field::display(bid));
    }
    if let Some(callback) = &job.batch_callback {
        // A batch callback: the batch and event it runs for.
        span.record("bid", tracing::field::display(callback.bid()));
        span.record("batch_event", tracing::field::display(callback.event()));
    }
    if !job.tags.is_empty() {
        span.record("tags", tracing::field::display(job.tags.join(",")));
    }
    if let Some(level) = job_log_level(job) {
        span.record("log_level", tracing::field::display(level_name(level)));
    }
    if let Some(attrs) = logged_attributes(job, attributes) {
        span.record("attrs", tracing::field::display(attrs));
    }
    span
}

/// The values the job has for `keys` (other than those with span fields of their own), as a
/// JSON object; `None` when it has none of them.
fn logged_attributes(job: &JobPayload, keys: &[String]) -> Option<String> {
    let mut attrs = Map::new();
    // The whole payload is serialized only for a key that is not a custom one.
    let mut payload: Option<Map<String, Value>> = None;
    for key in keys {
        if OWN_FIELDS.contains(&key.as_str()) || attrs.contains_key(key) {
            continue;
        }
        let value = match job.extra.get(key) {
            Some(value) => Some(value),
            None => payload
                .get_or_insert_with(|| match serde_json::to_value(job) {
                    Ok(Value::Object(fields)) => fields,
                    _ => Map::new(),
                })
                .get(key),
        };
        if let Some(value) = value.filter(|value| !value.is_null()) {
            attrs.insert(key.clone(), value.clone());
        }
    }
    (!attrs.is_empty()).then(|| Value::Object(attrs).to_string())
}

/// Logs the start and the outcome of each job (Sidekiq's `JobLogger`). The events are emitted
/// inside the job's span, so they carry its class and jid; `elapsed` is in seconds.
#[derive(Debug, Clone, Copy)]
pub(crate) struct JobLogger {
    enabled: bool,
}

impl JobLogger {
    /// A logger that logs when `enabled` (see `HenchBuilder::job_logging`).
    pub(crate) fn new(enabled: bool) -> Self {
        Self { enabled }
    }

    /// The logger for one job: silent when the job's log level is above `INFO` (`warn` or
    /// `error`), since its events are at `INFO`.
    pub(crate) fn for_job(self, job: &JobPayload) -> Self {
        let quieter = job_log_level(job).is_some_and(|level| level < Level::INFO);
        Self {
            enabled: self.enabled && !quieter,
        }
    }

    /// The job is about to run.
    pub(crate) fn start(&self) {
        if self.enabled {
            tracing::info!("start");
        }
    }

    /// The job succeeded (or server middleware skipped it).
    pub(crate) fn done(&self, elapsed: Duration) {
        if self.enabled {
            tracing::info!(elapsed = elapsed_secs(elapsed), "done");
        }
    }

    /// The job failed, panicked or was interrupted by a hard shutdown.
    pub(crate) fn fail(&self, elapsed: Duration) {
        if self.enabled {
            tracing::info!(elapsed = elapsed_secs(elapsed), "fail");
        }
    }

    /// The job (an iterable job) saved its progress and will continue later.
    pub(crate) fn interrupted(&self, elapsed: Duration) {
        if self.enabled {
            tracing::info!(elapsed = elapsed_secs(elapsed), "interrupted");
        }
    }

    /// The job ran and went over a limiter's limit: it is rescheduled (`n` of `max`
    /// reschedules) in `delay`.
    pub(crate) fn over_limit(&self, elapsed: Duration, label: &str, delay: Duration, n: u32) {
        if self.enabled {
            tracing::info!(
                elapsed = elapsed_secs(elapsed),
                limit = label,
                delay = elapsed_secs(delay),
                reschedules = n,
                "over_limit"
            );
        }
    }

    /// A throttle refused the job before it started: parked for `delay`.
    pub(crate) fn throttled(&self, label: &str, delay: Duration) {
        if self.enabled {
            tracing::debug!(limit = label, delay = elapsed_secs(delay), "throttled");
        }
    }

    /// The job's batch was cancelled: it is acknowledged without running again.
    pub(crate) fn skipped(&self, elapsed: Duration) {
        if self.enabled {
            tracing::info!(elapsed = elapsed_secs(elapsed), "skipped (batch cancelled)");
        }
    }
}

/// Elapsed time in seconds, rounded to the millisecond.
pub(crate) fn elapsed_secs(elapsed: Duration) -> f64 {
    (elapsed.as_secs_f64() * 1000.0).round() / 1000.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::{Arc, Mutex};

    /// A `tracing` writer that appends to a shared buffer.
    #[derive(Clone, Default)]
    struct Buffer(Arc<Mutex<Vec<u8>>>);

    impl Buffer {
        fn contents(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
    }

    impl Write for Buffer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// A second dispatcher kept alive for the whole test binary. With a single registered
    /// dispatcher, `tracing` caches a callsite's interest from the default dispatcher of the
    /// thread that first reaches it: a test thread without a subscriber could cache the `job`
    /// span as disabled while another test captures it. With two, the interest is "sometimes"
    /// and every thread asks its own dispatcher.
    static SECOND_DISPATCHER: std::sync::LazyLock<tracing::Dispatch> =
        std::sync::LazyLock::new(|| {
            tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default())
        });

    fn capture(run: impl FnOnce()) -> String {
        std::sync::LazyLock::force(&SECOND_DISPATCHER);
        let buffer = Buffer::default();
        let writer = buffer.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .finish();
        tracing::subscriber::with_default(subscriber, run);
        buffer.contents()
    }

    fn job() -> JobPayload {
        let mut job = JobPayload::new("FooJob", vec![]);
        job.jid = "1234abc".into();
        job
    }

    #[test]
    fn logs_start_and_done_with_the_job_context() {
        let mut job = job();
        job.tags = vec!["bar".into(), "baz".into()];
        job.bid = Some("b-xyz".into());
        let logger = JobLogger::new(true);
        let output = capture(|| {
            let _entered = job_span(&job).entered();
            logger.start();
            logger.done(Duration::from_millis(1500));
        });
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines.len(), 2, "{output}");
        for line in &lines {
            assert!(line.contains("class=FooJob"), "{line}");
            assert!(line.contains("jid=1234abc"), "{line}");
            assert!(line.contains("tags=bar,baz"), "{line}");
            assert!(line.contains("bid=b-xyz"), "{line}");
        }
        assert!(lines[0].ends_with("start"), "{}", lines[0]);
        assert!(lines[1].contains("elapsed=1.5"), "{}", lines[1]);
        assert!(lines[1].contains("done"), "{}", lines[1]);
    }

    #[test]
    fn logs_fail() {
        let logger = JobLogger::new(true);
        let job = job();
        let output = capture(|| {
            let _entered = job_span(&job).entered();
            logger.start();
            logger.fail(Duration::from_millis(2));
        });
        assert!(output.contains("fail"), "{output}");
        assert!(!output.contains("done"), "{output}");
        assert!(output.contains("elapsed=0.002"), "{output}");
    }

    #[test]
    fn interrupted_jobs_do_not_log_fail() {
        let logger = JobLogger::new(true);
        let job = job();
        let output = capture(|| {
            let _entered = job_span(&job).entered();
            logger.start();
            logger.interrupted(Duration::from_millis(7));
        });
        assert!(output.contains("interrupted"), "{output}");
        assert!(output.contains("elapsed=0.007"), "{output}");
        assert!(
            !output.contains("fail") && !output.contains("done"),
            "{output}"
        );
    }

    #[test]
    fn can_be_disabled() {
        let logger = JobLogger::new(false);
        let job = job();
        let output = capture(|| {
            let _entered = job_span(&job).entered();
            tracing::info!("mike");
            logger.start();
            logger.done(Duration::ZERO);
            logger.fail(Duration::ZERO);
            logger.interrupted(Duration::ZERO);
        });
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines.len(), 1, "{output}");
        // The span still gives user logs the job context.
        assert!(lines[0].contains("mike") && lines[0].contains("jid=1234abc"));
    }

    #[test]
    fn optional_fields_are_omitted() {
        let logger = JobLogger::new(true);
        let job = job();
        let output = capture(|| {
            let _entered = job_span(&job).entered();
            logger.start();
        });
        assert!(!output.contains("bid="), "{output}");
        assert!(!output.contains("tags="), "{output}");
        assert!(!output.contains("attrs="), "{output}");
    }

    /// Sidekiq's `logged_job_attributes`: the selected payload keys are on every line logged
    /// while the job runs.
    #[test]
    fn logged_attributes_are_on_every_line() {
        let logger = JobLogger::new(true);
        let mut job = job();
        job.extra.insert("request_id".to_owned(), "req-4242".into());
        job.extra.insert("tenant".to_owned(), 7.into());
        job.extra.insert("nothing".to_owned(), Value::Null);
        job.tags = vec!["bar".into()];
        job.queue = "critical".into();
        let attributes: Vec<String> = ["request_id", "queue", "tags", "missing", "nothing"]
            .map(String::from)
            .into();
        let output = capture(|| {
            let _entered = job_span_with(&job, &attributes).entered();
            logger.start();
            tracing::info!("inside the job");
            logger.done(Duration::ZERO);
        });
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines.len(), 3, "{output}");
        for line in &lines {
            // Custom keys and payload fields alike; keys with fields of their own (`tags`),
            // missing keys and nulls are left out, and unlisted keys (`tenant`) too.
            assert!(
                line.contains(r#"attrs={"queue":"critical","request_id":"req-4242"}"#),
                "{line}"
            );
            assert!(line.contains("tags=bar"), "{line}");
        }
        // No attributes listed, or none present: no field.
        assert_eq!(logged_attributes(&job, &[]), None);
        assert_eq!(logged_attributes(&job, &["missing".to_owned()]), None);
        assert_eq!(
            logged_attributes(&job, &["tenant".to_owned(), "tenant".to_owned()]).as_deref(),
            Some(r#"{"tenant":7}"#)
        );
    }

    /// Runs `run` with a subscriber filtering through `EnvFilter::new(filter)`.
    fn capture_filtered(filter: &str, run: impl FnOnce()) -> String {
        std::sync::LazyLock::force(&SECOND_DISPATCHER);
        let buffer = Buffer::default();
        let writer = buffer.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .with_env_filter(tracing_subscriber::EnvFilter::new(filter))
            .finish();
        tracing::subscriber::with_default(subscriber, run);
        buffer.contents()
    }

    fn job_at(level: &str) -> JobPayload {
        let mut job = job();
        job.extra.insert(LOG_LEVEL_KEY.to_owned(), level.into());
        job
    }

    #[test]
    fn job_log_levels() {
        assert_eq!(job_log_level(&job()), None);
        assert_eq!(job_log_level(&job_at("debug")), Some(Level::DEBUG));
        assert_eq!(job_log_level(&job_at("WARN")), Some(Level::WARN));
        assert_eq!(job_log_level(&job_at("warning")), Some(Level::WARN));
        assert_eq!(job_log_level(&job_at("fatal")), Some(Level::ERROR));
        assert_eq!(job_log_level(&job_at("loud")), None);
        for level in [
            Level::TRACE,
            Level::DEBUG,
            Level::INFO,
            Level::WARN,
            Level::ERROR,
        ] {
            assert_eq!(job_log_level(&job_at(level_name(level))), Some(level));
        }
        let options = crate::JobOptions::new().log_level(Level::DEBUG);
        assert_eq!(options.custom[LOG_LEVEL_KEY], "debug");
    }

    #[test]
    fn jobs_above_info_skip_the_job_events() {
        let logger = JobLogger::new(true);
        for (level, lines) in [("warn", 0), ("error", 0), ("info", 2), ("debug", 2)] {
            let job = job_at(level);
            let output = capture(|| {
                let _entered = job_span(&job).entered();
                let logger = logger.for_job(&job);
                logger.start();
                logger.done(Duration::ZERO);
            });
            assert_eq!(output.lines().count(), lines, "{level}: {output}");
            if lines > 0 {
                assert!(output.contains(&format!("log_level={level}")), "{output}");
            }
        }
        // Disabled stays disabled.
        assert!(!JobLogger::new(false).for_job(&job_at("debug")).enabled);
    }

    #[test]
    fn directives_let_debug_jobs_log_at_debug() {
        let filter = format!("info,{}", crate::JOB_LOG_LEVEL_DIRECTIVES);
        let output = capture_filtered(&filter, || {
            let debug_job = job_at("debug");
            let span = job_span(&debug_job);
            span.in_scope(|| tracing::debug!("details of the debug job"));
            let plain = job();
            job_span(&plain).in_scope(|| tracing::debug!("details of a plain job"));
            let info_job = job_at("info");
            job_span(&info_job).in_scope(|| {
                tracing::debug!("details of an info job");
                tracing::info!("info of an info job");
            });
        });
        assert!(output.contains("details of the debug job"), "{output}");
        assert!(!output.contains("details of a plain job"), "{output}");
        assert!(!output.contains("details of an info job"), "{output}");
        assert!(output.contains("info of an info job"), "{output}");

        // A process logging at warn still logs the start of an info job.
        let output = capture_filtered(&format!("warn,{}", crate::JOB_LOG_LEVEL_DIRECTIVES), || {
            let info_job = job_at("info");
            job_span(&info_job).in_scope(|| JobLogger::new(true).for_job(&info_job).start());
            tracing::info!("not from a job");
        });
        let plain = job();
        let silent = capture_filtered(&format!("warn,{}", crate::JOB_LOG_LEVEL_DIRECTIVES), || {
            let _entered = job_span(&plain).entered();
            JobLogger::new(true).for_job(&plain).start();
        });
        assert!(output.ends_with("start\n"), "{output}");
        assert!(silent.is_empty(), "{silent}");
    }

    #[test]
    fn elapsed_is_rounded_to_milliseconds() {
        assert_eq!(elapsed_secs(Duration::from_micros(1_234_567)), 1.235);
        assert_eq!(elapsed_secs(Duration::ZERO), 0.0);
    }
}
