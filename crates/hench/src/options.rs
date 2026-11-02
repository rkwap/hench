//! Job options (Sidekiq's `sidekiq_options`).

use std::time::Duration;

use serde_json::{Map, Value};

use crate::limit::ThrottleOption;
use crate::payload::{BacktraceOption, JobPayload, RetryOption};
use crate::unique::{OnConflict, Unique, UniqueOption, UniqueUntil};

/// Options applied to a job when it is pushed (Sidekiq's `sidekiq_options`): queue, retry
/// policy, backtrace, tags, ...
///
/// Options come from three layers, highest precedence first:
///
/// 1. options given explicitly when pushing (`J::set().queue("low")`, fields set on a raw
///    [`JobPayload`]),
/// 2. the job type's options ([`Job::options`](crate::Job::options)),
/// 3. the process-wide `default_job_options` (`{queue: "default", retry: true}` unless
///    configured otherwise).
///
/// Every field is optional: unset fields fall through to the next layer.
///
/// ```
/// use hench::JobOptions;
/// use std::time::Duration;
///
/// let options = JobOptions::new()
///     .queue("mailers")
///     .retry(5)
///     .backtrace(10)
///     .tags(["email"])
///     .retry_for(Duration::from_secs(3600))
///     .custom("locale", "fr");
/// assert_eq!(options.queue.as_deref(), Some("mailers"));
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct JobOptions {
    /// Queue name.
    pub queue: Option<String>,
    /// Retry policy.
    pub retry: Option<RetryOption>,
    /// Whether to store error backtraces.
    pub backtrace: Option<BacktraceOption>,
    /// `Some(false)` keeps exhausted jobs out of the morgue.
    pub dead: Option<bool>,
    /// Tags shown in the dashboard.
    pub tags: Option<Vec<String>>,
    /// Time-based retry limit (whole seconds are stored; must be below 1e9 seconds).
    pub retry_for: Option<Duration>,
    /// Queue used for retries.
    pub retry_queue: Option<String>,
    /// Additional payload keys (for middleware). Keys that name payload fields are ignored.
    pub custom: Map<String, Value>,
    /// Uniqueness (unique jobs; see [`crate::unique`]). Layers merge field by field: a higher
    /// [`UniqueOption::Disabled`] wins, two `Enabled` merge per field.
    pub unique: Option<UniqueOption>,
    /// The throttle of the job's starts (rate limiting; see [`crate::limit`]). Layers merge
    /// like [`unique`](Self::unique).
    pub throttle: Option<ThrottleOption>,
}

impl JobOptions {
    /// Empty options (every field unset).
    pub fn new() -> Self {
        Self::default()
    }

    /// The built-in `default_job_options`: queue `default`, `retry: true`.
    pub fn defaults() -> Self {
        Self {
            queue: Some("default".to_owned()),
            retry: Some(RetryOption::Bool(true)),
            ..Self::default()
        }
    }

    /// Sets the queue.
    #[must_use]
    pub fn queue(mut self, queue: impl Into<String>) -> Self {
        self.queue = Some(queue.into());
        self
    }

    /// Sets the retry policy: `true`, `false` or a maximum number of retries.
    #[must_use]
    pub fn retry(mut self, retry: impl Into<RetryOption>) -> Self {
        self.retry = Some(retry.into());
        self
    }

    /// Sets whether to store error backtraces: `true`, `false` or a number of lines.
    #[must_use]
    pub fn backtrace(mut self, backtrace: impl Into<BacktraceOption>) -> Self {
        self.backtrace = Some(backtrace.into());
        self
    }

    /// `false` keeps the job out of the morgue when its retries are exhausted.
    #[must_use]
    pub fn dead(mut self, dead: bool) -> Self {
        self.dead = Some(dead);
        self
    }

    /// Sets the tags.
    #[must_use]
    pub fn tags<I, S>(mut self, tags: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.tags = Some(tags.into_iter().map(Into::into).collect());
        self
    }

    /// Retries the job for this long after its first failure instead of a fixed number of
    /// times.
    #[must_use]
    pub fn retry_for(mut self, retry_for: Duration) -> Self {
        self.retry_for = Some(retry_for);
        self
    }

    /// Sends retries of the job to this queue.
    #[must_use]
    pub fn retry_queue(mut self, queue: impl Into<String>) -> Self {
        self.retry_queue = Some(queue.into());
        self
    }

    /// Sets the log level of the job's executions (Sidekiq's `log_level` option), stored in
    /// the payload's `log_level` key. Workers record it on the job's `job` span and skip their
    /// own `start`/`done` events when it is above `INFO`; with a filter that includes
    /// [`JOB_LOG_LEVEL_DIRECTIVES`](crate::JOB_LOG_LEVEL_DIRECTIVES), a job at `DEBUG` (or
    /// `TRACE`) also gets its own debug events logged while the rest of the process logs at
    /// `INFO`.
    #[must_use]
    pub fn log_level(self, level: tracing::Level) -> Self {
        self.custom(
            crate::job_logger::LOG_LEVEL_KEY,
            crate::job_logger::level_name(level),
        )
    }

    /// Sets the whole uniqueness option ([`Unique`] settings, a [`Duration`] for
    /// `unique_for` alone, or [`UniqueOption::Disabled`]).
    #[must_use]
    pub fn unique(mut self, unique: impl Into<UniqueOption>) -> Self {
        self.unique = Some(unique.into());
        self
    }

    /// Makes the job unique for `ttl` (whole seconds, at least one): pushes of a job with the
    /// same class, queue and key are duplicates while its lock is held.
    #[must_use]
    pub fn unique_for(self, ttl: Duration) -> Self {
        self.unique_field(|unique| unique.ttl = Some(ttl))
    }

    /// When the lock of a unique job is released (default [`UniqueUntil::Success`]).
    #[must_use]
    pub fn unique_until(self, until: UniqueUntil) -> Self {
        self.unique_field(|unique| unique.until = Some(until))
    }

    /// What a push of a unique job does when another job holds its lock (default
    /// [`OnConflict::Drop`]).
    #[must_use]
    pub fn on_conflict(self, on_conflict: OnConflict) -> Self {
        self.unique_field(|unique| unique.on_conflict = Some(on_conflict))
    }

    /// Whether the lock of a unique job ignores its queue (default `false`).
    #[must_use]
    pub fn unique_across_queues(self, across: bool) -> Self {
        self.unique_field(|unique| unique.across_queues = Some(across))
    }

    /// The value identifying duplicates of a unique job instead of its arguments.
    #[must_use]
    pub fn unique_key(self, key: impl Into<Value>) -> Self {
        let key = key.into();
        self.unique_field(|unique| unique.key = Some(key))
    }

    /// Not unique, whatever lower layers say.
    #[doc(alias = "no_unique")]
    #[must_use]
    pub fn not_unique(mut self) -> Self {
        self.unique = Some(UniqueOption::Disabled);
        self
    }

    /// Sets one field of the uniqueness settings (enabling uniqueness).
    fn unique_field(mut self, set: impl FnOnce(&mut Unique)) -> Self {
        let mut unique = match self.unique.take() {
            Some(UniqueOption::Enabled(unique)) => unique,
            Some(UniqueOption::Disabled) | None => Unique::default(),
        };
        set(&mut unique);
        self.unique = Some(UniqueOption::Enabled(unique));
        self
    }

    /// Subjects the job's starts to the throttle `name` (defined with
    /// [`HenchBuilder::throttle`](crate::HenchBuilder::throttle) or `Job::throttle`).
    #[must_use]
    pub fn throttle(self, name: impl Into<String>) -> Self {
        let name = name.into();
        self.throttle_field(|current, _| *current = Some(name))
    }

    /// The throttle key of the job (tenant, account): the throttle's per-key limits apply to it.
    #[must_use]
    pub fn throttle_key(self, key: impl Into<String>) -> Self {
        let key = key.into();
        self.throttle_field(|_, current| *current = Some(key))
    }

    /// Not throttled, whatever lower layers say.
    #[doc(alias = "not_throttled")]
    #[must_use]
    pub fn no_throttle(mut self) -> Self {
        self.throttle = Some(ThrottleOption::Disabled);
        self
    }

    /// Sets the name or key of the throttle option (enabling it).
    fn throttle_field(
        mut self,
        set: impl FnOnce(&mut Option<String>, &mut Option<String>),
    ) -> Self {
        let (mut name, mut key) = match self.throttle.take() {
            Some(ThrottleOption::Enabled { name, key }) => (name, key),
            Some(ThrottleOption::Disabled) | None => (None, None),
        };
        set(&mut name, &mut key);
        self.throttle = Some(ThrottleOption::Enabled { name, key });
        self
    }

    /// Adds a custom payload key. Keys naming payload fields (see
    /// [`JobPayload::RESERVED_KEYS`]) are ignored with a warning: use the typed setters.
    #[must_use]
    pub fn custom(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        let key = key.into();
        if JobPayload::is_reserved_key(&key) {
            tracing::warn!(
                key,
                "ignoring custom job option that names a payload field; use the typed setter"
            );
        } else {
            self.custom.insert(key, value.into());
        }
        self
    }

    /// Returns these options overlaid with `overrides`: every field set in `overrides` wins;
    /// custom keys are merged with `overrides` winning on conflicts.
    #[must_use]
    pub fn merge(&self, overrides: &JobOptions) -> JobOptions {
        let mut custom = self.custom.clone();
        for (key, value) in &overrides.custom {
            custom.insert(key.clone(), value.clone());
        }
        JobOptions {
            queue: overrides.queue.clone().or_else(|| self.queue.clone()),
            retry: overrides.retry.or(self.retry),
            backtrace: overrides.backtrace.or(self.backtrace),
            dead: overrides.dead.or(self.dead),
            tags: overrides.tags.clone().or_else(|| self.tags.clone()),
            retry_for: overrides.retry_for.or(self.retry_for),
            retry_queue: overrides
                .retry_queue
                .clone()
                .or_else(|| self.retry_queue.clone()),
            custom,
            unique: merge_unique(self.unique.as_ref(), overrides.unique.as_ref()),
            throttle: merge_throttle(self.throttle.as_ref(), overrides.throttle.as_ref()),
        }
    }

    /// Sets every field these options set on `payload`, replacing its value (custom keys are
    /// added or replaced): the counterpart of [`JobOptions::merge`] for a payload.
    pub(crate) fn apply_overrides(&self, payload: &mut JobPayload) {
        if let Some(queue) = &self.queue {
            payload.queue.clone_from(queue);
        }
        if self.retry.is_some() {
            payload.retry = self.retry;
        }
        if self.backtrace.is_some() {
            payload.backtrace = self.backtrace;
        }
        if self.dead.is_some() {
            payload.dead = self.dead;
        }
        if let Some(tags) = &self.tags {
            payload.tags.clone_from(tags);
        }
        if let Some(retry_for) = self.retry_for {
            payload.retry_for = Some(retry_for.as_secs());
        }
        if self.retry_queue.is_some() {
            payload.retry_queue.clone_from(&self.retry_queue);
        }
        if self.unique.is_some() {
            payload.unique = merge_unique(payload.unique.as_ref(), self.unique.as_ref());
        }
        if self.throttle.is_some() {
            payload.throttle = merge_throttle(payload.throttle.as_ref(), self.throttle.as_ref());
        }
        for (key, value) in &self.custom {
            if !JobPayload::is_reserved_key(key) {
                payload.extra.insert(key.clone(), value.clone());
            }
        }
    }

    /// Fills the fields `payload` leaves unset from these options. Fields the payload already
    /// sets (a non-empty queue, `Some` options, non-empty tags, existing extra keys) win.
    pub fn apply_defaults(&self, payload: &mut JobPayload) {
        if payload.queue.is_empty()
            && let Some(queue) = &self.queue
        {
            payload.queue.clone_from(queue);
        }
        if payload.retry.is_none() {
            payload.retry = self.retry;
        }
        if payload.backtrace.is_none() {
            payload.backtrace = self.backtrace;
        }
        if payload.dead.is_none() {
            payload.dead = self.dead;
        }
        if payload.tags.is_empty()
            && let Some(tags) = &self.tags
        {
            payload.tags.clone_from(tags);
        }
        if payload.retry_for.is_none() {
            payload.retry_for = self.retry_for.map(|retry_for| retry_for.as_secs());
        }
        if payload.retry_queue.is_none() {
            payload.retry_queue.clone_from(&self.retry_queue);
        }
        if self.unique.is_some() {
            payload.unique = merge_unique(self.unique.as_ref(), payload.unique.as_ref());
        }
        if self.throttle.is_some() {
            payload.throttle = merge_throttle(self.throttle.as_ref(), payload.throttle.as_ref());
        }
        for (key, value) in &self.custom {
            if !JobPayload::is_reserved_key(key) && !payload.extra.contains_key(key) {
                payload.extra.insert(key.clone(), value.clone());
            }
        }
    }
}

/// `upper` over `lower` (see [`UniqueOption::merge`]); an unset layer leaves the other.
fn merge_unique(
    lower: Option<&UniqueOption>,
    upper: Option<&UniqueOption>,
) -> Option<UniqueOption> {
    match (lower, upper) {
        (Some(lower), Some(upper)) => Some(lower.merge(upper)),
        (lower, upper) => upper.or(lower).cloned(),
    }
}

/// `upper` over `lower` (see [`ThrottleOption::merge`]); an unset layer leaves the other.
fn merge_throttle(
    lower: Option<&ThrottleOption>,
    upper: Option<&ThrottleOption>,
) -> Option<ThrottleOption> {
    match (lower, upper) {
        (Some(lower), Some(upper)) => Some(lower.merge(upper)),
        (lower, upper) => upper.or(lower).cloned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn unique_setters_merge_into_one_option() {
        let options = JobOptions::new()
            .unique_for(Duration::from_secs(600))
            .unique_until(UniqueUntil::Start)
            .on_conflict(OnConflict::Error)
            .unique_across_queues(true)
            .unique_key(json!([42]));
        let unique = options
            .unique
            .as_ref()
            .and_then(UniqueOption::settings)
            .unwrap();
        assert_eq!(unique.ttl, Some(Duration::from_secs(600)));
        assert_eq!(unique.until, Some(UniqueUntil::Start));
        assert_eq!(unique.on_conflict, Some(OnConflict::Error));
        assert_eq!(unique.across_queues, Some(true));
        assert_eq!(unique.key, Some(json!([42])));
        assert_eq!(
            options.clone().not_unique().unique,
            Some(UniqueOption::Disabled)
        );
        // A field set after `not_unique` enables uniqueness again, with that field alone.
        let again = options.not_unique().unique_until(UniqueUntil::Expired);
        let unique = again
            .unique
            .as_ref()
            .and_then(UniqueOption::settings)
            .unwrap();
        assert_eq!(
            (unique.ttl, unique.until),
            (None, Some(UniqueUntil::Expired))
        );
        let whole = JobOptions::new().unique(Duration::from_secs(5));
        assert_eq!(
            whole.unique,
            Some(UniqueOption::Enabled(Unique::new(Duration::from_secs(5))))
        );
    }

    #[test]
    fn unique_layers_merge_field_by_field() {
        let class = JobOptions::new()
            .unique_for(Duration::from_secs(600))
            .unique_until(UniqueUntil::Start);
        let explicit = JobOptions::new().unique_for(Duration::from_secs(60));
        let merged = class.merge(&explicit);
        let unique = merged
            .unique
            .as_ref()
            .and_then(UniqueOption::settings)
            .unwrap();
        assert_eq!(unique.ttl, Some(Duration::from_secs(60)));
        assert_eq!(unique.until, Some(UniqueUntil::Start));
        // A higher `Disabled` wins; a higher `Enabled` over a lower `Disabled` is alone.
        assert_eq!(
            class.merge(&JobOptions::new().not_unique()).unique,
            Some(UniqueOption::Disabled)
        );
        let over_disabled = JobOptions::new().not_unique().merge(&explicit);
        assert_eq!(over_disabled.unique, explicit.unique);
        assert_eq!(class.merge(&JobOptions::new()).unique, class.unique);
        // The payload is the upper layer of `apply_defaults` and the lower of
        // `apply_overrides`.
        let mut payload = JobPayload::new("A", vec![]);
        payload.unique = Some(UniqueOption::Enabled(Unique::default().key(json!("k"))));
        class.apply_defaults(&mut payload);
        let unique = payload
            .unique
            .as_ref()
            .and_then(UniqueOption::settings)
            .unwrap();
        assert_eq!(unique.ttl, Some(Duration::from_secs(600)));
        assert_eq!(unique.key, Some(json!("k")));
        JobOptions::new().not_unique().apply_overrides(&mut payload);
        assert_eq!(payload.unique, Some(UniqueOption::Disabled));
        JobOptions::new().apply_overrides(&mut payload);
        assert_eq!(payload.unique, Some(UniqueOption::Disabled));
    }

    #[test]
    fn throttle_setters_and_layers() {
        let class = JobOptions::new().throttle("stripe");
        assert_eq!(class.throttle, Some(ThrottleOption::named("stripe")));
        let explicit = JobOptions::new().throttle_key("42");
        let merged = class.merge(&explicit).throttle.unwrap();
        assert_eq!((merged.name(), merged.key()), (Some("stripe"), Some("42")));
        assert_eq!(
            class.merge(&JobOptions::new().no_throttle()).throttle,
            Some(ThrottleOption::Disabled)
        );
        let tier = class.merge(&JobOptions::new().throttle("stripe-vip"));
        assert_eq!(
            tier.throttle.as_ref().and_then(ThrottleOption::name),
            Some("stripe-vip")
        );
        let mut payload = JobPayload::new("A", vec![]);
        explicit.apply_overrides(&mut payload);
        class.apply_defaults(&mut payload);
        let throttle = payload.throttle.unwrap();
        assert_eq!(
            (throttle.name(), throttle.key()),
            (Some("stripe"), Some("42"))
        );
    }

    #[test]
    fn builder_sets_fields() {
        let options = JobOptions::new()
            .queue("q")
            .retry(false)
            .backtrace(3_u32)
            .dead(false)
            .tags(["a", "b"])
            .retry_for(Duration::from_secs(90))
            .retry_queue("rq")
            .custom("locale", "fr");
        assert_eq!(options.queue.as_deref(), Some("q"));
        assert_eq!(options.retry, Some(RetryOption::Bool(false)));
        assert_eq!(options.backtrace, Some(BacktraceOption::Lines(3)));
        assert_eq!(options.dead, Some(false));
        assert_eq!(options.tags, Some(vec!["a".to_owned(), "b".to_owned()]));
        assert_eq!(options.retry_for, Some(Duration::from_secs(90)));
        assert_eq!(options.retry_queue.as_deref(), Some("rq"));
        assert_eq!(options.custom["locale"], json!("fr"));
    }

    #[test]
    fn custom_ignores_reserved_keys() {
        let options = JobOptions::new().custom("queue", "nope").custom("jid", "x");
        assert!(options.custom.is_empty());
    }

    #[test]
    fn defaults_match_default_job_options() {
        let defaults = JobOptions::defaults();
        assert_eq!(defaults.queue.as_deref(), Some("default"));
        assert_eq!(defaults.retry, Some(RetryOption::Bool(true)));
        assert_eq!(defaults.backtrace, None);
    }

    #[test]
    fn merge_precedence() {
        let defaults = JobOptions::defaults().custom("a", 1).custom("b", 1);
        let class = JobOptions::new().queue("class_q").retry(12).custom("b", 2);
        let explicit = JobOptions::new().queue("explicit_q").custom("c", 3);
        let merged = defaults.merge(&class).merge(&explicit);
        assert_eq!(merged.queue.as_deref(), Some("explicit_q"));
        assert_eq!(merged.retry, Some(RetryOption::Count(12)));
        assert_eq!(merged.custom["a"], json!(1));
        assert_eq!(merged.custom["b"], json!(2));
        assert_eq!(merged.custom["c"], json!(3));

        let merged = defaults.merge(&class);
        assert_eq!(merged.queue.as_deref(), Some("class_q"));
        let merged = defaults.merge(&JobOptions::new());
        assert_eq!(merged, defaults);
    }

    #[test]
    fn apply_defaults_only_fills_unset_fields() {
        let options = JobOptions::defaults()
            .backtrace(true)
            .dead(false)
            .tags(["t"])
            .retry_for(Duration::from_millis(1500))
            .retry_queue("rq")
            .custom("locale", "fr")
            .custom("keep", "options");

        let mut empty = JobPayload::new("A", vec![]);
        options.apply_defaults(&mut empty);
        assert_eq!(empty.queue, "default");
        assert_eq!(empty.retry, Some(RetryOption::Bool(true)));
        assert_eq!(empty.backtrace, Some(BacktraceOption::Bool(true)));
        assert_eq!(empty.dead, Some(false));
        assert_eq!(empty.tags, vec!["t"]);
        assert_eq!(empty.retry_for, Some(1));
        assert_eq!(empty.retry_queue.as_deref(), Some("rq"));
        assert_eq!(empty.extra["locale"], json!("fr"));

        let mut set = JobPayload::new("A", vec![]);
        set.queue = "mine".into();
        set.retry = Some(RetryOption::Count(1));
        set.tags = vec!["own".into()];
        set.extra.insert("keep".into(), json!("payload"));
        options.apply_defaults(&mut set);
        assert_eq!(set.queue, "mine");
        assert_eq!(set.retry, Some(RetryOption::Count(1)));
        assert_eq!(set.tags, vec!["own"]);
        assert_eq!(set.extra["keep"], json!("payload"));
    }

    #[test]
    fn apply_overrides_replaces_what_the_options_set() {
        let options = JobOptions::new()
            .queue("theirs")
            .dead(false)
            .tags(["t"])
            .retry_for(Duration::from_millis(1500))
            .retry_queue("rq")
            .custom("keep", "options");
        let mut set = JobPayload::new("A", vec![]);
        set.queue = "mine".into();
        set.retry = Some(RetryOption::Count(1));
        set.backtrace = Some(BacktraceOption::Lines(3));
        set.tags = vec!["own".into()];
        set.extra.insert("keep".into(), json!("payload"));
        set.extra.insert("other".into(), json!("payload"));
        options.apply_overrides(&mut set);
        assert_eq!(set.queue, "theirs");
        assert_eq!(set.dead, Some(false));
        assert_eq!(set.tags, vec!["t"]);
        assert_eq!(set.retry_for, Some(1));
        assert_eq!(set.retry_queue.as_deref(), Some("rq"));
        assert_eq!(set.extra["keep"], json!("options"));
        // What the options leave unset stays.
        assert_eq!(set.retry, Some(RetryOption::Count(1)));
        assert_eq!(set.backtrace, Some(BacktraceOption::Lines(3)));
        assert_eq!(set.extra["other"], json!("payload"));
    }
}
