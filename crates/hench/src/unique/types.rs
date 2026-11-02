//! The types of unique jobs: the policies, the conflict strategies, the `unique` option and
//! payload field, the outcome of a push, and the crate-internal lock handles the engine passes
//! around (`docs/design/unique.md` §2.6, §2.9).

use std::fmt;
use std::time::Duration;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use crate::payload::JobPayload;

/// Longest `unique_for`, in seconds, excluded (larger values look like timestamps).
pub(crate) const MAX_UNIQUE_FOR_SECS: u64 = 1_000_000_000;

/// When a unique job's lock is released (the lifecycle table of [unique.md] §2.3).
///
/// [unique.md]: https://github.com/rkwap/hench/blob/v0.1.0/docs/design/unique.md
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum UniqueUntil {
    /// Held while the job waits, runs and retries; released when it succeeds (or server
    /// middleware skips it), dies, is discarded, or is deleted or killed through the API: "at
    /// most one pending or running copy". The default.
    #[default]
    Success,
    /// Released when `perform` starts (after server middleware); taken back, if free, when the
    /// job waits again for a retry: "at most one waiting copy".
    Start,
    /// Held for its whole time to live whatever happens: "at most one per window".
    Expired,
}

impl UniqueUntil {
    /// The name stored in payloads and locks: `success`, `start` or `expired`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Start => "start",
            Self::Expired => "expired",
        }
    }

    /// The policy named `name` (`success`, `start` or `expired`), if it is one.
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "success" => Some(Self::Success),
            "start" => Some(Self::Start),
            "expired" => Some(Self::Expired),
            _ => None,
        }
    }
}

impl fmt::Display for UniqueUntil {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for UniqueUntil {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for UniqueUntil {
    /// Lenient: a name this version does not know (written by a newer one) reads as
    /// [`UniqueUntil::Success`], the policy that keeps a lock the longest among those it knows.
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let name = String::deserialize(deserializer)?;
        Ok(Self::parse(&name).unwrap_or_default())
    }
}

/// What a push does when another job holds the lock ([unique.md] §2.5).
///
/// [unique.md]: https://github.com/rkwap/hench/blob/v0.1.0/docs/design/unique.md
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum OnConflict {
    /// The job is not pushed: `perform_async` and `push` return `Ok(None)`, `push_outcome`
    /// returns [`PushOutcome::Duplicate`]. The default.
    #[default]
    Drop,
    /// The job is not pushed and the push fails with
    /// [`Error::DuplicateJob`](crate::Error::DuplicateJob) (a bulk push reports `None` for it).
    Error,
    /// Debounce: if the holder still waits in the schedule, at the score its lock recorded, the
    /// new job replaces it and takes the lock; otherwise like [`OnConflict::Drop`].
    Replace,
}

impl OnConflict {
    /// The name used in payloads, scripts and configuration: `drop`, `error` or `replace`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Drop => "drop",
            Self::Error => "error",
            Self::Replace => "replace",
        }
    }

    /// The strategy named `name`, if it is one.
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "drop" => Some(Self::Drop),
            "error" => Some(Self::Error),
            "replace" => Some(Self::Replace),
            _ => None,
        }
    }
}

impl fmt::Display for OnConflict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for OnConflict {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for OnConflict {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let name = String::deserialize(deserializer)?;
        Self::parse(&name).ok_or_else(|| {
            D::Error::custom(format!(
                "`{name}` is not a conflict strategy (drop, error or replace)"
            ))
        })
    }
}

/// Uniqueness settings. Every field is optional so that the layers of options (the default job
/// options, the job type's options, the options of one push) merge field by field: a push with
/// `unique_for(300)` keeps the job type's `unique_until`. Stored in the payload's `unique`
/// object (`{"for": 600, "until": "success", "digest": "..."}` once pushed).
///
/// ```
/// use hench::unique::{OnConflict, Unique, UniqueUntil};
/// use std::time::Duration;
///
/// let unique = Unique::new(Duration::from_secs(600))
///     .until(UniqueUntil::Start)
///     .on_conflict(OnConflict::Error);
/// assert_eq!(unique.ttl, Some(Duration::from_secs(600)));
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct Unique {
    /// How long the lock lives at least (`for`, whole seconds, at least 1 s); required by the
    /// time of the push. A scheduled job's lock also covers its delay.
    pub ttl: Option<Duration>,
    /// When the lock is released (`until`; default [`UniqueUntil::Success`]).
    pub until: Option<UniqueUntil>,
    /// What a push does when another job holds the lock (`on_conflict`; default
    /// [`OnConflict::Drop`]). Used at push time only, never stored.
    pub on_conflict: Option<OnConflict>,
    /// Whether the lock ignores the queue (`across_queues`; default `false`). Push time only.
    pub across_queues: Option<bool>,
    /// The value identifying duplicates instead of the arguments (`key`). Push time only.
    pub key: Option<Value>,
    /// The lock's digest (`digest`), computed at the first push and stored with the job.
    pub digest: Option<String>,
}

impl Unique {
    /// Uniqueness for `ttl` (whole seconds, at least one), every other field unset.
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl: Some(ttl),
            ..Self::default()
        }
    }

    /// Sets when the lock is released.
    #[must_use]
    pub fn until(mut self, until: UniqueUntil) -> Self {
        self.until = Some(until);
        self
    }

    /// Sets what a push does when another job holds the lock.
    #[must_use]
    pub fn on_conflict(mut self, on_conflict: OnConflict) -> Self {
        self.on_conflict = Some(on_conflict);
        self
    }

    /// Sets whether the lock ignores the queue.
    #[must_use]
    pub fn across_queues(mut self, across: bool) -> Self {
        self.across_queues = Some(across);
        self
    }

    /// Identifies duplicates by `key` instead of the arguments.
    #[must_use]
    pub fn key(mut self, key: impl Into<Value>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// These settings overlaid with `overrides`: the fields `overrides` sets win, the others
    /// fall through.
    #[must_use]
    pub fn merge(&self, overrides: &Unique) -> Unique {
        Unique {
            ttl: overrides.ttl.or(self.ttl),
            until: overrides.until.or(self.until),
            on_conflict: overrides.on_conflict.or(self.on_conflict),
            across_queues: overrides.across_queues.or(self.across_queues),
            key: overrides.key.clone().or_else(|| self.key.clone()),
            digest: overrides.digest.clone().or_else(|| self.digest.clone()),
        }
    }
}

/// The JSON form of [`Unique`].
#[derive(Serialize, Deserialize)]
struct UniqueRepr {
    #[serde(
        rename = "for",
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "whole_seconds"
    )]
    ttl: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    until: Option<UniqueUntil>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    on_conflict: Option<OnConflict>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    across_queues: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    key: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    digest: Option<String>,
}

/// Whole seconds from an integer, or a float truncated (like `retry_for`).
fn whole_seconds<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<u64>, D::Error> {
    match Option::<Value>::deserialize(deserializer)? {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(number)) => {
            if let Some(secs) = number.as_u64() {
                Ok(Some(secs))
            } else if let Some(secs) = number
                .as_f64()
                .filter(|secs| secs.is_finite() && *secs >= 0.0)
            {
                // `as` saturates for out-of-range floats.
                Ok(Some(secs.trunc() as u64))
            } else {
                Err(D::Error::custom(
                    "`for` must be a non-negative number of seconds",
                ))
            }
        }
        Some(_) => Err(D::Error::custom("`for` must be a number of seconds")),
    }
}

impl Serialize for Unique {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        UniqueRepr {
            ttl: self.ttl.map(|ttl| ttl.as_secs()),
            until: self.until,
            on_conflict: self.on_conflict,
            across_queues: self.across_queues,
            key: self.key.clone(),
            digest: self.digest.clone(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Unique {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let repr = UniqueRepr::deserialize(deserializer)?;
        Ok(Self {
            ttl: repr.ttl.map(Duration::from_secs),
            until: repr.until,
            on_conflict: repr.on_conflict,
            across_queues: repr.across_queues,
            key: repr.key,
            digest: repr.digest,
        })
    }
}

/// The `unique` option of a job: the same type in [`JobOptions`](crate::JobOptions) and in
/// [`JobPayload`] (like [`RetryOption`](crate::RetryOption)).
///
/// Layers merge field by field: a higher [`Disabled`](Self::Disabled) wins; two
/// [`Enabled`](Self::Enabled) merge per field; a higher `Enabled` over a lower `Disabled` is
/// the higher one alone.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum UniqueOption {
    /// Not unique, whatever lower layers say (Sidekiq's `unique_for: false`). JSON `false`.
    Disabled,
    /// Unique with these settings. A JSON object.
    Enabled(Unique),
}

impl UniqueOption {
    /// `upper` over `self` (see the merge rules above).
    #[must_use]
    pub fn merge(&self, upper: &UniqueOption) -> UniqueOption {
        match (self, upper) {
            (_, UniqueOption::Disabled) => UniqueOption::Disabled,
            (UniqueOption::Enabled(lower), UniqueOption::Enabled(upper)) => {
                UniqueOption::Enabled(lower.merge(upper))
            }
            (UniqueOption::Disabled, enabled) => enabled.clone(),
        }
    }

    /// The settings, when enabled.
    pub fn settings(&self) -> Option<&Unique> {
        match self {
            UniqueOption::Disabled => None,
            UniqueOption::Enabled(unique) => Some(unique),
        }
    }
}

impl From<Unique> for UniqueOption {
    fn from(unique: Unique) -> Self {
        UniqueOption::Enabled(unique)
    }
}

impl From<Duration> for UniqueOption {
    fn from(ttl: Duration) -> Self {
        UniqueOption::Enabled(Unique::new(ttl))
    }
}

impl Serialize for UniqueOption {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            UniqueOption::Disabled => serializer.serialize_bool(false),
            UniqueOption::Enabled(unique) => unique.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for UniqueOption {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match Value::deserialize(deserializer)? {
            Value::Bool(false) => Ok(UniqueOption::Disabled),
            Value::Object(object) => Unique::deserialize(Value::Object(object))
                .map(UniqueOption::Enabled)
                .map_err(D::Error::custom),
            other => Err(D::Error::custom(format!(
                "`unique` must be `false` or an object, not {other}"
            ))),
        }
    }
}

/// What a push did (`Client::push_outcome`, `JobSetter::push_outcome`).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum PushOutcome {
    /// The job was pushed (on a collecting client of a batch: buffered, see the batches).
    Pushed {
        /// Its jid.
        jid: String,
    },
    /// The job was pushed and replaced the holder of its lock, which waited in the schedule
    /// ([`OnConflict::Replace`]).
    Replaced {
        /// The new job's jid.
        jid: String,
        /// The jid of the job it replaced (now gone).
        replaced: String,
    },
    /// The job was not pushed: another job holds its lock ([`OnConflict::Drop`]).
    Duplicate {
        /// The jid the job would have had.
        jid: String,
        /// The jid of the job holding the lock.
        holder: String,
    },
    /// Client middleware stopped the push.
    Stopped,
}

impl PushOutcome {
    /// The jid of the job now in Redis because of this push ([`Pushed`](Self::Pushed),
    /// [`Replaced`](Self::Replaced)).
    pub fn jid(&self) -> Option<&str> {
        match self {
            PushOutcome::Pushed { jid } | PushOutcome::Replaced { jid, .. } => Some(jid),
            PushOutcome::Duplicate { .. } | PushOutcome::Stopped => None,
        }
    }
}

/// The uniqueness of a stored job, as the dashboard and the API show it
/// (`JobRecord::unique`): read leniently from the job's `unique` field.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct UniqueRef {
    /// The lock's digest.
    pub digest: String,
    /// When the lock is released.
    pub until: UniqueUntil,
    /// The lock's time to live (`for`).
    pub unique_for: Duration,
}

/// The lock a fetched job holds, from the job as fetched (the processor never follows what
/// server middleware changed): built for `Enabled` uniqueness with a valid digest and a time to
/// live only. Crate-internal; the processor passes it on without looking inside.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LockRef {
    /// The lock's digest.
    pub(crate) digest: String,
    /// The lock's key, `unique:<digest>`.
    pub(crate) key: String,
    /// The job's jid (the holder it should be).
    pub(crate) jid: String,
    /// The job's class.
    pub(crate) class: String,
    /// The job's queue.
    pub(crate) queue: String,
    /// When the lock is released.
    pub(crate) until: UniqueUntil,
    /// The lock's time to live.
    pub(crate) ttl: Duration,
}

/// Characters a digest may contain, and their bounds (`docs/design/unique.md` §2.4).
pub(crate) fn valid_digest(digest: &str) -> bool {
    (1..=128).contains(&digest.len())
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

impl LockRef {
    /// The lock of `job` (as fetched), or `None` when the job is not unique, or its `unique`
    /// field lacks a valid digest or time to live (logged: such a job runs without a lock).
    pub(crate) fn from_payload(job: &JobPayload) -> Option<Self> {
        Self::parse(job, true)
    }

    /// [`LockRef::from_payload`] without the warning (for callers that read the same job again,
    /// or show it).
    pub(crate) fn from_payload_quietly(job: &JobPayload) -> Option<Self> {
        Self::parse(job, false)
    }

    fn parse(job: &JobPayload, warn: bool) -> Option<Self> {
        let unique = job.unique.as_ref()?.settings()?;
        let (Some(digest), Some(ttl)) = (unique.digest.as_deref(), unique.ttl) else {
            if warn {
                tracing::warn!(
                    jid = %job.jid,
                    "a unique job has no digest or no `for`; it runs without its lock"
                );
            }
            return None;
        };
        if !valid_digest(digest) || ttl.as_secs() == 0 || job.jid.is_empty() {
            if warn {
                tracing::warn!(
                    jid = %job.jid,
                    "a unique job has an invalid digest or `for`; it runs without its lock"
                );
            }
            return None;
        }
        Some(Self {
            digest: digest.to_owned(),
            key: crate::keys::unique_lock(digest),
            jid: job.jid.clone(),
            class: job.class.clone(),
            queue: job.queue.clone(),
            until: unique.until.unwrap_or_default(),
            ttl: Duration::from_secs(ttl.as_secs()),
        })
    }

    /// The lock's time to live in milliseconds (`for`).
    pub(crate) fn ttl_ms(&self) -> u64 {
        self.ttl.as_secs().saturating_mul(1000)
    }

    /// Whether the job's outcomes and removals hold or release the lock: every policy but
    /// [`UniqueUntil::Expired`], whose lock lives its whole time to live whatever happens.
    pub(crate) fn follows_the_job(&self) -> bool {
        self.until != UniqueUntil::Expired
    }
}

/// What an outcome script does with a job's lock (`unique::engine::lock_op` decides; the
/// fetcher's outcome writes carry it). Crate-internal and opaque to the processor and the
/// fetcher: only the unique engine reads it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LockOp {
    /// The lock.
    pub(crate) lock: LockRef,
    /// What to do with it.
    pub(crate) action: LockAction,
}

/// See [`LockOp`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum LockAction {
    /// Keep the lock if this job holds it (extending it to at least `ttl` plus the delay of the
    /// destination), take it if it is free, leave it to another holder.
    Hold,
    /// Release the lock if this job holds it.
    Release,
}

impl LockAction {
    /// The name the outcome scripts take: `hold` or `release`.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            LockAction::Hold => "hold",
            LockAction::Release => "release",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn policies_and_strategies_have_names() {
        for until in [
            UniqueUntil::Success,
            UniqueUntil::Start,
            UniqueUntil::Expired,
        ] {
            assert_eq!(UniqueUntil::parse(until.as_str()), Some(until));
            assert_eq!(serde_json::to_value(until).unwrap(), json!(until.as_str()));
        }
        // A policy of a newer version reads as the one keeping locks the longest.
        assert_eq!(
            serde_json::from_value::<UniqueUntil>(json!("forever")).unwrap(),
            UniqueUntil::Success
        );
        for strategy in [OnConflict::Drop, OnConflict::Error, OnConflict::Replace] {
            assert_eq!(OnConflict::parse(&strategy.to_string()), Some(strategy));
        }
        assert!(serde_json::from_value::<OnConflict>(json!("skip")).is_err());
        assert_eq!(UniqueUntil::default(), UniqueUntil::Success);
        assert_eq!(OnConflict::default(), OnConflict::Drop);
    }

    #[test]
    fn the_option_round_trips() {
        let unique = Unique::new(Duration::from_secs(600))
            .until(UniqueUntil::Start)
            .on_conflict(OnConflict::Replace)
            .across_queues(true)
            .key(json!([42]));
        let value = serde_json::to_value(UniqueOption::from(unique.clone())).unwrap();
        assert_eq!(
            value,
            json!({"for": 600, "until": "start", "on_conflict": "replace", "across_queues": true, "key": [42]})
        );
        assert_eq!(
            serde_json::from_value::<UniqueOption>(value).unwrap(),
            UniqueOption::Enabled(unique)
        );
        assert_eq!(
            serde_json::to_value(UniqueOption::Disabled).unwrap(),
            json!(false)
        );
        assert_eq!(
            serde_json::from_value::<UniqueOption>(json!(false)).unwrap(),
            UniqueOption::Disabled
        );
        // `for` accepts floats, truncated, like `retry_for`.
        let float: UniqueOption = serde_json::from_value(json!({"for": 90.9})).unwrap();
        assert_eq!(
            float.settings().and_then(|unique| unique.ttl),
            Some(Duration::from_secs(90))
        );
        for bad in [
            json!(true),
            json!(5),
            json!({"for": -1}),
            json!({"for": "1h"}),
        ] {
            assert!(
                serde_json::from_value::<UniqueOption>(bad.clone()).is_err(),
                "{bad}"
            );
        }
        assert_eq!(
            UniqueOption::from(Duration::from_secs(5)),
            Unique::new(Duration::from_secs(5)).into()
        );
    }

    #[test]
    fn layers_merge_field_by_field() {
        let class =
            UniqueOption::from(Unique::new(Duration::from_secs(600)).until(UniqueUntil::Start));
        let push = UniqueOption::from(Unique::new(Duration::from_secs(300)));
        let merged = class.merge(&push);
        let unique = merged.settings().unwrap();
        assert_eq!(unique.ttl, Some(Duration::from_secs(300)));
        assert_eq!(unique.until, Some(UniqueUntil::Start));
        // A higher `Disabled` wins; a higher `Enabled` over `Disabled` stands alone.
        assert_eq!(class.merge(&UniqueOption::Disabled), UniqueOption::Disabled);
        assert_eq!(UniqueOption::Disabled.merge(&push), push);
    }

    #[test]
    fn outcomes_name_the_job_in_redis() {
        let pushed = PushOutcome::Pushed { jid: "a".into() };
        let replaced = PushOutcome::Replaced {
            jid: "b".into(),
            replaced: "a".into(),
        };
        let duplicate = PushOutcome::Duplicate {
            jid: "c".into(),
            holder: "a".into(),
        };
        assert_eq!(pushed.jid(), Some("a"));
        assert_eq!(replaced.jid(), Some("b"));
        assert_eq!(duplicate.jid(), None);
        assert_eq!(PushOutcome::Stopped.jid(), None);
    }

    #[test]
    fn locks_come_from_valid_unique_fields_only() {
        let mut job = JobPayload::new("A", vec![]);
        job.jid = "0123456789abcdef01234567".into();
        job.queue = "default".into();
        assert_eq!(LockRef::from_payload(&job), None);
        let mut unique = Unique::new(Duration::from_secs(60));
        job.unique = Some(unique.clone().into());
        // No digest yet: no lock.
        assert_eq!(LockRef::from_payload(&job), None);
        unique.digest = Some("rP3m_-x".into());
        job.unique = Some(unique.clone().into());
        let lock = LockRef::from_payload(&job).unwrap();
        assert_eq!(lock.key, "unique:rP3m_-x");
        assert_eq!(lock.until, UniqueUntil::Success);
        assert_eq!(lock.ttl, Duration::from_secs(60));
        unique.digest = Some("not a digest".into());
        job.unique = Some(unique.into());
        assert_eq!(LockRef::from_payload(&job), None);
        job.unique = Some(UniqueOption::Disabled);
        assert_eq!(LockRef::from_payload(&job), None);
        assert!(valid_digest(&"a".repeat(128)));
        assert!(!valid_digest(&"a".repeat(129)));
        assert!(!valid_digest(""));
    }
}
