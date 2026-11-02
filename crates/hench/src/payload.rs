//! The job payload (the JSON object stored in Redis) and related helpers: jid generation,
//! timestamps and the `error_backtrace` codec.

use std::io::{Read, Write};
use std::sync::Once;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};

use crate::batch::BatchCallback;
use crate::error::{Error, Result};
use crate::limit::{Limited, ThrottleOption};
use crate::periodic::PeriodicMeta;
use crate::unique::UniqueOption;

/// A job as stored in Redis.
///
/// Field names, types and semantics follow Sidekiq 8 (see [DESIGN.md] §2):
/// timestamps in the payload are integer **milliseconds** since the Unix epoch, while
/// sorted-set scores (and [`at`](Self::at)) are **seconds** as `f64`.
///
/// Keys this struct does not know about (for example ones added by client middleware) are
/// kept in [`extra`](Self::extra) and survive a round trip. `extra` must not contain the
/// name of a named field (see [`JobPayload::RESERVED_KEYS`]); such keys are dropped when a
/// job is pushed.
///
/// When pushing, an empty [`jid`](Self::jid) means "generate one" and an empty
/// [`queue`](Self::queue) means "use the job's default queue".
///
/// [DESIGN.md]: https://github.com/rkwap/hench/blob/v0.1.0/docs/DESIGN.md
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct JobPayload {
    /// The job's registered name (`Job::NAME`).
    pub class: String,

    /// Positional arguments.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub args: Vec<Value>,

    /// Job id: 24 lowercase hex characters.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub jid: String,

    /// Queue name.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub queue: String,

    /// Retry policy: `true` (default maximum), `false` (no retries, no morgue) or a maximum
    /// number of retries.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry: Option<RetryOption>,

    /// Whether to store the error backtrace on failure (all lines or the first `n`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backtrace: Option<BacktraceOption>,

    /// `Some(false)`: do not move the job to the morgue when its retries are exhausted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dead: Option<bool>,

    /// Free-form tags shown in the dashboard.
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_null_default"
    )]
    pub tags: Vec<String>,

    /// Time-based retry limit in seconds (instead of a retry count). Must be below 1e9.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_opt_seconds"
    )]
    pub retry_for: Option<u64>,

    /// Queue to use for retries of this job.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_queue: Option<String>,

    /// When the job was first pushed (epoch milliseconds).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_opt_millis"
    )]
    pub created_at: Option<i64>,

    /// When the job was last pushed to a queue (epoch milliseconds).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_opt_millis"
    )]
    pub enqueued_at: Option<i64>,

    /// Run-at time (epoch seconds) while a scheduled job is being pushed. It is stripped
    /// before the job is stored.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at: Option<f64>,

    /// Number of retries so far: absent before the first failure, `0` after it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_count: Option<u32>,

    /// First failure time (epoch milliseconds).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_opt_millis"
    )]
    pub failed_at: Option<i64>,

    /// Last retry time (epoch milliseconds).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_opt_millis"
    )]
    pub retried_at: Option<i64>,

    /// When the job was discarded instead of being retried or killed (epoch milliseconds).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_opt_millis"
    )]
    pub discarded_at: Option<i64>,

    /// Message of the last error (truncated to [`MAX_ERROR_MESSAGE_CHARS`] characters).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,

    /// Class of the last error, e.g. `std::io::error::Error`, `panic`, `hench::UnknownJob`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_class: Option<String>,

    /// Compressed backtrace of the last error, see [`compress_backtrace`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_backtrace: Option<String>,

    /// The batch this job is a member of (batches: set by the collecting client of a batch
    /// definition; a job becomes a member only through a commit).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bid: Option<String>,

    /// The batch and event a callback job runs for (batches; set on callback jobs only).
    /// Read leniently: a malformed value is `None`, with a warning.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_lenient"
    )]
    pub batch_callback: Option<BatchCallback>,

    /// Uniqueness (unique jobs): `false`, or an object; stored jobs carry `{"for", "until",
    /// "digest"}`. Read leniently: a malformed value is `None`, with a warning.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_lenient"
    )]
    pub unique: Option<UniqueOption>,

    /// The schedule and tick that enqueued the job (periodic jobs: `{"name", "tick",
    /// "manual"?}`). Read leniently: a malformed value is `None`, with a warning.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_lenient"
    )]
    pub periodic: Option<PeriodicMeta>,

    /// The throttle the job's starts are subject to (rate limiting: `{"name", "key"}`). Read
    /// leniently: a malformed value is `None`, with a warning.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_lenient"
    )]
    pub throttle: Option<ThrottleOption>,

    /// The reschedules of the job by the rate limiter so far (`{"n", "by", "at"}`). Read
    /// leniently: a malformed value is `None`, with a warning.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_lenient"
    )]
    pub limited: Option<Limited>,

    /// Any other keys.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// Maximum number of characters kept in `error_message`.
pub const MAX_ERROR_MESSAGE_CHARS: usize = 10_000;

impl JobPayload {
    /// Names of the fields of [`JobPayload`]. These keys may not appear in
    /// [`extra`](Self::extra).
    pub const RESERVED_KEYS: &'static [&'static str] = &[
        "class",
        "args",
        "jid",
        "queue",
        "retry",
        "backtrace",
        "dead",
        "tags",
        "retry_for",
        "retry_queue",
        "created_at",
        "enqueued_at",
        "at",
        "retry_count",
        "failed_at",
        "retried_at",
        "discarded_at",
        "error_message",
        "error_class",
        "error_backtrace",
        "bid",
    ];

    /// Creates a payload for `class` with positional `args`; every other field is unset.
    pub fn new(class: impl Into<String>, args: Vec<Value>) -> Self {
        Self {
            class: class.into(),
            args,
            ..Self::default()
        }
    }

    /// Names of the fields of [`JobPayload`] that belong to the pro features
    /// ([`batch_callback`](Self::batch_callback), [`unique`](Self::unique),
    /// [`periodic`](Self::periodic), [`throttle`](Self::throttle), [`limited`](Self::limited)).
    /// Like [`RESERVED_KEYS`](Self::RESERVED_KEYS), they may not appear in
    /// [`extra`](Self::extra).
    pub const FEATURE_KEYS: &'static [&'static str] = &[
        "batch_callback",
        "unique",
        "periodic",
        "throttle",
        "limited",
    ];

    /// `true` if `key` is the name of a named field ([`RESERVED_KEYS`](Self::RESERVED_KEYS) or
    /// [`FEATURE_KEYS`](Self::FEATURE_KEYS)).
    pub fn is_reserved_key(key: &str) -> bool {
        Self::RESERVED_KEYS.contains(&key) || Self::FEATURE_KEYS.contains(&key)
    }

    /// Parses a payload from JSON.
    pub fn from_json(json: &str) -> Result<Self> {
        Ok(serde_json::from_str(json)?)
    }

    /// Parses a payload from JSON bytes.
    pub fn from_slice(json: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(json)?)
    }

    /// Serializes the payload to compact JSON.
    pub fn to_json(&self) -> Result<String> {
        Ok(serde_json::to_string(self)?)
    }

    /// Serializes the payload to a JSON value.
    pub fn to_value(&self) -> Result<Value> {
        Ok(serde_json::to_value(self)?)
    }

    /// The effective retry policy: absent means retries are enabled (`retry: true`).
    pub fn retry_option(&self) -> RetryOption {
        self.retry.unwrap_or(RetryOption::Bool(true))
    }

    /// Removes reserved keys from [`extra`](Self::extra), returning how many were removed.
    pub(crate) fn remove_reserved_extra(&mut self) -> usize {
        let before = self.extra.len();
        self.extra.retain(|key, _| !Self::is_reserved_key(key));
        before - self.extra.len()
    }
}

/// Retry policy of a job (`retry` in the payload): a boolean or a maximum retry count.
///
/// * `Bool(true)`: retry up to the configured maximum (25 by default), then move the job to
///   the morgue.
/// * `Bool(false)`: never retry and never move to the morgue (death handlers still run).
/// * `Count(n)`: retry at most `n` times, then move to the morgue. `Count(0)` sends a failed
///   job straight to the morgue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RetryOption {
    /// `true` or `false`.
    Bool(bool),
    /// Maximum number of retries.
    Count(u32),
}

impl RetryOption {
    /// `false` only for `Bool(false)`: failures of the job go through the retry machinery.
    pub fn is_enabled(&self) -> bool {
        !matches!(self, Self::Bool(false))
    }

    /// The maximum number of retries given the process default, or `None` when retries are
    /// disabled.
    pub fn max_retries(&self, default: u32) -> Option<u32> {
        match *self {
            Self::Bool(true) => Some(default),
            Self::Bool(false) => None,
            Self::Count(count) => Some(count),
        }
    }
}

impl Default for RetryOption {
    fn default() -> Self {
        Self::Bool(true)
    }
}

impl From<bool> for RetryOption {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<u32> for RetryOption {
    fn from(value: u32) -> Self {
        Self::Count(value)
    }
}

/// Whether a failing job stores its error backtrace (`backtrace` in the payload).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BacktraceOption {
    /// `true`: store every line; `false`: store nothing.
    Bool(bool),
    /// Store at most this many lines.
    Lines(u32),
}

impl BacktraceOption {
    /// The lines to store for this option, or `None` when backtraces are not stored.
    pub fn select(&self, lines: Vec<String>) -> Option<Vec<String>> {
        match *self {
            Self::Bool(false) => None,
            Self::Bool(true) => Some(lines),
            Self::Lines(count) => Some(
                lines
                    .into_iter()
                    .take(usize::try_from(count).unwrap_or(usize::MAX))
                    .collect(),
            ),
        }
    }
}

impl From<bool> for BacktraceOption {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<u32> for BacktraceOption {
    fn from(value: u32) -> Self {
        Self::Lines(value)
    }
}

/// Generates a job id: 24 lowercase hex characters (12 random bytes).
pub fn generate_jid() -> String {
    random_hex(12)
}

/// Returns `bytes` random bytes as lowercase hex (`2 * bytes` characters). Uses the operating
/// system's random source, falling back to a non-cryptographic generator if it fails.
///
/// Not part of the stable API: hench's own identifiers ([`generate_jid`]).
#[doc(hidden)]
pub fn random_hex(bytes: usize) -> String {
    let mut buffer = vec![0_u8; bytes];
    if let Err(error) = getrandom::fill(&mut buffer) {
        static WARN: Once = Once::new();
        WARN.call_once(|| {
            tracing::warn!(%error, "OS random source failed; falling back to fastrand for ids");
        });
        fastrand::fill(&mut buffer);
    }
    let mut out = String::with_capacity(bytes * 2);
    for byte in buffer {
        out.push(hex_digit(byte >> 4));
        out.push(hex_digit(byte & 0x0f));
    }
    out
}

fn hex_digit(nibble: u8) -> char {
    char::from(if nibble < 10 {
        b'0' + nibble
    } else {
        b'a' + nibble - 10
    })
}

/// Current time in milliseconds since the Unix epoch (payload timestamps).
pub fn now_millis() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// Current time in seconds since the Unix epoch, with sub-second precision (sorted-set
/// scores).
pub fn now_secs_f64() -> f64 {
    system_time_to_secs(SystemTime::now())
}

/// Converts a [`SystemTime`] to seconds since the Unix epoch (negative before 1970).
pub fn system_time_to_secs(time: SystemTime) -> f64 {
    match time.duration_since(UNIX_EPOCH) {
        Ok(elapsed) => elapsed.as_secs_f64(),
        Err(before) => -before.duration().as_secs_f64(),
    }
}

/// Converts seconds since the Unix epoch to a [`SystemTime`]. Non-finite values map to the
/// epoch itself.
pub fn secs_to_system_time(secs: f64) -> SystemTime {
    if !secs.is_finite() {
        return UNIX_EPOCH;
    }
    let magnitude = Duration::try_from_secs_f64(secs.abs()).unwrap_or(Duration::MAX);
    if secs >= 0.0 {
        UNIX_EPOCH.checked_add(magnitude).unwrap_or(UNIX_EPOCH)
    } else {
        UNIX_EPOCH.checked_sub(magnitude).unwrap_or(UNIX_EPOCH)
    }
}

/// Converts epoch milliseconds to a [`SystemTime`].
pub fn millis_to_system_time(millis: i64) -> SystemTime {
    let magnitude = Duration::from_millis(millis.unsigned_abs());
    if millis >= 0 {
        UNIX_EPOCH.checked_add(magnitude).unwrap_or(UNIX_EPOCH)
    } else {
        UNIX_EPOCH.checked_sub(magnitude).unwrap_or(UNIX_EPOCH)
    }
}

/// Upper bound on the size of a decompressed backtrace (protects readers of `error_backtrace`
/// from pathological data).
const MAX_BACKTRACE_BYTES: u64 = 16 * 1024 * 1024;

/// Encodes backtrace lines for `error_backtrace`: base64 (standard alphabet, padded, one
/// line) of the zlib-compressed JSON array of lines.
pub fn compress_backtrace(lines: &[String]) -> String {
    let json = match serde_json::to_vec(lines) {
        Ok(json) => json,
        Err(error) => {
            tracing::error!(%error, "failed to serialize a backtrace");
            b"[]".to_vec()
        }
    };
    let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    let compressed = match encoder.write_all(&json) {
        Ok(()) => encoder.finish(),
        Err(error) => Err(error),
    };
    match compressed {
        Ok(bytes) => base64::engine::general_purpose::STANDARD.encode(bytes),
        Err(error) => {
            // Writing into a Vec cannot fail; keep the encoder honest anyway.
            tracing::error!(%error, "failed to compress a backtrace");
            String::new()
        }
    }
}

/// Decodes an `error_backtrace` value produced by [`compress_backtrace`].
pub fn decompress_backtrace(encoded: &str) -> Result<Vec<String>> {
    let compressed = base64::engine::general_purpose::STANDARD
        .decode(encoded.trim())
        .map_err(|error| Error::InvalidJob(format!("error_backtrace is not base64: {error}")))?;
    let mut json = Vec::new();
    flate2::read::ZlibDecoder::new(compressed.as_slice())
        .take(MAX_BACKTRACE_BYTES)
        .read_to_end(&mut json)
        .map_err(|error| Error::InvalidJob(format!("error_backtrace is not zlib data: {error}")))?;
    Ok(serde_json::from_slice(&json)?)
}

/// Truncates an error message to [`MAX_ERROR_MESSAGE_CHARS`] characters.
pub fn truncate_error_message(message: &str) -> String {
    match message.char_indices().nth(MAX_ERROR_MESSAGE_CHARS) {
        Some((cut, _)) => message[..cut].to_owned(),
        None => message.to_owned(),
    }
}

/// A JSON number in any representation, for lenient numeric fields.
#[derive(Deserialize)]
#[serde(untagged)]
enum AnyNumber {
    Signed(i64),
    Unsigned(u64),
    Float(f64),
}

/// Values below this many milliseconds (about year 5138 when read as seconds) written as
/// floats are taken to be seconds, the representation used by older Sidekiq versions.
const FLOAT_SECONDS_LIMIT: f64 = 1e11;

fn deserialize_opt_millis<'de, D>(deserializer: D) -> std::result::Result<Option<i64>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<AnyNumber>::deserialize(deserializer)?;
    Ok(match value {
        None => None,
        Some(AnyNumber::Signed(millis)) => Some(millis),
        Some(AnyNumber::Unsigned(millis)) => Some(i64::try_from(millis).unwrap_or(i64::MAX)),
        Some(AnyNumber::Float(value)) => {
            if !value.is_finite() {
                return Err(serde::de::Error::custom("timestamp must be finite"));
            }
            let millis = if value.abs() < FLOAT_SECONDS_LIMIT {
                (value * 1000.0).round()
            } else {
                value.trunc()
            };
            // `as` saturates for out-of-range floats.
            Some(millis as i64)
        }
    })
}

fn deserialize_opt_seconds<'de, D>(deserializer: D) -> std::result::Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<AnyNumber>::deserialize(deserializer)?;
    match value {
        None => Ok(None),
        Some(AnyNumber::Unsigned(secs)) => Ok(Some(secs)),
        Some(AnyNumber::Signed(secs)) => u64::try_from(secs)
            .map(Some)
            .map_err(|_| serde::de::Error::custom("retry_for must not be negative")),
        Some(AnyNumber::Float(secs)) => {
            if secs.is_finite() && secs >= 0.0 {
                // `as` saturates for out-of-range floats.
                Ok(Some(secs.trunc() as u64))
            } else {
                Err(serde::de::Error::custom(
                    "retry_for must be a finite, non-negative number",
                ))
            }
        }
    }
}

/// Deserializes an optional feature field leniently: `null` is `None`, and so is a value that
/// does not have the field's shape (logged at `warn`), so that no feature field can make a job
/// unparseable (which would send it to the morgue).
fn deserialize_lenient<'de, D, T>(deserializer: D) -> std::result::Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: serde::de::DeserializeOwned,
{
    match Value::deserialize(deserializer)? {
        Value::Null => Ok(None),
        value => match serde_json::from_value(value) {
            Ok(field) => Ok(Some(field)),
            Err(error) => {
                tracing::warn!(
                    %error,
                    field = std::any::type_name::<T>(),
                    "ignoring a malformed feature field of a job"
                );
                Ok(None)
            }
        },
    }
}

fn deserialize_null_default<'de, D, T>(deserializer: D) -> std::result::Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Default + Deserialize<'de>,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn minimal_payload_round_trips() {
        let payload = JobPayload::new("SendEmail", vec![json!(1), json!("two")]);
        let json = payload.to_json().unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&json).unwrap(),
            json!({"class": "SendEmail", "args": [1, "two"], "jid": "", "queue": ""})
        );
        assert_eq!(JobPayload::from_json(&json).unwrap(), payload);
    }

    #[test]
    fn full_payload_round_trips_and_keeps_extra_keys() {
        let value = json!({
            "class": "HardJob",
            "args": [1, {"a": [true, null]}],
            "jid": "0123456789abcdef01234567",
            "queue": "critical",
            "retry": 5,
            "backtrace": true,
            "dead": false,
            "tags": ["alpha", "beta"],
            "retry_for": 3600,
            "retry_queue": "low",
            "created_at": 1_700_000_000_123_i64,
            "enqueued_at": 1_700_000_000_456_i64,
            "retry_count": 2,
            "failed_at": 1_700_000_001_000_i64,
            "retried_at": 1_700_000_002_000_i64,
            "error_message": "boom",
            "error_class": "std::io::error::Error",
            "error_backtrace": "eJyLjgUAARUAuQ==",
            "bid": "batch",
            "locale": "fr",
            "custom": {"nested": [1, 2]}
        });
        let payload: JobPayload = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(payload.retry, Some(RetryOption::Count(5)));
        assert_eq!(payload.backtrace, Some(BacktraceOption::Bool(true)));
        assert_eq!(payload.dead, Some(false));
        assert_eq!(payload.tags, vec!["alpha", "beta"]);
        assert_eq!(payload.retry_for, Some(3600));
        assert_eq!(payload.retry_count, Some(2));
        assert_eq!(payload.extra.len(), 2);
        assert_eq!(payload.extra["locale"], json!("fr"));
        assert_eq!(payload.to_value().unwrap(), value);
    }

    #[test]
    fn missing_optional_fields_default() {
        let payload = JobPayload::from_json(r#"{"class":"A"}"#).unwrap();
        assert_eq!(payload.class, "A");
        assert!(payload.args.is_empty());
        assert!(payload.jid.is_empty());
        assert!(payload.queue.is_empty());
        assert_eq!(payload.retry, None);
        assert_eq!(payload.retry_option(), RetryOption::Bool(true));
        let payload = JobPayload::from_json(r#"{"class":"A","tags":null}"#).unwrap();
        assert!(payload.tags.is_empty());
        let payload = JobPayload::from_json(
            r#"{"class":"A","args":null,"jid":null,"queue":null,"retry":null,"at":null}"#,
        )
        .unwrap();
        assert_eq!(payload, JobPayload::new("A", vec![]));
    }

    #[test]
    fn invalid_shapes_are_rejected() {
        for bad in [
            r#"{"args":[]}"#,
            r#"{"class":42,"args":[]}"#,
            r#"{"class":"A","args":"nope"}"#,
            r#"{"class":"A","args":[],"at":"soon"}"#,
            r#"{"class":"A","args":[],"tags":"a"}"#,
            r#"{"class":"A","args":[],"retry":"yes"}"#,
            r#"{"class":"A","args":[],"retry_for":-5}"#,
            r#"[1,2]"#,
            r#"not json"#,
        ] {
            assert!(
                matches!(JobPayload::from_json(bad), Err(Error::Json(_))),
                "{bad} should not parse"
            );
        }
    }

    #[test]
    fn lenient_numbers() {
        let payload = JobPayload::from_json(
            r#"{"class":"A","created_at":1700000000.5,"enqueued_at":1700000000123.0,"retry_for":100.9,"at":1700000000}"#,
        )
        .unwrap();
        assert_eq!(payload.created_at, Some(1_700_000_000_500));
        assert_eq!(payload.enqueued_at, Some(1_700_000_000_123));
        assert_eq!(payload.retry_for, Some(100));
        assert_eq!(payload.at, Some(1_700_000_000.0));
    }

    #[test]
    fn retry_option_semantics() {
        assert_eq!(
            serde_json::from_str::<RetryOption>("true").unwrap(),
            RetryOption::Bool(true)
        );
        assert_eq!(
            serde_json::from_str::<RetryOption>("0").unwrap(),
            RetryOption::Count(0)
        );
        assert_eq!(serde_json::to_string(&RetryOption::Count(3)).unwrap(), "3");
        assert_eq!(
            serde_json::to_string(&RetryOption::Bool(false)).unwrap(),
            "false"
        );
        assert!(RetryOption::Count(0).is_enabled());
        assert!(!RetryOption::Bool(false).is_enabled());
        assert_eq!(RetryOption::Bool(true).max_retries(25), Some(25));
        assert_eq!(RetryOption::Bool(false).max_retries(25), None);
        assert_eq!(RetryOption::Count(7).max_retries(25), Some(7));
        assert_eq!(RetryOption::from(4_u32), RetryOption::Count(4));
        assert_eq!(RetryOption::from(false), RetryOption::Bool(false));
        assert_eq!(RetryOption::default(), RetryOption::Bool(true));
    }

    #[test]
    fn backtrace_option_selects_lines() {
        let lines = || vec!["a".to_owned(), "b".to_owned(), "c".to_owned()];
        assert_eq!(BacktraceOption::Bool(false).select(lines()), None);
        assert_eq!(BacktraceOption::Bool(true).select(lines()), Some(lines()));
        assert_eq!(
            BacktraceOption::Lines(2).select(lines()),
            Some(vec!["a".to_owned(), "b".to_owned()])
        );
        assert_eq!(BacktraceOption::Lines(0).select(lines()), Some(vec![]));
        assert_eq!(
            serde_json::from_str::<BacktraceOption>("10").unwrap(),
            BacktraceOption::Lines(10)
        );
        assert_eq!(BacktraceOption::from(true), BacktraceOption::Bool(true));
        assert_eq!(BacktraceOption::from(3_u32), BacktraceOption::Lines(3));
    }

    #[test]
    fn jids_are_24_lowercase_hex_and_unique() {
        let mut seen = std::collections::HashSet::new();
        for _ in 0..1000 {
            let jid = generate_jid();
            assert_eq!(jid.len(), 24);
            assert!(
                jid.chars()
                    .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
            );
            assert!(seen.insert(jid));
        }
        assert_eq!(random_hex(6).len(), 12);
        assert_eq!(random_hex(0), "");
    }

    #[test]
    fn time_helpers() {
        let millis = now_millis();
        let secs = now_secs_f64();
        assert!((secs * 1000.0 - millis as f64).abs() < 5_000.0);
        assert!(millis > 1_600_000_000_000);
        let time = secs_to_system_time(1_700_000_000.25);
        assert!((system_time_to_secs(time) - 1_700_000_000.25).abs() < 1e-6);
        assert_eq!(secs_to_system_time(f64::NAN), UNIX_EPOCH);
        assert!((system_time_to_secs(secs_to_system_time(-5.0)) + 5.0).abs() < 1e-9);
        assert_eq!(
            millis_to_system_time(1_500),
            UNIX_EPOCH + Duration::from_millis(1_500)
        );
        assert_eq!(
            millis_to_system_time(-1_500),
            UNIX_EPOCH - Duration::from_millis(1_500)
        );
    }

    #[test]
    fn backtrace_codec_round_trips() {
        let lines = vec![
            "app::job::perform at src/job.rs:10:5".to_owned(),
            "caused by: ünïcödé".to_owned(),
            String::new(),
        ];
        let encoded = compress_backtrace(&lines);
        assert!(!encoded.contains('\n'));
        assert_eq!(decompress_backtrace(&encoded).unwrap(), lines);
        assert_eq!(
            decompress_backtrace(&compress_backtrace(&[])).unwrap(),
            Vec::<String>::new()
        );
    }

    #[test]
    fn backtrace_codec_matches_the_reference_format() {
        // zlib("[\"a\"]") base64-encoded, as produced by other Sidekiq-compatible tools.
        let encoded = base64::engine::general_purpose::STANDARD.encode({
            let mut e = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::best());
            e.write_all(br#"["a"]"#).unwrap();
            e.finish().unwrap()
        });
        assert_eq!(
            decompress_backtrace(&encoded).unwrap(),
            vec!["a".to_owned()]
        );
    }

    #[test]
    fn backtrace_decoding_errors() {
        assert!(matches!(
            decompress_backtrace("%%%"),
            Err(Error::InvalidJob(_))
        ));
        let not_zlib = base64::engine::general_purpose::STANDARD.encode(b"plain");
        assert!(matches!(
            decompress_backtrace(&not_zlib),
            Err(Error::InvalidJob(_))
        ));
        let not_lines = base64::engine::general_purpose::STANDARD.encode({
            let mut e = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
            e.write_all(b"[1]").unwrap();
            e.finish().unwrap()
        });
        assert!(matches!(
            decompress_backtrace(&not_lines),
            Err(Error::Json(_))
        ));
    }

    #[test]
    fn error_messages_are_truncated_on_char_boundaries() {
        let long = "é".repeat(MAX_ERROR_MESSAGE_CHARS + 5);
        let truncated = truncate_error_message(&long);
        assert_eq!(truncated.chars().count(), MAX_ERROR_MESSAGE_CHARS);
        assert_eq!(truncate_error_message("short"), "short");
    }

    #[test]
    fn feature_fields_round_trip() {
        let json = json!({
            "class": "A",
            "args": [],
            "jid": "j",
            "queue": "default",
            "bid": "0123456789abcdef01234567",
            "batch_callback": {"bid": "0123456789abcdef01234567", "event": "death"},
            "unique": {"for": 600, "until": "start", "digest": "rP3m"},
            "periodic": {"name": "nightly", "tick": 1_730_426_400_000_i64},
            "throttle": {"name": "stripe", "key": "42"},
            "limited": {"n": 3, "by": "w:stripe:42", "at": 1_730_426_400_123_i64},
            "locale": "fr"
        });
        let payload: JobPayload = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(
            payload.batch_callback.as_ref().map(BatchCallback::event),
            Some(crate::batch::BatchEvent::Death)
        );
        let unique = payload
            .unique
            .as_ref()
            .and_then(UniqueOption::settings)
            .unwrap();
        assert_eq!(unique.ttl, Some(Duration::from_secs(600)));
        assert_eq!(unique.until, Some(crate::unique::UniqueUntil::Start));
        assert_eq!(unique.digest.as_deref(), Some("rP3m"));
        assert_eq!(
            payload.periodic.as_ref().map(|meta| meta.tick),
            Some(1_730_426_400_000)
        );
        assert_eq!(
            payload.throttle.as_ref().and_then(ThrottleOption::key),
            Some("42")
        );
        assert_eq!(payload.limited.as_ref().map(|limited| limited.n), Some(3));
        assert_eq!(payload.extra.len(), 1);
        assert_eq!(serde_json::to_value(&payload).unwrap(), json);
        // `false` disables uniqueness and throttling.
        let off: JobPayload =
            serde_json::from_value(json!({"class": "A", "unique": false, "throttle": false}))
                .unwrap();
        assert_eq!(off.unique, Some(UniqueOption::Disabled));
        assert_eq!(off.throttle, Some(ThrottleOption::Disabled));
        assert_eq!(
            serde_json::to_value(&off).unwrap(),
            json!({"class": "A", "args": [], "jid": "", "queue": "", "unique": false, "throttle": false})
        );
    }

    #[test]
    fn malformed_feature_fields_are_ignored() {
        let payload: JobPayload = serde_json::from_value(json!({
            "class": "A",
            "batch_callback": {"bid": 1},
            "unique": "yes",
            "periodic": {"name": 3},
            "throttle": [1],
            "limited": {"n": -1},
        }))
        .unwrap();
        assert_eq!(payload.batch_callback, None);
        assert_eq!(payload.unique, None);
        assert_eq!(payload.periodic, None);
        assert_eq!(payload.throttle, None);
        assert_eq!(payload.limited, None);
        assert!(payload.extra.is_empty());
        // An unknown `until` written by a newer version reads as `success`.
        let newer: JobPayload = serde_json::from_value(json!({
            "class": "A",
            "unique": {"for": 1.9, "until": "later", "digest": "d"}
        }))
        .unwrap();
        let unique = newer
            .unique
            .as_ref()
            .and_then(UniqueOption::settings)
            .unwrap();
        assert_eq!(unique.until, Some(crate::unique::UniqueUntil::Success));
        assert_eq!(unique.ttl, Some(Duration::from_secs(1)));
    }

    #[test]
    fn feature_keys_are_reserved() {
        for key in JobPayload::FEATURE_KEYS {
            assert!(JobPayload::is_reserved_key(key), "{key}");
            assert!(!JobPayload::RESERVED_KEYS.contains(key), "{key}");
        }
        let mut payload = JobPayload::new("A", vec![]);
        payload.extra.insert("unique".into(), json!({"for": 1}));
        payload.extra.insert("limited".into(), json!(1));
        assert_eq!(payload.remove_reserved_extra(), 2);
        // Every feature field is listed.
        let value = serde_json::to_value(JobPayload {
            batch_callback: Some(BatchCallback::new(
                "0123456789abcdef01234567",
                crate::batch::BatchEvent::Complete,
            )),
            unique: Some(UniqueOption::Disabled),
            periodic: Some(PeriodicMeta::new("p", 1, false)),
            throttle: Some(ThrottleOption::Disabled),
            limited: Some(Limited::new(1, "x", None)),
            ..JobPayload::new("A", vec![])
        })
        .unwrap();
        let mut keys: Vec<&str> = value
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .filter(|key| !["class", "args", "jid", "queue"].contains(key))
            .collect();
        let mut features = JobPayload::FEATURE_KEYS.to_vec();
        keys.sort_unstable();
        features.sort_unstable();
        assert_eq!(keys, features);
    }

    #[test]
    fn reserved_keys_are_removed_from_extra() {
        let mut payload = JobPayload::new("A", vec![]);
        payload.extra.insert("queue".into(), json!("sneaky"));
        payload.extra.insert("locale".into(), json!("fr"));
        assert_eq!(payload.remove_reserved_extra(), 1);
        assert_eq!(payload.extra.len(), 1);
        assert!(JobPayload::is_reserved_key("jid"));
        assert!(!JobPayload::is_reserved_key("locale"));
        // Every named field is listed.
        let value = serde_json::to_value(JobPayload {
            retry: Some(RetryOption::Bool(true)),
            backtrace: Some(BacktraceOption::Bool(true)),
            dead: Some(true),
            tags: vec!["t".into()],
            retry_for: Some(1),
            retry_queue: Some("q".into()),
            created_at: Some(1),
            enqueued_at: Some(1),
            at: Some(1.0),
            retry_count: Some(1),
            failed_at: Some(1),
            retried_at: Some(1),
            discarded_at: Some(1),
            error_message: Some("m".into()),
            error_class: Some("c".into()),
            error_backtrace: Some("b".into()),
            bid: Some("b".into()),
            ..JobPayload::new("A", vec![])
        })
        .unwrap();
        let mut keys: Vec<&str> = value
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        let mut reserved = JobPayload::RESERVED_KEYS.to_vec();
        keys.sort_unstable();
        reserved.sort_unstable();
        assert_eq!(keys, reserved);
    }
}
