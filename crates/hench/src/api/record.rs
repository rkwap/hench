//! [`JobRecord`]: a job as stored in Redis, for display and management.

use std::borrow::Cow;
use std::fmt;
use std::time::SystemTime;

use serde_json::{Map, Value};

use crate::api::{Api, latency_of, timestamp};
use crate::error::{Error, Result};
use crate::keys;
use crate::payload::{JobPayload, decompress_backtrace};

/// What [`JobRecord::display_args`] shows instead of the last argument of a job marked
/// `"encrypt": true` (an encryption middleware's convention: the ciphertext is noise).
const ENCRYPTED_PLACEHOLDER: &str = "[encrypted]";

/// A job read from Redis: the raw JSON exactly as stored (needed to delete it) and its
/// parsed fields (Sidekiq's `JobRecord`).
///
/// Records are tolerant of malformed data, since a dashboard must be able to show and delete
/// anything it finds: if the JSON does not parse, [`item`](Self::item) is empty, the raw
/// value is shown as the only argument and [`parse_error`](Self::parse_error) says why. The
/// accessors return `None` for missing or mistyped fields instead of failing.
#[derive(Clone)]
pub struct JobRecord {
    api: Api,
    value: String,
    item: Map<String, Value>,
    /// `Some` when the value is not a JSON object.
    parse_error: Option<String>,
    /// The arguments shown for an unparseable value: the value itself.
    raw_args: Vec<Value>,
    queue: String,
}

impl JobRecord {
    /// A record for the raw job `value`. `queue` is the queue it was read from; `None` takes
    /// it from the payload.
    pub fn new(api: impl Into<Api>, value: impl Into<String>, queue: Option<&str>) -> Self {
        let value = value.into();
        let (item, parse_error, raw_args) = match serde_json::from_str::<Map<String, Value>>(&value)
        {
            Ok(item) => (item, None, Vec::new()),
            Err(error) => (
                Map::new(),
                Some(error.to_string()),
                vec![Value::String(value.clone())],
            ),
        };
        let queue = match queue {
            Some(queue) => queue.to_owned(),
            None => item
                .get("queue")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
        };
        Self {
            api: api.into(),
            value,
            item,
            parse_error,
            raw_args,
            queue,
        }
    }

    /// The job exactly as stored in Redis.
    pub fn value(&self) -> &str {
        &self.value
    }

    /// The parsed job (empty if the value is not a JSON object).
    pub fn item(&self) -> &Map<String, Value> {
        &self.item
    }

    /// The field `key` of the job (Sidekiq's `record["key"]`).
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.item.get(key)
    }

    /// `true` if the value is a JSON object.
    pub fn is_valid(&self) -> bool {
        self.parse_error.is_none()
    }

    /// Why the value could not be parsed, if it could not.
    pub fn parse_error(&self) -> Option<&str> {
        self.parse_error.as_deref()
    }

    /// The job as a typed payload. Fails for malformed jobs (see [`JobPayload`]'s rules).
    pub fn payload(&self) -> Result<JobPayload> {
        Ok(serde_json::from_value(Value::Object(self.item.clone()))?)
    }

    /// The queue the job was read from (or names, for jobs outside queues).
    pub fn queue(&self) -> &str {
        &self.queue
    }

    fn string(&self, key: &str) -> Option<&str> {
        self.item.get(key).and_then(Value::as_str)
    }

    /// The job's class (the registered name of its job type).
    pub fn class(&self) -> Option<&str> {
        self.string("class")
    }

    /// The name to show for the job: its `display_class` field if it has one (set by
    /// wrappers that run other jobs), else its class.
    pub fn display_class(&self) -> Option<&str> {
        self.string("display_class").or_else(|| self.class())
    }

    /// The job's arguments. For a value that is not valid JSON, the value itself.
    pub fn args(&self) -> &[Value] {
        match self.item.get("args") {
            Some(Value::Array(args)) => args,
            _ => &self.raw_args,
        }
    }

    /// The arguments to show: [`args`](Self::args), except that the last argument of a job
    /// marked `"encrypt": true` is replaced by a placeholder.
    pub fn display_args(&self) -> Cow<'_, [Value]> {
        let args = self.args();
        let encrypted = self.item.get("encrypt").is_some_and(|value| match value {
            Value::Bool(encrypted) => *encrypted,
            Value::Null => false,
            _ => true,
        });
        if encrypted && !args.is_empty() {
            let mut shown = args.to_vec();
            if let Some(last) = shown.last_mut() {
                *last = Value::String(ENCRYPTED_PLACEHOLDER.to_owned());
            }
            Cow::Owned(shown)
        } else {
            Cow::Borrowed(args)
        }
    }

    /// The job id.
    pub fn jid(&self) -> Option<&str> {
        self.string("jid")
    }

    /// The batch id.
    pub fn bid(&self) -> Option<&str> {
        self.string("bid")
    }

    /// The job's tags (non-string entries are skipped).
    pub fn tags(&self) -> Vec<&str> {
        match self.item.get("tags") {
            Some(Value::Array(tags)) => tags.iter().filter_map(Value::as_str).collect(),
            _ => Vec::new(),
        }
    }

    /// The message of the last error.
    pub fn error_message(&self) -> Option<&str> {
        self.string("error_message")
    }

    /// The class of the last error.
    pub fn error_class(&self) -> Option<&str> {
        self.string("error_class")
    }

    /// The number of retries so far (`0` after the first failure).
    ///
    /// A `u64`, where [`JobPayload::retry_count`](crate::JobPayload::retry_count) and
    /// [`JobContext::retry_count`](crate::JobContext::retry_count) are `u32`: a record reads
    /// whatever JSON is stored (possibly written by another tool), so any non-negative integer
    /// is shown rather than dropped; [`JobRecord::payload`] gives the typed form.
    pub fn retry_count(&self) -> Option<u64> {
        self.item.get("retry_count").and_then(Value::as_u64)
    }

    /// The backtrace of the last error, decoded from `error_backtrace`: `None` if the job has
    /// none. Old-style uncompressed backtraces (a JSON array of lines) are accepted too.
    pub fn error_backtrace(&self) -> Result<Option<Vec<String>>> {
        match self.item.get("error_backtrace") {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(encoded)) => decompress_backtrace(encoded).map(Some),
            Some(Value::Array(lines)) => Ok(Some(
                lines
                    .iter()
                    .map(|line| match line {
                        Value::String(line) => line.clone(),
                        other => other.to_string(),
                    })
                    .collect(),
            )),
            Some(_) => Err(Error::InvalidJob(
                "error_backtrace must be a compressed string or an array of lines".to_owned(),
            )),
        }
    }

    /// When the job was first pushed (`created_at`, else `enqueued_at`).
    pub fn created_at(&self) -> Option<SystemTime> {
        timestamp(self.item.get("created_at")).or_else(|| self.enqueued_at())
    }

    /// When the job was last pushed to a queue. `None` for scheduled jobs.
    pub fn enqueued_at(&self) -> Option<SystemTime> {
        timestamp(self.item.get("enqueued_at"))
    }

    /// When the job first failed.
    pub fn failed_at(&self) -> Option<SystemTime> {
        timestamp(self.item.get("failed_at"))
    }

    /// When the job was last retried.
    pub fn retried_at(&self) -> Option<SystemTime> {
        timestamp(self.item.get("retried_at"))
    }

    /// When the job was discarded.
    pub fn discarded_at(&self) -> Option<SystemTime> {
        timestamp(self.item.get("discarded_at"))
    }

    /// Seconds since the job was enqueued (or created): how long it has been waiting.
    pub fn latency(&self) -> f64 {
        latency_of(&self.item)
    }

    /// Removes this job from its queue. Returns `false` if it was no longer there (already
    /// processed or deleted).
    ///
    /// The `LREM` is sent once (repeated after a dropped connection it would answer 0 for a
    /// job it had removed); if it fails, the job's presence decides between `true` (gone) and
    /// the error (still there, or unknown).
    pub async fn delete(&self) -> Result<bool> {
        let featured = super::featured_job(&self.value);
        let reports = super::before_removal(
            &self.api,
            featured.as_slice(),
            crate::batch::api::Removal::Deleted,
        )
        .await;
        let deleted = self.remove(featured.as_ref()).await;
        super::after_removal(&self.api, reports).await;
        deleted
    }

    /// Removes this job from its queue (see [`JobRecord::delete`]); a unique job (`featured`)
    /// releases its lock in the same script.
    async fn remove(&self, featured: Option<&JobPayload>) -> Result<bool> {
        let key = keys::queue(&self.queue);
        if let Some(job) = featured
            && let Some(removed) =
                crate::unique::api::delete_from_queue(self.api.redis(), &key, &self.value, job)
                    .await
        {
            return removed;
        }
        let (key, value) = (&key, &self.value);
        let attempt: Result<u64> = async {
            let mut conn = self.api.redis().conn().await?;
            Ok(::redis::cmd("LREM")
                .arg(key)
                .arg(1)
                .arg(value)
                .query_async(&mut conn)
                .await?)
        }
        .await;
        let error = match attempt {
            Ok(removed) => return Ok(removed > 0),
            Err(error) => error,
        };
        let position: Result<Option<i64>> = self
            .api
            .redis()
            .with_retry(|mut conn| async move {
                ::redis::cmd("LPOS")
                    .arg(key)
                    .arg(value)
                    .query_async(&mut conn)
                    .await
            })
            .await;
        match position {
            Ok(None) => Ok(true),
            Ok(Some(_)) | Err(_) => Err(error),
        }
    }
}

impl fmt::Debug for JobRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JobRecord")
            .field("queue", &self.queue)
            .field("value", &self.value)
            .finish_non_exhaustive()
    }
}

impl PartialEq for JobRecord {
    /// Records are equal when they hold the same raw job read from the same queue.
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value && self.queue == other.queue
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::payload::{compress_backtrace, now_millis};
    use crate::redis::RedisPool;
    use serde_json::json;

    fn record(value: Value) -> JobRecord {
        JobRecord::new(RedisPool::detached(), value.to_string(), Some("default"))
    }

    #[test]
    fn malformed_payloads_are_rendered_safely() {
        let invalid = JobRecord::new(RedisPool::detached(), "not-json", Some("default"));
        assert!(invalid.item().is_empty());
        assert_eq!(invalid.args(), [json!("not-json")]);
        assert_eq!(invalid.class(), None);
        assert_eq!(invalid.display_class(), None);
        assert!(!invalid.is_valid());
        assert!(invalid.parse_error().is_some());
        assert!(invalid.payload().is_err());
        assert_eq!(invalid.latency(), 0.0);
        assert_eq!(invalid.queue(), "default");

        let not_an_object = JobRecord::new(RedisPool::detached(), "[1,2]", None);
        assert!(!not_an_object.is_valid());
        assert_eq!(not_an_object.queue(), "");

        let odd = record(json!({"class": 5, "args": "x", "tags": "t", "retry_count": -1}));
        assert!(odd.is_valid());
        assert_eq!(odd.class(), None);
        assert!(odd.args().is_empty());
        assert!(odd.tags().is_empty());
        assert_eq!(odd.retry_count(), None);
    }

    #[test]
    fn parses_json_payloads() {
        let now = now_millis();
        let job = record(json!({
            "class": "X", "args": [1, 2], "queue": "default", "jid": "abc", "bid": "b",
            "tags": ["t1", 7, "t2"], "error_message": "boom", "error_class": "E",
            "retry_count": 3, "created_at": now - 10_000, "enqueued_at": now - 1_000,
            "failed_at": now, "retried_at": now, "discarded_at": now
        }));
        assert_eq!(job.class(), Some("X"));
        assert_eq!(job.display_class(), Some("X"));
        assert_eq!(job.args(), [json!(1), json!(2)]);
        assert_eq!(job.jid(), Some("abc"));
        assert_eq!(job.bid(), Some("b"));
        assert_eq!(job.tags(), ["t1", "t2"]);
        assert_eq!(job.error_message(), Some("boom"));
        assert_eq!(job.error_class(), Some("E"));
        assert_eq!(job.retry_count(), Some(3));
        assert!(job.created_at() < job.enqueued_at());
        assert!(job.failed_at().is_some() && job.retried_at().is_some());
        assert!(job.discarded_at().is_some());
        assert!((job.latency() - 1.0).abs() < 0.5);
        assert_eq!(job.get("queue"), Some(&json!("default")));
        // The typed payload is strict where the record is lenient (the tag `7`).
        assert!(job.payload().is_err());
        let clean = record(json!({"class": "X", "jid": "abc", "tags": ["t"]}));
        assert_eq!(clean.payload().unwrap().jid, "abc");
        assert!(format!("{job:?}").contains("abc"));
        assert_eq!(job, job.clone());

        let wrapped = record(json!({"class": "Wrapper", "display_class": "Real", "args": []}));
        assert_eq!(wrapped.display_class(), Some("Real"));

        // `created_at` falls back to `enqueued_at`; scheduled jobs have no `enqueued_at`.
        let enqueued_only = record(json!({"class": "X", "enqueued_at": now}));
        assert_eq!(enqueued_only.created_at(), enqueued_only.enqueued_at());
        assert!(record(json!({"class": "X"})).enqueued_at().is_none());
    }

    #[test]
    fn encrypted_jobs_hide_their_last_argument() {
        let job = record(json!({"class": "X", "args": ["a", "b", "secret"], "encrypt": true}));
        assert_eq!(
            job.display_args().as_ref(),
            [json!("a"), json!("b"), json!(ENCRYPTED_PLACEHOLDER)]
        );
        assert_eq!(job.args()[2], json!("secret"));
        let plain = record(json!({"class": "X", "args": ["a"], "encrypt": false}));
        assert!(matches!(plain.display_args(), Cow::Borrowed(_)));
        let empty = record(json!({"class": "X", "args": [], "encrypt": true}));
        assert!(empty.display_args().is_empty());
    }

    #[test]
    fn error_backtraces_are_decoded() {
        let lines = vec!["a.rs:1".to_owned(), "b.rs:2".to_owned()];
        let job = record(json!({"class": "X", "error_backtrace": compress_backtrace(&lines)}));
        assert_eq!(job.error_backtrace().unwrap(), Some(lines.clone()));
        let none = record(json!({"class": "X"}));
        assert_eq!(none.error_backtrace().unwrap(), None);
        let old_style = record(json!({"class": "X", "error_backtrace": ["line1", 2]}));
        assert_eq!(
            old_style.error_backtrace().unwrap(),
            Some(vec!["line1".to_owned(), "2".to_owned()])
        );
        let garbage = record(json!({"class": "X", "error_backtrace": "%%%"}));
        assert!(garbage.error_backtrace().is_err());
        let wrong_type = record(json!({"class": "X", "error_backtrace": 5}));
        assert!(wrong_type.error_backtrace().is_err());
    }
}
