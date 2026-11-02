//! Definitions: what a schedule stores in its hash (`spec`, `timing`, `policy`, `job`,
//! `description`, `digest`; `docs/design/periodic.md` §2.4), how it is built from a
//! [`Periodic`](crate::periodic::Periodic), and how a poller reads it back.
//!
//! The digest is the SHA-256 (base64url) of the canonical JSON `{"v", "spec", "policy", "job",
//! "description"}` (object members sorted at every level, whatever `serde_json`'s
//! `preserve_order` says), so that two builds of one crontab always agree and an identical
//! re-registration changes nothing.

use std::time::Duration;

use serde_json::{Map, Value, json};

use crate::canonical::{canonical_json, digest as canonical_digest, sha256_b64url};
use crate::payload::JobPayload;
use crate::periodic::plan::{MAX_CATCH_UP, Policy};
use crate::periodic::timing::{Timing, TimingError};
use crate::periodic::{CatchUp, Timetable};
use crate::unique::{OnConflict, UniqueOption};

/// The version of the stored definition format (`spec.v`) this version writes and reads.
pub(crate) const SPEC_VERSION: u64 = 1;

/// Longest schedule name, in bytes.
pub(crate) const MAX_NAME_BYTES: usize = 200;

/// Longest description, in bytes.
pub(crate) const MAX_DESCRIPTION_BYTES: usize = 2_000;

/// Most schedules in one crontab (one registration script writes them all).
pub(crate) const MAX_CRONTAB: usize = 10_000;

/// Longest deadline: about 100 years.
const MAX_DEADLINE_SECS: u64 = 100 * 366 * 24 * 3_600;

/// A schedule ready to be stored: the hash fields `SYNC` and `UPSERT` write.
#[derive(Debug, Clone)]
pub(crate) struct Definition {
    /// The schedule's name.
    pub(crate) name: String,
    /// The timetable JSON (`{"v":1,"cron":..,"tz":..}` or `{"v":1,"every":..,"offset":..}`).
    pub(crate) spec: String,
    /// The canonical timetable key: a change of it reschedules the next tick.
    pub(crate) timing_key: String,
    /// The catch-up policy JSON.
    pub(crate) policy: String,
    /// The job template JSON.
    pub(crate) job: String,
    /// Free text.
    pub(crate) description: Option<String>,
    /// The definition's digest.
    pub(crate) digest: String,
    /// Created paused.
    pub(crate) paused: bool,
    /// The parsed timetable, to compute the next ticks.
    pub(crate) timing: Timing,
}

/// A stored definition, parsed by a poller (or the API) to fire, list or resume it.
#[derive(Debug, Clone)]
pub(crate) struct Compiled {
    /// The digest it was read with (`FIRE` checks it).
    pub(crate) digest: String,
    /// The parsed timetable.
    pub(crate) timing: Timing,
    /// The catch-up policy.
    pub(crate) policy: Policy,
    /// The job template (no jid, no `created_at`).
    pub(crate) template: JobPayload,
}

/// Why a stored definition cannot be used by this process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Unreadable {
    /// Written by a newer version (`spec.v` above [`SPEC_VERSION`], or a policy this version
    /// does not know): other processes may handle it.
    Newer(String),
    /// In a time zone this process's time-zone database lacks (older tzdata than the writer's):
    /// processes with a newer database may handle it.
    UnknownZone(String),
    /// Malformed, or invalid for any version.
    Invalid(String),
}

impl Unreadable {
    /// The reason, for logs and the schedule's `error` field.
    pub(crate) fn reason(&self) -> &str {
        match self {
            Unreadable::Newer(reason)
            | Unreadable::UnknownZone(reason)
            | Unreadable::Invalid(reason) => reason,
        }
    }
}

/// Whether `name` is a valid schedule name: 1 to 200 bytes, no control characters, no leading
/// or trailing whitespace, and not `.` or `..`: the dashboard and the JSON API name a schedule
/// in URL paths, where those are dot segments that browsers and HTTP clients resolve away.
pub(crate) fn check_name(name: &str) -> Result<(), String> {
    if matches!(name, "." | "..") {
        return Err(format!(
            "invalid name {name:?}: `.` and `..` are dot segments in URL paths, where the \
             dashboard and the JSON API name schedules"
        ));
    }
    if crate::config::valid_name(name, MAX_NAME_BYTES) {
        Ok(())
    } else {
        Err(format!(
            "invalid name {:?}: 1 to {MAX_NAME_BYTES} bytes, no control characters, no leading \
             or trailing whitespace",
            name
        ))
    }
}

/// Whole seconds of `duration`, or an error naming `what` when it has a fractional part.
fn whole_seconds(duration: Duration, what: &str) -> Result<u64, String> {
    if duration.subsec_nanos() != 0 {
        return Err(format!(
            "{what} must be whole seconds, not {}",
            humantime_like(duration)
        ));
    }
    Ok(duration.as_secs())
}

fn humantime_like(duration: Duration) -> String {
    format!("{:.3} s", duration.as_secs_f64())
}

impl Timetable {
    /// Parses the timetable (expression, zone, interval, offset).
    pub(crate) fn timing(&self) -> Result<Timing, TimingError> {
        match self {
            Timetable::Cron { expression, tz } => Timing::cron(expression, tz),
            Timetable::Every { interval, offset } => {
                let interval = whole_seconds(*interval, "the interval").map_err(TimingError)?;
                let offset = whole_seconds(*offset, "the offset").map_err(TimingError)?;
                Timing::every(interval, offset)
            }
        }
    }

    /// The timetable JSON stored as `spec`.
    pub(crate) fn spec_value(&self) -> Value {
        match self {
            Timetable::Cron { expression, tz } => json!({
                "v": SPEC_VERSION,
                "cron": crate::periodic::timing::normalize_expression(expression),
                "tz": tz,
            }),
            Timetable::Every { interval, offset } => json!({
                "v": SPEC_VERSION,
                "every": interval.as_secs(),
                "offset": offset.as_secs(),
            }),
        }
    }

    /// The canonical timetable key stored as `timing`: `cron|0 2 * * *|Europe/Paris`,
    /// `every|30|0`.
    pub(crate) fn timing_key(&self) -> String {
        match self {
            Timetable::Cron { expression, tz } => format!(
                "cron|{}|{tz}",
                crate::periodic::timing::normalize_expression(expression)
            ),
            Timetable::Every { interval, offset } => {
                format!("every|{}|{}", interval.as_secs(), offset.as_secs())
            }
        }
    }
}

/// The timetable stored as `spec`. Unknown versions and unknown fields of version 1 are
/// [`Unreadable::Newer`]: a newer process wrote them.
pub(crate) fn parse_spec(spec: &str) -> Result<Timetable, Unreadable> {
    let value: Value = serde_json::from_str(spec)
        .map_err(|error| Unreadable::Invalid(format!("the timetable is not JSON: {error}")))?;
    let Value::Object(fields) = value else {
        return Err(Unreadable::Invalid(
            "the timetable is not a JSON object".to_owned(),
        ));
    };
    let version = fields.get("v").and_then(Value::as_u64).unwrap_or(0);
    if version == 0 {
        return Err(Unreadable::Invalid(
            "the timetable has no version (`v`)".to_owned(),
        ));
    }
    if version > SPEC_VERSION {
        return Err(Unreadable::Newer(format!(
            "the timetable is in format version {version}; this process reads version \
             {SPEC_VERSION} (upgrade it)"
        )));
    }
    for key in fields.keys() {
        if !matches!(key.as_str(), "v" | "cron" | "tz" | "every" | "offset") {
            return Err(Unreadable::Newer(format!(
                "the timetable has a field this version does not know (`{key}`)"
            )));
        }
    }
    match (fields.get("cron"), fields.get("every")) {
        (Some(Value::String(expression)), None) => {
            let tz = match fields.get("tz") {
                None | Some(Value::Null) => "UTC".to_owned(),
                Some(Value::String(tz)) => tz.clone(),
                Some(_) => {
                    return Err(Unreadable::Invalid(
                        "the timetable's zone is not a string".to_owned(),
                    ));
                }
            };
            Ok(Timetable::Cron {
                expression: expression.clone(),
                tz,
            })
        }
        (None, Some(every)) => {
            let interval = every.as_u64().ok_or_else(|| {
                Unreadable::Invalid("the interval is not a whole number of seconds".to_owned())
            })?;
            let offset = match fields.get("offset") {
                None | Some(Value::Null) => 0,
                Some(offset) => offset.as_u64().ok_or_else(|| {
                    Unreadable::Invalid("the offset is not a whole number of seconds".to_owned())
                })?,
            };
            Ok(Timetable::Every {
                interval: Duration::from_secs(interval),
                offset: Duration::from_secs(offset),
            })
        }
        _ => Err(Unreadable::Invalid(
            "the timetable needs exactly one of `cron` and `every`".to_owned(),
        )),
    }
}

impl Policy {
    /// The policy JSON stored as `policy`. `Skip` stores its effective deadline, so that the
    /// stored definition says what it does.
    pub(crate) fn value(&self) -> Value {
        let mut fields = Map::new();
        fields.insert("catch_up".to_owned(), json!(self.catch_up.as_str()));
        if let CatchUp::All { max } = self.catch_up {
            fields.insert("max".to_owned(), json!(max));
        }
        if let Some(deadline) = self.effective_deadline() {
            fields.insert("deadline".to_owned(), json!(deadline.as_secs()));
        }
        Value::Object(fields)
    }

    /// Checks the bounds: `All { max }` from 1 to 10 000, a deadline of whole seconds, at least
    /// one.
    pub(crate) fn check(&self) -> Result<(), String> {
        if let CatchUp::All { max } = self.catch_up
            && !(1..=MAX_CATCH_UP).contains(&max)
        {
            return Err(format!(
                "CatchUp::All {{ max }} must be between 1 and {MAX_CATCH_UP}, not {max}"
            ));
        }
        if let Some(deadline) = self.deadline {
            let secs = whole_seconds(deadline, "the deadline")?;
            if secs == 0 || secs > MAX_DEADLINE_SECS {
                return Err(format!(
                    "the deadline must be between 1 second and {MAX_DEADLINE_SECS} seconds"
                ));
            }
        }
        Ok(())
    }
}

/// The policy stored as `policy`.
pub(crate) fn parse_policy(policy: &str) -> Result<Policy, Unreadable> {
    let value: Value = serde_json::from_str(policy)
        .map_err(|error| Unreadable::Invalid(format!("the policy is not JSON: {error}")))?;
    let Value::Object(fields) = value else {
        return Err(Unreadable::Invalid(
            "the policy is not a JSON object".to_owned(),
        ));
    };
    for key in fields.keys() {
        if !matches!(key.as_str(), "catch_up" | "max" | "deadline") {
            return Err(Unreadable::Newer(format!(
                "the policy has a field this version does not know (`{key}`)"
            )));
        }
    }
    let deadline = match fields.get("deadline") {
        None | Some(Value::Null) => None,
        Some(deadline) => Some(Duration::from_secs(deadline.as_u64().ok_or_else(|| {
            Unreadable::Invalid("the deadline is not a whole number of seconds".to_owned())
        })?)),
    };
    let catch_up = match fields.get("catch_up").and_then(Value::as_str) {
        None | Some("latest") => CatchUp::Latest,
        Some("skip") => CatchUp::Skip,
        Some("all") => {
            let max = fields
                .get("max")
                .and_then(Value::as_u64)
                .and_then(|max| u32::try_from(max).ok())
                .ok_or_else(|| Unreadable::Invalid("`all` needs a `max`".to_owned()))?;
            CatchUp::All { max }
        }
        Some(other) => {
            return Err(Unreadable::Newer(format!(
                "the catch-up policy `{other}` is unknown to this version"
            )));
        }
    };
    let policy = Policy { catch_up, deadline };
    policy.check().map_err(Unreadable::Invalid)?;
    Ok(policy)
}

/// The fields a template must not set: each job gets its own (`jid`, times, error fields),
/// another feature sets them per push or per execution (`bid`, `batch_callback`, `periodic`,
/// `limited`), or they make no sense before a push.
pub(crate) fn check_template(job: &JobPayload) -> Result<(), String> {
    if job.class.is_empty() {
        return Err("no job: set one with `job::<J>(args)` or `class(name, args)`".to_owned());
    }
    let per_job = [
        ("jid", !job.jid.is_empty()),
        ("at", job.at.is_some()),
        ("created_at", job.created_at.is_some()),
        ("enqueued_at", job.enqueued_at.is_some()),
        ("retry_count", job.retry_count.is_some()),
        ("failed_at", job.failed_at.is_some()),
        ("retried_at", job.retried_at.is_some()),
        ("discarded_at", job.discarded_at.is_some()),
        ("error_message", job.error_message.is_some()),
        ("error_class", job.error_class.is_some()),
        ("error_backtrace", job.error_backtrace.is_some()),
        ("bid", job.bid.is_some()),
        ("batch_callback", job.batch_callback.is_some()),
        ("periodic", job.periodic.is_some()),
        ("limited", job.limited.is_some()),
    ];
    if let Some((field, _)) = per_job.iter().find(|(_, set)| *set) {
        return Err(format!(
            "the job template must not set `{field}`: it is set for each job (or by another \
             feature) when the job is pushed"
        ));
    }
    if let Some(retry_for) = job.retry_for
        && retry_for >= 1_000_000_000
    {
        return Err("`retry_for` must be below 1000000000 seconds".to_owned());
    }
    if let Some(UniqueOption::Enabled(unique)) = &job.unique {
        if unique.digest.is_some() {
            return Err(
                "the job template's `unique` must not carry a digest: it is computed for each \
                 tick"
                    .to_owned(),
            );
        }
        if !matches!(unique.on_conflict, None | Some(OnConflict::Drop)) {
            return Err(
                "a periodic job's uniqueness can only drop duplicates (`on_conflict` drop): a \
                 poller has nobody to return an error to, and `replace` would let a tick remove \
                 another tick's job; when the job type declares another `on_conflict`, override \
                 it for the schedule with `.options(&JobOptions::new().on_conflict(\
                 OnConflict::Drop))`"
                    .to_owned(),
            );
        }
        match unique.ttl.map(|ttl| ttl.as_secs()) {
            Some(1..=999_999_999) => {}
            _ => {
                return Err(
                    "a unique periodic job needs a `unique_for` of at least one second and below \
                     1000000000 seconds"
                        .to_owned(),
                );
            }
        }
    }
    Ok(())
}

/// The template as stored: the payload's JSON without the fields that are set at each push
/// (an empty `jid` or `queue` is dropped), with object members sorted.
pub(crate) fn template_json(job: &JobPayload) -> Result<String, String> {
    let mut value = job
        .to_value()
        .map_err(|error| format!("the job template cannot be written as JSON: {error}"))?;
    if let Value::Object(fields) = &mut value {
        for key in ["jid", "created_at", "enqueued_at", "at"] {
            fields.remove(key);
        }
        if fields.get("queue").and_then(Value::as_str) == Some("") {
            fields.remove("queue");
        }
    }
    let bytes = canonical_json(&value).map_err(|error| error.to_string())?;
    String::from_utf8(bytes).map_err(|error| error.to_string())
}

/// The template stored as `job`, checked.
pub(crate) fn parse_template(job: &str) -> Result<JobPayload, Unreadable> {
    let payload = JobPayload::from_json(job)
        .map_err(|error| Unreadable::Invalid(format!("the job template is not valid: {error}")))?;
    check_template(&payload).map_err(Unreadable::Invalid)?;
    Ok(payload)
}

/// The digest of a definition.
pub(crate) fn definition_digest(
    spec: &Value,
    policy: &Value,
    job: &Value,
    description: Option<&str>,
) -> Result<String, String> {
    canonical_digest(&json!({
        "v": SPEC_VERSION,
        "spec": spec,
        "policy": policy,
        "job": job,
        "description": description,
    }))
    .map_err(|error| error.to_string())
}

/// The digest of a crontab: of its schedules' names and digests (`name`, `digest` pairs),
/// sorted. A definition's digest leaves its name out, so the names count here: two crontabs
/// with the same templates under other names differ.
pub(crate) fn crontab_digest<'a>(
    schedules: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> String {
    let mut schedules: Vec<(&str, &str)> = schedules.into_iter().collect();
    schedules.sort_unstable();
    let pairs: Vec<Value> = schedules
        .into_iter()
        .map(|(name, digest)| json!([name, digest]))
        .collect();
    let text = canonical_text(&Value::Array(pairs)).unwrap_or_default();
    sha256_b64url(text.as_bytes())
}

/// Canonical JSON text of `value`.
pub(crate) fn canonical_text(value: &Value) -> Result<String, String> {
    let bytes = canonical_json(value).map_err(|error| error.to_string())?;
    String::from_utf8(bytes).map_err(|error| error.to_string())
}

/// Reads a stored definition: the hash fields `spec`, `policy`, `job` and `digest`.
pub(crate) fn compile(
    spec: &str,
    policy: &str,
    job: &str,
    digest: &str,
) -> Result<Compiled, Unreadable> {
    let timetable = parse_spec(spec)?;
    if let Timetable::Cron { tz, .. } = &timetable
        && crate::periodic::timing::unknown_zone(tz)
    {
        return Err(Unreadable::UnknownZone(format!(
            "the time zone `{tz}` is not in this process's time-zone database (its tzdata may \
             be older than the writer's)"
        )));
    }
    let timing = timetable
        .timing()
        .map_err(|error| Unreadable::Invalid(error.0))?;
    let policy = parse_policy(policy)?;
    let template = parse_template(job)?;
    Ok(Compiled {
        digest: digest.to_owned(),
        timing,
        policy,
        template,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn specs_round_trip_and_newer_ones_are_recognized() {
        let cron = Timetable::Cron {
            expression: " 0  2 * * * ".to_owned(),
            tz: "Europe/Paris".to_owned(),
        };
        let spec = canonical_text(&cron.spec_value()).unwrap();
        assert_eq!(spec, r#"{"cron":"0 2 * * *","tz":"Europe/Paris","v":1}"#);
        assert_eq!(cron.timing_key(), "cron|0 2 * * *|Europe/Paris");
        assert_eq!(
            parse_spec(&spec).unwrap(),
            Timetable::Cron {
                expression: "0 2 * * *".to_owned(),
                tz: "Europe/Paris".to_owned()
            }
        );
        let every = Timetable::Every {
            interval: Duration::from_secs(30),
            offset: Duration::from_secs(5),
        };
        let spec = canonical_text(&every.spec_value()).unwrap();
        assert_eq!(spec, r#"{"every":30,"offset":5,"v":1}"#);
        assert_eq!(every.timing_key(), "every|30|5");
        assert_eq!(parse_spec(&spec).unwrap(), every);
        assert!(matches!(
            parse_spec(r#"{"v":2,"rrule":"FREQ=DAILY"}"#),
            Err(Unreadable::Newer(_))
        ));
        assert!(matches!(
            parse_spec(r#"{"v":1,"cron":"* * * * *","start":1}"#),
            Err(Unreadable::Newer(_))
        ));
        for invalid in [
            "x",
            "[]",
            r#"{"cron":"* * * * *"}"#,
            r#"{"v":1}"#,
            r#"{"v":1,"cron":"* * * * *","every":5}"#,
            r#"{"v":1,"every":-5}"#,
            r#"{"v":1,"cron":"* * * * *","tz":5}"#,
        ] {
            assert!(
                matches!(parse_spec(invalid), Err(Unreadable::Invalid(_))),
                "{invalid}"
            );
        }
    }

    #[test]
    fn policies_round_trip() {
        for policy in [
            Policy {
                catch_up: CatchUp::Latest,
                deadline: None,
            },
            Policy {
                catch_up: CatchUp::Latest,
                deadline: Some(Duration::from_secs(60)),
            },
            Policy {
                catch_up: CatchUp::All { max: 24 },
                deadline: None,
            },
        ] {
            let text = canonical_text(&policy.value()).unwrap();
            assert_eq!(parse_policy(&text).unwrap(), policy, "{text}");
        }
        // Skip stores its effective deadline.
        let skip = Policy {
            catch_up: CatchUp::Skip,
            deadline: None,
        };
        assert_eq!(skip.value(), json!({"catch_up": "skip", "deadline": 300}));
        assert_eq!(
            parse_policy(r#"{"catch_up":"skip","deadline":300}"#).unwrap(),
            Policy {
                catch_up: CatchUp::Skip,
                deadline: Some(Duration::from_secs(300))
            }
        );
        assert!(matches!(
            parse_policy(r#"{"catch_up":"spread"}"#),
            Err(Unreadable::Newer(_))
        ));
        assert!(matches!(
            parse_policy(r#"{"catch_up":"all"}"#),
            Err(Unreadable::Invalid(_))
        ));
        assert!(matches!(
            parse_policy(r#"{"catch_up":"all","max":0}"#),
            Err(Unreadable::Invalid(_))
        ));
        let bad = Policy {
            catch_up: CatchUp::Latest,
            deadline: Some(Duration::from_millis(1_500)),
        };
        assert!(bad.check().unwrap_err().contains("whole seconds"));
    }

    #[test]
    fn templates_refuse_per_job_and_other_features_fields() {
        let mut job = JobPayload::new("Report", vec![json!(1)]);
        assert!(check_template(&job).is_ok());
        assert!(check_template(&JobPayload::new("", vec![])).is_err());
        job.jid = "0123456789abcdef01234567".to_owned();
        assert!(check_template(&job).unwrap_err().contains("`jid`"));
        job.jid.clear();
        job.at = Some(1.0);
        assert!(check_template(&job).unwrap_err().contains("`at`"));
        job.at = None;
        job.bid = Some("89abcdef0123456789abcdef".to_owned());
        assert!(check_template(&job).unwrap_err().contains("`bid`"));
        job.bid = None;
        job.periodic = Some(crate::periodic::PeriodicMeta::new("x", 1, false));
        assert!(check_template(&job).unwrap_err().contains("`periodic`"));
        job.periodic = None;
        job.unique = Some(
            crate::unique::Unique::new(Duration::from_secs(60))
                .on_conflict(OnConflict::Replace)
                .into(),
        );
        assert!(
            check_template(&job)
                .unwrap_err()
                .contains("drop duplicates")
        );
        job.unique = Some(crate::unique::Unique::default().into());
        assert!(check_template(&job).unwrap_err().contains("unique_for"));
        job.unique = Some(crate::unique::Unique::new(Duration::from_secs(60)).into());
        assert!(check_template(&job).is_ok());
        job.retry_for = Some(1_000_000_000);
        assert!(check_template(&job).unwrap_err().contains("retry_for"));
    }

    #[test]
    fn templates_are_stored_without_per_push_fields_and_sorted() {
        let mut job = JobPayload::new("Report", vec![json!({"b": 1, "a": 2})]);
        job.retry = Some(crate::RetryOption::Count(3));
        let text = template_json(&job).unwrap();
        assert_eq!(
            text,
            r#"{"args":[{"a":2,"b":1}],"class":"Report","retry":3}"#
        );
        let parsed = parse_template(&text).unwrap();
        assert_eq!(parsed.class, "Report");
        assert_eq!(parsed.retry, Some(crate::RetryOption::Count(3)));
        assert!(parsed.jid.is_empty());
        assert!(matches!(
            parse_template(r#"{"class":"R","args":[],"jid":"x"}"#),
            Err(Unreadable::Invalid(_))
        ));
        assert!(matches!(parse_template("{"), Err(Unreadable::Invalid(_))));
    }

    #[test]
    fn digests_ignore_member_order_and_cover_every_part() {
        let spec = json!({"v": 1, "every": 30, "offset": 0});
        let policy = json!({"catch_up": "latest"});
        let job = json!({"class": "R", "args": []});
        let digest = definition_digest(&spec, &policy, &job, None).unwrap();
        assert_eq!(digest.len(), 43);
        let reordered = json!({"offset": 0, "every": 30, "v": 1});
        assert_eq!(
            definition_digest(&reordered, &policy, &job, None).unwrap(),
            digest
        );
        for changed in [
            definition_digest(
                &json!({"v": 1, "every": 60, "offset": 0}),
                &policy,
                &job,
                None,
            ),
            definition_digest(&spec, &json!({"catch_up": "skip"}), &job, None),
            definition_digest(&spec, &policy, &json!({"class": "S", "args": []}), None),
            definition_digest(&spec, &policy, &job, Some("text")),
        ] {
            assert_ne!(changed.unwrap(), digest);
        }
        assert_eq!(
            crontab_digest([("y", "b"), ("x", "a")]),
            crontab_digest([("x", "a"), ("y", "b")])
        );
        assert_ne!(
            crontab_digest([("x", "a")]),
            crontab_digest([("x", "a"), ("y", "b")])
        );
        // periodic-5: the names count (a definition's digest leaves its name out).
        assert_ne!(crontab_digest([("x", "a")]), crontab_digest([("y", "a")]));
    }

    #[test]
    fn names_have_bounds() {
        assert!(check_name("nightly-report").is_ok());
        assert!(check_name("tenant:42/digest ✓").is_ok());
        for bad in ["", " padded", "padded ", "tab\there", &"x".repeat(201)] {
            assert!(check_name(bad).is_err(), "{bad:?}");
        }
        assert!(check_name(&"x".repeat(200)).is_ok());
        // URL dot segments (round 3, web-api-security-7); other dots are fine.
        for bad in [".", ".."] {
            assert!(
                check_name(bad).unwrap_err().contains("dot segment"),
                "{bad:?}"
            );
        }
        for good in ["...", ".hidden", "v1.2", "./x"] {
            assert!(check_name(good).is_ok(), "{good:?}");
        }
    }
}
