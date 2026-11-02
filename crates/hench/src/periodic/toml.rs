//! `[[periodic]]` tables of the worker configuration file (`docs/design/periodic.md` §2.3.2):
//!
//! ```toml
//! [[periodic]]
//! name = "nightly-report"
//! cron = "0 2 * * *"            # or: every = 30 (seconds), offset = 5
//! tz = "Europe/Paris"
//! class = "nightly_report"
//! args = []
//! queue = "reports"
//! retry = 3                     # true | false | n
//! tags = ["reports"]
//! catch_up = "latest"           # "latest" | "skip" | "all"
//! catch_up_max = 24             # with "all"
//! deadline = 3600               # seconds
//! paused = false
//! description = "Daily PDF report"
//! unique_for = 3600             # seconds; with unique_until = "success" | "start" | "expired"
//! throttle = "warehouse"        # with throttle_key = "acme"
//! ```
//!
//! [`parse_schedules`] reads and checks a `periodic` array; the file's settings carry the
//! result to the builder. When the process registers the job type named by `class`, `build`
//! applies that job type's options (queue, retries, uniqueness, throttle) and its typed keys
//! under the table's own keys, as `Periodic::job` does, except its `on_conflict`: a schedule's
//! ticks drop duplicates. Without the `periodic` cargo feature, `schedules` refuses the array
//! rather than accept schedules nothing would register.

#[cfg(feature = "periodic")]
use std::time::Duration;

#[cfg(feature = "periodic")]
use serde::Deserialize;
#[cfg(feature = "periodic")]
use serde_json::Value;

use crate::error::{Error, Result};
#[cfg(feature = "periodic")]
use crate::payload::RetryOption;
#[cfg(feature = "periodic")]
use crate::periodic::{CatchUp, Periodic};
#[cfg(feature = "periodic")]
use crate::unique::UniqueUntil;

/// Refuses the `[[periodic]]` array of `path` in a build without the `periodic` cargo feature:
/// no process of this build would register its schedules.
#[cfg(not(feature = "periodic"))]
pub(crate) fn schedules(path: &str, _value: &toml::Value) -> Result<()> {
    Err(Error::Config(format!(
        "{path}: `[[periodic]]` schedules need hench's `periodic` cargo feature"
    )))
}

/// One `[[periodic]]` table.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg(feature = "periodic")]
struct FileSchedule {
    name: String,
    cron: Option<String>,
    every: Option<i64>,
    offset: Option<i64>,
    tz: Option<String>,
    class: String,
    args: Option<Vec<toml::Value>>,
    queue: Option<String>,
    retry: Option<toml::Value>,
    tags: Option<Vec<String>>,
    catch_up: Option<String>,
    catch_up_max: Option<i64>,
    deadline: Option<i64>,
    paused: Option<bool>,
    description: Option<String>,
    unique_for: Option<i64>,
    unique_until: Option<String>,
    throttle: Option<String>,
    throttle_key: Option<String>,
}

/// A TOML value as JSON (datetimes as their RFC 3339 text).
#[cfg(feature = "periodic")]
fn json_of(value: &toml::Value) -> Value {
    match value {
        toml::Value::String(text) => Value::String(text.clone()),
        toml::Value::Integer(number) => Value::from(*number),
        toml::Value::Float(number) => serde_json::Number::from_f64(*number)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        toml::Value::Boolean(flag) => Value::Bool(*flag),
        toml::Value::Datetime(datetime) => Value::String(datetime.to_string()),
        toml::Value::Array(items) => Value::Array(items.iter().map(json_of).collect()),
        toml::Value::Table(table) => Value::Object(
            table
                .iter()
                .map(|(key, value)| (key.clone(), json_of(value)))
                .collect(),
        ),
    }
}

/// Whole positive seconds.
#[cfg(feature = "periodic")]
fn seconds(value: i64, at: &str, key: &str) -> Result<Duration> {
    u64::try_from(value)
        .ok()
        .filter(|seconds| *seconds > 0)
        .map(Duration::from_secs)
        .ok_or_else(|| Error::Config(format!("{at}.{key}: a positive number of seconds")))
}

/// The schedules of a `periodic` array at `path` (the section's name, for errors), each
/// checked as [`Periodic::validate`] does: unknown keys, wrong types, both `cron` and `every`,
/// and invalid schedules are `Error::Config` naming the table (`periodic[2]`) and the key.
#[cfg(feature = "periodic")]
pub(crate) fn parse_schedules(path: &str, value: &toml::Value) -> Result<Vec<Periodic>> {
    let toml::Value::Array(tables) = value else {
        return Err(Error::Config(format!(
            "{path}: an array of tables (`[[{path}]]`)"
        )));
    };
    let mut schedules = Vec::with_capacity(tables.len());
    for (index, table) in tables.iter().enumerate() {
        let at = format!("{path}[{index}]");
        let file: FileSchedule = table.clone().try_into().map_err(|error: toml::de::Error| {
            Error::Config(format!("{at}: {}", error.to_string().trim_end()))
        })?;
        schedules.push(read_schedule(&at, file)?);
    }
    Ok(schedules)
}

#[cfg(feature = "periodic")]
fn read_schedule(at: &str, file: FileSchedule) -> Result<Periodic> {
    let invalid = |key: &str, reason: &str| Error::Config(format!("{at}.{key}: {reason}"));
    let mut periodic = match (file.cron, file.every) {
        (Some(expression), None) => {
            if file.offset.is_some() {
                return Err(invalid("offset", "an offset goes with `every`, not `cron`"));
            }
            let periodic = Periodic::cron(file.name.clone(), expression);
            match file.tz {
                Some(tz) => periodic.tz(tz),
                None => periodic,
            }
        }
        (None, Some(every)) => {
            if file.tz.is_some() {
                return Err(invalid(
                    "tz",
                    "a time zone goes with `cron`, not `every` (intervals are zone-independent)",
                ));
            }
            let periodic = Periodic::every(file.name.clone(), seconds(every, at, "every")?);
            match file.offset {
                Some(0) => periodic,
                Some(offset) => periodic.offset(seconds(offset, at, "offset")?),
                None => periodic,
            }
        }
        (Some(_), Some(_)) => {
            return Err(invalid("cron", "set either `cron` or `every`, not both"));
        }
        (None, None) => {
            return Err(Error::Config(format!(
                "{at}: a schedule needs `cron` (an expression) or `every` (seconds)"
            )));
        }
    };
    let args = file.args.unwrap_or_default().iter().map(json_of).collect();
    periodic = periodic.class(file.class, args);
    if let Some(queue) = file.queue {
        periodic = periodic.queue(queue);
    }
    if let Some(retry) = file.retry {
        periodic = periodic.retry(match retry {
            toml::Value::Boolean(flag) => RetryOption::Bool(flag),
            toml::Value::Integer(count) => RetryOption::Count(
                u32::try_from(count)
                    .map_err(|_| invalid("retry", "true, false or a number of retries"))?,
            ),
            _ => return Err(invalid("retry", "true, false or a number of retries")),
        });
    }
    if let Some(tags) = file.tags {
        periodic = periodic.tags(tags);
    }
    let catch_up = match file.catch_up.as_deref() {
        None | Some("latest") => {
            if file.catch_up_max.is_some() {
                return Err(invalid("catch_up_max", "goes with catch_up = \"all\""));
            }
            CatchUp::Latest
        }
        Some("skip") => {
            if file.catch_up_max.is_some() {
                return Err(invalid("catch_up_max", "goes with catch_up = \"all\""));
            }
            CatchUp::Skip
        }
        Some("all") => {
            let max = file
                .catch_up_max
                .ok_or_else(|| invalid("catch_up_max", "required with catch_up = \"all\""))?;
            CatchUp::All {
                max: u32::try_from(max)
                    .map_err(|_| invalid("catch_up_max", "between 1 and 10000"))?,
            }
        }
        Some(other) => {
            return Err(invalid(
                "catch_up",
                &format!("`{other}` is not a policy (\"latest\", \"skip\" or \"all\")"),
            ));
        }
    };
    periodic = periodic.catch_up(catch_up);
    if let Some(deadline) = file.deadline {
        periodic = periodic.deadline(seconds(deadline, at, "deadline")?);
    }
    if let Some(paused) = file.paused {
        periodic = periodic.paused(paused);
    }
    if let Some(description) = file.description {
        periodic = periodic.description(description);
    }
    if let Some(unique_for) = file.unique_for {
        periodic = periodic.unique_for(seconds(unique_for, at, "unique_for")?);
    }
    if let Some(until) = file.unique_until {
        let until = UniqueUntil::parse(&until).ok_or_else(|| {
            invalid(
                "unique_until",
                &format!("`{until}` is not a policy (\"success\", \"start\" or \"expired\")"),
            )
        })?;
        if file.unique_for.is_none() {
            return Err(invalid("unique_until", "goes with `unique_for`"));
        }
        periodic = periodic.unique_until(until);
    }
    if let Some(throttle) = file.throttle {
        periodic = periodic.throttle(throttle);
    }
    if let Some(key) = file.throttle_key {
        periodic = periodic.throttle_key(key);
    }
    periodic
        .validate()
        .map_err(|error| Error::Config(format!("{at}: {error}")))?;
    Ok(periodic.defined_in_file())
}

#[cfg(all(test, feature = "periodic"))]
mod tests {
    use super::*;
    use crate::periodic::Timetable;

    fn parse(text: &str) -> Result<Vec<Periodic>> {
        let value: toml::Table = toml::from_str(text).unwrap();
        parse_schedules("periodic", &value["periodic"])
    }

    fn error(text: &str) -> String {
        parse(text).unwrap_err().to_string()
    }

    #[test]
    fn tables_become_schedules() {
        let schedules = parse(
            r#"
            [[periodic]]
            name = "nightly-report"
            cron = "0 2 * * *"
            tz = "Europe/Paris"
            class = "nightly_report"
            args = [1, "two", { three = 3 }]
            queue = "reports"
            retry = 3
            tags = ["reports"]
            catch_up = "all"
            catch_up_max = 24
            deadline = 3600
            paused = true
            description = "Daily PDF report"
            unique_for = 3600
            unique_until = "start"
            throttle = "warehouse"
            throttle_key = "acme"

            [[periodic]]
            name = "refresh"
            every = 30
            offset = 5
            class = "rates::Refresh"
            retry = false
            catch_up = "skip"
            "#,
        )
        .unwrap();
        assert_eq!(schedules.len(), 2);
        let nightly = &schedules[0];
        assert_eq!(nightly.name(), "nightly-report");
        assert_eq!(
            nightly.timetable(),
            Timetable::Cron {
                expression: "0 2 * * *".to_owned(),
                tz: "Europe/Paris".to_owned()
            }
        );
        assert_eq!(nightly.catch_up_policy(), CatchUp::All { max: 24 });
        let job = nightly.payload(std::time::SystemTime::UNIX_EPOCH).unwrap();
        assert_eq!(
            job.args,
            vec![
                serde_json::json!(1),
                serde_json::json!("two"),
                serde_json::json!({"three": 3})
            ]
        );
        assert_eq!(job.queue, "reports");
        assert_eq!(job.retry, Some(RetryOption::Count(3)));
        assert_eq!(job.tags, ["reports"]);
        let refresh = &schedules[1];
        assert_eq!(
            refresh.timetable(),
            Timetable::Every {
                interval: Duration::from_secs(30),
                offset: Duration::from_secs(5)
            }
        );
        assert_eq!(refresh.catch_up_policy(), CatchUp::Skip);
    }

    #[test]
    fn every_mistake_names_the_table_and_the_key() {
        let base = "[[periodic]]\nname = \"a\"\nclass = \"C\"\n";
        for (extra, expected) in [
            (
                "cron = \"* * * * *\"\nnope = 1",
                "periodic[0]: unknown field `nope`",
            ),
            (
                "cron = \"* * * * *\"\nevery = 5",
                "periodic[0].cron: set either",
            ),
            ("", "periodic[0]: a schedule needs `cron`"),
            ("every = 0", "periodic[0].every: a positive number"),
            ("every = 30\ntz = \"UTC\"", "periodic[0].tz"),
            ("cron = \"* * * * *\"\noffset = 5", "periodic[0].offset"),
            ("cron = \"* * * * *\"\nretry = \"yes\"", "periodic[0].retry"),
            (
                "cron = \"* * * * *\"\ncatch_up = \"some\"",
                "periodic[0].catch_up",
            ),
            (
                "cron = \"* * * * *\"\ncatch_up = \"all\"",
                "periodic[0].catch_up_max: required",
            ),
            (
                "cron = \"* * * * *\"\ncatch_up_max = 3",
                "periodic[0].catch_up_max: goes with",
            ),
            (
                "cron = \"* * * * *\"\ndeadline = -1",
                "periodic[0].deadline",
            ),
            (
                "cron = \"0 0 30 2 *\"",
                "periodic[0]: invalid periodic job: `a`",
            ),
            (
                "cron = \"* * * * *\"\nunique_until = \"start\"",
                "periodic[0].unique_until: goes with",
            ),
            (
                "cron = \"* * * * *\"\nunique_for = 60\nunique_until = \"later\"",
                "periodic[0].unique_until",
            ),
            ("cron = \"* * * * *\"\nname2 = 1", "unknown field `name2`"),
        ] {
            let message = error(&format!("{base}{extra}"));
            assert!(message.contains(expected), "{extra:?}: {message}");
        }
        assert!(error("[periodic]\nname = \"x\"").contains("an array of tables"));
        // The index of the faulty table.
        let second = error(
            "[[periodic]]\nname = \"a\"\nclass = \"C\"\ncron = \"* * * * *\"\n\
             [[periodic]]\nname = \"b\"\nclass = \"C\"\n",
        );
        assert!(second.contains("periodic[1]"), "{second}");
    }
}
