//! Deploy marks: labelled points in time drawn on the metrics charts.

use std::collections::BTreeMap;

use chrono::{DateTime, NaiveDate, SecondsFormat, Timelike, Utc};

use super::query::{Granularity, days};
use crate::error::{Error, Result};
use crate::keys;
use crate::redis::RedisPool;

/// A deploy mark within a queried period.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct DeployMark {
    /// When the deploy happened (to the minute).
    pub at: DateTime<Utc>,
    /// Its label, typically a commit and its summary.
    pub label: String,
    /// The start of the metrics bucket it falls in.
    pub bucket: DateTime<Utc>,
}

/// Records deploys, so that the metrics charts show when the code changed.
///
/// Marks are stored per UTC day (hash [`keys::deploy_marks`], kept 90 days), keyed by the
/// minute of the deploy. Several processes marking the same deploy produce one mark: a mark is
/// only recorded if no mark with the same label was recorded within the last minute (lock
/// [`keys::deploy_lock`]), and only the first mark of a given minute is kept.
///
/// ```no_run
/// use hench::metrics::Deploy;
///
/// # async fn demo(redis: hench::RedisPool) -> hench::Result<()> {
/// let label = Deploy::git_label().unwrap_or_else(|| "manual deploy".to_owned());
/// Deploy::new(&redis).mark(&label, std::time::SystemTime::now()).await?;
/// # Ok(()) }
/// ```
#[derive(Debug, Clone)]
pub struct Deploy {
    redis: RedisPool,
}

impl Deploy {
    /// Deploy marks stored in `redis`.
    pub fn new(redis: &RedisPool) -> Self {
        Self {
            redis: redis.clone(),
        }
    }

    /// Records a deploy labelled `label` at `at` (rounded down to the minute). Returns `false`
    /// when it was not recorded because a mark with the same label was recorded less than a
    /// minute ago, or another mark already exists for that minute.
    pub async fn mark(&self, label: &str, at: impl Into<DateTime<Utc>>) -> Result<bool> {
        if label.trim().is_empty() {
            return Err(Error::Config("deploy labels must not be empty".to_owned()));
        }
        let floor = floor_to_minute(at.into());
        let stamp = format_stamp(floor);
        let lock = keys::deploy_lock(label);
        let mut conn = self.redis.conn().await?;
        // Sent once: a retried SET NX would report the lock as taken by ourselves.
        let locked: Option<String> = ::redis::cmd("SET")
            .arg(&lock)
            .arg(&stamp)
            .arg("NX")
            .arg("EX")
            .arg(keys::DEPLOY_LOCK_TTL.as_secs())
            .query_async(&mut conn)
            .await?;
        if locked.is_none() {
            return Ok(false);
        }
        let key = keys::deploy_marks(floor.date_naive());
        let mut pipe = ::redis::pipe();
        pipe.atomic()
            .cmd("HSETNX")
            .arg(&key)
            .arg(&stamp)
            .arg(label)
            .cmd("EXPIRE")
            .arg(&key)
            .arg(keys::DEPLOY_MARKS_TTL.as_secs())
            .ignore();
        let pipe = &pipe;
        let result: Result<(bool,)> = self
            .redis
            .with_retry(|mut conn| async move { pipe.query_async(&mut conn).await })
            .await;
        match result {
            Ok((added,)) => Ok(added),
            Err(error) => {
                // Let a retry mark the deploy rather than wait for the lock to expire.
                let released: ::redis::RedisResult<()> =
                    ::redis::cmd("DEL").arg(&lock).exec_async(&mut conn).await;
                if let Err(release_error) = released {
                    tracing::debug!(%release_error, "could not release a deploy lock");
                }
                Err(error)
            }
        }
    }

    /// The marks recorded on `date` (UTC), by time.
    pub async fn fetch(&self, date: NaiveDate) -> Result<BTreeMap<DateTime<Utc>, String>> {
        let key = keys::deploy_marks(date);
        let key = &key;
        let fields: Vec<(String, String)> = self
            .redis
            .with_retry(|mut conn| async move {
                ::redis::cmd("HGETALL")
                    .arg(key)
                    .query_async(&mut conn)
                    .await
            })
            .await?;
        Ok(fields
            .into_iter()
            .filter_map(|(stamp, label)| parse_stamp(&stamp).map(|at| (at, label)))
            .collect())
    }

    /// A label for the current deploy from git: the abbreviated hash and the summary of the
    /// last commit (`git log -1 --format="%h %s"` in the current directory), or `None` when git
    /// or a repository is not available.
    pub fn git_label() -> Option<String> {
        let output = std::process::Command::new("git")
            .args(["log", "-1", "--format=%h %s"])
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let label = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        (!label.is_empty()).then_some(label)
    }
}

/// The marks within `[starts_at, ends_at]`, oldest first, with their bucket at `granularity`.
pub(super) async fn fetch_marks(
    redis: &RedisPool,
    starts_at: DateTime<Utc>,
    ends_at: DateTime<Utc>,
    granularity: Granularity,
) -> Result<Vec<DeployMark>> {
    let mut pipe = ::redis::pipe();
    for day in days(starts_at, ends_at) {
        pipe.cmd("HGETALL").arg(keys::deploy_marks(day));
    }
    let pipe = &pipe;
    let per_day: Vec<Vec<(String, String)>> = redis
        .with_retry(|mut conn| async move { pipe.query_async(&mut conn).await })
        .await?;
    let mut marks: Vec<DeployMark> = per_day
        .into_iter()
        .flatten()
        .filter_map(|(stamp, label)| {
            let at = parse_stamp(&stamp)?;
            (starts_at <= at && at <= ends_at).then(|| DeployMark {
                at,
                label,
                bucket: granularity.truncate(at),
            })
        })
        .collect();
    marks.sort_by(|a, b| a.at.cmp(&b.at).then_with(|| a.label.cmp(&b.label)));
    Ok(marks)
}

fn floor_to_minute(at: DateTime<Utc>) -> DateTime<Utc> {
    at.with_second(0)
        .and_then(|at| at.with_nanosecond(0))
        .unwrap_or(at)
}

/// `2026-05-29T10:15:00Z`.
fn format_stamp(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn parse_stamp(stamp: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(stamp)
        .ok()
        .map(|at| at.with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn stamps() {
        let at = Utc.with_ymd_and_hms(2026, 5, 29, 10, 15, 30).unwrap();
        let floor = floor_to_minute(at + chrono::TimeDelta::milliseconds(250));
        assert_eq!(floor, Utc.with_ymd_and_hms(2026, 5, 29, 10, 15, 0).unwrap());
        assert_eq!(format_stamp(floor), "2026-05-29T10:15:00Z");
        assert_eq!(parse_stamp("2026-05-29T10:15:00Z"), Some(floor));
        assert_eq!(parse_stamp("2026-05-29T12:15:00+02:00"), Some(floor));
        assert_eq!(parse_stamp("yesterday"), None);
    }

    #[tokio::test]
    async fn labels_must_not_be_empty() {
        let deploy = Deploy::new(&RedisPool::detached());
        let result = deploy.mark(" ", Utc::now()).await;
        assert!(matches!(result, Err(Error::Config(_))), "{result:?}");
    }
}
