//! The per-minute counters of the features (`u|…` unique, `b|…` batches, `l|…` limits; one
//! hash per UTC minute, fields `<name>|<event>`, kept 8 hours): the one reader shared by
//! `UniqueLocks::events`, `Limits::events` and the Batches summary
//! (`docs/design/pro-features.md` §2.1).

use std::collections::BTreeMap;

use chrono::{DateTime, Duration as ChronoDuration, DurationRound, Utc};

use crate::error::{Error, Result};
use crate::keys;
use crate::metrics::Period;
use crate::redis::RedisPool;

/// Which feature's counters to read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum EventKind {
    /// `u|YYYYMMDD|H:MM`: per job class, `d` (duplicates), `r` (replaced), `z` (lapses).
    Unique,
    /// `b|YYYYMMDD|H:MM`: `created`, `complete`, `success`, `death`, `cancelled`.
    Batches,
    /// `l|YYYYMMDD|H:MM`: per limiter name, `g` (granted) and `r` (refused).
    Limits,
}

impl EventKind {
    /// The hash of the minute of `at`.
    fn key(self, at: DateTime<Utc>) -> String {
        match self {
            EventKind::Unique => keys::unique_minute(at),
            EventKind::Batches => keys::batch_minute(at),
            EventKind::Limits => keys::limit_minute(at),
        }
    }
}

/// The counters of a period, per name and event, minute by minute.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Events {
    /// The first minute of the period.
    pub(crate) starts_at: Option<DateTime<Utc>>,
    /// Sums over the period: `(name, event)` → count. Fields without a `|` count under the
    /// empty name.
    pub(crate) totals: BTreeMap<(String, String), u64>,
    /// Counts per minute (minutes without data are absent): minute → `(name, event)` → count.
    pub(crate) series: BTreeMap<DateTime<Utc>, BTreeMap<(String, String), u64>>,
}

/// Reads the counters of `kind` over `period` ending at `now`. Only minute periods of up to 8
/// hours exist (the counters are not rolled up): an hour period or a longer one is an
/// [`Error::Config`].
pub(crate) async fn read(
    redis: &RedisPool,
    kind: EventKind,
    period: Period,
    now: DateTime<Utc>,
) -> Result<Events> {
    let minutes = match period {
        Period::Minutes(minutes) if (1..=MAX_MINUTES).contains(&minutes) => minutes,
        other => {
            return Err(Error::Config(format!(
                "feature counters are kept per minute for 8 hours: a period of {} cannot be read",
                other.label()
            )));
        }
    };
    let now = now
        .duration_trunc(ChronoDuration::minutes(1))
        .unwrap_or(now);
    let times: Vec<DateTime<Utc>> = (0..minutes)
        .map(|index| now - ChronoDuration::minutes(i64::from(index)))
        .collect();
    let mut pipe = ::redis::pipe();
    for time in &times {
        pipe.cmd("HGETALL").arg(kind.key(*time));
    }
    let pipe = &pipe;
    let hashes: Vec<Vec<(String, String)>> = redis
        .with_retry(|mut conn| async move { pipe.query_async(&mut conn).await })
        .await?;
    let mut events = Events {
        starts_at: times.last().copied(),
        ..Events::default()
    };
    for (time, fields) in times.into_iter().zip(hashes) {
        for (field, value) in fields {
            let Ok(count) = value.parse::<u64>() else {
                continue;
            };
            let (name, event) = keys::parse_metric_field(&field).unwrap_or(("", &field));
            let entry = (name.to_owned(), event.to_owned());
            *events.totals.entry(entry.clone()).or_default() += count;
            *events
                .series
                .entry(time)
                .or_default()
                .entry(entry)
                .or_default() += count;
        }
    }
    Ok(events)
}

/// Longest period the counters cover, in minutes (their 8-hour expiry).
const MAX_MINUTES: u32 = 480;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_period_matches_the_expiry() {
        assert_eq!(
            u64::from(MAX_MINUTES) * 60,
            keys::FEATURE_MINUTE_TTL.as_secs()
        );
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    async fn counters_are_read_per_minute() {
        use chrono::TimeZone;
        let redis = crate::test_util::TestRedis::start().await;
        let pool = redis.pool().await;
        let now = Utc.with_ymd_and_hms(2026, 11, 1, 9, 30, 42).unwrap();
        let earlier = now - ChronoDuration::minutes(2);
        let mut conn = pool.conn().await.unwrap();
        let _: () = ::redis::pipe()
            .cmd("HSET")
            .arg(keys::limit_minute(now))
            .arg("stripe|g")
            .arg(3)
            .arg("stripe|r")
            .arg(1)
            .cmd("HSET")
            .arg(keys::limit_minute(earlier))
            .arg("stripe|g")
            .arg(2)
            .arg("junk")
            .arg("x")
            .cmd("HSET")
            .arg(keys::limit_minute(now - ChronoDuration::minutes(10)))
            .arg("stripe|g")
            .arg(100)
            .query_async(&mut conn)
            .await
            .unwrap();
        let events = read(&pool, EventKind::Limits, Period::Minutes(5), now)
            .await
            .unwrap();
        let key = |event: &str| ("stripe".to_owned(), event.to_owned());
        assert_eq!(events.totals.get(&key("g")), Some(&5));
        assert_eq!(events.totals.get(&key("r")), Some(&1));
        assert_eq!(events.series.len(), 2);
        assert_eq!(
            events.starts_at,
            Some(Utc.with_ymd_and_hms(2026, 11, 1, 9, 26, 0).unwrap())
        );
        assert!(matches!(
            read(&pool, EventKind::Limits, Period::Hours(1), now).await,
            Err(Error::Config(_))
        ));
        assert!(matches!(
            read(&pool, EventKind::Unique, Period::Minutes(481), now).await,
            Err(Error::Config(_))
        ));
    }
}
