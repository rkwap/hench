//! [`Stats`] (current totals), [`QueueSummary`] and [`StatsHistory`] (daily counters).

use chrono::{Days, NaiveDate, Utc};

use crate::api::queue::scan_set;
use crate::api::{Api, latency_of_raw, parse_count};
use crate::error::{Error, Result};
use crate::keys;

/// How far back daily counters are kept (they expire after five years).
const MAX_HISTORY_DAYS: u32 = 5 * 365;

/// A snapshot of the cluster's statistics (Sidekiq's `Stats`), read with two pipelined round
/// trips (plus `SSCAN`s of the `processes` and `queues` sets).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Stats {
    /// Jobs processed since the counters were last reset.
    pub processed: u64,
    /// Jobs that failed since the counters were last reset.
    pub failed: u64,
    /// Jobs in the scheduled set.
    pub scheduled_size: u64,
    /// Jobs in the retry set.
    pub retry_size: u64,
    /// Jobs in the morgue.
    pub dead_size: u64,
    /// Live processes: the members of the `processes` set with a heartbeat (processes that died
    /// stay in the set until they are pruned, about a minute after their heartbeat expired).
    pub processes_size: u64,
    /// Seconds the oldest job of the `default` queue has been waiting.
    pub default_queue_latency: f64,
    /// Jobs being run, from the processes' heartbeats.
    pub workers_size: u64,
    /// Jobs waiting in all queues.
    pub enqueued: u64,
}

impl Stats {
    /// Reads the current statistics.
    pub async fn fetch(api: impl Into<Api>) -> Result<Stats> {
        let api = api.into();
        let default_queue = keys::queue("default");
        let mut pipe = ::redis::pipe();
        pipe.cmd("GET")
            .arg(keys::STAT_PROCESSED)
            .cmd("GET")
            .arg(keys::STAT_FAILED)
            .cmd("ZCARD")
            .arg(keys::SCHEDULE)
            .cmd("ZCARD")
            .arg(keys::RETRY)
            .cmd("ZCARD")
            .arg(keys::DEAD)
            .cmd("LINDEX")
            .arg(&default_queue)
            .arg(-1);
        let pipe = &pipe;
        type Fast = (
            Option<String>,
            Option<String>,
            u64,
            u64,
            u64,
            Option<String>,
        );
        let (processed, failed, scheduled_size, retry_size, dead_size, oldest): Fast = api
            .redis()
            .with_retry(|mut conn| async move { pipe.query_async(&mut conn).await })
            .await?;

        let processes = scan_set(api.redis(), keys::PROCESSES).await?;
        let queues = scan_set(api.redis(), keys::QUEUES).await?;
        let (processes_size, workers_size, enqueued) = if processes.is_empty() && queues.is_empty()
        {
            (0, 0, 0)
        } else {
            let mut pipe = ::redis::pipe();
            for process in &processes {
                pipe.cmd("HGET").arg(keys::process(process)).arg("busy");
            }
            for queue in &queues {
                pipe.cmd("LLEN").arg(keys::queue(queue));
            }
            let pipe = &pipe;
            let replies: Vec<::redis::Value> = api
                .redis()
                .with_retry(|mut conn| async move { pipe.query_async(&mut conn).await })
                .await?;
            let (busy, lengths) = replies.split_at(processes.len().min(replies.len()));
            (
                // A heartbeat writes `busy` with the rest of the process's description: a
                // member without it died (or never beat).
                busy.iter()
                    .filter(|reply| !matches!(reply, ::redis::Value::Nil))
                    .count() as u64,
                busy.iter().map(count_reply).sum(),
                lengths.iter().map(count_reply).sum(),
            )
        };

        Ok(Stats {
            processed: parse_count(processed.as_deref()),
            failed: parse_count(failed.as_deref()),
            scheduled_size,
            retry_size,
            dead_size,
            processes_size,
            default_queue_latency: oldest.as_deref().map_or(0.0, latency_of_raw),
            workers_size,
            enqueued,
        })
    }

    /// The size of every known queue, largest first (ties by name).
    pub async fn queues(api: impl Into<Api>) -> Result<Vec<(String, u64)>> {
        let api = api.into();
        let names = scan_set(api.redis(), keys::QUEUES).await?;
        if names.is_empty() {
            return Ok(Vec::new());
        }
        let mut pipe = ::redis::pipe();
        for name in &names {
            pipe.cmd("LLEN").arg(keys::queue(name));
        }
        let pipe = &pipe;
        let sizes: Vec<u64> = api
            .redis()
            .with_retry(|mut conn| async move { pipe.query_async(&mut conn).await })
            .await?;
        let mut queues: Vec<(String, u64)> = names.into_iter().zip(sizes).collect();
        queues.sort_by(|(a_name, a_size), (b_name, b_size)| {
            b_size.cmp(a_size).then_with(|| a_name.cmp(b_name))
        });
        Ok(queues)
    }

    /// The name, size, latency, paused state and limits of every known queue and of every
    /// paused or limited queue, largest first (ties by name), in one pipelined round trip after
    /// the scans.
    ///
    /// Pausing and limits are by name: a paused or limited queue emptied by
    /// [`Queue::clear`](crate::api::Queue::clear), or paused or limited before its first job,
    /// is not in `queues`, but it stays paused or limited (and holds the jobs pushed to it
    /// later), so it is listed, with its size.
    pub async fn queue_summaries(api: impl Into<Api>) -> Result<Vec<QueueSummary>> {
        let api = api.into();
        let mut names = scan_set(api.redis(), keys::QUEUES).await?;
        names.extend(scan_set(api.redis(), keys::PAUSED).await?);
        names.extend(limited_queues(api.redis()).await?);
        names.sort_unstable();
        names.dedup();
        if names.is_empty() {
            return Ok(Vec::new());
        }
        let mut pipe = ::redis::pipe();
        for name in &names {
            let key = keys::queue(name);
            pipe.cmd("LLEN")
                .arg(&key)
                .cmd("LINDEX")
                .arg(&key)
                .arg(-1)
                .cmd("SISMEMBER")
                .arg(keys::PAUSED)
                .arg(name)
                .cmd("HGET")
                .arg(keys::QUEUE_LIMITS)
                .arg(name);
        }
        let pipe = &pipe;
        let replies: Vec<(u64, Option<String>, bool, Option<String>)> = api
            .redis()
            .with_retry(|mut conn| async move { pipe.query_async(&mut conn).await })
            .await?;
        let mut summaries: Vec<QueueSummary> = names
            .into_iter()
            .zip(replies)
            .map(|(name, (size, oldest, paused, limits))| QueueSummary {
                name,
                size,
                latency: oldest.as_deref().map_or(0.0, latency_of_raw),
                paused,
                limits,
            })
            .collect();
        summaries.sort_by(|a, b| b.size.cmp(&a.size).then_with(|| a.name.cmp(&b.name)));
        Ok(summaries)
    }

    /// Resets lifetime counters to zero: both when `counters` is empty, else the given ones.
    /// The daily counters are kept.
    pub async fn reset(api: impl Into<Api>, counters: &[StatCounter]) -> Result<()> {
        let api = api.into();
        let all = [StatCounter::Processed, StatCounter::Failed];
        let counters = if counters.is_empty() {
            &all[..]
        } else {
            counters
        };
        let mut command = ::redis::cmd("MSET");
        for counter in counters {
            command.arg(counter.key()).arg(0);
        }
        let command = &command;
        api.redis()
            .with_retry(|mut conn| async move { command.exec_async(&mut conn).await })
            .await
    }
}

/// A count from a pipelined reply (an integer, a numeric string or nil).
fn count_reply(value: &::redis::Value) -> u64 {
    match value {
        ::redis::Value::Int(count) => u64::try_from(*count).unwrap_or(0),
        ::redis::Value::BulkString(bytes) => parse_count(std::str::from_utf8(bytes).ok()),
        ::redis::Value::SimpleString(text) => parse_count(Some(text)),
        _ => 0,
    }
}

/// One of the lifetime counters, for [`Stats::reset`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum StatCounter {
    /// `stat:processed`.
    Processed,
    /// `stat:failed`.
    Failed,
}

impl StatCounter {
    fn key(self) -> &'static str {
        match self {
            Self::Processed => keys::STAT_PROCESSED,
            Self::Failed => keys::STAT_FAILED,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Processed => "processed",
            Self::Failed => "failed",
        }
    }
}

/// The size, latency, paused state and limits of a queue, from [`Stats::queue_summaries`].
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct QueueSummary {
    /// The queue's name.
    pub name: String,
    /// Jobs in the queue.
    pub size: u64,
    /// Seconds the oldest job has been waiting.
    pub latency: f64,
    /// `true` if the queue is in the `paused` set.
    pub paused: bool,
    /// The queue's limits as stored (`concurrency=3;window=100/1m`, see
    /// [`Queue::set_limits`](crate::api::Queue::set_limits)), if it has some.
    pub limits: Option<String>,
}

/// Daily processed and failed counts (Sidekiq's `Stats::History`), from the
/// `stat:<counter>:YYYY-MM-DD` keys the workers maintain (UTC days).
#[derive(Debug, Clone)]
pub struct StatsHistory {
    api: Api,
    days: u32,
    start: NaiveDate,
}

impl StatsHistory {
    /// The `days` days ending today (UTC). Fails with [`Error::Config`] unless `days` is
    /// between 1 and 1825 (five years: older counters have expired).
    pub fn new(api: impl Into<Api>, days: u32) -> Result<Self> {
        Self::ending(api, days, Utc::now().date_naive())
    }

    /// The `days` days ending on `start` (included).
    pub fn ending(api: impl Into<Api>, days: u32, start: NaiveDate) -> Result<Self> {
        if !(1..=MAX_HISTORY_DAYS).contains(&days) {
            return Err(Error::Config(format!(
                "a stats history covers 1 to {MAX_HISTORY_DAYS} days, not {days}"
            )));
        }
        Ok(Self {
            api: api.into(),
            days,
            start,
        })
    }

    /// Jobs processed each day, most recent first. Days without data count zero.
    pub async fn processed(&self) -> Result<Vec<(NaiveDate, u64)>> {
        self.daily(StatCounter::Processed).await
    }

    /// Jobs failed each day, most recent first. Days without data count zero.
    pub async fn failed(&self) -> Result<Vec<(NaiveDate, u64)>> {
        self.daily(StatCounter::Failed).await
    }

    /// The days covered, most recent first.
    pub fn dates(&self) -> Vec<NaiveDate> {
        (0..self.days)
            .map_while(|offset| self.start.checked_sub_days(Days::new(u64::from(offset))))
            .collect()
    }

    async fn daily(&self, counter: StatCounter) -> Result<Vec<(NaiveDate, u64)>> {
        let dates = self.dates();
        let keys: Vec<String> = dates
            .iter()
            .map(|date| keys::stat_on(counter.name(), *date))
            .collect();
        let keys = &keys;
        let values: Vec<Option<String>> = self
            .api
            .redis()
            .with_retry(|mut conn| async move {
                ::redis::cmd("MGET").arg(keys).query_async(&mut conn).await
            })
            .await?;
        Ok(dates
            .into_iter()
            .zip(values)
            .map(|(date, value)| (date, parse_count(value.as_deref())))
            .collect())
    }
}

/// The queues with limits (the fields of `queue-limits`).
async fn limited_queues(redis: &crate::redis::RedisPool) -> Result<Vec<String>> {
    let names: Vec<Vec<u8>> = redis
        .with_retry(|mut conn| async move {
            ::redis::cmd("HKEYS")
                .arg(keys::QUEUE_LIMITS)
                .query_async(&mut conn)
                .await
        })
        .await?;
    Ok(names
        .iter()
        .map(|name| String::from_utf8_lossy(name).into_owned())
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::redis::RedisPool;

    #[test]
    fn history_bounds() {
        let api = Api::new(RedisPool::detached());
        assert!(StatsHistory::new(&api, 0).is_err());
        assert!(StatsHistory::new(&api, 2_000).is_err());
        assert!(StatsHistory::new(&api, 200).is_ok());
        assert!(StatsHistory::new(&api, MAX_HISTORY_DAYS).is_ok());
        let start = NaiveDate::from_ymd_opt(2012, 12, 26).unwrap();
        let history = StatsHistory::ending(&api, 3, start).unwrap();
        assert_eq!(
            history.dates(),
            [
                start,
                NaiveDate::from_ymd_opt(2012, 12, 25).unwrap(),
                NaiveDate::from_ymd_opt(2012, 12, 24).unwrap()
            ]
        );
    }

    #[test]
    fn counts_from_replies() {
        assert_eq!(count_reply(&::redis::Value::Int(4)), 4);
        assert_eq!(count_reply(&::redis::Value::Int(-4)), 0);
        assert_eq!(count_reply(&::redis::Value::BulkString(b"12".to_vec())), 12);
        assert_eq!(count_reply(&::redis::Value::Nil), 0);
        assert_eq!(StatCounter::Processed.key(), "stat:processed");
        assert_eq!(StatCounter::Failed.name(), "failed");
    }
}
