//! The state of iterable jobs: the `it-<jid>` hash (cursor, executions, run time and the
//! cancellation mark), in Redis or, for the testing clients, in memory.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use serde_json::Value;

use crate::client::Client;
use crate::error::Result;
use crate::keys;
use crate::launcher::lock;
use crate::payload::secs_to_system_time;
use crate::redis::RedisPool;

pub(crate) use keys::{
    ITERATION_CANCELLED as CANCELLED, ITERATION_CURSOR as CURSOR, ITERATION_DONE as DONE,
    ITERATION_EXECUTIONS as EXECUTIONS, ITERATION_RUNTIME as RUNTIME,
};

/// The raw fields of a state hash.
pub(crate) type Fields = HashMap<String, String>;

/// Iteration states kept in memory by the testing clients (fake and inline), which have no
/// Redis; shared by a client's clones.
#[derive(Debug, Default)]
pub(crate) struct LocalStates {
    states: Mutex<HashMap<String, Fields>>,
}

/// Where a client keeps iteration states.
pub(crate) enum Store<'a> {
    Redis(&'a RedisPool),
    Local(&'a LocalStates),
}

impl<'a> Store<'a> {
    /// The store of `client`: Redis, or memory for testing clients.
    pub(crate) fn of(client: &'a Client) -> Self {
        match client.local_states() {
            Some(local) => Self::Local(local),
            None => Self::Redis(client.redis()),
        }
    }

    /// Every field of the state of `jid` (empty when there is none).
    pub(crate) async fn load(&self, jid: &str) -> Result<Fields> {
        match self {
            Self::Redis(redis) => {
                let key = keys::iteration_state(jid);
                let key = &key;
                redis
                    .with_retry(|mut conn| async move {
                        ::redis::cmd("HGETALL")
                            .arg(key)
                            .query_async(&mut conn)
                            .await
                    })
                    .await
            }
            Self::Local(local) => Ok(lock(&local.states).get(jid).cloned().unwrap_or_default()),
        }
    }

    /// Writes `fields` (refreshing the expiry) and returns whether the job was cancelled. With
    /// `forget_recoveries` (the job saved progress), also forgets how many times it was
    /// recovered from processes that died while they held it ([`keys::recoveries`]): a job that
    /// makes progress is not what kills them.
    pub(crate) async fn save(
        &self,
        jid: &str,
        fields: &[(&str, String)],
        forget_recoveries: bool,
    ) -> Result<bool> {
        match self {
            Self::Redis(redis) => {
                let key = keys::iteration_state(jid);
                let mut pipe = ::redis::pipe();
                pipe.atomic()
                    .cmd("HSET")
                    .arg(&key)
                    .arg(fields)
                    .ignore()
                    .cmd("EXPIRE")
                    .arg(&key)
                    .arg(keys::ITERATION_STATE_TTL.as_secs())
                    .ignore();
                if forget_recoveries {
                    pipe.cmd("DEL").arg(keys::recoveries(jid)).ignore();
                }
                pipe.cmd("HEXISTS").arg(&key).arg(CANCELLED);
                let pipe = &pipe;
                let (cancelled,): (bool,) = redis
                    .with_retry(|mut conn| async move { pipe.query_async(&mut conn).await })
                    .await?;
                Ok(cancelled)
            }
            Self::Local(local) => {
                let mut states = lock(&local.states);
                let state = states.entry(jid.to_owned()).or_default();
                for (field, value) in fields {
                    state.insert((*field).to_owned(), value.clone());
                }
                Ok(state.contains_key(CANCELLED))
            }
        }
    }

    /// Marks `jid` as cancelled at `now` (epoch seconds) unless it already is; returns the
    /// time of the cancellation.
    pub(crate) async fn cancel(&self, jid: &str, now: i64) -> Result<i64> {
        let recorded = match self {
            Self::Redis(redis) => {
                let key = keys::iteration_state(jid);
                let mut pipe = ::redis::pipe();
                pipe.atomic()
                    .cmd("HSETNX")
                    .arg(&key)
                    .arg(CANCELLED)
                    .arg(now)
                    .ignore()
                    .cmd("HGET")
                    .arg(&key)
                    .arg(CANCELLED)
                    .cmd("EXPIRE")
                    .arg(&key)
                    .arg(keys::ITERATION_STATE_TTL.as_secs())
                    .ignore();
                let pipe = &pipe;
                let (recorded,): (Option<String>,) = redis
                    .with_retry(|mut conn| async move { pipe.query_async(&mut conn).await })
                    .await?;
                recorded
            }
            Self::Local(local) => lock(&local.states)
                .entry(jid.to_owned())
                .or_default()
                .entry(CANCELLED.to_owned())
                .or_insert_with(|| now.to_string())
                .clone()
                .into(),
        };
        Ok(recorded
            .and_then(|value| value.trim().parse().ok())
            .unwrap_or(now))
    }

    /// Deletes the state of `jid`.
    pub(crate) async fn delete(&self, jid: &str) -> Result<()> {
        match self {
            Self::Redis(redis) => {
                let key = keys::iteration_state(jid);
                let key = &key;
                redis
                    .with_retry(|mut conn| async move {
                        ::redis::cmd("UNLINK").arg(key).exec_async(&mut conn).await
                    })
                    .await
            }
            Self::Local(local) => {
                lock(&local.states).remove(jid);
                Ok(())
            }
        }
    }
}

/// The saved state of an iterable job, as shown in the dashboard: how far it got, how many
/// times it ran, for how long, and whether it was cancelled (Sidekiq's `IterableJobQuery`).
///
/// ```no_run
/// use hench::iterable::IterationState;
///
/// # async fn demo(redis: hench::RedisPool) -> hench::Result<()> {
/// if let Some(state) = IterationState::fetch(&redis, "0123456789abcdef01234567").await? {
///     println!("{} runs, cursor {:?}", state.executions, state.cursor);
/// }
/// # Ok(()) }
/// ```
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct IterationState {
    /// The job's id.
    pub jid: String,
    /// Number of executions (runs) so far.
    pub executions: u64,
    /// Total run time of those executions.
    pub runtime: Duration,
    /// The cursor of the last item processed, as JSON (`None` before the first one). A
    /// cursor that is not valid JSON is given as a JSON string of the raw value.
    pub cursor: Option<Value>,
    /// When the job was cancelled ([`Client::cancel`]).
    pub cancelled: Option<SystemTime>,
}

impl IterationState {
    /// The state of the iterable job `jid`, or `None` if it has none (not an iterable job, not
    /// started, or finished).
    pub async fn fetch(redis: &RedisPool, jid: &str) -> Result<Option<IterationState>> {
        let fields = Store::Redis(redis).load(jid).await?;
        Ok(Self::from_fields(jid, &fields))
    }

    /// The states of several jobs in one round trip (duplicate jids are fetched once): the jobs
    /// without a state are left out.
    pub async fn fetch_many<I, S>(
        redis: &RedisPool,
        jids: I,
    ) -> Result<HashMap<String, IterationState>>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut unique: Vec<String> = jids
            .into_iter()
            .map(|jid| jid.as_ref().to_owned())
            .filter(|jid| !jid.is_empty())
            .collect();
        unique.sort_unstable();
        unique.dedup();
        if unique.is_empty() {
            return Ok(HashMap::new());
        }
        let mut pipe = ::redis::pipe();
        for jid in &unique {
            pipe.cmd("HGETALL").arg(keys::iteration_state(jid));
        }
        let pipe = &pipe;
        let all: Vec<Fields> = redis
            .with_retry(|mut conn| async move { pipe.query_async(&mut conn).await })
            .await?;
        Ok(unique
            .into_iter()
            .zip(all)
            .filter_map(|(jid, fields)| Self::from_fields(&jid, &fields).map(|state| (jid, state)))
            .collect())
    }

    /// Reads the fields of a state hash; `None` when there are none.
    pub(crate) fn from_fields(jid: &str, fields: &Fields) -> Option<IterationState> {
        if fields.is_empty() {
            return None;
        }
        let cursor = fields
            .get(CURSOR)
            .and_then(|raw| match serde_json::from_str::<Value>(raw) {
                Ok(Value::Null) => None,
                Ok(value) => Some(value),
                Err(_) => Some(Value::String(raw.clone())),
            });
        Some(IterationState {
            jid: jid.to_owned(),
            executions: parse_executions(fields),
            runtime: parse_runtime(fields),
            cursor,
            cancelled: fields
                .get(CANCELLED)
                .and_then(|value| value.trim().parse::<f64>().ok())
                .map(secs_to_system_time),
        })
    }
}

/// The `ex` field (0 when absent or unreadable).
pub(crate) fn parse_executions(fields: &Fields) -> u64 {
    fields
        .get(EXECUTIONS)
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(0)
}

/// The `rt` field (zero when absent, unreadable or negative).
pub(crate) fn parse_runtime(fields: &Fields) -> Duration {
    fields
        .get(RUNTIME)
        .and_then(|value| value.trim().parse::<f64>().ok())
        .and_then(|secs| Duration::try_from_secs_f64(secs).ok())
        .unwrap_or_default()
}

impl Client {
    /// Cancels the [iterable job](crate::iterable) `jid`: it stops before its next item
    /// (iterable jobs check at the start of each execution and at every checkpoint, see
    /// [`IterableJob::checkpoint_interval`](crate::iterable::IterableJob::checkpoint_interval)),
    /// runs its `on_cancel`, `on_stop` and `on_complete` hooks and completes. Cancelling a job
    /// that has not started yet works too (the mark is kept for 30 days); cancelling twice
    /// keeps the first time. Returns the time of the cancellation.
    ///
    /// Testing clients ([`Client::fake`], [`Client::inline`]) keep the mark in memory, where
    /// their jobs find it.
    pub async fn cancel(&self, jid: &str) -> Result<SystemTime> {
        let now = chrono::Utc::now().timestamp();
        let cancelled = Store::of(self).cancel(jid, now).await?;
        Ok(secs_to_system_time(cancelled as f64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fields(pairs: &[(&str, &str)]) -> Fields {
        pairs
            .iter()
            .map(|(field, value)| ((*field).to_owned(), (*value).to_owned()))
            .collect()
    }

    #[test]
    fn states_parse_leniently() {
        assert_eq!(IterationState::from_fields("j", &Fields::new()), None);
        let state = IterationState::from_fields(
            "j",
            &fields(&[("ex", "5"), ("rt", "1.5"), ("c", r#"{"offset":100}"#)]),
        )
        .unwrap();
        assert_eq!(state.executions, 5);
        assert_eq!(state.runtime, Duration::from_millis(1_500));
        assert_eq!(state.cursor, Some(serde_json::json!({"offset": 100})));
        assert_eq!(state.cancelled, None);

        let state =
            IterationState::from_fields("j", &fields(&[("c", "not-json"), ("rt", "-3")])).unwrap();
        assert_eq!(state.cursor, Some(Value::String("not-json".into())));
        assert_eq!(state.runtime, Duration::ZERO);
        assert_eq!(state.executions, 0);

        let state = IterationState::from_fields(
            "j",
            &fields(&[("ex", "1"), ("cancelled", "1700000000"), ("c", "null")]),
        )
        .unwrap();
        assert_eq!(state.cursor, None);
        assert_eq!(
            state.cancelled,
            Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000))
        );
    }

    #[tokio::test]
    async fn local_states() {
        let local = LocalStates::default();
        let store = Store::Local(&local);
        assert!(store.load("j").await.unwrap().is_empty());
        assert!(
            !store
                .save("j", &[(EXECUTIONS, "1".to_owned())], false)
                .await
                .unwrap()
        );
        assert_eq!(store.cancel("j", 10).await.unwrap(), 10);
        assert_eq!(store.cancel("j", 20).await.unwrap(), 10);
        assert!(
            store
                .save("j", &[(EXECUTIONS, "2".to_owned())], true)
                .await
                .unwrap()
        );
        assert_eq!(store.load("j").await.unwrap()[EXECUTIONS], "2");
        store.delete("j").await.unwrap();
        assert!(store.load("j").await.unwrap().is_empty());
    }
}
