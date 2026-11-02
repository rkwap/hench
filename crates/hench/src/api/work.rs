//! [`WorkSet`] and [`Work`]: the jobs being run right now.

use std::fmt;
use std::time::SystemTime;

use serde_json::Value;

use crate::api::process::identities;
use crate::api::{Api, JobRecord, json_escaped, parse_count, process_time_secs};
use crate::error::Result;
use crate::keys;
use crate::payload::secs_to_system_time;

/// The jobs running in every live process (Sidekiq's `WorkSet`), as reported by their last
/// heartbeat: this is a snapshot a few seconds old, not real time.
#[derive(Clone)]
pub struct WorkSet {
    api: Api,
}

impl WorkSet {
    /// The running jobs of `api`'s Redis.
    pub fn new(api: impl Into<Api>) -> Self {
        Self { api: api.into() }
    }

    /// Every running job, longest running first. One pipelined round trip for all processes.
    pub async fn list(&self) -> Result<Vec<Work>> {
        let identities = identities(&self.api).await?;
        if identities.is_empty() {
            return Ok(Vec::new());
        }
        let mut pipe = ::redis::pipe();
        for identity in &identities {
            pipe.cmd("HGETALL").arg(keys::work(identity));
        }
        let pipe = &pipe;
        let per_process: Vec<Vec<(String, String)>> = self
            .api
            .redis()
            .with_retry(|mut conn| async move { pipe.query_async(&mut conn).await })
            .await?;
        let mut works: Vec<Work> = identities
            .iter()
            .zip(per_process)
            .flat_map(|(identity, slots)| {
                slots
                    .into_iter()
                    .filter(|(_, json)| !json.is_empty())
                    .map(|(slot, json)| Work::parse(&self.api, identity, slot, &json))
            })
            .collect();
        works.sort_by(|a, b| a.run_at.total_cmp(&b.run_at));
        Ok(works)
    }

    /// The number of running jobs, from the processes' `busy` counts.
    pub async fn size(&self) -> Result<u64> {
        let identities = identities(&self.api).await?;
        if identities.is_empty() {
            return Ok(0);
        }
        let mut pipe = ::redis::pipe();
        for identity in &identities {
            pipe.cmd("HGET").arg(keys::process(identity)).arg("busy");
        }
        let pipe = &pipe;
        let busy: Vec<Option<String>> = self
            .api
            .redis()
            .with_retry(|mut conn| async move { pipe.query_async(&mut conn).await })
            .await?;
        Ok(busy.iter().map(|busy| parse_count(busy.as_deref())).sum())
    }

    /// The running job with this jid, if any.
    pub async fn find_work(&self, jid: &str) -> Result<Option<Work>> {
        if jid.is_empty() {
            return Ok(None);
        }
        let needle = json_escaped(jid);
        Ok(self
            .list()
            .await?
            .into_iter()
            .find(|work| work.payload.contains(&needle) && work.job().jid() == Some(jid)))
    }
}

impl fmt::Debug for WorkSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkSet")
            .field("redis", self.api.redis())
            .finish()
    }
}

/// A job being run (Sidekiq's `Work`): which process and slot runs it, from which queue, since
/// when, and the job itself.
#[derive(Clone)]
pub struct Work {
    api: Api,
    process: String,
    slot: String,
    queue: String,
    payload: String,
    run_at: f64,
}

impl Work {
    /// Parses one entry of a process's work hash: `{"queue", "payload", "run_at"}`, where the
    /// payload is the job JSON as a string (an object is accepted too).
    fn parse(api: &Api, process: &str, slot: String, json: &str) -> Self {
        let work: serde_json::Map<String, Value> = match serde_json::from_str(json) {
            Ok(work) => work,
            Err(error) => {
                tracing::warn!(process, slot, %error, "malformed work entry");
                serde_json::Map::new()
            }
        };
        let payload = match work.get("payload") {
            Some(Value::String(payload)) => payload.clone(),
            Some(Value::Null) | None => String::new(),
            Some(other) => other.to_string(),
        };
        Self {
            api: api.clone(),
            process: process.to_owned(),
            slot,
            queue: work
                .get("queue")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            payload,
            run_at: work
                .get("run_at")
                .and_then(process_time_secs)
                .unwrap_or(0.0),
        }
    }

    /// The identity of the process running the job.
    pub fn process_id(&self) -> &str {
        &self.process
    }

    /// The worker slot running the job within its process (Sidekiq's thread id).
    pub fn slot(&self) -> &str {
        &self.slot
    }

    /// The queue the job was fetched from.
    pub fn queue(&self) -> &str {
        &self.queue
    }

    /// When the job started running.
    pub fn run_at(&self) -> SystemTime {
        secs_to_system_time(self.run_at)
    }

    /// The job JSON.
    pub fn payload(&self) -> &str {
        &self.payload
    }

    /// The job as a record.
    pub fn job(&self) -> JobRecord {
        JobRecord::new(&self.api, self.payload.clone(), Some(&self.queue))
    }
}

impl fmt::Debug for Work {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Work")
            .field("process", &self.process)
            .field("slot", &self.slot)
            .field("queue", &self.queue)
            .field("run_at", &self.run_at)
            .field("payload", &self.payload)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::redis::RedisPool;
    use serde_json::json;

    fn work(value: &str) -> Work {
        Work::parse(
            &Api::new(RedisPool::detached()),
            "p",
            "slot-1".to_owned(),
            value,
        )
    }

    #[test]
    fn parses_work_entries() {
        let entry = work(
            &json!({"queue": "default", "payload": "{\"jid\":\"abc\",\"args\":[1]}", "run_at": 1_700_000_000.5})
                .to_string(),
        );
        assert_eq!(entry.process_id(), "p");
        assert_eq!(entry.slot(), "slot-1");
        assert_eq!(entry.queue(), "default");
        assert_eq!(entry.job().jid(), Some("abc"));
        assert_eq!(entry.job().args(), [json!(1)]);
        assert_eq!(
            entry
                .run_at()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            1_700_000_000_500
        );
        assert!(format!("{entry:?}").contains("slot-1"));

        // Object payloads, integer and millisecond times are accepted.
        let entry =
            work(&json!({"payload": {"jid": "x"}, "run_at": 1_700_000_000_000_i64}).to_string());
        assert_eq!(entry.job().jid(), Some("x"));
        assert_eq!(
            entry
                .run_at()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            1_700_000_000
        );
        let broken = work("not json");
        assert_eq!(broken.payload(), "");
        assert_eq!(broken.run_at(), std::time::UNIX_EPOCH);
    }
}
