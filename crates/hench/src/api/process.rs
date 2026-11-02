//! [`ProcessSet`] and [`Process`]: the hench processes alive in a Redis.

use std::fmt;
use std::time::SystemTime;

use serde::Deserialize;
use serde::de::{Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Value};

use crate::api::queue::scan_set;
use crate::api::{Api, parse_count, process_time_secs};
use crate::error::Result;
use crate::keys;
use crate::payload::secs_to_system_time;
use crate::redis::RedisPool;

/// The heartbeat fields read for each process, in this order.
const FIELDS: [&str; 7] = [
    "info",
    "concurrency",
    "busy",
    "beat",
    "quiet",
    "rss",
    "rtt_us",
];

/// One process's heartbeat hash, as returned by `HMGET` with [`FIELDS`].
type HeartbeatFields = (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

/// The live hench processes (Sidekiq's `ProcessSet`): the `processes` set, joined with each
/// process's heartbeat hash. Each process refreshes its heartbeat every few seconds and it
/// expires after a minute, so processes that died without cleaning up disappear from
/// [`list`](Self::list) quickly (and from the set itself with [`cleanup`](Self::cleanup)).
#[derive(Clone)]
pub struct ProcessSet {
    api: Api,
}

impl ProcessSet {
    /// The processes of `api`'s Redis.
    pub fn new(api: impl Into<Api>) -> Self {
        Self { api: api.into() }
    }

    /// Removes processes whose heartbeat expired from the `processes` set and returns how
    /// many were removed. Runs at most once a minute across all callers and processes (it
    /// takes the 60 s `process_cleanup` lock, shared with the workers' schedulers, which try
    /// the same cleanup on every poll); returns `0` otherwise.
    pub async fn cleanup(&self) -> Result<u64> {
        cleanup_processes(self.api.redis()).await
    }

    /// Every live process, sorted by identity. Processes in the set whose heartbeat expired
    /// are skipped. One pipelined round trip for all heartbeats.
    pub async fn list(&self) -> Result<Vec<Process>> {
        let identities = identities(&self.api).await?;
        if identities.is_empty() {
            return Ok(Vec::new());
        }
        let mut pipe = ::redis::pipe();
        for identity in &identities {
            pipe.cmd("HMGET").arg(keys::process(identity)).arg(&FIELDS);
        }
        let pipe = &pipe;
        let heartbeats: Vec<HeartbeatFields> = self
            .api
            .redis()
            .with_retry(|mut conn| async move { pipe.query_async(&mut conn).await })
            .await?;
        Ok(identities
            .into_iter()
            .zip(heartbeats)
            .filter_map(|(identity, fields)| Process::from_heartbeat(&self.api, identity, fields))
            .collect())
    }

    /// The live process with this identity, if any (Sidekiq's `ProcessSet[identity]`).
    pub async fn get(&self, identity: &str) -> Result<Option<Process>> {
        let mut pipe = ::redis::pipe();
        pipe.atomic()
            .cmd("SISMEMBER")
            .arg(keys::PROCESSES)
            .arg(identity)
            .cmd("HMGET")
            .arg(keys::process(identity))
            .arg(&FIELDS);
        let pipe = &pipe;
        let (member, fields): (bool, HeartbeatFields) = self
            .api
            .redis()
            .with_retry(|mut conn| async move { pipe.query_async(&mut conn).await })
            .await?;
        if !member {
            return Ok(None);
        }
        Ok(Process::from_heartbeat(
            &self.api,
            identity.to_owned(),
            fields,
        ))
    }

    /// The number of processes in the `processes` set (including any that died without
    /// cleaning up and were not pruned yet, which running workers do about once a minute;
    /// [`Stats::processes_size`](crate::api::Stats::processes_size) counts live ones only).
    pub async fn size(&self) -> Result<u64> {
        self.api
            .redis()
            .with_retry(|mut conn| async move {
                ::redis::cmd("SCARD")
                    .arg(keys::PROCESSES)
                    .query_async(&mut conn)
                    .await
            })
            .await
    }

    /// The total concurrency (job slots) of the live processes.
    pub async fn total_concurrency(&self) -> Result<u64> {
        Ok(self.list().await?.iter().map(Process::concurrency).sum())
    }

    /// The total memory (RSS, in KB) of the live processes.
    pub async fn total_rss(&self) -> Result<u64> {
        Ok(self.list().await?.iter().map(Process::rss_kb).sum())
    }
}

impl fmt::Debug for ProcessSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProcessSet")
            .field("redis", self.api.redis())
            .finish()
    }
}

/// Members of the `processes` set, sorted and deduplicated.
pub(crate) async fn identities(api: &Api) -> Result<Vec<String>> {
    scan_set(api.redis(), keys::PROCESSES).await
}

/// The number of members of the `processes` set that are alive: whose heartbeat hash still has
/// its `info` field (like [`ProcessSet::list`]). Processes that died stay in the set until a
/// cleanup prunes them; counting the set would count them too.
pub(crate) async fn live_process_count(redis: &RedisPool) -> Result<u64> {
    let identities = scan_set(redis, keys::PROCESSES).await?;
    if identities.is_empty() {
        return Ok(0);
    }
    let mut pipe = ::redis::pipe();
    for identity in &identities {
        pipe.cmd("HEXISTS").arg(keys::process(identity)).arg("info");
    }
    let pipe = &pipe;
    let alive: Vec<bool> = redis
        .with_retry(|mut conn| async move { pipe.query_async(&mut conn).await })
        .await?;
    Ok(alive.into_iter().filter(|alive| *alive).count() as u64)
}

/// Removes the processes whose heartbeat expired (no `info` field left) from the `processes`
/// set, at most once a minute across the cluster: the caller that takes the `process_cleanup`
/// lock (`SET NX EX 60`) does the work, the others return `0`. Returns how many were removed.
/// Used by [`ProcessSet::cleanup`] and by the workers' scheduler (on every poll).
pub(crate) async fn cleanup_processes(redis: &RedisPool) -> Result<u64> {
    // Sent once: repeated after a dropped connection, `SET NX` would find its own lock and
    // skip the cleanup.
    let mut conn = redis.conn().await?;
    let locked: Option<String> = ::redis::cmd("SET")
        .arg(keys::PROCESS_CLEANUP)
        .arg("1")
        .arg("NX")
        .arg("EX")
        .arg(keys::PROCESS_TTL.as_secs())
        .query_async(&mut conn)
        .await?;
    if locked.is_none() {
        return Ok(0);
    }
    let identities = scan_set(redis, keys::PROCESSES).await?;
    if identities.is_empty() {
        return Ok(0);
    }
    let mut pipe = ::redis::pipe();
    for identity in &identities {
        pipe.cmd("HGET").arg(keys::process(identity)).arg("info");
    }
    let pipe = &pipe;
    let infos: Vec<Option<String>> = redis
        .with_retry(|mut conn| async move { pipe.query_async(&mut conn).await })
        .await?;
    let dead: Vec<&String> = identities
        .iter()
        .zip(&infos)
        .filter(|(_, info)| info.is_none())
        .map(|(identity, _)| identity)
        .collect();
    if dead.is_empty() {
        return Ok(0);
    }
    let dead = &dead;
    redis
        .with_retry(|mut conn| async move {
            ::redis::cmd("SREM")
                .arg(keys::PROCESSES)
                .arg(dead)
                .query_async(&mut conn)
                .await
        })
        .await
}

/// The settings of one capsule of a process: its concurrency, queue mode and queues with
/// their weights, as reported in the heartbeat.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct CapsuleInfo {
    /// The capsule's name (`default` for the main one).
    pub name: String,
    /// The number of jobs it runs at a time.
    pub concurrency: u64,
    /// `strict`, `random` or `weighted`.
    pub mode: String,
    /// Its queues and their weights.
    pub weights: Vec<(String, u64)>,
}

/// A live process (Sidekiq's `Process`): its heartbeat data and its self-description (the
/// `info` JSON: hostname, pid, tag, labels, capsules, version, ...).
#[derive(Clone)]
pub struct Process {
    api: Api,
    identity: String,
    info: Map<String, Value>,
    capsules: Vec<CapsuleInfo>,
    concurrency: u64,
    busy: u64,
    beat: Option<f64>,
    quiet: bool,
    rss_kb: u64,
    rtt_us: u64,
}

impl Process {
    /// Builds a process from its heartbeat fields; `None` if the heartbeat expired.
    fn from_heartbeat(api: &Api, identity: String, fields: HeartbeatFields) -> Option<Self> {
        let (info, concurrency, busy, beat, quiet, rss, rtt) = fields;
        let info_json = info?;
        let info: Map<String, Value> = match serde_json::from_str(&info_json) {
            Ok(info) => info,
            Err(error) => {
                tracing::warn!(identity, %error, "ignoring a malformed process description");
                Map::new()
            }
        };
        let capsules = serde_json::from_str::<CapsulesOnly>(&info_json)
            .map(|parsed| parsed.capsules.0)
            .unwrap_or_default();
        let concurrency = match concurrency {
            Some(concurrency) => parse_count(Some(&concurrency)),
            None => info.get("concurrency").and_then(Value::as_u64).unwrap_or(0),
        };
        Some(Self {
            api: api.clone(),
            identity,
            info,
            capsules,
            concurrency,
            busy: parse_count(busy.as_deref()),
            beat: beat
                .and_then(|beat| beat.trim().parse::<f64>().ok())
                .filter(|beat| beat.is_finite()),
            quiet: quiet.as_deref() == Some("true"),
            rss_kb: parse_count(rss.as_deref()),
            rtt_us: parse_count(rtt.as_deref()),
        })
    }

    /// The process identity, `hostname:pid:nonce`.
    pub fn identity(&self) -> &str {
        &self.identity
    }

    /// A field of the process description (`info`).
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.info.get(key)
    }

    /// The whole process description.
    pub fn info(&self) -> &Map<String, Value> {
        &self.info
    }

    fn string(&self, key: &str) -> Option<&str> {
        self.info.get(key).and_then(Value::as_str)
    }

    /// The host name.
    pub fn hostname(&self) -> Option<&str> {
        self.string("hostname")
    }

    /// The operating system process id.
    pub fn pid(&self) -> Option<u64> {
        self.info.get("pid").and_then(Value::as_u64)
    }

    /// The tag (an application name; empty when unset).
    pub fn tag(&self) -> Option<&str> {
        self.string("tag").filter(|tag| !tag.is_empty())
    }

    /// The labels.
    pub fn labels(&self) -> Vec<&str> {
        string_array(self.info.get("labels"))
    }

    /// The hench version the process runs.
    pub fn version(&self) -> Option<&str> {
        self.string("version")
    }

    /// `true` if the process runs embedded in another server rather than standalone.
    pub fn is_embedded(&self) -> bool {
        self.info.get("embedded").and_then(Value::as_bool) == Some(true)
    }

    /// When the process started.
    pub fn started_at(&self) -> Option<SystemTime> {
        let started = self.info.get("started_at").and_then(process_time_secs)?;
        Some(secs_to_system_time(started))
    }

    /// The capsules, in the order the process reported them.
    pub fn capsules(&self) -> &[CapsuleInfo] {
        &self.capsules
    }

    /// The queues the process works on, without duplicates, in its configured order when it
    /// reports one.
    pub fn queues(&self) -> Vec<String> {
        let listed = string_array(self.info.get("queues"));
        let names: Vec<&str> = if listed.is_empty() {
            self.capsules
                .iter()
                .flat_map(|capsule| capsule.weights.iter().map(|(queue, _)| queue.as_str()))
                .collect()
        } else {
            listed
        };
        let mut queues: Vec<String> = Vec::with_capacity(names.len());
        for name in names {
            if !queues.iter().any(|queue| queue == name) {
                queues.push(name.to_owned());
            }
        }
        queues
    }

    /// Each queue's weight across the capsules (a queue in several capsules has the weight of
    /// the last one).
    pub fn weights(&self) -> Vec<(String, u64)> {
        let mut weights: Vec<(String, u64)> = Vec::new();
        for capsule in &self.capsules {
            for (queue, weight) in &capsule.weights {
                match weights.iter_mut().find(|(name, _)| name == queue) {
                    Some(existing) => existing.1 = *weight,
                    None => weights.push((queue.clone(), *weight)),
                }
            }
        }
        weights
    }

    /// The process's total concurrency (job slots).
    pub fn concurrency(&self) -> u64 {
        self.concurrency
    }

    /// How many jobs the process is running.
    pub fn busy(&self) -> u64 {
        self.busy
    }

    /// When the process last sent a heartbeat.
    pub fn beat(&self) -> Option<SystemTime> {
        self.beat.map(secs_to_system_time)
    }

    /// The process's memory use (RSS) in KB.
    pub fn rss_kb(&self) -> u64 {
        self.rss_kb
    }

    /// The round-trip time to Redis measured by the process, in microseconds.
    pub fn rtt_us(&self) -> u64 {
        self.rtt_us
    }

    /// `true` once the process is quiet or shutting down (it no longer fetches jobs).
    pub fn is_stopping(&self) -> bool {
        self.quiet
    }

    /// Asks the process to stop fetching new jobs (it finishes the running ones). The process
    /// picks the signal up on its next heartbeat, within seconds. Works for embedded
    /// processes too.
    pub async fn quiet(&self) -> Result<()> {
        self.signal(keys::SIGNAL_QUIET).await
    }

    /// Asks the process to shut down (within its shutdown timeout). The process picks the
    /// signal up on its next heartbeat, within seconds. Works for embedded processes too.
    pub async fn stop(&self) -> Result<()> {
        self.signal(keys::SIGNAL_STOP).await
    }

    /// Asks the process to log the jobs it is running: its identity, how many slots are busy
    /// and, for each, the job's jid, class and queue and how long it has run (Sidekiq's
    /// `dump_threads`; the process does the same on `SIGTTIN`). The process picks the signal up
    /// on its next heartbeat, within seconds, and goes on working.
    pub async fn dump(&self) -> Result<()> {
        self.signal(keys::SIGNAL_DUMP).await
    }

    /// Pushes a signal on the process's signal list, which expires after a minute so that a
    /// signal for a dead process does not linger.
    async fn signal(&self, signal: &str) -> Result<()> {
        let key = keys::signals(&self.identity);
        let mut pipe = ::redis::pipe();
        pipe.atomic()
            .cmd("LPUSH")
            .arg(&key)
            .arg(signal)
            .ignore()
            .cmd("EXPIRE")
            .arg(&key)
            .arg(keys::PROCESS_TTL.as_secs())
            .ignore();
        let pipe = &pipe;
        self.api
            .redis()
            .with_retry(|mut conn| async move { pipe.exec_async(&mut conn).await })
            .await
    }
}

impl fmt::Debug for Process {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Process")
            .field("identity", &self.identity)
            .field("busy", &self.busy)
            .field("concurrency", &self.concurrency)
            .field("quiet", &self.quiet)
            .finish_non_exhaustive()
    }
}

fn string_array(value: Option<&Value>) -> Vec<&str> {
    match value {
        Some(Value::Array(values)) => values.iter().filter_map(Value::as_str).collect(),
        _ => Vec::new(),
    }
}

/// Just the `capsules` of a process description, keeping the order of the JSON objects.
#[derive(Deserialize)]
struct CapsulesOnly {
    #[serde(default)]
    capsules: OrderedCapsules,
}

#[derive(Default)]
struct OrderedCapsules(Vec<CapsuleInfo>);

impl<'de> Deserialize<'de> for OrderedCapsules {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let entries = deserializer.deserialize_any(OrderedEntries::<CapsuleFields>::new())?;
        Ok(Self(
            entries
                .into_iter()
                .map(|(name, fields)| CapsuleInfo {
                    name,
                    concurrency: fields.concurrency,
                    mode: fields.mode,
                    weights: fields.weights.0,
                })
                .collect(),
        ))
    }
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct CapsuleFields {
    concurrency: u64,
    mode: String,
    weights: OrderedWeights,
}

#[derive(Default)]
struct OrderedWeights(Vec<(String, u64)>);

impl<'de> Deserialize<'de> for OrderedWeights {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let entries = deserializer.deserialize_any(OrderedEntries::<Value>::new())?;
        Ok(Self(
            entries
                .into_iter()
                .map(|(queue, weight)| (queue, weight.as_u64().unwrap_or(0)))
                .collect(),
        ))
    }
}

/// Visits a JSON object (or an array of `[key, value]` pairs) into a list, keeping the order.
struct OrderedEntries<T>(std::marker::PhantomData<T>);

impl<T> OrderedEntries<T> {
    fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<'de, T: Deserialize<'de>> Visitor<'de> for OrderedEntries<T> {
    type Value = Vec<(String, T)>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("an object or a list of [key, value] pairs")
    }

    fn visit_map<A: MapAccess<'de>>(
        self,
        mut map: A,
    ) -> std::result::Result<Self::Value, A::Error> {
        let mut entries = Vec::with_capacity(map.size_hint().unwrap_or(0));
        while let Some(entry) = map.next_entry::<String, T>()? {
            entries.push(entry);
        }
        Ok(entries)
    }

    fn visit_seq<A: SeqAccess<'de>>(
        self,
        mut seq: A,
    ) -> std::result::Result<Self::Value, A::Error> {
        let mut entries = Vec::with_capacity(seq.size_hint().unwrap_or(0));
        while let Some(entry) = seq.next_element::<(String, T)>()? {
            entries.push(entry);
        }
        Ok(entries)
    }

    fn visit_unit<E>(self) -> std::result::Result<Self::Value, E> {
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::redis::RedisPool;
    use serde_json::json;

    fn process(info: &str, fields: [Option<&str>; 6]) -> Process {
        let [concurrency, busy, beat, quiet, rss, rtt] = fields.map(|f| f.map(str::to_owned));
        Process::from_heartbeat(
            &Api::new(RedisPool::detached()),
            "host:1:abc".to_owned(),
            (
                Some(info.to_owned()),
                concurrency,
                busy,
                beat,
                quiet,
                rss,
                rtt,
            ),
        )
        .unwrap()
    }

    #[test]
    fn reads_heartbeat_fields() {
        // Written by hand: `json!` would sort the keys, and the order matters here.
        let info = r#"{
            "hostname": "host", "pid": 1, "tag": "app", "labels": ["a", "b"],
            "started_at": 1700000000.5, "version": "0.1.0", "embedded": false,
            "capsules": {
                "default": {"concurrency": 5, "mode": "weighted", "weights": {"foo": 1, "bar": 1}},
                "single": {"concurrency": 1, "mode": "strict", "weights": {"single": 0}}
            }
        }"#;
        let process = process(
            info,
            [
                Some("7"),
                Some("3"),
                Some("1700000001.25"),
                Some("true"),
                Some("1024"),
                Some("150"),
            ],
        );
        assert_eq!(process.identity(), "host:1:abc");
        assert_eq!(process.hostname(), Some("host"));
        assert_eq!(process.pid(), Some(1));
        assert_eq!(process.tag(), Some("app"));
        assert_eq!(process.labels(), ["a", "b"]);
        assert_eq!(process.version(), Some("0.1.0"));
        assert!(!process.is_embedded());
        assert!(process.started_at().is_some());
        assert_eq!(process.concurrency(), 7);
        assert_eq!(process.busy(), 3);
        assert!(process.beat().is_some());
        assert!(process.is_stopping());
        assert_eq!(process.rss_kb(), 1024);
        assert_eq!(process.rtt_us(), 150);
        // Without a top-level `queues` list, capsule order then weight order (as written).
        assert_eq!(process.queues(), ["foo", "bar", "single"]);
        assert_eq!(
            process.weights(),
            [
                ("foo".to_owned(), 1),
                ("bar".to_owned(), 1),
                ("single".to_owned(), 0)
            ]
        );
        assert_eq!(process.capsules()[1].name, "single");
        assert_eq!(process.capsules()[0].mode, "weighted");
        assert_eq!(process.get("pid"), Some(&json!(1)));
        assert!(format!("{process:?}").contains("host:1:abc"));
    }

    #[test]
    fn prefers_the_ordered_queue_list() {
        let process = process(
            &json!({"queues": ["critical", "default", "critical"], "capsules": {"default": {"weights": {"default": 0, "critical": 0}}}}).to_string(),
            [None; 6],
        );
        assert_eq!(process.queues(), ["critical", "default"]);
        assert_eq!(process.concurrency(), 0);
        assert!(!process.is_stopping());
        assert_eq!(process.tag(), None);
    }

    #[test]
    fn tolerates_odd_descriptions() {
        let process = process(
            &json!({"capsules": [["default", {"weights": [["q", 2]]}]], "concurrency": 4, "tag": "", "started_at": 1_700_000_000_000_i64}).to_string(),
            [None, Some("x"), Some("nan"), Some("false"), None, None],
        );
        assert_eq!(process.queues(), ["q"]);
        assert_eq!(process.weights(), [("q".to_owned(), 2)]);
        assert_eq!(process.concurrency(), 4);
        assert_eq!(process.busy(), 0);
        assert!(process.beat().is_none());
        assert_eq!(process.tag(), None);
        let started = process.started_at().unwrap();
        assert_eq!(
            started
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            1_700_000_000
        );

        let malformed = Process::from_heartbeat(
            &Api::new(RedisPool::detached()),
            "x".to_owned(),
            (
                Some("not json".to_owned()),
                None,
                None,
                None,
                None,
                None,
                None,
            ),
        )
        .unwrap();
        assert!(malformed.info().is_empty());
        assert!(malformed.capsules().is_empty());
        assert!(
            Process::from_heartbeat(
                &Api::new(RedisPool::detached()),
                "gone".to_owned(),
                (None, None, None, None, None, None, None)
            )
            .is_none()
        );
    }
}
