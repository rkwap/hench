//! The periodic schedules ([`PeriodicSet`], from [`Api::periodic`]): listing them, reading one
//! with its history, creating dynamic ones, pausing, resuming, removing, enqueueing a schedule's
//! job now, and repairing the indexes (`docs/design/periodic.md` §2.3.4). Also
//! [`JobRecord::periodic`].
//!
//! Every schedule is the hash `periodic:<name>`, listed by the sorted set `periodic-names`
//! (every name, score 0) and fired from `periodic` (active schedules, scored by their next
//! tick). Reads are lenient: a schedule whose definition this process cannot read (written by a
//! newer version, an unknown time zone) still lists, with `error` saying why.

use crate::api::JobRecord;
use crate::periodic::{PeriodicMeta, PeriodicTick};

impl JobRecord {
    /// The periodic schedule and tick that enqueued this job, read leniently from its `periodic`
    /// field: `None` when no schedule enqueued it or the field is malformed.
    pub fn periodic(&self) -> Option<PeriodicTick> {
        let value = self.get("periodic")?;
        let meta = serde_json::from_value::<PeriodicMeta>(value.clone()).ok()?;
        Some(PeriodicTick::from(&meta))
    }
}

#[cfg(feature = "periodic")]
pub use self::set::{
    CrontabGroup, HistoryEntry, LastEnqueue, LastRun, PeriodicEntry, PeriodicSet, PeriodicSummary,
};

#[cfg(feature = "periodic")]
mod set {
    use std::collections::{BTreeMap, HashMap};
    use std::time::{Duration, SystemTime};

    use futures::Stream;
    use serde_json::Value;

    use crate::api::{Api, Page, batched};
    use crate::error::{Error, Result};
    use crate::keys;
    use crate::payload::{JobPayload, generate_jid, millis_to_system_time};
    use crate::periodic::clock::Clock;
    use crate::periodic::definition::{
        Unreadable, compile, parse_policy, parse_spec, parse_template,
    };
    use crate::periodic::poller::{encode, events_key, history_entry};
    use crate::periodic::store::{
        self, EnqueueNow, Enqueued, Fields, Fire, Fired, UpsertReply, Write,
    };
    use crate::periodic::{
        CatchUp, FireResult, Periodic, PeriodicMeta, PeriodicSource, RunStatus, Timetable,
        Upserted, tick_time,
    };
    use crate::processor::is_unavailable;

    /// The largest page served (like the API's other pages).
    const MAX_PAGE_SIZE: usize = 1_000;

    /// Schedules read per round trip by [`PeriodicSet::entries`] and [`PeriodicSet::repair`].
    const BATCH: usize = 100;

    /// Attempts of an upsert whose schedule changed between its read and its write.
    const UPSERT_ATTEMPTS: usize = 5;

    /// The history entries [`PeriodicEntry::history`] and [`PeriodicSet::history`] read at most.
    const MAX_HISTORY: usize = 1_000;

    /// The periodic schedules ([`Api::periodic`]).
    #[derive(Clone)]
    pub struct PeriodicSet {
        api: Api,
        clock: Clock,
        /// Entries kept in the histories it writes to ("enqueue now"): the configuration's
        /// `periodic_history` or [`with_history`](PeriodicSet::with_history)'s; `None` when
        /// neither is known (a history then keeps its length, see [`PeriodicSet::enqueue`]).
        history: Option<usize>,
    }

    impl Api {
        /// The periodic schedules: list, read, create (dynamic schedules), pause, resume,
        /// remove, enqueue now. "Enqueue now" keeps the configuration's `periodic_history`
        /// entries of a schedule's history; without a configuration, it keeps the history as
        /// long as it is (see [`PeriodicSet::enqueue`]).
        pub fn periodic(&self) -> PeriodicSet {
            PeriodicSet {
                api: self.clone(),
                clock: Clock::redis(),
                history: self.config().map(|config| config.periodic_history),
            }
        }
    }

    impl std::fmt::Debug for PeriodicSet {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("PeriodicSet").finish_non_exhaustive()
        }
    }

    /// The schedules at a glance ([`PeriodicSet::summary`]), read from the two indexes only, so
    /// it is cheap at any size.
    #[derive(Debug, Clone, PartialEq)]
    #[non_exhaustive]
    pub struct PeriodicSummary {
        /// Schedules (every name).
        pub total: u64,
        /// Schedules in the due index: those not paused.
        pub active: u64,
        /// Active schedules due for longer than the summary's `late_after`: no poller handled
        /// them (none runs, or they keep failing).
        pub late: u64,
        /// The schedule due first and when (for a schedule a poller is firing, the end of its
        /// lease).
        pub next: Option<(String, SystemTime)>,
    }

    impl PeriodicSummary {
        /// Schedules not in the due index: paused (or missing from it: see
        /// [`PeriodicSet::repair`]).
        pub fn paused(&self) -> u64 {
            self.total.saturating_sub(self.active)
        }
    }

    /// The last tick a schedule handled (`last_*` fields).
    #[derive(Debug, Clone, PartialEq)]
    #[non_exhaustive]
    pub struct LastEnqueue {
        /// The tick.
        pub tick: SystemTime,
        /// When it was handled (Redis's clock).
        pub at: SystemTime,
        /// What was done with it (`None`: a result this version does not know).
        pub result: Option<FireResult>,
        /// The jid of the job written, if one was.
        pub jid: Option<String>,
        /// The process that handled it.
        pub by: Option<String>,
    }

    impl LastEnqueue {
        /// How late the tick was handled.
        pub fn late(&self) -> Duration {
            self.at.duration_since(self.tick).unwrap_or_default()
        }
    }

    /// How the latest execution of a schedule's job ended (`run_*` fields).
    #[derive(Debug, Clone, PartialEq)]
    #[non_exhaustive]
    pub struct LastRun {
        /// The tick of the job (for an "enqueue now": when it was requested).
        pub tick: SystemTime,
        /// When the outcome was recorded.
        pub at: SystemTime,
        /// The outcome (`None`: one this version does not know).
        pub status: Option<RunStatus>,
        /// How long the execution took.
        pub duration: Duration,
        /// The error of a failed run.
        pub error: Option<String>,
        /// The job's jid.
        pub jid: Option<String>,
    }

    /// One entry of a schedule's history (the latest enqueues, newest first).
    #[derive(Debug, Clone, PartialEq)]
    #[non_exhaustive]
    pub struct HistoryEntry {
        /// The tick (`None` for "enqueue now" and for ticks a catch-up policy dropped).
        pub tick: Option<SystemTime>,
        /// When it was handled.
        pub at: SystemTime,
        /// What was done (`None`: a result this version does not know).
        pub result: Option<FireResult>,
        /// The jid of the job written, if one was.
        pub jid: Option<String>,
        /// The process that handled it (or who asked, for "enqueue now").
        pub by: Option<String>,
        /// Ticks dropped by the catch-up policy at that moment.
        pub missed: u64,
        /// How late the tick was handled.
        pub late: Option<Duration>,
    }

    /// A registered crontab: the code-defined schedules of a group, as its last registration
    /// declared them.
    #[derive(Debug, Clone, PartialEq)]
    #[non_exhaustive]
    pub struct CrontabGroup {
        /// The group.
        pub group: String,
        /// The crontab's digest (processes of the group declaring another one disagree with
        /// the registered crontab).
        pub digest: String,
        /// Its schedules.
        pub count: u64,
        /// When it was registered.
        pub at: Option<SystemTime>,
        /// The process that registered it.
        pub by: Option<String>,
        /// The definition format version of that process.
        pub version: Option<u64>,
    }

    /// A schedule as stored: its definition and state.
    #[derive(Debug, Clone)]
    #[non_exhaustive]
    pub struct PeriodicEntry {
        /// The name.
        pub name: String,
        /// Who defines it: code (with its group) or the API.
        pub source: PeriodicSource,
        /// The timetable (`None` when this process cannot read it: see `error`).
        pub timetable: Option<Timetable>,
        /// Free text.
        pub description: Option<String>,
        /// The job template (`None` when this process cannot read it).
        pub job: Option<JobPayload>,
        /// The catch-up policy.
        pub catch_up: CatchUp,
        /// The deadline, if any (`Skip`'s default included).
        pub deadline: Option<Duration>,
        /// The next tick (`None` while paused, or parked with an error).
        pub next: Option<SystemTime>,
        /// Since when it is paused.
        pub paused_at: Option<SystemTime>,
        /// The last tick handled.
        pub last: Option<LastEnqueue>,
        /// The latest execution's outcome.
        pub last_run: Option<LastRun>,
        /// The last "enqueue now".
        pub manual_at: Option<SystemTime>,
        /// The jid of the last "enqueue now".
        pub manual_jid: Option<String>,
        /// Ticks whose job was written.
        pub fired: u64,
        /// Ticks a catch-up policy dropped.
        pub missed: u64,
        /// Ticks consumed without a job (halted by client middleware, duplicates of a unique
        /// job, refused by the monotonic guard).
        pub skipped: u64,
        /// Failed executions.
        pub run_failures: u64,
        /// Why the schedule cannot fire, or why this process cannot read its definition.
        pub error: Option<String>,
        /// When `error` was recorded.
        pub error_at: Option<SystemTime>,
        /// When it was created.
        pub created_at: Option<SystemTime>,
        /// When its definition last changed.
        pub updated_at: Option<SystemTime>,
        /// The definition's digest.
        pub digest: String,
        /// The hash as stored, every field (for inspection).
        pub fields: BTreeMap<String, String>,
        set: PeriodicSet,
    }

    /// Epoch milliseconds as an instant.
    fn millis(value: Option<&String>) -> Option<SystemTime> {
        value?.parse::<i64>().ok().map(millis_to_system_time)
    }

    /// Epoch seconds as an instant.
    fn seconds(value: Option<&String>) -> Option<SystemTime> {
        value?.parse::<i64>().ok().map(tick_time)
    }

    fn count(value: Option<&String>) -> u64 {
        value.and_then(|value| value.parse().ok()).unwrap_or(0)
    }

    impl PeriodicEntry {
        /// The entry of the hash `fields` of schedule `name`, or `None` when the hash is gone.
        fn read(set: &PeriodicSet, name: &str, fields: Fields) -> Option<Self> {
            if fields.is_empty() {
                return None;
            }
            let get = |key: &str| fields.get(key);
            let source = match (get("source").map(String::as_str), get("group")) {
                (Some("code"), Some(group)) => PeriodicSource::Code {
                    group: group.clone(),
                },
                (Some("code"), None) => PeriodicSource::Code {
                    group: String::new(),
                },
                _ => PeriodicSource::Api,
            };
            let mut problems = Vec::new();
            let mut note = |result: std::result::Result<(), Unreadable>| {
                if let Err(unreadable) = result {
                    problems.push(unreadable.reason().to_owned());
                }
            };
            let timetable = parse_spec(get("spec").map(String::as_str).unwrap_or_default());
            let policy = parse_policy(get("policy").map(String::as_str).unwrap_or_default());
            let job = parse_template(get("job").map(String::as_str).unwrap_or_default());
            let timetable = match timetable {
                Ok(timetable) => {
                    note(
                        timetable
                            .timing()
                            .map(|_| ())
                            .map_err(|error| Unreadable::Invalid(error.0)),
                    );
                    Some(timetable)
                }
                Err(unreadable) => {
                    note(Err(unreadable));
                    None
                }
            };
            let (catch_up, deadline) = match policy {
                Ok(policy) => (policy.catch_up, policy.effective_deadline()),
                Err(unreadable) => {
                    note(Err(unreadable));
                    (CatchUp::default(), None)
                }
            };
            let job = match job {
                Ok(job) => Some(job),
                Err(unreadable) => {
                    note(Err(unreadable));
                    None
                }
            };
            let last = seconds(get("last_tick")).map(|tick| LastEnqueue {
                tick,
                at: millis(get("last_at")).unwrap_or(tick),
                result: get("last_result").and_then(|result| FireResult::parse(result)),
                jid: get("last_jid").cloned(),
                by: get("last_by").cloned(),
            });
            let last_run = millis(get("run_tick")).map(|tick| LastRun {
                tick,
                at: millis(get("run_at")).unwrap_or(tick),
                status: get("run_status").and_then(|status| RunStatus::parse(status)),
                duration: Duration::from_millis(count(get("run_ms"))),
                error: get("run_error").cloned(),
                jid: get("run_jid").cloned(),
            });
            let error = get("error")
                .cloned()
                .or_else(|| (!problems.is_empty()).then(|| problems.join("; ")));
            Some(Self {
                name: name.to_owned(),
                source,
                timetable,
                description: get("description").cloned(),
                job,
                catch_up,
                deadline,
                next: seconds(get("next")),
                paused_at: millis(get("paused")),
                last,
                last_run,
                manual_at: millis(get("manual_at")),
                manual_jid: get("manual_jid").cloned(),
                fired: count(get("fired")),
                missed: count(get("missed")),
                skipped: count(get("skipped")),
                run_failures: count(get("run_failures")),
                error_at: if get("error").is_some() {
                    millis(get("error_at"))
                } else {
                    None
                },
                error,
                created_at: millis(get("created_at")),
                updated_at: millis(get("updated_at")),
                digest: get("digest").cloned().unwrap_or_default(),
                fields: fields.into_iter().collect(),
                set: set.clone(),
            })
        }

        /// Whether it is paused.
        pub fn is_paused(&self) -> bool {
            self.paused_at.is_some()
        }

        /// The next `count` ticks, from the pending one (`next`) on: none while paused; an error
        /// for a timetable this process cannot read.
        pub fn upcoming(&self, count: usize) -> Result<Vec<SystemTime>> {
            if self.is_paused() {
                return Ok(Vec::new());
            }
            let timetable = self.timetable.as_ref().ok_or_else(|| {
                Error::InvalidSchedule(format!(
                    "`{}`: {}",
                    self.name,
                    self.error.as_deref().unwrap_or("unreadable timetable")
                ))
            })?;
            let after = self.next.map_or_else(SystemTime::now, |next| {
                next.checked_sub(Duration::from_secs(1)).unwrap_or(next)
            });
            timetable.upcoming(after, count)
        }

        /// The timetable in English, or why it cannot be read.
        pub fn describe(&self) -> String {
            match &self.timetable {
                Some(timetable) => timetable.describe(),
                None => format!(
                    "unreadable timetable: {}",
                    self.error.as_deref().unwrap_or("unknown format")
                ),
            }
        }

        /// The latest `count` history entries, newest first.
        pub async fn history(&self, count: usize) -> Result<Vec<HistoryEntry>> {
            self.set.history(&self.name, count).await
        }

        /// Pauses it (see [`PeriodicSet::pause`]).
        pub async fn pause(&self) -> Result<bool> {
            self.set.pause(&self.name).await
        }

        /// Resumes it (see [`PeriodicSet::unpause`]).
        pub async fn unpause(&self) -> Result<bool> {
            self.set.unpause(&self.name).await
        }

        /// Enqueues its job now (see [`PeriodicSet::enqueue`]).
        pub async fn enqueue(&self) -> Result<Option<String>> {
            self.set.enqueue(&self.name).await
        }
    }

    impl PeriodicSet {
        /// A copy reading the time from `clock` instead of Redis (tests).
        #[cfg(feature = "test-util")]
        #[must_use]
        pub fn with_clock(&self, clock: crate::periodic::ManualClock) -> Self {
            Self {
                clock: Clock::manual(clock),
                ..self.clone()
            }
        }

        /// A copy keeping `entries` in the histories it writes to (an "enqueue now" adds one; 0
        /// keeps none), like the workers' `periodic_history`.
        #[must_use]
        pub fn with_history(&self, entries: usize) -> Self {
            Self {
                history: Some(entries),
                ..self.clone()
            }
        }

        /// The entries "enqueue now" keeps in schedule `name`'s history when the workers'
        /// `periodic_history` is unknown: the history's length plus the new entry, at least the
        /// default (10) and at most the 1,000 entries readers show, never shrinking a longer
        /// history (workers configured to keep more trim it at their next enqueue).
        async fn history_kept(&self, name: &str) -> Result<usize> {
            if let Some(entries) = self.history {
                return Ok(entries);
            }
            let key = keys::periodic_history(name);
            let key = &key;
            let len: usize = self
                .redis()
                .with_retry(|mut conn| async move {
                    ::redis::cmd("LLEN").arg(key).query_async(&mut conn).await
                })
                .await?;
            Ok(len
                .saturating_add(1)
                .min(MAX_HISTORY)
                .max(len)
                .max(crate::config::Config::DEFAULT_PERIODIC_HISTORY))
        }

        fn redis(&self) -> &crate::redis::RedisPool {
            self.api.redis()
        }

        /// How many schedules exist.
        pub async fn size(&self) -> Result<u64> {
            self.redis()
                .with_retry(|mut conn| async move {
                    ::redis::cmd("ZCARD")
                        .arg(keys::PERIODIC_NAMES)
                        .query_async::<u64>(&mut conn)
                        .await
                })
                .await
        }

        /// The names from `start` to `stop` (inclusive) in name order.
        async fn names(&self, start: usize, stop: usize) -> Result<Vec<String>> {
            self.redis()
                .with_retry(|mut conn| async move {
                    ::redis::cmd("ZRANGE")
                        .arg(keys::PERIODIC_NAMES)
                        .arg(start)
                        .arg(stop)
                        .query_async::<Vec<String>>(&mut conn)
                        .await
                })
                .await
        }

        /// The hashes of `names`, in one round trip.
        async fn read_many(&self, names: &[String]) -> Result<Vec<Fields>> {
            if names.is_empty() {
                return Ok(Vec::new());
            }
            let mut pipe = ::redis::pipe();
            for name in names {
                pipe.cmd("HGETALL").arg(keys::periodic(name));
            }
            let pipe = &pipe;
            self.redis()
                .with_retry(
                    |mut conn| async move { pipe.query_async::<Vec<Fields>>(&mut conn).await },
                )
                .await
        }

        /// The schedules at a glance: how many exist, are active (not paused) and are late
        /// (due for longer than `late_after`: no poller handled them; the dashboard uses the
        /// default lease plus the default poll interval), and which is due first. Reads the two
        /// indexes only.
        pub async fn summary(&self, late_after: Duration) -> Result<PeriodicSummary> {
            let redis = self.redis();
            let now_ms = self.clock.now_ms(redis).await?;
            let late_after_ms = i64::try_from(late_after.as_millis()).unwrap_or(i64::MAX);
            // Scores are epoch seconds; due before the cut-off means late.
            let cutoff = now_ms.saturating_sub(late_after_ms) as f64 / 1_000.0;
            let mut pipe = ::redis::pipe();
            pipe.cmd("ZCARD")
                .arg(keys::PERIODIC_NAMES)
                .cmd("ZCARD")
                .arg(keys::PERIODIC)
                .cmd("ZCOUNT")
                .arg(keys::PERIODIC)
                .arg("-inf")
                .arg(format!("({cutoff}"))
                .cmd("ZRANGE")
                .arg(keys::PERIODIC)
                .arg(0)
                .arg(0)
                .arg("WITHSCORES");
            let pipe = &pipe;
            let (total, active, late, first): (u64, u64, u64, Vec<(String, f64)>) = redis
                .with_retry(|mut conn| async move { pipe.query_async(&mut conn).await })
                .await?;
            let next = first.into_iter().next().map(|(name, score)| {
                let millis = (score * 1_000.0).round();
                // Scores are whole seconds in practice; anything out of range reads as now.
                let millis = if millis.is_finite() && millis.abs() < 9.0e15 {
                    millis as i64
                } else {
                    now_ms
                };
                (name, millis_to_system_time(millis))
            });
            Ok(PeriodicSummary {
                total,
                active,
                late,
                next,
            })
        }

        /// A page of schedules sorted by name (`page` from 1; at most 1 000 per page).
        /// Names whose hash is gone are left out (see [`PeriodicSet::repair`]).
        pub async fn page(&self, page: usize, size: usize) -> Result<Page<PeriodicEntry>> {
            let size = size.clamp(1, MAX_PAGE_SIZE);
            let page = page.max(1);
            let total = self.size().await?;
            // A page number from a request can be anything: past the end, nothing to read (and
            // Redis refuses offsets beyond 64-bit integers).
            let start = (page - 1).saturating_mul(size);
            let names = if u64::try_from(start).map_or(true, |start| start >= total) {
                Vec::new()
            } else {
                self.names(start, start.saturating_add(size - 1)).await?
            };
            let fields = self.read_many(&names).await?;
            let items = names
                .iter()
                .zip(fields)
                .filter_map(|(name, fields)| PeriodicEntry::read(self, name, fields))
                .collect();
            Ok(Page {
                items,
                number: page,
                size,
                total,
            })
        }

        /// Every schedule, by name, 100 per round trip.
        pub fn entries(&self) -> impl Stream<Item = Result<PeriodicEntry>> + Send + 'static {
            let set = self.clone();
            batched(0_usize, move |start| {
                let set = set.clone();
                async move {
                    let names = set.names(start, start.saturating_add(BATCH - 1)).await?;
                    if names.is_empty() {
                        return Ok(None);
                    }
                    let next = start + names.len();
                    let fields = set.read_many(&names).await?;
                    let entries = names
                        .iter()
                        .zip(fields)
                        .filter_map(|(name, fields)| PeriodicEntry::read(&set, name, fields))
                        .collect();
                    Ok(Some((entries, next)))
                }
            })
        }

        /// The schedule `name`, if it exists.
        pub async fn get(&self, name: &str) -> Result<Option<PeriodicEntry>> {
            let fields = store::read(self.redis(), name).await?;
            Ok(PeriodicEntry::read(self, name, fields))
        }

        /// The latest `count` history entries of schedule `name` (at most 1 000), newest first.
        pub async fn history(&self, name: &str, count: usize) -> Result<Vec<HistoryEntry>> {
            if count == 0 {
                return Ok(Vec::new());
            }
            let key = keys::periodic_history(name);
            let key = key.as_str();
            let stop = count.min(MAX_HISTORY) - 1;
            let entries: Vec<String> = self
                .redis()
                .with_retry(|mut conn| async move {
                    ::redis::cmd("LRANGE")
                        .arg(key)
                        .arg(0)
                        .arg(stop)
                        .query_async::<Vec<String>>(&mut conn)
                        .await
                })
                .await?;
            Ok(entries.iter().filter_map(|entry| history(entry)).collect())
        }

        /// Creates or updates the dynamic schedule `periodic` (source API). Refuses a name a
        /// code-defined schedule uses ([`Error::InvalidSchedule`]). An identical definition
        /// changes nothing; a new timetable moves the next tick to the first tick of the new
        /// timetable after the last minute (and after the last tick handled); a paused schedule
        /// stays paused.
        pub async fn upsert(&self, periodic: &Periodic) -> Result<Upserted> {
            let definition = periodic.definition()?;
            let redis = self.redis();
            let key = keys::periodic(&definition.name);
            // Compare-and-set on the digest read (the next tick of a changed timetable depends on
            // the state read), with a jittered pause between attempts; under heavy contention a
            // last attempt writes whatever is there (the last upsert wins).
            for attempt in 0..=UPSERT_ATTEMPTS {
                if attempt > 0 {
                    let pause = fastrand::u64(0..(1_u64 << attempt.min(6)));
                    tokio::time::sleep(Duration::from_millis(pause)).await;
                }
                let key = key.as_str();
                let (digest, last_tick): (Option<String>, Option<i64>) = redis
                    .with_retry(|mut conn| async move {
                        ::redis::cmd("HMGET")
                            .arg(key)
                            .arg("digest")
                            .arg("last_tick")
                            .query_async(&mut conn)
                            .await
                    })
                    .await?;
                let now_ms = self.clock.now_ms(redis).await?;
                let now = now_ms.div_euclid(1_000);
                let invalid = |error: crate::periodic::timing::TimingError| {
                    Error::InvalidSchedule(format!("`{}`: {error}", definition.name))
                };
                let next_new = definition.timing.next_after(now).map_err(invalid)?;
                let after = (now - 60).max(last_tick.unwrap_or(i64::MIN));
                let next_changed = definition.timing.next_after(after).map_err(invalid)?;
                let expected = if attempt == UPSERT_ATTEMPTS {
                    "*".to_owned()
                } else {
                    digest.unwrap_or_default()
                };
                match store::upsert(
                    redis,
                    &definition,
                    &expected,
                    "api",
                    next_new,
                    next_changed,
                    now_ms,
                )
                .await?
                {
                    UpsertReply::Done(1) => return Ok(Upserted::Created),
                    UpsertReply::Done(2) => return Ok(Upserted::Updated),
                    UpsertReply::Done(3) => return Ok(Upserted::Rescheduled),
                    UpsertReply::Done(_) => return Ok(Upserted::Unchanged),
                    UpsertReply::OtherSource(source) => {
                        return Err(Error::InvalidSchedule(format!(
                            "`{}` is defined by {} and cannot be changed through the API",
                            definition.name,
                            if source == "code" {
                                "code"
                            } else {
                                "another source"
                            }
                        )));
                    }
                    // Changed between the read and the write: read again.
                    UpsertReply::Conflict(_) => continue,
                }
            }
            Err(Error::InvalidSchedule(format!(
                "`{}` kept changing during {UPSERT_ATTEMPTS} attempts to update it",
                definition.name
            )))
        }

        /// Removes the dynamic schedule `name` (with its history). `false` when there is none;
        /// [`Error::InvalidSchedule`] for a code-defined one (it would come back at its group's
        /// next registration: see [`PeriodicSet::remove_any`]).
        pub async fn remove(&self, name: &str) -> Result<bool> {
            self.remove_with(name, Some("api")).await
        }

        /// Removes any schedule. A code-defined one comes back at its group's next
        /// registration, unless its code no longer defines it.
        pub async fn remove_any(&self, name: &str) -> Result<bool> {
            self.remove_with(name, None).await
        }

        async fn remove_with(&self, name: &str, source: Option<&str>) -> Result<bool> {
            let redis = self.redis();
            for _ in 0..3 {
                let key = keys::periodic(name);
                let key = key.as_str();
                let group: Option<String> = redis
                    .with_retry(|mut conn| async move {
                        ::redis::cmd("HGET")
                            .arg(key)
                            .arg("group")
                            .query_async(&mut conn)
                            .await
                    })
                    .await?;
                match store::remove(redis, name, source, group.as_deref()).await? {
                    1 => return Ok(true),
                    0 => return Ok(false),
                    -2 => {
                        return Err(Error::InvalidSchedule(format!(
                            "`{name}` is defined in code: remove it from the code (or use \
                             `remove_any`, and it comes back at its group's next registration \
                             if the code still defines it)"
                        )));
                    }
                    // Its group changed meanwhile: read it again.
                    _ => continue,
                }
            }
            Err(Error::InvalidSchedule(format!(
                "`{name}` kept changing during 3 attempts to remove it"
            )))
        }

        /// Pauses schedule `name`: it leaves the due index; no tick is counted as missed while
        /// it is paused. `true` when it was running; `false` when already paused or missing.
        pub async fn pause(&self, name: &str) -> Result<bool> {
            let now_ms = self.clock.now_ms(self.redis()).await?;
            Ok(store::pause(self.redis(), name, now_ms).await? == 1)
        }

        /// Resumes schedule `name` at its first tick after now (no backfill). `true` when it was
        /// paused; `false` when it was not, or is missing.
        pub async fn unpause(&self, name: &str) -> Result<bool> {
            let redis = self.redis();
            for _ in 0..UPSERT_ATTEMPTS {
                let fields = store::read(redis, name).await?;
                if fields.is_empty() || !fields.contains_key("paused") {
                    return Ok(false);
                }
                let get = |key: &str| fields.get(key).map(String::as_str).unwrap_or_default();
                let compiled = compile(get("spec"), get("policy"), get("job"), get("digest"))
                    .map_err(|unreadable| {
                        Error::InvalidSchedule(format!("`{name}`: {}", unreadable.reason()))
                    })?;
                let now_ms = self.clock.now_ms(redis).await?;
                let next = compiled
                    .timing
                    .next_after(now_ms.div_euclid(1_000))
                    .map_err(|error| Error::InvalidSchedule(format!("`{name}`: {error}")))?;
                match store::unpause(redis, name, next, &compiled.digest).await? {
                    1 => return Ok(true),
                    -1 => continue,
                    _ => return Ok(false),
                }
            }
            Err(Error::InvalidSchedule(format!(
                "`{name}` kept changing during {UPSERT_ATTEMPTS} attempts to resume it"
            )))
        }

        /// Pushes schedule `name`'s job now ("enqueue now": `periodic.manual` set, its tick the
        /// time of the request), through the API's client (client middleware runs). The
        /// timetable is untouched. Returns the jid, or `None` when middleware stopped the push or
        /// the job is unique and its lock is held. [`Error::InvalidSchedule`] when there is no
        /// such schedule or its definition cannot be read.
        ///
        /// The schedule's history gains an entry and keeps the configuration's
        /// `periodic_history` entries (or [`with_history`](Self::with_history)'s). An `Api`
        /// without a configuration (the dashboard, the JSON API, the CLI) does not know the
        /// workers' number: it keeps the history as long as it is, plus the new entry (at least
        /// 10, and at most 1,000 unless it is already longer).
        pub async fn enqueue(&self, name: &str) -> Result<Option<String>> {
            let redis = self.redis();
            let fields = store::read(redis, name).await?;
            if fields.is_empty() {
                return Err(Error::InvalidSchedule(format!(
                    "`{name}`: no such periodic job"
                )));
            }
            let template = parse_template(
                fields.get("job").map(String::as_str).unwrap_or_default(),
            )
            .map_err(|unreadable| {
                Error::InvalidSchedule(format!("`{name}`: {}", unreadable.reason()))
            })?;
            let now_ms = self.clock.now_ms(redis).await?;
            let mut job = template;
            job.jid = generate_jid();
            job.periodic = Some(PeriodicMeta::new(name, now_ms, true));
            let prepared = self.api.client().prepare_push(job).await?;
            let (write, lock, jid) =
                encode(prepared)?.unwrap_or((Write::Halted, None, String::new()));
            let identity = format!("api:{}", hostname_or_api());
            let result = if jid.is_empty() { "halted" } else { "manual" };
            let written = (!jid.is_empty()).then_some(jid.as_str());
            let entry = history_entry(&identity, None, result, written, 0, None, now_ms);
            let duplicate_entry =
                history_entry(&identity, None, "duplicate", None, 0, None, now_ms);
            let unique = lock.is_some();
            let history = self.history_kept(name).await?;
            let enqueue = EnqueueNow {
                name,
                write,
                lock,
                entry,
                duplicate_entry,
                history,
                now_ms,
                jid: &jid,
            };
            let enqueued = match store::enqueue_now(redis, &enqueue, &events_key()).await {
                Ok(enqueued) => enqueued,
                Err(error) if is_unavailable(&error) && !unique && !jid.is_empty() => {
                    // Sent once: did it run before the reply was lost?
                    let fields = store::read(redis, name).await?;
                    if fields.get("manual_jid") == Some(&jid) {
                        Enqueued::Done { without_lock: None }
                    } else {
                        return Err(error);
                    }
                }
                Err(error) => return Err(error),
            };
            Ok(match enqueued {
                Enqueued::Done { without_lock } => {
                    if let Some(error) = without_lock {
                        tracing::warn!(schedule = name, %jid, %error, "a periodic job was pushed without its unique lock");
                    }
                    (!jid.is_empty()).then_some(jid)
                }
                Enqueued::Already => Some(jid),
                Enqueued::Duplicate { holder } => {
                    tracing::debug!(schedule = name, %holder, "enqueue now: the job's unique lock is held");
                    None
                }
                Enqueued::Missing => {
                    return Err(Error::InvalidSchedule(format!(
                        "`{name}`: no such periodic job"
                    )));
                }
            })
        }

        /// The registered crontabs, by group.
        pub async fn groups(&self) -> Result<Vec<CrontabGroup>> {
            let records: HashMap<String, String> = self
                .redis()
                .with_retry(|mut conn| async move {
                    ::redis::cmd("HGETALL")
                        .arg(keys::PERIODIC_CRONTABS)
                        .query_async(&mut conn)
                        .await
                })
                .await?;
            let mut groups: Vec<CrontabGroup> = records
                .into_iter()
                .map(|(group, record)| {
                    let record: Value = serde_json::from_str(&record).unwrap_or(Value::Null);
                    CrontabGroup {
                        group,
                        digest: record["digest"].as_str().unwrap_or_default().to_owned(),
                        count: record["count"].as_u64().unwrap_or(0),
                        at: record["at"].as_i64().map(millis_to_system_time),
                        by: record["by"].as_str().map(str::to_owned),
                        version: record["version"].as_u64(),
                    }
                })
                .collect();
            groups.sort_by(|a, b| a.group.cmp(&b.group));
            Ok(groups)
        }

        /// Repairs the indexes: re-indexes active schedules whose due-index entry or `next` is
        /// missing (a lease in progress is kept), removes paused schedules from the due index,
        /// and forgets names whose hash is gone (also in the groups). Returns the repairs.
        /// Registration does the same for code-defined schedules.
        pub async fn repair(&self) -> Result<u64> {
            let redis = self.redis();
            let mut repairs = 0_u64;
            let mut start = 0_usize;
            loop {
                let names = self.names(start, start.saturating_add(BATCH - 1)).await?;
                if names.is_empty() {
                    break;
                }
                start += names.len();
                let fields = self.read_many(&names).await?;
                let mut pipe = ::redis::pipe();
                for name in &names {
                    pipe.cmd("ZSCORE").arg(keys::PERIODIC).arg(name);
                }
                let pipe = &pipe;
                let scores: Vec<Option<f64>> = redis
                    .with_retry(|mut conn| async move { pipe.query_async(&mut conn).await })
                    .await?;
                let now_ms = self.clock.now_ms(redis).await?;
                for ((name, fields), score) in names.iter().zip(fields).zip(scores) {
                    if fields.is_empty() {
                        repairs += u64::from(self.forget(name).await?);
                        continue;
                    }
                    if fields.contains_key("paused") {
                        if score.is_some() && store::unindex(redis, name).await? {
                            repairs += 1;
                        }
                        continue;
                    }
                    let next = fields.get("next").and_then(|next| next.parse::<i64>().ok());
                    if next.is_some() && score.is_some() {
                        continue;
                    }
                    let get = |key: &str| fields.get(key).map(String::as_str).unwrap_or_default();
                    let Ok(compiled) =
                        compile(get("spec"), get("policy"), get("job"), get("digest"))
                    else {
                        continue;
                    };
                    let new_next = match next {
                        Some(next) => Some(next),
                        None => compiled.timing.next_after(now_ms.div_euclid(1_000)).ok(),
                    };
                    let fire = Fire {
                        name,
                        expected_next: fields.get("next").map(String::as_str).unwrap_or_default(),
                        digest: &compiled.digest,
                        new_next,
                        tick: None,
                        write: Write::Skipped,
                        lock: None,
                        missed: 0,
                        entry: String::new(),
                        duplicate_entry: String::new(),
                        // No entry is written: any positive number leaves the history as it is.
                        history: self
                            .history
                            .unwrap_or(crate::config::Config::DEFAULT_PERIODIC_HISTORY),
                        jid: "",
                        identity: "api",
                        now_ms,
                    };
                    if let Fired::Done { .. } = store::fire(redis, &fire, &events_key()).await? {
                        repairs += 1;
                    }
                }
            }
            for group in self.groups().await? {
                let key = keys::periodic_group(&group.group);
                let key = key.as_str();
                let members: Vec<String> = redis
                    .with_retry(|mut conn| async move {
                        ::redis::cmd("SMEMBERS")
                            .arg(key)
                            .query_async(&mut conn)
                            .await
                    })
                    .await?;
                for member in members {
                    let hash = keys::periodic(&member);
                    let hash = hash.as_str();
                    let exists: bool = redis
                        .with_retry(|mut conn| async move {
                            ::redis::cmd("EXISTS")
                                .arg(hash)
                                .query_async(&mut conn)
                                .await
                        })
                        .await?;
                    if !exists {
                        let member = member.as_str();
                        let _: () = redis
                            .with_retry(|mut conn| async move {
                                ::redis::cmd("SREM")
                                    .arg(key)
                                    .arg(member)
                                    .query_async(&mut conn)
                                    .await
                            })
                            .await?;
                        repairs += 1;
                    }
                }
            }
            Ok(repairs)
        }

        /// Removes the index entries of a name whose hash is gone; `true` if there was one.
        async fn forget(&self, name: &str) -> Result<bool> {
            Ok(store::remove(self.redis(), name, None, None).await? == 0)
        }
    }

    /// The identity recorded for API actions: the host, so that operators see where an
    /// "enqueue now" came from.
    fn hostname_or_api() -> String {
        hostname::get()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|_| "unknown".to_owned())
    }

    /// A history entry, read leniently (`None` when it is not an object with a time).
    fn history(entry: &str) -> Option<HistoryEntry> {
        let value: Value = serde_json::from_str(entry).ok()?;
        let at = value["at"].as_i64().map(millis_to_system_time)?;
        Some(HistoryEntry {
            tick: value["tick"].as_i64().map(tick_time),
            at,
            result: value["result"].as_str().and_then(FireResult::parse),
            jid: value["jid"].as_str().map(str::to_owned),
            by: value["by"].as_str().map(str::to_owned),
            missed: value["missed"].as_u64().unwrap_or(0),
            late: value["late_ms"].as_u64().map(Duration::from_millis),
        })
    }
}
