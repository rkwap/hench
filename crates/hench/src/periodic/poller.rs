//! The periodic poller: registration of this process's crontab, the lease of due schedules and
//! the fire procedure (`docs/design/periodic.md` §2.6, §2.7). It runs until the shutdown starts
//! (not until quiet: time-driven enqueuing continues during rolling deploys).
//!
//! Every process polls; no leader. A poll leases the earliest due schedule (`PERIODIC_LEASE`
//! moves its score a lease ahead, out of the other pollers' reach), reads it, plans the catch-up,
//! builds the tick's job through the client (normalization, client middleware), and sends one
//! `PERIODIC_FIRE`, which pushes the job and advances the schedule only if its `next` still holds
//! the tick read. A tick is therefore enqueued at most once, whatever crashes, lost replies or
//! concurrent pollers do; the lease makes it at least once as long as a poller runs.

use std::sync::Arc;

use futures::future::BoxFuture;

use crate::launcher::Engine;

/// The poller's task, or `None` when this process neither polls nor registers schedules
/// (`periodic_poller(false)` without a crontab or an explicit group, or the `periodic` cargo
/// feature off). The launcher spawns it; it stops when the shutdown starts.
pub(crate) fn task(engine: &Arc<Engine>) -> Option<BoxFuture<'static, ()>> {
    #[cfg(feature = "periodic")]
    {
        let config = &engine.config;
        if !config.periodic_poller && !registers(config) {
            return None;
        }
        Some(Box::pin(running::run(engine.clone())))
    }
    #[cfg(not(feature = "periodic"))]
    {
        let _ = engine;
        None
    }
}

/// Whether a process registers a crontab: when it has schedules, or names its group (an empty
/// crontab of an explicit group prunes the group).
#[cfg(feature = "periodic")]
pub(crate) fn registers(config: &crate::config::Config) -> bool {
    !config.periodic.is_empty() || config.periodic_group.is_some()
}

/// The group this process registers its crontab in.
#[cfg(feature = "periodic")]
pub(crate) fn group(config: &crate::config::Config) -> &str {
    config
        .periodic_group
        .as_deref()
        .unwrap_or(crate::periodic::DEFAULT_GROUP)
}

#[cfg(feature = "periodic")]
pub(crate) use running::encode;

#[cfg(feature = "periodic")]
mod running {
    use std::collections::{HashMap, HashSet};
    use std::panic::AssertUnwindSafe;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use futures::FutureExt;
    use serde_json::{Value, json};

    use super::{group, registers};
    use crate::client::Stored;
    use crate::config::ErrorContext;
    use crate::error::{Error, JobError, Result};
    use crate::keys;
    use crate::launcher::Engine;
    use crate::payload::{JobPayload, generate_jid, now_millis, now_secs_f64};
    use crate::periodic::PeriodicMeta;
    use crate::periodic::clock::Clock;
    use crate::periodic::definition::{
        Compiled, Definition, SPEC_VERSION, Unreadable, compile, crontab_digest,
    };
    use crate::periodic::plan::{Boundary, Foreign, Plan, plan};
    use crate::periodic::store::{self, Fields, Fire, Fired, Lock, SyncEntry, Write};
    use crate::periodic::timing::spoken_duration;
    use crate::processor::is_unavailable;
    use crate::unique::push::take_lock;

    /// How long a definition this process cannot read stays on its skip list: other processes
    /// (newer ones, or ones with a newer time-zone database) may fire it meanwhile.
    const SKIP_FOR: Duration = Duration::from_secs(600);

    /// Most names on the skip list sent to `LEASE` (every one, short of a pathological count:
    /// past it, a skipped name `LEASE` hands back keeps its lease instead of its score).
    const MAX_SKIP: usize = 10_000;

    /// Definitions this process cannot read handled in one poll before the poll yields to the
    /// registration checks (it resumes at once).
    const UNREADABLE_PER_POLL: usize = 1_000;

    /// How long a parsed timetable is reused: parsing it again reads the zone's rules again
    /// (the time-zone database is re-read every five minutes), so that a tzdata update reaches a
    /// running poller.
    const TZ_REFRESH: Duration = Duration::from_secs(300);

    /// How long a schedule whose template can never be pushed waits before it is tried again.
    const INVALID_DELAY_SECS: f64 = 3_600.0;

    /// How many times a fire starts over when the definition changed under it.
    const CHANGED_RETRIES: usize = 3;

    /// How many times a registration reads the group again when it changed meanwhile.
    const SYNC_ATTEMPTS: usize = 5;

    /// The longest pause between registration attempts after a failure.
    const REGISTER_RETRY: Duration = Duration::from_secs(5);

    /// Pause after a failed poll.
    const ERROR_PAUSE: Duration = Duration::from_secs(1);

    /// Pause after repeated failed polls.
    const ERROR_PAUSE_REPEATED: Duration = Duration::from_secs(5);

    /// Pause after the poller itself panicked (a bug in hench).
    const CRASH_PAUSE: Duration = Duration::from_secs(1);

    /// The longest random delay added to each sleep, spreading simultaneous wake-ups.
    const JITTER: Duration = Duration::from_millis(250);

    /// A tick enqueued later than this is logged as a warning.
    const LATE_WARNING_MS: i64 = 60_000;

    /// Ticks of timetables faster than this are logged at debug level, not info.
    const QUIET_PERIOD_SECS: i64 = 60;

    /// Definitions remembered (parsed timetables, catch-up boundaries).
    const CACHE_CAPACITY: usize = 1_000;

    /// A definition parsed once per digest (and again every [`TZ_REFRESH`]), with the memory of
    /// its `All` catch-up.
    struct Cached {
        compiled: Arc<Compiled>,
        boundary: Boundary,
        compiled_at: Instant,
    }

    impl Cached {
        /// Whether this parse can serve the definition `digest` at `now`.
        fn serves(&self, digest: &str, now: Instant) -> bool {
            self.compiled.digest == digest && now.duration_since(self.compiled_at) < TZ_REFRESH
        }
    }

    /// The poller's state (one per process).
    struct Poller {
        engine: Arc<Engine>,
        clock: Clock,
        /// The crontab this process registers, if it registers one.
        crontab: Option<Crontab>,
        cache: HashMap<String, Cached>,
        /// Names this process cannot handle, until when.
        skip: HashMap<String, Instant>,
        /// Unreadable definitions already warned about (name, digest).
        warned: HashSet<(String, String)>,
        /// Schedules whose `next` was not a tick of their timetable, already warned about (name,
        /// digest).
        disagreed: HashSet<(String, String)>,
        /// Consecutive failed polls.
        failures: u32,
    }

    /// This process's crontab and its registration state.
    struct Crontab {
        group: String,
        definitions: Vec<Definition>,
        digest: String,
        registered: bool,
        /// When this process last registered the crontab (the record's `at`, Redis clock, ms).
        registered_at: Option<i64>,
        /// Whether another digest was seen registered for the group (logged once).
        superseded: bool,
    }

    /// What the data-loss check found.
    enum Check {
        /// Nothing to do.
        Fine,
        /// The group's record is gone, unreadable, or older than this process's registration
        /// (Redis went back in time): register again.
        Lost,
    }

    /// Runs the poller until the shutdown starts.
    pub(super) async fn run(engine: Arc<Engine>) {
        let mut poller = Poller::new(engine.clone());
        poller.register_or_report().await;
        let config = engine.config.clone();
        let check_every = config.periodic_check_interval;
        let mut next_check = Instant::now() + check_every;
        let mut next_register = Instant::now() + REGISTER_RETRY.min(check_every);
        while !engine.stopping.is_cancelled() {
            let pause = if config.periodic_poller {
                // A fire in progress finishes unless the end of the shutdown cancels it; its
                // tick then waits for the lease.
                let polled = engine
                    .background
                    .run_until_cancelled(AssertUnwindSafe(poller.poll()).catch_unwind())
                    .await;
                match polled {
                    None => break,
                    Some(Ok(Ok(idle))) => {
                        poller.failures = 0;
                        engine.periodic_outage.succeeded();
                        idle
                    }
                    Some(Ok(Err(error))) => {
                        poller.failures += 1;
                        engine.periodic_outage.failed(&config, error);
                        if poller.failures > 1 {
                            ERROR_PAUSE_REPEATED
                        } else {
                            ERROR_PAUSE
                        }
                    }
                    Some(Err(panic)) => {
                        config.handle_error(
                            &JobError::panic(panic),
                            &ErrorContext::new("periodic poller crashed; restarting it"),
                        );
                        CRASH_PAUSE
                    }
                }
            } else {
                check_every
            };
            if poller.needs_registration() && Instant::now() >= next_register {
                poller.register_or_report().await;
                next_register = Instant::now() + REGISTER_RETRY.min(check_every);
            }
            if Instant::now() >= next_check {
                poller.check_registration().await;
                next_check = Instant::now() + check_every;
            }
            let until_check = next_check.saturating_duration_since(Instant::now());
            let jitter = JITTER
                .min(config.periodic_poll_interval)
                .mul_f64(fastrand::f64());
            let pause = pause.min(until_check) + jitter;
            if engine
                .stopping
                .run_until_cancelled(tokio::time::sleep(pause))
                .await
                .is_none()
            {
                break;
            }
        }
        tracing::debug!("periodic poller stopped");
    }

    /// The outcome of one fire, for the poll's logs.
    enum Handled {
        /// A job was written.
        Enqueued,
        /// Nothing written (a duplicate, halted, skipped, handled elsewhere, ...).
        Other,
        /// A definition this process cannot read (left to others, or parked).
        Unreadable,
    }

    impl Poller {
        fn new(engine: Arc<Engine>) -> Self {
            let config = &engine.config;
            let clock = Clock::of(config);
            let crontab = registers(config).then(|| {
                let mut definitions = Vec::with_capacity(config.periodic.len());
                for periodic in &config.periodic {
                    match periodic.definition() {
                        Ok(definition) => {
                            let class = periodic.class_name();
                            if !class.is_empty() && engine.registry.resolve(class).is_none() {
                                tracing::warn!(
                                    schedule = periodic.name(),
                                    class,
                                    "the job of a periodic schedule is not registered in this \
                                     process (fine if another service runs it)"
                                );
                            }
                            definitions.push(definition);
                        }
                        // `build` checked them: only a timetable that stopped ticking since.
                        Err(error) => config.handle_error(
                            &error.into_job_error(),
                            &ErrorContext::new("periodic job is invalid"),
                        ),
                    }
                }
                let digest = crontab_digest(
                    definitions
                        .iter()
                        .map(|definition| (definition.name.as_str(), definition.digest.as_str())),
                );
                Crontab {
                    group: group(config).to_owned(),
                    definitions,
                    digest,
                    registered: false,
                    registered_at: None,
                    superseded: false,
                }
            });
            Self {
                engine,
                clock,
                crontab,
                cache: HashMap::new(),
                skip: HashMap::new(),
                warned: HashSet::new(),
                disagreed: HashSet::new(),
                failures: 0,
            }
        }

        fn needs_registration(&self) -> bool {
            self.crontab
                .as_ref()
                .is_some_and(|crontab| !crontab.registered)
        }

        /// Registers the crontab, reporting a failure (it is tried again later).
        async fn register_or_report(&mut self) {
            if self.crontab.is_none() {
                return;
            }
            if let Err(error) = self.register().await {
                if is_unavailable(&error) {
                    self.engine
                        .periodic_outage
                        .failed(&self.engine.config, error);
                } else {
                    self.engine.config.handle_error(
                        &error.into_job_error(),
                        &ErrorContext::new("could not register periodic jobs"),
                    );
                }
            }
        }

        /// Registers the crontab with `PERIODIC_SYNC`: creates, updates and prunes the group's
        /// schedules in one step (`docs/design/periodic.md` §2.7).
        async fn register(&mut self) -> Result<()> {
            let Some(crontab) = self.crontab.as_mut() else {
                return Ok(());
            };
            let engine = &self.engine;
            let redis = &engine.redis;
            let members_key = keys::periodic_group(&crontab.group);
            let names: HashSet<String> = crontab
                .definitions
                .iter()
                .map(|definition| definition.name.clone())
                .collect();
            for _ in 0..SYNC_ATTEMPTS {
                let key = members_key.as_str();
                let mut members: Vec<String> = redis
                    .with_retry(|mut conn| async move {
                        ::redis::cmd("SMEMBERS")
                            .arg(key)
                            .query_async::<Vec<String>>(&mut conn)
                            .await
                    })
                    .await?;
                members.sort_unstable();
                let stale: Vec<String> = members
                    .iter()
                    .filter(|member| !names.contains(member.as_str()))
                    .cloned()
                    .collect();
                let now_ms = self.clock.now_ms(redis).await?;
                let now = now_ms.div_euclid(1_000);
                let mut entries = Vec::with_capacity(crontab.definitions.len());
                for definition in &crontab.definitions {
                    let next_new = definition.timing.next_after(now);
                    let next_changed = definition.timing.next_after(now - 60);
                    match (next_new, next_changed) {
                        (Ok(next_new), Ok(next_changed)) => entries.push(SyncEntry {
                            definition,
                            next_new,
                            next_changed,
                        }),
                        (Err(error), _) | (_, Err(error)) => engine.config.handle_error(
                            &JobError::msg(format!(
                                "periodic job `{}` has no next tick: {error}",
                                definition.name
                            )),
                            &ErrorContext::new("periodic job is invalid"),
                        ),
                    }
                }
                let record = json!({
                    "digest": crontab.digest,
                    "count": crontab.definitions.len(),
                    "at": now_ms,
                    "by": engine.identity,
                    "version": SPEC_VERSION,
                })
                .to_string();
                let Some(synced) = store::sync(
                    redis,
                    &crontab.group,
                    &record,
                    &members,
                    &stale,
                    &entries,
                    now_ms,
                )
                .await?
                else {
                    // The group changed between the read and the script: read it again.
                    continue;
                };
                tracing::info!(
                    group = %crontab.group,
                    created = synced.created,
                    updated = synced.updated,
                    rescheduled = synced.rescheduled,
                    unchanged = synced.unchanged,
                    removed = synced.removed,
                    conflicts = synced.conflicts.len(),
                    "registered periodic jobs"
                );
                for name in &synced.conflicts {
                    engine.config.handle_error(
                        &JobError::msg(format!(
                            "periodic job `{name}` is not registered: an API schedule or another \
                             group already uses the name"
                        )),
                        &ErrorContext::new("could not register periodic jobs"),
                    );
                }
                crontab.registered = true;
                crontab.registered_at = Some(now_ms);
                crontab.superseded = false;
                return Ok(());
            }
            Err(Error::Config(format!(
                "periodic group `{}`: its schedules changed during {SYNC_ATTEMPTS} registrations \
                 in a row (do two deployments with different crontabs share the group?)",
                crontab.group
            )))
        }

        /// The data-loss check (`docs/design/periodic.md` §2.7, L1): the group's record gone
        /// means Redis lost the schedules (a restart without persistence, a failover to an empty
        /// replica, a deletion): register again. A record of another crontab older than this
        /// process's registration means Redis went back in time (a restore from a backup, a
        /// failover to a replica that missed the registration): register again too. A newer one
        /// means another process registered another crontab since: last registration wins.
        async fn check_registration(&mut self) {
            let check = match self.crontab.as_mut() {
                Some(crontab) if crontab.registered => {
                    let group = crontab.group.as_str();
                    let record: Result<Option<String>> = self
                        .engine
                        .redis
                        .with_retry(|mut conn| async move {
                            ::redis::cmd("HGET")
                                .arg(keys::PERIODIC_CRONTABS)
                                .arg(group)
                                .query_async::<Option<String>>(&mut conn)
                                .await
                        })
                        .await;
                    match record {
                        Ok(None) => {
                            tracing::warn!(
                                group,
                                "the periodic schedules of this group are missing from Redis; \
                                 registering them again"
                            );
                            crontab.registered = false;
                            Check::Lost
                        }
                        Ok(Some(record)) => {
                            let record = serde_json::from_str::<Value>(&record).ok();
                            let digest = record
                                .as_ref()
                                .and_then(|record| record["digest"].as_str().map(str::to_owned));
                            let at = record.as_ref().and_then(|record| record["at"].as_i64());
                            match (digest, at) {
                                (Some(digest), _) if digest == crontab.digest => Check::Fine,
                                (Some(_), Some(at))
                                    if crontab.registered_at.is_none_or(|ours| at >= ours) =>
                                {
                                    if !crontab.superseded {
                                        crontab.superseded = true;
                                        let by = record
                                            .as_ref()
                                            .and_then(|record| record["by"].as_str())
                                            .unwrap_or("?")
                                            .to_owned();
                                        tracing::warn!(
                                            group,
                                            by,
                                            "another process registered another crontab for \
                                             this group since this one did; the last registration \
                                             wins (fine while a deploy rolls out; otherwise two \
                                             deployments with different schedules share the group)"
                                        );
                                    }
                                    Check::Fine
                                }
                                (Some(_), Some(_)) => {
                                    tracing::warn!(
                                        group,
                                        "the periodic schedules of this group in Redis are older \
                                         than this process's registration (Redis was restored or \
                                         failed over to older data); registering them again"
                                    );
                                    crontab.registered = false;
                                    Check::Lost
                                }
                                _ => {
                                    tracing::warn!(
                                        group,
                                        "the registration record of this group's periodic \
                                         schedules is unreadable; registering them again"
                                    );
                                    crontab.registered = false;
                                    Check::Lost
                                }
                            }
                        }
                        Err(error) => {
                            tracing::debug!(%error, "could not check the periodic registration");
                            Check::Fine
                        }
                    }
                }
                _ => Check::Fine,
            };
            if let Check::Lost = check {
                self.register_or_report().await;
            }
        }

        /// The names to skip, without the expired ones: every one of them, so that `LEASE` never
        /// hands one back (which, with its past score restored each time, made the poll spin),
        /// up to [`MAX_SKIP`] (the most recent ones).
        fn skip_list(&mut self) -> Vec<String> {
            let now = Instant::now();
            self.skip.retain(|_, until| *until > now);
            let mut names: Vec<(String, Instant)> = self
                .skip
                .iter()
                .map(|(name, until)| (name.clone(), *until))
                .collect();
            if names.len() > MAX_SKIP {
                names.sort_by_key(|(_, until)| std::cmp::Reverse(*until));
                names.truncate(MAX_SKIP);
            }
            names.into_iter().map(|(name, _)| name).collect()
        }

        /// Fires every due schedule, then says how long the poller may sleep. Fails only when
        /// Redis is unavailable.
        async fn poll(&mut self) -> Result<Duration> {
            let config = self.engine.config.clone();
            let mut enqueued = 0_u64;
            let mut unreadable = 0_usize;
            loop {
                if self.engine.stopping.is_cancelled() {
                    return Ok(Duration::ZERO);
                }
                let skip = self.skip_list();
                let leased = store::lease(
                    &self.engine.redis,
                    config.periodic_lease,
                    self.clock.override_ms(),
                    &skip,
                )
                .await?;
                let Some((name, score)) = leased.due else {
                    if enqueued > 0 {
                        tracing::debug!(enqueued, "enqueued periodic jobs");
                    }
                    let idle = match leased.earliest {
                        Some(earliest) => {
                            let wait = earliest - leased.now_ms as f64 / 1_000.0;
                            Duration::try_from_secs_f64(wait.max(0.0)).unwrap_or(Duration::ZERO)
                        }
                        None => config.periodic_poll_interval,
                    };
                    return Ok(idle.min(config.periodic_poll_interval));
                };
                if self
                    .skip
                    .get(&name)
                    .is_some_and(|until| *until > Instant::now())
                {
                    // Skipped, but beyond the names sent: it keeps the lease just taken (newer
                    // processes fire it when the lease ends) rather than its past score, which
                    // would hand it back at once.
                    unreadable += 1;
                } else {
                    match self.fire(&name, score, leased.now_ms).await? {
                        Handled::Enqueued => enqueued += 1,
                        Handled::Unreadable => unreadable += 1,
                        Handled::Other => {}
                    }
                }
                if unreadable >= UNREADABLE_PER_POLL {
                    // Let the registration checks run; the poll resumes at once.
                    return Ok(Duration::ZERO);
                }
            }
        }

        /// The definition of a schedule, parsed (cached per digest).
        fn compiled(
            &mut self,
            name: &str,
            fields: &Fields,
        ) -> std::result::Result<Arc<Compiled>, Unreadable> {
            let field = |key: &str| fields.get(key).map(String::as_str).unwrap_or_default();
            let digest = field("digest");
            let now = Instant::now();
            if let Some(cached) = self.cache.get(name)
                && cached.serves(digest, now)
            {
                return Ok(cached.compiled.clone());
            }
            let compiled = Arc::new(compile(
                field("spec"),
                field("policy"),
                field("job"),
                digest,
            )?);
            // The same definition parsed again (for the zone's current rules) keeps the memory of
            // its catch-up.
            let boundary = self
                .cache
                .get(name)
                .filter(|cached| cached.compiled.digest == digest)
                .map(|cached| cached.boundary)
                .unwrap_or_default();
            if self.cache.len() >= CACHE_CAPACITY
                && !self.cache.contains_key(name)
                && let Some(evicted) = self.cache.keys().next().cloned()
            {
                self.cache.remove(&evicted);
            }
            self.cache.insert(
                name.to_owned(),
                Cached {
                    compiled: compiled.clone(),
                    boundary,
                    compiled_at: now,
                },
            );
            Ok(compiled)
        }

        /// Handles a leased schedule (`docs/design/periodic.md` §2.6, "fire"). Fails only when
        /// Redis is unavailable (the schedule's score is restored first when possible).
        async fn fire(&mut self, name: &str, leased_score: f64, now_ms: i64) -> Result<Handled> {
            for _ in 0..CHANGED_RETRIES {
                if let Some(handled) = self.fire_once(name, leased_score, now_ms).await? {
                    return Ok(handled);
                }
                // The definition changed meanwhile: read it again.
            }
            tracing::debug!(
                schedule = name,
                "the definition of a periodic schedule keeps changing; leaving it to its lease"
            );
            Ok(Handled::Other)
        }

        /// One attempt of [`Poller::fire`]; `None` when the definition changed under it.
        async fn fire_once(
            &mut self,
            name: &str,
            leased_score: f64,
            now_ms: i64,
        ) -> Result<Option<Handled>> {
            let redis = self.engine.redis.clone();
            let fields = store::read(&redis, name).await?;
            if fields.is_empty() || fields.contains_key("paused") {
                // Deleted or paused since it was indexed: nothing to fire. The entry is removed
                // only if it still is (a resume or a re-creation since the read keeps it).
                store::unindex(&redis, name).await?;
                return Ok(Some(Handled::Other));
            }
            let digest = fields.get("digest").cloned().unwrap_or_default();
            let compiled = match self.compiled(name, &fields) {
                Ok(compiled) => compiled,
                Err(unreadable) => {
                    self.unreadable(name, &digest, &unreadable, leased_score, now_ms)
                        .await?;
                    return Ok(Some(Handled::Unreadable));
                }
            };
            let now = now_ms.div_euclid(1_000);
            let expected = fields.get("next").cloned().unwrap_or_default();
            let next = expected.parse::<i64>().ok();
            let last_tick = fields
                .get("last_tick")
                .and_then(|tick| tick.parse::<i64>().ok());
            let timing = &compiled.timing;
            let Some(stored_next) = next else {
                // `next` was lost (or is not a number): repair it, enqueueing nothing.
                let repaired = timing.next_after(now).ok();
                return self
                    .advance(name, &expected, &digest, repaired, 0, String::new(), now_ms)
                    .await
                    .map(|fired| fired.map(|()| Handled::Other));
            };
            if stored_next > now {
                // Rescheduled meanwhile: due later.
                store::rescore(&redis, name, stored_next as f64).await?;
                return Ok(Some(Handled::Other));
            }
            // A `next` at or before the last tick fired (a timetable changed right after a tick)
            // is moved past it rather than refused by `FIRE`'s guard.
            let due = match last_tick {
                Some(last) if stored_next <= last => match timing.next_after(last) {
                    Ok(after) if after > now => {
                        return self
                            .advance(
                                name,
                                &expected,
                                &digest,
                                Some(after),
                                0,
                                String::new(),
                                now_ms,
                            )
                            .await
                            .map(|fired| fired.map(|()| Handled::Other));
                    }
                    Ok(after) => after,
                    Err(_) => stored_next,
                },
                _ => stored_next,
            };
            let mut boundary = self
                .cache
                .get(name)
                .map(|cached| cached.boundary)
                .unwrap_or_default();
            let planned = plan(
                timing,
                &compiled.policy,
                due,
                last_tick,
                now_ms,
                &mut boundary,
            );
            if let Some(cached) = self.cache.get_mut(name) {
                cached.boundary = boundary;
            }
            let planned = match planned {
                Ok(planned) => planned,
                Err(error) => {
                    // No next tick: park the schedule (out of the due index) with the reason.
                    let fired = self
                        .advance(name, &expected, &digest, None, 0, String::new(), now_ms)
                        .await?;
                    let reason = format!("no next tick: {error}");
                    store::set_error(&redis, name, &reason, now_ms).await?;
                    self.engine.config.handle_error(
                        &JobError::msg(format!("periodic job `{name}`: {reason}")),
                        &ErrorContext::new("periodic job is invalid"),
                    );
                    return Ok(fired.map(|()| Handled::Other));
                }
            };
            if planned.missed > 0 {
                warn_missed(name, &compiled, &planned);
            }
            if let Some(foreign) = planned.foreign {
                self.disagreement(name, &digest, due, foreign);
            }
            let Some(tick) = planned.fire else {
                // The policy drops every due tick: count them, advance. A `next` standing for a
                // tick handled already is recorded too (the history explains the gap).
                let entry = match planned.foreign {
                    Some(Foreign::Handled(_)) => self.entry(
                        Some(due),
                        "skipped",
                        None,
                        planned.missed,
                        Some(now_ms - due * 1_000),
                        now_ms,
                    ),
                    _ if planned.missed > 0 => {
                        self.entry(None, "skipped", None, planned.missed, None, now_ms)
                    }
                    _ => String::new(),
                };
                return self
                    .advance(
                        name,
                        &expected,
                        &digest,
                        Some(planned.next),
                        planned.missed,
                        entry,
                        now_ms,
                    )
                    .await
                    .map(|fired| fired.map(|()| Handled::Other));
            };
            let target = Target {
                name,
                expected: &expected,
                digest: &digest,
                leased_score,
                now_ms,
            };
            self.enqueue_tick(&target, &compiled, &planned, tick).await
        }

        /// A due `next` that was not a tick of the timetable as this process computes it
        /// (`docs/design/periodic.md` §2.6): warned about once per definition, then at debug
        /// level.
        fn disagreement(&mut self, name: &str, digest: &str, due: i64, foreign: Foreign) {
            let reading = match foreign {
                Foreign::InPlaceOf(tick) => format!("enqueued in place of the tick {tick}"),
                Foreign::Handled(tick) => {
                    format!("not enqueued: it stands for the tick {tick}, handled already")
                }
                Foreign::Extra => "enqueued as a tick of its own".to_owned(),
            };
            if self.disagreed.insert((name.to_owned(), digest.to_owned())) {
                tracing::warn!(
                    schedule = name,
                    next = due,
                    reading = %reading,
                    "a periodic schedule's next tick is not a tick of its timetable as this \
                     process computes it: the processes disagree on the time zone's rules (give \
                     every host the same tzdata and hench version) or it was edited by hand"
                );
            } else {
                tracing::debug!(
                    schedule = name,
                    next = due,
                    reading = %reading,
                    "a periodic schedule's next tick is not a tick of its timetable here"
                );
            }
        }

        /// A definition this process cannot read, recorded on the schedule (`error`) and warned
        /// about once. Written by a newer version or in a zone this process's time-zone database
        /// lacks, it is skipped here for a while and its score restored, so that the processes
        /// that can read it fire it; invalid for every version, it waits an hour (a new
        /// definition, from the code or the API, makes it due at once).
        async fn unreadable(
            &mut self,
            name: &str,
            digest: &str,
            unreadable: &Unreadable,
            leased_score: f64,
            now_ms: i64,
        ) -> Result<()> {
            let redis = &self.engine.redis;
            let reason = format!("{} (process {})", unreadable.reason(), self.engine.identity);
            store::set_error(redis, name, &reason, now_ms).await?;
            match unreadable {
                Unreadable::Newer(_) | Unreadable::UnknownZone(_) => {
                    self.skip.insert(name.to_owned(), Instant::now() + SKIP_FOR);
                    store::rescore(redis, name, leased_score).await?;
                }
                Unreadable::Invalid(_) => {
                    let later = now_ms as f64 / 1_000.0 + INVALID_DELAY_SECS;
                    store::rescore(redis, name, later).await?;
                }
            }
            if self.warned.insert((name.to_owned(), digest.to_owned())) {
                match unreadable {
                    Unreadable::Newer(reason) => tracing::warn!(
                        schedule = name,
                        reason = reason.as_str(),
                        "this process cannot read a periodic schedule written by a newer \
                         version; leaving it to the processes that can"
                    ),
                    Unreadable::UnknownZone(reason) => tracing::warn!(
                        schedule = name,
                        reason = reason.as_str(),
                        "this process's time-zone database lacks the zone of a periodic \
                         schedule; leaving it to the processes that have it (update this \
                         host's tzdata)"
                    ),
                    Unreadable::Invalid(reason) => self.engine.config.handle_error(
                        &JobError::msg(format!("periodic job `{name}`: {reason}")),
                        &ErrorContext::new("periodic job is invalid"),
                    ),
                }
            }
            Ok(())
        }

        /// A history entry.
        fn entry(
            &self,
            tick: Option<i64>,
            result: &str,
            jid: Option<&str>,
            missed: u64,
            late_ms: Option<i64>,
            now_ms: i64,
        ) -> String {
            history_entry(
                &self.engine.identity,
                tick,
                result,
                jid,
                missed,
                late_ms,
                now_ms,
            )
        }

        /// A `FIRE` that writes no job: a repair, a park, a policy dropping ticks. `None` when
        /// the definition changed.
        #[allow(clippy::too_many_arguments, reason = "the parts of one FIRE")]
        async fn advance(
            &self,
            name: &str,
            expected: &str,
            digest: &str,
            new_next: Option<i64>,
            missed: u64,
            entry: String,
            now_ms: i64,
        ) -> Result<Option<()>> {
            let fire = Fire {
                name,
                expected_next: expected,
                digest,
                new_next,
                tick: None,
                write: Write::Skipped,
                lock: None,
                missed,
                entry,
                duplicate_entry: String::new(),
                history: self.engine.config.periodic_history,
                jid: "",
                identity: &self.engine.identity,
                now_ms,
            };
            match store::fire(&self.engine.redis, &fire, &events_key()).await? {
                Fired::Changed => Ok(None),
                fired => {
                    tracing::debug!(
                        schedule = name,
                        ?fired,
                        ?new_next,
                        "advanced a periodic schedule"
                    );
                    Ok(Some(()))
                }
            }
        }

        /// Builds the tick's job, runs it through the client and fires it.
        async fn enqueue_tick(
            &mut self,
            target: &Target<'_>,
            compiled: &Compiled,
            planned: &Plan,
            tick: i64,
        ) -> Result<Option<Handled>> {
            let Target {
                name,
                expected,
                digest,
                leased_score,
                now_ms,
            } = *target;
            let engine = self.engine.clone();
            let redis = &engine.redis;
            let mut job = compiled.template.clone();
            job.jid = generate_jid();
            job.periodic = Some(PeriodicMeta::new(name, tick * 1_000, false));
            let jobstr = job.to_json().unwrap_or_default();
            // Client middleware runs as for any push; a panic there is a rejected push.
            let prepared = AssertUnwindSafe(engine.client.prepare_push(job))
                .catch_unwind()
                .await
                .unwrap_or_else(|panic| Err(Error::Job(JobError::panic(panic))));
            let (write, lock, jid) = match prepared.and_then(encode) {
                Ok(Some(encoded)) => encoded,
                Ok(None) => (Write::Halted, None, String::new()),
                Err(error @ (Error::InvalidJob(_) | Error::Json(_))) => {
                    // The template can never be pushed: recorded, tried again in an hour.
                    let reason = format!("the job cannot be pushed: {error}");
                    store::set_error(redis, name, &reason, now_ms).await?;
                    store::rescore(redis, name, now_ms as f64 / 1_000.0 + INVALID_DELAY_SECS)
                        .await?;
                    engine.config.handle_error(
                        &error.into_job_error(),
                        &ErrorContext::new("periodic job is invalid").with_jobstr(&jobstr),
                    );
                    return Ok(Some(Handled::Other));
                }
                Err(error) if is_unavailable(&error) => {
                    // Redis cannot serve now: give the schedule its score back; the poll stops.
                    if let Err(restore) = store::rescore(redis, name, leased_score).await {
                        tracing::debug!(
                            error = %restore,
                            "could not give a periodic schedule its score back"
                        );
                    }
                    return Err(error);
                }
                Err(error) => {
                    // Client middleware failing or panicking: recorded, retried when the lease
                    // ends, reported through the outage notices (rate-limited).
                    let reason = format!("the push failed: {error}");
                    store::set_error(redis, name, &reason, now_ms).await?;
                    engine
                        .periodic_outage
                        .failed_with_job(&engine.config, error, Some(&jobstr));
                    return Ok(Some(Handled::Other));
                }
            };
            let late_ms = now_ms - tick * 1_000;
            let result = match &write {
                Write::Enqueued { .. } => "enqueued",
                Write::Scheduled { .. } => "scheduled",
                Write::Halted | Write::Skipped => "halted",
            };
            let written = (!jid.is_empty()).then_some(jid.as_str());
            let entry = self.entry(
                Some(tick),
                result,
                written,
                planned.missed,
                Some(late_ms),
                now_ms,
            );
            let duplicate_entry = self.entry(
                Some(tick),
                "duplicate",
                None,
                planned.missed,
                Some(late_ms),
                now_ms,
            );
            let fire = Fire {
                name,
                expected_next: expected,
                digest,
                new_next: Some(planned.next),
                tick: Some(tick),
                write,
                lock,
                missed: planned.missed,
                entry,
                duplicate_entry,
                history: engine.config.periodic_history,
                jid: &jid,
                identity: &engine.identity,
                now_ms,
            };
            let fired = match store::fire(redis, &fire, &events_key()).await {
                Ok(fired) => fired,
                Err(error) if is_unavailable(&error) => {
                    if let Err(restore) = store::rescore(redis, name, leased_score).await {
                        tracing::debug!(
                            error = %restore,
                            "could not give a periodic schedule its score back"
                        );
                    }
                    return Err(error);
                }
                Err(error) => {
                    // The job's keys cannot take it (a key of the wrong type): nothing was
                    // written; recorded, retried when the lease ends.
                    let reason = format!("the job cannot be written: {error}");
                    store::set_error(redis, name, &reason, now_ms).await?;
                    engine
                        .periodic_outage
                        .failed_with_job(&engine.config, error, Some(&jobstr));
                    return Ok(Some(Handled::Other));
                }
            };
            let quiet = compiled
                .timing
                .next_after(tick)
                .is_ok_and(|after| after - tick < QUIET_PERIOD_SECS);
            Ok(Some(match fired {
                Fired::Done { without_lock } => {
                    if let Some(error) = without_lock {
                        tracing::warn!(
                            schedule = name,
                            %jid,
                            %error,
                            "a periodic job was pushed without its unique lock"
                        );
                    }
                    if jid.is_empty() {
                        tracing::debug!(
                            schedule = name,
                            tick,
                            "client middleware stopped a periodic job"
                        );
                        Handled::Other
                    } else {
                        if late_ms > LATE_WARNING_MS {
                            tracing::warn!(
                                schedule = name,
                                tick,
                                %jid,
                                late = %spoken_duration(late_ms / 1_000),
                                "periodic job enqueued late"
                            );
                        } else if quiet {
                            tracing::debug!(
                                schedule = name,
                                tick,
                                %jid,
                                late_ms,
                                "enqueued periodic job"
                            );
                        } else {
                            tracing::info!(
                                schedule = name,
                                tick,
                                %jid,
                                late_ms,
                                "enqueued periodic job"
                            );
                        }
                        Handled::Enqueued
                    }
                }
                Fired::Duplicate { holder } => {
                    tracing::info!(
                        schedule = name,
                        tick,
                        %holder,
                        "periodic tick skipped: the previous job still holds its unique lock"
                    );
                    Handled::Other
                }
                Fired::Guard => {
                    tracing::warn!(
                        schedule = name,
                        tick,
                        "a periodic tick was not after the last one handled (a clock jump?); \
                         skipped"
                    );
                    Handled::Other
                }
                Fired::Handled => {
                    // A resend after a lost reply, or another poller: ours if the hash names
                    // this process and this tick (and our jid, when one was written).
                    let fields = store::read(redis, name).await?;
                    let ours = fields
                        .get("last_tick")
                        .and_then(|last| last.parse::<i64>().ok())
                        == Some(tick)
                        && fields.get("last_by") == Some(&engine.identity)
                        && (jid.is_empty() || fields.get("last_jid") == Some(&jid));
                    tracing::debug!(schedule = name, tick, ours, "periodic tick handled already");
                    if ours && !jid.is_empty() {
                        Handled::Enqueued
                    } else {
                        Handled::Other
                    }
                }
                Fired::Changed => return Ok(None),
                Fired::Paused | Fired::Gone => {
                    tracing::debug!(
                        schedule = name,
                        ?fired,
                        "periodic schedule paused or deleted meanwhile"
                    );
                    Handled::Other
                }
            }))
        }
    }

    /// The schedule a fire is for, as read.
    #[derive(Clone, Copy)]
    struct Target<'a> {
        name: &'a str,
        expected: &'a str,
        digest: &'a str,
        leased_score: f64,
        now_ms: i64,
    }

    fn warn_missed(name: &str, compiled: &Compiled, planned: &Plan) {
        let missed = if planned.capped {
            format!("{}+", planned.missed)
        } else {
            planned.missed.to_string()
        };
        tracing::warn!(
            schedule = name,
            missed = %missed,
            policy = compiled.policy.catch_up.as_str(),
            "periodic ticks were missed"
        );
    }

    /// A history entry (`docs/design/periodic.md` §2.4): the tick (epoch seconds; `null` for
    /// manual enqueues and policy skips), when (ms), the result, the jid written, the process,
    /// the ticks missed, the lateness.
    pub(crate) fn history_entry(
        identity: &str,
        tick: Option<i64>,
        result: &str,
        jid: Option<&str>,
        missed: u64,
        late_ms: Option<i64>,
        now_ms: i64,
    ) -> String {
        let mut entry = json!({
            "tick": tick,
            "at": now_ms,
            "result": result,
            "by": identity,
            "missed": missed,
        });
        if let Some(jid) = jid {
            entry["jid"] = json!(jid);
        }
        if let Some(late_ms) = late_ms {
            entry["late_ms"] = json!(late_ms);
        }
        entry.to_string()
    }

    /// The per-minute counters of unique jobs (duplicates counted by `FIRE`).
    pub(crate) fn events_key() -> String {
        keys::unique_minute(chrono::Utc::now())
    }

    /// How a job that went through client middleware is written: its encoding as `raw_push`
    /// stores it, its lock, and its jid. `None` when middleware stopped it.
    pub(crate) fn encode(job: Option<JobPayload>) -> Result<Option<(Write, Option<Lock>, String)>> {
        let Some(mut job) = job else {
            return Ok(None);
        };
        let now = now_secs_f64();
        if job.at.is_some_and(|at| at <= now) {
            job.at = None;
        }
        let lock = take_lock(&mut job)?;
        let jid = job.jid.clone();
        let class = job.class.clone();
        let queue = job.queue.clone();
        let write = match Stored::encode(job, now, now_millis())? {
            Stored::Queued { queue, json } => Write::Enqueued { queue, json },
            Stored::Scheduled { at, json } => Write::Scheduled { at, json },
        };
        let lock = lock.map(|lock| Lock { lock, class, queue });
        Ok(Some((write, lock, jid)))
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn parsed_timetables_are_parsed_again_after_five_minutes() {
            // periodic-4: a parse cached for good kept the zone rules of the process's start, so
            // a tzdata update never reached a running poller.
            let compiled = compile(
                r#"{"cron":"0 9 * * *","tz":"Europe/Paris","v":1}"#,
                r#"{"catch_up":"latest"}"#,
                r#"{"args":[],"class":"Report"}"#,
                "d1",
            )
            .unwrap();
            let start = Instant::now();
            let cached = Cached {
                compiled: Arc::new(compiled),
                boundary: Boundary::default(),
                compiled_at: start,
            };
            assert!(cached.serves("d1", start + Duration::from_secs(299)));
            assert!(!cached.serves("d1", start + TZ_REFRESH));
            assert!(!cached.serves("d2", start), "another definition");
        }
    }
}

#[cfg(feature = "periodic")]
pub(crate) use running::{events_key, history_entry};
