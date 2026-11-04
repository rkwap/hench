//! The engine side of rate limiting (`docs/design/ratelimit.md` §2.5, §2.6): the throttle gate
//! before a job runs, the over-limit decision after it ran, and the permits the engine holds for
//! running throttled jobs (registered by processor slot, renewed by the heartbeat, released
//! after each outcome write).

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::time::Instant;

use tokio::sync::Mutex as AsyncMutex;

use crate::config::{Config, ErrorContext};
use crate::error::JobError;
use crate::fetch::UnitOfWork;
use crate::launcher::Engine;
use crate::limit::limiter::{Obtained, Request, obtain};
use crate::limit::scripts::{self, LeaseRef, ReleaseOutcome, RenewOutcome};
use crate::limit::target::Store;
use crate::limit::throttle::warn_now;
use crate::limit::{Limited, OverLimit, OverLimitDecision, Reschedule, reschedule, wakeups};
use crate::lua::ScriptCall;
use crate::payload::{JobPayload, random_hex};
use crate::pro::{Fetched, Gate};
use crate::processor::is_unavailable;

/// Separates the jid from the random suffix in the token of an engine-held lease.
const TOKEN_SEPARATOR: char = '~';

/// Hex digits of the random suffix of an engine-held lease's token.
const TOKEN_SUFFIX: usize = 16;

/// The token of the leases one execution of job `jid` holds: the jid, `~` and 16 random hex
/// digits. Each execution has its own, so that releasing the leases of an execution that
/// ended (after its job was requeued or pushed back, and possibly fetched again at once by
/// another processor) can never take the leases of the next execution of the same job.
pub(crate) fn execution_token(jid: &str) -> String {
    format!("{jid}{TOKEN_SEPARATOR}{}", random_hex(TOKEN_SUFFIX / 2))
}

/// The jid in the token of an engine-held lease ([`execution_token`]); `None` for the token of
/// a limiter's permit (16 random hex digits) or anything else.
pub(crate) fn token_jid(token: &str) -> Option<&str> {
    let split = token.len().checked_sub(TOKEN_SUFFIX + 1)?;
    let (jid, suffix) = token.split_at_checked(split)?;
    let suffix = suffix.strip_prefix(TOKEN_SEPARATOR)?;
    (!jid.is_empty()
        && suffix
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
    .then_some(jid)
}

/// How long the jobs of each throttle held their permits in this process: a decaying average
/// per throttle name. The wait line of an identity that has not measured its own hold times
/// yet (the first jobs of a new key, a throttle idle for a day) is spaced with it, instead of a
/// guess: too wide a guess leaves slots idle while parked jobs wait for theirs, too narrow a
/// one brings them back early.
pub(crate) mod holds {
    use std::collections::HashMap;
    use std::sync::{LazyLock, Mutex, PoisonError};
    use std::time::Duration;

    use crate::limit::scripts::DEFAULT_HOLD;

    /// Throttles remembered at most (names come from definitions, so this is a safety net).
    const MAX_THROTTLES: usize = 1_024;

    /// The most a job's parkings double the default hold of an identity measured by no one.
    const MAX_DOUBLINGS: u32 = 6;

    static HOLDS: LazyLock<Mutex<HashMap<String, Duration>>> =
        LazyLock::new(|| Mutex::new(HashMap::new()));

    /// The hold time to assume for `throttle` when its identity has not measured one: this
    /// process's average for the throttle, else the default doubled by each earlier parking of
    /// the job (`parked`), so that while nobody knows how long the throttle's jobs take, the
    /// first ones come back early and often, then less and less.
    pub(crate) fn expected(throttle: &str, parked: u32) -> Duration {
        let measured = HOLDS
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(throttle)
            .copied();
        measured.unwrap_or_else(|| DEFAULT_HOLD * 2_u32.pow(parked.min(MAX_DOUBLINGS)))
    }

    /// Records that a job of `throttle` held its permits for `held` (each new hold weighs a
    /// fifth of the average).
    pub(crate) fn record(throttle: &str, held: Duration) {
        let mut holds = HOLDS.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(average) = holds.get_mut(throttle) {
            *average = average.mul_f64(0.8) + held.mul_f64(0.2);
        } else if holds.len() < MAX_THROTTLES {
            holds.insert(throttle.to_owned(), held);
        }
    }
}

/// The concurrency permits the engine holds for its running jobs (a throttle's, and a queue
/// limit's), by processor slot (tid). A renewal holds the map for its round trip, so that a permit released meanwhile is
/// not taken back by it.
#[derive(Debug, Default)]
pub(crate) struct HeldPermits {
    slots: AsyncMutex<HashMap<String, Vec<HeldPermit>>>,
}

/// One lease the engine holds for a running job.
#[derive(Debug, Clone)]
pub(crate) struct HeldPermit {
    /// The lease (token: the execution's for a throttle, see [`execution_token`]; the fetch's
    /// for a queue limit).
    pub(crate) lease: LeaseRef,
    /// What granted it: the throttle's name, or `@<queue>` for a queue's limits (for logs).
    pub(crate) owner: String,
    /// When it was granted (this process's clock), for the throttle's average hold time.
    pub(crate) granted: Instant,
}

impl HeldPermits {
    /// Registers the leases of slot `tid`'s job, granted by `owner` (a throttle's name, or
    /// `@<queue>`).
    pub(crate) async fn hold(&self, tid: &str, leases: Vec<LeaseRef>, owner: &str) {
        let granted = Instant::now();
        let permits = leases.into_iter().map(|lease| HeldPermit {
            lease,
            owner: owner.to_owned(),
            granted,
        });
        self.slots
            .lock()
            .await
            .entry(tid.to_owned())
            .or_default()
            .extend(permits);
    }

    /// Takes the leases of the slots `tids` out of the map.
    pub(crate) async fn take(&self, tids: &[String]) -> Vec<HeldPermit> {
        let mut slots = self.slots.lock().await;
        tids.iter()
            .filter_map(|tid| slots.remove(tid))
            .flatten()
            .collect()
    }

    /// The number of leases held.
    #[allow(dead_code, reason = "read by the tests")]
    pub(crate) async fn len(&self) -> usize {
        self.slots.lock().await.values().map(Vec::len).sum()
    }
}

/// The throttle gate of a fetched job (`fetched.throttle`), before `start_work`
/// (`docs/design/ratelimit.md` §2.6). Granted: the concurrency leases are registered under the
/// slot `tid` and the job runs. Refused: the job is parked until its slot (or fails, once its
/// throttle's budget of reschedules is spent). Redis unavailable: the job goes back to its queue
/// (or runs unthrottled with `fail_open`). Any other error (a key of the wrong type): the job
/// fails with it, so that one bad key cannot block the front of a queue. The processor does every
/// write.
pub(crate) async fn gate(
    engine: &Engine,
    tid: &str,
    _work: &UnitOfWork,
    job: &JobPayload,
    fetched: &Fetched,
) -> Gate {
    let Some(reference) = &fetched.throttle else {
        return Gate::Run;
    };
    let Some(throttle) = engine.throttles.get(&reference.name) else {
        if warn_now(&format!("unknown-throttle:{}", reference.name)) {
            tracing::warn!(
                throttle = %reference.name,
                "a job names a throttle this process does not define: it runs unthrottled \
                 (define the throttle in every worker)"
            );
        }
        return Gate::Run;
    };
    let rules = match throttle.rules(reference.key.as_deref()) {
        Ok(rules) => rules,
        Err(error) => return Gate::Failed(JobError::from(error)),
    };
    if rules.is_empty() {
        warn_keyless(&reference.name, &job.class);
        return Gate::Run;
    }
    // A token of its own for this execution (resent as is after a lost reply, so that the
    // resend is granted idempotently): a job requeued or pushed back at the end of an earlier
    // execution can be fetched and gated again before that execution's permits are released,
    // and the release must not take the new execution's lease.
    let token = execution_token(&job.jid);
    let members: Vec<&str> = rules.iter().map(|rule| rule.member.as_str()).collect();
    // A job parked by this throttle comes back for its place in the line.
    let ticket = Limited::ticket_for(job, &members);
    let parked = job.limited.as_ref().map_or(0, |limited| limited.throttled);
    let request = Request {
        rules: &rules,
        token: &token,
        cost: 1,
        lease: throttle.lease_duration(),
        max_wait: throttle.max_wait_duration(),
        line: throttle.has_stats(),
        stats: throttle.has_stats(),
        stats_ttl: throttle.stats_ttl_duration(),
        ticket,
        comes_back: true,
        expected_hold: Some(holds::expected(throttle.name(), parked)),
    };
    let store = Store::Redis(engine.redis.clone());
    match obtain(&store, &request, Some(&engine.stopping)).await {
        Ok(Obtained::Granted { now_ms, .. }) => {
            let leases = request.leases(now_ms);
            if !leases.is_empty() {
                engine.permits.hold(tid, leases, throttle.name()).await;
            }
            Gate::Run
        }
        Ok(Obtained::Refused {
            limiting,
            wait,
            place,
        }) => {
            let rule = &rules[limiting];
            if let Some(max) = throttle.reschedule_budget()
                && parked.saturating_add(1) > max
            {
                let over = OverLimit::new(rule.member.clone())
                    .with_limit(rule.limit)
                    .with_retry_after(wait);
                return Gate::Failed(reschedule::exhausted(&over, max));
            }
            let delay =
                reschedule::throttle_delay(wait, throttle.max_delay_duration(), request.line);
            Gate::Refused(Reschedule::throttled(
                job,
                rule.member.clone(),
                delay,
                place,
            ))
        }
        Ok(Obtained::Impossible { limiting }) => Gate::Failed(JobError::msg(format!(
            "the throttle `{}` can never grant `{}`",
            throttle.name(),
            rules[limiting].member
        ))),
        Err(error) if is_unavailable(&error) => {
            if throttle.is_fail_open() {
                if warn_now(&format!("fail-open:{}", throttle.name())) {
                    tracing::warn!(
                        %error,
                        throttle = %throttle.name(),
                        "Redis is unavailable: running throttled jobs unthrottled (fail_open)"
                    );
                }
                Gate::Run
            } else {
                Gate::Unavailable(error)
            }
        }
        Err(error) => Gate::Failed(JobError::from(error)),
    }
}

/// Whether a job's error is a refusal by a limiter (`JobError::as_over_limit`, or one the
/// `over_limit_when` classifier recognizes), and what to do: reschedule within the budget, or
/// fail (class `hench::OverLimit`). `job` is the job as fetched (its `limited.n` counts the
/// reschedules so far; the count is never reset, so once a job's budget is spent its later
/// refusals fail at once and the retry backoff governs it).
pub(crate) fn over_limit(
    engine: &Engine,
    error: &JobError,
    job: &JobPayload,
) -> Option<OverLimitDecision> {
    decide(&engine.config, error, job)
}

/// [`over_limit`] with a configuration (the decision needs nothing else of the engine).
pub(crate) fn decide(
    config: &Config,
    error: &JobError,
    job: &JobPayload,
) -> Option<OverLimitDecision> {
    let over = match error.as_over_limit() {
        Some(over) => over.clone(),
        None => classify(config, error, job)?,
    };
    // The limiters' budget counts the reschedules since the job last made progress: an
    // execution that did (an iterable job that processed items before the refusal) starts it
    // again. Parkings by throttles are counted apart (`limited.t`) and never spend it.
    let before = if made_progress(error) {
        0
    } else {
        job.limited.as_ref().map_or(0, |limited| limited.over_limit)
    };
    let run = before.saturating_add(1);
    let max = over.reschedules().unwrap_or(config.over_limit_reschedules);
    if run > max {
        return Some(OverLimitDecision::Exhausted(reschedule::exhausted_with(
            &over,
            max,
            Some(error),
        )));
    }
    let report = |panic: JobError| {
        config.handle_error(
            &panic,
            &ErrorContext::new("over-limit backoff panicked; using the default").with_job(job),
        );
    };
    let delay = reschedule::over_limit_delay(
        &over,
        job,
        run,
        config.over_limit_backoff.as_ref(),
        config.over_limit_max_delay,
        &report,
    );
    Some(OverLimitDecision::Reschedule(Reschedule::over_limit(
        job,
        over.label(),
        delay,
        run,
        over.place(),
    )))
}

/// Logs (at most once a minute per throttle) that a job of `class` without a throttle key names
/// the throttle `throttle`, whose limits are all per key: nothing applies to it, and it runs.
/// Most often a mistake (the job type has no key), which leaves the resource unprotected.
fn warn_keyless(throttle: &str, class: &str) {
    if warn_now(&format!("keyless:{throttle}")) {
        tracing::warn!(
            throttle,
            class,
            "a job without a throttle key names a throttle whose limits are all per key: it runs \
             unthrottled (give the job a key: `Job::throttle_key`, `throttle_by`, or \
             `throttle_key` per push)"
        );
    }
}

/// Whether `error` (a limiter's refusal) ended an execution that made progress first: an
/// iterable job that processed items before its refusal ([`MadeProgress`]).
fn made_progress(error: &JobError) -> bool {
    error.as_anyhow().downcast_ref::<MadeProgress>().is_some()
}

/// The mark of a limiter's refusal that ended an execution after it made progress (an iterable
/// job that processed `items` items first, `crate::iterable`): its reschedule starts the
/// limiters' budget again (`docs/design/ratelimit.md` §2.5), since the job is being paced, not
/// stuck. Added as the error's context, so that its message tells how far the execution got.
#[derive(Debug, Clone, Copy)]
pub(crate) struct MadeProgress {
    /// Items the execution processed before its refusal.
    pub(crate) items: u64,
}

impl std::fmt::Display for MadeProgress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.items {
            1 => f.write_str("stopped after 1 item"),
            items => write!(f, "stopped after {items} items"),
        }
    }
}

/// The refusal the `over_limit_when` classifier sees in `error` (never for jobs this process
/// does not know: they did not run). A classifier that panics is reported and ignored.
fn classify(config: &Config, error: &JobError, job: &JobPayload) -> Option<OverLimit> {
    let classifier = config.over_limit_when.as_ref()?;
    if error.class() == JobError::UNKNOWN_JOB {
        return None;
    }
    match std::panic::catch_unwind(AssertUnwindSafe(|| classifier(error))) {
        Ok(over) => over,
        Err(panic) => {
            config.handle_error(
                &JobError::panic(panic),
                &ErrorContext::new("over_limit_when panicked").with_job(job),
            );
            None
        }
    }
}

/// Releases the permits held for the job of slot `tid`, after its outcome write (best effort:
/// failures are logged at `debug`; the leases end by themselves).
pub(crate) async fn release(engine: &Engine, tid: &str) {
    release_slots(engine, std::slice::from_ref(&tid.to_owned())).await;
}

/// Releases the permits of every slot in `tids` (after the hard shutdown's requeue, bounded by
/// the caller; or after an outcome).
pub(crate) async fn release_slots(engine: &Engine, tids: &[String]) {
    let held = engine.permits.take(tids).await;
    if held.is_empty() {
        return;
    }
    // One hold time per execution and throttle (an execution holds one lease per concurrency
    // limit of its throttle, all with its token). Queue limits keep no wait line.
    let mut recorded: Vec<(&str, &str)> = Vec::new();
    for permit in held
        .iter()
        .filter(|permit| !permit.owner.starts_with(crate::limit::queue::PREFIX))
    {
        let key = (permit.owner.as_str(), permit.lease.token.as_str());
        if !recorded.contains(&key) {
            recorded.push(key);
            holds::record(&permit.owner, permit.granted.elapsed());
        }
    }
    let leases: Vec<LeaseRef> = held.iter().map(|permit| permit.lease.clone()).collect();
    match scripts::release(&engine.redis, &leases, None).await {
        Ok(results) => {
            for (permit, result) in held.iter().zip(results) {
                if result != ReleaseOutcome::Released
                    && warn_now(&format!("late-release:{}", permit.owner))
                {
                    tracing::warn!(
                        by = %permit.owner,
                        limit = %permit.lease.state,
                        "a job finished after the lease of its permit ended (an overage): \
                         renewals did not keep up"
                    );
                }
            }
        }
        Err(error) => tracing::debug!(
            %error,
            "could not release the permits of a job; their leases end by themselves"
        ),
    }
    for permit in &held {
        wakeups::notify(&permit.lease.state);
    }
}

/// The permits of the engine, held for the round trip of a heartbeat's renewal
/// ([`HeldPermits::renewing`]): a permit released meanwhile waits for the renewal, which could
/// otherwise take back the lease of a job that just finished (a renewal finding a lease gone
/// restores it).
pub(crate) struct Renewing<'a> {
    slots: tokio::sync::MutexGuard<'a, HashMap<String, Vec<HeldPermit>>>,
    /// Every permit held, with its slot and its index in the slot.
    held: Vec<(String, usize, HeldPermit)>,
}

impl HeldPermits {
    /// Locks the permits for a heartbeat's renewal and takes stock of them.
    pub(crate) async fn renewing(&self) -> Renewing<'_> {
        let slots = self.slots.lock().await;
        let held = slots
            .iter()
            .flat_map(|(tid, permits)| {
                permits
                    .iter()
                    .enumerate()
                    .map(move |(index, permit)| (tid.clone(), index, permit.clone()))
            })
            .collect();
        Renewing { slots, held }
    }
}

impl Renewing<'_> {
    /// The `LIMIT_RENEW` calls renewing every permit held, in chunks; the heartbeat sends them
    /// with the unique locks' renewals in one pipeline (`pro::on_beat`).
    pub(crate) fn calls(&self) -> Vec<ScriptCall> {
        let leases: Vec<LeaseRef> = self
            .held
            .iter()
            .map(|(_, _, permit)| permit.lease.clone())
            .collect();
        leases
            .chunks(scripts::CHUNK)
            .map(|chunk| scripts::renew_call(chunk, None))
            .collect()
    }

    /// Applies what the [`Renewing::calls`] answered (one reply per call, in order), then lets
    /// the permits go. A permit whose lease was lost (another holder took its slot) is counted
    /// as an overage by the script, logged, and no longer renewed; its job keeps running. A
    /// failed call is logged at debug level: the next beat tries again.
    pub(crate) fn renewed(mut self, replies: Vec<::redis::RedisResult<::redis::Value>>) {
        let mut lost: Vec<(String, usize)> = Vec::new();
        for (chunk, reply) in self.held.chunks(scripts::CHUNK).zip(replies) {
            let outcomes = match reply
                .map_err(crate::error::Error::from)
                .and_then(scripts::renew_reply)
            {
                Ok(outcomes) => outcomes,
                Err(error) => {
                    tracing::debug!(%error, "could not renew the permits of running jobs; retrying next beat");
                    continue;
                }
            };
            for ((tid, index, permit), outcome) in chunk.iter().zip(outcomes) {
                match outcome {
                    RenewOutcome::Renewed => {}
                    RenewOutcome::Restored => {
                        if warn_now(&format!("restored:{}", permit.owner)) {
                            tracing::warn!(
                                by = %permit.owner,
                                limit = %permit.lease.state,
                                "the lease of a running job's permit had ended; it was taken back"
                            );
                        }
                    }
                    RenewOutcome::Lost => {
                        if warn_now(&format!("lost:{}", permit.owner)) {
                            tracing::warn!(
                                by = %permit.owner,
                                limit = %permit.lease.state,
                                "the lease of a running job's permit was lost: its slot went to \
                                 another job (an overage); the job keeps running"
                            );
                        }
                        lost.push((tid.clone(), *index));
                    }
                }
            }
        }
        let slots = &mut self.slots;
        // Lost leases are neither renewed nor released again: their overage is counted.
        lost.sort_unstable_by(|a, b| b.cmp(a));
        for (tid, index) in lost {
            if let Some(permits) = slots.get_mut(&tid) {
                if index < permits.len() {
                    permits.remove(index);
                }
                if permits.is_empty() {
                    slots.remove(&tid);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn each_execution_holds_its_leases_under_a_token_of_its_own() {
        let jid = "0123456789abcdef01234567";
        let first = execution_token(jid);
        let second = execution_token(jid);
        assert_ne!(first, second);
        for token in [&first, &second] {
            assert!(token.starts_with(&format!("{jid}~")), "{token}");
            assert_eq!(token_jid(token), Some(jid));
        }
        // Custom jids, with the separator inside, keep their jid.
        assert_eq!(token_jid(&execution_token("order~42")), Some("order~42"));
        // A limiter's permit (16 random hex digits) or anything else names no job.
        for token in [
            "0123456789abcdef",
            "",
            "~0123456789abcdef",
            "j~0123456789abcdeg",
            "j~01",
        ] {
            assert_eq!(token_jid(token), None, "{token}");
        }
    }

    fn job_with(limited: Option<Limited>) -> JobPayload {
        let mut job = JobPayload::new("Sync", vec![]);
        job.jid = "a".repeat(24);
        job.limited = limited;
        job
    }

    fn refusal() -> JobError {
        JobError::over_limit(OverLimit::new("w:api").with_retry_after(Duration::from_secs(2)))
    }

    #[test]
    fn throttle_parkings_never_spend_a_limiters_budget() {
        let config = Config::default();
        // Parked 25 times by its throttle: a limiter's refusal is still its first reschedule.
        let mut limited = Limited::new(25, "c:erp", None);
        limited.throttled = 25;
        let job = job_with(Some(limited));
        let Some(OverLimitDecision::Reschedule(reschedule)) = decide(&config, &refusal(), &job)
        else {
            panic!("rescheduled");
        };
        let after = reschedule.limited;
        assert_eq!((after.n, after.throttled, after.over_limit), (26, 25, 1));
        assert_eq!(after.by, "w:api");
        // Twenty limiter reschedules already: the failure path, with the limiter named.
        let mut limited = Limited::new(45, "w:api", None);
        limited.throttled = 25;
        limited.over_limit = 20;
        let job = job_with(Some(limited));
        let Some(OverLimitDecision::Exhausted(error)) = decide(&config, &refusal(), &job) else {
            panic!("exhausted");
        };
        assert!(error.is_over_limit());
        assert!(
            error
                .message()
                .starts_with("rescheduled 20 times by `w:api`"),
            "{}",
            error.message()
        );
    }

    #[test]
    fn progress_starts_the_limiters_budget_again() {
        let config = Config::default();
        let mut limited = Limited::new(40, "w:api", None);
        limited.over_limit = 20;
        let job = job_with(Some(limited));
        // An iterable execution that processed items before its refusal: rescheduled, its
        // count back to 1.
        let paced = refusal().context(MadeProgress { items: 5 });
        assert!(
            paced
                .message()
                .starts_with("stopped after 5 items: over the limit `w:api`")
        );
        let Some(OverLimitDecision::Reschedule(reschedule)) = decide(&config, &paced, &job) else {
            panic!("rescheduled");
        };
        assert_eq!(
            (reschedule.limited.n, reschedule.limited.over_limit),
            (41, 1)
        );
        // Without progress, the same job is past its budget.
        assert!(matches!(
            decide(&config, &refusal(), &job),
            Some(OverLimitDecision::Exhausted(_))
        ));
    }

    #[test]
    fn keyless_jobs_of_per_key_throttles_are_reported() {
        let logs = crate::limit::test_logs::warnings(|| {
            warn_keyless("engine-tests-keyless", "Export");
            warn_keyless("engine-tests-keyless", "Export");
        });
        assert_eq!(logs.matches("runs unthrottled").count(), 1, "{logs}");
        assert!(
            logs.contains("engine-tests-keyless") && logs.contains("Export"),
            "{logs}"
        );
    }

    #[test]
    fn leases_outlast_three_heartbeats() {
        let heartbeat = Duration::from_secs(10);
        let short = crate::limit::Throttle::new("erp")
            .concurrency(1)
            .lease(Duration::from_secs(5));
        let error = short.check_renewals(heartbeat).unwrap_err().to_string();
        assert!(
            error.contains("invalid throttle `erp`: its lease (5s) is shorter than 3 heartbeat"),
            "{error}"
        );
        short.check_renewals(Duration::from_secs(1)).unwrap();
        // Rate limits hold no lease.
        crate::limit::Throttle::new("api")
            .window(5, Duration::from_secs(1))
            .lease(Duration::from_secs(1))
            .check_renewals(heartbeat)
            .unwrap();
        crate::limit::Throttle::new("erp")
            .concurrency(1)
            .check_renewals(Duration::from_secs(20))
            .unwrap();
    }

    #[test]
    fn hold_times_are_remembered_per_throttle() {
        // Nothing measured: the default, doubled by each earlier parking (up to 64 times).
        let name = "engine-tests-hold";
        assert_eq!(holds::expected(name, 0), Duration::from_millis(25));
        assert_eq!(holds::expected(name, 2), Duration::from_millis(100));
        assert_eq!(holds::expected(name, 30), Duration::from_millis(1_600));
        // Measured: a decaying average, whatever the parkings.
        holds::record(name, Duration::from_millis(1_000));
        assert_eq!(holds::expected(name, 5), Duration::from_millis(1_000));
        holds::record(name, Duration::from_millis(2_000));
        assert_eq!(holds::expected(name, 0), Duration::from_millis(1_200));
    }
}
