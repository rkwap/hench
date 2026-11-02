//! Unique jobs in the testing modes: the in-memory locks of a fake or inline client
//! (`UniqueTable`, shared by the client, its clones and its collecting clients;
//! `docs/design/unique.md` §2.12). The rules are the scripts', ported:
//!
//! * a push takes the lock, is a duplicate of another holder (or replaces a holder captured in
//!   the schedule, with [`OnConflict::Replace`](crate::OnConflict::Replace)), or is already
//!   pushed when its own jid holds the lock;
//! * a `start` lock is released when `perform` starts;
//! * a success, a skipped batch member and a failure release the lock (testing clients do not
//!   retry: a failed job is gone), except an `expired` lock, which lives its time to live;
//! * an interrupted job pushed back to the client keeps its lock (or takes it back if free).
//!
//! Testing clients enforce locks by default ([`Testing::unique_jobs`]); each client has its
//! own table, so tests cannot leak locks into each other. Time is tokio's: under
//! `tokio::time::pause`, `advance` expires locks.
//!
//! [`Testing::unique_jobs`]: crate::testing::Testing::unique_jobs

use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use serde_json::Value;
use tokio::time::Instant;

use crate::client::{Client, ClientMode, PushResult};
use crate::job::Job;
use crate::payload::{JobPayload, now_secs_f64};
use crate::pro::{Classified, Fetched};
use crate::unique::{LockRef, OnConflict, UniqueOption};

/// The in-memory locks of a testing client, its clones and its collecting clients.
#[derive(Debug, Default)]
pub(crate) struct UniqueTable {
    state: Mutex<TableState>,
}

#[derive(Debug, Default)]
struct TableState {
    /// The locks, by digest.
    locks: HashMap<String, FakeLock>,
    /// The jids of interrupted jobs being pushed back: their push is admitted, holding the lock.
    pushing_back: HashSet<String>,
}

/// One in-memory lock.
#[derive(Debug, Clone)]
struct FakeLock {
    jid: String,
    expires_at: Instant,
    /// The run-at time of a holder captured as scheduled (a debounce push may replace it).
    scheduled_at: Option<f64>,
}

impl UniqueTable {
    fn state(&self) -> MutexGuard<'_, TableState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Releases `lock` if its jid holds it: `true` when it did.
    pub(crate) fn release(&self, lock: &LockRef) -> bool {
        let mut state = self.state();
        state.forget_expired(&lock.digest, Instant::now());
        match state.locks.get(&lock.digest) {
            Some(held) if held.jid == lock.jid => {
                state.locks.remove(&lock.digest);
                true
            }
            _ => false,
        }
    }

    /// `hu_hold` on the in-memory lock: keeps `lock` for at least `ttl` if its jid holds it,
    /// takes it if it is free, leaves it to another holder (for the model-based tests).
    #[cfg(all(test, feature = "test-util"))]
    pub(crate) fn hold(&self, lock: &LockRef, ttl: Duration) {
        self.state().hold(lock, ttl, None, Instant::now());
    }

    /// The jid holding the lock of `digest`, if any.
    pub(crate) fn holder(&self, digest: &str) -> Option<String> {
        let mut state = self.state();
        state.forget_expired(digest, Instant::now());
        state.locks.get(digest).map(|held| held.jid.clone())
    }

    /// Forgets the lock of `digest` (its time to live ended), for the model-based tests.
    #[cfg(all(test, feature = "test-util"))]
    pub(crate) fn forget(&self, digest: &str) {
        self.state().locks.remove(digest);
    }

    /// Forgets every lock.
    pub(crate) fn clear(&self) {
        let mut state = self.state();
        state.locks.clear();
        state.pushing_back.clear();
    }
}

impl TableState {
    /// Forgets the lock of `digest` if its time to live ended.
    fn forget_expired(&mut self, digest: &str, now: Instant) {
        if self
            .locks
            .get(digest)
            .is_some_and(|held| held.expires_at <= now)
        {
            self.locks.remove(digest);
        }
    }

    /// Takes the lock of `jid` for `ttl`.
    fn take(&mut self, lock: &LockRef, ttl: Duration, scheduled_at: Option<f64>, now: Instant) {
        self.locks.insert(
            lock.digest.clone(),
            FakeLock {
                jid: lock.jid.clone(),
                expires_at: now + ttl,
                scheduled_at,
            },
        );
    }

    /// `hu_hold`: keeps the lock of `jid` for at least `ttl` (never shortened) and records
    /// `scheduled_at`, takes it if it is free, leaves it to another holder.
    fn hold(&mut self, lock: &LockRef, ttl: Duration, scheduled_at: Option<f64>, now: Instant) {
        self.forget_expired(&lock.digest, now);
        match self.locks.get_mut(&lock.digest) {
            Some(held) if held.jid == lock.jid => {
                held.expires_at = held.expires_at.max(now + ttl);
                held.scheduled_at = scheduled_at;
            }
            Some(_) => {}
            None => self.take(lock, ttl, scheduled_at, now),
        }
    }
}

/// How long `job`'s lock lives from now: `for`, plus the delay of a scheduled job.
fn ttl_of(lock: &LockRef, scheduled_at: Option<f64>) -> Duration {
    let delay = scheduled_at
        .map(|at| at - now_secs_f64())
        .filter(|delay| delay.is_finite() && *delay > 0.0)
        .map_or(Duration::ZERO, Duration::from_secs_f64);
    lock.ttl + delay
}

/// Takes the in-memory locks of `jobs` (pushed through a fake or inline client, after client
/// middleware), with the push script's rules; one result per job, in order.
pub(crate) fn admit(client: &Client, jobs: &[JobPayload]) -> Vec<PushResult> {
    let mut replaced = Vec::new();
    let results = {
        let mut state = client.tables().unique.state();
        jobs.iter()
            .map(|job| admit_one(client, &mut state, job, &mut replaced))
            .collect()
    };
    for (class, jid) in replaced {
        remove_captured(client, &class, &jid);
    }
    results
}

fn admit_one(
    client: &Client,
    state: &mut TableState,
    job: &JobPayload,
    replaced: &mut Vec<(String, String)>,
) -> PushResult {
    let Some(lock) = LockRef::from_payload_quietly(job) else {
        return PushResult::Pushed;
    };
    let on_conflict = job
        .unique
        .as_ref()
        .and_then(UniqueOption::settings)
        .and_then(|unique| unique.on_conflict)
        .unwrap_or_default();
    // `raw_push` keeps `at` only on jobs scheduled in the future.
    let scheduled_at = job.at;
    let ttl = ttl_of(&lock, scheduled_at);
    let now = Instant::now();
    state.forget_expired(&lock.digest, now);
    if state.pushing_back.remove(&lock.jid) {
        // An interrupted job pushed back: written whatever the lock, which it holds or takes
        // back if free (PUSH_BACK_UNIQUE).
        state.hold(&lock, ttl, scheduled_at, now);
        return PushResult::Pushed;
    }
    let holder = match state.locks.get(&lock.digest) {
        None => {
            state.take(&lock, ttl, scheduled_at, now);
            return PushResult::Pushed;
        }
        Some(held) if held.jid == lock.jid => return PushResult::AlreadyPushed,
        Some(held) => held.clone(),
    };
    if on_conflict == OnConflict::Replace
        && holder.scheduled_at.is_some()
        && captured_as_scheduled(client, &lock.class, &holder.jid)
    {
        state.take(&lock, ttl, scheduled_at, now);
        replaced.push((lock.class.clone(), holder.jid.clone()));
        return PushResult::Replaced {
            replaced: holder.jid,
        };
    }
    tracing::debug!(
        class = %lock.class,
        jid = %lock.jid,
        holder = %holder.jid,
        digest = %lock.digest,
        "duplicate job not pushed"
    );
    PushResult::Duplicate { holder: holder.jid }
}

/// Whether a fake client captured the job `jid` of `class` as scheduled, outside a batch (inline
/// clients run every job at once: nothing waits to be replaced; a batch member is never
/// replaced, as in Redis: its batch would wait for it forever). Every captured job is found,
/// however many share its run-at time, as the push script finds the member its lock records.
fn captured_as_scheduled(client: &Client, class: &str, jid: &str) -> bool {
    client.is_fake()
        && client
            .jobs_for(class)
            .iter()
            .any(|job| job.jid == jid && job.at.is_some() && job.bid.is_none())
}

/// Removes the captured job `jid` of `class` from a fake client (the holder a debounce push
/// replaced). The fake store has no removal by jid, so the other captured jobs of the class are
/// captured again, in their order (non-scheduled ones get a fresh `enqueued_at`).
fn remove_captured(client: &Client, class: &str, jid: &str) {
    let ClientMode::Fake(store) = client.mode() else {
        return;
    };
    let kept: Vec<JobPayload> = client
        .jobs_for(class)
        .into_iter()
        .filter(|job| job.jid != jid)
        .collect();
    client.clear_for(class);
    // `fake_push` never waits: it completes on its first poll.
    if let Err(error) = futures::executor::block_on(crate::testing::fake_push(store, kept)) {
        tracing::warn!(%error, class, "could not keep the jobs captured beside a replaced job");
    }
}

/// Releases or keeps the in-memory lock of a job the testing executor ran (the job as fetched),
/// after the batch table's accounting (the processor's order). Testing clients do not retry: a
/// failed job is gone, and so is its lock.
pub(crate) fn after(client: &Client, fetched: &Fetched, classified: &Classified) {
    let Some(lock) = &fetched.lock else {
        return;
    };
    if !lock.follows_the_job() {
        return;
    }
    let table = &client.tables().unique;
    match classified {
        Classified::Success | Classified::Skipped | Classified::Failure(_) => {
            table.release(lock);
        }
        Classified::Interrupted => {
            let now = Instant::now();
            let mut state = table.state();
            state.hold(lock, lock.ttl, None, now);
            if client.is_fake() {
                // A fake client captures it again through a push: let it in.
                state.pushing_back.insert(lock.jid.clone());
            }
        }
        Classified::OverLimit(reschedule) | Classified::Throttled(reschedule) => {
            // Kept captured, parked until its slot: the lock stays, like PUSH_BACK_UNIQUE.
            let now = Instant::now();
            table
                .state()
                .hold(lock, lock.ttl + reschedule.delay, None, now);
        }
    }
}

/// Helpers of fake and inline clients about unique jobs (see [`crate::unique`]).
///
/// They panic when called on another kind of client, since that is a mistake in the test.
impl Client {
    fn unique_table(&self, method: &str) -> &UniqueTable {
        match self.mode() {
            ClientMode::Fake(_) | ClientMode::Inline(_) => &self.tables().unique,
            _ => panic!(
                "Client::{method} is only available on fake and inline clients (created by \
                 Client::fake, Client::inline or Testing), not on {self:?}"
            ),
        }
    }

    /// The jid holding the in-memory lock that a push of `J` with `args` would take, if any:
    /// the digest is computed as a push computes it (the job type's queue and options over this
    /// client's default options, then [`Job::unique_key`] or the arguments).
    ///
    /// # Panics
    ///
    /// If this is not a fake or inline client.
    pub fn unique_holder<J: Job>(&self, args: &J::Args) -> Option<String> {
        let table = self.unique_table("unique_holder");
        let digest = crate::unique::digest_of::<J>(self.default_job_options(), None, args).ok()?;
        table.holder(&digest)
    }

    /// The jid holding the in-memory lock of class `class`, queue `queue` (`None`: a lock
    /// across queues) and key `key` (the arguments, as a JSON array, unless the job has a key of
    /// its own), if any.
    ///
    /// # Panics
    ///
    /// If this is not a fake or inline client.
    pub fn unique_holder_for(
        &self,
        class: &str,
        queue: Option<&str>,
        key: &Value,
    ) -> Option<String> {
        let table = self.unique_table("unique_holder_for");
        let digest = crate::unique::digest(class, queue, key).ok()?;
        table.holder(&digest)
    }

    /// Forgets every in-memory lock of this client (and its clones). [`Client::clear_all`] and
    /// the other clearing helpers leave locks alone, as `Queue::clear` does in Redis.
    ///
    /// # Panics
    ///
    /// If this is not a fake or inline client.
    pub fn clear_unique_locks(&self) {
        self.unique_table("clear_unique_locks").clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::unique::{Unique, UniqueUntil};
    use serde_json::json;

    fn job(jid: &str, until: UniqueUntil) -> JobPayload {
        let mut job = JobPayload::new("Sync", vec![json!(1)]);
        job.jid = jid.to_owned();
        job.queue = "default".to_owned();
        let mut unique = Unique::new(Duration::from_secs(60)).until(until);
        unique.digest = Some("d1".to_owned());
        job.unique = Some(unique.into());
        job
    }

    #[tokio::test(start_paused = true)]
    async fn pushes_take_hold_and_release_locks() {
        let client = Client::fake();
        let first = job("aaaaaaaaaaaaaaaaaaaaaaaa", UniqueUntil::Success);
        let second = job("bbbbbbbbbbbbbbbbbbbbbbbb", UniqueUntil::Success);
        assert_eq!(
            admit(&client, &[first.clone(), second.clone(), first.clone()]),
            [
                PushResult::Pushed,
                PushResult::Duplicate {
                    holder: first.jid.clone()
                },
                PushResult::AlreadyPushed
            ]
        );
        let table = &client.tables().unique;
        assert_eq!(table.holder("d1").as_deref(), Some(first.jid.as_str()));
        // Another job never releases the lock; its holder does.
        let fetched = Fetched::from_payload(&second);
        after(&client, &fetched, &Classified::Success);
        assert_eq!(table.holder("d1").as_deref(), Some(first.jid.as_str()));
        after(
            &client,
            &Fetched::from_payload(&first),
            &Classified::Success,
        );
        assert_eq!(table.holder("d1"), None);
        // Locks expire with their time to live.
        assert_eq!(
            admit(&client, std::slice::from_ref(&second)),
            [PushResult::Pushed]
        );
        tokio::time::advance(Duration::from_secs(61)).await;
        assert_eq!(table.holder("d1"), None);
        assert_eq!(
            admit(&client, std::slice::from_ref(&first)),
            [PushResult::Pushed]
        );
        client.clear_unique_locks();
        assert_eq!(table.holder("d1"), None);
    }

    #[tokio::test(start_paused = true)]
    async fn expired_locks_ignore_outcomes_and_interrupted_jobs_keep_theirs() {
        let client = Client::fake();
        let expired = job("aaaaaaaaaaaaaaaaaaaaaaaa", UniqueUntil::Expired);
        admit(&client, std::slice::from_ref(&expired));
        after(
            &client,
            &Fetched::from_payload(&expired),
            &Classified::Success,
        );
        let table = &client.tables().unique;
        assert_eq!(table.holder("d1").as_deref(), Some(expired.jid.as_str()));
        client.clear_unique_locks();
        let interrupted = job("bbbbbbbbbbbbbbbbbbbbbbbb", UniqueUntil::Success);
        admit(&client, std::slice::from_ref(&interrupted));
        after(
            &client,
            &Fetched::from_payload(&interrupted),
            &Classified::Interrupted,
        );
        // Its push back is let in once, holding the lock.
        assert_eq!(
            admit(&client, std::slice::from_ref(&interrupted)),
            [PushResult::Pushed]
        );
        assert_eq!(
            admit(&client, std::slice::from_ref(&interrupted)),
            [PushResult::AlreadyPushed]
        );
    }
}
