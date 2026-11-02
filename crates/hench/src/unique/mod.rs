//! Unique jobs: at most one job per key (the *digest*) waiting or running at a time, like
//! Sidekiq Enterprise's unique jobs (the design documents: [unique.md], and §9g of
//! [DESIGN.md]).
//!
//! A job is unique when its options make it so ([`JobOptions::unique_for`] and the other
//! `unique*` setters, or [`Unique`] given whole). Pushing it takes a **lock** in Redis named
//! after its digest, a hash of its class, its queue and its arguments (or a part of them, or an
//! explicit key); while the lock exists, a push of a job with the same digest is a
//! **duplicate**: it is not pushed and [`Client::push`](crate::Client::push) and the typed
//! pushes return `Ok(None)` (see [`OnConflict`] for the other strategies). Every lock has a
//! time to live, so a lock never outlives its job by more than that.
//!
//! The lock is taken by the same Lua script that writes the job, and held or released by the
//! scripts that write the job's outcome: a push that fails leaves no lock, a lock is never
//! released while its job's outcome is unwritten, and a lost reply resent with the same jid is
//! recognized and not pushed twice.
//!
//! ```no_run
//! use std::time::Duration;
//! use hench::prelude::*;
//!
//! #[derive(Default)]
//! struct SyncAddress;
//!
//! impl Job for SyncAddress {
//!     const NAME: &'static str = "SyncAddress";
//!     type Args = (i64, String);
//!
//!     fn options() -> JobOptions {
//!         JobOptions::new().unique_for(Duration::from_secs(600))
//!     }
//!
//!     // Duplicates are jobs for the same user, whatever the reason.
//!     fn unique_key((user_id, _reason): &Self::Args) -> Option<serde_json::Value> {
//!         Some(serde_json::json!([user_id]))
//!     }
//!
//!     async fn perform(&self, _ctx: &JobContext, _args: Self::Args) -> Result<(), JobError> {
//!         Ok(())
//!     }
//! }
//!
//! # async fn demo(client: &Client) -> hench::Result<()> {
//! let first = SyncAddress::perform_async(client, (42, "form".into())).await?;
//! assert!(first.is_some());
//! // The same user while the first job waits or runs: not pushed.
//! assert_eq!(SyncAddress::perform_async(client, (42, "retry".into())).await?, None);
//! // Debounce: run 30 seconds after the last push, with its arguments.
//! SyncAddress::set()
//!     .delay(Duration::from_secs(30))
//!     .on_conflict(OnConflict::Replace)
//!     .perform_async(client, (7, "edit".into()))
//!     .await?;
//! # Ok(()) }
//! ```
//!
//! # Lock lifetimes
//!
//! | Policy ([`UniqueUntil`]) | The lock is released | Use |
//! |---|---|---|
//! | `Success` (default) | when the job succeeds (or server middleware skips it), dies, is discarded, or is deleted or killed through the API; kept, and extended, while it waits for a retry; renewed by the heartbeat while it runs | at most one pending or running copy |
//! | `Start` | when `perform` starts (after server middleware); taken back, if free, when the job waits again (a retry, a push back) | at most one waiting copy |
//! | `Expired` | only when its time to live ends (or by an operator) | at most one per window |
//!
//! The time to live is `unique_for` (whole seconds, at least one, mandatory), plus the delay of
//! a scheduled job, of a retry or of a job parked by a rate limit. Choose it at least as long as
//! the worst wait in the queue, at least three heartbeat intervals for long jobs, and at least
//! ten minutes where a duplicate after a crash matters: a lock whose time to live ends before
//! its job lets a duplicate in (a **lapse**, counted per class, see
//! [`UniqueLocks::events`]).
//!
//! Uniqueness is not concurrency control: with `Start`, a waiting copy can run beside a
//! running one. For "never two running", combine `Start` with a concurrency limit of one per key
//! (see [`crate::limit`]).
//!
//! # Where the locks are
//!
//! A lock is the hash `unique:<digest>` (holder jid, class, queue, policy, `for`, lock time,
//! and the score of a holder waiting in the schedule), indexed by the sorted set `unique-locks`
//! (scored by expiry). [`Api::unique_locks`](crate::api::Api::unique_locks) lists, finds and
//! releases them; the dashboard has a Locks tab and the `hench` command `hench locks`.
//!
//! Testing clients ([`Client::fake`](crate::Client::fake), [`Client::inline`](crate::Client::inline))
//! enforce locks in memory, per client: turn it off with
//! [`Testing::unique_jobs(false)`](crate::testing::Testing::unique_jobs).
//!
//! [`JobOptions::unique_for`]: crate::JobOptions::unique_for
//! [unique.md]: https://github.com/rkwap/hench/blob/v0.1.0/docs/design/unique.md
//! [DESIGN.md]: https://github.com/rkwap/hench/blob/v0.1.0/docs/DESIGN.md

pub(crate) mod api;
pub(crate) mod endpoint;
pub(crate) mod engine;
pub(crate) mod fake;
#[path = "../api/unique.rs"]
mod locks;
pub(crate) mod lua;
#[cfg(all(test, feature = "test-util"))]
mod parity_tests;
pub(crate) mod push;
mod types;

use std::time::Duration;

use serde_json::Value;

use crate::error::{Error, Result};
use crate::payload::JobPayload;

pub(crate) use self::endpoint::EarlyRelease;
pub use self::locks::{
    HolderPlace, LOCATE_LIMIT, UniqueEventCounts, UniqueEvents, UniqueLock, UniqueLocks,
};
pub(crate) use self::types::{LockAction, LockOp, LockRef};
pub use self::types::{OnConflict, PushOutcome, Unique, UniqueOption, UniqueRef, UniqueUntil};

/// The version tag at the head of every digest's input. Pinned forever: changing it changes
/// every digest, so that jobs pushed before and after an upgrade would not be duplicates.
pub(crate) const DIGEST_VERSION: &str = "hench-unique/1";

/// The version of the stored lock format this process understands, for the heartbeat's
/// `features` map (a process without it never releases locks: the dashboard warns while locks
/// exist).
pub(crate) const FORMAT: Option<u32> = Some(1);

/// The digest of a lock: the SHA-256, in base64url without padding (43 characters), of the
/// canonical JSON of `["hench-unique/1", class, queue or null, key]` ([unique.md]
/// §2.4). `queue` is `None` for locks across queues; `key` is the explicit key, the value of
/// [`Job::unique_key`](crate::Job::unique_key), or the job's arguments.
///
/// Public so that callers can find a job's lock. Fails with
/// [`Error::InvalidJob`] for a key nested more than 128 levels deep.
///
/// ```
/// use serde_json::json;
///
/// let digest = hench::unique::digest("Sync", Some("default"), &json!([42])).unwrap();
/// assert_eq!(digest.len(), 43);
/// // Object members are sorted, whatever their order.
/// assert_eq!(
///     hench::unique::digest("Sync", None, &json!({"b": 1, "a": 2})).unwrap(),
///     hench::unique::digest("Sync", None, &json!({"a": 2, "b": 1})).unwrap(),
/// );
/// ```
///
/// [unique.md]: https://github.com/rkwap/hench/blob/v0.1.0/docs/design/unique.md
pub fn digest(class: &str, queue: Option<&str>, key: &Value) -> Result<String> {
    crate::canonical::digest(&serde_json::json!([DIGEST_VERSION, class, queue, key]))
}

/// The canonical JSON [`digest`] hashes: no whitespace, object members sorted by the UTF-8
/// bytes of their keys at every level, everything else as `serde_json` writes it. Fails with
/// [`Error::InvalidJob`] beyond 128 levels of nesting.
pub fn canonical_json(value: &Value) -> Result<Vec<u8>> {
    crate::canonical::canonical_json(value)
}

/// The unique step of `Client::normalize` (`docs/design/unique.md` §2.6.5), after the options
/// are merged and the queue, jid and `created_at` are set, once the job has its arguments.
/// `enabled` is the client's switch ([`Client::with_unique_jobs`](crate::Client::with_unique_jobs)).
///
/// * Not unique (no option, or [`UniqueOption::Disabled`]): the option is removed.
/// * Unique, without a digest yet (a new push): with the switch off, the option is removed (no
///   lock); otherwise `unique_for` is checked (required, at least one second, below 1e9
///   seconds; stored in whole seconds), `until` defaults to `success`, and the digest is
///   computed from the explicit key (or the arguments), the class and the queue (or none,
///   across queues).
/// * Unique with a digest (a job pushed before, moved by the scheduler or the API, or pushed
///   back; a raw payload naming its digest): the digest is checked and kept, whatever the
///   switch says, so that the engine keeps holding and releasing its lock.
///
/// The push-time fields `key` and `across_queues` are removed; `on_conflict` stays until the
/// job is stored.
pub(crate) fn normalize(job: &mut JobPayload, enabled: bool) -> Result<()> {
    let Some(option) = job.unique.take() else {
        return Ok(());
    };
    let UniqueOption::Enabled(mut unique) = option else {
        return Ok(());
    };
    if !enabled && unique.digest.is_none() {
        return Ok(());
    }
    let secs = unique
        .ttl
        .ok_or_else(|| {
            invalid(
                "a unique job needs `unique_for` (`#[hench::job(unique_for = <seconds>)]`, \
                 JobOptions::unique_for, Unique::new, or `for` in the payload's `unique`)",
            )
        })?
        .as_secs();
    if secs == 0 {
        return Err(invalid("`unique_for` must be at least one second"));
    }
    if secs >= types::MAX_UNIQUE_FOR_SECS {
        return Err(invalid(
            "`unique_for` must be a relative amount of time below 1000000000 seconds",
        ));
    }
    unique.ttl = Some(Duration::from_secs(secs));
    unique.until = Some(unique.until.unwrap_or_default());
    match &unique.digest {
        Some(digest) => {
            if !types::valid_digest(digest) {
                return Err(invalid(&format!(
                    "the digest of a unique job must be 1 to 128 characters of A-Z, a-z, 0-9, `_` \
                     and `-`, not {digest:?}"
                )));
            }
        }
        None => {
            let queue = (!unique.across_queues.unwrap_or(false)).then_some(job.queue.as_str());
            let computed = match &unique.key {
                Some(key) => self::digest(&job.class, queue, key)?,
                None => self::digest(&job.class, queue, &Value::Array(job.args.clone()))?,
            };
            unique.digest = Some(computed);
        }
    }
    unique.key = None;
    unique.across_queues = None;
    job.unique = Some(UniqueOption::Enabled(unique));
    Ok(())
}

fn invalid(message: &str) -> Error {
    Error::InvalidJob(message.to_owned())
}

/// The digest a typed push of `J` with `args` gets from a client whose default job options are
/// `defaults`: the queue (`queue`, else that of the job type's options over the defaults, else
/// `default`) and `across_queues` of those options, their key, else [`Job::unique_key`], else
/// the arguments. A push with other per-push options (an explicit key) may differ.
pub(crate) fn digest_of<J: crate::Job>(
    defaults: &crate::JobOptions,
    queue: Option<&str>,
    args: &J::Args,
) -> Result<String> {
    let options = defaults.merge(&J::options());
    let unique = options.unique.as_ref().and_then(UniqueOption::settings);
    let queue = queue.or(options.queue.as_deref()).unwrap_or("default");
    let across = unique
        .and_then(|unique| unique.across_queues)
        .unwrap_or(false);
    let key = match unique.and_then(|unique| unique.key.clone()) {
        Some(key) => key,
        None => match J::unique_key(args) {
            Some(key) => key,
            None => Value::Array(crate::job::args_to_values(J::NAME, args)?),
        },
    };
    digest(J::NAME, (!across).then_some(queue), &key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use serde_json::json;

    fn job(args: Vec<Value>, unique: Unique) -> JobPayload {
        let mut job = JobPayload::new("Sync", args);
        job.jid = "0123456789abcdef01234567".to_owned();
        job.queue = "default".to_owned();
        job.unique = Some(unique.into());
        job
    }

    fn normalized(mut job: JobPayload) -> Unique {
        normalize(&mut job, true).unwrap();
        job.unique
            .and_then(|unique| unique.settings().cloned())
            .unwrap()
    }

    #[test]
    fn digests_are_pinned() {
        // Golden vectors (D6): every lock in Redis depends on them, so they never change.
        let input = br#"["hench-unique/1","Sync","default",[42]]"#;
        assert_eq!(
            digest("Sync", Some("default"), &json!([42])).unwrap(),
            crate::canonical::sha256_b64url(input)
        );
        let across = br#"["hench-unique/1","Sync",null,{"a":2,"b":[1,{"c":null}]}]"#;
        assert_eq!(
            digest("Sync", None, &json!({"b": [1, {"c": null}], "a": 2})).unwrap(),
            crate::canonical::sha256_b64url(across)
        );
        for (class, queue, key, expected) in [
            (
                "Sync",
                Some("default"),
                json!([42]),
                "TxtnAYS12bB1uE_EfAx9KKWbEBBnRKEpg7CRfxrokpM",
            ),
            (
                "Sync",
                None,
                json!({"b": [1, {"c": null}], "a": 2}),
                "EvFSG9aKIFckP6JoGqHd4afyXZ4B3SjA8xcaB8VZI3Q",
            ),
            (
                "billing::Charge",
                Some("critical"),
                json!(["acct-1", 1999, 1.5, true]),
                "R6O55edAPaflGY1XJjU6wTfoCrdRWEU9NGjIBpGDEXM",
            ),
            (
                "Mailer",
                Some("default"),
                json!([]),
                "o0itX_ieiQ_K8Mfl0qLj3eL1ki-qCtrYHSbsAVON97M",
            ),
            (
                "Ünïcödé",
                Some("q"),
                json!(["user:42", {"é": "\u{1F600}"}]),
                "YRfZ5xsy4cOmaUMo59DHhFqT4xVk6RPOVoofcmXadR8",
            ),
        ] {
            assert_eq!(
                digest(class, queue, &key).unwrap(),
                expected,
                "{class} {queue:?} {key}"
            );
        }
        assert_eq!(
            canonical_json(&json!({"b": 1, "a": 2})).unwrap(),
            br#"{"a":2,"b":1}"#.to_vec()
        );
    }

    #[test]
    fn digests_tell_values_apart() {
        // D1, D3, D4: any argument, its type, the queue (unless across queues) count.
        let base = digest("Sync", Some("default"), &json!([42])).unwrap();
        for other in [
            digest("Sync", Some("default"), &json!(["42"])).unwrap(),
            digest("Sync", Some("default"), &json!([42.0])).unwrap(),
            digest("Sync", Some("default"), &json!([43])).unwrap(),
            digest("Sync", Some("other"), &json!([42])).unwrap(),
            digest("Other", Some("default"), &json!([42])).unwrap(),
            digest("Sync", None, &json!([42])).unwrap(),
        ] {
            assert_ne!(base, other);
        }
        let values = [
            json!(null),
            json!(1),
            json!(1.0),
            json!(true),
            json!("1"),
            json!([null]),
            json!([]),
            json!({}),
        ];
        let digests: std::collections::HashSet<String> = values
            .iter()
            .map(|value| digest("A", None, value).unwrap())
            .collect();
        assert_eq!(digests.len(), values.len());
    }

    #[test]
    fn normalization_computes_the_digest_once() {
        let unique = normalized(job(vec![json!(42)], Unique::new(Duration::from_secs(600))));
        assert_eq!(
            unique.digest.as_deref(),
            Some(
                digest("Sync", Some("default"), &json!([42]))
                    .unwrap()
                    .as_str()
            )
        );
        assert_eq!(unique.until, Some(UniqueUntil::Success));
        assert_eq!(unique.on_conflict, None);
        // D4, D5: across queues; an explicit key wins over the arguments.
        let across = normalized(job(
            vec![json!(42)],
            Unique::new(Duration::from_secs(600)).across_queues(true),
        ));
        assert_eq!(
            across.digest,
            Some(digest("Sync", None, &json!([42])).unwrap())
        );
        assert_eq!(across.across_queues, None);
        let keyed = normalized(job(
            vec![json!(42), json!("noise")],
            Unique::new(Duration::from_secs(600)).key("user:42"),
        ));
        assert_eq!(
            keyed.digest,
            Some(digest("Sync", Some("default"), &json!("user:42")).unwrap())
        );
        assert_eq!(keyed.key, None);
        // A job that has a digest keeps it (the scheduler, API moves, push backs), even when
        // its arguments or key changed since.
        let mut pushed = job(vec![json!(1)], keyed.clone());
        pushed.args = vec![json!(2)];
        normalize(&mut pushed, true).unwrap();
        assert_eq!(
            pushed.unique.as_ref().and_then(|u| u.settings()),
            Some(&keyed)
        );
        // Whole seconds are stored.
        let fractional = normalized(job(vec![], Unique::new(Duration::from_millis(1_500))));
        assert_eq!(fractional.ttl, Some(Duration::from_secs(1)));
    }

    #[test]
    fn normalization_checks_the_settings() {
        // D7, D10.
        for (unique, message) in [
            (Unique::default(), "needs `unique_for`"),
            (
                Unique::new(Duration::from_millis(999)),
                "at least one second",
            ),
            (
                Unique::new(Duration::from_secs(1_000_000_000)),
                "below 1000000000",
            ),
        ] {
            let mut job = job(vec![], unique);
            let Err(Error::InvalidJob(error)) = normalize(&mut job, true) else {
                panic!("expected an invalid job");
            };
            assert!(error.contains(message), "{error}");
        }
        let mut supplied = Unique::new(Duration::from_secs(60));
        supplied.digest = Some("not valid!".to_owned());
        assert!(matches!(
            normalize(&mut job(vec![], supplied), true),
            Err(Error::InvalidJob(_))
        ));
        let mut deep = json!(1);
        for _ in 0..130 {
            deep = json!([deep]);
        }
        assert!(matches!(
            normalize(
                &mut job(vec![deep], Unique::new(Duration::from_secs(60))),
                true
            ),
            Err(Error::InvalidJob(_))
        ));
        assert_eq!(
            normalized(job(vec![], Unique::new(Duration::from_secs(999_999_999)))).ttl,
            Some(Duration::from_secs(999_999_999))
        );
    }

    #[test]
    fn disabled_uniqueness_and_the_switch_drop_the_option() {
        let mut disabled = job(vec![], Unique::new(Duration::from_secs(60)));
        disabled.unique = Some(UniqueOption::Disabled);
        normalize(&mut disabled, true).unwrap();
        assert_eq!(disabled.unique, None);
        let mut off = job(vec![], Unique::new(Duration::from_secs(60)));
        normalize(&mut off, false).unwrap();
        assert_eq!(off.unique, None);
        // A job that already has a digest keeps it: its lock must still be released.
        let mut stored = Unique::new(Duration::from_secs(60)).until(UniqueUntil::Start);
        stored.digest = Some("d1".to_owned());
        let mut moved = job(vec![], stored.clone());
        normalize(&mut moved, false).unwrap();
        assert_eq!(moved.unique, Some(stored.into()));
        let mut plain = JobPayload::new("Plain", vec![]);
        normalize(&mut plain, true).unwrap();
        assert_eq!(plain.unique, None);
    }

    proptest! {
        // D2, D12: the digest is a function of the canonical form: object members in any
        // insertion order give the same digest.
        #[test]
        fn digests_ignore_the_order_of_object_members(
            entries in proptest::collection::btree_map("[a-z]{1,6}", any::<i64>(), 0..8),
            seed in any::<u64>(),
        ) {
            let forward: serde_json::Map<String, Value> =
                entries.iter().map(|(k, v)| (k.clone(), json!(v))).collect();
            let mut shuffled: Vec<(String, Value)> =
                entries.iter().map(|(k, v)| (k.clone(), json!(v))).collect();
            let mut rng = fastrand::Rng::with_seed(seed);
            rng.shuffle(&mut shuffled);
            let mut reordered = serde_json::Map::new();
            for (key, value) in shuffled {
                reordered.insert(key, value);
            }
            let a = digest("C", Some("q"), &json!([Value::Object(forward)])).unwrap();
            let b = digest("C", Some("q"), &json!([Value::Object(reordered)])).unwrap();
            prop_assert_eq!(a, b);
        }
    }
}
