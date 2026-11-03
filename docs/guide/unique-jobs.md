# Unique jobs

A unique job holds a **lock** while it waits and runs: pushing a job with the same class,
queue and arguments meanwhile is a **duplicate**, which is not pushed (`perform_async` and
`push` return `Ok(None)`, like Sidekiq Enterprise). The lock is taken by the Lua script that
writes the job and held or released by the scripts that write its outcome, so a push that fails
leaves no lock, a lock is never released before its job's outcome is written, and a push
resent after a lost reply is not pushed twice. Every lock expires: `unique_for` is mandatory.

```rust
use std::time::Duration;

use hench::prelude::*;

#[hench::job(queue = "sync")]
async fn sync_address(user_id: i64, reason: String) {
    println!("syncing the address of user {user_id} ({reason})");
}

#[tokio::main]
async fn main() -> hench::Result<()> {
    // Testing clients keep the locks in memory; a Redis client takes them in Redis.
    let client = Client::fake();
    let unique = || sync_address::set().unique_for(Duration::from_secs(600));

    let first = unique().perform_async(&client, (42, "form".to_owned())).await?;
    assert!(first.is_some());
    // The same arguments while the first job waits or runs: a duplicate, not pushed.
    assert_eq!(unique().perform_async(&client, (42, "form".to_owned())).await?, None);
    // `push_outcome` names the holder.
    let outcome = unique().push_outcome(&client, (42, "form".to_owned())).await?;
    assert!(matches!(outcome, hench::PushOutcome::Duplicate { holder, .. } if Some(&holder) == first.as_ref()));
    // Other arguments take another lock.
    assert!(unique().perform_async(&client, (7, "form".to_owned())).await?.is_some());

    // Once the job succeeded, its lock is released.
    client.drain::<sync_address>().await?;
    assert!(unique().perform_async(&client, (42, "form".to_owned())).await?.is_some());
    Ok(())
}
```

When the lock is released (`UniqueUntil`):

| Policy | Released | For |
|---|---|---|
| `Success` (default) | when the job succeeds, dies (or is discarded), or is deleted or killed from the dashboard or the API; kept and extended while a retry waits; renewed by the heartbeat while the job runs | at most one copy waiting or running |
| `Start` | when `perform` starts; taken back, if free, while a retry waits | at most one copy waiting: a change during a run triggers one follow-up job |
| `Expired` | only when `unique_for` ends, whatever the job did | at most one per period (not to be confused with [throttles](rate-limiting.md), which limit how many start) |

Settings come from the client's `default_job_options`, the job type's options
(`JobOptions::unique_for` and the other `unique*` setters, or `#[hench::job(unique_for = 600,
unique_until = "start", on_conflict = "replace", unique_across_queues = true, unique_by =
[user_id])]`) and each push (`set()`, `client.job::<J>()`, `BulkPush`, raw
`JobPayload::unique`); each layer overrides the one before it field by field, and
`.not_unique()` turns uniqueness off for one push (the macro's other unique options need its
`unique_for`: without it they are a compile error). Duplicates are found by a digest (SHA-256)
of the class, the queue (left out with `unique_across_queues`) and the arguments, or the part
of them `Job::unique_key` returns (typed pushes, however the job became unique), or an explicit
`unique_key`:

```rust,no_run
use std::time::Duration;

use hench::api::Api;
use hench::prelude::*;
use serde_json::{Value, json};

/// Syncs a user's address with the CRM: one pending sync per user, whatever the reason.
#[derive(Default)]
struct SyncAddress;

impl Job for SyncAddress {
    const NAME: &'static str = "SyncAddress";
    type Args = (i64, String);

    fn options() -> JobOptions {
        JobOptions::new().queue("sync").unique_for(Duration::from_secs(600))
    }

    // The part of the arguments that makes a duplicate (all of them by default).
    fn unique_key((user_id, _reason): &Self::Args) -> Option<Value> {
        Some(json!([user_id]))
    }

    async fn perform(&self, _ctx: &JobContext, (user_id, reason): Self::Args) -> Result<(), JobError> {
        println!("syncing the address of user {user_id} ({reason})");
        Ok(())
    }
}

#[hench::job(queue = "search")]
async fn reindex(document_id: i64, version: u64) {
    println!("indexing version {version} of document {document_id}");
}

#[hench::job(queue = "mailers")]
async fn digest_email(user_id: i64) {
    println!("sending the daily digest to user {user_id}");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let redis = RedisPool::connect("redis://127.0.0.1:6379").await?;
    let client = Client::new(redis.clone());

    SyncAddress::perform_async(&client, (42, "form".to_owned())).await?;
    // The same user, another reason: a duplicate.
    assert_eq!(SyncAddress::perform_async(&client, (42, "retry".to_owned())).await?, None);

    // Debounce: index a document 30 s after its last change, with the last version. Each push
    // replaces the job still waiting in the schedule (a queued or running one is kept).
    reindex::set()
        .delay(Duration::from_secs(30))
        .unique_for(Duration::from_secs(60))
        .unique_key(json!([7]))
        .on_conflict(OnConflict::Replace)
        .perform_async(&client, (7, 12))
        .await?;

    // At most one per period: one digest per user and day, whatever happens to the job.
    let daily = Unique::new(Duration::from_secs(86_400)).until(UniqueUntil::Expired);
    digest_email::set().unique(daily).perform_async(&client, (42,)).await?;

    // Who holds the lock of user 42's sync? Release it by hand (the holder is compared).
    let locks = Api::new(redis).unique_locks();
    if let Some(lock) = locks.find::<SyncAddress>(&(42, String::new())).await? {
        println!("held by {} until {:?}", lock.jid, lock.expires_at);
        locks.release(&lock.digest, &lock.jid).await?;
    }
    Ok(())
}
```

- **Conflicts** (`on_conflict`): `Drop` (the default: `Ok(None)`), `Error` (the push fails with
  `Error::DuplicateJob`, naming the holder), `Replace` (the debounce above: the holder is
  replaced while it waits in the schedule, however many jobs share its run time — a bulk push
  with one delay, or every user's reminder at 09:00; anything but a holder waiting in the
  schedule makes the push a duplicate). `push_outcome` returns
  `PushOutcome::{Pushed, Replaced, Duplicate, Stopped}` whatever the strategy, but `Error`,
  whose duplicate fails the push with `Error::DuplicateJob` there too.
- **Choosing `unique_for`**: at least the longest time a job waits in its queue; at least
  three heartbeat intervals (30 s) for long jobs (the heartbeat renews the locks of running
  `Success` jobs); ten minutes or more where a duplicate after a crash matters (a crashed
  process's jobs are recovered after its heartbeat expires and the next orphan check). Retries
  extend a `Success` lock to the retry's time plus `unique_for`. A lock whose time runs out
  before its job lets a duplicate in: a **lapse**, counted per class and shown on the Locks
  tab, `hench locks` and `UniqueLocks::events`. Lapses mean `unique_for` is too short.
- **Uniqueness is not concurrency control**: with `Start`, a waiting copy can run next to a
  running one. For "at most one waiting and never two running", combine `Start` with a
  concurrency limit of one per key (`hench::limit`).
- **Where locks are not touched**: the scheduler, retries and moves from the dashboard keep the
  job's lock; a job moved to a later time (`SortedEntry::reschedule`, or the scheduler
  deferring a job it could not push) keeps it for the new wait, and one whose move client
  middleware stops is deleted, releasing it; `Queue::clear` and `JobSet::clear` leave locks to
  expire (`UniqueLocks::release_all` releases them); a job retried by hand from the dead set
  runs without a lock; a process that crashes keeps its jobs' locks until they expire or the
  recovered jobs finish (a job recovered more than `max_recoveries` times, a poison pill, goes
  to the morgue and releases its lock).
- **Seeing and releasing locks**: `Api::unique_locks()` (`page`, `find::<J>`, `release`,
  `force_release`, `release_all`, `events`), the dashboard's Locks tab (every lock, where its
  holder is, the last hour's duplicates and lapses, release one, a selection or all), `hench
  locks [list|show|release|release-all]`, `hench enqueue --unique-for 10m` and the JSON API
  (`GET <prefix>/api/v1/locks`, `DELETE <prefix>/api/v1/locks/{digest}?jid=`, `unique` in
  `POST <prefix>/api/v1/jobs`; `hench web --api`).
- **Testing**: `Client::fake()` and `Client::inline()` enforce locks in memory, per client
  (`Testing::unique_jobs(false)` turns that off); `client.unique_holder::<J>(&args)` tells who
  holds a lock. `HenchBuilder::unique_jobs(false)` (or `unique_jobs = false` in the
  configuration file) and `Client::with_unique_jobs(false)` make pushes ignore uniqueness.
- **Cost**: a unique push costs about three times the Redis CPU of a plain one; each lock
  takes about 440 bytes until it expires, plus a copy of the job while it waits in the schedule
  (what lets a debounce push find it at once). Jobs without uniqueness are pushed and
  acknowledged exactly as before.
- **Giving the lock up early**: `ctx.release_unique_lock()` frees the job's lock while it runs
  (for example once it has read the state it works on), whatever the policy, so that the next
  push is a job of its own. The job keeps no lock for the rest of its execution: a retry or a
  copy pushed back runs without one.
- **Upgrades**: deploy workers that understand unique jobs before clients push them (an older
  worker ignores the lock: it expires with its time to live).

---

[← Testing](testing.md) · [Guide](README.md) · [Periodic jobs →](periodic-jobs.md)
