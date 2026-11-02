//! Jobs and helpers shared by the batch integration tests (`tests/batch*.rs`): a member job
//! whose argument says what it does, a callback job recording the event it runs for, a log
//! of what ran, and readers of a batch's status and keys.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use hench::batch::{BatchStatus, Batches};
use hench::prelude::*;
use hench::test_util::TestRedis;
use hench::{Hench, HenchBuilder, Launcher};
use tokio::sync::Notify;

use crate::support::{Gate, WAIT, init_logging, wait_for};

/// What the jobs of one test did, in order.
#[derive(Default)]
pub struct Log {
    entries: Mutex<Vec<String>>,
    attempts: Mutex<HashMap<String, u32>>,
    statuses: Mutex<HashMap<String, BatchStatus>>,
    notify: Notify,
}

impl Log {
    /// Records `entry`.
    pub fn record(&self, entry: impl Into<String>) {
        self.entries.lock().unwrap().push(entry.into());
        self.notify.notify_waiters();
    }

    /// Every entry, in order.
    pub fn entries(&self) -> Vec<String> {
        self.entries.lock().unwrap().clone()
    }

    /// How many entries are `entry`.
    pub fn count(&self, entry: &str) -> usize {
        self.entries
            .lock()
            .unwrap()
            .iter()
            .filter(|e| *e == entry)
            .count()
    }

    /// How many entries start with `prefix`.
    pub fn count_prefix(&self, prefix: &str) -> usize {
        self.entries
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.starts_with(prefix))
            .count()
    }

    /// Where `entry` first appears.
    pub fn position(&self, entry: &str) -> Option<usize> {
        self.entries.lock().unwrap().iter().position(|e| e == entry)
    }

    /// Counts a run of `jid`: 1 for its first.
    pub fn attempt(&self, jid: &str) -> u32 {
        let mut attempts = self.attempts.lock().unwrap();
        let attempt = attempts.entry(jid.to_owned()).or_default();
        *attempt += 1;
        *attempt
    }

    /// Keeps the status a callback saw, under its name.
    pub fn keep_status(&self, name: &str, status: BatchStatus) {
        self.statuses
            .lock()
            .unwrap()
            .insert(name.to_owned(), status);
    }

    /// The status the callback `name` saw.
    pub fn status_seen(&self, name: &str) -> Option<BatchStatus> {
        self.statuses.lock().unwrap().get(name).cloned()
    }

    /// Waits until `entry` was recorded `count` times.
    pub async fn wait_count(&self, entry: &str, count: usize) {
        let what = format!("{count} × `{entry}` in the log");
        wait_for(&what, WAIT, || async { self.count(entry) >= count }).await;
    }
}

/// A batch member: records its run as its tag, then does what its mode says.
///
/// - `ok`: succeeds; `fail`: fails (retried after 1 hour, or by the test through the API);
///   `fail-once`: fails its first run, then succeeds; `second-fails`: succeeds its first run
///   and fails later ones (a stale copy); `discard`: fails and is discarded
///   (`RetryIn::Discard`);
/// - `cancelled`: returns `JobError::batch_cancelled()` whatever its batch;
/// - `watch`: a long job stopping at its batch's cancellation (`ctx.check_batch()`);
/// - `gate`, `gate-fail`: wait for the test's [`Gate`], then succeed or fail;
/// - `shutdown-once`: its first run waits for the process's shutdown and stops with
///   `hench::Shutdown`, later runs succeed; `hang-once`: its first run never returns
///   (interrupted by a hard shutdown), later runs succeed.
#[derive(Default)]
pub struct Work;

impl Job for Work {
    const NAME: &'static str = "batch::Work";
    type Args = (String, String);

    async fn perform(&self, ctx: &JobContext, (tag, mode): Self::Args) -> Result<(), JobError> {
        let log = ctx.data::<Log>()?;
        let attempt = log.attempt(ctx.jid());
        log.record(tag.clone());
        match mode.as_str() {
            "ok" => Ok(()),
            "fail" => Err(JobError::msg(format!("{tag} failed"))),
            "fail-once" if attempt == 1 => Err(JobError::msg(format!("{tag} failed once"))),
            "fail-once" => Ok(()),
            "discard" => Err(JobError::msg(format!("{tag}: discard me"))),
            "cancelled" => Err(JobError::batch_cancelled()),
            "watch" => {
                for _ in 0..1_000 {
                    ctx.check_batch().await?;
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Ok(())
            }
            "second-fails" if attempt == 1 => Ok(()),
            "second-fails" => Err(JobError::msg(format!("{tag}: a stale copy failed"))),
            "gate" => {
                ctx.data::<Gate>()?.pass().await;
                Ok(())
            }
            "gate-fail" => {
                ctx.data::<Gate>()?.pass().await;
                Err(JobError::msg(format!("{tag} failed after the gate")))
            }
            "shutdown-once" if attempt == 1 => {
                ctx.stopping().await;
                Err(JobError::new(
                    JobError::SHUTDOWN,
                    "interrupted at a safe point",
                ))
            }
            "shutdown-once" => Ok(()),
            "hang-once" if attempt == 1 => {
                std::future::pending::<()>().await;
                Ok(())
            }
            "hang-once" => Ok(()),
            other => Err(JobError::msg(format!("unknown mode {other}"))),
        }
    }

    fn retry_in(&self, _count: u32, error: &JobError, _job: &JobPayload) -> RetryIn {
        if error.message().contains("discard me") {
            RetryIn::Discard
        } else {
            // Far away: the tests retry through the API when they want to.
            RetryIn::After(Duration::from_secs(3_600))
        }
    }
}

/// A callback: records `<name>` and the event it runs for (`<name>:<event>`), and keeps the
/// status of the batch it runs for under its name.
#[derive(Default)]
pub struct Callback;

impl Job for Callback {
    const NAME: &'static str = "batch::Callback";
    type Args = (String,);

    async fn perform(&self, ctx: &JobContext, (name,): Self::Args) -> Result<(), JobError> {
        let log = ctx.data::<Log>()?;
        let callback = ctx
            .batch_callback()
            .ok_or_else(|| JobError::msg("not a callback"))?;
        if let Some(status) = callback.status(ctx).await? {
            log.keep_status(&name, status);
        }
        log.record(format!("{name}:{}", callback.event()));
        Ok(())
    }
}

/// Starts a worker on `redis` running [`Work`] and [`Callback`] (plus what `configure` adds)
/// on the queues `default`, `critical` and `low`, with `log` as data.
pub async fn worker(
    redis: &TestRedis,
    log: &Arc<Log>,
    configure: impl FnOnce(HenchBuilder) -> HenchBuilder,
) -> (Hench, Launcher) {
    init_logging();
    let builder = redis
        .builder()
        .await
        .concurrency(5)
        .queues(["default", "critical", "low"])
        .register(Work)
        .register(Callback)
        .data_arc(log.clone());
    let hench = configure(builder).build().await.unwrap();
    let launcher = hench.start().await.unwrap();
    (hench, launcher)
}

/// The arguments of a [`Work`] member.
pub fn work(tag: &str, mode: &str) -> (String, String) {
    (tag.to_owned(), mode.to_owned())
}

/// The arguments of a [`Callback`].
pub fn named(name: &str) -> (String,) {
    (name.to_owned(),)
}

/// The status of batch `bid` (panics when it does not exist).
pub async fn status_of(client: &Client, bid: &str) -> BatchStatus {
    client
        .batch_status(bid)
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("batch {bid} does not exist"))
}

/// The batches of `client`'s Redis.
pub fn batches(client: &Client) -> Batches {
    Batches::new(client.clone())
}

/// Waits until `check` holds for the status of `bid`.
pub async fn wait_status(
    client: &Client,
    bid: &str,
    what: &str,
    check: impl Fn(&BatchStatus) -> bool,
) -> BatchStatus {
    let found = Mutex::new(None);
    wait_for(what, WAIT, || async {
        match client.batch_status(bid).await.unwrap() {
            Some(status) if check(&status) => {
                *found.lock().unwrap() = Some(status);
                true
            }
            _ => false,
        }
    })
    .await;
    found.into_inner().unwrap().unwrap()
}

/// `(pending, failures, dead, skipped)` of a status.
pub fn counts(status: &BatchStatus) -> (u64, u64, u64, u64) {
    (status.pending, status.failures, status.dead, status.skipped)
}

/// Every key of the batches (`batch:*`), sorted.
pub async fn batch_keys(redis: &TestRedis) -> Vec<String> {
    let pool = redis.pool().await;
    let mut conn = pool.conn().await.unwrap();
    let mut keys: Vec<String> = hench::redis_rs::cmd("KEYS")
        .arg("batch:*")
        .query_async(&mut conn)
        .await
        .unwrap();
    keys.sort();
    keys
}
