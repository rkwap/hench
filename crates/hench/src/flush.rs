//! Batches of counter writes applied to Redis once each, whatever happens to their replies.
//!
//! Processes add what they counted to Redis on every heartbeat: the `stat:*` counters
//! ([`crate::stats`]) and the job metrics ([`crate::metrics::Tracker`]). A write whose reply
//! does not arrive in time may still run: Redis runs the requests it received, and a stalled
//! server (a long script, the fork of a large dataset, a paused VM) runs them once it
//! continues. Sending the counts again would then count them twice, and dropping them would lose
//! them whenever the request never reached Redis.
//!
//! So a [`Flusher`] numbers its batches and sends a batch, unchanged, until Redis confirms it.
//! A script applies a batch only if its number is above that of the last batch applied, which
//! it records in the flusher's guard key ([`keys::flush_guard`]) in the same atomic step: every
//! attempt after the first one that ran is a no-op. Batches are sent one at a time (a new one
//! only once the previous one is confirmed), so the numbers reach Redis in order, and an old
//! attempt that runs late is recognized as well. See `docs/DESIGN.md` §5.
//
// Shared by the engine's stats and the features lane's metrics.

use std::collections::HashMap;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use ::redis::Script;
use tokio::sync::Mutex;

use crate::error::Result;
use crate::keys;
use crate::payload::random_hex;
use crate::redis::RedisPool;

/// Applies a batch of writes unless a batch with the same or a higher number was applied
/// before.
///
/// KEYS: the guard (the number of the last batch applied), then the keys written. ARGV: the
/// batch's number, the guard's time to live in seconds, then each write as the number of its
/// arguments, its command, the index of its key in KEYS and its arguments.
///
/// The guard is written first: when Redis refuses writes (out of memory, a read-only replica)
/// the script fails there, before anything is counted, and the batch is sent again later. The
/// writes then run with `pcall`, so that one failing write (a key holding another type) neither
/// stops the others nor makes the batch run again. Returns -1 for a batch applied before, else
/// the number of writes that failed.
pub(crate) static APPLY_ONCE: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
local applied = tonumber(redis.call('GET', KEYS[1])) or 0
if tonumber(ARGV[1]) <= applied then
  return -1
end
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
local failed = 0
local i = 3
while i <= #ARGV do
  local count = tonumber(ARGV[i])
  local key = KEYS[tonumber(ARGV[i + 2])]
  local reply = redis.pcall(ARGV[i + 1], key, unpack(ARGV, i + 3, i + 2 + count))
  if type(reply) == 'table' and reply.err then
    failed = failed + 1
  end
  i = i + 3 + count
end
return failed
",
    )
});

/// How long a [`Flusher`] keeps sending a batch Redis does not confirm. Half the guard's
/// lifetime ([`keys::FLUSH_GUARD_TTL`]), so that the guard written by an attempt that ran
/// outlives every later attempt by hours, even one that Redis runs late.
const GIVE_UP_AFTER: Duration = Duration::from_secs(keys::FLUSH_GUARD_TTL.as_secs() / 2);

/// Writes for a [`Flusher`]: commands that each write one key (`INCRBY`, `HINCRBY`,
/// `BITFIELD`, `EXPIRE`, ...), applied in order.
#[derive(Debug, Clone, Default)]
pub(crate) struct Batch {
    /// The keys written, in the order of their first write.
    keys: Vec<String>,
    /// Position of each key in the script's KEYS (the guard is the first).
    positions: HashMap<String, usize>,
    /// The writes, encoded for the script.
    writes: Vec<String>,
    /// What the batch amounts to for its creator (for the metrics, the counters it increments).
    weight: usize,
}

impl Batch {
    /// An empty batch.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Adds the write `command key args...`.
    pub(crate) fn write<A: ToString>(
        &mut self,
        command: &str,
        key: &str,
        args: impl IntoIterator<Item = A>,
    ) -> &mut Self {
        let position = match self.positions.get(key) {
            Some(position) => *position,
            None => {
                self.keys.push(key.to_owned());
                // KEYS[1] is the guard; Lua indexes from 1.
                let position = self.keys.len() + 1;
                self.positions.insert(key.to_owned(), position);
                position
            }
        };
        let start = self.writes.len();
        // Placeholder for the number of arguments, known once they are written.
        self.writes.push(String::new());
        self.writes.push(command.to_owned());
        self.writes.push(position.to_string());
        self.writes
            .extend(args.into_iter().map(|arg| arg.to_string()));
        self.writes[start] = (self.writes.len() - start - 3).to_string();
        self
    }

    /// Sets what the batch amounts to, returned by [`Flusher::flush`] once it is written.
    pub(crate) fn set_weight(&mut self, weight: usize) {
        self.weight = weight;
    }

    /// What the batch amounts to (see [`Batch::set_weight`]).
    pub(crate) fn weight(&self) -> usize {
        self.weight
    }

    /// `true` when the batch writes nothing.
    pub(crate) fn is_empty(&self) -> bool {
        self.writes.is_empty()
    }
}

/// A batch sent to Redis and not yet confirmed.
#[derive(Debug)]
struct Unconfirmed {
    number: u64,
    batch: Batch,
    /// When it was first sent.
    since: Instant,
}

#[derive(Debug, Default)]
struct State {
    /// The number of the last batch created.
    last: u64,
    /// The batch Redis has not confirmed, sent again by the next flush.
    unconfirmed: Option<Unconfirmed>,
}

/// Writes batches to Redis one at a time, each applied once; see the [module docs](self).
#[derive(Debug)]
pub(crate) struct Flusher {
    /// The guard key: the number of the last batch applied.
    guard: String,
    state: Mutex<State>,
    /// Whether a batch awaits confirmation (readable without waiting for a flush).
    has_unconfirmed: AtomicBool,
    /// How long an unconfirmed batch is sent again before it is given up.
    give_up_after: Duration,
}

impl Default for Flusher {
    fn default() -> Self {
        Self::new()
    }
}

impl Flusher {
    /// A flusher with a guard key of its own.
    pub(crate) fn new() -> Self {
        Self {
            guard: keys::flush_guard(&random_hex(12)),
            state: Mutex::default(),
            has_unconfirmed: AtomicBool::new(false),
            give_up_after: GIVE_UP_AFTER,
        }
    }

    /// `true` while a batch awaits Redis's confirmation.
    pub(crate) fn has_unconfirmed(&self) -> bool {
        self.has_unconfirmed.load(Ordering::Relaxed)
    }

    /// Writes the batch an earlier flush could not confirm, if any, then the batch `next`
    /// returns (`None` or an empty batch: nothing new). `next` is only called once the earlier
    /// batch is confirmed, so what it would take stays with its owner while Redis does not
    /// answer. Returns the total weight of the batches confirmed.
    ///
    /// A batch is kept before it is sent: if Redis does not confirm it (an error, a timeout,
    /// or this future dropped midway), the next flush sends it again as it was, and Redis
    /// applies it only if no earlier attempt was applied. A batch still unconfirmed after
    /// twelve hours is given up (logged).
    pub(crate) async fn flush(
        &self,
        redis: &RedisPool,
        next: impl FnOnce() -> Option<Batch>,
    ) -> Result<usize> {
        let mut state = self.state.lock().await;
        let mut confirmed = 0;
        if let Some(unconfirmed) = &state.unconfirmed {
            if unconfirmed.since.elapsed() >= self.give_up_after {
                tracing::warn!(
                    batch = unconfirmed.number,
                    "Redis has not confirmed these counts for hours; giving them up"
                );
            } else {
                confirmed += self.send(redis, unconfirmed).await?;
            }
            state.unconfirmed = None;
            self.has_unconfirmed.store(false, Ordering::Relaxed);
        }
        let Some(batch) = next().filter(|batch| !batch.is_empty()) else {
            return Ok(confirmed);
        };
        let number = state.last + 1;
        state.last = number;
        let weight = {
            let unconfirmed = state.unconfirmed.insert(Unconfirmed {
                number,
                batch,
                since: Instant::now(),
            });
            self.has_unconfirmed.store(true, Ordering::Relaxed);
            self.send(redis, unconfirmed).await?
        };
        state.unconfirmed = None;
        self.has_unconfirmed.store(false, Ordering::Relaxed);
        Ok(confirmed + weight)
    }

    /// Sends one batch; returns its weight once Redis confirmed it (applied now, or by an
    /// earlier attempt).
    async fn send(&self, redis: &RedisPool, unconfirmed: &Unconfirmed) -> Result<usize> {
        let batch = &unconfirmed.batch;
        let mut invocation = APPLY_ONCE.prepare_invoke();
        invocation.key(&self.guard);
        for key in &batch.keys {
            invocation.key(key);
        }
        invocation
            .arg(unconfirmed.number)
            .arg(keys::FLUSH_GUARD_TTL.as_secs());
        for write in &batch.writes {
            invocation.arg(write);
        }
        // Sent once: a lost reply is resolved by sending the batch again later.
        let mut conn = redis.conn().await?;
        let failed: i64 = invocation.invoke_async(&mut conn).await?;
        match failed {
            -1 => tracing::debug!(
                batch = unconfirmed.number,
                "an earlier attempt had written these counts; not written again"
            ),
            0 => {}
            failed => tracing::warn!(
                batch = unconfirmed.number,
                failed,
                "some counts could not be written (a key holding another type?); they are \
                 not written again"
            ),
        }
        Ok(batch.weight())
    }

    /// Shortens [`GIVE_UP_AFTER`], for tests.
    #[cfg(test)]
    pub(crate) fn give_up_after(mut self, age: Duration) -> Self {
        self.give_up_after = age;
        self
    }

    /// The guard key, for tests.
    #[cfg(all(test, feature = "test-util"))]
    pub(crate) fn guard(&self) -> &str {
        &self.guard
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batches_encode_writes_with_key_positions() {
        let mut batch = Batch::new();
        assert!(batch.is_empty());
        batch
            .write("INCRBY", "a", [5])
            .write("EXPIRE", "a", [60])
            .write("HINCRBY", "b", ["field", "2"])
            .write("PERSIST", "a", Vec::<String>::new());
        assert_eq!(batch.keys, ["a", "b"]);
        assert_eq!(
            batch.writes,
            [
                "1", "INCRBY", "2", "5", "1", "EXPIRE", "2", "60", "2", "HINCRBY", "3", "field",
                "2", "0", "PERSIST", "2"
            ]
        );
        assert!(!batch.is_empty());
    }

    #[tokio::test]
    async fn unconfirmed_batches_are_kept_and_next_is_not_called() {
        let flusher = Flusher::new();
        assert!(!flusher.has_unconfirmed());
        let mut batch = Batch::new();
        batch.write("INCRBY", "a", [1]);
        let detached = RedisPool::detached();
        assert!(flusher.flush(&detached, || Some(batch)).await.is_err());
        assert!(flusher.has_unconfirmed());
        // The unconfirmed batch is sent first; while it fails, nothing new is taken.
        let mut called = false;
        assert!(
            flusher
                .flush(&detached, || {
                    called = true;
                    None
                })
                .await
                .is_err()
        );
        assert!(!called);
        assert!(flusher.has_unconfirmed());
        // Nothing to write at all.
        assert_eq!(Flusher::new().flush(&detached, || None).await.unwrap(), 0);
        assert_eq!(
            Flusher::new()
                .flush(&detached, || Some(Batch::new()))
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn old_unconfirmed_batches_are_given_up() {
        let flusher = Flusher::new().give_up_after(Duration::ZERO);
        let mut batch = Batch::new();
        batch.write("INCRBY", "a", [1]);
        let detached = RedisPool::detached();
        assert!(flusher.flush(&detached, || Some(batch)).await.is_err());
        assert!(flusher.has_unconfirmed());
        assert_eq!(flusher.flush(&detached, || None).await.unwrap(), 0);
        assert!(!flusher.has_unconfirmed());
    }
}

/// The script against a private Redis.
#[cfg(all(test, feature = "test-util"))]
mod redis_tests {
    use super::*;
    use crate::test_util::TestRedis;

    async fn get(redis: &RedisPool, key: &str) -> Option<String> {
        let mut conn = redis.conn().await.unwrap();
        ::redis::cmd("GET")
            .arg(key)
            .query_async(&mut conn)
            .await
            .unwrap()
    }

    fn increment(key: &str, by: u64, weight: usize) -> Batch {
        let mut batch = Batch::new();
        batch.write("INCRBY", key, [by]).write("EXPIRE", key, [600]);
        batch.set_weight(weight);
        batch
    }

    #[tokio::test]
    async fn batches_are_applied_once_and_in_order() {
        let redis = TestRedis::start().await;
        let pool = redis.pool().await;
        let flusher = Flusher::new();
        assert_eq!(
            flusher
                .flush(&pool, || Some(increment("n", 3, 7)))
                .await
                .unwrap(),
            7
        );
        assert_eq!(get(&pool, "n").await.as_deref(), Some("3"));
        assert_eq!(get(&pool, flusher.guard()).await.as_deref(), Some("1"));
        let ttl: i64 = {
            let mut conn = pool.conn().await.unwrap();
            ::redis::cmd("TTL")
                .arg(flusher.guard())
                .query_async(&mut conn)
                .await
                .unwrap()
        };
        assert!(ttl > 0 && ttl <= 86_400, "{ttl}");

        // Attempts of a batch already applied, and of an older one, change nothing.
        for number in [1, 0] {
            let attempt = Unconfirmed {
                number,
                batch: increment("n", 3, 7),
                since: Instant::now(),
            };
            assert_eq!(flusher.send(&pool, &attempt).await.unwrap(), 7);
        }
        assert_eq!(get(&pool, "n").await.as_deref(), Some("3"));

        // The next batch is applied.
        flusher
            .flush(&pool, || Some(increment("n", 2, 1)))
            .await
            .unwrap();
        assert_eq!(get(&pool, "n").await.as_deref(), Some("5"));
        assert_eq!(get(&pool, flusher.guard()).await.as_deref(), Some("2"));
        // Another flusher has a guard of its own.
        Flusher::new()
            .flush(&pool, || Some(increment("n", 10, 1)))
            .await
            .unwrap();
        assert_eq!(get(&pool, "n").await.as_deref(), Some("15"));
    }

    #[tokio::test]
    async fn a_failing_write_does_not_stop_the_others_nor_repeat_them() {
        let redis = TestRedis::start().await;
        let pool = redis.pool().await;
        {
            let mut conn = pool.conn().await.unwrap();
            let () = ::redis::cmd("LPUSH")
                .arg("list")
                .arg("x")
                .query_async(&mut conn)
                .await
                .unwrap();
        }
        let flusher = Flusher::new();
        let mut batch = Batch::new();
        batch
            .write("INCRBY", "a", [1])
            .write("INCRBY", "list", [1])
            .write("INCRBY", "b", [1]);
        flusher.flush(&pool, || Some(batch)).await.unwrap();
        assert!(!flusher.has_unconfirmed());
        assert_eq!(get(&pool, "a").await.as_deref(), Some("1"));
        assert_eq!(get(&pool, "b").await.as_deref(), Some("1"));
    }

    #[tokio::test]
    async fn nothing_is_counted_while_redis_refuses_writes() {
        let redis = TestRedis::start().await;
        let pool = redis.pool().await;
        let config = |value: &str| {
            let pool = pool.clone();
            let value = value.to_owned();
            async move {
                let mut conn = pool.conn().await.unwrap();
                let () = ::redis::cmd("CONFIG")
                    .arg("SET")
                    .arg("maxmemory")
                    .arg(value)
                    .query_async(&mut conn)
                    .await
                    .unwrap();
            }
        };
        config("1").await;
        let flusher = Flusher::new();
        let error = flusher
            .flush(&pool, || Some(increment("n", 1, 1)))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("OOM"), "{error}");
        assert!(flusher.has_unconfirmed());
        config("0").await;
        assert_eq!(get(&pool, "n").await, None);
        // Sent again once Redis accepts writes, and counted once.
        assert_eq!(flusher.flush(&pool, || None).await.unwrap(), 1);
        assert_eq!(get(&pool, "n").await.as_deref(), Some("1"));
    }
}
