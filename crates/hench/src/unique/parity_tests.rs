//! Model-based tests of the lock lifecycle (`docs/design/unique.md` §3.4 C6, §3.6 T3): random
//! sequences of pushes (immediate or scheduled, any conflict strategy), holds (a retry or a
//! push back), releases (a success, a death, an API removal, an operator) and expiries, run
//! against a pure model of the lifecycle table, the scripts in a real Redis, and the in-memory
//! table of the testing clients. After each step the three agree on the result and the holder.

use std::collections::HashMap;
use std::time::Duration;

use serde_json::json;

use crate::client::{Client, ClientMode, PushResult};
use crate::keys;
use crate::payload::{JobPayload, now_secs_f64};
use crate::redis::RedisPool;
use crate::test_util::TestRedis;
use crate::unique::lua::{UNIQUE_LOCK_OP, UNIQUE_RELEASE};
use crate::unique::{LockRef, OnConflict, Unique, UniqueOption};

/// The digests the sequences use (few, so that pushes collide).
const DIGESTS: [&str; 3] = ["model-a", "model-b", "model-c"];

/// Jobs per sequence's population.
const JOBS: usize = 6;

/// One step.
#[derive(Debug, Clone, Copy)]
enum Step {
    /// Push job `job` with digest `digest`, scheduled or not, with a conflict strategy.
    Push {
        job: usize,
        digest: usize,
        scheduled: bool,
        conflict: OnConflict,
    },
    /// The job's outcome holds its lock (a retry, a push back).
    Hold { job: usize, digest: usize },
    /// The job's outcome, an API removal or an operator releases its lock.
    Release { job: usize, digest: usize },
    /// The lock of `digest` expires.
    Expire { digest: usize },
}

/// What the lifecycle table says.
#[derive(Debug, Default)]
struct Model {
    /// digest → (holder, holder waiting in the schedule).
    locks: HashMap<usize, (usize, bool)>,
}

impl Model {
    fn apply(&mut self, step: Step) -> Option<PushResult> {
        match step {
            Step::Push {
                job,
                digest,
                scheduled,
                conflict,
            } => Some(match self.locks.get(&digest).copied() {
                None => {
                    self.locks.insert(digest, (job, scheduled));
                    PushResult::Pushed
                }
                Some((holder, _)) if holder == job => PushResult::AlreadyPushed,
                Some((holder, waiting)) if conflict == OnConflict::Replace && waiting => {
                    self.locks.insert(digest, (job, scheduled));
                    PushResult::Replaced {
                        replaced: jid(holder, digest),
                    }
                }
                Some((holder, _)) => PushResult::Duplicate {
                    holder: jid(holder, digest),
                },
            }),
            Step::Hold { job, digest } => {
                match self.locks.get(&digest).copied() {
                    // A hold to a queue: no longer waiting in the schedule.
                    Some((holder, _)) if holder == job => {
                        self.locks.insert(digest, (job, false));
                    }
                    Some(_) => {}
                    None => {
                        self.locks.insert(digest, (job, false));
                    }
                }
                None
            }
            Step::Release { job, digest } => {
                if self
                    .locks
                    .get(&digest)
                    .is_some_and(|(holder, _)| *holder == job)
                {
                    self.locks.remove(&digest);
                }
                None
            }
            Step::Expire { digest } => {
                self.locks.remove(&digest);
                None
            }
        }
    }

    fn holder(&self, digest: usize) -> Option<String> {
        self.locks
            .get(&digest)
            .map(|(holder, _)| jid(*holder, digest))
    }
}

/// The jid of `job` pushed with `digest`: one job has one digest (a job pushed with two
/// digests would be two jobs).
fn jid(job: usize, digest: usize) -> String {
    format!("{job:012x}{digest:012x}")
}

/// The payload of `job` pushed with `digest`.
fn payload(job: usize, digest: usize, scheduled: bool, conflict: OnConflict) -> JobPayload {
    let mut payload = JobPayload::new("Model", vec![json!(job)]);
    payload.jid = jid(job, digest);
    payload.queue = "model".to_owned();
    let mut unique = Unique::new(Duration::from_secs(600)).on_conflict(conflict);
    unique.digest = Some(DIGESTS[digest].to_owned());
    payload.unique = Some(UniqueOption::Enabled(unique));
    if scheduled {
        payload.at = Some(now_secs_f64() + 3_600.0);
    }
    payload
}

fn lock(job: usize, digest: usize) -> LockRef {
    LockRef::from_payload_quietly(&payload(job, digest, false, OnConflict::Drop))
        .unwrap_or_else(|| panic!("job {job} has a lock"))
}

fn random_step(rng: &mut fastrand::Rng) -> Step {
    let job = rng.usize(..JOBS);
    let digest = rng.usize(..DIGESTS.len());
    match rng.u8(..10) {
        0..=4 => Step::Push {
            job,
            digest,
            scheduled: rng.bool(),
            conflict: [OnConflict::Drop, OnConflict::Error, OnConflict::Replace][rng.usize(..3)],
        },
        5 | 6 => Step::Hold { job, digest },
        7 | 8 => Step::Release { job, digest },
        _ => Step::Expire { digest },
    }
}

/// The step against Redis: `UNIQUE_PUSH` through `deliver`, `UNIQUE_LOCK_OP` holds,
/// `UNIQUE_RELEASE`, and a `DEL` for an expiry.
async fn in_redis(pool: &RedisPool, step: Step) -> Option<PushResult> {
    match step {
        Step::Push {
            job,
            digest,
            scheduled,
            conflict,
        } => {
            let pushed = payload(job, digest, scheduled, conflict);
            let mut results = crate::unique::push::deliver(pool, vec![pushed], now_secs_f64())
                .await
                .unwrap();
            Some(results.remove(0))
        }
        Step::Hold { job, digest } => {
            let lock = lock(job, digest);
            let mut conn = pool.conn().await.unwrap();
            let _: i64 = UNIQUE_LOCK_OP
                .script()
                .key(&lock.key)
                .key(keys::UNIQUE_LOCKS)
                .key("u|model")
                .arg("hold")
                .arg(&lock.jid)
                .arg(&lock.digest)
                .arg(&lock.class)
                .arg(&lock.queue)
                .arg("success")
                .arg(lock.ttl_ms())
                .arg("")
                .arg("")
                .invoke_async(&mut conn)
                .await
                .unwrap();
            None
        }
        Step::Release { job, digest } => {
            let lock = lock(job, digest);
            let mut conn = pool.conn().await.unwrap();
            let _: i64 = UNIQUE_RELEASE
                .script()
                .key(&lock.key)
                .key(keys::UNIQUE_LOCKS)
                .key("u|model")
                .arg(&lock.jid)
                .arg(&lock.digest)
                .arg(&lock.class)
                .arg("0")
                .invoke_async(&mut conn)
                .await
                .unwrap();
            None
        }
        Step::Expire { digest } => {
            let mut conn = pool.conn().await.unwrap();
            let _: i64 = ::redis::cmd("DEL")
                .arg(keys::unique_lock(DIGESTS[digest]))
                .query_async(&mut conn)
                .await
                .unwrap();
            None
        }
    }
}

/// The step against the in-memory table of a fake client.
fn in_memory(client: &Client, step: Step) -> Option<PushResult> {
    let table = &client.tables().unique;
    match step {
        Step::Push {
            job,
            digest,
            scheduled,
            conflict,
        } => {
            let pushed = payload(job, digest, scheduled, conflict);
            let mut results = crate::unique::fake::admit(client, std::slice::from_ref(&pushed));
            // Capture what the push wrote, as a fake client does (a replaced holder must be
            // found captured in the schedule).
            if results[0].written() {
                let ClientMode::Fake(store) = client.mode() else {
                    panic!("a fake client");
                };
                futures::executor::block_on(crate::testing::fake_push(store, vec![pushed]))
                    .unwrap();
            }
            Some(results.remove(0))
        }
        Step::Hold { job, digest } => {
            let lock = lock(job, digest);
            table.hold(&lock, lock.ttl);
            None
        }
        Step::Release { job, digest } => {
            table.release(&lock(job, digest));
            None
        }
        Step::Expire { digest } => {
            table.forget(DIGESTS[digest]);
            None
        }
    }
}

async fn redis_holder(pool: &RedisPool, digest: usize) -> Option<String> {
    let mut conn = pool.conn().await.unwrap();
    ::redis::cmd("HGET")
        .arg(keys::unique_lock(DIGESTS[digest]))
        .arg("jid")
        .query_async(&mut conn)
        .await
        .unwrap()
}

#[tokio::test]
async fn redis_the_testing_table_and_the_model_agree() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let mut rng = fastrand::Rng::with_seed(0x5eed_u64);
    for sequence in 0..60 {
        let mut conn = pool.conn().await.unwrap();
        let () = ::redis::cmd("FLUSHALL")
            .query_async(&mut conn)
            .await
            .unwrap();
        let client = Client::fake();
        let mut model = Model::default();
        let mut steps = Vec::new();
        for _ in 0..30 {
            let step = random_step(&mut rng);
            steps.push(step);
            let expected = model.apply(step);
            let from_redis = in_redis(&pool, step).await;
            let from_memory = in_memory(&client, step);
            assert_eq!(from_redis, expected, "sequence {sequence}: {steps:?}");
            assert_eq!(from_memory, expected, "sequence {sequence}: {steps:?}");
            for (digest, name) in DIGESTS.iter().enumerate() {
                let holder = model.holder(digest);
                assert_eq!(
                    redis_holder(&pool, digest).await,
                    holder,
                    "sequence {sequence}, Redis, digest {digest}: {steps:?}"
                );
                assert_eq!(
                    client.tables().unique.holder(name),
                    holder,
                    "sequence {sequence}, memory, digest {digest}: {steps:?}"
                );
            }
        }
    }
}
