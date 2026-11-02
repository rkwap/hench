//! Helpers shared by the integration tests.
#![allow(dead_code)]

use std::future::Future;
use std::time::Duration;

use hench::prelude::*;
use hench::redis_rs;

/// A job with two integer arguments and default options.
#[derive(Default)]
pub struct MyJob;

impl Job for MyJob {
    const NAME: &'static str = "MyJob";
    type Args = (i64, i64);

    async fn perform(&self, _ctx: &JobContext, _args: (i64, i64)) -> Result<(), JobError> {
        Ok(())
    }
}

/// A job whose options route it to the `flimflam` queue.
#[derive(Default)]
pub struct QueuedJob;

impl Job for QueuedJob {
    const NAME: &'static str = "QueuedJob";
    type Args = (i64,);

    fn options() -> JobOptions {
        JobOptions::new().queue("flimflam")
    }

    async fn perform(&self, _ctx: &JobContext, _args: (i64,)) -> Result<(), JobError> {
        Ok(())
    }
}

/// A job with queue `foo` and 12 retries, taking any number of integers.
#[derive(Default)]
pub struct MySetJob;

impl Job for MySetJob {
    const NAME: &'static str = "MySetJob";
    type Args = Vec<i64>;

    fn options() -> JobOptions {
        JobOptions::new().queue("foo").retry(12)
    }

    async fn perform(&self, _ctx: &JobContext, _args: Vec<i64>) -> Result<(), JobError> {
        Ok(())
    }
}

/// Runs a Redis command on the shared connection.
pub async fn cmd<T: redis_rs::FromRedisValue>(pool: &RedisPool, command: redis_rs::Cmd) -> T {
    let mut conn = pool.conn().await.expect("redis connection");
    command.query_async(&mut conn).await.expect("redis command")
}

/// Number of jobs in `queue`.
pub async fn queue_size(pool: &RedisPool, queue: &str) -> usize {
    cmd(
        pool,
        redis_rs::cmd("LLEN").arg(hench::keys::queue(queue)).clone(),
    )
    .await
}

/// Raw JSON of the jobs in `queue`, oldest (next to run) first.
pub async fn raw_queue(pool: &RedisPool, queue: &str) -> Vec<String> {
    let mut jobs: Vec<String> = cmd(
        pool,
        redis_rs::cmd("LRANGE")
            .arg(hench::keys::queue(queue))
            .arg(0)
            .arg(-1)
            .clone(),
    )
    .await;
    jobs.reverse();
    jobs
}

/// Jobs in `queue`, oldest (next to run) first.
pub async fn queue_jobs(pool: &RedisPool, queue: &str) -> Vec<JobPayload> {
    raw_queue(pool, queue)
        .await
        .iter()
        .map(|json| JobPayload::from_json(json).expect("valid job JSON"))
        .collect()
}

/// Members of the `schedule` set with their scores, earliest first.
pub async fn scheduled(pool: &RedisPool) -> Vec<(JobPayload, f64)> {
    let entries: Vec<(String, f64)> = cmd(
        pool,
        redis_rs::cmd("ZRANGE")
            .arg(hench::keys::SCHEDULE)
            .arg(0)
            .arg(-1)
            .arg("WITHSCORES")
            .clone(),
    )
    .await;
    entries
        .into_iter()
        .map(|(json, score)| (JobPayload::from_json(&json).expect("valid job JSON"), score))
        .collect()
}

/// Raw members of the `schedule` set.
pub async fn raw_scheduled(pool: &RedisPool) -> Vec<String> {
    cmd(
        pool,
        redis_rs::cmd("ZRANGE")
            .arg(hench::keys::SCHEDULE)
            .arg(0)
            .arg(-1)
            .clone(),
    )
    .await
}

/// Size of the `schedule` set.
pub async fn scheduled_size(pool: &RedisPool) -> usize {
    cmd(
        pool,
        redis_rs::cmd("ZCARD").arg(hench::keys::SCHEDULE).clone(),
    )
    .await
}

/// Members of the `queues` set, sorted.
pub async fn known_queues(pool: &RedisPool) -> Vec<String> {
    let mut queues: Vec<String> = cmd(
        pool,
        redis_rs::cmd("SMEMBERS").arg(hench::keys::QUEUES).clone(),
    )
    .await;
    queues.sort();
    queues
}

/// Resets Redis command statistics.
pub async fn reset_command_stats(pool: &RedisPool) {
    let () = cmd(pool, redis_rs::cmd("CONFIG").arg("RESETSTAT").clone()).await;
}

/// How many times `command` (lowercase) ran since the last reset.
pub async fn command_calls(pool: &RedisPool, command: &str) -> u64 {
    let info: String = cmd(pool, redis_rs::cmd("INFO").arg("commandstats").clone()).await;
    let prefix = format!("cmdstat_{command}:");
    info.lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .and_then(|stats| {
            stats
                .split(',')
                .find_map(|field| field.strip_prefix("calls="))
                .and_then(|calls| calls.parse().ok())
        })
        .unwrap_or(0)
}

/// Polls `condition` every 10 ms until it holds, failing the test after `timeout`.
pub async fn wait_until<F, Fut>(timeout: Duration, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    while !condition().await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition not met within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// `true` if `jid` looks like a hench jid.
pub fn is_jid(jid: &str) -> bool {
    jid.len() == 24 && jid.chars().all(|c| matches!(c, '0'..='9' | 'a'..='f'))
}
