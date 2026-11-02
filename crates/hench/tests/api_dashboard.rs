//! Data API helpers added for the dashboard (`hench-web`): `Queue::exists` and
//! `JobSet::add_all_to_queue`.

use std::time::{Duration, SystemTime};

use hench::JobPayload;
use hench::api::Api;
use hench::redis_rs;
use hench::test_util::TestRedis;
use serde_json::json;

/// A job of class `class` for `queue`.
fn job(class: &str, args: Vec<serde_json::Value>, queue: &str) -> JobPayload {
    let mut job = JobPayload::new(class, args);
    job.queue = queue.to_owned();
    job
}

async fn setup() -> (TestRedis, Api) {
    let redis = TestRedis::start().await;
    let api = Api::new(redis.pool().await);
    (redis, api)
}

async fn raw(api: &Api, command: &redis_rs::Cmd) -> Vec<String> {
    let mut conn = api.redis().conn().await.unwrap();
    command.query_async(&mut conn).await.unwrap()
}

#[tokio::test]
async fn queues_exist_when_known_filled_or_paused() {
    let (_redis, api) = setup().await;
    let queue = api.queue("mail");
    assert!(!queue.exists().await.unwrap());

    // Paused before its first job.
    queue.pause().await.unwrap();
    assert!(queue.exists().await.unwrap());
    queue.unpause().await.unwrap();
    assert!(!queue.exists().await.unwrap());

    // Pushed to: in the `queues` set and holding a job.
    api.client()
        .push(job("Mailer", vec![json!(1)], "mail"))
        .await
        .unwrap();
    assert!(queue.exists().await.unwrap());

    // Emptied by workers: still known.
    let mut conn = api.redis().conn().await.unwrap();
    let _: Option<String> = redis_rs::cmd("RPOP")
        .arg("queue:mail")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(queue.size().await.unwrap(), 0);
    assert!(queue.exists().await.unwrap());

    // Cleared: forgotten.
    queue.clear().await.unwrap();
    assert!(!queue.exists().await.unwrap());
    assert!(!api.queue("never").exists().await.unwrap());
}

#[tokio::test]
async fn add_all_to_queue_enqueues_every_scheduled_job_without_touching_retry_counts() {
    let (_redis, api) = setup().await;
    let scheduled = api.scheduled();
    let later = SystemTime::now() + Duration::from_secs(3_600);
    for index in 0..250_i64 {
        let mut report = job("Report", vec![json!(index)], "reports");
        report.jid = format!("{index:024x}");
        if index == 0 {
            report.retry_count = Some(3);
        }
        scheduled.schedule(later, &report).await.unwrap();
    }
    // One entry that is not a job stays where it is.
    let mut conn = api.redis().conn().await.unwrap();
    let _: i64 = redis_rs::cmd("ZADD")
        .arg("schedule")
        .arg(1.0)
        .arg("{ not json")
        .query_async(&mut conn)
        .await
        .unwrap();

    assert_eq!(scheduled.add_all_to_queue().await.unwrap(), 250);
    assert_eq!(scheduled.size().await.unwrap(), 1);
    assert_eq!(api.queue("reports").size().await.unwrap(), 250);

    let jobs = raw(
        &api,
        redis_rs::cmd("LRANGE").arg("queue:reports").arg(0).arg(-1),
    )
    .await;
    let first = jobs
        .iter()
        .map(|json| JobPayload::from_json(json).unwrap())
        .find(|job| job.jid == format!("{:024x}", 0))
        .unwrap();
    assert_eq!(
        first.retry_count,
        Some(3),
        "add_all_to_queue keeps retry counts"
    );
    assert!(first.at.is_none() && first.enqueued_at.is_some());

    // retry_all, by contrast, decrements them.
    let retries = api.retries();
    let mut failed = job("Report", vec![], "reports");
    failed.jid = "f".repeat(24);
    failed.retry_count = Some(3);
    retries.schedule(later, &failed).await.unwrap();
    assert_eq!(retries.retry_all().await.unwrap(), 1);
    let retried = raw(
        &api,
        redis_rs::cmd("LRANGE").arg("queue:reports").arg(0).arg(0),
    )
    .await;
    assert_eq!(
        JobPayload::from_json(&retried[0]).unwrap().retry_count,
        Some(2)
    );
}
