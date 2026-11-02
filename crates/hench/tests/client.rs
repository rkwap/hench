//! Client behaviour, ported scenario by scenario from Sidekiq's `client_test.rb`,
//! `job_util_test.rb`, `job_test.rb` (`#set`, `#perform_in`) and `sharding_test.rb`.

mod common;

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use common::*;
use hench::prelude::*;
use hench::test_util::TestRedis;
use hench::{BulkAt, Error, RetryOption, now_secs_f64, redis_rs};
use serde_json::{Value, json};

fn args(values: &[i64]) -> Vec<Value> {
    values.iter().map(|value| json!(value)).collect()
}

fn bulk_args(count: i64) -> Vec<Vec<Value>> {
    (1..=count).map(|n| vec![json!(n)]).collect()
}

// ---------------------------------------------------------------------------------------
// Validation (JobUtil#validate / normalize_item)
// ---------------------------------------------------------------------------------------

#[tokio::test]
async fn rejects_a_job_without_a_class() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let result = client.push(JobPayload::new("", args(&[1]))).await;
    assert!(matches!(result, Err(Error::InvalidJob(_))), "{result:?}");
}

#[tokio::test]
async fn rejects_an_explicitly_empty_queue() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let result = client.job::<MyJob>().queue("").perform_async((1, 2)).await;
    assert!(
        matches!(result, Err(Error::InvalidJob(ref m)) if m.contains("queue")),
        "{result:?}"
    );

    // Default job options are merged over the defaults: an explicitly empty default queue is
    // what leaves a raw job without a queue.
    let no_queue = client.with_default_job_options(JobOptions::new().queue("").retry(true));
    let result = no_queue
        .push(JobPayload::new("Blah", args(&[1, 2, 3])))
        .await;
    assert!(matches!(result, Err(Error::InvalidJob(_))), "{result:?}");
    assert_eq!(known_queues(client.redis()).await, Vec::<String>::new());
}

#[tokio::test]
async fn rejects_non_finite_at() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    for at in [f64::NAN, f64::INFINITY] {
        let mut payload = JobPayload::new("Blah", args(&[1]));
        payload.at = Some(at);
        assert!(matches!(
            client.push(payload).await,
            Err(Error::InvalidJob(_))
        ));
    }
}

/// Round 3 (web-api-security-3): a job time after the year 9999 is refused (a unique job pushed
/// at 1e20 got a lock that never expired, and the pages showing it panicked); the last second
/// of 9999 is accepted.
#[tokio::test]
async fn rejects_times_after_the_year_9999() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    for at in [1e20, 253_402_300_800.0] {
        let mut payload = JobPayload::new("Blah", args(&[1]));
        payload.at = Some(at);
        let result = client.push(payload).await;
        assert!(
            matches!(result, Err(Error::InvalidJob(ref message)) if message.contains("10000")),
            "{at}: {result:?}"
        );
    }
    let mut payload = JobPayload::new("Blah", args(&[1]));
    payload.at = Some(253_402_300_799.0);
    assert!(client.push(payload).await.unwrap().is_some());
    let far = SystemTime::UNIX_EPOCH + Duration::from_secs(300_000_000_000);
    let result = client.job::<MyJob>().at(far).perform_async((1, 2)).await;
    assert!(matches!(result, Err(Error::InvalidJob(_))), "{result:?}");
    let result = client
        .push_bulk(BulkPush::new("Blah", vec![args(&[1])]).spread_interval(Duration::MAX))
        .await;
    assert!(
        matches!(result, Err(Error::InvalidJob(ref message)) if message.contains("spread_interval")),
        "{result:?}"
    );
}

#[tokio::test]
async fn rejects_an_absurdly_large_retry_for() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let mut payload = JobPayload::new("Blah", vec![]);
    payload.retry_for = Some(2_000_000_000);
    assert!(matches!(
        client.push(payload).await,
        Err(Error::InvalidJob(_))
    ));

    let jid = client
        .job::<MyJob>()
        .retry_for(Duration::from_secs(1_000))
        .perform_async((1, 2))
        .await
        .unwrap();
    assert!(jid.is_some());
}

#[tokio::test]
async fn rejects_arguments_that_are_not_native_json() {
    #[derive(Default)]
    struct FloatJob;
    impl Job for FloatJob {
        const NAME: &'static str = "FloatJob";
        type Args = (f64,);
        async fn perform(&self, _: &JobContext, _: (f64,)) -> Result<(), JobError> {
            Ok(())
        }
    }
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let result = FloatJob::perform_async(&client, (value,)).await;
        assert!(
            matches!(result, Err(Error::InvalidJob(ref m)) if m.contains("FloatJob")),
            "{result:?}"
        );
    }
    assert!(
        FloatJob::perform_async(&client, (1.5,))
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(queue_size(client.redis(), "default").await, 1);
}

#[tokio::test]
async fn accepts_deep_json_friendly_arguments() {
    #[derive(Default)]
    struct Interesting;
    impl Job for Interesting {
        const NAME: &'static str = "Interesting";
        type Args = (Value,);
        async fn perform(&self, _: &JobContext, _: (Value,)) -> Result<(), JobError> {
            Ok(())
        }
    }
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let nested = json!({"foo": ["a", "b", "c"], "bar": ["x", "y", {"z": [1, 2.5, null, true]}]});
    Interesting::perform_async(&client, (nested.clone(),))
        .await
        .unwrap();
    assert_eq!(
        queue_jobs(client.redis(), "default").await[0].args,
        [nested]
    );
}

#[tokio::test]
async fn generates_a_jid_unless_one_is_given() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let jid = client
        .push(JobPayload::new("Blah", args(&[1, 2, 3])))
        .await
        .unwrap()
        .unwrap();
    assert!(is_jid(&jid), "{jid}");

    let mut payload = JobPayload::new("Blah", vec![]);
    payload.jid = "abc123".into();
    assert_eq!(
        client.push(payload).await.unwrap().as_deref(),
        Some("abc123")
    );
    let stored = queue_jobs(client.redis(), "default").await;
    assert_eq!(stored[0].jid, jid);
    assert_eq!(stored[1].jid, "abc123");
}

#[tokio::test]
async fn merges_job_options_but_not_for_raw_pushes() {
    #[derive(Default)]
    struct CustomQueueJob;
    impl Job for CustomQueueJob {
        const NAME: &'static str = "CustomQueueJob";
        type Args = ();
        fn options() -> JobOptions {
            JobOptions::new().queue("custom_queue").retry(5)
        }
        async fn perform(&self, _: &JobContext, (): ()) -> Result<(), JobError> {
            Ok(())
        }
    }
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    CustomQueueJob::perform_async(&client, ()).await.unwrap();
    let job = &queue_jobs(client.redis(), "custom_queue").await[0];
    assert_eq!(job.retry, Some(RetryOption::Count(5)));
    assert_eq!(job.args, Vec::<Value>::new());

    // Pushing by class name uses only the default options, like Sidekiq.
    client
        .push(JobPayload::new("CustomQueueJob", vec![]))
        .await
        .unwrap();
    let job = &queue_jobs(client.redis(), "default").await[0];
    assert_eq!(job.retry, Some(RetryOption::Bool(true)));
}

#[tokio::test]
async fn sets_created_at_once_and_enqueued_at_on_every_push() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let before = hench::now_millis();
    client.push(JobPayload::new("Blah", vec![])).await.unwrap();
    let mut payload = JobPayload::new("Blah", vec![]);
    payload.created_at = Some(123);
    payload.enqueued_at = Some(456);
    client.push(payload).await.unwrap();

    let jobs = queue_jobs(client.redis(), "default").await;
    let created = jobs[0].created_at.unwrap();
    assert!(created >= before && created <= hench::now_millis());
    assert!(jobs[0].enqueued_at.unwrap() >= created);
    assert_eq!(jobs[1].created_at, Some(123));
    assert_ne!(
        jobs[1].enqueued_at,
        Some(456),
        "enqueued_at is reset on push"
    );
    assert!(jobs.iter().all(|job| job.at.is_none()));
}

#[tokio::test]
async fn stores_retry_for_as_whole_seconds() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    client
        .job::<MyJob>()
        .retry_for(Duration::from_millis(100_900))
        .perform_async((1, 2))
        .await
        .unwrap();
    let raw = &raw_queue(client.redis(), "default").await[0];
    assert!(raw.contains(r#""retry_for":100"#), "{raw}");
}

// ---------------------------------------------------------------------------------------
// Pushing
// ---------------------------------------------------------------------------------------

#[tokio::test]
async fn pushes_messages_to_redis() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = client.redis().clone();
    let mut payload = JobPayload::new("MyJob", args(&[1, 2]));
    payload.queue = "foo".into();
    let jid = client.push(payload).await.unwrap().unwrap();
    assert!(is_jid(&jid));
    assert_eq!(queue_size(&pool, "foo").await, 1);
    assert_eq!(known_queues(&pool).await, ["foo"]);

    let job = &queue_jobs(&pool, "foo").await[0];
    assert_eq!(job.class, "MyJob");
    assert_eq!(job.args, args(&[1, 2]));
    assert_eq!(job.queue, "foo");
    assert_eq!(job.jid, jid);
    assert!(job.created_at.is_some() && job.enqueued_at.is_some());
}

#[tokio::test]
async fn queues_are_first_in_first_out() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    for n in 0..5 {
        MyJob::perform_async(&client, (n, 0)).await.unwrap();
    }
    let order: Vec<Value> = queue_jobs(client.redis(), "default")
        .await
        .into_iter()
        .map(|job| job.args[0].clone())
        .collect();
    assert_eq!(order, args(&[0, 1, 2, 3, 4]));
    let popped: Option<String> = cmd(
        client.redis(),
        redis_rs::cmd("RPOP")
            .arg(hench::keys::queue("default"))
            .clone(),
    )
    .await;
    assert_eq!(
        JobPayload::from_json(&popped.unwrap()).unwrap().args[0],
        json!(0)
    );
}

#[tokio::test]
async fn enqueues_through_every_entry_point() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = client.redis().clone();

    assert!(
        MyJob::perform_async(&client, (1, 2))
            .await
            .unwrap()
            .is_some()
    );
    assert!(client.enqueue::<MyJob>((1, 2)).await.unwrap().is_some());
    assert_eq!(queue_size(&pool, "default").await, 2);

    let to_custom = MyJob::set().queue("custom_queue");
    assert!(
        to_custom
            .perform_async(&client, (1, 2))
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(queue_size(&pool, "custom_queue").await, 1);

    // Scheduled in the future: goes to the schedule.
    to_custom
        .perform_in(&client, Duration::from_secs(3), (1, 2))
        .await
        .unwrap();
    // "In the past" (Sidekiq: enqueue_to_in with -3 seconds): pushed immediately.
    to_custom
        .perform_at(&client, SystemTime::now() - Duration::from_secs(3), (1, 2))
        .await
        .unwrap();
    assert_eq!(queue_size(&pool, "custom_queue").await, 2);
    assert_eq!(scheduled_size(&pool).await, 1);

    client
        .enqueue_in::<MyJob>(Duration::from_secs(3), (1, 2))
        .await
        .unwrap();
    assert_eq!(scheduled_size(&pool).await, 2);

    assert!(
        QueuedJob::perform_async(&client, (1,))
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(queue_size(&pool, "flimflam").await, 1);
    assert_eq!(
        known_queues(&pool).await,
        ["custom_queue", "default", "flimflam"]
    );
}

#[tokio::test]
async fn scheduled_jobs_are_stored_without_at_and_enqueued_at() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let before = now_secs_f64();
    let jid = MyJob::perform_in(&client, Duration::from_secs(60), (1, 2))
        .await
        .unwrap()
        .unwrap();
    let after = now_secs_f64();

    let raw = raw_scheduled(client.redis()).await;
    assert_eq!(raw.len(), 1);
    assert!(!raw[0].contains("\"at\""), "{}", raw[0]);
    assert!(!raw[0].contains("enqueued_at"), "{}", raw[0]);

    let (job, score) = &scheduled(client.redis()).await[0];
    assert_eq!(job.jid, jid);
    assert!(job.created_at.is_some());
    assert!(*score >= before + 60.0 && *score <= after + 60.0, "{score}");
    assert_eq!(queue_size(client.redis(), "default").await, 0);
    assert!(known_queues(client.redis()).await.is_empty());
}

#[tokio::test]
async fn perform_in_treats_huge_intervals_as_timestamps() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    MySetJob::perform_in(&client, Duration::from_secs(2_000_000_000), vec![])
        .await
        .unwrap();
    let (_, score) = scheduled(client.redis()).await[0].clone();
    assert!((score - 2_000_000_000.0).abs() < 0.001, "{score}");
}

#[tokio::test]
async fn perform_in_without_delay_enqueues_immediately() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    MySetJob::perform_in(&client, Duration::ZERO, vec![])
        .await
        .unwrap();
    MySetJob::perform_at(&client, SystemTime::now() - Duration::from_secs(60), vec![])
        .await
        .unwrap();
    assert_eq!(scheduled_size(client.redis()).await, 0);
    assert_eq!(queue_size(client.redis(), "foo").await, 2);
}

#[tokio::test]
async fn raw_pushes_with_at_schedule_only_future_jobs() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let mut future = JobPayload::new("Blah", vec![]);
    future.at = Some(now_secs_f64() + 100.0);
    client.push(future).await.unwrap();
    let mut past = JobPayload::new("Blah", vec![]);
    past.at = Some(now_secs_f64() - 100.0);
    client.push(past).await.unwrap();
    assert_eq!(scheduled_size(client.redis()).await, 1);
    let queued = queue_jobs(client.redis(), "default").await;
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].at, None);
}

#[tokio::test]
async fn keeps_custom_payload_keys() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let mut payload = JobPayload::new("Blah", vec![]);
    payload.extra.insert("locale".into(), json!("fr"));
    payload.extra.insert("queue".into(), json!("shadowed"));
    client.push(payload).await.unwrap();
    let job = &queue_jobs(client.redis(), "default").await[0];
    assert_eq!(job.extra.get("locale"), Some(&json!("fr")));
    assert_eq!(job.queue, "default");
    assert!(!job.extra.contains_key("queue"));
}

#[tokio::test]
async fn surfaces_redis_errors() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let () = cmd(
        client.redis(),
        redis_rs::cmd("SET")
            .arg(hench::keys::queue("default"))
            .arg("not a list")
            .clone(),
    )
    .await;
    let result = client.push(JobPayload::new("Blah", vec![])).await;
    assert!(matches!(result, Err(Error::Redis(_))), "{result:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_pushes_are_neither_lost_nor_duplicated() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let tasks: Vec<_> = (0..20_i64)
        .map(|task| {
            let client = client.clone();
            tokio::spawn(async move {
                let mut jids = Vec::new();
                for n in 0..50_i64 {
                    let jid = MyJob::perform_async(&client, (task, n))
                        .await
                        .unwrap()
                        .unwrap();
                    jids.push(jid);
                }
                jids
            })
        })
        .collect();
    let mut pushed = Vec::new();
    for task in tasks {
        pushed.extend(task.await.unwrap());
    }
    let mut stored: Vec<String> = queue_jobs(client.redis(), "default")
        .await
        .into_iter()
        .map(|job| job.jid)
        .collect();
    pushed.sort();
    stored.sort();
    assert_eq!(stored.len(), 1000);
    assert_eq!(stored, pushed);
}

// ---------------------------------------------------------------------------------------
// Job.set (job_test.rb)
// ---------------------------------------------------------------------------------------

#[tokio::test]
async fn set_can_be_memoized() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let setter = MySetJob::set().queue("bar").custom("foo", "qaaz");
    for _ in 0..4 {
        setter.perform_async(&client, vec![1]).await.unwrap();
    }
    let jobs = queue_jobs(client.redis(), "bar").await;
    assert_eq!(jobs.len(), 4);
    let mut jids: Vec<&str> = jobs.iter().map(|job| job.jid.as_str()).collect();
    jids.dedup();
    assert_eq!(jids.len(), 4);
    assert!(jobs.iter().all(|job| job.extra["foo"] == json!("qaaz")));
    setter
        .perform_in(&client, Duration::from_secs(10), vec![1])
        .await
        .unwrap();
    assert_eq!(scheduled_size(client.redis()).await, 1);
}

#[tokio::test]
async fn set_overrides_some_options_and_keeps_the_others() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    MySetJob::set()
        .queue("bar")
        .perform_async(&client, vec![1])
        .await
        .unwrap();
    let job = &queue_jobs(client.redis(), "bar").await[0];
    assert_eq!(job.queue, "bar");
    assert_eq!(job.retry, Some(RetryOption::Count(12)));

    MySetJob::set()
        .queue("bar")
        .retry(11)
        .perform_async(&client, vec![1])
        .await
        .unwrap();
    assert_eq!(
        queue_jobs(client.redis(), "bar").await[1].retry,
        Some(RetryOption::Count(11))
    );

    // Without `set` the job's own options apply again.
    MySetJob::perform_async(&client, vec![1]).await.unwrap();
    assert_eq!(queue_size(client.redis(), "bar").await, 2);
    let job = &queue_jobs(client.redis(), "foo").await[0];
    assert_eq!(
        (job.queue.as_str(), job.retry),
        ("foo", Some(RetryOption::Count(12)))
    );
}

#[tokio::test]
async fn set_allows_multiple_calls() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    MySetJob::set()
        .queue("foo")
        .custom("bar", "xyz")
        .tags(["a", "b"])
        .backtrace(5_u32)
        .dead(false)
        .retry_queue("low")
        .perform_async(&client, vec![])
        .await
        .unwrap();
    let job = &queue_jobs(client.redis(), "foo").await[0];
    assert_eq!(job.extra["bar"], json!("xyz"));
    assert_eq!(job.tags, ["a", "b"]);
    assert_eq!(job.backtrace, Some(hench::BacktraceOption::Lines(5)));
    assert_eq!(job.dead, Some(false));
    assert_eq!(job.retry_queue.as_deref(), Some("low"));
}

#[tokio::test]
async fn set_schedules_jobs_when_a_delay_is_chained() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let jid = MySetJob::set()
        .queue("bar")
        .delay(Duration::from_secs(3600))
        .perform_async(&client, vec![1])
        .await
        .unwrap();
    assert!(jid.is_some());
    let (job, score) = &scheduled(client.redis()).await[0];
    assert_eq!(job.queue, "bar");
    assert_eq!(job.args, args(&[1]));
    assert!(*score > now_secs_f64() + 3500.0);

    MySetJob::set()
        .at(SystemTime::now() + Duration::from_secs(3600))
        .perform_async(&client, vec![2])
        .await
        .unwrap();
    assert_eq!(scheduled_size(client.redis()).await, 2);
}

#[tokio::test]
async fn set_with_an_explicit_jid() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let jid = MySetJob::set()
        .jid("0123456789abcdef01234567")
        .perform_async(&client, vec![])
        .await
        .unwrap();
    assert_eq!(jid.as_deref(), Some("0123456789abcdef01234567"));
}

#[tokio::test]
async fn set_works_with_perform_bulk() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let jids = MySetJob::set()
        .queue("bar")
        .perform_bulk(&client, (1..=1001).map(|n| vec![n]).collect())
        .await
        .unwrap();
    assert_eq!(jids.len(), 1001);
    assert_eq!(queue_size(client.redis(), "bar").await, 1001);
}

#[tokio::test]
async fn client_job_builder() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let builder = client
        .job::<MyJob>()
        .queue("built")
        .retry(false)
        .tags(["t"]);
    let jid = builder.perform_async((1, 2)).await.unwrap().unwrap();
    builder
        .perform_in(Duration::from_secs(30), (3, 4))
        .await
        .unwrap();
    builder
        .perform_at(SystemTime::now() + Duration::from_secs(30), (5, 6))
        .await
        .unwrap();
    let bulk = builder.perform_bulk(vec![(7, 8), (9, 10)]).await.unwrap();
    assert_eq!(bulk.len(), 2);

    let queued = queue_jobs(client.redis(), "built").await;
    assert_eq!(queued.len(), 3);
    assert_eq!(queued[0].jid, jid);
    assert_eq!(queued[0].retry, Some(RetryOption::Bool(false)));
    assert_eq!(queued[0].tags, ["t"]);
    assert_eq!(scheduled_size(client.redis()).await, 2);
}

/// `options(&JobOptions)` has one meaning on every builder: the options given win over what
/// was set before (later calls win over them in turn). `BulkPush::default_options` fills only
/// what is unset.
#[tokio::test]
async fn options_overlay_on_every_builder() {
    let client = Client::fake();
    let options = JobOptions::new().queue("reports").retry(3);
    client
        .push_bulk(
            BulkPush::new("MyJob", vec![args(&[1, 2])])
                .queue("default")
                .options(&options),
        )
        .await
        .unwrap();
    MyJob::set()
        .queue("default")
        .options(&options)
        .perform_async(&client, (3, 4))
        .await
        .unwrap();
    MyJob::set()
        .queue("default")
        .options(&options)
        .perform_bulk(&client, vec![(5, 6)])
        .await
        .unwrap();
    client
        .job::<MyJob>()
        .queue("default")
        .options(&options)
        .perform_async((7, 8))
        .await
        .unwrap();
    let jobs = client.jobs::<MyJob>();
    assert_eq!(jobs.len(), 4);
    for job in &jobs {
        assert_eq!(
            (job.queue.as_str(), job.retry),
            ("reports", Some(RetryOption::Count(3))),
            "{job:?}"
        );
    }

    // Filling only what is unset, and later calls winning over `options`.
    let filled = BulkPush::new("MyJob", vec![args(&[9, 10])])
        .queue("default")
        .default_options(&options);
    assert_eq!(filled.template.queue, "default");
    assert_eq!(filled.template.retry, Some(RetryOption::Count(3)));
    let later = BulkPush::new("MyJob", vec![args(&[9, 10])])
        .options(&options)
        .queue("late");
    assert_eq!(later.template.queue, "late");
}

// ---------------------------------------------------------------------------------------
// Bulk pushes
// ---------------------------------------------------------------------------------------

#[tokio::test]
async fn pushes_a_large_set_of_jobs_at_once() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let jids = client
        .push_bulk(BulkPush::new("QueuedJob", bulk_args(1001)))
        .await
        .unwrap();
    assert_eq!(jids.len(), 1001);
    assert!(jids.iter().all(|jid| jid.as_deref().is_some_and(is_jid)));
    let mut unique: Vec<_> = jids.iter().flatten().collect();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), 1001);
    assert_eq!(queue_size(client.redis(), "default").await, 1001);

    let jobs = queue_jobs(client.redis(), "default").await;
    assert_eq!(jobs[0].args, args(&[1]));
    assert_eq!(jobs[1000].args, args(&[1001]));
    let created = jobs[0].created_at;
    assert!(jobs.iter().all(|job| job.created_at == created));
}

#[tokio::test]
async fn pushes_a_large_set_of_typed_jobs() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let jids = QueuedJob::perform_bulk(&client, (1..=1001).map(|n| (n,)).collect())
        .await
        .unwrap();
    assert_eq!(jids.len(), 1001);
    assert_eq!(queue_size(client.redis(), "flimflam").await, 1001);

    let jids = QueuedJob::set()
        .batch_size(100)
        .perform_bulk(&client, (1..=1001).map(|n| (n,)).collect())
        .await
        .unwrap();
    assert_eq!(jids.len(), 1001);
    assert_eq!(queue_size(client.redis(), "flimflam").await, 2002);
}

#[tokio::test]
async fn bulk_pushes_with_a_different_batch_size() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let jids = client
        .push_bulk(BulkPush::new("QueuedJob", bulk_args(1001)).batch_size(100))
        .await
        .unwrap();
    assert_eq!(jids.len(), 1001);
    assert_eq!(queue_size(client.redis(), "default").await, 1001);
}

#[tokio::test]
async fn bulk_handles_no_jobs() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    assert!(
        client
            .push_bulk(BulkPush::new("QueuedJob", vec![]))
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        MyJob::perform_bulk(&client, vec![])
            .await
            .unwrap()
            .is_empty()
    );
    assert!(known_queues(client.redis()).await.is_empty());
}

#[tokio::test]
async fn bulk_pushes_jobs_scheduled_at_different_times() {
    for job_count in 1..=3_usize {
        let redis = TestRedis::start().await;
        let client = redis.client().await;
        let base = now_secs_f64() + 1_000.0;
        let times: Vec<f64> = (0..job_count).map(|i| base + 86_400.0 * i as f64).collect();
        let jids = client
            .push_bulk(
                BulkPush::new(
                    "QueuedJob",
                    (0..job_count).map(|i| vec![json!(i)]).collect(),
                )
                .at_each_timestamp(times.clone()),
            )
            .await
            .unwrap();
        assert_eq!(jids.len(), job_count);
        let scheduled = scheduled(client.redis()).await;
        for (jid, time) in jids.iter().zip(&times) {
            let (_, score) = scheduled
                .iter()
                .find(|(job, _)| Some(&job.jid) == jid.as_ref())
                .unwrap();
            assert!((score - time).abs() < 0.001, "{score} != {time}");
        }
    }
}

#[tokio::test]
async fn bulk_uses_the_right_time_for_jobs_beyond_the_first_batch() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let base = now_secs_f64() + 1_000.0;
    let times: Vec<f64> = (0..5).map(|i| base + f64::from(i)).collect();
    let jids = client
        .push_bulk(
            BulkPush::new("QueuedJob", (0..5).map(|i| vec![json!(i)]).collect())
                .at_each_timestamp(times.clone())
                .batch_size(2),
        )
        .await
        .unwrap();
    let scheduled = scheduled(client.redis()).await;
    assert_eq!(scheduled.len(), 5);
    for (jid, time) in jids.iter().zip(&times) {
        let (job, score) = scheduled
            .iter()
            .find(|(job, _)| Some(&job.jid) == jid.as_ref())
            .unwrap();
        assert!((score - time).abs() < 0.001);
        assert!(job.at.is_none());
    }
}

#[tokio::test]
async fn bulk_schedules_every_job_at_one_time() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let at = SystemTime::now() + Duration::from_secs(600);
    let jids = MyJob::set()
        .at(at)
        .perform_bulk(&client, (1..=1001).map(|n| (n, n)).collect())
        .await
        .unwrap();
    assert_eq!(jids.len(), 1001);
    assert_eq!(scheduled_size(client.redis()).await, 1001);
    let expected = hench::system_time_to_secs(at);
    assert!(
        scheduled(client.redis())
            .await
            .iter()
            .all(|(_, score)| (score - expected).abs() < 0.001)
    );
}

#[tokio::test]
async fn bulk_jobs_due_now_are_pushed_immediately() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let now = now_secs_f64();
    let jids = client
        .push_bulk(
            BulkPush::new("QueuedJob", bulk_args(4)).at_each_timestamp(vec![
                now - 10.0,
                now + 100.0,
                1.0,
                now + 200.0,
            ]),
        )
        .await
        .unwrap();
    assert_eq!(jids.len(), 4);
    assert_eq!(queue_size(client.redis(), "default").await, 2);
    assert_eq!(scheduled_size(client.redis()).await, 2);
}

#[tokio::test]
async fn bulk_returns_jids() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let jids = client
        .push_bulk(BulkPush::new("QueuedJob", bulk_args(2)))
        .await
        .unwrap();
    assert!(jids.iter().all(|jid| jid.as_deref().is_some_and(is_jid)));
}

#[tokio::test]
async fn bulk_spreads_jobs_over_the_interval() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let before = now_secs_f64();
    let jids = client
        .push_bulk(
            BulkPush::new("QueuedJob", bulk_args(4)).spread_interval(Duration::from_secs(10)),
        )
        .await
        .unwrap();
    let after = now_secs_f64();
    assert_eq!(jids.len(), 4);
    // A job whose random offset has already elapsed when its batch is pushed is due, so it
    // goes straight to its queue.
    let scheduled = scheduled(client.redis()).await;
    let queued = queue_size(client.redis(), "default").await;
    assert_eq!(scheduled.len() + queued, 4);
    for (_, score) in scheduled {
        assert!(score >= before && score <= after + 10.0, "{score}");
    }
}

#[tokio::test]
async fn bulk_spread_interval_is_at_least_five_seconds() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let before = now_secs_f64();
    client
        .push_bulk(
            BulkPush::new("QueuedJob", bulk_args(200)).spread_interval(Duration::from_secs(1)),
        )
        .await
        .unwrap();
    let after = now_secs_f64();
    let scores: Vec<f64> = scheduled(client.redis())
        .await
        .into_iter()
        .map(|(_, s)| s)
        .collect();
    assert_eq!(
        scores.len() + queue_size(client.redis(), "default").await,
        200
    );
    assert!(scores.len() > 100);
    assert!(
        scores
            .iter()
            .all(|score| *score >= before && *score <= after + 5.0)
    );
    let spread = scores.last().unwrap() - scores.first().unwrap();
    // 200 uniform samples over 5 s span more than 1 s with overwhelming probability.
    assert!(spread > 1.0, "spread {spread}");
}

#[tokio::test]
async fn bulk_slices_scheduled_jobs_into_batches_of_100() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = client.redis().clone();
    reset_command_stats(&pool).await;
    client
        .push_bulk(BulkPush::new("QueuedJob", bulk_args(250)).delay(Duration::from_secs(60)))
        .await
        .unwrap();
    assert_eq!(command_calls(&pool, "zadd").await, 3);
    assert_eq!(scheduled_size(&pool).await, 250);
}

#[tokio::test]
async fn bulk_slices_spread_jobs_into_batches_of_100() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = client.redis().clone();
    reset_command_stats(&pool).await;
    client
        .push_bulk(
            BulkPush::new("QueuedJob", bulk_args(250)).spread_interval(Duration::from_secs(60)),
        )
        .await
        .unwrap();
    assert_eq!(command_calls(&pool, "zadd").await, 3);
    // Jobs already due when their batch is pushed go straight to the queue.
    assert_eq!(
        scheduled_size(&pool).await + queue_size(&pool, "default").await,
        250
    );
}

#[tokio::test]
async fn bulk_pushes_immediate_jobs_in_batches_of_1000() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = client.redis().clone();
    reset_command_stats(&pool).await;
    client
        .push_bulk(BulkPush::new("QueuedJob", bulk_args(1001)))
        .await
        .unwrap();
    assert_eq!(command_calls(&pool, "lpush").await, 2);
    assert_eq!(command_calls(&pool, "exec").await, 2);
}

#[tokio::test]
async fn bulk_honors_an_explicit_batch_size_for_scheduled_jobs() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = client.redis().clone();
    reset_command_stats(&pool).await;
    client
        .push_bulk(
            BulkPush::new("QueuedJob", bulk_args(250))
                .delay(Duration::from_secs(60))
                .batch_size(200),
        )
        .await
        .unwrap();
    assert_eq!(command_calls(&pool, "zadd").await, 2);
}

#[tokio::test]
async fn bulk_rejects_invalid_parameters() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let now = now_secs_f64();
    let cases: Vec<(&str, BulkPush)> = vec![
        (
            "fewer times than jobs",
            BulkPush::new("QueuedJob", bulk_args(2)).at_each_timestamp(vec![now]),
        ),
        (
            "more times than jobs",
            BulkPush::new("QueuedJob", bulk_args(1)).at_each_timestamp(vec![now, now]),
        ),
        (
            "empty time list",
            BulkPush::new("QueuedJob", bulk_args(1)).at_each_timestamp(vec![]),
        ),
        (
            "non-finite time",
            BulkPush::new("QueuedJob", bulk_args(2)).at_each_timestamp(vec![now, f64::NAN]),
        ),
        (
            "non-finite single time",
            BulkPush::new("QueuedJob", bulk_args(1)).at_timestamp(f64::INFINITY),
        ),
        (
            "at and spread_interval",
            BulkPush::new("QueuedJob", bulk_args(1))
                .at_timestamp(now)
                .spread_interval(Duration::from_secs(10)),
        ),
        (
            "zero spread_interval",
            BulkPush::new("QueuedJob", bulk_args(1)).spread_interval(Duration::ZERO),
        ),
        (
            "jid with several jobs",
            BulkPush::new("QueuedJob", bulk_args(2)).jid("abc"),
        ),
        (
            "zero batch size",
            BulkPush::new("QueuedJob", bulk_args(2)).batch_size(0),
        ),
        ("empty class", BulkPush::new("", bulk_args(1))),
    ];
    for (name, bulk) in cases {
        let result = client.push_bulk(bulk).await;
        assert!(
            matches!(result, Err(Error::InvalidJob(_))),
            "{name}: {result:?}"
        );
    }
    let mut template = JobPayload::new("QueuedJob", vec![]);
    template.at = Some(now + 10.0);
    let result = client
        .push_bulk(BulkPush::from_template(template, bulk_args(1)))
        .await;
    assert!(matches!(result, Err(Error::InvalidJob(_))));
    assert!(known_queues(client.redis()).await.is_empty());
    assert_eq!(scheduled_size(client.redis()).await, 0);
}

#[tokio::test]
async fn bulk_accepts_an_explicit_jid_for_a_single_job() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let jids = client
        .push_bulk(BulkPush::new("QueuedJob", bulk_args(1)).jid("explicit"))
        .await
        .unwrap();
    assert_eq!(jids, [Some("explicit".to_owned())]);
    assert_eq!(
        queue_jobs(client.redis(), "default").await[0].jid,
        "explicit"
    );
}

#[tokio::test]
async fn bulk_applies_template_and_options() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    // `default_options` fills what the template leaves unset (`options` would overlay).
    let bulk = BulkPush::new("QueuedJob", bulk_args(2))
        .queue("bulk")
        .retry(3)
        .default_options(&JobOptions::new().tags(["x"]).queue("ignored"));
    client.push_bulk(bulk).await.unwrap();
    let jobs = queue_jobs(client.redis(), "bulk").await;
    assert_eq!(jobs.len(), 2);
    assert!(
        jobs.iter()
            .all(|job| job.retry == Some(RetryOption::Count(3)) && job.tags == ["x"])
    );
    assert_eq!(
        BulkPush::new("Q", vec![]).at_timestamp(5.0).at,
        Some(BulkAt::All(5.0))
    );
}

// ---------------------------------------------------------------------------------------
// Client middleware
// ---------------------------------------------------------------------------------------

/// Records what it sees, then continues.
#[derive(Clone, Default)]
struct Spy {
    seen: Arc<Mutex<Vec<JobPayload>>>,
}

#[async_trait]
impl ClientMiddleware for Spy {
    async fn call(
        &self,
        job: JobPayload,
        redis: &RedisPool,
        next: ClientNext<'_>,
    ) -> hench::Result<Option<JobPayload>> {
        // The pool is usable from middleware.
        let mut conn = redis.conn().await?;
        let () = redis_rs::cmd("SET")
            .arg("spy")
            .arg(&job.jid)
            .query_async(&mut conn)
            .await?;
        self.seen.lock().unwrap().push(job.clone());
        next.run(job).await
    }
}

/// Lets only jobs whose first argument is odd through.
struct Stopper;

#[async_trait]
impl ClientMiddleware for Stopper {
    async fn call(
        &self,
        job: JobPayload,
        _redis: &RedisPool,
        next: ClientNext<'_>,
    ) -> hench::Result<Option<JobPayload>> {
        if job
            .args
            .first()
            .and_then(Value::as_i64)
            .is_some_and(|n| n % 2 == 1)
        {
            next.run(job).await
        } else {
            Ok(None)
        }
    }
}

/// Routes jobs to `odd_queue` / `even_queue`.
struct DynamicQueue;

#[async_trait]
impl ClientMiddleware for DynamicQueue {
    async fn call(
        &self,
        mut job: JobPayload,
        _redis: &RedisPool,
        next: ClientNext<'_>,
    ) -> hench::Result<Option<JobPayload>> {
        let odd = job
            .args
            .first()
            .and_then(Value::as_i64)
            .is_some_and(|n| n % 2 == 1);
        job.queue = if odd { "odd_queue" } else { "even_queue" }.to_owned();
        next.run(job).await
    }
}

#[tokio::test]
async fn push_sends_normalized_jobs_and_the_pool_to_middleware() {
    let redis = TestRedis::start().await;
    let spy = Spy::default();
    let client = redis.client().await.with_middleware(|chain| {
        chain.add(spy.clone());
    });
    let jid = MyJob::perform_async(&client, (0, 0))
        .await
        .unwrap()
        .unwrap();
    let seen = spy.seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 1);
    let job = &seen[0];
    assert_eq!(
        (job.class.as_str(), job.queue.as_str(), job.jid.as_str()),
        ("MyJob", "default", jid.as_str())
    );
    assert_eq!(job.args, args(&[0, 0]));
    assert!(job.created_at.is_some());
    let flag: String = cmd(client.redis(), redis_rs::cmd("GET").arg("spy").clone()).await;
    assert_eq!(flag, jid);
}

#[tokio::test]
async fn push_bulk_sends_every_job_to_middleware() {
    let redis = TestRedis::start().await;
    let spy = Spy::default();
    let client = redis.client().await.with_middleware(|chain| {
        chain.add(spy.clone());
    });
    let jids = client
        .push_bulk(BulkPush::new("MyJob", bulk_args(3)))
        .await
        .unwrap();
    let seen = spy.seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 3);
    for (job, jid) in seen.iter().zip(&jids) {
        assert_eq!(Some(&job.jid), jid.as_ref());
        assert_eq!(job.class, "MyJob");
        assert!(job.created_at.is_some());
    }
}

#[tokio::test]
async fn middleware_can_stop_some_jobs() {
    let redis = TestRedis::start().await;
    let client = redis.client().await.with_middleware(|chain| {
        chain.add(Stopper);
    });
    assert_eq!(MyJob::perform_async(&client, (0, 0)).await.unwrap(), None);
    assert!(
        MyJob::perform_async(&client, (1, 0))
            .await
            .unwrap()
            .is_some_and(|jid| is_jid(&jid))
    );
    let result = client
        .push_bulk(BulkPush::new("MyJob", vec![args(&[0]), args(&[1])]))
        .await
        .unwrap();
    assert_eq!(result.len(), 2);
    assert!(result[0].is_none());
    assert!(result[1].as_deref().is_some_and(is_jid));
    assert_eq!(queue_size(client.redis(), "default").await, 2);
}

#[tokio::test]
async fn middleware_can_stop_bulk_jobs() {
    struct OnlyOne;
    #[async_trait]
    impl ClientMiddleware for OnlyOne {
        async fn call(
            &self,
            job: JobPayload,
            _redis: &RedisPool,
            next: ClientNext<'_>,
        ) -> hench::Result<Option<JobPayload>> {
            if job.args[0] == json!(1) {
                next.run(job).await
            } else {
                Ok(None)
            }
        }
    }
    let redis = TestRedis::start().await;
    let client = redis.client().await.with_middleware(|chain| {
        chain.add(OnlyOne);
    });
    let result = client
        .push_bulk(BulkPush::new("Blah", bulk_args(3)))
        .await
        .unwrap();
    let sizes: Vec<usize> = result
        .iter()
        .map(|jid| jid.as_ref().map_or(0, String::len))
        .collect();
    assert_eq!(sizes, [24, 0, 0]);
    assert_eq!(queue_size(client.redis(), "default").await, 1);

    // Nothing at all passes: no round trip.
    reset_command_stats(client.redis()).await;
    let result = client
        .push_bulk(BulkPush::new("Blah", vec![args(&[5]), args(&[6])]))
        .await
        .unwrap();
    assert_eq!(result, [None, None]);
    assert_eq!(command_calls(client.redis(), "exec").await, 0);
}

#[tokio::test]
async fn middleware_routes_bulk_jobs_to_queues() {
    let redis = TestRedis::start().await;
    let client = redis.client().await.with_middleware(|chain| {
        chain.add(DynamicQueue);
    });
    client
        .push_bulk(BulkPush::new(
            "MyJob",
            vec![args(&[0]), args(&[1]), args(&[2])],
        ))
        .await
        .unwrap();
    assert_eq!(queue_size(client.redis(), "even_queue").await, 2);
    assert_eq!(queue_size(client.redis(), "odd_queue").await, 1);
    assert_eq!(
        known_queues(client.redis()).await,
        ["even_queue", "odd_queue"]
    );
}

#[tokio::test]
async fn middleware_is_local_to_a_client() {
    let redis = TestRedis::start().await;
    let base = redis.client().await;
    let spy = Spy::default();
    let with_spy = base.with_middleware(|chain| {
        chain.add(spy.clone());
    });
    base.push(JobPayload::new("Blah", vec![])).await.unwrap();
    assert!(spy.seen.lock().unwrap().is_empty());
    with_spy
        .push(JobPayload::new("Blah", vec![]))
        .await
        .unwrap();
    assert_eq!(spy.seen.lock().unwrap().len(), 1);
    assert!(with_spy.middleware().exists::<Spy>());
    assert!(!base.middleware().exists::<Spy>());
}

#[tokio::test]
async fn middleware_errors_fail_the_push() {
    struct Failing;
    #[async_trait]
    impl ClientMiddleware for Failing {
        async fn call(
            &self,
            _job: JobPayload,
            _redis: &RedisPool,
            _next: ClientNext<'_>,
        ) -> hench::Result<Option<JobPayload>> {
            Err(Error::InvalidJob("rejected by middleware".into()))
        }
    }
    let redis = TestRedis::start().await;
    let client = redis.client().await.with_middleware(|chain| {
        chain.add(Failing);
    });
    let result = MyJob::perform_async(&client, (1, 2)).await;
    assert!(matches!(result, Err(Error::InvalidJob(ref m)) if m.contains("middleware")));
    assert!(known_queues(client.redis()).await.is_empty());
}

#[tokio::test]
async fn middleware_output_is_validated() {
    struct ClearsQueue;
    #[async_trait]
    impl ClientMiddleware for ClearsQueue {
        async fn call(
            &self,
            mut job: JobPayload,
            _redis: &RedisPool,
            next: ClientNext<'_>,
        ) -> hench::Result<Option<JobPayload>> {
            job.queue.clear();
            next.run(job).await
        }
    }
    let redis = TestRedis::start().await;
    let client = redis.client().await.with_middleware(|chain| {
        chain.add(ClearsQueue);
    });
    let result = client.push(JobPayload::new("Blah", vec![])).await;
    assert!(matches!(result, Err(Error::InvalidJob(_))), "{result:?}");
}

#[tokio::test]
async fn middleware_can_schedule_jobs() {
    struct Delay;
    #[async_trait]
    impl ClientMiddleware for Delay {
        async fn call(
            &self,
            mut job: JobPayload,
            _redis: &RedisPool,
            next: ClientNext<'_>,
        ) -> hench::Result<Option<JobPayload>> {
            job.at = Some(now_secs_f64() + 300.0);
            next.run(job).await
        }
    }
    let redis = TestRedis::start().await;
    let client = redis.client().await.with_middleware(|chain| {
        chain.add(Delay);
    });
    client.push(JobPayload::new("Blah", vec![])).await.unwrap();
    assert_eq!(scheduled_size(client.redis()).await, 1);
    assert_eq!(queue_size(client.redis(), "default").await, 0);
}

// ---------------------------------------------------------------------------------------
// Sharding (sharding_test.rb): one client per Redis
// ---------------------------------------------------------------------------------------

#[tokio::test]
async fn clients_route_jobs_to_their_own_shard() {
    let shard1 = TestRedis::start().await;
    let shard2 = TestRedis::start().await;
    let client1 = shard1.client().await;
    let client2 = client1.with_redis(shard2.pool().await);

    MyJob::perform_async(&client1, (1, 2)).await.unwrap();
    MyJob::perform_in(&client1, Duration::from_secs(3), (1, 2))
        .await
        .unwrap();
    assert_eq!(queue_size(client1.redis(), "default").await, 1);
    assert_eq!(scheduled_size(client1.redis()).await, 1);
    assert_eq!(queue_size(client2.redis(), "default").await, 0);
    assert_eq!(scheduled_size(client2.redis()).await, 0);

    MyJob::set().perform_async(&client2, (1, 2)).await.unwrap();
    MyJob::set()
        .perform_in(&client2, Duration::from_secs(4), (1, 2))
        .await
        .unwrap();
    assert_eq!(queue_size(client2.redis(), "default").await, 1);
    assert_eq!(scheduled_size(client2.redis()).await, 1);
    assert_eq!(queue_size(client1.redis(), "default").await, 1);
}

#[tokio::test]
async fn middleware_gets_the_pool_of_its_shard() {
    let shard1 = TestRedis::start().await;
    let shard2 = TestRedis::start().await;
    let spy = Spy::default();
    let client1 = shard1.client().await.with_middleware(|chain| {
        chain.add(spy.clone());
    });
    let client2 = client1.with_redis(shard2.pool().await);

    let flags = |client: Client| async move {
        let flag: Option<String> =
            cmd(client.redis(), redis_rs::cmd("GET").arg("spy").clone()).await;
        flag
    };
    let jid1 = MyJob::perform_async(&client1, (1, 2)).await.unwrap();
    assert_eq!(flags(client1.clone()).await, jid1);
    assert_eq!(flags(client2.clone()).await, None);
    let jid2 = MyJob::perform_async(&client2, (1, 2)).await.unwrap();
    assert_eq!(flags(client1.clone()).await, jid1);
    assert_eq!(flags(client2.clone()).await, jid2);
}

// ---------------------------------------------------------------------------------------
// Resilience: failover and reconnection
// ---------------------------------------------------------------------------------------

#[tokio::test]
async fn pushes_survive_a_redis_restart() {
    let mut redis = TestRedis::start().await;
    let client = redis.client().await;
    MyJob::perform_async(&client, (1, 2)).await.unwrap();
    redis.restart().await;
    // The first attempt hits the dead connection; the push is retried on a new one.
    let jid = MyJob::perform_async(&client, (3, 4)).await.unwrap();
    assert!(jid.is_some());
    let jobs = queue_jobs(client.redis(), "default").await;
    assert_eq!(jobs.len(), 1, "the restarted server starts empty");
    assert_eq!(jobs[0].args, args(&[3, 4]));
}

#[tokio::test]
async fn pushes_fail_while_redis_is_down_and_recover() {
    let mut redis = TestRedis::start().await;
    let config = redis
        .config()
        .reconnect_retries(1)
        .reconnect_delays(Duration::from_millis(10), Duration::from_millis(20));
    let client = Client::new(RedisPool::from_config(config).await.unwrap());
    redis.stop();
    let result = MyJob::perform_async(&client, (1, 2)).await;
    assert!(matches!(result, Err(Error::Redis(_))), "{result:?}");
    redis.restart().await;
    wait_until(Duration::from_secs(10), || {
        let client = client.clone();
        async move { MyJob::perform_async(&client, (1, 2)).await.is_ok() }
    })
    .await;
    assert!(queue_size(client.redis(), "default").await >= 1);
}

#[tokio::test]
async fn pushes_to_a_read_only_replica_fail_after_one_retry() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = client.redis().clone();
    // Turn the server into a replica of an unreachable primary: writes now fail with READONLY.
    let () = cmd(
        &pool,
        redis_rs::cmd("REPLICAOF").arg("127.0.0.1").arg("1").clone(),
    )
    .await;
    reset_command_stats(&pool).await;
    let result = MyJob::perform_async(&client, (1, 2)).await;
    assert!(matches!(result, Err(Error::Redis(_))), "{result:?}");
    assert_eq!(
        command_calls(&pool, "multi").await,
        2,
        "pushed, failed, retried once"
    );

    let () = cmd(
        &pool,
        redis_rs::cmd("REPLICAOF").arg("NO").arg("ONE").clone(),
    )
    .await;
    assert!(
        MyJob::perform_async(&client, (1, 2))
            .await
            .unwrap()
            .is_some()
    );
}

/// Default job options are merged over the defaults, like Sidekiq's `default_job_options=`:
/// setting one option (here `backtrace`) keeps the default queue and retries (it used to
/// replace them, so every push of a job without its own queue failed).
#[tokio::test]
async fn default_job_options_are_merged_over_the_defaults() {
    let redis = TestRedis::start().await;
    let hench = Hench::builder()
        .redis(redis.pool().await)
        .default_job_options(JobOptions::new().backtrace(true))
        .default_job_options(JobOptions::new().tags(["merged"]))
        .build()
        .await
        .unwrap();
    let options = &hench.config().default_job_options;
    assert_eq!(options.queue.as_deref(), Some("default"));
    assert_eq!(options.retry, Some(RetryOption::Bool(true)));
    assert_eq!(options.backtrace, Some(true.into()));
    assert_eq!(options.tags.as_deref(), Some(&["merged".to_owned()][..]));
    // A raw push, without a queue of its own, lands in the default queue with the options.
    let jid = hench
        .client()
        .push(JobPayload::new("Anything", args(&[1])))
        .await
        .unwrap()
        .unwrap();
    let queued: Vec<String> = cmd(
        hench.redis(),
        redis_rs::cmd("LRANGE")
            .arg("queue:default")
            .arg(0)
            .arg(-1)
            .clone(),
    )
    .await;
    assert_eq!(queued.len(), 1);
    let job = JobPayload::from_json(&queued[0]).unwrap();
    assert_eq!(job.jid, jid);
    assert_eq!(job.backtrace, Some(true.into()));
    assert_eq!(job.retry, Some(RetryOption::Bool(true)));
    // Testing clients merge the same way.
    let fake = hench::testing::Testing::new()
        .default_job_options(JobOptions::new().backtrace(true))
        .fake();
    fake.push(JobPayload::new("Anything", args(&[1])))
        .await
        .unwrap();
    let captured = fake.jobs_for("Anything");
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].queue, "default");
    assert_eq!(captured[0].backtrace, Some(true.into()));
}

/// Typed bulk pushes can spread their jobs over an interval or give each job its own time
/// (Sidekiq 8.0.8's `perform_bulk(args, spread_interval:)` and `at: [...]`), keeping the job
/// type's options: a raw `BulkPush` had both but dropped the type's queue and retries.
#[tokio::test]
async fn typed_bulk_pushes_spread_or_schedule_each_job() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;

    let args: Vec<Vec<i64>> = (0..100).map(|i| vec![i]).collect();
    let jids = MySetJob::set()
        .spread_interval(Duration::from_secs(3600))
        .perform_bulk(&client, args)
        .await
        .unwrap();
    assert_eq!(jids.len(), 100);
    let spread = scheduled(&pool).await;
    assert_eq!(spread.len(), 100);
    assert!(spread.iter().all(|(job, _)| job.queue == "foo"));
    assert!(
        spread
            .iter()
            .all(|(job, _)| job.retry == Some(RetryOption::Count(12)))
    );
    let now = now_secs_f64();
    let (first, last) = (spread[0].1, spread[99].1);
    assert!(first >= now - 1.0 && last <= now + 3601.0, "{first} {last}");
    assert!(last - first > 1_000.0, "spread over {} s", last - first);
    let _: () = cmd(
        &pool,
        redis_rs::cmd("DEL").arg(hench::keys::SCHEDULE).clone(),
    )
    .await;

    // One time per job, through the client's builder.
    let base = SystemTime::now() + Duration::from_secs(600);
    let jobs: Vec<(Vec<i64>, SystemTime)> = (0..3)
        .map(|i| (vec![i], base + Duration::from_secs(60 * i as u64)))
        .collect();
    let jids = client
        .job::<MySetJob>()
        .perform_bulk_at(jobs)
        .await
        .unwrap();
    assert_eq!(jids.len(), 3);
    let each = scheduled(&pool).await;
    let at = hench::system_time_to_secs(base);
    let scores: Vec<f64> = each.iter().map(|(_, score)| score - at).collect();
    assert!(
        scores
            .iter()
            .zip([0.0, 60.0, 120.0])
            .all(|(got, want)| (got - want).abs() < 0.01),
        "{scores:?}"
    );
    assert!(each.iter().all(|(job, _)| job.queue == "foo"));

    // Per-job times exclude a shared schedule.
    let error = MySetJob::set()
        .delay(Duration::from_secs(5))
        .perform_bulk_at(&client, vec![(vec![1], base)])
        .await
        .unwrap_err();
    assert!(matches!(error, Error::InvalidJob(_)), "{error}");
}
