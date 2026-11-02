//! The scheduler (Sidekiq's `scheduled_test.rb` and `scheduling_test.rb` scenarios): due jobs
//! of the retry and schedule sets go back to their queues through the client, invalid ones to
//! the morgue, and dead processes are pruned.

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use hench::prelude::*;
use hench::test_util::TestRedis;
use hench::{Error, Hench, Launcher, keys};
use serde_json::json;
use support::*;

async fn running(builder: hench::HenchBuilder) -> (Hench, Launcher) {
    init_logging();
    let hench = builder.build().await.unwrap();
    let launcher = hench.start().await.unwrap();
    (hench, launcher)
}

/// A builder whose workers only process `elsewhere`, so enqueued jobs stay in their queues.
async fn idle_builder(redis: &TestRedis) -> hench::HenchBuilder {
    redis.builder().await.queues(["elsewhere"])
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
        if job.args.first().and_then(|arg| arg.as_i64()).unwrap_or(0) % 2 == 1 {
            next.run(job).await
        } else {
            Ok(None)
        }
    }
}

#[tokio::test]
async fn client_middleware_runs_for_due_jobs() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let past = hench::now_secs_f64() - 60.0;
    zadd(
        &pool,
        keys::RETRY,
        past,
        &payload("Scheduled", vec![json!(0)], "queue_1"),
    )
    .await;
    zadd(
        &pool,
        keys::RETRY,
        past,
        &payload("Scheduled", vec![json!(1)], "queue_2"),
    )
    .await;
    zadd(
        &pool,
        keys::SCHEDULE,
        past,
        &payload("Scheduled", vec![json!(4)], "queue_5"),
    )
    .await;
    zadd(
        &pool,
        keys::SCHEDULE,
        past,
        &payload("Scheduled", vec![json!(5)], "queue_6"),
    )
    .await;
    let (_hench, launcher) = running(idle_builder(&redis).await.client_middleware(Stopper)).await;
    wait_for("the due jobs to be handled", WAIT, || async {
        zcard(&pool, keys::RETRY).await == 0 && zcard(&pool, keys::SCHEDULE).await == 0
    })
    .await;
    assert_eq!(queue_size(&pool, "queue_1").await, 0);
    assert_eq!(queue_size(&pool, "queue_2").await, 1);
    assert_eq!(queue_size(&pool, "queue_5").await, 0);
    assert_eq!(queue_size(&pool, "queue_6").await, 1);
    launcher.stop().await.unwrap();
}

#[tokio::test]
async fn due_jobs_are_enqueued_and_future_ones_kept() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let now = hench::now_secs_f64();
    let created = 1_359_849_600_000_i64; // 2013-02-03
    let job = |arg: i64, queue: &str| {
        let mut job = payload("Scheduled", vec![json!(arg)], queue);
        job.created_at = Some(created);
        job
    };
    zadd(&pool, keys::RETRY, now - 60.0, &job(0, "queue_1")).await;
    zadd(&pool, keys::RETRY, now - 50.0, &job(1, "queue_2")).await;
    zadd(&pool, keys::RETRY, now + 600.0, &job(2, "queue_3")).await;
    zadd(&pool, keys::SCHEDULE, now - 60.0, &job(3, "queue_4")).await;
    zadd(&pool, keys::SCHEDULE, now - 50.0, &job(4, "queue_5")).await;
    zadd(&pool, keys::SCHEDULE, now + 600.0, &job(5, "queue_6")).await;
    let (_hench, launcher) = running(idle_builder(&redis).await).await;
    wait_for("the due jobs to be enqueued", WAIT, || async {
        zcard(&pool, keys::RETRY).await == 1 && zcard(&pool, keys::SCHEDULE).await == 1
    })
    .await;
    for queue in ["queue_1", "queue_2", "queue_4", "queue_5"] {
        let jobs = queue_jobs(&pool, queue).await;
        assert_eq!(jobs.len(), 1, "{queue}");
        let enqueued_at = jobs[0].enqueued_at.unwrap();
        assert!(
            (enqueued_at - hench::now_millis()).abs() < 10_000,
            "{queue}"
        );
        assert_eq!(jobs[0].created_at, Some(created));
        assert_eq!(jobs[0].at, None);
    }
    for queue in ["queue_3", "queue_6"] {
        assert_eq!(queue_size(&pool, queue).await, 0);
    }
    assert!(
        smembers(&pool, keys::QUEUES)
            .await
            .contains(&"queue_1".to_owned())
    );
    launcher.stop().await.unwrap();
}

#[tokio::test]
async fn quiet_processes_do_not_enqueue() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let (_hench, launcher) = running(idle_builder(&redis).await).await;
    launcher.quiet().await;
    let past = hench::now_secs_f64() - 60.0;
    zadd(
        &pool,
        keys::RETRY,
        past,
        &payload("Scheduled", vec![], "queue_1"),
    )
    .await;
    zadd(
        &pool,
        keys::SCHEDULE,
        past,
        &payload("Scheduled", vec![], "queue_4"),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(zcard(&pool, keys::RETRY).await, 1);
    assert_eq!(zcard(&pool, keys::SCHEDULE).await, 1);
    launcher.stop().await.unwrap();
}

#[tokio::test]
async fn scheduled_jobs_run_when_due() {
    let redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    let (hench, launcher) =
        running(redis.builder().await.register(Echo).data_arc(runs.clone())).await;
    let pool = hench.redis().clone();
    Echo::perform_in(
        hench.client(),
        Duration::from_millis(300),
        ("later".into(),),
    )
    .await
    .unwrap();
    assert_eq!(zcard(&pool, keys::SCHEDULE).await, 1);
    assert_eq!(runs.len(), 0);
    runs.wait_for(1).await;
    assert_eq!(zcard(&pool, keys::SCHEDULE).await, 0);
    launcher.stop().await.unwrap();
}

#[tokio::test]
async fn retries_run_again() {
    /// Fails the first time, with a one-millisecond retry delay.
    #[derive(Default)]
    struct Once(AtomicUsize);
    impl Job for Once {
        const NAME: &'static str = "Once";
        type Args = ();
        async fn perform(&self, ctx: &JobContext, (): ()) -> Result<(), JobError> {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(JobError::msg("first attempt fails"));
            }
            ctx.data::<Runs>()?
                .record(format!("retry {:?}", ctx.retry_count()));
            Ok(())
        }
        fn retry_in(&self, _: u32, _: &JobError, _: &JobPayload) -> hench::RetryIn {
            hench::RetryIn::After(Duration::from_millis(1))
        }
    }
    let redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    let (hench, launcher) = running(
        redis
            .builder()
            .await
            .register(Once::default())
            .data_arc(runs.clone()),
    )
    .await;
    let pool = hench.redis().clone();
    Once::perform_async(hench.client(), ()).await.unwrap();
    wait_for("the retry", WAIT, || async {
        zcard(&pool, keys::RETRY).await == 1 || runs.len() == 1
    })
    .await;
    // Make the retry due now instead of waiting for the jitter (XX: unless the scheduler
    // already took it).
    if let Some((member, _)) = zrange(&pool, keys::RETRY).await.pop() {
        let _: i64 = cmd(
            &pool,
            &redis_cmd!("ZADD", keys::RETRY, "XX", hench::now_secs_f64(), member),
        )
        .await;
    }
    runs.wait_for(1).await;
    assert_eq!(runs.entries(), ["retry Some(0)"]);
    launcher.stop().await.unwrap();
}

#[tokio::test]
async fn invalid_scheduled_jobs_go_to_the_morgue() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let errors = Errors::default();
    let past = hench::now_secs_f64() - 1.0;
    let _: i64 = cmd(&pool, &redis_cmd!("ZADD", keys::SCHEDULE, past, "{broken")).await;
    // Valid JSON but not a valid job (an empty class).
    let _: i64 = cmd(
        &pool,
        &redis_cmd!("ZADD", keys::RETRY, past, r#"{"class":"","args":[]}"#),
    )
    .await;
    let (_hench, launcher) =
        running(idle_builder(&redis).await.error_handler(errors.handler())).await;
    wait_for("both jobs in the morgue", WAIT, || async {
        zcard(&pool, keys::DEAD).await == 2
    })
    .await;
    assert_eq!(zcard(&pool, keys::SCHEDULE).await, 0);
    assert_eq!(zcard(&pool, keys::RETRY).await, 0);
    assert_eq!(
        errors.with_context("scheduled job is not valid JSON").len(),
        1
    );
    assert_eq!(errors.with_context("scheduled job is invalid").len(), 1);
    launcher.stop().await.unwrap();
}

/// Fails the first `n` pushes with a Redis error.
struct Flaky(AtomicUsize);

#[async_trait]
impl ClientMiddleware for Flaky {
    async fn call(
        &self,
        job: JobPayload,
        _redis: &RedisPool,
        next: ClientNext<'_>,
    ) -> hench::Result<Option<JobPayload>> {
        // Decrement while positive. A compare-exchange loop instead of
        // `fetch_update`, which newer toolchains deprecate (its replacement,
        // `try_update`, is newer than the MSRV).
        let mut current = self.0.load(Ordering::SeqCst);
        let mut fail = false;
        while let Some(next) = current.checked_sub(1) {
            match self
                .0
                .compare_exchange_weak(current, next, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => {
                    fail = true;
                    break;
                }
                Err(actual) => current = actual,
            }
        }
        if fail {
            return Err(Error::Redis(hench::redis_rs::RedisError::from(
                std::io::Error::other("connection lost"),
            )));
        }
        next.run(job).await
    }
}

#[tokio::test]
async fn jobs_that_cannot_be_pushed_are_put_back() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let errors = Errors::default();
    let score = (hench::now_secs_f64() - 30.0).floor();
    let job = payload("Scheduled", vec![json!(1)], "queue_1");
    zadd(&pool, keys::SCHEDULE, score, &job).await;
    let (_hench, launcher) = running(
        idle_builder(&redis)
            .await
            .client_middleware(Flaky(AtomicUsize::new(2)))
            .error_handler(errors.handler()),
    )
    .await;
    wait_for("the job to be enqueued eventually", WAIT, || async {
        queue_size(&pool, "queue_1").await == 1
    })
    .await;
    assert_eq!(zcard(&pool, keys::SCHEDULE).await, 0);
    // Reported once, not once per attempt.
    assert_eq!(
        errors
            .with_context("could not enqueue scheduled jobs")
            .len(),
        1
    );
    launcher.stop().await.unwrap();
}

#[tokio::test]
async fn failed_pushes_keep_the_original_score() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let score = (hench::now_secs_f64() - 30.0).floor();
    let job = payload("Scheduled", vec![json!(1)], "queue_1");
    zadd(&pool, keys::SCHEDULE, score, &job).await;
    let (_hench, launcher) = running(
        idle_builder(&redis)
            .await
            .client_middleware(Flaky(AtomicUsize::new(usize::MAX / 2))),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    launcher.stop().await.unwrap();
    let scheduled = zrange(&pool, keys::SCHEDULE).await;
    assert_eq!(scheduled.len(), 1);
    assert_eq!(scheduled[0].1, score);
    assert_eq!(JobPayload::from_slice(&scheduled[0].0).unwrap(), job);
}

#[tokio::test]
async fn dead_processes_are_pruned() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let _: i64 = cmd(&pool, &redis_cmd!("SADD", keys::PROCESSES, "dead:1:abc")).await;
    let _: i64 = cmd(&pool, &redis_cmd!("SADD", keys::PROCESSES, "alive:2:def")).await;
    let _: i64 = cmd(&pool, &redis_cmd!("HSET", "alive:2:def", "info", "{}")).await;
    let (_hench, launcher) = running(redis.builder().await).await;
    let me = launcher.identity().to_owned();
    wait_for("the dead process to be pruned", WAIT, || async {
        !smembers(&pool, keys::PROCESSES)
            .await
            .contains(&"dead:1:abc".to_owned())
    })
    .await;
    let processes = smembers(&pool, keys::PROCESSES).await;
    assert!(processes.contains(&"alive:2:def".to_owned()));
    assert!(processes.contains(&me));
    // At most one cleanup a minute.
    let ttl = pttl(&pool, keys::PROCESS_CLEANUP).await;
    assert!(ttl > 50_000 && ttl <= 60_000, "{ttl}");
    let _: i64 = cmd(&pool, &redis_cmd!("SADD", keys::PROCESSES, "dead:3:ghi")).await;
    let second = redis
        .builder()
        .await
        .build()
        .await
        .unwrap()
        .start()
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        smembers(&pool, keys::PROCESSES)
            .await
            .contains(&"dead:3:ghi".to_owned())
    );
    second.stop().await.unwrap();
    launcher.stop().await.unwrap();
}

#[tokio::test]
async fn the_initial_wait_delays_the_first_poll() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    zadd(
        &pool,
        keys::SCHEDULE,
        hench::now_secs_f64() - 1.0,
        &payload("Scheduled", vec![], "queue_1"),
    )
    .await;
    let hench = Hench::builder()
        .redis(pool.clone())
        .queues(["elsewhere"])
        .initial_scheduler_wait(Duration::from_secs(30))
        .initial_scheduler_jitter(Duration::ZERO)
        .fetch_timeout(Duration::from_millis(100))
        .average_scheduled_poll_interval(Duration::from_millis(50))
        .build()
        .await
        .unwrap();
    let launcher = hench.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(zcard(&pool, keys::SCHEDULE).await, 1);
    // Quieting interrupts the wait: the stop does not wait for the background tasks' stop
    // timeout (5 s) to abort the scheduler.
    let started = std::time::Instant::now();
    launcher.stop().await.unwrap();
    let took = started.elapsed();
    assert!(
        took < Duration::from_secs(2),
        "stop took {took:?}: the initial wait was not interrupted"
    );
    // The due job was left for another process.
    assert_eq!(zcard(&pool, keys::SCHEDULE).await, 1);
}

#[tokio::test]
async fn scheduling_puts_jobs_in_the_schedule_set() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = client.redis().clone();
    Echo::perform_in(&client, Duration::from_secs(600), ("mike".into(),))
        .await
        .unwrap();
    Echo::perform_in(&client, Duration::from_secs(30 * 86_400), ("mike".into(),))
        .await
        .unwrap();
    assert_eq!(zcard(&pool, keys::SCHEDULE).await, 2);
    // A delay in the past pushes immediately.
    Echo::perform_at(
        &client,
        std::time::SystemTime::now() - Duration::from_secs(300),
        ("mike".into(),),
    )
    .await
    .unwrap();
    assert_eq!(zcard(&pool, keys::SCHEDULE).await, 2);
    assert_eq!(queue_size(&pool, "default").await, 1);
    let (job, _) = sorted_jobs(&pool, keys::SCHEDULE).await.remove(0);
    assert!(job.created_at.is_some());
    assert_eq!(job.enqueued_at, None);
}

/// Fails every push of jobs whose first argument is 13.
struct Unlucky;

#[async_trait]
impl ClientMiddleware for Unlucky {
    async fn call(
        &self,
        job: JobPayload,
        _redis: &RedisPool,
        next: ClientNext<'_>,
    ) -> hench::Result<Option<JobPayload>> {
        if job.args.first().and_then(|arg| arg.as_i64()) == Some(13) {
            return Err(Error::Config("unlucky".into()));
        }
        next.run(job).await
    }
}

#[tokio::test]
async fn a_job_that_keeps_failing_does_not_block_the_others() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let errors = Errors::default();
    let past = hench::now_secs_f64() - 60.0;
    // The failing job is the earliest one.
    zadd(
        &pool,
        keys::RETRY,
        past - 10.0,
        &payload("Scheduled", vec![json!(13)], "q"),
    )
    .await;
    zadd(
        &pool,
        keys::RETRY,
        past,
        &payload("Scheduled", vec![json!(1)], "q"),
    )
    .await;
    zadd(
        &pool,
        keys::RETRY,
        past,
        &payload("Scheduled", vec![json!(2)], "q"),
    )
    .await;
    let (_hench, launcher) = running(
        idle_builder(&redis)
            .await
            .client_middleware(Unlucky)
            .error_handler(errors.handler()),
    )
    .await;
    wait_for("the other jobs to be enqueued", WAIT, || async {
        queue_size(&pool, "q").await == 2
    })
    .await;
    launcher.stop().await.unwrap();
    // The failing job waits a minute before its next attempt.
    let retries = sorted_jobs(&pool, keys::RETRY).await;
    assert_eq!(retries.len(), 1);
    assert_eq!(retries[0].0.args, [json!(13)]);
    let delay = retries[0].1 - hench::now_secs_f64();
    assert!(delay > 50.0 && delay <= 60.0, "{delay}");
    assert_eq!(
        errors
            .with_context("could not enqueue scheduled jobs")
            .len(),
        1
    );
}

#[tokio::test]
async fn no_due_job_is_lost_while_redis_is_out_of_memory() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let past = hench::now_secs_f64() - 60.0;
    for i in 0..5 {
        zadd(
            &pool,
            keys::SCHEDULE,
            past,
            &payload("Scheduled", vec![json!(i)], "q"),
        )
        .await;
    }
    let (_hench, launcher) = running(idle_builder(&redis).await).await;
    // Writes that need memory now fail with OOM; removing a member from a set still works.
    let () = cmd(&pool, &redis_cmd!("CONFIG", "SET", "maxmemory", 1)).await;
    let () = cmd(
        &pool,
        &redis_cmd!("CONFIG", "SET", "maxmemory-policy", "noeviction"),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    // At most one job is held by the scheduler while it cannot be written back.
    let held = 5 - zcard(&pool, keys::SCHEDULE).await;
    assert!(held <= 1, "{held} jobs out of the schedule set");
    assert_eq!(queue_size(&pool, "q").await, 0);
    let () = cmd(&pool, &redis_cmd!("CONFIG", "SET", "maxmemory", 0)).await;
    wait_for("every job to be enqueued", WAIT, || async {
        queue_size(&pool, "q").await == 5
    })
    .await;
    assert_eq!(zcard(&pool, keys::SCHEDULE).await, 0);
    launcher.stop().await.unwrap();
}

/// Fails every push.
struct Broken;

#[async_trait]
impl ClientMiddleware for Broken {
    async fn call(
        &self,
        _job: JobPayload,
        _redis: &RedisPool,
        _next: ClientNext<'_>,
    ) -> hench::Result<Option<JobPayload>> {
        Err(Error::Config("the middleware's backend is down".into()))
    }
}

#[tokio::test]
async fn a_failing_middleware_defers_jobs_without_losing_or_churning_them() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let errors = Errors::default();
    let past = hench::now_secs_f64() - 60.0;
    for i in 0..30 {
        zadd(
            &pool,
            keys::RETRY,
            past,
            &payload("Scheduled", vec![json!(i)], "q"),
        )
        .await;
    }
    let (_hench, launcher) = running(
        idle_builder(&redis)
            .await
            .client_middleware(Broken)
            .error_handler(errors.handler()),
    )
    .await;
    // Each poll defers at most ten jobs (a minute later); within a second all are deferred.
    wait_for("every job to be deferred", WAIT, || async {
        sorted_jobs(&pool, keys::RETRY)
            .await
            .iter()
            .all(|(_, score)| *score > hench::now_secs_f64() + 30.0)
    })
    .await;
    launcher.stop().await.unwrap();
    assert_eq!(zcard(&pool, keys::RETRY).await, 30);
    assert_eq!(queue_size(&pool, "q").await, 0);
    // One report, not one per job.
    assert_eq!(
        errors
            .with_context("could not enqueue scheduled jobs")
            .len(),
        1
    );
}

#[tokio::test]
async fn a_job_specific_redis_error_does_not_block_the_set() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    // The earliest due job's queue key holds a string: pushing it fails with WRONGTYPE.
    let () = cmd(&pool, &redis_cmd!("SET", keys::queue("broken"), "oops")).await;
    let past = hench::now_secs_f64() - 60.0;
    zadd(
        &pool,
        keys::RETRY,
        past - 10.0,
        &payload("Scheduled", vec![json!(0)], "broken"),
    )
    .await;
    zadd(
        &pool,
        keys::RETRY,
        past,
        &payload("Scheduled", vec![json!(1)], "fine"),
    )
    .await;
    let (_hench, launcher) = running(idle_builder(&redis).await).await;
    wait_for("the other job to be enqueued", WAIT, || async {
        queue_size(&pool, "fine").await == 1
    })
    .await;
    launcher.stop().await.unwrap();
    let retries = sorted_jobs(&pool, keys::RETRY).await;
    assert_eq!(retries.len(), 1);
    assert_eq!(retries[0].0.queue, "broken");
    assert!(retries[0].1 > hench::now_secs_f64() + 30.0);
}

/// Rejects every job whose first argument is negative.
struct RejectNegative;

#[async_trait]
impl ClientMiddleware for RejectNegative {
    async fn call(
        &self,
        job: JobPayload,
        _redis: &RedisPool,
        next: ClientNext<'_>,
    ) -> hench::Result<Option<JobPayload>> {
        if job.args.first().and_then(|arg| arg.as_i64()).unwrap_or(0) < 0 {
            return Err(Error::Config("rejected".into()));
        }
        next.run(job).await
    }
}

#[tokio::test]
async fn failing_retries_do_not_starve_scheduled_jobs() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let past = hench::now_secs_f64() - 60.0;
    for i in 1..=50 {
        zadd(
            &pool,
            keys::RETRY,
            past,
            &payload("Scheduled", vec![json!(-i)], "q"),
        )
        .await;
    }
    zadd(
        &pool,
        keys::SCHEDULE,
        past,
        &payload("Scheduled", vec![json!(1)], "q"),
    )
    .await;
    let (_hench, launcher) = running(
        idle_builder(&redis)
            .await
            .client_middleware(RejectNegative)
            // One poll only during the test.
            .poll_interval_average(Duration::from_secs(3600)),
    )
    .await;
    wait_for("the scheduled job", WAIT, || async {
        queue_size(&pool, "q").await == 1
    })
    .await;
    launcher.stop().await.unwrap();
    // Only the first ten retries were tried (and deferred) in that poll.
    let deferred = sorted_jobs(&pool, keys::RETRY)
        .await
        .iter()
        .filter(|(_, score)| *score > hench::now_secs_f64())
        .count();
    assert_eq!(deferred, 10);
}

/// Panics for jobs of class `Boom`.
struct Panicky;

#[async_trait]
impl ClientMiddleware for Panicky {
    async fn call(
        &self,
        job: JobPayload,
        _redis: &RedisPool,
        next: ClientNext<'_>,
    ) -> hench::Result<Option<JobPayload>> {
        if job.class == "Boom" {
            panic!("a bug in a client middleware");
        }
        next.run(job).await
    }
}

/// A client middleware panicking while the scheduler pushes a due job is a rejected push: the
/// job is deferred a minute and reported, and the scheduler goes on with the other jobs (it
/// used to lose the job and stop scheduling for good).
#[tokio::test]
async fn a_panicking_client_middleware_defers_the_job_and_the_scheduler_goes_on() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let errors = Errors::default();
    let past = hench::now_secs_f64() - 60.0;
    let boom = payload("Boom", vec![json!(0)], "q");
    zadd(&pool, keys::RETRY, past - 10.0, &boom).await;
    zadd(
        &pool,
        keys::RETRY,
        past,
        &payload("Scheduled", vec![json!(1)], "q"),
    )
    .await;
    let (_hench, launcher) = running(
        idle_builder(&redis)
            .await
            .client_middleware(Panicky)
            .error_handler(errors.handler()),
    )
    .await;
    wait_for("the job behind it", WAIT, || async {
        queue_size(&pool, "q").await == 1
    })
    .await;
    // Jobs that become due later are enqueued too: the scheduler is alive.
    let now = hench::now_secs_f64();
    zadd(
        &pool,
        keys::SCHEDULE,
        now - 1.0,
        &payload("Scheduled", vec![json!(2)], "q"),
    )
    .await;
    zadd(
        &pool,
        keys::RETRY,
        now - 1.0,
        &payload("Scheduled", vec![json!(3)], "q"),
    )
    .await;
    wait_for("the later jobs", WAIT, || async {
        queue_size(&pool, "q").await == 3
    })
    .await;
    launcher.stop().await.unwrap();
    let retries = sorted_jobs(&pool, keys::RETRY).await;
    assert_eq!(retries.len(), 1);
    assert_eq!(retries[0].0, boom);
    let delay = retries[0].1 - hench::now_secs_f64();
    assert!(delay > 50.0 && delay <= 60.0, "{delay}");
    assert_eq!(zcard(&pool, keys::SCHEDULE).await, 0);
    assert_eq!(zcard(&pool, keys::DEAD).await, 0);
    let reported = errors.with_context("could not enqueue scheduled jobs");
    assert_eq!(reported.len(), 1, "{reported:?}");
    assert_eq!(reported[0].1, JobError::PANIC);
}

/// Every job whose first argument contains `marker`, wherever it is: `<set>:<arg>` for the
/// sorted sets, `queue:<arg>` for queue `q`.
async fn census(pool: &RedisPool, marker: &str) -> Vec<String> {
    let mut found = Vec::new();
    for set in [keys::RETRY, keys::SCHEDULE, keys::DEAD] {
        for (job, _) in sorted_jobs(pool, set).await {
            if job.args[0].as_str().is_some_and(|arg| arg.contains(marker)) {
                found.push(format!("{set}:{}", job.args[0].as_str().unwrap()));
            }
        }
    }
    for job in queue_jobs(pool, "q").await {
        if job.args[0].as_str().is_some_and(|arg| arg.contains(marker)) {
            found.push(format!("queue:{}", job.args[0].as_str().unwrap()));
        }
    }
    found.sort();
    found
}

/// Makes every member of `set` due now (`XX`: unless it moved meanwhile).
async fn make_due(pool: &RedisPool, set: &str) {
    for (member, _) in zrange(pool, set).await {
        let _: i64 = cmd(
            pool,
            &redis_cmd!("ZADD", set, "XX", hench::now_secs_f64() - 1.0, member),
        )
        .await;
    }
}

/// The reply to the script that takes a due job is lost (the connection drops after Redis ran
/// it, and `with_retry` sends it again, taking the next job): the first job stays in its set,
/// leased for a minute, instead of vanishing; once due again it is enqueued, once.
#[tokio::test]
async fn a_lost_reply_never_loses_a_due_job() {
    init_logging();
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let now = hench::now_secs_f64();
    let job = |arg: &str| payload("Scheduled", vec![json!(arg)], "q");
    zadd(&pool, keys::SCHEDULE, now - 60.0, &job("JOB-MARKER-first")).await;
    zadd(&pool, keys::SCHEDULE, now - 50.0, &job("JOB-second")).await;
    // The only reply carrying a job's JSON is the one that takes it.
    let proxy = ReplyDropper::start(redis.socket_path(), b"JOB-MARKER").await;
    let (_hench, launcher) = running(
        idle_builder(&redis)
            .await
            .redis_config(hench::RedisConfig::new(proxy.url())),
    )
    .await;
    wait_for("the second job", WAIT, || async {
        queue_size(&pool, "q").await == 1
    })
    .await;
    assert_eq!(proxy.dropped(), 1);
    assert_eq!(
        census(&pool, "JOB-").await,
        ["queue:JOB-second", "schedule:JOB-MARKER-first"]
    );
    // Leased: due again a minute after it was taken.
    let scheduled = zrange(&pool, keys::SCHEDULE).await;
    assert!(
        scheduled[0].1 > hench::now_secs_f64() + 50.0,
        "{scheduled:?}"
    );
    make_due(&pool, keys::SCHEDULE).await;
    wait_for("the first job", WAIT, || async {
        queue_size(&pool, "q").await == 2
    })
    .await;
    launcher.stop().await.unwrap();
    assert_eq!(
        census(&pool, "JOB-").await,
        ["queue:JOB-MARKER-first", "queue:JOB-second"]
    );
}

/// Keeps Redis busy for `ARGV[1]` seconds after adding two due jobs to each set.
const STALL: &str = r"
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[4])
redis.call('ZADD', KEYS[1], ARGV[3], ARGV[5])
redis.call('ZADD', KEYS[2], ARGV[2], ARGV[6])
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[7])
local t0 = redis.call('TIME')
local start = tonumber(t0[1]) + tonumber(t0[2]) / 1000000
while true do
  local t = redis.call('TIME')
  if tonumber(t[1]) + tonumber(t[2]) / 1000000 - start > tonumber(ARGV[1]) then break end
end
return 1
";

/// Redis stalls longer than the process's response timeout while the scheduler takes a due
/// job: the command times out on the client but runs once Redis resumes. No job is lost and
/// each is enqueued once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_lost_to_a_response_timeout_never_loses_a_due_job() {
    init_logging();
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let process_pool = RedisPool::from_config(
        hench::RedisConfig::new(redis.url()).response_timeout(Duration::from_millis(500)),
    )
    .await
    .unwrap();
    let (_hench, launcher) = running(idle_builder(&redis).await.redis(process_pool)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let now = hench::now_secs_f64();
    let job = |arg: &str| {
        payload("Scheduled", vec![json!(arg)], "q")
            .to_json()
            .unwrap()
    };
    let mut stall = hench::redis_rs::cmd("EVAL");
    stall
        .arg(STALL)
        .arg(2)
        .arg(keys::RETRY)
        .arg(keys::SCHEDULE)
        .arg(3)
        .arg(now - 60.0)
        .arg(now - 50.0)
        .arg(job("JOB-retry-first"))
        .arg(job("JOB-retry-second"))
        .arg(job("JOB-schedule-first"))
        .arg(job("JOB-schedule-second"));
    let stall_pool =
        RedisPool::from_config(redis.config().response_timeout(Duration::from_secs(10)))
            .await
            .unwrap();
    let mut conn = stall_pool.conn().await.unwrap();
    let _: i64 = stall.query_async(&mut conn).await.unwrap();
    // The scheduler polls again (it pauses 5 s when counting the processes timed out too).
    wait_for(
        "every job enqueued or leased",
        Duration::from_secs(15),
        || async {
            census(&pool, "JOB-").await.len() == 4
                && zrange(&pool, keys::RETRY)
                    .await
                    .into_iter()
                    .chain(zrange(&pool, keys::SCHEDULE).await)
                    .all(|(_, score)| score > hench::now_secs_f64())
        },
    )
    .await;
    make_due(&pool, keys::RETRY).await;
    make_due(&pool, keys::SCHEDULE).await;
    wait_for("every job enqueued", WAIT, || async {
        queue_size(&pool, "q").await == 4
    })
    .await;
    launcher.stop().await.unwrap();
    assert_eq!(
        census(&pool, "JOB-").await,
        [
            "queue:JOB-retry-first",
            "queue:JOB-retry-second",
            "queue:JOB-schedule-first",
            "queue:JOB-schedule-second"
        ]
    );
}

/// Takes `delay` for jobs of class `SlowPush`.
struct SlowPush(Duration, Arc<AtomicUsize>);

#[async_trait]
impl ClientMiddleware for SlowPush {
    async fn call(
        &self,
        job: JobPayload,
        _redis: &RedisPool,
        next: ClientNext<'_>,
    ) -> hench::Result<Option<JobPayload>> {
        if job.class == "SlowPush" {
            self.1.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(self.0).await;
        }
        next.run(job).await
    }
}

/// A shutdown while the scheduler pushes a due job through a slow client middleware neither
/// loses the job (it stays in its set, leased) nor waits for the middleware.
#[tokio::test]
async fn a_shutdown_during_a_slow_push_keeps_the_job() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let entered = Arc::new(AtomicUsize::new(0));
    let job = payload("SlowPush", vec![], "q");
    zadd(&pool, keys::SCHEDULE, 1.0, &job).await;
    let (_hench, launcher) = running(
        idle_builder(&redis)
            .await
            .client_middleware(SlowPush(Duration::from_secs(30), entered.clone())),
    )
    .await;
    wait_for("the middleware", WAIT, || async {
        entered.load(Ordering::SeqCst) == 1
    })
    .await;
    let started = std::time::Instant::now();
    launcher.stop().await.unwrap();
    // The push is dropped once the processors have stopped, not aborted 5 s later.
    assert!(
        started.elapsed() < Duration::from_secs(4),
        "{:?}",
        started.elapsed()
    );
    let scheduled = sorted_jobs(&pool, keys::SCHEDULE).await;
    assert_eq!(scheduled.len(), 1);
    assert_eq!(scheduled[0].0, job);
    assert!(scheduled[0].1 > hench::now_secs_f64() + 50.0);
    assert_eq!(queue_size(&pool, "q").await, 0);
}

/// Processes that crashed are pruned while workers run (about once a minute, under the
/// cleanup lock), not only when a process starts: a process that died just before the last
/// start, while its heartbeat key was still alive, used to stay in the set for good.
#[tokio::test]
async fn processes_that_die_while_workers_run_are_pruned() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let (_hench, launcher) = running(idle_builder(&redis).await).await;
    // The start-up cleanup ran and holds the minute-long lock.
    wait_for("the first cleanup", WAIT, || async {
        exists(&pool, keys::PROCESS_CLEANUP).await
    })
    .await;
    // Processes die (their heartbeat keys are gone)...
    for dead in ["dead:1:a", "dead:2:b", "dead:3:c"] {
        let _: i64 = cmd(&pool, &redis_cmd!("SADD", keys::PROCESSES, dead)).await;
    }
    // ...and a minute later the lock has expired.
    del(&pool, keys::PROCESS_CLEANUP).await;
    wait_for("the dead processes to be pruned", WAIT, || async {
        smembers(&pool, keys::PROCESSES).await == [launcher.identity().to_owned()]
    })
    .await;
    assert!(exists(&pool, keys::PROCESS_CLEANUP).await, "locked again");
    launcher.stop().await.unwrap();
}

/// The scheduler's poll interval is scaled by the number of live processes, not by the size of
/// the `processes` set, which counts processes that died until they are pruned: 40 dead
/// members used to make a lone process poll 41 times less often.
#[tokio::test]
async fn the_poll_interval_counts_live_processes_only() {
    init_logging();
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    // Nothing prunes the dead members during the test.
    let _: () = cmd(
        &pool,
        &redis_cmd!("SET", keys::PROCESS_CLEANUP, "1", "EX", 600),
    )
    .await;
    for index in 0..40 {
        let dead = format!("dead:{index}:x");
        let _: i64 = cmd(&pool, &redis_cmd!("SADD", keys::PROCESSES, &dead)).await;
    }
    // No `poll_interval_average`: the interval is the process count × 200 ms. (The workers
    // take jobs from `default`: the due jobs stay in `elsewhere`.)
    let hench = Hench::builder()
        .redis(pool.clone())
        .heartbeat_interval(Duration::from_millis(100))
        .average_scheduled_poll_interval(Duration::from_millis(200))
        .initial_scheduler_wait(Duration::ZERO)
        .initial_scheduler_jitter(Duration::ZERO)
        .fetch_timeout(Duration::from_millis(100))
        .timeout(Duration::from_secs(5))
        .systemd(false)
        .build()
        .await
        .unwrap();
    let launcher = hench.start().await.unwrap();
    let mut total = Duration::ZERO;
    for index in 0..4 {
        let job = payload("Elsewhere", vec![json!(index)], "elsewhere");
        let started = std::time::Instant::now();
        zadd(&pool, keys::SCHEDULE, hench::now_secs_f64() - 0.001, &job).await;
        wait_for(
            "the due job to be enqueued",
            Duration::from_secs(30),
            || async { queue_size(&pool, "elsewhere").await == index + 1 },
        )
        .await;
        total += started.elapsed();
    }
    // One live process: polls every 100-300 ms (41 processes: every 0-16 s).
    assert!(
        total < Duration::from_secs(4),
        "4 due jobs took {total:?} to be enqueued"
    );
    assert_eq!(smembers(&pool, keys::PROCESSES).await.len(), 41);
    launcher.stop().await.unwrap();
}
