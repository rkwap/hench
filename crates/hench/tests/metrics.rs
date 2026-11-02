//! Execution metrics and deploy marks (Sidekiq's `metrics_test` and `deploy_test` scenarios),
//! plus metrics written by real workers.

mod support;

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, NaiveDate, TimeDelta, TimeZone, Utc};
use hench::metrics::{Deploy, Granularity, Histogram, Outcome, Period, Query, SortBy, Tracker};
use hench::prelude::*;
use hench::test_util::TestRedis;
use support::{Runs, WAIT, wait_for};

fn fixed_time() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2022, 7, 22, 22, 3, 0).unwrap()
}

fn minute(h: u32, m: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2022, 7, 22, h, m, 0).unwrap()
}

/// Writes known metrics: three flushes, at `fixed_time`, a minute before and 100 minutes
/// before. Returns the counter count of the last flush.
async fn create_known_metrics(redis: &RedisPool) -> usize {
    let ms = Duration::from_millis;
    let tracker = Tracker::new();
    tracker.track("App::SomeJob", ms(1), Outcome::Success);
    tracker.track("App::FooJob", ms(1), Outcome::Success);
    tracker.track("App::SomeJob", ms(1), Outcome::Failure);
    tracker.flush_at(redis, fixed_time()).await.unwrap();
    tracker.track("App::FooJob", ms(1), Outcome::Success);
    tracker.track("App::FooJob", ms(20), Outcome::Success);
    tracker.track("App::FooJob", ms(1), Outcome::Success);
    tracker.track("App::SomeJob", ms(1), Outcome::Success);
    tracker
        .flush_at(redis, fixed_time() - TimeDelta::seconds(60))
        .await
        .unwrap();
    tracker.track("App::FooJob", ms(20), Outcome::Success);
    tracker.track("App::FooJob", ms(1), Outcome::Success);
    tracker.track("App::SomeJob", ms(1), Outcome::Success);
    tracker
        .flush_at(redis, fixed_time() - TimeDelta::seconds(6_000))
        .await
        .unwrap()
}

#[tokio::test]
async fn tracks_metrics() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    // Two classes with an execution count and a time each, in two hashes.
    assert_eq!(create_known_metrics(&pool).await, 8);
    // Nothing left to flush.
    assert_eq!(Tracker::new().flush(&pool).await.unwrap(), 0);
}

#[tokio::test]
async fn metric_keys_expire() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    create_known_metrics(&pool).await;
    let ttl = |key: String| {
        let pool = pool.clone();
        async move { support::pttl(&pool, &key).await }
    };
    let eight_hours = 8 * 3_600 * 1_000;
    let minute_ttl = ttl(hench::keys::metrics_minute(fixed_time())).await;
    assert!(minute_ttl > 0 && minute_ttl <= eight_hours, "{minute_ttl}");
    let ten_ttl = ttl(hench::keys::metrics_ten_minutes(fixed_time())).await;
    assert!(
        ten_ttl > eight_hours && ten_ttl <= 3 * 86_400 * 1_000,
        "{ten_ttl}"
    );
    let histogram_ttl = ttl(hench::keys::histogram("App::FooJob", fixed_time())).await;
    assert!(
        histogram_ttl > 0 && histogram_ttl <= eight_hours,
        "{histogram_ttl}"
    );
    // Failed executions are not timed: no histogram for a class that only failed.
    let tracker = Tracker::new();
    tracker.track("OnlyFails", Duration::from_millis(5), Outcome::Failure);
    tracker.flush_at(&pool, fixed_time()).await.unwrap();
    assert!(!support::exists(&pool, &hench::keys::histogram("OnlyFails", fixed_time())).await);
}

#[tokio::test]
async fn interrupted_jobs_are_timed_but_not_failed() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let tracker = Tracker::new();
    tracker.track(
        "App::SomeJob",
        Duration::from_millis(3),
        Outcome::Interrupted,
    );
    tracker.flush_at(&pool, fixed_time()).await.unwrap();
    let result = Query::new(&pool)
        .at(fixed_time())
        .for_job("App::SomeJob", Period::Minutes(60))
        .await
        .unwrap();
    let totals = result.jobs["App::SomeJob"].totals;
    assert_eq!((totals.processed, totals.failed, totals.ms), (1, 0, 3));
}

#[tokio::test]
async fn histograms_bucket_execution_times() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let tracker = Tracker::new();
    for ms in [10, 46, 47, 48, 300, 301, 302, 300_000_000] {
        tracker.track("App::FooJob", Duration::from_millis(ms), Outcome::Success);
    }
    tracker.flush_at(&pool, fixed_time()).await.unwrap();
    assert!(support::exists(&pool, "h|App::FooJob-22-22:3").await);

    let result = Query::new(&pool)
        .at(fixed_time())
        .for_job("App::FooJob", Period::Minutes(60))
        .await
        .unwrap();
    let job = &result.jobs["App::FooJob"];
    let histogram = job.histograms[&minute(22, 3)];
    assert_eq!(histogram.total(), 8);
    for (bucket, count) in [(0, 1), (3, 3), (7, 3), (25, 1)] {
        assert_eq!(histogram.count(bucket), count, "bucket {bucket}");
    }
    assert_eq!(job.histogram(), histogram);
}

#[tokio::test]
async fn histogram_counters_saturate() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let tracker = Tracker::new();
    for _ in 0..3 {
        for _ in 0..30_000 {
            tracker.track("Busy", Duration::ZERO, Outcome::Success);
        }
        tracker.flush_at(&pool, fixed_time()).await.unwrap();
    }
    let result = Query::new(&pool)
        .at(fixed_time())
        .for_job("Busy", Period::Minutes(1))
        .await
        .unwrap();
    let job = &result.jobs["Busy"];
    assert_eq!(job.histograms[&minute(22, 3)].count(0), u64::from(u16::MAX));
    // The counters themselves do not saturate.
    assert_eq!(job.totals.processed, 90_000);
}

#[tokio::test]
async fn handles_empty_metrics() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let query = Query::new(&pool).at(fixed_time());
    let top = query.top_jobs(Period::default(), None).await.unwrap();
    assert!(top.jobs.is_empty());
    assert!(top.marks.is_empty());
    let one = query
        .for_job("DoesntExist", Period::default())
        .await
        .unwrap();
    assert_eq!(one.jobs.keys().collect::<Vec<_>>(), ["DoesntExist"]);
    assert!(one.jobs["DoesntExist"].series.is_empty());
    assert!(one.jobs["DoesntExist"].histograms.is_empty());
}

#[tokio::test]
async fn filters_top_job_data() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    create_known_metrics(&pool).await;
    let result = Query::new(&pool)
        .at(fixed_time())
        .top_jobs(Period::Minutes(60), Some("some"))
        .await
        .unwrap();
    assert_eq!(result.starts_at, fixed_time() - TimeDelta::hours(1));
    assert_eq!(result.ends_at, fixed_time());
    assert_eq!(result.jobs.keys().collect::<Vec<_>>(), ["App::SomeJob"]);
    // An empty filter is no filter.
    let all = Query::new(&pool)
        .at(fixed_time())
        .top_jobs(Period::Minutes(60), Some(""))
        .await
        .unwrap();
    assert_eq!(all.jobs.len(), 2);
}

#[tokio::test]
async fn fetches_top_job_data() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    create_known_metrics(&pool).await;
    let deploy = Deploy::new(&pool);
    assert!(
        deploy
            .mark(
                "cafed00d - some git summary line",
                fixed_time() - TimeDelta::seconds(300)
            )
            .await
            .unwrap()
    );

    let result = Query::new(&pool)
        .at(fixed_time())
        .top_jobs(Period::Minutes(60), None)
        .await
        .unwrap();
    assert_eq!(result.granularity, Granularity::Minute);
    assert_eq!(result.starts_at, fixed_time() - TimeDelta::hours(1));
    assert_eq!(result.ends_at, fixed_time());
    assert_eq!(result.marks.len(), 1);
    assert_eq!(result.marks[0].label, "cafed00d - some git summary line");
    assert_eq!(result.marks[0].at, minute(21, 58));
    assert_eq!(result.marks[0].bucket, minute(21, 58));
    assert_eq!(
        result.jobs.keys().collect::<Vec<_>>(),
        ["App::FooJob", "App::SomeJob"]
    );

    let some = &result.jobs["App::SomeJob"];
    assert_eq!(some.series[&minute(22, 3)].processed, 2);
    assert_eq!(some.series[&minute(22, 3)].failed, 1);
    assert_eq!(some.series[&minute(22, 2)].processed, 1);
    assert_eq!(some.totals.processed, 3);
    assert_eq!(some.totals.failed, 1);
    assert_eq!(some.totals.ms, 2);
    assert_eq!(some.totals.average_ms(), 1.0);
    let foo = &result.jobs["App::FooJob"];
    assert_eq!(foo.totals.processed, 4);
    assert_eq!(foo.totals.ms, 23);
    // Only the queried minutes are read: the run 100 minutes before is not included.
    assert_eq!(foo.series.len(), 2);
    assert_eq!(result.buckets().len(), 60);
    let by_time: Vec<&str> = result
        .sorted(SortBy::TotalTime)
        .into_iter()
        .map(|(class, _)| class)
        .collect();
    assert_eq!(by_time, ["App::FooJob", "App::SomeJob"]);
    let by_failures: Vec<&str> = result
        .sorted(SortBy::Failed)
        .into_iter()
        .map(|(class, _)| class)
        .collect();
    assert_eq!(by_failures, ["App::SomeJob", "App::FooJob"]);

    let result = Query::new(&pool)
        .at(fixed_time())
        .top_jobs(Period::Hours(24), None)
        .await
        .unwrap();
    assert_eq!(result.granularity, Granularity::TenMinutes);
    let processed: Vec<(DateTime<Utc>, u64)> = result.jobs["App::SomeJob"]
        .series
        .iter()
        .map(|(bucket, counts)| (*bucket, counts.processed))
        .collect();
    assert_eq!(processed, [(minute(20, 20), 1), (minute(22, 0), 3)]);
    assert_eq!(result.marks.len(), 1);
    assert_eq!(result.marks[0].bucket, minute(21, 50));
    assert_eq!(result.buckets().len(), 144);
}

#[tokio::test]
async fn fetches_job_specific_data() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    create_known_metrics(&pool).await;
    Deploy::new(&pool)
        .mark(
            "cafed00d - some git summary line",
            fixed_time() - TimeDelta::seconds(300),
        )
        .await
        .unwrap();

    let result = Query::new(&pool)
        .at(fixed_time())
        .for_job("App::FooJob", Period::Minutes(60))
        .await
        .unwrap();
    assert_eq!(result.starts_at, fixed_time() - TimeDelta::hours(1));
    assert_eq!(result.ends_at, fixed_time());
    assert_eq!(result.marks.len(), 1);
    assert_eq!(result.marks[0].bucket, minute(21, 58));
    assert_eq!(result.jobs.keys().collect::<Vec<_>>(), ["App::FooJob"]);
    let job = &result.jobs["App::FooJob"];
    assert_eq!(job.series[&minute(22, 3)].processed, 1);
    assert_eq!(job.totals.processed, 4);
    assert_eq!(job.totals.failed, 0);
    // In the minute before: two executions under 20 ms, one of 20 ms.
    let histogram = job.histograms[&minute(22, 2)];
    assert_eq!(histogram.count(0), 2);
    assert_eq!(histogram.count(1), 1);
    assert_eq!(job.histogram().total(), 4);

    let result = Query::new(&pool)
        .at(fixed_time())
        .for_job("App::FooJob", Period::Hours(24))
        .await
        .unwrap();
    assert_eq!(result.granularity, Granularity::TenMinutes);
    let processed: Vec<(DateTime<Utc>, u64)> = result.jobs["App::FooJob"]
        .series
        .iter()
        .map(|(bucket, counts)| (*bucket, counts.processed))
        .collect();
    assert_eq!(processed, [(minute(20, 20), 2), (minute(22, 0), 4)]);
    // Histograms are only kept per minute.
    assert!(result.jobs["App::FooJob"].histograms.is_empty());
}

#[tokio::test]
async fn class_names_may_contain_the_separator() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let tracker = Tracker::new();
    tracker.track("odd|name", Duration::from_millis(2), Outcome::Success);
    tracker.flush_at(&pool, fixed_time()).await.unwrap();
    let result = Query::new(&pool)
        .at(fixed_time())
        .top_jobs(Period::Minutes(5), None)
        .await
        .unwrap();
    assert_eq!(result.jobs["odd|name"].totals.ms, 2);
}

// --- deploy marks (Sidekiq's deploy_test) ---

#[tokio::test]
async fn records_a_deploy_mark_keyed_by_the_minute() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let deploy = Deploy::new(&pool);
    let at = Utc.with_ymd_and_hms(2026, 5, 29, 10, 15, 30).unwrap();
    assert!(deploy.mark("abc123 fix the thing", at).await.unwrap());
    let marks = deploy.fetch(at.date_naive()).await.unwrap();
    let floor = Utc.with_ymd_and_hms(2026, 5, 29, 10, 15, 0).unwrap();
    assert_eq!(
        marks.into_iter().collect::<Vec<_>>(),
        [(floor, "abc123 fix the thing".to_owned())]
    );
    let raw = support::hgetall(&pool, "20260529-marks").await;
    assert_eq!(raw["2026-05-29T10:15:00Z"], "abc123 fix the thing");
    let ttl = support::pttl(&pool, "20260529-marks").await;
    assert!(
        ttl > 89 * 86_400 * 1_000 && ttl <= 90 * 86_400 * 1_000,
        "{ttl}"
    );
}

#[tokio::test]
async fn marks_within_a_minute_roll_up() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let deploy = Deploy::new(&pool);
    let at = Utc.with_ymd_and_hms(2026, 5, 29, 10, 15, 30).unwrap();
    assert!(deploy.mark("first", at).await.unwrap());
    // Same label within the minute: locked out.
    assert!(
        !deploy
            .mark("first", at + TimeDelta::seconds(20))
            .await
            .unwrap()
    );
    // Another label in the same minute: the minute already has a mark.
    assert!(
        !deploy
            .mark("second", at + TimeDelta::seconds(25))
            .await
            .unwrap()
    );
    let marks = deploy.fetch(at.date_naive()).await.unwrap();
    assert_eq!(marks.len(), 1);
    assert_eq!(marks.values().next().map(String::as_str), Some("first"));
}

#[tokio::test]
async fn marks_in_different_minutes_are_kept() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let deploy = Deploy::new(&pool);
    let at = Utc.with_ymd_and_hms(2026, 5, 29, 10, 15, 30).unwrap();
    deploy.mark("first", at).await.unwrap();
    deploy
        .mark("second", at + TimeDelta::seconds(60))
        .await
        .unwrap();
    let marks: Vec<(String, String)> = deploy
        .fetch(at.date_naive())
        .await
        .unwrap()
        .into_iter()
        .map(|(at, label)| (at.to_rfc3339(), label))
        .collect();
    assert_eq!(
        marks,
        [
            ("2026-05-29T10:15:00+00:00".to_owned(), "first".to_owned()),
            ("2026-05-29T10:16:00+00:00".to_owned(), "second".to_owned()),
        ]
    );
}

#[tokio::test]
async fn no_marks_for_a_date() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let date = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
    assert!(Deploy::new(&pool).fetch(date).await.unwrap().is_empty());
}

#[tokio::test]
async fn only_one_mark_per_minute_shows_in_queries() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let whence = Utc.with_ymd_and_hms(2022, 7, 17, 18, 43, 15).unwrap();
    let deploy = Deploy::new(&pool);
    deploy
        .mark("cafed00d - some git summary line", whence)
        .await
        .unwrap();
    deploy.mark("another label", whence).await.unwrap();
    let result = Query::new(&pool)
        .at(whence)
        .for_job("FooJob", Period::default())
        .await
        .unwrap();
    assert_eq!(result.marks.len(), 1);
    assert_eq!(result.marks[0].label, "cafed00d - some git summary line");
    let marks = deploy.fetch(whence.date_naive()).await.unwrap();
    let floor = Utc.with_ymd_and_hms(2022, 7, 17, 18, 43, 0).unwrap();
    assert_eq!(marks.len(), 1);
    assert_eq!(marks[&floor], "cafed00d - some git summary line");
}

#[tokio::test]
async fn queries_return_marks_from_every_day_of_the_period() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let whence = fixed_time();
    let deploy = Deploy::new(&pool);
    deploy
        .mark("yesterday deploy", whence - TimeDelta::hours(26))
        .await
        .unwrap();
    deploy.mark("today deploy", whence).await.unwrap();
    // A mark older than the period is left out.
    deploy
        .mark("old deploy", whence - TimeDelta::hours(80))
        .await
        .unwrap();
    let labels: Vec<String> = Query::new(&pool)
        .at(whence)
        .top_jobs(Period::Hours(72), None)
        .await
        .unwrap()
        .marks
        .into_iter()
        .map(|mark| mark.label)
        .collect();
    assert_eq!(labels, ["yesterday deploy", "today deploy"]);
}

#[test]
fn git_labels() {
    // In a git checkout (as when running the tests from the repository) the label is the
    // short hash and the summary of the last commit; elsewhere there is none.
    if let Some(label) = Deploy::git_label() {
        assert!(label.split_once(' ').is_some(), "{label}");
    }
}

// --- metrics written by real workers ---

/// Succeeds after a short pause, recording its argument.
#[derive(Default)]
struct Quick;

impl Job for Quick {
    const NAME: &'static str = "Quick";
    type Args = (String,);

    async fn perform(&self, ctx: &JobContext, (value,): (String,)) -> Result<(), JobError> {
        tokio::time::sleep(Duration::from_millis(25)).await;
        ctx.data::<Runs>()?.record(value);
        Ok(())
    }
}

/// Fails without retrying.
#[derive(Default)]
struct Broken;

impl Job for Broken {
    const NAME: &'static str = "Broken";
    type Args = ();

    fn options() -> JobOptions {
        JobOptions::new().retry(false)
    }

    async fn perform(&self, _ctx: &JobContext, (): ()) -> Result<(), JobError> {
        Err(JobError::msg("broken"))
    }
}

#[tokio::test]
async fn workers_record_metrics_on_heartbeats() {
    support::init_logging();
    let redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    let hench = redis
        .builder()
        .await
        .register(Quick)
        .register(Broken)
        .data_arc(runs.clone())
        .clear_error_handlers()
        .build()
        .await
        .unwrap();
    let client = hench.client().clone();
    for i in 0..4 {
        Quick::perform_async(&client, (format!("job {i}"),))
            .await
            .unwrap();
    }
    Broken::perform_async(&client, ()).await.unwrap();
    let launcher = hench.start().await.unwrap();
    runs.wait_for(4).await;

    let pool = redis.pool().await;
    let query = Query::new(&pool);
    wait_for("metrics of both classes", WAIT, || async {
        let result = query.top_jobs(Period::Minutes(5), None).await.unwrap();
        result
            .jobs
            .get("Quick")
            .is_some_and(|quick| quick.totals.processed == 4)
            && result
                .jobs
                .get("Broken")
                .is_some_and(|broken| broken.totals.failed == 1)
    })
    .await;
    let result = query.top_jobs(Period::Minutes(5), None).await.unwrap();
    let quick = result.jobs["Quick"].totals;
    assert_eq!(quick.failed, 0);
    assert!(quick.ms >= 4 * 25, "{quick:?}");
    let broken = result.jobs["Broken"].totals;
    assert_eq!((broken.processed, broken.failed, broken.ms), (1, 1, 0));

    // Each execution is in the histogram of its class, at or above 25 ms.
    let detail = query.for_job("Quick", Period::Minutes(5)).await.unwrap();
    let histogram = detail.jobs["Quick"].histogram();
    assert_eq!(histogram.total(), 4);
    assert_eq!(histogram.count(Histogram::bucket_index(0)), 0);
    launcher.stop().await.unwrap();
}

#[tokio::test]
async fn workers_flush_metrics_when_they_stop() {
    let redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    let hench = redis
        .builder()
        .await
        // No heartbeat during the test: only the shutdown can write the metrics.
        .heartbeat_interval(hench::Config::MAX_HEARTBEAT_INTERVAL)
        .register(Quick)
        .data_arc(runs.clone())
        .build()
        .await
        .unwrap();
    let launcher = hench.start().await.unwrap();
    Quick::perform_async(hench.client(), ("once".to_owned(),))
        .await
        .unwrap();
    runs.wait_for(1).await;
    let pool = redis.pool().await;
    let query = Query::new(&pool);
    // The startup beat flushed nothing yet.
    let before = query.top_jobs(Period::Minutes(5), None).await.unwrap();
    assert!(before.jobs.is_empty(), "{before:?}");
    launcher.stop().await.unwrap();
    let after = query.top_jobs(Period::Minutes(5), None).await.unwrap();
    assert_eq!(after.jobs["Quick"].totals.processed, 1);
}

#[tokio::test]
async fn metrics_can_be_turned_off() {
    let redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    let hench = redis
        .builder()
        .await
        .metrics(false)
        .register(Quick)
        .data_arc(runs.clone())
        .build()
        .await
        .unwrap();
    let launcher = hench.start().await.unwrap();
    Quick::perform_async(hench.client(), ("once".to_owned(),))
        .await
        .unwrap();
    runs.wait_for(1).await;
    launcher.stop().await.unwrap();
    let pool = redis.pool().await;
    let keys: Vec<String> = support::cmd(&pool, &redis_cmd!("KEYS", "j|*")).await;
    assert!(keys.is_empty(), "{keys:?}");
    let keys: Vec<String> = support::cmd(&pool, &redis_cmd!("KEYS", "h|*")).await;
    assert!(keys.is_empty(), "{keys:?}");
}

// --- Flushes across a Redis stall ---

/// `redis-server` stopped with SIGSTOP until dropped: it keeps its connections and receives
/// requests, but runs them only once it continues (as during a long script, a fork of a large
/// dataset or a paused VM), so the client's response timeouts fire for requests Redis runs
/// later.
#[cfg(unix)]
struct Stalled(u32);

#[cfg(unix)]
impl Stalled {
    async fn start(redis: &RedisPool) -> Self {
        let info: String = support::cmd(redis, &redis_cmd!("INFO", "server")).await;
        let pid: u32 = info
            .lines()
            .find_map(|line| line.strip_prefix("process_id:"))
            .and_then(|pid| pid.trim().parse().ok())
            .expect("the process id of redis-server");
        let status = std::process::Command::new("kill")
            .args(["-STOP", &pid.to_string()])
            .status()
            .expect("kill -STOP");
        assert!(status.success(), "kill -STOP {pid}: {status}");
        Self(pid)
    }
}

#[cfg(unix)]
impl Drop for Stalled {
    fn drop(&mut self) {
        // Not asserted: this may run while a failing test unwinds.
        let _ = std::process::Command::new("kill")
            .args(["-CONT", &self.0.to_string()])
            .status();
    }
}

/// A flush that times out may still be run by Redis later (the request was sent), so its data
/// must not be counted again by the next flushes, which send it again until Redis confirms it.
#[cfg(unix)]
#[tokio::test]
async fn flushes_that_time_out_are_counted_once() {
    let redis = TestRedis::start().await;
    let reader = redis.pool().await;
    // A pool of its own with a short response timeout: flushes give up quickly.
    let pool = RedisPool::from_config(redis.config().response_timeout(Duration::from_millis(250)))
        .await
        .unwrap();
    let ms = Duration::from_millis;
    let tracker = Tracker::new();
    // A first flush, so that what Redis runs after the stall is not refused as an unknown
    // script.
    tracker.track("Counted", ms(5), Outcome::Success);
    assert_eq!(tracker.flush_at(&pool, fixed_time()).await.unwrap(), 4);
    for _ in 0..10 {
        tracker.track("Counted", ms(5), Outcome::Success);
    }
    {
        let _stalled = Stalled::start(&reader).await;
        for _ in 0..3 {
            assert!(tracker.flush_at(&pool, fixed_time()).await.is_err());
            assert!(!tracker.is_empty());
        }
        // Tracked during the stall: written by the first flush that works.
        tracker.track("Counted", ms(5), Outcome::Failure);
    }
    // Redis runs the flushes it received; the next flush finds the batch they wrote already
    // applied, then writes the failure.
    tracker.flush_at(&pool, fixed_time()).await.unwrap();
    assert!(tracker.is_empty());
    let result = Query::new(&reader)
        .at(fixed_time())
        .for_job("Counted", Period::Minutes(60))
        .await
        .unwrap();
    let counted = &result.jobs["Counted"];
    assert_eq!(
        (counted.totals.processed, counted.totals.failed),
        (12, 1),
        "{:?}",
        counted.totals
    );
    assert_eq!(counted.totals.ms, 55);
    assert_eq!(counted.histogram().total(), 11);
    // The ten-minute rollup too.
    let rollup = Query::new(&reader)
        .at(fixed_time())
        .top_jobs(Period::Hours(24), None)
        .await
        .unwrap();
    assert_eq!(rollup.jobs["Counted"].totals.processed, 12);
}

/// Real workers whose heartbeats time out during a Redis stall count every execution once, in
/// the metrics and in the `stat:*` counters.
#[cfg(unix)]
#[tokio::test]
async fn workers_count_each_execution_once_across_a_redis_stall() {
    let redis = TestRedis::start().await;
    let reader = redis.pool().await;
    let pool = RedisPool::from_config(redis.config().response_timeout(Duration::from_millis(250)))
        .await
        .unwrap();
    let runs = Arc::new(Runs::default());
    let hench = redis
        .builder()
        .await
        .redis(pool)
        .heartbeat_interval(Duration::from_millis(400))
        .register(Quick)
        .data_arc(runs.clone())
        .clear_error_handlers()
        .build()
        .await
        .unwrap();
    let launcher = hench.start().await.unwrap();
    for i in 0..10 {
        Quick::perform_async(hench.client(), (format!("job {i}"),))
            .await
            .unwrap();
    }
    runs.wait_for(10).await;
    let in_progress = hench::keys::inprogress(launcher.identity());
    wait_for("the jobs to be acknowledged", WAIT, || async {
        support::llen(&reader, &in_progress).await == 0
    })
    .await;
    {
        // Several heartbeats time out meanwhile.
        let _stalled = Stalled::start(&reader).await;
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    launcher.stop().await.unwrap();

    let processed: Option<u64> =
        support::cmd(&reader, &redis_cmd!("GET", hench::keys::STAT_PROCESSED)).await;
    assert_eq!(processed, Some(10));
    let result = Query::new(&reader)
        .top_jobs(Period::Minutes(5), None)
        .await
        .unwrap();
    assert_eq!(result.jobs["Quick"].totals.processed, 10);
    assert_eq!(runs.len(), 10);
}
