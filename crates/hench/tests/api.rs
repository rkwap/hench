//! The data API (`hench::api`): scenarios of Sidekiq's api_test, api_utils_test,
//! dead_set_test, filtering_test and paginator_test, plus failure and concurrency cases.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::NaiveDate;
use futures::{StreamExt, TryStreamExt};
use hench::api::{
    Api, DeadSet, JobSet, Page, ProcessSet, Queue, RawEntry, RetrySet, ScheduledSet, SortOrder,
    SortedEntry, StatCounter, Stats, StatsHistory, WorkSet, page_key, parse_key,
};
use hench::prelude::*;
use hench::test_util::TestRedis;
use hench::{Config, Error, now_millis, now_secs_f64, redis_rs};
use serde_json::{Value, json};

/// A job taking any arguments (Sidekiq's `ApiJob`).
#[derive(Default)]
struct ApiJob;

impl Job for ApiJob {
    const NAME: &'static str = "ApiJob";
    type Args = Vec<Value>;

    async fn perform(&self, _ctx: &JobContext, _args: Vec<Value>) -> Result<(), JobError> {
        Ok(())
    }
}

/// A job with a tag.
#[derive(Default)]
struct JobWithTags;

impl Job for JobWithTags {
    const NAME: &'static str = "JobWithTags";
    type Args = ();

    fn options() -> JobOptions {
        JobOptions::new().tags(["foo"])
    }

    async fn perform(&self, _ctx: &JobContext, (): ()) -> Result<(), JobError> {
        Ok(())
    }
}

async fn setup() -> (TestRedis, Api) {
    let redis = TestRedis::start().await;
    let api = Api::new(redis.pool().await);
    (redis, api)
}

async fn run<T: redis_rs::FromRedisValue>(api: &Api, command: &mut redis_rs::Cmd) -> T {
    common::cmd(api.redis(), command.clone()).await
}

fn args(values: &[Value]) -> Vec<Value> {
    values.to_vec()
}

/// A retry entry like Sidekiq's test helper: `ApiJob` with `[1, "mike"]`, retry count 2.
async fn add_retry(api: &Api, jid: &str, at: f64) -> String {
    let payload = json!({
        "class": "ApiJob", "args": [1, "mike"], "queue": "default", "jid": jid,
        "retry_count": 2, "failed_at": now_millis(), "error_class": "RuntimeError",
        "error_message": "boom"
    })
    .to_string();
    let () = run(
        api,
        redis_rs::cmd("ZADD").arg("retry").arg(at).arg(&payload),
    )
    .await;
    payload
}

/// A dead entry: `ApiJob` with `[1, "mike"]`.
async fn add_dead(api: &Api, jid: &str, at: f64) -> String {
    let payload =
        json!({"class": "ApiJob", "args": [1, "mike"], "queue": "default", "jid": jid}).to_string();
    let () = run(api, redis_rs::cmd("ZADD").arg("dead").arg(at).arg(&payload)).await;
    payload
}

async fn collect<S, T>(stream: S) -> Vec<T>
where
    S: futures::Stream<Item = hench::Result<T>>,
{
    stream.try_collect().await.unwrap()
}

fn jids(entries: &[SortedEntry]) -> Vec<String> {
    entries
        .iter()
        .map(|entry| entry.jid().unwrap_or_default().to_owned())
        .collect()
}

// ------------------------------------------------------------------------------- stats --

#[tokio::test]
async fn stats_are_initially_zero() {
    let (_redis, api) = setup().await;
    let stats = api.stats().await.unwrap();
    assert_eq!(stats.processed, 0);
    assert_eq!(stats.failed, 0);
    assert_eq!(stats.enqueued, 0);
    assert_eq!(stats.default_queue_latency, 0.0);
    assert_eq!(stats.workers_size, 0);
    assert_eq!(stats.scheduled_size + stats.retry_size + stats.dead_size, 0);
    assert_eq!(stats.processes_size, 0);
}

#[tokio::test]
async fn stats_read_the_counters() {
    let (_redis, api) = setup().await;
    let () = run(&api, redis_rs::cmd("SET").arg("stat:processed").arg(5)).await;
    let () = run(&api, redis_rs::cmd("SET").arg("stat:failed").arg(7)).await;
    let stats = Stats::fetch(&api).await.unwrap();
    assert_eq!((stats.processed, stats.failed), (5, 7));

    // Garbage does not break the dashboard.
    let () = run(&api, redis_rs::cmd("SET").arg("stat:failed").arg("garbage")).await;
    assert_eq!(Stats::fetch(&api).await.unwrap().failed, 0);
}

#[tokio::test]
async fn stats_reset_every_counter_by_default_or_the_given_ones() {
    let (_redis, api) = setup().await;
    let set = || async {
        let () = run(
            &api,
            redis_rs::cmd("MSET")
                .arg("stat:processed")
                .arg(5)
                .arg("stat:failed")
                .arg(10),
        )
        .await;
    };
    set().await;
    Stats::reset(&api, &[]).await.unwrap();
    let stats = api.stats().await.unwrap();
    assert_eq!((stats.processed, stats.failed), (0, 0));

    set().await;
    Stats::reset(&api, &[StatCounter::Failed]).await.unwrap();
    let stats = api.stats().await.unwrap();
    assert_eq!((stats.processed, stats.failed), (5, 0));

    set().await;
    Stats::reset(&api, &[StatCounter::Processed, StatCounter::Failed])
        .await
        .unwrap();
    let stats = api.stats().await.unwrap();
    assert_eq!((stats.processed, stats.failed), (0, 0));
}

#[tokio::test]
async fn stats_count_busy_workers_and_enqueued_jobs() {
    let (_redis, api) = setup().await;
    let () = run(
        &api,
        redis_rs::cmd("SADD")
            .arg("processes")
            .arg("process_1")
            .arg("process_2"),
    )
    .await;
    let () = run(
        &api,
        redis_rs::cmd("HSET").arg("process_1").arg("busy").arg(1),
    )
    .await;
    let () = run(
        &api,
        redis_rs::cmd("HSET").arg("process_2").arg("busy").arg(2),
    )
    .await;
    let () = run(&api, redis_rs::cmd("RPUSH").arg("queue:foo").arg("{}")).await;
    let () = run(
        &api,
        redis_rs::cmd("RPUSH")
            .arg("queue:bar")
            .arg("{}")
            .arg("{}")
            .arg("{}"),
    )
    .await;
    let () = run(
        &api,
        redis_rs::cmd("SADD").arg("queues").arg("foo").arg("bar"),
    )
    .await;
    let stats = api.stats().await.unwrap();
    assert_eq!(stats.workers_size, 3);
    assert_eq!(stats.enqueued, 4);
    assert_eq!(stats.processes_size, 2);
    assert_eq!(api.work().size().await.unwrap(), 3);
}

#[tokio::test]
async fn stats_list_queues_by_size() {
    let (_redis, api) = setup().await;
    assert!(Stats::queues(&api).await.unwrap().is_empty());
    assert!(Stats::queue_summaries(&api).await.unwrap().is_empty());
    let () = run(&api, redis_rs::cmd("RPUSH").arg("queue:foo").arg("{}")).await;
    let () = run(
        &api,
        redis_rs::cmd("RPUSH")
            .arg("queue:bar")
            .arg("{}")
            .arg("{}")
            .arg("{}"),
    )
    .await;
    let () = run(
        &api,
        redis_rs::cmd("SADD")
            .arg("queues")
            .arg("foo")
            .arg("bar")
            .arg("empty"),
    )
    .await;
    assert_eq!(
        Stats::queues(&api).await.unwrap(),
        [
            ("bar".to_owned(), 3),
            ("foo".to_owned(), 1),
            ("empty".to_owned(), 0)
        ]
    );
}

#[tokio::test]
async fn stats_summarize_queues() {
    let (_redis, api) = setup().await;
    let job = |age_ms: i64| json!({"enqueued_at": now_millis() - age_ms}).to_string();
    let () = run(&api, redis_rs::cmd("RPUSH").arg("queue:foo").arg(job(50))).await;
    for _ in 0..3 {
        let () = run(&api, redis_rs::cmd("RPUSH").arg("queue:bar").arg(job(100))).await;
    }
    let () = run(
        &api,
        redis_rs::cmd("SADD").arg("queues").arg("foo").arg("bar"),
    )
    .await;
    let () = run(&api, redis_rs::cmd("SADD").arg("paused").arg("bar")).await;

    let summaries = Stats::queue_summaries(&api).await.unwrap();
    assert_eq!(summaries.len(), 2);
    assert_eq!((summaries[0].name.as_str(), summaries[0].size), ("bar", 3));
    assert!(summaries[0].latency > 0.09, "{}", summaries[0].latency);
    assert!(summaries[0].paused);
    assert_eq!((summaries[1].name.as_str(), summaries[1].size), ("foo", 1));
    assert!(summaries[1].latency > 0.04);
    assert!(!summaries[1].paused);
    assert!(api.queue("bar").is_paused().await.unwrap());
    assert!(!api.queue("foo").is_paused().await.unwrap());

    // A paused queue stays listed once emptied, or before its first job: it is still paused.
    api.queue("bar").clear().await.unwrap();
    api.queue("reports").pause().await.unwrap();
    let summaries = Stats::queue_summaries(&api).await.unwrap();
    let listed: Vec<(&str, u64, bool)> = summaries
        .iter()
        .map(|summary| (summary.name.as_str(), summary.size, summary.paused))
        .collect();
    assert_eq!(
        listed,
        [("foo", 1, false), ("bar", 0, true), ("reports", 0, true)]
    );
    // `Stats::queues` lists the `queues` set alone (Sidekiq's `Stats#queues`).
    assert_eq!(Stats::queues(&api).await.unwrap(), [("foo".to_owned(), 1)]);
}

#[tokio::test]
async fn queues_can_be_paused_and_unpaused() {
    let (_redis, api) = setup().await;
    let queue = api.queue("mailers");
    assert!(!queue.is_paused().await.unwrap());
    assert!(queue.pause().await.unwrap());
    assert!(
        !queue.pause().await.unwrap(),
        "pausing twice changes nothing"
    );
    assert!(queue.is_paused().await.unwrap());
    let members: Vec<String> = run(&api, redis_rs::cmd("SMEMBERS").arg("paused")).await;
    assert_eq!(members, ["mailers"], "the set the workers' fetch reads");

    // Pausing is by name: clearing the queue keeps it paused, as is a queue that does not
    // exist yet.
    queue.clear().await.unwrap();
    assert!(queue.is_paused().await.unwrap());
    assert!(api.queue("later").pause().await.unwrap());

    assert!(queue.unpause().await.unwrap());
    assert!(!queue.unpause().await.unwrap(), "not paused any more");
    assert!(!queue.is_paused().await.unwrap());
    assert!(api.queue("later").is_paused().await.unwrap());
}

#[tokio::test]
async fn stats_measure_the_default_queue_latency() {
    let (_redis, api) = setup().await;
    let job = json!({"enqueued_at": now_millis() - 50}).to_string();
    let () = run(&api, redis_rs::cmd("RPUSH").arg("queue:default").arg(job)).await;
    assert!(api.stats().await.unwrap().default_queue_latency > 0.04);
    assert!(api.queue("default").latency().await.unwrap() > 0.04);

    // Incomplete and malformed jobs have no latency.
    let () = run(&api, redis_rs::cmd("DEL").arg("queue:default")).await;
    let () = run(&api, redis_rs::cmd("RPUSH").arg("queue:default").arg("{}")).await;
    assert_eq!(api.stats().await.unwrap().default_queue_latency, 0.0);
    assert_eq!(api.queue("default").latency().await.unwrap(), 0.0);
    let () = run(
        &api,
        redis_rs::cmd("LPUSH").arg("queue:default").arg("not json"),
    )
    .await;
    let () = run(&api, redis_rs::cmd("RPOP").arg("queue:default")).await;
    assert_eq!(api.stats().await.unwrap().default_queue_latency, 0.0);
}

#[tokio::test]
async fn stats_history_reads_daily_counters() {
    let (_redis, api) = setup().await;
    for (day, processed, failed) in [(24, 4, 3), (25, 1, 2), (26, 6, 0), (27, 2, 9)] {
        let date = format!("2012-12-{day}");
        let () = run(
            &api,
            redis_rs::cmd("INCRBY")
                .arg(format!("stat:processed:{date}"))
                .arg(processed),
        )
        .await;
        let () = run(
            &api,
            redis_rs::cmd("INCRBY")
                .arg(format!("stat:failed:{date}"))
                .arg(failed),
        )
        .await;
    }
    let day = |d| NaiveDate::from_ymd_opt(2012, 12, d).unwrap();

    let history = StatsHistory::ending(&api, 2, day(26)).unwrap();
    assert_eq!(
        history.processed().await.unwrap(),
        [(day(26), 6), (day(25), 1)]
    );
    assert_eq!(
        history.failed().await.unwrap(),
        [(day(26), 0), (day(25), 2)]
    );

    let history = StatsHistory::ending(&api, 3, day(26)).unwrap();
    assert_eq!(
        history.processed().await.unwrap(),
        [(day(26), 6), (day(25), 1), (day(24), 4)]
    );

    let history = StatsHistory::ending(&api, 2, day(25)).unwrap();
    assert_eq!(
        history.processed().await.unwrap(),
        [(day(25), 1), (day(24), 4)]
    );

    // Days without data count zero; the default ends today.
    let history = StatsHistory::ending(&api, 2, day(23)).unwrap();
    assert_eq!(
        history.processed().await.unwrap(),
        [(day(23), 0), (day(22), 0)]
    );
    let today = StatsHistory::new(&api, 30)
        .unwrap()
        .processed()
        .await
        .unwrap();
    assert_eq!(today.len(), 30);
    assert!(today.iter().all(|(_, count)| *count == 0));

    assert!(matches!(StatsHistory::new(&api, 0), Err(Error::Config(_))));
    assert!(matches!(
        StatsHistory::new(&api, 2_000),
        Err(Error::Config(_))
    ));
}

// ------------------------------------------------------------------------------ queues --

#[tokio::test]
async fn empty_queues() {
    let (_redis, api) = setup().await;
    let queue = api.queue("default");
    assert_eq!(queue.size().await.unwrap(), 0);
    assert_eq!(queue.latency().await.unwrap(), 0.0);
    assert!(collect(queue.jobs()).await.is_empty());
    assert!(queue.find_job("nope").await.unwrap().is_none());
    assert!(api.queues().await.unwrap().is_empty());
    let page = queue.page(1, 25, SortOrder::Descending).await.unwrap();
    assert_eq!((page.total, page.items.len()), (0, 0));
}

#[tokio::test]
async fn queues_enumerate_their_jobs() {
    let (_redis, api) = setup().await;
    let year_ago = now_millis() - 365 * 24 * 60 * 60 * 1000;
    let job = json!({
        "class": "ApiJob", "args": [1, "mike"], "queue": "default",
        "jid": "0123456789abcdef01234567", "created_at": year_ago, "enqueued_at": year_ago
    });
    let () = run(
        &api,
        redis_rs::cmd("LPUSH")
            .arg("queue:default")
            .arg(job.to_string()),
    )
    .await;
    let () = run(&api, redis_rs::cmd("SADD").arg("queues").arg("default")).await;

    let queue = api.queue("default");
    let jobs = collect(queue.jobs()).await;
    assert_eq!(jobs.len(), 1);
    let record = &jobs[0];
    assert_eq!(record.class(), Some("ApiJob"));
    assert_eq!(record.jid().map(str::len), Some(24));
    assert_eq!(record.args(), args(&[json!(1), json!("mike")]));
    assert_eq!(record.queue(), "default");
    let enqueued = record.enqueued_at().unwrap();
    assert_eq!(
        enqueued.duration_since(UNIX_EPOCH).unwrap().as_millis(),
        year_ago as u128
    );
    assert!(queue.latency().await.unwrap() > 10_000_000.0);
    assert!(record.latency() > 10_000_000.0);

    assert_eq!(api.queue("other").size().await.unwrap(), 0);
}

#[tokio::test]
async fn queues_list_known_queues_by_name() {
    let (_redis, api) = setup().await;
    let client = api.client().clone();
    for queue in ["zeta", "alpha", "mid"] {
        ApiJob::set()
            .queue(queue)
            .perform_async(&client, vec![])
            .await
            .unwrap();
    }
    let names: Vec<String> = Queue::all(&api)
        .await
        .unwrap()
        .iter()
        .map(|queue| queue.name().to_owned())
        .collect();
    assert_eq!(names, ["alpha", "mid", "zeta"]);
    assert_eq!(api.queues().await.unwrap()[0], api.queue("alpha"));
}

#[tokio::test]
async fn queue_records_expose_job_fields() {
    let (_redis, api) = setup().await;
    let client = api.client().clone();
    let jid = ApiJob::perform_async(&client, vec![json!(1), json!("mike")])
        .await
        .unwrap()
        .unwrap();
    let record = api.queue("default").find_job(&jid).await.unwrap().unwrap();
    assert_eq!(record.display_class(), Some("ApiJob"));
    assert_eq!(record.display_args().as_ref(), [json!(1), json!("mike")]);
    assert!(record.tags().is_empty());
    assert!(record.created_at().is_some());
    assert!(record.latency() < 5.0);
    let payload = record.payload().unwrap();
    assert_eq!(payload.jid, jid);
    assert_eq!(payload.class, "ApiJob");

    let tagged = JobWithTags::perform_async(&client, ())
        .await
        .unwrap()
        .unwrap();
    let record = api
        .queue("default")
        .find_job(&tagged)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.tags(), ["foo"]);
}

#[tokio::test]
async fn queue_jobs_can_be_deleted() {
    let (_redis, api) = setup().await;
    ApiJob::perform_async(api.client(), vec![json!(1), json!("mike")])
        .await
        .unwrap();
    let queue = api.queue("default");
    assert_eq!(queue.size().await.unwrap(), 1);
    let record = collect(queue.jobs()).await.remove(0);
    assert!(record.delete().await.unwrap());
    assert_eq!(queue.size().await.unwrap(), 0);
    // Already gone.
    assert!(!record.delete().await.unwrap());

    // A record built from a raw value (what a dashboard form posts) deletes too.
    ApiJob::perform_async(api.client(), vec![]).await.unwrap();
    let raw = queue.page(1, 1, SortOrder::Descending).await.unwrap().items[0]
        .value()
        .to_owned();
    assert!(
        hench::api::JobRecord::new(&api, raw, Some("default"))
            .delete()
            .await
            .unwrap()
    );
    assert_eq!(queue.size().await.unwrap(), 0);
}

#[tokio::test]
async fn queues_iterate_newest_first_across_pages() {
    let (_redis, api) = setup().await;
    let jobs: Vec<Vec<Value>> = (0..120).map(|i| vec![json!(i)]).collect();
    hench::BulkPush::new("ApiJob", jobs);
    api.client()
        .push_bulk(hench::BulkPush::new(
            "ApiJob",
            (0..120).map(|i| vec![json!(i)]).collect(),
        ))
        .await
        .unwrap();
    let seen: Vec<i64> = collect(api.queue("default").jobs())
        .await
        .iter()
        .map(|record| record.args()[0].as_i64().unwrap())
        .collect();
    assert_eq!(seen, (0..120).rev().collect::<Vec<_>>());
}

#[tokio::test]
async fn queue_jobs_can_be_removed_while_iterating() {
    let (_redis, api) = setup().await;
    // More than one iteration page.
    for i in 0..51 {
        ApiJob::perform_async(api.client(), vec![json!(i), json!("aaron")])
            .await
            .unwrap();
    }
    let queue = api.queue("default");
    let mut jobs = std::pin::pin!(queue.jobs());
    let mut deleted = 0;
    while let Some(record) = jobs.try_next().await.unwrap() {
        assert!(record.delete().await.unwrap());
        deleted += 1;
    }
    assert_eq!(deleted, 51);
    assert_eq!(queue.size().await.unwrap(), 0);
}

#[tokio::test]
async fn queues_find_jobs_by_jid() {
    let (_redis, api) = setup().await;
    let jid = ApiJob::perform_async(api.client(), vec![json!(1), json!("jason")])
        .await
        .unwrap()
        .unwrap();
    // A jid that only appears in another job's arguments is not a match.
    ApiJob::perform_async(api.client(), vec![json!("decoy"), json!(format!("{jid}x"))])
        .await
        .unwrap();
    for i in 0..600 {
        ApiJob::perform_async(api.client(), vec![json!(i)])
            .await
            .unwrap();
    }
    let queue = api.queue("default");
    let found = queue.find_job(&jid).await.unwrap().unwrap();
    assert_eq!(found.jid(), Some(jid.as_str()));
    assert!(queue.find_job(&format!("{jid}x")).await.unwrap().is_none());
    assert!(queue.find_job("").await.unwrap().is_none());
}

#[tokio::test]
async fn queues_can_be_cleared() {
    let (_redis, api) = setup().await;
    for _ in 0..2 {
        ApiJob::set()
            .queue("foo")
            .perform_async(api.client(), vec![json!(1)])
            .await
            .unwrap();
    }
    ApiJob::perform_async(api.client(), vec![]).await.unwrap();
    api.queue("foo").clear().await.unwrap();
    assert_eq!(common::known_queues(api.redis()).await, ["default"]);
    let exists: u64 = run(&api, redis_rs::cmd("EXISTS").arg("queue:foo")).await;
    assert_eq!(exists, 0);
    assert_eq!(api.queue("default").size().await.unwrap(), 1);
}

#[tokio::test]
async fn queues_are_paged_in_both_directions() {
    let (_redis, api) = setup().await;
    api.client()
        .push_bulk(hench::BulkPush::new(
            "ApiJob",
            (0..30).map(|i| vec![json!(i)]).collect(),
        ))
        .await
        .unwrap();
    let queue = api.queue("default");
    let numbers = |page: Page<hench::api::JobRecord>| -> Vec<i64> {
        page.items
            .iter()
            .map(|record| record.args()[0].as_i64().unwrap())
            .collect()
    };
    let newest = queue.page(1, 10, SortOrder::Descending).await.unwrap();
    assert_eq!((newest.number, newest.total, newest.size), (1, 30, 10));
    assert_eq!(numbers(newest), (20..30).rev().collect::<Vec<_>>());
    let oldest = queue.page(1, 10, SortOrder::Ascending).await.unwrap();
    assert_eq!(numbers(oldest), (0..10).collect::<Vec<_>>());
    let second = queue.page(2, 10, SortOrder::Ascending).await.unwrap();
    assert!(second.has_previous() && second.has_next());
    assert_eq!(numbers(second), (10..20).collect::<Vec<_>>());
    let past_the_end = queue.page(4, 10, SortOrder::Descending).await.unwrap();
    assert!(past_the_end.items.is_empty());
    assert_eq!(past_the_end.total, 30);
    // Page numbers below 1 mean the first page; sizes are capped.
    let first = queue.page(0, 5_000, SortOrder::Ascending).await.unwrap();
    assert_eq!(
        (first.number, first.size, first.items.len()),
        (1, 1_000, 30)
    );
    let empty = queue.page(1, 0, SortOrder::Ascending).await.unwrap();
    assert_eq!((empty.total, empty.items.len()), (30, 0));
}

#[tokio::test]
async fn malformed_jobs_are_shown_and_can_be_deleted() {
    let (_redis, api) = setup().await;
    let () = run(
        &api,
        redis_rs::cmd("LPUSH").arg("queue:default").arg("not-json"),
    )
    .await;
    let page = api
        .queue("default")
        .page(1, 10, SortOrder::Ascending)
        .await
        .unwrap();
    let record = &page.items[0];
    assert!(!record.is_valid());
    assert_eq!(record.args(), [json!("not-json")]);
    assert_eq!(record.class(), None);
    assert!(record.delete().await.unwrap());
}

// ------------------------------------------------------------------------- sorted sets --

#[tokio::test]
async fn sorted_sets_enumerate_in_descending_score_order() {
    let (_redis, api) = setup().await;
    // More than one iteration page.
    for i in 0..51 {
        ApiJob::perform_in(
            api.client(),
            Duration::from_secs(100 + i),
            vec![json!(1), json!("foo")],
        )
        .await
        .unwrap();
    }
    let entries = collect(api.scheduled().entries()).await;
    assert_eq!(entries.len(), 51);
    let scores: Vec<f64> = entries.iter().map(SortedEntry::score).collect();
    let mut sorted = scores.clone();
    sorted.sort_by(|a, b| b.total_cmp(a));
    assert_eq!(scores, sorted);
}

#[tokio::test]
async fn scheduled_jobs_have_no_enqueued_at() {
    let (_redis, api) = setup().await;
    let jid = ApiJob::perform_in(
        api.client(),
        Duration::from_secs(100),
        vec![json!(1), json!("foo")],
    )
    .await
    .unwrap()
    .unwrap();
    let entry = api.scheduled().find_job(&jid).await.unwrap().unwrap();
    assert!(entry.enqueued_at().is_none());
    assert!(entry.created_at().is_some());
    let at = entry.at().duration_since(SystemTime::now()).unwrap();
    assert!(at > Duration::from_secs(90) && at <= Duration::from_secs(100));
}

#[tokio::test]
async fn scheduled_jobs_can_be_moved_to_their_queue() {
    let (_redis, api) = setup().await;
    let remain = ApiJob::perform_in(
        api.client(),
        Duration::from_secs(100),
        vec![json!(1), json!("jason")],
    )
    .await
    .unwrap()
    .unwrap();
    let jid = ApiJob::perform_in(
        api.client(),
        Duration::from_secs(100),
        vec![json!(1), json!("jason")],
    )
    .await
    .unwrap()
    .unwrap();
    let entry = api.scheduled().find_job(&jid).await.unwrap().unwrap();
    assert!(entry.add_to_queue().await.unwrap());
    let queued = api.queue("default").find_job(&jid).await.unwrap().unwrap();
    assert_eq!(queued.jid(), Some(jid.as_str()));
    assert!(queued.enqueued_at().is_some());
    assert!(api.scheduled().find_job(&jid).await.unwrap().is_none());
    assert!(api.scheduled().find_job(&remain).await.unwrap().is_some());
    // Moving it again does nothing.
    assert!(!entry.add_to_queue().await.unwrap());
    assert_eq!(api.queue("default").size().await.unwrap(), 1);
}

#[tokio::test]
async fn scheduled_jobs_with_the_same_score_are_moved_one_by_one() {
    let (_redis, api) = setup().await;
    let at = SystemTime::now() + Duration::from_secs(60);
    let jids = api
        .client()
        .push_bulk(hench::BulkPush::new("ApiJob", vec![vec![json!(1)], vec![json!(2)]]).at(at))
        .await
        .unwrap();
    let (remain, jid) = (jids[0].clone().unwrap(), jids[1].clone().unwrap());
    let entry = api.scheduled().find_job(&jid).await.unwrap().unwrap();
    assert!(entry.add_to_queue().await.unwrap());
    assert!(api.queue("default").find_job(&jid).await.unwrap().is_some());
    assert!(api.scheduled().find_job(&jid).await.unwrap().is_none());
    assert!(api.scheduled().find_job(&remain).await.unwrap().is_some());
}

#[tokio::test]
async fn many_pages_of_scheduled_jobs_can_be_enqueued() {
    let (_redis, api) = setup().await;
    let at = SystemTime::now() + Duration::from_secs(60);
    let jids = api
        .client()
        .push_bulk(hench::BulkPush::new("ApiJob", vec![vec![]; 200]).at(at))
        .await
        .unwrap();
    assert_eq!(jids.len(), 200);
    let scheduled = api.scheduled();
    assert_eq!(scheduled.size().await.unwrap(), 200);
    let mut entries = std::pin::pin!(scheduled.entries());
    while let Some(entry) = entries.try_next().await.unwrap() {
        assert!(entry.add_to_queue().await.unwrap());
    }
    assert_eq!(scheduled.size().await.unwrap(), 0);
    assert_eq!(api.queue("default").size().await.unwrap(), 200);
}

#[tokio::test]
async fn sorted_set_entries_can_be_removed_while_iterating() {
    let (_redis, api) = setup().await;
    for i in 0..51 {
        ApiJob::perform_in(
            api.client(),
            Duration::from_secs(100 + i),
            vec![json!("aaron")],
        )
        .await
        .unwrap();
    }
    let set = api.scheduled();
    let deleted: Vec<bool> = set
        .entries()
        .then(|entry| async move { entry.unwrap().delete().await.unwrap() })
        .collect()
        .await;
    assert_eq!(deleted.len(), 51);
    assert!(deleted.iter().all(|deleted| *deleted));
    assert_eq!(set.size().await.unwrap(), 0);
}

#[tokio::test]
async fn scheduled_jobs_can_be_found_by_jid() {
    let (_redis, api) = setup().await;
    for i in 0..10 {
        ApiJob::perform_in(api.client(), Duration::from_secs(10 + i), vec![json!(1)])
            .await
            .unwrap();
    }
    let jid = ApiJob::perform_in(api.client(), Duration::from_secs(5), vec![json!(1)])
        .await
        .unwrap()
        .unwrap();
    let entry = api.scheduled().find_job(&jid).await.unwrap().unwrap();
    assert_eq!(entry.jid(), Some(jid.as_str()));

    ApiJob::perform_in(
        api.client(),
        Duration::from_secs(100),
        vec![json!(1), json!({"jid": "jid_in_args"})],
    )
    .await
    .unwrap();
    assert!(
        api.scheduled()
            .find_job("jid_in_args")
            .await
            .unwrap()
            .is_none()
    );
    assert!(api.scheduled().find_job("").await.unwrap().is_none());
}

#[tokio::test]
async fn sorted_sets_fetch_by_score_and_jid() {
    let (_redis, api) = setup().await;
    let now = now_secs_f64();
    add_retry(&api, "bob1", now).await;
    add_retry(&api, "bob2", now).await;
    add_retry(&api, "bob3", now + 1.0).await;
    add_retry(&api, "bob4", now + 2.0).await;
    let retries = api.retries();
    assert_eq!(retries.fetch(now, None).await.unwrap().len(), 2);
    assert_eq!(
        jids(&retries.fetch(now, Some("bob1")).await.unwrap()),
        ["bob1"]
    );
    assert!(retries.fetch(now, Some("bob3")).await.unwrap().is_empty());
    let range = retries.fetch_range(now, now + 1.0, None).await.unwrap();
    assert_eq!(range.len(), 3);
    let range = retries
        .fetch_range(now, now + 1.0, Some("bob3"))
        .await
        .unwrap();
    assert_eq!(jids(&range), ["bob3"]);
    assert!(retries.fetch(now - 100.0, None).await.unwrap().is_empty());
}

#[tokio::test]
async fn retries_are_enumerated_with_their_fields() {
    let (_redis, api) = setup().await;
    let retries = api.retries();
    assert_eq!(retries.size().await.unwrap(), 0);
    let time = now_secs_f64();
    add_retry(&api, "bob", time).await;
    assert_eq!(retries.size().await.unwrap(), 1);
    let entries = collect(retries.entries()).await;
    assert_eq!(entries.len(), 1);
    let entry = &entries[0];
    assert_eq!(entry.class(), Some("ApiJob"));
    assert_eq!(entry.queue(), "default");
    assert_eq!(entry.jid(), Some("bob"));
    assert_eq!(entry.score(), time);
    assert!((entry.at().duration_since(UNIX_EPOCH).unwrap().as_secs_f64() - time).abs() < 1e-6);
    assert_eq!(entry.retry_count(), Some(2));
    assert!(entry.has_error());
    assert_eq!(entry.error_message(), Some("boom"));
    assert_eq!(entry.set().name(), "retry");
    assert_eq!(parse_key(&entry.key()), Some((time, Some("bob"))));
}

#[tokio::test]
async fn sorted_set_entries_are_deleted_by_score_and_jid() {
    let (_redis, api) = setup().await;
    let now = now_secs_f64();
    add_retry(&api, "bob1", now).await;
    add_retry(&api, "bob2", now).await;
    let retries = api.retries();
    assert_eq!(retries.size().await.unwrap(), 2);
    assert!(retries.delete_by_jid(now, "bob1").await.unwrap());
    assert_eq!(retries.size().await.unwrap(), 1);
    assert!(!retries.delete_by_jid(now, "bob1").await.unwrap());
    assert!(!retries.delete_by_jid(now + 5.0, "bob2").await.unwrap());
    let value = retries.fetch(now, Some("bob2")).await.unwrap()[0]
        .value()
        .to_owned();
    assert!(retries.delete_by_value(&value).await.unwrap());
    assert!(!retries.delete_by_value(&value).await.unwrap());
    assert_eq!(retries.size().await.unwrap(), 0);
}

#[tokio::test]
async fn retries_can_be_retried() {
    let (_redis, api) = setup().await;
    add_retry(&api, "bob", now_secs_f64()).await;
    let retries = api.retries();
    let entry = collect(retries.entries()).await.remove(0);
    assert!(entry.retry().await.unwrap());
    assert_eq!(retries.size().await.unwrap(), 0);
    let queue = api.queue("default");
    assert_eq!(queue.size().await.unwrap(), 1);
    let job = collect(queue.jobs()).await.remove(0);
    assert_eq!(job.jid(), Some("bob"));
    // A manual retry does not count against the limit.
    assert_eq!(job.retry_count(), Some(1));
    assert_eq!(job.error_class(), Some("RuntimeError"));
    assert!(!entry.retry().await.unwrap());
}

#[tokio::test]
async fn retries_can_be_cleared() {
    let (_redis, api) = setup().await;
    add_retry(&api, "bob", now_secs_f64()).await;
    add_retry(&api, "test", now_secs_f64()).await;
    let retries = api.retries();
    assert_eq!(retries.size().await.unwrap(), 2);
    retries.clear().await.unwrap();
    assert_eq!(retries.size().await.unwrap(), 0);
}

#[tokio::test]
async fn retries_can_be_scanned() {
    let (_redis, api) = setup().await;
    add_retry(&api, "bob", now_secs_f64()).await;
    add_retry(&api, "test", now_secs_f64()).await;
    let retries = api.retries();
    assert_eq!(collect(retries.scan("ApiJob")).await.len(), 2);
    assert_eq!(collect(retries.scan("*test*")).await.len(), 1);
    assert_eq!(collect(retries.scan("")).await.len(), 2);
    assert!(collect(retries.scan("nothing-matches")).await.is_empty());
    // Without `*` the pattern is a substring and glob metacharacters are taken literally;
    // with `*` it is a glob matched against the whole entry.
    assert!(collect(retries.scan("b?b")).await.is_empty());
    assert!(collect(retries.scan("b*b")).await.is_empty());
    assert_eq!(collect(retries.scan("*b?b*")).await.len(), 1);
}

#[tokio::test]
async fn scans_cover_large_sets_once() {
    let (_redis, api) = setup().await;
    let now = now_secs_f64();
    let mut pipe = redis_rs::pipe();
    for i in 0..1_000 {
        let job = json!({"class": "ApiJob", "args": [i], "jid": format!("jid{i}")}).to_string();
        pipe.cmd("ZADD")
            .arg("retry")
            .arg(now + f64::from(i))
            .arg(job)
            .ignore();
    }
    let mut conn = api.redis().conn().await.unwrap();
    let () = pipe.query_async(&mut conn).await.unwrap();
    let entries = collect(api.retries().scan("ApiJob")).await;
    assert_eq!(entries.len(), 1_000);
    let unique: std::collections::HashSet<String> = jids(&entries).into_iter().collect();
    assert_eq!(unique.len(), 1_000);
    let found = api.retries().find_job("jid999").await.unwrap().unwrap();
    assert_eq!(found.args(), [json!(999)]);
}

#[tokio::test]
async fn retries_can_be_rescheduled() {
    let (_redis, api) = setup().await;
    add_retry(&api, "foo1", now_secs_f64()).await;
    add_retry(&api, "foo2", now_secs_f64()).await;
    let retries = api.retries();
    let now = now_secs_f64();
    let later = |entries: &[SortedEntry], secs: f64| {
        entries
            .iter()
            .filter(|entry| entry.score() > now + secs)
            .count()
    };
    let entries = collect(retries.entries()).await;
    assert_eq!(later(&entries, 9.0), 0);
    for mut entry in entries {
        let delay = if entry.jid() == Some("foo1") { 15 } else { 10 };
        let at = SystemTime::now() + Duration::from_secs(delay);
        assert!(entry.reschedule(at).await.unwrap());
        // The entry follows the change.
        let score = entry.score();
        assert!(
            (entry.at().duration_since(UNIX_EPOCH).unwrap().as_secs_f64() - score).abs() < 1e-6
        );
    }
    let entries = collect(retries.entries()).await;
    assert_eq!(retries.size().await.unwrap(), 2);
    assert_eq!(later(&entries, 9.0), 2);
    assert_eq!(later(&entries, 14.0), 1);

    // A deleted entry cannot be rescheduled (and is not resurrected).
    let mut entry = entries[0].clone();
    assert!(entry.delete().await.unwrap());
    let before = entry.score();
    assert!(!entry.reschedule(SystemTime::now()).await.unwrap());
    assert_eq!(entry.score(), before);
    assert_eq!(retries.size().await.unwrap(), 1);
}

#[tokio::test]
async fn sorted_sets_are_paged_by_score() {
    let (_redis, api) = setup().await;
    let now = now_secs_f64();
    for i in 0..30 {
        add_retry(&api, &format!("jid{i:02}"), now + f64::from(i)).await;
    }
    let earliest = api
        .retries()
        .page(1, 10, SortOrder::Ascending)
        .await
        .unwrap();
    assert_eq!(earliest.total, 30);
    assert_eq!(jids(&earliest.items)[0], "jid00");
    assert_eq!(jids(&earliest.items)[9], "jid09");
    let latest = api
        .retries()
        .page(1, 10, SortOrder::Descending)
        .await
        .unwrap();
    assert_eq!(jids(&latest.items)[0], "jid29");
    let third = api
        .retries()
        .page(3, 10, SortOrder::Ascending)
        .await
        .unwrap();
    assert_eq!(jids(&third.items)[9], "jid29");
    assert!(!third.has_next());
    // Entries of a page are fully functional.
    assert!(third.items[0].delete().await.unwrap());
    assert_eq!(api.retries().size().await.unwrap(), 29);
}

#[tokio::test]
async fn jobs_can_be_scheduled_into_a_set() {
    let (_redis, api) = setup().await;
    let mut job = JobPayload::new("ApiJob", vec![json!(1)]);
    job.jid = "manual".into();
    job.at = Some(1.0);
    job.enqueued_at = Some(now_millis());
    let at = SystemTime::now() + Duration::from_secs(30);
    api.scheduled().schedule(at, &job).await.unwrap();
    let entry = api.scheduled().find_job("manual").await.unwrap().unwrap();
    assert!(entry.get("at").is_none());
    assert!(entry.enqueued_at().is_none());
    assert!((entry.score() - at.duration_since(UNIX_EPOCH).unwrap().as_secs_f64()).abs() < 1e-3);
    // Any set name works with a plain JobSet.
    let custom = JobSet::new(&api, "custom");
    custom.schedule(at, &job).await.unwrap();
    assert_eq!(custom.size().await.unwrap(), 1);
}

// --------------------------------------------------------------------- kill and retry_all --

/// An API whose configuration counts death handler calls and records the errors' classes.
fn api_with_death_handler(api: &Api, calls: Arc<std::sync::Mutex<Vec<(String, String)>>>) -> Api {
    let mut config = Config::default();
    config
        .death_handlers
        .push(Arc::new(move |job: &JobPayload, error: &JobError| {
            calls
                .lock()
                .unwrap()
                .push((job.jid.clone(), format!("{error:#}")));
        }));
    api.with_config(Arc::new(config))
}

#[tokio::test]
async fn scheduled_jobs_can_be_killed() {
    let (_redis, api) = setup().await;
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let api = api_with_death_handler(&api, calls.clone());
    let jid = ApiJob::perform_in(
        api.client(),
        Duration::from_secs(100),
        vec![json!(1), json!("{\"foo\":123}")],
    )
    .await
    .unwrap()
    .unwrap();
    let entry = api.scheduled().find_job(&jid).await.unwrap().unwrap();
    let dead = api.dead();
    assert_eq!(dead.size().await.unwrap(), 0);
    assert!(entry.kill().await.unwrap());
    assert_eq!(dead.size().await.unwrap(), 1);
    assert_eq!(api.scheduled().size().await.unwrap(), 0);
    let killed = dead.find_job(&jid).await.unwrap().unwrap();
    assert_eq!(killed.value(), entry.value());
    assert_eq!(
        *calls.lock().unwrap(),
        [(
            jid.clone(),
            "hench::Killed: the job was killed through the API".to_owned()
        )]
    );
    // Killing it again does nothing.
    assert!(!entry.kill().await.unwrap());
    assert_eq!(calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn dead_set_kill_notifies_unless_asked_not_to() {
    let (_redis, api) = setup().await;
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let api = api_with_death_handler(&api, calls.clone());
    let job = |jid: &str| json!({"class": "ApiJob", "args": [1], "jid": jid}).to_string();

    api.dead().kill(&job("notified"), true).await.unwrap();
    api.dead().kill(&job("silent"), false).await.unwrap();
    let custom = JobError::new("billing::Declined", "custom error");
    api.dead()
        .kill_with_error(&job("custom"), &custom)
        .await
        .unwrap();
    // Unparseable jobs are still killed; handlers are skipped.
    api.dead().kill("not json", true).await.unwrap();

    assert_eq!(api.dead().size().await.unwrap(), 4);
    assert_eq!(
        *calls.lock().unwrap(),
        [
            (
                "notified".to_owned(),
                "hench::Killed: the job was killed through the API".to_owned()
            ),
            (
                "custom".to_owned(),
                "billing::Declined: custom error".to_owned()
            ),
        ]
    );
    // Without a configuration nothing is notified (and nothing breaks).
    let plain = Api::new(api.redis().clone());
    plain.dead().kill(&job("plain"), true).await.unwrap();
    assert_eq!(calls.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn dead_set_kill_stores_the_job_as_given() {
    let (_redis, api) = setup().await;
    let serialized = json!({"jid": "123123", "class": "SomeJob", "args": []}).to_string();
    api.dead().kill(&serialized, false).await.unwrap();
    let entry = api.dead().find_job("123123").await.unwrap().unwrap();
    assert_eq!(entry.value(), serialized);
}

#[tokio::test]
async fn dead_set_removes_jobs_older_than_the_timeout() {
    let (_redis, api) = setup().await;
    let api = api.with_dead_limits(10_000, Duration::from_secs(100));
    let now = now_secs_f64();
    // Wide margins around the 100 s timeout, so a slow machine cannot change the outcome.
    add_dead(&api, "000103", now - 150.0).await;
    add_dead(&api, "000102", now - 50.0).await;
    api.dead()
        .kill(
            &json!({"jid": "000101", "class": "MyJob1", "args": []}).to_string(),
            false,
        )
        .await
        .unwrap();
    let dead = api.dead();
    assert!(dead.find_job("000103").await.unwrap().is_none());
    assert!(dead.find_job("000102").await.unwrap().is_some());
    assert!(dead.find_job("000101").await.unwrap().is_some());
}

#[tokio::test]
async fn dead_set_keeps_the_newest_max_jobs_minus_one() {
    let (_redis, api) = setup().await;
    let api = api.with_dead_limits(3, Config::DEFAULT_DEAD_TIMEOUT);
    let dead = api.dead();
    for (index, jid) in ["000101", "000102", "000103"].into_iter().enumerate() {
        let job = json!({"jid": jid, "class": format!("MyJob{index}"), "args": []}).to_string();
        dead.kill(&job, false).await.unwrap();
        // Distinct death times.
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert!(dead.find_job("000101").await.unwrap().is_none());
    assert!(dead.find_job("000102").await.unwrap().is_some());
    assert!(dead.find_job("000103").await.unwrap().is_some());

    // An explicit trim applies the same limits.
    let now = now_secs_f64();
    for i in 0..5 {
        add_dead(&api, &format!("extra{i}"), now + f64::from(i)).await;
    }
    dead.trim().await.unwrap();
    assert_eq!(dead.size().await.unwrap(), 2);
}

#[tokio::test]
async fn retry_all_and_kill_all_move_every_job() {
    let (_redis, api) = setup().await;
    let api = api.with_dead_limits(1_000, Config::DEFAULT_DEAD_TIMEOUT);
    let now = now_secs_f64();
    let mut pipe = redis_rs::pipe();
    for i in 0..1_000 {
        let job = json!({"class": "ApiJob", "args": [1, "mike"], "queue": "default", "jid": format!("dead{i}"), "retry_count": 25}).to_string();
        pipe.cmd("ZADD")
            .arg("dead")
            .arg(now - f64::from(i))
            .arg(job)
            .ignore();
    }
    for i in 0..500 {
        let job = json!({"class": "ApiJob", "args": [1, "mike"], "queue": "default", "jid": format!("retry{i}"), "retry_count": 2}).to_string();
        pipe.cmd("ZADD")
            .arg("retry")
            .arg(now + f64::from(i))
            .arg(job)
            .ignore();
    }
    let mut conn = api.redis().conn().await.unwrap();
    let () = pipe.query_async(&mut conn).await.unwrap();

    let (queue, dead, retries) = (api.queue("default"), api.dead(), api.retries());
    assert_eq!(dead.size().await.unwrap(), 1_000);
    assert_eq!(retries.size().await.unwrap(), 500);

    assert_eq!(dead.retry_all().await.unwrap(), 1_000);
    assert_eq!(dead.size().await.unwrap(), 0);
    assert_eq!(queue.size().await.unwrap(), 1_000);
    let job = queue
        .page(1, 1, SortOrder::Descending)
        .await
        .unwrap()
        .items
        .remove(0);
    assert_eq!(job.retry_count(), Some(24));

    assert_eq!(retries.kill_all(false).await.unwrap(), 500);
    assert_eq!(dead.size().await.unwrap(), 500);
    assert_eq!(retries.size().await.unwrap(), 0);
    // Nothing left: both are no-ops.
    assert_eq!(retries.retry_all().await.unwrap(), 0);
    assert_eq!(retries.kill_all(true).await.unwrap(), 0);
}

#[tokio::test]
async fn kill_all_notifies_and_trims() {
    let (_redis, api) = setup().await;
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let api = api_with_death_handler(&api, calls.clone())
        .with_dead_limits(3, Config::DEFAULT_DEAD_TIMEOUT);
    let now = now_secs_f64();
    for i in 0..5 {
        add_retry(&api, &format!("r{i}"), now + f64::from(i)).await;
    }
    assert_eq!(api.retries().kill_all(true).await.unwrap(), 5);
    assert_eq!(calls.lock().unwrap().len(), 5);
    assert_eq!(api.dead().size().await.unwrap(), 2);
}

#[tokio::test]
async fn retry_all_leaves_unparseable_jobs_in_place() {
    let (_redis, api) = setup().await;
    add_retry(&api, "good", now_secs_f64()).await;
    let () = run(
        &api,
        redis_rs::cmd("ZADD").arg("retry").arg(1.0).arg("not json"),
    )
    .await;
    assert_eq!(api.retries().retry_all().await.unwrap(), 1);
    let left = collect(api.retries().entries()).await;
    assert_eq!(left.len(), 1);
    assert_eq!(left[0].value(), "not json");
    assert_eq!(left[0].score(), 1.0);
    assert!(left[0].add_to_queue().await.is_err());
    assert!(left[0].retry().await.is_err());
    assert_eq!(api.retries().size().await.unwrap(), 1);
}

/// Fails every push of a job whose first argument is `"fail"`.
struct FailingPush;

#[async_trait]
impl ClientMiddleware for FailingPush {
    async fn call(
        &self,
        job: JobPayload,
        _redis: &RedisPool,
        next: ClientNext<'_>,
    ) -> hench::Result<Option<JobPayload>> {
        if job.args.first() == Some(&json!("fail")) {
            return Err(Error::InvalidJob("refused by middleware".into()));
        }
        next.run(job).await
    }
}

#[tokio::test]
async fn failed_pushes_put_jobs_back() {
    let (_redis, api) = setup().await;
    let client = api.client().with_middleware(|chain| {
        chain.add(FailingPush);
    });
    let api = Api::from_client(client);
    let now = now_secs_f64();
    for (i, first) in ["ok", "fail", "ok"].into_iter().enumerate() {
        let job =
            json!({"class": "ApiJob", "args": [first], "queue": "default", "jid": format!("j{i}")})
                .to_string();
        let () = run(
            &api,
            redis_rs::cmd("ZADD")
                .arg("retry")
                .arg(now + i as f64)
                .arg(job),
        )
        .await;
    }
    // The rejected job is put back and skipped; the others are pushed.
    assert_eq!(api.retries().retry_all().await.unwrap(), 2);
    let queued: Vec<String> = common::queue_jobs(api.redis(), "default")
        .await
        .into_iter()
        .map(|job| job.jid)
        .collect();
    assert_eq!(queued.len(), 2);
    assert!(!queued.contains(&"j1".to_owned()));
    let left = collect(api.retries().entries()).await;
    assert_eq!(jids(&left), ["j1"]);
    assert_eq!(left[0].score(), now + 1.0);
    // It does not block later calls either.
    assert_eq!(api.retries().retry_all().await.unwrap(), 0);
    assert_eq!(api.retries().size().await.unwrap(), 1);

    // Single entries report the failure.
    let failing = left[0].clone();
    let error = failing.add_to_queue().await.unwrap_err();
    assert!(matches!(error, Error::InvalidJob(_)), "{error:?}");
    assert!(failing.retry().await.is_err());
    assert_eq!(api.retries().size().await.unwrap(), 1);
}

/// Stops (without an error) every push of a job whose first argument is `"drop"`.
struct DropPush;

#[async_trait]
impl ClientMiddleware for DropPush {
    async fn call(
        &self,
        job: JobPayload,
        _redis: &RedisPool,
        next: ClientNext<'_>,
    ) -> hench::Result<Option<JobPayload>> {
        if job.args.first() == Some(&json!("drop")) {
            return Ok(None);
        }
        next.run(job).await
    }
}

#[tokio::test]
async fn pushes_stopped_by_middleware_drop_the_job_like_sidekiq() {
    let (_redis, api) = setup().await;
    let api = Api::from_client(api.client().with_middleware(|chain| {
        chain.add(DropPush);
    }));
    let now = now_secs_f64();
    for (i, first) in ["keep", "drop", "keep", "drop"].into_iter().enumerate() {
        let job =
            json!({"class": "ApiJob", "args": [first], "queue": "default", "jid": format!("j{i}")})
                .to_string();
        let () = run(
            &api,
            redis_rs::cmd("ZADD")
                .arg("retry")
                .arg(now + i as f64)
                .arg(job),
        )
        .await;
    }
    // A single entry: taken from the set, then dropped by the middleware.
    let dropped = api.retries().find_job("j1").await.unwrap().unwrap();
    assert!(dropped.retry().await.unwrap());
    assert!(api.retries().find_job("j1").await.unwrap().is_none());
    assert!(api.queue("default").find_job("j1").await.unwrap().is_none());

    // retry_all counts only the jobs actually pushed.
    assert_eq!(api.retries().retry_all().await.unwrap(), 2);
    assert_eq!(api.retries().size().await.unwrap(), 0);
    let mut queued: Vec<String> = common::queue_jobs(api.redis(), "default")
        .await
        .into_iter()
        .map(|job| job.jid)
        .collect();
    // Moves run concurrently: compare without order.
    queued.sort();
    assert_eq!(queued, ["j0", "j2"]);
}

/// Delays every push, to cancel callers in the middle of one.
struct SlowPush;

#[async_trait]
impl ClientMiddleware for SlowPush {
    async fn call(
        &self,
        job: JobPayload,
        _redis: &RedisPool,
        next: ClientNext<'_>,
    ) -> hench::Result<Option<JobPayload>> {
        tokio::time::sleep(Duration::from_millis(300)).await;
        next.run(job).await
    }
}

#[tokio::test]
async fn cancelled_moves_lose_nothing() {
    let (_redis, api) = setup().await;
    let api = Api::from_client(api.client().with_middleware(|chain| {
        chain.add(SlowPush);
    }));
    add_retry(&api, "slow", now_secs_f64()).await;
    let entry = api.retries().find_job("slow").await.unwrap().unwrap();
    // The caller gives up while client middleware runs: nothing has changed in Redis yet.
    let outcome = tokio::time::timeout(Duration::from_millis(50), entry.retry()).await;
    assert!(outcome.is_err(), "the retry should still be running");
    assert_eq!(api.retries().size().await.unwrap(), 1);
    assert_eq!(api.queue("default").size().await.unwrap(), 0);
    // And nothing happens later either.
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(api.retries().size().await.unwrap(), 1);
    assert_eq!(api.queue("default").size().await.unwrap(), 0);

    // Same for retry_all: at any time, every job is in exactly one place.
    for i in 0..3 {
        add_retry(&api, &format!("all{i}"), now_secs_f64() + f64::from(i)).await;
    }
    let outcome = tokio::time::timeout(Duration::from_millis(50), api.retries().retry_all()).await;
    assert!(outcome.is_err());
    let queued = api.queue("default").size().await.unwrap();
    let left = api.retries().size().await.unwrap();
    assert_eq!(queued + left, 4, "{queued} queued, {left} left");
    // The retry completes when given time.
    assert_eq!(api.retries().retry_all().await.unwrap(), left);
    assert_eq!(api.queue("default").size().await.unwrap(), 4);
}

#[tokio::test]
async fn deleting_entries_not_yet_iterated_does_not_skip_others() {
    let (_redis, api) = setup().await;
    let now = now_secs_f64();
    for i in 0..120 {
        add_retry(&api, &format!("j{i:03}"), now + f64::from(i)).await;
    }
    let retries = api.retries();
    let mut seen = Vec::new();
    let mut entries = std::pin::pin!(retries.entries());
    while let Some(entry) = entries.try_next().await.unwrap() {
        if seen.is_empty() {
            // Through the same handle: the lowest entry, which has not been yielded yet.
            assert!(retries.delete_by_jid(now, "j000").await.unwrap());
        }
        seen.push(entry.jid().unwrap().to_owned());
    }
    let expected: Vec<String> = (1..120).rev().map(|i| format!("j{i:03}")).collect();
    assert_eq!(seen, expected);
}

/// Removes the lowest entry of the retry set during the first push, like a scheduler taking
/// a due job while `retry_all` runs.
struct RemoveLowestOnce(std::sync::atomic::AtomicBool);

#[async_trait]
impl ClientMiddleware for RemoveLowestOnce {
    async fn call(
        &self,
        job: JobPayload,
        redis: &RedisPool,
        next: ClientNext<'_>,
    ) -> hench::Result<Option<JobPayload>> {
        if !self.0.swap(true, Ordering::SeqCst) {
            let mut conn = redis.conn().await?;
            let () = redis_rs::cmd("ZPOPMIN")
                .arg("retry")
                .query_async(&mut conn)
                .await?;
        }
        next.run(job).await
    }
}

#[tokio::test]
async fn retry_all_is_not_confused_by_unparseable_jobs_disappearing() {
    let (_redis, api) = setup().await;
    let api = Api::from_client(api.client().with_middleware(|chain| {
        chain.add(RemoveLowestOnce(std::sync::atomic::AtomicBool::new(false)));
    }));
    let now = now_secs_f64();
    let () = run(
        &api,
        redis_rs::cmd("ZADD")
            .arg("retry")
            .arg(1.0)
            .arg("garbage 1")
            .arg(2.0)
            .arg("garbage 2"),
    )
    .await;
    for i in 0..150 {
        add_retry(&api, &format!("j{i:03}"), now + f64::from(i)).await;
    }
    assert_eq!(api.retries().retry_all().await.unwrap(), 150);
    assert_eq!(api.queue("default").size().await.unwrap(), 150);
    let left: Vec<String> = collect(api.retries().entries())
        .await
        .iter()
        .map(|entry| entry.value().to_owned())
        .collect();
    assert_eq!(left, ["garbage 2"]);
}

/// Panics on every push of a job whose first argument is `"panic"`.
struct PanickingPush;

#[async_trait]
impl ClientMiddleware for PanickingPush {
    async fn call(
        &self,
        job: JobPayload,
        _redis: &RedisPool,
        next: ClientNext<'_>,
    ) -> hench::Result<Option<JobPayload>> {
        assert!(job.args.first() != Some(&json!("panic")), "middleware bug");
        next.run(job).await
    }
}

#[tokio::test]
async fn a_panicking_push_puts_the_job_back() {
    use futures::FutureExt;
    let (_redis, api) = setup().await;
    let api = Api::from_client(api.client().with_middleware(|chain| {
        chain.add(PanickingPush);
    }));
    let job =
        json!({"class": "ApiJob", "args": ["panic"], "queue": "default", "jid": "p"}).to_string();
    let () = run(&api, redis_rs::cmd("ZADD").arg("retry").arg(5.0).arg(&job)).await;
    let entry = api.retries().find_job("p").await.unwrap().unwrap();
    let outcome = std::panic::AssertUnwindSafe(entry.retry())
        .catch_unwind()
        .await;
    assert!(outcome.is_err(), "the panic propagates");
    let left = collect(api.retries().entries()).await;
    assert_eq!(jids(&left), ["p"]);
    assert_eq!(left[0].score(), 5.0);
    assert_eq!(api.queue("default").size().await.unwrap(), 0);
}

#[tokio::test]
async fn jobs_with_unusual_jids_can_be_found() {
    let (_redis, api) = setup().await;
    let jid = "a\\b\"c*d?";
    ApiJob::set()
        .jid(jid)
        .perform_async(api.client(), vec![])
        .await
        .unwrap();
    ApiJob::set()
        .jid(jid)
        .perform_in(api.client(), Duration::from_secs(60), vec![])
        .await
        .unwrap();
    assert_eq!(
        api.queue("default")
            .find_job(jid)
            .await
            .unwrap()
            .unwrap()
            .jid(),
        Some(jid)
    );
    assert_eq!(
        api.scheduled().find_job(jid).await.unwrap().unwrap().jid(),
        Some(jid)
    );
}

#[tokio::test]
async fn retry_all_skips_jobs_whose_push_fails_on_redis_or_panics() {
    let (_redis, api) = setup().await;
    let api = Api::from_client(api.client().with_middleware(|chain| {
        chain.add(PanickingPush);
    }));
    // `queue:broken` is not a list: pushing there fails with WRONGTYPE, a Redis error about
    // that job only.
    let () = run(
        &api,
        redis_rs::cmd("SET").arg("queue:broken").arg("not a list"),
    )
    .await;
    let now = now_secs_f64();
    let jobs = [
        (
            "wrongtype",
            json!({"class": "ApiJob", "args": [1], "queue": "broken", "jid": "wrongtype"}),
        ),
        (
            "panics",
            json!({"class": "ApiJob", "args": ["panic"], "queue": "default", "jid": "panics"}),
        ),
        (
            "good",
            json!({"class": "ApiJob", "args": [2], "queue": "default", "jid": "good"}),
        ),
    ];
    for (i, (_, job)) in jobs.iter().enumerate() {
        let () = run(
            &api,
            redis_rs::cmd("ZADD")
                .arg("retry")
                .arg(now + i as f64)
                .arg(job.to_string()),
        )
        .await;
    }
    assert_eq!(api.retries().retry_all().await.unwrap(), 1);
    let mut left = jids(&collect(api.retries().entries()).await);
    left.sort();
    assert_eq!(left, ["panics", "wrongtype"]);
    // They do not block later calls.
    assert_eq!(api.retries().retry_all().await.unwrap(), 0);
    assert_eq!(api.queue("default").size().await.unwrap(), 1);
}

#[tokio::test]
async fn retry_all_handles_many_jobs_with_the_same_score() {
    let (_redis, api) = setup().await;
    let api = Api::from_client(api.client().with_middleware(|chain| {
        chain.add(FailingPush);
    }));
    // 250 jobs at one score (more than a batch), every tenth rejected, then later jobs.
    let mut pipe = redis_rs::pipe();
    for i in 0..250 {
        let first = if i % 10 == 0 { "fail" } else { "ok" };
        let job = json!({"class": "ApiJob", "args": [first, i], "queue": "default", "jid": format!("same{i:03}")});
        pipe.cmd("ZADD")
            .arg("retry")
            .arg(1_000.0)
            .arg(job.to_string())
            .ignore();
    }
    for i in 0..30 {
        let job = json!({"class": "ApiJob", "args": ["ok", i], "queue": "default", "jid": format!("later{i:02}")});
        pipe.cmd("ZADD")
            .arg("retry")
            .arg(2_000.0 + f64::from(i))
            .arg(job.to_string())
            .ignore();
    }
    let mut conn = api.redis().conn().await.unwrap();
    let () = pipe.query_async(&mut conn).await.unwrap();

    assert_eq!(api.retries().retry_all().await.unwrap(), 225 + 30);
    let left = collect(api.retries().entries()).await;
    assert_eq!(left.len(), 25);
    assert!(left.iter().all(|entry| entry.args()[0] == json!("fail")));
    assert_eq!(api.queue("default").size().await.unwrap(), 255);
}

#[tokio::test]
async fn fetch_range_reads_wide_ranges_in_pages() {
    let (_redis, api) = setup().await;
    let mut pipe = redis_rs::pipe();
    // 700 jobs sharing one score and 600 with distinct scores: several pages, a tie group
    // spanning a page boundary.
    for i in 0..700 {
        let job = json!({"class": "ApiJob", "args": [i], "jid": format!("tie{i:03}")});
        pipe.cmd("ZADD")
            .arg("dead")
            .arg(10.0)
            .arg(job.to_string())
            .ignore();
    }
    for i in 0..600 {
        let job = json!({"class": "ApiJob", "args": [i], "jid": format!("solo{i:03}")});
        pipe.cmd("ZADD")
            .arg("dead")
            .arg(20.0 + f64::from(i))
            .arg(job.to_string())
            .ignore();
    }
    let mut conn = api.redis().conn().await.unwrap();
    let () = pipe.query_async(&mut conn).await.unwrap();

    let dead = api.dead();
    let all = dead
        .fetch_range(f64::NEG_INFINITY, f64::INFINITY, None)
        .await
        .unwrap();
    assert_eq!(all.len(), 1_300);
    let unique: std::collections::HashSet<String> = jids(&all).into_iter().collect();
    assert_eq!(unique.len(), 1_300);
    assert_eq!(dead.fetch_range(10.0, 10.0, None).await.unwrap().len(), 700);
    let found = dead
        .fetch_range(0.0, 1_000.0, Some("solo599"))
        .await
        .unwrap();
    assert_eq!(jids(&found), ["solo599"]);
    assert!(dead.fetch_range(11.0, 19.0, None).await.unwrap().is_empty());
}

#[tokio::test]
async fn moves_lose_nothing_when_redis_is_out_of_memory() {
    let (_redis, api) = setup().await;
    let now = now_secs_f64();
    for i in 0..20 {
        add_retry(&api, &format!("oom{i:02}"), now + f64::from(i)).await;
    }
    let () = run(
        &api,
        redis_rs::cmd("CONFIG")
            .arg("SET")
            .arg("maxmemory-policy")
            .arg("noeviction"),
    )
    .await;
    let () = run(
        &api,
        redis_rs::cmd("CONFIG").arg("SET").arg("maxmemory").arg(1),
    )
    .await;

    let entry = api.retries().find_job("oom00").await.unwrap().unwrap();
    let error = entry.retry().await.unwrap_err();
    assert!(error.to_string().contains("OOM"), "{error}");
    let error = api.retries().retry_all().await.unwrap_err();
    assert!(error.to_string().contains("OOM"), "{error}");
    assert!(api.retries().kill_all(false).await.is_err());
    assert!(entry.kill().await.is_err());
    assert!(entry.add_to_queue().await.is_err());

    let () = run(
        &api,
        redis_rs::cmd("CONFIG").arg("SET").arg("maxmemory").arg(0),
    )
    .await;
    // Every job is still in the retry set, and nothing reached the queue or the morgue.
    assert_eq!(api.retries().size().await.unwrap(), 20);
    assert_eq!(api.queue("default").size().await.unwrap(), 0);
    assert_eq!(api.dead().size().await.unwrap(), 0);
    assert_eq!(api.retries().retry_all().await.unwrap(), 20);
}

#[tokio::test]
async fn killing_a_dead_job_refreshes_it() {
    let (_redis, api) = setup().await;
    add_dead(&api, "old", 1_000.0).await;
    let entry = api.dead().find_job("old").await.unwrap().unwrap();
    assert!(entry.kill().await.unwrap());
    let dead = collect(api.dead().entries()).await;
    assert_eq!(jids(&dead), ["old"]);
    assert!(dead[0].score() > now_secs_f64() - 60.0);
    // kill_all on the morgue itself keeps every job, with a fresh time.
    add_dead(&api, "older", 500.0).await;
    assert_eq!(api.dead().kill_all(false).await.unwrap(), 2);
    let dead = collect(api.dead().entries()).await;
    assert_eq!(dead.len(), 2);
    assert!(
        dead.iter()
            .all(|entry| entry.score() > now_secs_f64() - 60.0)
    );
}

/// Schedules jobs whose first argument is `"later"` an hour from now.
struct Postpone;

#[async_trait]
impl ClientMiddleware for Postpone {
    async fn call(
        &self,
        mut job: JobPayload,
        _redis: &RedisPool,
        next: ClientNext<'_>,
    ) -> hench::Result<Option<JobPayload>> {
        if job.args.first() == Some(&json!("later")) {
            job.at = Some(now_secs_f64() + 3_600.0);
        }
        next.run(job).await
    }
}

#[tokio::test]
async fn jobs_staying_in_their_set_do_not_disturb_iteration() {
    let (_redis, api) = setup().await;
    let api = Api::from_client(api.client().with_middleware(|chain| {
        chain.add(Postpone);
    }));
    let now = now_secs_f64();
    let mut pipe = redis_rs::pipe();
    for i in 0..120 {
        let first = if i % 3 == 0 { "later" } else { "now" };
        let job = json!({"class": "ApiJob", "args": [first, i], "queue": "default", "jid": format!("s{i:03}")});
        pipe.cmd("ZADD")
            .arg("schedule")
            .arg(now - 1_000.0 + f64::from(i))
            .arg(job.to_string())
            .ignore();
    }
    let mut conn = api.redis().conn().await.unwrap();
    let () = pipe.query_async(&mut conn).await.unwrap();

    // Postponed jobs move within the scheduled set (to the top): the iteration still visits
    // every job exactly once.
    let mut seen = std::collections::HashSet::new();
    let mut entries = std::pin::pin!(api.scheduled().entries());
    while let Some(entry) = entries.try_next().await.unwrap() {
        assert!(
            seen.insert(entry.jid().unwrap().to_owned()),
            "{:?} twice",
            entry.jid()
        );
        assert!(entry.add_to_queue().await.unwrap());
    }
    assert_eq!(seen.len(), 120);
    assert_eq!(api.queue("default").size().await.unwrap(), 80);
    let postponed = collect(api.scheduled().entries()).await;
    assert_eq!(postponed.len(), 40);
    assert!(postponed.iter().all(|entry| entry.score() > now + 3_000.0));

    // Killing morgue entries refreshes them in place; iterating still visits each once.
    for i in 0..60 {
        add_dead(&api, &format!("d{i:02}"), now - 1_000.0 + f64::from(i)).await;
    }
    let mut seen = std::collections::HashSet::new();
    let mut entries = std::pin::pin!(api.dead().entries());
    while let Some(entry) = entries.try_next().await.unwrap() {
        assert!(seen.insert(entry.jid().unwrap().to_owned()));
        assert!(entry.kill().await.unwrap());
    }
    assert_eq!(seen.len(), 60);
    assert_eq!(api.dead().size().await.unwrap(), 60);
}

#[tokio::test]
async fn concurrent_moves_of_one_entry_push_it_once() {
    let (_redis, api) = setup().await;
    let jid = ApiJob::perform_in(api.client(), Duration::from_secs(100), vec![])
        .await
        .unwrap()
        .unwrap();
    let entry = api.scheduled().find_job(&jid).await.unwrap().unwrap();
    let attempts: Vec<_> = (0..10)
        .map(|i| {
            let entry = entry.clone();
            tokio::spawn(async move {
                if i % 2 == 0 {
                    entry.add_to_queue().await.unwrap()
                } else {
                    entry.kill().await.unwrap()
                }
            })
        })
        .collect();
    let mut successes = 0;
    for attempt in attempts {
        if attempt.await.unwrap() {
            successes += 1;
        }
    }
    assert_eq!(successes, 1);
    let queued = api.queue("default").size().await.unwrap();
    let dead = api.dead().size().await.unwrap();
    assert_eq!(queued + dead, 1);
    assert_eq!(api.scheduled().size().await.unwrap(), 0);
}

#[tokio::test]
async fn concurrent_retry_all_calls_push_each_job_once() {
    let (_redis, api) = setup().await;
    let now = now_secs_f64();
    let mut pipe = redis_rs::pipe();
    for i in 0..300 {
        let job =
            json!({"class": "ApiJob", "args": [i], "queue": "default", "jid": format!("j{i}")})
                .to_string();
        pipe.cmd("ZADD")
            .arg("retry")
            .arg(now + f64::from(i))
            .arg(job)
            .ignore();
    }
    let mut conn = api.redis().conn().await.unwrap();
    let () = pipe.query_async(&mut conn).await.unwrap();

    let calls: Vec<_> = (0..3)
        .map(|_| {
            let retries = api.retries();
            tokio::spawn(async move { retries.retry_all().await.unwrap() })
        })
        .collect();
    let mut pushed = 0;
    for call in calls {
        pushed += call.await.unwrap();
    }
    assert_eq!(pushed, 300);
    assert_eq!(api.retries().size().await.unwrap(), 0);
    let jobs = common::queue_jobs(api.redis(), "default").await;
    let unique: std::collections::HashSet<String> =
        jobs.iter().map(|job| job.jid.clone()).collect();
    assert_eq!((jobs.len(), unique.len()), (300, 300));
}

// --------------------------------------------------------------------------- filtering --

#[tokio::test]
async fn filtering_finds_entries_containing_a_substring() {
    let (_redis, api) = setup().await;
    let add = |set: &'static str, jid: &'static str, arg: &'static str| {
        let api = api.clone();
        async move {
            let job = json!({"class": "FilterJob", "args": [arg], "queue": "default", "jid": jid})
                .to_string();
            let () = run(
                &api,
                redis_rs::cmd("ZADD").arg(set).arg(now_secs_f64()).arg(job),
            )
            .await;
        }
    };
    add("retry", "jid123", "mike").await;
    add("retry", "jid456", "jim").await;
    add("dead", "jid123", "mike").await;
    add("dead", "jid456", "jim").await;

    let matching = |set: JobSet, pattern: &'static str| async move {
        let mut jids = jids(&collect(set.scan(pattern)).await);
        jids.sort();
        jids
    };
    assert_eq!(
        matching(api.retries().into_inner(), "").await,
        ["jid123", "jid456"]
    );
    assert_eq!(
        matching(api.retries().into_inner(), "mike").await,
        ["jid123"]
    );
    assert_eq!(matching(api.dead().into_inner(), "jim").await, ["jid456"]);

    let jid1 = ApiJob::perform_in(
        api.client(),
        Duration::from_secs(5),
        vec![json!("bob"), json!("tammy")],
    )
    .await
    .unwrap()
    .unwrap();
    ApiJob::perform_in(
        api.client(),
        Duration::from_secs(5),
        vec![json!("mike"), json!("jim")],
    )
    .await
    .unwrap();
    assert_eq!(matching(api.scheduled().into_inner(), "").await.len(), 2);
    assert_eq!(
        matching(api.scheduled().into_inner(), "tammy").await,
        [jid1]
    );
}

// ------------------------------------------------------------------------- processes --

const IDENTITY: &str = "identity_string";

/// Info JSON written by hand: capsule and queue order matter.
fn process_info(embedded: bool) -> String {
    format!(
        r#"{{"pid":123,"hostname":"host","identity":"{IDENTITY}","started_at":{},"tag":"app","labels":["blue"],"capsules":{{"default":{{"mode":"weighted","concurrency":5,"weights":{{"foo":1,"bar":1}}}},"single":{{"mode":"strict","concurrency":1,"weights":{{"single":0}}}}}},"version":"0.1.0","embedded":{embedded},"concurrency":99}}"#,
        now_secs_f64() - 15.0
    )
}

async fn add_process(api: &Api, identity: &str, info: &str, fields: &[(&str, &str)]) {
    let () = run(api, redis_rs::cmd("SADD").arg("processes").arg(identity)).await;
    let mut command = redis_rs::cmd("HSET");
    command.arg(identity).arg("info").arg(info);
    for (field, value) in fields {
        command.arg(*field).arg(*value);
    }
    let () = run(api, &mut command).await;
}

#[tokio::test]
async fn processes_are_enumerated_with_their_heartbeat() {
    let (_redis, api) = setup().await;
    let beat = now_secs_f64();
    add_process(
        &api,
        IDENTITY,
        &process_info(false),
        &[("busy", "10"), ("beat", &beat.to_string())],
    )
    .await;
    // A process whose heartbeat expired.
    let () = run(&api, redis_rs::cmd("SADD").arg("processes").arg("fake:pid")).await;

    let processes = ProcessSet::new(&api).list().await.unwrap();
    assert_eq!(processes.len(), 1);
    let process = &processes[0];
    assert_eq!(process.identity(), IDENTITY);
    assert_eq!(process.busy(), 10);
    let beat_secs = process
        .beat()
        .unwrap()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64();
    assert!((beat_secs - beat).abs() < 1e-3);
    assert_eq!(process.pid(), Some(123));
    assert_eq!(process.hostname(), Some("host"));
    assert_eq!(process.tag(), Some("app"));
    assert_eq!(process.labels(), ["blue"]);
    assert_eq!(process.queues(), ["foo", "bar", "single"]);
    assert_eq!(
        process.weights(),
        [
            ("foo".to_owned(), 1),
            ("bar".to_owned(), 1),
            ("single".to_owned(), 0)
        ]
    );
    let capsules = process.capsules();
    assert_eq!(capsules.len(), 2);
    assert_eq!(
        (
            capsules[0].name.as_str(),
            capsules[0].mode.as_str(),
            capsules[0].concurrency
        ),
        ("default", "weighted", 5)
    );
    assert_eq!(capsules[1].weights, [("single".to_owned(), 0)]);
    assert_eq!(process.version(), Some("0.1.0"));
    assert!(!process.is_embedded());
    assert!(process.started_at().unwrap() < SystemTime::now());
    // Without a `concurrency` heartbeat field, the description's is used.
    assert_eq!(process.concurrency(), 99);

    process.quiet().await.unwrap();
    process.stop().await.unwrap();
    let signals = format!("{IDENTITY}-signals");
    let first: Option<String> = run(&api, redis_rs::cmd("LPOP").arg(&signals)).await;
    let second: Option<String> = run(&api, redis_rs::cmd("LPOP").arg(&signals)).await;
    assert_eq!(
        (first.as_deref(), second.as_deref()),
        (Some("TERM"), Some("TSTP"))
    );
}

#[tokio::test]
async fn process_concurrency_comes_from_the_heartbeat() {
    let (_redis, api) = setup().await;
    add_process(
        &api,
        IDENTITY,
        &process_info(false),
        &[("concurrency", "7"), ("busy", "10")],
    )
    .await;
    let process = api.processes().list().await.unwrap().remove(0);
    assert_eq!(process.concurrency(), 7);
}

#[tokio::test]
async fn processes_can_be_found_by_identity() {
    let (_redis, api) = setup().await;
    let beat = now_secs_f64();
    add_process(
        &api,
        IDENTITY,
        &process_info(true),
        &[
            ("busy", "10"),
            ("beat", &beat.to_string()),
            ("rss", "2048"),
            ("rtt_us", "350"),
            ("quiet", "true"),
        ],
    )
    .await;
    let processes = api.processes();
    assert!(processes.get("nope").await.unwrap().is_none());
    let process = processes.get(IDENTITY).await.unwrap().unwrap();
    assert_eq!(process.busy(), 10);
    assert_eq!(process.pid(), Some(123));
    assert_eq!(process.queues(), ["foo", "bar", "single"]);
    assert_eq!(process.version(), Some("0.1.0"));
    assert!(process.is_embedded());
    assert_eq!((process.rss_kb(), process.rtt_us()), (2048, 350));
    assert!(process.is_stopping());

    // Registered but without a heartbeat: not a live process.
    let () = run(&api, redis_rs::cmd("SADD").arg("processes").arg("ghost")).await;
    assert!(processes.get("ghost").await.unwrap().is_none());
    // A heartbeat without registration: not listed either.
    let () = run(
        &api,
        redis_rs::cmd("HSET")
            .arg("unregistered")
            .arg("info")
            .arg("{}"),
    )
    .await;
    assert!(processes.get("unregistered").await.unwrap().is_none());
}

#[tokio::test]
async fn embedded_processes_can_be_signalled_too() {
    let (_redis, api) = setup().await;
    add_process(&api, IDENTITY, &process_info(true), &[]).await;
    let process = api.processes().get(IDENTITY).await.unwrap().unwrap();
    assert!(process.is_embedded());
    process.quiet().await.unwrap();
    let signals: Vec<String> = run(
        &api,
        redis_rs::cmd("LRANGE")
            .arg(format!("{IDENTITY}-signals"))
            .arg(0)
            .arg(-1),
    )
    .await;
    assert_eq!(signals, ["TSTP"]);
    let ttl: i64 = run(
        &api,
        redis_rs::cmd("TTL").arg(format!("{IDENTITY}-signals")),
    )
    .await;
    assert!(ttl > 0 && ttl <= 60, "{ttl}");
}

#[tokio::test]
async fn process_stopping_reflects_the_quiet_flag() {
    let (_redis, api) = setup().await;
    for (identity, quiet) in [
        ("h1:1", None),
        ("h2:2", Some("true")),
        ("h3:3", Some("false")),
    ] {
        let fields: Vec<(&str, &str)> = quiet
            .map(|quiet| vec![("quiet", quiet)])
            .unwrap_or_default();
        add_process(&api, identity, "{}", &fields).await;
    }
    let stopping: Vec<(String, bool)> = api
        .processes()
        .list()
        .await
        .unwrap()
        .iter()
        .map(|process| (process.identity().to_owned(), process.is_stopping()))
        .collect();
    assert_eq!(
        stopping,
        [
            ("h1:1".to_owned(), false),
            ("h2:2".to_owned(), true),
            ("h3:3".to_owned(), false)
        ]
    );
}

#[tokio::test]
async fn dead_processes_are_pruned() {
    let (_redis, api) = setup().await;
    add_process(
        &api,
        "app1:123",
        r#"{"pid":123,"hostname":"app1"}"#,
        &[("busy", "0")],
    )
    .await;
    let processes = api.processes();
    assert_eq!(processes.cleanup().await.unwrap(), 0);
    assert_eq!(processes.size().await.unwrap(), 1);

    let () = run(
        &api,
        redis_rs::cmd("SADD")
            .arg("processes")
            .arg("bar:987")
            .arg("bar:986"),
    )
    .await;
    // The cleanup lock is held for a minute.
    assert_eq!(processes.cleanup().await.unwrap(), 0);
    assert_eq!(processes.size().await.unwrap(), 3);
    let () = run(&api, redis_rs::cmd("DEL").arg("process_cleanup")).await;
    assert_eq!(processes.cleanup().await.unwrap(), 2);
    assert_eq!(processes.size().await.unwrap(), 1);
    assert_eq!(processes.list().await.unwrap().len(), 1);
    let ttl: i64 = run(&api, redis_rs::cmd("TTL").arg("process_cleanup")).await;
    assert!(ttl > 0 && ttl <= 60, "{ttl}");
}

#[tokio::test]
async fn process_totals() {
    let (_redis, api) = setup().await;
    add_process(
        &api,
        "h1:1",
        r#"{"pid":1,"hostname":"h1"}"#,
        &[("concurrency", "5"), ("rss", "1024")],
    )
    .await;
    add_process(
        &api,
        "h2:2",
        r#"{"pid":2,"hostname":"h2"}"#,
        &[("concurrency", "12"), ("rss", "2048")],
    )
    .await;
    let processes = api.processes();
    assert_eq!(processes.total_concurrency().await.unwrap(), 17);
    assert_eq!(processes.total_rss().await.unwrap(), 3072);
}

// ------------------------------------------------------------------------------ work --

#[tokio::test]
async fn running_work_is_enumerated_longest_first() {
    let (_redis, api) = setup().await;
    let work = WorkSet::new(&api);
    assert_eq!(work.size().await.unwrap(), 0);
    assert!(work.list().await.unwrap().is_empty());

    let key = "host:42:abc";
    add_process(
        &api,
        key,
        r#"{"pid":42,"hostname":"host"}"#,
        &[("busy", "0")],
    )
    .await;
    let now = now_secs_f64();
    let entry = |payload: Value, run_at: f64| {
        json!({"payload": payload, "queue": "default", "run_at": run_at}).to_string()
    };
    let () = run(
        &api,
        redis_rs::cmd("HSET")
            .arg(format!("{key}:work"))
            .arg("1234")
            .arg(entry(json!("{}"), now)),
    )
    .await;

    let works = work.list().await.unwrap();
    assert_eq!(works.len(), 1);
    let first = &works[0];
    assert_eq!(first.process_id(), key);
    assert_eq!(first.slot(), "1234");
    assert_eq!(first.payload(), "{}");
    assert!(first.job().item().is_empty());
    assert_eq!(first.queue(), "default");
    let age = SystemTime::now().duration_since(first.run_at()).unwrap();
    assert!(age < Duration::from_secs(5));

    // An older job, plus a key that only looks like a work hash.
    let older = entry(json!({}), now - 2.0 * 60.0 * 60.0);
    let () = run(
        &api,
        redis_rs::cmd("HSET")
            .arg(format!("{key}:work"))
            .arg("5678")
            .arg(&older),
    )
    .await;
    let () = run(
        &api,
        redis_rs::cmd("HSET")
            .arg(format!("b{key}:work"))
            .arg("5678")
            .arg(&older),
    )
    .await;
    let slots: Vec<String> = work
        .list()
        .await
        .unwrap()
        .iter()
        .map(|work| work.slot().to_owned())
        .collect();
    assert_eq!(slots, ["5678", "1234"]);
}

#[tokio::test]
async fn running_work_can_be_found_by_jid() {
    let (_redis, api) = setup().await;
    let key = "host:42:abc";
    let () = run(&api, redis_rs::cmd("SADD").arg("processes").arg(key)).await;
    let payload = json!({"class": "ApiJob", "args": ["foo"], "jid": "abcdef"}).to_string();
    let entry =
        json!({"payload": payload, "queue": "default", "run_at": now_secs_f64()}).to_string();
    let () = run(
        &api,
        redis_rs::cmd("HSET")
            .arg(format!("{key}:work"))
            .arg("1234")
            .arg(entry),
    )
    .await;

    let work = api.work();
    assert!(work.find_work("nonexistent").await.unwrap().is_none());
    assert!(work.find_work("").await.unwrap().is_none());
    let found = work.find_work("abcdef").await.unwrap().unwrap();
    assert_eq!(found.job().args(), [json!("foo")]);
    assert_eq!(found.job().class(), Some("ApiJob"));
}

// ------------------------------------------------------------------------- paginator --

fn raw(entries: &[RawEntry]) -> Vec<(String, Option<f64>)> {
    entries
        .iter()
        .map(|entry| (entry.value.clone(), entry.score))
        .collect()
}

#[tokio::test]
async fn paginator_pages_sorted_sets_and_lists() {
    let (_redis, api) = setup().await;
    let () = run(
        &api,
        redis_rs::cmd("ZADD")
            .arg("test:zset")
            .arg(1)
            .arg("a")
            .arg(2)
            .arg("b")
            .arg(3)
            .arg("c"),
    )
    .await;
    let page = page_key(&api, "test:zset", 1, 25, SortOrder::Ascending)
        .await
        .unwrap();
    assert_eq!((page.number, page.total), (1, 3));
    assert_eq!(
        raw(&page.items),
        [
            ("a".to_owned(), Some(1.0)),
            ("b".to_owned(), Some(2.0)),
            ("c".to_owned(), Some(3.0))
        ]
    );
    let page = page_key(&api, "test:zset", 1, 25, SortOrder::Descending)
        .await
        .unwrap();
    assert_eq!(raw(&page.items)[0], ("c".to_owned(), Some(3.0)));

    let () = run(
        &api,
        redis_rs::cmd("RPUSH")
            .arg("test:list")
            .arg("x")
            .arg("y")
            .arg("z"),
    )
    .await;
    let page = page_key(&api, "test:list", 1, 25, SortOrder::Descending)
        .await
        .unwrap();
    assert_eq!((page.number, page.total), (1, 3));
    let values: Vec<String> = page.items.iter().map(|entry| entry.value.clone()).collect();
    assert_eq!(values, ["x", "y", "z"]);
    let page = page_key(&api, "test:list", 1, 25, SortOrder::Ascending)
        .await
        .unwrap();
    let values: Vec<String> = page.items.iter().map(|entry| entry.value.clone()).collect();
    assert_eq!(values, ["z", "y", "x"]);
    let page = page_key(&api, "test:list", 2, 2, SortOrder::Descending)
        .await
        .unwrap();
    assert_eq!(raw(&page.items), [("z".to_owned(), None)]);

    let missing = page_key(&api, "test:missing", 3, 25, SortOrder::Ascending)
        .await
        .unwrap();
    assert_eq!(
        (missing.number, missing.total, missing.items.len()),
        (1, 0, 0)
    );
    assert_eq!(
        page_key(&api, "test:zset", 0, 25, SortOrder::Ascending)
            .await
            .unwrap()
            .number,
        1
    );

    let () = run(&api, redis_rs::cmd("SET").arg("test:string").arg("v")).await;
    let error = page_key(&api, "test:string", 1, 25, SortOrder::Ascending)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("not a list or sorted set"),
        "{error}"
    );

    // hench's own keys are recognized without asking Redis for their type.
    add_dead(&api, "d", now_secs_f64()).await;
    let page = page_key(&api, "dead", 1, 25, SortOrder::Descending)
        .await
        .unwrap();
    assert_eq!(page.total, 1);
    ApiJob::perform_async(api.client(), vec![]).await.unwrap();
    let page = page_key(&api, "queue:default", 1, 25, SortOrder::Descending)
        .await
        .unwrap();
    assert_eq!(page.total, 1);
}

// ------------------------------------------------------------------------- handles --

#[tokio::test]
async fn api_handles_come_from_a_configured_hench() {
    let redis = TestRedis::start().await;
    let deaths = Arc::new(AtomicUsize::new(0));
    let counter = deaths.clone();
    let hench = redis
        .builder()
        .await
        .auto_register(false)
        .dead_max_jobs(2)
        .death_handler(move |_job, _error| {
            counter.fetch_add(1, Ordering::SeqCst);
        })
        .build()
        .await
        .unwrap();
    let api = Api::from_hench(&hench);
    assert_eq!(api.dead_max_jobs(), 2);
    let dead = DeadSet::new(&hench);
    dead.kill(&json!({"class": "A", "jid": "x"}).to_string(), true)
        .await
        .unwrap();
    assert_eq!(deaths.load(Ordering::SeqCst), 1);
    assert!(format!("{api:?}").contains("death_handlers: true"));
    let _ = (
        ScheduledSet::new(api.redis()),
        RetrySet::new(api.client()),
        format!("{:?}", api.scheduled()),
        format!("{:?}", api.retries()),
        format!("{:?}", api.dead()),
        format!("{:?}", api.processes()),
        format!("{:?}", api.work()),
        format!("{:?}", api.queue("q")),
    );
}

#[tokio::test]
async fn redis_info_describes_the_server() {
    let (_redis, api) = setup().await;
    let info = api.redis_info().await.unwrap();
    assert!(info["redis_version"].starts_with(char::is_numeric));
    for field in [
        "uptime_in_days",
        "connected_clients",
        "used_memory_human",
        "used_memory_peak_human",
    ] {
        assert!(info.contains_key(field), "{field} missing");
    }
}

#[tokio::test]
async fn api_calls_surface_redis_errors() {
    let mut redis = TestRedis::start().await;
    let api = Api::new(
        hench::RedisPool::from_config(redis.config().reconnect_retries(0))
            .await
            .unwrap(),
    );
    redis.stop();
    assert!(matches!(api.stats().await, Err(Error::Redis(_))));
    assert!(matches!(
        api.queue("default").size().await,
        Err(Error::Redis(_))
    ));
    assert!(matches!(
        api.retries().retry_all().await,
        Err(Error::Redis(_))
    ));
    assert!(matches!(api.processes().list().await, Err(Error::Redis(_))));
    let failed: Vec<hench::Result<SortedEntry>> = api.scheduled().entries().collect().await;
    assert!(matches!(failed.as_slice(), [Err(Error::Redis(_))]));

    // The API recovers once Redis is back.
    redis.restart().await;
    common::wait_until(Duration::from_secs(10), || async {
        api.stats().await.is_ok()
    })
    .await;
}

/// Every API future and stream can be awaited from a multi-threaded runtime, for example in
/// an axum handler (compile-time check).
#[test]
fn api_futures_are_send() {
    fn send<T: Send>(_: &T) {}
    fn check(
        api: Api,
        mut entry: SortedEntry,
        record: hench::api::JobRecord,
        process: hench::api::Process,
    ) {
        send(&api.stats());
        send(&api.redis_info());
        send(&Stats::queues(&api));
        send(&Stats::queue_summaries(&api));
        send(&Stats::reset(&api, &[]));
        send(&api.queues());
        let queue = api.queue("q");
        send(&queue.size());
        send(&queue.latency());
        send(&queue.page(1, 25, SortOrder::Ascending));
        send(&queue.jobs());
        send(&queue.find_job("j"));
        send(&queue.clear());
        send(&record.delete());
        let retries = api.retries();
        send(&retries.page(1, 25, SortOrder::Ascending));
        send(&retries.entries());
        send(&retries.scan("x"));
        send(&retries.fetch(1.0, None));
        send(&retries.find_job("j"));
        send(&retries.delete_by_jid(1.0, "j"));
        send(&retries.retry_all());
        send(&retries.kill_all(true));
        send(&api.dead().kill("{}", true));
        send(&api.dead().trim());
        send(&entry.retry());
        send(&entry.add_to_queue());
        send(&entry.kill());
        send(&entry.delete());
        send(&entry.reschedule(SystemTime::now()));
        send(&api.processes().list());
        send(&api.processes().cleanup());
        send(&process.quiet());
        send(&api.work().list());
        send(&page_key(&api, "k", 1, 25, SortOrder::Ascending));
    }
    let _ = check;
}

/// `processes_size` counts live processes: members of `processes` without a heartbeat died and
/// stay in the set until pruned.
#[tokio::test]
async fn stats_count_live_processes_only() {
    let (_redis, api) = setup().await;
    let () = run(
        &api,
        redis_rs::cmd("SADD")
            .arg("processes")
            .arg("alive:1:a")
            .arg("dead:2:b")
            .arg("dead:3:c"),
    )
    .await;
    let () = run(
        &api,
        redis_rs::cmd("HSET")
            .arg("alive:1:a")
            .arg("info")
            .arg("{}")
            .arg("busy")
            .arg(2),
    )
    .await;
    let stats = api.stats().await.unwrap();
    assert_eq!(stats.processes_size, 1);
    assert_eq!(stats.workers_size, 2);
    assert_eq!(api.processes().size().await.unwrap(), 3, "the raw set");
    assert_eq!(api.processes().list().await.unwrap().len(), 1);
}
