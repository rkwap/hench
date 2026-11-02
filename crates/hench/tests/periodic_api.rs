//! The periodic schedules through the API (`docs/design/periodic.md` §3.2 items 8, 15; BullMQ's
//! job scheduler scenarios): dynamic schedules created, updated, rescheduled and removed,
//! listing, reading leniently, pausing, resuming, "enqueue now", groups, repairs, and
//! `JobRecord::periodic`. Against private `redis-server`s.
#![cfg(all(feature = "test-util", feature = "periodic"))]

mod support;

use std::time::{Duration, SystemTime};

use futures::TryStreamExt;
use hench::api::Api;
use hench::periodic::{
    CatchUp, FireResult, ManualClock, Periodic, PeriodicSource, Timetable, Upserted,
};
use hench::prelude::*;
use hench::test_util::TestRedis;
use hench::{Error, keys};
use serde_json::json;
use support::*;

/// 2026-11-02 10:00:30 UTC.
const T0: u64 = 1_793_613_630;

fn at(seconds: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(seconds)
}

fn every(name: &str, seconds: u64) -> Periodic {
    Periodic::every(name, Duration::from_secs(seconds))
        .class("Report", vec![json!(1)])
        .queue("out")
}

#[tokio::test]
async fn upserts_create_update_reschedule_or_change_nothing() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let clock = ManualClock::new(at(T0));
    let set = Api::new(pool.clone()).periodic().with_clock(clock.clone());
    assert_eq!(
        set.upsert(&every("tenant-1", 60)).await.unwrap(),
        Upserted::Created
    );
    assert_eq!(
        set.upsert(&every("tenant-1", 60)).await.unwrap(),
        Upserted::Unchanged
    );
    let entry = set.get("tenant-1").await.unwrap().unwrap();
    assert_eq!(entry.next, Some(at(1_793_613_660)));
    assert_eq!(entry.source, PeriodicSource::Api);
    assert_eq!(
        entry.timetable,
        Some(Timetable::Every {
            interval: Duration::from_secs(60),
            offset: Duration::ZERO
        })
    );
    // A template change keeps the next tick.
    let updated = every("tenant-1", 60).queue("other");
    assert_eq!(set.upsert(&updated).await.unwrap(), Upserted::Updated);
    assert_eq!(
        set.get("tenant-1").await.unwrap().unwrap().next,
        Some(at(1_793_613_660))
    );
    // Same name, another interval: one schedule, rescheduled (BullMQ "same id and different
    // every").
    clock.set(at(T0 + 10));
    assert_eq!(
        set.upsert(&every("tenant-1", 30).queue("other"))
            .await
            .unwrap(),
        Upserted::Rescheduled
    );
    let entry = set.get("tenant-1").await.unwrap().unwrap();
    // The first tick of the new timetable after the last minute (09:59:40): 10:00:00.
    assert_eq!(entry.next, Some(at(1_793_613_600)));
    assert_eq!(set.size().await.unwrap(), 1);
    // Upserted in quick succession: still one schedule.
    let mut tasks = Vec::new();
    for i in 0..10 {
        let set = set.clone();
        tasks.push(tokio::spawn(async move {
            set.upsert(&every("burst", 60 + i)).await.unwrap()
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(set.size().await.unwrap(), 2);
    let names: Vec<String> = cmd(&pool, &redis_cmd!("ZRANGE", keys::PERIODIC, 0, -1)).await;
    assert_eq!(names.len(), 2);
}

#[tokio::test]
async fn removal_rules_and_remove_then_add_again() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let set = Api::new(pool.clone()).periodic();
    assert!(
        !set.remove("missing").await.unwrap(),
        "removing a missing schedule: false"
    );
    set.upsert(&every("s", 60)).await.unwrap();
    assert!(set.remove("s").await.unwrap());
    assert!(set.get("s").await.unwrap().is_none());
    // Adding it again after removing it works.
    assert_eq!(
        set.upsert(&every("s", 60)).await.unwrap(),
        Upserted::Created
    );
    // A code-defined schedule is refused by `remove`, removed by `remove_any`.
    let worker = redis
        .builder()
        .await
        .queues(["nothing"])
        .periodic_poller(false)
        .periodic(every("code", 60))
        .build()
        .await
        .unwrap()
        .start()
        .await
        .unwrap();
    wait_for("registered", WAIT, || async {
        set.get("code").await.unwrap().is_some()
    })
    .await;
    worker.stop().await.unwrap();
    assert!(matches!(
        set.remove("code").await,
        Err(Error::InvalidSchedule(_))
    ));
    // `upsert` refuses a name a code schedule uses.
    assert!(matches!(
        set.upsert(&every("code", 30)).await,
        Err(Error::InvalidSchedule(_))
    ));
    assert!(set.remove_any("code").await.unwrap());
    assert!(
        smembers(&pool, &keys::periodic_group("default"))
            .await
            .is_empty()
    );
    // Invalid definitions are refused before any write.
    assert!(matches!(
        set.upsert(&Periodic::cron("bad", "0 0 30 2 *").class("X", vec![]))
            .await,
        Err(Error::InvalidSchedule(_))
    ));
    assert!(set.get("bad").await.unwrap().is_none());
}

#[tokio::test]
async fn listing_pages_streams_and_reads_leniently() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let set = Api::new(pool.clone()).periodic();
    for i in 0..30 {
        set.upsert(&every(&format!("s{i:02}"), 60)).await.unwrap();
    }
    set.upsert(
        &Periodic::cron("described", "0 2 * * *")
            .tz("Europe/Paris")
            .class("Report", vec![])
            .catch_up(CatchUp::Skip)
            .description("Daily report"),
    )
    .await
    .unwrap();
    assert_eq!(set.size().await.unwrap(), 31);
    let page = set.page(2, 10).await.unwrap();
    assert_eq!(page.total, 31);
    let names: Vec<&str> = page.items.iter().map(|entry| entry.name.as_str()).collect();
    assert_eq!(
        names,
        [
            "s09", "s10", "s11", "s12", "s13", "s14", "s15", "s16", "s17", "s18"
        ]
    );
    let all: Vec<_> = set.entries().try_collect().await.unwrap();
    assert_eq!(all.len(), 31);
    assert_eq!(all[0].name, "described");
    let described = set.get("described").await.unwrap().unwrap();
    assert_eq!(described.description.as_deref(), Some("Daily report"));
    assert_eq!(described.catch_up, CatchUp::Skip);
    assert_eq!(described.deadline, Some(Duration::from_secs(300)));
    assert_eq!(described.describe(), "At 02:00 (Europe/Paris)");
    assert_eq!(described.upcoming(3).unwrap().len(), 3);
    assert_eq!(described.job.as_ref().unwrap().class, "Report");
    assert!(described.fields.contains_key("digest"));
    // A hash a newer version wrote, and one broken by hand, still list.
    let _: () = cmd(
        &pool,
        &redis_cmd!("HSET", keys::periodic("s00"), "spec", r#"{"v":9}"#),
    )
    .await;
    let _: () = cmd(
        &pool,
        &redis_cmd!("HSET", keys::periodic("s01"), "job", "{"),
    )
    .await;
    let newer = set.get("s00").await.unwrap().unwrap();
    assert!(newer.timetable.is_none());
    assert!(newer.error.as_deref().unwrap().contains("version 9"));
    assert!(newer.describe().starts_with("unreadable timetable"));
    assert!(newer.upcoming(1).is_err());
    let broken = set.get("s01").await.unwrap().unwrap();
    assert!(broken.job.is_none() && broken.error.is_some());
    // A name whose hash is gone is left out.
    let _: () = cmd(&pool, &redis_cmd!("DEL", keys::periodic("s02"))).await;
    let page = set.page(1, 5).await.unwrap();
    assert!(page.items.iter().all(|entry| entry.name != "s02"));
}

#[tokio::test]
async fn pause_resume_and_enqueue_now() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let clock = ManualClock::new(at(T0));
    let set = Api::new(pool.clone()).periodic().with_clock(clock.clone());
    set.upsert(&every("s", 60)).await.unwrap();
    let entry = set.get("s").await.unwrap().unwrap();
    assert!(entry.pause().await.unwrap());
    assert!(!set.pause("s").await.unwrap(), "already paused");
    assert!(!set.pause("missing").await.unwrap());
    let paused = set.get("s").await.unwrap().unwrap();
    assert!(paused.is_paused() && paused.next.is_none());
    clock.set(at(T0 + 600));
    assert!(paused.unpause().await.unwrap());
    assert!(!set.unpause("s").await.unwrap(), "not paused");
    assert_eq!(
        set.get("s").await.unwrap().unwrap().next,
        Some(at(1_793_614_260)),
        "resumed at the first tick after now"
    );
    // Enqueue now: a manual job, the timetable untouched, a history entry.
    let jid = set.enqueue("s").await.unwrap().unwrap();
    let jobs = queue_jobs(&pool, "out").await;
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].jid, jid);
    let meta = jobs[0].periodic.clone().unwrap();
    assert_eq!((meta.name.as_str(), meta.manual), ("s", true));
    assert_eq!(
        meta.tick,
        (T0 + 600) as i64 * 1_000,
        "the time of the request"
    );
    let entry = set.get("s").await.unwrap().unwrap();
    assert_eq!(entry.manual_jid.as_deref(), Some(jid.as_str()));
    assert_eq!(entry.next, Some(at(1_793_614_260)));
    let history = entry.history(10).await.unwrap();
    assert_eq!(history[0].result, Some(FireResult::Manual));
    assert_eq!(history[0].jid.as_deref(), Some(jid.as_str()));
    // JobRecord::periodic reads the tick back.
    let record = Api::new(pool.clone())
        .queue("out")
        .jobs()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let tick = record[0].periodic().unwrap();
    assert!(tick.manual);
    assert_eq!(tick.name, "s");
}

/// Round 3 (periodic-8): "enqueue now" through an `Api` that does not know the workers'
/// configuration (the dashboard, the JSON API, `hench periodic enqueue`) keeps a schedule's
/// history as long as it is (adding the new entry, up to the 1,000 entries readers show); it
/// used to trim it to 10 entries whatever the workers' `periodic_history`. An `Api` with a
/// configuration, and `with_history`, keep their number.
#[tokio::test]
async fn enqueue_now_without_a_configuration_keeps_the_history() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let clock = ManualClock::new(at(T0));
    let set = Api::new(pool.clone()).periodic().with_clock(clock.clone());
    set.upsert(&every("s", 60)).await.unwrap();
    let history = keys::periodic_history("s");
    for n in 1..=20 {
        assert!(set.enqueue("s").await.unwrap().is_some());
        assert_eq!(llen(&pool, &history).await, n, "nothing trimmed");
    }
    let hench = redis
        .builder()
        .await
        .periodic_history(15)
        .build()
        .await
        .unwrap();
    let configured = Api::new(pool.clone())
        .with_config(hench.config().clone())
        .periodic()
        .with_clock(clock.clone());
    configured.enqueue("s").await.unwrap();
    assert_eq!(
        llen(&pool, &history).await,
        15,
        "the workers' periodic_history"
    );
    set.with_history(5).enqueue("s").await.unwrap();
    assert_eq!(llen(&pool, &history).await, 5, "with_history");
    set.enqueue("s").await.unwrap();
    assert_eq!(llen(&pool, &history).await, 6, "the new entry added");
}

#[tokio::test]
async fn enqueue_now_respects_unique_locks_and_middleware() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let set = Api::new(pool.clone()).periodic();
    set.upsert(&every("unique", 60).unique_for(Duration::from_secs(600)))
        .await
        .unwrap();
    assert!(set.enqueue("unique").await.unwrap().is_some());
    // Its lock is held: a duplicate.
    assert_eq!(set.enqueue("unique").await.unwrap(), None);
    assert_eq!(queue_size(&pool, "out").await, 1);
    let history = set.history("unique", 10).await.unwrap();
    assert_eq!(history[0].result, Some(FireResult::Duplicate));
    // Client middleware of the API's client runs, and may stop the push.
    let client = redis.client().await.with_middleware(|chain| {
        chain.add(Halting);
    });
    let halted = Api::from_client(client).periodic();
    halted.upsert(&every("halted", 60)).await.unwrap();
    assert_eq!(halted.enqueue("halted").await.unwrap(), None);
    assert_eq!(
        set.history("halted", 1).await.unwrap()[0].result,
        Some(FireResult::Halted)
    );
}

/// Stops every push.
struct Halting;

#[async_trait]
impl ClientMiddleware for Halting {
    async fn call(
        &self,
        _job: JobPayload,
        _redis: &RedisPool,
        _next: ClientNext<'_>,
    ) -> hench::Result<Option<JobPayload>> {
        Ok(None)
    }
}

#[tokio::test]
async fn repairs_restore_the_indexes() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let set = Api::new(pool.clone()).periodic();
    for name in ["lost-entry", "lost-next", "paused", "fine"] {
        set.upsert(&every(name, 60)).await.unwrap();
    }
    set.pause("paused").await.unwrap();
    assert_eq!(set.repair().await.unwrap(), 0, "nothing to repair");
    let _: () = cmd(&pool, &redis_cmd!("ZREM", keys::PERIODIC, "lost-entry")).await;
    let _: () = cmd(
        &pool,
        &redis_cmd!("HDEL", keys::periodic("lost-next"), "next"),
    )
    .await;
    let _: () = cmd(&pool, &redis_cmd!("ZADD", keys::PERIODIC, 1, "paused")).await;
    let _: () = cmd(&pool, &redis_cmd!("ZADD", keys::PERIODIC_NAMES, 0, "ghost")).await;
    let _: () = cmd(&pool, &redis_cmd!("ZADD", keys::PERIODIC, 1, "ghost")).await;
    assert_eq!(set.repair().await.unwrap(), 4);
    let score: Option<f64> = cmd(&pool, &redis_cmd!("ZSCORE", keys::PERIODIC, "lost-entry")).await;
    assert!(score.is_some());
    assert!(set.get("lost-next").await.unwrap().unwrap().next.is_some());
    let paused: Option<f64> = cmd(&pool, &redis_cmd!("ZSCORE", keys::PERIODIC, "paused")).await;
    assert_eq!(paused, None);
    let ghost: Option<f64> = cmd(&pool, &redis_cmd!("ZSCORE", keys::PERIODIC_NAMES, "ghost")).await;
    assert_eq!(ghost, None);
    assert_eq!(set.repair().await.unwrap(), 0);
}

#[tokio::test]
async fn groups_list_the_registered_crontabs() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let set = Api::new(pool.clone()).periodic();
    assert!(set.groups().await.unwrap().is_empty());
    for (group, names) in [("billing", ["a", "b"]), ("reports", ["c", "d"])] {
        let mut builder = redis
            .builder()
            .await
            .queues(["nothing"])
            .periodic_poller(false)
            .periodic_group(group);
        for name in names {
            builder = builder.periodic(every(name, 60));
        }
        let launcher = builder.build().await.unwrap().start().await.unwrap();
        wait_for("registered", WAIT, || async {
            set.get(names[1]).await.unwrap().is_some()
        })
        .await;
        launcher.stop().await.unwrap();
    }
    let groups = set.groups().await.unwrap();
    assert_eq!(groups.len(), 2);
    assert_eq!((groups[0].group.as_str(), groups[0].count), ("billing", 2));
    assert_eq!(groups[0].digest.len(), 43);
    assert!(groups[0].by.is_some() && groups[0].at.is_some());
    assert_eq!(
        set.get("c").await.unwrap().unwrap().source,
        PeriodicSource::Code {
            group: "reports".to_owned()
        }
    );
}

#[tokio::test]
async fn fake_clients_take_periodic_jobs_with_their_tick() {
    // Testing clients: no poller; `Periodic::enqueue` pushes a tick's job like any push.
    let client = Client::fake();
    let periodic = every("s", 60);
    let tick = at(T0);
    let jid = periodic.enqueue(&client, tick).await.unwrap().unwrap();
    let job = periodic.payload(tick).unwrap();
    assert_eq!(job.periodic.unwrap().tick, T0 as i64 * 1_000);
    assert!(!jid.is_empty());
}

#[tokio::test]
async fn summaries_count_the_schedules_from_the_indexes() {
    let redis = TestRedis::start().await;
    let api = Api::new(redis.pool().await);
    let clock = ManualClock::new(at(T0));
    let set = api.periodic().with_clock(clock.clone());
    let late_after = Duration::from_secs(65);
    let empty = set.summary(late_after).await.unwrap();
    assert_eq!(
        (empty.total, empty.active, empty.late, empty.paused()),
        (0, 0, 0, 0)
    );
    assert_eq!(empty.next, None);
    set.upsert(&every("a", 60)).await.unwrap();
    set.upsert(&every("b", 3_600)).await.unwrap();
    set.upsert(&every("c", 60).paused(true)).await.unwrap();
    let summary = set.summary(late_after).await.unwrap();
    assert_eq!((summary.total, summary.active, summary.late), (3, 2, 0));
    assert_eq!(summary.paused(), 1);
    // The first due: `a`, at the next minute.
    assert_eq!(summary.next, Some(("a".to_owned(), at(1_793_613_660))));
    // Ten minutes later without a poller: `a` is overdue, `b` (due at 11:00) is not.
    clock.set(at(T0 + 600));
    let summary = set.summary(late_after).await.unwrap();
    assert_eq!(summary.late, 1);
    // Due for less than `late_after`: not yet overdue.
    clock.set(at(1_793_613_660 + 60));
    assert_eq!(set.summary(late_after).await.unwrap().late, 0);
}
