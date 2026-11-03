//! Periodic jobs through real workers (`docs/design/periodic.md` §3.2, §3.3): registration of
//! crontabs, firing on the timetable's ticks (a manual clock crosses hours and months without
//! waiting), catch-up policies, client middleware, history, runs, unique periodic jobs, and the
//! guarantees under contention, crashes, lost replies, restarts and out of memory. Against
//! private `redis-server`s.
#![cfg(all(feature = "test-util", feature = "periodic"))]

mod support;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};

use hench::api::Api;
use hench::periodic::{CatchUp, ManualClock, Periodic, PeriodicSource};
use hench::prelude::*;
use hench::test_util::TestRedis;
use hench::{Hench, Launcher, RetryIn, keys};
use serde_json::{Value, json};
use support::*;

/// 2026-11-02 10:00:30 UTC.
const T0: u64 = 1_793_613_630;

fn at(seconds: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(seconds)
}

/// `(argument, schedule, tick ms, manual, jid)` of an execution.
type Run = (String, String, i64, bool, String);

/// What the jobs of a test saw.
#[derive(Default)]
struct Shared {
    /// Every execution.
    runs: std::sync::Mutex<Vec<Run>>,
    gate: Gate,
    executions: AtomicUsize,
}

impl Shared {
    fn runs(&self) -> Vec<Run> {
        self.runs.lock().unwrap().clone()
    }
}

/// A periodic job behaving as its argument says: `ok`, `fail`, `fail_once`, `gate`.
#[derive(Default)]
struct Tick;

impl Job for Tick {
    const NAME: &'static str = "periodic::Tick";
    type Args = (String,);

    fn options() -> JobOptions {
        JobOptions::new().retry(3)
    }

    fn retry_in(&self, _count: u32, _error: &JobError, _job: &JobPayload) -> RetryIn {
        RetryIn::After(Duration::from_millis(100))
    }

    async fn perform(&self, ctx: &JobContext, (mode,): Self::Args) -> Result<(), JobError> {
        let shared = ctx.data::<Shared>()?;
        let execution = shared.executions.fetch_add(1, Ordering::SeqCst) + 1;
        let tick = ctx.periodic();
        shared.runs.lock().unwrap().push((
            mode.clone(),
            tick.as_ref()
                .map(|tick| tick.name.clone())
                .unwrap_or_default(),
            tick.as_ref()
                .map(|tick| {
                    tick.tick
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as i64
                })
                .unwrap_or_default(),
            tick.as_ref().is_some_and(|tick| tick.manual),
            ctx.jid().to_owned(),
        ));
        match mode.as_str() {
            "ok" => Ok(()),
            "fail" => Err(JobError::msg("boom")),
            "fail_once" if execution == 1 => Err(JobError::msg("boom")),
            "fail_once" => Ok(()),
            "gate" => {
                shared.gate.pass().await;
                Ok(())
            }
            other => Err(JobError::msg(format!("unknown mode {other}"))),
        }
    }
}

struct Worker {
    launcher: Launcher,
    shared: Arc<Shared>,
    errors: Errors,
}

/// A worker with `Tick` registered, configured further by `configure`.
async fn worker(
    redis: &TestRedis,
    configure: impl FnOnce(hench::HenchBuilder) -> hench::HenchBuilder,
) -> Worker {
    init_logging();
    let shared = Arc::new(Shared::default());
    let errors = Errors::default();
    let builder = redis
        .builder()
        .await
        .register(Tick)
        .data_arc(shared.clone())
        .error_handler(errors.handler());
    let hench = configure(builder).build().await.unwrap();
    let launcher = hench.start().await.unwrap();
    Worker {
        launcher,
        shared,
        errors,
    }
}

/// A worker that runs no jobs (its queues are elsewhere): what it enqueues stays in Redis.
async fn poller(
    redis: &TestRedis,
    configure: impl FnOnce(hench::HenchBuilder) -> hench::HenchBuilder,
) -> Worker {
    worker(redis, |builder| configure(builder.queues(["nothing-here"]))).await
}

async fn schedule(pool: &RedisPool, name: &str) -> HashMap<String, String> {
    hgetall(pool, &keys::periodic(name)).await
}

async fn wait_for_field(pool: &RedisPool, name: &str, field: &str, value: &str) {
    wait_for(&format!("{name}.{field} = {value}"), WAIT, || async {
        schedule(pool, name).await.get(field).map(String::as_str) == Some(value)
    })
    .await;
}

/// The jobs in queue `out`, by schedule: their ticks (seconds).
async fn ticks(pool: &RedisPool, queue: &str) -> HashMap<String, Vec<i64>> {
    let mut ticks: HashMap<String, Vec<i64>> = HashMap::new();
    for job in queue_jobs(pool, queue).await {
        let meta = job.periodic.expect("a periodic job");
        ticks.entry(meta.name).or_default().push(meta.tick / 1_000);
    }
    for list in ticks.values_mut() {
        list.sort_unstable();
    }
    ticks
}

fn hourly(name: &str) -> Periodic {
    Periodic::cron(name, "0 * * * *")
        .class("Report", vec![])
        .queue("out")
}

#[tokio::test]
async fn registration_creates_schedules_without_enqueueing_and_keeps_operator_state() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let clock = ManualClock::new(at(T0));
    let crontab = |builder: hench::HenchBuilder, clock: &ManualClock| {
        builder.data(clock.clone()).periodic(hourly("a")).periodic(
            Periodic::cron("b", "30 * * * *")
                .class("Report", vec![])
                .queue("out"),
        )
    };
    let first = poller(&redis, |builder| crontab(builder, &clock)).await;
    wait_for_field(&pool, "a", "next", "1793617200").await;
    // The first ticks are strictly after now (11:00 and 10:30), nothing enqueued yet.
    assert_eq!(schedule(&pool, "b").await["next"], "1793615400");
    assert_eq!(queue_size(&pool, "out").await, 0);
    let record: Option<String> = cmd(
        &pool,
        &redis_cmd!("HGET", keys::PERIODIC_CRONTABS, "default"),
    )
    .await;
    let record: Value = serde_json::from_str(&record.unwrap()).unwrap();
    assert_eq!(record["count"], 2);
    assert_eq!(record["by"], first.launcher.identity());
    // Operators' state: a pause and some counters.
    let api = Api::new(pool.clone()).periodic();
    assert!(api.pause("b").await.unwrap());
    let _: () = cmd(&pool, &redis_cmd!("HSET", keys::periodic("a"), "fired", 7)).await;
    first.launcher.stop().await.unwrap();
    // The same crontab again: nothing changes.
    let second = poller(&redis, |builder| crontab(builder, &clock)).await;
    second.launcher.stop().await.unwrap();
    let a = schedule(&pool, "a").await;
    assert_eq!(
        (a["next"].as_str(), a["fired"].as_str()),
        ("1793617200", "7")
    );
    assert!(schedule(&pool, "b").await.contains_key("paused"));
    // A template change keeps `next`; a timetable change moves it; a schedule removed from the
    // code is deleted with its history; another group's and the API's schedules stay.
    let other = poller(&redis, |builder| {
        builder
            .data(clock.clone())
            .periodic_group("other")
            .periodic(hourly("o"))
    })
    .await;
    wait_for_field(&pool, "o", "group", "other").await;
    other.launcher.stop().await.unwrap();
    api.upsert(&hourly("dynamic")).await.unwrap();
    let _: () = cmd(
        &pool,
        &redis_cmd!("RPUSH", keys::periodic_history("b"), "{}"),
    )
    .await;
    let third = poller(&redis, |builder| {
        builder.data(clock.clone()).periodic(
            Periodic::cron("a", "15 * * * *")
                .class("Report", vec![json!(1)])
                .queue("out"),
        )
    })
    .await;
    wait_for("b deleted", WAIT, || async {
        schedule(&pool, "b").await.is_empty()
    })
    .await;
    third.launcher.stop().await.unwrap();
    assert_eq!(
        schedule(&pool, "a").await["next"],
        "1793614500",
        "10:15 of the new timetable"
    );
    assert_eq!(llen(&pool, &keys::periodic_history("b")).await, 0);
    assert_eq!(schedule(&pool, "o").await["group"], "other");
    assert_eq!(schedule(&pool, "dynamic").await["source"], "api");
    assert!(third.errors.all().is_empty(), "{:?}", third.errors.all());
}

#[tokio::test]
async fn names_used_elsewhere_are_reported_and_left_alone() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let api = Api::new(pool.clone()).periodic();
    api.upsert(&hourly("taken")).await.unwrap();
    let digest = schedule(&pool, "taken").await["digest"].clone();
    let worker = poller(&redis, |builder| {
        builder.periodic(Periodic::cron("taken", "5 * * * *").class("Report", vec![]))
    })
    .await;
    wait_for("the conflict reported", WAIT, || async {
        !worker
            .errors
            .with_context("could not register periodic jobs")
            .is_empty()
    })
    .await;
    worker.launcher.stop().await.unwrap();
    let taken = schedule(&pool, "taken").await;
    assert_eq!(
        (taken["source"].as_str(), taken["digest"].as_str()),
        ("api", digest.as_str())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_registrations_leave_one_crontab_or_the_other() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let first: Vec<String> = (0..20).map(|i| format!("first-{i}")).collect();
    let second: Vec<String> = (0..20).map(|i| format!("second-{i}")).collect();
    for _ in 0..3 {
        let crontab = |names: Vec<String>| {
            move |mut builder: hench::HenchBuilder| {
                for name in &names {
                    builder = builder.periodic(hourly(name));
                }
                builder.periodic_poller(false)
            }
        };
        let (a, b) = tokio::join!(
            poller(&redis, crontab(first.clone())),
            poller(&redis, crontab(second.clone()))
        );
        wait_for("registered", WAIT, || async {
            let members = smembers(&pool, &keys::periodic_group("default")).await;
            members.len() == 20
        })
        .await;
        a.launcher.stop().await.unwrap();
        b.launcher.stop().await.unwrap();
        let mut members = smembers(&pool, &keys::periodic_group("default")).await;
        members.sort();
        let mut expected_first = first.clone();
        expected_first.sort();
        let mut expected_second = second.clone();
        expected_second.sort();
        assert!(
            members == expected_first || members == expected_second,
            "a mix of two crontabs: {members:?}"
        );
        let names: Vec<String> =
            cmd(&pool, &redis_cmd!("ZRANGE", keys::PERIODIC_NAMES, 0, -1)).await;
        let mut names = names;
        names.sort();
        assert_eq!(names, members);
    }
}

#[tokio::test]
async fn lost_schedules_are_registered_again() {
    let mut redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let worker = poller(&redis, |builder| builder.periodic(hourly("nightly"))).await;
    wait_for("registered", WAIT, || async {
        !schedule(&pool, "nightly").await.is_empty()
    })
    .await;
    // FLUSHDB while the worker runs: back within the check interval.
    let _: () = cmd(&pool, &redis_cmd!("FLUSHDB")).await;
    wait_for("registered again", WAIT, || async {
        !schedule(&pool, "nightly").await.is_empty()
    })
    .await;
    // A restart without persistence: the same.
    redis.restart().await;
    wait_for(
        "registered after a restart",
        Duration::from_secs(20),
        || async {
            let pool = redis.pool().await;
            !schedule(&pool, "nightly").await.is_empty()
        },
    )
    .await;
    worker.launcher.stop().await.unwrap();
}

#[tokio::test]
async fn timetables_fire_only_on_their_own_ticks_each_once() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    // 10:00:30; every minute until 10:20:30.
    let clock = ManualClock::new(at(T0));
    let worker = poller(&redis, |builder| {
        builder
            .data(clock.clone())
            .periodic(
                Periodic::cron("two", "*/2 * * * *")
                    .class("Report", vec![])
                    .queue("out"),
            )
            .periodic(
                Periodic::cron("ten", "*/10 * * * *")
                    .class("Report", vec![])
                    .queue("out"),
            )
    })
    .await;
    wait_for_field(&pool, "ten", "next", "1793614200").await;
    for minute in 1..=20 {
        clock.set(at(T0 + minute * 60));
        let expected_two = (minute / 2) as usize;
        let expected_ten = (minute / 10) as usize;
        wait_for(&format!("the ticks of minute {minute}"), WAIT, || async {
            let ticks = ticks(&pool, "out").await;
            ticks.get("two").map_or(0, Vec::len) == expected_two
                && ticks.get("ten").map_or(0, Vec::len) == expected_ten
        })
        .await;
    }
    // A moment later, nothing more.
    tokio::time::sleep(Duration::from_millis(400)).await;
    worker.launcher.stop().await.unwrap();
    let ticks = ticks(&pool, "out").await;
    let start = 1_793_613_600; // 10:00
    assert_eq!(
        ticks["two"],
        (1..=10).map(|k| start + k * 120).collect::<Vec<_>>()
    );
    assert_eq!(ticks["ten"], vec![start + 600, start + 1_200]);
    let two = schedule(&pool, "two").await;
    assert_eq!(two["fired"], "10");
    assert_eq!(two["last_tick"], (start + 1_200).to_string());
    assert!(!two.contains_key("missed"));
}

#[tokio::test]
async fn paused_schedules_never_fire_and_resume_without_backfill() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let clock = ManualClock::new(at(T0));
    let worker = poller(&redis, |builder| {
        builder
            .data(clock.clone())
            .periodic(hourly("h").paused(true))
    })
    .await;
    wait_for("registered", WAIT, || async {
        !schedule(&pool, "h").await.is_empty()
    })
    .await;
    // Three hours pass while it is paused: nothing fires, nothing is missed.
    clock.set(at(T0 + 3 * 3_600));
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(queue_size(&pool, "out").await, 0);
    let api = Api::new(pool.clone()).periodic().with_clock(clock.clone());
    assert!(api.unpause("h").await.unwrap());
    // Resumed at the first tick after now (14:00), no backfill.
    assert_eq!(schedule(&pool, "h").await["next"], "1793628000");
    clock.set(at(T0 + 4 * 3_600));
    wait_for("the first tick after resuming", WAIT, || async {
        queue_size(&pool, "out").await == 1
    })
    .await;
    worker.launcher.stop().await.unwrap();
    assert!(!schedule(&pool, "h").await.contains_key("missed"));
}

#[tokio::test]
async fn jobs_carry_the_template_the_options_and_their_tick() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let clock = ManualClock::new(at(T0));
    let worker = worker(&redis, |builder| {
        builder.data(clock.clone()).periodic(
            Periodic::every("every-minute", Duration::from_secs(60))
                .job::<Tick>(("ok".to_owned(),))
                .retry(false)
                .tags(["periodic"]),
        )
    })
    .await;
    wait_for_field(&pool, "every-minute", "next", "1793613660").await;
    clock.set(at(T0 + 30));
    wait_for("the job ran", WAIT, || async {
        !worker.shared.runs().is_empty()
    })
    .await;
    let runs = worker.shared.runs();
    let (mode, name, tick, manual, jid) = &runs[0];
    assert_eq!(
        (mode.as_str(), name.as_str(), *tick, *manual),
        ("ok", "every-minute", 1_793_613_660_000, false)
    );
    // The template: the job type's queue and options under the schedule's.
    let fields = schedule(&pool, "every-minute").await;
    let template: Value = serde_json::from_str(&fields["job"]).unwrap();
    assert_eq!(template["retry"], false, "the schedule's option wins");
    assert_eq!(template["tags"], json!(["periodic"]));
    assert_eq!(fields["last_jid"], *jid);
    // The run is recorded in the schedule.
    wait_for_field(&pool, "every-minute", "run_status", "ok").await;
    let fields = schedule(&pool, "every-minute").await;
    assert_eq!(fields["run_jid"], *jid);
    assert_eq!(fields["run_tick"], "1793613660000");
    worker.launcher.stop().await.unwrap();
}

/// Client middleware for the middleware test: modifies, halts, schedules or fails jobs by their
/// schedule's name.
struct Middle {
    failures: Arc<AtomicUsize>,
}

#[async_trait]
impl ClientMiddleware for Middle {
    async fn call(
        &self,
        mut job: JobPayload,
        _redis: &RedisPool,
        next: ClientNext<'_>,
    ) -> hench::Result<Option<JobPayload>> {
        let name = job
            .periodic
            .as_ref()
            .map(|meta| meta.name.clone())
            .unwrap_or_default();
        match name.as_str() {
            "modify" => {
                job.extra.insert("tenant".to_owned(), json!("acme"));
            }
            "halt" => return Ok(None),
            "schedule" => job.at = Some(hench::now_secs_f64() + 3_600.0),
            "fail" => {
                self.failures.fetch_add(1, Ordering::SeqCst);
                return Err(hench::Error::Job(JobError::msg("middleware down")));
            }
            "panic" => panic!("middleware bug"),
            _ => {}
        }
        next.run(job).await
    }
}

#[tokio::test]
async fn client_middleware_modifies_halts_schedules_or_fails_ticks() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let clock = ManualClock::new(at(T0));
    let failures = Arc::new(AtomicUsize::new(0));
    let middle = Middle {
        failures: failures.clone(),
    };
    let worker = poller(&redis, |mut builder| {
        builder = builder.data(clock.clone());
        for name in ["modify", "halt", "schedule", "fail", "panic"] {
            builder = builder.periodic(
                Periodic::every(name, Duration::from_secs(60))
                    .class("Report", vec![])
                    .queue("out"),
            );
        }
        builder.client_middleware(middle)
    })
    .await;
    wait_for_field(&pool, "panic", "next", "1793613660").await;
    clock.set(at(T0 + 30));
    wait_for_field(&pool, "halt", "last_result", "halted").await;
    wait_for_field(&pool, "schedule", "last_result", "scheduled").await;
    wait_for_field(&pool, "modify", "last_result", "enqueued").await;
    let jobs = queue_jobs(&pool, "out").await;
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].extra["tenant"], json!("acme"));
    let scheduled = sorted_jobs(&pool, keys::SCHEDULE).await;
    assert_eq!(scheduled.len(), 1);
    assert_eq!(scheduled[0].0.periodic.as_ref().unwrap().name, "schedule");
    // A halted tick is consumed: no job, the tick counted as skipped, a history entry.
    let halt = schedule(&pool, "halt").await;
    assert_eq!(
        (halt["skipped"].as_str(), halt["next"].as_str()),
        ("1", "1793613720")
    );
    let history = Api::new(pool.clone())
        .periodic()
        .history("halt", 10)
        .await
        .unwrap();
    assert_eq!(history[0].result, Some(hench::periodic::FireResult::Halted));
    // Failing and panicking middleware: the error is recorded, the tick waits for its lease, and
    // the failure is reported.
    wait_for("the failures recorded", WAIT, || async {
        schedule(&pool, "fail").await.contains_key("error")
            && schedule(&pool, "panic").await.contains_key("error")
    })
    .await;
    assert!(schedule(&pool, "fail").await["error"].contains("middleware down"));
    assert_eq!(
        schedule(&pool, "fail").await["next"],
        "1793613660",
        "not consumed"
    );
    wait_for("reported", WAIT, || async {
        !worker
            .errors
            .with_context("could not enqueue periodic jobs")
            .is_empty()
    })
    .await;
    // Retried when the lease ends (2 s in tests, on the schedule's clock).
    let before = failures.load(Ordering::SeqCst);
    clock.set(at(T0 + 33));
    wait_for("a retry after the lease", WAIT, || async {
        failures.load(Ordering::SeqCst) > before
    })
    .await;
    worker.launcher.stop().await.unwrap();
}

#[tokio::test]
async fn an_invalid_template_parks_only_its_schedule() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let clock = ManualClock::new(at(T0));
    let api = Api::new(pool.clone()).periodic().with_clock(clock.clone());
    api.upsert(
        &Periodic::every("good", Duration::from_secs(60))
            .class("Report", vec![])
            .queue("out"),
    )
    .await
    .unwrap();
    api.upsert(
        &Periodic::every("bad", Duration::from_secs(60))
            .class("Report", vec![])
            .queue("out"),
    )
    .await
    .unwrap();
    // A template edited by hand that cannot be pushed (a jid in a template).
    let _: () = cmd(
        &pool,
        &redis_cmd!(
            "HSET",
            keys::periodic("bad"),
            "job",
            r#"{"class":"Report","args":[],"jid":"x"}"#
        ),
    )
    .await;
    let worker = poller(&redis, |builder| builder.data(clock.clone())).await;
    clock.set(at(T0 + 30));
    wait_for_field(&pool, "good", "last_result", "enqueued").await;
    wait_for("bad parked", WAIT, || async {
        schedule(&pool, "bad").await.contains_key("error")
    })
    .await;
    assert!(schedule(&pool, "bad").await["error"].contains("jid"));
    assert_eq!(queue_size(&pool, "out").await, 1);
    wait_for("reported", WAIT, || async {
        !worker
            .errors
            .with_context("periodic job is invalid")
            .is_empty()
    })
    .await;
    worker.launcher.stop().await.unwrap();
}

#[tokio::test]
async fn histories_are_capped() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let clock = ManualClock::new(at(T0));
    let worker = poller(&redis, |builder| {
        builder.data(clock.clone()).periodic_history(2).periodic(
            Periodic::every("m", Duration::from_secs(60))
                .class("Report", vec![])
                .queue("out"),
        )
    })
    .await;
    wait_for_field(&pool, "m", "next", "1793613660").await;
    for minute in 1..=4 {
        clock.set(at(T0 + minute * 60));
        wait_for("fired", WAIT, || async {
            queue_size(&pool, "out").await == minute as usize
        })
        .await;
    }
    worker.launcher.stop().await.unwrap();
    let history = Api::new(pool.clone())
        .periodic()
        .history("m", 10)
        .await
        .unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(
        history[0].tick,
        Some(at(1_793_613_840)),
        "newest first: {history:?}"
    );
    // No history kept at all.
    let worker = poller(&redis, |builder| {
        builder.data(clock.clone()).periodic_history(0).periodic(
            Periodic::every("m", Duration::from_secs(60))
                .class("Report", vec![])
                .queue("out"),
        )
    })
    .await;
    clock.set(at(T0 + 5 * 60));
    wait_for("fired", WAIT, || async {
        queue_size(&pool, "out").await == 5
    })
    .await;
    worker.launcher.stop().await.unwrap();
    assert_eq!(llen(&pool, &keys::periodic_history("m")).await, 0);
}

#[tokio::test]
async fn enqueue_now_pushes_a_manual_job_and_keeps_the_timetable() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let worker = worker(&redis, |builder| {
        builder.periodic(Periodic::cron("nightly", "0 2 * * *").job::<Tick>(("ok".to_owned(),)))
    })
    .await;
    wait_for("registered", WAIT, || async {
        !schedule(&pool, "nightly").await.is_empty()
    })
    .await;
    let next = schedule(&pool, "nightly").await["next"].clone();
    let api = Api::new(pool.clone()).periodic();
    let jid = api.enqueue("nightly").await.unwrap().unwrap();
    wait_for("the manual job ran", WAIT, || async {
        !worker.shared.runs().is_empty()
    })
    .await;
    let (_, name, _, manual, ran) = worker.shared.runs()[0].clone();
    assert_eq!((name.as_str(), manual, ran), ("nightly", true, jid.clone()));
    let fields = schedule(&pool, "nightly").await;
    assert_eq!(fields["next"], next, "the timetable is untouched");
    assert_eq!(fields["manual_jid"], jid);
    let history = api.history("nightly", 10).await.unwrap();
    assert_eq!(history[0].result, Some(hench::periodic::FireResult::Manual));
    assert_eq!(history[0].tick, None);
    assert!(matches!(
        api.enqueue("missing").await,
        Err(hench::Error::InvalidSchedule(_))
    ));
    worker.launcher.stop().await.unwrap();
}

#[tokio::test]
async fn failures_are_recorded_and_retries_keep_the_tick() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let clock = ManualClock::new(at(T0));
    let worker = worker(&redis, |builder| {
        builder.data(clock.clone()).periodic(
            Periodic::every("flaky", Duration::from_secs(60))
                .job::<Tick>(("fail_once".to_owned(),)),
        )
    })
    .await;
    wait_for_field(&pool, "flaky", "next", "1793613660").await;
    clock.set(at(T0 + 30));
    wait_for("the retry ran", WAIT, || async {
        worker.shared.runs().len() == 2
    })
    .await;
    let runs = worker.shared.runs();
    // The retry carries the same job, hence the same tick.
    assert_eq!(runs[0].2, runs[1].2);
    assert_eq!(runs[0].4, runs[1].4);
    wait_for_field(&pool, "flaky", "run_status", "ok").await;
    let fields = schedule(&pool, "flaky").await;
    assert_eq!(fields["run_failures"], "1");
    // Later ticks keep firing.
    clock.set(at(T0 + 90));
    wait_for("the next tick ran", WAIT, || async {
        worker.shared.runs().len() == 3
    })
    .await;
    worker.launcher.stop().await.unwrap();
}

#[tokio::test]
async fn catch_up_policies_after_three_hours_down() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let clock = ManualClock::new(at(T0));
    let api = Api::new(pool.clone()).periodic().with_clock(clock.clone());
    for (name, policy) in [
        ("latest", CatchUp::Latest),
        ("skip", CatchUp::Skip),
        ("all", CatchUp::All { max: 24 }),
        ("two", CatchUp::All { max: 2 }),
    ] {
        api.upsert(&hourly(name).catch_up(policy)).await.unwrap();
    }
    // Nobody polls from 10:00:30 to 13:20; then a poller starts.
    clock.set(at(T0 + 3 * 3_600 + 19 * 60 + 30));
    let worker = poller(&redis, |builder| builder.data(clock.clone())).await;
    wait_for("caught up", WAIT, || async {
        let ticks = ticks(&pool, "out").await;
        ticks.get("latest").map_or(0, Vec::len) == 1
            && ticks.get("all").map_or(0, Vec::len) == 3
            && ticks.get("two").map_or(0, Vec::len) == 2
            && schedule(&pool, "skip")
                .await
                .get("missed")
                .map(String::as_str)
                == Some("3")
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    worker.launcher.stop().await.unwrap();
    let ticks = ticks(&pool, "out").await;
    let eleven = 1_793_617_200;
    assert_eq!(ticks["latest"], vec![eleven + 2 * 3_600]);
    assert_eq!(schedule(&pool, "latest").await["missed"], "2");
    assert!(
        !ticks.contains_key("skip"),
        "13:00 is 20 minutes late, past Skip's deadline"
    );
    assert_eq!(ticks["all"], vec![eleven, eleven + 3_600, eleven + 7_200]);
    assert_eq!(ticks["two"], vec![eleven + 3_600, eleven + 7_200]);
    assert_eq!(schedule(&pool, "two").await["missed"], "1");
    for name in ["latest", "skip", "all", "two"] {
        assert_eq!(
            schedule(&pool, name).await["next"],
            "1793628000",
            "{name}: 14:00"
        );
    }
}

#[tokio::test]
async fn rare_timetables_fire_on_the_right_days_across_a_year() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    // 2026-01-01 00:00:30 UTC.
    let start = 1_767_225_630;
    let clock = ManualClock::new(at(start));
    let last_day = Periodic::cron("last-day", "0 0 L * *")
        .class("Report", vec![])
        .queue("out");
    let last_friday = Periodic::cron("last-friday", "0 9 * * 5#L")
        .class("Report", vec![])
        .queue("out");
    let seventh = Periodic::cron("seventh", "25 9 7 * *")
        .class("Report", vec![])
        .queue("out");
    let mut expected: Vec<(String, u64)> = Vec::new();
    for periodic in [&last_day, &last_friday, &seventh] {
        for tick in periodic.upcoming(at(start), 12).unwrap() {
            let tick = tick
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            expected.push((periodic.name().to_owned(), tick));
        }
    }
    expected.sort_by_key(|(_, tick)| *tick);
    let worker = poller(&redis, |builder| {
        builder
            .data(clock.clone())
            .periodic(last_day.clone())
            .periodic(last_friday.clone())
            .periodic(seventh.clone())
    })
    .await;
    wait_for("registered", WAIT, || async {
        !schedule(&pool, "seventh").await.is_empty()
    })
    .await;
    for (count, (_, tick)) in expected.iter().enumerate() {
        clock.set(at(*tick + 1));
        wait_for("the tick", WAIT, || async {
            queue_size(&pool, "out").await == count + 1
        })
        .await;
    }
    worker.launcher.stop().await.unwrap();
    let ticks = ticks(&pool, "out").await;
    let days =
        |name: &str| -> Vec<String> { ticks[name].iter().map(|tick| jiff_day(*tick)).collect() };
    assert_eq!(
        days("last-day"),
        [
            "2026-01-31",
            "2026-02-28",
            "2026-03-31",
            "2026-04-30",
            "2026-05-31",
            "2026-06-30",
            "2026-07-31",
            "2026-08-31",
            "2026-09-30",
            "2026-10-31",
            "2026-11-30",
            "2026-12-31"
        ]
    );
    assert_eq!(
        days("last-friday")[..3],
        ["2026-01-30", "2026-02-27", "2026-03-27"]
    );
    assert!(days("seventh").iter().all(|day| day.ends_with("-07")));
}

/// The UTC day of an instant, `YYYY-MM-DD` (civil-from-days, Howard Hinnant's algorithm).
fn jiff_day(seconds: i64) -> String {
    let days = seconds.div_euclid(86_400);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}")
}

#[tokio::test]
async fn quiet_processes_keep_firing_and_a_shutdown_deletes_nothing() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let clock = ManualClock::new(at(T0));
    let worker = poller(&redis, |builder| {
        builder.data(clock.clone()).periodic(
            Periodic::every("m", Duration::from_secs(60))
                .class("Report", vec![])
                .queue("out"),
        )
    })
    .await;
    wait_for_field(&pool, "m", "next", "1793613660").await;
    worker.launcher.quiet().await;
    clock.set(at(T0 + 30));
    wait_for("fired while quiet", WAIT, || async {
        queue_size(&pool, "out").await == 1
    })
    .await;
    worker.launcher.stop().await.unwrap();
    clock.set(at(T0 + 90));
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        queue_size(&pool, "out").await,
        1,
        "a stopped process fires nothing"
    );
    let fields = schedule(&pool, "m").await;
    assert_eq!(fields["next"], "1793613720");
    assert!(
        !smembers(&pool, &keys::periodic_group("default"))
            .await
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_due_schedules_are_enqueued_quickly_and_once_by_several_processes() {
    const SCHEDULES: usize = 2_000;
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let clock = ManualClock::new(at(T0));
    // One registration of 2 000 schedules (a crontab), then four pollers once they are due.
    let registrar = poller(&redis, |mut builder| {
        builder = builder.data(clock.clone()).periodic_poller(false);
        for i in 0..SCHEDULES {
            builder = builder.periodic(
                Periodic::every(format!("s{i}"), Duration::from_secs(60))
                    .class("Report", vec![json!(i)])
                    .queue("out"),
            );
        }
        builder
    })
    .await;
    wait_for("registered", Duration::from_secs(30), || async {
        cmd::<u64>(&pool, &redis_cmd!("ZCARD", keys::PERIODIC_NAMES)).await == SCHEDULES as u64
    })
    .await;
    let mut pollers = Vec::new();
    for _ in 0..4 {
        pollers.push(poller(&redis, |builder| builder.data(clock.clone())).await);
    }
    let started = std::time::Instant::now();
    clock.set(at(T0 + 30));
    wait_for("every tick enqueued", Duration::from_secs(60), || async {
        queue_size(&pool, "out").await >= SCHEDULES
    })
    .await;
    let elapsed = started.elapsed();
    tokio::time::sleep(Duration::from_millis(300)).await;
    for worker in pollers {
        worker.launcher.stop().await.unwrap();
    }
    registrar.launcher.stop().await.unwrap();
    let ticks = ticks(&pool, "out").await;
    assert_eq!(ticks.len(), SCHEDULES);
    assert!(
        ticks.values().all(|ticks| ticks == &vec![1_793_613_660]),
        "a tick enqueued twice"
    );
    assert!(elapsed < Duration::from_secs(30), "{elapsed:?}");
}

/// A unique job that waits at the gate.
#[derive(Default)]
struct Held;

impl Job for Held {
    const NAME: &'static str = "periodic::Held";
    type Args = ();

    async fn perform(&self, ctx: &JobContext, (): Self::Args) -> Result<(), JobError> {
        let shared = ctx.data::<Shared>()?;
        shared.executions.fetch_add(1, Ordering::SeqCst);
        shared.gate.pass().await;
        Ok(())
    }
}

#[tokio::test]
async fn unique_schedules_skip_ticks_while_the_previous_job_holds_its_lock() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let clock = ManualClock::new(at(T0));
    let worker = worker(&redis, |builder| {
        builder.register(Held).data(clock.clone()).periodic(
            Periodic::every("overlap", Duration::from_secs(60))
                .job::<Held>(())
                .unique_for(Duration::from_secs(600)),
        )
    })
    .await;
    wait_for_field(&pool, "overlap", "next", "1793613660").await;
    clock.set(at(T0 + 30));
    wait_for("the first job running", WAIT, || async {
        worker.shared.gate.entered() == 1
    })
    .await;
    // Two more ticks while it runs: duplicates, the lock untouched.
    for minute in [2, 3] {
        clock.set(at(T0 + 30 + (minute - 1) * 60));
        wait_for_field(
            &pool,
            "overlap",
            "next",
            &(1_793_613_600 + (minute + 1) * 60).to_string(),
        )
        .await;
    }
    let fields = schedule(&pool, "overlap").await;
    assert_eq!(fields["last_result"], "duplicate");
    assert_eq!(fields["skipped"], "2");
    assert_eq!(fields["fired"], "1");
    let history = Api::new(pool.clone())
        .periodic()
        .history("overlap", 10)
        .await
        .unwrap();
    assert_eq!(
        history[0].result,
        Some(hench::periodic::FireResult::Duplicate)
    );
    let minutes: Vec<String> = cmd(&pool, &redis_cmd!("KEYS", "u|*")).await;
    let mut duplicates = 0;
    for key in minutes {
        duplicates += hgetall(&pool, &key)
            .await
            .get("periodic::Held|d")
            .and_then(|count| count.parse::<i64>().ok())
            .unwrap_or(0);
    }
    assert_eq!(duplicates, 2);
    // Once the job succeeds, its lock is released and the next tick fires again.
    worker.shared.gate.open();
    wait_for("the lock released", WAIT, || async {
        cmd::<u64>(&pool, &redis_cmd!("ZCARD", keys::UNIQUE_LOCKS)).await == 0
    })
    .await;
    clock.set(at(T0 + 30 + 3 * 60));
    wait_for("the next tick ran", WAIT, || async {
        worker.shared.executions.load(Ordering::SeqCst) == 2
    })
    .await;
    worker.launcher.stop().await.unwrap();
    assert_eq!(schedule(&pool, "overlap").await["fired"], "2");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_second_ticks_are_enqueued_exactly_once_by_competing_processes() {
    const SCHEDULES: usize = 30;
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let api = Api::new(pool.clone()).periodic();
    for i in 0..SCHEDULES {
        api.upsert(
            &Periodic::cron(format!("s{i}"), "* * * * * *")
                .class("Report", vec![])
                .queue("out"),
        )
        .await
        .unwrap();
    }
    let mut pollers = Vec::new();
    for _ in 0..4 {
        pollers.push(poller(&redis, |builder| builder).await);
    }
    tokio::time::sleep(Duration::from_secs(3)).await;
    for worker in pollers {
        worker.launcher.stop().await.unwrap();
    }
    let ticks = ticks(&pool, "out").await;
    assert_eq!(ticks.len(), SCHEDULES);
    for (name, ticks) in &ticks {
        let unique: HashSet<i64> = ticks.iter().copied().collect();
        assert_eq!(unique.len(), ticks.len(), "{name}: a tick enqueued twice");
        // Contiguous: one per second, no gap.
        assert!(
            ticks.windows(2).all(|pair| pair[1] - pair[0] == 1),
            "{name}: a gap in {ticks:?}"
        );
        assert!(ticks.len() >= 2, "{name}: {ticks:?}");
        // Never early: each tick handled at or after its instant (Redis's clock).
        let history = api.history(name, 1).await.unwrap();
        let last = &history[0];
        assert!(last.at >= last.tick.unwrap());
    }
}

/// Client middleware that blocks the ticks of one process at a gate.
struct Blocking(Arc<Gate>);

#[async_trait]
impl ClientMiddleware for Blocking {
    async fn call(
        &self,
        job: JobPayload,
        _redis: &RedisPool,
        next: ClientNext<'_>,
    ) -> hench::Result<Option<JobPayload>> {
        if job.periodic.is_some() {
            self.0.pass().await;
        }
        next.run(job).await
    }
}

#[tokio::test]
async fn a_poller_that_dies_between_lease_and_fire_leaves_its_tick_to_another_after_the_lease() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let clock = ManualClock::new(at(T0));
    let api = Api::new(pool.clone()).periodic().with_clock(clock.clone());
    api.upsert(
        &Periodic::every("m", Duration::from_secs(60))
            .class("Report", vec![])
            .queue("out"),
    )
    .await
    .unwrap();
    let gate = Arc::new(Gate::default());
    let blocked_gate = gate.clone();
    let doomed = poller(&redis, |builder| {
        builder
            .data(clock.clone())
            .client_middleware(Blocking(blocked_gate))
    })
    .await;
    clock.set(at(T0 + 30));
    wait_for("the tick leased and blocked", WAIT, || async {
        gate.entered() == 1
    })
    .await;
    doomed.launcher.abandon();
    let survivor = poller(&redis, |builder| builder.data(clock.clone())).await;
    // Nothing before the lease (2 s on the schedule's clock) ends; then exactly one job.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(queue_size(&pool, "out").await, 0);
    clock.set(at(T0 + 33));
    wait_for("fired after the lease", WAIT, || async {
        queue_size(&pool, "out").await == 1
    })
    .await;
    gate.open();
    tokio::time::sleep(Duration::from_millis(300)).await;
    survivor.launcher.stop().await.unwrap();
    assert_eq!(queue_size(&pool, "out").await, 1);
    assert_eq!(schedule(&pool, "m").await["fired"], "1");
}

/// Client middleware that stalls Redis once it is armed, right before the tick's `FIRE` is
/// sent, so that the `FIRE` times out on the client but runs once Redis resumes.
struct Staller {
    redis: RedisPool,
    armed: Arc<AtomicBool>,
}

#[async_trait]
impl ClientMiddleware for Staller {
    async fn call(
        &self,
        job: JobPayload,
        _redis: &RedisPool,
        next: ClientNext<'_>,
    ) -> hench::Result<Option<JobPayload>> {
        if self.armed.swap(false, Ordering::SeqCst) {
            let stall = self.redis.clone();
            tokio::spawn(async move {
                let mut conn = stall.conn().await.unwrap();
                let _: i64 = hench::redis_rs::cmd("EVAL")
                    .arg(BUSY_LOOP)
                    .arg(0)
                    .arg(1_200)
                    .query_async(&mut conn)
                    .await
                    .unwrap();
            });
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        next.run(job).await
    }
}

/// A `FIRE` whose reply is lost (Redis stalls past the poller's response timeout, then runs it)
/// enqueues its tick once: neither the poller's next attempts nor the score it restores after
/// the timeout enqueue it again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fire_whose_reply_is_lost_enqueues_once() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    allow_long_scripts(&pool).await;
    let clock = ManualClock::new(at(T0));
    let api = Api::new(pool.clone()).periodic().with_clock(clock.clone());
    api.upsert(
        &Periodic::every("m", Duration::from_secs(60))
            .class("Report", vec![])
            .queue("out"),
    )
    .await
    .unwrap();
    let engine_pool =
        RedisPool::from_config(redis.config().response_timeout(Duration::from_millis(300)))
            .await
            .unwrap();
    let stall_pool =
        RedisPool::from_config(redis.config().response_timeout(Duration::from_secs(10)))
            .await
            .unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let staller = Staller {
        redis: stall_pool,
        armed: armed.clone(),
    };
    let worker = poller(&redis, |builder| {
        builder
            .redis(engine_pool)
            .data(clock.clone())
            .client_middleware(staller)
    })
    .await;
    // A first tick, fired as usual, loads `FIRE`'s script: a stalled `FIRE` that Redis then
    // refused for want of it would run nothing, and prove nothing.
    clock.set(at(T0 + 30));
    wait_for("the first tick", WAIT, || async {
        queue_size(&pool, "out").await >= 1
    })
    .await;
    armed.store(true, Ordering::SeqCst);
    clock.set(at(T0 + 90));
    wait_for("the stalled tick", WAIT, || async {
        queue_size(&pool, "out").await >= 2
    })
    .await;
    clock.set(at(T0 + 100));
    tokio::time::sleep(Duration::from_secs(3)).await;
    worker.launcher.stop().await.unwrap();
    assert_eq!(queue_size(&pool, "out").await, 2);
    let fields = schedule(&pool, "m").await;
    assert_eq!(
        (fields["fired"].as_str(), fields["next"].as_str()),
        ("2", "1793613780")
    );
}

#[tokio::test]
async fn out_of_memory_defers_a_tick_until_memory_frees() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let clock = ManualClock::new(at(T0));
    let api = Api::new(pool.clone()).periodic().with_clock(clock.clone());
    api.upsert(
        &Periodic::every("m", Duration::from_secs(60))
            .class("Report", vec![])
            .queue("out"),
    )
    .await
    .unwrap();
    let worker = poller(&redis, |builder| builder.data(clock.clone())).await;
    let _: () = cmd(&pool, &redis_cmd!("CONFIG", "SET", "maxmemory", "1")).await;
    clock.set(at(T0 + 30));
    wait_for("the outage reported", WAIT, || async {
        !worker
            .errors
            .with_context("could not enqueue periodic jobs")
            .is_empty()
    })
    .await;
    let fields = schedule(&pool, "m").await;
    assert_eq!(fields["next"], "1793613660");
    assert!(!fields.contains_key("fired"));
    assert_eq!(queue_size(&pool, "out").await, 0);
    let _: () = cmd(&pool, &redis_cmd!("CONFIG", "SET", "maxmemory", "0")).await;
    wait_for(
        "fired once memory freed",
        Duration::from_secs(15),
        || async { queue_size(&pool, "out").await == 1 },
    )
    .await;
    worker.launcher.stop().await.unwrap();
    assert_eq!(schedule(&pool, "m").await["fired"], "1");
}

#[tokio::test]
async fn definitions_of_a_newer_version_are_left_to_newer_processes() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let clock = ManualClock::new(at(T0));
    let api = Api::new(pool.clone()).periodic().with_clock(clock.clone());
    for name in ["newer", "plain"] {
        api.upsert(
            &Periodic::every(name, Duration::from_secs(60))
                .class("Report", vec![])
                .queue("out"),
        )
        .await
        .unwrap();
    }
    let _: () = cmd(
        &pool,
        &redis_cmd!(
            "HSET",
            keys::periodic("newer"),
            "spec",
            r#"{"v":2,"rrule":"FREQ=DAILY"}"#
        ),
    )
    .await;
    let worker = poller(&redis, |builder| builder.data(clock.clone())).await;
    clock.set(at(T0 + 30));
    wait_for_field(&pool, "plain", "last_result", "enqueued").await;
    wait_for("the newer one noted", WAIT, || async {
        schedule(&pool, "newer").await.contains_key("error")
    })
    .await;
    let newer = schedule(&pool, "newer").await;
    assert!(newer["error"].contains("version 2"), "{}", newer["error"]);
    // Left due for others (its score restored), not fired, not stolen for a lease.
    let score: Option<f64> = cmd(&pool, &redis_cmd!("ZSCORE", keys::PERIODIC, "newer")).await;
    assert_eq!(score, Some(1_793_613_660.0));
    // The entry still lists, with the reason.
    let entry = api.get("newer").await.unwrap().unwrap();
    assert!(entry.timetable.is_none());
    assert!(entry.error.unwrap().contains("version 2"));
    assert_eq!(entry.source, PeriodicSource::Api);
    worker.launcher.stop().await.unwrap();
    assert_eq!(ticks(&pool, "out").await.get("newer"), None);
}

/// Client middleware slower than the lease.
struct Slow;

#[async_trait]
impl ClientMiddleware for Slow {
    async fn call(
        &self,
        job: JobPayload,
        _redis: &RedisPool,
        next: ClientNext<'_>,
    ) -> hench::Result<Option<JobPayload>> {
        if job.periodic.is_some() {
            tokio::time::sleep(Duration::from_millis(2_500)).await;
        }
        next.run(job).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn middleware_slower_than_the_lease_still_enqueues_once() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let clock = ManualClock::new(at(T0));
    let api = Api::new(pool.clone()).periodic().with_clock(clock.clone());
    api.upsert(
        &Periodic::every("m", Duration::from_secs(60))
            .class("Report", vec![])
            .queue("out"),
    )
    .await
    .unwrap();
    let a = poller(&redis, |builder| {
        builder.data(clock.clone()).client_middleware(Slow)
    })
    .await;
    let b = poller(&redis, |builder| {
        builder.data(clock.clone()).client_middleware(Slow)
    })
    .await;
    clock.set(at(T0 + 30));
    wait_for("fired", Duration::from_secs(15), || async {
        queue_size(&pool, "out").await == 1
    })
    .await;
    // Long enough for the second poller (after the lease) to finish its preparation too.
    tokio::time::sleep(Duration::from_secs(4)).await;
    a.launcher.stop().await.unwrap();
    b.launcher.stop().await.unwrap();
    assert_eq!(queue_size(&pool, "out").await, 1);
    assert_eq!(schedule(&pool, "m").await["fired"], "1");
}

#[tokio::test]
async fn processes_without_the_poller_register_but_fire_nothing() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let clock = ManualClock::new(at(T0));
    let worker = poller(&redis, |builder| {
        builder.data(clock.clone()).periodic_poller(false).periodic(
            Periodic::every("m", Duration::from_secs(60))
                .class("Report", vec![])
                .queue("out"),
        )
    })
    .await;
    wait_for_field(&pool, "m", "next", "1793613660").await;
    clock.set(at(T0 + 30));
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(queue_size(&pool, "out").await, 0);
    worker.launcher.stop().await.unwrap();
    // The registered crontab is attributed to this group and digest.
    let groups = Api::new(pool.clone()).periodic().groups().await.unwrap();
    assert_eq!(groups.len(), 1);
    assert_eq!((groups[0].group.as_str(), groups[0].count), ("default", 1));
}

#[tokio::test]
async fn an_unregistered_job_class_is_only_a_warning() {
    let redis = TestRedis::start().await;
    // Builds and starts: the class may be another service's job.
    let worker = poller(&redis, |builder| {
        builder.periodic(
            Periodic::every("other", Duration::from_secs(60)).class("billing::Close", vec![]),
        )
    })
    .await;
    worker.launcher.stop().await.unwrap();
    assert!(worker.errors.all().is_empty());
}

#[tokio::test]
async fn builds_refuse_invalid_crontabs() {
    let error = Hench::builder()
        .periodic(Periodic::cron("broken", "61 * * * *").class("X", vec![]))
        .build()
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("periodic job `broken`"),
        "{error}"
    );
}

/// The scripts (`EVALSHA`) Redis served so far.
async fn script_calls(pool: &RedisPool) -> u64 {
    let info: String = cmd(pool, &redis_cmd!("INFO", "commandstats")).await;
    info.lines()
        .find(|line| line.starts_with("cmdstat_evalsha:"))
        .and_then(|line| line.split("calls=").nth(1))
        .and_then(|rest| rest.split(',').next())
        .and_then(|calls| calls.parse().ok())
        .unwrap_or(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn many_newer_definitions_neither_stall_nor_spin_the_poller() {
    // periodic-2: the skip list sent to `LEASE` held at most 100 names. With more due
    // definitions of a newer format, the oldest skipped one was leased again at once, its past
    // score restored, and so on forever: the poller spun at thousands of scripts a second and
    // fired nothing else.
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let clock = ManualClock::new(at(T0));
    let api = Api::new(pool.clone()).periodic().with_clock(clock.clone());
    for n in 0..150 {
        let name = format!("v2-{n:03}");
        api.upsert(
            &Periodic::every(&name, Duration::from_secs(60))
                .class("Report", vec![])
                .queue("out"),
        )
        .await
        .unwrap();
        let _: () = cmd(
            &pool,
            &redis_cmd!(
                "HSET",
                keys::periodic(&name),
                "spec",
                r#"{"v":2,"every":60,"offset":0}"#
            ),
        )
        .await;
    }
    api.upsert(
        &Periodic::every("zz-normal", Duration::from_secs(60))
            .class("Report", vec![])
            .queue("out"),
    )
    .await
    .unwrap();
    let worker = poller(&redis, |builder| builder.data(clock.clone())).await;
    clock.set(at(T0 + 30));
    wait_for_field(&pool, "zz-normal", "last_result", "enqueued").await;
    // Idle once every newer definition is skipped.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let before = script_calls(&pool).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let calls = script_calls(&pool).await - before;
    assert!(calls < 500, "{calls} scripts in a second");
    // Each is left due, unfired, for the processes that can read it.
    for n in [0, 99, 100, 149] {
        let score: Option<f64> = cmd(
            &pool,
            &redis_cmd!("ZSCORE", keys::PERIODIC, format!("v2-{n:03}")),
        )
        .await;
        assert_eq!(score, Some(1_793_613_660.0), "v2-{n:03}");
    }
    worker.launcher.stop().await.unwrap();
    assert_eq!(queue_size(&pool, "out").await, 1);
}

#[tokio::test]
async fn a_zone_this_process_does_not_know_is_left_to_the_processes_that_do() {
    // periodic-3: a zone missing from this process's time-zone database (a host whose tzdata is
    // older than the writer's) parked the schedule for an hour, for every process.
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let clock = ManualClock::new(at(T0));
    let api = Api::new(pool.clone()).periodic().with_clock(clock.clone());
    api.upsert(
        &Periodic::cron("zoned", "* * * * *")
            .tz("Europe/Paris")
            .class("Report", vec![])
            .queue("out"),
    )
    .await
    .unwrap();
    api.upsert(
        &Periodic::every("plain", Duration::from_secs(60))
            .class("Report", vec![])
            .queue("out"),
    )
    .await
    .unwrap();
    // A newer host wrote a zone this one does not know.
    let _: () = cmd(
        &pool,
        &redis_cmd!(
            "HSET",
            keys::periodic("zoned"),
            "spec",
            r#"{"v":1,"cron":"* * * * *","tz":"America/Brand_New_Zone"}"#
        ),
    )
    .await;
    let worker = poller(&redis, |builder| builder.data(clock.clone())).await;
    clock.set(at(T0 + 31));
    wait_for_field(&pool, "plain", "last_result", "enqueued").await;
    wait_for("the zone noted", WAIT, || async {
        schedule(&pool, "zoned").await.contains_key("error")
    })
    .await;
    let zoned = schedule(&pool, "zoned").await;
    assert!(
        zoned["error"].contains("America/Brand_New_Zone"),
        "{}",
        zoned["error"]
    );
    // Left due for the processes that know the zone, not parked for an hour.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let score: Option<f64> = cmd(&pool, &redis_cmd!("ZSCORE", keys::PERIODIC, "zoned")).await;
    assert_eq!(score, Some(1_793_613_660.0));
    worker.launcher.stop().await.unwrap();
    assert_eq!(ticks(&pool, "out").await.get("zoned"), None);
    // Not reported as invalid: the processes that know the zone fire it.
    assert!(
        worker
            .errors
            .with_context("periodic job is invalid")
            .is_empty()
    );
}

/// The history of schedule `name`, newest first.
async fn history(pool: &RedisPool, name: &str) -> Vec<Value> {
    let entries: Vec<String> = cmd(
        pool,
        &redis_cmd!("LRANGE", keys::periodic_history(name), 0, -1),
    )
    .await;
    entries
        .iter()
        .map(|entry| serde_json::from_str(entry).unwrap())
        .collect()
}

#[tokio::test]
async fn a_next_tick_computed_under_other_zone_rules_is_not_dropped() {
    // periodic-4: a stored `next` that is not a tick of this process's timetable — computed by
    // a process with other time-zone rules (older or newer tzdata, rules that changed, another
    // version's DST rules) — was dropped: no job, no missed count, no history entry.
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    // 2026-11-03 09:00 UTC, the tick of `0 9 * * *` this process computes.
    const NINE: i64 = 1_793_696_400;
    let clock = ManualClock::new(at(T0));
    let api = Api::new(pool.clone()).periodic().with_clock(clock.clone());
    for name in ["later", "handled", "earlier"] {
        api.upsert(
            &Periodic::cron(name, "0 9 * * *")
                .class("Report", vec![json!(name)])
                .queue("out"),
        )
        .await
        .unwrap();
        assert_eq!(schedule(&pool, name).await["next"], NINE.to_string());
    }
    // Another process's rules put the tick an hour later: 10:00.
    for name in ["later", "handled"] {
        let ten = (NINE + 3_600).to_string();
        let _: () = cmd(
            &pool,
            &redis_cmd!("HSET", keys::periodic(name), "next", &ten),
        )
        .await;
        let _: () = cmd(&pool, &redis_cmd!("ZADD", keys::PERIODIC, &ten, name)).await;
    }
    // ... and `handled` already ran today's tick (09:00), under this process's rules.
    let _: () = cmd(
        &pool,
        &redis_cmd!(
            "HSET",
            keys::periodic("handled"),
            "last_tick",
            NINE.to_string()
        ),
    )
    .await;
    // Rules that put it an hour earlier: 08:00.
    let eight = (NINE - 3_600).to_string();
    let _: () = cmd(
        &pool,
        &redis_cmd!("HSET", keys::periodic("earlier"), "next", &eight),
    )
    .await;
    let _: () = cmd(
        &pool,
        &redis_cmd!("ZADD", keys::PERIODIC, &eight, "earlier"),
    )
    .await;
    let worker = poller(&redis, |builder| builder.data(clock.clone())).await;
    // 08:00:01: `earlier` runs in place of today's 09:00, which it stands for.
    clock.set(at(NINE as u64 - 3_599));
    wait_for_field(&pool, "earlier", "last_tick", &eight).await;
    assert_eq!(
        schedule(&pool, "earlier").await["next"],
        (NINE + 86_400).to_string()
    );
    // 10:00:01: `later` runs (in place of 09:00, which nobody ran); `handled` does not run
    // twice.
    clock.set(at(NINE as u64 + 3_601));
    wait_for_field(&pool, "later", "last_tick", &(NINE + 3_600).to_string()).await;
    wait_for_field(&pool, "handled", "next", &(NINE + 86_400).to_string()).await;
    worker.launcher.stop().await.unwrap();
    let by_schedule = ticks(&pool, "out").await;
    assert_eq!(by_schedule.get("earlier"), Some(&vec![NINE - 3_600]));
    assert_eq!(by_schedule.get("later"), Some(&vec![NINE + 3_600]));
    assert_eq!(by_schedule.get("handled"), None);
    assert_eq!(
        schedule(&pool, "later").await["next"],
        (NINE + 86_400).to_string()
    );
    // Each one left a trace in its history.
    assert_eq!(history(&pool, "later").await[0]["result"], "enqueued");
    let handled = history(&pool, "handled").await;
    assert_eq!(handled[0]["result"], "skipped", "{handled:?}");
}

/// Every key of the database, dumped.
async fn backup(pool: &RedisPool) -> Vec<(String, Vec<u8>, i64)> {
    let names: Vec<String> = cmd(pool, &redis_cmd!("KEYS", "*")).await;
    let mut dumps = Vec::new();
    for name in names {
        let dump: Option<Vec<u8>> = cmd(pool, &redis_cmd!("DUMP", &name)).await;
        let ttl: i64 = cmd(pool, &redis_cmd!("PTTL", &name)).await;
        if let Some(dump) = dump {
            dumps.push((name, dump, ttl.max(0)));
        }
    }
    dumps
}

/// The crontab digest the group `default` holds.
async fn registered_digest(pool: &RedisPool) -> String {
    let record: Option<String> = cmd(
        pool,
        &redis_cmd!("HGET", keys::PERIODIC_CRONTABS, "default"),
    )
    .await;
    let record: Value = serde_json::from_str(&record.unwrap()).unwrap();
    record["digest"].as_str().unwrap().to_owned()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_crontab_lost_to_a_restore_of_older_data_is_registered_again() {
    // periodic-5: after Redis went back in time (a restore from a backup, a failover to a
    // replica that missed the latest registration), the registration check found the group's
    // record with another digest, took it for a newer registration and did nothing: removed
    // schedules came back and new ones were missing until a process restarted.
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    // Deploy A, {a1, a2}: identical templates, so that only the names tell the crontabs apart.
    let a = poller(&redis, |builder| {
        builder.periodic(hourly("a1")).periodic(hourly("a2"))
    })
    .await;
    wait_for("A registered", WAIT, || async {
        exists(&pool, &keys::periodic("a2")).await
    })
    .await;
    a.launcher.stop().await.unwrap();
    let digest_a = registered_digest(&pool).await;
    let dumps = backup(&pool).await;
    // Deploy B, {a1, b1}, keeps running.
    let b = poller(&redis, |builder| {
        builder.periodic(hourly("a1")).periodic(hourly("b1"))
    })
    .await;
    wait_for("B registered", WAIT, || async {
        exists(&pool, &keys::periodic("b1")).await && !exists(&pool, &keys::periodic("a2")).await
    })
    .await;
    let digest_b = registered_digest(&pool).await;
    assert_ne!(digest_a, digest_b, "crontabs differing by names only");
    // Redis is restored to the backup taken under deploy A.
    let _: () = cmd(&pool, &redis_cmd!("FLUSHALL")).await;
    for (name, dump, ttl) in &dumps {
        let _: () = cmd(
            &pool,
            &redis_cmd!("RESTORE", name, *ttl, dump.as_slice(), "REPLACE"),
        )
        .await;
    }
    wait_for("B registered again", WAIT, || async {
        exists(&pool, &keys::periodic("b1")).await && !exists(&pool, &keys::periodic("a2")).await
    })
    .await;
    assert_eq!(registered_digest(&pool).await, digest_b);
    b.launcher.stop().await.unwrap();
}
