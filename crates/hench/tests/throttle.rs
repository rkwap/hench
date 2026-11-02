//! Throttles through real workers (`docs/design/ratelimit.md` §3.4.7–14, §3.5): the gate
//! before any middleware, parked jobs coming back at their slots, permits held by the engine
//! (renewed by the heartbeat, released after every outcome, freed after a crash), and the gate's
//! failures.

mod support;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use hench::limit::{Limit, OverLimit, Throttle};
use hench::prelude::*;
use hench::test_util::TestRedis;
use hench::{Hench, HenchBuilder, Launcher, keys};
use serde_json::{Value, json};
use support::*;

/// When each execution of a throttled job started and ended, with its key.
#[derive(Default)]
struct Executions(Mutex<Vec<(String, Instant, Option<Instant>)>>);

impl Executions {
    fn start(&self, key: &str) -> usize {
        let mut log = self.0.lock().unwrap();
        log.push((key.to_owned(), Instant::now(), None));
        log.len() - 1
    }

    fn end(&self, index: usize) {
        self.0.lock().unwrap()[index].2 = Some(Instant::now());
    }

    fn finished(&self) -> usize {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, _, end)| end.is_some())
            .count()
    }

    /// Most executions running at once.
    fn max_overlap(&self) -> usize {
        self.max_overlap_where(|_| true)
    }

    /// Most executions of `key` running at once.
    fn max_overlap_of(&self, key: &str) -> usize {
        self.max_overlap_where(|k| k == key)
    }

    /// Most executions whose key `matches` running at once.
    fn max_overlap_where(&self, matches: impl Fn(&str) -> bool) -> usize {
        let log = self.0.lock().unwrap();
        let mut events: Vec<(Instant, i32)> = Vec::new();
        for (_, start, end) in log.iter().filter(|(key, _, _)| matches(key)) {
            events.push((*start, 1));
            events.push((end.unwrap_or_else(Instant::now), -1));
        }
        // Ends before starts at the same instant.
        events.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
        let mut running = 0;
        let mut most = 0;
        for (_, change) in events {
            running += change;
            most = most.max(running);
        }
        usize::try_from(most).unwrap()
    }

    /// The start times of the executions of `key`, in order.
    fn starts(&self, key: &str) -> Vec<Instant> {
        let mut starts: Vec<Instant> = self
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _, _)| k == key)
            .map(|(_, start, _)| *start)
            .collect();
        starts.sort();
        starts
    }
}

/// A job throttled by `slow`, keyed by its first argument; it runs for its second argument in
/// milliseconds, or does what its third argument says.
struct Slow;

impl Job for Slow {
    const NAME: &'static str = "throttle::Slow";
    type Args = (String, u64, String);

    fn options() -> JobOptions {
        JobOptions::new().throttle("slow")
    }

    fn throttle_key(args: &Self::Args) -> Option<String> {
        Some(args.0.clone())
    }

    async fn perform(
        &self,
        ctx: &JobContext,
        (key, millis, mode): Self::Args,
    ) -> Result<(), JobError> {
        let log = ctx.data::<Executions>()?;
        let index = log.start(&key);
        match mode.as_str() {
            "hang" => ctx.data::<Gate>()?.pass().await,
            "ignore-shutdown" => tokio::time::sleep(Duration::from_secs(30)).await,
            _ => tokio::time::sleep(Duration::from_millis(millis)).await,
        }
        log.end(index);
        match mode.as_str() {
            "fail" => Err(JobError::msg("failed on purpose")),
            "interrupt"
                if ctx.retry_count().is_none() && ctx.payload().extra.get("again").is_none() =>
            {
                Err(JobError::interrupted())
            }
            "over" => Err(JobError::over_limit(OverLimit::new("w:inner"))),
            _ => Ok(()),
        }
    }
}

/// A plain job, recording when it ran.
struct Plain;

impl Job for Plain {
    const NAME: &'static str = "throttle::Plain";
    type Args = (u64,);

    async fn perform(&self, ctx: &JobContext, (n,): Self::Args) -> Result<(), JobError> {
        ctx.data::<Runs>()?.record(format!("plain {n}"));
        Ok(())
    }
}

struct Started {
    hench: Hench,
    launcher: Launcher,
    log: Arc<Executions>,
    runs: Arc<Runs>,
    gate: Arc<Gate>,
}

async fn start(
    redis: &TestRedis,
    concurrency: usize,
    throttle: Throttle,
    configure: impl FnOnce(HenchBuilder) -> HenchBuilder,
) -> Started {
    init_logging();
    let log = Arc::new(Executions::default());
    let runs = Arc::new(Runs::default());
    let gate = Arc::new(Gate::default());
    let builder = redis
        .builder()
        .await
        .concurrency(concurrency)
        .register(Slow)
        .register(Plain)
        .throttle(throttle)
        .data_arc(log.clone())
        .data_arc(runs.clone())
        .data_arc(gate.clone());
    let hench = configure(builder).build().await.unwrap();
    let launcher = hench.start().await.unwrap();
    Started {
        hench,
        launcher,
        log,
        runs,
        gate,
    }
}

async fn push(client: &Client, key: &str, millis: u64, mode: &str) -> String {
    Slow::perform_async(client, (key.to_owned(), millis, mode.to_owned()))
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_concurrency_throttle_parks_what_it_cannot_run() {
    let redis = TestRedis::start().await;
    let started = start(&redis, 6, Throttle::new("slow").concurrency(3), |b| b).await;
    let pool = started.hench.redis().clone();
    let client = started.hench.client().clone();
    for index in 0..12 {
        push(&client, &format!("k{index}"), 150, "").await;
    }
    // Plain jobs in the same queue are not held up by the throttled ones.
    for n in 0..4 {
        Plain::perform_async(&client, (n,)).await.unwrap();
    }
    wait_for("the plain jobs", Duration::from_secs(2), || async {
        started.runs.len() == 4
    })
    .await;
    // Some throttled jobs were parked in the schedule meanwhile.
    wait_for("a parked job", WAIT, || async {
        zcard(&pool, keys::SCHEDULE).await > 0
    })
    .await;
    let parked = sorted_jobs(&pool, keys::SCHEDULE).await;
    for (job, _) in &parked {
        let limited = job.limited.as_ref().unwrap();
        assert_eq!(limited.by, "c:slow");
        assert!(limited.n >= 1);
    }
    wait_for("every throttled job", Duration::from_secs(30), || async {
        started.log.finished() == 12
    })
    .await;
    started.launcher.stop().await.unwrap();
    assert!(
        started.log.max_overlap() <= 3,
        "{}",
        started.log.max_overlap()
    );
    assert_eq!(get_int(&pool, keys::STAT_FAILED).await, 0);
    assert_eq!(zcard(&pool, keys::RETRY).await, 0);
    // Every slot was given back.
    assert_eq!(zcard(&pool, &keys::limit_state('c', "slow")).await, 0);
    // Throttled jobs are counted as throttled (`t`), not processed.
    let processed = get_int(&pool, keys::STAT_PROCESSED).await;
    assert_eq!(
        processed, 16,
        "12 throttled jobs and 4 plain ones, each run once"
    );
    let stats = hgetall(&pool, &keys::limit_stats('c', "slow")).await;
    assert!(stats["r"].parse::<u32>().unwrap() >= 1);
    assert_eq!(stats["g"], "12");
    assert_eq!(stats["h"], "12");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_key_windows_limit_each_key_on_its_own() {
    let redis = TestRedis::start().await;
    let throttle = Throttle::new("slow").window_per_key(2, Duration::from_secs(1));
    let started = start(&redis, 8, throttle, |b| b).await;
    let client = started.hench.client().clone();
    let begin = Instant::now();
    for _ in 0..4 {
        push(&client, "a", 10, "").await;
    }
    for _ in 0..4 {
        push(&client, "b", 10, "").await;
    }
    wait_for("every job", Duration::from_secs(20), || async {
        started.log.finished() == 8
    })
    .await;
    started.launcher.stop().await.unwrap();
    for key in ["a", "b"] {
        let starts = started.log.starts(key);
        assert_eq!(starts.len(), 4);
        // At most 2 starts in any second (a little slack for the two clocks).
        for pair in starts.windows(3) {
            let apart = pair[2].duration_since(pair[0]);
            assert!(apart >= Duration::from_millis(900), "{key}: {apart:?}");
        }
        // `b` was not held up by `a`: its first two ran at once.
        assert!(
            starts[1].duration_since(begin) < Duration::from_millis(800),
            "{key}"
        );
    }
}

#[tokio::test]
async fn permits_are_released_after_every_outcome() {
    let redis = TestRedis::start().await;
    let started = start(&redis, 1, Throttle::new("slow").concurrency(1), |b| b).await;
    let pool = started.hench.redis().clone();
    let client = started.hench.client().clone();
    let state = keys::limit_state('c', "slow");
    // Success, failure (retry), over limit inside (rescheduled), one after the other: each finds
    // the slot free, so no job is parked.
    for mode in ["", "fail", "over"] {
        push(&client, "k", 10, mode).await;
    }
    wait_for("the three executions", WAIT, || async {
        started.log.finished() == 3
    })
    .await;
    wait_for("the outcomes written", WAIT, || async {
        zcard(&pool, keys::RETRY).await == 1 && zcard(&pool, keys::SCHEDULE).await == 1
    })
    .await;
    wait_for("the slot given back", WAIT, || async {
        zcard(&pool, &state).await == 0
    })
    .await;
    started.launcher.stop().await.unwrap();
    let stats = hgetall(&pool, &keys::limit_stats('c', "slow")).await;
    assert_eq!(stats["g"], "3");
    assert_eq!(stats["h"], "3");
    assert!(!stats.contains_key("r"), "{stats:?}");
    // The over-limit job was rescheduled by the inner limiter, not by the throttle.
    let (job, _) = sorted_jobs(&pool, keys::SCHEDULE).await.remove(0);
    assert_eq!(job.limited.unwrap().by, "w:inner");
}

#[tokio::test]
async fn the_heartbeat_renews_the_permits_of_long_jobs() {
    let redis = TestRedis::start().await;
    // A one-second lease, a job running for three of them.
    let throttle = Throttle::new("slow")
        .concurrency(1)
        .lease(Duration::from_secs(1));
    let started = start(&redis, 2, throttle, |b| b).await;
    let pool = started.hench.redis().clone();
    let client = started.hench.client().clone();
    push(&client, "long", 3_200, "").await;
    wait_for("the long job running", WAIT, || async {
        started.log.0.lock().unwrap().len() == 1
    })
    .await;
    // Another job of the throttle cannot take the slot while the first runs: it is parked.
    push(&client, "other", 10, "").await;
    let state = keys::limit_state('c', "slow");
    for _ in 0..12 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        if started.log.finished() == 0 {
            assert_eq!(zcard(&pool, &state).await, 1);
            assert!(pttl(&pool, &state).await > 0);
        }
    }
    wait_for("both jobs", Duration::from_secs(15), || async {
        started.log.finished() == 2
    })
    .await;
    started.launcher.stop().await.unwrap();
    assert!(started.log.max_overlap() <= 1);
    let stats = hgetall(&pool, &keys::limit_stats('c', "slow")).await;
    assert!(!stats.contains_key("ov"), "{stats:?}");
    assert!(!stats.contains_key("rc"), "{stats:?}");
}

#[tokio::test]
async fn a_crashed_process_frees_its_slots_within_a_lease() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let throttle = || {
        Throttle::new("slow")
            .concurrency(2)
            .lease(Duration::from_secs(1))
    };
    let crashed = start(&redis, 2, throttle(), |b| b).await;
    let client = crashed.hench.client().clone();
    push(&client, "x", 0, "hang").await;
    push(&client, "y", 0, "hang").await;
    wait_for("both jobs holding their slots", WAIT, || async {
        crashed.gate.entered() == 2
    })
    .await;
    assert_eq!(zcard(&pool, &keys::limit_state('c', "slow")).await, 2);
    crashed.launcher.abandon();
    del(&pool, crashed.launcher.identity()).await;
    // Nobody renews the leases: within a lease, no slot is live any more.
    let state = keys::limit_state('c', "slow");
    wait_for("the leases ended", Duration::from_secs(3), || async {
        let live: i64 = cmd(
            &pool,
            &redis_cmd!(
                "ZCOUNT",
                &state,
                format!("({}", hench::now_millis()),
                "+inf"
            ),
        )
        .await;
        live == 0
    })
    .await;
    // Another process recovers the jobs; they take their slots again and run.
    let survivor = start(&redis, 2, throttle(), |b| b).await;
    survivor.gate.open();
    wait_for("the recovered jobs", Duration::from_secs(15), || async {
        survivor.log.finished() == 2
    })
    .await;
    survivor.launcher.stop().await.unwrap();
    assert_eq!(zcard(&pool, &state).await, 0);
}

#[tokio::test]
async fn a_hard_shutdown_releases_the_permits_of_interrupted_jobs() {
    let redis = TestRedis::start().await;
    let started = start(&redis, 1, Throttle::new("slow").concurrency(1), |b| {
        b.timeout(Duration::from_millis(300))
    })
    .await;
    let pool = started.hench.redis().clone();
    push(started.hench.client(), "k", 0, "ignore-shutdown").await;
    wait_for("the job running", WAIT, || async {
        started.log.0.lock().unwrap().len() == 1
    })
    .await;
    assert_eq!(zcard(&pool, &keys::limit_state('c', "slow")).await, 1);
    started.launcher.stop().await.unwrap();
    // Requeued, its slot given back.
    assert_eq!(queue_size(&pool, "default").await, 1);
    assert_eq!(zcard(&pool, &keys::limit_state('c', "slow")).await, 0);
}

#[tokio::test]
async fn unknown_throttles_run_unthrottled() {
    let redis = TestRedis::start().await;
    // `slow` is defined (the job type names it), `unknown` is not.
    let started = start(&redis, 2, Throttle::new("slow").concurrency(1), |b| b).await;
    let pool = started.hench.redis().clone();
    // Pushed by another service naming a throttle this worker does not define.
    let mut job = JobPayload::new("throttle::Slow", vec![json!("k"), json!(10), json!("")]);
    job.queue = "default".to_owned();
    job.throttle = Some(hench::limit::ThrottleOption::named("unknown"));
    for _ in 0..3 {
        started.hench.client().push(job.clone()).await.unwrap();
    }
    wait_for("the jobs", WAIT, || async { started.log.finished() == 3 }).await;
    started.launcher.stop().await.unwrap();
    assert!(
        redis.keys_without_ttl(&["limit*"]).await.is_empty()
            && !exists(&pool, &keys::limit_state('c', "unknown")).await
    );
}

#[tokio::test]
async fn a_poisoned_throttle_key_fails_the_job_instead_of_blocking_the_queue() {
    let redis = TestRedis::start().await;
    let started = start(&redis, 1, Throttle::new("slow").concurrency(1), |b| b).await;
    let pool = started.hench.redis().clone();
    // The throttle's state key holds a value of the wrong type.
    let () = cmd(
        &pool,
        &redis_cmd!("SET", keys::limit_state('c', "slow"), "oops"),
    )
    .await;
    push(started.hench.client(), "k", 10, "").await;
    Plain::perform_async(started.hench.client(), (1,))
        .await
        .unwrap();
    wait_for("the throttled job failed", WAIT, || async {
        zcard(&pool, keys::RETRY).await == 1
    })
    .await;
    wait_for("the plain job ran", WAIT, || async {
        started.runs.len() == 1
    })
    .await;
    started.launcher.stop().await.unwrap();
    let (job, _) = sorted_jobs(&pool, keys::RETRY).await.remove(0);
    assert!(job.error_message.unwrap().contains("WRONGTYPE"));
    assert_eq!(started.log.finished(), 0);
}

#[tokio::test]
async fn a_spent_throttle_budget_hands_the_job_to_retries() {
    let redis = TestRedis::start().await;
    let throttle = Throttle::new("slow").concurrency(1).reschedules(Some(1));
    let started = start(&redis, 2, throttle, |b| b).await;
    let pool = started.hench.redis().clone();
    let client = started.hench.client().clone();
    push(&client, "busy", 0, "hang").await;
    wait_for("the slot taken", WAIT, || async {
        started.gate.entered() == 1
    })
    .await;
    // A job already parked once: its next refusal fails it.
    let mut job = JobPayload::new("throttle::Slow", vec![json!("k"), json!(10), json!("")]);
    job.queue = "default".to_owned();
    job.throttle = Some(hench::limit::ThrottleOption::Enabled {
        name: Some("slow".to_owned()),
        key: Some("k".to_owned()),
    });
    let mut limited = hench::limit::Limited::new(1, "c:slow", Some(1));
    limited.throttled = 1;
    job.limited = Some(limited);
    client
        .job::<Slow>()
        .perform_async((String::from("k"), 10, String::new()))
        .await
        .unwrap();
    client.push(job).await.unwrap();
    wait_for("one parked, one failed", WAIT, || async {
        zcard(&pool, keys::SCHEDULE).await == 1 && zcard(&pool, keys::RETRY).await == 1
    })
    .await;
    started.gate.open();
    started.launcher.stop().await.unwrap();
    let (failed, _) = sorted_jobs(&pool, keys::RETRY).await.remove(0);
    assert_eq!(failed.error_class.as_deref(), Some(JobError::OVER_LIMIT));
    let (parked, _) = sorted_jobs(&pool, keys::SCHEDULE).await.remove(0);
    assert_eq!(parked.limited.unwrap().n, 1);
}

#[tokio::test]
async fn processes_advertise_their_throttles() {
    let redis = TestRedis::start().await;
    let throttle = Throttle::new("slow")
        .concurrency(25)
        .limit_per_key(Limit::Window {
            limit: 5,
            period: Duration::from_secs(1),
        });
    let started = start(&redis, 1, throttle, |b| b).await;
    let pool = started.hench.redis().clone();
    let identity = started.launcher.identity().to_owned();
    wait_for("the heartbeat", WAIT, || async {
        exists(&pool, &identity).await
    })
    .await;
    let process = hgetall(&pool, &identity).await;
    let info: Value = serde_json::from_str(&process["info"]).unwrap();
    assert_eq!(info["throttles"]["slow"], json!("c=25;w=5/1000 per key"));
    assert_eq!(info["features"]["limits"], json!(1));
    started.launcher.stop().await.unwrap();
}

#[tokio::test]
async fn other_queues_scheduled_jobs_are_promoted_while_throttled_jobs_wait() {
    let redis = TestRedis::start().await;
    let started = start(&redis, 3, Throttle::new("slow").concurrency(1), |b| b).await;
    let client = started.hench.client().clone();
    push(&client, "busy", 0, "hang").await;
    wait_for("the slot taken", WAIT, || async {
        started.gate.entered() == 1
    })
    .await;
    for _ in 0..5 {
        push(&client, "k", 10, "").await;
    }
    Plain::set()
        .delay(Duration::from_millis(300))
        .perform_async(&client, (7,))
        .await
        .unwrap();
    wait_for(
        "the scheduled plain job",
        Duration::from_secs(3),
        || async { started.runs.len() == 1 },
    )
    .await;
    started.gate.open();
    started.launcher.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parked_jobs_come_back_at_their_slots_not_at_the_next_scheduler_poll() {
    // The scheduler polls once at the start, then about a minute later: only the wake-ups the
    // gate asks for when it parks a job bring the parked jobs back in time.
    let redis = TestRedis::start().await;
    let started = start(&redis, 4, Throttle::new("slow").concurrency(1), |builder| {
        builder.poll_interval_average(Duration::from_secs(60))
    })
    .await;
    let pool = started.hench.redis().clone();
    let client = started.hench.client().clone();
    let begun = Instant::now();
    for index in 0..4 {
        push(&client, &format!("k{index}"), 100, "").await;
    }
    wait_for("a parked job", WAIT, || async {
        zcard(&pool, keys::SCHEDULE).await > 0
    })
    .await;
    wait_for("the four jobs", Duration::from_secs(20), || async {
        started.log.finished() == 4
    })
    .await;
    assert!(
        begun.elapsed() < Duration::from_secs(20),
        "{:?}",
        begun.elapsed()
    );
    assert_eq!(started.log.max_overlap(), 1);
    started.launcher.stop().await.unwrap();
}

/// What the executions of [`Relay`] saw.
#[derive(Default)]
struct RelayProbe {
    running: std::sync::atomic::AtomicUsize,
    most: std::sync::atomic::AtomicUsize,
    executions: Mutex<std::collections::HashMap<u64, u32>>,
    done: Mutex<Vec<u64>>,
    leaseless: std::sync::atomic::AtomicUsize,
}

/// A job of the throttle `relay` (concurrency 1) that stops once (`JobError::interrupted`: it
/// is pushed back to its queue, where an idle processor fetches it at once), then runs again
/// and checks that its own execution holds a lease.
struct Relay;

impl Job for Relay {
    const NAME: &'static str = "throttle::Relay";
    type Args = (u64,);

    fn options() -> JobOptions {
        JobOptions::new().throttle("relay")
    }

    async fn perform(&self, ctx: &JobContext, (round,): Self::Args) -> Result<(), JobError> {
        use std::sync::atomic::Ordering;
        let probe = ctx.data::<RelayProbe>()?;
        let now = probe.running.fetch_add(1, Ordering::SeqCst) + 1;
        probe.most.fetch_max(now, Ordering::SeqCst);
        let execution = {
            let mut executions = probe.executions.lock().unwrap();
            let entry = executions.entry(round).or_insert(0);
            *entry += 1;
            *entry
        };
        let result = if execution == 1 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Err(JobError::interrupted())
        } else {
            // Let the release of the first execution's permits land, then look for this
            // execution's lease: a token naming the jid.
            tokio::time::sleep(Duration::from_millis(100)).await;
            let pool = ctx.client().redis().clone();
            let holders: Vec<String> = cmd(
                &pool,
                &redis_cmd!("ZRANGE", keys::limit_state('c', "relay"), 0, -1),
            )
            .await;
            let prefix = format!("{}~", ctx.jid());
            if !holders.iter().any(|token| token.starts_with(&prefix)) {
                probe.leaseless.fetch_add(1, Ordering::SeqCst);
            }
            probe.done.lock().unwrap().push(round);
            Ok(())
        };
        probe.running.fetch_sub(1, Ordering::SeqCst);
        result
    }
}

/// ratelimit-1: a job pushed back to its queue (or requeued) can be fetched and gated again
/// by an idle processor before the slot that ran it has released its permits. Each execution
/// holds its leases under a token of its own, so that release cannot take the lease of the
/// next execution: the next execution waits for the slot (or holds a lease of its own), and the
/// throttle never runs two jobs at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_job_fetched_again_before_its_release_keeps_its_own_lease() {
    use std::sync::atomic::Ordering;
    const ROUNDS: u64 = 15;
    init_logging();
    let redis = TestRedis::start().await;
    let probe = Arc::new(RelayProbe::default());
    let hench = redis
        .builder()
        .await
        // Renewals would restore a lost lease within a beat: keep them out of the way.
        .heartbeat_interval(Duration::from_secs(10))
        .concurrency(4)
        .register(Relay)
        .throttle(Throttle::new("relay").concurrency(1))
        .data_arc(probe.clone())
        .build()
        .await
        .unwrap();
    let launcher = hench.start().await.unwrap();
    let client = hench.client().clone();
    for round in 0..ROUNDS {
        Relay::perform_async(&client, (round,)).await.unwrap();
        wait_for("the round", Duration::from_secs(30), || async {
            probe.done.lock().unwrap().contains(&round)
        })
        .await;
    }
    launcher.stop().await.unwrap();
    assert_eq!(probe.leaseless.load(Ordering::SeqCst), 0);
    assert_eq!(probe.most.load(Ordering::SeqCst), 1);
    let stats = hgetall(hench.redis(), &keys::limit_stats('c', "relay")).await;
    assert_eq!(stats.get("ov"), None, "{stats:?}");
}

/// The tokens of engine-held leases name the job, so the API and the dashboard can link a
/// holder to its job; each execution's is its own.
#[tokio::test]
async fn holders_name_the_job_of_each_execution() {
    let redis = TestRedis::start().await;
    let started = start(&redis, 2, Throttle::new("slow").concurrency(2), |b| b).await;
    let client = started.hench.client().clone();
    let jid = push(&client, "x", 0, "hang").await;
    wait_for("the job holding its slot", WAIT, || async {
        started.gate.entered() == 1
    })
    .await;
    let holders = hench::api::Api::new(started.hench.redis().clone())
        .limits()
        .holders("slow")
        .await
        .unwrap();
    assert_eq!(holders.len(), 1);
    assert_eq!(holders[0].jid.as_deref(), Some(jid.as_str()));
    assert!(holders[0].token.starts_with(&format!("{jid}~")));
    assert_eq!(holders[0].token.len(), jid.len() + 17);
    started.gate.open();
    started.runs.wait_for(0).await;
    wait_for("the slot given back", WAIT, || async {
        !exists(started.hench.redis(), &keys::limit_state('c', "slow")).await
    })
    .await;
    started.launcher.stop().await.unwrap();
}

/// Most executions of the `Burst` jobs running at once, and when the last one ended.
#[derive(Default)]
struct BurstProbe {
    running: std::sync::atomic::AtomicUsize,
    most: std::sync::atomic::AtomicUsize,
    done: std::sync::atomic::AtomicUsize,
}

/// A short job (its argument: milliseconds) of the throttle `burst`, keyed by its key.
struct Burst;

impl Job for Burst {
    const NAME: &'static str = "throttle::Burst";
    type Args = (String, u64);

    fn options() -> JobOptions {
        JobOptions::new().throttle("burst")
    }

    fn throttle_key(args: &Self::Args) -> Option<String> {
        Some(args.0.clone())
    }

    async fn perform(&self, ctx: &JobContext, (_, millis): Self::Args) -> Result<(), JobError> {
        use std::sync::atomic::Ordering;
        let probe = ctx.data::<BurstProbe>()?;
        let now = probe.running.fetch_add(1, Ordering::SeqCst) + 1;
        probe.most.fetch_max(now, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(millis)).await;
        probe.running.fetch_sub(1, Ordering::SeqCst);
        probe.done.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// Runs `jobs` (key, milliseconds) through a worker of `slots` processors with `throttle`
/// (named `burst`), all pushed at once to a fresh Redis: how long they took, and the most that
/// ran at once.
async fn drain(throttle: Throttle, slots: usize, jobs: &[(String, u64)]) -> (Duration, usize) {
    use std::sync::atomic::Ordering;
    init_logging();
    let redis = TestRedis::start().await;
    let probe = Arc::new(BurstProbe::default());
    let hench = redis
        .builder()
        .await
        .concurrency(slots)
        .register(Burst)
        .throttle(throttle)
        .data_arc(probe.clone())
        .build()
        .await
        .unwrap();
    let launcher = hench.start().await.unwrap();
    let client = hench.client().clone();
    let started = Instant::now();
    for (key, millis) in jobs {
        Burst::perform_async(&client, (key.clone(), *millis))
            .await
            .unwrap();
    }
    wait_for("the backlog drained", Duration::from_secs(60), || async {
        probe.done.load(Ordering::SeqCst) == jobs.len()
    })
    .await;
    let elapsed = started.elapsed();
    launcher.stop().await.unwrap();
    println!("{} jobs drained in {elapsed:?}", jobs.len());
    (elapsed, probe.most.load(Ordering::SeqCst))
}

/// cross-soak-perf-1: a backlog of short jobs pushed at once to a throttle whose hold time
/// nobody has measured yet drains near the throttle's limit. The wait line used to space the
/// first parked jobs a second apart per slot whatever their length (60 jobs of 100 ms through 3
/// slots: 20 s instead of 2 s), park each one at least a second, plus a jitter of up to a tenth.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_backlog_of_short_jobs_drains_near_the_limit() {
    let jobs: Vec<(String, u64)> = (0..60).map(|_| ("all".to_owned(), 100)).collect();
    let (elapsed, most) = drain(Throttle::new("burst").concurrency(3), 12, &jobs).await;
    assert!(most <= 3, "{most} ran at once");
    // Ideal: 2 s. Allow for a debug build on a busy machine (at least 40 % of the limit).
    assert!(elapsed < Duration::from_secs(5), "{elapsed:?}");
}

/// The same with one job at a time per key and fresh keys (the README's recipe): each key's
/// line is spaced by the hold time its first jobs reveal, not by a guess of a second.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fresh_keys_of_a_per_key_throttle_drain_near_their_limit() {
    let jobs: Vec<(String, u64)> = (0..60)
        .map(|index| (format!("tenant-{}", index % 4), 50))
        .collect();
    let (elapsed, most) = drain(Throttle::new("burst").concurrency_per_key(1), 12, &jobs).await;
    assert!(most <= 4, "{most} ran at once");
    // Ideal: 15 jobs of 50 ms per key, 0.75 s.
    assert!(elapsed < Duration::from_secs(3), "{elapsed:?}");
}

/// ratelimit-4: the heartbeat renews the permits of running jobs; a lease shorter than three
/// heartbeat intervals would end while jobs run (and let other jobs take their slots), so
/// `build` refuses it.
#[tokio::test]
async fn leases_shorter_than_three_heartbeats_are_refused() {
    let redis = TestRedis::start().await;
    let error = redis
        .builder()
        .await
        .heartbeat_interval(Duration::from_secs(10))
        .throttle(
            Throttle::new("short")
                .concurrency(1)
                .lease(Duration::from_secs(5)),
        )
        .build()
        .await
        .unwrap_err();
    assert!(matches!(error, hench::Error::Config(_)), "{error}");
    let message = error.to_string();
    assert!(
        message.contains("throttle `short`") && message.contains("heartbeat"),
        "{message}"
    );
    // Three heartbeats or more: accepted.
    redis
        .builder()
        .await
        .heartbeat_interval(Duration::from_secs(10))
        .throttle(
            Throttle::new("long")
                .concurrency(1)
                .lease(Duration::from_secs(30)),
        )
        .build()
        .await
        .unwrap();
}

/// ratelimit-6/7: a job parked by its throttle carries its place in the throttle's wait line
/// (`limited.s`) and its parkings (`limited.t`), and is parked exactly until its slot: no
/// jitter, which would only make it late for its place (and let a newcomer take its unit).
#[tokio::test]
async fn parked_jobs_carry_their_place_and_come_back_for_it() {
    let redis = TestRedis::start().await;
    let throttle = Throttle::new("slow").window(1, Duration::from_secs(2));
    let started = start(&redis, 2, throttle, |b| b).await;
    let pool = started.hench.redis().clone();
    let client = started.hench.client().clone();
    let begin = now_secs();
    push(&client, "a", 0, "").await;
    let parked = push(&client, "b", 0, "").await;
    wait_for("the second job parked", WAIT, || async {
        zcard(&pool, keys::SCHEDULE).await == 1
    })
    .await;
    let (job, score) = sorted_jobs(&pool, keys::SCHEDULE).await.remove(0);
    assert_eq!(job.jid, parked);
    let limited = job.limited.unwrap();
    assert_eq!(limited.by, "w:slow");
    assert_eq!(
        (limited.n, limited.throttled, limited.over_limit),
        (1, 1, 0)
    );
    let slot = limited.slot.expect("its place in the line");
    // Parked until its slot (when the first start leaves the 2 s window), exactly.
    let slot_secs = slot as f64 / 1_000.0;
    assert!((score - slot_secs).abs() < 0.05, "{score} vs {slot_secs}");
    assert!(
        slot_secs - begin > 1.5 && slot_secs - begin < 2.5,
        "{slot_secs}"
    );
    wait_for("both jobs ran", Duration::from_secs(10), || async {
        started.log.finished() == 2
    })
    .await;
    started.launcher.stop().await.unwrap();
}

fn now_secs() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

/// cross-soak-perf-6: a per-key throttle keeps counters, an index entry and a wait line per
/// identity (one per key) for `stats_ttl` after its last use; with `stats(false)` it keeps none,
/// and still limits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn throttle_statistics_follow_stats_and_stats_ttl() {
    for (stats, ttl) in [
        (true, Duration::from_secs(600)),
        (false, Throttle::STATS_TTL),
    ] {
        let redis = TestRedis::start().await;
        let throttle = Throttle::new("slow")
            .concurrency_per_key(1)
            .stats(stats)
            .stats_ttl(ttl);
        let started = start(&redis, 4, throttle, |b| b).await;
        let pool = started.hench.redis().clone();
        let client = started.hench.client().clone();
        // Two jobs of one key (one is refused at least once), and three other keys.
        for key in ["a", "a", "b", "c", "d"] {
            push(&client, key, 120, "").await;
        }
        wait_for("every job", Duration::from_secs(20), || async {
            started.log.finished() == 5
        })
        .await;
        started.launcher.stop().await.unwrap();
        assert!(started.log.max_overlap_of("a") <= 1);
        let indexed = zcard(&pool, keys::LIMITS).await;
        let mut conn = pool.conn().await.unwrap();
        let stats_keys: Vec<String> = hench::redis_rs::cmd("KEYS")
            .arg(keys::limit_stats('c', "slow*"))
            .query_async(&mut conn)
            .await
            .unwrap();
        if stats {
            assert_eq!(indexed, 4, "one index entry per key");
            assert_eq!(stats_keys.len(), 4, "{stats_keys:?}");
            let left = pttl(&pool, &keys::limit_stats('c', "slow:a")).await;
            assert!(left > 0 && left <= 600_000, "{left} ms");
        } else {
            assert_eq!(indexed, 0);
            assert!(stats_keys.is_empty(), "{stats_keys:?}");
        }
    }
}
