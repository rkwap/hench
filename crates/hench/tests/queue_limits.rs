//! Queue limits through real workers (`docs/design/ratelimit.md` §3.4.13): limits on every
//! fetch of a queue, enforced by the fetch. A queue over its limits is skipped like a paused one:
//! its jobs stay in it, in order (nothing is parked in the schedule), and the other queues keep
//! flowing.

mod support;

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use hench::api::{Api, Queue};
use hench::fetch::Fetcher;
use hench::limit::Limit;
use hench::prelude::*;
use hench::test_util::TestRedis;
use hench::{FetchStrategy, keys, redis_rs};
use support::*;

/// What the tracked jobs saw: the order they started in, and per queue the most running at
/// once.
#[derive(Default)]
struct Tracker {
    started: Mutex<Vec<(String, u32, Instant)>>,
    running: Mutex<HashMap<String, usize>>,
    most: Mutex<HashMap<String, usize>>,
    finished: AtomicUsize,
}

impl Tracker {
    fn start(&self, queue: &str, marker: u32) {
        self.started
            .lock()
            .unwrap()
            .push((queue.to_owned(), marker, Instant::now()));
        let mut running = self.running.lock().unwrap();
        let now = running.entry(queue.to_owned()).or_default();
        *now += 1;
        let mut most = self.most.lock().unwrap();
        let top = most.entry(queue.to_owned()).or_default();
        *top = (*top).max(*now);
    }

    fn finish(&self, queue: &str) {
        *self
            .running
            .lock()
            .unwrap()
            .entry(queue.to_owned())
            .or_default() -= 1;
        self.finished.fetch_add(1, Ordering::SeqCst);
    }

    fn most(&self, queue: &str) -> usize {
        self.most.lock().unwrap().get(queue).copied().unwrap_or(0)
    }

    fn order(&self, queue: &str) -> Vec<u32> {
        self.started
            .lock()
            .unwrap()
            .iter()
            .filter(|(q, _, _)| q == queue)
            .map(|(_, marker, _)| *marker)
            .collect()
    }

    fn times(&self, queue: &str) -> Vec<Instant> {
        self.started
            .lock()
            .unwrap()
            .iter()
            .filter(|(q, _, _)| q == queue)
            .map(|(_, _, at)| *at)
            .collect()
    }

    fn finished(&self) -> usize {
        self.finished.load(Ordering::SeqCst)
    }

    /// The jobs of `queue` that ran to the end.
    fn finished_in(&self, queue: &str) -> usize {
        let running = self
            .running
            .lock()
            .unwrap()
            .get(queue)
            .copied()
            .unwrap_or(0);
        self.order(queue).len() - running
    }
}

/// Records its start, sleeps `ms`, records its end.
struct Tracked;

impl Job for Tracked {
    const NAME: &'static str = "queue_limits::Tracked";
    type Args = (u32, u64);

    async fn perform(&self, ctx: &JobContext, (marker, ms): Self::Args) -> Result<(), JobError> {
        let tracker = ctx.data::<Tracker>()?;
        let queue = ctx.payload().queue.clone();
        tracker.start(&queue, marker);
        tokio::time::sleep(Duration::from_millis(ms)).await;
        tracker.finish(&queue);
        Ok(())
    }
}

/// Waits at the shared `Gate`, then records itself.
struct Held;

impl Job for Held {
    const NAME: &'static str = "queue_limits::Held";
    type Args = (u32,);

    async fn perform(&self, ctx: &JobContext, (marker,): Self::Args) -> Result<(), JobError> {
        let runs = ctx.data::<Runs>()?;
        runs.record(format!("entered:{marker}"));
        ctx.data::<Gate>()?.pass().await;
        runs.record(format!("done:{marker}"));
        Ok(())
    }
}

async fn push(hench: &Hench, queue: &str, marker: u32, ms: u64) {
    Tracked::set()
        .queue(queue)
        .perform_async(hench.client(), (marker, ms))
        .await
        .unwrap();
}

async fn push_held(hench: &Hench, queue: &str, marker: u32) {
    Held::set()
        .queue(queue)
        .perform_async(hench.client(), (marker,))
        .await
        .unwrap();
}

fn concurrency(limit: u32) -> Limit {
    Limit::Concurrency { limit }
}

/// `concurrency=2` on a queue of 20 jobs: at most 2 run at once, they start in the order they
/// were pushed, nothing is parked in the schedule, and another queue of the capsule keeps
/// flowing meanwhile (BullMQ: "should not put a job into the delayed queue when limit is hit").
async fn concurrency_limit_with(fetch: FetchStrategy) {
    let redis = TestRedis::start().await;
    let tracker = Arc::new(Tracker::default());
    let hench = redis
        .builder()
        .await
        .fetch(fetch)
        .concurrency(6)
        .queues(["exports", "default"])
        .queue_limits("exports", [concurrency(2)])
        .register(Tracked)
        .data_arc(tracker.clone())
        .build()
        .await
        .unwrap();
    let pool = hench.redis().clone();
    for marker in 0..20 {
        push(&hench, "exports", marker, 60).await;
    }
    for marker in 0..10 {
        push(&hench, "default", marker, 10).await;
    }
    let started = Instant::now();
    let launcher = hench.start().await.unwrap();
    wait_for("the default queue drained", WAIT, || async {
        tracker.order("default").len() == 10
    })
    .await;
    let default_done = started.elapsed();
    wait_for("every job ran", Duration::from_secs(20), || async {
        tracker.finished() == 30
    })
    .await;
    let all_done = started.elapsed();
    launcher.stop().await.unwrap();
    assert!(tracker.most("exports") <= 2, "{}", tracker.most("exports"));
    assert_eq!(tracker.most("exports"), 2, "the limit was used");
    assert_eq!(tracker.order("exports"), (0..20).collect::<Vec<_>>());
    // 20 jobs of 60 ms two at a time take at least 600 ms; the other queue did not wait for them.
    assert!(all_done >= Duration::from_millis(600), "{all_done:?}");
    assert!(default_done < all_done, "{default_done:?} vs {all_done:?}");
    assert_eq!(zcard(&pool, keys::SCHEDULE).await, 0);
    assert_eq!(zcard(&pool, keys::RETRY).await, 0);
    // Every slot was given back.
    assert_eq!(zcard(&pool, &keys::limit_state('c', "@exports")).await, 0);
    let stats = hgetall(&pool, &keys::limit_stats('c', "@exports")).await;
    assert_eq!(stats["g"], "20");
    assert_eq!(stats["h"], "20", "each slot released once: {stats:?}");
}

#[tokio::test]
async fn a_concurrency_limit_caps_a_queue_and_keeps_its_order() {
    concurrency_limit_with(FetchStrategy::Reliable).await;
}

#[tokio::test]
async fn a_concurrency_limit_caps_a_queue_with_basic_fetch() {
    concurrency_limit_with(FetchStrategy::Basic).await;
}

/// `window=5/1s` over 20 jobs: they run in at least 3 seconds (BullMQ: "should obey the rate
/// limit"), the first 5 at once.
#[tokio::test]
async fn a_window_limit_paces_a_queue() {
    let redis = TestRedis::start().await;
    let tracker = Arc::new(Tracker::default());
    let hench = redis
        .builder()
        .await
        .concurrency(5)
        .queues(["reports"])
        .queue_limits(
            "reports",
            [Limit::Window {
                limit: 5,
                period: Duration::from_secs(1),
            }],
        )
        .register(Tracked)
        .data_arc(tracker.clone())
        .build()
        .await
        .unwrap();
    let pool = hench.redis().clone();
    for marker in 0..20 {
        push(&hench, "reports", marker, 1).await;
    }
    let launcher = hench.start().await.unwrap();
    wait_for("every job ran", Duration::from_secs(20), || async {
        tracker.finished() == 20
    })
    .await;
    launcher.stop().await.unwrap();
    let times = tracker.times("reports");
    let first = times[0];
    // Any 6 consecutive starts span at least the period: no second holds more than 5.
    for window in times.windows(6) {
        let span = window[5].duration_since(window[0]);
        assert!(span >= Duration::from_millis(950), "{span:?}");
    }
    let span = times[19].duration_since(first);
    assert!(span >= Duration::from_millis(2_900), "{span:?}");
    assert!(span < Duration::from_secs(8), "{span:?}");
    assert_eq!(tracker.order("reports"), (0..20).collect::<Vec<_>>());
    assert_eq!(zcard(&pool, keys::SCHEDULE).await, 0);
}

/// A limit set through the API applies within a heartbeat, and clearing it lifts it.
#[tokio::test]
async fn limits_set_at_run_time_apply_within_a_heartbeat() {
    let redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    let gate = Arc::new(Gate::default());
    let hench = redis
        .builder()
        .await
        .concurrency(4)
        .queues(["q"])
        .register(Held)
        .data_arc(runs.clone())
        .data_arc(gate.clone())
        .build()
        .await
        .unwrap();
    let api = Api::new(hench.redis().clone());
    let queue = Queue::new(&api, "q");
    let launcher = hench.start().await.unwrap();
    queue.set_limits(&[concurrency(1)]).await.unwrap();
    assert_eq!(queue.limits().await.unwrap(), [concurrency(1)]);
    // A few heartbeats (100 ms in tests).
    tokio::time::sleep(Duration::from_millis(400)).await;
    for marker in 0..3 {
        push_held(&hench, "q", marker).await;
    }
    wait_for("one job entered", WAIT, || async { gate.entered() == 1 }).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(gate.entered(), 1, "the limit holds: {:?}", runs.entries());
    assert_eq!(queue_size(hench.redis(), "q").await, 2);
    // Cleared: the two others start without waiting for the first.
    assert!(queue.clear_limits().await.unwrap());
    assert!(!queue.clear_limits().await.unwrap());
    assert!(queue.limits().await.unwrap().is_empty());
    wait_for("all three entered", WAIT, || async { gate.entered() == 3 }).await;
    gate.open();
    wait_for("all three done", WAIT, || async {
        runs.count("done:0") + runs.count("done:1") + runs.count("done:2") == 3
    })
    .await;
    launcher.stop().await.unwrap();
}

/// A job arriving on an empty queue whose concurrency limit is full wakes the processor
/// watching the queue, which leaves it where it is (not in the schedule): it runs first once a
/// slot frees.
#[tokio::test]
async fn a_job_arriving_while_its_queue_is_full_stays_first_in_line() {
    let redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    let gate = Arc::new(Gate::default());
    let hench = redis
        .builder()
        .await
        .concurrency(2)
        .queues(["q"])
        .queue_limits("q", [concurrency(1)])
        .register(Held)
        .data_arc(runs.clone())
        .data_arc(gate.clone())
        .build()
        .await
        .unwrap();
    let pool = hench.redis().clone();
    push_held(&hench, "q", 0).await;
    let launcher = hench.start().await.unwrap();
    wait_for("the first job holds the slot", WAIT, || async {
        gate.entered() == 1
    })
    .await;
    // The other processor waits on the empty queue: the next job reaches it there.
    tokio::time::sleep(Duration::from_millis(300)).await;
    push_held(&hench, "q", 1).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    push_held(&hench, "q", 2).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(gate.entered(), 1, "{:?}", runs.entries());
    let waiting: Vec<u64> = queue_jobs(&pool, "q")
        .await
        .iter()
        .map(|job| job.args[0].as_u64().unwrap())
        .collect();
    // Next to run first: job 1, ahead of job 2.
    assert_eq!(waiting, [1, 2]);
    assert_eq!(zcard(&pool, keys::SCHEDULE).await, 0);
    let stats = hgetall(&pool, &keys::limit_stats('c', "@q")).await;
    assert!(stats["r"].parse::<u32>().unwrap() >= 1, "{stats:?}");
    gate.open();
    wait_for("all three done", WAIT, || async { runs.len() == 6 }).await;
    launcher.stop().await.unwrap();
    let entered: Vec<String> = runs
        .entries()
        .into_iter()
        .filter(|entry| entry.starts_with("entered"))
        .collect();
    assert_eq!(entered, ["entered:0", "entered:1", "entered:2"]);
}

/// A paused queue stays paused whatever its limits; unpaused, it runs within them.
#[tokio::test]
async fn a_paused_limited_queue_stays_paused() {
    let redis = TestRedis::start().await;
    let tracker = Arc::new(Tracker::default());
    let hench = redis
        .builder()
        .await
        .concurrency(3)
        .queues(["q"])
        .queue_limits("q", [concurrency(1)])
        .register(Tracked)
        .data_arc(tracker.clone())
        .build()
        .await
        .unwrap();
    let api = Api::new(hench.redis().clone());
    let queue = Queue::new(&api, "q");
    queue.pause().await.unwrap();
    for marker in 0..3 {
        push(&hench, "q", marker, 20).await;
    }
    let launcher = hench.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(tracker.finished(), 0);
    queue.unpause().await.unwrap();
    wait_for("every job ran", WAIT, || async { tracker.finished() == 3 }).await;
    launcher.stop().await.unwrap();
    assert_eq!(tracker.most("q"), 1);
}

/// The slot of a running job is renewed by the heartbeat, and released after its outcome,
/// failures included.
#[tokio::test]
async fn slots_are_renewed_while_jobs_run_and_released_after_every_outcome() {
    let redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    let gate = Arc::new(Gate::default());
    let hench = redis
        .builder()
        .await
        .concurrency(2)
        .queues(["q"])
        .queue_limits("q", [concurrency(2)])
        .register(Held)
        .register(Fail)
        .data_arc(runs.clone())
        .data_arc(gate.clone())
        .build()
        .await
        .unwrap();
    let pool = hench.redis().clone();
    let state = keys::limit_state('c', "@q");
    push_held(&hench, "q", 0).await;
    let launcher = hench.start().await.unwrap();
    wait_for("the job runs", WAIT, || async { gate.entered() == 1 }).await;
    let leases = zrange(&pool, &state).await;
    assert_eq!(leases.len(), 1);
    let (token, first_end) = leases[0].clone();
    // A minute's lease, renewed by later heartbeats.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let renewed = zrange(&pool, &state).await;
    assert_eq!(renewed.len(), 1);
    assert_eq!(renewed[0].0, token);
    assert!(renewed[0].1 > first_end, "{renewed:?} vs {first_end}");
    gate.open();
    wait_for("the slot released", WAIT, || async {
        zcard(&pool, &state).await == 0
    })
    .await;
    // A failure releases its slot too (the job goes to the retry set).
    Fail::set()
        .queue("q")
        .perform_async(hench.client(), vec![])
        .await
        .unwrap();
    wait_for("the failure retried", WAIT, || async {
        zcard(&pool, keys::RETRY).await == 1
    })
    .await;
    wait_for("its slot released", WAIT, || async {
        zcard(&pool, &state).await == 0
    })
    .await;
    launcher.stop().await.unwrap();
    let stats = hgetall(&pool, &keys::limit_stats('c', "@q")).await;
    assert_eq!(stats["g"], "2");
    assert_eq!(stats["h"], "2");
}

/// The limits of the configuration replace the stored ones at start (code wins over run-time
/// edits at the next deploy).
#[tokio::test]
async fn configured_limits_are_written_at_start() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let api = Api::new(pool.clone());
    Queue::new(&api, "exports")
        .set_limits(&[concurrency(5)])
        .await
        .unwrap();
    Queue::new(&api, "other")
        .set_limits_from_str("window = 10/1s")
        .await
        .unwrap();
    let hench = redis
        .builder()
        .await
        .queues(["exports"])
        .queue_limits("exports", [concurrency(9)])
        .queue_limits("exports", [concurrency(1)])
        .register(Tracked)
        .build()
        .await
        .unwrap();
    let launcher = hench.start().await.unwrap();
    assert_eq!(
        Queue::new(&api, "exports").limits().await.unwrap(),
        [concurrency(1)]
    );
    // Queues the configuration does not name keep theirs.
    let all = Queue::all_limits(&api).await.unwrap();
    assert_eq!(all["exports"], "concurrency=1");
    assert_eq!(all["other"], "window=10/1s");
    launcher.stop().await.unwrap();
}

/// Limits a fetch cannot enforce, or a queue with two limits of one kind, are refused when the
/// configuration is built and when they are set.
#[tokio::test]
async fn limits_a_fetch_cannot_enforce_are_refused() {
    let redis = TestRedis::start().await;
    let error = redis
        .builder()
        .await
        .queue_limits(
            "exports",
            [Limit::Points {
                capacity: 10,
                refill: Duration::from_secs(1),
            }],
        )
        .build()
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("points limits need a cost"), "{error}");
    let error = redis
        .builder()
        .await
        .queue_limits("exports", [concurrency(1), concurrency(2)])
        .build()
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("two concurrency limits"), "{error}");
    let error = redis
        .builder()
        .await
        .queue_limits("exports", [])
        .build()
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("no limit"), "{error}");
    let api = Api::new(redis.pool().await);
    let queue = Queue::new(&api, "exports");
    let error = queue.set_limits(&[]).await.unwrap_err().to_string();
    assert!(error.contains("no limit"), "{error}");
    let error = queue
        .set_limits_from_str("concurrency=2 per key")
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("`per key` applies to throttles only"),
        "{error}"
    );
    assert!(queue.limits().await.unwrap().is_empty());
}

/// Stored limits that are not valid (an edit by hand) leave the queue alone until they are
/// fixed: a broken limit must not lift the protection it was meant to give.
#[tokio::test]
async fn a_queue_with_broken_stored_limits_is_not_fetched() {
    let redis = TestRedis::start().await;
    let tracker = Arc::new(Tracker::default());
    let hench = redis
        .builder()
        .await
        .concurrency(2)
        .queues(["q", "other"])
        .register(Tracked)
        .data_arc(tracker.clone())
        .build()
        .await
        .unwrap();
    let pool = hench.redis().clone();
    let _: i64 = cmd(
        &pool,
        redis_rs::cmd("HSET")
            .arg(keys::QUEUE_LIMITS)
            .arg("q")
            .arg("concurrency=lots"),
    )
    .await;
    let api = Api::new(pool.clone());
    let queue = Queue::new(&api, "q");
    let error = queue.limits().await.unwrap_err().to_string();
    assert!(error.contains("are not valid"), "{error}");
    push(&hench, "q", 0, 1).await;
    push(&hench, "other", 1, 1).await;
    let launcher = hench.start().await.unwrap();
    wait_for("the other queue ran", WAIT, || async {
        tracker.finished() == 1
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(queue_size(&pool, "q").await, 1);
    queue.set_limits(&[concurrency(1)]).await.unwrap();
    wait_for("the queue runs again", WAIT, || async {
        tracker.finished() == 2
    })
    .await;
    launcher.stop().await.unwrap();
}

/// A fetcher outside a running process applies the limits too, and gives its slots back with
/// `release_limits`.
#[tokio::test]
async fn a_standalone_fetcher_takes_and_releases_slots() {
    let redis = TestRedis::start().await;
    let hench = redis
        .builder()
        .await
        .queues(["q"])
        .register(Tracked)
        .build()
        .await
        .unwrap();
    let pool = hench.redis().clone();
    let api = Api::new(pool.clone());
    Queue::new(&api, "q")
        .set_limits(&[concurrency(1)])
        .await
        .unwrap();
    push(&hench, "q", 0, 1).await;
    push(&hench, "q", 1, 1).await;
    let capsule = &hench.config().capsules[0];
    let fetcher = Fetcher::new(FetchStrategy::Basic, pool.clone(), "tool:1:abc", capsule)
        .with_timeout(Duration::from_millis(100));
    let mut worker = fetcher.worker(0);
    let first = worker.retrieve_work().await.unwrap().unwrap();
    // Full: the second job stays.
    assert!(worker.retrieve_work().await.unwrap().is_none());
    assert_eq!(queue_size(&pool, "q").await, 1);
    let state = keys::limit_state('c', "@q");
    assert_eq!(zcard(&pool, &state).await, 1);
    fetcher.release_limits(&first).await.unwrap();
    assert_eq!(zcard(&pool, &state).await, 0);
    let second = worker.retrieve_work().await.unwrap().unwrap();
    assert_ne!(first.job(), second.job());
    fetcher.release_limits(&second).await.unwrap();
    // A job of an unlimited queue holds nothing.
    Queue::new(&api, "q").clear_limits().await.unwrap();
}

/// The jobs interrupted by a hard shutdown go back to their queue, and their slots are freed.
#[tokio::test]
async fn a_hard_shutdown_frees_the_slots_of_interrupted_jobs() {
    let redis = TestRedis::start().await;
    let runs = Arc::new(Runs::default());
    let hench = redis
        .builder()
        .await
        .concurrency(2)
        .queues(["q"])
        .timeout(Duration::from_millis(200))
        .queue_limits("q", [concurrency(2)])
        .register(Stubborn)
        .data_arc(runs.clone())
        .build()
        .await
        .unwrap();
    let pool = hench.redis().clone();
    for _ in 0..2 {
        Stubborn::set()
            .queue("q")
            .perform_async(hench.client(), ("forever".to_owned(),))
            .await
            .unwrap();
    }
    let state = keys::limit_state('c', "@q");
    let launcher = hench.start().await.unwrap();
    runs.wait_for(2).await;
    assert_eq!(zcard(&pool, &state).await, 2);
    launcher.stop().await.unwrap();
    assert_eq!(queue_size(&pool, "q").await, 2);
    assert_eq!(zcard(&pool, &state).await, 0);
}

/// Several processes share a queue's limits: three workers of four processors each, a queue
/// limited to 3 at a time across them, 45 jobs. At no time do more than 3 run, every job runs
/// once, and every slot is given back.
#[tokio::test]
async fn processes_share_a_queues_limits() {
    let redis = TestRedis::start().await;
    let tracker = Arc::new(Tracker::default());
    let mut launchers = Vec::new();
    let mut client = None;
    for _ in 0..3 {
        let hench = redis
            .builder()
            .await
            .concurrency(4)
            .queues(["shared"])
            .queue_limits("shared", [concurrency(3)])
            .register(Tracked)
            .data_arc(tracker.clone())
            .build()
            .await
            .unwrap();
        launchers.push(hench.start().await.unwrap());
        client.get_or_insert(hench);
    }
    let hench = client.unwrap();
    for marker in 0..45 {
        push(&hench, "shared", marker, 30).await;
    }
    wait_for("every job ran", Duration::from_secs(20), || async {
        tracker.finished() == 45
    })
    .await;
    for launcher in launchers {
        launcher.stop().await.unwrap();
    }
    assert_eq!(tracker.most("shared"), 3);
    let mut order = tracker.order("shared");
    order.sort_unstable();
    assert_eq!(order, (0..45).collect::<Vec<_>>(), "each job ran once");
    let pool = hench.redis().clone();
    assert_eq!(zcard(&pool, &keys::limit_state('c', "@shared")).await, 0);
    let stats = hgetall(&pool, &keys::limit_stats('c', "@shared")).await;
    assert_eq!(stats["g"], "45");
    assert_eq!(stats["h"], "45");
    assert!(!stats.contains_key("ov"), "no overage: {stats:?}");
}

/// Starts `processors` processors on the queue `paced` limited by `limit`, with `jobs` jobs of
/// `ms` each already waiting; returns once every job ran, with the queue's counters.
async fn run_backlog(
    processors: usize,
    limit: Limit,
    jobs: u32,
    ms: u64,
) -> (HashMap<String, String>, Arc<Tracker>) {
    let redis = TestRedis::start().await;
    let tracker = Arc::new(Tracker::default());
    let hench = redis
        .builder()
        .await
        .concurrency(processors)
        .queues(["paced"])
        .queue_limits("paced", [limit])
        .register(Tracked)
        .data_arc(tracker.clone())
        .build()
        .await
        .unwrap();
    let pool = hench.redis().clone();
    for marker in 0..jobs {
        push(&hench, "paced", marker, ms).await;
    }
    let launcher = hench.start().await.unwrap();
    wait_for("every job ran", Duration::from_secs(60), || async {
        tracker.finished() == jobs as usize
    })
    .await;
    launcher.stop().await.unwrap();
    let letter = match limit {
        Limit::Concurrency { .. } => 'c',
        Limit::Window { .. } => 'w',
        Limit::Bucket { .. } => 'b',
        _ => 'g',
    };
    let stats = hgetall(&pool, &keys::limit_stats(letter, "@paced")).await;
    (stats, tracker)
}

fn refusals(stats: &HashMap<String, String>) -> u32 {
    stats.get("r").map_or(0, |r| r.parse().unwrap())
}

/// Idle processors do not stampede a queue over its limits: a grant that fills the queue's
/// limits says so, and once the queue's cooldown ends a single fetch of the process checks it
/// again. So the refusals (each a write to Redis) do not grow with the jobs or the processors
/// waiting (only the processors starting together are refused once), and the queue still
/// keeps its order.
#[tokio::test]
async fn idle_processors_check_a_full_queue_one_at_a_time() {
    let (few, _) = run_backlog(2, concurrency(1), 30, 30).await;
    let (many, tracker) = run_backlog(20, concurrency(1), 30, 30).await;
    assert_eq!(tracker.order("paced"), (0..30).collect::<Vec<_>>());
    assert_eq!(tracker.most("paced"), 1);
    // Before: 29 refusals with 2 processors, 551 with 20 (now 1 and 19).
    assert!(refusals(&few) <= 5, "{few:?}");
    assert!(refusals(&many) <= 30, "{many:?}");
    let (leaky, tracker) = run_backlog(
        20,
        Limit::Leaky {
            size: 10,
            drain: Duration::from_secs(1),
        },
        30,
        1,
    )
    .await;
    assert_eq!(tracker.order("paced"), (0..30).collect::<Vec<_>>());
    // Before: 543 refusals for 40 jobs (now about 10).
    assert!(refusals(&leaky) <= 30, "{leaky:?}");
}

/// While a job holds the only slot, the idle processors check the queue about once per
/// cooldown between them, not each on its own.
#[tokio::test]
async fn a_full_queue_is_polled_once_per_cooldown_by_a_process() {
    let redis = TestRedis::start().await;
    let tracker = Arc::new(Tracker::default());
    let hench = redis
        .builder()
        .await
        .concurrency(20)
        .queues(["paced"])
        .queue_limits("paced", [concurrency(1)])
        .register(Tracked)
        .data_arc(tracker.clone())
        .build()
        .await
        .unwrap();
    let pool = hench.redis().clone();
    let launcher = hench.start().await.unwrap();
    push(&hench, "paced", 0, 3_000).await;
    wait_for("the long job started", WAIT, || async {
        tracker.order("paced").len() == 1
    })
    .await;
    push(&hench, "paced", 1, 1).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let before = refusals(&hgetall(&pool, &keys::limit_stats('c', "@paced")).await);
    tokio::time::sleep(Duration::from_secs(2)).await;
    let after = refusals(&hgetall(&pool, &keys::limit_stats('c', "@paced")).await);
    // One probe per 250 ms cooldown: about 8 in 2 s (before: about 150).
    assert!(after - before <= 16, "{before} then {after}");
    wait_for("both ran", WAIT, || async { tracker.finished() == 2 }).await;
    launcher.stop().await.unwrap();
    assert_eq!(tracker.order("paced"), [0, 1]);
}

/// A burst arriving on an empty queue whose concurrency limit is full, while many processors
/// wait: the jobs leave the queue only through the gate, so they run in the order they came.
async fn a_burst_on_a_full_queue_keeps_its_order_with(fetch: FetchStrategy) {
    let redis = TestRedis::start().await;
    let tracker = Arc::new(Tracker::default());
    let hench = redis
        .builder()
        .await
        .fetch(fetch)
        .concurrency(20)
        .queues(["paced"])
        .queue_limits("paced", [concurrency(1)])
        .register(Tracked)
        .data_arc(tracker.clone())
        .build()
        .await
        .unwrap();
    let pool = hench.redis().clone();
    let launcher = hench.start().await.unwrap();
    push(&hench, "paced", 0, 1_000).await;
    wait_for("the long job started", WAIT, || async {
        tracker.order("paced").len() == 1
    })
    .await;
    // Every idle processor goes back to waiting on the empty queue.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut burst = redis_rs::cmd("LPUSH");
    burst.arg(keys::queue("paced"));
    for marker in 1..=6u32 {
        let job = payload(
            "queue_limits::Tracked",
            vec![serde_json::json!(marker), serde_json::json!(1)],
            "paced",
        );
        burst.arg(serde_json::to_vec(&job).unwrap());
    }
    let _: i64 = cmd(&pool, &burst).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    // Still waiting, in their order.
    let waiting: Vec<u64> = queue_jobs(&pool, "paced")
        .await
        .iter()
        .map(|job| job.args[0].as_u64().unwrap())
        .collect();
    assert_eq!(waiting, [1, 2, 3, 4, 5, 6]);
    wait_for("every job ran", WAIT, || async { tracker.finished() == 7 }).await;
    launcher.stop().await.unwrap();
    assert_eq!(tracker.order("paced"), (0..=6).collect::<Vec<_>>());
    assert_eq!(tracker.most("paced"), 1);
}

#[tokio::test]
async fn a_burst_on_a_full_queue_keeps_its_order() {
    a_burst_on_a_full_queue_keeps_its_order_with(FetchStrategy::Reliable).await;
}

#[tokio::test]
async fn a_burst_on_a_full_queue_keeps_its_order_with_basic_fetch() {
    a_burst_on_a_full_queue_keeps_its_order_with(FetchStrategy::Basic).await;
}

/// A queue that waits on its limits does not hold back the capsule's unlimited queues, and a job
/// arriving on a watched limited queue is fetched within the blocking wait (not after it).
async fn watched_and_plain_queues_both_flow_with(fetch: FetchStrategy) {
    let redis = TestRedis::start().await;
    let tracker = Arc::new(Tracker::default());
    let hench = redis
        .builder()
        .await
        .fetch(fetch)
        .concurrency(3)
        .queues(["limited", "plain"])
        .queue_limits("limited", [concurrency(2)])
        .register(Tracked)
        .data_arc(tracker.clone())
        .build()
        .await
        .unwrap();
    let launcher = hench.start().await.unwrap();
    // Every processor blocks on the empty queues.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let pushed = Instant::now();
    push(&hench, "limited", 1, 1).await;
    push(&hench, "plain", 2, 1).await;
    wait_for("both ran", WAIT, || async { tracker.finished() == 2 }).await;
    // Well within the 2 s fetch timeout: someone waited on each queue.
    assert!(
        pushed.elapsed() < Duration::from_millis(1_500),
        "{:?}",
        pushed.elapsed()
    );
    launcher.stop().await.unwrap();
}

#[tokio::test]
async fn watched_and_plain_queues_both_flow() {
    watched_and_plain_queues_both_flow_with(FetchStrategy::Reliable).await;
}

#[tokio::test]
async fn watched_and_plain_queues_both_flow_with_basic_fetch() {
    watched_and_plain_queues_both_flow_with(FetchStrategy::Basic).await;
}

/// Calls per command so far (`INFO commandstats`).
async fn command_calls(pool: &RedisPool) -> HashMap<String, u64> {
    let info: String = cmd(pool, redis_rs::cmd("INFO").arg("commandstats")).await;
    info.lines()
        .filter_map(|line| {
            let (name, stats) = line.strip_prefix("cmdstat_")?.split_once(':')?;
            let calls = stats
                .split(',')
                .find_map(|field| field.strip_prefix("calls="))?
                .parse()
                .ok()?;
            Some((name.to_owned(), calls))
        })
        .collect()
}

fn calls_between(before: &HashMap<String, u64>, after: &HashMap<String, u64>, name: &str) -> u64 {
    after.get(name).copied().unwrap_or(0) - before.get(name).copied().unwrap_or(0)
}

/// Pushes jobs `markers` of `ms` each to `queue` in one `LPUSH`.
async fn burst(pool: &RedisPool, queue: &str, markers: impl IntoIterator<Item = u32>, ms: u64) {
    let mut command = redis_rs::cmd("LPUSH");
    command.arg(keys::queue(queue));
    for marker in markers {
        let job = payload(
            "queue_limits::Tracked",
            vec![serde_json::json!(marker), serde_json::json!(ms)],
            queue,
        );
        command.arg(serde_json::to_vec(&job).unwrap());
    }
    let _: i64 = cmd(pool, &command).await;
}

/// A busy queue at `concurrency=1` makes its slot change hands for every job; the idle workers
/// are not all woken each time to look at the capsule's other queues (an empty limited queue,
/// or a paused one), so the script calls stay a few per job however many workers wait.
#[tokio::test]
async fn idle_workers_are_not_woken_for_every_job_of_a_busy_queue() {
    for (processors, others) in [(5, "idle"), (40, "idle"), (20, "paused")] {
        let redis = TestRedis::start().await;
        let tracker = Arc::new(Tracker::default());
        let hench = redis
            .builder()
            .await
            .fetch_timeout(Duration::from_secs(2))
            .concurrency(processors)
            .queues(["busy", others])
            .queue_limits("busy", [concurrency(1)])
            .queue_limits("idle", [concurrency(5)])
            .register(Tracked)
            .data_arc(tracker.clone())
            .build()
            .await
            .unwrap();
        let pool = hench.redis().clone();
        let _: i64 = cmd(&pool, redis_rs::cmd("SADD").arg(keys::PAUSED).arg("paused")).await;
        burst(&pool, "busy", 0..2_000, 5).await;
        let launcher = hench.start().await.unwrap();
        tokio::time::sleep(Duration::from_millis(1_000)).await;
        let before = command_calls(&pool).await;
        let ran_before = tracker.finished();
        tokio::time::sleep(Duration::from_secs(2)).await;
        let after = command_calls(&pool).await;
        let ran = tracker.finished() - ran_before;
        launcher.stop().await.unwrap();
        let scripts = calls_between(&before, &after, "evalsha");
        // Before: 8.5 calls a job with 5 workers, 39 with 20, 99 with 50 (now about 3).
        assert!(ran > 50, "{processors} workers, {others}: {ran} jobs");
        assert!(
            scripts <= 8 * ran as u64,
            "{processors} workers, {others}: {scripts} script calls for {ran} jobs"
        );
        assert_eq!(tracker.most("busy"), 1);
    }
}

/// A capsule without limited queues blocks for the whole fetch timeout, whatever another
/// capsule's limited queue does.
#[tokio::test]
async fn a_capsule_without_limits_is_not_woken_by_another_capsules_cooldowns() {
    let redis = TestRedis::start().await;
    let tracker = Arc::new(Tracker::default());
    let hench = redis
        .builder()
        .await
        .fetch_timeout(Duration::from_secs(2))
        .concurrency(10)
        .queues(["plain"])
        .capsule("limited", |capsule| {
            capsule.concurrency(3).queues(["paced"])
        })
        .queue_limits("paced", [concurrency(1)])
        .register(Tracked)
        .data_arc(tracker.clone())
        .build()
        .await
        .unwrap();
    let pool = hench.redis().clone();
    // One long job holds the slot: the other two keep cooling down and probing.
    burst(&pool, "paced", 0..3, 6_000).await;
    let launcher = hench.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let before = command_calls(&pool).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let after = command_calls(&pool).await;
    launcher.stop().await.unwrap();
    // The plain capsule's 10 workers block once each in 2 s (before: 70 times).
    let blocks = calls_between(&before, &after, "blmove");
    assert!(blocks <= 25, "{blocks} blocking calls");
}

/// Two capsules sharing a limited queue share its cooldowns, so they share its wake-ups: a
/// burst taken by the one capsule's watcher brings the other capsule's sleeping workers at
/// once, not at the end of their fetch timeout.
async fn capsules_sharing_a_queue_wake_each_other_with(fetch: FetchStrategy) {
    let redis = TestRedis::start().await;
    let tracker = Arc::new(Tracker::default());
    let hench = redis
        .builder()
        .await
        .fetch(fetch)
        .fetch_timeout(Duration::from_secs(2))
        .concurrency(5)
        .queues(["q", "side"])
        .capsule("solo", |capsule| capsule.concurrency(1).queues(["q"]))
        .queue_limits("q", [concurrency(5)])
        .queue_limits("side", [concurrency(10)])
        .register(Tracked)
        .data_arc(tracker.clone())
        .build()
        .await
        .unwrap();
    let pool = hench.redis().clone();
    // The default capsule's workers are busy at start, so the solo worker watches `q`; then
    // they sleep (`side` is empty and limited, `q` is watched).
    burst(&pool, "side", 100..105, 1_500).await;
    let launcher = hench.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(2_200)).await;
    let pushed = Instant::now();
    burst(&pool, "q", 0..6, 1_000).await;
    wait_for("the burst ran", Duration::from_secs(20), || async {
        tracker.finished_in("q") == 6
    })
    .await;
    launcher.stop().await.unwrap();
    let latencies: Vec<Duration> = tracker
        .times("q")
        .iter()
        .map(|at| at.duration_since(pushed))
        .collect();
    assert_eq!(tracker.order("q"), (0..6).collect::<Vec<_>>());
    assert!(tracker.most("q") <= 5);
    // Five slots, five idle workers: the first five start at once (before: one at once, the
    // others 1.3 s later).
    assert!(latencies[4] < Duration::from_millis(500), "{latencies:?}");
}

#[tokio::test]
async fn capsules_sharing_a_queue_wake_each_other() {
    capsules_sharing_a_queue_wake_each_other_with(FetchStrategy::Reliable).await;
}

#[tokio::test]
async fn capsules_sharing_a_queue_wake_each_other_with_basic_fetch() {
    capsules_sharing_a_queue_wake_each_other_with(FetchStrategy::Basic).await;
}

/// A rate-limited queue shared by two capsules keeps its pace when the capsule that probes it
/// takes a job elsewhere first: the probe is handed to a worker of the other capsule.
#[tokio::test]
async fn a_rate_limited_queue_shared_by_two_capsules_keeps_its_pace() {
    let redis = TestRedis::start().await;
    let tracker = Arc::new(Tracker::default());
    let hench = redis
        .builder()
        .await
        .fetch_timeout(Duration::from_secs(2))
        .concurrency(3)
        .queues(["q"])
        // Strict order: its workers look at `busy` first, and `busy` always has jobs.
        .capsule("mixed", |capsule| {
            capsule.concurrency(4).queues(["busy", "q"])
        })
        .queue_limits(
            "q",
            [Limit::Window {
                limit: 5,
                period: Duration::from_millis(500),
            }],
        )
        .register(Tracked)
        .data_arc(tracker.clone())
        .build()
        .await
        .unwrap();
    let pool = hench.redis().clone();
    burst(&pool, "busy", 0..20_000, 1).await;
    burst(&pool, "q", 0..200, 1).await;
    let launcher = hench.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let start = Instant::now();
    let from = tracker.finished_in("q");
    tokio::time::sleep(Duration::from_secs(4)).await;
    let ran = tracker.finished_in("q") - from;
    launcher.stop().await.unwrap();
    let end = start + Duration::from_secs(4);
    let mut times: Vec<Instant> = tracker
        .times("q")
        .into_iter()
        .filter(|at| *at >= start && *at <= end)
        .collect();
    times.push(end);
    let gap = times
        .windows(2)
        .map(|pair| pair[1].duration_since(pair[0]))
        .max()
        .unwrap_or_default();
    // The window allows 40 in 4 s (before: 25 to 33, with 2 s gaps).
    assert!(ran >= 35, "{ran} jobs in 4 s");
    assert!(gap < Duration::from_millis(700), "q left alone for {gap:?}");
}
