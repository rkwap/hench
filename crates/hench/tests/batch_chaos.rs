//! Batches under concurrency and failures (`docs/design/batches.md` §3.5, C1–C9): many
//! processes finishing the members of the same batches, crashes, duplicate copies, lost
//! replies, Redis restarts with and without persistence, `OOM`, and races between a batch's
//! last member and a reopen, a cancel or another dispatcher. (C10, the soak scenario, belongs to
//! the soak harness.)

mod batch_support;
mod support;

use std::sync::Arc;
use std::time::Duration;

use batch_support::*;
use hench::batch::{BatchError, BatchState};
use hench::prelude::*;
use hench::test_util::TestRedis;
use hench::{RedisConfig, keys};
use support::*;

/// A longer wait, for the tests that run thousands of jobs.
const LONG: Duration = Duration::from_secs(90);

/// C1: 4 processes × 8 workers finish 10 000 members of 10 batches: each event fires once
/// (the minute counters) and each callback runs once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_event_fires_once_across_processes() {
    const BATCHES: usize = 10;
    const MEMBERS: usize = 1_000;
    let redis = TestRedis::start().await;
    let log = Arc::new(Log::default());
    let client = redis.client().await;
    let mut bids = Vec::new();
    for b in 0..BATCHES {
        let committed = Batch::new(&client)
            .on_complete::<Callback>(named(&format!("b{b}")))
            .on_success::<Callback>(named(&format!("b{b}")))
            .jobs(|jobs| async move {
                let args = (0..MEMBERS).map(|_| work("m", "ok")).collect();
                Work::perform_bulk(&jobs, args).await?;
                Ok::<_, hench::Error>(())
            })
            .await
            .unwrap();
        bids.push(committed.bid);
    }
    let mut launchers = Vec::new();
    for _ in 0..4 {
        launchers.push(worker(&redis, &log, |builder| builder.concurrency(8)).await);
    }
    wait_for("every callback", LONG, || async {
        (0..BATCHES).all(|b| log.count(&format!("b{b}:success")) >= 1)
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(log.count("m"), BATCHES * MEMBERS);
    for b in 0..BATCHES {
        assert_eq!(log.count(&format!("b{b}:success")), 1, "b{b}");
        assert_eq!(log.count(&format!("b{b}:complete")), 1, "b{b}");
    }
    let pool = redis.pool().await;
    for bid in &bids {
        let status = status_of(&client, bid).await;
        assert_eq!(status.total, MEMBERS as u64);
        assert_eq!(counts(&status), (0, 0, 0, 0));
    }
    // Each event fired once: the minute counters count every firing.
    let events = batches(&client)
        .events(hench::metrics::Period::Minutes(5))
        .await
        .unwrap();
    assert_eq!((events.complete, events.success), (10, 10));
    assert_eq!(zcard(&pool, keys::BATCH_EVENTS).await, 0);
    for (_, launcher) in launchers {
        launcher.stop().await.unwrap();
    }
}

/// C2: a process dies mid-member (reliable fetch); orphan recovery runs it again elsewhere and
/// it is counted once.
#[tokio::test]
async fn a_member_of_a_crashed_process_is_counted_once() {
    let redis = TestRedis::start().await;
    let log = Arc::new(Log::default());
    let pool = redis.pool().await;
    let (hench, crashed) = worker(&redis, &log, |builder| builder).await;
    let client = hench.client().clone();
    let committed = Batch::new(&client)
        .on_success::<Callback>(named("ok"))
        .jobs(|jobs| async move {
            Work::perform_async(&jobs, work("x", "hang-once")).await?;
            Work::perform_async(&jobs, work("y", "ok")).await?;
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap();
    log.wait_count("x", 1).await;
    log.wait_count("y", 1).await;
    crashed.abandon();
    // Its heartbeat expires.
    del(&pool, crashed.identity()).await;
    let (_hench, survivor) = worker(&redis, &log, |builder| builder).await;
    log.wait_count("ok:success", 1).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(log.count("ok:success"), 1);
    assert_eq!(log.count("x"), 2);
    let status = status_of(&client, &committed.bid).await;
    assert_eq!((status.total, status.pending, status.failures), (2, 0, 0));
    survivor.stop().await.unwrap();
}

/// C3: two copies of one member (one jid): it is counted once, and the failure of the stale
/// copy is ignored.
#[tokio::test]
async fn a_stale_copy_of_a_member_is_ignored() {
    let redis = TestRedis::start().await;
    let log = Arc::new(Log::default());
    let pool = redis.pool().await;
    let client = redis.client().await;
    let committed = Batch::new(&client)
        .on_success::<Callback>(named("ok"))
        .on_death::<Callback>(named("dead"))
        .jobs(|jobs| async move {
            Work::set()
                .retry(0u32)
                .perform_async(&jobs, work("twin", "second-fails"))
                .await?;
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap();
    // A second copy of the same job (as a recovered or duplicated push would leave).
    let raw = lrange(&pool, &keys::queue("default")).await.remove(0);
    push_raw(&pool, "default", &raw).await;
    assert_eq!(queue_size(&pool, "default").await, 2);
    let (_hench, launcher) = worker(&redis, &log, |builder| builder.concurrency(1)).await;
    wait_for("both copies", WAIT, || async {
        log.count("twin") == 2 && zcard(&pool, keys::DEAD).await == 1
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(log.count("ok:success"), 1);
    assert_eq!(
        log.count("dead:death"),
        0,
        "the stale copy's death is ignored"
    );
    let status = status_of(&client, &committed.bid).await;
    assert_eq!(counts(&status), (0, 0, 0, 0));
    assert_eq!(status.state(), BatchState::Succeeded);
    launcher.stop().await.unwrap();
}

/// Runs `scenario` with a worker whose connections go through a proxy that loses every reply
/// containing `marker` (armed again every 10 ms): 20 batches of 5 members; the counts are exact
/// and each callback runs once.
async fn with_lost_replies(marker: &'static [u8]) {
    init_logging();
    let redis = TestRedis::start().await;
    let log = Arc::new(Log::default());
    let pool = redis.pool().await;
    let client = redis.client().await;
    let proxy = Arc::new(ReplyDropper::start(redis.socket_path(), marker).await);
    let rearm = {
        let proxy = proxy.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(10)).await;
                proxy.arm();
            }
        })
    };
    let (_hench, launcher) = worker(&redis, &log, |builder| {
        builder.redis_config(RedisConfig::new(proxy.url()))
    })
    .await;
    let mut bids = Vec::new();
    for b in 0..20 {
        let committed = Batch::new(&client)
            .on_complete::<Callback>(named(&format!("b{b}")))
            .on_success::<Callback>(named(&format!("b{b}")))
            .jobs(|jobs| async move {
                let args = (0..5).map(|_| work("m", "ok")).collect();
                Work::perform_bulk(&jobs, args).await?;
                Ok::<_, hench::Error>(())
            })
            .await
            .unwrap();
        bids.push(committed.bid);
    }
    wait_for("every callback", LONG, || async {
        (0..20).all(|b| {
            log.count(&format!("b{b}:success")) >= 1 && log.count(&format!("b{b}:complete")) >= 1
        })
    })
    .await;
    rearm.abort();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(proxy.dropped() >= 1, "no reply was lost");
    for b in 0..20 {
        assert_eq!(log.count(&format!("b{b}:success")), 1, "b{b}");
        assert_eq!(log.count(&format!("b{b}:complete")), 1, "b{b}");
    }
    for bid in &bids {
        let status = status_of(&client, bid).await;
        assert_eq!((status.total, status.pending, status.failures), (5, 0, 0));
    }
    assert_eq!(zcard(&pool, keys::BATCH_EVENTS).await, 0);
    launcher.stop().await.unwrap();
}

/// C4: the replies of the accounting scripts that fire `success` are lost: each is sent again
/// (a no-op), and the dispatch follows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_accounting_replies_count_once() {
    with_lost_replies(b"$7\r\nsuccess\r\n").await;
}

/// C4: the replies of the dispatches of `complete` and `success` are lost: the callbacks were
/// pushed once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_dispatch_replies_push_callbacks_once() {
    with_lost_replies(b"complete,success").await;
}

/// C5: Redis restarts with its append-only file mid-batch: the batch completes, once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_with_persistence_finishes_the_batch() {
    let mut redis =
        TestRedis::start_with_args(&["--appendonly", "yes", "--appendfsync", "always"]).await;
    let log = Arc::new(Log::default());
    let gate = Arc::new(Gate::default());
    let client = redis.client().await;
    let committed = Batch::new(&client)
        .on_success::<Callback>(named("ok"))
        .jobs(|jobs| async move {
            let args = (0..20).map(|_| work("m", "gate")).collect();
            Work::perform_bulk(&jobs, args).await?;
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap();
    let (_hench, launcher) = worker(&redis, &log, |builder| builder.data_arc(gate.clone())).await;
    wait_for("members at the gate", WAIT, || async {
        gate.entered() >= 5
    })
    .await;
    redis.restart().await;
    gate.open();
    log.wait_count("ok:success", 1).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(log.count("ok:success"), 1);
    let client = redis.client().await;
    let status = status_of(&client, &committed.bid).await;
    assert_eq!((status.total, status.pending, status.failures), (20, 0, 0));
    launcher.stop().await.unwrap();
}

/// C5: Redis restarts without persistence: the batch and its members are gone; the members
/// that were running finish without errors or retry loops.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_without_persistence_loses_the_batch_quietly() {
    let mut redis = TestRedis::start().await;
    let log = Arc::new(Log::default());
    let gate = Arc::new(Gate::default());
    let errors = Errors::default();
    let client = redis.client().await;
    let committed = Batch::new(&client)
        .on_success::<Callback>(named("ok"))
        .jobs(|jobs| async move {
            let args = (0..3).map(|_| work("m", "gate")).collect();
            Work::perform_bulk(&jobs, args).await?;
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap();
    let handler = errors.handler();
    let (_hench, launcher) = worker(&redis, &log, |builder| {
        builder.data_arc(gate.clone()).error_handler(handler)
    })
    .await;
    wait_for("members at the gate", WAIT, || async {
        gate.entered() == 3
    })
    .await;
    redis.restart().await;
    gate.open();
    tokio::time::sleep(Duration::from_secs(1)).await;
    let client = redis.client().await;
    assert!(client.batch_status(&committed.bid).await.unwrap().is_none());
    assert_eq!(log.count("ok:success"), 0);
    assert!(
        errors
            .with_context("could not record a job in its batch")
            .is_empty(),
        "{:?}",
        errors.all()
    );
    launcher.stop().await.unwrap();
}

/// C6: under `OOM` successes are counted, a failure waits until memory is freed, and a commit
/// is refused with nothing written.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn out_of_memory_counts_successes_and_holds_failures() {
    let redis = TestRedis::start().await;
    let log = Arc::new(Log::default());
    let gate = Arc::new(Gate::default());
    let pool = redis.pool().await;
    let client = redis.client().await;
    let committed = Batch::new(&client)
        .on_complete::<Callback>(named("done"))
        .jobs(|jobs| async move {
            Work::perform_async(&jobs, work("g0", "gate")).await?;
            Work::perform_async(&jobs, work("g1", "gate")).await?;
            Work::perform_async(&jobs, work("f", "gate-fail")).await?;
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap();
    let (_hench, launcher) = worker(&redis, &log, |builder| builder.data_arc(gate.clone())).await;
    wait_for("members at the gate", WAIT, || async {
        gate.entered() == 3
    })
    .await;
    cmd::<()>(&pool, &redis_cmd!("CONFIG", "SET", "maxmemory", "1")).await;
    let before: u64 = cmd(&pool, &redis_cmd!("DBSIZE")).await;
    let refused = Batch::new(&client)
        .jobs(|jobs| async move {
            Work::perform_async(&jobs, work("late", "ok")).await?;
            Ok::<_, hench::Error>(())
        })
        .await;
    assert!(refused.is_err(), "{refused:?}");
    assert_eq!(cmd::<u64>(&pool, &redis_cmd!("DBSIZE")).await, before);
    gate.open();
    wait_status(&client, &committed.bid, "the successes", |status| {
        status.pending == 1
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let held = status_of(&client, &committed.bid).await;
    assert_eq!(counts(&held), (1, 0, 0, 0), "the failure waits for memory");
    assert_eq!(log.count("done:complete"), 0);
    cmd::<()>(&pool, &redis_cmd!("CONFIG", "SET", "maxmemory", "0")).await;
    log.wait_count("done:complete", 1).await;
    let status = status_of(&client, &committed.bid).await;
    assert_eq!(counts(&status), (1, 1, 0, 0));
    launcher.stop().await.unwrap();
}

/// C7: reopening races the last member's success, 1 000 times: each time either the reopen is
/// refused (`Succeeded`, the batch has its one member) or its member is registered and
/// `success` waits for it; never both.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reopening_races_the_last_success() {
    const ROUNDS: usize = 1_000;
    let redis = TestRedis::start().await;
    let log = Arc::new(Log::default());
    let (hench, launcher) = worker(&redis, &log, |builder| builder.concurrency(10)).await;
    let client = hench.client().clone();
    let mut rounds = Vec::new();
    let (mut refused, mut added) = (0, 0);
    for round in 0..ROUNDS {
        let name = format!("r{round}");
        let committed = Batch::new(&client)
            .on_success::<Callback>(named(&name))
            .jobs(|jobs| async move {
                Work::perform_async(&jobs, work("first", "ok")).await?;
                Ok::<_, hench::Error>(())
            })
            .await
            .unwrap();
        if round % 3 > 0 {
            tokio::time::sleep(Duration::from_micros(fastrand::u64(0..2_000))).await;
        }
        let reopened = Batch::open(&client, &committed.bid)
            .unwrap()
            .jobs(|jobs| async move {
                Work::perform_async(&jobs, work("second", "ok")).await?;
                Ok::<_, hench::Error>(())
            })
            .await;
        let total = match reopened {
            Ok(_) => {
                added += 1;
                2
            }
            Err(hench::Error::Batch(BatchError::Succeeded { .. })) => {
                refused += 1;
                1
            }
            Err(other) => panic!("round {round}: {other}"),
        };
        rounds.push((name, committed.bid, total));
    }
    wait_for("every success", LONG, || async {
        rounds
            .iter()
            .all(|(name, _, _)| log.count(&format!("{name}:success")) >= 1)
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    for (name, bid, total) in &rounds {
        assert_eq!(log.count(&format!("{name}:success")), 1, "{name}");
        // `success` saw every member succeed: the one added too, when the reopen won.
        let seen = log.status_seen(name).unwrap();
        assert_eq!((seen.total, seen.pending), (*total, 0), "{name} ({bid})");
    }
    eprintln!("reopens refused: {refused}, reopens added: {added}");
    launcher.stop().await.unwrap();
}

/// C8: cancelling races the last success: either `success` fired and the cancel is refused
/// (`Succeeded`), or the batch is cancelled and `success` never fires; `complete` fires once
/// either way.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelling_races_the_last_success() {
    const ROUNDS: usize = 300;
    let redis = TestRedis::start().await;
    let log = Arc::new(Log::default());
    let (hench, launcher) = worker(&redis, &log, |builder| builder.concurrency(10)).await;
    let client = hench.client().clone();
    let mut rounds = Vec::new();
    let (mut cancelled, mut succeeded) = (0, 0);
    for round in 0..ROUNDS {
        let name = format!("r{round}");
        let committed = Batch::new(&client)
            .on_complete::<Callback>(named(&name))
            .on_success::<Callback>(named(&name))
            .jobs(|jobs| async move {
                Work::perform_async(&jobs, work("m", "ok")).await?;
                Ok::<_, hench::Error>(())
            })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_micros(fastrand::u64(0..3_000))).await;
        let won = match client.cancel_batch(&committed.bid).await {
            Ok(1) => {
                cancelled += 1;
                true
            }
            Err(hench::Error::Batch(BatchError::Succeeded { .. })) => {
                succeeded += 1;
                false
            }
            other => panic!("round {round}: {other:?}"),
        };
        rounds.push((name, committed.bid, won));
    }
    wait_for("every complete", LONG, || async {
        rounds
            .iter()
            .all(|(name, _, _)| log.count(&format!("{name}:complete")) >= 1)
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    for (name, bid, cancel_won) in &rounds {
        let status = status_of(&client, bid).await;
        assert_eq!(log.count(&format!("{name}:complete")), 1, "{name}");
        if *cancel_won {
            assert!(
                status.cancelled_at.is_some() && status.success_at.is_none(),
                "{name}"
            );
            assert_eq!(log.count(&format!("{name}:success")), 0, "{name}");
        } else {
            assert!(
                status.cancelled_at.is_none() && status.success_at.is_some(),
                "{name}"
            );
            assert_eq!(log.count(&format!("{name}:success")), 1, "{name}");
        }
    }
    eprintln!("cancels won: {cancelled}, successes won: {succeeded}");
    launcher.stop().await.unwrap();
}

/// C9: processors and maintenance (every millisecond, no grace) dispatch the same events at
/// the same time: each callback is pushed once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_dispatchers_push_callbacks_once() {
    const BATCHES: usize = 200;
    let redis = TestRedis::start().await;
    let log = Arc::new(Log::default());
    let client = redis.client().await;
    let racing = |builder: hench::HenchBuilder| {
        builder
            .concurrency(8)
            .batch_maintenance_interval(Duration::from_millis(1))
            .batch_dispatch_grace(Duration::from_millis(1))
    };
    let first = worker(&redis, &log, racing).await;
    let second = worker(&redis, &log, racing).await;
    for b in 0..BATCHES {
        Batch::new(&client)
            .on_complete::<Callback>(named(&format!("b{b}")))
            .on_success::<Callback>(named(&format!("b{b}")))
            .jobs(|jobs| async move {
                let args = (0..3).map(|_| work("m", "ok")).collect();
                Work::perform_bulk(&jobs, args).await?;
                Ok::<_, hench::Error>(())
            })
            .await
            .unwrap();
    }
    wait_for("every callback", LONG, || async {
        (0..BATCHES).all(|b| log.count(&format!("b{b}:success")) >= 1)
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    for b in 0..BATCHES {
        assert_eq!(log.count(&format!("b{b}:success")), 1, "b{b}");
        assert_eq!(log.count(&format!("b{b}:complete")), 1, "b{b}");
    }
    first.1.stop().await.unwrap();
    second.1.stop().await.unwrap();
}
