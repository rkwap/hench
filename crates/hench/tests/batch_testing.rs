//! Batches with the testing clients (`docs/design/batches.md` §2.11 and §3.7, T1–T3): a fake
//! client captures members and callbacks and drains them in hench's order; an inline client
//! runs members at the commit and the callbacks after them; `Client::batch_status` and the
//! cancellation checks read the in-memory table.

mod batch_support;
mod support;

use std::sync::Arc;

use batch_support::*;
use hench::batch::{BatchState, Workflow};
use hench::prelude::*;
use hench::testing::Testing;

/// Checks its batch from the inside: cancels it when its tag says so, then records what
/// `is_batch_cancelled` and `batch_status` answer.
#[derive(Default)]
struct Probe;

impl Job for Probe {
    const NAME: &'static str = "batch::Probe";
    type Args = (bool,);

    async fn perform(&self, ctx: &JobContext, (cancel,): Self::Args) -> Result<(), JobError> {
        let log = ctx.data::<Log>()?;
        let status = ctx
            .batch_status()
            .await?
            .ok_or_else(|| JobError::msg("a probe runs in a batch"))?;
        if cancel {
            ctx.client().cancel_batch(&status.bid).await?;
        }
        log.record(format!(
            "probe cancelled={} pending={}",
            ctx.is_batch_cancelled().await?,
            status.pending
        ));
        Ok(())
    }
}

/// Adds a child batch with one member to its own batch (a nested definition at run time).
#[derive(Default)]
struct Spawner;

impl Job for Spawner {
    const NAME: &'static str = "batch::Spawner";
    type Args = ();

    async fn perform(&self, ctx: &JobContext, (): ()) -> Result<(), JobError> {
        ctx.data::<Log>()?.record("spawner");
        let own = ctx
            .batch()
            .ok_or_else(|| JobError::msg("a spawner runs in a batch"))?;
        own.jobs(|jobs| async move {
            Batch::new(&jobs)
                .on_complete::<Callback>(named("spawned"))
                .jobs(|inner| async move {
                    Work::perform_async(&inner, work("grandchild", "ok")).await?;
                    Ok::<_, hench::Error>(())
                })
                .await?;
            Ok::<_, hench::Error>(())
        })
        .await?;
        Ok(())
    }
}

fn testing(log: &Arc<Log>) -> Testing {
    Testing::new()
        .register(Work)
        .register(Callback)
        .register(Probe)
        .register(Spawner)
        .data_arc(log.clone())
}

/// Drains a fake client to the end. A member skipped because its batch was cancelled is no
/// error: the testing executor counts the skip and goes on (`docs/design/batches.md` §2.11).
async fn drain(client: &Client) -> usize {
    match client.drain_all().await {
        Ok(drained) => drained,
        Err(error) => panic!("the drain failed: {error}"),
    }
}

/// T1: a fake client captures members with their `bid`; draining runs the members, then the
/// callbacks, a child's callbacks as members of its parent; a cancelled batch's members are
/// skipped.
#[tokio::test]
async fn fake_clients_capture_members_and_drain_callbacks() {
    let log = Arc::new(Log::default());
    let client = testing(&log).fake();
    let committed = Batch::new(&client)
        .on_complete::<Callback>(named("root"))
        .on_success::<Callback>(named("root"))
        .jobs(|jobs| async move {
            Work::perform_async(&jobs, work("m0", "ok")).await?;
            Work::perform_async(&jobs, work("m1", "ok")).await?;
            Batch::new(&jobs)
                .on_success::<Callback>(named("child"))
                .jobs(|inner| async move {
                    Work::perform_async(&inner, work("c0", "ok")).await?;
                    Ok::<_, hench::Error>(())
                })
                .await?;
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap();
    let child = committed.children[0].clone();
    let captured = client.jobs::<Work>();
    assert_eq!(captured.len(), 3);
    for job in &captured {
        let expected = if job.args[0] == "c0" {
            &child
        } else {
            &committed.bid
        };
        assert_eq!(job.bid.as_ref(), Some(expected));
    }
    let pending = status_of(&client, &committed.bid).await;
    assert_eq!(
        (pending.total, pending.pending, pending.children),
        (2, 2, 1)
    );
    assert_eq!(drain(&client).await, 6, "three members, three callbacks");
    let entries = log.entries();
    let at = |entry: &str| entries.iter().position(|e| e == entry).unwrap();
    assert!(at("child:success") > at("c0"));
    assert!(at("root:success") > at("child:success"), "{entries:?}");
    assert!(at("root:complete") > at("m1"));
    assert_eq!(log.count("root:success"), 1);
    let done = status_of(&client, &committed.bid).await;
    assert_eq!(done.state(), BatchState::Succeeded);
    // The child's callback was a member of the parent.
    assert_eq!(done.total, 3);
    assert!(client.all_jobs().is_empty());

    // Cancelled: the members are skipped, `complete` fires, `success` does not.
    let cancelled = Batch::new(&client)
        .on_complete::<Callback>(named("gone"))
        .on_success::<Callback>(named("gone"))
        .jobs(|jobs| async move {
            Work::perform_async(&jobs, work("never0", "ok")).await?;
            Work::perform_async(&jobs, work("never1", "ok")).await?;
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap();
    assert_eq!(client.cancel_batch(&cancelled.bid).await.unwrap(), 1);
    drain(&client).await;
    assert_eq!(log.count_prefix("never"), 0);
    assert_eq!(log.count("gone:complete"), 1);
    assert_eq!(log.count("gone:success"), 0);
    let status = status_of(&client, &cancelled.bid).await;
    assert_eq!((status.skipped, status.pending), (2, 0));
    assert_eq!(status.state(), BatchState::Cancelled);
}

/// T2: an inline client runs the members at the commit and the callbacks after them, nested
/// batches included; a failing member counts dead, its `death` callbacks run, and the commit
/// returns its error.
#[tokio::test]
async fn inline_clients_run_members_then_callbacks() {
    let log = Arc::new(Log::default());
    let client = testing(&log).inline();
    let committed = Batch::new(&client)
        .on_complete::<Callback>(named("root"))
        .on_success::<Callback>(named("root"))
        .jobs(|jobs| async move {
            Work::perform_async(&jobs, work("m0", "ok")).await?;
            Spawner::perform_async(&jobs, ()).await?;
            Batch::new(&jobs)
                .on_success::<Callback>(named("child"))
                .jobs(|inner| async move {
                    Work::perform_async(&inner, work("c0", "ok")).await?;
                    Ok::<_, hench::Error>(())
                })
                .await?;
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap();
    // Everything ran during the commit.
    let entries = log.entries();
    assert_eq!(log.count("root:success"), 1, "{entries:?}");
    let at = |entry: &str| entries.iter().position(|e| e == entry).unwrap();
    assert!(at("m0") < at("spawner"));
    assert!(at("grandchild") < at("spawned:complete"));
    assert!(at("c0") < at("child:success"));
    assert!(at("child:success") < at("root:success"));
    assert!(at("spawned:complete") < at("root:complete"));
    let status = status_of(&client, &committed.bid).await;
    assert_eq!(status.state(), BatchState::Succeeded);
    assert_eq!(status.children, 2);

    // A failing member.
    let failed = Batch::new(&client)
        .on_death::<Callback>(named("dead"))
        .on_success::<Callback>(named("never"))
        .jobs(|jobs| async move {
            Work::perform_async(&jobs, work("broken", "fail")).await?;
            Ok::<_, hench::Error>(())
        })
        .await;
    let error = failed.unwrap_err();
    assert!(error.to_string().contains("broken failed"), "{error}");
    assert_eq!(log.count("dead:death"), 1);
    assert_eq!(log.count("never:success"), 0);
}

/// T3: `batch_status` and the cancellation checks read the in-memory table of both testing
/// clients.
#[tokio::test]
async fn testing_clients_answer_status_and_cancellation() {
    for inline in [false, true] {
        let log = Arc::new(Log::default());
        let testing = testing(&log);
        let client = if inline {
            testing.inline()
        } else {
            testing.fake()
        };
        let committed = Batch::new(&client)
            .jobs(|jobs| async move {
                Probe::perform_async(&jobs, (false,)).await?;
                Probe::perform_async(&jobs, (true,)).await?;
                Ok::<_, hench::Error>(())
            })
            .await
            .unwrap();
        if !inline {
            assert_eq!(status_of(&client, &committed.bid).await.pending, 2);
            drain(&client).await;
        }
        assert_eq!(
            log.entries(),
            [
                "probe cancelled=false pending=2",
                "probe cancelled=true pending=1"
            ],
            "inline: {inline}"
        );
        let status = status_of(&client, &committed.bid).await;
        assert!(status.cancelled_at.is_some());
        // The second probe had started: it ran to its end and succeeded.
        assert_eq!((status.pending, status.skipped), (0, 0));
        assert!(
            client
                .batch_status("0123456789abcdef01234567")
                .await
                .unwrap()
                .is_none()
        );
    }
}

/// Workflows run with the testing clients too (`hench::WorkflowNext` is resolved by every
/// registry).
#[tokio::test]
async fn workflows_run_with_testing_clients() {
    for inline in [false, true] {
        let log = Arc::new(Log::default());
        let testing = testing(&log);
        let client = if inline {
            testing.inline()
        } else {
            testing.fake()
        };
        let started = Workflow::new(&client)
            .stage(|s| s.job::<Work>(work("a", "ok")))
            .stage(|s| s.job::<Work>(work("b", "ok")).job::<Work>(work("c", "ok")))
            .on_success::<Callback>(named("wf"))
            .start()
            .await
            .unwrap();
        if !inline {
            drain(&client).await;
        }
        let entries = log.entries();
        assert_eq!(
            entries.first().map(String::as_str),
            Some("a"),
            "{entries:?}"
        );
        assert_eq!(entries.last().map(String::as_str), Some("wf:success"));
        assert_eq!(log.count("b") + log.count("c"), 2);
        let status = status_of(&client, &started.bid).await;
        assert_eq!(status.state(), BatchState::Succeeded);
        assert_eq!(status.workflow_stages, Some(2));
    }
}

/// batches-5: an inline workflow runs each stage after the job that started it, not inside it,
/// so a workflow of the largest number of stages (100) runs on a test's default stack: nesting
/// every stage in the one before overflowed it from the sixth stage on, aborting the whole test
/// binary.
#[tokio::test]
async fn inline_workflows_of_many_stages_run_in_a_loop() {
    let log = Arc::new(Log::default());
    let client = Testing::new()
        .register(Work)
        .register(Callback)
        .data_arc(log.clone())
        .inline();
    let mut workflow = Workflow::new(&client);
    for stage in 0..100 {
        workflow = workflow.stage(move |s| s.job::<Work>(work(&format!("s{stage}"), "ok")));
    }
    let started = workflow
        .on_success::<Callback>(named("wf"))
        .start()
        .await
        .unwrap();
    assert_eq!(log.count("wf:success"), 1);
    // Every stage ran, in order.
    let positions: Vec<usize> = (0..100)
        .map(|stage| log.position(&format!("s{stage}")).unwrap())
        .collect();
    assert!(
        positions.windows(2).all(|pair| pair[0] < pair[1]),
        "{positions:?}"
    );
    assert!(log.position("wf:success").unwrap() > positions[99]);
    let status = client.batch_status(&started.bid).await.unwrap().unwrap();
    assert_eq!(status.state(), BatchState::Succeeded);
}

/// batches-1: a workflow's `complete` waits for its last stage. A dead member stops the
/// workflow at its stage: `death` fires, `complete` does not (it fired at once before, the
/// stages defined so far having all "run"); with every stage run, `complete` fires once, after
/// the last stage's jobs.
#[tokio::test]
async fn workflow_complete_waits_for_the_last_stage() {
    let log = Arc::new(Log::default());
    let client = testing(&log).inline();
    let stuck = Workflow::new(&client)
        .stage(|s| s.job::<Work>(work("a", "fail")))
        .stage(|s| s.job::<Work>(work("b", "ok")))
        .on_complete::<Callback>(named("stuck"))
        .on_death::<Callback>(named("stuck"))
        .start()
        .await;
    assert!(stuck.is_err());
    assert_eq!(log.count("stuck:death"), 1, "{:?}", log.entries());
    assert_eq!(log.count("stuck:complete"), 0, "{:?}", log.entries());
    assert_eq!(log.count("b"), 0);

    for inline in [false, true] {
        let log = Arc::new(Log::default());
        let testing = testing(&log);
        let client = if inline {
            testing.inline()
        } else {
            testing.fake()
        };
        let started = Workflow::new(&client)
            .stage(|s| s.job::<Work>(work("a", "ok")))
            .stage(|s| s.job::<Work>(work("b", "ok")))
            .stage(|s| s.job::<Work>(work("c", "ok")))
            .on_complete::<Callback>(named("wf"))
            .on_success::<Callback>(named("wf"))
            .start()
            .await
            .unwrap();
        if !inline {
            drain(&client).await;
        }
        let entries = log.entries();
        assert_eq!(log.count("wf:complete"), 1, "{entries:?}");
        assert!(
            log.position("c").unwrap() < log.position("wf:complete").unwrap(),
            "{entries:?}"
        );
        let seen = log.status_seen("wf").unwrap();
        assert_eq!(seen.children, 3, "the callback saw every stage");
        let status = status_of(&client, &started.bid).await;
        assert_eq!(status.state(), BatchState::Succeeded);
    }
}

/// batches-6: a workflow started with a chosen bid and commit id starts once, however often
/// the job starting it runs.
#[tokio::test]
async fn workflows_with_a_chosen_bid_start_once() {
    let log = Arc::new(Log::default());
    let client = testing(&log).fake();
    let bid = "0000000000000000000f10a7";
    for round in 0..2 {
        let started = Workflow::with_bid(&client, bid)
            .unwrap()
            .commit_id("starter-jid")
            .stage(|s| s.job::<Work>(work("charge", "ok")))
            .stage(|s| s.job::<Work>(work("ship", "ok")))
            .start()
            .await
            .unwrap();
        assert_eq!(started.bid, bid);
        assert_eq!(started.already_applied, round == 1);
    }
    assert_eq!(client.jobs::<Work>().len(), 1);
    drain(&client).await;
    assert_eq!((log.count("charge"), log.count("ship")), (1, 1));
    assert!(matches!(
        Workflow::with_bid(&client, "not a bid"),
        Err(hench::Error::Batch(
            hench::batch::BatchError::InvalidBid { .. }
        ))
    ));
}
