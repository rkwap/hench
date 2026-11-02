//! Defining and committing batches against a private Redis (`docs/design/batches.md` §3.3,
//! D1–D14, and the callback templates of U4): atomic definitions, client middleware, nesting,
//! reopening, commit ids, lost replies, unconfirmed commits, limits, scheduled members, empty
//! batches and unique members.

mod batch_support;
mod support;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use batch_support::*;
use hench::batch::{BatchDefaults, BatchError, BatchEvent, Committed};
use hench::prelude::*;
use hench::test_util::TestRedis;
use hench::{RedisConfig, keys, now_secs_f64};
use serde_json::{Value, json};
use support::*;

/// An error of the application, which a definition's closure returns as is.
#[derive(Debug, PartialEq)]
enum AppError {
    Stop(&'static str),
    Hench(String),
}

impl From<hench::Error> for AppError {
    fn from(error: hench::Error) -> Self {
        AppError::Hench(error.to_string())
    }
}

/// `DBSIZE`: nothing at all was written when it is 0.
async fn dbsize(pool: &RedisPool) -> u64 {
    cmd(pool, &redis_cmd!("DBSIZE")).await
}

fn batch_error(error: hench::Error) -> BatchError {
    match error {
        hench::Error::Batch(error) => error,
        other => panic!("not a batch error: {other:?}"),
    }
}

/// D1: an error from the definition's closure writes nothing and is returned unchanged.
#[tokio::test]
async fn a_failing_definition_writes_nothing() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let result: Result<Committed<()>, AppError> = Batch::new(&client)
        .description("never written")
        .on_success::<Callback>(named("never"))
        .jobs(|jobs| async move {
            Work::perform_async(&jobs, work("a", "ok")).await?;
            Work::perform_async(&jobs, work("b", "ok")).await?;
            Err(AppError::Stop("the loader gave up"))
        })
        .await;
    assert_eq!(result.unwrap_err(), AppError::Stop("the loader gave up"));
    assert_eq!(dbsize(&pool).await, 0, "nothing at all was written");
}

/// Stops the push of jobs whose first argument is `skip`, and records the `bid` of every job
/// it sees.
#[derive(Clone, Default)]
struct Halting(Arc<Mutex<Vec<Option<String>>>>);

#[async_trait]
impl ClientMiddleware for Halting {
    async fn call(
        &self,
        job: JobPayload,
        redis: &RedisPool,
        next: ClientNext<'_>,
    ) -> hench::Result<Option<JobPayload>> {
        self.0.lock().unwrap().push(job.bid.clone());
        if job.args.first() == Some(&json!("skip")) {
            return Ok(None);
        }
        let _ = redis;
        next.run(job).await
    }
}

/// D2, D3: client middleware runs for member pushes, sees their `bid`, and a push it stops
/// is neither pushed nor registered.
#[tokio::test]
async fn client_middleware_sees_members_and_can_stop_them() {
    let redis = TestRedis::start().await;
    let halting = Halting::default();
    let middleware = halting.clone();
    let client = redis.client().await.with_middleware(|chain| {
        chain.add(middleware);
    });
    let pool = redis.pool().await;
    let committed = Batch::new(&client)
        .jobs(|jobs| async move {
            let first = Work::perform_async(&jobs, work("a", "ok")).await?;
            let stopped = Work::perform_async(&jobs, work("skip", "ok")).await?;
            let last = Work::perform_async(&jobs, work("c", "ok")).await?;
            assert!(stopped.is_none(), "the middleware stopped this push");
            Ok::<_, hench::Error>(vec![first.unwrap(), last.unwrap()])
        })
        .await
        .unwrap();
    assert_eq!(committed.jids, committed.value);
    let seen = halting.0.lock().unwrap().clone();
    assert_eq!(seen, vec![Some(committed.bid.clone()); 3]);
    let status = status_of(&client, &committed.bid).await;
    assert_eq!((status.total, status.pending), (2, 2));
    let queued: Vec<String> = queue_jobs(&pool, "default")
        .await
        .into_iter()
        .map(|job| job.jid)
        .collect();
    assert_eq!(queued.len(), 2);
    for jid in &committed.jids {
        assert!(queued.contains(jid));
    }
    for job in queue_jobs(&pool, "default").await {
        assert_eq!(job.bid.as_deref(), Some(committed.bid.as_str()));
    }
}

/// D4: `jobs` returns the jids in push order, through `perform_async`, `perform_bulk` and
/// `client.job::<J>()`.
#[tokio::test]
async fn commits_report_their_jids_in_push_order() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let committed = Batch::new(&client)
        .jobs(|jobs| async move {
            let mut pushed = Vec::new();
            pushed.push(
                Work::perform_async(&jobs, work("one", "ok"))
                    .await?
                    .unwrap(),
            );
            let bulk =
                Work::perform_bulk(&jobs, vec![work("two", "ok"), work("three", "ok")]).await?;
            pushed.extend(bulk.into_iter().flatten());
            pushed.push(
                jobs.job::<Work>()
                    .queue("low")
                    .perform_async(work("four", "ok"))
                    .await?
                    .unwrap(),
            );
            Ok::<_, hench::Error>(pushed)
        })
        .await
        .unwrap();
    assert_eq!(committed.jids.len(), 4);
    assert_eq!(committed.jids, committed.value);
    assert!(!committed.already_applied);
    assert!(committed.duplicates.is_empty() && committed.children.is_empty());
    let pool = redis.pool().await;
    assert_eq!(queue_size(&pool, "default").await, 3);
    assert_eq!(queue_size(&pool, "low").await, 1);
    let mut pending = smembers(&pool, &keys::batch_pending(&committed.bid)).await;
    pending.sort();
    let mut jids = committed.jids.clone();
    jids.sort();
    assert_eq!(pending, jids);
}

/// D5: a batch defined inside another's definition is its child, written by the one commit.
#[tokio::test]
async fn nested_definitions_are_one_commit() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let committed = Batch::new(&client)
        .description("parent")
        .on_complete::<Callback>(named("parent"))
        .jobs(|jobs| async move {
            Work::perform_async(&jobs, work("p1", "ok")).await?;
            let child = Batch::new(&jobs)
                .description("child")
                .on_success::<Callback>(named("child"));
            let child_bid = child.bid().to_owned();
            let staged = child
                .jobs(|inner| async move {
                    Work::perform_async(&inner, work("c1", "ok")).await?;
                    Work::perform_async(&inner, work("c2", "ok")).await?;
                    Ok::<_, hench::Error>(())
                })
                .await?;
            // A child's commit only stages it: nothing exists before the root's commit.
            assert_eq!(staged.bid, child_bid);
            Ok::<_, hench::Error>(child_bid)
        })
        .await
        .unwrap();
    let child = committed.value.clone();
    assert_eq!(committed.children, vec![child.clone()]);
    assert_eq!(committed.jids.len(), 3);
    let parent_hash = hgetall(&pool, &keys::batch(&committed.bid)).await;
    let child_hash = hgetall(&pool, &keys::batch(&child)).await;
    assert_eq!(child_hash["parent"], committed.bid);
    assert!(!parent_hash.contains_key("parent"));
    assert_eq!(parent_hash["kids"], "1");
    assert_eq!(
        smembers(&pool, &keys::batch_kids(&committed.bid)).await,
        vec![child.clone()]
    );
    let parent = status_of(&client, &committed.bid).await;
    assert_eq!((parent.total, parent.children), (1, 1));
    let child_status = status_of(&client, &child).await;
    assert_eq!(child_status.total, 2);
    assert_eq!(child_status.parent.as_deref(), Some(committed.bid.as_str()));
    // Every member names its own batch.
    for job in queue_jobs(&pool, "default").await {
        let tag = job.args[0].as_str().unwrap().to_owned();
        let expected = if tag.starts_with('p') {
            &committed.bid
        } else {
            &child
        };
        assert_eq!(job.bid.as_ref(), Some(expected), "{tag}");
    }
}

/// D5: a child dropped without its commit is left out of its root's commit, with its members.
#[tokio::test]
async fn a_child_dropped_without_commit_is_left_out() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let committed = Batch::new(&client)
        .jobs(|jobs| async move {
            Work::perform_async(&jobs, work("kept", "ok")).await?;
            let child = Batch::new(&jobs);
            Work::perform_async(&child.client(), work("dropped", "ok")).await?;
            drop(child);
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap();
    assert!(committed.children.is_empty());
    assert_eq!(committed.jids.len(), 1);
    assert_eq!(queue_size(&pool, "default").await, 1);
    assert_eq!(status_of(&client, &committed.bid).await.children, 0);
}

/// D6: reopening adds members; a succeeded, cancelled, missing or malformed batch is refused.
#[tokio::test]
async fn reopening_follows_the_rules() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let first = Batch::new(&client)
        .jobs(|jobs| async move {
            Work::perform_async(&jobs, work("a", "ok")).await?;
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap();
    let reopened = Batch::open(&client, &first.bid)
        .unwrap()
        .jobs(|jobs| async move {
            Work::perform_async(&jobs, work("b", "ok")).await?;
            Work::perform_async(&jobs, work("c", "ok")).await?;
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap();
    assert_eq!(reopened.bid, first.bid);
    assert_eq!(reopened.jids.len(), 2);
    let after = status_of(&client, &first.bid).await;
    assert_eq!((after.total, after.pending), (3, 3));
    assert_eq!(queue_size(&pool, "default").await, 3);

    // An empty batch succeeds at its commit: it cannot be reopened.
    let empty = Batch::new(&client).commit().await.unwrap();
    let refused = Batch::open(&client, &empty.bid).unwrap().commit().await;
    assert_eq!(
        batch_error(refused.unwrap_err()),
        BatchError::Succeeded {
            bid: empty.bid.clone()
        }
    );

    // A cancelled batch.
    assert_eq!(client.cancel_batch(&first.bid).await.unwrap(), 1);
    let refused = Batch::open(&client, &first.bid)
        .unwrap()
        .jobs(|jobs| async move {
            Work::perform_async(&jobs, work("late", "ok")).await?;
            Ok::<_, hench::Error>(())
        })
        .await;
    assert_eq!(
        batch_error(refused.unwrap_err()),
        BatchError::Cancelled {
            bid: first.bid.clone()
        }
    );
    assert_eq!(queue_size(&pool, "default").await, 3, "nothing was added");

    // A batch that never existed.
    let missing = "0123456789abcdef01234567";
    let refused = Batch::open(&client, missing).unwrap().commit().await;
    assert_eq!(
        batch_error(refused.unwrap_err()),
        BatchError::NotFound {
            bid: missing.to_owned()
        }
    );
    // Not a batch id.
    for malformed in [
        "XYZ",
        "0123456789ABCDEF01234567",
        "0123456789abcdef0123456",
        "",
    ] {
        let refused = Batch::open(&client, malformed);
        assert!(
            matches!(
                refused.as_ref().map(|_| ()).map_err(|error| match error {
                    hench::Error::Batch(BatchError::InvalidBid { bid }) => bid.as_str(),
                    _ => "other",
                }),
                Err(bid) if bid == malformed
            ),
            "{malformed}"
        );
    }
    // A reopened batch only gains members and children.
    let refused = Batch::open(&client, &reopened.bid)
        .unwrap()
        .description("renamed")
        .commit()
        .await;
    assert!(matches!(refused, Err(hench::Error::InvalidJob(_))));
}

/// D7: a commit id applied before writes nothing.
#[tokio::test]
async fn a_commit_id_applies_once() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let root = Batch::new(&client)
        .jobs(|jobs| async move {
            Work::perform_async(&jobs, work("a", "ok")).await?;
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap();
    for round in 0..3 {
        let committed = Batch::open(&client, &root.bid)
            .unwrap()
            .commit_id("loader-7")
            .jobs(|jobs| async move {
                Work::perform_async(&jobs, work("loaded", "ok")).await?;
                Work::perform_async(&jobs, work("loaded", "ok")).await?;
                Ok::<_, hench::Error>(())
            })
            .await
            .unwrap();
        assert_eq!(committed.already_applied, round > 0, "round {round}");
        assert_eq!(committed.jids.len(), if round == 0 { 2 } else { 0 });
    }
    assert_eq!(queue_size(&pool, "default").await, 3);
    assert_eq!(status_of(&client, &root.bid).await.total, 3);
}

/// `with_bid` and the creating job's jid as commit id: a re-run writes nothing; a bid taken by
/// another commit is refused.
#[tokio::test]
async fn idempotent_creation_with_a_chosen_bid() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let bid = "00000000000000000000beef";
    for round in 0..2 {
        let committed = Batch::with_bid(&client, bid)
            .unwrap()
            .commit_id("creator-jid")
            .on_success::<Callback>(named("done"))
            .jobs(|jobs| async move {
                Work::perform_async(&jobs, work("a", "ok")).await?;
                Ok::<_, hench::Error>(())
            })
            .await
            .unwrap();
        assert_eq!(committed.bid, bid);
        assert_eq!(committed.already_applied, round == 1);
    }
    let refused = Batch::with_bid(&client, bid)
        .unwrap()
        .commit_id("another")
        .commit()
        .await;
    assert_eq!(
        batch_error(refused.unwrap_err()),
        BatchError::Exists {
            bid: bid.to_owned()
        }
    );
    assert_eq!(queue_size(&redis.pool().await, "default").await, 1);
}

/// The reply of a successful commit (`{'ok', ...}`).
const COMMIT_REPLY: &[u8] = b"*4\r\n$2\r\nok\r\n";

/// D8: the reply of a commit is lost: it is sent again and applied once.
#[tokio::test]
async fn a_lost_commit_reply_is_resent_and_applied_once() {
    init_logging();
    let redis = TestRedis::start().await;
    let direct = redis.pool().await;
    let proxy = ReplyDropper::start(redis.socket_path(), COMMIT_REPLY).await;
    let pool = RedisPool::from_config(RedisConfig::new(proxy.url()))
        .await
        .unwrap();
    let client = Client::new(pool);
    let committed = Batch::new(&client)
        .on_success::<Callback>(named("done"))
        .jobs(|jobs| async move {
            for n in 0..3 {
                Work::perform_async(&jobs, work(&format!("m{n}"), "ok")).await?;
            }
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap();
    assert_eq!(proxy.dropped(), 1, "the first reply was lost");
    // The resend found the commit applied: this call wrote it, and reports its members.
    assert!(!committed.already_applied);
    assert_eq!(committed.jids.len(), 3);
    assert_eq!(queue_size(&direct, "default").await, 3);
    let status = status_of(&Client::new(direct.clone()), &committed.bid).await;
    assert_eq!((status.total, status.pending), (3, 3));
}

/// D9: no reply within `batch_commit_timeout` is `Unconfirmed`; committing again with the
/// same commit id settles it, applied exactly once.
#[tokio::test]
async fn an_unconfirmed_commit_is_settled_by_its_commit_id() {
    init_logging();
    let redis = TestRedis::start().await;
    let direct = redis.pool().await;
    let impatient =
        RedisPool::from_config(redis.config().response_timeout(Duration::from_millis(200)))
            .await
            .unwrap();
    let mut defaults = BatchDefaults::default();
    defaults.commit_timeout = Duration::from_secs(1);
    let client = Client::new(impatient).with_batch_defaults(defaults);
    // Loads the script (a later EVALSHA runs once the pause ends).
    Batch::new(&client).commit().await.unwrap();
    let bid = "0000000000000000000c0de1";
    let define = |client: &Client| {
        Batch::with_bid(client, bid)
            .unwrap()
            .commit_id("import-9")
            .jobs(|jobs| async move {
                for n in 0..3 {
                    Work::set()
                        .jid(format!("{n:024x}"))
                        .perform_async(&jobs, work("m", "ok"))
                        .await?;
                }
                Ok::<_, hench::Error>(())
            })
    };
    let pause = Duration::from_millis(2_500);
    let paused_at = Instant::now();
    cmd::<()>(
        &direct,
        &redis_cmd!("CLIENT", "PAUSE", pause.as_millis() as u64, "ALL"),
    )
    .await;
    let unconfirmed = define(&client).await.unwrap_err();
    assert!(
        paused_at.elapsed() < pause,
        "it gave up before the pause ended"
    );
    assert_eq!(
        batch_error(unconfirmed),
        BatchError::Unconfirmed {
            bid: bid.to_owned(),
            commit_id: "import-9".to_owned()
        }
    );
    tokio::time::sleep(pause.saturating_sub(paused_at.elapsed()) + Duration::from_millis(200))
        .await;
    let settled = define(&Client::new(direct.clone())).await.unwrap();
    assert!(
        settled.already_applied,
        "a send held by the pause applied it"
    );
    assert_eq!(queue_size(&direct, "default").await, 3);
    assert_eq!(status_of(&Client::new(direct), bid).await.total, 3);
}

/// D10: more pushes than `batch_max_commit_jobs` fail at the push; nothing is written.
#[tokio::test]
async fn too_large_definitions_fail_at_the_push() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let mut defaults = BatchDefaults::default();
    defaults.max_commit_jobs = 5;
    let client = redis.client().await.with_batch_defaults(defaults);
    let result = Batch::new(&client)
        .jobs(|jobs| async move {
            for n in 0..5 {
                Work::perform_async(&jobs, work(&format!("m{n}"), "ok")).await?;
            }
            Work::perform_async(&jobs, work("one too many", "ok")).await?;
            Ok::<_, hench::Error>(())
        })
        .await;
    assert_eq!(
        batch_error(result.unwrap_err()),
        BatchError::TooLarge { jobs: 6, limit: 5 }
    );
    // A bulk push over the limit buffers none of its jobs.
    let batch = Batch::new(&client);
    let jobs = batch.client();
    let refused = Work::perform_bulk(&jobs, (0..6).map(|n| work(&n.to_string(), "ok")).collect())
        .await
        .unwrap_err();
    assert!(matches!(
        batch_error(refused),
        BatchError::TooLarge { jobs: 6, limit: 5 }
    ));
    Work::perform_async(&jobs, work("fits", "ok"))
        .await
        .unwrap();
    assert_eq!(batch.commit().await.unwrap().jids.len(), 1);
    assert_eq!(queue_size(&pool, "default").await, 1);
}

/// batches-8: child batches count toward `batch_max_commit_jobs` (10 jobs each: a child costs
/// the commit about as much as ten members), so that thousands of children no longer make one
/// commit block Redis for seconds; the refusal comes at the child's commit or the next push,
/// before anything is written.
#[tokio::test]
async fn children_count_toward_the_commit_cap() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let mut defaults = BatchDefaults::default();
    defaults.max_commit_jobs = 25;
    let client = redis.client().await.with_batch_defaults(defaults);
    // Two children with two members each: 2 × 10 + 4 = 24 jobs, within the cap.
    let fits = Batch::new(&client)
        .jobs(|jobs| async move {
            for child in 0..2 {
                Batch::new(&jobs)
                    .jobs(|inner| async move { members_of(&inner, &format!("c{child}-"), 2).await })
                    .await?;
            }
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap();
    assert_eq!((fits.children.len(), fits.jids.len()), (2, 4));
    // A third child is refused at its commit, before anything is written.
    let before = dbsize(&pool).await;
    let result = Batch::new(&client)
        .jobs(|jobs| async move {
            for _ in 0..3 {
                Batch::new(&jobs).commit().await?;
            }
            Ok::<_, hench::Error>(())
        })
        .await;
    assert_eq!(
        batch_error(result.unwrap_err()),
        BatchError::TooLarge {
            jobs: 30,
            limit: 25
        }
    );
    assert_eq!(dbsize(&pool).await, before);
    // With two children, the sixth member is one too many.
    let result = Batch::new(&client)
        .jobs(|jobs| async move {
            Batch::new(&jobs).commit().await?;
            Batch::new(&jobs).commit().await?;
            members_of(&jobs, "m", 6).await
        })
        .await;
    assert_eq!(
        batch_error(result.unwrap_err()),
        BatchError::TooLarge {
            jobs: 26,
            limit: 25
        }
    );
    // A dropped child does not count.
    let result = Batch::new(&client)
        .jobs(|jobs| async move {
            drop(Batch::new(&jobs));
            Batch::new(&jobs).commit().await?;
            members_of(&jobs, "m", 15).await
        })
        .await
        .unwrap();
    assert_eq!(result.jids.len(), 15);
}

/// Pushes `n` members tagged `<prefix><i>` through `jobs`.
async fn members_of(jobs: &Client, prefix: &str, n: usize) -> hench::Result<()> {
    for i in 0..n {
        Work::perform_async(jobs, work(&format!("{prefix}{i}"), "ok")).await?;
    }
    Ok(())
}

/// batches-11: reopening a child to add a member scheduled beyond its parent's expiry extends
/// the parent (and its parent) too: a parent lives at least as long as each of its children.
#[tokio::test]
async fn a_reopened_child_extends_its_ancestors() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let root = Batch::new(&client);
    let child = Batch::new(&root.client());
    let child_bid = child.bid().to_owned();
    child
        .jobs(|jobs| async move { members_of(&jobs, "first", 1).await })
        .await
        .unwrap();
    let root = root.commit().await.unwrap();
    Batch::open(&client, &child_bid)
        .unwrap()
        .jobs(|jobs| async move {
            Work::set()
                .delay(Duration::from_secs(40 * 86_400))
                .perform_async(&jobs, work("follow-up", "ok"))
                .await?;
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap();
    let child_life = pttl(&pool, &keys::batch(&child_bid)).await;
    assert!(child_life > 69 * 86_400_000, "{child_life} ms");
    for key in [keys::batch(&root.bid), keys::batch_kids(&root.bid)] {
        let life = pttl(&pool, &key).await;
        assert!(
            life >= child_life - 5_000,
            "{key}: {life} ms vs {child_life} ms"
        );
    }
}

/// batches-6: a workflow started with a chosen bid and commit id (the starting job's jid)
/// starts once: a re-run of that job writes nothing.
#[tokio::test]
async fn workflows_with_a_chosen_bid_start_once() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let bid = "0000000000000000000000f1";
    for round in 0..2 {
        let started = hench::batch::Workflow::with_bid(&client, bid)
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
    assert_eq!(queue_size(&pool, "default").await, 1);
    // `Workflow::new` gives a new bid each time: its commit id cannot recognize a re-run.
    let first = hench::batch::Workflow::new(&client)
        .commit_id("starter-jid")
        .stage(|s| s.job::<Work>(work("x", "ok")))
        .start()
        .await
        .unwrap();
    assert_ne!(first.bid, bid);
}

/// round3-regressions-3: every stage of a workflow is one batch commit when it starts (its
/// batch counting as 10 jobs toward `batch_max_commit_jobs`): `start` checks them all, so that a
/// later stage too large for one commit fails the start, before anything is written, instead of
/// failing in its `hench::WorkflowNext` once the earlier stages ran (the workflow then stalled).
#[tokio::test]
async fn workflows_check_every_stage_against_the_commit_cap() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let mut defaults = BatchDefaults::default();
    defaults.max_commit_jobs = 12;
    let client = redis.client().await.with_batch_defaults(defaults);
    let before = dbsize(&pool).await;
    // Stage 1: 1 job + its batch = 11, fits; stage 2: 5 jobs + its batch = 15, does not.
    let refused = hench::batch::Workflow::new(&client)
        .stage(|s| s.job::<Work>(work("a", "ok")))
        .stage(|s| (0..5).fold(s, |s, n| s.job::<Work>(work(&format!("b{n}"), "ok"))))
        .start()
        .await
        .unwrap_err();
    assert_eq!(
        batch_error(refused),
        BatchError::StageTooLarge {
            stage: 2,
            jobs: 5,
            limit: 12
        }
    );
    assert_eq!(dbsize(&pool).await, before, "nothing written");
    // The first stage is checked the same way.
    let refused = hench::batch::Workflow::new(&client)
        .stage(|s| (0..3).fold(s, |s, n| s.job::<Work>(work(&format!("a{n}"), "ok"))))
        .start()
        .await
        .unwrap_err();
    assert_eq!(
        batch_error(refused),
        BatchError::StageTooLarge {
            stage: 1,
            jobs: 3,
            limit: 12
        }
    );
    // Stages of up to 2 jobs fit.
    hench::batch::Workflow::new(&client)
        .stage(|s| s.job::<Work>(work("a", "ok")))
        .stage(|s| {
            s.job::<Work>(work("b0", "ok"))
                .job::<Work>(work("b1", "ok"))
        })
        .start()
        .await
        .unwrap();
}

/// D11: a jid twice, a push after the commit, a job naming another batch.
#[tokio::test]
async fn members_are_checked_at_their_push() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let batch = Batch::new(&client);
    let jobs = batch.client();
    let jid = "00000000000000000000000a";
    Work::set()
        .jid(jid)
        .perform_async(&jobs, work("a", "ok"))
        .await
        .unwrap();
    let twice = Work::set()
        .jid(jid)
        .perform_async(&jobs, work("a", "ok"))
        .await
        .unwrap_err();
    assert_eq!(
        batch_error(twice),
        BatchError::DuplicateJid {
            jid: jid.to_owned()
        }
    );
    let mut foreign = JobPayload::new(Work::NAME, vec![json!("x"), json!("ok")]);
    foreign.bid = Some("0123456789abcdef01234567".to_owned());
    assert!(matches!(
        jobs.push(foreign).await,
        Err(hench::Error::InvalidJob(_))
    ));
    let committed = batch.commit().await.unwrap();
    assert_eq!(committed.jids, vec![jid.to_owned()]);
    let late = Work::perform_async(&jobs, work("late", "ok"))
        .await
        .unwrap_err();
    assert_eq!(batch_error(late), BatchError::Closed);
    assert_eq!(queue_size(&redis.pool().await, "default").await, 1);
}

/// D12: a scheduled member extends its batch's expiry past its run time; the batch completes
/// only once it ran.
#[tokio::test]
async fn scheduled_members_keep_their_batch_alive() {
    let redis = TestRedis::start().await;
    let log = Arc::new(Log::default());
    let client = redis.client().await;
    let pool = redis.pool().await;
    let ttl = Duration::from_secs(3_600);
    let delay = Duration::from_secs(2);
    let started = now_secs_f64();
    let committed = Batch::new(&client)
        .expires_in(ttl)
        .on_complete::<Callback>(named("all ran"))
        .jobs(|jobs| async move {
            Work::set()
                .delay(delay)
                .perform_async(&jobs, work("later", "ok"))
                .await?;
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap();
    let lifetime = pttl(&pool, &keys::batch(&committed.bid)).await;
    assert!(
        lifetime > (ttl + delay).as_millis() as i64 - 1_500,
        "{lifetime} ms"
    );
    for key in [
        keys::batch_pending(&committed.bid),
        keys::batch_commits(&committed.bid),
    ] {
        assert!(pttl(&pool, &key).await > (ttl + delay).as_millis() as i64 - 1_500);
    }
    assert_eq!(zcard(&pool, keys::SCHEDULE).await, 1);
    let (_hench, launcher) = worker(&redis, &log, |builder| builder).await;
    log.wait_count("all ran:complete", 1).await;
    assert!(now_secs_f64() - started >= delay.as_secs_f64() - 0.1);
    assert_eq!(log.position("later"), Some(0), "{:?}", log.entries());
    launcher.stop().await.unwrap();
}

/// D13: an empty batch fires at its commit, its callbacks pushed by the committing client; an
/// empty reopen adds nothing and fires nothing.
#[tokio::test]
async fn empty_batches_fire_at_their_commit() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let empty = Batch::new(&client)
        .on_complete::<Callback>(named("complete"))
        .on_success::<Callback>(named("success"))
        .on_death::<Callback>(named("death"))
        .commit()
        .await
        .unwrap();
    let callbacks = queue_jobs(&pool, "default").await;
    assert_eq!(callbacks.len(), 2, "{callbacks:?}");
    let mut events: Vec<String> = callbacks
        .iter()
        .map(|job| job.batch_callback.as_ref().unwrap().event().to_string())
        .collect();
    events.sort();
    assert_eq!(events, ["complete", "success"]);
    let fired = status_of(&client, &empty.bid).await;
    assert!(fired.complete_at.is_some() && fired.success_at.is_some());
    assert!(fired.complete_dispatched_at.is_some() && fired.success_dispatched_at.is_some());
    assert!(fired.death_at.is_none());
    assert!(!fired.undispatched);
    // A finished batch lingers.
    assert!(pttl(&pool, &keys::batch(&empty.bid)).await <= 24 * 3_600 * 1_000);

    // An empty reopen of a pending batch: nothing added, nothing fired.
    let pending = Batch::new(&client)
        .on_complete::<Callback>(named("pending"))
        .jobs(|jobs| async move {
            Work::perform_async(&jobs, work("a", "ok")).await?;
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap();
    let reopened = Batch::open(&client, &pending.bid)
        .unwrap()
        .commit()
        .await
        .unwrap();
    assert!(reopened.jids.is_empty());
    let after = status_of(&client, &pending.bid).await;
    assert_eq!((after.total, after.pending), (1, 1));
    assert!(after.complete_at.is_none());
    assert_eq!(queue_size(&pool, "default").await, 3);
}

/// The callback templates of `event` stored in `bid`'s hash (`{q, jid, j}`), each job (`j`,
/// stored as JSON text) parsed.
async fn templates(pool: &RedisPool, bid: &str, event: &str) -> Vec<Value> {
    let hash = hgetall(pool, &keys::batch(bid)).await;
    let mut templates: Vec<Value> = serde_json::from_str(&hash[&format!("cb_{event}")]).unwrap();
    for template in &mut templates {
        let job: Value = serde_json::from_str(template["j"].as_str().unwrap()).unwrap();
        template["j"] = job;
    }
    templates
}

/// U4: callback templates: no `enqueued_at`, the parent's `bid` for a child's callbacks and
/// none at the root, `batch_callback` set, the queue chosen by `callback_queue` then the
/// setter; `at`/`delay`, a 17th callback and 64 KiB of templates refused.
#[tokio::test]
async fn callback_templates_are_checked_and_stored() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pool = redis.pool().await;
    let committed = Batch::new(&client)
        .callback_queue("low")
        .on_complete::<Callback>(named("root"))
        .on(
            BatchEvent::Complete,
            Callback::set().queue("critical").retry(2u32),
            named("explicit"),
        )
        .jobs(|jobs| async move {
            Work::perform_async(&jobs, work("a", "ok")).await?;
            let child = Batch::new(&jobs).on_death::<Callback>(named("child"));
            let child_bid = child.bid().to_owned();
            child
                .jobs(|inner| async move {
                    Work::perform_async(&inner, work("b", "ok")).await?;
                    Ok::<_, hench::Error>(())
                })
                .await?;
            Ok::<_, hench::Error>(child_bid)
        })
        .await
        .unwrap();
    let root = templates(&pool, &committed.bid, "complete").await;
    assert_eq!(root.len(), 2);
    assert_eq!(root[0]["q"], "low");
    assert_eq!(root[1]["q"], "critical");
    for template in &root {
        let job = &template["j"];
        assert!(job.get("enqueued_at").is_none(), "{job}");
        assert!(job.get("bid").is_none(), "{job}");
        assert_eq!(job["batch_callback"]["bid"], committed.bid.as_str());
        assert_eq!(job["batch_callback"]["event"], "complete");
        assert_eq!(job["jid"], template["jid"]);
        assert_eq!(job["queue"], template["q"]);
    }
    assert_eq!(root[1]["j"]["retry"], 2);
    let child = templates(&pool, &committed.value, "death").await;
    assert_eq!(child.len(), 1);
    assert_eq!(child[0]["j"]["bid"], committed.bid.as_str());
    assert_eq!(
        child[0]["j"]["batch_callback"]["bid"],
        committed.value.as_str()
    );
    // A child does not inherit its parent's callback queue.
    assert_eq!(child[0]["q"], "default");

    // Refusals, at the commit, with nothing written.
    let before = dbsize(&pool).await;
    let delayed = Batch::new(&client)
        .on(
            BatchEvent::Success,
            Callback::set().delay(Duration::from_secs(5)),
            named("delayed"),
        )
        .commit()
        .await;
    assert!(
        matches!(delayed, Err(hench::Error::InvalidJob(_))),
        "{delayed:?}"
    );
    let at = Batch::new(&client)
        .on(
            BatchEvent::Success,
            Callback::set().at(SystemTime::now() + Duration::from_secs(60)),
            named("at"),
        )
        .commit()
        .await;
    assert!(matches!(at, Err(hench::Error::InvalidJob(_))), "{at:?}");
    let mut many = Batch::new(&client);
    for n in 0..17 {
        many = many.on_success::<Callback>(named(&n.to_string()));
    }
    assert!(matches!(
        many.commit().await,
        Err(hench::Error::InvalidJob(_))
    ));
    let mut sixteen = Batch::new(&client);
    for n in 0..16 {
        sixteen = sixteen.on_success::<Callback>(named(&n.to_string()));
    }
    let huge = "x".repeat(70 * 1024);
    let too_big = Batch::new(&client)
        .on_success::<Callback>((huge,))
        .commit()
        .await;
    assert!(
        matches!(too_big, Err(hench::Error::InvalidJob(_))),
        "{too_big:?}"
    );
    assert_eq!(dbsize(&pool).await, before, "nothing was written");
    // Sixteen are fine.
    sixteen.commit().await.unwrap();
}

/// D14: a duplicate unique member is left out (`duplicates`) and the lock stays with the
/// first; `OnConflict::Error` refuses the whole commit; `Replace` is refused at the push.
#[tokio::test]
async fn unique_members_are_locked_by_the_commit() {
    let redis = TestRedis::start().await;
    let client = redis.client().await.with_unique_jobs(true);
    let pool = redis.pool().await;
    let unique = || Work::set().unique_for(Duration::from_secs(600));
    let committed = Batch::new(&client)
        .jobs(|jobs| async move {
            let first = unique().perform_async(&jobs, work("same", "ok")).await?;
            let second = unique().perform_async(&jobs, work("same", "ok")).await?;
            Ok::<_, hench::Error>((first.unwrap(), second.unwrap()))
        })
        .await
        .unwrap();
    let (first, second) = committed.value.clone();
    assert_eq!(committed.jids, vec![first.clone()]);
    assert_eq!(committed.duplicates, vec![second]);
    assert_eq!(queue_size(&pool, "default").await, 1);
    assert_eq!(status_of(&client, &committed.bid).await.total, 1);
    // A later commit with the lock held: left out again.
    let again = Batch::new(&client)
        .jobs(|jobs| async move {
            unique().perform_async(&jobs, work("same", "ok")).await?;
            Work::perform_async(&jobs, work("plain", "ok")).await?;
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap();
    assert_eq!(again.jids.len(), 1);
    assert_eq!(again.duplicates.len(), 1);
    // `OnConflict::Error`: the whole commit is refused.
    let refused = Batch::new(&client)
        .jobs(|jobs| async move {
            Work::perform_async(&jobs, work("plain", "ok")).await?;
            unique()
                .on_conflict(OnConflict::Error)
                .perform_async(&jobs, work("same", "ok"))
                .await?;
            Ok::<_, hench::Error>(())
        })
        .await;
    assert!(
        matches!(refused, Err(hench::Error::DuplicateJob { .. })),
        "{refused:?}"
    );
    assert_eq!(queue_size(&pool, "default").await, 2);
    // `Replace` could take a job away from another batch.
    let batch = Batch::new(&client);
    let replace = unique()
        .on_conflict(OnConflict::Replace)
        .perform_async(&batch.client(), work("same", "ok"))
        .await;
    // The error says how to override a job type's `replace` (docs-usability-14).
    assert!(
        matches!(&replace, Err(hench::Error::InvalidJob(message))
            if message.contains("on_conflict(OnConflict::Drop)")),
        "{replace:?}"
    );
}

/// The worker configuration file's batch settings (`docs/design/pro-features.md` §2.10) reach
/// the configuration and the client's batch defaults.
#[cfg(feature = "cli")]
#[tokio::test]
async fn the_configuration_file_sets_the_batch_settings() {
    use clap::Parser;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("hench.toml");
    std::fs::write(
        &path,
        "batch_ttl = 7200\nbatch_linger = 60\nbatch_max_commit_jobs = 500\nbatch_maintenance = \
         false\n",
    )
    .unwrap();
    let worker =
        hench::cli::WorkerArgs::try_parse_from(["worker", "-C", path.to_str().unwrap()]).unwrap();
    let builder = hench::Hench::builder()
        .redis(RedisPool::detached())
        .auto_register(false);
    let hench = worker.apply(builder).unwrap().build().await.unwrap();
    let config = hench.config();
    assert_eq!(config.batch_ttl, Duration::from_secs(7_200));
    assert_eq!(config.batch_linger, Duration::from_secs(60));
    assert_eq!(config.batch_max_commit_jobs, 500);
    assert!(!config.batch_maintenance);
    // The client's batch settings come from the configuration (`HenchBuilder::build` passes
    // them to `Hench::client()` once the integrator wires it; `with_batch_defaults` meanwhile).
    let defaults = hench
        .client()
        .with_batch_defaults(config.batch_defaults())
        .batch_defaults();
    assert_eq!(
        (defaults.ttl, defaults.linger, defaults.max_commit_jobs),
        (Duration::from_secs(7_200), Duration::from_secs(60), 500)
    );
    // Out of bounds: refused before any I/O.
    std::fs::write(&path, "batch_linger = 5\n").unwrap();
    let worker =
        hench::cli::WorkerArgs::try_parse_from(["worker", "-C", path.to_str().unwrap()]).unwrap();
    let builder = hench::Hench::builder()
        .redis(RedisPool::detached())
        .auto_register(false);
    let refused = worker.apply(builder).unwrap().build().await;
    assert!(matches!(refused, Err(hench::Error::Config(_))));
}
