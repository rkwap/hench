//! The `Batches` API against a private Redis (`docs/design/batches.md` §3.6, A1–A3): listings by
//! state and text, status, failures, pending members, children, `wait`, cancellation of a tree,
//! deletion, and an operator's skip.

mod batch_support;
mod support;

use std::sync::Arc;
use std::time::Duration;

use batch_support::*;
use hench::api::Api;
use hench::batch::{BatchError, BatchFilter, BatchState};
use hench::keys;
use hench::prelude::*;
use hench::test_util::TestRedis;
use support::*;

fn batch_error(error: hench::Error) -> BatchError {
    match error {
        hench::Error::Batch(error) => error,
        other => panic!("not a batch error: {other:?}"),
    }
}

/// A batch of `n` members in queue `parked`, which no worker reads.
async fn parked(client: &Client, description: &str, n: usize) -> String {
    Batch::new(client)
        .description(description)
        .jobs(|jobs| async move {
            for i in 0..n {
                Work::set()
                    .queue("parked")
                    .perform_async(&jobs, work(&format!("p{i}"), "ok"))
                    .await?;
            }
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap()
        .bid
}

/// A1: listings per state and text, a batch's status, its failures page by page, its pending
/// members and children, and `wait`.
#[tokio::test]
async fn batches_are_listed_and_inspected() {
    let redis = TestRedis::start().await;
    let log = Arc::new(Log::default());
    let pool = redis.pool().await;
    let client = redis.client().await;
    let api = batches(&client);
    // Pending, with three members and a child.
    let active = Batch::new(&client)
        .description("Import #7 of customers")
        .jobs(|jobs| async move {
            for i in 0..3 {
                Work::set()
                    .queue("parked")
                    .perform_async(&jobs, work(&format!("p{i}"), "ok"))
                    .await?;
            }
            Batch::new(&jobs)
                .description("child of the import")
                .jobs(|inner| async move {
                    Work::set()
                        .queue("parked")
                        .perform_async(&inner, work("c", "ok"))
                        .await?;
                    Ok::<_, hench::Error>(())
                })
                .await?;
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap();
    let child = active.children[0].clone();
    // Succeeded (empty), cancelled, dead (its only member deleted through the API).
    let succeeded = Batch::new(&client)
        .description("nothing to do")
        .commit()
        .await
        .unwrap()
        .bid;
    let cancelled = parked(&client, "cancelled one", 1).await;
    api.cancel(&cancelled).await.unwrap();
    let dead = parked(&client, "dead one", 1).await;
    let member = smembers(&pool, &keys::batch_pending(&dead)).await.remove(0);
    let record = Api::new(pool.clone())
        .queue("parked")
        .find_job(&member)
        .await
        .unwrap()
        .unwrap();
    assert!(record.delete().await.unwrap());
    // Complete without a death: a member failing (retried later), through a worker.
    let (hench, launcher) = worker(&redis, &log, |builder| builder).await;
    let complete = Batch::new(hench.client())
        .description("retrying one")
        .jobs(|jobs| async move {
            for i in 0..12 {
                Work::perform_async(&jobs, work(&format!("f{i}"), "fail")).await?;
            }
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap()
        .bid;
    let waited = api.wait(&complete, Duration::from_secs(10)).await.unwrap();
    assert!(waited.is_complete());
    launcher.stop().await.unwrap();

    let listed = |filter: BatchFilter| {
        let api = api.clone();
        async move {
            let mut bids: Vec<String> = api
                .page(&filter, 1, 50)
                .await
                .unwrap()
                .items
                .into_iter()
                .map(|summary| summary.bid)
                .collect();
            bids.sort();
            bids
        }
    };
    let sorted = |mut bids: Vec<&String>| {
        bids.sort();
        bids.into_iter().cloned().collect::<Vec<_>>()
    };
    assert_eq!(api.size().await.unwrap(), 6);
    assert_eq!(listed(BatchFilter::All).await.len(), 6);
    // Active: not succeeded and not finished (what needs a look, or is still running).
    assert_eq!(
        listed(BatchFilter::Active).await,
        sorted(vec![&active.bid, &child, &cancelled, &dead, &complete])
    );
    assert_eq!(
        listed(BatchFilter::Succeeded).await,
        vec![succeeded.clone()]
    );
    assert_eq!(
        listed(BatchFilter::Cancelled).await,
        vec![cancelled.clone()]
    );
    assert_eq!(listed(BatchFilter::Dead).await, vec![dead.clone()]);
    assert_eq!(
        listed(BatchFilter::Complete).await,
        sorted(vec![&succeeded, &dead, &complete])
    );
    assert_eq!(
        listed(BatchFilter::Text("Import *7".to_owned())).await,
        vec![active.bid.clone()]
    );
    assert_eq!(
        listed(BatchFilter::Text(child[..10].to_owned())).await,
        vec![child.clone()]
    );
    let search = api
        .search(&BatchFilter::Active, Some("child"), 1, 10)
        .await
        .unwrap();
    assert_eq!(search.page.items.len(), 1);
    assert!(!search.truncated);
    // Paging the whole index, most recently active first.
    let first = api.page(&BatchFilter::All, 1, 4).await.unwrap();
    let second = api.page(&BatchFilter::All, 2, 4).await.unwrap();
    assert_eq!(
        (first.items.len(), second.items.len(), first.total),
        (4, 2, 6)
    );

    // The status of one batch.
    let status = api.fetch(&active.bid).await.unwrap().unwrap();
    assert_eq!(
        status.description.as_deref(),
        Some("Import #7 of customers")
    );
    assert_eq!((status.total, status.pending, status.children), (3, 3, 1));
    assert_eq!(status.state(), BatchState::Pending);
    assert!(status.expires_at.is_some());
    assert!(
        api.fetch("0123456789abcdef01234567")
            .await
            .unwrap()
            .is_none()
    );
    // Its pending members and children.
    let mut pending = api.pending_jids(&active.bid, 10).await.unwrap();
    pending.sort();
    let child_members = smembers(&pool, &keys::batch_pending(&child)).await;
    let mut expected = active.jids.clone();
    expected.retain(|jid| !child_members.contains(jid));
    expected.sort();
    assert_eq!(pending, expected);
    assert_eq!(api.pending_jids(&active.bid, 2).await.unwrap().len(), 2);
    let children = api.children(&active.bid).await.unwrap();
    assert_eq!(children.len(), 1);
    assert_eq!(children[0].bid, child);
    assert_eq!(children[0].parent.as_deref(), Some(active.bid.as_str()));
    // Failures, page by page.
    let first = api.failures(&complete, 1, 5).await.unwrap();
    let last = api.failures(&complete, 3, 5).await.unwrap();
    assert_eq!(
        (first.items.len(), last.items.len(), first.total),
        (5, 2, 12)
    );
    assert!(first.items.iter().all(|failure| failure.reason == "retry"));
    // `wait`: the timeout returns the status as it is; a deleted batch is `NotFound`.
    let pending = api
        .wait(&active.bid, Duration::from_millis(200))
        .await
        .unwrap();
    assert!(!pending.is_complete());
    assert!(api.delete(&active.bid, false).await.unwrap());
    assert_eq!(
        batch_error(
            api.wait(&active.bid, Duration::from_secs(1))
                .await
                .unwrap_err()
        ),
        BatchError::NotFound {
            bid: active.bid.clone()
        }
    );
}

/// A2: cancelling a tree of three levels cancels each batch once; a succeeded batch cannot be
/// cancelled; deleting a root deletes its descendants; deleting an unfinished child of a live
/// parent needs `force`.
#[tokio::test]
async fn trees_are_cancelled_and_deleted() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let client = redis.client().await;
    let api = batches(&client);
    let tree = |client: Client| async move {
        let mut grandchild = String::new();
        let committed = Batch::new(&client)
            .jobs(|jobs| async move {
                Work::set()
                    .queue("parked")
                    .perform_async(&jobs, work("root", "ok"))
                    .await?;
                let child = Batch::new(&jobs);
                let grandchild = child
                    .jobs(|inner| async move {
                        Work::set()
                            .queue("parked")
                            .perform_async(&inner, work("child", "ok"))
                            .await?;
                        let staged = Batch::new(&inner)
                            .jobs(|deepest| async move {
                                Work::set()
                                    .queue("parked")
                                    .perform_async(&deepest, work("grandchild", "ok"))
                                    .await?;
                                Ok::<_, hench::Error>(())
                            })
                            .await?;
                        Ok::<_, hench::Error>(staged.bid)
                    })
                    .await?;
                Ok::<_, hench::Error>(grandchild.value)
            })
            .await
            .unwrap();
        grandchild.clone_from(&committed.value);
        (committed.bid, committed.children, grandchild)
    };
    let (root, children, grandchild) = tree(client.clone()).await;
    assert_eq!(children.len(), 2, "the child and the grandchild");
    assert_eq!(api.cancel(&root).await.unwrap(), 3);
    assert_eq!(api.cancel(&root).await.unwrap(), 0, "already cancelled");
    for bid in &children {
        let status = api.fetch(bid).await.unwrap().unwrap();
        assert!(status.cancelled_at.is_some(), "{bid}");
    }
    assert!(
        api.fetch(&grandchild)
            .await
            .unwrap()
            .unwrap()
            .cancelled_at
            .is_some()
    );
    // A succeeded batch.
    let done = Batch::new(&client).commit().await.unwrap().bid;
    assert_eq!(
        batch_error(api.cancel(&done).await.unwrap_err()),
        BatchError::Succeeded { bid: done.clone() }
    );
    assert_eq!(
        batch_error(api.cancel("0123456789abcdef01234567").await.unwrap_err()),
        BatchError::NotFound {
            bid: "0123456789abcdef01234567".to_owned()
        }
    );
    // Deleting the root deletes the whole tree.
    assert!(api.delete(&root, false).await.unwrap());
    for bid in [&root, &grandchild] {
        assert!(api.fetch(bid).await.unwrap().is_none());
    }
    for bid in &children {
        assert!(api.fetch(bid).await.unwrap().is_none());
    }
    assert!(!api.delete(&root, false).await.unwrap(), "nothing left");
    let leftovers: Vec<String> = batch_keys(&redis)
        .await
        .into_iter()
        .filter(|key| key.contains(&root))
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");
    // An unfinished child of a live parent.
    let (root, children, _) = tree(client.clone()).await;
    let mut child = String::new();
    for bid in &children {
        if hgetall(&pool, &keys::batch(bid)).await.get("parent") == Some(&root) {
            child.clone_from(bid);
        }
    }
    assert!(!child.is_empty());
    assert_eq!(
        batch_error(api.delete(&child, false).await.unwrap_err()),
        BatchError::DeleteChild {
            bid: child.clone(),
            parent: root.clone()
        }
    );
    assert!(api.delete(&child, true).await.unwrap());
    assert!(api.fetch(&child).await.unwrap().is_none());
    assert!(api.fetch(&root).await.unwrap().is_some());
}

/// A3: an operator skips a dead member: the batch can succeed.
#[tokio::test]
async fn skipping_a_dead_member_lets_the_batch_succeed() {
    let redis = TestRedis::start().await;
    let log = Arc::new(Log::default());
    let (hench, launcher) = worker(&redis, &log, |builder| builder).await;
    let client = hench.client().clone();
    let pool = hench.redis().clone();
    let committed = Batch::new(&client)
        .on_success::<Callback>(named("ok"))
        .on_death::<Callback>(named("dead"))
        .jobs(|jobs| async move {
            Work::perform_async(&jobs, work("fine", "ok")).await?;
            Work::set()
                .retry(0u32)
                .perform_async(&jobs, work("broken", "fail"))
                .await?;
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap();
    log.wait_count("dead:death", 1).await;
    let (dead, _) = sorted_jobs(&pool, keys::DEAD).await.remove(0);
    let api = batches(&client);
    assert!(api.skip(&committed.bid, &dead.jid).await.unwrap());
    assert!(
        !api.skip(&committed.bid, &dead.jid).await.unwrap(),
        "no longer pending"
    );
    log.wait_count("ok:success", 1).await;
    let status = api.fetch(&committed.bid).await.unwrap().unwrap();
    assert_eq!(status.state(), BatchState::Succeeded);
    assert_eq!((status.pending, status.skipped, status.dead), (0, 1, 0));
    assert_eq!(
        batch_error(
            api.skip("0123456789abcdef01234567", &dead.jid)
                .await
                .unwrap_err()
        ),
        BatchError::NotFound {
            bid: "0123456789abcdef01234567".to_owned()
        }
    );
    launcher.stop().await.unwrap();
}

/// Child batches one commit of the trees below defines: a child counts as 10 jobs toward
/// `batch_max_commit_jobs` (10 000 by default), so wide trees are built over several commits,
/// the first creating the root and the next ones reopening it, as an application adds
/// children beyond one commit's cap.
const CHILDREN_PER_COMMIT: usize = 800;

/// Root batch `root` reopened (`Batch::open`), or a new batch when there is none yet.
fn root_batch(client: &Client, root: Option<&String>) -> Batch {
    match root {
        Some(bid) => Batch::open(client, bid.clone()).unwrap(),
        None => Batch::new(client),
    }
}

/// A batch of one member in queue `parked` with `children` child batches, each of one member
/// too, the first `grandchildren` of which have a child batch of one member: returns the root
/// and its descendants.
async fn wide_tree(
    client: &Client,
    children: usize,
    grandchildren: usize,
) -> (String, Vec<String>) {
    let mut root: Option<String> = None;
    let mut descendants = Vec::new();
    let mut from = 0;
    while root.is_none() || from < children {
        let to = (from + CHILDREN_PER_COMMIT).min(children);
        let first = root.is_none();
        let committed = root_batch(client, root.as_ref())
            .jobs(|jobs| async move {
                if first {
                    Work::set()
                        .queue("parked")
                        .perform_async(&jobs, work("root", "ok"))
                        .await?;
                }
                for n in from..to {
                    Batch::new(&jobs)
                        .jobs(|child| async move {
                            Work::set()
                                .queue("parked")
                                .perform_async(&child, work(&format!("c{n}"), "ok"))
                                .await?;
                            if n < grandchildren {
                                Batch::new(&child)
                                    .jobs(|inner| async move {
                                        Work::set()
                                            .queue("parked")
                                            .perform_async(&inner, work(&format!("g{n}"), "ok"))
                                            .await?;
                                        Ok::<_, hench::Error>(())
                                    })
                                    .await?;
                            }
                            Ok::<_, hench::Error>(())
                        })
                        .await?;
                }
                Ok::<_, hench::Error>(())
            })
            .await
            .unwrap();
        root.get_or_insert(committed.bid);
        descendants.extend(committed.children);
        from = to;
    }
    (root.unwrap(), descendants)
}

/// Round 3 (batches-4, web-api-security-2): a tree of more than a thousand batches is cancelled
/// whole by one call, and a descendant that is gone or has succeeded is left alone. Cancelling
/// used to stop at 1 000 batches with an error, and every later call walked the same first
/// thousand again, so the rest of the tree was never cancelled.
#[tokio::test]
async fn a_large_tree_is_cancelled_in_one_call() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let client = redis.client().await;
    let api = batches(&client);
    let (root, descendants) = wide_tree(&client, 1_200, 3).await;
    assert_eq!(descendants.len(), 1_203);
    // One child is deleted, and its grandchild with it: the tree names a batch that is gone.
    let mut gone = String::new();
    for bid in &descendants {
        if hgetall(&pool, &keys::batch(bid)).await.get("parent") == Some(&root)
            && !smembers(&pool, &keys::batch_kids(bid)).await.is_empty()
        {
            gone.clone_from(bid);
            break;
        }
    }
    assert!(api.delete(&gone, true).await.unwrap());
    assert_eq!(api.cancel(&root).await.unwrap(), 1 + 1_200 - 1 + 3 - 1);
    let mut cancelled = 0;
    for bid in &descendants {
        if let Some(status) = api.fetch(bid).await.unwrap() {
            assert!(status.cancelled_at.is_some(), "{bid} was not cancelled");
            cancelled += 1;
        }
    }
    assert_eq!(cancelled, 1_201);
    assert_eq!(api.cancel(&root).await.unwrap(), 0, "already cancelled");
}

/// Round 3 (batches-12): deleting a tree of more than 10 000 batches deletes all of it.
/// Deleting used to stop at 10 000 batches and leave the rest without a word.
#[tokio::test]
async fn a_huge_tree_is_deleted_whole() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let api = batches(&client);
    let mut root: Option<String> = None;
    let mut children = Vec::new();
    while children.len() < 10_050 {
        let count = (10_050 - children.len()).min(CHILDREN_PER_COMMIT);
        let first = root.is_none();
        let committed = root_batch(&client, root.as_ref())
            .jobs(|jobs| async move {
                if first {
                    Work::set()
                        .queue("parked")
                        .perform_async(&jobs, work("root", "ok"))
                        .await?;
                }
                for _ in 0..count {
                    Batch::new(&jobs).commit().await?;
                }
                Ok::<_, hench::Error>(())
            })
            .await
            .unwrap();
        root.get_or_insert(committed.bid);
        children.extend(committed.children);
    }
    let root = root.unwrap();
    assert_eq!(children.len(), 10_050);
    assert!(api.delete(&root, false).await.unwrap());
    let left = batch_keys(&redis).await;
    assert!(
        left.is_empty(),
        "{} keys left, such as {:?}",
        left.len(),
        left.first()
    );
    assert!(!api.delete(&root, false).await.unwrap(), "nothing left");
}

/// Round 3 (web-api-security-3): a failure summary whose retry time no clock can show (a
/// `retry_in` of `Duration::MAX` wrote one, before retry delays were bounded) is read without
/// it; reading it used to panic the dashboard's and the JSON API's batch pages and `hench
/// batches show`.
#[tokio::test]
async fn failure_summaries_with_times_out_of_range_are_read() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let client = redis.client().await;
    let api = batches(&client);
    let bid = parked(&client, "far retries", 1).await;
    for (jid, next_at) in [
        ("0123456789abcdef01234567", "1.8446744073709552e19"),
        ("0123456789abcdef01234568", "1e19"),
        ("0123456789abcdef01234569", "1e300"),
        ("0123456789abcdef0123456a", "1e12"),
    ] {
        let summary = format!(
            r#"{{"class":"Work","state":"retry","reason":"","retry_count":0,"at":1700000000000,"next_at":{next_at}}}"#
        );
        let _: () = cmd(
            &pool,
            &redis_cmd!("HSET", keys::batch_failed(&bid), jid, summary),
        )
        .await;
    }
    let failures = api.failures(&bid, 1, 10).await.unwrap().items;
    assert_eq!(failures.len(), 4);
    for failure in &failures {
        let expected = failure.jid.ends_with('a');
        assert_eq!(failure.next_at.is_some(), expected, "{failure:?}");
    }
}
