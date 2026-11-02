//! The Batches tab (`docs/design/batches.md` §2.12, §3.6 W1–W4): the list by state (progress
//! bars with their labels, counts, chips, filters, escaping), a batch's page (facts, callbacks,
//! failures linked to their retry or morgue entries, children, pending jobs, 404), the Cancel,
//! Delete, Skip and Push callbacks actions with their CSRF and read-only refusals, and the batch
//! rows of job pages.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::{App, TOKEN, post_request};
use hench::batch::{BatchEvent, Batches};
use hench::prelude::*;
use hench::{Hench, Launcher};
use http::StatusCode;

/// A member: fails when its argument says so (retried after an hour), else succeeds.
#[derive(Default)]
struct Member;

impl Job for Member {
    const NAME: &'static str = "web::Member";
    type Args = (String,);

    async fn perform(&self, _ctx: &JobContext, (mode,): Self::Args) -> Result<(), JobError> {
        match mode.as_str() {
            "fail" => Err(JobError::msg("<script>alert('member')</script> went wrong")),
            _ => Ok(()),
        }
    }

    fn retry_in(&self, _count: u32, _error: &JobError, _job: &JobPayload) -> RetryIn {
        RetryIn::After(Duration::from_secs(3_600))
    }
}

/// A callback that counts its runs.
#[derive(Default)]
struct Done;

impl Job for Done {
    const NAME: &'static str = "web::Done";
    type Args = (String,);

    async fn perform(&self, ctx: &JobContext, _args: Self::Args) -> Result<(), JobError> {
        ctx.data::<AtomicUsize>()?.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn client(app: &App) -> Client {
    Client::new(app.pool.clone())
}

fn batches(app: &App) -> Batches {
    app.api.batches()
}

/// A worker running the members and callbacks of `app`'s Redis (the queues `default` and
/// `critical`).
async fn worker(app: &App) -> (Hench, Launcher, Arc<AtomicUsize>) {
    let done = Arc::new(AtomicUsize::new(0));
    let hench = app
        .redis
        .builder()
        .await
        .queues(["default", "critical"])
        .register(Member)
        .register(Done)
        .data_arc(done.clone())
        .build()
        .await
        .unwrap();
    let launcher = hench.start().await.unwrap();
    (hench, launcher, done)
}

/// Waits until `check` holds for the status of `bid`.
async fn wait_for(app: &App, bid: &str, check: impl Fn(&hench::batch::BatchStatus) -> bool) {
    for _ in 0..500 {
        if let Some(status) = batches(app).fetch(bid).await.unwrap()
            && check(&status)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("batch {bid} never got there");
}

/// A batch of `n` members parked in queue `parked` (no worker reads it).
async fn parked(app: &App, description: &str, n: usize) -> String {
    Batch::new(&client(app))
        .description(description)
        .on_complete::<Done>(("complete".to_owned(),))
        .jobs(|jobs| async move {
            for _ in 0..n {
                Member::set()
                    .queue("parked")
                    .perform_async(&jobs, ("ok".to_owned(),))
                    .await?;
            }
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap()
        .bid
}

#[tokio::test]
async fn the_list_shows_batches_by_state() {
    let app = App::new().await;
    let empty = app.get("/batches").await;
    empty.assert_status(StatusCode::OK);
    assert_eq!(
        empty.texts(".empty"),
        ["No batch exists, or every batch expired."]
    );
    assert_eq!(empty.texts(".nav a[aria-current=page]"), ["Batches"]);
    // Live refresh is offered.
    assert!(empty.body.contains("data-live-page"));

    let pending = parked(&app, "Import #7", 4).await;
    let done = Batch::new(&client(&app))
        .description("nothing to do")
        .commit()
        .await
        .unwrap()
        .bid;
    let res = app.get("/batches").await;
    res.assert_status(StatusCode::OK);
    assert_eq!(res.title(), "Batches (1) · Hench");
    assert_eq!(res.texts(".batch-bid"), std::slice::from_ref(&pending));
    assert_eq!(res.texts(".batch-description"), ["Import #7"]);
    assert_eq!(
        res.attrs(".batch-bid", "href"),
        [format!("/batches/{pending}")]
    );
    // The progress bar's label says everything its colors say.
    assert_eq!(
        res.attrs(".progress-bar", "aria-label"),
        ["0 of 4 jobs succeeded, 0 retrying, 0 dead, 0 skipped, 4 not run yet"]
    );
    assert_eq!(res.texts(".progress-counts"), ["0 / 4"]);
    assert_eq!(res.count(".progress-segment"), 0);
    assert!(
        res.body
            .contains("2 batches in all. Last hour: 2 created, 1 succeeded"),
        "{}",
        res.body
    );
    assert_eq!(res.texts(".batches-states a[aria-current]"), ["Active"]);

    // Other states, and all of them.
    let succeeded = app.get("/batches?state=succeeded").await;
    assert_eq!(succeeded.texts(".batch-bid"), std::slice::from_ref(&done));
    assert_eq!(
        succeeded.attrs(".progress-segment", "class"),
        Vec::<String>::new(),
        "an empty batch has no segment"
    );
    assert_eq!(succeeded.texts(".chip"), ["succeeded", "complete"]);
    assert_eq!(
        succeeded.texts(".batches-states a[aria-current]"),
        ["Succeeded"]
    );
    let all = app.get("/batches?state=all").await;
    assert_eq!(all.count(".batch-bid"), 2);
    assert_eq!(
        app.get("/batches?state=dead").await.texts(".empty"),
        ["No batch is in this state."]
    );
    // The state links keep the filter, the filter form keeps the state.
    let filtered = app.get("/batches?state=all&substr=Import").await;
    assert_eq!(filtered.texts(".batch-bid"), std::slice::from_ref(&pending));
    assert!(
        filtered
            .attrs(".batches-states a", "href")
            .iter()
            .all(|href| href.contains("substr=Import")),
        "{:?}",
        filtered.attrs(".batches-states a", "href")
    );
    assert_eq!(
        filtered.attrs("form[role=search] input[name=state]", "value"),
        ["all"]
    );
    // Every batch was searched: no note says otherwise.
    assert_eq!(filtered.count(".batches-searched"), 0);
    let none = app.get("/batches?state=all&substr=zzz").await;
    assert_eq!(none.texts(".empty"), ["No batch matches the filter."]);
    // A state that does not exist.
    app.get("/batches?state=finished")
        .await
        .assert_status(StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn lists_page_and_keep_their_state() {
    let app = App::new().await;
    for n in 0..5 {
        parked(&app, &format!("batch {n}"), 1).await;
    }
    let first = app.get("/batches?state=all&count=2").await;
    assert_eq!(first.count(".batch-bid"), 2);
    let next = first.attrs("a[rel=next]", "href");
    assert_eq!(next.len(), 2, "a pager above and below the table");
    assert!(
        next[0].contains("state=all") && next[0].contains("count=2") && next[0].contains("page=2")
    );
    let last = app.get("/batches?state=all&count=2&page=9").await;
    assert_eq!(last.count(".batch-bid"), 1, "past the end: the last page");
}

#[tokio::test]
async fn hostile_text_is_escaped_and_long_text_cut() {
    let app = App::new().await;
    let hostile = format!(
        "<img src=x onerror=alert(1)> \u{1b}[31m {}",
        "long ".repeat(40)
    );
    let bid = parked(&app, &hostile, 1).await;
    let res = app.get("/batches").await;
    res.assert_status(StatusCode::OK);
    assert!(!res.body.contains("<img src=x"), "{}", res.body);
    assert!(res.body.contains("&#60;img src"), "{}", res.body);
    let shown = res.texts(".batch-description").remove(0);
    assert!(shown.ends_with('…'), "{shown}");
    assert!(shown.chars().count() <= 121);
    assert!(
        shown.contains("\\u{1b}"),
        "control characters are spelled out"
    );
    // The whole text in its title.
    let title = res.attrs(".batch-description", "title").remove(0);
    assert!(title.contains("long long long"));
    let detail = app.get(&format!("/batches/{bid}")).await;
    assert!(!detail.body.contains("<img src=x"));
}

#[tokio::test]
async fn a_batch_page_shows_its_facts_failures_children_and_pending_jobs() {
    let app = App::new().await;
    let (_hench, launcher, _done) = worker(&app).await;
    let parent = Batch::new(&client(&app))
        .description("orders")
        .on_success::<Done>(("success".to_owned(),))
        .on(
            BatchEvent::Complete,
            Done::set().queue("critical"),
            ("complete".to_owned(),),
        )
        .jobs(|jobs| async move {
            Member::perform_async(&jobs, ("ok".to_owned(),)).await?;
            Member::perform_async(&jobs, ("fail".to_owned(),)).await?;
            Member::set()
                .retry(0u32)
                .perform_async(&jobs, ("fail".to_owned(),))
                .await?;
            Member::set()
                .queue("parked")
                .perform_async(&jobs, ("ok".to_owned(),))
                .await?;
            Batch::new(&jobs)
                .description("child")
                .jobs(|inner| async move {
                    Member::perform_async(&inner, ("ok".to_owned(),)).await?;
                    Ok::<_, hench::Error>(())
                })
                .await?;
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap();
    let bid = parent.bid.clone();
    let child = parent.children[0].clone();
    wait_for(&app, &bid, |status| {
        status.failures == 2 && status.children_complete == 1
    })
    .await;
    launcher.stop().await.unwrap();

    let res = app.get(&format!("/batches/{bid}")).await;
    res.assert_status(StatusCode::OK);
    assert_eq!(res.title(), format!("Batch {bid} · Hench"));
    assert_eq!(res.texts(".batch-description-full"), ["orders"]);
    assert!(res.body.contains("data-live-page"));
    // The facts.
    let facts = res.texts(".kv tr").join("\n");
    assert!(facts.contains("Jobs4"), "{facts}");
    for fact in [
        "Pending", "Failures", "Dead", "Children", "Created", "Expires",
    ] {
        assert!(facts.contains(fact), "{fact}: {facts}");
    }
    assert!(facts.contains("1 (1 complete, 1 succeeded)"), "{facts}");
    // The progress bar: of its 4 members, 1 succeeded, 1 retrying, 1 dead, 1 not run yet.
    let label = res
        .attrs(".batch-progress .progress-bar", "aria-label")
        .remove(0);
    assert_eq!(
        label,
        "1 of 4 jobs succeeded, 1 retrying, 1 dead, 0 skipped, 1 not run yet"
    );
    let segments = res.attrs(".batch-progress .progress-segment", "class");
    assert!(
        segments.iter().any(|class| class.contains("progress-dead")),
        "{segments:?}"
    );
    assert!(
        segments
            .iter()
            .any(|class| class.contains("progress-retrying"))
    );
    // Callbacks: their queue and state.
    let callbacks = res.texts("section[aria-labelledby=callbacks-heading] tbody tr");
    assert_eq!(callbacks.len(), 2, "{callbacks:?}");
    // The parked member never ran: neither event fired.
    assert!(
        callbacks.iter().any(|row| row.starts_with("complete")
            && row.contains("critical")
            && row.ends_with("not fired")),
        "{callbacks:?}"
    );
    assert!(
        callbacks.iter().any(|row| row.starts_with("success")
            && row.contains("default")
            && row.ends_with("not fired")),
        "{callbacks:?}"
    );
    // Failures, linked to their entries; the message escaped.
    let links = res.attrs(
        "section[aria-labelledby=failures-heading] tbody th a",
        "href",
    );
    assert_eq!(links.len(), 2, "{links:?}");
    assert!(links.iter().any(|href| href.starts_with("/retries/")));
    assert!(links.iter().any(|href| href.starts_with("/morgue/")));
    assert!(!res.body.contains("<script>alert('member')"));
    assert!(res.body.contains("&#60;script&#62;"));
    let states = res.texts("section[aria-labelledby=failures-heading] tbody td .chip");
    assert_eq!(states, ["dead: in the morgue"]);
    // The failure links lead to the job pages, which link back to the batch.
    let retry = links
        .iter()
        .find(|href| href.starts_with("/retries/"))
        .unwrap();
    let job = app.get(retry).await;
    job.assert_status(StatusCode::OK);
    assert!(
        job.attrs("a", "href").contains(&format!("/batches/{bid}")),
        "{}",
        job.body
    );
    assert!(
        job.texts("tr")
            .join("\n")
            .contains(&format!("member of batch {bid}"))
    );
    // Children.
    assert_eq!(
        res.attrs(
            "section[aria-labelledby=children-heading] .batch-bid",
            "href"
        ),
        [format!("/batches/{child}")]
    );
    // Pending jobs, each with links that find it.
    let pending = res.texts(".batch-pending code");
    assert_eq!(pending.len(), 3, "{pending:?}");
    let finds = res.attrs(".batch-pending a", "href");
    assert!(
        finds
            .iter()
            .any(|href| href.starts_with("/retries?substr="))
    );
    // The child's page links to its parent.
    let child_page = app.get(&format!("/batches/{child}")).await;
    assert_eq!(
        child_page.attrs(".breadcrumb a.mono", "href"),
        [format!("/batches/{bid}")]
    );
    // Unknown, malformed and expired batches.
    app.get("/batches/0123456789abcdef01234567")
        .await
        .assert_status(StatusCode::NOT_FOUND);
    app.get("/batches/..%2Fetc")
        .await
        .assert_status(StatusCode::NOT_FOUND);
    batches(&app).delete(&bid, true).await.unwrap();
    let gone = app.get(&format!("/batches/{bid}")).await;
    gone.assert_status(StatusCode::NOT_FOUND);
    assert!(gone.body.contains("does not exist or its data has expired"));
}

#[tokio::test]
async fn actions_cancel_skip_dispatch_and_delete_with_a_notice() {
    let app = App::new().await;
    let bid = parked(&app, "to cancel", 2).await;
    let res = app
        .post(&format!("/batches/{bid}"), &[("cancel", "1")])
        .await;
    res.assert_status(StatusCode::SEE_OTHER);
    assert_eq!(res.location(), format!("/batches/{bid}"));
    assert!(
        res.cookie("hench_flash")
            .unwrap()
            .starts_with(&format!("hench_flash=b-cancelled.0.0.{bid}")),
        "{:?}",
        res.cookie("hench_flash")
    );
    let status = batches(&app).fetch(&bid).await.unwrap().unwrap();
    assert!(status.cancelled_at.is_some());
    // The page no longer offers Cancel.
    let page = app.get(&format!("/batches/{bid}")).await;
    assert_eq!(page.count("button[name=cancel]"), 0);
    assert_eq!(page.count("button[name=delete]"), 1);

    // Skip a pending member, then one that no longer is.
    let other = parked(&app, "to skip", 1).await;
    let jid = batches(&app)
        .pending_jids(&other, 1)
        .await
        .unwrap()
        .remove(0);
    let res = app
        .post(
            &format!("/batches/{other}"),
            &[("skip", "1"), ("jid", &jid)],
        )
        .await;
    res.assert_status(StatusCode::SEE_OTHER);
    assert!(
        res.cookie("hench_flash")
            .unwrap()
            .starts_with("hench_flash=b-skipped.1.0")
    );
    let again = app
        .post(
            &format!("/batches/{other}"),
            &[("skip", "1"), ("jid", &jid)],
        )
        .await;
    assert!(
        again
            .cookie("hench_flash")
            .unwrap()
            .starts_with("hench_flash=b-skipped.0.1")
    );
    app.post(&format!("/batches/{other}"), &[("skip", "1")])
        .await
        .assert_status(StatusCode::BAD_REQUEST);
    // Its `complete` fired when the skip finished it: dispatched already.
    let res = app
        .post(&format!("/batches/{other}"), &[("dispatch", "1")])
        .await;
    res.assert_status(StatusCode::SEE_OTHER);
    assert!(
        res.cookie("hench_flash")
            .unwrap()
            .starts_with("hench_flash=b-dispatched.0.0")
    );
    // A succeeded batch cannot be cancelled: said in words.
    let done = Batch::new(&client(&app)).commit().await.unwrap().bid;
    let refused = app
        .post(&format!("/batches/{done}"), &[("cancel", "1")])
        .await;
    refused.assert_status(StatusCode::BAD_REQUEST);
    assert!(refused.body.contains("already succeeded"));
    // An unfinished child of a live parent is not deleted.
    let tree = Batch::new(&client(&app))
        .jobs(|jobs| async move {
            Member::set()
                .queue("parked")
                .perform_async(&jobs, ("ok".to_owned(),))
                .await?;
            Batch::new(&jobs)
                .jobs(|inner| async move {
                    Member::set()
                        .queue("parked")
                        .perform_async(&inner, ("ok".to_owned(),))
                        .await?;
                    Ok::<_, hench::Error>(())
                })
                .await?;
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap();
    let child = tree.children[0].clone();
    let refused = app
        .post(&format!("/batches/{child}"), &[("delete", "1")])
        .await;
    refused.assert_status(StatusCode::BAD_REQUEST);
    assert!(
        refused.body.contains(&tree.bid),
        "the notice names the parent"
    );
    // Delete: back to the list.
    let res = app
        .post(&format!("/batches/{}", tree.bid), &[("delete", "1")])
        .await;
    res.assert_status(StatusCode::SEE_OTHER);
    assert_eq!(res.location(), "/batches");
    assert!(
        res.cookie("hench_flash")
            .unwrap()
            .starts_with("hench_flash=b-deleted.0.0.")
    );
    assert!(batches(&app).fetch(&tree.bid).await.unwrap().is_none());
    assert!(batches(&app).fetch(&child).await.unwrap().is_none());
    // No action named.
    app.post(&format!("/batches/{bid}"), &[("nothing", "1")])
        .await
        .assert_status(StatusCode::BAD_REQUEST);
    app.post("/batches/0123456789abcdef01234567", &[("cancel", "1")])
        .await
        .assert_status(StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn actions_need_a_token_and_are_refused_when_read_only_or_cross_site() {
    let app = App::new().await;
    let bid = parked(&app, "guarded", 1).await;
    let res = app
        .send(post_request(
            &format!("/batches/{bid}"),
            &[("cancel", "1")],
            TOKEN,
            &"f".repeat(64),
        ))
        .await;
    res.assert_status(StatusCode::FORBIDDEN);
    let mut cross = post_request(&format!("/batches/{bid}"), &[("cancel", "1")], TOKEN, TOKEN);
    cross
        .headers_mut()
        .insert("sec-fetch-site", "cross-site".parse().unwrap());
    app.send(cross).await.assert_status(StatusCode::FORBIDDEN);
    assert!(
        batches(&app)
            .fetch(&bid)
            .await
            .unwrap()
            .unwrap()
            .cancelled_at
            .is_none()
    );

    let read_only = App::build(|web| web.read_only(true), "").await;
    let bid = parked(&read_only, "read only", 1).await;
    let page = read_only.get(&format!("/batches/{bid}")).await;
    page.assert_status(StatusCode::OK);
    assert_eq!(page.count("form[method=post]"), 0);
    read_only
        .post(&format!("/batches/{bid}"), &[("cancel", "1")])
        .await
        .assert_status(StatusCode::FORBIDDEN);
    assert!(
        batches(&read_only)
            .fetch(&bid)
            .await
            .unwrap()
            .unwrap()
            .cancelled_at
            .is_none()
    );
}

#[tokio::test]
async fn callback_jobs_show_their_event_and_the_tab_can_be_turned_off() {
    let app = App::new().await;
    // An empty batch pushes its callbacks at once.
    let done = Batch::new(&client(&app))
        .on_success::<Done>(("success".to_owned(),))
        .commit()
        .await
        .unwrap()
        .bid;
    // The callback, moved to the schedule, shows its row on its page.
    let raw: Vec<String> = app
        .redis(
            hench::redis_rs::cmd("LRANGE")
                .arg("queue:default")
                .arg(0)
                .arg(-1),
        )
        .await;
    let payload: hench::JobPayload = serde_json::from_str(&raw[0]).unwrap();
    let at = common::now_secs() + 3_600.0;
    let _: () = app
        .redis(
            hench::redis_rs::cmd("ZADD")
                .arg("schedule")
                .arg(at)
                .arg(serde_json::to_string(&payload).unwrap()),
        )
        .await;
    let list = app.get("/scheduled").await;
    let entry = list
        .attrs("a[href*='/scheduled/']", "href")
        .into_iter()
        .find(|href| href.contains(&payload.jid))
        .unwrap();
    let page = app.get(&entry).await;
    page.assert_status(StatusCode::OK);
    let rows = page.texts("tr").join("\n");
    assert!(
        rows.contains(&format!("success callback of batch {done}")),
        "{rows}"
    );
    assert!(
        page.attrs("a", "href")
            .contains(&format!("/batches/{done}"))
    );

    // Off: no tab, and the pages answer 404.
    let off = App::build(|web| web.batches(false), "").await;
    let home = off.get("/").await;
    assert!(!home.texts(".nav a").contains(&"Batches".to_owned()));
    off.get("/batches")
        .await
        .assert_status(StatusCode::NOT_FOUND);
    off.get(&format!("/batches/{done}"))
        .await
        .assert_status(StatusCode::NOT_FOUND);
}

/// The calls of Redis command `command` since the last `CONFIG RESETSTAT`.
async fn calls(app: &App, command: &str) -> u64 {
    let info: String = app
        .redis(hench::redis_rs::cmd("INFO").arg("commandstats"))
        .await;
    let prefix = format!("cmdstat_{command}:calls=");
    info.lines()
        .find_map(|line| line.strip_prefix(prefix.as_str()))
        .and_then(|rest| rest.split(',').next())
        .and_then(|calls| calls.parse().ok())
        .unwrap_or(0)
}

/// A job of a retry storm, as JSON. A `crowd` job's JSON has a space after its brace, so that
/// it sorts before every compact job JSON of the same score (`ZRANGE` orders a score's members
/// by their bytes).
fn storm_job(n: usize, crowd: bool) -> String {
    let job = serde_json::json!({
        "class": "web::Storm", "args": [n], "jid": format!("storm-{n}-{crowd}"),
        "queue": "default", "retry_count": 1, "error_class": "RuntimeError",
        "error_message": "upstream timed out",
    })
    .to_string();
    if crowd {
        job.replacen('{', "{ ", 1)
    } else {
        job
    }
}

/// Adds `jobs` (score, JSON) to `set`, in one `ZADD`.
async fn zadd(app: &App, set: &str, jobs: &[(f64, String)]) {
    let mut zadd = hench::redis_rs::cmd("ZADD");
    zadd.arg(set);
    for (score, job) in jobs {
        zadd.arg(*score).arg(job);
    }
    let _: u64 = app.redis(&zadd).await;
}

/// The score of the entry of `set` holding the job `jid`.
async fn score_in(app: &App, set: &str, jid: &str) -> f64 {
    let entries: Vec<(String, f64)> = app
        .redis(
            hench::redis_rs::cmd("ZRANGE")
                .arg(set)
                .arg(0)
                .arg(-1)
                .arg("WITHSCORES"),
        )
        .await;
    entries
        .into_iter()
        .find(|(job, _)| job.contains(&format!("\"jid\":\"{jid}\"")))
        .unwrap_or_else(|| panic!("{jid} is not in {set}"))
        .1
}

/// The links of the failure rows of batch `bid`'s page, by jid, after checking that the page
/// scanned no set and looked each failure up in two pipelined reads at most.
async fn failure_links(app: &App, bid: &str) -> std::collections::BTreeMap<String, String> {
    let _: () = app
        .redis(hench::redis_rs::cmd("CONFIG").arg("RESETSTAT"))
        .await;
    let res = app.get(&format!("/batches/{bid}")).await;
    res.assert_status(StatusCode::OK);
    assert_eq!(calls(app, "zscan").await, 0, "no scan of a set");
    let links = res.attrs(
        "section[aria-labelledby=failures-heading] tbody th a",
        "href",
    );
    let lookups = calls(app, "zrange").await;
    assert!(
        lookups as usize <= 2 * links.len(),
        "{lookups} ZRANGE calls"
    );
    let jids = res.texts("section[aria-labelledby=failures-heading] tbody th a");
    jids.into_iter().zip(links).collect()
}

#[tokio::test]
async fn failures_are_located_by_score_without_scanning_the_sets() {
    // Round-3 web-api-security-1: each failure row looked its job up with `find_job`, a
    // `ZSCAN MATCH` of the whole retry set (or morgue) that reads every entry when the job is
    // not there, on every render of a Live page. A failure summary records where its job went
    // (the retry's score, the time of death): the page looks there, ten entries at most.
    let app = App::new().await;
    let (_hench, launcher, _done) = worker(&app).await;
    let bid = Batch::new(&client(&app))
        .description("storm")
        .jobs(|jobs| async move {
            for _ in 0..4 {
                Member::perform_async(&jobs, ("fail".to_owned(),)).await?;
            }
            Member::set()
                .retry(0u32)
                .perform_async(&jobs, ("fail".to_owned(),))
                .await?;
            Member::set()
                .retry(false)
                .perform_async(&jobs, ("fail".to_owned(),))
                .await?;
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap()
        .bid;
    wait_for(&app, &bid, |status| status.failures == 6).await;
    launcher.stop().await.unwrap();
    let failures = batches(&app).failures(&bid, 1, 25).await.unwrap().items;
    let jid_of = |reason: &str| -> Vec<String> {
        let mut jids: Vec<String> = failures
            .iter()
            .filter(|failure| failure.reason == reason)
            .map(|failure| failure.jid.clone())
            .collect();
        jids.sort();
        jids
    };
    let (mut retried, died, dropped) = (jid_of("retry"), jid_of("morgue"), jid_of("no_retry"));
    assert_eq!((retried.len(), died.len(), dropped.len()), (4, 1, 1));
    // One retry killed from the dashboard's API: dead in its batch (`killed`), in the morgue.
    let killed = retried.pop().unwrap();
    let entry = app
        .api
        .retries()
        .fetch(score_in(&app, "retry", &killed).await, Some(&killed))
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert!(entry.kill().await.unwrap());
    let failures = batches(&app).failures(&bid, 1, 25).await.unwrap().items;
    assert!(
        failures
            .iter()
            .any(|failure| failure.jid == killed && failure.dead && failure.reason == "killed")
    );

    // A storm: thousands of other jobs around the members in both sets, none of them between
    // a death and its write to the morgue nor within a millisecond of a retry (the crowds
    // below are the ones that come first).
    let mut retries = Vec::new();
    for jid in &retried {
        retries.push(score_in(&app, "retry", jid).await);
    }
    let mut deaths = Vec::new();
    for jid in [&died[0], &killed] {
        let failure = failures.iter().find(|failure| &failure.jid == jid).unwrap();
        let died_at = hench::system_time_to_secs(failure.failed_at.unwrap());
        let score = score_in(&app, "dead", jid).await;
        assert!(score >= died_at - 0.001, "{score} {died_at}");
        deaths.push((died_at, score));
    }
    let first = retries.iter().copied().fold(f64::INFINITY, f64::min);
    let storm: Vec<(f64, String)> = (0..5_000)
        .map(|n| (first - 25.0 + n as f64 * 0.01, storm_job(n, false)))
        .filter(|(at, _)| retries.iter().all(|retry| (at - retry).abs() > 0.001))
        .collect();
    zadd(&app, "retry", &storm).await;
    let storm: Vec<(f64, String)> = (0..5_000)
        .map(|n| (deaths[0].0 - 25.0 + n as f64 * 0.01, storm_job(n, false)))
        .filter(|(at, _)| {
            deaths
                .iter()
                .all(|(died_at, score)| *at < died_at - 0.002 || *at > score + 0.001)
        })
        .collect();
    zadd(&app, "dead", &storm).await;
    // Crowds at the very scores of two retries, read before them: four (the job is the fifth
    // entry, found in the second read) and twelve (beyond the ten entries a lookup reads).
    let crowd = |at: f64, n: usize| -> Vec<(f64, String)> {
        (0..n).map(|i| (at, storm_job(10_000 + i, true))).collect()
    };
    zadd(&app, "retry", &crowd(retries[0], 4)).await;
    zadd(&app, "retry", &crowd(retries[1], 12)).await;

    let links = failure_links(&app, &bid).await;
    assert_eq!(links.len(), 5, "{links:?}");
    assert!(
        !links.contains_key(&dropped[0]),
        "a dropped job has no entry: {links:?}"
    );
    let entry_link = |list: &str, jid: &str| {
        let link = &links[jid];
        assert!(
            link.starts_with(&format!("/{list}/")) && link.ends_with(jid),
            "{jid}: {link}"
        );
        link.clone()
    };
    for (list, jid) in [
        ("retries", &retried[0]),
        ("retries", &retried[2]),
        ("morgue", &died[0]),
        ("morgue", &killed),
    ] {
        let page = app.get(&entry_link(list, jid)).await;
        page.assert_status(StatusCode::OK);
        assert!(page.body.contains(jid.as_str()));
    }
    // Twelve others first at its score: the row links to a search of the list instead.
    assert_eq!(
        links[&retried[1]],
        format!("/retries?substr={}", retried[1])
    );

    // The members are no longer where their failures left them (their retries are running,
    // the dead jobs were deleted): nothing is scanned either, each row links to a search of
    // its list (bounded, and only when clicked).
    for set in ["retry", "dead"] {
        let members: Vec<String> = app
            .redis(hench::redis_rs::cmd("ZRANGE").arg(set).arg(0).arg(-1))
            .await;
        for member in members
            .iter()
            .filter(|member| !member.contains("web::Storm"))
        {
            let _: u64 = app
                .redis(hench::redis_rs::cmd("ZREM").arg(set).arg(member))
                .await;
        }
    }
    let links = failure_links(&app, &bid).await;
    let expected: std::collections::BTreeMap<String, String> = retried
        .iter()
        .map(|jid| (jid.clone(), format!("/retries?substr={jid}")))
        .chain(
            [&died[0], &killed]
                .into_iter()
                .map(|jid| (jid.clone(), format!("/morgue?substr={jid}"))),
        )
        .collect();
    assert_eq!(links, expected);
}
