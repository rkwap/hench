//! The progress of iterable jobs on the busy page, the retries and the job pages (Sidekiq's
//! `web_test` iteration scenarios: busy shows `exec=7`, the retry page shows the iteration
//! row).

mod common;

use common::*;
use hench::keys;
use hench::redis_rs;
use http::StatusCode;
use serde_json::json;

/// Saves an iteration state for `jid` like an iterable job does.
async fn add_iteration_state(app: &App, jid: &str, cursor: &str, executions: u64, runtime: f64) {
    let _: i64 = app
        .redis(
            redis_rs::cmd("HSET")
                .arg(keys::iteration_state(jid))
                .arg(keys::ITERATION_EXECUTIONS)
                .arg(executions)
                .arg(keys::ITERATION_CURSOR)
                .arg(cursor)
                .arg(keys::ITERATION_RUNTIME)
                .arg(runtime),
        )
        .await;
}

#[tokio::test]
async fn busy_page_shows_the_progress_of_running_jobs() {
    let app = App::new().await;
    let identity = "foo:1234:0123456789ab";
    add_process(
        &app,
        identity,
        json!({"hostname": "foo", "pid": 1234, "started_at": now_secs(), "concurrency": 10,
               "identity": identity, "queues": ["critical"]}),
        2,
    )
    .await;
    let iterable = jid();
    let plain = jid();
    for (slot, job_jid) in [("a1", &iterable), ("b2", &plain)] {
        let payload =
            json!({"class": "WebJob", "args": [1, "abc"], "jid": job_jid, "queue": "critical"});
        add_work(&app, identity, slot, "critical", &payload).await;
    }
    add_iteration_state(&app, &iterable, r#"{"offset":500}"#, 7, 12.3456).await;

    let res = app.get("/busy").await;
    res.assert_status(StatusCode::OK);
    let progress = res.texts("section[aria-labelledby=jobs-heading] .iteration");
    assert_eq!(
        progress,
        [r#"Iteration: cursor {"offset":500} · 7 executions · 12.346 s"#]
    );
    // Only the iterable job has one.
    let rows = res.texts("section[aria-labelledby=jobs-heading] tbody tr");
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows.iter().filter(|row| row.contains("Iteration")).count(),
        1
    );
}

#[tokio::test]
async fn retries_show_the_progress_of_iterable_jobs() {
    let app = App::new().await;
    let iterable = jid();
    let (job, score) = add_retry(&app, &iterable).await;
    add_retry(&app, &jid()).await;
    add_iteration_state(&app, &iterable, r#"{"id":42}"#, 3, 12.345).await;

    let res = app.get("/retries").await;
    res.assert_status(StatusCode::OK);
    assert_eq!(
        res.texts(".error-cell .iteration"),
        [r#"Iteration: cursor {"id":42} · 3 executions · 12.345 s"#]
    );

    // The job's page has an Iteration row.
    let res = app.get(&format!("/retries/{}", key(&job, score))).await;
    res.assert_status(StatusCode::OK);
    let rows = res.texts(".kv tr");
    let row = rows
        .iter()
        .find(|row| row.starts_with("Iteration"))
        .expect("an Iteration row");
    assert!(row.contains(r#"cursor {"id":42}"#), "{row}");
    assert!(row.contains("3 executions"), "{row}");
    assert_eq!(res.count(".iteration-detail .chip-warn"), 0);
}

#[tokio::test]
async fn job_pages_show_cancellations_and_whole_cursors() {
    let app = App::new().await;
    let cancelled = jid();
    let (job, score) = add_dead(&app, &cancelled).await;
    let long_cursor = format!("\"{}\"", "x".repeat(300));
    add_iteration_state(&app, &cancelled, &long_cursor, 1, 0.5).await;
    let _: i64 = app
        .redis(
            redis_rs::cmd("HSET")
                .arg(keys::iteration_state(&cancelled))
                .arg(keys::ITERATION_CANCELLED)
                .arg(now_secs()),
        )
        .await;

    let res = app.get(&format!("/morgue/{}", key(&job, score))).await;
    res.assert_status(StatusCode::OK);
    let detail = res.texts(".iteration-detail code").concat();
    // The whole cursor on the job's page.
    assert!(detail.contains(&"x".repeat(300)), "{detail}");
    assert!(detail.contains("1 execution ·"), "{detail}");
    assert_eq!(res.texts(".iteration-detail .chip-warn"), ["cancelled"]);

    // Lists cut long cursors (the morgue list shows none: like Sidekiq, only the retries do).
    let res = app.get("/morgue").await;
    assert_eq!(res.count(".iteration"), 0);
    let (retry, _) = add_retry(&app, &cancelled).await;
    let res = app.get("/retries").await;
    let progress = res.texts(".iteration").concat();
    assert!(progress.contains('…'), "{progress}");
    assert!(!progress.contains(&"x".repeat(150)), "{progress}");
    assert!(progress.contains("cancelled"), "{progress}");
    assert_eq!(retry["jid"], cancelled.as_str());

    // A job that is not iterable has no Iteration row; nor does a job without a jid.
    let (plain, score) = add_scheduled(&app, &jid()).await;
    let res = app.get(&format!("/scheduled/{}", key(&plain, score))).await;
    res.assert_status(StatusCode::OK);
    assert_eq!(res.count(".iteration-detail"), 0);
}

#[tokio::test]
async fn unreadable_cursors_are_shown_raw() {
    let app = App::new().await;
    let iterable = jid();
    let (job, score) = add_retry(&app, &iterable).await;
    add_iteration_state(&app, &iterable, "not-json <b>", 2, 1.0).await;
    let res = app.get(&format!("/retries/{}", key(&job, score))).await;
    let detail = res.texts(".iteration-detail code").concat();
    assert!(detail.contains(r#"cursor "not-json <b>""#), "{detail}");
    assert!(!res.body.contains("not-json <b>"), "escaped");
}
