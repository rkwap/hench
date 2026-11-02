//! Every action changes Redis as expected and redirects back with a one-shot notice:
//! scenarios of Sidekiq's web_test (busy, queue and set actions) and web_action_flash_test.

mod common;

use common::*;
use hench::redis_rs;
use http::StatusCode;
use serde_json::json;

async fn add_identity(app: &App, identity: &str, embedded: bool) {
    add_process(
        app,
        identity,
        json!({"hostname": "foo", "identity": identity, "embedded": embedded}),
        0,
    )
    .await;
}

#[tokio::test]
async fn quiet_and_stop_one_process_or_all() {
    let app = App::new().await;
    add_identity(&app, "identity", false).await;
    add_identity(&app, "embedded", true).await;

    assert_eq!(pop_signal(&app, "identity").await, None);
    let res = app
        .post("/busy", &[("quiet", "1"), ("identity", "identity")])
        .await;
    res.assert_status(StatusCode::SEE_OTHER);
    assert_eq!(res.location(), "/busy");
    assert_eq!(pop_signal(&app, "identity").await.as_deref(), Some("TSTP"));

    app.post("/busy", &[("stop", "1"), ("identity", "identity")])
        .await
        .assert_status(StatusCode::SEE_OTHER);
    assert_eq!(pop_signal(&app, "identity").await.as_deref(), Some("TERM"));

    // "All" skips embedded processes, like Sidekiq.
    app.post("/busy", &[("quiet", "Quiet all")])
        .await
        .assert_status(StatusCode::SEE_OTHER);
    assert_eq!(pop_signal(&app, "identity").await.as_deref(), Some("TSTP"));
    assert_eq!(pop_signal(&app, "embedded").await, None);
    let res = app.post("/busy", &[("stop", "Stop all")]).await;
    assert_eq!(pop_signal(&app, "identity").await.as_deref(), Some("TERM"));
    assert_eq!(pop_signal(&app, "embedded").await, None);
    assert!(
        res.cookie("hench_flash")
            .unwrap()
            .starts_with("hench_flash=stopped.1.0.")
    );

    // An embedded process can still be signalled on its own.
    app.post("/busy", &[("quiet", "1"), ("identity", "embedded")])
        .await;
    assert_eq!(pop_signal(&app, "embedded").await.as_deref(), Some("TSTP"));

    // A process that is gone: nothing is sent, the notice says so.
    let res = app
        .post("/busy", &[("stop", "1"), ("identity", "ghost")])
        .await;
    res.assert_status(StatusCode::SEE_OTHER);
    assert!(
        res.cookie("hench_flash")
            .unwrap()
            .starts_with("hench_flash=nopid.")
    );
    assert_eq!(pop_signal(&app, "ghost").await, None);

    // No action named: a bad request.
    app.post("/busy", &[("identity", "identity")])
        .await
        .assert_status(StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn queues_can_be_deleted_paused_and_resumed() {
    let app = App::new().await;
    push_raw(
        &app,
        "foo",
        &json!({"args": [], "enqueued_at": 1_567_894_960}),
    )
    .await;
    app.get("/queues/foo").await.assert_status(StatusCode::OK);

    let res = app.post("/queues/foo", &[("pause", "1")]).await;
    res.assert_status(StatusCode::SEE_OTHER);
    assert_eq!(res.location(), "/queues");
    assert!(app.api.queue("foo").is_paused().await.unwrap());
    // From the queue's own page, back to it.
    let res = app
        .post("/queues/foo", &[("unpause", "1"), ("return_to", "queue")])
        .await;
    assert_eq!(res.location(), "/queues/foo");
    assert!(!app.api.queue("foo").is_paused().await.unwrap());

    let res = app
        .post("/queues/foo", &[("delete", "1"), ("return_to", "queue")])
        .await;
    res.assert_status(StatusCode::SEE_OTHER);
    // Deleting always goes back to the list: the queue's page is gone.
    assert_eq!(res.location(), "/queues");
    let queues: Vec<String> = app.redis(redis_rs::cmd("SMEMBERS").arg("queues")).await;
    assert!(!queues.contains(&"foo".to_owned()));
    let exists: bool = app.redis(redis_rs::cmd("EXISTS").arg("queue:foo")).await;
    assert!(!exists);
    app.get("/queues/foo")
        .await
        .assert_status(StatusCode::NOT_FOUND);

    // A POST naming no action is refused (Sidekiq deletes the queue).
    push_raw(&app, "bar", &json!({"args": []})).await;
    app.post("/queues/bar", &[])
        .await
        .assert_status(StatusCode::BAD_REQUEST);
    assert_eq!(llen(&app, "bar").await, 1);
}

#[tokio::test]
async fn one_job_can_be_deleted_from_a_queue() {
    let app = App::new().await;
    for job in [
        r#"{"args":[],"enqueued_at":1567894960}"#,
        r#"{"foo":"bar","args":[],"enqueued_at":1567894960}"#,
        r#"{"foo2":"bar2","args":[],"enqueued_at":1567894960}"#,
    ] {
        let _: i64 = app
            .redis(redis_rs::cmd("RPUSH").arg("queue:foo").arg(job))
            .await;
    }
    let _: i64 = app
        .redis(redis_rs::cmd("SADD").arg("queues").arg("foo"))
        .await;
    let page = app.get("/queues/foo").await;
    page.assert_status(StatusCode::OK);
    // Each row's form names the job by its place and the digest of its JSON (newest first:
    // the jobs were pushed to the end, so `foo2` comes first).
    let keys = page.attrs("input[name=key]", "value");
    assert_eq!(keys.len(), 3);
    assert!(keys.iter().all(|key| key.len() < 50), "{keys:?}");
    let key = keys[1].clone();
    assert!(key.starts_with("1-"), "{key}");

    let res = app
        .post(
            "/queues/foo/delete",
            &[("key", key.as_str()), ("delete", "1")],
        )
        .await;
    res.assert_status(StatusCode::SEE_OTHER);
    assert_eq!(res.location(), "/queues/foo");
    let left: Vec<String> = app
        .redis(redis_rs::cmd("LRANGE").arg("queue:foo").arg(0).arg(-1))
        .await;
    assert_eq!(left.len(), 2);
    assert!(!left.iter().any(|job| job.contains("\"foo\":\"bar\"")));

    // Deleting it again: it is gone.
    let res = app
        .post(
            "/queues/foo/delete",
            &[("key", key.as_str()), ("delete", "1")],
        )
        .await;
    assert!(
        res.cookie("hench_flash")
            .unwrap()
            .starts_with("hench_flash=gone.")
    );
}

#[tokio::test]
async fn queue_pages_stay_small_whatever_the_job_size() {
    // web-security-5: every row used to carry its job's whole JSON (HTML-escaped) in a hidden
    // field; 25 jobs of 200 KB made a 10 MB page.
    let app = App::new().await;
    let document: Vec<String> = (0..20_000).map(|i| format!("row{i:05}")).collect();
    for _ in 0..25 {
        let mut job = hench::JobPayload::new("Docs::Import", vec![json!({ "rows": document })]);
        job.queue = "big".to_owned();
        app.api.client().push(job).await.unwrap();
    }
    let page = app.get("/queues/big").await;
    page.assert_status(StatusCode::OK);
    let keys = page.attrs("input[name=key]", "value");
    assert_eq!(keys.len(), 25);
    assert!(keys.iter().all(|key| key.len() < 50), "{keys:?}");
    // What remains is each row's arguments, cut at 10,000 characters for "Show all".
    assert!(
        page.body.len() < 1_000_000,
        "a 25-row queue page is {} bytes",
        page.body.len()
    );
    // The rows still delete their own job.
    let key = page.attrs("input[name=key]", "value").remove(3);
    app.post(
        "/queues/big/delete",
        &[("key", key.as_str()), ("delete", "1")],
    )
    .await
    .assert_status(StatusCode::SEE_OTHER);
    assert_eq!(llen(&app, "big").await, 24);
}

#[tokio::test]
async fn queue_rows_find_their_job_after_it_moved() {
    let app = App::new().await;
    for index in 0..30 {
        push_raw(&app, "busy", &json!({"class": "Job", "args": [index]})).await;
    }
    // Page 2 (newest first) shows jobs 4 down to 0.
    let page = app.get("/queues/busy?page=2&count=25").await;
    let keys = page.attrs("input[name=key]", "value");
    assert_eq!(page.texts("td.args-cell"), ["4", "3", "2", "1", "0"]);
    // Since the page was shown, 250 jobs were pushed and the newest job was deleted.
    for index in 30..280 {
        push_raw(&app, "busy", &json!({"class": "Job", "args": [index]})).await;
    }
    let _: Option<String> = app.redis(redis_rs::cmd("LPOP").arg("queue:busy")).await;
    let res = app
        .post(
            "/queues/busy/delete",
            &[("key", keys[1].as_str()), ("delete", "1")],
        )
        .await;
    assert!(
        res.cookie("hench_flash")
            .unwrap()
            .starts_with("hench_flash=deleted.1.")
    );
    let left: Vec<String> = app
        .redis(redis_rs::cmd("LRANGE").arg("queue:busy").arg(0).arg(-1))
        .await;
    assert_eq!(left.len(), 278);
    assert!(!left.iter().any(|job| job.contains("\"args\":[3]")));
    // A key whose job ran in the meantime: gone, nothing else deleted.
    let _: i64 = app
        .redis(
            redis_rs::cmd("LREM")
                .arg("queue:busy")
                .arg(1)
                .arg(json!({"class": "Job", "args": [2]}).to_string()),
        )
        .await;
    let res = app
        .post(
            "/queues/busy/delete",
            &[("key", keys[2].as_str()), ("delete", "1")],
        )
        .await;
    assert!(
        res.cookie("hench_flash")
            .unwrap()
            .starts_with("hench_flash=gone.")
    );
    assert_eq!(llen(&app, "busy").await, 277);
    // Malformed keys are refused.
    for key in ["", "3", "x-abc", "3-not-a-digest"] {
        app.post("/queues/busy/delete", &[("key", key), ("delete", "1")])
            .await
            .assert_status(StatusCode::BAD_REQUEST);
    }
    assert_eq!(llen(&app, "busy").await, 277);
}

#[tokio::test]
async fn jobs_with_line_breaks_can_be_deleted_from_a_queue() {
    // web-functional-13: browsers send line breaks of form fields as CRLF, so a job stored
    // with LF (pretty-printed by another producer) never matched its hidden JSON field.
    let app = App::new().await;
    let pretty =
        "{\"class\":\"PrettyJob\",\n \"args\":[1],\n \"jid\":\"abcdefabcdefabcdefabcdef\"}";
    let _: i64 = app
        .redis(redis_rs::cmd("LPUSH").arg("queue:nlq").arg(pretty))
        .await;
    let _: i64 = app
        .redis(redis_rs::cmd("SADD").arg("queues").arg("nlq"))
        .await;
    let page = app.get("/queues/nlq").await;
    let key = page.attrs("input[name=key]", "value").remove(0);
    assert!(!key.contains('\n'));
    let res = app
        .post(
            "/queues/nlq/delete",
            &[("key", key.as_str()), ("delete", "1")],
        )
        .await;
    assert!(
        res.cookie("hench_flash")
            .unwrap()
            .starts_with("hench_flash=deleted.1.")
    );
    assert_eq!(llen(&app, "nlq").await, 0);
}

#[tokio::test]
async fn queues_with_control_characters_in_their_name_can_be_managed() {
    // web-security-7: such queues were listed with links and buttons that all answered 404.
    let app = App::new().await;
    let name = "evil\u{1b}[31mRED\u{1b}[0m\n  default";
    let mut job = hench::JobPayload::new("X", vec![]);
    job.queue = name.to_owned();
    app.api.client().push(job).await.unwrap();

    let list = app.get("/queues").await;
    let href = list
        .attrs("a.queue-name", "href")
        .into_iter()
        .find(|href| href.starts_with("/queues/evil"))
        .expect("the queue is listed");
    assert_eq!(
        href,
        "/queues/evil%1B[31mRED%1B[0m%0A%20%20default"
            .replace('[', "%5B")
            .replace(']', "%5D")
    );
    // Its name is shown with the control characters spelled out.
    assert!(
        list.texts("a.queue-name")
            .contains(&"evil\\u{1b}[31mRED\\u{1b}[0m\\n  default".to_owned()),
        "{:?}",
        list.texts("a.queue-name")
    );
    let page = app.get(&href).await;
    page.assert_status(StatusCode::OK);
    assert_eq!(page.count("tbody tr"), 1);
    let key = page.attrs("input[name=key]", "value").remove(0);
    app.post(
        &format!("{href}/delete"),
        &[("key", key.as_str()), ("delete", "1")],
    )
    .await
    .assert_status(StatusCode::SEE_OTHER);
    assert_eq!(app.api.queue(name).size().await.unwrap(), 0);
    push_raw(&app, name, &json!({"class": "X", "args": []})).await;
    let res = app.post(&href, &[("pause", "1")]).await;
    res.assert_status(StatusCode::SEE_OTHER);
    assert!(app.api.queue(name).is_paused().await.unwrap());
    let res = app.post(&href, &[("delete", "1")]).await;
    res.assert_status(StatusCode::SEE_OTHER);
    assert_eq!(app.api.queue(name).size().await.unwrap(), 0);
    // The notice names it readably.
    let notice = app
        .send(
            http::Request::get("/queues")
                .header(
                    "cookie",
                    res.cookie("hench_flash")
                        .unwrap()
                        .split(';')
                        .next()
                        .unwrap(),
                )
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await;
    assert!(
        notice
            .texts(".notice")
            .concat()
            .contains("evil\\u{1b}[31mRED"),
        "{:?}",
        notice.texts(".notice")
    );
}

#[tokio::test]
async fn retries_can_be_deleted_one_by_one_selected_or_all() {
    let app = App::new().await;
    let (job, score) = add_retry(&app, &jid()).await;
    let res = app
        .post(
            "/retries",
            &[("key", &key(&job, score)), ("delete", "Delete")],
        )
        .await;
    res.assert_status(StatusCode::SEE_OTHER);
    assert_eq!(res.location(), "/retries");
    assert_eq!(zcard(&app, "retry").await, 0);

    let (job, score) = add_retry(&app, &jid()).await;
    let res = app
        .post(
            &format!("/retries/{}", key(&job, score)),
            &[("delete", "Delete")],
        )
        .await;
    assert_eq!(res.location(), "/retries");
    assert_eq!(zcard(&app, "retry").await, 0);

    for _ in 0..3 {
        add_retry(&app, &jid()).await;
    }
    let res = app
        .post("/retries/all/delete", &[("delete", "Delete")])
        .await;
    res.assert_status(StatusCode::SEE_OTHER);
    assert_eq!(res.location(), "/retries");
    assert_eq!(zcard(&app, "retry").await, 0);
    assert!(
        res.cookie("hench_flash")
            .unwrap()
            .starts_with("hench_flash=deleted.3.0.")
    );
}

#[tokio::test]
async fn retries_can_be_retried_or_killed() {
    let app = App::new().await;
    // Retry one now: it goes to its queue.
    let (job, score) = add_retry(&app, &jid()).await;
    let res = app
        .post(
            &format!("/retries/{}", key(&job, score)),
            &[("retry", "Retry")],
        )
        .await;
    assert_eq!(res.location(), "/retries");
    assert_eq!(llen(&app, "default").await, 1);
    let queue = app.get("/queues/default").await;
    assert!(queue.body.contains(&job["args"][2].to_string()));

    // Kill one now: it goes to the morgue.
    let (job, score) = add_retry(&app, &jid()).await;
    app.post(
        &format!("/retries/{}", key(&job, score)),
        &[("kill", "Kill")],
    )
    .await
    .assert_status(StatusCode::SEE_OTHER);
    assert_eq!(zcard(&app, "dead").await, 1);
    let morgue = app.get("/morgue").await;
    assert!(morgue.body.contains(job["jid"].as_str().unwrap()));

    // Selected jobs.
    let (first, first_score) = add_retry(&app, &jid()).await;
    let (second, second_score) = add_retry(&app, &jid()).await;
    let res = app
        .post(
            "/retries",
            &[
                ("key", &key(&first, first_score)),
                ("key", &key(&second, second_score)),
                ("retry", "1"),
            ],
        )
        .await;
    assert!(
        res.cookie("hench_flash")
            .unwrap()
            .starts_with("hench_flash=retried.2.0.")
    );
    assert_eq!(llen(&app, "default").await, 3);

    // All of them.
    for _ in 0..2 {
        add_retry(&app, &jid()).await;
    }
    app.post("/retries/all/retry", &[]).await;
    assert_eq!(zcard(&app, "retry").await, 0);
    assert_eq!(llen(&app, "default").await, 5);
    for _ in 0..3 {
        add_retry(&app, &jid()).await;
    }
    let res = app.post("/retries/all/kill", &[]).await;
    assert_eq!(res.location(), "/retries");
    assert_eq!(zcard(&app, "retry").await, 0);
    assert_eq!(zcard(&app, "dead").await, 4);
}

#[tokio::test]
async fn scheduled_jobs_can_be_enqueued_or_deleted() {
    let app = App::new().await;
    let (job, score) = add_scheduled(&app, &jid()).await;
    let res = app
        .post(
            &format!("/scheduled/{}", key(&job, score)),
            &[("add_to_queue", "true")],
        )
        .await;
    assert_eq!(res.location(), "/scheduled");
    assert_eq!(llen(&app, "default").await, 1);
    let queue = app.get("/queues/default").await;
    assert!(queue.body.contains(&job["args"][2].to_string()));

    let (job, score) = add_scheduled(&app, &jid()).await;
    app.post(
        &format!("/scheduled/{}", key(&job, score)),
        &[("delete", "Delete")],
    )
    .await;
    assert_eq!(zcard(&app, "schedule").await, 0);

    let (job, score) = add_scheduled(&app, &jid()).await;
    app.post(
        "/scheduled",
        &[("key", &key(&job, score)), ("delete", "Delete")],
    )
    .await;
    assert_eq!(zcard(&app, "schedule").await, 0);

    let (job, score) = add_scheduled(&app, &jid()).await;
    app.post(
        "/scheduled",
        &[("key", &key(&job, score)), ("add_to_queue", "AddToQueue")],
    )
    .await;
    assert_eq!(zcard(&app, "schedule").await, 0);
    assert_eq!(llen(&app, "default").await, 2);

    // Kill is not an action of the schedule.
    let (job, score) = add_scheduled(&app, &jid()).await;
    app.post("/scheduled", &[("key", &key(&job, score)), ("kill", "1")])
        .await
        .assert_status(StatusCode::BAD_REQUEST);
    assert_eq!(zcard(&app, "schedule").await, 1);
    app.post("/scheduled/all/delete", &[]).await;
    assert_eq!(zcard(&app, "schedule").await, 0);

    let mut args = Vec::new();
    for _ in 0..3 {
        let (job, _) = add_scheduled(&app, &jid()).await;
        args.push(job["args"][2].to_string());
    }
    let res = app.post("/scheduled/all/add_to_queue", &[]).await;
    assert_eq!(res.location(), "/scheduled");
    assert_eq!(zcard(&app, "schedule").await, 0);
    assert_eq!(llen(&app, "default").await, 5);
    let queue = app.get("/queues/default").await;
    for arg in args {
        assert!(queue.body.contains(&arg));
    }
}

#[tokio::test]
async fn dead_jobs_can_be_retried_or_deleted() {
    let app = App::new().await;
    let (job, score) = add_dead(&app, &jid()).await;
    let res = app
        .post(
            "/morgue",
            &[("key", &key(&job, score)), ("delete", "Delete")],
        )
        .await;
    assert_eq!(res.location(), "/morgue");
    assert_eq!(zcard(&app, "dead").await, 0);

    for _ in 0..3 {
        add_dead(&app, &jid()).await;
    }
    app.post("/morgue/all/delete", &[]).await;
    assert_eq!(zcard(&app, "dead").await, 0);

    for _ in 0..3 {
        add_dead(&app, &jid()).await;
    }
    let res = app.post("/morgue/all/retry", &[]).await;
    assert_eq!(res.location(), "/morgue");
    assert_eq!(zcard(&app, "dead").await, 0);
    assert_eq!(llen(&app, "foo").await, 3);

    let (job, score) = add_dead(&app, &jid()).await;
    app.post(
        &format!("/morgue/{}", key(&job, score)),
        &[("retry", "Retry")],
    )
    .await;
    assert_eq!(zcard(&app, "dead").await, 0);
    // A jid with a hyphen still parses (the score is before the first hyphen).
    let (job, score) = add_dead(&app, "jid-with-hyphen").await;
    app.post(
        &format!("/morgue/{}", key(&job, score)),
        &[("retry", "Retry")],
    )
    .await
    .assert_status(StatusCode::SEE_OTHER);
    assert_eq!(zcard(&app, "dead").await, 0);
    assert_eq!(llen(&app, "foo").await, 5);
    let queue = app.get("/queues/foo").await;
    assert!(queue.body.contains(&job["args"][2].to_string()));
}

#[tokio::test]
async fn bulk_actions_need_a_selection_and_return_to_the_same_page() {
    // Reached as dash.example (the requests below name it).
    let app = App::build(
        |web| web.allowed_hosts(hench_web::AllowedHosts::new(["dash.example"])),
        "",
    )
    .await;
    let res = app.post("/retries", &[("retry", "1")]).await;
    res.assert_status(StatusCode::SEE_OTHER);
    assert!(
        res.cookie("hench_flash")
            .unwrap()
            .starts_with("hench_flash=none.")
    );

    // Back to the page, size and filter the form was sent from.
    let (job, score) = add_retry(&app, &jid()).await;
    let mut request = post_request(
        "/retries",
        &[("key", &key(&job, score)), ("delete", "1")],
        TOKEN,
        TOKEN,
    );
    request.headers_mut().insert(
        "referer",
        "http://dash.example/retries?page=2&count=5&jid=x"
            .parse()
            .unwrap(),
    );
    request
        .headers_mut()
        .insert("host", "dash.example".parse().unwrap());
    let res = app.send(request).await;
    assert_eq!(res.location(), "/retries?page=2&count=5");
    // Not from another host's page, nor from another page.
    for referer in [
        "http://elsewhere.example/retries?page=2",
        "http://dash.example/morgue?page=2",
    ] {
        let mut request = post_request("/retries", &[("retry", "1")], TOKEN, TOKEN);
        request
            .headers_mut()
            .insert("referer", referer.parse().unwrap());
        request
            .headers_mut()
            .insert("host", "dash.example".parse().unwrap());
        assert_eq!(app.send(request).await.location(), "/retries", "{referer}");
    }
    // Missing entries are skipped quietly.
    let res = app
        .post(
            "/retries",
            &[("key", "1.5-missing"), ("key", "nonsense"), ("delete", "1")],
        )
        .await;
    assert!(
        res.cookie("hench_flash")
            .unwrap()
            .starts_with("hench_flash=deleted.0.0.")
    );
}

#[tokio::test]
async fn notices_are_shown_once() {
    // Sidekiq's web_action_flash_test: stored by the action, shown and removed by the next
    // page.
    let app = App::new().await;
    for _ in 0..2 {
        add_retry(&app, &jid()).await;
    }
    let res = app.post("/retries/all/delete", &[]).await;
    let cookie = res.cookie("hench_flash").unwrap();
    assert!(cookie.contains("Max-Age=60"), "{cookie}");
    assert!(
        cookie.contains("HttpOnly") && cookie.contains("SameSite=Strict"),
        "{cookie}"
    );
    let value = cookie
        .split(';')
        .next()
        .unwrap()
        .trim_start_matches("hench_flash=")
        .to_owned();

    let shown = app
        .send(
            http::Request::get("/retries")
                .header("cookie", format!("hench_flash={value}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(shown.texts(".notice"), ["Deleted 2 jobs."]);
    let cleared = shown.cookie("hench_flash").unwrap();
    assert!(
        cleared.starts_with("hench_flash=;") && cleared.contains("Max-Age=0"),
        "{cleared}"
    );

    let later = app.get("/retries").await;
    assert_eq!(later.count(".notice"), 0);

    // A forged or stale notice cookie is ignored (and cleared).
    let forged = app
        .send(
            http::Request::get("/retries")
                .header("cookie", "hench_flash=<script>")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(forged.count(".notice"), 0);
    assert!(forged.cookie("hench_flash").is_some());
}

#[tokio::test]
async fn every_job_actions_name_the_whole_set_and_are_not_offered_under_a_filter() {
    // web-functional-1: in a filtered list the "Every job" buttons acted on the whole set,
    // after a confirmation that said "every job in this list".
    let app = App::new().await;
    let now = now_secs();
    for index in 0..40 {
        let args = if index < 6 {
            json!(["premium", index])
        } else {
            json!(["basic", index])
        };
        let job = json!({"class": "ChargeCard", "args": args, "queue": "billing", "jid": jid(),
                         "error_class": "Timeout", "error_message": "boom",
                         "failed_at": now_millis()});
        add_to_set(&app, "dead", job, now - f64::from(index)).await;
    }
    let filtered = app.get("/morgue?substr=premium").await;
    assert_eq!(filtered.count("tbody tr"), 6);
    assert_eq!(filtered.count("button[formaction]"), 0);
    // The matches can still be selected and changed (up to every match shown).
    assert_eq!(
        filtered.count("button[name=delete][data-needs-selection]"),
        1
    );

    let all = app.get("/morgue").await;
    assert_eq!(
        all.attrs("button[formaction]", "data-confirm"),
        ["Retry all 40 dead jobs now?", "Delete all 40 dead jobs?"]
    );
    for _ in 0..2 {
        add_retry(&app, &jid()).await;
    }
    let retries = app.get("/retries").await;
    assert_eq!(
        retries.attrs("button[formaction]", "data-confirm"),
        [
            "Retry all 2 retries now?",
            "Delete all 2 retries?",
            "Move all 2 retries to the dead set?"
        ]
    );
    add_scheduled(&app, &jid()).await;
    let scheduled = app.get("/scheduled").await;
    assert_eq!(
        scheduled.attrs("button[formaction]", "data-confirm"),
        [
            "Enqueue the only scheduled job now?",
            "Delete the only scheduled job?"
        ]
    );
}

/// `request` sent from `referer`, as a browser posting a form of that page.
fn from_page(
    mut request: http::Request<axum::body::Body>,
    referer: &str,
) -> http::Request<axum::body::Body> {
    request
        .headers_mut()
        .insert("referer", referer.parse().expect("referer"));
    request
}

#[tokio::test]
async fn clearing_the_last_page_shows_the_new_last_page() {
    // web-functional-4: after its last rows were deleted, the page past the end said "This
    // queue is empty." (or "No scheduled jobs found.") with no pager, while jobs remained.
    let app = App::new().await;
    for index in 0..6 {
        push_raw(
            &app,
            "static",
            &json!({"class": "StaticJob", "args": [index], "jid": jid()}),
        )
        .await;
    }
    let page2 = app.get("/queues/static?page=2&count=5").await;
    assert_eq!(page2.count("tbody tr"), 1);
    let key = page2.attrs("input[name=key]", "value").remove(0);
    let res = app
        .send(from_page(
            post_request(
                "/queues/static/delete",
                &[("key", key.as_str()), ("delete", "1")],
                TOKEN,
                TOKEN,
            ),
            "/queues/static?page=2&count=5",
        ))
        .await;
    assert_eq!(res.location(), "/queues/static?page=2&count=5");
    let after = app.get(res.location()).await;
    assert_eq!(after.texts(".count-badge"), ["5"]);
    assert_eq!(after.count("tbody tr"), 5);
    assert_eq!(after.count("p.empty"), 0);

    for index in 0..11 {
        add_to_set(
            &app,
            "schedule",
            json!({"class": "LaterJob", "args": [index], "queue": "default", "jid": jid()}),
            now_secs() + 3_600.0 + f64::from(index),
        )
        .await;
    }
    let page2 = app.get("/scheduled?page=2&count=10").await;
    let key = page2.attrs("input[name=key]", "value").remove(0);
    let res = app
        .send(from_page(
            post_request(
                "/scheduled",
                &[("key", key.as_str()), ("delete", "1")],
                TOKEN,
                TOKEN,
            ),
            "/scheduled?page=2&count=10",
        ))
        .await;
    let after = app.get(res.location()).await;
    assert_eq!(after.texts(".count-badge"), ["10"]);
    assert_eq!(after.count("tbody tr"), 10);
    assert_eq!(after.count("p.empty"), 0);
    // Far past the end, too: the last page.
    let far = app.get("/scheduled?count=3&page=99").await;
    assert_eq!(far.count("tbody tr"), 1);
    assert_eq!(far.texts(".pager-current"), ["Page 4 of 4", "Page 4 of 4"]);
}

/// A failed job with retry count 3 in `set`, scored now plus `offset` seconds.
async fn add_failed(app: &App, set: &str, queue: &str, offset: f64) -> (serde_json::Value, f64) {
    let job = json!({"class": "HardJob", "args": ["bob", 1, now_secs()], "queue": queue,
                     "error_message": "Some fake message", "error_class": "RuntimeError",
                     "retry_count": 3, "failed_at": now_millis(), "jid": jid()});
    add_to_set(app, set, job, now_secs() + offset).await
}

/// The retry counts of the jobs in `queue`, newest first.
async fn queued_counts(app: &App, queue: &str) -> Vec<Option<u64>> {
    let jobs: Vec<String> = app
        .redis(
            redis_rs::cmd("LRANGE")
                .arg(format!("queue:{queue}"))
                .arg(0)
                .arg(-1),
        )
        .await;
    jobs.iter()
        .map(|job| serde_json::from_str::<serde_json::Value>(job).unwrap()["retry_count"].as_u64())
        .collect()
}

async fn clear_queue(app: &App, queue: &str) {
    let _: i64 = app
        .redis(redis_rs::cmd("DEL").arg(format!("queue:{queue}")))
        .await;
}

#[tokio::test]
async fn retrying_from_the_dashboard_lowers_the_retry_count() {
    // Sidekiq's "can retry a single retry now", "can retry all retries" and "can retry a dead
    // job": a manual retry does not use up one of the job's automatic retries, while
    // "Enqueue now" of a scheduled job leaves its count alone (tests-9).
    let app = App::new().await;
    let (job, score) = add_failed(&app, "retry", "default", 0.0).await;
    app.post(
        &format!("/retries/{}", key(&job, score)),
        &[("retry", "Retry")],
    )
    .await
    .assert_status(StatusCode::SEE_OTHER);
    assert_eq!(queued_counts(&app, "default").await, [Some(2)]);
    clear_queue(&app, "default").await;

    let (job, score) = add_failed(&app, "retry", "default", 0.0).await;
    app.post("/retries", &[("key", &key(&job, score)), ("retry", "1")])
        .await;
    assert_eq!(queued_counts(&app, "default").await, [Some(2)]);
    clear_queue(&app, "default").await;

    add_failed(&app, "retry", "default", 0.0).await;
    add_failed(&app, "retry", "default", 1.0).await;
    app.post("/retries/all/retry", &[]).await;
    assert_eq!(queued_counts(&app, "default").await, [Some(2), Some(2)]);
    clear_queue(&app, "default").await;

    let (job, score) = add_failed(&app, "dead", "foo", 0.0).await;
    app.post(
        &format!("/morgue/{}", key(&job, score)),
        &[("retry", "Retry")],
    )
    .await;
    assert_eq!(queued_counts(&app, "foo").await, [Some(2)]);
    clear_queue(&app, "foo").await;
    let (job, score) = add_failed(&app, "dead", "foo", 0.0).await;
    app.post("/morgue", &[("key", &key(&job, score)), ("retry", "1")])
        .await;
    assert_eq!(queued_counts(&app, "foo").await, [Some(2)]);
    clear_queue(&app, "foo").await;
    add_failed(&app, "dead", "foo", 0.0).await;
    add_failed(&app, "dead", "foo", 1.0).await;
    app.post("/morgue/all/retry", &[]).await;
    assert_eq!(queued_counts(&app, "foo").await, [Some(2), Some(2)]);

    add_failed(&app, "schedule", "later", 3_600.0).await;
    let (job, score) = add_failed(&app, "schedule", "later", 7_200.0).await;
    app.post(
        &format!("/scheduled/{}", key(&job, score)),
        &[("add_to_queue", "true")],
    )
    .await;
    assert_eq!(queued_counts(&app, "later").await, [Some(3)]);
    let (job, score) = add_failed(&app, "schedule", "later", 9_000.0).await;
    app.post(
        "/scheduled",
        &[("key", &key(&job, score)), ("add_to_queue", "1")],
    )
    .await;
    assert_eq!(queued_counts(&app, "later").await, [Some(3), Some(3)]);
    app.post("/scheduled/all/add_to_queue", &[]).await;
    assert_eq!(
        queued_counts(&app, "later").await,
        [Some(3), Some(3), Some(3)]
    );
}

#[tokio::test]
async fn paused_queues_stay_listed_when_emptied() {
    // web-functional-3: a paused queue vanished from the list once deleted (or before its
    // first job), while it stayed paused: nothing showed it, nothing could resume it.
    let app = App::new().await;
    for index in 0..40 {
        push_raw(
            &app,
            "mailers",
            &json!({"class": "SendNewsletter", "args": [index], "jid": jid()}),
        )
        .await;
    }
    app.post("/queues/mailers", &[("pause", "1")])
        .await
        .assert_status(StatusCode::SEE_OTHER);
    app.post("/queues/mailers", &[("delete", "1")])
        .await
        .assert_status(StatusCode::SEE_OTHER);
    assert_eq!(llen(&app, "mailers").await, 0);
    app.api.queue("reports").pause().await.unwrap();
    let list = app.get("/queues").await;
    assert_eq!(list.texts("a.queue-name"), ["mailers", "reports"]);
    assert_eq!(list.texts("td.num"), ["0", "0.00", "0", "0.00"]);
    assert_eq!(list.count("button[name=unpause]"), 2);
    // Resuming an empty queue that was never used forgets it.
    app.post("/queues/reports", &[("unpause", "1")])
        .await
        .assert_status(StatusCode::SEE_OTHER);
    assert_eq!(app.get("/queues").await.texts("a.queue-name"), ["mailers"]);
}
