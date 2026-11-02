//! Every page renders with data: scenarios of Sidekiq's web_test (pages, titles, sorting,
//! pagination, filtering, 404s) and web_helpers_test (argument display, natural process
//! order).

mod common;

use common::*;
use http::StatusCode;
use serde_json::json;

#[tokio::test]
async fn dashboard_shows_totals_history_and_redis() {
    let app = App::new().await;
    let _: () = app
        .redis(
            hench::redis_rs::cmd("MSET")
                .arg("stat:processed")
                .arg(5)
                .arg("stat:failed")
                .arg(2),
        )
        .await;
    add_retry(&app, &jid()).await;
    let res = app.get("/").await;
    res.assert_status(StatusCode::OK);
    // The home page's title is the product name alone.
    assert_eq!(res.title(), "Hench");
    assert_eq!(res.texts("[data-total=processed]"), ["5"]);
    assert_eq!(res.texts("[data-total=failed]"), ["2"]);
    assert_eq!(res.texts("[data-total=retries]"), ["1"]);
    assert_eq!(res.texts("#redis-heading"), ["Redis"]);
    assert_eq!(res.count("#redis-version"), 1);
    assert!(!res.texts("#redis-version")[0].is_empty());
    for id in [
        "#redis-uptime",
        "#redis-connections",
        "#redis-memory",
        "#redis-peak-memory",
    ] {
        assert_eq!(res.count(id), 1, "{id}");
    }
    // The history: 30 days by default, oldest first in the chart data.
    let series: serde_json::Value =
        serde_json::from_str(&res.attrs("#history-chart", "data-series")[0]).unwrap();
    assert_eq!(series["dates"].as_array().unwrap().len(), 30);
    assert_eq!(series["processed"].as_array().unwrap().len(), 30);
    assert_eq!(res.count(".table-view tbody tr"), 30);
    assert_eq!(res.texts(".segmented [aria-current]"), ["1 month"]);
    assert_eq!(res.attrs("#live-chart", "data-stats-url"), ["/stats"]);
    // The active tab.
    assert_eq!(res.texts(".nav [aria-current=page]"), ["Dashboard"]);

    let week = app.get("/?days=7").await;
    week.assert_status(StatusCode::OK);
    assert_eq!(week.count(".table-view tbody tr"), 7);
    assert_eq!(week.texts(".segmented [aria-current]"), ["1 week"]);
    // Out-of-range histories are refused (Sidekiq answers 401).
    for days in ["0", "181", "1000000", "x"] {
        app.get(&format!("/?days={days}"))
            .await
            .assert_status(StatusCode::BAD_REQUEST);
    }
}

#[tokio::test]
async fn head_is_a_cheap_health_check() {
    let app = App::new().await;
    push_raw(&app, "default", &json!({"class": "X", "args": []})).await;
    let res = app
        .send(
            http::Request::head("/")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await;
    res.assert_status(StatusCode::OK);
    assert_eq!(res.body, "");
}

async fn busy_app() -> (App, String) {
    let app = App::new().await;
    let jid = jid();
    add_process(
        &app,
        "foo:1234:abcdef",
        json!({"hostname": "foo", "pid": 1234, "started_at": now_secs(), "concurrency": 10,
               "tag": "myapp", "labels": ["frumduz"], "queues": ["critical", "default"],
               "capsules": {"default": {"concurrency": 10, "mode": "strict",
                                        "weights": {"critical": 0, "default": 0}}},
               "identity": "foo:1234:abcdef", "version": "0.0.1-other"}),
        4,
    )
    .await;
    let _: i64 = app
        .redis(
            hench::redis_rs::cmd("HSET")
                .arg("foo:1234:abcdef")
                .arg("concurrency")
                .arg(10),
        )
        .await;
    add_work(
        &app,
        "foo:1234:abcdef",
        "1001",
        "critical",
        &json!({"class": "WebJob", "args": [1, "abc"], "jid": jid,
                "tags": ["foobar_100", "<eviltag/>"]}),
    )
    .await;
    (app, jid)
}

#[tokio::test]
async fn busy_shows_processes_and_running_jobs() {
    let (app, jid) = busy_app().await;
    let res = app.get("/busy").await;
    res.assert_status(StatusCode::OK);
    assert_eq!(res.title(), "Busy (4/10) · Hench");
    let process = res.texts(".process-cell").concat();
    assert!(process.contains("foo:1234"), "{process}");
    assert!(
        process.contains("myapp") && process.contains("frumduz"),
        "{process}"
    );
    assert!(process.contains("critical, default"), "{process}");
    // A process running another hench version says which.
    assert!(process.contains("0.0.1-other"), "{process}");
    assert!(res.body.contains("WebJob"));
    assert!(res.body.contains(&jid));
    assert!(res.body.contains("critical"));
    // Tags are escaped, never markup.
    assert_eq!(
        res.texts(".chip-tag"),
        ["frumduz", "foobar_100", "<eviltag/>"]
    );
    assert!(!res.body.contains("<eviltag/>"));
    assert_eq!(res.count("eviltag"), 0);
    // Status figures.
    let facts = res.texts(".facts dd");
    assert_eq!(facts[..4], ["1", "10", "4", "40%"]);
    // Per-process and global actions.
    assert_eq!(
        res.count("input[name=identity][value='foo:1234:abcdef']"),
        1
    );
    assert_eq!(res.texts("button[name=quiet]"), ["Quiet all", "Quiet"]);
}

#[tokio::test]
async fn busy_can_show_one_table_and_survives_odd_parameters() {
    let (app, _) = busy_app().await;
    // Sidekiq: "renders without error when requesting a page beyond available workers".
    app.get("/busy?page=99").await.assert_status(StatusCode::OK);
    // "does not crash on a negative count parameter".
    app.get("/busy?count=-1")
        .await
        .assert_status(StatusCode::OK);

    let jobs = app.get("/busy?only=jobs").await;
    jobs.assert_status(StatusCode::OK);
    assert!(jobs.body.contains("WebJob"));
    assert_eq!(jobs.count("#processes-heading"), 0);

    let processes = app.get("/busy?only=processes").await;
    processes.assert_status(StatusCode::OK);
    assert!(!processes.body.contains("WebJob"));
    assert_eq!(processes.count("#processes-heading"), 1);

    app.get("/busy?only=everything")
        .await
        .assert_status(StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn processes_are_listed_in_natural_host_order() {
    let app = App::new().await;
    for host in ["a.10", "a.2", "a.1", "b.1"] {
        add_process(
            &app,
            &format!("{host}:1:x"),
            json!({"hostname": host, "pid": 1, "started_at": now_secs()}),
            0,
        )
        .await;
    }
    let res = app.get("/busy").await;
    let names: Vec<String> = res
        .texts(".process-name")
        .iter()
        .map(|name| {
            name.split_whitespace()
                .next()
                .unwrap_or_default()
                .to_owned()
        })
        .collect();
    assert_eq!(names, ["a.1:1", "a.2:1", "a.10:1", "b.1:1"]);
}

#[tokio::test]
async fn queues_show_size_and_latency() {
    let app = App::new().await;
    app.api
        .client()
        .push({
            let mut job = hench::JobPayload::new("WebJob", vec![json!(1), json!(3)]);
            job.queue = "foo".to_owned();
            job
        })
        .await
        .unwrap();
    let res = app.get("/queues").await;
    res.assert_status(StatusCode::OK);
    assert_eq!(res.title(), "Queues (1) · Hench");
    assert_eq!(res.texts(".queue-name"), ["foo"]);
    assert_eq!(res.texts("tbody td.num")[0], "1");
    assert!(res.texts("tbody td.num")[1].starts_with("0.0"));
    assert_eq!(res.count("tbody time"), 0);

    // A job waiting for 65 seconds: the latency and when it was enqueued.
    app.api.queue("foo").clear().await.unwrap();
    push_raw(
        &app,
        "foo",
        &json!({"class": "WebJob", "args": [1, 3], "enqueued_at": now_millis() - 65_000}),
    )
    .await;
    let res = app.get("/queues").await;
    let latency = res.texts("tbody td.num")[1].clone();
    assert!(latency.starts_with("65."), "{latency}");
    assert_eq!(res.count("tbody time[datetime]"), 1);
}

#[tokio::test]
async fn queue_pages_exist_only_for_known_queues() {
    let app = App::new().await;
    // Sidekiq answers 404 for names outside [a-z_:.-0-9]; any name works here, but an unknown
    // queue is a 404.
    app.get("/queues/onmouseover=alert()")
        .await
        .assert_status(StatusCode::NOT_FOUND);
    app.get("/queues/default")
        .await
        .assert_status(StatusCode::NOT_FOUND);

    push_raw(&app, "foo_bar:123-wow.", &json!({"class": "X", "args": []})).await;
    let res = app.get("/queues/foo_bar:123-wow.").await;
    res.assert_status(StatusCode::OK);
    assert!(res.body.contains("foo_bar:123-wow."));
    assert_eq!(res.title(), "Queue “foo_bar:123-wow.” · Hench");

    // A paused queue exists before its first job.
    app.api.queue("later").pause().await.unwrap();
    let res = app.get("/queues/later").await;
    res.assert_status(StatusCode::OK);
    assert_eq!(res.texts("h1 .chip-warn"), ["paused"]);
    assert_eq!(res.texts(".empty"), ["This queue is empty."]);

    // Names with slashes and spaces round-trip through the URL.
    push_raw(&app, "a/b c", &json!({"class": "X", "args": []})).await;
    let res = app.get("/queues").await;
    assert!(
        res.attrs(".queue-name", "href")
            .contains(&"/queues/a%2Fb%20c".to_owned())
    );
    app.get("/queues/a%2Fb%20c")
        .await
        .assert_status(StatusCode::OK);
}

#[tokio::test]
async fn queue_jobs_can_be_sorted_both_ways() {
    let app = App::new().await;
    for index in 1000..=1005 {
        push_raw(
            &app,
            "default",
            &json!({"class": "X", "args": [index], "enqueued_at": now_millis() + index}),
        )
        .await;
    }
    // Newest first by default.
    let res = app.get("/queues/default?count=3").await;
    let args = res.texts("code.args");
    assert_eq!(args, ["1005", "1004", "1003"]);
    assert_eq!(res.texts("tbody td.num"), ["6", "5", "4"]);
    let res = app.get("/queues/default?count=3&direction=asc").await;
    assert_eq!(res.texts("code.args"), ["1000", "1001", "1002"]);
    assert_eq!(res.texts("tbody td.num"), ["1", "2", "3"]);
    assert_eq!(res.texts(".segmented [aria-current]"), ["Oldest first"]);
    app.get("/queues/default?direction=sideways")
        .await
        .assert_status(StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn lists_are_paginated() {
    let app = App::new().await;
    for _ in 0..30 {
        add_retry(&app, &jid()).await;
    }
    let first = app.get("/retries").await;
    assert_eq!(first.count("[data-check-item]"), 25);
    assert_eq!(first.title(), "Retries (30) · Hench");
    let next: Vec<String> = first.attrs(".pager a[rel=next]", "href");
    assert_eq!(next[0], "/retries?page=2");
    let second = app.get("/retries?page=2").await;
    assert_eq!(second.count("[data-check-item]"), 5);
    assert_eq!(second.texts(".pager-current")[0], "Page 2 of 2");
    assert!(
        second
            .attrs(".pager a[rel=prev]", "href")
            .contains(&"/retries?page=1".to_owned())
    );
    // Page sizes carry over.
    let small = app.get("/retries?count=10&page=2").await;
    assert_eq!(small.count("[data-check-item]"), 10);
    assert!(
        small
            .attrs(".pager a[rel=next]", "href")
            .contains(&"/retries?page=3&count=10".to_owned())
    );
    // A page past the end is empty, not an error.
    let past = app.get("/retries?page=50").await;
    past.assert_status(StatusCode::OK);
    // A page size above the limit falls back on the default.
    assert_eq!(
        app.get("/retries?count=5000")
            .await
            .count("[data-check-item]"),
        25
    );
}

#[tokio::test]
async fn retries_list_and_detail() {
    let app = App::new().await;
    let res = app.get("/retries").await;
    res.assert_status(StatusCode::OK);
    assert_eq!(res.texts(".empty"), ["No retries found."]);
    assert!(!res.body.contains("HardJob"));

    let (job, score) = add_retry(&app, &jid()).await;
    let res = app.get("/retries").await;
    assert!(res.body.contains("HardJob"));
    assert_eq!(res.count(".empty"), 0);
    assert_eq!(
        res.texts(".error-cell"),
        ["RuntimeError: Some fake message"]
    );
    assert_eq!(res.attrs("[data-check-item]", "value"), [key(&job, score)]);

    app.get("/retries/0-shouldntexist")
        .await
        .assert_status(StatusCode::NOT_FOUND);
    app.get("/retries/garbage")
        .await
        .assert_status(StatusCode::NOT_FOUND);
    let detail = app.get(&format!("/retries/{}", key(&job, score))).await;
    detail.assert_status(StatusCode::OK);
    assert!(detail.body.contains("HardJob"));
    assert!(detail.body.contains("Some fake message"));
    assert!(detail.body.contains("RuntimeError"));
    assert_eq!(detail.texts(".nav [aria-current=page]"), ["Retries"]);
    assert_eq!(detail.count("button[name=retry]"), 1);
    assert_eq!(detail.count("button[name=kill]"), 1);
}

#[tokio::test]
async fn scheduled_list_and_detail() {
    let app = App::new().await;
    let res = app.get("/scheduled").await;
    assert_eq!(res.texts(".empty"), ["No scheduled jobs found."]);
    let (job, score) = add_scheduled(&app, &jid()).await;
    let res = app.get("/scheduled").await;
    assert!(res.body.contains("HardJob"));
    assert_eq!(res.title(), "Scheduled jobs (1) · Hench");
    // Tags link to the filtered list.
    assert_eq!(
        res.attrs("a.chip-tag", "href"),
        ["/scheduled?substr=tag1", "/scheduled?substr=tag2"]
    );
    app.get("/scheduled/0-shouldntexist")
        .await
        .assert_status(StatusCode::NOT_FOUND);
    let detail = app.get(&format!("/scheduled/{}", key(&job, score))).await;
    detail.assert_status(StatusCode::OK);
    assert!(detail.body.contains("HardJob"));
    assert_eq!(detail.texts(".chip-tag"), ["tag1", "tag2"]);
    assert_eq!(detail.count("button[name=add_to_queue]"), 1);
}

#[tokio::test]
async fn morgue_list_and_detail() {
    let app = App::new().await;
    assert_eq!(
        app.get("/morgue").await.texts(".empty"),
        ["No dead jobs found."]
    );
    let (job, score) = add_dead(&app, &jid()).await;
    let res = app.get("/morgue").await;
    res.assert_status(StatusCode::OK);
    assert!(res.body.contains(&job["jid"].as_str().unwrap().to_owned()));
    assert_eq!(res.title(), "Dead jobs (1) · Hench");
    let detail = app.get(&format!("/morgue/{}", key(&job, score))).await;
    detail.assert_status(StatusCode::OK);
    assert!(detail.body.contains("Some fake message"));
    app.get("/morgue/1-nope")
        .await
        .assert_status(StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn titles_carry_counts() {
    let app = App::new().await;
    push_raw(&app, "foo", &json!({"class": "WebJob", "args": [1, 3]})).await;
    for (path, title) in [
        ("/queues", "Queues (1) · Hench"),
        ("/retries", "Retries (0) · Hench"),
        ("/scheduled", "Scheduled jobs (0) · Hench"),
        ("/morgue", "Dead jobs (0) · Hench"),
    ] {
        assert_eq!(app.get(path).await.title(), title, "{path}");
    }
}

#[tokio::test]
async fn invalid_json_in_the_morgue_is_shown_and_can_be_deleted() {
    let app = App::new().await;
    let score = now_secs();
    let _: i64 = app
        .redis(
            hench::redis_rs::cmd("ZADD")
                .arg("dead")
                .arg(score)
                .arg("{ something bad }"),
        )
        .await;
    let res = app.get("/morgue").await;
    res.assert_status(StatusCode::OK);
    assert!(res.body.contains("something bad"));
    let key = format!("{score}-");
    assert_eq!(
        res.attrs("[data-check-item]", "value"),
        std::slice::from_ref(&key)
    );
    let detail = app.get(&format!("/morgue/{key}")).await;
    detail.assert_status(StatusCode::OK);
    assert!(detail.body.contains("This job is not valid JSON"));
    // Sidekiq cannot delete such an entry by key (it has no jid); hench can.
    app.post(&format!("/morgue/{key}"), &[("delete", "1")])
        .await
        .assert_status(StatusCode::SEE_OTHER);
    assert_eq!(zcard(&app, "dead").await, 0);
}

#[tokio::test]
async fn filters_search_every_set() {
    let app = App::new().await;
    for _ in 0..3 {
        add_retry(&app, &jid()).await;
        add_scheduled(&app, &jid()).await;
        add_dead(&app, &jid()).await;
    }
    add_retry(&app, "MIKE1234").await;
    add_scheduled(&app, "MIKE1234").await;
    add_dead(&app, "MIKE1234").await;
    for list in ["/retries", "/scheduled", "/morgue"] {
        let none = app.get(&format!("{list}?substr=nope")).await;
        none.assert_status(StatusCode::OK);
        assert!(!none.body.contains("HardJob"), "{list}");
        assert_eq!(none.texts(".empty"), ["No jobs match this filter."]);
        let found = app.get(&format!("{list}?substr=MIKE1234")).await;
        assert!(found.body.contains("MIKE1234"), "{list}");
        assert_eq!(found.count("[data-check-item]"), 1, "{list}");
        assert_eq!(
            found.texts(".filter-summary"),
            ["1 job matching “MIKE1234”."]
        );
        assert_eq!(found.attrs("#filter-input", "value"), ["MIKE1234"]);
        // Glob metacharacters are taken literally unless the filter has a `*`.
        let literal = app.get(&format!("{list}?substr=MIKE%3F234")).await;
        assert_eq!(literal.count("[data-check-item]"), 0, "{list}");
        let glob = app.get(&format!("{list}?substr=*MIKE*")).await;
        assert_eq!(glob.count("[data-check-item]"), 1, "{list}");
    }
    // Queues filter too, on a literal substring of the job.
    push_raw(&app, "default", &json!({"class": "X", "args": ["needle"]})).await;
    push_raw(&app, "default", &json!({"class": "X", "args": ["hay"]})).await;
    let res = app.get("/queues/default?substr=needle").await;
    assert_eq!(res.texts("code.args"), ["\"needle\""]);
    assert_eq!(res.texts(".filter-summary"), ["1 job matching “needle”."]);
}

#[tokio::test]
async fn long_arguments_are_cut_in_lists_and_whole_on_detail_pages() {
    let app = App::new().await;
    let long = "x".repeat(500);
    let job = json!({"class": "HardJob", "args": [long], "jid": jid(), "queue": "default",
                     "error_class": "E", "error_message": "m"});
    let (job, score) = add_to_set(&app, "retry", job, now_secs()).await;
    let list = app.get("/retries").await;
    let short = list.texts("summary code.args")[0].clone();
    assert!(
        short.chars().count() < 200 && short.ends_with('…'),
        "{short}"
    );
    assert_eq!(list.count("details.args-toggle"), 1);
    assert!(list.texts("code.args-full")[0].contains(&long));
    let detail = app.get(&format!("/retries/{}", key(&job, score))).await;
    assert!(detail.texts(".arg-list pre")[0].contains(&long));
}

#[tokio::test]
async fn encrypted_arguments_stay_hidden() {
    let app = App::new().await;
    let job = json!({"class": "Secret", "args": ["visible", "CIPHERTEXT"], "encrypt": true,
                     "jid": jid(), "queue": "default", "error_class": "E", "error_message": "m"});
    let (job, score) = add_to_set(&app, "retry", job, now_secs()).await;
    let list = app.get("/retries").await;
    assert!(list.body.contains("visible"));
    assert!(!list.body.contains("CIPHERTEXT"));
    let detail = app.get(&format!("/retries/{}", key(&job, score))).await;
    assert!(detail.body.contains("visible") && detail.body.contains("[encrypted]"));
    assert!(!detail.body.contains("CIPHERTEXT"));
}

#[tokio::test]
async fn unknown_pages_are_404_pages_with_navigation() {
    let app = App::new().await;
    let res = app.get("/nowhere").await;
    res.assert_status(StatusCode::NOT_FOUND);
    assert_eq!(res.texts("h1"), ["Not found"]);
    // Dashboard, Busy, Queues, Retries, Scheduled, Dead, Periodic, Batches, Limits, Locks,
    // Metrics.
    assert_eq!(res.count(".nav a"), 11);
    assert_eq!(res.count(".nav [aria-current]"), 0);
    // An action on every job that does not exist for that set.
    app.post("/morgue/all/kill", &[])
        .await
        .assert_status(StatusCode::NOT_FOUND);
    // Unsupported methods.
    let res = app
        .send(
            http::Request::patch("/")
                .header("sec-fetch-site", "same-origin")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(res.status, StatusCode::METHOD_NOT_ALLOWED);
}

#[tokio::test]
async fn custom_tabs_link_to_host_routes() {
    // Sidekiq's webext_test: an extension adds a tab and routes to the dashboard.
    let redis = hench::test_util::TestRedis::start().await;
    let pool = redis.pool().await;
    let dashboard = hench_web::router(
        hench_web::HenchWeb::new(&pool)
            .tab("Reports", "reports")
            .tab("Admin", "/admin")
            .app_url("/"),
    )
    .route("/reports", axum::routing::get(|| async { "My reports" }));
    let app = axum::Router::new().nest("/hench", dashboard);
    let get = |path: &'static str| {
        let app = app.clone();
        async move {
            use tower::ServiceExt;
            let response = app
                .oneshot(
                    http::Request::get(path)
                        .body(axum::body::Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            let status = response.status();
            let body = http_body_util::BodyExt::collect(response.into_body())
                .await
                .unwrap()
                .to_bytes();
            (status, String::from_utf8_lossy(&body).into_owned())
        }
    };
    let (status, html) = get("/hench/queues").await;
    assert_eq!(status, StatusCode::OK);
    let document = scraper::Html::parse_document(&html);
    let tabs: Vec<(String, String)> = document
        .select(&scraper::Selector::parse(".nav a").unwrap())
        .map(|a| {
            (
                a.text().collect(),
                a.value().attr("href").unwrap_or_default().to_owned(),
            )
        })
        .collect();
    // Custom tabs come after the built-in ones, Metrics last of those (like Sidekiq).
    assert_eq!(
        tabs[6],
        ("Periodic".to_owned(), "/hench/periodic".to_owned())
    );
    assert_eq!(tabs[7], ("Batches".to_owned(), "/hench/batches".to_owned()));
    assert_eq!(tabs[8], ("Limits".to_owned(), "/hench/limits".to_owned()));
    assert_eq!(tabs[9], ("Locks".to_owned(), "/hench/locks".to_owned()));
    assert_eq!(
        tabs[10],
        ("Metrics".to_owned(), "/hench/metrics".to_owned())
    );
    assert_eq!(
        tabs[11],
        ("Reports".to_owned(), "/hench/reports".to_owned())
    );
    assert_eq!(tabs[12], ("Admin".to_owned(), "/admin".to_owned()));
    let back: Vec<_> = document
        .select(&scraper::Selector::parse(".header-tools a").unwrap())
        .map(|a| a.value().attr("href").unwrap_or_default().to_owned())
        .collect();
    assert_eq!(back, ["/"]);
    assert_eq!(
        get("/hench/reports").await,
        (StatusCode::OK, "My reports".to_owned())
    );
}

/// `GET path`, in a task of its own so that a panicking handler fails with its message.
async fn status_of(app: &App, path: &str) -> Result<StatusCode, String> {
    use tower::ServiceExt;
    let router = app.router.clone();
    let request = http::Request::get(app.path(path))
        .body(axum::body::Body::empty())
        .expect("request");
    tokio::spawn(async move { router.oneshot(request).await.expect("infallible").status() })
        .await
        .map_err(|error| {
            let panic = error.into_panic();
            panic
                .downcast_ref::<&str>()
                .map(|text| (*text).to_owned())
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "a panic".to_owned())
        })
}

#[tokio::test]
async fn times_beyond_the_calendar_do_not_break_pages() {
    let app = App::new().await;
    // What `POST /api/v1/jobs {"at": 1e13}` stored before the API refused such times (an `at`
    // sent in microseconds does the same; the client refuses them too since round 3, so the
    // job is written as an older version stored it); `RetryIn::After(huge)` scored retries that
    // way.
    let jid = jid_of("farfuture");
    add_to_set(
        &app,
        "schedule",
        json!({"class": "FarFuture", "args": [], "queue": "default", "jid": jid,
               "created_at": 1.7e9, "retry": true}),
        1e13,
    )
    .await;
    let mut retry = hench::JobPayload::new("FarRetry", vec![]);
    retry.jid = "farretry0000000000000000".to_owned();
    app.api
        .retries()
        .schedule(hench::secs_to_system_time(2e15), &retry)
        .await
        .unwrap();
    // Before the calendar too, and absurd timestamps inside the jobs.
    let (dead, score) = add_to_set(
        &app,
        "dead",
        json!({"class": "LongDead", "args": [], "queue": "default", "jid": jid_of("dead"),
               "created_at": -1e17, "failed_at": 1e18, "error_class": "E", "error_message": "m"}),
        -1e13,
    )
    .await;
    push_raw(
        &app,
        "odd",
        &json!({"class": "Odd", "args": [], "jid": jid_of("odd"), "enqueued_at": 9e18}),
    )
    .await;
    add_process(
        &app,
        "far:1:abc",
        json!({"hostname": "far", "pid": 1, "started_at": 1e17, "identity": "far:1:abc"}),
        0,
    )
    .await;
    let _: i64 = app
        .redis(
            hench::redis_rs::cmd("HSET")
                .arg("far:1:abc")
                .arg("beat")
                .arg(-9e12),
        )
        .await;
    add_work(
        &app,
        "far:1:abc",
        "1",
        "default",
        &json!({"class": "Running", "args": [], "jid": jid_of("running")}),
    )
    .await;
    let _: i64 = app
        .redis(
            hench::redis_rs::cmd("HSET")
                .arg("far:1:abc:work")
                .arg("2")
                .arg(
                    json!({"queue": "default", "run_at": 1e13,
                            "payload": json!({"class": "Late", "args": []}).to_string()})
                    .to_string(),
                ),
        )
        .await;

    let mut failures = Vec::new();
    for path in [
        "/".to_owned(),
        "/busy".to_owned(),
        "/scheduled".to_owned(),
        format!("/scheduled/10000000000000-{jid}"),
        "/scheduled?substr=FarFuture".to_owned(),
        "/retries".to_owned(),
        "/retries/2000000000000000-farretry0000000000000000".to_owned(),
        "/morgue".to_owned(),
        format!("/morgue/{}", key(&dead, score)),
        "/queues/odd".to_owned(),
    ] {
        match status_of(&app, &path).await {
            Ok(StatusCode::OK) => {}
            Ok(status) => failures.push(format!("{path}: {status}")),
            Err(panic) => failures.push(format!("{path}: the handler panicked: {panic}")),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    // The relative wording still says how far; the tooltip gives the raw Unix time.
    let res = app.get("/scheduled").await;
    let text = res.texts("tbody time").concat();
    assert!(
        text.starts_with("in 317,0") && text.ends_with(" years"),
        "{text}"
    );
    assert_eq!(
        res.attrs("tbody time", "title"),
        ["Unix time 10000000000000"]
    );
    assert!(res.attrs("tbody time", "datetime").is_empty());
}

/// A recognizable jid.
fn jid_of(label: &str) -> String {
    format!("{label:0>24}")
}
