//! Mounting under a prefix: every link, form action, asset URL, redirect and cookie path
//! follows the path the dashboard is nested at.

mod common;

use axum::Router;
use axum::body::Body;
use common::*;
use http::{Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;

/// Every URL a page links to, posts to or loads.
fn urls(res: &Res) -> Vec<String> {
    let mut urls = Vec::new();
    for (selector, attribute) in [
        ("a[href]", "href"),
        ("form[action]", "action"),
        ("[formaction]", "formaction"),
        ("link[href]", "href"),
        ("script[src]", "src"),
        ("[data-stats-url]", "data-stats-url"),
    ] {
        urls.extend(res.attrs(selector, attribute));
    }
    urls
}

async fn seeded(prefix: &str) -> (App, String) {
    let app = App::build(|web| web.app_url("https://acme.example/"), prefix).await;
    let (job, score) = add_retry(&app, &jid()).await;
    add_scheduled(&app, &jid()).await;
    add_dead(&app, &jid()).await;
    push_raw(
        &app,
        "default",
        &json!({"class": "X", "args": [1], "tags": ["t"]}),
    )
    .await;
    add_process(&app, "host:1:x", json!({"hostname": "host", "pid": 1}), 0).await;
    (app, key(&job, score))
}

async fn assert_links_stay_under(prefix: &str) {
    let (app, key) = seeded(prefix).await;
    let entry = format!("/retries/{key}");
    let paths = [
        "/",
        "/busy",
        "/queues",
        "/queues/default",
        "/retries",
        "/scheduled",
        "/morgue",
        entry.as_str(),
        "/nowhere",
    ];
    for path in paths {
        let res = app.get(path).await;
        assert!(
            res.status == StatusCode::OK || path == "/nowhere",
            "{path}: {}",
            res.status
        );
        let urls = urls(&res);
        assert!(!urls.is_empty(), "{path}");
        for url in urls {
            if url.starts_with("https://") || url.starts_with('#') {
                continue;
            }
            let expected = if prefix.is_empty() { "/" } else { prefix };
            let path_part = url.split(['?', '#']).next().unwrap_or_default();
            assert!(
                path_part == expected || path_part.starts_with(&format!("{prefix}/")),
                "{prefix}{path}: {url} is outside the mount"
            );
            assert!(!url.starts_with("//"), "{url}");
        }
        assert_eq!(
            res.attrs("body", "data-prefix"),
            [prefix.to_owned()],
            "{path}"
        );
    }
    // The brand and the Dashboard tab lead home.
    let page = app.get("/queues").await;
    let home = if prefix.is_empty() { "/" } else { prefix };
    assert_eq!(page.attrs("a.brand", "href"), [home]);
    assert_eq!(page.attrs(".nav a", "href")[0], home);
    // Redirects stay under the mount.
    let res = app.post(&entry, &[("delete", "1")]).await;
    res.assert_status(StatusCode::SEE_OTHER);
    assert_eq!(res.location(), format!("{prefix}/retries"));
    // The token cookie is scoped to the mount.
    let cookie = page.cookie("hench_csrf").unwrap();
    assert!(cookie.contains(&format!("; Path={home};")), "{cookie}");
    // The JSON endpoints and assets are there too.
    app.get("/stats").await.assert_status(StatusCode::OK);
    let css = page.attrs("link[rel=stylesheet]", "href")[0].clone();
    let res = app
        .send(Request::get(&css).body(Body::empty()).unwrap())
        .await;
    res.assert_status(StatusCode::OK);
}

#[tokio::test]
async fn links_work_at_the_root() {
    assert_links_stay_under("").await;
}

#[tokio::test]
async fn links_work_under_a_prefix() {
    assert_links_stay_under("/hench").await;
}

#[tokio::test]
async fn links_work_several_levels_deep() {
    assert_links_stay_under("/admin/background/jobs").await;
}

#[tokio::test]
async fn links_follow_path_parameters_of_the_mount() {
    let redis = hench::test_util::TestRedis::start().await;
    let pool = redis.pool().await;
    let dashboard = hench_web::router(hench_web::HenchWeb::new(&pool));
    let app = Router::new().nest("/tenants/{tenant}/jobs", dashboard);
    let mut job = hench::JobPayload::new("X", vec![json!(1)]);
    job.queue = "default".to_owned();
    hench::Client::new(pool.clone()).push(job).await.unwrap();
    // Pages with path parameters of their own work under a parameterized mount.
    let response = app
        .clone()
        .oneshot(
            Request::get("/tenants/acme/jobs/queues/default")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    for tenant in ["acme", "globex"] {
        let response = app
            .clone()
            .oneshot(
                Request::get(format!("/tenants/{tenant}/jobs/queues"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = http_body_util::BodyExt::collect(response.into_body())
            .await
            .unwrap()
            .to_bytes();
        let html = String::from_utf8_lossy(&bytes);
        assert!(
            html.contains(&format!("href=\"/tenants/{tenant}/jobs/busy\"")),
            "{tenant}"
        );
        assert!(!html.contains("{tenant}"));
    }
}

#[tokio::test]
async fn request_values_in_the_mount_cannot_point_elsewhere() {
    // Under a mount whose first segment is a parameter (possible with `nest_service`),
    // the prefix carries a value of the request; a backslash there would make `/\host`
    // links, which browsers follow to `host`.
    let redis = hench::test_util::TestRedis::start().await;
    let pool = redis.pool().await;
    // Reached as dash.example (the POST below names it).
    let dashboard = hench_web::router(
        hench_web::HenchWeb::new(&pool)
            .allowed_hosts(hench_web::AllowedHosts::new(["dash.example"])),
    );
    let app = Router::new().nest_service("/{tenant}/jobs", dashboard);
    let response = app
        .clone()
        .oneshot(
            Request::get("/\\evil.example/jobs/queues")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = http_body_util::BodyExt::collect(response.into_body())
        .await
        .unwrap()
        .to_bytes();
    let html = String::from_utf8_lossy(&bytes);
    assert!(
        html.contains("href=\"/%5Cevil.example/jobs/busy\""),
        "{html}"
    );
    assert!(!html.contains("/\\evil"));
    let mut post = post_request(
        "/\\evil.example/jobs/retries",
        &[("retry", "1")],
        TOKEN,
        TOKEN,
    );
    post.headers_mut()
        .insert("host", "dash.example".parse().unwrap());
    let response = app.oneshot(post).await.unwrap();
    let location = response.headers()["location"].to_str().unwrap();
    assert_eq!(location, "/%5Cevil.example/jobs/retries");
}

#[tokio::test]
async fn the_mount_root_is_the_dashboard() {
    let (app, _) = seeded("/hench").await;
    // `/hench` itself (no trailing slash) is the dashboard.
    let res = app.get("/").await;
    res.assert_status(StatusCode::OK);
    assert_eq!(res.title(), "Hench");
    let res = app
        .send(
            Request::get("/hench/dashboard/stats")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(res.location(), "/hench/stats");
}

#[tokio::test]
async fn nest_service_also_serves_the_mount_with_a_trailing_slash() {
    // web-functional-2: `Router::nest` does not route `/hench/` (axum's rule), so a proxy whose
    // `location /hench/` redirects `/hench` there ended on an empty 404. `nest_service`, which
    // the docs recommend (and `hench web` uses), routes both to the home page.
    let redis = hench::test_util::TestRedis::start().await;
    let pool = redis.pool().await;
    let dashboard = hench_web::router(hench_web::HenchWeb::new(&pool));
    let app = Router::new().nest_service("/hench", dashboard);
    for path in ["/hench", "/hench/"] {
        let response = app
            .clone()
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{path}");
        let bytes = http_body_util::BodyExt::collect(response.into_body())
            .await
            .unwrap()
            .to_bytes();
        let html = String::from_utf8_lossy(&bytes);
        assert!(html.contains("href=\"/hench/busy\""), "{path}");
        assert!(
            html.contains("<a class=\"brand\" href=\"/hench\">"),
            "{path}"
        );
    }
    let response = app
        .clone()
        .oneshot(Request::get("/hench/queues").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}
