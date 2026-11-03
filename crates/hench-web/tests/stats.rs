//! The JSON endpoints: `/stats` and `/stats/queues` (Sidekiq's web_test "stats" and
//! "stats/queues" scenarios), and the embedded assets.

mod common;

use common::*;
use hench::redis_rs;
use http::StatusCode;
use serde_json::json;

async fn seeded() -> App {
    let app = App::new().await;
    let _: () = app
        .redis(
            redis_rs::cmd("MSET")
                .arg("stat:processed")
                .arg(5)
                .arg("stat:failed")
                .arg(2),
        )
        .await;
    let _: i64 = app
        .redis(
            redis_rs::cmd("SADD")
                .arg("queues")
                .arg("default")
                .arg("queue2"),
        )
        .await;
    for _ in 0..2 {
        add_retry(&app, &jid()).await;
    }
    for _ in 0..3 {
        add_scheduled(&app, &jid()).await;
    }
    add_process(
        &app,
        "host:1:x",
        json!({"hostname": "foo", "started_at": now_secs()}),
        4,
    )
    .await;
    app
}

#[tokio::test]
async fn stats_have_the_dashboard_numbers() {
    let app = seeded().await;
    let res = app.get("/stats").await;
    res.assert_status(StatusCode::OK);
    assert_eq!(res.header("content-type"), Some("application/json"));
    let stats = res.json();
    let hench = &stats["hench"];
    assert_eq!(hench["processed"], 5);
    assert_eq!(hench["failed"], 2);
    assert_eq!(hench["busy"], 4);
    assert_eq!(hench["processes"], 1);
    assert_eq!(hench["retries"], 2);
    assert_eq!(hench["scheduled"], 3);
    assert_eq!(hench["dead"], 0);
    assert_eq!(hench["enqueued"], 0);
    assert_eq!(hench["default_latency"], 0.0);
    let redis = stats["redis"].as_object().unwrap();
    for key in [
        "redis_version",
        "uptime_in_days",
        "connected_clients",
        "used_memory_human",
        "used_memory_peak_human",
        "store_name",
        "store_version",
    ] {
        assert!(redis.contains_key(key), "{key}");
    }
    assert_eq!(redis["store_name"], app.store_name().await);
    let clock = stats["server_utc_time"].as_str().unwrap();
    assert!(clock.ends_with(" UTC") && clock.len() == 12, "{clock}");
    assert!(stats["server_time"].as_str().unwrap().ends_with('Z'));
}

#[tokio::test]
async fn stats_queues_report_each_queue_size() {
    let app = seeded().await;
    push_raw(&app, "queue2", &json!({"class": "X", "args": []})).await;
    let res = app.get("/stats/queues").await;
    res.assert_status(StatusCode::OK);
    assert_eq!(res.json(), json!({"default": 0, "queue2": 1}));
}

#[tokio::test]
async fn the_old_stats_address_redirects() {
    let app = App::new().await;
    let res = app.get("/dashboard/stats").await;
    assert_eq!(res.status, StatusCode::SEE_OTHER);
    assert_eq!(res.location(), "/stats");
}

#[tokio::test]
async fn redis_failures_are_reported() {
    let mut app = App::new().await;
    app.redis.stop();
    let res = app.get("/stats").await;
    res.assert_status(StatusCode::SERVICE_UNAVAILABLE);
    assert!(res.json()["error"].is_string());
    app.get("/stats/queues")
        .await
        .assert_status(StatusCode::SERVICE_UNAVAILABLE);
    // Pages show an error page; the heartbeat says the server is unavailable.
    let page = app.get("/retries").await;
    page.assert_status(StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(page.texts("h1"), ["Redis is not answering"]);
    let head = app
        .send(
            http::Request::head("/")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(head.status, StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn assets_are_fingerprinted_and_cached_forever() {
    let app = App::new().await;
    let page = app.get("/").await;
    let mut assets = page.attrs("link[rel=stylesheet]", "href");
    assets.extend(page.attrs("link[rel=icon]", "href"));
    assets.extend(page.attrs("script[src]", "src"));
    assert_eq!(assets.len(), 5, "{assets:?}");
    for (asset, kind) in assets.iter().zip([
        "text/css; charset=utf-8",
        "image/svg+xml",
        "text/javascript; charset=utf-8",
        "text/javascript; charset=utf-8",
        "text/javascript; charset=utf-8",
    ]) {
        let name = asset.trim_start_matches("/assets/");
        // name.<16 hex digits>.extension
        let parts: Vec<&str> = name.split('.').collect();
        assert_eq!(parts.len(), 3, "{name}");
        assert_eq!(parts[1].len(), 16, "{name}");
        let res = app.get(asset).await;
        res.assert_status(StatusCode::OK);
        assert_eq!(res.header("content-type"), Some(kind), "{asset}");
        assert_eq!(
            res.header("cache-control"),
            Some("public, max-age=31536000, immutable")
        );
        assert!(!res.body.is_empty());
    }
    // Unknown or outdated names are not served.
    for path in [
        "/assets/hench.css",
        "/assets/hench.0000000000000000.css",
        "/assets/../Cargo.toml",
    ] {
        assert_eq!(app.get(path).await.status, StatusCode::NOT_FOUND, "{path}");
    }
}
