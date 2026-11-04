//! `hench queues limit`, the limits column of `hench queues`, and the JSON API's
//! `/queues/{name}/limits` routes against a private Redis (`docs/design/ratelimit.md` §2.16).

use axum::Router;
use axum::body::Body;
use clap::Parser;
use hench::api::{Api, Queue};
use hench::limit::Limit;
use hench::test_util::TestRedis;
use hench_server::cli::{Cli, Command};
use hench_server::server::{ServerConfig, app};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

/// Runs `hench <args>` against `redis`, returning what it printed.
async fn hench(redis: &TestRedis, args: &[&str]) -> anyhow::Result<String> {
    let url = redis.url();
    let mut argv = vec!["hench", "--redis-url", url.as_str()];
    argv.extend_from_slice(args);
    let cli = Cli::try_parse_from(argv)?;
    let mut out = Vec::new();
    hench_server::run(cli, &mut out).await?;
    Ok(String::from_utf8(out)?)
}

#[tokio::test]
async fn the_command_sets_shows_and_clears_queue_limits() {
    let redis = TestRedis::start().await;
    let api = Api::new(redis.pool().await);
    let out = hench(
        &redis,
        &["queues", "limit", "exports", "--set", "window=100/1m; c=3"],
    )
    .await
    .unwrap();
    assert!(
        out.contains("Limited the queue `exports`: concurrency=3;window=100/1m."),
        "{out}"
    );
    assert_eq!(
        Queue::new(&api, "exports").limits().await.unwrap(),
        [
            Limit::Concurrency { limit: 3 },
            Limit::Window {
                limit: 100,
                period: std::time::Duration::from_secs(60)
            }
        ]
    );
    // `hench queues` lists a limited queue, even without jobs, with its limits.
    let list = hench(&redis, &["queues"]).await.unwrap();
    assert!(list.contains("LIMITS"), "{list}");
    assert!(list.contains("concurrency=3;window=100/1m"), "{list}");
    let json: Value =
        serde_json::from_str(&hench(&redis, &["queues", "--json"]).await.unwrap()).unwrap();
    assert_eq!(json[0]["name"], "exports");
    assert_eq!(json[0]["limits"], "concurrency=3;window=100/1m");
    // JSON output of the change.
    let json: Value = serde_json::from_str(
        &hench(
            &redis,
            &["queues", "limit", "exports", "--set", "c=1", "--json"],
        )
        .await
        .unwrap(),
    )
    .unwrap();
    assert_eq!(json["limits"], "concurrency=1");
    // Mistakes are refused, naming the reason, and change nothing.
    for specs in ["points=5/1s", "c=1;c=2", "c=1 per key", "speed=3", ""] {
        let error = hench(&redis, &["queues", "limit", "exports", "--set", specs])
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("limit"), "{specs}: {error}");
    }
    assert_eq!(
        Queue::new(&api, "exports").limits().await.unwrap(),
        [Limit::Concurrency { limit: 1 }]
    );
    // --set or --clear is required, not both.
    assert!(
        hench(&redis, &["queues", "limit", "exports"])
            .await
            .is_err()
    );
    assert!(
        hench(
            &redis,
            &["queues", "limit", "exports", "--set", "c=1", "--clear"]
        )
        .await
        .is_err()
    );
    let out = hench(&redis, &["queues", "limit", "exports", "--clear"])
        .await
        .unwrap();
    assert!(
        out.contains("Cleared the limits of the queue `exports`"),
        "{out}"
    );
    let out = hench(&redis, &["queues", "limit", "exports", "--clear"])
        .await
        .unwrap();
    assert!(out.contains("had no limits"), "{out}");
    // Without limits, the table has no limits column.
    let list = hench(&redis, &["queues"]).await.unwrap();
    assert!(!list.contains("LIMITS"), "{list}");
}

/// The router of `hench web --api <args>` on `redis`.
async fn server(redis: &TestRedis, args: &[&str]) -> Router {
    let api = Api::new(redis.pool().await);
    let mut argv = vec!["hench", "web", "--api"];
    argv.extend_from_slice(args);
    let Command::Web(web) = Cli::try_parse_from(argv).unwrap().command else {
        panic!("not `hench web`");
    };
    app(&ServerConfig::from_args(&web).unwrap(), api)
}

async fn send(router: &Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| panic!("not JSON: {}", String::from_utf8_lossy(&bytes)));
    (status, body)
}

fn put(path: &str, body: &str) -> Request<Body> {
    Request::put(path)
        .header("content-type", "application/json")
        .body(Body::from(body.to_owned()))
        .unwrap()
}

fn delete(path: &str) -> Request<Body> {
    Request::delete(path).body(Body::empty()).unwrap()
}

#[tokio::test]
async fn the_json_api_sets_lists_and_clears_queue_limits() {
    let redis = TestRedis::start().await;
    let router = server(&redis, &[]).await;
    let (status, set) = send(
        &router,
        put(
            "/api/v1/queues/low%3Aprio/limits",
            r#"{"limits": "leaky=60/1m; concurrency=2"}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{set}");
    assert_eq!(set["queue"], "low:prio");
    assert_eq!(set["limits"], "concurrency=2;leaky=60/1m");
    let (status, queues) = send(
        &router,
        Request::get("/api/v1/queues").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(queues[0]["name"], "low:prio");
    assert_eq!(queues[0]["limits"], "concurrency=2;leaky=60/1m");
    assert_eq!(queues[0]["size"], 0);
    // Invalid limits and bodies are refused.
    for body in [
        r#"{"limits": "points=1/1s"}"#,
        r#"{"limits": "c=1;c=2"}"#,
        r#"{"limits": ""}"#,
    ] {
        let (status, error) = send(&router, put("/api/v1/queues/q/limits", body)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}: {error}");
        assert!(error["error"].is_string());
    }
    for body in [r#"{"limit": "c=1"}"#, r#"{"limits": 3}"#, "not json"] {
        let (status, error) = send(&router, put("/api/v1/queues/q/limits", body)).await;
        assert!(status.is_client_error(), "{body}: {status}");
        assert!(error["error"].is_string());
    }
    // A browser on another site cannot change them.
    let cross = Request::put("/api/v1/queues/q/limits")
        .header("content-type", "application/json")
        .header("sec-fetch-site", "cross-site")
        .body(Body::from(r#"{"limits": "c=1"}"#))
        .unwrap();
    assert_eq!(send(&router, cross).await.0, StatusCode::FORBIDDEN);
    let (status, cleared) = send(&router, delete("/api/v1/queues/low%3Aprio/limits")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(cleared["cleared"], true);
    let (_, cleared) = send(&router, delete("/api/v1/queues/low%3Aprio/limits")).await;
    assert_eq!(cleared["cleared"], false);
    // Wrong methods answer 405.
    let (status, _) = send(
        &router,
        Request::get("/api/v1/queues/q/limits")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
}

#[tokio::test]
async fn a_read_only_json_api_refuses_limit_changes() {
    let redis = TestRedis::start().await;
    let router = server(&redis, &["--read-only"]).await;
    let (status, _) = send(
        &router,
        put("/api/v1/queues/q/limits", r#"{"limits": "c=1"}"#),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _) = send(&router, delete("/api/v1/queues/q/limits")).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let api = Api::new(redis.pool().await);
    assert!(Queue::new(&api, "q").limits().await.unwrap().is_empty());
}
