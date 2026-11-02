//! `hench limits`, `hench stats limits` and the JSON API's `/limits` routes against a private
//! Redis (`docs/design/ratelimit.md` §2.16, §3.7.3).

use std::time::Duration;

use axum::Router;
use axum::body::Body;
use clap::Parser;
use hench::RedisPool;
use hench::api::Api;
use hench::limit::{LimitKind, Limiter, Permit};
use hench::test_util::TestRedis;
use hench_server::cli::{Cli, Command};
use hench_server::server::{ServerConfig, app};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

const MINUTE: Duration = Duration::from_secs(60);

/// A full window `api:acme` with one refusal, of a caller that waits in its line (a caller
/// that does not wait for its slot takes no place in the line), and a held concurrency limit
/// `erp`.
async fn seed(pool: &RedisPool) -> Permit {
    let api = Limiter::window("api", 2, MINUTE)
        .max_wait(Duration::ZERO)
        .key("acme");
    for _ in 0..2 {
        let _permit = api.acquire(pool).await.unwrap();
    }
    let patient = api.clone().max_wait(Duration::from_secs(3_600));
    let target = pool.clone();
    tokio::spawn(async move {
        let _ = patient.acquire(&target).await;
    });
    let stats = hench::keys::limit_stats('w', "api:acme");
    for _ in 0..500 {
        let mut conn = pool.conn().await.unwrap();
        let line: Option<String> = hench::redis_rs::cmd("HGET")
            .arg(&stats)
            .arg("line")
            .query_async(&mut conn)
            .await
            .unwrap();
        if line.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    tokio::time::sleep(Duration::from_millis(5)).await;
    Limiter::concurrent("erp", 2)
        .max_wait(Duration::ZERO)
        .acquire(pool)
        .await
        .unwrap()
}

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
async fn the_list_prints_every_limit_in_use() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let empty = hench(&redis, &["limits"]).await.unwrap();
    assert!(
        empty.starts_with("No limiter or throttle has been used in the last day."),
        "{empty}"
    );
    let _held = seed(&pool).await;
    let out = hench(&redis, &["limits"]).await.unwrap();
    let lines: Vec<&str> = out.lines().collect();
    assert!(
        lines[0].starts_with("LIMIT") && lines[0].ends_with("LAST-USED"),
        "{out}"
    );
    assert!(lines[1].starts_with("c:erp"), "{out}");
    assert!(
        lines[1].contains("concurrency=2") && lines[1].contains("1/2"),
        "{out}"
    );
    assert!(lines[2].starts_with("w:api:acme"), "{out}");
    assert!(
        lines[2].contains("window=2/1m") && lines[2].contains("2/2"),
        "{out}"
    );
    assert!(out.contains("Last hour: 3 granted, 1 refused"), "{out}");
    let by_name = out
        .lines()
        .skip_while(|line| !line.starts_with("NAME"))
        .collect::<Vec<_>>();
    assert!(by_name[1].starts_with("api"), "{out}");

    let named = hench(&redis, &["limits", "--sort", "name"]).await.unwrap();
    assert!(
        named.lines().nth(1).unwrap().starts_with("w:api:acme"),
        "{named}"
    );
    let filtered = hench(
        &redis,
        &["limits", "list", "--filter", "acme", "--limit", "5"],
    )
    .await
    .unwrap();
    assert!(
        filtered.contains("w:api:acme") && !filtered.contains("c:erp"),
        "{filtered}"
    );
    let first = hench(&redis, &["limits", "--limit", "1"]).await.unwrap();
    assert!(first.contains("(1 of 2; --limit to see more)"), "{first}");
    let none = hench(&redis, &["limits", "--filter", "zzz"]).await.unwrap();
    assert!(none.starts_with("No limit matches the filter."), "{none}");
    assert!(hench(&redis, &["limits", "--limit", "0"]).await.is_err());
    assert!(hench(&redis, &["limits", "--sort", "size"]).await.is_err());

    let json: Value =
        serde_json::from_str(&hench(&redis, &["limits", "--json"]).await.unwrap()).unwrap();
    assert_eq!(json["total"], 2);
    assert_eq!(json["limits"][0]["identity"], "c:erp");
    assert_eq!(json["limits"][1]["identity"], "w:api:acme");
    assert_eq!(json["limits"][1]["kind"], "window");
    assert_eq!(json["limits"][1]["key"], "acme");
    assert_eq!(json["limits"][1]["spec"], "window=2/1m");
    assert_eq!(json["limits"][1]["capacity"], 2);
    assert_eq!(json["limits"][1]["refused"], 1);
    assert!(json["limits"][1]["line_ends_at"].is_string());
    assert_eq!(json["last_hour"]["granted"], 3);
    assert_eq!(json["last_hour"]["by_name"]["api"]["refused"], 1);
}

#[tokio::test]
async fn show_reset_and_clear_line() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let _held = seed(&pool).await;
    let api = Api::new(pool.clone());

    let erp = hench(&redis, &["limits", "show", "c:erp"]).await.unwrap();
    assert!(erp.contains("Identity") && erp.contains("c:erp"), "{erp}");
    assert!(erp.contains("Overages") && erp.contains("HOLDER"), "{erp}");
    let window = hench(&redis, &["limits", "show", "w:api:acme"])
        .await
        .unwrap();
    assert!(
        window.contains("Wait line") && window.contains("clears in"),
        "{window}"
    );
    assert!(!window.contains("HOLDER"), "{window}");
    let json: Value = serde_json::from_str(
        &hench(&redis, &["limits", "show", "c:erp", "--json"])
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(json["holders"].as_array().unwrap().len(), 1);
    assert_eq!(json["holders"][0]["ended"], false);
    let error = hench(&redis, &["limits", "show", "w:nope"])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("is not in use"), "{error}");
    let error = hench(&redis, &["limits", "show", "stripe"])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("is not a limit"), "{error}");

    let cleared = hench(&redis, &["limits", "clear-line", "w:api:acme"])
        .await
        .unwrap();
    assert_eq!(cleared, "Cleared the wait line of w:api:acme.\n");
    let again = hench(&redis, &["limits", "clear-line", "w:api:acme"])
        .await
        .unwrap();
    assert_eq!(again, "w:api:acme had no wait line.\n");

    // Reset needs --yes.
    let error = hench(&redis, &["limits", "reset", "c:erp"])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("--yes"), "{error}");
    assert!(
        api.limits()
            .get(LimitKind::Concurrency, "erp")
            .await
            .unwrap()
            .is_some()
    );
    let reset = hench(&redis, &["limits", "reset", "c:erp", "--yes"])
        .await
        .unwrap();
    assert_eq!(reset, "Reset c:erp.\n");
    assert!(
        api.limits()
            .get(LimitKind::Concurrency, "erp")
            .await
            .unwrap()
            .is_none()
    );
    let reset = hench(&redis, &["limits", "reset", "c:erp", "--yes"])
        .await
        .unwrap();
    assert_eq!(reset, "c:erp was not in use; nothing to reset.\n");
}

#[tokio::test]
async fn keys_are_printed_with_control_characters_spelled_out() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let _permit = Limiter::window("x", 5, MINUTE)
        .key("evil\u{1b}]0;title\u{7}\nforged row")
        .acquire(&pool)
        .await
        .unwrap();
    let out = hench(&redis, &["limits"]).await.unwrap();
    assert!(!out.contains('\u{1b}') && !out.contains('\u{7}'), "{out:?}");
    assert!(
        out.contains("w:x:evil\\u{1b}]0;title\\u{7}\\nforged row"),
        "{out}"
    );
}

#[tokio::test]
async fn stats_limits_prints_the_last_hour() {
    let redis = TestRedis::start().await;
    let quiet = hench(&redis, &["stats"]).await.unwrap();
    assert!(!quiet.contains("Limits:"), "{quiet}");
    let pool = redis.pool().await;
    let _held = seed(&pool).await;
    let out = hench(&redis, &["stats", "limits"]).await.unwrap();
    assert!(
        out.starts_with("Limits: 2 in use\nLast hour: 3 granted, 1 refused\n"),
        "{out}"
    );
    assert!(out.contains("NAME"), "{out}");
    let json: Value =
        serde_json::from_str(&hench(&redis, &["stats", "limits", "--json"]).await.unwrap())
            .unwrap();
    assert_eq!(json["in_use"], 2);
    assert_eq!(json["refused"], 1);
    let all = hench(&redis, &["stats"]).await.unwrap();
    assert!(all.contains("Limits: 2 in use"), "{all}");
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

fn get(path: &str) -> Request<Body> {
    Request::get(path).body(Body::empty()).unwrap()
}

fn delete(path: &str) -> Request<Body> {
    Request::delete(path).body(Body::empty()).unwrap()
}

#[tokio::test]
async fn the_json_api_lists_shows_and_resets_limits() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let _held = seed(&pool).await;
    let router = server(&redis, &[]).await;

    let (status, list) = send(&router, get("/api/v1/limits")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list["total"], 2);
    assert_eq!(list["limits"][0]["identity"], "c:erp");
    let (_, filtered) = send(
        &router,
        get("/api/v1/limits?filter=acme&limit=1&sort=refused"),
    )
    .await;
    assert_eq!(filtered["limits"].as_array().unwrap().len(), 1);
    assert_eq!(filtered["limits"][0]["identity"], "w:api:acme");
    let (status, error) = send(&router, get("/api/v1/limits?sort=size")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(error["error"].as_str().unwrap().contains("sort"));
    let (status, _) = send(&router, get("/api/v1/limits?limit=many")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, erp) = send(&router, get("/api/v1/limits/c/erp")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(erp["used"], 1.0);
    assert_eq!(erp["holders"].as_array().unwrap().len(), 1);
    let (status, window) = send(&router, get("/api/v1/limits/w/api%3Aacme")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(window["key"], "acme");
    assert!(window.get("holders").is_none());
    for path in [
        "/api/v1/limits/w/nope",
        "/api/v1/limits/x/erp",
        "/api/v1/limits/ww/erp",
    ] {
        let (status, error) = send(&router, get(path)).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{path}");
        assert!(error["error"].is_string());
    }

    // A browser on another site cannot reset.
    let cross = Request::delete("/api/v1/limits/c/erp")
        .header("sec-fetch-site", "cross-site")
        .body(Body::empty())
        .unwrap();
    assert_eq!(send(&router, cross).await.0, StatusCode::FORBIDDEN);
    let (status, reset) = send(&router, delete("/api/v1/limits/c/erp")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(reset["reset"], true);
    let (_, again) = send(&router, delete("/api/v1/limits/c/erp")).await;
    assert_eq!(again["reset"], false);
    assert_eq!(
        send(&router, get("/api/v1/limits/c/erp")).await.0,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn a_read_only_api_refuses_resets() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let _held = seed(&pool).await;
    let router = server(&redis, &["--read-only"]).await;
    let (status, error) = send(&router, delete("/api/v1/limits/c/erp")).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(error["error"].as_str().unwrap().contains("read-only"));
    let api = Api::new(pool);
    assert!(
        api.limits()
            .get(LimitKind::Concurrency, "erp")
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(send(&router, get("/api/v1/limits")).await.0, StatusCode::OK);
}
