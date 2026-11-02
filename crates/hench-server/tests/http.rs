//! `hench web`'s HTTP surface, driven through its router: Basic authentication, the JSON
//! API, the health check and serving under a prefix.

use std::time::Duration;

use axum::Router;
use axum::body::Body;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use clap::Parser;
use hench::api::Api;
use hench::redis_rs;
use hench::test_util::TestRedis;
use hench_server::cli::{Cli, Command};
use hench_server::server::{ServerConfig, app};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

/// The router of `hench web <args>` on a private Redis.
async fn server(args: &[&str]) -> (TestRedis, Api, Router) {
    let redis = TestRedis::start().await;
    let api = Api::new(redis.pool().await);
    let mut argv = vec!["hench", "web"];
    argv.extend_from_slice(args);
    let cli = Cli::try_parse_from(argv).expect("valid arguments");
    let Command::Web(web) = cli.command else {
        panic!("not `hench web`");
    };
    let config = ServerConfig::from_args(&web).expect("valid configuration");
    let router = app(&config, api.clone());
    (redis, api, router)
}

struct Res {
    status: StatusCode,
    headers: http::HeaderMap,
    body: String,
}

impl Res {
    fn json(&self) -> Value {
        serde_json::from_str(&self.body).unwrap_or_else(|_| panic!("not JSON: {}", self.body))
    }
}

async fn send(router: &Router, request: Request<Body>) -> Res {
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    Res {
        status,
        headers,
        body: String::from_utf8_lossy(&bytes).into_owned(),
    }
}

fn get(path: &str) -> Request<Body> {
    Request::get(path).body(Body::empty()).unwrap()
}

fn basic(user: &str, password: &str) -> String {
    format!("Basic {}", STANDARD.encode(format!("{user}:{password}")))
}

fn post_json(path: &str, body: &Value) -> Request<Body> {
    Request::post(path)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

#[tokio::test]
async fn basic_authentication_guards_the_dashboard_and_the_api() {
    let (_redis, _api, router) = server(&["--username", "a", "--password", "b", "--api"]).await;
    for path in ["/", "/queues", "/api/v1/stats", "/nowhere"] {
        let res = send(&router, get(path)).await;
        assert_eq!(res.status, StatusCode::UNAUTHORIZED, "{path}");
        let challenge = res
            .headers
            .get("www-authenticate")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(
            challenge.starts_with("Basic realm=\"hench\""),
            "{challenge}"
        );
    }
    let wrong = Request::get("/")
        .header("authorization", basic("a", "nope"))
        .body(Body::empty())
        .unwrap();
    assert_eq!(send(&router, wrong).await.status, StatusCode::UNAUTHORIZED);
    let right = Request::get("/")
        .header("authorization", basic("a", "b"))
        .body(Body::empty())
        .unwrap();
    let res = send(&router, right).await;
    assert_eq!(res.status, StatusCode::OK);
    assert!(res.body.contains("<title>Hench</title>"));
    // Sidekiq's "?days=1000000" scenario: authenticated, but refused.
    let bad_days = Request::get("/?days=1000000")
        .header("authorization", basic("a", "b"))
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        send(&router, bad_days).await.status,
        StatusCode::BAD_REQUEST
    );
    // The health check needs no credentials.
    let health = send(&router, get("/healthz")).await;
    assert_eq!(health.status, StatusCode::OK);
    assert_eq!(health.body, "ok\n");
}

#[tokio::test]
async fn the_api_reports_stats_queues_and_processes() {
    let (_redis, api, router) = server(&["--api"]).await;
    let mut conn = api.redis().conn().await.unwrap();
    let _: () = redis_rs::cmd("MSET")
        .arg("stat:processed")
        .arg(5)
        .arg("stat:failed")
        .arg(2)
        .query_async(&mut conn)
        .await
        .unwrap();
    let _: i64 = redis_rs::cmd("SADD")
        .arg("processes")
        .arg("host:1:abc")
        .query_async(&mut conn)
        .await
        .unwrap();
    let info = json!({"hostname": "host", "pid": 1, "tag": "app", "queues": ["a", "b"],
                      "started_at": 1_700_000_000.0, "version": "9.9.9",
                      "features": {"unique": 1, "periodic": 1, "batches": 1, "limits": 1}});
    let _: i64 = redis_rs::cmd("HSET")
        .arg("host:1:abc")
        .arg("info")
        .arg(info.to_string())
        .arg("busy")
        .arg(2)
        .arg("concurrency")
        .arg(5)
        .query_async(&mut conn)
        .await
        .unwrap();
    api.queue("b-queue").pause().await.unwrap();
    for queue in ["b-queue", "a-queue"] {
        let mut job = hench::JobPayload::new("X", vec![]);
        job.queue = queue.to_owned();
        api.client().push(job).await.unwrap();
    }

    let stats = send(&router, get("/api/v1/stats")).await;
    assert_eq!(stats.status, StatusCode::OK);
    assert_eq!(stats.headers["cache-control"], "no-store");
    assert_eq!(stats.headers["x-content-type-options"], "nosniff");
    assert_eq!(stats.headers["content-type"], "application/json");
    let stats = stats.json();
    assert_eq!(stats["processed"], 5);
    assert_eq!(stats["failed"], 2);
    assert_eq!(stats["busy"], 2);
    assert_eq!(stats["processes"], 1);
    assert_eq!(stats["enqueued"], 2);

    let queues = send(&router, get("/api/v1/queues")).await.json();
    let names: Vec<&str> = queues
        .as_array()
        .unwrap()
        .iter()
        .map(|queue| queue["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["a-queue", "b-queue"]);
    assert_eq!(queues[1]["paused"], true);
    assert_eq!(queues[1]["size"], 1);

    let processes = send(&router, get("/api/v1/processes")).await.json();
    assert_eq!(processes.as_array().unwrap().len(), 1);
    let process = &processes[0];
    assert_eq!(process["identity"], "host:1:abc");
    assert_eq!(process["tag"], "app");
    assert_eq!(process["busy"], 2);
    assert_eq!(process["concurrency"], 5);
    // The queue names (fresh-user-4: one display string, `["a, b"]`, before); this heartbeat
    // reports no capsule.
    assert_eq!(process["queues"], json!(["a", "b"]));
    assert_eq!(process["capsules"], json!({}));
    assert_eq!(process["version"], "9.9.9");
    // The pro features it understands (X17 of docs/design/pro-features.md).
    assert_eq!(
        process["features"],
        json!({"batches": 1, "limits": 1, "periodic": 1, "unique": 1})
    );
    assert!(
        process["started_at"]
            .as_str()
            .unwrap()
            .starts_with("2023-11-14T22:13:20")
    );
}

/// Registers a live process whose heartbeat's `info` is `info` (raw JSON text: `json!` would
/// sort the keys, and the order of capsules and queues matters).
async fn add_process(api: &Api, identity: &str, info: &str) {
    let mut conn = api.redis().conn().await.unwrap();
    let _: i64 = redis_rs::cmd("SADD")
        .arg("processes")
        .arg(identity)
        .query_async(&mut conn)
        .await
        .unwrap();
    let _: i64 = redis_rs::cmd("HSET")
        .arg(identity)
        .arg("info")
        .arg(info)
        .arg("busy")
        .arg(0)
        .query_async(&mut conn)
        .await
        .unwrap();
}

/// The raw JSON text of the process `identity` in `body` (a JSON array of processes), from
/// its field `field` on: `serde_json::Value` sorts the keys of objects, the text keeps them.
fn process_text<'a>(body: &'a str, identity: &str, field: &str) -> &'a str {
    let start = body
        .find(&format!("\"identity\":\"{identity}\""))
        .unwrap_or_else(|| panic!("no process {identity} in {body}"));
    let rest = &body[start..];
    let rest = &rest[rest
        .find(&format!("\"{field}\":"))
        .unwrap_or_else(|| panic!("no {field} in {rest}"))..];
    match rest.find("\"identity\":") {
        Some(end) => &rest[..end],
        None => rest,
    }
}

#[tokio::test]
async fn the_api_lists_each_process_queues_by_name() {
    // fresh-user-4: `queues` held one comma-joined display string per capsule
    // (`["critical, default, sync, erp, imports"]`), so a script could not tell whether a
    // process serves a queue (`jq 'any(.[]; .queues | index("default"))'` said no) and the
    // capsules' weights were lost.
    let (_redis, api, router) = server(&["--api"]).await;
    // As a worker's heartbeat stores them: one capsule, five queues in priority order.
    add_process(
        &api,
        "vm:1:abc",
        r#"{"hostname": "vm", "pid": 1, "tag": "app", "concurrency": 5,
            "capsules": {"default": {"concurrency": 5, "mode": "strict",
                "weights": {"critical": 0, "default": 0, "erp": 0, "imports": 0, "sync": 0}}},
            "queues": ["critical", "default", "sync", "erp", "imports"], "version": "0.1.0"}"#,
    )
    .await;
    // Three capsules, one weighted, in the order a worker built with serde_json's
    // `preserve_order` lists them (the configured one).
    add_process(
        &api,
        "vm:2:def",
        r#"{"hostname": "vm", "pid": 2, "concurrency": 13, "capsules": {
                "default": {"concurrency": 10, "mode": "strict",
                    "weights": {"critical": 0, "default": 0}},
                "bulk": {"concurrency": 2, "mode": "weighted", "weights": {"low": 3, "imports": 1}},
                "serial": {"concurrency": 1, "mode": "strict", "weights": {"sync": 0}}},
            "queues": ["critical", "default", "low", "imports", "sync"], "version": "0.1.0"}"#,
    )
    .await;
    // A heartbeat without a queue list: the capsules' queues, in their order.
    add_process(
        &api,
        "vm:3:ghi",
        r#"{"hostname": "vm", "pid": 3, "capsules": {"default": {"concurrency": 1,
            "mode": "random", "weights": {"b": 1, "a": 1}}}}"#,
    )
    .await;
    // Neither (a process of another program): no queue, no capsule.
    add_process(&api, "vm:4:jkl", r#"{"hostname": "vm", "pid": 4}"#).await;
    // Capsules and queues listed twice (heartbeats come from Redis and can hold anything):
    // each name once, with its last settings, so that no JSON object repeats a key.
    add_process(
        &api,
        "vm:5:mno",
        r#"{"hostname": "vm", "pid": 5, "queues": ["q", "r"], "capsules": [
            ["default", {"concurrency": 1, "mode": "strict", "weights": [["q", 0], ["q", 2]]}],
            ["default", {"concurrency": 2, "mode": "weighted",
                "weights": [["q", 1], ["r", 4], ["q", 3]]}]]}"#,
    )
    .await;

    let res = send(&router, get("/api/v1/processes")).await;
    assert_eq!(res.status, StatusCode::OK);
    let processes = res.json();
    let processes = processes.as_array().unwrap();
    assert_eq!(processes.len(), 5, "{}", res.body);
    let serves = |process: &Value, queue: &str| {
        process["queues"]
            .as_array()
            .unwrap()
            .iter()
            .any(|name| name == queue)
    };

    let single = &processes[0];
    assert_eq!(single["identity"], "vm:1:abc");
    assert_eq!(
        single["queues"],
        json!(["critical", "default", "sync", "erp", "imports"])
    );
    assert!(serves(single, "default"));
    assert!(!serves(single, "low"));
    assert_eq!(
        single["capsules"],
        json!({"default": {"concurrency": 5, "mode": "strict",
            "weights": {"critical": 0, "default": 0, "erp": 0, "imports": 0, "sync": 0}}})
    );

    let several = &processes[1];
    assert_eq!(
        several["queues"],
        json!(["critical", "default", "low", "imports", "sync"])
    );
    assert!(serves(several, "imports") && serves(several, "sync"));
    assert_eq!(
        several["capsules"],
        json!({
            "bulk": {"concurrency": 2, "mode": "weighted", "weights": {"imports": 1, "low": 3}},
            "default": {"concurrency": 10, "mode": "strict",
                "weights": {"critical": 0, "default": 0}},
            "serial": {"concurrency": 1, "mode": "strict", "weights": {"sync": 0}},
        })
    );
    // Capsules and weights in the order the process reports them.
    let text = process_text(&res.body, "vm:2:def", "capsules");
    let at = |name: &str| text.find(&format!("\"{name}\":")).unwrap();
    assert!(
        at("default") < at("bulk") && at("bulk") < at("serial"),
        "{text}"
    );
    assert!(at("low") < at("imports"), "{text}");

    assert_eq!(processes[2]["queues"], json!(["b", "a"]));
    assert_eq!(
        processes[2]["capsules"],
        json!({"default": {"concurrency": 1, "mode": "random", "weights": {"a": 1, "b": 1}}})
    );
    assert_eq!(processes[3]["queues"], json!([]));
    assert_eq!(processes[3]["capsules"], json!({}));

    let repeated = &processes[4];
    assert_eq!(repeated["queues"], json!(["q", "r"]));
    assert_eq!(
        repeated["capsules"],
        json!({"default": {"concurrency": 2, "mode": "weighted", "weights": {"q": 3, "r": 4}}})
    );
    let text = process_text(&res.body, "vm:5:mno", "capsules");
    assert_eq!(text.matches("\"default\":").count(), 1, "{text}");
    assert_eq!(text.matches("\"q\":").count(), 1, "{text}");
}

/// `path` with each `{a|b}` replaced by each of its alternatives, and each other `{param}` by
/// a value of its kind.
fn expand(path: &str) -> Vec<String> {
    let mut paths = vec![String::new()];
    let mut rest = path;
    while let Some(start) = rest.find('{') {
        let end = start + rest[start..].find('}').expect("a closing brace");
        let (literal, inner) = (&rest[..start], &rest[start + 1..end]);
        let choices: Vec<&str> = if inner.contains('|') {
            inner.split('|').collect()
        } else {
            vec![match inner {
                "bid" => "0123456789abcdef01234567",
                "kind" => "c",
                _ => "nightly",
            }]
        };
        paths = paths
            .iter()
            .flat_map(|path| {
                choices
                    .iter()
                    .map(move |choice| format!("{path}{literal}{choice}"))
            })
            .collect();
        rest = &rest[end + 1..];
    }
    paths.into_iter().map(|path| path + rest).collect()
}

#[tokio::test]
async fn every_route_the_api_documents_answers() {
    // fresh-user-3: the `json_api` documentation (and `--api`'s help) listed stats, queues,
    // processes and POST /jobs only. Every `METHOD /path` it lists now answers with that
    // method; on a read-only server, so that the changes are refused rather than made.
    let docs = include_str!("../src/json_api.rs")
        .lines()
        .map_while(|line| line.strip_prefix("//!"))
        .collect::<Vec<_>>()
        .join("\n");
    let (_redis, _api, router) = server(&["--api", "--read-only"]).await;
    let mut documented = Vec::new();
    for span in docs.split('`').skip(1).step_by(2) {
        let Some((method, path)) = span.split_once(' ') else {
            continue;
        };
        if ["GET", "POST", "DELETE"].contains(&method) && path.starts_with('/') {
            documented.push((method, path));
        }
    }
    for feature in ["/periodic", "/batches", "/limits", "/locks"] {
        assert!(
            documented.contains(&("GET", feature)),
            "GET {feature} is not documented"
        );
    }
    for (method, path) in &documented {
        for path in expand(path) {
            let request = Request::builder()
                .method(*method)
                .uri(format!("/api/v1{path}"))
                .body(Body::empty())
                .unwrap();
            let res = send(&router, request).await;
            assert_ne!(
                res.status,
                StatusCode::METHOD_NOT_ALLOWED,
                "{method} {path}"
            );
            assert!(
                !res.body.contains("no such route"),
                "{method} {path}: {}",
                res.body
            );
            if *method != "GET" {
                assert_eq!(res.status, StatusCode::FORBIDDEN, "{method} {path}");
            }
        }
    }
    assert!(documented.len() >= 16, "{documented:?}");
}

#[tokio::test]
async fn the_api_pushes_jobs() {
    let (_redis, api, router) = server(&["--api"]).await;
    let res = send(
        &router,
        post_json(
            "/api/v1/jobs",
            &json!({"class": "Mailer", "args": [1, "a"], "queue": "mail", "tags": ["api"],
                    "retry": 3, "custom": {"tenant": "acme"}}),
        ),
    )
    .await;
    assert_eq!(res.status, StatusCode::CREATED, "{}", res.body);
    let jid = res.json()["jid"].as_str().unwrap().to_owned();
    assert_eq!(jid.len(), 24);
    let record = api.queue("mail").find_job(&jid).await.unwrap().unwrap();
    assert_eq!(record.class(), Some("Mailer"));
    assert_eq!(record.args(), [json!(1), json!("a")]);
    assert_eq!(record.tags(), ["api"]);
    assert_eq!(record.get("retry"), Some(&json!(3)));
    assert_eq!(record.get("tenant"), Some(&json!("acme")));

    // Scheduled, by delay or by time.
    let res = send(
        &router,
        post_json("/api/v1/jobs", &json!({"class": "Later", "in": 3600})),
    )
    .await;
    assert_eq!(res.status, StatusCode::CREATED);
    let at = hench::now_secs_f64() + 7_200.0;
    let res = send(
        &router,
        post_json("/api/v1/jobs", &json!({"class": "Later", "at": at})),
    )
    .await;
    assert_eq!(res.status, StatusCode::CREATED);
    assert_eq!(api.scheduled().size().await.unwrap(), 2);
    let jid = res.json()["jid"].as_str().unwrap().to_owned();
    let entry = api.scheduled().find_job(&jid).await.unwrap().unwrap();
    assert!((entry.score() - at).abs() < 0.001);

    // Invalid requests: JSON errors, nothing pushed.
    for (body, status) in [
        (json!({"class": ""}), StatusCode::BAD_REQUEST),
        (json!({"class": "X", "queue": ""}), StatusCode::BAD_REQUEST),
        (
            json!({"class": "X", "at": 1, "in": 1}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"class": "X", "custom": {"jid": "x"}}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"class": "X", "unknown": 1}),
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        (json!({"args": [1]}), StatusCode::UNPROCESSABLE_ENTITY),
        (json!([1, 2]), StatusCode::UNPROCESSABLE_ENTITY),
    ] {
        let res = send(&router, post_json("/api/v1/jobs", &body)).await;
        assert_eq!(res.status, status, "{body}: {}", res.body);
        assert!(res.json()["error"].is_string(), "{}", res.body);
    }
    // Names with control characters (terminal escapes, line breaks) are refused.
    for body in [
        json!({"class": "X\u{1b}]0;pwned\u{7}"}),
        json!({"class": "X", "queue": "evil\u{1b}[31mRED\n  default"}),
        json!({"class": "X", "retry_queue": "a\rb"}),
        json!({"class": "X", "tags": ["ok", "bad\u{0}"]}),
        json!({"class": "X", "jid": "abc\n"}),
    ] {
        let res = send(&router, post_json("/api/v1/jobs", &body)).await;
        assert_eq!(res.status, StatusCode::BAD_REQUEST, "{body}");
        assert!(
            res.json()["error"]
                .as_str()
                .unwrap()
                .contains("control characters")
        );
    }
    let not_json = Request::post("/api/v1/jobs")
        .header("content-type", "application/json")
        .body(Body::from("{oops"))
        .unwrap();
    assert_eq!(
        send(&router, not_json).await.status,
        StatusCode::BAD_REQUEST
    );
    let form = Request::post("/api/v1/jobs")
        .header("content-type", "application/x-www-form-urlencoded")
        .body(Body::from("class=X"))
        .unwrap();
    assert_eq!(
        send(&router, form).await.status,
        StatusCode::UNSUPPORTED_MEDIA_TYPE
    );
    // Browsers on other sites are refused.
    let mut cross = post_json("/api/v1/jobs", &json!({"class": "X"}));
    cross
        .headers_mut()
        .insert("sec-fetch-site", "cross-site".parse().unwrap());
    assert_eq!(send(&router, cross).await.status, StatusCode::FORBIDDEN);
    assert_eq!(api.queue("default").size().await.unwrap(), 0);
}

#[tokio::test]
async fn read_only_servers_refuse_to_push() {
    let (_redis, api, router) = server(&["--api", "--read-only"]).await;
    let res = send(&router, post_json("/api/v1/jobs", &json!({"class": "X"}))).await;
    assert_eq!(res.status, StatusCode::FORBIDDEN);
    assert_eq!(api.queue("default").size().await.unwrap(), 0);
    // The dashboard is read-only too.
    let page = send(&router, get("/queues")).await;
    assert!(page.body.contains("read-only"));
}

#[tokio::test]
async fn everything_can_live_under_a_prefix() {
    let (_redis, _api, router) = server(&["--prefix", "/hench/", "--api"]).await;
    let res = send(&router, get("/hench")).await;
    assert_eq!(res.status, StatusCode::OK);
    assert!(res.body.contains("href=\"/hench/busy\""));
    assert_eq!(
        send(&router, get("/hench/api/v1/stats")).await.status,
        StatusCode::OK
    );
    assert_eq!(
        send(&router, get("/hench/healthz")).await.status,
        StatusCode::OK
    );
    // Probes that bypass the proxy find the health check at the root too.
    assert_eq!(send(&router, get("/healthz")).await.status, StatusCode::OK);
    let root = send(&router, get("/")).await;
    assert_eq!(root.status, StatusCode::SEE_OTHER);
    assert_eq!(root.headers.get("location").unwrap(), "/hench");
    // The prefix with a trailing slash is the home page too (a proxy's `location /hench/`
    // redirects `/hench` there), with the same links.
    let slash = send(&router, get("/hench/")).await;
    assert_eq!(slash.status, StatusCode::OK);
    assert!(slash.body.contains("href=\"/hench/busy\""));
    assert_eq!(
        send(&router, get("/hench/nowhere")).await.status,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        send(&router, get("/hench/api/v1/nowhere")).await.status,
        StatusCode::NOT_FOUND
    );
    for outside in ["/api/v1/stats", "/busy", "/queues"] {
        assert_eq!(
            send(&router, get(outside)).await.status,
            StatusCode::NOT_FOUND,
            "{outside}"
        );
    }
}

/// Checks that `res` is a JSON error with status `status`, the API's own headers included.
fn assert_json_error(res: &Res, status: StatusCode, what: &str) {
    assert_eq!(res.status, status, "{what}: {}", res.body);
    assert_eq!(
        res.headers
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("application/json"),
        "{what}: {}",
        res.body
    );
    assert!(res.json()["error"].is_string(), "{what}: {}", res.body);
    assert_eq!(res.headers["cache-control"], "no-store", "{what}");
    assert_eq!(res.headers["x-content-type-options"], "nosniff", "{what}");
}

#[tokio::test]
async fn api_errors_are_json_for_bad_paths_unknown_routes_and_methods() {
    // Round-3 web-api-security-9: the feature routes answered a path parameter that is not
    // UTF-8 once decoded with axum's 400 in text/plain, an empty segment with the dashboard's
    // HTML 404, and a wrong method with an empty 405, though the API documents JSON errors.
    for prefix in ["", "/hench"] {
        let args: &[&str] = if prefix.is_empty() {
            &["--api"]
        } else {
            &["--api", "--prefix", "/hench"]
        };
        let (_redis, _api, router) = server(args).await;
        let api = |path: &str| format!("{prefix}/api/v1{path}");
        let request = |method: &str, path: &str| {
            Request::builder()
                .method(method)
                .uri(api(path))
                .body(Body::empty())
                .unwrap()
        };
        // Path parameters that are not UTF-8 once percent-decoded.
        for (method, path) in [
            ("GET", "/locks/%FF"),
            ("DELETE", "/locks/%FF?force=true"),
            ("GET", "/periodic/%FF"),
            ("POST", "/periodic/%FF/pause"),
            ("POST", "/periodic/x/%FF"),
            ("GET", "/limits/c/%FF"),
            ("DELETE", "/limits/%FF/x"),
            ("GET", "/batches/%FF"),
            ("POST", "/batches/%FF/cancel"),
        ] {
            let res = send(&router, request(method, path)).await;
            assert_json_error(&res, StatusCode::BAD_REQUEST, &format!("{method} {path}"));
            assert!(
                res.json()["error"].as_str().unwrap().contains("UTF-8"),
                "{}",
                res.body
            );
        }
        // No such route: empty segments, unknown paths, the API's root.
        for path in [
            "/limits/c/",
            "/limits/c",
            "/locks/",
            "/periodic/",
            "/batches/",
            "/nope",
            "/stats/more",
            "/",
            "",
        ] {
            let res = send(&router, request("GET", path)).await;
            assert_json_error(&res, StatusCode::NOT_FOUND, &format!("GET {}", api(path)));
        }
        let res = send(&router, request("DELETE", "/nope")).await;
        assert_json_error(&res, StatusCode::NOT_FOUND, "DELETE /nope");
        // Wrong methods: 405 with the allowed ones.
        for (method, path, allowed) in [
            ("GET", "/periodic/x/enqueue", "POST"),
            ("PUT", "/batches/0123456789abcdef01234567/cancel", "POST"),
            ("GET", "/jobs", "POST"),
            ("POST", "/stats", "GET,HEAD"),
            ("PATCH", "/locks/abc", "GET,HEAD,DELETE"),
        ] {
            let res = send(&router, request(method, path)).await;
            assert_json_error(
                &res,
                StatusCode::METHOD_NOT_ALLOWED,
                &format!("{method} {path}"),
            );
            assert_eq!(res.headers["allow"], allowed, "{method} {path}");
        }
        // The routes themselves still answer.
        assert_eq!(
            send(&router, request("GET", "/stats")).await.status,
            StatusCode::OK
        );
        assert_eq!(
            send(&router, request("GET", "/limits/c/nothing"))
                .await
                .status,
            StatusCode::NOT_FOUND
        );
    }
}

#[tokio::test]
async fn the_api_is_only_served_when_asked() {
    let (_redis, _api, router) = server(&[]).await;
    assert_eq!(
        send(&router, get("/api/v1/stats")).await.status,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn health_checks_report_redis_outages() {
    let (mut redis, _api, router) = server(&[]).await;
    assert_eq!(send(&router, get("/healthz")).await.status, StatusCode::OK);
    redis.stop();
    let res = tokio::time::timeout(Duration::from_secs(30), send(&router, get("/healthz")))
        .await
        .expect("the health check answers");
    assert_eq!(res.status, StatusCode::SERVICE_UNAVAILABLE);
}

/// A page on a hostname the attacker controls, rebound to 127.0.0.1 (DNS rebinding).
const REBOUND: &str = "rebind.attacker.example:7420";

/// A request carrying a `Host` header (and what a browser adds for a same-origin request).
fn for_host(mut request: Request<Body>, host: &str) -> Request<Body> {
    let headers = request.headers_mut();
    headers.insert("host", host.parse().unwrap());
    headers.insert("origin", format!("http://{host}").parse().unwrap());
    headers.insert("sec-fetch-site", "same-origin".parse().unwrap());
    request
}

#[tokio::test]
async fn foreign_host_names_are_refused_without_credentials() {
    // The default setup: loopback, no credentials, plus the API. A page whose hostname was
    // rebound to 127.0.0.1 passes every same-origin check (its Origin is the Host it sends), so
    // the Host itself must be checked.
    let (_redis, api, router) = server(&["--bind", "127.0.0.1:7420", "--api"]).await;
    let mut job = hench::JobPayload::new("Billing::Charge", vec![json!("secret-api-key")]);
    job.queue = "critical".to_owned();
    api.client().push(job).await.unwrap();

    let push = for_host(
        post_json(
            "/api/v1/jobs",
            &json!({"class": "Attacker::Job", "args": [1], "queue": "critical"}),
        ),
        REBOUND,
    );
    let res = send(&router, push).await;
    assert_eq!(res.status, StatusCode::FORBIDDEN, "{}", res.body);
    for path in ["/queues/critical", "/api/v1/queues", "/", "/nowhere"] {
        let res = send(&router, for_host(get(path), REBOUND)).await;
        assert_eq!(res.status, StatusCode::FORBIDDEN, "{path}");
        assert!(!res.body.contains("secret-api-key"), "{path}");
        assert!(res.headers.get("set-cookie").is_none(), "{path}");
        assert!(res.body.contains("--allowed-host"), "{}", res.body);
    }
    let delete = Request::post("/queues/critical")
        .header("content-type", "application/x-www-form-urlencoded")
        .header("cookie", format!("hench_csrf={}", "a".repeat(64)))
        .body(Body::from(format!("_csrf={}&delete=1", "a".repeat(64))))
        .unwrap();
    let res = send(&router, for_host(delete, REBOUND)).await;
    assert_eq!(res.status, StatusCode::FORBIDDEN);
    assert_eq!(api.queue("critical").size().await.unwrap(), 1);

    // Names that cannot be rebound keep working: localhost and IP addresses, any port.
    for host in [
        "127.0.0.1:7420",
        "localhost:7420",
        "LOCALHOST",
        "localhost.:7420",
        "dash.localhost:7420",
        "[::1]:7420",
        "10.1.2.3:7420",
        "[2001:db8::1]",
    ] {
        let res = send(&router, for_host(get("/"), host)).await;
        assert_eq!(res.status, StatusCode::OK, "{host}");
    }
    // HTTP/1.0 clients may send no Host at all (browsers always do).
    assert_eq!(send(&router, get("/")).await.status, StatusCode::OK);
    // Health checks answer whatever the Host (probes and load balancers use many names).
    let res = send(&router, for_host(get("/healthz"), REBOUND)).await;
    assert_eq!(res.status, StatusCode::OK);
}

#[tokio::test]
async fn allowed_hosts_open_the_server_to_other_names() {
    // Behind a proxy that forwards its own host name.
    let (_redis, _api, router) =
        server(&["--allowed-host", "jobs.example.com,.internal.example"]).await;
    for (host, status) in [
        ("jobs.example.com", StatusCode::OK),
        ("JOBS.example.com:8443", StatusCode::OK),
        ("internal.example", StatusCode::OK),
        ("a.b.internal.example:80", StatusCode::OK),
        ("localhost:7420", StatusCode::OK),
        ("127.0.0.1", StatusCode::OK),
        ("evil.example", StatusCode::FORBIDDEN),
        ("jobs.example.com.evil.example", StatusCode::FORBIDDEN),
        ("xinternal.example", StatusCode::FORBIDDEN),
    ] {
        assert_eq!(
            send(&router, for_host(get("/"), host)).await.status,
            status,
            "{host}"
        );
    }
    // `*` accepts every name.
    let (_redis, _api, router) = server(&["--allowed-host", "*"]).await;
    assert_eq!(
        send(&router, for_host(get("/"), REBOUND)).await.status,
        StatusCode::OK
    );
}

#[tokio::test]
async fn credentials_protect_every_host_name() {
    // With Basic authentication a rebound page has no credentials for its own origin, so any
    // Host is served to authenticated requests (unless allowed hosts are listed).
    let (_redis, _api, router) = server(&["--username", "a", "--password", "b"]).await;
    let request = Request::get("/")
        .header("authorization", basic("a", "b"))
        .body(Body::empty())
        .unwrap();
    let res = send(&router, for_host(request, "jobs.example.com")).await;
    assert_eq!(res.status, StatusCode::OK);
    let res = send(&router, for_host(get("/"), "jobs.example.com")).await;
    assert_eq!(res.status, StatusCode::UNAUTHORIZED);

    let (_redis, _api, router) = server(&[
        "--username",
        "a",
        "--password",
        "b",
        "--allowed-host",
        "jobs.example.com",
    ])
    .await;
    for (host, status) in [
        ("jobs.example.com", StatusCode::OK),
        ("other.example.com", StatusCode::FORBIDDEN),
    ] {
        let request = Request::get("/")
            .header("authorization", basic("a", "b"))
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            send(&router, for_host(request, host)).await.status,
            status,
            "{host}"
        );
    }
}

#[tokio::test]
async fn times_beyond_the_calendar_are_refused_or_reported_as_unknown() {
    let (_redis, api, router) = server(&["--api"]).await;
    // An `at` in microseconds, or absurd delays: refused instead of scheduling a job no page
    // can show the time of.
    for body in [
        json!({"class": "X", "at": 1e13}),
        json!({"class": "X", "at": 1_793_509_572_430_123.0_f64}),
        json!({"class": "X", "in": 1e13}),
        json!({"class": "X", "at": f64::MAX}),
    ] {
        let res = send(&router, post_json("/api/v1/jobs", &body)).await;
        assert_eq!(res.status, StatusCode::BAD_REQUEST, "{body}: {}", res.body);
        assert!(
            res.json()["error"].as_str().unwrap().contains("year 10000"),
            "{}",
            res.body
        );
    }
    // The last moment of the year 9999 is fine.
    let res = send(
        &router,
        post_json(
            "/api/v1/jobs",
            &json!({"class": "X", "at": 253_402_300_799.0}),
        ),
    )
    .await;
    assert_eq!(res.status, StatusCode::CREATED, "{}", res.body);
    assert_eq!(api.scheduled().size().await.unwrap(), 1);

    // A process whose times are beyond the calendar is still listed.
    let mut conn = api.redis().conn().await.unwrap();
    let _: i64 = redis_rs::cmd("SADD")
        .arg("processes")
        .arg("far:1:abc")
        .query_async(&mut conn)
        .await
        .unwrap();
    let info = json!({"hostname": "far", "pid": 1, "started_at": 1e17});
    let _: i64 = redis_rs::cmd("HSET")
        .arg("far:1:abc")
        .arg("info")
        .arg(info.to_string())
        .arg("beat")
        .arg(1e13)
        .query_async(&mut conn)
        .await
        .unwrap();
    let res = send(&router, get("/api/v1/processes")).await;
    assert_eq!(res.status, StatusCode::OK);
    let process = &res.json()[0];
    assert_eq!(process["identity"], "far:1:abc");
    assert_eq!(process["started_at"], Value::Null);
    assert_eq!(process["beat"], Value::Null);
}

#[tokio::test]
async fn the_health_check_path_cannot_be_the_prefix() {
    // security-supply-chain-6: `--prefix /healthz` passed validation, then building the routes
    // panicked (the health check and the dashboard both wanted `/healthz`).
    for prefix in ["/healthz", "/healthz/"] {
        let error = Cli::try_parse_from(["hench", "web", "--prefix", prefix])
            .unwrap_err()
            .to_string();
        assert!(error.contains("health check"), "{prefix}: {error}");
    }
    // Arguments built in code are checked too.
    let Command::Web(mut web) = Cli::try_parse_from(["hench", "web"]).unwrap().command else {
        panic!("not `hench web`");
    };
    web.prefix = "/healthz".to_owned();
    let error = ServerConfig::from_args(&web).unwrap_err().to_string();
    assert!(error.contains("health check"), "{error}");
    // A prefix below it is fine: both health checks and the dashboard answer.
    let (_redis, _api, router) = server(&["--prefix", "/healthz/jobs"]).await;
    for path in ["/healthz", "/healthz/jobs/healthz"] {
        let res = send(&router, get(path)).await;
        assert_eq!(res.status, StatusCode::OK, "{path}");
        assert_eq!(res.body, "ok\n", "{path}");
    }
    let res = send(&router, get("/healthz/jobs")).await;
    assert_eq!(res.status, StatusCode::OK);
    assert!(res.body.contains("<title>Hench</title>"), "{}", res.body);
}
