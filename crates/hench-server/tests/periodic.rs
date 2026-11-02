//! The periodic schedules in `hench web --api` (`docs/design/periodic.md` §2.12, §3.4 item 33):
//! `GET /periodic`, `GET /periodic/{name}` and `POST /periodic/{name}/{enqueue|pause|unpause}`,
//! with read-only refusals and unknown names.

use std::time::Duration;

use axum::Router;
use axum::body::Body;
use clap::Parser;
use hench::api::Api;
use hench::periodic::Periodic;
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
    body: String,
}

impl Res {
    #[track_caller]
    fn expect(self, status: StatusCode) -> Value {
        assert_eq!(self.status, status, "{}", self.body);
        serde_json::from_str(&self.body).unwrap_or_else(|_| panic!("not JSON: {}", self.body))
    }
}

async fn send(router: &Router, request: Request<Body>) -> Res {
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    Res {
        status,
        body: String::from_utf8_lossy(&bytes).into_owned(),
    }
}

async fn get(router: &Router, path: &str) -> Res {
    send(router, Request::get(path).body(Body::empty()).unwrap()).await
}

async fn post(router: &Router, path: &str) -> Res {
    send(router, Request::post(path).body(Body::empty()).unwrap()).await
}

/// `nightly` (02:00 in Paris), `a/b` (every minute, unique) and `cleanup` (paused).
async fn seed(api: &Api) {
    let set = api.periodic();
    for periodic in [
        Periodic::cron("nightly", "0 2 * * *")
            .tz("Europe/Paris")
            .class("Report", vec![json!(42)])
            .queue("reports")
            .description("The nightly report."),
        Periodic::every("a/b", Duration::from_secs(60))
            .class("Sync", vec![])
            .unique_for(Duration::from_secs(600)),
        Periodic::cron("cleanup", "*/5 * * * *")
            .class("Cleanup", vec![])
            .paused(true),
    ] {
        set.upsert(&periodic).await.unwrap();
    }
}

#[tokio::test]
async fn the_api_lists_and_shows_the_schedules() {
    let (_redis, api, router) = server(&["--api"]).await;
    let empty = get(&router, "/api/v1/periodic")
        .await
        .expect(StatusCode::OK);
    assert_eq!(
        empty,
        json!({"size": 0, "page": 1, "count": 100, "schedules": []})
    );
    seed(&api).await;
    let listed = get(&router, "/api/v1/periodic")
        .await
        .expect(StatusCode::OK);
    assert_eq!(listed["size"], 3);
    let names: Vec<&str> = listed["schedules"]
        .as_array()
        .unwrap()
        .iter()
        .map(|schedule| schedule["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["a/b", "cleanup", "nightly"]);
    let nightly = &listed["schedules"][2];
    assert_eq!(
        nightly["timetable"],
        json!({"cron": "0 2 * * *", "tz": "Europe/Paris"})
    );
    assert_eq!(nightly["class"], "Report");
    assert_eq!(nightly["queue"], "reports");
    assert_eq!(nightly["source"], "api");
    assert_eq!(nightly["paused"], false);
    assert_eq!(nightly["overdue"], false);
    assert!(nightly["next"].as_str().unwrap().contains("T01:00:00"));
    assert_eq!(listed["schedules"][1]["paused"], true);
    // Pages.
    let second = get(&router, "/api/v1/periodic?page=2&count=2")
        .await
        .expect(StatusCode::OK);
    assert_eq!(second["schedules"].as_array().unwrap().len(), 1);
    assert_eq!(second["schedules"][0]["name"], "nightly");
    // Pages past the end, however far: empty.
    let far = get(
        &router,
        &format!("/api/v1/periodic?page={}&count=1000", usize::MAX),
    )
    .await
    .expect(StatusCode::OK);
    assert_eq!(far["schedules"], json!([]));
    assert_eq!(far["size"], 3);
    for query in ["count=0", "count=1001", "page=0", "count=x"] {
        let error = get(&router, &format!("/api/v1/periodic?{query}"))
            .await
            .expect(StatusCode::BAD_REQUEST);
        assert!(error["error"].is_string(), "{query}");
    }
    // One schedule: next ticks, template, history, fields.
    let shown = get(&router, "/api/v1/periodic/nightly")
        .await
        .expect(StatusCode::OK);
    assert_eq!(shown["upcoming"].as_array().unwrap().len(), 5);
    assert!(shown["describe"].as_str().unwrap().contains("Europe/Paris"));
    assert_eq!(shown["template"]["class"], "Report");
    assert_eq!(shown["template"]["args"], json!([42]));
    assert_eq!(shown["history"], json!([]));
    assert_eq!(shown["description"], "The nightly report.");
    assert_eq!(shown["fields"]["source"], "api");
    // Names with `/`, percent-encoded.
    let slashed = get(&router, "/api/v1/periodic/a%2Fb")
        .await
        .expect(StatusCode::OK);
    assert_eq!(slashed["name"], "a/b");
    assert_eq!(slashed["timetable"], json!({"every": 60, "offset": 0}));
    get(&router, "/api/v1/periodic/missing")
        .await
        .expect(StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn the_api_enqueues_pauses_and_resumes() {
    let (_redis, api, router) = server(&["--api"]).await;
    seed(&api).await;
    let enqueued = post(&router, "/api/v1/periodic/nightly/enqueue")
        .await
        .expect(StatusCode::OK);
    assert_eq!(enqueued["enqueued"], true);
    let jid = enqueued["jid"].as_str().unwrap().to_owned();
    assert_eq!(api.queue("reports").size().await.unwrap(), 1);
    let shown = get(&router, "/api/v1/periodic/nightly")
        .await
        .expect(StatusCode::OK);
    assert_eq!(shown["history"][0]["result"], "manual");
    assert_eq!(shown["history"][0]["jid"], jid);
    assert_eq!(shown["manual"]["jid"], jid);
    // A unique job whose lock is held: nothing pushed.
    let first = post(&router, "/api/v1/periodic/a%2Fb/enqueue")
        .await
        .expect(StatusCode::OK);
    assert_eq!(first["enqueued"], true);
    let second = post(&router, "/api/v1/periodic/a%2Fb/enqueue")
        .await
        .expect(StatusCode::OK);
    assert_eq!(second, json!({"enqueued": false, "jid": null}));
    // Pause and resume.
    let paused = post(&router, "/api/v1/periodic/nightly/pause")
        .await
        .expect(StatusCode::OK);
    assert_eq!(
        paused,
        json!({"name": "nightly", "paused": true, "changed": true})
    );
    let again = post(&router, "/api/v1/periodic/nightly/pause")
        .await
        .expect(StatusCode::OK);
    assert_eq!(again["changed"], false);
    assert!(
        api.periodic()
            .get("nightly")
            .await
            .unwrap()
            .unwrap()
            .is_paused()
    );
    let resumed = post(&router, "/api/v1/periodic/nightly/unpause")
        .await
        .expect(StatusCode::OK);
    assert_eq!(
        resumed,
        json!({"name": "nightly", "paused": false, "changed": true})
    );
    // Unknown actions and names.
    post(&router, "/api/v1/periodic/nightly/explode")
        .await
        .expect(StatusCode::BAD_REQUEST);
    post(&router, "/api/v1/periodic/missing/pause")
        .await
        .expect(StatusCode::NOT_FOUND);
    // A definition this version cannot read: refused with the reason.
    let () = {
        let mut conn = api.redis().conn().await.unwrap();
        hench::redis_rs::cmd("HSET")
            .arg(hench::keys::periodic("cleanup"))
            .arg("spec")
            .arg(r#"{"v":99}"#)
            .query_async(&mut conn)
            .await
            .unwrap()
    };
    let refused = post(&router, "/api/v1/periodic/cleanup/unpause")
        .await
        .expect(StatusCode::BAD_REQUEST);
    assert!(
        refused["error"].as_str().unwrap().contains("cleanup"),
        "{refused}"
    );
    let shown = get(&router, "/api/v1/periodic/cleanup")
        .await
        .expect(StatusCode::OK);
    assert_eq!(shown["timetable"], Value::Null);
    assert!(
        shown["error"].as_str().unwrap().contains("version 99"),
        "{shown}"
    );
}

#[tokio::test]
async fn read_only_servers_show_schedules_but_do_not_change_them() {
    let (_redis, api, router) = server(&["--api", "--read-only"]).await;
    seed(&api).await;
    get(&router, "/api/v1/periodic")
        .await
        .expect(StatusCode::OK);
    get(&router, "/api/v1/periodic/nightly")
        .await
        .expect(StatusCode::OK);
    for action in ["enqueue", "pause", "unpause"] {
        post(&router, &format!("/api/v1/periodic/nightly/{action}"))
            .await
            .expect(StatusCode::FORBIDDEN);
    }
    assert_eq!(api.queue("reports").size().await.unwrap(), 0);
    assert!(
        !api.periodic()
            .get("nightly")
            .await
            .unwrap()
            .unwrap()
            .is_paused()
    );
}

#[tokio::test]
async fn the_periodic_routes_need_the_api() {
    let (_redis, api, router) = server(&[]).await;
    seed(&api).await;
    let res = get(&router, "/api/v1/periodic").await;
    assert_eq!(res.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn schedules_and_limiters_cannot_be_named_dot_or_dot_dot() {
    // Round 3, web-api-security-7: `.` and `..` were valid names, but every URL naming them
    // (`/periodic/..`, `/limits/c/..`, the JSON API's) is resolved by browsers and HTTP clients
    // as a dot segment (`%2e` too), so the dashboard's links and buttons reached other pages:
    // such a schedule or limiter could not be opened or managed. They are refused up front.
    let (_redis, api, router) = server(&["--api"]).await;
    for name in [".", ".."] {
        let error = api
            .periodic()
            .upsert(&Periodic::cron(name, "0 2 * * *").class("Report", vec![]))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("dot segment"), "{name}: {error}");
        let error = hench::limit::Limiter::concurrent(name, 1)
            .acquire(api.redis())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("dot segment"), "{name}: {error}");
    }
    assert_eq!(api.periodic().size().await.unwrap(), 0);
    // Names that merely contain dots are fine.
    for name in ["...", ".hidden", "v1.2"] {
        api.periodic()
            .upsert(&Periodic::cron(name, "0 2 * * *").class("Report", vec![]))
            .await
            .unwrap();
        let shown = get(
            &router,
            &format!("/api/v1/periodic/{}", name.replace('.', "%2E")),
        )
        .await
        .expect(StatusCode::OK);
        assert_eq!(shown["name"], name);
        let _permit = hench::limit::Limiter::concurrent(name, 1)
            .acquire(api.redis())
            .await
            .unwrap();
    }
}
