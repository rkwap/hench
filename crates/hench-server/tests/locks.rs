//! The locks of unique jobs in `hench web --api` (`docs/design/unique.md` §2.14, §3.5 S2):
//! `GET /locks`, `GET /locks/{digest}`, `DELETE /locks/{digest}`, and what `POST /jobs` answers
//! for unique jobs (201 pushed or replaced, 200 for a dropped duplicate, 409 with
//! `on_conflict: "error"`, 400 or 422 for invalid settings).

use std::time::Duration;

use axum::Router;
use axum::body::Body;
use clap::Parser;
use hench::api::Api;
use hench::test_util::TestRedis;
use hench::{JobPayload, Unique, UniqueUntil};
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
    fn json(&self) -> Value {
        serde_json::from_str(&self.body).unwrap_or_else(|_| panic!("not JSON: {}", self.body))
    }

    #[track_caller]
    fn expect(self, status: StatusCode) -> Value {
        assert_eq!(self.status, status, "{}", self.body);
        self.json()
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

async fn delete(router: &Router, path: &str) -> Res {
    send(router, Request::delete(path).body(Body::empty()).unwrap()).await
}

async fn post_job(router: &Router, body: &Value) -> Res {
    let request = Request::post("/api/v1/jobs")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    send(router, request).await
}

/// Pushes a unique job with one argument, `1`; its jid, or `None` for a duplicate.
async fn push(api: &Api, class: &str, queue: &str, ttl: u64, until: UniqueUntil) -> Option<String> {
    let mut job = JobPayload::new(class, vec![json!(1)]);
    job.queue = queue.to_owned();
    job.unique = Some(Unique::new(Duration::from_secs(ttl)).until(until).into());
    api.client().push(job).await.unwrap()
}

fn digest(class: &str, queue: &str) -> String {
    hench::unique::digest(class, Some(queue), &json!([1])).unwrap()
}

/// Three locks expiring in 1, 10 and 20 minutes, and two duplicates of the first.
async fn seed(api: &Api) -> [String; 3] {
    let sync = push(api, "Sync", "default", 60, UniqueUntil::Success)
        .await
        .unwrap();
    let mailer = push(api, "Mailer", "mail", 600, UniqueUntil::Start)
        .await
        .unwrap();
    let report = push(api, "Report", "default", 1_200, UniqueUntil::Expired)
        .await
        .unwrap();
    for _ in 0..2 {
        assert_eq!(
            push(api, "Sync", "default", 60, UniqueUntil::Success).await,
            None
        );
    }
    [sync, mailer, report]
}

#[tokio::test]
async fn the_api_lists_the_locks_and_the_last_hour() {
    let (_redis, api, router) = server(&["--api"]).await;
    let [sync, mailer, report] = seed(&api).await;
    let listed = get(&router, "/api/v1/locks").await.expect(StatusCode::OK);
    assert_eq!(listed["size"], 3);
    assert_eq!(listed["truncated"], false);
    assert_eq!(
        listed["last_hour"],
        json!({"duplicates": 2, "replaced": 0, "lapsed": 0})
    );
    // Soonest expiry first, every field of the lock.
    let locks = listed["locks"].as_array().unwrap();
    let jids: Vec<&str> = locks
        .iter()
        .map(|lock| lock["jid"].as_str().unwrap())
        .collect();
    assert_eq!(jids, [sync.as_str(), mailer.as_str(), report.as_str()]);
    let first = &locks[0];
    assert_eq!(first["digest"], digest("Sync", "default"));
    assert_eq!(first["class"], "Sync");
    assert_eq!(first["queue"], "default");
    assert_eq!(first["until"], "success");
    assert_eq!(first["unique_for"], 60);
    for time in ["locked_at", "expires_at"] {
        let text = first[time].as_str().unwrap();
        assert!(chrono::DateTime::parse_from_rfc3339(text).is_ok(), "{text}");
    }
    assert_eq!(first["scheduled_at"], Value::Null);
    assert_eq!(locks[1]["until"], "start");
    assert_eq!(locks[2]["until"], "expired");

    // One class (its own counters), a limit.
    let mailers = get(&router, "/api/v1/locks?class=Mailer")
        .await
        .expect(StatusCode::OK);
    assert_eq!(mailers["locks"].as_array().unwrap().len(), 1);
    assert_eq!(mailers["locks"][0]["jid"], mailer.as_str());
    assert_eq!(mailers["last_hour"]["duplicates"], 0);
    let syncs = get(&router, "/api/v1/locks?class=Sync")
        .await
        .expect(StatusCode::OK);
    assert_eq!(syncs["last_hour"]["duplicates"], 2);
    let none = get(&router, "/api/v1/locks?class=Nope")
        .await
        .expect(StatusCode::OK);
    assert_eq!(none["locks"], json!([]));
    assert_eq!(none["size"], 3, "the size counts every lock");
    let two = get(&router, "/api/v1/locks?limit=2")
        .await
        .expect(StatusCode::OK);
    assert_eq!(two["locks"].as_array().unwrap().len(), 2);
    assert_eq!(two["truncated"], true);
    // An empty class is no filter.
    let all = get(&router, "/api/v1/locks?class=")
        .await
        .expect(StatusCode::OK);
    assert_eq!(all["locks"].as_array().unwrap().len(), 3);

    for query in ["limit=0", "limit=1001", "limit=x", "limit=-1"] {
        let error = get(&router, &format!("/api/v1/locks?{query}"))
            .await
            .expect(StatusCode::BAD_REQUEST);
        assert!(error["error"].is_string(), "{query}: {error}");
    }
}

#[tokio::test]
async fn the_api_shows_a_lock_and_where_its_holder_is() {
    let (_redis, api, router) = server(&["--api"]).await;
    let [sync, ..] = seed(&api).await;
    let path = format!("/api/v1/locks/{}", digest("Sync", "default"));
    let lock = get(&router, &path).await.expect(StatusCode::OK);
    assert_eq!(lock["jid"], sync.as_str());
    assert_eq!(lock["class"], "Sync");
    assert_eq!(lock["fields"]["jid"], sync.as_str());
    assert_eq!(lock["fields"]["until"], "success");
    assert_eq!(lock["fields"]["for"], "60");
    assert_eq!(
        lock["holder_place"],
        json!({"place": "queued", "queue": "default", "position": 1})
    );
    // The holder gone (an `expired` lock outlives its job): no place, the search complete.
    let mut conn = api.redis().conn().await.unwrap();
    let () = hench::redis_rs::cmd("DEL")
        .arg(hench::keys::queue("default"))
        .query_async(&mut conn)
        .await
        .unwrap();
    let path = format!("/api/v1/locks/{}", digest("Report", "default"));
    let lock = get(&router, &path).await.expect(StatusCode::OK);
    assert_eq!(
        lock["holder_place"],
        json!({"place": null, "complete": true})
    );
    // Unknown and malformed digests.
    for digest in ["unknown", "not%20a%20digest", &"x".repeat(200)] {
        let error = get(&router, &format!("/api/v1/locks/{digest}"))
            .await
            .expect(StatusCode::NOT_FOUND);
        assert_eq!(error["error"], "no unique job holds this lock");
    }
}

#[tokio::test]
async fn the_api_releases_a_lock_its_holder_still_holds() {
    let (_redis, api, router) = server(&["--api"]).await;
    let [sync, mailer, _] = seed(&api).await;
    let locks = api.unique_locks();
    let sync_path = format!("/api/v1/locks/{}", digest("Sync", "default"));

    // The holder must be named, or the release forced; not both.
    for query in ["", "?jid=", "?force=false"] {
        delete(&router, &format!("{sync_path}{query}"))
            .await
            .expect(StatusCode::BAD_REQUEST);
    }
    delete(&router, &format!("{sync_path}?jid={sync}&force=true"))
        .await
        .expect(StatusCode::BAD_REQUEST);
    delete(&router, &format!("{sync_path}?force=maybe"))
        .await
        .expect(StatusCode::BAD_REQUEST);
    // Another job holds it: left alone, the holder named.
    let conflict = delete(
        &router,
        &format!("{sync_path}?jid=0123456789abcdef01234567"),
    )
    .await
    .expect(StatusCode::CONFLICT);
    assert_eq!(conflict["holder"], sync.as_str());
    assert_eq!(locks.size().await.unwrap(), 3);

    let released = delete(&router, &format!("{sync_path}?jid={sync}"))
        .await
        .expect(StatusCode::OK);
    assert_eq!(released["released"], true);
    assert_eq!(locks.get(&digest("Sync", "default")).await.unwrap(), None);
    // Gone now.
    delete(&router, &format!("{sync_path}?jid={sync}"))
        .await
        .expect(StatusCode::NOT_FOUND);
    // A duplicate can be pushed again.
    assert!(
        push(&api, "Sync", "default", 60, UniqueUntil::Success)
            .await
            .is_some()
    );

    // Forced: whoever holds it.
    let mailer_path = format!("/api/v1/locks/{}", digest("Mailer", "mail"));
    delete(&router, &format!("{mailer_path}?force=true"))
        .await
        .expect(StatusCode::OK);
    assert_eq!(locks.get(&digest("Mailer", "mail")).await.unwrap(), None);
    delete(&router, &format!("{mailer_path}?force=true"))
        .await
        .expect(StatusCode::NOT_FOUND);
    drop(mailer);
}

#[tokio::test]
async fn read_only_servers_show_locks_but_do_not_release_them() {
    let (_redis, api, router) = server(&["--api", "--read-only"]).await;
    let [sync, ..] = seed(&api).await;
    let path = format!("/api/v1/locks/{}", digest("Sync", "default"));
    get(&router, "/api/v1/locks").await.expect(StatusCode::OK);
    get(&router, &path).await.expect(StatusCode::OK);
    for query in [format!("?jid={sync}"), "?force=true".to_owned()] {
        delete(&router, &format!("{path}{query}"))
            .await
            .expect(StatusCode::FORBIDDEN);
    }
    assert_eq!(api.unique_locks().size().await.unwrap(), 3);
}

#[tokio::test]
async fn the_lock_routes_need_the_api() {
    let (_redis, api, router) = server(&[]).await;
    seed(&api).await;
    let res = get(&router, "/api/v1/locks").await;
    assert_eq!(res.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn post_jobs_answers_duplicates_and_replacements() {
    // S2: 201 pushed, 200 for a dropped duplicate, 409 with `on_conflict: "error"`, 201 with
    // the replaced jid for a debounce push.
    let (_redis, api, router) = server(&["--api"]).await;
    let job = json!({"class": "Sync", "args": [42], "unique": {"for": 600}});
    let pushed = post_job(&router, &job).await.expect(StatusCode::CREATED);
    let jid = pushed["jid"].as_str().unwrap().to_owned();
    let duplicate = post_job(&router, &job).await.expect(StatusCode::OK);
    assert_eq!(duplicate, json!({"jid": null, "duplicate_of": jid}));
    let refused = post_job(
        &router,
        &json!({"class": "Sync", "args": [42], "unique": {"for": 600, "on_conflict": "error"}}),
    )
    .await
    .expect(StatusCode::CONFLICT);
    assert!(
        refused["error"].as_str().unwrap().contains(&jid),
        "{refused}"
    );
    assert_eq!(api.queue("default").size().await.unwrap(), 1);
    // The lock is visible through the API, with the job's settings.
    let lock = get(
        &router,
        &format!(
            "/api/v1/locks/{}",
            hench::unique::digest("Sync", Some("default"), &json!([42])).unwrap()
        ),
    )
    .await
    .expect(StatusCode::OK);
    assert_eq!(lock["jid"], jid.as_str());
    assert_eq!(lock["unique_for"], 600);

    // An explicit key and another queue: a duplicate whatever the arguments, only with
    // `across_queues`.
    let keyed = |args: Value, queue: &str, across: bool| {
        json!({"class": "Mail", "args": args, "queue": queue,
               "unique": {"for": 60, "key": "user:7", "across_queues": across}})
    };
    post_job(&router, &keyed(json!([1]), "a", true))
        .await
        .expect(StatusCode::CREATED);
    post_job(&router, &keyed(json!([2]), "b", true))
        .await
        .expect(StatusCode::OK);
    post_job(&router, &keyed(json!([3]), "b", false))
        .await
        .expect(StatusCode::CREATED);

    // Debounce: each push replaces the job still waiting in the schedule.
    let debounce = json!({"class": "Reindex", "args": [1], "in": 60,
                          "unique": {"for": 60, "on_conflict": "replace"}});
    let first = post_job(&router, &debounce)
        .await
        .expect(StatusCode::CREATED);
    let first = first["jid"].as_str().unwrap().to_owned();
    let second = post_job(&router, &debounce)
        .await
        .expect(StatusCode::CREATED);
    assert_eq!(second["replaced"], first.as_str());
    assert_ne!(second["jid"], first.as_str());
    assert_eq!(api.scheduled().size().await.unwrap(), 1);

    // `unique: false` pushes without a lock.
    for _ in 0..2 {
        post_job(
            &router,
            &json!({"class": "Sync", "args": [42], "unique": false}),
        )
        .await
        .expect(StatusCode::CREATED);
    }
}

#[tokio::test]
async fn post_jobs_refuses_invalid_unique_settings() {
    let (_redis, api, router) = server(&["--api"]).await;
    // Settings the push refuses: 400, with the reason.
    for unique in [
        json!({}),
        json!({"for": 0}),
        json!({"for": 1_000_000_000}),
        json!({"for": 60, "digest": "not a digest!"}),
    ] {
        let error = post_job(&router, &json!({"class": "Sync", "unique": unique}))
            .await
            .expect(StatusCode::BAD_REQUEST);
        assert!(error["error"].is_string(), "{unique}: {error}");
    }
    // Values of the wrong shape: the body is refused before the push (422).
    for unique in [
        json!("yes"),
        json!(true),
        json!({"for": 60, "on_conflict": "skip"}),
        json!({"for": "a minute"}),
    ] {
        let res = post_job(&router, &json!({"class": "Sync", "unique": unique})).await;
        assert_eq!(
            res.status,
            StatusCode::UNPROCESSABLE_ENTITY,
            "{unique}: {}",
            res.body
        );
    }
    assert_eq!(api.queue("default").size().await.unwrap(), 0);
    assert_eq!(api.unique_locks().size().await.unwrap(), 0);
    // A policy this version does not know is refused (422) at the API: only stored jobs are
    // read leniently (a newer version's policy reads as `success`, unique.md §2.6.4), a request
    // is checked like the macro's and `hench enqueue`'s options.
    let res = post_job(
        &router,
        &json!({"class": "Sync", "unique": {"for": 60, "until": "later"}}),
    )
    .await;
    assert_eq!(res.status, StatusCode::UNPROCESSABLE_ENTITY, "{}", res.body);
    assert!(res.body.contains("later"), "{}", res.body);
    assert_eq!(api.queue("default").size().await.unwrap(), 0);
    // Each policy by its name.
    for until in ["success", "start", "expired"] {
        let pushed = post_job(
            &router,
            &json!({"class": format!("Policy-{until}"), "unique": {"for": 60, "until": until}}),
        )
        .await
        .expect(StatusCode::CREATED);
        let jid = pushed["jid"].as_str().unwrap();
        let record = api.queue("default").find_job(jid).await.unwrap().unwrap();
        assert_eq!(record.get("unique").unwrap()["until"], until);
    }
    let locks = api.unique_locks();
    let page = locks
        .page(1, 10, hench::api::SortOrder::Ascending)
        .await
        .unwrap();
    assert!(
        page.items
            .iter()
            .any(|lock| lock.until == UniqueUntil::Expired)
    );
}

#[tokio::test]
async fn post_jobs_refuses_misspelled_unique_settings() {
    // Round-3 web-api-security-4: `unique` was read like a stored job, leniently: a misspelled
    // policy became `success` (released when the job succeeds: `"expire"` meant at most one
    // per window, a second push minutes later ran the side effect twice) and unknown keys were
    // ignored (`acrossQueues` gave per-queue locks). Requests are checked strictly: 422, nothing
    // pushed, no lock taken.
    let (_redis, api, router) = server(&["--api"]).await;
    for (unique, named) in [
        (json!({"for": 60, "until": "expire"}), "expire"),
        (json!({"for": 60, "until": "Start"}), "Start"),
        (json!({"for": 60, "until": "start "}), "start "),
        (json!({"for": 60, "until": 3}), "until"),
        (json!({"for": 60, "untill": "expired"}), "untill"),
        (json!({"for": 60, "acrossQueues": true}), "acrossQueues"),
        (json!({"for": 60, "ttl": 5}), "ttl"),
    ] {
        let res = post_job(
            &router,
            &json!({"class": "Sync", "args": [1], "unique": unique}),
        )
        .await;
        assert_eq!(
            res.status,
            StatusCode::UNPROCESSABLE_ENTITY,
            "{unique}: {}",
            res.body
        );
        let error = res.json();
        let message = error["error"].as_str().unwrap();
        assert!(message.contains(named), "{unique}: {message}");
    }
    assert_eq!(api.queue("default").size().await.unwrap(), 0);
    assert_eq!(api.unique_locks().size().await.unwrap(), 0);
    // The documented keys still work, `null` for an unset one, and `unique: null` is no
    // uniqueness at all.
    post_job(
        &router,
        &json!({"class": "Sync", "args": [1], "queue": "q1",
                "unique": {"for": 60, "until": null, "across_queues": true, "key": "k"}}),
    )
    .await
    .expect(StatusCode::CREATED);
    let duplicate = post_job(
        &router,
        &json!({"class": "Sync", "args": [2], "queue": "q2",
                "unique": {"for": 60, "across_queues": true, "key": "k"}}),
    )
    .await
    .expect(StatusCode::OK);
    assert!(duplicate["duplicate_of"].is_string(), "{duplicate}");
    for _ in 0..2 {
        post_job(
            &router,
            &json!({"class": "Sync", "args": [3], "unique": null}),
        )
        .await
        .expect(StatusCode::CREATED);
    }
}
