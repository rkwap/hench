//! `hench batches`, `hench stats batches` and the JSON API's `/batches` routes against a private
//! Redis (`docs/design/batches.md` §2.13, §3.6 S1).

use axum::Router;
use axum::body::Body;
use clap::Parser;
use hench::Client;
use hench::api::Api;
use hench::batch::Batch;
use hench::test_util::TestRedis;
use hench::{Job, JobContext, JobError, JobExt};
use hench_server::cli::{Cli, Command};
use hench_server::server::{ServerConfig, app};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

/// A member (never run here: the tests read and change batches, workers are not needed).
struct Member;

impl Job for Member {
    const NAME: &'static str = "cli::Member";
    type Args = (u32,);

    async fn perform(&self, _ctx: &JobContext, _args: Self::Args) -> Result<(), JobError> {
        Ok(())
    }
}

/// A batch of `n` members, described by `description`.
async fn batch(client: &Client, description: &str, n: u32) -> String {
    Batch::new(client)
        .description(description)
        .on_success::<Member>((0,))
        .jobs(|jobs| async move {
            for i in 0..n {
                Member::perform_async(&jobs, (i,)).await?;
            }
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap()
        .bid
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
async fn the_list_and_a_batch_are_printed() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let empty = hench(&redis, &["batches"]).await.unwrap();
    assert!(empty.starts_with("No batch is in this state."), "{empty}");
    let hostile = batch(&client, "Import \u{1b}[31m#7\nforged row", 3).await;
    let done = batch(&client, "empty", 0).await;

    let out = hench(&redis, &["batches"]).await.unwrap();
    let lines: Vec<&str> = out.lines().collect();
    assert!(
        lines[0].starts_with("BID") && lines[0].ends_with("DESCRIPTION"),
        "{out}"
    );
    assert!(lines[1].starts_with(&hostile), "{out}");
    assert!(lines[1].contains("pending"), "{out}");
    // Control characters are spelled out: no escape sequence, no forged line.
    assert!(
        lines[1].contains("Import \\u{1b}[31m#7\\nforged row"),
        "{out}"
    );
    assert!(!out.contains('\u{1b}'));
    assert!(
        !out.contains(&done),
        "succeeded batches are not active: {out}"
    );
    assert!(out.contains("Last hour: 2 created"), "{out}");
    let all = hench(&redis, &["batches", "list", "--state", "all"])
        .await
        .unwrap();
    assert!(all.contains(&done) && all.contains(&hostile), "{all}");
    let succeeded = hench(&redis, &["batches", "--state", "succeeded", "--json"])
        .await
        .unwrap();
    let json: Value = serde_json::from_str(&succeeded).unwrap();
    assert_eq!(json["total"], 1);
    assert_eq!(json["batches"][0]["bid"], done.as_str());
    assert_eq!(json["batches"][0]["state"], "succeeded");
    assert!(json["batches"][0]["created_at"].as_f64().unwrap() > 1.7e9);
    assert_eq!(json["last_hour"]["created"], 2);
    let filtered = hench(&redis, &["batches", "--state", "all", "--substr", "emp*"])
        .await
        .unwrap();
    assert!(
        filtered.contains(&done) && !filtered.contains(&hostile),
        "{filtered}"
    );

    // One batch.
    let show = hench(&redis, &["batches", "show", &hostile]).await.unwrap();
    assert!(show.contains("State") && show.contains("pending"), "{show}");
    assert!(show.contains("Total") && show.contains('3'), "{show}");
    assert!(
        show.contains("Description  Import \\u{1b}[31m#7\\nforged row"),
        "{show}"
    );
    assert!(
        show.contains("success") && show.contains("cli::Member"),
        "{show}"
    );
    assert!(show.contains("Pending jobs (first 10)"), "{show}");
    let json: Value = serde_json::from_str(
        &hench(&redis, &["batches", "show", &hostile, "--json"])
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(json["total"], 3);
    assert_eq!(json["callbacks"][0]["event"], "success");
    assert_eq!(json["pending_jids"].as_array().unwrap().len(), 3);
    // Missing and malformed bids.
    let missing = hench(&redis, &["batches", "show", "0123456789abcdef01234567"])
        .await
        .unwrap_err()
        .to_string();
    assert!(missing.contains("does not exist"), "{missing}");
    let malformed = hench(&redis, &["batches", "show", "../x"])
        .await
        .unwrap_err()
        .to_string();
    assert!(malformed.contains("is not a batch id"), "{malformed}");
}

#[tokio::test]
async fn show_spells_out_jids_and_says_when_the_batch_expires() {
    // Round-3 web-api-security-6: `hench batches show` printed the jids of failed and pending
    // members raw, so a job whose jid holds escape sequences (application-chosen jids) wrote to
    // the operator's terminal, and a CR/LF forged lines. Round-3 web-api-security-10: every
    // time was "ago", so the expiry (always ahead for a live batch) read "(just now)".
    const ESC: &str = "m-\u{1b}]0;PWNED\u{7}\u{1b}[31mRED\u{1b}[0m";
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let api = Api::new(redis.pool().await);
    let bid = Batch::new(&client)
        .description("hostile jids")
        .jobs(|jobs| async move {
            for jid in [
                format!("{ESC}-failed"),
                format!("{ESC}-pending"),
                "m-crlf\r\nFORGED ROW".to_owned(),
            ] {
                Member::set()
                    .jid(jid)
                    .queue("parked")
                    .perform_async(&jobs, (1,))
                    .await?;
            }
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap()
        .bid;
    // One member deleted from its queue: dead in its batch (and pending until skipped).
    let failed = api
        .queue("parked")
        .find_job(&format!("{ESC}-failed"))
        .await
        .unwrap()
        .unwrap();
    assert!(failed.delete().await.unwrap());
    let status = api.batches().fetch(&bid).await.unwrap().unwrap();
    assert_eq!((status.failures, status.dead, status.pending), (1, 1, 3));

    let out = hench(&redis, &["batches", "show", &bid]).await.unwrap();
    assert!(!out.contains('\u{1b}'), "a raw escape sequence: {out:?}");
    assert!(!out.contains('\u{7}') && !out.contains('\r'), "{out:?}");
    assert!(
        !out.lines().any(|line| line.starts_with("FORGED ROW")),
        "a forged line: {out}"
    );
    // Spelled out instead, in the failures table and the pending jobs.
    let spelled = "m-\\u{1b}]0;PWNED\\u{7}\\u{1b}[31mRED\\u{1b}[0m";
    assert!(
        out.lines()
            .any(|line| line.starts_with(&format!("{spelled}-failed"))),
        "{out}"
    );
    let pending = out
        .lines()
        .find(|line| line.starts_with("Pending jobs"))
        .unwrap();
    assert!(pending.contains(&format!("{spelled}-pending")), "{pending}");
    assert!(pending.contains("m-crlf\\r\\nFORGED ROW"), "{pending}");
    // Times ahead read as such: the batch expires in 30 days (its default time to live).
    let expires = out
        .lines()
        .find(|line| line.starts_with("Expires"))
        .unwrap();
    assert!(expires.ends_with("(in 29 days)"), "{expires}");
    let created = out
        .lines()
        .find(|line| line.starts_with("Created"))
        .unwrap();
    assert!(created.ends_with("(just now)"), "{created}");
}

#[tokio::test]
async fn batches_are_cancelled_skipped_dispatched_and_deleted() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let api = Api::new(redis.pool().await);
    let bid = batch(&client, "cancel me", 2).await;
    let refused = hench(&redis, &["batches", "cancel", &bid])
        .await
        .unwrap_err()
        .to_string();
    assert!(refused.contains("--yes"), "{refused}");
    assert!(
        api.batches()
            .fetch(&bid)
            .await
            .unwrap()
            .unwrap()
            .cancelled_at
            .is_none()
    );
    let out = hench(&redis, &["batches", "cancel", &bid, "--yes"])
        .await
        .unwrap();
    assert_eq!(out, format!("Cancelled batch {bid}.\n"));
    let again = hench(&redis, &["batches", "cancel", &bid, "--yes"])
        .await
        .unwrap();
    assert_eq!(again, format!("Batch {bid} was already cancelled.\n"));

    // Skip: a pending member, then one that no longer is.
    let other = batch(&client, "skip", 1).await;
    let jid = api
        .batches()
        .pending_jids(&other, 1)
        .await
        .unwrap()
        .remove(0);
    let out = hench(&redis, &["batches", "skip", &other, &jid])
        .await
        .unwrap();
    assert_eq!(out, format!("Skipped job {jid} of batch {other}.\n"));
    let out = hench(&redis, &["batches", "skip", &other, &jid])
        .await
        .unwrap();
    assert!(out.contains("is not pending"), "{out}");
    // Its events fired and were dispatched by the skip.
    let out = hench(&redis, &["batches", "dispatch", &other])
        .await
        .unwrap();
    assert!(out.contains("no callbacks waiting"), "{out}");
    // A succeeded batch cannot be cancelled.
    let refused = hench(&redis, &["batches", "cancel", &other, "--yes"])
        .await
        .unwrap_err()
        .to_string();
    assert!(refused.contains("already succeeded"), "{refused}");

    // Delete needs --yes; an unfinished child of a live parent needs --force.
    let tree = Batch::new(&client)
        .jobs(|jobs| async move {
            Member::perform_async(&jobs, (1,)).await?;
            Batch::new(&jobs)
                .jobs(|inner| async move {
                    Member::perform_async(&inner, (2,)).await?;
                    Ok::<_, hench::Error>(())
                })
                .await?;
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap();
    let child = tree.children[0].clone();
    assert!(
        hench(&redis, &["batches", "delete", &child])
            .await
            .unwrap_err()
            .to_string()
            .contains("--yes")
    );
    let refused = hench(&redis, &["batches", "delete", &child, "--yes"])
        .await
        .unwrap_err()
        .to_string();
    assert!(
        refused.contains(&tree.bid) && refused.contains("--force"),
        "{refused}"
    );
    let out = hench(&redis, &["batches", "delete", &child, "--yes", "--force"])
        .await
        .unwrap();
    assert_eq!(out, format!("Deleted batch {child}.\n"));
    let out = hench(&redis, &["batches", "delete", &tree.bid, "--yes"])
        .await
        .unwrap();
    assert_eq!(out, format!("Deleted batch {}.\n", tree.bid));
    let out = hench(&redis, &["batches", "delete", &tree.bid, "--yes"])
        .await
        .unwrap();
    assert!(out.contains("did not exist"), "{out}");
}

#[tokio::test]
async fn stats_print_the_batches_section() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    // No batch yet: `hench stats all` leaves the section out.
    let all = hench(&redis, &["stats", "all"]).await.unwrap();
    assert!(!all.contains("Batches:"), "{all}");
    batch(&client, "one", 2).await;
    batch(&client, "two", 0).await;
    let out = hench(&redis, &["stats", "batches"]).await.unwrap();
    assert!(
        out.starts_with("Batches: 2 (1 active, 0 with a dead job, 0 with callbacks waiting"),
        "{out}"
    );
    assert!(
        out.contains("Last hour: 2 created, 1 complete, 1 succeeded"),
        "{out}"
    );
    let json: Value = serde_json::from_str(
        &hench(&redis, &["stats", "batches", "--json"])
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(json["batches"], 2);
    assert_eq!(json["active"], 1);
    assert_eq!(json["undispatched"], 0);
    assert_eq!(json["last_hour"]["success"], 1);
    let all = hench(&redis, &["stats", "all"]).await.unwrap();
    assert!(all.contains("Batches: 2"), "{all}");
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

fn post(path: &str) -> Request<Body> {
    Request::post(path).body(Body::empty()).unwrap()
}

#[tokio::test]
async fn the_json_api_lists_shows_and_cancels_batches() {
    let redis = TestRedis::start().await;
    let client = redis.client().await;
    let pending = batch(&client, "import", 2).await;
    let done = batch(&client, "done", 0).await;
    let router = server(&redis, &[]).await;

    let (status, list) = send(&router, get("/api/v1/batches")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list["total"], 1);
    assert_eq!(list["batches"][0]["bid"], pending.as_str());
    assert_eq!(list["batches"][0]["state"], "pending");
    assert_eq!(list["batches"][0]["pending"], 2);
    let (_, all) = send(&router, get("/api/v1/batches?state=all&count=1&page=2")).await;
    assert_eq!(all["total"], 2);
    assert_eq!(all["page"], 2);
    assert_eq!(all["batches"].as_array().unwrap().len(), 1);
    let (_, found) = send(&router, get("/api/v1/batches?state=all&substr=don")).await;
    assert_eq!(found["batches"][0]["bid"], done.as_str());
    let (status, error) = send(&router, get("/api/v1/batches?state=finished")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(error["error"].as_str().unwrap().contains("state"));
    let (status, _) = send(&router, get("/api/v1/batches?page=many")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, one) = send(&router, get(&format!("/api/v1/batches/{pending}"))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(one["total"], 2);
    assert_eq!(one["callbacks"][0]["class"], "cli::Member");
    assert_eq!(one["failures"].as_array().unwrap().len(), 0);
    assert_eq!(one["failures_total"], 0);
    assert!(one["expires_at"].as_f64().unwrap() > 1.7e9);
    for path in [
        "/api/v1/batches/0123456789abcdef01234567",
        "/api/v1/batches/nope",
    ] {
        let (status, _) = send(&router, get(path)).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{path}");
    }

    // Cancel: done once, refused for a succeeded batch, 404 for a missing one.
    let (status, cancelled) =
        send(&router, post(&format!("/api/v1/batches/{pending}/cancel"))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(cancelled["cancelled"], 1);
    let (status, _) = send(&router, post(&format!("/api/v1/batches/{done}/cancel"))).await;
    assert_eq!(status, StatusCode::CONFLICT);
    let (status, _) = send(
        &router,
        post("/api/v1/batches/0123456789abcdef01234567/cancel"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // Read-only, and cross-site.
    let other = batch(&client, "guarded", 1).await;
    let read_only = server(&redis, &["--read-only"]).await;
    let (status, _) = send(&read_only, post(&format!("/api/v1/batches/{other}/cancel"))).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let mut cross = post(&format!("/api/v1/batches/{other}/cancel"));
    cross
        .headers_mut()
        .insert("sec-fetch-site", "cross-site".parse().unwrap());
    let (status, _) = send(&router, cross).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let api = Api::new(redis.pool().await);
    assert!(
        api.batches()
            .fetch(&other)
            .await
            .unwrap()
            .unwrap()
            .cancelled_at
            .is_none()
    );
}
