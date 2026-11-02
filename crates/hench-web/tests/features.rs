//! The pro features together in the dashboard (`docs/design/pro-features.md` §8: X16, X17,
//! X19): the navigation lists the four feature tabs in their fixed order and each switch removes
//! its tab and answers 404 on its paths; the page of a job carrying every feature shows the
//! feature rows in their fixed order (Batch, Periodic, Unique, Throttle, Limited, then the host's
//! rows) and its list row shows the feature chips; the metrics table shows the features'
//! outcome columns only when some class has a value; the Busy page names the features a live
//! process lacks. Each tab's own pages are tested in `tests/{batches,limits}.rs` and in the
//! page modules (Periodic, Locks).

mod common;

use std::time::Duration;

use common::*;
use hench::metrics::{Outcome, Tracker};
use hench_web::JobInfoRow;
use http::StatusCode;
use serde_json::{Value, json};

/// Every built-in tab, in order.
const TABS: [&str; 11] = [
    "Dashboard",
    "Busy",
    "Queues",
    "Retries",
    "Scheduled",
    "Dead",
    "Periodic",
    "Batches",
    "Limits",
    "Locks",
    "Metrics",
];

/// The feature tabs: name, path, and the switch turning it off.
type Switch = fn(hench_web::HenchWeb) -> hench_web::HenchWeb;
const FEATURES: [(&str, &str, Switch); 4] = [
    ("Periodic", "/periodic", |web| web.periodic(false)),
    ("Batches", "/batches", |web| web.batches(false)),
    ("Limits", "/limits", |web| web.limits(false)),
    ("Locks", "/locks", |web| web.locks(false)),
];

#[tokio::test]
async fn the_feature_tabs_follow_the_job_sets_and_each_switch_removes_its_tab() {
    let app = App::new().await;
    let res = app.get("/").await;
    res.assert_status(StatusCode::OK);
    assert_eq!(res.texts(".nav a"), TABS);
    for (name, path, _) in FEATURES {
        let page = app.get(path).await;
        page.assert_status(StatusCode::OK);
        assert_eq!(page.texts(".nav [aria-current=page]"), [name], "{path}");
    }
    for (name, path, off) in FEATURES {
        let app = App::build(off, "").await;
        let tabs = app.get("/").await.texts(".nav a");
        let expected: Vec<&str> = TABS.into_iter().filter(|tab| *tab != name).collect();
        assert_eq!(tabs, expected, "{name} off");
        app.get(path).await.assert_status(StatusCode::NOT_FOUND);
        app.get(&format!("{path}/x"))
            .await
            .assert_status(StatusCode::NOT_FOUND);
        // The other feature tabs stay.
        for (_, other, _) in FEATURES.iter().filter(|(other, ..)| *other != name) {
            app.get(other).await.assert_status(StatusCode::OK);
        }
    }
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
                "key" => "1700000000.5-0123456789abcdef01234567",
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
async fn every_route_the_crate_documents_is_served() {
    // release-surface-8: the crate documentation's list of routes left out the feature tabs'
    // nine routes (and the four switches that turn them off). Every path it lists now is
    // served with the method it names (`POST` for an action, `HEAD` for the health check,
    // otherwise `GET`): none falls to the dashboard's 404 page or answers 405.
    let routes: Vec<&str> = include_str!("../src/lib.rs")
        .lines()
        .skip_while(|line| *line != "//! ## Routes")
        .map_while(|line| line.strip_prefix("//!"))
        .collect();
    let routes = routes.join("\n");
    let mut documented = Vec::new();
    for span in routes.split('`').skip(1).step_by(2) {
        let (method, path) = match span.split_once(' ') {
            Some((method @ ("POST" | "HEAD"), path)) => (method, path),
            _ => ("GET", span),
        };
        if path.starts_with('/') {
            documented.push((method, path));
        }
    }
    for (_, path, _) in FEATURES {
        assert!(documented.contains(&("GET", path)), "{path} is not listed");
    }
    let app = App::new().await;
    for (method, path) in &documented {
        for path in expand(path) {
            let res = match *method {
                "POST" => app.post(&path, &[]).await,
                "HEAD" => {
                    let request = http::Request::head(app.path(&path))
                        .body(axum::body::Body::empty())
                        .unwrap();
                    app.send(request).await
                }
                _ => app.get(&path).await,
            };
            assert_ne!(
                res.status,
                StatusCode::METHOD_NOT_ALLOWED,
                "{method} {path}"
            );
            // The text of the dashboard's 404 page for unknown paths.
            assert!(
                !res.body.contains("This page does not exist."),
                "{method} {path} is not served"
            );
        }
    }
    assert!(documented.len() >= 30, "{documented:?}");
}

#[tokio::test]
async fn a_host_route_can_take_a_tab_s_path_only_with_the_tab_off() {
    // What `HenchWeb::routes` says: building the router panics (axum's merge) when a route of
    // the host's takes a path the dashboard serves, a tab's while the tab is on; with the tab
    // off, the path is the host's.
    let api = hench::api::Api::new(hench::RedisPool::detached());
    for (name, path, off) in FEATURES {
        let mine = || axum::Router::new().route(path, axum::routing::get(|| async { "mine" }));
        let on = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            hench_web::router(hench_web::HenchWeb::new(&api).routes(mine()))
        }));
        assert!(on.is_err(), "{name} on: {path} was taken twice");
        let router = hench_web::router(off(hench_web::HenchWeb::new(&api)).routes(mine()));
        let response = tower::ServiceExt::oneshot(
            router,
            http::Request::get(path)
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{name} off");
        let body = http_body_util::BodyExt::collect(response.into_body())
            .await
            .unwrap()
            .to_bytes();
        assert_eq!(&body[..], b"mine", "{name} off");
    }
}

/// A job carrying every feature (X15's job, as stored while parked over a limit).
fn featured_job(jid: &str) -> Value {
    json!({
        "class": "Featured",
        "args": [1],
        "jid": jid,
        "queue": "default",
        "retry": true,
        "created_at": 1_730_000_000_000_i64,
        "bid": "0123456789abcdef01234567",
        "periodic": {"name": "nightly", "tick": 1_730_000_000_000_i64},
        "unique": {"for": 600, "until": "start", "digest": "rP3m"},
        "throttle": {"name": "stripe", "key": "42"},
        "limited": {"n": 3, "by": "w:stripe:42", "at": 1_730_000_000_000_i64},
    })
}

/// The labels of a job page's detail rows, in order.
fn row_labels(res: &Res) -> Vec<String> {
    res.texts("table.kv tr th")
}

#[tokio::test]
async fn a_job_carrying_every_feature_shows_its_rows_in_order_and_its_chips() {
    let app = App::build(
        |web| web.job_info(|_| vec![JobInfoRow::text("Tenant", "acme")]),
        "",
    )
    .await;
    let jid = "aaaaaaaaaaaaaaaaaaaaaaaa";
    let (job, score) = add_to_set(&app, "schedule", featured_job(jid), now_secs() + 600.0).await;
    let list = app.get("/scheduled").await;
    list.assert_status(StatusCode::OK);
    assert_eq!(
        list.texts("td .chip-muted"),
        [
            "unique",
            "throttle: stripe:42",
            "over limit: w:stripe:42 ×3"
        ]
    );
    let page = app.get(&format!("/scheduled/{}", key(&job, score))).await;
    page.assert_status(StatusCode::OK);
    let labels = row_labels(&page);
    let position = |label: &str| {
        labels
            .iter()
            .position(|text| text == label)
            .unwrap_or_else(|| panic!("no {label} row in {labels:?}"))
    };
    let order: Vec<usize> = [
        "Batch", "Periodic", "Unique", "Throttle", "Limited", "Tenant",
    ]
    .into_iter()
    .map(position)
    .collect();
    assert!(order.windows(2).all(|pair| pair[0] < pair[1]), "{labels:?}");
    // The built-in rows come first, and the feature fields are not repeated among the extras.
    assert!(position("JID") < order[0], "{labels:?}");
    assert!(
        !labels.iter().any(|label| label.starts_with("Extra")),
        "{labels:?}"
    );
    // Each row links to its tab's page.
    let links = page.attrs("table.kv a", "href");
    for expected in [
        "/batches/0123456789abcdef01234567",
        "/periodic/nightly",
        "/locks/rP3m?jid=aaaaaaaaaaaaaaaaaaaaaaaa",
        "/limits?substr=stripe",
        "/limits/w/stripe:42",
    ] {
        assert!(
            links.iter().any(|href| href == expected),
            "{expected} in {links:?}"
        );
    }
    // Deleting a batch member counts it dead in its batch: the confirmation says so.
    let confirm = page.attrs("button[name=delete]", "data-confirm").join("");
    assert!(confirm.contains("0123456789abcdef01234567"), "{confirm}");

    // With the tabs off, the rows stay, as text.
    let app = App::build(
        |web| {
            web.periodic(false)
                .batches(false)
                .limits(false)
                .locks(false)
        },
        "",
    )
    .await;
    let (job, score) = add_to_set(&app, "schedule", featured_job(jid), now_secs() + 600.0).await;
    let page = app.get(&format!("/scheduled/{}", key(&job, score))).await;
    page.assert_status(StatusCode::OK);
    for label in ["Batch", "Periodic", "Unique", "Throttle", "Limited"] {
        assert!(
            row_labels(&page).iter().any(|text| text == label),
            "{label}"
        );
    }
    let links = page.attrs("table.kv td a", "href");
    assert!(
        !links
            .iter()
            .any(|href| ["/batches", "/periodic", "/locks", "/limits"]
                .iter()
                .any(|tab| href.starts_with(tab))),
        "{links:?}"
    );
}

#[tokio::test]
async fn the_metrics_table_shows_the_feature_outcomes_only_when_some_class_has_them() {
    let app = App::new().await;
    let tracker = Tracker::new();
    tracker.track("Plain", Duration::from_millis(20), Outcome::Success);
    tracker.flush(&app.pool).await.unwrap();
    let res = app.get("/metrics").await;
    res.assert_status(StatusCode::OK);
    let headers = res.texts(".metrics-table thead th");
    for column in ["Over limit", "Throttled", "Skipped"] {
        assert!(
            !headers.iter().any(|header| header == column),
            "{headers:?}"
        );
    }

    // One class over a limit, throttled and skipped: those three columns appear, after the
    // failures, with zeros for the other classes.
    for outcome in [
        Outcome::Success,
        Outcome::OverLimit,
        Outcome::OverLimit,
        Outcome::Throttled,
        Outcome::Skipped,
        Outcome::Failure,
    ] {
        tracker.track("Featured", Duration::from_millis(30), outcome);
    }
    tracker.flush(&app.pool).await.unwrap();
    let res = app.get("/metrics").await;
    let headers = res.texts(".metrics-table thead th");
    let failed = headers.iter().position(|header| header.contains("Failed"));
    let over = headers.iter().position(|header| header == "Over limit");
    assert!(
        failed.is_some() && over == failed.map(|index| index + 1),
        "{headers:?}"
    );
    assert!(headers.iter().any(|header| header == "Throttled"));
    assert!(headers.iter().any(|header| header == "Skipped"));
    // Each row has a cell per column: the plain class shows zeros in the feature columns.
    for row in res
        .html()
        .select(&scraper::Selector::parse(".metrics-table tbody tr").unwrap())
    {
        let cells = row.select(&scraper::Selector::parse("td").unwrap()).count();
        assert_eq!(cells, headers.len() - 1, "{}", row.html());
    }
    let page = app.get("/metrics/Featured").await;
    page.assert_status(StatusCode::OK);
    let facts = page.texts(".facts dt, .facts th");
    for fact in ["Over limit", "Throttled", "Skipped"] {
        assert!(facts.iter().any(|text| text == fact), "{fact} in {facts:?}");
    }
}

#[tokio::test]
async fn the_busy_page_names_the_features_a_live_process_lacks() {
    let app = App::new().await;
    let old = json!({"hostname": "old", "pid": 1, "identity": "old:1:a", "concurrency": 5,
        "queues": ["default"], "features": {"unique": 1, "periodic": 1, "limits": 1}});
    let new = json!({"hostname": "new", "pid": 2, "identity": "new:2:b", "concurrency": 5,
        "queues": ["default"], "features": {"unique": 1, "periodic": 1, "batches": 1,
        "limits": 1}});
    add_process(&app, "old:1:a", old, 0).await;
    add_process(&app, "new:2:b", new, 0).await;
    let busy = app.get("/busy").await;
    busy.assert_status(StatusCode::OK);
    assert!(!busy.body.contains("lacks"), "nothing in use yet");
    // A batch exists: the process without the batches format lacks it.
    let _: i64 = app
        .redis(
            hench::redis_rs::cmd("ZADD")
                .arg("batches")
                .arg(now_secs() + 3_600.0)
                .arg("0123456789abcdef01234567"),
        )
        .await;
    let busy = app.get("/busy").await;
    let chips: Vec<String> = busy
        .texts(".chip")
        .into_iter()
        .filter(|chip| chip.starts_with("lacks"))
        .collect();
    assert_eq!(chips, ["lacks: batches"], "{}", busy.body);
}
