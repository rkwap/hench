//! The metrics pages: Sidekiq's `web_test` "Metrics" scenarios (the page renders, filtering,
//! the per-job page and its periods), plus sorting, chart data, the per-job figures and
//! charts, deploy marks, escaping, mounting, outages, turning the pages off, and metrics
//! written by live workers.

mod common;

use std::future::Future;
use std::time::Duration;

use common::*;
use hench::metrics::{Deploy, Outcome, Tracker};
use hench::prelude::*;
use http::StatusCode;
use serde_json::Value;

/// Records executions `(class, milliseconds, outcome)` and flushes them to Redis now.
async fn track(app: &App, runs: &[(&str, u64, Outcome)]) {
    let tracker = Tracker::new();
    for (class, ms, outcome) in runs {
        tracker.track(class, Duration::from_millis(*ms), *outcome);
    }
    tracker.flush(&app.pool).await.expect("metrics flush");
}

/// Sidekiq's setup: one `MikeJob` execution of about 20 ms.
async fn track_mike(app: &App) {
    track(app, &[("MikeJob", 20, Outcome::Success)]).await;
}

/// The `data-chart` JSON of the first element matching `selector`.
fn chart(res: &Res, selector: &str) -> Value {
    let raw = res.attrs(selector, "data-chart");
    serde_json::from_str(raw.first().expect("a chart")).expect("chart JSON")
}

/// The job classes of the metrics table, in order.
fn classes(res: &Res) -> Vec<String> {
    res.texts(".metrics-table tbody .job-class")
}

#[tokio::test]
async fn renders_the_metrics_page() {
    let app = App::new().await;
    track_mike(&app).await;
    let res = app.get("/metrics?period=4h").await;
    res.assert_status(StatusCode::OK);
    assert_eq!(res.texts("h1"), ["Metrics"]);
    assert_eq!(res.title(), "Metrics · Hench");
    assert_eq!(classes(&res), ["MikeJob"]);
    assert_eq!(res.texts(".segmented a[aria-current=true]"), ["4h"]);
    assert_eq!(
        res.texts(".nav a[aria-current=page]"),
        ["Metrics"],
        "the Metrics tab is the active one"
    );
    assert_eq!(
        res.texts(".filter-summary"),
        ["1 job class ran in the last 4 hours."]
    );
    // The totals strip, like every page.
    assert_eq!(res.count("#totals"), 1);
    // The chart's script is linked; no inline script anywhere.
    assert_eq!(res.count("script[src*='/assets/metrics.']"), 1);
    assert_eq!(res.count("script:not([src])"), 0);
}

#[tokio::test]
async fn filters_job_classes() {
    let app = App::new().await;
    track_mike(&app).await;
    track(&app, &[("OtherJob", 5, Outcome::Success)]).await;

    // Case-insensitive substring of the class.
    let res = app.get("/metrics?substr=mike").await;
    res.assert_status(StatusCode::OK);
    assert_eq!(classes(&res), ["MikeJob"]);
    assert_eq!(
        res.texts(".filter-summary"),
        ["1 job class matching “mike” ran in the last hour."]
    );
    assert_eq!(res.attrs("#filter-input", "value"), ["mike"]);

    let res = app.get("/metrics?substr=notfound").await;
    res.assert_status(StatusCode::OK);
    assert!(!res.body.contains("MikeJob"));
    assert_eq!(
        res.texts(".empty"),
        ["No job class matches this filter in this period."]
    );
    assert_eq!(res.count("#metrics-chart"), 0);

    // The period and the order survive filtering (hidden fields), and the period links keep
    // the filter.
    let res = app.get("/metrics?period=8h&sort=count&substr=job").await;
    assert_eq!(
        res.attrs(".filter-form input[name=period]", "value"),
        ["8h"]
    );
    assert_eq!(
        res.attrs(".filter-form input[name=sort]", "value"),
        ["count"]
    );
    let links = res.attrs(".segmented a", "href");
    assert!(
        links.contains(&"/metrics?period=2h&sort=count&substr=job".to_owned()),
        "{links:?}"
    );
    assert!(
        links.contains(&"/metrics?sort=count&substr=job".to_owned()),
        "{links:?}"
    );
    // Clearing the filter keeps the rest.
    assert_eq!(
        res.attrs(".filter-form a.btn", "href"),
        ["/metrics?period=8h&sort=count"]
    );
}

#[tokio::test]
async fn job_page_offers_minute_periods() {
    let app = App::new().await;
    track(&app, &[("FooJob", 30, Outcome::Success)]).await;
    let res = app.get("/metrics/FooJob?period=2h").await;
    res.assert_status(StatusCode::OK);
    assert!(res.body.contains("Metrics"));
    assert_eq!(res.texts("h1 .job-class"), ["FooJob"]);
    assert_eq!(res.title(), "Metrics of “FooJob” · Hench");
    // Histograms are kept per minute: 8 hours at most.
    assert_eq!(res.texts(".segmented a"), ["1h", "2h", "4h", "8h"]);
    assert!(!res.body.contains(">24h<"));
    assert_eq!(res.texts(".segmented a[aria-current=true]"), ["2h"]);
    // The breadcrumb goes back to the list with the same period.
    assert_eq!(res.attrs("h1 a.crumb", "href"), ["/metrics?period=2h"]);
    assert_eq!(
        chart(&res, "#job-series-chart")["buckets"]
            .as_array()
            .unwrap()
            .len(),
        120
    );

    // Longer periods (from the list's links) show 8 hours and say why.
    let res = app.get("/metrics/FooJob?period=24h").await;
    res.assert_status(StatusCode::OK);
    assert_eq!(res.texts(".segmented a[aria-current=true]"), ["8h"]);
    assert!(res.texts(".filter-summary").concat().contains("8 hours"));
    assert_eq!(res.attrs("h1 a.crumb", "href"), ["/metrics?period=24h"]);
    assert_eq!(
        chart(&res, "#job-series-chart")["buckets"]
            .as_array()
            .unwrap()
            .len(),
        480
    );
}

#[tokio::test]
async fn bad_parameters_are_refused() {
    let app = App::new().await;
    for path in [
        "/metrics?period=3h",
        "/metrics?period=1H",
        "/metrics?sort=name",
        "/metrics/FooJob?period=forever",
    ] {
        let res = app.get(path).await;
        assert_eq!(res.status, StatusCode::BAD_REQUEST, "{path}");
    }
    let long = format!("/metrics?substr={}", "x".repeat(257));
    app.get(&long).await.assert_status(StatusCode::BAD_REQUEST);
    // A class name too long to be one.
    let path = format!("/metrics/{}", "x".repeat(1_025));
    app.get(&path).await.assert_status(StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn sorts_by_each_column() {
    let app = App::new().await;
    track(
        &app,
        &[
            // A: 3 executions, 1 failure, 20 ms in all.
            ("A", 10, Outcome::Success),
            ("A", 10, Outcome::Success),
            ("A", 0, Outcome::Failure),
            // B: 1 execution of 500 ms.
            ("B", 500, Outcome::Success),
            // C: 4 executions, 2 failures, 2 ms in all.
            ("C", 1, Outcome::Success),
            ("C", 1, Outcome::Success),
            ("C", 0, Outcome::Failure),
            ("C", 0, Outcome::Failure),
        ],
    )
    .await;
    for (sort, order, column) in [
        ("", ["B", "A", "C"], "Total time (s)"),
        ("time", ["B", "A", "C"], "Total time (s)"),
        ("count", ["C", "A", "B"], "Executions"),
        ("failures", ["C", "A", "B"], "Failed"),
        ("average", ["B", "A", "C"], "Average (ms)"),
    ] {
        let path = if sort.is_empty() {
            "/metrics".to_owned()
        } else {
            format!("/metrics?sort={sort}")
        };
        let res = app.get(&path).await;
        res.assert_status(StatusCode::OK);
        assert_eq!(classes(&res), order, "sort={sort}");
        assert_eq!(
            res.texts("th[aria-sort=descending]"),
            [format!("Sort by {column}")],
            "sort={sort}"
        );
        // The chart lists the classes in the table's order.
        let names: Vec<String> = chart(&res, "#metrics-chart")["series"]
            .as_array()
            .unwrap()
            .iter()
            .map(|series| series["name"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(names, order, "sort={sort}");
    }
    // The figures of each row.
    let res = app.get("/metrics").await;
    let rows = res.texts(".metrics-table tbody tr");
    assert_eq!(rows.len(), 3);
    let cells = |row: usize| -> Vec<String> {
        res.texts(&format!(
            ".metrics-table tbody tr:nth-child({}) td",
            row + 1
        ))
    };
    assert_eq!(cells(0), ["1", "0", "0.50", "500.0"]);
    assert_eq!(cells(1), ["3", "1", "0.02", "10.0"]);
    assert_eq!(cells(2), ["4", "2", "0.00", "1.0"]);
    // Column links keep the period.
    let res = app.get("/metrics?period=2h").await;
    let links = res.attrs("th a.sort-link", "href");
    assert_eq!(
        links,
        [
            "/metrics?period=2h&sort=count",
            "/metrics?period=2h&sort=failures",
            "/metrics?period=2h",
            "/metrics?period=2h&sort=average",
        ]
    );
}

#[tokio::test]
async fn chart_data_follows_the_period() {
    let app = App::new().await;
    let mut runs = Vec::new();
    for index in 0..7 {
        runs.push((format!("Job{index}"), 100 * (index + 1), Outcome::Success));
    }
    let runs: Vec<(&str, u64, Outcome)> = runs
        .iter()
        .map(|(class, ms, outcome)| (class.as_str(), *ms, *outcome))
        .collect();
    track(&app, &runs).await;

    let res = app.get("/metrics").await;
    let data = chart(&res, "#metrics-chart");
    assert_eq!(data["granularity"], "minute");
    assert_eq!(data["buckets"].as_array().unwrap().len(), 60);
    assert_eq!(data["visible"], 5);
    let series = data["series"].as_array().unwrap();
    assert_eq!(series.len(), 7);
    // Busiest first; each series sums to the class's total seconds.
    assert_eq!(series[0]["name"], "Job6");
    let total: f64 = series[0]["values"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_f64().unwrap())
        .sum();
    assert!((total - 0.7).abs() < 1e-9, "{total}");
    // The five busiest are ticked for the chart; the others can be.
    let ticked = res.attrs("[data-series-toggle][checked]", "value");
    assert_eq!(ticked, ["0", "1", "2", "3", "4"]);
    assert_eq!(res.count("[data-series-toggle]"), 7);
    // The chart's table view: the ticked classes per minute.
    assert_eq!(res.texts("details.table-view thead th").len(), 6);
    let row = res.texts("details.table-view tbody tr td");
    assert_eq!(row, ["0.700", "0.600", "0.500", "0.400", "0.300"]);
    // Pages of the last hour refresh themselves; the others do not; none has the "Live"
    // switch of the list pages (Sidekiq's metrics pages are not pollable either).
    assert_eq!(res.attrs("#metrics-root", "data-auto-refresh"), ["60"]);
    assert_eq!(res.count("body[data-live-page]"), 0);
    assert_eq!(res.count("#live-toggle"), 0);

    let res = app.get("/metrics?period=24h").await;
    let data = chart(&res, "#metrics-chart");
    assert_eq!(data["granularity"], "ten_minutes");
    assert_eq!(data["buckets"].as_array().unwrap().len(), 144);
    assert_eq!(res.count("#metrics-root[data-auto-refresh]"), 0);
    assert_eq!(
        res.texts(".chart-title"),
        ["Execution time per 10 minutes (s)"]
    );
    // The job links carry the period.
    assert!(
        res.attrs(".metrics-table a.job-class", "href")
            .contains(&"/metrics/Job0?period=24h".to_owned())
    );
}

#[tokio::test]
async fn shows_only_the_busiest_classes() {
    let app = App::new().await;
    let names: Vec<String> = (0..30).map(|index| format!("Class{index:02}")).collect();
    let runs: Vec<(&str, u64, Outcome)> = names
        .iter()
        .enumerate()
        .map(|(index, name)| (name.as_str(), 10 + index as u64, Outcome::Success))
        .collect();
    track(&app, &runs).await;
    let res = app.get("/metrics").await;
    assert_eq!(classes(&res).len(), 25);
    assert_eq!(classes(&res)[0], "Class29");
    assert!(
        res.body
            .contains("Showing the first 25 of 30 job classes; filter by name to find the others.")
    );
    assert_eq!(
        res.texts(".filter-summary"),
        ["30 job classes ran in the last hour."]
    );
    // A filter finds the others.
    let res = app.get("/metrics?substr=class0").await;
    assert_eq!(classes(&res).len(), 10);
}

#[tokio::test]
async fn job_page_summarizes_executions() {
    let app = App::new().await;
    let mut runs = vec![("ImportJob", 10, Outcome::Success); 6];
    runs.extend([("ImportJob", 25, Outcome::Success); 3]);
    runs.push(("ImportJob", 1_215, Outcome::Success));
    runs.extend([("ImportJob", 0, Outcome::Failure); 2]);
    track(&app, &runs).await;

    let res = app.get("/metrics/ImportJob").await;
    res.assert_status(StatusCode::OK);
    let facts: Vec<String> = res.texts("#job-facts .fact dd");
    assert_eq!(
        facts,
        ["12", "2", "17%", "1.35 s", "135.0 ms", "< 20 ms", "< 1.7 s"]
    );
    assert_eq!(
        res.texts("#job-facts .fact dt"),
        [
            "Executions",
            "Failed",
            "Failure rate",
            "Total time",
            "Average",
            "Median",
            "95th percentile"
        ]
    );

    // Executions and failures per minute.
    let series = chart(&res, "#job-series-chart");
    let sum = |index: usize| -> u64 {
        series["series"][index]["values"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_u64().unwrap())
            .sum()
    };
    assert_eq!((sum(0), sum(1)), (12, 2));
    assert_eq!(series["series"][0]["name"], "Executions");

    // The distribution: buckets from the fastest used to one past the slowest.
    let histogram = chart(&res, "#histogram-chart");
    let labels = histogram["labels"].as_array().unwrap();
    assert_eq!(labels.first().unwrap(), "<20ms");
    assert_eq!(labels.last().unwrap(), "<2.5s");
    assert_eq!(labels.len(), 13);
    let counts: Vec<u64> = histogram["counts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|count| count.as_u64().unwrap())
        .collect();
    assert_eq!(counts, [6, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0]);
    assert_eq!(histogram["ranges"][11], "1.1 s – 1.7 s");
    let table = res.texts("section[aria-labelledby=distribution-heading] tbody tr");
    assert_eq!(table.len(), 13);
    assert!(table[0].starts_with("under 20 ms"), "{table:?}");

    // The heat map: one row per shown bucket, one column per minute of the period.
    let heat = chart(&res, "#heatmap-chart");
    let rows = heat["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 13);
    assert_eq!(heat["buckets"].as_array().unwrap().len(), 60);
    let cells: u64 = rows[0]["counts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|count| count.as_u64().unwrap())
        .sum();
    assert_eq!(cells, 6);
    // Its table view: the minute with executions.
    let minutes = res.texts("section[aria-labelledby=heatmap-heading] tbody tr td");
    assert_eq!(minutes, ["12", "2", "1.350", "< 20 ms", "< 1.7 s"]);
}

#[tokio::test]
async fn job_page_without_data() {
    let app = App::new().await;
    let res = app.get("/metrics/Nothing").await;
    res.assert_status(StatusCode::OK);
    assert_eq!(
        res.texts(".empty"),
        ["No execution of “Nothing” was recorded in the last hour."]
    );
    assert_eq!(res.count("#histogram-chart"), 0);
    assert_eq!(res.count("#job-facts"), 0);
    // Failures only: no timing, so no quantiles.
    track(&app, &[("Broken", 0, Outcome::Failure)]).await;
    let res = app.get("/metrics/Broken").await;
    assert_eq!(
        res.texts("#job-facts .fact dd"),
        ["1", "1", "100%", "0.00 s", "0.0 ms", "–", "–"]
    );
}

#[tokio::test]
async fn shows_deploy_marks() {
    let app = App::new().await;
    track_mike(&app).await;
    let recorded = Deploy::new(&app.pool)
        .mark(
            "abc1234 Fix <b>everything</b>",
            std::time::SystemTime::now(),
        )
        .await
        .unwrap();
    assert!(recorded);

    for path in ["/metrics", "/metrics/MikeJob"] {
        let res = app.get(path).await;
        res.assert_status(StatusCode::OK);
        assert_eq!(
            res.texts(".deploy-list .deploy-label"),
            ["abc1234 Fix <b>everything</b>"],
            "{path}"
        );
        assert!(!res.body.contains("<b>everything"), "{path}: escaped");
        let selector = if path == "/metrics" {
            "#metrics-chart"
        } else {
            "#job-series-chart"
        };
        let data = chart(&res, selector);
        let marks = data["marks"].as_array().unwrap();
        assert_eq!(marks.len(), 1, "{path}");
        assert_eq!(marks[0]["label"], "abc1234 Fix <b>everything</b>");
        // A mark sits on a bucket of the chart.
        let bucket = marks[0]["bucket"].as_str().unwrap();
        assert!(
            data["buckets"]
                .as_array()
                .unwrap()
                .iter()
                .any(|b| b.as_str() == Some(bucket)),
            "{path}"
        );
    }
}

#[tokio::test]
async fn class_names_are_escaped_and_encoded() {
    let app = App::new().await;
    track(
        &app,
        &[
            ("<script>alert(1)</script>", 5, Outcome::Success),
            ("Billing/Invoice Monthly", 5, Outcome::Success),
        ],
    )
    .await;
    let res = app.get("/metrics").await;
    assert!(!res.body.contains("<script>alert(1)"));
    assert!(classes(&res).contains(&"<script>alert(1)</script>".to_owned()));
    let links = res.attrs(".metrics-table a.job-class", "href");
    assert!(
        links.contains(&"/metrics/Billing%2FInvoice%20Monthly".to_owned()),
        "{links:?}"
    );
    assert!(
        links.contains(&"/metrics/%3Cscript%3Ealert(1)%3C%2Fscript%3E".to_owned()),
        "{links:?}"
    );
    for link in links {
        let res = app.get(&link).await;
        res.assert_status(StatusCode::OK);
        assert!(!res.body.contains("<script>alert(1)"));
        assert_eq!(res.texts("#job-facts .fact dd")[0], "1", "{link}");
    }
}

#[tokio::test]
async fn metrics_can_be_turned_off() {
    let app = App::build(|web| web.metrics(false), "").await;
    track_mike(&app).await;
    let home = app.get("/").await;
    assert!(!home.texts(".nav a").contains(&"Metrics".to_owned()));
    app.get("/metrics")
        .await
        .assert_status(StatusCode::NOT_FOUND);
    app.get("/metrics/MikeJob")
        .await
        .assert_status(StatusCode::NOT_FOUND);
    // On by default.
    let app = App::new().await;
    let home = app.get("/").await;
    assert!(home.texts(".nav a").contains(&"Metrics".to_owned()));
}

#[tokio::test]
async fn links_follow_the_mount() {
    let app = App::build(|web| web.read_only(true), "/admin/jobs").await;
    track_mike(&app).await;
    let res = app.get("/metrics").await;
    res.assert_status(StatusCode::OK);
    assert_eq!(
        res.attrs(".metrics-table a.job-class", "href"),
        ["/admin/jobs/metrics/MikeJob"]
    );
    assert!(
        res.attrs(".segmented a", "href")
            .iter()
            .all(|href| href.starts_with("/admin/jobs/metrics"))
    );
    assert_eq!(res.attrs(".filter-form", "action"), ["/admin/jobs/metrics"]);
    assert_eq!(res.count("script[src^='/admin/jobs/assets/metrics.']"), 1);
    let res = app.get("/metrics/MikeJob").await;
    res.assert_status(StatusCode::OK);
    assert_eq!(res.attrs("h1 a.crumb", "href"), ["/admin/jobs/metrics"]);
    // The metrics script itself is served.
    let script = res.attrs("script[src*='metrics.']", "src");
    let asset = app
        .send(
            http::Request::get(&script[0])
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await;
    asset.assert_status(StatusCode::OK);
    assert!(asset.body.contains("HeatMap"));
}

#[tokio::test]
async fn redis_outages_show_the_error_page() {
    let mut app = App::new().await;
    app.redis.stop();
    let res = app.get("/metrics").await;
    assert_eq!(res.status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(res.texts("h1"), ["Redis is not answering"]);
    let res = app.get("/metrics/MikeJob").await;
    assert_eq!(res.status, StatusCode::INTERNAL_SERVER_ERROR);
}

/// Sleeps for the given milliseconds.
#[derive(Default)]
struct Nap;

impl Job for Nap {
    const NAME: &'static str = "Nap";
    type Args = (u64,);

    async fn perform(&self, _ctx: &JobContext, (ms,): (u64,)) -> Result<(), JobError> {
        tokio::time::sleep(Duration::from_millis(ms)).await;
        Ok(())
    }
}

/// Polls `condition` every 20 ms until it holds; panics after 10 seconds.
async fn wait_for<F, Fut>(what: &str, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !condition().await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for: {what}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn shows_what_live_workers_record() {
    let app = App::new().await;
    let hench = app
        .redis
        .builder()
        .await
        .register(Nap)
        .auto_register(false)
        .build()
        .await
        .unwrap();
    let launcher = hench.start().await.unwrap();
    for ms in [30, 40, 50] {
        Nap::perform_async(hench.client(), (ms,)).await.unwrap();
    }
    // The workers flush their metrics on every heartbeat (100 ms in tests).
    wait_for("the metrics of three naps", || async {
        let res = app.get("/metrics/Nap").await;
        res.texts("#job-facts .fact dd").first().map(String::as_str) == Some("3")
    })
    .await;
    launcher.stop().await.unwrap();
    let res = app.get("/metrics").await;
    assert_eq!(classes(&res), ["Nap"]);
    let res = app.get("/metrics/Nap").await;
    let facts = res.texts("#job-facts .fact dd");
    assert_eq!(facts[1], "0");
    // Three timed naps: the median is a bucket bound (how long a sleep really takes depends
    // on the machine's load).
    assert!(facts[5].starts_with("< "), "{facts:?}");
}
