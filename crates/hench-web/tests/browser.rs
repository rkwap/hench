//! The dashboard in a real browser: the behaviour of its scripts and styles (live refresh,
//! the dashboard's refresh slider and status line, the metrics pages' refresh, layout at
//! phone width, contrast; the feature tabs and a job carrying every feature in both themes).
//!
//! Each test serves a dashboard over TCP and runs a Playwright script from `tests/browser/`
//! with Node. They need `node` with the `playwright` package and its Chromium; without them
//! they are skipped (with a note), unless `HENCH_BROWSER_TESTS=1` makes that a failure.
//! `HENCH_BROWSER_TESTS=0` skips them.

mod common;

use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use common::*;
use hench::metrics::{Outcome, Tracker};
use hench_web::HenchWeb;
use serde_json::json;

/// `true` when the browser tests can run; panics when they must and cannot.
fn browser_available() -> bool {
    let wanted = std::env::var("HENCH_BROWSER_TESTS").ok();
    if wanted.as_deref() == Some("0") {
        return false;
    }
    // Playwright and the Chromium it drives (its browsers are installed separately).
    let probe = "const path = require('playwright').chromium.executablePath(); \
                 process.exit(require('fs').existsSync(path) ? 0 : 1)";
    let available = std::process::Command::new("node")
        .args(["-e", probe])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success());
    if !available {
        assert!(
            wanted.as_deref() != Some("1"),
            "HENCH_BROWSER_TESTS=1 but Node cannot run Playwright's Chromium"
        );
        eprintln!("skipping a browser test: Node with Playwright and Chromium is not available");
    }
    available
}

/// Serves `app`'s dashboard (configured by `configure`) over TCP; returns its base URL.
async fn serve(app: &App, configure: impl FnOnce(HenchWeb) -> HenchWeb) -> String {
    let router = hench_web::router(configure(HenchWeb::new(&app.api)));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a port");
    let address = listener.local_addr().expect("an address");
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    format!("http://{address}")
}

/// Runs `tests/browser/<script>` with `args`; panics with its output unless it succeeds.
async fn run_script(script: &str, args: &[&str]) {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("browser")
        .join(script);
    let output = tokio::time::timeout(
        Duration::from_secs(120),
        tokio::process::Command::new("node")
            .arg(&path)
            .args(args)
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("the browser script finishes")
    .expect("node runs");
    assert!(
        output.status.success(),
        "{script} failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// A few retries, a queue and a process, so that every list has rows.
async fn seed(app: &App) {
    for (class, index) in [
        ("ChargeCard", 1),
        ("Billing::Invoices::SendReminderEmail", 2),
        ("Flaky", 3),
    ] {
        let job = json!({"class": class, "args": ["customer", index], "queue": "critical",
                         "jid": jid(), "error_class": "Timeout", "error_message": "boom",
                         "retry_count": 1, "failed_at": now_millis()});
        add_to_set(app, "retry", job, now_secs() + 600.0 + f64::from(index)).await;
    }
    push_raw(
        app,
        "maintenance",
        &json!({"class": "StaticJob", "args": [1]}),
    )
    .await;
    add_process(
        app,
        "worker-1.example:42:abcdef",
        json!({"hostname": "worker-1.example", "pid": 42, "started_at": now_secs(),
               "concurrency": 5, "queues": ["critical", "maintenance"], "embedded": true,
               "identity": "worker-1.example:42:abcdef"}),
        0,
    )
    .await;
}

#[tokio::test]
async fn live_refresh_keeps_the_keyboard_focus() {
    if !browser_available() {
        return;
    }
    let app = App::new().await;
    seed(&app).await;
    let base = serve(&app, |web| web.poll_interval(Duration::from_secs(2))).await;
    run_script("live_focus.js", &[&base]).await;
}

#[tokio::test]
async fn the_refresh_slider_shows_the_interval_in_use_and_polls_stay_quiet() {
    if !browser_available() {
        return;
    }
    let app = App::new().await;
    seed(&app).await;
    let fast = serve(&app, |web| web.poll_interval(Duration::from_secs(1))).await;
    let slow = serve(&app, |web| web.poll_interval(Duration::from_secs(60))).await;
    run_script("dashboard.js", &[&fast, &slow]).await;
}

#[tokio::test]
async fn the_metrics_refresh_keeps_open_tables_and_focus() {
    if !browser_available() {
        return;
    }
    let app = App::new().await;
    let tracker = Tracker::new();
    for index in 0..8_u64 {
        let class = format!("FakeClass{index}");
        for run in 0..=index {
            tracker.track(
                &class,
                Duration::from_millis(10 + 30 * run),
                Outcome::Success,
            );
        }
    }
    tracker.flush(&app.pool).await.expect("metrics flush");
    let base = serve(&app, |web| web).await;
    run_script("metrics_refresh.js", &[&base]).await;
}

#[tokio::test]
async fn pages_read_well_on_phones_and_have_enough_contrast() {
    if !browser_available() {
        return;
    }
    let app = App::new().await;
    seed(&app).await;
    let base = serve(&app, |web| web).await;
    run_script("layout.js", &[&base]).await;
}

/// A job of the feature seeds (never run here).
struct Report;

impl hench::Job for Report {
    const NAME: &'static str = "Report";
    type Args = (u32,);

    async fn perform(
        &self,
        _ctx: &hench::JobContext,
        _args: (u32,),
    ) -> Result<(), hench::JobError> {
        Ok(())
    }
}

/// Something on each feature tab: a schedule, a batch with members and callbacks, limits in
/// use (a window that refused a call, a concurrency limit with a holder), a unique lock, and a
/// job carrying every feature in the schedule. Returns the pages to visit with the tab each
/// belongs to, and the permit of the holder.
async fn seed_features(app: &App) -> (Vec<(String, &'static str)>, hench::limit::Permit) {
    use hench::prelude::*;

    let client = Client::new(app.pool.clone());
    app.api
        .periodic()
        .upsert(
            &Periodic::cron("nightly-report", "0 2 * * *")
                .job::<Report>((1,))
                .description("The PDF report of the day before"),
        )
        .await
        .expect("a schedule");
    let committed = Batch::new(&client)
        .description("Import #7")
        .on_complete::<Report>((7,))
        .on_success::<Report>((7,))
        .jobs(|jobs| async move {
            for row in 0..3 {
                jobs.job::<Report>().perform_async((row,)).await?;
            }
            Ok::<_, hench::Error>(())
        })
        .await
        .expect("a batch");
    let window = Limiter::window("api", 2, Duration::from_secs(60)).max_wait(Duration::ZERO);
    for _ in 0..2 {
        let _granted = window.acquire(&app.pool).await.expect("a slot");
    }
    assert!(
        window.acquire(&app.pool).await.is_err(),
        "the third call is refused"
    );
    let permit = Limiter::concurrent("erp", 2)
        .acquire(&app.pool)
        .await
        .expect("a permit");
    let mut job = JobPayload::new("Sync", vec![json!(42)]);
    job.unique = Some(Unique::new(Duration::from_secs(600)).into());
    client.push(job).await.expect("a unique job");
    let digests: Vec<String> = app
        .redis(
            hench::redis_rs::cmd("ZRANGE")
                .arg("unique-locks")
                .arg(0)
                .arg(-1),
        )
        .await;
    let digest = digests.first().cloned().expect("a lock");
    let featured = json!({
        "class": "Featured", "args": [1], "jid": jid(), "queue": "default", "retry": true,
        "created_at": now_millis(), "bid": committed.bid,
        "periodic": {"name": "nightly-report", "tick": now_millis()},
        "unique": {"for": 600, "until": "start", "digest": digest},
        "throttle": {"name": "erp", "key": "42"},
        "limited": {"n": 3, "by": "w:api", "at": now_millis()},
    });
    let (job, score) = add_to_set(app, "schedule", featured, now_secs() + 600.0).await;
    let pages = vec![
        ("/periodic".to_owned(), "Periodic"),
        ("/periodic/nightly-report".to_owned(), "Periodic"),
        ("/batches".to_owned(), "Batches"),
        (format!("/batches/{}", committed.bid), "Batches"),
        ("/limits".to_owned(), "Limits"),
        ("/limits/w/api".to_owned(), "Limits"),
        ("/limits/c/erp".to_owned(), "Limits"),
        ("/locks".to_owned(), "Locks"),
        (format!("/locks/{digest}"), "Locks"),
        (format!("/scheduled/{}", key(&job, score)), "Scheduled"),
    ];
    (pages, permit)
}

#[tokio::test]
async fn the_feature_tabs_read_well_in_both_themes() {
    if !browser_available() {
        return;
    }
    let app = App::new().await;
    seed(&app).await;
    let (pages, _permit) = seed_features(&app).await;
    let base = serve(&app, |web| web).await;
    let pages = serde_json::to_string(&pages).expect("the pages as JSON");
    run_script("features.js", &[&base, &pages]).await;
}

#[tokio::test]
async fn forms_post_back_values_holding_line_breaks() {
    // Round 3, web-api-security-5: Chromium posts a lone LF or CR in a field's value as CRLF,
    // so forms that carried such a jid, key or identity raw acted on nothing.
    use hench::limit::{LimitKind, Limiter};
    use hench::prelude::*;

    if !browser_available() {
        return;
    }
    let app = App::new().await;
    let client = Client::new(app.pool.clone());
    let locks = app.api.unique_locks();
    for (n, jid) in [(1, "lone\nlf"), (2, "lone\rcr")] {
        Report::set()
            .jid(jid)
            .queue("parked")
            .unique_for(Duration::from_secs(600))
            .perform_async(&client, (n,))
            .await
            .expect("a unique job");
    }
    let held = locks
        .page(1, 10, hench::api::SortOrder::Ascending)
        .await
        .expect("the locks")
        .items;
    let lock = held
        .iter()
        .find(|lock| lock.jid == "lone\nlf")
        .expect("the lock")
        .digest
        .clone();
    let bid = Batch::new(&client)
        .jobs(|jobs| async move {
            Report::set()
                .jid("member\rcr")
                .queue("parked")
                .perform_async(&jobs, (3,))
                .await?;
            Ok::<_, hench::Error>(())
        })
        .await
        .expect("a batch")
        .bid;
    let _permit = Limiter::concurrent("odd", 2)
        .key("lone\nlf")
        .acquire(&app.pool)
        .await
        .expect("a permit");
    add_retry(&app, "retry\nlf").await;
    let identity = "odd\nhost:1:abc";
    add_process(
        &app,
        identity,
        json!({"hostname": "odd\nhost", "identity": identity}),
        0,
    )
    .await;

    let base = serve(&app, |web| web).await;
    let seeded = json!({"lock": lock, "batch": bid}).to_string();
    run_script("form_values.js", &[&base, &seeded]).await;

    assert_eq!(locks.size().await.expect("the locks"), 0);
    assert!(
        app.api
            .batches()
            .pending_jids(&bid, 10)
            .await
            .expect("the batch")
            .is_empty()
    );
    assert!(
        app.api
            .limits()
            .get(LimitKind::Concurrency, "odd:lone\nlf")
            .await
            .expect("the limit")
            .is_none()
    );
    assert_eq!(zcard(&app, "retry").await, 0);
    assert_eq!(pop_signal(&app, identity).await.as_deref(), Some("TSTP"));
}
