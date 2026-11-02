//! The dashboard against live workers (a real `Hench` launcher): heartbeats, running jobs,
//! and quiet and stop sent from the busy page.

mod common;

use std::future::Future;
use std::time::Duration;

use common::*;
use hench::CancellationToken;
use hench::prelude::*;
use http::StatusCode;

/// Runs until the test releases it (or the process stops).
struct Blocking {
    release: CancellationToken,
}

impl Job for Blocking {
    const NAME: &'static str = "Blocking";
    type Args = (String,);

    fn options() -> JobOptions {
        JobOptions::new().queue("slow").tags(["live"])
    }

    async fn perform(&self, ctx: &JobContext, (_label,): (String,)) -> Result<(), JobError> {
        tokio::select! {
            () = self.release.cancelled() => {}
            () = ctx.stopping() => {}
        }
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
async fn busy_page_shows_and_controls_live_workers() {
    let app = App::new().await;
    let release = CancellationToken::new();
    let hench = app
        .redis
        .builder()
        .await
        .queues(["slow", "default"])
        .concurrency(3)
        .tag("live-test")
        .register(Blocking {
            release: release.clone(),
        })
        .build()
        .await
        .unwrap();
    let launcher = hench.start().await.unwrap();
    let identity = launcher.identity().to_owned();
    Blocking::perform_async(hench.client(), ("first <job>".to_owned(),))
        .await
        .unwrap();

    wait_for("the job to run", || async {
        app.get("/stats").await.json()["hench"]["busy"] == 1
    })
    .await;

    let busy = app.get("/busy").await;
    busy.assert_status(StatusCode::OK);
    let process = busy.texts(".process-cell").concat();
    assert!(process.contains(&identity), "{process}");
    assert!(process.contains("live-test"), "{process}");
    assert!(process.contains("slow, default"), "{process}");
    // Same hench version as the dashboard: no version line.
    assert!(!process.contains("Version"), "{process}");
    let facts = busy.texts(".facts dd");
    assert_eq!(facts[..4], ["1", "3", "1", "33%"]);
    assert_eq!(busy.title(), "Busy (1/3) · Hench");
    // The running job, from the process's work hash.
    let jobs = busy.texts("section[aria-labelledby=jobs-heading] tbody tr");
    assert_eq!(jobs.len(), 1, "{jobs:?}");
    assert!(
        jobs[0].contains("Blocking") && jobs[0].contains("slow"),
        "{jobs:?}"
    );
    assert!(busy.body.contains("&#34;first &#60;job&#62;&#34;"));
    assert!(jobs[0].contains(&identity));

    // Quiet it from the dashboard: the process stops fetching and says so.
    app.post("/busy", &[("quiet", "1"), ("identity", &identity)])
        .await
        .assert_status(StatusCode::SEE_OTHER);
    wait_for("the process to be quiet", || async {
        launcher.is_quiet()
            && app
                .api
                .processes()
                .get(&identity)
                .await
                .unwrap()
                .is_some_and(|process| process.is_stopping())
    })
    .await;
    let busy = app.get("/busy").await;
    assert_eq!(busy.texts(".process-cell .chip-warn"), ["quiet"]);
    // A quiet process offers Stop only.
    assert_eq!(busy.count("tbody button[name=quiet]"), 0);
    assert_eq!(busy.count("tbody button[name=stop]"), 1);
    // Jobs pushed now wait.
    Blocking::perform_async(hench.client(), ("second".to_owned(),))
        .await
        .unwrap();

    // Stop it from the dashboard. A process started with `start` is embedded, so "Stop all"
    // leaves it alone; its own Stop button reaches it.
    assert_eq!(busy.count("button[name=stop][title]"), 0);
    release.cancel();
    app.post("/busy", &[("stop", "1"), ("identity", &identity)])
        .await
        .assert_status(StatusCode::SEE_OTHER);
    tokio::time::timeout(Duration::from_secs(10), launcher.wait())
        .await
        .expect("the process stops");
    wait_for("the process to leave the dashboard", || async {
        app.get("/busy").await.count(".process-cell") == 0
    })
    .await;
    let queue = app.get("/queues/slow").await;
    assert!(queue.body.contains("second"));
    let stats = app.get("/stats").await.json();
    assert_eq!(stats["hench"]["processed"], 1);
    assert_eq!(stats["hench"]["busy"], 0);
}
