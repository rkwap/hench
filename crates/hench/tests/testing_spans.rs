//! Testing clients run jobs inside the same `job{class, jid, ..}` tracing span as workers.
//!
//! In a test binary of its own: `tracing` caches globally whether a callsite is enabled, so a
//! test that installs a thread-local subscriber races other test threads registering the same
//! callsite (the `job` span every job runs in) and can find it cached as disabled.

use std::sync::{Arc, Mutex};

use hench::prelude::*;
use hench::testing::Testing;
use tracing::field::{Field, Visit};
use tracing_subscriber::layer::SubscriberExt;

/// Records `name class=.. jid=..` for every new span.
struct Spans(Arc<Mutex<Vec<String>>>);

/// Collects the `class` and `jid` fields of a span.
#[derive(Default)]
struct Fields(Vec<String>);

impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if matches!(field.name(), "class" | "jid") {
            self.0.push(format!("{}={value:?}", field.name()));
        }
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Spans {
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        _id: &tracing::span::Id,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut fields = Fields::default();
        attrs.record(&mut fields);
        self.0.lock().unwrap().push(format!(
            "{} {}",
            attrs.metadata().name(),
            fields.0.join(" ")
        ));
    }
}

/// Counts its runs.
#[derive(Default)]
struct Counted;

impl Job for Counted {
    const NAME: &'static str = "Counted";
    type Args = (i64,);

    async fn perform(&self, ctx: &JobContext, _args: (i64,)) -> Result<(), JobError> {
        ctx.data::<std::sync::atomic::AtomicUsize>()?
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn testing_clients_run_jobs_in_the_workers_job_span() {
    let spans = Arc::new(Mutex::new(Vec::new()));
    let _guard =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(Spans(spans.clone())));
    let runs = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let testing = Testing::new().register(Counted).data_arc(runs.clone());

    let inline = testing.inline();
    let ran_inline = Counted::perform_async(&inline, (1,))
        .await
        .unwrap()
        .unwrap();
    let fake = testing.fake();
    let drained = Counted::perform_async(&fake, (2,)).await.unwrap().unwrap();
    assert_eq!(fake.drain::<Counted>().await.unwrap(), 1);
    assert_eq!(runs.load(std::sync::atomic::Ordering::SeqCst), 2);

    let spans = spans.lock().unwrap().clone();
    for jid in [ran_inline, drained] {
        let expected = format!("job class=Counted jid={jid}");
        assert!(spans.contains(&expected), "{expected} not in {spans:?}");
    }
}
