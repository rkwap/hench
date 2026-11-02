//! Every supported shape of job function.

use std::time::{Duration, SystemTime};

use hench::prelude::*;

/// No arguments, cannot fail.
#[hench::job]
async fn nothing() {}

/// Explicit unit return type.
#[hench::job]
async fn explicit_unit() -> () {}

/// Only the context.
#[hench::job]
async fn context_only(ctx: &JobContext) -> Result<(), JobError> {
    let _ = ctx.jid();
    Ok(())
}

/// The context under another name, through a full path.
#[hench::job]
async fn renamed_context(context: &hench::JobContext, n: u32) {
    let _ = (context.queue(), n);
}

/// Several arguments of different kinds.
#[hench::job(queue = "mailers", retry = 5)]
async fn many(
    ctx: &JobContext,
    id: i64,
    name: String,
    tags: Vec<String>,
    extra: Option<serde_json::Value>,
    pair: (u8, f64),
    map: std::collections::HashMap<String, i32>,
) -> Result<(), JobError> {
    let _ = (ctx, id, name, tags, extra, pair, map);
    Ok(())
}

/// Patterns and `mut` bindings.
#[hench::job]
async fn patterns((a, b): (i32, i32), mut c: i32, _: bool) {
    c += a + b;
    let _ = c;
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Order {
    id: u64,
}

/// Custom argument and error types.
#[hench::job(name = "orders::Ship")]
async fn ship(order: Order) -> Result<(), std::io::Error> {
    let _ = order.id;
    Ok(())
}

/// `anyhow::Result` through a type alias.
#[hench::job]
async fn with_anyhow(fail: bool) -> anyhow::Result<()> {
    if fail {
        anyhow::bail!("failed");
    }
    Ok(())
}

/// Every option.
#[hench::job(
    name = "Everything",
    queue = "q",
    retry = false,
    backtrace = 20,
    dead = false,
    tags = ["a", "b"],
    retry_for = 3600,
    retry_queue = "rq"
)]
async fn everything() {}

/// An explicitly empty tag list.
#[hench::job(tags = [])]
async fn untagged() {}

/// Other attributes apply to the body.
#[hench::job]
#[allow(clippy::too_many_arguments)]
#[inline]
async fn attributed(a: u8, b: u8, c: u8, d: u8, e: u8, f: u8, g: u8, h: u8) {
    let _ = (a, b, c, d, e, f, g, h);
}

/// A raw identifier as the name.
#[hench::job]
async fn r#match() {}

/// Recursion through the queue.
#[hench::job]
async fn countdown(ctx: &JobContext, n: u32) -> Result<(), JobError> {
    if n > 0 {
        countdown::perform_async(ctx.client(), (n - 1,)).await?;
    }
    Ok(())
}

mod jobs {
    /// Visible outside the module.
    #[hench::job]
    pub async fn public(value: String) {
        let _ = value;
    }

    /// Visible in the crate.
    #[hench::job]
    pub(crate) async fn crate_visible() {}
}

fn assert_job<J: Job + Clone + Copy + Default + std::fmt::Debug + Send + Sync>() {}

#[allow(dead_code)]
async fn usage(client: &Client) -> hench::Result<()> {
    nothing::perform_async(client, ()).await?;
    many::perform_async(
        client,
        (1, "x".into(), vec![], None, (1, 2.0), Default::default()),
    )
    .await?;
    ship::perform_in(client, Duration::from_secs(5), (Order { id: 1 },)).await?;
    ship::perform_at(client, SystemTime::now(), (Order { id: 1 },)).await?;
    let _: Vec<Option<String>> = patterns::perform_bulk(client, vec![((1, 2), 3, true)]).await?;
    everything::set().queue("other").perform_async(client, ()).await?;
    jobs::public::perform_async(client, ("v".into(),)).await?;
    jobs::crate_visible::perform_async(client, ()).await?;
    r#match::perform_async(client, ()).await?;
    let _: Result<(), JobError> = explicit_unit::perform_inline(()).await;
    let _: Result<(), JobError> = with_anyhow.perform_inline((false,)).await;
    Ok(())
}

fn main() {
    assert_job::<nothing>();
    assert_job::<explicit_unit>();
    assert_job::<context_only>();
    assert_job::<renamed_context>();
    assert_job::<many>();
    assert_job::<patterns>();
    assert_job::<ship>();
    assert_job::<with_anyhow>();
    assert_job::<everything>();
    assert_job::<attributed>();
    assert_job::<untagged>();
    assert_eq!(untagged::options().tags, Some(Vec::new()));
    assert_job::<r#match>();
    assert_job::<countdown>();
    assert_job::<jobs::public>();
    assert_job::<jobs::crate_visible>();
    assert_eq!(ship::NAME, "orders::Ship");
    assert_eq!(r#match::NAME, "match");
}
