//! `GET /`: the dashboard (totals, live chart, history, Redis), and `HEAD /`.

use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::response::Response;
use hench::api::StatsHistory;
use http::StatusCode;
use serde_json::json;

use crate::config::HenchWeb;
use crate::ctx::Ctx;
use crate::error::WebResult;
use crate::format;
use crate::params::{Query, query_string};
use crate::strings::EN;
use crate::templates::DashboardPage;
use crate::views::{DayView, RangeLink, RedisView, Totals};

/// The history ranges offered: days and label.
fn ranges() -> [(u32, &'static str); 4] {
    [
        (7, EN.one_week),
        (30, EN.one_month),
        (90, EN.three_months),
        (180, EN.six_months),
    ]
}

/// `GET /`.
pub(crate) async fn page(ctx: Ctx) -> Response {
    let result = render(&ctx).await;
    super::finish(&ctx, result)
}

async fn render(ctx: &Ctx) -> WebResult<Response> {
    let days = Query::parse(&ctx.query).days()?;
    let api = ctx.web.api();
    let stats = api.stats().await?;
    let history = StatsHistory::new(api, days)?;
    let processed = history.processed().await?;
    let failed = history.failed().await?;
    let info = match api.redis_info().await {
        Ok(info) => Some(info),
        Err(error) => {
            tracing::debug!(%error, "INFO is not available");
            None
        }
    };

    // Oldest first for the chart, newest first for the table.
    let mut dates = Vec::with_capacity(processed.len());
    let mut processed_counts = Vec::with_capacity(processed.len());
    let mut failed_counts = Vec::with_capacity(processed.len());
    let mut table = Vec::with_capacity(processed.len());
    for ((date, done), (_, fails)) in processed.iter().zip(failed.iter()) {
        let date = date.format("%Y-%m-%d").to_string();
        table.push(DayView {
            date: date.clone(),
            processed: format::delimit(*done),
            failed: format::delimit(*fails),
        });
        dates.push(date);
        processed_counts.push(*done);
        failed_counts.push(*fails);
    }
    dates.reverse();
    processed_counts.reverse();
    failed_counts.reverse();
    let history_json = json!({
        "dates": dates,
        "processed": processed_counts,
        "failed": failed_counts,
    })
    .to_string();

    let range_links = ranges()
        .iter()
        .map(|&(range, label)| RangeLink {
            label,
            href: if range == 30 {
                ctx.home()
            } else {
                format!(
                    "{}{}",
                    ctx.home(),
                    query_string(&[("days", &range.to_string())])
                )
            },
            active: range == days,
        })
        .collect();
    let range_label = ranges()
        .iter()
        .find(|(range, _)| *range == days)
        .map_or_else(|| format!("{days} days"), |(_, label)| (*label).to_owned());

    let page = DashboardPage {
        layout: super::layout(ctx, None, Some(Totals::new(&stats)), false),
        s: &EN,
        poll_secs: ctx.web.poll_interval.as_secs(),
        stats_url: ctx.url("/stats"),
        ranges: range_links,
        history_title: format!("{} · {range_label}", EN.history),
        history_json,
        history: table,
        redis: RedisView::new(info.as_ref()),
    };
    Ok(super::render(ctx, StatusCode::OK, &page))
}

/// How long `HEAD /` waits for Redis before answering `503`.
pub(crate) const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(1);

/// `HEAD /`: the cheapest health check that still talks to Redis (the length of the default
/// queue): `200` when Redis answers within [`HEARTBEAT_TIMEOUT`], `503` otherwise (a probe
/// gets an answer before its own timeout, however long reconnecting takes).
pub(crate) async fn heartbeat(State(web): State<Arc<HenchWeb>>) -> StatusCode {
    let error =
        match tokio::time::timeout(HEARTBEAT_TIMEOUT, web.api().queue("default").size()).await {
            Ok(Ok(_)) => return StatusCode::OK,
            Ok(Err(error)) => error.to_string(),
            Err(_) => format!("no answer within {HEARTBEAT_TIMEOUT:?}"),
        };
    tracing::warn!(error, "dashboard heartbeat: Redis is not answering");
    StatusCode::SERVICE_UNAVAILABLE
}
