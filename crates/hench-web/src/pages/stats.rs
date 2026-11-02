//! `GET /stats` and `GET /stats/queues`: the dashboard's numbers as JSON, polled by the live
//! chart and usable by monitoring tools.

use std::sync::Arc;
use std::time::SystemTime;

use axum::Json;
use axum::extract::State;
use axum::response::{IntoResponse, Redirect, Response};
use hench::api::Stats;
use http::StatusCode;
use serde_json::{Map, Value, json};

use crate::config::HenchWeb;
use crate::ctx::Ctx;
use crate::format;
use crate::views::{REDIS_FIELDS, store};

/// `GET /stats`: `{"hench": {processed, failed, busy, processes, enqueued, scheduled,
/// retries, dead, default_latency}, "redis": {the INFO summary, store_name, store_version},
/// "server_utc_time": "HH:MM:SS UTC", "server_time": RFC 3339}`. `redis` is empty when the
/// server does not allow `INFO`.
pub(crate) async fn stats(State(web): State<Arc<HenchWeb>>) -> Response {
    let api = web.api();
    let stats = match api.stats().await {
        Ok(stats) => stats,
        Err(error) => return unavailable(&error),
    };
    let mut redis = Map::new();
    match api.redis_info().await {
        Ok(info) => {
            for field in REDIS_FIELDS {
                if let Some(value) = info.get(field) {
                    redis.insert(field.to_owned(), Value::String(value.clone()));
                }
            }
            let (name, version) = store(&info);
            redis.insert("store_name".to_owned(), json!(name));
            redis.insert("store_version".to_owned(), json!(version));
        }
        Err(error) => tracing::debug!(%error, "INFO is not available"),
    }
    let now = SystemTime::now();
    Json(json!({
        "hench": summary(&stats),
        "redis": redis,
        "server_utc_time": format::clock(now),
        "server_time": format::iso(now),
    }))
    .into_response()
}

/// The totals of `stats` in the JSON shape of `/stats`.
pub(crate) fn summary(stats: &Stats) -> Value {
    json!({
        "processed": stats.processed,
        "failed": stats.failed,
        "busy": stats.workers_size,
        "processes": stats.processes_size,
        "enqueued": stats.enqueued,
        "scheduled": stats.scheduled_size,
        "retries": stats.retry_size,
        "dead": stats.dead_size,
        "default_latency": stats.default_queue_latency,
    })
}

/// `GET /stats/queues`: every queue's size, `{"name": size}`.
pub(crate) async fn queues(State(web): State<Arc<HenchWeb>>) -> Response {
    match Stats::queues(web.api()).await {
        Ok(queues) => {
            let sizes: Map<String, Value> = queues
                .into_iter()
                .map(|(name, size)| (name, json!(size)))
                .collect();
            Json(Value::Object(sizes)).into_response()
        }
        Err(error) => unavailable(&error),
    }
}

/// `GET /dashboard/stats`: the address older dashboards polled.
pub(crate) async fn legacy_redirect(ctx: Ctx) -> Redirect {
    Redirect::to(&ctx.url("/stats"))
}

/// `503` with a JSON error, for Redis failures.
fn unavailable(error: &hench::Error) -> Response {
    tracing::error!(%error, "dashboard statistics: Redis error");
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({"error": error.to_string()})),
    )
        .into_response()
}
