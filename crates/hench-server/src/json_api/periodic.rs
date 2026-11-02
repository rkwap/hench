//! The JSON API of the periodic schedules:
//!
//! * `GET /periodic?page=&count=`: `{"size", "page", "count", "schedules": [...]}`: a page of
//!   the schedules by name (`count` from 1 to 1000, default 100);
//! * `GET /periodic/{name}`: one schedule with its next ticks (`upcoming`), its English
//!   description (`describe`), its job template (`template`), its history (newest first) and
//!   every field of its hash (`fields`); `404` when there is none;
//! * `POST /periodic/{name}/enqueue`: pushes the schedule's job now (its timetable does not
//!   change): `200 {"enqueued": true, "jid"}`, or `{"enqueued": false, "jid": null}` when
//!   client middleware stopped the job or it is unique and its lock is held;
//! * `POST /periodic/{name}/pause` and `/unpause`: `200 {"name", "paused", "changed"}`
//!   (`changed`: false when it already was).
//!
//! The changes answer `403` when the server is read-only, `404` for an unknown schedule and
//! `400` for an unknown action or a definition this version cannot read. A schedule is the
//! object `hench periodic --json` prints (`name`, `source`, `group`, `timetable`, `class`,
//! `queue`, `args`, `next`, `paused`, `last`, `last_run`, `counters`, `error`, ...), times in
//! RFC 3339.

use std::sync::Arc;
use std::time::SystemTime;

use axum::extract::rejection::{PathRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use hench::api::Api;
use hench::periodic::PeriodicSet;
use http::StatusCode;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::commands::periodic::{HISTORY, detail_json, schedule_json};

/// Schedules a page answers at most.
const MAX_COUNT: usize = 1_000;

/// Schedules a page answers by default.
const DEFAULT_COUNT: usize = 100;

/// What the handlers share.
struct PeriodicState {
    api: Api,
    read_only: bool,
}

/// The routes, mounted under the API's prefix (`read_only`: refuse changes).
pub(crate) fn routes(api: &Api, read_only: bool) -> Router {
    let state = Arc::new(PeriodicState {
        api: api.clone(),
        read_only,
    });
    Router::new()
        .route("/periodic", get(list))
        .route("/periodic/{name}", get(show))
        .route("/periodic/{name}/{action}", post(act))
        .with_state(state)
}

/// The query of `GET /periodic`.
#[derive(Debug, Deserialize)]
struct ListQuery {
    page: Option<usize>,
    count: Option<usize>,
}

/// The `{name}` of a schedule's route (names may contain `/`, sent as `%2F`).
#[derive(Debug, Deserialize)]
struct ScheduleName {
    name: String,
}

/// The `{name}` and `{action}` of `POST /periodic/{name}/{action}`.
#[derive(Debug, Deserialize)]
struct ScheduleAction {
    name: String,
    action: String,
}

/// A malformed query string, as a JSON error.
fn bad_query(rejection: &QueryRejection) -> Response {
    super::error(StatusCode::BAD_REQUEST, rejection.body_text())
}

/// No such schedule.
fn not_found() -> Response {
    super::error(
        StatusCode::NOT_FOUND,
        "there is no periodic job with this name",
    )
}

/// An error of the API: a refusal (`InvalidSchedule`: an unreadable definition, a schedule
/// that kept changing) is a bad request; anything else is a Redis failure.
fn failed(failure: &hench::Error) -> Response {
    match failure {
        hench::Error::InvalidSchedule(message) => {
            super::error(StatusCode::BAD_REQUEST, message.clone())
        }
        other => super::unavailable(other),
    }
}

/// `GET /periodic`.
async fn list(
    State(state): State<Arc<PeriodicState>>,
    query: Result<Query<ListQuery>, QueryRejection>,
) -> Response {
    let query = match query {
        Ok(Query(query)) => query,
        Err(rejection) => return bad_query(&rejection),
    };
    let count = query.count.unwrap_or(DEFAULT_COUNT);
    if !(1..=MAX_COUNT).contains(&count) {
        return super::error(
            StatusCode::BAD_REQUEST,
            format!("`count` must be between 1 and {MAX_COUNT}"),
        );
    }
    let page = query.page.unwrap_or(1);
    if page == 0 {
        return super::error(StatusCode::BAD_REQUEST, "`page` starts at 1");
    }
    let found = match state.api.periodic().page(page, count).await {
        Ok(found) => found,
        Err(failure) => return super::unavailable(&failure),
    };
    let now = SystemTime::now();
    let schedules: Vec<Value> = found
        .items
        .iter()
        .map(|entry| schedule_json(entry, now))
        .collect();
    Json(json!({
        "size": found.total,
        "page": found.number,
        "count": found.size,
        "schedules": schedules,
    }))
    .into_response()
}

/// `GET /periodic/{name}`.
async fn show(
    State(state): State<Arc<PeriodicState>>,
    path: Result<Path<ScheduleName>, PathRejection>,
) -> Response {
    let name = match path {
        Ok(Path(ScheduleName { name })) => name,
        Err(rejection) => return super::bad_path(&rejection),
    };
    let set = state.api.periodic();
    let entry = match set.get(&name).await {
        Ok(Some(entry)) => entry,
        Ok(None) => return not_found(),
        Err(failure) => return super::unavailable(&failure),
    };
    let history = match set.history(&name, HISTORY).await {
        Ok(history) => history,
        Err(failure) => return super::unavailable(&failure),
    };
    Json(detail_json(&entry, &history, SystemTime::now())).into_response()
}

/// `POST /periodic/{name}/{action}`.
async fn act(
    State(state): State<Arc<PeriodicState>>,
    path: Result<Path<ScheduleAction>, PathRejection>,
) -> Response {
    if state.read_only {
        return super::error(StatusCode::FORBIDDEN, "the server is read-only");
    }
    let (name, action) = match path {
        Ok(Path(ScheduleAction { name, action })) => (name, action),
        Err(rejection) => return super::bad_path(&rejection),
    };
    if !matches!(action.as_str(), "enqueue" | "pause" | "unpause") {
        return super::error(
            StatusCode::BAD_REQUEST,
            "the action must be enqueue, pause or unpause",
        );
    }
    let set = state.api.periodic();
    match set.get(&name).await {
        Ok(Some(_)) => {}
        Ok(None) => return not_found(),
        Err(failure) => return super::unavailable(&failure),
    }
    let answer = match action.as_str() {
        "enqueue" => enqueue(&set, &name).await,
        "pause" => set
            .pause(&name)
            .await
            .map(|changed| json!({"name": name, "paused": true, "changed": changed})),
        _ => set
            .unpause(&name)
            .await
            .map(|changed| json!({"name": name, "paused": false, "changed": changed})),
    };
    match answer {
        Ok(body) => {
            tracing::info!(schedule = name, action, "JSON API changed a periodic job");
            Json(body).into_response()
        }
        Err(failure) => failed(&failure),
    }
}

/// "Enqueue now".
async fn enqueue(set: &PeriodicSet, name: &str) -> hench::Result<Value> {
    Ok(match set.enqueue(name).await? {
        Some(jid) => json!({"enqueued": true, "jid": jid}),
        None => json!({"enqueued": false, "jid": null}),
    })
}
