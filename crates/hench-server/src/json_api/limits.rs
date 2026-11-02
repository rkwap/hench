//! The JSON API of the limiters and throttles ([ratelimit.md] §2.16):
//!
//! * `GET /limits[?filter=TEXT][&sort=last|name|refused][&limit=N]`: `{"total", "limits":
//!   [...], "last_hour": {"granted", "refused", "by_name"}}` (at most 1000 limits, 50 by
//!   default; `filter` matches ids, `*` standing for any run of characters);
//! * `GET /limits/{kind}/{id}`: one limit (`kind` its letter: `c`, `w`, `b`, `g`; `id`
//!   percent-encoded), with its `holders` for concurrency; `404` when it is not in use;
//! * `DELETE /limits/{kind}/{id}`: resets it, `{"reset": <whether it was in use>}`; `403` when
//!   the API is read-only.
//!
//! [ratelimit.md]: https://github.com/rkwap/hench/blob/v0.1.0/docs/design/ratelimit.md

use std::sync::Arc;

use axum::extract::rejection::{PathRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use hench::api::Api;
use hench::limit::{LimitKind, LimitOrder};
use hench::metrics::Period;
use http::StatusCode;
use serde::Deserialize;
use serde_json::json;

use crate::commands::limits::{events_json, info_json};

/// The most limits a list answers with.
const MAX_LIST: usize = 1_000;

/// The default size of a list.
const DEFAULT_LIST: usize = 50;

/// What the handlers share.
struct LimitsState {
    api: Api,
    read_only: bool,
}

/// The routes, mounted under the API's prefix (`read_only`: refuse changes).
pub(crate) fn routes(api: &Api, read_only: bool) -> Router {
    Router::new()
        .route("/limits", get(list))
        .route("/limits/{kind}/{id}", get(show).delete(reset))
        .with_state(Arc::new(LimitsState {
            api: api.clone(),
            read_only,
        }))
}

/// The parameters of `GET /limits`.
#[derive(Debug, Deserialize)]
struct ListQuery {
    filter: Option<String>,
    sort: Option<String>,
    limit: Option<usize>,
}

/// `GET /limits`.
async fn list(
    State(state): State<Arc<LimitsState>>,
    query: Result<Query<ListQuery>, QueryRejection>,
) -> Response {
    let Query(query) = match query {
        Ok(query) => query,
        Err(rejection) => return super::error(StatusCode::BAD_REQUEST, rejection.body_text()),
    };
    let order = match query.sort.as_deref() {
        None | Some("" | "last") => LimitOrder::LastUsed,
        Some("name") => LimitOrder::Name,
        Some("refused") => LimitOrder::Refused,
        Some(_) => {
            return super::error(
                StatusCode::BAD_REQUEST,
                "unknown sort: use last, name or refused",
            );
        }
    };
    let size = query.limit.unwrap_or(DEFAULT_LIST).clamp(1, MAX_LIST);
    let limits = state.api.limits();
    let filter = query.filter.as_deref().unwrap_or_default();
    let page = match limits.page_matching(1, size, order, filter).await {
        Ok(page) => page,
        Err(error) => return super::unavailable(&error),
    };
    let events = match limits.events(Period::Minutes(60)).await {
        Ok(events) => events,
        Err(error) => return super::unavailable(&error),
    };
    Json(json!({
        "total": page.total,
        "limits": page.items.iter().map(info_json).collect::<Vec<_>>(),
        "last_hour": events_json(&events),
    }))
    .into_response()
}

/// The `{kind}/{id}` of a limit's routes.
#[derive(Debug, Deserialize)]
struct LimitPath {
    kind: String,
    id: String,
}

/// The kind of a path's `{kind}` (its letter: `c`, `w`, `b`, `g`), `None` for anything else.
fn kind_of(path: &LimitPath) -> Option<LimitKind> {
    let mut letters = path.kind.chars();
    let kind = LimitKind::from_letter(letters.next()?)?;
    (letters.next().is_none() && !path.id.is_empty()).then_some(kind)
}

/// The answer to a path whose `{kind}` is not one.
fn no_such_kind() -> Response {
    super::error(
        StatusCode::NOT_FOUND,
        "no such limit: the kind is c, w, b or g",
    )
}

/// `GET /limits/{kind}/{id}`.
async fn show(
    State(state): State<Arc<LimitsState>>,
    path: Result<Path<LimitPath>, PathRejection>,
) -> Response {
    let path = match path {
        Ok(Path(path)) => path,
        Err(rejection) => return super::bad_path(&rejection),
    };
    let Some(kind) = kind_of(&path) else {
        return no_such_kind();
    };
    let limits = state.api.limits();
    let info = match limits.get(kind, &path.id).await {
        Ok(Some(info)) => info,
        Ok(None) => {
            return super::error(
                StatusCode::NOT_FOUND,
                "this limit is not in use: its counters expired, or it was reset",
            );
        }
        Err(error) => return super::unavailable(&error),
    };
    let mut value = info_json(&info);
    if kind == LimitKind::Concurrency {
        let holders = match limits.holders(&info.id).await {
            Ok(holders) => holders,
            Err(error) => return super::unavailable(&error),
        };
        value["holders"] = holders
            .iter()
            .map(|holder| {
                json!({
                    "token": holder.token,
                    "jid": holder.jid,
                    // `null` beyond the calendar (lease ends come from Redis).
                    "lease_ends_at": crate::monitor::datetime(holder.lease_ends_at).map(|at| {
                        at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
                    }),
                    "ended": holder.ended,
                })
            })
            .collect();
    }
    Json(value).into_response()
}

/// `DELETE /limits/{kind}/{id}`.
async fn reset(
    State(state): State<Arc<LimitsState>>,
    path: Result<Path<LimitPath>, PathRejection>,
) -> Response {
    if state.read_only {
        return super::error(StatusCode::FORBIDDEN, "the API is read-only");
    }
    let path = match path {
        Ok(Path(path)) => path,
        Err(rejection) => return super::bad_path(&rejection),
    };
    let Some(kind) = kind_of(&path) else {
        return no_such_kind();
    };
    match state.api.limits().reset(kind, &path.id).await {
        Ok(existed) => {
            tracing::info!(
                identity = %format!("{}:{}", kind.letter(), path.id).escape_debug(),
                "JSON API reset a limit"
            );
            Json(json!({"reset": existed})).into_response()
        }
        Err(error) => super::unavailable(&error),
    }
}
