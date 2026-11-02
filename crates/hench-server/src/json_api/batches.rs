//! The JSON API of the batches ([batches.md] §2.13); times are seconds since the
//! Unix epoch (floats), like the rest of the API:
//!
//! * `GET /batches[?state=active|dead|complete|succeeded|cancelled|all][&substr=TEXT][&page=N]
//!   [&count=N]`: `{"total", "page", "count", "searched", "truncated", "batches": [...]}`, most
//!   recently active first (`state` defaults to `active`: not succeeded, not finished; at most
//!   1000 per page, 50 by default; `substr` matches bids and descriptions, `*` standing for any
//!   run of characters);
//! * `GET /batches/{bid}`: one batch with its callbacks, its first 100 failures and their total,
//!   `404` when it does not exist (the polling endpoint of a batch's status);
//! * `POST /batches/{bid}/cancel`: cancels it and its descendants, `{"cancelled": <newly
//!   cancelled batches>}`; `409` when it already succeeded, `404` when it does not exist, `403`
//!   when the API is read-only (and for cross-site requests, as every change).
//!
//! [batches.md]: https://github.com/rkwap/hench/blob/v0.1.0/docs/design/batches.md

use std::sync::Arc;

use axum::extract::rejection::{PathRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use hench::api::Api;
use hench::batch::{BatchError, BatchFilter};
use http::StatusCode;
use serde::Deserialize;
use serde_json::json;

use crate::commands::batches::{failure_json, status_json, summary_json};

/// The most batches a page answers with.
const MAX_PAGE: usize = 1_000;

/// The default size of a page.
const DEFAULT_PAGE: usize = 50;

/// Failures a batch's answer carries.
const FAILURES: usize = 100;

/// What the handlers share.
struct BatchesState {
    api: Api,
    read_only: bool,
}

/// The routes, mounted under the API's prefix (`read_only`: refuse changes).
pub(crate) fn routes(api: &Api, read_only: bool) -> Router {
    Router::new()
        .route("/batches", get(list))
        .route("/batches/{bid}", get(show))
        .route("/batches/{bid}/cancel", post(cancel))
        .with_state(Arc::new(BatchesState {
            api: api.clone(),
            read_only,
        }))
}

/// The parameters of `GET /batches`.
#[derive(Debug, Deserialize)]
struct ListQuery {
    state: Option<String>,
    substr: Option<String>,
    page: Option<usize>,
    count: Option<usize>,
}

/// `GET /batches`.
async fn list(
    State(state): State<Arc<BatchesState>>,
    query: Result<Query<ListQuery>, QueryRejection>,
) -> Response {
    let Query(query) = match query {
        Ok(query) => query,
        Err(rejection) => return super::error(StatusCode::BAD_REQUEST, rejection.body_text()),
    };
    let filter = match query.state.as_deref() {
        None | Some("" | "active") => BatchFilter::Active,
        Some("dead") => BatchFilter::Dead,
        Some("complete") => BatchFilter::Complete,
        Some("succeeded") => BatchFilter::Succeeded,
        Some("cancelled") => BatchFilter::Cancelled,
        Some("all") => BatchFilter::All,
        Some(_) => {
            return super::error(
                StatusCode::BAD_REQUEST,
                "unknown state: use active, dead, complete, succeeded, cancelled or all",
            );
        }
    };
    let page = query.page.unwrap_or(1).max(1);
    let count = query.count.unwrap_or(DEFAULT_PAGE).clamp(1, MAX_PAGE);
    let text = query.substr.as_deref().filter(|text| !text.is_empty());
    match state.api.batches().search(&filter, text, page, count).await {
        Ok(found) => Json(json!({
            "total": found.page.total,
            "page": found.page.number,
            "count": found.page.size,
            "searched": found.searched,
            "truncated": found.truncated,
            "batches": found.page.items.iter().map(summary_json).collect::<Vec<_>>(),
        }))
        .into_response(),
        Err(error) => super::unavailable(&error),
    }
}

/// The `{bid}` of a batch's routes.
#[derive(Debug, Deserialize)]
struct BatchPath {
    bid: String,
}

/// 24 lowercase hexadecimal characters.
fn valid_bid(bid: &str) -> bool {
    bid.len() == 24
        && bid
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn not_found() -> Response {
    super::error(
        StatusCode::NOT_FOUND,
        "no such batch: it never existed, expired, or was deleted",
    )
}

/// `GET /batches/{bid}`.
async fn show(
    State(state): State<Arc<BatchesState>>,
    path: Result<Path<BatchPath>, PathRejection>,
) -> Response {
    let path = match path {
        Ok(Path(path)) => path,
        Err(rejection) => return super::bad_path(&rejection),
    };
    if !valid_bid(&path.bid) {
        return not_found();
    }
    let batches = state.api.batches();
    let status = match batches.fetch(&path.bid).await {
        Ok(Some(status)) => status,
        Ok(None) => return not_found(),
        Err(hench::Error::Batch(error @ BatchError::Unsupported { .. })) => {
            return super::error(StatusCode::CONFLICT, error.to_string());
        }
        Err(error) => return super::unavailable(&error),
    };
    let failures = match batches.failures(&path.bid, 1, FAILURES).await {
        Ok(failures) => failures,
        Err(error) => return super::unavailable(&error),
    };
    let mut value = status_json(&status);
    value["failures"] = failures.items.iter().map(failure_json).collect();
    value["failures_total"] = json!(failures.total);
    Json(value).into_response()
}

/// `POST /batches/{bid}/cancel`.
async fn cancel(
    State(state): State<Arc<BatchesState>>,
    path: Result<Path<BatchPath>, PathRejection>,
) -> Response {
    if state.read_only {
        return super::error(StatusCode::FORBIDDEN, "the API is read-only");
    }
    let path = match path {
        Ok(Path(path)) => path,
        Err(rejection) => return super::bad_path(&rejection),
    };
    if !valid_bid(&path.bid) {
        return not_found();
    }
    match state.api.batches().cancel(&path.bid).await {
        Ok(cancelled) => {
            tracing::info!(bid = %path.bid, cancelled, "JSON API cancelled a batch");
            Json(json!({"cancelled": cancelled})).into_response()
        }
        Err(hench::Error::Batch(BatchError::NotFound { .. })) => not_found(),
        Err(hench::Error::Batch(error @ BatchError::Succeeded { .. })) => {
            super::error(StatusCode::CONFLICT, error.to_string())
        }
        Err(error) => super::unavailable(&error),
    }
}
