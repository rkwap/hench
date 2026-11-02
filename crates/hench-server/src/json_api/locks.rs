//! The JSON API of the locks of unique jobs:
//!
//! * `GET /locks?class=&limit=`: `{"size", "locks": [...], "truncated", "last_hour"}`: the
//!   first `limit` live locks (default 50, at most 1000), soonest expiry first, of the job class
//!   `class` if given (a filtered listing reads at most 50,000 locks), and the last hour's
//!   `duplicates`, `replaced` and `lapsed` counters (of the class, if given);
//! * `GET /locks/{digest}`: one lock, with every field of its hash (`fields`) and where its
//!   holder is (`holder_place`); `404` when no job holds it;
//! * `DELETE /locks/{digest}?jid=J`: releases the lock if job `J` holds it (`?force=true`:
//!   whoever holds it): `200 {"released": true}`; `404` when no job holds it; `409 {"error",
//!   "holder"}` when another job does (it was taken again meanwhile). Refused with `403` when
//!   the server is read-only.
//!
//! A lock is `{"digest", "jid", "class", "queue", "until", "unique_for" (seconds),
//! "locked_at", "expires_at", "scheduled_at"}`, times in RFC 3339.

use std::sync::Arc;
use std::time::SystemTime;

use axum::extract::rejection::{PathRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use hench::api::Api;
use hench::metrics::Period;
use http::StatusCode;
use serde::Deserialize;
use serde_json::json;

use crate::commands::locks::{LockInfo, counts_json, listing, place};

/// Locks a listing answers at most.
const MAX_LIMIT: u32 = 1_000;

/// Locks a listing answers by default.
const DEFAULT_LIMIT: u32 = 50;

/// What the handlers share.
struct LocksState {
    api: Api,
    read_only: bool,
}

/// The routes, mounted under the API's prefix (`read_only`: refuse changes).
pub(crate) fn routes(api: &Api, read_only: bool) -> Router {
    let state = Arc::new(LocksState {
        api: api.clone(),
        read_only,
    });
    Router::new()
        .route("/locks", get(list))
        .route("/locks/{digest}", get(show).delete(release))
        .with_state(state)
}

/// The query of `GET /locks`.
#[derive(Debug, Deserialize)]
struct ListQuery {
    class: Option<String>,
    limit: Option<u32>,
}

/// The `{digest}` of a lock's route.
#[derive(Debug, Deserialize)]
struct LockDigest {
    digest: String,
}

/// The query of `DELETE /locks/{digest}`.
#[derive(Debug, Deserialize)]
struct ReleaseQuery {
    jid: Option<String>,
    #[serde(default)]
    force: bool,
}

/// A malformed query string, as a JSON error.
fn bad_query(rejection: &QueryRejection) -> Response {
    super::error(StatusCode::BAD_REQUEST, rejection.body_text())
}

/// No job holds the lock.
fn not_held() -> Response {
    super::error(StatusCode::NOT_FOUND, "no unique job holds this lock")
}

/// `GET /locks`.
async fn list(
    State(state): State<Arc<LocksState>>,
    query: Result<Query<ListQuery>, QueryRejection>,
) -> Response {
    let query = match query {
        Ok(Query(query)) => query,
        Err(rejection) => return bad_query(&rejection),
    };
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT);
    if !(1..=MAX_LIMIT).contains(&limit) {
        return super::error(
            StatusCode::BAD_REQUEST,
            format!("`limit` must be between 1 and {MAX_LIMIT}"),
        );
    }
    let class = query.class.as_deref().filter(|class| !class.is_empty());
    let found = match listing(
        &state.api,
        class,
        usize::try_from(limit).unwrap_or(usize::MAX),
    )
    .await
    {
        Ok(found) => found,
        Err(failure) => return super::unavailable(&failure),
    };
    let events = match state.api.unique_locks().events(Period::Minutes(60)).await {
        Ok(events) => events,
        Err(failure) => return super::unavailable(&failure),
    };
    let counts = match class {
        Some(class) => events.by_class.get(class).copied().unwrap_or_default(),
        None => events.totals,
    };
    let locks: Vec<LockInfo> = found.locks.iter().map(LockInfo::from).collect();
    Json(json!({
        "size": found.size,
        "locks": locks,
        "truncated": found.truncated,
        "last_hour": counts_json(&counts),
    }))
    .into_response()
}

/// `GET /locks/{digest}`.
async fn show(
    State(state): State<Arc<LocksState>>,
    path: Result<Path<LockDigest>, PathRejection>,
) -> Response {
    let digest = match path {
        Ok(Path(LockDigest { digest })) => digest,
        Err(rejection) => return super::bad_path(&rejection),
    };
    let locks = state.api.unique_locks();
    let lock = match locks.get(&digest).await {
        Ok(Some(lock)) => lock,
        Ok(None) => return not_held(),
        Err(failure) => return super::unavailable(&failure),
    };
    let found = match locks.locate(&lock).await {
        Ok(found) => found,
        Err(failure) => return super::unavailable(&failure),
    };
    let (_, holder_place) = place(&found, &lock, SystemTime::now());
    let mut value = match serde_json::to_value(LockInfo::from(&lock)) {
        Ok(value) => value,
        Err(failure) => {
            return super::error(StatusCode::INTERNAL_SERVER_ERROR, failure.to_string());
        }
    };
    value["holder_place"] = holder_place;
    value["fields"] = json!(lock.fields);
    Json(value).into_response()
}

/// `DELETE /locks/{digest}`.
async fn release(
    State(state): State<Arc<LocksState>>,
    path: Result<Path<LockDigest>, PathRejection>,
    query: Result<Query<ReleaseQuery>, QueryRejection>,
) -> Response {
    if state.read_only {
        return super::error(StatusCode::FORBIDDEN, "the server is read-only");
    }
    let digest = match path {
        Ok(Path(LockDigest { digest })) => digest,
        Err(rejection) => return super::bad_path(&rejection),
    };
    let query = match query {
        Ok(Query(query)) => query,
        Err(rejection) => return bad_query(&rejection),
    };
    let locks = state.api.unique_locks();
    let jid = query.jid.filter(|jid| !jid.is_empty());
    let released = match (&jid, query.force) {
        (Some(_), true) => {
            return super::error(StatusCode::BAD_REQUEST, "give `jid` or `force`, not both");
        }
        (Some(jid), false) => locks.release(&digest, jid).await,
        (None, true) => locks.force_release(&digest).await,
        (None, false) => {
            return super::error(
                StatusCode::BAD_REQUEST,
                "give `jid` (the job whose lock to release) or `force=true` (whoever holds it)",
            );
        }
    };
    match released {
        Ok(true) => {
            tracing::info!(digest, jid, "JSON API released a unique lock");
            Json(json!({"released": true, "digest": digest})).into_response()
        }
        Ok(false) => match locks.get(&digest).await {
            Ok(Some(lock)) => (
                StatusCode::CONFLICT,
                Json(json!({
                    "error": "another job holds this lock",
                    "holder": lock.jid,
                })),
            )
                .into_response(),
            Ok(None) => not_held(),
            Err(failure) => super::unavailable(&failure),
        },
        Err(failure) => super::unavailable(&failure),
    }
}
