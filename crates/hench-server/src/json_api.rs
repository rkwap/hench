//! The JSON API of `hench web --api`, under `<prefix>/api/v1`:
//!
//! * `GET /stats`: the totals (`processed`, `failed`, `busy`, `processes`, `enqueued`,
//!   `scheduled`, `retries`, `dead`, `default_latency`);
//! * `GET /queues`: `[{name, size, latency, paused, limits}]`, by name (`limits`: the queue's
//!   limits as stored, `concurrency=3;window=100/1m`, or `null`);
//! * `PUT /queues/{name}/limits`: `{"limits": "concurrency=3; window=100/1m"}` sets the limits
//!   on every fetch of the queue (`hench::api::Queue::set_limits`) and answers `{"queue",
//!   "limits"}` with the stored form; limits that are not valid answer `400`;
//! * `DELETE /queues/{name}/limits`: clears them, `{"cleared": <whether it had some>}`;
//! * `GET /processes`: the live processes, by identity, as `hench processes --json` prints
//!   them: `identity`, `hostname`, `pid`, `tag`, `labels`, `started_at`, `beat` (RFC 3339,
//!   `null` when unknown), `concurrency`, `busy`, `quiet`, `embedded`, `rss_kb`, `version`,
//!   `queues` (the names of the queues it works on, each once, in its configured order),
//!   `capsules` (each capsule by name: `{"concurrency", "mode", "weights": {queue: weight}}`,
//!   `mode` `strict`, `weighted` or `random`, weights `0` when strict) and `features` (the
//!   stored-format version of each pro feature the process understands);
//! * `POST /jobs`: pushes a job, `{class, args?, queue?, at? (epoch seconds) | in? (seconds),
//!   retry?, backtrace?, dead?, tags?, retry_for?, retry_queue?, jid?, custom?, unique?,
//!   throttle?, throttle_key?}`, and answers `201 {"jid": ...}`. A push by class name: the
//!   server knows no job types, so a job without `queue` goes to `default` with the default
//!   options, whatever its job type declares. Requests are read strictly: an unknown field,
//!   in the body or in `unique`, and an unknown `unique.until` or `unique.on_conflict` are
//!   refused (`422`);
//!
//! and the routes of the features, each described in its module:
//!
//! * [`periodic`]: `GET /periodic`, `GET /periodic/{name}`,
//!   `POST /periodic/{name}/{enqueue|pause|unpause}`;
//! * [`batches`]: `GET /batches`, `GET /batches/{bid}`, `POST /batches/{bid}/cancel`;
//! * [`limits`]: `GET /limits`, `GET /limits/{kind}/{id}`,
//!   `DELETE /limits/{kind}/{id}` (a reset);
//! * [`locks`]: `GET /locks`, `GET /locks/{digest}`, `DELETE /locks/{digest}` (a release).
//!
//! When the server is read-only, every change (`POST`, `PUT`, `DELETE`) answers `403`, like a
//! change a browser sends from another site.
//!
//! Errors are JSON too: `{"error": "..."}`, with the status that says what went wrong: `400`
//! for a malformed query or path (a path parameter that is not UTF-8 once percent-decoded) and
//! for queue limits that are not valid,
//! `404` for a path the API does not serve, `405` for a method a route does not take (with
//! the `Allow` header), `422` for a request that is not a valid job, `503` when Redis fails.

use std::sync::Arc;
use std::time::SystemTime;

use axum::extract::Path;
use axum::extract::rejection::{JsonRejection, PathRejection};
use axum::extract::{Request, State};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use hench::api::{Api, Stats};
use hench::limit::ThrottleOption;
use hench::{
    BacktraceOption, Error, JobPayload, PushOutcome, RetryOption, UniqueOption, now_secs_f64,
};
use http::header::{CACHE_CONTROL, HOST, ORIGIN, X_CONTENT_TYPE_OPTIONS};
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer};
use serde_json::{Map, Value, json};

use crate::monitor::{ProcessInfo, QueueInfo};

// The routes of the features, one module each (public for their documentation: they export
// nothing).
pub mod batches;
pub mod limits;
pub mod locks;
pub mod periodic;

/// `10000-01-01T00:00:00Z` in seconds since the Unix epoch: jobs must be scheduled before it.
/// Later times are almost always units mistaken (milliseconds or microseconds for seconds),
/// and no calendar shows them.
pub const AT_LIMIT: f64 = 253_402_300_800.0;

/// What the API handlers share.
#[derive(Clone)]
struct ApiState {
    api: Api,
    read_only: bool,
}

/// The API's router: mount it at `<prefix>/api/v1` with `Router::nest_service`, so that every
/// path under it, the unknown ones included, gets the API's JSON answers (a `nest` would leave
/// them to the dashboard's HTML fallback when the dashboard is mounted at `<prefix>`). With
/// `read_only`, every change (`POST /jobs`, the queue limits' `PUT` and `DELETE`, and the
/// features' `POST` and `DELETE` routes) answers `403`.
pub fn router(api: Api, read_only: bool) -> Router {
    let features = Router::new()
        .merge(periodic::routes(&api, read_only))
        .merge(batches::routes(&api, read_only))
        .merge(limits::routes(&api, read_only))
        .merge(locks::routes(&api, read_only));
    let state = Arc::new(ApiState { api, read_only });
    Router::new()
        .route("/stats", get(stats))
        .route("/queues", get(queues))
        .route(
            "/queues/{name}/limits",
            put(set_queue_limits).delete(clear_queue_limits),
        )
        .route("/processes", get(processes))
        .route("/jobs", post(enqueue))
        .with_state(state)
        .merge(features)
        // After every route: it applies to the routes registered so far.
        .method_not_allowed_fallback(wrong_method)
        .fallback(no_route)
        .layer(middleware::from_fn(refuse_cross_site))
}

/// A JSON error response.
fn error(status: StatusCode, message: impl Into<String>) -> Response {
    (status, Json(json!({"error": message.into()}))).into_response()
}

/// A path parameter that could not be read (not UTF-8 once percent-decoded), as a JSON error.
fn bad_path(rejection: &PathRejection) -> Response {
    error(rejection.status(), rejection.body_text())
}

/// A path the API does not serve.
async fn no_route() -> Response {
    error(StatusCode::NOT_FOUND, "no such route in the JSON API")
}

/// A method the route does not take (axum adds the `Allow` header with those it takes).
async fn wrong_method(method: Method) -> Response {
    error(
        StatusCode::METHOD_NOT_ALLOWED,
        format!("this route does not take {method}"),
    )
}

/// A Redis failure.
fn unavailable(failure: &Error) -> Response {
    tracing::error!(error = %failure, "JSON API: Redis error");
    error(StatusCode::SERVICE_UNAVAILABLE, failure.to_string())
}

/// Adds the response headers, and refuses state-changing requests that a browser sent from
/// another site: `Sec-Fetch-Site` must be `same-origin` when present, else `Origin` (when
/// present) must name this host. Scripts and command-line clients send neither and are not
/// affected; a browser page on another site can only reach `POST /jobs` with a JSON body
/// through CORS, which this API does not allow, so this is defense in depth.
async fn refuse_cross_site(request: Request, next: Next) -> Response {
    let mut response = if request.method() != Method::GET
        && request.method() != Method::HEAD
        && is_cross_site(request.headers())
    {
        error(
            StatusCode::FORBIDDEN,
            "cross-site requests are not accepted",
        )
    } else {
        next.run(request).await
    };
    // Responses are never cached, and never sniffed as anything but what they say.
    let headers = response.headers_mut();
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    response
}

fn is_cross_site(headers: &HeaderMap) -> bool {
    if let Some(site) = headers.get("sec-fetch-site") {
        return site.as_bytes() != b"same-origin";
    }
    let Some(origin) = headers.get(ORIGIN).and_then(|value| value.to_str().ok()) else {
        return headers.contains_key(ORIGIN);
    };
    let authority = origin
        .split_once("://")
        .map(|(_, rest)| rest)
        .filter(|rest| !rest.is_empty() && !rest.contains('/'));
    let host = headers.get(HOST).and_then(|value| value.to_str().ok());
    match (authority, host) {
        (Some(authority), Some(host)) => !authority.eq_ignore_ascii_case(host),
        _ => true,
    }
}

/// `GET /stats`.
async fn stats(State(state): State<Arc<ApiState>>) -> Response {
    match state.api.stats().await {
        Ok(stats) => Json(json!({
            "processed": stats.processed,
            "failed": stats.failed,
            "busy": stats.workers_size,
            "processes": stats.processes_size,
            "enqueued": stats.enqueued,
            "scheduled": stats.scheduled_size,
            "retries": stats.retry_size,
            "dead": stats.dead_size,
            "default_latency": stats.default_queue_latency,
        }))
        .into_response(),
        Err(failure) => unavailable(&failure),
    }
}

/// `GET /queues`.
async fn queues(State(state): State<Arc<ApiState>>) -> Response {
    match Stats::queue_summaries(&state.api).await {
        Ok(mut summaries) => {
            summaries.sort_by(|a, b| a.name.cmp(&b.name));
            let queues: Vec<QueueInfo> = summaries
                .into_iter()
                .map(|summary| QueueInfo {
                    name: summary.name,
                    size: summary.size,
                    latency: summary.latency,
                    paused: summary.paused,
                    limits: summary.limits,
                })
                .collect();
            Json(queues).into_response()
        }
        Err(failure) => unavailable(&failure),
    }
}

/// The body of `PUT /queues/{name}/limits`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LimitsBody {
    /// The limits in the spec grammar, joined by `;`.
    limits: String,
}

/// The `{name}` of a queue route.
#[derive(Debug, Deserialize)]
struct QueuePath {
    name: String,
}

/// `PUT /queues/{name}/limits`.
async fn set_queue_limits(
    State(state): State<Arc<ApiState>>,
    path: Result<Path<QueuePath>, PathRejection>,
    body: Result<Json<LimitsBody>, JsonRejection>,
) -> Response {
    if state.read_only {
        return error(StatusCode::FORBIDDEN, "the API is read-only");
    }
    let name = match path {
        Ok(Path(QueuePath { name })) => name,
        Err(rejection) => return bad_path(&rejection),
    };
    let body = match body {
        Ok(Json(body)) => body,
        Err(rejection) => return error(rejection.status(), rejection.body_text()),
    };
    match state
        .api
        .queue(&name)
        .set_limits_from_str(&body.limits)
        .await
    {
        Ok(stored) => {
            tracing::info!(
                queue = %name.escape_debug(),
                limits = %stored,
                "JSON API set the limits of a queue"
            );
            Json(json!({"queue": name, "limits": stored})).into_response()
        }
        Err(Error::Config(message)) => error(StatusCode::BAD_REQUEST, message),
        Err(failure) => unavailable(&failure),
    }
}

/// `DELETE /queues/{name}/limits`.
async fn clear_queue_limits(
    State(state): State<Arc<ApiState>>,
    path: Result<Path<QueuePath>, PathRejection>,
) -> Response {
    if state.read_only {
        return error(StatusCode::FORBIDDEN, "the API is read-only");
    }
    let name = match path {
        Ok(Path(QueuePath { name })) => name,
        Err(rejection) => return bad_path(&rejection),
    };
    match state.api.queue(&name).clear_limits().await {
        Ok(cleared) => {
            tracing::info!(queue = %name.escape_debug(), "JSON API cleared the limits of a queue");
            Json(json!({"cleared": cleared})).into_response()
        }
        Err(failure) => unavailable(&failure),
    }
}

/// `GET /processes`.
async fn processes(State(state): State<Arc<ApiState>>) -> Response {
    match state.api.processes().list().await {
        Ok(processes) => {
            let now = SystemTime::now();
            let processes: Vec<ProcessInfo> = processes
                .iter()
                .map(|process| ProcessInfo::new(process, now))
                .collect();
            Json(processes).into_response()
        }
        Err(failure) => unavailable(&failure),
    }
}

/// The body of `POST /jobs`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnqueueRequest {
    /// The job's class (its registered name).
    pub class: String,
    /// Positional arguments.
    #[serde(default)]
    pub args: Vec<Value>,
    /// The queue (default: `default`, whatever the job type declares: the server knows no
    /// job types).
    #[serde(default)]
    pub queue: Option<String>,
    /// Run at this time (seconds since the Unix epoch).
    #[serde(default)]
    pub at: Option<f64>,
    /// Run after this many seconds.
    #[serde(default, rename = "in")]
    pub delay: Option<f64>,
    /// Retries: `true`, `false` or a maximum.
    #[serde(default)]
    pub retry: Option<RetryOption>,
    /// Keep error backtraces: `true`, `false` or a number of lines.
    #[serde(default)]
    pub backtrace: Option<BacktraceOption>,
    /// `false`: no dead set when retries are exhausted.
    #[serde(default)]
    pub dead: Option<bool>,
    /// Tags shown in the dashboard.
    #[serde(default)]
    pub tags: Vec<String>,
    /// Time-based retry limit, in seconds.
    #[serde(default)]
    pub retry_for: Option<u64>,
    /// The queue for retries.
    #[serde(default)]
    pub retry_queue: Option<String>,
    /// An explicit job id (default: a new one).
    #[serde(default)]
    pub jid: Option<String>,
    /// Extra keys stored with the job (for middleware).
    #[serde(default)]
    pub custom: Map<String, Value>,
    /// Uniqueness: `false`, or `{"for": seconds, "until", "on_conflict", "across_queues",
    /// "key", "digest"}` (see `hench::unique`), read strictly: an unknown field or policy is
    /// refused.
    #[serde(default, deserialize_with = "strict_unique")]
    pub unique: Option<UniqueOption>,
    /// The throttle of the job's starts.
    #[serde(default)]
    pub throttle: Option<String>,
    /// The throttle key (with `throttle`).
    #[serde(default)]
    pub throttle_key: Option<String>,
}

/// The fields `unique` may hold in a request.
const UNIQUE_FIELDS: [&str; 6] = [
    "for",
    "until",
    "on_conflict",
    "across_queues",
    "key",
    "digest",
];

/// The `unique` field of a request, read strictly. [`UniqueOption`] reads stored jobs, and
/// leniently: unknown fields are ignored, and a policy it does not know (one a newer version
/// wrote) reads as `success`, so that no stored job becomes unreadable. At the API's door that
/// leniency would turn a typo into other lock semantics (`"until": "expire"` held as `success`:
/// released when the job succeeds, not at the end of its window), so a request's `unique` must
/// be `false`, `null` or an object of the documented fields only, whose `until` (when given) is
/// `success`, `start` or `expired`, before `UniqueOption` reads it.
fn strict_unique<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<UniqueOption>, D::Error> {
    let value = Value::deserialize(deserializer)?;
    match &value {
        Value::Null => return Ok(None),
        Value::Object(object) => {
            if let Some(unknown) = object
                .keys()
                .find(|field| !UNIQUE_FIELDS.contains(&field.as_str()))
            {
                return Err(D::Error::custom(format!(
                    "unknown field `{unknown}` in `unique`, expected one of `{}`",
                    UNIQUE_FIELDS.join("`, `")
                )));
            }
            match object.get("until") {
                None | Some(Value::Null) => {}
                Some(Value::String(name)) if hench::UniqueUntil::parse(name).is_some() => {}
                Some(other) => {
                    return Err(D::Error::custom(format!(
                        "`{}` is not a unique policy for `until` (success, start or expired)",
                        other
                            .as_str()
                            .map_or_else(|| other.to_string(), str::to_owned)
                    )));
                }
            }
        }
        _ => {}
    }
    UniqueOption::deserialize(value)
        .map(Some)
        .map_err(D::Error::custom)
}

impl EnqueueRequest {
    /// The job to push, or why the request is invalid.
    pub fn into_payload(self) -> Result<JobPayload, String> {
        let at = match (self.at, self.delay) {
            (Some(_), Some(_)) => return Err("give `at` or `in`, not both".to_owned()),
            (Some(at), None) => Some(at),
            (None, Some(delay)) if delay.is_finite() && delay >= 0.0 => {
                Some(now_secs_f64() + delay)
            }
            (None, Some(_)) => return Err("`in` must be a number of seconds".to_owned()),
            (None, None) => None,
        };
        if at.is_some_and(|at| at.is_nan() || at >= AT_LIMIT) {
            return Err(
                "`at` and `in` must give a time before the year 10000 (`at` is in \
                        seconds since the Unix epoch, not milliseconds)"
                    .to_owned(),
            );
        }
        if let Some(key) = self
            .custom
            .keys()
            .find(|key| JobPayload::is_reserved_key(key))
        {
            return Err(format!("`custom` may not set the reserved key `{key}`"));
        }
        // An empty queue or jid would mean "the default" to the client: refuse them when given
        // explicitly, like an empty queue in job options.
        if self
            .queue
            .as_deref()
            .is_some_and(|queue| queue.trim().is_empty())
        {
            return Err("`queue` must not be empty".to_owned());
        }
        if self.jid.as_deref().is_some_and(|jid| jid.trim().is_empty()) {
            return Err("`jid` must not be empty".to_owned());
        }
        // Names end up in URLs, logs and terminal reports: no control characters.
        let names = [
            ("class", Some(self.class.as_str())),
            ("queue", self.queue.as_deref()),
            ("retry_queue", self.retry_queue.as_deref()),
            ("jid", self.jid.as_deref()),
        ];
        let tags = self.tags.iter().map(|tag| ("tags", Some(tag.as_str())));
        if let Some((field, _)) = names
            .into_iter()
            .chain(tags)
            .find(|(_, value)| value.is_some_and(|value| value.chars().any(char::is_control)))
        {
            return Err(format!("`{field}` must not contain control characters"));
        }
        let throttle = match (self.throttle, self.throttle_key) {
            (None, Some(_)) => return Err("`throttle_key` needs a `throttle`".to_owned()),
            (Some(name), _) if name.trim().is_empty() => {
                return Err("`throttle` must not be empty".to_owned());
            }
            (Some(name), key) => Some(ThrottleOption::Enabled {
                name: Some(name),
                key,
            }),
            (None, None) => None,
        };
        let mut job = JobPayload::new(self.class, self.args);
        job.unique = self.unique;
        job.throttle = throttle;
        job.queue = self.queue.unwrap_or_default();
        job.jid = self.jid.unwrap_or_default();
        job.at = at;
        job.retry = self.retry;
        job.backtrace = self.backtrace;
        job.dead = self.dead;
        job.tags = self.tags;
        job.retry_for = self.retry_for;
        job.retry_queue = self.retry_queue;
        job.extra = self.custom;
        Ok(job)
    }
}

/// `POST /jobs`.
async fn enqueue(
    State(state): State<Arc<ApiState>>,
    body: Result<Json<EnqueueRequest>, JsonRejection>,
) -> Response {
    if state.read_only {
        return error(StatusCode::FORBIDDEN, "the server is read-only");
    }
    let request = match body {
        Ok(Json(request)) => request,
        Err(rejection) => return error(rejection.status(), rejection.body_text()),
    };
    let job = match request.into_payload() {
        Ok(job) => job,
        Err(message) => return error(StatusCode::BAD_REQUEST, message),
    };
    let class = job.class.clone();
    match state.api.client().push_outcome(job).await {
        Ok(PushOutcome::Pushed { jid }) => {
            tracing::info!(class, jid, "JSON API pushed a job");
            (StatusCode::CREATED, Json(json!({"jid": jid}))).into_response()
        }
        Ok(PushOutcome::Replaced { jid, replaced }) => {
            tracing::info!(
                class,
                jid,
                replaced,
                "JSON API pushed a job in place of another"
            );
            (
                StatusCode::CREATED,
                Json(json!({"jid": jid, "replaced": replaced})),
            )
                .into_response()
        }
        Ok(PushOutcome::Duplicate { holder, .. }) => (
            StatusCode::OK,
            Json(json!({"jid": null, "duplicate_of": holder})),
        )
            .into_response(),
        Ok(_) => (StatusCode::OK, Json(json!({"jid": null}))).into_response(),
        Err(Error::DuplicateJob { holder, .. }) => error(
            StatusCode::CONFLICT,
            format!("a job with the same unique key is held by {holder}"),
        ),
        Err(Error::InvalidJob(message)) => error(StatusCode::BAD_REQUEST, message),
        Err(Error::Json(failure)) => error(StatusCode::BAD_REQUEST, failure.to_string()),
        Err(failure) => unavailable(&failure),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    #[test]
    fn requests_become_payloads() {
        let request: EnqueueRequest = serde_json::from_value(json!({
            "class": "Mailer", "args": [1, "a"], "queue": "mail", "in": 60,
            "retry": 3, "tags": ["t"], "custom": {"tenant": "acme"}
        }))
        .unwrap();
        let job = request.into_payload().unwrap();
        assert_eq!(job.class, "Mailer");
        assert_eq!(job.args, [json!(1), json!("a")]);
        assert_eq!(job.queue, "mail");
        assert!(job.at.unwrap() > now_secs_f64() + 50.0);
        assert_eq!(job.retry, Some(RetryOption::Count(3)));
        assert_eq!(job.extra["tenant"], "acme");

        let both: EnqueueRequest =
            serde_json::from_value(json!({"class": "X", "at": 1.0, "in": 1.0})).unwrap();
        assert!(both.into_payload().is_err());
        let reserved: EnqueueRequest =
            serde_json::from_value(json!({"class": "X", "custom": {"queue": "x"}})).unwrap();
        assert!(reserved.into_payload().is_err());
        let negative: EnqueueRequest =
            serde_json::from_value(json!({"class": "X", "in": -5})).unwrap();
        assert!(negative.into_payload().is_err());
        assert!(
            serde_json::from_value::<EnqueueRequest>(json!({"class": "X", "bogus": 1})).is_err()
        );
        assert!(serde_json::from_value::<EnqueueRequest>(json!({"args": []})).is_err());
    }

    #[test]
    fn unique_is_read_strictly() {
        let parse = |unique: Value| {
            serde_json::from_value::<EnqueueRequest>(json!({"class": "X", "unique": unique}))
        };
        for unique in [
            json!({"for": 60, "until": "expire"}),
            json!({"for": 60, "until": "Success"}),
            json!({"for": 60, "until": ""}),
            json!({"for": 60, "until": true}),
            json!({"for": 60, "untill": "expired"}),
            json!({"for": 60, "acrossQueues": true}),
            json!({"for": 60, "on_conflict": "eror"}),
            json!(true),
            json!("yes"),
        ] {
            assert!(parse(unique.clone()).is_err(), "{unique}");
        }
        let every = parse(
            json!({"for": 60.5, "until": "expired", "on_conflict": "replace",
                                 "across_queues": true, "key": [1], "digest": "abc"}),
        )
        .unwrap();
        let unique = every
            .unique
            .as_ref()
            .and_then(UniqueOption::settings)
            .unwrap();
        assert_eq!(unique.ttl, Some(std::time::Duration::from_secs(60)));
        assert_eq!(unique.until, Some(hench::UniqueUntil::Expired));
        assert_eq!(unique.on_conflict, Some(hench::OnConflict::Replace));
        assert_eq!(unique.across_queues, Some(true));
        assert_eq!(unique.key, Some(json!([1])));
        assert_eq!(unique.digest.as_deref(), Some("abc"));
        let unset = parse(json!({"for": 60, "until": null})).unwrap();
        let unique = unset
            .unique
            .as_ref()
            .and_then(UniqueOption::settings)
            .unwrap();
        assert_eq!(unique.until, None);
        assert_eq!(parse(json!(null)).unwrap().unique, None);
        assert_eq!(
            parse(json!(false)).unwrap().unique,
            Some(UniqueOption::Disabled)
        );
        let missing: EnqueueRequest = serde_json::from_value(json!({"class": "X"})).unwrap();
        assert_eq!(missing.unique, None);
    }

    #[test]
    fn cross_site_requests_are_recognized() {
        let headers = |pairs: &[(&'static str, &'static str)]| {
            let mut map = HeaderMap::new();
            for (name, value) in pairs {
                map.insert(*name, HeaderValue::from_static(value));
            }
            map
        };
        assert!(!is_cross_site(&headers(&[])));
        assert!(!is_cross_site(&headers(&[(
            "sec-fetch-site",
            "same-origin"
        )])));
        assert!(is_cross_site(&headers(&[("sec-fetch-site", "cross-site")])));
        assert!(!is_cross_site(&headers(&[
            ("origin", "http://localhost:7420"),
            ("host", "localhost:7420")
        ])));
        assert!(is_cross_site(&headers(&[
            ("origin", "https://evil.example"),
            ("host", "localhost:7420")
        ])));
        assert!(is_cross_site(&headers(&[
            ("origin", "null"),
            ("host", "x")
        ])));
    }
}
