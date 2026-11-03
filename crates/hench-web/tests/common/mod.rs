//! Helpers shared by the dashboard's integration tests: an app on a private Redis, request
//! helpers that pass the CSRF checks the way a browser does, and data seeding.
#![allow(dead_code)]

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::Body;
use hench::api::Api;
use hench::test_util::TestRedis;
use hench::{RedisPool, redis_rs};
use hench_web::HenchWeb;
use http::header::{CONTENT_TYPE, COOKIE, LOCATION, SET_COOKIE};
use http::{HeaderMap, Request, StatusCode};
use http_body_util::BodyExt;
use scraper::{Html, Selector};
use serde_json::{Value, json};
use tower::ServiceExt;

/// A CSRF token as the dashboard issues them (64 lowercase hex characters). The token is a
/// double-submit token: any well-formed value works when the cookie and the field agree.
pub const TOKEN: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

/// A dashboard on a private Redis.
pub struct App {
    pub redis: TestRedis,
    pub pool: RedisPool,
    pub api: Api,
    pub router: Router,
    /// Where the dashboard is mounted (`""` at the root).
    pub prefix: String,
}

impl App {
    /// The dashboard at the root of the router, default settings.
    pub async fn new() -> Self {
        Self::build(|web| web, "").await
    }

    /// The dashboard configured by `configure`, mounted at `prefix` (`""`: the root).
    pub async fn build(configure: impl FnOnce(HenchWeb) -> HenchWeb, prefix: &str) -> Self {
        let redis = TestRedis::start().await;
        let pool = redis.pool().await;
        let api = Api::new(pool.clone());
        let dashboard = hench_web::router(configure(HenchWeb::new(&api)));
        let router = if prefix.is_empty() {
            dashboard
        } else {
            Router::new().nest(prefix, dashboard)
        };
        Self {
            redis,
            pool,
            api,
            router,
            prefix: prefix.to_owned(),
        }
    }

    /// A dashboard path under the mount prefix.
    pub fn path(&self, path: &str) -> String {
        if path == "/" && !self.prefix.is_empty() {
            self.prefix.clone()
        } else {
            format!("{}{path}", self.prefix)
        }
    }

    /// Sends a request to the router.
    pub async fn send(&self, request: Request<Body>) -> Res {
        let response = self
            .router
            .clone()
            .oneshot(request)
            .await
            .expect("the router is infallible");
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("response body")
            .to_bytes();
        Res {
            status,
            headers,
            body: String::from_utf8_lossy(&bytes).into_owned(),
        }
    }

    /// `GET path` (under the mount prefix).
    pub async fn get(&self, path: &str) -> Res {
        self.send(
            Request::get(self.path(path))
                .body(Body::empty())
                .expect("request"),
        )
        .await
    }

    /// A same-origin form POST to `path`, carrying the CSRF token like a browser would.
    pub async fn post(&self, path: &str, fields: &[(&str, &str)]) -> Res {
        self.send(post_request(&self.path(path), fields, TOKEN, TOKEN))
            .await
    }

    /// A raw Redis command.
    pub async fn redis<T: redis_rs::FromRedisValue>(&self, command: &redis_rs::Cmd) -> T {
        let mut conn = self.pool.conn().await.expect("redis connection");
        command.query_async(&mut conn).await.expect("redis command")
    }

    /// The product the test's server is, which the dashboard names: Redis, or a
    /// Redis-compatible server that announces itself with its own version field in `INFO`.
    pub async fn store_name(&self) -> &'static str {
        let info: String = self.redis(redis_rs::cmd("INFO").arg("server")).await;
        if info.contains("dragonfly_version:") {
            "Dragonfly"
        } else if info.contains("valkey_version:") {
            "Valkey"
        } else {
            "Redis"
        }
    }
}

/// A form POST with `cookie_token` in the cookie and `field_token` in the form.
pub fn post_request(
    uri: &str,
    fields: &[(&str, &str)],
    cookie_token: &str,
    field_token: &str,
) -> Request<Body> {
    let mut body = form_urlencoded::Serializer::new(String::new());
    body.append_pair("_csrf", field_token);
    for (key, value) in fields {
        body.append_pair(key, value);
    }
    Request::post(uri)
        .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
        .header(COOKIE, format!("hench_csrf={cookie_token}"))
        .header("sec-fetch-site", "same-origin")
        .body(Body::from(body.finish()))
        .expect("request")
}

/// A response, read to the end.
pub struct Res {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: String,
}

impl Res {
    /// The parsed HTML.
    pub fn html(&self) -> Html {
        Html::parse_document(&self.body)
    }

    /// The text of every element matching `selector`.
    pub fn texts(&self, selector: &str) -> Vec<String> {
        let selector = Selector::parse(selector).expect("selector");
        self.html()
            .select(&selector)
            .map(|element| element.text().collect::<String>().trim().to_owned())
            .collect()
    }

    /// The attribute `attribute` of every element matching `selector`.
    pub fn attrs(&self, selector: &str, attribute: &str) -> Vec<String> {
        let selector = Selector::parse(selector).expect("selector");
        self.html()
            .select(&selector)
            .filter_map(|element| element.value().attr(attribute).map(str::to_owned))
            .collect()
    }

    /// How many elements match `selector`.
    pub fn count(&self, selector: &str) -> usize {
        let selector = Selector::parse(selector).expect("selector");
        self.html().select(&selector).count()
    }

    /// The `<title>`.
    pub fn title(&self) -> String {
        self.texts("title").concat()
    }

    /// The `Location` header.
    pub fn location(&self) -> &str {
        self.headers
            .get(LOCATION)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
    }

    /// The `Set-Cookie` headers.
    pub fn cookies(&self) -> Vec<String> {
        self.headers
            .get_all(SET_COOKIE)
            .iter()
            .filter_map(|value| value.to_str().ok().map(str::to_owned))
            .collect()
    }

    /// The `Set-Cookie` header of the cookie `name`.
    pub fn cookie(&self, name: &str) -> Option<String> {
        self.cookies()
            .into_iter()
            .find(|cookie| cookie.starts_with(&format!("{name}=")))
    }

    /// A header as text.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|value| value.to_str().ok())
    }

    /// The body as JSON.
    pub fn json(&self) -> Value {
        serde_json::from_str(&self.body).expect("JSON body")
    }

    /// Panics with the body unless the status is `status`.
    #[track_caller]
    pub fn assert_status(&self, status: StatusCode) -> &Self {
        assert_eq!(self.status, status, "body: {}", self.body);
        self
    }
}

/// Seconds since the epoch, as a float (sorted-set scores).
pub fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs_f64()
}

/// Milliseconds since the epoch (payload timestamps).
pub fn now_millis() -> i64 {
    (now_secs() * 1000.0) as i64
}

/// A random-enough jid (24 hex characters).
pub fn jid() -> String {
    hench::generate_jid()
}

/// Adds a job to a sorted set and returns it with its score.
pub async fn add_to_set(app: &App, set: &str, job: Value, score: f64) -> (Value, f64) {
    let _: i64 = app
        .redis(
            redis_rs::cmd("ZADD")
                .arg(set)
                .arg(score)
                .arg(job.to_string()),
        )
        .await;
    (job, score)
}

/// Sidekiq's `add_retry`: a failed `HardJob` in the retry set, due now.
pub async fn add_retry(app: &App, jid: &str) -> (Value, f64) {
    let job = json!({
        "class": "HardJob",
        "args": ["bob", 1, now_secs()],
        "queue": "default",
        "error_message": "Some fake message",
        "error_class": "RuntimeError",
        "retry_count": 0,
        "failed_at": now_millis(),
        "jid": jid,
    });
    add_to_set(app, "retry", job, now_secs()).await
}

/// Sidekiq's `add_scheduled`: a tagged `HardJob` in the schedule.
pub async fn add_scheduled(app: &App, jid: &str) -> (Value, f64) {
    let job = json!({
        "class": "HardJob",
        "args": ["bob", 1, now_secs()],
        "jid": jid,
        "queue": "default",
        "tags": ["tag1", "tag2"],
    });
    add_to_set(app, "schedule", job, now_secs() + 3_600.0).await
}

/// Sidekiq's `add_dead`: a dead `HardJob` of queue `foo`.
pub async fn add_dead(app: &App, jid: &str) -> (Value, f64) {
    let job = json!({
        "class": "HardJob",
        "args": ["bob", 1, now_secs()],
        "queue": "foo",
        "error_message": "Some fake message",
        "error_class": "RuntimeError",
        "retry_count": 0,
        "failed_at": now_millis(),
        "jid": jid,
    });
    add_to_set(app, "dead", job, now_secs()).await
}

/// The URL key of a sorted-set entry (`score-jid`), as the dashboard writes it.
pub fn key(job: &Value, score: f64) -> String {
    format!("{score}-{}", job["jid"].as_str().unwrap_or_default())
}

/// Pushes a raw job to `queue` (and registers the queue).
pub async fn push_raw(app: &App, queue: &str, job: &Value) {
    let _: i64 = app
        .redis(
            redis_rs::cmd("LPUSH")
                .arg(format!("queue:{queue}"))
                .arg(job.to_string()),
        )
        .await;
    let _: i64 = app
        .redis(redis_rs::cmd("SADD").arg("queues").arg(queue))
        .await;
}

/// Registers a fake process `identity` with heartbeat data (Sidekiq's busy test setup).
pub async fn add_process(app: &App, identity: &str, info: Value, busy: u64) {
    let _: i64 = app
        .redis(redis_rs::cmd("SADD").arg("processes").arg(identity))
        .await;
    let _: i64 = app
        .redis(
            redis_rs::cmd("HSET")
                .arg(identity)
                .arg("info")
                .arg(info.to_string())
                .arg("busy")
                .arg(busy)
                .arg("beat")
                .arg(now_secs())
                .arg("quiet")
                .arg("false"),
        )
        .await;
}

/// Records a running job for `identity` in slot `slot`.
pub async fn add_work(app: &App, identity: &str, slot: &str, queue: &str, payload: &Value) {
    let work = json!({"queue": queue, "payload": payload.to_string(), "run_at": now_secs()});
    let _: i64 = app
        .redis(
            redis_rs::cmd("HSET")
                .arg(format!("{identity}:work"))
                .arg(slot)
                .arg(work.to_string()),
        )
        .await;
}

/// Pops the oldest signal sent to `identity`.
pub async fn pop_signal(app: &App, identity: &str) -> Option<String> {
    app.redis(redis_rs::cmd("RPOP").arg(format!("{identity}-signals")))
        .await
}

/// `ZCARD` of a set.
pub async fn zcard(app: &App, set: &str) -> u64 {
    app.redis(redis_rs::cmd("ZCARD").arg(set)).await
}

/// `LLEN` of a queue.
pub async fn llen(app: &App, queue: &str) -> u64 {
    app.redis(redis_rs::cmd("LLEN").arg(format!("queue:{queue}")))
        .await
}
