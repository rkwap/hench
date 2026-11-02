//! `hench web`: the dashboard (and optionally the JSON API) as a standalone server.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use axum::Router;
use axum::extract::{Request, State};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::get;
use hench::api::Api;
use hench::{RedisConfig, RedisPool};
use hench_web::HenchWeb;
use http::StatusCode;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

use crate::auth::{self, Credentials};
use crate::cli::{self, HEALTH_PATH, WebArgs};
use crate::connections::{self, Limits};
use crate::hosts::AllowedHosts;
use crate::json_api;

/// Reconnection attempts of the server's Redis connection per command. Workers make several
/// to ride out a failover; a page or a health check should rather fail within moments while
/// Redis is down (the next request tries again).
const RECONNECT_RETRIES: usize = 1;

/// How long `/healthz` waits for Redis before answering `503`.
pub const HEALTH_TIMEOUT: Duration = Duration::from_secs(1);

/// The validated settings of `hench web`.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// The address to listen on.
    pub bind: SocketAddr,
    /// Basic authentication, if required.
    pub credentials: Option<Credentials>,
    /// Dashboard and API refuse changes.
    pub read_only: bool,
    /// The path everything is served under (`""` or `/path`).
    pub prefix: String,
    /// Serve the JSON API.
    pub api: bool,
    /// The dashboard's title.
    pub title: String,
    /// The dashboard's default refresh interval.
    pub poll_interval: Duration,
    /// The host names served besides `localhost` and IP addresses (see [`crate::hosts`]);
    /// `None` when every name is served: with credentials and no `--allowed-host`, or with
    /// `--allowed-host '*'`.
    pub allowed_hosts: Option<AllowedHosts>,
    /// The connection limit and the timeouts of each phase of a connection.
    pub limits: Limits,
}

impl ServerConfig {
    /// Validates the command-line arguments: a user name needs a password (and the other
    /// way around), the JSON API, which can push jobs, is only served without credentials on
    /// a loopback address, and the prefix is a path other than the health check's. Without
    /// credentials only `localhost`, IP addresses and the `--allowed-host` names are served
    /// (DNS rebinding); with credentials, every name unless `--allowed-host` lists some. The
    /// limits are the defaults ([`Limits::DEFAULT`]) with `--max-connections`.
    pub fn from_args(args: &WebArgs) -> anyhow::Result<Self> {
        // Checked again for arguments built in code rather than parsed.
        let prefix = cli::parse_prefix(&args.prefix).map_err(anyhow::Error::msg)?;
        let credentials = match (&args.username, &args.password) {
            (Some(username), Some(password)) => {
                if username.is_empty() || password.is_empty() {
                    bail!("--username and --password must not be empty");
                }
                Some(Credentials::new(username, password))
            }
            (None, None) => None,
            _ => bail!("--username and --password go together"),
        };
        if args.api && credentials.is_none() && !args.bind.ip().is_loopback() {
            bail!(
                "refusing to serve the JSON API on {} without authentication: set \
                 --username and --password (or HENCH_WEB_USERNAME/HENCH_WEB_PASSWORD), or \
                 listen on a loopback address",
                args.bind
            );
        }
        if credentials.is_none() && !args.bind.ip().is_loopback() {
            tracing::warn!(
                bind = %args.bind,
                "the dashboard is reachable without authentication; set --username and \
                 --password, or put an authenticating proxy in front"
            );
        }
        let every_host = args.allowed_hosts.iter().any(|host| host == "*")
            || (credentials.is_some() && args.allowed_hosts.is_empty());
        let allowed_hosts =
            (!every_host).then(|| AllowedHosts::new(args.allowed_hosts.iter().cloned()));
        Ok(Self {
            bind: args.bind,
            credentials,
            read_only: args.read_only,
            prefix,
            api: args.api,
            title: args.title.clone(),
            poll_interval: args.poll_interval,
            allowed_hosts,
            limits: Limits {
                max_connections: usize::try_from(args.max_connections)
                    .unwrap_or(Limits::DEFAULT.max_connections),
                ..Limits::DEFAULT
            },
        })
    }
}

/// The server's routes: the dashboard at `<prefix>`, the JSON API at `<prefix>/api/v1` (with
/// `api`), both behind Basic authentication when configured and refused (`403`) for host
/// names that are not allowed, and an unauthenticated `/healthz` (also at
/// `<prefix>/healthz`, whatever the host name): `200 ok` when Redis answers `PING`, else
/// `503`. With a prefix, `/` redirects to it. Request bodies are read within the
/// configuration's [`Limits::body_timeout`], once the host and the credentials are checked.
pub fn app(config: &ServerConfig, api: Api) -> Router {
    let prefix = config.prefix.as_str();
    let hosts = config
        .allowed_hosts
        .as_ref()
        .map_or_else(hench_web::AllowedHosts::any, |hosts| hosts.web().clone());
    let dashboard = hench_web::router(
        HenchWeb::new(&api)
            .read_only(config.read_only)
            .title(config.title.clone())
            .poll_interval(config.poll_interval)
            .allowed_hosts(hosts.clone()),
    );
    // `nest_service` (unlike `nest`) also routes `<prefix>/` to the dashboard's home page: a
    // proxy's `location /hench/` answers `/hench` with a redirect there.
    let mut protected = if prefix.is_empty() {
        dashboard
    } else {
        Router::new().nest_service(prefix, dashboard)
    };
    if config.api {
        // A service, like the dashboard: every path under it reaches the API's router, whose
        // own fallbacks answer unknown paths and methods in JSON.
        protected = protected.nest_service(
            &format!("{prefix}/api/v1"),
            json_api::router(api.clone(), config.read_only),
        );
    }
    // Inside the checks below: a refused request's body is never read.
    let body_timeout = config.limits.body_timeout;
    protected = protected.layer(middleware::from_fn(move |request: Request, next: Next| {
        connections::read_body(body_timeout, request, next)
    }));
    if let Some(credentials) = &config.credentials {
        protected = protected.layer(middleware::from_fn_with_state(
            credentials.clone(),
            auth::require,
        ));
    }
    // Outermost: a request for a foreign host name is refused before anything else (the
    // credentials' challenge included), for the API as for the dashboard.
    if !hosts.allows_any() {
        protected = protected.layer(middleware::from_fn_with_state(
            hosts,
            hench_web::require_host,
        ));
    }
    let mut health = Router::new().route(HEALTH_PATH, get(healthz));
    if !prefix.is_empty() {
        health = health.route(&format!("{prefix}{HEALTH_PATH}"), get(healthz));
    }
    let mut app = health.with_state(api).merge(protected);
    if !prefix.is_empty() {
        let home = prefix.to_owned();
        app = app.route(
            "/",
            get(move || {
                let home = home.clone();
                async move { Redirect::to(&home) }
            }),
        );
    }
    app.layer(middleware::from_fn(log_requests))
}

/// `GET /healthz`: `PING`, answered within [`HEALTH_TIMEOUT`].
async fn healthz(State(api): State<Api>) -> Response {
    let error = match tokio::time::timeout(HEALTH_TIMEOUT, api.redis().ping()).await {
        Ok(Ok(())) => return (StatusCode::OK, "ok\n").into_response(),
        Ok(Err(error)) => error.to_string(),
        Err(_) => format!("no answer within {HEALTH_TIMEOUT:?}"),
    };
    tracing::warn!(error, "health check: Redis is not answering");
    (StatusCode::SERVICE_UNAVAILABLE, "redis unavailable\n").into_response()
}

/// Logs each request with its status and duration (assets and health checks at debug).
async fn log_requests(request: Request, next: Next) -> Response {
    let started = Instant::now();
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let response = next.run(request).await;
    let status = response.status().as_u16();
    let millis = started.elapsed().as_secs_f64() * 1000.0;
    if path.contains("/assets/") || path.ends_with(HEALTH_PATH) {
        tracing::debug!(%method, path, status, millis, "request");
    } else {
        tracing::info!(%method, path, status, millis, "request");
    }
    response
}

/// Serves until the first message of `signals` (see [`connections::shutdown_signals`]), then
/// lets the requests in flight finish (within [`Limits::shutdown_timeout`], or until a second
/// message). Redis is contacted lazily: the server listens at once even when Redis is down
/// (pages show the error, `/healthz` answers `503`, until it answers), and each request makes
/// one reconnection attempt at most.
pub async fn serve(
    config: ServerConfig,
    redis: RedisConfig,
    signals: mpsc::UnboundedReceiver<()>,
) -> anyhow::Result<()> {
    let url = redis.redacted_url();
    let pool = RedisPool::lazy(redis.reconnect_retries(RECONNECT_RETRIES))
        .context("invalid Redis configuration")?;
    // Checked in the background: the server does not wait for Redis to listen.
    tokio::spawn({
        let pool = pool.clone();
        let url = url.clone();
        async move {
            if let Err(error) = pool.ping().await {
                tracing::warn!(redis = url, %error, "Redis is not answering yet");
            }
        }
    });
    let listener = TcpListener::bind(config.bind)
        .await
        .with_context(|| format!("cannot listen on {}", config.bind))?;
    serve_on(listener, &config, Api::new(pool), signals).await
}

/// [`serve`] on a listener already bound, with `api`'s Redis.
pub async fn serve_on(
    listener: TcpListener,
    config: &ServerConfig,
    api: Api,
    signals: mpsc::UnboundedReceiver<()>,
) -> anyhow::Result<()> {
    let address = listener.local_addr()?;
    let url = api.redis().config().redacted_url();
    let app = app(config, api);
    tracing::info!(
        address = %address,
        redis = url,
        prefix = config.prefix,
        api = config.api,
        read_only = config.read_only,
        authentication = config.credentials.is_some(),
        max_connections = config.limits.max_connections,
        "hench web listening on http://{address}{}",
        if config.prefix.is_empty() { "/" } else { &config.prefix }
    );
    connections::run(listener, app, config.limits, signals).await;
    tracing::info!("hench web stopped");
    Ok(())
}
