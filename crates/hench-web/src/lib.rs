//! # hench-web
//!
//! The [hench](https://docs.rs/hench) dashboard as an [`axum::Router`]: live statistics and
//! history, the processes and the jobs they run, queues, retries, scheduled jobs and dead
//! jobs, periodic schedules, batches, rate limits and the locks of unique jobs, with the
//! actions to manage them (retry, enqueue now, delete, kill, pause queues, quiet and stop
//! processes, pause schedules, cancel batches, reset limits, release locks), and execution
//! metrics per job class. The tabs of the features and the metrics are on by default; each has
//! a switch that turns it off ([`HenchWeb::periodic`], [`HenchWeb::batches`],
//! [`HenchWeb::limits`], [`HenchWeb::locks`], [`HenchWeb::metrics`]). [Routes](#routes) lists
//! every path the router serves.
//!
//! ```no_run
//! use hench_web::HenchWeb;
//!
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let redis = hench::RedisPool::connect("redis://127.0.0.1/").await?;
//! let app = axum::Router::new()
//!     // ... your routes ...
//!     .nest_service("/hench", hench_web::router(HenchWeb::new(&redis)));
//! let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
//! axum::serve(listener, app).await?;
//! # Ok(()) }
//! ```
//!
//! The router works at the root of a server or nested at any path (even several levels
//! deep, or under a path with parameters): every link, form and asset URL is computed per
//! request from axum's [`OriginalUri`](axum::extract::OriginalUri). Mount it with
//! [`Router::nest_service`](axum::Router::nest_service), which routes both `/hench` and
//! `/hench/` to the dashboard's home page (a proxy configured with `location /hench/`
//! redirects `/hench` there). [`Router::nest`](axum::Router::nest) works too, but it does not
//! route `/hench/`, and axum refuses it under a path whose first segment is a parameter.
//!
//! ## Authentication
//!
//! The dashboard has none of its own; it is the host application's job, so wrap the router
//! in whatever your application uses, for example a middleware:
//!
//! ```no_run
//! use axum::extract::Request;
//! use axum::http::StatusCode;
//! use axum::middleware::{self, Next};
//! use axum::response::{IntoResponse, Response};
//!
//! async fn require_admin(request: Request, next: Next) -> Response {
//!     // Check a session cookie, a header set by your proxy, ...
//!     let is_admin = request.headers().contains_key("x-admin");
//!     if !is_admin {
//!         return StatusCode::UNAUTHORIZED.into_response();
//!     }
//!     next.run(request).await
//! }
//!
//! # fn demo(redis: hench::RedisPool) -> axum::Router {
//! let dashboard = hench_web::router(hench_web::HenchWeb::new(&redis))
//!     .layer(middleware::from_fn(require_admin));
//! axum::Router::new().nest_service("/hench", dashboard)
//! # }
//! ```
//!
//! The `hench` binary of the `hench-server` crate serves the dashboard standalone, with HTTP
//! Basic authentication.
//!
//! Listening on a loopback address is no substitute: a web page on another site can point its
//! own host name at `127.0.0.1` (DNS rebinding) and then use the dashboard as if it were its
//! own origin. So the dashboard answers only requests for `localhost`, `*.localhost` and IP
//! addresses, names nobody can re-point, unless told the names it is reached under with
//! [`HenchWeb::allowed_hosts`] (see [`AllowedHosts`]; [`require_host`] applies the same check
//! to routes of your own):
//!
//! ```no_run
//! use hench_web::{AllowedHosts, HenchWeb};
//!
//! # fn demo(redis: hench::RedisPool) -> axum::Router {
//! let dashboard = HenchWeb::new(&redis).allowed_hosts(AllowedHosts::new(["jobs.example.com"]));
//! axum::Router::new().nest_service("/hench", hench_web::router(dashboard))
//! # }
//! ```
//!
//! ## Security
//!
//! * Requests for host names the dashboard does not answer for are refused (`403`) before
//!   anything else ([`HenchWeb::allowed_hosts`]).
//! * Every page is rendered by askama templates with HTML escaping; job arguments, class
//!   names, queue names and error messages are always escaped.
//! * Every state-changing request (`POST`) must carry the CSRF token of the page it was
//!   sent from (double-submit: a random `HttpOnly`, `SameSite=Lax` cookie scoped to the
//!   dashboard's path, compared in constant time with a hidden form field) and must not come
//!   from another site (`Sec-Fetch-Site`, else `Origin`).
//! * Responses carry a strict `Content-Security-Policy` (no inline scripts or styles, no
//!   external resources), `X-Content-Type-Options: nosniff`, `Referrer-Policy: same-origin`
//!   and `X-Frame-Options: SAMEORIGIN`. Pages are never cached; the embedded assets are
//!   served under content-hashed names with immutable caching.
//! * [`HenchWeb::read_only`] hides every action and refuses every `POST` with `403`.
//! * Query parameters are validated and bounded (page sizes below 1000, filters up to 256
//!   characters); a filter is literal text (`*` stands for any run of characters) matched by
//!   the dashboard over at most 50,000 jobs, never a Redis glob.
//!
//! ## Pages of your own
//!
//! Like Sidekiq's web extensions, an application can add pages to the dashboard: routes given
//! to [`HenchWeb::routes`] are served by it, behind its guard (host names, read-only mode,
//! cross-site refusal, security headers); their handlers render in its layout with [`Page`]
//! (links, navigation with the tab of [`HenchWeb::tab`] highlighted, totals, notices) and read
//! forms, checking its CSRF token, with [`Form`]. [`HenchWeb::job_info`] adds rows to the job
//! pages, such as links to a job's logs ([`JobInfoRow`]).
//!
//! ```no_run
//! use axum::response::Response;
//! use axum::routing::get;
//! use hench_web::{HenchWeb, JobInfoRow, Page, escape_html};
//!
//! async fn reports(page: Page) -> Response {
//!     let title = "Monthly reports";
//!     let content = format!("<section class=\"panel\"><h1>{}</h1></section>", escape_html(title));
//!     page.render(title, &content).await
//! }
//!
//! # fn demo(redis: hench::RedisPool) -> axum::Router {
//! let dashboard = HenchWeb::new(&redis)
//!     .tab("Reports", "reports")
//!     .routes(axum::Router::new().route("/reports", get(reports)))
//!     .job_info(|job| {
//!         let jid = job.jid().unwrap_or_default();
//!         vec![JobInfoRow::link("Logs", "Logs", format!("https://logs.example.com/{jid}"))]
//!     });
//! axum::Router::new().nest_service("/hench", hench_web::router(dashboard))
//! # }
//! ```
//!
//! ## Routes
//!
//! Relative to where the router is mounted. Pages answer `GET`; the paths marked `POST` take
//! the actions, which carry the page's CSRF token and are refused in read-only mode
//! ([`HenchWeb::read_only`]) like every other `POST`:
//!
//! * `/`: the dashboard (`HEAD /` is a cheap health check), with the JSON it polls, `/stats`
//!   and `/stats/queues` (`/dashboard/stats` redirects to `/stats`);
//! * `/busy` (`POST`: quiet or stop processes);
//! * `/queues`, `/queues/{name}` (`POST`: pause, resume, set or clear the queue's limits, or
//!   delete the queue) and
//!   `POST /queues/{name}/delete` (one of its jobs);
//! * `/retries`, `/scheduled` and `/morgue` (`POST`: the selected jobs), each job at
//!   `/retries/{key}`, `/scheduled/{key}` and `/morgue/{key}` (`POST`: that job), and the
//!   whole sets at `POST /retries/all/{retry|delete|kill}`,
//!   `POST /scheduled/all/{add_to_queue|delete}` and `POST /morgue/all/{retry|delete}`. Entry
//!   keys are `"{score}-{jid}"`, as in Sidekiq's dashboard;
//! * `/metrics` and `/metrics/{class}`: the execution metrics the workers record (see
//!   [`hench::metrics`]; [`HenchWeb::metrics`] turns them off);
//! * the tabs of the features, mounted unless their switch turns them off (their paths then
//!   answer `404`):
//!   * `/periodic` and `/periodic/{name}` (`POST`: enqueue now, pause, resume or delete
//!     schedules; [`HenchWeb::periodic`]),
//!   * `/batches` and `/batches/{bid}` (`POST`: cancel or delete the batch, skip one of its
//!     jobs, push its callbacks; [`HenchWeb::batches`]),
//!   * `/limits` and `/limits/{kind}/{id}` (`POST`: reset limits, clear a wait line;
//!     [`HenchWeb::limits`]),
//!   * `/locks`, `/locks/{digest}` and `POST /locks/all/release` (`POST`: release locks;
//!     [`HenchWeb::locks`]);
//! * `/assets/{file}`: the stylesheets and scripts, under content-hashed names.
//!
//! Every other path answers the dashboard's `404` page, unless it is one of the routes given
//! to [`HenchWeb::routes`].

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]

mod assets;
mod config;
mod csrf;
mod ctx;
mod error;
mod extension;
mod flash;
mod form;
mod format;
mod guard;
mod hosts;
mod pages;
mod params;
mod search;
mod strings;
mod templates;
mod views;

pub use crate::config::{HenchWeb, Tab};
pub use crate::extension::{Form, JobInfoRow, Page, escape_html};
pub use crate::hosts::{AllowedHosts, InvalidHostName, require_host};

/// The dashboard's router: mount it at the root of a server or nest it at any path with
/// [`axum::Router::nest_service`] (see the crate documentation). Its state is already
/// applied, so it merges into any router. Routes added to it are not behind the dashboard's
/// guard (host names, read-only mode, cross-site refusal, security headers): give pages of
/// your own to [`HenchWeb::routes`] instead.
pub fn router(web: HenchWeb) -> axum::Router {
    pages::router(web)
}
