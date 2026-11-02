//! [`HenchWeb`]: what the dashboard shows and allows.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use hench::api::{Api, JobRecord};

use crate::extension::{JobInfo, JobInfoRow};
use crate::hosts::AllowedHosts;

/// The dashboard's configuration. Build it from anything that converts into a
/// [`hench::api::Api`] (an `Api`, a [`hench::RedisPool`], a [`hench::Client`] or a
/// [`&Hench`](hench::Hench)), adjust it with the builder methods, then turn it into an
/// [`axum::Router`] with [`router`](crate::router) (or [`HenchWeb::into_router`]).
///
/// ```no_run
/// use std::time::Duration;
/// use hench_web::HenchWeb;
///
/// # async fn demo(redis: hench::RedisPool) {
/// let dashboard = HenchWeb::new(&redis)
///     .title("Acme jobs")
///     .app_url("/admin")
///     .poll_interval(Duration::from_secs(3))
///     .tab("Reports", "/admin/reports");
/// let app = axum::Router::new().nest_service("/jobs", hench_web::router(dashboard));
/// # let _: axum::Router = app;
/// # }
/// ```
///
/// Jobs are moved (retried, enqueued, killed) through the `Api`'s client, so its client
/// middleware runs; create the dashboard from [`&Hench`](hench::Hench) to use your
/// application's middleware and morgue limits, and its death handlers for killed jobs.
#[derive(Clone)]
pub struct HenchWeb {
    pub(crate) api: Api,
    pub(crate) read_only: bool,
    pub(crate) poll_interval: Duration,
    pub(crate) title: String,
    pub(crate) app_url: Option<String>,
    pub(crate) tabs: Vec<Tab>,
    pub(crate) page_size: usize,
    /// Whether the Metrics tab and pages are shown ([`HenchWeb::metrics`]).
    pub(crate) metrics: bool,
    /// Whether the Periodic tab and pages are shown ([`HenchWeb::periodic`]).
    pub(crate) periodic: bool,
    /// Whether the Batches tab and pages are shown ([`HenchWeb::batches`]).
    pub(crate) batches: bool,
    /// Whether the Limits tab and pages are shown ([`HenchWeb::limits`]).
    pub(crate) limits: bool,
    /// Whether the Locks tab and pages are shown ([`HenchWeb::locks`]).
    pub(crate) locks: bool,
    /// The host names served ([`HenchWeb::allowed_hosts`]).
    pub(crate) hosts: AllowedHosts,
    /// The host's routes ([`HenchWeb::routes`]), merged into the router when it is built.
    pub(crate) extensions: Option<Router>,
    /// The rows the job pages add ([`HenchWeb::job_info`]).
    pub(crate) job_info: Vec<JobInfo>,
}

impl HenchWeb {
    /// The default refresh interval of live views.
    pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(5);

    /// The shortest refresh interval accepted.
    pub const MIN_POLL_INTERVAL: Duration = Duration::from_secs(1);

    /// The longest refresh interval accepted.
    pub const MAX_POLL_INTERVAL: Duration = Duration::from_secs(60);

    /// The default number of rows per page.
    pub const DEFAULT_PAGE_SIZE: usize = 25;

    /// The largest page size (also the largest `?count=` accepted).
    pub const MAX_PAGE_SIZE: usize = crate::params::MAX_COUNT;

    /// The default product name shown in the header and the window title.
    pub const DEFAULT_TITLE: &'static str = "Hench";

    /// A dashboard on `api`'s Redis: not read-only, refreshing every 5 seconds, 25 rows per
    /// page, titled "Hench", with the Metrics pages and the Periodic, Batches, Limits and Locks
    /// tabs (each has a switch: [`HenchWeb::metrics`], [`HenchWeb::periodic`],
    /// [`HenchWeb::batches`], [`HenchWeb::limits`], [`HenchWeb::locks`]), answering requests for
    /// `localhost` and IP addresses only (see [`HenchWeb::allowed_hosts`]).
    pub fn new(api: impl Into<Api>) -> Self {
        Self {
            api: api.into(),
            read_only: false,
            poll_interval: Self::DEFAULT_POLL_INTERVAL,
            title: Self::DEFAULT_TITLE.to_owned(),
            app_url: None,
            tabs: Vec::new(),
            page_size: Self::DEFAULT_PAGE_SIZE,
            metrics: true,
            periodic: true,
            batches: true,
            limits: true,
            locks: true,
            hosts: AllowedHosts::local(),
            extensions: None,
            job_info: Vec::new(),
        }
    }

    /// The host names the dashboard answers for (a defense against DNS rebinding, see
    /// [`AllowedHosts`]); requests for other names get `403 Forbidden`. By default only
    /// `localhost`, `*.localhost` and IP addresses: name the ones your users reach the
    /// dashboard under, or allow every name ([`AllowedHosts::any`]) when every request is
    /// authenticated by credentials a page on another site cannot use (cookies, HTTP Basic).
    ///
    /// ```no_run
    /// use hench_web::{AllowedHosts, HenchWeb};
    ///
    /// # fn demo(redis: hench::RedisPool) {
    /// let dashboard = HenchWeb::new(&redis)
    ///     .allowed_hosts(AllowedHosts::new(["jobs.example.com"]));
    /// # }
    /// ```
    #[must_use]
    pub fn allowed_hosts(mut self, hosts: AllowedHosts) -> Self {
        self.hosts = hosts;
        self
    }

    /// Read-only mode: every button and form is hidden and every POST is refused with
    /// `403 Forbidden`, so the dashboard can be shown to people who must not change anything.
    #[must_use]
    pub fn read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }

    /// How often live views refresh by default (the dashboard's charts, and list pages with
    /// "Live" switched on); viewers can change it with the dashboard's slider, which covers
    /// the same range. Rounded to whole seconds and clamped to 1–60 seconds.
    #[must_use]
    pub fn poll_interval(mut self, interval: Duration) -> Self {
        let rounded = Duration::from_secs(interval.as_secs_f64().round() as u64);
        self.poll_interval = rounded.clamp(Self::MIN_POLL_INTERVAL, Self::MAX_POLL_INTERVAL);
        self
    }

    /// The product name in the header and the window title (default "Hench"). A blank title
    /// keeps the default.
    #[must_use]
    pub fn title(mut self, title: impl Into<String>) -> Self {
        let title = title.into();
        let title = title.trim();
        self.title = if title.is_empty() {
            Self::DEFAULT_TITLE.to_owned()
        } else {
            title.to_owned()
        };
        self
    }

    /// Adds a "Back to app" link to the header, pointing at `url`: an absolute path
    /// (`/admin`) or an `http(s)://` URL. Other values (such as `javascript:` URLs) are
    /// ignored with a warning.
    #[must_use]
    pub fn app_url(mut self, url: impl Into<String>) -> Self {
        let url = url.into();
        if is_safe_link(&url) {
            self.app_url = Some(url);
        } else {
            tracing::warn!(url, "ignoring an app URL that is not a path or http(s) URL");
        }
        self
    }

    /// Adds a tab to the navigation, after the built-in ones, linking to a page your
    /// application serves (Sidekiq's custom tabs). `path` is either relative to the
    /// dashboard (`"reports"` links to `<mount>/reports`, which you serve with
    /// [`HenchWeb::routes`]) or an absolute path or `http(s)://` URL used as is. The tab is
    /// highlighted while the dashboard path starts with a relative `path`. Unsafe links (such
    /// as `javascript:` URLs) and blank names are ignored with a warning.
    #[must_use]
    pub fn tab(mut self, name: impl Into<String>, path: impl Into<String>) -> Self {
        let tab = Tab {
            name: name.into(),
            path: path.into(),
        };
        if tab.name.trim().is_empty() || !is_safe_link(&tab.path) {
            tracing::warn!(name = tab.name, path = tab.path, "ignoring an invalid tab");
        } else {
            self.tabs.push(tab);
        }
        self
    }

    /// Routes of your own, served by the dashboard (Sidekiq's web extensions): their paths
    /// are relative to where it is mounted (`/reports` is `<mount>/reports`; link a tab to it
    /// with [`HenchWeb::tab`]), and they go through everything that guards the dashboard's
    /// own pages: the host-name check, the refusal of `POST`s (and other unsafe methods) in
    /// read-only mode or from other sites, and its security headers (a strict content
    /// security policy, `Cache-Control: no-store` unless a response sets its own caching).
    /// Their handlers render pages in the dashboard's layout with [`Page`](crate::Page) and
    /// read forms, checking the CSRF token, with [`Form`](crate::Form):
    ///
    /// ```no_run
    /// use axum::response::{Redirect, Response};
    /// use axum::routing::get;
    /// use hench_web::{Form, HenchWeb, Page};
    ///
    /// async fn page(page: Page) -> Response {
    ///     let form = if page.is_read_only() {
    ///         String::new()
    ///     } else {
    ///         format!(
    ///             "<form method=\"post\">{}<button class=\"btn\" name=\"rebuild\">Rebuild</button></form>",
    ///             page.csrf_field()
    ///         )
    ///     };
    ///     page.render("Reports", &format!("<section class=\"panel\"><h1>Reports</h1>{form}</section>"))
    ///         .await
    /// }
    ///
    /// async fn rebuild(page: Page, form: Form) -> Redirect {
    ///     if form.has("rebuild") {
    ///         // ...
    ///     }
    ///     Redirect::to(&page.url("/reports"))
    /// }
    ///
    /// # fn demo(redis: hench::RedisPool) -> axum::Router {
    /// let dashboard = HenchWeb::new(&redis)
    ///     .tab("Reports", "reports")
    ///     .routes(axum::Router::new().route("/reports", get(page).post(rebuild)));
    /// axum::Router::new().nest_service("/hench", hench_web::router(dashboard))
    /// # }
    /// ```
    ///
    /// Several calls add up. Routes added to the router [`router`](crate::router) returns
    /// are not guarded: give them here. As with [`axum::Router::merge`], building the
    /// router panics when a path is also one of the dashboard's (`/`, `/busy`, `/queues`,
    /// `/retries`, `/scheduled`, `/morgue`, `/metrics`, `/stats`, `/assets`, …, and
    /// `/periodic`, `/batches`, `/limits` and `/locks` while their tabs are on: see the crate
    /// documentation's list of routes), and when `routes` has a fallback (the dashboard answers
    /// unknown paths itself).
    #[must_use]
    pub fn routes(mut self, routes: Router) -> Self {
        self.extensions = Some(match self.extensions.take() {
            Some(existing) => existing.merge(routes),
            None => routes,
        });
        self
    }

    /// Rows to add to the job pages (a retry, a scheduled job, a dead job; Sidekiq's
    /// `custom_job_info_rows`), after the dashboard's own: `rows` gets each job shown and
    /// returns its rows, such as a link to the job's logs. Several calls add up.
    ///
    /// ```no_run
    /// use hench_web::{HenchWeb, JobInfoRow};
    ///
    /// # fn demo(redis: hench::RedisPool) {
    /// let dashboard = HenchWeb::new(&redis).job_info(|job| {
    ///     job.jid()
    ///         .map(|jid| {
    ///             let logs = format!("https://logs.example.com/jobs/{jid}");
    ///             JobInfoRow::link("Logs", format!("Logs of {jid}"), logs)
    ///         })
    ///         .into_iter()
    ///         .collect()
    /// });
    /// # }
    /// ```
    #[must_use]
    pub fn job_info<F>(mut self, rows: F) -> Self
    where
        F: Fn(&JobRecord) -> Vec<JobInfoRow> + Send + Sync + 'static,
    {
        self.job_info.push(Arc::new(rows));
        self
    }

    /// The default number of rows per page (25), between 1 and 999. Viewers can still pick
    /// another size with `?count=`.
    #[must_use]
    pub fn page_size(mut self, size: usize) -> Self {
        self.page_size = size.clamp(1, Self::MAX_PAGE_SIZE);
        self
    }

    /// Whether to show the Metrics tab and pages (on by default): the execution time, counts
    /// and failures of each job class over the last hours, from the numbers the workers
    /// record ([`hench::metrics`]). Turn it off when the workers run with
    /// [`HenchBuilder::metrics(false)`](hench::HenchBuilder::metrics): the pages would stay
    /// empty. When off, the pages answer `404`.
    #[must_use]
    pub fn metrics(mut self, enabled: bool) -> Self {
        self.metrics = enabled;
        self
    }

    /// Whether to show the Periodic tab and pages (on by default): the periodic schedules
    /// ([`hench::periodic`]). When off, the tab is not shown and the pages answer `404`.
    #[must_use]
    pub fn periodic(mut self, enabled: bool) -> Self {
        self.periodic = enabled;
        self
    }

    /// Whether to show the Batches tab and pages (on by default): the batches
    /// ([`hench::batch`]). When off, the tab is not shown and the pages answer `404`.
    #[must_use]
    pub fn batches(mut self, enabled: bool) -> Self {
        self.batches = enabled;
        self
    }

    /// Whether to show the Limits tab and pages (on by default): the limiters and throttles
    /// ([`hench::limit`]). When off, the tab is not shown and the pages answer `404`.
    #[must_use]
    pub fn limits(mut self, enabled: bool) -> Self {
        self.limits = enabled;
        self
    }

    /// Whether to show the Locks tab and pages (on by default): the locks of unique jobs
    /// ([`hench::unique`]). When off, the tab is not shown and the pages answer `404`.
    #[must_use]
    pub fn locks(mut self, enabled: bool) -> Self {
        self.locks = enabled;
        self
    }

    /// The data API the dashboard reads and changes Redis through.
    pub fn api(&self) -> &Api {
        &self.api
    }

    /// `true` in read-only mode.
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    /// The custom tabs, in order.
    pub fn tabs(&self) -> &[Tab] {
        &self.tabs
    }

    /// `true` when the Metrics tab and pages are shown ([`HenchWeb::metrics`]).
    pub fn shows_metrics(&self) -> bool {
        self.metrics
    }

    /// The host names the dashboard answers for ([`HenchWeb::allowed_hosts`]).
    pub fn hosts(&self) -> &AllowedHosts {
        &self.hosts
    }

    /// The router serving this dashboard; the same as [`router`](crate::router).
    pub fn into_router(self) -> Router {
        crate::pages::router(self)
    }
}

impl fmt::Debug for HenchWeb {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HenchWeb")
            .field("api", &self.api)
            .field("read_only", &self.read_only)
            .field("poll_interval", &self.poll_interval)
            .field("title", &self.title)
            .field("app_url", &self.app_url)
            .field("tabs", &self.tabs)
            .field("page_size", &self.page_size)
            .field("metrics", &self.metrics)
            .field("periodic", &self.periodic)
            .field("batches", &self.batches)
            .field("limits", &self.limits)
            .field("locks", &self.locks)
            .field("hosts", &self.hosts)
            .field("routes", &self.extensions.is_some())
            .field("job_info", &self.job_info.len())
            .finish_non_exhaustive()
    }
}

/// A custom navigation tab, see [`HenchWeb::tab`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tab {
    name: String,
    path: String,
}

impl Tab {
    /// The label.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The link: relative to the dashboard, an absolute path, or a URL.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// `true` when the link is relative to the dashboard.
    pub(crate) fn is_relative(&self) -> bool {
        !self.path.starts_with('/') && !self.path.contains("://")
    }

    /// The `href` of the tab for a dashboard mounted at `prefix`.
    pub(crate) fn href(&self, prefix: &str) -> String {
        if self.is_relative() {
            format!("{prefix}/{}", self.path.trim_start_matches("./"))
        } else {
            self.path.clone()
        }
    }

    /// `true` when the dashboard path `path` (e.g. `/reports/7`) belongs to this tab.
    pub(crate) fn is_active(&self, path: &str) -> bool {
        if !self.is_relative() {
            return false;
        }
        let own = self.path.trim_start_matches("./").trim_end_matches('/');
        let path = path.trim_start_matches('/');
        !own.is_empty()
            && (path == own
                || path
                    .strip_prefix(own)
                    .is_some_and(|rest| rest.starts_with('/')))
    }
}

/// `true` for links that are safe in an `href`: absolute paths, relative paths without a
/// scheme, and `http://`/`https://` URLs. Refuses `javascript:`, `data:`, protocol-relative
/// `//host` links and anything with control characters or a backslash.
pub(crate) fn is_safe_link(link: &str) -> bool {
    if link.is_empty() || link.chars().any(|c| c.is_control() || c == '\\') {
        return false;
    }
    let lower = link.to_ascii_lowercase();
    if lower.starts_with("http://") || lower.starts_with("https://") {
        return lower.len() > "https://".len();
    }
    if lower.starts_with("//") {
        return false;
    }
    // No scheme: nothing before the first `/`, `?` or `#` may contain a colon.
    let head = lower.split(['/', '?', '#']).next().unwrap_or_default();
    !head.contains(':')
}

#[cfg(test)]
mod tests {
    use super::*;
    use hench::RedisPool;

    #[test]
    fn builder_clamps_and_validates() {
        let web = HenchWeb::new(RedisPool::detached())
            .read_only(true)
            .poll_interval(Duration::from_millis(10))
            .page_size(0)
            .title("  ")
            .app_url("javascript:alert(1)")
            .tab("", "x")
            .tab("Evil", "javascript:alert(1)")
            .tab("Reports", "reports");
        assert!(web.is_read_only());
        assert!(web.shows_metrics());
        assert!(!web.clone().metrics(false).shows_metrics());
        assert_eq!(web.poll_interval, HenchWeb::MIN_POLL_INTERVAL);
        assert_eq!(web.page_size, 1);
        assert_eq!(web.title, "Hench");
        assert_eq!(web.app_url, None);
        assert_eq!(web.tabs().len(), 1);
        assert_eq!(web.tabs()[0].name(), "Reports");
        let web = web
            .poll_interval(Duration::from_secs(3_600))
            .page_size(5_000)
            .title("Acme")
            .app_url("https://acme.example/admin");
        assert_eq!(web.poll_interval, HenchWeb::MAX_POLL_INTERVAL);
        let web = web.poll_interval(Duration::from_millis(2_600));
        assert_eq!(web.poll_interval, Duration::from_secs(3));
        assert_eq!(web.page_size, HenchWeb::MAX_PAGE_SIZE);
        assert_eq!(web.title, "Acme");
        assert_eq!(web.app_url.as_deref(), Some("https://acme.example/admin"));
        assert!(format!("{web:?}").contains("Acme"));
        // The switches of the tabs show too.
        let debug = format!("{:?}", web.locks(false));
        assert!(debug.contains("periodic: true"), "{debug}");
        assert!(debug.contains("locks: false"), "{debug}");
    }

    #[test]
    fn links_are_checked() {
        for safe in [
            "/admin",
            "reports",
            "reports/daily?x=1",
            "https://acme.example",
            "HTTP://acme.example/x",
            "a/b:c",
        ] {
            assert!(is_safe_link(safe), "{safe}");
        }
        for unsafe_link in [
            "",
            "javascript:alert(1)",
            " javascript:alert(1)",
            "JaVaScRiPt:alert(1)",
            "data:text/html,x",
            "//evil.example",
            "https://",
            "/a\\b",
            "/a\nb",
            "vbscript:x",
        ] {
            assert!(!is_safe_link(unsafe_link), "{unsafe_link:?}");
        }
    }

    #[test]
    fn tabs_link_relative_to_the_mount() {
        let tab = Tab {
            name: "Reports".into(),
            path: "reports".into(),
        };
        assert_eq!(tab.href(""), "/reports");
        assert_eq!(tab.href("/hench"), "/hench/reports");
        assert!(tab.is_active("/reports"));
        assert!(tab.is_active("/reports/7"));
        assert!(!tab.is_active("/reportsx"));
        assert!(!tab.is_active("/"));
        let absolute = Tab {
            name: "Admin".into(),
            path: "/admin".into(),
        };
        assert_eq!(absolute.href("/hench"), "/admin");
        assert!(!absolute.is_active("/admin"));
    }
}
