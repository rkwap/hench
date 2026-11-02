//! Pages of your own inside the dashboard (Sidekiq's web extensions): routes given to
//! [`HenchWeb::routes`](crate::HenchWeb::routes) are served behind the dashboard's guard, and
//! their handlers render in its layout with [`Page`], read forms with [`Form`] and add rows to
//! the job pages with [`JobInfoRow`].

use std::fmt;
use std::sync::Arc;

use axum::extract::{FromRequest, FromRequestParts, Request};
use axum::response::{IntoResponse, Response};
use hench::api::{Api, JobRecord};
use http::StatusCode;
use http::request::Parts;

use crate::config::{HenchWeb, is_safe_link};
use crate::ctx::Ctx;
use crate::form::PostForm;
use crate::strings::EN;
use crate::templates::ExtensionPage;
use crate::views::Totals;

/// The dashboard behind a request, set by its guard for the routes it serves (the extractors
/// of [`HenchWeb::routes`](crate::HenchWeb::routes) handlers read it).
#[derive(Clone)]
pub(crate) struct Dashboard(pub(crate) Arc<HenchWeb>);

/// What a handler outside the dashboard's router gets from [`Page`] and [`Form`].
fn outside() -> Response {
    tracing::error!(
        "hench_web::Page and hench_web::Form only work in the routes given to \
         HenchWeb::routes"
    );
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        "This page is not part of the dashboard.\n",
    )
        .into_response()
}

/// The dashboard's context in the handler of a route of your own
/// ([`HenchWeb::routes`](crate::HenchWeb::routes)): its links, the CSRF token of its forms,
/// and pages in its layout (header, navigation with your tab highlighted, totals, notices,
/// footer, styles and scripts).
///
/// ```no_run
/// use axum::routing::get;
/// use hench_web::{HenchWeb, Page, escape_html};
///
/// async fn reports(page: Page) -> axum::response::Response {
///     let queue = "mailers";
///     let content = format!(
///         "<section class=\"panel\"><h1>Reports</h1><p><a href=\"{}\">{}</a></p></section>",
///         page.url("/queues/mailers"),
///         escape_html(queue),
///     );
///     page.render("Reports", &content).await
/// }
///
/// # fn demo(redis: hench::RedisPool) -> axum::Router {
/// let dashboard = HenchWeb::new(&redis)
///     .tab("Reports", "reports")
///     .routes(axum::Router::new().route("/reports", get(reports)));
/// axum::Router::new().nest_service("/hench", hench_web::router(dashboard))
/// # }
/// ```
pub struct Page {
    ctx: Ctx,
}

impl<S: Send + Sync> FromRequestParts<S> for Page {
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let Some(Dashboard(web)) = parts.extensions.get::<Dashboard>().cloned() else {
            return Err(outside());
        };
        Ctx::from_parts(parts, &web)
            .map(|ctx| Self { ctx })
            .map_err(IntoResponse::into_response)
    }
}

impl Page {
    /// The URL of a dashboard path: `page.url("/reports")` is `/hench/reports` for a
    /// dashboard mounted at `/hench`.
    pub fn url(&self, path: &str) -> String {
        self.ctx.url(path)
    }

    /// Where the dashboard is mounted: `""` at the root, else a path like `/hench`.
    pub fn prefix(&self) -> &str {
        &self.ctx.prefix
    }

    /// The path of the request within the dashboard (`/reports/7`).
    pub fn path(&self) -> &str {
        &self.ctx.path
    }

    /// The CSRF token every form posting to the dashboard carries in its `_csrf` field (see
    /// [`Page::csrf_field`] and [`Form`]).
    pub fn csrf_token(&self) -> &str {
        &self.ctx.csrf
    }

    /// The hidden `_csrf` field of a form: `<input type="hidden" name="_csrf" value="…">`.
    pub fn csrf_field(&self) -> String {
        format!(
            "<input type=\"hidden\" name=\"{}\" value=\"{}\">",
            crate::csrf::FIELD,
            self.ctx.csrf
        )
    }

    /// `true` when the dashboard is read-only: show no forms (their POSTs are refused anyway).
    pub fn is_read_only(&self) -> bool {
        self.ctx.web.read_only
    }

    /// The dashboard's data API.
    pub fn api(&self) -> &Api {
        self.ctx.web.api()
    }

    /// A `200` page in the dashboard's layout, with `title` in the window title and
    /// `content_html` as its content, inserted as it is: it is your HTML, so escape every
    /// value you did not write ([`escape_html`]). The dashboard's content security policy
    /// forbids inline scripts and styles: link scripts and stylesheets your routes serve
    /// (`<script src=…>`, `<link rel="stylesheet" href=…>`, in the content), and use the
    /// dashboard's classes (`panel`, `panel-head`, `btn`, `table-wrap`, …). The totals strip
    /// is left out while Redis does not answer.
    pub async fn render(&self, title: &str, content_html: &str) -> Response {
        let totals = match self.ctx.web.api().stats().await {
            Ok(stats) => Some(Totals::new(&stats)),
            Err(error) => {
                tracing::debug!(%error, path = self.ctx.path, "no totals for a host page");
                None
            }
        };
        let page = ExtensionPage {
            layout: crate::pages::layout(&self.ctx, Some(title), totals, false),
            s: &EN,
            content: content_html,
        };
        crate::pages::render(&self.ctx, StatusCode::OK, &page)
    }
}

impl fmt::Debug for Page {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Page")
            .field("prefix", &self.ctx.prefix)
            .field("path", &self.ctx.path)
            .finish_non_exhaustive()
    }
}

/// The fields of a form posted to a route of your own, accepted only with the dashboard's
/// CSRF token: its `_csrf` field ([`Page::csrf_field`]) must match the token cookie, else the
/// request is refused with the dashboard's `403` page before your handler runs, as are bodies
/// that are not `application/x-www-form-urlencoded` (`415`). (Cross-site and read-only
/// refusals happen before, for every route of the dashboard.)
#[derive(Debug, Default)]
pub struct Form {
    fields: PostForm,
}

impl Form {
    /// The first value of `name`.
    pub fn get(&self, name: &str) -> Option<&str> {
        self.fields.get(name)
    }

    /// `true` if the form has a field `name` (such as a pressed submit button).
    pub fn has(&self, name: &str) -> bool {
        self.fields.has(name)
    }

    /// Every value of `name`, in order.
    pub fn all<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a str> + 'a {
        self.fields.all(name)
    }
}

impl<S: Send + Sync> FromRequest<S> for Form {
    type Rejection = Response;

    async fn from_request(request: Request, _state: &S) -> Result<Self, Self::Rejection> {
        let Some(Dashboard(web)) = request.extensions().get::<Dashboard>().cloned() else {
            return Err(outside());
        };
        PostForm::from_request(request, &web)
            .await
            .map(|fields| Self { fields })
    }
}

/// A row the job pages (a retry, a scheduled job, a dead job) show for a job, added by
/// [`HenchWeb::job_info`](crate::HenchWeb::job_info) (Sidekiq's `custom_job_info_rows`): a
/// label and a text, or a link.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobInfoRow {
    pub(crate) label: String,
    pub(crate) text: String,
    pub(crate) href: Option<String>,
}

impl JobInfoRow {
    /// A row showing `text`.
    pub fn text(label: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            text: text.into(),
            href: None,
        }
    }

    /// A row showing `text` as a link to `href`: an absolute path or an `http(s)://` URL
    /// (other links, such as `javascript:` ones, are shown as text, with a warning).
    pub fn link(
        label: impl Into<String>,
        text: impl Into<String>,
        href: impl Into<String>,
    ) -> Self {
        let href = href.into();
        let safe = is_safe_link(&href) && (href.starts_with('/') || href.contains("://"));
        if !safe {
            tracing::warn!(
                href,
                "showing a job info link that is not a path or URL as text"
            );
        }
        Self {
            label: label.into(),
            text: text.into(),
            href: safe.then_some(href),
        }
    }

    /// The label.
    pub fn label(&self) -> &str {
        &self.label
    }

    /// The text.
    pub fn value(&self) -> &str {
        &self.text
    }

    /// The link, if the row is one.
    pub fn href(&self) -> Option<&str> {
        self.href.as_deref()
    }
}

/// What [`HenchWeb::job_info`](crate::HenchWeb::job_info) keeps: the rows of a job.
pub(crate) type JobInfo = Arc<dyn Fn(&JobRecord) -> Vec<JobInfoRow> + Send + Sync>;

/// `text` escaped for HTML text and attribute values (`&`, `<`, `>`, `"` and `'`), for the
/// content of [`Page::render`].
pub fn escape_html(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#x27;"),
            c => escaped.push(c),
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn html_is_escaped() {
        assert_eq!(
            escape_html(r#"<a href="x" onclick='y'>&amp;</a>"#),
            "&lt;a href=&quot;x&quot; onclick=&#x27;y&#x27;&gt;&amp;amp;&lt;/a&gt;"
        );
        assert_eq!(escape_html("plain"), "plain");
    }

    #[test]
    fn job_info_links_must_be_paths_or_urls() {
        let row = JobInfoRow::link("Logs", "logs", "https://logs.example/j/1");
        assert_eq!(row.href(), Some("https://logs.example/j/1"));
        assert_eq!(
            JobInfoRow::link("Logs", "logs", "/logs/1").href(),
            Some("/logs/1")
        );
        for unsafe_link in ["javascript:alert(1)", "//evil.example", "relative/path", ""] {
            let row = JobInfoRow::link("Logs", "logs", unsafe_link);
            assert_eq!(row.href(), None, "{unsafe_link:?}");
            assert_eq!(row.value(), "logs");
        }
        let text = JobInfoRow::text("Tenant", "acme");
        assert_eq!(
            (text.label(), text.value(), text.href()),
            ("Tenant", "acme", None)
        );
    }
}
