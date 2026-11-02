//! The per-request context of a dashboard page: where the dashboard is mounted, the CSRF
//! token, the pending notice, and how to build links, redirects and responses.

use std::sync::Arc;
use std::time::SystemTime;

use axum::extract::{FromRequestParts, OriginalUri};
use axum::response::{IntoResponse, Response};
use http::header::{CONTENT_TYPE, COOKIE, HOST, LOCATION, REFERER, SET_COOKIE};
use http::request::Parts;
use http::{HeaderMap, HeaderValue, StatusCode, Uri};

use crate::config::HenchWeb;
use crate::csrf;
use crate::flash::{self, Notice};
use crate::params;
use crate::strings::errors;

/// What every handler needs to know about the request it serves.
pub(crate) struct Ctx {
    /// The dashboard's configuration.
    pub(crate) web: Arc<HenchWeb>,
    /// The path the dashboard is mounted at: `""` at the root, else `/hench`-like (no
    /// trailing slash).
    pub(crate) prefix: String,
    /// The request path within the dashboard (`/retries`).
    pub(crate) path: String,
    /// The raw query string (without `?`), or `""`.
    pub(crate) query: String,
    /// The CSRF token forms of this page carry.
    pub(crate) csrf: String,
    /// `true` when the token is new and must be set as a cookie.
    new_csrf: bool,
    /// The notice to show, if a previous action left one.
    pub(crate) notice: Option<Notice>,
    /// `true` when the request carried a notice cookie that must be cleared.
    clear_notice: bool,
    /// `true` when the request came over HTTPS (cookies get `Secure`).
    secure: bool,
    /// The `Host` header.
    host: Option<String>,
    /// The `Referer` header.
    referer: Option<String>,
    /// When the request started.
    pub(crate) now: SystemTime,
}

/// The context cannot be built: the operating system's random source failed, so no CSRF
/// token can be issued. Answers `500`.
#[derive(Debug)]
pub(crate) struct NoRandomness;

impl IntoResponse for NoRandomness {
    fn into_response(self) -> Response {
        (StatusCode::INTERNAL_SERVER_ERROR, errors::NO_RANDOMNESS).into_response()
    }
}

impl FromRequestParts<Arc<HenchWeb>> for Ctx {
    type Rejection = NoRandomness;

    async fn from_request_parts(
        parts: &mut Parts,
        web: &Arc<HenchWeb>,
    ) -> Result<Self, Self::Rejection> {
        Ctx::from_parts(parts, web)
    }
}

impl Ctx {
    /// Builds the context from a request's head.
    pub(crate) fn from_parts(parts: &Parts, web: &Arc<HenchWeb>) -> Result<Self, NoRandomness> {
        let inner = parts.uri.path();
        let original = parts
            .extensions
            .get::<OriginalUri>()
            .map_or(inner, |original| original.0.path());
        let prefix = mount_prefix(original, inner);
        let cookies = cookies(&parts.headers);
        let existing = cookies
            .iter()
            .find(|(name, value)| name == csrf::COOKIE && csrf::is_well_formed(value))
            .map(|(_, value)| value.clone());
        let (token, new_csrf) = match existing {
            Some(token) => (token, false),
            None => match csrf::generate() {
                Ok(token) => (token, true),
                Err(error) => {
                    tracing::error!(%error, "cannot create a CSRF token");
                    return Err(NoRandomness);
                }
            },
        };
        let notice_cookie = cookies.iter().find(|(name, _)| name == flash::COOKIE);
        let header = |name| {
            parts
                .headers
                .get(name)
                .and_then(|value: &HeaderValue| value.to_str().ok())
                .map(str::to_owned)
        };
        Ok(Self {
            web: Arc::clone(web),
            path: inner.to_owned(),
            query: parts.uri.query().unwrap_or_default().to_owned(),
            prefix,
            csrf: token,
            new_csrf,
            notice: notice_cookie.and_then(|(_, value)| Notice::decode(value)),
            clear_notice: notice_cookie.is_some(),
            secure: is_https(&parts.uri, &parts.headers),
            host: header(HOST),
            referer: header(REFERER),
            now: SystemTime::now(),
        })
    }

    /// The same context without a new token cookie: for the answers to refused cross-site
    /// requests, whose new token would replace the one the forms of open pages carry (a
    /// browser accepts cookies set by top-level navigations from other sites).
    pub(crate) fn without_new_token(mut self) -> Self {
        self.new_csrf = false;
        self
    }

    /// The URL of a dashboard path (`/busy` → `/hench/busy`).
    pub(crate) fn url(&self, path: &str) -> String {
        format!("{}{path}", self.prefix)
    }

    /// The URL of the dashboard's home page.
    pub(crate) fn home(&self) -> String {
        if self.prefix.is_empty() {
            "/".to_owned()
        } else {
            self.prefix.clone()
        }
    }

    /// The `Path` attribute of the dashboard's cookies.
    fn cookie_path(&self) -> &str {
        let usable = self
            .prefix
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && !matches!(byte, b';' | b',' | b'"' | b'\\'));
        if self.prefix.is_empty() || !usable {
            "/"
        } else {
            &self.prefix
        }
    }

    /// A cookie of the dashboard. The token cookie is `SameSite=Lax`: browsers send it along
    /// with links from other sites (a chat, a ticket, an SSO redirect), so following one does
    /// not issue a new token that would replace the one the forms of open pages carry. Lax
    /// cookies never reach POSTs from other sites, which the fetch-metadata check refuses
    /// anyway. The notice cookie is only read after the dashboard's own redirects: `Strict`.
    fn cookie(&self, name: &str, value: &str, max_age: Option<u64>) -> Option<HeaderValue> {
        let same_site = if name == csrf::COOKIE {
            "Lax"
        } else {
            "Strict"
        };
        let mut cookie = format!(
            "{name}={value}; Path={}; HttpOnly; SameSite={same_site}",
            self.cookie_path()
        );
        if let Some(max_age) = max_age {
            cookie.push_str(&format!("; Max-Age={max_age}"));
        }
        if self.secure {
            cookie.push_str("; Secure");
        }
        HeaderValue::from_str(&cookie).ok()
    }

    /// Adds the cookies this request owes the browser: a new CSRF token, the removal of a
    /// shown notice.
    fn add_cookies(&self, headers: &mut HeaderMap) {
        if self.new_csrf
            && let Some(cookie) = self.cookie(csrf::COOKIE, &self.csrf, None)
        {
            headers.append(SET_COOKIE, cookie);
        }
        if self.clear_notice
            && let Some(cookie) = self.cookie(flash::COOKIE, "", Some(0))
        {
            headers.append(SET_COOKIE, cookie);
        }
    }

    /// An HTML response.
    pub(crate) fn html(&self, status: StatusCode, body: String) -> Response {
        let mut response = (status, body).into_response();
        response.headers_mut().insert(
            CONTENT_TYPE,
            HeaderValue::from_static("text/html; charset=utf-8"),
        );
        self.add_cookies(response.headers_mut());
        response
    }

    /// A `303 See Other` to `location` (a dashboard URL), leaving `notice` for the next page.
    pub(crate) fn redirect(&self, location: &str, notice: Option<Notice>) -> Response {
        let mut response = StatusCode::SEE_OTHER.into_response();
        let headers = response.headers_mut();
        match HeaderValue::from_str(location) {
            Ok(location) => {
                headers.insert(LOCATION, location);
            }
            Err(_) => {
                // Only reachable with a malformed prefix; fall back on the home page.
                if let Ok(home) = HeaderValue::from_str(&self.home()) {
                    headers.insert(LOCATION, home);
                }
            }
        }
        let cookie = match notice {
            Some(notice) => self.cookie(flash::COOKIE, &notice.encode(), Some(flash::MAX_AGE_SECS)),
            None if self.clear_notice => self.cookie(flash::COOKIE, "", Some(0)),
            None => None,
        };
        if let Some(cookie) = cookie {
            headers.append(SET_COOKIE, cookie);
        }
        response
    }

    /// Where a POST on a list goes back to: the list page (`list_path`, e.g. `/retries`),
    /// with the page, size, order and filter of the referring page when the form was sent
    /// from that same list (Sidekiq's `redirect_with_query`).
    pub(crate) fn back_to(&self, list_path: &str) -> String {
        let list = self.url(list_path);
        let query = self
            .referer
            .as_deref()
            .and_then(|referer| referer.parse::<Uri>().ok())
            .filter(|referer| self.is_same_host(referer) && referer.path() == list)
            .and_then(|referer| referer.query().map(str::to_owned));
        match query {
            Some(query) => format!("{list}{}", params::carry(&query, &[])),
            None => list,
        }
    }

    /// `true` when `uri` has no authority or names the host this request was sent to.
    fn is_same_host(&self, uri: &Uri) -> bool {
        match (uri.authority(), self.host.as_deref()) {
            (None, _) => true,
            (Some(authority), Some(host)) => authority.as_str().eq_ignore_ascii_case(host),
            (Some(_), None) => false,
        }
    }
}

/// The path a dashboard is mounted at, from the original request path and the path left
/// after axum stripped the nesting prefixes: `/hench/retries` and `/retries` give `/hench`;
/// `/hench` and `/` give `/hench`; equal paths give `""`.
///
/// A prefix can contain values of the request when the dashboard is nested under a path with
/// parameters, so it is made safe to use in links and redirects: repeated slashes collapse
/// and backslashes are percent-encoded (browsers read `/\host` like `//host`), so it never
/// starts with `//`, a link to another host.
pub(crate) fn mount_prefix(original: &str, inner: &str) -> String {
    let raw = match original.strip_suffix(inner) {
        Some(prefix) => prefix,
        None if inner == "/" || inner.is_empty() => original,
        None => "",
    };
    let mut prefix = String::with_capacity(raw.len());
    for segment in raw.split('/').filter(|segment| !segment.is_empty()) {
        prefix.push('/');
        prefix.push_str(&segment.replace('\\', "%5C"));
    }
    prefix
}

/// The `name=value` pairs of the `Cookie` headers, in order.
pub(crate) fn cookies(headers: &HeaderMap) -> Vec<(String, String)> {
    headers
        .get_all(COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(';'))
        .filter_map(|pair| {
            let (name, value) = pair.trim().split_once('=')?;
            Some((
                name.trim().to_owned(),
                value.trim().trim_matches('"').to_owned(),
            ))
        })
        .collect()
}

/// `true` when the request reached the server (or the proxy in front of it) over HTTPS.
fn is_https(uri: &Uri, headers: &HeaderMap) -> bool {
    if uri.scheme_str() == Some("https") {
        return true;
    }
    let first = |name: &str| {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(|value| {
                value
                    .split(',')
                    .next()
                    .unwrap_or_default()
                    .trim()
                    .to_owned()
            })
    };
    if let Some(proto) = first("x-forwarded-proto") {
        return proto.eq_ignore_ascii_case("https");
    }
    first("forwarded").is_some_and(|forwarded| {
        forwarded
            .split(';')
            .filter_map(|pair| pair.trim().split_once('='))
            .any(|(key, value)| {
                key.eq_ignore_ascii_case("proto")
                    && value.trim_matches('"').eq_ignore_ascii_case("https")
            })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mount_prefixes_come_from_the_original_path() {
        assert_eq!(mount_prefix("/", "/"), "");
        assert_eq!(mount_prefix("/retries", "/retries"), "");
        assert_eq!(mount_prefix("/hench", "/"), "/hench");
        assert_eq!(mount_prefix("/hench/", "/"), "/hench");
        assert_eq!(mount_prefix("/hench/retries", "/retries"), "/hench");
        assert_eq!(mount_prefix("/a/b/queues/x%2Fy", "/queues/x%2Fy"), "/a/b");
        assert_eq!(mount_prefix("//evil.example/x", "/x"), "/evil.example");
        assert_eq!(
            mount_prefix("/\\evil.example/jobs/x", "/x"),
            "/%5Cevil.example/jobs"
        );
        assert_eq!(mount_prefix("/a/b\\c/x", "/x"), "/a/b%5Cc");
        assert_eq!(mount_prefix("/odd", "/other"), "");
    }

    #[test]
    fn cookies_are_parsed() {
        let mut headers = HeaderMap::new();
        headers.append(COOKIE, HeaderValue::from_static("a=1; hench_csrf=\"x\""));
        headers.append(COOKIE, HeaderValue::from_static("b = 2;broken; c="));
        assert_eq!(
            cookies(&headers),
            [
                ("a".to_owned(), "1".to_owned()),
                ("hench_csrf".to_owned(), "x".to_owned()),
                ("b".to_owned(), "2".to_owned()),
                ("c".to_owned(), String::new()),
            ]
        );
    }

    #[test]
    fn https_is_detected_behind_proxies() {
        let uri: Uri = "/x".parse().unwrap();
        let mut headers = HeaderMap::new();
        assert!(!is_https(&uri, &headers));
        headers.insert("x-forwarded-proto", HeaderValue::from_static("https, http"));
        assert!(is_https(&uri, &headers));
        headers.insert("x-forwarded-proto", HeaderValue::from_static("http"));
        assert!(!is_https(&uri, &headers));
        let mut headers = HeaderMap::new();
        headers.insert(
            "forwarded",
            HeaderValue::from_static("for=1.2.3.4;proto=https;by=x"),
        );
        assert!(is_https(&uri, &headers));
        assert!(is_https(
            &"https://dash.example/x".parse().unwrap(),
            &HeaderMap::new()
        ));
    }
}
