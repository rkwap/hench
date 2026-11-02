//! [`AllowedHosts`]: the host names a dashboard answers for, a defense against DNS rebinding.
//!
//! A page on a host name its author controls (`http://evil.example:3000`) can have that name
//! re-resolved to `127.0.0.1` (or to any address its visitors can reach) after it loaded. Its
//! scripts then talk to a dashboard listening there as if it were their own origin: the
//! browser sends `Host: evil.example:3000`, a matching `Origin` and `Sec-Fetch-Site:
//! same-origin`, so every cross-site check passes and the pages, with their CSRF token, are
//! readable. Only the `Host` header tells such requests apart, and scripts cannot set it.

use std::fmt;
use std::net::IpAddr;
use std::sync::Arc;

use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use http::header::{CACHE_CONTROL, CONTENT_TYPE, HOST, X_CONTENT_TYPE_OPTIONS};
use http::uri::Authority;
use http::{HeaderMap, HeaderValue, StatusCode, Uri};

/// The default answer to a refused request (see [`AllowedHosts::refusal`]).
const REFUSAL: &str = "This dashboard does not answer for this host name. It serves localhost, \
                       IP addresses and the names it is configured with (HenchWeb::allowed_hosts), \
                       a defense against DNS rebinding.";

/// The host names a dashboard answers for: a defense against DNS rebinding.
///
/// A web page on another site can point its own host name at the address of a dashboard
/// (`127.0.0.1`, or a machine on its visitors' network) and then use the dashboard as if it
/// were its own origin: every same-origin check passes, and authentication that such a page
/// can borrow or forge (none at all, the client's network address, a header a script can set)
/// does not stop it. Its requests carry its own host name in `Host`, which scripts cannot
/// change, so the dashboard refuses names it does not know with `403 Forbidden`.
///
/// By default ([`AllowedHosts::local`]) a dashboard answers `localhost`, `*.localhost` and IP
/// addresses, the names nobody can re-point. Name the others it is reached under:
///
/// ```
/// use hench_web::AllowedHosts;
///
/// // jobs.example.com, and example.org with its subdomains (any port, any case).
/// let hosts = AllowedHosts::new(["jobs.example.com", ".example.org"]);
/// assert!(hosts.allows("jobs.example.com:443"));
/// assert!(hosts.allows("admin.example.org"));
/// assert!(hosts.allows("127.0.0.1:3000") && hosts.allows("localhost"));
/// assert!(!hosts.allows("evil.example"));
/// ```
///
/// [`AllowedHosts::any`] turns the check off. Do that only when every request is
/// authenticated by something such a page cannot use: cookies and HTTP Basic credentials
/// belong to the name the browser used, so a rebound page has none for the dashboard; a
/// header the page's scripts could set, or the client's network address, are no protection.
///
/// Behind a reverse proxy, the dashboard sees the `Host` the proxy sends: forward the
/// original one (nginx: `proxy_set_header Host $host`) and allow that name, or have the proxy
/// refuse names it does not serve.
#[derive(Clone, PartialEq, Eq)]
pub struct AllowedHosts {
    /// The names besides the local ones (normalized by [`AllowedHosts::parse_name`]); `None`:
    /// every name.
    names: Option<Arc<[String]>>,
    /// The text of the `403` answer.
    refusal: Arc<str>,
}

impl AllowedHosts {
    /// `localhost`, `*.localhost` and IP addresses: the default of a dashboard.
    pub fn local() -> Self {
        Self {
            names: Some(Arc::from(Vec::new())),
            refusal: Arc::from(REFUSAL),
        }
    }

    /// Every host name: no check (see the type's documentation for when that is safe).
    pub fn any() -> Self {
        Self {
            names: None,
            refusal: Arc::from(REFUSAL),
        }
    }

    /// The local names plus `names`: exact host names (`jobs.example.com`), a domain with its
    /// subdomains (`.example.com`), or `*` for every name (as [`AllowedHosts::any`]); see
    /// [`AllowedHosts::parse_name`]. Names are compared case-insensitively, whatever the
    /// port. Invalid values (with a scheme, a port or a path) are ignored with a warning.
    pub fn new<I>(names: I) -> Self
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        let mut valid = Vec::new();
        for name in names {
            match Self::parse_name(name.as_ref()) {
                Ok(name) if name == "*" => return Self::any(),
                Ok(name) => valid.push(name),
                Err(error) => tracing::warn!(%error, "ignoring an invalid allowed host name"),
            }
        }
        Self {
            names: Some(Arc::from(valid)),
            refusal: Arc::from(REFUSAL),
        }
    }

    /// The same names, refused with `text` (a plain-text body) instead of the default
    /// message, which names [`HenchWeb::allowed_hosts`](crate::HenchWeb::allowed_hosts): say
    /// where your application configures them.
    #[must_use]
    pub fn refusal(mut self, text: impl AsRef<str>) -> Self {
        self.refusal = Arc::from(text.as_ref().trim_end());
        self
    }

    /// Parses an allowed name: a host name (`jobs.example.com`), a domain with its subdomains
    /// (`.example.com`), an IP address (always allowed anyway) or `*` (every name). Returns
    /// it normalized: lowercase, without a final dot or IPv6 brackets.
    pub fn parse_name(text: &str) -> Result<String, InvalidHostName> {
        let value = text.trim().to_ascii_lowercase();
        if value == "*" {
            return Ok(value);
        }
        let unbracketed = value
            .strip_prefix('[')
            .and_then(|inner| inner.strip_suffix(']'))
            .unwrap_or(&value);
        if unbracketed.parse::<IpAddr>().is_ok() {
            return Ok(unbracketed.to_owned());
        }
        let value = value.strip_suffix('.').unwrap_or(&value);
        let name = value.strip_prefix('.').unwrap_or(value);
        let valid = !name.is_empty()
            && name.split('.').all(|label| {
                !label.is_empty()
                    && label.len() <= 63
                    && label
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'))
            });
        if valid {
            Ok(value.to_owned())
        } else {
            Err(InvalidHostName {
                text: text.to_owned(),
            })
        }
    }

    /// `true` when every host name is allowed ([`AllowedHosts::any`]).
    pub fn allows_any(&self) -> bool {
        self.names.is_none()
    }

    /// `true` when `host`, a `Host` header value (`name[:port]`), may be served.
    pub fn allows(&self, host: &str) -> bool {
        let Some(names) = &self.names else {
            return true;
        };
        let Some(name) = host_name(host) else {
            return false;
        };
        if name.parse::<IpAddr>().is_ok() || name == "localhost" || name.ends_with(".localhost") {
            return true;
        }
        names.iter().any(|allowed| match allowed.strip_prefix('.') {
            Some(domain) => {
                name == domain
                    || name
                        .strip_suffix(domain)
                        .is_some_and(|sub| sub.ends_with('.'))
            }
            None => name == *allowed,
        })
    }

    /// `true` when a request may be served: every `Host` header it carries is allowed, and so
    /// is the authority of its URI (HTTP/2, or an absolute URI). A request with neither (HTTP/1.0;
    /// browsers always send one) is served.
    pub fn allows_request<B>(&self, request: &http::Request<B>) -> bool {
        self.allows_head(request.headers(), request.uri())
    }

    /// [`AllowedHosts::allows_request`] for a request's headers and URI.
    pub(crate) fn allows_head(&self, headers: &HeaderMap, uri: &Uri) -> bool {
        if self.allows_any() {
            return true;
        }
        let hosts_allowed = headers
            .get_all(HOST)
            .iter()
            .all(|value| value.to_str().is_ok_and(|host| self.allows(host)));
        let authority_allowed = uri
            .authority()
            .is_none_or(|authority| self.allows(authority.as_str()));
        hosts_allowed && authority_allowed
    }

    /// The `403 Forbidden` answer to a request for a name that is not allowed.
    pub(crate) fn refuse(&self, headers: &HeaderMap, uri: &Uri) -> Response {
        let host = headers
            .get(HOST)
            .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned());
        tracing::warn!(
            host,
            authority = uri.authority().map(Authority::as_str),
            path = uri.path(),
            "refused a request for a host name that is not allowed (DNS rebinding?)"
        );
        let mut response = (StatusCode::FORBIDDEN, format!("{}\n", self.refusal)).into_response();
        let headers = response.headers_mut();
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        );
        headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
        headers.insert(X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
        response
    }
}

impl Default for AllowedHosts {
    /// [`AllowedHosts::local`].
    fn default() -> Self {
        Self::local()
    }
}

impl fmt::Debug for AllowedHosts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.names {
            None => f.write_str("AllowedHosts(*)"),
            Some(names) => f
                .debug_tuple("AllowedHosts")
                .field(&format_args!("localhost, IP addresses"))
                .field(names)
                .finish(),
        }
    }
}

/// An allowed host name that is not one: it has a scheme, a port, a path or characters host
/// names do not have (see [`AllowedHosts::parse_name`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidHostName {
    text: String,
}

impl fmt::Display for InvalidHostName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "`{}` is not a host name like jobs.example.com or .example.com (no scheme, port or \
             path)",
            self.text
        )
    }
}

impl std::error::Error for InvalidHostName {}

/// Middleware refusing (`403 Forbidden`, plain text) the requests for host names `hosts` does
/// not allow, for routes you serve next to the dashboard (the dashboard checks its own):
///
/// ```no_run
/// use axum::middleware;
/// use axum::routing::get;
/// use hench_web::AllowedHosts;
///
/// # fn demo(redis: hench::RedisPool) -> axum::Router {
/// let hosts = AllowedHosts::new(["jobs.example.com"]);
/// axum::Router::new()
///     .route("/admin/stats", get(|| async { "..." }))
///     .layer(middleware::from_fn_with_state(hosts, hench_web::require_host))
/// # }
/// ```
pub async fn require_host(
    State(hosts): State<AllowedHosts>,
    request: Request,
    next: Next,
) -> Response {
    if hosts.allows_request(&request) {
        next.run(request).await
    } else {
        hosts.refuse(request.headers(), request.uri())
    }
}

/// The lowercase host name of a `Host` value, without port, IPv6 brackets or a final dot;
/// `None` when the value is not an authority.
fn host_name(host: &str) -> Option<String> {
    let authority: Authority = host.trim().parse().ok()?;
    if authority.as_str().contains('@') {
        return None;
    }
    let name = authority.host();
    // A port, when present, must be a number.
    if authority.as_str().len() > name.len() && authority.port_u16().is_none() {
        return None;
    }
    let name = name
        .strip_prefix('[')
        .and_then(|inner| inner.strip_suffix(']'))
        .unwrap_or(name);
    let name = name.strip_suffix('.').unwrap_or(name);
    (!name.is_empty()).then(|| name.to_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_that_cannot_be_rebound_are_always_allowed() {
        let hosts = AllowedHosts::local();
        for host in [
            "localhost",
            "LocalHost:7420",
            "localhost.:80",
            "dash.localhost",
            "127.0.0.1",
            "127.0.0.1:7420",
            "10.0.0.7:8080",
            "[::1]:7420",
            "[2001:db8::1]",
        ] {
            assert!(hosts.allows(host), "{host}");
        }
        for host in [
            "evil.example",
            "evil.example:7420",
            "localhost.evil.example",
            "127.0.0.1.nip.io",
            "",
            "user@localhost",
            "local host",
            "localhost:port",
        ] {
            assert!(!hosts.allows(host), "{host:?}");
        }
        assert!(!hosts.allows_any());
        assert_eq!(AllowedHosts::default(), hosts);
    }

    #[test]
    fn allowed_names_and_domains_match_whole_labels() {
        let hosts = AllowedHosts::new(["jobs.example.com", ".corp.example"]);
        assert!(hosts.allows("jobs.example.com"));
        assert!(hosts.allows("JOBS.EXAMPLE.COM:443"));
        assert!(hosts.allows("jobs.example.com.:443"));
        assert!(!hosts.allows("x.jobs.example.com"));
        assert!(!hosts.allows("jobs.example.com.evil.example"));
        assert!(hosts.allows("corp.example"));
        assert!(hosts.allows("a.b.corp.example:8080"));
        assert!(!hosts.allows("xcorp.example"));
        assert!(!hosts.allows("corp.example.evil"));
        // Invalid values are skipped, `*` allows every name.
        let hosts = AllowedHosts::new(["https://jobs.example.com", "ok.example"]);
        assert!(hosts.allows("ok.example") && !hosts.allows("jobs.example.com"));
        let every = AllowedHosts::new(["x.example", "*"]);
        assert!(every.allows_any() && every.allows("evil.example"));
        assert_eq!(every, AllowedHosts::any());
        assert_eq!(format!("{every:?}"), "AllowedHosts(*)");
        assert!(format!("{hosts:?}").contains("ok.example"));
    }

    #[test]
    fn names_are_validated() {
        assert_eq!(
            AllowedHosts::parse_name(" Jobs.Example.COM ").unwrap(),
            "jobs.example.com"
        );
        assert_eq!(
            AllowedHosts::parse_name(".example.com").unwrap(),
            ".example.com"
        );
        assert_eq!(
            AllowedHosts::parse_name("example.com.").unwrap(),
            "example.com"
        );
        assert_eq!(AllowedHosts::parse_name("*").unwrap(), "*");
        assert_eq!(AllowedHosts::parse_name("[::1]").unwrap(), "::1");
        assert_eq!(AllowedHosts::parse_name("10.0.0.1").unwrap(), "10.0.0.1");
        for bad in [
            "",
            ".",
            "https://jobs.example.com",
            "jobs.example.com:443",
            "jobs.example.com/path",
            "a..b",
            "*.example.com",
            "jobs example",
        ] {
            let error = AllowedHosts::parse_name(bad).unwrap_err();
            assert!(error.to_string().contains("not a host name"), "{bad:?}");
        }
    }

    #[test]
    fn requests_are_checked_by_every_name_they_carry() {
        let hosts = AllowedHosts::local();
        let request = |host: Option<&str>, uri: &str| {
            let mut builder = http::Request::get(uri);
            if let Some(host) = host {
                builder = builder.header(HOST, host);
            }
            builder.body(()).unwrap()
        };
        assert!(hosts.allows_request(&request(None, "/")));
        assert!(hosts.allows_request(&request(Some("127.0.0.1:3000"), "/")));
        assert!(!hosts.allows_request(&request(Some("evil.example"), "/")));
        // HTTP/2 or an absolute URI: the authority counts too.
        assert!(hosts.allows_request(&request(None, "http://localhost:3000/")));
        assert!(!hosts.allows_request(&request(None, "http://evil.example/")));
        assert!(!hosts.allows_request(&request(Some("localhost"), "http://evil.example/")));
        assert!(!hosts.allows_request(&request(Some("evil.example"), "http://localhost/")));
        // Several Host headers: all of them.
        let mut twice = request(Some("localhost"), "/");
        twice
            .headers_mut()
            .append(HOST, HeaderValue::from_static("evil.example"));
        assert!(!hosts.allows_request(&twice));
        // Not text: refused.
        let mut binary = request(None, "/");
        binary
            .headers_mut()
            .insert(HOST, HeaderValue::from_bytes(b"\xfflocalhost").unwrap());
        assert!(!hosts.allows_request(&binary));
        assert!(AllowedHosts::any().allows_request(&binary));
    }
}
