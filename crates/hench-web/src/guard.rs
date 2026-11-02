//! The middleware every dashboard request goes through: it refuses requests for host names
//! the dashboard does not answer for, and state-changing requests in read-only mode or from
//! other sites, and adds the security headers to every response.

use std::sync::Arc;

use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use http::header::{
    CACHE_CONTROL, CONTENT_SECURITY_POLICY, REFERRER_POLICY, X_CONTENT_TYPE_OPTIONS,
    X_FRAME_OPTIONS,
};
use http::{HeaderMap, HeaderValue, Method};

use crate::config::HenchWeb;
use crate::csrf;
use crate::ctx::Ctx;
use crate::error::WebError;
use crate::extension::Dashboard;
use crate::strings::errors;

/// The content security policy: everything from the dashboard's own origin, no inline
/// scripts or styles, no plugins, no framing by other sites, forms only to the dashboard.
pub(crate) const CSP: &str = "default-src 'self'; base-uri 'none'; form-action 'self'; \
                              frame-ancestors 'self'; object-src 'none'";

/// Refuses requests for host names that are not allowed (DNS rebinding), and unsafe
/// requests that read-only mode or the cross-site check forbid, then adds the security
/// headers.
pub(crate) async fn guard(
    State(web): State<Arc<HenchWeb>>,
    request: Request,
    next: Next,
) -> Response {
    if !web.hosts.allows_head(request.headers(), request.uri()) {
        // Before anything else: no page, no token cookie.
        let mut response = web.hosts.refuse(request.headers(), request.uri());
        add_security_headers(response.headers_mut());
        return response;
    }
    let safe = matches!(
        *request.method(),
        Method::GET | Method::HEAD | Method::OPTIONS
    );
    let refusal = if safe {
        None
    } else if web.read_only {
        Some(errors::READ_ONLY)
    } else if let Some(reason) = csrf::cross_site(request.headers()) {
        tracing::warn!(
            reason,
            path = request.uri().path(),
            "refused a cross-site request to the dashboard"
        );
        Some(errors::CROSS_SITE)
    } else {
        None
    };
    let mut response = match refusal {
        None => {
            // For the extractors of the host's routes (`Page`, `Form`).
            let mut request = request;
            request.extensions_mut().insert(Dashboard(Arc::clone(&web)));
            next.run(request).await
        }
        Some(message) => {
            let (parts, _) = request.into_parts();
            // No new token cookie on a refusal: it would replace the token of open pages.
            match Ctx::from_parts(&parts, &web) {
                Ok(ctx) => crate::pages::error_page(
                    &ctx.without_new_token(),
                    WebError::Forbidden(message.to_owned()),
                ),
                Err(error) => error.into_response(),
            }
        }
    };
    add_security_headers(response.headers_mut());
    response
}

/// Adds the security headers (and `Cache-Control: no-store` unless the response set its own
/// caching, as assets do).
pub(crate) fn add_security_headers(headers: &mut HeaderMap) {
    headers.insert(CONTENT_SECURITY_POLICY, HeaderValue::from_static(CSP));
    headers.insert(X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    headers.insert(REFERRER_POLICY, HeaderValue::from_static("same-origin"));
    headers.insert(X_FRAME_OPTIONS, HeaderValue::from_static("SAMEORIGIN"));
    headers.insert(
        "cross-origin-opener-policy",
        HeaderValue::from_static("same-origin"),
    );
    headers
        .entry(CACHE_CONTROL)
        .or_insert(HeaderValue::from_static("no-store"));
}
