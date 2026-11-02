//! Cross-site request forgery protection for every state-changing request.
//!
//! Two independent checks:
//!
//! * **Double-submit token.** Each browser gets a random token in an `HttpOnly`,
//!   `SameSite=Lax` cookie scoped to the dashboard's path; every form carries the same
//!   token in a hidden field. A POST is accepted only when the field matches the cookie
//!   (compared in constant time). Another site can make the browser send the cookie but
//!   cannot read it to fill in the field. (Lax rather than Strict: a link from another site
//!   carries the cookie, so it does not issue a new token that would break the forms of
//!   pages already open.)
//! * **Fetch metadata.** Browsers say where a request comes from: `Sec-Fetch-Site` must be
//!   `same-origin` when present, else `Origin` (when present) must name this host.

use http::HeaderMap;
use http::header::{HOST, ORIGIN};
use subtle::ConstantTimeEq;

/// The name of the token cookie.
pub(crate) const COOKIE: &str = "hench_csrf";

/// The name of the form field carrying the token.
pub(crate) const FIELD: &str = "_csrf";

/// Random bytes per token.
const TOKEN_BYTES: usize = 32;

/// A fresh token: 32 random bytes from the operating system, as lowercase hex. Fails when the
/// operating system's random source does (there is no safe fallback for a security token).
pub(crate) fn generate() -> Result<String, getrandom::Error> {
    let mut bytes = [0_u8; TOKEN_BYTES];
    getrandom::fill(&mut bytes)?;
    let mut token = String::with_capacity(TOKEN_BYTES * 2);
    for byte in bytes {
        token.push(char::from(HEX[usize::from(byte >> 4)]));
        token.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    Ok(token)
}

const HEX: &[u8; 16] = b"0123456789abcdef";

/// `true` for a value that looks like a token this module generated.
pub(crate) fn is_well_formed(token: &str) -> bool {
    token.len() == TOKEN_BYTES * 2
        && token
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// `true` if `submitted` equals one of the cookie `tokens` (a browser may hold several when
/// dashboards are mounted at nested paths). Each comparison takes constant time.
pub(crate) fn matches<'a>(tokens: impl IntoIterator<Item = &'a str>, submitted: &str) -> bool {
    if !is_well_formed(submitted) {
        return false;
    }
    let mut found = subtle::Choice::from(0);
    for token in tokens {
        if token.len() == submitted.len() {
            found |= token.as_bytes().ct_eq(submitted.as_bytes());
        }
    }
    found.into()
}

/// Why a state-changing request must be refused as cross-site, if it must.
///
/// `Sec-Fetch-Site` is authoritative when the browser sends it (all current browsers do):
/// anything but `same-origin` is refused. Otherwise an `Origin` header, when present, must
/// match the `Host` (or `X-Forwarded-Host`) the request was sent to; `Origin: null` (sandboxed
/// documents, some redirects) is refused. Requests with neither header (old browsers, scripts)
/// fall back on the token check alone.
pub(crate) fn cross_site(headers: &HeaderMap) -> Option<&'static str> {
    if let Some(site) = headers.get("sec-fetch-site") {
        return (site.as_bytes() != b"same-origin").then_some("sec-fetch-site");
    }
    let origin = headers.get(ORIGIN)?;
    let Ok(origin) = origin.to_str() else {
        return Some("origin");
    };
    let Some(authority) = origin_authority(origin) else {
        return Some("origin");
    };
    let hosts = [HOST.as_str(), "x-forwarded-host"];
    let same_host = hosts
        .iter()
        .filter_map(|name| headers.get(*name)?.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|host| host.trim().eq_ignore_ascii_case(authority));
    (!same_host).then_some("origin")
}

/// The `host[:port]` of an `Origin` value (`scheme://host[:port]`); `None` for `null` or
/// anything else.
fn origin_authority(origin: &str) -> Option<&str> {
    let (scheme, rest) = origin.split_once("://")?;
    if !matches!(scheme, "http" | "https") || rest.is_empty() || rest.contains(['/', '?', '#']) {
        return None;
    }
    Some(rest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    fn headers(pairs: &[(&'static str, &'static str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.append(*name, HeaderValue::from_static(value));
        }
        map
    }

    #[test]
    fn tokens_are_random_hex() {
        let (a, b) = (generate().unwrap(), generate().unwrap());
        assert_ne!(a, b);
        assert!(is_well_formed(&a) && is_well_formed(&b));
        assert!(!is_well_formed("abc"));
        assert!(!is_well_formed(&a.to_uppercase()));
        assert!(!is_well_formed(&format!("{}g", &a[1..])));
    }

    #[test]
    fn tokens_must_match_a_cookie() {
        let token = generate().unwrap();
        let other = generate().unwrap();
        assert!(matches([token.as_str()], &token));
        assert!(matches([other.as_str(), token.as_str()], &token));
        assert!(!matches([other.as_str()], &token));
        assert!(!matches([], &token));
        assert!(!matches([""], ""));
        assert!(!matches(["short"], "short"));
    }

    #[test]
    fn cross_site_requests_are_detected() {
        assert_eq!(cross_site(&headers(&[])), None);
        assert_eq!(
            cross_site(&headers(&[("sec-fetch-site", "same-origin")])),
            None
        );
        for site in ["cross-site", "same-site", "none"] {
            let map = {
                let mut map = HeaderMap::new();
                map.insert("sec-fetch-site", HeaderValue::from_static(site));
                map
            };
            assert!(cross_site(&map).is_some(), "{site}");
        }
        // Sec-Fetch-Site wins over Origin.
        assert_eq!(
            cross_site(&headers(&[
                ("sec-fetch-site", "same-origin"),
                ("origin", "https://evil.example"),
                ("host", "dash.example"),
            ])),
            None
        );
        assert_eq!(
            cross_site(&headers(&[
                ("origin", "https://dash.example:8443"),
                ("host", "dash.example:8443"),
            ])),
            None
        );
        assert_eq!(
            cross_site(&headers(&[
                ("origin", "http://DASH.example"),
                ("host", "internal:7420"),
                ("x-forwarded-host", "dash.example"),
            ])),
            None
        );
        assert!(
            cross_site(&headers(&[
                ("origin", "https://evil.example"),
                ("host", "dash.example"),
            ]))
            .is_some()
        );
        assert!(cross_site(&headers(&[("origin", "null"), ("host", "dash.example")])).is_some());
        assert!(
            cross_site(&headers(&[
                ("origin", "https://dash.example/path"),
                ("host", "dash.example"),
            ]))
            .is_some()
        );
        assert!(cross_site(&headers(&[("origin", "https://dash.example")])).is_some());
    }
}
