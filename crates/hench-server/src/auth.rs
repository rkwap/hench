//! HTTP Basic authentication for the dashboard and the JSON API.

use std::collections::hash_map::RandomState;
use std::fmt;
use std::hash::BuildHasher;
use std::sync::Arc;

use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use http::header::{AUTHORIZATION, WWW_AUTHENTICATE};
use http::{HeaderValue, StatusCode};
use subtle::ConstantTimeEq;

/// The user name and password a request must present.
///
/// They are never kept in the clear: each is reduced to a 128-bit digest with two hash
/// functions keyed randomly per process (SipHash), and a request's credentials are digested
/// the same way and compared in constant time. The comparison therefore takes the same time
/// whatever the submitted values (their length included), and a false match would need a
/// collision of two independently keyed 64-bit hashes.
#[derive(Clone)]
pub struct Credentials {
    inner: Arc<Digests>,
}

struct Digests {
    keys: [RandomState; 2],
    username: [u64; 2],
    password: [u64; 2],
}

impl Credentials {
    /// Credentials for `username` and `password`.
    pub fn new(username: &str, password: &str) -> Self {
        let keys = [RandomState::new(), RandomState::new()];
        let digest = |value: &str| [keys[0].hash_one(value), keys[1].hash_one(value)];
        let (username, password) = (digest(username), digest(password));
        Self {
            inner: Arc::new(Digests {
                keys,
                username,
                password,
            }),
        }
    }

    fn digest(&self, value: &str) -> [u64; 2] {
        let keys = &self.inner.keys;
        [keys[0].hash_one(value), keys[1].hash_one(value)]
    }

    /// `true` when `header` (an `Authorization` value) carries these credentials.
    pub fn allows(&self, header: Option<&HeaderValue>) -> bool {
        let Some((username, password)) = header.and_then(decode_basic) else {
            return false;
        };
        let (user, pass) = (self.digest(&username), self.digest(&password));
        let expected = &self.inner;
        let matches = user[0].ct_eq(&expected.username[0])
            & user[1].ct_eq(&expected.username[1])
            & pass[0].ct_eq(&expected.password[0])
            & pass[1].ct_eq(&expected.password[1]);
        matches.into()
    }
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Credentials(..)")
    }
}

/// The user name and password of a `Basic` `Authorization` header.
fn decode_basic(header: &HeaderValue) -> Option<(String, String)> {
    let header = header.to_str().ok()?.trim();
    let (scheme, encoded) = header.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("basic") {
        return None;
    }
    let decoded = STANDARD.decode(encoded.trim()).ok()?;
    let decoded = String::from_utf8(decoded).ok()?;
    let (username, password) = decoded.split_once(':')?;
    Some((username.to_owned(), password.to_owned()))
}

/// Middleware: lets requests with the right credentials through, answers the others with
/// `401 Unauthorized` and a `WWW-Authenticate: Basic` challenge.
pub async fn require(
    State(credentials): State<Credentials>,
    request: Request,
    next: Next,
) -> Response {
    if credentials.allows(request.headers().get(AUTHORIZATION)) {
        return next.run(request).await;
    }
    let mut response = (StatusCode::UNAUTHORIZED, "Authentication required\n").into_response();
    response.headers_mut().insert(
        WWW_AUTHENTICATE,
        HeaderValue::from_static("Basic realm=\"hench\", charset=\"UTF-8\""),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    fn basic(user: &str, password: &str) -> HeaderValue {
        let token = STANDARD.encode(format!("{user}:{password}"));
        HeaderValue::from_str(&format!("Basic {token}")).unwrap()
    }

    #[test]
    fn only_the_right_credentials_pass() {
        let credentials = Credentials::new("admin", "s3cret:with:colons");
        assert!(credentials.allows(Some(&basic("admin", "s3cret:with:colons"))));
        assert!(!credentials.allows(Some(&basic("admin", "s3cret"))));
        assert!(!credentials.allows(Some(&basic("Admin", "s3cret:with:colons"))));
        assert!(!credentials.allows(Some(&basic("", ""))));
        assert!(!credentials.allows(None));
        for header in [
            "Bearer abc",
            "Basic",
            "Basic !!!",
            "basic YWRtaW4=",
            "Digest username=admin",
        ] {
            assert!(
                !credentials.allows(Some(&HeaderValue::from_static(header))),
                "{header}"
            );
        }
        // The scheme is case-insensitive.
        let token = STANDARD.encode("admin:s3cret:with:colons");
        let lower = HeaderValue::from_str(&format!("basic {token}")).unwrap();
        assert!(credentials.allows(Some(&lower)));
        assert_eq!(format!("{credentials:?}"), "Credentials(..)");
    }

    #[test]
    fn empty_credentials_are_still_checked() {
        let credentials = Credentials::new("", "");
        assert!(credentials.allows(Some(&basic("", ""))));
        assert!(!credentials.allows(Some(&basic("", "x"))));
    }
}
