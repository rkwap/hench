//! [`PostForm`]: the body of a dashboard form, accepted only with a valid CSRF token; and
//! [`field`], the form a value read from Redis takes in a form field, so that it comes back
//! as it was.

use std::borrow::Cow;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{FromRequest, Request};
use axum::response::{IntoResponse, Response};
use http::header::CONTENT_TYPE;
use percent_encoding::{AsciiSet, CONTROLS, percent_decode_str, utf8_percent_encode};

use crate::config::HenchWeb;
use crate::csrf;
use crate::ctx::{Ctx, cookies};
use crate::error::WebError;
use crate::strings::errors;

/// What [`field`] percent-encodes besides non-ASCII text: `%` (the escape itself) and the ASCII
/// control characters, which do not survive a round trip through a page: an HTML parser turns
/// NUL into U+FFFD (and CR or CRLF into LF), and a browser submitting a form turns a lone CR or
/// LF into CRLF.
const FIELD: &AsciiSet = &CONTROLS.add(b'%');

/// `value` (a jid, a limit's identity, an entry's key: text read from Redis, which may hold
/// any character) as the value of a form field, so that the server reads back exactly
/// `value` with [`PostForm::value`] or [`PostForm::values`]. A value without `%`, control
/// characters or non-ASCII text, such as a generated jid, is unchanged.
pub(crate) fn field(value: &str) -> String {
    utf8_percent_encode(value, FIELD).to_string()
}

/// A submitted value written with [`field`], decoded (`%` followed by anything but two
/// hexadecimal digits stays as it is, and bytes that are not UTF-8 become U+FFFD: neither
/// comes from a page).
fn decoded(value: &str) -> Cow<'_, str> {
    percent_decode_str(value).decode_utf8_lossy()
}

/// The fields of an `application/x-www-form-urlencoded` POST body (repeated names kept in
/// order). Extracting it checks the CSRF token: the `_csrf` field must match a `hench_csrf`
/// cookie, else the request is refused with `403 Forbidden` before the handler runs.
#[derive(Debug, Default)]
pub(crate) struct PostForm {
    fields: Vec<(String, String)>,
}

impl PostForm {
    /// The first value of `name`.
    pub(crate) fn get(&self, name: &str) -> Option<&str> {
        self.fields
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }

    /// `true` if the form has a field `name` (a pressed submit button).
    pub(crate) fn has(&self, name: &str) -> bool {
        self.get(name).is_some()
    }

    /// Every value of `name`, in order.
    pub(crate) fn all<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a str> + 'a {
        self.fields
            .iter()
            .filter(move |(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }

    /// The first value of `name`, a field the page wrote with [`field`], decoded.
    pub(crate) fn value(&self, name: &str) -> Option<Cow<'_, str>> {
        self.get(name).map(decoded)
    }

    /// Every value of `name`, fields the page wrote with [`field`], decoded, in order.
    pub(crate) fn values<'a>(&'a self, name: &'a str) -> impl Iterator<Item = Cow<'a, str>> + 'a {
        self.all(name).map(decoded)
    }

    /// Parses a body, checking its token against the cookie tokens.
    fn parse(body: &[u8], tokens: &[String]) -> Result<Self, WebError> {
        let fields: Vec<(String, String)> = form_urlencoded::parse(body)
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect();
        let form = Self { fields };
        let submitted = form.get(csrf::FIELD).unwrap_or_default();
        if !csrf::matches(tokens.iter().map(String::as_str), submitted) {
            return Err(WebError::Forbidden(errors::BAD_TOKEN.to_owned()));
        }
        Ok(form)
    }
}

impl FromRequest<Arc<HenchWeb>> for PostForm {
    type Rejection = Response;

    async fn from_request(request: Request, web: &Arc<HenchWeb>) -> Result<Self, Self::Rejection> {
        let (parts, body) = request.into_parts();
        let fail = |error: WebError| match Ctx::from_parts(&parts, web) {
            Ok(ctx) => crate::pages::error_page(&ctx, error),
            Err(error) => error.into_response(),
        };
        let is_form = parts
            .headers
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .is_some_and(|mime| {
                mime.trim()
                    .eq_ignore_ascii_case("application/x-www-form-urlencoded")
            });
        if !is_form {
            return Err(fail(WebError::Unsupported(errors::NOT_A_FORM.to_owned())));
        }
        let tokens: Vec<String> = cookies(&parts.headers)
            .into_iter()
            .filter(|(name, _)| name == csrf::COOKIE)
            .map(|(_, value)| value)
            .collect();
        let request = Request::from_parts(parts.clone(), body);
        let bytes = match Bytes::from_request(request, web).await {
            Ok(bytes) => bytes,
            Err(rejection) => return Err(rejection.into_response()),
        };
        PostForm::parse(&bytes, &tokens).map_err(fail)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forms_need_the_cookie_token() {
        let token = csrf::generate().unwrap();
        let body = format!("_csrf={token}&key=1&key=2&retry=Retry+now");
        let form = PostForm::parse(body.as_bytes(), std::slice::from_ref(&token)).unwrap();
        assert_eq!(form.all("key").collect::<Vec<_>>(), ["1", "2"]);
        assert!(form.has("retry"));
        assert_eq!(form.get("retry"), Some("Retry now"));
        assert!(!form.has("delete"));
        assert!(PostForm::parse(b"key=1", std::slice::from_ref(&token)).is_err());
        assert!(PostForm::parse(body.as_bytes(), &[]).is_err());
        let other = csrf::generate().unwrap();
        assert!(PostForm::parse(body.as_bytes(), std::slice::from_ref(&other)).is_err());
        assert!(PostForm::parse(body.as_bytes(), &[other, token]).is_ok());
    }

    /// What a page's value becomes on its way back: the HTML parser's preprocessing (CR and
    /// CRLF become LF, NUL becomes U+FFFD in attributes), then the newline normalization of a
    /// form submission (a lone CR or LF becomes CRLF).
    fn through_a_browser(value: &str) -> String {
        value
            .replace("\r\n", "\n")
            .replace('\r', "\n")
            .replace('\0', "\u{fffd}")
            .replace('\n', "\r\n")
    }

    #[test]
    fn fields_bring_back_any_value() {
        // Round 3 (web-api-security-5): raw values with a lone LF or CR came back with CRLF.
        let token = csrf::generate().unwrap();
        for value in [
            "0123456789abcdef01234567",
            "lone\nlf",
            "lone\rcr",
            "a\r\nb",
            "nul\0x",
            "tab\tand\u{1b}[31mesc\u{7f}",
            "100%",
            "%25 and %0A and %zz and %",
            "c:stripe:é ünïcödé 日本",
            "digest.jid.with.dots",
            "",
        ] {
            let encoded = field(value);
            assert!(!encoded.chars().any(char::is_control), "{encoded:?}");
            assert_eq!(through_a_browser(&encoded), encoded);
            let mut body = form_urlencoded::Serializer::new(String::new());
            body.append_pair("_csrf", &token)
                .append_pair("jid", &through_a_browser(&encoded))
                .append_pair("key", &encoded)
                .append_pair("key", &field("second"));
            let form =
                PostForm::parse(body.finish().as_bytes(), std::slice::from_ref(&token)).unwrap();
            assert_eq!(form.value("jid").as_deref(), Some(value), "{value:?}");
            assert_eq!(form.values("key").collect::<Vec<_>>(), [value, "second"]);
            // Raw, these would not have come back.
            if ["lone\nlf", "lone\rcr", "nul\0x"].contains(&value) {
                assert_ne!(through_a_browser(value), value);
            }
        }
        // Generated jids and plain identities are written as they are.
        assert_eq!(
            field("0123456789abcdef01234567"),
            "0123456789abcdef01234567"
        );
        assert_eq!(field("w:stripe:42"), "w:stripe:42");
        assert_eq!(field("a\nb%"), "a%0Ab%25");
    }
}
