//! Errors of the dashboard's handlers and how they are shown.

use http::StatusCode;

use crate::strings::errors;

/// Why a page or action could not be served.
#[derive(Debug)]
pub(crate) enum WebError {
    /// 404: an unknown page, queue or job (the message says which).
    NotFound(String),
    /// 400: a malformed parameter.
    BadRequest(String),
    /// 403: a refused change (read-only mode, cross-site request, bad token).
    Forbidden(String),
    /// 415: a POST that is not a form.
    Unsupported(String),
    /// 500: Redis failed.
    Redis(hench::Error),
    /// 500: anything else.
    Internal(String),
}

impl WebError {
    /// The HTTP status.
    pub(crate) fn status(&self) -> StatusCode {
        match self {
            WebError::NotFound(_) => StatusCode::NOT_FOUND,
            WebError::BadRequest(_) => StatusCode::BAD_REQUEST,
            WebError::Forbidden(_) => StatusCode::FORBIDDEN,
            WebError::Unsupported(_) => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            WebError::Redis(_) | WebError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// The heading of the error page.
    pub(crate) fn heading(&self) -> &'static str {
        match self {
            WebError::NotFound(_) => errors::NOT_FOUND,
            WebError::BadRequest(_) => errors::BAD_REQUEST,
            WebError::Forbidden(_) => errors::FORBIDDEN,
            WebError::Unsupported(_) => errors::UNSUPPORTED,
            WebError::Redis(_) => errors::UNAVAILABLE,
            WebError::Internal(_) => errors::INTERNAL,
        }
    }

    /// The explanation shown under the heading.
    pub(crate) fn message(&self) -> String {
        match self {
            WebError::NotFound(message)
            | WebError::BadRequest(message)
            | WebError::Forbidden(message)
            | WebError::Unsupported(message)
            | WebError::Internal(message) => message.clone(),
            WebError::Redis(error) => format!("{}: {error}", errors::REDIS),
        }
    }

    /// Logs server-side failures (client mistakes are not worth a log line).
    pub(crate) fn log(&self, path: &str) {
        match self {
            WebError::Redis(error) => {
                tracing::error!(path, %error, "dashboard request failed: Redis error");
            }
            WebError::Internal(message) => {
                tracing::error!(path, message, "dashboard request failed");
            }
            WebError::Forbidden(message) => {
                tracing::warn!(path, message, "dashboard refused a request");
            }
            WebError::NotFound(_) | WebError::BadRequest(_) | WebError::Unsupported(_) => {}
        }
    }
}

impl From<hench::Error> for WebError {
    fn from(error: hench::Error) -> Self {
        WebError::Redis(error)
    }
}

impl From<askama::Error> for WebError {
    fn from(error: askama::Error) -> Self {
        WebError::Internal(format!("could not render the page: {error}"))
    }
}

/// The result of a page or action.
pub(crate) type WebResult<T> = Result<T, WebError>;
