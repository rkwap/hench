//! The stylesheet, scripts and icon, embedded in the binary and served under
//! content-fingerprinted names (`hench.<hash>.css`) with immutable cache headers: a new
//! release changes the names, so browsers never use a stale copy and never revalidate.

use std::sync::LazyLock;

use axum::extract::Path;
use axum::response::{IntoResponse, Response};
use http::StatusCode;
use http::header::{CACHE_CONTROL, CONTENT_TYPE};
use serde::Deserialize;

/// One embedded file.
pub(crate) struct Asset {
    /// The logical name (`hench.css`).
    pub(crate) name: &'static str,
    /// The served, fingerprinted name (`hench.0123456789abcdef.css`).
    pub(crate) file: String,
    /// The contents.
    pub(crate) body: &'static [u8],
    /// The `Content-Type`.
    pub(crate) content_type: &'static str,
}

/// The `Cache-Control` of assets: a year, and never revalidated.
pub(crate) const IMMUTABLE: &str = "public, max-age=31536000, immutable";

const CSS: &str = "text/css; charset=utf-8";
const JS: &str = "text/javascript; charset=utf-8";
const SVG: &str = "image/svg+xml";

const SOURCES: [(&str, &[u8], &str); 10] = [
    ("hench.css", include_bytes!("../assets/hench.css"), CSS),
    ("hench.js", include_bytes!("../assets/hench.js"), JS),
    ("theme.js", include_bytes!("../assets/theme.js"), JS),
    ("dashboard.js", include_bytes!("../assets/dashboard.js"), JS),
    ("metrics.js", include_bytes!("../assets/metrics.js"), JS),
    ("icon.svg", include_bytes!("../assets/icon.svg"), SVG),
    // The pages of the features, one stylesheet each.
    (
        "periodic.css",
        include_bytes!("../assets/periodic.css"),
        CSS,
    ),
    ("batches.css", include_bytes!("../assets/batches.css"), CSS),
    ("limits.css", include_bytes!("../assets/limits.css"), CSS),
    ("locks.css", include_bytes!("../assets/locks.css"), CSS),
];

/// Every asset, fingerprinted once.
pub(crate) static ASSETS: LazyLock<Vec<Asset>> = LazyLock::new(|| {
    SOURCES
        .iter()
        .map(|&(name, body, content_type)| Asset {
            name,
            file: fingerprinted(name, body),
            body,
            content_type,
        })
        .collect()
});

/// `name` with the content hash before its extension.
fn fingerprinted(name: &str, body: &[u8]) -> String {
    let hash = fnv1a(body);
    match name.rsplit_once('.') {
        Some((stem, extension)) => format!("{stem}.{hash:016x}.{extension}"),
        None => format!("{name}.{hash:016x}"),
    }
}

/// 64-bit FNV-1a: plenty to tell versions of a file apart (this is not a security measure).
fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

/// The URL of the asset `name` for a dashboard mounted at `prefix`.
pub(crate) fn url(prefix: &str, name: &str) -> String {
    let file = ASSETS
        .iter()
        .find(|asset| asset.name == name)
        .map_or(name, |asset| asset.file.as_str());
    format!("{prefix}/assets/{file}")
}

/// The asset URLs a page links to.
pub(crate) struct AssetUrls {
    /// The stylesheet.
    pub(crate) css: String,
    /// The script every page loads (deferred).
    pub(crate) js: String,
    /// The tiny script that applies the stored color theme before the first paint.
    pub(crate) theme_js: String,
    /// The dashboard's charts.
    pub(crate) dashboard_js: String,
    /// The metrics pages' charts.
    pub(crate) metrics_js: String,
    /// The favicon.
    pub(crate) icon: String,
    /// The styles of the Periodic pages.
    pub(crate) periodic_css: String,
    /// The styles of the Batches pages.
    pub(crate) batches_css: String,
    /// The styles of the Limits pages.
    pub(crate) limits_css: String,
    /// The styles of the Locks pages.
    pub(crate) locks_css: String,
}

impl AssetUrls {
    /// The URLs for a dashboard mounted at `prefix`.
    pub(crate) fn new(prefix: &str) -> Self {
        Self {
            css: url(prefix, "hench.css"),
            js: url(prefix, "hench.js"),
            theme_js: url(prefix, "theme.js"),
            dashboard_js: url(prefix, "dashboard.js"),
            metrics_js: url(prefix, "metrics.js"),
            icon: url(prefix, "icon.svg"),
            periodic_css: url(prefix, "periodic.css"),
            batches_css: url(prefix, "batches.css"),
            limits_css: url(prefix, "limits.css"),
            locks_css: url(prefix, "locks.css"),
        }
    }
}

/// The `{file}` of the asset route (extracted by name: see `pages::queues::QueueName`).
#[derive(Deserialize)]
pub(crate) struct AssetFile {
    file: String,
}

/// `GET /assets/{file}`: an embedded file by its fingerprinted name.
pub(crate) async fn serve(Path(AssetFile { file }): Path<AssetFile>) -> Response {
    match ASSETS.iter().find(|asset| asset.file == file) {
        Some(asset) => (
            [
                (CONTENT_TYPE, asset.content_type),
                (CACHE_CONTROL, IMMUTABLE),
            ],
            asset.body,
        )
            .into_response(),
        None => (StatusCode::NOT_FOUND, "no such asset").into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_carry_the_content_hash() {
        assert_eq!(fnv1a(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(fingerprinted("x.css", b""), "x.cbf29ce484222325.css");
        assert_eq!(fingerprinted("noext", b""), "noext.cbf29ce484222325");
        assert_ne!(fingerprinted("x.css", b"a"), fingerprinted("x.css", b"b"));
        let urls = AssetUrls::new("/hench");
        assert!(urls.css.starts_with("/hench/assets/hench."));
        assert!(urls.css.ends_with(".css"));
        assert_ne!(urls.js, urls.dashboard_js);
        assert!(urls.metrics_js.starts_with("/hench/assets/metrics."));
        assert_eq!(url("", "missing.txt"), "/assets/missing.txt");
        for asset in ASSETS.iter() {
            assert!(!asset.body.is_empty(), "{} is empty", asset.name);
        }
    }

    #[test]
    fn the_feature_stylesheets_are_registered() {
        let urls = AssetUrls::new("");
        for (name, url) in [
            ("periodic", &urls.periodic_css),
            ("batches", &urls.batches_css),
            ("limits", &urls.limits_css),
            ("locks", &urls.locks_css),
        ] {
            assert!(url.starts_with(&format!("/assets/{name}.")), "{url}");
            assert!(url.ends_with(".css"), "{url}");
            let file = url.trim_start_matches("/assets/");
            let asset = ASSETS.iter().find(|asset| asset.file == file).unwrap();
            assert_eq!(asset.content_type, CSS);
        }
    }
}
