//! The router and the page handlers.

mod batches;
mod busy;
mod dashboard;
mod limits;
mod locks;
mod metrics;
mod periodic;
mod queues;
mod sets;
mod stats;

use std::sync::Arc;

use askama::Template;
use axum::Router;
use axum::middleware;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use http::StatusCode;

use crate::assets::{self, AssetUrls};
use crate::config::HenchWeb;
use crate::ctx::Ctx;
use crate::error::{WebError, WebResult};
use crate::format;
use crate::guard;
use crate::strings::{self, EN, errors};
use crate::templates::{ErrorPage, Footer, Layout, NavLink, NoticeView};
use crate::views::{IterationView, JobRow, SHORT_CURSOR, Totals};

/// Builds the dashboard's router (see [`crate::router`]): its pages, the host's routes
/// ([`HenchWeb::routes`]), and around both the guard.
pub(crate) fn router(mut web: HenchWeb) -> Router {
    let extensions = web.extensions.take();
    let web = Arc::new(web);
    let mut router = Router::new()
        .route("/", get(dashboard::page).head(dashboard::heartbeat))
        .route("/stats", get(stats::stats))
        .route("/stats/queues", get(stats::queues))
        .route("/dashboard/stats", get(stats::legacy_redirect))
        .route("/busy", get(busy::page).post(busy::action))
        .route("/queues", get(queues::list))
        .route("/queues/{name}", get(queues::page).post(queues::action))
        .route("/queues/{name}/delete", post(queues::delete_job))
        .route("/retries", get(sets::retries).post(sets::retries_bulk))
        .route("/retries/all/{action}", post(sets::retries_all))
        .route(
            "/retries/{key}",
            get(sets::retry_entry).post(sets::retry_entry_action),
        )
        .route(
            "/scheduled",
            get(sets::scheduled).post(sets::scheduled_bulk),
        )
        .route("/scheduled/all/{action}", post(sets::scheduled_all))
        .route(
            "/scheduled/{key}",
            get(sets::scheduled_entry).post(sets::scheduled_entry_action),
        )
        .route("/morgue", get(sets::morgue).post(sets::morgue_bulk))
        .route("/morgue/all/{action}", post(sets::morgue_all))
        .route(
            "/morgue/{key}",
            get(sets::dead_entry).post(sets::dead_entry_action),
        )
        .route("/metrics", get(metrics::overview))
        .route("/metrics/{class}", get(metrics::job))
        .route("/assets/{file}", get(assets::serve));
    // The tabs of the features, each mounted when its switch is on (off, its pages answer 404,
    // like the Metrics pages).
    for (on, routes) in [
        (web.periodic, periodic::routes()),
        (web.batches, batches::routes()),
        (web.limits, limits::routes()),
        (web.locks, locks::routes()),
    ] {
        if on {
            router = router.merge(routes);
        }
    }
    let mut router = router.fallback(not_found).with_state(Arc::clone(&web));
    if let Some(extensions) = extensions {
        router = router.merge(extensions);
    }
    // Last, so that it wraps the host's routes too.
    router.layer(middleware::from_fn_with_state(web, guard::guard))
}

/// Unknown paths.
async fn not_found(ctx: Ctx) -> Response {
    error_page(&ctx, WebError::NotFound(errors::NO_SUCH_PAGE.to_owned()))
}

/// Which built-in section a dashboard path belongs to (for the active tab).
fn section_of(path: &str) -> Option<&'static str> {
    let first = path.trim_start_matches('/').split('/').next()?;
    Some(match first {
        "" => "",
        "busy" => "busy",
        "queues" => "queues",
        "retries" => "retries",
        "scheduled" => "scheduled",
        "morgue" => "morgue",
        "periodic" => "periodic",
        "batches" => "batches",
        "limits" => "limits",
        "locks" => "locks",
        "metrics" => "metrics",
        _ => return None,
    })
}

/// The navigation: built-in tabs (Metrics unless turned off), then custom tabs.
fn nav(ctx: &Ctx) -> Vec<NavLink> {
    let section = section_of(&ctx.path);
    let mut builtin: Vec<(&str, &str)> = vec![
        (EN.dashboard, ""),
        (EN.busy, "busy"),
        (EN.queues, "queues"),
        (EN.retries, "retries"),
        (EN.scheduled, "scheduled"),
        (EN.dead, "morgue"),
    ];
    // The tabs of the features follow the job sets, in this order.
    let web = &ctx.web;
    for (on, name, path) in [
        (web.periodic, strings::periodic::TAB, "periodic"),
        (web.batches, strings::batches::TAB, "batches"),
        (web.limits, strings::limits::TAB, "limits"),
        (web.locks, strings::locks::TAB, "locks"),
    ] {
        if on {
            builtin.push((name, path));
        }
    }
    if ctx.web.metrics {
        builtin.push((EN.metrics, "metrics"));
    }
    let mut links: Vec<NavLink> = builtin
        .into_iter()
        .map(|(name, path)| NavLink {
            name: name.to_owned(),
            href: if path.is_empty() {
                ctx.home()
            } else {
                ctx.url(&format!("/{path}"))
            },
            active: section == Some(path),
        })
        .collect();
    links.extend(ctx.web.tabs.iter().map(|tab| NavLink {
        name: tab.name().to_owned(),
        href: tab.href(&ctx.prefix),
        active: tab.is_active(&ctx.path),
    }));
    links
}

/// The rows a job's detail page shows about the features, in their fixed order: Batch,
/// Periodic, Unique, then Throttle and Limited (the host's `job_info` rows come after).
pub(crate) fn feature_rows(ctx: &Ctx, record: &hench::api::JobRecord) -> crate::views::FeatureRows {
    let mut rows = crate::views::FeatureRows::default();
    for part in [
        batches::job_rows(ctx, record),
        periodic::job_rows(ctx, record),
        locks::job_rows(ctx, record),
        limits::job_rows(ctx, record),
    ] {
        rows.extend(part);
    }
    rows
}

/// The chips a job list row shows next to the class about the features, in the order of the
/// job pages' rows: `callback` (a batch callback), `unique`, then the throttle and the
/// over-limit reschedules (`docs/design/pro-features.md` §2.8).
pub(crate) fn feature_chips(record: &hench::api::JobRecord) -> Vec<String> {
    let mut chips = Vec::new();
    if record.batch_callback().is_some() {
        chips.push(strings::batches::CALLBACK_CHIP.to_owned());
    }
    if record.unique().is_some() {
        chips.push(strings::locks::UNIQUE_CHIP.to_owned());
    }
    chips.extend(limits::list_chips(record));
    chips
}

/// The features in use in this Redis, by their names in the heartbeat's `features` map (for
/// the Busy page's "lacks" chip). Features whose tab is off are not checked.
pub(crate) async fn features_in_use(ctx: &Ctx) -> WebResult<Vec<&'static str>> {
    let web = &ctx.web;
    let mut in_use = Vec::new();
    if web.batches && batches::in_use(ctx).await? {
        in_use.push(batches::FEATURE);
    }
    if web.periodic && periodic::in_use(ctx).await? {
        in_use.push(periodic::FEATURE);
    }
    if web.locks && locks::in_use(ctx).await? {
        in_use.push(locks::FEATURE);
    }
    if web.limits && limits::in_use(ctx).await? {
        in_use.push(limits::FEATURE);
    }
    Ok(in_use)
}

/// The data of the shared layout for a page titled `title` (`None`: the product name alone).
pub(crate) fn layout(ctx: &Ctx, title: Option<&str>, totals: Option<Totals>, live: bool) -> Layout {
    let web = &ctx.web;
    Layout {
        product: web.title.clone(),
        window_title: strings::page_title(title, &web.title),
        prefix: ctx.prefix.clone(),
        home: ctx.home(),
        nav: nav(ctx),
        csrf: ctx.csrf.clone(),
        notice: ctx.notice.as_ref().map(|notice| NoticeView {
            text: strings::notice(notice),
            warning: notice.is_warning(),
        }),
        read_only: web.read_only,
        app_url: web.app_url.clone(),
        poll_ms: web.poll_interval.as_millis(),
        live,
        totals,
        assets: AssetUrls::new(&ctx.prefix),
        now_ms: format::epoch_millis(ctx.now),
        footer: Footer::new(web.api.redis().config().redacted_url(), ctx.now),
    }
}

/// Adds the progress of the iterable jobs among `rows` (one round trip for the page).
pub(crate) async fn add_iterations(ctx: &Ctx, rows: &mut [JobRow]) -> WebResult<()> {
    let states = hench::iterable::IterationState::fetch_many(
        ctx.web.api().redis(),
        rows.iter().map(|row| row.jid.as_str()),
    )
    .await?;
    for row in rows {
        if let Some(state) = states.get(&row.jid) {
            row.iteration = Some(IterationView::new(state, SHORT_CURSOR));
        }
    }
    Ok(())
}

/// Renders a template into a response with `status`.
pub(crate) fn render(ctx: &Ctx, status: StatusCode, page: &impl Template) -> Response {
    match page.render() {
        Ok(html) => ctx.html(status, html),
        Err(error) => {
            tracing::error!(%error, path = ctx.path, "could not render a dashboard page");
            (StatusCode::INTERNAL_SERVER_ERROR, errors::INTERNAL).into_response()
        }
    }
}

/// The error page for `error`.
pub(crate) fn error_page(ctx: &Ctx, error: WebError) -> Response {
    error.log(&ctx.path);
    let status = error.status();
    let page = ErrorPage {
        layout: layout(ctx, Some(error.heading()), None, false),
        s: &EN,
        code: status.as_u16(),
        heading: error.heading(),
        message: error.message(),
    };
    render(ctx, status, &page)
}

/// A handler's outcome as a response: errors become error pages.
pub(crate) fn finish(ctx: &Ctx, result: WebResult<Response>) -> Response {
    result.unwrap_or_else(|error| error_page(ctx, error))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sections_match_whole_segments() {
        assert_eq!(section_of("/"), Some(""));
        assert_eq!(section_of(""), Some(""));
        assert_eq!(section_of("/busy"), Some("busy"));
        assert_eq!(section_of("/queues/default"), Some("queues"));
        assert_eq!(section_of("/retries/12.5-abc"), Some("retries"));
        assert_eq!(section_of("/metrics"), Some("metrics"));
        assert_eq!(section_of("/metrics/ImportJob"), Some("metrics"));
        assert_eq!(section_of("/busyness"), None);
        assert_eq!(section_of("/custom"), None);
    }
}
