//! The Limits tab (`docs/design/ratelimit.md` §2.15): the limit identities in use (their use,
//! counters and wait lines), the last hour's grants and refusals, the throttles live processes
//! declare, and each identity's page (holders, waiting jobs) with Reset and Clear wait line. A
//! job's page shows its Throttle and Limited rows.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use askama::Template;
use axum::Router;
use axum::extract::Path;
use axum::response::Response;
use axum::routing::get;
use hench::api::{Api, JobRecord};
use hench::limit::{Limit, LimitInfo, LimitKind, LimitOrder};
use hench::metrics::Period;
use http::StatusCode;
use serde::Deserialize;
use serde_json::Value;

use crate::config::HenchWeb;
use crate::ctx::Ctx;
use crate::error::{WebError, WebResult};
use crate::extension::JobInfoRow;
use crate::flash::{Notice, NoticeKind};
use crate::form::{self, PostForm};
use crate::format::{self, TimeView};
use crate::params::{Query, query_string};
use crate::strings::limits::{self as texts, Texts};
use crate::strings::{EN, Strings, errors};
use crate::templates::Layout;
use crate::views::{FeatureRows, Pager, Totals, last_page, segment};

/// The name of the feature in the heartbeat's `features` map of the processes.
pub(crate) const FEATURE: &str = "limits";

/// Identities a bulk reset takes at most.
const MAX_BULK: usize = 1_000;

/// Waiting jobs an identity's page lists at most.
const MAX_WAITING: usize = 100;

/// Names listed under "Most refused".
const MOST_REFUSED: usize = 5;

/// The tab's routes, mounted under `/limits` when its switch is on.
pub(crate) fn routes() -> Router<Arc<HenchWeb>> {
    Router::new()
        .route("/limits", get(list).post(bulk))
        .route("/limits/{kind}/{id}", get(detail).post(action))
}

/// The rows a job's detail page shows about the feature (Throttle and Limited), and the payload
/// fields they show. Malformed fields get no row and stay among the extra fields.
pub(crate) fn job_rows(ctx: &Ctx, record: &JobRecord) -> FeatureRows {
    let mut rows = FeatureRows::default();
    // Linked to the Limits pages when the tab is on.
    let tab = ctx.web.limits;
    if let Some(throttle) = record.throttle() {
        let text = texts::throttle_row(&throttle.name, throttle.key.as_deref());
        rows.rows.push(if tab {
            let href = format!(
                "{}{}",
                ctx.url("/limits"),
                query_string(&[("substr", throttle.name.as_str())])
            );
            JobInfoRow::link(texts::THROTTLE_LABEL, text, href)
        } else {
            JobInfoRow::text(texts::THROTTLE_LABEL, text)
        });
        rows.shown.push("throttle");
    }
    if let Some(limited) = record.limited() {
        let text = texts::limited_row(&limited.by, limited.n);
        let row = match split_identity(&limited.by).filter(|_| tab) {
            Some((kind, id)) => {
                JobInfoRow::link(texts::LIMITED_LABEL, text, detail_url(ctx, kind, id))
            }
            None => JobInfoRow::text(texts::LIMITED_LABEL, text),
        };
        rows.rows.push(row);
        rows.shown.push("limited");
    }
    rows
}

/// The chips a job list row shows about the feature (`throttle: stripe:42`, `over limit:
/// w:stripe:42 ×3`).
pub(crate) fn list_chips(record: &JobRecord) -> Vec<String> {
    let mut chips = Vec::new();
    if let Some(throttle) = record.throttle() {
        chips.push(texts::throttle_chip(
            &throttle.name,
            throttle.key.as_deref(),
        ));
    }
    if let Some(limited) = record.limited() {
        chips.push(texts::limited_chip(&limited.by, limited.n));
    }
    chips
}

/// Whether the feature is in use in this Redis (for the Busy page's "lacks" chip of processes
/// that do not understand it).
pub(crate) async fn in_use(ctx: &Ctx) -> WebResult<bool> {
    Ok(ctx.web.api().limits().size().await? > 0)
}

/// `limits.html`.
#[derive(Template)]
#[template(path = "limits.html")]
pub(crate) struct LimitsPage {
    pub(crate) layout: Layout,
    pub(crate) s: &'static Strings,
    pub(crate) t: &'static Texts,
    pub(crate) summary: String,
    pub(crate) most_refused: Vec<String>,
    pub(crate) throttles: Vec<ThrottleView>,
    pub(crate) total: String,
    pub(crate) rows: Vec<LimitRow>,
    pub(crate) pager: Option<Pager>,
    pub(crate) list_url: String,
    pub(crate) filter_action: String,
    pub(crate) filter_value: String,
    pub(crate) sorts: Vec<SortLink>,
    /// The `sort` parameter of the page, carried by the bulk form.
    pub(crate) sort: &'static str,
    pub(crate) empty_text: &'static str,
}

/// One identity of the list.
pub(crate) struct LimitRow {
    /// The identity as stored (`w:stripe:42`), as the value of the row's checkbox
    /// ([`form::field`]: keys may hold any character but NUL).
    pub(crate) identity: String,
    /// The identity with its control characters spelled out.
    pub(crate) label: String,
    pub(crate) href: String,
    pub(crate) kind: &'static str,
    pub(crate) spec: String,
    pub(crate) usage: UsageView,
    pub(crate) next_free: String,
    pub(crate) granted: String,
    pub(crate) refused: String,
    pub(crate) line: Option<String>,
    pub(crate) last_used: Option<TimeView>,
}

/// The use of a limit: its text (`37 / 50`) and its bar.
pub(crate) struct UsageView {
    pub(crate) text: String,
    /// The bar's fill, in steps of 5 % (`None` when the capacity is unknown).
    pub(crate) fill: Option<u8>,
    /// `ok`, `high` (from 80 %) or `full`.
    pub(crate) level: &'static str,
}

impl UsageView {
    fn of(info: &LimitInfo) -> Self {
        let fraction = info.fraction_used();
        let level = match fraction {
            Some(fraction) if fraction >= 1.0 => "full",
            Some(fraction) if fraction >= 0.8 => "high",
            _ => "ok",
        };
        Self {
            text: texts::usage(info.used, info.capacity()),
            fill: fraction.map(fill_step),
            level,
        }
    }
}

/// A fraction as a bar fill: 0 to 100, in steps of 5 (any use shows at least one step).
fn fill_step(fraction: f64) -> u8 {
    let percent = (fraction.clamp(0.0, 1.0) * 100.0).round();
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "clamped to 0..=100 just above"
    )]
    let percent = percent as u8;
    let step = (percent / 5) * 5;
    if step == 0 && fraction > 0.0 { 5 } else { step }
}

/// A throttle declared by live processes, with each spec they declare for it.
pub(crate) struct ThrottleView {
    pub(crate) name: String,
    pub(crate) href: String,
    /// The specs and how many processes declare each.
    pub(crate) specs: Vec<(String, String)>,
    /// Processes declare different specs.
    pub(crate) differs: bool,
}

/// A link of the sort selector.
pub(crate) struct SortLink {
    pub(crate) label: &'static str,
    pub(crate) href: String,
    pub(crate) active: bool,
}

/// `limit.html`.
#[derive(Template)]
#[template(path = "limit.html")]
pub(crate) struct LimitPage {
    pub(crate) layout: Layout,
    pub(crate) s: &'static Strings,
    pub(crate) t: &'static Texts,
    pub(crate) heading: String,
    pub(crate) back_href: String,
    pub(crate) action_href: String,
    pub(crate) kind: &'static str,
    pub(crate) usage: UsageView,
    /// Label and value of each fact.
    pub(crate) facts: Vec<(&'static str, String)>,
    pub(crate) has_line: bool,
    /// Concurrency limits only.
    pub(crate) holders: Option<Vec<HolderRow>>,
    pub(crate) waiting: Vec<WaitingRow>,
}

/// A holder of a concurrency limit.
pub(crate) struct HolderRow {
    pub(crate) token: String,
    /// The Busy page, when the token is the jid of a running job.
    pub(crate) busy_href: Option<String>,
    pub(crate) lease_ends: TimeView,
    pub(crate) ended: bool,
}

/// A scheduled job waiting for the identity.
pub(crate) struct WaitingRow {
    pub(crate) href: String,
    pub(crate) at: TimeView,
    pub(crate) class: String,
    pub(crate) jid: String,
    pub(crate) reschedules: String,
}

/// The `{kind}/{id}` of an identity's routes. Extracted by name, so that the dashboard also
/// works when nested under a path with parameters of its own.
#[derive(Deserialize)]
pub(crate) struct LimitPath {
    kind: String,
    id: String,
}

/// The sort orders of the list, by their `sort` parameter.
const SORTS: [(&str, LimitOrder); 3] = [
    ("last", LimitOrder::LastUsed),
    ("name", LimitOrder::Name),
    ("refused", LimitOrder::Refused),
];

/// The `sort` parameter: `last` (or missing), `name` or `refused`; anything else is refused.
fn sort_of(query: &Query) -> WebResult<(&'static str, LimitOrder)> {
    let wanted = query.get("sort").unwrap_or_default();
    if wanted.is_empty() {
        return Ok(SORTS[0]);
    }
    SORTS
        .into_iter()
        .find(|(param, _)| *param == wanted)
        .ok_or_else(|| {
            WebError::BadRequest("Unknown sort order: use last, name or refused.".to_owned())
        })
}

/// `GET /limits[?page=N][&count=N][&sort=last|name|refused][&substr=text]`.
pub(crate) async fn list(ctx: Ctx) -> Response {
    let result = render_list(&ctx).await;
    super::finish(&ctx, result)
}

async fn render_list(ctx: &Ctx) -> WebResult<Response> {
    let query = Query::parse(&ctx.query);
    let (sort, order) = sort_of(&query)?;
    let filter = query.filter()?;
    let count = query.count(ctx.web.page_size);
    let api = ctx.web.api();
    let limits = api.limits();
    let pattern = filter.as_deref().unwrap_or_default();
    let mut page = limits
        .page_matching(query.page(), count, order, pattern)
        .await?;
    // Past the end (its last rows were just reset): the last page instead.
    if let Some(last) = last_page(&page) {
        page = limits.page_matching(last, count, order, pattern).await?;
    }
    let in_use = limits.size().await?;
    let events = limits.events(Period::Minutes(60)).await?;
    let stats = api.stats().await?;
    let list_url = ctx.url("/limits");
    let rows: Vec<LimitRow> = page
        .items
        .iter()
        .map(|info| LimitRow {
            identity: form::field(&info.identity()),
            label: format::visible(&info.identity()).into_owned(),
            href: detail_url(ctx, info.kind, &info.id),
            kind: kind_label(info),
            spec: spec_text(info.spec.as_ref()),
            usage: UsageView::of(info),
            next_free: info
                .next_free_in
                .map_or_else(|| texts::EN.unknown.to_owned(), texts::wait),
            granted: format::delimit(info.granted),
            refused: format::delimit(info.refused),
            line: line_text(ctx, info),
            last_used: info.last_used.map(|at| TimeView::new(at, ctx.now)),
        })
        .collect();
    let sort_link = |param: &str| {
        let count = (count != ctx.web.page_size).then(|| count.to_string());
        let sort = (param != "last").then_some(param);
        format!(
            "{list_url}{}",
            query_string(&[
                ("sort", sort.unwrap_or_default()),
                ("count", count.as_deref().unwrap_or_default()),
                ("substr", filter.as_deref().unwrap_or_default()),
            ])
        )
    };
    let sorts = [
        (texts::EN.sort_last, "last"),
        (texts::EN.sort_name, "name"),
        (texts::EN.sort_refused, "refused"),
    ]
    .into_iter()
    .map(|(label, param)| SortLink {
        label,
        href: sort_link(param),
        active: param == sort,
    })
    .collect();
    let pager = Pager::new(ctx, &list_url, page.number, page.size, page.total);
    let title = texts::list_title(in_use);
    let page = LimitsPage {
        layout: super::layout(ctx, Some(&title), Some(Totals::new(&stats)), true),
        s: &EN,
        t: &texts::EN,
        summary: texts::summary(in_use, events.totals.0, events.totals.1),
        most_refused: events
            .most_refused(MOST_REFUSED)
            .into_iter()
            .map(|(name, _, refused)| texts::refused_name(&name, refused))
            .collect(),
        throttles: declared_throttles(ctx, api).await?,
        total: format::delimit(page.total),
        rows,
        pager,
        filter_action: list_url.clone(),
        list_url,
        filter_value: filter.clone().unwrap_or_default(),
        sorts,
        sort,
        empty_text: if filter.is_some() {
            texts::EN.no_matches
        } else {
            texts::EN.empty
        },
    };
    Ok(super::render(ctx, StatusCode::OK, &page))
}

/// The throttles the live processes declare in their heartbeats, by name, with each spec and
/// how many processes declare it.
async fn declared_throttles(ctx: &Ctx, api: &Api) -> WebResult<Vec<ThrottleView>> {
    let mut by_name: BTreeMap<String, BTreeMap<String, usize>> = BTreeMap::new();
    for process in api.processes().list().await? {
        let Some(throttles) = process.get("throttles").and_then(Value::as_object) else {
            continue;
        };
        for (name, spec) in throttles {
            let spec = spec.as_str().unwrap_or_default().to_owned();
            *by_name
                .entry(name.clone())
                .or_default()
                .entry(spec)
                .or_default() += 1;
        }
    }
    Ok(by_name
        .into_iter()
        .map(|(name, specs)| ThrottleView {
            href: format!(
                "{}{}",
                ctx.url("/limits"),
                query_string(&[("substr", name.as_str())])
            ),
            name: format::visible(&name).into_owned(),
            differs: specs.len() > 1,
            specs: specs
                .into_iter()
                .map(|(spec, count)| {
                    (
                        format::visible(&spec).into_owned(),
                        texts::process_count(count),
                    )
                })
                .collect(),
        })
        .collect())
}

/// `POST /limits`: resets the selected identities (`identity` fields, at most 1000).
pub(crate) async fn bulk(ctx: Ctx, form: PostForm) -> Response {
    let result = reset_selected(&ctx, &form).await;
    super::finish(&ctx, result)
}

async fn reset_selected(ctx: &Ctx, form: &PostForm) -> WebResult<Response> {
    // Back to the list as it was: `back_to` carries the page, size and filter, the form its sort.
    let mut back = ctx.back_to("/limits");
    if let Some(sort) = form
        .get("sort")
        .filter(|sort| matches!(*sort, "name" | "refused"))
    {
        back.push(if back.contains('?') { '&' } else { '?' });
        back.push_str("sort=");
        back.push_str(sort);
    }
    if !form.has("reset") {
        return Err(WebError::BadRequest(errors::NO_ACTION.to_owned()));
    }
    let selected: Vec<_> = form.values("identity").take(MAX_BULK).collect();
    if selected.is_empty() {
        return Ok(ctx.redirect(&back, Some(Notice::new(NoticeKind::NothingSelected, 0, 0))));
    }
    let limits = ctx.web.api().limits();
    let (mut reset, mut failed) = (0_u64, 0_u64);
    for identity in &selected {
        let identity = identity.as_ref();
        let Some((kind, id)) = split_identity(identity) else {
            failed += 1;
            continue;
        };
        // Counted when it was in use: an identity whose counters expired meanwhile (or that
        // never was) is not reset by this request, like a job already gone in the other lists.
        match limits.reset(kind, id).await {
            Ok(true) => reset += 1,
            Ok(false) => {}
            Err(error) => {
                tracing::warn!(%error, identity, "dashboard could not reset a limit");
                failed += 1;
            }
        }
    }
    tracing::info!(reset, failed, "dashboard reset limits");
    Ok(ctx.redirect(
        &back,
        Some(Notice::new(NoticeKind::LimitsReset, reset, failed)),
    ))
}

/// `GET /limits/{kind}/{id}`: 404 for an identity not in use.
pub(crate) async fn detail(ctx: Ctx, Path(LimitPath { kind, id }): Path<LimitPath>) -> Response {
    let result = render_detail(&ctx, &kind, &id).await;
    super::finish(&ctx, result)
}

async fn render_detail(ctx: &Ctx, kind: &str, id: &str) -> WebResult<Response> {
    let kind = kind_of(kind, id)?;
    let api = ctx.web.api();
    let limits = api.limits();
    let identity = format!("{}:{id}", kind.letter());
    let Some(info) = limits.get(kind, id).await? else {
        return Err(WebError::NotFound(texts::not_in_use(&identity)));
    };
    let holders = if kind == LimitKind::Concurrency {
        let holders = limits.holders(&info.id).await?;
        let running: HashSet<String> = if holders.is_empty() {
            HashSet::new()
        } else {
            api.work()
                .list()
                .await?
                .iter()
                .filter_map(|work| work.job().jid().map(str::to_owned))
                .collect()
        };
        Some(
            holders
                .into_iter()
                .map(|holder| HolderRow {
                    // A throttled job's token names its jid (older workers used the jid alone).
                    busy_href: running
                        .contains(holder.jid.as_deref().unwrap_or(&holder.token))
                        .then(|| ctx.url("/busy")),
                    token: format::visible(&holder.token).into_owned(),
                    lease_ends: TimeView::new(holder.lease_ends_at, ctx.now),
                    ended: holder.ended,
                })
                .collect(),
        )
    } else {
        None
    };
    let waiting = limits
        .waiting(kind, &info.id, MAX_WAITING)
        .await?
        .iter()
        .map(|entry| {
            let record = entry.record();
            WaitingRow {
                href: ctx.url(&format!("/scheduled/{}", segment(&entry.key()))),
                at: TimeView::new(entry.at(), ctx.now),
                class: format::visible(record.display_class().unwrap_or_default()).into_owned(),
                jid: record.jid().unwrap_or_default().to_owned(),
                reschedules: record
                    .limited()
                    .map(|limited| limited.n.to_string())
                    .unwrap_or_default(),
            }
        })
        .collect();
    let heading = texts::limit_heading(&info.identity());
    let page = LimitPage {
        // Not Live: each view looks for the waiting jobs among the 50,000 earliest scheduled
        // ones (round 3, web-api-security-8), which a page refreshing itself would repeat.
        layout: super::layout(ctx, Some(&heading), None, false),
        s: &EN,
        t: &texts::EN,
        heading,
        back_href: ctx.url("/limits"),
        action_href: detail_url(ctx, info.kind, &info.id),
        kind: kind_label(&info),
        usage: UsageView::of(&info),
        facts: facts(ctx, &info),
        has_line: info.line_ends_at.is_some(),
        holders,
        waiting,
    };
    Ok(super::render(ctx, StatusCode::OK, &page))
}

/// The facts of an identity's page, in order.
fn facts(ctx: &Ctx, info: &LimitInfo) -> Vec<(&'static str, String)> {
    let t = &texts::EN;
    let count = format::delimit;
    let average = |duration: Option<Duration>| {
        duration.map_or_else(
            || "–".to_owned(),
            |duration| format::millis(millis(duration)),
        )
    };
    let mut facts = vec![
        (t.identity, format::visible(&info.identity()).into_owned()),
        (t.name, format::visible(&info.name).into_owned()),
    ];
    if let Some(key) = &info.key {
        facts.push((t.key, format::visible(key).into_owned()));
    }
    if let Some(queue) = &info.queue {
        facts.push((t.queue, format::visible(queue).into_owned()));
    }
    facts.extend([
        (t.spec, spec_text(info.spec.as_ref())),
        (
            t.next_free,
            info.next_free_in
                .map_or_else(|| t.unknown.to_owned(), texts::wait),
        ),
        (t.granted, count(info.granted)),
        (t.refused, count(info.refused)),
        (t.waited, count(info.waited)),
        (t.average_wait, average(info.average_wait())),
    ]);
    if info.kind == LimitKind::Concurrency {
        facts.extend([
            (t.releases, count(info.releases)),
            (t.average_hold, average(info.average_hold())),
            (t.reclaimed, count(info.reclaimed)),
            (t.overages, count(info.overages)),
            (t.restored, count(info.restored)),
        ]);
    }
    if matches!(info.spec, Some(Limit::Points { .. })) || info.adjustments > 0 {
        facts.push((t.adjustments, count(info.adjustments)));
    }
    facts.push((
        t.wait_line,
        line_text(ctx, info).unwrap_or_else(|| t.no_line.to_owned()),
    ));
    facts.push((
        t.last_used,
        info.last_used.map_or_else(
            || t.never.to_owned(),
            |at| {
                let at = TimeView::new(at, ctx.now);
                format!("{} ({})", at.text, at.utc)
            },
        ),
    ));
    facts
}

/// `POST /limits/{kind}/{id}`: `reset` (back to the list) or `clear_line` (back to the page).
pub(crate) async fn action(
    ctx: Ctx,
    Path(LimitPath { kind, id }): Path<LimitPath>,
    form: PostForm,
) -> Response {
    let result = act(&ctx, &kind, &id, &form).await;
    super::finish(&ctx, result)
}

async fn act(ctx: &Ctx, kind: &str, id: &str, form: &PostForm) -> WebResult<Response> {
    let kind = kind_of(kind, id)?;
    let limits = ctx.web.api().limits();
    let identity = format!("{}:{id}", kind.letter());
    if form.has("reset") {
        limits.reset(kind, id).await?;
        tracing::info!(identity, "dashboard reset a limit");
        Ok(ctx.redirect(
            &ctx.back_to("/limits"),
            Some(Notice::about(NoticeKind::LimitsReset, &identity)),
        ))
    } else if form.has("clear_line") {
        limits.clear_line(kind, id).await?;
        tracing::info!(identity, "dashboard cleared the wait line of a limit");
        Ok(ctx.redirect(
            &detail_url(ctx, kind, id),
            Some(Notice::about(NoticeKind::LineCleared, &identity)),
        ))
    } else {
        Err(WebError::BadRequest(errors::NO_ACTION.to_owned()))
    }
}

/// The kind of a path's `{kind}` (its letter: `c`, `w`, `b`, `g`); anything else is not found.
fn kind_of(kind: &str, id: &str) -> WebResult<LimitKind> {
    let mut letters = kind.chars();
    match (
        letters.next().and_then(LimitKind::from_letter),
        letters.next(),
    ) {
        (Some(kind), None) if !id.is_empty() => Ok(kind),
        _ => Err(WebError::NotFound(texts::not_in_use(&format!(
            "{kind}:{id}"
        )))),
    }
}

/// The kind and id of an identity (`w:stripe:42`).
fn split_identity(identity: &str) -> Option<(LimitKind, &str)> {
    let (letter, id) = identity.split_once(':')?;
    let mut letters = letter.chars();
    let kind = LimitKind::from_letter(letters.next()?)?;
    (letters.next().is_none() && !id.is_empty()).then_some((kind, id))
}

/// The page of an identity.
fn detail_url(ctx: &Ctx, kind: LimitKind, id: &str) -> String {
    ctx.url(&format!("/limits/{}/{}", kind.letter(), segment(id)))
}

/// The kind chip of an identity (`points` for the points budgets of the leaky kind).
fn kind_label(info: &LimitInfo) -> &'static str {
    texts::kind_name(info.kind, matches!(info.spec, Some(Limit::Points { .. })))
}

/// A limit in the spec grammar (`window=100/1s`), or "unknown".
fn spec_text(spec: Option<&Limit>) -> String {
    spec.map_or_else(|| texts::EN.unknown_spec.to_owned(), ToString::to_string)
}

/// The wait line column (`line clears in 4 min`), when someone waits.
fn line_text(ctx: &Ctx, info: &LimitInfo) -> Option<String> {
    let ends = info.line_ends_at?;
    let wait = ends.duration_since(ctx.now).unwrap_or(Duration::ZERO);
    Some(texts::line_clears(wait))
}

/// Whole milliseconds of a duration (saturating).
fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identities_split_into_kind_and_id() {
        assert_eq!(
            split_identity("w:stripe:42"),
            Some((LimitKind::Window, "stripe:42"))
        );
        assert_eq!(
            split_identity("c:erp"),
            Some((LimitKind::Concurrency, "erp"))
        );
        assert_eq!(split_identity("x:erp"), None);
        assert_eq!(split_identity("cw:erp"), None);
        assert_eq!(split_identity("w:"), None);
        assert_eq!(split_identity(""), None);
    }

    #[test]
    fn bars_fill_in_steps() {
        assert_eq!(fill_step(0.0), 0);
        assert_eq!(fill_step(0.01), 5);
        assert_eq!(fill_step(0.37), 35);
        assert_eq!(fill_step(0.74), 70);
        assert_eq!(fill_step(1.0), 100);
        assert_eq!(fill_step(3.5), 100);
        assert_eq!(fill_step(-1.0), 0);
    }

    #[test]
    fn sorts_are_checked() {
        assert_eq!(sort_of(&Query::parse("")).unwrap().0, "last");
        assert_eq!(sort_of(&Query::parse("sort=name")).unwrap().0, "name");
        assert_eq!(
            sort_of(&Query::parse("sort=refused")).unwrap().1,
            LimitOrder::Refused
        );
        assert!(sort_of(&Query::parse("sort=size")).is_err());
    }
}
