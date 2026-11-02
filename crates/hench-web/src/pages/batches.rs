//! The Batches tab (`docs/design/batches.md` §2.12): the batches by state (progress, counts,
//! state chips), a batch's page (facts, callbacks, failures, children, pending jobs) with
//! Cancel, Delete, Skip and Push callbacks, and the Batch rows of job pages.

use std::sync::Arc;
use std::time::SystemTime;

use askama::Template;
use axum::Router;
use axum::extract::Path;
use axum::response::Response;
use axum::routing::get;
use hench::api::{Api, JobRecord};
use hench::batch::{
    BatchError, BatchEvent, BatchFilter, BatchState, BatchStatus, BatchSummary, Batches,
    FailedMember,
};
use hench::metrics::Period;
use hench::{keys, redis_rs};
use http::StatusCode;
use serde::Deserialize;

use crate::config::HenchWeb;
use crate::ctx::Ctx;
use crate::error::{WebError, WebResult};
use crate::extension::JobInfoRow;
use crate::flash::{Notice, NoticeKind};
use crate::form::{self, PostForm};
use crate::format::{self, TimeView};
use crate::params::{Query, query_string};
use crate::strings::batches::{self as texts, Texts};
use crate::strings::{EN, Strings, errors};
use crate::templates::Layout;
use crate::views::{FeatureRows, Pager, Totals, segment};

/// The name of the feature in the heartbeat's `features` map of the processes.
pub(crate) const FEATURE: &str = "batches";

/// Characters of a description shown in a list row.
const DESCRIPTION_CHARS: usize = 120;

/// Characters of an error message shown in the failures table.
const MESSAGE_CHARS: usize = 200;

/// Characters of a callback's arguments shown in its table.
const ARGS_CHARS: usize = 120;

/// Failures, children and pending jobs shown per page of a batch's page.
const PER_PAGE: usize = 25;

/// Entries of its set the lookup of a failed member's job reads at most: the first entry at
/// the job's place, then the next ones when another job came first.
const LOCATE_ENTRIES: u64 = 10;

/// The scores around a retry's own that its lookup accepts, at least: the failure summary
/// records the retry set's score, which reaches the page through a `SystemTime` (exact to the
/// nanosecond), then a float again; a few units in the last place of the score for scores so
/// large that this is more.
const RETRY_SLACK_SECS: f64 = 1e-6;

/// How long after a member's death its morgue entry is looked for. The job is written to the
/// morgue as soon as its batch has recorded the failure (or the API kill), unless Redis became
/// unreachable in between; the morgue's score is taken after the batch's time of death, which is
/// whole milliseconds (hence the millisecond before it).
const MORGUE_WINDOW_SECS: (f64, f64) = (0.001, 60.0);

/// The `state` filters of the list, by parameter, `active` first (the default).
const STATES: [&str; 6] = [
    "active",
    "dead",
    "complete",
    "succeeded",
    "cancelled",
    "all",
];

/// The tab's routes, mounted under `/batches` when its switch is on.
pub(crate) fn routes() -> Router<Arc<HenchWeb>> {
    Router::new()
        .route("/batches", get(list))
        .route("/batches/{bid}", get(detail).post(action))
}

/// The rows a job's detail page shows about the feature: the batch a member belongs to, and
/// the batch and event a callback runs for, each linking to the batch's page.
pub(crate) fn job_rows(ctx: &Ctx, record: &JobRecord) -> FeatureRows {
    let mut rows = FeatureRows::default();
    // Linked to the batch's page when the tab is on.
    let row = |label: &str, text: String, bid: &str| {
        if ctx.web.batches {
            JobInfoRow::link(label, text, batch_url(ctx, bid))
        } else {
            JobInfoRow::text(label, text)
        }
    };
    if let Some(bid) = record.bid() {
        rows.rows
            .push(row(texts::EN.member_label, texts::member_row(bid), bid));
    }
    if let Some(callback) = record.batch_callback() {
        rows.rows.push(row(
            texts::EN.callback_label,
            texts::callback_row(callback.event().as_str(), callback.bid()),
            callback.bid(),
        ));
        rows.shown.push("batch_callback");
    }
    rows
}

/// Whether the feature is in use in this Redis (for the Busy page's "lacks" chip of processes
/// that do not understand it).
pub(crate) async fn in_use(ctx: &Ctx) -> WebResult<bool> {
    Ok(ctx.web.api().batches().size().await? > 0)
}

/// `batches.html`.
#[derive(Template)]
#[template(path = "batches.html")]
pub(crate) struct BatchesPage {
    pub(crate) layout: Layout,
    pub(crate) s: &'static Strings,
    pub(crate) t: &'static Texts,
    pub(crate) summary: String,
    pub(crate) searched: Option<String>,
    pub(crate) states: Vec<StateLink>,
    pub(crate) state_name: &'static str,
    pub(crate) total: String,
    pub(crate) rows: Vec<BatchRow>,
    pub(crate) pager: Option<Pager>,
    pub(crate) filter_action: String,
    pub(crate) filter_value: String,
    /// The `state` parameter of the page, carried by the filter form.
    pub(crate) state: &'static str,
    pub(crate) empty_text: &'static str,
}

/// A link of the state selector.
pub(crate) struct StateLink {
    pub(crate) label: &'static str,
    pub(crate) href: String,
    pub(crate) active: bool,
}

/// One batch of a list.
pub(crate) struct BatchRow {
    pub(crate) bid: String,
    pub(crate) href: String,
    /// The description, cut for the list.
    pub(crate) description: String,
    /// The whole description, when it was cut.
    pub(crate) description_full: Option<String>,
    pub(crate) created: TimeView,
    pub(crate) progress: ProgressView,
    pub(crate) total: String,
    pub(crate) pending: String,
    pub(crate) failures: String,
    pub(crate) dead: String,
    pub(crate) children: String,
    pub(crate) chips: Vec<Chip>,
    pub(crate) expires: Option<TimeView>,
}

/// A state chip: its text and its class.
pub(crate) struct Chip {
    pub(crate) text: &'static str,
    pub(crate) class: &'static str,
}

/// A batch's progress bar: its segments, their accessible label and the counts beside it.
pub(crate) struct ProgressView {
    pub(crate) segments: Vec<Segment>,
    pub(crate) label: String,
    pub(crate) counts: String,
}

/// A segment of a progress bar: its kind (`succeeded`, `retrying`, `dead`, `skipped`) and its
/// width in steps of 5 %.
pub(crate) struct Segment {
    pub(crate) kind: &'static str,
    pub(crate) width: u8,
}

/// The counts behind a progress bar.
struct Counts {
    total: u64,
    pending: u64,
    failures: u64,
    dead: u64,
    skipped: u64,
}

impl Counts {
    fn of_summary(summary: &BatchSummary) -> Self {
        Self {
            total: summary.total,
            pending: summary.pending,
            failures: summary.failures,
            dead: summary.dead,
            skipped: summary.skipped,
        }
    }

    fn of_status(status: &BatchStatus) -> Self {
        Self {
            total: status.total,
            pending: status.pending,
            failures: status.failures,
            dead: status.dead,
            skipped: status.skipped,
        }
    }
}

impl ProgressView {
    fn of(counts: &Counts) -> Self {
        // Pending members include those that failed (retrying or dead).
        let succeeded = counts
            .total
            .saturating_sub(counts.pending)
            .saturating_sub(counts.skipped);
        let dead = counts.dead.min(counts.failures);
        let retrying = counts.failures.saturating_sub(dead);
        let waiting = counts.pending.saturating_sub(counts.failures);
        let parts = [
            ("succeeded", succeeded),
            ("retrying", retrying),
            ("dead", dead),
            ("skipped", counts.skipped),
        ];
        let widths = widths(&parts.map(|(_, count)| count), counts.total);
        Self {
            segments: parts
                .iter()
                .zip(widths)
                .filter(|(_, width)| *width > 0)
                .map(|((kind, _), width)| Segment { kind, width })
                .collect(),
            label: texts::progress_label(
                counts.total,
                succeeded,
                retrying,
                dead,
                counts.skipped,
                waiting,
            ),
            counts: texts::progress_counts(succeeded + counts.skipped, counts.total),
        }
    }
}

/// The widths of a bar's segments out of `total`, in steps of 5 % (20 steps): the largest
/// remainders get the steps left over, and a segment with any count gets at least one step
/// (taken from the largest), so that no state is hidden by rounding.
fn widths(counts: &[u64; 4], total: u64) -> [u8; 4] {
    const STEPS: u64 = 20;
    let mut steps = [0_u64; 4];
    if total == 0 {
        return [0; 4];
    }
    let mut remainders = [(0_u64, 0_usize); 4];
    for (index, count) in counts.iter().enumerate() {
        let scaled = count.saturating_mul(STEPS);
        steps[index] = scaled / total;
        remainders[index] = (scaled % total, index);
    }
    let shown: u64 = counts.iter().sum::<u64>().min(total);
    let target = shown.saturating_mul(STEPS) / total
        + u64::from(shown.saturating_mul(STEPS) % total * 2 >= total && shown > 0);
    remainders.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
    let mut given: u64 = steps.iter().sum();
    for (_, index) in remainders {
        if given >= target {
            break;
        }
        if counts[index] > 0 {
            steps[index] += 1;
            given += 1;
        }
    }
    for index in 0..4 {
        if counts[index] > 0 && steps[index] == 0 {
            if let Some(largest) = (0..4).max_by_key(|&other| steps[other])
                && steps[largest] > 1
            {
                steps[largest] -= 1;
            }
            steps[index] = 1;
        }
    }
    steps.map(|step| u8::try_from(step.min(STEPS) * 5).unwrap_or(100))
}

/// The state chips of a batch: its state when it is final or dead, `complete` once that
/// event fired, and `dispatch pending` while callbacks wait to be pushed.
fn chips(state: BatchState, complete: bool, undispatched: bool) -> Vec<Chip> {
    let mut chips = Vec::new();
    match state {
        BatchState::Succeeded => chips.push(Chip {
            text: "succeeded",
            class: "batch-state-succeeded",
        }),
        BatchState::Cancelled => chips.push(Chip {
            text: "cancelled",
            class: "batch-state-cancelled",
        }),
        BatchState::Dead => chips.push(Chip {
            text: "dead",
            class: "batch-state-dead",
        }),
        _ => {}
    }
    if complete {
        chips.push(Chip {
            text: "complete",
            class: "batch-state-complete",
        });
    }
    if undispatched {
        chips.push(Chip {
            text: "dispatch pending",
            class: "batch-state-dispatch",
        });
    }
    chips
}

impl BatchRow {
    fn of(ctx: &Ctx, summary: &BatchSummary) -> Self {
        let description = summary.description.as_deref().unwrap_or_default();
        let visible = format::visible(description);
        let (cut, was_cut) = format::truncate(&visible, DESCRIPTION_CHARS);
        Self {
            bid: summary.bid.clone(),
            href: batch_url(ctx, &summary.bid),
            description: cut,
            description_full: was_cut.then(|| visible.into_owned()),
            created: TimeView::new(summary.created_at, ctx.now),
            progress: ProgressView::of(&Counts::of_summary(summary)),
            total: format::delimit(summary.total),
            pending: format::delimit(summary.pending),
            failures: format::delimit(summary.failures),
            dead: format::delimit(summary.dead),
            children: format::delimit(summary.children),
            chips: chips(
                summary.state(),
                summary.complete_at.is_some(),
                summary.undispatched,
            ),
            expires: summary.expires_at.map(|at| TimeView::new(at, ctx.now)),
        }
    }
}

/// The `state` parameter: `active` (or missing), `dead`, `complete`, `succeeded`, `cancelled`
/// or `all`; anything else is refused.
fn state_of(query: &Query) -> WebResult<(&'static str, BatchFilter)> {
    let wanted = query.get("state").unwrap_or_default();
    let state = if wanted.is_empty() {
        STATES[0]
    } else {
        STATES
            .into_iter()
            .find(|state| *state == wanted)
            .ok_or_else(|| {
                WebError::BadRequest(
                    "Unknown state: use active, dead, complete, succeeded, cancelled or all."
                        .to_owned(),
                )
            })?
    };
    let filter = match state {
        "dead" => BatchFilter::Dead,
        "complete" => BatchFilter::Complete,
        "succeeded" => BatchFilter::Succeeded,
        "cancelled" => BatchFilter::Cancelled,
        "all" => BatchFilter::All,
        _ => BatchFilter::Active,
    };
    Ok((state, filter))
}

/// The pager of a list whose links carry `params` and the page number as `page_param`;
/// `None` when everything fits on one page.
fn pager(
    base: &str,
    params: &[(&str, &str)],
    page_param: &str,
    page: usize,
    size: usize,
    total: u64,
) -> Option<Pager> {
    let size = u64::try_from(size.max(1)).unwrap_or(1);
    let pages = total.div_ceil(size).max(1);
    if pages <= 1 {
        return None;
    }
    let page = u64::try_from(page).unwrap_or(1).clamp(1, pages);
    let link = |number: u64| {
        let number = number.to_string();
        let mut pairs: Vec<(&str, &str)> = params.to_vec();
        pairs.push((page_param, &number));
        format!("{base}{}", query_string(&pairs))
    };
    Some(Pager {
        label: crate::strings::page_of(page, pages),
        first: (page > 1).then(|| link(1)),
        previous: (page > 1).then(|| link(page - 1)),
        next: (page < pages).then(|| link(page + 1)),
        last: (page < pages).then(|| link(pages)),
    })
}

/// `GET /batches[?state=…][&substr=text][&page=N][&count=N]`.
pub(crate) async fn list(ctx: Ctx) -> Response {
    let result = render_list(&ctx).await;
    super::finish(&ctx, result)
}

async fn render_list(ctx: &Ctx) -> WebResult<Response> {
    let query = Query::parse(&ctx.query);
    let (state, filter) = state_of(&query)?;
    let text = query.filter()?;
    let count = query.count(ctx.web.page_size);
    let api = ctx.web.api();
    let batches = api.batches();
    let mut found = batches
        .search(&filter, text.as_deref(), query.page(), count)
        .await?;
    // Past the end (its last rows were just deleted): the last page instead.
    if let Some(last) = crate::views::last_page(&found.page) {
        found = batches
            .search(&filter, text.as_deref(), last, count)
            .await?;
    }
    let size = batches.size().await?;
    let events = batches.events(Period::Minutes(60)).await?;
    let stats = api.stats().await?;
    let list_url = ctx.url("/batches");
    let count_param = (count != ctx.web.page_size).then(|| count.to_string());
    let state_param = (state != STATES[0]).then_some(state);
    let filter_value = text.clone().unwrap_or_default();
    let params = [
        ("state", state_param.unwrap_or_default()),
        ("substr", filter_value.as_str()),
        ("count", count_param.as_deref().unwrap_or_default()),
    ];
    let pager = pager(
        &list_url,
        &params,
        "page",
        found.page.number,
        found.page.size,
        found.page.total,
    );
    let states = STATES
        .into_iter()
        .map(|candidate| StateLink {
            label: texts::state_name(candidate),
            href: format!(
                "{list_url}{}",
                query_string(&[
                    (
                        "state",
                        if candidate == STATES[0] {
                            ""
                        } else {
                            candidate
                        }
                    ),
                    ("substr", filter_value.as_str()),
                    ("count", count_param.as_deref().unwrap_or_default()),
                ])
            ),
            active: candidate == state,
        })
        .collect();
    let filtered = text.is_some() || state != "all";
    let searched = (filtered && (found.truncated || found.searched < size))
        .then(|| texts::searched(found.searched, found.truncated));
    let rows: Vec<BatchRow> = found
        .page
        .items
        .iter()
        .map(|summary| BatchRow::of(ctx, summary))
        .collect();
    let title = texts::list_title(found.page.total);
    let page = BatchesPage {
        layout: super::layout(ctx, Some(&title), Some(Totals::new(&stats)), true),
        s: &EN,
        t: &texts::EN,
        summary: texts::summary(
            size,
            events.created,
            events.success,
            events.death,
            events.cancelled,
        ),
        searched,
        states,
        state_name: texts::state_name(state),
        total: format::delimit(found.page.total),
        rows,
        pager,
        filter_action: list_url,
        filter_value,
        state,
        empty_text: if text.is_some() {
            texts::EN.no_matches
        } else if size > 0 {
            texts::EN.empty_state
        } else {
            texts::EN.empty
        },
    };
    Ok(super::render(ctx, StatusCode::OK, &page))
}

/// `batch.html`.
#[derive(Template)]
#[template(path = "batch.html")]
pub(crate) struct BatchPage {
    pub(crate) layout: Layout,
    pub(crate) s: &'static Strings,
    pub(crate) t: &'static Texts,
    pub(crate) heading: String,
    pub(crate) back_href: String,
    pub(crate) parent: Option<(String, String)>,
    pub(crate) action_href: String,
    pub(crate) chips: Vec<Chip>,
    pub(crate) progress: ProgressView,
    /// Label and value of each fact.
    pub(crate) facts: Vec<(&'static str, String)>,
    pub(crate) description: Option<String>,
    pub(crate) callbacks: Vec<CallbackRow>,
    pub(crate) failures: Vec<FailureRow>,
    pub(crate) failures_total: String,
    pub(crate) failures_pager: Option<Pager>,
    pub(crate) children: Vec<BatchRow>,
    pub(crate) children_total: String,
    pub(crate) children_pager: Option<Pager>,
    pub(crate) pending: Vec<PendingRow>,
    pub(crate) can_cancel: bool,
    pub(crate) can_dispatch: bool,
    pub(crate) confirm_cancel: String,
}

/// A callback of the batch.
pub(crate) struct CallbackRow {
    pub(crate) event: &'static str,
    pub(crate) class: String,
    pub(crate) queue: String,
    pub(crate) args: String,
    pub(crate) jid: String,
    /// `pushed`, `waiting to be pushed` or `not fired`.
    pub(crate) state: &'static str,
}

/// A failed member.
pub(crate) struct FailureRow {
    /// Its jid, control characters spelled out.
    pub(crate) jid: String,
    /// Its jid as the Skip form's field ([`form::field`]: jids may hold any character).
    pub(crate) jid_field: String,
    /// Its retry or morgue entry where its failure left it; else a search of that list (when
    /// the job is in neither: none).
    pub(crate) href: Option<String>,
    /// The link's explanation, for a search.
    pub(crate) href_title: Option<&'static str>,
    pub(crate) class: String,
    pub(crate) error_class: String,
    pub(crate) message: String,
    pub(crate) retry_count: String,
    pub(crate) failed_at: Option<TimeView>,
    pub(crate) state: String,
    /// A dead member, which an operator may skip.
    pub(crate) dead: bool,
}

/// A pending member, with links that look for it by jid.
pub(crate) struct PendingRow {
    /// Its jid, control characters spelled out.
    pub(crate) jid: String,
    /// Its jid as the Skip form's field ([`form::field`]).
    pub(crate) jid_field: String,
    pub(crate) finds: Vec<(&'static str, String)>,
}

/// The `{bid}` of a batch's routes. Extracted by name, so that the dashboard also works when
/// nested under a path with parameters of its own.
#[derive(Deserialize)]
pub(crate) struct BatchPath {
    bid: String,
}

/// `GET /batches/{bid}[?failures=N][&children=N]`: 404 for a batch that does not exist.
pub(crate) async fn detail(ctx: Ctx, Path(BatchPath { bid }): Path<BatchPath>) -> Response {
    let result = render_detail(&ctx, &bid).await;
    super::finish(&ctx, result)
}

/// The status of `bid`, or 404 (a malformed bid included).
async fn status_or_404(batches: &Batches, bid: &str) -> WebResult<BatchStatus> {
    if !valid_bid(bid) {
        return Err(WebError::NotFound(texts::not_found(bid)));
    }
    match batches.fetch(bid).await {
        Ok(Some(status)) => Ok(status),
        Ok(None) => Err(WebError::NotFound(texts::not_found(bid))),
        Err(hench::Error::Batch(BatchError::Unsupported { .. })) => {
            Err(WebError::BadRequest(format!(
                "Batch {} was written by a newer hench: upgrade the dashboard to show it.",
                format::visible(bid)
            )))
        }
        Err(error) => Err(error.into()),
    }
}

/// 24 lowercase hexadecimal characters.
fn valid_bid(bid: &str) -> bool {
    bid.len() == 24
        && bid
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

async fn render_detail(ctx: &Ctx, bid: &str) -> WebResult<Response> {
    let api = ctx.web.api();
    let batches = api.batches();
    let status = status_or_404(&batches, bid).await?;
    let query = Query::parse(&ctx.query);
    let number = |name: &str| {
        query
            .get(name)
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(1)
            .max(1)
    };
    let base = batch_url(ctx, bid);
    let (failures_page, children_page) = (number("failures"), number("children"));
    let failed = batches.failures(bid, failures_page, PER_PAGE).await?;
    let pending_jids = batches.pending_jids(bid, PER_PAGE).await?;
    let located = locate(api, &failed.items).await?;
    let failures: Vec<FailureRow> = failed
        .items
        .iter()
        .zip(located)
        .map(|(member, key)| failure_row(ctx, member, key))
        .collect();
    let all_children = batches.children(bid).await?;
    let children: Vec<BatchRow> = all_children
        .iter()
        .skip((children_page - 1).saturating_mul(PER_PAGE))
        .take(PER_PAGE)
        .map(|summary| BatchRow::of(ctx, summary))
        .collect();
    let children_total = u64::try_from(all_children.len()).unwrap_or(u64::MAX);
    let pending = pending_jids
        .iter()
        .map(|jid| PendingRow {
            jid: format::visible(jid).into_owned(),
            jid_field: form::field(jid),
            finds: [
                (EN.retries, "/retries"),
                (EN.scheduled, "/scheduled"),
                (EN.dead, "/morgue"),
            ]
            .into_iter()
            .map(|(name, path)| {
                (
                    name,
                    format!("{}{}", ctx.url(path), query_string(&[("substr", jid)])),
                )
            })
            .collect(),
        })
        .collect();
    let callbacks = status
        .callbacks
        .iter()
        .map(|callback| {
            let fired = match callback.event {
                BatchEvent::Complete => status.complete_at.is_some(),
                BatchEvent::Success => status.success_at.is_some(),
                _ => status.death_at.is_some(),
            };
            let (args, _) = format::truncate(&format::args(&callback.args), ARGS_CHARS);
            CallbackRow {
                event: callback.event.as_str(),
                class: format::visible(&callback.class).into_owned(),
                queue: format::visible(&callback.queue).into_owned(),
                args,
                jid: format::visible(&callback.jid).into_owned(),
                state: if callback.dispatched {
                    texts::EN.pushed
                } else if fired {
                    texts::EN.waiting
                } else {
                    texts::EN.not_fired
                },
            }
        })
        .collect();
    let heading = texts::batch_heading(bid);
    // Each pager keeps the other list's page.
    let failures_param = (failures_page > 1).then(|| failures_page.to_string());
    let children_param = (children_page > 1).then(|| children_page.to_string());
    let page = BatchPage {
        layout: super::layout(ctx, Some(&heading), None, true),
        s: &EN,
        t: &texts::EN,
        heading,
        back_href: ctx.url("/batches"),
        parent: status
            .parent
            .as_ref()
            .map(|parent| (parent.clone(), batch_url(ctx, parent))),
        action_href: base.clone(),
        chips: chips(
            status.state(),
            status.complete_at.is_some(),
            status.undispatched,
        ),
        progress: ProgressView::of(&Counts::of_status(&status)),
        facts: facts(ctx, &status),
        description: status
            .description
            .as_deref()
            .map(|text| format::visible(text).into_owned()),
        callbacks,
        failures,
        failures_total: format::delimit(failed.total),
        failures_pager: pager(
            &base,
            &[("children", children_param.as_deref().unwrap_or_default())],
            "failures",
            failed.number,
            PER_PAGE,
            failed.total,
        ),
        children,
        children_total: format::delimit(children_total),
        children_pager: pager(
            &base,
            &[("failures", failures_param.as_deref().unwrap_or_default())],
            "children",
            children_page,
            PER_PAGE,
            children_total,
        ),
        pending,
        can_cancel: status.cancelled_at.is_none() && status.success_at.is_none(),
        can_dispatch: status.undispatched,
        confirm_cancel: texts::confirm_cancel(status.children),
    };
    Ok(super::render(ctx, StatusCode::OK, &page))
}

/// Where the job of a failed member should be: the retries for a member that runs again, the
/// morgue for one that died there or was killed; `None` for a job dropped (`retry: false`),
/// discarded or deleted.
fn list_of(member: &FailedMember) -> Option<(&'static str, &'static str)> {
    if !member.dead {
        Some((keys::RETRY, "/retries"))
    } else if matches!(member.reason.as_str(), "morgue" | "killed") {
        Some((keys::DEAD, "/morgue"))
    } else {
        None
    }
}

/// The scores between which a failed member's entry should be, from its failure summary: the
/// retry's score, or the minute after the member's death.
fn scores_of(member: &FailedMember) -> Option<(f64, f64)> {
    if member.dead {
        member.failed_at.map(death_range)
    } else {
        member.next_at.map(retry_range)
    }
}

/// The scores of a retry due at `at` (as a failure summary records it).
fn retry_range(at: SystemTime) -> (f64, f64) {
    let at = hench::system_time_to_secs(at);
    let slack = RETRY_SLACK_SECS.max(at.abs() * 4.0 * f64::EPSILON);
    (at - slack, at + slack)
}

/// The scores of a morgue entry written after a death at `died` (milliseconds).
fn death_range(died: SystemTime) -> (f64, f64) {
    let died = hench::system_time_to_secs(died);
    (died - MORGUE_WINDOW_SECS.0, died + MORGUE_WINDOW_SECS.1)
}

/// A score as `ZRANGE ... BYSCORE` takes it: Rust prints finite floats in their shortest
/// exact form, which Redis parses back to the same score.
fn score_bound(score: f64) -> String {
    if score.is_finite() {
        score.to_string()
    } else if score > 0.0 {
        "+inf".to_owned()
    } else {
        "-inf".to_owned()
    }
}

/// The jid of a job's JSON, without reading the rest.
fn jid_of(value: &str) -> Option<String> {
    #[derive(Deserialize)]
    struct Jid {
        jid: Option<String>,
    }
    serde_json::from_str::<Jid>(value).ok()?.jid
}

/// The keys (`{score}-{jid}`, as [`hench::api::SortedEntry::key`] writes them) of the failed
/// members' entries in the retry set or the morgue, `None` for a member not found at the place
/// its failure summary records (its retry came due and runs, or it moved). Each lookup reads
/// a range of scores, [`LOCATE_ENTRIES`] entries at most: the first at its place, then the next
/// ones only when another job came first. Two pipelined round trips at most, whatever the size
/// of the sets: `JobSet::find_job` would scan a whole set per member (`ZSCAN MATCH`, every
/// entry when the job is not there), on every refresh of this Live page.
async fn locate(api: &Api, members: &[FailedMember]) -> WebResult<Vec<Option<String>>> {
    let places: Vec<Option<(&str, f64, f64)>> = members
        .iter()
        .map(|member| {
            let (key, _) = list_of(member)?;
            let (min, max) = scores_of(member)?;
            Some((key, min, max))
        })
        .collect();
    let mut found = vec![None; members.len()];
    let mut pending: Vec<usize> = (0..members.len())
        .filter(|&index| places[index].is_some())
        .collect();
    for (skip, count) in [(0, 1), (1, LOCATE_ENTRIES - 1)] {
        if pending.is_empty() {
            break;
        }
        let mut pipe = redis_rs::pipe();
        for (key, min, max) in pending.iter().filter_map(|&index| places[index]) {
            pipe.cmd("ZRANGE")
                .arg(key)
                .arg(score_bound(min))
                .arg(score_bound(max))
                .arg("BYSCORE")
                .arg("LIMIT")
                .arg(skip)
                .arg(count)
                .arg("WITHSCORES");
        }
        let pipe = &pipe;
        let replies: Vec<Vec<(String, f64)>> = api
            .redis()
            .with_retry(|mut conn| async move { pipe.query_async(&mut conn).await })
            .await?;
        let mut next = Vec::new();
        for (&index, entries) in pending.iter().zip(replies) {
            let jid = members[index].jid.as_str();
            let read = u64::try_from(entries.len()).unwrap_or(u64::MAX);
            match entries
                .iter()
                .find(|(value, _)| jid_of(value).as_deref() == Some(jid))
            {
                Some((_, score)) => found[index] = Some(format!("{score}-{jid}")),
                // Others came first and the range may hold more: read on.
                None if read == count => next.push(index),
                None => {}
            }
        }
        pending = next;
    }
    Ok(found)
}

/// A failed member's row: a link to its retry or morgue entry when `key` names it, else to a
/// search of that list (the job may have moved within it, or its lookup ran out of entries).
fn failure_row(ctx: &Ctx, member: &FailedMember, key: Option<String>) -> FailureRow {
    let (href, href_title) = match (list_of(member), key) {
        (Some((_, list)), Some(key)) => (Some(ctx.url(&format!("{list}/{}", segment(&key)))), None),
        (Some((_, list)), None) => (
            Some(format!(
                "{}{}",
                ctx.url(list),
                query_string(&[("substr", member.jid.as_str())])
            )),
            Some(texts::EN.search_failed),
        ),
        (None, _) => (None, None),
    };
    let next = member.next_at.map(|at| TimeView::new(at, ctx.now).text);
    let message = member
        .error
        .as_deref()
        .map(|message| format::truncate(&format::visible(message), MESSAGE_CHARS).0)
        .unwrap_or_default();
    FailureRow {
        jid: format::visible(&member.jid).into_owned(),
        jid_field: form::field(&member.jid),
        href,
        href_title,
        class: format::visible(&member.class).into_owned(),
        error_class: format::visible(member.error_class.as_deref().unwrap_or_default())
            .into_owned(),
        message,
        retry_count: member
            .retry_count
            .map(|count| count.to_string())
            .unwrap_or_default(),
        failed_at: member.failed_at.map(|at| TimeView::new(at, ctx.now)),
        state: texts::failure_state(member.dead, &member.reason, next.as_deref()),
        dead: member.dead,
    }
}

/// The facts of a batch's page, in order.
fn facts(ctx: &Ctx, status: &BatchStatus) -> Vec<(&'static str, String)> {
    let t = &texts::EN;
    let count = format::delimit;
    let when = |at: SystemTime| {
        let at = TimeView::new(at, ctx.now);
        format!("{} ({})", at.text, at.utc)
    };
    let mut facts = vec![
        (t.created, when(status.created_at)),
        (
            t.expires,
            status.expires_at.map_or_else(|| t.never.to_owned(), when),
        ),
        (t.total, count(status.total)),
        (t.succeeded, count(status.succeeded())),
        (t.pending, count(status.pending)),
        (t.failures, count(status.failures)),
        (t.dead, count(status.dead)),
        (t.skipped, count(status.skipped)),
        (
            t.children,
            texts::children_fact(
                status.children,
                status.children_complete,
                status.children_succeeded,
            ),
        ),
    ];
    let has = |event: hench::batch::BatchEvent| {
        status
            .callbacks
            .iter()
            .any(|callback| callback.event == event)
    };
    for (label, fired, pushed, event) in [
        (
            "Complete",
            status.complete_at,
            status.complete_dispatched_at,
            BatchEvent::Complete,
        ),
        (
            "Success",
            status.success_at,
            status.success_dispatched_at,
            BatchEvent::Success,
        ),
        (
            "Death",
            status.death_at,
            status.death_dispatched_at,
            BatchEvent::Death,
        ),
    ] {
        let Some(fired) = fired else {
            continue;
        };
        let fired = when(fired);
        let value = if has(event) {
            texts::event_fact(&fired, pushed.map(when).as_deref())
        } else {
            fired
        };
        facts.push((label, value));
    }
    if let Some(at) = status.cancelled_at {
        facts.push(("Cancelled", when(at)));
    }
    if let Some(at) = status.finished_at {
        facts.push(("Finished", when(at)));
    }
    if let Some(duration) = status.duration() {
        facts.push(("Duration", texts::duration(duration)));
    }
    if let Some(stages) = status.workflow_stages {
        facts.push(("Workflow stages", count(stages)));
    }
    facts
}

/// `POST /batches/{bid}`: `cancel` and `dispatch` (back to the batch's page), `delete` (back
/// to the list), `skip` with `jid` (back to the batch's page).
pub(crate) async fn action(
    ctx: Ctx,
    Path(BatchPath { bid }): Path<BatchPath>,
    form: PostForm,
) -> Response {
    let result = act(&ctx, &bid, &form).await;
    super::finish(&ctx, result)
}

async fn act(ctx: &Ctx, bid: &str, form: &PostForm) -> WebResult<Response> {
    let batches = ctx.web.api().batches();
    let status = status_or_404(&batches, bid).await?;
    let page = batch_url(ctx, bid);
    let refused = |error: hench::Error| -> WebError {
        match error {
            hench::Error::Batch(
                error @ (BatchError::Succeeded { .. }
                | BatchError::DeleteChild { .. }
                | BatchError::NotFound { .. }),
            ) => WebError::BadRequest(refusal(&error)),
            other => other.into(),
        }
    };
    if form.has("cancel") {
        let cancelled = batches.cancel(bid).await.map_err(refused)?;
        tracing::info!(bid, cancelled, "dashboard cancelled a batch");
        let notice = if cancelled == 1 {
            Notice::about(NoticeKind::BatchCancelled, bid)
        } else {
            Notice::new(
                NoticeKind::BatchCancelled,
                u64::try_from(cancelled).unwrap_or(u64::MAX),
                0,
            )
        };
        Ok(ctx.redirect(&page, Some(notice)))
    } else if form.has("delete") {
        batches.delete(bid, false).await.map_err(refused)?;
        tracing::info!(bid, children = status.children, "dashboard deleted a batch");
        Ok(ctx.redirect(
            &ctx.back_to("/batches"),
            Some(Notice::about(NoticeKind::BatchDeleted, bid)),
        ))
    } else if form.has("skip") {
        let jid = form
            .value("jid")
            .filter(|jid| !jid.is_empty())
            .ok_or_else(|| WebError::BadRequest("Name the job to skip (jid).".to_owned()))?;
        let jid = jid.as_ref();
        let skipped = batches.skip(bid, jid).await.map_err(refused)?;
        tracing::info!(bid, jid, skipped, "dashboard skipped a batch member");
        Ok(ctx.redirect(
            &page,
            Some(Notice::new(
                NoticeKind::MemberSkipped,
                u64::from(skipped),
                u64::from(!skipped),
            )),
        ))
    } else if form.has("dispatch") {
        let dispatched = batches.dispatch(bid).await.map_err(refused)?;
        tracing::info!(bid, dispatched, "dashboard dispatched batch callbacks");
        Ok(ctx.redirect(
            &page,
            Some(Notice::new(
                NoticeKind::CallbacksDispatched,
                u64::try_from(dispatched).unwrap_or(u64::MAX),
                0,
            )),
        ))
    } else {
        Err(WebError::BadRequest(errors::NO_ACTION.to_owned()))
    }
}

/// Why an action was refused, in words.
fn refusal(error: &BatchError) -> String {
    match error {
        BatchError::Succeeded { bid } => format!(
            "Batch {} already succeeded: it cannot be cancelled.",
            format::visible(bid)
        ),
        BatchError::DeleteChild { bid, parent } => format!(
            "Batch {} is an unfinished child of batch {}, which could then never finish: \
             delete or cancel the parent instead.",
            format::visible(bid),
            format::visible(parent)
        ),
        other => format::visible(&other.to_string()).into_owned(),
    }
}

/// The page of a batch.
fn batch_url(ctx: &Ctx, bid: &str) -> String {
    ctx.url(&format!("/batches/{}", segment(bid)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_retry_is_looked_for_around_the_score_its_summary_recorded() {
        // The score goes through the summary's JSON, then `Duration::from_secs_f64` (as
        // `Batches::failures` reads it), then back to a float: the range still holds it, now
        // and up to the year 10 000, with a slack of a few microseconds at most.
        for score in [
            1_800_000_000.123_456_7,
            1_800_000_000.0,
            1_798_765_432.987_654_3,
            4_294_967_296.000_001,
            100_000_000_000.5,
            253_402_300_799.999,
        ] {
            let stored: f64 =
                serde_json::from_str(&serde_json::to_string(&score).unwrap()).unwrap();
            let at = std::time::UNIX_EPOCH + std::time::Duration::from_secs_f64(stored);
            let (min, max) = retry_range(at);
            assert!(min <= score && score <= max, "{min} <= {score} <= {max}");
            assert!(max - min < 1e-3, "{score}: {}", max - min);
        }
        // A death at a whole millisecond: the morgue's score comes later.
        let died = std::time::UNIX_EPOCH + std::time::Duration::from_millis(1_800_000_000_123);
        let (min, max) = death_range(died);
        assert!(min <= 1_800_000_000.123 && 1_800_000_000.124 <= max);
        assert_eq!(score_bound(f64::INFINITY), "+inf");
        assert_eq!(score_bound(f64::NEG_INFINITY), "-inf");
        assert_eq!(
            score_bound(1_800_000_000.5).parse::<f64>().unwrap(),
            1_800_000_000.5
        );
    }

    #[test]
    fn bars_round_to_steps_without_hiding_a_state() {
        assert_eq!(widths(&[0, 0, 0, 0], 0), [0, 0, 0, 0]);
        assert_eq!(widths(&[10, 0, 0, 0], 10), [100, 0, 0, 0]);
        assert_eq!(widths(&[5, 0, 0, 0], 10), [50, 0, 0, 0]);
        // 997 succeeded, 1 dead, 2 pending of 1 000: the death still shows.
        assert_eq!(widths(&[997, 0, 1, 0], 1_000), [95, 0, 5, 0]);
        // Thirds: the steps left over go to the largest remainders, the sum stays 100.
        let thirds = widths(&[1, 1, 1, 0], 3);
        assert_eq!(thirds.iter().map(|w| u32::from(*w)).sum::<u32>(), 100);
        assert!(thirds.iter().take(3).all(|width| *width >= 30));
        // Never more than the whole bar.
        let crowded = widths(&[1, 1, 1, 1], 4);
        assert_eq!(crowded.iter().map(|w| u32::from(*w)).sum::<u32>(), 100);
    }

    #[test]
    fn states_are_checked() {
        assert_eq!(state_of(&Query::parse("")).unwrap().0, "active");
        assert_eq!(
            state_of(&Query::parse("state=dead")).unwrap().1,
            BatchFilter::Dead
        );
        assert_eq!(
            state_of(&Query::parse("state=all")).unwrap().1,
            BatchFilter::All
        );
        assert!(state_of(&Query::parse("state=finished")).is_err());
        assert!(valid_bid("0123456789abcdef01234567"));
        assert!(!valid_bid("0123456789ABCDEF01234567"));
        assert!(!valid_bid("../x"));
    }
}
