//! `/retries`, `/scheduled` and `/morgue`: the three sorted sets of jobs, one job's details,
//! and the actions on selected jobs or on all of them.

use std::panic::AssertUnwindSafe;

use axum::extract::Path;
use axum::response::Response;
use futures::FutureExt;
use hench::api::{Api, JobSet, SortOrder, SortedEntry, parse_key};
use hench::iterable::IterationState;
use http::StatusCode;
use serde::Deserialize;

use crate::ctx::Ctx;
use crate::error::{WebError, WebResult};
use crate::flash::{Notice, NoticeKind};
use crate::form::PostForm;
use crate::format;
use crate::params::Query;
use crate::search::{self, Pattern};
use crate::strings::{self, EN, errors};
use crate::templates::{AllActions, EntryPage, SetPage};
use crate::views::{EntryView, FULL_CURSOR, IterationView, JobRow, Pager, Totals, last_page};

/// The `{key}` of an entry route (extracted by name: see `queues::QueueName`).
#[derive(Deserialize)]
pub(crate) struct EntryKey {
    key: String,
}

/// The `{action}` of an "all" route.
#[derive(Deserialize)]
pub(crate) struct AllAction {
    action: String,
}

/// Jobs one bulk POST may name (a page shows at most 999).
const MAX_KEYS: usize = 1_000;

/// One of the three sorted sets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Retries,
    Scheduled,
    Dead,
}

/// What to do to one job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Action {
    Retry,
    AddToQueue,
    Delete,
    Kill,
}

impl Action {
    fn notice(self) -> NoticeKind {
        match self {
            Action::Retry => NoticeKind::Retried,
            Action::AddToQueue => NoticeKind::Enqueued,
            Action::Delete => NoticeKind::Deleted,
            Action::Kill => NoticeKind::Killed,
        }
    }
}

impl Kind {
    /// The list's path within the dashboard.
    fn path(self) -> &'static str {
        match self {
            Kind::Retries => "/retries",
            Kind::Scheduled => "/scheduled",
            Kind::Dead => "/morgue",
        }
    }

    fn set(self, api: &Api) -> JobSet {
        match self {
            Kind::Retries => api.retries().into_inner(),
            Kind::Scheduled => api.scheduled().into_inner(),
            Kind::Dead => api.dead().into_inner(),
        }
    }

    /// Retries and scheduled jobs come soonest first, dead jobs latest first.
    fn order(self) -> SortOrder {
        match self {
            Kind::Retries | Kind::Scheduled => SortOrder::Ascending,
            Kind::Dead => SortOrder::Descending,
        }
    }

    fn heading(self) -> &'static str {
        match self {
            Kind::Retries => EN.retries_title,
            Kind::Scheduled => EN.scheduled_title,
            Kind::Dead => EN.dead_title,
        }
    }

    fn time_label(self) -> &'static str {
        match self {
            Kind::Retries => EN.next_retry,
            Kind::Scheduled => EN.when,
            Kind::Dead => EN.died,
        }
    }

    /// The label of the time row on a job's page.
    fn detail_time_label(self) -> &'static str {
        match self {
            Kind::Retries => EN.next_retry,
            Kind::Scheduled => EN.scheduled_for,
            Kind::Dead => EN.died,
        }
    }

    /// The confirmations of the actions on all `total` jobs of the set.
    fn all_actions(self, total: u64) -> AllActions {
        let all = match self {
            Kind::Retries => strings::all_of(total, "retry", "retries"),
            Kind::Scheduled => strings::all_of(total, "scheduled job", "scheduled jobs"),
            Kind::Dead => strings::all_of(total, "dead job", "dead jobs"),
        };
        AllActions {
            requeue: match self {
                Kind::Scheduled => strings::confirm_enqueue_all(&all),
                Kind::Retries | Kind::Dead => strings::confirm_retry_all(&all),
            },
            delete: strings::confirm_delete_all(&all),
            kill: strings::confirm_kill_all(&all),
        }
    }

    fn empty_text(self) -> &'static str {
        match self {
            Kind::Retries => EN.no_retries,
            Kind::Scheduled => EN.no_scheduled,
            Kind::Dead => EN.no_dead,
        }
    }

    /// The actions a form may ask for on this set's jobs, in order of precedence.
    fn actions(self) -> &'static [(&'static str, Action)] {
        match self {
            Kind::Retries => &[
                ("retry", Action::Retry),
                ("delete", Action::Delete),
                ("kill", Action::Kill),
            ],
            Kind::Scheduled => &[
                ("add_to_queue", Action::AddToQueue),
                ("delete", Action::Delete),
            ],
            Kind::Dead => &[("retry", Action::Retry), ("delete", Action::Delete)],
        }
    }

    /// The action of the pressed button.
    fn action(self, form: &PostForm) -> WebResult<Action> {
        self.actions()
            .iter()
            .find(|(name, _)| form.has(name))
            .map(|(_, action)| *action)
            .ok_or_else(|| WebError::BadRequest(errors::NO_ACTION.to_owned()))
    }
}

/// The entry `key` (`score-jid`) of `set`, if it is (still) there. A key without a jid
/// names a job without one (such as an entry that is not valid JSON).
async fn find(set: &JobSet, key: &str) -> WebResult<Option<SortedEntry>> {
    let Some((score, jid)) = parse_key(key) else {
        return Ok(None);
    };
    let entries = set.fetch(score, jid).await?;
    Ok(match jid {
        Some(_) => entries.into_iter().next(),
        None => entries
            .into_iter()
            .find(|entry| entry.jid().is_none_or(str::is_empty)),
    })
}

/// Applies `action` to `entry`: `Ok(true)` when it was done, `Ok(false)` when the job was
/// gone. A panic of client middleware (re-raised by the API after putting the job back) is
/// turned into an error.
async fn apply(entry: &SortedEntry, action: Action) -> Result<bool, String> {
    let outcome = AssertUnwindSafe(async {
        match action {
            Action::Retry => entry.retry().await,
            Action::AddToQueue => entry.add_to_queue().await,
            Action::Delete => entry.delete().await,
            Action::Kill => entry.kill().await,
        }
    })
    .catch_unwind()
    .await;
    match outcome {
        Ok(Ok(done)) => Ok(done),
        Ok(Err(error)) => Err(error.to_string()),
        Err(_) => Err("client middleware panicked".to_owned()),
    }
}

/// The list page of `kind`.
async fn list(ctx: &Ctx, kind: Kind) -> WebResult<Response> {
    let query = Query::parse(&ctx.query);
    let filter = query.filter()?;
    let api = ctx.web.api();
    let stats = api.stats().await?;
    let set = kind.set(api);
    let list_url = ctx.url(kind.path());

    let (entries, total, pager, filter_summary, empty_text) = match &filter {
        Some(filter) => {
            let results = search::search(
                &Pattern::new(filter),
                |number, size| set.page(number, size, kind.order()),
                |entry: &SortedEntry| entry.value(),
            )
            .await?;
            let summary = strings::filter_summary(results.found.len(), results.outcome, filter);
            let entries = results.found.into_iter().map(|found| found.item).collect();
            (
                entries,
                results.total,
                None,
                Some(summary),
                strings::no_matches(results.outcome),
            )
        }
        None => {
            let count = query.count(ctx.web.page_size);
            let mut page = set.page(query.page(), count, kind.order()).await?;
            // Past the end (its last jobs were just moved or deleted): the last page instead.
            if let Some(last) = last_page(&page) {
                page = set.page(last, count, kind.order()).await?;
            }
            let pager = Pager::new(ctx, &list_url, page.number, page.size, page.total);
            (page.items, page.total, pager, None, kind.empty_text())
        }
    };

    let mut rows: Vec<JobRow> = entries
        .iter()
        .map(|entry| JobRow::entry(ctx, entry, kind.path()))
        .collect();
    if kind == Kind::Retries {
        super::add_iterations(ctx, &mut rows).await?;
    }
    // A filtered list does not offer the actions on every job: they would act on the whole
    // set, not on the matches (select the matches instead).
    let all_actions = filter.is_none().then(|| kind.all_actions(total));
    let title = strings::counted(kind.heading(), total);
    let page = SetPage {
        layout: super::layout(ctx, Some(&title), Some(Totals::new(&stats)), true),
        s: &EN,
        heading: kind.heading(),
        total: format::delimit(total),
        is_retries: kind == Kind::Retries,
        is_scheduled: kind == Kind::Scheduled,
        time_label: kind.time_label(),
        empty_text,
        filter_action: list_url.clone(),
        filter_value: filter.unwrap_or_default(),
        filter_summary,
        rows,
        list_url,
        pager,
        all_actions,
    };
    Ok(super::render(ctx, StatusCode::OK, &page))
}

/// `POST` on a list: the pressed action on every selected job (`key` fields, written with
/// [`crate::form::field`]).
async fn bulk(ctx: &Ctx, kind: Kind, form: &PostForm) -> WebResult<Response> {
    let action = kind.action(form)?;
    let back = ctx.back_to(kind.path());
    let keys: Vec<_> = form.values("key").take(MAX_KEYS + 1).collect();
    if keys.is_empty() {
        return Ok(ctx.redirect(&back, Some(Notice::new(NoticeKind::NothingSelected, 0, 0))));
    }
    if keys.len() > MAX_KEYS {
        return Err(WebError::BadRequest(format!(
            "At most {MAX_KEYS} jobs can be changed at once."
        )));
    }
    let set = kind.set(ctx.web.api());
    let (mut done, mut failed) = (0, 0);
    for key in &keys {
        let key = key.as_ref();
        let Some(entry) = find(&set, key).await? else {
            continue;
        };
        match apply(&entry, action).await {
            Ok(true) => done += 1,
            Ok(false) => {}
            Err(error) => {
                tracing::warn!(
                    set = set.name(),
                    key,
                    ?action,
                    error,
                    "dashboard action failed"
                );
                failed += 1;
            }
        }
    }
    tracing::info!(
        set = set.name(),
        ?action,
        done,
        failed,
        "dashboard bulk action"
    );
    Ok(ctx.redirect(&back, Some(Notice::new(action.notice(), done, failed))))
}

/// `POST /<list>/all/{action}`: the action on every job of the set.
async fn all(ctx: &Ctx, kind: Kind, action: &str) -> WebResult<Response> {
    let set = kind.set(ctx.web.api());
    let notice = match (kind, action) {
        (Kind::Retries | Kind::Dead, "retry") => {
            Notice::new(NoticeKind::Retried, set.retry_all().await?, 0)
        }
        (Kind::Scheduled, "add_to_queue") => {
            Notice::new(NoticeKind::Enqueued, set.add_all_to_queue().await?, 0)
        }
        (_, "delete") => {
            let size = set.size().await?;
            set.clear().await?;
            Notice::new(NoticeKind::Deleted, size, 0)
        }
        (Kind::Retries, "kill") => Notice::new(NoticeKind::Killed, set.kill_all(false).await?, 0),
        _ => return Err(WebError::NotFound(errors::NO_SUCH_PAGE.to_owned())),
    };
    tracing::info!(
        set = set.name(),
        action,
        count = notice.count,
        "dashboard changed every job of a set"
    );
    Ok(ctx.redirect(&ctx.url(kind.path()), Some(notice)))
}

/// The detail page of one job.
async fn entry_page(ctx: &Ctx, kind: Kind, key: &str) -> WebResult<Response> {
    let api = ctx.web.api();
    let Some(entry) = find(&kind.set(api), key).await? else {
        return Err(WebError::NotFound(strings::entry_not_found(
            &kind.heading().to_lowercase(),
        )));
    };
    let stats = api.stats().await?;
    let heading = format!(
        "{} · {}",
        kind.heading(),
        entry.display_class().unwrap_or(EN.unknown)
    );
    let mut view = EntryView::new(ctx, &entry, kind.path());
    if let Some(jid) = entry.jid().filter(|jid| !jid.is_empty()) {
        view.iteration = IterationState::fetch(api.redis(), jid)
            .await?
            .map(|state| IterationView::new(&state, FULL_CURSOR));
    }
    let page = EntryPage {
        layout: super::layout(ctx, Some(&heading), Some(Totals::new(&stats)), false),
        s: &EN,
        heading,
        is_retries: kind == Kind::Retries,
        is_scheduled: kind == Kind::Scheduled,
        is_dead: kind == Kind::Dead,
        time_label: kind.detail_time_label(),
        entry: view,
    };
    Ok(super::render(ctx, StatusCode::OK, &page))
}

/// `POST` on one job's page.
async fn entry_action(ctx: &Ctx, kind: Kind, key: &str, form: &PostForm) -> WebResult<Response> {
    let action = kind.action(form)?;
    let back = ctx.back_to(kind.path());
    let set = kind.set(ctx.web.api());
    let Some(entry) = find(&set, key).await? else {
        return Ok(ctx.redirect(&back, Some(Notice::new(NoticeKind::Gone, 0, 0))));
    };
    let notice = match apply(&entry, action).await {
        Ok(true) => Notice::new(action.notice(), 1, 0),
        Ok(false) => Notice::new(NoticeKind::Gone, 0, 0),
        Err(error) => {
            tracing::warn!(
                set = set.name(),
                key,
                ?action,
                error,
                "dashboard action failed"
            );
            Notice::new(action.notice(), 0, 1)
        }
    };
    Ok(ctx.redirect(&back, Some(notice)))
}

macro_rules! set_handlers {
    ($kind:expr, $list:ident, $bulk:ident, $all:ident, $entry:ident, $entry_action:ident) => {
        /// The list page.
        pub(crate) async fn $list(ctx: Ctx) -> Response {
            let result = list(&ctx, $kind).await;
            super::finish(&ctx, result)
        }

        /// An action on the selected jobs.
        pub(crate) async fn $bulk(ctx: Ctx, form: PostForm) -> Response {
            let result = bulk(&ctx, $kind, &form).await;
            super::finish(&ctx, result)
        }

        /// An action on every job.
        pub(crate) async fn $all(
            ctx: Ctx,
            Path(AllAction { action }): Path<AllAction>,
            _form: PostForm,
        ) -> Response {
            let result = all(&ctx, $kind, &action).await;
            super::finish(&ctx, result)
        }

        /// One job's page.
        pub(crate) async fn $entry(ctx: Ctx, Path(EntryKey { key }): Path<EntryKey>) -> Response {
            let result = entry_page(&ctx, $kind, &key).await;
            super::finish(&ctx, result)
        }

        /// An action on one job.
        pub(crate) async fn $entry_action(
            ctx: Ctx,
            Path(EntryKey { key }): Path<EntryKey>,
            form: PostForm,
        ) -> Response {
            let result = entry_action(&ctx, $kind, &key, &form).await;
            super::finish(&ctx, result)
        }
    };
}

set_handlers!(
    Kind::Retries,
    retries,
    retries_bulk,
    retries_all,
    retry_entry,
    retry_entry_action
);
set_handlers!(
    Kind::Scheduled,
    scheduled,
    scheduled_bulk,
    scheduled_all,
    scheduled_entry,
    scheduled_entry_action
);
set_handlers!(
    Kind::Dead,
    morgue,
    morgue_bulk,
    morgue_all,
    dead_entry,
    dead_entry_action
);
