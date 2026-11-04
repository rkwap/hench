//! `/queues` and `/queues/{name}`: queue sizes and latencies, a queue's jobs, deleting jobs
//! and queues, pausing and resuming.

use axum::extract::Path;
use axum::response::Response;
use hench::api::{Api, JobRecord, SortOrder, Stats, page_key};
use http::StatusCode;
use serde::Deserialize;

use crate::ctx::Ctx;
use crate::error::{WebError, WebResult};
use crate::flash::{Notice, NoticeKind};
use crate::form::PostForm;
use crate::format;
use crate::params::{Query, query_string};
use crate::search::{self, Pattern};
use crate::strings::{self, EN, errors};
use crate::templates::{QueuePage, QueuesPage};
use crate::views::{
    JobRow, Pager, QueueRow, Totals, job_digest, last_page, parse_queue_row_key, segment,
};

/// Jobs before its place on the page that the search for a job to delete looks at (deleting
/// newer jobs moves a job towards the front).
const FIND_BEFORE: u64 = 10;

/// Jobs read per round trip while looking for a job to delete beyond its place on the page.
const FIND_PAGE: usize = 100;

/// Jobs the search for a job to delete looks through at most (pushes move a job back).
const FIND_LIMIT: usize = 10_000;

/// The `{name}` of a queue route. Extracted by name, so that the dashboard also works when
/// nested under a path with parameters of its own.
#[derive(Deserialize)]
pub(crate) struct QueueName {
    name: String,
}

/// `GET /queues`.
pub(crate) async fn list(ctx: Ctx) -> Response {
    let result = render_list(&ctx).await;
    super::finish(&ctx, result)
}

async fn render_list(ctx: &Ctx) -> WebResult<Response> {
    let api = ctx.web.api();
    let stats = api.stats().await?;
    let mut summaries = Stats::queue_summaries(api).await?;
    summaries.sort_by(|a, b| a.name.cmp(&b.name));
    let title = strings::counted(
        EN.queues,
        u64::try_from(summaries.len()).unwrap_or(u64::MAX),
    );
    let page = QueuesPage {
        layout: super::layout(ctx, Some(&title), Some(Totals::new(&stats)), true),
        s: &EN,
        queues: summaries
            .iter()
            .map(|summary| QueueRow::new(ctx, summary))
            .collect(),
    };
    Ok(super::render(ctx, StatusCode::OK, &page))
}

/// A queue name from the path. Every name a queue can have is accepted, control characters
/// included, so that every queue the list shows can be opened, paused and deleted.
fn checked_name(name: &str) -> WebResult<&str> {
    if name.is_empty() {
        return Err(WebError::NotFound(strings::queue_not_found(name)));
    }
    Ok(name)
}

/// `GET /queues/{name}[?page=N][&count=N][&direction=asc|desc][&substr=text]`: 404 for a
/// queue that does not exist (not known, empty, neither paused nor limited).
pub(crate) async fn page(ctx: Ctx, Path(QueueName { name }): Path<QueueName>) -> Response {
    let result = render_queue(&ctx, &name).await;
    super::finish(&ctx, result)
}

async fn render_queue(ctx: &Ctx, name: &str) -> WebResult<Response> {
    let name = checked_name(name)?;
    let query = Query::parse(&ctx.query);
    let order = query.direction(SortOrder::Descending)?;
    let filter = query.filter()?;
    let count = query.count(ctx.web.page_size);
    let api = ctx.web.api();
    let queue = api.queue(name);
    if !queue.exists().await? {
        return Err(WebError::NotFound(strings::queue_not_found(name)));
    }
    let stats = api.stats().await?;
    let paused = queue.is_paused().await?;
    let queue_url = ctx.url(&format!("/queues/{}", segment(name)));
    let stored_limits = queue.stored_limits().await?;
    let (limits_invalid, limit_links) = match queue.limits().await {
        Ok(limits) => {
            // A link to the usage of each limit in use (a limit no fetch has used yet has no
            // page).
            let id = format!("@{name}");
            let mut links = Vec::new();
            for limit in &limits {
                let kind = limit.kind();
                if api.limits().get(kind, &id).await?.is_some() {
                    let letter = kind.letter();
                    links.push((
                        letter,
                        ctx.url(&format!("/limits/{letter}/{}", segment(&id))),
                    ));
                }
            }
            (false, links)
        }
        Err(hench::Error::Config(_)) => (true, Vec::new()),
        Err(error) => return Err(error.into()),
    };

    let (rows, total, pager, filter_summary, empty_text) = match &filter {
        Some(filter) => {
            let results = search::search(
                &Pattern::new(filter),
                |number, size| queue.page(number, size, order),
                JobRecord::value,
            )
            .await?;
            let rows = results
                .found
                .iter()
                .map(|found| {
                    let position = search::place_in_line(order, found.index, found.total);
                    JobRow::queued(ctx, &found.item, position, found.total)
                })
                .collect();
            let summary = strings::filter_summary(results.found.len(), results.outcome, filter);
            (
                rows,
                results.total,
                None,
                Some(summary),
                strings::no_matches(results.outcome),
            )
        }
        None => {
            let mut page = queue.page(query.page(), count, order).await?;
            // Past the end (its last jobs were just deleted): the last page instead.
            if let Some(last) = last_page(&page) {
                page = queue.page(last, count, order).await?;
            }
            let first = u64::try_from((page.number - 1).saturating_mul(page.size)).unwrap_or(0);
            let rows = page
                .items
                .iter()
                .zip(0_u64..)
                .map(|(record, offset)| {
                    let position = search::place_in_line(order, first + offset, page.total);
                    JobRow::queued(ctx, record, position, page.total)
                })
                .collect();
            let pager = Pager::new(ctx, &queue_url, page.number, page.size, page.total);
            (rows, page.total, pager, None, EN.empty_queue)
        }
    };

    let order_link = |direction: &str| {
        let count = (count != ctx.web.page_size).then(|| count.to_string());
        format!(
            "{queue_url}{}",
            query_string(&[
                ("direction", direction),
                ("count", count.as_deref().unwrap_or_default())
            ])
        )
    };
    let heading = strings::queue_heading(name);
    let page = QueuePage {
        layout: super::layout(ctx, Some(&heading), Some(Totals::new(&stats)), true),
        s: &EN,
        heading,
        total: format::delimit(total),
        paused,
        limits: stored_limits
            .as_deref()
            .map(|limits| format::visible(limits).into_owned()),
        limits_invalid,
        limits_value: stored_limits.unwrap_or_default(),
        limit_links,
        delete_url: format!("{queue_url}/delete"),
        ascending: order == SortOrder::Ascending,
        newest_href: order_link(""),
        oldest_href: order_link("asc"),
        filter_action: queue_url.clone(),
        filter_value: filter.unwrap_or_default(),
        filter_summary,
        empty_text,
        queue_url,
        rows,
        pager,
    };
    Ok(super::render(ctx, StatusCode::OK, &page))
}

/// `POST /queues/{name}`: `pause`, `unpause`, `set_limits` (with `limits`, the specs joined by
/// `;`), `clear_limits` or `delete` (the whole queue). Pausing, resuming and the limits go back
/// to the page the form was on (`return_to=queue`: the queue's page), deleting to the queue
/// list. Limits that are not valid answer 400 with the reason.
pub(crate) async fn action(
    ctx: Ctx,
    Path(QueueName { name }): Path<QueueName>,
    form: PostForm,
) -> Response {
    let result = act(&ctx, &name, &form).await;
    super::finish(&ctx, result)
}

async fn act(ctx: &Ctx, name: &str, form: &PostForm) -> WebResult<Response> {
    let name = checked_name(name)?;
    let queue = ctx.web.api().queue(name);
    let queue_page = ctx.url(&format!("/queues/{}", segment(name)));
    let back = if form.get("return_to") == Some("queue") {
        queue_page
    } else {
        ctx.url("/queues")
    };
    if form.has("pause") {
        queue.pause().await?;
        tracing::info!(queue = name, "dashboard paused a queue");
        Ok(ctx.redirect(&back, Some(Notice::about(NoticeKind::QueuePaused, name))))
    } else if form.has("unpause") {
        queue.unpause().await?;
        tracing::info!(queue = name, "dashboard resumed a queue");
        Ok(ctx.redirect(&back, Some(Notice::about(NoticeKind::QueueUnpaused, name))))
    } else if form.has("set_limits") {
        let specs = form.get("limits").unwrap_or_default();
        match queue.set_limits_from_str(specs).await {
            Ok(_) => {}
            Err(hench::Error::Config(message)) => return Err(WebError::BadRequest(message)),
            Err(error) => return Err(error.into()),
        }
        tracing::info!(
            queue = name,
            limits = specs,
            "dashboard set the limits of a queue"
        );
        Ok(ctx.redirect(&back, Some(Notice::about(NoticeKind::QueueLimited, name))))
    } else if form.has("clear_limits") {
        queue.clear_limits().await?;
        tracing::info!(queue = name, "dashboard cleared the limits of a queue");
        Ok(ctx.redirect(&back, Some(Notice::about(NoticeKind::QueueUnlimited, name))))
    } else if form.has("delete") {
        queue.clear().await?;
        tracing::info!(queue = name, "dashboard deleted a queue");
        Ok(ctx.redirect(
            &ctx.url("/queues"),
            Some(Notice::about(NoticeKind::QueueCleared, name)),
        ))
    } else {
        Err(WebError::BadRequest(errors::NO_ACTION.to_owned()))
    }
}

/// `POST /queues/{name}/delete`: deletes the job of the row `key` (see
/// [`queue_row_key`](crate::views::queue_row_key)).
pub(crate) async fn delete_job(
    ctx: Ctx,
    Path(QueueName { name }): Path<QueueName>,
    form: PostForm,
) -> Response {
    let result = delete(&ctx, &name, &form).await;
    super::finish(&ctx, result)
}

async fn delete(ctx: &Ctx, name: &str, form: &PostForm) -> WebResult<Response> {
    let name = checked_name(name)?;
    let Some((index, digest)) = form.get("key").and_then(parse_queue_row_key) else {
        return Err(WebError::BadRequest(errors::NO_ACTION.to_owned()));
    };
    let api = ctx.web.api();
    let deleted = match find_job(api, name, index, digest).await? {
        Some(value) => JobRecord::new(api, value, Some(name)).delete().await?,
        None => false,
    };
    let notice = if deleted {
        Notice::new(NoticeKind::Deleted, 1, 0)
    } else {
        Notice::new(NoticeKind::Gone, 0, 0)
    };
    let back = ctx.back_to(&format!("/queues/{}", segment(name)));
    Ok(ctx.redirect(&back, Some(notice)))
}

/// The JSON of the job of queue `name` whose SHA-1 is `digest`, last seen at `index`: first
/// at that very place (one job read), then from a little before it onwards (pushes move jobs
/// back), [`FIND_LIMIT`] jobs at most. `None` when it is not found: it ran or was deleted.
async fn find_job(api: &Api, name: &str, index: u64, digest: &str) -> WebResult<Option<String>> {
    let key = hench::keys::queue(name);
    let matching = |entries: Vec<hench::api::RawEntry>| {
        entries
            .into_iter()
            .find(|entry| job_digest(&entry.value) == digest)
            .map(|entry| entry.value)
    };
    let place = usize::try_from(index).unwrap_or(usize::MAX);
    let exact = page_key(api, &key, place.saturating_add(1), 1, SortOrder::Descending).await?;
    if let Some(value) = matching(exact.items) {
        return Ok(Some(value));
    }
    let start = usize::try_from(index.saturating_sub(FIND_BEFORE)).unwrap_or(usize::MAX);
    let mut number = start / FIND_PAGE + 1;
    let mut scanned = 0;
    loop {
        let page = page_key(api, &key, number, FIND_PAGE, SortOrder::Descending).await?;
        let read = page.items.len();
        if let Some(value) = matching(page.items) {
            return Ok(Some(value));
        }
        scanned += read;
        if read < FIND_PAGE || scanned >= FIND_LIMIT {
            return Ok(None);
        }
        number += 1;
    }
}
