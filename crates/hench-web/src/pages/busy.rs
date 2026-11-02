//! `/busy`: the processes and the jobs they are running; quiet and stop.

use axum::response::Response;
use hench::api::{Process, page_items};
use http::StatusCode;

use crate::ctx::Ctx;
use crate::error::{WebError, WebResult};
use crate::flash::{Notice, NoticeKind};
use crate::form::PostForm;
use crate::format;
use crate::params::{Only, Query};
use crate::strings::{self, EN, errors};
use crate::templates::BusyPage;
use crate::views::{BusyStatus, JobRow, Pager, ProcessRow, Totals};

/// Running jobs per page by default (like Sidekiq's busy page).
const DEFAULT_COUNT: usize = 100;

/// `GET /busy[?only=jobs|processes][&page=N][&count=N]`.
pub(crate) async fn page(ctx: Ctx) -> Response {
    let result = render(&ctx).await;
    super::finish(&ctx, result)
}

async fn render(ctx: &Ctx) -> WebResult<Response> {
    let query = Query::parse(&ctx.query);
    let only = query.only()?;
    let count = query.count(DEFAULT_COUNT);
    let api = ctx.web.api();
    let stats = api.stats().await?;
    let mut processes = api.processes().list().await?;
    processes.sort_by(|a, b| {
        format::natural_cmp(
            a.hostname().unwrap_or(a.identity()),
            b.hostname().unwrap_or(b.identity()),
        )
        .then_with(|| a.identity().cmp(b.identity()))
    });
    let threads: u64 = processes.iter().map(Process::concurrency).sum();
    let busy: u64 = processes.iter().map(Process::busy).sum();
    let rss: u64 = processes.iter().map(Process::rss_kb).sum();
    let status = BusyStatus {
        processes: format::delimit(u64::try_from(processes.len()).unwrap_or(u64::MAX)),
        threads: format::delimit(threads),
        busy: format::delimit(busy),
        utilization: format::percent(busy, threads),
        rss: format::memory(rss),
    };

    let show_jobs = only != Only::Processes;
    let (works, pager) = if show_jobs {
        let works = api.work().list().await?;
        let page = page_items(works, query.page(), count);
        let pager = Pager::new(ctx, &ctx.url("/busy"), page.number, count, page.total);
        let mut rows: Vec<JobRow> = page
            .items
            .iter()
            .map(|work| JobRow::running(ctx, work))
            .collect();
        super::add_iterations(ctx, &mut rows).await?;
        (rows, pager)
    } else {
        (Vec::new(), None)
    };

    let in_use = super::features_in_use(ctx).await?;
    let title = strings::busy_title(busy, threads);
    let page = BusyPage {
        layout: super::layout(ctx, Some(&title), Some(Totals::new(&stats)), true),
        s: &EN,
        busy_url: ctx.url("/busy"),
        status,
        show_processes: only != Only::Jobs,
        show_jobs,
        any_standalone: processes.iter().any(|process| !process.is_embedded()),
        processes: processes
            .iter()
            .map(|process| ProcessRow::new(process, ctx.now).with_lacks(process, &in_use))
            .collect(),
        works,
        pager,
    };
    Ok(super::render(ctx, StatusCode::OK, &page))
}

/// `POST /busy`: `quiet` or `stop` one process (`identity`) or every process that is not
/// embedded (no `identity`, like Sidekiq's Quiet All / Stop All).
pub(crate) async fn action(ctx: Ctx, form: PostForm) -> Response {
    let result = act(&ctx, &form).await;
    super::finish(&ctx, result)
}

async fn act(ctx: &Ctx, form: &PostForm) -> WebResult<Response> {
    let stop = form.has("stop");
    if !stop && !form.has("quiet") {
        return Err(WebError::BadRequest(errors::NO_ACTION.to_owned()));
    }
    let kind = if stop {
        NoticeKind::Stopped
    } else {
        NoticeKind::Quieted
    };
    let signal = |process: Process| async move {
        if stop {
            process.stop().await
        } else {
            process.quiet().await
        }
    };
    let processes = ctx.web.api().processes();
    let back = ctx.back_to("/busy");
    let identity = form.value("identity");
    let signalled = match identity.as_deref().filter(|identity| !identity.is_empty()) {
        Some(identity) => match processes.get(identity).await? {
            Some(process) => {
                signal(process).await?;
                1
            }
            None => {
                return Ok(ctx.redirect(&back, Some(Notice::new(NoticeKind::ProcessGone, 0, 0))));
            }
        },
        None => {
            let mut signalled = 0;
            for process in processes.list().await? {
                if process.is_embedded() {
                    continue;
                }
                signal(process).await?;
                signalled += 1;
            }
            signalled
        }
    };
    tracing::info!(processes = signalled, stop, "dashboard signalled processes");
    Ok(ctx.redirect(&back, Some(Notice::new(kind, signalled, 0))))
}
