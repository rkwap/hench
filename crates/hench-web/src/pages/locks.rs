//! The Locks tab: the locks of unique jobs (`hench::unique`). The list shows the live locks,
//! soonest expiry first, with the last hour's duplicates, replaced holders and lapses; a lock's
//! page shows every field of the lock and where its holder is. Locks are released one at a time
//! (naming the holder shown, so that a lock that changed hands meanwhile is left alone), by
//! selection, or all at once. Job pages get a Unique row that links to the job's lock.

use std::sync::Arc;

use askama::Template;
use axum::Router;
use axum::extract::Path;
use axum::response::Response;
use axum::routing::{get, post};
use hench::api::{JobRecord, SortOrder};
use hench::metrics::Period;
use hench::unique::{HolderPlace, UniqueEventCounts, UniqueEvents, UniqueLock, UniqueLocks};
use http::StatusCode;
use serde::Deserialize;

use crate::config::HenchWeb;
use crate::ctx::Ctx;
use crate::error::{WebError, WebResult};
use crate::extension::JobInfoRow;
use crate::flash::{Notice, NoticeKind};
use crate::form::{self, PostForm};
use crate::format::{self, TimeView};
use crate::params::{self, Query};
use crate::search::{self, Outcome, Pattern};
use crate::strings::locks::{self as text, LOCKS, LockStrings};
use crate::strings::{self, EN, Strings, errors};
use crate::templates::Layout;
use crate::views::{FeatureRows, Pager, Totals, last_page, segment};

/// The name of the feature in the heartbeat's `features` map of the processes.
pub(crate) const FEATURE: &str = "unique";

/// Locks one bulk release may name (a page shows at most 999).
const MAX_KEYS: usize = 1_000;

/// Locks released per round trip by a bulk release.
const RELEASE_CHUNK: usize = 100;

/// Index entries a filter reads per round trip.
const SEARCH_PAGE: usize = 1_000;

/// Classes in the table of the last hour's counters.
const TOP_CLASSES: usize = 10;

/// The label of a job's Unique row.
const UNIQUE_ROW: &str = "Unique";

/// The `{digest}` of a lock's route (extracted by name: see `queues::QueueName`).
#[derive(Deserialize)]
pub(crate) struct LockDigest {
    digest: String,
}

/// The tab's routes, mounted under `/locks` when its switch is on.
pub(crate) fn routes() -> Router<Arc<HenchWeb>> {
    Router::new()
        .route("/locks", get(list).post(release_selected))
        .route("/locks/all/release", post(release_all))
        .route("/locks/{digest}", get(lock).post(release_one))
}

/// The rows a job's detail page shows about the feature (Unique), and the payload fields
/// they show.
pub(crate) fn job_rows(ctx: &Ctx, record: &JobRecord) -> FeatureRows {
    unique_rows(ctx, record)
}

/// The Unique row of a unique job: its policy and `unique_for`, linking to its lock's page
/// (which says whether the job still holds it) when the tab is on. A malformed `unique` field
/// stays among the job's extra fields.
fn unique_rows(ctx: &Ctx, record: &JobRecord) -> FeatureRows {
    let Some(unique) = record.unique() else {
        return FeatureRows::default();
    };
    let row_text = text::job_row(unique.until, unique.unique_for);
    let row = if ctx.web.locks {
        let jid = record.jid().unwrap_or_default();
        let href = format!(
            "{}{}",
            ctx.url(&format!("/locks/{}", segment(&unique.digest))),
            params::query_string(&[("jid", jid)])
        );
        JobInfoRow::link(UNIQUE_ROW, row_text, href)
    } else {
        JobInfoRow::text(UNIQUE_ROW, row_text)
    };
    FeatureRows {
        rows: vec![row],
        shown: vec!["unique"],
    }
}

/// Whether the feature is in use in this Redis (for the Busy page's "lacks" chip of processes
/// that do not understand it): some lock is live.
pub(crate) async fn in_use(ctx: &Ctx) -> WebResult<bool> {
    Ok(ctx.web.api().unique_locks().size().await? > 0)
}

/// One lock of the list.
struct LockRow {
    /// `{digest}.{jid}`: what a bulk release names (digests never contain a `.`), as a form
    /// field ([`form::field`]: jids may hold any character).
    key: String,
    /// The lock's page.
    href: String,
    class: String,
    queue: String,
    queue_href: String,
    policy: &'static str,
    jid: String,
    locked: Option<TimeView>,
    expires: Option<TimeView>,
}

impl LockRow {
    fn new(ctx: &Ctx, lock: &UniqueLock) -> Self {
        let time = |at| TimeView::new(at, ctx.now);
        Self {
            key: form::field(&format!("{}.{}", lock.digest, lock.jid)),
            href: ctx.url(&format!("/locks/{}", segment(&lock.digest))),
            class: format::visible(&lock.class).into_owned(),
            queue_href: ctx.url(&format!("/queues/{}", segment(&lock.queue))),
            queue: format::visible(&lock.queue).into_owned(),
            policy: text::policy(lock.until),
            jid: format::visible(&lock.jid).into_owned(),
            locked: lock.locked_at.map(time),
            expires: lock.expires_at.map(time),
        }
    }
}

/// The last hour's counters: a sentence, and the classes with the most duplicates.
struct EventsView {
    sentence: String,
    classes: Vec<ClassEvents>,
}

/// One class's counters.
struct ClassEvents {
    class: String,
    duplicates: String,
    replaced: String,
    lapsed: String,
}

impl EventsView {
    fn new(events: &UniqueEvents) -> Self {
        let mut classes: Vec<(&String, &UniqueEventCounts)> = events
            .by_class
            .iter()
            .filter(|(_, counts)| counts.duplicates + counts.replaced + counts.lapsed > 0)
            .collect();
        classes.sort_by(|(a_class, a), (b_class, b)| {
            b.duplicates
                .cmp(&a.duplicates)
                .then(b.lapsed.cmp(&a.lapsed))
                .then(b.replaced.cmp(&a.replaced))
                .then(a_class.cmp(b_class))
        });
        classes.truncate(TOP_CLASSES);
        Self {
            sentence: text::last_hour(&events.totals),
            classes: classes
                .into_iter()
                .map(|(class, counts)| ClassEvents {
                    class: format::visible(class).into_owned(),
                    duplicates: format::delimit(counts.duplicates),
                    replaced: format::delimit(counts.replaced),
                    lapsed: format::delimit(counts.lapsed),
                })
                .collect(),
        }
    }
}

/// `locks.html`: the locks.
#[derive(Template)]
#[template(path = "locks.html")]
struct LocksPage {
    layout: Layout,
    s: &'static Strings,
    t: &'static LockStrings,
    total: String,
    summary: String,
    events: EventsView,
    ascending: bool,
    soonest_href: String,
    latest_href: String,
    list_url: String,
    filter_value: String,
    filter_summary: Option<String>,
    /// What the page says when it has no rows.
    empty_text: &'static str,
    rows: Vec<LockRow>,
    pager: Option<Pager>,
    /// The confirmation of "Release all", offered on the unfiltered list only (on a filtered
    /// list it would not act on the matches).
    release_all: Option<String>,
}

/// `GET /locks`.
async fn list(ctx: Ctx) -> Response {
    let result = list_page(&ctx).await;
    super::finish(&ctx, result)
}

async fn list_page(ctx: &Ctx) -> WebResult<Response> {
    let query = Query::parse(&ctx.query);
    let filter = query.filter()?;
    let order = query.direction(SortOrder::Ascending)?;
    let api = ctx.web.api();
    let locks = api.unique_locks();
    let stats = api.stats().await?;
    let events = locks.events(Period::Minutes(60)).await?;
    let list_url = ctx.url("/locks");

    let (found, total, pager, filter_summary, empty_text) = match &filter {
        Some(filter) => {
            let (found, outcome, total) =
                search_locks(&locks, &Pattern::new(filter), order).await?;
            let summary = text::filter_summary(found.len(), outcome, filter);
            (found, total, None, Some(summary), text::no_matches(outcome))
        }
        None => {
            let count = query.count(ctx.web.page_size);
            let mut page = locks.page(query.page(), count, order).await?;
            // Past the end (its last locks were just released): the last page instead.
            if let Some(last) = last_page(&page) {
                page = locks.page(last, count, order).await?;
            }
            let pager = Pager::new(ctx, &list_url, page.number, page.size, page.total);
            (page.items, page.total, pager, None, LOCKS.empty)
        }
    };
    let link = |direction: &str| {
        format!(
            "{list_url}{}",
            params::carry(&ctx.query, &[("direction", direction), ("page", "")])
        )
    };
    let title = strings::counted(LOCKS.heading, total);
    let page = LocksPage {
        layout: super::layout(ctx, Some(&title), Some(Totals::new(&stats)), true),
        s: &EN,
        t: &LOCKS,
        total: format::delimit(total),
        summary: text::summary(total),
        events: EventsView::new(&events),
        ascending: order == SortOrder::Ascending,
        soonest_href: link(""),
        latest_href: link("desc"),
        rows: found.iter().map(|lock| LockRow::new(ctx, lock)).collect(),
        release_all: (filter.is_none() && total > 0).then(|| text::confirm_release_all(total)),
        filter_value: filter.unwrap_or_default(),
        list_url,
        filter_summary,
        empty_text,
        pager,
    };
    Ok(super::render(ctx, StatusCode::OK, &page))
}

/// The text a filter is matched against: the lock's class, queue and holder, in that order
/// (a `*` can span them: `Mailer*critical`).
fn haystack(lock: &UniqueLock) -> String {
    format!("{}\n{}\n{}", lock.class, lock.queue, lock.jid)
}

/// The locks matching `pattern`, in the list's order, read a page of the index at a time: at
/// most [`search::SCAN_LIMIT`] index entries are read and [`search::MAX_MATCHES`] locks kept.
/// Returns the matches, how the search ended and the size of the index.
async fn search_locks(
    locks: &UniqueLocks,
    pattern: &Pattern,
    order: SortOrder,
) -> WebResult<(Vec<UniqueLock>, Outcome, u64)> {
    let mut found = Vec::new();
    let mut number = 1;
    loop {
        let page = locks.page(number, SEARCH_PAGE, order).await?;
        let total = page.total;
        // Index entries this page covered (entries of released locks are skipped).
        let read = u64::try_from(number.saturating_mul(SEARCH_PAGE))
            .unwrap_or(u64::MAX)
            .min(total);
        for lock in page.items {
            if pattern.matches(&haystack(&lock)) {
                found.push(lock);
                if found.len() >= search::MAX_MATCHES {
                    let outcome = if read < total {
                        Outcome::MatchLimit
                    } else {
                        Outcome::Complete
                    };
                    return Ok((found, outcome, total));
                }
            }
        }
        if read >= total {
            return Ok((found, Outcome::Complete, total));
        }
        if read >= search::SCAN_LIMIT {
            let outcome = Outcome::ScanLimit {
                searched: read,
                total,
            };
            return Ok((found, outcome, total));
        }
        number += 1;
    }
}

/// `POST /locks`: releases the selected locks (`key` fields, `{digest}.{jid}`), each only if
/// the holder shown still holds it.
async fn release_selected(ctx: Ctx, form: PostForm) -> Response {
    let result = release_keys(&ctx, &form).await;
    super::finish(&ctx, result)
}

async fn release_keys(ctx: &Ctx, form: &PostForm) -> WebResult<Response> {
    if !form.has("release") {
        return Err(WebError::BadRequest(errors::NO_ACTION.to_owned()));
    }
    let back = ctx.back_to("/locks");
    let keys: Vec<_> = form.values("key").take(MAX_KEYS + 1).collect();
    if keys.is_empty() {
        return Ok(ctx.redirect(&back, Some(Notice::new(NoticeKind::NothingSelected, 0, 0))));
    }
    if keys.len() > MAX_KEYS {
        return Err(WebError::BadRequest(text::too_many(MAX_KEYS)));
    }
    // Split at the first `.`: digests never contain one, jids may.
    let held: Vec<(&str, &str)> = keys.iter().filter_map(|key| key.split_once('.')).collect();
    let locks = ctx.web.api().unique_locks();
    let (mut released, mut failed) = (0, 0);
    for chunk in held.chunks(RELEASE_CHUNK) {
        match locks.release_each(chunk).await {
            Ok(count) => released += count,
            Err(error) => {
                tracing::warn!(%error, locks = chunk.len(), "dashboard could not release unique locks");
                failed += u64::try_from(chunk.len()).unwrap_or(u64::MAX);
            }
        }
    }
    tracing::info!(released, failed, "dashboard released unique locks");
    Ok(ctx.redirect(
        &back,
        Some(Notice::new(NoticeKind::LocksReleased, released, failed)),
    ))
}

/// `POST /locks/all/release`: releases every lock, whoever holds it.
async fn release_all(ctx: Ctx, _form: PostForm) -> Response {
    let result = release_every_lock(&ctx).await;
    super::finish(&ctx, result)
}

async fn release_every_lock(ctx: &Ctx) -> WebResult<Response> {
    let released = ctx.web.api().unique_locks().release_all(None).await?;
    tracing::info!(released, "dashboard released every unique lock");
    Ok(ctx.redirect(
        &ctx.url("/locks"),
        Some(Notice::new(NoticeKind::LocksReleased, released, 0)),
    ))
}

/// What the lock's page says about the job the visitor came from (`?jid=`).
struct NoteView {
    text: String,
    warning: bool,
}

/// One lock, ready to print.
struct LockView {
    digest: String,
    /// The holder as stored, as the release form's field ([`form::field`]).
    jid: String,
    /// The holder for display.
    jid_shown: String,
    class: String,
    queue: String,
    queue_href: String,
    policy: &'static str,
    policy_help: &'static str,
    unique_for: String,
    locked: Option<TimeView>,
    expires: Option<TimeView>,
    /// The time left before the lock expires.
    ttl: Option<String>,
    /// When the holder runs, while it waits in the schedule.
    holder_runs: Option<TimeView>,
}

/// Where the holder is, ready to print: a sentence, maybe followed by a time and a link.
struct HolderView {
    text: String,
    time: Option<TimeView>,
    /// A link to the holder (its page, or its queue) and the link's text.
    link: Option<(String, &'static str)>,
}

impl HolderView {
    fn new(ctx: &Ctx, lock: &UniqueLock, place: HolderPlace) -> Self {
        let time = |at| Some(TimeView::new(at, ctx.now));
        let entry_link = |list: &str, key: String| {
            Some((
                ctx.url(&format!("{list}/{}", segment(&key))),
                LOCKS.show_job,
            ))
        };
        match place {
            HolderPlace::Running(work) => Self {
                text: text::running(&format::visible(work.process_id())),
                time: time(work.run_at()),
                link: None,
            },
            HolderPlace::Queued { record, position } => {
                let queue = record.queue();
                let href = format!(
                    "{}{}",
                    ctx.url(&format!("/queues/{}", segment(queue))),
                    params::query_string(&[("substr", &lock.jid)])
                );
                Self {
                    text: text::queued(&format::visible(queue), position),
                    time: None,
                    link: Some((href, LOCKS.show_in_queue)),
                }
            }
            HolderPlace::Scheduled(entry) => Self {
                text: text::SCHEDULED.to_owned(),
                time: time(entry.at()),
                link: entry_link("/scheduled", entry.key()),
            },
            HolderPlace::Retrying(entry) => Self {
                text: text::RETRYING.to_owned(),
                time: time(entry.at()),
                link: entry_link("/retries", entry.key()),
            },
            HolderPlace::Dead(entry) => Self {
                text: text::DEAD.to_owned(),
                time: time(entry.at()),
                link: entry_link("/morgue", entry.key()),
            },
            HolderPlace::NotFound { complete, .. } => Self::nowhere(ctx, lock, complete),
            _ => Self::nowhere(ctx, lock, false),
        }
    }

    fn nowhere(ctx: &Ctx, lock: &UniqueLock, complete: bool) -> Self {
        let expires = lock.expires_at.map_or_else(
            || LOCKS.no_expiry.to_owned(),
            |at| format::relative(at, ctx.now),
        );
        Self {
            text: text::not_found(lock.until, complete, &expires),
            time: None,
            link: None,
        }
    }
}

/// `lock.html`: one lock.
#[derive(Template)]
#[template(path = "lock.html")]
struct LockPage {
    layout: Layout,
    s: &'static Strings,
    t: &'static LockStrings,
    heading: String,
    list_href: String,
    action_href: String,
    note: Option<NoteView>,
    lock: LockView,
    holder: HolderView,
    /// The lock's hash as stored, as JSON.
    raw: String,
}

/// `GET /locks/{digest}`.
async fn lock(ctx: Ctx, Path(LockDigest { digest }): Path<LockDigest>) -> Response {
    let result = lock_page(&ctx, &digest).await;
    super::finish(&ctx, result)
}

async fn lock_page(ctx: &Ctx, digest: &str) -> WebResult<Response> {
    let api = ctx.web.api();
    let locks = api.unique_locks();
    let Some(lock) = locks.get(digest).await? else {
        return Err(WebError::NotFound(text::LOCK_NOT_FOUND.to_owned()));
    };
    let place = locks.locate(&lock).await?;
    let stats = api.stats().await?;
    let query = Query::parse(&ctx.query);
    let note = query.get("jid").filter(|jid| !jid.is_empty()).map(|jid| {
        if jid == lock.jid {
            NoteView {
                text: text::HELD_BY_THIS_JOB.to_owned(),
                warning: false,
            }
        } else {
            NoteView {
                text: text::held_by_another(&format::visible(&lock.jid)),
                warning: true,
            }
        }
    });
    let time = |at| TimeView::new(at, ctx.now);
    let class = format::visible(&lock.class).into_owned();
    let heading = text::lock_heading(&class);
    let raw = serde_json::to_string_pretty(&lock.fields).unwrap_or_default();
    let view = LockView {
        digest: lock.digest.clone(),
        jid: form::field(&lock.jid),
        jid_shown: format::visible(&lock.jid).into_owned(),
        class,
        queue_href: ctx.url(&format!("/queues/{}", segment(&lock.queue))),
        queue: format::visible(&lock.queue).into_owned(),
        policy: text::policy(lock.until),
        policy_help: text::policy_help(lock.until),
        unique_for: text::span(lock.unique_for),
        locked: lock.locked_at.map(time),
        expires: lock.expires_at.map(time),
        ttl: lock
            .expires_at
            .and_then(|at| at.duration_since(ctx.now).ok())
            .map(text::span),
        holder_runs: lock.scheduled_at.map(time),
    };
    let holder = HolderView::new(ctx, &lock, place);
    let page = LockPage {
        // A detail page: no "Live" refresh (each load searches for the holder).
        layout: super::layout(ctx, Some(&heading), Some(Totals::new(&stats)), false),
        s: &EN,
        t: &LOCKS,
        heading,
        list_href: ctx.url("/locks"),
        action_href: ctx.url(&format!("/locks/{}", segment(&lock.digest))),
        note,
        lock: view,
        holder,
        raw,
    };
    Ok(super::render(ctx, StatusCode::OK, &page))
}

/// `POST /locks/{digest}`: releases the lock if the holder the page showed (`jid`) still
/// holds it; else the notice says it is gone.
async fn release_one(
    ctx: Ctx,
    Path(LockDigest { digest }): Path<LockDigest>,
    form: PostForm,
) -> Response {
    let result = release_shown(&ctx, &digest, &form).await;
    super::finish(&ctx, result)
}

async fn release_shown(ctx: &Ctx, digest: &str, form: &PostForm) -> WebResult<Response> {
    let jid = form.value("jid").unwrap_or_default();
    let jid = jid.as_ref();
    if jid.is_empty() {
        return Err(WebError::BadRequest(text::NO_HOLDER.to_owned()));
    }
    let back = ctx.back_to("/locks");
    let released = ctx.web.api().unique_locks().release(digest, jid).await?;
    let notice = if released {
        tracing::info!(digest, jid, "dashboard released a unique lock");
        Notice {
            count: 1,
            ..Notice::about(NoticeKind::LocksReleased, digest)
        }
    } else {
        Notice::new(NoticeKind::Gone, 0, 0)
    };
    Ok(ctx.redirect(&back, Some(notice)))
}

#[cfg(test)]
mod tests {
    //! The Locks pages (`docs/design/unique.md` §3.5, W1-W3) on a private Redis, served by
    //! their routes composed like the dashboard composes them (the state, then the guard).

    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use axum::body::Body;
    use hench::api::Api;
    use hench::test_util::TestRedis;
    use hench::{JobPayload, Unique, UniqueUntil, keys, redis_rs};
    use http::header::{CONTENT_TYPE, COOKIE, LOCATION, SET_COOKIE};
    use http::{HeaderMap, Request};
    use http_body_util::BodyExt;
    use scraper::{Html, Selector};
    use serde_json::{Value, json};
    use tower::ServiceExt;

    use super::*;

    /// A CSRF token as the dashboard issues them (any well-formed value works when the cookie
    /// and the field agree).
    const TOKEN: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    struct App {
        _redis: TestRedis,
        api: Api,
        router: Router,
    }

    struct Res {
        status: StatusCode,
        headers: HeaderMap,
        body: String,
    }

    impl Res {
        fn html(&self) -> Html {
            Html::parse_document(&self.body)
        }

        fn texts(&self, selector: &str) -> Vec<String> {
            let selector = Selector::parse(selector).unwrap();
            self.html()
                .select(&selector)
                .map(|element| element.text().collect::<String>().trim().to_owned())
                .collect()
        }

        fn attrs(&self, selector: &str, attribute: &str) -> Vec<String> {
            let selector = Selector::parse(selector).unwrap();
            self.html()
                .select(&selector)
                .filter_map(|element| element.value().attr(attribute).map(str::to_owned))
                .collect()
        }

        fn count(&self, selector: &str) -> usize {
            let selector = Selector::parse(selector).unwrap();
            self.html().select(&selector).count()
        }

        fn location(&self) -> &str {
            self.headers
                .get(LOCATION)
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
        }

        /// The value of the notice cookie the response sets.
        fn notice_cookie(&self) -> Option<String> {
            self.headers
                .get_all(SET_COOKIE)
                .iter()
                .filter_map(|value| value.to_str().ok())
                .find_map(|cookie| cookie.strip_prefix("hench_flash="))
                .and_then(|rest| rest.split(';').next())
                .map(str::to_owned)
        }

        #[track_caller]
        fn assert_status(&self, status: StatusCode) -> &Self {
            assert_eq!(self.status, status, "body: {}", self.body);
            self
        }
    }

    impl App {
        async fn new() -> Self {
            Self::with(|web| web).await
        }

        async fn with(configure: impl FnOnce(HenchWeb) -> HenchWeb) -> Self {
            let redis = TestRedis::start().await;
            let api = Api::new(redis.pool().await);
            let web = Arc::new(configure(HenchWeb::new(&api)));
            let router =
                routes()
                    .with_state(Arc::clone(&web))
                    .layer(axum::middleware::from_fn_with_state(
                        web,
                        crate::guard::guard,
                    ));
            Self {
                _redis: redis,
                api,
                router,
            }
        }

        async fn send(&self, request: Request<Body>) -> Res {
            let response = self.router.clone().oneshot(request).await.unwrap();
            let status = response.status();
            let headers = response.headers().clone();
            let bytes = response.into_body().collect().await.unwrap().to_bytes();
            Res {
                status,
                headers,
                body: String::from_utf8_lossy(&bytes).into_owned(),
            }
        }

        async fn get(&self, path: &str) -> Res {
            self.send(Request::get(path).body(Body::empty()).unwrap())
                .await
        }

        /// `GET path` showing the notice a redirect left.
        async fn follow(&self, redirect: &Res) -> Res {
            let notice = redirect.notice_cookie().expect("a notice");
            self.send(
                Request::get(redirect.location())
                    .header(COOKIE, format!("hench_csrf={TOKEN}; hench_flash={notice}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
        }

        /// A same-origin form POST carrying the CSRF token like a browser would.
        async fn post(&self, path: &str, fields: &[(&str, &str)]) -> Res {
            self.send(form(
                path,
                fields,
                TOKEN,
                &[("sec-fetch-site", "same-origin")],
            ))
            .await
        }

        async fn redis<T: redis_rs::FromRedisValue>(&self, command: &redis_rs::Cmd) -> T {
            let mut conn = self.api.redis().conn().await.unwrap();
            command.query_async(&mut conn).await.unwrap()
        }

        /// Pushes a unique job; its jid, or `None` for a duplicate.
        async fn push(&self, job: Pushed<'_>) -> Option<String> {
            let mut payload = JobPayload::new(job.class, vec![json!(job.key)]);
            payload.queue = job.queue.to_owned();
            payload.unique = Some(
                Unique::new(Duration::from_secs(job.unique_for))
                    .until(job.until)
                    .into(),
            );
            payload.at = job.at;
            self.api.client().push(payload).await.unwrap()
        }

        async fn lock_of(&self, job: Pushed<'_>) -> UniqueLock {
            self.api
                .unique_locks()
                .get(&job.digest())
                .await
                .unwrap()
                .expect("a lock")
        }
    }

    /// A unique job to push.
    #[derive(Clone, Copy)]
    struct Pushed<'a> {
        class: &'a str,
        queue: &'a str,
        key: i64,
        unique_for: u64,
        until: UniqueUntil,
        at: Option<f64>,
    }

    impl Pushed<'_> {
        fn digest(&self) -> String {
            hench::unique::digest(self.class, Some(self.queue), &json!([self.key])).unwrap()
        }
    }

    const SYNC: Pushed<'static> = Pushed {
        class: "Sync",
        queue: "default",
        key: 1,
        unique_for: 60,
        until: UniqueUntil::Success,
        at: None,
    };
    const MAILER: Pushed<'static> = Pushed {
        class: "Mailer",
        queue: "mail",
        key: 2,
        unique_for: 600,
        until: UniqueUntil::Start,
        at: None,
    };
    const REPORT: Pushed<'static> = Pushed {
        class: "Report",
        queue: "default",
        key: 3,
        unique_for: 1_200,
        until: UniqueUntil::Expired,
        at: None,
    };

    fn now_secs() -> f64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64()
    }

    fn form(
        path: &str,
        fields: &[(&str, &str)],
        token: &str,
        headers: &[(&str, &str)],
    ) -> Request<Body> {
        let mut body = form_urlencoded::Serializer::new(String::new());
        body.append_pair("_csrf", token);
        for (key, value) in fields {
            body.append_pair(key, value);
        }
        let mut request = Request::post(path)
            .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header(COOKIE, format!("hench_csrf={TOKEN}"));
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        request.body(Body::from(body.finish())).unwrap()
    }

    /// Pushes SYNC, MAILER and REPORT (REPORT an hour ahead), and two duplicates of SYNC.
    async fn seeded() -> (App, [String; 3]) {
        let app = App::new().await;
        let sync = app.push(SYNC).await.unwrap();
        let mailer = app.push(MAILER).await.unwrap();
        let report = app
            .push(Pushed {
                at: Some(now_secs() + 3_600.0),
                ..REPORT
            })
            .await
            .unwrap();
        for _ in 0..2 {
            assert_eq!(app.push(SYNC).await, None, "a duplicate");
        }
        (app, [sync, mailer, report])
    }

    #[tokio::test]
    async fn the_list_shows_the_locks_and_the_last_hour() {
        // W1.
        let (app, [sync, ..]) = seeded().await;
        let res = app.get("/locks").await;
        res.assert_status(StatusCode::OK);
        assert!(res.headers.contains_key("content-security-policy"));
        assert_eq!(res.texts("title"), ["Unique locks (3) · Hench"]);
        assert_eq!(res.texts(".count-badge"), ["3"]);
        let summary = res.texts(".locks-summary").concat();
        assert!(summary.contains("3 unique jobs hold a lock."), "{summary}");
        assert!(
            summary.contains("Last hour: 2 duplicates not pushed, 0 replaced, 0 lapsed."),
            "{summary}"
        );
        // Soonest expiry first.
        assert_eq!(
            res.texts(".locks-table .job-class"),
            ["Sync", "Mailer", "Report"]
        );
        assert_eq!(
            res.texts(".locks-table .lock-policy"),
            ["until success", "until start", "until expired"]
        );
        assert_eq!(res.texts(".locks-table .lock-holder-link")[0], sync);
        assert_eq!(
            res.attrs(".locks-table .lock-holder-link", "href")[0],
            format!("/locks/{}", SYNC.digest())
        );
        assert_eq!(
            res.attrs("input[name=key]", "value")[0],
            format!("{}.{sync}", SYNC.digest())
        );
        assert_eq!(res.count("button[formaction='/locks/all/release']"), 1);
        // The last hour's counters per class.
        assert_eq!(res.texts(".locks-events tbody td"), ["Sync", "2", "0", "0"]);
        // Latest expiry first.
        let res = app.get("/locks?direction=desc").await;
        assert_eq!(
            res.texts(".locks-table .job-class"),
            ["Report", "Mailer", "Sync"]
        );
        // Pages.
        let res = app.get("/locks?count=2").await;
        assert_eq!(res.count(".locks-table tbody tr"), 2);
        assert!(res.count(".pager") > 0);
        let res = app.get("/locks?count=2&page=2").await;
        assert_eq!(res.texts(".locks-table .job-class"), ["Report"]);
        let res = app.get("/locks?count=2&page=9").await;
        assert_eq!(
            res.texts(".locks-table .job-class"),
            ["Report"],
            "the last page"
        );
        app.get("/locks?direction=sideways")
            .await
            .assert_status(StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn filters_match_the_class_queue_and_holder() {
        let (app, [_, _, report]) = seeded().await;
        let res = app.get("/locks?substr=Mail").await;
        assert_eq!(res.texts(".locks-table .job-class"), ["Mailer"]);
        assert_eq!(res.texts(".filter-summary"), ["1 lock matching “Mail”."]);
        // No "Release all" on a filtered list: it would not act on the matches.
        assert_eq!(res.count("button[formaction='/locks/all/release']"), 0);
        let res = app.get("/locks?substr=Sync*default").await;
        assert_eq!(res.texts(".locks-table .job-class"), ["Sync"]);
        let res = app.get(&format!("/locks?substr={report}")).await;
        assert_eq!(res.texts(".locks-table .job-class"), ["Report"]);
        let res = app.get("/locks?substr=default&direction=desc").await;
        assert_eq!(res.texts(".locks-table .job-class"), ["Report", "Sync"]);
        let res = app.get("/locks?substr=nothing").await;
        assert_eq!(res.texts(".empty"), [LOCKS.no_matches]);
        let long = "x".repeat(257);
        app.get(&format!("/locks?substr={long}"))
            .await
            .assert_status(StatusCode::BAD_REQUEST);
        // An empty list.
        let empty = App::new().await;
        let res = empty.get("/locks").await;
        assert_eq!(res.texts(".empty"), [LOCKS.empty]);
        assert_eq!(res.count(".locks-events"), 0);
    }

    #[tokio::test]
    async fn selected_locks_are_released_if_their_holder_still_holds_them() {
        // W1: a bulk release with CSRF; a lock that changed hands is left alone.
        let (app, [sync, _, report]) = seeded().await;
        let res = app
            .post(
                "/locks",
                &[
                    ("key", &format!("{}.{sync}", SYNC.digest())),
                    (
                        "key",
                        &format!("{}.0123456789abcdef01234567", MAILER.digest()),
                    ),
                    ("key", &format!("{}.{report}", REPORT.digest())),
                    ("key", "no dot"),
                    ("release", "1"),
                ],
            )
            .await;
        res.assert_status(StatusCode::SEE_OTHER);
        assert_eq!(res.location(), "/locks");
        let shown = app.follow(&res).await;
        assert_eq!(shown.texts(".notice"), ["Released 2 locks."]);
        assert_eq!(shown.texts(".locks-table .job-class"), ["Mailer"]);
        // Nothing selected.
        let res = app.post("/locks", &[("release", "1")]).await;
        let shown = app.follow(&res).await;
        assert_eq!(shown.texts(".notice"), ["Select at least one job first."]);
        // No action, too many locks.
        app.post("/locks", &[("key", "a.b")])
            .await
            .assert_status(StatusCode::BAD_REQUEST);
        let keys: Vec<(&str, &str)> = std::iter::repeat_n(("key", "a.b"), MAX_KEYS + 1)
            .chain([("release", "1")])
            .collect();
        app.post("/locks", &keys)
            .await
            .assert_status(StatusCode::BAD_REQUEST);
        // No token, another site.
        let key = format!("{}.x", MAILER.digest());
        let fields = [("key", key.as_str()), ("release", "1")];
        let forged = form("/locks", &fields, &"f".repeat(64), &[]);
        app.send(forged).await.assert_status(StatusCode::FORBIDDEN);
        let cross = form(
            "/locks",
            &fields,
            TOKEN,
            &[("sec-fetch-site", "cross-site")],
        );
        app.send(cross).await.assert_status(StatusCode::FORBIDDEN);
        assert_eq!(app.api.unique_locks().size().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn every_lock_is_released_at_once() {
        let (app, _) = seeded().await;
        let res = app.post("/locks/all/release", &[]).await;
        res.assert_status(StatusCode::SEE_OTHER);
        let shown = app.follow(&res).await;
        assert_eq!(shown.texts(".notice"), ["Released 3 locks."]);
        assert_eq!(shown.texts(".empty"), [LOCKS.empty]);
        assert_eq!(app.api.unique_locks().size().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn read_only_dashboards_show_no_forms_and_refuse_releases() {
        let app = App::with(|web| web.read_only(true)).await;
        app.push(SYNC).await.unwrap();
        let res = app.get("/locks").await;
        res.assert_status(StatusCode::OK);
        assert_eq!(res.count("form[method=post]"), 0);
        assert_eq!(res.count("input[type=checkbox]"), 0);
        let page = app.get(&format!("/locks/{}", SYNC.digest())).await;
        page.assert_status(StatusCode::OK);
        assert_eq!(page.count("form[method=post]"), 0);
        for path in [
            "/locks".to_owned(),
            "/locks/all/release".to_owned(),
            format!("/locks/{}", SYNC.digest()),
        ] {
            app.post(&path, &[("release", "1")])
                .await
                .assert_status(StatusCode::FORBIDDEN);
        }
        assert_eq!(app.api.unique_locks().size().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn the_lock_page_shows_the_lock_and_finds_its_holder() {
        // W2: queued, scheduled, retrying, dead, running, nowhere.
        let app = App::new().await;
        let jid = app.push(SYNC).await.unwrap();
        let path = format!("/locks/{}", SYNC.digest());
        let res = app.get(&path).await;
        res.assert_status(StatusCode::OK);
        assert_eq!(res.texts("h1"), ["Lock of “Sync”"]);
        let fields = res.texts(".kv td").join("\n");
        for expected in [
            "Sync",
            "default",
            jid.as_str(),
            "until success",
            "1 minute",
            SYNC.digest().as_str(),
        ] {
            assert!(fields.contains(expected), "{expected}: {fields}");
        }
        assert!(
            res.texts("pre.code")
                .concat()
                .contains(&format!("\"jid\": \"{jid}\""))
        );
        assert_eq!(
            res.texts(".lock-holder"),
            ["Waiting in queue “default”, next to run. Show it in the queue"]
        );
        assert_eq!(
            res.attrs(".lock-holder a", "href"),
            [format!("/queues/default?substr={jid}")]
        );
        assert_eq!(
            res.attrs("input[name=jid]", "value"),
            std::slice::from_ref(&jid)
        );
        // Running.
        let job = app
            .api
            .queue("default")
            .find_job(&jid)
            .await
            .unwrap()
            .unwrap();
        let () = app
            .redis(redis_rs::cmd("SADD").arg(keys::PROCESSES).arg("host:1:abc"))
            .await;
        let work = json!({"queue": "default", "payload": job.value(), "run_at": now_secs()});
        let () = app
            .redis(
                redis_rs::cmd("HSET")
                    .arg(keys::work("host:1:abc"))
                    .arg("slot")
                    .arg(work.to_string()),
            )
            .await;
        let res = app.get(&path).await;
        assert!(
            res.texts(".lock-holder")[0].starts_with("Running on host:1:abc, started"),
            "{:?}",
            res.texts(".lock-holder")
        );
        // Nowhere: it ran (its lock released when it expires).
        let () = app
            .redis(redis_rs::cmd("DEL").arg(keys::work("host:1:abc")))
            .await;
        let () = app
            .redis(redis_rs::cmd("DEL").arg(keys::queue("default")))
            .await;
        let res = app.get(&path).await;
        assert!(
            res.texts(".lock-holder")[0].starts_with("The holder is not running"),
            "{:?}",
            res.texts(".lock-holder")
        );
        // Scheduled, retrying, dead.
        let at = now_secs() + 3_600.0;
        let scheduled = app
            .push(Pushed {
                at: Some(at),
                ..REPORT
            })
            .await
            .unwrap();
        let path = format!("/locks/{}", REPORT.digest());
        let res = app.get(&path).await;
        assert!(res.texts(".lock-holder")[0].starts_with("Scheduled to run in "));
        let href = &res.attrs(".lock-holder a", "href")[0];
        assert!(
            href.starts_with("/scheduled/") && href.ends_with(&scheduled),
            "{href}"
        );
        assert!(res.texts(".kv th").contains(&LOCKS.holder_runs.to_owned()));
        for (from, to, text, list) in [
            (keys::SCHEDULE, keys::RETRY, text::RETRYING, "/retries/"),
            (keys::RETRY, keys::DEAD, text::DEAD, "/morgue/"),
        ] {
            let members: Vec<(String, f64)> = app
                .redis(
                    redis_rs::cmd("ZRANGE")
                        .arg(from)
                        .arg(0)
                        .arg(-1)
                        .arg("WITHSCORES"),
                )
                .await;
            let (member, score) = members.into_iter().next().unwrap();
            let () = app
                .redis(redis_rs::cmd("ZREM").arg(from).arg(&member))
                .await;
            let () = app
                .redis(redis_rs::cmd("ZADD").arg(to).arg(score).arg(&member))
                .await;
            let res = app.get(&path).await;
            assert!(res.texts(".lock-holder")[0].starts_with(text), "{text}");
            assert!(res.attrs(".lock-holder a", "href")[0].starts_with(list));
        }
        // Unknown and invalid digests.
        let res = app.get("/locks/unknown").await;
        res.assert_status(StatusCode::NOT_FOUND);
        assert!(res.body.contains("No unique job holds this lock"));
        app.get("/locks/not%20a%20digest")
            .await
            .assert_status(StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn the_lock_page_says_whether_the_visitor_s_job_holds_it() {
        // W3: from a job's Unique row (`?jid=`).
        let app = App::new().await;
        let jid = app.push(SYNC).await.unwrap();
        let path = format!("/locks/{}", SYNC.digest());
        let res = app.get(&format!("{path}?jid={jid}")).await;
        assert_eq!(res.texts(".notice"), [text::HELD_BY_THIS_JOB]);
        assert_eq!(res.count(".notice-warning"), 0);
        let res = app
            .get(&format!("{path}?jid=0123456789abcdef01234567"))
            .await;
        assert_eq!(res.count(".notice-warning"), 1);
        assert!(res.texts(".notice-warning")[0].contains(&jid));
        let res = app.get(&path).await;
        assert_eq!(res.count(".notice"), 0);
    }

    #[tokio::test]
    async fn one_lock_is_released_with_the_holder_shown() {
        let app = App::new().await;
        let jid = app.push(SYNC).await.unwrap();
        let path = format!("/locks/{}", SYNC.digest());
        let res = app.post(&path, &[("jid", &jid), ("release", "1")]).await;
        res.assert_status(StatusCode::SEE_OTHER);
        assert_eq!(res.location(), "/locks");
        let shown = app.follow(&res).await;
        assert_eq!(
            shown.texts(".notice"),
            [format!("Released lock “{}”.", SYNC.digest())]
        );
        // Gone, then taken by another job: the old holder's release leaves it.
        let res = app.post(&path, &[("jid", &jid), ("release", "1")]).await;
        let shown = app.follow(&res).await;
        assert_eq!(shown.count(".notice-warning"), 1);
        let holder = app.push(SYNC).await.unwrap();
        let res = app.post(&path, &[("jid", &jid), ("release", "1")]).await;
        let shown = app.follow(&res).await;
        assert!(shown.texts(".notice")[0].starts_with("That job is gone"));
        assert_eq!(app.lock_of(SYNC).await.jid, holder);
        // No holder named.
        app.post(&path, &[("release", "1")])
            .await
            .assert_status(StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn hostile_names_are_escaped_and_spelled_out() {
        let app = App::new().await;
        let hostile = Pushed {
            class: "<script>alert(1)</script>\u{1b}[31m",
            queue: "q\nx",
            ..SYNC
        };
        app.push(hostile).await.unwrap();
        for path in ["/locks".to_owned(), format!("/locks/{}", hostile.digest())] {
            let res = app.get(&path).await;
            res.assert_status(StatusCode::OK);
            assert!(!res.body.contains("<script>alert(1)"), "{path}");
            let start = res
                .body
                .find("alert(1)")
                .map_or(0, |at| at.saturating_sub(40));
            assert!(
                res.body.contains("&#60;script&#62;alert(1)")
                    || res.body.contains("&lt;script&gt;alert(1)"),
                "{path}: {}",
                &res.body[start..(start + 120).min(res.body.len())]
            );
            assert!(res.body.contains(r"\u{1b}[31m"), "{path}");
            assert!(res.body.contains(r"q\nx"), "{path}");
        }
    }

    /// The context of a request for `path` to the dashboard `web`.
    fn ctx(web: HenchWeb, path: &str) -> Ctx {
        let (parts, ()) = Request::get(path).body(()).unwrap().into_parts();
        Ctx::from_parts(&parts, &Arc::new(web)).unwrap()
    }

    #[tokio::test]
    async fn job_pages_get_a_unique_row() {
        // W3: the row's policy and unique_for, linking to the lock with the job's jid.
        let redis = TestRedis::start().await;
        let api = Api::new(redis.pool().await);
        let record = |unique: Value| {
            let job = json!({"class": "Sync", "args": [1], "jid": "abc", "unique": unique});
            JobRecord::new(&api, job.to_string(), None)
        };
        let job = record(json!({"for": 600, "until": "start", "digest": "rP3m"}));
        let rows = unique_rows(&ctx(HenchWeb::new(&api), "/scheduled/x"), &job);
        assert_eq!(rows.shown, ["unique"]);
        let [row] = rows.rows.as_slice() else {
            panic!("one row");
        };
        assert_eq!(row.label(), "Unique");
        assert_eq!(row.value(), "until start, unique for 10 minutes");
        assert!(
            row.value().contains("unique"),
            "the job pages' tests look for it"
        );
        assert_eq!(row.href(), Some("/locks/rP3m?jid=abc"));
        // The tab off: no link.
        let rows = unique_rows(&ctx(HenchWeb::new(&api).locks(false), "/x"), &job);
        assert_eq!(rows.rows[0].href(), None);
        // Under a prefix.
        let (parts, ()) = Request::get("/hench/scheduled/x")
            .body(())
            .unwrap()
            .into_parts();
        let mut parts = parts;
        parts.uri = "/scheduled/x".parse().unwrap();
        parts.extensions.insert(axum::extract::OriginalUri(
            "/hench/scheduled/x".parse().unwrap(),
        ));
        let nested = Ctx::from_parts(&parts, &Arc::new(HenchWeb::new(&api))).unwrap();
        assert_eq!(
            unique_rows(&nested, &job).rows[0].href(),
            Some("/hench/locks/rP3m?jid=abc")
        );
        // Not unique, or malformed: no row, the field stays among the extras.
        for unique in [json!(false), json!({"for": 600}), json!("yes")] {
            let rows = unique_rows(&ctx(HenchWeb::new(&api), "/x"), &record(unique));
            assert!(rows.rows.is_empty() && rows.shown.is_empty());
        }
        let rows = job_rows(&ctx(HenchWeb::new(&api), "/x"), &job);
        assert_eq!(rows.rows.len(), 1);
    }

    #[tokio::test]
    async fn the_feature_is_in_use_while_a_lock_is_held() {
        let app = App::new().await;
        let web = || HenchWeb::new(&app.api);
        assert!(!in_use(&ctx(web(), "/busy")).await.unwrap());
        app.push(SYNC).await.unwrap();
        assert!(in_use(&ctx(web(), "/busy")).await.unwrap());
    }
}
