//! The Periodic tab: the periodic schedules (`hench::periodic`). The list shows every schedule,
//! by name, with its timetable, job, next run, last enqueue and last run, under banners for what
//! keeps schedules from firing (no poller, overdue schedules, processes of one group defining
//! different crontabs); a schedule's page shows its definition with its next ticks in its time
//! zone, its state and its history. Schedules are enqueued now, paused, resumed and deleted one
//! at a time or by selection; a schedule defined in the code of running processes is not
//! deleted (they would register it again). Job pages get a Periodic row that links to the
//! schedule that enqueued the job.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use askama::Template;
use axum::Router;
use axum::extract::Path;
use axum::response::Response;
use axum::routing::get;
use hench::Config;
use hench::api::{Api, JobRecord, Process};
use hench::periodic::{
    HistoryEntry, PeriodicEntry, PeriodicSet, PeriodicSource, RunStatus, Timetable,
};
use http::StatusCode;
use serde::Deserialize;
use serde_json::Value;

use crate::config::HenchWeb;
use crate::ctx::Ctx;
use crate::error::{WebError, WebResult};
use crate::extension::JobInfoRow;
use crate::flash::{Notice, NoticeKind};
use crate::form::PostForm;
use crate::format::{self, TimeView};
use crate::params::Query;
use crate::search::{self, Outcome, Pattern};
use crate::strings::periodic::{self as text, PERIODIC, PeriodicStrings};
use crate::strings::{self, EN, Strings, errors};
use crate::templates::Layout;
use crate::views::{FeatureRows, Pager, Totals, last_page, segment};

/// The name of the feature in the heartbeat's `features` map of the processes.
pub(crate) const FEATURE: &str = "periodic";

/// Schedules one bulk action may name (a page shows at most 999).
const MAX_NAMES: usize = 1_000;

/// Schedules a filter reads per round trip.
const SEARCH_PAGE: usize = 1_000;

/// Characters of a template's arguments shown in a list row.
const SHORT_ARGS: usize = 120;

/// Characters of a template's arguments in the row's tooltip.
const FULL_ARGS: usize = 2_000;

/// Next ticks shown on a schedule's page.
const UPCOMING: usize = 5;

/// History entries shown on a schedule's page (the processes keep 10 unless set otherwise).
const HISTORY: usize = 100;

/// The label of a job's Periodic row.
const PERIODIC_ROW: &str = "Periodic";

/// How long past its time an active schedule counts as overdue: the default lease plus the
/// default poll interval (a poller handles a due schedule within its poll interval, and a lease
/// left by a crashed poller ends after the lease).
const LATE_AFTER: Duration = Duration::from_secs(
    Config::DEFAULT_PERIODIC_LEASE.as_secs() + Config::DEFAULT_PERIODIC_POLL_INTERVAL.as_secs(),
);

/// The `{name}` of a schedule's route (extracted by name: see `queues::QueueName`). Names may
/// contain `/` (sent as `%2F`, decoded within the segment).
#[derive(Deserialize)]
pub(crate) struct ScheduleName {
    name: String,
}

/// The tab's routes, mounted under `/periodic` when its switch is on.
pub(crate) fn routes() -> Router<Arc<HenchWeb>> {
    Router::new()
        .route("/periodic", get(list).post(act_on_selected))
        .route("/periodic/{name}", get(schedule).post(act_on_one))
}

/// The rows a job's detail page shows about the feature (Periodic), and the payload fields
/// they show.
pub(crate) fn job_rows(ctx: &Ctx, record: &JobRecord) -> FeatureRows {
    periodic_rows(ctx, record)
}

/// The Periodic row of a job a schedule enqueued: the schedule and the tick, linking to the
/// schedule's page when the tab is on. A malformed `periodic` field stays among the job's extra
/// fields.
fn periodic_rows(ctx: &Ctx, record: &JobRecord) -> FeatureRows {
    let Some(tick) = record.periodic() else {
        return FeatureRows::default();
    };
    let row_text = text::job_row(
        &format::visible(&tick.name),
        &format::utc(tick.tick),
        tick.manual,
    );
    let row = if ctx.web.periodic {
        let href = ctx.url(&format!("/periodic/{}", segment(&tick.name)));
        JobInfoRow::link(PERIODIC_ROW, row_text, href)
    } else {
        JobInfoRow::text(PERIODIC_ROW, row_text)
    };
    FeatureRows {
        rows: vec![row],
        shown: vec!["periodic"],
    }
}

/// Whether the feature is in use in this Redis (for the Busy page's "lacks" chip of processes
/// that do not understand it): some schedule exists.
pub(crate) async fn in_use(ctx: &Ctx) -> WebResult<bool> {
    Ok(ctx.web.api().periodic().size().await? > 0)
}

/// What the live processes say about periodic jobs, from the `periodic` object of their
/// heartbeats (`{"poller", "group", "digest", "schedules"}`). A process that understands
/// periodic jobs (`features.periodic`) without that object (a version that does not describe
/// its poller) may poll and register anything: the pages then assume it does.
#[derive(Debug, Default)]
struct Fleet {
    /// Processes running a poller.
    pollers: usize,
    /// Processes that understand periodic jobs but do not say what they do with them.
    undeclared: usize,
    /// The crontab digests each group's processes declare.
    groups: BTreeMap<String, BTreeSet<String>>,
}

impl Fleet {
    async fn read(api: &Api) -> WebResult<Self> {
        let mut fleet = Self::default();
        for process in api.processes().list().await? {
            fleet.add(&process);
        }
        Ok(fleet)
    }

    fn add(&mut self, process: &Process) {
        match process.get("periodic") {
            Some(Value::Object(periodic)) => {
                if periodic.get("poller").and_then(Value::as_bool) == Some(true) {
                    self.pollers += 1;
                }
                if let Some(group) = periodic.get("group").and_then(Value::as_str) {
                    let digests = self.groups.entry(group.to_owned()).or_default();
                    if let Some(digest) = periodic.get("digest").and_then(Value::as_str) {
                        digests.insert(digest.to_owned());
                    }
                }
            }
            _ => {
                let understands = process
                    .get("features")
                    .and_then(Value::as_object)
                    .is_some_and(|features| features.contains_key(FEATURE));
                if understands {
                    self.undeclared += 1;
                }
            }
        }
    }

    /// Whether some live process polls the schedules (or may: one that does not say).
    fn polls(&self) -> bool {
        self.pollers > 0 || self.undeclared > 0
    }

    /// Whether no live process registers `group`: then nothing would register its schedules
    /// again, and deleting them is safe.
    fn orphaned(&self, group: &str) -> bool {
        self.undeclared == 0 && !self.groups.contains_key(group)
    }

    /// The groups whose live processes declare different crontabs, with how many.
    fn disagreements(&self) -> Vec<(&str, usize)> {
        self.groups
            .iter()
            .filter(|(_, digests)| digests.len() > 1)
            .map(|(group, digests)| (group.as_str(), digests.len()))
            .collect()
    }

    /// Whether `entry` may be deleted from the dashboard: dynamic schedules always, code
    /// schedules when their group is orphaned.
    fn deletable(&self, entry: &PeriodicEntry) -> bool {
        match &entry.source {
            PeriodicSource::Code { group } => self.orphaned(group),
            _ => true,
        }
    }
}

/// A time on a schedule's clock: relative text, the UTC time for the tooltip and the time in
/// the schedule's zone.
struct ZonedTime {
    iso: String,
    utc: String,
    text: String,
    local: String,
}

impl ZonedTime {
    fn new(at: SystemTime, now: SystemTime, timetable: Option<&Timetable>) -> Self {
        let view = TimeView::new(at, now);
        let local = timetable
            .and_then(|timetable| timetable.local_time(at))
            .unwrap_or_else(|| view.utc.clone());
        Self {
            iso: view.iso,
            utc: view.utc,
            text: view.text,
            local,
        }
    }
}

/// A timetable, ready to print: the cron expression (none for intervals) and its English
/// description (with the zone).
struct TimetableView {
    expression: Option<String>,
    description: String,
}

impl TimetableView {
    fn new(entry: &PeriodicEntry) -> Self {
        let expression = match &entry.timetable {
            Some(Timetable::Cron { expression, .. }) => {
                Some(format::visible(expression).into_owned())
            }
            _ => None,
        };
        Self {
            expression,
            description: format::visible(&entry.describe()).into_owned(),
        }
    }
}

/// A schedule's job, ready to print.
struct JobView {
    class: String,
    queue: String,
    queue_href: String,
    /// The arguments, cut for a list row.
    args: String,
    /// The arguments in full (up to a bound) when the row's text was cut.
    args_full: Option<String>,
}

impl JobView {
    fn new(ctx: &Ctx, entry: &PeriodicEntry) -> Option<Self> {
        let job = entry.job.as_ref()?;
        let (args, args_full) = format::args_summary(&job.args, SHORT_ARGS, FULL_ARGS);
        // A template without a queue gets the client's default one when pushed.
        let queue = if job.queue.is_empty() {
            "default"
        } else {
            job.queue.as_str()
        };
        Some(Self {
            class: format::visible(&job.class).into_owned(),
            queue_href: ctx.url(&format!("/queues/{}", segment(queue))),
            queue: format::visible(queue).into_owned(),
            args: format::visible(&args).into_owned(),
            args_full: args_full.map(|full| format::visible(&full).into_owned()),
        })
    }
}

/// The last tick handled, for a list row.
struct LastView {
    at: TimeView,
    result: &'static str,
    warning: bool,
    late: Option<String>,
}

/// The last run, for a list row and a schedule's page.
struct RunView {
    at: TimeView,
    status: &'static str,
    failed: bool,
    duration: String,
    error: Option<String>,
    jid: String,
}

impl RunView {
    fn new(entry: &PeriodicEntry, now: SystemTime) -> Option<Self> {
        let run = entry.last_run.as_ref()?;
        Some(Self {
            at: TimeView::new(run.at, now),
            status: text::run_status(run.status),
            failed: run.status == Some(RunStatus::Failed),
            duration: format::millis(u64::try_from(run.duration.as_millis()).unwrap_or(u64::MAX)),
            error: run
                .error
                .as_deref()
                .filter(|error| !error.is_empty())
                .map(|error| format::visible(error).into_owned()),
            jid: run
                .jid
                .as_deref()
                .map(|jid| format::visible(jid).into_owned())
                .unwrap_or_default(),
        })
    }
}

/// `text` with its control characters spelled out (like [`format::visible`]), line breaks
/// kept: for descriptions and errors, shown with their lines.
fn visible_lines(text: &str) -> String {
    text.split('\n')
        .map(|line| format::visible(line).into_owned())
        .collect::<Vec<_>>()
        .join("\n")
}

/// Whether `entry` is active and overdue (see [`LATE_AFTER`]).
fn is_late(entry: &PeriodicEntry, now: SystemTime) -> bool {
    !entry.is_paused()
        && entry.next.is_some_and(|next| {
            now.duration_since(next)
                .is_ok_and(|overdue| overdue > LATE_AFTER)
        })
}

/// One schedule of the list.
struct ScheduleRow {
    /// The name as stored (what the forms send).
    name: String,
    /// The name for display.
    shown: String,
    /// The schedule's page (also where its row actions post).
    href: String,
    paused: bool,
    /// The group of a code schedule (`None`: a dynamic one).
    group: Option<String>,
    /// Who defines it, in words (the source chip's tooltip).
    source: String,
    error: Option<String>,
    late: bool,
    timetable: TimetableView,
    job: Option<JobView>,
    next: Option<ZonedTime>,
    last: Option<LastView>,
    run: Option<RunView>,
    /// The confirmation of its Delete button, offered when it may be deleted.
    delete_confirm: Option<&'static str>,
}

impl ScheduleRow {
    fn new(ctx: &Ctx, entry: &PeriodicEntry, fleet: &Fleet) -> Self {
        let now = ctx.now;
        let group = match &entry.source {
            PeriodicSource::Code { group } => Some(group.as_str()),
            _ => None,
        };
        Self {
            name: entry.name.clone(),
            shown: format::visible(&entry.name).into_owned(),
            href: ctx.url(&format!("/periodic/{}", segment(&entry.name))),
            paused: entry.is_paused(),
            group: group.map(|group| format::visible(group).into_owned()),
            source: format::visible(&text::source(group)).into_owned(),
            error: entry
                .error
                .as_deref()
                .map(|error| format::visible(error).into_owned()),
            late: is_late(entry, now),
            timetable: TimetableView::new(entry),
            job: JobView::new(ctx, entry),
            next: entry
                .next
                .filter(|_| !entry.is_paused())
                .map(|next| ZonedTime::new(next, now, entry.timetable.as_ref())),
            last: entry.last.as_ref().map(|last| LastView {
                at: TimeView::new(last.at, now),
                result: text::result(last.result),
                warning: text::result_is_warning(last.result),
                late: text::late(last.late()),
            }),
            run: RunView::new(entry, now),
            delete_confirm: delete_confirm(entry, fleet),
        }
    }
}

/// The confirmation of deleting `entry`, when it may be deleted.
fn delete_confirm(entry: &PeriodicEntry, fleet: &Fleet) -> Option<&'static str> {
    if !fleet.deletable(entry) {
        return None;
    }
    Some(match entry.source {
        PeriodicSource::Code { .. } => PERIODIC.confirm_delete_code,
        _ => PERIODIC.confirm_delete,
    })
}

/// `periodic.html`: the schedules.
#[derive(Template)]
#[template(path = "periodic.html")]
struct ListPage {
    layout: Layout,
    s: &'static Strings,
    t: &'static PeriodicStrings,
    total: String,
    summary: String,
    banners: Vec<String>,
    list_url: String,
    filter_value: String,
    filter_summary: Option<String>,
    /// What the page says when it has no rows.
    empty_text: &'static str,
    rows: Vec<ScheduleRow>,
    pager: Option<Pager>,
}

/// `GET /periodic`.
async fn list(ctx: Ctx) -> Response {
    let result = list_page(&ctx).await;
    super::finish(&ctx, result)
}

async fn list_page(ctx: &Ctx) -> WebResult<Response> {
    let query = Query::parse(&ctx.query);
    let filter = query.filter()?;
    let api = ctx.web.api();
    let set = api.periodic();
    let stats = api.stats().await?;
    let summary = set.summary(LATE_AFTER).await?;
    let fleet = Fleet::read(api).await?;
    let list_url = ctx.url("/periodic");

    let (found, pager, filter_summary, empty_text) = match &filter {
        Some(filter) => {
            let (found, outcome) = search_schedules(&set, &Pattern::new(filter)).await?;
            let summary = text::filter_summary(found.len(), outcome, filter);
            (found, None, Some(summary), text::no_matches(outcome))
        }
        None => {
            let count = query.count(ctx.web.page_size);
            let mut page = set.page(query.page(), count).await?;
            // Past the end (its last schedules were just deleted): the last page instead.
            if let Some(last) = last_page(&page) {
                page = set.page(last, count).await?;
            }
            let pager = Pager::new(ctx, &list_url, page.number, page.size, page.total);
            (page.items, pager, None, PERIODIC.empty)
        }
    };
    let mut banners = Vec::new();
    if summary.active > 0 && !fleet.polls() {
        banners.push(text::NO_POLLER.to_owned());
    } else if summary.late > 0 {
        banners.push(text::late_banner(summary.late));
    }
    for (group, count) in fleet.disagreements() {
        banners.push(text::crontabs_differ(&format::visible(group), count));
    }
    let title = strings::counted(PERIODIC.heading, summary.total);
    let page = ListPage {
        layout: super::layout(ctx, Some(&title), Some(Totals::new(&stats)), true),
        s: &EN,
        t: &PERIODIC,
        total: format::delimit(summary.total),
        summary: text::summary(summary.total, summary.paused()),
        banners,
        rows: found
            .iter()
            .map(|entry| ScheduleRow::new(ctx, entry, &fleet))
            .collect(),
        filter_value: filter.unwrap_or_default(),
        list_url,
        filter_summary,
        empty_text,
        pager,
    };
    Ok(super::render(ctx, StatusCode::OK, &page))
}

/// The text a filter is matched against: the schedule's name, its job's class and its cron
/// expression, in that order (a `*` can span them: `nightly*Report`).
fn haystack(entry: &PeriodicEntry) -> String {
    let class = entry.job.as_ref().map_or("", |job| job.class.as_str());
    let expression = match &entry.timetable {
        Some(Timetable::Cron { expression, .. }) => expression.as_str(),
        _ => "",
    };
    format!("{}\n{class}\n{expression}", entry.name)
}

/// The schedules matching `pattern`, by name, read a page at a time: at most
/// [`search::SCAN_LIMIT`] schedules are read and [`search::MAX_MATCHES`] kept.
async fn search_schedules(
    set: &PeriodicSet,
    pattern: &Pattern,
) -> WebResult<(Vec<PeriodicEntry>, Outcome)> {
    let mut found = Vec::new();
    let mut number = 1;
    loop {
        let page = set.page(number, SEARCH_PAGE).await?;
        let total = page.total;
        // Names this page covered (those whose hash is gone are skipped).
        let read = u64::try_from(number.saturating_mul(SEARCH_PAGE))
            .unwrap_or(u64::MAX)
            .min(total);
        for entry in page.items {
            if pattern.matches(&haystack(&entry)) {
                found.push(entry);
                if found.len() >= search::MAX_MATCHES {
                    let outcome = if read < total {
                        Outcome::MatchLimit
                    } else {
                        Outcome::Complete
                    };
                    return Ok((found, outcome));
                }
            }
        }
        if read >= total {
            return Ok((found, Outcome::Complete));
        }
        if read >= search::SCAN_LIMIT {
            return Ok((
                found,
                Outcome::ScanLimit {
                    searched: read,
                    total,
                },
            ));
        }
        number += 1;
    }
}

/// What the forms ask for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Action {
    Enqueue,
    Pause,
    Unpause,
    Delete,
}

impl Action {
    fn of(form: &PostForm) -> WebResult<Self> {
        match form.get("action") {
            Some("enqueue") => Ok(Action::Enqueue),
            Some("pause") => Ok(Action::Pause),
            Some("unpause") => Ok(Action::Unpause),
            Some("delete") => Ok(Action::Delete),
            _ => Err(WebError::BadRequest(errors::NO_ACTION.to_owned())),
        }
    }

    fn kind(self) -> NoticeKind {
        match self {
            Action::Enqueue => NoticeKind::PeriodicEnqueued,
            Action::Pause => NoticeKind::PeriodicPaused,
            Action::Unpause => NoticeKind::PeriodicResumed,
            Action::Delete => NoticeKind::PeriodicDeleted,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Action::Enqueue => "enqueue",
            Action::Pause => "pause",
            Action::Unpause => "unpause",
            Action::Delete => "delete",
        }
    }
}

/// What an action did to one schedule.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Applied {
    /// The schedule is as asked: its job enqueued (the jid), paused, resumed, or deleted.
    Done(Option<String>),
    /// "Enqueue now" pushed nothing: client middleware stopped the job, or it is unique and its
    /// lock is held.
    NotPushed,
    /// Not deleted: running processes define it in their code.
    InCode,
}

/// Applies `action` to `entry`. A refusal of the API (an unreadable definition, a schedule
/// that changed meanwhile) is an error.
async fn apply(
    set: &PeriodicSet,
    fleet: &Fleet,
    action: Action,
    entry: &PeriodicEntry,
) -> hench::Result<Applied> {
    let name = entry.name.as_str();
    Ok(match action {
        Action::Enqueue => match set.enqueue(name).await? {
            Some(jid) => Applied::Done(Some(jid)),
            None => Applied::NotPushed,
        },
        Action::Pause => {
            set.pause(name).await?;
            Applied::Done(None)
        }
        Action::Unpause => {
            set.unpause(name).await?;
            Applied::Done(None)
        }
        Action::Delete if !fleet.deletable(entry) => Applied::InCode,
        Action::Delete => {
            match entry.source {
                PeriodicSource::Code { .. } => set.remove_any(name).await?,
                _ => set.remove(name).await?,
            };
            Applied::Done(None)
        }
    })
}

/// An API error as the dashboard shows it: a schedule the API refused to change is a bad
/// request with the API's reason; anything else is a Redis failure.
fn refused(error: hench::Error) -> WebError {
    match error {
        hench::Error::InvalidSchedule(message) => WebError::BadRequest(message),
        other => WebError::from(other),
    }
}

/// `POST /periodic`: applies `action` (`enqueue`, `pause`, `unpause`, `delete`) to the selected
/// schedules (`name` fields).
async fn act_on_selected(ctx: Ctx, form: PostForm) -> Response {
    let result = act_on_names(&ctx, &form).await;
    super::finish(&ctx, result)
}

async fn act_on_names(ctx: &Ctx, form: &PostForm) -> WebResult<Response> {
    let action = Action::of(form)?;
    let back = ctx.back_to("/periodic");
    let names: Vec<&str> = form.all("name").take(MAX_NAMES + 1).collect();
    if names.is_empty() {
        return Ok(ctx.redirect(&back, Some(Notice::new(NoticeKind::NothingSelected, 0, 0))));
    }
    if names.len() > MAX_NAMES {
        return Err(WebError::BadRequest(text::too_many(MAX_NAMES)));
    }
    let api = ctx.web.api();
    let set = api.periodic();
    let fleet = if action == Action::Delete {
        Fleet::read(api).await?
    } else {
        Fleet::default()
    };
    let (mut done, mut failed) = (0_u64, 0_u64);
    for name in names {
        let entry = match set.get(name).await {
            Ok(Some(entry)) => entry,
            // Deleted meanwhile: nothing left to delete; nothing to act on otherwise.
            Ok(None) if action == Action::Delete => {
                done += 1;
                continue;
            }
            Ok(None) => {
                tracing::info!(
                    schedule = name,
                    action = action.name(),
                    "dashboard: no such periodic job"
                );
                failed += 1;
                continue;
            }
            Err(error) => {
                tracing::warn!(schedule = name, action = action.name(), %error, "dashboard could not change a periodic job");
                failed += 1;
                continue;
            }
        };
        match apply(&set, &fleet, action, &entry).await {
            Ok(Applied::Done(_)) => done += 1,
            Ok(Applied::NotPushed) => {
                tracing::info!(
                    schedule = name,
                    "dashboard: enqueue now pushed nothing (client middleware stopped the job, or its unique lock is held)"
                );
                failed += 1;
            }
            Ok(Applied::InCode) => {
                tracing::info!(
                    schedule = name,
                    "dashboard: not deleting a periodic job running processes define in their code"
                );
                failed += 1;
            }
            Err(error) => {
                tracing::warn!(schedule = name, action = action.name(), %error, "dashboard could not change a periodic job");
                failed += 1;
            }
        }
    }
    tracing::info!(
        action = action.name(),
        done,
        failed,
        "dashboard changed periodic jobs"
    );
    Ok(ctx.redirect(&back, Some(Notice::new(action.kind(), done, failed))))
}

/// `POST /periodic/{name}`: applies `action` to one schedule; back to its page when the form
/// came from there (`from=entry`), else to the list (and to the list after a deletion).
async fn act_on_one(
    ctx: Ctx,
    Path(ScheduleName { name }): Path<ScheduleName>,
    form: PostForm,
) -> Response {
    let result = act_on_name(&ctx, &name, &form).await;
    super::finish(&ctx, result)
}

async fn act_on_name(ctx: &Ctx, name: &str, form: &PostForm) -> WebResult<Response> {
    let action = Action::of(form)?;
    let api = ctx.web.api();
    let set = api.periodic();
    let Some(entry) = set.get(name).await? else {
        return Err(WebError::NotFound(text::NOT_FOUND.to_owned()));
    };
    let fleet = if action == Action::Delete {
        Fleet::read(api).await?
    } else {
        Fleet::default()
    };
    let applied = apply(&set, &fleet, action, &entry).await.map_err(refused)?;
    let notice = match &applied {
        Applied::Done(jid) => {
            tracing::info!(
                schedule = name,
                action = action.name(),
                jid,
                "dashboard changed a periodic job"
            );
            Notice::about(action.kind(), name)
        }
        Applied::NotPushed => {
            tracing::info!(
                schedule = name,
                "dashboard: enqueue now pushed nothing (client middleware stopped the job, or its unique lock is held)"
            );
            Notice {
                failed: 1,
                ..Notice::about(action.kind(), name)
            }
        }
        Applied::InCode => {
            return Err(WebError::BadRequest(PERIODIC.delete_help_code.to_owned()));
        }
    };
    let back = if action == Action::Delete {
        ctx.back_to("/periodic")
    } else if form.get("from") == Some("entry") {
        ctx.url(&format!("/periodic/{}", segment(name)))
    } else {
        ctx.back_to("/periodic")
    };
    Ok(ctx.redirect(&back, Some(notice)))
}

/// One schedule, ready to print.
struct ScheduleView {
    shown: String,
    paused: bool,
    group: Option<String>,
    source: String,
    description: Option<String>,
    timetable: TimetableView,
    catch_up: String,
    job: Option<JobView>,
    created: Option<TimeView>,
    updated: Option<TimeView>,
    digest: String,
    late: bool,
    next: Option<ZonedTime>,
    paused_at: Option<TimeView>,
    last: Option<LastDetail>,
    manual: Option<ManualView>,
    run: Option<RunView>,
    counters: String,
    error: Option<ErrorView>,
}

/// The last tick handled, for a schedule's page.
struct LastDetail {
    tick: ZonedTime,
    at: TimeView,
    result: &'static str,
    warning: bool,
    late: Option<String>,
    jid: String,
    by: String,
}

/// The last "enqueue now".
struct ManualView {
    at: TimeView,
    jid: String,
}

/// The error that parked a schedule (or why its definition cannot be read).
struct ErrorView {
    text: String,
    at: Option<TimeView>,
}

/// A next tick, with what changes on the clocks that day.
struct UpcomingView {
    at: ZonedTime,
    note: Option<String>,
}

/// One history entry, ready to print.
struct HistoryRow {
    tick: Option<ZonedTime>,
    at: TimeView,
    late: String,
    result: &'static str,
    warning: bool,
    jid: String,
    by: String,
    missed: String,
}

impl HistoryRow {
    fn new(entry: &HistoryEntry, timetable: Option<&Timetable>, now: SystemTime) -> Self {
        Self {
            tick: entry.tick.map(|tick| ZonedTime::new(tick, now, timetable)),
            at: TimeView::new(entry.at, now),
            late: entry.late.and_then(text::late).unwrap_or_default(),
            result: text::result(entry.result),
            warning: text::result_is_warning(entry.result),
            jid: entry
                .jid
                .as_deref()
                .map(|jid| format::visible(jid).into_owned())
                .unwrap_or_default(),
            by: entry
                .by
                .as_deref()
                .map(|by| format::visible(by).into_owned())
                .unwrap_or_default(),
            missed: if entry.missed > 0 {
                format::delimit(entry.missed)
            } else {
                String::new()
            },
        }
    }
}

impl ScheduleView {
    fn new(ctx: &Ctx, entry: &PeriodicEntry) -> Self {
        let now = ctx.now;
        let timetable = entry.timetable.as_ref();
        let time = |at| TimeView::new(at, now);
        let group = match &entry.source {
            PeriodicSource::Code { group } => Some(group.as_str()),
            _ => None,
        };
        Self {
            shown: format::visible(&entry.name).into_owned(),
            paused: entry.is_paused(),
            group: group.map(|group| format::visible(group).into_owned()),
            source: format::visible(&text::source(group)).into_owned(),
            description: entry
                .description
                .as_deref()
                .filter(|description| !description.is_empty())
                .map(visible_lines),
            timetable: TimetableView::new(entry),
            catch_up: text::catch_up(entry.catch_up, entry.deadline),
            job: JobView::new(ctx, entry),
            created: entry.created_at.map(time),
            updated: entry.updated_at.map(time),
            digest: format::visible(&entry.digest).into_owned(),
            late: is_late(entry, now),
            next: entry.next.map(|next| ZonedTime::new(next, now, timetable)),
            paused_at: entry.paused_at.map(time),
            last: entry.last.as_ref().map(|last| LastDetail {
                tick: ZonedTime::new(last.tick, now, timetable),
                at: time(last.at),
                result: text::result(last.result),
                warning: text::result_is_warning(last.result),
                late: text::late(last.late()),
                jid: last
                    .jid
                    .as_deref()
                    .map(|jid| format::visible(jid).into_owned())
                    .unwrap_or_default(),
                by: last
                    .by
                    .as_deref()
                    .map(|by| format::visible(by).into_owned())
                    .unwrap_or_default(),
            }),
            manual: entry.manual_at.map(|at| ManualView {
                at: time(at),
                jid: entry
                    .manual_jid
                    .as_deref()
                    .map(|jid| format::visible(jid).into_owned())
                    .unwrap_or_default(),
            }),
            run: RunView::new(entry, now),
            counters: text::counters(entry.fired, entry.missed, entry.skipped, entry.run_failures),
            error: entry.error.as_deref().map(|error| ErrorView {
                text: visible_lines(error),
                at: entry.error_at.map(time),
            }),
        }
    }
}

/// `periodic_entry.html`: one schedule.
#[derive(Template)]
#[template(path = "periodic_entry.html")]
struct SchedulePage {
    layout: Layout,
    s: &'static Strings,
    t: &'static PeriodicStrings,
    heading: String,
    list_href: String,
    action_href: String,
    banners: Vec<String>,
    view: ScheduleView,
    upcoming: Vec<UpcomingView>,
    /// What the next ticks say when there are none.
    upcoming_empty: String,
    /// The job template, as pretty JSON.
    template: String,
    history: Vec<HistoryRow>,
    /// The confirmation of its Delete button, offered when it may be deleted.
    delete_confirm: Option<&'static str>,
    /// The schedule's hash as stored, as JSON.
    raw: String,
}

/// `GET /periodic/{name}`.
async fn schedule(ctx: Ctx, Path(ScheduleName { name }): Path<ScheduleName>) -> Response {
    let result = schedule_page(&ctx, &name).await;
    super::finish(&ctx, result)
}

async fn schedule_page(ctx: &Ctx, name: &str) -> WebResult<Response> {
    let api = ctx.web.api();
    let set = api.periodic();
    let Some(entry) = set.get(name).await? else {
        return Err(WebError::NotFound(text::NOT_FOUND.to_owned()));
    };
    let history = set.history(name, HISTORY).await?;
    let fleet = Fleet::read(api).await?;
    let stats = api.stats().await?;
    let now = ctx.now;
    let timetable = entry.timetable.as_ref();
    let (upcoming, upcoming_empty) = match entry.upcoming(UPCOMING) {
        Ok(ticks) if ticks.is_empty() && entry.is_paused() => {
            (Vec::new(), PERIODIC.no_upcoming_paused.to_owned())
        }
        Ok(ticks) => (
            ticks
                .into_iter()
                .map(|at| UpcomingView {
                    at: ZonedTime::new(at, now, timetable),
                    note: timetable.and_then(|timetable| timetable.transition_note(at)),
                })
                .collect(),
            EN.unknown.to_owned(),
        ),
        Err(error) => (Vec::new(), format::visible(&error.to_string()).into_owned()),
    };
    let mut banners = Vec::new();
    if !entry.is_paused() && !fleet.polls() {
        banners.push(text::NO_POLLER.to_owned());
    }
    let template = entry
        .fields
        .get("job")
        .map(|job| {
            serde_json::from_str::<Value>(job)
                .and_then(|value| serde_json::to_string_pretty(&value))
                .unwrap_or_else(|_| job.clone())
        })
        .unwrap_or_default();
    let raw = serde_json::to_string_pretty(&entry.fields).unwrap_or_default();
    let shown = format::visible(&entry.name).into_owned();
    let heading = text::heading(&shown);
    let page = SchedulePage {
        // A detail page: no "Live" refresh.
        layout: super::layout(ctx, Some(&heading), Some(Totals::new(&stats)), false),
        s: &EN,
        t: &PERIODIC,
        heading,
        list_href: ctx.url("/periodic"),
        action_href: ctx.url(&format!("/periodic/{}", segment(&entry.name))),
        banners,
        upcoming,
        upcoming_empty,
        template,
        history: history
            .iter()
            .map(|item| HistoryRow::new(item, timetable, now))
            .collect(),
        delete_confirm: delete_confirm(&entry, &fleet),
        raw,
        view: ScheduleView::new(ctx, &entry),
    };
    Ok(super::render(ctx, StatusCode::OK, &page))
}

#[cfg(test)]
mod tests {
    //! The Periodic pages (`docs/design/periodic.md` §2.11, §3.4 item 32) on a private Redis,
    //! served by their routes composed like the dashboard composes them (the state, then the
    //! guard). Code-defined schedules are dynamic ones marked as registered by a group (what a
    //! worker's registration writes).

    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use axum::body::Body;
    use hench::api::Api;
    use hench::periodic::{CatchUp, ManualClock, Periodic};
    use hench::test_util::TestRedis;
    use hench::{keys, redis_rs};
    use http::header::{CONTENT_TYPE, COOKIE, LOCATION, SET_COOKIE};
    use http::{HeaderMap, Request};
    use http_body_util::BodyExt;
    use scraper::{Html, Selector};
    use serde_json::{Value, json};
    use tower::ServiceExt;

    use super::*;

    /// The notice an action left (not the banners).
    const NOTICE: &str = "div.notice:not(.periodic-banner)";

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
                .map(|element| {
                    element
                        .text()
                        .collect::<String>()
                        .split_whitespace()
                        .collect::<Vec<_>>()
                        .join(" ")
                })
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

        /// `GET` the page a redirect leads to, showing the notice it left.
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
            self.send(form(path, fields, TOKEN)).await
        }

        async fn redis<T: redis_rs::FromRedisValue>(&self, command: &redis_rs::Cmd) -> T {
            let mut conn = self.api.redis().conn().await.unwrap();
            command.query_async(&mut conn).await.unwrap()
        }

        async fn upsert(&self, periodic: Periodic) {
            self.api.periodic().upsert(&periodic).await.unwrap();
        }

        /// Defines `periodic` as of two hours ago: its next tick passed long ago, as when no
        /// poller runs.
        async fn upsert_overdue(&self, periodic: Periodic) {
            let clock = ManualClock::new(SystemTime::now() - Duration::from_secs(7_200));
            self.api
                .periodic()
                .with_clock(clock)
                .upsert(&periodic)
                .await
                .unwrap();
        }

        /// Marks schedule `name` as defined in the code of `group`'s processes.
        async fn as_code(&self, name: &str, group: &str) {
            let () = self
                .redis(
                    redis_rs::cmd("HSET")
                        .arg(keys::periodic(name))
                        .arg("source")
                        .arg("code")
                        .arg("group")
                        .arg(group),
                )
                .await;
            let () = self
                .redis(
                    redis_rs::cmd("SADD")
                        .arg(keys::periodic_group(group))
                        .arg(name),
                )
                .await;
        }

        /// A live process whose heartbeat says `info`.
        async fn process(&self, identity: &str, info: Value) {
            let () = self
                .redis(redis_rs::cmd("SADD").arg(keys::PROCESSES).arg(identity))
                .await;
            let () = self
                .redis(
                    redis_rs::cmd("HSET")
                        .arg(keys::process(identity))
                        .arg("info")
                        .arg(info.to_string())
                        .arg("beat")
                        .arg(now_secs())
                        .arg("busy")
                        .arg(0)
                        .arg("quiet")
                        .arg("false"),
                )
                .await;
        }

        /// A live process polling the schedules and registering `group` with `digest`.
        async fn poller(&self, identity: &str, group: &str, digest: &str) {
            let info = json!({
                "hostname": "host",
                "features": {"periodic": 1},
                "periodic": {"poller": true, "group": group, "digest": digest, "schedules": 1},
            });
            self.process(identity, info).await;
        }

        async fn queue_size(&self, queue: &str) -> u64 {
            self.api.queue(queue).size().await.unwrap()
        }
    }

    fn now_secs() -> f64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64()
    }

    fn form(path: &str, fields: &[(&str, &str)], token: &str) -> Request<Body> {
        let mut body = form_urlencoded::Serializer::new(String::new());
        body.append_pair("_csrf", token);
        for (key, value) in fields {
            body.append_pair(key, value);
        }
        Request::post(path)
            .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header(COOKIE, format!("hench_csrf={TOKEN}"))
            .header("sec-fetch-site", "same-origin")
            .body(Body::from(body.finish()))
            .unwrap()
    }

    fn nightly() -> Periodic {
        Periodic::cron("nightly", "0 2 * * *")
            .tz("Europe/Paris")
            .class("Report", vec![json!(42), json!("all")])
            .queue("reports")
            .description("The nightly report.\nSent to the team.")
    }

    fn sync() -> Periodic {
        Periodic::every("sync", Duration::from_secs(30)).class("Sync", vec![])
    }

    fn cleanup() -> Periodic {
        Periodic::cron("cleanup", "*/5 * * * *")
            .class("Cleanup", vec![json!({"older_than": 30})])
            .paused(true)
    }

    /// The three schedules above, and a poller in group `web`.
    async fn seeded() -> App {
        let app = App::new().await;
        for periodic in [nightly(), sync(), cleanup()] {
            app.upsert(periodic).await;
        }
        app.poller("host:1:a", "web", "d1").await;
        app
    }

    #[tokio::test]
    async fn the_list_shows_the_schedules() {
        let app = App::new().await;
        let res = app.get("/periodic").await;
        res.assert_status(StatusCode::OK);
        assert_eq!(res.texts(".empty"), [PERIODIC.empty]);
        assert_eq!(
            res.texts(".periodic-summary"),
            ["No periodic job is defined."]
        );
        assert_eq!(res.count(".periodic-banner"), 0, "nothing to poll");

        let app = seeded().await;
        let res = app.get("/periodic").await;
        res.assert_status(StatusCode::OK);
        assert!(res.headers.contains_key("content-security-policy"));
        assert_eq!(res.texts("title"), ["Periodic jobs (3) · Hench"]);
        assert_eq!(res.texts(".count-badge"), ["3"]);
        assert_eq!(
            res.texts(".periodic-summary"),
            ["3 periodic jobs, 1 paused."]
        );
        assert_eq!(
            res.count(".periodic-banner"),
            0,
            "{:?}",
            res.texts(".periodic-banner")
        );
        assert_eq!(res.count("#live-toggle"), 1, "the Live switch");
        // By name.
        assert_eq!(res.texts(".periodic-name"), ["cleanup", "nightly", "sync"]);
        assert_eq!(
            res.attrs(".periodic-name", "href"),
            ["/periodic/cleanup", "/periodic/nightly", "/periodic/sync"]
        );
        assert_eq!(res.texts(".periodic-paused"), ["paused"]);
        assert_eq!(res.texts(".periodic-source"), ["api", "api", "api"]);
        assert_eq!(
            res.texts(".periodic-expression"),
            ["*/5 * * * *", "0 2 * * *"]
        );
        let described = res.texts(".periodic-describe");
        assert!(
            described[1].contains("02:00") && described[1].contains("Europe/Paris"),
            "{described:?}"
        );
        assert_eq!(described[2], "Every 30 seconds");
        assert_eq!(res.texts(".job-class"), ["Cleanup", "Report", "Sync"]);
        assert_eq!(
            res.texts(".periodic-queue"),
            ["default", "reports", "default"]
        );
        assert_eq!(res.attrs(".periodic-queue", "href")[1], "/queues/reports");
        assert_eq!(
            res.texts(".periodic-args"),
            [r#"{"older_than":30}"#, r#"42, "all""#]
        );
        // Next runs: none while paused; the time in the schedule's zone.
        let next = res.texts(".periodic-next");
        assert_eq!(next[0], "paused");
        assert!(
            next[1].contains("02:00:00 +01:00") || next[1].contains("02:00:00 +02:00"),
            "{next:?}"
        );
        assert!(next[2].ends_with("+00:00"), "{next:?}");
        assert_eq!(res.texts(".periodic-last"), ["never", "never", "never"]);
        assert_eq!(res.texts(".periodic-run"), ["never", "never", "never"]);
        // Forms: the selection and the row actions.
        assert_eq!(
            res.attrs("input[name=name]", "value"),
            ["cleanup", "nightly", "sync"]
        );
        assert_eq!(
            res.attrs("button[formaction='/periodic/cleanup']", "value"),
            ["enqueue", "unpause", "delete"]
        );
        assert_eq!(
            res.attrs("button[formaction='/periodic/nightly']", "value"),
            ["enqueue", "pause", "delete"]
        );
        assert_eq!(res.count(".bulk-actions button[name=action]"), 4);
    }

    #[tokio::test]
    async fn the_list_has_pages_and_filters() {
        let app = App::new().await;
        for index in 0..30 {
            app.upsert(
                Periodic::cron(format!("job-{index:02}"), "0 * * * *")
                    .class(if index % 2 == 0 { "Even" } else { "Odd" }, vec![]),
            )
            .await;
        }
        app.upsert(Periodic::cron("weekly", "0 3 * * 1").class("Digest", vec![]))
            .await;
        let res = app.get("/periodic").await;
        assert_eq!(res.count(".periodic-table tbody tr"), 25);
        assert_eq!(res.count(".pager"), 2);
        let res = app.get("/periodic?page=2").await;
        assert_eq!(res.count(".periodic-table tbody tr"), 6);
        // Past the end: the last page.
        let res = app.get("/periodic?page=9").await;
        assert_eq!(res.count(".periodic-table tbody tr"), 6);
        // Filters: names, classes and expressions, with `*`.
        let res = app.get("/periodic?substr=job-1").await;
        assert_eq!(res.count(".periodic-table tbody tr"), 10);
        assert_eq!(
            res.texts(".filter-summary"),
            ["10 periodic jobs matching “job-1”."]
        );
        assert_eq!(res.count(".pager"), 0);
        let res = app.get("/periodic?substr=Digest").await;
        assert_eq!(res.texts(".periodic-name"), ["weekly"]);
        let res = app.get("/periodic?substr=3%20*%20*%201").await;
        assert_eq!(res.texts(".periodic-name"), ["weekly"]);
        let res = app.get("/periodic?substr=job-0*Odd").await;
        assert_eq!(
            res.texts(".periodic-name"),
            ["job-01", "job-03", "job-05", "job-07", "job-09"]
        );
        let res = app.get("/periodic?substr=nothing").await;
        assert_eq!(res.texts(".empty"), [PERIODIC.no_matches]);
        assert_eq!(res.attrs("#filter-input", "value"), ["nothing"]);
    }

    #[tokio::test]
    async fn banners_say_what_keeps_schedules_from_firing() {
        let app = App::new().await;
        app.upsert(nightly()).await;
        // No process: nothing polls.
        let res = app.get("/periodic").await;
        assert_eq!(res.texts(".periodic-banner"), [text::NO_POLLER]);
        let page = app.get("/periodic/nightly").await;
        assert_eq!(page.texts(".periodic-banner"), [text::NO_POLLER]);
        // A process of a version that does not describe its poller: assumed to poll.
        app.process("old:1:a", json!({"features": {"periodic": 1}}))
            .await;
        assert_eq!(app.get("/periodic").await.count(".periodic-banner"), 0);
        // Processes that do not understand periodic jobs, or run no poller: nothing polls.
        let () = app
            .redis(redis_rs::cmd("SREM").arg(keys::PROCESSES).arg("old:1:a"))
            .await;
        app.process("plain:1:a", json!({"features": {}})).await;
        app.process(
            "web:1:a",
            json!({"features": {"periodic": 1}, "periodic": {"poller": false, "group": null}}),
        )
        .await;
        let res = app.get("/periodic").await;
        assert_eq!(res.texts(".periodic-banner"), [text::NO_POLLER]);
        // A poller: no banner, until a schedule is overdue.
        app.poller("web:2:a", "web", "d1").await;
        assert_eq!(app.get("/periodic").await.count(".periodic-banner"), 0);
        app.upsert_overdue(Periodic::cron("overdue", "*/10 * * * *").class("Late", vec![]))
            .await;
        let res = app.get("/periodic").await;
        assert_eq!(res.texts(".periodic-banner"), [text::late_banner(1)]);
        assert_eq!(res.texts(".periodic-late"), ["overdue"]);
        let page = app.get("/periodic/overdue").await;
        assert_eq!(page.texts(".periodic-late"), ["overdue"]);
        // Processes of one group defining different crontabs.
        app.poller("web:3:a", "web", "d2").await;
        let res = app.get("/periodic").await;
        assert_eq!(
            res.texts(".periodic-banner"),
            [text::late_banner(1), text::crontabs_differ("web", 2)]
        );
    }

    #[tokio::test]
    async fn the_page_of_a_schedule_shows_its_definition_state_and_history() {
        let app = seeded().await;
        let res = app.get("/periodic/nightly").await;
        res.assert_status(StatusCode::OK);
        assert_eq!(res.texts("h1"), ["Periodic job “nightly”"]);
        assert_eq!(res.count("#live-toggle"), 0, "a detail page");
        let definition = res.texts(".periodic-definition td").join("\n");
        for expected in [
            "nightly",
            "the API, the dashboard or the command line",
            "0 2 * * *",
            "Europe/Paris",
            "the latest one is enqueued",
            "Report",
            "reports",
        ] {
            assert!(definition.contains(expected), "{expected}: {definition}");
        }
        assert_eq!(
            res.texts(".periodic-description"),
            ["The nightly report. Sent to the team."]
        );
        // The next five ticks, in the schedule's zone.
        let upcoming = res.texts(".periodic-upcoming li");
        assert_eq!(upcoming.len(), 5, "{upcoming:?}");
        assert!(
            upcoming.iter().all(|tick| tick.contains(" 02:00:00 +0")),
            "{upcoming:?}"
        );
        // The template, pretty.
        let template: Value = serde_json::from_str(&res.texts(".periodic-template")[0]).unwrap();
        assert_eq!(template["class"], "Report");
        assert_eq!(template["args"], json!([42, "all"]));
        assert_eq!(res.texts(".periodic-last"), ["never"]);
        assert_eq!(
            res.texts(".periodic-counters"),
            ["0 enqueued, 0 missed, 0 skipped, 0 failed runs"]
        );
        assert_eq!(
            res.texts(".periodic-history + .empty, .empty"),
            [PERIODIC.no_history]
        );
        // Enqueue now: the history and the state say so.
        let jid = app
            .api
            .periodic()
            .enqueue("nightly")
            .await
            .unwrap()
            .unwrap();
        let res = app.get("/periodic/nightly").await;
        let manual = res.texts(".periodic-manual").concat();
        assert!(manual.contains(&jid), "{manual}");
        assert_eq!(
            res.texts(".periodic-history tbody .periodic-result"),
            ["enqueued by hand"]
        );
        assert_eq!(res.texts(".periodic-history tbody td.mono"), [jid.as_str()]);
        // A run recorded by a worker.
        let () = app
            .redis(
                redis_rs::cmd("HSET")
                    .arg(keys::periodic("nightly"))
                    .arg("run_tick")
                    .arg(1_000)
                    .arg("run_at")
                    .arg(2_000)
                    .arg("run_status")
                    .arg("failed")
                    .arg("run_ms")
                    .arg(1_500)
                    .arg("run_error")
                    .arg("Timeout: <slow>")
                    .arg("run_jid")
                    .arg(&jid),
            )
            .await;
        let res = app.get("/periodic/nightly").await;
        let run = res.texts(".periodic-run").concat();
        assert!(run.starts_with("failed"), "{run}");
        assert!(
            run.contains("1.5 s") && run.contains("Timeout: <slow>"),
            "{run}"
        );
        // The paused one: no next ticks.
        let res = app.get("/periodic/cleanup").await;
        assert_eq!(res.texts(".periodic-paused"), ["paused"]);
        assert!(res.body.contains(PERIODIC.no_upcoming_paused));
        assert_eq!(res.count("button[value=unpause]"), 1);
        // The raw hash.
        let raw: Value = serde_json::from_str(&res.texts("details pre.code")[0]).unwrap();
        assert_eq!(raw["source"], "api");
    }

    #[tokio::test]
    async fn next_ticks_note_daylight_saving_changes() {
        let app = App::new().await;
        let clock = ManualClock::new(UNIX_EPOCH + Duration::from_secs(1_806_062_400)); // 2027-03-26 12:00 UTC
        app.api
            .periodic()
            .with_clock(clock)
            .upsert(
                &Periodic::cron("london", "30 1 * * *")
                    .tz("Europe/London")
                    .class("X", vec![]),
            )
            .await
            .unwrap();
        app.poller("host:1:a", "web", "d1").await;
        let res = app.get("/periodic/london").await;
        let upcoming = res.texts(".periodic-upcoming li");
        assert_eq!(upcoming.len(), 5);
        assert!(
            upcoming[0].starts_with("2027-03-27 01:30:00 +00:00"),
            "{upcoming:?}"
        );
        assert!(
            upcoming[2].starts_with("2027-03-29 01:30:00 +01:00"),
            "{upcoming:?}"
        );
        let notes = res.texts(".periodic-dst");
        assert!(!notes.is_empty(), "{upcoming:?}");
        assert!(
            notes.iter().any(|note| note.contains("+01:00")),
            "{notes:?}"
        );
    }

    #[tokio::test]
    async fn schedules_are_enqueued_paused_resumed_and_deleted_one_at_a_time() {
        let app = seeded().await;
        // Enqueue now from the list: back to the list.
        let res = app
            .post("/periodic/nightly", &[("action", "enqueue")])
            .await;
        res.assert_status(StatusCode::SEE_OTHER);
        assert_eq!(res.location(), "/periodic");
        assert_eq!(
            app.follow(&res).await.texts(NOTICE),
            ["Enqueued the job of schedule “nightly” now."]
        );
        assert_eq!(app.queue_size("reports").await, 1);
        let job = app
            .api
            .queue("reports")
            .page(1, 1, hench::api::SortOrder::Ascending)
            .await
            .unwrap()
            .items
            .remove(0);
        let tick = job.periodic().unwrap();
        assert_eq!(tick.name, "nightly");
        assert!(tick.manual);
        // Pause from the schedule's page: back to the page.
        let res = app
            .post(
                "/periodic/nightly",
                &[("action", "pause"), ("from", "entry")],
            )
            .await;
        assert_eq!(res.location(), "/periodic/nightly");
        let shown = app.follow(&res).await;
        assert_eq!(shown.texts(NOTICE), ["Paused schedule “nightly”."]);
        assert_eq!(shown.texts(".periodic-paused"), ["paused"]);
        assert!(
            app.api
                .periodic()
                .get("nightly")
                .await
                .unwrap()
                .unwrap()
                .is_paused()
        );
        let res = app
            .post(
                "/periodic/nightly",
                &[("action", "unpause"), ("from", "entry")],
            )
            .await;
        assert_eq!(
            app.follow(&res).await.texts(NOTICE),
            ["Resumed schedule “nightly”."]
        );
        assert!(
            !app.api
                .periodic()
                .get("nightly")
                .await
                .unwrap()
                .unwrap()
                .is_paused()
        );
        // Delete: back to the list.
        let res = app
            .post(
                "/periodic/nightly",
                &[("action", "delete"), ("from", "entry")],
            )
            .await;
        assert_eq!(res.location(), "/periodic");
        assert_eq!(
            app.follow(&res).await.texts(NOTICE),
            ["Deleted schedule “nightly”."]
        );
        assert!(app.api.periodic().get("nightly").await.unwrap().is_none());
        // Unknown names, unknown actions.
        app.get("/periodic/nightly")
            .await
            .assert_status(StatusCode::NOT_FOUND);
        app.post("/periodic/nightly", &[("action", "pause")])
            .await
            .assert_status(StatusCode::NOT_FOUND);
        app.post("/periodic/sync", &[("action", "explode")])
            .await
            .assert_status(StatusCode::BAD_REQUEST);
        app.post("/periodic/sync", &[])
            .await
            .assert_status(StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn an_enqueue_that_pushes_nothing_says_so() {
        let app = App::new().await;
        app.upsert(
            Periodic::cron("unique", "0 * * * *")
                .class("Once", vec![])
                .unique_for(Duration::from_secs(3_600)),
        )
        .await;
        let res = app.post("/periodic/unique", &[("action", "enqueue")]).await;
        assert_eq!(
            app.follow(&res).await.texts(NOTICE),
            ["Enqueued the job of schedule “unique” now."]
        );
        // Its lock is held by the first job.
        let res = app.post("/periodic/unique", &[("action", "enqueue")]).await;
        let notice = app.follow(&res).await.texts(NOTICE).concat();
        assert!(
            notice.starts_with("Did not enqueue the job of schedule “unique”"),
            "{notice}"
        );
        assert_eq!(app.queue_size("default").await, 1);
    }

    #[tokio::test]
    async fn code_schedules_are_deleted_only_when_no_running_process_defines_them() {
        let app = seeded().await;
        app.as_code("nightly", "web").await;
        app.as_code("sync", "api-workers").await;
        let res = app.get("/periodic").await;
        assert_eq!(res.texts(".periodic-source"), ["api", "web", "api-workers"]);
        // `web` has a live process: no Delete for its schedule; `api-workers` has none.
        assert_eq!(
            res.attrs("button[formaction='/periodic/nightly']", "value"),
            ["enqueue", "pause"]
        );
        assert_eq!(
            res.attrs("button[formaction='/periodic/sync']", "value"),
            ["enqueue", "pause", "delete"]
        );
        let page = app.get("/periodic/nightly").await;
        assert_eq!(page.count("button[value=delete]"), 0);
        assert!(page.texts(".periodic-actions-help")[0].contains("Defined in the code"));
        let definition = page.texts(".periodic-definition td").join("\n");
        assert!(definition.contains("code (group “web”)"), "{definition}");
        app.post("/periodic/nightly", &[("action", "delete")])
            .await
            .assert_status(StatusCode::BAD_REQUEST);
        assert!(app.api.periodic().get("nightly").await.unwrap().is_some());
        // The orphaned group's schedule goes.
        let res = app.post("/periodic/sync", &[("action", "delete")]).await;
        assert_eq!(
            app.follow(&res).await.texts(NOTICE),
            ["Deleted schedule “sync”."]
        );
        assert!(app.api.periodic().get("sync").await.unwrap().is_none());
        // Selected: the dynamic one goes, the code one stays.
        let res = app
            .post(
                "/periodic",
                &[
                    ("action", "delete"),
                    ("name", "cleanup"),
                    ("name", "nightly"),
                ],
            )
            .await;
        let notice = app.follow(&res).await.texts(NOTICE).concat();
        assert!(notice.starts_with("Deleted 1 schedule."), "{notice}");
        assert!(notice.contains("1 job could not be changed"), "{notice}");
        assert!(app.api.periodic().get("cleanup").await.unwrap().is_none());
        assert!(app.api.periodic().get("nightly").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn selected_schedules_are_changed_at_once() {
        let app = seeded().await;
        let res = app
            .post(
                "/periodic",
                &[("action", "pause"), ("name", "nightly"), ("name", "sync")],
            )
            .await;
        res.assert_status(StatusCode::SEE_OTHER);
        assert_eq!(
            app.follow(&res).await.texts(NOTICE),
            ["Paused 2 schedules."]
        );
        let summary = app.api.periodic().summary(LATE_AFTER).await.unwrap();
        assert_eq!(summary.paused(), 3);
        let res = app
            .post(
                "/periodic",
                &[
                    ("action", "unpause"),
                    ("name", "nightly"),
                    ("name", "sync"),
                    ("name", "cleanup"),
                ],
            )
            .await;
        assert_eq!(
            app.follow(&res).await.texts(NOTICE),
            ["Resumed 3 schedules."]
        );
        assert_eq!(
            app.api
                .periodic()
                .summary(LATE_AFTER)
                .await
                .unwrap()
                .paused(),
            0
        );
        let res = app
            .post(
                "/periodic",
                &[
                    ("action", "enqueue"),
                    ("name", "nightly"),
                    ("name", "sync"),
                    ("name", "gone"),
                ],
            )
            .await;
        let notice = app.follow(&res).await.texts(NOTICE).concat();
        assert!(
            notice.starts_with("Enqueued the jobs of 2 schedules now."),
            "{notice}"
        );
        assert!(notice.contains("1 job could not be changed"), "{notice}");
        assert_eq!(app.queue_size("reports").await, 1);
        assert_eq!(app.queue_size("default").await, 1);
        // Nothing selected; too many.
        let res = app.post("/periodic", &[("action", "pause")]).await;
        assert_eq!(
            app.follow(&res).await.texts(NOTICE),
            ["Select at least one job first."]
        );
        let names: Vec<String> = (0..=MAX_NAMES).map(|index| format!("n{index}")).collect();
        let mut fields = vec![("action", "pause")];
        fields.extend(names.iter().map(|name| ("name", name.as_str())));
        app.post("/periodic", &fields)
            .await
            .assert_status(StatusCode::BAD_REQUEST);
        // Back to the list as the referring page showed it.
        let request = Request::post("/periodic")
            .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header(COOKIE, format!("hench_csrf={TOKEN}"))
            .header("sec-fetch-site", "same-origin")
            .header("referer", "/periodic?substr=sync")
            .body(Body::from(format!("_csrf={TOKEN}&action=pause&name=sync")))
            .unwrap();
        let res = app.send(request).await;
        assert_eq!(res.location(), "/periodic?substr=sync");
    }

    #[tokio::test]
    async fn read_only_dashboards_show_no_forms_and_refuse_changes() {
        let app = App::with(|web| web.read_only(true)).await;
        app.upsert(nightly()).await;
        let res = app.get("/periodic").await;
        res.assert_status(StatusCode::OK);
        assert_eq!(res.count("form[method=post]"), 0);
        assert_eq!(res.count("input[type=checkbox]"), 0);
        assert_eq!(res.count("button[name=action]"), 0);
        let page = app.get("/periodic/nightly").await;
        page.assert_status(StatusCode::OK);
        assert_eq!(page.count("form[method=post]"), 0);
        for path in ["/periodic", "/periodic/nightly"] {
            app.post(path, &[("action", "delete"), ("name", "nightly")])
                .await
                .assert_status(StatusCode::FORBIDDEN);
        }
        assert!(app.api.periodic().get("nightly").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn changes_need_the_dashboard_s_token() {
        let app = seeded().await;
        let request = form(
            "/periodic/nightly",
            &[("action", "delete")],
            "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        );
        app.send(request).await.assert_status(StatusCode::FORBIDDEN);
        assert!(app.api.periodic().get("nightly").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn hostile_names_are_escaped_spelled_out_and_reachable() {
        let app = App::new().await;
        // Names cannot hold control characters; descriptions can.
        let name = "<script>alert(1)</script> a/b: ü";
        app.upsert(
            Periodic::cron(name, "0 * * * *")
                .class("<b>Class</b>", vec![json!("<i>")])
                .description("<img src=x onerror=alert(2)>\u{1b}[31m"),
        )
        .await;
        let res = app.get("/periodic").await;
        res.assert_status(StatusCode::OK);
        assert!(!res.body.contains("<script>alert(1)"));
        assert!(!res.body.contains("<b>Class"));
        let href = res.attrs(".periodic-name", "href")[0].clone();
        assert_eq!(href, format!("/periodic/{}", segment(name)));
        assert!(href.contains("a%2Fb:%20"), "{href}");
        let page = app.get(&href).await;
        page.assert_status(StatusCode::OK);
        assert!(!page.body.contains("<script>alert(1)"));
        assert!(!page.body.contains("<img src=x"));
        assert!(page.body.contains(r"\u{1b}[31m"));
        assert_eq!(
            page.texts("h1"),
            [format!("Periodic job “{}”", format::visible(name))]
        );
        // Actions reach it too.
        let res = app
            .post(&href, &[("action", "pause"), ("from", "entry")])
            .await;
        assert_eq!(res.location(), href);
        assert!(
            app.api
                .periodic()
                .get(name)
                .await
                .unwrap()
                .unwrap()
                .is_paused()
        );
    }

    /// The context of a request for `path` to the dashboard `web`.
    fn ctx(web: HenchWeb, path: &str) -> Ctx {
        let (parts, ()) = Request::get(path).body(()).unwrap().into_parts();
        Ctx::from_parts(&parts, &Arc::new(web)).unwrap()
    }

    #[tokio::test]
    async fn job_pages_get_a_periodic_row() {
        let redis = TestRedis::start().await;
        let api = Api::new(redis.pool().await);
        let record = |periodic: Value| {
            let job = json!({"class": "Report", "args": [], "jid": "abc", "periodic": periodic});
            JobRecord::new(&api, job.to_string(), None)
        };
        let job = record(json!({"name": "a/b", "tick": 1_793_613_600_000_i64}));
        let rows = periodic_rows(&ctx(HenchWeb::new(&api), "/retries/x"), &job);
        assert_eq!(rows.shown, ["periodic"]);
        let [row] = rows.rows.as_slice() else {
            panic!("one row");
        };
        assert_eq!(row.label(), "Periodic");
        assert_eq!(row.value(), "a/b, tick 2026-11-02 10:00:00 UTC");
        assert_eq!(row.href(), Some("/periodic/a%2Fb"));
        let manual = record(json!({"name": "n", "tick": 1_793_613_600_000_i64, "manual": true}));
        assert_eq!(
            periodic_rows(&ctx(HenchWeb::new(&api), "/x"), &manual).rows[0].value(),
            "n, enqueued by hand at 2026-11-02 10:00:00 UTC"
        );
        // The tab off: no link.
        let rows = periodic_rows(&ctx(HenchWeb::new(&api).periodic(false), "/x"), &job);
        assert_eq!(rows.rows[0].href(), None);
        // Malformed: no row, the field stays among the extras.
        for periodic in [json!("nightly"), json!({"tick": 5}), json!(null)] {
            let rows = periodic_rows(&ctx(HenchWeb::new(&api), "/x"), &record(periodic));
            assert!(rows.rows.is_empty() && rows.shown.is_empty());
        }
        let rows = job_rows(&ctx(HenchWeb::new(&api), "/x"), &job);
        assert_eq!(rows.rows.len(), 1);
    }

    #[tokio::test]
    async fn the_feature_is_in_use_while_a_schedule_exists() {
        let app = App::new().await;
        let web = || HenchWeb::new(&app.api);
        assert!(!in_use(&ctx(web(), "/busy")).await.unwrap());
        app.upsert(sync()).await;
        assert!(in_use(&ctx(web(), "/busy")).await.unwrap());
    }

    #[test]
    fn policies_show_on_the_page() {
        assert_eq!(
            text::catch_up(CatchUp::All { max: 10 }, Some(Duration::from_secs(600))),
            "all enqueued, oldest first (at most 10 per catch-up; ticks older than 10 minutes \
             are skipped)"
        );
    }
}
