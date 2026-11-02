//! [`Periodic`]: the definition of a schedule (`docs/design/periodic.md` §2.3.1).

use std::fmt;
use std::time::{Duration, SystemTime};

use serde_json::Value;

use crate::client::Client;
use crate::error::{Error, Result};
use crate::job::{Job, args_to_values};
use crate::limit::ThrottleOption;
use crate::options::JobOptions;
use crate::payload::{BacktraceOption, JobPayload, RetryOption};
use crate::periodic::definition::{
    Definition, MAX_DESCRIPTION_BYTES, canonical_text, check_name, check_template,
    definition_digest, template_json,
};
use crate::periodic::plan::Policy;
use crate::periodic::{CatchUp, PeriodicMeta, Timetable, epoch_millis, epoch_secs};
use crate::unique::{UniqueOption, UniqueUntil};

/// The timetable as given to the constructor; the zone and the offset are kept apart so that
/// one set on the wrong kind of timetable is reported by [`Periodic::validate`].
#[derive(Debug, Clone, PartialEq)]
enum Kind {
    Cron(String),
    Every(Duration),
}

/// A periodic job: a named timetable that enqueues a job at each tick.
///
/// Register code-defined schedules with [`HenchBuilder::periodic`](crate::HenchBuilder::periodic)
/// (or the worker file's `[[periodic]]` tables); create dynamic ones through the API
/// ([`PeriodicSet::upsert`](crate::periodic::PeriodicSet::upsert)). Every process runs a poller
/// that
/// enqueues each tick's job at most once, whatever the number of processes.
///
/// The job's options are those set here, over the job type's
/// ([`Job::options`](crate::Job::options)); the stored template is self-contained, so a process
/// that does not know the job type fires it identically. The pushing process's default job
/// options and client middleware still apply when a tick is pushed.
///
/// ```
/// use std::time::Duration;
/// use hench::periodic::{CatchUp, Periodic};
///
/// let refresh = Periodic::every("refresh-rates", Duration::from_secs(30))
///     .class("rates::Refresh", vec![])
///     .queue("rates")
///     .catch_up(CatchUp::Skip);
/// refresh.validate().unwrap();
/// assert_eq!(refresh.name(), "refresh-rates");
///
/// let nightly = Periodic::cron("nightly-report", "0 2 * * *")
///     .tz("Europe/Paris")
///     .class("reports::Nightly", vec![])
///     .description("Daily PDF report for the previous day");
/// assert_eq!(nightly.describe(), "At 02:00 (Europe/Paris)");
/// ```
#[derive(Clone, PartialEq)]
pub struct Periodic {
    name: String,
    kind: Kind,
    tz: Option<String>,
    offset: Option<Duration>,
    class: String,
    /// The arguments, or why they could not be serialized (reported by validation).
    args: std::result::Result<Vec<Value>, String>,
    /// The job type's options (`Periodic::job`), under `options`.
    class_options: Option<JobOptions>,
    /// `J::unique_key(&args)`, used when the merged options make the job unique without a key.
    typed_unique_key: Option<Value>,
    /// `J::throttle_key(&args)`, used when the merged options name a throttle without a key.
    typed_throttle_key: Option<String>,
    /// Options set on the definition (they win over the job type's).
    options: JobOptions,
    catch_up: CatchUp,
    deadline: Option<Duration>,
    paused: bool,
    description: Option<String>,
    /// Built from a `[[periodic]]` table of the worker's file (class and JSON arguments): the
    /// job type the process registers for the class applies at build, as with
    /// [`Periodic::job`].
    from_file: bool,
}

impl Periodic {
    fn with_kind(name: impl Into<String>, kind: Kind) -> Self {
        Self {
            name: name.into(),
            kind,
            tz: None,
            offset: None,
            class: String::new(),
            args: Ok(Vec::new()),
            class_options: None,
            typed_unique_key: None,
            typed_throttle_key: None,
            options: JobOptions::new(),
            catch_up: CatchUp::default(),
            deadline: None,
            paused: false,
            description: None,
            from_file: false,
        }
    }

    /// Ticks from a cron expression: 5 fields (minute resolution) or 6 (seconds first), or an
    /// alias such as `@daily`. Evaluated in UTC unless [`Periodic::tz`] says otherwise.
    ///
    /// The dialect: lists, ranges, `*/n` and `a-b/n` steps, month and weekday names in any
    /// case, weekday 7 for Sunday, `?` as `*` in the day fields, `L` (last day of the month;
    /// `5L` or `5#L`: last Friday), `15W` (the weekday nearest the 15th), `5#2` (second Friday),
    /// and a `+` before the weekday field to require both day fields (without it, as in POSIX
    /// cron, either day field matching is enough when both are restricted). Aliases:
    /// `@yearly`, `@annually`, `@monthly`, `@weekly`, `@daily`, `@midnight`, `@hourly`. Not
    /// supported: a year field, `0/10` (write `*/10`), `@reboot`, `@every` (use
    /// [`Periodic::every`]).
    ///
    /// Daylight saving time follows Vixie cron: a fixed-time job (seconds, minutes and hours
    /// fields that do not start with `*`: single values, lists and ranges, such as `30 2 * * *`,
    /// `30 1,13 * * *` or `15 1-3 * * *`) whose time is skipped runs right after the gap and runs
    /// once when the time repeats; fields starting with `*` (`*/15 * * * *`, `0 * * * *`) follow
    /// real time. An expression whose day and month fields match no date (`0 0 30 2 *`) is
    /// refused.
    pub fn cron(name: impl Into<String>, expression: impl Into<String>) -> Self {
        Self::with_kind(name, Kind::Cron(expression.into()))
    }

    /// Ticks every `interval` (whole seconds, at least one), aligned to the Unix epoch: every
    /// process computes the same ticks. `every(7 days)` ticks on Thursdays at 00:00 UTC; use
    /// [`Periodic::cron`] for calendar schedules.
    pub fn every(name: impl Into<String>, interval: Duration) -> Self {
        Self::with_kind(name, Kind::Every(interval))
    }

    /// The job: `J` with these arguments; `J::options()` apply under the options set here.
    ///
    /// When the merged options make the job unique (or throttled) without an explicit key,
    /// [`Job::unique_key`] (and [`Job::throttle_key`]) of these arguments is stored as the
    /// template's key, so that every process fires the same lock (and throttle key) as a typed
    /// push.
    #[must_use]
    pub fn job<J: Job>(mut self, args: J::Args) -> Self {
        self.class = J::NAME.to_owned();
        self.typed_unique_key = J::unique_key(&args);
        self.typed_throttle_key = J::throttle_key(&args);
        self.args = args_to_values(J::NAME, &args).map_err(|error| error.to_string());
        self.class_options = Some(J::options());
        self
    }

    /// The job by class name and JSON arguments (a job this process may not know).
    #[must_use]
    pub fn class(mut self, class: impl Into<String>, args: Vec<Value>) -> Self {
        self.class = class.into();
        self.args = Ok(args);
        self.class_options = None;
        self.typed_unique_key = None;
        self.typed_throttle_key = None;
        self
    }

    /// Marks a schedule of the worker's file: [`Periodic::apply_job_type`] applies at build when
    /// the process registers its class.
    #[cfg(feature = "cli")]
    #[must_use]
    pub(crate) fn defined_in_file(mut self) -> Self {
        self.from_file = true;
        self
    }

    /// Whether this schedule comes from the worker's file (see [`Periodic::defined_in_file`]).
    pub(crate) fn is_defined_in_file(&self) -> bool {
        self.from_file
    }

    /// Applies `job`, the job type this process registers for the schedule's class, as
    /// [`Periodic::job`] does: its options under the ones set here, and its typed unique and
    /// throttle keys of the schedule's arguments, except its `on_conflict` (a schedule's ticks
    /// drop duplicates, the only policy a poller can follow, and the file has no override).
    /// `Err` with the reason when the arguments do not fit the job type.
    pub(crate) fn apply_job_type(
        &mut self,
        job: &dyn crate::job::ErasedJob,
    ) -> std::result::Result<(), String> {
        let args = self.args.clone()?;
        let (unique_key, throttle_key) = job.dyn_keys(&args).map_err(|error| {
            format!(
                "its arguments do not fit the job type registered for `{}`: {error}",
                self.class
            )
        })?;
        let mut options = job.job_options();
        if let Some(UniqueOption::Enabled(unique)) = &mut options.unique {
            unique.on_conflict = None;
        }
        self.class_options = Some(options);
        self.typed_unique_key = unique_key;
        self.typed_throttle_key = throttle_key;
        Ok(())
    }

    /// Job options for the enqueued jobs, over those set before (they win over the job type's
    /// options).
    #[must_use]
    pub fn options(mut self, options: &JobOptions) -> Self {
        self.options = self.options.merge(options);
        self
    }

    fn option(mut self, set: impl FnOnce(JobOptions) -> JobOptions) -> Self {
        self.options = set(self.options);
        self
    }

    /// The queue of the enqueued jobs.
    #[must_use]
    pub fn queue(self, queue: impl Into<String>) -> Self {
        self.option(|options| options.queue(queue))
    }

    /// The retry policy of the enqueued jobs (frequent schedules: `false` or a small count, so
    /// that retries do not overlap later ticks).
    #[must_use]
    pub fn retry(self, retry: impl Into<RetryOption>) -> Self {
        self.option(|options| options.retry(retry))
    }

    /// Whether (or how many lines of) error backtraces are stored.
    #[must_use]
    pub fn backtrace(self, backtrace: impl Into<BacktraceOption>) -> Self {
        self.option(|options| options.backtrace(backtrace))
    }

    /// `false` keeps exhausted jobs out of the morgue.
    #[must_use]
    pub fn dead(self, dead: bool) -> Self {
        self.option(|options| options.dead(dead))
    }

    /// Tags shown in the dashboard.
    #[must_use]
    pub fn tags<I, S>(self, tags: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.option(|options| options.tags(tags))
    }

    /// Time-based retry limit of the enqueued jobs.
    #[must_use]
    pub fn retry_for(self, retry_for: Duration) -> Self {
        self.option(|options| options.retry_for(retry_for))
    }

    /// The queue of the enqueued jobs' retries.
    #[must_use]
    pub fn retry_queue(self, queue: impl Into<String>) -> Self {
        self.option(|options| options.retry_queue(queue))
    }

    /// The log level of the enqueued jobs (see [`JobOptions::log_level`]).
    #[must_use]
    pub fn log_level(self, level: tracing::Level) -> Self {
        self.option(|options| options.log_level(level))
    }

    /// A custom payload key of the enqueued jobs (keys naming payload fields are ignored, with a
    /// warning).
    #[must_use]
    pub fn custom(self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.option(|options| options.custom(key, value))
    }

    /// Makes the enqueued jobs unique for `ttl` (see [`crate::unique`]): a tick whose previous
    /// job still holds its lock (pending or running) is skipped. About one period with
    /// [`UniqueUntil::Success`] prevents overlapping runs.
    #[must_use]
    pub fn unique_for(self, ttl: Duration) -> Self {
        self.option(|options| options.unique_for(ttl))
    }

    /// When the lock of a unique periodic job is released (default
    /// [`UniqueUntil::Success`]).
    #[must_use]
    pub fn unique_until(self, until: UniqueUntil) -> Self {
        self.option(|options| options.unique_until(until))
    }

    /// The value identifying duplicates of a unique periodic job, instead of its arguments (or
    /// [`Job::unique_key`]).
    #[must_use]
    pub fn unique_key(self, key: impl Into<Value>) -> Self {
        self.option(|options| options.unique_key(key))
    }

    /// Subjects the enqueued jobs to the throttle `name` (see [`crate::limit`]).
    #[must_use]
    pub fn throttle(self, name: impl Into<String>) -> Self {
        self.option(|options| options.throttle(name))
    }

    /// The throttle key of the enqueued jobs, instead of [`Job::throttle_key`].
    #[must_use]
    pub fn throttle_key(self, key: impl Into<String>) -> Self {
        self.option(|options| options.throttle_key(key))
    }

    /// IANA time zone of a cron timetable (default `UTC`, never the host's zone). An interval
    /// timetable has none: setting one is reported by [`Periodic::validate`].
    #[must_use]
    pub fn tz(mut self, zone: impl Into<String>) -> Self {
        self.tz = Some(zone.into());
        self
    }

    /// Offset of an interval timetable's ticks (whole seconds, below the interval): ticks at
    /// `k × interval + offset` epoch seconds. Changing it is a timetable change. A cron
    /// timetable has none: setting one is reported by [`Periodic::validate`].
    #[must_use]
    pub fn offset(mut self, offset: Duration) -> Self {
        self.offset = Some(offset);
        self
    }

    /// What to do with ticks missed while nothing could enqueue them (default
    /// [`CatchUp::Latest`]).
    #[must_use]
    pub fn catch_up(mut self, policy: CatchUp) -> Self {
        self.catch_up = policy;
        self
    }

    /// Never enqueue a tick more than this late (whole seconds; default: none, or 5 minutes for
    /// [`CatchUp::Skip`]).
    #[must_use]
    pub fn deadline(mut self, deadline: Duration) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// Create the schedule paused (ignored when it already exists: pausing is the operators'
    /// state, kept across registrations).
    #[must_use]
    pub fn paused(mut self, paused: bool) -> Self {
        self.paused = paused;
        self
    }

    /// Free text shown by the dashboard and the command line (at most 2 000 bytes).
    #[must_use]
    pub fn description(mut self, text: impl Into<String>) -> Self {
        self.description = Some(text.into());
        self
    }

    /// The schedule's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The timetable.
    pub fn timetable(&self) -> Timetable {
        match &self.kind {
            Kind::Cron(expression) => Timetable::Cron {
                expression: expression.clone(),
                tz: self.tz.clone().unwrap_or_else(|| "UTC".to_owned()),
            },
            Kind::Every(interval) => Timetable::Every {
                interval: *interval,
                offset: self.offset.unwrap_or_default(),
            },
        }
    }

    /// The catch-up policy.
    pub fn catch_up_policy(&self) -> CatchUp {
        self.catch_up
    }

    /// The job's class (empty until [`Periodic::job`] or [`Periodic::class`]).
    pub(crate) fn class_name(&self) -> &str {
        &self.class
    }

    /// Checks everything a poller needs: the name (1 to 200 bytes, no control characters, no
    /// leading or trailing whitespace, not `.` or `..`), the expression (it must tick:
    /// `0 0 30 2 *` never does), the zone, the interval and offset, the catch-up policy and
    /// deadline, and the job (a class, arguments that serialize, and options a periodic job may
    /// have: no `on_conflict` other than `drop`, a `unique_for` when unique). Fails with
    /// [`Error::InvalidSchedule`].
    pub fn validate(&self) -> Result<()> {
        self.definition().map(|_| ())
    }

    /// The next `count` ticks strictly after `after` (whole seconds).
    pub fn upcoming(&self, after: SystemTime, count: usize) -> Result<Vec<SystemTime>> {
        self.timetable()
            .upcoming(after, count)
            .map_err(|error| self.invalid(&error.to_string()))
    }

    /// The timetable in English: `At 02:00 (Europe/Paris)`, `Every 30 seconds`.
    pub fn describe(&self) -> String {
        self.timetable().describe()
    }

    /// The job the poller would push for `tick`, before client middleware: the template with
    /// the tick's `periodic` field (its jid and `created_at` are set when it is pushed).
    pub fn payload(&self, tick: SystemTime) -> Result<JobPayload> {
        let mut job = self.template().map_err(|reason| self.invalid(&reason))?;
        job.periodic = Some(PeriodicMeta::new(
            self.name.clone(),
            epoch_millis(tick),
            false,
        ));
        Ok(job)
    }

    /// Pushes [`Periodic::payload`]`(tick)` through `client` (normalization, client middleware,
    /// uniqueness), as the poller would: for tests, with fake or inline clients. Returns the
    /// jid, or `None` when middleware stopped the push or the job is a duplicate.
    pub async fn enqueue(&self, client: &Client, tick: SystemTime) -> Result<Option<String>> {
        self.validate()?;
        client.push(self.payload(tick)?).await
    }

    /// The error of this schedule, for `reason`.
    fn invalid(&self, reason: &str) -> Error {
        Error::InvalidSchedule(format!("`{}`: {reason}", self.name.escape_debug()))
    }

    /// The job template: the class and arguments, the options set here over the job type's,
    /// and the typed keys where the merged options call for them.
    fn template(&self) -> std::result::Result<JobPayload, String> {
        let args = self.args.clone()?;
        let mut job = JobPayload::new(self.class.clone(), args);
        self.options.apply_overrides(&mut job);
        if let Some(class_options) = &self.class_options {
            class_options.apply_defaults(&mut job);
        }
        if let Some(UniqueOption::Enabled(unique)) = &mut job.unique
            && unique.key.is_none()
        {
            unique.key.clone_from(&self.typed_unique_key);
        }
        if let Some(ThrottleOption::Enabled { key, .. }) = &mut job.throttle
            && key.is_none()
        {
            key.clone_from(&self.typed_throttle_key);
        }
        check_template(&job)?;
        Ok(job)
    }

    /// The stored definition (everything checked).
    pub(crate) fn definition(&self) -> Result<Definition> {
        self.build_definition()
            .map_err(|reason| self.invalid(&reason))
    }

    fn build_definition(&self) -> std::result::Result<Definition, String> {
        check_name(&self.name)?;
        match (&self.kind, &self.tz, &self.offset) {
            (Kind::Every(_), Some(_), _) => {
                return Err(
                    "a time zone applies to cron timetables only (intervals tick at the same \
                     instants in every zone)"
                        .to_owned(),
                );
            }
            (Kind::Cron(_), _, Some(_)) => {
                return Err("an offset applies to interval timetables only".to_owned());
            }
            _ => {}
        }
        let timetable = self.timetable();
        let timing = timetable.timing().map_err(|error| error.0)?;
        let policy = Policy {
            catch_up: self.catch_up,
            deadline: self.deadline,
        };
        policy.check()?;
        if let Some(description) = &self.description
            && description.len() > MAX_DESCRIPTION_BYTES
        {
            return Err(format!(
                "the description must be at most {MAX_DESCRIPTION_BYTES} bytes"
            ));
        }
        let template = self.template()?;
        let job = template_json(&template)?;
        let job_value: Value = serde_json::from_str(&job).map_err(|error| error.to_string())?;
        let spec_value = timetable.spec_value();
        let policy_value = policy.value();
        let digest = definition_digest(
            &spec_value,
            &policy_value,
            &job_value,
            self.description.as_deref(),
        )?;
        Ok(Definition {
            name: self.name.clone(),
            spec: canonical_text(&spec_value)?,
            timing_key: timetable.timing_key(),
            policy: canonical_text(&policy_value)?,
            job,
            description: self.description.clone(),
            digest,
            paused: self.paused,
            timing,
        })
    }
}

impl Timetable {
    /// The next `count` ticks strictly after `after` (whole seconds), or why the timetable is
    /// invalid.
    pub fn upcoming(&self, after: SystemTime, count: usize) -> Result<Vec<SystemTime>> {
        let timing = self
            .timing()
            .map_err(|error| Error::InvalidSchedule(error.0))?;
        let ticks = timing
            .upcoming(epoch_secs(after), count)
            .map_err(|error| Error::InvalidSchedule(error.0))?;
        Ok(ticks.into_iter().map(crate::periodic::tick_time).collect())
    }

    /// The timetable in English (`At 02:00 (Europe/Paris)`, `Every 30 seconds`), or why it is
    /// invalid.
    pub fn describe(&self) -> String {
        match self.timing() {
            Ok(timing) => timing.describe(),
            Err(error) => format!("invalid timetable: {error}"),
        }
    }

    /// `at` on the timetable's clock (`2026-10-25 01:30:00 +01:00`; UTC for intervals), or
    /// `None` when the timetable is invalid.
    pub fn local_time(&self, at: SystemTime) -> Option<String> {
        self.timing().ok()?.local(epoch_secs(at)).ok()
    }

    /// For an instant on a day the timetable's zone changes its offset (daylight saving time),
    /// what changes: `the clocks change from +01:00 to +00:00 at 2026-10-25 02:00:00 (local
    /// time before the change)`. `None` otherwise, and for intervals.
    pub fn transition_note(&self, at: SystemTime) -> Option<String> {
        self.timing().ok()?.transition_note(epoch_secs(at))
    }

    /// Checks the timetable: the expression (it must tick), the zone, the interval and offset.
    pub fn validate(&self) -> Result<()> {
        self.timing()
            .map(|_| ())
            .map_err(|error| Error::InvalidSchedule(error.0))
    }
}

impl fmt::Debug for Periodic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Periodic")
            .field("name", &self.name)
            .field("timetable", &self.timetable())
            .field("class", &self.class)
            .field("args", &self.args)
            .field("options", &self.options)
            .field("catch_up", &self.catch_up)
            .field("deadline", &self.deadline)
            .field("paused", &self.paused)
            .field("description", &self.description)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::JobContext;
    use crate::error::JobError;
    use serde_json::json;

    struct Rollup;

    impl Job for Rollup {
        const NAME: &'static str = "Rollup";
        type Args = (i64, String);

        fn options() -> JobOptions {
            JobOptions::new()
                .queue("rollups")
                .retry(5)
                .tags(["metrics"])
                .unique_for(Duration::from_secs(3_600))
                .throttle("warehouse")
        }

        fn unique_key((hour, _): &Self::Args) -> Option<Value> {
            Some(json!([hour]))
        }

        fn throttle_key((_, tenant): &Self::Args) -> Option<String> {
            Some(tenant.clone())
        }

        async fn perform(&self, _ctx: &JobContext, _args: Self::Args) -> Result<(), JobError> {
            Ok(())
        }
    }

    struct Floats;

    impl Job for Floats {
        const NAME: &'static str = "Floats";
        type Args = (f64,);

        async fn perform(&self, _ctx: &JobContext, _args: Self::Args) -> Result<(), JobError> {
            Ok(())
        }
    }

    fn template(periodic: &Periodic) -> Value {
        let definition = periodic.definition().unwrap();
        serde_json::from_str(&definition.job).unwrap()
    }

    #[test]
    fn templates_merge_the_options_and_store_the_typed_keys() {
        let periodic = Periodic::cron("rollup", "5 * * * *")
            .job::<Rollup>((3, "acme".to_owned()))
            .retry(false);
        let stored = template(&periodic);
        assert_eq!(stored["class"], json!("Rollup"));
        assert_eq!(stored["args"], json!([3, "acme"]));
        assert_eq!(stored["queue"], json!("rollups"));
        assert_eq!(stored["retry"], json!(false));
        assert_eq!(stored["tags"], json!(["metrics"]));
        assert_eq!(stored["unique"]["for"], json!(3_600));
        assert_eq!(stored["unique"]["key"], json!([3]));
        assert_eq!(stored["throttle"]["name"], json!("warehouse"));
        assert_eq!(stored["throttle"]["key"], json!("acme"));
        assert!(stored.get("jid").is_none());
        // Explicit keys win over the typed ones.
        let explicit = Periodic::cron("rollup", "5 * * * *")
            .job::<Rollup>((3, "acme".to_owned()))
            .unique_key("all")
            .throttle_key("shared");
        let stored = template(&explicit);
        assert_eq!(stored["unique"]["key"], json!("all"));
        assert_eq!(stored["throttle"]["key"], json!("shared"));
        // Not unique and not throttled: no key.
        let plain = Periodic::cron("rollup", "5 * * * *")
            .job::<Rollup>((3, "acme".to_owned()))
            .options(&JobOptions::new().not_unique().no_throttle());
        let stored = template(&plain);
        assert!(stored["unique"].get("key").is_none(), "{stored}");
        assert!(stored["throttle"].get("key").is_none(), "{stored}");
    }

    #[test]
    fn identical_definitions_have_identical_digests_and_changes_change_them() {
        let base = || {
            Periodic::cron("report", "0 2 * * *")
                .tz("Europe/Paris")
                .class("Report", vec![json!({"b": 1, "a": 2})])
        };
        let digest = base().definition().unwrap().digest;
        assert_eq!(base().definition().unwrap().digest, digest);
        let timing_key = base().definition().unwrap().timing_key;
        for (changed, same_timing) in [
            (base().queue("other"), true),
            (base().catch_up(CatchUp::Skip), true),
            (base().deadline(Duration::from_secs(60)), true),
            (base().description("text"), true),
            (base().tz("UTC"), false),
            (
                Periodic::cron("report", "0 3 * * *")
                    .tz("Europe/Paris")
                    .class("Report", vec![json!({"b": 1, "a": 2})]),
                false,
            ),
        ] {
            let definition = changed.definition().unwrap();
            assert_ne!(definition.digest, digest, "{changed:?}");
            assert_eq!(
                definition.timing_key == timing_key,
                same_timing,
                "{changed:?}"
            );
        }
        // Created paused is operator state, not part of the definition.
        assert_eq!(base().paused(true).definition().unwrap().digest, digest);
    }

    #[test]
    fn validation_reports_every_mistake() {
        let job = |periodic: Periodic| periodic.class("Report", vec![]);
        for (periodic, reason) in [
            (job(Periodic::cron("", "* * * * *")), "invalid name"),
            (job(Periodic::cron(" x", "* * * * *")), "invalid name"),
            (job(Periodic::cron("x", "0 0 30 2 *")), "never ticks"),
            (job(Periodic::cron("x", "@reboot")), "@reboot"),
            (
                job(Periodic::cron("x", "* * * * *").tz("Nowhere/City")),
                "unknown time zone",
            ),
            (
                job(Periodic::every("x", Duration::from_secs(60)).tz("UTC")),
                "time zone applies",
            ),
            (
                job(Periodic::cron("x", "* * * * *").offset(Duration::from_secs(1))),
                "offset applies",
            ),
            (
                job(Periodic::every("x", Duration::ZERO)),
                "at least one second",
            ),
            (
                job(Periodic::every("x", Duration::from_millis(1_500))),
                "whole seconds",
            ),
            (
                job(Periodic::every("x", Duration::from_secs(60)).offset(Duration::from_secs(60))),
                "below the interval",
            ),
            (Periodic::cron("x", "* * * * *"), "no job"),
            (
                job(Periodic::cron("x", "* * * * *")).catch_up(CatchUp::All { max: 0 }),
                "between 1 and 10000",
            ),
            (
                job(Periodic::cron("x", "* * * * *")).deadline(Duration::ZERO),
                "deadline",
            ),
            (
                job(Periodic::cron("x", "* * * * *")).description("d".repeat(2_001)),
                "description",
            ),
            (
                job(Periodic::cron("x", "* * * * *"))
                    .unique_for(Duration::from_secs(60))
                    .options(&JobOptions::new().on_conflict(crate::OnConflict::Error)),
                "drop duplicates",
            ),
            (
                job(Periodic::cron("x", "* * * * *")).unique_until(UniqueUntil::Start),
                "unique_for",
            ),
        ] {
            let error = periodic.validate().unwrap_err();
            assert!(matches!(error, Error::InvalidSchedule(_)), "{error}");
            assert!(error.to_string().contains(reason), "{reason}: {error}");
        }
        // docs-usability-14: a conflict policy other than `drop` says how to override it (a job
        // type declaring `on_conflict = "error"` for its web handlers can still be scheduled).
        let strict = job(Periodic::cron("x", "* * * * *"))
            .unique_for(Duration::from_secs(60))
            .options(&JobOptions::new().on_conflict(crate::OnConflict::Error));
        let error = strict.validate().unwrap_err().to_string();
        assert!(
            error.contains(".options(&JobOptions::new().on_conflict(OnConflict::Drop))"),
            "{error}"
        );
        strict
            .options(&JobOptions::new().on_conflict(crate::OnConflict::Drop))
            .validate()
            .unwrap();
        // Arguments that do not serialize back are reported too.
        let nan = Periodic::cron("x", "* * * * *").job::<Floats>((f64::NAN,));
        assert!(
            nan.validate()
                .unwrap_err()
                .to_string()
                .contains("non-finite")
        );
    }

    #[test]
    fn payloads_carry_the_tick() {
        let periodic =
            Periodic::every("refresh", Duration::from_secs(30)).class("Refresh", vec![json!(1)]);
        let tick = SystemTime::UNIX_EPOCH + Duration::from_secs(1_800_000_000);
        let job = periodic.payload(tick).unwrap();
        assert_eq!(job.class, "Refresh");
        assert_eq!(job.args, vec![json!(1)]);
        assert!(job.jid.is_empty());
        assert_eq!(
            job.periodic,
            Some(PeriodicMeta::new("refresh", 1_800_000_000_000, false))
        );
        let upcoming = periodic.upcoming(tick, 2).unwrap();
        assert_eq!(
            upcoming,
            [
                tick + Duration::from_secs(30),
                tick + Duration::from_secs(60)
            ]
        );
        assert_eq!(periodic.describe(), "Every 30 seconds");
        assert!(
            Periodic::cron("x", "nonsense")
                .describe()
                .starts_with("invalid timetable")
        );
    }

    #[tokio::test]
    async fn fake_clients_capture_enqueued_ticks() {
        let client = Client::fake();
        let periodic = Periodic::every("refresh", Duration::from_secs(30))
            .class("Refresh", vec![])
            .unique_for(Duration::from_secs(60));
        let tick = SystemTime::UNIX_EPOCH + Duration::from_secs(1_800_000_000);
        let first = periodic.enqueue(&client, tick).await.unwrap();
        assert!(first.is_some());
        // The previous tick's job still holds its lock: a duplicate.
        let second = periodic
            .enqueue(&client, tick + Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(second, None);
        let invalid = Periodic::every("bad", Duration::ZERO).class("Refresh", vec![]);
        assert!(invalid.enqueue(&client, tick).await.is_err());
    }
}
