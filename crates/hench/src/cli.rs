//! Command-line options and a configuration file for worker binaries (feature `cli`).
//!
//! [`WorkerArgs`] parses the usual worker options with [clap](https://docs.rs/clap) (use it as
//! the binary's parser, or flatten it into your own) and [`WorkerArgs::apply`] applies them to a
//! [`HenchBuilder`], together with the TOML configuration file given with `-C` (or
//! `config/hench.toml` in the current directory, when it exists). Options on the command line
//! win over the file, and the file wins over what the program set on the builder before.
//!
//! ```text
//! worker -c 10 -q critical,3 -q default -q low -t 30 -C config/hench.toml -e production -v
//! ```
//!
//! | Option | Meaning |
//! |---|---|
//! | `-c`, `--concurrency INT` | jobs processed at once by the default capsule |
//! | `-q`, `--queue NAME[,WEIGHT]` | a queue of the default capsule (repeat it); without weights queues are checked in the order given, with weights in a random order biased by them |
//! | `-t`, `--timeout SECONDS` | how long a shutdown waits for running jobs |
//! | `-C`, `--config PATH` | the configuration file |
//! | `-e`, `--environment ENV` | the section of the configuration file to use (default `APP_ENV`, else `development`) |
//! | `-g`, `--tag TAG` | the tag shown in the dashboard |
//! | `-v`, `--verbose` | log at debug level ([`Settings::log_level`]) |
//!
//! The configuration file takes the same settings, plus labels, capsules, a few process
//! settings and the settings of the features. A top-level table named after an environment
//! overrides the settings above it when that environment is selected:
//!
//! ```toml
//! concurrency = 10
//! queues = [["critical", 3], ["default", 2], "low,1"]
//! timeout = 25            # seconds
//! tag = "billing"
//! labels = ["eu-west", "canary"]
//! verbose = false
//! max_retries = 25
//! max_recoveries = 3      # recoveries from dead processes before a job is a poison pill
//! dead_max_jobs = 10000
//! dead_timeout = 15552000 # seconds
//! max_iteration_runtime = 60.0
//! average_scheduled_poll_interval = 5.0 # scaled by the number of processes
//! fetch = "reliable"      # or "basic"
//! metrics = true
//! job_logging = true
//!
//! [capsules.serial]
//! concurrency = 1
//! queues = ["serial"]
//!
//! [production]
//! concurrency = 25
//! ```
//!
//! Queues are given as `"name"`, `"name,weight"`, `["name"]` or `["name", weight]`.
//! `poll_interval_average` fixes the scheduler's average poll interval instead of scaling
//! `average_scheduled_poll_interval`.
//!
//! **Times are numbers of seconds**, never strings: `timeout = 30`, `lease = 0.5`, not
//! `"30s"` (the command line's `--unique-for 10m` style is the `hench` command's). Some take a
//! whole number of seconds (written `30`), others any number (`30` or `0.5`), as the table
//! below says; a string is refused with an error that says so.
//!
//! | Key | Value | Default |
//! |---|---|---|
//! | `concurrency`, `queues`, `timeout`, `tag`, `labels`, `verbose` | as the options above (`timeout`: whole seconds) | |
//! | `capsules.<name>` | table: `concurrency`, `queues` | |
//! | `max_retries` | retries of `retry: true` jobs | 25 |
//! | `max_recoveries` | recoveries of a job from processes that died while they held it before it is moved to the morgue as a poison pill ([`Config::max_recoveries`](crate::Config::max_recoveries)) | 3 |
//! | `dead_max_jobs`, `dead_timeout` | morgue size, and age (whole seconds) | 10 000, 180 days |
//! | `max_iteration_runtime` | seconds an iterable job runs before it saves its cursor and continues later | none |
//! | `poll_interval_average`, `average_scheduled_poll_interval` | seconds | none, 5 |
//! | `fetch` | `"reliable"` or `"basic"` | `"reliable"` |
//! | `metrics`, `job_logging` | booleans | `true` |
//! | `unique_jobs` | whether pushes honour uniqueness ([`crate::unique`]) | `true` |
//! | `periodic_group` | the group whose schedules this process registers ([`crate::periodic`]) | `"default"` |
//! | `periodic_poller` | whether this process fires schedules | `true` |
//! | `periodic_poll_interval` | longest wait between two looks at the schedules, seconds | 5 |
//! | `periodic_lease` | how long a poller that dies while firing a tick holds it back, seconds (at least twice `periodic_poll_interval`) | 60 |
//! | `periodic_history` | enqueues remembered per schedule | 10 |
//! | `batch_ttl`, `batch_linger` | expiry of an unfinished batch after its last activity, and of a finished one, whole seconds ([`crate::batch`]) | 30 days, 24 hours |
//! | `batch_max_commit_jobs` | most members of one batch commit | 10 000 |
//! | `batch_maintenance` | whether this process runs batch maintenance | `true` |
//! | `[throttles.<name>]` | a throttle ([`crate::limit::Throttle`]): `limits` (specs such as `"concurrency=25"` or `"window=5/1s per key"`; a per-key limit applies to jobs pushed with a throttle key), `reschedules`, `max_delay`, `max_wait` and `lease` (seconds), `fail_open`, `stats` (whether to keep counters, the index entry and the wait line; `true`) and `stats_ttl` (seconds; 1 day) | |
//! | `[queue_limits]` | limits on every fetch of a queue ([`crate::api::Queue::set_limits`]): queue name → a string of specs joined by `;` (`exports = "concurrency=3; window=100/1m"`) or an array of specs; concurrency, window, bucket and leaky limits, at most one of each kind; written to Redis at start, replacing the stored limits of those queues | |
//! | `[[periodic]]` | a schedule ([`crate::periodic`]): `name`, `cron` and `tz`, or `every` and `offset` (whole seconds), `class`, `args`, `queue`, `retry`, `tags`, `catch_up` (`"latest"`, `"skip"`, `"all"`) and `catch_up_max`, `deadline` (whole seconds), `paused`, `description`, `unique_for` (whole seconds), `unique_until`, `throttle`, `throttle_key` | |
//!
//! ```toml
//! unique_jobs = true
//! periodic_poll_interval = 5.0
//! batch_ttl = 2592000          # 30 days, in seconds
//!
//! [throttles.stripe]
//! limits = ["concurrency=3", "window=100/1s per key"]
//! max_wait = 0.5               # seconds
//! lease = 30                   # seconds
//!
//! [queue_limits]
//! exports = "concurrency=3"
//!
//! [[periodic]]
//! name = "nightly-report"
//! cron = "0 2 * * *"
//! tz = "Europe/Paris"
//! class = "nightly_report"
//! args = []
//! unique_for = 3600            # seconds
//! ```
//!
//! `concurrency` and `queues` configure the default capsule, which is also the capsule named
//! `default`: a `[capsules.default]` table sets the same two settings (and wins over the
//! top-level keys). `-c` and `-q` override both, like every setting of the file.
//!
//! Every part of the file is checked when it is read, the sections of the other environments
//! included (a mistake in `[production]` fails in development already). The tables
//! `capsules`, `throttles`, `queue_limits` and `periodic` are settings; any other top-level
//! table is an environment's section, which holds settings only (so a misspelled
//! `[capsule.serial]` is an error, not an ignored environment). Section names are
//! case-sensitive. A table in an environment's section replaces the top-level one as a whole
//! (`[production.queue_limits]` drops every top-level `[queue_limits]` entry it does not
//! repeat).
//!
//! [`Settings::apply`] logs the file and the environment in use, and warnings when an
//! environment chosen with `-e` or `APP_ENV` selects nothing: no configuration file was read
//! (the default file is relative to the current directory, so start the worker in the
//! application's directory, as systemd's `WorkingDirectory=` does, or name the file with
//! `-C`), or the file has no section for it.
//!
//! ```no_run
//! use clap::Parser;
//! use hench::Hench;
//! use hench::cli::WorkerArgs;
//!
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let args = WorkerArgs::parse();
//! let settings = args.settings()?;
//! // Set up logging at `settings.log_level()` (`apply` logs the settings in use), then:
//! let hench = settings.apply(Hench::builder().queues(["default"])).build().await?;
//! hench.run().await?;
//! # Ok(()) }
//! ```

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

use crate::config::{CapsuleConfig, FetchStrategy, HenchBuilder};
use crate::error::{Error, Result};

/// The configuration file read when `-C` is not given (and it exists), relative to the
/// current directory.
pub const DEFAULT_CONFIG_FILE: &str = "config/hench.toml";

/// The environment used when neither `-e` nor `APP_ENV` gives one.
pub const DEFAULT_ENVIRONMENT: &str = "development";

/// Command-line options of a worker process. See the [module documentation](self).
#[derive(Debug, Clone, Default, PartialEq, Eq, clap::Parser)]
#[command(about = "Processes background jobs", long_about = None)]
#[non_exhaustive]
pub struct WorkerArgs {
    /// Jobs processed at once by the default capsule
    #[arg(
        short = 'c',
        long,
        value_name = "INT",
        value_parser = parse_concurrency,
        allow_negative_numbers = true
    )]
    pub concurrency: Option<usize>,

    /// A queue to process, with an optional weight (repeatable); without weights queues are
    /// checked in the order given
    #[arg(short = 'q', long = "queue", value_name = "QUEUE[,WEIGHT]", value_parser = parse_queue)]
    pub queues: Vec<String>,

    /// Seconds a shutdown waits for running jobs
    #[arg(
        short = 't',
        long,
        value_name = "SECONDS",
        value_parser = parse_timeout,
        allow_negative_numbers = true
    )]
    pub timeout: Option<u64>,

    /// Configuration file (TOML) [default: config/hench.toml in the current directory, if it
    /// exists]
    #[arg(short = 'C', long = "config", value_name = "PATH")]
    pub config: Option<PathBuf>,

    /// Application environment: selects a section of the configuration file [default:
    /// APP_ENV, else development]
    #[arg(short = 'e', long, value_name = "ENV")]
    pub environment: Option<String>,

    /// Tag shown in the dashboard
    #[arg(short = 'g', long, value_name = "TAG")]
    pub tag: Option<String>,

    /// Log at debug level
    #[arg(short = 'v', long)]
    pub verbose: bool,
}

impl WorkerArgs {
    /// The settings to run with: the configuration file's (its environment section over its
    /// top level) with these options over them. Fails when the file named with `-C` does not
    /// exist, cannot be read or holds invalid settings.
    pub fn settings(&self) -> Result<Settings> {
        let directory = std::env::current_dir()?;
        self.settings_with(&directory, |name| std::env::var(name).ok())
    }

    /// Loads the settings (see [`WorkerArgs::settings`]) and applies them to `builder`.
    pub fn apply(&self, builder: HenchBuilder) -> Result<HenchBuilder> {
        Ok(self.settings()?.apply(builder))
    }

    /// [`WorkerArgs::settings`] with the default configuration file looked up in `directory`
    /// and environment variables read through `env`.
    fn settings_with(
        &self,
        directory: &Path,
        env: impl Fn(&str) -> Option<String>,
    ) -> Result<Settings> {
        // The environment, and what chose it (`None` for the default).
        let (environment, chosen_by) = match (
            self.environment
                .clone()
                .filter(|environment| !environment.is_empty()),
            env("APP_ENV").filter(|environment| !environment.is_empty()),
        ) {
            (Some(environment), _) => (environment, Some("-e")),
            (None, Some(environment)) => (environment, Some("APP_ENV")),
            (None, None) => (DEFAULT_ENVIRONMENT.to_owned(), None),
        };
        let config_file = match &self.config {
            Some(path) => {
                if !path.is_file() {
                    return Err(Error::Config(format!(
                        "no such configuration file: {}",
                        path.display()
                    )));
                }
                Some(path.clone())
            }
            None => Some(directory.join(DEFAULT_CONFIG_FILE)).filter(|path| path.is_file()),
        };
        let mut settings = match &config_file {
            Some(path) => {
                let text = std::fs::read_to_string(path).map_err(|error| {
                    Error::Config(format!("cannot read {}: {error}", path.display()))
                })?;
                let file = FileSettings::parse(&text, &environment)
                    .map_err(|error| Error::Config(format!("{}: {error}", path.display())))?;
                let mut settings = file.settings;
                settings.section_applied = file.has_section;
                if !file.has_section {
                    // A section named like the environment but for its case is a typo.
                    let near = file
                        .sections
                        .iter()
                        .find(|section| section.eq_ignore_ascii_case(&environment));
                    match (near, chosen_by) {
                        (Some(near), _) => settings.warnings.push(format!(
                            "{} has no `[{environment}]` section for the environment \
                             `{environment}`, but it has `[{near}]`: section names are \
                             case-sensitive; its top-level settings apply",
                            path.display()
                        )),
                        (None, Some(chosen_by)) => settings.warnings.push(format!(
                            "{} has no `[{environment}]` section for the environment \
                             `{environment}` (from {chosen_by}): its top-level settings \
                             apply (add an empty `[{environment}]` table if that is meant)",
                            path.display()
                        )),
                        (None, None) => {}
                    }
                }
                settings
            }
            None => {
                let mut settings = Settings::default();
                if let Some(chosen_by) = chosen_by {
                    settings.warnings.push(format!(
                        "the environment `{environment}` (from {chosen_by}) selects a section of \
                         the configuration file, but no configuration file was read: there is \
                         no {DEFAULT_CONFIG_FILE} in {} (start the worker in the directory that \
                         holds it, or name the file with -C); the program's own settings apply",
                        directory.display()
                    ));
                }
                settings
            }
        };
        settings.environment = environment;
        settings.config_file = config_file;
        if let Some(concurrency) = self.concurrency {
            settings.concurrency = Some(concurrency);
        }
        if !self.queues.is_empty() {
            settings.queues = Some(self.queues.clone());
        }
        if let Some(timeout) = self.timeout {
            settings.timeout = Some(Duration::from_secs(timeout));
        }
        if let Some(tag) = &self.tag {
            settings.tag = Some(tag.clone());
        }
        settings.verbose |= self.verbose || env("DEBUG_INVOCATION").as_deref() == Some("1");
        Ok(settings)
    }
}

/// A worker's settings, from the command line and the configuration file
/// ([`WorkerArgs::settings`]). Unset settings leave the builder as it is.
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct Settings {
    /// The selected environment.
    pub environment: String,
    /// The configuration file read, if any.
    pub config_file: Option<PathBuf>,
    /// Concurrency of the default capsule.
    pub concurrency: Option<usize>,
    /// Queues of the default capsule (`"name"` or `"name,weight"`).
    pub queues: Option<Vec<String>>,
    /// Shutdown timeout.
    pub timeout: Option<Duration>,
    /// Tag shown in the dashboard.
    pub tag: Option<String>,
    /// Labels shown in the dashboard (added to the builder's).
    pub labels: Vec<String>,
    /// Debug logging (`-v`, `verbose = true`, or systemd's `DEBUG_INVOCATION=1`).
    pub verbose: bool,
    /// Additional capsules, by name. (A `[capsules.default]` table of the configuration file
    /// configures the default capsule: it is read into [`Settings::concurrency`] and
    /// [`Settings::queues`], not kept here.)
    pub capsules: BTreeMap<String, CapsuleSettings>,
    /// Retries of jobs with `retry: true`.
    pub max_retries: Option<u32>,
    /// Recoveries of a job from dead processes before it is taken for a poison pill.
    pub max_recoveries: Option<u32>,
    /// Size limit of the morgue.
    pub dead_max_jobs: Option<usize>,
    /// Age limit of the morgue.
    pub dead_timeout: Option<Duration>,
    /// Iteration time limit of iterable jobs.
    pub max_iteration_runtime: Option<Duration>,
    /// Fixed average scheduler poll interval.
    pub poll_interval_average: Option<Duration>,
    /// Target interval between scheduler polls across all processes, which each process scales
    /// by the number of processes (unless `poll_interval_average` fixes it).
    pub average_scheduled_poll_interval: Option<Duration>,
    /// Fetch strategy.
    pub fetch: Option<FetchStrategy>,
    /// Execution metrics.
    pub metrics: Option<bool>,
    /// Logging of every job execution.
    pub job_logging: Option<bool>,
    /// Whether pushes honour uniqueness (unique jobs).
    pub unique_jobs: Option<bool>,
    /// The group whose periodic crontab this process registers.
    pub periodic_group: Option<String>,
    /// Whether this process runs the periodic poller.
    pub periodic_poller: Option<bool>,
    /// Longest wait between two looks at the periodic schedules.
    pub periodic_poll_interval: Option<Duration>,
    /// Enqueues remembered per periodic schedule.
    pub periodic_history: Option<usize>,
    /// How long a poller that dies while firing a periodic tick holds it back.
    pub periodic_lease: Option<Duration>,
    /// Expiry of an unfinished batch after its last activity.
    pub batch_ttl: Option<Duration>,
    /// How long a finished batch's data stays.
    pub batch_linger: Option<Duration>,
    /// Most members of one batch commit (each child batch counting as 10).
    pub batch_max_commit_jobs: Option<usize>,
    /// Whether this process runs batch maintenance.
    pub batch_maintenance: Option<bool>,
    /// The `[throttles.<name>]` tables: defined last, so that they replace the throttles of the
    /// same names defined in code.
    pub throttles: Vec<crate::limit::Throttle>,
    /// The `[queue_limits]` table, by queue: applied after the code's, so that a queue's limits
    /// from the file replace the code's.
    pub queue_limits: Vec<(String, Vec<crate::limit::Limit>)>,
    /// The `[[periodic]]` schedules (a schedule named like one defined in code is a
    /// configuration error at build, as two code schedules of one name are). A table names its
    /// job by `class`: when the process registers that job type, `build` gives the schedule the
    /// job type's options (queue, retries, uniqueness, throttle; not its `on_conflict`) and its
    /// typed unique and throttle keys of the table's `args`, under the table's own keys, as
    /// `Periodic::job` does; a class the process does not register takes the table's keys alone.
    #[cfg(feature = "periodic")]
    pub periodic: Vec<crate::periodic::Periodic>,
    /// What looks wrong in how the settings were chosen without making them invalid, for
    /// example an environment chosen with `-e` while no configuration file was read (the
    /// default file is looked up in the current directory). [`Settings::apply`] logs them as
    /// warnings.
    pub warnings: Vec<String>,
    /// Whether the configuration file had a section for the environment.
    section_applied: bool,
}

/// The settings of one capsule in the configuration file.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct CapsuleSettings {
    /// Jobs processed at once by the capsule.
    pub concurrency: Option<usize>,
    /// Its queues (`"name"` or `"name,weight"`).
    pub queues: Option<Vec<String>>,
}

impl Settings {
    /// The log level these settings ask for: debug when verbose, else info.
    pub fn log_level(&self) -> tracing::Level {
        if self.verbose {
            tracing::Level::DEBUG
        } else {
            tracing::Level::INFO
        }
    }

    /// `tracing-subscriber` `EnvFilter` directives for these settings: [`Settings::log_level`]
    /// for everything, plus [`JOB_LOG_LEVEL_DIRECTIVES`](crate::JOB_LOG_LEVEL_DIRECTIVES) so
    /// that jobs pushed with a lower log level are logged at that level.
    pub fn log_directives(&self) -> String {
        let level = crate::job_logger::level_name(self.log_level());
        format!("{level},{}", crate::JOB_LOG_LEVEL_DIRECTIVES)
    }

    /// Applies the settings to `builder`, leaving what they do not set as it is.
    ///
    /// [`Settings::concurrency`] and [`Settings::queues`] are applied after
    /// [`Settings::capsules`], so they win over an entry for the default capsule
    /// ([`CapsuleConfig::DEFAULT_NAME`]) added to `capsules` by hand.
    ///
    /// It also logs (through `tracing`, so set up logging first) which configuration file and
    /// environment the settings come from, and the [`Settings::warnings`].
    pub fn apply(&self, mut builder: HenchBuilder) -> HenchBuilder {
        self.log();
        for (name, capsule) in &self.capsules {
            builder = builder.capsule(name.clone(), |mut builder| {
                if let Some(concurrency) = capsule.concurrency {
                    builder = builder.concurrency(concurrency);
                }
                if let Some(queues) = &capsule.queues {
                    builder = builder.queues(queues.clone());
                }
                builder
            });
        }
        if let Some(concurrency) = self.concurrency {
            builder = builder.concurrency(concurrency);
        }
        if let Some(queues) = &self.queues {
            builder = builder.queues(queues.clone());
        }
        if let Some(timeout) = self.timeout {
            builder = builder.timeout(timeout);
        }
        if let Some(tag) = &self.tag {
            builder = builder.tag(tag.clone());
        }
        builder = builder.labels(self.labels.clone());
        if let Some(retries) = self.max_retries {
            builder = builder.max_retries(retries);
        }
        if let Some(recoveries) = self.max_recoveries {
            builder = builder.max_recoveries(recoveries);
        }
        if let Some(jobs) = self.dead_max_jobs {
            builder = builder.dead_max_jobs(jobs);
        }
        if let Some(timeout) = self.dead_timeout {
            builder = builder.dead_timeout(timeout);
        }
        if let Some(limit) = self.max_iteration_runtime {
            builder = builder.max_iteration_runtime(limit);
        }
        if let Some(interval) = self.poll_interval_average {
            builder = builder.poll_interval_average(interval);
        }
        if let Some(interval) = self.average_scheduled_poll_interval {
            builder = builder.average_scheduled_poll_interval(interval);
        }
        if let Some(fetch) = self.fetch {
            builder = builder.fetch(fetch);
        }
        if let Some(enabled) = self.metrics {
            builder = builder.metrics(enabled);
        }
        if let Some(enabled) = self.job_logging {
            builder = builder.job_logging(enabled);
        }
        if let Some(enabled) = self.unique_jobs {
            builder = builder.unique_jobs(enabled);
        }
        if let Some(group) = &self.periodic_group {
            builder = builder.periodic_group(group.clone());
        }
        if let Some(enabled) = self.periodic_poller {
            builder = builder.periodic_poller(enabled);
        }
        if let Some(interval) = self.periodic_poll_interval {
            builder = builder.periodic_poll_interval(interval);
        }
        if let Some(entries) = self.periodic_history {
            builder = builder.periodic_history(entries);
        }
        if let Some(lease) = self.periodic_lease {
            builder = builder.periodic_lease(lease);
        }
        if let Some(ttl) = self.batch_ttl {
            builder = builder.batch_ttl(ttl);
        }
        if let Some(linger) = self.batch_linger {
            builder = builder.batch_linger(linger);
        }
        if let Some(jobs) = self.batch_max_commit_jobs {
            builder = builder.batch_max_commit_jobs(jobs);
        }
        if let Some(enabled) = self.batch_maintenance {
            builder = builder.batch_maintenance(enabled);
        }
        #[cfg(feature = "periodic")]
        for periodic in &self.periodic {
            builder = builder.periodic_from_file(periodic.clone());
        }
        // Last, so that the file's throttles and queue limits replace the code's.
        for throttle in &self.throttles {
            builder = builder.throttle(throttle.clone());
        }
        for (queue, limits) in &self.queue_limits {
            builder = builder.queue_limits(queue.clone(), limits.iter().copied());
        }
        builder
    }

    /// Logs where the settings come from, and the warnings.
    fn log(&self) {
        match &self.config_file {
            Some(file) => tracing::info!(
                file = %file.display(),
                environment = %self.environment,
                section = self.section_applied,
                "worker settings from a configuration file"
            ),
            None => tracing::info!(
                environment = %self.environment,
                "worker settings without a configuration file"
            ),
        }
        for warning in &self.warnings {
            tracing::warn!("{warning}");
        }
    }

    /// Reads a configuration file's text for `environment` (tests).
    #[cfg(test)]
    fn from_toml(text: &str, environment: &str) -> std::result::Result<Self, String> {
        FileSettings::parse(text, environment).map(|file| file.settings)
    }
}

/// What a configuration file says for one environment.
struct FileSettings {
    /// Its top level, with the environment's section merged over it.
    settings: Settings,
    /// Whether it has a section for the environment.
    has_section: bool,
    /// The names of its environment sections.
    sections: Vec<String>,
}

impl FileSettings {
    /// Reads a configuration file's text for `environment`. Every part of the file is checked,
    /// the sections of other environments included, so that a mistake in the production
    /// section fails in development already.
    fn parse(text: &str, environment: &str) -> std::result::Result<Self, String> {
        Self::read(text, environment).map_err(with_unit_hint)
    }

    /// [`FileSettings::parse`], without the hint about numbers written as strings.
    fn read(text: &str, environment: &str) -> std::result::Result<Self, String> {
        let mut table: toml::Table = toml::from_str(text).map_err(|error| error.to_string())?;
        if FileConfig::RESERVED_TABLES.contains(&environment) {
            return Err(format!(
                "`{environment}` cannot name an environment: the configuration file reserves it \
                 for its `{environment}` settings"
            ));
        }
        if let Some(value) = table.get(environment)
            && !value.is_table()
        {
            return Err(format!(
                "`{environment}` must be a table of settings for that environment"
            ));
        }
        // Every top-level table but the settings' (`capsules`) is an environment section.
        let section_names: Vec<String> = table
            .iter()
            .filter(|(key, value)| value.is_table() && !FileConfig::FIELDS.contains(&key.as_str()))
            .map(|(key, _)| key.clone())
            .collect();
        let mut sections = BTreeMap::new();
        for name in &section_names {
            if let Some(toml::Value::Table(section)) = table.remove(name) {
                check_section(name, &section)?;
                sections.insert(name.clone(), section);
            }
        }
        FileConfig::from_table(table.clone())?;
        // The selected environment's section overrides the top level (a shallow merge).
        let has_section = match sections.remove(environment) {
            Some(section) => {
                table.extend(section);
                true
            }
            None => false,
        };
        Ok(Self {
            settings: FileConfig::from_table(table)?,
            has_section,
            sections: section_names,
        })
    }
}

/// Checks the environment section `name`: settings only (nested tables are the mistake of a
/// setting's table written at the top level, such as `[capsule.serial]` for
/// `[capsules.serial]`), each valid.
fn check_section(name: &str, section: &toml::Table) -> std::result::Result<(), String> {
    for (key, value) in section {
        if value.is_table() && !FileConfig::FIELDS.contains(&key.as_str()) {
            let hint = FileConfig::FIELDS
                .iter()
                .find(|field| is_near(name, field))
                .map(|field| format!(" (did you mean `[{field}.{key}]`?)"))
                .unwrap_or_default();
            return Err(format!(
                "unknown table `{key}` in the `[{name}]` section: a top-level table is an \
                 environment's section, which holds settings (environment sections cannot be \
                 nested){hint}"
            ));
        }
    }
    FileConfig::from_table(section.clone())
        .map(|_| ())
        .map_err(|error| format!("in the `[{name}]` section: {error}"))
}

/// `true` when `name` is `field` with one character added, removed or changed, or differs
/// only by case (a misspelled setting).
fn is_near(name: &str, field: &str) -> bool {
    let (name, field): (Vec<char>, Vec<char>) = (
        name.to_lowercase().chars().collect(),
        field.chars().collect(),
    );
    if name == field {
        return true;
    }
    match name.len().abs_diff(field.len()) {
        0 => name.iter().zip(&field).filter(|(a, b)| a != b).count() == 1,
        1 => {
            let (shorter, longer) = if name.len() < field.len() {
                (&name, &field)
            } else {
                (&field, &name)
            };
            (0..longer.len()).any(|skip| {
                longer
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| *index != skip)
                    .map(|(_, c)| c)
                    .eq(shorter.iter())
            })
        }
        _ => false,
    }
}

/// The settings a configuration file can hold (an environment section holds the same).
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    concurrency: Option<i64>,
    queues: Option<Vec<toml::Value>>,
    timeout: Option<i64>,
    tag: Option<String>,
    labels: Option<Vec<String>>,
    verbose: Option<bool>,
    capsules: Option<BTreeMap<String, FileCapsule>>,
    max_retries: Option<u32>,
    max_recoveries: Option<u32>,
    dead_max_jobs: Option<i64>,
    dead_timeout: Option<i64>,
    max_iteration_runtime: Option<f64>,
    poll_interval_average: Option<f64>,
    average_scheduled_poll_interval: Option<f64>,
    fetch: Option<String>,
    metrics: Option<bool>,
    job_logging: Option<bool>,
    unique_jobs: Option<bool>,
    periodic_group: Option<String>,
    periodic_poller: Option<bool>,
    periodic_poll_interval: Option<f64>,
    periodic_history: Option<i64>,
    periodic_lease: Option<f64>,
    batch_ttl: Option<i64>,
    batch_linger: Option<i64>,
    batch_max_commit_jobs: Option<i64>,
    batch_maintenance: Option<bool>,
    /// `[throttles.<name>]` tables (rate limiting).
    throttles: Option<toml::Value>,
    /// The `[queue_limits]` table (rate limiting).
    queue_limits: Option<toml::Value>,
    /// The `[[periodic]]` schedules.
    periodic: Option<toml::Value>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileCapsule {
    concurrency: Option<i64>,
    queues: Option<Vec<toml::Value>>,
}

impl FileConfig {
    /// Every setting's key (other top-level tables are environment sections).
    const FIELDS: &'static [&'static str] = &[
        "concurrency",
        "queues",
        "timeout",
        "tag",
        "labels",
        "verbose",
        "capsules",
        "max_retries",
        "max_recoveries",
        "dead_max_jobs",
        "dead_timeout",
        "max_iteration_runtime",
        "poll_interval_average",
        "average_scheduled_poll_interval",
        "fetch",
        "metrics",
        "job_logging",
        "unique_jobs",
        "periodic_group",
        "periodic_poller",
        "periodic_poll_interval",
        "periodic_history",
        "periodic_lease",
        "batch_ttl",
        "batch_linger",
        "batch_max_commit_jobs",
        "batch_maintenance",
        "throttles",
        "queue_limits",
        "periodic",
    ];

    /// The tables of the features, reserved: they cannot name an environment (`capsules` is
    /// reserved too, as it always was).
    const RESERVED_TABLES: &'static [&'static str] = &["throttles", "queue_limits", "periodic"];

    /// The settings of a table of the file: its keys and their values checked.
    fn from_table(table: toml::Table) -> std::result::Result<Settings, String> {
        let file: FileConfig = toml::Value::Table(table)
            .try_into()
            .map_err(|error: toml::de::Error| error.to_string())?;
        file.into_settings()
    }

    fn into_settings(self) -> std::result::Result<Settings, String> {
        // The feature tables, read by their features.
        let throttles = match &self.throttles {
            Some(value) => crate::limit::toml::parse_throttles("throttles", value)
                .map_err(|error| crate::limit::toml::reason(&error))?,
            None => Vec::new(),
        };
        let queue_limits = match &self.queue_limits {
            Some(value) => crate::limit::toml::parse_queue_limits("queue_limits", value)
                .map_err(|error| crate::limit::toml::reason(&error))?,
            None => Vec::new(),
        };
        #[cfg(feature = "periodic")]
        let periodic = match &self.periodic {
            Some(value) => crate::periodic::toml::parse_schedules("periodic", value)
                .map_err(|error| error.to_string())?,
            None => Vec::new(),
        };
        #[cfg(not(feature = "periodic"))]
        if let Some(value) = &self.periodic {
            crate::periodic::toml::schedules("periodic", value)
                .map_err(|error| error.to_string())?;
        }
        if let Some(group) = &self.periodic_group
            && !crate::config::valid_name(group, 100)
        {
            return Err(format!(
                "periodic_group: `{group}` is not a valid group name (1 to 100 bytes, no control \
                 characters, no leading or trailing whitespace)"
            ));
        }
        let mut capsules = BTreeMap::new();
        for (name, capsule) in self.capsules.unwrap_or_default() {
            if name.is_empty() {
                return Err("capsule names must not be empty".to_owned());
            }
            let concurrency = capsule
                .concurrency
                .map(|value| positive(value, &format!("capsules.{name}.concurrency")))
                .transpose()?;
            let queues = capsule
                .queues
                .map(|queues| queue_list(&queues, &format!("capsules.{name}.queues")))
                .transpose()?;
            capsules.insert(
                name,
                CapsuleSettings {
                    concurrency,
                    queues,
                },
            );
        }
        let fetch = match self.fetch.as_deref() {
            None => None,
            Some("reliable") => Some(FetchStrategy::Reliable),
            Some("basic") => Some(FetchStrategy::Basic),
            Some(other) => {
                return Err(format!(
                    "fetch: `{other}` is not a fetch strategy (use \"reliable\" or \"basic\")"
                ));
            }
        };
        let mut concurrency = self
            .concurrency
            .map(|value| positive(value, "concurrency"))
            .transpose()?;
        let mut queues = self
            .queues
            .map(|queues| queue_list(&queues, "queues"))
            .transpose()?;
        // `[capsules.default]` names the capsule the top-level keys configure: its settings
        // become those keys (winning over them), so that `-c` and `-q`, which set the same
        // two settings, override it like every other setting of the file.
        if let Some(default) = capsules.remove(CapsuleConfig::DEFAULT_NAME) {
            if default.concurrency.is_some() {
                concurrency = default.concurrency;
            }
            if default.queues.is_some() {
                queues = default.queues;
            }
        }
        Ok(Settings {
            environment: String::new(),
            config_file: None,
            concurrency,
            queues,
            timeout: self
                .timeout
                .map(|secs| positive_secs(secs, "timeout"))
                .transpose()?,
            tag: self.tag,
            labels: self.labels.unwrap_or_default(),
            verbose: self.verbose.unwrap_or(false),
            capsules,
            max_retries: self.max_retries,
            max_recoveries: self.max_recoveries,
            dead_max_jobs: self
                .dead_max_jobs
                .map(|value| positive(value, "dead_max_jobs"))
                .transpose()?,
            dead_timeout: self
                .dead_timeout
                .map(|secs| positive_secs(secs, "dead_timeout"))
                .transpose()?,
            max_iteration_runtime: self
                .max_iteration_runtime
                .map(|secs| seconds(secs, "max_iteration_runtime"))
                .transpose()?,
            poll_interval_average: self
                .poll_interval_average
                .map(|secs| seconds(secs, "poll_interval_average"))
                .transpose()?,
            average_scheduled_poll_interval: self
                .average_scheduled_poll_interval
                .map(|secs| seconds(secs, "average_scheduled_poll_interval"))
                .transpose()?,
            fetch,
            metrics: self.metrics,
            job_logging: self.job_logging,
            unique_jobs: self.unique_jobs,
            periodic_group: self.periodic_group,
            periodic_poller: self.periodic_poller,
            periodic_poll_interval: self
                .periodic_poll_interval
                .map(|secs| seconds(secs, "periodic_poll_interval"))
                .transpose()?,
            periodic_history: self
                .periodic_history
                .map(|value| non_negative(value, "periodic_history"))
                .transpose()?,
            periodic_lease: self
                .periodic_lease
                .map(|secs| seconds(secs, "periodic_lease"))
                .transpose()?,
            batch_ttl: self
                .batch_ttl
                .map(|secs| positive_secs(secs, "batch_ttl"))
                .transpose()?,
            batch_linger: self
                .batch_linger
                .map(|secs| positive_secs(secs, "batch_linger"))
                .transpose()?,
            batch_max_commit_jobs: self
                .batch_max_commit_jobs
                .map(|value| positive(value, "batch_max_commit_jobs"))
                .transpose()?,
            batch_maintenance: self.batch_maintenance,
            throttles,
            queue_limits,
            #[cfg(feature = "periodic")]
            periodic,
            warnings: Vec::new(),
            section_applied: false,
        })
    }
}

/// `error` (from reading a configuration file), with a hint when it is a number written as a
/// string: TOML's message names the type expected (`expected f64`), not that times are
/// numbers of seconds.
fn with_unit_hint(error: String) -> String {
    const NUMBERS: [&str; 7] = [
        "expected i64",
        "expected f64",
        "expected u64",
        "expected u32",
        "expected i32",
        "expected usize",
        "expected a number",
    ];
    if error.contains("invalid type: string") && NUMBERS.iter().any(|kind| error.contains(kind)) {
        format!(
            "{error}\n(numbers are written without quotes, and times are numbers of seconds: \
             `30` or `0.5`, not `\"30s\"`)"
        )
    } else {
        error
    }
}

/// A non-negative integer setting.
fn non_negative(value: i64, name: &str) -> std::result::Result<usize, String> {
    usize::try_from(value)
        .map_err(|_| format!("{name}: {value} is not a valid value (it must be at least 0)"))
}

/// A positive integer setting.
fn positive(value: i64, name: &str) -> std::result::Result<usize, String> {
    match usize::try_from(value) {
        Ok(value) if value > 0 => Ok(value),
        _ => Err(format!(
            "{name}: {value} is not a valid value (it must be at least 1)"
        )),
    }
}

/// A positive whole number of seconds.
fn positive_secs(value: i64, name: &str) -> std::result::Result<Duration, String> {
    match u64::try_from(value) {
        Ok(secs) if secs > 0 => Ok(Duration::from_secs(secs)),
        _ => Err(format!(
            "{name}: {value} is not a valid value (it must be at least 1 second)"
        )),
    }
}

/// A positive number of seconds.
fn seconds(value: f64, name: &str) -> std::result::Result<Duration, String> {
    match Duration::try_from_secs_f64(value) {
        Ok(duration) if !duration.is_zero() => Ok(duration),
        _ => Err(format!("{name}: {value} is not a valid number of seconds")),
    }
}

/// Queues given as `"name"`, `"name,weight"`, `["name"]` or `["name", weight]`, as
/// `"name"`/`"name,weight"` strings.
fn queue_list(values: &[toml::Value], name: &str) -> std::result::Result<Vec<String>, String> {
    values
        .iter()
        .map(|value| {
            let queue = match value {
                toml::Value::String(queue) => queue.clone(),
                toml::Value::Array(pair) => match pair.as_slice() {
                    [toml::Value::String(queue)] => queue.clone(),
                    [toml::Value::String(queue), toml::Value::Integer(weight)] => {
                        format!("{queue},{weight}")
                    }
                    _ => return Err(queue_error(name, value)),
                },
                _ => return Err(queue_error(name, value)),
            };
            parse_queue(&queue).map_err(|error| format!("{name}: {error}"))
        })
        .collect()
}

fn queue_error(name: &str, value: &toml::Value) -> String {
    format!(
        "{name}: `{value}` is not a queue (use \"name\", \"name,weight\", [\"name\"] or \
         [\"name\", weight])"
    )
}

/// Validates a `NAME[,WEIGHT]` queue (the value parser of `-q`).
fn parse_queue(value: &str) -> std::result::Result<String, String> {
    let (name, weight) = match value.split_once(',') {
        Some((name, weight)) => (name.trim(), Some(weight.trim())),
        None => (value.trim(), None),
    };
    if name.is_empty() {
        return Err(format!("`{value}` names no queue"));
    }
    match weight {
        None => Ok(name.to_owned()),
        Some(weight) => match weight.parse::<u32>() {
            Ok(weight) if weight <= CapsuleConfig::MAX_WEIGHT => Ok(format!("{name},{weight}")),
            _ => Err(format!(
                "invalid weight in `{value}`: weights are integers from 0 to {}",
                CapsuleConfig::MAX_WEIGHT
            )),
        },
    }
}

/// The value of `-c`: at least 1.
fn parse_concurrency(value: &str) -> std::result::Result<usize, String> {
    let parsed: i64 = value
        .trim()
        .parse()
        .map_err(|_| format!("`{value}` is not a number"))?;
    positive(parsed, "concurrency")
}

/// The value of `-t`: at least 1 second.
fn parse_timeout(value: &str) -> std::result::Result<u64, String> {
    let parsed: i64 = value
        .trim()
        .parse()
        .map_err(|_| format!("`{value}` is not a number of seconds"))?;
    positive_secs(parsed, "timeout").map(|timeout| timeout.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn feature_settings() {
        let settings = Settings::from_toml(
            r#"
            unique_jobs = false
            periodic_group = "billing"
            periodic_poller = false
            periodic_poll_interval = 2.5
            periodic_history = 0
            batch_ttl = 7200
            batch_linger = 60
            batch_max_commit_jobs = 500
            batch_maintenance = false
            "#,
            "development",
        )
        .unwrap();
        assert_eq!(settings.unique_jobs, Some(false));
        assert_eq!(settings.periodic_group.as_deref(), Some("billing"));
        assert_eq!(settings.periodic_poller, Some(false));
        assert_eq!(
            settings.periodic_poll_interval,
            Some(Duration::from_millis(2_500))
        );
        assert_eq!(settings.periodic_history, Some(0));
        assert_eq!(settings.batch_ttl, Some(Duration::from_secs(7_200)));
        assert_eq!(settings.batch_linger, Some(Duration::from_secs(60)));
        assert_eq!(settings.batch_max_commit_jobs, Some(500));
        assert_eq!(settings.batch_maintenance, Some(false));
        let config = settings
            .apply(crate::Hench::builder())
            .config_for_test()
            .unwrap();
        assert!(!config.unique_jobs);
        assert_eq!(config.periodic_group.as_deref(), Some("billing"));
        assert_eq!(config.batch_max_commit_jobs, 500);
    }

    #[test]
    fn recovery_and_lease_settings() {
        let settings = Settings::from_toml(
            "max_recoveries = 5\nperiodic_lease = 30.0\nperiodic_poll_interval = 2.0",
            "development",
        )
        .unwrap();
        assert_eq!(settings.max_recoveries, Some(5));
        assert_eq!(settings.periodic_lease, Some(Duration::from_secs(30)));
        let config = settings
            .apply(crate::Hench::builder())
            .config_for_test()
            .unwrap();
        assert_eq!(config.max_recoveries, 5);
        assert_eq!(config.periodic_lease, Duration::from_secs(30));
        // 0: a job whose process died never runs again.
        let settings = Settings::from_toml("max_recoveries = 0", "development").unwrap();
        assert_eq!(settings.max_recoveries, Some(0));
        for (text, expected) in [
            ("max_recoveries = -1", "max_recoveries"),
            ("periodic_lease = 0", "periodic_lease"),
        ] {
            let error = Settings::from_toml(text, "development").unwrap_err();
            assert!(error.contains(expected), "{text}: {error}");
        }
    }

    /// Round 3 (docs-usability-8): a schedule of the file named like one defined in code is
    /// refused, in either order, with both origins named (it does not replace the code's).
    #[cfg(feature = "periodic")]
    #[test]
    fn a_file_schedule_named_like_a_code_schedule_is_refused_with_both_origins() {
        let settings = Settings::from_toml(
            "[[periodic]]\nname = \"nightly\"\nevery = 60\nclass = \"Report\"",
            "development",
        )
        .unwrap();
        let code = || {
            crate::periodic::Periodic::every("nightly", Duration::from_secs(60))
                .class("Report", Vec::new())
        };
        for builder in [
            settings.apply(crate::Hench::builder().periodic(code())),
            settings.apply(crate::Hench::builder()).periodic(code()),
        ] {
            let error = builder.config_for_test().unwrap_err().to_string();
            assert!(
                error.contains("in code") && error.contains("configuration file"),
                "{error}"
            );
        }
        // Alone, the file's schedule is fine.
        assert!(
            settings
                .apply(crate::Hench::builder())
                .config_for_test()
                .is_ok()
        );
    }

    #[test]
    fn times_written_as_strings_are_refused_with_their_unit() {
        for text in [
            "timeout = \"30s\"",
            "periodic_poll_interval = \"5s\"",
            "batch_ttl = \"30d\"",
            "[throttles.erp]\nlimits = [\"concurrency=1\"]\nmax_delay = \"1h\"",
            #[cfg(feature = "periodic")]
            "[[periodic]]\nname = \"a\"\nevery = \"30s\"\nclass = \"A\"",
        ] {
            let error = Settings::from_toml(text, "development").unwrap_err();
            assert!(
                error.contains("times are numbers of seconds"),
                "{text}: {error}"
            );
        }
        // Other mistakes get no such hint.
        let error = Settings::from_toml("fetch = 3", "development").unwrap_err();
        assert!(!error.contains("numbers of seconds"), "{error}");
    }

    #[test]
    fn feature_tables_are_checked_and_carried_to_the_builder() {
        let settings = Settings::from_toml(
            r#"
            [throttles.stripe]
            limits = ["concurrency=1"]

            [[periodic]]
            name = "nightly"
            cron = "0 2 * * *"
            class = "reports::Nightly"

            [production.throttles.stripe]
            limits = ["concurrency=25", "window=5/1s per key"]
            reschedules = 50
            "#,
            "production",
        )
        .unwrap();
        // The section's table replaces the top level's (a shallow merge, as for every setting).
        assert_eq!(settings.throttles.len(), 1);
        assert_eq!(settings.throttles[0].name(), "stripe");
        assert_eq!(settings.throttles[0].limits().len(), 2);
        #[cfg(feature = "periodic")]
        assert_eq!(settings.periodic.len(), 1);
        let config = settings
            .apply(crate::Hench::builder().throttle(crate::Throttle::new("stripe").concurrency(3)))
            .config_for_test()
            .unwrap();
        // The file's throttle comes last: it replaces the code's of the same name.
        let throttles = crate::limit::Throttles::new(config.throttles.iter().cloned());
        assert_eq!(
            throttles
                .get("stripe")
                .map(|throttle| throttle.limits().len()),
            Some(2)
        );
        #[cfg(feature = "periodic")]
        assert_eq!(config.periodic.len(), 1);

        for (text, expected) in [
            #[cfg(not(feature = "periodic"))]
            (
                "[[periodic]]\nname = \"a\"",
                "needs hench's `periodic` cargo feature",
            ),
            #[cfg(feature = "periodic")]
            ("[[periodic]]\nname = \"a\"", "periodic[0]"),
            (
                "[throttles.stripe]\nlimits = [\"rate=1\"]",
                "throttles.stripe",
            ),
            (
                "[queue_limits]\nexports = \"c=3;c=4\"",
                "queue_limits.exports",
            ),
            ("[staging.throttles.x]\nlimits = []", "throttles.x"),
            ("periodic_group = \" padded\"", "periodic_group"),
            ("periodic_history = -1", "periodic_history"),
        ] {
            let error = Settings::from_toml(text, "staging").unwrap_err();
            assert!(error.contains(expected), "{text}: {error}");
        }
        for environment in ["throttles", "queue_limits", "periodic"] {
            let error = Settings::from_toml("tag = \"x\"", environment).unwrap_err();
            assert!(error.contains("cannot name an environment"), "{error}");
        }
        // The feature tables' errors are not wrapped twice (the caller names the file and
        // adds the kind of error once).
        for text in [
            "[queue_limits]\nexports = \"c=3;c=4\"",
            "[throttles.stripe]\nlimits = [\"rate=1\"]",
        ] {
            let error = Settings::from_toml(text, "staging").unwrap_err();
            assert!(!error.contains("configuration error"), "{text}: {error}");
        }
    }
    use clap::Parser;

    fn parse(args: &[&str]) -> WorkerArgs {
        WorkerArgs::try_parse_from(std::iter::once("worker").chain(args.iter().copied())).unwrap()
    }

    fn parse_error(args: &[&str]) -> String {
        WorkerArgs::try_parse_from(std::iter::once("worker").chain(args.iter().copied()))
            .unwrap_err()
            .to_string()
    }

    fn no_env(_: &str) -> Option<String> {
        None
    }

    #[test]
    fn options() {
        let args = parse(&[
            "-c", "60", "-q", "foo", "-q", "bar,3", "-t", "30", "-g", "app", "-e", "staging", "-v",
            "-C", "x.toml",
        ]);
        assert_eq!(args.concurrency, Some(60));
        assert_eq!(args.queues, ["foo", "bar,3"]);
        assert_eq!(args.timeout, Some(30));
        assert_eq!(args.tag.as_deref(), Some("app"));
        assert_eq!(args.environment.as_deref(), Some("staging"));
        assert!(args.verbose);
        assert_eq!(args.config, Some(PathBuf::from("x.toml")));
        let long = parse(&[
            "--concurrency",
            "2",
            "--queue",
            "a",
            "--timeout",
            "1",
            "--verbose",
        ]);
        assert_eq!((long.concurrency, long.timeout), (Some(2), Some(1)));
        assert_eq!(parse(&[]), WorkerArgs::default());
    }

    #[test]
    fn invalid_options_are_rejected() {
        for (args, message) in [
            (&["-c", "0"][..], "concurrency: 0"),
            (&["-c", "-2"][..], "concurrency: -2"),
            (&["-c", "many"][..], "not a number"),
            (&["-t", "0"][..], "timeout: 0"),
            (&["-t", "-2"][..], "timeout: -2"),
            (&["-q", ""][..], "names no queue"),
            (&["-q", ",3"][..], "names no queue"),
            (&["-q", "a,x"][..], "invalid weight"),
            (&["-q", "a,1001"][..], "invalid weight"),
            (&["-r", "x"][..], "unexpected argument"),
        ] {
            let error = parse_error(args);
            assert!(error.contains(message), "{args:?}: {error}");
        }
    }

    #[test]
    fn queue_values() {
        assert_eq!(parse_queue("foo.bar").unwrap(), "foo.bar");
        assert_eq!(parse_queue("queue-two").unwrap(), "queue-two");
        assert_eq!(parse_queue(" a , 2 ").unwrap(), "a,2");
        assert_eq!(parse_queue("a,0").unwrap(), "a,0");
    }

    #[test]
    fn environment_resolution() {
        let directory = tempfile::tempdir().unwrap();
        let settings = parse(&[]).settings_with(directory.path(), no_env).unwrap();
        assert_eq!(settings.environment, "development");
        let env = |name: &str| (name == "APP_ENV").then(|| "xyzzy".to_owned());
        let settings = parse(&[]).settings_with(directory.path(), env).unwrap();
        assert_eq!(settings.environment, "xyzzy");
        // `-e` wins over APP_ENV.
        let settings = parse(&["-e", "snoop"])
            .settings_with(directory.path(), env)
            .unwrap();
        assert_eq!(settings.environment, "snoop");
    }

    #[test]
    fn verbose_and_debug_invocation() {
        let directory = tempfile::tempdir().unwrap();
        let quiet = parse(&[]).settings_with(directory.path(), no_env).unwrap();
        assert_eq!(quiet.log_level(), tracing::Level::INFO);
        assert!(
            quiet
                .log_directives()
                .starts_with("info,[job{log_level=trace}]=trace")
        );
        let verbose = parse(&["-v"])
            .settings_with(directory.path(), no_env)
            .unwrap();
        assert_eq!(verbose.log_level(), tracing::Level::DEBUG);
        assert!(verbose.log_directives().starts_with("debug,"));
        // systemd's RestartMode=debug.
        let debug = |name: &str| (name == "DEBUG_INVOCATION").then(|| "1".to_owned());
        let restarted = parse(&[]).settings_with(directory.path(), debug).unwrap();
        assert!(restarted.verbose);
    }

    #[test]
    fn file_settings() {
        let text = r#"
            concurrency = 50
            queues = [["very_often", 2], ["seldom", 1]]
            timeout = 10
            tag = "tag"
            labels = ["a", "b"]
            verbose = false
            max_retries = 3
            dead_max_jobs = 7
            dead_timeout = 60
            max_iteration_runtime = 1.5
            poll_interval_average = 2.0
            average_scheduled_poll_interval = 15
            fetch = "basic"
            metrics = false
            job_logging = false

            [capsules.binary]
            concurrency = 2
            queues = ["sirius", "sirius_b"]

            [capsules.non_concurrent]
            queues = ["non_concurrent"]
            concurrency = 1
        "#;
        let settings = Settings::from_toml(text, "development").unwrap();
        assert_eq!(settings.concurrency, Some(50));
        assert_eq!(
            settings.queues,
            Some(vec!["very_often,2".to_owned(), "seldom,1".to_owned()])
        );
        assert_eq!(settings.timeout, Some(Duration::from_secs(10)));
        assert_eq!(settings.tag.as_deref(), Some("tag"));
        assert_eq!(settings.labels, ["a", "b"]);
        assert!(!settings.verbose);
        assert_eq!(settings.max_retries, Some(3));
        assert_eq!(settings.dead_max_jobs, Some(7));
        assert_eq!(settings.dead_timeout, Some(Duration::from_secs(60)));
        assert_eq!(
            settings.max_iteration_runtime,
            Some(Duration::from_millis(1_500))
        );
        assert_eq!(settings.poll_interval_average, Some(Duration::from_secs(2)));
        assert_eq!(
            settings.average_scheduled_poll_interval,
            Some(Duration::from_secs(15))
        );
        assert_eq!(settings.fetch, Some(FetchStrategy::Basic));
        assert_eq!(settings.metrics, Some(false));
        assert_eq!(settings.job_logging, Some(false));
        assert_eq!(settings.capsules["binary"].concurrency, Some(2));
        assert_eq!(
            settings.capsules["binary"].queues,
            Some(vec!["sirius".to_owned(), "sirius_b".to_owned()])
        );
        assert_eq!(settings.capsules["non_concurrent"].concurrency, Some(1));
    }

    #[test]
    fn queue_forms() {
        let settings =
            Settings::from_toml(r#"queues = ["a", "b,3", ["c"], ["d", 4]]"#, "development")
                .unwrap();
        assert_eq!(
            settings.queues,
            Some(vec![
                "a".to_owned(),
                "b,3".to_owned(),
                "c".to_owned(),
                "d,4".to_owned()
            ])
        );
        let empty = Settings::from_toml("", "development").unwrap();
        assert_eq!(empty, Settings::default());
    }

    #[test]
    fn environment_sections() {
        let text = r#"
            concurrency = 50
            queues = ["base"]

            [staging]
            concurrency = 10
            verbose = true

            [production]
            concurrency = 99
        "#;
        let staging = Settings::from_toml(text, "staging").unwrap();
        assert_eq!(staging.concurrency, Some(10));
        assert!(staging.verbose);
        assert_eq!(staging.queues, Some(vec!["base".to_owned()]));
        let development = Settings::from_toml(text, "development").unwrap();
        assert_eq!(development.concurrency, Some(50));
        assert!(!development.verbose);
    }

    #[test]
    fn invalid_files_are_rejected() {
        for (text, message) in [
            ("concurrency = 0", "concurrency: 0"),
            ("concurrency = -2", "concurrency: -2"),
            ("timeout = 0", "timeout: 0"),
            ("concurrency = \"many\"", "invalid type"),
            ("queues = [1]", "is not a queue"),
            ("queues = [[\"a\", \"b\"]]", "is not a queue"),
            ("queues = [\"a,x\"]", "invalid weight"),
            ("unknown = 1", "unknown field `unknown`"),
            ("fetch = \"super\"", "not a fetch strategy"),
            ("max_iteration_runtime = 0.0", "max_iteration_runtime"),
            ("max_iteration_runtime = -1.0", "max_iteration_runtime"),
            ("dead_max_jobs = 0", "dead_max_jobs"),
            ("[capsules.x]\nconcurrency = 0", "capsules.x.concurrency"),
            ("[capsules.x]\nqueue = [\"a\"]", "unknown field `queue`"),
            ("development = 3", "must be a table"),
            ("[development.nested]\nconcurrency = 1", "cannot be nested"),
            ("concurrency = ", "expected"),
            (
                "average_scheduled_poll_interval = 0",
                "average_scheduled_poll_interval",
            ),
            (
                "average_scheduled_poll_interval = -5.0",
                "average_scheduled_poll_interval",
            ),
        ] {
            let error = Settings::from_toml(text, "development").unwrap_err();
            assert!(error.contains(message), "{text}: {error}");
        }
    }

    /// Every part of the file is checked, the sections of the environments not selected
    /// included, so that a mistake in the production section fails in development already.
    #[test]
    fn every_section_is_checked() {
        for (text, messages) in [
            (
                "concurrency = 10\n[production]\nconcurency = 30",
                &[
                    "in the `[production]` section",
                    "unknown field `concurency`",
                ][..],
            ),
            (
                "[staging]\nconcurrency = 0",
                &["in the `[staging]` section", "concurrency: 0"],
            ),
            (
                "[staging]\nqueues = [\"a,x\"]",
                &["in the `[staging]` section", "invalid weight"],
            ),
            (
                "[staging.capsules.x]\nconcurrency = 0",
                &["in the `[staging]` section", "capsules.x.concurrency"],
            ),
            // A setting's table misspelled at the top level is no environment section.
            (
                "concurrency = 10\n[capsule.serial]\nconcurrency = 1\nqueues = [\"serial\"]",
                &[
                    "unknown table `serial` in the `[capsule]` section",
                    "did you mean `[capsules.serial]`?",
                ],
            ),
            (
                "[Capsules.serial]\nconcurrency = 1",
                &["did you mean `[capsules.serial]`?"],
            ),
            (
                "[production.nested]\nconcurrency = 1",
                &["cannot be nested"],
            ),
        ] {
            let error = Settings::from_toml(text, "development").unwrap_err();
            for message in messages {
                assert!(error.contains(message), "{text}: {error}");
            }
        }
        // A section for settings of its own is fine, selected or not.
        let text = "[production.capsules.serial]\nconcurrency = 1\n[staging]\ntag = \"s\"";
        Settings::from_toml(text, "development").unwrap();
        let production = Settings::from_toml(text, "production").unwrap();
        assert_eq!(production.capsules["serial"].concurrency, Some(1));
    }

    #[test]
    fn near_names() {
        assert!(is_near("capsule", "capsules"));
        assert!(is_near("capsuls", "capsules"));
        assert!(is_near("Capsules", "capsules"));
        assert!(is_near("capsulez", "capsules"));
        assert!(!is_near("production", "capsules"));
        assert!(!is_near("caps", "capsules"));
    }

    /// An environment chosen with `-e` or `APP_ENV` that selects nothing is a warning: no
    /// configuration file was read (the default file is looked up in the current directory),
    /// or the file has no section for it; so is a section whose name differs by case.
    #[test]
    fn environments_that_select_nothing_are_warned_about() {
        let directory = tempfile::tempdir().unwrap();
        let settings = parse(&["-e", "production"])
            .settings_with(directory.path(), no_env)
            .unwrap();
        assert_eq!(settings.warnings.len(), 1, "{:?}", settings.warnings);
        assert!(
            settings.warnings[0].contains("`production` (from -e)")
                && settings.warnings[0].contains("no configuration file was read")
                && settings.warnings[0].contains(&directory.path().display().to_string()),
            "{:?}",
            settings.warnings
        );
        let app_env = |name: &str| (name == "APP_ENV").then(|| "staging".to_owned());
        let settings = parse(&[]).settings_with(directory.path(), app_env).unwrap();
        assert!(
            settings.warnings[0].contains("`staging` (from APP_ENV)"),
            "{:?}",
            settings.warnings
        );
        // The default environment selects nothing silently.
        let settings = parse(&[]).settings_with(directory.path(), no_env).unwrap();
        assert!(settings.warnings.is_empty(), "{:?}", settings.warnings);

        std::fs::create_dir(directory.path().join("config")).unwrap();
        let file = directory.path().join(DEFAULT_CONFIG_FILE);
        std::fs::write(&file, "concurrency = 10\n[staging]\nconcurrency = 2\n").unwrap();
        let settings = parse(&["-e", "staging"])
            .settings_with(directory.path(), no_env)
            .unwrap();
        assert!(settings.warnings.is_empty(), "{:?}", settings.warnings);
        assert!(settings.section_applied);
        let settings = parse(&["-e", "production"])
            .settings_with(directory.path(), no_env)
            .unwrap();
        assert!(!settings.section_applied);
        assert!(
            settings.warnings[0].contains("no `[production]` section")
                && settings.warnings[0].contains("top-level settings apply"),
            "{:?}",
            settings.warnings
        );
        assert_eq!(settings.concurrency, Some(10));
        // Without -e or APP_ENV a missing section is not worth a warning...
        let settings = parse(&[]).settings_with(directory.path(), no_env).unwrap();
        assert!(settings.warnings.is_empty(), "{:?}", settings.warnings);
        // ...unless one differs from the environment only by case.
        std::fs::write(&file, "[Development]\nconcurrency = 2\n").unwrap();
        let settings = parse(&[]).settings_with(directory.path(), no_env).unwrap();
        assert!(
            settings.warnings[0].contains("`[Development]`")
                && settings.warnings[0].contains("case-sensitive"),
            "{:?}",
            settings.warnings
        );
    }

    #[test]
    fn command_line_overrides_the_file() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("hench.toml");
        std::fs::write(
            &path,
            "concurrency = 50\nqueues = [[\"very_often\", 2], [\"seldom\", 1]]\ntag = \"file\"\n\
             timeout = 9\n",
        )
        .unwrap();
        let path_arg = path.to_str().unwrap();
        let settings = parse(&[
            "-C", path_arg, "-e", "snoop", "-c", "100", "-q", "often,7", "-q", "seldom,3",
        ])
        .settings_with(directory.path(), no_env)
        .unwrap();
        assert_eq!(settings.config_file.as_deref(), Some(path.as_path()));
        assert_eq!(settings.environment, "snoop");
        assert_eq!(settings.concurrency, Some(100));
        assert_eq!(
            settings.queues,
            Some(vec!["often,7".to_owned(), "seldom,3".to_owned()])
        );
        // What the command line leaves out comes from the file.
        assert_eq!(settings.tag.as_deref(), Some("file"));
        assert_eq!(settings.timeout, Some(Duration::from_secs(9)));
    }

    #[test]
    fn config_files() {
        let directory = tempfile::tempdir().unwrap();
        // A missing file named with -C is an error.
        let missing = directory.path().join("missing.toml");
        let error = parse(&["-C", missing.to_str().unwrap()])
            .settings_with(directory.path(), no_env)
            .unwrap_err();
        assert!(
            error.to_string().contains("no such configuration file"),
            "{error}"
        );
        // Without -C, config/hench.toml is used when it exists.
        let settings = parse(&[]).settings_with(directory.path(), no_env).unwrap();
        assert_eq!(settings.config_file, None);
        std::fs::create_dir(directory.path().join("config")).unwrap();
        std::fs::write(
            directory.path().join(DEFAULT_CONFIG_FILE),
            "concurrency = 25\n",
        )
        .unwrap();
        let settings = parse(&[]).settings_with(directory.path(), no_env).unwrap();
        assert_eq!(settings.concurrency, Some(25));
        assert_eq!(
            settings.config_file,
            Some(directory.path().join(DEFAULT_CONFIG_FILE))
        );
        // Errors name the file.
        std::fs::write(
            directory.path().join(DEFAULT_CONFIG_FILE),
            "concurrency = 0\n",
        )
        .unwrap();
        let error = parse(&[])
            .settings_with(directory.path(), no_env)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("hench.toml") && error.contains("concurrency: 0"),
            "{error}"
        );
    }
}
