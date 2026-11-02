//! The worker command line and configuration file applied to a builder (Sidekiq's `cli_test`
//! scenarios).
#![cfg(feature = "cli")]

use std::path::Path;
use std::time::Duration;

use clap::Parser;
use hench::cli::WorkerArgs;
use hench::{
    CapsuleConfig, Config, Error, FetchStrategy, Hench, HenchBuilder, QueueMode, RedisPool,
};

fn args(args: &[&str]) -> WorkerArgs {
    WorkerArgs::try_parse_from(std::iter::once("worker").chain(args.iter().copied())).unwrap()
}

/// A builder that never connects to Redis.
fn builder() -> HenchBuilder {
    Hench::builder()
        .redis(RedisPool::detached())
        .auto_register(false)
}

async fn config_with(worker: WorkerArgs, builder: HenchBuilder) -> std::sync::Arc<Config> {
    let builder = worker.apply(builder).unwrap();
    builder.build().await.unwrap().config().clone()
}

async fn configure(worker_args: &[&str]) -> std::sync::Arc<Config> {
    config_with(args(worker_args), builder()).await
}

fn default_capsule(config: &Config) -> &CapsuleConfig {
    config.capsule(CapsuleConfig::DEFAULT_NAME).unwrap()
}

fn count(capsule: &CapsuleConfig, queue: &str) -> usize {
    capsule
        .expanded_queues()
        .into_iter()
        .filter(|name| *name == queue)
        .count()
}

/// Writes a configuration file in `directory`.
fn write(directory: &Path, name: &str, text: &str) -> String {
    let path = directory.join(name);
    std::fs::write(&path, text).unwrap();
    path.to_str().unwrap().to_owned()
}

#[tokio::test]
async fn concurrency() {
    let config = configure(&["-c", "60"]).await;
    assert_eq!(default_capsule(&config).concurrency, 60);
}

#[tokio::test]
async fn queues() {
    let config = configure(&["-q", "foo"]).await;
    assert_eq!(default_capsule(&config).queues, ["foo"]);

    // Without weights: strict order.
    let config = configure(&["-q", "foo", "-q", "bar"]).await;
    let capsule = default_capsule(&config);
    assert_eq!(capsule.queues, ["foo", "bar"]);
    assert_eq!(capsule.mode, QueueMode::Strict);

    // With weights: weighted.
    let config = configure(&["-q", "foo,3", "-q", "bar"]).await;
    let capsule = default_capsule(&config);
    assert_eq!(capsule.expanded_queues(), ["foo", "foo", "foo", "bar"]);
    assert_eq!(capsule.mode, QueueMode::Weighted);

    let config = configure(&["-q", "queue_one", "-q", "queue-two", "-q", "foo.bar"]).await;
    assert_eq!(
        default_capsule(&config).queues,
        ["queue_one", "queue-two", "foo.bar"]
    );
}

#[tokio::test]
async fn queues_default_to_the_builders() {
    let config = configure(&[]).await;
    assert_eq!(default_capsule(&config).queues, ["default"]);

    let directory = tempfile::tempdir().unwrap();
    let empty = write(directory.path(), "empty.toml", "");
    let config = config_with(args(&["-C", &empty]), builder().queues(["critical", "low"])).await;
    assert_eq!(default_capsule(&config).queues, ["critical", "low"]);
}

#[tokio::test]
async fn timeout() {
    let config = configure(&["-t", "30"]).await;
    assert_eq!(config.timeout, Duration::from_secs(30));
}

#[tokio::test]
async fn config_file() {
    let directory = tempfile::tempdir().unwrap();
    let path = write(
        directory.path(),
        "hench.toml",
        "verbose = false\nconcurrency = 50\nqueues = [[\"very_often\", 2], [\"seldom\", 1]]\n",
    );
    let worker = args(&["-C", &path]);
    let settings = worker.settings().unwrap();
    assert_eq!(
        settings.config_file.as_deref(),
        Some(Path::new(path.as_str()))
    );
    assert!(!settings.verbose);
    assert_eq!(settings.environment, "development");
    let config = config_with(worker, builder()).await;
    let capsule = default_capsule(&config);
    assert_eq!(capsule.concurrency, 50);
    assert_eq!(count(capsule, "very_often"), 2);
    assert_eq!(count(capsule, "seldom"), 1);
}

#[tokio::test]
async fn environment_specific_config() {
    let directory = tempfile::tempdir().unwrap();
    let path = write(
        directory.path(),
        "hench.toml",
        "concurrency = 50\n\n[staging]\nverbose = false\nqueues = [[\"very_often\", 2], [\"seldom\", 1]]\n\n\
         [production]\nverbose = true\nconcurrency = 75\n",
    );
    let worker = args(&["-e", "staging", "-C", &path]);
    assert_eq!(worker.settings().unwrap().environment, "staging");
    assert!(!worker.settings().unwrap().verbose);
    let config = config_with(worker, builder()).await;
    let capsule = default_capsule(&config);
    assert_eq!(capsule.concurrency, 50);
    assert_eq!(count(capsule, "very_often"), 2);

    let worker = args(&["-e", "production", "-C", &path]);
    assert!(worker.settings().unwrap().verbose);
    let config = config_with(worker, builder()).await;
    assert_eq!(default_capsule(&config).concurrency, 75);
    assert_eq!(default_capsule(&config).queues, ["default"]);
}

#[tokio::test]
async fn capsules_from_the_config_file() {
    let directory = tempfile::tempdir().unwrap();
    let path = write(
        directory.path(),
        "hench.toml",
        "concurrency = 50\nqueues = [[\"very_often\", 2], [\"seldom\", 1]]\n\n\
         [capsules.non_concurrent]\nqueues = [\"non_concurrent\"]\nconcurrency = 1\n\n\
         [capsules.binary]\nqueues = [\"sirius\", \"sirius_b\"]\nconcurrency = 2\n",
    );
    let config = configure(&["-C", &path]).await;
    let single = config.capsule("non_concurrent").unwrap();
    assert_eq!(single.concurrency, 1);
    assert_eq!(single.queues, ["non_concurrent"]);
    let binary = config.capsule("binary").unwrap();
    assert_eq!(binary.concurrency, 2);
    assert_eq!(binary.queues, ["sirius", "sirius_b"]);
    assert_eq!(config.total_concurrency(), 53);
}

#[tokio::test]
async fn command_line_options_override_the_file() {
    let directory = tempfile::tempdir().unwrap();
    let path = write(
        directory.path(),
        "hench.toml",
        "verbose = false\nconcurrency = 50\nqueues = [[\"very_often\", 2], [\"seldom\", 1]]\n",
    );
    let worker = args(&[
        "-C", &path, "-e", "snoop", "-c", "100", "-q", "often,7", "-q", "seldom,3",
    ]);
    let settings = worker.settings().unwrap();
    assert_eq!(settings.environment, "snoop");
    assert!(!settings.verbose);
    let config = config_with(worker, builder()).await;
    let capsule = default_capsule(&config);
    assert_eq!(capsule.concurrency, 100);
    assert_eq!(count(capsule, "often"), 7);
    assert_eq!(count(capsule, "seldom"), 3);
    assert_eq!(count(capsule, "very_often"), 0);
}

/// `[capsules.default]` configures the default capsule, which `-c` and `-q` set too: the
/// command line wins over it, as over every setting of the file.
#[tokio::test]
async fn command_line_options_override_the_default_capsule_of_the_file() {
    let directory = tempfile::tempdir().unwrap();
    let path = write(
        directory.path(),
        "hench.toml",
        "[capsules.default]\nconcurrency = 2\nqueues = [\"from_file\"]\n\n\
         [capsules.serial]\nconcurrency = 1\nqueues = [\"serial\"]\n",
    );
    let config = configure(&["-C", &path, "-c", "50", "-q", "from_cli"]).await;
    let capsule = default_capsule(&config);
    assert_eq!(capsule.concurrency, 50);
    assert_eq!(capsule.queues, ["from_cli"]);
    // The other capsules are untouched.
    let serial = config.capsule("serial").unwrap();
    assert_eq!(
        (serial.concurrency, serial.queues.as_slice()),
        (1, &["serial".to_owned()][..])
    );

    // Only one of the two options on the command line: the other comes from the file.
    let config = configure(&["-C", &path, "-c", "50"]).await;
    let capsule = default_capsule(&config);
    assert_eq!(capsule.concurrency, 50);
    assert_eq!(capsule.queues, ["from_file"]);
    let config = configure(&["-C", &path, "-q", "from_cli"]).await;
    let capsule = default_capsule(&config);
    assert_eq!(capsule.concurrency, 2);
    assert_eq!(capsule.queues, ["from_cli"]);

    // Without command-line options the file applies, and `[capsules.default]` wins over the
    // top-level keys (it names the capsule they configure).
    let both = write(
        directory.path(),
        "both.toml",
        "concurrency = 9\nqueues = [\"top\"]\ntimeout = 7\n\n\
         [capsules.default]\nconcurrency = 2\n",
    );
    let worker = args(&["-C", &both]);
    let settings = worker.settings().unwrap();
    assert_eq!(settings.concurrency, Some(2));
    assert_eq!(settings.queues, Some(vec!["top".to_owned()]));
    assert!(!settings.capsules.contains_key(CapsuleConfig::DEFAULT_NAME));
    let config = config_with(worker, builder()).await;
    let capsule = default_capsule(&config);
    assert_eq!(capsule.concurrency, 2);
    assert_eq!(capsule.queues, ["top"]);
    assert_eq!(config.timeout, Duration::from_secs(7));
    assert_eq!(config.capsules.len(), 1);

    // Settings built by hand with an entry for the default capsule: the top-level settings
    // (where the command line goes) still win.
    let mut settings = hench::cli::Settings::default();
    let mut by_hand = hench::cli::CapsuleSettings::default();
    by_hand.concurrency = Some(3);
    by_hand.queues = Some(vec!["by_hand".to_owned()]);
    settings
        .capsules
        .insert(CapsuleConfig::DEFAULT_NAME.to_owned(), by_hand);
    settings.concurrency = Some(4);
    let hench = settings.apply(builder()).build().await.unwrap();
    let capsule = default_capsule(hench.config());
    assert_eq!(capsule.concurrency, 4);
    assert_eq!(capsule.queues, ["by_hand"]);
}

#[tokio::test]
async fn the_file_overrides_the_builder() {
    let directory = tempfile::tempdir().unwrap();
    let path = write(
        directory.path(),
        "hench.toml",
        "timeout = 10\nconcurrency = 50\ntag = \"tag\"\nlabels = [\"blue\"]\nqueues = [[\"often\"], [\"seldom\"]]\n\
         fetch = \"basic\"\nmax_retries = 3\nmetrics = false\nmax_iteration_runtime = 2.5\n",
    );
    let config = config_with(
        args(&["-C", &path]),
        builder()
            .concurrency(3)
            .tag("builder")
            .label("green")
            .max_retries(10)
            .dead_max_jobs(42),
    )
    .await;
    let capsule = default_capsule(&config);
    assert_eq!(capsule.concurrency, 50);
    assert_eq!(capsule.queues, ["often", "seldom"]);
    assert_eq!(capsule.mode, QueueMode::Strict);
    assert_eq!(config.timeout, Duration::from_secs(10));
    assert_eq!(config.tag.as_deref(), Some("tag"));
    assert_eq!(config.labels, ["green", "blue"]);
    assert_eq!(config.fetch, FetchStrategy::Basic);
    assert_eq!(config.max_retries, 3);
    assert!(!config.metrics);
    assert_eq!(
        config.max_iteration_runtime,
        Some(Duration::from_millis(2_500))
    );
    // What the file does not set keeps the builder's value.
    assert_eq!(config.dead_max_jobs, 42);
}

/// Sidekiq reads `average_scheduled_poll_interval` from its configuration file too: the knob
/// that scales the scheduler's polling with the number of processes.
#[tokio::test]
async fn the_file_sets_the_average_scheduled_poll_interval() {
    let directory = tempfile::tempdir().unwrap();
    let path = write(
        directory.path(),
        "hench.toml",
        "average_scheduled_poll_interval = 15\n[production]\naverage_scheduled_poll_interval = 2.5\n",
    );
    let config = config_with(args(&["-C", &path]), builder()).await;
    assert_eq!(
        config.average_scheduled_poll_interval,
        Duration::from_secs(15)
    );
    // It scales with the processes; a fixed `poll_interval_average` stays unset.
    assert_eq!(config.poll_interval_average, None);
    let config = config_with(args(&["-C", &path, "-e", "production"]), builder()).await;
    assert_eq!(
        config.average_scheduled_poll_interval,
        Duration::from_millis(2_500)
    );
}

/// A mistake anywhere in the file is reported whatever the environment: here in the section
/// of an environment that is not selected, and a capsule table misspelled at the top level
/// (it would have been taken for an environment's section and ignored).
#[tokio::test]
async fn every_part_of_the_file_is_checked() {
    let directory = tempfile::tempdir().unwrap();
    for (text, expected) in [
        (
            "concurrency = 10\n[production]\nconcurency = 30\n",
            ["`[production]`", "unknown field `concurency`"],
        ),
        (
            "concurrency = 10\n[capsule.serial]\nconcurrency = 1\nqueues = [\"serial\"]\n",
            [
                "unknown table `serial`",
                "did you mean `[capsules.serial]`?",
            ],
        ),
    ] {
        let path = write(directory.path(), "hench.toml", text);
        match args(&["-C", &path]).settings() {
            Err(Error::Config(message)) => {
                for part in expected {
                    assert!(message.contains(part), "{text}: {message}");
                }
                assert!(message.contains("hench.toml"), "{message}");
            }
            other => panic!("{text}: {other:?}"),
        }
    }
}

/// An environment chosen with `-e` that selects nothing is a warning (`Settings::apply` logs
/// it): no configuration file was read — the default file is looked up in the current
/// directory, which a systemd unit without `WorkingDirectory=` sets to `/` — or the file has
/// no section for it (or one whose name differs by case).
#[test]
fn environments_that_select_nothing_are_warned_about() {
    // The tests run in the crate's directory, which has no config/hench.toml.
    let settings = args(&["-e", "production"]).settings().unwrap();
    assert_eq!(settings.config_file, None);
    assert!(
        settings
            .warnings
            .iter()
            .any(|warning| warning.contains("no configuration file was read")),
        "{:?}",
        settings.warnings
    );
    let directory = tempfile::tempdir().unwrap();
    let path = write(
        directory.path(),
        "cased.toml",
        "[Production]\nconcurrency = 25\n",
    );
    let settings = args(&["-C", &path, "-e", "production"]).settings().unwrap();
    assert_eq!(settings.concurrency, None);
    assert!(
        settings
            .warnings
            .iter()
            .any(|warning| warning.contains("`[Production]`")),
        "{:?}",
        settings.warnings
    );
    let path = write(
        directory.path(),
        "fine.toml",
        "[production]\nconcurrency = 25\n",
    );
    let settings = args(&["-C", &path, "-e", "production"]).settings().unwrap();
    assert_eq!(settings.concurrency, Some(25));
    assert!(settings.warnings.is_empty(), "{:?}", settings.warnings);
}

#[tokio::test]
async fn invalid_settings_are_reported() {
    let directory = tempfile::tempdir().unwrap();
    let missing = directory.path().join("missing.toml");
    let result = args(&["-C", missing.to_str().unwrap()]).apply(builder());
    assert!(matches!(result, Err(Error::Config(ref message)) if message.contains("missing.toml")));

    let bad = write(directory.path(), "bad.toml", "concurrency = -2\n");
    let result = args(&["-C", &bad]).apply(builder());
    assert!(
        matches!(result, Err(Error::Config(ref message)) if message.contains("concurrency: -2"))
    );

    let syntax = write(directory.path(), "syntax.toml", "queues = [\n");
    let result = args(&["-C", &syntax]).apply(builder());
    assert!(matches!(result, Err(Error::Config(ref message)) if message.contains("syntax.toml")));

    for invalid in [&["-c", "0"][..], &["-c", "-2"], &["-t", "0"], &["-t", "-2"]] {
        let parsed =
            WorkerArgs::try_parse_from(std::iter::once("worker").chain(invalid.iter().copied()));
        assert!(parsed.is_err(), "{invalid:?}");
    }
}

#[test]
fn help_lists_the_options() {
    let help = <WorkerArgs as clap::CommandFactory>::command()
        .render_help()
        .to_string();
    for option in [
        "--concurrency",
        "--queue",
        "--timeout",
        "--config",
        "--environment",
        "--tag",
        "--verbose",
    ] {
        assert!(help.contains(option), "{option} missing from:\n{help}");
    }
}

/// A job type with options of its own, as `#[hench::job(queue = "reports", retry = 3,
/// unique_for = 3600, unique_by = [day])]` declares them.
#[cfg(feature = "periodic")]
#[derive(Default)]
struct NightlyReport;

#[cfg(feature = "periodic")]
impl hench::Job for NightlyReport {
    const NAME: &'static str = "nightly_report";
    type Args = (i64,);

    fn options() -> hench::JobOptions {
        hench::JobOptions::new()
            .queue("reports")
            .retry(3)
            .unique_for(Duration::from_secs(3_600))
    }

    fn unique_key((day,): &Self::Args) -> Option<serde_json::Value> {
        Some(serde_json::json!({ "day": day }))
    }

    async fn perform(
        &self,
        _ctx: &hench::JobContext,
        _args: Self::Args,
    ) -> Result<(), hench::JobError> {
        Ok(())
    }
}

/// A job type whose web handlers want to be told about duplicates.
#[cfg(feature = "periodic")]
#[derive(Default)]
struct StrictSync;

#[cfg(feature = "periodic")]
impl hench::Job for StrictSync {
    const NAME: &'static str = "strict_sync";
    type Args = ();

    fn options() -> hench::JobOptions {
        hench::JobOptions::new()
            .unique_for(Duration::from_secs(600))
            .on_conflict(hench::unique::OnConflict::Error)
    }

    async fn perform(
        &self,
        _ctx: &hench::JobContext,
        _args: Self::Args,
    ) -> Result<(), hench::JobError> {
        Ok(())
    }
}

#[cfg(feature = "periodic")]
#[tokio::test]
async fn file_schedules_take_the_options_of_the_job_types_they_name() {
    // docs-usability-4: a `[[periodic]]` table named its job by class and was built without the
    // job type's options: its jobs went to `queue:default` with 25 retries and no uniqueness,
    // where workers polling the job type's queue never ran them.
    let directory = tempfile::tempdir().unwrap();
    let path = write(
        directory.path(),
        "hench.toml",
        r#"
[[periodic]]
name = "nightly"
every = 5
class = "nightly_report"
args = [7]

[[periodic]]
name = "urgent"
every = 5
class = "nightly_report"
args = [8]
queue = "urgent"
retry = false

[[periodic]]
name = "elsewhere"
every = 5
class = "billing::Close"
"#,
    );
    let config = config_with(args(&["-C", &path]), builder().register(NightlyReport)).await;
    let job = |name: &str| {
        config
            .periodic
            .iter()
            .find(|periodic| periodic.name() == name)
            .unwrap()
            .payload(std::time::UNIX_EPOCH)
            .unwrap()
    };
    // The job type's options, and its typed unique key of the table's arguments.
    let nightly = job("nightly");
    assert_eq!(nightly.queue, "reports");
    assert_eq!(nightly.retry, Some(hench::RetryOption::Count(3)));
    let unique = nightly.unique.as_ref().and_then(|unique| unique.settings());
    assert_eq!(
        unique.and_then(|unique| unique.key.clone()),
        Some(serde_json::json!({ "day": 7 }))
    );
    // The table's own keys win.
    let urgent = job("urgent");
    assert_eq!(urgent.queue, "urgent");
    assert_eq!(urgent.retry, Some(hench::RetryOption::Bool(false)));
    // A class this process does not register keeps the table's keys alone.
    let elsewhere = job("elsewhere");
    assert_eq!(elsewhere.queue, "");
    assert!(elsewhere.unique.is_none());

    // Arguments that do not fit the job type: refused at build, naming the schedule.
    let misfit = write(
        directory.path(),
        "misfit.toml",
        "[[periodic]]\nname = \"misfit\"\nevery = 5\nclass = \"nightly_report\"\nargs = [\"x\"]\n",
    );
    let error = args(&["-C", &misfit])
        .apply(builder().register(NightlyReport))
        .unwrap()
        .build()
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("periodic job `misfit`")
            && error.to_string().contains("nightly_report"),
        "{error}"
    );
}

#[cfg(feature = "periodic")]
#[tokio::test]
async fn a_file_schedule_drops_duplicates_whatever_its_job_type_declares() {
    // docs-usability-14: a job type declaring `on_conflict = "error"` (for its web handlers)
    // made its schedules invalid; a schedule's ticks can only drop duplicates, and the file has
    // no override: the job type's `on_conflict` does not apply to them (its uniqueness does).
    let directory = tempfile::tempdir().unwrap();
    let strict = write(
        directory.path(),
        "strict.toml",
        "[[periodic]]\nname = \"strict\"\nevery = 60\nclass = \"strict_sync\"\n",
    );
    let config = config_with(args(&["-C", &strict]), builder().register(StrictSync)).await;
    let job = config.periodic[0].payload(std::time::UNIX_EPOCH).unwrap();
    let unique = job.unique.as_ref().and_then(|unique| unique.settings());
    assert_eq!(
        unique.map(|unique| (unique.ttl, unique.on_conflict)),
        Some((Some(Duration::from_secs(600)), None))
    );
}
