//! The pro features in the binary: every feature command and `hench stats` section runs (each
//! feature's own tests check what they print: `tests/{periodic,batches,limits,locks}.rs`);
//! `hench periodic next` answers without Redis; `hench enqueue` and `POST /jobs` set the payload
//! fields of unique jobs and throttles, and report dropped duplicates without failing.

use std::time::Duration;

use clap::Parser;
use hench::api::Api;
use hench::limit::ThrottleOption;
use hench::test_util::TestRedis;
use hench::unique::{OnConflict, UniqueUntil};
use hench_server::cli::{Cli, Command, Section};
use hench_server::json_api::EnqueueRequest;
use serde_json::json;

fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
    let mut argv = vec!["hench"];
    argv.extend_from_slice(args);
    Cli::try_parse_from(argv)
}

#[test]
fn enqueue_takes_unique_and_throttle_flags() {
    let cli = parse(&[
        "enqueue",
        "sync",
        "--unique-for",
        "10m",
        "--unique-until",
        "start",
        "--unique-key",
        "[42]",
        "--unique-across-queues",
        "--on-conflict",
        "replace",
        "--throttle",
        "stripe:acme:42",
    ])
    .unwrap();
    let Command::Enqueue(args) = cli.command else {
        panic!("not enqueue");
    };
    assert_eq!(args.unique_for, Some(Duration::from_secs(600)));
    assert_eq!(args.unique_until.as_deref(), Some("start"));
    assert_eq!(args.unique_key, Some(json!([42])));
    assert!(args.unique_across_queues);
    assert_eq!(args.on_conflict.as_deref(), Some("replace"));
    assert_eq!(
        args.throttle,
        Some(("stripe".to_owned(), Some("acme:42".to_owned())))
    );
    // The unique options need --unique-for; a throttle needs a name.
    assert!(parse(&["enqueue", "sync", "--unique-until", "start"]).is_err());
    assert!(parse(&["enqueue", "sync", "--on-conflict", "error"]).is_err());
    assert!(
        parse(&[
            "enqueue",
            "sync",
            "--unique-for",
            "1m",
            "--unique-until",
            "never"
        ])
        .is_err()
    );
    assert!(parse(&["enqueue", "sync", "--throttle", ":42"]).is_err());
    assert!(parse(&["enqueue", "sync", "--unique-for", "1m", "--unique-key", "{"]).is_err());
}

#[test]
fn feature_commands_and_sections_parse() {
    for (args, expected) in [
        (&["periodic", "list"][..], "periodic"),
        (
            &["batches", "show", "0123456789abcdef01234567"][..],
            "batches",
        ),
        (&["limits"][..], "limits"),
        (&["locks", "release", "--all"][..], "locks"),
    ] {
        let command = parse(args).unwrap().command;
        let name = match command {
            Command::Periodic(_) => "periodic",
            Command::Batches(_) => "batches",
            Command::Limits(_) => "limits",
            Command::Locks(_) => "locks",
            _ => "other",
        };
        assert_eq!(name, expected);
    }
    for (section, expected) in [
        ("periodic", Section::Periodic),
        ("batches", Section::Batches),
        ("limits", Section::Limits),
        ("locks", Section::Locks),
    ] {
        let Command::Stats(stats) = parse(&["stats", section]).unwrap().command else {
            panic!("not stats");
        };
        assert_eq!(stats.section, expected);
    }
}

#[tokio::test]
async fn feature_commands_and_sections_run() {
    let redis = TestRedis::start().await;
    let url = redis.url();
    for args in [
        &["periodic", "list"][..],
        &["locks"][..],
        &["locks", "release-all", "--yes"][..],
        &["batches"][..],
        &["limits", "--json"][..],
        &["stats", "periodic"][..],
        &["stats", "locks", "--json"][..],
        &["stats", "batches"][..],
        &["stats", "limits"][..],
    ] {
        let mut argv = vec!["--redis-url", url.as_str()];
        argv.extend_from_slice(args);
        let mut out = Vec::new();
        hench_server::run(parse(&argv).unwrap(), &mut out)
            .await
            .unwrap_or_else(|error| panic!("{args:?}: {error}"));
    }
}

#[tokio::test]
async fn periodic_next_needs_no_redis() {
    // Nothing listens there: `next` answers before connecting.
    let mut out = Vec::new();
    let cli = parse(&[
        "--redis-url",
        "redis://127.0.0.1:1",
        "periodic",
        "next",
        "0 2 * * *",
        "--count",
        "2",
    ])
    .unwrap();
    hench_server::run(cli, &mut out).await.unwrap();
    let text = String::from_utf8(out).unwrap();
    let ticks = text
        .lines()
        .filter(|line| line.contains("T02:00:00"))
        .count();
    assert_eq!(ticks, 2, "{text}");
}

#[tokio::test]
async fn enqueue_reports_duplicates_without_failing() {
    let redis = TestRedis::start().await;
    let url = redis.url();
    let enqueue = |extra: &[&str], json: bool| {
        let mut argv = vec![
            "--redis-url",
            url.as_str(),
            "enqueue",
            "sync",
            "--queue",
            "default",
            "--unique-for",
            "1m",
        ];
        argv.extend_from_slice(extra);
        if json {
            argv.push("--json");
        }
        parse(&argv).unwrap()
    };
    let mut out = Vec::new();
    hench_server::run(enqueue(&[], false), &mut out)
        .await
        .unwrap();
    let holder = String::from_utf8(out).unwrap().trim().to_owned();
    assert_eq!(holder.len(), 24, "{holder}");
    // A dropped duplicate names its holder and succeeds (cron-style pushes expect them).
    let mut out = Vec::new();
    hench_server::run(enqueue(&[], false), &mut out)
        .await
        .unwrap();
    assert_eq!(
        String::from_utf8(out).unwrap().trim(),
        format!("duplicate of {holder}")
    );
    let mut out = Vec::new();
    hench_server::run(enqueue(&[], true), &mut out)
        .await
        .unwrap();
    let printed: serde_json::Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(printed, json!({"jid": null, "duplicate_of": holder}));
    // `--on-conflict error` makes it a failure.
    let mut out = Vec::new();
    let error = hench_server::run(enqueue(&["--on-conflict", "error"], false), &mut out)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("duplicate job"), "{error}");
    assert!(error.to_string().contains(&holder), "{error}");
    let api = Api::new(redis.pool().await);
    assert_eq!(api.queue("default").size().await.unwrap(), 1);
}

#[tokio::test]
async fn unique_and_throttle_flags_push_the_job() {
    let redis = TestRedis::start().await;
    let url = redis.url();
    let mut out = Vec::new();
    let cli = parse(&[
        "enqueue",
        "sync",
        "--queue",
        "default",
        "--unique-for",
        "1m",
        "--throttle",
        "stripe:42",
        "--json",
        "--redis-url",
        &url,
    ])
    .unwrap();
    hench_server::run(cli, &mut out).await.unwrap();
    let printed: serde_json::Value = serde_json::from_slice(&out).unwrap();
    assert!(printed["jid"].is_string(), "{printed}");
    let api = Api::new(redis.pool().await);
    assert_eq!(api.queue("default").size().await.unwrap(), 1);
}

#[test]
fn post_jobs_takes_unique_and_throttle_fields() {
    let request: EnqueueRequest = serde_json::from_value(json!({
        "class": "sync",
        "args": [42],
        "unique": {"for": 600, "until": "start", "on_conflict": "error", "key": [42]},
        "throttle": "stripe",
        "throttle_key": "acme",
    }))
    .unwrap();
    let job = request.into_payload().unwrap();
    let unique = job
        .unique
        .as_ref()
        .and_then(|unique| unique.settings())
        .unwrap();
    assert_eq!(unique.ttl, Some(Duration::from_secs(600)));
    assert_eq!(unique.until, Some(UniqueUntil::Start));
    assert_eq!(unique.on_conflict, Some(OnConflict::Error));
    assert_eq!(unique.key, Some(json!([42])));
    assert_eq!(
        job.throttle,
        Some(ThrottleOption::Enabled {
            name: Some("stripe".to_owned()),
            key: Some("acme".to_owned()),
        })
    );
    let disabled: EnqueueRequest =
        serde_json::from_value(json!({"class": "sync", "unique": false})).unwrap();
    assert_eq!(
        disabled.into_payload().unwrap().unique,
        Some(hench::UniqueOption::Disabled)
    );
    for body in [
        json!({"class": "sync", "throttle_key": "acme"}),
        json!({"class": "sync", "throttle": " "}),
    ] {
        let request: EnqueueRequest = serde_json::from_value(body.clone()).unwrap();
        assert!(request.into_payload().is_err(), "{body}");
    }
}

#[tokio::test]
async fn stats_all_prints_every_feature_in_use() {
    use hench::prelude::*;
    use std::time::Duration;

    let redis = TestRedis::start().await;
    let url = redis.url();
    let pool = redis.pool().await;
    let client = Client::new(pool.clone());
    let api = Api::new(pool.clone());
    let stats_all = || async {
        let mut out = Vec::new();
        hench_server::run(parse(&["--redis-url", &url, "stats"]).unwrap(), &mut out)
            .await
            .unwrap();
        String::from_utf8(out).unwrap()
    };
    // Nothing in use: no feature section.
    let text = stats_all().await;
    for section in ["---- Periodic", "Batches:", "Limits:", "---- Locks"] {
        assert!(!text.contains(section), "{section} in {text}");
    }
    // A lock, a schedule, a batch and a limiter in use.
    let mut job = JobPayload::new("Sync", vec![json!(1)]);
    job.unique = Some(hench::Unique::new(Duration::from_secs(60)).into());
    client.push(job).await.unwrap();
    api.periodic()
        .upsert(&Periodic::cron("nightly", "0 2 * * *").class("Report", vec![]))
        .await
        .unwrap();
    Batch::new(&client)
        .jobs(|jobs| async move {
            jobs.push(JobPayload::new("Member", vec![])).await?;
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap();
    let _permit = Limiter::concurrent("stats", 2)
        .acquire(&pool)
        .await
        .unwrap();
    let text = stats_all().await;
    for section in ["---- Periodic", "Batches:", "Limits:", "---- Locks"] {
        assert!(text.contains(section), "{section} not in {text}");
    }
}
