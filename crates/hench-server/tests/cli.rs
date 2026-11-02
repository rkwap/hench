//! The command line: parsing, the reports of `hench stats` (Sidekiq's sidekiqmon_test
//! scenarios against a real Redis), signals, pushing jobs, and the binary end to end.

use std::process::Stdio;
use std::time::Duration;

use clap::{CommandFactory, Parser};
use hench::api::Api;
use hench::redis_rs;
use hench::test_util::TestRedis;
use hench_server::cli::{Cli, Command, Section};
use hench_server::server::ServerConfig;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
    let mut argv = vec!["hench"];
    argv.extend_from_slice(args);
    Cli::try_parse_from(argv)
}

#[test]
fn the_help_names_every_subcommand_and_what_the_api_serves() {
    // fresh-user-3: `hench --help` summed `batches` up as "(list, show, cancel, delete,
    // skip)", without `dispatch`, and `hench web --help` said `--api` served "stats, queues,
    // processes, and POST /jobs", without the features' routes.
    let mut cli = Cli::command();
    for command in cli.get_subcommands() {
        let about = command.get_about().map(ToString::to_string);
        for subcommand in command.get_subcommands() {
            let name = subcommand.get_name();
            if name == "help" {
                continue;
            }
            let about = about.as_deref().unwrap_or_default();
            assert!(
                about.contains(name),
                "`hench {}`'s summary leaves out `{name}`: {about}",
                command.get_name()
            );
        }
    }
    let web = cli.find_subcommand("web").unwrap();
    let api = web
        .get_arguments()
        .find(|arg| arg.get_id() == "api")
        .unwrap();
    let help = api.get_help().map(ToString::to_string).unwrap_or_default();
    for served in [
        "stats",
        "queues",
        "processes",
        "POST /jobs",
        "periodic",
        "batches",
        "limits",
        "locks",
    ] {
        assert!(help.contains(served), "--api leaves out {served}: {help}");
    }
    let text = cli.render_help().to_string();
    assert!(
        text.contains("The batches (list, show, cancel, delete, skip, dispatch)"),
        "{text}"
    );
}

#[test]
fn the_synopsis_names_every_command_option_and_section() {
    // fresh-user-3: the synopsis of the crate documentation (the front page of docs.rs)
    // predated the features: no `periodic`, `batches`, `limits` or `locks` command, `stats`
    // without their sections, `enqueue` without its unique, throttle and JSON options.
    let docs = include_str!("../src/lib.rs");
    let synopsis: Vec<&str> = docs
        .lines()
        .skip_while(|line| *line != "//! ```text")
        .skip(1)
        .take_while(|line| *line != "//! ```")
        .map(|line| line.strip_prefix("//! ").unwrap_or(line))
        .collect();
    assert!(!synopsis.is_empty(), "no synopsis in src/lib.rs");
    let cli = Cli::command();
    for command in cli.get_subcommands() {
        let name = command.get_name();
        // Its entry: the line `hench <name> ...` and the indented lines continuing it.
        let start = synopsis
            .iter()
            .position(|line| line.split_whitespace().take(2).eq(["hench", name]))
            .unwrap_or_else(|| panic!("the synopsis leaves out `hench {name}`"));
        let entry = synopsis[start..]
            .iter()
            .enumerate()
            .take_while(|(index, line)| *index == 0 || line.starts_with(' '))
            .map(|(_, line)| *line)
            .collect::<Vec<_>>()
            .join("\n");
        let subcommands: Vec<&str> = command
            .get_subcommands()
            .map(|subcommand| subcommand.get_name())
            .filter(|name| *name != "help")
            .collect();
        for subcommand in &subcommands {
            assert!(
                entry.contains(subcommand),
                "`{subcommand}` missing:\n{entry}"
            );
        }
        if !subcommands.is_empty() {
            // Their options are in their modules' documentation and `--help`.
            continue;
        }
        // Every option, and every value of those that take a fixed set (the sections of
        // `hench stats`, `--unique-until`, `--on-conflict`).
        for arg in command.get_arguments() {
            if let Some(long) = arg.get_long() {
                assert!(entry.contains(&format!("--{long}")), "--{long}:\n{entry}");
            }
            if arg.get_action().takes_values() {
                for value in arg.get_possible_values() {
                    let value = value.get_name();
                    assert!(entry.contains(value), "`{value}` missing:\n{entry}");
                }
            }
        }
    }
}

#[test]
fn web_arguments_are_parsed_and_validated() {
    let cli = parse(&[
        "web",
        "--bind",
        "0.0.0.0:8080",
        "--username",
        "admin",
        "--password",
        "secret",
        "--read-only",
        "--prefix",
        "/jobs/",
        "--api",
        "--poll-interval",
        "3s",
        "--redis-url",
        "redis://cache:6379/2",
    ])
    .unwrap();
    assert_eq!(cli.redis_url.as_deref(), Some("redis://cache:6379/2"));
    let Command::Web(web) = cli.command else {
        panic!("not web")
    };
    assert_eq!(web.bind.to_string(), "0.0.0.0:8080");
    assert!(web.read_only && web.api);
    assert_eq!(web.prefix, "/jobs");
    assert_eq!(web.poll_interval, Duration::from_secs(3));
    let config = ServerConfig::from_args(&web).unwrap();
    assert!(config.credentials.is_some());

    // The API needs credentials unless it listens on a loopback address.
    let Command::Web(open) = parse(&["web", "--bind", "0.0.0.0:8080", "--api"])
        .unwrap()
        .command
    else {
        panic!("not web")
    };
    let error = ServerConfig::from_args(&open).unwrap_err().to_string();
    assert!(error.contains("refusing to serve the JSON API"), "{error}");
    for bind in ["127.0.0.1:7420", "[::1]:7420"] {
        let Command::Web(local) = parse(&["web", "--bind", bind, "--api"]).unwrap().command else {
            panic!("not web")
        };
        assert!(ServerConfig::from_args(&local).is_ok(), "{bind}");
    }
    // Credentials go in pairs.
    let Command::Web(half) = parse(&["web", "--username", "a"]).unwrap().command else {
        panic!("not web")
    };
    assert!(ServerConfig::from_args(&half).is_err());
    // Defaults.
    let Command::Web(defaults) = parse(&["web"]).unwrap().command else {
        panic!("not web")
    };
    assert_eq!(defaults.bind.to_string(), "127.0.0.1:7420");
    assert_eq!(defaults.prefix, "");
    assert!(!defaults.api && !defaults.read_only);
    assert!(defaults.allowed_hosts.is_empty());
    // Without credentials only localhost and IP addresses are served, plus allowed names.
    let config = ServerConfig::from_args(&defaults).unwrap();
    let hosts = config.allowed_hosts.expect("host names are checked");
    assert!(hosts.allows("localhost:7420") && !hosts.allows("jobs.example.com"));
    let Command::Web(named) = parse(&[
        "web",
        "--allowed-host",
        "Jobs.Example.com,.corp.example",
        "--allowed-host",
        "other.example",
    ])
    .unwrap()
    .command
    else {
        panic!("not web")
    };
    assert_eq!(
        named.allowed_hosts,
        ["jobs.example.com", ".corp.example", "other.example"]
    );
    let hosts = ServerConfig::from_args(&named)
        .unwrap()
        .allowed_hosts
        .unwrap();
    assert!(hosts.allows("jobs.example.com:443") && hosts.allows("x.corp.example"));
    // With credentials every name is served, unless names are listed.
    assert!(
        config_of(&["web", "--username", "a", "--password", "b"])
            .allowed_hosts
            .is_none()
    );
    let listed = config_of(&[
        "web",
        "--username",
        "a",
        "--password",
        "b",
        "--allowed-host",
        "x.example",
    ]);
    assert!(listed.allowed_hosts.is_some());
    assert!(
        config_of(&["web", "--allowed-host", "*"])
            .allowed_hosts
            .is_none()
    );
    // Malformed values.
    for args in [
        &["web", "--bind", "nowhere"][..],
        &["web", "--prefix", "no-slash"],
        &["web", "--poll-interval", "soon"],
        &["web", "--allowed-host", "https://jobs.example.com"],
        &["web", "--allowed-host", "jobs.example.com:443"],
    ] {
        assert!(parse(args).is_err(), "{args:?}");
    }
}

/// The server configuration of `hench <args>` (which must be valid).
fn config_of(args: &[&str]) -> ServerConfig {
    let Command::Web(web) = parse(args).unwrap().command else {
        panic!("not web")
    };
    ServerConfig::from_args(&web).unwrap()
}

#[test]
fn other_commands_are_parsed() {
    let Command::Stats(stats) = parse(&["stats", "queues", "--json"]).unwrap().command else {
        panic!("not stats")
    };
    assert_eq!(stats.section, Section::Queues);
    assert!(stats.json);
    let Command::Stats(all) = parse(&["stats"]).unwrap().command else {
        panic!("not stats")
    };
    assert_eq!(all.section, Section::All);
    // Sidekiq's "reports an unknown section and lists valid ones".
    let error = parse(&["stats", "bogus"]).unwrap_err().to_string();
    assert!(error.contains("bogus"), "{error}");
    assert!(
        error.contains("all") && error.contains("version") && error.contains("overview"),
        "{error}"
    );

    let Command::Quiet(quiet) = parse(&["quiet", "all"]).unwrap().command else {
        panic!("not quiet")
    };
    assert_eq!(quiet.target, "all");
    assert!(parse(&["stop"]).is_err());

    let Command::Enqueue(enqueue) = parse(&[
        "enqueue",
        "Mailer",
        "--args",
        "[1, \"x\"]",
        "--queue",
        "mail",
        "--in",
        "5m",
        "--retry",
        "3",
    ])
    .unwrap()
    .command
    else {
        panic!("not enqueue")
    };
    assert_eq!(enqueue.class, "Mailer");
    assert_eq!(enqueue.args.0, [json!(1), json!("x")]);
    assert_eq!(enqueue.delay, Some(Duration::from_secs(300)));
    assert_eq!(enqueue.retry, Some(json!(3)));
    assert!(parse(&["enqueue", "X", "--in", "5m", "--at", "2030-01-01T00:00:00Z"]).is_err());
    assert!(parse(&["enqueue", "X", "--args", "[1"]).is_err());
    assert!(parse(&[]).is_err());
}

/// Runs `hench <args>` in process against `redis` and returns what it printed.
async fn run(redis: &TestRedis, args: &[&str]) -> anyhow::Result<String> {
    let url = redis.url();
    let mut argv = vec!["--redis-url", url.as_str()];
    argv.extend_from_slice(args);
    let cli = parse(&argv)?;
    let mut out = Vec::new();
    hench_server::run(cli, &mut out).await?;
    Ok(String::from_utf8(out)?)
}

async fn seed(redis: &TestRedis) -> Api {
    let api = Api::new(redis.pool().await);
    let mut conn = api.redis().conn().await.unwrap();
    let _: () = redis_rs::cmd("MSET")
        .arg("stat:processed")
        .arg(420_710)
        .arg("stat:failed")
        .arg(12)
        .query_async(&mut conn)
        .await
        .unwrap();
    let _: i64 = redis_rs::cmd("SADD")
        .arg("processes")
        .arg("foobar:1:abc")
        .query_async(&mut conn)
        .await
        .unwrap();
    let info = json!({
        "hostname": "foobar", "pid": 1, "tag": "baz", "started_at": hench::now_secs_f64(),
        "capsules": {"mike": {"concurrency": 3, "mode": "weighted",
                              "weights": {"default": 2, "high": 3, "low": 1}},
                     "single": {"concurrency": 1, "mode": "strict", "weights": {"single": 0}}},
        "queues": ["low", "default", "high", "single"],
        "version": env!("CARGO_PKG_VERSION"),
    });
    let _: i64 = redis_rs::cmd("HSET")
        .arg("foobar:1:abc")
        .arg("info")
        .arg(info.to_string())
        .arg("busy")
        .arg(2)
        .arg("concurrency")
        .arg(5)
        .arg("quiet")
        .arg("false")
        .query_async(&mut conn)
        .await
        .unwrap();
    for (queue, count) in [("foobar", 12), ("a_long_queue_name", 3)] {
        for index in 0..count {
            let mut job = hench::JobPayload::new("X", vec![json!(index)]);
            job.queue = queue.to_owned();
            api.client().push(job).await.unwrap();
        }
    }
    api
}

#[tokio::test]
async fn stats_print_an_overview() {
    let redis = TestRedis::start().await;
    seed(&redis).await;
    let text = run(&redis, &["stats"]).await.unwrap();
    assert!(
        text.contains(&format!("hench {}", env!("CARGO_PKG_VERSION"))),
        "{text}"
    );
    assert!(text.contains(" UTC\n"), "{text}");
    assert!(text.contains("---- Overview ----"));
    assert!(text.contains("  Processed: 420,710"), "{text}");
    assert!(text.contains("     Failed: 12"));
    assert!(text.contains("       Busy: 2"));
    assert!(text.contains("   Enqueued: 15"));
    assert!(text.contains("---- Processes (1) ----"));
    assert!(text.contains("foobar:1:abc [baz]"), "{text}");
    assert!(text.contains("(just now)"), "{text}");
    assert!(text.contains("  Threads: 5 (2 busy)"));
    // Capsule weights are reported as JSON objects, so each capsule's queues come by name.
    assert!(
        text.contains("   Queues: default, high, low; single"),
        "{text}"
    );
    assert!(text.contains("---- Queues (2) ----"));
    assert!(
        text.contains("NAME                 SIZE  LATENCY"),
        "{text}"
    );
    let row = |name: &str, size: u64| format!("{name:<19}{size:>6}");
    assert!(text.contains(&row("a_long_queue_name", 3)), "{text}");
    assert!(text.contains(&row("foobar", 12)), "{text}");

    // One section at a time, and the shortcuts.
    let overview = run(&redis, &["stats", "overview"]).await.unwrap();
    assert!(overview.starts_with("---- Overview ----") && !overview.contains("Queues"));
    let queues = run(&redis, &["queues"]).await.unwrap();
    assert!(queues.starts_with("---- Queues (2) ----"));
    let processes = run(&redis, &["processes"]).await.unwrap();
    assert!(processes.starts_with("---- Processes (1) ----"));
    let version = run(&redis, &["stats", "version"]).await.unwrap();
    assert!(version.starts_with("hench "));
}

#[tokio::test]
async fn processes_with_times_beyond_the_calendar_are_listed() {
    let redis = TestRedis::start().await;
    let api = Api::new(redis.pool().await);
    let mut conn = api.redis().conn().await.unwrap();
    let _: i64 = redis_rs::cmd("SADD")
        .arg("processes")
        .arg("far:1:abc")
        .query_async(&mut conn)
        .await
        .unwrap();
    let info = json!({"hostname": "far", "pid": 1, "started_at": 1e17});
    let _: i64 = redis_rs::cmd("HSET")
        .arg("far:1:abc")
        .arg("info")
        .arg(info.to_string())
        .arg("beat")
        .arg(-9e12)
        .query_async(&mut conn)
        .await
        .unwrap();
    let text = run(&redis, &["processes"]).await.unwrap();
    assert!(text.contains("far:1:abc\n  Started: unknown\n"), "{text}");
    let json: Value =
        serde_json::from_str(&run(&redis, &["processes", "--json"]).await.unwrap()).unwrap();
    assert_eq!(json[0]["started_at"], Value::Null);
    assert_eq!(json[0]["beat"], Value::Null);
}

#[tokio::test]
async fn stats_print_the_busiest_job_classes() {
    let redis = TestRedis::start().await;
    seed(&redis).await;
    // Nothing recorded yet.
    let text = run(&redis, &["stats", "metrics"]).await.unwrap();
    assert_eq!(
        text,
        "---- Metrics (last hour) ----\nNo job ran in the last hour.\n"
    );

    let tracker = hench::metrics::Tracker::new();
    let ms = Duration::from_millis;
    for _ in 0..3 {
        tracker.track("ImportCsv", ms(400), hench::metrics::Outcome::Success);
    }
    tracker.track("ImportCsv", ms(0), hench::metrics::Outcome::Failure);
    tracker.track("SendEmail", ms(20), hench::metrics::Outcome::Success);
    tracker.flush(&redis.pool().await).await.unwrap();

    let text = run(&redis, &["stats", "metrics"]).await.unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines[0], "---- Metrics (last hour) ----");
    assert!(lines[1].starts_with("CLASS"), "{text}");
    // Busiest (most time) first.
    assert!(lines[2].starts_with("ImportCsv"), "{text}");
    assert!(lines[2].contains("  4  "), "{text}");
    assert!(lines[2].ends_with("1.20     400.0"), "{text}");
    assert!(lines[3].starts_with("SendEmail"), "{text}");
    // The whole report ends with them.
    let all = run(&redis, &["stats"]).await.unwrap();
    assert!(
        all.contains("---- Metrics (last hour) ----\nCLASS"),
        "{all}"
    );
    let json: Value =
        serde_json::from_str(&run(&redis, &["stats", "metrics", "--json"]).await.unwrap()).unwrap();
    assert_eq!(json[0]["class"], "ImportCsv");
    assert_eq!(json[0]["executions"], 4);
    assert_eq!(json[0]["failed"], 1);
    assert_eq!(json[1]["class"], "SendEmail");
    let report: Value =
        serde_json::from_str(&run(&redis, &["stats", "--json"]).await.unwrap()).unwrap();
    assert_eq!(report["metrics"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn stats_print_json() {
    let redis = TestRedis::start().await;
    seed(&redis).await;
    let report: Value =
        serde_json::from_str(&run(&redis, &["stats", "--json"]).await.unwrap()).unwrap();
    assert_eq!(report["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(report["overview"]["processed"], 420_710);
    assert_eq!(report["overview"]["enqueued"], 15);
    assert_eq!(report["processes"][0]["identity"], "foobar:1:abc");
    // The queue names in the process's order, and each capsule by name with its settings
    // (fresh-user-4: one display string per capsule, `["default, high, low", "single"]`,
    // before).
    let capsules = json!({
        "mike": {"concurrency": 3, "mode": "weighted",
                 "weights": {"default": 2, "high": 3, "low": 1}},
        "single": {"concurrency": 1, "mode": "strict", "weights": {"single": 0}},
    });
    assert_eq!(
        report["processes"][0]["queues"],
        json!(["low", "default", "high", "single"])
    );
    assert_eq!(report["processes"][0]["capsules"], capsules);
    assert_eq!(report["queues"][0]["name"], "a_long_queue_name");
    let queues: Value =
        serde_json::from_str(&run(&redis, &["queues", "--json"]).await.unwrap()).unwrap();
    assert_eq!(queues.as_array().unwrap().len(), 2);
    assert_eq!(queues[1]["size"], 12);
    // `hench processes --json` and `hench stats processes --json` print the same.
    for args in [
        &["processes", "--json"][..],
        &["stats", "processes", "--json"],
    ] {
        let processes: Value = serde_json::from_str(&run(&redis, args).await.unwrap()).unwrap();
        assert_eq!(processes, report["processes"], "{args:?}");
        assert_eq!(processes[0]["capsules"], capsules, "{args:?}");
    }
}

#[tokio::test]
async fn processes_can_be_quieted_and_stopped() {
    let redis = TestRedis::start().await;
    let api = seed(&redis).await;
    let mut conn = api.redis().conn().await.unwrap();
    let signals = conn.clone();
    let pop = |identity: &'static str| {
        let mut conn = signals.clone();
        async move {
            redis_rs::cmd("RPOP")
                .arg(format!("{identity}-signals"))
                .query_async::<Option<String>>(&mut conn)
                .await
                .unwrap()
        }
    };
    let text = run(&redis, &["quiet", "foobar:1:abc"]).await.unwrap();
    assert_eq!(text, "Asked foobar:1:abc to quiet down.\n");
    assert_eq!(pop("foobar:1:abc").await.as_deref(), Some("TSTP"));
    let text = run(&redis, &["stop", "all"]).await.unwrap();
    assert_eq!(text, "Asked 1 process to stop: foobar:1:abc\n");
    assert_eq!(pop("foobar:1:abc").await.as_deref(), Some("TERM"));
    let error = run(&redis, &["stop", "ghost:1:x"])
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("ghost:1:x"), "{error}");

    // Embedded processes are left out of `all`.
    let info = json!({"hostname": "web", "pid": 2, "embedded": true});
    let _: i64 = redis_rs::cmd("HSET")
        .arg("foobar:1:abc")
        .arg("info")
        .arg(info.to_string())
        .query_async(&mut conn)
        .await
        .unwrap();
    let text = run(&redis, &["quiet", "all"]).await.unwrap();
    assert!(text.starts_with("No process to quiet down."), "{text}");
    assert!(text.contains("Left 1 process embedded"), "{text}");
    assert_eq!(pop("foobar:1:abc").await, None);
}

#[tokio::test]
async fn jobs_can_be_pushed() {
    let redis = TestRedis::start().await;
    let api = Api::new(redis.pool().await);
    let jid = run(
        &redis,
        &[
            "enqueue",
            "Mailer",
            "--args",
            "[42, \"welcome\"]",
            "--queue",
            "mail",
        ],
    )
    .await
    .unwrap();
    let jid = jid.trim();
    assert_eq!(jid.len(), 24);
    let record = api.queue("mail").find_job(jid).await.unwrap().unwrap();
    assert_eq!(record.class(), Some("Mailer"));
    assert_eq!(record.args(), [json!(42), json!("welcome")]);

    // A single non-array value is the only argument; the default queue is `default`.
    run(&redis, &["enqueue", "Ping", "--args", "{\"a\": 1}"])
        .await
        .unwrap();
    let page = api
        .queue("default")
        .page(1, 10, Default::default())
        .await
        .unwrap();
    assert_eq!(page.items[0].args(), [json!({"a": 1})]);

    // Later.
    let out = run(
        &redis,
        &[
            "enqueue", "Later", "--in", "1h", "--json", "--retry", "false",
        ],
    )
    .await
    .unwrap();
    let pushed: Value = serde_json::from_str(&out).unwrap();
    let at = pushed["at"].as_f64().unwrap();
    assert!((at - hench::now_secs_f64() - 3_600.0).abs() < 60.0, "{at}");
    let entry = api
        .scheduled()
        .find_job(pushed["jid"].as_str().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(entry.get("retry"), Some(&json!(false)));
    run(
        &redis,
        &["enqueue", "Later", "--at", "2099-01-01T00:00:00Z"],
    )
    .await
    .unwrap();
    assert_eq!(api.scheduled().size().await.unwrap(), 2);
    // Times no calendar shows are refused (a delay in milliseconds read as seconds...).
    for args in [
        &["enqueue", "Later", "--in", "10000000000000"][..],
        &["enqueue", "Later", "--at", "9999-12-31T23:59:59-23:59"],
    ] {
        let error = run(&redis, args).await.unwrap_err().to_string();
        assert!(error.contains("before the year 10000"), "{args:?}: {error}");
    }
    assert_eq!(api.scheduled().size().await.unwrap(), 2);

    // Invalid jobs are refused.
    let error = run(&redis, &["enqueue", ""]).await.unwrap_err().to_string();
    assert!(error.contains("invalid job"), "{error}");
}

#[tokio::test]
async fn unreachable_redis_is_a_clear_error() {
    let mut redis = TestRedis::start().await;
    redis.stop();
    let error = run(&redis, &["stats"]).await.unwrap_err();
    assert!(
        format!("{error:#}").contains("cannot reach Redis"),
        "{error:#}"
    );
}

/// The path of the `hench` binary under test.
fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_hench")
}

#[tokio::test]
async fn the_binary_prints_stats() {
    let redis = TestRedis::start().await;
    seed(&redis).await;
    let output = tokio::process::Command::new(binary())
        .arg("stats")
        .arg("overview")
        .env("HENCH_REDIS_URL", redis.url())
        .output()
        .await
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Processed: 420,710"), "{stdout}");

    let failed = tokio::process::Command::new(binary())
        .args(["quiet", "nobody"])
        .env("HENCH_REDIS_URL", redis.url())
        .output()
        .await
        .unwrap();
    assert_eq!(failed.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&failed.stderr).contains("hench: no live process"));
}

/// Sends a bare HTTP/1.1 request and returns the status line and the body.
async fn http_get(address: &str, path: &str) -> (String, String) {
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    let status = response.lines().next().unwrap_or_default().to_owned();
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_owned())
        .unwrap_or_default();
    (status, body)
}

#[cfg(unix)]
#[tokio::test]
async fn the_binary_serves_the_dashboard_and_stops_on_sigterm() {
    let redis = TestRedis::start().await;
    seed(&redis).await;
    let mut child = tokio::process::Command::new(binary())
        .args([
            "web",
            "--bind",
            "127.0.0.1:0",
            "--api",
            "--prefix",
            "/hench",
        ])
        .env("HENCH_REDIS_URL", redis.url())
        .env("HENCH_LOG", "info")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let stderr = child.stderr.take().unwrap();
    let mut lines = BufReader::new(stderr).lines();
    let address = tokio::time::timeout(Duration::from_secs(20), async {
        while let Some(line) = lines.next_line().await.unwrap() {
            if let Some(rest) = line.split("listening on http://").nth(1) {
                return rest.split('/').next().unwrap_or_default().to_owned();
            }
        }
        panic!("the server exited before listening");
    })
    .await
    .expect("the server starts");
    // Keep draining the logs so the server never blocks on a full pipe.
    tokio::spawn(async move { while let Ok(Some(_)) = lines.next_line().await {} });

    let (status, body) = http_get(&address, "/hench/healthz").await;
    assert!(status.contains("200"), "{status}");
    assert_eq!(body, "ok\n");
    let (status, body) = http_get(&address, "/hench/busy").await;
    assert!(status.contains("200"), "{status}");
    assert!(body.contains("foobar:1:abc"));
    let (status, body) = http_get(&address, "/hench/api/v1/stats").await;
    assert!(status.contains("200"), "{status}");
    assert!(body.contains("\"processed\":420710"), "{body}");

    let pid = i32::try_from(child.id().unwrap()).unwrap();
    // SAFETY: sending a signal to a child process we own.
    let sent = unsafe { libc::kill(pid, libc::SIGTERM) };
    assert_eq!(sent, 0);
    let status = tokio::time::timeout(Duration::from_secs(20), child.wait())
        .await
        .expect("the server stops")
        .unwrap();
    assert!(status.success(), "{status:?}");
}

/// Sends a bare HTTP/1.1 request with `method` and returns the status line and how long the
/// answer took.
async fn timed_request(address: &str, method: &str, path: &str) -> (String, Duration) {
    let started = std::time::Instant::now();
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream
        .write_all(
            format!("{method} {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    let status = response.lines().next().unwrap_or_default().to_owned();
    (status, started.elapsed())
}

#[cfg(unix)]
#[tokio::test]
async fn the_server_starts_and_answers_quickly_while_redis_is_down() {
    // web-functional-12: with Redis unreachable, `hench web` waited ~7.5 s for a ping before
    // listening, `/healthz` took 7.5 s and pages up to 15 s (six reconnection attempts each),
    // past the timeouts of load balancers and probes.
    let started = std::time::Instant::now();
    let mut child = tokio::process::Command::new(binary())
        .args(["web", "--bind", "127.0.0.1:0"])
        .env("HENCH_REDIS_URL", "redis://127.0.0.1:1")
        .env("HENCH_LOG", "info")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let stderr = child.stderr.take().unwrap();
    let mut lines = BufReader::new(stderr).lines();
    let address = tokio::time::timeout(Duration::from_secs(20), async {
        while let Some(line) = lines.next_line().await.unwrap() {
            if let Some(rest) = line.split("listening on http://").nth(1) {
                return rest.split('/').next().unwrap_or_default().to_owned();
            }
        }
        panic!("the server exited before listening");
    })
    .await
    .expect("the server starts");
    let startup = started.elapsed();
    tokio::spawn(async move { while let Ok(Some(_)) = lines.next_line().await {} });
    assert!(
        startup < Duration::from_secs(3),
        "listening after {startup:?}"
    );

    for (method, path, status, limit) in [
        ("GET", "/healthz", "503", Duration::from_millis(2_500)),
        ("HEAD", "/", "503", Duration::from_secs(3)),
        ("GET", "/", "500", Duration::from_secs(3)),
        ("GET", "/retries", "500", Duration::from_secs(3)),
        ("GET", "/stats", "503", Duration::from_secs(3)),
    ] {
        let (line, took) = timed_request(&address, method, path).await;
        assert!(line.contains(status), "{method} {path}: {line}");
        assert!(took < limit, "{method} {path} took {took:?}");
    }
    child.kill().await.unwrap();
}

/// Starts `hench web <args>` (Redis unreachable unless `redis_url` is given): the process and
/// the address it listens on.
#[cfg(unix)]
async fn spawn_web(args: &[&str], redis_url: Option<&str>) -> (tokio::process::Child, String) {
    let mut child = tokio::process::Command::new(binary())
        .arg("web")
        .args(args)
        .env(
            "HENCH_REDIS_URL",
            redis_url.unwrap_or("redis://127.0.0.1:1"),
        )
        .env("HENCH_LOG", "info")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let stderr = child.stderr.take().unwrap();
    let mut lines = BufReader::new(stderr).lines();
    let address = tokio::time::timeout(Duration::from_secs(20), async {
        while let Some(line) = lines.next_line().await.unwrap() {
            if let Some(rest) = line.split("listening on http://").nth(1) {
                return rest.split('/').next().unwrap_or_default().to_owned();
            }
        }
        panic!("the server exited before listening");
    })
    .await
    .expect("the server starts");
    tokio::spawn(async move { while let Ok(Some(_)) = lines.next_line().await {} });
    (child, address)
}

/// Sends `signal` to the process `child`.
#[cfg(unix)]
fn send_signal(child: &tokio::process::Child, signal: i32) {
    let pid = i32::try_from(child.id().unwrap()).unwrap();
    // SAFETY: sending a signal to a child process we own.
    assert_eq!(unsafe { libc::kill(pid, signal) }, 0);
}

#[cfg(unix)]
#[tokio::test]
async fn the_binary_closes_a_request_head_that_never_ends() {
    // security-supply-chain-1: without a timer, hyper had no request-head timeout, so a client
    // sending half a request kept its connection (and a file descriptor) forever; about a
    // thousand of them exhausted the open-file limit and took every page, the JSON API and
    // `/healthz` offline.
    let (_child, address) = spawn_web(&["--bind", "127.0.0.1:0"], None).await;
    let mut stream = tokio::net::TcpStream::connect(&address).await.unwrap();
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n")
        .await
        .unwrap();
    let started = std::time::Instant::now();
    let mut buf = [0_u8; 256];
    let read = tokio::time::timeout(Duration::from_secs(20), stream.read(&mut buf)).await;
    assert!(
        read.is_ok(),
        "the half-open connection is still open after {:?}",
        started.elapsed()
    );
    // The default header timeout (10 s), not much less.
    assert!(started.elapsed() >= Duration::from_secs(5));
}

#[cfg(unix)]
#[tokio::test]
async fn the_binary_stops_on_signals_while_a_request_is_unfinished() {
    // security-supply-chain-2: the graceful shutdown waited for every connection without a
    // deadline, an unfinished request never finished, and later signals were swallowed: only
    // SIGKILL stopped the server. A second signal now stops it at once (the first one alone
    // would have waited for the request-head or shutdown timeout, 10 s).
    let (mut child, address) = spawn_web(&["--bind", "127.0.0.1:0"], None).await;
    let mut stream = tokio::net::TcpStream::connect(&address).await.unwrap();
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let started = std::time::Instant::now();
    send_signal(&child, libc::SIGTERM);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        child.try_wait().unwrap().is_none(),
        "the first signal waits for the request in flight"
    );
    // An operator pressing Ctrl-C.
    send_signal(&child, libc::SIGINT);
    let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .unwrap_or_else(|_| {
            panic!(
                "hench web still runs {:?} after SIGTERM and SIGINT",
                started.elapsed()
            )
        })
        .unwrap();
    assert!(status.success(), "{status:?}");
    drop(stream);
}

#[cfg(unix)]
#[tokio::test]
async fn the_binary_refuses_the_health_check_path_as_prefix() {
    // security-supply-chain-6: `--prefix /healthz` passed validation, then the router panicked
    // on the conflicting routes (exit code 101).
    for prefix in ["/healthz", "/healthz/"] {
        let output = tokio::process::Command::new(binary())
            .args(["web", "--bind", "127.0.0.1:0", "--prefix", prefix])
            .env("HENCH_REDIS_URL", "redis://127.0.0.1:1")
            .output()
            .await
            .unwrap();
        assert_eq!(output.status.code(), Some(2), "{prefix}: {output:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("health check"), "{prefix}: {stderr}");
        assert!(!stderr.contains("panicked"), "{prefix}: {stderr}");
    }
}

#[tokio::test]
async fn enqueue_says_that_the_job_types_options_do_not_apply() {
    // new-user-5: the help promised the job type's own queue, which `hench` cannot know (it
    // has no registry of the application's job types): such jobs went to `default` silently,
    // with the default retries.
    let help = tokio::process::Command::new(binary())
        .args(["enqueue", "--help"])
        .output()
        .await
        .unwrap();
    assert!(help.status.success(), "{help:?}");
    let help = String::from_utf8_lossy(&help.stdout);
    assert!(!help.contains("the job's own"), "{help}");
    assert!(help.contains("The queue (default: \"default\""), "{help}");
    assert!(help.contains("do not apply"), "{help}");

    let redis = TestRedis::start().await;
    let api = Api::new(redis.pool().await);
    let enqueue = |args: &[&str]| {
        tokio::process::Command::new(binary())
            .arg("enqueue")
            .args(args)
            .env("HENCH_REDIS_URL", redis.url())
            .env_remove("HENCH_LOG")
            .env_remove("RUST_LOG")
            .output()
    };
    let output = enqueue(&["send_welcome", "--args", "[7]"]).await.unwrap();
    assert!(output.status.success(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("no --queue") && stderr.contains("`default`"),
        "{stderr}"
    );
    assert_eq!(api.queue("default").size().await.unwrap(), 1);
    // Given the queue and the retries, the job gets them, without a warning.
    let output = enqueue(&[
        "send_welcome",
        "--args",
        "[7]",
        "--queue",
        "mailers",
        "--retry",
        "3",
    ])
    .await
    .unwrap();
    assert!(output.status.success(), "{output:?}");
    assert!(output.stderr.is_empty(), "{output:?}");
    let jid = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    let record = api.queue("mailers").find_job(&jid).await.unwrap().unwrap();
    assert_eq!(record.get("retry"), Some(&json!(3)));
}
