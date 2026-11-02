//! Pins the Redis commands of jobs that use none of the pro features (unique jobs, periodic
//! jobs, batches, rate limiting): they must stay byte for byte what they were before those
//! features existed (`docs/design/pro-features.md` §4, phase 0's gate).
//!
//! The test runs real workers against a private `redis-server` while a `MONITOR` connection
//! records every command. It keeps the commands that carry one of the test's jobs (their jid,
//! or a marker in their arguments) and, for a script call carrying one, the commands the script
//! ran (MONITOR shows them right after the call, tagged `lua`: a script runs alone). Times,
//! the process identity and generated jids are masked; everything else (keys, scripts' SHA-1,
//! the jobs' JSON, the order) must match `golden/featureless_commands.txt`, recorded from the
//! code before the pro features. Set `HENCH_RECORD_GOLDEN=1` to record it again (only for a
//! deliberate change of the stored format or of a script, reviewed like any golden file).

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use futures::StreamExt;
use hench::api::Api;
use hench::prelude::*;
use hench::test_util::TestRedis;
use tokio::sync::Mutex;

/// Jids of the test's jobs (hex, like generated ones, but recognizable).
const J_OK: &str = "00000000000000000000a001";
const J_RETRY: &str = "00000000000000000000a002";
const J_DROP: &str = "00000000000000000000a003";
const J_DEAD: &str = "00000000000000000000a004";
const J_INTERRUPT: &str = "00000000000000000000a005";
const J_SCHEDULED: &str = "00000000000000000000a006";
const J_RAW: &str = "00000000000000000000a007";
const J_PARKED: &str = "00000000000000000000a008";
const J_KILLED: &str = "00000000000000000000a009";
const J_KILL_ALL_1: &str = "00000000000000000000a00a";
const J_KILL_ALL_2: &str = "00000000000000000000a00b";

/// The argument that marks the jobs of the bulk push (their jids are generated).
const BULK_MARKER: &str = "featureless-bulk-";

/// Executions of the test's jobs.
static RUNS: AtomicUsize = AtomicUsize::new(0);

/// Whether the interrupted job was interrupted already.
static INTERRUPTED: AtomicBool = AtomicBool::new(false);

#[derive(Default)]
struct Succeeds;

impl Job for Succeeds {
    const NAME: &'static str = "featureless::Succeeds";
    type Args = Vec<serde_json::Value>;

    async fn perform(&self, _: &JobContext, _: Self::Args) -> Result<(), JobError> {
        RUNS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Default)]
struct Fails;

impl Job for Fails {
    const NAME: &'static str = "featureless::Fails";
    type Args = ();

    async fn perform(&self, _: &JobContext, (): ()) -> Result<(), JobError> {
        RUNS.fetch_add(1, Ordering::SeqCst);
        Err(JobError::new("featureless::Error", "it failed"))
    }

    fn retry_in(&self, _: u32, _: &JobError, _: &JobPayload) -> RetryIn {
        // Never due while the test runs: the scheduler leaves it alone.
        RetryIn::After(Duration::from_secs(3_600))
    }
}

#[derive(Default)]
struct InterruptsOnce;

impl Job for InterruptsOnce {
    const NAME: &'static str = "featureless::InterruptsOnce";
    type Args = ();

    async fn perform(&self, _: &JobContext, (): ()) -> Result<(), JobError> {
        RUNS.fetch_add(1, Ordering::SeqCst);
        if INTERRUPTED.swap(true, Ordering::SeqCst) {
            Ok(())
        } else {
            Err(JobError::interrupted())
        }
    }
}

/// Waits until the jobs ran `runs` times in all.
async fn wait_for_runs(runs: usize) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while RUNS.load(Ordering::SeqCst) < runs {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {runs} runs (got {})",
            RUNS.load(Ordering::SeqCst)
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Waits until the sorted set `key` holds `size` jobs. (Polled with `ZCARD`, which carries no
/// jid: polling must not add commands to the transcript.)
async fn wait_for_size(pool: &RedisPool, key: &str, size: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let mut conn = pool.conn().await.unwrap();
        let current: u64 = ::hench::redis_rs::cmd("ZCARD")
            .arg(key)
            .query_async(&mut conn)
            .await
            .unwrap();
        if current == size {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {size} jobs in {key} (got {current})"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// One line of `MONITOR` output: who sent the command (`client` or `lua`) and its arguments.
#[derive(Debug, Clone)]
struct Logged {
    lua: bool,
    args: Vec<String>,
}

/// Parses `<time> [<db> <source>] "ARG" "ARG" ...`.
fn parse_monitor_line(line: &str) -> Option<Logged> {
    let open = line.find('[')?;
    let close = open + line[open..].find(']')?;
    let source = &line[open + 1..close];
    let lua = source.split_whitespace().nth(1) == Some("lua");
    let mut args = Vec::new();
    let mut chars = line[close + 1..].chars().peekable();
    loop {
        // Skip to the next quoted argument.
        match chars.next() {
            None => break,
            Some('"') => {}
            Some(_) => continue,
        }
        let mut bytes: Vec<u8> = Vec::new();
        loop {
            match chars.next()? {
                '"' => break,
                '\\' => match chars.next()? {
                    'n' => bytes.push(b'\n'),
                    'r' => bytes.push(b'\r'),
                    't' => bytes.push(b'\t'),
                    'a' => bytes.push(0x07),
                    'b' => bytes.push(0x08),
                    'x' => {
                        let high = chars.next()?.to_digit(16)?;
                        let low = chars.next()?.to_digit(16)?;
                        bytes.push(u8::try_from(high * 16 + low).ok()?);
                    }
                    other => {
                        let mut buffer = [0; 4];
                        bytes.extend_from_slice(other.encode_utf8(&mut buffer).as_bytes());
                    }
                },
                other => {
                    let mut buffer = [0; 4];
                    bytes.extend_from_slice(other.encode_utf8(&mut buffer).as_bytes());
                }
            }
        }
        args.push(String::from_utf8_lossy(&bytes).into_owned());
    }
    Some(Logged { lua, args })
}

/// The masked form of an argument: the identity, times (10 digits or more, with or without a
/// fraction) and jids other than `known` are replaced.
fn mask(arg: &str, identity: &str, known: &HashSet<&str>) -> String {
    let arg = arg.replace(identity, "<identity>");
    let mut out = String::with_capacity(arg.len());
    let chars: Vec<char> = arg.chars().collect();
    let mut index = 0;
    while index < chars.len() {
        // A run of 24 lowercase hex characters that is not one of the test's jids.
        if index + 24 <= chars.len()
            && chars[index..index + 24]
                .iter()
                .all(|c| c.is_ascii_digit() || ('a'..='f').contains(c))
            && (index + 24 == chars.len() || !chars[index + 24].is_ascii_hexdigit())
            && (index == 0 || !chars[index - 1].is_ascii_hexdigit())
        {
            let run: String = chars[index..index + 24].iter().collect();
            if known.contains(run.as_str()) {
                out.push_str(&run);
            } else {
                out.push_str("<jid>");
            }
            index += 24;
            continue;
        }
        if chars[index].is_ascii_digit()
            && (index == 0 || !chars[index - 1].is_ascii_alphanumeric())
        {
            let start = index;
            while index < chars.len() && chars[index].is_ascii_digit() {
                index += 1;
            }
            let digits = index - start;
            let mut end = index;
            if end < chars.len() && chars[end] == '.' {
                end += 1;
                while end < chars.len() && chars[end].is_ascii_digit() {
                    end += 1;
                }
            }
            if digits >= 10 {
                out.push_str("<T>");
                index = end;
            } else {
                out.extend(&chars[start..index]);
            }
            continue;
        }
        out.push(chars[index]);
        index += 1;
    }
    out
}

/// The commands of the test's jobs, masked, one per line.
fn transcript(logged: &[Logged], identity: &str) -> String {
    let known: HashSet<&str> = [
        J_OK,
        J_RETRY,
        J_DROP,
        J_DEAD,
        J_INTERRUPT,
        J_SCHEDULED,
        J_RAW,
        J_PARKED,
        J_KILLED,
        J_KILL_ALL_1,
        J_KILL_ALL_2,
    ]
    .into_iter()
    .collect();
    let ours = |command: &Logged| {
        command
            .args
            .iter()
            .any(|arg| arg.contains(BULK_MARKER) || known.iter().any(|jid| arg.contains(jid)))
    };
    let mut lines = Vec::new();
    let mut in_script = false;
    for command in logged {
        let name = command.args.first().map(String::as_str).unwrap_or_default();
        let keep = if command.lua {
            in_script || ours(command)
        } else {
            in_script = matches!(name, "EVAL" | "EVALSHA") && ours(command);
            ours(command)
        };
        if keep {
            let args: Vec<String> = command
                .args
                .iter()
                .map(|arg| mask(arg, identity, &known))
                .collect();
            let source = if command.lua { "lua   " } else { "client" };
            lines.push(format!(
                "{source} {}",
                serde_json::to_string(&args).expect("arguments as JSON")
            ));
        }
    }
    lines.join("\n") + "\n"
}

#[tokio::test]
async fn jobs_without_features_send_the_same_commands() {
    let redis = TestRedis::start().await;
    let monitor = ::hench::redis_rs::Client::open(redis.url())
        .expect("monitor client")
        .get_async_monitor()
        .await
        .expect("MONITOR connection");
    let logged: Arc<Mutex<Vec<Logged>>> = Arc::default();
    let end_marker = "featureless-end-of-transcript";
    let recorder = {
        let logged = Arc::clone(&logged);
        tokio::spawn(async move {
            let mut lines = monitor.into_on_message::<String>();
            while let Some(line) = lines.next().await {
                let Some(command) = parse_monitor_line(&line) else {
                    continue;
                };
                let done = command.args.iter().any(|arg| arg == end_marker);
                logged.lock().await.push(command);
                if done {
                    break;
                }
            }
        })
    };

    let hench = redis
        .builder()
        .await
        .auto_register(false)
        .register(Succeeds)
        .register(Fails)
        .register(InterruptsOnce)
        .concurrency(1)
        .queues(["default"])
        // One beat at the start; later ones would record the running job in the work state.
        .heartbeat_interval(Duration::from_secs(20))
        .poll_interval_average(Duration::from_millis(50))
        .fetch_timeout(Duration::from_millis(50))
        .orphan_check_interval(Duration::from_secs(60))
        .build()
        .await
        .expect("hench");
    let client = hench.client().clone();
    let api = Api::from_client(client.clone());
    let pool = redis.pool().await;
    let launcher = hench.start().await.expect("start");
    let identity = launcher.identity().to_owned();

    // A success, acknowledged.
    Succeeds::set()
        .jid(J_OK)
        .perform_async(&client, vec![serde_json::json!(1)])
        .await
        .unwrap();
    wait_for_runs(1).await;
    // A failure retried later.
    Fails::set()
        .jid(J_RETRY)
        .retry(1)
        .perform_async(&client, ())
        .await
        .unwrap();
    wait_for_size(&pool, "retry", 1).await;
    // A failure without retries (dropped).
    Fails::set()
        .jid(J_DROP)
        .retry(false)
        .perform_async(&client, ())
        .await
        .unwrap();
    wait_for_runs(3).await;
    // A failure with no retry left (to the morgue).
    Fails::set()
        .jid(J_DEAD)
        .retry(0)
        .perform_async(&client, ())
        .await
        .unwrap();
    wait_for_size(&pool, "dead", 1).await;
    // An interruption pushed back to its queue, then a success.
    InterruptsOnce::set()
        .jid(J_INTERRUPT)
        .perform_async(&client, ())
        .await
        .unwrap();
    wait_for_runs(6).await;
    // A scheduled job, enqueued by the scheduler.
    Succeeds::set()
        .jid(J_SCHEDULED)
        .delay(Duration::from_millis(300))
        .perform_async(&client, vec![serde_json::json!(6)])
        .await
        .unwrap();
    wait_for_runs(7).await;
    // A raw push.
    let mut raw = JobPayload::new(Succeeds::NAME, vec![serde_json::json!("raw")]);
    raw.jid = J_RAW.to_owned();
    client.push(raw).await.unwrap();
    wait_for_runs(8).await;
    // A bulk push.
    Succeeds::perform_bulk(
        &client,
        vec![
            vec![serde_json::json!(format!("{BULK_MARKER}1"))],
            vec![serde_json::json!(format!("{BULK_MARKER}2"))],
        ],
    )
    .await
    .unwrap();
    wait_for_runs(10).await;
    // The dead job retried by hand: it fails again, back to the morgue.
    let dead = api
        .dead()
        .find_job(J_DEAD)
        .await
        .unwrap()
        .expect("dead job");
    assert!(dead.retry().await.unwrap());
    wait_for_runs(11).await;
    wait_for_size(&pool, "dead", 1).await;

    // The API's deletes and kills.
    let retry = api
        .retries()
        .find_job(J_RETRY)
        .await
        .unwrap()
        .expect("retry");
    assert!(retry.delete().await.unwrap());
    Succeeds::set()
        .jid(J_PARKED)
        .queue("parked")
        .perform_async(&client, vec![])
        .await
        .unwrap();
    let parked = api
        .queue("parked")
        .find_job(J_PARKED)
        .await
        .unwrap()
        .expect("parked job");
    assert!(parked.delete().await.unwrap());
    for jid in [J_KILLED, J_KILL_ALL_1, J_KILL_ALL_2] {
        Succeeds::set()
            .jid(jid)
            .delay(Duration::from_secs(3_600))
            .perform_async(&client, vec![])
            .await
            .unwrap();
    }
    let killed = api
        .scheduled()
        .find_job(J_KILLED)
        .await
        .unwrap()
        .expect("scheduled job");
    assert!(killed.kill().await.unwrap());
    assert_eq!(api.scheduled().kill_all(false).await.unwrap(), 2);

    launcher.stop().await.unwrap();
    let mut conn = pool.conn().await.unwrap();
    let _: String = ::hench::redis_rs::cmd("ECHO")
        .arg(end_marker)
        .query_async(&mut conn)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), recorder)
        .await
        .expect("the monitor saw the end marker")
        .unwrap();

    let logged = logged.lock().await;
    let actual = transcript(&logged, &identity);
    let golden_path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/golden/featureless_commands.txt"
    );
    if std::env::var_os("HENCH_RECORD_GOLDEN").is_some() {
        std::fs::write(golden_path, &actual).expect("write the golden transcript");
        return;
    }
    let expected = std::fs::read_to_string(golden_path).expect("the golden transcript");
    if actual != expected {
        let first = actual
            .lines()
            .zip(expected.lines())
            .position(|(a, e)| a != e)
            .unwrap_or_else(|| actual.lines().count().min(expected.lines().count()));
        panic!(
            "the commands of jobs without pro features changed (first difference at line {}):\n\
             expected: {:?}\nactual:   {:?}\n\nwhole transcript:\n{actual}",
            first + 1,
            expected.lines().nth(first),
            actual.lines().nth(first)
        );
    }
}

#[test]
fn monitor_lines_are_parsed() {
    let line = r#"1793550574.986581 [0 unix:/tmp/r.sock] "LPUSH" "queue:default" "{\"jid\":\"abc\"}" "a\\b" "\xc3\xa9""#;
    let logged = parse_monitor_line(line).unwrap();
    assert!(!logged.lua);
    assert_eq!(
        logged.args,
        ["LPUSH", "queue:default", r#"{"jid":"abc"}"#, r"a\b", "é"]
    );
    let lua = parse_monitor_line(r#"1.5 [0 lua] "LPOS" "k" "v""#).unwrap();
    assert!(lua.lua);
    let known: HashSet<&str> = [J_OK].into_iter().collect();
    assert_eq!(
        mask(
            "h:1:ab {\"jid\":\"00000000000000000000a001\",\"created_at\":1793550574986,\"x\":\"0123456789abcdef01234567\"} 1793550574.98 -10000 12",
            "h:1:ab",
            &known
        ),
        "<identity> {\"jid\":\"00000000000000000000a001\",\"created_at\":<T>,\"x\":\"<jid>\"} <T> -10000 12"
    );
}
