//! The Lua scripts of the pro features (unique jobs, periodic jobs, batches, rate limiting):
//! the shared prelude, the script builder, the list of every script (for the tests that load
//! each one into a real `redis-server` and check its commands) and the start-up check of
//! effects replication. See `docs/design/pro-features.md` §2.2.
//!
//! A script is assembled from parts: the prelude (`prelude.lua`: `h_now_ms`, `h_score`,
//! `h_err`, `h_extend_at_least`), the libraries it needs (each feature's own, plus the unique
//! jobs' frozen library for scripts that take locks) and its body, all embedded with
//! `include_str!`. Every script follows the conventions of the design: declared keys only (no
//! `SCAN`, no key built in Lua), Redis 6.2 commands only (no `PEXPIRE ... GT`, no script flags),
//! `TIME` read once and before the first write, checks before writes, the first write being the
//! one whose refusal under `OOM` leaves a consistent state, bookkeeping under `pcall`, index
//! trims last, and idempotence (or a guard) so that a resend after a lost reply is harmless.
//!
//! The scripts that existed before the pro features (fetch, settle, push back, the scheduler's
//! lease, the API's moves, the counter flushes) stay where they are, byte for byte: jobs that
//! use no feature run exactly the commands they ran before (a test pins their SHA-1s).

use std::collections::HashMap;
use std::sync::OnceLock;

use ::redis::Script;

use crate::redis::RedisPool;

/// The prelude every script of the pro features starts with.
pub(crate) const PRELUDE: &str = include_str!("prelude.lua");

/// A script of the pro features: its name in code and logs (`UNIQUE_PUSH`, `BATCH_COMMIT`,
/// `LIMIT_ACQUIRE`, ...: feature-prefixed, so that generic names never collide) and the parts
/// it is assembled from, joined by line breaks. The `redis::Script` (source and SHA-1) is built
/// once, on first use; `invoke_async` retries a `NOSCRIPT` reply with the source, which covers
/// failovers to a server that never saw the script.
///
/// ```ignore
/// pub(crate) static UNIQUE_PUSH: LuaScript =
///     LuaScript::new("UNIQUE_PUSH", &[lua::PRELUDE, LIB, include_str!("lua/push.lua")]);
/// ```
pub(crate) struct LuaScript {
    name: &'static str,
    parts: &'static [&'static str],
    script: OnceLock<Script>,
}

impl LuaScript {
    /// A script named `name` assembled from `parts` (the prelude first, then libraries, then
    /// the body).
    pub(crate) const fn new(name: &'static str, parts: &'static [&'static str]) -> Self {
        Self {
            name,
            parts,
            script: OnceLock::new(),
        }
    }

    /// The script's name, for logs.
    pub(crate) fn name(&self) -> &'static str {
        self.name
    }

    /// The assembled source.
    pub(crate) fn source(&self) -> String {
        self.parts.join("\n")
    }

    /// The script, ready to invoke (`.key(..).arg(..).invoke_async(..)`).
    pub(crate) fn script(&self) -> &Script {
        self.script.get_or_init(|| Script::new(&self.source()))
    }
}

/// One run of a script with its keys and arguments, for [`run_pipelined`].
#[derive(Debug, Clone)]
pub(crate) struct ScriptCall {
    /// The script.
    pub(crate) script: &'static LuaScript,
    /// Its `KEYS`.
    pub(crate) keys: Vec<String>,
    /// Its `ARGV`.
    pub(crate) args: Vec<String>,
}

impl ScriptCall {
    /// `EVALSHA <sha1> <number of keys> <keys...> <args...>`.
    fn evalsha(&self) -> ::redis::Cmd {
        let mut command = ::redis::cmd("EVALSHA");
        command
            .arg(self.script.script().get_hash())
            .arg(self.keys.len())
            .arg(&self.keys)
            .arg(&self.args);
        command
    }
}

/// Sends `calls` in one pipeline (one round trip; each script still runs on its own) and
/// returns each call's reply or error, in order. A server that does not know a script
/// (`NOSCRIPT`: it restarted, or a failover reached a replica that never saw it) gets every
/// script of the calls loaded and the pipeline sent again, once: the calls must be harmless to
/// repeat (the heartbeat's renewals are). A dropped connection or a failover is retried once by
/// [`RedisPool::with_retry`], like any command.
pub(crate) async fn run_pipelined(
    redis: &RedisPool,
    calls: &[ScriptCall],
) -> crate::error::Result<Vec<::redis::RedisResult<::redis::Value>>> {
    if calls.is_empty() {
        return Ok(Vec::new());
    }
    let mut pipe = ::redis::pipe();
    pipe.ignore_errors();
    for call in calls {
        pipe.add_command(call.evalsha());
    }
    let pipe = &pipe;
    let replies: Vec<::redis::RedisResult<::redis::Value>> = redis
        .with_retry(|mut conn| async move { pipe.query_async(&mut conn).await })
        .await?;
    let unknown = |reply: &::redis::RedisResult<::redis::Value>| {
        reply.as_ref().is_err_and(|error| {
            error.kind() == ::redis::ErrorKind::Server(::redis::ServerErrorKind::NoScript)
        })
    };
    if !replies.iter().any(unknown) {
        return Ok(replies);
    }
    let mut scripts: Vec<&'static LuaScript> = Vec::new();
    for call in calls {
        if !scripts
            .iter()
            .any(|script| std::ptr::eq(*script, call.script))
        {
            scripts.push(call.script);
        }
    }
    tracing::debug!(
        scripts = ?scripts.iter().map(|script| script.name()).collect::<Vec<_>>(),
        "Redis did not know the scripts of a pipeline; loading them"
    );
    let scripts = &scripts;
    redis
        .with_retry(|mut conn| async move {
            for script in scripts {
                script.script().load_async(&mut conn).await?;
            }
            pipe.query_async(&mut conn).await
        })
        .await
}

impl std::fmt::Debug for LuaScript {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LuaScript")
            .field("name", &self.name)
            .field("parts", &self.parts.len())
            .finish_non_exhaustive()
    }
}

/// Every script of the pro features, from the features' lists (each feature adds its scripts
/// to its own `scripts()`), for the tests: each must load into Redis and use allowed commands
/// only, and `test_util::feature_scripts` names them in transcripts.
#[cfg(any(test, feature = "test-util"))]
pub(crate) fn all_scripts() -> Vec<&'static LuaScript> {
    let mut scripts = Vec::new();
    scripts.extend_from_slice(crate::unique::lua::scripts());
    scripts.extend_from_slice(crate::periodic::lua::scripts());
    scripts.extend_from_slice(crate::batch::lua::scripts());
    scripts.extend_from_slice(crate::limit::lua::scripts());
    // The API's and the scheduler's lock extension of a job moved later, and the API's search
    // of a set in Redis.
    scripts.push(&crate::api::RESCORE_UNIQUE);
    scripts.push(&crate::api::SCAN_CONTAINING);
    scripts
}

/// Warns at start-up when Redis runs scripts without effects replication: every script of the
/// pro features reads `TIME` before it writes, which Redis 6.2 refuses with
/// `lua-replicate-commands no` (effects replication is the only mode from Redis 7 on, where
/// the setting no longer exists). Skipped, with a debug log, when `CONFIG` is refused (managed
/// services often rename or disable it).
pub(crate) async fn check_effects_replication(redis: &RedisPool) {
    let reply = redis
        .with_retry(|mut conn| async move {
            ::redis::cmd("CONFIG")
                .arg("GET")
                .arg("lua-replicate-commands")
                .query_async::<HashMap<String, String>>(&mut conn)
                .await
        })
        .await;
    match reply {
        Ok(settings) => {
            if settings
                .get("lua-replicate-commands")
                .is_some_and(|value| value.trim().eq_ignore_ascii_case("no"))
            {
                tracing::warn!(
                    "Redis runs scripts with lua-replicate-commands no: the scripts of unique \
                     jobs, periodic jobs, batches and rate limits read TIME before writing and \
                     will fail; set lua-replicate-commands yes (or use Redis 7)"
                );
            }
        }
        Err(error) => tracing::debug!(
            %error,
            "CONFIG is not available; skipping the lua-replicate-commands check"
        ),
    }
}

/// Commands no script of the pro features may call: commands that touch keys a script does not
/// declare (`KEYS`, the `SCAN` family, flushes), commands newer than Redis 6.2, and commands
/// that make no sense or are refused in scripts.
#[cfg(test)]
const FORBIDDEN_COMMANDS: [&str; 27] = [
    "KEYS",
    "SCAN",
    "SSCAN",
    "HSCAN",
    "ZSCAN",
    "FLUSHALL",
    "FLUSHDB",
    "RANDOMKEY",
    "MIGRATE",
    "LMPOP",
    "BLMPOP",
    "ZMPOP",
    "BZMPOP",
    "SINTERCARD",
    "ZINTERCARD",
    "EXPIRETIME",
    "PEXPIRETIME",
    "FUNCTION",
    "FCALL",
    "FCALL_RO",
    "EVAL",
    "EVALSHA",
    "SCRIPT",
    "CONFIG",
    "DEBUG",
    "SHUTDOWN",
    "WAIT",
];

/// What in `source` breaks the scripts' rules: a forbidden command, an expiry option of Redis 7
/// (`EXPIRE`/`PEXPIRE` with `NX`, `XX`, `GT` or `LT`), or script flags (a `#!lua` shebang, Redis
/// 7).
#[cfg(test)]
pub(crate) fn rule_violations(source: &str) -> Vec<String> {
    let mut violations = Vec::new();
    for (number, line) in source.lines().enumerate() {
        let code = line.split("--").next().unwrap_or_default();
        let upper = code.to_ascii_uppercase();
        if number == 0 && line.starts_with("#!") {
            violations.push("script flags (#! shebang, Redis 7)".to_owned());
        }
        for command in FORBIDDEN_COMMANDS {
            for quote in ['\'', '"'] {
                for call in ["REDIS.CALL(", "REDIS.PCALL("] {
                    if upper.contains(&format!("{call}{quote}{command}{quote}")) {
                        violations.push(format!("line {}: {command}", number + 1));
                    }
                }
            }
        }
        if upper.contains("EXPIRE'") || upper.contains("EXPIRE\"") {
            for option in ["NX", "XX", "GT", "LT"] {
                if upper.contains(&format!("'{option}'"))
                    || upper.contains(&format!("\"{option}\""))
                {
                    violations.push(format!(
                        "line {}: EXPIRE option {option} (Redis 7)",
                        number + 1
                    ));
                }
            }
        }
    }
    violations
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scripts_are_assembled_from_their_parts() {
        static PARTS: LuaScript = LuaScript::new("TEST_PARTS", &[PRELUDE, "return h_score(1500)"]);
        assert_eq!(PARTS.name(), "TEST_PARTS");
        let source = PARTS.source();
        assert!(source.starts_with(PRELUDE));
        assert!(source.ends_with("\nreturn h_score(1500)"));
        assert_eq!(
            PARTS.script().get_hash(),
            Script::new(&source).get_hash(),
            "the script's SHA-1 is the assembled source's"
        );
        assert!(format!("{PARTS:?}").contains("TEST_PARTS"));
    }

    #[test]
    fn the_rules_catch_forbidden_commands() {
        assert!(rule_violations(PRELUDE).is_empty());
        for script in all_scripts() {
            let violations = rule_violations(&script.source());
            assert!(violations.is_empty(), "{}: {violations:?}", script.name());
        }
        assert_eq!(
            rule_violations("redis.call('KEYS', '*')\nredis.pcall(\"scan\", 0)").len(),
            2
        );
        assert_eq!(
            rule_violations("redis.call('PEXPIRE', KEYS[1], 10, 'GT')").len(),
            1
        );
        assert_eq!(rule_violations("#!lua flags=no-writes\nreturn 1").len(), 1);
        // Comments and helper names are not commands.
        assert!(rule_violations("-- redis.call('KEYS')\nlocal scan = 1").is_empty());
        assert!(rule_violations("redis.call('ZADD', KEYS[1], 'NX', 1, 'm')").is_empty());
    }

    /// The scripts of hench before the pro features, by SHA-1 of their source: jobs that use no
    /// feature must keep running exactly these (see also `tests/featureless.rs`, which pins the
    /// commands themselves).
    #[test]
    fn scripts_from_before_the_features_are_unchanged() {
        let pinned: [(&str, &Script, &str); 13] = [
            (
                "RELIABLE_FETCH",
                &crate::fetch::RELIABLE_FETCH_SCRIPT,
                "647174909457748f7ab2d2810dcd8f61c58f4b85",
            ),
            (
                "BASIC_FETCH",
                &crate::fetch::BASIC_FETCH_SCRIPT,
                "b720f5ac9360adf019fef728b360f18867b066a1",
            ),
            (
                "REQUEUE",
                &crate::fetch::REQUEUE_SCRIPT,
                "26a86a79977ad90a65e7204e6188f8cf02f2f90a",
            ),
            (
                "BURY",
                &crate::fetch::BURY_SCRIPT,
                "366c1601165e0ca4233e533473b3bbb8e923fa27",
            ),
            (
                "RELEASE",
                &crate::fetch::RELEASE_SCRIPT,
                "ab0602db39f44097b192d20280e124d229bffc35",
            ),
            (
                "PUSH_BACK",
                &crate::fetch::PUSH_BACK_SCRIPT,
                "a2d99c853219ee511ad9c2a0a159c977ed5bcd87",
            ),
            (
                "SETTLE",
                &crate::fetch::SETTLE_SCRIPT,
                "24db3aa51a30c4374ed527bc842345297cb60d4f",
            ),
            (
                "REFRESH_LIVENESS",
                &crate::launcher::REFRESH_LIVENESS_SCRIPT,
                "132ef6eb2383f137ae3cad926e39252d0ba99d23",
            ),
            (
                "LEASE_DUE",
                &crate::scheduled::LEASE_DUE_SCRIPT,
                "28ea113c4882a9018505f0bd0b5777830a61d180",
            ),
            (
                "MOVE_TO_MORGUE",
                &crate::api::MOVE_TO_MORGUE,
                "003b512bb490c4bd4d7cea4d7cbf4fb7fa46be90",
            ),
            (
                "MOVE_OUT",
                &crate::api::MOVE_OUT,
                "e1cfc2f17257a913e7ddd42c4a94cfcf3d7589a9",
            ),
            (
                "MOVE_LOWEST",
                &crate::api::MOVE_LOWEST,
                "9e5fa7cfa58baddec3255900add480360c6b2a65",
            ),
            (
                "APPLY_ONCE",
                &crate::flush::APPLY_ONCE,
                "7a55f89ca0205bd31aa488cd61de9190c7443862",
            ),
        ];
        for (name, script, sha) in pinned {
            assert_eq!(script.get_hash(), sha, "{name} changed");
        }
    }
}

/// Tests against a private Redis.
#[cfg(all(test, feature = "test-util"))]
mod redis_tests {
    use super::*;
    use crate::test_util::TestRedis;

    /// Runs the test script `body` after the prelude (on the test's private Redis), with
    /// `keys` and `args`.
    async fn run_lua<T: ::redis::FromRedisValue>(
        pool: &RedisPool,
        body: &str,
        keys: &[&str],
        args: &[&str],
    ) -> T {
        let script = Script::new(&format!("{PRELUDE}\n{body}"));
        let mut invocation = script.prepare_invoke();
        for key in keys {
            invocation.key(*key);
        }
        for arg in args {
            invocation.arg(*arg);
        }
        let mut conn = pool.conn().await.unwrap();
        invocation.invoke_async(&mut conn).await.unwrap()
    }

    #[tokio::test]
    async fn every_script_loads_into_redis() {
        let redis = TestRedis::start().await;
        let pool = redis.pool().await;
        let mut conn = pool.conn().await.unwrap();
        let mut sources: Vec<(String, String)> = all_scripts()
            .into_iter()
            .map(|script| (script.name().to_owned(), script.source()))
            .collect();
        // The libraries on their own, as a script would include them.
        sources.extend(crate::unique::lua::library_checks());
        sources.push(("PRELUDE".to_owned(), format!("{PRELUDE}\nreturn 1")));
        for (name, source) in sources {
            let loaded: Result<String, _> = ::redis::cmd("SCRIPT")
                .arg("LOAD")
                .arg(&source)
                .query_async(&mut conn)
                .await;
            let sha = loaded.unwrap_or_else(|error| panic!("{name} does not load: {error}"));
            assert_eq!(sha, Script::new(&source).get_hash(), "{name}");
        }
    }

    #[tokio::test]
    async fn the_prelude_helpers_work() {
        let redis = TestRedis::start().await;
        let pool = redis.pool().await;
        // h_now_ms reads Redis's clock.
        let now: i64 = run_lua(&pool, "return h_now_ms()", &[], &[]).await;
        let local = crate::payload::now_millis();
        assert!((now - local).abs() < 5_000, "{now} vs {local}");
        let score: String = run_lua(&pool, "return h_score(1234567)", &[], &[]).await;
        assert_eq!(score, "1234.567");
        let _: () = run_lua(
            &pool,
            "redis.call('RPUSH', KEYS[1], 'x') return nil",
            &["list"],
            &[],
        )
        .await;
        let wrong: String = run_lua(
            &pool,
            "local ok, e = pcall(redis.call, 'INCR', KEYS[1]) return h_err(e)",
            &["list"],
            &[],
        )
        .await;
        assert!(wrong.contains("WRONGTYPE"), "{wrong}");
        // h_extend_at_least never shortens, gives a missing expiry one, ignores missing keys.
        let extend = "return h_extend_at_least(KEYS[1], tonumber(ARGV[1]))";
        let _: () = run_lua(
            &pool,
            "redis.call('SET', KEYS[1], 1, 'PX', 60000) return nil",
            &["k"],
            &[],
        )
        .await;
        let changed: i64 = run_lua(&pool, extend, &["k"], &["1000"]).await;
        assert_eq!(changed, 0);
        let changed: i64 = run_lua(&pool, extend, &["k"], &["120000"]).await;
        assert_eq!(changed, 1);
        let ttl: i64 = run_lua(&pool, "return redis.call('PTTL', KEYS[1])", &["k"], &[]).await;
        assert!(ttl > 60_000 && ttl <= 120_000, "{ttl}");
        let _: () = run_lua(
            &pool,
            "redis.call('SET', KEYS[1], 1) return nil",
            &["forever"],
            &[],
        )
        .await;
        let changed: i64 = run_lua(&pool, extend, &["forever"], &["5000"]).await;
        assert_eq!(changed, 1);
        let changed: i64 = run_lua(&pool, extend, &["missing"], &["5000"]).await;
        assert_eq!(changed, 0);
        let exists: i64 = run_lua(
            &pool,
            "return redis.call('EXISTS', KEYS[1])",
            &["missing"],
            &[],
        )
        .await;
        assert_eq!(exists, 0);
    }

    #[tokio::test]
    async fn the_effects_replication_check_tolerates_redis_7() {
        // Redis 7 has no `lua-replicate-commands`: nothing to warn about, and no error.
        let redis = TestRedis::start().await;
        check_effects_replication(&redis.pool().await).await;
        // Without CONFIG, the check is skipped.
        let renamed = TestRedis::start_with_args(&["--rename-command", "CONFIG", ""]).await;
        check_effects_replication(&renamed.pool().await).await;
    }
}
