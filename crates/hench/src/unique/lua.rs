//! The Lua of unique jobs: the lock library (a frozen interface: the batches' `BATCH_COMMIT`
//! and the periodic jobs' `PERIODIC_FIRE` and `PERIODIC_ENQUEUE_NOW` include it too) and the
//! scripts of unique jobs (`UNIQUE_PUSH`, `ACK_UNIQUE`, `SETTLE_UNIQUE`, `PUSH_BACK_UNIQUE`,
//! `REMOVE_UNIQUE`, `KILL_UNIQUE`, `UNIQUE_RELEASE`, `UNIQUE_LOCK_OP`, `UNIQUE_RENEW`,
//! `UNIQUE_RELEASE_MANY`, `UNIQUE_TIDY`), assembled as `<prelude> + LIB [+ OUTCOME] + <body>`
//! (`docs/design/unique.md` §2.8).
//!
//! Every script declares the keys it touches, uses Redis 6.2 commands only, reads `TIME` once,
//! writes a job before its lock and before removing anything, runs bookkeeping (counters) and
//! the lock work of outcomes under `pcall`, compares the holder's jid before every release or
//! extension, never shortens a lock's time to live, and is idempotent or guarded (by the
//! in-progress list) so that a resend after a lost reply is harmless.

use crate::lua::{LuaScript, PRELUDE};

/// The lock helpers (`hu_check`, `hu_take`, `hu_hold`, `hu_release`, `hu_push_one`, `hu_count`,
/// `hu_trim`), included after the prelude by every script that touches a lock. Their names,
/// arguments and return values are frozen (`docs/design/unique.md` §2.8.2).
pub(crate) const LIB: &str = include_str!("lua/lib.lua");

/// The lock work of the outcome scripts (`hu_hold_ttl`, `hu_lock_work`), after the library.
const OUTCOME: &str = include_str!("lua/outcome.lua");

/// Writes a chunk of jobs and takes the locks of those that carry a digest.
pub(crate) static UNIQUE_PUSH: LuaScript =
    LuaScript::new("UNIQUE_PUSH", &[PRELUDE, LIB, include_str!("lua/push.lua")]);

/// Acknowledges a job (reliable fetch) and does its lock work.
pub(crate) static ACK_UNIQUE: LuaScript = LuaScript::new(
    "ACK_UNIQUE",
    &[PRELUDE, LIB, OUTCOME, include_str!("lua/ack.lua")],
);

/// Settles a failed job (reliable fetch) and holds or releases its lock.
pub(crate) static SETTLE_UNIQUE: LuaScript = LuaScript::new(
    "SETTLE_UNIQUE",
    &[PRELUDE, LIB, OUTCOME, include_str!("lua/settle.lua")],
);

/// Pushes a job back (reliable fetch) and holds its lock.
pub(crate) static PUSH_BACK_UNIQUE: LuaScript = LuaScript::new(
    "PUSH_BACK_UNIQUE",
    &[PRELUDE, LIB, OUTCOME, include_str!("lua/push_back.lua")],
);

/// The lock work of an outcome after its write (basic fetch).
pub(crate) static UNIQUE_LOCK_OP: LuaScript = LuaScript::new(
    "UNIQUE_LOCK_OP",
    &[PRELUDE, LIB, OUTCOME, include_str!("lua/lock_op.lua")],
);

/// Releases the lock of a jid.
pub(crate) static UNIQUE_RELEASE: LuaScript = LuaScript::new(
    "UNIQUE_RELEASE",
    &[PRELUDE, LIB, include_str!("lua/release.lua")],
);

/// Extends the locks of running jobs.
pub(crate) static UNIQUE_RENEW: LuaScript = LuaScript::new(
    "UNIQUE_RENEW",
    &[PRELUDE, LIB, include_str!("lua/renew.lua")],
);

/// Removes one job (the API's delete) and releases its lock.
pub(crate) static REMOVE_UNIQUE: LuaScript = LuaScript::new(
    "REMOVE_UNIQUE",
    &[PRELUDE, LIB, include_str!("lua/remove.lua")],
);

/// Moves one job to the morgue (the API's kill) and releases its lock.
pub(crate) static KILL_UNIQUE: LuaScript =
    LuaScript::new("KILL_UNIQUE", &[PRELUDE, LIB, include_str!("lua/kill.lua")]);

/// Releases many locks.
pub(crate) static UNIQUE_RELEASE_MANY: LuaScript = LuaScript::new(
    "UNIQUE_RELEASE_MANY",
    &[PRELUDE, LIB, include_str!("lua/release_many.lua")],
);

/// Removes index entries whose lock is gone.
pub(crate) static UNIQUE_TIDY: LuaScript =
    LuaScript::new("UNIQUE_TIDY", &[PRELUDE, LIB, include_str!("lua/tidy.lua")]);

/// Every script of unique jobs, for the tests that load and check each one.
#[cfg(any(test, feature = "test-util"))]
pub(crate) fn scripts() -> &'static [&'static LuaScript] {
    static SCRIPTS: [&LuaScript; 11] = [
        &UNIQUE_PUSH,
        &ACK_UNIQUE,
        &SETTLE_UNIQUE,
        &PUSH_BACK_UNIQUE,
        &UNIQUE_LOCK_OP,
        &UNIQUE_RELEASE,
        &UNIQUE_RENEW,
        &REMOVE_UNIQUE,
        &KILL_UNIQUE,
        &UNIQUE_RELEASE_MANY,
        &UNIQUE_TIDY,
    ];
    &SCRIPTS
}

/// The libraries as a script includes them, for the load test (`crate::lua`).
#[cfg(all(test, feature = "test-util"))]
pub(crate) fn library_checks() -> Vec<(String, String)> {
    vec![
        (
            "UNIQUE_LIB".to_owned(),
            format!("{PRELUDE}\n{LIB}\nreturn 1"),
        ),
        (
            "UNIQUE_OUTCOME".to_owned(),
            format!("{PRELUDE}\n{LIB}\n{OUTCOME}\nreturn 1"),
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_events_expire_with_the_other_minute_counters() {
        // `hu_count` sets the expiry of the minute hashes as a literal.
        let seconds = crate::keys::FEATURE_MINUTE_TTL.as_secs();
        assert!(LIB.contains(&format!("redis.call('EXPIRE', events, {seconds})")));
        assert!(crate::lua::rule_violations(LIB).is_empty());
        assert!(crate::lua::rule_violations(OUTCOME).is_empty());
    }

    #[test]
    fn the_frozen_functions_are_defined() {
        for function in [
            "hu_check(lock, jid)",
            "hu_take(lock, index, digest, jid, ttl_ms, now_ms, fields, at)",
            "hu_hold(lock, index, digest, jid, ttl_ms, now_ms, fields, at)",
            "hu_release(lock, index, digest, jid)",
            "hu_push_one(lock, index, events, digest, jid, class, ttl_ms, now_ms, fields, at, write)",
            "hu_count(events, class, event)",
            "hu_trim(index, now_ms, limit)",
        ] {
            assert!(
                LIB.contains(&format!("local function {function}")),
                "{function} is part of the frozen interface"
            );
        }
    }

    #[test]
    fn scripts_follow_the_conventions() {
        for script in scripts() {
            let name = script.name();
            assert!(name.contains("UNIQUE"), "{name}: feature-prefixed");
            let source = script.source();
            assert!(crate::lua::rule_violations(&source).is_empty(), "{name}");
            // `TIME` is read once: the prelude defines `h_now_ms()`, the body calls it at most
            // once, and nothing else reads the clock.
            let libraries = format!("{PRELUDE}\n{LIB}\n{OUTCOME}");
            let calls = source.matches("h_now_ms()").count()
                - libraries.matches("h_now_ms()").count().min(1);
            assert!(calls <= 1, "{name} reads the time {calls} times");
            assert_eq!(source.matches("'TIME'").count(), 1, "{name}");
            // `until` is a Lua keyword: never a table field.
            assert!(!source.contains(".until"), "{name}");
        }
    }
}

/// The library and the scripts against a private Redis: the lab checks of
/// `docs/design/unique.md` §2.8 and `pro-features.md` Appendix A, then each script's
/// scenarios, resends (lost replies) and concurrent callers.
#[cfg(all(test, feature = "test-util"))]
mod redis_tests;
