//! The Lua of rate limiting (`docs/design/ratelimit.md` §2.9): `LIMIT_ACQUIRE`,
//! `LIMIT_RELEASE`, `LIMIT_RENEW`, `LIMIT_ADJUST`, `LIMIT_USAGE`, `LIMIT_RESET`, `LIMIT_TRIM`
//! and `LIMIT_FETCH_GATE` (queue limits' fetch, §2.7), each assembled by [`crate::lua`] as
//! `<prelude> + LIB + <body>`. (Clearing a wait line is a single `HDEL`.)
//!
//! Every script reads the time once, from Redis (`ARGV[1]` may fix it, for tests only: the
//! wrappers in [`crate::limit::scripts`] send `''` outside tests), checks every limit before its
//! first write, makes the grant its first write (under `OOM` nothing is granted), runs its
//! bookkeeping under `pcall` (a refusal still answers under `OOM`), trims the index last, never
//! shortens an expiry, and gives every key it writes an expiry.

use crate::lua::{LuaScript, PRELUDE};

/// The helpers of the limiter scripts (`hl_now`, `hl_flag`, `hl_check`, `hl_grant`, `hl_touch`,
/// `hl_count`, `hl_minute`, `hl_parse_signature`), after the prelude.
pub(crate) const LIB: &str = include_str!("lua/lib.lua");

/// Checks every limit of a call and grants all of them or none (with the wait line and the
/// bookkeeping).
pub(crate) static LIMIT_ACQUIRE: LuaScript = LuaScript::new(
    "LIMIT_ACQUIRE",
    &[PRELUDE, LIB, include_str!("lua/acquire.lua")],
);

/// Gives concurrency leases back.
pub(crate) static LIMIT_RELEASE: LuaScript = LuaScript::new(
    "LIMIT_RELEASE",
    &[PRELUDE, LIB, include_str!("lua/release.lua")],
);

/// Extends (or restores) the leases of permits still held.
pub(crate) static LIMIT_RENEW: LuaScript = LuaScript::new(
    "LIMIT_RENEW",
    &[PRELUDE, LIB, include_str!("lua/renew.lua")],
);

/// Corrects the cost of a points grant.
pub(crate) static LIMIT_ADJUST: LuaScript = LuaScript::new(
    "LIMIT_ADJUST",
    &[PRELUDE, LIB, include_str!("lua/adjust.lua")],
);

/// Reads the current use of limit identities.
pub(crate) static LIMIT_USAGE: LuaScript = LuaScript::new(
    "LIMIT_USAGE",
    &[PRELUDE, LIB, include_str!("lua/usage.lua")],
);

/// Deletes the state and counters of limit identities.
pub(crate) static LIMIT_RESET: LuaScript = LuaScript::new(
    "LIMIT_RESET",
    &[PRELUDE, LIB, include_str!("lua/reset.lua")],
);

/// Drops the index entries of identities whose state and counters are both gone.
pub(crate) static LIMIT_TRIM: LuaScript =
    LuaScript::new("LIMIT_TRIM", &[PRELUDE, LIB, include_str!("lua/trim.lua")]);

/// The non-blocking step of a fetch from a capsule with limited queues: takes the next job of
/// the first open queue whose limits grant one, charging them in the same step.
pub(crate) static LIMIT_FETCH_GATE: LuaScript = LuaScript::new(
    "LIMIT_FETCH_GATE",
    &[PRELUDE, LIB, include_str!("lua/fetch_gate.lua")],
);

/// Every script of rate limiting, for the tests that load and check each one.
#[cfg(any(test, feature = "test-util"))]
pub(crate) fn scripts() -> &'static [&'static LuaScript] {
    static SCRIPTS: [&LuaScript; 8] = [
        &LIMIT_ACQUIRE,
        &LIMIT_RELEASE,
        &LIMIT_RENEW,
        &LIMIT_ADJUST,
        &LIMIT_USAGE,
        &LIMIT_RESET,
        &LIMIT_TRIM,
        &LIMIT_FETCH_GATE,
    ];
    &SCRIPTS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_minute_counters_expire_with_the_others() {
        // `hl_minute` sets the expiry of the minute hashes as a literal.
        let seconds = crate::keys::FEATURE_MINUTE_TTL.as_secs();
        assert!(LIB.contains(&format!("redis.call, 'EXPIRE', events, {seconds})")));
    }

    #[test]
    fn the_scripts_follow_the_rules() {
        assert!(crate::lua::rule_violations(LIB).is_empty());
        // Helpers carry the feature's prefix: no clash with another library.
        for line in LIB.lines() {
            if let Some(name) = line.strip_prefix("local function ") {
                assert!(name.starts_with("hl_"), "{name}");
            }
        }
        for script in scripts() {
            let source = script.source();
            assert!(
                crate::lua::rule_violations(&source).is_empty(),
                "{}",
                script.name()
            );
            assert!(script.name().starts_with("LIMIT_"), "{}", script.name());
            // `TIME` is read once, through `hl_now` (or not at all).
            assert!(source.matches("hl_now(ARGV[1])").count() <= 1);
        }
    }
}
