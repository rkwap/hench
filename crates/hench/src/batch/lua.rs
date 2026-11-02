//! The Lua of batches (`docs/design/batches.md` §2.5): `BATCH_COMMIT`, `BATCH_SUCCEEDED`,
//! `BATCH_FAILED`, `BATCH_SKIPPED`, `BATCH_DISPATCH`, `BATCH_TOUCH`, `BATCH_CANCEL` and
//! `BATCH_DELETE`, each assembled by [`crate::lua`] as `<prelude> + LIB + <body>`;
//! `BATCH_COMMIT` also includes the unique jobs' frozen lock library, with which it takes the
//! locks of unique members in the same step as their push.
//!
//! Every script declares the keys it touches, reads the time once (from Redis), checks before
//! its first write (so that under `OOM`, or with a key of the wrong type, nothing is written),
//! runs its bookkeeping (the per-minute counters) under `pcall` as its last writes, gives every
//! key it may create the batch's expiry, never shortens an expiry when it refreshes one, and is
//! idempotent (keyed by jid, commit id or outbox entry), so that a resend after a lost reply is
//! harmless.

use crate::lua::{LuaScript, PRELUDE};

/// The helpers of the batch scripts (`hb_wrong_type`, `hb_inherit_ttl`, `hb_count`,
/// `hb_count_events`, `hb_evaluate`, `hb_touch`, `hb_finish`), after the prelude.
pub(crate) const LIB: &str = include_str!("lua/lib.lua");

/// Writes a batch definition: new batches or a reopened one, their members and their locks.
pub(crate) static BATCH_COMMIT: LuaScript = LuaScript::new(
    "BATCH_COMMIT",
    &[
        PRELUDE,
        LIB,
        crate::unique::lua::LIB,
        include_str!("lua/commit.lua"),
    ],
);

/// Records a member's success.
pub(crate) static BATCH_SUCCEEDED: LuaScript = LuaScript::new(
    "BATCH_SUCCEEDED",
    &[PRELUDE, LIB, include_str!("lua/succeeded.lua")],
);

/// Records a member's failure (`retry` or `dead`).
pub(crate) static BATCH_FAILED: LuaScript = LuaScript::new(
    "BATCH_FAILED",
    &[PRELUDE, LIB, include_str!("lua/failed.lua")],
);

/// Records a skipped member (its batch cancelled, or an operator's decision).
pub(crate) static BATCH_SKIPPED: LuaScript = LuaScript::new(
    "BATCH_SKIPPED",
    &[PRELUDE, LIB, include_str!("lua/skipped.lua")],
);

/// Pushes the callbacks of the events a batch fired and updates its parent.
pub(crate) static BATCH_DISPATCH: LuaScript = LuaScript::new(
    "BATCH_DISPATCH",
    &[PRELUDE, LIB, include_str!("lua/dispatch.lua")],
);

/// Refreshes the expiry of an ancestor.
pub(crate) static BATCH_TOUCH: LuaScript = LuaScript::new(
    "BATCH_TOUCH",
    &[PRELUDE, LIB, include_str!("lua/touch.lua")],
);

/// Cancels one batch (the caller walks its descendants).
pub(crate) static BATCH_CANCEL: LuaScript = LuaScript::new(
    "BATCH_CANCEL",
    &[PRELUDE, LIB, include_str!("lua/cancel.lua")],
);

/// Deletes one batch's data (the caller deletes its descendants first).
pub(crate) static BATCH_DELETE: LuaScript = LuaScript::new(
    "BATCH_DELETE",
    &[PRELUDE, LIB, include_str!("lua/delete.lua")],
);

/// Every script of batches, for the tests that load and check each one.
#[cfg(any(test, feature = "test-util"))]
pub(crate) fn scripts() -> &'static [&'static LuaScript] {
    static SCRIPTS: [&LuaScript; 8] = [
        &BATCH_COMMIT,
        &BATCH_SUCCEEDED,
        &BATCH_FAILED,
        &BATCH_SKIPPED,
        &BATCH_DISPATCH,
        &BATCH_TOUCH,
        &BATCH_CANCEL,
        &BATCH_DELETE,
    ];
    &SCRIPTS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_minute_counters_expire_with_the_others() {
        // `hb_count` sets the expiry of the minute hashes as a literal.
        assert!(LIB.contains(&format!(
            "redis.call('EXPIRE', minute, {})",
            crate::keys::FEATURE_MINUTE_TTL.as_secs()
        )));
    }

    #[test]
    fn scripts_follow_the_rules() {
        for script in scripts() {
            let violations = crate::lua::rule_violations(&script.source());
            assert!(violations.is_empty(), "{}: {violations:?}", script.name());
            assert!(script.name().starts_with("BATCH_"));
        }
        // The commit takes the locks of unique members with the frozen library.
        assert!(BATCH_COMMIT.source().contains("local function hu_check"));
        assert!(BATCH_COMMIT.source().contains("pcall(hu_take"));
    }
}
