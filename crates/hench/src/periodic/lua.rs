//! The Lua of periodic jobs (`docs/design/periodic.md` §2.5): `PERIODIC_LEASE`, `PERIODIC_FIRE`,
//! `PERIODIC_SYNC`, `PERIODIC_UPSERT`, `PERIODIC_REMOVE`, `PERIODIC_PAUSE`, `PERIODIC_UNPAUSE`,
//! `PERIODIC_ENQUEUE_NOW`, `PERIODIC_RECORD_RUN`, `PERIODIC_ERROR` and `PERIODIC_UNINDEX`,
//! assembled by
//! [`crate::lua`] as `<prelude> [+ periodic lib + unique lib] + <body>`: the two scripts that
//! write jobs (`FIRE`, `ENQUEUE_NOW`) take the jobs' unique locks with the frozen unique library.
//!
//! Every script declares the keys it touches (a schedule's keys, the queues and the lock of the
//! job it writes), uses Redis 6.2 commands only, checks before it writes (the job is the first
//! write of the scripts that push one: under `OOM` nothing is written), and is idempotent or
//! guarded by a compare-and-set (`FIRE` on the schedule's `next`, `UPSERT` and `UNPAUSE` on its
//! digest, `SYNC` on its group's members, `RECORD_RUN` on a token), except `ENQUEUE_NOW`, which
//! its caller sends once.

use crate::lua::{LuaScript, PRELUDE};

/// The helpers of the scripts that write jobs (`hp_write`, `hp_history`, `hp_lock_ttl`).
#[cfg(feature = "periodic")]
const LIB: &str = include_str!("lua/lib.lua");

/// Leases the earliest due schedule.
#[cfg(feature = "periodic")]
pub(crate) static PERIODIC_LEASE: LuaScript =
    LuaScript::new("PERIODIC_LEASE", &[PRELUDE, include_str!("lua/lease.lua")]);

/// Enqueues one tick's job and advances the schedule, at most once per tick.
#[cfg(feature = "periodic")]
pub(crate) static PERIODIC_FIRE: LuaScript = LuaScript::new(
    "PERIODIC_FIRE",
    &[
        PRELUDE,
        LIB,
        crate::unique::lua::LIB,
        include_str!("lua/fire.lua"),
    ],
);

/// Registers a group's crontab.
#[cfg(feature = "periodic")]
pub(crate) static PERIODIC_SYNC: LuaScript =
    LuaScript::new("PERIODIC_SYNC", &[PRELUDE, include_str!("lua/sync.lua")]);

/// Creates or updates one schedule.
#[cfg(feature = "periodic")]
pub(crate) static PERIODIC_UPSERT: LuaScript = LuaScript::new(
    "PERIODIC_UPSERT",
    &[PRELUDE, include_str!("lua/upsert.lua")],
);

/// Deletes a schedule.
#[cfg(feature = "periodic")]
pub(crate) static PERIODIC_REMOVE: LuaScript = LuaScript::new(
    "PERIODIC_REMOVE",
    &[PRELUDE, include_str!("lua/remove.lua")],
);

/// Pauses a schedule.
#[cfg(feature = "periodic")]
pub(crate) static PERIODIC_PAUSE: LuaScript =
    LuaScript::new("PERIODIC_PAUSE", &[PRELUDE, include_str!("lua/pause.lua")]);

/// Resumes a schedule.
#[cfg(feature = "periodic")]
pub(crate) static PERIODIC_UNPAUSE: LuaScript = LuaScript::new(
    "PERIODIC_UNPAUSE",
    &[PRELUDE, include_str!("lua/unpause.lua")],
);

/// Pushes a schedule's job outside its timetable.
#[cfg(feature = "periodic")]
pub(crate) static PERIODIC_ENQUEUE_NOW: LuaScript = LuaScript::new(
    "PERIODIC_ENQUEUE_NOW",
    &[
        PRELUDE,
        LIB,
        crate::unique::lua::LIB,
        include_str!("lua/enqueue_now.lua"),
    ],
);

/// Records why a schedule cannot fire.
#[cfg(feature = "periodic")]
pub(crate) static PERIODIC_ERROR: LuaScript =
    LuaScript::new("PERIODIC_ERROR", &[PRELUDE, include_str!("lua/error.lua")]);

/// Removes a deleted or paused schedule from the due index.
#[cfg(feature = "periodic")]
pub(crate) static PERIODIC_UNINDEX: LuaScript = LuaScript::new(
    "PERIODIC_UNINDEX",
    &[PRELUDE, include_str!("lua/unindex.lua")],
);

/// Records how a schedule's job ended (sent by every process, with or without the `periodic`
/// cargo feature: a worker runs jobs other processes enqueued).
pub(crate) static PERIODIC_RECORD_RUN: LuaScript = LuaScript::new(
    "PERIODIC_RECORD_RUN",
    &[PRELUDE, include_str!("lua/record_run.lua")],
);

/// Every script of periodic jobs, for the tests that load and check each one.
#[cfg(any(test, feature = "test-util"))]
pub(crate) fn scripts() -> &'static [&'static LuaScript] {
    #[cfg(feature = "periodic")]
    static SCRIPTS: [&LuaScript; 11] = [
        &PERIODIC_LEASE,
        &PERIODIC_FIRE,
        &PERIODIC_SYNC,
        &PERIODIC_UPSERT,
        &PERIODIC_REMOVE,
        &PERIODIC_PAUSE,
        &PERIODIC_UNPAUSE,
        &PERIODIC_ENQUEUE_NOW,
        &PERIODIC_ERROR,
        &PERIODIC_UNINDEX,
        &PERIODIC_RECORD_RUN,
    ];
    #[cfg(not(feature = "periodic"))]
    static SCRIPTS: [&LuaScript; 1] = [&PERIODIC_RECORD_RUN];
    &SCRIPTS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scripts_follow_the_conventions() {
        for script in scripts() {
            let source = script.source();
            assert!(
                crate::lua::rule_violations(&source).is_empty(),
                "{}",
                script.name()
            );
            assert!(script.name().starts_with("PERIODIC_"), "{}", script.name());
        }
        #[cfg(feature = "periodic")]
        {
            // Helpers carry the `hp_` prefix, so that the unique library's `hu_` never clashes.
            for line in LIB.lines() {
                if let Some(rest) = line.strip_prefix("local function ") {
                    assert!(rest.starts_with("hp_"), "{line}");
                }
            }
            // The scripts that write jobs take their locks with the frozen library.
            for script in [&PERIODIC_FIRE, &PERIODIC_ENQUEUE_NOW] {
                assert!(
                    script.source().contains("hu_push_one("),
                    "{}",
                    script.name()
                );
            }
            // `SYNC` cuts the 9-byte `periodic:` prefix from key names.
            assert!(PERIODIC_SYNC.source().contains(&format!(
                "string.sub(key, {})",
                crate::keys::PERIODIC_PREFIX.len() + 1
            )));
        }
    }
}

/// The scripts against a private Redis.
#[cfg(all(test, feature = "test-util", feature = "periodic"))]
#[path = "lua/redis_tests.rs"]
mod redis_tests;
