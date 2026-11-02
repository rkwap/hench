//! Soak and chaos harness for hench: several worker OS processes (`Hench::run`, real signals)
//! against one private `redis-server`, random job outcomes, scheduled jobs, retries, bulk pushes,
//! iterable jobs, and faults; every execution is recorded in append-only logs outside Redis, and
//! `soak check` verifies the invariants afterwards. It finds what the in-process tests cannot:
//! problems that need several processes, real signals and restarts, Redis restarts with and
//! without persistence, and minutes of load. Unix only (signals, `/proc` samples).
//!
//! ```sh
//! cargo build --release -p hench --example soak
//! # optional: the dashboard's JSON API is sampled and checked when a `hench` binary is given
//! cargo build --release -p hench-server
//! target/release/examples/soak run --scenario crash --duration 180 --workers 4 \
//!     --actions kill9,term,tstp --hench-bin target/release/hench
//! target/release/examples/soak check target/soak/crash      # check a finished scenario again
//! ci/soak.sh                     # the suite of scenarios (about 30 minutes); --short for CI
//! ```
//!
//! `run` and `check` print a summary and exit with status 1 when an invariant is violated.
//! Each scenario writes `<dir>` (default `target/soak/<scenario>`): `pushes.jsonl` (every push
//! and its outcome), `exec-<worker>.jsonl` (one per worker process launch: each execution's
//! start and end), `chaos.jsonl` (spawns, exits with their shutdown time, `kill -9`, SIGTERM,
//! SIGTSTP, remote quiet/stop, Redis restarts, `CLIENT PAUSE`, proxy faults, the drain),
//! `samples.jsonl` (every 2 s: workers' RSS, threads and descriptors, Redis clients and memory,
//! counts read from Redis, due but not enqueued scheduled and retry jobs, and the JSON API's
//! `/stats` and `/processes`), `final.json` (every key left in Redis), `worker-*.log` (hench's
//! own logs) and `report.json` (the checker's verdicts).
//!
//! * **Jobs** (`worker.rs`): `SoakJob(plan, param, nonce)` with plans `ok`, `slow`, `vslow`
//!   (longer than the shutdown timeout), `coop` (stops on `ctx.stopping()` with
//!   `hench::Shutdown`), `errk`/`panick` (fail or panic `param` times, then succeed),
//!   `aerr`/`apanic` (always fail or panic: the dead set after `retry + 1` attempts), `noretry`
//!   (`retry: false`), `deadfalse` (`dead: false`); `SoakIter` (iterable, 20 to 60 items, a 4 s
//!   run-time limit, 1 s checkpoints). A server middleware logs each execution's start and end;
//!   a drop guard logs executions that never end (hard shutdown); death and error handlers are
//!   logged too.
//! * **Load** (`orchestrator.rs`): single pushes with jids of their own (15% scheduled 1 to 20 s
//!   ahead), bulk pushes of 100 to 600 jobs, iterable jobs; every push is logged with its
//!   outcome.
//! * **Chaos** (`--actions`): `kill9`, `term`, `dterm` (two SIGTERMs), `tstp` (then SIGTERM),
//!   `remote_stop`, `remote_quiet` (through `hench::api`), `redis_restart`, `redis_kill9`
//!   (`--persist aof|rdb|none`), `pause_write`, `pause_all` (`CLIENT PAUSE` 1 to 12 s),
//!   `long_pause` (70 s), `proxy_drop` (replies swallowed 1 to 8 s, then the connections that
//!   lost some closed), `proxy_delay` (replies 0.2 to 4.5 s late), `proxy_reset`, `queue_pause`
//!   (`--proxy` puts the workers behind the fault-injecting proxy, `proxy.rs`). A worker slot
//!   whose process ends starts a new one.
//! * **Drain**: load and chaos stop, faults are cleared, the harness waits until no job is in a
//!   queue, the schedule, the retry set or an in-progress list and nobody is busy, then until
//!   the `processes` set lists the running workers only (a process killed with `kill -9` is
//!   pruned by a scheduler about a minute after its heartbeat expired; the wait gives up after
//!   4 minutes), then stops every worker with SIGTERM and snapshots Redis.
//! * **Checks** (`check.rs`): every pushed job reached the end its plan says (success; the dead
//!   set after exactly `retry + 1` attempts; dropped or discarded), or is lost in a documented
//!   way (Redis without persistence, the last second of an AOF-everysec Redis killed with
//!   `kill -9`, basic fetch and a crash); extra runs of an attempt by cause (crash recovery,
//!   hard-shutdown requeue, cooperative shutdown, a fault window, a push that may have run
//!   twice, or unexplained); concurrent runs in two processes; nothing left in in-progress lists
//!   or in the `processes` and `inprogress` sets; `stat:processed`/`stat:failed` against the
//!   executions observed (never above, below only by what a `kill -9` can lose); live processes
//!   whose jobs were recovered as orphans; iterable jobs failed after their last item;
//!   scheduler lateness; the dashboard API against Redis; worker resources.
//! * **Pro features** (`--pro`, the `pro` scenario; `pro.rs`, `check_pro.rs`): batches of
//!   throttled members that take a window limiter's slot, cancelled batches, and a unique
//!   periodic schedule. Checked from the logs and `pro.json`: each tick enqueued at most once
//!   and its runs never overlapping while its lock holds; every batch's counts, events and
//!   callbacks (each fired event's callback ran, no other did); the outbox empty; no unique
//!   lock outliving its `for`; at most 3 members running at once, overages counted by the
//!   limit aside; every feature key expiring but configuration and index keys.

#[cfg(unix)]
mod check;
#[cfg(unix)]
mod check_pro;
#[cfg(unix)]
mod jsonlog;
#[cfg(unix)]
mod orchestrator;
#[cfg(unix)]
mod pro;
#[cfg(unix)]
mod proxy;
#[cfg(unix)]
mod worker;

#[cfg(unix)]
mod app;

#[cfg(unix)]
fn main() -> std::process::ExitCode {
    app::main()
}

#[cfg(not(unix))]
fn main() -> std::process::ExitCode {
    eprintln!("the soak harness needs Unix (signals, /proc)");
    std::process::ExitCode::FAILURE
}
