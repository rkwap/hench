# Deployment

**Redis.** Version 6.2 or newer (Hench uses `LMOVE`, `BLMOVE` and `ZRANGE BYSCORE`; workers
check at startup). Set `maxmemory-policy noeviction` (an evicting Redis silently drops jobs;
workers warn about it) and enable persistence (AOF) if jobs must survive a Redis restart.
Keys are not namespaced, exactly like Sidekiq: give each application its own Redis database
or instance, and do not share it with a cache.

**Signals.** `SIGTERM` or `SIGINT` stops a process gracefully: it stops fetching, waits up to
`timeout` (25 s) for running jobs, then interrupts the rest and requeues them; a second
signal interrupts at once. `SIGTSTP` quiets a process (it finishes its jobs and takes no new
ones), which is how a deploy drains old workers before stopping them. `SIGTTIN` logs what a
process is running (each busy slot's job and how long it has run), the first thing to look at
when a worker seems stuck; `api::Process::dump()` asks a process for the same from anywhere. Give your process
manager a stop timeout above `timeout` plus a few seconds (Kubernetes:
`terminationGracePeriodSeconds`). Embedded workers stop with your application
(`run_until`), and remote quiet/stop from the dashboard or `hench` reach every process (a
remote stop ends `run_until`: stop the rest of the application then, as in the
[embedding example](getting-started.md#5-or-embed-them-next-to-your-web-application)).

**Panics.** Workers catch a job's panic by unwinding, Rust's default panic strategy: keep
`panic = "unwind"` in the profile that builds a worker binary. Under `panic = "abort"` a
panicking job aborts the whole process instead of failing, taking down the jobs running beside
it; with reliable fetch it is recovered and aborts the processes that run it after it, up to
`max_recoveries` + 1 processes in all (4 by default), and then lands in the dead set as a
[poison pill](reliability.md#delivery-guarantees-reliable-and-basic-fetch) (class `hench::CrashLoop`), never
in the retries. Workers log a warning at startup when built that way. (A stack overflow or an
allocation failure aborts the process whatever the strategy.)

**systemd.** Workers run with `Hench::run` notify systemd when started under `Type=notify`:
`READY=1` once the process is registered and its workers are running, a `STATUS=` line with
busy counts on every heartbeat, `STOPPING=1` when the shutdown begins, and watchdog keep-alives
(from a task of their own, twice per `WatchdogSec`, whatever Redis is doing) when `WatchdogSec`
is set. Workers embedded with `start` or `run_until` leave the notifications to the program
that embeds them (give them a notifier with `systemd_notifier` to send them anyway):

```ini
[Service]
Type=notify
# Where config/hench.toml is (or name the file: my-worker -C /etc/my-worker/hench.toml).
WorkingDirectory=/opt/my-app
ExecStart=/usr/local/bin/my-worker -e production
Environment=REDIS_URL=redis://10.0.0.5:6379/0
WatchdogSec=60
TimeoutStopSec=35
KillSignal=SIGTERM
Restart=always
```

**Logging.** Everything goes through `tracing`: each job runs in a `job{class, jid}` span,
with `start` and `done`/`fail` events and their duration (`info`), and failures reach the
default error handler (`warn`). Install a subscriber that shows `info`, as the examples do
(`RUST_LOG` overriding it): `EnvFilter`'s default without `RUST_LOG` is `error`, which hides
failing jobs. Set a job's `log_level` to quieten or detail it, and add
`hench::JOB_LOG_LEVEL_DIRECTIVES` to your `EnvFilter` to let a job log below the process
level.

**Health and capacity.** `HEAD /` on the dashboard and `GET /healthz` on `hench web` check
Redis. Processes appear on the busy page with their memory, concurrency and running jobs.
Run CPU-heavy or blocking work in `tokio::task::spawn_blocking` so it does not stall the other
jobs of the process, and scale with more processes or more concurrency. A thread of its own
keeps the process's heartbeat key alive, so a job that blocks the runtime does not make the
process look dead to the others (which would run its jobs again); but it does hold up the
heartbeat (remote signals, stats) and, under `WatchdogSec`, the keep-alives.

---

[← The `hench` command](cli.md) · [Guide](README.md) · [Differences from Sidekiq →](sidekiq.md)
