# Changelog

Every notable change to Hench is listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the version numbers follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html): before 1.0, a minor release
(0.x.0) may change the public API, a patch release (0.x.y) does not.

The four crates (`hench`, `hench-macros`, `hench-web` and `hench-server`) share one version and
are released together; `hench` requires the `hench-macros` of its own release. For
`hench-server` these rules cover the `hench` command (its command line and its JSON API), not
its library, which exists for that binary.

## [Unreleased]

Nothing yet. Planned for 0.2 (the README's [Roadmap](README.md#roadmap) says what each item
brings and what to do until then):

- **Queue limits**: a concurrency or rate limit on a whole queue, checked when workers fetch
  from it.
- **Staged batch commits**: batches larger than `batch_max_commit_jobs` (10 000 jobs) defined
  in one go, and still visible all at once.
- **Batch cancellation seen sooner**: iterable jobs that check their batch at each checkpoint,
  and a process-wide cache of cancelled batches.
- **Batch tags**, searchable on Redis 7.4 or newer.
- **A lean lock mode for unique jobs**, if users ask for it.

## [0.1.0] - 2026-11-02

The first release: background jobs for tokio applications, with the architecture, Redis data
model and semantics of Sidekiq 8.1, the Pro and Enterprise features unique jobs, periodic jobs,
batches and rate limiting, and a dashboard.

### Added

- **Jobs.** `#[hench::job]` turns an `async fn` (or an `impl Job` block) into a job type that
  registers itself, or implement `Job` by hand. Each job type can set its queue, its retries (a
  count, or a time limit with `retry_for`), backtraces, tags, the dead-set opt-out, its log
  level, a unique lock and a throttle. Arguments are serde types, checked when the job is
  pushed.
- **Client.** Push a job now, after a delay, at a time or in bulk (optionally spread over a
  period), with custom payload fields; raw pushes for jobs other services consume; client
  middleware; pushes retried through a Redis failover; fake and inline clients for tests.
- **Workers.** Any concurrency, several capsules with their own queues and concurrency, strict,
  weighted or random queue order, pausable queues, server middleware, and lifecycle hooks
  (startup, quiet, shutdown, exit, heartbeat). Graceful and hard shutdowns requeue unfinished
  jobs, and a thread of its own keeps each process's heartbeat alive.
- **Delivery.** Reliable fetch, the default, keeps each job in its process's in-progress list
  until its outcome is written, so a crashed process loses nothing: an orphan check (every
  `orphan_check_interval`, 60 s by default) that finds the process's heartbeat gone, and still
  gone after `orphan_grace` (30 s), pushes its jobs back to their queues, about 1.5 to 2.5
  minutes after the crash. Basic fetch is available too (cheaper; the jobs a crashed process
  was running are lost).
- **Crash-loop protection.** A job that kills the process running it (an out-of-memory kill,
  an abort, a crash in foreign code, a panic in a binary built with `panic = "abort"`) is
  recovered at most `max_recoveries` times (`HenchBuilder::max_recoveries`, or
  `max_recoveries` in the worker's file; 3 by default, counted for 72 hours after the last
  recovery). At its next crash it goes to the dead set with the error class
  `hench::CrashLoop` and a message saying how often it was recovered: its batch counts it dead,
  its unique lock is released, and the death handlers run once. Jobs recovered for the first
  time go back at once; a job recovered before comes back alone, one per orphan check, so only
  the culprit is buried, not the jobs that happened to run beside it. An iterable job that
  saves progress between crashes starts its count again, and jobs requeued at a shutdown are
  never counted.
- **Retries.** Exponential backoff with jitter (Sidekiq's formula), `retry_in` and
  `retries_exhausted` hooks, a dead set trimmed by age and size, error and death handlers.
  Outcome writes are retried through Redis outages.
- **Scheduler.** Scheduled jobs and retries reach their queues through a scheduler that leases
  due jobs, so a crash or a lost reply delays a job instead of losing it, and that adapts its
  polling to the number of processes.
- **Data API** (`hench::api`): statistics, queues, retries, scheduled and dead jobs, processes
  and running jobs, with atomic moves (retry, kill, enqueue now) and remote quiet and stop.
- **Unique jobs.** A lock per job type and arguments, taken atomically with the push and
  released when the job succeeds, when it starts, or when its time is up; duplicates are
  dropped, refused with an error, or replace the waiting job (debouncing). The `UniqueLocks` API
  lists and releases locks.
- **Periodic jobs.** Cron expressions (with Vixie cron's rules at daylight saving changes) or
  fixed intervals, IANA time zones, catch-up policies after downtime, and one push per tick
  across every process, without a leader. Schedules are defined in code, in the worker's file,
  or at runtime through the API (`Api::periodic()`) and `hench periodic add`.
- **Batches and workflows.** Callbacks when a group of jobs completes, succeeds or has a job
  die, nested batches, jobs added later, cancellation, the `Batches` API, and workflows that
  run stages of jobs one after the other. `Workflow::start` checks every stage against
  `batch_max_commit_jobs` before it writes anything (`BatchError::StageTooLarge`).
- **Rate limiting.** Limiters inside jobs (concurrency, window, bucket, leaky bucket, points)
  and throttles checked before a job starts, shared by every process. A job over its limit
  waits in the schedule for its slot in the limit's wait line, without using a retry, and
  keeps its place when it comes back. `try_acquire` never takes a place in the line; `acquire`
  in a job does.
- **Metrics.** Execution counts, failures, time and histograms per job class, and deploy
  marks.
- **Iterable jobs.** Jobs that save their position, resume after an interruption, a failure or
  a deploy, and can be cancelled.
- **Testing.** Capture jobs in memory (`Client::fake()`), run them as they are pushed
  (`Client::inline(..)`) or call one directly (`perform_inline`); limits in memory on tokio's
  clock (`TestLimits::InMemory`). `hench::test_util` (feature `test-util`) runs a private
  `redis-server` for each integration test.
- **Operations.** TLS (`rediss://`, through rustls with the `ring` provider, which hench
  installs as the process default unless the application installed one), Unix socket and
  Redis Sentinel connections. systemd `Type=notify` with watchdog keep-alives. Per-job log
  levels through `tracing`. `SIGTTIN` logs the busy jobs. A worker command line with a TOML
  configuration file (`hench::cli`, feature `cli`).
- **Dashboard** (`hench-web`), an `axum::Router` to mount in an application:
  - live and historical statistics, busy processes, queues, retries, scheduled and dead jobs
    with bulk actions, and metrics charts;
  - the Periodic, Batches, Limits and Locks tabs, each of which can be turned off;
  - dark mode and read-only mode;
  - CSRF protection, Host checks and a strict content security policy;
  - routes of your own behind the same protections, in the dashboard's layout.
- **The `hench` command** (`hench-server`, `cargo install hench-server --locked`):
  - a standalone dashboard with Basic authentication;
  - a JSON API (`--api`) to inspect and enqueue jobs and to manage the features: each process
    of `GET /api/v1/processes` lists the names of its queues and its capsules by name;
  - `stats`, `processes`, `queues`, `quiet`, `stop`, `enqueue`, `periodic`, `batches`,
    `limits` and `locks` subcommands, with `--json` output for the reports.

### Known limitations

- After a crash loop, the jobs that ran beside the poison pill come back one at a time, at
  most one per orphan check, so they run minutes later; while they wait they are in the
  `recovery-suspects` sorted set, which the dashboard does not show.
- A freed throttle permit is not handed straight to a waiting job: throttled jobs shorter than
  the scheduler's round trip drain at about two thirds of their limit (`max_wait` keeps them
  waiting in the process instead).
- Nothing warns when a workflow's callback lands in a queue that no process works on.
- A job requeued by a shutdown after it gave its unique lock up (`until: start`) counts a lapse
  of its lock when it succeeds.
- Pushes by class name (`hench enqueue`, `POST /api/v1/jobs`, `hench periodic add`, raw
  `JobPayload` pushes) know no job type: they get the default queue and retries and no throttle
  unless they name them.
- The dashboard is in English only and does not create or edit schedules (the API and `hench
  periodic add` do); a queue named `.` or `..` cannot be opened from it.
- TLS through Sentinel (`rediss+sentinel://`) is refused, and the `hench` command has no
  options for a private certificate authority or a client certificate.
- Redis-compatible servers (Valkey, Dragonfly) should work but are not tested.

### Requirements

- Rust 1.88 or newer (edition 2024).
- Redis 6.2 or newer, with `maxmemory-policy noeviction` (workers warn otherwise) and
  persistence (AOF) if jobs must survive a Redis restart.

[Unreleased]: https://github.com/rkwap/hench/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/rkwap/hench/releases/tag/v0.1.0
