# Periodic (cron) jobs for hench: design

Status: proposal, 2026-11-01. Baseline: `implement-hench` at `fbcba4d` (DESIGN.md §0–§10),
re-checked against `91c05b6` (round-2 merges: dashboard extension points, Sentinel, TTIN), which
changes nothing below. Companion design: unique jobs, `/home/user/scratch/design/unique.md`.
Author: design workflow (research only; nothing in `/home/user/hench` was changed).

This document has four parts: a survey of how the reference projects implement periodic jobs
and what went wrong for them in production (§1), the hench design (§2), a test plan (§3), and
the decisions the owner must take, with a recommendation each, plus risks (§4). Appendix A
records a probe of the recommended cron library on Rust 1.88; Appendix B records the
verification of every Lua script of §2.5 against a scratch `redis-server` 7.0.15.

Cross-feature review (2026-11-01, `/home/user/scratch/design/pro-features.md`, which also holds the
implementation lanes): aligned with the unique, batches and rate-limiting designs. Changes: unique and
throttle setters on `Periodic`, with the typed keys (`Job::unique_key`, `Job::throttle_key`) evaluated at
definition and stored in the template (§2.3.1); template rules refusing other features' reserved fields
(§2.3.1); definition digests through the crate-wide canonical JSON and SHA-256 helpers (§2.4, §2.14);
`RECORD_RUN` statuses `rescheduled` and the job-as-fetched rule (§2.5, §2.10); feature-prefixed script
names and `.lua` files (§2.5); `JobRecord::periodic()` (§2.3.5); heartbeat `"features"` map (§2.4);
`periodic` as a reserved TOML name (§2.3.2); interplay with batches and rate limiting (§2.10); dashboard
navigation order, `HenchWeb::periodic` switch, flash kinds and job-row order (§2.11); DESIGN section
§9h (§2.15).

Licensing: Sidekiq Enterprise (closed), the Sidekiq wiki and Faktory Enterprise were read for
behaviour only; nothing below quotes or adapts their text or code. The Lua scripts and Rust
sketches are original. sidekiq-cron, BullMQ and rusty-sidekiq are MIT and Oban is Apache-2.0;
no code from them is reused either (only behaviour and test scenarios).

---

## 0. The design in one page

- **What.** A *schedule* (a name, a timetable — a cron expression in an IANA time zone, or a
  fixed interval — and a job template) produces one job per *tick*. Schedules are defined in
  code on the `HenchBuilder` (or in the worker's TOML file), or at runtime through
  `hench::api` ("dynamic" schedules, for example one per tenant). Both kinds live in Redis and
  fire the same way.
- **Who fires.** Every worker process runs a small *periodic poller* (no leader). A Redis sorted
  set indexes every active schedule by the time of its next tick. A poller *leases* the earliest
  due schedule (a script moves its score a minute ahead), builds the job (client middleware
  runs, as for any push), and fires it with one script, **FIRE**, which pushes the job and
  advances the schedule only if the schedule's `next` field still holds the tick it read
  (compare-and-set). A tick is therefore enqueued **at most once**, whatever crashes, lost
  replies or concurrent pollers do; the lease and the catch-up policy make it **at least once**
  as long as some poller runs. Jobs then run with hench's usual at-least-once execution.
- **Clock.** "Is it due?" is decided with Redis's clock (`TIME` inside the lease script), so
  clock skew between worker hosts cannot fire a tick early or twice. Tick instants are computed
  from the previous tick, never from a wall clock reading.
- **Cron dialect.** 5 fields (minute resolution) or 6 (seconds first); `@yearly`, `@annually`,
  `@monthly`, `@weekly`, `@daily`, `@midnight`, `@hourly`; lists, ranges, `*/n` and `a-b/n`
  steps, month and weekday names; the `L`, `W`, `#` and `+` extensions. POSIX/Vixie day rules
  (day of month OR day of week when both are restricted). Parsed by the `croner` crate (MIT)
  behind a private wrapper; IANA time zones from `jiff` (system tz database, bundled fallback).
  Default zone UTC. DST follows Vixie cron: a fixed-time job falling in a skipped hour runs
  right after the gap; in a repeated hour it runs once; wildcard/step jobs follow real time.
- **Missed ticks.** Per schedule `CatchUp::Latest` (default: after downtime, enqueue one job
  for the most recent missed tick), `CatchUp::Skip` (never enqueue a tick later than a deadline,
  5 min by default) or `CatchUp::All { max }` (backfill up to `max` ticks, oldest first), plus an
  optional `deadline`. Pausing and resuming never backfills.
- **Code-defined schedules** are registered atomically per *group* (default `default`) at
  startup: created, updated (template changes keep the next tick, timetable changes reschedule
  it) and pruned (removed from the code ⇒ deleted from Redis). Operator state (paused, history,
  counters) survives re-registration. Registration is re-asserted if Redis loses the data.
- **Operations.** Pause/resume, "enqueue now", per-schedule history of the last enqueues
  (capped list), last run outcome (success/failure, duration, error), counters, a dashboard tab
  (list, detail, actions), `hench periodic …` commands, JSON API endpoints, heartbeat info
  telling which processes run a poller and which crontab they declare.
- **Cost.** Idle: one small script per process every ≤ 5 s. Per tick: three round trips (lease,
  read, fire). No `KEYS`, no scans of all schedules per poll, no TTL-less growth (history is
  capped; ticks are not stored individually).

---

## 1. Survey of the references

### 1.1 Sidekiq Enterprise periodic jobs (behaviour only)

Sources: `sidekiq.wiki/Ent-Periodic-Jobs.md`, `Ent-Leader-Election.md`, `Ent-Unique-Jobs.md`,
`sidekiq/Ent-Changes.md`.

- **Definition.** Registered in the server configuration block at boot: a crontab string, the
  job's class *name* (a string since Ent 2.0.1, #4271, so initializers need not load classes) and
  job options, which override the class's own options. Static arguments only (evaluated at
  boot). The configuration is global: every process must register the same set.
- **Firing.** Only the elected *leader* process creates the jobs. The leader renews its
  leadership every 15 s; followers check every 60 s and take over if the leader's record
  expired. A quiet leader keeps enqueuing; it stops only when shutting down.
- **Resolution and catch-up.** One minute at best. No backfill: ticks missed while the cluster
  is down are not created; users are told to make jobs resilient (process "the last N hours").
- **Time zones.** Originally the process's local zone (system `TZ`); per-entry and global zones
  since Ent 2.2.1 (#4749); the Web UI shows the next run in the entry's zone since 2.2.3
  (#4833).
- **Dynamic schedules.** Not supported; the documented pattern is a static every-minute job that
  scans application database rows holding cron expressions and `next_run_at`.
- **Introspection.** `LoopSet` lists the registered loops with their schedule, class, a loop
  id (`lid`), options and enqueue history. A Web UI tab ("Cron", renamed
  "Periodic" in Ent 7.0, #5590) lists loops and history; pause, unpause and manual enqueue were
  added in Ent 8.0.1 (#6675).
- **Testing.** A config tester (Ent 7.1, #5832) runs the registration block and raises on an
  invalid crontab or an unknown class.

Trade-offs: the leader makes per-tick logic trivial (one evaluator, no claim) but adds an
election subsystem, a failover gap during which ticks are missed, and duplicate risks when
leadership flaps; it also requires identical configuration everywhere (a leader that registered
nothing fires nothing).

### 1.2 sidekiq-cron (MIT, Ruby)

Sources: `lib/sidekiq/cron/{job,poller,launcher,namespace,schedule_loader,web_extension}.rb`,
`test/unit/*`, `test/integration/performance_test.rb`, `CHANGELOG.md`, `README.md`.

- **Data model.** `cron_jobs:<namespace>` (SET of job keys); `cron_job:<ns>:<name>` (HASH: name,
  class, cron, args, queue, retry, message, status `enabled`/`disabled`, source
  `schedule`/`dynamic`, `last_enqueue_time`, description, …);
  `cron_job:<ns>:<name>:enqueued` (ZSET: one member per fired tick — the tick as ISO-8601,
  scored by its epoch time — trimmed to the last 24 h); `cron_job:<ns>:<name>:jid_history`
  (LIST of `{jid, enqueued}`, trimmed to `cron_history_size`, default 10).
- **Algorithm.** Every process runs a `Sidekiq::Cron::Poller` (a subclass of Sidekiq's
  scheduled-set poller) every `cron_poll_interval` (30 s by default, randomized like Sidekiq's
  poller). Each poll loads *every* job (`SMEMBERS` plus pipelined `HGETALL`), and for each enabled
  job computes the most recent tick ≤ now (`previous_time`, Fugit). It skips the job if that
  tick is older than `reschedule_grace_period` (60 s by default) or not newer than
  `last_enqueue_time`; otherwise it *claims* the tick with `ZADD <job>:enqueued <tick> <tick
  iso>` (true only for the first claimant) and, in a separate step, pushes the job through
  `Sidekiq::Client`, then records `last_enqueue_time` and the jid history.
- **Atomicity.** Claim and push are separate commands: a crash or a Redis error between them
  loses the tick; a push that fails after the claim is logged and lost.
- **Registration.** `load_from_hash!`/`load_from_array!` (schedule file `config/schedule.yml`,
  loaded at startup) upsert jobs and destroy jobs of source `schedule` that are no longer in the
  file; `dynamic` jobs (created at runtime) are untouched. `save` initializes the claim set with
  the current tick so that a new job does not fire for a tick that passed before it existed.
- **Dialect.** Fugit: 5 fields, optional seconds (6 fields), a time zone suffix
  (`0 22 * * 1-5 America/Chicago`), natural language (`every day at five`).
- **UI.** Namespaced list, per-job page with jid history, enqueue now, enable/disable, delete,
  bulk actions per namespace.

Trade-offs: simple and leaderless; but O(all jobs) work per poll per process (hence the
namespace options of 2.1–2.3 and a performance test of 10 000 jobs), non-atomic claim/push,
`Time.now`-based decisions on every host, and a 60 s grace period that silently skips daily jobs
whose tick falls in a deploy window (README, "Under the hood").

### 1.3 BullMQ job schedulers (MIT, TypeScript + Lua)

Sources: `src/classes/job-scheduler.ts`, `src/classes/worker.ts` (`nextJobFromJobData`),
`src/commands/{addJobScheduler-11,updateJobScheduler-12,removeJobScheduler-3}.lua`,
`includes/{storeJobScheduler,getJobSchedulerEveryNextMillis}.lua`,
`tests/job_scheduler*.test.ts`, `docs/gitbook/guide/job-schedulers/*`, `docs/gitbook/changelog*`.

- **Data model.** `<prefix>:repeat` (ZSET: scheduler id → next millis) and
  `<prefix>:repeat:<id>` (HASH: name, pattern or every, tz, startDate, endDate, limit, offset,
  iteration count `ic`, template data and options). There is always exactly one *delayed job*
  per scheduler, with the deterministic id `repeat:<id>:<millis>`.
- **Algorithm (chaining).** `upsertJobScheduler(id, repeatOpts, template)` stores the scheduler
  and adds the delayed job for the next tick in one script. When a worker moves that job to
  *active*, it calls the update script, which adds the job for the following tick only if the
  scheduler still exists and the producing job's id matches the scheduler's current millis
  (`producerId == repeat:<id>:<prevMillis>`); an existing id means a duplicate (event
  `duplicated`). Ticks therefore advance only when jobs start: a backed-up queue never
  accumulates more than one pending job per scheduler, but the production rate drops with it.
- **Timetables.** `pattern` (cron-parser, optional seconds field, `tz`), or `every` (ms,
  aligned to the clock grid plus an `offset`), `startDate`, `endDate`, `limit`, custom
  strategies (e.g. RRULE). Missed ticks: the pending delayed job runs late once, then the next
  tick is computed from now.
- **Legacy.** The earlier "repeatable jobs" identified a schedule by a key derived from its
  options (`name:id:endDate:tz:pattern`), so changing a pattern created a second schedule; Job
  Schedulers with explicit ids replaced them (v5.16) and v6 removed the legacy API.

Trade-offs: deterministic job ids plus Lua give strong deduplication, and chaining prevents
pile-ups; but scheduling is coupled to job execution (a worker crash or a failing update script
can break the chain — the test "should move job to delayed with backoff when scheduler update
fails" covers one such path), retries and stalled jobs need special cases, and the changelog
shows a long series of fixes around upserts racing with the chain (§1.8).

### 1.4 Oban Cron (Apache-2.0, Elixir)

Sources: `lib/oban/cron.ex`, `lib/oban/cron/expression.ex`, `test/oban/cron_test.exs`,
`test/oban/cron/expression_test.exs`, `guides/learning/periodic_jobs.md`,
`guides/recipes/reliable-scheduling.md`.

- **Definition.** A static crontab in configuration: `{expr, Worker, opts}`; `:timezone` global
  and per entry; jobs carry `meta: {cron: true, cron_expr, cron_name, cron_tz}`.
- **Algorithm.** A plugin process wakes at the start of every minute; only the cluster *leader*
  (Oban's peer election) evaluates `Expression.now?(expr, now_in_zone)` for each entry and
  inserts matching jobs in one database transaction. No per-tick claim: duplicate prevention
  relies on leadership. A minute in which no leader evaluates is skipped (no catch-up).
- **Dialect.** 5 fields, nicknames including `@midnight` and `@reboot` (once per leadership
  acquisition). Days of month and of week are combined with AND (POSIX uses OR when both are
  restricted). Validation rejects day/month combinations that can never occur.
- **DST.** `now?` matches wall-clock fields, so a fixed-time job in a repeated hour matches
  twice and one in a skipped hour never; `next_at`/`last_at` instead resolve gaps to "just after"
  and folds to one side (tests "calculating the next match within a repeated or skipped hour").
  What is displayed and what is inserted can disagree.

### 1.5 rusty-sidekiq `periodic.rs` (MIT, Rust)

Sources: `src/periodic.rs`, `src/scheduled.rs`, `README.md`.

- **Data model.** One ZSET `periodic`: member = the JSON of the definition (name, class, cron,
  queue, args, retry), score = next tick (UTC).
- **Algorithm.** Each poll takes up to 100 members with score ≤ now; for each, `ZADD CH` of the
  next upcoming time *computed from the local clock*; the process whose `ZADD` changed the score
  enqueues the job (a separate command). After downtime it fires once and jumps to the next
  upcoming tick.
- **Issues.** Identity = definition JSON: editing a definition leaves the old member firing
  (the README tells users to call `destroy_all` at startup). Claim and push are not atomic. UTC
  only (`cron_clock`, a 2021 fork of the `cron` crate).

### 1.6 Faktory Enterprise cron (behaviour only)

Sources: `faktory/Ent-Changes.md`, `cli/cli_test.go`, `example/`.

- A single server owns the crontab, declared in its TOML configuration (`[[cron]] schedule =
  "*/5 * * * *"` with a `[cron.job]` table); no distributed claim is needed. Later releases let
  cron jobs carry `expires_in` (1.5.5, #384) and `reserve_for` (1.6.1, #381): periodic jobs need
  the full set of job options. The Web UI has a cron tab.

### 1.7 Comparison

| | Sidekiq Ent | sidekiq-cron | BullMQ schedulers | Oban (OSS) | sidekiq-rs | **hench (proposed)** |
|---|---|---|---|---|---|---|
| Definitions | code, at boot | file + runtime API | runtime API (upsert) | config | code | code/TOML + runtime API |
| Identity | loop id (`lid`) | name | explicit id | hash of entry | definition JSON | name |
| Stored in | Redis (display) | Redis | Redis | config | Redis | Redis |
| Who fires | leader | every process | the worker taking the previous job | leader | every process | every process (lease) |
| Duplicate guard | leadership | per-tick ZADD claim | deterministic job id + producer check | leadership | `ZADD CH` | CAS on `next` in the push script |
| Claim + push atomic | n/a | no | yes | yes (one transaction) | no | yes |
| Clock | leader's | each poller's | producer's | leader's | each poller's | Redis `TIME` |
| Poll cost | 1 evaluator | O(all jobs) per poll | none (delayed jobs) | 1 evaluator / min | O(due ≤ 100) | O(due) + 1 script per ≤ 5 s |
| Missed ticks | skipped | last one if < 60 s late | one late run | skipped | one run | policy: latest / skip / all |
| Resolution | 1 min | 1 s | 1 ms (`every`) | 1 min | 1 s | 1 s |
| Time zones | yes | yes | yes | yes | UTC only | yes (IANA, default UTC) |
| Pause / enqueue now | yes (8.0.1) | yes | remove/upsert | no | no | yes |
| History | enqueues | enqueues (10) | iteration count | jobs table | none | enqueues (10) + last run outcome |
| Stale code schedules | n/a (leader's memory) | pruned on load (`schedule` source) | n/a | n/a | linger | pruned per group, atomically |

### 1.8 Production lessons

Each row is a bug or limitation that hit users, where it is recorded, and what hench does about
it.

| # | Lesson | Source | Hench answer |
|---|---|---|---|
| L1 | Periodic jobs stopped until a process restarted after Redis went down (state lost, never re-registered). | Ent-Changes 1.2.3 (#3047) | Pollers check the group record every 60 s and re-register if it vanished (§2.7). |
| L2 | The repeated ("fallback") hour of a DST change was missed. | Ent-Changes 2.3.1 (#5049) | Vixie DST rules via `croner`, tested in four zones (§2.2.4, Appendix A). |
| L3 | Flapping leadership fired old periodic jobs once; leaders re-elected more often "to minimize missed cron jobs". | Ent-Changes 1.7.2 (#3974), 2.3.1 | No leader; CAS on `next`; a tick ≤ the last fired tick is never enqueued (monotonic guard in FIRE). |
| L4 | Some minutely jobs ran every other minute; slow processing missed clock ticks (a warning was added). | Ent-Changes 1.2.4 | Next tick computed from the previous tick, not from "now"; sleep until due; lateness recorded and logged. |
| L5 | Time zone handling fixes; next run shown in the entry's zone; default zone was the host's. | Ent-Changes 2.2.1–2.2.3 (#4749, #4796, #4833) | Explicit IANA zones, UTC default, never the host zone; dashboard shows the zone and UTC. |
| L6 | Registration by class broke initializers; a tester was added for typos and bad crontabs. | Ent-Changes 2.0.1 (#4271), 7.1.0 (#5832) | Typed `.job::<J>()`, validation at `build()`, `Periodic::validate()`. |
| L7 | A racy shutdown made periodic jobs disappear; a follower's shutdown crashed the subsystem. | Ent-Changes 0.7.7, 1.0.1 | Shutdown never writes periodic state; definitions persist in Redis. |
| L8 | Duplicate periodic jobs; empty periodic data crashed the UI. | Ent-Changes 0.7.4, 2.5.1 (#5374) | Atomic CAS; scripts tolerate missing hashes, missing `next`, unknown formats (§2.5, §2.6). |
| L9 | Claim and push are separate; multiple enqueues were fixed once. | sidekiq-cron `job.rb`; CHANGELOG 0.5.0 (#84) | One script pushes and advances. |
| L10 | The cron poller, a subclass of the scheduled-set poller, disturbed it. | sidekiq-cron CHANGELOG 1.8.0 (#359) | A separate task with its own settings and outage reporting. |
| L11 | A 60 s grace period skipped daily jobs whose tick fell in a deploy; made configurable. | sidekiq-cron 2.0.0.rc1 (#465), README "Under the hood" | Default `CatchUp::Latest` (one late run), `Skip` opt-in. |
| L12 | Jobs removed from the schedule file kept firing until `source` and pruning were added; rusty-sidekiq tells users to `destroy_all` at startup. | sidekiq-cron 1.10.0 (#405), 1.11.0 (#431); sidekiq-rs README | Atomic per-group sync prunes removed code schedules; dynamic ones untouched. |
| L13 | Scanning all jobs per poll was slow; namespace discovery used `KEYS`. | sidekiq-cron 2.1.0–2.3.0 (`available_namespaces`), `namespace.rb`, performance test | A due ZSET: each poll touches due schedules only; listing pages use a name index. |
| L14 | Reflected XSS in the cron UI; manual enqueue from the UI broke. | sidekiq-cron 2.4.0 (#568), 2.3.1 (#564) | hench-web escapes all output; dashboard tests cover enqueue now end to end. |
| L15 | Re-saving a job near its tick skipped it; reloading reset the status. | sidekiq-cron tests "doesnt skip enqueuing if job is resaved near next enqueue time", "status shouldn't be rewritten after save without status" | Identical definitions are no-ops (digest); timetable changes use a 60 s grace; paused state preserved. |
| L16 | `date_as_argument` passes the *enqueue* time. | sidekiq-cron `job.rb` (`enqueue_args`) | The job carries its *tick* (`periodic.tick`), retries included. |
| L17 | Option-derived schedule keys created a second schedule when options changed. | BullMQ legacy repeatable keys; v5.16 Job Schedulers; #2617 | Explicit names. |
| L18 | Upserts raced with the chain: duplicates in quick succession, hazards under concurrency, add-and-update in one script then reverted, producer-id check. | BullMQ #2991, #2993, #2997, 9f0f1ba, #3001; "avoid hazards when upserting job schedulers concurrently" | One CAS script per transition; definition digest checked by FIRE. |
| L19 | Retries and stalled jobs created extra iterations. | BullMQ "avoid duplicated delayed jobs when repeatable jobs are retried"; tests "should not create a new delayed job if the failed job is retried/stalled" | Ticks are independent of execution; retries reuse the job and its tick. |
| L20 | Deterministic ids collided with leftovers of a previous scheduler under the same id. | BullMQ #4299 (issue #3063) | Random jids; the CAS is on schedule state, not on job ids. |
| L21 | Timetable changes did not reach the pending job; `every` slot/offset arithmetic was fixed many times; `startDate` ignored for patterns. | BullMQ #3524, #3551, #4113, #3307, #2944, #3142, #3385 | `next` recomputed whenever the timetable key changes; `every` aligned to the epoch plus an explicit offset. |
| L22 | Removing a scheduler left its next job; stalled jobs of removed schedulers. | BullMQ #3203, #4222 | Removal is one script; FIRE removes dangling index entries; already-enqueued jobs are normal jobs. |
| L23 | Metadata (iteration count, limit) lost on upsert; empty scheduler hash. | BullMQ #3134, #3071, #3796 | Upserts write definition fields only; state fields are never rewritten by them. |
| L24 | Keys built inside Lua broke clustered deployments. | BullMQ #2662 (CROSSSLOT) | Every script declares all its keys (also required by Dragonfly). |
| L25 | Leader-only evaluation skips minutes during leadership changes; `@reboot` depends on leadership delays. | Oban `cron.ex`, periodic jobs guide | No leader; missed ticks handled by policy. |
| L26 | Insert-time and display-time DST semantics disagree; AND day semantics differ from POSIX. | Oban `expression.ex` and its tests | One tick function for firing and display; POSIX OR semantics. |
| L27 | Impossible dates (`0 0 30 2 *`) must be rejected up front. | Oban `validate_days`; croner probe (Appendix A) | Validation requires a tick within the search window. |
| L28 | Periodic jobs need full job options (expiry, reservation). | Faktory Ent 1.5.5 (#384), 1.6.1 (#381) | Templates accept every `JobOptions` field and custom keys. |

### 1.9 Rust cron and time-zone libraries (checked 2026-11-01 on crates.io)

| Crate | Version (date) | Licence | MSRV | Notes |
|---|---|---|---|---|
| `croner` | 4.0.2 (2026-10-21) | MIT | none declared (builds on 1.88, Appendix A) | 5–7 fields, aliases, `L`/`W`/`#`/`+`, POSIX OR rule, OCPS strict steps, Vixie DST rules, `describe()` in English, forward and backward search, `chrono` or `jiff` backend; deps `derive_builder`, `strum`. 4 majors since 2023 (API churn). Fails on the 24 h Apia gap of 2011 (Appendix A). |
| `cron` | 0.17.0 (2026-06-18) | MIT/Apache-2.0 | none | Seconds field required (`* * * * *` is a parse error); skips fixed-time ticks in gaps and fires both passes of a fold (measured, Appendix A); chrono only. |
| `cronexpr` | 1.7.0 (2026-08-24) | Apache-2.0 | 1.88 | jiff-based; zone inside the expression; matches fields against real-time instants, so a fixed-time job fires twice in a fold and not at all on a gap day (measured, Appendix A). |
| `saffron` | 0.1.0 (2021-02) | non-standard per crates.io | none | Unmaintained since 2021. |
| `cron_clock` | 0.8.0 (2021-10) | MIT/Apache-2.0 | none | Fork used by rusty-sidekiq; unmaintained. |
| `jiff` | 0.2.38 (2026-10-06) | Unlicense/MIT | 1.70 | IANA zones from the system tz database (re-validated every 5 min), concatenated db, or a bundled copy (`tzdb-bundle-always`, `jiff-tzdb` is a 63 KB crate). |
| `chrono-tz` | 0.10.4 (2025-07) | MIT/Apache-2.0 | 1.65 | tz data compiled into the binary; new rules need a crate release and a rebuild. |

Recommendation: `croner` (default features off, `jiff` backend) wrapped in a private module,
and `jiff` for zones. Rationale in D4/D5 (§4).

---

## 2. Design

### 2.1 Goals, non-goals, guarantees

Goals:

- Code-defined and runtime-defined schedules, cron or fixed interval, any job (typed or by
  class name) with any job options.
- Exactly one enqueue per tick across any number of processes, with no leader, robust to
  crashes, lost replies, Redis restarts and clock skew.
- Explicit, documented behaviour for time zones, DST, downtime and deploys.
- Operator tooling: dashboard tab, CLI, API, history, pause, enqueue now.

Non-goals (v1): sub-second schedules; start/end dates and run limits (BullMQ's `startDate`,
`endDate`, `limit`); RRULE or custom strategies; natural-language expressions; `@reboot`;
creating schedules from the dashboard; built-in overlap prevention (delegated to unique jobs,
§2.10); Redis Cluster (hench does not support it anywhere).

Guarantees (stated in the README and DESIGN):

1. **Enqueue: at most once per tick.** For a schedule, the ticks that are enqueued form a
   strictly increasing sequence of instants; no instant is enqueued twice. The only exception is
   a Redis failover that loses acknowledged writes (asynchronous replication): the tick can then
   fire again, like any write hench makes.
2. **Enqueue: at least once per tick, subject to the catch-up policy,** as long as at least one
   process runs the poller and Redis is reachable. A tick taken by a poller that crashed is
   picked up when its lease ends (60 s).
3. **Execution: at least once,** as for every hench job (reliable fetch, retries).
4. Ticks are evaluated with Redis's clock; a tick is never enqueued before Redis's clock reaches
   it (client middleware may still schedule the job later).

### 2.2 Concepts and semantics

#### 2.2.1 Vocabulary

- **Schedule** (`Periodic` in Rust): name, timetable, job template, catch-up policy, optional
  deadline, description; *source* `code` (with a *group*) or `api`.
- **Timetable**: `Cron { expression, tz }` or `Every { interval, offset }`.
- **Tick**: an instant (whole epoch seconds) at which the timetable produces a job.
- **`next`**: the next tick to enqueue, stored in the schedule's hash; the authority for the CAS.
- **Due index**: ZSET `periodic`, score = `next`, or a lease's end while a poller handles it.
- **Lease**: moving a due schedule's score `lease` (60 s) ahead so that other pollers leave it
  alone while one poller prepares its job.
- **Fire**: the FIRE script: if `next` (and the definition digest) are unchanged, push the
  tick's job, record it, set `next` to the following tick.
- **Late** by `now − tick`; **missed** ticks are the ones a catch-up policy drops.

#### 2.2.2 Timetables

- **Cron.** Parsed with `croner::parser::CronParser::builder().seconds(Seconds::Optional)
  .year(Year::Disallowed).dom_and_dow(false).alternative_weekdays(false)
  .sloppy_ranges(false)`. Accepted: 5 fields (`min hour dom month dow`, ticks at second 0) or 6
  fields (seconds first); lists, ranges, `*/n`, `a-b/n`; `JAN`–`DEC`, `SUN`–`SAT`
  (case-insensitive); weekday 0–7 (0 and 7 = Sunday); `?` as `*` in the day fields; `L` (last
  day of month; `5L` or `5#L` last Friday), `nW` (nearest weekday, within the month), `n#k` (k-th
  weekday), `+` prefix on the weekday field (AND instead of OR). Aliases: `@yearly`,
  `@annually`, `@monthly`, `@weekly`, `@daily`, `@hourly` (croner) and `@midnight` (hench maps it
  to `@daily` before parsing; croner lacks it). Rejected: 4 or 7+ fields (the year field),
  `0/10` (write `*/10`), `@reboot`, `@every …` (use `Periodic::every`), expressions with no tick
  within croner's search window from now (impossible dates such as `0 0 30 2 *`; round 3: the
  day and month fields are first checked against 29 years of dates, which refuses such a date in
  milliseconds where croner's search, repeated by the retries below, took up to two seconds).
  The expression is stored as written (trimmed, inner whitespace collapsed).
- **Every.** `interval` in whole seconds, ≥ 1 s; `offset` in whole seconds, `< interval`
  (default 0). Ticks are `k × interval + offset` epoch seconds: aligned to the Unix epoch, so every
  process and every restart computes the same ticks, and changing the offset is a timetable
  change. (Note in the docs: `every(7 days)` ticks on Thursdays 00:00 UTC — use cron for calendar
  schedules.)
- **Zone.** `tz` is an IANA name resolved with `jiff::tz::TimeZone::get` (system database first,
  bundled copy otherwise); default `UTC`; never the host's local zone. `tz` is rejected for
  `Every` (intervals are zone-independent).

Tick functions (private module `periodic::timing`), all on whole epoch seconds:

```rust
impl Timing {
    /// The first tick strictly after `t`.
    fn next_after(&self, t: i64) -> Result<i64, TimingError>;
    /// The last tick at or before `t`.
    fn latest_at_or_before(&self, t: i64) -> Result<Option<i64>, TimingError>;
    /// Number of ticks in [from, to), counted up to `cap`.
    fn count(&self, from: i64, to: i64, cap: u64) -> Result<u64, TimingError>;
}
```

`Every` is arithmetic. `Cron` converts `t` to `jiff::Zoned` in the schedule's zone and calls
`find_next_occurrence(&z, false)` / `find_previous_occurrence(&z, true)`. Because croner gives
up on very long gaps (`TimeSearchLimitExceeded` across the 24 h skip of Pacific/Apia on
2011-12-30), `next_after` retries from the zone's transitions and from `t + 1 h`, `t + 2 h`, …
up to 48 times and keeps the first result `> t` (round 3: only when the zone has a transition
in those 48 hours, the only cause of such a failure); a schedule that still has no next tick is
parked with an error (§2.6).

#### 2.2.3 Creation, changes, pause

- A new schedule's first tick is the first tick **strictly after** the creation time (Redis
  clock): creating a schedule never enqueues immediately (sidekiq-cron does the same; users who
  want an immediate run call "enqueue now").
- An upsert with an identical definition (same digest) changes nothing (L15).
- A change of template, policy, deadline or description keeps `next`.
- A change of timetable (cron, zone, interval, offset) sets `next` to the first tick of the new
  timetable strictly after `max(now − 60 s, last_tick)`: a tick of the new timetable that fell
  in the last minute (a deploy that lands exactly on a tick) still fires; nothing before is
  backfilled; an instant already fired is never fired again (the FIRE guard enforces it).
- Pause removes the schedule from the due index and drops `next`; no tick is counted as missed
  while paused. Resume sets `next` to the first tick strictly after now (no backfill). A
  schedule created paused stays paused across re-registrations (operator state).

#### 2.2.4 DST (Vixie rules, as implemented by croner; Appendix A has the measured ticks)

| Case | Rule | Example |
|---|---|---|
| Fixed-time tick in a skipped interval (spring forward) | runs at the first valid instant after the gap, same day | `30 2 * * *` America/New_York on 2026-03-08 → 03:00 EDT |
| Fixed-time tick in a repeated interval (fall back) | runs once, in the first pass | `30 1 * * *` Europe/London on 2026-10-25 → 01:30 BST only |
| Wildcard/step tick in a skipped interval | does not exist; skipped | `*/20 1 * * *` London 2026-03-29 → no tick that day |
| Wildcard/step tick in a repeated interval | runs in both passes | `0 * * * *` New York 2026-11-01 → 01:00 EDT and 01:00 EST |
| 30-minute shift | handled like any gap | `15 2 * * *` Australia/Lord_Howe 2026-10-04 → 02:30 |
| Whole-day skip | retried past the gap (§2.2.2) | `0 12 * * *` Pacific/Apia 2011-12-30 → 2011-12-31 12:00 |

"Fixed-time" means, as in Vixie cron (cronie's `MIN_STAR`/`HR_STAR` flags), seconds, minutes
and hours fields that do not start with `*`: single values, lists and ranges. Round 3: croner's
`determine_job_type` counts single values only, so lists and ranges such as `30 1,13 * * *` ran
twice in a fold and not at all in a gap; the wrapper now classifies by the fields' text and,
for lists and ranges, drops the second pass of a fold and adds the instant after a gap (of up to
two hours) in which a skipped wall clock time matches. The dashboard shows ticks in the
schedule's zone with the UTC instant in the tooltip.

#### 2.2.5 Catch-up policies

Inputs: `next` = T (due, T ≤ now), `now` (Redis), the timetable, the policy, the optional
`deadline`. `L` = `latest_at_or_before(now)` (≥ T). One FIRE call enqueues at most one tick.

| Policy | Tick enqueued now | Missed (counted, not enqueued) | New `next` |
|---|---|---|---|
| `Latest` (default) | L, unless `deadline` is set and `now − L > deadline` | ticks in [T, L), plus L if dropped | `next_after(now)` |
| `Skip` | L if `now − L ≤ deadline` (default 5 min) | as above | `next_after(now)` |
| `All { max }` | the oldest tick C of the most recent `max` ticks in [T, L] that are within `deadline` | ticks in [T, C) | `next_after(C)` (may be due at once: the poller loops) |

Examples, hourly `0 * * * *`, `next` = 10:00:

- now 10:00:03 → every policy enqueues 10:00, next 11:00.
- now 13:20 (down 3 h 20 min): `Latest` enqueues 13:00 (20 min late), missed 3, next 14:00;
  `Skip` enqueues nothing (13:00 is 20 min late), missed 4, next 14:00; `All { max: 24 }`
  enqueues 10:00, 11:00, 12:00, 13:00 in successive FIRE calls; `All { max: 2 }` enqueues 12:00
  and 13:00, missed 2.

Counting is capped at 10 000 for cron timetables (shown as "10 000+"); `Every` counts exactly.
A warning is logged for every FIRE that drops ticks, and for every tick enqueued more than 60 s
late (L4).

#### 2.2.6 Why the enqueue is exactly once

Invariants maintained by the scripts of §2.5:

1. `next` is only written by FIRE (from T to a value > T), by UPSERT/SYNC (creation, timetable
   change, index repair), by UNPAUSE and PAUSE (removal).
2. FIRE pushes a job for tick T only if `next == T`, the digest matches, the schedule is not
   paused, and `T > last_tick`; in the same atomic step it sets `next` to T' > T and `last_tick`
   to T (`last_tick` never decreases: an instant not after it is refused with result 2).
3. Every write of FIRE happens after its checks, the push is its first write (an out-of-memory
   refusal therefore leaves everything untouched; verified, Appendix B), and Redis replicates a
   script's effects atomically.

Hence two FIRE calls for the same T cannot both push (the second sees `next ≠ T` or
`last_tick ≥ T`), whoever sends them and however often (a resent script after a lost reply is
just a second call). Liveness: a leased schedule whose poller died becomes due again when the
lease ends; a schedule whose `next` was lost is repaired when it is next leased (FIRE with an
empty expected `next`, §2.6), and one whose due-index entry was lost (only a manual deletion
can do that) by the next registration of its group (code schedules) or by
`PeriodicSet::repair` (`hench periodic repair`).

### 2.3 Rust API

Module `hench::periodic` (cargo feature `periodic`, on by default), re-exported items
`hench::Periodic`. All new public types are `#[non_exhaustive]` where they may grow; builder
methods are `#[must_use]`; everything is documented.

#### 2.3.1 Defining schedules

```rust
use std::time::Duration;
use hench::prelude::*;
use hench::periodic::{CatchUp, Periodic};

#[hench::job(queue = "reports", retry = 3)]
async fn nightly_report(ctx: &JobContext) -> Result<(), ReportError> {
    // The tick this job is for (not when it runs): retries keep it.
    let covers_until = ctx.periodic().map(|tick| tick.tick);
    /* ... */
    Ok(())
}

#[hench::job(queue = "rollups")]
async fn rollup(hour_offset: i64) { /* ... */ }

let hench = Hench::builder()
    .periodic(
        Periodic::cron("nightly-report", "0 2 * * *")
            .tz("Europe/Paris")
            .job::<nightly_report>(())
            .description("Daily PDF report for the previous day"),
    )
    .periodic(
        Periodic::cron("hourly-rollup", "5 * * * *")
            .job::<rollup>((0,))
            .catch_up(CatchUp::All { max: 24 }),
    )
    .periodic(
        // A job of another service, by class name.
        Periodic::every("refresh-rates", Duration::from_secs(30))
            .class("rates::Refresh", vec![])
            .queue("rates")
            .retry(false)
            .catch_up(CatchUp::Skip),
    )
    .periodic_group("billing") // optional; see §2.7
    .build()
    .await?;
```

```rust
/// A periodic job: a named timetable that enqueues a job at each tick.
#[derive(Debug, Clone)]
pub struct Periodic { /* private */ }

impl Periodic {
    /// Ticks from a cron expression: 5 fields (minute resolution) or 6 (seconds first), or an
    /// alias such as `@daily`. Evaluated in UTC unless [`Periodic::tz`] says otherwise.
    pub fn cron(name: impl Into<String>, expression: impl Into<String>) -> Self;
    /// Ticks every `interval` (whole seconds, at least one), aligned to the Unix epoch.
    pub fn every(name: impl Into<String>, interval: Duration) -> Self;

    /// The job: `J` with these arguments; `J::options()` apply under the options set here.
    pub fn job<J: Job>(self, args: J::Args) -> Self;
    /// The job by class name and JSON arguments (a job this process may not know).
    pub fn class(self, class: impl Into<String>, args: Vec<serde_json::Value>) -> Self;

    // Job options, as on `JobOptions`/`JobSetter` (they win over the job type's options).
    pub fn queue(self, queue: impl Into<String>) -> Self;
    pub fn retry(self, retry: impl Into<RetryOption>) -> Self;
    pub fn backtrace(self, backtrace: impl Into<BacktraceOption>) -> Self;
    pub fn dead(self, dead: bool) -> Self;
    pub fn tags<I, S>(self, tags: I) -> Self where I: IntoIterator<Item = S>, S: Into<String>;
    pub fn retry_for(self, retry_for: Duration) -> Self;
    pub fn retry_queue(self, queue: impl Into<String>) -> Self;
    pub fn log_level(self, level: tracing::Level) -> Self;
    pub fn custom(self, key: impl Into<String>, value: impl Into<serde_json::Value>) -> Self;
    pub fn options(self, options: &JobOptions) -> Self;
    // Unique jobs and throttles (unique and rate-limiting designs), same meaning as on `JobSetter`.
    pub fn unique_for(self, ttl: Duration) -> Self;
    pub fn unique_until(self, until: UniqueUntil) -> Self;
    pub fn unique_key(self, key: impl Into<serde_json::Value>) -> Self;   // explicit key
    pub fn throttle(self, name: impl Into<String>) -> Self;
    pub fn throttle_key(self, key: impl Into<String>) -> Self;

    // Timetable and policy.
    /// IANA time zone of a cron timetable (default `UTC`).
    pub fn tz(self, zone: impl Into<String>) -> Self;
    /// Offset of an interval timetable's ticks (whole seconds, below the interval).
    pub fn offset(self, offset: Duration) -> Self;
    /// What to do with ticks missed while nothing could enqueue them (default `Latest`).
    pub fn catch_up(self, policy: CatchUp) -> Self;
    /// Never enqueue a tick more than this late (default: none, or 5 minutes for `Skip`).
    pub fn deadline(self, deadline: Duration) -> Self;
    /// Create the schedule paused (ignored when it already exists).
    pub fn paused(self, paused: bool) -> Self;
    /// Free text shown by the dashboard and the CLI.
    pub fn description(self, text: impl Into<String>) -> Self;

    // Introspection, validation and testing.
    pub fn name(&self) -> &str;
    /// Checks the name, the expression (it must tick within croner's search window), the zone,
    /// the interval and offset, the job (class, arguments, options): `Error::InvalidSchedule`.
    pub fn validate(&self) -> Result<()>;
    /// The next `count` ticks strictly after `after`.
    pub fn upcoming(&self, after: SystemTime, count: usize) -> Result<Vec<SystemTime>>;
    /// English description, e.g. "At 02:00 (Europe/Paris)" (croner's `describe` + zone).
    pub fn describe(&self) -> String;
    /// The job the poller would push for `tick`, before client middleware.
    pub fn payload(&self, tick: SystemTime) -> Result<JobPayload>;
    /// Pushes `payload(tick)` through `client` (fake, inline or Redis clients): for tests.
    pub async fn enqueue(&self, client: &Client, tick: SystemTime) -> Result<Option<String>>;
}

/// What to do with ticks that could not be enqueued on time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum CatchUp {
    /// Enqueue one job for the most recent missed tick (default).
    #[default]
    Latest,
    /// Enqueue a tick only within its deadline (5 minutes unless set); never catch up.
    Skip,
    /// Enqueue every missed tick, oldest first, at most the `max` most recent ones.
    All { max: u32 },
}

/// The tick a job was enqueued for (`JobContext::periodic`).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct PeriodicTick {
    /// The schedule's name.
    pub name: String,
    /// The tick (for a manual enqueue: when it was requested).
    pub tick: SystemTime,
    /// `true` for "enqueue now" (dashboard, CLI, API).
    pub manual: bool,
}

impl JobContext {
    /// The periodic schedule and tick that enqueued this job, if any.
    pub fn periodic(&self) -> Option<PeriodicTick>;
}
```

Template rules (checked by `validate`): class non-empty; arguments serialize to a JSON array that
deserializes back into `J::Args` (the `args_to_values` check); no `jid` and no `at` (they are
per tick); queue non-empty if set; `retry_for` < 1e9 s; custom keys must not name payload
fields; no field another feature sets per push or per execution: `bid` and `batch_callback`
(batches: a template's `bid` would add nothing to that batch, since members are registered only by
commits), `periodic`, `limited` (rate limiting); `unique.on_conflict` must be `drop` or unset (a poller
has nobody to return `Error` to, and `Replace` would let a tick remove another tick's job). At
registration the template is the payload `{class, args}` with the options set on
the `Periodic` applied (`JobOptions::apply_overrides`), then `J::options()` filling the gaps
(`apply_defaults`): the stored template is self-contained, so a process that does not know `J`
(another version, another service) fires it identically. Typed keys are part of that: when the merged
options make the job unique (or throttled) and no explicit key was set, `job::<J>(args)` evaluates
`J::unique_key(&args)` (and `J::throttle_key(&args)`) at definition time and stores the result as the
template's explicit `unique.key` (and `throttle.key`), because the fire-time normalization
(`Client::prepare_push`) is untyped and would otherwise hash all the arguments. The firing process's
`default_job_options` and client middleware still apply at push time (`Client::prepare_push`),
exactly as for scheduled jobs; normalization then computes the digest and the throttle field from the
template.

Name rules: 1–200 bytes, no control characters, no leading or trailing whitespace, and not `.`
or `..` (the dashboard and the JSON API name schedules in URL paths, where those are dot
segments that browsers and HTTP clients resolve away; round 3, web-api-security-7); names are
unique across code and API schedules. Group names: same rules, 1–100 bytes.

#### 2.3.2 Builder and configuration

```rust
impl HenchBuilder {
    /// Registers a code-defined schedule (repeatable; names must be unique).
    pub fn periodic(self, periodic: Periodic) -> Self;
    /// The group whose crontab this process registers and prunes (default "default"). Setting
    /// it explicitly also makes an empty crontab authoritative (it prunes the group).
    pub fn periodic_group(self, group: impl Into<String>) -> Self;
    /// Runs the periodic poller in this process (default true).
    pub fn periodic_poller(self, enabled: bool) -> Self;
    /// Longest wait between two looks at the due index (default 5 s).
    pub fn periodic_poll_interval(self, interval: Duration) -> Self;
    /// Enqueues remembered per schedule (default 10; 0 keeps none).
    pub fn periodic_history(self, entries: usize) -> Self;
    #[doc(hidden)] pub fn periodic_lease(self, lease: Duration) -> Self;          // 60 s
    #[doc(hidden)] pub fn periodic_check_interval(self, every: Duration) -> Self; // 60 s
    #[cfg(feature = "test-util")] #[doc(hidden)]
    pub fn periodic_clock(self, clock: hench::test_util::ManualClock) -> Self;
}
```

New `Config` fields (public, like the others): `periodic: Vec<Periodic>`, `periodic_group:
Option<String>`, `periodic_poller: bool`, `periodic_poll_interval: Duration`, `periodic_history:
usize`, `periodic_lease: Duration`, `periodic_check_interval: Duration`. `build()` validates
before any I/O: each `Periodic::validate()` (errors become `Error::Config("periodic job
`<name>`: <reason>")`), duplicate names, group name, positive intervals, lease ≥ 2 × poll
interval; a `class(...)` template whose class is not in the registry is a warning (it may be
another service's job), not an error. `TestRedis::builder()` sets a 100 ms poll interval, a 2 s
lease and a 200 ms check interval.

Defaults:

| Setting | Default | Notes |
|---|---|---|
| `periodic_group` | `"default"` (implicit) | explicit ⇒ empty crontab prunes |
| `periodic_poller` | `true` | every worker process polls |
| `periodic_poll_interval` | 5 s | max sleep; pollers wake exactly when the earliest tick is due |
| `periodic_history` | 10 | sidekiq-cron's default |
| `periodic_lease` | 60 s | same as the scheduler's lease |
| `periodic_check_interval` | 60 s | registration check (L1) |
| poll jitter | 0–250 ms | internal constant, spreads simultaneous wake-ups |
| `Skip` deadline | 5 min | longer than a lease plus a poll |
| timetable-change grace | 60 s | §2.2.3 |
| missed-tick count cap | 10 000 | cron timetables |
| `All { max }` | 1 ≤ max ≤ 10 000 | validated |

Worker TOML (`hench::cli`, feature `cli`): top-level keys `periodic_group` (string),
`periodic_poller` (bool), `periodic_poll_interval` (float seconds), `periodic_history`
(integer), and an array of tables `[[periodic]]` (an environment section may replace it, shallow
merge as for other keys):

```toml
[[periodic]]
name = "nightly-report"
cron = "0 2 * * *"            # or: every = 30 (seconds), offset = 5
tz = "Europe/Paris"
class = "nightly_report"
args = []
queue = "reports"
retry = 3                     # true | false | n
tags = ["reports"]
catch_up = "latest"           # "latest" | "skip" | "all"
catch_up_max = 24             # with "all"
deadline = 3600               # seconds
paused = false
description = "Daily PDF report"
```

Unknown keys, wrong types, both `cron` and `every`, or an invalid schedule are `Error::Config`
naming the file, the table index and the key.

Today every top-level table of the file other than `capsules` is an environment section (DESIGN §9e).
`periodic` therefore becomes a reserved name, like the rate-limiting design's `throttles` and
`queue_limits` (cross-feature review): accepted at the top level and inside an environment section (an
environment's `[[periodic]]` array replaces the top-level one as a whole), and a file whose environment
is literally named `periodic` gets an `Error::Config` naming the reserved word (release note). Template
tables also accept `unique_for` (seconds), `unique_until`, `throttle` and `throttle_key`.

#### 2.3.3 Errors

`Error` is `#[non_exhaustive]`; add `Error::InvalidSchedule(String)` ("invalid periodic job:
…"), returned by `Periodic::validate`, the API (`upsert` of an invalid definition, `remove` of a
code-defined schedule without force) and the CLI. The builder maps it to `Error::Config`.

#### 2.3.4 API (`hench::api`)

```rust
impl Api {
    /// The periodic schedules.
    pub fn periodic(&self) -> PeriodicSet;
}

#[derive(Clone)]
pub struct PeriodicSet { /* api */ }

impl PeriodicSet {
    pub async fn size(&self) -> Result<u64>;                                  // ZCARD periodic-names
    /// A page of schedules sorted by name (ZRANGE BYLEX on the name index + pipelined HGETALL).
    pub async fn page(&self, page: usize, size: usize) -> Result<Page<PeriodicEntry>>;
    /// Every schedule, by name, 100 per round trip.
    pub fn entries(&self) -> impl Stream<Item = Result<PeriodicEntry>> + Send + 'static;
    pub async fn get(&self, name: &str) -> Result<Option<PeriodicEntry>>;
    /// Creates or updates a schedule of source `api` (refuses names owned by code).
    pub async fn upsert(&self, periodic: &Periodic) -> Result<Upserted>;
    /// Removes an `api` schedule; `Error::InvalidSchedule` for a code-defined one.
    pub async fn remove(&self, name: &str) -> Result<bool>;
    /// Removes any schedule (a code-defined one comes back at its group's next registration).
    pub async fn remove_any(&self, name: &str) -> Result<bool>;
    pub async fn pause(&self, name: &str) -> Result<bool>;                    // changed?
    pub async fn unpause(&self, name: &str) -> Result<bool>;
    /// Pushes the schedule's job now (tick = now, manual); the timetable is untouched.
    pub async fn enqueue(&self, name: &str) -> Result<Option<String>>;       // jid; None: halted
    /// The registered crontab groups (digest, size, when and by which process).
    pub async fn groups(&self) -> Result<Vec<CrontabGroup>>;
    /// Audit: re-indexes active schedules whose due-index entry or `next` is missing (with
    /// `ZADD NX`, so leases are kept) and drops names whose hash is gone; returns the repairs.
    /// Registration does the same for code schedules automatically.
    pub async fn repair(&self) -> Result<u64>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Upserted { Created, Updated, Rescheduled, Unchanged }

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct PeriodicEntry {
    pub name: String,
    pub source: PeriodicSource,            // Code { group } | Api
    pub timetable: Timetable,              // Cron { expression, tz } | Every { interval, offset }
    pub description: Option<String>,
    pub job: JobPayload,                   // the stored template
    pub catch_up: CatchUp,
    pub deadline: Option<Duration>,
    pub next: Option<SystemTime>,          // None while paused (or parked with an error)
    pub paused_at: Option<SystemTime>,
    pub last: Option<LastEnqueue>,         // tick, at, jid, by (identity), result
    pub last_run: Option<LastRun>,         // tick, at, status, duration, error, jid
    pub manual_at: Option<SystemTime>,
    pub fired: u64,
    pub missed: u64,
    pub skipped: u64,
    pub run_failures: u64,
    pub error: Option<String>,             // why the poller cannot fire it (parked)
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
}

impl PeriodicEntry {
    pub fn upcoming(&self, count: usize) -> Result<Vec<SystemTime>>;
    pub fn describe(&self) -> String;
    pub async fn history(&self, count: usize) -> Result<Vec<HistoryEntry>>;
    pub async fn pause(&self) -> Result<bool>;
    pub async fn unpause(&self) -> Result<bool>;
    pub async fn enqueue(&self) -> Result<Option<String>>;
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct HistoryEntry {
    pub tick: Option<SystemTime>,          // None for manual enqueues
    pub at: SystemTime,
    pub result: FireResult,                // Enqueued | Scheduled | Halted | Skipped | Manual
    pub jid: Option<String>,
    pub by: Option<String>,
    pub missed: u64,
    pub late: Option<Duration>,
}
```

`Timetable`, `PeriodicSource`, `LastEnqueue`, `LastRun` (`RunStatus::{Succeeded, Failed,
Interrupted, Rescheduled}`; `Rescheduled` = the job ran and returned over a limiter's limit, so the
rate limiter put it back in `schedule`), `FireResult` and `CrontabGroup` are plain `#[non_exhaustive]`
data types. Entries
are lenient like `JobRecord`: a hash with fields the reader does not understand still lists
(with `error` set).

Dynamic schedules (for example one per tenant):

```rust
let set = Api::from_client(client.clone()).periodic();
set.upsert(
    &Periodic::cron(format!("digest-{tenant}"), "0 8 * * MON-FRI")
        .tz(tenant_zone)
        .job::<send_digest>((tenant,)),
)
.await?;
set.pause(&format!("digest-{tenant}")).await?;
```

#### 2.3.5 Payload metadata

The poller sets `"periodic": {"name": "<name>", "tick": <ms>}` on the job; "enqueue now" adds
`"manual": true` (tick = request time). Integer milliseconds, like every payload timestamp.
`JobPayload` gains a named field `pub periodic: Option<PeriodicMeta>` (serde `default`, skipped
when `None`) and `"periodic"` joins `JobPayload::RESERVED_KEYS`, the approach the unique-jobs
design takes for its `"unique"` field (`/home/user/scratch/design/unique.md` §2.6.4): a custom
key cannot impersonate it, and reading is lenient (a malformed value becomes `None` with a
`warn`, never an unparseable job). `JobContext::periodic()` returns it as a `PeriodicTick`.
Retries, reliable-fetch recovery and interruptions keep the payload, hence the tick. The
dashboard's job pages (retries, scheduled, morgue) show a "Periodic" row linking to the
schedule (built in, like the iteration row; the new `HenchWeb::job_info` extension point is for
host applications), read through `JobRecord::periodic() -> Option<PeriodicTick>` (new, lenient, in
`api/periodic.rs` next to the unique and rate-limiting designs' `JobRecord::unique()`,
`limited()` and `throttle()`).

### 2.4 Redis data model

All keys come from `hench::keys`; none is hand-formatted elsewhere. Names are free text, so each
key kind has its own prefix and no name can produce another kind's key (`periodic:` vs
`periodic-…`).

| Key (`hench::keys`) | Type | Contents | TTL |
|---|---|---|---|
| `periodic` (`PERIODIC`) | ZSET | member = schedule name; score = epoch seconds of `next`, or of the end of a lease, or of a parked retry. Active (not paused) schedules only. | none |
| `periodic-names` (`PERIODIC_NAMES`) | ZSET | every schedule name, score 0 (listing by `ZRANGE BYLEX`, counting by `ZCARD`) | none |
| `periodic:<name>` (`periodic(name)`) | HASH | definition and state, below | none |
| `periodic-history:<name>` (`periodic_history(name)`) | LIST | JSON history entries, newest first, `LTRIM` to `periodic_history` (an "enqueue now" through an `Api` without the configuration keeps the list's length plus the new entry, at least 10, at most 1 000 unless longer: round 3) | none (deleted with the schedule) |
| `periodic-group:<group>` (`periodic_group(group)`) | SET | names of the code schedules registered by the group | none |
| `periodic-crontabs` (`PERIODIC_CRONTABS`) | HASH | field = group; value = JSON `{"digest","count","at" (ms),"by" (identity),"version"}` | none |

Hash `periodic:<name>`:

| Field | Written by | Contents |
|---|---|---|
| `name` | UPSERT, SYNC | the name (redundant, for readers) |
| `source` | UPSERT, SYNC | `code` or `api` |
| `group` | SYNC | the group (code schedules) |
| `spec` | UPSERT, SYNC | timetable JSON: `{"v":1,"cron":"0 2 * * *","tz":"Europe/Paris"}` or `{"v":1,"every":30,"offset":0}` |
| `timing` | UPSERT, SYNC | canonical timetable key, `cron\|0 2 * * *\|Europe/Paris` or `every\|30\|0`: a change of it reschedules |
| `policy` | UPSERT, SYNC | `{"catch_up":"latest"}`, `{"catch_up":"skip","deadline":300}`, `{"catch_up":"all","max":24,"deadline":null}` |
| `job` | UPSERT, SYNC | the template (payload JSON without jid, at, created_at, enqueued_at) |
| `description` | UPSERT, SYNC | optional text |
| `digest` | UPSERT, SYNC | SHA-256, base64url (43 characters), of the canonical JSON `{"v":1,"spec","policy","job","description"}`, both from the crate-wide helpers the unique design's digests use (`crate::canonical`: object keys sorted recursively, whatever `serde_json`'s `preserve_order` feature says; a `serde_json` map is *not* sorted when another crate enables that feature, which would make two builds of one crontab disagree) |
| `created_at`, `updated_at` | UPSERT, SYNC | epoch ms |
| `next` | FIRE, UPSERT, SYNC, UNPAUSE, PAUSE (deletes) | epoch seconds of the next tick to enqueue |
| `paused` | PAUSE, UPSERT/SYNC (created paused), UNPAUSE (deletes) | epoch ms when paused |
| `last_tick`, `last_at`, `last_jid`, `last_by`, `last_result` | FIRE | last tick handled (s), when (ms, Redis clock), jid, process identity, `enqueued`/`scheduled`/`halted`/`skipped` |
| `fired`, `skipped`, `missed` | FIRE | counters |
| `manual_at`, `manual_jid` | ENQUEUE_NOW | last manual enqueue |
| `run_tick`, `run_status`, `run_ms`, `run_at`, `run_jid`, `run_error`, `run_failures` | RECORD_RUN | outcome of the latest execution (`ok`/`failed`/`interrupted`/`rescheduled`) |
| `error`, `error_at` | poller (HSET) | why the schedule is parked (unsupported definition, no next tick, invalid template) |

History entry (JSON, built by the poller): `{"tick": 1730426400, "at": 1730426400183,
"result": "enqueued", "jid": "…", "by": "host:4242:0a1b2c3d4e5f", "missed": 0, "late_ms": 183}`;
manual: `{"tick": null, "at": …, "result": "manual", "jid": "…", "by": "<who>"}`.

Memory: a schedule costs roughly 1 KB of hash plus about 200 bytes per history entry; 100 000
dynamic schedules with the default history ≈ 300 MB. Large tenant-style deployments should set
`periodic_history(0)` or a small value (§4, R7).

Heartbeat `info` JSON (key `<identity>`) gains `"periodic": {"poller": true, "group":
"default", "digest": "<crontab digest>", "schedules": 3}` so that the dashboard can tell
whether any live process polls and whether processes of one group disagree on its crontab. It also
carries the crate-wide `"features"` map (cross-feature review) with `"periodic": 1`, the version of the
stored definition format the process understands (`spec.v`), so the dashboard can name processes that
cannot read a newer definition.

### 2.5 Lua scripts

Conventions: every key is declared (`KEYS`), so the scripts work on Dragonfly's default script
mode; writes that can fail (out of memory) come first; times come from the poller (one Redis
`TIME` per lease) except in LEASE itself. All nine scripts were run against `redis-server`
7.0.15 (Appendix B). Lua `false` replies arrive as nil. In code (cross-feature review) the scripts
are `.lua` files under `crates/hench/src/periodic/lua/`, assembled by the crate-wide script builder
after the shared prelude, and named with the feature prefix so that generic names (`REMOVE`, `PAUSE`,
`LEASE`) never collide with other features' scripts in code or logs: `PERIODIC_LEASE`, `PERIODIC_FIRE`,
`PERIODIC_SYNC`, `PERIODIC_UPSERT`, `PERIODIC_REMOVE`, `PERIODIC_PAUSE`, `PERIODIC_UNPAUSE`,
`PERIODIC_ENQUEUE_NOW`, `PERIODIC_RECORD_RUN`. Helpers, if any, use the `hp_` prefix (the unique,
batch and limit libraries use `hu_`, `hb_` and `hl_`, so `FIRE` can prepend the unique library
without clashes). The text below keeps the short names.

**LEASE** — take the earliest due schedule; also tells the poller how long to sleep.

```lua
-- LEASE: take the earliest due schedule out of the other pollers' reach for ARGV[1] seconds.
-- KEYS[1] periodic (due index). ARGV[1] lease seconds, ARGV[2] now override ('' = Redis TIME),
-- ARGV[3..] names this poller cannot handle (skipped).
-- Returns {now, name, score} for a due schedule, {now, false, earliest score or false} otherwise.
local now
if ARGV[2] ~= '' then
  now = tonumber(ARGV[2])
else
  local t = redis.call('TIME')
  now = tonumber(t[1]) + tonumber(t[2]) / 1000000
end
local skip = {}
for i = 3, #ARGV do skip[ARGV[i]] = true end
local due = redis.call('ZRANGE', KEYS[1], '-inf', now, 'BYSCORE', 'LIMIT', 0, #ARGV - 1, 'WITHSCORES')
for i = 1, #due, 2 do
  if not skip[due[i]] then
    redis.call('ZADD', KEYS[1], 'XX', now + tonumber(ARGV[1]), due[i])
    return {tostring(now), due[i], due[i + 1]}
  end
end
local first = redis.call('ZRANGE', KEYS[1], 0, #ARGV - 2, 'WITHSCORES')
for i = 1, #first, 2 do
  if not skip[first[i]] and tonumber(first[i + 1]) > now then
    return {tostring(now), false, first[i + 1]}
  end
end
return {tostring(now), false, false}
```

`TIME` followed by writes is allowed in scripts from Redis 5 on (effects replication; mandatory
in 7). The override exists for tests (§2.3.2) and as a fallback for stores without `TIME` in
scripts. The skip list lets an older process ignore definitions it cannot evaluate (a newer
`v`, an unknown zone) without stealing them from newer processes for a whole lease. Round 3:
every skipped name is sent (up to 10 000; the first design sent the 100 most recent, and with
more due skipped definitions the poll leased, restored and leased again forever); past the cap a
skipped name handed back keeps its lease, and a poll yields after 1 000 unreadable
definitions.

**FIRE** — enqueue one tick and advance, at most once per tick.

```lua
-- FIRE: enqueue the job of one tick and advance the schedule, at most once per tick.
-- KEYS: 1 periodic, 2 periodic:<name>, 3 periodic-history:<name>, 4 queues,
--       5 target (queue:<q> when enqueued, schedule when scheduled, any key otherwise).
-- ARGV: 1 name, 2 expected next ('' = none), 3 expected digest, 4 new next ('' = none), 5 tick fired
--       ('' = none), 6 mode (enqueued|scheduled|halted|skipped), 7 job JSON, 8 queue name or
--       run-at score, 9 missed ticks, 10 history entry JSON, 11 history cap, 12 jid,
--       13 identity, 14 now (ms).
-- Returns 1 done, 2 tick not after last_tick (advanced, nothing enqueued), 0 tick already
-- handled, -1 definition changed, -2 paused, -3 gone.
local due, hash = KEYS[1], KEYS[2]
if redis.call('EXISTS', hash) == 0 then
  redis.call('ZREM', due, ARGV[1])
  return -3
end
local s = redis.call('HMGET', hash, 'next', 'digest', 'paused', 'last_tick')
if s[3] then return -2 end
if s[2] ~= ARGV[3] then return -1 end
if (s[1] or '') ~= ARGV[2] then return 0 end
local mode, tick, result = ARGV[6], ARGV[5], 1
if tick ~= '' and s[4] and tonumber(tick) <= tonumber(s[4]) then
  -- Never enqueue an instant that was already handled, nor move last_tick backwards.
  mode, tick, result = 'skipped', '', 2
  redis.call('HINCRBY', hash, 'skipped', 1)
end
if mode == 'enqueued' then
  redis.call('SADD', KEYS[4], ARGV[8])
  redis.call('LPUSH', KEYS[5], ARGV[7])
elseif mode == 'scheduled' then
  redis.call('ZADD', KEYS[5], ARGV[8], ARGV[7])
end
if mode == 'enqueued' or mode == 'scheduled' then
  redis.call('HSET', hash, 'last_tick', tick, 'last_at', ARGV[14], 'last_jid', ARGV[12],
    'last_by', ARGV[13], 'last_result', mode)
  redis.call('HINCRBY', hash, 'fired', 1)
elseif tick ~= '' then
  redis.call('HSET', hash, 'last_tick', tick, 'last_at', ARGV[14], 'last_result', mode)
  redis.call('HINCRBY', hash, 'skipped', 1)
end
if tonumber(ARGV[9]) > 0 then
  redis.call('HINCRBY', hash, 'missed', ARGV[9])
end
if ARGV[4] ~= '' then
  redis.call('HSET', hash, 'next', ARGV[4])
  redis.call('ZADD', due, ARGV[4], ARGV[1])
else
  redis.call('HDEL', hash, 'next')
  redis.call('ZREM', due, ARGV[1])
end
local cap = tonumber(ARGV[11])
if cap > 0 and ARGV[10] ~= '' and result == 1 then
  redis.call('LPUSH', KEYS[3], ARGV[10])
  redis.call('LTRIM', KEYS[3], 0, cap - 1)
end
return result
```

Counters: `fired` counts ticks whose job was written, `skipped` ticks consumed without a job
(halted by middleware, duplicates with unique jobs, the guard), `missed` ticks dropped by the
catch-up policy. Result `2` (the guard fired: `next` held an instant not after `last_tick`,
which only a bug or a clock jump can produce) is logged as a warning by the poller.

Modes: `enqueued`/`scheduled` mirror `client::Stored` (queued with `enqueued_at`, or scheduled
by client middleware without `at`/`enqueued_at`, exactly as `raw_push` and the API's `MOVE_OUT`
encode jobs); `halted` = client middleware stopped the push (the tick is consumed, as Sidekiq
drops a halted push); `skipped` = the policy enqueues nothing now (only `missed` and `next`
change; also used with an empty expected `next` to repair a schedule whose `next` was lost).
With unique jobs, a fifth mode `duplicate` and the lock step of `UNIQUE_PUSH` join the
`enqueued`/`scheduled` branch (§2.10); the script above is the version without them.

**SYNC** — register a group's crontab atomically (§2.7).

```lua
-- SYNC a group's crontab (code-defined schedules) in one step: create or update every
-- schedule of the crontab and delete the group's schedules that are no longer in it.
-- KEYS: 1 periodic, 2 periodic-names, 3 periodic-crontabs, 4 periodic-group:<group>,
--       then periodic:<name> for each of the n schedules (ARGV order),
--       then periodic:<stale>, periodic-history:<stale> for each stale name read beforehand.
-- ARGV: 1 group, 2 crontab record JSON, 3 the group's members as read (sorted, joined by
--       "\n"), 4 n, 5 now (ms), then 10 values per schedule: name, digest, spec, timing,
--       policy, job, description ('' = none), next for a new schedule, next for a changed
--       timetable, created paused ('1'/'0').
-- Returns {-1} if the group changed since it was read (read again and retry), else
--         {1, created, updated, rescheduled, unchanged, removed, conflicts...} where the
--         conflicts are names already used by an API schedule or by another group.
local members = redis.call('SMEMBERS', KEYS[4])
table.sort(members)
if table.concat(members, '\n') ~= ARGV[3] then return {-1} end
local n = tonumber(ARGV[4])
local now = ARGV[5]
local created, updated, rescheduled, unchanged, removed = 0, 0, 0, 0, 0
local result = {1}
local conflicts = {}
for i = 1, n do
  local a = 5 + (i - 1) * 10
  local name, digest, spec, timing, policy, job, descr, next_new, next_changed, paused =
    ARGV[a + 1], ARGV[a + 2], ARGV[a + 3], ARGV[a + 4], ARGV[a + 5], ARGV[a + 6], ARGV[a + 7],
    ARGV[a + 8], ARGV[a + 9], ARGV[a + 10]
  local key = KEYS[4 + i]
  local cur = redis.call('HMGET', key, 'digest', 'source', 'timing', 'paused', 'group', 'next')
  if cur[1] and (cur[2] ~= 'code' or cur[5] ~= ARGV[1]) then
    conflicts[#conflicts + 1] = name
  elseif cur[1] == digest then
    unchanged = unchanged + 1
    redis.call('SADD', KEYS[4], name)
    redis.call('ZADD', KEYS[2], 0, name)
    if not cur[4] then -- repair a lost index entry (NX keeps a poller's lease)
      if cur[6] then
        redis.call('ZADD', KEYS[1], 'NX', cur[6], name)
      else
        redis.call('HSET', key, 'next', next_new)
        redis.call('ZADD', KEYS[1], next_new, name)
      end
    end
  else
    redis.call('HSET', key, 'name', name, 'digest', digest, 'source', 'code', 'group', ARGV[1],
      'spec', spec, 'timing', timing, 'policy', policy, 'job', job, 'updated_at', now)
    if descr ~= '' then redis.call('HSET', key, 'description', descr)
    else redis.call('HDEL', key, 'description') end
    redis.call('SADD', KEYS[4], name)
    if not cur[1] then
      redis.call('HSET', key, 'created_at', now)
      redis.call('ZADD', KEYS[2], 0, name)
      if paused == '1' then
        redis.call('HSET', key, 'paused', now)
      else
        redis.call('HSET', key, 'next', next_new)
        redis.call('ZADD', KEYS[1], next_new, name)
      end
      created = created + 1
    elseif cur[3] ~= timing and not cur[4] then
      redis.call('HSET', key, 'next', next_changed)
      redis.call('ZADD', KEYS[1], next_changed, name)
      rescheduled = rescheduled + 1
    else
      updated = updated + 1
    end
  end
end
for k = 5 + n, #KEYS, 2 do
  local key = KEYS[k]
  local name = string.sub(key, 10) -- after 'periodic:'
  local s = redis.call('HMGET', key, 'source', 'group')
  redis.call('SREM', KEYS[4], name)
  if not s[1] then
    redis.call('ZREM', KEYS[1], name)
    redis.call('ZREM', KEYS[2], name)
  elseif s[1] == 'code' and s[2] == ARGV[1] then
    redis.call('ZREM', KEYS[1], name)
    redis.call('ZREM', KEYS[2], name)
    redis.call('DEL', key, KEYS[k + 1])
    removed = removed + 1
  end
end
redis.call('HSET', KEYS[3], ARGV[1], ARGV[2])
result[2], result[3], result[4], result[5], result[6] = created, updated, rescheduled, unchanged, removed
for _, name in ipairs(conflicts) do result[#result + 1] = name end
return result
```

(`string.sub(key, 10)` relies on the `periodic:` prefix of `keys::periodic`; a unit test pins
the prefix length.)

**UPSERT** — one schedule, source `api` (also usable for `code` by tools).

```lua
-- UPSERT one schedule (API). KEYS: 1 periodic, 2 periodic:<name>, 3 periodic-names.
-- ARGV: 1 name, 2 expected digest ('' = must not exist, '*' = any), 3 digest, 4 source,
--       5 spec JSON, 6 timing key, 7 policy JSON, 8 job JSON, 9 description ('' = none),
--       10 next for a new schedule, 11 next for a changed timetable, 12 created paused
--       ('1'/'0'), 13 now (ms).
-- Returns {1} created, {2} updated, {3} rescheduled (timing changed), {0} unchanged,
--         {-1, digest} conflict, {-2, source} owned by the other source.
local cur = redis.call('HMGET', KEYS[2], 'digest', 'source', 'timing', 'paused')
local exists = cur[1] ~= false
if ARGV[2] == '' and exists then return {-1, cur[1]} end
if ARGV[2] ~= '' and ARGV[2] ~= '*' and ARGV[2] ~= (cur[1] or '') then return {-1, cur[1] or ''} end
if exists and cur[2] ~= ARGV[4] then return {-2, cur[2] or ''} end
if exists and cur[1] == ARGV[3] then return {0} end
redis.call('HSET', KEYS[2], 'name', ARGV[1], 'digest', ARGV[3], 'source', ARGV[4], 'spec', ARGV[5],
  'timing', ARGV[6], 'policy', ARGV[7], 'job', ARGV[8], 'updated_at', ARGV[13])
if ARGV[9] ~= '' then redis.call('HSET', KEYS[2], 'description', ARGV[9])
else redis.call('HDEL', KEYS[2], 'description') end
if not exists then
  redis.call('HSET', KEYS[2], 'created_at', ARGV[13])
  redis.call('ZADD', KEYS[3], 0, ARGV[1])
  if ARGV[12] == '1' then
    redis.call('HSET', KEYS[2], 'paused', ARGV[13])
  else
    redis.call('HSET', KEYS[2], 'next', ARGV[10])
    redis.call('ZADD', KEYS[1], ARGV[10], ARGV[1])
  end
  return {1}
end
if cur[3] ~= ARGV[6] and not cur[4] then
  redis.call('HSET', KEYS[2], 'next', ARGV[11])
  redis.call('ZADD', KEYS[1], ARGV[11], ARGV[1])
  return {3}
end
return {2}
```

`PeriodicSet::upsert` reads the current digest, computes both `next` candidates from Redis
`TIME`, and sends the script with that digest as expected value; on `{-1, …}` it re-reads and
retries (at most 5 times).

**REMOVE**, **PAUSE**, **UNPAUSE**, **ENQUEUE_NOW**, **RECORD_RUN**:

```lua
-- REMOVE a schedule. KEYS: 1 periodic, 2 periodic-names, 3 periodic:<name>,
-- 4 periodic-history:<name>, 5 periodic-group:<group> (any key when none).
-- ARGV: 1 name, 2 required source ('' = any). Returns 1 removed, 0 none, -2 other source.
local s = redis.call('HMGET', KEYS[3], 'source', 'group')
if not s[1] then
  redis.call('ZREM', KEYS[1], ARGV[1])
  redis.call('ZREM', KEYS[2], ARGV[1])
  return 0
end
if ARGV[2] ~= '' and s[1] ~= ARGV[2] then return -2 end
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[2], ARGV[1])
if s[2] then redis.call('SREM', KEYS[5], ARGV[1]) end
redis.call('DEL', KEYS[3], KEYS[4])
return 1
```

```lua
-- PAUSE. KEYS: 1 periodic, 2 periodic:<name>. ARGV: 1 name, 2 now (ms).
-- Returns 1 paused, 2 already paused, 0 no such schedule.
if redis.call('EXISTS', KEYS[2]) == 0 then return 0 end
if redis.call('HEXISTS', KEYS[2], 'paused') == 1 then return 2 end
redis.call('HSET', KEYS[2], 'paused', ARGV[2])
redis.call('HDEL', KEYS[2], 'next')
redis.call('ZREM', KEYS[1], ARGV[1])
return 1
```

```lua
-- UNPAUSE. KEYS: 1 periodic, 2 periodic:<name>. ARGV: 1 name, 2 next tick, 3 expected digest.
-- Returns 1 resumed, 2 not paused, 0 no such schedule, -1 definition changed (recompute).
if redis.call('EXISTS', KEYS[2]) == 0 then return 0 end
local s = redis.call('HMGET', KEYS[2], 'paused', 'digest')
if not s[1] then return 2 end
if s[2] ~= ARGV[3] then return -1 end
redis.call('HDEL', KEYS[2], 'paused')
redis.call('HSET', KEYS[2], 'next', ARGV[2])
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
return 1
```

```lua
-- ENQUEUE_NOW: push a schedule's job outside its timetable; the timetable is not touched.
-- KEYS: 1 periodic:<name>, 2 periodic-history:<name>, 3 queues, 4 target (queue or schedule).
-- ARGV: 1 mode (enqueued|scheduled|halted), 2 job JSON, 3 queue name or run-at score,
--       4 history entry JSON, 5 history cap, 6 now (ms), 7 jid.
-- Returns 1 done, 0 no such schedule.
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
if ARGV[1] == 'enqueued' then
  redis.call('SADD', KEYS[3], ARGV[3])
  redis.call('LPUSH', KEYS[4], ARGV[2])
elseif ARGV[1] == 'scheduled' then
  redis.call('ZADD', KEYS[4], ARGV[3], ARGV[2])
end
redis.call('HSET', KEYS[1], 'manual_at', ARGV[6], 'manual_jid', ARGV[7])
local cap = tonumber(ARGV[5])
if cap > 0 and ARGV[4] ~= '' then
  redis.call('LPUSH', KEYS[2], ARGV[4])
  redis.call('LTRIM', KEYS[2], 0, cap - 1)
end
return 1
```

```lua
-- RECORD_RUN: remember how the last execution of a schedule's job ended.
-- KEYS: 1 periodic:<name>. ARGV: 1 tick (s), 2 status (ok|failed|interrupted|rescheduled), 3 error text,
-- 4 duration (ms), 5 at (ms), 6 jid. Returns 1 recorded, 0 ignored (older tick or gone).
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
local cur = redis.call('HGET', KEYS[1], 'run_tick')
if cur and tonumber(cur) > tonumber(ARGV[1]) then return 0 end
redis.call('HSET', KEYS[1], 'run_tick', ARGV[1], 'run_status', ARGV[2], 'run_ms', ARGV[4],
  'run_at', ARGV[5], 'run_jid', ARGV[6])
if ARGV[3] ~= '' then redis.call('HSET', KEYS[1], 'run_error', ARGV[3])
else redis.call('HDEL', KEYS[1], 'run_error') end
if ARGV[2] == 'failed' then redis.call('HINCRBY', KEYS[1], 'run_failures', 1) end
return 1
```

Retry rules (with `RedisPool::with_retry`, DESIGN §4): LEASE, PAUSE, UNPAUSE, UPSERT, SYNC,
REMOVE and RECORD_RUN are idempotent or CAS-guarded and may be resent. FIRE may be resent too
(the CAS makes a second run a no-op), but a `0` after a resend is ambiguous, so the poller then
reads `last_tick`/`last_jid`: equal to its own tick and jid ⇒ its first attempt ran (reply
lost). ENQUEUE_NOW is sent once (a resend would push twice); on an error, `manual_jid` tells
whether it ran, the same way the API's moves check presence.

### 2.6 The poller

A background task per process (`periodic::poller::run(engine)`), spawned by the launcher next
to the scheduler when `periodic_poller` is true. It runs from start-up until the **shutdown
starts** (`engine.stopping`), not until quiet: a quiet process still enqueues periodic jobs,
which other processes run (Sidekiq Enterprise behaves the same; a rolling deploy that quiets
every old process before new ones start would otherwise drop ticks). An in-flight fire finishes
unless the end of the shutdown cancels the background tasks; its tick then waits for the lease.

```text
run(engine):
    register()                             # §2.7; retried every 5 s until it succeeds or stops
    loop until engine.stopping:
        outcome = background-cancellable, catch_unwind(poll())
        match outcome:
            shutdown end      -> break
            Ok(idle_for)      -> periodic_outage.succeeded(); sleep = idle_for
            Err(redis error)  -> periodic_outage.failed(..); sleep = 1 s (5 s if repeated)
            panic             -> handle_error("periodic poller crashed; restarting it"); sleep = 1 s
        every periodic_check_interval: check_registration()      # L1
        sleep(sleep + random 0..250 ms), cancellable by engine.stopping

poll():
    loop:
        (now, due, earliest) = LEASE(periodic_lease, clock override, skip list)
        if due is None:
            return clamp(earliest - now, 0, periodic_poll_interval)   # or the max if none
        fire(due.name, due.score, now)?     # Err only when Redis is unavailable
```

`fire(name, leased_score, now)`:

1. `HGETALL periodic:<name>`. Empty ⇒ nothing to do (FIRE would return -3; send
   `PERIODIC_UNINDEX` and return). `paused` set ⇒ `PERIODIC_UNINDEX` and return (round 3: the
   script removes the entry only if the hash is still missing or paused; a plain `ZREM` in a
   later round trip dropped the entry of a schedule resumed or re-created meanwhile).
2. Parse the definition (cached per digest, parsed again after five minutes so that a tzdata
   update reaches the poller). Unsupported `spec.v` or an unknown zone (old tzdata; the bundled
   database is consulted per name first) ⇒ `HSET error, error_at`, put the name on the local
   skip list for 10 minutes, restore the score (`ZADD XX <leased_score>`), warn once; return.
   Unparseable expression or template (invalid for every version) ⇒ the same, parked an hour.
3. `T = next`. Missing ⇒ repair: FIRE with expected `''`, mode `skipped`, new next =
   `next_after(now)`. `T > now` (rescheduled meanwhile) ⇒ `ZADD XX T` and return.
4. `plan = catch_up(timing, policy, deadline, T, now)` (§2.2.5): the tick to enqueue (or none),
   the missed count, the new `next`. A timing error with no next tick ⇒ park: FIRE with new
   next `''` (removes it from the index), `HSET error`, report `periodic job is invalid`.
5. No tick to enqueue ⇒ FIRE mode `skipped` (counts missed, advances), warn
   `periodic ticks were missed` with the name, count and policy; return.
6. Build the payload: template + fresh jid + `created_at` now + `periodic: {name, tick}`. Run
   `Client::prepare_push` under `catch_unwind` (a panic is a rejected push):
   - `Ok(Some(job))` ⇒ `Stored::encode(job, now, now_millis())` ⇒ mode `enqueued` (queue) or
     `scheduled` (middleware set a future `at`).
   - `Ok(None)` ⇒ mode `halted`.
   - `Err(InvalidJob | Json)` ⇒ the template can never be pushed: `HSET error`, score `now +
     1 h` (`ZADD XX`), report `periodic job is invalid`; return.
   - `Err(unavailable)` (processor's `is_unavailable`) ⇒ restore `ZADD XX T`; return `Err` (the
     poll stops; outage notice).
   - other `Err` (middleware failure, panic, `WRONGTYPE` on the queue) ⇒ `HSET error`, leave the
     lease (retry in 60 s), report through the rate-limited outage notices with the job JSON.
7. FIRE. `1` ⇒ info log `enqueued periodic job` (name, tick, jid, late ms), warn if late > 60 s.
   `2` ⇒ warn (the monotonic guard refused an instant already handled). `0` ⇒ disambiguate a
   lost reply (§2.5) and debug-log. `-1` ⇒ definition changed: back to 1 (at most 3 times, then
   leave the lease). `-2`/`-3` ⇒ debug-log.

A poll handles every due schedule before sleeping; there is no per-poll cap (other processes
lease due schedules concurrently, which spreads a burst such as 10 000 schedules due at
midnight). A process that cannot reach Redis stops polling until the next attempt; nothing is
held in memory across polls except the parse cache and the skip list.

Testing hook: with `HenchBuilder::periodic_clock(clock)` (feature `test-util`), LEASE receives
`ARGV[2] = clock.now()` and the registration, upsert and unpause paths use the same clock
instead of Redis `TIME`, so tests can jump hours without sleeping.

### 2.7 Code-defined schedules: registration, groups, deploys, data loss

- **Crontab.** The schedules given to the builder (or the TOML file) form the process's
  crontab for its group; its digest is the SHA-256 (base64url, crate-wide helper) of the sorted
  per-schedule digests.
- **Registration** (at start, in the poller task, before polling; also by processes with
  `periodic_poller(false)`): read `SMEMBERS periodic-group:<g>` and Redis `TIME`; stale names =
  members not in the crontab; compute `next_new = next_after(now)` and `next_changed =
  next_after(now − 60 s)` per schedule; send SYNC with all keys declared; `-1` ⇒ the group
  changed concurrently: read again and retry (at most 5 times, then report `could not register
  periodic jobs` and retry later). Log `registered periodic jobs` with the counts; each conflict
  (a name used by an API schedule or another group) is reported through the error handlers and
  that schedule is not registered.
- **When it runs.** Only when the crontab is non-empty or `periodic_group` was set explicitly:
  worker deployments without schedules never touch others' schedules.
- **Last registration wins.** A process re-registers only at start (and after data loss), never
  periodically. During a rolling deploy the new processes' crontab wins as they start; an old
  process that restarts mid-deploy re-registers the old crontab until the next new process
  starts. Timetable flips are harmless for the guarantees (CAS, monotonic ticks); the
  dashboard warns while live processes of a group declare different digests.
- **Groups.** Deployments with different crontabs sharing one Redis must use different groups;
  otherwise each start prunes the other deployment's schedules. Names stay unique across groups
  (a clash is a conflict). Sidekiq Enterprise has no such notion and requires every process to
  register the same jobs; groups keep that rule per group instead of per cluster.
- **Data loss** (L1). Every `periodic_check_interval` the poller reads `HGET periodic-crontabs
  <group>`: missing or unreadable ⇒ register again (Redis restarted without persistence, failed
  over to an empty replica, or someone deleted the keys); a different digest with an `at` before
  this process's own registration ⇒ Redis went back in time (a restore from a backup, a
  failover to a replica that missed the registration): register again (round 3; the first
  design took any other digest for a newer registration); a different digest registered since ⇒
  another process registered another crontab more recently: warn once, do nothing. A crontab's
  digest covers its schedules' names (round 3: renames were invisible). Ticks missed during the
  loss are not backfilled (their state is gone).
- **Removing the last schedule** of a group: keep `.periodic_group("x")` with an empty crontab
  (it prunes), or remove the schedules with `hench periodic remove --force`. The dashboard lets
  operators delete code schedules of a group that no live process declares.
- **Shutdown** writes nothing (L7): schedules belong to the cluster, not to processes.

### 2.8 Dynamic schedules

`PeriodicSet::upsert` stores schedules of source `api`. They are never pruned by registration
and never overwritten by code (a code schedule with the same name is a conflict). They fire like
code schedules. The CLI can create them (`hench periodic add`); the dashboard can pause, resume,
enqueue and delete them but not create or edit them in v1. Sidekiq Enterprise's guidance (keep
user-defined schedules in the application database, enqueued by one static schedule) remains a
valid pattern and is mentioned in the docs for very large sets (millions of user schedules).

### 2.9 Behaviour under failures

| Failure | Behaviour |
|---|---|
| Poller crashes after LEASE, before FIRE | Tick due again when the lease ends (60 s); fired once by any poller. |
| Reply of FIRE lost (timeout, dropped connection) | The script ran or not; a resend or another poller sees `next ≠ T`: no duplicate. The poller tells "mine" from "someone else's" by `last_jid`. |
| Reply of LEASE lost | That schedule waits for its lease (60 s); others are unaffected. |
| Client middleware slow (> lease) | Another poller may prepare the same tick; only one FIRE succeeds. Middleware side effects can run twice (documented). |
| Client middleware fails / panics | Error recorded on the schedule, reported (rate-limited); retried every lease period until the policy's deadline drops the tick. |
| Invalid template, unsupported definition, unknown zone | Schedule parked with `error`; other schedules unaffected; old processes skip definitions they cannot read. |
| Redis unavailable (down, `LOADING`, `OOM`, `READONLY`, …) | Poll stops, score restored when possible; outage reported once per episode; catch-up policy applies on recovery. FIRE under `OOM` writes nothing (Appendix B). |
| Redis restart with persistence | State intact; due ticks fire per policy. |
| Redis restart without persistence / empty failover target | Schedules re-registered within `periodic_check_interval`; dynamic schedules are lost (they live only in Redis — document: persistence is required for them). |
| Asynchronous failover losing the last writes | A fired tick can fire again (at-least-once); documented. |
| Worker clock skew | Irrelevant: due-ness uses Redis `TIME`; sleeps use monotonic time. |
| Redis clock jumps | Backward: ticks late by the jump. Forward: ticks early or missed per policy. Never duplicated (monotonic guard). |
| Process crash while a periodic job runs | Reliable fetch recovers and reruns it (same jid, same tick); at-least-once execution. |
| Job fails | Normal retries with the same tick; RECORD_RUN shows the failure; the timetable is unaffected (L19). |
| Many processes wake at one instant | One LEASE each (O(log n)); due schedules are spread across processes. |
| Shutdown / quiet | Quiet: keeps firing. Shutdown: stops polling; nothing is deleted (L7). |
| Mixed versions | Newer definition versions skipped by older processes (skip list); templates are self-contained JSON, so any version can fire them. |

### 2.10 Interplay with the rest of hench

- **Client middleware** runs for every periodic push (`Client::prepare_push`), as for scheduled
  jobs and API moves; it may modify, schedule or halt the job (halt consumes the tick).
- **Unique jobs** (designed in parallel: `/home/user/scratch/design/unique.md`). That design takes
  a lock `unique:<digest>` in the same Lua script that writes a *first push* (`UNIQUE_PUSH`, built
  from shared helpers `hu_take`/`hu_hold`/`hu_release` prepended to each script), stores
  `"unique": {"for", "until", "digest"}` in the payload (digest computed in `Client::normalize`
  over class, queue and arguments or a key — so the `periodic` metadata, which differs per tick,
  is not part of it), and never takes locks in the scheduler's `MOVE_OUT` or API moves. A periodic
  enqueue is a first push (fresh jid), so the contract is:
  1. `Periodic` gets the unique setters of `JobSetter` (`unique_for`, `unique_until`), or the same
     fields through `options(&JobOptions)`; the template stores them; `prepare_push` computes the
     digest at fire time.
  2. FIRE and ENQUEUE_NOW prepend the shared helpers and, for a job carrying a digest, declare
     three more keys (the lock, `unique-locks`, the events hash) and take ARGV `until`, `for`
     (ms), `at`, `class`, `digest`; before writing the job they `HGET` the lock's holder: another
     jid ⇒ nothing is written, the tick is consumed with mode `duplicate` (counted in `skipped`,
     history result `duplicate`, the unique design's `d` event counted); free ⇒ write the job,
     then `pcall(hu_take, …)` with the TTL rule `for + max(0, at − now)`. All of it stays inside
     FIRE's CAS, so a resent FIRE neither pushes nor locks twice. Concretely, after its CAS checks
     FIRE calls the unique library's frozen `hu_push_one(lock, index, events, digest, jid, class,
     ttl_ms, now_ms, fields, at, write)` with `write` = the closure doing FIRE's `SADD`+`LPUSH` or
     `ZADD`; `{0, holder}` maps to mode `duplicate`, `{1}` and `{4, err}` to `enqueued`/`scheduled`
     (the latter with a warning: the job was written without its lock), `{-1, err}` (the lock key
     has the wrong type) to an error recorded on the schedule with the tick left for the next lease.
     The unique design's `now_ms` is read with `TIME` inside FIRE (the poller's `now` argument is
     kept for the schedule's bookkeeping; the lock TTL follows Redis's clock).
  3. `on_conflict` other than `drop` is refused by `Periodic::validate` (a poller cannot return an
     error to anybody, and `replace` only reaches holders waiting in `schedule`).
  Overlap prevention for slow periodic jobs ("skip a tick while the previous run is pending or
  running") is expressed with `unique_for` about one period and `until = success`, as the unique
  design's §2.18 suggests, not with a periodic-specific option. Without it, ticks keep enqueuing
  while workers lag (BullMQ avoids this by chaining; we document it). The prefixes `unique:`,
  `unique-locks` and `u|` reserved by that design do not overlap the `periodic…` keys.
- **Retries / morgue / dead jobs:** unchanged; recommend `retry(false)` or a small `retry_for`
  for frequent schedules, so that retries do not overlap later ticks.
- **Reliable fetch, orphan recovery, interruptions (iterable jobs):** unchanged; the payload and
  its tick travel with the job.
- **Processor hook (RECORD_RUN):** after the outcome of a job whose payload has `periodic` is
  written, the processor spawns a task sending RECORD_RUN (status `ok`, `failed` with error class
  and message truncated to 500 characters, `interrupted`, or `rescheduled` when the job ran and
  returned an over-limit refusal that the rate limiter rescheduled; duration; jid). The metadata is
  read from the job *as fetched*, like every feature's per-execution state (middleware cannot redirect
  the record). Nothing is recorded for a job a throttle parked before it started, for shutdown
  requeues or for hard-shutdown interruptions: they did not run to an outcome. The hook is the last
  step of the processor pipeline (after batch accounting, the outcome script with its unique-lock work,
  and the permit release). Not awaited by the processor; errors debug-logged. Testing clients skip it.
- **Metrics:** periodic jobs are tracked per class like any job; schedules add their own
  counters (§2.13).
- **Testing modes:** `Periodic::payload`/`enqueue` and `validate`/`upcoming` cover unit tests;
  fake clients capture pushed periodic jobs with their metadata; no poller runs without Redis
  (time-driven features are off in testing clients, as the rate limiter's limits are). A fake or
  inline client's unique table applies to `Periodic::enqueue` like to any push.
- **Batches:** a template cannot carry `bid` or `batch_callback` (`validate`). A periodic job may
  define a batch per tick in its `perform`; since the job can run twice (at-least-once), a second
  batch would be created on a re-run. The batches design's `Batch::with_bid` makes this idempotent:
  `Batch::with_bid(ctx.client(), ctx.jid())?.commit_id(ctx.jid())` (the jid is 24 hex characters like a
  bid, and a re-run's commit is recognized as already applied).
- **Rate limiting:** ticks of a throttled job are enqueued on time and wait in `schedule` like any
  throttled job (a parked tick with `unique_for` keeps its lock, so later ticks are skipped as
  duplicates while it waits); typed throttle keys are stored in the template (§2.3.1); `RECORD_RUN`
  reports `rescheduled` as above. A schedule whose job a limiter keeps refusing shows it in its last
  run status, not in its enqueue history.
- **Several applications on one Redis DB** are unsupported in hench generally; with periodic
  jobs, any process may fire any schedule, with its own client middleware.

### 2.11 Dashboard (`hench-web`)

Tab "Periodic" (Sidekiq Enterprise's name since 7.0). Navigation order across the features
(cross-feature review): Dashboard, Busy, Queues, Retries, Scheduled, Dead, Periodic, Batches, Limits,
Locks, Metrics, then custom tabs (the three job sets stay together; the feature tabs follow). Like the
other feature tabs it has a switch, `HenchWeb::periodic(bool)` (default `true`; off, the routes answer
404, as with `metrics(false)`). Routes (relative to the mount):

- `GET /periodic` (`?page=`, `?substr=`): summary line (`12 periodic jobs, 1 paused.`); banners
  when no live process runs a poller, when live processes of a group declare different crontab
  digests, and when a schedule's `next` is more than `lease + poll interval` in the past
  (nothing is firing); a table sorted by name, 25 per page: checkbox, name (link) with chips
  (`paused`, `api`/group name, `error`), timetable (expression, zone, croner's English
  description as text), job (class, queue, arguments cut at 120 characters), next run (relative,
  absolute time in the schedule's zone with UTC in `title`), last enqueue (relative, result chip,
  "late 3 min" when late), last run (status chip, relative, duration), row actions (Enqueue now,
  Pause/Resume, Delete when allowed). Filter: literal text with `*` wildcards on name, class
  and expression, over at most 50 000 names (same rules as other lists).
- `POST /periodic`: `action` = `enqueue` | `pause` | `unpause` | `delete` on the selected `name`s
  (at most 1000). Delete is refused for code schedules unless their group is orphaned (no live
  process declares it); the notice counts them (`2 deleted, 1 defined in code`).
- `GET /periodic/{name}`: definition (source and group, description, timetable with the next 5
  ticks, catch-up policy and deadline, template with pretty-printed arguments and options,
  created/updated), state (next, paused since, last enqueue with tick, time, lateness, jid and
  process, last manual enqueue, last run with status, duration, error and jid, counters, parked
  error), history table (tick, enqueued at, lateness, result, jid, process). Actions as above.
- `POST /periodic/{name}`: the same actions for one schedule; unknown name ⇒ 404 page.

Names are percent-encoded in paths (names may contain `/`, `:`, spaces, any Unicode except
control characters; axum decodes `%2F` within a segment). Everything goes through askama
escaping (L14); actions answer `303` with a flash code in the dashboard's existing format
(`kind.count.failed.subject`, `flash.rs`), with new kinds `PeriodicEnqueued`, `PeriodicPaused`,
`PeriodicResumed`, `PeriodicDeleted` (the subject is the schedule's name for single actions);
read-only mode removes the forms and refuses the POSTs; CSRF and cross-site checks as for other
actions; the "Live" switch as on list pages. The job pages of the three sorted sets show a
"Periodic" row (schedule link, tick) for jobs carrying the metadata; feature rows come after the
built-in rows and the Iteration row in one order: Batch, Periodic, Unique, Throttle, Limited, then the
host's `job_info` rows. Strings go to `strings.rs` (its per-feature part).

### 2.12 The `hench` binary

- `hench periodic [list] [--json]`: `NAME  TIMETABLE  ZONE  NEXT  LAST  STATUS` (names and
  expressions printed with control characters spelled out, like other reports); JSON: one object
  per schedule (name, source, group, timetable, class, queue, next, paused, last, last_run,
  counters, error).
- `hench periodic show <name> [--json]`: definition, state and history.
- `hench periodic enqueue <name>`: prints the jid.
- `hench periodic pause|unpause <name>`.
- `hench periodic remove <name> [--force]` (`--force` for code schedules).
- `hench periodic add <name> (--cron EXPR [--tz ZONE] | --every DURATION [--offset DURATION])
  --class CLASS [--args JSON] [--queue Q] [--retry true|false|N] [--catch-up
  latest|skip|all:N] [--deadline DURATION] [--paused] [--description TEXT]`: dynamic upsert;
  prints `created`/`updated`/`rescheduled`/`unchanged`.
- `hench periodic repair`: runs `PeriodicSet::repair` and prints the number of repairs.
- `hench periodic next <EXPR> [--tz ZONE] [--count 5] [--after RFC3339]`: validates and previews
  an expression offline (no Redis), with the English description and DST notes when a listed
  tick falls on a transition. Operators use it like crontab.guru.
- `hench stats periodic`: count, paused, parked, next due, late schedules (also in `all`). Every
  feature has such a section (`periodic`, `batches`, `limits`, `locks`), printed by `all` when the
  feature has data; the overview stays as it is.
- JSON API (`hench web --api`): `GET /api/v1/periodic`, `GET /api/v1/periodic/{name}`, `POST
  /api/v1/periodic/{name}/{enqueue|pause|unpause}` (403 when read-only; 404 unknown). The four
  features follow one shape: `GET` for lists and items, `POST /<resource>/{id}/<action>` for actions,
  `DELETE /<resource>/{id}` for removing a resource (a lock, a limiter's state).
- `hench periodic add` also takes `--unique-for DURATION`, `--unique-until success|start|expired`,
  `--throttle NAME[:KEY]` (the template fields of §2.3.1; the binary has no job registry, so typed keys
  cannot be computed: an explicit `--unique-key JSON` is accepted).

### 2.13 Observability

- **Logs** (`tracing`, target `hench::periodic`): `registered periodic jobs` (info, counts);
  `enqueued periodic job` (info: name, tick, jid, late_ms; debug for intervals below a minute
  to avoid noise); `periodic job enqueued late` (warn, > 60 s); `periodic ticks were missed`
  (warn: name, missed, policy); `periodic job could not be enqueued` (through outage notices).
- **Error contexts** (added to DESIGN §5's list): `could not enqueue periodic jobs`, `periodic
  job is invalid`, `could not register periodic jobs`, `periodic poller crashed; restarting
  it`.
- **Counters per schedule** (hash): `fired`, `skipped`, `missed`, `run_failures`; lateness of the
  last enqueue (`last_at − last_tick`); exposed by the API, dashboard, CLI.
- **Heartbeat info:** poller flag, group, crontab digest and size (§2.4).
- **Rate-limited reporting** reuses `launcher::Outage` (`periodic_outage`, like
  `schedule_outage`).

### 2.14 Dependencies, feature flag, MSRV

```toml
[features]
default = ["macros", "periodic"]
periodic = ["dep:croner", "dep:jiff"]

[dependencies]
croner = { version = "4.0.2", default-features = false, features = ["jiff"], optional = true }
jiff = { version = "0.2.35", default-features = false, features = ["std", "tzdb-zoneinfo", "tzdb-concatenated", "tzdb-bundle-always"], optional = true }
# Digests: the crate-wide SHA-256 helper (`sha2`, added by the unique design and always built), not
# `sha1_smol` (cross-feature review: one hash for every hench digest, no SHA-1 audit findings).
```

`periodic` is the only one of the four features behind a cargo feature, because only it brings
heavy dependencies (`croner`, `jiff`); unique jobs, batches and rate limiting are always compiled (their
only new dependency is `sha2`). The dashboard's `HenchWeb::periodic(bool)` switch is independent of the
cargo feature (`hench-web` enables it).

- `croner`'s types never appear in hench's public API (a private `timing` module wraps it), so a
  future replacement (in-house parser, another crate) is not a breaking change.
- `croner` declares no MSRV; it built and ran on Rust 1.88 on 2026-11-01 (Appendix A). CI's
  existing MSRV job (`cargo +1.88 check --all-targets`) guards it; pin with `=4.0.2` if a patch
  release raises it.
- `jiff` without `tz-system`: hench never uses the host zone. `tzdb-bundle-always` keeps zones
  working in images without `/usr/share/zoneinfo`; the system database still wins when present
  and is re-read every 5 minutes, so tzdata updates apply without a rebuild.
- `hench-web` and `hench-server` enable `hench/periodic`.
- Licences (MIT; Unlicense OR MIT; MIT OR Apache-2.0 for `derive_builder`; MIT for `strum`)
  fit hench's MIT licence and the existing `cargo deny`/audit setup.

### 2.15 Module layout, lanes, order of work, documentation

| Path | Contents |
|---|---|
| `crates/hench/src/periodic/mod.rs` | `Periodic`, `CatchUp`, `PeriodicTick`, re-exports |
| `periodic/timing.rs` | croner/jiff wrapper: parse, validate, `next_after`, `latest_at_or_before`, `count`, `describe`, Apia retry |
| `periodic/definition.rs` | template building, canonical JSON, digests, timing keys, `spec` versions |
| `periodic/plan.rs` | catch-up planning (pure) |
| `periodic/store.rs` | the nine scripts and their Rust wrappers; `PeriodicClock` |
| `periodic/poller.rs` | the task, registration, check, fire procedure |
| `api/periodic.rs` | `PeriodicSet`, `PeriodicEntry`, history, groups |
| `keys.rs`, `error.rs`, `config.rs`, `job.rs` (`JobContext::periodic`) | spine additions |
| `launcher.rs` (spawn, info JSON, outage), `processor.rs` (RECORD_RUN hook) | engine additions |
| `cli.rs` | `[[periodic]]` and the new keys |
| `crates/hench-web/src/pages/periodic.rs`, `templates/periodic*.html`, `strings.rs`, router | dashboard |
| `crates/hench-server/src/{cli,commands,monitor,json_api}.rs` | binary |

Order: (1) timing + definition + unit/property tests; (2) keys, store scripts, store tests on
`TestRedis`; (3) poller, registration, launcher wiring, `JobContext::periodic`, integration and
chaos tests; (4) API; (5) processor hook; (6) dashboard; (7) CLI, TOML, JSON API; (8) docs
(README "Periodic jobs" section: definition, time zones and DST, catch-up, guarantees, deploys
and groups, dynamic schedules, dashboard and CLI, testing; DESIGN §9h (the cross-feature review
numbers §9g unique, §9h periodic, §9i batches, §9j rate limiting, §9k the features together) plus the
keys table and error contexts; STATUS entries; `examples/periodic.rs`).

Lanes (cross-feature review, `pro-features.md` §6): the spine and engine additions in the table above
(`keys.rs`, `error.rs`, `config.rs`, `job.rs`, `launcher.rs`, `processor.rs`, `cli.rs`, the dashboard's
router, navigation and `strings.rs`, the binary's command and route registration) are made once, as
hook points, in the common phase; lane A ("periodic and unique") then owns `periodic/`,
`api/periodic.rs`, the periodic dashboard pages, templates and strings file, and the `hench periodic`
command files, and does this design after the unique library (`FIRE` embeds its lock step).

---

## 3. Test plan

Conventions: integration tests use `TestRedis` (private `redis-server`), the manual clock for
anything involving hours or days, and 1-second timetables (`*/1 * * * * *`, `every(1 s)`) for
real-time behaviour. Every scenario names its origin.

### 3.1 Unit and property tests (`periodic/timing.rs`, `definition.rs`, `plan.rs`)

1. Parsing: 5 and 6 fields; each alias including `@midnight`; lists, ranges, steps; names in
   any case; weekday 7; `?`; `L`, `5#L`, `15W`, `5#2`, `+FRI`. Errors: 4 and 7 fields, `60 * * *
   *`, `0 25 * * *`, `*/0`, `0/10`, `@reboot`, `0 0 30 2 *` and `0 0 31 4 *` (never), unknown
   zone, `tz` on `every`, zero or sub-second interval, offset ≥ interval (Oban validation tests;
   probe, Appendix A).
2. Previous/next ticks (sidekiq-cron "cron test": previous minute, hour, day; Oban `last_at` and
   `next_at` tables, re-expressed).
3. DST, with the expected instants of Appendix A: London fall-back and spring-forward for fixed
   and wildcard expressions; New York both transitions and the hourly double; Lord Howe 30-minute
   shift; Apia whole-day skip via the retry; Kathmandu (+05:45) offsets; `latest_at_or_before`
   across the same transitions (Oban "calculating the last match within a repeated or skipped
   hour").
4. POSIX OR vs `+` AND (`0 0 13 * FRI`, `0 0 13 * +FRI`).
5. Property tests (proptest): random valid expressions and instants ⇒ `next_after(t) > t`;
   iteration strictly increasing; `latest_at_or_before(next_after(t)) == next_after(t)`; no tick
   in `(t, next_after(t))` (checked by brute force over seconds for small windows); `every`
   arithmetic equals brute force; `count` matches iteration.
6. Catch-up planning: the §2.2.5 examples for each policy, deadline boundaries (exactly at the
   deadline), `All { max }` larger and smaller than the backlog, caps (an every-second cron after
   a week of downtime completes quickly and reports 10 000+).
7. Definitions: identical definitions ⇒ identical digests across runs and processes (maps
   sorted); template, policy and description changes change the digest but not the timing key;
   timetable changes change both; `J::options()` applied under explicit options; `jid`/`at` in a
   template rejected; reserved custom keys dropped with a warning.
8. Names: length bounds, control characters, whitespace; key formats pinned (`periodic:` prefix
   length used by SYNC).

### 3.2 Integration tests (`crates/hench/tests/periodic.rs`, `periodic_api.rs`)

Registration (sidekiq-cron "load" and "save" scenarios, BullMQ upsert scenarios):

1. First start creates schedules with `next` strictly after now; nothing is enqueued at creation
   (sidekiq-cron "not enqueue any job - new jobs").
2. Re-registration of an identical crontab changes nothing: `next`, `paused`, history and
   counters intact ("status shouldn't be rewritten after save without status",
   "last_enqueue_time shouldn't be rewritten after save").
3. Template change keeps `next`; timetable change reschedules with the 60 s grace; a change
   exactly at a tick still enqueues that tick once ("doesnt skip enqueuing if job is resaved
   near next enqueue time").
4. Removed code schedules are deleted with their history; API schedules survive ("dynamic jobs
   are not cleared"); another group's schedules survive (namespace isolation tests).
5. Name conflicts (API vs code, group vs group) are reported and not overwritten.
6. Two processes with different crontabs register concurrently: the final state equals one of
   the two crontabs, never a mix (SYNC atomicity; BullMQ "avoid hazards when upserting
   concurrently").
7. `FLUSHDB` while workers run ⇒ schedules back within the check interval (Ent #3047).
8. API upsert results (`Created`, `Updated`, `Rescheduled`, `Unchanged`); quick successive
   upserts ⇒ one schedule (BullMQ "upserted in quick succession", "same id and different every
   ⇒ one scheduler"); remove then upsert works ("should allow adding a repeatable job after
   removing it"); removing a missing schedule returns `false` ("returns false").

Firing:

9. Two timetables (`*/2 * * * *`, `*/10 * * * *` with the manual clock) enqueue only on their
   own ticks, each once (sidekiq-cron poller tests "should enqueue only job with cron */2", "both
   jobs but only one time each").
10. Paused schedules never fire; resume does not backfill ("be always false when status is
    disabled").
11. Payload: class, args, queue and retry from the template and the job type, overrides win
    ("overwrite queue", "overwrite retry"); `periodic` metadata; `JobContext::periodic()` in the
    running job; `enqueued_at` set.
12. Client middleware: modifies the job; halts it (tick consumed, history `halted`); schedules
    it in the future (job in `schedule`, mode `scheduled`); fails or panics (error recorded,
    retried after the lease, reported once per episode).
13. Invalid template stored by hand ⇒ parked with `error`, other schedules keep firing.
14. History capped at `periodic_history` ("remove old enqueue times"); `periodic_history(0)`
    keeps none.
15. Enqueue now: job pushed with `manual: true`, `next` unchanged, history `manual` (sidekiq-cron
    2.3.1 #564).
16. Failing periodic job: retries carry the same tick; later ticks keep firing; RECORD_RUN shows
    `failed` then `ok`; an older tick's outcome does not overwrite a newer one; a deleted
    schedule is not recreated by RECORD_RUN (BullMQ "should continue repeating" when failing,
    "should not create a new delayed job if the failed job is retried").
17. Catch-up with the manual clock: Latest, Skip, All after 3 h; deadline drops; missed counters;
    warnings logged.
18. Monthly and rare timetables ("repeat 7th day every month at 9:25"; `0 0 L * *`;
    `0 9 * * 5#L`) fire on the right days with the manual clock across a year.
19. Quiet keeps firing, shutdown stops; nothing deleted at shutdown (Ent 0.7.7).
20. Performance: 10 000 schedules due at once are enqueued by one process in < 5 s, and by four
    processes without duplicates (sidekiq-cron performance test: 10 000 in < 35 s).
20b. With unique jobs (whichever lands second): a schedule with `unique_for` skips ticks while
    the previous job holds the lock (history `duplicate`, `skipped` counted, the lock untouched),
    fires again once the job succeeded; the lock is taken by FIRE itself (no lock without a job
    under `OOM` or a lost reply); a resent FIRE takes no second lock; `on_conflict` other than
    `drop` is refused by `validate` (unique design §2.18).

### 3.3 Concurrency and chaos

21. Exactly once under contention: 20 processes, 200 schedules `*/1 * * * * *` for 60 s;
    afterwards, for every (name, tick) exactly one job exists (count jobs by
    `periodic.name`/`periodic.tick`), and ticks are contiguous (no gap) per schedule.
22. Crash between LEASE and FIRE: a client middleware blocks on a `Gate` (tests/support), the
    test calls `Launcher::abandon()`; another process fires the tick once after the lease.
23. Lost FIRE reply: stall Redis with the `BUSY_LOOP` script used by `tests/chaos.rs` so that
    the poller's FIRE times out while Redis still executes it later; no duplicate; history shows
    the poller's own jid.
24. Network partition with the chaos `Proxy`: pollers behind the proxy lose Redis mid-fire;
    after healing, every tick once (Latest).
25. Redis restart without persistence (`TestRedis::restart`): code schedules re-registered,
    dynamic ones gone (documented); with persistence (`appendonly yes` via
    `start_with_args`): state intact.
26. `READONLY` failover simulation (existing failover tests' technique): pollers retry, no
    duplicates.
27. Clock skew: two processes with local clocks offset by ±30 s (manual clock offsets applied
    to everything except LEASE, which uses Redis `TIME`): no early enqueue, no duplicates (BullMQ
    "when clocks are slightly out of sync ⇒ only one job").
28. Out of memory: `maxmemory` tiny with `noeviction`; FIRE errors; state unchanged; after memory
    frees, the tick fires once (Appendix B reproduces the script part).
29. Mixed versions: a hash with `spec.v = 2` is skipped by a v1 process (skip list) without
    delaying a process that understands it beyond one poll.
30. Long middleware (> lease): two pollers prepare the same tick; one FIRE succeeds; the other
    returns 0.
31. Soak (optional, nightly): the existing soak harness (`ci/` soak and chaos runner) extended
    with periodic schedules and random kills; invariant: no (name, tick) twice, no gap beyond
    policy.

### 3.4 Dashboard, CLI, TOML

32. hench-web: list (empty, many pages, paused, parked, banners for no poller and digest
    mismatch), detail (definition, upcoming ticks in the zone, history), each action and bulk
    action, delete refused for code schedules unless orphaned, read-only (no forms, 403 on
    POST), CSRF, 404 for unknown names, names with `/`, `:`, spaces, Unicode, HTML special
    characters (escaping; sidekiq-cron #568), filters, Live switch; job pages show the Periodic
    row (sidekiq-cron web tests: display list, history, enqueue, enqueue all, enable/disable,
    destroy, redirect when not found).
33. hench-server: `periodic list/show/enqueue/pause/unpause/remove/add/next` outputs (text and
    JSON), control characters spelled out, `--force` rule, `next` offline preview with DST
    notes; JSON API endpoints and read-only refusal.
34. TOML: `[[periodic]]` parsing, environment section override, every error message (file,
    index, key).
35. README examples compile as doc tests (`ReadmeDoctests`).

### 3.5 Ported scenarios (source → hench test)

| Source | Scenario | Hench test |
|---|---|---|
| sidekiq-cron `job_test` "test of enqueue" | disabled, same tick once, next tick, past ticks, grace, resave, history trim | §3.2 items 1–3, 9, 10, 14, 17 |
| sidekiq-cron `job_test` "load" | create/update, duplicates, dynamic kept, errors, destroy removed | items 2, 4, 5 |
| sidekiq-cron `poller_test` | */2 vs */10, both once, namespaces | items 4, 9 |
| sidekiq-cron `web_extension_test` | list, history, enqueue, enable/disable, destroy | item 32 |
| sidekiq-cron `performance_test` | 10 000 jobs | item 20 |
| BullMQ `job_scheduler.test.ts` | quick upserts, same id/different every, clocks out of sync, continue after failure, retries do not add iterations, remove/re-add, monthly pattern, remove missing | items 8, 16, 18, 27 |
| BullMQ `job_scheduler_stress.test.ts` | many upserts keep guarantees; disconnection retries | items 6, 8, 24 |
| Oban `expression_test.exs` | parse properties, out of bounds, impossible days, next/last across DST | §3.1 items 1–3, 5 |
| Oban `cron_test.exs` | validation, per-entry zones, meta in jobs | §3.1 item 1, §3.2 item 11 |
| Sidekiq Ent changelog | #3047, #5049, #3974, 1.2.4, 0.7.7, #4271/#5832 | items 7, §3.1 item 3, 21, 9, 19, §3.1 item 1 |

---

## 4. Decisions for the owner, and risks

### 4.1 Decisions (recommendation first)

- **D1. Who fires: leaderless per-tick CAS** (recommended) vs leader election. Leader election
  needs a new subsystem, has failover gaps and flapping duplicates (L3, L25) and makes
  configuration-per-process mismatches fatal. The CAS costs one small script per process every
  few seconds and three round trips per tick.
- **D2. Default catch-up: `Latest`** (recommended; deviates from Sidekiq Enterprise's "no
  backfill"). Losing a daily job because its tick fell in a deploy window (L11) is the more
  common and more harmful surprise; `Skip` stays one call away.
- **D3. Clock authority: Redis `TIME`** (recommended) vs each process's clock. Removes skew
  issues entirely; needs `TIME` in scripts (Redis ≥ 5; to be checked on Dragonfly and Valkey,
  with the override as fallback).
- **D4. Cron library: `croner` 4 with the `jiff` backend, behind a private wrapper**
  (recommended). It is the only maintained crate with 5- and 6-field expressions, POSIX day
  rules, `L`/`W`/`#`, aliases and Vixie DST semantics. Costs: two proc-macro dependencies
  (`derive_builder`, `strum`), no declared MSRV, four majors in three years, and the Apia
  limitation (worked around). Alternative: an in-house parser and tick function (about 800 lines
  plus tests; Oban's and Sidekiq Enterprise's choice) — full control, no dependency, more code
  to own. Not recommended: `cron` (no 5-field expressions, double fixed-time ticks in folds),
  `cronexpr` (real-time iteration: fixed-time jobs twice in folds, never in gaps).
- **D5. Time zone data: `jiff`, system database first, bundled fallback** (recommended) vs
  `chrono-tz` (rebuild for every tz rule change).
- **D6. Cargo feature `periodic`, on by default** (recommended) vs always compiled.
- **D7. Code schedules: per-group registration at start, last registration wins, atomic prune**
  (recommended) vs (a) re-asserting crontabs on every heartbeat (union of live crontabs;
  flip-flops between versions every few seconds) or (b) never pruning (stale schedules fire
  forever, L12).
- **D8. Resolution: seconds allowed** (6-field cron, `every` ≥ 1 s) (recommended; Sidekiq
  Enterprise and Oban stop at one minute). Sub-second stays out of scope.
- **D9. Firing while quiet: yes, until the shutdown starts** (recommended, Sidekiq
  Enterprise's behaviour) vs stopping at quiet like hench's scheduled poller.
- **D10. Dynamic schedules: API and CLI yes; dashboard creation and editing no** in v1
  (recommended).
- **D11. Overlap prevention: delegated to unique jobs** (recommended) vs a built-in
  "skip while pending" option (needs a start hook and a safety TTL — the same machinery as
  unique jobs).
- **D12. Last-run tracking through a processor hook** (recommended; one script per periodic job
  execution, not awaited) vs enqueue history only.
- **D13. History length 10 by default** (recommended), configurable.
- **D14. Day rules: POSIX OR, `+` for AND** (recommended; Oban uses AND).
- **D15. Dialect limits**: reject the year field, `0/10`-style steps, `@reboot`, `@every`;
  accept `L`/`W`/`#`/`+` and `@midnight` (recommended).
- **D16. Names required** (1–200 bytes, no control characters) (recommended) vs defaulting the
  name to the job class.
- **D17. Deleting code schedules from the dashboard only for orphaned groups** (recommended);
  otherwise pause.
- **D18. Retry defaults for periodic jobs unchanged** (recommended, documented advice) vs
  forcing `retry: false`.
- **D19. No macro attribute in v1** (`#[hench::job(cron = …)]` registering through
  `inventory`) — it would register schedules in every binary that links the job, across groups
  (recommended: builder and TOML only; revisit later).
- **D20. No start/end dates, run limits or RRULE in v1** (recommended; cheap to add later to
  `spec` with a version bump).

### 4.2 Risks

- **R1. croner.** DST and gap edge cases in a young crate (the Apia failure shows one), API
  churn, no MSRV policy. Mitigations: private wrapper, exhaustive DST tests (§3.1), MSRV CI,
  pinned version, D4's in-house fallback.
- **R2. Differing tz databases across hosts.** Processes with different tzdata compute different
  ticks for a zone whose rules changed; the CAS still prevents duplicates, but the instant can
  differ by which process fires. Mitigations: system db re-read every 5 min (and the poller's
  parsed timetables after five minutes), the bundled copy for a name the system database lacks
  (round 3: `jiff` alone never falls back per name), a `next` another process computed read as
  the nearest tick within three hours (enqueued in its place, or not when that tick was handled:
  each period runs once; before round 3 it was dropped without a trace), docs (keep tzdata
  current; prefer UTC).
- **R3. `TIME` in scripts.** Compatibility with Dragonfly/Valkey and Redis's own clock
  (failover to a replica with a different clock, NTP steps). Mitigations: override argument, the
  monotonic guard, a start-up check that the lease script works (log and fall back to local
  time with a warning).
- **R4. Asynchronous replication** can re-fire a tick after failover (documented
  at-least-once in that case).
- **R5. Pile-up while workers lag** (ticks keep enqueuing). Mitigation: unique jobs (D11), docs.
- **R6. Group misconfiguration.** Two deployments sharing a group prune each other's schedules
  at every start. Mitigations: dashboard digest-mismatch banner, conflict reporting, docs; maybe
  a start-up warning when a live process of the same group declares another digest.
- **R7. Memory with very many dynamic schedules** (~3 KB each with default history).
  Mitigations: `periodic_history(0)`, docs pointing to the "one static schedule scanning the
  database" pattern for millions of schedules.
- **R8. Bursts at popular instants** (midnight across thousands of schedules and processes):
  bounded by O(log n) leases and FIRE scripts; jitter spreads wake-ups.
- **R9. Client middleware side effects** may run twice for one tick when preparation outlives
  the lease; FIRE still pushes once. Documented.
- **R10. SYNC size.** A crontab of thousands of entries makes one large script (milliseconds
  of Redis time at start); acceptable, documented limit (crontabs above 10 000 entries are
  refused at build).
- **R11. Coupling with the unique-jobs design.** FIRE and ENQUEUE_NOW must embed that design's
  lock step (shared `hu_*` helpers, extra keys, `duplicate` mode) and stay in sync with its
  `UNIQUE_PUSH` rules; its digest already excludes extras such as `periodic`. Whichever feature
  lands second adds the integration and the tests of §3.2 item 20b.
- **R12. Nobody polls** (every process configured with `periodic_poller(false)`, or no workers):
  ticks silently stop. Mitigations: dashboard banner, `hench stats periodic` "late" count, docs.
- **R13. Expectations on precision.** Ticks fire within jitter (≤ 250 ms) plus a round trip
  when idle pollers exist, up to `periodic_poll_interval` after a change made elsewhere, and up
  to a lease (60 s) after a crash. Documented with the guarantees.

---

## Appendix A. Probe of `croner` 4.0.2 + `jiff` 0.2.38 on Rust 1.88 (2026-11-01)

Throwaway crate at `/home/user/scratch/design-lab/periodic/cron-probe` (build output deleted):
`croner = { version = "=4.0.2", default-features = false, features = ["jiff"] }`, `jiff = { version
= "0.2", features = ["tzdb-bundle-always"] }`, parser `Seconds::Optional`. Built with `cargo
+1.88`. Dependency tree: `croner` → `derive_builder` (`darling`), `strum` (`strum_macros`),
`jiff` (`jiff-core`, `jiff-tzdb`). Results (local time, then UTC):

- `30 1 * * *` Europe/London: 2026-10-24 01:30 BST (00:30Z), **2026-10-25 01:30 BST (00:30Z)
  only**, 2026-10-26 01:30 GMT (01:30Z). Classified `FixedTime`; described "At 01:30.".
- `*/30 1 * * *` London 2026-10-25: 01:00 BST, 01:30 BST, 01:00 GMT, 01:30 GMT (both passes);
  `IntervalWildcard`.
- `30 1 * * *` London 2026-03-29: **02:00 BST (01:00Z)** (first instant after the gap).
- `*/20 1 * * *` London: no tick on 2026-03-29.
- `30 2 * * *` America/New_York 2026-03-08: **03:00 EDT (07:00Z)**; `30 1 * * *` 2026-11-01:
  01:30 EDT only; `0 * * * *` 2026-11-01: 01:00 EDT, 01:00 EST, 02:00 EST.
- `15 2 * * *` Australia/Lord_Howe 2026-10-04: 02:30 (+11:00).
- `0 12 * * *` Pacific/Apia from 2011-12-29 12:00 (−10:00): **error
  `TimeSearchLimitExceeded`** (the day 2011-12-30 does not exist; croner searches at most 2 h
  past a gap). Searching again from 2011-12-31 00:00 (+14:00) gives 2011-12-31 12:00: hence the
  hourly-retry wrapper (§2.2.2). `0 * * * *` across the same skip works.
- `*/15 * * * * *` (6 fields): every 15 s. `0 0 L * *`: Jan 31, Feb 28, Mar 31.
  `0 9 * * 5#L`: last Fridays. `0 0 13 * FRI`: "on day 13 or Friday" (OR). `0 0 13 * +FRI`:
  "on day 13 (if it is also Friday)": 2026-02-13, 2026-03-13. `@hourly` ok; `@reboot` refused.
- Previous occurrence (`find_previous_occurrence(…, inclusive)`) works across the London fold.
- Parse errors: `60 * * * *`, `0 25 * * *`, `0 0 * * 8` (out of bounds); `* * * *` and 8 fields
  (field count); `*/0` (zero step); `0/10` (strict step syntax). Accepted but never ticking:
  `0 0 30 2 *`, `0 0 31 4 *` → `TimeSearchLimitExceeded` after about 85–100 ms (debug build), so
  validation must compute a first tick. A 7-field expression is read as having a year; hench
  disables it with `Year::Disallowed`.
- Cost (debug build): 86 400 `next` calls on `* * * * * *` in 209 ms; 3 650 on `0 2 * * *`
  Europe/Paris in 103 ms; `0 0 29 2 *` next leap day in 80 µs.
- `jiff::tz::db()` used `/usr/share/zoneinfo`.
- For comparison (second throwaway crate, `cron = "=0.17.0"`, `cronexpr = "=1.7.0"`, Rust
  1.88): `cron` refuses `* * * * *` (needs the seconds field); for `30 1 * * *` in Europe/London
  both `cron` (`0 30 1 * * *`) and `cronexpr` return 2026-10-25 01:30 BST **and** 01:30 GMT (two
  runs in the fold) and nothing on 2026-03-29 (the gap day is skipped; next 2026-03-30 01:30).

## Appendix B. Script verification (redis-server 7.0.15, unix socket, scratch)

Scripts kept at `/home/user/scratch/design-lab/periodic/lua/*.lua` (the texts of §2.5); all nine
load (`SCRIPT LOAD`). Checked with `redis-cli --eval`:

- UPSERT: create (1), identical digest (0), expected-absent conflict (−1 with digest), source
  conflict (−2), template change keeps `next` (2), timetable change uses the second `next` (3).
- LEASE: nothing due returns the earliest score; a due member is leased (score + 60) and a
  second lease at the same time gets nothing; the skip list leases the next due member and
  reports the earliest non-skipped score; `TIME` path returns Redis time and moves the score.
- FIRE: fires once (1); a resend returns 0 and pushes nothing; digest mismatch −1; paused −2;
  missing hash −3 and the index entry is removed; the `last_tick` guard (a `next` forced below
  `last_tick`) returns 2, pushes nothing, keeps `last_tick`, writes no history and advances
  `next`; `halted` consumes the tick (`last_tick` set, `skipped` counted); a policy skip with no
  tick only counts `missed` and advances; `scheduled` mode adds to `schedule`; history trimmed
  to the cap; empty expected `next` repairs a lost `next`. **Out of memory**
  (`maxmemory` just above usage, `noeviction`): FIRE fails at its first write (`SADD`) and
  `next`, `last_tick`, `fired` and the queue are unchanged; with memory available it fires.
- SYNC: creates, unchanged on re-sync, CAS on group membership (−1), timetable change
  reschedules, prunes a removed schedule with its history, reports API and cross-group conflicts
  without touching them, prunes names containing `:`, removes a leaked member whose hash is gone,
  repairs a lost index entry (keeping a lease with `NX`) and a lost `next`, other groups
  untouched.
- PAUSE/UNPAUSE: 1/2/0 results, unpause refuses a stale digest (−1), index entries follow.
- ENQUEUE_NOW: pushes and records `manual_at`; 0 for a missing schedule.
- RECORD_RUN: records, ignores older ticks, counts failures, never recreates a deleted hash.
- REMOVE: refuses another source (−2), removes hash, history, indexes and group membership.
