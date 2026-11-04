# Hench — implementation status

Tracks progress against `docs/DESIGN.md`. Update in the same commit as the work.
Legend: `[ ]` todo · `[~]` in progress / partial · `[x]` done and tested.

**Release state: 0.1.0 (2026-11-03), published on crates.io and tagged `v0.1.0`.** The four
crates are at 0.1.0, with `CHANGELOG.md` (its 0.1.0 entry: features, known limitations,
requirements) and the roadmap (`docs/ROADMAP.md`); every gate passes (review log, release review round 4, and the
release gate on `main`; two timing-sensitive tests that failed once under load have been fixed
since). The repository is public.
Phase 3 of the pro features (below) is planned for 0.2: queue limits are done (on the
`queue-limits` branch, not released), the batch items and the lean lock mode are not started.

## Core (`crates/hench`)
- [x] keys, payload (+ jid, time helpers, backtrace codec), options, error/JobError
- [x] RedisPool (shared multiplexed + dedicated blocking connections, failover retry, detached pools)
- [x] Job trait, JobContext, data type-map, registry (inventory + manual), JobExt
- [x] Client: push, push_bulk, scheduling, builder/setter, validation, failover retry (Redis, fake and inline modes)
- [x] Middleware chains (client + server, `PerformEndpoint`, `execute_job` shared by workers and testing modes)
- [x] Config + HenchBuilder + capsules; `Hench::start`/`run`/`run_until`
- [x] test_util::TestRedis
- [x] Fetch: basic (BRPOP) + reliable (LMOVE) + orphan recovery (+ paused queues, weighted order)
- [x] Processor (+ panic catching, work state, stats counters, outcome writes retried through outages)
- [x] Retry / death handling / morgue
- [x] Scheduler poller + process cleanup
- [x] Launcher: heartbeat, stats flush, remote signals, lifecycle events, quiet/stop/hard shutdown
- [x] Job logger (tracing)
- [x] API: Stats, History, Queue (+ pause/unpause), JobRecord, Sorted sets, ProcessSet, WorkSet, paginator (`hench::api`)
- [x] Lanes integrated: one job execution path for workers and testing modes, one process cleanup for the scheduler and the API, the pause API feeding the fetchers, the API checked against a running engine (`tests/api_engine.rs`)
- [x] Testing modes (fake / inline, `Testing` builder: registry, data, middleware; jobs run through the workers' execution path and `job` span)
- [x] `#[hench::job]` macro (+ trybuild pass/fail/typeck UI tests, runtime tests)
- [x] Metrics (`hench::metrics`): per-class executions, failures, milliseconds and 26-bucket
  histograms tracked by the processor and flushed on every heartbeat (and at shutdown) into
  minute / ten-minute hashes and BITFIELD histograms; `Query` (top jobs with series, one job
  with per-minute histograms, periods 1h–72h, sorting), deploy marks (`Deploy`); `.metrics(bool)`
- [x] Iterable jobs (`hench::iterable`): `IterableJob` (blanket `Job`), `Enumerator` helpers
  (vec, iter, range, keyset pagination, batches, any stream), hooks, cursor state in `it-<jid>`,
  interruption on shutdown or `max_iteration_runtime` pushes the job back to the end of its
  queue (atomic with the acknowledgement under reliable fetch), failures retry from the saved
  cursor, `Client::cancel`, `IterationState` queries, testing modes (in-memory state)
- [x] Worker CLI + TOML configuration file (`hench::cli`, feature `cli`); `examples/worker.rs`
  uses it
- [x] systemd notifications (`hench::systemd`): READY, STATUS, STOPPING, watchdog keep-alives,
  abstract sockets
- [x] Per-job log level (`JobOptions::log_level`, macro `log_level`, `JOB_LOG_LEVEL_DIRECTIVES`)
- [x] `#[hench::job]` on `impl Job` / `impl IterableJob` blocks (automatic registration)

## Web (`crates/hench-web`)
- [x] Router (root or nested anywhere, mount prefix from `OriginalUri`), layout, embedded
  fingerprinted assets, CSRF (double-submit token + fetch metadata), security headers,
  one-shot notices, read-only mode, live refresh, light/dark theme with toggle
- [x] Host names checked by default (DNS rebinding): `AllowedHosts` (localhost, IP addresses
  and the names given to `HenchWeb::allowed_hosts`), `require_host` for routes next to it
- [x] Extensions: `HenchWeb::routes` (host routes behind the guard), `Page` (the layout),
  `Form` (CSRF-checked forms), `HenchWeb::job_info` (rows on the job pages)
- [x] Dashboard (totals, live SVG chart polling `/stats`, history 1w/1m/3m/6m with table view,
  Redis panel) + `/stats`, `/stats/queues` JSON, `HEAD /` heartbeat
- [x] Busy (status, processes in natural order, running jobs; quiet/stop per process and all)
- [x] Queues / queue (sort, pause/resume, delete queue, delete job)
- [x] Retries / retry, Scheduled / scheduled job, Morgue / dead job (+ bulk and all-jobs
  actions, detail pages)
- [x] Filtering (literal text with `*` as the only wildcard, the same for queues and sets,
  matched in the list's order over at most 50 000 jobs; summaries say what was searched)
- [x] `examples/embedded.rs` (dashboard next to workers; `--seed` fills Redis)
- [x] Metrics pages (`/metrics`: period selector, class filter, table sortable by executions,
  failures, total and average time, execution-time chart of the ticked classes with deploy
  marks; `/metrics/{class}`: figures with median and 95th percentile, executions per minute,
  execution-time distribution, heat map of execution times per minute, deploys; table views;
  in-place refresh of the last hour); `HenchWeb::metrics(bool)`, on by default
- [x] Iteration progress of iterable jobs on the busy page, the retries and the job pages
- [x] The crate documentation lists every route (the feature tabs' included, with their
  switches and actions); a test requests each listed route

## Server (`crates/hench-server`)
- [x] `hench web` (Basic auth, read-only, prefix, `/healthz`, graceful shutdown, request logs;
  refuses `--api` on a non-loopback bind without credentials; without credentials, answers
  only localhost, IP addresses and `--allowed-host` names (DNS rebinding); fails fast while
  Redis is down)
- [x] `hench web` connections (`connections.rs`): its own accept loop on hyper, at most
  `--max-connections` (512), deadlines for request heads and idle connections (10 s), bodies
  (30 s, 2 MiB) and stalled responses (30 s); the shutdown gives requests in flight 10 s, a
  second signal stops at once
- [x] JSON API (`--api`): stats, queues, processes (queue names and capsules by name), `POST
  /jobs`, and the features' routes (below)
- [x] CLI: `stats` (sidekiqmon report plus the busiest job classes of the last hour,
  `--json`), `processes`, `queues`, `quiet`, `stop`, `enqueue`, and the features' commands
  (below); the crate documentation's synopsis and `--help` name every command, subcommand,
  section and option (tests derive the list from clap's command tree)

## Pro features (`docs/design/pro-features.md`)

### Phase 0: shared infrastructure (integrator)
- [x] `canonical` (canonical JSON, SHA-256 base64url, golden vectors), `lua` (prelude, script
  builder, rules and `SCRIPT LOAD` tests, SHA-1 pins of the scripts of before, start-up
  `lua-replicate-commands` check), `pro` (`Fetched`, `Gate`, `Classified`, beat, background
  tasks, `info`, `validate`), `api::events` (per-minute counter reader)
- [x] Feature modules with complete `types.rs` and no-op hooks: `unique` (+ frozen
  `lua/lib.lua`, tested against Redis), `periodic` (+ `Periodic` builder), `batch` (+ `Batch`,
  `hench::WorkflowNext` stub), `limit` (+ spec grammar, `Limiter`, `Throttle`, `OverLimit`)
- [x] Spine: keys (every helper of §2.1), payload fields (lenient), options and setters with
  field-wise merges, `Job::{unique_key, throttle_key, throttle}`, push outcomes, errors and
  job-error classes, config settings with bounds, client normalization hooks and per-job push
  results
- [x] Engine: the processor pipeline of §3.2 with every hook, `*_with` outcome writes,
  endpoint hooks, feature state of running jobs, heartbeat `features`/`throttles`, background
  tasks, manager permit release, metrics `o`/`t`/`s`, job logger events, testing tables and
  hooks, API removal hooks
- [x] Surfaces: worker TOML keys and reserved tables (refused until supported), `TestRedis`
  feature intervals and `keys_without_ttl`, macro options parsed then refused (+ spec grammar
  copy and parity test), dashboard switches/tabs (hidden)/rows/notices/assets/"lacks" chip,
  server commands, stats sections, JSON routes (`501`) and `enqueue`/`POST /jobs` fields
- [x] Gate: the existing suite passes unchanged; `tests/featureless.rs` pins the Redis commands
  of jobs without features byte for byte

### Unique jobs (lane A, §9g)
- [x] normalization and digest (golden vectors, canonical JSON proptest), `UNIQUE_PUSH` in
  chunks of 100 (replace, resends, OOM, wrong-type keys, index trim, counters), delivery,
  `push_outcome` (`tests/unique.rs`)
- [x] engine scripts and hooks: `ACK_UNIQUE`, `SETTLE_UNIQUE`, `PUSH_BACK_UNIQUE`,
  `UNIQUE_LOCK_OP` (basic fetch: hold before the write, release after), `UNIQUE_RELEASE` at
  the start of `start` jobs, `UNIQUE_RENEW` on every beat; API removal scripts
  (`REMOVE_UNIQUE`, `KILL_UNIQUE`, `UNIQUE_RELEASE_MANY`, `UNIQUE_TIDY`); every script tested
  against Redis (`unique::lua` tests), real workers, crashes, hard shutdowns, restarts and
  concurrent pushers (`tests/unique_engine.rs`)
- [x] `UniqueTable` of the testing clients (`tests/unique_testing.rs`; a model-based test runs
  random push/hold/release/expiry sequences against the scripts, the table and a pure model),
  `UniqueLocks` (list, find, release, trim, counters; `tests/unique_api.rs`), the API's deletes
  and kills releasing locks, `JobContext::{unique, release_unique_lock}`
- [x] macro options `unique_for`, `unique_until`, `on_conflict`, `unique_across_queues`,
  `unique_by`: parsed, checked and generated by `hench-macros/src/unique.rs` into the job's
  `options()` and `unique_key` (unit tests, `tests/ui/fail/unique_options.rs`,
  `tests/ui/pass/feature_options.rs`)
- [x] Locks pages (`pages/locks.rs`, `templates/lock{,s}.html`, `strings/locks.rs`,
  `assets/locks.css`: list with summary, last hour's counters, filter, pages, bulk and
  all releases naming the holders shown, lock page locating the holder, Unique row on job
  pages; W1–W3 tests in the module) and `hench locks [list|show|release|release-all]`,
  `hench stats locks` (S1 tests in the module)
- [x] JSON API `GET /locks`, `GET /locks/{digest}`, `DELETE /locks/{digest}`, and `POST /jobs`
  with uniqueness: 201/200/409/400/422 (`hench-server/tests/locks.rs`)
- [x] docs: DESIGN §9g, README "Unique jobs"

### Periodic jobs (lane A, §9h)
- [x] timing (`croner` 4 and `jiff` behind a private wrapper: 5 and 6 fields, aliases, `L`, `W`,
  `#`, `+`, Vixie DST rules, searches past whole skipped days, backward searches checked against
  the forward sequence), definitions and digests (canonical JSON, versioned `spec`, template
  rules, typed unique and throttle keys stored), catch-up planning (`Latest`, `Skip`, `All` with
  deadlines, capped missed counts, constant-time backlogs); `Periodic` setters, `validate`,
  `upcoming`, `describe`, `payload`, `enqueue`; build-time checks (unit and property tests in
  `periodic/{timing,plan,definition,builder}.rs`)
- [x] scripts (`PERIODIC_LEASE`, `FIRE` with the unique lock step, `SYNC`, `UPSERT`, `REMOVE`,
  `PAUSE`, `UNPAUSE`, `ENQUEUE_NOW`, `RECORD_RUN`, `ERROR`), each tested on a real
  `redis-server` (resends, compare-and-sets, the monotonic guard, `OOM`, wrong-type locks,
  duplicates, concurrent fires: `periodic/lua/redis_tests.rs`)
- [x] poller (leaderless leases, catch-up, client middleware, lost-reply disambiguation, outage
  reporting, skip list for newer definitions, parse cache), registration per group with pruning
  and conflicts, data-loss check, `ManualClock` for tests, `JobContext::periodic`, heartbeat
  `info` (`periodic::info`, wired by the integrator) (`tests/periodic.rs`: real workers,
  catch-up, DST-free rare timetables across a year, quiet/shutdown, 2 000 schedules by four
  processes, exactly once under contention, crash between lease and fire, lost `FIRE` reply,
  restart without persistence, `OOM`, mixed versions, slow middleware)
- [x] `PeriodicSet` (`api/periodic.rs`: list, page, get, upsert, remove, pause, unpause,
  enqueue now, groups, repair, history, `summary`), `JobRecord::periodic`, `RECORD_RUN` after
  outcomes (`tests/periodic_api.rs`)
- [x] TOML `[[periodic]]`: parsed and checked (`periodic::toml::parse_schedules`) and applied
  after the builder's own schedules
- [x] Periodic pages (list with banners for no poller, overdue schedules and groups whose
  processes disagree; filters; pages; the schedule's page with next ticks in its zone, DST notes,
  state and history; enqueue now, pause, resume and delete, one at a time and by selection, code
  schedules only when orphaned; read-only; the Periodic row of job pages): tested in
  `pages/periodic.rs`
- [x] `hench periodic` (`list`, `show`, `enqueue`, `pause`, `unpause`, `remove [--force]`, `add`,
  `repair`, `next` without Redis) and `hench stats periodic` (tested in `commands/periodic.rs`);
  JSON API `GET /periodic`, `GET /periodic/{name}`, `POST
  /periodic/{name}/{enqueue|pause|unpause}` (`hench-server/tests/periodic.rs`)
- [x] docs: DESIGN §9h, README "Periodic jobs"; `examples/periodic.rs`

### Rate limiting (lane B, §9j)
- [x] limiters (`Limiter`: concurrent, window, bucket, leaky, points, unlimited, `all`), permits
  (leases renewed by a task, released on drop, `lost()`, `points_used`), waits (slot sleeps,
  concurrency polling with local wake-ups, wait line with tickets), the scripts `LIMIT_ACQUIRE`,
  `LIMIT_RELEASE`, `LIMIT_RENEW`, `LIMIT_ADJUST`, `LIMIT_USAGE`, `LIMIT_RESET` (tested against
  `redis-server` with fixed times: the lab's scenarios, TTLs, `OOM`, `TIME`, parity with the
  in-memory table), over-limit reschedules (budget, backoff, jitter, `over_limit_when`),
  iterable integration (a refused item stops like an interruption), in-memory backend
  (`limit::testing::memory_target`)
- [x] throttles: registry and checks, the gate (wait line, `max_wait`, `fail_open`, budgets),
  engine-held permits (renewed by the heartbeat, released after every outcome and after a hard
  shutdown), `features.limits = 1`, TOML `[throttles.*]` (`limit::toml::parse_throttles`) and
  the macro options (`ThrottleAttrs`: parsing, checks, generated `throttle_key`/`throttle()`),
  `Client::limited_jobs`; the worker's file applies its throttles after the builder's, the macro
  generates the options and inline definitions, and `HenchBuilder::build` refuses jobs naming
  an undefined throttle
- [x] `Limits` API (`api/limits.rs`: pages, filters, entries, holders, reset, clear line,
  per-minute events, waiting jobs; `LIMIT_TRIM`), the dashboard's Limits tab (list, identity
  page, reset, bulk reset, clear line, declared throttles, job rows), `hench limits` and
  `hench stats limits`, the JSON API's `/limits` routes, README section and DESIGN §9j; tests
  `tests/limits_api.rs`, `hench-web/tests/limits.rs`, `hench-server/tests/limits.rs`
- [x] the test plan's remaining items: the in-memory algorithms against brute-force models
  (proptest), the testing gate's hooks on tokio's clock, chaos (`tests/limit_chaos.rs`: lost
  replies, a failover; `tests/throttle_chaos.rs`: four processes sharing a throttle and a
  limiter, renewals stalled past the lease, a restart without persistence, Redis unreachable
  at the gate, fail-open, a reschedule written during a partition), the macro's diagnostics
  (`tests/ui/fail/throttle_options.rs`), inline refusals, the signature-mismatch warning
- [x] wiring of DESIGN §9j item 7 (phase 2): validation at build, the file's throttles, the
  macro expansion, the testing executor (`TestLimits::InMemory` parks refused jobs), the
  `hench::api` re-exports, list chips, the metrics columns; parked jobs wake their process's
  scheduler at their slot (§9j item 8, `scheduled::Wakeups`)
- [x] queue limits (phase 3, DESIGN §9j "Queue limits"): `LIMIT_FETCH_GATE`
  (`limit/lua/fetch_gate.lua`), the `queue-limits` hash and the process's cache with its
  cooldowns, single probes and watches (`limit/queue.rs`), the blocking step's watch of a
  limited queue (`BLMOVE q q RIGHT RIGHT`), permits held per processor slot,
  `Queue::{limits, set_limits, set_limits_from_str, clear_limits, all_limits}`,
  `HenchBuilder::queue_limits`, `[queue_limits]`, `hench queues limit`, `PUT/DELETE
  /queues/{name}/limits`, the limits of `GET /queues` and `hench queues`, the dashboard's chips
  and queue form, `Fetcher::release_limits`; tests: the gate against `redis-server`
  (`limit/script_tests.rs` `fetch_gate`: plain queues, stale views, concurrency, rate waits,
  every kind, empty and paused queues, ended leases), real workers (`tests/queue_limits.rs`:
  FIFO under a concurrency limit with both fetch strategies, a window's pace, run-time changes,
  a job arriving on a full queue, a burst on a full queue with 20 waiting processors (both
  strategies), refusals bounded with 20 idle processors, one poll per cooldown, script calls
  per job independent of idle workers, a plain capsule beside a cooling one, capsules sharing a
  limited queue (bursts and a rate limit), watched and plain queues together, pause, renewals and releases, configuration at start, refusals,
  broken stored limits, a standalone fetcher, a hard shutdown, three processes sharing a
  limit), the file (`limit/toml.rs`, `cli.rs`), the command and JSON API
  (`hench-server/tests/queue_limits.rs`), the dashboard (`hench-web/tests/actions.rs`)

### Batches (lane B, §9i)
- [x] scripts (`BATCH_COMMIT` with its unique section on the frozen library, `BATCH_SUCCEEDED`,
  `BATCH_FAILED`, `BATCH_SKIPPED`, `BATCH_DISPATCH`, `BATCH_TOUCH`, `BATCH_CANCEL`,
  `BATCH_DELETE`), each tested on a real `redis-server` (`batch/script_tests.rs`: the design
  lab's scenarios S1–S18, `OOM`, wrong types, the expiry of every key, the unique section,
  the minute counters); the pure model (`batch/model.rs`) and the parity proptest model ↔
  scripts (`batch/parity_tests.rs`, 256 cases, with a mutation check)
- [x] collecting clients, `Batch` (new, `open`, `with_bid`, callbacks, children), the commit
  (resent until a definite reply, `Unconfirmed`), accounting before the outcome write, the
  endpoint check, dispatch, maintenance, `BatchTable` (fake and inline clients), workflows and
  `hench::WorkflowNext`, the `Batches` API, `JobContext::{batch, batch_callback, batch_status,
  is_batch_cancelled, check_batch}`; tests: `tests/batch.rs` (definitions and commits, D1–D14,
  callback templates; D14 runs once unique jobs are merged), `tests/batch_engine.rs` (real
  workers, E1–E21: events, retries, deaths and revivals, the outcomes that do and do not count,
  shutdowns, cancellation, loaders, nesting, hand-written and `Workflow` workflows, callback
  queues, maintenance after a crash, API removals, paused queues), `tests/batch_chaos.rs`
  (C1–C9: 4 processes × 10 000 members, crashes, stale copies, lost accounting and dispatch
  replies, restarts with and without persistence, `OOM`, reopen/cancel/dispatch races),
  `tests/batch_api.rs` (A1–A3), `tests/batch_testing.rs` (T1–T3, workflows in testing modes);
  the parity proptest also checks the model's invariants (U6)
- [x] the dashboard's Batches tab (`/batches` by state and text with labelled progress bars,
  counts, chips and Live refresh; `/batches/{bid}` with facts, callbacks, failures linked to
  their retry or morgue entries, children, pending jobs; Cancel, Delete, Skip, Push callbacks
  now; Batch rows on job pages), `hench batches` (`list`, `show`, `cancel --yes`, `delete
  --yes [--force]`, `skip`, `dispatch`) and `hench stats batches`, the JSON API's `/batches`
  routes; tests `hench-web/tests/batches.rs`, `hench-server/tests/batches.rs`; README section
  and DESIGN §9i
- [x] wiring of DESIGN §9i item 8 (phase 2): the client's batch settings from the
  configuration, validation at build, the testing executor's skips, the `hench::api`
  re-exports, the error of `retry: false` summaries, job page links and confirmations, §2's
  `:commits` row; C10 is the soak scenario `pro`, W5 the browser test of the feature tabs
- [ ] phase 3, planned for 0.2: staged commits above `batch_max_commit_jobs` (loader jobs
  until then), the iterable checkpoint's cancellation check, a process-wide cache of cancelled
  batches, batch tags (Redis 7.4+)

### Phase 2: integration
- [x] both lanes merged; every wiring item of the lanes' reports done (macro options, the
  file's throttles and schedules, the client's unique switch and batch settings, validation at
  build, the heartbeat's `periodic` object, the testing executor's parking and skips, API
  re-exports, `hench enqueue` duplicates, `hench periodic next` offline, list chips, metrics
  columns, batch rows and confirmations); the phase-0 `AVAILABLE` scaffolding removed
- [x] one heartbeat pipeline for `UNIQUE_RENEW` and `LIMIT_RENEW`
- [x] cross-feature tests: `tests/pro_features.rs` (X2-X16, X18, X24, the pipeline order and
  the renewals, the key sweep X23 after each), `tests/periodic.rs` and
  `periodic/lua/redis_tests.rs` (X1), `hench-web/tests/features.rs` (X16 columns, X17, X19),
  `hench-server/tests/features.rs` (X20) and `tests/http.rs` (the `features` of `GET
  /processes`)
- [x] soak scenario `pro` (X21: `examples/soak/{pro,check_pro}.rs`, `ci/soak.sh pro`; in
  `--short` too): its short run at the integrated tree held every invariant (37 batches, 456
  members, 24 ticks, kills, Redis restarts and proxy faults; DESIGN §9k); its first run found
  the slow return of parked jobs, fixed by the scheduler's wake-ups (§9j item 8)
- [x] performance gate X22 (`ci/throughput.sh --base f3da43d`, `examples/throughput.rs`):
  before phase 0 (`f3da43d`) a median of 46,157 no-op jobs/s, the integrated tree 46,212
  (+0.1 %; 15 alternating runs of 100,000 jobs each, standard deviations about 1,700): no
  regression
- [x] the dashboard's four feature tabs and a job carrying every feature in a real browser, in
  both themes and at phone width (`hench-web/tests/browser.rs`, `tests/browser/features.js`)
- [x] DESIGN §9k (the features together, the decisions of phase 2), §10's rows, README "Using
  the features together" (an example using every feature, recipes, the rollout rule, sizing)

### Phase 3 (planned for 0.2; `docs/design/pro-features.md` §6.1)
The owner deferred it to 0.2 when releasing 0.1.0; the roadmap and `CHANGELOG.md`'s
`[Unreleased]` section list it for users, each item with what happens until then.
- [x] queue limits: `LIMIT_FETCH_GATE` and its cache (`docs/design/ratelimit.md` §2.4.5, §2.7,
  §2.9.7, as implemented in DESIGN §9j), with their API, builder, file, command, JSON and
  dashboard surfaces (see Rate limiting above)
- [ ] staged batch commits above `batch_max_commit_jobs` (`docs/design/batches.md` §2.20)
- [ ] the iterable checkpoint's cancellation check and the process-wide cache of cancelled
  batches (`docs/design/batches.md` §2.6.5, D12); batch tags on Redis 7.4+ (D15)
- [ ] the unique `lean` lock mode, if asked (`docs/design/unique.md` U7)

## Quality
- [~] CI: `.github/workflows/ci.yml` (fmt, clippy `-D warnings`, rustdoc `-D warnings`, `cargo check` on Rust 1.88) and `coverage.yml` (the whole suite, every feature, with redis-server, under `cargo llvm-cov`, uploaded to Codecov with an OIDC login, no stored token; it serves the README's coverage badge). Both run on every push to `main` and every pull request while the repository is public, where Actions minutes are free and unlimited (on a private repository the jobs skip themselves, so they use none of the account's minutes), and by hand (`workflow_dispatch`). Actions pinned by commit SHA. Not yet seen running on GitHub. Locally: fmt, clippy, the suite, rustdoc `-D warnings`, `cargo +1.88 check --all-targets`, `ci/check-readme-examples.sh`, `cargo deny --all-features check`, `ci/check-package.sh` (CI's `package` job, with `-D warnings`), `cargo publish --dry-run --workspace`, the docs.rs-style nightly build (`--cfg docsrs`, `-D warnings`) and `ci/soak.sh --short` pass
- [~] Ported Sidekiq test scenarios (see DESIGN §10): client, job_util, job (`set`, `perform_in`, `perform_bulk`, `perform_inline`, macro jobs), middleware, sharding, fetch, processor, job_logger, exception_handler, retry, retry_exhausted, scheduled, scheduling, manager, launcher, capsule (except "customized middleware chains": chains are per process, DESIGN §5), actors, testing, testing_fake, testing_inline, api, api_utils, dead_set, filtering, paginator, metrics, deploy, iterable (+ iterable_job_query), cli, systemd, web (including its Metrics and iteration scenarios), web_helpers, web_router, web_action_flash, webext, sidekiqmon done (`web_test`'s "displays custom job info" included); not ported: `web_test`'s locale scenarios (see the gaps)
- [x] Chaos / end-to-end / property tests: payload/args/backtrace, retry-delay-bound and weighted-order proptests; Redis restart/outage/read-only replica tests; e2e (5 000 jobs × both fetch strategies, several processes, retries); chaos (crashed process recovery, crash mid-stream, Redis restart and outage mid-run, shutdown with Redis down, network partition, out of memory); cross-lane (the API against live workers, macro jobs on real workers); the multi-process soak and chaos harness (`examples/soak`, `ci/soak.sh`, DESIGN §10)
- [x] Portability: the whole suite passes on macOS against Valkey 9.0.1 (which Homebrew installs
  as `redis-server`) with Homebrew's Rust. Fixed for it: the lost-reply tests (`chaos`,
  `periodic`, `scheduled`) stalled Redis with a script looping on `TIME`, which Valkey keeps
  still inside a script, so the stall lasted until the 5 s busy threshold and other clients got
  `BUSY` replies: their shared `BUSY_LOOP` (`tests/support`) also ends on the server's CPU time
  (`os.clock`, Redis 7 and Valkey), and they raise the busy threshold (`lua-time-limit`), so
  that a stall a loaded machine slows down still holds the commands sent meanwhile. The periodic
  one stalls its second tick: its first loads `FIRE`'s script, without which the late `FIRE`
  failed (`NOSCRIPT`) and the test passed only through the poller's next attempt, when that one
  came after the stall (now it checks the late `FIRE` itself). The dashboard tests expect the
  product the server announces (Redis, Valkey or Dragonfly) rather than Redis. Stored backtraces
  leave out the standard library's frames whatever the toolchain's source layout (its
  `library/<crate>/src/` paths, not only rustup's `/rustc/`): on Homebrew's Rust an inlined
  `Pin<Box<dyn Future>>::poll` frame stayed in them; the backtrace tests know a job's frames by
  their locations, since names depend on the platform's symbolization (`{async_fn#0}` on macOS).
- [x] README, guide, examples, rustdoc: the top-level `README.md` is an overview (the
  dashboard's screenshot in light and dark, highlights, a quick start, the dashboard, the
  benchmark results against Sidekiq, a feature table, links to the documentation, the crates,
  MSRV, license and credits); the guide, `docs/guide`, holds what the README used to (a page
  per topic: getting started, jobs, configuration, retries and delivery guarantees, middleware,
  testing, unique jobs, periodic jobs, batches and workflows, rate limiting, iterable jobs,
  metrics, the features together, the dashboard, the `hench` command, deployment, differences
  from Sidekiq) and the roadmap moved to `docs/ROADMAP.md`. The 23 Rust examples (2 in the
  README, 21 in the guide) are doc tests of `hench-server` (`ReadmeDoctests` and
  `guide_doctests`: all compile, those that need no Redis run), and
  `ci/check-readme-examples.sh` builds each document's examples against the dependencies it
  lists; `tests/readme.rs` checks that every guide page with an example is a doc test and that
  the documents' links resolve (the README's are absolute, since crates.io shows it). `hench`,
  `hench-web` and `hench-server` package the README as their readme, and its links into the
  repository, its images included, like the rustdoc's links to the design documents, are pinned
  to the release tag. Examples `axum_embedded`, `worker` (on `hench::cli`) and `hench-web`'s
  `embedded` run against a real Redis; rustdoc complete (`-D warnings`)
- [x] Benchmarks (`benchmarks/`, a workspace crate that is not published): `henchload`,
  `multi_queue_bench` and `webload` port Sidekiq 8.1's `bin/sidekiqload`, `bin/multi_queue_bench`
  and `bin/webload` (their parameters, jobs, arguments, clocks and output lines);
  `benchmarks/run.sh` runs them and Sidekiq 8.1.7's own scripts (from the gem, pinned by
  `benchmarks/sidekiq/Gemfile.lock`) on one machine, each run against a fresh private
  `redis-server`, rounds alternating, and records the environment, every run's output, a CSV
  and the medians under `benchmarks/results/`; `benchmarks/README.md` has the results next to
  the figures Sidekiq published. On an EC2 c7i.4xlarge (medians of five runs, the CPU idle
  state C6 disabled, which `setup-ubuntu.sh` now does and `run.sh` records; Hench with `TCP_NODELAY`,
  its dashboard from the session before), Hench runs `bin/sidekiqload` 2.79 times as fast as Sidekiq 8.1.7
  with reliable fetch and 4.82 times with basic fetch; with 1 ms of latency 0.52 and 0.98 times at
  30 threads (two Redis round trips per job with reliable fetch, one with basic fetch as with
  Sidekiq) and 1.38 and 2.36 times at 100; `bin/multi_queue_bench`, which Redis bounds, 0.77 and
  1.29 times; the dashboard 1.69 (`/busy`) and 1.97 (`/queues`) times. Turning `TCP_NODELAY` on
  made the latency runs 1.33 to 1.73 times as fast (before it, Nagle's algorithm
  on the shared connection held commands back: 0.30 and 0.59 times Sidekiq at 30 threads). A
  first session with C6 enabled, which slowed both systems down by up to six times, is kept.
- [x] Release: `docs/RELEASING.md` (repository settings, version bumps, the gate, the tag, the
  publishing order, compatibility promises); `CHANGELOG.md` (Keep a Changelog: 0.1.0's
  features, known limitations and requirements; `[Unreleased]` names what 0.2 plans); the
  roadmap and installation from crates.io; `ci/check-package.sh` (CI job `package`)
  packages the crates, builds every target of the packaged `hench`, `hench-web` and
  `hench-server` from their `.crate` files and runs the doc tests of the README and the guide
  from the packaged `hench-server` (`crates/hench-server/README.md` and `guide` link to the
  repository's README and `docs/guide`)
- [x] Review rounds (log below): spine 4, engine 5, api lane 7, integration 6, features lane 1, web lane 3, surface fixes 2, engine fixes 2, whole-tree review rounds 4 (the third on the pro features, fixed by four lanes; the fourth, the release review, by two)

## Remaining gaps (versus DESIGN.md and Sidekiq's test suite)
- Every section of DESIGN.md is implemented; what follows is missing versus Sidekiq or limited
  on purpose. Phase 3 of the pro features is planned for 0.2; the roadmap (`docs/ROADMAP.md`)
  lists it with a few of the gaps below ("later") for users.
- Dashboard: English only (no locale switching: `web_test`'s "can show text with any
  locales", "can change the locale" and "user defined tab with custom locales" are not
  ported); extensions bring no locales, views or assets of their own (their pages are HTML
  strings rendered in the layout, their assets routes of their own); no profiles page
  (Sidekiq 8's Vernier profiling).
- Rate limiting: a job a limiter refuses is rescheduled at least 1 s later (`MIN_DELAY`, since a
  reschedule runs the job again from its start), even when it kept its place in the wait line
  and its unit frees milliseconds later. So when one job takes its unit a little late, the next
  job, back for its own slot, waits a further second, and with a slow scheduler a later job can
  take the free unit first: with `max_wait(ZERO)`, jobs in a line are not strictly served in
  order (none is lost, and the limit is never exceeded). A shorter minimum for a job that kept
  its place, as throttles have (50 ms), would tighten this; it is a change of semantics, not
  made yet. Found through `tests/limit.rs`'s
  `a_rescheduled_job_moved_back_early_runs_at_its_place`, which failed once under load during
  the 0.1.0 release gate and now checks only its subject (B at its place).
- The process title (proctitle) does not exist. `TTIN` (signal or `api::Process::dump`) logs
  each busy slot's job, not the tasks' stacks (tokio's task dumps need `tokio_unstable`), and
  the `hench` command has no `dump` subcommand yet (hench-server's).
- Redis: with latency between a process and Redis, one shared multiplexed connection
  (`RedisConfig::shared_connections`, 1 by default) also holds a process with many processors
  back: at 100 threads and 1 ms, `bin/sidekiqload` with basic fetch ran 47,381 jobs/s (median)
  with one and 61,439 (a single run) with 64 shared connections (`benchmarks/README.md`, "Why
  Hench falls behind with latency"). The default stays 1; a process with many processors and a distant
  Redis can raise it.
- Redis: TLS through Sentinel (`rediss+sentinel://`) is refused; the `hench` command has no
  options for a private CA or a client certificate (`rediss://...#insecure` works for
  self-signed servers); `hench::cli`'s worker options have no TLS settings either.
- Dashboard: a queue named `.` or `..` (queue names are any text, as in Sidekiq) cannot be
  opened from the dashboard: its URL is a dot segment that browsers resolve away (schedules and
  limiters refuse such names since round 3; refusing them for queues would break pushes that
  Sidekiq accepts).
- Sidekiq Pro/Enterprise features: queue pausing, unique jobs, periodic jobs, batches and rate
  limiting are implemented (`docs/design/pro-features.md`); encrypted arguments (only their
  display is handled) and leader election are out of scope (periodic jobs need no leader).
  What the four feature documents defer (phase 3 and "not in v1"), queue limits aside (done
  for 0.2): staged batch commits above `batch_max_commit_jobs` (loader
  jobs meanwhile), the iterable checkpoint's cancellation check (an iterable member of a
  cancelled batch is skipped at its next execution), a process-wide cache of cancelled
  batches and batch tags (batches.md §2.20, D12, D15); the unique `lean` lock mode (unique.md
  U7), locks taken back by crash, shutdown and orphan requeues or by a revival from the morgue
  (U13, U14); creating and editing schedules in the dashboard, a `cron` macro attribute,
  start and end dates, run limits and RRULE, sub-second schedules (periodic.md D10, D19, D20);
  per-key round-robin sub-queues (ratelimit.md D2). Not exercised on Dragonfly; the suite passes
  on Valkey 9 (run by hand, see Quality).
- Metrics pages: metrics written by Sidekiq itself (two-digit-year keys) are not read; the
  current minute is partial until the next heartbeats flush; at 8 hours the heat map's minute
  columns are a few pixels wide on narrow screens (the table view has every minute); only
  the 1-hour views refresh themselves; `hench web` has no switch to hide the Metrics tab
  (embedders use `HenchWeb::metrics(false)`).
- Features lane limitations (DESIGN §9b–§9f): no `around_iteration` hook; a cancellation is
  seen at the next checkpoint (5 s by default, `checkpoint_interval`), not instantly; iterable
  items run at least once (a crash or hard shutdown resumes from the last checkpoint); metrics
  hash keys use a four-digit year (Sidekiq's two-digit keys are not read), executions count in
  the minute of the flush after them, and counts Redis has not confirmed for 12 hours (an
  outage that long) are given up rather than risk counting them twice; a per-job level above
  the process's only silences hench's own job events (`tracing` filters cannot remove what
  other directives enable); the configuration file has no templating (Sidekiq's ERB) and no
  `-r`.
- Web lane limitations (DESIGN §9): no `X-Forwarded-Prefix` support (a proxy that strips the
  path prefix must forward the full path and use `hench web --prefix`); axum's `nest` neither
  routes `<mount>/` nor accepts a mount whose first segment is a parameter (the docs mount with
  `nest_service`); a filter searches at most 50 000 jobs of a list, in its order (the rest is
  reported as not searched), and a queue row's Delete looks for its job at most 10 000 jobs
  past where the page showed it; `hench web` has Basic authentication only (no sessions or
  logout), no TLS termination and no rate limiting (per client: its connection limit and
  timeouts bound what slow or idle clients hold, but a client opening connections as fast as
  they close competes with everyone for the 512; expose it behind a proxy that limits rates);
  live refresh re-fetches the page's HTML; job JSON that is not valid UTF-8 cannot be shown
  (the API reads values as strings); `hench::Client` still accepts control characters in queue
  and class names (the dashboard and the `hench` reports cope with them; the JSON API and
  `hench enqueue` refuse them; times beyond the year 9999 are refused since round 3); the
  browser tests (`crates/hench-web/tests/browser.rs`) need Node with Playwright and are skipped
  where it is missing (CI installs neither).
- Partly covered by spine unit tests rather than ported one by one: `config_test`,
  `sidekiq_test` (error handlers that panic, lifecycle events, default job options, failover
  reconnects), `redis_connection_test` (URL/provider resolution, credential redaction; pool
  sizing does not apply to a multiplexed connection), `logger_test` (`tracing` replaces
  Sidekiq's logger).
- Not applicable (Ruby/Rails specific): `active_job_adapter_test`, `current_attributes_test`,
  `rails_test`, `job_generator_test`, `loader_test`, `profiling_test`,
  `transaction_aware_client_test`, `ring_buffer_test`, Ractor and edition checks of
  `sidekiq_test`, `iterable/active_record_enumerator_test` and `iterable/csv_enumerator_test`
  (ActiveRecord and CSV enumerators; hench has keyset pagination, batches, ranges, vectors and
  any stream), `web_test`'s Pro-disabled pause scenarios (pausing is built in); `tui_test` (no
  TUI planned).
- Known limitations kept on purpose (DESIGN §5, §7, §8): a fetched job whose reply is lost
  waits in its live process's in-progress list until the process's own reconciliation pushes
  it back (within `fetch_timeout + response timeout + 1 s` plus a check interval of at most
  30 s); basic fetch can lose a job whose reply is lost; a due job taken by a scheduler that
  crashes, or whose reply is lost, is due again when its one-minute lease ends; orphans of a
  crashed process wait for its heartbeat to expire (60 s), the next orphan check (up to
  `orphan_check_interval`) and then `orphan_grace` (30 s: Redis may have lost a live process's
  key); a job recovered again without progress since its first recovery (a suspect of a crash
  loop) waits in `recovery-suspects`, which neither the dashboard nor the API shows, and the
  suspects come back one at a time (at most one per orphan check, the next once the last is
  done), so after a crash loop the jobs that ran beside the pill run minutes later; the
  restart path of a processor that panics inside hench itself has no test (no fault-injection
  hook); workers run server middleware for unknown classes while testing clients report them
  first; jobs pushed by a running job on an inline testing
  client run nested (as in Sidekiq), so a chain of a few dozen can overflow a 2 MiB test
  thread in debug builds (fake clients run chains iteratively; interrupted iterable jobs re-run
  in a loop); `JobExt::perform_inline` skips middleware (Sidekiq runs both chains); API moves
  run client middleware before the atomic script (two concurrent moves of one entry may both
  run it; the job still moves once) and `retry_all` moves 16 jobs at a time (order within a
  batch not kept); death handlers of API kills need an `Api` with a `Config`
  (`Api::from_hench`); the typeck trybuild cases only run on the recorded rustc (1.97.0; CI's
  `macro-diagnostics` job is pinned to it and fails on a mismatch).
- Left open by review round 3 (each of its findings is fixed; these parts of them are not): a
  freed throttle permit is not handed straight to a parked job (the release script would have
  to move jobs out of the schedule, which the scheduler owns), so jobs shorter than the
  scheduler's round trip drain at about two thirds of the limit (`max_wait` keeps them waiting
  in process instead; ratelimit-7, cross-soak-perf-1); no warning when a workflow's callback
  lands in a queue no live process polls (docs-usability-2: the step now runs in the queue of
  the stage it continues); a job requeued by a shutdown after giving its unique lock up runs as
  fetched, and its success counts a lapse (unique-4); the soak measures its burst's drain
  without asserting it (the members' window limiter, not the throttle, paces that drain: a
  check of the throttle's drain needs members that skip the limiter, ideally in a window
  without faults) and runs no workflow with nested batches (`tests/batch_chaos.rs` covers them;
  cross-soak-perf-4).
- Deferred findings (review round of 2026-11-01; rounds 2 and 3 deferred none whole):
  redis-performance-3, the fetch's per-call Lua script (lane `fix-engine`). Its cost grows with
  the number of strict queues and the payload size (measured 1.5-6x the Redis CPU of plain
  `BRPOP`/`BLMOVE` with 20 strict queues or 100 KB jobs; about 10% in the default setup of one
  queue and small jobs). The fixes proposed trade DESIGN §5's atomic skipping of paused queues
  for a per-process cache of the `paused` set, need `LMPOP` (Redis 7, while hench supports 6.2)
  or one in-progress list per processor slot (a data-model change touching orphan recovery, the
  shutdown's release and the reconciliation), so they need a design decision rather than a fix.
  **Decision (2026-11-01): keep the Lua fetch.** Atomic paused-queue skipping and Redis 6.2
  support are worth more than the Redis CPU it costs; reliable fetch already measures 49-65k
  no-op jobs/s per process. Guidance for heavy setups (many strict queues, very large payloads,
  Redis CPU-bound): keep payloads small (store large data elsewhere and pass ids), prefer
  weighted over long strict queue lists, or use basic fetch where its at-most-once delivery on
  a lost reply is acceptable. Revisit if per-slot in-progress lists become worthwhile for other
  reasons.

## Review log
_(newest first: date, scope, findings fixed)_

- 2026-11-04, review of queue limits, round 4: a final check of round 3 for regressions and
  flakiness. Nothing blocks merging.
  - Verified:
    - the waker registry is bounded by capsules × queues, with no lock-order or panic
      hazard;
    - no wake-up storm with Redis killed, paused or out of memory (same counts as round 2);
    - the 250 ms backstop adds no steady-state calls;
    - `queue_limits` passed 20 runs under CPU load (up to 6 busy loops and 3 suites at
      once, or one shared core).
  - Fixed:
    - a public `retrieve_work` future dropped while it waited kept timing its cooldowns. It
      now hands them on (`a_dropped_fetch_hands_on_the_cooldowns_it_timed`);
    - two sentences of DESIGN and ratelimit.md still described round-2 wake-ups.
  - Documented: wake-ups fan out to every capsule sharing a queue. On a trickle that costs a
    read-only gate call per extra capsule (5 a job for 4 capsules, against 2). It is the
    price of the round-3 fix, which doubled what a shared backlog runs.
- 2026-11-04, review of queue limits, round 3: a hunt for stalls and lost wake-ups after
  round 2 removed the broadcast wake-ups.
  - Verified: within one capsule nothing is lost (hand-offs, the grant chain, timer duty); the
    herd fix holds (2.5 script calls per job with 20 and 50 workers); FIFO kept and no
    over-admission (multi-thread runtime, 4 shared connections).
  - Fixed, both reproduced first: every wake-up went to the waking worker's own capsule, so
    capsules sharing a limited queue missed each other's.
    - A burst taken by one capsule's watcher left four idle workers of the other capsule,
      and four free slots, waiting 1.3 s.
    - A shared rate-limited queue whose probe a busy capsule kept handing on ran 35–50 of
      80 jobs, with 2 s gaps.
    - Now each capsule registers its queues and waker with the shared cooldowns, and a
      wake-up for a queue reaches one sleeping worker of each capsule fetching it. Now 5
      start at once, and 80 of 80 run with gaps of at most 518 ms.
    - A worker waiting for a queue probed elsewhere now looks again within 250 ms.
  - Also fixed:
    - a worker that failed a fetch or ended (a crashed processor) kept the cooldowns it
      timed; it now hands them on;
    - a cooling queue could be watched, making its timer re-fetch every 10 ms.
  - Tests: `capsules_sharing_a_queue_wake_each_other` (both strategies),
    `a_rate_limited_queue_shared_by_two_capsules_keeps_its_pace`, and unit tests for the
    registry and the 250 ms backstop.
- 2026-11-04, review of queue limits, round 2: an adversarial review of the round-1 engine
  (production 2 s fetch timeout, repro tests).
  - Verified: no over-admission (3 processes switching limits during bursts), FIFO kept, no
    stall beyond the fetch timeout, no spin, `release_unused` safe.
  - Fixed, all reproduced first:
    - Every freed slot and probe refusal woke every sleeping worker, which then fetched again.
      With an idle limited queue beside a busy one, script calls ran at 39 per job with 20
      workers and 99 with 50. Now nothing but changed limits wakes them all; each cooldown is
      timed by one worker; a watched queue is left to its watcher. Now about 2.5 per job with
      5 or 50 workers, and 3.5 (was 40) beside a paused queue.
    - A cooldown in one capsule shortened another capsule's waits: a plain capsule's 10
      workers blocked 70 times in 2 s instead of 10. Waits now count only the capsule's
      queues.
    - A slot freed while a probe was in flight could cool the queue for 250 ms anyway.
    - A stale reply lost the refusals before it.
    - `blocking()` scanned the keys linearly.
  - Documented (bounded by the fetch timeout, like reliable fetch's rotation):
    - the rest of a burst when the capsule's other workers block on unlimited queues;
    - with basic fetch and few workers, an unlimited job while its worker watches;
    - a first burst reordered when limits are set during blocking waits;
    - the watch's `BLMOVE` being refused under out-of-memory.
  - Tests: `idle_workers_are_not_woken_for_every_job_of_a_busy_queue` and
    `a_capsule_without_limits_is_not_woken_by_another_capsules_cooldowns`, plus unit tests for
    timing, freed-during-probe and watched queues.
  - Throughput (`ci/throughput.sh`, 50 000 jobs, 4 cores, run-to-run deviation about 10 %):
    - plain jobs against the branch's base `4a4d0a9`: medians 26 337 against 27 578 jobs/s
      (−4.5 %), and +6.3 % in an earlier run: no change beyond noise;
    - the `queue-limited` kind against the first implementation `bccdbd3`: 9 527 against
      10 363 (−8.1 %; the grant's re-check adds one read);
    - against round 1 `e66e189`: +4.0 %.
  - `ci/throughput.sh` built both revisions in one target directory, so cargo could link the
    head example against the base revision's library (a stale build that failed to compile).
    The base revision now builds in `<target>/throughput-base`.
- 2026-11-04, review of queue limits (branch `queue-limits`), round 1: two reviewers, one on
  the engine (gate, cache, cooldowns, blocking step, permits) and one on the surfaces (API,
  builder, file, command, JSON API, dashboard, docs).
  - Engine, 2 major findings, both reproduced and fixed:
    - A burst on a full queue lost its order: several blocked fetches each took a job, were
      refused and pushed it back in the order of their refusals (5 of 5 rounds).
      - Fix: the blocking step never takes from a limited queue. One fetch per process
        watches it (`BLMOVE q q RIGHT RIGHT`), and jobs leave it only through the gate.
    - Idle processors stampeded a full queue (Redis writes grew with the idle processors).
      - Before: 551 refusals for 30 jobs with 20 processors; about 76 a second while one
        job held a `c=1` slot.
      - Fix: one probe per process when a cooldown ends; a grant that fills the limits says
        so; skipped queues are reported with a grant; idle workers are woken one at a time.
      - After: 19 refusals (only the processors starting together); about 4 a second.
  - Engine, 4 minor findings, all fixed:
    - Concurrent refreshes could roll the cache back; they are now serialized.
    - A limit state of the wrong type stopped the whole capsule; it now closes its queue
      alone.
    - A basic fetch's job whose push-back failed was lost silently; it is now logged in full.
    - A lost or failed gate reply leaked a slot for a whole lease; the fetch's token is now
      released.
  - Surfaces: 11 findings, all fixed:
    - the `queue_limits` heartbeat feature and the Busy page's "lacks" chip;
    - doubled "configuration error" prefixes;
    - queue names in errors, and a hint for limits joined by `,`;
    - usage links only for limits in use;
    - `set_limits` returns the stored form, plus `stored_limits`;
    - 400 for invalid limits in the JSON API;
    - docs: start semantics, rollout, environment sections, standalone fetchers, and the
      throttle's rate charge.

- 2026-11-02, final check of the 0.1.0 tree: GitHub CI's stable toolchain (rustc 1.99.1) has a
  clippy lint the local 1.97 lacks, `chunks_exact_to_as_chunks`, which failed CI's Clippy step
  on `api/sorted.rs` (`chunks_exact(2)` → `as_chunks::<2>().0`, available since 1.88); clippy
  1.99.1 now passes on the whole workspace, all targets and features. A suite run also caught a
  race in `tests/unique_moves.rs` (`the_scheduler_extends_the_lock_of_a_due_job_it_defers`):
  its wait accepted the scheduler's lease (which also moves a due job a minute ahead) for the
  deferral, so it could read the lock between the two writes; it now waits for the lock to
  record the deferral. Product behaviour unchanged; 1,732 tests pass.
- 2026-11-02, integration of the release review (round 4) and the 0.1.0 release: `fix4-core`
  and `fix4-surface` merged into `implement-hench` (`12a89e9`, `7d5772e`; branches kept,
  worktrees removed). The review's four lenses (crashloop, round3-regressions,
  release-surface, fresh-user) sent 22 verified findings to the two lanes; crashloop-3 (a job
  retried from the morgue keeps its recovery count) was refuted by verification: the count
  outliving the burial is the documented design (DESIGN §5). Outcomes, from the lanes' reports
  and checked against the merged tree (every regression test they name is in it and passes):
  - `fix4-core`: 19 findings, 19 fixed, 0 not a bug, 0 deferred (crashloop-1, -2, -4, -5,
    round3-regressions-1–4, release-surface-1–7 and -9, fresh-user-1, -2, -5).
  - `fix4-surface`: 3 findings, 3 fixed, 0 not a bug, 0 deferred (fresh-user-3, -4,
    release-surface-8).
  - Total: 22 finding ids, 22 fixed, 0 not a bug, 0 deferred. The owner's scope decision
    stands: phase 3 (queue limits, staged batch commits, the iterable checkpoint's
    cancellation check, the cancelled-batch cache, batch tags, the unique lean lock mode) is
    deferred to 0.2 and not started; no document claims it exists (`[queue_limits]` is listed as
    reserved and refused, `keys::QUEUE_LIMITS` and the `queue-limits` row as reserved).

  Conflicts: `docs/STATUS.md` only (both lanes' review-log entries kept, `fix4-core`'s first);
  git combined the README, DESIGN, the two manifests and the three hench-server files both
  lanes had changed. The lanes shared one `CARGO_TARGET_DIR`, so every source of the merged tree
  was touched before the gate, and every workspace crate was built again from it.

  Integration fixes (`3a0a94d`, and the last commit for `docs/RELEASING.md`):
  - `fix4-surface` made the JSON API's feature modules public (for docs.rs), which put their
    `docs/design/batches.md` and `docs/design/ratelimit.md` mentions on three public pages as
    plain code: they are links pinned to the `v0.1.0` tag, like `fix4-core`'s (release-surface-4);
    the rendered documentation has no `docs/` code span left.
  - The item `fix4-core` left outside its lane: hench-server's README doc tests included
    `../../../README.md`, a path out of its package, so `cargo test` of the published crate
    could not build them. `crates/hench-server/README.md` is now a symbolic link to the
    repository's README (`readme = "README.md"`), which `cargo package` stores as a file, and
    `ReadmeDoctests` includes `../README.md`: the 22 examples build and pass from the packaged
    crate (checked by hand first, then by the script). `ci/check-package.sh` (and CI's
    `package` job) now also builds every target of the packaged `hench-web` and
    `hench-server` and runs those doc tests from the package.
  - `docs/RELEASING.md`: `cargo deny --all-features check` (cargo-deny 0.20.2 refuses
    `check --all-features`, which the list gave), the package script's scope, the throughput
    gate for fetch and processor changes, and the changelog step.

  Release documents (`a849dcf`): `CHANGELOG.md` (Keep a Changelog; 0.1.0, 2026-11-02: what the
  release does, with the crash-loop protection (`max_recoveries`, 3 by default,
  `hench::CrashLoop`, suspects released alone), the orphan recovery (`orphan_check_interval`
  60 s, `orphan_grace` 30 s: jobs back 1.5 to 2.5 minutes after a crash) and the other
  behaviour settled by rounds 3 and 4, its known limitations and its requirements; every
  statement checked against the code; `[Unreleased]` names the 0.2 plan); README: a Roadmap
  section (phase 3 for 0.2, each item with what it brings and what to do until then, then
  "later" items from the gaps above), its Contents entry (with the MSRV section's), the
  Quick start's crates.io lines (`hench = "0.1"`, `hench-web = "0.1"`, `cargo install
  hench-server --locked`), the other "not yet" statements pointing at the roadmap, the
  features list naming the feature commands, the changelog linked; DESIGN: the reserved
  `queue-limits` row, the throttle as a push-time payload field (fresh-user-1), the packaging
  checks; this file's release state.

  Gate on the release tree (the merges, `3a0a94d` and `a849dcf`, with the final documentation
  edits): `cargo fmt --all --check`; `cargo clippy --workspace --all-targets --all-features --
  -D warnings`; `cargo test --workspace --all-features` three times in a row with
  `HENCH_BROWSER_TESTS=1` (83 test binaries and doc-test groups each: 1,732 passed, 0 failed,
  6 ignored; 1,706 at `4a753f9`, 26 added by the lanes; 3.6 minutes a run once built, the
  macro's typeck cases on rustc 1.97.0, the Playwright browser tests forced on), after a first
  run on the merged tree before the documentation edits with the same counts: no flake to
  chase, and every regression test the lanes name passed in each run; `RUSTDOCFLAGS="-D
  warnings" cargo doc --workspace --no-deps --all-features` (no `docs/` code span left on any
  page, 23 pages link the design documents at the tag); `cargo +1.88 check --workspace
  --all-features --all-targets`; `ci/check-readme-examples.sh` (22 examples); `cargo deny
  --all-features check` (advisories, bans, licenses, sources ok); `ci/check-package.sh` with
  `RUSTFLAGS="-D warnings"` in a target directory of its own, as CI runs it (the packaged
  `hench`'s targets with three feature sets and its 528 unit tests, every target of the
  packaged `hench-web` and `hench-server`, and the README's 22 doc tests from the packaged
  `hench-server`); `cargo publish --dry-run --workspace --allow-dirty` (packaged and verified
  in order: `hench-macros` 12 files, 29.5 KiB compressed; `hench` 261 files, 1005.4 KiB;
  `hench-web` 96 files, 289.5 KiB; `hench-server` 32 files, 162.7 KiB; the upload aborted as a
  dry run); the docs.rs-style build with nightly 1.101.0 (2026-11-01), `--cfg docsrs` and `-D
  warnings` (`hench` with every feature, the other crates with their defaults); `ci/soak.sh
  --short` with `HENCH_BIN` (crash, proxy, redis-none, stale-sched, pro, pro-crash,
  pro-redis-none; 20 minutes): every scenario held its invariants (49,519 pushes, 61,092
  executions, every extra run with a documented cause; in the three pro scenarios 143 batches,
  2,182 members, 24 batches cancelled, 133 ticks with at most 19 s between two, 448 unique
  pushes accepted with 269 duplicates and 27 debounces, no overlapping unique runs, never
  more than 3 members at once, no lock left, no throttle overage; `recovery-suspects` empty
  after every drain); `ci/throughput.sh --base 4a753f9` (fetch.rs, job.rs and the limiter
  changed: 15 alternating runs of 100,000 no-op jobs each, run twice): `4a753f9` median 41,564
  jobs/s (mean 41,191, standard deviation 1,974) against 40,474 for the release tree (mean
  40,418, standard deviation 1,661), -2.6 % (Welch's t = -1.2); again, 42,765 (mean 42,281,
  standard deviation 2,178) against 42,498 (mean 42,720, standard deviation 1,350), -0.6 %
  with the higher mean: within the run-to-run noise, so no plain-job regression. (A first
  attempt, chained after the soak, matched the soak's `exits:` lines and started during its
  proxy scenario; it was stopped while still compiling, nothing was measured, and the proxy
  scenario still held its invariants.)

- 2026-11-02, release review (round 4), fix lane `fix4-core` (branch `fix4-core`: the core
  runtime, the docs, the examples, the soak harness, CI and the workspace manifests; the
  adversarially verified findings of the crashloop, round3-regressions, release-surface and
  fresh-user lenses assigned to the lane; phase 3 stays deferred to 0.2). Each code fix comes
  with a regression test written first, which failed on `4a753f9`:
  - **crashloop-1** (verified medium): every job in flight when a process died was recovered
    with the poison pill and pushed back to the front of its queue at once, so the next idle
    processors (the restarted process, or the only one of a small deployment) fetched the
    whole group again and buried it with the pill as `hench::CrashLoop` (the reviewers'
    scenarios, one to ten processes, idle or loaded, 4 s neighbours: 1 to 5 jobs buried per
    pill; death handlers and batch deaths for each). The jobs of a dead list are now judged
    together, once, on the list as the crash left it (`fetch/poison.rs`): a job at its first
    recovery goes back at once, as before; a job recovered before (a *suspect*) waits in
    `recovery-suspects` (ZSET), and the suspects come back alone, at most one per orphan check
    and only once the one released before is no longer waiting near the front of its queue or
    running (`recovery-suspects:released`, at most ten check intervals), the most recovered
    first; several suspects of one crash reaching the limit together each get one recovery
    more, alone. The `hench::CrashLoop` message says how many other jobs were in flight at the
    pill's last crash and the warning names them. All nine reviewer scenarios now bury the pill
    only (their driver, re-run against the fix, kept killing until the pill was buried). One
    suspect per check without the hold still overlapped neighbours longer than the interval
    (4 s jobs, 0.5 s checks: four buried), hence the hold. Tests (`tests/poison.rs`):
    `suspects_come_back_one_per_orphan_check`,
    `jobs_that_reach_the_limit_together_get_one_more_recovery_alone`,
    `the_crash_loop_error_counts_the_jobs_in_flight_beside_the_pill`,
    `jobs_that_ran_beside_a_poison_pill_are_not_buried_with_it` (real workers, one process at a
    time). The soak harness's drain also waits for `recovery-suspects` to empty.
  - **crashloop-2**: an iterable job's first save of progress in an execution (a cursor beyond
    the one it started from) deletes `recovered-<jid>` in its state's pipeline, so only crashes
    with no progress in between count, and a long iteration crashed by unrelated processes is
    never buried; one that crashes before any progress still is. Plain jobs restart from
    scratch at each crash and keep counting (clearing the count at their failures would change
    `SETTLE` and `PUSH_BACK`, which jobs without features run byte for byte). Tests:
    `an_iterable_job_that_makes_progress_between_crashes_is_not_a_poison_pill` (four crashes,
    `max_recoveries(1)`), `an_iterable_job_that_crashes_before_any_progress_is_a_poison_pill`.
  - **crashloop-4**: the orphan check's lock is kept while the check runs
    (`KEEP_RECOVERY_LOCK`: taken back when it lapsed, at least half the interval left; a check
    that finds another process holding it stops), and a pill is claimed before it is settled
    (`recovered-<jid>:claim`, `of` the dead list, `by` the settling process, 10 minutes): 40
    pills under overlapping checks ran 52 to 79 death handlers, now 40. Tests:
    `overlapping_recoveries_settle_each_poison_pill_once`,
    `a_lost_claim_reply_buries_the_poison_pill_once`,
    `a_poison_pill_claimed_by_another_process_is_left_to_it`.
  - **crashloop-5**: README "Panics", DESIGN §3 and the startup warning say that under
    `panic = "abort"` a panicking job aborts up to `max_recoveries + 1` processes, with the
    jobs running beside it, then lands in the morgue as `hench::CrashLoop`.
  - **round3-regressions-1** (verified low): a limiter stored a rescheduled job's place in the
    wait line (`limited.s`) but never handed it back, so the job queued again at the end of the
    line. A job's context now hands it over as the ticket of its first acquisition of that limit
    in the execution (once: a stale ticket would jump the line). Tests (`tests/limit.rs`): back
    early, it keeps its place (the line does not move; it moved 2 s before); back a little
    late, it gets the next unit; the place is used once per execution; with real workers a job
    moved back early runs at its place (A, B, C, D in order; C, D, B before).
  - **round3-regressions-2**: `try_acquire` never takes a place in the wait line, whatever its
    target (40 refusals through a job's context pushed the line 21 s away); `acquire` from a
    job still takes one, for the job's reschedule. Test
    `try_acquire_in_a_job_takes_no_place_in_the_line`; README and DESIGN §9j say a job that
    handles a refusal itself should use `try_acquire`.
  - **round3-regressions-3**: `Workflow::start` checks every stage against
    `batch_max_commit_jobs` (its jobs plus 10 for its batch) before writing anything, refusing
    with the new `BatchError::StageTooLarge { stage, jobs, limit }` (a later stage used to fail
    in its `hench::WorkflowNext`, stalling the workflow). Test
    `workflows_check_every_stage_against_the_commit_cap`.
  - **round3-regressions-4** and **release-surface-7**: `hench::cli` lists `[queue_limits]` as
    reserved (refused) and the throttles' `stats` and `stats_ttl`; `LimitInfo::queue` and
    `keys::QUEUE_LIMITS` say queue limits come later; `lease.lua` says 10 000 skipped names.
  - **fresh-user-1** (verified: a docs gap; the throttle is a push-time payload field, D16):
    README says that a push by class name (`hench enqueue`, `POST /api/v1/jobs`, a schedule
    made with `hench periodic add`, `Client::push` of a `JobPayload`) runs unthrottled unless
    it names the throttle (`--throttle NAME[:KEY]`, `throttle`/`throttle_key`).
  - **fresh-user-2**: README no longer says the dashboard creates schedules.
  - **fresh-user-5**: limit errors name the limit in the spec grammar (`` `concurrency=0` ``),
    not as a Rust value; unit test `invalid_limits_are_named_in_the_spec_grammar`.
  - **release-surface-2** (verified medium): with the `tls` feature, the first `rediss://`
    connection panicked when another dependency enabled rustls's default aws-lc-rs provider
    next to hench's `ring` (rustls picks a provider from its features only when they name
    one). Creating a `rediss://` client now installs `ring` as rustls's process default unless
    one is installed (`redis.rs`, `install_crypto_provider`); an application that wants another
    provider installs it first (README "Redis", `TlsConfig`, DESIGN §1, the feature's doc).
    Unit tests, each alone in a child process (the default is per process):
    `a_rediss_client_installs_ring_unless_a_provider_is_installed` (failed before),
    `a_provider_installed_by_the_application_is_kept`. The reviewer's program with both
    providers (`ring` and `aws_lc_rs`) now gets "Connection refused" instead of the panic.
  - **release-surface-5**: the published `hench` builds and tests from its `.crate` file: the
    eleven feature test targets that start a `redis-server` declare `required-features =
    ["test-util"]`, the soak example `required-features = ["periodic"]`, the repository checks
    `tests/spec_parity.rs` and `tests/readme.rs` are excluded from the package, and test-only
    helpers used by `test-util` tests (`Flusher::guard`, `UniqueTable::{hold, forget}`) and
    the periodic-only `Api::config` and `Engine::periodic_outage` are compiled only with what
    uses them (no dead code without the default features). New `ci/check-package.sh` (CI job
    `package`): `cargo package --workspace` with verification, then, in the extracted `hench`
    with `-D warnings`, `cargo check` of every target with the default features, with all of
    them and of the library and examples without them, and its 528 unit tests; the
    integration targets `batch`, `limit`, `throttle`, `poison` and `featureless` also passed
    from the package. Not in this lane: the packaged `hench-server`'s doc tests include the
    repository's README (`../../../README.md`, a path that leaves the package), so they cannot
    build from its `.crate` file (by inspection; its other tests can).
  - **release-surface-6**: `hench` requires `hench-macros` with `=0.1.0` (the expansion uses
    `hench::__private`); `docs/RELEASING.md` (new) has the release steps: the repository
    settings before 0.1.0, version bumps, pinned links, the gate, the tag, the publishing
    order, and the compatibility promises.
  - **release-surface-1, -3, -4**: `homepage` (equal to `repository`) is gone from the four
    manifests; the README's links into the repository (crate table, examples, DESIGN and
    STATUS, LICENSE) and the rustdoc's mentions of `docs/DESIGN.md` and `docs/design/*.md` on
    its 17 public `hench` pages and 3 `hench-server` pages are links pinned to the `v0.1.0`
    tag, which work on GitHub, crates.io and docs.rs alike (crates.io resolved the relative
    links under `crates/<crate>/`). Making the repository public and `main` current are
    repository settings, listed in `docs/RELEASING.md`.
  - **release-surface-9**: `#[non_exhaustive]` on `cli::WorkerArgs`, `api::{Page, RawEntry,
    CapsuleInfo, StatCounter}`, `metrics::{QueryResult, JobMetrics, DeployMark, SortBy,
    Period}` and `Exhausted` (`Page::new` builds pages outside hench; hench-web's two unit
    tests use it); `metrics::Granularity` stays exhaustive (it names the two resolutions the
    metrics are stored at); `random_hex` is `#[doc(hidden)]`; the `hench-server` library says
    its API is not covered by semantic versioning (DESIGN §1, RELEASING.md).
  - Validation of the lane's head: the full suite, `ci/check-package.sh` (with `-D warnings`),
    `cargo deny check` and `ci/soak.sh --short` (its seven scenarios held every invariant; the
    crash scenarios' drains ended with `recovery-suspects` empty).

- 2026-11-02, release review for 0.1.0, fix lane `fix4-surface` (`crates/hench-web`,
  `crates/hench-server`; the verified release-surface and fresh-user findings of these crates;
  branch `fix4-surface` from `4a753f9`; the code bug reproduced first by a test that failed on
  the previous tree, the tests kept):
  - fresh-user-4 (an API shape, settled now because 0.1.0 publishes it): `GET
    /api/v1/processes`, `hench processes --json` and `hench stats --json` gave each process's
    `queues` as one comma-joined display string per capsule (`["critical, default, sync, erp,
    imports"]`) although the heartbeat stores the names, so a script could not tell whether a
    process serves a queue (`jq 'any(.[]; .queues | index("default"))'` said no), the
    capsules' weights were lost, and a queue named `a, b` read as two. Taking the
    production-ready option, the JSON carries the data and only the text renders it: `queues`
    is the list of the names of the queues the process works on, each once, in its configured
    order (`api::Process::queues`), and the new `capsules` gives each capsule by name with its
    `concurrency`, `mode` and `weights` (`{queue: weight}`), in the order the heartbeat lists
    them; a capsule or queue a heartbeat lists twice appears once with its last value, so no
    JSON object repeats a key. The text reports keep their per-capsule groups (`Queues:
    default, high, low; single`), built from these fields (`Queues: none` when a process
    reports no queue, a blank before). The two tests that pinned the joined strings now pin
    the names and the capsules (`tests/http.rs` `the_api_reports_stats_queues_and_processes`;
    `tests/cli.rs` `stats_print_json`, which now also checks `hench processes --json` and
    `hench stats processes --json`). Tests: `tests/http.rs`
    `the_api_lists_each_process_queues_by_name` (the reviewer's reproduction, five strict
    queues and `default`'s membership; three capsules in a non-alphabetical order; a heartbeat
    without a queue list; one with neither; repeated capsules and queues), `monitor.rs` unit
    tests (the serialized shape, the text's groups, repeated names, escapes through capsule
    queue names). The reviewer's reproduction now prints `"queues":
    ["critical","default","sync","erp","imports"]` and finds `default`.
  - fresh-user-3: the docs.rs front page's synopsis, the `json_api` docs and two clap help
    texts predated the features. The synopsis names every command (the four features' with
    their subcommands), every `hench stats` section and every option of `web` and `enqueue`,
    and points to `--help` and the `commands::*` modules (`commands::batches` and
    `commands::limits` gained synopses like the other two); the `json_api` docs list every
    route, the shape of `GET /processes` and the read-only rule, and its four feature
    submodules are public so that docs.rs shows their details (they export nothing); `hench
    --help` lists `dispatch` for `batches` and says what `stats` prints; `hench web --help`
    says `--api` also serves, and can change, the schedules, batches, limits and locks. The
    package description names the features. Tests: `tests/cli.rs`
    `the_help_names_every_subcommand_and_what_the_api_serves` and
    `the_synopsis_names_every_command_option_and_section` (both walk clap's command tree, so
    a command, subcommand, option, section or value added later without its help or
    synopsis fails), `tests/http.rs` `every_route_the_api_documents_answers` (every
    `METHOD /path` the docs list answers with that method on a read-only server, each change
    with 403; each feature's `GET` is listed). All three fail on the previous texts.
  - release-surface-8: hench-web's crate docs (introduction and Routes), its package
    description and the README's Pages paragraph name the four feature tabs, their nine
    routes, what their `POST`s do and the switches that remove them
    (`HenchWeb::{periodic, batches, limits, locks}(false)`); the Routes section lists every
    route with its actions, for a proxy that filters paths; `HenchWeb::new` and
    `HenchWeb::routes` name the tabs' paths (an extension route on one panics while its tab
    is on); `HenchWeb`'s `Debug` shows the four switches. Tests: `tests/features.rs`
    `every_route_the_crate_documents_is_served` (each listed path is served with its method:
    none falls to the dashboard's 404 page or answers 405; each tab's path is listed; fails
    on the previous list), `a_host_route_can_take_a_tab_s_path_only_with_the_tab_off` (what
    `HenchWeb::routes` now says), and `config.rs`' builder test checks the `Debug` output. The
    verification's corrections stand: the switches were documented on `HenchWeb`, and
    read-only mode already refused every `POST`, the tabs' included.
  - Outside the two crates, docs only: README.md (Pages; the feature commands in "The `hench`
    command"; the shape of `/processes`), docs/DESIGN.md (the dashboard's routes, `GET
    /processes`, `hench stats`), this file.
  - Gate: fmt, clippy `-D warnings`, the suite (1 714 tests, 0 failures, 6 ignored; 1 706 at
    `4a753f9`, eight added; run twice), rustdoc `-D warnings` and `cargo +1.88 check
    --all-targets` pass. The built binary's `hench --help`, `hench web --help` and `hench
    batches --help` show the new texts, and `target/doc` has the routes and the synopsis.

- 2026-11-02, integration of review round 3's fix lanes: `fix3-web`, `fix3-feat-a`,
  `fix3-feat-b` and `fix3-core` merged into `implement-hench` in that order (branches kept,
  worktrees removed). The merges (`0d8037e`, `22182f5`, `89382c1`, `637bb03`) were made by an
  earlier run of this integration that stopped during the gate; this run checked them, kept
  them and finished. Round 3's seven review lenses (unique, periodic, ratelimit, batches,
  cross-soak-perf, web-api-security, docs-usability) and two directed findings sent 73
  finding ids to the four lanes. Outcomes, from the lanes' reports and checked against the
  merged tree (every regression test they name is in it and passes):
  - `fix3-web`: 8 findings, 8 fixed, 0 not a bug, 0 deferred (web-api-security-1, -4–7, -9,
    -10, docs-usability-7).
  - `fix3-feat-a`: 13 findings, 13 fixed, 0 not a bug, 0 deferred (unique-1, -3, -4,
    periodic-1–7, cross-soak-perf-2, docs-usability-4, -14).
  - `fix3-feat-b`: 25 findings, 25 fixed, 0 not a bug, 0 deferred (ratelimit-1–10, batches-1–3
    and -5–11, cross-soak-perf-1, -6, docs-usability-2, -13, -15).
  - `fix3-core`: 27 findings, 27 fixed, 0 not a bug, 0 deferred (directed-1, -2, unique-2, -5,
    -6, periodic-8, batches-4, -12, web-api-security-2, -3, -8, cross-soak-perf-3–5, -7, -8,
    docs-usability-1, -3, -5, -6, -8–12, -16, -17).
  - Total: 73 finding ids (70 distinct issues: web-api-security-2 is batches-4 seen from the
    dashboard, cross-soak-perf-2 is unique-3's leak, docs-usability-9 is part of
    cross-soak-perf-8's sizing claim), 73 fixed, 0 not a bug, 0 deferred whole. The parts of
    findings the lanes left open, with their reasons, are in the gaps above ("Left open by
    review round 3"): a freed throttle permit handed straight to a parked job, a warning for
    workflow callbacks in unpolled queues, the lapse counted after a shutdown requeue of a job
    that gave its lock up, and two soak additions (the burst's check, a nested workflow).

  Conflicts resolved in the merges (git combined the other files both sides had changed):
  `fix3-feat-a`, the review log only; `fix3-feat-b`, `limit::rule::check_name_of` (the lane's
  messages per kind and the web lane's refusal of `.` and `..`), the limits JSON API (the
  holder's jid and lease ends formatted without panicking), README and DESIGN §9i (the web
  lane's batch page is item 9, the lane's decisions item 10); `fix3-core`, the
  `batch_max_commit_jobs` docs (the children's weight and the commit's Redis cost), the
  sizing and cost paragraphs (README, DESIGN §9k: the measured costs, the lock's copy of its
  holder, the throttle statistics) and the README's unique, conflict and throttle bullets
  (every lane's sentences, with `.reschedules(n)`). Checked across lanes, where no test of
  one lane exercises the other's change: `RESCORE_UNIQUE` (core) records the holder's member
  `m` with `at` as `fix3-feat-a`'s `hu_hold` does, so `OnConflict::Replace` finds a job moved
  by a reschedule (`tests/unique_moves.rs` replaces one on the merged tree); `SCAN_CONTAINING`
  (core) still finds `fix3-feat-b`'s `limited` records (`"by"` serialized as before, the new
  `t`, `o` and `s` after it); the pipelined tree cancel (core) builds each call like the
  single cancel, so `fix3-feat-b`'s `BATCH_CANCEL` (holds released, a cancelled batch finished
  once nothing is pending) applies to every batch of a tree; the soak's throttles (30 s
  leases, 10 s heartbeat) and the features example pass `fix3-feat-b`'s three-heartbeat lease
  rule; schedules are pushed by `PERIODIC_FIRE`, not through the client's pipeline, so
  `fix3-core`'s merged default options (unique-6) never applied to them and `fix3-feat-a`'s
  file schedules need nothing more.

  Integration fixes:
  - `tests/batch_api.rs` (`846a24f`): `fix3-core`'s tests defined a tree of 1,203 batches and
    one of 10,050 children in one commit, which `fix3-feat-b`'s child weight (each child
    counts as 10 jobs toward `batch_max_commit_jobs`, 10,000 by default) refuses
    (`BatchError::TooLarge`, the only failure of the merged tree's first run). The trees are
    built as an application adds children beyond a commit's cap: the root reopened with
    `Batch::open`, 800 children a commit; their sizes stay above the old limits of cancel
    (1,000) and delete (10,000).
  - Documentation the merge made stale. The per-feature Redis CPU of README "Sizing" and DESIGN
    §9k was measured before `fix3-feat-b`'s throttle changes: measured again with the review's
    harness (`r3_perf`, exec phase, 30,000 no-op jobs, 25 slots, median of three), alternating
    the merged tree with the reviewed tree `91e9449` (which reproduced the documented figures):
    throttled 82 µs a job instead of 78, per key 88 instead of 85, periodic 29 instead of 28,
    the other kinds within 3 % (plain 16 µs, every feature 193): about 12,000 throttled jobs a
    second per Redis core, not 13,000. A concurrency holder is about 30 B for a limiter's
    permit but 60 B for a throttled job since `fix3-feat-b`'s execution tokens (`MEMORY
    USAGE`; about 140 B each beyond 128 holders), where the docs said 32 B (README, DESIGN
    §9k, `docs/design/ratelimit.md`). The gap "`Client` accepts `at` beyond the year 9999" is
    gone (refused since `fix3-core`).
  - The soak's burst: `fix3-core` asked the integrator to turn the burst's slot utilization
    into a check (such as 70 % busy) once `fix3-feat-b`'s drain fix was merged. Not done, for a
    reason the merged runs show: the burst's members take the window limiter (8 grants a
    second) inside `perform`, so the limiter paces the drain (5 to 8 grants a second during
    each burst) and the throttle's slots stayed 20 to 26 % busy, as before the fix (21 to
    26 %); such a check would fail by construction. The harness now says so (`pro.rs`,
    `check_pro.rs`); the throttle's drain stays asserted by `tests/throttle.rs`.

  Gate on the merged tree: fmt, clippy `-D warnings`, `cargo test --workspace --all-features`
  (79 test binaries and 4 doc-test groups: 1,706 passed, 0 failed, 6 ignored; the Playwright
  browser tests forced on with `HENCH_BROWSER_TESTS=1`, the macro's typeck cases on rustc
  1.97.0), rustdoc `-D warnings`, `cargo +1.88 check --workspace --all-features
  --all-targets`, `ci/check-readme-examples.sh` (22 examples), `cargo deny check`
  (advisories, bans, licenses, sources) and `examples/features.rs` (CI's step) pass. The lanes
  had shared one `CARGO_TARGET_DIR`, whose artifacts cargo names relative to the workspace
  root, so the gate first touched every source of the merged tree, other revisions were built
  in a target directory of their own, and every binary measured was checked for strings only
  its revision has. The suite then passed three more times in a row (four in all, 3.4 minutes
  a run once built): no flake to chase. `ci/soak.sh --short` (crash, proxy, redis-none,
  stale-sched, pro, pro-crash, pro-redis-none; `hench web` sampled) held every invariant:
  45,662 pushes, 56,024 executions, every extra run with a documented cause; in the three
  pro scenarios 139 batches (2,140 members, 26 batches cancelled), 123 ticks (the longest
  stretch without one 46 s, in pro-crash, within its faults' allowance), 471 unique pushes
  accepted with 224 duplicates and 27 debounces, no overlapping unique runs, never more than
  3 members at once, no lock left; no `hench::CrashLoop` death (none expected in short runs).
  `ci/throughput.sh --base b4dddf5` (15 alternating runs of 100,000 no-op jobs): `b4dddf5`
  median 42,027 jobs/s (mean 42,749, standard deviation 1,956), the merged tree median 43,245
  (mean 43,131, standard deviation 1,794): +2.9 %, within the run-to-run noise (Welch's
  t = 0.6), so no plain-job regression.

- 2026-11-02, review round 3, fix lane `fix3-web` (`crates/hench-web`, `crates/hench-server`;
  the verified web-api-security and docs-usability findings of these crates; each bug
  reproduced first by a test that failed on the previous tree, the tests kept):
  - web-api-security-1: a batch's page looked up each failed member's job with
    `JobSet::find_job`, an unbounded `ZSCAN MATCH` of the whole retry set or morgue (every
    entry when the job was not there), up to 25 times per render of a Live page: 10 to 21 s
    and as much Redis CPU per view against a 100 000-job retry set. The page now locates each
    job from its failure summary (DESIGN §9i item 9): a retry at its `next_at` score, a job
    sent to the morgue or killed in the minute after its death (the morgue's score is taken
    after the batch records the death), with `ZRANGE BYSCORE … LIMIT` (one entry, then nine
    more when another job came first), pipelined: two round trips per page whatever the sets'
    sizes. A job not found there (running again, moved, trimmed) links to its list filtered
    by its jid (the bounded search, run on click; the link's tooltip says why); a job
    dropped, discarded or deleted has no link. Tests: `hench-web/tests/batches.rs`
    `failures_are_located_by_score_without_scanning_the_sets` (5 000-job storms in both sets,
    same-score crowds read before two members, a member killed through the API; counts the
    `ZSCAN` (0; 85 before) and `ZRANGE` calls with `INFO commandstats` and follows every
    link), a unit test of the score ranges (a summary's `next_at` round trip, up to the year
    10 000).
  - web-api-security-4: `POST /jobs` read `unique` like a stored job, leniently: a
    misspelled `until` became `success` and unknown keys were ignored, so a typo changed the
    lock's semantics (`"until": "expire"` released the lock when the job succeeded;
    `acrossQueues` gave per-queue locks). The API checks `unique` strictly before reading it:
    `false`, `null` or an object of `for`, `until`, `on_conflict`, `across_queues`, `key` and
    `digest`, `until` one of `success`, `start`, `expired`; anything else is a 422 naming the
    field or value. Stored jobs stay lenient. The existing test that pushed an unknown policy
    (`"later"`, stored as `success`) now expects the 422. Tests: `hench-server/tests/locks.rs`
    (`post_jobs_refuses_misspelled_unique_settings`, the updated
    `post_jobs_refuses_invalid_unique_settings`), the `json_api` unit test
    `unique_is_read_strictly`.
  - web-api-security-5: the new forms posted raw jids, identities and keys, which browsers
    post with a lone LF or CR turned into CRLF (and an HTML parser turns NUL into U+FFFD): a
    lock held by such a jid could not be released, a batch member not skipped, a limit not
    reset (the notice said it was). Every field that carries a value from Redis back to the
    server — the lock page's holder, the Locks list's keys, the Limits list's identities, the
    batch page's Skip jids, and, the same class, the retry, scheduled and dead lists' keys and
    the Busy page's process identities — holds it with `%`, control characters and non-ASCII
    text percent-encoded (`form::field`), decoded by `PostForm::value(s)` (DESIGN §9 Security
    (7)); generated jids and plain identities are written as they are. The Limits list's bulk
    Reset counts the identities that were in use (`Limits::reset` → `Ok(true)`), as the other
    lists count jobs still there. Batch pages spell out control characters in jids. Tests:
    `hench-web/tests/form_values.rs` (5: each form read from its page and posted as a browser
    posts it), the Playwright test `forms_post_back_values_holding_line_breaks`
    (`tests/browser/form_values.js` drives Chromium through the six forms; with raw values
    each one failed), the `form.rs` unit test (any value round trips through a browser's
    normalization), and `tests/limits.rs`' escaping test, which asserted the raw checkbox value
    and now asserts the encoded one and that posting it resets that limit.
  - web-api-security-6: `hench batches show` printed failed, pending and callback jids raw, so
    escape sequences reached the terminal and CR/LF forged rows: they are spelled out like
    every other value from Redis (the parent too). Test: `hench-server/tests/batches.rs`
    `show_spells_out_jids_and_says_when_the_batch_expires`.
  - web-api-security-7: schedules and limiters named `.` or `..` were valid but could not be
    opened or managed from the dashboard: every URL naming them is a dot segment (`%2e` too)
    that browsers and HTTP clients resolve away. `periodic::check_name` and
    `limit::check_name` refuse them (names that merely contain dots stay valid); the throttle
    macro's compile-time check follows, and its inline throttle names, which became `.` for a
    job name without one allowed character, get a leading `_`. Outside the web crates (the
    only such changes of the lane): `crates/hench/src/periodic/{definition,builder}.rs`,
    `crates/hench/src/limit/rule.rs`, `crates/hench-macros/src/throttle.rs`. Tests:
    `hench-server/tests/periodic.rs` `schedules_and_limiters_cannot_be_named_dot_or_dot_dot`,
    the name unit tests of those three modules.
  - web-api-security-9: the JSON API answered a path parameter that is not UTF-8 once decoded
    with axum's text/plain 400, an empty segment or an unknown path with the dashboard's HTML
    404, and a wrong method with an empty 405. Path rejections are JSON 400s; the API router
    has JSON fallbacks for unknown paths (404) and methods (405, axum still adds `Allow`), and
    `hench web` mounts it with `nest_service`, so that every path under `/api/v1` (itself
    included) reaches them, with or without a prefix. Test: `hench-server/tests/http.rs`
    `api_errors_are_json_for_bad_paths_unknown_routes_and_methods`.
  - web-api-security-10: `hench batches show` wrote every time as "ago", so the expiry read
    "(just now)": `monitor::relative` says `in 29 days` for times ahead (one helper for every
    command; the locks and periodic commands had copies). Same test as -6. Found on the way:
    `DateTime::<Utc>::from(SystemTime)` panics beyond the calendar; the batch and limit
    commands and the limits JSON API now format times read from Redis with
    `monitor::datetime` (`?`, or `null`, beyond it).
  - docs-usability-7: `HenchWeb::{periodic, batches, limits, locks}` said the tabs were "not
    available in this version yet": the docs now say what the switch does (the tab and its
    pages, 404 when off; `tests/features.rs` covers it).
  - Gate: fmt, clippy `-D warnings`, the suite (1 612 tests, 0 failures, 6 ignored; the
    Playwright browser tests forced on with `HENCH_BROWSER_TESTS=1`), rustdoc `-D warnings` and
    `cargo +1.88 check --all-targets` pass. The reviewers' reproductions, run on the final tree:
    the batch page with 25 failures against a 100 025-entry retry set answers in 4 ms with the
    members in the set and 3 ms without them (9.0 s and 18.9 s before) and sends no `ZSCAN`
    (11 442 and 24 925 before); the `unique` typos are 422s, `hench batches show` prints no raw
    escape and `(in 29 days)`, `.` and `..` are refused, and every JSON API error is JSON.

- 2026-11-02, review round 3, fix lane `fix3-feat-a` (unique and periodic jobs:
  `src/unique/`, `src/periodic/`, `hench-macros/src/unique.rs`; adversarially verified findings
  of the unique, periodic, cross-soak-perf and docs-usability lenses; each bug reproduced first
  by a failing test, the tests kept):
  - unique-1 (high): `OnConflict::Replace` looked for the holder among the first 16 members of
    the schedule at the score its lock recorded, and members sharing a score sort by their
    bytes, so with more than 16 jobs at one run time (a bulk push with one delay, every user's
    reminder at 09:00, or jobs of other classes at that second) most holders were never found:
    the push was a silent duplicate and the stale job ran. The lock now records its holder's
    member (`m`, written with `at` by `UNIQUE_PUSH`, the outcome scripts that park a job in the
    schedule, `UNIQUE_LOCK_OP`, `PERIODIC_FIRE` and `PERIODIC_ENQUEUE_NOW`): one `ZSCORE` finds
    it whatever shares its score. A lock without it (written by `BATCH_COMMIT` or an older
    version) is looked for among the first 100 members at its score, matched as text before
    decoding. The fake client now also refuses to replace a batch member, as Redis does. Cost:
    a copy of the job's JSON in the lock while its holder waits in the schedule (DESIGN §9g).
    Tests: `tests/unique.rs` (40 jobs of one bulk push, 30 reminders at one time, 40 jobs of
    another class at the holder's score, a lock without `m`, Redis against `Client::fake()`),
    `unique::lua` tests (300 members at the score; `UNIQUE_LOCK_OP` records and clears the
    member), `periodic::lua` tests (`FIRE` and `ENQUEUE_NOW` record it).
  - unique-3 and cross-soak-perf-2 (medium): `release_unique_lock` recorded the jid in a
    process-wide map that only the outcomes of `Success`/`Start` jobs emptied: the notes of
    `Expired` jobs (and of jobs requeued at a shutdown) stayed for 24 hours, every insert past
    10,000 scanned them all under the mutex every unique outcome took (an `Expired` job type
    releasing early fell from 23,000 to 3,500 jobs/s over 60,000 jobs, quadratically), and a
    later job reusing the jid skipped its lock work. The early release is now a flag of the
    execution's own state (`unique::EarlyRelease`, shared by the job's context and the
    processor's `Fetched`): nothing to clean up, nothing another job can see. Tests:
    `tests/unique_engine.rs` (50 `Expired` jobs reopening their window, then a `Success` job
    reusing such a jid releases its lock at success), unit tests in `unique::engine`.
  - unique-4 (low): the heartbeat's renewal worked from a snapshot taken before its round trip
    and warned about every job its script reported, including jobs acknowledged meanwhile and
    jobs that had released their lock on purpose; and a retry after `release_unique_lock`
    counted a `z` lapse at its success. The renewal now skips executions that gave their lock
    up and reports only those still running once the reply came; the copy continuing an
    execution that gave its lock up (retry, push back, reschedule, morgue) is stored with
    `"unique": false`, so it runs as the job it is, without a lock. Tests:
    `tests/logging.rs` (a job working across eight beats after releasing its lock logs no
    lapse; it failed before the fix), `tests/unique_engine.rs` (retries and push backs after an
    early release, both fetch strategies: no lock, no lapse), unit tests of the renewal's
    filtering. A job requeued by a shutdown after giving its lock up still runs as fetched
    (unique, without a lock: its success counts a lapse); the requeue moves the job's bytes
    back unchanged and is left so.
  - periodic-1 (high, verified medium): the wrapper took `croner`'s classification, which counts
    only single values as fixed-time, so lists and ranges (`30 1,13 * * *`, `15 1-3 * * *`) ran
    twice in a repeated hour and not at all in a skipped one, against the documented Vixie rule.
    Fixed-time is now Vixie's (cronie's `MIN_STAR`/`HR_STAR`: no field of seconds, minutes and
    hours starting with `*`), and for lists and ranges the wrapper drops the second pass of a
    fold and adds the instant after a gap of up to two hours in which a skipped wall clock time
    matches; the latest-tick search walks the forward sequence. Tests: `periodic::timing`
    (London, New York, Santiago, Paris, Lord Howe, ticks, latest ticks, counts, notes; a
    brute-force model of Vixie cron minute by minute; the property test now draws lists and
    ranges and Santiago).
  - periodic-2 (medium): the skip list sent to `LEASE` held the 100 most recent names, so with
    more due definitions of a newer format the poller leased, restored and leased again forever
    (about 4,000 scripts a second, no other schedule fired, no registration check). Every
    skipped name is sent now (up to 10,000; past that a skipped name handed back keeps its
    lease), and a poll yields after 1,000 unreadable definitions. Test: `tests/periodic.rs` (150
    newer definitions and a normal schedule: it fires, the poller goes idle, the others stay due).
  - periodic-3 (medium, verified low): a zone missing from this process's tzdata was "invalid"
    and parked for an hour for every process. It is now `Unreadable::UnknownZone`: skip-listed
    with its score restored, like a newer format; and a zone the system database lacks comes
    from the bundled copy (`jiff` never falls back per name). Tests: `tests/periodic.rs`,
    `periodic::timing`.
  - periodic-4 (medium): a due `next` that is not a tick of this process's timetable (other
    tzdata, changed rules, another version's DST rules, an edit by hand) was dropped: no job, no
    count, no history. It now stands for the nearest tick within three hours: enqueued in its
    place, or not when that tick was handled (`last_tick`; a `skipped` history entry), so that
    processes that disagree run each period once; warned once per definition. Parsed timetables
    are reused for five minutes at most, so tzdata updates reach running pollers. Tests:
    `periodic::plan` (both directions, handled, hand edits, deadline), `tests/periodic.rs`. The
    plan test that asserted the dropping was rewritten to assert the new reading.
  - periodic-5 (medium): after Redis went back in time (a restore, a failover to a replica that
    missed the registration) the check took the older record for a newer registration and never
    registered again. A record of another crontab older than this process's registration now
    triggers a registration (warned); a newer one is warned about once instead of a debug line.
    Crontab digests now cover the names (crontabs differing by names only had one digest). Test:
    `tests/periodic.rs` (deploy A, backup, deploy B, restore: B registers again).
  - periodic-6 (medium): validating an impossible date took 0.5 to 2 s in a release build
    (49 failing searches), blocking the caller's thread (`PeriodicSet::upsert` from a request
    handler). The day and month fields are now checked against 29 years of dates first (well
    under 250 ms in a debug build, about 0.02 s for the test's eight expressions), and a failed
    search is retried only past the zone's transitions. Test: `periodic::timing`.
  - periodic-7 (low): a paused or deleted schedule found by the poller left the due index
    through a plain `ZREM` in a later round trip, so a resume or re-creation in between lost its
    entry for good. `PERIODIC_UNINDEX` checks and removes in one step (the repair counts only
    real removals). Test: `periodic::lua` script tests.
  - docs-usability-4 (medium): `[[periodic]]` tables were built without the job type's options:
    their jobs went to `queue:default` with default retries, no uniqueness and no throttle. At
    `build`, a file schedule now takes the options of the job type the process registers for its
    class and its typed keys (`ErasedJob::dyn_keys`), under the table's keys; arguments that do
    not fit the job type are refused at build. The job type's `on_conflict` does not apply (a
    schedule's ticks drop duplicates; the file has no override). Tests: `tests/cli.rs`.
  - docs-usability-14 (low): the refusals of a job type's `on_conflict` in schedules and batch
    members now name the override (`.options(&JobOptions::new().on_conflict(OnConflict::Drop))`,
    `J::set().on_conflict(OnConflict::Drop)`), and the README's recipes say so. Tests:
    `periodic::builder`, `tests/batch.rs`.
  Touches outside the lane's modules, all minimal: `job.rs` (`ErasedJob::dyn_keys`, the
  context's early-release flag), `config.rs` (one call applying the file's job types at
  `build`), `cli.rs` (the `Settings::periodic` doc), `batch/collect.rs` (the message),
  `pro.rs`, `processor.rs` (above), `tests/{logging,cli,batch}.rs`.
  Gate at each commit: fmt, clippy `-D warnings`, the suite (78 test binaries, 1,622 tests at
  the last commit, 0 failures, 6 ignored), rustdoc `-D warnings`, `cargo +1.88 check
  --all-targets`; the new integration tests ran three more times each without a failure. For
  the integration: worktrees that share `CARGO_TARGET_DIR` and package versions reuse each
  other's artifacts as fresh (cargo compares the sources' modification times with artifacts
  another worktree built), so a gate there can check another lane's code: this lane touched its
  sources before each gate step and checked the test counts.

- 2026-11-02, review round 3, fix lane `fix3-feat-b` (rate limiting, batches, iterable jobs and
  the throttle macro; the adversarially verified findings `ratelimit-1`–`10`, `batches-1`–`3`,
  `5`–`11`, `cross-soak-perf-1`, `-6` and `docs-usability-2`, `-13`, `-15`). Each was
  reproduced by a test that failed first (kept as the regression test); DESIGN §9i item 10 and
  §9j item 9 record the decisions.
  - `ratelimit-1` (high): the gate's concurrency leases used the jid as token, so the release
    of an execution that ended (requeue, push back) could take the lease of the job's next
    execution, fetched again at once, and a second job got in. Each execution now has its own
    token (`<jid>~<16 hex>`, kept for resends after a lost reply); the API and the dashboard
    read the jid from it (`Holder::jid`, minimal edits in `hench-web` and `hench-server`).
  - `ratelimit-2`, `-3`: throttle parkings spent the limiters' reschedule budget and the other
    way round, and an iterable job paced by a per-item limiter failed after 20 refusals although
    each run made progress. `limited` now keeps `t` (throttle parkings) and `o` (limiter
    reschedules since the last progress) apart, besides `n` (both, for display); an iterable
    run that processed items before its refusal (`MadeProgress`, named in the error) starts
    `o` again.
  - `ratelimit-5`, `-6`, `-7`, `cross-soak-perf-1` (high): every refusal pushed the wait line,
    even for callers that never come back; nobody kept a place (waiters were overtaken, parked
    jobs re-queued at the end); a jitter of up to a tenth of the delay turned spaced returns
    into random arrivals; the line was spaced at 1000 ms / limit until holds were measured, and
    parks lasted at least 1 s. Now: only callers that come back take a place (parked or
    rescheduled jobs, waiters within their `max_wait`; `try_acquire` is told where the line
    ends); a place is kept through its ticket (a waiter's across polls, a job's in
    `limited.s`; a rate-limited caller a little late gets the next unit); no jitter with a
    line; concurrency spacing from a decaying average hold (`hold`), else this process's
    average for the throttle, else 25 ms doubled per parking; parks from 50 ms. Measured
    (release build, default intervals, one worker of 25 slots, `concurrency(5)`): 250 jobs of
    100 ms drain in 5.9 s instead of 52.9 s (85 % of the limit used, ideal 5.0 s; 6.3-6.5 s
    warm, was 8.3 s); 2,000 jobs of 10 ms in 6.0 s instead of 34.5 s (ideal 4.0 s); 100 jobs
    of 500 ms in 12.0 s instead of 20.9 s (ideal 10.0 s); per key (`concurrency_per_key(1)`,
    10 fresh keys × 40 jobs of 50 ms) 3.1 s instead of 42.3 s (ideal 2.0 s). Not done: handing
    a freed permit to a parked job (a per-identity index of parked jobs moved by
    `LIMIT_RELEASE` from the schedule to their queue would cross the scheduler's ownership of
    the schedule); the gap left (15-35 %) is the scheduler's round trip, and `max_wait` (98 %
    in the finding's measurement) is the documented choice for jobs that short.
  - `ratelimit-4`: a throttle lease shorter than three heartbeat intervals is refused at build
    (workers renew once per heartbeat). `ratelimit-8`: the macro refuses an inline throttle
    whose limits are all per key without `throttle_by`, and the gate warns (once a minute per
    throttle) when a keyless job names such a throttle. `ratelimit-9`: the mismatch warning is
    once per process per limit name and pair of definitions (not per key; bounded at 1,024,
    then once a minute), reworded for deploys, and also given at refusals
    (`docs-usability-15`). `ratelimit-10`: empty and NUL-holding keys are hashed like long
    ones instead of failing the push.
  - `cross-soak-perf-6`: `Throttle::stats(bool)`/`stats_ttl` (and the file's `stats`,
    `stats_ttl`); the documented cost is about 330 B per identity, index entry included.
  - `docs-usability-13`: the `throttle_by` error lists the job's arguments; the macro's
    `unique_until`, `on_conflict`, `unique_across_queues` and `unique_by` without
    `unique_for` are compile errors (every push failed at runtime, or nothing was unique), and
    the runtime message names `#[hench::job(unique_for = <seconds>)]` (minimal edits in
    `hench-macros/src/unique.rs`, `expand.rs` and `unique/mod.rs`); configuration errors no
    longer repeat their prefix, and a throttle's are about a throttle.
  - `docs-usability-15`: the README and the `Throttle`/`Limiter` docs say that throttles and
    limiters share their names; the rustdoc examples no longer collide (`stripe`,
    `stripe-api`).
  - `batches-1` (high): a workflow's `complete` fired at the first failure of a stage member,
    before the later stages existed, and never again. The overall batch is held open (`open`)
    until the commit of its last stage releases it (a cancel too); the model and the parity
    proptest know holds.
  - `docs-usability-2` (high): `hench::WorkflowNext` went to `default`, which apps naming
    their queues never poll, so workflows stalled after stage 1 without a word. It now runs in
    the queue of the first job of the stage that just succeeded (a queue some worker polls),
    or in `callback_queue`; documented in the README, the module and the `Workflow` docs.
    Not done: a warning when a callback lands in a queue no live process polls (optional in
    the finding; the new default removes the trap).
  - `batches-2`: a cancelled batch finished (its linger started) at its `complete`, so members
    waiting for a retry or in the morgue ran once the linger ended; it now finishes once
    nothing is pending. `batches-3`: the expiry refresh waited an hour, so a 1-hour batch died
    while active; the threshold is min(1 h, `ttl`/4) on the hash's remaining lifetime.
    `batches-7`: the commit ids of a `with_bid` root stay `ttl` after it finished, so a late
    re-run of its creating job writes nothing. `batches-11`: the refresh after a reopen floors
    each ancestor with the child's lifetime. `batches-10`: a revived dead member failing with
    a retry is no longer dead.
  - `batches-6`: `Workflow::with_bid` (with `commit_id`, a workflow starts once from a job that
    may run twice); the `commit_id` docs say what it does. `batches-8`: children count as 10
    jobs toward `batch_max_commit_jobs` (refused at the child's commit or the next push), and
    the batches a commit fires are dispatched 16 at a time: 1,000 empty children with two
    callbacks (the most the default cap allows) take 41 ms of Redis at most and 0.41 s in all
    (debug build), against 1.1 s and 13.9 s for the 20,000 the finding committed. `batches-9`:
    an outbox entry whose dispatch fails goes back a minute, so 100 of them no longer block
    maintenance. `batches-5`: inline commits made by jobs an inline commit runs are queued and
    run in a loop (a 100-stage inline workflow runs on a test thread's stack).
  - Found while gating: the doc examples of one crate are merged into one test binary, whose
    testing registry refused the two `charge_card` jobs of two examples (all of the crate's
    testing-client doc tests failed): the new `Workflow::with_bid` example names its own jobs.
  - Minimal edits outside the lane: `api/limits.rs`, `hench-web/src/pages/limits.rs`,
    `hench-server/src/{json_api,commands}/limits.rs` (holder jids), `testing.rs` and a test
    helper of `unique/engine.rs` (the in-memory gate's signature), `config.rs` and `cli.rs`
    (docs of `batch_max_commit_jobs`), the unique macro and message above, and the seeds of
    `hench-web/tests/limits.rs` and `hench-server/tests/limits.rs` (their refusal now comes
    from a caller waiting in the line: one that does not wait takes no place in it).
  Gate: fmt, clippy `-D warnings`, rustdoc `-D warnings` and `cargo +1.88 check --all-targets`
  pass; the full suite passed (78 test binaries, 1,639 tests, 0 failures, 6 ignored); the
  batch parity proptest also passed 3,000 cases.

- 2026-11-02, review round 3, fix lane `fix3-core` (branch `fix3-core`: the core runtime, the
  API, the docs, the examples, the soak harness and CI; adversarially verified findings plus the
  owner's directed improvements). Each fix comes with a regression test written first.
  - **directed-1** (owner-directed, high): poison pills. A job that kills the process running it
    was recovered and crashed the next process forever. Orphan recovery of a dead process now
    counts each job's recoveries (`recovered-<jid>`, 72 hours after the last one, Sidekiq Pro's
    rule; the `RECOVER` script copies, counts under `pcall`, then removes, guarded by `LPOS`,
    the job's bytes unchanged) and settles a job past `max_recoveries` (3; builder, worker
    file) like an exhausted job, in the dead process's list (`fetch/poison.rs`): batch counted
    dead first, then `SETTLE`/`SETTLE_UNIQUE` (the morgue with `hench::CrashLoop`, the unique
    lock released; `dead: false` drops it), then the recovering process's death and error
    handlers (`job recovered too many times`), the batch's events and the schedule's run record.
    Shutdown requeues and the own-list reconciliation are not counted. The soak workers set
    `max_recoveries` and the checker requires every `CrashLoop` death to follow at least that
    many executions ended by `kill -9`. Tests: `tests/poison.rs` (six, one a job that kills four
    workers in a row through `Launcher::abandon`).
  - **directed-2** (owner-directed): `orphan_check_interval` defaults to 60 s (was 5 min): a
    crashed process's jobs are back 1.5 to 2.5 minutes after the crash. Cost: one `SET NX` per
    process and minute; the winner reads `inprogress` and one `EXISTS` per member.
  - **docs-usability-5** and **-10**: the `hench::cli` docs list every key of the worker file
    with its unit and an example of the feature tables; a number written as a string is refused
    with a hint that times are numbers of seconds (TOML's message only named the type); the
    sentence about reserved tables is fixed; `periodic_lease` is a file key and a documented
    builder setting (with its bound), `max_recoveries` a file key.
  - **unique-2**: a unique job whose move client middleware stopped (the scheduler's due job,
    the API's `retry`, `add_to_queue`, `retry_all`, `add_all_to_queue`) was only removed, and
    its lock blocked every push until it expired. Such a job of a feature is now deleted as the
    API deletes one (`REMOVE_UNIQUE`; a batch member first counted dead, reason `deleted`), and
    a due job the scheduler cannot push is killed as the API kills one (`KILL_UNIQUE`, reason
    `killed`), where it used to keep both its lock and its pending count in its batch.
  - **unique-5**: moving a waiting unique job later (`SortedEntry::reschedule`, or the
    scheduler deferring by a minute a due job whose push failed) let its lock expire at the old
    time, so a duplicate was accepted while the job still waited. `RESCORE_UNIQUE` (in
    `api/sorted.rs`) moves the member and, only while the job holds its lock, extends it to
    `unique_for` beyond the new time (never shortening it) and records the new time for a
    debounce push (`at`, and the member for the unique-1 fix of `fix3-feat-a`); a lapsed lock
    is not taken back. Locks of `Expired` jobs and moves to queues are untouched, as before.
  - **unique-6**: a job type made unique only by the client's `default_job_options` got its
    digest from all its arguments, ignoring `Job::unique_key` (`UniqueLocks::find` computed the
    other digest, so it never found the lock). Typed pushes now merge the client's defaults
    under the job type's options and the push's, like the push itself. Tests:
    `tests/unique_moves.rs` (seven, with Redis and the fake client).
  - **batches-4** and **web-api-security-2**: cancelling a tree of more than 1,000 batches
    stopped with an error, and every later call walked the same first thousand, so the rest
    was never cancelled (the dashboard, the JSON API and `hench batches cancel` all failed).
    `Batches::cancel` now cancels the whole tree in one call, level by level, 500 scripts per
    pipelined round trip (`batch::scripts::cancel_each`); descendants that are gone or have
    succeeded are skipped, any other refusal is returned once the rest is cancelled and
    dispatched.
  - **batches-12**: deleting a tree of more than 10,000 batches left the rest silently.
    `Batches::delete` reads the whole tree level by level (pipelined `SMEMBERS`) and deletes
    it deepest level first (pipelined forced `BATCH_DELETE`, `delete_each`), the batch named
    last. Tests: `tests/batch_api.rs` (1,203 descendants with three levels and a deleted one;
    10,050 children).
  - **web-api-security-3**: times no clock can show panicked the batch and lock pages, the
    JSON API and the CLI (`Duration::from_secs_f64`, `SystemTime +`), and a unique push at
    `at = 1e20` left a lock without expiry. Pushes refuse an `at` after the year 9999
    (`client::MAX_AT`, 253402300799; a `spread_interval` ending later too), retries are
    scheduled no later (a `retry_in` of `Duration::MAX` is clamped, with a warning), and the
    readers (`FailedMember::next_at`, `UniqueLock` times) read such stored times as `None`.
    Tests in `tests/client.rs`, `tests/retry.rs`, `tests/batch_api.rs`, `tests/unique_api.rs`.
  - **web-api-security-8**: a limit's page read and parsed up to 50,000 scheduled jobs per view
    (about 48 MiB with jobs of 1 KB) and could refresh itself. `Limits::waiting` now has Redis
    look for `"by":"<label>"` (`SCAN_CONTAINING`, read only, 500 jobs per call) and checks the
    candidates it sends; the page is no longer Live (one boolean in `hench-web`'s
    `pages/limits.rs`, outside this lane, kept minimal). Measured with 50,000 jobs of 1 KB:
    nothing sent instead of 48 MiB, Redis CPU 0.17 s instead of 0.05 s per search (the
    matching costs Redis more than sending; a view is now explicit). Test: `tests/limits_api.rs`
    (bytes sent for two matches among 3,000 jobs; escaped labels; a look-alike argument).
  - **periodic-8**: "enqueue now" through an `Api` without a configuration (the dashboard, the
    JSON API, `hench periodic enqueue`) trimmed every history to 10 entries whatever the
    workers' `periodic_history`. Such an `Api` now keeps the history's length plus the new
    entry (at least 10, at most 1,000 unless already longer); an `Api` with a configuration
    and `with_history` keep their number. Test: `tests/periodic_api.rs`.
  - **docs-usability-1**: four README examples (batches, workflows, the limiter, the features
    together) registered `#[hench::job]` functions with `register`, which `build` refuses
    ("registered twice"). The calls are gone, and `tests/readme.rs` fails on any README
    example that registers a macro job it defines.
  - **docs-usability-3**: the README's inline throttle had a per-key limit and no key, so the
    limit never applied: it now has `throttle_by`, and the README and the macro's documentation
    say that per-key limits need a key (the macro's own refusal of such an inline throttle is
    the `fix3-feat-b` lane's).
  - **docs-usability-6**: `push_outcome` returns `Err(DuplicateJob)` for a job with
    `OnConflict::Error`, as `Client::push_outcome` said: the README and
    `JobSetter`/`JobBuilder::push_outcome` say so too.
  - **docs-usability-8**: DESIGN §4 steps 1 and 3 no longer describe the phase-0 placeholders,
    §3 says `IterableJob` forwards the feature keys, and §9h item 8 and §9k item 4 agree with
    the code: a file schedule named like a code schedule is a configuration error. That error
    now says where both come from (`in code (HenchBuilder::periodic) and in the configuration
    file ([[periodic]])`; `HenchBuilder::periodic_from_file`, test in `cli.rs`).
  - **docs-usability-9** and **cross-soak-perf-8**: the sizing paragraphs (README, DESIGN §9k)
    give the real per-execution costs (a `start` lock's release before `perform`, basic
    fetch's two lock scripts, the heartbeat's renewals) and the measured Redis CPU per job of
    each feature (15 µs plain, 77 µs throttled: about 13,000 throttled jobs a second per Redis
    core).
  - **docs-usability-11**: the crate's front page presents the four features with links, the
    `api` module names their handles, the README lists the `periodic` cargo feature (on by
    default) and tokio's `test-util` for paused clocks, the macro reference block shows the
    eight feature options, the macro's documentation table lists them, and the builder table
    has the feature methods with their defaults.
  - **docs-usability-12**: the opt-outs keep their names, with documentation aliases
    (`no_unique` finds `not_unique`, `not_throttled` finds `no_throttle`);
    `Throttle::reschedules` takes `5` as well as `Some(5)` (`impl Into<Option<u32>>`, a small
    change in `limit/types.rs`, outside this lane; test in `tests/pro_features.rs`); the README
    calls `Expired` "at most one per period" and no longer "throttling". Durations stay numbers
    of seconds in the macro and the worker's file (one unit there; the file refuses a string
    with that hint since this round), while command-line flags take `10m` for people typing
    them. Keys differ by design: a unique key is JSON (part of a digest), a throttle key a
    string (part of Redis key names).
  - **docs-usability-16**: `examples/features.rs` runs a unique job, a batch with its
    `success` callback, a per-key throttle and a schedule in one worker against a private
    `redis-server` and fails unless each did its part; CI runs it, and the README points to it
    next to the embedding examples and in "Using the features together".
  - **docs-usability-17**: the README's feature sections cite the JSON API under
    `<prefix>/api/v1/...` (with `hench web --api`); `tests/readme.rs` checks it.
  - **cross-soak-perf-3**: the `pro` soak checker checks that ticks come back after faults: a
    stretch without a tick may last 30 s (the poll interval, `unique_for`, a tick's run and a
    margin) past the recovery of the last fault in it, a fault costing at most its length and a
    lease (a lost reply of `PERIODIC_LEASE` or `FIRE`, a poller killed or stopped while it held
    the lease) and a restart that lost the data the registration check too; longer is
    `TICKS_STOPPED`, and the summary gives the longest stretch (`tick_gap_max_ms`). The soak's
    workers use a 15 s lease and registration check (the defaults are 60 s), and the three pro
    scenarios end with a minute without faults, so a schedule that stopped fails the run (unit
    tests of the model in `check_pro.rs`, run by `cargo test`). The reviewer's synthetic check,
    replayed: the tick executions removed from the logs of this branch's short `pro`,
    `pro-crash` and `pro-redis-none` runs, `soak check` fails each with `TICKS_STOPPED`; the
    runs themselves held every invariant (49 to 57 ticks, the longest stretch 7 to 16 s).
  - **cross-soak-perf-4**: the `pro` load adds client-pushed unique jobs (`SoakUniq`: six keys,
    `success` and `start`, a quarter debounced with `replace`, a tenth failing once so that a
    retry holds the lock) under a per-key throttle (one at a time per key), checked by
    `UNIQUE_ACCEPTED_WHILE_HELD`, `UNIQUE_REPLACED_RAN`, `UNIQUE_JOB_LOST` and
    `UNIQUE_RUNS_OVERLAP`; and a burst batch of 150 members 40% into the load, a backlog of
    fifty times the throttle's slots, whose drain time and slot utilization the summary
    reports. `examples/throughput_features.rs` and `ci/throughput.sh --kind` time jobs that use
    one feature each (unique, unique `start`, throttled, per-key, batch member). Deferred, with
    reasons: asserting the burst's utilization waits for the rate-limiting lane's drain fix
    (cross-soak-perf-1, `fix3-feat-b`), which this branch does not have (the integrator turns
    the measurement into a check, e.g. at least 70% busy while a backlog exists, after the
    merge); a workflow with nested batches in the soak waits for that lane's workflow fixes
    (batches-1..11 change workflow completion and idempotency), so that the soak's checks are
    written against the merged semantics; nested batches and workflows under faults stay
    covered by `tests/batch_chaos.rs` and `tests/batch*.rs` meanwhile.
  - **cross-soak-perf-5**: `check_pro` excuses `BATCH_MISSING` for batches committed before a
    restart without persistence or in the second before a `kill -9` of Redis with AOF
    (`batches_lost_with_redis_data`), as the generic checks excuse jobs; `ci/soak.sh` runs the
    features under the other faults in two new scenarios, `pro-crash` (dterm, tstp, remote
    stop and quiet, queue pauses, `CLIENT PAUSE`, `kill -9` of Redis) and `pro-redis-none`
    (restarts without persistence), in the short CI suite too. Runs on this branch (short
    durations): `pro-redis-none` 0 violations (an earlier run without the calm end excused 18
    batches as `batches_lost_with_redis_data`, which were `BATCH_MISSING` before), `pro-crash`
    0, `pro` 0; unique pushes 160 to 169 accepted per run with 60 to 92 duplicates and 10 or 11
    debounces, no overlap; the burst's slots were 21 to 26% busy while it drained (with faults
    during it; the measurement the drain fix should raise).
  - **cross-soak-perf-7**: `batch_max_commit_jobs` and the README say that a commit blocks
    Redis about 4 µs per member (some 40 ms at 10,000, 0.4 s at 100,000).
  - Notes for the integrator: outside this lane, minimal changes in `batch/scripts.rs`
    (pipelined `cancel_each`/`delete_each`), `limit/types.rs` (`Throttle::reschedules`
    signature), `hench-macros/src/lib.rs` (documentation), `hench-web/src/pages/limits.rs`
    (the identity page is no longer Live) and two `hench-web` tests (`limits.rs`, `pages.rs`).
    The `fix3-feat-b` lane's macro error mentions `.not_unique()`, which this lane keeps. Lanes
    building in a shared `CARGO_TARGET_DIR` can overwrite each other's test binaries (cargo
    names path packages relative to the workspace root, so every worktree's `hench` gets the
    same artifact names): this lane gated in a target directory of its own.

- 2026-11-02, pro features phase 2 (`docs/design/pro-features.md` §6.4): `pro-a` (unique and
  periodic jobs) then `pro-b` (rate limiting and batches) merged into `implement-hench`
  (branches kept, worktrees removed). Conflicts only in the four integrator-owned test files
  both lanes had edited to keep their gates green (`tests/pro_features.rs`, `hench-web/tests/
  {pages,features}.rs`, `hench-server/tests/features.rs`): resolved by asserting both lanes'
  features. Every wiring item of the lanes' four reports is done (DESIGN §9g–§9k), and the
  phase-0 scaffolding is gone. Found and fixed while integrating:
  - parked jobs came back at the scheduler's next random poll, so a busy throttle ran at a
    fraction of its limit (the soak's first run: a 3-slot throttle drained its backlog at
    about one job a second): the parking process now wakes its scheduler at the job's slot
    (§9j item 8);
  - the `features` of `GET /processes` were missing (X17): added;
  - `hench enqueue` failed on a dropped duplicate, `Hench::client()` ignored `unique_jobs` and
    the batch settings of the configuration, `retry: false` failure summaries had no error,
    and the testing executor reported a skipped member as an error (lane reports): fixed;
  - D14 skipped itself unless unique jobs were present, and the batch testing-mode tests
    accepted either skip behaviour: both now assert the integrated behaviour;
  - the README's workflow example had no `main`, so `ci/check-readme-examples.sh` could not
    build it: added;
  - the soak checker reported a job as lost when its worker was killed between writing it to
    the morgue and running the death handler (in-process, after the write: a `kill -9` there
    skips it, now documented in DESIGN §5): the checker recognises that case and still
    requires the job to be where its outcome put it.
  Gate: fmt, clippy `-D warnings`, rustdoc `-D warnings`, `cargo +1.88 check --all-targets`,
  `ci/check-readme-examples.sh` (which found the README's workflow example without a `main`)
  and `cargo deny` pass; the full suite passed three times in a row (78 test binaries, 1,598
  tests, 0 failures, 6 ignored; the browser tests forced on), no flaky test; `ci/soak.sh
  --short` held every invariant, `pro` included; X22: +0.1 % plain-job throughput against
  `f3da43d` (within noise).

- 2026-11-01, integration of review round 2's fix lanes: `fix2-web`, `fix2-surface` and
  `fix2-engine` merged into `implement-hench` in that order (branches kept, worktrees removed).
  The round's six review lenses (fix-regressions, soak-chaos, new-user, macros-testing-api,
  security-supply-chain, parity-gaps) sent 43 verified finding ids to the three lanes
  (new-user-4 and -15 and parity-gaps-5 and -6 were assigned to no lane and are not counted).
  Outcomes, from the lanes' reports and checked against the merged tree (every regression
  test they name is in it and passes):
  - `fix2-web`: 6 findings, 6 fixed, 0 not a bug, 0 deferred (security-supply-chain-1, -2, -3
    and -6, new-user-5, parity-gaps-4).
  - `fix2-surface`: 18 findings, 18 fixed, 0 not a bug, 0 deferred (fix-regressions-1,
    soak-chaos-3–6, new-user-3, -6, -7 and -9–12, macros-testing-api-2, -4, -5 and -7,
    security-supply-chain-4, parity-gaps-11).
  - `fix2-engine`: 19 findings, 19 fixed, 0 not a bug, 0 deferred (fix-regressions-2,
    soak-chaos-1 and -2, new-user-1, -2, -8, -13 and -14, macros-testing-api-1, -3 and -6,
    security-supply-chain-5, parity-gaps-1–3 and -7–10).
  - Total: 43 finding ids (40 distinct issues: macros-testing-api-1 and -3 are new-user-1 and
    -13 seen through another lens, and new-user-11 is security-supply-chain-3 seen from the
    README: the web lane fixed the code, the surface lane the README), 43 fixed, 0 not a bug,
    0 deferred. Deferred list: none. What the lanes left out of their fixes on purpose is in
    the gaps above, not deferred: TLS through Sentinel, a `hench dump` subcommand and TLS
    options for the `hench` command, the tasks' stacks in dumps (parity-gaps-1–3), and a
    per-client rate limit for `hench web` (security-supply-chain-1).

  Conflicts were in documentation and one doc comment only: the README's `hench enqueue`
  example keeps the surface lane's class name (`monthly_report`, the registered name) and the
  web lane's note that the queue is `default` unless given, and its differences from Sidekiq
  keep the web lane's host check and the engine lane's `SIGTTIN`; `JobBuilder` keeps the
  engine's `spread_interval` above the surface's push docs; DESIGN §3 keeps the macro's own
  errors for borrowed signatures (surface) with `do_not_recommend` (engine), and `CaughtPanic`
  (surface) with the execution boundary (engine); STATUS keeps every lane's entries and gaps.
  Git combined the rest (`error.rs`, `keys.rs`, `testing.rs`, `job.rs`, `api/mod.rs`, the
  `hench` manifest, `tests/{job,logging,testing}.rs`, `hench-web/src/lib.rs`) without
  conflicts, and the merged code needed no change. Checked across lanes: an iterable job's
  resumed panic keeps its origin (`CaughtPanic`) through the engine's execution boundary,
  which now ends its stored frames (the surface's 80 panicking jobs on real workers pass);
  `Testing::from_builder` still needs no Redis with the engine's `build`; the engine's typeck
  expectations (`error_types`, `argument_types`, `return_type`) hold with the surface's
  restructured macro; the soak harness (surface) runs against the engine's fixes and samples
  the web lane's `hench web`.

  Two integration fixes, both found by running the soak harness on the merged tree:
  - CI: the `Soak` workflow built the harness and the `hench` binary with one `cargo build -p
    hench --example soak -p hench-server`, which builds the example alone, so `HENCH_BIN`
    named a file that did not exist and every scenario failed at once ("No such file or
    directory"): two builds now, and `ci/soak.sh` refuses a `HENCH_BIN` that is not an
    executable, saying how to build it.
  - The harness: `redis-none` reported `STALE_PROCESSES_LEFT` for a worker killed 116 s before
    the drain ended. Not an engine bug: soak-chaos-1's fix prunes a dead process about a minute
    after its heartbeat expired (the key lives up to 60 s after the kill, then the cluster
    cleans up once per 60 s lock, on a scheduler's next poll); in that run the first sample
    without the key came 59 s after the kill and the workers were stopped 57 s later, while
    kills earlier in a run were pruned (`proxy`: 72 s after the kill; `stale-sched`: seven, in
    two passes 60 s apart). The harness stopped the workers as soon as the jobs drained, so a
    kill in the last two minutes or so left a member that nothing had had time to prune. The
    drain now also waits until `processes` lists the running workers only (ending at the prune;
    it gives up after 4 minutes: the key's 60 s, the lock's 60 s and a scheduler's next poll,
    which a crash loop can push to 120 s since killed workers count until their keys expire),
    and the check, still strict, names each member's worker and how it ended. Checked: a dead
    member added to `processes` as the drain went quiet held the drain 10 s, until a scheduler
    pruned it, and the scenario passed; two runs with a `kill -9` late in the load passed too.
    The scheduler's comment that dead processes stay listed "about a minute at most" now says
    about a minute after the heartbeat expired, as DESIGN and the API docs do.

  Also: the gaps' orphan delay includes `orphan_grace`, and paragraphs of DESIGN and STATUS
  left ragged by the round are reflowed (words unchanged). The merged tree passes the gate:
  fmt, clippy `-D warnings`, the suite (983 tests, 4 ignored: the previous 891 plus the lanes'
  39, 19 and 34; the Playwright browser suite ran), rustdoc `-D warnings`, `cargo +1.88 check
  --all-targets` and `ci/check-readme-examples.sh`; `cargo deny check` passes too. The whole
  suite ran four times in a row without a failure, and once more with the integration fixes: no
  flake to chase. `ci/soak.sh --short` (crash, proxy, redis-none and stale-sched, `hench web`
  sampled; 27,541 pushes, 32,157 executions) then held every invariant: exact stats, every
  extra run with a documented cause, no concurrent run, `processes` clean. Its `redis-none` met
  the failing timing again (the jobs drained 111 s after a `kill -9`, 4.7 s before a scheduler
  pruned that worker) and the drain waited those 6 s; `stale-sched` ran once more with the
  final harness (seven kills, pruned in two passes) and held too.

- 2026-11-01, review round 2, fix lane `fix2-web` (`crates/hench-web`, `crates/hench-server`;
  adversarially verified findings of the new-user, security-supply-chain and parity-gaps
  lenses; each bug reproduced first by a failing test, the tests kept):
  - security-supply-chain-1 and -2: `axum::serve` gave hyper no timer (so no request-head
    timeout) and no connection limit, and its graceful shutdown waited for every connection:
    a client sending half a request head held its connection forever, about a thousand of them
    exhausted the open-file limit (pages, the JSON API and `/healthz` offline), and one of them
    kept a stopping server alive whatever the signals. `hench web` now runs its own accept
    loop (`connections.rs`, hyper's HTTP/1 with a timer): request heads and idle keep-alive
    connections 10 s, bodies 30 s once the host and credentials are checked (408; 413 over
    2 MiB), responses the client stops reading 30 s without progress, heads 64 KiB (431), at
    most `--max-connections` (512) connections (accepting pauses instead of failing with
    `EMFILE`); the first signal closes idle connections and gives requests in flight 10 s, a
    second one stops at once. Tests: `tests/connections.rs` (8, in process with short limits:
    unfinished and silent heads, idle keep-alive, stalled and oversized bodies, bodies of
    unauthenticated requests left unread, the connection limit, requests in flight finishing,
    the shutdown timeout, a second signal), the reviewers' reproductions against the binary in
    `tests/cli.rs` (a head that never ends is closed; SIGTERM then SIGINT stop it with a
    request unfinished), a unit test of the stalled-write timeout (paused clock). New direct
    dependencies of `hench-server`: `hyper` and `hyper-util` (already built for axum).
  - security-supply-chain-3 (a duplicate of new-user-11): only `hench web` checked host names,
    so the dashboard embedded as in README §5 or `examples/embedded.rs` (no authentication, on
    127.0.0.1) could be driven by a DNS-rebound page (dead set emptied, queues paused). The
    check moved into `hench-web` and is on by default: `HenchWeb::allowed_hosts(AllowedHosts)`
    (localhost, `*.localhost`, IP addresses, plus names; `AllowedHosts::any()` for dashboards
    behind authentication a rebound page cannot use), `hench_web::require_host` for routes next
    to the dashboard; `hench web` uses both (its refusal still names `--allowed-host`). README
    §5 and the example are protected without a change; the README's dashboard example names
    its host. A behaviour change for embedders: a dashboard reached under a host name must
    now be told that name (`allowed_hosts`), or allow every name behind authentication
    (`AllowedHosts::any()`); the refusal says so. Three existing tests that send
    `Host: dash.example` now declare that name.
    Tests: `tests/security.rs` (the reviewers' reproduction: a rebound page is refused, reads
    no token, changes nothing; local names, allowed names, `any`, a custom refusal,
    `require_host`) and unit tests of `hench-web`'s `hosts.rs` (those of `hench-server` stay
    and now test its wrapper).
  - security-supply-chain-6: `--prefix /healthz` passed validation and the router panicked
    (exit 101): the prefix is refused with a usage error, also for arguments built in code.
  - new-user-5: `hench enqueue --help` promised the job type's own queue, which `hench` (no
    registry of job types) cannot know: the help and the README say the queue is `default` and
    the job type's options do not apply, and `hench enqueue` without `--queue` warns; the JSON
    API's docs say the same.
  - parity-gaps-4: the only extension point was `HenchWeb::tab`, whose docs said to add routes
    to the dashboard's router: those came after its guard (no read-only or cross-site refusal,
    no security headers: a cross-site POST ran an extension's action) and could not render in
    the layout. Implemented Sidekiq's web extensions: `HenchWeb::routes` merges the host's
    routes before the guard; their handlers extract `hench_web::Page` (links, CSRF token and
    field, read-only, the API, `render(title, html)` in the layout with the tab highlighted
    and the totals; `escape_html`) and `hench_web::Form` (the built-in actions' token check);
    `HenchWeb::job_info` adds rows to the job pages (Sidekiq's `custom_job_info_rows`; links
    limited to paths and http(s) URLs; a panicking callback loses its rows, not the page).
    Tests: `tests/extensions.rs` (6: the reviewer's reproduction, read-only or not, now
    refused; the layout, navigation, totals and token of a host page; the token check of
    `Form`; `webext_test`'s routes with a path and a query parameter under a parameterized
    mount; `Page`/`Form` outside the dashboard; `web_test`'s "displays custom job info" on
    the three job pages, with an unsafe link and a panicking callback), unit tests (links,
    escaping).

  The new regression tests fail on the previous tree (checked in a scratch worktree at
  `fbcba4d`; those of the new APIs do not compile there). Checked by hand with the
  reviewers' scripts: 1050 half-open requests to the binary under `ulimit -n 1024` no longer
  take it down (no `EMFILE`; `/healthz`, authenticated pages and the API answer during and
  after; idle sockets are closed after 10 s), 600 unfinished 300 KB heads cost no memory
  (431), SIGTERM then SIGINT stop it 2 s after the first signal with a request unfinished,
  and the Playwright DNS-rebinding page gets 403 from `examples/embedded.rs` (dead set and
  queues unchanged). The full gate passes on both commits of the lane (fmt, clippy
  `-D warnings`, the suite — 918 then 930 tests, 4 ignored, with the Playwright browser
  suite —, rustdoc `-D warnings`, `cargo +1.88 check --all-targets`).

- 2026-11-01, surface fix round 2 (branch `fix2-surface`: iterable jobs, the macro, the worker
  CLI, testing and API docs, README, CI; verified round-2 findings of the fix-regressions,
  soak-chaos, new-user, macros-testing-api, security-supply-chain and parity-gaps reviews;
  every bug reproduced first by a test that failed on the previous tree, the tests kept):
  - fix-regressions-1: an iterable job that enables `backtrace` and panics in
    `each_iteration` (or its enumerator) stored an empty backtrace, or another job's panic
    site (64 of 80 jobs on a 4-thread runtime, 40 of 40 on one thread): the round-1 cursor
    save awaited Redis between catching the panic and resuming it, while the panic hook's
    record stayed in the panicking thread's local, where other panics replaced or took it.
    `error::CaughtPanic` carries the record across the await (taken after the catch, put back
    before the resume, within the poll that reaches `catch_panic`). Tests: two iterable job
    types panicking on different lines on real workers, 4 threads × 16 processors and one
    thread × 16 (every stored backtrace starts with its own `panicked at` line; the
    reviewer's eight scenarios pass too).
  - soak-chaos-4: the shutdown and time-limit checks ran before the enumerator was asked for
    the next item, so an execution whose time ran out (or whose process started stopping)
    during the last item was interrupted and went to the end of its queue for an execution
    with nothing to do. The item is now taken first (none left: complete; else the checks,
    and the item is dropped for the next execution), like Sidekiq. Test: the time running out
    and a shutdown starting during the last item complete; one item earlier still
    interrupts. (`inline_clients_run_interrupted_jobs_again_without_nesting` counted the old
    extra execution's push; it now expects one push fewer.)
  - soak-chaos-3: once the iteration was over, a failing `UNLINK it-<jid>` (a timeout during a
    Redis stall, applied anyway) failed the completed job, whose retry found no state and
    redid every item and `on_complete`. The deletion is now housekeeping (a failure is logged;
    the state expires), and the state records `done` (how the iteration ended) before the
    finishing hooks, so that a retry after a failing hook, or a recovery after a crash before
    the acknowledgement, runs only `on_cancel`/`on_stop`/`on_complete` again instead of the
    items since the last checkpoint. Tests: a 1.5 s `CLIENT PAUSE ALL` from `on_complete`
    with a 300 ms response timeout (directly and on a worker: no failure reported, no retry,
    every item once), failing `on_complete`/`on_stop` (memory and Redis states).
  - macros-testing-api-2 and -5: the generated `impl Job` carried `#[allow(deprecated)]`
    around the job's body (no deprecation warning from any job), and the helpers
    `#[allow(dead_code, deprecated)]`, which crates forbidding those lints refuse (E0453). The
    impl, the helpers and the registration now sit in one `const _` block that references
    the helpers (used, so no `allow(dead_code)`); only deprecated jobs get an
    `allow(deprecated)`, on an alias they are named through. Tests: `ui/typeck/
    deprecated_in_body.rs` (`#![deny(deprecated)]`: calls in a job and in a deprecated job
    are errors), `ui/pass/forbidden_lints.rs` (`#![forbid(dead_code, deprecated)]`), a
    deprecated job running in `tests/job.rs`, unit tests of the expansion. Left as documented:
    the struct's `allow(non_camel_case_types)` (its name is the function's).
  - macros-testing-api-4: `clippy::unused_async` (pedantic) fired on every job without
    `.await`, and its advice (drop `async`) is refused by the macro: the body gets
    `#[allow(clippy::unused_async)]`. Test: a module of `tests/job.rs` warns on the lint, so
    the workspace's `clippy -D warnings` fails without the allow.
  - macros-testing-api-7: `Cow<'_, str>`, `Cow<str>`, `'a` in argument types, `-> impl Trait`,
    `ctx: &'static JobContext` and a context through a type alias got compiler errors about
    generated code (or the macro's "must be owned" on the context): the macro reports lifetimes
    other than `'static`, `Cow` without its lifetime, `impl Trait` in the return type and a
    lifetime on the context, and its error for a borrowed first parameter named like a
    context says that the context must be spelled `&JobContext`. Test:
    `ui/fail/borrowed_arguments.rs` (elided lifetimes other than `Cow`'s stay the compiler's).
  - new-user-9: the README's systemd unit had no `WorkingDirectory=`, so the default
    `config/hench.toml` (relative to the current directory) was not found from `/` and
    `-e production` silently selected nothing: the worker ran with the program's settings,
    without the file's queues and capsules. `Settings::warnings` now holds what selects
    nothing (an environment from `-e` or `APP_ENV` without a file read, or without a section
    in it; a section named like the environment but for its case), and `Settings::apply` logs
    the file and environment in use (info) and the warnings. The README unit sets
    `WorkingDirectory=` and the docs say that the default file is relative to the current
    directory. Tests: unit tests of the warnings, `tests/cli.rs` (from the crate's directory,
    which has no `config/hench.toml`), `tests/logging.rs` (the logged lines).
  - new-user-10: only the selected environment's section was checked, and any unknown
    top-level table was taken for an environment's: a typo in `[production]` passed every
    check in development, and `[capsule.serial]` (for `[capsules.serial]`) silently dropped the
    capsule. Every section is now checked when the file is read (errors name the section), and
    a table inside an environment section other than `capsules` is an error, with a hint when
    the section's name is a setting's misspelled (`did you mean [capsules.serial]?`). Tests:
    unit tests and `tests/cli.rs` (both cases accepted by the previous code).
  - parity-gaps-11: the configuration file refused `average_scheduled_poll_interval`, which
    Sidekiq reads from its file: it is a setting (float seconds, positive) of the file, the
    environment sections and `Settings`, applied to the builder. Tests: unit tests (values,
    refusals) and `tests/cli.rs` (applied, also from a section).
  - new-user-7: `Testing::from_hench` was advertised as running jobs "without Redis", but a
    `Hench` comes from `build()`, which connects (7.5 s, then an error, where no Redis runs):
    `Testing::from_builder(builder)` builds with `RedisPool::detached()` instead, for tests
    that share the program's builder; the docs of `from_hench` and the README say which to use.
    Test: `tests/testing.rs` (a builder pointing at a closed port gives a working testing client
    within a second; an invalid configuration fails as `build` would).
  - soak-chaos-5: a push whose reply timed out returned an error although Redis had often run
    it (2,156 of 2,858 such jobs ran in the soak), and nothing said so: `Client`'s docs have a
    "When a push fails" section (what is certain, what is not, the client's own retry after a
    dropped connection, idempotent jobs with jids of the caller's own), linked from every push
    method, the macro's `perform_async` and the README's "Enqueuing jobs" and "Delivery
    guarantees". (Documentation; the behaviour is DESIGN §4's.)
  - new-user-3: the README's worker called `tracing_subscriber::fmt::init()`, which with the
    `env-filter` feature its Quick start enables logs only errors unless `RUST_LOG` is set, so
    failing jobs went unseen: the top example and step 4 log at `info` unless `RUST_LOG` says
    otherwise, and the Logging section explains it. (Documentation; the README examples build
    with the listed dependencies, `ci/check-readme-examples.sh`.)
  - new-user-6: the README's `hench enqueue MonthlyReport` did not name the README's
    `monthly_report` job: fixed, with a note that the class is the job's registered name.
  - new-user-11: the README's embedding example served the dashboard without authentication
    and without saying so: it now says that the dashboard has none and points at the
    dashboard section. (The dashboard's own refusal of unknown `Host` names, which stops the
    DNS-rebinding attack, is the web lane's security-supply-chain-3.)
  - new-user-12: the Quick start lists `serde_json` and `tracing` (used by rustdoc examples and
    raw pushes) and `futures` (the data API's streams); the `hench::api` docs show how to read
    a stream (`std::pin::pin!` and `StreamExt::next`), and `Queue::jobs`, `JobSet::entries` and
    `JobSet::scan` say so; `hench-web`'s authentication example imports
    `axum::http::StatusCode` (the one line changed outside this lane's crates).
  - security-supply-chain-4: CI had no dependency audit (and cargo-deny's defaults already
    failed on basic-toml, unmaintained, RUSTSEC-2026-0374), no `permissions:` block, actions
    on movable tags, and the README installed `hench` without `--locked`. Now: `deny.toml`
    (advisories, the permissive licenses in use, bans, crates.io only; RUSTSEC-2026-0374
    accepted with its reason: askama 0.16 needs it for file templates, at compile time) and a
    `deny` job (cargo-deny-action, cargo-deny 0.20.2, which passes locally), `permissions:
    contents: read`, every action pinned to a commit SHA (version in a comment) with
    `.github/dependabot.yml` proposing updates, `cargo install hench-server --locked`.
  - soak-chaos-6: the reviewer's multi-process soak and chaos harness is
    `crates/hench/examples/soak` (Unix; `clap` and tokio's `process`/`io-util` added to the
    dev-dependencies), with `ci/soak.sh` (the suite, `--short` for CI; exit 1 on any violated
    invariant) and a `Soak` workflow (nightly and by hand; reports kept as artifacts). Ported
    with fixes: run directories under `target/soak/` (made absolute: redis-server opened its
    log relative to its own directory), a non-zero exit status on violations, no signal to pid
    0 (a process group), no unused fields. Checked here: a 45 s crash scenario (2,185 pushes,
    2,529 executions, `kill -9`/SIGTERM/SIGTSTP/remote quiet) reports only
    STALE_PROCESSES_LEFT (soak-chaos-1, the engine lane's), and `ci/soak.sh --short
    sched-control` (3,180 pushes, exact stats) none. The per-finding regression tests stay in
    the suite: the harness is for nightly and manual runs.

  Outcome: 18 findings, 18 fixed, 0 not a bug, 0 deferred. Outside this lane's files, minimal:
  `error.rs` (`CaughtPanic`), `keys.rs` (`ITERATION_DONE`), `client.rs` and `job.rs` (push
  docs), `hench-web/src/lib.rs` (one doc import). The full gate passes on every commit: fmt,
  clippy `-D warnings`, the suite (910 tests, 4 ignored), rustdoc `-D warnings`,
  `cargo +1.88 check --all-targets`; also `ci/check-readme-examples.sh` and
  `cargo deny check`.

- 2026-11-01, round 2, engine fix lane (`fix2-engine`; adversarially verified findings of the
  fix-regressions, soak-chaos, new-user, macros-testing-api, security-supply-chain and
  parity-gaps lenses). Each bug was reproduced by a failing test first (checked against the
  previous code), and the tests are kept.
  - fix-regressions-2: the liveness thread kept its first connection forever; after a
    failover that keeps clients connected (`REPLICAOF`, Redis's `FAILOVER`) every refresh
    answered `READONLY`, logged at debug level only, and the key on the new primary was no
    longer refreshed while a job blocked the runtime. A failed refresh now replaces the
    connection and is tried again at once; failures lasting 20 s are warned about (once a
    minute). Tests: `tests/launcher.rs` (`REPLICAOF` and coordinated `FAILOVER` behind a
    switchable proxy, `support::SwitchProxy`).
  - soak-chaos-2: after Redis lost its keys (restart without persistence, old snapshot,
    `FLUSHALL`), an orphan check before a live process's next beat took it for dead and ran its
    in-progress jobs a second time, concurrently. An owner found without a heartbeat key is now
    looked at again after `orphan_grace` (new setting, 30 s, at least twice the heartbeat
    interval), and the liveness thread re-creates a lost key (`liveness` field; the heartbeat
    keys its `heartbeat` event off `info`). Tests: `tests/chaos.rs` (restart, then a process
    starting before the other's next beat: the job runs once), `tests/fetch.rs` (the grace),
    `tests/launcher.rs` (the key re-created while a job blocks the runtime). Found on the way:
    `start()` of a `Hench` built before a Redis restart failed on its version check (`INFO`
    not retried after a dropped connection); retried once now (`tests/launcher.rs`).
  - soak-chaos-1: dead processes were pruned only once, when a process's scheduler started,
    so processes that died while others started (a crash loop) stayed in `processes` for good,
    inflating `Stats::processes_size` (dashboard `/stats`, `/api/v1/stats`) and slowing every
    scheduler (poll interval × process count). Schedulers now try the cleanup before every
    poll (its lock keeps it to once a minute cluster-wide), and both the poll interval and
    `processes_size` count live processes only (`api::stats.rs` and `api::process.rs`, small
    changes outside the engine files). The dashboard and the JSON API need no change: they read
    those counts. Tests: `tests/scheduled.rs`, `tests/api.rs`.
  - new-user-1 / macros-testing-api-1 (one root cause): a worker binary that never refers to
    the library defining its jobs links none of them (rustc drops unused crates), silently. The
    README, the `job` attribute's and `Registry::from_inventory`'s docs say so and give the
    fix (`use my_jobs as _;`); the `starting` log lists the jobs, a process without any warns,
    and a worker's unknown-job error names the jobs it knows and that cause (`tests/logging.rs`,
    `tests/processor.rs`).
  - new-user-2: registering a `#[hench::job]` explicitly too failed with "duplicate job name".
    The build error stays (the documented contract, pinned by
    `manual_and_inventory_jobs_must_not_share_a_name`; testing clients deliberately let an
    explicit job shadow one, DESIGN §8), but its message now says the job registers itself
    and what to do, and real name clashes name both types (`ErasedJob::dyn_type_name`, a new
    provided method) (`tests/job.rs`).
  - new-user-8: the first connection retried permanent errors (wrong password, unknown
    database) for 7.5 s and nothing said which URL was tried. `RedisPool::from_config` (and
    `build`) logs the URL and where it came from (`REDIS_URL`, the default...) like Sidekiq and
    fails at once on errors no retry fixes (`tests/redis.rs`). The error itself stays
    `Error::Redis` (pinned by `build_fails_when_redis_is_unreachable`).
  - new-user-13 / macros-testing-api-3: `Result<(), String>` and `Box<dyn Error (+ Send +
    Sync)>` returns got anyhow's nested-bound errors ("size cannot be known"); the `JobReturn`
    impl is `#[diagnostic::do_not_recommend]` now, so they get the macro's message and a note
    on the conversions to use (`ui/typeck/error_types.rs`; `return_type.stderr` and
    `argument_types.stderr` re-recorded: the same errors with the note).
  - new-user-14: `Error::Redis`, `Json` and `Io` no longer expose the error they print as their
    `source()` (`hench stats` said the cause three times); testing clients no longer prefix
    registry errors twice (`testing.rs`, a two-line change in the surface lane's file);
    `Config`/`InvalidJob` messages are shown without their trailing line break (TOML's). The
    README's `Box<dyn Error>` mains keep Rust's standard `Debug` report.
  - macros-testing-api-6: `perform_inline` returned `hench::Interrupted` from an iterable job
    whose run time was up, after partial work, with a message promising the job "will
    continue". It stays one execution (Sidekiq's `perform_inline` too; `tests/iterable.rs`
    pins it), as the finding's documented alternative allows: the error's message says nothing runs
    the job again and names the inline testing client, which runs it to the end; the method's
    docs say so (`tests/job.rs`).
  - security-supply-chain-5: `RedisConfig`'s derived `Debug` printed the password; it prints
    the redacted URL (unit test).
  - parity-gaps-10: typed bulk pushes could neither spread their jobs nor schedule each one
    (a raw `BulkPush` could, but dropped the job type's queue and retries):
    `JobSetter`/`JobBuilder::spread_interval` and `perform_bulk_at` (`tests/client.rs`).
  - parity-gaps-1: no Redis Sentinel: after a failover, clients and workers configured with
    the old primary's address failed every write with `READONLY` for good.
    `redis+sentinel://` URLs (`redis/sentinel.rs`): the pool asks the sentinels for the
    primary (`SENTINEL GET-MASTER-ADDR-BY-NAME`, then `ROLE` on the server named), and asks
    again after a failover error, a lost connection, and for every dedicated connection. The
    lookup is hench's own (a few commands), not redis-rs's `sentinel` feature, whose lookup
    (`SENTINEL MASTERS`) still names the old primary during a failover and adds dependencies.
    TLS through Sentinel is refused for now. Tests: `tests/sentinel.rs` (a real primary,
    replica and sentinel: pushes and workers follow `SENTINEL FAILOVER`; about 7 s) and unit
    tests of the URLs and their redaction.
  - parity-gaps-2: SIGTTIN (Sidekiq's thread-dump signal) kept its default action and
    stopped the whole process, liveness thread included, so another process recovered its
    jobs and ran them a second time. `run()` handles it, and a remote `TTIN`
    (`api::Process::dump`, new): the process logs its busy slots (jid, class, queue, run
    time) and goes on (`tests/signals.rs`, `tests/logging.rs`).
  - parity-gaps-3: no equivalent of `ssl_params: { verify_mode: VERIFY_NONE }`, so a
    self-signed managed Redis (Heroku) was unreachable. `TlsConfig::danger_accept_invalid_certs`
    and `danger_accept_invalid_hostnames`, and the `tls` feature enables redis-rs's
    `tls-rustls-insecure` so that `rediss://...#insecure` works (for the `hench` command
    too); either logs a warning (`tests/tls.rs`). CA and client-certificate options for the
    `hench` command are left to hench-server.
  - parity-gaps-7: payload keys could not reach a job's log lines (a span's fields are fixed
    when it is created). `HenchBuilder::logged_job_attributes` records the listed keys the
    job has in the span's new `attrs` field, as a JSON object (`job_logger.rs` unit test,
    `tests/logging.rs`).
  - parity-gaps-8: about 95% of a stored backtrace was hench's and tokio's frames. Stored
    frames now end where hench called the failing code, the job or a server middleware
    (`error::hench_execution_boundary`, a never-inlined frame; checked in a release build
    with and without line tables), and `HenchBuilder::backtrace_cleaner` (Sidekiq's) filters
    them; a panicking cleaner is reported and the lines kept (`tests/retry.rs`, unit tests).
  - parity-gaps-9: a startup callback could not stop the start (`LifecycleCallback` returns
    nothing; a panic was reported and the process took jobs anyway).
    `HenchBuilder::on_startup_try` adds fallible checks, and a panicking `startup` callback
    stops the start too, as Sidekiq's re-raising startup event does: `start`/`run`/`run_until`
    return the new `Error::Startup` before the process registers or fetches
    (`tests/launcher.rs`). `Config::fire_event` itself still reports and goes on (its unit
    test is unchanged).

  Outcomes: 19 finding ids (17 distinct issues: macros-testing-api-1 and -3 are new-user-1
  and -13 seen through another lens), 19 fixed, 0 not a bug, 0 deferred (fix-regressions-2,
  soak-chaos-1 and -2, new-user-1, -2, -8, -13 and -14, macros-testing-api-1, -3 and -6,
  security-supply-chain-5, parity-gaps-1, -2, -3, -7, -8, -9 and -10). Files outside the
  engine's own touched: `api/process.rs` and `api/stats.rs` (live process counts,
  `Process::dump`), `testing.rs` (two lines), `lib.rs`, and new `tests/sentinel.rs`.

- 2026-11-01, integration of the review round's fix lanes: `fix-web`, `fix-surface` and
  `fix-engine` merged into `implement-hench` in that order (branches kept, worktrees removed).
  The round's eight review lenses (web-security, web-functional, tests, features, api-docs,
  engine-semantics, concurrency-safety, redis-performance) sent 69 verified findings to the
  three lanes (findings refuted when verified, such as api-docs-7, went to none). Outcomes,
  from the lanes' reports and checked against the merged tree (every regression test they
  name is in it and passes):
  - `fix-web`: 22 findings, 22 fixed, 0 not a bug, 0 deferred (web-security-1–7,
    web-functional-1–13 and -15, tests-9).
  - `fix-surface`: 11 findings, 11 fixed, 0 not a bug, 0 deferred (features-1 and -4–7,
    api-docs-3, -8, -10 and -11, web-security-8, web-functional-14).
  - `fix-engine`: 36 findings, 35 fixed, 0 not a bug, 1 deferred (engine-semantics-1–5,
    concurrency-safety-1–6, redis-performance-1, -2, -4 and -5, api-docs-1, -2, -4–6 and -9,
    features-2 and -3, tests-1–8 and -10–13 fixed; redis-performance-3 deferred).
  - Total: 69 findings, 68 fixed, 0 not a bug, 1 deferred. Deferred list: redis-performance-3
    (the cost of the fetch's per-call Lua script; it needs a design decision, see "Deferred
    findings" above).

  Git combined the files two lanes changed (`client.rs`, `options.rs`, `testing.rs`,
  `stats.rs`, `launcher.rs`, `lib.rs`, `tests/client.rs`, `tests/metrics.rs`, `ci.yml`) without
  conflicts; conflicts were only in the README (the `hench web` example keeps fix-web's
  `--allowed-host` and fix-surface's `HENCH_WEB_PASSWORD`), DESIGN §5 (the heartbeat paragraph
  keeps fix-engine's liveness thread and shared work-state bytes with fix-surface's counter
  flushes) and STATUS (every lane's entries kept; their claims corrected where the merged
  tree says otherwise: the engine round's last commit brought its suite to 835 tests, not
  833, and three of the surface round's fixes are documentation, not test-first). One
  integration fix: `tests/embedding.rs` (fix-surface) starts a `redis-server` but was missing
  from fix-engine's `required-features = ["test-util"]` list of such targets. The merged tree
  passes the gate: fmt, clippy `-D warnings`, the suite (891 tests, 4 ignored; the Playwright
  browser suite ran, Node, Playwright and Chromium being installed here), rustdoc
  `-D warnings`, `cargo +1.88 check --all-targets`, and `ci/check-readme-examples.sh`. The
  whole suite then ran three more times in a row without a failure (four runs in all), and
  once more at 32 test threads (`hench`, then `hench-web` with `hench-server`), also without
  one: no flake to chase. The round's review, verification and fix builds had left 15 GB of
  stale artifacts in the shared target directory (`cargo clean -p` of the workspace crates
  freed them).

- 2026-11-01, web lane round 2 (fix lane `fix-web`, from the web-security, web-functional and
  tests reviews; every bug reproduced by a failing test first, the tests kept; tests-9 asked
  for a missing test):
  `hench web` checked no `Host`, so a page on a host name rebound to 127.0.0.1 passed every
  same-origin check and drove the default loopback dashboard and API (web-security-1): without
  credentials it now answers only `localhost`, `*.localhost` and IP addresses, plus
  `--allowed-host`/`HENCH_WEB_ALLOWED_HOSTS` names (403 otherwise; `/healthz` exempt; with
  credentials every name unless some are listed). Times beyond chrono's calendar (an `at` of
  1e13, or in microseconds) panicked every page that showed them, and `hench processes`
  (web-security-2): the formatters are total (`Unix time N`, no `datetime`), the JSON API and
  `hench enqueue` refuse times at or after the year 10000. Set filters ran the viewer's text
  as a `ZSCAN MATCH` glob (`?`, `[..]` live: 13 s of Redis CPU for one request) over the whole
  set, kept an arbitrary 500 matches, and the queue filter said "No jobs match" after
  searching only 50 000 jobs; filtered queue rows were numbered 1, 2, 3… (web-security-3,
  web-functional-5, -6): one search for queues and sets (literal text, `*` the only wildcard,
  matched in process, list order, 50 000 jobs, 500 matches) whose summary says what was
  searched, rows keep their place in line. The `SameSite=Strict` token cookie was replaced by
  every cross-site link, breaking the forms of open tabs (web-security-4): it is `Lax`, and
  refusals set no cookie. Queue rows carried their job's whole JSON (10 MB pages for 200 KB
  jobs; CRLF-normalized by browsers, so pretty-printed jobs could never be deleted:
  web-security-5, web-functional-13): rows carry `"{index}-{sha1}"` and the server finds the
  exact JSON near that index. `hench queues|stats|processes` printed names raw (terminal
  escapes, forged rows; web-security-6): control characters are spelled out, and the JSON API
  refuses them in names. Queues with control characters were listed but every link and button
  answered 404 (web-security-7): any name is accepted and shown escaped. "Every job" actions
  under a filter acted on the whole set after a "this list" confirmation (web-functional-1):
  hidden under a filter, confirmations name the set and its size. `<prefix>/` was an empty 404
  (web-functional-2): `hench web` mounts with `nest_service`, the docs recommend it. A paused
  queue vanished from every listing once emptied (web-functional-3): `Stats::queue_summaries`
  (in `crates/hench/src/api/stats.rs`, the one change outside the web crates) lists paused
  queues too. Deleting the last rows of a page showed "This queue is empty." with jobs left
  (web-functional-4): such pages show the last page. The refresh slider (2–20 s) disagreed
  with the interval in use (web-functional-7): 1–60 s, whole seconds. The metrics refresh
  closed table views and dropped the focus, and their table ignored the ticked classes
  (web-functional-8), and Live list refresh dropped the focus of links and buttons
  (web-functional-9): both wait while the viewer is busy in the content, and `metrics.js`
  rebuilds the table from the ticked classes. Every poll was announced to screen readers
  (web-functional-10): the status line is no live region; a failing refresh is announced once.
  `overflow-wrap: anywhere` broke names every few letters at 390 px (web-functional-11): names
  break only between words, argument and error columns keep a minimum width. With Redis down
  the server waited 7.5 s to listen, `/healthz` took 7.5 s and pages 15 s (web-functional-12):
  it listens at once, makes one reconnection attempt per command, and `/healthz` and `HEAD /`
  answer within a second. Muted labels were 4.44:1 on the secondary surface
  (web-functional-15): `--muted` is darker (4.5:1 on every light surface). Dashboard retries
  were not checked to lower the retry count (tests-9): now they are, per route. New suites:
  `crates/hench-web/tests/filters.rs` (5) and `tests/browser.rs` (4 Playwright scripts in
  `tests/browser/`, run with Node when Playwright and its Chromium are installed: live refresh
  focus, the slider and the status line, the metrics refresh, phone-width layout and
  contrast), plus new tests in `actions.rs` (8), `pages.rs`, `security.rs`, `mount.rs`,
  `hench-server`'s `http.rs` (4) and `cli.rs` (2), cases added to existing ones (prefix with a
  trailing slash, control characters and absurd times refused by the JSON API, `--allowed-host`
  parsing), and unit tests (`hosts`, `search`, `views`, `format`, `monitor`, `strings`).
  The new tests fail on the previous tree (checked in a scratch worktree), except the
  retry-count and `nest_service` ones, which pin existing behaviour. New dependencies:
  `sha1_smol` (already built for redis-rs's scripts) and `tokio` (time) in `hench-web`.

- 2026-11-01, surface fix round (branch `fix-surface`: api, testing, iterable, metrics, cli,
  docs, CI; eleven verified findings, all fixed, each reproduced first by a failing test — or,
  for the README's dependency list, by the new check script — except three documentation
  fixes: api-docs-10, which also adds a startup warning, web-security-8, web-functional-14):
  - features-1: a metrics or stats flush that timed out during a Redis stall was applied once
    Redis continued *and* merged back and resent by every later beat (10 executions counted
    30 after a 20 s stall). Counts now go through `flush::Flusher` (DESIGN §5 "Counter
    flushes"): numbered batches, kept and resent unchanged until confirmed, applied by a script
    guarded by `flush:<id>` (the last number applied), so a batch counts once whatever happens
    to replies; OOM and read-only replicas fail before counting; a failing write is not
    repeated. Tests SIGSTOP the test `redis-server` across several timed-out flushes: the
    tracker (42 → 12), `Stats` (unit test), and real workers across a 2 s stall (30 → 10); plus
    unit and Redis tests of the script (order, duplicates, OOM, `WRONGTYPE`, giving up). The
    engine's `stats.rs` changed minimally to use it (no API change).
  - features-4: an inline testing client re-ran an interrupted iterable job from inside the
    execution that was interrupted; ~40 interruptions overflowed a 2 MiB test stack in debug
    builds (aborting the test binary). Re-runs are now a loop (client middleware still runs
    on each push back); a test runs 500 interruptions on a 2 MiB thread.
  - features-5: a panic in `each_iteration` or the enumerator skipped the cursor save that
    errors get (the retry restarted from `on_start`); it now saves, then resumes the panic.
  - features-6: `Iteration::abort()` from `on_start`/`on_resume` was documented as having no
    effect but completed the job after one item; it now ends the iteration before the first
    item (documented; no effect from the later hooks, tested).
  - features-7: `[capsules.default]` in the file silently overrode `-c`/`-q`; it is now read
    into the top-level settings, which the command line overrides (`Settings::apply` also
    applies capsules first).
  - api-docs-11: `BulkPush::options` filled unset fields while `JobSetter`/`JobBuilder::options`
    overlay; `BulkPush::options` now overlays too and `BulkPush::default_options` fills
    (an existing test of the fill semantics now calls `default_options`).
    `JobRecord::retry_count` stays `u64` on purpose (lenient reading), now documented.
  - api-docs-3: the README's examples needed `axum`, `clap` and tracing-subscriber's
    `env-filter`, which its Quick start did not list (the README doc tests see every
    dependency of `hench-server`). The list is complete, and `ci/check-readme-examples.sh`
    (CI job `readme`) builds every example in a crate with exactly that list; run against
    the old README it reports the reviewer's errors.
  - api-docs-8: the README's embedding example kept serving HTTP after a remote stop ended its
    workers, and ignored SIGTERM; it now stops both sides when either ends and handles
    SIGTERM. `tests/embedding.rs` runs that wiring (fails with the old one).
  - api-docs-10: "a job never takes a worker down" holds only with `panic = "unwind"`;
    README and DESIGN say so, and `start` warns when built with `panic = "abort"`
    (`launcher.rs`, a few lines; not testable under the unwinding test profile).
  - web-security-8: the README passed the dashboard password on the command line (visible in
    `ps`); it now uses `HENCH_WEB_PASSWORD` and says why.
  - web-functional-14: DESIGN's busy-page wording ("Stop only once quiet") said the opposite
    of the template; reworded.
  The full gate passes (fmt, clippy `-D warnings`, the suite, rustdoc `-D warnings`,
  `cargo +1.88 check --all-targets`, the README examples check).

- 2026-11-01, engine fix round (`fix-engine`; adversarially verified findings of the
  engine-semantics, concurrency-safety, redis-performance, api-docs, features and tests
  lenses): the scheduler leases due jobs instead of popping them (`ZADD XX` to now + 60 s,
  then client middleware under `catch_unwind`, then the API's `MOVE_OUT` script, which writes
  before it removes): a panicking client middleware (which lost the job and killed the
  scheduler for good), a lost reply to the pop (timeout or dropped connection, which lost the
  job) and a shutdown during a slow push (which aborted the scheduler holding the job) now
  delay a job by at most its lease; the poller restarts after a panic. A thread of its own,
  with its own Redis connection, refreshes the heartbeat key, so jobs blocking the runtime
  or slow callbacks no longer let a live process look dead (its jobs then ran twice at the
  same time); `heartbeat_interval` is capped at 20 s; remote quiet runs the quiet callbacks
  in a task of their own (the heartbeat stopped while they ran) and a shutdown waits for
  them before the shutdown callbacks (it aborted them); watchdog keep-alives come from a
  task of their own (a Redis stall delayed them past `WatchdogSec`); embedded workers
  (`start`, `run_until`) no longer send `READY=1`/`STOPPING=1` for the host's service unless
  given a notifier; dropping the `run`/`run_until` future shuts the workers down. Reliable
  fetch: a process reconciles its own in-progress list (jobs stranded by lost fetch replies
  ran only after a restart); a failed job's verdict (retry set or morgue) and its
  acknowledgement are one script, before the death handlers (a crash or hard shutdown in
  between ran a dead job again); in-progress lists are searched from the tail (`LREM -1`,
  `LPOS RANK -1`); the work state shares the fetched bytes instead of copying them;
  `RedisConfig::shared_connections(n)` spreads a busy process over several multiplexed
  connections. Scheduler outage reports keep a panic's class. Regression tests (each
  checked to fail on the previous code): `tests/scheduled.rs` (panicking middleware, lost
  reply through a proxy, response timeout during a Redis stall, shutdown during a slow
  push), `tests/launcher.rs` (heartbeat key refreshed while a job blocks a single-threaded
  runtime, remote quiet with a slow callback then stop, dropped `run_until`),
  `tests/systemd.rs` (keep-alives while Redis is `SIGSTOP`ped), `tests/signals.rs` (by
  default only `run` notifies systemd; the binary's tests now run one at a time),
  `tests/chaos.rs` (jobs stranded by a Redis stall run while the process lives, each once),
  `tests/retry.rs` (a dead job is acknowledged before the death handlers run),
  `tests/redis.rs` (several shared connections), plus unit tests. Tests that used an
  hour-long heartbeat interval to mean "no beat during the test" use the 20 s maximum.
  Second part: jobs that enable `backtrace` store where they failed whatever the
  environment (errors force-capture the stack while such a job is polled; a chained panic
  hook records a panic's location and stack; frames start at the job's code); the stored
  backtrace used to be empty without `RUST_BACKTRACE`, and a panic's never reached the
  panicking line. `default_job_options` (builder, `Testing`, `Client`) merges over the
  defaults like Sidekiq (setting `backtrace` dropped the default queue, so every push of a
  job without its own queue failed); two tests that built a client without a default queue
  that way now set an explicitly empty one. docs.rs metadata (every feature, `doc_cfg`);
  LICENSE and README in every crate; Redis-backed test targets declare
  `required-features = ["test-util"]` (the self dev-dependency is dropped on publication);
  `#[non_exhaustive]` on `JobOptions`, `JobPayload`, `LifecycleEvent`, `RetryIn`,
  `FetchStrategy`, `QueueMode`; `anyhow` re-exported (not `chrono`/`futures`: their names
  lengthen users' diagnostics; the one typeck expectation re-recorded only shows
  `hench::anyhow::Error`); doc fixes (`ErrorContext::context`, the `heartbeat` event,
  `retry_all`'s retry count, `retry_in`'s `count`). Test coverage: a `hench::Shutdown`
  failure outside a shutdown is retried and reported, fetch failures reach the error
  handlers once per episode, a second SIGTERM interrupts at once, `RetryIn::Kill` runs
  `retries_exhausted` (and honours `Discard` and `dead: false`), deaths and orphan burials
  trim the morgue by age alone, basic requeue goes to the front, quieting interrupts the
  initial scheduler wait (timed), real workers log `start`/`done`/`fail`, the start-up
  checks (no `INFO`, Redis 6.0 refused, eviction warning) — the new `tests/logging.rs`;
  test servers die with a killed test process (a watchdog; checked by killing a child test
  process), `TestRedis::start_with_args`; CI runs the macro's typeck cases on the recorded
  rustc and fails on a mismatch; capsule middleware chains documented as a deviation.
  Reconciliation checks that run late (a stalled runtime) restart the stranded entries'
  clocks. The touched suites ran three times at 32 test threads and the whole `hench` crate
  twice at 64 without a failure; the workspace suite (835 tests) passes.

- 2026-11-01, integration of the features and web lanes: both merged into `implement-hench`
  (conflicts only in DESIGN and STATUS, resolved keeping both lanes; the merged tree passed
  the gate, 766 tests). Glue built on top: the dashboard's metrics pages on
  `hench::metrics::Query` (DESIGN §9 "Metrics pages"; Sidekiq's `web_test` Metrics scenarios
  ported in `crates/hench-web/tests/metrics.rs`: the page renders with a period, filtering
  finds and hides classes, the job page asks for its period and offers 8h but not 24h; plus
  sorting by every column, chart data per period, the 25-class cut, figures and charts of one
  class, deploy marks, escaping and URL encoding of class names, `metrics(false)`, nested
  mounts, outages, and metrics written by live workers), iteration progress on the busy,
  retries and job pages (Sidekiq's iteration scenarios in `tests/iteration.rs`), and a metrics
  section in `hench stats`. Charts checked in headless Chrome against eight hours of seeded
  metrics, light and dark, 1280 px and 390 px: no console errors or CSP violations, no
  horizontal overflow; toggling classes keeps each class's color; hover and keyboard
  tooltips on lines, columns and heat cells. Fixed while checking: class names broke letter
  by letter in the phone-width table, the last column labels of the distribution collided,
  `1 jobs` in tooltips, and the heat map's help said "darker" (brighter means more in the dark
  theme). The categorical slots and the heat ramps were run through the palette validator
  against the dashboard's own surfaces (a six-step heat ramp could not clear the step-gap and
  light-end gates on white; five steps do). Also: the top-level README (its examples are doc
  tests), readme/description metadata for `hench-web` and `hench-server`, and the lane
  worktrees removed (branches `lane-web` and `lane-features` kept). The shared disk filled up
  once during the integration (23 GB of stale build artifacts from three branches; `cargo
  clean` fixed it, nothing in the code). The full gate passes (fmt, clippy `-D warnings`, the
  suite, rustdoc `-D warnings`, `cargo +1.88 check --all-targets`), the whole suite (804
  tests) ran three times in a row on the final tree without a failure, and the new and touched
  suites (`hench-web` metrics, iteration, security, pages, live; `hench-server` cli) five more
  times at 64 test threads, also without one.

- 2026-11-01, features lane review round 1 (self-review): a client middleware panicking while
  an interrupted job was pushed back would have crashed the processor (the job stayed in
  progress until the shutdown); it is a rejected push handled as a failure that keeps the
  panic's class (fixed before the first commit, with a test). A broken `NOTIFY_SOCKET` is
  warned about once, then logged at debug level. The iteration state's field names moved to
  `hench::keys`. New tests: `Fetcher::push_back` against Redis (one copy, idempotent when run
  again, scheduling, basic fetch) and the job logger's `interrupted` event. The new suites ran
  five times at 64 test threads and three times concurrently with the launcher suite without a
  failure; the `worker` example was run against a real Redis (configuration file with an
  environment section, `-v`, sample jobs: metrics hashes and histograms written, the iterable
  job completed and its state deleted, SIGTERM shutdown), and its `--help` no longer states
  defaults the program overrides.

- 2026-11-01, features lane (`lane-features`): metrics (tracker in the processor, flushes on
  beats and at shutdown, query, deploy marks), iterable jobs (trait, enumerators, Redis and
  in-memory state, interruption and push back, cancellation, state queries), worker CLI and
  TOML configuration file (feature `cli`, worker example rewritten on it), systemd
  notifications, per-job log levels, `#[hench::job]` on impl blocks and its `log_level` option.
  New suites: `tests/metrics.rs` (20: Sidekiq's metrics and deploy scenarios with fixed times,
  saturating histograms, real workers writing metrics on beats and at shutdown, `.metrics(false)`),
  `tests/iterable.rs` (20: Sidekiq's iterable scenarios — resume, hooks, failure, stopping,
  runtime limit, cancellation before and during iteration, arguments — against memory and Redis
  state, `IterationState`, real workers under both fetch strategies pushing interrupted jobs
  behind other jobs with every item done once, a shutdown handing over to the next process,
  client middleware on the push back, macro registration), `tests/cli.rs` (11) and
  `tests/systemd.rs` (4), plus unit tests (histogram, tracker, periods, enumerators, state
  parsing, CLI parsing and files, notifier and abstract sockets, log levels) and trybuild cases
  (impl blocks, `log_level`). Fixed during development: a zero checkpoint interval looped
  without progressing (a checkpoint now proceeds to the next item).

- 2026-11-01, web lane (`hench-web`, `hench-server`, branch `lane-web`): DESIGN §9 dashboard
  and server implemented. Ported scenarios: `web_test` (every page with data, busy/queue/set
  actions, filters, titles with counts, sorting, bad JSON in the morgue, escaping, stats JSON,
  custom tabs and host routes as in `webext_test`), `web_helpers_test` (argument display,
  query-string escaping, natural process order, memory format, relative times),
  `web_router_test` (whole-segment route matching, named parameters, also under a
  parameterized mount), `web_action_flash_test` (notice stored, shown once, cleared) and
  `sidekiqmon_test` (version, overview, processes, queue table, helpers). Plus: CSRF
  accepted/refused (missing, wrong, malformed tokens; cross-site `Sec-Fetch-Site`/`Origin`),
  read-only pages and POSTs, security headers on every response kind, no inline script or
  style, parameter bounds, mounting at the root, one and three levels deep and under
  `/tenants/{tenant}/jobs`, assets (types, immutable caching, unknown names), Redis outages
  (500 page, 503 JSON and heartbeat), live workers (a real launcher on the busy page, quiet
  and stop from the dashboard), Basic auth, the JSON API (shapes, pushes, validation, read-only,
  cross-site), the CLI in process and the real binary (stats, exit codes, `hench web` serving
  then stopping on SIGTERM). Checked in headless Chrome (light and dark): pages render without
  console errors or CSP violations; selection, confirmations, notices, theme toggle and live
  refresh work. API additions (additive, tested in `tests/api_dashboard.rs`): `Queue::exists`,
  `JobSet::add_all_to_queue`. New workspace dependencies: `subtle`, `percent-encoding`,
  `form_urlencoded` (already in the lock file through other crates).

- 2026-10-31, integration of the engine and api lanes: merged both into `implement-hench`
  (STATUS conflicts resolved keeping both lanes' entries). The trybuild `argument_types`
  expectation was re-blessed: axum, a dev-dependency since the engine lane's example, turns on
  `serde_json`'s `raw_value`, which adds `RawValue` to rustc's list of `Deserialize` types (only
  that incidental hint changed). Wiring: the processor and the testing modes now run jobs
  through one `middleware::execute_job`; `ProcessSet::cleanup` and the scheduler share one
  cleanup; `Queue::pause`/`unpause` write the `paused` set the fetchers already skipped
  (nothing wrote it before); `JobError::KILLED` is public; DESIGN's signal-list row described
  `RPOP` instead of the engine's `LINDEX`/`LREM`. New `tests/api_engine.rs` (10 tests) checks
  the API against live workers: heartbeat fields, work state, stats and daily history, remote
  quiet/stop, retries and the morgue (inspect, retry, kill, revive), scheduled jobs (enqueue
  early, reschedule), paused queues, crashed-process cleanup, and `Testing::from_hench`.
  Follow-ups in the same round: pushes and API moves share one Redis encoding
  (`client::Stored`; the api lane had a copy); macro jobs run on real workers in a test (the
  macro and the runtime had only been tested apart); error backtraces written by workers are
  decoded through the API in a test; testing-mode jobs run in the workers' `job` span (its
  test has a binary of its own because `tracing` caches callsite interest process-wide: with
  one registered dispatcher a subscriber-less test thread can cache the span as disabled —
  found by running the suites with 64 threads; the job logger's unit tests now keep a second
  dispatcher alive for the same reason); retry-delay and weighted-order proptests (DESIGN
  §10); the axum example serves `/stats` through `hench::api` (smoke-tested against a real
  Redis). Suite (546 tests) run three times in a row plus five rounds of the Redis-heavy
  binaries at 64 test threads; MSRV 1.88 check and rustdoc `-D warnings` pass.

- 2026-10-31, engine review round 5 (third independent review): "Redis unavailable" (unreachable,
  failover, OOM, LOADING, BUSY, MISCONF, MASTERDOWN, TRYAGAIN, CLUSTERDOWN, also inside
  transactions) is told apart from job-specific Redis errors: only the former stops a
  scheduler set (a WRONGTYPE queue no longer blocks it) and is retried until the shutdown
  (outcome writes, pending scheduler writes); the per-poll failure cap is per set (retries
  cannot starve scheduled jobs); a failing owner `SADD` can no longer drop a job moved by
  `BLMOVE` (pipeline ignores its errors); scheduler failures carry the job JSON and are not
  double-counted; hard-shutdown requeues reported per chunk of 100.

- 2026-10-31, engine review round 4 (second independent review, of the round-2 fixes): any
  Redis error while enqueueing a due job (out of memory included) now stops the set instead
  of deferring and popping on, which could drop every due job under OOM; a popped job whose
  write back fails stays pending in memory (JSON logged) and is retried before anything else
  is popped; at most ten deferrals per poll; outage notices rate-limited across flapping
  episodes (at most one a minute); a failed removal of a handled signal no longer fails
  `start`; the process is removed before the final stats flush, each step bounded;
  `BLMOVE` + `SADD` pipelined on the blocking connection (no extra round trip); a timed-out
  hard-shutdown requeue is reported as possibly failed. New tests: Redis out of memory
  during scheduling (no job lost), a middleware failing every push.

- 2026-10-31, engine round 3: automated test of `Hench::run` with real SIGTSTP/SIGTERM (own
  test binary), unusual queue names, capsules sharing a queue; full suite (355 tests) run
  repeatedly, also with 64 parallel test threads, without a failure.

- 2026-10-31, engine review round 2 (independent code review): a due job whose push keeps
  failing no longer blocks its set (deferred a minute; only an unreachable Redis keeps the
  original score); scheduler put-backs and burials retried with backoff instead of once;
  outage tracking rewritten (one lock, fetch and scheduler episodes tracked separately,
  persistent errors re-logged every minute instead of silenced); remote signals peeked and
  removed after handling (a lost reply no longer loses one; unknown ones are dropped);
  `BLMOVE` registers the in-progress owner; `Fetcher` docs state the heartbeat requirement;
  lock helper and morgue bounds shared; orphan liveness checks pipelined.

- 2026-10-31, engine review round 1: shutdown cleanup steps bounded (5 s each) and lingering
  background tasks aborted, so a blackholed Redis cannot hold a stopping process; duration
  settings saturate instead of panicking; quiet is published by an immediate heartbeat;
  crashed processors restart after a pause; scheduler outages reported once. New chaos tests
  with a partition-simulating TCP proxy (bounded shutdown, no job lost across a partition).
  Known limitation (shared with Sidekiq Pro's super_fetch): a reply lost to a timeout or a
  partition can leave a fetched job in the process's own in-progress list; it is pushed back
  at the next shutdown (or recovered by another process after a crash), not before.

- 2026-10-31, engine (fetch, processor, retry, scheduled, launcher, manager, stats,
  job_logger): implemented DESIGN §5 with tests ported from Sidekiq plus e2e/chaos suites.
  Deliberate improvements over Sidekiq, documented in DESIGN §5: outcome writes retried
  through Redis outages, scheduler puts jobs back instead of losing them on push errors,
  stats kept on failed flushes, reliable requeue never duplicates an acknowledged job,
  second stop signal forces the shutdown, remote signals work when embedded.

- 2026-10-31, api lane round 7 (independent review of the atomic moves; no loss path found):
  `SADD queues` now precedes `LPUSH` in the move script (a failing `SADD` left the job queued
  and still in its set); jobs that stay in their own set (rescheduled by middleware, killed in
  the morgue) no longer count as removals for iteration offsets; `EXECABORT` is classified by
  its inner errors only; a move that failed in transit and found the job gone is not counted
  as pushed; single-entry moves skip middleware for stale entries; the client's middleware
  step is shared by `push` and `prepare_push`.

- 2026-10-31, api lane round 6 (independent review, OOM test): job moves are now atomic. Under
  `maxmemory`/OOM the client's `MULTI` fails with `EXECABORT` (not recognized as an outage),
  and the restore after a failed push failed too, so `retry_all` could strip a whole set;
  the kill scripts popped jobs before adding them to the morgue, so OOM lost them (reproduced
  by a new test). Now client validation/middleware run first (`Client::prepare_push`), then a
  Lua script writes the job to its queue (or the schedule/morgue) before removing it from its
  set, so every failure leaves jobs in place; outage detection looks inside `EXECABORT`;
  spawned tasks and restores are gone; `retry_all` stops at the end of the set without extra
  round trips; `ScoreBound` simplified.

- 2026-10-31, api lane round 5 (independent review): `retry_all` redesigned to walk the set by
  score instead of tracking the jobs it leaves in place (round 4's list grew with every
  rejected job and was re-checked with `ZMSCORE` each batch); per-job Redis errors
  (`WRONGTYPE`) and middleware panics no longer stop it or wedge later calls, only Redis
  outages do; `fetch_range` pages by score cursor (offsets skipped entries under concurrent
  removals and cost O(offset)); the macro finds `crate = ".."` token by token; drain and
  `retry_all` docs clarified; three tests made independent of task scheduling and timing.

- 2026-10-31, api lane round 4 (independent review): a panicking client middleware during a
  move no longer loses the job (restored, then the panic resumes); one job whose push is
  always rejected no longer blocks `retry_all` forever (it is put back and skipped; Redis
  errors still stop the call); fake drains resolve the job before removing it (an unknown
  class deleted captured jobs); explicit test registrations shadow duplicated inventory
  names; `find_job` finds jids that JSON escapes or that contain glob metacharacters;
  latency falls back to `created_at` when `enqueued_at` is not a number; `JobSet::schedule`
  drops `enqueued_at`; the cleanup lock is sent once; `retry_all` ordering documented.

- 2026-10-31, api lane round 3 (independent review): generated code no longer breaks when a
  job is named like one of its bindings (`args`, `client`, `delay`, `at`: unit structs are
  patterns); `#[expect]` on jobs applies to both generated items; the error fallback keeps a
  valid `crate` path; morgue age trim made inclusive in the Lua move (was exclusive);
  `fetch_range` reads in pages of 500; `retry_all` moves 16 jobs concurrently and finishes
  in-flight moves before returning an error; `find_job` reuses `scan`; `SSCAN` dedup
  centralized. Also covered by this log: `Send` checks for every API/testing future and a
  polling (not sleeping) cancellation test (commit 81956c2), and round 0 hardening (scheduled
  jobs in testing modes, `#[deprecated]` jobs, `Api::redis_info`, commit 44f4c6d).

- 2026-10-31, api lane round 2 (independent review): `retry`/`add_to_queue`/`retry_all` run
  their remove-then-push pair in a spawned task, so a cancelled caller (dropped web request,
  timeout) can no longer lose a job between the two; deletes (`ZREM`/`LREM`) are sent once and
  resolved by presence; `entries()` counts only removals of yielded entries (deleting others
  through the same handle skipped entries); `retry_all` offsets by the unparseable entries
  still present (`ZMSCORE`), not a stale count; `kill_all` records and trims moved batches
  before returning an error; macro bodies keep the job's name (instrument spans were all
  `__hench_job`), errors come with a fallback expansion (no cascades), `#[cfg]` on
  parameters is rejected; testing clients build their registry lazily (`perform_inline`
  rebuilt it on every call).

- 2026-10-31, api lane round 1 (api/, testing, macro; independent review): removing a job
  from a set before re-pushing it no longer goes through `with_retry` (a repeated `ZREM`
  answers 0 and the job was skipped, i.e. lost) and resolves failures by membership; the
  move-to-morgue Lua scripts are sent once; re-pushes stopped by client middleware are logged
  and not counted; `SSCAN` duplicates no longer inflate `Stats::workers_size`/`enqueued`;
  `tags = []` compiles; entry keys with negative scores parse; scan dedup keeps hashes, not
  copies; page boundary made consistent; timestamp rules shared; morgue-limit docs corrected.
  Earlier in the round: `retry_all` removes jobs one by one (a crash lost up to 100),
  `#[deprecated]` jobs, testing clients run scheduled jobs as enqueued, `Api::redis_info`.

- 2026-10-31, spine round 4 (panic/alloc sweep of library code): queue weights are capped
  at 1000 (`CapsuleConfig::MAX_WEIGHT`) since weighted ordering expands each queue `weight`
  times; remaining casts are saturating and slices are on checked boundaries.

- 2026-10-31, spine round 3 (TLS): the `tls` feature compiled rustls without a crypto
  provider, so every `rediss://` connection panicked; it now enables rustls's `ring` provider
  and TLS 1.2. Added `TlsConfig` (custom root CA, client certificate for mutual TLS) and
  end-to-end tests against a TLS `redis-server` with certificates generated per test
  (trusted CA, unknown CA rejected, mutual TLS).

- 2026-10-31, spine round 2: payloads with `null` `jid`/`queue`/`args` no longer fail to parse
  (they would have been sent to the morgue); `REDIS_PROVIDER`/`REDIS_URL` resolution and the
  `redis-server` lookup made unit-testable (no process-global env mutation); documented that
  `blocking_conn` waits through reconnection retries; added concurrency (1000 parallel
  pushes, no loss/duplication) and build-from-URL/unreachable-Redis tests. Suite run 6× in a
  row without flakes.

- 2026-10-31, spine (error, keys, payload, options, redis, job, client, middleware, config,
  test_util): `RedisPool::conn` no longer serializes callers behind a retrying connection
  attempt during outages (lazy `ConnectionManager`); serde_json `float_roundtrip` enabled
  (default parser changed ~30% of `f64` job arguments by one ULP); spread bulk jobs already
  due are pushed immediately (tests adjusted); module docs moved onto public items so they
  render; `hench-server` binary docs no longer overwrite the `hench` library docs.
