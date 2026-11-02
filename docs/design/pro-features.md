# Pro features for hench: cross-feature review and integrated plan

Status: final design, 2026-11-01. Scope: unique jobs, periodic jobs, batches (with simple workflows)
and rate limiting, designed in parallel in:

- `/home/user/scratch/design/unique.md` (unique jobs)
- `/home/user/scratch/design/periodic.md` (periodic jobs)
- `/home/user/scratch/design/batches.md` (batches and workflows)
- `/home/user/scratch/design/ratelimit.md` (limiters, throttles, queue limits)

Baseline: `docs/DESIGN.md` of `implement-hench` at `a8a5909`. Nothing in `/home/user/hench` was changed.
The four documents were reviewed against each other and against the code they hook into
(`client.rs`, `fetch.rs`, `processor.rs`, `middleware.rs`, `stats.rs`, `payload.rs`, `keys.rs`,
hench-web's `strings.rs`/`flash.rs`), and every conflict or gap found was fixed **in the four documents
themselves**, which stay self-contained (each starts with a "Cross-feature review" paragraph listing what
changed). This document holds what spans them: the review findings (§1), the conventions all four follow
(§2), the combined behaviour and the processor pipeline (§3), the common infrastructure (§4), the per-feature
work (§5), the implementation plan with two parallel lanes and disjoint file ownership (§6), the DESIGN.md
additions (§7), the cross-feature test matrix (§8), the consolidated owner decisions (§9) and risks (§10).
The new cross-feature Lua interface was verified on redis-server 7.0.15 (Appendix A).

Licensing, unchanged from the four designs: Sidekiq Enterprise/Pro (closed), the Sidekiq wiki, Sidekiq OSS
(LGPL-3.0) and Faktory (AGPL) were used for behaviour only; BullMQ, sidekiq-cron, sidekiq-unique-jobs,
sidekiq-throttled, sidekiq-batch, sidekiq-rs (MIT) and Oban (Apache-2.0) were read for ideas; nothing is
adapted from any of them (an implementer who adapts MIT/Apache code later adds a credit comment). The lab
code is original.

---

## 0. Summary

Hench gets the four "pro" features of Sidekiq Pro/Enterprise with stronger atomicity than the references,
because each feature does its Redis work **inside the scripts that already move the job** (push, outcome,
fetch) instead of in middleware around them. The review keeps each design's core and fixes how they meet:

- **One processor pipeline** for every job, fixed order (§3.2): throttle gate (rate limiting) → work state,
  span, server middleware → endpoint: batch cancellation check, then the unique `Start` release, then
  `perform` → classification of the result → batch accounting → the outcome script, which holds or
  releases the unique lock inside it → engine-held permit release → batch dispatch → periodic
  `RECORD_RUN`. Every contract between the designs is a consequence of this order.
- **One push pipeline** (§3.1): normalization (options merged, unique digest, throttle field), the
  collecting client's `bid`, client middleware, then delivery by exactly one script that writes the job
  and its lock together: `UNIQUE_PUSH`/`MULTI` (`raw_push`), `BATCH_COMMIT` (batches) or `FIRE`/`ENQUEUE_NOW`
  (periodic). The unique design's Lua library is the **frozen interface** the other two embed
  (`hu_check`, `hu_take`, `hu_push_one`, `hu_count`, `hu_trim`).
- **Shared infrastructure, built once** (§4): canonical JSON and SHA-256 (`crate::canonical`), the Lua
  prelude and script builder (`crate::lua`), key helpers and the per-minute counter convention, the five
  new payload fields, the `unique` and `throttle` options, the new errors, the processor/endpoint/heartbeat/
  testing/API hook points, the heartbeat `"features"` map, the dashboard's navigation, switches and job-row
  order, the CLI and TOML skeleton.
- **Two lanes with disjoint files** (§6): after a common phase owned by the integrator (phase 0), lane A
  implements unique jobs then periodic jobs, lane B rate limiting and batches, each only in its own
  module directories, API files, macro option file, dashboard pages and server commands; the integrator
  merges them and owns the cross-feature tests, the soak scenario and the shared docs (phase 2). Queue
  limits (`FETCH_GATE`), staged batch commits and the iterable cancellation checkpoint come after (phase 3).
- **30 findings** were fixed (§1), among them five behavioural ones: callback templates carrying a unique
  digest without a lock (lapse noise), duplicates inside one batch commit invisible to a check-before-write
  script, typed unique/throttle keys lost in periodic templates, members of cancelled batches cycling
  through the throttle's wait line for hours, and skipped members distorting execution-time metrics.

---

## 1. Review findings and their resolutions

"Fixed in" names the sections of the four documents that now carry the rule.

| # | Finding | Resolution | Fixed in |
|---|---|---|---|
| F1 | Batch callback templates are normalized, so a unique callback job carries a digest, but the dispatch script takes no lock: every acknowledgement and heartbeat renewal of such a callback would count a unique "lapse" (`z`), polluting the signal operators use to size `unique_for`. | Callback templates are normalized with uniqueness removed (`unique` stripped); a callback is pushed once per event by construction. | batches §2.3.1, §2.9, §2.18; unique §2.18 |
| F2 | `BATCH_COMMIT` checks everything before its first write (all or nothing), so two members of one commit with the same digest both see a free lock; "already pushed" (a member whose explicit jid holds the lock) would register a job that never reports to the batch; KEYS/ARGV/trim were unspecified. | Check phase: `hu_check` per member plus a per-commit `seen[digest]` table; `'self'` counts as a duplicate; `error` refuses before any write; write phase: job, `pcall(hu_take)`, registration; `hu_count` per duplicate; `hu_trim` last; trailing unique ARGV section (commits without unique members keep their size). Verified (Appendix A). | batches §2.5.3, §2.9; unique §2.8.2 |
| F3 | Batches asked for `hu_push_one`, which the unique library did not have; periodic's `FIRE` described the lock step inline. | Frozen Lua interface: `hu_check`, `hu_take`, `hu_hold`, `hu_release`, `hu_push_one`, `hu_count`, `hu_trim` with fixed signatures; the library file is created in phase 0 from the lab prototype. | unique §2.8.2; periodic §2.10 |
| F4 | `Periodic::job::<J>(args)` stores an untyped template; fire-time normalization (`Client::prepare_push`) cannot call `J::unique_key` or `J::throttle_key`, so the digest would cover all arguments and the throttle key would be missing. | The typed keys are evaluated when the schedule is defined and stored as the template's explicit `unique.key` / `throttle.key`; `Periodic` gains `unique_for`, `unique_until`, `unique_key`, `throttle`, `throttle_key`. | periodic §2.3.1; unique §2.4; ratelimit §2.12 |
| F5 | Periodic templates could carry other features' per-push fields (`bid`, `batch_callback`, `limited`, `periodic`) or `on_conflict` `error`/`replace`. | `Periodic::validate` refuses them. | periodic §2.3.1; unique §2.5 |
| F6 | `RECORD_RUN` knew only `ok`/`failed`/`interrupted`; the rate limiter asked for `rescheduled`; which payload it reads and what happens for jobs that never ran were unspecified. | Status `rescheduled` (`RunStatus::Rescheduled`); metadata from the job as fetched; nothing recorded for throttled (parked before start), shutdown requeues or hard-shutdown interruptions. | periodic §2.3.4, §2.4, §2.5, §2.10; ratelimit §2.5, §2.6, §2.12 |
| F7 | The throttle gate runs before the endpoint's cancellation check, so throttled members of a cancelled batch would be parked again and again (default `max_delay` 24 h, unlimited reschedules) before being skipped. Both designs listed the fix as a "possible refinement". | Adopted: on a gate refusal of a batch member, one `HGET cancelled_at`; if cancelled, the member is skipped (`BATCH_SKIPPED cancel`, acknowledged, `ACK_UNIQUE` when locked) instead of parked. | ratelimit §2.6, §2.12; batches §2.6.1, §2.9 |
| F8 | Skipped members were tracked as `Outcome::Success` (timed), skewing execution-time averages and histograms with near-zero durations, while the rate limiter added dedicated outcomes for its non-executions. | `Outcome::Skipped`: processed, new per-class field `s`, not timed (beside `o` and `t`). | batches §2.6.1, D18; ratelimit §2.8 |
| F9 | Order of the two endpoint hooks (batch cancellation check, unique `Start` release) undefined; a skipped member's `Start` lock could be released by a `perform` that never happens. | Cancellation check first, `Start` release second; a skipped member's lock is released by its acknowledgement. Both read `ctx.payload()` (the job as fetched). | batches §2.6.2; unique §2.9 |
| F10 | The batches processor pseudo-code had no over-limit or throttled branch. | Added (no accounting; skip for cancelled members on a gate refusal). | batches §2.6.1 |
| F11 | The unique lifecycle table had no rows for batch commits, periodic ticks, throttle parking, over-limit reschedules and batch skips. | Rows added. | unique §2.3 |
| F12 | Script name collisions: `RELEASE` (fetcher, unique, rate limiter), `RENEW` (unique, rate limiter), generic periodic names (`REMOVE`, `PAUSE`, `LEASE`). | Feature prefixes in code and logs (`UNIQUE_RELEASE`, `UNIQUE_RENEW`, `LIMIT_RELEASE`, `LIMIT_RENEW`, `PERIODIC_FIRE`, ...); helper prefixes `hu_`, `hp_`, `hb_`, `hl_`. | unique §2.8.1; periodic §2.5; batches §2.5.1; ratelimit §2.9.1 |
| F13 | Per-minute counter helpers named three ways (`unique_events`, `batch_events_minute`, `limit_events`), "events" meaning both counters and batch callbacks, and readers accepting hour periods although the hashes live 8 h without rollup. | `keys::unique_minute`/`batch_minute`/`limit_minute`, `FEATURE_MINUTE_TTL` (8 h), one reader accepting minute periods up to 8 h (`Error::Config` beyond). | unique §2.7, §2.11; batches §2.4; ratelimit §2.8, §2.13 |
| F14 | Hashes: SHA-256 for unique digests, SHA-1 for periodic definition digests and limiter keys longer than 128 bytes. | One crate-wide SHA-256/base64url helper (`sha2` is added anyway); limiter keys hash to `#` + 22 characters (132 bits). | periodic §2.4, §2.7, §2.14; ratelimit §2.4.1, §2.8 |
| F15 | Periodic definition digests relied on `serde_json` maps being sorted, which is false when another crate enables `preserve_order` (feature unification): two builds of one crontab would disagree, flipping definitions and raising the digest-mismatch banner. | Crate-wide canonical JSON writer (recursive key sort), shared with unique digests. | periodic §2.4; unique §2.4 |
| F16 | Only batches advertised support in the heartbeat; unique (old workers never release locks), rate limiting (old workers fail `OverLimit` jobs) and periodic (spec versions) have the same rollout hazard. | Heartbeat `info` gains `"features": {"unique": 1, "periodic": 1, "batches": 1, "limits": 1}` (stored-format versions); the dashboard names live processes lacking a feature in use. | all four (§2.9/§2.4/§2.15/§2.10) |
| F17 | Dashboard: three tab positions chosen independently, toggles inconsistent (`unique_locks(bool)`, none for periodic, batches "always shown"), job-page rows unordered, periodic's flash code format differing from `flash.rs`'s `kind.count.failed.subject`. | One navigation order; `HenchWeb::{periodic, batches, limits, locks}(bool)` (default on, 404 when off, like `metrics`); one job-row order; new `NoticeKind`s in the existing format. | unique §2.13; periodic §2.11; batches §2.12; ratelimit §2.15 |
| F18 | CLI shapes: `hench batches` plus `hench batch <sub>`; `hench stats` changes differing per feature (overview line vs sections). | One command per feature with subcommands (`list` default); `hench stats <feature>` sections, printed by `all` when the feature has data; overview unchanged. | batches §2.13; unique §2.14; periodic §2.12; ratelimit §2.16 |
| F19 | `hench enqueue` and `POST /jobs` took unique fields but no throttle fields, although the server knows no job types (typed throttle options never apply there). | `--throttle NAME[:KEY]`, `throttle`/`throttle_key`. | ratelimit §2.16; periodic §2.12 (`periodic add`) |
| F20 | TOML: every top-level table other than `capsules` is an environment (§9e), so `[[periodic]]`, `[throttles.*]` and `[queue_limits]` need reserving; only the rate limiter said so. | Reserved top-level names: `capsules`, `throttles`, `queue_limits`, `periodic` (release note). | periodic §2.3.2; ratelimit §2.17 |
| F21 | DESIGN.md section numbers: three designs claimed §9g. | §9g unique, §9h periodic, §9i batches, §9j rate limiting, §9k the features together. | all four |
| F22 | `Job::unique_args` vs `Job::throttle_key`: two names for "a key derived from the typed arguments". | `Job::unique_key(args)`; explicit `unique_key(..)`/`throttle_key(..)` setters win over the trait methods. | unique §2.6.2 |
| F23 | Testing modes: no design said how its in-memory model interacts with the others' (a batch commit's duplicates, a captured over-limit job's lock, an inline over-limit member). | Rules written in each testing section; one testing-tables container; defaults principle (push-time deterministic features on, time-driven off). | unique §2.12; batches §2.11; ratelimit §2.14; periodic §2.10 |
| F24 | `TIME` followed by writes needs effects replication; only the rate limiter checked `lua-replicate-commands`, though all four rely on it. | Common start-up check. | ratelimit §2.9.1; batches §2.5.1 |
| F25 | A periodic job that defines a batch per tick creates a second batch when it runs twice (at-least-once); nothing offered idempotent creation. | `Batch::with_bid(client, bid)` with the job's jid as bid and commit id (re-run: `already_applied`); `BatchError::Exists`. | batches §2.3.1, D17; periodic §2.10 |
| F26 | The dashboard's Periodic row on job pages had no accessor. | `JobRecord::periodic()`. | periodic §2.3.5 |
| F27 | A rate-limit reschedule writes the job to `schedule` with `PUSH_BACK_UNIQUE` recording `at`, which makes it replaceable by a debounce push; undocumented. | Documented as intended (the parked job has not run its effect); an iterable one leaves its state to expire. | unique §2.3; ratelimit §2.5 |
| F28 | Lab finding: `until` is a Lua keyword, so a table field named `until` does not compile. | Scripts keep the policy in a field named `policy`. | unique §2.8.2 |
| F29 | All four designs modify the same spine and engine files (payload, options, job, client, error, keys, config, processor, fetch, middleware, launcher, stats, testing, api, metrics, cli, macros, web, server): four lanes would conflict everywhere. | Phase 0 adds every shared type and a hook point per feature; lanes then own disjoint files (§6). | §4, §6 here; each design's plan section |
| F30 | Queue limits (rate limiting phase 3) change `fetch.rs`, which the unique design changes for its outcome scripts. | Phase 3 starts after both lanes merged. | ratelimit §2.21 |

---

## 2. Conventions shared by the four features

### 2.1 Redis keys

All helpers live in `hench::keys` (DESIGN §2: "never hand-format keys elsewhere"). Prefixes are reserved and
disjoint from each other and from hench's existing keys (`queue:`, `schedule`, `retry`, `dead`, `processes`,
`<identity>…`, `inprogress…`, `stat:…`, `flush:`, `paused`, `it-`, metrics `j|`, `h|`, `…-marks`,
`deploylock-`).

| Feature | Key | Type | Expiry | Helper |
|---|---|---|---|---|
| unique | `unique:<digest>` | HASH | `PEXPIRE` always (`for` + delay of scheduled/retry/parked jobs; renewed while running; never shortened) | `unique_lock(digest)` |
| unique | `unique-locks` | ZSET (score = expiry) | none; trimmed by score | `UNIQUE_LOCKS` |
| unique | `u\|YYYYMMDD\|H:MM` | HASH | 8 h | `unique_minute(at)` |
| periodic | `periodic` | ZSET (due index; score = `next` or lease end) | none (configuration) | `PERIODIC` |
| periodic | `periodic-names` | ZSET (name index) | none | `PERIODIC_NAMES` |
| periodic | `periodic:<name>` | HASH (definition + state) | none; removed with the schedule | `periodic(name)` |
| periodic | `periodic-history:<name>` | LIST (capped) | none; capped, removed with the schedule | `periodic_history(name)` |
| periodic | `periodic-group:<group>` | SET | none | `periodic_group(group)` |
| periodic | `periodic-crontabs` | HASH | none | `PERIODIC_CRONTABS` |
| batches | `batch:<bid>` and `:pending`, `:failed`, `:dead`, `:kids`, `:commits` | HASH, SET, HASH, SET, SET, SET | `batch_ttl` (30 d) after the last activity, linger (24 h) once finished; every key of a batch shares the hash's expiry | `batch(bid)`, `batch_pending(bid)`, … |
| batches | `batches` | ZSET (score = expiry) | none; trimmed by score | `BATCHES` |
| batches | `batch-events` | ZSET (outbox) | none; entries removed by dispatch | `BATCH_EVENTS` |
| batches | `batch-maintenance` | STRING (lock) | `PX` 0.8 × interval | `BATCH_MAINTENANCE` |
| batches | `b\|YYYYMMDD\|H:MM` | HASH | 8 h | `batch_minute(at)` |
| batches (staged commits, later) | `batch:<bid>:stage:<cid>`, `batch-publishing` | LIST, ZSET | 1 day; none (entries removed by the last publish) | |
| limits | `limit:c:<id>`, `limit:w:<id>`, `limit:b:<id>`, `limit:g:<id>` | ZSET, LIST, HASH, STRING | derived from meaning (longest lease; period + 1 s; bucket end + 1 s; drain end + 1 s); `DEL` when a concurrency set empties | `limit_state(kind, id)` |
| limits | `limit-stats:<k>:<id>` | HASH | `stats_ttl` (24 h) after the last use | `limit_stats(kind, id)` |
| limits | `limits` | ZSET (score = stats expiry) | none; trimmed by score | `LIMITS` |
| limits | `l\|YYYYMMDD\|H:MM` | HASH | 8 h | `limit_minute(at)` |
| limits (queue limits, later) | `queue-limits`, `limit:*:@<queue>` | HASH; as above | none (configuration); as above | `QUEUE_LIMITS` |
| metrics (existing) | `j\|YYYYMMDD\|H:MM`, `j\|YYYYMMDD\|H:M` | HASH | 8 h / 3 d (unchanged) | gains fields `<class>\|o`, `\|t`, `\|s` |

TTL policy, common to all four: **every per-entity key has an expiry derived from what it means, set by the
script that may create it and never shortened; global index ZSETs have no expiry but are scored by the
expiry of what they index and trimmed by score (bounded trims in hot scripts, full trims by readers and
maintenance); configuration (periodic schedules, `queue-limits`) has no expiry; per-minute counters expire
after 8 h.** A cross-feature test sweeps every key after each scenario and allows a missing TTL only for
the configuration and index keys listed above (§8, X23).

Per-minute counters: key `<letter>|YYYYMMDD|H:MM` (`u`, `b`, `l`; `j`/`h` stay the metrics'), UTC minute of
the writing client's clock (declared by the caller), `HINCRBY` under `pcall` as a script's last writes,
`EXPIRE` 8 h (`keys::FEATURE_MINUTE_TTL` = `METRICS_MINUTE_TTL`), no ten-minute rollup, one reader
(`api::events`, phase 0) for minute periods up to 8 h.

Free-text names (periodic schedule names, limiter keys, queue names) cannot produce another kind's key
because every kind has its own prefix. A residual hazard inherited from Sidekiq's un-namespaced identities:
a host literally named `periodic` produces heartbeat keys `periodic:<pid>:<nonce>`, which a schedule named
`<pid>:<nonce>` would collide with (as a host named `queue` already can with a queue); documented, not
designed around.

### 2.2 Lua scripts

- Files: `crates/hench/src/<feature>/lua/*.lua`, embedded with `include_str!` and assembled by the
  crate-wide builder (`crate::lua`, phase 0) as `<prelude> + <feature lib> [+ unique lib] + <body>` into
  `LazyLock<redis::Script>` (redis-rs retries `NOSCRIPT` with `EVAL`, which covers failovers).
- Names carry the feature prefix in code and logs: `UNIQUE_PUSH`, `ACK_UNIQUE`, `SETTLE_UNIQUE`,
  `PUSH_BACK_UNIQUE`, `REMOVE_UNIQUE`, `KILL_UNIQUE`, `UNIQUE_RELEASE`, `UNIQUE_LOCK_OP`, `UNIQUE_RENEW`,
  `UNIQUE_RELEASE_MANY`; `PERIODIC_LEASE`, `PERIODIC_FIRE`, `PERIODIC_SYNC`, `PERIODIC_UPSERT`,
  `PERIODIC_REMOVE`, `PERIODIC_PAUSE`, `PERIODIC_UNPAUSE`, `PERIODIC_ENQUEUE_NOW`, `PERIODIC_RECORD_RUN`;
  `BATCH_COMMIT`, `BATCH_SUCCEEDED`, `BATCH_FAILED`, `BATCH_SKIPPED`, `BATCH_DISPATCH`, `BATCH_TOUCH`,
  `BATCH_CANCEL`, `BATCH_DELETE`; `LIMIT_ACQUIRE`, `LIMIT_RELEASE`, `LIMIT_RENEW`, `LIMIT_ADJUST`,
  `LIMIT_USAGE`, `LIMIT_RESET`, `LIMIT_CLEAR_LINE`, `FETCH_GATE`. Helper prefixes: `hu_`, `hp_`, `hb_`,
  `hl_` (no clash when a script prepends two libraries).
- Rules every script follows (the four designs' rules, merged): declared keys only (Dragonfly; no `SCAN`,
  no key built in Lua); Redis 6.2 commands only (no `PEXPIRE GT`, no script flags); `TIME` read once per
  script, from Redis (test overrides only where a design provides one); checks before writes, and **the
  first write is the one whose refusal under `OOM` leaves a consistent state** (Redis checks memory only
  before a script's first write); bookkeeping (`HINCRBY` counters, stats) under `pcall`; index trims (a
  removal, which would make later writes bypass the memory limit) last; write before remove; compare the
  holder (jid, token) before every release or extension; never shorten a TTL; scripts are idempotent or
  guarded so that `with_retry`'s resend after a dropped connection is harmless (or sent once, with the
  result disambiguated as the designs describe).
- Lua pitfalls: `until`, `end`, `repeat` and other keywords cannot be table fields (`t.until` does not
  compile; Appendix A); Lua numbers are doubles, so job JSON is never decoded in Lua except hench's own
  templates (batches' rule 7), and integer arguments stay below 2^53.
- Start-up: `start` warns when `CONFIG GET lua-replicate-commands` answers `no` (Redis 6.2; skipped when
  `CONFIG` is refused): every feature script reads `TIME` before writing.
- Tests: a phase-0 test loads every registered script (`SCRIPT LOAD` on `TestRedis`) and greps the sources
  for forbidden commands; each feature ports its lab scenarios to `tests/<feature>_scripts.rs`.

### 2.3 Payload fields

| Field | Feature | Shape | Set by | Read by |
|---|---|---|---|---|
| `bid` (existing, reserved) | batches | 24 lowercase hex | the collecting client before client middleware | the processor (job as fetched), the endpoint check, the API |
| `batch_callback` | batches | `{"bid", "event"}` | callback templates | `JobContext::batch_callback`, job pages |
| `unique` | unique | `{"for", "until", "digest"}` (push-time `on_conflict`, `key`, `across_queues` stripped) | normalization | outcome scripts (via `LockRef` from the job as fetched), endpoint, heartbeat |
| `periodic` | periodic | `{"name", "tick" (ms), "manual"?}` | the poller, "enqueue now" | `JobContext::periodic`, `RECORD_RUN`, job pages |
| `throttle` | rate limiting | `{"name", "key"}` | normalization | the throttle gate |
| `limited` | rate limiting | `{"n", "by", "at"}` | the over-limit/throttle reschedule | reschedule budget, job pages, `Limits::waiting` |

All join `JobPayload::RESERVED_KEYS` (a custom key cannot impersonate them; `normalize` drops such extras
with a warning) and are read **leniently**: a malformed value becomes `None` with a `warn`, so no feature
field can make a job unparseable and send it to the morgue. Every feature derives its per-execution state
from the job **as fetched**, never as server middleware changed it (the retry rule of DESIGN §5), and the
endpoint hooks read `ctx.payload()`, which is the job as fetched (server middleware passes the context it
received to `next`). Release note: an application already using a custom key named `unique`, `periodic`,
`throttle`, `limited` or `batch_callback` sees it reinterpreted.

### 2.4 Options, `Job` methods and the macro

| Concept | `JobOptions` / `JobSetter` / `JobBuilder` / `BulkPush` / `Periodic` | `Job` trait | `#[hench::job(..)]` |
|---|---|---|---|
| Uniqueness | `unique(..)`, `unique_for(d)`, `unique_until(u)`, `on_conflict(c)`, `unique_across_queues(b)`, `unique_key(v)`, `not_unique()` | `fn unique_key(args) -> Option<Value>` | `unique_for = secs`, `unique_until = "..."`, `on_conflict = "..."`, `unique_across_queues = bool`, `unique_by = [params]` |
| Throttling | `throttle(name)`, `throttle_key(k)`, `no_throttle()` | `fn throttle_key(args) -> Option<String>`, `fn throttle() -> Option<Throttle>` (+ `ErasedJob::dyn_throttle`) | `throttle = "name"`, `throttle_by = [params]`, `throttle(concurrency = n, window = "N/period", ..)` |
| Batches | none (membership comes from the collecting client) | none | none |
| Periodic | none on jobs (`Periodic` builder; D19 of periodic: no `cron =` attribute) | none | none |

Merge rules follow `queue`'s (higher layer wins), with the unique design's field-wise merge for `Unique`
(`Disabled` above wins; two `Enabled` merge per field). `*_by = [params]` generates the trait method from
the named parameters in the given order; an explicit setter wins over the trait method. Integers are
seconds (`unique_for`, like `retry_for`). The throttle spec grammar (`concurrency=10`, `window=100/1s per
key`) is parsed at compile time by `hench-macros` with its own copy of the grammar (a proc-macro crate
cannot depend on `hench`); a parity test feeds both parsers one corpus.

### 2.5 Errors and error contexts

- `hench::Error` gains `DuplicateJob { class, digest, holder }`, `InvalidSchedule(String)`,
  `Batch(BatchError)`, `OverLimit(OverLimit)`; `JobError` gains the classes `hench::BatchCancelled` and
  `hench::OverLimit` with constructors and predicates; the internal job name `hench::WorkflowNext` is
  reserved (a user job with that name is `Error::Config`).
- Error contexts added to DESIGN §5's list: `could not enqueue periodic jobs`, `periodic job is invalid`,
  `could not register periodic jobs`, `periodic poller crashed; restarting it`, `could not record a job in
  its batch`, `could not dispatch batch callbacks`, `could not maintain batches`, `batch maintenance
  crashed; restarting it`, `over-limit backoff panicked; using the default`, `over_limit_when panicked`
  (throttle-gate Redis errors use the existing `could not fetch jobs` outage notices).
- Background tasks report through `launcher::Outage` (rate-limited notices), one per task:
  `periodic_outage`, `batch_maintenance_outage`, beside `schedule_outage`.

### 2.6 Hashing and canonical JSON

`crate::canonical` (phase 0): `canonical_json(&Value) -> Result<Vec<u8>>` (no whitespace, numbers as
`serde_json` writes them, object keys sorted by UTF-8 bytes recursively whatever `preserve_order` says,
depth ≤ 128) and `sha256_b64url(&[u8]) -> String` (43 characters). Used by unique digests (`hench-unique/1`
input, golden vectors pinned forever), periodic definition and crontab digests, and limiter keys longer
than 128 bytes (`#` + the first 22 characters). New dependency: `sha2 = "0.10"`.

### 2.7 Heartbeat and rollout

The heartbeat `info` JSON gains `"features": {"unique": 1, "periodic": 1, "batches": 1, "limits": 1}`
(the stored-format version each feature of this process understands; `periodic` only when the cargo
feature is on), plus `"periodic": {poller, group, digest, schedules}` and `"throttles": {name: spec}`.
The dashboard's Busy page shows "lacks: …" on a live process missing a feature that is in use (a lock,
schedule, batch or limiter exists). **Rollout rule** (README): upgrade every worker before any client uses
one of these features; older workers ignore the new fields (unique locks then expire by TTL, batch members
are never counted, `OverLimit` is a failure, newer schedule definitions are skipped).

### 2.8 Dashboard

- Navigation: Dashboard, Busy, Queues, Retries, Scheduled, Dead, **Periodic, Batches, Limits, Locks**,
  Metrics, then custom tabs (the three job sets stay together; the feature tabs follow).
- Switches: `HenchWeb::periodic(bool)`, `batches(bool)`, `limits(bool)`, `locks(bool)`, default `true`;
  off, the tab disappears and its routes answer 404, as `metrics(false)` does. No automatic hiding (it would
  cost a Redis read per page; an empty list explains itself).
- Job pages (queue rows, retries, scheduled, morgue lists and detail pages): chips next to the class
  (`unique`, `throttle: name:key`, `over limit ×n`, `callback`), and feature rows after the built-in rows
  and the Iteration row in one order: **Batch, Periodic, Unique, Throttle, Limited**, then the host's
  `job_info` rows.
- Flash notices keep `flash.rs`'s `kind.count.failed.subject` format with new `NoticeKind`s:
  `PeriodicEnqueued`, `PeriodicPaused`, `PeriodicResumed`, `PeriodicDeleted`, `BatchCancelled`,
  `BatchDeleted`, `MemberSkipped`, `CallbacksDispatched`, `LimitsReset`, `LineCleared`, `LocksReleased`.
- Lists follow the existing rules: bounded literal filters with `*`, "Live" switch, pages of 25, CSRF,
  read-only, cross-site refusal, askama escaping, control characters spelled out. Strings live in
  per-feature parts of `strings.rs`; feature CSS in per-feature embedded assets (`periodic.css`,
  `batches.css`, `limits.css`, `locks.css`), registered in phase 0.
- Metrics pages: "Over limit", "Throttled", "Skipped" columns appear only when some class has a non-zero
  value in the period.

### 2.9 Server (`hench`)

- One command per feature with subcommands, `list` by default: `hench periodic
  [list|show|enqueue|pause|unpause|remove|add|repair|next]`, `hench batches [list|show|cancel|delete|skip]`,
  `hench limits [list|show|reset|clear-line]`, `hench locks [list|show|release|release-all]`.
- `hench stats <periodic|batches|limits|locks>`, each also printed by `hench stats all` when the feature has
  data; the overview is unchanged.
- `hench enqueue` gains `--unique-for`, `--unique-until`, `--unique-key`, `--unique-across-queues`,
  `--on-conflict` and `--throttle NAME[:KEY]`.
- JSON API (`--api`): `GET` for lists and items, `POST /<resource>/{id}/<action>` for actions,
  `DELETE /<resource>/{id}` for removing a resource: `GET /periodic`, `GET /periodic/{name}`,
  `POST /periodic/{name}/{enqueue|pause|unpause}`; `GET /batches`, `GET /batches/{bid}`,
  `POST /batches/{bid}/cancel`; `GET /limits`, `GET /limits/{kind}/{id}`, `DELETE /limits/{kind}/{id}`;
  `GET /locks`, `DELETE /locks/{digest}?jid=`; `POST /jobs` gains the unique and throttle fields (201
  pushed or replaced, 200 dropped duplicate, 409 `on_conflict: "error"`). Phase 3: `PUT/DELETE
  /queues/{name}/limits`.

### 2.10 Worker configuration file (TOML)

Reserved top-level names: `capsules`, `throttles`, `queue_limits`, `periodic` (accepted at the top level and
inside environment sections; an environment named like one of them is an `Error::Config` naming the word).
New scalar keys: `unique_jobs`, `periodic_group`, `periodic_poller`, `periodic_poll_interval`,
`periodic_history`, `batch_ttl`, `batch_linger`, `batch_max_commit_jobs`, `batch_maintenance`; tables:
`[[periodic]]`, `[throttles.<name>]`, `[queue_limits]` (phase 3).

### 2.11 Metrics and logging

| Processor outcome | `stat:processed` | `stat:failed` | per-class metrics | timed | job logger event |
|---|---|---|---|---|---|
| success (or middleware halt) | +1 | | `p` | yes | `done` |
| failure (any verdict), shutdown requeue, hard-shutdown interruption | +1 | +1 | `p`, `f` | no | `fail` |
| interrupted (iterable push back) | +1 | | `p` | yes | `interrupted` |
| over limit while running (rescheduled) | +1 | | `p`, `o` | no | `over_limit` (info) |
| throttled (parked before start) | | | `t` | no | `throttled` (debug) |
| skipped (batch cancelled) | +1 | | `p`, `s` | no | `skipped (batch cancelled)` (info) |

New events follow the per-job log-level rule of DESIGN §9f (skipped for `warn`/`error` jobs). Feature
counters: unique `d`/`r`/`z` per class (`u|`), batches `created`/`complete`/`success`/`death`/`cancelled`
(`b|`), limits `g`/`r` per limiter name (`l|`), periodic per schedule (`fired`, `skipped`, `missed`,
`run_failures` in its hash). Tracing targets are the module paths (`hench::unique`, `hench::periodic`,
`hench::batch`, `hench::limit`).

### 2.12 Testing modes

| Feature | `Client::fake()` / fake `Testing` | inline | default | switch |
|---|---|---|---|---|
| unique | locks enforced in a per-client `UniqueTable` (duplicates return `None`; released at success/failure; `Start` before `perform`; `Expired` kept) | same | **on** | `Testing::unique_jobs(bool)` |
| batches | `BatchTable` model: commit captures members, drains count outcomes (failure = dead), callbacks captured and drained after members | members run at commit, then callbacks | **on** (always) | none |
| rate limiting | unlimited; `InMemory` enforces limits on tokio time (refused jobs stay captured with `at` and `limited`, not rerun by the same drain) | over-limit refusal returned as a failure | **off** (`Unlimited`) | `Testing::limits(TestLimits::InMemory)` |
| periodic | no poller; `Periodic::payload`/`enqueue`/`upcoming`/`validate` | same | n/a | none |

Principle: **push-time, deterministic behaviour is enforced by testing clients by default; time-driven
behaviour is not.** Interplay (each design's testing section): a batch commit consults the `UniqueTable`
with `BATCH_COMMIT`'s rules; an outcome updates the batch table before the unique table releases (the
processor's order); with `InMemory`, a refused job keeps its lock and stays pending in its batch, except
that a refused member of a cancelled batch is skipped; an inline over-limit refusal counts the member as
dead (reason `no_retry`: nothing would run it again); `hench::WorkflowNext` is registered in every testing
registry. One container (`testing::Tables { unique, batches, limits }`) is shared by a client, its clones
and its collecting clients. `Testing::from_hench` copies the unique switch, the throttle definitions (not
the limits mode), the batch defaults and `hench::WorkflowNext`.

### 2.13 Background tasks

| Task | Runs until | Why |
|---|---|---|
| scheduler (existing), orphan check, batch maintenance | quiet | they move existing work; a quiet process hands the work to others |
| periodic poller | the shutdown starts | time-driven enqueuing must not stop during a rolling deploy that quiets old processes first (Sidekiq Enterprise behaves the same); missed ticks are caught up by policy anyway |
| heartbeat renewals (unique locks of running jobs, engine-held permits) | the heartbeat's end | they protect running jobs; sent in one pipeline |

### 2.14 Cargo features and dependencies

Only `periodic` is a cargo feature (default on: `croner` with `jiff`, and `jiff`'s tz database); unique jobs,
batches and rate limiting are always compiled (their only new dependency is `sha2`, shared). `hench-web`
and `hench-server` enable `hench/periodic`. Prelude additions: `Periodic`, `CatchUp`, `Unique`,
`UniqueUntil`, `OnConflict`, `Batch`, `Limiter`, `OverLimit`, `Throttle`.

---

## 3. Combined behaviour

### 3.1 The push pipeline

```text
push / perform_async / perform_bulk / Periodic fire / API move / batch definition
  1. normalize (spine)            options merged (default < class < explicit), queue, jid, created_at;
                                  unique: validate, digest = sha256(canonical(["hench-unique/1", class,
                                  queue|null, key])), strip key/across_queues (or strip `unique` when the
                                  client's unique switch is off);
                                  throttle: payload `throttle` {name, key from setter or Job::throttle_key};
                                  reserved extras dropped
  2. collecting client only       set `bid` (refuse another batch's bid); refuse on_conflict = replace
  3. client middleware            may modify, schedule or halt (halt: no lock, nothing buffered)
  4. revalidate
  5. deliver (exactly one script writes the job and its lock together)
       Redis client, no digest    MULTI (SADD queues + LPUSH | ZADD schedule)       unchanged
       Redis client, digest       UNIQUE_PUSH in chunks of 100 (with_retry safe)   unique §2.8.3
       collecting client          append to the collector; BATCH_COMMIT at commit  batches §2.5.3
       periodic poller            PERIODIC_FIRE (CAS on next + hu_push_one)        periodic §2.5, §2.10
       "enqueue now"              PERIODIC_ENQUEUE_NOW (+ hu_push_one)             periodic §2.5
       scheduler / API moves      MOVE_OUT (never takes locks: digest already set) DESIGN §5, §7
       outcome re-pushes          PUSH_BACK / SETTLE variants (hold, never take)   unique §2.8.4
       fake / inline client       testing tables with the same rules               §2.12
```

`raw_push` returns one result per job (pushed, already pushed, duplicate with holder, replaced, pushed
without lock), mapped to `Option<String>` by the existing methods and to `PushOutcome` by `push_outcome`.
On a collecting client every result is provisional `Pushed`; duplicates come back in
`Committed::duplicates`.

### 3.2 The execution pipeline

```text
fetch (unchanged; phase 3: FETCH_GATE for capsules with limited queues)
parse (invalid JSON → morgue, unchanged)
F = Fetched::from(job as fetched): LockRef (unique), BatchMember (batches), PeriodicRef, ThrottleRef
1. throttle gate (limits, only when the payload names a known throttle)
     granted      → permits held by the engine (keyed by tid, token = jid)
     refused      → member of a cancelled batch? (HGET, only now) → BATCH_SKIPPED cancel + ack → done
                    else reschedule: PUSH_BACK schedule (PUSH_BACK_UNIQUE hold for+delay, at) → done
     unavailable  → requeue front + fetch outage notice (or run unthrottled with fail_open)
2. start_work, job span, `start` log, server middleware chain
3. endpoint (PerformEndpoint::call)
     a. batch member and its batch cancelled (HGET)   → Err(batch_cancelled)
     b. unique `Start` lock                            → UNIQUE_RELEASE (failure logged; job runs)
     c. perform
4. classify (job as fetched for every feature):
     Ok | batch_cancelled (verified) | interrupted | shutdown | over limit (budget left) | failure(verdict)
5. batch accounting (before the outcome write): BATCH_SUCCEEDED | BATCH_SKIPPED | BATCH_FAILED retry/dead
6. outcome write, unique lock work inside the script (reliable fetch; basic fetch: write then LOCK_OP):
     ack (ACK_UNIQUE) | PUSH_BACK queue/schedule (PUSH_BACK_UNIQUE hold) | SETTLE retry/dead/drop
     (SETTLE_UNIQUE hold/release) | REQUEUE (lock unchanged)
7. after the write: engine-held permits released (LIMIT_RELEASE); death handlers; batch dispatch_if /
   refresh_if; periodic RECORD_RUN (spawned); error handlers; metrics and stats as in §2.11
```

| Outcome | 5. batch | 6. outcome script (unique part) | 7. after | handlers |
|---|---|---|---|---|
| success, or server middleware halting | `BATCH_SUCCEEDED` | ack (`ACK_UNIQUE`: release for `Success`/`Start`) | permits; dispatch/refresh; `RECORD_RUN ok` | none |
| skipped: endpoint found the batch cancelled, `BATCH_SKIPPED` confirms | `BATCH_SKIPPED cancel` | ack (`ACK_UNIQUE` release) | permits; dispatch | none |
| `batch_cancelled` returned but the batch is not cancelled | → failure | | | |
| interrupted (iterable) | none | `PUSH_BACK` to the queue end after client middleware (`PUSH_BACK_UNIQUE` hold) | permits; `RECORD_RUN interrupted` | none |
| `hench::Shutdown` while stopping | none | `REQUEUE` front (lock unchanged) | permits | none |
| hard shutdown | none | manager's bulk requeue (lock unchanged) | manager releases permits (bounded 5 s) | none |
| over limit while running, budget left | none | `PUSH_BACK` to `schedule`, job as fetched + `limited` (`PUSH_BACK_UNIQUE` hold `for` + delay, `at`) | permits; `RECORD_RUN rescheduled` | none |
| throttled at the gate | none | as above, before anything ran | no permit held | none |
| failure, retry scheduled (includes over limit past its budget) | `BATCH_FAILED retry` | `SETTLE` retry (`SETTLE_UNIQUE` hold `for` + retry delay) | permits; dispatch; `RECORD_RUN failed` | error handlers |
| failure, will not run again (morgue, `retry: false`, discard, `dead: false`) | `BATCH_FAILED dead` (death fires once) | `SETTLE` dead/drop (`SETTLE_UNIQUE` release) | death handlers; permits; dispatch; `RECORD_RUN failed` | error + death handlers |
| API delete / kill (not the processor) | `BATCH_FAILED dead` (`deleted`/`killed`) first | `REMOVE_UNIQUE` / `KILL_UNIQUE` (release) | dispatch | killed: death handlers when notified |

Why this order: accounting before the outcome write means a crash between them re-runs the job, whose
accounting is then a no-op (the reverse would acknowledge a job its batch never hears of); the lock work
inside the outcome script makes "job settled" and "lock held/released" one atomic step under reliable
fetch; permits are released after the write so a crash can only under-use a slot for one lease, never
over-admit; dispatch and `RECORD_RUN` are informational and retried elsewhere (maintenance, the next run).

### 3.3 Combinations

**Periodic + unique, per tick.** A schedule whose job has `unique_for` (about one period, `until =
success`) skips ticks while the previous job is pending or running: `PERIODIC_FIRE` calls `hu_push_one`
inside its compare-and-set; a held lock gives mode `duplicate` (no push, tick consumed, `skipped` +1, `d`
counted, history `duplicate`). A resent FIRE is a CAS no-op and takes no second lock; under `OOM` nothing
is written and the tick waits for the lease. "Enqueue now" uses the same step (a duplicate returns
`None`). Typed keys are evaluated when the schedule is defined (F4), so any process fires the same digest.
`on_conflict` other than `drop` is refused. Tests X1, X2.

**Unique + retries, death, revival.** `Success` locks are held through retries (`SETTLE_UNIQUE` extends to
`for` + retry delay), released at death, discard, `retry: false` and API delete/kill; `Start` takes the lock
back while a retry waits; `Expired` ignores outcomes. A job revived by hand from the morgue runs without
a lock (unique D14): a duplicate may be pushed meanwhile, and the revived job's success releases nothing
and counts a lapse. With batches: the revived member is still pending in its batch (it was never
detached) and its success counts. Test X5.

**Batches + retries, death, revival, reliable-fetch duplicates.** Accounting is keyed by jid and guarded
by membership in `:pending`, so retries update the failure summary, the first death fires `death` once, a
revival's success completes the batch, and duplicate executions (orphan recovery of a live partitioned
process, a client resend after a dropped connection, basic fetch) are counted once. A unique member's lock
follows its own rules; a stale copy's acknowledgement finds the lock released (or another holder's) and
does not resurrect it. Tests X6, X11.

**Rate limiting + reliable fetch + iterable jobs + batches.** Reschedules (over limit or throttled) are
written by `PUSH_BACK` in its `schedule` mode, atomic with the acknowledgement and guarded by `LPOS`, so a
lost reply never duplicates or loses the job; they are not batch outcomes (the member stays pending; past
its budget the job fails and is counted as a failure). An iterable job refused by a limiter on an item
saves its cursor, runs `on_stop`, returns the `OverLimit` and is rescheduled; the next execution resumes
after the last item; its batch completes when it finishes all items. Engine-held permits are keyed by jid,
so a job recovered after a crash re-acquires its own live lease; renewals ride the heartbeat. A throttled
member of a cancelled batch is skipped at its first refusal (F7). Tests X10, X12.

**Periodic + batches.** Templates cannot carry `bid`/`batch_callback`. A periodic job that defines a batch
per tick uses `Batch::with_bid(ctx.client(), ctx.jid())?.commit_id(ctx.jid())`, so a re-run (crash between
commit and acknowledgement) commits nothing new. Test X4.

**Periodic + rate limiting.** Ticks are enqueued on time and parked by the throttle like any job; with
`unique_for` the parked tick keeps its lock (`PUSH_BACK_UNIQUE` hold), so later ticks are skipped as
duplicates instead of piling up. `RECORD_RUN` reports `rescheduled` for over-limit runs, nothing for
throttled ones. Test X3.

**Unique + rate limiting.** "At most one waiting and one running, never two running" is `Start` plus a
per-key concurrency throttle of 1 (`Throttle::new(..).concurrency_per_key(1)` with `throttle_by`): the
`Start` lock is released only when `perform` starts (after the gate), so a parked job keeps it. A job
parked in `schedule` records `at` in its lock and can be replaced by a debounce push. Tests X8, X9.

**Unique + batches.** Locks are taken by `BATCH_COMMIT` with the frozen library; intra-commit duplicates
and already-held jids are left out (`Committed::duplicates`); `error` refuses the commit; `replace` is
refused at push time; callback templates carry no `unique`; a skipped member's lock is released by its
acknowledgement. Tests X6, X7, X13, X15.

**API delete and kill.** For a job that is a batch member and unique: `BATCH_FAILED dead` (deleted/killed)
first, then `REMOVE_UNIQUE`/`KILL_UNIQUE` (removal and release in one script, sent once, presence checked
on error), then dispatch; `kill_all` accounts each chunk of 100 before moving it and releases the moved
chunk's locks after (`UNIQUE_RELEASE_MANY`). `Queue::clear`/`JobSet::clear` touch neither (documented).
Test X14.

### 3.4 Crash points across features

| Crash between | Effect | Recovery |
|---|---|---|
| gate grant and execution | permit leased, job in progress | lease ends (≤ 60 s); job recovered by orphan recovery, re-acquires by jid |
| batch accounting and the outcome write | member counted, job still in progress | job re-runs; accounting a no-op; the batch may have fired `success` before (at-least-once, documented) |
| outcome write and permit release | slot leased | lease ends (under-use only) |
| outcome write and dispatch | event in the outbox | maintenance dispatches after the grace period |
| LEASE and FIRE (periodic) | schedule leased | due again when the lease ends; FIRE's CAS prevents a second push |
| push of a unique job and its reply | job and lock written | resend: "already pushed" (same jid) |
| batch commit and its reply | definition written | resend with the same commit id: `already` |

### 3.5 Cost and sizing

Round trips per job (reliable fetch; today a plain job costs a fetch and an acknowledgement):

| Feature on a job | Extra round trips per execution | Redis CPU (measured by the designs) |
|---|---|---|
| unique | none (lock work inside the push and outcome scripts) | push about 3x a plain push (29k/s vs 100k/s per core); outcome +1 `HGET` and a `DEL`/`PEXPIRE` |
| batch member | +1 accounting script, +1 `HGET` (cancellation check) | accounting median 24 µs; commit 3.3 µs per member |
| throttled | +1 `LIMIT_ACQUIRE`, +1 `LIMIT_RELEASE` (+ renewals per beat) | acquire about 3.5 commands (31k-34k/s per core with bookkeeping) |
| periodic | +1 spawned `RECORD_RUN` | negligible |
| all of them | up to +5 round trips (7 instead of 2, one of them spawned) | dominated by the acquire and accounting scripts |

Memory: unique lock + index entry about 436 B; batch about 200 B per batch, 72 B per pending member,
300 B per failed member; periodic schedule about 1 KB plus 200 B per history entry; limiter state 88 B
(leaky, bucket), about 10 B per unit of a window, 32 B per concurrency holder, 180 B of stats and index per
identity for 24 h. Jobs that use no feature pay nothing: their scripts are byte for byte today's, and the
processor's extra work is reading four optional fields of the parsed payload (`unique`, `bid`, `periodic`,
`throttle`).

---

## 4. Common infrastructure (phase 0, the integrator)

Phase 0 is one branch, done by the integrator before the lanes start, that changes every shared file once.
It adds the shared types completely (they are fully specified) and every hook point with a no-op or
pass-through body in the feature modules, so that afterwards each lane only edits files it owns. **Its gate:
the full existing suite passes unchanged, scripts of jobs without features are byte for byte today's, and
the new payload fields, options and keys have their unit tests.**

### 4.1 New crate-wide modules

| Module | Contents |
|---|---|
| `crate::canonical` | `canonical_json`, `sha256_b64url`; golden vectors (§2.6). |
| `crate::lua` | `prelude.lua` (`h_now_ms`, `h_score`, `h_err`, `h_extend_at_least`), the script builder (prelude + libraries + body → `LazyLock<Script>`), the `SCRIPT LOAD` and forbidden-command tests, the `lua-replicate-commands` start-up check. |
| `crate::pro` (crate-internal glue) | `Fetched` (per-execution feature state from the job as fetched), the `Gate` and `Classified` enums, `on_beat`, `spawn_background`, `info`, `validate`; it only calls the feature modules' hook functions in the fixed order of §3.2. |
| `crate::api::events` | the per-minute counter reader (minute periods ≤ 8 h) used by `UniqueLocks::events`, `Limits::events` and the Batches summary. |
| `unique/`, `periodic/`, `batch/`, `limit/` | each with `types.rs` complete (payload and option types, public enums, `BatchError`, `OverLimit`, and the definition types the spine names: `Unique`, `UniqueOption`, `PushOutcome`, `Throttle`, `Limit`, `Interval`, `PeriodicMeta`, `BatchCallback`) and hook files with no-op bodies (below). `unique/lua/lib.lua` complete: the lab prototype plus `hu_check`/`hu_push_one` (the frozen interface). |

### 4.2 Shared files changed once

| File | Phase 0 change |
|---|---|
| `Cargo.toml` (hench) | `sha2`; optional `croner`, `jiff`; feature `periodic` (default); `hench-web`/`hench-server` enable it. |
| `lib.rs` | modules, re-exports, prelude (§2.14). |
| `keys.rs` | every helper and constant of §2.1, with tests (periodic's prefix-length test included). |
| `payload.rs` | the five fields of §2.3 with lenient deserializers; `RESERVED_KEYS`; round-trip tests. |
| `options.rs` | `unique` and `throttle` options, setters, merge rules. |
| `job.rs` | `Job::unique_key`, `Job::throttle_key`, `Job::throttle`; `ErasedJob::dyn_throttle`; `JobSetter`/`JobBuilder` setters and `push_outcome`/`push_bulk_outcomes`; `pub(crate)` accessors that the lanes' `impl JobContext` blocks need (the feature methods of `JobContext` live in the feature modules). |
| `error.rs` | the variants and classes of §2.5. |
| `client.rs` | normalization hooks (`unique::normalize`, `limit::normalize`; phase 0 bodies strip `unique` so behaviour is unchanged); `raw_push -> Vec<PushResult>`; `PushOutcome`; `ClientMode::Collect` calling `batch::collect::append`; Redis delivery of digest-carrying jobs through `unique::push::deliver` (phase 0: never reached); `with_unique_jobs`; `BulkPush` setters. |
| `config.rs` | every new `Config` field and builder method with its bounds (§2.10's keys), `pro::validate` (calls `periodic::validate_all`, `limit::Throttles::build`; phase 0 bodies accept everything); registration of `hench::WorkflowNext` (phase 0: a stub job failing with a clear error until lane B lands it). |
| `processor.rs` | the pipeline of §3.2 with hook calls: `limit::engine::gate`, `batch::account::before_outcome`, `unique::engine::lock_op`, `limit::engine::release`, `batch::dispatch::after_outcome`, `periodic::record_run::spawn`, the over-limit and skipped branches (classification), metrics outcomes. |
| `fetch.rs` | `acknowledge`/`settle`/`push_back` take `Option<&unique::LockOp>`; `Some` calls `unique::engine::{ack, settle, push_back}` (phase 0: the plain scripts). |
| `middleware.rs` | `PerformEndpoint::call` calls `batch::endpoint::check(ctx)` then `unique::endpoint::release_start(ctx)` before `perform_json`. |
| `stats.rs` | `Work` gains `#[serde(skip)] fetched: Arc<pro::Fetched>`; `Stats::running()` for renewals. |
| `launcher.rs` | `pro::spawn_background` (periodic poller until `stopping`, batch maintenance until quiet), `pro::on_beat` (unique and limit renewals in one pipeline), `pro::info` (`features`, `periodic`, `throttles`), the start-up checks, `Engine` fields (`periodic_outage`, `batch_maintenance_outage`, `throttles`, `permits`). |
| `manager.rs` | after the hard-shutdown requeue, `limit::engine::release_slots`. |
| `metrics/tracker.rs`, `metrics/query.rs` | `Outcome::{OverLimit, Throttled, Skipped}`, fields `o`, `t`, `s`, `Counts` fields. |
| `job_logger.rs` | events `over_limit`, `throttled`, `skipped`; the span's `batch_event` field. |
| `testing.rs` | `testing::Tables { unique, batches, limits }` shared by a client, its clones and its collectors; hook calls at push (`unique::fake::push`, `batch::model::collect`), before perform (endpoint hooks), after the outcome (`batch::model::account`, `unique::fake::after`, `limit::memory::after`); `Testing::{unique_jobs, limits}`; `from_hench` copying. |
| `api/mod.rs`, `api/record.rs`, `api/sorted.rs`, `api/queue.rs` | removal hooks in every single-job delete and kill path and in `kill_all` chunks: `batch::api::before_removal` (accounting) then the removal script chosen by `unique::api::removal_script` (phase 0: today's), then `batch::api::after_removal` (dispatch). Feature factories (`Api::unique_locks`, `periodic`, `batches`, `limits`) and `JobRecord` accessors live in the lanes' `api/<feature>.rs` as `impl` blocks. |
| `cli.rs` | reserved table names, scalar keys, dispatch of `[[periodic]]` to `periodic::toml` and `[throttles.*]`/`[queue_limits]` to `limit::toml` (phase 0: refuse with "not supported yet"). |
| `test_util.rs` | `TestRedis::keys_without_ttl(patterns)`; `TestRedis::builder()` short intervals for the features (periodic 100 ms poll / 2 s lease / 200 ms check; batch maintenance 100 ms, grace 100 ms). |
| `iterable/mod.rs` | none in phase 0 (lane B owns it). |
| `hench-macros/src/lib.rs` | option dispatch to `unique.rs` and `throttle.rs` (phase 0: options parsed and rejected with "not supported yet"); the throttle spec grammar copy with its parity test. |
| `hench-web` router, layout, navigation, `views.rs`, `flash.rs`, `strings.rs`, `assets.rs`, `config.rs` | navigation and switches (§2.8), routes delegated to `pages::{periodic, batches, limits, locks}::routes()` (phase 0: 404), the job-row hook `views::feature_rows(&JobRecord)` calling each page module's row builder in the fixed order, the new `NoticeKind`s, per-feature `strings` parts, per-feature CSS assets, the "lacks: …" chip. |
| `hench-server` CLI enum, JSON router, `monitor.rs` | subcommands and routes delegated to `commands::{periodic, batches, limits, locks}` and `json_api::{…}` (phase 0: "not supported yet"); `hench stats` section dispatch; `hench enqueue` unique and throttle flags (complete: they only set payload fields). |
| `docs/DESIGN.md`, `docs/STATUS.md` | §7's shared rows and empty §9g–§9k headings; STATUS sections per feature. |

### 4.3 Hook signatures (crate-internal; frozen at the end of phase 0)

As implemented by phase 0 (the deviations from the first draft of this section, and why, are
listed in DESIGN §9k):

```rust
// crate::pro — built once per execution from the job as fetched (processor), or from the
// payload (testing executor).
pub(crate) struct Fetched {
    pub(crate) lock: Option<unique::LockRef>,          // unique
    pub(crate) member: Option<batch::BatchMember>,     // batches
    pub(crate) periodic: Option<periodic::PeriodicRef>,
    pub(crate) throttle: Option<limit::ThrottleRef>,   // resolved against the throttles by the gate
}
impl Fetched { fn from_payload(&JobPayload) -> Fetched; fn is_plain(&self) -> bool; }

pub(crate) enum Gate { Run, Refused(limit::Reschedule), Unavailable(Error), Failed(JobError) }
pub(crate) enum Classified {
    Success, Skipped, Interrupted, Shutdown,
    OverLimit(limit::Reschedule),                  // delay, label, n (budget left)
    Throttled(limit::Reschedule),                  // refused at the gate
    Failure(Box<retry::Verdict>),                  // includes over limit past its budget
}
pub(crate) struct limit::Reschedule { delay: Duration, label: String, n: u32 }
    // fn stored(&self, job: JobPayload, now: f64, now_ms: i64) -> Result<Stored>
pub(crate) enum limit::OverLimitDecision { Reschedule(limit::Reschedule), Exhausted(JobError) }
pub(crate) enum batch::Accounting { Nothing, Recorded(batch::Report), NotCancelled }
pub(crate) struct unique::LockOp { lock: unique::LockRef, action: unique::LockAction /* Hold | Release */ }

// processor.rs, in order (each lane fills its functions; each is called only for the jobs that
// use its feature):
limit::engine::gate(&Engine, tid: &str, &UnitOfWork, &JobPayload, &Fetched) -> Gate      // async
batch::endpoint::member_cancelled(&Engine, &Fetched) -> bool                              // async, refusals only
batch::endpoint::check(&JobContext) -> Result<(), JobError>                               // async, PerformEndpoint
unique::endpoint::release_start(&JobContext)                                              // async, PerformEndpoint
limit::engine::over_limit(&Engine, &JobError, &JobPayload /* as fetched */) -> Option<OverLimitDecision>
batch::account::before_outcome(&Engine, &Fetched, &Classified) -> batch::Accounting       // async
unique::engine::lock_op(&Fetched, &Classified, Option<&Settlement>) -> Option<LockOp>
Fetcher::{acknowledge_with, settle_with, push_back_with}(.., Option<&LockOp>)             // None = the plain write
unique::engine::{ack, settle, push_back}(&Fetcher, &UnitOfWork, .., &LockOp)              // async, the Some case
limit::engine::release(&Engine, tid: &str)                                                // async, after the write
batch::dispatch::after_outcome(&Engine, Option<batch::Report>)                            // async
periodic::record_run::spawn(&Engine, &Fetched, &Classified, elapsed: Duration)            // filters inside

// launcher, manager, heartbeat:
pro::on_beat(&Engine)                     // unique::engine::renew, then limit::engine::renew
pro::background_tasks(&Arc<Engine>) -> Vec<BoxFuture<'static, ()>>
    // periodic::poller::task(&Arc<Engine>), batch::maintenance::task(&Arc<Engine>): Option<..>
pro::info(&Engine) -> Map                 // "features" from {unique,periodic,batch,limit}::FORMAT, "throttles"
pro::validate(&Config) -> Result<()>      // periodic::validate_all; throttles warned
pro::start_checks(&RedisPool)             // lua::check_effects_replication
limit::engine::release_slots(&Engine, tids: &[String])   // after the hard-shutdown requeue

// client:
unique::normalize(&mut JobPayload, enabled: bool) -> Result<()>
limit::normalize(&mut JobPayload) -> Result<()>
batch::collect::normalize(&Collector, &mut JobPayload) -> Result<()>
batch::collect::append(&Collector, Vec<JobPayload>) -> Result<Vec<PushResult>>
unique::push::deliver(&RedisPool, Vec<JobPayload>, now: f64) -> Result<Vec<PushResult>>   // async
unique::fake::admit(&Client, &[JobPayload]) -> Vec<PushResult>
enum PushResult { Pushed, AlreadyPushed, Duplicate { holder }, Replaced { replaced }, PushedWithoutLock { error } }

// testing executor (jobs that use a feature):
limit::memory::gate(&Client, &Fetched) -> Gate
batch::model::account(&Client, &Fetched, &Classified)
unique::fake::after(&Client, &Fetched, &Classified)
limit::memory::after(&Client, &Fetched, &Classified)

// API removals:
batch::api::before_removal(&RedisPool, &[JobPayload], batch::api::Removal) -> Vec<batch::Report>   // async
unique::api::{delete_from_set, delete_from_queue}(&RedisPool, key, value, &JobPayload) -> Option<Result<bool>>
unique::api::kill_from_set(&RedisPool, source, unique::api::MorgueBounds, value, &JobPayload) -> Option<Result<i64>>
unique::api::release_many(&RedisPool, &[JobPayload])                                      // kill_all chunks
batch::api::after_removal(&RedisPool, Vec<batch::Report>)

// surfaces:
periodic::toml::schedules(name, &toml::Value) -> Result<()>; limit::toml::{throttles, queue_limits}(..)
hench-web pages::<feature>::{AVAILABLE, FEATURE, routes(), job_rows(&Ctx, &JobRecord) -> FeatureRows, in_use(&Ctx)}
hench-server commands::<feature>::{<Feature>Args, run, stats, has_data}; json_api::<feature>::routes(&Api, read_only)
hench-macros unique::parse_option, throttle::parse_option (spec.rs: the grammar copy)
```

A lane that needs a hook changed asks the integrator; the change lands on the phase-0 branch and both lanes
rebase. Lane-local tests exercise their hooks through the real processor (the hooks are already wired).

---

## 5. Per-feature work after phase 0

| Feature | Lane | Remaining work (in each design's order) | Spec |
|---|---|---|---|
| Unique jobs | A, first | normalization and digest; `UNIQUE_PUSH` and `unique::push::deliver`; engine scripts (`ACK_UNIQUE`, `SETTLE_UNIQUE`, `PUSH_BACK_UNIQUE`, `UNIQUE_LOCK_OP`, `UNIQUE_RELEASE`, `UNIQUE_RENEW`) and their hooks; API removal scripts and `UniqueLocks`; `UniqueTable`; macro options; Locks pages; `hench locks`; docs §9g | unique §2.6–§2.20 |
| Periodic jobs | A, second | timing (croner/jiff wrapper), definitions and digests, catch-up planning; the nine scripts (`PERIODIC_FIRE` with `hu_push_one`); poller, registration, data-loss check; `PeriodicSet`; `RECORD_RUN`; TOML `[[periodic]]`; Periodic pages; `hench periodic`; docs §9h; `examples/periodic.rs` | periodic §2.2–§2.15 |
| Rate limiting | B, first | `Limiter`, `Permit`, spec grammar, scripts; over-limit reschedule and iterable integration; in-memory backend; throttles (registry, gate, held permits, renewal, shutdown release, TOML, macro); `Limits` API; Limits pages; `hench limits`; docs §9j | ratelimit §2.3–§2.21 |
| Batches | B, second | scripts and model with the parity proptest; collector and `BATCH_COMMIT` (with the unique section against the frozen library); `Batch`, `Committed`, `with_bid`; accounting, endpoint check, dispatch, maintenance; `BatchTable`; workflows and `hench::WorkflowNext`; `Batches` API; Batches pages; `hench batches`; docs §9i | batches §2.3–§2.20 |

Within lane B either order works (they share no file); rate limiting first is recommended because its
over-limit branch exercises the reschedule path that batches must treat as a non-outcome.

---

## 6. Implementation plan

### 6.1 Phases

| Phase | Who | Content | Exit gate |
|---|---|---|---|
| 0 | integrator | §4 | existing suite green; no behaviour change; hook signatures reviewed by both lane owners |
| 1 | lane A ‖ lane B | §5, each lane in its own worktree, branching from phase 0 | each lane's gate: `cargo fmt`, `clippy -D warnings`, `cargo test --workspace --all-features`; its design's tests |
| 2 | integrator | merge A then B (or B then A: the files are disjoint); one-pipeline renewals; cross-feature tests §8; soak scenario `pro`; performance gate; DESIGN §9k, README "Using the features together", STATUS | full gate 3 times, soak short run, no plain-job throughput regression beyond noise (X22) |
| 3 | lane B owner (then A) | queue limits (`FETCH_GATE`, `fetch.rs` changes allowed now; the rate-limiting design's own "phase 3"), staged batch commits (the batches design's "phase 2"), the iterable cancellation checkpoint and the optional cancellation cache (its "phase 3"); unique lean mode if asked | as phase 1 |

### 6.2 File ownership during phase 1

| Path | Owner |
|---|---|
| `crates/hench/src/unique/**`, `crates/hench/src/periodic/**` | lane A |
| `crates/hench/src/api/unique.rs`, `api/periodic.rs` | lane A |
| `crates/hench/tests/unique*.rs`, `tests/periodic*.rs`, `examples/periodic.rs` | lane A |
| `crates/hench-macros/src/unique.rs` and its trybuild cases (`tests/ui/unique_*`) | lane A |
| `crates/hench-web/src/pages/{locks,periodic}.rs`, `templates/{locks,lock,periodic,periodic_entry}*.html`, `strings/{locks,periodic}.rs`, `assets/{locks,periodic}.css`, `tests/{locks,periodic}.rs` | lane A |
| `crates/hench-server/src/commands/{locks,periodic}.rs`, `json_api/{locks,periodic}.rs`, their tests | lane A |
| `crates/hench/src/batch/**`, `crates/hench/src/limit/**`, `crates/hench/src/iterable/**` | lane B |
| `crates/hench/src/api/batch.rs`, `api/limits.rs` | lane B |
| `crates/hench/tests/batch*.rs`, `tests/limit*.rs`, `tests/throttle*.rs` | lane B |
| `crates/hench-macros/src/throttle.rs` and its trybuild cases (`tests/ui/throttle_*`) | lane B |
| `crates/hench-web/src/pages/{batches,limits}.rs`, `templates/{batches,batch,limits,limit}*.html`, `strings/{batches,limits}.rs`, `assets/{batches,limits}.css`, `tests/{batches,limits}.rs` | lane B |
| `crates/hench-server/src/commands/{batches,limits}.rs`, `json_api/{batches,limits}.rs`, their tests | lane B |
| everything else (every file of §4.2, `crate::{canonical, lua, pro}`, `api/events.rs`, `tests/pro_features.rs`, `examples/soak`, `ci/`, shared docs) | integrator |

Lane rules: no edit outside owned files; the frozen interfaces (hook signatures, the unique `lib.lua`
interface, payload and option types, key helpers, `NoticeKind`s) change only through the integrator;
each lane writes its DESIGN section text (§9g/§9h or §9i/§9j) and README sections in its own branch under
the headings phase 0 created, and its STATUS lines under its section, so merges touch disjoint lines;
cross-lane behaviour is tested in phase 2 (lane B may test `BATCH_COMMIT`'s unique section at script level
from day one, since the library exists after phase 0).

### 6.3 Order inside the lanes

- Lane A: A1 unique normalization, `UNIQUE_PUSH`, delivery, `push_outcome`; A2 engine scripts and hooks
  (ack/settle/push back/start release/renew), API removal scripts; A3 `UniqueTable`, `UniqueLocks`, macro
  options; A4 Locks pages, `hench locks`; A5 periodic timing, definitions, planning (pure, property tests);
  A6 periodic scripts with the lock step, poller, registration; A7 `PeriodicSet`, `RECORD_RUN`, TOML;
  A8 Periodic pages, `hench periodic`; A9 docs.
- Lane B: B1 limiters, permits, scripts, over-limit reschedule, iterable integration, in-memory backend;
  B2 throttles (gate, held permits, renewals, manager release, TOML, macro); B3 batch scripts, model, parity
  proptest; B4 collector and `BATCH_COMMIT` (unique section), `Batch`, `with_bid`; B5 accounting, endpoint
  check, dispatch, maintenance, `BatchTable`; B6 workflows; B7 `Limits` and `Batches` APIs; B8 pages;
  B9 `hench limits`, `hench batches`; B10 docs.

### 6.4 What the integrator wires in phase 2

1. Merge; resolve nothing structural (disjoint files); rerun every gate.
2. Send `UNIQUE_RENEW` and `LIMIT_RENEW` in one heartbeat pipeline.
3. Check the pipeline order end to end with one job that uses every feature (X15, X19).
4. `tests/pro_features.rs` (§8), the soak scenario `pro` (`examples/soak`, `ci/soak.sh`), X22's performance
   gate (a plain-job throughput run before and after, same machine).
5. Dashboard: navigation, switches, job rows on a job carrying every feature; a browser test of the four
   new tabs in both themes.
6. Docs: DESIGN §9k and the shared rows of §7, README "Using the features together" (recipes: unique
   periodic jobs, throttled batch members, debounce, one-at-a-time per key, idempotent batch creation;
   the rollout rule; sizing), STATUS.

---

## 7. DESIGN.md additions

| Section | Addition |
|---|---|
| §1 | feature flag `periodic` (default); new dependency `sha2`; optional `croner`, `jiff`. |
| §2 keys table | every row of §2.1 here (the two rows marked "later" when phase 3 lands), the TTL policy paragraph, the per-minute counter convention, the residual identity/name collision note. |
| §2 payload table | `unique`, `periodic`, `batch_callback`, `throttle`, `limited`; `bid` semantics (membership only through commits); the lenient-read and job-as-fetched rules. |
| §3 | `Job::unique_key`, `Job::throttle_key`, `Job::throttle`; macro options `unique_for`, `unique_until`, `on_conflict`, `unique_across_queues`, `unique_by`, `throttle`, `throttle_by`, `throttle(..)`; `JobContext::{unique, release_unique_lock, periodic, batch, batch_callback, batch_status, is_batch_cancelled, check_batch}`; `JobError::{BATCH_CANCELLED, OVER_LIMIT}`; the reserved job name `hench::WorkflowNext`. |
| §4 | the push pipeline of §3.1 here; `push_outcome`/`PushOutcome`, `Error::DuplicateJob`, `with_unique_jobs`; collecting clients (`ClientMode::Collect`), `Client::batch`, `batch_status`, `with_batch_defaults`; `raw_push`'s per-job results. |
| §5 | the execution pipeline of §3.2 and its outcome table; the throttle gate; over-limit reschedules; batch accounting; the unique outcome-script variants; heartbeat renewals and the `features` map; background tasks (§2.13); error contexts (§2.5); start-up `lua-replicate-commands` check. |
| §6 | the endpoint's two hooks and their order; "server middleware passes the context it received to `next`". |
| §7 | `UniqueLocks`, `PeriodicSet`, `Batches`, `Limits`, `Queue::{limits, set_limits, clear_limits}` (phase 3); delete and kill: batch accounting, then the unique removal script; `JobRecord::{unique, periodic, batch_callback, limited, throttle}`; the per-minute reader. |
| §8 | the testing matrix and principle of §2.12; `Testing::{unique_jobs, limits}`; testing tables. |
| §9 | dashboard tabs, switches, routes, job rows, notices (§2.8); server commands, stats sections, JSON API and `enqueue` flags (§2.9); metrics fields `o`, `t`, `s`. |
| §9a | ownership: lanes A and B and the integrator's files (§6.2). |
| §9e | reserved TOML names and new keys (§2.10). |
| §9g | Unique jobs (unique.md condensed). |
| §9h | Periodic jobs (periodic.md condensed). |
| §9i | Batches and workflows (batches.md condensed). |
| §9j | Rate limiting and throttling (ratelimit.md condensed). |
| §9k | The features together: the conventions of §2, the pipelines of §3, the rollout rule. |
| §10 | rows for every new test binary (§8 and the designs' test plans), the soak scenarios `batches`, `throttled`, `pro`. |

---

## 8. Cross-feature test matrix (phase 2, `crates/hench/tests/pro_features.rs` unless noted)

Each design's own test plan stays with its lane. These tests need two or more features and belong to the
integrator; all use `TestRedis` (a private `redis-server` per test) and short intervals.

| ID | Scenario | Asserts |
|---|---|---|
| X1 | Periodic + unique: `every(1 s)` schedule whose job sleeps 2.5 s, `unique_for` 10 s | ticks during the run are `duplicate` (history, `skipped`, the unique counter `d`); one job at a time; the next tick after success fires; a FIRE resent by a proxy dropping replies takes no second lock; `OOM` during a duplicate tick writes nothing |
| X2 | Periodic typed keys: `Periodic::job::<J>` with `unique_by = [a]` and `throttle_by = [b]`, fired by a process without `J` registered | stored template carries the explicit keys; digest and throttle key equal a typed push's |
| X3 | Periodic + throttle + limiter | throttled ticks parked, no `RECORD_RUN`; a limiter refusal inside the job gives `RECORD_RUN rescheduled`; with `unique_for`, later ticks are duplicates while the tick is parked |
| X4 | Periodic job creating a batch per tick with `Batch::with_bid(ctx.jid())` + `commit_id`, `Launcher::abandon` after the commit and before the ack | after recovery the re-run commits nothing (`already_applied`); one batch per tick |
| X5 | Unique + retries + death + revival | lock held across two retries (PTTL ≥ retry delay + `for`); released at death; revived job runs without lock, its success counts `z`; a duplicate pushed meanwhile keeps its own lock |
| X6 | Unique + batch commit: intra-commit duplicate, explicit jid holding the lock, `error`, `replace`, fake and inline clients | Redis and testing tables agree: `Committed::duplicates`, `DuplicateJob` with nothing written, `InvalidJob` at push |
| X7 | Unique + batch cancellation: `Start` member of a cancelled batch | `perform` not called; skipped; lock released by the acknowledgement; `Success` member likewise |
| X8 | Unique + throttle: parked job | lock kept with TTL ≥ `for` + delay and `at` set; a `Replace` push replaces the parked job; a `Start` job's lock is not released by the gate |
| X9 | Unique + limiter refusal while running | `PUSH_BACK_UNIQUE` hold; `Start` lock taken back if free; no lapse counted |
| X10 | Batch + rate limiting: members over limit and throttled; budget exhausted; batch cancelled while members are parked | members stay pending while rescheduled; exhaustion counted as a failure; cancelled members skipped at their next refusal (not parked again); counts exact |
| X11 | Batch + unique + reliable-fetch duplicate: abandon a process mid-member (unique `Success`) | member counted once; lock released once, never resurrected by renewal or the stale copy |
| X12 | Iterable member over limit mid-iteration, batch of three such members | cursor kept, `on_stop` then `on_resume`, executions counted; batch `complete`/`success` only after the last item of each |
| X13 | Callback templates of a batch whose callback job type is unique | no `unique` field stored; dispatch pushes; no `z` counted at their acknowledgements |
| X14 | API delete and kill of a unique batch member; `kill_all` over 250 such jobs | batch counts dead (`deleted`/`killed`), `death` once; locks released; batches of 100 accounted before the move |
| X15 | One job using every feature (unique `Start`, throttle, batch member, periodic tick, limiter inside) | hook order, observed through Redis state snapshots taken by a test server middleware, by the job itself and by a `test-util` probe after each hook: gate → cancellation check → `Start` release → perform → accounting → outcome script → permit release → dispatch → `RECORD_RUN` |
| X16 | Metrics: success, over limit, throttled, skipped, failure for one class | `p`, `f`, `o`, `t`, `s` as in §2.11; `ms` only for timed outcomes; dashboard columns shown only when non-zero (hench-web test) |
| X17 | Heartbeat `features`: a fake process `info` without `"batches"` while a batch exists | Busy page chip "lacks: batches"; JSON `/processes` exposes `features` |
| X18 | Testing modes together: fake client with unique on, batches, `TestLimits::InMemory` under `tokio::time::pause`; inline client | drain order and outcomes as §2.12; inline over-limit member counts dead; `WorkflowNext` registered with an explicit registry |
| X19 | Dashboard (hench-web tests): navigation order, each switch off → 404 and tab gone, job page of X15's job | rows in the order Batch, Periodic, Unique, Throttle, Limited, then `job_info` rows; notices with the new kinds |
| X20 | Server (hench-server tests): `hench stats all` with every feature in use; `hench enqueue --unique-for 1m --throttle stripe:42`; JSON `POST /jobs` with both | sections printed; payload fields set; 200/409 rules |
| X21 | Soak scenario `pro` (`examples/soak`, `ci/soak.sh`): random mix (periodic `every(1 s)` with `unique_for`, throttled batch members, limiters inside jobs, cancels) with `kill -9`, SIGTERM, Redis restarts and the reply-dropping proxy | invariants: each (schedule, tick) enqueued at most once; batch counts consistent and events once; no lock held by a job that left the system for longer than its TTL; concurrency throttles never exceed their limit except counted overages; no key without TTL except §2.1's configuration and index keys |
| X22 | Performance gate: plain jobs (no feature) throughput before phase 0 and after phase 2, same machine | within noise (target ±3 %); per-feature costs within §3.5's figures |
| X23 | Key sweep after every X test | `TestRedis::keys_without_ttl` lists only `periodic*`, `queue-limits`, `unique-locks`, `batches`, `batch-events`, `limits` and the existing hench keys |
| X24 | Mixed versions: payloads carrying unknown future values (`unique.until = "x"`, `throttle` naming an unknown throttle, `spec.v = 2`) | jobs still parse and run (lenient reads; unthrottled with a warning; schedule skipped by the old process) |

---

## 9. Owner decisions (recommendation first)

### 9.1 New decisions from the cross-feature review

| # | Decision | Recommendation | Alternatives |
|---|---|---|---|
| X1 | Implementation structure | **Phase 0 by the integrator (all shared types and hook points), then two lanes with disjoint files (A: unique → periodic; B: rate limiting → batches), then an integration phase** | Four lanes (conflicts in about twenty shared files); features one after the other (no parallelism). |
| X2 | Processor pipeline order | **Gate → cancellation check → `Start` release → perform → batch accounting → outcome script with lock work → permit release → dispatch → `RECORD_RUN`** (§3.2) | Any other order breaks a contract: accounting after the ack loses outcomes on a crash; release before the outcome write over-admits. |
| X3 | Rename `Job::unique_args` to `Job::unique_key` | **Yes** (symmetry with `Job::throttle_key`) | Keep Sidekiq-style `unique_args`. |
| X4 | One hash and one canonical JSON writer for every digest | **SHA-256/base64url and recursive key sorting everywhere** (unique digests, periodic definitions, limiter keys) | SHA-1 for internal identifiers (`sha1_smol` is already built) at the cost of audit findings and, for periodic, digests that depend on `preserve_order`. |
| X5 | Rollout markers | **Heartbeat `"features"` map with format versions + a generic dashboard warning; README rule "upgrade workers first"** | Per-feature flags; refusing feature use while an old process is alive (couples clients to workers). |
| X6 | Testing defaults | **Push-time deterministic features on (unique, batches), time-driven off (limits, periodic polling)** | Everything on (nondeterministic drains with limits) or everything off (unique as Sidekiq Ent advises). |
| X7 | Metrics of skipped batch members | **`Outcome::Skipped` with a new `s` field, not timed** | Count them as successes. |
| X8 | Throttle refusal of a member of a cancelled batch | **Check `cancelled_at` on refusal and skip instead of parking** | Park it until its slot (up to `max_delay`, repeatedly). |
| X9 | Uniqueness on batch callback templates | **Stripped** | Take locks in the dispatch script (more keys and work for no benefit). |
| X10 | Idempotent batch creation by a job that may re-run | **`Batch::with_bid(client, ctx.jid())` + `commit_id`; `BatchError::Exists`** | No API; document duplicates. |
| X11 | Dashboard navigation and switches | **Dashboard, Busy, Queues, Retries, Scheduled, Dead, Periodic, Batches, Limits, Locks, Metrics; `HenchWeb::{periodic,batches,limits,locks}(bool)` default on; one job-row order** | Hide unused tabs automatically (a Redis read per page); tabs next to related sets (Periodic after Scheduled). |
| X12 | CLI and JSON API shape | **One command per feature with subcommands; `hench stats <feature>`; GET lists/items, POST actions, DELETE resources** | The designs' mixed shapes (`hench batch` + `hench batches`, overview lines). |
| X13 | Per-minute counters | **One convention, 8 h, minute periods only** | Ten-minute rollups for longer windows (more writes per event). |
| X14 | TOML reserved names | **`capsules`, `throttles`, `queue_limits`, `periodic`** (release note) | Nest feature tables under `[features]` (no reserved words, but unlike the rest of the file). |
| X15 | Script conventions | **Feature-prefixed names, `.lua` files, shared prelude and builder, common `lua-replicate-commands` check** | Inline Lua strings per module (as `fetch.rs` today). |
| X16 | Cargo features | **Only `periodic`** (heavy dependencies); unique, batches and limits always compiled | A feature per pro feature (more `cfg`s in shared files and docs.rs combinations). |
| X17 | Phase 3 items | **After both lanes merge: queue limits (`FETCH_GATE`), staged batch commits, iterable cancellation checkpoint, cancellation cache** | Queue limits in phase 1 (conflicts with lane A in `fetch.rs`). |
| X18 | Raw pushes and throttles | **`hench enqueue --throttle`, `POST /jobs` `throttle`/`throttle_key`, `hench periodic add --throttle`** | Throttles only for typed pushes. |

### 9.2 Decisions of the four designs, consolidated

All are recommended as written in their documents; the ones with the largest consequences come first in
each list.

| Feature | Decisions (recommendation) |
|---|---|
| Unique (unique.md §4) | U1 locks taken in the push script and released/held in the outcome scripts (vs a middleware pair); U2 policies `Success`/`Start`/`Expired`, no while-executing lock (rate limiting's concurrency limiter instead); U3 retries keep and extend `Success` locks (Ent lets them expire); U4 heartbeat renews running `Success` locks; U5 API delete/kill release locks, `clear` does not; U6 `Drop` (default), `Error`, `Replace` only for scheduled holders; U7 HASH + index (436 B, listable) vs lean `SET` (212 B); U8 SHA-256 base64url, versioned canonical input; U9 testing clients enforce locks by default; U10 duplicates return `Ok(None)`; U11 digest before client middleware; U12 one nested `unique` payload object; U13 crash and shutdown requeues leave locks alone in v1; U14 morgue revival takes no lock in v1; U15 `unique_for` mandatory, no global default; U16 per-minute counters written by the scripts. |
| Periodic (periodic.md §4.1) | P1 leaderless per-tick CAS (vs leader election); P2 default catch-up `Latest`; P3 Redis `TIME` decides due-ness; P4 `croner` 4.0.2 with `jiff` behind a private wrapper; P5 system tz database with bundled fallback; P6 cargo feature `periodic` on by default; P7 per-group atomic registration, last registration wins, pruning, re-registration after data loss; P8 6-field cron and `every` ≥ 1 s; P9 keep firing while quiet; P10 dynamic schedules through API and CLI, dashboard without create/edit; P11 overlap prevention through `unique_for`; P12 `RECORD_RUN` hook; P13 history of 10; P14 POSIX OR for day fields, `+` for AND; P15 dialect limits (no year, `0/10`, `@reboot`); P16 required, unique names; P17 dashboard deletes code schedules only of orphaned groups; P18 retry defaults kept; P19 no `#[hench::job(cron = ..)]`; P20 no start/end dates, run limits or RRULE in v1; confirm the reserved `periodic` payload field. |
| Batches (batches.md §4) | B1 API delete/kill count the member dead; B2 discard and `retry: false` are deaths; B3 halting server middleware counts as success; B4 cancelled: `complete` fires, `success` never; B5 single-script commits capped at 10 000 jobs, loaders beyond, staged commits in phase 3; B6 callback queue = the callback's own, `callback_queue`, then its setter; B7 client middleware runs for callbacks at definition time; B8 anyone may reopen (atomic refusal after success/cancel); B9 workflow layer in v1; B10 per-minute batch counters (`b\|…`), last in the plan; B11 72 B per pending member accepted; B12 one `HGET` per member execution for cancellation; B13 30 days after activity, 24 h linger; B14 reserved `hench::WorkflowNext`; B15 no tags in v1; B16 heartbeat feature flag and dashboard warning; B17 `Batch::with_bid` (= X10); B18 `Outcome::Skipped` (= X7). |
| Rate limiting (ratelimit.md §4) | R1 phasing: limiters and reschedules, throttles, then queue limits (deferrable); R2 throttles enforced in the processor right after the fetch, parking refused jobs at a wait-line slot; R3 reschedule budget 20 for limiters (then retries), unlimited for throttles; R4 wait line on; R5 per-call bookkeeping on (`stats(false)` per limiter); R6 fail closed on Redis errors, `fail_open` opt-in; R7 testing clients unlimited by default; R8 queue limits stored in Redis, code wins at start; R9 engine permits released after the outcome write, best effort; R10 over limit = processed not failed, throttled counts only `t`; R11 reschedule target `schedule` through the push-back path; R12 Redis `TIME`; R13 atomic composite limits; R14 defaults (leases 30/60 s, `max_wait` 5 s/0, `max_delay` 24 h, stats 24 h, budget 20); R15 UTC-aligned buckets; R16 per-key tiers through separate throttle names; R17 restricted names, free keys hashed beyond 128 bytes; R18 jittered polling plus local wake-ups for concurrency waits; R19 two nested payload objects; R20 unknown throttle names run unthrottled with a warning; R21 gate in the processor before server middleware. |

---

## 10. Risks

| # | Risk | Mitigation |
|---|---|---|
| K1 | Phase 0 is large (about twenty shared files) and its hook signatures may prove wrong once the lanes implement them. | Signatures reviewed by both lane owners before phase 1; a change-request path to the phase-0 branch; the hooks are thin (each lane's logic stays in its module). |
| K2 | Hot-path regressions in the processor, fetcher and heartbeat. | Feature-free jobs keep today's scripts byte for byte and pay four `Option` checks; X22 performance gate; soak X21; engine-lane-style review of `processor.rs` in phase 0 and 2. |
| K3 | Redis CPU and round trips grow with each feature on a job (up to 7 round trips instead of 2). | §3.5's budget in the README; `stats(false)` for limiters; a dedicated Redis for limiters (any `RedisPool`); features only where needed. |
| K4 | Memory: 436 B per lock, 72 B per pending member, about 1 KB per schedule, limiter state and stats per identity. | Sizing table in the README; bounded history and summaries; every per-entity key expires (X23); the existing `noeviction` warning. |
| K5 | Contract drift between lanes (frozen Lua interface, pipeline order, payload shapes). | Frozen in phase 0 with tests; lane B tests `BATCH_COMMIT` against the real `lib.lua`; X6–X15 in phase 2. |
| K6 | Mixed-version rollouts: older workers ignore locks, batch membership, `OverLimit` and newer schedule specs. | `features` map, dashboard warning, README rollout rule, lenient reads (X24). |
| K7 | Semantics confusion: uniqueness vs concurrency, throttles vs limiters vs queue limits, batch success vs at-least-once members, periodic "at most once per tick" vs at-least-once execution. | README "Using the features together" with a decision table and recipes; dashboard labels. |
| K8 | Clock assumptions: Redis `TIME` everywhere (failover to a host with another clock shifts leases, TTLs and windows); client clocks only in per-minute keys, `enqueued_at`, run-at times. | NTP on Redis hosts (documented); monotonic guards (periodic `last_tick`); never-shorten TTL rules. |
| K9 | Store compatibility: `TIME` before writes (6.2 needs `lua-replicate-commands yes`), Dragonfly and Valkey untested. | Start-up check; declared keys everywhere; CI against Valkey when available. |
| K10 | `croner` (young, API churn, Apia gap bug worked around). | Private wrapper, pinned version, DST/property tests, MSRV CI. |
| K11 | Scheduler throughput for parked jobs and periodic bursts at popular instants. | Wait lines return parked jobs at the limit's rate; queue limits (phase 3) for very high rates; periodic jitter and O(log n) scripts. |
| K12 | Script blocking: a 10 000-job commit runs about 34 ms (plus one `HGET` per unique member). | The commit cap; loader jobs; staged commits in phase 3. |
| K13 | Lua pitfalls (keywords as fields, numbers as doubles, script size). | Conventions (§2.2), the `SCRIPT LOAD` test, scenario ports of every lab. |

---

## Appendix A. Verification of the cross-feature Lua interface

Files in `/home/user/scratch/design-lab/pro-features/` (original lab code): `ulib.lua` (the unique lab
library plus `hu_check` and `hu_push_one`, as frozen in unique.md §2.8.2), `commit_unique.lua` (a minimal
`BATCH_COMMIT` exercising only the unique section of batches.md §2.5.3), `fire_unique.lua` (a minimal
`PERIODIC_FIRE` with the compare-and-set and `hu_push_one`), `check.py` (scenarios, stdlib only),
`resp.py` (the rate-limit lab's RESP client), `start.sh`. Run on redis-server 7.0.15 over a unix socket;
the server was stopped and its files removed afterwards.

Result: 31 checks, all passing.

- Commit: an intra-commit duplicate is left out and reported with its holder; the first member's lock has
  a TTL of about `unique_for`; `d` is counted; a resend with the same commit id is `already` and writes
  nothing; `on_conflict = error` refuses with `DUPLICATE <class> <digest> <holder>` before any write; a
  member whose explicit jid already holds the lock is a duplicate of itself and is not registered; a lock
  key of the wrong type refuses with `WRONGTYPE` before any write; under `maxmemory 1` the commit is refused
  with `OOM` and writes nothing, then applies once memory frees; 10 of 12 stale index entries are trimmed.
- FIRE: the first tick enqueues and takes the lock; a resent FIRE is a CAS no-op (no second job or lock);
  the next tick while the lock is held is `duplicate` (no push, `next` advanced, `skipped` +1, the lock
  untouched); after the lock is released the next tick enqueues and locks again; a wrong-type lock writes
  nothing and leaves `next`; a `duplicate` tick under `OOM` is refused with nothing written.
- Found: `until` is a Lua keyword (`u.until` does not compile); recorded in unique.md §2.8.2 and §2.2 here.
