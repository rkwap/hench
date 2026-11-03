# Hench — design

Hench is a Redis-backed background job system for Rust (tokio), modelled closely on
Sidekiq 8.1. Rust only: no Ruby interop is required. We keep Sidekiq's proven
architecture, data model, semantics and web dashboard *structure*, and improve where
Rust allows (typed jobs, reliable fetch by default, cancel-safe shutdown).

This document is the contract every contributor (human or agent) follows. If you
change a decision here, update this file in the same commit.

## 0. Ground rules

- **Reference sources** (read-only, outside the repo):
  - Sidekiq 8.1.7: `/home/user/ref/sidekiq` (`lib/`, `test/`, `web/`)
  - rusty-sidekiq (MIT, Rust): `/home/user/ref/sidekiq-rs`
  - Faktory (Go): `/home/user/ref/faktory`
- **Licensing.** Sidekiq is LGPL-3.0. Read it to understand *behaviour*, data formats,
  defaults and edge cases, then write original Rust. Never paste Sidekiq source,
  comments, Lua, ERB templates, CSS, JS, images or locale strings. Test *scenarios* may be
  ported (re-expressed in Rust); test code may not be copied. rusty-sidekiq is MIT, so
  small adaptations are allowed with a credit comment.
- **Name.** Never call the product "Sidekiq". "Inspired by Sidekiq" is fine in docs.
- **Quality bar.** No `unwrap()`/`expect()` in library code paths that can fail at
  runtime (allowed in tests and for truly impossible states with a comment). All public
  items documented. `cargo fmt`, `cargo clippy --workspace --all-targets -- -D warnings`
  and `cargo test --workspace` must pass before every commit. MSRV 1.88, edition 2024.
- **Logging** uses `tracing`. Never `println!` in library code.

## 1. Workspace

| Crate | Purpose |
|---|---|
| `crates/hench` | Core: client, job trait + registry, worker runtime (fetch, processor, retry, scheduler, heartbeat, launcher), API (stats/queues/sets/processes), middleware, testing modes, metrics. |
| `crates/hench-macros` | `#[hench::job]` attribute macro (re-exported as `hench::job`). |
| `crates/hench-web` | Dashboard as an axum `Router` (askama templates, embedded assets). |
| `crates/hench-server` | `hench` binary: standalone dashboard + JSON API + CLI utilities. |

Every crate ships the MIT `LICENSE` and a README; docs.rs documents `hench` with every feature
(`package.metadata.docs.rs`, `doc_cfg` marks what each item needs). Public types that are likely
to grow (`JobOptions`, `JobPayload`, `LifecycleEvent`, `RetryIn`, `FetchStrategy`, `QueueMode`,
`Exhausted`, `cli::WorkerArgs`, the API's results such as `api::Page` (built outside hench with
`Page::new`), `RawEntry`, `CapsuleInfo` and `StatCounter`, and the metrics' `QueryResult`,
`JobMetrics`, `DeployMark`, `SortBy` and `Period`, beside `Config` and the other structs already
so) are `#[non_exhaustive]`, so a new field, flag or variant is not a breaking change;
`metrics::Granularity` names the two resolutions the metrics are stored at and stays exhaustive.
`random_hex` is `#[doc(hidden)]` (an implementation detail of the identifiers). The
`hench-server` library exists for its binary and tests: its API is not covered by semantic
versioning (`docs/RELEASING.md`). `redis_rs` and `anyhow` (whose error `JobError` wraps) are
re-exported. `chrono` (dates in the API and metrics) and `futures` (the API's `Stream`s) are
not: re-exporting a whole crate makes its names visible to rustc's shortened diagnostic paths,
and names such as `futures`'s `Send` (a future) then lengthen users' error messages
(`std::marker::Send`); both are long-stable versions (0.4, 0.3). The integration tests that need
a `redis-server` declare `required-features = ["test-util"]`: in the workspace the self
dev-dependency enables it; from the published crate, `cargo test --features test-util`. The
checks of the repository itself (`tests/spec_parity.rs`, which reads the macro crate's sources,
and `tests/readme.rs`) are left out of the package, and `ci/check-package.sh` builds every
target of the packaged `hench`, `hench-web` and `hench-server` from their `.crate` files and
runs the doc tests of the README and of the guide from the packaged `hench-server` (CI's
`package` job). `crates/hench-server/README.md` is a symbolic link to the repository's README,
and `crates/hench-server/guide` one to `docs/guide`, which the package carries as files, so
`ReadmeDoctests` and `guide_doctests` include them in the repository and in the package alike.
The README is an overview (crates.io shows it, so its links are absolute and pinned to the
release's tag); the guide (`docs/guide`, a page per topic) holds the details, and
`tests/readme.rs` checks that every page with an example is a doc test and that the documents'
links resolve. `benchmarks/` (a crate of the workspace that is never published) ports Sidekiq's
benchmark scripts and records Hench and Sidekiq running them on one machine. `CHANGELOG.md` (Keep a Changelog) lists each release's changes, the known
limitations of the release and what the next one plans; `docs/RELEASING.md` the release
steps.

Feature flags on `hench`: `macros` (default), `tls` (rustls `rediss://`, with rustls's `ring`
crypto provider and TLS 1.2 enabled; redis-rs builds its TLS configuration on rustls's
process-wide default provider, which rustls picks from its cargo features only when they name
exactly one, and panics otherwise: with aws-lc-rs also enabled by another dependency, there are
two. So creating a `rediss://` client installs `ring` as the process default unless one is
installed: an application that wants another provider installs it before), `test-util` (exports
`hench::test_util::TestRedis`), `cli` (`hench::cli`: worker command line with clap and a TOML
configuration file, §9e; the `worker` example needs it), `periodic` (default: periodic jobs'
timetables, with `croner` 4.0.2 and `jiff` and its bundled time-zone database; `hench-web` and
`hench-server` enable it). Unique jobs, batches and rate limiting are always compiled; their
only dependency is `sha2` (0.10, every digest of the features: §2 below), shared with periodic
jobs (`docs/design/pro-features.md` §2.14).

## 2. Redis data model (mirror of Sidekiq 8)

All keys are un-namespaced, exactly like Sidekiq (use a separate Redis DB per app).
Key helpers live in `hench::keys`; never hand-format keys elsewhere.

| Key | Type | Contents |
|---|---|---|
| `queues` | SET | names of known queues |
| `queue:<name>` | LIST | job JSON. Enqueue = `LPUSH`; fetch pops from the right (FIFO). Requeue on shutdown = `RPUSH` (goes first). |
| `schedule` | ZSET | score = run-at epoch **seconds** (f64); member = job JSON *without* `at`/`enqueued_at` |
| `retry` | ZSET | score = retry-at epoch seconds; member = job JSON with error fields. In `schedule` and `retry`, a due member a scheduler is enqueueing is *leased*: its score is moved a minute ahead meanwhile (§5) |
| `dead` | ZSET | score = death time epoch seconds; trimmed by age (`dead_timeout`, 180 days) and size (`dead_max_jobs`, 10 000) |
| `processes` | SET | live process identities |
| `<identity>` | HASH | `info` (JSON), `concurrency`, `busy`, `beat` (f64 secs), `rtt_us`, `quiet` ("true"/"false"), `rss` (KB). `EXPIRE 60`. A key Redis lost is re-created by the liveness thread with a `liveness` field (epoch secs) alone until the next beat (§5). |
| `<identity>:work` | HASH | field = worker slot id (tid), value = JSON `{"queue","payload" (job JSON string),"run_at" (epoch secs f64)}`. `EXPIRE 60`. |
| `<identity>-signals` | LIST | remote signals pushed (`LPUSH`, `EXPIRE 60`) by `api::Process::quiet`/`stop`/`dump`: `TSTP` (quiet), `TERM` (stop), `TTIN` (log the running jobs). Each heartbeat reads the oldest (`LINDEX -1`) and removes it with `LREM` once handled. |
| `<identity>:inprogress` | LIST | **reliable fetch**: jobs currently held by this process |
| `inprogress` | SET | identities that own an in-progress list (for orphan recovery) |
| `stat:processed`, `stat:failed` | STRING | lifetime counters |
| `stat:processed:YYYY-MM-DD`, `stat:failed:YYYY-MM-DD` | STRING | daily counters (UTC), `EXPIRE` 5 years |
| `flush:<id>` | STRING | number of the last batch of counts applied by one flusher (a process's stats, or its metrics tracker; `id` random), `EXPIRE` 1 day, refreshed by every batch (§5 "Counter flushes") |
| `process_cleanup` | STRING | 60 s NX lock for pruning dead identities from `processes` |
| `inprogress_recovery` | STRING | NX lock for orphan recovery |
| `recovered-<jid>` | STRING | how many times orphan recovery pushed the job back from the in-progress list of a dead process; `PEXPIRE` 72 h (`fetch::RECOVERY_WINDOW`) after the last one; deleted by an iterable job's first save of progress in an execution (poison pills, §5) |
| `recovered-<jid>:claim` | HASH | `of` (the dead process whose list holds a poison pill) and `by` (the process settling it): orphan checks that overlap never both settle it; `PEXPIRE` 10 minutes (`fetch::RECOVERY_CLAIM_TTL`) |
| `recovery-suspects` | ZSET | job JSON recovered from dead processes that was in flight at an earlier crash too (suspects of a crash loop), waiting to go back to its queue alone, one per orphan check; score: the most recovered first (10¹³ per recovery short of 100), then the time parked (ms) and the place in the dead list |
| `recovery-suspects:released` | HASH | the suspect last released (`job` its JSON, `queue`, `at` ms): the next one waits until it is no longer waiting or running; `PEXPIRE` ten orphan-check intervals |
| `paused` | SET | names of paused queues (`api::Queue::pause`/`unpause`): fetchers skip them |
| metrics keys | HASH/BITFIELD | see §9 |
| `it-<jid>` | HASH | iterable job state: `ex` (executions), `c` (cursor JSON), `rt` (run time, f64 secs), `cancelled` (epoch secs), `done` (`completed`/`aborted`/`cancelled`, written once the iteration is over, before its finishing hooks); `EXPIRE` 30 days, refreshed on every write (§9c) |
| `unique:<digest>` | HASH | unique-job lock: `jid`, `class`, `queue`, `until`, `for`, `at` (score of a holder waiting in `schedule`). `PEXPIRE` always (`for` + the delay of scheduled, retrying or parked jobs; renewed while running; never shortened) (§9g) |
| `unique-locks` | ZSET | digest → lock expiry (epoch secs); no expiry, trimmed by score |
| `u\|YYYYMMDD\|H:MM` | HASH | per-minute unique counters, `<class>\|d` (duplicates), `\|r` (replaced), `\|z` (lapses); `EXPIRE` 8 h |
| `periodic` | ZSET | due index of active schedules: name → next tick or lease end (epoch secs); configuration, no expiry (§9h) |
| `periodic-names` | ZSET | every schedule name (score 0); no expiry |
| `periodic:<name>` | HASH | a schedule's definition and state; no expiry (removed with the schedule) |
| `periodic-history:<name>` | LIST | the latest enqueues of a schedule, capped; removed with the schedule |
| `periodic-group:<group>` | SET | names of the code schedules a group registered; no expiry |
| `periodic-crontabs` | HASH | group → registered crontab (digest, count, when, by); no expiry |
| `batch:<bid>` and `:pending`, `:failed`, `:dead`, `:kids`, `:commits` | HASH, SET, HASH, SET, SET, HASH (commit id → the duplicates it left out) | a batch; every key shares the hash's expiry: `batch_ttl` (30 d) after the last activity, `batch_linger` (24 h) once finished (§9i) |
| `batches` | ZSET | bid → batch expiry; no expiry, trimmed by score |
| `batch-events` | ZSET | the outbox of undispatched batch events; entries removed by dispatch |
| `batch-maintenance` | STRING | NX lock of batch maintenance, `PX` 0.8 × its interval |
| `b\|YYYYMMDD\|H:MM` | HASH | per-minute batch counters (`created`, `complete`, `success`, `death`, `cancelled`); `EXPIRE` 8 h |
| `limit:c:<id>`, `limit:w:<id>`, `limit:b:<id>`, `limit:g:<id>` | ZSET, LIST, HASH, STRING | limiter state (concurrency leases, window grants, bucket, leaky/points level); expiry derived from the limit (longest lease; period + 1 s; bucket end + 1 s; drain end + 1 s); a concurrency set is deleted when it empties (§9j) |
| `limit-stats:<k>:<id>` | HASH | a limit identity's counters and signature; `stats_ttl` (24 h) after the last use |
| `limits` | ZSET | identity → stats expiry; no expiry, trimmed by score |
| `l\|YYYYMMDD\|H:MM` | HASH | per-minute limiter counters, `<name>\|g` (granted), `\|r` (refused); `EXPIRE` 8 h |
| `queue-limits` | HASH | reserved for queue limits (queue → limit specs, configuration; phase 3 of rate limiting, planned for 0.2): 0.1 neither reads nor writes it, and refuses a worker file's `[queue_limits]`; no expiry |

Identity = `"{hostname}:{pid}:{nonce}"` where nonce is 12 hex chars (6 random bytes).

**The keys of the pro features** (`docs/design/pro-features.md` §2.1) follow one TTL policy:
every per-entity key has an expiry derived from what it means, set by the script that may
create it and never shortened; the global indexes (`unique-locks`, `batches`, `limits`,
`batch-events`) have no expiry but are scored by the expiry of what they index and trimmed by
score (bounded trims in hot scripts, full trims by readers and maintenance); configuration
(periodic schedules, `queue-limits`) has no expiry; the per-minute counters
(`<letter>|YYYYMMDD|H:MM`, the UTC minute of the writing client's clock, `HINCRBY` under
`pcall` as a script's last writes) expire after 8 h (`keys::FEATURE_MINUTE_TTL` =
`METRICS_MINUTE_TTL`), with no ten-minute rollup and one reader (`api::events`, minute
periods up to 8 h). Every kind has its own prefix, so no free-text name (schedule, limiter
key, queue) can produce another kind's key; inherited from the un-namespaced identities, a host
literally named `periodic` would produce heartbeat keys `periodic:<pid>:<nonce>`, which a
schedule named `<pid>:<nonce>` collides with (as a host named `queue` already can with a
queue): documented, not designed around. `TestRedis::keys_without_ttl` lists the keys a test
leaves without an expiry.

### Job payload (JSON object)

`hench::JobPayload` (serde struct + `#[serde(flatten)] extra: Map<String, Value>` so
middleware-added custom keys survive):

| Field | Type | Notes |
|---|---|---|
| `class` | string | the job's registered name (`Job::NAME`) |
| `args` | array | positional args (JSON array) |
| `jid` | string | 24 lowercase hex chars (12 random bytes from `getrandom`) |
| `queue` | string | non-empty |
| `retry` | bool \| int | `true` = default max (25); `false` = no retries; `n` = max n retries |
| `backtrace` | bool \| int | store error backtrace (all lines / first n lines) |
| `dead` | bool | `false` = don't put in morgue when exhausted |
| `tags` | [string] | displayed in dashboard |
| `retry_for` | int secs | time-based retry limit instead of count (must be < 1e9) |
| `retry_queue` | string | queue to use for retries |
| `created_at` | int ms | set once at first push |
| `enqueued_at` | int ms | set on every push to a queue |
| `at` | f64 secs | only in-flight during push of scheduled jobs; stripped before storing |
| `retry_count` | int | 0 after first failure, +1 per retry |
| `failed_at`, `retried_at`, `discarded_at` | int ms | |
| `error_message` | string | truncated to 10 000 chars |
| `error_class` | string | `std::any::type_name` of the error, e.g. `std::io::error::Error`; or `panic`, `hench::UnknownJob`, `hench::InvalidArgs`, `hench::MissingData` |
| `error_backtrace` | string | base64(zlib(JSON array of lines)) |
| `bid` | string | the batch a member belongs to (24 lowercase hex): set by the collecting client of a batch definition; a job becomes a member only through a commit; another value is "not a member" |
| `batch_callback` | object | `{"bid", "event"}` (`complete`, `success`, `death`): the batch and event a callback job runs for (§9i) |
| `unique` | false \| object | uniqueness: `false`, or before normalization any subset of `{"for", "until", "on_conflict", "across_queues", "key", "digest"}`; stored jobs carry `{"for", "until", "digest"}` (§9g) |
| `periodic` | object | `{"name", "tick" (ms), "manual"?}`: the schedule and tick that enqueued the job (§9h) |
| `throttle` | object | `{"name", "key"}`: the throttle of the job's starts (§9j) |
| `limited` | object | `{"n", "by", "at" (ms)}`: reschedules by the rate limiter so far, the label of the limit that refused last, when; never reset (§9j) |

Timestamps in payloads are integer **milliseconds**; sorted-set scores are f64
**seconds**. Same as Sidekiq 8. Reading is lenient: a float timestamp below 1e11 is taken as
seconds (older Sidekiq format) and converted; `retry_for` accepts floats (truncated) like
Sidekiq's `to_i`; `"tags": null` reads as no tags.

- When **pushing**, an empty `jid` means "generate one" and an empty `queue` means "use the
  job's default queue" (Rust has no `nil`). An *explicitly* empty queue in options
  (`J::set().queue("")`, job options, `default_job_options`) is still rejected, like Sidekiq.
- `extra` (`#[serde(flatten)]`) must not repeat a named field; `normalize` drops such keys
  (`JobPayload::RESERVED_KEYS`, and `JobPayload::FEATURE_KEYS` for the five fields of the
  features) with a warning. Release note: an application that used a custom key named
  `unique`, `periodic`, `throttle`, `limited` or `batch_callback` sees it reinterpreted.
- The fields of the features are read **leniently**: a malformed value is `None` with a `warn`
  (no feature field can make a job unparseable and send it to the morgue); an unknown `until`
  written by a newer version reads as `success`. Every feature derives its per-execution state
  from the job **as fetched**, never as server middleware changed it (`pro::Fetched`, §5).
- `serde_json` is built with `float_roundtrip`, so floats in job arguments parse back to the
  exact same value (the default parser is off by one ULP for ~30% of `f64` bit patterns).

## 3. Defining jobs

```rust
use hench::prelude::*;

#[hench::job(queue = "mailers", retry = 5)]
async fn send_email(ctx: &JobContext, user_id: i64, template: String) -> Result<(), MyError> {
    let db = ctx.data::<Db>()?;           // shared state registered on the builder
    /* ... */
    Ok(())
}

// enqueue (typed, generated inherent fns delegate to `JobExt`):
send_email::perform_async(&client, (42, "welcome".into())).await?;
send_email::perform_in(&client, Duration::from_secs(300), (42, "x".into())).await?;
send_email::set().queue("low").retry(false).perform_async(&client, args).await?;
```

Manual equivalent (the macro expands to roughly this):

```rust
pub trait Job: Send + Sync + 'static {
    const NAME: &'static str;                                   // stored in payload `class`
    type Args: Serialize + DeserializeOwned + Send + 'static;   // must serialize to a JSON array; `()` ⇔ `[]`
    fn options() -> JobOptions { JobOptions::default() }        // sidekiq_options
    fn perform(&self, ctx: &JobContext, args: Self::Args) -> impl Future<Output = Result<(), JobError>> + Send;
    fn retry_in(&self, count: u32, err: &JobError, job: &JobPayload) -> RetryIn { RetryIn::Default } // sidekiq_retry_in
    fn retries_exhausted(&self, job: &JobPayload, err: &JobError) -> impl Future<Output = Exhausted> + Send; // default: Exhausted::Default
}
```

- `RetryIn::{Default, After(Duration), Discard, Kill}`; `Exhausted::{Default, Discard}`.
- The macro accepts `name`, `queue`, `retry` (bool/int), `backtrace` (bool/int), `dead`,
  `tags = [..]`, `retry_for` (secs), `retry_queue`, `log_level` (`trace`/`debug`/`info`/`warn`/
  `error`, §9f). The fn may take an optional first param `ctx: &JobContext`. On an
  `impl Job for T` or `impl IterableJob for T` block (non-generic, `T: Default`) the macro keeps
  the block and registers `T::default()` through `inventory`; only `crate` is accepted there. Args tuple: 0 params → `()`, 1 → `(A,)`, n → `(A, B, ..)`.
  The generated item is a unit struct with the fn's name and visibility
  (`#[allow(non_camel_case_types)]`), implementing `Job`, plus inherent `perform_async`,
  `perform_in`, `perform_at`, `perform_bulk`, `perform_inline`, `set`.
- Macro details: the job's `NAME` defaults to the function name (raw identifiers unescaped,
  not module-qualified, so moving the function between modules does not orphan queued jobs;
  duplicates are caught at build time). Option values are literals checked at compile time
  (non-empty strings, `retry`/`backtrace` `true`/`false`/`u32`, `retry_for < 1e9`);
  `crate = "path"` supports a renamed dependency. The context parameter is detected by type
  (`&…JobContext`, any name, first position only; not through a type alias or a renamed
  import, which the error about a borrowed first parameter named like a context says; a
  lifetime on it, such as `&'static`, is refused); arguments must be owned, concrete types:
  references, `impl Trait`, `_` and lifetimes other than `'static` are refused with the macro's
  own messages, and so is `Cow` without its lifetime (other elided lifetimes are invisible in
  the syntax and left to the compiler). The return type must be `()` or `Result<(), E>` with
  `JobError: From<E>`, checked through `hench::__private::JobReturn` so errors point at the
  return type (`impl Trait` in it is refused by the macro); its `Result` impl is
  `#[diagnostic::do_not_recommend]`, so an error type that does not convert (`String`,
  `Box<dyn Error>`) gets `JobReturn`'s own message and note (which conversions to use) instead
  of anyhow's nested bounds. `#[cfg]` attributes are
  copied to every generated item, doc comments, `#[deprecated]` (and `#[allow]`) to the
  struct, everything else (e.g. `#[tracing::instrument]`) to the body, which becomes a nested
  `async fn` with the job's name inside `perform` (so spans are named after the job).
  Lints: the generated code allows only `non_camel_case_types` (the struct's name) and
  `clippy::unused_async` on the body (the macro needs an `async fn`, even without `.await`),
  so that the body keeps every other lint of the crate (deprecation warnings included) and
  crates forbidding `dead_code` or `deprecated` can define jobs (a forbidden lint cannot be
  allowed: E0453). The trait impl, the helpers and the registration sit in one `const _`
  block, which also references the helpers (so they count as used: no `allow(dead_code)`);
  for a `#[deprecated]` job, whose name warns even in its own generated code, they name it
  through an alias defined there under the expansion's only `allow(deprecated)`. A crate
  forbidding `non_camel_case_types` cannot use the attribute on functions (impl blocks are
  fine); under `#![forbid(warnings)]` that allow, and `inventory`'s
  `non_upper_case_globals`, are `forbidden_lint_groups` warnings.
  `#[cfg]` on parameters is rejected (the argument list is part of the stored format);
  `#[expect(..)]` becomes `#[allow(..)]` on both the struct and the body (the lint may fire on
  either). Every binding in generated code is `__hench_`-prefixed: a job is a unit struct, so a
  job named like a binding (`args`, `client`, ...) would turn it into a pattern. On an error the
  macro emits the `compile_error!` plus a fallback (the expansion without the bad options but
  with a valid `crate` path, or the item as written) to reduce follow-up errors. Because the job is a unit
  struct, its name is a constant pattern: `let send_email = …` does not compile where the job
  is in scope.
- **Registration**: macro jobs auto-register through `inventory`
  (`hench::__private::JobRegistration`). Manual jobs: `builder.register(instance)` or
  `builder.register_default::<J>()`. Duplicate names are a build-time `Error::Config` that
  names both types (`ErasedJob::dyn_type_name`, `std::any::type_name` by default); a
  `#[hench::job]` registered with `register` too gets its own message (it registers itself:
  drop the call, or `auto_register(false)`). The inventory holds the jobs of the crates linked
  into the binary, and rustc links only the crates a binary refers to: a worker whose jobs live
  in a library it never names gets none (the README says to `use my_jobs as _;`). So the
  `starting` log lists the registered jobs, a process without any warns, and a worker's
  unknown-job error names the jobs the process knows (ten at most) and that cause.
- Job instances are shared (`Arc`) across concurrent executions; per-run state lives in
  locals. Unknown `class` at execution time → `JobError` class `hench::UnknownJob` → normal
  retry path (like Ruby's NameError).
- `hench::Error` variants wrapping another error (`Redis`, `Json`, `Io`) put its message in
  their own and do not expose it as `source()`: chain printers (anyhow's `{:#}`) repeated it.
  `Config` and `InvalidJob` messages are displayed without trailing whitespace (TOML's end
  with a line break).
- `JobError` wraps `anyhow::Error` and records `class` (`std::any::type_name` of the
  source error, or `panic`). `impl<E: Into<anyhow::Error> + 'static> From<E> for JobError`
  so `?` works for std errors, custom errors, `hench::Error` and anyhow. Like
  `anyhow::Error`, `JobError` does **not** implement `std::error::Error` (that is what keeps
  the blanket `From` coherent); use `as_anyhow()`/`into_anyhow()`. `message()` is the error
  followed by its causes joined with `": "` (a cause already ending the text is not
  repeated); `backtrace_lines()` is one `caused by: …` line per cause, then where the error
  came from. Workers and testing clients poll a job whose `backtrace` option is on with
  origin capture on (`error::capturing_origins`, a thread-local flag set during each poll):
  every `JobError` created meanwhile force-captures the stack (whatever `RUST_BACKTRACE`
  says), and a panic hook — installed when the first such job runs, calling the hook
  installed before it — records a panic's location and stack, which `catch_panic` attaches
  (`panicked at file:line:col`, then the frames from the panic site). The record is
  thread-local, read where the unwinding stops; code that catches a panic, awaits, then
  resumes it (an iterable job saving its cursor) carries the record across the `.await`
  (`error::CaughtPanic`: taken right after the catch, put back right before the panic resumes),
  since meanwhile the task may move to another thread and other jobs panic on this one. Frames
  start at the job's own code (the capture's and the panic machinery's frames are dropped) and
  end where hench called the code that failed, the job's `perform` or a server middleware:
  hench polls each through `error::hench_execution_boundary`, a function that is never inlined
  (and does not end in a tail call), whose frame marks the cut in optimized builds too; the
  standard library's frames just above it go as well (known by their names, or by their
  sources' `library/<crate>/src/` paths whatever the toolchain: rustup's `/rustc/<commit>/`,
  Homebrew's or a distribution's; inlined frames have short names). Hench's middleware chain,
  its panic catching, the processor and tokio below the cut made up about 95% of a stored
  backtrace.
  Other errors carry anyhow's backtrace when `RUST_BACKTRACE`/`RUST_LIB_BACKTRACE` is set.
  Constructors: `msg` (class `hench::JobError`), `new(class, msg)`, `panic(payload)`,
  `unknown_job`, `invalid_args`, `from_boxed`, `with_anyhow`; class constants
  `JobError::PANIC`, `UNKNOWN_JOB`, `INVALID_ARGS`, `MISSING_DATA`, `SHUTDOWN`, `KILLED` (death
  handlers of jobs killed through the API). Panics in jobs
  are caught (`catch_unwind`) and become `JobError` class `panic` — they never kill a worker.
  This needs Rust's default `panic = "unwind"`: in a binary built with `panic = "abort"` a
  panicking job aborts the process, and with reliable fetch the processes that recover it after
  it, with the jobs running beside it each time, until it is recovered past `max_recoveries` and
  moved to the morgue as a poison pill (`hench::CrashLoop`, §5); `start` logs a warning when
  `cfg!(panic = "abort")`.
- `ErasedJob` (object-safe, blanket-implemented for every `Job`, stored as
  `Arc<dyn ErasedJob>` in the `Registry`): `name() -> &str`, `job_options()`,
  `perform_json(ctx, Vec<Value>) -> BoxFuture`, `dyn_retry_in(..)`,
  `dyn_retries_exhausted(..)`. The prefixed names avoid ambiguity with `Job`'s methods when
  both traits are in scope; it can also be implemented directly for jobs whose name is only
  known at runtime (`register_dyn`).
- Arguments: `args_to_values` maps `()`/`null` to `[]`, rejects values that are not arrays
  (wrap structs in a 1-tuple) and rejects arguments that do not deserialize back into `Args`
  (e.g. NaN, which serde_json would silently write as `null`) — the equivalent of Sidekiq's
  strict-args check. `args_from_values` also accepts `[]` for `()`/unit structs; a failure is
  a `JobError` of class `hench::InvalidArgs`.
- `JobRegistration::of::<J>()` (const fn, `J: Job + Default`) is what the macro submits with
  `hench::__private::inventory::submit!`; `Registry::from_inventory()` collects them.
- `JobContext`: `jid()`, `class()`, `queue()`, `retry_count()` (`None` on the first
  attempt), `payload()`, `tags()`, `bid()`, `client()` (enqueue more jobs), `data::<T>()`
  (type-map state, `Result<&T, JobError>`; class `hench::MissingData` when absent),
  `data_arc::<T>()`, `is_stopping()` / `stopping().await` / `stopping_token()` (shutdown
  signal, for long jobs), `max_iteration_runtime()` and `shutdown_timeout()` (set by workers
  from the config, read by iterable jobs). `JobContext::new(payload, client)` is public (for
  unit-testing jobs) with `with_queue`, `with_data`, `with_stopping`, `with_payload`,
  `with_max_iteration_runtime`, `with_shutdown_timeout`.
- `JobError::interrupted()` (class `JobError::INTERRUPTED` = `hench::Interrupted`,
  `is_interrupted()`): a job returning it asks to run again later. Workers push it back to
  the *end* of its queue and acknowledge it, without a failure, retry or error report (§5);
  iterable jobs return it when they stop early (§9c).
- `JobExt::perform_inline(&self, args)` runs the job in process without Redis: arguments make
  a JSON round trip, the context's client is `Client::fake()`, no middleware runs, panics are
  returned as `JobError`s. It is one execution, as Sidekiq's: an interrupted job
  (`JobError::interrupted`, an iterable job whose run time is up) returns that class, with a
  message saying so and naming the inline testing client, which runs such a job to the end
  (§8). `Client::inline(registry)` is the full in-process pipeline.

### The pro features in job types (`docs/design/pro-features.md` §2.4)

- `Job` gains `fn unique_key(args) -> Option<Value>` (the part of the arguments identifying a
  duplicate; all of them by default), `fn throttle_key(args) -> Option<String>` and
  `fn throttle() -> Option<Throttle>` (a throttle defined with the job type, registered under
  its name when the worker is built); `ErasedJob` gains `dyn_throttle()`. Typed pushes call the
  key methods when the merged options (the client's `default_job_options`, under the job
  type's, under the setter's, as the push merges them; round 3: the client's defaults were left
  out, so a job type made unique by them alone was locked on all its arguments) make the job
  unique or throttled without an explicit key; an explicit `unique_key(..)`/`throttle_key(..)`
  wins.
  `IterableJob` forwards them (`unique_key`, `throttle_key`, `throttle` are `IterableJob`
  methods too).
- `JobOptions` gains `unique: Option<UniqueOption>` and `throttle: Option<ThrottleOption>`
  with the setters `unique`, `unique_for`, `unique_until`, `on_conflict`,
  `unique_across_queues`, `unique_key`, `not_unique`, `throttle`, `throttle_key`,
  `no_throttle` (the same on `JobSetter`, `JobBuilder` and `BulkPush`). Layers merge field by
  field: a higher `Disabled` wins, two `Enabled` merge per field, a higher `Enabled` over a lower
  `Disabled` is the higher one alone.
- `#[hench::job(..)]` parses `unique_for = secs`, `unique_until = "..."`, `on_conflict =
  "..."`, `unique_across_queues = bool`, `unique_by = [params]`, `throttle = "name"`,
  `throttle_by = [params]` and `throttle(concurrency = n, window = "N/period", ..)` (specs
  checked at compile time with the macro's copy of the limit grammar, `tests/spec_parity.rs`)
  and generates the matching `options()`, `unique_key`, `throttle_key` and, for an inline
  throttle, the `throttle()` definition; the unknown-option message lists every option.
- `JobError` gains the classes `hench::BatchCancelled` (`batch_cancelled()`,
  `is_batch_cancelled()`) and `hench::OverLimit` (`over_limit(OverLimit)`, `as_over_limit()`,
  which sees through `hench::Error::OverLimit` and context, `is_over_limit()`). The job name
  `hench::WorkflowNext` is reserved: `Registry::register*` refuses it (`Error::Config`), and
  every worker and testing executor resolves it to hench's own job (`Registry::resolve`: the
  next stage of a `Workflow`, §9i). `Registry::names` does not list it.
- `hench::Error` gains `DuplicateJob { class, digest, holder }`, `InvalidSchedule(String)`,
  `Batch(BatchError)` and `OverLimit(OverLimit)`.

## 4. Client

(The pro features' additions are at the end of this section.)

`hench::Client` (Clone, cheap): wraps `RedisPool` + client middleware chain (+ optional
testing mode).

- `push(JobPayload) -> Result<Option<String>>` — normalize (merge class options and
  `default_job_options` {queue: "default", retry: true}), validate (Sidekiq's
  `JobUtil#validate` rules; `at` must be finite and no later than 9999-12-31T23:59:59Z,
  253402300799 — `client::MAX_AT`, round 3: a unique push at 1e20 left a lock with no expiry,
  and pages showing such times panicked; a `spread_interval` must end before then too), run
  client middleware (it may mutate or halt → `None`), then
  `raw_push`: scheduled (`at` set and in the future) → `ZADD schedule`; otherwise one
  MULTI with `SADD queues` + `LPUSH queue:<q>` (sets `enqueued_at`). `at` ≤ now ⇒ immediate.
- `push_bulk(BulkPush)` — Sidekiq `push_bulk` semantics: `args: Vec<Vec<Value>>`, `at`
  single or per-job, `spread_interval` (min 5 s, exclusive with `at`), `batch_size`
  (default 1000, or 100 when scheduled), explicit `jid` only allowed for a single job;
  returns jids (None where middleware halted).
- Typed: `client.enqueue::<J>(args)`, `client.job::<J>()` builder (`.queue() .retry()
  .backtrace() .dead() .tags() .retry_for() .jid() .at() .delay()` then `.perform_async(args)`),
  `JobExt` (`perform_async/perform_in/perform_at/perform_bulk/set/perform_inline`). Typed bulk
  pushes also take `.spread_interval(d)` and `perform_bulk_at(Vec<(Args, SystemTime)>)` (Sidekiq
  8.0.8's `spread_interval:` and `at:` array), mapped onto `BulkPush` with the job type's
  options (a per-job schedule with `at`/`delay`/`spread_interval` is an `InvalidJob` error).
  `perform_in` with a delay > 1e9 secs is treated as a timestamp like Sidekiq.
- Retries transient `READONLY`/connection errors once (failover), like Sidekiq. A push whose
  reply times out (never retried) or whose connection drops after the request left may have
  been applied: its error does not mean that the job was not pushed (and the retry after a
  dropped connection can push it twice, same jid). `Client`'s docs ("When a push fails"),
  every push method and the README say so, and recommend idempotent jobs with jids of the
  caller's own for pushes that may be repeated.

Implementation notes (spine):

- `raw_push` splits jobs per job (not per batch like Sidekiq): `at` in the future → one
  `ZADD schedule` (member without `at`/`enqueued_at`); others → `SADD queues` + one `LPUSH`
  per queue; all in one `MULTI`. A job whose `at` is not in the future loses its `at` and is
  pushed immediately — this also applies to `push_bulk` (Sidekiq adds past bulk times to the
  schedule) and to spread jobs already due when their batch is pushed.
- `push_bulk`: an explicit `jid` is honored for a single job (Sidekiq silently replaces it);
  setting the template's `at` is an error (use `BulkPush::at*`); `batch_size` 0 is an error.
- Failover retry lives in `RedisPool::with_retry`: `READONLY`/`NOREPLICAS`/`UNBLOCKED` are
  detected structurally (also inside an aborted `MULTI`), replace the shared connection and
  retry once; dropped/refused connections retry once on the auto-reconnecting connection;
  timeouts are never retried (the command may have run).
- Raw `push` applies only `default_job_options` (as Sidekiq does for string classes); typed
  pushes apply `Job::options()` too. `default_job_options(opts)` (builder, `Testing`,
  `Client::with_default_job_options`) merges `opts` over the current defaults, like Sidekiq's
  `default_job_options=`: setting `backtrace` keeps the default queue and retries. `J::set()` (`JobSetter`) and `client.job::<J>()`
  (`JobBuilder`) also accept `custom(k, v)`, `options(&JobOptions)` and `batch_size(n)` (for
  `perform_bulk`). `options(&JobOptions)` means the same on every builder, `BulkPush`
  included: the options it sets win over what was set before (later calls win in turn);
  `BulkPush::default_options` fills only the template's unset fields.
- `Client::with_middleware(|chain| ..)` returns a copy with a modified chain (Sidekiq's local
  `client.middleware`); `Client::with_redis(pool)` replaces `Client.via(pool)` for sharding;
  `Client::with_default_job_options(opts)`.

### The push pipeline of the pro features (`docs/design/pro-features.md` §3.1)

1. `Client::normalize`: options merged, queue, jid, `created_at` (as before), then the features'
   steps once the job has its arguments: `unique::normalize` (validation and digest; the
   client's switch `with_unique_jobs(false)` drops the field), `limit::normalize` (the `throttle`
   field), and for a collecting client `batch::collect::normalize` (`bid`; `replace` refused).
   Bulk pushes run the features' steps per job, after its arguments are set (with the typed keys
   of `JobSetter::perform_bulk`, `BulkPush::keys`). A job without the features' fields is
   pushed and stored exactly as before them (`tests/featureless.rs`).
2. Client middleware; revalidation (unchanged).
3. `raw_push` returns one `PushResult` per job (pushed, already pushed, duplicate with its holder,
   replaced, pushed without its lock). Redis mode: jobs without a digest go through today's
   `MULTI` (byte for byte, pinned by `tests/featureless.rs`); a batch with a digest goes through
   `unique::push::deliver` (`UNIQUE_PUSH`). Testing
   clients first admit jobs through `unique::fake::admit`; a collecting client appends to its
   batch's buffer (`batch::collect::append`).
4. `push`, `push_bulk` and the typed pushes keep their signatures (`None` for a duplicate);
   `push_outcome`/`push_bulk_outcomes` (on `Client`, `JobSetter`, `JobBuilder`) return
   `PushOutcome::{Pushed, Replaced, Duplicate, Stopped}`; a duplicate with `OnConflict::Error`
   is `Err(Error::DuplicateJob)` except in bulk pushes. `Client::with_batch_defaults`/
   `batch_defaults` carry the batch settings (`HenchBuilder::batch_*`).

## 5. Running workers

```rust
let hench = Hench::builder()
    .redis_url("redis://127.0.0.1/")          // or .redis(pool)
    .concurrency(10)
    .queues(["critical", "default", "low"])    // strict priority
    // .weighted_queues([("critical", 3), ("default", 2), ("low", 1)])
    .capsule("single", |c| c.concurrency(1).queues(["serial"]))
    .data(AppState { .. })                     // available via ctx.data::<AppState>()
    .register(SomeManualJob::new())
    .server_middleware(MyMiddleware)
    .error_handler(|err, ctx| { .. })
    .death_handler(|job, err| { .. })
    .on_startup(|| async {}) /* on_quiet, on_shutdown, on_exit, on_heartbeat, on_beat */
    .on_startup_try(|| async { check_db().await })  // an error stops the start
    .logged_job_attributes(["request_id"])     // payload keys on every line of a job
    .backtrace_cleaner(|lines| lines)          // filters stored backtraces
    .build().await?;

let client = hench.client();
hench.run().await?;   // SIGINT/SIGTERM (stop), SIGTSTP (quiet), SIGTTIN (log the running jobs)
// or: let launcher = hench.start().await?; ... launcher.quiet().await; launcher.stop().await?;
// or: hench.run_until(shutdown_future).await?;  // embedded next to axum::serve
```

Config defaults (Sidekiq `DEFAULTS`): concurrency 5, queues `["default"]`, timeout 25 s,
`poll_interval_average` None (→ process_count × `average_scheduled_poll_interval` 5 s),
`max_retries` 25, `dead_max_jobs` 10 000, `dead_timeout` 180 days, heartbeat every 10 s (at
most `Config::MAX_HEARTBEAT_INTERVAL` = 20 s, a third of the heartbeat key's 60 s expiry, so
that a slow beat cannot make the process look dead and remote signals, which expire after
60 s, are always read), fetch = `Reliable`, `max_iteration_runtime` None. Additional knobs (all settable on the builder
so tests can shrink them): `initial_scheduler_wait` 10 s + `initial_scheduler_jitter` up to
5 s, `fetch_timeout` 2 s (BRPOP/BLMOVE block), `orphan_check_interval` 60 s (round 3: was 5 min),
`orphan_grace` 30 s (at least twice the heartbeat interval; see orphan recovery),
`max_recoveries` 3 (poison pills, see orphan recovery), `metrics` (true,
§9), `systemd` (`systemd::Notifications::FromEnv`, §9d).

Spine details:

- `Config` has public fields (`#[non_exhaustive]`, immutable behind `Arc` once built) and
  helpers shared by every lane: `handle_error(err, &ErrorContext)` (runs every error handler,
  a panicking handler is logged), `handle_death(job, err)` (a panicking death handler is
  reported as an error), `fire_event(event, reverse)` (sequential, panics reported),
  `fire_startup()` (the startup checks, then the `startup` callbacks; stops at the first that
  fails or panics, reported and returned), `callbacks(event)`, `total_concurrency()`,
  `capsule(name)`, `queue_names()`.
- Handlers: `ErrorHandler = Arc<dyn Fn(&JobError, &ErrorContext<'_>)>`,
  `DeathHandler = Arc<dyn Fn(&JobPayload, &JobError)>`,
  `LifecycleCallback = Arc<dyn Fn() -> BoxFuture<'static, ()>>`,
  `StartupCheck = Arc<dyn Fn() -> BoxFuture<'static, Result<(), JobError>>>`
  (`on_startup_try`, any error converting into `JobError`),
  `BacktraceCleaner = Arc<dyn Fn(Vec<String>) -> Vec<String>>`. `ErrorContext { context,
  job, jobstr, queue, event }`. A default error handler logs through `tracing` (like Sidekiq's
  default handler); `.error_handler(f)` adds to it, `.clear_error_handlers()` removes it.
- Capsules: `.capsule(name, |c| ..)` creates or updates the capsule with that name (the
  default capsule is `default`); `.queues([...])` accepts Sidekiq's `"name,weight"` strings;
  `.weighted_queues([(name, weight)])`. Mode: all weights 0 → strict, all 1 → random, else
  weighted; `CapsuleConfig::expanded_queues()` repeats each queue `max(weight, 1)` times;
  weights above `CapsuleConfig::MAX_WEIGHT` (1000) are rejected (only ratios matter).
  Middleware chains belong to the process, not to capsules (deviation): Sidekiq 7+ capsules
  carry their own chains, inheriting the global ones, but in Sidekiq 8.1 its processors run
  the global server chain anyway (capsule chains only show up in debug logs, and a capsule's
  client chain only for clients built with `config: capsule`), so `capsule_test`'s
  "customized middleware chains" is not ported.
- `build()` validates before any I/O: capsule names unique and non-empty, concurrency ≥ 1,
  ≥ 1 queue, no empty or duplicate queue names, positive intervals, `dead_max_jobs` ≥ 1, no
  empty default queue; then builds the registry (inventory jobs unless `.auto_register(false)`,
  plus `.register(..)`, `.register_default::<J>()`, `.register_dyn(..)`; duplicates are
  `Error::Config`), then connects (`.redis(pool)`, `.redis_url(url)`, `.redis_config(cfg)`, or
  the environment: `REDIS_PROVIDER` naming the variable, else `REDIS_URL`, else
  `redis://127.0.0.1:6379`).
- `RedisPool`: `conn()` hands out the shared auto-reconnecting `ConnectionManager` (created
  lazily, never waits for Redis: concurrent callers share one connection attempt; with
  `RedisConfig::shared_connections(n)`, default 1, at most 64, each of `n` such connections in
  turn, since one connection's encoding and decoding runs on one core at a time and caps a
  busy process's throughput);
  `blocking_conn(block)` opens a dedicated `ConnectionManager` whose response timeout is
  `block + max(response_timeout, 1 s)`; `reconnect()`; `with_retry(..)`; `ping()`;
  `RedisPool::detached()` backs fake/inline clients (every connection request fails).
  `RedisConfig` defaults: connection and response timeouts 3 s (Sidekiq's), 6 reconnection
  attempts with 100 ms–2 s exponential backoff. Every TCP connection (to a server, a sentinel
  or the primary a sentinel names) has `TCP_NODELAY` on: redis-rs leaves it off, and Nagle's
  algorithm on a shared multiplexed connection holds commands back by up to a round trip. `from_config` (and so `build`) logs the URL
  (redacted) and where it came from (`REDIS_URL`, the variable `REDIS_PROVIDER` names, or the
  default) at info level, like Sidekiq, then connects with attempts of its own: retried while
  Redis is unreachable or not ready (`LOADING`, ...), but failing at once on errors no retry
  fixes (authentication, an unknown database). `RedisConfig`'s `Debug` prints the redacted URL.
  URLs: `redis://`, `rediss://` (feature `tls`), `redis+unix://` and `unix://`
  (`?db=N&pass=..`), and `redis+sentinel://` (below); passwords are redacted in logs.
  TLS settings (Sidekiq's `ssl_params`): `RedisConfig::tls(TlsConfig::new().root_cert(pem)
  .client_identity(cert_pem, key_pem))` for private CAs and mutual TLS; without it the system
  roots are trusted. `TlsConfig`'s `Debug` never prints key material. Explicit opt-outs, for
  servers whose certificate cannot be verified (a self-signed certificate that is also a CA,
  or names another host, as Heroku's Redis has): `danger_accept_invalid_certs(true)` (no
  verification at all, Sidekiq's `verify_mode: VERIFY_NONE`; the same as redis-rs's
  `rediss://host:port/#insecure`, which the feature `tls` now enables, so the `hench` command
  can use it too) and `danger_accept_invalid_hostnames(true)` (the chain is still verified).
  A pool using either logs a warning when it is created.
- **Redis Sentinel** (`redis/sentinel.rs`; Sidekiq's `sentinels:`):
  `redis+sentinel://[[user]:password@]host[:port][,host[:port]...]/service[/db]
  [?sentinel_username=..&sentinel_password=..]` (sentinels' port 26379 by default; the
  credentials and database are the primary's). The pool asks the sentinels in turn
  (`SENTINEL GET-MASTER-ADDR-BY-NAME`, with the configured timeouts) until one names a server
  whose `ROLE` is `master`, and remembers it. Shared connections are opened to the known
  primary; `reconnect()` (after `READONLY`, ...) forgets it, and behind Sentinel `with_retry`
  also asks again after a dropped or refused connection; every dedicated connection
  (`blocking_conn`: fetchers, the liveness thread) asks afresh, since one is opened after an
  error. A lookup that finds a new address drops the shared connections. Lookups run one at a
  time and callers waiting for one take its result. No sentinel knowing the service fails at
  once (`MasterNameNotFoundBySentinel`); a sentinel naming a server that is not a primary (a
  failover under way) or no sentinel answering is transient. `RedisConfig::connection_info`
  refuses Sentinel URLs (they name no single server), and `rediss+sentinel://` (or a
  `TlsConfig` with a Sentinel URL) is refused: TLS through Sentinel is not supported yet. As
  with any Sentinel client, jobs pushed to the old primary between the promotion and its
  demotion are lost when it resynchronizes from the new one.
- `Hench::start` / `run` / `run_until` are implemented in `launcher.rs` (engine lane) as an
  `impl Hench` block; `Hench`'s fields are `pub(crate)`.

### Components (one task each, all driven by a `CancellationToken` tree)

- **Fetch** (`fetch.rs`, public as `hench::fetch`): `Fetcher` (one per capsule; strategy
  `FetchStrategy::{Basic, Reliable}`), `FetchWorker` (one per processor: owns the dedicated
  blocking connection) and `UnitOfWork { queue (key), job (exact bytes) }`. Both strategies
  honour strict vs weighted queue order: weighted/random = a fresh order per fetch in which
  each queue comes next with probability `weight / remaining total` (zero weights count as
  1) — the distribution Sidekiq gets by shuffling the weight-expanded list and
  deduplicating, without materializing it. Every fetch first runs one non-blocking script
  over the queues in that order, **skipping queues in the `paused` set**; only when every
  open queue is empty does it block for `fetch_timeout` (default 2 s) on a *dedicated*
  connection (redis-rs 1.8 has a 500 ms default response timeout — blocking commands never
  use the shared multiplexed one). If every queue is paused it sleeps `fetch_timeout`
  instead.
  - `Basic`: the script `RPOP`s the first available job; the blocking step is `BRPOP` on all
    open queues. Ack = no-op; requeue = `RPUSH` (front of the queue).
  - `Reliable` (default): the script `LMOVE`s the first available job from `queue:<q>` to
    `<identity>:inprogress` and `SADD`s the identity to `inprogress`; the blocking step is
    `BLMOVE` on one open queue, rotated per processor (`(index + turn) % open`) so that
    every queue has waiters. Ack = `LREM <identity>:inprogress -1 <raw job>`: in-progress
    lists are searched from the tail (every `LREM`/`LPOS` on them), where the oldest jobs
    are since fetches push to the head and jobs mostly finish in fetch order, so an ack
    costs about the same at any concurrency (from the head it scanned most of the list).
    Requeue = script: `LREM` and, only if the job was still there, `RPUSH` (so a job already
    acknowledged or recovered is never duplicated); bulk requeue does the same per job in
    batches of 100. `BLMOVE` is pipelined with `SADD inprogress <identity>` on the same
    connection (no extra round trip; the `SADD`'s errors are ignored so that they can never
    drop a moved job), so a job taken that way also re-registers the owner, as the script
    does atomically. Basic requeue (`RPUSH`) is retried after a dropped connection, so a
    lost reply can push a job twice: duplicates are preferred to losses.
  - **Orphan recovery** (reliable only), at startup and every `orphan_check_interval` until
    quiet, under the `inprogress_recovery` lock (`SET NX PX`, held for the whole interval, kept
    while the check runs and not released, so the cluster checks at most once per interval and
    one check runs at a time; see the lock below): for every identity
    in `inprogress` other than ours whose heartbeat key is gone, and still gone after
    `orphan_grace` (a second look, 30 s by default and never less than twice the heartbeat
    interval: Redis can lose the keys of live processes, a restart without persistence, an old
    snapshot or `FLUSHALL`, and they re-create theirs within a heartbeat interval; the wait ends
    early, recovering nothing, when the process goes quiet), each in-progress job goes
    back to the front of the queue named in its `queue` field (newest first, so the oldest
    job runs first), except the suspects and poison pills below; jobs that cannot be parsed or
    name no queue go to the morgue (same trims); the identity leaves `inprogress` once its list
    is empty. The same procedure empties our own list at the end of a shutdown (without the
    counting of poison pills below).
    With the default interval (60 s), a crashed process's jobs are back in their queues about
    1.5 to 2.5 minutes after the crash (its heartbeat key expires within 60 s, the next check
    finds it gone and waits the grace); the check costs one `SET NX` per process and interval,
    and the winner reads `inprogress` and one `EXISTS` per member. A process registers its first heartbeat
    before fetching, and re-adds itself to `inprogress` on every beat and every fetch; the
    expiry of its heartbeat key is also refreshed every `heartbeat_interval` by a thread of
    its own with a dedicated connection (see the heartbeat), so neither jobs that block the
    tokio runtime nor slow lifecycle callbacks can let it expire: a live process is never
    treated as dead (except after a partition longer than 60 s: at-least-once), even after Redis
    lost its keys (the thread re-creates a lost key, and the orphan grace gives it the time to).
    As in Sidekiq, liveness means the process runs, not that its runtime makes progress: the
    jobs of a process whose runtime is wedged stay in its list until it stops or dies (systemd's
    watchdog, §9d, restarts such a process).
  - **Poison pills** (owner-directed improvement, round 3, refined in the release review;
    Sidekiq Pro's super_fetch kills a job recovered three times in 72 hours, BullMQ fails a job
    stalled more than `maxStalledCount` times): a job that kills the process running it
    (out-of-memory kill, abort, foreign-code crash) would be recovered and crash the next process
    forever. Orphan recovery of a *dead* process counts each job's recoveries in
    `recovered-<jid>` (read with `MGET` when the list is read; the `RECOVER` script writes the
    copy, then `INCR`s and `PEXPIRE`s the counter under `pcall`, then removes the entry, all
    guarded by `LPOS`, so a repeated script moves and counts once; the job's bytes are
    unchanged). The jobs of a dead list are judged together, once, on the list as the crash left
    it (`fetch/poison.rs`):
    - *first recovery* (no count): back to the front of its queue at once, with the others —
      the usual case (a deploy's `kill -9`, a lost node, an out-of-memory kill of a busy
      process);
    - *suspect* (recovered before, so in flight at an earlier crash too, and within the limit):
      moved to `recovery-suspects` (`PARK`: `LPOS` guard, `ZADD`, `LREM`), not counted yet. Each
      orphan check releases at most one suspect (`RELEASE_SUSPECT`: back to the front of its
      queue, its recovery counted, guarded by `ZSCORE`, recorded in
      `recovery-suspects:released`), dead processes or not, and only once the suspect released
      before it is no longer in flight: not within 1 000 jobs of the front of its queue (unless
      the queue is paused) and in no in-progress list (`LPOS`, pipelined), or released ten
      orphan-check intervals ago (a job running that long without killing its process is not
      what kills them quickly). Two suspects never run at once, however long they run (one
      suspect per check only overlapped neighbours longer than the interval). Order: the most
      recovered first, then the first parked, and of one crash the most recently fetched first
      (a job that kills its process soon after it starts was fetched after the jobs running
      beside it). Every job
      in flight at a crash is counted, the culprit and the jobs that merely ran beside it; before
      the release review all of them went back together, to the front of their queues, so the
      next idle processors (the restarted process, or the only one of a small deployment)
      fetched them together again, they were in flight at every crash and all reached the
      limit with the pill (crashloop-1). Released alone, a suspect that kills its process again
      dies beside first-time jobs only; the jobs that ran beside it come back one by one and
      run;
    - *poison pill*: a job whose recovery would be number `max_recoveries + 1` or later (each
      within 72 h of the previous one), when it is the only suspect of its list at the limit
      (with `max_recoveries(0)`, every job). Several suspects of one crash reaching the limit
      together (a crash cannot be pinned on one of them; `max_recoveries(1)` at the second
      crash, or two suspects that overlapped) each get one recovery more, alone, as suspects; a
      job recovered beyond the limit already is a pill whatever crashed with it.

    A pill is settled in the dead process's list like a job whose retries are exhausted: it is
    first claimed (`recovered-<jid>:claim`, `of` the dead process, `by` this one, 10 minutes; a
    claim of another process on the same list makes this check leave it alone, so that two
    checks that overlap — a lock that lapsed during a stall — never run its handlers twice); its
    batch counts it dead (`BATCH_FAILED`, reason `morgue`/`discarded`) before the write;
    `SETTLE` (or `SETTLE_UNIQUE`, releasing a `success`/`start` lock) moves it to the morgue with
    `error_class` `hench::CrashLoop`, an `error_message` naming the count and how many other
    jobs were in flight at its last crash, and `failed_at` (a `dead: false` job is dropped,
    `discarded_at` set); then the recovering process runs its death handlers and its error
    handlers (context `job recovered too many times`), dispatches the batch's events and records
    a failed run in the job's schedule; its log line names the jobs that were in flight beside
    it. `retries_exhausted` does not run (no execution failed), and the retry count is unchanged.
    The counter outlives the burial (until its window ends), so a copy retried from the morgue
    that still crashes goes back at its next recovery. Progress resets the count: an iterable
    job's first save of an execution beyond the cursor it started from deletes the counter (in
    its state's pipeline), so a long iteration that keeps making progress is never buried by
    unrelated crashes (crashloop-2); other jobs restart from scratch at each crash and their
    recoveries count until the window ends, whatever ran in between. Jobs without a jid, and
    requeues at a shutdown or by the own-list reconciliation, are not counted. Cost: suspects
    come back one at a time, at most one per orphan check (60 s by default), so after a crash
    loop the jobs that ran beside the pill run minutes later rather than at once (they wait in
    `recovery-suspects`, which only `redis-cli` shows); a crash whose jobs are all at their
    first recovery is not delayed. The reviewers' multi-process scenarios (one to ten worker
    processes, idle or loaded, 4 s neighbours) buried one to five jobs per pill before, only the
    pill since. `max_recoveries(0)` never runs a job again after its process died; `u32::MAX`
    never gives up (suspects still come back one at a time). The soak checker verifies that
    every `hench::CrashLoop` death followed at least `max_recoveries` executions ended by
    `kill -9`.
  - **The orphan check's lock** is taken with `SET NX PX orphan_check_interval` and kept while
    the check runs (`KEEP_RECOVERY_LOCK`: still this process's, taken back if it lapsed, at
    least half the interval left, at most 5 s more; checked during the grace wait, before each
    dead list and before each pill): a check that finds another process holding it stops, so
    one process recovers at a time even when the grace exceeds the interval or the runtime
    stalls; a check still starts at most once per interval (the lock is not released at the
    end).
  - **Own-list reconciliation** (reliable only): the processors register the units of work
    they hold (`Held`: shared job bytes, from the fetch until the outcome is written or the
    job given up). Every `min(orphan_check_interval, 30 s)` until quiet the process reads
    its own in-progress list (`LRANGE`) and pushes back to their queues (`REQUEUE`; the
    morgue for unreadable jobs) the entries no held job accounts for that were also unheld
    at a check at least `fetch_timeout + max(response_timeout, 1 s) + 1 s` earlier (longer
    than any fetch waits for its reply, so a job on its way to a processor is never taken
    for one): jobs moved there by a fetch whose reply was lost (a timeout runs the script
    anyway, a dropped connection makes `with_retry` take another job) or left there by an
    outcome write that failed. They used to stay until the process stopped. A check that
    runs more than a second late (a stalled runtime, during which a reply may be waiting for
    its processor) restarts every entry's clock.
  - Cancellation is safe: a command that was sent always completes (a job taken from Redis
    is never dropped); only connection setup and the paused-queues sleep are cancelled.
- **Processor** (`processor.rs`): `concurrency` per capsule. Loop until quiet: fetch → (if
  quiet by then: requeue, exit) → parse (invalid JSON → straight to the morgue, acknowledged
  in the same step, error handlers with `jobstr`, not counted in stats) → work state → job span → `start` log →
  server middleware chain (the class is resolved first; unknown class → the chain's endpoint
  fails with `hench::UnknownJob`) → deserialize args → `perform`, all under `catch_unwind`
  (panics → class `panic`) → stats (`processed` always, `failed` on error) → `done`/`fail`
  log → outcome. Success → ack. Failure → retry handler (on the job *as fetched*, not as
  middleware changed it) → its verdict written and the job acknowledged (`Fetcher::settle`:
  with reliable fetch one script — `LPOS` check, `ZADD retry`, or `ZADD dead` + the morgue's
  trims, or nothing, then `LREM` — that writes nothing if the job is no longer in progress,
  so it can be repeated, and leaves no window in which a crash or a hard shutdown finds a
  failed job both written and still in progress, to run again; basic fetch only writes) →
  death handlers (in the process, after the write, also when a repeated script found the job
  gone; a crash or `kill -9` between the write and them skips them, the job being in the
  morgue already) → error handlers (context `job failed`, the job as middleware left it, the queue). A job failing with class `hench::Shutdown` while
  the process is stopping is requeued instead (no retry, no error report) — the equivalent
  of Sidekiq's "exception caused by Shutdown". A job failing with class `hench::Interrupted`
  (`JobError::interrupted`, returned by iterable jobs that saved their cursor) is pushed back
  to the *end* of its queue: the job as fetched goes through `Client::prepare_push` (client
  middleware runs; jid and `created_at` kept, `enqueued_at` refreshed), then
  `Fetcher::push_back` writes it and acknowledges the fetched copy — for reliable fetch in one
  script (`LPOS` check, `SADD queues` + `LPUSH` or `ZADD schedule` if middleware scheduled it,
  then `LREM`: writes before the removal, nothing written if the job is no longer in progress,
  so retries are idempotent); basic fetch pushes in a `MULTI`. It counts as processed, not
  failed, uses no retry and reports no error; the job logger logs `interrupted`. Middleware
  stopping the push drops the job (warned); a rejected push (invalid job, middleware error)
  falls back to the failure path. Each execution is also tracked for metrics (§9): `Success`,
  `Failure` (errors, panics, shutdown requeues, hard-shutdown interruptions) or `Interrupted`,
  under the registered job's name (the payload's class for unknown jobs), with the time of the
  whole middleware chain. Writing the outcome (ack, retry, morgue,
  requeue) is retried with backoff (100 ms → 5 s) for as long as Redis is *unavailable* —
  unreachable, failing over, or refusing everything (`OOM`, `LOADING`, `BUSY`, `MISCONF`,
  `MASTERDOWN`, `TRYAGAIN`, `CLUSTERDOWN`, also inside a transaction) — until a hard
  shutdown; other Redis errors get 3 attempts. If it still fails, reliable fetch leaves the
  job in progress (pushed back by the own-list reconciliation, or recovered by another
  process after a crash), basic fetch logs the job's JSON (lost, as in Sidekiq).
  Fetch errors are retried every second and reported without flooding (the scheduler is
  tracked separately, the same way): at most one notice a minute, the first of an episode of
  failures to the error handlers (`could not fetch jobs`), later ones as warn logs; the end
  of a reported episode is logged. An activity that keeps failing and recovering (say one
  broken queue) therefore produces at most one notice a minute. A panic in the loop itself
  (a hench bug) restarts the processor after requeueing its job and a 1 s pause. Only a hard
  shutdown interrupts a job: its future is dropped and its unit of work stays in the
  processor's slot for the manager to requeue; interrupted jobs count as processed and
  failed (as in Sidekiq).
- **Retry** (`retry.rs`): Sidekiq `JobRetry` semantics, split into `decide` (hooks, pure),
  `settlement` (what the fetcher writes with the acknowledgement: retry set, morgue or
  nothing; idempotent, retried by the processor) and `finish` (death handlers). `retry` is the payload's, else the job type's options' (written into the
  payload), else `true`; `false` → no retry set, no morgue, death handlers get the job as
  fetched. Otherwise: queue = non-empty `retry_queue`, else the queue fetched from;
  `error_message` (truncated to 10 000 chars), `error_class`; `retry_count` 0 + `failed_at`
  on the first failure, else +1 + `retried_at`; `error_backtrace` when `backtrace` is set
  (`true` = all lines, `n` = first n, counted after `HenchBuilder::backtrace_cleaner`, Sidekiq's
  `backtrace_cleaner`, filtered them; a cleaner that panics is reported and the lines are
  stored as they were). Exhausted when `failed_at + retry_for ≤ now` if
  `retry_for` is set (it replaces the count limit; `retry_for: 0` exhausts immediately),
  else when `retry_count ≥ max` (`retry: n`, or `max_retries`). Then `retry_in` (not
  consulted once exhausted; a panic → default, reported): `Discard` → `discarded_at`, death
  handlers; `Kill` → exhausted; `After(d)` with `d > 0` → `d`; otherwise `count⁴ + 15` s.
  Plus jitter: a whole number of seconds below `10 × (count+1)`; a retry later than
  `MAX_AT` is scheduled at `MAX_AT`, with a warning (round 3: `After(Duration::MAX)` stored a
  score of 1.8e19 seconds). `ZADD retry`. Exhausted →
  `retries_exhausted` (panic → default, reported) → morgue (MULTI ZADD + `ZREMRANGEBYSCORE`
  older than `dead_timeout` + `ZREMRANGEBYRANK 0 -dead_max_jobs`, which like Sidekiq keeps
  the newest `dead_max_jobs - 1`) unless `dead: false` or `Exhausted::Discard` (→
  `discarded_at`); death handlers run for every job that will not be retried. Unknown
  classes get no hooks.
- **Scheduler** (`scheduled.rs`): due jobs of `retry`, then `schedule`, one at a time,
  earliest first; each is re-pushed through the process client (client middleware runs again;
  `enqueued_at` is refreshed, `created_at` kept). A job never leaves Redis before it is written
  elsewhere: a script *leases* the earliest member with score ≤ now (`ZADD XX` of its score to
  now + 60 s, so that no other scheduler takes it meanwhile) and returns it with its original
  score; then `Client::prepare_push` (normalization and client middleware, under
  `catch_unwind`: a panic is a rejected push) and one script, the API's `MOVE_OUT` (§7), that
  pushes the job to its queue (or the schedule, if middleware scheduled it) and only then
  `ZREM`s it, provided it is still in the set (so a script repeated after a dropped connection,
  or racing an API move, moves it once; middleware stopping the push only removes it, like
  Sidekiq, and a batch member or a unique job is then deleted as the API deletes one: counted
  dead in its batch first, its lock released in the removal's script, `REMOVE_UNIQUE`; round
  3). A lost reply, a crash, a panic or the end of a shutdown at any point therefore
  leaves the job in its set, due again when its lease ends at the latest (the shutdown drops a
  push still in progress, such as a slow client middleware, as soon as it stops the background
  tasks). A member that is not valid JSON, or a push failing with `InvalidJob`/`Json`, moves to
  the morgue through the API's `MOVE_TO_MORGUE` (reported; a batch member or a unique job is
  killed as the API kills one, `KILL_UNIQUE`, so that its batch counts it dead and its lock is
  released). A push failing because Redis is
  unavailable (see the processor) gives the member its original score back (`ZADD XX`; if that
  fails too, the lease ends a minute later) and leaves the rest of that set for the next poll;
  any other push error (client middleware failing or panicking, or a Redis error specific to
  the job's keys such as `WRONGTYPE`) scores it a minute later (a unique job holding its lock
  keeps it for that minute: `RESCORE_UNIQUE`, the API's `reschedule`) and moves on, so a job that keeps
  failing cannot block the set — after ten such failures in a set the rest of that set waits
  for the next poll, bounding the work while middleware fails for every job without starving
  the other set (Sidekiq would lose the job in all these cases). Failures are reported (with
  the job's JSON, and a panic's class `panic`) through the scheduler's rate-limited notices. A
  panic in the poller itself (a hench bug) is reported (`scheduler crashed; restarting it`)
  and polling resumes after a second. While leased, a job shows its lease's score (about a
  minute ahead) in the dashboard.
  Random poll interval: if process_count < 10 → `avg × rand + avg/2`, else `avg × rand × 2`
  (at least 10 ms), `avg` = `poll_interval_average` or process_count (the live members of
  `processes`, those whose heartbeat has `info`, min 1: the size of the set counts processes
  that died until they are pruned) × `average_scheduled_poll_interval`. Errors are reported
  like fetch errors (`could not enqueue scheduled jobs`). Initial wait
  `initial_scheduler_wait` (skipped when `poll_interval_average` is set) + rand ×
  `initial_scheduler_jitter`, then before every poll prune dead processes (`process_cleanup`
  lock: `SET NX EX 60`, so the cluster does the work once a minute; `SSCAN processes`, remove
  members without an `info` field — the same function as `api::ProcessSet::cleanup`). It used
  to prune once at start, so processes that died while others started (a crash loop) stayed
  listed for good. Stops on quiet.
- **Heartbeat / Launcher** (`launcher.rs`): `start` checks Redis (`INFO`, retried once after a
  dropped connection: version ≥ 6.2 for `LMOVE`/`BLMOVE`/`ZRANGE BYSCORE`, else `Error::Config`;
  warns unless `maxmemory-policy noeviction`; skipped if `INFO` is unavailable), logs `starting`
  with the registered jobs (warning when there are none), runs the startup checks
  (`on_startup_try`) and the `startup` callbacks in registration order — one that fails or
  panics is reported (`Exception during lifecycle event`) and stops the start:
  `start`/`run`/`run_until` return `Error::Startup` before the process registers or fetches
  anything, as Sidekiq fires `startup` re-raising (it used to be reported and the start went on,
  so a failed check left a process taking jobs that would all fail) — beats once inline (so
  `start` fails if Redis is unreachable, and the process is registered before it fetches), then
  spawns the heartbeat, the liveness thread, the orphan check, the scheduler, the managers and a
  supervisor (and, under a systemd watchdog, the keep-alive task, §9d). The
  liveness thread (an OS thread with a current-thread runtime and its own Redis connection)
  runs a script every `heartbeat_interval` until the background tasks stop at the end of the
  shutdown (or the process is abandoned, or every handle on it is gone), so the heartbeat key
  never depends on the runtime the jobs run on: `EXPIRE identity 60`, and if the key is gone
  (Redis lost it) `HSET identity liveness <now>` + `EXPIRE` (the heartbeat writes the other
  fields, and fires `heartbeat`, when it finds no `info`). A failed refresh drops the
  connection and is tried at once on a new one: a primary demoted by a failover keeps its
  clients connected and answers `READONLY`, which the connection manager does not reconnect
  after, so the thread used to stay on the demoted node for good; failures lasting 20 s are
  logged as warnings (once a minute). Every `heartbeat_interval`: flush the stats (`INCRBY`
  lifetime + daily keys + `EXPIRE` 5 years, one batch written once, see "Counter flushes"),
  flush the metrics (§9b; also at the end of a shutdown, after the stats, bounded to 5 s),
  rewrite `<identity>:work` (MULTI `UNLINK` + `HSET` + `EXPIRE 60`; the work state shares each
  job's fetched bytes and decodes them only here, so a job is not copied to be shown), measure
  RTT (`PING`, µs; a warning when 5 readings in a row exceed 50 ms), MULTI `SADD processes`,
  `HEXISTS identity info`,
  `HSET identity info concurrency busy beat rtt_us quiet rss` (`rss` = `VmRSS` of
  `/proc/self/status` in kB, 0 elsewhere), `EXPIRE identity 60`, `LINDEX <identity>-signals
  -1` (+ `SADD inprogress identity` with reliable fetch). The oldest signal is handled and
  then removed (`LREM … -1`), so a lost reply never loses a signal (signals are idempotent);
  unknown signals are logged and removed. `heartbeat` fires whenever the identity key had no
  `info`: the first beat, or one after the key expired or Redis lost it (a key the liveness
  thread re-created has no `info` either); `beat` fires on every beat; both
  run inside the heartbeat, as in Sidekiq (keep them quick: a slow one delays the next beat
  and the handling of remote signals, not the liveness thread). Quieting triggers a beat
  right away, so the dashboard sees `quiet=true` immediately. Remote `TSTP` → quiet (its
  callbacks run in a task of their own, never in the heartbeat), `TERM` → stop, `TTIN` →
  dump (below), in every mode (embedded too). `info` JSON: hostname (`DYNO` if set),
  started_at, pid, tag, capsules {name: {concurrency, mode, weights}}, queues, weights,
  labels, identity, version, embedded (`false` only for `run`). Identity nonce: per
  launcher. Worker slot ids (`tid`): base-36, unique per process.
- **Counter flushes** (`flush.rs`, crate-internal, shared by the stats and the metrics
  tracker): counts added to Redis by a heartbeat must be counted once although a reply that
  timed out does not mean the write was dropped (Redis runs the requests it received; a
  stalled server — a long script, a fork, a paused VM — runs them when it continues; redis-rs
  keeps the connection after a timeout). Each `Flusher` (one per `Stats`, one per metrics
  `Tracker`) numbers its batches (1, 2, ...) and keeps the batch being written until Redis
  confirms it; the next flush sends it again, unchanged (same keys, so metrics stay in the
  minute of their first attempt), before taking new counts, which accumulate meanwhile. A
  script applies a batch only if its number is above the one stored in the flusher's guard
  (`flush:<id>`, random id; `SET … EX` 1 day in the same script) and returns -1 otherwise, so
  every attempt after the first one applied is a no-op, including an old attempt run late
  (batches are sent one at a time, in order). The guard is written first: under `OOM` or on a
  read-only replica the script fails before counting anything. The writes run with `pcall`, so
  a key of the wrong type loses only its own write (logged, not retried). A batch unconfirmed
  for 12 hours is given up (logged): its guard (a day) outlives every attempt by hours. Sidekiq
  drops counts whose flush failed; hench's earlier "merge back and resend" counted a timed-out
  flush once per heartbeat of the stall.
- **Launcher API**: `Launcher` (Clone, `#[must_use]`; dropping it does not stop the
  process): `identity()`, `quiet()`, `stop()` (returns once stopped; idempotent), `wait()`
  (until stopped for any reason, e.g. a remote `TERM`), `is_quiet()`, `is_stopping()`,
  `is_stopped()`; `abandon()` (feature `test-util`, hidden) aborts every task without
  cleanup to simulate a crash. `run()` listens to SIGINT/SIGTERM (stop; a second one during
  the shutdown interrupts running jobs right away), SIGTSTP (quiet) and SIGTTIN (dump, also
  during the shutdown) — Ctrl-C only on non-Unix — and also returns after a remote stop. A
  dump (Sidekiq's thread dump on `TTIN`; Rust cannot list its tasks' stacks) logs at `WARN`
  the process's identity, busy slots, concurrency and quiet/stopping state, then, per busy
  slot, the job's jid, class and queue and how long it has run. Left to its default action,
  SIGTTIN stopped the whole process, liveness thread included, so that other processes
  recovered its jobs and ran them a second time; its SIGTSTP starts the quiet without
  waiting for the callbacks, so a stop signal is still handled meanwhile. `run_until(fut)`
  stops when `fut` completes or after a remote stop. Dropping the future of `run` or
  `run_until` before it completes (a lost `select!` branch, a timeout) requests the shutdown
  (a drop guard on the stop token), which the supervisor carries out as long as the runtime
  runs. `quiet()` returns once the `quiet` callbacks have run, whoever started them.
- **Manager / shutdown** (`manager.rs`, supervisor in `launcher.rs`): `quiet()` (once):
  cancel the quiet token (processors finish their job and exit; scheduler and orphan check
  stop; the heartbeat continues with `quiet=true`), fire `quiet` (reverse order) in a task of
  its own, whose end every `quiet()` caller waits for. `stop()`: deadline = now + `timeout`;
  quiet, waiting for the quiet callbacks (also when a remote or SIGTSTP quiet started them
  earlier) until the deadline or a forced stop, so that they finish before the shutdown ones;
  cancel the `stopping` token (`ctx.stopping()`); each
  manager waits for its processors until the deadline while `shutdown` callbacks run
  (reverse); then hard shutdown: cancel the hard token (running jobs are dropped), wait up
  to 3 s + `fetch_timeout` for processors to exit (one blocked in a fetch completes it and
  requeues what it got), bulk-requeue the units of work left in the slots, in chunks of 100
  so that a failure is reported for exactly the jobs concerned (a requeue that times out is
  reported as possibly, not certainly, failed), warn about tasks that ignore cancellation
  (blocking code). Then stop the heartbeat (aborted after 5 s), empty our in-progress list
  (reliable: requeue leftovers, leave `inprogress`), flush the stats, `SREM processes` +
  `UNLINK work`, fire `exit` (reverse). Errors talking to Redis during the shutdown are
  logged; `stop()` returns `Ok`. Each Redis cleanup step (requeueing interrupted jobs,
  emptying the in-progress list, removing the process — before flushing the stats, so a slow
  flush cannot keep a stopped process on the dashboard) is bounded to 5 s, so an unreachable
  Redis (whose connection attempts can each take seconds) cannot hold the process.
- **Job logger** (`job_logger.rs`): `tracing` span `job{class, jid, bid, tags, log_level,
  attrs}` (bid/tags/log_level/attrs recorded only when present, tags comma-joined) around the
  whole processing of a job, so logs of the job, middleware and handlers carry them. `attrs`
  holds, as a JSON object (`attrs={"request_id":"req-4242"}`), the payload keys listed by
  `HenchBuilder::logged_job_attributes` (Sidekiq's `logged_job_attributes`; default none:
  `bid` and `tags` have their own fields and are left out of it, like `class`, `jid` and
  `log_level`) that the job has, custom keys and payload fields alike (null values left out);
  a span's fields are declared when it is created, so they cannot be added by middleware
  (the testing clients' spans have no `attrs`); `start`,
  `done`/`fail`/`interrupted` events with `elapsed` (seconds, ms precision) at info level.
  `.job_logging(false)` drops the events, not the span; so does a job `log_level` of `warn` or
  `error` (§9f).
- **Error contexts** passed to the error handlers by the engine: `job failed`, `job is not
  valid JSON`, `could not record a failed job`, `could not fetch jobs`, `processor crashed;
  restarting it`, `retry_in panicked; using the default retry delay`, `retries_exhausted
  panicked`, `could not enqueue scheduled jobs`, `scheduled job is not valid JSON`, `scheduled
  job is invalid`, `scheduler crashed; restarting it`, `the shutdown panicked`, `job recovered
  too many times` (a poison pill), `backtrace_cleaner panicked; storing the backtrace uncleaned` (plus the spine's
  death-handler and lifecycle contexts).

### The execution pipeline of the pro features (`docs/design/pro-features.md` §3.2)

`Processor::run_job` builds `pro::Fetched` (the unique lock, batch membership, schedule and
throttle of the job **as fetched**) and runs the features' steps in this fixed order, each only
for the jobs that use the feature (a job without any takes exactly the path, and sends exactly
the commands, it did before them):

1. **Throttle gate** (`limit::engine::gate`, jobs naming a throttle), before `start_work`:
   `Gate::Run`; `Refused(reschedule)`: a member of a cancelled batch is skipped
   (`batch::endpoint::member_cancelled`, accounting, acknowledgement), any other job is parked
   in `schedule` with `limited` set (`PUSH_BACK` in its schedule mode; not processed; metrics
   `t`); `Unavailable(error)`: requeued at the front, fetch outage notice, 1 s pause;
   `Failed(error)`: the failure path.
2. `start_work`, the job's span, `start` log, server middleware, then `PerformEndpoint`: the
   batch cancellation check (`batch::endpoint::check`, `Err(JobError::batch_cancelled())`),
   the release of a `start` unique lock (`unique::endpoint::release_start`), `perform`.
3. Classification (`pro::Classified`): `Success` (or a halt), `Skipped` (`batch_cancelled` from
   a member, confirmed by the accounting; else a failure), `Interrupted`, `Shutdown`,
   `OverLimit(reschedule)` (`limit::engine::over_limit`: a limiter's refusal within its budget;
   past it, `OverLimitDecision::Exhausted(error)` takes the failure path), `Throttled`,
   `Failure(verdict)`.
4. **Batch accounting** before the outcome write (`batch::account::before_outcome`, members
   only): a crash between the two re-runs the job, whose accounting is then a no-op.
5. **Outcome write with the unique lock work in the same script**: `Fetcher::acknowledge_with`,
   `settle_with`, `push_back_with` take `unique::engine::lock_op(..)` (jobs holding a lock);
   `None` is exactly the plain write. A requeue at shutdown leaves the lock alone.
6. After the write: the permits the engine holds are released (`limit::engine::release`, keyed
   by the processor's slot), then the death handlers, the dispatch of the batch events the
   accounting recorded (`batch::dispatch::after_outcome`), the periodic record of the execution
   (`periodic::record_run::spawn`, which filters the outcomes), then the error handlers.

| Outcome | `stat:processed` | `stat:failed` | metrics | timed | job logger |
|---|---|---|---|---|---|
| success, halt | +1 | | `p` | yes | `done` |
| failure, shutdown requeue, hard shutdown | +1 | +1 | `p`, `f` | no | `fail` |
| interrupted | +1 | | `p` | yes | `interrupted` |
| over limit while running (rescheduled) | +1 | | `p`, `o` | no | `over_limit` (info) |
| throttled at the gate (parked) | | | `t` | no | `throttled` (debug) |
| skipped (batch cancelled) | +1 | | `p`, `s` | no | `skipped (batch cancelled)` (info) |

The heartbeat runs the features' renewals after the work state (`pro::on_beat`: unique locks,
then engine-held permits) and its `info` JSON gains `"features"` (the stored-format version of
each feature the process understands: `{"unique": 1, "periodic": 1, "batches": 1, "limits": 1}`,
`periodic` only with the cargo feature), `"periodic"` (`periodic::info`: the poller, the group,
its crontab digest and schedule count) and `"throttles"` (the definitions, when there are some). Background tasks (`pro::background_tasks`): the periodic
poller (until the shutdown starts) and batch maintenance (until quiet), each with its outage
notices (`Engine::periodic_outage`, `batch_maintenance_outage`). At start, `pro::start_checks`
warns when Redis 6.2 answers `lua-replicate-commands no` (every feature script reads `TIME`
before writing). The manager releases the permits of the jobs a hard shutdown interrupted
(`limit::engine::release_slots`, bounded like the other cleanup steps). Error contexts added by
the features: `could not enqueue periodic jobs`, `periodic job is invalid`, `could not register
periodic jobs`, `periodic poller crashed; restarting it`, `could not record a job in its batch`,
`could not dispatch batch callbacks`, `could not maintain batches`, `batch maintenance crashed;
restarting it`, `over-limit backoff panicked; using the default`, `over_limit_when panicked`.

## 6. Middleware

Tower-like "around" chains (`async_trait`):

```rust
#[async_trait]
pub trait ServerMiddleware: Send + Sync + 'static {
    async fn call(&self, ctx: &JobContext, job: &mut JobPayload, next: ServerNext<'_>) -> Result<(), JobError>;
}
#[async_trait]
pub trait ClientMiddleware: Send + Sync + 'static {
    async fn call(&self, job: JobPayload, redis: &RedisPool, next: ClientNext<'_>) -> Result<Option<JobPayload>>;
}
```

Not calling `next` halts (client: job not pushed, `push` returns `None`; server: job
considered done and acked). Chains support `add`, `prepend`, `insert_before::<T>`,
`insert_after::<T>`, `remove::<T>`, `exists::<T>` (by `TypeId`).

- A chain holds at most one middleware per type: `add`/`prepend`/`insert_*` replace an
  existing entry of the same type (Sidekiq keeps the old entry when moving it with
  `insert_*`; we use the new instance). `insert_before` a missing type inserts first,
  `insert_after` a missing type appends.
- Middleware instances are shared by every invocation (Sidekiq instantiates one per job):
  keep per-job state in locals.
- `ServerChain::invoke(ctx, &mut job, &dyn ServerEndpoint)` runs the chain;
  `PerformEndpoint::new(Arc<dyn ErasedJob>)` performs the job with the payload as modified by
  middleware (`ctx.with_payload(job)`) and does not catch panics (callers wrap the whole
  chain). The processor and the testing modes share one crate-internal execution path,
  `middleware::execute_job(chain, ctx, &mut job, Option<Arc<dyn ErasedJob>>)`: the chain, then
  `PerformEndpoint` (or, for an unregistered class, an endpoint failing with
  `hench::UnknownJob`), all under `catch_panic`. Each builds its own context (the processor:
  fetched queue, process data, the `stopping` token; testing: the testing data and client).
  `ClientChain::invoke(job, &RedisPool)` returns the job to push or `None`. Client middleware
  output is validated again (class, queue, `at`).

Before `perform`, `PerformEndpoint` takes the features' steps on the job as fetched
(`ctx.payload()`: server middleware passes the context it received to `next`): first the batch
cancellation check, then the release of a `start` unique lock (§5).

## 7. API (`hench::api`, Sidekiq `api.rb`)

`Stats` (processed, failed, scheduled_size, retry_size, dead_size, enqueued,
processes_size, workers_size, default_queue_latency, queues), `StatsHistory`, `Queue`
(`all`, `exists`, `size`, `latency`, `is_paused`/`pause`/`unpause`, `jobs` page/iter, `find_job`, `clear`), `JobRecord`
(parsed + raw value; `display_class`, `display_args`, error backtrace decode, `latency`,
`delete`), `SortedEntry` (`at`, `retry` [decrements retry_count then pushes], `add_to_queue`,
`delete`, `kill`, `reschedule`), `SortedSet`/`JobSet` (`size`, `scan(pattern)`, `page`,
`fetch(score, jid)`, `find_job`, `delete_by_value`, `delete_by_jid`, `clear`, `retry_all`,
`add_all_to_queue`, `kill_all`), `ScheduledSet`, `RetrySet`, `DeadSet` (`kill(job, notify)` + trim),
`ProcessSet` (`cleanup`, iterate, `size`, `total_concurrency`, `total_rss`), `Process`
(`quiet()`, `stop()`, `is_stopping()`), `WorkSet` (iterate `(process, slot, Work)`).

Implementation (api lane):

- Everything hangs off `api::Api` (cheap clone): the `Client` used to re-push jobs (so client
  middleware runs), the morgue limits (defaults 10 000 / 180 days) and optionally a `Config`
  whose death handlers run on notified kills. `Api::new(redis)`, `from_client`,
  `from_hench(&hench)`, `with_config`, `with_dead_limits`; every API type's constructor takes
  `impl Into<Api>` (`&RedisPool`, `&Client`, `&Hench`, `&Api` all work). Factories:
  `api.stats()`, `queue(name)`, `queues()`, `scheduled()`, `retries()`, `dead()`,
  `processes()`, `work()`.
- Snapshots are structs (`Stats::fetch(&api)` with pub fields: two pipelines plus `SSCAN`s;
  `Stats::queues`, `Stats::queue_summaries`, `Stats::reset(&api, &[StatCounter])`).
  `StatsHistory::new(&api, days)` / `ending(&api, days, date)` (1..=1825 days, else
  `Error::Config`) returns `Vec<(NaiveDate, u64)>` newest first, one `MGET`.
- Collections are read in bounded pages: `Page<T> { items, number, size, total }` with
  `SortOrder::{Ascending, Descending}` (by time: queue push order, set score), page numbers
  below 1 mean 1, sizes capped at 1000, size 0 = no items; `page_items` (in memory, out-of-range
  page → first page, like Sidekiq) and `page_key(&api, key, ..)` (any list/zset; `TYPE` only
  for unknown keys). Iteration is a `'static` `Stream` (`Queue::jobs`, `JobSet::entries`, 50 per
  round trip, deletion-safe like Sidekiq: queues offset by `LLEN` shrinkage, sets by the
  entries removed through the same handle; sets stop after their initial page count).
- Timestamps are `SystemTime` (lenient: integer ms, float seconds < 1e11, float ms); latency is
  seconds as `f64`, never negative. `JobRecord` keeps the raw value and a `serde_json` map, is
  lenient (invalid JSON → empty map, the raw value as the only argument), `payload()` gives
  the strict typed form, `display_args()` hides the last argument of `"encrypt": true` jobs.
  `SortedEntry` derefs to `JobRecord`, the named sets deref to `JobSet`. Entry keys for URLs:
  `SortedEntry::key()` = `"{score}-{jid}"`, `api::parse_key`.
- `JobSet::scan(pattern)` uses `ZSCAN MATCH`: a pattern with `*` is a glob, anything else a
  substring with glob metacharacters escaped (Sidekiq passes them through); results are
  deduplicated. `find_job` scans with the jid and checks the parsed jid.
- Deviations from Sidekiq, for safety:
  - Moves are atomic. `add_to_queue`/`retry`/`retry_all` parse the job (unparseable → error,
    job kept), drop any `at`, run the client's normalization and middleware through
    `Client::prepare_push` (a crate-internal spine helper: `push` without delivery), then one
    Lua script checks the member is still in the set, `LPUSH`es the job (encoded as `raw_push`
    does: `enqueued_at` set; or `ZADD schedule` without `at`/`enqueued_at` if middleware
    scheduled it) with `SADD queues`, and only then `ZREM`s it. Writes that can fail (OOM)
    come before the removal and Redis does not roll scripts back, so any failure — middleware
    error or panic, Redis unavailable or out of memory, a cancelled caller — leaves the job
    where it was, and concurrent moves of one entry succeed once. (Sidekiq removes, then pushes
    through the client: a crash in between loses the job.) A move stopped by middleware only
    removes the job, like Sidekiq. Middleware runs before the script, so it may run for a job
    another caller has just moved (single-entry moves first check that the entry is still
    there). The scripts write before they remove (`SADD queues` before `LPUSH`) and report a job
    that stays in its own set (rescheduled by middleware, or killed in the morgue) separately,
    so iteration offsets only count real removals. `kill` and `kill_all` are scripts that add to the morgue
    before removing from the source (and do not remove when the source is the morgue), so a job
    is never lost nor both enqueued and dead. `reschedule` is `ZADD XX` (never resurrects or duplicates a
    job; Sidekiq's `ZINCRBY` of re-serialized JSON could), returns `false` for a vanished entry.
  - `retry_all` moves jobs one by one (as above) instead of popping and pushing. It walks
    the set by score (`ZRANGE BYSCORE` from just past the last score handled; a score whose
    jobs fill a whole batch is paged by rank within that score), so jobs left in place never
    need tracking. Unparseable jobs, rejected pushes (validation, middleware `Err`, Redis
    errors about one job such as `WRONGTYPE`) and middleware panics leave the job in place
    with a warning and the call continues; only Redis being unavailable (I/O, timeouts,
    `READONLY`, `LOADING`, `OOM`, ...) stops it. `kill_all` moves batches of 100 atomically
    (Lua) and trims once at the end.
  - Commands whose repetition would misreport jobs are never repeated by
    `RedisPool::with_retry`: the move scripts and the `ZREM`/`LREM` of deletes are sent once,
    and if they fail the job's presence decides (still there: the error is returned; gone: it
    moved or was deleted). Idempotent commands keep the retry (a remote signal may be
    delivered twice after a dropped connection, which is harmless). Redis being unavailable is
    recognized by error kind and code, including the errors inside an aborted transaction or
    a pipeline. `Client::push` and `Client::prepare_push` share one middleware-and-revalidate
    step (`run_middleware`), and `redis_push` and the move scripts share one encoding
    (`client::Stored`: scheduled without `at`/`enqueued_at`, or queued with `enqueued_at`).
  - `entries()` offsets pages by the removals of the entries it yielded only (a private
    counter), so deleting other entries through the same handle does not skip any;
    `retry_all` moves 16 jobs at a time (so jobs of one batch may reach their queues out of
    order; after a Redis outage error, in-flight moves finish and no new one starts); a
    single-entry `retry`/`add_to_queue` returns a rejected push as its error and resumes a
    middleware panic after putting the job back; `kill_all` records and trims the batches moved before an error. The morgue age
    trim is inclusive everywhere (like the engine's). `fetch_range` reads 500 entries per
    round trip, continuing from the last score read (skipping only the entries read at that
    score), so removals elsewhere cannot shift its pages; `scan_set` (`SSCAN`) returns sorted, deduplicated members for every caller.
    `find_job` searches for the jid as written in JSON, with every glob metacharacter escaped;
    `JobSet::schedule` stores the job without `at` and `enqueued_at`; the `process_cleanup`
    lock (`SET NX`) is sent once.
  - A re-push stopped by client middleware drops the job, as in Sidekiq, with a warning;
    `retry_all` does not count it. A batch member or a unique job is dropped as `delete`
    removes one (counted dead in its batch, its lock released; round 3).
  - `page_items` also treats a page starting exactly at the end as out of range.
  - A manual retry decrements `retry_count` saturating at 0 (Sidekiq can store -1, which the
    unsigned field cannot hold): a job retried by hand after its first failure gets one
    fewer automatic retry.
  - `DeadSet::trim` keeps the newest `dead_max_jobs - 1` jobs like Sidekiq (and the engine's
    morgue). Killed jobs notify death handlers with a `JobError` of class `hench::Killed`.
  - `Process::quiet`/`stop` work for embedded processes too (remote signals are handled in
    process). `Process::queues` prefers the ordered top-level `queues` list (capsule maps are
    serialized with sorted keys). `Process::dump` (Sidekiq's `dump_threads`) pushes `TTIN`: the
    process logs its running jobs (§5; no thread backtraces in Rust). No `leader`
    (Enterprise) or `ProfileSet`. `Queue::pause`/`unpause` add the queue's name to / remove it
    from the `paused` set (Sidekiq Pro's pause, here in the open; they return whether the state
    changed), which every fetch skips; a processor already blocked on the queue may still take
    one job arriving within its `fetch_timeout`. Pausing is by name: it outlives `clear` and can
    precede the queue's first job. `Queue::is_paused` and `Stats::queue_summaries` read the set;
    `queue_summaries` lists the queues of `queues` plus every paused queue (an emptied or
    never-used queue stays listed, with size 0, while it is paused, so that it can be resumed;
    `Stats::queues`, like Sidekiq's, lists `queues` alone).
  - `ProcessSet::cleanup` and the workers' scheduler (before every poll) share one
    implementation (`api::cleanup_processes`: the `process_cleanup` lock `SET NX EX 60` sent
    once, then `SSCAN processes`, pipelined `HGET <identity> info`, `SREM` of the members
    without one). `Stats::processes_size` counts the live members (those whose heartbeat has
    `busy`, written with `info`), not the size of the set (Sidekiq's `SCARD`, after the
    cleanup its `ProcessSet.new` runs), so processes that died never inflate it;
    `ProcessSet::size` stays the raw `SCARD`.

### The pro features in the API

Removals: every single-job delete and kill (`JobSet::delete_by_value` and what calls it,
`SortedEntry::kill`, `DeadSet::kill`/`kill_with_error`, `JobRecord::delete`) first counts a
batch member as dead (`batch::api::before_removal`, reasons `deleted`/`killed`), then removes
the job with the unique module's script when the job holds a lock (`unique::api::delete_from_set`,
`delete_from_queue`, `kill_from_set`, each `None` when it has nothing to do), then dispatches the
batch events (`batch::api::after_removal`). Jobs that are neither members nor unique are not even
parsed (`api::featured_job`). `kill_all` accounts each chunk right after `MOVE_LOWEST` moved it
(the move chooses the chunk), releases the chunk's locks (`unique::api::release_many`), then
dispatches. `Queue::clear` and `JobSet::clear` touch neither. The feature factories
(`Api::unique_locks`, `periodic`, `batches`, `limits`) and the `JobRecord` accessors
(`unique`, `periodic`, `batch_callback`, `limited`, `throttle`) come with the features
(`api/<feature>.rs`); `api::events` reads the per-minute counters (minute periods up to 8 h,
`Error::Config` beyond).

## 8. Testing support

- `hench::test_util::TestRedis` (feature `test-util`, used by every integration test):
  spawns a private `redis-server` (`--port 0 --unixsocket <tmp>/redis.sock --save ""
  --appendonly no`) per test, exposes `url()` (`redis+unix://…`), kills it on drop. Tests are
  parallel-safe with no shared state. CI installs `redis-server`.
- `TestRedis` API: `start()` (panics with a clear message, e.g. when `redis-server` is
  missing; set `HENCH_REDIS_SERVER` to override the binary) / `try_start()`,
  `start_with_args(&[..])` (extra `redis-server` arguments, kept on restarts, e.g. a renamed
  `INFO`), `url()`,
  `config()` (short timeouts, fast reconnection), `pool()`, `client()`, `builder()` (a
  `HenchBuilder` on this server with 100 ms intervals, no initial scheduler wait, 5 s
  timeout, no systemd notifications), `stop()` (simulate an outage), `restart()` (same socket, so pools reconnect),
  `is_running()`. The child is killed and reaped on drop; a watchdog (`sh`, reading a pipe
  from the test process) kills it and removes its directory if the test process dies without
  dropping it (a `kill -9`, an abort), which the system tells it by closing the pipe.
- `hench::testing` (Sidekiq::Testing): per-client modes instead of a global switch:
  `Client::fake()` (jobs captured in memory: `jobs::<J>()`, `jobs_for(name)`, `clear`,
  `clear_all`, `drain::<J>()`, `perform_one::<J>()`, `drain_all()`), and `Client::inline(..)`
  (executes on push via a `Registry`). Spine hooks (in `client.rs`): `pub(crate) enum
  ClientMode { Redis, Fake(testing::FakeStore), Inline(testing::InlineExecutor) }`,
  `Client::mode()`, `Client::with_mode(..)`; `raw_push` calls `testing::fake_push(store,
  payloads)` / `testing::inline_push(executor, client, payloads)` with the jobs as they leave
  client middleware (`at` only on jobs scheduled in the future).
- `hench::testing::Testing` configures testing clients (Sidekiq's
  `Sidekiq::Testing.server_middleware` and more): `register`/`register_default`/
  `register_dyn` (these win over same-named jobs, so tests can stub a job), `registry(r)`
  (instead of the inventory), `auto_register(bool)`, `data`/`data_arc`, `server_middleware`,
  `client_middleware`, `default_job_options`, `from_hench(&hench)`, `from_builder(builder)`
  (async: `from_hench` of what the builder builds with `RedisPool::detached()`, so tests that
  share the program's builder need no Redis, where `build` would connect); then `.fake()` /
  `.inline()`. `Client::fake()` = `Testing::new().fake()` (inventory jobs);
  `Client::inline(registry)` runs exactly `registry`. An explicit registration also shadows a
  name the inventory has twice (otherwise an error, as in workers). The registry is built when
  the first job runs (most fake clients, e.g. the one behind `perform_inline`, never run one); problems
  (duplicate inventory names) surface then as `Error::Config`.
- An interrupted job (`JobError::interrupted`, from an iterable job) is pushed again through the
  testing client, as a worker would push it back: a fake client captures it at the end of its
  queue (`drain` goes on and finishes it), an inline client runs it again at once — in a loop
  in the execution that was interrupted (after `Client::prepare_push`, so client middleware
  runs, and with a fresh `enqueued_at`), not through a nested push, so hundreds of
  interruptions do not grow the stack. (Jobs a running job pushes to an inline client still run
  nested, as in Sidekiq: a chain of a few dozen can overflow a 2 MiB test thread in debug
  builds; fake clients run chains one job after the other.)
  Iterable job state lives in memory for testing clients (shared by a client's clones;
  `Client::cancel` works on them). `Testing::max_iteration_runtime(d)` sets the limit
  workers take from the config (`from_hench` copies it).
- Both modes make a JSON round trip, run the testing server middleware, catch panics (through
  the workers' own execution path, `middleware::execute_job`), and give
  jobs a context whose client is the testing client (so nested pushes stay captured / inline)
  and whose data is the testing data; the job runs inside the workers' `job{class, jid, ..}`
  tracing span. `Testing::from_hench(&hench)` therefore runs a job exactly as that `Hench`'s
  workers would (same registry, data, server and client middleware), minus Redis, retries and
  the job logger's `start`/`done` events. Fake clients capture immediate jobs with `enqueued_at` and
  scheduled ones with `at` (like Sidekiq); when a job runs (drained or inline), a scheduled job
  looks as if the scheduler had enqueued it (no `at`, fresh `enqueued_at`). Jobs are resolved by
  name like in a worker; an unregistered class is `Error::UnknownJob`.
- Fake helpers are inherent methods on `Client`: `jobs::<J>()`, `jobs_for(name)`, `all_jobs()`,
  `jobs_in_queue(q)` (Sidekiq's `Queues[q]`), `clear::<J>()`/`clear_for(name)` (only that
  class; Sidekiq also empties the class's queue index), `clear_all()`, `drain::<J>()`/
  `drain_for(name)` and `drain_all()` (return how many ran; the first failure stops and is
  returned as `Error::Job`, the failed job is already removed; an unknown class or invalid
  registry fails before anything is removed), `perform_one::<J>()`/
  `perform_one_for(name)` (`Ok(None)` when nothing is captured, where Sidekiq raises). The
  synchronous accessors panic on non-fake clients (a test bug); the async ones return
  `Error::Config`. Inline pushes run each job in order and stop at the first failure.
- Deviation: `perform_inline` (spine) does not run middleware, while Sidekiq's runs client and
  server middleware; an inline `Testing` client is the full pipeline.

### The pro features in the testing modes (`docs/design/pro-features.md` §2.12)

Principle: push-time deterministic behaviour is enforced by the testing clients by default
(unique locks, batches); time-driven behaviour is not (rate limits, periodic polling). A testing
client, its clones and its collecting clients share `testing::Tables { unique, batches, limits }`.
The executor runs the throttle gate (`limit::memory::gate`) before server middleware and, for
jobs that use a feature, the outcome hooks in the processor's order (`batch::model::account`,
`unique::fake::after`, `limit::memory::after`; a testing failure is `Failure(NoRetry)`).
`Testing::unique_jobs(bool)` (default on), `Testing::limits(TestLimits)` (default `Unlimited`;
`InMemory` runs the limits on tokio's clock: a refused job is parked with its `limited` field,
`drain` leaves it until its slot, `Client::limited_jobs` lists it) and `Testing::throttle`;
`Testing::from_hench` copies the unique switch, the throttles (not the limits mode) and the
batch defaults. The executor's failures keep their error (`Verdict::NoRetry` carries the class
and the message), so a `retry: false` batch member's failure summary names its error.

## 9. Metrics, dashboard, server

- **Metrics** (Sidekiq 7+ `metrics/`): per-minute per-class `p` (processed), `f` (failed),
  `ms` (total ms) in hash `j|YYYYMMDD|H:MM` (`keys::metrics_minute`, 8 h TTL; a ten-minute
  rollup `j|YYYYMMDD|H:M` with the tens digit of the minute, 3 days), execution-time
  histogram buckets (`h|<class>-<day>-<hour>:<minute>`, 8 h), and deploy marks
  (`YYYYMMDD-marks`, `deploylock-<label>`). Query API for top jobs and per-job series: §9b.
- **Dashboard** (`hench-web`): the pages, routes and actions of Sidekiq Web, with our own
  askama templates, CSS and JS (distinct look, dark mode, no external resources). Details below.
- **Server** (`hench-server`, binary `hench`): standalone dashboard, JSON API and
  sidekiqmon-style CLI. Details below.

### Dashboard (`hench-web`)

- **API.** `HenchWeb::new(impl Into<Api>)` (an `Api`, `RedisPool`, `Client` or `&Hench`;
  `&Hench` gives the dashboard the application's client middleware, morgue limits and death
  handlers) with builder methods `read_only(bool)`, `poll_interval(Duration)` (default 5 s,
  rounded to whole seconds and clamped to 1–60 s), `title(..)` (default "Hench"), `app_url(..)` ("Back to app"; absolute
  paths and `http(s)://` URLs only), `tab(name, path)` (custom tabs: a relative path is relative
  to the mount, so the host serves it with `routes`; absolute paths and URLs are used as is;
  `javascript:`-style links are dropped with a warning),
  `page_size(n)` (default 25, 1–999), `metrics(bool)` (the Metrics tab and pages, on by
  default; off, they answer 404), `allowed_hosts(AllowedHosts)` (the host names served, see
  Security; default `AllowedHosts::local()`), `routes(Router)` and `job_info(F)` (see
  Extensions); getters `api()`, `is_read_only()`, `tabs()`, `shows_metrics()`, `hosts()`.
  `hench_web::router(web)` / `web.into_router()` return an `axum::Router` with its state applied.
  Authentication is the host's job (any tower layer around the router; the crate docs show an
  `axum::middleware::from_fn` example); `hench web` adds Basic authentication.
- **Extensions** (Sidekiq's `register_extension` and `custom_job_info_rows`).
  `HenchWeb::routes(Router)`: routes of the host, relative to the mount, merged into the
  dashboard's router before its guard, so they get the host check, the read-only and
  cross-site refusals of unsafe methods and the security headers like the built-in pages
  (routes added to the router `router()` returns get none of that; overlapping paths or a
  second fallback panic when the router is built, as `Router::merge` does). Their handlers
  extract `hench_web::Page` (the guard puts the dashboard in the request's extensions; outside
  it, `Page` and `Form` answer 500): `url(path)`, `prefix()`, `path()`, `csrf_token()`,
  `csrf_field()`, `is_read_only()`, `api()`, and `render(title, content_html)`, a 200 page in
  the layout (navigation with the custom tab highlighted, totals unless Redis fails, notice,
  footer, assets; the content is the host's HTML, the only unescaped output, `escape_html`
  helps; the CSP still forbids inline scripts and styles). `hench_web::Form` reads a form
  like the built-in actions (`application/x-www-form-urlencoded` else 415, the `_csrf` field
  must match a token cookie else the 403 page). `HenchWeb::job_info(Fn(&JobRecord) ->
  Vec<JobInfoRow>)` adds rows after the built-in ones on the three job pages
  (`JobInfoRow::text(label, text)`, `JobInfoRow::link(label, text, href)`: absolute paths and
  `http(s)://` URLs only, other links shown as text); a callback that panics loses its rows,
  not the page. Sidekiq's extensions also bring locales, views and assets of their own; here
  pages are HTML strings and assets are routes of the extension.
- **Mounting.** The router works at the root or nested anywhere (`.nest_service("/hench", ..)`,
  several levels, or under a path with parameters such as `/tenants/{tenant}/jobs`): each request
  computes its mount prefix from axum's `OriginalUri` minus the path left after axum stripped
  the nesting (repeated slashes collapse and backslashes are percent-encoded, so a prefix built
  from request values never reads as `//host`). Every link, form action, asset URL, redirect
  and cookie `Path` uses it; the home page is `<prefix>` itself (`/` at the root). Path
  parameters are extracted by name, so a parameterized mount does not break them. The docs
  mount with `nest_service`, which routes both `<prefix>` and `<prefix>/` to the home page (a
  proxy's `location /hench/` redirects `/hench` there); axum's `nest` does not route
  `<prefix>/`, and refuses a router with a fallback under a path whose *first* segment is a
  parameter.
- **Routes** (relative to the mount): `GET /` (dashboard; `?days=1..180`, default 30, else
  400) and `HEAD /` (cheap health check: `LLEN queue:default`, 200, or 503 when Redis fails or
  takes more than a second); `GET /busy`
  (`?only=jobs|processes`, else 400; running jobs paged, 100 per page) and `POST /busy`
  (`quiet` or `stop`, with `identity` for one process, else every process that is not
  embedded); `GET /queues`; `GET /queues/{name}` (`?direction=asc|desc`, `?substr=`) and
  `POST /queues/{name}` (`pause`, `unpause` or `delete`; `return_to=queue` comes back to the
  queue's page); `POST /queues/{name}/delete` (`key` = `"{index}-{sha1}"`: the job's index in
  the queue when the page was rendered, 0 the newest, and the SHA-1 of its JSON; the server
  looks for that JSON at the index, then from 10 jobs before it onwards, 10 000 jobs at most —
  pushes move jobs back — and removes the exact value with `LREM`; not found: "gone". Rows stay
  small whatever the job's size, and browsers' CRLF normalization of form fields cannot break
  the match);
  `GET|POST /retries`, `/scheduled`, `/morgue` (lists; POST = the pressed action on the selected
  `key`s, at most 1000); `GET|POST /<set>/{key}` (one job); `POST /retries/all/{retry|delete|kill}`,
  `/scheduled/all/{add_to_queue|delete}`, `/morgue/all/{retry|delete}`; `GET /stats`,
  `GET /stats/queues`, `GET /dashboard/stats` (redirects to `/stats`); `GET /metrics`
  (`?period=1h|2h|4h|8h|24h|48h|72h`, default `1h`; `?sort=count|failures|time|average`,
  default `time`; `?substr=` class filter; other values 400) and `GET /metrics/{class}`
  (`?period=`; `24h` and longer show `8h`); `GET /assets/{file}`; while their switches are on
  (the default; see "The pro features in the dashboard" below), the feature tabs' `GET|POST
  /periodic`, `GET|POST /periodic/{name}`, `GET /batches`, `GET|POST /batches/{bid}`, `GET|POST
  /limits`, `GET|POST /limits/{kind}/{id}`, `GET|POST /locks`, `POST /locks/all/release` and
  `GET|POST /locks/{digest}` (§9g–§9j); anything else is a 404 page. The crate documentation
  of `hench-web` lists every route and the switches that remove the feature tabs (release
  review: it predated the tabs, so a proxy allowlist or a stricter rule for state-changing
  routes built from it missed nine routes). Entry keys are `"{score}-{jid}"` (`SortedEntry::key`,
  `api::parse_key`); a key with an empty jid addresses the jid-less entries at that score (such
  as invalid JSON), which can therefore be deleted (Sidekiq cannot).
- **Pages.** Every page but the error pages shows the totals strip (processed, failed, busy,
  enqueued, retries, scheduled, dead). Dashboard: a live SVG chart of the jobs processed and
  failed between two polls of `/stats` (60 points; interval slider 1–60 s, the range the
  server accepts, so it always shows the interval in use, remembered in `localStorage`; paused
  while the tab is hidden; a status line with the latest numbers for screen readers that is
  not a live region — routine polls are not announced — and a polite live region that
  announces a refresh starting to fail, once per failure), the daily
  history chart (1 week / 1 month / 3 months / 6 months, oldest first, with a table view),
  and the Redis panel (store name — Redis, Valkey or Dragonfly — version, uptime, connections,
  used and peak memory; "not available" when `INFO` is refused). Busy: status figures
  (processes, threads, busy, utilization, RSS), processes in natural host-name order
  (identity, host:pid, tag, labels, embedded/quiet chips, a version line when it differs from
  the dashboard's, started, last heartbeat, RSS, threads, busy, queues per capsule — the ordered
  queue list for single-capsule processes), Stop per process and Quiet for those not yet quiet, Quiet
  all/Stop all (shown when some process is not embedded), running jobs (process, slot, JID,
  queue, class and tags, arguments, started). Queues: by name (every queue of
  `Stats::queue_summaries`, so paused queues even when empty), with size, latency (and when the
  oldest job was enqueued, past a minute), paused chip, Pause/Resume/Delete; names with control
  characters are shown with them spelled out (`\n`, `\u{1b}`) and can be opened and managed like
  any other. Queue: newest or oldest first with positions (the place in line: 1 is the next to
  run, also in filtered lists), pager, Delete per job. Retries/scheduled/morgue: soonest first
  (dead jobs: latest first), checkboxes (select all, shift-click ranges, buttons disabled until
  something is selected), bulk actions, and all-jobs actions whose confirmations name the whole
  set and its size (`Delete all 340 dead jobs?`) — not offered in filtered lists, where they
  would not act on the matches only (select the matches instead). A page past the end of a list
  that is not empty (its last rows were just removed) shows the last page instead of an empty
  one. Detail pages show every
  field (full arguments pretty-printed, extras, retry and error fields, decoded backtrace, raw
  JSON). Lists cut arguments at 120 characters (expandable to 10 000); detail pages show them
  whole. Times are relative ("3 minutes ago", the wording of `Intl.RelativeTimeFormat`) with
  the absolute UTC time in `title`, refreshed every 15 s in the browser and corrected for the
  server's clock; times beyond the calendar (about ±262 000 years: an `at` sent in
  microseconds, an absurd retry delay) show their relative wording with `Unix time N` as the
  absolute time and no `datetime` attribute, instead of failing the page. List pages have a
  "Live" switch (remembered) that re-fetches the page every poll interval and swaps the
  content, skipped while the viewer has a selection, an open detail or the keyboard focus on
  anything in the content (a field, a link, a button: the swap would destroy it).
- **Metrics pages** (`hench::metrics::Query`). `/metrics`: the period selector (segmented
  links) and the class filter (case-insensitive substring) in one row; a summary (`N job
  classes ran in the last hour.`); a line chart of the execution time (seconds) of each class
  per bucket (minutes up to 8 hours, ten minutes beyond) over the whole period, with deploy
  marks (a vertical line and a flag; the tooltip names the deploy); the table of the first 25
  classes in the selected order (executions, failures, total seconds, average milliseconds per
  execution that did not fail; column headers are the sort links, largest first, ties by
  name; a note when more classes ran), whose checkboxes choose the charted classes (the first
  five when the page opens, eight at most: one categorical color slot each, kept by a class
  while others come and go); the chart's table view (charted classes per bucket); the
  period's bounds; the deploy list. `/metrics/{class}`: breadcrumb, minute periods (1h–8h:
  histograms are only kept per minute), figures (executions, failures, failure rate, total
  time, average, median and 95th percentile as bucket bounds `< 45 ms`), executions and
  failures per minute (line chart with deploy marks), the execution-time distribution
  (columns over the used buckets plus one on each side, the tallest labelled), execution
  times per minute (a heat map: minutes × duration buckets, five sequential steps, linear up
  to five executions per cell and logarithmic beyond, with a key), table views (the
  distribution; per minute: executions, failures, seconds, median, 95th percentile), the
  deploy list; `No execution of “X” was recorded in the last hour.` when the class did not
  run. Neither page has the "Live" switch (Sidekiq's metrics pages are not pollable either);
  pages of the last hour re-fetch their content every 60 s in place (keeping the charted
  classes), skipped while the tab is hidden or the viewer is busy with the content (a table
  view open, or the keyboard focus on something in it: a field, a link, a chart explored with
  the arrow keys). The overview's table view follows the ticked classes (`metrics.js` rebuilds
  it from the chart's data; rows are the buckets where a charted class took time).
- **Iterable jobs.** The busy page (running jobs) and the retries list show the progress of
  iterable jobs under the time or error (`cursor {"offset":500} · 7 executions · 12.346 s`,
  cursors cut at 100 characters, a `cancelled` chip), from one
  `IterationState::fetch_many` per page; the job pages of the three sets have an Iteration
  row with the whole cursor (`IterationState::fetch`), as in Sidekiq.
- **Filtering.** `?substr=` (at most 256 characters, else 400), the same on queues and sets:
  literal text the job's JSON must contain, `*` standing for any run of characters (`?`, `[`,
  `]` are literal: there is no other wildcard). The dashboard reads the list in the order it
  shows it, 1 000 jobs per round trip (`Queue::page`/`JobSet::page`), and matches in process —
  never a Redis glob, whose `?`/`[..]` patterns cost Redis O(length × pattern) per entry — over
  at most 50 000 jobs, keeping at most 500 matches: so the matches shown are the first ones in
  the list's order. The summary says what was searched: `3 jobs matching “x”.`, `Showing the
  first 500 jobs matching “x”.`, or `N jobs matching “x” in the first 50,000 of 120,000 jobs;
  the others were not searched.` (with `None of the jobs searched match this filter.` instead
  of `No jobs match this filter.`).
- **Actions** answer `303 See Other` back to the list (with the referring page's `page`,
  `count`, `direction`, `substr` and `only` when the form was sent from that same list on the
  same host; nothing else from the referer is used) and leave a one-shot notice: the
  `hench_flash` cookie (60 s, `HttpOnly`, `SameSite=Strict`) holds a code
  (`kind.count.failed.subject`, never free text); the next page shows it and clears it. Bulk
  actions count what was done and what failed (logged); missing entries are skipped.
- **Security.** (0) Host names (DNS rebinding: a page on a host name its author re-points at
  the dashboard's address passes every same-origin check, since its `Origin` is the `Host` it
  sends, and authentication it can borrow — none, the client's address, a header a script can
  set — does not stop it): `AllowedHosts` refuses, before anything else, every request whose
  `Host` headers or URI authority (HTTP/2, absolute URIs) name a host that is not `localhost`,
  `*.localhost`, an IP address or one of its names (exact, `.example.com` for a domain and its
  subdomains; case-insensitive, any port; a request with neither, such as HTTP/1.0, is
  served): plain-text `403` (no page, no cookie; the text names `HenchWeb::allowed_hosts`, or
  what `AllowedHosts::refusal` says). `AllowedHosts::new(names)` (`*` or `AllowedHosts::any()`:
  every name, for dashboards behind authentication such a page cannot use: cookies, HTTP
  Basic), `local()` (the default), `parse_name`, `allows(host)`, `allows_request(&request)`;
  `hench_web::require_host` is the same check as an `axum::middleware::from_fn_with_state`
  function for routes next to the dashboard. Sidekiq has no such check (Rails' host
  authorization covers development only).
  (1) CSRF double-submit: a random token (32 bytes from the OS, no fallback) in the
  `hench_csrf` cookie (`HttpOnly`, `SameSite=Lax`, `Path=<mount>`, `Secure` when the request
  came over HTTPS, `X-Forwarded-Proto`/`Forwarded` included) and in every form's `_csrf` field;
  a POST must carry a field equal (constant time) to one of its `hench_csrf` cookies, else 403.
  Lax, not Strict: a link from another site (chat, ticket, SSO redirect) must carry the cookie,
  or the dashboard issues a new token whose cookie replaces the one the forms of open pages
  carry; Lax cookies never reach other sites' POSTs, which (2) refuses anyway. Refusals of
  cross-site or read-only POSTs set no new token cookie.
  (2) A middleware refuses every unsafe method from another site: `Sec-Fetch-Site` must be
  `same-origin` when present, else `Origin` (when present) must name the `Host` (or
  `X-Forwarded-Host`); `Origin: null` is refused. (3) Read-only mode removes every form and
  refuses every unsafe method with 403. (4) Every response gets `Content-Security-Policy:
  default-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'self'; object-src
  'none'` (no inline scripts or styles anywhere: data reaches scripts through `data-*`
  attributes), `X-Content-Type-Options: nosniff`, `Referrer-Policy: same-origin`,
  `X-Frame-Options: SAMEORIGIN`, `Cross-Origin-Opener-Policy: same-origin`, and
  `Cache-Control: no-store` unless it sets its own. (5) All output goes through askama's HTML
  escaping (nothing is marked safe, except the content a host page gives `Page::render`).
  (6) Parameters are bounded: page 1–100 000 (lenient), page size 1–999 (else the default),
  filters 256 characters, enumerations strict, forms must be
  `application/x-www-form-urlencoded` (else 415). (7) A form field that carries a value read
  from Redis back to the server (a lock's holder, a batch member's jid, a limit's identity, a
  set entry's `score-jid` key, a process identity) holds it with `%` and the ASCII control
  characters percent-encoded (`form::field`; non-ASCII text too), decoded on the way in
  (`PostForm::value`, `PostForm::values`): an HTML parser turns NUL into U+FFFD and CR into
  LF, and a browser submitting a form turns a lone CR or LF into CRLF, so a raw jid or key
  with a line break never came back as it was (round 3, web-api-security-5; queue rows use
  `"{index}-{sha1}"` keys). The lock, Limits, batch and Busy pages show such values with their
  control characters spelled out.
- **Assets.** `hench.css`, `hench.js`, `theme.js` (applies the stored theme before the first
  paint), `dashboard.js`, `metrics.js` and `icon.svg` are embedded with `include_bytes!` and served as
  `<name>.<FNV-1a 64 hex>.<ext>` with `Cache-Control: public, max-age=31536000, immutable`;
  other names are 404. Light and dark themes are CSS custom properties: the system preference,
  overridden by a three-state toggle (automatic, light, dark) stored in `localStorage`. The
  chart series use the first two slots of a validated categorical palette (blue processed,
  orange failed; colorblind-safe in both themes); charts have a crosshair tooltip (pointer and
  arrow keys), an accessible summary and a legend. The metrics charts use all eight slots of
  that palette (`--cat-1` … `--cat-8`, in its fixed order; validated against both themes'
  surfaces) and a five-step one-hue ramp for the heat map (`--heat-1` … `--heat-5`, its own
  steps per theme: darker means more in the light theme, brighter in the dark one); columns
  and heat cells have per-mark tooltips (pointer and arrow keys).
- **Strings.** Every text of the pages is in `strings.rs` (English only; no locale switching,
  no profiles page).
- **Deviations from Sidekiq Web.** `POST /queues/{name}` needs an explicit action (Sidekiq
  deletes the queue when no other action is named); unknown queues and entries are 404 pages
  (Sidekiq: 200 for any well-formed queue name, a redirect for missing entries); queue pages
  accept any queue name, control characters included (Sidekiq only `[a-z_:.-0-9]`; escaping
  makes the restriction unnecessary);
  pause/resume is built in (Sidekiq Pro); the retry page also offers Kill; filters also exist on
  queue pages; jid-less entries can be deleted by key; JSON totals are under `"hench"`
  (Sidekiq: `"sidekiq"`) plus `server_time`; cross-site POSTs are refused by fetch metadata
  *and* a token (Sidekiq 8 relies on `Sec-Fetch-Site` alone); host names are checked (Sidekiq
  serves any). Metrics: the table sorts by any column (Sidekiq: total time only) and shows 25
  classes (Sidekiq: 20); an unknown `period` or
  `sort` is a 400 (Sidekiq falls back to `1h`); the per-job page adds executions per minute,
  quantile figures and table views, and draws the time × duration counts as a heat map
  (Sidekiq: a bubble chart); the last hour refreshes in place instead of with a `Refresh`
  header. Filters are literal text with `*` as the only wildcard, matched by the dashboard in
  the list's order over at most 50 000 jobs (Sidekiq passes the filter to `ZSCAN MATCH` as a
  glob and scans the whole set); queue rows delete by `"{index}-{sha1}"` keys (Sidekiq posts
  the job's whole JSON); the "Every job" actions are not offered in filtered lists (Sidekiq
  shows Delete All under search results, and deletes everything).
- **API helpers added for the dashboard** (additive, in `hench::api`): `Queue::exists` (known,
  non-empty or paused, one round trip) and `JobSet::add_all_to_queue` (`retry_all` without the
  retry-count decrement, Sidekiq's "add all to queue"; tests in `tests/api_dashboard.rs`).

### Server (`hench-server`, binary `hench`)

- **CLI** (clap): `--redis-url URL` (global; env `HENCH_REDIS_URL`; else the worker rules:
  `REDIS_PROVIDER`, `REDIS_URL`, `redis://127.0.0.1:6379`). One-shot commands connect with a
  single reconnection attempt so an unreachable Redis fails fast with "cannot reach Redis at
  <redacted URL>"; errors print `hench: <error>` and exit 1 (usage errors: clap, exit 2).
  Logs go to standard error (`HENCH_LOG`, else `RUST_LOG`; default `info` for `web`, `warn`
  otherwise); command output goes to standard output.
- `hench web [--bind 127.0.0.1:7420] [--username U --password P] [--read-only] [--prefix
  /path] [--api] [--allowed-host HOST].. [--title T] [--poll-interval 5s] [--max-connections
  512]` (env `HENCH_WEB_BIND`, `HENCH_WEB_USERNAME`, `HENCH_WEB_PASSWORD`,
  `HENCH_WEB_READ_ONLY`, `HENCH_WEB_PREFIX`, `HENCH_WEB_API`, `HENCH_WEB_ALLOWED_HOSTS`
  comma-separated, `HENCH_WEB_MAX_CONNECTIONS`): the dashboard at the prefix (mounted with
  `nest_service`, so `<prefix>/` is the home page too; `/` redirects to it; `/healthz` is
  refused as a prefix), the JSON API at `<prefix>/api/v1` with `--api`, `GET /healthz` (also
  `<prefix>/healthz`; unauthenticated, whatever the host name; `PING` within 1 s: 200 `ok`,
  else 503). Basic authentication covers the dashboard and the API (401 with a
  `WWW-Authenticate` challenge): credentials are reduced to 128-bit keyed (SipHash, random keys
  per process) digests and compared in constant time, never stored in the clear. The server
  refuses to start with `--api` on a non-loopback address without credentials, and warns when
  the dashboard alone is exposed that way; a username needs a password. **Host names** (DNS
  rebinding: a page on a host name its author re-points at `127.0.0.1` passes every same-origin
  check, since its `Origin` is the `Host` it sends): without credentials the dashboard and the
  API answer only requests whose `Host` (else the URI's authority; none: HTTP/1.0, served) is
  `localhost`, `*.localhost` or an IP address — names nobody can re-point — or an
  `--allowed-host` value (an exact name, `.example.com` for a domain and its subdomains, `*` for
  any; case-insensitive, any port); others get a plain-text 403 naming the option. With
  credentials every name is served unless `--allowed-host` lists some (a rebound page has no
  credentials for its own origin). The check is the dashboard's (`hench_web::AllowedHosts` and
  `require_host`, applied outside the credentials, so a refused name gets no challenge). Redis
  is contacted lazily: the server listens at once (its first `PING` runs in the background), and
  its connection makes one reconnection attempt per command (like the one-shot commands), so
  pages and health checks fail within moments while Redis is down instead of waiting through the
  workers' six attempts. Every request is logged (method, path, status, milliseconds; assets and
  health checks at debug).
- **Connections** (`hench_server::connections`; `axum::serve` gives hyper no timer, so no
  request-head timeout, and limits neither connections nor the shutdown: a thousand clients
  sending half a request head each exhausted the file descriptors, and one of them kept a
  stopping server alive). The server runs its own accept loop on hyper's HTTP/1 connections,
  every phase bounded (`Limits`, `--max-connections` sets the first): at most 512 connections
  (the next one is accepted when one closes; the kernel's backlog holds the clients, a warning
  is logged at most once a minute); a request head must arrive within 10 s of the first read
  (an idle keep-alive connection is closed after the same time; heads over 64 KiB get 431);
  a request body within 30 s of passing the host and credential checks, which come first so
  that a refused request's body is never read (408, and 413 over 2 MiB, axum's limit,
  declared or not; the connection is closed after them); a response the client stops reading
  fails after 30 s without progress. The first SIGINT/SIGTERM stops accepting and closes idle
  connections; requests in flight get 10 s to finish, then what is left is closed; a second
  signal closes everything at once (`connections::shutdown_signals` delivers every signal).
  An internet-facing deployment still belongs behind a reverse proxy (rate limits, TLS).
- **JSON API** (`--api`): `GET /stats` (`processed`, `failed`, `busy`, `processes`, `enqueued`,
  `scheduled`, `retries`, `dead`, `default_latency`), `GET /queues` (`[{name, size, latency,
  paused}]` by name), `GET /processes` (identity, hostname, pid, tag, labels, started_at, beat,
  concurrency, busy, quiet, embedded, rss_kb, version, `queues`: the names of the queues the
  process works on, each once, in its configured order (`api::Process::queues`), `capsules`:
  each capsule by name, `{concurrency, mode, weights: {queue: weight}}`, in the order the
  heartbeat lists them, a capsule or queue listed twice once with its last value, and
  `features`; release review: `queues` held one comma-joined display string per capsule, so
  a script could not test membership and the weights were lost), `POST /jobs`
  (`{class, args?, queue?, at? (epoch seconds) | in? (seconds), retry?, backtrace?, dead?,
  tags?, retry_for?, retry_queue?, jid?, custom? (extra keys, reserved names refused)}`, unknown
  fields refused; pushed with `hench::Client`: 201 `{"jid"}`; 400 for invalid jobs, explicitly
  empty `queue`/`jid`, `at` with `in`, a time at or after the year 10000 (`at`, or now + `in`:
  almost always milliseconds or microseconds sent for seconds), or control characters in
  `class`, `queue`, `retry_queue`, `jid` or `tags`; 415/422 for non-JSON or mistyped bodies; 403 when
  read-only or sent by a browser from another site; 503 when Redis fails). Errors are
  `{"error": "..."}`, every one: a path parameter that is not UTF-8 once percent-decoded is a
  400, a path the API does not serve a 404 and a method a route does not take a 405 (with
  `Allow`), all in JSON (the API is mounted with `nest_service` and has its own fallbacks,
  so no path under `/api/v1` reaches the dashboard's HTML pages). The server knows no job
  types: without `queue` a job goes to `default` with the default options, whatever its job
  type declares (Sidekiq's raw `Client.push`).
- `hench stats [all|version|overview|processes|queues|metrics|periodic|batches|limits|locks]
  [--json]` (the features' sections below), `hench processes [--json]`,
  `hench queues [--json]`: sidekiqmon's report — `hench <version>` and the UTC time; the
  overview (`Processed`, `Failed`, `Busy`, `Enqueued`, `Retries`, `Scheduled`, `Dead`,
  right-aligned labels, thousands separators); each process (`identity [tag] [labels]
  [embedded] [quiet]`, `Started: <time> (<just now | a minute ago | N minutes ago | an hour
  ago | N hours ago>)`, `Threads: N (M busy)`, `Queues:` per capsule joined with `; ` and
  wrapped at 80 columns, `Version:` when it differs); the queues table (`NAME SIZE LATENCY`,
  name left-aligned, numbers right-aligned, two-space gutters, paused queues marked); the
  metrics of the last hour (`---- Metrics (last hour) ----`: the ten classes with the most
  execution time, `CLASS EXECUTIONS FAILED TIME (S) AVG (MS)`, or `No job ran in the last
  hour.`). `--json` prints the same data (`metrics`: `[{class, executions, failed, seconds,
  average_ms}]`; `processes`: the objects of the JSON API's `GET /processes`, whose `queues`
  are names: the text's per-capsule groups exist only in the text). Names from Redis (queues,
  classes, identities, tags, labels, versions) are
  printed with their control characters spelled out (`\n`, `\u{1b}`), so that no job can send
  escape sequences or extra lines to the operator's terminal; process times the calendar
  cannot show are `unknown` (`null` in JSON).
- `hench quiet|stop <identity|all>`: sends `TSTP`/`TERM` through `api::Process`; `all` skips
  embedded processes (and says how many it left alone), an unknown identity is an error.
- `hench enqueue <class> [--args JSON] [--queue Q] [--in 5m | --at RFC3339] [--retry
  true|false|N] [--json]`: `--args` is a JSON array (another JSON value is the only argument);
  durations accept `ms`, `s`, `m`, `h`, `d`, combinations (`1h30m`) and bare seconds; times at
  or after the year 10000 are refused; prints the jid (or `{"jid", "at"}`). A raw push by class
  name: `hench` has no registry of the application's job types, so their declared queue and
  options do not apply (queue `default`, the default retries) unless `--queue`/`--retry` give
  them; without `--queue` it warns on standard error. Its help says so.
- The binary links the `tls` feature by default (`rediss://`).

### The pro features in the dashboard, the server and the metrics

- **Metrics**: `Outcome` is `#[non_exhaustive]` and gains `OverLimit` (`o`), `Throttled` (`t`,
  not an execution) and `Skipped` (`s`); `Counts` gains `over_limit`, `throttled`, `skipped`
  and `timed()`, and `average_ms` divides by the timed executions (when the feature counters
  exceed the executions that did not fail, by those). A flush writes only the counters that
  moved, so jobs without features write exactly what they did.
- **Dashboard**: tabs Dashboard, Busy, Queues, Retries, Scheduled, Dead, Periodic, Batches,
  Limits, Locks, Metrics, then custom tabs; switches `HenchWeb::{periodic, batches, limits,
  locks}(bool)` (default on; off, the tab disappears and its routes answer 404, while job pages
  keep the feature rows as text). Each feature's page module
  (`pages/{periodic,batches,limits,locks}.rs`) declares `routes()`,
  `job_rows(ctx, record)` (the rows of a job's detail page in the fixed order Batch, Periodic,
  Unique, Throttle, Limited, before the host's `job_info` rows; the fields they show leave the
  extras) and `in_use(ctx)` (the Busy page shows "lacks: …" on a process whose heartbeat's
  `features` map misses a feature in use). Notices gain `PeriodicEnqueued`, `PeriodicPaused`,
  `PeriodicResumed`, `PeriodicDeleted`, `BatchCancelled`, `BatchDeleted`, `MemberSkipped`,
  `CallbacksDispatched`, `LimitsReset`, `LineCleared`, `LocksReleased` (texts in
  `strings/<feature>.rs`); stylesheets `periodic.css`, `batches.css`, `limits.css`,
  `locks.css` are served with the other assets.
- **Server**: `hench periodic|batches|limits|locks [subcommand..]` (argument types in
  `commands/<feature>.rs`), `hench stats <periodic|batches|limits|locks>` (and each section in
  `hench stats all` when the feature has data), and JSON routes `/periodic`, `/batches`,
  `/limits`, `/locks` (`json_api/<feature>.rs`). `hench enqueue` takes `--unique-for`,
  `--unique-until`, `--unique-key`, `--unique-across-queues`, `--on-conflict` and `--throttle
  NAME[:KEY]` (a dropped duplicate prints `duplicate of <holder>`, or `{"jid": null,
  "duplicate_of"}` with `--json`, and exits 0; `--on-conflict error` makes it a failure), and
  `POST /jobs` the fields `unique`, `throttle` and `throttle_key` (`201` pushed or replaced,
  `200` with `duplicate_of` for a dropped duplicate, `409` for `on_conflict: "error"`).
  `unique` is read strictly at the API (round 3, web-api-security-4): `false`, `null` or an
  object of `for`, `until`, `on_conflict`, `across_queues`, `key` and `digest` only, `until`
  one of `success`, `start`, `expired` (`422` otherwise); a stored job's `unique` stays
  lenient (a policy a newer version wrote reads as `success`), which at the API turned typos
  into other lock semantics (`"until": "expire"` held until success). The commands print the
  values they read from Redis (jids, parents, classes, names) with control characters
  spelled out, and times relative to now in both directions (`5 minutes ago`, `in 29 days`).
  `hench periodic next` answers without connecting to Redis.

## 9a. Module ownership (spine and lanes)

| Module | Owner | Contents |
|---|---|---|
| `error`, `keys`, `payload`, `options`, `redis`, `job`, `client`, `middleware`, `config`, `test_util`, `lib` | spine | everything above; lanes extend them only when unavoidable |
| `fetch`, `processor`, `retry`, `scheduled`, `launcher` (incl. `impl Hench { start, run, run_until }`), `manager`, `stats`, `job_logger` | engine lane | worker runtime (§5) |
| `api/`, `testing`, `crates/hench-macros` | api lane | §7, §8, macro (§3) |
| `metrics/`, `iterable/`, `cli`, `systemd` | features lane | §9b–§9f (plus their hooks in the processor, launcher, config, client, testing and job logger) |
| `crates/hench-web`, `crates/hench-server` (+ additive `api` helpers: `Queue::exists`, `JobSet::add_all_to_queue`) | web lane | §9 dashboard and server |
| `hench-web` metrics pages (`pages/metrics.rs`, `metrics.js`), iteration progress on the busy/retries/job pages, `hench stats metrics` | integration | §9 (glue between the features and web lanes) |

The engine and api lanes are merged into `implement-hench`. Cross-lane glue added by the
integration: `middleware::execute_job` (processor and testing modes), `api::cleanup_processes`
(scheduler and `ProcessSet::cleanup`), `api::Queue::pause`/`unpause` (the `paused` set the
fetchers read), `JobError::KILLED`, and `tests/api_engine.rs`, which drives the API against a
running engine. The features and web lanes are merged as well; their glue is the integration
row above (the dashboard reads `hench::metrics` and `hench::iterable::IterationState`, the
`hench` command the metrics query). The examples of the README and of the guide are doc tests
of `hench-server` (`ReadmeDoctests` and `guide_doctests`, included from the files its package
carries, §1), the crate that depends on everything they use.

Crate-internal helpers for the lanes: `flush::{Flusher, Batch}` (counter flushes, §5, used by
the engine's `stats` and the metrics tracker), `job::catch_panic(fut)`, `Client::normalize`,
`Client::raw_push`, `payload::{truncate_error_message, compress_backtrace,
system_time_to_secs, ..}`, `RetryOption::max_retries`, `BacktraceOption::select`,
`CapsuleConfig::expanded_queues`, `keys::*` (including `PROCESS_TTL`, `STATS_TTL`,
`identity(host, pid, nonce)`, `SIGNAL_QUIET`/`SIGNAL_STOP`), `random_hex(6)` for the identity
nonce.

**The pro features** (`docs/design/pro-features.md` §6.2) are built in three phases: phase 0
(the integrator) changed every shared file once (types, hooks, pipelines, switches); in phase 1
two lanes work on disjoint files; phase 2 integrates. Lane A owns `crates/hench/src/unique/**`,
`periodic/**`, `api/unique.rs`, `api/periodic.rs`, `tests/unique*.rs`, `tests/periodic*.rs`,
`examples/periodic.rs`, `hench-macros/src/unique.rs` (and `tests/ui/unique_*`), the Locks and
Periodic pages, templates, `strings/{locks,periodic}.rs`, `assets/{locks,periodic}.css` and
tests of `hench-web`, and `commands/{locks,periodic}.rs`, `json_api/{locks,periodic}.rs` of
`hench-server`. Lane B owns `batch/**`, `limit/**`, `iterable/**`, `api/batch.rs`,
`api/limits.rs`, `tests/batch*.rs`, `tests/limit*.rs`, `tests/throttle*.rs`,
`hench-macros/src/throttle.rs` (and `tests/ui/throttle_*`), and the same web and server files
for batches and limits. Everything else (every shared file, `canonical`, `lua`, `pro`,
`api/events.rs`, `tests/pro_features.rs`, the soak harness, CI, shared docs) is the
integrator's. The frozen interfaces (the hook signatures of `docs/design/pro-features.md` §4.3,
the unique `lib.lua` functions, the payload and option types, the key helpers, the notice kinds)
change only through the integrator. Each lane writes its section (§9g/§9h or §9i/§9j) under the
headings below and its STATUS lines under its section.

## 9b. Metrics (`hench::metrics`)

- **Tracking** (`metrics::Tracker`, one per process unless `.metrics(false)`): the processor
  calls `track(class, elapsed, Outcome)` once per execution (one mutex and one hash lookup; the
  class name is allocated once per flush interval). `Success` and `Interrupted` add 1 to `p`,
  the elapsed whole milliseconds to `ms` and the duration to the class's histogram; `Failure`
  adds 1 to `p` and `f` (failures are not timed). The heartbeat flushes it (and the shutdown,
  after the stats): one batch, applied atomically by the counter-flush script (§5 "Counter
  flushes"), with, per timed class, `BITFIELD h|<class>-<day>-<H>:<M> OVERFLOW SAT INCRBY u16
  #<bucket> <n>…` + `EXPIRE 8h`, then for the minute hash (`EXPIRE 8h`) and the ten-minute
  hash (`EXPIRE 3d`) one `HINCRBY <class>|<metric>` per non-zero metric (`ms` only for timed
  classes). Times are UTC and the bucket is the flush's minute (a job is attributed to the
  minute of the beat after it ran, like Sidekiq). A batch Redis does not confirm is sent again
  as it was by the next flushes (for its first attempt's minute) and counted once even if an
  attempt whose reply was lost ran (Sidekiq drops it instead); `flush_at` returns the counters
  of every batch confirmed by the call, and `is_empty` is false while a batch is unconfirmed.
- **Histogram scheme** (`metrics::Histogram`, `BUCKETS` = 26): an execution of `ms`
  milliseconds is counted in the first bucket whose exclusive upper bound exceeds `ms`; the
  bounds grow about ×1.5: 20, 30, 45, 65, 100, 150, 225, 335, 500, 750 ms, 1.1, 1.7, 2.5, 3.8,
  5.75, 8.5, 13, 20, 30, 45, 65, 100, 150, 225, 335 s, then one unbounded bucket. Counters are
  `u16`, saturating in Redis (`OVERFLOW SAT`).
- **Key differences from Sidekiq**: the hash keys use a four-digit year (`j|20250214|8:43`,
  Sidekiq writes `j|250214|8:43`); field names, histogram key and bucket layout are Sidekiq's.
  Class names may contain `|` (fields are split at the last one).
- **Query** (`metrics::Query::new(&redis)`, `.at(time)` for tests): `top_jobs(period,
  class_filter)` (case-insensitive substring filter) pipelines one `HGETALL` per bucket;
  `for_job(class, period)` one `HMGET <class>|ms <class>|p <class>|f` per bucket plus, for
  minute periods, one `BITFIELD_RO … GET u16 #0..#25` per minute. `Period::{Minutes(n),
  Hours(n)}` (presets `1h`, `2h`, `4h`, `8h` in minutes; `24h`, `48h`, `72h` in ten-minute
  buckets; capped at 480 minutes and 72 hours, like Sidekiq). The window holds `n` buckets
  ending with the one containing the query time; `starts_at` = time − period. The result
  (`QueryResult { granularity, starts_at, ends_at, jobs: BTreeMap<class, JobMetrics>, marks }`)
  has, per class, `totals` and sparse per-bucket `series` of `Counts { processed, failed, ms }`
  (`succeeded()`, `seconds()`, `average_ms()` = ms per non-failed execution) and per-minute
  `histograms`; `buckets()` lists every bucket of the window (the chart axis) and
  `sorted(SortBy::{TotalTime, Processed, Failed, AverageTime})` orders classes (largest
  first, ties by name). `for_job` always returns an entry for the class.
- **Deploy marks** (`metrics::Deploy::new(&redis)`): `mark(label, at)` floors `at` to the
  minute, takes `SET deploylock-<label> <stamp> NX EX 60` (sent once; released again if the
  write fails) and then `MULTI HSETNX <YYYYMMDD>-marks <RFC 3339 minute> <label>` + `EXPIRE
  90d`: one mark per label per minute and one mark per minute (returns whether it was
  recorded). `fetch(date)`; `Deploy::git_label()` (`git log -1 --format="%h %s"`). Queries
  return the marks of every UTC day of the window that fall within it, with their bucket.

## 9c. Iterable jobs (`hench::iterable`)

- `trait IterableJob: Send + Sync + Sized + 'static` with `NAME`, `Args`, `options()`,
  `retry_in`, `retries_exhausted` (as on `Job`), `type Item: Send`, `type Cursor: Serialize +
  DeserializeOwned + Clone + Send + Sync`, `build_enumerator(&self, ctx, &args, Option<Cursor>)
  -> Result<Enumerator<'_, Item, Cursor>, JobError>`, `each_iteration(&self, item,
  &Iteration<Self>)`, hooks `on_start`, `on_resume`, `on_stop`, `on_cancel`, `on_complete`
  (each `&Iteration`), `max_iteration_runtime()` (per-job override) and
  `checkpoint_interval()` (default 5 s). A blanket `impl<J: IterableJob> Job for J` makes every
  iterable job a regular job (registration, `JobExt`, testing); with both traits imported,
  `J::NAME` is ambiguous (`<J as Job>::NAME`). No `around_iteration` (do it in
  `each_iteration`).
- `Iteration` (derefs to `JobContext`): `args()`, `cursor()` (of the last item processed),
  `executions()`, `runtime()` (all executions), `abort()` (end after this item, as completed;
  from `on_start`/`on_resume`, before the first item; from `on_stop`/`on_cancel`/`on_complete`,
  no effect). (Sidekiq's `throw :abort` is only caught around the iteration: from its start
  hooks it fails the job.)
- `Enumerator<'a, T, C>` wraps a boxed `Stream<Item = Result<(T, C), JobError>>`; each item
  carries the cursor reached once it is processed, and an enumerator built from a cursor must
  yield what comes after it. Helpers: `new`, `from_stream`, `from_pairs`, `empty`, `vec`/`iter`
  (cursor = items done), `range` (integers; cursor = next value), `paginate` (keyset pages from
  an async fetch after a cursor, item by item; stops at an empty page) and `batches` (one item
  per page, cursor of its last item). (Sidekiq's cursor is the position of the item itself;
  values coincide for arrays.)
- Execution (`Job::perform` of the blanket impl): load `it-<jid>` (`HGETALL`), restore cursor
  (`hench::InvalidCursor` error when unreadable), executions + 1; a state with `done` goes
  straight to the finishing hooks (below). Otherwise build the enumerator, `on_start` (first
  execution) or `on_resume`; then, item after item: stop if cancelled, or aborted (`abort()`
  from an item or a start hook); take the next item from the enumerator — none left: finished;
  stop before it (it is dropped: the enumerator yields it again after the saved cursor) if
  `ctx.is_stopping()`, or, after at least one item of this execution, if the run time exceeds
  the limit (job override, else `ctx.max_iteration_runtime()`) — interrupted; checkpoint
  every `checkpoint_interval` (`MULTI HSET ex c rt` + `EXPIRE 30d` + `HEXISTS cancelled`;
  cancelled → stop); process it. Taking the item before the shutdown and time checks makes an
  execution whose time runs out (or whose process starts stopping) during the last item
  complete, like Sidekiq, which checks inside its enumerator's `each`; checking first pushed
  such a job to the end of its queue for an execution with nothing to do. An item's error (or
  the stream's) saves the state (cursor of the last item done) and fails the job (no
  `on_stop`): its retry starts with that item. A panic in `each_iteration` or in the stream
  does the same (caught with `catch_unwind`, state saved, then resumed with its recorded origin,
  §3: workers report class `panic`); Sidekiq saves on any exception from `each_iteration` too.
  Interrupted: save; if the save reports a cancellation, finish as cancelled; else `on_stop`
  and return `JobError::interrupted()` (pushed back to the end of the queue by workers, §5).
  Finished (no more items, cancelled, aborted): save the state with `done` (how it ended; best
  effort: a failure is logged), then the finishing hooks — `on_cancel` (cancelled), `on_stop`,
  `on_complete` — and `UNLINK it-<jid>`. A failing finishing hook fails the job, whose retry
  (or a recovery after a crash before the acknowledgement) finds `done` and runs only the
  finishing hooks again, not the items since the last checkpoint. Once they ran, the job is
  complete: a failing `UNLINK` is logged, not an error (failing the job would run it again for
  nothing, and from its first item when the `UNLINK` was applied although its reply timed out;
  the state expires). Sidekiq's `cleanup` fails the job then. A warning is logged when one
  item takes longer than the shutdown timeout. Items run at least once (a crash or hard
  shutdown resumes from the last checkpoint).
- `Client::cancel(jid) -> SystemTime`: `MULTI HSETNX it-<jid> cancelled <now> + HGET + EXPIRE
  30d` (the first cancellation time is kept); seen at the next start or checkpoint.
  `IterationState::fetch(&redis, jid)` / `fetch_many(&redis, jids)` (one pipeline, deduped,
  jobs without state left out) read `executions`, `runtime`, `cursor` (JSON; raw string when
  invalid; `null` → none) and `cancelled` for the dashboard (Sidekiq's `IterableJobQuery`).
- TTL: 30 days, refreshed on every write (Sidekiq sets it once with `EXPIRE NX`, which needs
  Redis 7; hench supports 6.2). Testing clients keep states in memory (§8).

## 9d. systemd (`hench::systemd`)

- `Notifier` sends `sd_notify` datagrams (std `UnixDatagram`, unbound, non-blocking; `@name` =
  Linux abstract socket): `notify(state)`, `ready`, `reloading`, `stopping`, `status`,
  `watchdog_keep_alive`, `errno`, `main_pid`. `Notifier::from_env()`: `None` without
  `NOTIFY_SOCKET`; watchdog = `WATCHDOG_USEC` (> 0) when `WATCHDOG_PID` is unset or ours;
  `keep_alive_interval()` = half of it.
- Workers (`Config::systemd: Notifications::{FromEnv (default), Disabled, To(notifier)}`,
  `HenchBuilder::systemd(bool)` / `systemd_notifier(n)`; `TestRedis::builder` disables it):
  `FromEnv` applies to `Hench::run` only: workers embedded with `start` or `run_until` send
  nothing unless given a notifier explicitly (`To`), because the program that embeds them owns
  its service's notifications (an embedded `READY=1` could precede the program's readiness,
  and `STOPPING=1` after a remote stop of the embedded workers would make systemd stop, then
  kill, the whole service, and refuse any later `READY=1`). Sidekiq notifies only from its
  `sidekiq` binary too. Notifications: `READY=1` + `STATUS=processing jobs: B of C busy` once
  the process is registered and everything is spawned; `STATUS` on every beat (`quiet: …` /
  `stopping: …` variants) and on quiet; `STOPPING=1` + `STATUS=stopping: waiting up to Ns …`
  when the shutdown starts. Under a watchdog a background task of its own sends `WATCHDOG=1`
  every half watchdog timeout until the end of the shutdown, whatever the heartbeat is doing
  (a beat waits up to the Redis timeouts for each of its commands, and for the `beat`
  callbacks): keep-alives reflect that the runtime runs tasks, not that Redis answers; beats
  add `WATCHDOG=1` to their `STATUS`. Send failures are logged at debug level, never fatal.

## 9e. Worker CLI and configuration file (`hench::cli`, feature `cli`)

- `WorkerArgs` (clap `Parser`, so also `Args` for `#[command(flatten)]`): `-c/--concurrency`
  (≥ 1), `-q/--queue NAME[,WEIGHT]` (repeatable; validated; strict order without weights,
  weighted otherwise — `CapsuleBuilder::queues` rules), `-t/--timeout SECONDS` (≥ 1),
  `-C/--config PATH`, `-e/--environment` (else `APP_ENV`, else `development`), `-g/--tag`,
  `-v/--verbose`. Not applicable: `-r` (no code loading), `RAILS_MAX_THREADS`, ERB, YAML
  aliases.
- `settings()` → `Settings` (also `log_level()`: debug when verbose or systemd's
  `DEBUG_INVOCATION=1`; `log_directives()` for an `EnvFilter`), `apply(builder)` /
  `Settings::apply(builder)`: command line > configuration file (environment section > top
  level) > what the program set on the builder; unset settings leave the builder alone.
  `apply` also logs (info) the file and environment in use, and each of `Settings::warnings`:
  an environment chosen with `-e` or `APP_ENV` while no configuration file was read (the
  default file is relative to the current directory: a systemd unit without
  `WorkingDirectory=` starts in `/`, where a production worker silently ran with the
  program's settings) or while the file has no section for it, and, whatever chose the
  environment, a section whose name differs from it only by case. (Sidekiq warns about none
  of these.)
- The file is `-C PATH` (must exist) or `config/hench.toml` in the current directory if present.
  TOML keys: `concurrency`, `queues` (`"name"`, `"name,weight"`, `["name"]`, `["name", w]`),
  `timeout` (secs), `tag`, `labels`, `verbose`, `[capsules.<name>]` (`concurrency`,
  `queues`), `max_retries`, `max_recoveries`, `dead_max_jobs`, `dead_timeout` (secs),
  `max_iteration_runtime`, `poll_interval_average` and `average_scheduled_poll_interval` (float
  secs), `fetch` (`reliable`/`basic`), `metrics`, `job_logging`. Times are numbers of seconds,
  never strings (whole seconds or any number, as each key says; the `hench::cli` module docs
  list every key with its unit): a number written as a string is refused with a hint saying so
  (round 3, docs-usability-5; TOML's own message only named the type expected). The same goes
  for `#[hench::job]` (`unique_for = 600`, `retry_for = 3600`); only command-line flags, typed
  by hand, also take units (`--unique-for 10m`) (round 3, docs-usability-12). Top-level
  tables other than `capsules`, `throttles`, `queue_limits` and `periodic` are environment
  sections; the selected one is merged over the top level (shallow, like
  Sidekiq). Every part of the file is checked when it is read, the sections of the
  environments not selected included (a typo in `[production]` fails in development already):
  unknown keys, wrong types, non-positive numbers and malformed queues are `Error::Config`
  naming the file, the section and the key; a table inside an environment section other than
  `capsules` is refused (so a misspelled `[capsule.serial]`, which reads as the `capsule`
  environment's section, is an error suggesting `[capsules.serial]` instead of a capsule
  silently missing).
- `[capsules.default]` configures the default capsule, like the top-level `concurrency` and
  `queues`: it is read into those two settings (winning over the top-level keys), so `-c`/`-q`
  override it like any setting of the file. `Settings::apply` applies `capsules` before the
  top-level settings, so they also win over a `default` entry added by hand. (Sidekiq applies
  `capsules.default` after the command line, which hench's precedence rule forbids.)

- **Reserved names of the pro features** (`docs/design/pro-features.md` §2.10): `throttles`
  (`[throttles.<name>]`), `queue_limits` and `periodic` (`[[periodic]]`) are settings, not
  environment sections, at the top level and inside an environment section (whose table
  replaces the top-level one as a whole); an environment named like one of them is refused,
  naming the word (release note; `capsules` keeps its old behaviour). `[throttles.<name>]` are
  applied after the builder's own throttles (the file's throttle replaces a code throttle of the
  same name); `[[periodic]]` schedules are added after the builder's own, and a name both
  define is a configuration error at `build` (`duplicate periodic job name`), as two code
  schedules of one name are; `[queue_limits]` (queue limits, phase 3) is still refused with "not
  supported by this version of hench yet", never silently ignored. New scalar keys: `unique_jobs`, `periodic_group`, `periodic_poller`,
  `periodic_poll_interval` and `periodic_lease` (float secs; round 3: the lease is a documented
  setting now), `periodic_history`, `batch_ttl`, `batch_linger` (secs), `batch_max_commit_jobs`,
  `batch_maintenance`.

## 9f. Per-job log level

- `JobOptions::log_level(tracing::Level)` / `JobSetter::log_level` / macro `log_level = "debug"`
  store `"log_level": "<level>"` in the payload (Sidekiq's `log_level` option; read
  case-insensitively, `warning`/`fatal` accepted). The job span records `log_level`; the job
  logger skips its `start`/`done`/`fail`/`interrupted` events for `warn`/`error` jobs.
  `hench::JOB_LOG_LEVEL_DIRECTIVES` (`[job{log_level=trace}]=trace,…debug…,…info…`) appended to
  an `EnvFilter` logs a job's events down to its level whatever the process level (`tracing`
  filters can add, not remove: a level above the process's only silences hench's own job
  events).

## 9g. Unique jobs (`hench::unique`)

`docs/design/unique.md` condensed (its decisions U1–U16 taken as recommended). A unique job holds
a **lock** in Redis named after its **digest**; while the lock exists, a push with the same digest
is a **duplicate**. The lock is taken by the script that writes the job and held or released by
the scripts that write its outcome, never by middleware around them.

- **Guarantees.** Concurrent pushes of one digest: at most one takes the lock. A failed push
  leaves no lock (the job is written before its lock; under `OOM` the job's write, the first,
  is refused). A lock is released only by its holder's lifecycle (compare-and-delete on the
  jid) or by an operator. Every lock has a TTL. A push resent after a lost reply with the same
  jid is "already pushed", never written twice. Not guaranteed (documented): a lock whose TTL
  ends before its job lets a duplicate in (a **lapse**, counted); crash, shutdown and orphan
  requeues and morgue revivals do not take a lapsed lock back (U13, U14); uniqueness is not
  concurrency control (`Start` + a per-key concurrency limit of one, §9j, for "never two
  running").
- **Policies** (`UniqueUntil`; unknown names read leniently as `Success`): `Success` (default:
  held while waiting, running and retrying; released at success, server-middleware halt,
  batch skip, death, discard, `retry: false`, `dead: false`, API delete or kill), `Start`
  (released by `PerformEndpoint` just before `perform`, after the batch cancellation check;
  taken back, if free, by a retry, push back or limit reschedule), `Expired` (only its TTL or
  an operator). TTL = `unique_for` (whole seconds, 1 to 999 999 999, mandatory: no global
  default) `+ max(0, at − now)` for a scheduled push, a retry or a parked job (Redis clock; at
  least 1 s); running `Success` jobs are renewed to at least `unique_for` by every heartbeat
  (`UNIQUE_RENEW`, which never re-creates a lock).
- **Digest**: `base64url(SHA-256(canonical_json(["hench-unique/1", class, queue or null, key])))`
  (43 characters, `crate::canonical`, golden vectors pinned); `key` = the explicit `unique_key`,
  else `Job::unique_key(&args)` (typed pushes; macro `unique_by`), else the `args` array; queue
  `null` with `unique_across_queues(true)`. Computed once by `Client::normalize` before client
  middleware and stored in the payload (`unique: {"for", "until", "digest"}`; `key`,
  `across_queues` removed, `on_conflict` stripped by `Stored::encode`); a supplied digest must be
  1–128 characters of `[A-Za-z0-9_-]`. Retries, requeues, the scheduler and API moves reuse it
  (they never take locks: Sidekiq Ent #2499; a job moved later keeps the lock it holds, and one
  whose move client middleware stops is deleted, releasing it). Raw pushes never call
  `Job::unique_key`.
- **API.** `Unique { ttl, until, on_conflict, across_queues, key, digest }` (field-wise
  `merge`), `UniqueOption::{Disabled, Enabled}` (JSON `false` or object; the payload field is
  read leniently, a malformed value is `None` with a warning). Setters on `JobOptions`,
  `JobSetter`, `JobBuilder`, `BulkPush`: `unique`, `unique_for`, `unique_until`, `on_conflict`,
  `unique_across_queues`, `unique_key`, `not_unique` (higher layer wins; `Disabled` above wins;
  two `Enabled` merge per field). `Job::unique_key(args) -> Option<Value>` (forwarded for
  iterable jobs). Macro options `unique_for = secs`, `unique_until = "..."`, `on_conflict =
  "..."`, `unique_across_queues = bool`, `unique_by = [params]` (`hench-macros/src/unique.rs`:
  parsed, checked and generated into the job's `options()` and `unique_key`; the others without
  `unique_for` are compile errors, since every push would fail or nothing would be unique, and a
  push opts out with `.not_unique()`). `OnConflict::Drop`
  (default:
  `Ok(None)`, `PushOutcome::Duplicate { jid, holder }`), `Error` (`Error::DuplicateJob { class,
  digest, holder }`; `None` in bulk pushes), `Replace` (debounce: replaces a holder still waiting
  in `schedule` at the score its lock recorded, not leased by a scheduler and not a batch member;
  the lock records the holder's member there, so one `ZSCORE` finds it however many jobs share
  its run time; otherwise like `Drop`; a new jid). `Client::push_outcome`, `push_bulk_outcomes`,
  `JobSetter::push_outcome`, `push_bulk_outcomes` → `PushOutcome::{Pushed, Replaced, Duplicate,
  Stopped}`. `Client::with_unique_jobs(false)`, `HenchBuilder::unique_jobs(false)`, TOML
  `unique_jobs = false`: pushes drop uniqueness (the engine still holds and releases the locks of
  jobs that carry one). `JobContext::unique()` (settings as fetched) and
  `release_unique_lock()` (compare on the jid; the job gives its lock up for the rest of its
  execution: its outcome does no lock work and counts no lapse, the heartbeat stops renewing the
  lock, and the copy continuing the execution — a retry, a push back, a reschedule, the morgue's
  copy — is stored with `"unique": false`, so it waits and runs without a lock; the flag lives in
  the execution's state, shared by its context and the processor, nothing process-wide).
- **Data model.** `unique:<digest>` HASH {`jid`, `class`, `queue`, `until`, `for` (s), `t` (lock
  time), `at` (score while the holder waits in `schedule`), `m` (its member there, the job's
  JSON: a debounce push finds it with one `ZSCORE`)} with `PEXPIRE` always;
  `unique-locks` ZSET (member digest, score expiry; trimmed 10 per push, by reads and
  `UniqueLocks::trim`); `u|YYYYMMDD|H:MM` HASH `<class>|d` duplicates, `|r` replaced, `|z`
  lapses (8 h, under `pcall`). About 436 B per lock, plus the job's JSON while its holder waits
  in the schedule.
- **Scripts** (`unique/lua/*.lua` after the prelude and the frozen `lib.lua`, §9k): `UNIQUE_PUSH`
  (chunks of 100; per job `{1}` pushed, `{2, jid}` already pushed, `{0, holder}` duplicate, `{3,
  old}` replaced, `{4, err}` pushed without lock, `{-1, err}` not pushed; trim last),
  `ACK_UNIQUE`, `SETTLE_UNIQUE`, `PUSH_BACK_UNIQUE` (reliable fetch: `LPOS`/`LREM` guard, the
  outcome write, the lock work under `pcall` counting `z` for a `Success` job that does not hold
  its lock, the removal last), `UNIQUE_LOCK_OP` (basic fetch), `UNIQUE_RELEASE`, `UNIQUE_RENEW`,
  `REMOVE_UNIQUE`, `KILL_UNIQUE` (API delete/kill: removal and release in one script, sent once,
  presence checked on error), `RESCORE_UNIQUE` (`api/sorted.rs`: `reschedule` and the
  scheduler's deferral; `ZADD XX`, then a hold only while the job holds its lock),
  `UNIQUE_RELEASE_MANY` (`kill_all` chunks, `release_all`),
  `UNIQUE_TIDY` (index entries whose lock is gone). Each is tested on a real `redis-server`
  (`unique::lua` tests): resends, `OOM`, wrong-type keys, concurrency, trims.
- **Engine.** `unique::engine::lock_op` maps the outcome: success, halt, skip → release;
  interrupted, over limit, throttled → hold (`for` + delay, `at` recorded for the schedule);
  retry → hold (`for` + retry delay, no `at`: a retry is never replaced); morgue, discard, drop
  → release; shutdown requeue → none; `Expired` → none. Basic fetch holds **before** the outcome
  write (a job fetched again must find its lock held) and releases **after** it (the lock
  protects the job until its outcome is written); failures there are logged, the TTL bounds
  them. Lapses are counted by the scripts and warned once per class and minute per process. The
  heartbeat renews the locks of the running `Success` jobs that kept theirs, and reports a
  renewal that found no lock only for a job still running once the reply came (a job
  acknowledged during the round trip released its lock with its outcome).
- **API (`hench::api`).** `Api::unique_locks() -> UniqueLocks`: `size`, `page(n, size, order)`
  (by expiry; stale entries skipped and removed), `entries()`, `get(digest)`, `find::<J>(args)`,
  `find_in::<J>(queue, args)`, `find_for(class, queue, key)` (Ent `locked?`), `release(digest,
  jid)`, `force_release(digest)`, `release_each(&[(digest, jid)])`, `release_all(class)`,
  `trim()`, `locate(&lock) -> HolderPlace::{Running, Queued { position }, Scheduled, Retrying,
  Dead, NotFound { complete }}` (bounded to `LOCATE_LIMIT` = 50 000 entries per place),
  `events(Period::Minutes(n ≤ 480)) -> UniqueEvents { totals, by_class }`. `UniqueLock` carries
  every stored field (`fields`; a time no clock can show, written before pushes refused
  them, reads as `None`). `JobRecord::unique() -> Option<UniqueRef>`. Deletes and kills
  release locks (compare on the jid), and so does a move to a queue that client middleware
  stops (the job is deleted); `SortedEntry::reschedule` of a scheduled or retrying job holding
  its lock extends the lock to `for` beyond the new time (`RESCORE_UNIQUE`: never shortened,
  never taken back once lapsed, `at` recorded in the schedule so that a debounce push finds the
  job); retries, moves to queues and `clear` leave locks alone (round 3: drops kept locks and
  reschedules let them lapse before the job ran, unique-2 and unique-5).
- **Testing.** Fake and inline clients enforce locks in a per-client `UniqueTable` (shared with
  clones and collecting clients), a Rust port of the scripts' decisions: duplicates `None`,
  `Start` released before `perform`, `Success`/`Start` released at success and at failure (no
  retries in testing), `Expired` kept, interrupted jobs keep theirs, debounce replaces a
  captured scheduled job that is not a batch member (as in Redis, whatever shares its run time).
  `Testing::unique_jobs(bool)` (default on; `from_hench` copies the
  builder's switch); `Client::unique_holder::<J>`, `unique_holder_for`, `clear_unique_locks`. A
  model-based test runs random sequences against the scripts, the table and a pure model.
- **Surfaces.** Dashboard Locks tab (`HenchWeb::locks`): `GET /locks` (summary, last hour's
  counters with the ten classes with most duplicates, filter over class/queue/holder bounded to
  50 000 entries, pages, Live), `POST /locks` (release selected `{digest}.{jid}`, at most 1000,
  only while the holder shown still holds it), `POST /locks/all/release`, `GET /locks/{digest}`
  (fields, raw hash, TTL, where the holder is; `?jid=` says whether that job holds it),
  `POST /locks/{digest}` (release naming the holder shown); a Unique row on job pages linking to
  the lock with the job's jid (notice `u-released`). `hench locks [list|show|release|
  release-all]`, `hench stats locks` (digests may start with `-`). JSON API: `GET /locks?class=&
  limit=`, `GET /locks/{digest}`, `DELETE /locks/{digest}?jid=` or `?force=true` (200 released;
  400 neither or both; 404 no lock; 409 another holder; 403 read-only); `POST /jobs` answers 201
  (pushed or replaced, with `replaced`), 200 `{"jid": null, "duplicate_of"}` for a dropped
  duplicate, 409 with `on_conflict: "error"`.
- **Deviations from `docs/design/unique.md`**, each the most production-ready option the
  frozen interfaces left:
  1. `UNIQUE_RENEW` counts no lapse: it returns the jobs that lost their lock (warned, rate
     limited); their outcome counts the lapse once (counting per beat would count one lapse
     many times).
  2. Basic fetch orders the lock work around the write (hold before, release after) instead of
     always after it.
  3. `Replace` never replaces a batch member (its batch would wait for it forever).
  4. `release_unique_lock` makes the job's outcome skip its lock work (no lapse counted, no hold
     on retry): the job gave its lock up. Round 3: the copy continuing the execution is stored
     without uniqueness (its retry used to count a lapse), the renewals skip the execution, and
     the flag is the execution's own (a process-wide map of jids leaked the notes of `Expired`
     jobs, scanned them under a mutex every unique outcome took, and could make a later job
     reusing the jid skip its lock work).
  5. The job-page Unique row cannot read Redis (`job_rows` is synchronous): it links to the
     lock's page with `?jid=`, which says "held by this job", "held by another job" or (404) that
     no job holds it.
  6. Additions: `UniqueLocks::{release_each, locate}`, `HolderPlace`, `UniqueLock::fields`,
     `UNIQUE_TIDY`, `GET /locks/{digest}` and `?force=true` in the JSON API, `hench locks release
     --all` (alias of `release-all`), a DIGEST column first in `hench locks` (what `show` and
     `release` take).
  7. Phase 2 (§9k) turned on the Locks tab and `hench locks` and wired the macro options into
     the expansion (`tests/ui/pass/feature_options.rs`).
- **Tests** (`docs/design/unique.md` §3; the references' scenarios ported as behaviour):
  digests and options (D1–D12) in `unique/mod.rs`, `unique/types.rs` and `canonical.rs`; every
  script on a real `redis-server` (`unique/lua/redis_tests.rs`: holder comparison, resends,
  `OOM`, wrong-type keys, concurrent pushes, trims, E13); `tests/unique.rs` (P1–P14),
  `tests/unique_engine.rs` (E1–E12 on real workers, C1–C5: crash recovery, hard shutdown, Redis
  restart, concurrent pushers against two workers, concurrent debounce), `tests/unique_api.rs`
  (A1–A4, holder lookup), `tests/unique_testing.rs` (T1, T2) and `unique/parity_tests.rs` (C6,
  T3: random sequences against Redis, the testing table and a model); dashboard W1–W3 in
  `hench-web/src/pages/locks.rs`; `hench locks` S1 in `hench-server/src/commands/locks.rs`; JSON
  API and `POST /jobs` S2 in `hench-server/tests/locks.rs`; macro M1 in
  `hench-macros/src/unique.rs` and `tests/ui/fail/unique_options.rs`. The soak scenario (C7) is
  the `pro` scenario (§10; `docs/design/pro-features.md` X21).

## 9h. Periodic jobs (`hench::periodic`)

`docs/design/periodic.md` condensed (its decisions taken as recommended). A **schedule** (a
name, a **timetable** — a cron expression in an IANA zone, or a fixed interval — and a job
**template**) produces one job per **tick**. Schedules are defined in code (`HenchBuilder::
periodic`, TOML `[[periodic]]`) or at runtime (`PeriodicSet::upsert`, the CLI): both live in
Redis and fire the same way, from a leaderless poller in every worker.

- **Guarantees.** A tick is enqueued **at most once** whatever crashes, lost replies or
  concurrent pollers do: `PERIODIC_FIRE` pushes the job and advances the schedule only if the
  hash's `next` and `digest` still hold what the poller read (compare-and-set), and refuses a
  tick at or before `last_tick` (monotonic guard). **At least once** while some poller runs: a
  poller *leases* a due schedule (its score moves a lease ahead, 60 s by default; a crashed
  poller's lease simply ends) and the catch-up policy decides what missed ticks become. Due-ness
  uses Redis's clock (`TIME` in the lease script), so host clock skew cannot fire early or twice;
  ticks are computed from the previous tick, never from a wall-clock reading. Jobs then run with
  hench's at-least-once execution; overlap of slow jobs is prevented with `unique_for` (a tick
  whose previous job holds the lock is consumed as a `duplicate`).
- **Timetables** (`periodic/timing.rs`, `croner` 4 behind a private wrapper, zones from `jiff`:
  the system database, and for a name it lacks the copy bundled with hench — `jiff` alone reads
  only the system database when there is one): 5 fields, or 6 with seconds first; aliases
  `@yearly`, `@annually`, `@monthly`, `@weekly`, `@daily`, `@midnight`, `@hourly`; lists, ranges,
  steps, names, `L`, `W`, `#`, `+`; POSIX day rule (day of month OR day of week when both are
  restricted); `@reboot`, `@every` and a year field are refused, and an expression must tick (an
  impossible date such as `0 0 30 2 *` is refused by checking the day and month fields against
  29 years of dates, in milliseconds, before any search). Default zone UTC. DST follows Vixie
  cron: an expression is fixed-time when none of its seconds, minutes and hours fields starts
  with `*` (cronie's `MIN_STAR`/`HR_STAR`: single values, lists, ranges); a fixed-time tick in a
  skipped interval (of up to two hours) runs right after the gap, in a repeated interval once;
  ticks of fields starting with `*` follow real time. `croner` applies those rules to single
  values only: for lists and ranges the wrapper drops the second pass of a fold and adds the
  instant after a gap in which a skipped wall clock time matches. Searches cross whole skipped
  days (Pacific/Apia 2011; a failed search is tried again only past the zone's transitions);
  backward searches are checked against the forward sequence. `Periodic::every(name, interval)` ticks at whole multiples of the interval since the
  epoch, shifted by `offset` (1 s to 100 years, offset below the interval).
- **Catch-up** (`periodic/plan.rs`, pure): `CatchUp::Latest` (default: one job for the most
  recent missed tick), `Skip` (a tick is enqueued only within its deadline, 5 min by default),
  `All { max }` (each missed tick oldest first, at most `max` from 1 to 10 000), with an optional
  `deadline` (inclusive) for every policy. Missed counts are capped at 10 000 per catch-up;
  `All` backlogs are walked in constant time per tick. Pausing and resuming never backfills. A
  due `next` that is not a tick of the timetable as this process computes it (another process
  computed it under other zone rules — older or newer tzdata, a rule change, another version's
  DST rules — or it was edited by hand) stands for the nearest tick within three hours: enqueued
  in that tick's place (the next one after it follows), not at all when that tick was handled
  already (`last_tick`; a `skipped` history entry), as a tick of its own when no tick is that
  close; warned about once per definition.
- **Definitions** (`periodic/definition.rs`): `spec` (`{"v": 1, ...}`, versioned), `timing`
  key, `policy`, the canonical template JSON and a **digest**, SHA-256 base64url of the
  canonical `{"v", "spec", "policy", "job", "description"}`; a crontab's digest is SHA-256 of its
  sorted digests. Templates refuse what belongs to one push (`jid`, `at`, timestamps, retry
  state, errors, `bid`, `batch_callback`, `periodic`, `limited`, a supplied unique digest),
  `on_conflict` other than `drop`, and `unique` without `unique_for`; typed `Job::unique_key` and
  `Job::throttle_key` are computed once (`Periodic::job::<J>`) and stored. Names: 1 to 200 bytes,
  no control characters, no surrounding spaces, not `.` or `..` (dot segments in the dashboard's
  and the JSON API's URLs, round 3); descriptions up to 2 000 bytes; at most 10 000
  code schedules per process (`build` refuses more, duplicates and invalid definitions).
- **Data model.** `periodic` ZSET (due index: name → next tick, or lease end), `periodic-names`
  ZSET (every name, score 0), `periodic:<name>` HASH (`name`, `source` `code`|`api`, `group`,
  `spec`, `timing`, `policy`, `job`, `description`, `digest`, `next`, `paused`, `last_tick`,
  `last_at`, `last_result`, `last_jid`, `last_by`, `manual_at`, `manual_jid`, `run_tick` (ms),
  `run_at`, `run_status`, `run_ms`, `run_error`, `run_jid`, `run_token`, `fired`, `missed`,
  `skipped`, `run_failures`, `error`, `error_at`, `created_at`, `updated_at`),
  `periodic-history:<name>` LIST (JSON `{tick, at, result, jid, by, missed, late_ms}`, newest
  first, capped by `periodic_history`, 10 by default), `periodic-group:<group>` SET,
  `periodic-crontabs` HASH (group → `{digest, count, at, by, version}`). No expiry: removed with
  the schedule. Jobs carry `periodic: {"name", "tick" (ms), "manual"?}`.
- **Scripts** (`periodic/lua/*.lua` after the prelude; each tested on a real `redis-server`):
  `PERIODIC_LEASE` (lease the earliest due schedule, skipping names this process cannot read;
  answer the next due time otherwise), `PERIODIC_FIRE` (the compare-and-set, the monotonic
  guard, the write — queue, schedule, or none for a halt, a skip or a duplicate —, the unique
  lock step through the frozen `hu_push_one`, counters, history, the new `next`; replies `{1}`
  done, `{2}` guard, `{3, holder}` duplicate, `{0}` already handled, `{-1}` changed, `{-2}`
  paused, `{-3}` gone, `{-4, err}` lock unreadable), `PERIODIC_SYNC` (a group's registration in
  one script: creations, updates, reschedules, pruning, conflicts with dynamic names),
  `PERIODIC_UPSERT`, `PERIODIC_REMOVE`, `PERIODIC_PAUSE`, `PERIODIC_UNPAUSE` (compare on the
  digest), `PERIODIC_ENQUEUE_NOW`, `PERIODIC_RECORD_RUN` (idempotent through a token: a failure
  counts once), `PERIODIC_ERROR` (records an error without re-creating a deleted hash) and
  `PERIODIC_UNINDEX` (removes a due-index entry only while its schedule is paused or deleted).
- **Poller** (`periodic/poller.rs`, one task per process unless `periodic_poller(false)`):
  lease, read, compile (cached by digest), plan, build the job through `Client::prepare_push`
  (client middleware runs: it may change, schedule or halt the job — a halt consumes the tick),
  FIRE; a lost FIRE reply is settled from `last_tick`/`last_by`/`last_jid`. Sleeps until the
  next due time (at most `periodic_poll_interval`, 5 s), with jitter up to 250 ms (capped by the
  interval). An invalid definition is parked (error recorded, retried an hour later, `periodic
  job is invalid`); a definition in a newer format, or in a zone this process's database (and the
  bundled copy) lacks, is skipped by this process for 10 minutes and left due for the pollers
  that can read it: every skipped name goes to `LEASE` (up to 10 000; past that a skipped name
  handed back keeps its lease), and a poll yields after 1 000 such definitions. Parsed
  timetables are reused for five minutes at most, so that a tzdata update reaches a running
  poller. A paused or deleted schedule found in the due index leaves it through
  `PERIODIC_UNINDEX`, which checks and removes in one step. Redis outages go through
  `Engine::periodic_outage` (`could not
  enqueue periodic jobs`); a panic restarts the loop after a second (`periodic poller crashed;
  restarting it`); the loop stops with the engine. Logs: `enqueued periodic job` (info, debug
  under a minute), `periodic job enqueued late` (warn, > 60 s), `periodic ticks were missed`.
- **Registration** (code schedules): at start, every process that defines schedules or sets
  `periodic_group` (default group `default`) registers its crontab with `PERIODIC_SYNC`
  (retries every 5 s, `could not register periodic jobs` after 5 attempts; polling goes on
  meanwhile). Template changes keep the next tick; timetable changes reschedule it; names the
  code no longer defines are pruned; operator state (paused, history, counters) survives; a name
  taken by a dynamic schedule is a conflict (reported, left alone). Every
  `periodic_check_interval` (60 s) each process checks its group's record and re-registers when
  it is missing or unreadable (FLUSHDB, a restart without persistence) or holds another crontab
  registered before this process's own registration (Redis went back in time: a restore, a
  failover to a replica that missed it); a record of another crontab registered since is warned
  about once (the last registration wins). A crontab's digest covers its schedules' names and
  digests. The heartbeat's `periodic` object (`periodic::info`: `{poller, group, digest,
  schedules}`) tells the dashboard who polls and which crontab each process declares. Schedules
  of the worker's file (`[[periodic]]`, by class and JSON arguments) take, at `build`, the
  options of the job type the process registers for the class and its typed keys, under the
  table's keys (not its `on_conflict`: a schedule's ticks drop duplicates); arguments that do
  not fit the job type are refused.
- **API.** `Api::periodic() -> PeriodicSet`: `size`, `summary(late_after)` (`PeriodicSummary`:
  total, active, late, next due; indexes only), `page(n, size)` (by name), `entries()`, `get`,
  `history(name, n)`, `upsert(&Periodic) -> Upserted::{Created, Updated, Rescheduled,
  Unchanged}` (dynamic; a code schedule's name is refused; compare-and-set on the digest with
  back-off, then the last upsert wins), `remove` (dynamic only), `remove_any`, `pause`, `unpause`
  (resumes at the first tick from now on), `enqueue(name)` ("enqueue now": `periodic.manual`,
  tick = now, through the API's client middleware; `None` when middleware halted it or its unique
  lock is held; the history keeps the configuration's `periodic_history`, or `with_history`'s,
  and without either — the dashboard, the JSON API, the CLI — its length plus the new entry,
  at least 10 and at most 1 000 unless already longer: round 3, it trimmed every history to 10),
  `groups()`, `repair()`, `with_history(n)`, `with_clock(ManualClock)` (test-util).
  `PeriodicEntry` reads leniently (`timetable`, `job` are `None` and `error` says why for a
  definition this version cannot read) and offers `is_paused`, `upcoming(n)` (from the pending
  tick; none while paused), `describe`, `history`, `pause`, `unpause`, `enqueue`. `Timetable`
  has `upcoming`, `describe`, `local_time`, `transition_note`, `validate`.
  `JobRecord::periodic()` and `JobContext::periodic()` give `PeriodicTick { name, tick, manual }`.
  After each outcome the processor spawns `RECORD_RUN` (`ok`, `failed` with `class: message` up
  to 500 characters, `interrupted`, `rescheduled`; nothing for jobs that did not run to an
  outcome), never awaited.
- **Testing.** `Periodic::payload(tick)`, `enqueue(&client, tick)`, `validate`, `upcoming` and
  `describe` serve unit tests; fake and inline clients capture or run periodic jobs with their
  metadata (and their unique tables apply); no poller runs without Redis. `ManualClock`
  (`hench::periodic`, feature `test-util`), installed with `HenchBuilder::data(clock)` and
  `PeriodicSet::with_clock`, drives the timetables of real workers across hours, days and DST.
- **Surfaces.** Dashboard Periodic tab (`HenchWeb::periodic`): `GET /periodic` (summary,
  banners for no poller, overdue schedules — due for more than the default lease plus poll
  interval — and groups whose live processes declare different crontabs, filter over name, class
  and expression bounded to 50 000 schedules, 25 per page, Live), rows with chips (paused, group
  or `api`, overdue, error), timetable, job, next run in the schedule's zone, last enqueue, last
  run and row actions; `POST /periodic` (`action` = enqueue|pause|unpause|delete on the selected
  `name`s, at most 1000), `GET /periodic/{name}` (definition, next 5 ticks with DST notes,
  template, state, history, raw hash), `POST /periodic/{name}`; a schedule defined in code is
  deleted only when no live process declares its group; a Periodic row on job pages. `hench
  periodic [list|show|enqueue|pause|unpause|remove [--force]|add|repair|next]`, `hench stats
  periodic`. JSON API: `GET /periodic?page=&count=`, `GET /periodic/{name}`, `POST
  /periodic/{name}/{enqueue|pause|unpause}` (403 read-only, 404 unknown, 400 unknown action or
  unreadable definition).
- **Deviations from `docs/design/periodic.md`**, each the most production-ready option the
  frozen interfaces left:
  1. A tenth script, `PERIODIC_ERROR`: recording an error must not re-create a hash an operator
     deleted meanwhile (an `HSET` would).
  2. `FIRE` answers `{code, detail}` with two more codes, `duplicate` (the holder's jid, for the
     log) and lock unreadable (the tick stays due, the error is recorded); it clears a recorded
     error when a tick goes through, and forgets `last_jid` when nothing was pushed.
  3. History entries are written for every handled tick (skipped, halted, duplicate, manual
     too), so the history explains gaps in the enqueues.
  4. `SYNC` compares a group's members as sets (no reliance on Lua's ordering) and re-scores a
     changed definition (its error cleared); `UPSERT` re-scores an updated one; `upsert` falls
     back to an unconditional write after five compare-and-set attempts (the last upsert wins).
  5. `run_tick` is stored in milliseconds (manual ticks are), `run_at` comes from Redis's clock,
     and a token makes `RECORD_RUN` idempotent; a job dropped with `retry: false` records a
     generic failure (the job as fetched carries no error; the handlers got it).
  6. The poller moves a `next` at or before `last_tick` past it (no guard warnings after a
     restore); invalid definitions are parked an hour, newer ones skip-listed; processes with the
     poller off still register their crontab; jitter is capped by the poll interval.
  7. The API types live in `hench::periodic` (`api/periodic.rs` is compiled from there) and are
     re-exported by `hench::api`; additions `PeriodicSet::{summary, with_history}`,
     `PeriodicSummary`, the `Timetable` helpers; `PeriodicEntry::{timetable, job}` are `Option`.
  8. The manual clock goes through `HenchBuilder::data` (the frozen builder has no
     `periodic_clock`, and phase 2 kept it so); the heartbeat carries the `periodic` object
     (`periodic::info`: group, poller, schedules) and the worker's file its `[[periodic]]`
     tables (`periodic::toml::parse_schedules`), added after the builder's own; a schedule
     of the file named like one defined in code (or another of the file) is a configuration
     error at `build`, which names where both come from (it does not replace the code's).
  9. Dashboard: a bulk delete that keeps code schedules reports them as failures ("could not be
     changed; see the server logs", which say why): the frozen notice format has no third count;
     a process that understands periodic jobs but does not describe its poller (no `periodic`
     object) counts as possibly polling, so no false "no poller" banner. Notices say "schedule"
     (the integrator's strings test).
  10. `hench periodic next` is answered by `commands::periodic::offline` before the binary
      connects (it needs no Redis). `GET /periodic` pages with `page`/`count` (100 by default,
      at most 1000).
  11. Round 3 (review findings periodic-1 to -7, docs-usability-4 and -14): Vixie's fixed-time
      rule for lists and ranges (the design said "numeric fields", `croner` counts single values
      only); impossible dates refused before searching, retries only past transitions; an
      unknown zone skip-listed rather than parked, with the bundled database per name; every
      skipped name sent to `LEASE`; a foreign `next` read as the nearest tick; timetables parsed
      again every five minutes; re-registration after Redis went back in time, crontab digests
      with names; a conditional unindex script (an eleventh script); file schedules with their
      job types' options; refusals that name the override.
- **Tests** (`docs/design/periodic.md` §3; the references' scenarios ported as behaviour):
  timing, planning and definitions with property tests (`PERIODIC_PROPTEST_CASES`, 64 by
  default) in `periodic/{timing,plan,definition,builder}.rs`; every script on a real
  `redis-server` in `periodic/lua/redis_tests.rs` (resends, guards, `OOM`, wrong-type locks,
  concurrent fires); `tests/periodic.rs` (registration and conflicts, FLUSHDB re-registration,
  */2 vs */10, pause, payload and `JobContext::periodic`, client middleware, history caps,
  catch-up after hours, rare timetables over a year, shutdown, 2 000 schedules on four processes,
  unique overlap, exactly once under contention, crash between lease and fire, lost `FIRE`
  reply, `OOM`, newer versions, slow middleware); `tests/periodic_api.rs` (upserts, removal
  rules, lenient reads, pause, enqueue now, repairs, groups, summaries, fake clients);
  dashboard in `hench-web/src/pages/periodic.rs` and `strings/periodic.rs`; `hench periodic` in
  `hench-server/src/commands/periodic.rs`; JSON API in `hench-server/tests/periodic.rs`; TOML in
  `periodic/toml.rs`.

## 9i. Batches and workflows (`hench::batch`)

`docs/design/batches.md` is the binding design; this section condenses it as implemented and
records the decisions it left open.

**Model.** A batch is the hash `batch:<bid>` (format `v` 1; `created_at`, `ttl`, `linger`,
`total`, `skipped`, `kids`, `kc`/`ks` children whose `complete`/`success` were dispatched,
`touched_at`, optional `desc`, `parent`, `wf`, `open` (a hold: while above 0, neither `complete`
nor `success` fires; a workflow's overall batch until its last stage is defined), `xbid` (a root
created with a chosen bid: its commit ids outlive its linger), the callback templates
`cb_complete`,
`cb_success`, `cb_death` as JSON arrays of `{q, jid, j}` with `j` the job's JSON without
`enqueued_at`, the event times `complete_at`/`success_at`/`death_at`, their dispatch times
`*_dt`, `cancelled_at`, `finished_at`) and five keys sharing its expiry: `:pending` (SET of the
jids not yet succeeded or skipped), `:failed` (HASH jid → failure summary `{class, error_class,
error, state, reason, retry_count, at, next_at}`, the message cut at 200 characters), `:dead`
(SET), `:kids` (SET of child bids) and `:commits` (HASH commit id → the duplicates that commit
left out). Counts are the sizes of these collections, never counters, so that a repeated report
changes nothing. Index `batches` (ZSET scored by each batch's expiry, trimmed by maintenance),
outbox `batch-events` (ZSET bid → when an event fired), lock `batch-maintenance`, per-minute
counters `b|YYYYMMDD|H:MM` (`created`, `complete`, `success`, `death`, `cancelled`, 8 hours).
Every key of a batch lives `ttl` (30 days) after its last activity, or the latest scheduled
member plus `ttl`, and `linger` (24 hours) once it finished; activity refreshes the expiry once
a quarter of `ttl` (an hour at most) went by, as the hash's remaining lifetime shows, so a batch
expires between 3/4 of `ttl` and `ttl` after its last activity (an hour at most early with the
default). A parent lives at least as long as each of its children: a commit links the
expiries of the batches it declares, and the refresh of the ancestors after a child's refresh
or reopen gives each ancestor at least the lifetime of the level below. Expiries are never
shortened while a batch is active. The commit ids of a root created by `Batch::with_bid`
(`xbid`) stay `ttl` after it finished (at least its linger), so that a re-run of the job that
created it, days later, still finds its commit applied.

**Semantics.** A member's outcome is counted once, keyed by its jid and guarded by `:pending`:
success (including a server middleware that does not call `next`), a failure that will be
retried, a death (the morgue, `retry: false`, a discard, `dead: false`, a delete or kill through
the API, counted with its reason), a skip (its batch cancelled, or an operator's decision).
Interruptions of iterable jobs, shutdown requeues, hard-shutdown requeues and rate-limit
reschedules are not outcomes. `complete` fires when every member left has failed at least once
and every child's `complete` was dispatched; `success` when nothing is pending, every child's
`success` was dispatched and the batch is not cancelled; `death` at the first death. Each fires
at most once (write-if-absent, then the outbox); a dead member revived from the morgue can still
let the batch succeed. A child's callbacks are pushed as members of its parent, so the parent's
`complete` and `success` wait for them; a child's death is its parent's. Cancelling marks a
batch and its descendants: members not run yet are skipped right before they run (the
endpoint's `HGET` of `cancelled_at`, and the throttle gate's check), `success` never fires,
`complete` fires once every member left has run, and a cancelled batch whose `complete` was
dispatched finishes (linger) once nothing is pending: a member that failed before the cancel
and waits in the retry set or the morgue is skipped when it comes up, which needs the batch to
remember its cancellation until then (one that never comes back leaves the batch to expire
`ttl` after its last activity). A member counted dead that runs again and fails with a retry
is no longer dead.

**Scripts** (`batch/lua/`, `<prelude> + lib.lua + body`, every check before the first write,
bookkeeping under `pcall` last): `BATCH_COMMIT` (new batches with their children, or one
reopened batch with new children; idempotent per commit id; the unique section runs the frozen
`hu_check`/`hu_take`/`hu_count`/`hu_trim` with a per-commit `seen`, a member whose own jid
holds its lock or that a job holds is left out and reported, `OnConflict::Error` refuses the
whole commit with `DUPLICATE <job index> <holder>`; new batches without members fire at once;
the crate's own fields of a reopened batch, such as a released hold `open = 0`, are written and
the batch evaluated, so that a release fires what holds),
`BATCH_SUCCEEDED`, `BATCH_FAILED` and `BATCH_SKIPPED` (the first write is the `SREM` allowed
under `OOM`; `BATCH_FAILED` and the commit are refused under `OOM` and retried), `BATCH_DISPATCH`
(death, complete, success; pushes the stored templates with `enqueued_at`, registers a child's
callbacks in its parent, updates `kc`/`ks`/death and evaluates the parent; guarded by the
outbox entry and `*_dt`), `BATCH_TOUCH` (an ancestor's expiry after a child's refresh or
reopen, floored with that child's lifetime),
`BATCH_CANCEL`, `BATCH_DELETE` (descendants first; an unfinished child of a live parent only
with `force`). Time is Redis `TIME`.

**Client.** `Batch::new(&client)` (a child when `client` is a collecting client),
`Batch::open` (reopen: members and children only), `Batch::with_bid` (idempotent creation with
a chosen bid and commit id); `description`, `callback_queue`, `expires_in`, `linger`,
`commit_id`, `on_complete`/`on_success`/`on_death`/`on(event, setter, args)` (at most 16 per
event and 64 KiB of templates; `at`/`delay` refused; the job type's options, then
`callback_queue`, then the setter's); `client()` and `jobs(closure)`; `commit()`. Pushes
through a collecting client are normalized and pass client middleware at once (they see the
`bid`), are buffered (`batch_max_commit_jobs`, 10 000, each child batch counting as 10: a
child costs the commit about 25 commands and its templates, and its dispatch when empty; else
`TooLarge` at the push or the child's commit; a jid twice
is `DuplicateJid`; a job naming another batch is `InvalidJob`; `Replace` is refused), and are
written by one `BATCH_COMMIT`, resent after an unknown outcome (timeout, dropped connection)
until `batch_commit_timeout` (30 s, then `BatchError::Unconfirmed`: commit again with the same
id), sent again on a new primary after a failover's refusal; a resend that finds its commit id
applied reports the members it wrote. An empty new batch's callbacks are dispatched by the
committing client, 16 batches at a time. `Committed {bid, jids, duplicates, children,
already_applied, value}`.
`Client::{batch, batch_status, cancel_batch}`, `JobContext::{batch, batch_callback,
batch_status, is_batch_cancelled, check_batch}`, `BatchCallback::status`.

**Engine.** The processor records a member's outcome before writing it (retried while Redis is
unavailable until a hard shutdown, three attempts otherwise, then reported to the error
handlers as `could not record a job in its batch` and the outcome written anyway: the member
stays pending until an operator skips it), then dispatches what the accounting found waiting
(at most 5 s; maintenance finishes) and refreshes the ancestors. A member whose batch was
cancelled is skipped before it runs (metric `s`, processed, not failed); `batch_cancelled()`
from a member of a live batch is an ordinary failure. Maintenance (every
`batch_maintenance_interval`, 30 s, jittered; lock `SET NX PX` 0.8 × interval held by one
process at a time) dispatches the outbox entries older than `batch_dispatch_grace` (10 s, at
most 100 per round; an entry whose dispatch fails for another reason than Redis being
unavailable is logged and put back a minute, behind the others) and trims the index of expired
batches. The heartbeat advertises
`features.batches = 1`.

**Workflows.** `Workflow::new(&client).stage(|s| s.job::<J>(args)…)…start()`: an overall batch
`W` holding the user's callbacks and the stored stages 2 to N (`wf`), stage 1's batch a child
of `W` whose `on_success` is `hench::WorkflowNext(W, 2)` (a reserved job resolved by every
registry): being a child's callback it is a member of `W`; it reopens `W` with commit id `wf-n`
(a re-run adds nothing) and defines the next stage. It runs in the workflow's `callback_queue`,
else in the queue of the first job of the stage that just succeeded (a queue some worker polls,
since that job ran). `W` is held open (`open = 1`) from its start until the commit of its last
stage releases the hold (`open = 0`): between stages `W` has no member of its own and its
defined stages may all have completed (a member failed once), which must not fire its
`complete` before the later stages exist. So the workflow's `complete` means every job of every
stage ran at least once. A dead member is the workflow's death; skipping it continues the
workflow; cancelling `W` releases the hold and stops it. `Workflow::with_bid` (a chosen bid for
`W`) and `commit_id` start a workflow once from a job that may run twice. Each stage starts with
one commit holding its batch (a child: 10 toward `batch_max_commit_jobs`) and its jobs, so
`start` checks every stage against the starting client's cap before writing anything
(`BatchError::StageTooLarge`, naming the stage; release review, round3-regressions-3: a later
stage within 10 jobs of the cap used to pass `start` and fail in its `hench::WorkflowNext`,
stalling the workflow); the workers that run the later stages should have the same setting.

**Testing.** Fake and inline clients keep a `BatchTable` (`batch/model.rs`, the scripts'
state machine in Rust): a fake commit captures members and dispatches callbacks as captured
jobs (a child's callbacks as members of its parent), an inline commit runs the members in
order, each followed by the callbacks it fired (a failing member counts dead, its death
callbacks run, the commit returns its error); a commit made by a job that an inline commit
runs (a workflow's next stage, a step adding to its batch) only queues its members, which the
outer commit runs next, in a loop, so that the stack does not grow with the number of stages;
`batch_status` and the cancellation checks read the table.

**Tests.** Scripts against `redis-server` (`batch/script_tests.rs`: the design lab's
scenarios S1–S18, `OOM`, wrong types, the expiry of every key, the unique section, the minute
counters); the parity proptest model ↔ scripts with the model's invariants
(`batch/parity_tests.rs`); definitions and commits (`tests/batch.rs`, D1–D14), real workers
(`tests/batch_engine.rs`, E1–E21), concurrency and chaos (`tests/batch_chaos.rs`, C1–C9), the
API (`tests/batch_api.rs`), testing modes (`tests/batch_testing.rs`), the dashboard
(`hench-web/tests/batches.rs`) and the command and JSON API (`hench-server/tests/batches.rs`).

**Operability.** `hench::batch::Batches` (`Api::batches()`): `size`, `undispatched`, `page`,
`search` (filters by state and text over the 50 000 most recently active batches, at most 500
matches), `fetch`, `failures` (a `next_at` no clock can show reads as `None`),
`pending_jids`, `children`, `cancel`, `delete`, `skip`, `dispatch`, `wait`, `events`;
`JobRecord::batch_callback`. `cancel` and `delete` act on the whole tree in one call (round 3:
they stopped at 1 000 and 10 000 batches, and a repeated cancel never got further): `cancel`
runs `BATCH_CANCEL` level by level, 500 batches per pipelined round trip, leaving descendants
that are gone or succeeded alone and returning the first other refusal after cancelling the
rest and dispatching; `delete` reads the tree level by level (pipelined `SMEMBERS`), then
deletes it from the deepest level up (pipelined forced `BATCH_DELETE`), the batch named last. Dashboard Batches tab (`/batches`
by state with progress bars labelled in words, `/batches/{bid}` with facts, callbacks,
failures linked to their retry or morgue entries, children and pending jobs; Cancel, Delete,
Skip, Push callbacks now; Batch rows on job pages). `hench batches [list|show|cancel
--yes|delete --yes [--force]|skip|dispatch]`, `hench stats batches`, JSON API `GET /batches`,
`GET /batches/{bid}`, `POST /batches/{bid}/cancel`. Logs `batch committed`, `batch event
fired`, `batch callbacks dispatched`, `batch cancelled`, `batch deleted`.

**Decisions and deviations** (each the most production-ready option):

1. `:commits` is a HASH (commit id → the duplicates that commit left out), not a SET: a resend
   after a lost reply reports the same `Committed::duplicates` as the send it repeats (§2's
   keys table and `keys::batch_commits` say so).
2. The commit declares the six keys of every batch it touches and refreshes them all (a reopen
   used to leave `:failed` and `:dead` behind); parents outlive their children.
3. Accounting gives up after three attempts on errors other than unavailability (wrong types,
   scripts refused) and lets the outcome be written; while Redis is unavailable it retries
   until a hard shutdown.
4. `batch::validate` takes `batch_maintenance_interval` and `batch_dispatch_grace` from 1 ms
   (the design said 1 s): the test builder uses 100 ms and the dispatch race test 1 ms; the
   defaults stay 30 s and 10 s.
5. The JSON API's list answers `{total, page, count, searched, truncated, batches}` instead of
   a bare array, so that clients can page; `hench batches cancel` and `delete` need `--yes`
   (like `hench limits reset`), and `hench batches dispatch` pushes waiting callbacks.
6. `Batches::undispatched` (the outbox's size) feeds `hench stats batches`; `kill_all` records
   its members one by one.
7. The script and parity tests live in the crate (`batch/script_tests.rs`,
   `batch/parity_tests.rs`: the scripts are crate-internal), the testing-mode tests in
   `tests/batch_testing.rs` (`tests/testing.rs` is the integrator's), and the integration
   tests share `tests/batch_support/`. D14 runs end to end (unique members through a real
   commit) as well as at script level. E16
   makes the crashed process's dispatch fail (a callback queue of the wrong type) before
   abandoning it. C10 (soak) is the `pro` scenario (§10), W5 the browser test of the four
   feature tabs (`hench-web/tests/browser.rs`).
8. Wired in phase 2 (§9k): `Hench::client()` (and so job contexts) carries the configuration's
   batch settings (`batch_ttl`, `batch_linger`, `batch_max_commit_jobs`,
   `batch_commit_timeout`); `pro::validate` calls `batch::validate`; the testing executor
   treats a skipped member's `batch_cancelled` as the skip it is and skips throttle-refused
   members of cancelled batches; collecting clients are made by `Client::collecting` and share
   their client's testing tables; `hench::api` re-exports the `Batches` types;
   `Verdict::NoRetry` carries the error, so a `retry: false` member's failure summary names
   it; job pages show the batch in their Batch row (linked to its page, the `bid` no longer
   repeated among the extras), and delete and kill confirmations name the batch whose member
   is removed.
9. A batch's page locates the job of each failed member from its failure summary, never with
   `find_job` (an unbounded `ZSCAN MATCH` of the whole set per failure, on every refresh of a
   Live page): a retry at its `next_at` score, a job sent to the morgue or killed in the minute
   after its death. Each lookup reads ten entries at most, in two pipelined round trips for
   the whole page; a job not found there (running again, moved, trimmed) links to its list
   filtered by its jid (`?substr=`, the bounded search, run only on click), and a job dropped,
   discarded or deleted has no link (round 3, web-api-security-1).
10. Round 3 (`STATUS.md`): a workflow's overall batch is held open until its last stage is
    defined (`open`), so its `complete` waits for every stage (it fired at the first failure
    of a stage member, before the later stages existed); `hench::WorkflowNext` runs in the
    queue of the stage it continues (it went to `default`, which apps naming their queues do
    not poll); `Workflow::with_bid`; a cancelled batch finishes only once nothing is pending;
    the expiry refresh threshold is min(1 h, `ttl`/4) on the hash's remaining lifetime (a
    1-hour batch never refreshed); the commit ids of a `with_bid` root outlive its linger by
    `ttl`; children count as 10 jobs toward `batch_max_commit_jobs`; failing outbox entries go
    behind the others; a retry of a revived dead member clears `dead`; the refresh after a
    reopen floors the ancestors with the child's lifetime; inline commits made inside inline
    runs are queued, not nested.

## 9j. Rate limiting and throttling (`hench::limit`)

`docs/design/ratelimit.md` is the binding design; this section condenses it as implemented
and records the decisions it left open.

**Model.** A limit identity is `<kind>:<id>`: the state letter (`c` concurrency, `w` window, `b`
bucket, `g` leaky and points) and `<name>[:<key>]` (names 1-100 of `[A-Za-z0-9_.-]`, not `.` or
`..`, which are dot segments in the dashboard's and the JSON API's URLs; keys any text, those
over 128 bytes, empty or holding NUL stored as `#` + 22 characters of SHA-256, so that a key
computed from a job's arguments never fails its push; a throttle's
second limit of one kind and scope is `<name>~2`, queue limits `@<queue>`). State
`limit:<identity>` (ZSET token → lease end; LIST of grant times, newest first, at most `limit`
entries; HASH `b`, `n`; STRING theoretical arrival time `%.3f`), counters
`limit-stats:<identity>` (HASH: `g` granted, `r` refused, `w`/`wms` grants that waited and
their wait, `h`/`hms` releases and hold time, `hold` the decaying average hold time (each
release weighs a fifth), `rc` reclaimed, `ov` overages, `restored`, `adj` cost corrections, `p`
signature `w|100|1000`, `last` last use, `line` wait-line end), index
`limits` (ZSET scored by the counters' expiry, `ZADD GT`, trimmed 10 stale entries per touch),
per-minute counters `l|YYYYMMDD|H:MM` (`<name>|g`, `<name>|r`, 8 hours). Every key expires
once it carries no information; expiries are never shortened.

**Scripts** (`limit/lua/`, `<prelude> + lib.lua + body`): `LIMIT_ACQUIRE` (checks every limit,
then grants all or none; the grant is the first write; bookkeeping under `pcall`; lean flag
`N` skips counters, index and line), `LIMIT_RELEASE`, `LIMIT_RENEW` (renewed, restored when the
slot is still free, or lost with an overage counted), `LIMIT_ADJUST` (points corrections; a
refund never goes above full), `LIMIT_USAGE` (read only; the limit comes from the signature
when the caller does not know it), `LIMIT_RESET`, `LIMIT_TRIM` (drops index entries whose state
and counters are both gone, atomically with the check). Clearing a wait line is one `HDEL`.
Time is Redis `TIME`, read once (`ARGV[1]` fixes it in tests only).

**Wait line.** A refusal starts at `now + own wait`; `earliest` is the exact wait of the rate
limits (concurrency contributes 0). A caller with a ticket (the slot an earlier refusal gave it:
an in-process waiter's across its polls, a parked or rescheduled job's in `limited.s`, which the
throttle gate passes and a limiter takes from the job's context on the job's first acquisition of
that limit in the execution) keeps its place while `slot >= now + earliest`; a caller of rate limits only, back a little late
(within max(100 ms, 2 × the widest spacing)), takes the next unit that frees, still without a
new place. Otherwise it gets `start = max(start, line)` and moves the line to `max(line, start +
spacing)`, but only when it will come back for that slot: a job parked or rescheduled for it
always (a throttle's parking, a limiter's `acquire` through a job's context), a waiter when the
slot is within its `max_wait`; any other caller (`try_acquire`, whatever its target, a job's
context included; a slot beyond the wait) is told where the line ends without joining it, so
that callers that never return cannot push the line away from the limit's real availability. Spacing: period ×
cost / limit for the rate kinds; for concurrency the decaying average hold `hold` / limit, else
the 24-hour average, else the caller's expected hold (a throttle's: this process's average for
the throttle, else 25 ms doubled by each earlier parking of the job), at least 1 ms. The reply
says whether the refusal gave a new place, kept the caller's or gave none. Refusals are counted
only when a new place is given.

**Limiters.** `Limiter::{concurrent, window, bucket, leaky, points, all, unlimited}`, `.key`,
`.max_wait` (5 s), `.lease` (30 s), `.reschedules`, `.max_delay`, `.backoff`, `.spread`,
`.stats`/`.stats_ttl` (1 day), `.fail_open`; `acquire`, `acquire_n`, `try_acquire`, `run`,
`usage`, `reset` on any `LimitTarget` (`RedisPool`, `Client`, `Hench`, `JobContext`, the
in-memory `limit::testing::MemoryTarget`). Waits sleep to the slot (rate kinds: exactly with a
line, each waiter has its own slot; without one a jitter of up to a tenth spreads callers told
the same wait) or poll with local wake-ups (concurrency, 25-400 ms), end at `max_wait` or when
the job is stopping. A refusal names the caller's place (`OverLimit::retry_after` leads there).
`Permit`: concurrency leases renewed every third of the lease by a task, given back on
`release` or drop (spawned), `lost()` after a lease without renewal; `points_used` corrects a
points cost.

**Over-limit reschedules.** An `OverLimit` returned by a job (or recognized by
`over_limit_when`) reschedules the job *as fetched* through the processor's `PUSH_BACK`
schedule mode with `limited {n, by, at, t, o, s}`: `o` counts the limiter reschedules since
the job last made progress, against a budget (20 by default, `over_limit_reschedules`; the
limiter's `reschedules` wins); an execution that made progress before its refusal (an iterable
job that processed items: `MadeProgress`, which the error message reports) starts it again
from 1, so a job paced by a per-item limiter is not failed for being paced; `t` counts the
parkings by throttles, against the throttle's own budget (each budget is spent by its own kind
only); `n` counts both (never reset, for display); `s` is the job's place in the line of `by`.
Past its budget the job fails with class `hench::OverLimit` into the retry system. Delay: the
limiter's backoff, else the process's, else the refusal's slot, else 60 s × n; clamped to [1 s,
max delay]; exact when it is the job's own place in the line, else with a jitter of up to a
tenth (at most 30 s). An iterable job's refused item saves the cursor and stops like an
interruption.

**Throttles.** Registered by `HenchBuilder::throttle`, `Job::throttle()` and the file's
`[throttles.<name>]` (later wins, logged); the gate runs after fetch and before middleware:
granted, the concurrency leases (token: the jid, `~` and 16 random hex digits, one per
execution, so that the release of an execution that ended after a requeue or a push back never
takes the lease of the job's next execution; lease 60 s, at least three heartbeat intervals,
else `build` refuses the throttle) are held by the engine per slot, renewed on every heartbeat
and released after the outcome write (and after a hard shutdown's requeue); refused, the job is
parked at its place in the line (`Classified::Throttled`; the slot, exact, at least 50 ms,
kept in `limited.s` and handed back as its ticket), unlimited reschedules by default; Redis
unavailable, the job goes back to its queue (or runs, with `fail_open`); an unknown throttle
name runs the job with a rate-limited warning, and so does a job without a key whose throttle
has only per-key limits; a key of the wrong type fails the job. `stats(false)` keeps no
counters, index entry or line for the throttle (each identity costs about 330 B for
`stats_ttl`, 24 h by default, otherwise); refused jobs then come back at their own wait with a
jitter. The heartbeat advertises `features.limits = 1` and the throttle specs. The throttle is
a payload field written at push time (`throttle`; `docs/design/ratelimit.md` §2.4.4, D16),
never looked up from the job type when the job is fetched: a typed push of a job type that
declares a throttle carries it, and a push by class name (`hench enqueue --throttle
NAME[:KEY]`, `POST /api/v1/jobs` with `throttle`, `hench periodic add --throttle`, a
`JobPayload` pushed with `Client::push`) is throttled only when it names one (release review,
fresh-user-1: documented in the README).

**Testing.** Testing clients are unlimited by default; the in-memory `LimitTable` ports the
algorithms, the wait line and the leases to tokio's clock (parity tests against Redis with
fixed times); `Client::limited_jobs`.

**Tests.** Scripts against `redis-server` with fixed times (`limit/script_tests.rs`: the lab's
scenarios, every key's TTL, `OOM`, `TIME`, the in-memory parity); the in-memory algorithms
against brute-force models (`limit/memory.rs`, proptest: window, bucket, leaky, concurrency,
composite); limiters (`tests/limit.rs`), over-limit reschedules (`tests/limit_engine.rs`),
throttles (`tests/throttle.rs`), the API (`tests/limits_api.rs`); chaos
(`tests/limit_chaos.rs`: lost replies, a coordinated failover; `tests/throttle_chaos.rs`:
four processes sharing a throttle and a limiter, renewals stalled past the lease, a Redis
restart without persistence, Redis unreachable at the gate, a reschedule written during a
partition); the macro's diagnostics (`tests/ui/fail/throttle_options.rs`); the dashboard
(`hench-web/tests/limits.rs`) and the command and JSON API (`hench-server/tests/limits.rs`).

**Operability.** `hench::limit::Limits` (`Api::limits()`): `size`, `page`, `page_matching`,
`entries`, `get`, `holders`, `reset`, `clear_line`, `events`, `waiting`; `JobRecord::limited`
and `JobRecord::throttle` (lenient). `waiting` looks for `"by":"<label>"` (as the engine
writes `limited`) in Redis, among the 50 000 earliest scheduled jobs, 500 per call of the
read-only `SCAN_CONTAINING` script, so that only candidates travel (each is then checked
parsed); round 3 (web-api-security-8): it read and parsed every job it looked at, about
48 MiB per view of a limit's page with 50 000 jobs of 1 KB, a page that refreshed itself.
The search costs Redis about 3.4 µs per job of 1 KB (some 0.17 s for 50 000, in slices of
1.7 ms), three times a plain `ZRANGE` of it but nothing sent. Dashboard Limits tab
(`/limits`, `/limits/{kind}/{id}`, not Live since round 3, Reset, bulk reset, Clear wait line;
Throttle and Limited rows on job pages). `hench limits
[list|show|reset --yes|clear-line]`, `hench stats limits`, JSON API `GET /limits`, `GET` and
`DELETE /limits/{kind}/{id}`. Job metrics count `o` (over-limit reschedules) and `t`
(throttled starts).

**Decisions and deviations** (each the most production-ready option):

1. A window limit is at most 100,000 (its LIST holds `limit` grant times); larger rates use a
   bucket or a leaky bucket.
2. A concurrency waiter that polls keeps its place in the line (the `earliest` rule above)
   instead of moving back one spacing per poll.
3. A throttled job's delay is its slot clamped to [50 ms, the throttle's `max_delay`], without
   jitter (round 3: the jitter turned evenly spaced returns into random arrivals); the
   process's over-limit backoff applies to limiter refusals only.
4. An inline `#[hench::job(throttle(..))]` throttle is named after the job's `NAME`, each run
   of characters a throttle name cannot hold written `.` (`Billing::Charge` → `Billing.Charge`),
   with `_` first when that leaves `.` or `..` (names that are URL dot segments are refused:
   round 3).
5. `adj` counts corrections, not units; `LIMIT_TRIM` is a seventh script; clearing a line is a
   plain `HDEL`.
6. The `Limits` API lives in `api/limits.rs`, compiled as `limit::api` (`#[path]`, like the
   unique lane's `api/unique.rs`: `api/mod.rs` is the integrator's) and exported as
   `hench::limit::{Limits, LimitInfo, Holder, LimitEvents}`; it adds
   `page_matching` (the filters of the dashboard, the CLI and the JSON API; orders other than
   last-used and filters look at the 10,000 most recent identities), `LimitInfo::{id,
   adjustments}` and `LimitEvents::{totals_of, most_refused}`; `waiting` takes the kind like
   `get` and `reset`.
7. Wired in phase 2 (§9k): `pro::validate` calls `limit::validate`; `HenchBuilder::build`
   calls `limit::throttle::check_registry` (a job naming a throttle nobody defines is a
   configuration error); the worker's file applies its `[throttles.*]` after the builder's
   (later wins); the macro emits the throttle options and the inline definitions; the testing
   clients configure their `LimitTable` and the executor parks refused jobs
   (`TestLimits::InMemory`); `hench::api` re-exports the `Limits` types; job lists show the
   throttle and over-limit chips; the metrics pages show the `o`/`t` columns (and `s`) when
   some class has them.
8. Parked jobs come back at their slots: the process that parks a job (throttled at the gate,
   or rescheduled by a limiter's refusal) asks its own scheduler to poll when the job comes
   due (`scheduled::Wakeups`, at most 1,024 pending, a wake-up merged into one at most 20 ms
   later). Without it, the parked jobs of a busy limit came back at the scheduler's next random
   poll (with N processes, one every N × `average_scheduled_poll_interval` on average), in
   bursts that the limit refused again but for a few: in the `pro` soak scenario a 3-slot
   throttle drained its backlog at about one job a second instead of its capacity.
9. Round 3 (`STATUS.md`): per-execution lease tokens; separate budgets for throttle parkings
   and limiter reschedules, the latter restarted by progress; places kept in the line (tickets
   in `limited.s`, line admission only for callers that come back, no jitter with a line, the
   decaying `hold` and an expected hold instead of 1000 ms / limit, 50 ms minimum park):
   backlogs of short jobs drain at 66-85 % of a concurrency limit instead of 9-60 %; a lease
   must cover three heartbeats; empty and NUL keys are hashed; mismatched definitions are
   logged once per limit name and pair of definitions, at refusals too; `Throttle::stats` and
   `stats_ttl`. Freed permits are not handed to parked jobs (they come back at their slots):
   jobs much shorter than the scheduler's round trip are better throttled with a `max_wait`.

## 9k. The features together

The conventions shared by the four features (`docs/design/pro-features.md` §2), their pipelines
(§3 there, §4 and §5 here), the rollout rule and the decisions of the integration (phase 2).

- **Canonical JSON and digests** (`crate::canonical`): `canonical_json` (no whitespace, numbers
  and strings as `serde_json` writes them, object members sorted by the UTF-8 bytes of their
  keys at every level whatever `preserve_order` says, depth ≤ 128, `Error::InvalidJob` beyond)
  and `sha256_b64url` (43 characters of `[A-Za-z0-9_-]`); `unique::digest(class, queue, key)`
  hashes `["hench-unique/1", class, queue or null, key]` (golden vectors pinned).
- **Lua** (`crate::lua`): scripts are `.lua` files assembled as `<prelude> + <feature lib>
  [+ unique lib] + <body>` into `LuaScript` (a `redis::Script` built once). The prelude
  defines `h_now_ms` (Redis `TIME` in ms), `h_score` (ms as a seconds score string), `h_err`
  (a `pcall` error's text) and `h_extend_at_least` (never shortens a TTL). Names are
  feature-prefixed (`UNIQUE_PUSH`, `PERIODIC_FIRE`, `BATCH_COMMIT`, `LIMIT_ACQUIRE`, ...),
  helpers too (`hu_`, `hp_`, `hb_`, `hl_`). Rules: declared keys only, Redis 6.2 commands only,
  `TIME` once, checks before writes with the first write the one whose refusal under `OOM`
  leaves a consistent state, bookkeeping under `pcall`, trims last, never shorten a TTL,
  idempotent or guarded against a resend. The tests load every registered script into a real
  `redis-server`, check the sources against the rules, and pin the SHA-1 of every script of the
  jobs without features (`scripts_from_before_the_features_are_unchanged`).
- **The unique lock library** (`unique/lua/lib.lua`, frozen; `BATCH_COMMIT`, `PERIODIC_FIRE`
  and `PERIODIC_ENQUEUE_NOW` include it): `hu_check(lock, jid)` → `'free'`, `'self'` or the
  holder; `hu_take(lock, index, digest, jid, ttl_ms, now_ms, fields, at)`;
  `hu_hold(..same..)` → 1 held (extended, never shortened), 2 taken (it was free), 0 another
  job holds it; `hu_release(lock, index, digest, jid)` → 1 released, 0 nothing to release,
  -1 another holder; `hu_count(events, class, event)` (per-minute `HINCRBY` under `pcall`,
  `EXPIRE` 8 h); `hu_trim(index, now_ms, limit)`; `hu_push_one(lock, index, events, digest,
  jid, class, ttl_ms, now_ms, fields, at, write)` → `{1}` pushed (`write()` then the lock),
  `{2, jid}` already pushed by this jid, `{0, holder}` duplicate (counted `d`), `{4, error}`
  written without its lock, `{-1, error}` refused before any write. `until` is a Lua keyword:
  the library never uses it as a table field.
- **Glue** (`crate::pro`): `Fetched`, `Gate`, `Classified`, `on_beat`, `background_tasks`,
  `info`, `validate`, `start_checks` only call the features' hooks, in the order of §5.
- **One job, every feature** (`docs/design/pro-features.md` §3; checked end to end by
  `tests/pro_features.rs` with a job that is unique (`until: start`), throttled, a batch member,
  a periodic tick and takes a limiter inside, through Redis snapshots and a `MONITOR` transcript
  of the scripts each step sends). Push: client middleware, normalization of the feature
  fields, then one script writes the job with its lock (`UNIQUE_PUSH`), or the batch commit
  writes every member with theirs (`BATCH_COMMIT`), or the poller's `PERIODIC_FIRE` writes the
  tick's job with its lock. Execution: throttle gate (before any middleware; refused: parked at
  its slot) → server middleware → endpoint (the batch's cancellation check, then the release of
  a `start` lock) → perform → classification → batch accounting (before the outcome write, so a
  crash re-runs an accounted member rather than losing its outcome) → the outcome script with
  the lock work (ack, settle, push back) → release of the throttle permits → dispatch of the
  batch events → `RECORD_RUN` (spawned). A job without a feature skips every step on one
  `Option` check each, and its scripts are byte for byte those of before (`tests/featureless.rs`).
- **One heartbeat pipeline**: `pro::on_beat` sends `UNIQUE_RENEW` (the `success` locks of the
  running jobs, 200 a call) and `LIMIT_RENEW` (the engine's throttle permits, 256 a call) in one
  pipelined round trip (`lua::run_pipelined`: `EVALSHA`s, each script loaded once and the
  pipeline sent again after a `NOSCRIPT`). The permits stay locked from the snapshot to the
  replies, so a permit released meanwhile is not restored by its renewal; a lock that lapsed is
  counted once (`z`) and warned about; a permit lost to another holder is an overage, no longer
  renewed.
- **Parked jobs come back at their slots** (§9j item 8): the process that parks a job (throttle
  gate, limiter refusal) asks its scheduler to poll when the job comes due
  (`scheduled::Wakeups`).
- **Heartbeat `features` and the rollout rule**: each process advertises the stored formats it
  understands (`"features": {"unique": 1, "periodic": 1, "batches": 1, "limits": 1}`) with its
  `periodic` object and its throttles; the Busy page names a live process lacking a feature
  that is in use ("lacks: batches"). Rule (README): upgrade every worker before any client uses
  a feature; an older worker ignores the new fields (locks then expire by their TTL, batch
  members are never counted, a limiter's refusal is a failure, newer schedules are skipped).
- **Testing modes together** (§8): one `testing::Tables { unique, batches, limits }` per testing
  client, shared by its clones and its collecting clients (`Client::collecting`); the executor
  runs the gate, the hooks in the processor's order, parks over-limit jobs under
  `TestLimits::InMemory` and counts a skipped member as the skip it is.
- **Cost** (`docs/design/pro-features.md` §3.5): a plain job pays nothing but four `Option`
  checks; per execution a unique job with a `success` or `expired` lock and reliable fetch adds
  no round trip (lock work inside the push and outcome scripts), a `start` lock one
  `UNIQUE_RELEASE` before `perform`, basic fetch two `UNIQUE_LOCK_OP` (hold before the outcome
  write, release after), and a running `success` job its share of the heartbeat's
  `UNIQUE_RENEW`; a batch member one accounting script and one `HGET`, a throttled job one
  acquire and one release (plus its share of the renewals), a periodic job one spawned
  `RECORD_RUN`: up to seven round trips instead of two with every feature. Redis CPU per job
  (cross-soak-perf-8's measurement, repeated on the tree that integrates round 3: one worker
  with 25 slots, 30,000 no-op jobs, median of three runs; jobs/s, Redis CPU per job, scripts
  per job): plain 40,011, 16 µs, 1; unique `success` 27,476, 31 µs, 2; unique `start` 20,068,
  41 µs, 3; batch member 17,130, 45 µs, 2; throttled 10,569, 82 µs, 3; per-key throttled
  9,979, 88 µs; inside a limiter 14,711, 57 µs; periodic 25,023, 29 µs; every feature 4,531,
  193 µs, 7. A Redis core therefore runs about 12,000 throttled jobs a second, cluster-wide.
  (Before round 3's fixes, measured the same way in alternation: throttled 78 µs, per key
  85 µs, periodic 28 µs, the rest within 3 %; the throttle's kept places and hold times cost
  about 4 µs a job.)
  Memory: about 436 B per unique lock (plus the job's JSON while its holder waits in the
  schedule), 200 B per batch plus 72 B per pending and 300 B per failed member, 1 KB per schedule
  plus 200 B per history entry, limiter state of 88 B (bucket, leaky), about 10 B per unit of a
  window, per concurrency holder about 30 B for a limiter's permit and 60 B for a throttled
  job (its token names the execution: the jid, `~` and 16 hex digits; some 140 B each beyond
  128 holders, where Redis stops packing the set), and about 330 B of statistics per limit
  identity (its index entry included) for 24 h (`stats_ttl`; none with `stats(false)`).
- **Performance gate** (X22; `ci/throughput.sh --base f3da43d`, §10: no-op jobs pushed first,
  then one worker with reliable fetch and 25 slots timed until it ran them all, a fresh local
  `redis-server` for each run, the two builds in alternation; a 4-vCPU VM; 15 runs of 100,000
  jobs each): before phase 0 (`f3da43d`) a median of 46,157 jobs/s (mean 46,152, standard
  deviation 1,727), the integrated tree 46,212 jobs/s (mean 46,070, standard deviation 1,598):
  +0.1 %, well within the run-to-run noise (Welch's t = −0.13) and the ±3 % target. An earlier
  comparison during the integration (45 runs each) measured −2.5 % (t = −1.7), within its noise
  too. A plain job sends the same Redis commands as before, byte for byte
  (`tests/featureless.rs`); the features' costs per step are the scripts of X15's transcript,
  one per step (§3.5 of the design).
- **Soak** (X21, §10): the `pro` scenario in its short form (90 s of load, four workers
  restarted after SIGTERMs and a `kill -9`, two Redis restarts with AOF, two windows of the
  reply-delaying proxy) held every invariant at the integrated tree: 4,938 pushes and 6,213
  executions, 456 members in 37 batches of which 6 cancelled (48 members skipped, 23 of them at
  the throttle gate), 24 ticks, at most 3 members running at once with no overage, no lock or
  outbox entry left. Its window limiter (8 a second) was seen granting up to 11 within one
  second of the workers' logs only right after a window of the proxy, which holds Redis's
  replies (the requests reach Redis on time, so the window holds on Redis's clock; the held
  replies arrive together). The first run of the scenario, during the integration, found the
  slow return of parked jobs that the wake-ups fixed (over-limit reschedules fell from 1,482
  to 182, the drain from 256 s to 104 s), and that a lost reply to `PERIODIC_LEASE` holds the
  schedule back for the lease (`periodic_lease`, 60 s), as a crashed poller does
  (`docs/design/periodic.md` accepts it: at most once always, at least once after the lease); a
  schedule ticking every few seconds may want a shorter lease.
- **Deviations of phase 0 from `docs/design/pro-features.md`** (each the most
  production-ready option the constraints left):
  1. The gate returns `Gate::{Run, Refused(Reschedule), Unavailable(Error), Failed(JobError)}`
     instead of `{Run(Held), Parked, Skipped, Requeued}`, and takes the processor's slot id
     (`tid`) and the unit of work: the permits are registered in the engine under the slot, and
     the processor performs every write (park, skip, requeue), so the gate stays a pure
     decision and the writes stay with the outcome code that owns them.
  2. `Classified` gains `Throttled(Reschedule)` (a refusal at the gate goes through the same
     outcome path: `PUSH_BACK` schedule mode with the unique hold, no record) and holds the
     failure's verdict boxed (`Failure(Box<Verdict>)`, the verdict holds the job).
  3. `batch::account::before_outcome` returns `Accounting::{Nothing, Recorded(Report),
     NotCancelled}` instead of `Option<Report>`, so that a `batch_cancelled` error whose batch
     is not cancelled turns into a failure; `limit::engine::over_limit` returns
     `OverLimitDecision::{Reschedule(Reschedule), Exhausted(JobError)}` instead of
     `Option<Reschedule>`, carrying the failure error past the budget.
  4. `Fetcher::acknowledge`, `settle` and `push_back` keep their signatures (the existing tests
     call them); the lock work goes through `acknowledge_with`, `settle_with` and
     `push_back_with`, whose `None` is exactly the plain write.
  5. `stats::Work` keeps its fields (an existing test builds it literally); the feature state
     of running jobs is a separate per-slot map (`Stats::start_featured`/`featured`), touched
     only by jobs that use a feature.
  6. `hench::WorkflowNext` is not a registry entry (the registry lists the application's jobs,
     which tests check): every worker and testing executor resolves the reserved name to
     hench's job (`Registry::resolve`), and registering the name is an `Error::Config`.
  7. `JobPayload::RESERVED_KEYS` is unchanged (a test pins it as the list of the named fields
     of before); the five feature fields are `JobPayload::FEATURE_KEYS`, which
     `is_reserved_key` checks too.
  8. `kill_all` accounts each chunk right after `MOVE_LOWEST` moved it, not before: the move
     chooses the chunk atomically, and reading it first would add a command to every
     `kill_all`. A crash between the move and the accounting leaves a killed member uncounted
     (its batch then completes only through maintenance or by hand); single-job kills account
     before the move.
  9. The dashboard's feature tabs stayed hidden (`AVAILABLE = false`) until each lane landed
     its pages; phase 2 removed the constants: the tabs are always mounted, and only their
     switches turn them off.
  10. The heartbeat's `features` map stayed empty, and phase 0's normalization dropped the
      `unique` and `throttle` fields, until the features landed; a process now advertises
      `{"unique": 1, "periodic": 1, "batches": 1, "limits": 1}` (`periodic` with the cargo
      feature) and keeps both fields.

- **Decisions of phase 2** (each the most production-ready option):
  1. The phase-0 scaffolding is gone: `AVAILABLE`, the "not supported yet" answers and
     refusals (only `[queue_limits]`, phase 3, is still refused), and `Classified::Shutdown`
     (a job stopped by the shutdown is requeued before any classification).
  2. `Verdict::NoRetry` carries the error's class and message, so that the batch failure
     summary of a `retry: false` member names its error.
  3. `hench enqueue` reports a dropped duplicate (`duplicate of <holder>`) and exits 0, like
     the client's `Ok(None)`: cron-style pushes expect duplicates; `--on-conflict error` makes
     it a failure (exit 1, the holder in the message).
  4. `HenchBuilder::build` checks the registry against the throttles (a job naming an unknown
     throttle is a configuration error, not a warning at run time) and the worker's file
     carries `[throttles.*]` and `[[periodic]]` into the builder after its own: a file
     throttle replaces the code's of its name (later wins), while a file schedule named like
     a code schedule is a configuration error naming both origins (round 3: this item said
     "later wins" for both).
  5. The heartbeat's renewals keep the engine's permits locked for the round trip (above).
  6. Parked jobs wake their process's scheduler (above): without it, a busy throttle ran at a
     fraction of its limit.
  7. The soak scenarios `batches` and `throttled` of the lane designs are one scenario, `pro`,
     which runs batches of throttled members with a limiter inside, cancellations and a unique
     periodic schedule together (§10).
  8. `HenchBuilder::periodic_clock` is not added: the manual clock goes through
     `HenchBuilder::data` (§9h item 8).
  9. The JSON API's `GET /processes` (and `hench processes --json`) carries each process's
     `features` (X17), so that a deployment script can check a rollout as the Busy page does.
  10. The performance gate is reproducible: `examples/throughput.rs` uses only API older than
      the features, and `ci/throughput.sh --base <rev>` builds it at both revisions (the older
      one in a temporary worktree) and runs them in alternation against a fresh `redis-server`
      each time.
  11. Tests that probed for a feature of the other lane now assert it (the batch commit's
      unique members, D14; the testing executor's skips of cancelled members).

## 10. Test plan

Port the relevant Sidekiq test files scenario-by-scenario (behaviour, not code):

| Sidekiq test | Hench test |
|---|---|
| client_test, job_util_test, job_test, sharding_test | `crates/hench/tests/client.rs`, `job.rs` |
| fetch_test | `tests/fetch.rs` (basic + reliable + orphan recovery) |
| processor_test, job_logger_test, exception_handler_test | `tests/processor.rs` (+ `tests/logging.rs`: the job logger's events from real workers, and `cli_test`'s maxmemory-policy warning with the other start-up checks, in a binary of its own with a global subscriber) |
| retry_test, retry_exhausted_test | `tests/retry.rs` |
| scheduled_test, scheduling_test | `tests/scheduled.rs` |
| manager_test, launcher_test, capsule_test, actors_test | `tests/launcher.rs` |
| api_test, api_utils_test, dead_set_test, filtering_test, paginator_test | `tests/api.rs` |
| (cross-lane) the API against running workers | `tests/api_engine.rs` |
| (README and guide) the embedding wiring of Getting started; the examples of each against the dependencies it lists; the guide's pages as doc tests, and the documents' links | `tests/embedding.rs`; `ci/check-readme-examples.sh` (CI job `readme`); `tests/readme.rs` |
| (supply chain) RustSec advisories, licenses, bans and sources of the dependency graph | `deny.toml` (CI job `deny`, cargo-deny) |
| middleware_test | `tests/middleware.rs` |
| testing_test, testing_fake_test, testing_inline_test | `tests/testing.rs` (+ `tests/testing_spans.rs`: a binary of its own, since `tracing` caches callsite interest process-wide) |
| metrics_test, deploy_test | `tests/metrics.rs` (+ metrics written by real workers) |
| iterable/iterable_test, iterable_job_query_test | `tests/iterable.rs` (fake/inline clients, Redis state, real workers) |
| cli_test | `src/cli.rs` unit tests + `tests/cli.rs` |
| (pro features, phase 0) jobs without features keep their Redis commands byte for byte | `tests/featureless.rs` (a `MONITOR` transcript of every path a plain job takes, against `tests/golden/featureless_commands.txt`; `HENCH_RECORD_GOLDEN=1` records it again) and `lua::tests::scripts_from_before_the_features_are_unchanged` (the SHA-1 of each script) |
| (pro features) the limit spec grammar of `hench::limit` and of `hench-macros` | `tests/spec_parity.rs` (one corpus, both parsers) |
| (pro features) the shared infrastructure | unit tests of `canonical`, `lua` (every script loaded into Redis, the rules), `unique::lua` (the frozen library against Redis), `keys`, `payload`, `options`, `pro`, `api::events`; `hench-server/tests/features.rs` |
| (pro features) unique jobs (`docs/design/unique.md` §3) | `tests/unique.rs`, `tests/unique_engine.rs` (real workers), `tests/unique_api.rs`, `tests/unique_testing.rs`; `unique/parity_tests.rs` (Redis, the testing table and a model) |
| (pro features) periodic jobs (`docs/design/periodic.md` §3) | `tests/periodic.rs`, `tests/periodic_api.rs`; every script in `periodic/lua/redis_tests.rs`; property tests in `periodic/{timing,plan,definition,builder}.rs` |
| (pro features) batches and workflows (`docs/design/batches.md` §3) | `tests/batch.rs`, `tests/batch_engine.rs`, `tests/batch_chaos.rs`, `tests/batch_api.rs`, `tests/batch_testing.rs`; `batch/{script_tests,parity_tests}.rs` |
| (pro features) rate limiting and throttling (`docs/design/ratelimit.md` §3) | `tests/limit.rs`, `tests/limit_engine.rs`, `tests/limit_chaos.rs`, `tests/limits_api.rs`, `tests/throttle.rs`, `tests/throttle_chaos.rs`; `limit/script_tests.rs`, the in-memory algorithms against brute-force models (`limit/memory.rs`) |
| (pro features) the features together (`docs/design/pro-features.md` §8, X1–X24) | `tests/pro_features.rs` (cross-feature scenarios, the order of the pipeline for one job using every feature, the heartbeat's renewals, the expiry of every feature key); `hench-web/tests/features.rs` (navigation, switches, job rows, metrics columns, the "lacks" chip) and `tests/browser.rs` (the four tabs and a job carrying every feature, in both themes); `hench-server/tests/features.rs` (commands, `hench stats` sections, `enqueue` flags and duplicates); per feature `hench-web/tests/{batches,limits}.rs`, `hench-web/src/pages/{periodic,locks}.rs` and `hench-server/tests/{locks,periodic,batches,limits}.rs`; the macro's options in `tests/ui/{pass,fail}/` |
| systemd_test | `src/systemd.rs` unit tests + `tests/systemd.rs` (real workers) |
| web_test, web_helpers_test, web_router_test, web_action_flash_test, webext_test | `crates/hench-web/tests/{pages,actions,filters,security,mount,stats,live}.rs` (+ unit tests of `format`, `params`, `csrf`, `flash`, `ctx`, `search`, `views`); the scripts and styles in a real browser: `tests/browser.rs` runs the Playwright scripts of `tests/browser/` with Node (skipped without Node and Playwright, unless `HENCH_BROWSER_TESTS=1`; `=0` skips them) |
| web_test "Metrics" and iteration scenarios | `crates/hench-web/tests/metrics.rs` (+ live workers), `tests/iteration.rs` (+ unit tests of `pages::metrics`) |
| sidekiqmon_test (+ the `hench` binary) | `crates/hench-server/tests/{cli,http}.rs` (+ unit tests of `monitor`, `cli`, `auth`, `json_api`) |

Plus: proptest (payload round-trips, retry delay bounds, weighted ordering), trybuild UI
tests for the macro, end-to-end throughput test (thousands of jobs, none lost or duplicated),
chaos tests (abort a process mid-job with reliable fetch → recovered; Redis restart →
reconnect), shutdown timing tests.

Beyond the suite, the multi-process soak and chaos harness `crates/hench/examples/soak`
(Unix): worker OS processes (`Hench::run`, real signals) against a private `redis-server`
under minutes of load, with `kill -9`, SIGTERM (once and twice), SIGTSTP, remote quiet/stop,
Redis restarts and `kill -9` (AOF, RDB, no persistence), `CLIENT PAUSE` (up to 70 s), a proxy
that drops, delays or resets Redis's replies, and paused queues; every execution logged outside
Redis, then checked: planned ends and exact attempts, extra and concurrent runs by cause,
clean Redis (in-progress lists, `inprogress`, and `processes` once the drain has waited for the
processes that died to be pruned, about a minute after their heartbeat expired), exact
`stat:*`, live processes recovered as orphans, iterable jobs, scheduler lateness, the dashboard
API, worker resources. The `pro` scenario (`--pro`) adds the four features together: batches of
throttled members that take a window limiter's slot, a fifth of them cancelled, and a unique
periodic schedule ticking every second, under `kill -9`, SIGTERM, Redis restarts and the
reply-dropping proxy. Since round 3 it also pushes unique jobs from the client (six keys,
`success` and `start`, debounces with `replace`, a tenth failing once so that a retry holds the
lock) throttled one at a time per key, and once, 40% into the load, a burst batch of 150 short
members (fifty times the throttle's slots). Its checks (`check_pro.rs`): each tick enqueued at
most once and two ticks' jobs never running at once while a lock could hold; ticks come back
after the faults (`TICKS_STOPPED`: a stretch without a tick may last the poll interval,
`unique_for`, a run and a margin past the recovery of the last fault in it, a fault costing at
most its length and a lease, 15 s in the soak's workers, and a restart that lost the data the
registration check too; the pro scenarios end with a minute without faults, so a schedule that
stopped fails); every batch's counts,
events and callbacks (each fired event's callback ran, no other), but batches a documented data
loss erased (committed before a restart without persistence, or in the second before a `kill
-9` of Redis with AOF `everysec`); the outbox empty; every unique lock expiring within its
`for`; at most the throttle's limit running at once (overages counted by the limit aside); a
unique push accepted only once the holder's lock was released (it started, or succeeded) or
lapsed, but a debounce's replacement; a replaced job never running; two jobs of one key never at
once; every accepted unique job succeeding (faults near the moment excuse a break: locks of
killed jobs lapse, permits of killed processes wait for their lease); and no feature key
without expiry but configuration and index keys. The burst's drain (time and how busy the
throttle's slots were) is measured, not asserted (the drain itself is the rate-limiting lane's
round-3 fix). `ci/soak.sh` runs the scenarios (`--short`: crash, proxy, redis-none,
stale-sched, pro, pro-crash and pro-redis-none for CI; `pro-crash` runs the features under
stopped, remotely stopped and quieted workers, paused queues, `CLIENT PAUSE` and `kill -9` of
Redis, `pro-redis-none` under restarts without persistence); the `Soak` workflow
runs it nightly and by hand, not on every push (minutes per scenario, fixed ports). It found
problems no in-process test had (a stale `processes` set, completed iterable jobs redone,
ambiguous push errors, parked jobs returning only at the scheduler's random polls), each then
reproduced by a test of the suite. A job's death handlers run in its worker after the outcome
write, so a `kill -9` between them skips the handlers: the checker accepts a missing death
record only then, and only for a job its outcome put where it belongs (the morgue for a dead
one).

The plain-job throughput gate (X22 of `docs/design/pro-features.md`): `ci/throughput.sh --base
<rev>` builds `examples/throughput.rs` (no-op jobs pushed first, then one worker with reliable
fetch and 25 slots timed until it ran them all) at the checkout and at `<rev>`, and runs both in
alternation against a fresh local `redis-server`; a change of the medians beyond the run-to-run
noise is a regression to explain (§9k has the last comparison). `--kind unique`, `unique-start`,
`throttled`, `per-key` or `member` times `examples/throughput_features.rs` instead, whose jobs
each use that feature (a throttle that never refuses, so that only its permits cost): a
feature's cost per job, against a base revision that has the features (round 3,
cross-soak-perf-4).

Not applicable (Ruby/Rails specific): ActiveJob adapter, Rails reloader, CurrentAttributes,
profiling (Vernier), generators, `require` loader.
