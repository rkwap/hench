# Unique jobs (deduplication) for hench: design

Status: proposal, 2026-11-01. Scope: `crates/hench` (spine, engine, api, testing), `crates/hench-macros`,
`crates/hench-web`, `crates/hench-server`. Binding context: `docs/DESIGN.md` (data model §2, client §4,
engine §5, middleware §6, api §7, testing §8, dashboard and server §9).

Lua prototypes of the main scripts, exercised against Redis 7.0.15 during this design, are kept in
`/home/user/scratch/design-lab/unique/` (`lib.lua`, `push.lua`, `ack.lua`, `settle.lua`, `start.lua`,
`renew.lua`, `run.sh`). They are original code written for this design.

Cross-feature review (2026-11-01, `/home/user/scratch/design/pro-features.md`, which also holds the
implementation lanes): this document was aligned with the periodic, batches and rate-limiting designs.
Changes: `Job::unique_args` renamed `Job::unique_key` (§2.6.2); lifecycle rows for batch commits,
periodic ticks, throttle parking, over-limit reschedules and batch skips (§2.3); the frozen Lua helper
interface with `hu_check` and `hu_push_one` (§2.8.2); feature-prefixed script names and the shared Lua
prelude (§2.8.1 rule 10); crate-wide canonical JSON and hashing (§2.4); per-minute counter convention and
`keys::unique_minute` (§2.7); restrictions of `Error`/`Replace` in periodic templates and batches (§2.5);
the processor pipeline order (§2.9); testing-mode interplay (§2.12); `HenchWeb::locks`, navigation order
and job-row order (§2.13); `hench stats locks` (§2.14); resolved contracts (§2.18).

Licensing of what follows: Sidekiq Enterprise (closed), Sidekiq OSS (LGPL), the Sidekiq wiki and Faktory
(AGPL) were used for behaviour only. sidekiq-unique-jobs, BullMQ and sidekiq-rs (MIT) and Oban
(Apache-2.0) were read for ideas; no code is adapted from them. If an implementer adapts any of their code
later, it needs a credit comment.

---

## 0. Summary

A unique job holds a **lock** in Redis, keyed by a **digest** of its class, queue and arguments (or a
chosen subset, or an explicit key). While the lock exists, pushing a job with the same digest is a
**duplicate**: by default it is not pushed and the push returns `Ok(None)`, as in Sidekiq. Every lock has a
TTL, so a lock can never outlive its job by more than its TTL.

The main design choice, and the main departure from Sidekiq Enterprise and sidekiq-unique-jobs: **the lock
is taken by the same Lua script that writes the job, and released by the same Lua scripts that write the
job's outcome** (acknowledgement, retry, morgue, push back). It is not a client/server middleware pair
around the existing chains. In hench the client chain runs before the Redis write and the outcome is
written by the fetcher after the server chain returns, so middleware cannot make lock and job atomic; the
references' bug history is largely about exactly those gaps (section 1.6). The existing chains keep
working unchanged: client middleware runs before the lock is taken (a halted or failed push leaves no
lock), and server middleware runs inside the lock's lifetime. Jobs without uniqueness keep using today's
scripts byte for byte.

| Topic | Decision |
|---|---|
| Lock lifetimes | `UniqueUntil::Success` (default: held while the job waits, runs and retries; released on success or death), `Start` (released when `perform` starts; taken back when the job waits again for a retry), `Expired` (held for the TTL whatever happens). No "while executing" lock: runtime mutual exclusion belongs to the rate-limiting design's concurrency limiter. |
| TTL | Mandatory (`unique_for`, whole seconds, at least 1 s). First push: `unique_for` plus the delay of a scheduled job. Retry: extended to cover the retry delay plus `unique_for`. Running `Success` jobs: extended by every heartbeat. No reaper. |
| Digest | `base64url(SHA-256(canonical JSON of ["hench-unique/1", class, queue or null, key]))`, 43 characters; `key` = the job's arguments, a typed subset (`Job::unique_key`, macro `unique_by = [..]`), or an explicit `unique_key`. Computed once at the first push (before client middleware) and stored in the payload, so retries, requeues and the scheduler reuse it. |
| Conflicts | `OnConflict::Drop` (default, `Ok(None)`), `Error` (`Err(Error::DuplicateJob { holder, .. })`), `Replace` (debounce: replaces a holder that is still waiting in the schedule; otherwise like `Drop`). No reschedule or reject strategies. |
| Atomicity | One script pushes a batch and takes its locks; a lost reply retried with the same jid is recognized (`already pushed`), so a unique job is never pushed twice by `with_retry`. Releases compare the holder jid (never release another job's lock). |
| Data model | `unique:<digest>` HASH (holder jid, class, queue, policy, TTL, lock time, schedule score) with `PEXPIRE`; `unique-locks` ZSET index (score = expiry) for the dashboard; `u\|YYYYMMDD\|H:MM` per-minute event counters (duplicates, replaced, lapsed). |
| Operability | `api::UniqueLocks` (list, find, release); dashboard "Locks" tab and a lock row on job pages; `hench locks` CLI; `hench enqueue --unique-for`; JSON API fields; lapse counters tell when `unique_for` is too short. |
| Testing | Fake and inline clients enforce locks in memory by default (per client, so no leakage between tests); `Testing::unique_jobs(false)` turns it off. |

---

## 1. Survey

### 1.1 Sidekiq Enterprise (behaviour from `sidekiq.wiki/Ent-Unique-Jobs.md`, `sidekiq/Ent-Changes.md`)

- **API.** Enabled globally (`Sidekiq::Enterprise.unique!`), then per job `sidekiq_options unique_for:
  10.minutes`; `unique_until: :success` (default) or `:start`; per push `set(unique_for: false)` (bypass) or
  `set(unique_for: 300)`; `sidekiq_unique_context(job)` returns the `[class, queue, args]` context
  (Ent 7.0.3, #5544); `Sidekiq::Enterprise::Unique.locked?(queue, klass, args)` returns the holder jid or nil
  (Ent 1.2.2, #2932).
- **Data model (inferred from behaviour).** One lock per `(class, queue, args)`; the lock value identifies
  the holder jid (Ent 7.0.4 prints it on duplicates, #5736); digest SHA-1, then SHA-256 since Ent 8.0,
  encoded in base64 rather than hex so the locks grew by only about 10%. TTL mandatory, so that the locks
  of a crashed process cannot last forever.
- **Lifetimes.** Scheduled jobs: TTL = delay + `unique_for`. Retries keep the lock "until the retry
  succeeds or the timeout passes"; if it expires while pending retry, a duplicate can be pushed. Automatic
  death removes the lock (death handler, Ent 2.0, #4162); removing a job from the retry set by hand or
  killing it in the Web UI does not. `:start` unlocks right before execution and does not take the lock back
  after an error (motivated by the race in sidekiq#3471: a job that has read its state but not yet unlocked
  makes the push of a needed follow-up job fail).
- **Atomicity.** A client middleware makes an extra Redis call before the push; it is "not protected by
  `reliable_push`". With batches, an error while defining the batch left locks whose jobs were never pushed
  (Batches wiki, sidekiq#3662; Ent 1.7.0 adjusted the middleware). Encryption and uniqueness are mutually
  exclusive (the encrypted argument is always different).
- **Testing.** "Disable uniqueness in testing, this has the potential to cause much head scratching."
- **Lessons in the changelog.** Ent 0.7.1 (#2499): scheduled unique jobs could not be enqueued by the
  scheduler because of their own lock. Ent 0.7.8 (#2658): `unique_for: false` broken. Ent 2.2.1 (#4763):
  negative expiry edge case. Ent 7.0.5 (#5793): an implementation needing Redis 7 had to be reverted.
  Ent 7.0.4: lock format change incompatible with older versions. Ent 7.3.2 (#6460): uniqueness did not
  work with `perform_inline` until the server middleware was also activated in client mode (an execution
  path that bypasses the lock's release path leaks locks).
- **Guidance.** Design jobs so that uniqueness is "best effort, not a 100% guarantee"; keep `unique_for`
  short (minutes) and retries few.

### 1.2 sidekiq-unique-jobs (MIT; `lib/sidekiq_unique_jobs`, v9.0.0.alpha1 with v6-v8 history)

- **Lock types.** `until_executing`, `until_executed`, `until_expired`, `while_executing` (server only,
  separate `:RUN` digest), `until_and_while_executing` (both), `while_executing_reject`.
- **Digest** (`lock_digest.rb`, `lock_args.rb`). `"<lock_prefix>:" + MD5` (legacy) or SHA3-256 (modern,
  #848) of JSON of the sorted top-level hash `{class, queue, lock_args, apartment}`; options
  `lock_args_method` (proc or class method), `unique_across_queues`, `unique_across_workers`; stored in the
  payload as `lock_digest` so later stages reuse it. Only top-level keys are sorted: nested hashes keep
  insertion order.
- **Data model.** v8 kept about 13 keys per lock (queued/primed/locked lists and hashes, info, a changelog
  ZSET...); v9 keeps 2: HASH `<digest>:LOCKED` (field = jid, value = metadata JSON; `lock_limit` holders)
  with `PEXPIRE` when a TTL is set, and the global ZSET `uniquejobs:digests` (score = lock time, or expiry
  for `until_expired`). PRs #922/#923 cut Redis commands per lock lifecycle by 44% then 76.5%.
- **Lua** (`lua/*.lua`). `lock.lua`: `HEXISTS` the jid (re-locking by the holder is idempotent), `HLEN`
  against the limit, `HSET`, `ZADD`, `PEXPIRE`. `unlock.lua`: `HDEL`, then `ZREM` + `UNLINK` when empty;
  `until_expired` never unlocks unless forced. v9 adds its own reliable fetch: `fetch.lua` (`LMOVE` plus a
  check that the job still holds its lock, reported as `uniqueness_lapsed`), `ack.lua` (`LREM` and unlock in
  one script: "lock-aware acknowledge"), requeue keeps locks, `recover.lua` (scans for working lists inside
  Lua, touching undeclared keys).
- **Server side.** `UntilExecuted#execute` re-acquires the lock before running and skips the job if
  another jid holds it (silent drop, reported as `execution_failed`). `UntilExecuting` unlocks before
  running and re-locks on error.
- **Conflicts** (`on_conflict/*`). `log` (drop), `raise`, `reject` (to the dead set), `replace` (scans the
  queue, schedule and retry set for the digest substring, deletes the job and the lock, re-locks), and
  `reschedule` (pushes the duplicate again in 5 s with a `rescheduled` flag that bypasses the lock);
  separate client and server strategies.
- **TTL and orphans.** `lock_ttl` is optional (nil = no expiry), so locks whose job vanished live forever
  unless a **reaper** finds them: a Ruby or Lua reaper that checks each digest against queues, the
  schedule, the retry set and the processes' work hashes, with a timeout, a registration lock and a
  "resurrector". The death handler (`server.rb`) deletes the lock by digest without checking the holder.
- **Lessons** (CHANGELOG issue titles): reapers deleting locks of running or long jobs (#488, #517, #580,
  #738 "Job executed twice when reaper runs", #779, #559 race in reaper); leaked keys and memory bloat
  (#346, #359, #368, #393, #417, #519, #637, #653 "Expired locks remain in zset of digests"); CPU and
  timeouts from heavy scripts (#668 "Slow evalsha causing timeouts", #684 "10X increase in read ops",
  #840 "Read Timeouts and high CPU spike"); NOSCRIPT after failover (#914); retries duplicated
  (#890 "Using `:until_executed`, you can get many duplicates to appear in the RetrySet", fixed by #931
  "preserve until_executed lock on job failure"); reschedule loops (#457 "rescheduled forever", #940
  "prevent infinite recursion when lock is held", #331); replace problems (#597, #635, #778); runtime-lock
  combinations dropping jobs (#788, #862 "Jobs silently fail when lock_ttl is reached and
  until_and_while_executing", #630, #711, #632, #846, #588); scheduled unique jobs never enqueued
  (#1, #15, #93, #334); digests changing across versions (#649, #891); "Scheduled jobs are not unlocked when
  deleted" (#97); testing breakage (#157).

### 1.3 BullMQ (MIT; `src/commands/includes/*deduplicat*.lua`, `docs/gitbook/guide/jobs/deduplication.md`,
`tests/deduplication.test.ts`)

- **Model.** The caller provides `deduplication: { id, ttl?, extend?, replace?, keepLastIfActive? }`. Key
  `<prefix>de:<id>` = STRING holding the winning job id. The check lives **inside the add-job scripts**
  (`addStandardJob`, `addDelayedJob`), atomic with the insert, and emits a `deduplicated` event.
- **Modes.** Simple (no TTL; the key is deleted when the job completes or finally fails, by
  `removeDeduplicationKeyIfNeededOnFinalization`, comparing the id), Throttle (TTL; not deleted on
  completion), Debounce (TTL + `extend` + `replace`: if the holder is still **delayed**, `ZREM` it and add
  the new job; otherwise the new job is deduplicated), keepLastIfActive (while the holder is active, store
  the latest payload in `dn:<id>`, `PERSIST` the key, and enqueue the stored job when the holder
  finishes).
- **Removal.** Removing a job deletes its dedup key if it still holds it; `removeDeduplicationKey(id)` and
  `job.removeDeduplicationKey()` (compare id); `getDeduplicationJobId(id)`.
- **Lessons.** #4768 "remove deduplication key if job key does not longer exist": a TTL-less key whose job
  was lost in an outage blocked every later add, fixed by `recoverStaleDeduplicationKey`; #4956 membership
  check of active jobs had to move to `LPOS` for performance; the `debounce` option was replaced by
  `deduplication` (API churn); deduplication was disallowed on parent flow nodes.

### 1.4 Oban (Apache-2.0; `lib/oban/job.ex`, `lib/oban/engines/basic.ex`, `guides/learning/unique_jobs.md`)

- **Model.** Uniqueness is a query over the jobs table at insert time: `fields` (worker, queue, args,
  meta), `keys` (subset of args or meta), `period` (default 60 s, or infinity), `states` (default: every
  state but cancelled and discarded, so completed jobs count), `timestamp` (`inserted_at` or
  `scheduled_at`). The Basic engine wraps the query and insert in a transaction with
  `pg_try_advisory_xact_lock(hash)`; if that advisory lock is busy, the insert is reported as a conflict.
  `insert_all` does not apply uniqueness in the Basic engine; Pro's Smart engine uses a unique index for
  strong guarantees.
- **Conflicts.** `{:ok, job}` with `conflict?: true` (the existing job, so the caller learns its id);
  `replace: [scheduled: [:scheduled_at, :args, ...]]` updates fields of the existing job by state (the
  debounce recipe). Unique options deep-merge between worker and call (`worker_test.exs` "building a job
  deep merges unique options").
- **Lessons.** CHANGELOG: in the Lite engine, `nil` argument values did not conflict (duplicates) and `1`
  matched `true`, fixed with type-aware comparison; warnings when `states` omit every insertion state. Guide: "uniqueness has no
  bearing on whether jobs are executed concurrently" (a common misunderstanding).

### 1.5 Faktory Enterprise (behaviour from `client/job.go`, `manager/manager.go`) and sidekiq-rs (MIT)

- **Faktory.** `unique_for` (seconds) and `unique_until` (`success` default, `start`) custom fields;
  the server answers a duplicate push with a `NOTUNIQUE` error (error by default). Ent 1.8.0: SHA-1 to
  SHA-256 "to appease linters".
- **sidekiq-rs** (`src/lib.rs` `enqueue_direct`). `SET sidekiq:unique:<queue>:<class>:<sha256(args JSON)>
  "" NX EX ttl`, then separate `SADD` and `LPUSH` (a failure between them leaves a lock without a job); the
  lock is never released (until-expired semantics) and the args JSON is not canonicalized.

### 1.6 Production lessons and how this design answers them

| # | Lesson | Seen in | Hench answer |
|---|---|---|---|
| L1 | Lock taken in a separate step from the push leaves a lock without a job when the push fails or the definition aborts | Ent Batches wiki (#3662), sidekiq-rs, Ent "not protected by reliable_push" | Lock and job are written by one script (section 2.8.2). Client middleware runs before it, so a halted or failed push takes no lock. |
| L2 | Locks without TTL leak forever; reapers that guess liveness delete live locks or run slowly | sidekiq-unique-jobs #417 #393 #368 #488 #517 #580 #738 #779 #559 #637; BullMQ #4768 | TTL mandatory on every lock, set by every script that creates or keeps one; releases are part of the outcome scripts; no reaper. |
| L3 | Index of locks drifts from the locks (expired keys stay listed, memory grows) | sidekiq-unique-jobs #653 #346 #519 | The index score is the expiry; every lock script updates it; expired entries are trimmed (at most 10 per unique push) and the dashboard verifies entries it shows. |
| L4 | The scheduler's re-push conflicts with the job's own lock, so scheduled unique jobs never run | Ent #2499; sidekiq-unique-jobs #1 #15 #93 #334 | Locks are only taken by first pushes (`raw_push`); the scheduler's `MOVE_OUT` and API moves never take locks. A push whose jid already holds the lock counts as already pushed. |
| L5 | Releasing on failure lets duplicates pile up in the retry set | sidekiq-unique-jobs #890, #931 | `Success` locks are kept, and extended, while the job waits for a retry. |
| L6 | Death or deletion releasing another job's lock (force delete by digest) | sidekiq-unique-jobs `DEATH_HANDLER`, `Unlockable.delete!` | Every release compares the holder jid inside Lua; only an operator's explicit force release ignores it. |
| L7 | Duplicates "rescheduled" through a bypass flag loop forever or multiply | sidekiq-unique-jobs #457 #940 #331 #800 | No reschedule strategy. Debounce is `Replace` on a scheduled holder. |
| L8 | Replace that scans whole queues is slow and racy (holder already running) | sidekiq-unique-jobs #597 #635 #778 | `Replace` only touches a holder still waiting in `schedule`, found in O(log n) by its recorded score; otherwise the push is a duplicate. |
| L9 | Runtime locks combined with enqueue locks drop jobs silently | sidekiq-unique-jobs #788 #862 #630 #711 #846 #588 | No while-executing lock here; concurrency control is the rate limiter's job (section 2.18). Hench never skips a fetched job because of its lock. |
| L10 | Digests change across versions or serializers, so duplicates slip through after a deploy | sidekiq-unique-jobs #649 #891; Ent 7.0.4 note | Versioned digest input (`hench-unique/1`), recursive key sorting independent of `serde_json`'s `preserve_order`, golden test vectors, digest stored in the payload. |
| L11 | Heavy scripts and many keys per lock cost CPU and time out | sidekiq-unique-jobs #668 #684 #840 #922 #923 | One HASH and one index entry per lock; no scans in Lua; batches of at most 100 jobs per script; cost measured (section 2.17). |
| L12 | NOSCRIPT after failover | sidekiq-unique-jobs #914 | redis-rs `Script` retries with `EVAL` on `NOSCRIPT` (already the case for every hench script). |
| L13 | Features needing a newer Redis had to be reverted | Ent #5793 | Only Redis 6.2 commands (`ZRANGE ... BYSCORE LIMIT`, `LPOS`, `PEXPIRE` without `GT`, no `SET ... GET`, no script flags). |
| L14 | Negative expiry for jobs scheduled in the past or with skewed clocks | Ent #4763 | TTL = `unique_for + max(0, delay)`, computed in Lua from Redis `TIME`; at least 1 000 ms. |
| L15 | Uniqueness mistaken for concurrency control | Oban guide | Documented prominently; `Start` + concurrency limiter recipe. |
| L16 | Nil or type-confused values collide or not | Oban CHANGELOG | Canonical JSON keeps types (`null`, `1`, `1.0`, `true` all differ) and object members. |
| L17 | Uniqueness breaks test modes, or tests leak locks into each other | Ent wiki, sidekiq-unique-jobs #157 | Locks emulated in memory per testing client. |
| L18 | Duplicate silently dropped with no visibility | Ent #5736, BullMQ `deduplicated` event, sidekiq-unique-jobs reflections | `debug` log with the holder jid, per-class counters, `PushOutcome::Duplicate { holder }`, `Error::DuplicateJob`. |
| L19 | Locks that lapse silently (TTL too short) | sidekiq-unique-jobs v9 `uniqueness_lapsed` | "Lapsed" counter per class when a job reaches a point where it should hold its lock and does not. |
| L20 | SHA-1 or MD5 flagged by audits | Ent 8.0, Faktory Ent 1.8.0, sidekiq-unique-jobs #848 | SHA-256. |

---

## 2. Design

### 2.1 Goals and non-goals

Goals: prevent duplicate *pending* work with strong atomicity in Redis; clear, documented lifetimes; no
orphaned lock longer than its TTL; correct under lost replies, crashes, retries, reliable-fetch recovery
and failover; observable (dashboard, CLI, counters); testable without Redis.

Non-goals: mutual exclusion of running jobs (rate limiting's concurrency limiter); exactly-once execution
(hench is at-least-once); uniqueness across Redis databases or shards (a lock lives in the Redis the job is
pushed to; `Client::with_redis` sharding gives one lock space per shard); interoperability with Sidekiq's
lock format.

### 2.2 Concepts and guarantees

- **Unique job**: a job pushed with `unique_for`. **Digest**: identifies duplicates (section 2.4).
  **Lock**: `unique:<digest>`, held by one jid. **Holder**: the jid in the lock. **Duplicate**: a push whose
  digest is locked by another jid. **Lapse**: a job reaches a point of its lifecycle where it should hold its
  lock and does not (expired, or held by another job).
- Guarantees:
  - G1. Concurrent pushes of the same digest: at most one takes the lock (the push script is atomic).
  - G2. A unique job and its lock are written by the same script: a failed push leaves no lock (the only
    exception, reported, is a lock write failing after the job write, which cannot happen under `OOM`
    because Redis checks memory only before a script's first write: section 2.8.1).
  - G3. A lock is released only by its holder's lifecycle (compare-and-delete on the jid) or by an explicit
    operator action.
  - G4. Every lock has a TTL; after hench loses track of a job (crash with basic fetch, manual `clear`,
    data loss), its lock disappears within `unique_for` plus whatever delay it was covering.
  - G5. A unique push retried after a lost reply with the same jid is not pushed twice.
- Non-guarantees (documented): a lock whose TTL ends before the job runs or finishes lets a duplicate in;
  jobs pushed back by crash recovery or a shutdown do not take a lapsed lock back (v1); a job retried by hand
  from the morgue runs without a lock; uniqueness does not limit concurrency.

### 2.3 Lock lifecycle

Three policies (`UniqueUntil`), with their counterparts:

| Policy | Released | Counterparts | Use |
|---|---|---|---|
| `Success` (default) | when the job succeeds (or server middleware skips it), dies, is discarded, or is deleted or killed through the API | Ent `:success`, sidekiq-unique-jobs `until_executed`, BullMQ simple, Faktory `success` | "at most one pending or running copy" |
| `Start` | when `perform` starts (after server middleware); taken back if free when the job waits again (retry, push back) | Ent `:start`, sidekiq-unique-jobs `until_executing`, Faktory `start` | "at most one waiting copy"; a change during a run triggers one follow-up job (sidekiq#3471) |
| `Expired` | only by its TTL (or an operator) | sidekiq-unique-jobs `until_expired`, BullMQ throttle, Oban `period` | throttling: "at most one per window" |

Transitions (all lock work happens inside the scripts named; "hold" = keep the lock if held by this jid
and extend its TTL to at least the value given (never shorten), else take it if free, else leave it to
its holder; "release" = delete it if held by this jid):

| Event | `Success` | `Start` | `Expired` |
|---|---|---|---|
| First push, immediate or scheduled (`UNIQUE_PUSH`) | take; TTL = `for + max(0, at - now)` | same | same |
| Pushed by a batch commit (`BATCH_COMMIT`, batches design §2.5.3) | take, with `hu_check`/`hu_take` (section 2.8.2); a duplicate, or a jid that already holds the lock, is left out of the batch (`Committed::duplicates`) | same | same |
| Pushed by a periodic tick or "enqueue now" (`FIRE`/`ENQUEUE_NOW`, periodic design §2.10) | take, with `hu_push_one`; a duplicate writes nothing and consumes the tick (FIRE mode `duplicate`) | same | same |
| Push of a jid that already holds the lock | "already pushed": nothing written, reported as pushed | same | same |
| Duplicate | per `on_conflict` | same | same |
| Scheduler enqueues the due job (`MOVE_OUT`) | unchanged | unchanged | unchanged |
| Scheduler: client middleware stops the due job's push (deleted as by the API, `REMOVE_UNIQUE`; a batch member first counted dead, reason `deleted`; round 3) | release | release | - |
| Scheduler: the due job cannot be pushed (`InvalidJob`/`Json`; killed as by the API, `KILL_UNIQUE`, batch accounting first; round 3) | release | release | - |
| Scheduler: any other push error defers the due job a minute (`RESCORE_UNIQUE`; round 3) | extend to `for` + the new delay while held, `at` recorded | same | - |
| Fetch | unchanged | unchanged | unchanged |
| Refused by a throttle before it starts, rescheduled to `schedule` (rate-limiting design §2.6, `PUSH_BACK_UNIQUE`) | hold, TTL = `for` + delay, `at` set | hold (the lock was never released: `perform` did not start) | - |
| `perform` starts (`UNIQUE_RELEASE`, from `PerformEndpoint`, after the batch cancellation check) | - | release | - |
| While running (`UNIQUE_RENEW`, every heartbeat) | extend to at least `for` if still held | - | - |
| Success, or server middleware skipping the job (`ACK_UNIQUE`) | release | release (no-op unless the start release failed) | - |
| Skipped because its batch was cancelled (acknowledged like a success, `ACK_UNIQUE`) | release | release | - |
| Over limit while running, rescheduled to `schedule` (rate-limiting design §2.5, `PUSH_BACK_UNIQUE`) | hold, TTL = `for` + delay, `at` set | hold (takes it back if free) | - |
| Failure, retry scheduled (`SETTLE_UNIQUE`) | hold, TTL = `for + (retry_at - now)` | hold (takes it back if free) | - |
| Failure to the morgue, `retry: false`, `Discard`, `dead: false` (`SETTLE_UNIQUE`) | release | release | - |
| Interrupted (iterable), pushed back (`PUSH_BACK_UNIQUE`) | hold, TTL = `for` (+ delay if scheduled) | hold | - |
| `Shutdown` requeue, hard-shutdown requeue, orphan recovery, own-list reconciliation | unchanged (v1) | unchanged (v1) | unchanged |
| Orphan recovery of a poison pill (recovered more than `max_recoveries` times in 72 hours: settled as a death, `SETTLE_UNIQUE`; round 3) | release | release | - |
| API: delete one job from a queue or set (`REMOVE_UNIQUE`; a batch member is first counted dead in its batch) | release | release | - |
| API: kill one job, `kill_all` (`KILL_UNIQUE`, `UNIQUE_RELEASE_MANY`; batch accounting first) | release | release | - |
| API: retry, add to queue, `retry_all`, `add_all_to_queue` | unchanged | unchanged | unchanged |
| API: one of those moves stopped by client middleware (the job is deleted, `REMOVE_UNIQUE`; round 3) | release | release | - |
| API: `SortedEntry::reschedule` of a job in `schedule` or `retry` (`RESCORE_UNIQUE`; round 3) | extend to `for` beyond the new time while held (never shortened, never taken back once lapsed); `at` recorded in `schedule`, cleared in `retry` | same | unchanged |
| API: `Queue::clear`, `JobSet::clear` | unchanged (locks expire) | unchanged | unchanged |
| Operator release (dashboard, CLI, `UniqueLocks::release`) | release (jid compared) | same | same |

Deviations from Sidekiq Enterprise, deliberate: retries extend the lock instead of letting it expire
(L5); `Start` takes the lock back while a retry waits (sidekiq-unique-jobs behaviour), so "the lock is
held whenever the job waits to run"; killing or deleting a job through the API releases its lock (Ent
does not; sidekiq-unique-jobs and BullMQ do; sidekiq-unique-jobs #97); running `Success` jobs are kept
locked by the heartbeat.

Notes from the cross-feature review (`/home/user/scratch/design/pro-features.md`):

- A job parked in `schedule` by the rate limiter records its score in the lock (`at`), exactly as a
  scheduled first push does, so a later `OnConflict::Replace` push may replace it (it has not run its
  effect yet, or has stopped before it). An iterable job replaced this way leaves its `it-<jid>` state to
  expire (30 days).
- A job revived by hand from the morgue runs without its lock (decision 14); its success finds no lock
  and counts a lapse (`z`). The lapse counter is a signal for operators, not an invariant.
- The order of lock work relative to the other features is fixed by the processor pipeline: batch
  accounting runs before the outcome script (which holds or releases the lock), and the rate limiter's
  permit release, batch dispatch and periodic `RECORD_RUN` run after it.

### 2.4 The digest

```
digest = base64url_nopad(SHA-256(canonical_json(["hench-unique/1", class, queue_or_null, key])))
```

- `class`: the payload's class after normalization. `queue_or_null`: the payload's queue after
  normalization (default queue resolved), or JSON `null` with `unique_across_queues(true)`.
- `key`, first match: the explicit `unique_key` (any JSON value: `"user:42"`, `[42]`, `{"id": 42}`); the
  value returned by `Job::unique_key(&args)` (typed pushes only); the payload's `args` array.
- **Canonical JSON** (hand-written writer, `unique::canonical_json`): no whitespace; `null`, `true`,
  `false`; numbers exactly as `serde_json` writes the `Number` (`1` and `1.0` differ); strings with
  `serde_json`'s escaping; arrays in order; object members sorted by the UTF-8 bytes of their keys
  **recursively, whatever the `Map`'s iteration order** (another crate may enable `serde_json`'s
  `preserve_order` feature through feature unification; a `HashMap` argument then serializes in random
  order). Depth limited to 128 (`Error::InvalidJob` beyond).
- Encoding: base64url without padding, 43 characters (Sidekiq Ent 8.0's reason: smaller than hex, which
  saves about 48 bytes per lock across key and index). Characters `[A-Za-z0-9_-]` only: safe in keys, URLs
  and the dashboard's form keys.
- Computed in `Client::normalize` (after the class, queue and options are resolved, **before client
  middleware**), only if the payload has no `digest` yet, and stored in the payload. Consequences:
  encryption-style middleware cannot break uniqueness (Ent forbids `encrypt` + `unique_for`); middleware
  may replace the digest (for example to add a tenant) or remove uniqueness; retries, `retry_queue`
  changes, push backs and scheduler re-pushes keep the original lock even if the code's `Job::unique_key`
  changed in a deploy.
- A digest supplied in a raw payload (or by middleware) must be 1 to 128 characters of `[A-Za-z0-9_-]`,
  else `Error::InvalidJob`.
- New dependency: `sha2 = "0.10"` (RustCrypto, MIT/Apache-2.0, MSRV well below 1.88). `base64` is already a
  dependency.
- **Shared helpers (cross-feature review).** The canonical JSON writer and the SHA-256/base64url helper
  live in a crate-wide module (`crate::canonical`: `canonical_json(&Value)`, `sha256_b64url(&[u8])`), not
  inside `unique/`: the periodic design hashes its definitions with them (its digests must not depend on
  `serde_json`'s `preserve_order` either) and the rate-limiting design hashes limiter keys longer than
  128 bytes with them. `hench::unique::canonical_json` and `hench::unique::digest` stay the public entry
  points.
- Raw pushes (`Client::push` with a payload) do not know the job type, so they use the arguments or an
  explicit key, never `Job::unique_key`. A raw push and a typed push of a job whose `Job::unique_key` is
  customized have different digests unless both use the same explicit key (documented). A periodic
  template built with `Periodic::job::<J>` evaluates `J::unique_key` when it is defined and stores the
  result as the template's explicit key, so ticks fired by any process use the typed key (periodic design
  §2.3.1).

### 2.5 Conflict strategies (`OnConflict`)

| Strategy | Effect | `perform_async` / `push` | `push_outcome` | Bulk |
|---|---|---|---|---|
| `Drop` (default) | not pushed | `Ok(None)` | `Duplicate { jid, holder }` | `None` for that job |
| `Error` | not pushed | `Err(Error::DuplicateJob { class, digest, holder })` | same error | `None` for that job (a bulk call cannot fail for some jobs only) |
| `Replace` | if the holder still waits in `schedule` at the score recorded in its lock: the new job is written, the holder removed, the lock passed to the new jid with a new TTL; otherwise like `Drop` | `Ok(Some(new_jid))` | `Replaced { jid, replaced }` | per job |

- `Replace` is the debounce recipe: `J::set().delay(30s).on_conflict(OnConflict::Replace)`: every push
  replaces the waiting job, so the job runs 30 s after the last push with the last arguments. A holder that
  is queued, running, retrying, or currently leased by a scheduler (its score moved a minute ahead) is not
  replaceable: the push is a duplicate (BullMQ's rule; Oban updates in place by state). Replacing creates a
  new jid (the old one disappears).
- Not offered: reschedule (L7), reject-to-morgue (noise; use `Error` and log), "extend the window on each
  duplicate" (BullMQ `extend`, possible later for `Expired`), "keep last if active" (BullMQ; `Start`
  approximates it for jobs that read current state, section 2.18).
- Restrictions in the other features (cross-feature review): a periodic template may only use `Drop`
  (`Periodic::validate` refuses `Error` and `Replace`: a poller has no caller to return an error to, and a
  tick must not remove another tick's job). Inside a batch definition, `Replace` is refused when the job
  is pushed to the collecting client (`Error::InvalidJob`, before anything is buffered), `Error` makes the
  whole commit fail with `Error::DuplicateJob` naming the first duplicate (nothing written), and `Drop`
  leaves the duplicate out of the batch (`Committed::duplicates`). On a collecting client every push
  returns a provisional result (`Some(jid)`, `PushOutcome::Pushed`): duplicates are only known at commit.
- Every duplicate is logged at `debug` (`duplicate job not pushed`, fields `class`, `jid`, `holder`,
  `digest`) and counted (section 2.16).

### 2.6 Rust API

#### 2.6.1 New public module `hench::unique` (`crates/hench/src/unique/mod.rs`)

```rust
/// When a unique job's lock is released (see the lifecycle table in the module docs).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum UniqueUntil { #[default] Success, Start, Expired }

/// What a push does when another job holds the lock.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum OnConflict { #[default] Drop, Error, Replace }

/// Uniqueness settings. Every field is optional so that layers (default job options, the job type's
/// options, per-push options) merge field by field, like Sidekiq's `set(unique_for: 300)` keeping the
/// class's `unique_until`. Stored in the payload's `unique` object.
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct Unique {
    pub ttl: Option<Duration>,            // "for": whole seconds, required by the time of the push
    pub until: Option<UniqueUntil>,       // "until": default Success
    pub on_conflict: Option<OnConflict>,  // "on_conflict": push time only, never stored
    pub across_queues: Option<bool>,      // "across_queues": push time only
    pub key: Option<serde_json::Value>,   // "key": push time only (explicit key)
    pub digest: Option<String>,           // "digest": computed at the first push, stored
}
impl Unique {
    pub fn new(ttl: Duration) -> Self;
    #[must_use] pub fn until(self, until: UniqueUntil) -> Self;
    #[must_use] pub fn on_conflict(self, on_conflict: OnConflict) -> Self;
    #[must_use] pub fn across_queues(self, across: bool) -> Self;
    #[must_use] pub fn key(self, key: impl Into<serde_json::Value>) -> Self;
    /// Fields of `overrides` win; unset ones fall through.
    #[must_use] pub fn merge(&self, overrides: &Unique) -> Unique;
}

/// The `unique` option of a job: like `RetryOption`, the same type in `JobOptions` and `JobPayload`.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum UniqueOption {
    /// Not unique, whatever lower layers say (Sidekiq's `unique_for: false`). JSON `false`.
    Disabled,
    /// Unique with these settings. JSON object.
    Enabled(Unique),
}
impl From<Unique> for UniqueOption { .. }
impl From<Duration> for UniqueOption { .. }   // Enabled(Unique::new(d))

/// The digest of a job's lock key (section 2.4). Public so callers can find locks.
pub fn digest(class: &str, queue: Option<&str>, key: &serde_json::Value) -> Result<String>;
/// The canonical JSON used by `digest`.
pub fn canonical_json(value: &serde_json::Value) -> Result<Vec<u8>>;

/// What a push did (see `Client::push_outcome`).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum PushOutcome {
    Pushed { jid: String },
    Replaced { jid: String, replaced: String },
    Duplicate { jid: String, holder: String },
    /// Client middleware stopped the push.
    Stopped,
}
impl PushOutcome {
    /// The jid of the job now in Redis because of this push (`Pushed`, `Replaced`).
    pub fn jid(&self) -> Option<&str>;
}
```

`UniqueUntil`, `OnConflict`, `Unique`, `UniqueOption` and `PushOutcome` are re-exported at the crate root
and the first four in the prelude.

#### 2.6.2 Options, setters, the `Job` trait and the macro

- `JobOptions` (`options.rs`) gains `pub unique: Option<UniqueOption>` and builder methods:
  `unique(impl Into<UniqueOption>)` (whole value), `unique_for(Duration)`, `unique_until(UniqueUntil)`,
  `on_conflict(OnConflict)`, `unique_across_queues(bool)`, `unique_key(impl Into<Value>)` (each merges one
  field into `Enabled`), `not_unique()` (`Disabled`). `merge`, `apply_defaults` and `apply_overrides`
  combine layers: a higher `Disabled` wins; two `Enabled` merge field by field; a higher `Enabled` over a
  lower `Disabled` is the higher one alone.
- The same setters on `JobSetter` (`job.rs`), `JobBuilder` and `BulkPush` (`client.rs`), plus
  `JobSetter::push_outcome(&client, args) -> Result<PushOutcome>` and
  `JobSetter::push_bulk_outcomes(&client, args) -> Result<Vec<PushOutcome>>`.
- `Job` (and `IterableJob`, forwarded by the blanket impl) gains:

  ```rust
  /// The part of the arguments that identifies a duplicate of a unique job (all of them when `None`).
  fn unique_key(args: &Self::Args) -> Option<serde_json::Value> { None }
  ```

  `JobSetter::perform_async` and `perform_bulk` call it (per job) when the merged options enable
  uniqueness and no explicit key was set, and put the result in the payload's `unique.key`. (Named
  `unique_key` rather than Sidekiq-style `unique_args` for symmetry with the rate-limiting design's
  `Job::throttle_key(args)`: both derive a key from the typed arguments, and in both an explicit
  `unique_key(..)`/`throttle_key(..)` setter wins over the trait method.)
- `#[hench::job(...)]` accepts `unique_for = <seconds>` (integer, 1 to 999 999 999),
  `unique_until = "success" | "start" | "expired"`, `on_conflict = "drop" | "error" | "replace"`,
  `unique_across_queues = <bool>`, and `unique_by = [param, ...]` (names of the function's job parameters;
  generates `unique_key` returning the selected arguments, in the order given, as a JSON array). Compile
  errors: unknown option values, unknown or duplicated parameter names in `unique_by`, the context
  parameter in `unique_by`, `unique_for = 0`. On `impl Job` blocks only `crate` is accepted (as today):
  such jobs set uniqueness in `options()` and override `unique_key` by hand.

```rust
#[hench::job(queue = "sync", unique_for = 600, unique_by = [user_id])]
async fn sync_address(ctx: &JobContext, user_id: i64, reason: String) -> Result<(), SyncError> { .. }

sync_address::perform_async(&client, (42, "form".into())).await?;      // Some(jid)
sync_address::perform_async(&client, (42, "retry".into())).await?;     // None: same user_id
sync_address::set().not_unique().perform_async(&client, (42, "x".into())).await?; // bypass

// Debounce: run 30 s after the last change, with the last arguments.
reindex::set().delay(Duration::from_secs(30)).on_conflict(OnConflict::Replace)
    .perform_async(&client, (doc_id, version)).await?;

// Throttle: at most one digest email per user per day.
digest_email::set().unique(Unique::new(Duration::from_secs(86_400)).until(UniqueUntil::Expired))
    .perform_async(&client, (user_id,)).await?;

// Raw push by class name.
let mut payload = JobPayload::new("sync_address", vec![json!(42), json!("cron")]);
payload.unique = Some(Unique::new(Duration::from_secs(600)).key(json!([42])).into());
client.push(payload).await?;
```

#### 2.6.3 Client

- `Client::push_outcome(payload) -> Result<PushOutcome>` and
  `Client::push_bulk_outcomes(bulk) -> Result<Vec<PushOutcome>>`. Existing `push`, `push_bulk`,
  `enqueue*`, `JobExt::*` keep their signatures (`None` for duplicates).
- `Error::DuplicateJob { class: String, digest: String, holder: String }` (new variant of the
  `#[non_exhaustive]` `Error`): "duplicate job: a {class} job with the same unique key is held by
  {holder}".
- `Client::with_unique_jobs(bool) -> Client` and `HenchBuilder::unique_jobs(bool)` (default `true`):
  `false` makes pushes ignore uniqueness (the `unique` field is removed at normalization, no lock). The
  engine always releases and holds locks of jobs that carry one, whatever this switch says, so turning it
  off never strands locks.
- `JobContext::unique() -> Option<&Unique>` (the job's settings as fetched, digest included) and
  `JobContext::release_unique_lock() -> Result<bool>` (releases the job's own lock early, compare on the
  jid; works for every policy; fake and inline clients release in memory). Round 3: the job gives its
  lock up for the rest of its execution, a flag of the execution's own state (no process-wide note):
  its outcome does no lock work and counts no lapse, the heartbeat stops renewing it, and the copy
  continuing the execution (retry, push back, reschedule, morgue) is stored with `"unique": false`.

#### 2.6.4 Payload format

`JobPayload` gains `pub unique: Option<UniqueOption>` (`"unique"` joins `RESERVED_KEYS`). Stored form
(queued, scheduled, retry, dead, in progress):

```json
"unique": {"for": 600, "until": "success", "digest": "rP3m...43 chars"}
```

- Before normalization the object may hold any subset, plus `on_conflict`, `across_queues` and `key`;
  `false` means `Disabled`. Normalization removes `Disabled`, `key` and `across_queues`;
  `Stored::encode` removes `on_conflict`. Stored jobs therefore only carry `for`, `until` and `digest`.
- Reading is lenient (a custom `deserialize_with`): a malformed `unique` value (wrong types, unknown
  `until`) becomes `None` with a `warn` log, so a job is never made unparseable (and sent to the morgue) by
  this field; `for` accepts floats (truncated) like `retry_for`. An unknown `until` written by a newer
  version is read as `Success`.

#### 2.6.5 Normalization (`Client::normalize`)

After the existing steps (options merged, queue resolved, jid and `created_at` set):

1. If the client's `unique_jobs` switch is off, or `unique` is `Disabled`: set `unique = None`; done.
2. If `Enabled(u)`: `u.ttl` is required (`Error::InvalidJob("unique jobs need unique_for")`), at least
   1 s and below 1e9 s (whole seconds stored); `until` defaults to `Success`; `on_conflict` to `Drop`.
3. If `u.digest` is set, validate it (section 2.4); else compute it from `u.key` (or `args`), class, and
   queue (or `null` when `across_queues`).
4. Remove `key` and `across_queues`.

`Client::prepare_push` (scheduler and API moves) runs the same normalization: a job that already has a
digest keeps it, so nothing is recomputed.

#### 2.6.6 `raw_push` (`client.rs`)

`raw_push(payloads) -> Result<Vec<PushResult>>` (crate-internal; `PushResult` = pushed, already pushed,
duplicate (holder), replaced (old jid), pushed without lock (error text)).

- Redis mode: if no job in the batch has a digest, the existing `MULTI` path runs unchanged. Otherwise the
  batch is sent through `UNIQUE_PUSH` in chunks of at most 100 jobs (each chunk is one script run of a few
  milliseconds at most), with `RedisPool::with_retry` as today (safe: a retried chunk finds its jobs'
  locks held by their own jids and reports them as already pushed).
- A per-job write error (for example `WRONGTYPE` on a queue key, `OOM`) fails the call with the first
  error, as the `MULTI` path does today; other jobs of the chunk may have been pushed (unchanged
  semantics).
- "Pushed without lock" (the job was written but its lock write failed) is reported as pushed with a
  `warn` log. Under `OOM` it cannot happen (section 2.8.1).
- Fake and inline modes go through the in-memory store (section 2.12).
- `push_normalized`, `push_bulk_with` and the new outcome methods map results per `on_conflict`
  (section 2.5). In `push_bulk_with` the digest is computed per job after its arguments are assigned
  (typed bulk pushes pass the per-job keys computed from `unique_key`).

### 2.7 Redis data model

| Key (helper in `keys.rs`) | Type | Contents | Expiry |
|---|---|---|---|
| `unique:<digest>` (`keys::unique_lock(digest)`, `UNIQUE_PREFIX`) | HASH | `jid` holder; `class`; `queue`; `until` (`success`/`start`/`expired`); `for` (seconds); `t` lock time (epoch seconds, ms precision, Redis clock); `at` the holder's score in `schedule` (only while it waits there; used by `Replace`) | `PEXPIRE` always: first push `for + max(0, at - now)`; retry `for + (retry_at - now)`; push back `for` (+ delay); running `Success` jobs extended to at least `for` each heartbeat |
| `unique-locks` (`keys::UNIQUE_LOCKS`) | ZSET | member = digest, score = the lock's expiry (epoch seconds, Redis clock) | none; members removed with their lock, and expired members trimmed (10 per unique push, plus by dashboard reads) |
| `u\|YYYYMMDD\|H:MM` (`keys::unique_minute(at)`) | HASH | `<class>\|d` duplicates (dropped or refused), `<class>\|r` replaced holders, `<class>\|z` lapses, for the minute (UTC, client clock, like `metrics_minute`) | `EXPIRE` 8 h (`keys::FEATURE_MINUTE_TTL`, the same value as `METRICS_MINUTE_TTL`) |

Notes: un-namespaced like every hench key; the prefixes `unique:`, `unique-locks` and `u|` are reserved
(the cron, batch and rate-limit designs must not reuse them). The per-minute counter hashes of the four
features follow one convention (cross-feature review): key `<letter>|YYYYMMDD|H:MM` from a
`keys::<feature>_minute(at)` helper (`u` unique, `b` batches, `l` limits; `j` and `h` stay the metrics'),
UTC minute of the writing client's clock, `HINCRBY` under `pcall` as a script's last writes, 8 h expiry,
no ten-minute rollup; readers (`UniqueLocks::events`) accept minute periods of at most 8 hours
(`metrics::Period::Minutes(n)`, `n` ≤ 480; longer periods are `Error::Config`). Measured memory (Redis 7.0.15, 50 000 locks):
lock HASH + index entry with a 43-character digest about 436 bytes per lock; HASH alone 308 bytes (with a
64-character digest); a bare `SET` lock with the jid as value 212 bytes. 100 000 live locks therefore cost
about 44 MB.

### 2.8 Lua scripts (`crates/hench/src/unique/lua.rs`)

#### 2.8.1 Rules every unique script follows

1. **Declared keys only** (Dragonfly and Redis Cluster rules): every key touched is in `KEYS`. No `SCAN`,
   no key built in Lua.
2. **Redis 6.2 commands only** (L13).
3. **Time comes from Redis** (`TIME`), for the TTL arithmetic of scheduled jobs and the index scores, so
   the index agrees with key expiry whatever the clients' clocks. (Scripts with `TIME` followed by writes
   are allowed since Redis 5's effects replication.)
4. **Write before remove**, as every hench script: the job is written before its old copy is removed and
   before its lock is taken. Lock-before-job would be wrong: a lock written and a job write failing would
   make the client's retry of the same jid look "already pushed", losing the job.
5. **Out of memory.** Redis checks `maxmemory` only before a script's *first* write (verified on 7.0.15: a
   script whose first write succeeded keeps writing past the limit; a script whose first write is refused
   gets `OOM` for every write; 6.2 has the same rule, `lua_write_dirty`). So: the first write of each script
   must be the one whose refusal leaves a consistent state (the job's own write), counters run under
   `pcall` (`HINCRBY` is a `denyoom` write and must never abort a script), and the index trim (a `ZREM`,
   which would make the script "dirty" and let later writes bypass the limit) runs **last**.
6. **Compare on the jid** for every release and every extension; only `force_release` deletes by digest.
7. **Never shorten** a TTL (`PTTL` compared before `PEXPIRE`; no `PEXPIRE GT`, which needs Redis 7).
8. **Unique jobs get separate script variants** (`ACK_UNIQUE`, `SETTLE_UNIQUE`, `PUSH_BACK_UNIQUE`,
   `REMOVE_UNIQUE`, `KILL_UNIQUE`); the existing scripts stay byte for byte for every other job.
9. **In engine scripts the lock helper runs under `pcall`, after the outcome write and before the
   removal from the in-progress list**, so an unexpected error (a `unique:<digest>` key of the wrong
   type) can never abort a script between its write and its removal (which would leave a failed job both
   in the retry set and in progress). The helper's failure is returned as the lock status (`-2`) and
   counted as a lapse; the outcome itself is complete.
10. **Conventions shared by the four features** (cross-feature review): scripts live in `.lua` files
    under `crates/hench/src/unique/lua/`, embedded with `include_str!` and assembled by the crate-wide
    script builder after the shared prelude (`crate::lua`); `TIME` is read once per script; generic names
    carry the feature prefix in code and logs, so they never collide with the fetcher's `RELEASE` or the
    rate limiter's scripts: `UNIQUE_PUSH`, `ACK_UNIQUE`, `SETTLE_UNIQUE`, `PUSH_BACK_UNIQUE`,
    `REMOVE_UNIQUE`, `KILL_UNIQUE`, `UNIQUE_RELEASE` (was `RELEASE`), `UNIQUE_LOCK_OP` (was `LOCK_OP`),
    `UNIQUE_RENEW` (was `RENEW`), `UNIQUE_RELEASE_MANY` (was `RELEASE_MANY`). The tables below keep the
    short names.

#### 2.8.2 Shared helpers (prepended to each script; prototype `lib.lua`)

The helpers are a **frozen interface** (cross-feature review): the periodic design's `FIRE` and
`ENQUEUE_NOW` and the batches design's `BATCH_COMMIT` prepend `lib.lua` and call `hu_check`, `hu_take`,
`hu_push_one`, `hu_count` and `hu_trim` with the signatures below. The library file is created in the
common phase of the implementation (from the lab prototype plus `hu_check` and `hu_push_one`); later
changes may alter the bodies, never the names, arguments or return values.

```lua
local function hu_now_ms()
  local t = redis.call('TIME')
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end
local function hu_score(ms) return string.format('%.3f', ms / 1000) end
local function hu_err(e) if type(e) == 'table' and e.err then return e.err end return tostring(e) end

-- Removes up to `limit` index entries whose lock expired before now.
local function hu_trim(index, now_ms, limit)
  local stale = redis.call('ZRANGE', index, '-inf', hu_score(now_ms), 'BYSCORE', 'LIMIT', 0, limit)
  if #stale > 0 then redis.call('ZREM', index, unpack(stale)) end
end

-- Counts d (duplicate), r (replaced) or z (lapsed) for a class. Always called through pcall.
local function hu_count(events, class, event)
  redis.call('HINCRBY', events, class .. '|' .. event, 1)
  redis.call('EXPIRE', events, 28800)
end

-- Creates the lock of `jid`. `fields`: class, queue, until, for; `at` = schedule score or ''.
local function hu_take(lock, index, digest, jid, ttl_ms, now_ms, fields, at)
  redis.call('HSET', lock, 'jid', jid, 't', hu_score(now_ms), unpack(fields))
  if at ~= '' then redis.call('HSET', lock, 'at', at) end
  redis.call('PEXPIRE', lock, ttl_ms)
  redis.call('ZADD', index, hu_score(now_ms + ttl_ms), digest)
end

-- Keeps `jid`'s lock for at least ttl_ms. 1 = held and kept, 2 = was free and taken, 0 = other holder.
local function hu_hold(lock, index, digest, jid, ttl_ms, now_ms, fields, at)
  local holder = redis.call('HGET', lock, 'jid')
  if holder == jid then
    if redis.call('PTTL', lock) < ttl_ms then
      redis.call('PEXPIRE', lock, ttl_ms)
      redis.call('ZADD', index, hu_score(now_ms + ttl_ms), digest)
    end
    if at ~= '' then redis.call('HSET', lock, 'at', at) else redis.call('HDEL', lock, 'at') end
    return 1
  elseif holder then
    return 0
  end
  hu_take(lock, index, digest, jid, ttl_ms, now_ms, fields, at)
  return 2
end

-- Releases `jid`'s lock. 1 = released, 0 = no lock, -1 = another job holds it.
local function hu_release(lock, index, digest, jid)
  local holder = redis.call('HGET', lock, 'jid')
  if holder == jid then
    redis.call('DEL', lock)
    redis.call('ZREM', index, digest)
    return 1
  elseif holder then
    return -1
  end
  redis.call('ZREM', index, digest)   -- tidy a stale index entry
  return 0
end

-- Read only: 'free', 'self' (the lock is held by `jid`) or the holder's jid (24 hex characters, so it
-- cannot be mistaken for the two words). Call it through pcall: a key of the wrong type raises.
local function hu_check(lock, jid)
  local holder = redis.call('HGET', lock, 'jid')
  if not holder then return 'free' end
  if holder == jid then return 'self' end
  return holder
end

-- One push under a lock, for scripts that write one job at a time (UNIQUE_PUSH without `replace`,
-- the periodic design's FIRE and ENQUEUE_NOW). `write` is the caller's closure writing the job; it runs
-- only when the job may be pushed, and before the lock is taken (rule 4). Returns {1} pushed,
-- {2, jid} already pushed (nothing written), {0, holder} duplicate (nothing written, `d` counted),
-- {4, err} written without its lock, {-1, err} the lock key is unreadable (nothing written).
local function hu_push_one(lock, index, events, digest, jid, class, ttl_ms, now_ms, fields, at, write)
  local ok, state = pcall(hu_check, lock, jid)
  if not ok then return {-1, hu_err(state)} end
  if state == 'self' then return {2, jid} end
  if state ~= 'free' then
    pcall(hu_count, events, class, 'd')
    return {0, state}
  end
  write()
  local took, err = pcall(hu_take, lock, index, digest, jid, ttl_ms, now_ms, fields, at)
  if not took then return {4, hu_err(err)} end
  return {1}
end
```

TTL rule used wherever a run-at time `at` (epoch seconds, the score given to `ZADD`) exists:
`ttl = for_ms + max(0, floor(at * 1000) - now_ms)`.

How a multi-job script that must check everything before its first write (the batches design's
`BATCH_COMMIT`, all or nothing) uses the library: in its check phase it calls `hu_check` for every
member that carries a digest and keeps a Lua table `seen[digest] = jid` of the members of the same
commit, so that a second member with the same digest is a duplicate of the first even though no lock is
written yet; `'self'` (a member whose explicit jid already holds the lock: that job is already in Redis,
outside the batch) counts as a duplicate too, since registering it would leave the batch waiting for a
job that never reports to it; with `on_conflict = error` any duplicate refuses the whole commit before
the first write. In its write phase it writes each member, then calls `pcall(hu_take, ...)` (TTL rule),
then registers the jid in the batch; `hu_trim(index, now, 10)` runs last, and `hu_count(.., 'd')` under
`pcall` for each duplicate left out.

Verified by the cross-feature review on redis-server 7.0.15 (`/home/user/scratch/design-lab/pro-features/`,
`check.py`, 31 checks passing): a minimal `BATCH_COMMIT` with this procedure (intra-commit duplicate,
already-held jid, `error` refusing before any write, `WRONGTYPE` lock refused before any write, `OOM`
refused at the first write then applied once memory frees, resend with the same commit id, trim of 10
stale index entries) and a minimal `FIRE` with `hu_push_one` (lock taken with the job, resend a CAS no-op,
`duplicate` consuming the tick without a push, wrong-type lock writing nothing, `duplicate` mode refused
under `OOM` with nothing written). Implementation gotcha found there: `until` is a Lua keyword, so a Lua
table field cannot be named `until` (`u.until` does not compile); scripts keep the policy in a field
named `policy` and write the hash field as the string `'until'`.

#### 2.8.3 `UNIQUE_PUSH` (client; prototype `push.lua`)

- KEYS: `[1]` `queues`, `[2]` `schedule`, `[3]` `unique-locks`, `[4]` events minute hash, then per job
  `i`: `[3 + 2i]` its lock key (`KEYS[3]` again for a job without a lock), `[4 + 2i]` its destination
  (queue list or `schedule`).
- ARGV: `[1]` number of jobs, then 11 values per job: jid, JSON as stored, mode (`queue`/`schedule`),
  target (queue name or score), queue name, until (`''` for a job without a lock), for (ms), at (run-at
  seconds or `''`), on_conflict (`drop`/`error`/`replace`), class, digest.
- Per job: no lock: write (`SADD` + `LPUSH`, or `ZADD`) under `pcall`. With a lock: `HGET jid` (under
  `pcall`: `WRONGTYPE` reports an error for that job); same jid: `{2}` already pushed, nothing written;
  another holder and `replace`: the lock records its holder's member in `schedule` (`m`, the job's JSON,
  written with `at` whenever the holder is written to or parked in the schedule): `ZSCORE schedule m`
  must equal `at`, and the decoded member must carry the holder's jid and no `bid` (round 3: the first
  16 members at the score were decoded instead, and members sharing the score sort by their bytes, so
  with more than 16 jobs at one run time most holders were never found). A lock without `m` (written
  by `BATCH_COMMIT`, or by an older version) is looked for among the first 100 members at its score,
  matched as text before decoding; another holder not
  replaceable: count `d`, `{0, holder}`; otherwise write the job (`pcall`), then (replace) `ZREM` the old
  member, `DEL` the lock, count `r`, then take the lock with `pcall(hu_take, ...)` (TTL rule above; `at` set
  for scheduled jobs). Results: `{1}` pushed, `{2, jid}` already pushed, `{0, holder}` duplicate,
  `{3, old}` replaced, `{4, err}` pushed without lock, `{-1, err}` not pushed. Trim the index last.

#### 2.8.4 Engine scripts

| Script | KEYS | ARGV | Steps | Returns |
|---|---|---|---|---|
| `ACK_UNIQUE` (reliable ack) | in-progress list, lock, index, events | job as fetched, jid, digest, class, until | `LREM inprogress -1 job`; if 0 return; `hu_release`; if `until == success` and status != 1, count `z` | `{acked, status}` |
| `SETTLE_UNIQUE` (reliable failure) | in-progress, retry or dead set, lock, index, events | the six `SETTLE` arguments, then op (`hold`/`release`), jid, digest, class, queue, until, for (ms) | `LPOS` guard; the `SETTLE` write (`ZADD` + trims); `hold` with the TTL rule on the retry score, or `release`; count `z` for `success` when status != 1; `LREM` | `{settled, status}` |
| `PUSH_BACK_UNIQUE` (reliable interruption) | in-progress, `queues`, destination, lock, index, events | the four `PUSH_BACK` arguments, then jid, digest, class, queue, until, for (ms) | `LPOS` guard; write the copy; `hold` (TTL `for` + delay if scheduled; `at` set when it goes to `schedule`); count `z` for `success` when status != 1; `LREM` | `{written, status}` |
| `RELEASE` (start of `Start` jobs; basic-fetch success; early release; operator release) | lock, index, events | jid, digest, class, count lapse (`1`/`0`) | `hu_release`; count `z` if asked and status != 1 | status |
| `LOCK_OP` (basic fetch: after the `MULTI` settle or push back) | lock, index, events | op, jid, digest, class, queue, until, for (ms), at | `hold` or `release` as above | status |
| `RENEW` (heartbeat; prototype `renew.lua`) | index, events, then one lock per running `Success` job | per job: jid, digest, for (ms), class | if `HGET jid` is the job's jid and `PTTL < for`: `PEXPIRE` + `ZADD index`; if not held: count `z`. **Never re-creates a lock**: a heartbeat working from a snapshot taken before the job's acknowledgement must not resurrect a released lock | lapses |
| `REMOVE_UNIQUE` (API delete) | queue list or sorted set, lock, index | `list`/`zset`, member, jid, digest | `LREM key 1 member` or `ZREM`; if removed, `hu_release` | `{removed, status}` |
| `KILL_UNIQUE` (API kill of one job) | source set, morgue, lock, index | `MOVE_TO_MORGUE`'s four arguments, jid, digest | `MOVE_TO_MORGUE`'s steps, then `hu_release` if the job moved | `{moved, status}` |
| `RELEASE_MANY` (after `kill_all` batches; `release_all`) | index, then locks | per lock: jid (`''` = any holder), digest, class filter (`''` = any) | release each lock whose holder (and class) match | count |

All engine scripts keep the existing guarantees: the `LPOS`/`LREM` guard makes a repeated script (lost
reply, `persist` retry) a no-op, and the lock operation sits inside the guard, so it happens once with the
outcome write and never for a job no longer in progress (for example one recovered by another process
after a partition). Basic fetch has no in-progress list: its lock operations run as a separate `LOCK_OP`
or `RELEASE` after the outcome write; they are idempotent (compare on the jid, extend-only).

### 2.9 Engine and spine integration (file by file)

- **`unique/lock.rs`** (crate-internal): `LockRef { digest, key, jid, class, queue, until, ttl }` built by
  `LockRef::from_payload(&JobPayload)` (only for `Enabled` with a valid digest and TTL; anything else means
  "no lock" with a `warn`), `LockOp::{Hold { ttl_ms, at }, Release}`, the script invocations, and the event
  key of the current minute.
- **`processor.rs`**: in `run_job`, compute `lock = LockRef::from_payload(&job)` from the job *as
  fetched* (before middleware may change it) and store it in `Work` (`stats.rs`: new field
  `#[serde(skip)] lock: Option<LockRef>`, so the `<identity>:work` format does not change).
  `acknowledge(work, lock)` uses `Fetcher::acknowledge` with the lock (`ACK_UNIQUE` when the policy
  releases on success, else plain `LREM`). `fail` computes `LockOp` from the settlement (`Retry` -> hold
  with `for + retry delay`, `Morgue`/`Drop` -> release; nothing for `Expired`) and passes it to
  `Fetcher::settle`. `push_back` passes a hold to `Fetcher::push_back`. `requeue` and the shutdown paths are
  unchanged. All writes keep going through `persist` (backoff while Redis is unavailable).
- **`fetch.rs`**: `acknowledge`, `settle` and `push_back` take `lock: Option<&LockOp>` (plus the
  `LockRef`); with `None` they run today's scripts unchanged, with `Some` the `_UNIQUE` variants (reliable)
  or the existing write followed by `LOCK_OP`/`RELEASE` (basic). Recovery, reconciliation, bulk requeue and
  release do not change.
- **`middleware.rs`**: `PerformEndpoint::call`, before `perform_json`, releases the lock of a `Start`
  job (`ctx.payload()` is the payload as fetched; the job's `unique` there is authoritative) through
  `ctx.client()`: `RELEASE` in Redis mode, the in-memory store in testing modes. A failure is logged at
  `warn` and the job runs anyway (the outcome scripts release again).
- **`launcher.rs`** (heartbeat `beat`): after writing the work state, collect the `LockRef`s of running
  `Success` jobs (`Stats::running_locks()`) and run `RENEW` (skipped when empty; errors at `debug`, the
  next beat tries again). A running job's lock then never expires while the process lives, provided
  `unique_for` comfortably exceeds the heartbeat interval (documented: at least three intervals, 30 s with
  the default 10 s heartbeat; the interval is capped at 20 s by `Config::MAX_HEARTBEAT_INTERVAL`).
- **`retry.rs`**, **`scheduled.rs`**, **`manager.rs`**: no change. The scheduler's `MOVE_OUT` never
  touches locks; regression tests cover L4. (Round 3: the scheduler deletes a due job that client
  middleware stops and kills one it cannot push as the API does, releasing their locks, and keeps a
  deferred job's lock for its extra wait; `tests/unique_moves.rs`.)
- **`client.rs`**, **`options.rs`**, **`payload.rs`**, **`job.rs`**, **`error.rs`**, **`keys.rs`**,
  **`config.rs`** (`HenchBuilder::unique_jobs`), **`lib.rs`** (module, re-exports, prelude): as in 2.6.
- **`api/`**: section 2.11. **`testing.rs`**: section 2.12. **`hench-macros`**: section 2.6.2.
- **Heartbeat `info`** gains the crate-wide `"features"` map with `"unique": 1` (the stored format's
  version), so that the dashboard warns while a live process cannot release locks (an older version).

Cross-feature contract (also in section 2.18): any feature that puts a fetched job back to run later
(rate limiting's "over limit", future `Settlement::Reschedule`) must use a lock-preserving outcome (push
back or settle with hold), never "acknowledge and push a copy", which would release the lock.

**Place in the processor pipeline** (cross-feature review; the shared files `processor.rs`, `fetch.rs`,
`middleware.rs`, `launcher.rs` and `stats.rs` are changed once, in the common phase, with hooks that this
design's module fills): (1) the throttle gate (rate limiting) may park the job before anything runs, with
`PUSH_BACK_UNIQUE` holding the lock; (2) in `PerformEndpoint::call` the batch cancellation check runs
first and the `Start` release second, so a skipped member's lock is released by its acknowledgement and
not before; (3) the batch accounting script runs before the outcome script; (4) the outcome script
holds or releases the lock; (5) permit release (rate limiting), batch dispatch and `RECORD_RUN` (periodic)
follow. The heartbeat sends `UNIQUE_RENEW` and the rate limiter's `LIMIT_RENEW` in one pipeline.

### 2.10 Behaviour under failures

| Situation | Behaviour |
|---|---|
| Push: connection dropped after the script ran | `with_retry` resends; the job's own jid holds the lock: "already pushed", no second copy (G5). Residual window: if the job was fetched and finished before the resend, it is pushed again (at-least-once). |
| Push: timeout | not retried (existing rule); the error reaches the caller. The script may have run: a retry with the same jid reports `Pushed`; with a new jid, `Duplicate { holder: first jid }`. |
| Push: `OOM`, failover (`READONLY`) | the first write is refused, nothing is written, no lock; `READONLY` is retried once after reconnecting. |
| Client middleware errors, panics or stops the push | no lock was taken (it is taken after middleware). |
| Process crash while running (reliable fetch) | renewal stops; the lock keeps up to `unique_for`; the job is recovered after its heartbeat expires (60 s) and the next orphan check (`orphan_check_interval`, 60 s by default; 5 min before round 3) and runs again; its success releases the lock if it still holds it. If the lock expired meanwhile, a duplicate may have been pushed; the recovered job's success does not release the duplicate's lock. Guidance: `unique_for` at least 10 min where duplicates after crashes matter. |
| Process crash (basic fetch) | the job is lost; its lock expires within `unique_for`. |
| Hard shutdown | interrupted jobs are requeued with their lock untouched (`Success` keeps protecting them; `Start` runs without one). |
| Lost reply on ack, settle, push back | the repeated script is a no-op (guard); the lock operation is inside the guard. |
| Lost reply on a release | compare-and-delete repeated: no-op. |
| Heartbeat renewal racing with completion | `RENEW` only extends a lock still held by the jid; a released lock is never recreated. |
| Redis failover with asynchronous replication | each script replicates as one transaction of effects: job and lock (or outcome and release) are lost or kept together. |
| Redis restart | without persistence: jobs and locks vanish together. With persistence: locks keep their absolute expiry; expired ones are dropped at load. |
| Eviction | locks are volatile keys and go first under `volatile-*`/`allkeys-*` policies: duplicates. `start` already warns unless `maxmemory-policy noeviction`. |
| Clock skew | TTLs are relative and enforced by Redis. The delay part of a scheduled job's TTL is the run-at time computed by the pushing client minus Redis's `TIME`: a pushing client whose clock is off by Δ lengthens or shortens that part by Δ, exactly as it moves the job's due time for schedulers whose clocks agree with Redis; the `unique_for` part is unaffected. A failover to a primary whose clock differs shifts every expiry by the difference (keep NTP). |
| Mixed versions during a rollout | older workers keep `unique` as an extra key and never release: locks expire by TTL. Deploy workers before clients start pushing unique jobs. |
| A lock key of the wrong type (`WRONGTYPE`) | that job's push fails with the error; the other jobs of the batch proceed. In engine scripts the lock helper runs under `pcall` (rule 9 of 2.8.1): the outcome is still written and the error is counted as a lapse. |

### 2.11 API (`hench::api`)

New `api/unique.rs`, `Api::unique_locks()`:

```rust
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct UniqueLock {
    pub digest: String,
    pub jid: String,
    pub class: String,
    pub queue: String,
    pub until: UniqueUntil,
    pub unique_for: Duration,
    pub locked_at: Option<SystemTime>,
    pub expires_at: Option<SystemTime>,   // from PTTL at read time
    pub scheduled_at: Option<SystemTime>, // `at`, while the holder waits in the schedule
}

impl UniqueLocks {
    pub async fn size(&self) -> Result<u64>;                         // ZCOUNT index (now, +inf]
    pub async fn page(&self, page: usize, size: usize, order: SortOrder) -> Result<Page<UniqueLock>>;
    pub fn entries(&self) -> impl Stream<Item = Result<UniqueLock>> + Send + 'static;
    pub async fn get(&self, digest: &str) -> Result<Option<UniqueLock>>;
    pub async fn find<J: Job>(&self, args: &J::Args) -> Result<Option<UniqueLock>>; // Ent `locked?`
    pub async fn find_in<J: Job>(&self, queue: &str, args: &J::Args) -> Result<Option<UniqueLock>>;
    pub async fn find_for(&self, class: &str, queue: Option<&str>, key: &Value) -> Result<Option<UniqueLock>>;
    pub async fn release(&self, digest: &str, jid: &str) -> Result<bool>;   // compare-and-delete
    pub async fn force_release(&self, digest: &str) -> Result<bool>;
    pub async fn release_all(&self, class: Option<&str>) -> Result<u64>;   // pages of 100, RELEASE_MANY
    /// Removes every index entry whose lock is gone (expired entries by score, in batches of 1000,
    /// plus entries whose key no longer exists). Pushes trim 10 entries each; this is for operators
    /// and tests after bursts.
    pub async fn trim(&self) -> Result<u64>;
    pub async fn events(&self, period: metrics::Period) -> Result<UniqueEvents>;
}
```

- `page` reads the index by score (soonest expiry first by default), pipelines `HGETALL` + `PTTL` per
  digest, skips (and `ZREM`s, bounded) entries whose key is gone, so pages may be short by the number of
  stale entries; it never lists a stale lock. `find*` computes the digest like a push (`find::<J>` uses
  `J::options()` for the queue and `across_queues`, and `J::unique_key`).
- `UniqueEvents { totals, by_class: BTreeMap<String, UniqueEventCounts> }` with `duplicates`, `replaced`,
  `lapsed`, read from the `u|...` minute hashes of the period (one pipelined `HGETALL` per minute, like
  `metrics::Query`). Only minute periods of at most 8 hours exist (`metrics::Period::Minutes(n)`,
  `n` ≤ 480); an hour period is `Error::Config` (the hashes expire after 8 h and have no rollup). The
  reader is the crate-wide feature-counter reader shared with batches and limits.
- `JobRecord::unique() -> Option<UniqueRef { digest, until, unique_for }>` (lenient, from the raw map).
- Changed operations: `JobRecord::delete` (queue job) and `JobSet::delete_by_value` / `SortedEntry::delete`
  use `REMOVE_UNIQUE` for jobs with a releasable lock (still sent once, presence checked on error, as today);
  `SortedEntry::kill` / `DeadSet::move_from` use `KILL_UNIQUE`; `JobSet::kill_all` releases the locks of each
  moved batch with `RELEASE_MANY` (best effort, after the move: the jobs are already in the morgue and
  releases compare jids). Moves to queues (`retry`, `add_to_queue`, `retry_all`, `add_all_to_queue`)
  and `clear` do not touch locks, except that a move whose push client middleware stops deletes the
  job (`REMOVE_UNIQUE`, after the batch accounting of a delete). `SortedEntry::reschedule` of a job in
  `schedule` or `retry` whose lock follows it (`Success`, `Start`) runs `RESCORE_UNIQUE`: `ZADD XX` of
  the new score, then, only if the job holds its lock (`hu_check` is `self`), `hu_hold` with `for` plus
  the new delay (never shortening the lock) and `at` (the new score, with the member) in `schedule`,
  none in `retry`. A lapsed lock is not taken back (another push may already have been accepted) and
  another job's lock is left alone. The scheduler defers a due job whose push failed with the same
  script (round 3: both used to move the job past its lock's expiry, so a duplicate was accepted
  while the job still waited; unique-5).

### 2.12 Testing modes

- `FakeStore` and `InlineExecutor` share an in-memory `UniqueTable` per client (and its clones):
  `Mutex<HashMap<digest, FakeLock { jid, until, ttl, expires_at: Instant, scheduled_at: Option<f64> }>>`.
  The decision logic is a Rust port of the scripts (`unique/fake.rs`), with the same outcomes:
  - push: duplicate / already pushed / replace (a captured scheduled holder is removed from the captured
    jobs) / take;
  - `drain`, `perform_one`, inline execution: `Start` released before `perform`; on success `Success` and
    `Start` released; on failure the job leaves the fake store (no retries in testing modes), so its lock
    is released (except `Expired`);
  - interrupted iterable jobs re-pushed through the testing client keep their lock.
- `Testing::unique_jobs(bool)` (default `true`; `Testing::from_hench` copies the builder's switch).
  Rationale for on: testing clients are per test, so the "head scratching" Ent warns about (locks left by
  earlier tests in a shared Redis) cannot happen, and tests see production behaviour.
- Helpers on `Client` (fake and inline only, panicking otherwise like `jobs::<J>()`):
  `unique_holder::<J>(&args) -> Option<String>`, `unique_holder_for(class, queue, key)`,
  `clear_unique_locks()`.
- `JobExt::perform_inline` pushes nothing and takes no lock (unchanged).
- With the other features (cross-feature review; the testing modes hold one table per feature, shared by
  a client, its clones and its collecting clients):
  - a batch commit on a fake or inline client asks the `UniqueTable` for every member that carries a
    digest, with the same rules as `BATCH_COMMIT` (duplicates and already-held jids left out and listed
    in `Committed::duplicates`, `Error` refusing the whole commit); members drained later release their
    locks after the batch table recorded their outcome, the order the processor follows;
  - `Periodic::enqueue(&client, tick)` on a fake client goes through the table like any push (a tick
    whose previous job still holds the lock is a duplicate: `Ok(None)`);
  - with `TestLimits::InMemory` (rate-limiting design), a captured job refused by a throttle or a limiter
    stays captured with its lock held, as `PUSH_BACK_UNIQUE` does in Redis.
- Default on, while the rate limiter's testing default is off: the rule across the four features is that
  push-time, deterministic behaviour (locks, batch accounting) is enforced by testing clients by default,
  and time-driven behaviour (limits, periodic polling) is not.

### 2.13 Dashboard (`hench-web`)

- `HenchWeb::locks(bool)` (default `true`; named like the tab, and like the other feature switches
  `HenchWeb::periodic`, `batches`, `limits` and `metrics`) shows the **Locks** tab and routes; off, they
  answer 404. Navigation order across the features: Dashboard, Busy, Queues, Retries, Scheduled, Dead,
  Periodic, Batches, Limits, Locks, Metrics, then custom tabs. Notices use the existing flash format
  (`kind.count.failed.subject`) with new kinds (`LocksReleased`).
- `GET /locks` (`?page=`, `?count=`, `?direction=asc|desc` by expiry, `?substr=`): summary
  (`N unique locks.`; "Last hour: D duplicates not pushed, R replaced, L lapsed" with a per-class table of
  the 10 classes with the most duplicates, from `UniqueLocks::events`); rows: checkbox, class, queue,
  policy chip (`until success`/`start`/`expired`), holder jid (link to the lock page), locked (relative
  time), expires (relative time). The filter follows the dashboard's rules (literal text with `*`, matched
  against class, queue and holder jid, over at most 50 000 index entries, summaries saying what was
  searched). "Live" switch like the other lists.
- `POST /locks` (action `release` on selected keys `{digest}.{jid}`, at most 1000; split at the first `.`:
  digests never contain one), `POST /locks/all/release` (unfiltered list only; confirmation "Release all
  1,234 unique locks?"), `GET /locks/{digest}` (every field, the raw hash, TTL; **Holder** section locating
  the holder: running (`WorkSet` scan for the jid), scheduled, retrying or dead (`find_job`), queued
  (`Queue::find_job` on the lock's queue, bounded like the dashboard's searches), or "not found: the lock
  expires in ..."), `POST /locks/{digest}` (release with the `jid` shown, so a lock that changed hands
  is not released; the notice says "gone" then). Flash codes, CSRF, read-only and fetch-metadata checks as
  every other action.
- Job pages (queue rows, retries, scheduled, morgue lists and detail pages): a `unique` chip next to the
  class; on detail pages a **Unique** row: policy, `unique_for`, digest (link to `/locks/{digest}`), and
  status: "lock held by this job", "held by another job (jid link)", or "not held". The feature rows of
  job pages come after the built-in rows and the Iteration row, in one order for every feature: Batch,
  Periodic, Unique, Throttle, Limited; then the host's `job_info` rows.
- Strings in `strings.rs`; no new JavaScript beyond the existing list behaviour.

### 2.14 Server (`hench-server`)

- `hench locks [--class C] [--limit 50] [--json]`: table `CLASS QUEUE POLICY HOLDER EXPIRES`, soonest
  expiry first, names with control characters spelled out (existing rule), then the last hour's counters.
- `hench locks show <digest> [--json]`; `hench locks release <digest> (--jid J | --force)`;
  `hench locks release-all [--class C] --yes`. `hench locks` alone is `hench locks list`, like the other
  feature commands (`hench periodic`, `hench batches`, `hench limits`); `hench stats locks` (also part of
  `hench stats all` when locks exist) prints the lock count and the last hour's counters.
- `hench enqueue ... [--unique-for 10m] [--unique-until success|start|expired] [--unique-key JSON]
  [--unique-across-queues] [--on-conflict drop|error|replace]`: prints the jid; a dropped duplicate prints
  `duplicate of <holder>` and exits 0 (cron-style pushes expect duplicates); `--on-conflict error` exits 1
  with `hench: duplicate job (held by <holder>)`; `--json` prints `{"jid": null, "duplicate_of": ...}`.
- JSON API: `POST /jobs` accepts `unique_for` (seconds), `unique_until`, `unique_key` (any JSON),
  `unique_across_queues`, `on_conflict`; answers 201 `{"jid"}` (plus `"replaced"` when it replaced a job),
  200 `{"jid": null, "duplicate_of": "<holder>"}` for a dropped duplicate, 409 `{"error": "duplicate job",
  "duplicate_of": "<holder>"}` with `on_conflict: "error"`. `GET /locks?class=&limit=`; `DELETE
  /locks/{digest}?jid=` (refused when read-only, like other writes).

### 2.15 Configuration

- Builder: `HenchBuilder::unique_jobs(bool)`; `Testing::unique_jobs(bool)`; `HenchWeb::locks(bool)`;
  `Client::with_unique_jobs(bool)`.
- TOML (`cli` feature): `unique_jobs = true|false`.
- Per job and per push: section 2.6. `default_job_options` may carry unique settings (then every job is
  unique; allowed, documented as rarely wise).
- No global default TTL: `unique_for` must be given (Ent: "a time limit is mandatory"; a hidden default
  would hide the choice).

### 2.16 Metrics and logging

- Counters in `u|YYYYMMDD|H:MM` written by the scripts themselves (no extra round trip; works from web
  processes that have no metrics tracker): `d` duplicates (dropped or refused), `r` replaced holders,
  `z` lapses (`Success`: at retry, success, death, push back or renewal when the job does not hold its
  lock; `Start`: at start when it does not).
- `tracing`: `debug` for duplicates (`duplicate job not pushed`, with `class`, `jid`, `holder`, `digest`),
  replacements and releases; `warn` for "pushed without its unique lock", for invalid `unique` fields read
  from Redis, and for lapses, rate-limited to one notice per class per minute in a process (the engine's
  outage notices use the same pattern).
- Dashboard and `hench locks` show the counters; `api::UniqueLocks::events` exposes them.

### 2.17 Performance

Measured with `redis-benchmark` (20 clients, Redis 7.0.15, scratch VM), prototypes from the lab:

| Operation | Throughput (one Redis core) |
|---|---|
| Plain push as a script (`SADD` + `LPUSH`) | about 100 000/s |
| `UNIQUE_PUSH`, lock taken | about 29 000/s (about 32 000/s without the index trim) |
| `UNIQUE_PUSH`, duplicate | about 32 000/s |
| Bare `SET NX PX` lock + push (lean alternative) | about 1.3x the plain push's cost |

So a unique push costs about 3 to 3.5 times the Redis CPU of a plain push (roughly 35 µs against 10 µs
here); pushes of jobs without uniqueness are unchanged (they keep the `MULTI` path). A chunk of 100 unique
jobs runs for about 3 ms. Engine scripts add one `HGET` and a `DEL`/`PEXPIRE` to an outcome write that
already runs a script. The heartbeat adds one `RENEW` per beat per process when `Success` jobs run.

### 2.18 Cross-feature contracts

Resolved by the cross-feature review (`/home/user/scratch/design/pro-features.md`); the other designs
state the same rules.

- **Batches.** Jobs added to a batch are pushed by `BATCH_COMMIT` at commit time, never with a lock
  taken earlier (Ent #3662). The commit prepends this design's `lib.lua` and follows the procedure of
  section 2.8.2 (`hu_check` with a per-commit `seen` table in its check phase, `hu_take` after each
  member's write, `hu_trim` last). A duplicate (or a member whose explicit jid already holds the lock) is
  neither pushed nor registered and is listed in `Committed::duplicates`; its holder belongs to another
  batch or none. `OnConflict::Error` refuses the whole commit with `Error::DuplicateJob` before any
  write; `OnConflict::Replace` is refused when the job is pushed to the collecting client. Callback
  templates are normalized with uniqueness removed (`unique` stripped): a callback is pushed once per
  event by construction, and a template carrying a digest without a lock would count a lapse at every
  acknowledgement and renewal. Batch accounting runs before this design's outcome scripts.
- **Rate limiting.** Its concurrency limiter is the answer to "while executing" locks. Recipe for
  "at most one waiting and one running copy, never two running": `Start` + a concurrency limit of 1 keyed by
  the same arguments. A job refused by a throttle (before `perform`) or over a limiter's limit (while
  running) is rescheduled with `PUSH_BACK_UNIQUE` holding the lock with TTL `for` + delay and `at` set, so
  it stays protected while parked and a debounce push can replace it.
- **Periodic jobs.** Each tick is enqueued once by its own compare-and-set (that design's concern).
  `FIRE` and `ENQUEUE_NOW` prepend `lib.lua` and call `hu_push_one` inside their compare-and-set; a
  duplicate consumes the tick (mode `duplicate`, counted `d` here and `skipped` there). Users add
  `unique_for` (about one period, `until = success`) to skip ticks while the previous run is pending or
  running, which prevents pile-ups. `Periodic::job::<J>` stores `J::unique_key(&args)` as the template's
  explicit key; `Periodic::validate` refuses `on_conflict` other than `drop`.
- **Shared infrastructure.** Canonical JSON and SHA-256/base64url (`crate::canonical`), the Lua prelude
  and script builder (`crate::lua`), the per-minute counter convention (section 2.7), the heartbeat
  `"features"` map, and the testing-tables container are built once for the four features.

### 2.19 Documentation to update with the implementation

- `docs/DESIGN.md`: §2 keys table (three rows) and payload table (`unique`); §3 macro options and
  `Job::unique_key`; §4 `push_outcome`, `Error::DuplicateJob`, `unique_jobs`; §5 processor (outcome
  scripts with locks), heartbeat (`UNIQUE_RENEW`), endpoint release; §7 `UniqueLocks` and the changed
  delete/kill operations; §8 testing; §9 dashboard routes and server commands; new §9g "Unique jobs" (this
  design, condensed; the cross-feature review numbers the sections §9g unique, §9h periodic, §9i batches,
  §9j rate limiting, §9k the features together); §9a ownership (lane A, "periodic and unique", owns
  `unique/`, `api/unique.rs` and the unique pages and commands; the shared files are changed in the
  common phase); §10 test plan rows.
- `README.md`: a "Unique jobs" section: the three policies with the problem each solves, choosing
  `unique_for` (at least the worst queue latency; at least 10 min where crash duplicates matter; at least
  30 s for long jobs), uniqueness is not concurrency control, debounce and throttle recipes, what the
  dashboard shows, the lapse counter.
- `docs/STATUS.md`: feature lines; remove "unique jobs" from the out-of-scope list.

### 2.20 Implementation plan (each step merges with its tests)

1. `hench::unique` types, canonical JSON and digest (unit tests, golden vectors), payload field with
   lenient serde, `JobOptions` merging, setters, `Job::unique_key`, normalization, `Error::DuplicateJob`,
   `PushOutcome`, keys.
2. Lua library and `UNIQUE_PUSH`, `raw_push` routing and results, `push_outcome`, `with_unique_jobs`;
   client integration tests.
3. Engine: `LockRef`, `ACK_UNIQUE`, `SETTLE_UNIQUE`, `PUSH_BACK_UNIQUE`, `LOCK_OP`, `RELEASE` at the
   endpoint, `RENEW` in the heartbeat; engine and chaos tests.
4. API: `UniqueLocks`, `REMOVE_UNIQUE`, `KILL_UNIQUE`, `RELEASE_MANY`, record accessors; API tests.
5. Testing modes: `UniqueTable`, parity tests against Redis.
6. Macro options and `unique_by`; trybuild cases.
7. Dashboard pages and job rows; web tests.
8. Server: `hench locks`, `enqueue` flags, JSON API; tests.
9. Docs (2.19), STATUS, full gate.

Placement in the combined plan (cross-feature review, `pro-features.md` §6): the shared parts of steps
1 to 3 (payload field, option and setters, `Error::DuplicateJob`, keys, `raw_push` returning per-job
results, the hook points in `client.rs`, `fetch.rs`, `processor.rs`, `middleware.rs`, `launcher.rs`,
`stats.rs`, `testing.rs` and `api/`, and the frozen `lib.lua`) are done once in the common phase by the
integrator; lane A then implements everything inside `unique/`, `api/unique.rs`, the unique macro options
file, the Locks pages and the `hench locks` commands, before the periodic work that depends on the
library (`FIRE`'s lock step).

---

## 3. Test plan

Unit tests run without Redis; integration tests use `TestRedis` (a private `redis-server` per test) in new
binaries `tests/unique.rs` (push side), `tests/unique_engine.rs` (real workers), `tests/unique_api.rs`,
`tests/unique_testing.rs`, `hench-web/tests/locks.rs`, and additions to `hench-server/tests/{cli,http}.rs`.
"Port" names the reference scenario re-expressed (behaviour, not code).

### 3.1 Digest and options (unit)

| ID | Scenario | Port |
|---|---|---|
| D1 | Same class, queue, args: same digest; any argument change: different | sidekiq-unique-jobs `lock_digest_spec` "same/different unique args" |
| D2 | Object members in different insertion orders (`Map` built both ways, with and without `preserve_order` semantics) give the same digest | L10 |
| D3 | `null`, `1`, `1.0`, `true`, `"1"` all give different digests; `[null]` differs from `[]` | Oban "nil arg values", "1 vs true" |
| D4 | `unique_across_queues` ignores the queue; default includes it | sidekiq-unique-jobs `unique_across_queues` |
| D5 | Explicit key overrides args; typed `unique_key` (macro `unique_by`) selects arguments in the given order | Ent `sidekiq_unique_context`; Oban `keys` |
| D6 | Golden vectors: five fixed inputs with their expected 43-character digests (pinned forever) | L10 |
| D7 | Depth over 128 is `InvalidJob`; digest validation of supplied digests (length, characters) | |
| D8 | Option layering: class `{for 600, until start}` + push `unique_for(300)` keeps `start` | Oban "deep merges unique options", Ent `set(unique_for: 300)` |
| D9 | `not_unique()` overrides class options; `Enabled` above `Disabled` | Ent #2658 |
| D10 | Normalization errors: missing TTL, 0 s, at least 1e9 s | |
| D11 | Payload JSON: stored shape (`for`, `until`, `digest` only); `Stored::encode` strips `on_conflict`; lenient read of malformed or future `unique` values (job still parses) | Ent 7.0.4 format note |
| D12 | proptest: digest is a function of the canonical form (random JSON values, shuffled object orders) | |

### 3.2 Push side (Redis)

| ID | Scenario | Port |
|---|---|---|
| P1 | First push takes the lock (HASH fields, PTTL near `for`, index score near expiry); identical push returns `None`; `push_outcome` returns `Duplicate { holder }` | Ent wiki; sidekiq-unique-jobs `middleware/client/call_spec` |
| P2 | Different args, or different queue: pushed | Ent wiki "(class, args, queue)" |
| P3 | Scheduled push: PTTL near delay + `for`; an immediate identical push while it waits is a duplicate | Ent wiki "Scheduled Jobs" |
| P4 | Same jid pushed twice (simulated lost reply): second reports pushed, one copy in the queue | G5 |
| P5 | Bulk push with duplicates inside one batch and across batches: first wins, `None` for the rest; with `Error` strategy still `None`; more than 100 unique jobs span several script chunks | Oban `insert_all` limitation |
| P6 | `Error` strategy: `Err(Error::DuplicateJob { holder })` | sidekiq-unique-jobs `raise`; Faktory `NOTUNIQUE` |
| P7 | `Replace`: holder scheduled -> replaced (old member gone, new present, lock jid and TTL updated, `Replaced { replaced }`); holder queued -> duplicate; holder leased (score moved ahead) -> duplicate; holder in the retry set -> duplicate | BullMQ "removes last job if it is in delayed state"; Oban "replacing fields on unique conflict" |
| P8 | Client middleware stopping or failing a unique push: no lock left | Ent #3662 (L1) |
| P9 | Middleware changing args after normalization does not change the digest; middleware removing `unique` pushes without a lock; middleware replacing the digest is honoured | |
| P10 | `with_unique_jobs(false)`: pushed without lock, `unique` absent from the stored job | |
| P11 | `OOM` (`CONFIG SET maxmemory`): push fails, no lock; after memory is freed the same push succeeds and is not reported as already pushed | 2.8.1 |
| P12 | `WRONGTYPE` at `unique:<digest>`: that job fails, others in the batch are pushed | |
| P13 | Index trim: expired entries disappear on later pushes; never more than 10 per push | L3 |
| P14 | Events hash: `d` and `r` counted per class and minute, 8 h expiry | L18 |

### 3.3 Engine (real workers)

| ID | Scenario | Port |
|---|---|---|
| E1 | `Success`: lock held while queued and running (a push from inside the job is a duplicate), released after success, index entry gone | sidekiq-unique-jobs `until_executed_spec` "unlocks after executing" |
| E2 | `Success` + failure: lock kept with PTTL at least the retry delay + `for`; duplicates rejected while it waits; retry succeeds -> released | `until_executed_spec` "keeps the lock ... while in retry set", "prevents process_two from locking"; #890/#931 |
| E3 | `retry: false` failure, exhausted retries (morgue), `Exhausted::Discard`, `dead: false`: released | Ent #4162 |
| E4 | `Start`: released when `perform` starts (a push from inside the job succeeds); failure -> retry takes it back if free; if a duplicate took it meanwhile, both run | `until_executing_spec` "unlocks before executing", "locks the job again" |
| E5 | `Expired`: lock survives success and death, expires on time, never extended by retries | `until_expired_spec` "keeps lock after executing"; BullMQ throttle |
| E6 | Scheduled unique job is enqueued by the scheduler despite its own lock and runs once | Ent #2499; sidekiq-unique-jobs #1 #15 #93 (L4) |
| E7 | Interrupted iterable job pushed back keeps its lock (`Success`) or takes it back (`Start`) | |
| E8 | Server middleware skipping a job: acknowledged and released | |
| E9 | Unknown class: retried with the lock kept | |
| E10 | Long job (runs three times `unique_for`, heartbeat interval shortened): PTTL never reaches 0 while it runs; after success the lock is gone (renewal never resurrects it) | L19 |
| E11 | Lapse: `unique_for` 1 s, queue paused 3 s: lock expires; a duplicate pushed meanwhile holds it; the first job's success does not release the duplicate's lock; `z` counted | sidekiq-unique-jobs death handler force delete (L6) |
| E12 | Basic fetch: the same lifecycle with the non-atomic lock operations | |
| E13 | Repeated `ACK_UNIQUE`, `SETTLE_UNIQUE`, `PUSH_BACK_UNIQUE` (called twice directly): second is a no-op, a new holder's lock untouched | |

### 3.4 Failures and concurrency

| ID | Scenario |
|---|---|
| C1 | Crash mid-job (`Launcher::abandon`, reliable fetch): duplicates rejected while the lock lives; the job is recovered and run; its success releases the lock. With `unique_for` shorter than the recovery delay: a duplicate is accepted and both run (documented behaviour asserted). |
| C2 | Hard shutdown (`timeout` short, job sleeping): requeued with its `Success` lock; the next process runs it and releases. |
| C3 | Redis restart mid-run (`TestRedis::restart`): pushes and releases resume. |
| C4 | Contention: 16 tasks on 4 clients push the same 50 digests 200 times each, concurrently with 2 worker processes running them: at no sampled instant are there two `Success` jobs of one digest in `queue ∪ schedule ∪ retry ∪ in-progress` (sampling with a `MULTI` snapshot, tolerance for transitions documented in the test), and every digest ends unlocked. |
| C5 | Concurrent `Replace` pushes on one digest with a delay: exactly one scheduled job remains, holding the lock. |
| C6 | Model-based proptest: random sequences of push / push scheduled / succeed / fail to retry / die / delete via API / release / advance time (short TTLs) against a pure Rust model of the lifecycle table; after each step the Redis lock holder equals the model's. The same sequences run against the fake store (parity). |
| C7 | Soak: add unique jobs to the existing soak harness (`scratch/soak-chaos` style) with process kills and Redis restarts. Invariants: no `unique:*` key without a TTL (`PTTL` never -1); no lock older than its job's lifetime plus `unique_for`; no job lost; after the run and `UniqueLocks::trim()`, `unique-locks` holds exactly the digests whose key exists. |

### 3.5 API, dashboard, CLI

| ID | Scenario | Port |
|---|---|---|
| A1 | Deleting a queued, scheduled, retrying job through the API releases its lock; deleting a job whose lock another jid holds does not | sidekiq-unique-jobs `sidekiq/api_spec` "deletes uniqueness lock on delete"; #97; BullMQ "when removing deduplicated job" and "does not remove deduplication key" for finished jobs |
| A2 | `kill` and `kill_all` release; `retry`, `add_to_queue`, `retry_all` move the job and keep the lock | sidekiq-unique-jobs `retry_set_spec` "can be put back on queue" |
| A3 | `clear` keeps locks (they expire) | deliberate deviation from sidekiq-unique-jobs `UniqueExtension#clear` |
| A4 | `UniqueLocks`: size, pages ordered by expiry, stale entries skipped, `find::<J>(args)` returns the holder, `release` compares the jid, `force_release`, `release_all(Some(class))`, `events` | Ent `locked?`; BullMQ `getDeduplicationJobId`, `removeDeduplicationKey` (with and without matching id) |
| W1 | `/locks` renders summary, rows, filter summaries, paging; read-only mode has no forms; release (single, bulk, all) with CSRF; a lock that changed hands is reported gone; 404 when `locks(false)` | web_test patterns |
| W2 | Lock page locates the holder in each place (running, queue, schedule, retry, dead, nowhere) | |
| W3 | Job detail "Unique" row: held by this job / another job / not held | |
| S1 | `hench locks` table and `--json`; `show`; `release` refuses without `--jid` or `--force` | sidekiqmon_test style |
| S2 | `hench enqueue --unique-for` duplicate output and exit codes; JSON API 201/200/409 and validation of the new fields | |

### 3.6 Testing modes and macro

| ID | Scenario |
|---|---|
| T1 | Fake client: duplicate push returns `None`; `drain` releases `Success` locks; `Start` released before `perform`; `Expired` persists; failure releases (no retries in testing); `Testing::unique_jobs(false)` disables; `unique_holder::<J>` |
| T2 | Inline client: a job pushing itself from inside `perform` (`Success`: duplicate; `Start`: runs) |
| T3 | Parity: the P1-P7 and E1-E5 scenario table run against both the fake store and Redis with identical expectations |
| M1 | trybuild pass: every option; fail: `unique_for = 0`, `unique_until = "done"`, `on_conflict = "skip"`, `unique_by = [nope]`, duplicated name, context parameter in `unique_by`, string `unique_for` |

---

## 4. Decisions for the owner (with recommendations)

1. **Where locks are taken and released.** In the push script and the outcome scripts (recommended), or
   as a client/server middleware pair as in Sidekiq Enterprise and sidekiq-unique-jobs (simpler to bolt
   on, but leaves a lock without a job when the push fails, and a released lock with an unacknowledged
   job; L1, L5, L6). The recommendation touches spine and engine code, mitigated by separate script
   variants.
2. **Policies offered.** `Success` (default), `Start`, `Expired`; no while-executing lock (recommended:
   delegate to rate limiting; L9).
3. **Retries.** Keep and extend the lock over retry delays (recommended; Ent lets it expire and documents
   duplicates).
4. **Renewal while running** through the heartbeat (recommended; removes Ent's "long job loses its lock"
   caveat at the cost of one script per beat).
5. **API deletes and kills release locks** (recommended; Ent keeps them, sidekiq-unique-jobs and BullMQ
   release). `clear` does not (cost); `UniqueLocks::release_all` exists for that.
6. **Conflict strategies.** `Drop` default, `Error`, `Replace` limited to scheduled holders (recommended);
   no reschedule, reject or "keep last if active" in v1.
7. **Lock record.** HASH + index ZSET (recommended: about 436 B and about 3x the push CPU, but the dashboard
   can list and explain locks) or a lean `SET` with the jid (212 B, 1.3x, no listing without `SCAN`). A
   later `lean` switch is possible if users hit memory limits with `Expired` windows over millions of keys.
8. **Digest.** SHA-256 with the new `sha2` dependency, base64url, versioned input (recommended); the
   alternative without a new dependency is `sha1_smol` (already in the tree through redis-rs) at the cost
   of audit findings (L20).
9. **Testing modes enforce locks by default** (recommended), unlike Ent's advice.
10. **Default conflict result** `Ok(None)` like Sidekiq (recommended) rather than an error like Faktory.
11. **Digest computed before client middleware** (recommended), so middleware that rewrites arguments
    (encryption) does not break uniqueness.
12. **Payload shape**: one nested `unique` object (recommended) rather than flat `unique_for`/
    `unique_until`/`unique_digest` keys; Ruby interop is not a goal.
13. **Crash and shutdown requeues do not touch locks in v1** (recommended for scope). Phase 2 option: pass
    lock holds to the requeue scripts (recovery already parses jobs), so a recovered job takes a lapsed
    lock back.
14. **Retrying a dead job by hand does not take a lock** (recommended for v1).
15. **`unique_for` is mandatory, no global default TTL** (recommended).
16. **Event counters in a per-minute hash written by scripts** (recommended) rather than only logs.

## 5. Risks

1. **Regression in hot engine paths.** New scripts beside `SETTLE`/`PUSH_BACK`/ack. Mitigation: variants
   only for unique jobs, existing scripts unchanged, E13 and C4-C7, review by the engine lane.
2. **Cost.** About 3x Redis CPU per unique push and about 436 B per lock; large `Expired` key spaces
   (for example a day-long window per user for millions of users) need hundreds of MB. Mitigation:
   documented numbers, decision 7.
3. **TTL misconfiguration.** Too short: duplicates (lapse counter, dashboard summary). Too long: a lock
   leaked by a lost job (basic fetch crash, `clear`) blocks pushes until it expires (dashboard and CLI
   release, `release_all`).
4. **Semantics surprises.** `Replace` only replaces scheduled holders; `Start` allows a waiting copy beside
   a running one; uniqueness is not concurrency control. Mitigation: README recipes and the policy table.
5. **Rollouts with mixed versions.** Old workers never release (TTL bounds it); new `until` values read by
   old versions fall back to `Success` (lenient read).
6. **`unique` becomes a reserved payload key.** An application already using a custom `unique` extra key
   would see it reinterpreted (lenient read turns a foreign shape into "not unique" with a warning). Release
   note needed.
7. **`serde_json` features.** Key order is handled; `arbitrary_precision` (if enabled in an application's
   build) renders raw-JSON numbers as written (`1.50`), which can change digests of raw pushes between
   builds. Golden vectors in CI catch changes in hench's own build.
8. **Cross-feature contracts** (2.18) must be honoured by the batch, rate-limit and cron designs.
9. **Dragonfly and Valkey.** Scripts declare their keys and use `TIME`; untested on Dragonfly. Add Valkey
   to CI if available.
10. **Script blocking.** A 100-job chunk runs about 3 ms; `Replace` finds the holder with one `ZSCORE`
    (at most 100 text matches for a lock written without the holder's member).
11. **Clock skew and failover** shift TTLs by the skew (documented; NTP).
