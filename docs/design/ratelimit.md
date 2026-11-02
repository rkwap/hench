# Rate limiting and throttling for hench: design

Status: proposal, 2026-11-01. Baseline: `implement-hench` at `3a10c3a` (DESIGN.md §0–§10). Scope:
`crates/hench` (spine, engine, api, testing, iterable, metrics), `crates/hench-macros`, `crates/hench-web`,
`crates/hench-server`. Companion designs written in parallel: unique jobs
(`/home/user/scratch/design/unique.md`), periodic jobs (`/home/user/scratch/design/periodic.md`), batches
(`/home/user/scratch/design/batches.md`); their cross-feature contracts are honoured in §2.12. Nothing in `/home/user/hench` was changed.

Lua prototypes of every script of §2.9, a deterministic scenario suite (52 checks, all passing) and a
benchmark script, exercised against `redis-server` 7.0.15 on a unix socket, are kept in
`/home/user/scratch/design-lab/ratelimit/` (`lib.lua`, `acquire.lua`, `release.lua`, `renew.lua`,
`adjust.lua`, `usage.lua`, `fetch_gate.lua`, `scenarios.py`, `resp.py`, `bench.sh`, `start.sh`). They are
original code written for this design (Appendix A has the results).

Cross-feature review (2026-11-01, `/home/user/scratch/design/pro-features.md`, which also holds the
implementation lanes): aligned with the unique, periodic and batches designs. Changes: feature-prefixed
script names, the shared Lua prelude and the `lua-replicate-commands` check moved to common start-up
checks (§2.9.1); `keys::limit_minute` and the per-minute counter convention (§2.8); long keys hashed
with the crate-wide SHA-256 helper (§2.4.1, §2.8); throttle refusals of members of cancelled batches
skip the member instead of parking it, and `RECORD_RUN` statuses for periodic jobs (§2.5, §2.6, §2.12);
`Outcome::Skipped` next to `OverLimit`/`Throttled` in metrics (§2.8, §2.18); testing-mode interplay
(§2.14); navigation order, flash kinds, job-row order and metrics columns (§2.15); `hench enqueue
--throttle`, `POST /jobs` throttle fields and `hench stats limits` (§2.16); `periodic` as a reserved
TOML name (§2.17); heartbeat `"features"` map (§2.10); DESIGN section §9j (§2.20).

Licensing of what follows: Sidekiq Enterprise (closed), the Sidekiq wiki, Sidekiq OSS (LGPL-3.0) and
Faktory Enterprise were used for behaviour only; nothing below quotes or adapts their text or code.
sidekiq-throttled, BullMQ and sidekiq-rs are MIT and Oban is Apache-2.0: they were read for behaviour,
algorithms and test scenarios; no code is adapted from them. If an implementer later adapts any of their
code, it needs a credit comment.

---

## 0. Summary

Hench gets rate limiting at three levels, sharing one Redis data model and one set of Lua scripts:

1. **Limiters** (`hench::limit::Limiter`): a library API usable inside jobs or around any code, in any
   process that has a `Client` or `RedisPool`. Kinds: **concurrent** (at most N holders, leases that are
   renewed while held and expire after a crash), **window** (at most N in any sliding period), **bucket**
   (at most N per calendar-aligned UTC interval), **leaky** (burst N, then a steady drip; GCRA), **points**
   (leaky bucket with weighted costs and after-the-fact correction), **unlimited** (tests, exemptions), and
   **all of** several limits at once (atomic, all or nothing). `acquire` returns a `Permit` or
   `Error::OverLimit(OverLimit)` with a `retry_after` hint (exact for rate limits, an estimate from
   measured hold times for concurrency). Returned from a job (`?`), an `OverLimit`
   makes the processor **reschedule** the job (same jid, as fetched, into `schedule`) instead of failing
   it: no retry consumed, no error reported, `limited.n` counted in the payload, and after 20
   reschedules (configurable) the retry system takes over, as in Sidekiq Enterprise.
2. **Throttles** (declarative, per job class): `#[hench::job(throttle = "stripe", throttle_by = [account])]`
   or `JobOptions::throttle(..)` names a `Throttle` registered on the builder (or the TOML file), made of
   global and per-key limits. The processor checks it **right after the fetch, before any middleware**;
   a job over its throttle is parked in `schedule` at its own slot in a **wait line** (spaced at the
   limit's rate), so it neither runs, nor busy-loops, nor holds a processor, nor blocks the jobs behind it
   in its queue. Concurrency permits are held by the engine for the job's whole execution, renewed by the
   heartbeat and released after the outcome is written.
3. **Queue limits** (phase 3; fetch-time): a concurrency or rate limit on a whole queue, stored in Redis
   (`queue-limits`), enforced inside the fetch script: a queue over its limit is skipped like a paused
   queue, its jobs stay in place (FIFO, no churn), other queues keep flowing.

| Topic | Decision |
|---|---|
| Clock | Redis `TIME` inside every script (ms). Clients' clocks never matter; a test-only `now` override makes scripts deterministic. |
| Atomicity | One script checks every limit of a call and grants all or none (no partial charge: sidekiq-throttled's starvation bug). Grants' first write is the grant (OOM-safe); bookkeeping runs under `pcall`. |
| Data model | `limit:<k>:<id>` state (ZSET / LIST / HASH / STRING by kind), `limit-stats:<k>:<id>` counters and wait line, `limits` index ZSET, `l\|YYYYMMDD\|H:MM` per-minute counters, `queue-limits` HASH (phase 3). Every key has a TTL derived from its meaning (no 90-day leftovers). |
| Over-limit handling | Reschedule into `schedule` through the existing push-back path (`PUSH_BACK` in its `schedule` mode: written and acknowledged in one script under reliable fetch), job as fetched, unique lock held (unique design's `PUSH_BACK_UNIQUE`), not an outcome for batches (batch design's contract), delay = the limiter's hint (wait line) clamped to [1 s, 24 h] plus up to 10 % jitter. |
| Herd control | A per-limit **wait line**: each refusal gets the next slot, spaced by period × cost / limit (rate kinds) or average hold time / limit (concurrency, measured). Rescheduled jobs come back spread out instead of all at once. |
| Leases | User permits: 30 s, renewed every 10 s by a task while held, released on drop. Engine-held throttle permits: 60 s, renewed by the heartbeat, released after the outcome. A crash frees a slot within one lease. |
| Failures | Fail-closed on Redis errors by default (`fail_open` opt-in). Lost replies: concurrent acquires are idempotent by token; rate acquires may waste a unit (never over-admit). |
| Operability | `api::Limits`; dashboard **Limits** tab (usage, counters, holders, wait line, reset); `hench limits` commands; JSON API; job metrics gain over-limit (`o`) and throttled (`t`) counts. |
| Testing | Testing clients run limits as **unlimited** by default (`Testing::limits(TestLimits::InMemory)` enforces them in memory on tokio time). |
| Cost | About 31 000 to 34 000 acquires/s per Redis core for one limit with bookkeeping (50 000 to 59 000 lean), 27 000/s concurrent, 18 000/s for two limits at once; window state about 10 bytes per unit of limit; leaky/bucket state 88 bytes. |

---

## 1. Survey

### 1.1 Sidekiq Enterprise (behaviour from `sidekiq.wiki/Ent-Rate-Limiting.md`, `sidekiq/Ent-Changes.md`)

- **API.** `Sidekiq::Limiter.concurrent / bucket / window / leaky / points / unlimited(name, ...)`, each
  with `within_limit { ... }`. Works in any Ruby process; all processes sharing the Redis share the
  limits. Names are letters, digits, hyphens, underscores; dynamic per-user limiters are made by putting
  the id in the name. Limiters are meant to be created once and reused (creation is expensive there).
- **Concurrent**: N holders; `lock_timeout` (default 30 s) after which another thread may reclaim a lock
  (a crashed process must not hold it forever); `wait_timeout` (default 5 s) during which a caller blocks
  for a free slot (with blocking Redis commands rather than polling); `policy: :raise` or `:ignore`.
  Metrics: held, held time, immediate, waited, wait time, overages (the block outlived `lock_timeout`)
  and reclaims (another caller took an expired lock, which can break the limit), the last two flagged as
  warning signs. A concurrent limiter does not
  limit how many jobs start: 20 threads take 20 jobs, 15 of which wait and then raise.
- **Bucket**: counts per calendar interval (second/minute/hour/day): bursts of 2N across a boundary are
  allowed. Per-second buckets sleep until the next second within `wait_timeout`; longer intervals raise
  at once.
- **Window**: "each interval is started at time of use"; arbitrary window sizes since 1.0.2; sleeps 0.5 s
  steps until `wait_timeout`; a `wait_timeout` shorter than the window raises immediately.
- **Leaky** (2.2): bucket of N that drains in D: burst N, then one per D/N; `wait_timeout` 5 s by
  default, `0` raises at once.
- **Points** (7.1): GraphQL-style cost budgets; `within_limit(estimate: n) { |h| h.points_used(actual) }`
  corrects the estimate after the call. 7.2.1 added `within_limit(used: n)` to window and bucket.
- **OverLimit handling**: server middleware catches `OverLimit` and reschedules the job with a linear
  backoff (default `300 × overrated + rand(300) + 1` s, customizable globally or per limiter with a proc
  receiving limiter, job and exception); the job hash counts `overrated`; after 20 reschedules
  (`reschedule:` per limiter, `0` disables) the error is re-raised into the normal retry system. Custom
  exception classes can be declared as over-limit errors (`config.errors << SlowDownPlease`).
- **Limiting does not throttle**: limiters do not slow job processing; 1000 pushed jobs run as fast as
  possible and many fail with `OverLimit`; trickling requires scheduling at push time.
- **Not composable**: nested limiters ("100/hour and 5/minute") do not work; recommendation is to keep
  the smaller one and treat the remote's errors as over-limit.
- **Operations**: `ttl:` for limiter metadata (default 90 days); separate Redis for limiters at scale;
  cluster-safe data model since 7.1; `Limiter.via` to target a shard; Web UI "Limits" tab listing every
  limiter (paginated, filterable by name), concurrent metrics; bucket history graph removed in 7.0.
- **Clock**: documented as clock-sensitive (hosts must run NTP; the docs state that scripts cannot use
  Redis's clock), a constraint that predates effects replication (Redis 5): scripts may call `TIME` and
  then write.

### 1.2 sidekiq-throttled (MIT; `lib/sidekiq/throttled/*`, specs, `CHANGES.md`)

- **Declarative per class**: `sidekiq_throttle(concurrency: {limit:, key_suffix:, ttl/lost_job_threshold:,
  avg_job_duration:, max_delay:}, threshold: {limit:, period:, key_suffix:}, requeue: {with:, to:},
  observer:)`; arrays of several concurrency or threshold rules; dynamic `limit`, `period` and
  `key_suffix` procs evaluated with the job's arguments at fetch time; `sidekiq_throttle_as` shares a
  strategy between classes.
- **Enforcement at fetch**: `BasicFetch`/`SuperFetch` are patched; after a job is fetched,
  `throttled?(jid, *args)` runs the strategies; a throttled job is pushed back (`:enqueue`, `LPUSH`: the
  back of the line, or `SuperFetch#requeue`) or rescheduled (`:schedule`, since 1.5.0) as a **new job**
  (`enqueue_to_in`, new jid) before acknowledging the old one ("could be processed twice if the process
  dies between the two lines"). A server middleware `ensure` block calls `finalize!` (releases the
  concurrency slot).
- **Concurrency** (`concurrency.lua`): ZSET jid → `now + lost_job_threshold`; stale members purged by
  score; throttled when `ZCARD ≥ limit` and the jid is not already in; a backlog estimate (decayed by
  `limit / lost_job_threshold` per second) drives the `:schedule` delay `backlog × avg / limit`, softly
  capped by `max_delay × (1 − e^(−d / max_delay))`. Default lease 15 min ("if your job takes longer, you
  may run more jobs concurrently than expected").
- **Threshold** (`threshold.lua`): a list of the last `limit` timestamps (`LPUSH`, `LTRIM`, `PEXPIRE`):
  an exact sliding window with O(limit) memory. Time comes from the client (`Time.now.to_f`; a TODO says
  to use Redis `TIME`).
- **Cooldown**: a process excludes a queue from polling for `cooldown_period` (1 s) after
  `cooldown_threshold` (100) throttled jobs in a row.
- **Web**: a "Throttled" tab listing static strategies with usage and a Reset button.

### 1.3 BullMQ (MIT; `src/commands/moveToActive-11.lua`, `includes/getRateLimitTTL.lua`,
`includes/prepareJobForProcessing.lua`, `moveJobFromActiveToWait-9.lua`, `src/classes/worker.ts`,
`docs/gitbook/guide/rate-limiting.md`, `bullmq-pro/groups/*`, `tests/rate_limiter.test.ts`)

- **Queue-level limiter, checked before taking a job**: `moveToActive` first checks a counter key
  (`<prefix>:limiter`, `INCR` per job moved to active, `PEXPIRE duration` on the first increment, i.e. a
  window anchored at first use); at `max` it returns the key's `PTTL` and **the job stays in wait**; the
  worker sleeps `min(ttl, maximumRateLimitDelay = 30 s)` before fetching again. Global concurrency
  (`setGlobalConcurrency`) compares `LLEN active` with a value stored in the queue's meta hash. Global rate
  limits (`setGlobalRateLimit`) live in Redis too.
- **Manual rate limit**: `worker.rateLimit(ms)` / `queue.rateLimit(ms)` + `throw RateLimitError()`: the
  job moves back to wait without counting an attempt (`attemptsStarted` vs `attemptsMade`), and every
  worker of the queue pauses for the duration.
- **Group keys removed in 3.0** "in favor of a much simpler and efficient rate-limit implementation": per
  group limits in one FIFO list caused head-of-line blocking and churn. **BullMQ Pro groups** (behaviour
  from the docs) solve it with per-group virtual queues served round-robin, each with its own rate limit,
  concurrency (default or per group, stored in Redis), pause and max size.

### 1.4 Oban (Apache-2.0; `lib/oban/queues/executor.ex`, `lib/oban/engines/basic.ex`, `lib/oban/worker.ex`,
`guides/learning/defining_queues.md`, `CHANGELOG.md`)

- Limits are **per queue**: `limit` (per node), `dispatch_cooldown`, `paused`; `global_limit` and
  `rate_limit` (optionally partitioned by worker or arguments) are Oban Pro features. A SQL fetch can skip
  limited partitions without dequeuing anything, which a Redis list cannot.
- `{:snooze, period}` returns a job to `scheduled` without consuming an attempt (v2.24.0, backported from
  Oban Pro: the attempt is rolled back and a `snoozed` counter is kept in `meta`; before, snoozing raised
  `max_attempts`, which skewed backoff).

### 1.5 Faktory Enterprise (behaviour from `faktory/Changes.md`, `Ent-Changes.md`) and sidekiq-rs

- Faktory Enterprise has **queue throttling** in the server (concurrency per queue and per worker): the
  server simply does not hand out jobs of a saturated queue. 1.9.1 fixed a crash under heavily contended
  per-worker throttles (#483).
- sidekiq-rs (MIT) has no rate limiting.

### 1.6 Comparison

| | Sidekiq Ent | sidekiq-throttled | BullMQ | Oban (Pro) | hench (this design) |
|---|---|---|---|---|---|
| In-code limiter API | yes (5 kinds) | no | `rateLimit()` only | snooze only | yes (5 kinds + composite) |
| Declarative per class | no | yes | no (removed 3.0; Pro groups) | per queue/partition | yes (throttles) |
| Dynamic keys from args | in the name | `key_suffix` procs | Pro groups | partitions | `throttle_by` / `throttle_key` |
| Where enforced | inside `perform` | after fetch | before fetch (queue) | fetch (SQL) | inside code; after fetch; in the fetch script (queues) |
| Over limit | reschedule, linear 5 min backoff | push back or new job | stays in wait, worker sleeps | snooze | reschedule to a wait-line slot; queues: stays in place |
| Attempts consumed | no (`overrated`) | no | no (`attemptsStarted`) | no (`snoozed`) | no (`limited.n`) |
| Clock | client | client | client | DB | Redis `TIME` |
| Multi-limit atomicity | no | no (release after the fact) | n/a | yes (SQL) | yes (one script) |
| Crash recovery of slots | `lock_timeout` reclaim | 15 min ZSET score | lock expiry | DB | lease renewed while held |

### 1.7 Production lessons and how this design answers them

| # | Lesson (source) | Answer here |
|---|---|---|
| L1 | Rescheduling an over-limit job created a new job; redesigned to keep the job (Sidekiq Ent 0.7.6, #2619). sidekiq-throttled's `:schedule` still pushes a new jid, then acknowledges ("could be processed twice"). | Same jid, written into `schedule` by the `PUSH_BACK` script that acknowledges it (§2.5): never both or neither. |
| L2 | Job arguments mutated during the run were saved on reschedule (Ent 7.0.8, #5859); encrypted arguments were encrypted twice by retry or rate-limit reschedule (Ent 1.5.2 and 1.5.3, #3368); Oban's snooze overwrote meta changes made during the run (Oban CHANGELOG). | The job **as fetched** is rescheduled (like hench retries); client middleware is not re-run then (it runs when the scheduler moves the job to its queue). Only `limited` is updated. |
| L3 | Window limiter race left a counter without expiry: non-stop `OverLimit` (Ent 0.7.9 and 1.0.2, #2704). BullMQ deleted a limiter key whose `PTTL` was 0 (#1553): a counter without TTL blocked the queue forever. | Counter and expiry are written by the same script; every state write sets or extends the TTL; a scenario test asserts that every key has a TTL after every script. |
| L4 | Concurrent limiter keys leaked (Ent 1.5.2, #3323, cleanup by `scan_each` + `expire`); millions of dynamic limiters, 7 M keys (sidekiq#3981); TTL refreshed on one of four structures only, so short TTLs broke limiters created at boot. | TTLs derive from meaning (window: period; bucket: interval end; leaky: drain end; concurrent: the longest lease), refreshed by every write; stats expire 24 h after last use; one index with per-entry expiry, trimmed by every grant. |
| L5 | TTL must never be shorter than the longest hold, or mutual exclusion breaks (sidekiq#3981); sidekiq-throttled's 15 min lease lets long jobs exceed the limit. | Leases are renewed while held (permit task or heartbeat); the key's TTL is extended to at least the lease ("never shorten"). |
| L6 | Blocking `BLMOVE` with a sub-millisecond timeout hung the client and each occurrence shrank the limiter's capacity by one (Ent 7.2.2, #6188, redis#11732); spurious read timeouts (Ent 7.0.1 #5611, 7.0.7 #5838); thread thrashing under contention (Ent 1.2.2, #2944). | No blocking Redis commands for limiters (hench's multiplexed connection forbids them anyway): waits are local sleeps toward an exact hint, with jittered polling for concurrency; slots are leases, so a lost reply can never shrink capacity for longer than a lease. |
| L7 | Excessive lock reclaims (Ent 1.8.1, #4105); the docs flag overages and reclaims as signs of trouble. | Renewal prevents reclaims of live holders; overages, reclaims and restorations are counted and shown. |
| L8 | Releasing a concurrency slot after a separate threshold refusal was needed to avoid starvation (sidekiq-throttled spec "avoids concurrency limit starvation"). | All limits of a call are checked first and granted together, or none is charged. |
| L9 | `:enqueue` requeueing burnt Redis CPU and starved other queues; cooldown threshold 1 excluded whole queues for one job (1.5.0 changed it to 100 / 1 s, #195); hence `:schedule` (#150) and backlog-aware delays (#209). Race between the throttle check and the delay computation (#208). | Throttled jobs leave the queue for `schedule` at a wait-line slot computed **by the same script** that refused them; no queue exclusion for class throttles; queue limits skip only the limited queue. |
| L10 | Group keys in one list were removed (BullMQ 3.0); rate-limited queues stopped promoting delayed jobs (#2697, #3561); a manual rate limit must move the job back even when the TTL is 0 (#2403); closing a worker had to abort its rate-limit sleep. | Per-key throttles never block a list; hench's scheduler is independent of fetch; an over-limit outcome always writes the job somewhere; limiter waits are cancelled by the shutdown. |
| L11 | Snoozes consumed attempts and skewed backoff (Oban, fixed in v2.24.0 by rolling back `attempt` and counting `snoozed`); BullMQ separates `attemptsStarted` from `attemptsMade`. | `retry_count`, `failed_at` and error fields are untouched by reschedules; `limited.n` counts them. |
| L12 | Rate limiting is hard on Redis; a separate Redis or connection pool was advised (Ent docs, #5685, #5752). | Limiters take any `RedisPool` (`Limiter::acquire(&pool)`), so a dedicated Redis is a configuration choice; bookkeeping can be turned off per limiter. |
| L13 | Deferred failures counted against the rate limit (BullMQ #4607). | Jobs that do not run (refused, parked) are never charged; only grants are. |

---

## 2. Design

### 2.1 Goals, non-goals, guarantees

Goals: protect external resources from every hench process (and any other process sharing the Redis) with
exact, cluster-wide limits; let job classes and tenants be throttled without starving other work, without
busy loops and without consuming retries; keep reliable fetch's guarantees (nothing lost, nothing both
written and still in progress); be observable and tunable at runtime; be testable without Redis.

Non-goals: per-process (local) concurrency, which capsules already give; fairness between tenants beyond
per-key limits (round-robin virtual sub-queues, BullMQ Pro groups, are future work, §4 D2); exactness
across a Redis failover with asynchronous replication or a restart without persistence; Redis Cluster
(hench as a whole is single-node; scripts declare their keys, so Dragonfly's rules are met).

Guarantees (single Redis primary, no data loss):

- G1. For window, bucket, leaky and points limits, no more units are granted than the definition allows,
  whatever the number of processes (one script per decision, Redis time).
- G2. For concurrency limits, at most `limit` leases are live at any instant. A holder whose lease
  ended without renewal (crash, stalled runtime longer than a lease) loses its slot; it keeps running and
  the overage is counted (the only way the number of *running holders* can exceed the limit).
- G3. A call with several limits is charged on all or none.
- G4. An over-limit job is never lost or duplicated by its reschedule: with reliable fetch the reschedule
  and the acknowledgement are one script; repeated after a lost reply it does nothing.
- G5. Reschedules never consume retries; after `reschedules` of them the failure path takes over.
- G6. No limiter key lives without a TTL; idle state disappears once it carries no information.

### 2.2 Concepts

- **Limit**: a rule of one kind (`Limit::Concurrency`, `Window`, `Bucket`, `Leaky`, `Points`) with its
  parameters. **Limiter**: a named set of limits (one, or several with `Limiter::all`), plus options.
  **Key**: optional dynamic part (tenant, user) appended to the name. **Identity** of one limit's state:
  `(kind, name, key)`, written `<k>:<name>[:<key>]` with `k` ∈ `c`, `w`, `b`, `g` (leaky and points share
  `g`). Two definitions with the same identity share state (on purpose: several jobs can share "stripe");
  they should agree on parameters (the stats keep the last signature; a mismatch is logged once per
  process at `warn`).
- **Permit**: proof of a grant. Concurrency permits hold a lease and must be released (on drop, or
  explicitly); rate permits are receipts (points permits can correct their cost).
- **OverLimit**: the refusal, with `retry_after` (when this caller's slot comes) and the limiter's
  reschedule policy.
- **Wait line**: per limit identity, the time at which the last waiter is expected to be served
  (`line` field of the stats hash). A refusal is given `slot = max(now + own_wait, line)` and pushes the
  line to `slot + spacing`. `spacing` = period × cost / limit (rate kinds), or average hold time / limit
  for concurrency (measured from releases; `1000 / limit` ms until measured). The line is an absolute
  time, so it resets itself once in the past. Callers that wait in process pass their slot back
  (`ticket`) on later attempts and keep their place without lengthening the line. (Round 3: only a
  caller that will come back for its slot takes a place, a parked or rescheduled job always, a
  waiter for a slot within its `max_wait`; others are told where the line ends. A parked or
  rescheduled job keeps its slot in `limited.s` and hands it back as its ticket, and a rate-limited
  caller back a little late takes the next unit that frees. Release review: a job rescheduled by a
  *limiter* hands its slot back too, from its context, on its first acquisition of that limit in
  the execution (before, only the throttle gates did, so such a job queued again at the end of the
  line); `try_acquire` never takes a place, whatever its target, a job's context included (a job
  that handles a refusal itself must not push the line); `acquire` from a job always does, since
  the job returns the refusal and is rescheduled for that place. Concurrency spacing comes from a
  decaying average hold `hold`, else the 24-hour average, else the caller's expected hold, 25 ms
  by default, instead of `1000 / limit` ms, which parked backlogs of short jobs for minutes.)
- **Throttle**: a named, declarative set of limits for job starts, each global or per key, applied by the
  processor to jobs whose payload names it. **Queue limit**: a set of limits applied to every fetch of
  one queue, inside the fetch script (phase 3).
- **Reschedule**: the outcome of an over-limit job: the job as fetched, with `limited` updated, written to
  `schedule` at `now + delay`, acknowledged in the same step.

### 2.3 Algorithms (exact semantics; `now` = Redis `TIME` in ms)

| Kind | State (type) | Grant condition for cost `c` | Effect of a grant | `own_wait` when refused | State TTL |
|---|---|---|---|---|---|
| Concurrency N, lease L | ZSET token → lease end | the token already holds a live lease (idempotent), or `ZCOUNT (now, +inf) + 1 ≤ N` | `ZADD token now+L`; extend key TTL to ≥ L; purge leases ended before now (counted as reclaimed) | one spacing (avg hold / N) | ≥ the longest lease, extended by grants and renewals; `DEL` when empty |
| Window N per P (sliding) | LIST of grant times, newest first, at most N entries | entry at index `N − c` is missing or `≤ now − P` | `LPUSH now` c times; `LTRIM 0 N−1`; `PEXPIRE P + 1 s` | `entry[N−c] + P − now` (exact) | P + 1 s after the last grant |
| Bucket N per I (UTC-aligned) | HASH `b` (index `floor(now/I)`), `n` (units) | `used + c ≤ N` (`used` = 0 if `b` is another bucket) | `HSET b, n`; `PEXPIRE` to the bucket's end + 1 s | end of the bucket − now | end of the bucket + 1 s |
| Leaky N drained in D (GCRA) | STRING `tat` (ms, 3 decimals) | `max(tat, now) + c·D/N − now ≤ D` | `SET tat' PX (tat' − now + 1 s)` | `tat' − D − now` (exact) | until the bucket is empty + 1 s |
| Points C refilled in R | same as leaky (N = C, D = R) | same, with `c` = estimate | same | same | same; `ADJUST` refunds or charges `actual − estimate` |

- `c > N` can never be granted: the call fails with `Error::Config` (not `OverLimit`, which would loop).
- Leaky semantics match Sidekiq's documented ones (verified, Appendix A): `leaky(60, 60 s)` grants a burst of
  60, then one per second; 5 s later 5 more; after a full drain a burst of 60 again.
- Window is exact (any interval of length P holds at most N grants), at O(N) memory (about 10 bytes per
  unit; 10.4 KB for N = 1000). For large N with many keys prefer leaky or bucket (O(1), 88 bytes).
- Bucket boundaries are UTC multiples of the interval since the epoch (`Interval::Second | Minute | Hour |
  Day | Every(d)`); a day bucket resets at 00:00 UTC.
- Concurrency counts live leases only (`ZCOUNT (now, +inf)`), so a check needs no write; ended leases are
  purged by the next grant (after the grant's own write: OOM rule, §2.9).
- Points correction (`Permit::points_used(actual)`): `tat ← max(tat, now) + (actual − estimate)·R/C`;
  a refund never goes below "empty"; an extra charge may go above "full" (debt that later callers wait out).

### 2.4 Rust API

New public module `hench::limit` (`crates/hench/src/limit/`: `mod.rs`, `spec.rs`, `scripts.rs`, `permit.rs`,
`throttle.rs`, `memory.rs`, `queue.rs`). Every type below is `#[non_exhaustive]` where it is a struct or enum
users do not construct field by field.

#### 2.4.1 Limits and limiters

```rust
/// One rule. `FromStr` / `Display` use the spec grammar of §2.17 ("window=100/1s").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Limit {
    Concurrency { limit: u32 },
    Window { limit: u32, period: Duration },
    Bucket { limit: u32, interval: Interval },
    Leaky { size: u32, drain: Duration },
    Points { capacity: u32, refill: Duration },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Interval { Second, Minute, Hour, Day, Every(Duration) }

/// A named limiter: cheap to build and to clone (an `Arc` inside); no I/O until used.
#[derive(Clone, Debug)]
pub struct Limiter { /* name, key, limits, options */ }

impl Limiter {
    pub fn concurrent(name: impl Into<String>, limit: u32) -> Limiter;
    pub fn window(name: impl Into<String>, limit: u32, period: Duration) -> Limiter;
    pub fn bucket(name: impl Into<String>, limit: u32, interval: Interval) -> Limiter;
    pub fn leaky(name: impl Into<String>, size: u32, drain: Duration) -> Limiter;
    pub fn points(name: impl Into<String>, capacity: u32, refill: Duration) -> Limiter;
    pub fn new(name: impl Into<String>, limit: Limit) -> Limiter;
    /// Always grants, without Redis (tests, exempt callers).
    pub fn unlimited() -> Limiter;
    /// Every limit of every limiter given, checked and charged atomically (all or none).
    pub fn all(limiters: impl IntoIterator<Item = Limiter>) -> Limiter;

    /// The instance of this limiter for one key: `stripe` + `42` -> `stripe:42`. Keys of more than
    /// 128 bytes are replaced by `#` + 22 base64url characters (132 bits) of their SHA-256, from the
    /// crate-wide helper the unique design's digests use (cross-feature review: one hash, no SHA-1).
    #[must_use] pub fn key(&self, key: impl fmt::Display) -> Limiter;
    /// How long `acquire` may wait in process for a slot (default 5 s; zero never waits). It sleeps
    /// only when the slot it was given comes within the remaining time.
    #[must_use] pub fn max_wait(self, wait: Duration) -> Limiter;
    /// Concurrency: lease length (default 30 s, at least 1 s), renewed every third of it while held.
    #[must_use] pub fn lease(self, lease: Duration) -> Limiter;
    /// Times a job may be rescheduled by this limiter before the failure path takes over
    /// (default 20, Sidekiq's; 0 = an `OverLimit` fails the job at once).
    #[must_use] pub fn reschedules(self, max: u32) -> Limiter;
    /// Longest reschedule delay (default 24 h); delays are at least 1 s.
    #[must_use] pub fn max_delay(self, delay: Duration) -> Limiter;
    /// Reschedule delay policy (default: the wait-line hint, else 60 s × n), see §2.5.
    #[must_use] pub fn backoff(self, f: impl Fn(&BackoffInput<'_>) -> Duration + Send + Sync + 'static) -> Limiter;
    /// Keep a wait line (default true). Off: refusals return the limit's own wait only.
    #[must_use] pub fn spread(self, on: bool) -> Limiter;
    /// Per-limiter counters and index entry (default true; off saves about 40 % of the script's cost,
    /// and also turns the wait line off). Expiry of the counters after the last use (default 24 h).
    #[must_use] pub fn stats(self, on: bool) -> Limiter;
    #[must_use] pub fn stats_ttl(self, ttl: Duration) -> Limiter;
    /// On Redis errors: refuse (default) or grant without limiting (rate kinds only).
    #[must_use] pub fn fail_open(self, on: bool) -> Limiter;

    pub fn name(&self) -> &str;
    pub fn id(&self) -> String;                       // "stripe:42"
    pub fn limits(&self) -> &[Limit];
    /// Checks names, keys and parameters (also done by every call, which returns `Error::Config`).
    pub fn validate(&self) -> Result<()>;

    /// Takes one unit (cost 1) from every limit; waits up to `max_wait`.
    pub async fn acquire(&self, on: impl LimitTarget) -> Result<Permit>;
    /// Takes `cost` units (window, bucket, leaky, points; concurrency ignores the cost).
    pub async fn acquire_n(&self, on: impl LimitTarget, cost: u32) -> Result<Permit>;
    /// Never waits.
    pub async fn try_acquire(&self, on: impl LimitTarget) -> Result<Permit>;
    /// Sidekiq's `within_limit`: acquires, runs `f`, releases. `Err(Error::OverLimit)` if refused.
    pub async fn run<F, T>(&self, on: impl LimitTarget, f: F) -> Result<T>
    where F: Future<Output = T> + Send;
    /// Current use (read only).
    pub async fn usage(&self, on: impl LimitTarget) -> Result<Vec<Usage>>;
    /// Deletes the state (frees every slot) and counters of this limiter's identities.
    pub async fn reset(&self, on: impl LimitTarget) -> Result<()>;
}

/// Where a limiter runs: Redis, or the testing client's in-memory store, or nowhere (unlimited).
/// Implemented for `&JobContext` (its client; its stopping signal cancels waits), `&Client`,
/// `&RedisPool`, `&Hench`. Sealed.
pub trait LimitTarget: sealed::Sealed { /* backend(), stopping() */ }
```

Example (inside a job):

```rust
static STRIPE: LazyLock<Limiter> = LazyLock::new(|| Limiter::window("stripe", 100, Duration::from_secs(1)));
static ERP: LazyLock<Limiter> = LazyLock::new(|| Limiter::concurrent("erp", 50).lease(Duration::from_secs(60)));

#[hench::job(queue = "billing")]
async fn charge(ctx: &JobContext, account: i64, cents: i64) -> Result<(), JobError> {
    let _slot = ERP.acquire(ctx).await?;                    // held until the end of the job
    let _rate = STRIPE.key(account).acquire(ctx).await?;  // OverLimit -> rescheduled, not failed
    call_stripe(account, cents).await?;
    Ok(())
}

// Outside jobs (an axum handler): answer 429 with the hint.
match STRIPE.key(user).try_acquire(&redis).await {
    Ok(_) => serve().await,
    Err(hench::Error::OverLimit(over)) => too_many(over.retry_after()),
    Err(e) => internal(e),
}

// GraphQL points: estimate, then report the real cost.
let mut permit = SHOPIFY.acquire_n(ctx, 200).await?;
let used = run_query().await?.cost;
permit.points_used(used).await?;

// "100 per hour and 5 per minute", atomically (Sidekiq Ent: not composable).
let both = Limiter::all([Limiter::window("crm-h", 100, HOUR), Limiter::window("crm-m", 5, MINUTE)]);
```

#### 2.4.2 Permits and errors

```rust
/// Bind it for as long as the resource is used (`let _permit = ...`): `let _ = ...` drops it at once,
/// which releases concurrency slots immediately (rate permits are receipts: dropping them changes nothing).
#[must_use = "a permit releases its concurrency slots when dropped; bind it with `let _permit = ...`"]
pub struct Permit { /* identities, token, acquired_at, renewal task handle, lost flag */ }

impl Permit {
    /// Releases now and reports Redis errors (dropping also releases, in a spawned task when a tokio
    /// runtime is running; otherwise the lease ends by itself).
    pub async fn release(self) -> Result<()>;
    /// Points: replaces the estimate by the actual cost (refund or extra charge).
    pub async fn points_used(&mut self, actual: u32) -> Result<()>;
    /// Concurrency: whether the lease was lost (not renewable and no capacity to restore it).
    pub fn is_lost(&self) -> bool;
    /// Resolves when the lease is lost (for long holders that want to stop).
    pub async fn lost(&self);
    /// How long `acquire` waited.
    pub fn waited(&self) -> Duration;
}

/// The refusal of a limiter. Implements `std::error::Error` and `Display`
/// ("over the limit `w:stripe:42` (window=100/1s); next slot in 1.234 s").
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct OverLimit { /* label, kind, retry_after, reschedules, max_delay, backoff */ }

impl OverLimit {
    /// A refusal of your own (an HTTP 429, a quota error): `label` names the resource.
    pub fn new(label: impl Into<String>) -> OverLimit;
    #[must_use] pub fn with_retry_after(self, after: Duration) -> OverLimit;
    #[must_use] pub fn with_reschedules(self, max: u32) -> OverLimit;
    pub fn label(&self) -> &str;
    pub fn retry_after(&self) -> Option<Duration>;
    pub fn reschedules(&self) -> Option<u32>;
}

/// Current use of one limit identity (`Limiter::usage`, `api::Limits`).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Usage {
    pub identity: String,          // "w:stripe:42"
    pub limit: Limit,
    pub used: f64,                 // units in use (leaky/points: fractional level)
    pub next_free_in: Option<Duration>, // concurrency: the earliest lease end (a worst case)
}

/// What a backoff closure sees.
#[non_exhaustive]
pub struct BackoffInput<'a> {
    pub job: &'a JobPayload,       // as fetched
    pub over: &'a OverLimit,
    pub reschedules: u32,          // this one included (1 the first time)
}
```

Spine additions:

- `hench::Error::OverLimit(limit::OverLimit)` (new variant of the `#[non_exhaustive]` enum; `Display`
  forwards). `Error::Config` for invalid definitions.
- `JobError::OVER_LIMIT = "hench::OverLimit"`; `JobError::over_limit(OverLimit) -> JobError` (class
  `OVER_LIMIT`, the `OverLimit` as its source); `JobError::as_over_limit(&self) -> Option<&OverLimit>`
  (downcasts to `OverLimit`, or to `hench::Error` and matches the variant; anyhow sees through
  `.context(..)`), `is_over_limit()`. A job returning `limiter.acquire(ctx).await?` therefore needs nothing
  special. Wrapping the refusal in an error type of one's own hides it (documented: return it with `?` or
  `JobError::over_limit`).
- `HenchBuilder::over_limit_when(F)` with `F: Fn(&JobError) -> Option<OverLimit> + Send + Sync + 'static`
  (Sidekiq's custom error list): called by the processor for job errors that are not already an
  `OverLimit` (for example a client library's 429 error carrying `Retry-After`). A panicking classifier is
  reported and ignored.
- `HenchBuilder::over_limit_backoff(F)` (process default for limiters without their own) and
  `HenchBuilder::over_limit_reschedules(u32)` (default 20) and `over_limit_max_delay(Duration)` (24 h).
- Prelude: `Limiter`, `OverLimit`.

#### 2.4.3 Throttles

```rust
/// Limits on job starts, applied by workers to jobs whose payload names the throttle.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Throttle { /* name, Vec<(Limit, Scope)>, lease, reschedules, max_delay, max_wait, fail_open */ }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Scope { Global, PerKey }

impl Throttle {
    pub fn new(name: impl Into<String>) -> Throttle;
    #[must_use] pub fn limit(self, limit: Limit) -> Throttle;          // global
    #[must_use] pub fn limit_per_key(self, limit: Limit) -> Throttle;  // per job key
    // shorthands
    #[must_use] pub fn concurrency(self, n: u32) -> Throttle;
    #[must_use] pub fn concurrency_per_key(self, n: u32) -> Throttle;
    #[must_use] pub fn window(self, n: u32, period: Duration) -> Throttle;
    #[must_use] pub fn window_per_key(self, n: u32, period: Duration) -> Throttle;
    #[must_use] pub fn leaky(self, size: u32, drain: Duration) -> Throttle;
    #[must_use] pub fn leaky_per_key(self, size: u32, drain: Duration) -> Throttle;
    #[must_use] pub fn bucket(self, n: u32, interval: Interval) -> Throttle;
    #[must_use] pub fn bucket_per_key(self, n: u32, interval: Interval) -> Throttle;
    /// Lease of the engine-held concurrency permits (default 60 s, renewed by the heartbeat).
    #[must_use] pub fn lease(self, lease: Duration) -> Throttle;
    /// Default: unlimited (a throttled job waits as long as it takes); `Some(n)`: then the failure path.
    #[must_use] pub fn reschedules(self, max: impl Into<Option<u32>>) -> Throttle; // 5 or None (round 3)
    #[must_use] pub fn max_delay(self, delay: Duration) -> Throttle;   // default 24 h
    /// In-process wait before parking the job (default zero: never hold a processor for a job that
    /// has not started).
    #[must_use] pub fn max_wait(self, wait: Duration) -> Throttle;
    /// Run jobs unthrottled when Redis fails the check (default false: requeue them).
    #[must_use] pub fn fail_open(self, on: bool) -> Throttle;
}
```

- **Builder**: `HenchBuilder::throttle(Throttle)` (a later definition with the same name replaces an
  earlier one); `build()` collects, in order, the per-class throttles of registered jobs
  (`Job::throttle()`), then the builder's, then the TOML file's (later wins, logged at `info`), validates
  them (`Error::Config`: invalid limits, duplicate kind and scope inside one throttle is allowed and gets a
  `~2` suffix, names), and checks that every registered job whose options name a throttle finds one
  (`Error::Config` naming the job and the throttle).
- **Job side.** `JobOptions` gains `pub throttle: Option<ThrottleRef>` with `ThrottleRef { name: String,
  key: Option<String> }` or `Disabled`, and setters `throttle(name)`, `throttle_key(key)`,
  `no_throttle()` (same on `JobSetter`, `JobBuilder`, `BulkPush`; merge rules like `queue`). The `Job`
  trait (and `IterableJob`, forwarded) gains:

  ```rust
  /// The throttle key of these arguments (tenant, account); `None`: only global limits apply.
  fn throttle_key(args: &Self::Args) -> Option<String> { None }
  /// A throttle defined with the job (registered under its name by `build`).
  fn throttle() -> Option<Throttle> { None }
  ```

  `ErasedJob` gains `dyn_throttle()` (default `None`). Typed pushes call `throttle_key` when the merged
  options name a throttle and set no explicit key.
- **Macro.** `#[hench::job(throttle = "stripe", throttle_by = [account])]` sets the option and generates
  `throttle_key` (the selected parameters, in order, as compact JSON: `42`, `["acme",42]`; strings
  unquoted when alone). Inline definitions: `#[hench::job(throttle(concurrency = 5, window = "100/1s",
  window_per_key = "5/1s"))]` generates `Job::throttle()` named after the job and sets the option.
  Compile errors for unknown limit names, malformed specs (parsed at compile time with the §2.17 grammar),
  unknown or duplicated parameters, the context parameter in `throttle_by`, and `throttle_by` without a
  throttle. On `impl Job` blocks, as today, only `crate` is accepted.

```rust
#[hench::job(queue = "billing", throttle = "stripe", throttle_by = [account])]
async fn charge(ctx: &JobContext, account: i64, cents: i64) -> Result<(), JobError> { .. }

#[hench::job(queue = "exports", throttle(concurrency = 3))]
async fn export_all(ctx: &JobContext, org: i64) -> Result<(), JobError> { .. }

let hench = Hench::builder()
    .throttle(Throttle::new("stripe").concurrency(25).window_per_key(5, Duration::from_secs(1)))
    .build().await?;

// Tiers: pick the throttle per push.
charge::set().throttle("stripe-vip").perform_async(&client, (42, 990)).await?;
```

#### 2.4.4 Payload fields

`JobPayload` gains two optional fields (`throttle` and `limited` join `RESERVED_KEYS`):

```json
"throttle": {"name": "stripe", "key": "42"}
"limited":  {"n": 3, "by": "w:stripe:42", "at": 1730426400123}
```

- `throttle` is written by normalization from the merged options (key from the explicit key, else
  `throttle_key`); kept through retries, reschedules and scheduler moves; middleware may change it.
- `limited` is written by the processor on each reschedule: `n` reschedules so far, `by` the label of the
  limit that refused last, `at` when (ms, the worker's clock). It is never reset (Sidekiq's `overrated`).
  (Round 3: also `t`, parkings by throttles, which only a throttle's budget counts; `o`, limiter
  reschedules since the job last made progress, which only the limiters' budget counts, restarted by
  an execution that made progress before its refusal; and `s`, the slot of the job's place in the
  line of `by`.)
- Reading is lenient: a malformed value becomes `None` with a `warn` log, so these fields can never make
  a job unparseable (and send it to the morgue).

#### 2.4.5 Queue limits (phase 3)

```rust
impl api::Queue {
    pub async fn limits(&self) -> Result<Vec<Limit>>;            // HGET queue-limits <name>
    pub async fn set_limits(&self, limits: &[Limit]) -> Result<()>; // concurrency, window, bucket, leaky
    pub async fn clear_limits(&self) -> Result<bool>;
}
impl HenchBuilder {
    /// Written to Redis at start (code wins over runtime edits at every start; §4 D8).
    pub fn queue_limits(self, queue: impl Into<String>, limits: impl IntoIterator<Item = Limit>) -> Self;
}
```

### 2.5 Over-limit outcome (processor)

`Processor::run_job` gains one outcome branch (before the generic failure branch):

1. `result` is `Err(error)` and `error.as_over_limit()` is `Some(over)`, or the configured
   `over_limit_when` classifier returns `Some(over)`.
2. `original = work.parse()` (the job as fetched, L2). `n = original.limited.n + 1`;
   `max = over.reschedules().unwrap_or(config.over_limit_reschedules)`. (Round 3: the budget compares
   `o + 1`, where `o` restarts after an execution that made progress, an iterable job that processed
   items first, and never counts throttle parkings.)
3. If `n > max`: the failure path (`fail_and_report`) with `error.with_class(JobError::OVER_LIMIT)` and
   the message "rescheduled {max} times by `{label}`; handing over to retries". Retries then follow the
   job's retry policy; `Job::retry_in` sees `error.is_over_limit()` and may pick a delay. `limited.n`
   is not reset, so later over-limit refusals of the same job go to the failure path at once and the
   retry backoff governs it from then on (Sidekiq's documentation does not say whether its counter
   resets; §4 D3 lists the alternative of a fresh budget after each retry).
4. Else the delay: the limiter's `backoff` closure, else the process's `over_limit_backoff`, else
   `over.retry_after()` (the wait-line slot), else `60 s × n`; clamped to `[1 s, max_delay]`; plus a
   uniform jitter in `[0, min(delay / 10, 30 s)]` (round 3: none when the delay leads to the job's own
   place in the line, which is exact; a throttle's parking also has none, and lasts from 50 ms). A
   closure that panics is reported (`over-limit backoff panicked; using the default`) and the default
   is used.
5. `original.limited = {n, by: over.label(), at: now_ms}`; the job is encoded as
   `Stored::Scheduled { at: now + delay, json }` (no `at` or `enqueued_at` in the member) and written by
   the existing `Fetcher::push_back(work, &stored)`, without running client middleware (it runs when the
   scheduler moves the job, as for retries): with reliable fetch the `PUSH_BACK` script in its `schedule`
   mode (`LPOS` guard, `ZADD schedule`, then `LREM`; no new Lua), with basic fetch a `ZADD` in a `MULTI`.
   Not `Fetcher::settle`: a reschedule is not an outcome, and the batch design counts settles as
   failures (its §2.18 contract asks for the push-back path). For a unique job, the unique design's
   `PUSH_BACK_UNIQUE` holds the lock with TTL `for + delay` and records the job's score in the lock
   (`at`), as for any job waiting in `schedule`, so a debounce push (`OnConflict::Replace`) may
   replace it (its §2.18 contract). Written through
   `persist` (retried while Redis is unavailable); `Ok(false)` (no longer in progress: recovered by
   another process, or an earlier attempt whose reply was lost) is logged at `debug`.
6. Accounting: `stat:processed` +1 (it ran), not failed; metrics `Outcome::OverLimit` (`p` +1 and the new
   `o` +1, not timed, not failed); job logger event `over_limit` at `info`
   (`over limit of w:stripe:42; rescheduled in 12.3 s (3 of 20)`); error handlers are **not** called;
   death handlers are not called.
7. Engine-held throttle permits of the job are released after the write (§2.6).
8. Then, for a periodic job, the periodic design's `RECORD_RUN` reports the run as `rescheduled` (it
   ran, and is neither a success nor a failure). Batches count nothing (not an outcome).

Unknown classes cannot produce `OverLimit` (they fail before running). `retry: false` jobs are still
rescheduled (an over-limit refusal is not a failure); once `max` is exceeded they are dropped by the
failure path like any failure.

**Iterable jobs** (`iterable/mod.rs`): an `each_iteration` (or enumerator) error that is an over-limit
refusal is handled like an interruption with a delay: save the cursor, run `on_stop`, return the
`OverLimit` error unchanged (not `JobError::interrupted()`), so the processor reschedules the job at its
slot; the next execution runs `on_resume` and continues after the last item processed. Limiters used per
item should keep a short `max_wait` (default 5 s) so that brief saturation is absorbed in process.
Throttles apply per execution, not per item (documented).

### 2.6 Throttles in the processor

Flow in `Processor::run_job`, after parsing and before `stats.start_work` (so throttled jobs never appear
busy). The unit of work is held by the processor meanwhile, as between any fetch and execution today; the
gate's Redis calls are not cut short by a hard shutdown (they are short), and an in-process wait
(`max_wait`) ends at once when the process starts stopping, parking the job:

1. `engine.throttles.resolve(&job)` -> `Option<Resolved>`: the payload's `throttle.name` is looked up
   (unknown name: a `warn` once per name per minute and the job runs unthrottled; `build` already refuses
   registered jobs naming unknown throttles, so this covers raw pushes and other deployments). The limits
   are the throttle's global limits plus, when the payload has a key, its per-key limits (`name:key`).
   A job without a key gets the global limits only (a per-key-only throttle without key does nothing; a
   `debug` log).
2. `ACQUIRE` with token = the jid (round 3: the jid, `~` and 16 random hex digits, one per execution,
   so that the release of an execution that ended after a requeue never takes the lease of the job's
   next execution), flags `L` (wait line), cost 1, waiting up to the throttle's
   `max_wait` (default zero). A jid re-acquiring a concurrency slot it still holds (a job recovered after
   a crash before its lease ended) is granted idempotently.
3. Granted: the concurrency leases (if any) are registered in `engine.permits`
   (`HeldPermits: Mutex<HashMap<tid, Vec<HeldPermit>>>`), then the job runs as today.
4. Refused: the reschedule of §2.5 with `n` and the throttle's `reschedules` (default unlimited), the
   slot's delay, no execution: not processed, not failed, metrics `Outcome::Throttled` (`t` +1), log
   `debug` (`throttled by c:stripe; parked for 4.2 s`). No periodic `RECORD_RUN` (the job did not run).
   **Members of cancelled batches** (cross-feature review, adopting the refinement both this design and
   the batches design proposed): when the refused job is a batch member (valid `bid`, read from the job
   as fetched), the processor first reads `cancelled_at` of its batch (one `HGET`, only on refusals);
   if the batch is cancelled the member is counted as skipped (`BATCH_SKIPPED cancel`) and acknowledged
   (with `ACK_UNIQUE` when it holds a unique lock) instead of being parked, so a cancelled batch's
   throttled members do not cycle through `schedule`. Metrics `Outcome::Skipped`; server middleware does
   not see it (no throttled job reaches middleware).
5. Redis unavailable (`processor::is_unavailable`: unreachable, failing over, `OOM`, `LOADING`, ...):
   `fail_open` runs the job unthrottled with a `warn`; otherwise the job is requeued to the front of its
   queue (`Fetcher::requeue`), the error goes to the fetch outage notices (rate limited, like fetch
   errors) and the processor pauses 1 s. Any other error (a throttle key of the wrong type, a definition
   Redis rejects): the job fails with that error through the normal failure path (retry set), so one
   poisoned key can never sit at the front of a queue and block it.

After the job's outcome is written (ack, retry, morgue, push back, reschedule, requeue at shutdown), its
held permits are released with one `RELEASE` per permit (best effort: a failure is logged at `debug`; the
lease ends within 60 s). Separate from the outcome script on purpose: a crash between the two leaves a
lease to expire (under-use for at most one lease), never an over-admission, and the hot outcome scripts
stay unchanged (§4 D9).

Renewal: every heartbeat (`beat`), after the work state, one batched `RENEW` of every held permit
(KEYS: state and stats per permit; ARGV: token, lease, limit). Results: renewed; restored (the lease had
ended or the key was lost, a Redis restart for instance, and capacity allowed taking it back); lost
(counted as an overage, logged at `warn` once per throttle per minute; the job keeps running). With the
default 10 s heartbeat (20 s at most) and a 60 s lease, a lease ends only if renewals stop for 40 to 60 s
(a runtime stall, a partition) or the process dies.

Shutdown: jobs interrupted by the hard shutdown are requeued by the manager as today; their permits are
then released (bounded to 5 s with the other cleanup steps) or expire.

**Why after the fetch and not in the fetch script.** Per-key limits need the job's key; reading it inside
the fetch script would mean decoding every job (cost proportional to payload size) and touching keys the
script did not declare (not allowed on Dragonfly, and unsafe for any future cluster support). A Redis list
cannot skip its head anyway (BullMQ removed group keys for this reason). Moving refused jobs out of the
queue is what keeps them from blocking the jobs behind them.

**Fairness and cost.** A refusal costs one fetch, one `ACQUIRE` and one `PUSH_BACK`: a processor parks about
600 refused jobs per second at 0.5 ms round trips, so a burst of 10 000 jobs of one throttled tenant is
parked in seconds while other tenants' jobs, interleaved in the same queue, keep running. Parked jobs come
back at their slots, spaced at the limit's rate, so most are granted on their first return (the scheduler
moves about as many jobs per second as the throttle grants). Ordering per key is approximately FIFO
(slots are handed out in refusal order); a returning job that loses its slot to a newcomer goes to the end
of the line. With `reschedules` unlimited (the default for throttles) a parked job never fails because of
its throttle.

### 2.7 Queue limits in the fetch script (phase 3)

- **Data**: `queue-limits` HASH, field = queue name, value = the normalized spec (`c=5;w=100/60000`:
  concurrency, window, bucket, leaky allowed). State keys use the limiter namespace with the reserved name
  `@<queue>` (`limit:c:@exports`, `limit:w:@exports`, stats `limit-stats:c:@exports`): `@` cannot appear
  in limiter names, so no collision.
- **Cache**: each process reads `queue-limits` (`HGETALL`) at start and on every heartbeat. A capsule none
  of whose queues has limits keeps using today's `RELIABLE_FETCH`/`BASIC_FETCH` byte for byte. Otherwise its
  non-blocking step uses `FETCH_GATE` (§2.9.7): per queue in fetch order, paused queues are skipped, the
  script compares the limits the process believes with `HGET queue-limits <q>` (stale: returns `{2, name}`,
  the process re-reads the hash and retries at once); an unlimited queue is fetched as today; a limited,
  non-empty queue is checked (all its limits, atomically) and, if granted, `LMOVE`d with its permit token
  (a random 16-hex token per fetch, kept with the unit of work); an over-limit queue is skipped and
  reported with its wait. A limit set or changed through the API takes effect in every process within one
  heartbeat interval (the cache refresh), or at once where the process already uses `FETCH_GATE`.
- **Local cooldown**: a skipped queue is left out of the fetch order (script and blocking step) until its
  reported wait ends (rate limits) or for 250 ms (concurrency; ended early when this process releases a
  permit of that queue). No other queue is ever excluded (L9).
- **Blocking step**: `BLMOVE` may wait on a limited queue that is not cooling down. A job taken that way
  from a limited queue is then checked with `ACQUIRE` (token = the fetch's token); refused, it goes back to
  the **front** of its queue with `REQUEUE` (order kept) and the queue cools down. This keeps idle latency
  at `BLMOVE`'s while charging limits for every job.
- **Release and renewal**: the queue's concurrency permit is an engine-held permit like a throttle's
  (heartbeat `RENEW`, released after the outcome, 60 s lease).
- Ordering is FIFO and nothing churns: a limited queue's jobs wait in the queue itself (BullMQ's and
  Faktory's model; Oban Pro's per queue limits).

### 2.8 Redis data model

All keys come from `hench::keys` helpers; un-namespaced like every hench key. The prefixes `limit:`,
`limit-stats:`, `limits`, `l|` and `queue-limits` are reserved by this design (they do not overlap
`unique:`, `unique-locks`, `u|` (unique), `periodic…` (periodic), `batch:`, `batches`, `batch-events`,
`batch-maintenance`, `b|` (batches), or metrics `j|`/`h|`).

| Key (helper) | Type | Contents | Expiry |
|---|---|---|---|
| `limit:c:<id>` (`keys::limit_state('c', id)`) | ZSET | member = permit token (`<jid>~<16 hex>` for engine-held permits since round 3, 16 random hex otherwise), score = lease end (ms, Redis clock) | extended to at least the longest lease by grants and renewals; deleted when the last lease is released |
| `limit:w:<id>` | LIST | grant times (ms), newest first, at most `limit` entries | `period + 1 s` after the last grant |
| `limit:b:<id>` | HASH | `b` bucket index (`floor(ms / interval)`), `n` units used | end of the bucket + 1 s |
| `limit:g:<id>` | STRING | theoretical arrival time (ms, 3 decimals) of the leaky or points bucket | until the bucket is empty + 1 s |
| `limit-stats:<k>:<id>` (`keys::limit_stats`) | HASH | `p` signature (`w\|100\|1000`), `last` (ms), counters `g` granted, `r` refused, `w`/`wms` grants after waiting and total wait (ms), `h`/`hms` releases and total hold (ms), `rc` reclaimed leases, `ov` overages (lease lost, release after the lease ended), `restored`, `adj` points corrections; `line` wait-line end (ms) | `stats_ttl` (24 h) after the last use |
| `limits` (`keys::LIMITS`) | ZSET | member = `<k>:<id>`, score = its stats' expiry (epoch s) | none; members expire by score and are trimmed (10 per grant, and by API reads) |
| `l\|YYYYMMDD\|H:MM` (`keys::limit_minute(at)`) | HASH | `<name>\|g`, `<name>\|r` for the minute (per name, not per key; UTC, client clock like `metrics_minute`) | 8 h (`keys::FEATURE_MINUTE_TTL`) |
| `queue-limits` (`keys::QUEUE_LIMITS`, phase 3) | HASH | field = queue, value = spec | none (configuration) |

Identity rules: `name` is 1 to 100 characters of `[A-Za-z0-9_.-]`, not `.` or `..` (dot segments in the
dashboard's and the JSON API's URLs, round 3) (`~` and `@` are reserved for generated
names: duplicate limits inside a throttle get `~2`, `~3`; queue limits use `@<queue>`); `key` is any UTF-8
text without NUL of at most 128 bytes, else `#` + 22 base64url characters of its SHA-256 (crate-wide helper). `id` = `name` or `name:key`
(the first `:` separates them: names have none).

Memory (measured, Redis 7.0.15, Appendix A): window state about 10 bytes per unit (1 160 B at 100, 10.4 KB
at 1 000); leaky, points and bucket state 88 B; concurrency state about 30 B per holder of a limiter's permit and
60 B per throttled job (its token is the jid, `~` and 16 hex digits; about 140 B each beyond 128
holders, where Redis stops packing the set; measured at integration, round 3); stats hash about
120 B; index entry about 60 B. One million per-tenant leaky limiters used within a day keep about 180 MB
of stats and index entries (none with `stats(false)`), plus 88 B for each tenant whose bucket is not yet
empty (state disappears once it carries no information).

Job metrics (`j|…` minute and ten-minute hashes) gain two fields per class: `<class>|o` (over-limit
reschedules of running jobs) and `<class>|t` (throttled before starting). The query ignores unknown fields
today (`metrics/query.rs`: `_ => return`), so older readers are unaffected. The batches design adds a third
in the same way, `<class>|s` (members skipped because their batch was cancelled); the three are added
together to `metrics::Outcome` and `Counts` in the common phase (cross-feature review).

The per-minute counter hashes of the four features share one convention (cross-feature review): key
`<letter>|YYYYMMDD|H:MM` from `keys::<feature>_minute(at)` (`u` unique, `b` batches, `l` limits), UTC
minute of the writing client, `HINCRBY` under `pcall` as a script's last writes, 8 h expiry, no rollup,
read for minute periods of at most 8 hours by one crate-wide reader (`Limits::events` refuses longer
periods with `Error::Config`).

### 2.9 Lua scripts (`crates/hench/src/limit/scripts.rs`)

#### 2.9.1 Rules every limiter script follows

1. **Declared keys only.** Every key in `KEYS`; no `SCAN`, no key built in Lua.
2. **Redis 6.2 commands only**; no `PEXPIRE GT` (Redis 7): "extend to at least" is `PTTL` + `PEXPIRE`.
3. **Time from Redis**: `TIME` → ms. `ARGV[1]` may carry a fixed `now` for tests only (production code
   always sends `''`; a unit test pins this). `TIME` followed by writes is allowed under effects
   replication (Redis ≥ 5; the only mode in 7). On 6.2 with `lua-replicate-commands no` scripts would fail;
   `start` logs a warning when `CONFIG GET lua-replicate-commands` answers `no` (skipped if `CONFIG` is
   refused). That check is one of the common start-up checks (cross-feature review): every script of the
   unique, periodic and batches designs also reads `TIME` before writing.
4. **Check, then write.** Every limit of the call is checked with reads only; writes start only when the
   call is granted (G3).
5. **Out of memory.** Redis refuses a script's writes only before its first write (the unique design
   verified this on 7.0.15). A grant's first write is the grant itself (`ZADD`/`LPUSH`/`HSET`/`SET`): under
   `OOM` nothing is granted and the caller gets the error (fail-closed). Bookkeeping always runs under
   `pcall`, so a refusal still answers under `OOM` (verified), and the index trim (a removal, which would
   let later writes bypass the limit) runs last.
6. **Never shorten** a key's TTL; every state write sets or extends one (G6, L3).
7. **Tokens make concurrency idempotent**: a repeated acquire with the same token is granted without a new
   slot; releases and renewals compare the token.
8. Bookkeeping is skipped entirely when the call says so (`stats(false)`): no stats, no line, no index.
9. **Conventions shared by the four features** (cross-feature review): the scripts are `.lua` files under
   `crates/hench/src/limit/lua/` (the module stays `limit/scripts.rs` for the Rust wrappers), assembled by
   the crate-wide script builder after the shared prelude; helpers use the `hl_` prefix (unique `hu_`,
   batches `hb_`, periodic `hp_`); `TIME` is read once per script; names carry the feature prefix in code
   and logs, so they never collide with the unique design's `UNIQUE_RELEASE`/`UNIQUE_RENEW` or the
   fetcher's `RELEASE`: `LIMIT_ACQUIRE`, `LIMIT_RELEASE`, `LIMIT_RENEW`, `LIMIT_ADJUST`, `LIMIT_USAGE`,
   `LIMIT_RESET`, `LIMIT_CLEAR_LINE`, `FETCH_GATE`. The sections below keep the short names. The heartbeat
   sends `LIMIT_RENEW` and the unique design's `UNIQUE_RENEW` in one pipeline.

#### 2.9.2 `ACQUIRE` (prototype `acquire.lua` + `lib.lua`)

- KEYS: per limit `i`: `[2i−1]` state, `[2i]` stats; then `[2n+1]` `limits`, `[2n+2]` the minute's
  events hash.
- ARGV: `[1]` now override (`''`), `[2]` token (`''` without concurrency limits), `[3]` n, `[4]` ticket
  (the slot ms this caller was given earlier, or `''`), `[5]` flags (`L` wait line, `N` no bookkeeping),
  `[6]` waited ms (`''` or the time already waited, for the stats of a grant); then 9 per limit: kind
  (`c`/`w`/`b`/`g`), limit, period ms (lease ms for `c`), cost, stats TTL ms, index member (`<k>:<id>`),
  name (for the minute counters), signature, default spacing ms (`c`: `1000 / limit`).
- Steps: checks (§2.3 table); all granted → grants in order (first write = first grant), then per limit
  `HINCRBY g`, `HSET p last`, `PEXPIRE stats`, minute counter, `ZADD limits` (all `pcall`), `w`/`wms` if
  waited, `rc` += reclaimed; index trim (10 expired members) last; return
  `{1, reclaimed, held, previous signature if it differs else ''}`. Refused → spacing per failing limit
  (`c`: measured `hms / h / limit`, else the default; others `period × cost / limit`), `own_wait` (`c`:
  one spacing), the latest start among failing limits; with `L`: a ticket at or after the start returns
  `ticket − now` without touching the line; otherwise `start = max(start, line_i)` over failing limits and
  each failing limit's line becomes `max(line_i, start + spacing_i)`; `HINCRBY r` and the minute counter
  (`pcall`); return `{0, wait_ms, index of the limiting limit, impossible (1 when cost > limit), slot_ms}`.
- Costs: one script; about 4 + 6 per limit commands with bookkeeping.

#### 2.9.3 `RELEASE` (prototype `release.lua`)

KEYS: state, stats. ARGV: now override, token, acquired-at ms. `ZSCORE`; missing → `ov` +1, return 0
(lost: reclaimed after its lease, or reset); else `ZREM`, `h` +1, `hms` += held time, `DEL` the key when
empty, and if the lease had ended → `ov` +1, return 2; else 1. Sent once per release (repeating is
harmless: the second finds nothing and counts an overage, so the client does not retry it).

#### 2.9.4 `RENEW` (prototype `renew.lua`)

KEYS: state, stats per permit. ARGV: now override, n, then token, lease ms, limit per permit. Live lease →
`ZADD XX` + extend TTL → 1. Ended or missing and `ZCOUNT (now, +inf) < limit` → re-added (`restored` +1)
→ 2. Otherwise → removed if present, `ov` +1 → 0 (lost). Used by permit tasks (one permit) and the
heartbeat (all engine-held permits).

#### 2.9.5 `ADJUST` (prototype `adjust.lua`)

Points correction: KEYS state, stats; ARGV now, signed delta units, capacity, refill ms; §2.3 rule;
returns the new level (units).

#### 2.9.6 `USAGE`, `RESET`, `CLEAR_LINE`

`USAGE` (read only; prototype `usage.lua`): used units and when the next unit frees (`c`: earliest lease
end, a worst case), plus `HGETALL stats`. `RESET`: `DEL state stats`, `ZREM limits member` (API and
dashboard). `CLEAR_LINE`: `HDEL stats line`.

#### 2.9.7 `FETCH_GATE` (phase 3; prototype `fetch_gate.lua`)

KEYS: in-progress list, owners set, `paused`, `queue-limits`, then 4 per queue (queue list, concurrency
state, rate state, stats). ARGV: now override, identity, permit token, then per queue: name, the limits the
process believes it has (`''` = none). Returns `{1, {queue key, job}, token or ''}`, `{0, {open unlimited
queue keys}, {name, wait ms, …}}` or `{2, name}` (stale view). An empty limited queue is not charged
(`LLEN` first). The grant's writes follow the `LMOVE` (both in one script).

#### 2.9.8 Reschedule

No new script: `PUSH_BACK` in its `schedule` mode (`ZADD schedule <at> <member>` before `LREM`), and the
unique design's `PUSH_BACK_UNIQUE` (lock held, TTL `for` + delay) for unique jobs.

### 2.10 Engine and spine integration (file by file)

- **`limit/`** (new, spine-owned module with engine hooks): types of §2.4, script invocations through
  `RedisPool::with_retry` (a dropped connection resends once: idempotent for concurrency tokens; for rate
  kinds a resend may charge a unit twice, which only under-admits; timeouts are never resent, as
  everywhere in hench), the wait loop (sleep toward the slot with `tokio::time::sleep`,
  at least 10 ms, plus up to 10 % jitter; concurrency polls with jittered backoff 25 ms → 400 ms and wakes
  on local releases of the same identity through a process-wide `Notify` map; the `JobContext`'s stopping
  token ends a wait at once with the `OverLimit`), permit renewal tasks (`tokio::spawn`, every lease / 3,
  stopped by a `CancellationToken` when the permit is released or dropped), `Drop` release (spawned on
  `Handle::try_current()`), in-memory backend (`memory.rs`, §2.14), `Throttles` registry and resolution
  (`throttle.rs`), queue limit cache and spec grammar (`queue.rs`, `spec.rs`).
- **`error.rs`**: `Error::OverLimit`, `JobError::OVER_LIMIT`, `over_limit`, `as_over_limit`,
  `is_over_limit`.
- **`keys.rs`**: `limit_state`, `limit_stats`, `LIMITS`, `limit_minute`, `QUEUE_LIMITS`, TTL constants
  (`LIMIT_STATS_TTL` 24 h, the shared `FEATURE_MINUTE_TTL` 8 h, `DEFAULT_LEASE` 30 s, `ENGINE_LEASE` 60 s).
- **`payload.rs`**: `throttle`, `limited` fields with lenient deserializers; `RESERVED_KEYS`.
- **`options.rs`**, **`job.rs`**, **`client.rs`**: `throttle` option and setters; `Job::throttle_key`,
  `Job::throttle`, `ErasedJob::dyn_throttle`; normalization writes the payload's `throttle`.
- **`config.rs`**: `Config` gains `throttles: Arc<Throttles>`, `over_limit: OverLimitConfig` (classifier,
  backoff, reschedules, max delay), `queue_limits` (seed); builder methods of §2.4; `build` validation.
- **`processor.rs`**: throttle gate (§2.6), over-limit branch (§2.5), held-permit release after outcomes,
  metrics outcomes. The reschedule (delay, jitter, `limited`, `Stored::Scheduled` encoding) lives in a
  new `limit::reschedule` and is written with the existing `Fetcher::push_back`.
- **`fetch.rs`**: nothing for reschedules (`push_back` already writes `Stored::Scheduled`); phase 3: `FETCH_GATE`, cache,
  cooldown, blocking-step post-check, permit tokens on `UnitOfWork` (`pub(crate) permit: Option<String>`).
- **`launcher.rs`** (heartbeat): batched `RENEW` of `engine.permits` (in one pipeline with the unique
  design's `UNIQUE_RENEW`); queue-limits cache refresh (phase 3);
  `info` JSON gains `"throttles": {"stripe": "c=25;w=5/1s per key"}` (definitions in use, for the
  dashboard) and `"limits": 1` in the crate-wide `"features"` map (older workers treat `OverLimit` as a
  failure; the dashboard names them). Startup: seeding `queue_limits`; the `lua-replicate-commands`
  check is a common start-up check.
- **`manager.rs`**: after the hard-shutdown requeue, release the permits of the requeued slots.
- **`iterable/mod.rs`**: §2.5.
- **`metrics/tracker.rs`**: `Outcome::OverLimit`, `Outcome::Throttled`; `ClassStats` gains `over_limit`,
  `throttled`; flushed as `o`, `t`. `metrics/query.rs`: `Counts` gains `over_limit`, `throttled`.
- **`job_logger.rs`**: `over_limit` (info) and `throttled` (debug) events.
- **`testing.rs`**: §2.14. **`api/`**: §2.13. **`cli.rs`**: §2.17.
- **`hench-macros`**: `throttle`, `throttle_by`, inline `throttle(...)` (§2.4.3).

### 2.11 Behaviour under failures

| Situation | Behaviour |
|---|---|
| Process crash holding user or engine permits | Leases are no longer renewed: each slot frees within its lease (30 s user, 60 s engine). Reliable fetch recovers the job later (heartbeat expiry 60 s + `orphan_grace` + next orphan check), by which time its lease has ended; the recovered job acquires again (round 3: with a token of its own; a lease of the crashed execution still alive ends with its lease). |
| Process crash between a reschedule and the permit release | The job is in `schedule`; the lease ends by itself (under-use for at most one lease). |
| Crash between a grant and the job start | Engine-held: the lease ends; the job is recovered later. User permit: the lease ends. |
| Hard shutdown | Interrupted jobs are requeued as today; their permits are released (bounded 5 s) or expire. Limiter waits observe `ctx.stopping()` and return `OverLimit` at once, so a shutdown never waits for a limiter. |
| Retries | A failed job releases its permits after its retry is written; reschedules never touch `retry_count`, `failed_at` or error fields; a retried job that hits a limit is rescheduled (its `limited.n` continues). |
| Reliable-fetch own-list reconciliation | Unchanged; permits are tied to tokens, not to the in-progress list. |
| Lost reply on `ACQUIRE` | Dropped connection: resent once. Concurrency: same token, idempotent. Rate kinds: the unit may be charged twice (under-admission only). Timeout (not resent): the call fails; for concurrency the client sends a best-effort `RELEASE` of the token, and the lease ends anyway; for rate kinds the unit may be consumed. |
| Lost reply on `RELEASE`/`RENEW` | Release: the lease ends by itself. Renew: the next renewal (10 s later) catches up; a lease is lost only after a lease's worth of failures. |
| Lost reply on the reschedule `PUSH_BACK` | Repeated by `persist`; the `LPOS` guard makes it run once. |
| Redis unreachable | Limiter calls fail with the Redis error (fail-closed; `fail_open` grants rate kinds). Throttle gate: requeue + outage notice (or run, with `fail_open`). Renewals keep failing; leases end; permits are flagged lost after a lease without renewal. |
| Redis restart without persistence, `FLUSHALL`, failover to a replica that missed writes | Limits restart from zero: up to one extra window's worth may be admitted; running holders are restored by their next renewal when capacity allows, else counted as overages. Documented as the limit of the guarantees (G1, G2 hold per Redis lifetime). |
| `OOM` | Grants are refused (error, fail-closed); refusals still answer; reschedules are retried by `persist` until memory frees (like any outcome write). |
| Eviction (`allkeys-*`, `volatile-*`) | Limiter keys have TTLs and are evicted first under `volatile-*`: limits reset (over-admission). `start` already warns unless `noeviction`. |
| Clock skew between hosts | Irrelevant to decisions (Redis `TIME`). Only `limited.at` and the minute of the event counters use the worker's clock. |
| Redis clock jumps (failover to a host with another clock) | Backward: leases, windows and lines last longer (stricter, never looser). Forward: they end early (looser by the jump). Fixed buckets follow the new clock. Keep NTP on Redis hosts. |
| Throttle name unknown to this worker | The job runs unthrottled with a rate-limited `warn` (raw pushes, mixed deployments). |
| Mixed versions during a rollout | Older workers ignore `throttle`/`limited` (extra keys) and treat `OverLimit` as a failure (retry). Deploy workers before relying on throttles. |
| A key of the wrong type (`WRONGTYPE`) | The call fails with the error (the kind letter in key names prevents clashes between kinds). |

### 2.12 Interplay with the rest of hench

- **Retries**: §2.5, §2.11. `Job::retry_in` can tell over-limit failures apart (`error.is_over_limit()`).
- **Iterable jobs**: §2.5; the saved cursor survives reschedules; a reschedule counts as an execution
  (`ex` +1) since the job ran.
- **Unique jobs** (`/home/user/scratch/design/unique.md` §2.18): reschedules keep the lock
  (`PUSH_BACK_UNIQUE`, TTL `for + delay`); a throttled job never reached `PerformEndpoint`, so a
  `Start` lock was not released; an over-limit refusal raised by `perform` of a `Start` job takes the lock
  back if free (the unique design's rule for jobs waiting again). The unique design's "while executing"
  need is met here: `Throttle::new("one-at-a-time").concurrency_per_key(1)` with `throttle_by`.
- **Batches** (`/home/user/scratch/design/batches.md` §2.18, written in parallel): its contract asks that
  an over-limit job be put back through the processor's push-back path, never by server middleware halting
  the job and pushing a copy (the halt would count as a success). This design does exactly that: a
  reschedule (over limit or throttled) goes through `Fetcher::push_back` and is not an outcome, so the job
  stays pending in its batch and no batch callback fires because of it (that design's table: "rate limiter
  reschedules it: none"). A job that exhausts its reschedules fails like any failure and is counted by the
  batch's failure accounting. The throttle gate runs before that design's cancellation check (made right
  before `perform`); a member the gate refuses is checked for cancellation on the spot and skipped
  instead of parked when its batch is cancelled (§2.6 step 4, adopted by the cross-feature review). A
  member granted by the gate and then skipped by the endpoint releases its permit after the
  acknowledgement, like any outcome.
- **Periodic jobs** (`/home/user/scratch/design/periodic.md` §2.10): `RECORD_RUN` reports an over-limit
  outcome as `rescheduled` (neither `ok` nor `failed`; the periodic design adds the status), and nothing
  for a throttled job that did not start. Ticks of a throttled periodic job are enqueued normally and
  wait in `schedule` like any throttled job. `Periodic::job::<J>` evaluates `J::throttle_key(&args)`
  when the schedule is defined and stores it in the template (fire-time normalization is untyped), and
  `Periodic` has `throttle`/`throttle_key` setters.
- **Scheduler**: unchanged; it moves rescheduled jobs back to their queues through client middleware (so
  middleware sees them as it sees retries). It runs independently of fetch, so delayed jobs are always
  promoted (L10). Its sequential one-job-at-a-time moves bound the throughput of parked jobs returning
  (§5 R1).
- **Paused queues**: unchanged; queue limits are checked after the pause check.
- **Server middleware**: throttles run before the chain (a throttled job runs no middleware); limiters
  inside middleware work like limiters inside jobs (an `OverLimit` returned by middleware reschedules).
- **Client middleware**: not run by the reschedule itself; run when the scheduler moves the job.
- **Capsules**: throttles and queue limits are cluster-wide, independent of capsules.
- **API moves** (`retry`, `add_to_queue`, `reschedule`): unchanged; `limited` stays in the payload.

### 2.13 API (`hench::api`)

New `api/limits.rs`; `Api::limits()`:

```rust
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct LimitInfo {
    pub kind: LimitKind,                 // Concurrency, Window, Bucket, Leaky (points included)
    pub name: String,
    pub key: Option<String>,
    pub queue: Option<String>,           // for queue limits (`@<queue>`)
    pub spec: Option<Limit>,             // parsed from the signature
    pub used: f64,
    pub next_free_in: Option<Duration>,
    pub granted: u64, pub refused: u64,
    pub waited: u64, pub wait_time: Duration,
    pub releases: u64, pub hold_time: Duration,
    pub reclaimed: u64, pub overages: u64, pub restored: u64,
    pub line_ends_at: Option<SystemTime>,
    pub last_used: Option<SystemTime>,
}

impl Limits {
    pub async fn size(&self) -> Result<u64>;                                   // live index entries
    pub async fn page(&self, page: usize, size: usize, order: LimitOrder) -> Result<Page<LimitInfo>>;
    pub fn entries(&self) -> impl Stream<Item = Result<LimitInfo>> + Send + 'static;
    pub async fn get(&self, kind: LimitKind, id: &str) -> Result<Option<LimitInfo>>;
    pub async fn holders(&self, id: &str) -> Result<Vec<Holder>>;            // concurrency: token, lease end
    pub async fn reset(&self, kind: LimitKind, id: &str) -> Result<bool>;
    pub async fn clear_line(&self, kind: LimitKind, id: &str) -> Result<bool>;
    pub async fn events(&self, period: metrics::Period) -> Result<LimitEvents>; // per name, per minute; minute periods ≤ 8 h
    pub async fn waiting(&self, id: &str, max: usize) -> Result<Vec<SortedEntry>>; // scheduled jobs whose `limited.by` names it (bounded scan, like the dashboard's filters; matched in Redis since round 3)
}
```

Supporting types (all `#[non_exhaustive]`): `LimitKind::{Concurrency, Window, Bucket, Leaky}` (the state
shape: points limits are `Leaky` with a `Points` spec); `LimitOrder::{LastUsed, Name, Refused}`;
`Holder { token: String, lease_ends_at: SystemTime, ended: bool }`; `LimitEvents { totals: (u64, u64),
by_name: BTreeMap<String, Vec<(SystemTime, u64, u64)>> }` (granted and refused per minute, like
`metrics::Query`); `Limited { n: u32, by: String, at: Option<SystemTime> }`. Pages read the index by
score (expiry ≈ last use), pipeline `USAGE` per entry, skip and trim entries whose state and stats are
gone. `JobRecord::limited() -> Option<Limited>` and `JobRecord::throttle() -> Option<ThrottleRef>`
(lenient).

### 2.14 Testing modes

- `Testing::limits(TestLimits)` with `TestLimits::{Unlimited (default), InMemory}`; `Testing::from_hench`
  copies the throttles, not the mode. `Client::fake()` and `JobExt::perform_inline` are unlimited.
- `InMemory`: one `LimitTable` per testing client (shared by its clones; no leakage between tests): a Rust
  port of the five algorithms, the wait line and leases, on `tokio::time::Instant` (so
  `tokio::time::pause()` and `advance()` drive it deterministically). Throttles are enforced on `drain` /
  `perform_one` / inline execution.
- Fake clients: an over-limit or throttled job stays captured, with `at` set to its slot and `limited`
  updated; a job rescheduled during a `drain` call is not run again by that call (no infinite loops); the
  next `drain` tries it again. Helpers: `Client::limited_jobs::<J>()` (captured jobs with `limited`).
- Inline clients: an over-limit refusal is returned to the pusher as `Error::Job` (class
  `hench::OverLimit`), like a failure (documented: inline runs once; use fake clients and `InMemory` to
  test throttling).
- Unit tests of user code: `Limiter::unlimited()`; `limit::testing::memory_target()` (a `LimitTarget`
  backed by a fresh in-memory table) for testing code that uses limiters outside jobs.
- With the other features (cross-feature review; the testing modes hold one table per feature, shared by
  a client, its clones and its collecting clients): with `InMemory`, a captured job refused by a throttle
  or a limiter keeps its unique lock (the unique design's table, as `PUSH_BACK_UNIQUE` does) and stays
  pending in its batch (not an outcome), except that a refused member of a cancelled batch is skipped, as
  in workers; an inline client's over-limit refusal is a failure for the batch table too (the member
  counts as dead: nothing would run it again).
- Default off, while the unique design's testing default is on: the rule across the four features is
  that push-time, deterministic behaviour (locks, batch accounting) is enforced by testing clients by
  default, and time-driven behaviour (limits, periodic polling) is not.

### 2.15 Dashboard (`hench-web`)

- `HenchWeb::limits(bool)` (default true) shows the **Limits** tab; off, its routes answer 404. Navigation
  order across the features (cross-feature review): Dashboard, Busy, Queues, Retries, Scheduled, Dead,
  Periodic, Batches, Limits, Locks, Metrics, then custom tabs. Notices use the existing flash format
  (`kind.count.failed.subject`) with new kinds `LimitsReset` and `LineCleared`.
- `GET /limits` (`?page`, `?count`, `?sort=last|name|refused`, `?substr=` on name and key, the dashboard's
  filter rules): summary (`N limiters in use. Last hour: G granted, R refused.` from the minute counters,
  and the five names with the most refusals); declared throttles of live processes (from heartbeat `info`)
  with their specs; rows: name and key (control characters spelled out), kind chip, spec, a usage bar
  (`37 / 50`, colored by fraction with the validated palette), next free in, granted, refused, wait line
  (`line clears in 4 min`), last used, Reset button (confirmation). "Live" switch like other lists.
- `GET /limits/{kind}/{id}` (`id` percent-encoded): every field of `LimitInfo`; concurrency holders (token,
  jid link when the token is a jid found in the busy work state, lease end, overage chip when ended);
  waiting jobs (`Limits::waiting`, at most 100, with links to their scheduled entries; the page is not
  Live, since each view searches the schedule: round 3); actions Reset,
  Clear line. `POST /limits/{kind}/{id}` (`reset` | `clear_line`), `POST /limits` (bulk reset of
  selected rows, at most 1000). Flash codes, CSRF, read-only and fetch-metadata checks like every action.
- Scheduled list and entry pages: a `limited` chip on rescheduled jobs (`over limit: w:stripe:42 ×3`) and a
  **Limited** row on detail pages; a `throttle: stripe:42` chip where the payload names one. The feature
  rows of job pages come after the built-in rows and the Iteration row, in one order for every feature:
  Batch, Periodic, Unique, Throttle, Limited, then the host's `job_info` rows.
- Queues page (phase 3): a limit chip (`c=5`) per limited queue; the queue page shows its limits with
  usage, and (not read-only) a form to set or clear them (spec text field validated server side).
- Metrics pages: the per-class table gains "Over limit" and "Throttled" columns (and "Skipped", the
  batches design's `s`), each shown only when some class has a non-zero value in the period, so
  deployments without these features see today's table; the class page shows them per minute in its
  table view.
- Strings in `strings.rs`; no new JavaScript beyond the existing list behaviour.

### 2.16 Server (`hench-server`)

- `hench limits [--filter TEXT] [--sort last|name|refused] [--limit 50] [--json]`: table `LIMIT KIND SPEC
  USED NEXT-FREE GRANTED REFUSED LAST-USED`, then the last hour's totals per name.
- `hench limits show <kind>:<id> [--json]`; `hench limits reset <kind>:<id> --yes`;
  `hench limits clear-line <kind>:<id>`.
- Phase 3: `hench queues limit <queue> (--set "c=5;w=100/1m" | --clear)`; `hench queues` shows limits.
- JSON API (`--api`): `GET /limits?filter=&limit=`, `GET /limits/{kind}/{id}`, `DELETE /limits/{kind}/{id}`
  (reset; refused when read-only), phase 3 `PUT /queues/{name}/limits` (`{"limits": "c=5;w=100/1m"}`) and
  `DELETE /queues/{name}/limits`. `GET /queues` adds `limits`.
- `hench stats limits` (top refusals of the last hour), also printed by `hench stats all` when limiters are
  in use; `hench limits` alone is `hench limits list`, like the other feature commands.
- `hench enqueue ... [--throttle NAME[:KEY]]` and `POST /jobs` fields `throttle` (name) and `throttle_key`
  (cross-feature review: the server knows no job types, so raw pushes from other services need them, as
  they need the unique design's `--unique-for`); an unknown name is accepted (workers run such jobs
  unthrottled with a warning, §2.6).

### 2.17 Configuration

- Builder: `throttle`, `queue_limits`, `over_limit_when`, `over_limit_backoff`, `over_limit_reschedules`,
  `over_limit_max_delay`; `Testing::limits`; `HenchWeb::limits`.
- **Spec grammar** (TOML, CLI, dashboard, `Limit: FromStr`): `kind=N[/period][ per key]`, kinds
  `concurrency` (`c`), `window` (`w`), `bucket` (`b`), `leaky` (`l`), `points` (`p`); periods use the
  `hench enqueue --in` duration syntax (`500ms`, `1s`, `5m`, `1h30m`, `1d`; bucket also `second`,
  `minute`, `hour`, `day`); several specs joined by `;`. Examples: `concurrency=10`,
  `window=100/1s per key`, `bucket=10000/day`, `leaky=60/1m`. Normalized form for Redis: `c=10`,
  `w=100/1000`, `b=10000/86400000`, `l=60/60000`.
- TOML (`cli` feature), checked like every other part of the file (unknown keys, malformed specs):

  ```toml
  [throttles.stripe]
  limits = ["concurrency=25", "window=5/1s per key"]
  reschedules = 200        # optional; default unlimited
  max_delay = 3600         # seconds; default 86400
  max_wait = 0.5           # seconds; default 0
  fail_open = false

  [queue_limits]           # phase 3; written to Redis at start (code and file win over edits)
  exports = "concurrency=3"
  ```

  Today every top-level table other than `capsules` is an environment section (§9e): `throttles` and
  `queue_limits` join `capsules` as reserved table names, accepted at the top level and inside an
  environment section (shallow merge, like the rest of the file: an environment's `throttles` table
  replaces the top-level one as a whole). A file whose environment is literally named `throttles` or
  `queue_limits` gets an `Error::Config` naming the reserved word (release note). The periodic design
  reserves `periodic` the same way (its `[[periodic]]` array), so the reserved names are `capsules`,
  `throttles`, `queue_limits` and `periodic`. Precedence as in §9e:
  file (environment section > top level) over the program's builder; throttles defined in the file
  replace same-named ones from code. Spec letters (`c`, `w`, `b`, `l`, `p`) are not the state-key letters
  (`c`, `w`, `b`, `g`: leaky and points share one state shape).

### 2.18 Metrics and logging

- Per limit identity: counters in its stats hash (written by the scripts: works from any process,
  including web processes that have no metrics tracker); per name and minute: granted and refused in
  `l|…` hashes; per job class: `o` and `t` in the job metrics.
- `tracing`: `info` `over_limit` job event (limit label, delay, `n` of `max`); `debug` for throttled jobs
  and every grant after a wait; `warn` (rate limited to one per identity per minute) for lost or restored
  leases, overages, unknown throttle names, signature mismatches, `fail_open` grants; the throttle gate's
  Redis errors go through the fetch outage notices.

### 2.19 Performance (measured, Appendix A)

| Operation (Redis 7.0.15, 20 clients, unix socket) | Throughput per Redis core |
|---|---|
| Trivial script (`return 1`) / `INCR` | 119 000 / 117 000 per s |
| `ACQUIRE` leaky or window, one limit, with bookkeeping | 33 000 to 34 000 per s |
| same without bookkeeping (`stats(false)`) | 50 000 (leaky) to 59 000 (window) per s |
| `ACQUIRE` window 100 000/1 s (index lookup deep in a long list) | 31 000 per s |
| `ACQUIRE` concurrency (grants, random tokens) / lean | 27 000 / 39 500 per s |
| `RELEASE` | 64 500 per s |
| `ACQUIRE` of two limits at once (window + concurrency) | 18 000 per s |
| Current `RELIABLE_FETCH`, 3 queues | 73 000 per s |
| `FETCH_GATE`, no limited queue / first queue limited and granted / first queue skipped | 59 000 / 38 700 / 56 400 per s |

So an acquire costs about 3.5 times the Redis CPU of a plain command, bookkeeping about 40 % of that;
jobs without throttles pay nothing (the gate reads one field of the parsed payload); capsules without
limited queues keep today's fetch script.

### 2.20 Documentation to update with the implementation

- `docs/DESIGN.md`: §2 keys (five rows) and payload table (`throttle`, `limited`); §3 macro options and
  `Job::throttle_key`/`throttle`; §4 normalization of `throttle`; §5 processor (throttle gate, over-limit
  outcome through `push_back`, permit release), heartbeat (`RENEW`, queue-limits cache), fetch (phase 3
  gate); §7 `Limits`, `Queue::limits`; §8 testing; §9 dashboard routes, server commands, metrics
  fields; new §9j "Rate limiting" (this design, condensed; the cross-feature review numbers §9g unique,
  §9h periodic, §9i batches, §9j rate limiting, §9k the features together); §9a ownership (a `limit` lane touching spine,
  engine, api, testing, metrics, web, server, macro); §10 test plan rows.
- `README.md`: "Rate limiting and throttling": how limiting differs from throttling (and which level to use),
  limiter kinds with one example each, reschedule semantics and counters, throttles with keys, queue
  limits, choosing leases and `max_wait`, what the dashboard shows, testing.
- `docs/STATUS.md`: feature lines; remove "rate limiting" from the out-of-scope list.

### 2.21 Implementation plan (each step merges with its tests)

1. Spine: `limit` types and grammar, keys, `Error::OverLimit`, `JobError` helpers, payload fields,
   options, `Job` methods (no engine behaviour yet).
2. Scripts and Redis backend: `ACQUIRE`, `RELEASE`, `RENEW`, `ADJUST`, `USAGE`, `RESET`; `Limiter`,
   `Permit` (renewal, drop), waits; Lua scenario tests (§3.2).
3. Engine: over-limit outcome (reschedule through `push_back`), iterable integration, metrics outcomes, job
   logger; engine tests.
4. In-memory backend and testing modes; parity tests against Redis.
5. Throttles: registry, build validation, macro, processor gate, held permits, heartbeat renewal,
   shutdown release, TOML; engine and chaos tests.
6. API `Limits`, dashboard tab and chips, server commands and JSON API.
7. Phase 3: queue limits (`queue-limits`, cache, `FETCH_GATE`, cooldown, blocking-step post-check, API,
   dashboard form, CLI); fetch tests and soak scenario.
8. Docs (§2.20), STATUS, full gate.

Placement in the combined plan (cross-feature review, `pro-features.md` §6): the spine parts of step 1
(payload fields, options, `Job` methods, errors, keys, metrics outcomes) and the hook points of steps 3
and 5 in `processor.rs`, `launcher.rs`, `manager.rs`, `iterable/`, `cli.rs` and the testing modes are done
once in the common phase by the integrator; lane B ("batches and rate limiting") then owns `limit/`,
`api/limits.rs`, the throttle macro options file, the Limits pages and the `hench limits` commands. Phase 3
(queue limits) changes `fetch.rs`, which lane A changes for the unique outcome scripts: it is scheduled
after both lanes are merged.

---

## 3. Test plan

Every Redis test uses `hench::test_util::TestRedis` (a private `redis-server` per test). Script tests pass
a fixed `now` (`ARGV[1]`) so expectations are exact; one test per script runs on Redis `TIME`.

### 3.1 Unit tests (no Redis)

1. Spec grammar: every kind, short and long names, periods (`500ms`, `1h30m`, `day`), `per key`, `;`
   lists; rejections (zero limit or period, unknown kind, an `Every` interval that is not a whole number of
   milliseconds, cost > limit at call time); `Display` round trip; normalized form.
2. Identities: name rules, `~`/`@` reserved, keys over 128 bytes hashed (golden vector), `id` parsing at
   the first `:`.
3. Delay computation: hint used and clamped to [1 s, max_delay]; `60 s × n` without hint; jitter bounds
   (proptest: `delay ≤ result ≤ delay + min(delay/10, 30 s)`); limiter backoff over process backoff over
   default; panicking closures fall back and are reported.
4. Payload: `throttle`/`limited` round trip; malformed values read as `None` with a warning; reserved keys.
5. Options: `throttle` merge precedence (explicit > class > defaults), `no_throttle`, typed key at push.
6. `JobError::as_over_limit` through `?` from `hench::Error`, from a bare `OverLimit`, through
   `.context(..)`; not through a user error type wrapping it (documented); class `OVER_LIMIT` on failure.
7. In-memory algorithms (proptest against a brute-force model over random arrival times and costs):
   window never exceeds N in any period and grants whenever the model would; leaky equals the GCRA
   model; bucket per aligned interval; concurrency with leases and renewals; composite all-or-nothing.

### 3.2 Lua scripts (TestRedis, fixed `now`; the lab suite ported, plus)

1. Window: 3/1000: three grants at 0, the 4th waits 1000, at 999 waits 1, at 1000 granted; sliding
   grants at 0/400/800 then 1000 granted and 1100 waits 300; list capped at the limit; TTL present.
2. Costs: window 5/1000 cost 3 then 3 → waits 990; cost 2 granted; cost 6 → impossible.
3. Bucket: 2/1000 at 500, 900 granted, 950 waits 50, 1000 new bucket; TTL ends with the bucket; UTC day
   alignment.
4. Leaky: 3 drained in 3000: burst 3, 4th waits 1000, drip at 1000; Sidekiq's documented semantics for
   `leaky(60, 60 s)` (burst 60, one per second, 5 after 5 s, full burst after draining).
5. Points: five estimates of 200 out of 1000/20 s, sixth waits 4 s; refund 150 → 850 used; 200 more waits
   1 s; extra charge creates debt.
6. Concurrency: two holders, third refused; same token again granted without a new slot; leases end →
   granted with 2 reclaimed; release of a reclaimed lease reports lost; renew live → 1; renew missing with
   capacity → restored; renew when full → lost; release records hold time; stats counters exact.
7. Composite: window + concurrency refused by concurrency leaves the window uncharged (L8).
8. Wait line: 10/min full, five refusals get 60, 66, 72, 78, 84 s; a ticket keeps its place without
   lengthening the line; the line resets once in the past; concurrency spacing from measured hold
   (avg 2 s, limit 2 → 1 s, 2 s).
9. Every state key has a TTL after every script, for every kind (L3); stats and index entries expire.
10. `OOM` (`CONFIG SET maxmemory 1`): grants refused whole with `OOM`, nothing written; refusals still
    answer; the reschedule's `PUSH_BACK` refused and retried by `persist` once memory is back.
11. `TIME` path (no override) works with writes; `redis.replicate_commands()` not called.
12. `FETCH_GATE` (phase 3): unlimited queues behave like `RELIABLE_FETCH` (same results on the existing
    fetch test cases); stale view → `{2, name}`; limited queue granted with the token; over-limit queue
    skipped with its wait and the next queue served; empty limited queue not charged; paused before limits.

### 3.3 Limiter API (Redis)

1. `acquire` waits in process when the slot is within `max_wait` and sleeps once toward it (measured
   elapsed ≈ the hint); returns `OverLimit` at once when the slot is beyond `max_wait` (no sleep).
2. Concurrency wait: a holder releasing wakes a local waiter in under 50 ms (local `Notify`); a holder in
   another process is noticed by polling within 400 ms.
3. `try_acquire` never sleeps; `run` releases after the future completes and when it panics or is
   dropped (cancelled).
4. Permit drop releases (spawned); explicit `release` returns errors; a permit held for 3 leases is
   renewed and never reclaimed (L5, L7); `is_lost`/`lost()` after the key is deleted while full.
5. `points_used` refund and extra charge visible to the next caller.
6. `Limiter::unlimited()` makes no Redis call (a detached pool works).
7. Fail-closed: Redis stopped → `Error::Redis`; `fail_open(true)` grants rate kinds and warns.
8. A `JobContext` target: the process stopping ends a wait at once with `OverLimit` (BullMQ "quickly close
   a worker even with slow rate-limit").
9. Lost replies (the soak harness's proxy dropping replies): concurrent acquire resent with the same token
   uses one slot; rate acquire never over-admits.
10. Signature mismatch between two definitions of one identity logs one warning.

### 3.4 Engine (real workers)

1. A job returning `OverLimit` is rescheduled: same jid, in `schedule` at about the hint, not in
   progress, `limited.n` = 1, `retry_count` absent, `stat:failed` unchanged, error handlers not called,
   metrics `o` +1, `over_limit` log line (Sidekiq "reschedule keeps the job", L1).
2. The rescheduled payload is the job as fetched (server middleware that mutates `args` or adds keys does
   not leak into it; L2); client middleware runs when the scheduler moves it (counted once per move).
3. After `reschedules` (set to 2 in the test), the third refusal goes to the retry set with error class
   `hench::OverLimit` and `retry_count` 0; `retry_in` sees `is_over_limit()`; `reschedules(0)` fails at
   once (Sidekiq `reschedule: 0`).
4. `over_limit_when` turns a custom error (a fake HTTP 429 with `Retry-After`) into a reschedule at that
   delay (Sidekiq custom errors); a panicking classifier is reported and the error handled as a failure.
5. Unique job (with the unique design): rescheduled while holding its lock; the lock's TTL covers the
   delay; no duplicate can be pushed meanwhile.
6. Iterable job: a limiter refusing at item 50 saves the cursor, runs `on_stop`, is rescheduled; the next
   execution runs `on_resume` and continues at item 51; executions = 2.
7. Throttle gate: 30 jobs of a class with `concurrency(3)` and 10 processors: at most 3 run at any time
   (execution log), all 30 complete, none fails, every refused job is parked in `schedule` (none spins:
   fetch count ≤ 30 + parked returns), other classes in the same queue are not delayed by more than a few
   fetches (fairness; L9).
8. Per-key throttle `window_per_key(2, 1 s)`: 20 jobs for key A and 20 for key B interleaved: each key
   ≤ 2 per second (execution log); B is not slowed by A.
9. Throttle permits: released right after the outcome (success, failure → retry, interruption, over-limit
   reschedule); renewed by the heartbeat for a job running 3 leases (no overage); `kill -9`
   (`Launcher::abandon`) holding 3 permits frees them within one lease and the recovered jobs run.
10. Throttle `fail_open(false)` with Redis stopped during the gate: jobs requeued to the front, outage
    notice once, no job runs unthrottled; `fail_open(true)`: runs with a warning.
11. Shutdown: a job waiting in a limiter returns at the stop; hard shutdown releases permits of
    interrupted jobs.
12. Unknown throttle name in a raw push: runs unthrottled with one warning per minute; `build` refuses a
    registered job naming an unknown throttle.
13. Phase 3 queue limits: `concurrency=2` on a queue with 20 jobs: at most 2 in progress, jobs stay in the
    queue in FIFO order (BullMQ "should not put a job into the delayed queue when limit is hit"), another
    queue in the same capsule keeps flowing; a `window=5/1s` queue processes 20 jobs in ≥ 3 s (BullMQ
    "should obey the rate limit"); a limit set through the API takes effect within one heartbeat; a job
    taken by `BLMOVE` from an over-limit queue goes back to the front; paused + limited queue stays
    paused.
14. Scheduler independence: scheduled jobs of other queues are promoted while everything throttled is
    parked (BullMQ #2697).

### 3.5 Concurrency and chaos

1. Multi-process (4 worker processes, soak harness): a `concurrency(5)` throttle and a `window(50/1s)`
   limiter under load for minutes: from the execution log, overlapping executions ≤ 5 except where an
   overage was counted; grants per sliding second ≤ 50.
2. Redis restart without persistence mid-run: limits restart; running holders restored by their next
   renewal; no job lost; counts of overages match the log.
3. Failover (`READONLY` from a demoted primary): acquires fail then succeed after reconnection; no permit
   leaks beyond one lease.
4. Proxy delaying replies past the response timeout: no double reschedule (`PUSH_BACK` guard), no
   over-admission.
5. Renewals suspended for longer than the engine lease (a `test-util` hook pausing the heartbeat's renewal
   step, standing in for a stalled runtime or a partition) while another process waits for the slot: the
   lease ends, the waiter is granted, an overage is counted, the first job keeps running, and its next
   renewal reports the permit lost.
6. Soak scenario `throttled` added to `ci/soak.sh`'s long runs: the checks above plus "no job ended in the
   morgue because of a throttle" and clean Redis (no `limit:` key without TTL).

### 3.6 Testing modes and macro

1. Default testing clients ignore limits and throttles; `perform_inline` too.
2. `TestLimits::InMemory` with `tokio::time::pause`: a `window(2, 1 s)` throttle and 5 captured jobs:
   `drain` runs 2, keeps 3 captured with `limited`; `advance(1 s)` and `drain` runs 2 more; no infinite
   loop when time never advances.
3. Inline client: an over-limit refusal returns `Error::Job` with class `hench::OverLimit`.
4. Parity: the same sequences against Redis (fixed `now`) and in memory give the same decisions and hints.
5. Macro (trybuild): `throttle = "x"`, `throttle_by = [a, b]`, inline `throttle(concurrency = 5,
   window_per_key = "5/1s")`; errors for unknown limit names, malformed specs, unknown or repeated
   parameters, the context parameter in `throttle_by`, `throttle_by` without a throttle, `throttle` on an
   `impl Job` block.

### 3.7 API, dashboard, CLI

1. `Limits::page` lists live identities with usage, skips and trims stale index entries; `reset` frees a
   full concurrency limiter; `clear_line`; `waiting` finds scheduled jobs limited by an identity.
2. Dashboard: Limits tab rows (escaped names and keys with control characters), detail page holders and
   waiting jobs, Reset/Clear line actions with CSRF and read-only refusals, scheduled-job chips, metrics
   columns, phase 3 queue limit form (validation errors shown, read-only hides it).
3. `hench limits` text and `--json`, `show`, `reset --yes` (refused without), JSON API routes (DELETE
   refused when read-only).

### 3.8 Ported scenarios

| Source scenario | hench test |
|---|---|
| sidekiq-throttled concurrency spec: limit exceeded / not; dynamic limit nil → unlimited; "invalidates expired locks avoiding strategy starvation"; finalize reduces the level and allows exactly one more; reset; dynamic key same / different input; limit 0 or negative → throttled | §3.2.6, §3.4.7–8 (`tests/limit_scripts.rs`, `tests/throttle.rs`). Deviation: a limit of 0 (or below) is refused by validation (`Error::Config`) instead of throttling everything, which with unlimited reschedules would park jobs in a loop; stopping a class is done by pausing its queue. |
| sidekiq-throttled threshold spec: exceeded; chill period over; retry_in exact (8 s ago → 2 s) | §3.2.1 |
| sidekiq-throttled strategy spec: both concurrency and threshold "avoids concurrency limit starvation"; multiple rules with the same suffix; observer called | §3.2.7; duplicate same-scope limits get `~n`; observer → logs and counters |
| sidekiq-throttled basic fetch spec: throttled job pushed back, other queue not starved; cooldown excludes the queue | §3.4.7 (class throttles park, never exclude), §3.4.13 (queue cooldown) |
| sidekiq-throttled server middleware spec: finalize with jid and args after the job | §3.4.9 |
| sidekiq-throttled concurrency retry_in: proportional to backlog, never above max_delay, lost jobs do not delay forever | §3.2.8 (wait line), §3.1.3 (clamp), line resets once in the past |
| BullMQ rate_limiter: no delayed move when the limit is hit; obey the rate limit; respect processing time without adding delay; quickly close a worker; a never-completing job does not block the limit; paused between limits; dynamic limit (`RateLimitError`) moves back without consuming attempts; priority/FIFO kept; delayed jobs promoted while limited; processes jobs as max from the beginning | §3.4.13, §3.4.1, §3.3.8, §3.4.9 (lease), §3.4.14, §3.2.4 (burst first) |
| Sidekiq Ent docs/changes: reschedule keeps the job (#2619); job as fetched (#5859, #3368); 20 reschedules then retry; `reschedule: 0`; custom errors; unlimited limiter; window counter never without TTL (#2704); no leak (#3323); no reclaim of live holders (#4105); `used: n` (#6146); leaky semantics | §3.4.1–4, §3.3.6, §3.2.9, §3.3.4, §3.2.2, §3.2.4 |
| Oban: snooze does not consume an attempt; snooze counter; meta changes during execution not lost | §3.4.1–2 (`limited.n`, job as fetched by design) |

---

## 4. Decisions for the owner (with recommendations)

1. **D1 Scope and phasing.** Recommended: limiters + over-limit reschedules (phase 1), throttles
   (phase 2), queue limits in the fetch script (phase 3), all designed here. Alternative: stop after phase
   2 (queue limits only matter for very large backlogs of one class, where parking costs scheduler moves).
2. **D2 Where throttles are enforced.** Recommended: in the processor right after the fetch, parking
   refused jobs in `schedule` at a wait-line slot. Alternatives: in the fetch script (impossible for
   per-key limits without decoding every job and touching undeclared keys), or virtual per-key
   sub-queues with round-robin fetch (BullMQ Pro groups: the fairest and churn-free, but a new queue model
   for the dashboard, API, fetch and recovery; future work).
3. **D3 Reschedule budget.** Limiters: 20 then the retry system (Sidekiq parity), the counter never
   reset, so the retry backoff governs a job once its budget is spent. Throttles: unlimited (a throttle
   should delay, never fail). Recommended as stated. Alternative: a fresh budget of reschedules after each
   retry (more patient, but a job can then wait up to 20 x 25 rounds).
4. **D4 Wait line.** On by default (recommended): spreads parked jobs at the limit's rate, so most are
   granted on their first return. Off: plain hints + jitter (simpler; returning jobs clump and churn).
5. **D5 Per-call bookkeeping.** On by default (recommended; it feeds the dashboard and the concurrency
   line, costs about 40 % of the script) with `stats(false)` per limiter. Alternative: counters
   aggregated in process and flushed by the heartbeat (cheaper, but invisible from non-worker processes).
6. **D6 Redis errors.** Fail-closed by default (recommended); `fail_open` for rate kinds and throttles.
7. **D7 Testing clients.** Limits off by default (recommended: limits depend on time, which would make
   drains nondeterministic), `TestLimits::InMemory` to test throttling. The unique design enforces locks
   by default; the difference is deliberate.
8. **D8 Queue limits storage.** In Redis (`queue-limits`), editable at runtime from the API, dashboard and
   CLI (recommended, like `paused`); the builder and TOML write theirs at every start (code wins over
   runtime edits at the next deploy). Alternative: code only (no runtime tuning) or Redis only (no code).
9. **D9 Releasing engine-held permits.** After the outcome write, best effort (recommended: no new
   variants of the hot outcome scripts; a crash costs one lease of under-use). Alternative: inside the
   outcome scripts (exact, but more variants beside the unique design's).
10. **D10 Accounting.** An over-limit reschedule of a job that ran counts as processed, not failed
    (Sidekiq's middleware swallows the error the same way); a throttled job that did not start counts
    nowhere but `t`. Recommended as stated.
11. **D11 Reschedule target.** `schedule` with the job as fetched, written by the existing push-back path
    (`PUSH_BACK` in its `schedule` mode, atomic with the acknowledgement), client middleware not re-run
    until the scheduler moves it (recommended; like retries; keeps unique locks and is not a batch
    outcome, as both sibling designs require). Alternative: push back to the end of the queue (churns
    under sustained limits, L9).
12. **D12 Clock.** Redis `TIME` (recommended); hosts' NTP then only matters for Redis hosts.
13. **D13 Composite limits** atomic in one call (recommended; an improvement over Sidekiq Enterprise,
    whose limiters do not compose).
14. **D14 Defaults.** Lease 30 s (user permits, renewed), 60 s (engine-held, heartbeat-renewed); `max_wait`
    5 s for limiters (Sidekiq), 0 for throttles; `max_delay` 24 h; stats TTL 24 h (Sidekiq: 90 days);
    reschedules 20. Recommended as stated.
15. **D15 Bucket alignment** to UTC epoch multiples (recommended; day buckets reset at 00:00 UTC). A
    time-zone option can come later.
16. **D16 Per-key tiers** (VIP vs standard): separate throttle names chosen at push (recommended), rather
    than closures computing limits per key (sidekiq-throttled's dynamic `limit` procs) or limits stored per
    key in Redis (BullMQ Pro's `setGroupConcurrency`).
17. **D17 Names** limited to `[A-Za-z0-9_.-]`, keys free text hashed beyond 128 bytes (recommended).
18. **D18 Concurrency waits** poll with jittered backoff plus local wake-ups (recommended; no blocking
    Redis commands, no extra connections). Alternative: a pub/sub channel per limiter for cross-process
    wake-ups (one subscriber connection per process; lower latency, more moving parts).
19. **D19 Payload shape**: `throttle` and `limited` as two nested objects (recommended), not flat keys or
    Sidekiq's `overrated`.
20. **D20 Unknown throttle names** at execution time: run unthrottled with a warning (recommended) rather
    than park forever (`build` already refuses registered jobs naming unknown throttles).
21. **D21 Throttle gate placement.** In the processor, before the server middleware chain (recommended:
    refused jobs run no code, and the reschedule is written atomically with the acknowledgement).
    Alternative: a `ServerMiddleware` users position themselves (sidekiq-throttled style): it cannot write
    the reschedule together with the acknowledgement, and a middleware that halts would count as a
    success for batches.

## 5. Risks

1. **R1 Scheduler throughput.** Parked jobs return through the scheduler, which moves one job per two
   round trips per process. Throttles granting thousands of jobs per second would make the scheduler the
   bottleneck. Mitigation: the wait line returns jobs at the throttle's rate only; queue limits (phase 3)
   for very high rates; document the numbers.
2. **R2 Redis CPU.** An acquire costs about 3.5 plain commands (2.19); a throttled job about 3 extra round
   trips per attempt. Mitigation: `stats(false)`, a dedicated Redis for limiters (any `RedisPool`),
   throttles only where needed.
3. **R3 Inflated wait lines.** Parked jobs deleted by an operator still occupy their slots in the line;
   new jobs may wait longer than needed until the line's end passes (bounded by `max_delay`). Mitigation:
   dashboard/CLI "clear line", the line resets itself once in the past, metrics show refusals vs grants.
4. **R4 Memory of dynamic keys.** Window state is O(limit) per key; stats 120 B + index 60 B per key for
   24 h. Mitigation: leaky/bucket for per-key limits (88 B), `stats(false)`, documented arithmetic.
5. **R5 Leases and stalls.** A runtime stalled longer than a lease (or a partition longer than a lease)
   lets other processes take the slot while the stalled holder still runs (counted as an overage). Same
   class of risk as hench's liveness model; mitigation: lease ≥ 3 renewal intervals, overage counters, a
   `lost()` signal for long holders.
6. **R6 Data loss.** Restart without persistence, eviction or failover resets limits (over-admission for up
   to one period). Documented; `noeviction` warning exists.
7. **R7 Semantics confusion.** Limiting vs throttling vs queue limits; per execution vs per item;
   reschedules counting as processed; tests unlimited by default. Mitigation: README section with a
   decision table, dashboard labels, examples.
8. **R8 Cross-feature contracts.** Unique (lock held on reschedule through `PUSH_BACK_UNIQUE`), batches
   (reschedules through the push-back path are not outcomes; members stay pending), periodic
   (`rescheduled` run status) must be honoured by those implementations; mixed versions treat
   `OverLimit` as a failure.
9. **R9 Fetch path (phase 3).** `FETCH_GATE` costs about 19 % per fetch when used; only capsules with
   limited queues use it, and limits set at runtime take up to one heartbeat to apply everywhere.
10. **R10 Hot paths changed.** Processor branches, heartbeat renewal, the gate before execution. Mitigation: jobs
    without throttles take today's path (one `Option` check), new code isolated in `limit::`, engine lane
    review, soak scenario.
11. **R11 Redis 6.2 with `lua-replicate-commands no`** breaks `TIME` + writes; detected and warned at
    start when `CONFIG` is allowed. Dragonfly/Valkey untested (scripts declare keys and use `TIME`).
12. **R12 Clock jumps on Redis failover** shift windows and leases by the jump (stricter backward, looser
    forward); keep NTP on Redis hosts.

---

## Appendix A. Lab verification (redis-server 7.0.15, unix socket, scratch)

Prototypes and the suite: `/home/user/scratch/design-lab/ratelimit/` (`./start.sh`, then
`python3 -I scenarios.py`, then `./bench.sh`; the server was stopped and its files removed afterwards).

- `scenarios.py`: 52 checks, all passing: window (exact sliding, costs, impossible cost, cap, TTL), bucket
  (boundaries, TTL), leaky (burst, drip, refill), points (estimates, refund, debt), concurrency (grants,
  refusal, idempotent token, reclaim of ended leases, release of a reclaimed lease, renew, restore, lost,
  stats), composite all-or-nothing, wait line (spread 6 s apart for 10/min, tickets, reset), concurrency
  spacing from measured hold times, Redis `TIME` followed by writes, `OOM` (grant refused whole; refusal
  still answers), dashboard usage read.
- Sidekiq's documented leaky behaviour checked separately: `leaky(60, 60 s)`: 60 granted at once, the 61st
  waits 1000 ms, 5 granted 5 s later, 60 again after a full drain.
- `redis.replicate_commands()` exists as a no-op on 7.0.15 and `lua-replicate-commands` is not a config
  there; scripts do not call it.
- One issue found and fixed during the lab: a refused concurrency acquire first returned a 0 ms hint; it
  now returns one spacing (average hold / limit), as sidekiq-throttled's backlog formula implies.
- Throughput and memory: the table of §2.19 and the figures of §2.8 (redis-benchmark, 200 000 requests,
  20 clients; numbers are indicative, measured on a shared VM).
- `fetch_gate.lua` was checked by hand against three queues (a limited queue granted with its token, the
  same queue skipped once full with the next queue served) and benchmarked against a copy of hench's
  current `RELIABLE_FETCH` (73 000 vs 59 000 fetches/s with no limited queue).
- Specified but not prototyped: the signature-mismatch return value of `ACQUIRE`, `RESET` and
  `CLEAR_LINE` (single commands), the in-memory backend. The `N` (no bookkeeping) flag was prototyped as a
  measurement variant only.
