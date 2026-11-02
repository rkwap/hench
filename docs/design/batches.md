# Batches (job groups with callbacks) and simple workflows for hench: design

Status: proposal, 2026-11-01. Scope: `crates/hench` (spine: client, payload, keys, config, error,
job context; engine: processor, endpoint, launcher; api; testing), `crates/hench-web`,
`crates/hench-server`. Binding context: `docs/DESIGN.md` §2 to §9. Sibling designs read for
consistency: `unique.md` (its §2.18 contract for batches is honoured in §2.18 below),
`periodic.md`, and `ratelimit.md`, written in parallel. Its §2.12 meets the contract stated in
§2.18 here: over-limit jobs are rescheduled through the push-back path, which is not a batch
outcome.

Prototypes. The Lua scripts of §2.5 exist as original prototypes in
`/home/user/scratch/design-lab/batches/` (`lib.lua`, `commit.lua`, `succeeded.lua`,
`failed.lua`, `skipped.lua`, `dispatch.lua`, `touch.lua`, `cancel.lua`), with a stdlib-only Python harness
(`bt.py`, which builds KEYS and ARGV exactly as the Rust code should), 100 scenario checks
(`scenarios.py`), a concurrency check (`concurrency.py`) and measurements (`bench.py`), all run
against redis-server 7.0.15 on a unix socket (Appendix A). They are original code written for this
design and are meant to become `crates/hench/src/batch/lua/*.lua`.

Cross-feature review (2026-11-01, `/home/user/scratch/design/pro-features.md`, which also holds the
implementation lanes): aligned with the unique, periodic and rate-limiting designs. Changes: the unique
lock step of `BATCH_COMMIT` specified against the unique design's frozen Lua interface (`hu_check`,
`hu_take`), with intra-commit duplicates and already-held jids (§2.5.3, §2.9); callback templates
normalized without uniqueness (§2.3.1, §2.9); `Batch::with_bid` for idempotent creation from a job and
`BatchError::Exists` (§2.3.1); skipped members tracked as `Outcome::Skipped` in metrics (§2.6.1); the
over-limit and throttled branches in the processor pseudo-code and the cancelled-member check on a
throttle refusal (§2.6.1, §2.9); endpoint hook order (§2.6.2); `keys::batch_minute` (§2.4);
feature-prefixed scripts and the shared Lua prelude (§2.5.1); testing-mode interplay (§2.11); dashboard
navigation order, `HenchWeb::batches` switch, heartbeat `"features"` map, flash kinds and job-row order
(§2.12, §2.15); `hench batches <subcommand>` as one command and `hench stats batches` (§2.13); DESIGN
section §9i (§2.19).

Licensing. Sidekiq Pro (closed; behaviour from `sidekiq.wiki` and `sidekiq/Pro-Changes.md`,
`sidekiq/docs/Pro-*-Upgrade.md`), Sidekiq OSS (LGPL) and Faktory (AGPL) were used for behaviour
only: no text, code, Lua or templates of theirs is reproduced. sidekiq-batch and BullMQ (MIT) were
read for ideas; nothing is adapted from them, so no credit comment is needed. An implementer who
later adapts code from them must add one.

---

## 0. Summary

A **batch** is a group of jobs that Redis tracks together. Its member jobs carry the batch id in
the payload's reserved `bid` field. Hench counts each member's outcome once, whatever the number of
executions. It fires three **events**, each at most once: `complete` (every member has run at
least once, though some may be retrying), `success` (every member succeeded) and `death` (the
first member that will not run again). Each event pushes the **callback jobs** registered for it.
Batches nest: a child batch counts as part of its parent, and the child's callbacks run before
the parent's. A small **workflow** layer chains stages of parallel jobs on top of nested batches.

The central choices, and where they depart from the references:

| Topic | Decision |
|---|---|
| Model | `bid` = 24 lowercase hex characters (the jid format). Per batch: hash `batch:<bid>` and three collections of jids: `pending` (SET: not yet succeeded), `failed` (HASH jid → summary: has failed and not succeeded since), `dead` (SET: will not run again on its own). Counts are set sizes, never counters that are incremented and decremented, so a repeated report cannot skew them (the cause of Sidekiq Pro's "pending == -1", #5524). |
| Defining | Pushes made through a **collecting client** (`Batch::client()`, or the client handed to the `Batch::jobs` closure) are buffered. One Lua script, `BATCH_COMMIT`, then writes everything: the batch rows, the registration of every jid and the pushes. It is all or nothing, and checks run before the first write. Child batches defined inside go into the same script. |
| Idempotent commits | A commit carries a **commit id**, and the script applies it at most once. A commit whose reply is lost is therefore resent until a definite answer arrives, and is never pushed twice; a plain `Client::push` cannot promise this. An explicit `commit_id` makes a loader job's reopen idempotent across its own retries. |
| Reopening | `ctx.batch()` in a member job, or in a callback of a child batch, returns a handle on that batch; `Batch::open(client, bid)` does the same from anywhere. The commit refuses a batch that has succeeded or been cancelled, in the same atomic step, so reopening can never race with success. |
| Accounting | Each outcome runs one small idempotent script keyed by jid: `BATCH_SUCCEEDED`, `BATCH_FAILED` (`retry` or `dead`) or `BATCH_SKIPPED`. The processor runs it **before** writing the outcome (acknowledgement, retry set, morgue), so the reliable-fetch scripts, and the unique design's variants of them, stay unchanged. Crash re-runs, orphan-recovery duplicates, lost replies and basic fetch are all counted once. An interrupted iterable job, a shutdown requeue or a rate-limit reschedule is not an outcome. |
| Events and dispatch | The script that changes the counts also fires the event: it records the time in the hash and adds the batch to an **outbox** (`batch-events`). `BATCH_DISPATCH` pushes the callback jobs and updates the parent, once per event. The process that fired the event dispatches it at once, and a maintenance loop in every worker dispatches anything left behind. |
| Callbacks | Ordinary typed jobs (`on_success::<J>(args)`), normalized and passed through client middleware when the batch is defined, and stored as templates that the dispatch script pushes atomically. The callbacks of a child batch are **members of its parent**. This gives Sidekiq Pro's ordering (a child's callbacks have run before its parent's events fire) and lets a callback reopen the parent for the next step. |
| Failures | Retries, the morgue, revival from the morgue (a dead member stays pending, so its later success counts), discards and `retry: false` (counted as deaths, as in Sidekiq Pro). Deleting or killing a member through the API also counts as a death, unlike Sidekiq Pro, where the batch then waits forever. An operator can `skip` a dead member so that the batch can succeed. |
| Cancellation | `cancel(bid)` marks the batch and its descendants. Members are skipped right before `perform` (one `HGET` per member execution); long jobs can call `ctx.check_batch()`. `success` never fires after a cancel; `complete` fires under its usual rule (every remaining member has run at least once). Jobs are not removed from queues and sets. |
| Expiry | Every key of an unfinished batch expires 30 days after its last activity (refreshed once a quarter of the expiry, an hour at most, went by; round 3), and extended to cover scheduled members. After success, and after the `complete` of a cancelled batch once nothing is pending (round 3), the keys linger 24 hours, so status reads and late callbacks still work. |
| Operations | `api::Batches` (list, status, failures, children, pending jids, cancel, delete, skip, wait); a dashboard Batches tab (list with state filters, detail page with progress, failures, children and callbacks, actions); JSON endpoints; `hench batches` CLI. |
| Workflows | `Workflow::new(client).stage(..).stage(..).on_success::<J>(..)`: stages of parallel jobs, each starting when the previous one succeeds, built on one overall batch with one child batch per stage. |
| Cost (measured) | Commit: 3.3 µs of Redis time per job (1 000 jobs 3.4 ms, 10 000 jobs 34 ms). 72 bytes per pending member. Accounting: 24 µs per outcome. Jobs outside batches: unchanged. |

---

## 1. Survey

### 1.1 Sidekiq Pro batches (closed source; behaviour from the wiki, `Pro-Changes.md`, upgrade notes)

- **Defining.** A batch gets its id, description, callbacks (`on(event, target, options)`) and
  callback queue, and is then filled inside a `jobs` block. Every push made inside the block, by
  any code on that thread, is collected. The pushes are written at the end of the block together
  with the batch's registration; Pro 2.x made the registration part of the same atomic push
  (#2714). An exception in the block writes nothing. The block must run only once per definition,
  because the jobs of a first call may finish before a second one. A recent `autoflush` option
  (#6967) writes every N jobs to bound memory with millions of jobs, and gives up atomicity.
- **Reopening.** A running member may add jobs to its own batch: it is itself still pending, so
  the batch cannot finish meanwhile. The documented rule is that jobs open their own batch and
  callbacks open their parent. Adding to a finished batch raises `Batch::Immutable` (#4845).
- **Nesting.** A batch defined inside another's `jobs` block is its child. The parent's callbacks
  wait until the child's callbacks have run successfully (Pro 2.0 upgrade notes). A child's
  success precedes its parent's success, and a child's complete precedes its parent's complete,
  but nothing orders a child's success against its parent's complete. Workflows chain steps by
  reopening the parent from a child's success callback and defining the next child there.
- **Events.** `complete` fires when every member has run at least once, `success` when all have
  succeeded, and `death` the first time a member dies (Pro 4.0, #3841). `death` also fires when
  retries are disabled or `dead: false` is set. Death and success are not exclusive: once the
  dead job is fixed and re-enqueued, the batch can still succeed. A death in a child batch runs
  the parent's death callbacks (5.2.1, #4774). Callbacks are jobs (#1744) and are retried like
  jobs. They go to the queue of the batch's last job unless a `callback_queue` is set (#2200), and
  they receive the batch's status and the callback's options (JSON).
- **Status.** `total`, `pending`, `failures`, `created_at`, `complete?`, `join` (blocks until
  complete), `failed_jids` (Pro 8 replaced the stored failure info, #6580), `dead_jids`,
  `complete_at`/`success_at`/`death_at` (#5818), `invalidated?` (#3326), and `data` (JSON), which
  a Rack endpoint serves for polling. `BatchSet` lists the batches with outstanding jobs, and
  `Batch::DeadSet` the dead batches.
- **Empty batches** were undefined until 7.1. Since then an internal job fires the callbacks
  (#5903, #5914), but reopening a batch without adding a job must not (#6489).
- **Expiry.** After success the data stays for 24 hours so that polling still works (#3011). The
  linger is configurable (#4772), with a 30-second floor against nested-callback races. An
  unfinished batch expires after 30 days, a fixed value since 2.0; earlier versions had
  `expires_in`, defaulting to 3 days.
- **Cancellation** is cooperative: `invalidate_all` marks the batch, and each job checks
  `valid_within_batch?`. Jobs are not removed, because removing an element from a Redis list
  costs O(n).
- **Deleting a member by hand** leaves the batch waiting until it expires.
- **Web UI.** It lists in-progress batches (successful ones disappear) and shows a detail page with
  a progress bar (made colour-blind friendly in #3387). Failures link to the retries (#4209),
  dead jids are listed (#4926), pending jids are shown when there are few (#3130), along with the
  callback data (#2347) and the expiry (#1981); Pro 8.1 adds tags, which need Redis 7.4.
- **Testing.** The batch server middleware must be added to the testing chain. Nested batches run
  inline (#6057).
- **Operational notes.** Do not disable retries in a batch job, or a failure makes it vanish and
  the batch never succeeds. Batches may contain scheduled jobs, which delay the finish. Batches get
  stuck after `kill -9`, or when a process is killed before its shutdown timeout. Negative pending
  counts appear when Redis evicts keys; positive pending counts with no visible job mean the jobs
  were left in private reliable-fetch queues.

### 1.2 sidekiq-batch (MIT, Ruby; `lib/sidekiq/batch*.rb`, specs)

- **Keys.** `BID-<bid>` hash (created_at, parent_bid, children, pending, total, description,
  callback_queue, complete/success flags), plus sets `-jids`, `-failed`, `-success` (child bids),
  `-complete` (child bids) and `-callbacks-<event>` (JSON callback definitions), and a string
  `invalidated-bid-<bid>`. Every key expires after 30 days.
- **Defining.** A client middleware tags each push with the current thread's batch and records
  its jid. Jobs are pushed **immediately**. `pending` and `total` are incremented at the end of the
  block, or periodically with `batch_push_interval`. The README documents the resulting race: if
  child job B finishes before parent job A has incremented the counts, both callbacks fire early.
- **Accounting.** A server middleware does it around `perform`. Success runs `HINCRBY pending -1`
  without checking that the jid was still pending, so a second execution decrements twice. Any
  exception, retried or not, adds the jid to `-failed`. `complete` means pending equals the
  number of failed jids and every child is complete; `success` means pending is zero and every
  child succeeded. The decision is taken in Ruby from values read by a `MULTI`, not atomically
  with the change. Only callback enqueueing is guarded, by a test-and-set (`HGET` + `HSET` in
  one `MULTI`).
- **Callbacks.** A *callback batch* holds the callback jobs, and its own complete callback
  (`Finalize`) propagates to the parent and cleans up. There is no death callback. A failure in a
  child adds 1 to the parent's pending and the jid to the parent's failed set.
- **Specs worth porting** (§3): new and reused bids, persisted description and callback queue,
  `jobs` returning the jids, the empty batch, nested child counts and `parent_bid`, invalidation
  cascading from parent to child, callbacks enqueued once each with their options, a deep-nesting
  integration, a workflow built from steps, and every key cleaned after success.

### 1.3 BullMQ flows (MIT; `src/classes/flow-producer.ts`, `src/commands/*.lua`, `docs/gitbook/guide/flows`)

- **Model.** A *parent job* waits in the `waiting-children` state. Its `:dependencies` set holds
  the keys of its unprocessed children, `:processed` maps each child to its return value,
  `:failed` holds ignored failures and `:unsuccessful` the failures of `failParentOnFailure`
  children. A child stores its parent key and options. `FlowProducer.add`/`addBulk` write a whole
  tree in one `MULTI`.
- **Completion.** `moveToFinished` removes the child from the parent's dependencies with `SREM`,
  and only a removal that returns 1 continues, which makes it idempotent per child. It then
  records the result and, when no dependency is left, moves the parent to wait (possibly in
  another queue, delayed or prioritized). All of this happens inside the same script that
  finishes the child.
- **Terminal failure** of a child (attempts exhausted) depends on the child's options:
  - default: the parent waits forever, until the child is retried;
  - `failParentOnFailure`: the parent fails lazily ("child X failed", #3228), possibly up to the
    grandparents;
  - `continueParentOnFailure`: the parent starts at once;
  - `ignoreDependencyOnFailure`: the dependency is removed, the failure recorded, and the parent
    starts when nothing is pending;
  - `removeDependencyOnFailure`: the dependency is removed silently.

  `removeChildDependency` and `removeUnprocessedChildren` let code prune children; removing a
  child releases the parent if it was the last one.
- **Lessons from the changelogs.**
  - A parent could be released before all its children were added (#4988, fixing #4944).
  - Retrying a failed or completed child must put it back into the parent's dependencies (#4879,
    #4235, #3402).
  - The stalled-job recovery path ignored the dependency options (#2708, #2702, #2540, #2526).
  - Drain, clean and remove had to learn to update parents (#992, #1048, #990).
  - Parents in delayed, prioritized or not-yet-waiting states were mishandled (#3098, #3112,
    #3160, #3163).
  - Adding with a parent that does not exist used to drop the job silently (#3264, #4058).
  - A deduplicated child is not attached to its parent, so the parent becomes eligible early: the
    docs warn against deduplication on flow nodes, and v6 rejects it on parents.
- **Tests worth porting** (§3): children before the parent; an empty child list; chains; child
  and parent delays; a paused queue; each failure option, including grandparents; continually
  adding children; a child added while a sibling runs; re-adding a completed child; removal;
  retry re-attaching the child; a parent that does not exist.

### 1.4 Faktory Enterprise (AGPL client; behaviour only), Oban, rusty-sidekiq

- **Faktory.** `BATCH NEW` creates the batch server-side with its success and complete callback
  jobs. Jobs are pushed with the bid and **start running at once**. `BATCH COMMIT` marks the
  definition finished, and no callback fires before it, which avoids the half-defined race
  without buffering anything. `BATCH OPEN` reopens a committed batch; its test reopens from
  outside the batch, although the docs restrict reopening to member jobs. Nested batches name
  their parent. The status reports each callback as pending, enqueued or finished. The Enterprise
  changes record a crash when callback jobs retried (#408), `transaction failed` errors under load
  from `WATCH`/`MULTI` contention (#305), and a crash pushing batch jobs (#274).
- **Oban** OSS has no batches or workflows; Oban Pro's are closed. **rusty-sidekiq** has none.

### 1.5 Comparison

| | Sidekiq Pro | sidekiq-batch | BullMQ flows | Faktory Ent | hench (this design) |
|---|---|---|---|---|---|
| Unit | batch of jobs | batch of jobs | parent job + children | batch of jobs | batch of jobs |
| Definition atomic | yes (block, until autoflush) | no | yes (`MULTI`) | no, gated by commit | yes (one script); gate for multi-step (phase 2) |
| Idempotent commit | unknown | no | no | no | yes (commit id) |
| Accounting | server middleware | server middleware | in the finish script | server | own script before the outcome write |
| Idempotent per job | unknown (bugs #5524, #6852) | no | yes (`SREM` guard) | unknown | yes (jid sets) |
| Events | complete, success, death | complete, success | parent runs | complete, success | complete, success, death |
| Callback delivery | jobs | jobs in a callback batch | the parent job itself | jobs | jobs pushed by a script, from an outbox |
| Nesting order | child callbacks first | via callback batches | tree | parent bid | child callbacks are parent members |
| Cancel | cooperative flag | cooperative flag | remove children | n/a | flag + automatic skip before `perform` |
| Deleted member | batch stuck | stuck | parent released | n/a | counted as dead; operator skip |
| Expiry | 30 d, 24 h linger | 30 d | job retention | n/a | 30 d after last activity, 24 h linger |

### 1.6 Production lessons and how this design answers them

| # | Lesson | Sources | Answer |
|---|---|---|---|
| L1 | A batch can complete before it is fully defined, when jobs are pushed before the definition ends | Pro 1.x changelog; sidekiq-batch README; BullMQ #4944/#4988; Faktory's commit step | One script writes the whole definition (§2.5.2). Definitions too large for one script are a phase-2 feature with an explicit `open` gate (§2.20). |
| L2 | Registration not atomic with the push: metadata out of sync, or jobs registered but never pushed | Pro #2714; Pro 7.2.2 (transactional push window) | Same script; the collecting client buffers in memory, so a failure before the commit writes nothing. |
| L3 | Double counting, negative pending, clamping | Pro #5524, #5659; sidekiq-batch | Counts are sizes of jid sets; every transition is guarded by jid membership (B3). |
| L4 | Duplicate callbacks, death fired many times | Pro #6852, #5740 | An event fires through a write-if-absent in the same script that changes the counts; its dispatch is guarded per event (B4). |
| L5 | Shutdown or interruption treated as an outcome; an iterable interruption fired `complete` early | Pro (Shutdown refactor), Pro #6889 | Only acknowledged or settled outcomes are counted. Push back (interruption), requeue (shutdown) and hard-shutdown requeues never are (§2.2.3). |
| L6 | A job stopped by client middleware was counted | Pro #2406 | Only jobs that left client middleware and were written by the commit are registered; unique duplicates are excluded (`Committed::duplicates`). |
| L7 | Uniqueness combined with a batch error leaves locks; a deduplicated child releases its parent early | sidekiq#3662; BullMQ flow docs | Locks are taken by the commit script; a duplicate is neither pushed nor registered (§2.18). |
| L8 | Parent expired before its child finished; keys without expiry; died-batch structures growing without bound | Pro #2889, #1057, #7033, #7080 | Every key gets its expiry from the script that may create it. Activity refreshes the expiry; a refreshed batch refreshes its ancestors (`BATCH_TOUCH`), and dispatch refreshes the parent. There is no unbounded global structure: the index is trimmed by score and the outbox empties itself. |
| L9 | Deleting or draining members leaves batches stuck | Pro wiki; BullMQ #990, #992, #1048 | API deletes and kills count the member as dead (death fires, `complete` can fire). An operator `skip` lets the batch succeed. Bulk `clear` is documented as not batch-aware. |
| L10 | Retrying a failed member must re-attach it | BullMQ #4879, #4235, #3402 | A dead member is never detached: it stays pending until it succeeds or is skipped, so revival needs nothing. |
| L11 | Recovery paths ignore batch semantics | BullMQ #2708, #2702, #2540, #2526 | Accounting is keyed by jid at the outcome, so recovered copies, stale copies and duplicates are the same member. |
| L12 | Memory: buffering millions of jobs; backtraces and long messages in batch data | Pro #6967, #3570, Pro 2.0 and 8.0 notes | Commit size is capped (`batch_max_commit_jobs`, 10 000), with loaders and phase-2 staging beyond. Failure summaries keep 200 characters and no backtrace. |
| L13 | XSS through descriptions and error data | Pro security fixes | askama escaping everywhere; description limited to 1 000 characters. |
| L14 | Callback crashes on retry; `WATCH`/`MULTI` contention under load | Faktory Ent #408, #305 | Callbacks are ordinary jobs; only scripts are used, no `WATCH`. |
| L15 | Data-model changes broke upgrades | Pro 2.0, 8.0 (#6594) | Every batch hash carries a format field `v`; readers accept unknown fields and refuse unknown versions explicitly. |
| L16 | Polling a finished or deleted batch | Pro #3011, #3519 | Linger after finish; `NotFound` is distinct from "succeeded". |
| L17 | Eviction corrupts counts | Pro notes | Start-up already warns unless `maxmemory-policy noeviction`; the batch docs repeat it. |
| L18 | Jobs left in private queues keep batches pending | Pro notes | Reliable-fetch recovery and own-list reconciliation already push such jobs back; the dashboard lists pending jids so they can be found. |

---

## 2. Design

### 2.1 Goals, non-goals, guarantees

Goals:

- Group jobs, observe the group, and get callbacks exactly once per event.
- Keep counts correct under hench's at-least-once execution: crashes, lost replies, failovers,
  reliable-fetch recovery and basic fetch.
- Support nesting, additions from running jobs, and cancellation.
- Make batches visible in the API, the dashboard and the CLI, and testable without Redis.
- Cost nothing for jobs that are not in a batch.

Non-goals:

- Passing results between jobs: hench jobs return `()`.
- Arbitrary DAGs: hench offers trees of batches and sequences of stages.
- Batches spanning several Redis servers. A batch lives in the Redis its client pushes to; Sidekiq
  Pro dropped multi-shard batches too (#4642).
- Compatibility with Sidekiq Pro's data format.
- Batch tags and search in v1 (§4, D15).

Guarantees:

- **B1. All or nothing.** A commit writes either its whole definition (new batches, children,
  callback templates, registrations, pushes) or nothing. It is one script, all checks run before
  its first write, and an `OOM` refusal falls on that first write.
- **B2. At most once per commit id.** A commit is applied at most once per commit id, so resending
  it after an unknown outcome is safe.
- **B3. Each outcome counted once.** A member's outcome is counted once per jid, however many
  times the member runs or its outcome is reported.
- **B4. Each event fires once.** Each event fires at most once per batch, and its callback jobs are
  pushed exactly once, as long as Redis keeps its data. Firing is a write-if-absent plus an outbox
  entry in the same script as the count change. Dispatch is a guarded script, and any process
  finishes a dispatch that another one left undone.
- **B5. No early events.** No event fires while the definition is not fully committed. `complete`
  waits until the children's `complete` callbacks have run once; `success` waits until the
  children's callbacks have succeeded.
- **B6. Every key expires.** Every key of a batch expires: 30 days (`batch_ttl`) after the last
  activity of an unfinished batch, and the linger time after it finishes.

Not guaranteed (documented):

- Members run at least once, like every hench job. A member can run again after its batch
  succeeded, if its process crashed between the accounting and the acknowledgement.
- Callbacks run at least once.
- `complete` does not fire again for jobs added after it fired.
- `Queue::clear`, `JobSet::clear`, `FLUSHDB`, eviction and expiry leave batches waiting or wrong.

### 2.2 Semantics

#### 2.2.1 Vocabulary

| Term | Meaning |
|---|---|
| batch, bid | A group of jobs; its id is 24 lowercase hex characters (`batch::new_bid()`, same generator as jids). |
| member | A job registered in a batch by a commit; its payload's `bid` names the batch. Setting `bid` on a payload by hand does not make a job a member (§2.9). |
| definition, commit | The jobs, child batches and settings collected by a `Batch` handle; the script that writes them. |
| reopen | A commit that adds members (and children) to an existing batch. |
| parent, child | A batch defined inside another batch's definition is its child. |
| event, fire, dispatch | `complete`, `success`, `death`; *fire* = the script that changes the counts records the event; *dispatch* = pushing its callbacks and updating the parent. |
| callback | A job pushed when an event is dispatched; its payload's `batch_callback` names the batch and the event. |
| outbox | `batch-events`: batches with fired, undispatched events. |
| linger | How long a finished batch's data stays readable. |

#### 2.2.2 Member states

| State | in `pending` | in `failed` | in `dead` | Reached by |
|---|---|---|---|---|
| waiting or running | yes | | | the commit that registered it |
| failed, will be retried | yes | yes | | `BATCH_FAILED retry` |
| dead (will not run again on its own) | yes | yes | yes | `BATCH_FAILED dead` |
| succeeded | | | | `BATCH_SUCCEEDED` |
| skipped | | | | `BATCH_SKIPPED` (cancelled batch, or operator) |

Invariants: `dead ⊆ failed ⊆ pending`. The counts are `total` (registrations, a hash field),
`pending` (SCARD), `failures` (HLEN), `dead` (SCARD), `skipped` (a hash field) and `succeeded`
(`total − pending − skipped`). A transition applies only when the jid is still pending, so a
stale copy, a duplicate or a repeated report changes nothing.

#### 2.2.3 Outcomes

| What happens to a member | Batch transition |
|---|---|
| `perform` returns `Ok` | succeeded |
| server middleware does not call `next` (the job is acknowledged) | succeeded |
| fails; retry scheduled (`Verdict::Retry`) | failed (`retry`) |
| fails; retries exhausted or `RetryIn::Kill`, to the morgue (`Verdict::Kill`) | dead (`morgue`) |
| fails; `retry: false` (`Verdict::NoRetry`) | dead (`no_retry`) |
| fails; `dead: false`, `RetryIn::Discard`, `Exhausted::Discard` (`Verdict::Discard`) | dead (`discarded`) |
| fails with `JobError::batch_cancelled()` and its batch is cancelled | skipped |
| fails with `JobError::batch_cancelled()` but its batch is not cancelled | treated as an ordinary failure |
| interrupted (iterable job pushed back to its queue) | none |
| `hench::Shutdown` while stopping, requeued; hard-shutdown requeue | none |
| rate limiter reschedules it (contract, §2.18) | none |
| unparseable JSON (to the morgue by the processor) | none: no bid can be read |
| deleted through the API (queue entry, retry/scheduled/morgue entry) | dead (`deleted`) |
| killed through the API (moved to the morgue) | dead (`killed`) |
| retried or added to a queue through the API (revival) | none: still pending; its later success counts |
| `Queue::clear`, `JobSet::clear` | none (documented) |
| operator skip (`Batches::skip`, dashboard) | skipped |

Counting a halted job as a success is consistent with hench treating it as done and
acknowledging it. Counting discards and `retry: false` as deaths follows Sidekiq Pro, which fires
`death` for them: the batch never claims success for work that did not succeed. Counting API
deletes as deaths differs from Sidekiq Pro, where the batch then waits until it expires (L9).

#### 2.2.4 Events

Each event fires at most once. Times come from Redis `TIME`. When `complete` and `success` fire in
the same step, `complete` comes first.

| Event | Fires when |
|---|---|
| `complete` | `SCARD pending == HLEN failed` (every member left has run and failed at least once), every child's `complete` has been dispatched (`kc ≥ kids`), and no multi-step definition is open (`open == 0`, phase 2). |
| `success` | `SCARD pending == 0`, every child's `success` has been dispatched (`ks ≥ kids`), the batch is not cancelled, and `open == 0`. |
| `death` | the first member reaches dead; or the dispatch of a child's `death`. |

- Adding members after `complete` fired is allowed, as in the step pattern below; `complete` does
  not fire again. Adding after `success` is refused (`BatchError::Succeeded`): the check and the
  write happen in the same script, so the race that makes Sidekiq Pro document reopening from
  outside as unsafe cannot happen.
- A batch that has died can still succeed: when every dead member is revived and succeeds, or is
  skipped by an operator. `death` is not fired again.
- An empty new batch (no member, no child) fires `complete` and `success` at its commit. Hench
  needs no internal empty job, because the outbox dispatches directly. A reopen that adds nothing
  changes nothing (Pro #6489).
- No ordering is guaranteed between a batch's own `complete` and `success` callbacks: they are
  pushed together and may run concurrently.

#### 2.2.5 Nesting

- A batch whose handle was created from a collecting client (`Batch::new(&jobs)` inside another
  definition) is a **child** of the batch being defined. Its definition is staged into the parent's
  commit and written by the same script.
- A batch created inside a job with `Batch::new(ctx.client())` is a new root: hench does not
  guess a parent from the running job, unlike sidekiq-batch. To create a child of the job's batch,
  reopen it (`ctx.batch()`) and define the child inside, as Sidekiq Pro requires.
- The parent counts its children (`kids`), the children whose `complete` was dispatched (`kc`),
  and those whose `success` was dispatched (`ks`).
- Dispatching a child's event registers the child's callback jobs as **members of the parent**,
  in the same script. The templates already carry `"bid": <parent>`. Hence:
  - The parent's `complete` waits until its own members and the children's callbacks have run once,
    and every child's `complete` is dispatched. A child's `complete` callback therefore runs
    before its parent's `complete` fires.
  - The parent's `success` waits until its own members and every callback of its children have
    succeeded, and every child's `success` is dispatched.
  - A callback of a child is a job of the parent, so `ctx.batch()` in it reopens the parent: this
    is how a workflow adds its next step. Sidekiq Pro's rule "callbacks open their parent" falls
    out of the model instead of being a convention.
  - Cancelling the parent also skips the children's callbacks that have not run yet. That is the
    wanted behaviour for workflows, since those callbacks would start the next steps.
- A child's `death` sets the parent's `death_at` and fires its `death`, if the parent had not
  died yet; this repeats up the tree.
- Depth is unlimited: dispatch walks up one level per script. A child's data cannot be deleted
  while its parent waits for it (§2.10).

#### 2.2.6 Cancellation

`Batches::cancel(bid)` (also `Batch` handles, the dashboard, the CLI, the JSON API):

- `cancelled_at` is set once, the first time.
- The same is done for every descendant, read from the `kids` sets, breadth first with a bounded
  number of round trips. A child defined later under a cancelled batch cannot exist: reopening a
  cancelled batch is refused.
- Members are not removed. Removing jobs from lists costs O(n), and Sidekiq Pro does not remove
  them either. Instead, right before `perform`, after server middleware, the endpoint reads
  `cancelled_at` of the member's batch (one `HGET`). If it is set, the endpoint returns
  `JobError::batch_cancelled()`, and the processor counts the member as skipped and acknowledges
  it. Long jobs may call `ctx.check_batch().await?` between steps.
- Members in the retry or scheduled sets are skipped when they come due. Until then the batch shows
  them as pending.
- After a cancel, `success` never fires. `complete` fires under its usual rule, as soon as every
  remaining member has run at least once (members waiting in the retry set have), or at once if
  that already holds. Its callbacks see `cancelled_at` in the status, so cleanup code can
  branch. A cancelled batch is
  finished once its `complete` is dispatched, and its data then lingers.
- Cancelling a succeeded batch is refused (`BatchError::Succeeded`); cancelling twice keeps the
  first time.

#### 2.2.7 Expiry and linger

- **Unfinished batches.** Each key gets the expiry of the batch hash. It is `batch_ttl` (default
  30 days, per batch with `Batch::expires_in`) at the commit, extended for scheduled members to
  `latest run-at + batch_ttl`. Activity refreshes it at most once an hour and never shortens it:
  the accounting and dispatch scripts call `hb_touch`. (Round 3: once a quarter of `batch_ttl`, an
  hour at most, went by, as the hash's remaining lifetime shows: with the shortest `ttl`, 1 hour,
  the hourly rule never refreshed anything.) When a batch is refreshed, its ancestors
  are refreshed too (`BATCH_TOUCH`), so a parent does not expire while a child is still active
  (Pro #2889; round 3: each ancestor gets at least the lifetime of the level below, also after a
  reopen that added a member scheduled beyond the parent's expiry). A script that may create a
  key gives it the hash's remaining lifetime right away (`hb_inherit_ttl`), which avoids the leak
  of Pro #1057.
- **Finished batches** (dispatched `success`, or dispatched `complete` of a cancelled batch): every
  key's expiry is set to the batch's linger (default 24 hours, at least 30 s as in Sidekiq Pro, per
  batch with `Batch::linger`), and `finished_at` stops any later refresh. A succeeded batch's sets
  are empty by then. A finished cancelled batch may still list members waiting in the retry set,
  which are skipped when they come due. (Round 3: a cancelled batch finishes only once nothing is
  pending, since a member skipped after the linger found no batch and ran; the commit ids of a
  root created by `with_bid` stay `batch_ttl` after the finish, so that a late re-run of the job
  that created it writes nothing.)
- **The index** `batches` is scored by expiry. Entries of expired batches are trimmed by score by
  the maintenance loop (§2.6.4) and by readers.

### 2.3 Rust API

New public module `hench::batch` (`crates/hench/src/batch/`), plus additions to `JobContext`,
`JobError`, `Error`, `Client`, `HenchBuilder` and `hench::api`.

#### 2.3.1 Defining a batch

```rust
use hench::batch::Batch;
use hench::prelude::*;

#[hench::job(queue = "imports")]
async fn import_row(import_id: i64, row: u32) -> Result<(), JobError> { /* ... */ Ok(()) }

#[hench::job(queue = "critical")]
async fn import_finished(ctx: &JobContext, import_id: i64) -> Result<(), JobError> {
    let callback = ctx.batch_callback().ok_or_else(|| JobError::msg("not a batch callback"))?;
    if let Some(status) = callback.status(ctx).await? {
        tracing::info!(bid = callback.bid(), failures = status.failures, "import finished");
    }
    Ok(())
}

// Anywhere with a client (an axum handler, a job, a CLI):
let committed = Batch::new(hench.client())
    .description(format!("Import #{import_id}"))
    .on_success::<import_finished>((import_id,))
    .on_death::<import_failed>((import_id,))
    .jobs(|jobs| async move {
        // `jobs` is a collecting `Client`: every typed or raw push is buffered.
        for row in 0..rows {
            import_row::perform_async(&jobs, (import_id, row)).await?;
        }
        Ok::<_, hench::Error>(())
    })
    .await?;
tracing::info!(bid = %committed.bid, jobs = committed.jids.len(), "import started");
```

The same without a closure:

```rust
let batch = Batch::new(&client).on_complete::<report>((day,));
let jobs = batch.client();
send_mail::perform_bulk(&jobs, recipients).await?;   // buffered: returns jids, writes nothing
let committed = batch.commit().await?;               // one script
```

Signatures (all public items documented; types `#[non_exhaustive]` where they may grow):

```rust
/// A batch being defined: a new batch, or an existing one reopened to add members.
/// Nothing is written until `commit`/`jobs` completes.
#[must_use = "a batch is written only by `commit` or `jobs`"]
pub struct Batch { /* client, bid, mode (new | open), settings, callbacks, collector */ }

impl Batch {
    /// A new batch pushing to `client`'s Redis (or testing store). If `client` is the collecting
    /// client of another definition, the new batch is a child of that batch, committed with it.
    pub fn new(client: &Client) -> Self;
    /// Reopens batch `bid` to add members (checked at commit: exists, not succeeded, not
    /// cancelled). `Err(Error::Batch(BatchError::InvalidBid))` if `bid` is not 24 hex chars.
    pub fn open(client: &Client, bid: impl Into<String>) -> Result<Self>;
    /// A new batch with a chosen bid (24 lowercase hex characters, `InvalidBid` otherwise), for
    /// idempotent creation by a job that may run twice: with `bid` = the job's jid and
    /// `commit_id(jid)`, a re-run's commit finds its commit id applied and writes nothing
    /// (`already_applied`); a bid that exists with another commit id is `BatchError::Exists`.
    /// (Cross-feature review: a periodic job defining one batch per tick is the common case.)
    pub fn with_bid(client: &Client, bid: impl Into<String>) -> Result<Self>;
    /// The batch id (generated by `new`).
    pub fn bid(&self) -> &str;
    /// Text shown in the dashboard (new batches; at most 1 000 characters, longer is an error
    /// at commit).
    pub fn description(self, text: impl Into<String>) -> Self;
    /// Queue of every callback of this batch (default: each callback job's own queue).
    pub fn callback_queue(self, queue: impl Into<String>) -> Self;
    /// Expiry after the last activity of the unfinished batch (default `batch_ttl`, 30 days;
    /// at least 1 hour).
    pub fn expires_in(self, ttl: Duration) -> Self;
    /// How long the data stays after the batch finished (default `batch_linger`, 24 hours;
    /// at least 30 s).
    pub fn linger(self, linger: Duration) -> Self;
    /// Idempotency key of this commit (default: random). A commit whose id the root batch has
    /// already applied writes nothing and reports `already_applied`.
    pub fn commit_id(self, id: impl Into<String>) -> Self;
    /// Callback job `J` with `args` when the event fires (several per event allowed, at most
    /// 16 per event; arguments and options are checked at commit).
    pub fn on_complete<J: Job>(self, args: J::Args) -> Self;
    pub fn on_success<J: Job>(self, args: J::Args) -> Self;
    pub fn on_death<J: Job>(self, args: J::Args) -> Self;
    /// Same with per-callback options (`J::set().queue("critical").retry(5)`); a setter with
    /// `at`/`delay` is an error at commit (callbacks run when the event is dispatched).
    pub fn on<J: Job>(self, event: BatchEvent, setter: JobSetter<J>, args: J::Args) -> Self;
    /// A client whose pushes join this batch (buffered until the commit). Clones share the
    /// buffer; pushes after the commit fail with `BatchError::Closed`.
    pub fn client(&self) -> Client;
    /// Writes the definition (one script; resent until a definite reply, at most
    /// `batch_commit_timeout`). For a child (created from a collecting client) it only stages
    /// the definition into the parent's commit and returns at once.
    pub async fn commit(self) -> Result<Committed>;
    /// `client()`, then `define(client)`, then `commit()`. An error from `define` discards the
    /// definition (nothing written) and is returned as is.
    pub async fn jobs<T, E, F, Fut>(self, define: F) -> std::result::Result<Committed<T>, E>
    where
        F: FnOnce(Client) -> Fut,
        Fut: Future<Output = std::result::Result<T, E>> + Send,
        T: Send,
        E: From<Error>;
}

/// What a commit wrote.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Committed<T = ()> {
    /// The batch defined (the root of the commit).
    pub bid: String,
    /// Members pushed by this commit, children's included, in push order.
    pub jids: Vec<String>,
    /// Jobs not pushed because a unique lock was held by another job (`OnConflict::Drop`).
    pub duplicates: Vec<String>,
    /// Child batches created by this commit.
    pub children: Vec<String>,
    /// The commit id had been applied before this call (an explicit `commit_id` reused):
    /// nothing was written now.
    pub already_applied: bool,
    /// What the `jobs` closure returned.
    pub value: T,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum BatchEvent { Complete, Success, Death }

/// The `batch_callback` of a callback job: the batch whose event it is for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchCallback { bid: String, event: BatchEvent }
impl BatchCallback {
    pub fn bid(&self) -> &str;
    pub fn event(&self) -> BatchEvent;
    /// The status of that batch (`None` once its data has expired).
    pub async fn status(&self, ctx: &JobContext) -> Result<Option<api::BatchStatus>>;
}

/// Why a batch operation was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum BatchError {
    NotFound { bid: String },         // reopen, cancel, skip of a batch that does not exist
    Succeeded { bid: String },        // reopen or cancel after success
    Cancelled { bid: String },        // reopen of a cancelled batch
    Exists { bid: String },           // `with_bid` of a bid already used by another commit
    InvalidBid { bid: String },
    TooLarge { jobs: usize, limit: usize },        // more than batch_max_commit_jobs
    Closed,                           // push through a collecting client after its commit
    DuplicateJid { jid: String },     // the same jid twice in one definition
    DeleteChild { bid: String, parent: String },   // §2.10
    Unconfirmed { bid: String, commit_id: String }, // no definite reply within the timeout
}
// `hench::Error` (already #[non_exhaustive]) gains `Batch(BatchError)`.
```

Notes:

- The closure receives the collecting `Client` **by value**. That keeps the returned future
  nameable and checkable for `Send` (`Fut: Send`), so `Batch::jobs` works in axum handlers and
  spawned tasks. An async closure taking `&Client` cannot be bounded `Send` on stable Rust 1.88.
- `E: From<Error>` lets the closure use the caller's error type: `hench::Error`,
  `anyhow::Error`, `JobError` (inside jobs, `?` just works), or an application error. Type
  inference may need `Ok::<_, E>(..)`; the docs show it.
- A collecting client behaves like the client it came from: same default job options, client
  middleware run at push time, same Redis pool for middleware. A push returns its jid at once.
  Testing modes are honoured at commit (§2.11).
- **Callbacks** are built at commit, because building them is async: normalized with the callback
  job type's options; `callback_queue`, then the setter's options, override them; then client
  middleware runs; then they are stored without `enqueued_at`, with `bid` set to the parent's bid
  (for a child) and `batch_callback` set. Middleware that halts a callback drops it with a
  warning; middleware that schedules one (sets `at`) is an error at commit. Callback templates are
  normalized with uniqueness removed (the `unique` field is stripped, cross-feature review): the
  dispatch script takes no lock, a callback is pushed once per event by construction, and a template
  carrying a digest without a lock would count a unique "lapse" at every acknowledgement.
- **A reopened batch only gains members and children.** Settings that belong to its creation
  (`description`, callbacks, `callback_queue`, `expires_in`, `linger`) on an `open` handle are an
  `InvalidJob` error at commit. Sidekiq Pro likewise allows nothing but adding jobs once a batch
  exists. `Batch::open` on a collecting client is refused (`InvalidJob`), because a commit has one
  root.
- **A child's `commit`/`jobs` returns at once**, with a `Committed` describing what will be written
  with its root (bid, jids, children). Nothing of it exists in Redis until the root's commit
  succeeds, and nothing ever exists if the root's definition fails.
- **Limits** are checked at push time, so they fail fast inside the closure:
  `batch_max_commit_jobs` (default 10 000, measured in §2.16; round 3: each child batch counts
  as 10 jobs, since thousands of children made one commit block Redis for over a second), unique
  jids within one definition, at most 16 callbacks per event, templates up to 64 KiB per event.

#### 2.3.2 Inside jobs

```rust
impl JobContext {
    /// A handle reopening this job's batch (no I/O; checked at commit). For a callback of a
    /// child batch this is the parent: callbacks are members of their parent.
    pub fn batch(&self) -> Option<Batch>;
    /// The batch and event this callback job is for.
    pub fn batch_callback(&self) -> Option<BatchCallback>;
    /// The status of this job's batch.
    pub async fn batch_status(&self) -> Result<Option<api::BatchStatus>, JobError>;
    /// Whether this job's batch was cancelled (one HGET; `false` outside batches).
    pub async fn is_batch_cancelled(&self) -> Result<bool, JobError>;
    /// `Err(JobError::batch_cancelled())` when this job's batch was cancelled: stop early with
    /// `ctx.check_batch().await?` between steps of a long job.
    pub async fn check_batch(&self) -> Result<(), JobError>;
}

impl JobError {
    /// Class `hench::BatchCancelled`.
    pub const BATCH_CANCELLED: &'static str = "hench::BatchCancelled";
    /// Returned by the endpoint for members of cancelled batches; workers count the member as
    /// skipped and acknowledge it (no retry, no error report).
    pub fn batch_cancelled() -> Self;
    pub fn is_batch_cancelled(&self) -> bool;
}
```

Huge batches use loader jobs that each reopen the batch. An explicit commit id makes each
loader's addition idempotent across its own retries:

```rust
#[hench::job(queue = "loaders")]
async fn load_page(ctx: &JobContext, import_id: i64, page: u32) -> Result<(), JobError> {
    let ids = fetch_page(import_id, page).await?;
    let batch = ctx.batch().ok_or_else(|| JobError::msg("load_page runs in a batch"))?;
    batch
        .commit_id(format!("page-{page}"))      // a retry of this loader adds nothing twice
        .jobs(|jobs| async move {
            import_row::perform_bulk(&jobs, ids.into_iter().map(|id| (import_id, id)).collect()).await?;
            Ok::<_, JobError>(())
        })
        .await?;
    Ok(())
}
```

The step pattern (Sidekiq Pro's "really complex workflows"), written by hand:

```rust
#[hench::job]
async fn step1_done(ctx: &JobContext, order_id: i64) -> Result<(), JobError> {
    // A callback of the step-1 batch is a member of the overall batch: reopen it.
    let overall = ctx.batch().ok_or_else(|| JobError::msg("step1_done is a child callback"))?;
    overall
        .commit_id("step-2")
        .jobs(|jobs| async move {
            Batch::new(&jobs)                                  // child of the overall batch
                .description("step 2")
                .on_success::<step2_done>((order_id,))
                .jobs(|step2| async move {
                    charge_card::perform_async(&step2, (order_id,)).await?;
                    print_label::perform_async(&step2, (order_id,)).await?;
                    Ok::<_, JobError>(())
                })
                .await?;                                       // staged into the overall commit
            Ok::<_, JobError>(())
        })
        .await?;
    Ok(())
}
```

#### 2.3.3 Payload

| Field | Type | Notes |
|---|---|---|
| `bid` | string | already reserved (§2 of DESIGN.md): the batch a member belongs to. Set by the collecting client before client middleware runs, so middleware sees it. A push through a collecting client of a payload whose `bid` is set to another batch is refused (`InvalidJob`). |
| `batch_callback` | object `{"bid": string, "event": "complete" \| "success" \| "death"}` | **new named field** (`Option<BatchCallback>`, added to `JobPayload::RESERVED_KEYS`), set on callback jobs only. |

Readers stay lenient. A `bid` that is not a valid bid (24 lowercase hex), such as one from another
system's payload, is treated as "not a member": no accounting and no cancel check, with one warn
log per process per bid.

#### 2.3.4 Configuration (`HenchBuilder`, `Config`, `Client`)

| Builder method | Config field | Default | Meaning |
|---|---|---|---|
| `batch_ttl(Duration)` | `batch_ttl` | 30 days | expiry of unfinished batches after their last activity (≥ 1 h) |
| `batch_linger(Duration)` | `batch_linger` | 24 h | data kept after a batch finishes (≥ 30 s) |
| `batch_max_commit_jobs(usize)` | `batch_max_commit_jobs` | 10 000 | members per commit (1 to 100 000) |
| `batch_commit_timeout(Duration)` | `batch_commit_timeout` | 30 s | how long a commit with an unknown outcome is resent |
| `batch_maintenance(bool)` | `batch_maintenance` | true | run the maintenance loop (§2.6.4) |
| `batch_maintenance_interval(Duration)` | `batch_maintenance_interval` | 30 s | its period (± 20 % jitter) |
| `batch_dispatch_grace(Duration)` | `batch_dispatch_grace` | 10 s | age of an undispatched event before maintenance dispatches it |

`Client` carries the first four as `BatchDefaults`: `Client::with_batch_defaults(..)` for clients
created without a builder, and the builder copies them into `hench.client()`. TOML (`hench::cli`):
`batch_ttl`, `batch_linger` (seconds), `batch_max_commit_jobs`, `batch_maintenance`.

### 2.4 Redis data model

All key names come from `hench::keys` helpers. Bids are validated as 24 lowercase hex characters,
so no bid can produce another kind's key. The prefixes `batch:`, `batches`, `batch-events`,
`batch-maintenance` and `b|` are reserved; they overlap neither the unique design's (`unique:`,
`unique-locks`, `u|`) nor the periodic design's (`periodic…`).

| Key (`keys.rs` helper) | Type | Contents | Expiry |
|---|---|---|---|
| `batch:<bid>` (`batch(bid)`) | HASH | definition, counters, event times (below) | `batch_ttl` after the last activity (≥ the latest scheduled member + `batch_ttl`); linger once finished |
| `batch:<bid>:pending` (`batch_pending(bid)`) | SET | jids of members not yet succeeded or skipped | the hash's (inherited by every script that may create it) |
| `batch:<bid>:failed` (`batch_failed(bid)`) | HASH | jid → failure summary JSON, for members that failed and have not succeeded since | the hash's |
| `batch:<bid>:dead` (`batch_dead(bid)`) | SET | jids of dead members | the hash's |
| `batch:<bid>:kids` (`batch_kids(bid)`) | SET | bids of child batches | the hash's |
| `batch:<bid>:commits` (`batch_commits(bid)`) | SET | commit ids applied with this batch as root | the hash's |
| `batches` (`BATCHES`) | ZSET | member = bid; score = expiry of its keys (epoch s, Redis clock) | none; expired members trimmed by score |
| `batch-events` (`BATCH_EVENTS`) | ZSET | outbox: bid → time its oldest undispatched event fired (epoch s) | none; members removed by their dispatch |
| `batch-maintenance` (`BATCH_MAINTENANCE`) | STRING | `SET NX PX` lock of the maintenance loop (holder identity) | `PX` 0.8 × interval |
| `b\|YYYYMMDD\|H:MM` (`batch_minute(at)`) | HASH | per-minute counters `created`, `complete`, `success`, `death`, `cancelled` (optional, §2.15) | 8 h (`keys::FEATURE_MINUTE_TTL`) |

The per-minute counter hashes of the four features share one convention (cross-feature review): key
`<letter>|YYYYMMDD|H:MM` from `keys::<feature>_minute(at)` (`u` unique, `b` batches, `l` limits), the
writing client's UTC minute, `HINCRBY` under `pcall` as a script's last writes, 8 h expiry, no rollup,
read for minute periods of at most 8 hours by one crate-wide reader. `BATCH_EVENTS` stays the name of
the outbox (`batch-events`): in this design "event" means `complete`/`success`/`death`.

Fields of `batch:<bid>` (short names in the style of `it-<jid>`):

| Field | Written by | Contents |
|---|---|---|
| `v` | COMMIT | data format version, `1`; readers refuse unknown versions explicitly (L15) |
| `created_at` | COMMIT | epoch ms, Redis clock |
| `desc` | COMMIT | description (≤ 1 000 chars) |
| `parent` | COMMIT | parent bid (children only) |
| `ttl`, `linger` | COMMIT | seconds, copied from the definition's settings |
| `total` | COMMIT, DISPATCH (callbacks of children) | members registered |
| `skipped` | SKIPPED | members skipped |
| `kids`, `kc`, `ks` | COMMIT (`kids`), DISPATCH of a child (`kc`, `ks`) | children; children with `complete` dispatched; with `success` dispatched |
| `open` | phase 2 (staged commits) | definitions in progress; events wait while > 0 |
| `complete_at`, `success_at`, `death_at` | the script that fires the event | epoch ms |
| `complete_dt`, `success_dt`, `death_dt` | DISPATCH | when the event was dispatched |
| `cancelled_at` | CANCEL | epoch ms of the first cancel |
| `finished_at` | DISPATCH | when the linger started (success, or complete of a cancelled batch) |
| `touched_at` | COMMIT, `hb_touch`, TOUCH | last expiry refresh |
| `cb_complete`, `cb_success`, `cb_death` | COMMIT | callback templates: JSON array of `{"q": queue, "jid": jid, "j": job JSON without enqueued_at}` |

Failure summary (value in `:failed`), built by the processor or the API:
`{"class":"ImportRow","error_class":"std::io::error::Error","error":"<first 200 chars>",
"state":"retry"|"dead","reason":"retry"|"morgue"|"no_retry"|"discarded"|"deleted"|"killed",
"retry_count":3,"at":<ms>,"next_at":<retry epoch s, retries only>}`. There is no backtrace (L12):
the job in the retry set or the morgue has the details, and the dashboard links to it by jid.

Memory, measured on Redis 7.0.15 (Appendix A): 72 bytes per pending member (hashtable encoding;
Redis 7.2+ stores sets of up to 128 short strings as listpacks, smaller still), about 200 bytes for
the hash without callbacks, plus about 300 bytes per failed member and the callback templates. A
batch of 100 000 jobs costs about 7 MB at its peak and shrinks as members succeed; 1 million
pending members cost about 72 MB, which the sizing notes in the README must state (R1).

### 2.5 Lua scripts (`crates/hench/src/batch/lua/`, embedded with `include_str!`)

#### 2.5.1 Rules every batch script follows

1. **Declared keys only.** Every key a script touches is in `KEYS`, the rule hench's existing
   scripts and the sibling designs follow, which Dragonfly's default script mode requires. Callers
   read whatever they need to name keys first; dispatch, for example, reads the parent bid and
   the callback queues.
2. **Redis 6.2 commands only**: `SADD SREM SISMEMBER SCARD SMEMBERS HSET HSETNX HMGET HGET HDEL
   HLEN HINCRBY ZADD NX ZSCORE ZREM PEXPIRE PTTL TYPE EXISTS LPUSH UNLINK TIME`.
3. **Time comes from Redis** (`TIME`) for every stored time, index score and expiry. The client
   clock only enters job fields it already sets today (`enqueued_at`, run-at times) and the
   `horizon` it derives from them.
4. **Checks before writes; the first write decides under `OOM`.** Redis checks `maxmemory` only
   before a script's first write. A commit refused under `OOM` writes nothing (verified), and a
   failure report likewise (its first write is a refused `HSET`). A success report starts with
   `SREM`, which is allowed under `OOM`, and then completes, fired events included (verified): a
   finishing batch is never left half-updated.
5. **Idempotent by construction**: membership tests on the jid, write-if-absent for event times,
   the outbox entry plus `*_dt` fields for dispatch, commit ids for commits. Every script can be
   resent after a lost reply.
6. **Every write that may create a key sets its expiry in the same script** (`hb_inherit_ttl`).
7. **No user payload is decoded in Lua.** `cjson` decodes only hench's own template arrays; job
   JSON is never decoded. Number precision of i64 arguments is therefore safe, and the dispatch
   script adds `enqueued_at` by replacing the final `}` of a template that the client stored
   without that key.
8. **Independent of the fetcher's scripts.** Accounting runs *before* the outcome write (§2.6.1),
   so `ACK`/`SETTLE`/`PUSH_BACK`, and the unique design's `*_UNIQUE` variants, need no batch
   variant: there is no script combinatorics.
9. **Conventions shared by the four features** (cross-feature review): the scripts are `.lua` files
   under `crates/hench/src/batch/lua/`, assembled by the crate-wide script builder after the shared
   prelude (`crate::lua`, which also checks at start-up that `TIME` may be followed by writes); names
   carry the `BATCH_` prefix (already so); helpers use `hb_` (unique's `hu_`, limits' `hl_` and
   periodic's `hp_` never clash, so `BATCH_COMMIT` can prepend the unique library); `TIME` is read
   once per script.

#### 2.5.2 Shared helpers (`lib.lua`, prepended to each script)

```lua
local function hb_now_ms()
  local t = redis.call('TIME')
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

local function hb_secs(ms) return string.format('%.3f', ms / 1000) end

-- Gives `key` the remaining lifetime of the batch hash, so that every key of a batch expires
-- together. Called after any write that may have created `key`.
local function hb_inherit_ttl(hash, key)
  local ttl = redis.call('PTTL', hash)
  if ttl > 0 then redis.call('PEXPIRE', key, ttl) end
end

-- Fires the events whose conditions now hold: `complete` (every job not yet succeeded has
-- failed at least once, every child batch's complete phase is over), then `success` (no job
-- left, every child succeeded, not cancelled). A definition in progress (`open` > 0) blocks
-- both. Records the fire time in the hash and puts the batch in the dispatch outbox.
-- `failed` may be nil (a batch created by this script has no failures).
-- Returns the list of events fired by this call.
local function hb_evaluate(hash, pending, failed, events, bid, now)
  local h = redis.call('HMGET', hash, 'v', 'kids', 'kc', 'ks', 'open',
    'complete_at', 'success_at', 'cancelled_at')
  if not h[1] then return {} end
  if tonumber(h[5] or '0') > 0 then return {} end
  local kids, kc, ks = tonumber(h[2] or '0'), tonumber(h[3] or '0'), tonumber(h[4] or '0')
  local npending = redis.call('SCARD', pending)
  local nfailed = failed and redis.call('HLEN', failed) or 0
  local fired = {}
  if not h[6] and npending == nfailed and kc >= kids then
    redis.call('HSET', hash, 'complete_at', now)
    fired[#fired + 1] = 'complete'
  end
  if not h[7] and not h[8] and npending == 0 and ks >= kids then
    redis.call('HSET', hash, 'success_at', now)
    fired[#fired + 1] = 'success'
  end
  if #fired > 0 then
    redis.call('ZADD', events, 'NX', hb_secs(now), bid)
  end
  return fired
end

-- Refreshes the expiry of an unfinished batch at most once an hour, never shortening it.
-- A finished batch (`finished_at`, set when its linger starts) keeps its linger.
-- Returns 1 when it refreshed (the caller then refreshes the ancestors, BATCH_TOUCH), else 0.
-- `keys` lists every key of the batch (hash first).
local function hb_touch(keys, index, bid, now)
  local hash = keys[1]
  local h = redis.call('HMGET', hash, 'ttl', 'touched_at', 'finished_at')
  if not h[1] or h[3] then return 0 end
  if now - tonumber(h[2] or '0') < 3600000 then return 0 end
  local ttl = math.max(tonumber(h[1]) * 1000, redis.call('PTTL', hash))
  for _, key in ipairs(keys) do redis.call('PEXPIRE', key, ttl) end
  redis.call('HSET', hash, 'touched_at', now)
  redis.call('ZADD', index, hb_secs(now + ttl), bid)
  return 1
end
```

`hb_evaluate` is the only place where events fire. It reads the counts and fires what now holds,
recording the time and adding the batch to the outbox in the same script that changed the counts.
Every script that can change a batch's counts calls it: success, failure, skip, cancel, commit
(new batches only) and dispatch (for the parent).

#### 2.5.3 `BATCH_COMMIT` (client; prototype `commit.lua`)

- **KEYS**: `[1]` `queues`, `[2]` `schedule`, `[3]` `batches`, `[4]` `batch-events`. Then four
  keys per batch of the commit, root first and then children in definition order: hash,
  `:pending`, `:kids`, `:commits`. Then the distinct destination queue lists. When some member
  carries a unique digest (cross-feature review): then `unique-locks`, the unique per-minute
  counter hash (`keys::unique_minute`), and one lock key `unique:<digest>` per distinct digest.
- **ARGV**:
  - `[1]` commit id, `[2]` number of batches;
  - 10 values per batch: mode (`new`/`open`), bid, parent (1-based index of an earlier batch of
    this commit, `0` = none), `ttl` (s), `linger` (s), description, `cb_complete`, `cb_success`,
    `cb_death` (template arrays or `''`), horizon (latest run-at among its scheduled members,
    epoch s, `0` = none);
  - the number of jobs, then 6 values per job: batch index, jid, KEYS index of its destination,
    kind (`q`/`s`), queue name (`q`) or score (`s`), JSON as stored (`Stored::encode`: queued
    jobs with the client's `enqueued_at`, scheduled ones without `at`/`enqueued_at`);
  - a trailing unique section, so that commits without unique members keep this exact size: the
    number of unique members (`0` when none), then 8 values each: job index (1-based, in the job
    list above), KEYS index of its lock, digest, class, queue, until, for (ms), on_conflict (`drop`
    or `error`; `replace` never reaches the script: the collecting client refuses it at push time).
- **Steps**:
  1. `TIME` (once; the unique helpers receive this `now`).
  2. **Idempotency**: `SISMEMBER <root>:commits <cid>`; if the id is there, return `{'already'}`.
  3. **Checks** (no write yet):
     - each `open` batch must exist (`NOBATCH`), not have succeeded (`SUCCEEDED`) and not be
       cancelled (`CANCELLED`);
     - each `new` batch must not exist (`EXISTS`, a bid collision, or `Batch::with_bid` reusing a
       bid with another commit id);
     - parent indexes must point to earlier batches, and only `new` batches have one;
     - each distinct destination must be of type `none` or `list` (`zset` for `schedule`), else
       `WRONGTYPE <key>`;
     - each unique member: `pcall(hu_check, lock, jid)` from the unique design's frozen library
       (prepended), with a Lua table `seen[digest] = jid` of the members already accepted by this
       commit. A member is a **duplicate** when the lock has another holder, when `seen` already has
       its digest, or when the result is `'self'` (its explicit jid already holds the lock: that job
       is already in Redis outside this batch). With `on_conflict = error` the first duplicate
       refuses the commit (`DUPLICATE <class> <digest> <holder>`, mapped to `Error::DuplicateJob`);
       with `drop` it is marked to be left out. A lock key of the wrong type is `WRONGTYPE <key>`.
  4. **Writes**, batch rows first, so that under `OOM` the first write is refused before anything
     changes:
     - `HSET` each new batch hash (`v`, `created_at`, `ttl`, `linger`, zero counters,
       `touched_at`, plus `desc`, `parent` and callback templates when present); for a child,
       `SADD <parent>:kids` and `HINCRBY <parent> kids 1`;
     - for each job not marked as a duplicate: `SADD queues` once per queue, then `LPUSH`, or
       `ZADD schedule`; for a unique member, then `pcall(hu_take, ...)` with the unique TTL rule
       (`for + max(0, at - now)`, `at` = its score when scheduled); then `SADD <batch>:pending jid`,
       whose results are added to `total` with one `HINCRBY` per batch;
     - for each duplicate: `pcall(hu_count, events, class, 'd')`.
  5. **Expiry**: each batch's keys get
     `max(ttl, horizon − now + ttl, current PTTL)` and its index score. Then
     `SADD <root>:commits cid`, which takes the root hash's expiry.
  6. **Empty new batches** (no member, no child, duplicates excluded) fire through `hb_evaluate`,
     deepest first. Last, when locks were taken: `hu_trim(unique-locks, now, 10)` (a removal: it
     must be the script's last write, unique design §2.8.1 rule 5).
- **Returns** `{'ok', {bids that fired…}, {duplicate jid, holder, …}}` (the client dispatches the
  fired bids, §2.6.3, and reports the duplicates in `Committed::duplicates`), `{'already'}`, or an
  error reply `NOBATCH|SUCCEEDED|CANCELLED|EXISTS|DUPLICATE|WRONGTYPE <name>`, which maps to
  `BatchError` (`EXISTS` to `BatchError::Exists`), `Error::DuplicateJob` or `Error::Redis`.
- **Client side** (`batch/commit.rs`):
  - Encode the jobs and group the destinations.
  - Refuse more than `batch_max_commit_jobs` jobs. The limit is on the number of jobs, but the
    payloads' total size is logged at debug.
  - Send through `RedisPool::with_retry`, which retries once after a dropped connection or
    `READONLY`.
  - If the outcome is unknown (a timeout, or a dropped connection after that retry), resend the
    *same* script and arguments with backoff (100 ms doubling to 2 s) until a definite reply or
    `batch_commit_timeout`. An `'already'` reply to a resend means the first attempt applied
    (`already_applied = false`); to the first attempt, `already_applied = true`. No definite reply
    in time gives `BatchError::Unconfirmed { bid, commit_id }`, and the caller may commit again
    with that `commit_id`.
  - Definite refusals (`OOM`, errors) return at once, like `push`.

#### 2.5.4 Accounting scripts (processor and API; prototypes `succeeded.lua`, `failed.lua`, `skipped.lua`)

All three take **KEYS** `[1]` hash, `[2]` `:pending`, `[3]` `:failed`, `[4]` `:dead`, `[5]`
`:kids`, `[6]` `:commits`, `[7]` `batches`, `[8]` `batch-events`, and **ARGV** `[1]` jid, `[2]`
bid. They return `{applied (1/0), dispatch needed (1/0), refreshed (1/0), events fired…}`.
- "Dispatch needed" means the batch is in the outbox, whoever fired the event, so any processor
  that touches a batch helps dispatch it.
- "Refreshed" means `hb_touch` extended the batch's expiry (at most hourly). The caller then
  refreshes the ancestors with `BATCH_TOUCH` (§2.5.6), so a parent cannot expire while a child
  is still active (Pro #2889).

```lua
-- BATCH_SUCCEEDED (ARGV [1] jid, [2] bid)
local jid, bid = ARGV[1], ARGV[2]
local now = hb_now_ms()
local fired = {}
local touched = 0
local applied = redis.call('SREM', KEYS[2], jid)
if applied == 1 then
  redis.call('HDEL', KEYS[3], jid)
  redis.call('SREM', KEYS[4], jid)
  fired = hb_evaluate(KEYS[1], KEYS[2], KEYS[3], KEYS[8], bid, now)
  touched = hb_touch({KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6]}, KEYS[7], bid, now)
end
local undispatched = redis.call('ZSCORE', KEYS[8], bid) and 1 or 0
return {applied, undispatched, touched, unpack(fired)}
```

| Script | Extra ARGV | Steps (each only if the jid is still pending) |
|---|---|---|
| `BATCH_SUCCEEDED` | none | `SREM pending` (the guard); `HDEL failed`; `SREM dead`; `hb_evaluate`; `hb_touch` |
| `BATCH_FAILED` | `[3]` kind `retry`/`dead`, `[4]` summary JSON | `SISMEMBER pending` (the guard); `HSET failed jid summary` (kept current); for `dead`: `SADD dead`, `HSETNX death_at`, and if first, outbox entry and `death` fired; `hb_evaluate` (`complete` may fire); `hb_touch` |
| `BATCH_SKIPPED` | `[3]` mode `cancel`/`operator` | `cancel`: return `{-1, 0, 0}` unless `cancelled_at` is set (verified atomically, so a stray `JobError::batch_cancelled()` cannot drop a member of a live batch); `SREM pending` (guard); `HDEL failed`; `SREM dead`; `HINCRBY skipped 1`; `hb_evaluate`; `hb_touch` |

#### 2.5.5 `BATCH_DISPATCH` (processor, API, maintenance; prototype `dispatch.lua`)

- **Client side first**: `HMGET batch:<bid> parent cb_death cb_complete cb_success`; parse the
  template arrays to list their queues. The templates never change after the commit, so the
  later script sees the same ones.
- **KEYS**: `[1]` `batch-events`, `[2]` `batches`, `[3]` `queues`, `[4..9]` the batch's six keys,
  `[10..15]` the parent's six keys (or the batch's own six when it has no parent), `[16..]` the
  callback queues.
- **ARGV**: `[1]` bid, `[2]` parent bid or `''`, `[3]` the number of callback queues, then their
  names (`ARGV[3+i]` names `KEYS[15+i]`).
- **Steps**:
  1. If the bid is not in the outbox, return `{0}` (already dispatched).
  2. Check that the stored parent matches the declared one.
  3. Collect the fired, undispatched events in the order `death`, `complete`, `success`, and check
     every destination's type before any write. Dispatching `complete` before `success` guarantees
     `ks ≤ kc` in the parent.
  4. For each event:
     - push its templates with `,"enqueued_at":<now>}` replacing the final `}`, plus
       `SADD queues`;
     - if the parent exists, `SADD <parent>:pending <callback jid>`, then the parent's `kc` +1
       (`complete`), `ks` +1 (`success`), or, for `death`, `HSETNX death_at` and an outbox entry
       for the parent;
     - `HSET <event>_dt`;
     - after `success`, or `complete` of a cancelled batch: `HSET finished_at`, every key's expiry
       set to `linger`, and the index score updated.
  5. `ZREM` the outbox entry.
  6. For the parent: `HINCRBY total` by the callbacks registered, `hb_inherit_ttl` on its
     `:pending`, `hb_evaluate`, and `hb_touch`.
- **Returns** `{1, events fired in the parent…}`. The client then dispatches the parent if it is
  in the outbox, repeating up the tree.
- If the parent's data is gone (expired, deleted), the callbacks are pushed but registered
  nowhere, and the parent's counters are not touched.

#### 2.5.6 `BATCH_TOUCH` (processor, after a refresh; prototype `touch.lua`)

- **KEYS**: the batch's six keys, `[7]` `batches`. **ARGV**: bid.
- **Steps**: if the batch exists and is not finished, set every key's expiry to
  `max(ttl, current PTTL)` (never shorter), update `touched_at` and the index score.
- **Returns** its parent bid (`''` for a root, a missing or a finished batch). The caller walks
  up the chain one script per level.
- It runs only when an accounting script reported a refresh, that is, at most once an hour per
  active batch.

#### 2.5.7 `BATCH_CANCEL` (API; prototype `cancel.lua`)

- **KEYS**: hash, `:pending`, `:failed`, `:kids`, `batch-events`. **ARGV**: bid.
- **Steps**: `NOBATCH` or `SUCCEEDED` errors; `HSETNX cancelled_at`; `hb_evaluate` (`complete` may
  fire at once if every member left has already failed, or if nothing is left).
- **Returns** `{first (1/0), dispatch needed, child bids…}`. The client cancels each child the
  same way, level by level, 500 scripts per pipelined round trip, the whole tree in one call
  (round 3: it stopped at 1 000 batches with an error, and a repeated call walked the same
  thousand again; batches-4), and then dispatches. Descendants that are gone or succeeded are
  left alone; another refusal is returned after the rest of the tree is cancelled.

#### 2.5.8 `BATCH_DELETE` (API)

- **KEYS**: the six keys, `batches`, `batch-events`, the parent's hash. **ARGV**: bid, `force`.
- **Steps**: refuse (`DELETE_CHILD`) a batch that has a parent which still exists and has not
  finished, unless `force` (§2.10); then `UNLINK` the six keys, `ZREM batches`,
  `ZREM batch-events`.
- **Returns**: the number of keys removed. The client reads the whole tree from the `kids`
  sets (pipelined, level by level) and deletes it from the deepest level up, forced, 500
  scripts per round trip, the batch named last (round 3: it stopped at 10 000 batches and left
  the rest without a word; batches-12).
- Members still queued run normally; their accounting finds no batch and does nothing.

#### 2.5.9 Maintenance commands (no script)

- `SET batch-maintenance <identity> NX PX <0.8 × interval>`
- `ZRANGE batch-events -inf (<now − grace> BYSCORE LIMIT 0 100`, then dispatch each
- `ZREMRANGEBYSCORE batches -inf <now>` (expired index entries)

#### 2.5.10 Cost and size

| Script | Redis time |
|---|---|
| `BATCH_COMMIT` | 3.3 µs per job (1 000 jobs: 3.4 ms; 10 000: 34 ms; 50 000: 250 ms, when the pending set's rehashing shows) |
| accounting | median 24 µs; maximum 2 ms over 20 000 calls (the last call fires both events) |
| dispatch | proportional to its callbacks |

A commit is one script, so it blocks Redis for its whole duration; hence the 10 000-job default
cap (34 ms), which is comparable to the latency of a slow `KEYS` call and documented. Larger
definitions use loader jobs, or phase 2 (§2.20).

### 2.6 Engine integration (file by file)

#### 2.6.1 `processor.rs`

Membership is read from the job **as fetched** (`work.parse()`), not as server middleware may
have changed it, the same rule that retries follow: `BatchMember { bid, jid }` exists only when
`bid` is valid. Nothing changes for other jobs.

```text
run_job(work, job):                        # inside the job's span, as today
    member = BatchMember::from_fetched(&work)
    gate = throttle_gate(...)              # rate-limiting design §2.6, before start_work and middleware
    if gate refused:
        if member.is_some() and HGET batch:<bid> cancelled_at:   # only on a refusal (§2.9)
            account(member.skipped(Cancel)); acknowledge(work)  # skipped, never parked
        else:
            reschedule(...)                # PUSH_BACK to `schedule`: not an outcome, no accounting
        return
    outcome = execute(...)                 # server chain + endpoint (cancel check, §2.6.2) + perform
    ...                                    # stats, metrics, logger as today
    match outcome:
      Ok(()) =>
          report = member.map(|m| account(m.succeeded()))      # before the acknowledgement
          acknowledge(work)
          dispatch_if(report); refresh_if(report)
      Err(e) if e.is_batch_cancelled() && member.is_some() =>
          match account(member.skipped(Cancel)):
              NotCancelled => fail_and_report(work, job, e)     # treated as a failure
              _            => acknowledge(work); dispatch_if(..) # no error report, not "failed"
      Err(e) if e.is_interrupted()          => push_back(...)   # unchanged: no accounting
      Err(shutdown) while stopping          => requeue(...)     # unchanged: no accounting
      Err(e) if over limit (budget left)    => reschedule(...)  # rate limiting §2.5: no accounting
      Err(e) => fail(...):                  # includes over-limit jobs past their budget
          verdict = retry::decide(...)                          # unchanged
          report  = member.map(|m| account(m.failed(kind(&verdict), summary(&verdict, &e))))
          settle(work, settlement(verdict))                     # unchanged script
          retry::finish(verdict)                                # death handlers, unchanged
          dispatch_if(report); refresh_if(report)

account(op) = persist(|| op.run(redis))   # write_with_backoff: retried while Redis is unavailable
                                          # (OOM included) until a hard shutdown, 3 tries otherwise
kind(Retry) = retry; kind(Kill) = dead/morgue; kind(NoRetry) = dead/no_retry;
kind(Discard) = dead/discarded
```

- **Why before the outcome write.** If the process dies between the two writes, the job runs
  again (reliable fetch recovers it, or the own-list reconciliation pushes it back), and its
  accounting is then a no-op. The reverse order could acknowledge a job whose batch never hears
  of it. With asynchronous replication, the accounting precedes the acknowledgement in the
  replication stream, so a failover that loses a suffix never keeps the acknowledgement without
  the accounting.
- **When the accounting fails for good** (a non-unavailable error three times, such as
  `WRONGTYPE` on a batch key, or a hard shutdown during an outage):
  - reported to the error handlers with the new context `could not record a job in its batch`,
    then the outcome is written anyway, so a broken batch never makes a job run forever;
  - the batch shows the member as pending, and the dashboard can skip it;
  - during a hard shutdown the job is not acknowledged either: it stays in progress, is recovered
    later, and the accounting is retried then.
- **`refresh_if(report)`**: when the report says "refreshed", read `HGET batch:<bid> parent` and run
  `BATCH_TOUCH` up the chain (best effort, after the outcome write; errors at debug). It
  happens once an hour per active batch.
- **`dispatch_if(report)`**: when the report says "dispatch needed", run `batch::dispatch_chain`
  (read the hash, `BATCH_DISPATCH`, repeat for the parent while it is in the outbox), bounded to
  5 s. Errors are logged at `warn` with the bid; maintenance recovers the dispatch after the
  grace period. It runs after the outcome write, in the processor's slot: it is rare (once per
  event) and keeps tests deterministic.
- **Metrics and stats.** A skipped member counts as processed (not failed) and is tracked as the new
  `Outcome::Skipped` (cross-feature review: `p` +1 and a new per-class field `s` +1, not timed, like
  the rate-limiting design's `o` and `t`, so near-zero skip durations do not distort execution-time
  averages and histograms; existing readers ignore unknown fields). The job logger logs `skipped
  (batch cancelled)` at info instead of `done`, under the same per-job log-level rules as `done`.
- **Place in the processor pipeline** (cross-feature review): the order is fixed for all features —
  throttle gate; execution (endpoint: cancellation check, then the unique `Start` release, then
  `perform`); classification; batch accounting; the outcome script (with the unique design's lock
  hold or release inside it); then permit release (rate limiting), `dispatch_if`/`refresh_if`, and
  the periodic design's `RECORD_RUN`. The shared `processor.rs` is changed once, in the common
  phase, with hook calls into `batch::account`, `batch::dispatch`, `limit::engine`,
  `unique::engine` and `periodic::record_run`.
- **No change**: reliable-fetch recovery, own-list reconciliation, bulk requeue at shutdown,
  `reject` (unparseable JSON).

#### 2.6.2 `middleware.rs` (`PerformEndpoint::call`)

Before `perform_json`, if `ctx.payload()` has a valid `bid`, `batch::is_cancelled(ctx)` runs:
- **Redis clients**: `HGET batch:<bid> cancelled_at`;
- **testing clients**: their in-memory table.

If it reports a cancel, the endpoint returns `Err(JobError::batch_cancelled())` without running
the job. A failed read is logged at `warn` and the job runs: only a positive answer skips a job.
Middleware therefore sees skipped members too (and can log them), just as it sees jobs that fail.
The cost is one `HGET` per member execution (§4, D12 discusses a process-wide cache instead).
The check runs **before** the unique design's release of `Start` locks (same endpoint), so a skipped
member's lock is released by its acknowledgement (`ACK_UNIQUE`), never by a `perform` that does not
happen. Both read `ctx.payload()`, the job as fetched (server middleware passes the context it
received to `next`).

#### 2.6.3 `batch/` (new module, crate-internal parts)

| File | Contents |
|---|---|
| `mod.rs` | public types (§2.3), `valid_bid`, `new_bid` |
| `define.rs` | `Batch`, the collector (`ClientMode::Collect`), staging of children, callback templates |
| `commit.rs` | encoding, `BATCH_COMMIT` invocation and resend loop, result mapping |
| `account.rs` | `BatchMember`, the three accounting scripts, failure summaries |
| `dispatch.rs` | `dispatch_chain`, `BATCH_DISPATCH` |
| `maintenance.rs` | the loop of §2.6.4 |
| `model.rs` | the in-memory state machine used by the testing modes (§2.11) |
| `lua/*.lua` | the scripts, built with `concat!(include_str!("lua/lib.lua"), include_str!(..))` into `LazyLock<Script>` values, like `fetch.rs` |

#### 2.6.4 `launcher.rs`: the maintenance loop

When `config.batch_maintenance` is true, the launcher spawns `batch::maintenance::run(engine)`
next to the scheduler, cancelled by `quiet` like the scheduler. Every
`batch_maintenance_interval`, with ± 20 % jitter, it:

1. takes the lock `batch-maintenance` (`SET NX PX`, 0.8 × interval, sent once), and skips the
   round if another process holds it;
2. lists outbox entries older than `batch_dispatch_grace`, at most 100, and dispatches each
   through `dispatch_chain`;
3. trims the index (`ZREMRANGEBYSCORE batches -inf now`).

Failures go through the scheduler-style rate-limited notices (`could not maintain batches`,
first one to the error handlers, then warn logs). A panic is reported
(`batch maintenance crashed; restarting it`) and the loop resumes after a second. The extra Redis
load is one `SET NX` per process per interval, and the work happens once per interval
cluster-wide.

#### 2.6.5 Iterable jobs (`iterable/`)

- Interruption is a push back, so no accounting (Sidekiq Pro #6889 cannot happen).
- Phase 3 (§2.20): the checkpoint `MULTI` of a member also reads `HGET batch:<bid> cancelled_at`.
  If it is set, the job stops like a cancelled iteration (`on_cancel` and the other finishing
  hooks, with `done = cancelled` saved), then returns `JobError::batch_cancelled()`, so the
  member is counted as skipped.
- Without phase 3, an iterable member of a cancelled batch is skipped at its next execution.

#### 2.6.6 Unchanged

`fetch.rs` (no script changes), `retry.rs` (only a helper `batch_kind(&Verdict)`),
`scheduled.rs`, `manager.rs`, `stats.rs`, `flush.rs`.

### 2.7 Client and spine integration

- **`client.rs`.**
  - `ClientMode` gains `Collect { collector: Arc<Collector>, batch: usize }`, the batch index
    inside the collector. A collecting client shares its parent client's pool, middleware chain,
    default job options and `BatchDefaults`.
  - The push path is unchanged up to delivery: normalize, set `bid` (refusing a payload whose
    `bid` names another batch), run middleware, revalidate. Only `raw_push` changes: in `Collect`
    mode it appends the jobs to the collector, under a mutex, instead of writing them. It
    enforces `batch_max_commit_jobs` and unique jids, and fails with `BatchError::Closed` once the
    collector is committed or dropped. It also refuses a job whose `unique.on_conflict` is
    `replace` (`Error::InvalidJob`: replacing a holder that belongs to another batch would strand
    that batch), and keeps each unique member's `on_conflict` for the commit (the stored JSON drops
    it, unique design §2.6.4). `raw_push` returns per-job results since the unique design (pushed,
    duplicate, ...); a collecting client reports every buffered job as pushed, so `push_outcome` on
    it returns a provisional `PushOutcome::Pushed`: duplicates are known at commit
    (`Committed::duplicates`).
  - `Client::batch(&self) -> Batch` is shorthand for `Batch::new(self)`.
  - `Client::batch_status(bid)` returns the status in every mode: Redis through `api`, testing
    clients from memory.
  - `Client::with_batch_defaults`.
  - `push_bulk` through a collecting client works unchanged: `batch_size` is ignored, since the
    commit is the round trip.
- **`payload.rs`**: field `batch_callback: Option<BatchCallback>`, added to `RESERVED_KEYS`,
  plus tests for lenient reading (unknown event → the field is ignored with a warning).
- **`keys.rs`**: the helpers and constants of §2.4, `BATCH_TTL` (30 days), `BATCH_LINGER`
  (24 h), the shared `FEATURE_MINUTE_TTL` (8 h), and unit tests for them.
- **`config.rs`**: the fields and builder methods of §2.3.4, validated in `build()` (bounds), plus
  the new error contexts `could not record a job in its batch`, `could not dispatch batch
  callbacks` and `could not maintain batches`. The registry always contains the internal job
  `hench::WorkflowNext` (§2.17); a user job registered under that name is a build-time
  `Error::Config`.
- **`error.rs`**:
  - `Error::Batch(BatchError)`, displayed as `batch <bid>: <reason>`;
  - `JobError::BATCH_CANCELLED`, `batch_cancelled()`, `is_batch_cancelled()`;
  - `From<Error> for JobError` already exists, so `?` inside jobs works.
- **`job.rs`**: the `JobContext` methods of §2.3.2. `ctx.batch()` builds an `open` handle on the
  job's client. For workers that is the process client, not a collector, so a reopen made by a
  job commits on its own.
- **`job_logger.rs`**: the span already records `bid`; callback jobs also record `batch_event`
  (`complete`/`success`/`death`), and `batch_callback`'s bid goes into `attrs` when
  `logged_job_attributes` asks for it.
- **`lib.rs`**: `pub mod batch;` and the prelude gains `Batch`.
- **`#[hench::job]`**: no new option. Callback jobs are ordinary jobs.

### 2.8 Behaviour under failures

| Situation | Behaviour |
|---|---|
| Error, panic or early return inside the `jobs` closure | nothing written; the closure's error is returned |
| Client middleware fails or panics on a push inside the closure | that push returns the error, as `push` does; the closure decides, and usually aborts the whole definition |
| Process crash during the definition | nothing written (buffer in memory) |
| Commit: connection dropped after the script ran | `with_retry` resends; the commit id makes it `'already'`, reported as applied once (B2) |
| Commit: timeout or no reply | resent until a definite reply or `batch_commit_timeout`, then `BatchError::Unconfirmed` (the definition may or may not be written; a commit with the same `commit_id` settles it) |
| Commit: `OOM`, `READONLY` | refused before any write (verified for `OOM`); `READONLY` is retried once after reconnecting, as for pushes |
| Commit: a destination of the wrong type | `WRONGTYPE <key>` before any write (verified) |
| Crash between a member's accounting and its acknowledgement | the job runs again (recovered or reconciled); its accounting is a no-op; the batch may have fired `success` before the re-run (at-least-once, documented) |
| Lost reply on an accounting script | `persist` resends it; the repeat is a no-op; if an event fired, "dispatch needed" is still returned, because the outbox entry is still there |
| Crash after an event fired, before its dispatch | the outbox entry stays; maintenance dispatches it after `batch_dispatch_grace` (≤ interval + grace) |
| Lost reply on a dispatch | the repeat finds no outbox entry: no-op (verified) |
| Two processes dispatch at once | one dispatches, the other gets `{0}` (verified with 8 concurrent clients) |
| Duplicate executions of one member (orphan recovery of a live but partitioned process, a client resend after a dropped connection, basic fetch) | counted once per jid; a later failure of a stale copy is ignored (verified) |
| Redis unavailable while recording an outcome | accounting retried with backoff (`persist`), then the outcome written; jobs wait in their slots, as today |
| Failover with asynchronous replication | every script replicates as one unit; the accounting precedes the acknowledgement in the stream |
| Redis restarted without persistence | jobs and batches vanish together |
| Restore from an older snapshot | members that ran since run again and the batch replays from the snapshot's state; events that fired after the snapshot fire again (as for any job) |
| Eviction (`volatile-*`, `allkeys-*`) | batch keys are volatile, so batches can be lost or miscounted; start-up already warns unless `noeviction`; the README repeats it (L17) |
| Clock skew between clients and Redis | stored times, index scores and expiries use Redis `TIME`; client clocks only affect `enqueued_at` and run-at times, as for pushes |
| A batch key of the wrong type | accounting fails (reported); the outcome is still written; the batch shows a pending member that an operator can skip |
| A callback queue key of the wrong type | dispatch refused before any write; the outbox entry stays; maintenance retries and reports through rate-limited notices until fixed |
| Parent expired or deleted when a child dispatches | callbacks pushed, not registered; parent effects skipped |
| Batch expired before its members ran (scheduled beyond `expires_in`) | accounting finds no batch: no-op; no callbacks. Commits extend the expiry to the latest scheduled member, and activity refreshes it, so this needs a member scheduled after the commit's horizon (for example by client middleware at retry time) or 30 days without any activity |
| Members processed by a hench version without batch support (rolling deploy) | they are never counted: the batch stays pending until it expires. Deploy workers before defining batches; the heartbeat `info`'s `"features"` map gains `"batches": 1` and the dashboard warns when a live process lacks it (R6) |

### 2.9 Interplay with the rest of hench

- **Retries**: members retry as usual. Each failure updates the summary in `:failed`; the first
  failure makes the member count as "ran" for `complete`.
- **Dead set revival**: a dead member stays pending. `retry`/`add_to_queue` from the morgue or
  the retry set (API, dashboard, `retry_all`) changes nothing in the batch; the revived job's
  success removes it from `pending`, `failed` and `dead`, and `success` fires if it was the last
  (Sidekiq Pro semantics; BullMQ's re-attach bug class cannot occur, L10). `death` is not fired
  again.
- **API delete and kill** (`SortedEntry::delete`, `JobSet::delete_by_value`/`delete_by_jid`,
  dashboard queue-entry deletes, `SortedEntry::kill`, `kill_all`, `DeadSet::kill`):
  - for a job with a valid `bid`, `BATCH_FAILED dead` (`deleted`/`killed`) runs **before** the
    removal; `kill_all` pipelines the accounting of each chunk of 100 before moving it;
  - then the event is dispatched.
  - Accounting first means a crash between the two steps cannot strand the batch. The price is a
    rare race: a delete that loses to a concurrent move of the same entry (the scheduler enqueued
    it meanwhile) reports a death for a job that then runs, and its success repairs the counts.
  - `Queue::clear` and `JobSet::clear` do not read every job, so they do not account; the docs
    and the dashboard confirmations say so.
- **Reliable fetch**: recovery, reconciliation and requeues are not outcomes. Duplicates are
  counted once.
- **Basic fetch**: works the same, since accounting does not depend on the in-progress list. A job
  lost by basic fetch keeps its batch pending, and an operator can skip it.
- **Unique jobs** (`unique.md`), honouring its §2.18 contract as resolved by the cross-feature
  review:
  - members are pushed by `BATCH_COMMIT`, which takes their locks in the same script, prepending
    the unique design's frozen Lua library and using `hu_check` in its check phase (with a per-commit
    `seen` table, so two members with one digest are a duplicate of each other although no lock is
    written yet) and `hu_take` after each member's write (§2.5.3);
  - a duplicate under `OnConflict::Drop` is neither pushed nor registered, and is reported in
    `Committed::duplicates`; so is a member whose explicit jid already holds the lock (that job is
    in Redis outside the batch, and registering it would leave the batch waiting for it);
  - under `OnConflict::Error` the whole commit is refused before any write
    (`Error::DuplicateJob`), keeping the commit all or nothing;
  - `OnConflict::Replace` is refused when the job is pushed to the collecting client
    (`InvalidJob`), because replacing a holder that belongs to another batch would strand that
    batch;
  - locks are released by the unchanged outcome scripts, which run after the accounting; a skipped
    member is acknowledged with `ACK_UNIQUE`, which releases its lock;
  - callback templates are normalized with uniqueness removed (§2.3.1): the dispatch script takes no
    lock, and a template carrying a digest without a lock would count a lapse at its
    acknowledgement;
  - testing clients apply the same rules with the unique design's in-memory table (§2.11).
- **Rate limiting** (`ratelimit.md` §2.12 meets this):
  - an over-limit or throttled job is put back through the processor's push-back path
    (`Fetcher::push_back` in its `schedule` mode), never by server middleware halting the job and
    pushing a copy, which would count as a success and could let the batch succeed while the copy
    waits;
  - a reschedule is therefore not an outcome, and the member stays pending;
  - a job that runs out of reschedules fails like any failure and is counted as one;
  - the throttle gate runs before the endpoint's cancellation check. Adopted refinement (cross-feature
    review): when the gate **refuses** a member, the processor reads `cancelled_at` of its batch (one
    `HGET`, only on refusals) and, if the batch is cancelled, counts the member as skipped
    (`BATCH_SKIPPED cancel`) and acknowledges it instead of parking it, so members of a cancelled
    batch do not cycle through `schedule` for up to `max_delay`; server middleware does not see such
    a member (a throttled job never reaches middleware anyway);
  - concurrency limits need nothing.
- **Periodic jobs**: a periodic job may create a batch per tick in its `perform`. A periodic
  template carrying a `bid` (or `batch_callback`) adds nothing to that batch (registration only
  happens in commits), so `Periodic::validate` refuses it with a clear message. Because the periodic
  job can run twice (at-least-once), it should create its batch with
  `Batch::with_bid(ctx.client(), ctx.jid())?.commit_id(ctx.jid())`, which makes a re-run's commit a
  no-op (`already_applied`) instead of a second batch.
- **Iterable jobs**: §2.6.5. A batch of iterable jobs completes when each finished all its items.
- **Interrupted jobs** (`JobError::interrupted`): not outcomes.
- **Sharding** (`Client::with_redis`): a batch and its members live in the Redis of the client
  that committed them; a reopen must use a client on the same Redis. Commits never span pools.
- **Middleware**: client middleware runs at definition time, for members and for callback
  templates. Server middleware runs for members, callbacks and skipped members. A middleware
  that changes `bid` changes nothing: membership comes from the job as fetched.
- **Death handlers and error handlers**: unchanged. They run for members as for any job.
  Skipped members report nothing.
- **Encrypted arguments** (a custom middleware convention): stored members and templates are
  opaque to the scripts.

### 2.10 API (`hench::api`, new `api/batch.rs`)

```rust
impl Api { pub fn batches(&self) -> Batches; }

/// Batches in Redis (the `batches` index).
#[derive(Clone)]
pub struct Batches { /* api */ }

impl Batches {
    pub fn new(api: impl Into<Api>) -> Self;
    /// Entries of the index (includes expired ones until trimmed: an upper bound).
    pub async fn size(&self) -> Result<u64>;
    /// One page of batch summaries, most recently active first (index score descending),
    /// filtered in process like the dashboard's job filters (at most 50 000 index entries read,
    /// 500 matches kept; `Page::total` says what was searched).
    pub async fn page(&self, filter: &BatchFilter, page: usize, size: usize) -> Result<Page<BatchSummary>>;
    /// The full status, or `None` (never existed, expired, deleted).
    pub async fn fetch(&self, bid: &str) -> Result<Option<BatchStatus>>;
    /// Failed members, most recent failure first (HSCAN of `:failed`, at most 1 000 read).
    pub async fn failures(&self, bid: &str, page: usize, size: usize) -> Result<Page<FailedMember>>;
    /// Up to `limit` pending jids (SSCAN), for finding stuck members.
    pub async fn pending_jids(&self, bid: &str, limit: usize) -> Result<Vec<String>>;
    /// Child batches (at most 1 000, SMEMBERS + one HMGET each, pipelined).
    pub async fn children(&self, bid: &str) -> Result<Vec<BatchSummary>>;
    /// Cancels the batch and its descendants (§2.2.6); returns how many were newly cancelled.
    pub async fn cancel(&self, bid: &str) -> Result<usize>;
    /// Deletes the batch's data and its descendants'; refuses an unfinished child of a live
    /// parent unless `force` (that parent could then never finish).
    pub async fn delete(&self, bid: &str, force: bool) -> Result<bool>;
    /// Skips a pending member (operator decision: it no longer blocks success).
    pub async fn skip(&self, bid: &str, jid: &str) -> Result<bool>;
    /// Polls (default every second) until `complete` fired or `timeout` passed (Sidekiq's
    /// `join`); `Err(Error::Batch(NotFound))` if the batch disappears.
    pub async fn wait(&self, bid: &str, timeout: Duration) -> Result<BatchStatus>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum BatchFilter { All, Active, Dead, Complete, Succeeded, Cancelled, Text(String) }

/// A batch's state for display: the first that applies of Succeeded, Cancelled, Dead (died
/// and not succeeded), Complete (complete fired), Pending.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum BatchState { Pending, Complete, Dead, Cancelled, Succeeded }

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct BatchStatus {
    pub bid: String,
    pub description: Option<String>,
    pub parent: Option<String>,
    pub created_at: SystemTime,
    pub expires_at: Option<SystemTime>,           // from PTTL
    pub total: u64,
    pub pending: u64,
    pub failures: u64,
    pub dead: u64,
    pub skipped: u64,
    pub children: u64,
    pub children_complete: u64,
    pub children_succeeded: u64,
    pub complete_at: Option<SystemTime>,
    pub success_at: Option<SystemTime>,
    pub death_at: Option<SystemTime>,
    pub cancelled_at: Option<SystemTime>,
    pub finished_at: Option<SystemTime>,
    pub callbacks: Vec<CallbackInfo>,               // event, class, queue, jid, display args
    pub undispatched: bool,                         // in the outbox
}
impl BatchStatus {
    pub fn state(&self) -> BatchState;
    pub fn succeeded(&self) -> u64;                 // total − pending − skipped
    pub fn is_complete(&self) -> bool;
    pub fn is_success(&self) -> bool;
    pub fn duration(&self) -> Option<Duration>;     // success_at − created_at
}
// BatchSummary: the hash fields and counts (no callbacks); FailedMember: jid + summary fields.
```

- `fetch` is one pipeline: `HGETALL hash`, `SCARD`/`HLEN`/`SCARD`, `PTTL`, `ZSCORE` outbox.
  `page` reads index entries 1 000 at a time and one pipelined `HMGET` + counts per page.
- Reading is lenient (unknown fields ignored); a `v` above 1 gives
  `Err(Error::Batch(BatchError::…))` with "written by a newer hench".
- `JobRecord::batch_callback()` is added next to the existing `bid()`.

### 2.11 Testing modes (`testing.rs`, `batch/model.rs`)

- `batch/model.rs` is the batch state machine in Rust, with the same transitions as the scripts:
  commit, succeeded, failed, skipped, dispatch, cancel, evaluate, delete, skip. It is pure (no
  I/O), with the time passed in.
- Fake and inline clients keep a `BatchTable` (a `Mutex<HashMap<bid, model::Batch>>`) shared by a
  client's clones and its collectors.
- **Fake mode.**
  - A commit applies the model's commit to the table and captures the members like pushes.
  - `drain::<J>()`, `drain_all()` and `perform_one` run a job through the workers' execution path
    (`execute_job`), then apply the outcome:
    - success, or a halting middleware → succeeded;
    - failure → dead (testing clients have no retries, as today, so the first failure stops the
      drain and is returned);
    - `batch_cancelled` → skipped.
  - Events are dispatched in memory, and the callbacks are captured as jobs, so `drain_all` runs
    them too, in hench's order: a child's callbacks are members of its parent.
- **Inline mode.**
  - A commit first applies the model's commit (registration before execution), then runs every
    member in order, then runs the callbacks dispatched meanwhile, inline.
  - A failing member is counted dead, its death callbacks run, and the commit returns the
    member's error: inline pushes stop at the first failure, as today.
  - Nested batches run inline (Sidekiq Pro #6057).
- `Client::batch_status(bid)` reads the table.
- `JobContext::is_batch_cancelled`, `check_batch` and the endpoint check use the table.
- `Batches` (`api`) is Redis-only, as the rest of `api`.
- **With the other features** (cross-feature review; the testing modes hold one table per feature,
  shared by a client, its clones and its collectors):
  - a commit asks the unique design's `UniqueTable` for every member with a digest, with the rules of
    `BATCH_COMMIT` (duplicates and already-held jids left out and listed in `Committed::duplicates`;
    `Error` refuses the whole commit; `Replace` refused at push time); when a member's outcome is
    applied, the batch table is updated before the unique table releases the lock, the processor's
    order;
  - with the rate-limiting design's `TestLimits::InMemory`, a member refused by a throttle or a
    limiter stays captured with `at` and `limited` and stays pending in the batch (not an outcome),
    except that a refused member of a cancelled batch is skipped, as in workers; with the default
    `TestLimits::Unlimited` nothing is ever refused;
  - an inline client runs a job once, so an over-limit refusal there (from `OverLimit::new` or a
    classifier) is returned to the pusher as a failure and the member counts as dead (reason
    `no_retry`): nothing would ever run it again;
  - the internal job `hench::WorkflowNext` is registered in every testing registry, including an
    explicit `Testing::registry(r)` and `auto_register(false)`.
- **Defaults across the features**: push-time, deterministic behaviour (batch accounting, unique locks)
  is enforced by testing clients by default; time-driven behaviour (rate limits, periodic polling) is
  not.
- **Parity**: a proptest (`tests/batch_model.rs`) generates random operation sequences (commits
  with nesting, outcomes in random order with repeats, cancels, skips, dispatches in random order)
  and applies them both to `model.rs` and to a `TestRedis` through the real scripts. It then
  compares counts, event times (as present or absent), outbox contents, pushed callbacks and
  `finished_at` after every step.

### 2.12 Dashboard (`hench-web`)

- **Navigation**: a "Batches" tab, shown by default, since its absence would be confusing once
  batches exist and an empty list explains itself. Order across the features (cross-feature
  review): Dashboard, Busy, Queues, Retries, Scheduled, Dead, Periodic, Batches, Limits, Locks,
  Metrics, then custom tabs. Like the other feature tabs it has a switch, `HenchWeb::batches(bool)`
  (default `true`; off, the routes answer 404, as with `metrics(false)`).
- **`GET /batches`**: the list.
  - Query: `?state=active|dead|complete|succeeded|cancelled|all` (default `active`: not
    succeeded, not finished; others → 400), `?substr=` (bid or description, the same bounded
    literal filter as job lists), `?page=`, `?count=`.
  - Columns:
    - bid (link), description, created (relative time);
    - a progress bar: succeeded (`--cat-1`), failed and retrying (`--cat-2`), dead (a third
      categorical slot), skipped (hatched), pending (neutral track), with text counts next to it
      and an accessible label, so that colour is never the only cue (Pro #3387);
    - total, pending, failures, dead, children;
    - state chips (`complete`, `dead`, `cancelled`, `succeeded`, `dispatch pending`);
    - expires (relative).
  - The "Live" switch works as on the other list pages.
  - The summary line says what was searched, like the job filters.
- **`GET /batches/{bid}`**: the detail page.
  - Breadcrumb with the parent link.
  - Facts: description, created, expires, total, succeeded, pending, failures, dead, skipped,
    children (complete/succeeded), the event times with their dispatch times, cancelled and
    finished times, and the duration.
  - Progress bar.
  - Callbacks table: event, class, queue, display arguments (`JobRecord::display_args` rules,
    which hide encrypted ones), jid (a link to the job when it can be found in a set or queue).
  - Failures table (paged, 25): jid (links to `/retries/{key}` or `/morgue/{key}`, located from
    the failure summary: a retry at its `next_at` score, a job killed or sent to the morgue in
    the minute after its death; each lookup reads ten entries at most, all in two pipelined
    round trips per page, never a scan of the set. A job not found there, which may be running
    again or have moved, links to the list filtered by its jid, `?substr=`, a bounded search
    run on click; a job dropped, discarded or deleted has no link), class, error class,
    message (escaped), retry count, failed at, state (retrying next at … / dead: reason).
    Round 3 (web-api-security-1) replaced the `find_job` lookups (an unbounded `ZSCAN MATCH`
    of the whole set per failure, on every refresh of this Live page).
  - Children table (paged), and pending jids (first 25, each with a "find" link that searches the
    queues and sets by jid).
  - "Live" refresh.
  - `404` page for unknown or expired batches ("this batch does not exist or its data has
    expired").
- **`POST /batches/{bid}`** (CSRF, read-only rules as for every action), with `action`:
  - `cancel`, confirmed with "Cancel batch … and its N child batches? Jobs not yet run will be
    skipped";
  - `delete`, refused for an unfinished child with a notice naming the parent; `force` is not
    offered in the UI;
  - `skip` with `jid`, from the dead row or pending list, confirmed with "Skip this job? The batch
    can then succeed without it";
  - `dispatch`, offered while "dispatch pending" is shown for longer than the grace period, to
    retry the dispatch now.
  - Notices use the existing flash format (`kind.count.failed.subject`) with new kinds
    `BatchCancelled`, `BatchDeleted`, `MemberSkipped`, `CallbacksDispatched` (subject: the bid).
- **Job pages** (retries, scheduled, morgue, busy, queue entries): the existing `bid` row links to
  `/batches/{bid}`; callback jobs show "Callback (success) of batch …". Delete and kill
  confirmations on members add "This job belongs to batch …; it will count as dead there." The
  feature rows of job pages come after the built-in rows and the Iteration row, in one order for
  every feature: Batch, Periodic, Unique, Throttle, Limited, then the host's `job_info` rows.
- **Busy page and processes**: a process whose heartbeat `info` lacks `"batches"` in its
  `"features"` map gets a "no batch support" chip when any batch exists (R6). The chip is the
  batches case of a general one (cross-feature review): "lacks: unique, batches, ..." for every
  feature in use that a live process does not declare.
- **Security**: everything goes through askama escaping (L13). Descriptions and messages are
  bounded and cut in lists (120 characters, expandable).

### 2.13 Server (`hench-server`)

- **JSON API** (`--api`):
  - `GET /batches?state=&substr=&page=` → `[{bid, description, state, total, pending, failures,
    dead, skipped, children, created_at, complete_at, success_at, death_at, cancelled_at,
    expires_at}]`;
  - `GET /batches/{bid}` → the same plus `parent`, `children_complete`, `children_succeeded`,
    `callbacks`, `failures` (first 100), `undispatched`, `finished_at`; this is the polling
    endpoint Sidekiq Pro offers as Rack middleware;
  - `POST /batches/{bid}/cancel` → `{cancelled: n}`; read-only and cross-site rules as for
    `POST /jobs`.
  - Times are epoch seconds (float), like the rest of the JSON API.
- **CLI** (one command with subcommands, like `hench periodic`, `hench locks` and `hench limits`;
  cross-feature review):
  - `hench batches [list] [--state S] [--substr TEXT] [--json]`: the table
    `BID STATE TOTAL PENDING FAILED DEAD CREATED DESCRIPTION`, with control characters spelled
    out as elsewhere;
  - `hench batches show <bid> [--json]` (status, callbacks, first failures);
  - `hench batches cancel <bid>`, `hench batches delete <bid> [--force]`,
    `hench batches skip <bid> <jid>`;
  - `hench stats batches` (active, dead, undispatched, the last hour's counters), also printed by
    `hench stats all` when the index is not empty; the overview stays as it is.

### 2.14 Configuration

Settings and defaults are in §2.3.4. `build()` checks the bounds before any I/O:

- `batch_ttl` from 1 hour to 365 days;
- `batch_linger` from 30 s to 30 days;
- `batch_max_commit_jobs` from 1 to 100 000;
- `batch_commit_timeout` from 1 s to 10 minutes;
- `batch_maintenance_interval` from 1 s to 10 minutes;
- `batch_dispatch_grace` from 1 s to 1 hour.

`TestRedis::builder()` uses a 100 ms maintenance interval and a 100 ms grace, so the tests do
not sleep.

### 2.15 Metrics and logging

- **Logs** (`tracing`, target `hench::batch`):
  - info: `batch committed` (bid, members, children, reopen yes/no, `already_applied`),
    `batch event fired` (bid, event), `batch callbacks dispatched` (bid, events, callbacks,
    parent), `batch cancelled` (bid, descendants), `job skipped: batch cancelled` (from the
    processor's job logger);
  - warn: dispatch failures, an invalid `bid` (once per bid), accounting giving up.
- **Counters** (optional, D10): `b|YYYYMMDD|H:MM` hashes with fields `created`, `complete`,
  `success`, `death`, `cancelled`, kept 8 hours. The key is computed by the client for the current
  minute (client clock, as `metrics_minute`) and declared. The scripts run `HINCRBY` under
  `pcall` as their last write, so it can never break them. The dashboard's Batches page shows the
  last hour's totals above the list.
- **Status times**: each batch keeps `created_at`, the event times and `finished_at`, so
  `BatchStatus::duration()` gives Sidekiq Pro's batch duration metric without another store.
- **Heartbeat `info`**: the crate-wide `"features"` map gains `"batches": 1` (the batch data format
  version this process understands), which advertises batch support (R6); the unique, periodic and
  rate-limiting designs add their entries to the same map.

### 2.16 Performance

Measured on redis-server 7.0.15 on a unix socket (Appendix A):

- **Commit**: 3.3 µs of Redis time per job, about 3 to 4 times a plain `LPUSH` push, because each
  job is also registered (`SADD`) and the script interprets its arguments. One round trip per
  commit, instead of one per 1 000 jobs for `push_bulk`.
- **Accounting**: one more round trip per member outcome, median 24 µs of Redis time. The endpoint
  cancel check adds one `HGET` per member execution, about 1 µs of Redis time plus a round trip.
- **Dispatch**: one `HMGET` and one script per event. Maintenance: one `SET NX` per process per
  30 s, and one outbox query and one trim per 30 s cluster-wide.
- **Non-members**: no change at all. The processor reads `bid` from the job it already parsed.
- **Memory**: §2.4 (72 bytes per pending member).
- **Bounded batch size**: a 10 000-job commit blocks Redis for about 34 ms. Applications that
  define larger batches use loader jobs (each reopen is one bounded commit), or phase 2.

### 2.17 Simple workflows (`hench::batch::Workflow`)

Stages of parallel jobs, each starting when the previous stage succeeded. This covers chains,
fan-out/fan-in and Sidekiq Pro's "really complex workflow" without hand-written callbacks.

```rust
use hench::batch::Workflow;

let started = Workflow::new(hench.client())
    .description(format!("Fulfil order {order_id}"))
    .stage(|s| s.job::<reserve_stock>((order_id,)))
    .stage(|s| s.job::<charge_card>((order_id,)).job::<print_label>((order_id,)))
    .stage(|s| s.job::<ship>((order_id,)))
    .on_success::<order_shipped>((order_id,))
    .on_death::<order_stuck>((order_id,))
    .start()
    .await?;          // Committed: bid of the overall batch; stage 1 is running
```

```rust
#[must_use]
pub struct Workflow { /* the overall batch W (a Batch), stages */ }
impl Workflow {
    pub fn new(client: &Client) -> Self;                       // a child workflow on a collecting client
    pub fn description(self, text: impl Into<String>) -> Self;
    pub fn stage(self, build: impl FnOnce(Stage) -> Stage) -> Self;
    pub fn on_complete<J: Job>(self, args: J::Args) -> Self;  // also on_success, on_death, on(..)
    pub fn callback_queue(self, queue: impl Into<String>) -> Self;
    pub fn expires_in(self, ttl: Duration) -> Self;
    pub fn linger(self, linger: Duration) -> Self;
    pub async fn start(self) -> Result<Committed>;
}
#[must_use]
pub struct Stage { /* job templates, description */ }
impl Stage {
    pub fn job<J: Job>(self, args: J::Args) -> Self;
    pub fn job_with<J: Job>(self, setter: JobSetter<J>, args: J::Args) -> Self;
    pub fn description(self, text: impl Into<String>) -> Self;
}
```

Mechanism, on top of §2.2.5:

1. **Start.**
   - Validate: 1 to 100 stages, 1 to 1 000 jobs per stage, every stage within one commit (its
     jobs plus its batch's weight of 10 at most `batch_max_commit_jobs`, else
     `BatchError::StageTooLarge`; release review), at most 1 MiB of templates; setters with
     `at`/`delay` are refused. Larger stages contain a loader job, which adds to its stage batch
     through `ctx.batch()`.
   - Normalize every job: class options, default options, arguments checked, no jid.
   - Commit, in one `BATCH_COMMIT`:
     - the overall batch `W` with the user's callbacks and the field `wf`, a JSON array of the
       templates of stages 2 to N;
     - its child `S1`, with stage 1's jobs as members, run through client middleware now, and
       `on_success` = the internal job `hench::WorkflowNext` with arguments `[W, 2]`. The last
       stage gets no such callback.
   - The commit takes the extra field as generic `field, value` pairs for new batches, an ARGV
     extension of §2.5.3.
2. **Stage k succeeds.** `S_k`'s dispatch pushes `WorkflowNext(W, k + 1)` as a **member of W**
   (it is a child's callback). `W.ks` grows, but `W` keeps a pending member, so it cannot succeed.
3. **`WorkflowNext(W, n)` runs.**
   - It reads `HGET batch:W wf`.
   - It reopens `W` with commit id `wf-<n>`, so a re-run of the same `WorkflowNext` adds nothing
     (verified, scenario S17).
   - It defines child `S_n` with fresh jids (client middleware runs now, in the worker) and the
     next `WorkflowNext`, and commits.
   - If `W` was cancelled, the endpoint skips `WorkflowNext` and no further stage starts.
4. **The last stage succeeds.** Its dispatch makes `ks == kids` with nothing pending in `W`, so
   `W` fires `success`, and the user's callback runs.
5. **Failures.** A member that dies makes its stage batch die, and `W` with it (`on_death`). After
   the member is revived or skipped and the stage succeeds, the workflow continues.

The dashboard shows `W` with its stages as children ("stage 2 of 3"). The internal job is
registered in every registry, workers' and testing clients' alike, under the reserved name
`hench::WorkflowNext`. Its queue is the workflow's `callback_queue`, else `default`. (Round 3:
else the queue of the first job of the stage that just succeeded, a queue some worker polls;
`default` stalled every workflow of apps that name their queues. `W` is also held open, its field
`open = 1`, until the commit of its last stage writes `open = 0`, or a cancel removes it: before,
a stage member's first failure completed every defined stage and fired `W`'s `complete` before
the later stages existed. `Workflow::with_bid` gives `W` a chosen bid, so that a workflow is
started once from a job that may run twice.)

### 2.18 Cross-feature contracts

Resolved by the cross-feature review (`/home/user/scratch/design/pro-features.md`); the other designs
state the same rules.

- **Unique jobs**: members' locks are taken in `BATCH_COMMIT` from the unique design's frozen Lua
  library (prepended): `hu_check` in the check phase with a per-commit `seen` table, `hu_take` after
  each member's write, `hu_count`/`hu_trim` last (§2.5.3). Duplicates, and members whose explicit
  jid already holds the lock, are excluded under `drop`; under `error` the commit is refused;
  `replace` is refused at push time (§2.7, §2.9). Callback templates carry no `unique` field. The
  library file and its interface are created in the common phase of the implementation, so this
  design can be built in parallel with the unique design.
- **Rate limiting**: over-limit and throttled jobs go back through push back (a non-outcome path),
  never "halt and push a copy" (§2.9). `ratelimit.md` does this, with its own `Outcome::OverLimit`
  and `Outcome::Throttled` for metrics and a reschedule through `Fetcher::push_back`. A refused
  member of a cancelled batch is skipped instead of parked (§2.6.1, §2.9).
- **Periodic jobs**: templates with a `bid` or `batch_callback` are refused by `Periodic::validate`
  (§2.9); a periodic job defining one batch per tick uses `Batch::with_bid` with its jid (§2.3.1).
- **Outcome ordering** (fixed for all features): throttle gate; endpoint (cancellation check, then
  the unique `Start` release, then `perform`); batch accounting; the outcome script (with the unique
  lock work inside); permit release, dispatch, `RECORD_RUN`.
- **Metrics**: skipped members are `Outcome::Skipped` (`s`), next to the rate limiter's `o` and `t`.
- **Key prefixes**: §2.4; per-minute counters follow the shared convention (`keys::batch_minute`).
- **Testing**: every feature's in-memory model coexists in the fake and inline stores (batch table,
  unique table, limit table), with the interplay rules of §2.11.
- **Heartbeat and dashboard**: the `"features"` map, the navigation order and the job-row order are
  shared (§2.12, §2.15).

### 2.19 Documentation to update with the implementation

- **`docs/DESIGN.md`**:
  - §2: the keys table (seven rows) and the payload table (`bid` semantics, `batch_callback`);
  - §3: the `JobContext` methods, `JobError::BATCH_CANCELLED`;
  - §4: `ClientMode::Collect`, `Client::batch`, `batch_status`, batch defaults;
  - §5: accounting before the outcome write, the endpoint check, the maintenance loop, the new
    error contexts;
  - §7: `Batches`, plus API delete and kill accounting;
  - §8: the batch table in the testing modes;
  - §9: dashboard routes and pages, JSON endpoints, CLI;
  - a new §9i "Batches and workflows", this design condensed (the cross-feature review numbers §9g
    unique, §9h periodic, §9i batches, §9j rate limiting, §9k the features together);
  - §9a: module ownership; §10: test plan rows.
- **`README.md`**: a "Batches" section covering:
  - the events, with an example;
  - nesting and the ordering guarantee;
  - reopening and loader jobs with `commit_id`;
  - cancellation;
  - workflows;
  - sizing (bytes per pending member, the commit cap);
  - the rollout rule (workers before batches);
  - `noeviction`;
  - what deleting or clearing does.
- **`docs/STATUS.md`**: the feature lines, and batches removed from the out-of-scope list.

### 2.20 Implementation plan

Each step lands with its tests.

1. **Spine**: `keys` helpers, the payload field, `BatchError` and `Error::Batch`, the `JobError`
   class, config fields and builder methods, `BatchDefaults`, `valid_bid`/`new_bid`.
2. **Scripts and model**: `batch/lua/*.lua` (from the prototypes), `model.rs`, the parity proptest
   against `TestRedis`, and the prototype scenarios (S1 to S17) ported to `tests/batch_scripts.rs`.
3. **Definition**: the collector (`ClientMode::Collect`), `Batch`, callback templates, the commit
   with resend, `Committed`; client tests.
4. **Engine**: processor accounting, the endpoint check, `dispatch_chain`, the maintenance loop,
   heartbeat `"batches"`; engine and chaos tests.
5. **API**: `Batches`, plus the delete and kill accounting; API tests.
6. **Testing modes**: the batch table in fake and inline clients; testing tests.
7. **Workflows**: `Workflow`, `Stage`, `hench::WorkflowNext`.
8. **Dashboard**: pages and actions; web tests, and a browser test of the progress bar and the
   live refresh.
9. **Server**: the JSON endpoints and the CLI.
10. **Docs and gate**: docs (§2.19), STATUS, a soak scenario (`batches`: random failures, kills
    and restarts, checking each batch's events and callbacks against the execution log), full
    gate.

Placement in the combined plan (cross-feature review, `pro-features.md` §6): step 1, the hook points
of step 4 in `processor.rs`, `middleware.rs`, `launcher.rs` and `api/` (delete and kill), the
`ClientMode::Collect` arm of `raw_push`, the testing-tables container and the registration of
`hench::WorkflowNext` are done once in the common phase by the integrator; lane B ("batches and rate
limiting") then owns `batch/`, `api/batch.rs`, the Batches pages and the `hench batches` commands.

Later phases (decisions D5, D12, D15):

- **Phase 2: staged commits for definitions above `batch_max_commit_jobs`.**
  - Bounded client memory, bounded script time, and the definition still becomes visible all at
    once.
  - **Staging.** While the closure runs, the collector writes each full chunk of 1 000 jobs to
    `batch:<bid>:stage:<cid>`, a LIST of length-prefixed `jid|destination|kind|json` entries,
    with a one-day expiry. Nothing else is written, so a failing closure leaves only garbage that
    expires.
  - **Decision.** The closure's success runs `BATCH_COMMIT` with the staged key: it writes the
    batch rows, increments `open` on the root (events wait while `open > 0`), and records the
    commit in `batch-publishing` (a ZSET scored by a 60 s lease).
  - **Publishing.** `BATCH_PUBLISH` scripts pop up to 1 000 staged entries, push and register
    them, and refresh the lease. The last one (stage empty) decrements `open`, removes the
    `batch-publishing` entry and evaluates.
  - **Roll forward.** If the client dies while publishing, the maintenance loop resumes commits
    whose lease ended. Publishing is idempotent because entries are popped and registered in the
    same script.
  - **Visibility.** Members may start running while later chunks are published. Callbacks cannot
    fire early, because `open` blocks them. This is the Faktory commit idea combined with Sidekiq
    Pro's all-or-nothing definition.
- **Phase 3**:
  - the iterable checkpoint cancel check (§2.6.5);
  - an optional process-wide cache of cancelled bids, replacing the per-member `HGET` (D12);
  - batch tags searchable on Redis 7.4+ (D15).

---

## 3. Test plan

Every integration test uses `TestRedis` (a private `redis-server`). Workers come from
`TestRedis::builder()`, which uses a 100 ms maintenance interval and grace. IDs are referenced by
the implementation plan.

### 3.1 Unit tests (`batch/*.rs`, `keys.rs`, `payload.rs`)

| ID | Scenario |
|---|---|
| U1 | `valid_bid`: 24 lowercase hex accepted; uppercase, 23/25 chars, non-hex, empty refused; `new_bid` matches |
| U2 | key helpers produce the documented names |
| U3 | `batch_callback` round-trips; unknown event read leniently (field dropped, warning); in `RESERVED_KEYS` |
| U4 | callback templates: no `enqueued_at`; `bid` = parent for children, none at the root; `batch_callback` set; precedence `callback_queue` < setter options; `at`/`delay` refused; 17 callbacks per event refused; 64 KiB per event enforced |
| U5 | failure summary: message cut at 200 characters on a char boundary; reasons from every `Verdict`; `next_at` only for retries |
| U6 | `model.rs`: scenarios S1 to S17 of Appendix A in pure Rust; proptest invariants after random sequences (dead ⊆ failed ⊆ pending; each event at most once; success ⇒ complete fired; cancelled ⇒ never success; `succeeded = total − pending − skipped ≥ 0`) |
| U7 | dispatch's `enqueued_at` surgery: a template with i64::MIN/MAX and float arguments, extra keys and unicode parses back into the same `JobPayload` plus `enqueued_at` |

### 3.2 Scripts against Redis (`tests/batch_scripts.rs`)

| ID | Scenario |
|---|---|
| R1 | Prototype scenarios ported: basic success and dispatch (S1), failure then success with `complete` first (S2), death once and revival (S3), stale duplicates ignored (S4), commit idempotency (S5), reopen rules (S6), empty batch (S7), child callbacks as parent members (S8), cascade without callbacks (S9), death propagation (S10), cancellation (S11), `WRONGTYPE` before writes (S12), operator skip (S13), horizon expiry (S14), outbox recovery (S15), linger of a finished cancelled batch and late skips (S16), workflow pattern with an idempotent `wf-<n>` commit (S17), ancestors refreshed with a child (S18) |
| R2 | Parity proptest model ↔ scripts (§2.11), 256 cases of up to 60 operations |
| R3 | `OOM`: commit refused, nothing written; success under `OOM` completes and fires; failure under `OOM` refused, applied after memory is freed |
| R4 | Every script leaves no batch key without an expiry (`SCAN` `batch:*` + `PTTL` after each step); the index score equals the hash's expiry (± 1 s) (Pro #1057, #2889) |
| R5 | Dispatch refuses a wrongly typed callback queue before any write; the outbox entry stays |
| R6 | Delete: descendants first; a child of a live parent refused unless forced; members of a deleted batch are no-ops |

### 3.3 Definition and commit (`tests/batch.rs`)

| ID | Scenario | Source |
|---|---|---|
| D1 | closure returns `Err` → no key, no queue entry, error returned unchanged | Pro wiki (atomic `jobs`) |
| D2 | client middleware halting one push → that job neither pushed nor registered; `total` excludes it | Pro #2406 |
| D3 | client middleware sees `bid` on member pushes | Pro (bid set by client middleware) |
| D4 | `jobs` returns jids in push order; `perform_bulk` and `client.job::<J>()` through the collector | sidekiq-batch spec "jobs returns jids" |
| D5 | nested definitions → one commit; child `parent`, parent `kids` and `:kids`, `Committed::children` | sidekiq-batch flow spec (nested) |
| D6 | reopen: adds members; succeeded → `Succeeded`; cancelled → `Cancelled`; missing → `NotFound`; malformed → `InvalidBid` | Pro `Immutable` #4845; BullMQ #3264 |
| D7 | same `commit_id` twice → `already_applied`, nothing pushed | loader retries |
| D8 | proxy drops the reply of the first commit attempt → resent → one copy of every member | B2 |
| D9 | `CLIENT PAUSE` longer than `batch_commit_timeout` → `Unconfirmed`; commit again with the same id → applied exactly once | B2 |
| D10 | `batch_max_commit_jobs + 1` pushes → `TooLarge` from the push; nothing written | Pro #6967 |
| D11 | duplicate explicit jid → `DuplicateJid`; push after the commit → `Closed`; a payload whose `bid` names another batch → `InvalidJob` | |
| D12 | scheduled member: expiry ≥ run-at + ttl; batch completes only after it runs | Pro notes (scheduled jobs) |
| D13 | empty batch → callbacks pushed by the committing client; reopen with zero jobs → nothing | Pro 7.1, #5903, #6489 |
| D14 | unique: a duplicate member excluded (`duplicates`), lock held by the first; `OnConflict::Error` refuses the commit; `Replace` refused | sidekiq#3662; BullMQ dedup warning |

### 3.4 Engine, real workers (`tests/batch_engine.rs`)

| ID | Scenario | Source |
|---|---|---|
| E1 | N members succeed → `complete` then `success` callbacks run once each, status counts right, keys linger then expire (short linger) | Pro wiki; sidekiq-batch integration "keys cleaned" |
| E2 | a member fails once then succeeds → `complete` callback sees `failures > 0`; `success` after the retry | Pro wiki (complete vs success) |
| E3 | retries exhausted → morgue → `death` callback once; a second death fires nothing; `complete` fires; `success` never | Pro #3841, #5740 |
| E4 | the dead member retried from the morgue through the API → succeeds → `success` callback | Pro wiki (revival) |
| E5 | `retry: false` → death; `RetryIn::Discard` → death; `dead: false` → death | Pro wiki |
| E6 | server middleware not calling `next` → counted succeeded | |
| E7 | an iterable member interrupted (tiny `max_iteration_runtime`) → batch not complete until the last item | Pro #6889 |
| E8 | `stop()` while a member runs and returns `hench::Shutdown` → not counted; after restart counted once | Pro (Shutdown refactor) |
| E9 | hard shutdown interrupts a member → requeued → counted once | |
| E10 | cancel with members queued → `perform` never called, skipped count, `complete` callback with `cancelled_at`, no `success`; `ctx.check_batch()` stops a long member; `batch_cancelled()` from a member of a live batch → ordinary failure (retry) | Pro invalidation; sidekiq-batch invalidation specs |
| E11 | loader pattern: 20 loaders × 50 members via `ctx.batch()` + `commit_id`; one loader fails after its commit and is retried → no duplicate members; one `success` | Pro "Huge Batches" |
| E12 | parent and child with callbacks: recorded order shows child `complete` callback before parent `complete`, child callbacks succeeded before parent `success`; child death → parent `death` callback | Pro 2.0 notes; #4774 |
| E13 | Sidekiq Pro's order-fulfilment workflow written with callbacks reopening the parent (5 steps, parallel jobs inside steps) → steps in order, `shipped` once | Pro "Really complex workflows"; sidekiq-batch `workflow.rb` |
| E14 | `Workflow` API: 3 stages in order; a duplicate `WorkflowNext` execution adds nothing; a dead member → workflow `death`; skip → workflow continues | BullMQ chain/step tests |
| E15 | `callback_queue` and per-callback setter queues honoured | Pro #2200 |
| E16 | crash (`Launcher::abandon`, test-util) right after a member's accounting fired `success` and before dispatch → another process's maintenance dispatches within interval + grace; callbacks once | B4 |
| E17 | basic fetch: E1 and E2 | |
| E18 | skipped members count as processed, not failed; metrics `Success` | |
| E19 | deep nesting (4 levels, each level created by a job of the previous) → every level's callbacks once, outermost last | sidekiq-batch `nested.rb` |
| E20 | API delete of a queued member and kill of a retrying member → counted dead (`deleted`/`killed`), `death` callback, `complete` fires | L9 |
| E21 | members in a paused queue keep the batch pending; after `unpause` it completes; a paused callback queue holds the callbacks without blocking dispatch | BullMQ "not process parent until queue is unpaused" |

### 3.5 Concurrency and chaos (`tests/batch_chaos.rs`, soak harness)

| ID | Scenario |
|---|---|
| C1 | 4 processes × 8 workers, 10 000 members → each event fired once (`*_at` written once; callback execution counter = 1) (prototype: `concurrency.py`) |
| C2 | `abandon` a process mid-member (reliable fetch) → orphan recovery → member counted once; batch succeeds once |
| C3 | two copies of one member (same jid pushed twice) → counted once; the stale copy's failure ignored |
| C4 | a proxy dropping replies of accounting and dispatch scripts at random → counts exact, callbacks once |
| C5 | Redis restart with AOF mid-batch → batch completes; without persistence → batch and members gone, no error loops |
| C6 | `OOM` while members finish → successes counted; failures retried until memory frees; commits refused cleanly |
| C7 | reopen racing the last member's success, 1 000 iterations → each time either `Succeeded` or the new members registered and `success` waits; never both |
| C8 | cancel racing the last success → either `success` fired (cancel → `Succeeded`) or cancelled (no `success`) |
| C9 | processor dispatch and maintenance dispatch racing → callbacks pushed once |
| C10 | soak scenario `batches` (`examples/soak`, `ci/soak.sh --short` includes it): random failures, `kill -9`, SIGTERM, Redis restarts and reply-dropping proxy. Invariants from the execution log and Redis: per batch, `success_at` written once and success callbacks executed ≥ 1 (= 1 without crashes); `total = succeeded + pending + skipped`; no batch key without expiry; the outbox empty at the end; no member counted twice |

### 3.6 API, dashboard, server

| ID | Scenario |
|---|---|
| A1 | `Batches::page` per filter and text search; `fetch`; `failures` paging; `pending_jids`; `children`; `wait` (returns on complete, `NotFound` when deleted, timeout) |
| A2 | `cancel` cascade over 3 levels (count); cancel after success → `Succeeded`; `delete` root deletes descendants; child of a live parent refused, `force` works |
| A3 | `skip` of a dead member → `success` fires |
| W1 | `/batches`: renders, filters, 400 on a bad `state`, Live refresh, hostile description and error message escaped (XSS regression, L13), long text cut |
| W2 | `/batches/{bid}`: facts, accessible progress bar (label with the counts), failures linked to their retry or morgue entries, children, pending jids, 404 for an expired batch |
| W3 | actions `cancel`/`delete`/`skip`/`dispatch`: CSRF required, read-only refuses, cross-site refused, notices |
| W4 | job pages link members to their batch; callback jobs show their event; delete/kill confirmations mention the batch |
| W5 | browser test (Playwright): progress bar segments and labels in both themes, Live refresh keeps focus |
| S1 | JSON endpoints (`GET /batches`, `GET /batches/{bid}`, `POST /batches/{bid}/cancel`); CLI commands (tables, `--json`, control characters spelled out); `hench stats` batch line |

### 3.7 Testing modes (`tests/testing.rs` additions)

| ID | Scenario | Source |
|---|---|---|
| T1 | fake: commit captures members with `bid`; `drain_all` runs members then callbacks; child callbacks drained as parent members; cancel → drained members skipped | Pro testing notes |
| T2 | inline: members run at commit, callbacks after; nested batches inline; a failing member → `death` callback runs, the commit returns the error | Pro #6057; Pro testing example |
| T3 | `Client::batch_status` in every mode; `ctx.is_batch_cancelled` in testing modes | |

### 3.8 Ported scenarios (source → hench test)

| Source | Hench tests |
|---|---|
| Sidekiq Pro wiki: Batches (atomic jobs, reopen, status, callbacks, death, empty, linger, cancel, huge batches, deleting a job, testing) | D1, D6, D13, E1 to E5, E10, E11, E20, T1, T2, R1 |
| Sidekiq Pro wiki: complex and really complex workflows | E12, E13, E14 |
| `Pro-Changes.md` | #2406 → D2; #2714 → D1; #2889 → R1 (S18), R4; #1057 → R4; #3011 and #3519 → E1, A1; #3841 and #5740 → E3; #4774 → E12; #4845 → D6; #5524 and #5659 → C3, U6; #6489 → D13; #6852 → C9; #6889 → E7; #6967 → D10; Shutdown refactor → E8 |
| sidekiq-batch specs (`batch_spec`, `flow_spec`, `status_spec`, `callback_spec`, integration `simple`/`nested`/`workflow`) | U1, D4, D5, E10, E19, E13, A1, R1 |
| BullMQ flow tests | children before parent → E1; empty children → D13; chains → E14; child delays → D12; paused queue → E21; failure options → E3 to E5; continually adding → E11, C7; re-adding a completed child → D7; removal → E20; retry re-attaches → E4; missing parent → D6 |
| Faktory Ent (commit gating, reopen, nested, callback retries) | D6, D5, E3 |

---

## 4. Decisions for the owner (recommendation first)

| # | Decision | Recommendation | Alternatives and trade-offs |
|---|---|---|---|
| D1 | What deleting or killing a member through the API means for its batch | **Count it as dead** (reason `deleted`/`killed`): `death` fires, `complete` can fire, the batch cannot succeed unless an operator skips the member | Sidekiq Pro: nothing, so the batch waits until it expires (silent and stuck). BullMQ: the dependency is removed, so the parent proceeds (a silent false success). |
| D2 | Members discarded or with `retry: false` | **Dead** (Sidekiq Pro semantics: never claim success for work that did not succeed) | Count them as succeeded: friendlier for "best effort" members, but `success` would then lie. Such members can use their own batch. |
| D3 | Server middleware that does not call `next` | **Succeeded** (the job is acknowledged as done) | A separate "halted" count: more state, for little value. Rate limiting must not use halting (§2.18). |
| D4 | Callbacks of cancelled batches | **`complete` fires under its usual rule, `success` never**, and the status shows `cancelled_at` | No callbacks at all (cleanup code cannot react); or a dedicated `cancelled` event (a fourth event to explain; could be added later without breaking anything). |
| D5 | Size of one definition | **Single-script commits capped at 10 000 jobs** (34 ms of Redis time), loader jobs beyond, staged commits (§2.20 phase 2) only when needed | A lower cap (1 000 jobs, 3.4 ms, like `push_bulk`'s chunk) hurts common mid-size batches. Implementing phase 2 now gives atomic huge batches with bounded memory at the cost of a publishing roll-forward process. |
| D6 | Where callbacks are queued | **The callback job's own queue, overridden by `callback_queue`, then by the callback's setter** (predictable, set at definition) | Sidekiq Pro: the queue of the batch's last job to finish (not known at definition time; surprising). |
| D7 | When client middleware runs for callbacks | **At definition time** (the dispatch script pushes stored templates atomically) | At fire time: middleware sees the worker's context, but the push could no longer be atomic with the dispatch, so a crash would duplicate or lose callbacks. |
| D8 | Who may reopen a batch | **Anyone** (`Batch::open`); the commit refuses succeeded and cancelled batches atomically, so reopening is safe | Restrict reopening to member jobs, as Sidekiq Pro advises (hench needs no such rule; the docs explain that `complete` may already have fired). |
| D9 | Workflow layer in v1 | **Yes**: small (one type, one internal job), built on batches, and it covers chains and fan-out/fan-in | Leave workflows to hand-written callbacks (E13 shows how), or later add a DAG layer (dependencies between arbitrary jobs), a much larger feature. |
| D10 | Per-minute batch counters `b\|…` | **Yes, but last in the plan**: cheap (`pcall` `HINCRBY`) and they feed the Batches page's summary | Logs and status times only. |
| D11 | Memory per pending member (72 B with jid strings) | **Accept**, and document sizing | A per-batch bitmap indexed by member number (1 bit per member): about 500 times smaller, but pending jids could no longer be listed and every payload would need an index field; or binary jids (−25 %, only for generated jids). |
| D12 | Cost of the cancellation check | **One `HGET` per member execution**, always on | A process-wide cache of cancelled bids, refreshed every few seconds (no per-job round trip, but a cancel takes effect after the refresh); or an opt-out per batch. |
| D13 | Defaults | **30 days after the last activity, 24 h linger** (Sidekiq Pro's), linger at least 30 s | Shorter defaults save memory for high-volume users; they can set `batch_ttl` and `batch_linger`. |
| D14 | Reserved internal job name | **`hench::WorkflowNext`** (the `hench::` prefix is already used for error classes) | Any other reserved prefix. |
| D15 | Batch tags and search | **Not in v1** (Sidekiq Pro needs Redis 7.4 for them; hench supports 6.2) | A tag index per tag (`batch-tag:<tag>` ZSET), deferred to phase 3. |
| D16 | Rollout safety (R6) | **Advertise `"batches": 1` in the heartbeat's `"features"` map (shared by the four features) and warn in the dashboard** | Make commits refuse while a live process lacks support (one `SSCAN` of `processes` per minute per client, cached): safer, but it couples clients to workers. |
| D17 | Idempotent creation of a batch by a job that may run twice (cross-feature review) | **`Batch::with_bid(client, bid)` + `commit_id`**, used with the job's own jid (a re-run's commit is `already_applied`; another commit id on an existing bid is `BatchError::Exists`) | No API: document that a periodic or retried job may create a second batch, or keep a jid → bid map in the application. |
| D18 | Metrics of skipped members (cross-feature review) | **`Outcome::Skipped`: processed, a new per-class field `s`, not timed** (like the rate limiter's `o` and `t`) | `Outcome::Success`: no new field, but near-zero durations distort the execution-time averages and histograms. |

## 5. Risks

| # | Risk | Mitigation |
|---|---|---|
| R1 | Memory of huge batches: 72 bytes per pending member, 72 MB per million | Documented sizing; the set shrinks as members succeed; D11 alternatives if needed. |
| R2 | Latency spikes from large commits (34 ms per 10 000 jobs) | Cap (D5), documented; loader jobs; phase 2 staged commits. |
| R3 | At-least-once surprises: a member may run again after its batch's `success` callback (crash between accounting and acknowledgement) | Documented prominently; jobs must be idempotent (already hench's rule). |
| R4 | Stuck batches: `clear`, `FLUSHDB`, eviction, expiry, members lost by basic fetch, a broken batch key | Dashboard visibility (pending jids, "find" links, dispatch-pending chip), operator skip, delete, start-up eviction warning, activity-based expiry. |
| R5 | Coupling with unique jobs: the commit script needs unique's per-job push logic as a Lua function | §2.18; the synthesis step must make the unique library expose `hu_push_one`; batches can ship first without unique support (refuse members with `unique_for` until then). |
| R6 | Rolling deploys: older workers ignore `bid` and never count, leaving batches pending | Rollout rule in the README; heartbeat `"features": {"batches": 1}` and a dashboard warning (D16). |
| R7 | Stale callback templates: middleware ran at definition time (an auth token in a custom key may expire before a 3-day batch finishes) | Documented (D7); middleware should not put short-lived secrets in payloads. |
| R8 | API learning curve: pushes through a collecting client return jids before anything is written; children commit with their root | Rustdoc examples and README; `#[must_use]` on `Batch`; `Closed` error for pushes after the commit. |
| R9 | Dragonfly and Valkey differences (`TYPE` reply shape in Lua, script flags) | Declared keys everywhere; a CI job against Valkey if one is added; the scripts use only basic commands. |
| R10 | Large `:failed` hashes when most members fail (300 bytes each) | Summaries bounded (200-character messages, no backtrace); a batch with 100 000 failures costs about 30 MB until it finishes. |
| R11 | Callbacks of top-level batches are not tracked (unlike children's) | Their status is visible like any job's (retries, morgue); the batch's `success_dt` shows the dispatch. |

---

## Appendix A. Prototype verification (redis-server 7.0.15, unix socket, 2026-11-01)

Files in `/home/user/scratch/design-lab/batches/`:

| File | Contents |
|---|---|
| `lib.lua` | helpers (§2.5.2) |
| `commit.lua`, `succeeded.lua`, `failed.lua`, `skipped.lua`, `dispatch.lua`, `touch.lua`, `cancel.lua` | the scripts |
| `bt.py` | stdlib RESP client and argument builders mirroring the Rust side |
| `scenarios.py` | 100 checks |
| `concurrency.py` | the concurrency check |
| `bench.py` | measurements |
| `notes.md` | research notes |

Run with `redis-server --port 0 --unixsocket <dir>/redis.sock --save "" --appendonly no`, then
`python3 scenarios.py`.

Scenarios, all passing:

| ID | Scenario |
|---|---|
| S1 | basic success, dispatch, idempotent repeat, linger |
| S2 | failure, `complete` with a retrying member, then `success` |
| S3 | `death` once, `complete` with dead members, revival to `success` |
| S4 | stale duplicates ignored |
| S5 | commit idempotency |
| S6 | reopen rules (`SUCCEEDED`, `NOBATCH`, zero-job reopen fires nothing) |
| S7 | empty batch fires at commit |
| S8 | child callbacks registered as parent members, parent waits for them |
| S9 | child without callbacks finishes the parent at dispatch |
| S10 | death propagation |
| S11 | cancellation (skip refused unless cancelled, `complete` without `success`, reopen refused) |
| S12 | `WRONGTYPE` before any write |
| S13 | operator skip lets a dead batch succeed |
| S14 | expiry covers a member scheduled 7 days ahead |
| S15 | outbox recovery |
| S16 | linger of a finished cancelled batch is kept by later skips |
| S17 | workflow stages with `Next` callbacks and an idempotent `wf-1` commit |
| S18 | a child's hourly refresh extends every key of its parent and grandparent; a second refresh within the hour is skipped |

Concurrency: 8 processes completed the 4 000 members of one batch, every 7th member reported
twice, each dispatching whenever told. Events fired: exactly `complete` and `success`; callbacks
pushed: 2 of 2; outbox empty.

Measurements:

| What | Result |
|---|---|
| Commit, Redis time per job (about 250-byte jobs) | 1 000 jobs ≈ 3.4 ms (below the slowlog threshold; 3.3 µs per job extrapolated), 5 000: 16.5 ms, 10 000: 33.8 ms, 50 000: 250 ms |
| Commit round trip from Python (includes argument transfer) | 1 000: 11 ms, 10 000: 77 ms |
| Pending set | 64 to 73 bytes per jid (hashtable) at 100 to 100 000 members |
| Batch hash without callbacks | 200 bytes |
| `BATCH_SUCCEEDED` (20 000 sequential calls) | median 24 µs, maximum 2 ms of Redis time; 104 µs round trip in Python |
| `OOM` (`maxmemory` below usage, `noeviction`) | commit refused with nothing written; three successes counted and `complete` plus `success` fired; a failure report refused with nothing written, then applied once `maxmemory` was lifted |
