# Differences from Sidekiq

Hench is a Rust system with Sidekiq's model, not a Sidekiq client: it does not run Ruby jobs
or share queues with Ruby processes. The Redis layout mirrors Sidekiq's so that the concepts
and tools carry over. Notable differences:

- Jobs are typed; arguments are serde types, checked on push (no `NaN`, no non-array args).
- Reliable fetch (Sidekiq Pro's `super_fetch`) is the default, with orphan recovery.
- Queue pausing (a Sidekiq Pro feature) is built in, and so are the Pro and Enterprise
  features unique jobs, periodic jobs, batches and rate limiting. Their Redis work happens in
  the scripts that push, fetch and settle the job (a unique job's lock is taken with the push
  and released with the outcome write, atomically), not in middleware around them. Unique
  locks of failed jobs are held through their retries; periodic jobs are fired by every worker
  without a leader, each tick at most once; throttled jobs wait in the schedule for their slot
  in the limit's wait line, without using a retry.
- Outcome writes are retried through Redis outages; the scheduler leases due jobs instead of
  popping them, so a crash, a lost reply or a failing (even panicking) client middleware
  delays a job instead of losing it; API moves (retry, kill, add to queue) are atomic scripts.
- Middleware instances are shared by every job (Sidekiq instantiates them per job); chains
  hold one middleware per type.
- Panics are caught and handled as failures of class `panic` (with `panic = "unwind"`, the
  default).
- Testing modes are per client instead of a global switch.
- Metrics hash keys use a four-digit year; counts whose flush failed (metrics and `stat:*`
  counters) are sent again, and counted once even when an earlier attempt's reply was lost.
- The dashboard sorts the metrics table by any column, offers pause/resume, refuses
  cross-site POSTs by token *and* fetch metadata, and answers only the host names it knows
  (localhost and IP addresses unless told others); unknown queues and jobs are 404 pages.
- No ActiveJob, no Rails reloader, no `CurrentAttributes`, no profiling, no thread dumps:
  `SIGTTIN` and `api::Process::dump()` log each busy slot's job instead.

[`docs/DESIGN.md`][design] is the full contract (data model, semantics and every deviation);
[`docs/STATUS.md`][status] tracks what is done, and [`CHANGELOG.md`][changelog] what each
release changed. How the two compare when they run Sidekiq's own benchmark scripts on one
machine: [`benchmarks/`](../../benchmarks/README.md).

---

[← Deployment](deployment.md) · [Guide](README.md)

[design]: ../DESIGN.md
[status]: ../STATUS.md
[changelog]: ../../CHANGELOG.md
