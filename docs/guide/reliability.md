# Retries and delivery guarantees

## Retries, the dead set and scheduled jobs

A failed job is retried with exponential backoff: retry `n` (from 0) waits `n⁴ + 15` seconds
plus a random jitter below `10 × (n + 1)` seconds, so 25 retries span about 20 days. Each
failure records `error_class`, `error_message` (and the backtrace when asked) in the job,
visible in the dashboard.

- `retry: false` drops the job at its first failure (death handlers still run); `retry: n`
  caps the retries; `retry_for` retries for a duration instead of a count; `retry_queue`
  sends retries elsewhere.
- Out of retries, `retries_exhausted` runs, then the job goes to the **dead set** (unless
  `dead: false`), where it stays for 180 days or until 10,000 newer dead jobs push it out.
  Death handlers (`.death_handler(..)`) run for every job that will not be retried.
- Error handlers (`.error_handler(..)`) see every failure with its context, for error
  reporting services.
- **Scheduled jobs** and **retries** sit in sorted sets scored by their due time; every
  process's scheduler polls them at a random interval averaging `processes × 5 s`, so the
  whole cluster checks about every 5 seconds, and enqueues due jobs through the client
  (client middleware runs again).
- **Timing precision.** A scheduled job or retry therefore reaches its queue a few seconds
  after it is due on average (up to about 10 s), like in Sidekiq. For tighter timing lower
  `average_scheduled_poll_interval` (for example to 1 s): every process then polls more
  often, which costs Redis a few more cheap commands per second. Jobs parked by a throttle or
  a limiter do not wait for that poll: the process that parked them wakes its scheduler when
  they are due.
- From the API or the dashboard, retries and dead jobs can be retried now, deleted or
  killed, and scheduled jobs enqueued early; all of these moves are atomic.

## Delivery guarantees: reliable and basic fetch

Hench delivers every job **at least once**: make jobs idempotent (a job can run again after
a crash, a lost reply or a hard shutdown, and a push that failed with a timeout may have
enqueued it, see [Enqueuing jobs](jobs.md#enqueuing-jobs)).

| | Reliable fetch (default) | Basic fetch |
|---|---|---|
| Fetch | `LMOVE` from the queue into the process's `<identity>:inprogress` list | `RPOP` / `BRPOP` |
| Acknowledge | `LREM` from the in-progress list | nothing |
| Process crashes | its in-progress jobs are pushed back by another process once its heartbeat expires (60 s, checked every minute, and again after `orphan_grace`, 30 s: a live process re-creates a heartbeat Redis lost), about 1.5 to 2.5 minutes after the crash | the running jobs are lost |
| A job that crashes its process | recovered and run again up to `max_recoveries` times (3), alone after the first time, then moved to the morgue as a poison pill | the job is lost |
| Shutdown | unfinished jobs are requeued at the front of their queues | the same |
| Cost | one more list operation per job | the cheapest |

**Poison pills.** A job that kills the process running it (an out-of-memory kill, an abort, a
crash in foreign code) would otherwise be recovered and crash the next process, forever. Each
recovery from a dead process is counted per job (the key `recovered-<jid>`, kept 72 hours after
the last one); a job recovered more than `max_recoveries` times (3 by default; 0 never runs a job
again after its process died) goes to the morgue instead, with the error class
`hench::CrashLoop` and a message saying how often it was recovered and how many other jobs were
in flight at its last crash: its batch counts it dead, its unique lock is released, and the death
and error handlers of the process that found it run, once (a job with `dead: false` is dropped
instead). Fix the cause, then retry it from the morgue; a copy that still crashes its process
goes back there at its next recovery.

Every job running when a process dies is counted, the culprit and the jobs that merely ran
beside it. So that the next crash tells them apart, a job recovered for the first time goes back
to its queue at once (the usual case: a deploy, a lost machine), but a job recovered before — in
flight at an earlier crash too — comes back alone: such jobs wait in the `recovery-suspects`
sorted set, and an orphan check (every minute by default) releases one, the most recovered
first, once the one released before it is no longer waiting or running (or ten checks later);
the dashboard does not list that set. The poison pill then crashes its next process beside
first-time jobs only, and the jobs that ran beside it come back one by one and run: in a crash
loop they run minutes later rather than at once. When several jobs of one crash reach the limit
together, each gets one more recovery, alone, and the one that crashes its process again is the
poison pill. An iterable job forgets its recoveries when it saves progress, so a long iteration
that keeps making progress is never buried by unrelated crashes. Jobs pushed back at a shutdown
are never counted.

On `stop()` (or SIGTERM), a process quiets (fetches nothing more), waits up to `timeout` for
running jobs (which can watch `ctx.stopping()`), then interrupts the rest and requeues them;
a second signal interrupts immediately. Writing a job's outcome is retried for as long as
Redis is unreachable, so an outage delays acknowledgements and retries instead of losing
them. Paused queues are skipped by every fetch.

---

[← Configuration](configuration.md) · [Guide](README.md) · [Middleware →](middleware.md)
