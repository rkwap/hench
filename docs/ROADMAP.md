# Roadmap

[CHANGELOG.md][changelog] lists what each release changed. Planned for 0.2:

- **Queue limits**: a concurrency or rate limit on a whole queue, enforced by the fetch, so that
  a queue over its limit is skipped like a paused one while its jobs keep their order and the
  other queues keep flowing. Until then, a [throttle](guide/rate-limiting.md) on the
  queue's job types limits each type, and a `[queue_limits]` table in the worker's file is
  refused rather than ignored.
- **Staged batch commits**: a batch of any size defined in one closure, written in chunks with
  bounded memory and still visible all at once. Until then one commit holds at most
  `batch_max_commit_jobs` jobs (10 000 by default, a child batch counting as 10), and a larger
  batch adds the rest from loader jobs (`Batch::open`, each with its own `commit_id`).
- **Iterable jobs that see their batch's cancellation**: an iterable job of a cancelled batch
  stops at its next checkpoint. Until then it runs to the end of its current execution, unless
  it calls `it.check_batch()` itself, and is skipped at its next one.
- **A process-wide cache of cancelled batches**: a batch's jobs learn of its cancellation
  without a Redis round trip each. Until then every execution of a batch's job reads its
  batch's cancellation from Redis (one `HGET`).
- **Batch tags**, on Redis 7.4 or newer: tag batches and find them by tag. Until then a batch is
  found by its bid or by the text of its description.
- **A lean lock mode for unique jobs**, if users ask for it: locks of about half the size, for
  applications that hold millions of them, which the Locks tab could not list. Until then every
  lock keeps the record (about 440 B) that the Locks tab shows and explains.

Later, without a date:

- the dashboard in other languages (it is in English only);
- creating and editing schedules in the dashboard (the API and `hench periodic add` create
  them);
- TLS through Redis Sentinel (`rediss+sentinel://` is refused), and options for a private
  certificate authority or a client certificate in the `hench` command;
- Dragonfly in the tests (it should work, but nothing checks that it does), and Valkey in CI
  (the suite passes against Valkey 9, run by hand);
- a `hench dump` command (`SIGTTIN` and `api::Process::dump()` log the busy jobs);
- a warning when a workflow's callback lands in a queue no process works on, and freed throttle
  permits handed straight to the jobs waiting for them.

[changelog]: ../CHANGELOG.md
