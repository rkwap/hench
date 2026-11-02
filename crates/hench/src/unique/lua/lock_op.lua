-- UNIQUE_LOCK_OP (basic fetch, which has no in-progress list to guard the outcome with): the
-- lock work of an outcome, after its write. Idempotent: holds compare the jid and only extend,
-- releases compare the jid. Assembled after the prelude, the unique library and the outcome
-- helpers.
--
-- KEYS: [1] the lock, [2] unique-locks, [3] the events hash of the minute.
-- ARGV: [1] op (hold|release), [2] jid, [3] digest, [4] class, [5] queue, [6] until, [7] for
--       (ms), [8] when the job runs next (a score, or '': the hold lasts `for` beyond it), [9]
--       the score the lock records as `at` (a job waiting in the schedule), or '', [10] the
--       job's member in the schedule the lock records with it, or ''.
-- Returns the lock status (see hu_lock_work).

local now_ms = h_now_ms()
return hu_lock_work(ARGV[1], KEYS[1], KEYS[2], KEYS[3], ARGV[2], ARGV[3], ARGV[4], ARGV[5],
  ARGV[6], tonumber(ARGV[7]), ARGV[8], ARGV[9], ARGV[10] or '', now_ms)
