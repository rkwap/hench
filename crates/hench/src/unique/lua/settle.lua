-- SETTLE_UNIQUE (reliable fetch): settles a failed job like SETTLE (to the retry set, to the
-- morgue, or nowhere) and does its lock work in the same step: a hold while a retry waits
-- (until the retry plus `for`), a release when the job will not run again. Assembled after the
-- prelude, the unique library and the outcome helpers.
--
-- KEYS: [1] the in-progress list, [2] the set written (retry or dead; any key for drop), [3]
--       the lock, [4] unique-locks, [5] the events hash of the minute.
-- ARGV: [1] the job as fetched, [2] mode (retry|dead|drop), [3] the job to write, [4] its
--       score, [5] the oldest score the morgue keeps, [6] the rank bound of its size trim (as
--       SETTLE), then [7] op (hold|release), [8] jid, [9] digest, [10] class, [11] queue,
--       [12] until, [13] for (ms).
-- Returns {settled (1, or 0 when the job was no longer in progress: nothing written), lock
-- status}. The write comes first (refused under OOM: nothing changes), the removal from the
-- in-progress list last, the lock work between them under pcall.

local now_ms = h_now_ms()
if not redis.call('LPOS', KEYS[1], ARGV[1], 'RANK', -1) then
  return {0, 0}
end
if ARGV[2] ~= 'drop' then
  redis.call('ZADD', KEYS[2], ARGV[4], ARGV[3])
  if ARGV[2] == 'dead' then
    redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', ARGV[5])
    redis.call('ZREMRANGEBYRANK', KEYS[2], 0, ARGV[6])
  end
end
-- A retry waits in the retry set, where a debounce push never replaces it: no `at`.
local until_at = ''
if ARGV[2] == 'retry' then until_at = ARGV[4] end
local status = hu_lock_work(ARGV[7], KEYS[3], KEYS[4], KEYS[5], ARGV[8], ARGV[9], ARGV[10],
  ARGV[11], ARGV[12], tonumber(ARGV[13]), until_at, '', '', now_ms)
redis.call('LREM', KEYS[1], -1, ARGV[1])
return {1, status}
