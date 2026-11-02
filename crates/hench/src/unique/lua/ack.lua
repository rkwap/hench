-- ACK_UNIQUE (reliable fetch): acknowledges a job and does its lock work (a release, for a
-- success or a skipped batch member) in one step. Assembled after the prelude, the unique
-- library and the outcome helpers.
--
-- KEYS: [1] the in-progress list, [2] the lock, [3] unique-locks, [4] the events hash of the
--       minute.
-- ARGV: [1] the job as fetched, [2] op (hold|release), [3] jid, [4] digest, [5] class, [6] queue,
--       [7] until, [8] for (ms).
-- Returns {acknowledged (1, or 0 when the job was no longer in progress: nothing done), lock
-- status (see hu_lock_work)}.

local now_ms = h_now_ms()
if redis.call('LREM', KEYS[1], -1, ARGV[1]) == 0 then
  return {0, 0}
end
local status = hu_lock_work(ARGV[2], KEYS[2], KEYS[3], KEYS[4], ARGV[3], ARGV[4], ARGV[5], ARGV[6],
  ARGV[7], tonumber(ARGV[8]), '', '', '', now_ms)
return {1, status}
