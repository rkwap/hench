-- REMOVE_UNIQUE (the API deleting one job from a queue or a sorted set): removes the job and
-- releases its lock in one step (the holder compared: another job's lock is left alone).
-- Assembled after the prelude and the unique library.
--
-- KEYS: [1] the queue's list or the sorted set, [2] the lock, [3] unique-locks.
-- ARGV: [1] list or zset, [2] the job as stored, [3] jid, [4] digest.
-- Returns {removed (1, or 0 when the job was not there: nothing done), lock status (1
-- released, 0 no lock, -1 another holder, -2 the lock key could not be used)}.

local removed
if ARGV[1] == 'list' then
  removed = redis.call('LREM', KEYS[1], 1, ARGV[2])
else
  removed = redis.call('ZREM', KEYS[1], ARGV[2])
end
if removed == 0 then return {0, 0} end
local ok, status = pcall(hu_release, KEYS[2], KEYS[3], ARGV[4], ARGV[3])
if not ok then status = -2 end
return {1, status}
