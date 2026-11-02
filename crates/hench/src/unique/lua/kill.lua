-- KILL_UNIQUE (the API killing one job of a sorted set): MOVE_TO_MORGUE's steps (add to the
-- morgue, remove from the source unless it is the morgue, trim the morgue), then the release
-- of the job's lock (the holder compared). Assembled after the prelude and the unique library.
--
-- KEYS: [1] the source set, [2] the morgue, [3] the lock, [4] unique-locks.
-- ARGV: [1] the job as stored, [2] now (its score in the morgue), [3] the oldest score the
--       morgue keeps, [4] the rank bound of its size trim, [5] jid, [6] digest.
-- Returns {moved (1 moved, 2 refreshed in place: the source is the morgue, 0 not in the source:
-- nothing done), lock status}.

if not redis.call('ZSCORE', KEYS[1], ARGV[1]) then
  return {0, 0}
end
redis.call('ZADD', KEYS[2], ARGV[2], ARGV[1])
local moved = 2
if KEYS[1] ~= KEYS[2] then
  redis.call('ZREM', KEYS[1], ARGV[1])
  moved = 1
end
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', ARGV[3])
redis.call('ZREMRANGEBYRANK', KEYS[2], 0, ARGV[4])
local ok, status = pcall(hu_release, KEYS[3], KEYS[4], ARGV[6], ARGV[5])
if not ok then status = -2 end
return {moved, status}
