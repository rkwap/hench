-- PERIODIC_UNPAUSE: resumes a schedule at its first tick after now (no backfill).
--
-- KEYS: [1] periodic, [2] periodic:<name>. ARGV: [1] name, [2] the next tick, computed from the
-- definition whose digest is [3].
-- Returns 1 resumed, 2 not paused, 0 no such schedule, -1 the definition changed (compute the
-- next tick again).

if redis.call('EXISTS', KEYS[2]) == 0 then return 0 end
local s = redis.call('HMGET', KEYS[2], 'paused', 'digest')
if not s[1] then return 2 end
if s[2] ~= ARGV[3] then return -1 end
redis.call('HSET', KEYS[2], 'next', ARGV[2])
redis.call('HDEL', KEYS[2], 'paused')
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
return 1
