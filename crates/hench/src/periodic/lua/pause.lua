-- PERIODIC_PAUSE: pauses a schedule: it leaves the due index and loses its `next`; no tick is
-- counted as missed while it is paused.
--
-- KEYS: [1] periodic, [2] periodic:<name>. ARGV: [1] name, [2] now (ms).
-- Returns 1 paused, 2 already paused, 0 no such schedule.

if redis.call('EXISTS', KEYS[2]) == 0 then return 0 end
if redis.call('HEXISTS', KEYS[2], 'paused') == 1 then return 2 end
redis.call('HSET', KEYS[2], 'paused', ARGV[2])
redis.call('HDEL', KEYS[2], 'next')
redis.call('ZREM', KEYS[1], ARGV[1])
return 1
