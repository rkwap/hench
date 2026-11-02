-- PERIODIC_ERROR: records why a schedule cannot fire (a definition this process cannot read, a
-- template that cannot be pushed, a timetable without a next tick), if the schedule still
-- exists: a deleted schedule is never recreated. PERIODIC_FIRE clears it once the schedule
-- fires again.
--
-- KEYS: [1] periodic:<name>. ARGV: [1] the error, [2] when (ms).
-- Returns 1 recorded, 0 no such schedule.

if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
redis.call('HSET', KEYS[1], 'error', ARGV[1], 'error_at', ARGV[2])
return 1
