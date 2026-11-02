-- PERIODIC_UPSERT: creates or updates one schedule (the API's dynamic schedules; usable for code
-- schedules by tools) (docs/design/periodic.md 2.5, 2.8).
--
-- KEYS: [1] periodic, [2] periodic:<name>, [3] periodic-names.
-- ARGV: [1] name, [2] the digest expected ('' = must not exist, '*' = any), [3] digest,
--       [4] source (api | code), [5] spec, [6] timing, [7] policy, [8] job, [9] description
--       ('' = none), [10] next for a new schedule, [11] next for a changed timetable,
--       [12] created paused ('1' or '0'), [13] now (ms).
-- Returns {1} created, {2} updated, {3} rescheduled (the timetable changed), {0} unchanged,
-- {-1, digest} the digest is not the one expected (read again), {-2, source} the schedule
-- belongs to the other source.

local cur = redis.call('HMGET', KEYS[2], 'digest', 'source', 'timing', 'paused', 'next')
local exists = cur[1] ~= false
if ARGV[2] == '' and exists then return {-1, cur[1]} end
if ARGV[2] ~= '' and ARGV[2] ~= '*' and ARGV[2] ~= (cur[1] or '') then
  return {-1, cur[1] or ''}
end
if exists and cur[2] ~= ARGV[4] then return {-2, cur[2] or ''} end
if exists and cur[1] == ARGV[3] then return {0} end
redis.call('HSET', KEYS[2], 'name', ARGV[1], 'digest', ARGV[3], 'source', ARGV[4],
  'spec', ARGV[5], 'timing', ARGV[6], 'policy', ARGV[7], 'job', ARGV[8], 'updated_at', ARGV[13])
redis.call('HDEL', KEYS[2], 'error', 'error_at')
if ARGV[9] ~= '' then
  redis.call('HSET', KEYS[2], 'description', ARGV[9])
else
  redis.call('HDEL', KEYS[2], 'description')
end
if not exists then
  redis.call('HSET', KEYS[2], 'created_at', ARGV[13])
  redis.call('ZADD', KEYS[3], 0, ARGV[1])
  if ARGV[12] == '1' then
    redis.call('HSET', KEYS[2], 'paused', ARGV[13])
    redis.call('HDEL', KEYS[2], 'next')
    redis.call('ZREM', KEYS[1], ARGV[1])
  else
    redis.call('HSET', KEYS[2], 'next', ARGV[10])
    redis.call('ZADD', KEYS[1], ARGV[10], ARGV[1])
  end
  return {1}
end
if cur[4] then return {2} end
if cur[3] ~= ARGV[6] then
  redis.call('HSET', KEYS[2], 'next', ARGV[11])
  redis.call('ZADD', KEYS[1], ARGV[11], ARGV[1])
  return {3}
end
-- Due at its `next` again (a lost `next` gets the new schedule's), so that a schedule parked for
-- its old definition is retried.
if cur[5] then
  redis.call('ZADD', KEYS[1], cur[5], ARGV[1])
else
  redis.call('HSET', KEYS[2], 'next', ARGV[10])
  redis.call('ZADD', KEYS[1], ARGV[10], ARGV[1])
end
return {2}
