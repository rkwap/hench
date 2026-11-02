-- PERIODIC_REMOVE: deletes a schedule with its history and index entries.
--
-- KEYS: [1] periodic, [2] periodic-names, [3] periodic:<name>, [4] periodic-history:<name>,
--       [5] periodic-group:<group> of the group read beforehand (any key when none).
-- ARGV: [1] name, [2] the source required ('' = any), [3] the group read ('' = none).
-- Returns 1 removed, 0 no such schedule (stray index entries tidied), -2 another source,
-- -3 the schedule's group is not the one read (read again).

local s = redis.call('HMGET', KEYS[3], 'source', 'group')
if not s[1] then
  redis.call('ZREM', KEYS[1], ARGV[1])
  redis.call('ZREM', KEYS[2], ARGV[1])
  return 0
end
if ARGV[2] ~= '' and s[1] ~= ARGV[2] then return -2 end
if (s[2] or '') ~= ARGV[3] then return -3 end
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[2], ARGV[1])
if s[2] then redis.call('SREM', KEYS[5], ARGV[1]) end
redis.call('DEL', KEYS[3], KEYS[4])
return 1
