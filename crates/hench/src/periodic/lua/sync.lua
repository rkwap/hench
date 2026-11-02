-- PERIODIC_SYNC: registers a group's crontab (its code-defined schedules) in one step: creates
-- or updates every schedule of the crontab and deletes the group's schedules that are no longer
-- in it (docs/design/periodic.md 2.7).
--
-- KEYS: [1] periodic, [2] periodic-names, [3] periodic-crontabs, [4] periodic-group:<group>,
--       then periodic:<name> for each of the n schedules (in ARGV order), then
--       periodic:<stale> and periodic-history:<stale> for each member of the group read
--       beforehand that the crontab no longer has.
-- ARGV: [1] group, [2] the crontab's record (JSON), [3] the group's members as read (joined by
--       newlines), [4] n, [5] now (ms), then 10 values per schedule: name, digest, spec,
--       timing, policy, job, description ('' = none), next for a new schedule, next for a
--       changed timetable, created paused ('1' or '0').
-- Returns {-1} when the group's members changed since they were read (read again), else
-- {1, created, updated, rescheduled, unchanged, removed, conflicts...}, the conflicts being
-- the names an API schedule or another group already uses (left as they are).
--
-- Operators' state (paused, history, counters, last enqueue and run) is never rewritten. A
-- schedule that should be in the due index and is not (or has lost its `next`) is put back;
-- `NX` keeps a poller's lease.

local due, names, crontabs, members_key = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local group, now = ARGV[1], ARGV[5]

-- The group's members must be exactly those read (compared as sets: string order in Lua
-- depends on the server's locale).
local expected, count = {}, 0
for member in string.gmatch(ARGV[3], '[^\n]+') do
  expected[member] = true
  count = count + 1
end
local members = redis.call('SMEMBERS', members_key)
if #members ~= count then return {-1} end
for _, member in ipairs(members) do
  if not expected[member] then return {-1} end
end

-- Puts an active schedule back in the due index at its `next` (`NX`: unless it is there, which
-- keeps a poller's lease), or at `fallback` when its `next` was lost.
local function ensure_due(key, name, current_next, fallback, force)
  if current_next and force then
    redis.call('ZADD', due, current_next, name)
  elseif current_next then
    redis.call('ZADD', due, 'NX', current_next, name)
  else
    redis.call('HSET', key, 'next', fallback)
    redis.call('ZADD', due, fallback, name)
  end
end

local n = tonumber(ARGV[4])
local created, updated, rescheduled, unchanged, removed = 0, 0, 0, 0, 0
local conflicts = {}
for i = 1, n do
  local a = 5 + (i - 1) * 10
  local name, digest, spec, timing, policy, job, description, next_new, next_changed, paused =
    ARGV[a + 1], ARGV[a + 2], ARGV[a + 3], ARGV[a + 4], ARGV[a + 5], ARGV[a + 6], ARGV[a + 7],
    ARGV[a + 8], ARGV[a + 9], ARGV[a + 10]
  local key = KEYS[4 + i]
  local cur = redis.call('HMGET', key, 'digest', 'source', 'timing', 'paused', 'group', 'next')
  if cur[1] and (cur[2] ~= 'code' or cur[5] ~= group) then
    conflicts[#conflicts + 1] = name
  elseif cur[1] == digest then
    unchanged = unchanged + 1
    redis.call('SADD', members_key, name)
    redis.call('ZADD', names, 0, name)
    if not cur[4] then ensure_due(key, name, cur[6], next_new, false) end
  else
    redis.call('HSET', key, 'name', name, 'digest', digest, 'source', 'code', 'group', group,
      'spec', spec, 'timing', timing, 'policy', policy, 'job', job, 'updated_at', now)
    -- A new or changed definition: an error the pollers recorded may no longer apply.
    redis.call('HDEL', key, 'error', 'error_at')
    if description ~= '' then
      redis.call('HSET', key, 'description', description)
    else
      redis.call('HDEL', key, 'description')
    end
    redis.call('SADD', members_key, name)
    redis.call('ZADD', names, 0, name)
    if not cur[1] then
      redis.call('HSET', key, 'created_at', now)
      if paused == '1' then
        redis.call('HSET', key, 'paused', now)
        redis.call('HDEL', key, 'next')
        redis.call('ZREM', due, name)
      else
        redis.call('HSET', key, 'next', next_new)
        redis.call('ZADD', due, next_new, name)
      end
      created = created + 1
    elseif cur[3] ~= timing and not cur[4] then
      redis.call('HSET', key, 'next', next_changed)
      redis.call('ZADD', due, next_changed, name)
      rescheduled = rescheduled + 1
    else
      -- Due at its `next` again, so that a schedule parked for its old definition is retried.
      if not cur[4] then ensure_due(key, name, cur[6], next_new, true) end
      updated = updated + 1
    end
  end
end

for k = 5 + n, #KEYS, 2 do
  local key = KEYS[k]
  -- The name after the 9-byte prefix `periodic:` (pinned by a test of hench::keys).
  local name = string.sub(key, 10)
  local s = redis.call('HMGET', key, 'source', 'group')
  redis.call('SREM', members_key, name)
  if not s[1] then
    -- A member whose hash is gone: tidy its index entries.
    redis.call('ZREM', due, name)
    redis.call('ZREM', names, name)
  elseif s[1] == 'code' and s[2] == group then
    redis.call('ZREM', due, name)
    redis.call('ZREM', names, name)
    redis.call('DEL', key, KEYS[k + 1])
    removed = removed + 1
  end
end

redis.call('HSET', crontabs, group, ARGV[2])
local result = {1, created, updated, rescheduled, unchanged, removed}
for _, name in ipairs(conflicts) do result[#result + 1] = name end
return result
