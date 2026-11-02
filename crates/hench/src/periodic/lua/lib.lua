-- hench periodic jobs: helpers shared by PERIODIC_FIRE and PERIODIC_ENQUEUE_NOW, after the
-- prelude and the unique library (docs/design/periodic.md 2.5). Helpers carry the hp_ prefix.

-- Writes a job: `enqueued` pushes it to its queue `target` (registering the queue `where` in
-- `queues`), `scheduled` adds it to the schedule `target` at score `where`; any other mode
-- writes nothing. Raises on errors (a key of the wrong type, OOM).
local function hp_write(queues, target, mode, job, where)
  if mode == 'enqueued' then
    redis.call('SADD', queues, where)
    redis.call('LPUSH', target, job)
  elseif mode == 'scheduled' then
    redis.call('ZADD', target, where, job)
  end
end

-- Records `entry` at the head of the history list, which keeps at most `cap` entries (0: none,
-- and an existing list is removed).
local function hp_history(history, entry, cap)
  if cap <= 0 then
    redis.call('DEL', history)
  elseif entry ~= '' then
    redis.call('LPUSH', history, entry)
    redis.call('LTRIM', history, 0, cap - 1)
  end
end

-- The TTL of the lock of a job taken now: `for_ms`, plus the delay of a job scheduled at `at`
-- (epoch seconds, '' when it is not scheduled).
local function hp_lock_ttl(for_ms, at, now_ms)
  local ttl = for_ms
  if at ~= '' then
    local delay = math.floor(tonumber(at) * 1000) - now_ms
    if delay > 0 then ttl = ttl + delay end
  end
  return ttl
end
