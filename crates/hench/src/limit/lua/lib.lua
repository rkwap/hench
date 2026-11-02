-- hench rate limiting: the helpers of the limiter scripts, after the shared prelude.
--
-- The state of one limit identity, by kind (the letter in its key, limit:<kind>:<id>):
--   c  concurrency: ZSET token -> lease end (ms, Redis time)
--   w  sliding window: LIST of grant times (ms), newest first, at most `limit` entries
--   b  fixed bucket: HASH b = bucket index (floor(ms / interval)), n = units used in it
--   g  leaky bucket or points budget: STRING, the theoretical arrival time (ms, 3 decimals)
-- Its counters, signature and wait line live in limit-stats:<kind>:<id> (a HASH).
--
-- ARGV[1] of every script is '' (the time comes from Redis) or a fixed time in ms (tests only).

-- The time of the call, in ms.
local function hl_now(override)
  if override ~= nil and override ~= '' then return tonumber(override) end
  return h_now_ms()
end

-- The flags of a call: L keeps a wait line, N skips the bookkeeping (counters, signature,
-- index, wait line).
local function hl_flag(flags, letter)
  return string.find(flags or '', letter, 1, true) ~= nil
end

-- Read-only check of one limit for `cost` units at `now`. Returns ok, wait (ms until the
-- limit could grant: exact for rate kinds, 0 for concurrency, which the caller estimates; -1
-- when `cost` can never be granted), and what the grant needs (the units used in the bucket,
-- the new arrival time of a leaky bucket, 'held' when the token already holds a live lease).
local function hl_check(kind, state, limit, period, cost, token, now)
  if kind == 'c' then
    if token ~= '' then
      local mine = redis.call('ZSCORE', state, token)
      if mine and tonumber(mine) > now then return true, 0, 'held' end
    end
    local live = redis.call('ZCOUNT', state, '(' .. now, '+inf')
    if live + 1 <= limit then return true, 0, nil end
    return false, 0, nil
  elseif kind == 'w' then
    if cost > limit then return false, -1, nil end
    -- The grant at index limit - cost (0-based) must have left the window: then at most
    -- limit - cost grants remain in it.
    local edge = redis.call('LINDEX', state, limit - cost)
    if not edge or tonumber(edge) <= now - period then return true, 0, nil end
    return false, tonumber(edge) + period - now, nil
  elseif kind == 'b' then
    if cost > limit then return false, -1, nil end
    local bucket = math.floor(now / period)
    local stored = redis.call('HMGET', state, 'b', 'n')
    local used = 0
    if tonumber(stored[1]) == bucket then used = tonumber(stored[2]) or 0 end
    if used + cost <= limit then return true, 0, used end
    return false, (bucket + 1) * period - now, used
  elseif kind == 'g' then
    if cost > limit then return false, -1, nil end
    local interval = period / limit
    local tat = tonumber(redis.call('GET', state) or now) or now
    if tat < now then tat = now end
    local new_tat = tat + cost * interval
    if new_tat - now <= period then return true, 0, new_tat end
    return false, math.ceil(new_tat - period - now), new_tat
  end
  return error('unknown limit kind ' .. tostring(kind))
end

-- Applies a granted check; the first write of a grant is this one (under OOM it is refused
-- and nothing is written). Returns the number of ended leases purged (concurrency).
local function hl_grant(kind, state, limit, period, cost, token, now, extra)
  if kind == 'c' then
    redis.call('ZADD', state, now + period, token)
    h_extend_at_least(state, period)
    return redis.call('ZREMRANGEBYSCORE', state, '-inf', now)
  elseif kind == 'w' then
    -- In chunks: unpack has a limit on the values it returns.
    local left = cost
    while left > 0 do
      local chunk = math.min(left, 1000)
      local stamps = {}
      for i = 1, chunk do stamps[i] = now end
      redis.call('LPUSH', state, unpack(stamps))
      left = left - chunk
    end
    redis.call('LTRIM', state, 0, limit - 1)
    h_extend_at_least(state, period + 1000)
  elseif kind == 'b' then
    local bucket = math.floor(now / period)
    redis.call('HSET', state, 'b', bucket, 'n', extra + cost)
    h_extend_at_least(state, (bucket + 1) * period - now + 1000)
  elseif kind == 'g' then
    redis.call('SET', state, string.format('%.3f', extra), 'PX', math.ceil(extra - now) + 1000)
  end
  return 0
end

-- Bookkeeping of a use of a limit (under pcall: it never fails a call, and runs under OOM):
-- the counter `field`, the signature and the last use, the expiry of the stats (`ttl` ms after
-- the last use, never shortened) and the index entry scored by that expiry. Returns the
-- signature stored before when it differed from `sig`.
local function hl_touch(stats, index, member, sig, ttl, now, field)
  local previous = nil
  local ok, stored = pcall(redis.call, 'HGET', stats, 'p')
  if ok and type(stored) == 'string' and stored ~= sig then previous = stored end
  pcall(redis.call, 'HINCRBY', stats, field, 1)
  pcall(redis.call, 'HSET', stats, 'p', sig, 'last', now)
  pcall(h_extend_at_least, stats, ttl)
  pcall(redis.call, 'ZADD', index, 'GT', h_score(now + ttl), member)
  return previous
end

-- Adds `by` to the counter `field` of a stats hash, giving the hash an expiry if it has none
-- (a stats hash never lives without one). Under pcall.
local function hl_count(stats, field, by, ttl)
  if by == 0 then return end
  pcall(redis.call, 'HINCRBY', stats, field, by)
  if ttl and ttl > 0 then pcall(h_extend_at_least, stats, ttl) end
end

-- The per-minute counter of a limiter name (`<name>|g` granted, `<name>|r` refused), kept
-- 8 hours like the other per-minute counters. Under pcall.
local function hl_minute(events, name, field)
  pcall(redis.call, 'HINCRBY', events, name .. '|' .. field, 1)
  pcall(redis.call, 'EXPIRE', events, 28800)
end

-- The limit and period of a signature (`w|100|1000`, `c|25`), when the caller does not know
-- them (the API reads limits it did not define).
local function hl_parse_signature(sig)
  if type(sig) ~= 'string' then return nil, nil end
  local letter, limit, period = string.match(sig, '^(%a)|(%d+)|?(%d*)$')
  if not letter then return nil, nil end
  return tonumber(limit), tonumber(period)
end
