-- LIMIT_USAGE (read only): the current use of limit identities, for limiters and the API.
--
-- KEYS: per identity i: [2i-1] its state, [2i] its stats hash.
-- ARGV: [1] '' or a fixed time (ms, tests only), [2] n, then 3 per identity: its kind (c, w, b,
--   g), its limit and its period ms (the lease for c), both '' to read them from the signature
--   in the stats.
-- Returns per identity {units used (a string, '' when the limit is unknown), ms until a unit is
-- free (0 when one is free now, -1 when unknown; concurrency: the earliest lease end, a worst
-- case), the stats hash as a flat list, now}.

local now = hl_now(ARGV[1])
local n = tonumber(ARGV[2])
local out = {}
for i = 1, n do
  local state, stats = KEYS[2 * i - 1], KEYS[2 * i]
  local b = 2 + (i - 1) * 3
  local kind, limit, period = ARGV[b + 1], tonumber(ARGV[b + 2]), tonumber(ARGV[b + 3])
  local fields = redis.call('HGETALL', stats)
  if not limit then
    for f = 1, #fields - 1, 2 do
      if fields[f] == 'p' then limit, period = hl_parse_signature(fields[f + 1]) end
    end
  end
  local used, free_in = '', -1
  if kind == 'c' then
    local live = redis.call('ZCOUNT', state, '(' .. now, '+inf')
    used = tostring(live)
    if limit then
      if live < limit then
        free_in = 0
      else
        local first = redis.call('ZRANGEBYSCORE', state, '(' .. now, '+inf', 'WITHSCORES', 'LIMIT', 0, 1)
        if #first > 0 then free_in = math.max(0, math.ceil(tonumber(first[2]) - now)) end
      end
    end
  elseif limit and period then
    if kind == 'w' then
      local stamps = redis.call('LRANGE', state, 0, limit - 1)
      local count = 0
      for _, stamp in ipairs(stamps) do
        if tonumber(stamp) > now - period then count = count + 1 end
      end
      used = tostring(count)
      if count >= limit then
        free_in = math.max(0, math.ceil(tonumber(stamps[limit]) + period - now))
      else
        free_in = 0
      end
    elseif kind == 'b' then
      local bucket = math.floor(now / period)
      local stored = redis.call('HMGET', state, 'b', 'n')
      local count = 0
      if tonumber(stored[1]) == bucket then count = tonumber(stored[2]) or 0 end
      used = tostring(count)
      if count >= limit then free_in = (bucket + 1) * period - now else free_in = 0 end
    elseif kind == 'g' then
      local interval = period / limit
      local tat = tonumber(redis.call('GET', state) or now) or now
      if tat < now then tat = now end
      used = string.format('%.3f', (tat - now) / interval)
      local next_tat = tat + interval
      if next_tat - now <= period then
        free_in = 0
      else
        free_in = math.ceil(next_tat - period - now)
      end
    end
  end
  out[i] = {used, free_in, fields, now}
end
return out
