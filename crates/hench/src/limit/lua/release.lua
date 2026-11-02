-- LIMIT_RELEASE: gives concurrency leases back and records how long they were held.
--
-- KEYS: per lease i: [2i-1] the concurrency state, [2i] its stats hash.
-- ARGV: [1] '' or a fixed time (ms, tests only), [2] n, then 3 per lease: the token, when it was
--   acquired (ms, Redis time; '' when unknown: no hold time is recorded), the stats TTL ms (''
--   without bookkeeping).
-- Returns per lease: 1 released, 2 released after its lease had ended (an overage), 0 not held
-- (reclaimed after its lease ended, or reset: an overage too).
-- Repeating a release is harmless: the second finds nothing.
--
-- Besides the totals `h` and `hms`, the stats keep `hold`, an average of the hold times that
-- decays (each release weighs a fifth): the wait line's spacing follows how long holders take
-- now, not over the last day.

local now = hl_now(ARGV[1])
local n = tonumber(ARGV[2])
local out = {}
for i = 1, n do
  local state, stats = KEYS[2 * i - 1], KEYS[2 * i]
  local b = 2 + (i - 1) * 3
  local token, acquired, ttl = ARGV[b + 1], tonumber(ARGV[b + 2]), tonumber(ARGV[b + 3])
  local score = redis.call('ZSCORE', state, token)
  if not score then
    if ttl then hl_count(stats, 'ov', 1, ttl) end
    out[i] = 0
  else
    -- Removing the last lease deletes the set.
    redis.call('ZREM', state, token)
    -- Hold times only when the caller knows when the lease was acquired.
    if ttl and acquired then
      local held = math.max(0, math.floor(now - acquired))
      hl_count(stats, 'h', 1, ttl)
      hl_count(stats, 'hms', held, ttl)
      local ok, recent = pcall(redis.call, 'HGET', stats, 'hold')
      recent = ok and tonumber(recent) or nil
      if recent then held = recent + (held - recent) / 5 end
      pcall(redis.call, 'HSET', stats, 'hold', string.format('%.1f', held))
    end
    if tonumber(score) <= now then
      if ttl then hl_count(stats, 'ov', 1, ttl) end
      out[i] = 2
    else
      out[i] = 1
    end
  end
end
return out
