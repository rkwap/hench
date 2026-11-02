-- LIMIT_ADJUST: replaces the estimated cost of a points grant by the actual one, refunding or
-- charging the difference. A refund never takes the bucket below empty; a charge may take it
-- above full (debt, which later callers wait out).
--
-- KEYS: [1] the state (theoretical arrival time), [2] its stats hash.
-- ARGV: [1] '' or a fixed time (ms, tests only), [2] the difference in units (signed),
--   [3] the capacity, [4] the refill time (ms), [5] the stats TTL ms ('' without bookkeeping).
-- Returns the level after the correction, in units (a string; above the capacity in debt).

local now = hl_now(ARGV[1])
local delta, capacity, refill = tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4])
local ttl = tonumber(ARGV[5])
local interval = refill / capacity
local tat = tonumber(redis.call('GET', KEYS[1]) or now) or now
if tat < now then tat = now end
local new_tat = tat + delta * interval
if new_tat <= now then
  redis.call('DEL', KEYS[1])
  new_tat = now
else
  redis.call('SET', KEYS[1], string.format('%.3f', new_tat), 'PX', math.ceil(new_tat - now) + 1000)
end
if ttl and delta ~= 0 then hl_count(KEYS[2], 'adj', 1, ttl) end
return string.format('%.3f', (new_tat - now) / interval)
