-- LIMIT_TRIM: removes from the `limits` index the identities whose state and counters are both
-- gone (evicted, or deleted by hand, while their index entry still lived). The check and the
-- removal are atomic, so an identity used again meanwhile keeps its entry.
--
-- KEYS: per identity i: [2i-1] its state, [2i] its stats hash; then [2n+1] the `limits` index.
-- ARGV: [1] n, then per identity its index member (<kind>:<id>).
-- Returns how many entries were removed.

local n = tonumber(ARGV[1])
local index_key = KEYS[2 * n + 1]
local removed = 0
for i = 1, n do
  if redis.call('EXISTS', KEYS[2 * i - 1], KEYS[2 * i]) == 0 then
    removed = removed + redis.call('ZREM', index_key, ARGV[1 + i])
  end
end
return removed
