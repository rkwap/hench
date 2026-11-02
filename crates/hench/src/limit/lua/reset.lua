-- LIMIT_RESET: deletes the state and the counters of limit identities, freeing every slot
-- (holders keep running; their releases and renewals find nothing and count an overage).
--
-- KEYS: per identity i: [2i-1] its state, [2i] its stats hash; then [2n+1] the `limits` index.
-- ARGV: [1] n, then per identity its index member (<kind>:<id>).
-- Returns how many of the keys existed (the index entries not counted).

local n = tonumber(ARGV[1])
local index_key = KEYS[2 * n + 1]
local deleted = 0
for i = 1, n do
  deleted = deleted + redis.call('DEL', KEYS[2 * i - 1], KEYS[2 * i])
  redis.call('ZREM', index_key, ARGV[1 + i])
end
return deleted
