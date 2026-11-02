-- LIMIT_RENEW: extends the leases of concurrency permits still held (the heartbeat renews every
-- permit its process holds in one call; the task of a permit held in code renews its own).
--
-- KEYS: per permit i: [2i-1] the concurrency state, [2i] its stats hash.
-- ARGV: [1] '' or a fixed time (ms, tests only), [2] n, then 4 per permit: the token, the lease
--   (ms), the limit, the stats TTL ms ('' without bookkeeping).
-- Returns per permit: 1 renewed; 2 restored (its lease had ended, or its key was lost, and a
-- slot was free to take it back); 0 lost (no free slot: counted as an overage; the holder keeps
-- running, without a lease).

local now = hl_now(ARGV[1])
local n = tonumber(ARGV[2])
local out = {}
for i = 1, n do
  local state, stats = KEYS[2 * i - 1], KEYS[2 * i]
  local b = 2 + (i - 1) * 4
  local token, lease, limit = ARGV[b + 1], tonumber(ARGV[b + 2]), tonumber(ARGV[b + 3])
  local ttl = tonumber(ARGV[b + 4])
  local score = redis.call('ZSCORE', state, token)
  if score and tonumber(score) > now then
    redis.call('ZADD', state, 'XX', now + lease, token)
    h_extend_at_least(state, lease)
    out[i] = 1
  elseif redis.call('ZCOUNT', state, '(' .. now, '+inf') < limit then
    redis.call('ZADD', state, now + lease, token)
    h_extend_at_least(state, lease)
    if ttl then hl_count(stats, 'restored', 1, ttl) end
    out[i] = 2
  else
    if score then redis.call('ZREM', state, token) end
    if ttl then hl_count(stats, 'ov', 1, ttl) end
    out[i] = 0
  end
end
return out
