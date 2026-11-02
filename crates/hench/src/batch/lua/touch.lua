-- BATCH_TOUCH: refreshes the expiry of an unfinished batch, never shortening it: run for the
-- ancestors of a batch whose expiry an accounting, a dispatch or a reopen just extended, so that
-- a parent never expires while a child is still active (docs/design/batches.md 2.5.6). The batch
-- gets the longest of its `ttl`, its remaining lifetime and the floor: the lifetime of the child
-- the walk comes from (a reopened child may have gained a member scheduled far ahead).
--
-- KEYS: [1..6] the batch (hash, pending, failed, dead, kids, commits), [7] batches.
-- ARGV: [1] bid, [2] the floor, ms ('' or 0: none).
-- Returns {the parent's bid ('' for a root, a batch that is gone or a finished one), the
-- batch's lifetime now, ms (0 when it was not refreshed)}: the caller walks up the chain, one
-- call per level, passing each level's lifetime as the next floor.

local now = h_now_ms()
local h = redis.call('HMGET', KEYS[1], 'ttl', 'finished_at', 'parent')
if not h[1] or h[2] then return {'', 0} end
local ttl = math.max(tonumber(h[1]) * 1000, redis.call('PTTL', KEYS[1]), tonumber(ARGV[2]) or 0)
for i = 1, 6 do redis.call('PEXPIRE', KEYS[i], ttl) end
redis.call('HSET', KEYS[1], 'touched_at', now)
redis.call('ZADD', KEYS[7], hb_secs(now + ttl), ARGV[1])
return {h[3] or '', ttl}
