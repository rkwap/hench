-- BATCH_DELETE: deletes the data of one batch (docs/design/batches.md 2.5.8). A batch that is
-- an unfinished child of a parent that still exists and has not finished is refused unless
-- forced: that parent could then never finish. The caller deletes the descendants first.
-- Members still queued run normally: their accounting finds no batch and changes nothing.
--
-- KEYS: [1..6] the batch (hash, pending, failed, dead, kids, commits), [7] batches, [8]
--       batch-events, [9] the parent's hash (the batch's own when it has no parent).
-- ARGV: [1] bid, [2] force (1|0).
-- Returns the number of keys removed, or an error reply DELETE_CHILD <parent bid>.

local h = redis.call('HMGET', KEYS[1], 'parent', 'finished_at')
if ARGV[2] ~= '1' and h[1] and not h[2] then
  local p = redis.call('HMGET', KEYS[9], 'v', 'finished_at')
  if p[1] and not p[2] then return redis.error_reply('DELETE_CHILD ' .. h[1]) end
end
local removed = redis.call('UNLINK', KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6])
redis.call('ZREM', KEYS[7], ARGV[1])
redis.call('ZREM', KEYS[8], ARGV[1])
return removed
