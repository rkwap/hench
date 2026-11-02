-- BATCH_SKIPPED: a member will not run and no longer blocks its batch: skipped because its
-- batch was cancelled (`cancel`: verified here, atomically, so that a stray
-- JobError::batch_cancelled() cannot drop a member of a live batch), or by an operator
-- (`operator`). Idempotent per jid (docs/design/batches.md 2.5.4). A cancelled batch whose last
-- pending member this was is finished (its linger starts).
--
-- KEYS: as BATCH_SUCCEEDED. ARGV: [1] jid, [2] bid, [3] mode (cancel|operator).
-- Returns {applied (1/0, or -1 for `cancel` when the batch is not cancelled), dispatch needed
-- (1/0), refreshed (1/0), the events fired...}.

local jid, bid, mode = ARGV[1], ARGV[2], ARGV[3]
local now = h_now_ms()
if mode == 'cancel' and not redis.call('HGET', KEYS[1], 'cancelled_at') then
  return {-1, 0, 0}
end
local fired, touched = {}, 0
local applied = redis.call('SREM', KEYS[2], jid)
if applied == 1 then
  redis.call('HDEL', KEYS[3], jid)
  redis.call('SREM', KEYS[4], jid)
  redis.call('HINCRBY', KEYS[1], 'skipped', 1)
  fired = hb_evaluate(KEYS[1], KEYS[2], KEYS[3], KEYS[8], bid, now)
  local keys = {KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6]}
  hb_finish_cancelled(keys, KEYS[7], bid, now)
  touched = hb_touch(keys, KEYS[7], bid, now)
  hb_count_events(KEYS[9], fired)
end
local undispatched = redis.call('ZSCORE', KEYS[8], bid) and 1 or 0
return {applied, undispatched, touched, unpack(fired)}
