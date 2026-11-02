-- BATCH_SUCCEEDED: a member succeeded (or server middleware did not run it), recorded before
-- its acknowledgement (docs/design/batches.md 2.5.4). Idempotent per jid: a member no longer
-- pending (already counted, skipped, or unknown) changes nothing, so a repeat after a lost
-- reply, a duplicate execution or a stale copy is counted once. Its first write, SREM, is
-- allowed under OOM: a finishing batch is never left half-updated. A cancelled batch whose last
-- pending member this was is finished (its linger starts).
--
-- KEYS: [1..6] the batch (hash, pending, failed, dead, kids, commits), [7] batches, [8]
--       batch-events, [9] the batch counters of the minute.
-- ARGV: [1] jid, [2] bid.
-- Returns {applied (1/0), dispatch needed (1/0: the batch is in the outbox), refreshed (1/0:
-- its expiry was extended, refresh the ancestors), the events fired...}.

local jid, bid = ARGV[1], ARGV[2]
local now = h_now_ms()
local fired, touched = {}, 0
local applied = redis.call('SREM', KEYS[2], jid)
if applied == 1 then
  redis.call('HDEL', KEYS[3], jid)
  redis.call('SREM', KEYS[4], jid)
  fired = hb_evaluate(KEYS[1], KEYS[2], KEYS[3], KEYS[8], bid, now)
  local keys = {KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6]}
  hb_finish_cancelled(keys, KEYS[7], bid, now)
  touched = hb_touch(keys, KEYS[7], bid, now)
  hb_count_events(KEYS[9], fired)
end
local undispatched = redis.call('ZSCORE', KEYS[8], bid) and 1 or 0
return {applied, undispatched, touched, unpack(fired)}
