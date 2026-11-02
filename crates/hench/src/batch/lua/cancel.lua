-- BATCH_CANCEL: marks a batch cancelled; the first time is kept (docs/design/batches.md 2.5.7).
-- Members are not removed: workers skip them when they come up. `success` can no longer fire;
-- `complete` fires as soon as every member left has run at least once (at once if that already
-- holds). A hold (`open`: a workflow's later stages) is released: they will never be defined. A
-- batch whose `complete` was dispatched before the cancel is finished now if no member is left
-- pending (its keys keep their linger); otherwise once the last one is skipped.
--
-- KEYS: [1..6] the batch (hash, pending, failed, dead, kids, commits), [7] batches, [8]
--       batch-events, [9] the batch counters of the minute.
-- ARGV: [1] bid.
-- Returns {first cancel (1/0), dispatch needed (1/0), the bids of its children...} (the caller
-- cancels each child the same way), or an error reply NOBATCH <bid>, SUCCEEDED <bid>.

local bid = ARGV[1]
local now = h_now_ms()
local h = redis.call('HMGET', KEYS[1], 'v', 'success_at')
if not h[1] then return redis.error_reply('NOBATCH ' .. bid) end
if h[2] then return redis.error_reply('SUCCEEDED ' .. bid) end
local first = redis.call('HSETNX', KEYS[1], 'cancelled_at', now)
redis.call('HDEL', KEYS[1], 'open')
local fired = hb_evaluate(KEYS[1], KEYS[2], KEYS[3], KEYS[8], bid, now)
hb_finish_cancelled({KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6]}, KEYS[7], bid, now)
if first == 1 then pcall(hb_count, KEYS[9], 'cancelled', 1) end
hb_count_events(KEYS[9], fired)
local undispatched = redis.call('ZSCORE', KEYS[8], bid) and 1 or 0
return {first, undispatched, unpack(redis.call('SMEMBERS', KEYS[5]))}
