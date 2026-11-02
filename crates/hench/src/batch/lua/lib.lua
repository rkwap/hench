-- hench batches: the helpers of the batch scripts, after the shared prelude
-- (docs/design/batches.md 2.5.2). Helpers carry the hb_ prefix.
--
-- A batch is the hash batch:<bid> (definition, counters, event times) and five keys sharing its
-- expiry: :pending (SET of the jids not yet succeeded or skipped), :failed (HASH jid -> failure
-- summary), :dead (SET of the jids that will not run again on their own), :kids (SET of child
-- bids) and :commits (HASH commit id -> the duplicates that commit left out). Counts are the
-- sizes of these collections, never counters incremented and decremented, so that a report
-- repeated after a lost reply or by a duplicate execution changes nothing.
--
-- Scripts list every key of a batch in this order: hash, pending, failed, dead, kids, commits.

-- Epoch seconds with millisecond precision, as a sorted-set score.
local hb_secs = h_score

-- `true` when `key` exists and is not of type `want` (a write to it would fail half-way).
local function hb_wrong_type(key, want)
  local kind = redis.call('TYPE', key).ok
  return kind ~= 'none' and kind ~= want
end

-- Gives `key` the remaining lifetime of the batch hash, so that every key of a batch expires
-- together. Called after any write that may have created `key`.
local function hb_inherit_ttl(hash, key)
  local ttl = redis.call('PTTL', hash)
  if ttl > 0 then redis.call('PEXPIRE', key, ttl) end
end

-- Adds `n` to `field` in the batch counters of the minute (created, complete, success, death,
-- cancelled), kept 8 hours. Bookkeeping: always call it through pcall, as a script's last
-- writes (HINCRBY is refused under OOM and must never abort a script).
local function hb_count(minute, field, n)
  redis.call('HINCRBY', minute, field, n)
  redis.call('EXPIRE', minute, 28800)
end

-- Counts each event of `fired` in the minute's counters (under pcall).
local function hb_count_events(minute, fired)
  for _, event in ipairs(fired) do
    pcall(hb_count, minute, event, 1)
  end
end

-- Fires the events whose conditions now hold: `complete` (every member not yet succeeded has
-- failed at least once, and every child's `complete` was dispatched), then `success` (no
-- member left, every child's `success` dispatched, not cancelled). A batch held open (`open` >
-- 0: a workflow whose later stages are not defined yet) fires neither. Records the time of each
-- event in the hash and puts
-- the batch in the outbox (`events`), so that its callbacks are dispatched. `failed` may name a
-- key that does not exist (no failure yet). Returns the events fired by this call, in order.
local function hb_evaluate(hash, pending, failed, events, bid, now)
  local h = redis.call('HMGET', hash, 'v', 'kids', 'kc', 'ks', 'open',
    'complete_at', 'success_at', 'cancelled_at')
  if not h[1] then return {} end
  if tonumber(h[5] or '0') > 0 then return {} end
  local kids, kc, ks = tonumber(h[2] or '0'), tonumber(h[3] or '0'), tonumber(h[4] or '0')
  local npending = redis.call('SCARD', pending)
  local nfailed = redis.call('HLEN', failed)
  local fired = {}
  if not h[6] and npending == nfailed and kc >= kids then
    redis.call('HSET', hash, 'complete_at', now)
    fired[#fired + 1] = 'complete'
  end
  if not h[7] and not h[8] and npending == 0 and ks >= kids then
    redis.call('HSET', hash, 'success_at', now)
    fired[#fired + 1] = 'success'
  end
  if #fired > 0 then
    redis.call('ZADD', events, 'NX', hb_secs(now), bid)
  end
  return fired
end

-- Refreshes the expiry of an unfinished batch, never shortening it, once a quarter of its expiry
-- (an hour at most) went by since the last refresh, as its remaining lifetime shows: every key
-- gets `ttl` again (or more: the hash's remaining lifetime when a scheduled member made it
-- longer), and the index its new expiry. So a batch expires between 3/4 of `ttl` and `ttl` after
-- its last activity (with the default 30 days: an hour at most before). A finished batch
-- (`finished_at`: its linger started) keeps its linger. `keys` lists the six keys of the batch
-- (hash first). Returns 1 when it refreshed (the caller then refreshes the ancestors with
-- BATCH_TOUCH, so that a parent never expires before its child), else 0.
local function hb_touch(keys, index, bid, now)
  local hash = keys[1]
  local h = redis.call('HMGET', hash, 'ttl', 'finished_at')
  if not h[1] or h[2] then return 0 end
  local ttl = tonumber(h[1]) * 1000
  local left = redis.call('PTTL', hash)
  if left >= ttl - math.min(3600000, math.floor(ttl / 4)) then return 0 end
  ttl = math.max(ttl, left)
  for _, key in ipairs(keys) do redis.call('PEXPIRE', key, ttl) end
  redis.call('HSET', hash, 'touched_at', now)
  redis.call('ZADD', index, hb_secs(now + ttl), bid)
  return 1
end

-- Starts the linger of a batch that just finished (its `success` dispatched, or a cancelled batch
-- with nothing left pending: hb_finish_cancelled): `finished_at` stops later refreshes, every key
-- keeps only `linger` seconds, and the index follows. The commit ids of a batch created with a
-- chosen bid (`xbid`: Batch::with_bid) stay `ttl` longer (never less than the linger): a job
-- that created it and runs again later (a retry days after) still finds its commit applied
-- instead of creating the batch, and pushing its members, a second time.
local function hb_finish(keys, index, bid, now)
  local h = redis.call('HMGET', keys[1], 'linger', 'ttl', 'xbid')
  local linger = tonumber(h[1] or '86400') * 1000
  redis.call('HSET', keys[1], 'finished_at', now)
  for _, key in ipairs(keys) do redis.call('PEXPIRE', key, linger) end
  if h[3] then
    redis.call('PEXPIRE', keys[6], math.max(linger, tonumber(h[2] or '0') * 1000))
  end
  redis.call('ZADD', index, hb_secs(now + linger), bid)
end

-- Finishes a cancelled batch once its `complete` was dispatched and no member is left pending.
-- Members that failed wait in the retry set or the morgue; the batch must remember its
-- cancellation until each of them comes up and is skipped, so its linger starts only then (a
-- member that never comes back leaves the batch to expire `ttl` after its last activity).
-- Returns 1 when it finished the batch.
local function hb_finish_cancelled(keys, index, bid, now)
  local h = redis.call('HMGET', keys[1], 'cancelled_at', 'complete_dt', 'finished_at')
  if not h[1] or not h[2] or h[3] then return 0 end
  if redis.call('SCARD', keys[2]) > 0 then return 0 end
  hb_finish(keys, index, bid, now)
  return 1
end
