-- BATCH_FAILED: a member failed, recorded before the failure is written (docs/design/batches.md
-- 2.5.4). `kind` is `retry` (it runs again later) or `dead` (it will not run again on its own:
-- the morgue, retries disabled, discarded, deleted or killed through the API). The summary is
-- kept current; the first death fires `death`; a member counted dead that runs again (revived
-- from the morgue) and fails with a retry is no longer dead. Idempotent per jid: a member no
-- longer pending (succeeded, skipped, or a stale duplicate) is ignored. Under OOM the first
-- write (HSET) is refused and nothing changes.
--
-- KEYS: as BATCH_SUCCEEDED. ARGV: [1] jid, [2] bid, [3] kind (retry|dead), [4] summary JSON.
-- Returns {applied (1/0), dispatch needed (1/0), refreshed (1/0), the events fired...}.

local jid, bid, kind = ARGV[1], ARGV[2], ARGV[3]
local now = h_now_ms()
local fired, touched = {}, 0
local applied = redis.call('SISMEMBER', KEYS[2], jid)
if applied == 1 then
  redis.call('HSET', KEYS[3], jid, ARGV[4])
  hb_inherit_ttl(KEYS[1], KEYS[3])
  if kind == 'dead' then
    redis.call('SADD', KEYS[4], jid)
    hb_inherit_ttl(KEYS[1], KEYS[4])
    if redis.call('HSETNX', KEYS[1], 'death_at', now) == 1 then
      redis.call('ZADD', KEYS[8], 'NX', hb_secs(now), bid)
      fired[#fired + 1] = 'death'
    end
  else
    redis.call('SREM', KEYS[4], jid)
  end
  for _, event in ipairs(hb_evaluate(KEYS[1], KEYS[2], KEYS[3], KEYS[8], bid, now)) do
    fired[#fired + 1] = event
  end
  touched = hb_touch({KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6]}, KEYS[7], bid, now)
  hb_count_events(KEYS[9], fired)
end
local undispatched = redis.call('ZSCORE', KEYS[8], bid) and 1 or 0
return {applied, undispatched, touched, unpack(fired)}
