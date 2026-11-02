-- PERIODIC_LEASE: takes the earliest due schedule out of the other pollers' reach for a lease,
-- and tells the poller when the next schedule is due (docs/design/periodic.md 2.5).
--
-- KEYS: [1] periodic (the due index: member = name, score = next tick or end of a lease).
-- ARGV: [1] the lease (ms), [2] the time to use instead of Redis's clock (ms; '' = TIME, the
--       override is for tests), [3..] names this poller cannot handle (skipped, at most 10 000).
-- Returns {now_ms, name, score} for a leased schedule (`score`: its score before the lease),
-- or {now_ms, false, earliest score or false} when none is due.
--
-- A lease only moves the score: the hash's `next` stays the authority, so a lease whose poller
-- crashed simply makes the schedule due again when it ends.

local now_ms
if ARGV[2] ~= '' then
  now_ms = tonumber(ARGV[2])
else
  now_ms = h_now_ms()
end
local skip = {}
local skips = #ARGV - 2
for i = 3, #ARGV do skip[ARGV[i]] = true end
local due = redis.call('ZRANGE', KEYS[1], '-inf', h_score(now_ms), 'BYSCORE', 'LIMIT', 0, skips + 1,
  'WITHSCORES')
for i = 1, #due, 2 do
  if not skip[due[i]] then
    redis.call('ZADD', KEYS[1], 'XX', h_score(now_ms + tonumber(ARGV[1])), due[i])
    return {now_ms, due[i], due[i + 1]}
  end
end
-- At most `skips` due members are skipped, so the first one not skipped is in the future.
local first = redis.call('ZRANGE', KEYS[1], 0, skips, 'WITHSCORES')
for i = 1, #first, 2 do
  if not skip[first[i]] then
    return {now_ms, false, first[i + 1]}
  end
end
return {now_ms, false, false}
