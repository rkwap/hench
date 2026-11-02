-- UNIQUE_RENEW (every heartbeat): extends the locks of the running `success` jobs that still
-- hold them, to at least `for` from now. Never creates a lock: a heartbeat working from a
-- snapshot taken before a job's acknowledgement must not bring back a released lock. A job
-- that no longer holds its lock is reported (its outcome counts the lapse, once). Assembled
-- after the prelude and the unique library.
--
-- KEYS: [1] unique-locks, then one lock per job.
-- ARGV: three values per job: jid, digest, for (ms).
-- Returns the positions (1-based) of the jobs that do not hold their lock.

local now_ms = h_now_ms()

local function extend(lock, digest, for_ms)
  if redis.call('PTTL', lock) < for_ms then
    redis.call('PEXPIRE', lock, for_ms)
    redis.call('ZADD', KEYS[1], h_score(now_ms + for_ms), digest)
  end
end

local lapsed = {}
for i = 2, #KEYS do
  local b = (i - 2) * 3
  local jid, digest, for_ms = ARGV[b + 1], ARGV[b + 2], tonumber(ARGV[b + 3])
  local ok, holder = pcall(redis.call, 'HGET', KEYS[i], 'jid')
  if ok and holder == jid then
    pcall(extend, KEYS[i], digest, for_ms)
  else
    lapsed[#lapsed + 1] = i - 1
  end
end
return lapsed
