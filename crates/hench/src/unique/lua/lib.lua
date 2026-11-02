-- hench unique jobs: the lock helpers, prepended (after the shared prelude) to every script
-- that touches a lock: the unique design's own scripts, and BATCH_COMMIT, PERIODIC_FIRE and
-- PERIODIC_ENQUEUE_NOW, which take locks with the jobs they write.
--
-- FROZEN INTERFACE (docs/design/unique.md 2.8.2): hu_check, hu_take, hu_hold, hu_release,
-- hu_push_one, hu_count and hu_trim keep their names, arguments and return values; only their
-- bodies may change.
--
-- Lock: HASH unique:<digest> {jid, class, queue, until, for, t[, at, m]}, always with PEXPIRE.
-- `at` and `m` describe a holder waiting in `schedule`: its score and its member there (so a
-- debounce push finds it with one ZSCORE, however many jobs share its score).
-- Index: ZSET unique-locks, member = digest, score = the lock's expiry (epoch seconds, Redis
-- clock). Events: HASH u|YYYYMMDD|H:MM, fields <class>|d (duplicate), |r (replaced), |z
-- (lapsed), kept 8 hours.
-- `until` is a Lua keyword: a policy kept in a table lives in a field named `policy`; the
-- hash field is written as the string 'until'.

local hu_now_ms, hu_score, hu_err = h_now_ms, h_score, h_err

-- Removes up to `limit` index entries whose lock expired before `now_ms`. A removal: call it
-- as a script's last write (rule 5 of the design: it would let later writes pass OOM).
local function hu_trim(index, now_ms, limit)
  local stale = redis.call('ZRANGE', index, '-inf', hu_score(now_ms), 'BYSCORE', 'LIMIT', 0, limit)
  if #stale > 0 then
    redis.call('ZREM', index, unpack(stale))
  end
end

-- Counts `event` (d duplicate, r replaced, z lapsed) for `class` in the minute's hash `events`.
-- Always call it through pcall: HINCRBY is refused under OOM and must never abort a script.
local function hu_count(events, class, event)
  redis.call('HINCRBY', events, class .. '|' .. event, 1)
  redis.call('EXPIRE', events, 28800)
end

-- The member `fields` carries for the schedule ('m', the job's JSON there), or nil.
local function hu_member(fields)
  for i = 1, #fields - 1, 2 do
    if fields[i] == 'm' then return fields[i + 1] end
  end
  return nil
end

-- Creates the lock of `jid` for `ttl_ms`. `fields`: the flat list
-- {'class', class, 'queue', queue, 'until', policy, 'for', seconds}, followed by {'m', json}
-- for a job written to `schedule` (its member there); `at` = the job's score in `schedule`
-- while it waits there, or ''.
local function hu_take(lock, index, digest, jid, ttl_ms, now_ms, fields, at)
  redis.call('HSET', lock, 'jid', jid, 't', hu_score(now_ms), unpack(fields))
  if at ~= '' then redis.call('HSET', lock, 'at', at) end
  redis.call('PEXPIRE', lock, ttl_ms)
  redis.call('ZADD', index, hu_score(now_ms + ttl_ms), digest)
end

-- Keeps `jid`'s lock for at least `ttl_ms` (never shortens it) and records `at` with the member
-- `fields` carries (or clears both). Returns 1 = held and kept, 2 = was free and taken, 0 =
-- another job holds it (untouched).
local function hu_hold(lock, index, digest, jid, ttl_ms, now_ms, fields, at)
  local holder = redis.call('HGET', lock, 'jid')
  if holder == jid then
    if redis.call('PTTL', lock) < ttl_ms then
      redis.call('PEXPIRE', lock, ttl_ms)
      redis.call('ZADD', index, hu_score(now_ms + ttl_ms), digest)
    end
    local member = hu_member(fields)
    if at ~= '' and member then
      redis.call('HSET', lock, 'at', at, 'm', member)
    elseif at ~= '' then
      redis.call('HSET', lock, 'at', at)
      redis.call('HDEL', lock, 'm')
    else
      redis.call('HDEL', lock, 'at', 'm')
    end
    return 1
  elseif holder then
    return 0
  end
  hu_take(lock, index, digest, jid, ttl_ms, now_ms, fields, at)
  return 2
end

-- Releases `jid`'s lock. Returns 1 = released, 0 = no lock (a stale index entry is tidied),
-- -1 = another job holds it (untouched).
local function hu_release(lock, index, digest, jid)
  local holder = redis.call('HGET', lock, 'jid')
  if holder == jid then
    redis.call('DEL', lock)
    redis.call('ZREM', index, digest)
    return 1
  elseif holder then
    return -1
  end
  redis.call('ZREM', index, digest)
  return 0
end

-- Read only: 'free', 'self' (the lock is held by `jid`) or the holder's jid (24 hex
-- characters, so it cannot be mistaken for the two words). Call it through pcall: a lock key
-- of the wrong type raises.
local function hu_check(lock, jid)
  local holder = redis.call('HGET', lock, 'jid')
  if not holder then return 'free' end
  if holder == jid then return 'self' end
  return holder
end

-- One push under a lock, for scripts that write one job at a time (UNIQUE_PUSH without
-- `replace`, PERIODIC_FIRE, PERIODIC_ENQUEUE_NOW). `write` is the caller's function writing the
-- job: it runs only when the job may be pushed, and before the lock is taken (write before
-- lock: a lock without its job would make a retry of the same jid look already pushed).
-- Returns {1} pushed, {2, jid} already pushed (nothing written), {0, holder} duplicate (nothing
-- written, `d` counted), {4, err} written without its lock, {-1, err} the lock key is
-- unreadable (nothing written).
local function hu_push_one(lock, index, events, digest, jid, class, ttl_ms, now_ms, fields, at, write)
  local ok, state = pcall(hu_check, lock, jid)
  if not ok then return {-1, hu_err(state)} end
  if state == 'self' then return {2, jid} end
  if state ~= 'free' then
    pcall(hu_count, events, class, 'd')
    return {0, state}
  end
  write()
  local took, err = pcall(hu_take, lock, index, digest, jid, ttl_ms, now_ms, fields, at)
  if not took then return {4, hu_err(err)} end
  return {1}
end
