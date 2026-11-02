-- UNIQUE_PUSH: writes a chunk of jobs and takes the locks of those that carry a digest, in one
-- step (docs/design/unique.md 2.8.3). Assembled after the prelude and the unique library.
--
-- KEYS: [1] queues, [2] schedule, [3] unique-locks, [4] the events hash of the minute, then per
--       job i: [3 + 2i] its lock (KEYS[3] again for a job without one), [4 + 2i] its destination
--       (its queue's list, or the schedule).
-- ARGV: [1] the number of jobs, then 11 values per job: jid, the JSON as stored, mode
--       (queue|schedule), target (the queue's name, or the score), the queue's name, until
--       ('' for a job without a lock), for (ms), at (the score of a scheduled job, or ''),
--       on_conflict (drop|error|replace), class, digest.
-- Returns one {code, detail} per job: {1, ''} pushed; {2, jid} already pushed by an earlier
-- attempt (its jid holds the lock: nothing written); {0, holder} a duplicate (nothing written,
-- `d` counted); {3, replaced} pushed in place of the scheduled holder of the lock (`r` counted);
-- {4, error} pushed, but the lock could not be taken; {-1, error} not pushed.
--
-- A job is written before its lock is taken (a lock without its job would make a resend of the
-- same jid look already pushed), so the first write of the script is a job's: under OOM it is
-- refused and nothing is written at all. A replaced holder is removed after the new job is
-- written. The index trim, a removal, comes last.

local queues, schedule, index, events = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local now_ms = h_now_ms()

-- Writes a job to its queue (registering the queue) or to the schedule. Raises on errors.
local function write_job(dest, mode, target, json)
  if mode == 'queue' then
    redis.call('SADD', queues, target)
    redis.call('LPUSH', dest, json)
  else
    redis.call('ZADD', dest, target, json)
  end
end

-- Members a lock that does not record its holder's member (written by BATCH_COMMIT, or by an
-- older version) is looked for among, at its score.
local UNRECORDED_SCAN = 100

-- `member` if it is the job `holder`, not a member of a batch (its batch would wait for it
-- forever), else nil.
local function holder_member(member, holder)
  local ok, job = pcall(cjson.decode, member)
  if ok and type(job) == 'table' and job.jid == holder and type(job.bid) ~= 'string' then
    return member
  end
  return nil
end

-- The member of `holder` in the schedule, if it still waits there at the score its lock
-- recorded (a scheduler's lease moves the score a minute ahead, so a job being enqueued is not
-- found), and does not belong to a batch. The lock records the member (`m`): one ZSCORE finds
-- it, however many jobs share the score. A lock without it is looked for among the first
-- UNRECORDED_SCAN members at the score (matched as text before decoding).
local function replaceable(lock, holder)
  local recorded = redis.call('HMGET', lock, 'at', 'm')
  local at, member = recorded[1], recorded[2]
  if not at then return nil end
  if member then
    local score = redis.call('ZSCORE', schedule, member)
    if not score or tonumber(score) ~= tonumber(at) then return nil end
    return holder_member(member, holder)
  end
  local needle = '"jid":"' .. holder .. '"'
  local members = redis.call('ZRANGE', schedule, at, at, 'BYSCORE', 'LIMIT', 0, UNRECORDED_SCAN)
  for _, candidate in ipairs(members) do
    if string.find(candidate, needle, 1, true) then
      local found = holder_member(candidate, holder)
      if found then return found end
    end
  end
  return nil
end

local results = {}
for i = 1, tonumber(ARGV[1]) do
  local b = 1 + (i - 1) * 11
  local jid, json, mode, target, queue = ARGV[b + 1], ARGV[b + 2], ARGV[b + 3], ARGV[b + 4], ARGV[b + 5]
  local policy, for_ms, at = ARGV[b + 6], tonumber(ARGV[b + 7]), ARGV[b + 8]
  local conflict, class, digest = ARGV[b + 9], ARGV[b + 10], ARGV[b + 11]
  local lock, dest = KEYS[3 + 2 * i], KEYS[4 + 2 * i]
  local function write() write_job(dest, mode, target, json) end
  if policy == '' then
    local ok, err = pcall(write)
    if ok then results[i] = {1, ''} else results[i] = {-1, h_err(err)} end
  else
    -- TTL: `for`, plus the delay of a scheduled job.
    local ttl = for_ms
    if at ~= '' then
      local delay = math.floor(tonumber(at) * 1000) - now_ms
      if delay > 0 then ttl = ttl + delay end
    end
    local fields = {'class', class, 'queue', queue, 'until', policy, 'for', tostring(math.floor(for_ms / 1000))}
    -- A job waiting in the schedule: the lock records its member there.
    if mode == 'schedule' then
      fields[#fields + 1] = 'm'
      fields[#fields + 1] = json
    end
    local old = nil
    if conflict == 'replace' then
      local readable, state = pcall(hu_check, lock, jid)
      if readable and state ~= 'free' and state ~= 'self' then
        local found, member = pcall(replaceable, lock, state)
        if found and member then old = {member, state} end
      end
    end
    if old then
      local ok, err = pcall(write)
      if not ok then
        results[i] = {-1, h_err(err)}
      else
        redis.call('ZREM', schedule, old[1])
        redis.call('DEL', lock)
        pcall(hu_count, events, class, 'r')
        local took, take_err = pcall(hu_take, lock, index, digest, jid, ttl, now_ms, fields, at)
        if took then results[i] = {3, old[2]} else results[i] = {4, h_err(take_err)} end
      end
    else
      local ok, result = pcall(hu_push_one, lock, index, events, digest, jid, class, ttl, now_ms, fields, at, write)
      if not ok then
        results[i] = {-1, h_err(result)}
      elseif result[1] == 1 then
        results[i] = {1, ''}
      else
        results[i] = {result[1], result[2]}
      end
    end
  end
end
pcall(hu_trim, index, now_ms, 10)
return results
