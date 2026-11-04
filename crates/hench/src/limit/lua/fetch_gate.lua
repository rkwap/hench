-- LIMIT_FETCH_GATE: the non-blocking step of a fetch for a capsule with limited queues. Takes
-- the next job of the first open (not paused), non-empty queue whose limits grant one more job,
-- in fetch order: an unlimited queue is fetched as the plain fetch scripts do; a limited one is
-- checked first (every limit of the queue, all or none, like LIMIT_ACQUIRE with a cost of 1) and,
-- if granted, its job is taken and the grant written in the same step. A queue over its limits
-- is skipped (its jobs stay where they are, in order) and reported with its wait; an empty
-- limited queue is not charged.
--
-- Before using a queue, the script checks that the limits the process believes it has are the
-- stored ones (`queue-limits`): when they differ, nothing is taken and the process re-reads them.
--
-- KEYS: [1] the in-progress list (reliable fetch), [2] the in-progress owners set, [3] the
--   paused set, [4] the queue-limits hash, [5] the limits index, [6] the minute's counters
--   (l|YYYYMMDD|H:MM, the caller's clock); then per queue its list, followed by 2 keys per limit
--   (its state, its stats hash).
-- ARGV: [1] '' or a fixed time (ms, tests only), [2] the process identity, [3] the permit token,
--   [4] the fetch mode (R reliable: LMOVE to the in-progress list; B basic: RPOP), [5] the stats
--   TTL (ms), [6] the number of queues; then per queue: its name, the stored form of the limits
--   the process believes it has ('' for none), the number of limits n, then 5 per limit: kind
--   (c, w, b, g), limit, period ms (the lease for c), index member (<kind>:@<queue>), signature.
-- Returns {1, queue key, job, token ('' for an unlimited queue), now (0 when not read), {skipped},
--   more, wait ms, busy} when a job was taken, where more is 0 when the queue's limits would
--   refuse another job now (with the wait and busy of a skipped queue; an unlimited queue
--   returns no more); {0, {open queue keys}, {skipped}} when none was; {2, name, {skipped}}
--   when the stored limits of queue `name` differ from the process's. The open queues are the ones
--   neither paused nor skipped (for the blocking step). Skipped lists name, wait ms, busy for
--   each queue over its limits that the script reached: the wait is the time until its rate
--   limits free a unit; busy is 1 when a concurrency limit is full, 2 when the state of one of
--   its limits cannot be read (a key of the wrong type).

local inprogress, owners, paused, configured = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local index_key, events_key = KEYS[5], KEYS[6]
local identity, token, reliable = ARGV[2], ARGV[3], ARGV[4] == 'R'
local ttl, count = tonumber(ARGV[5]), tonumber(ARGV[6])
local now = nil

local function take(queue)
  if reliable then
    local job = redis.call('LMOVE', queue, inprogress, 'RIGHT', 'LEFT')
    if job then redis.call('SADD', owners, identity) end
    return job
  end
  return redis.call('RPOP', queue)
end

local open, skipped = {}, {}
local k, a = 7, 7
for _ = 1, count do
  local queue = KEYS[k]
  local name, believed, n = ARGV[a], ARGV[a + 1], tonumber(ARGV[a + 2])
  local limits = {}
  for r = 1, n do
    local b = a + 3 + (r - 1) * 5
    local kind = ARGV[b]
    limits[r] = {
      kind = kind, limit = tonumber(ARGV[b + 1]), period = tonumber(ARGV[b + 2]),
      member = ARGV[b + 3], sig = ARGV[b + 4],
      state = KEYS[k + 2 * r - 1], stats = KEYS[k + 2 * r],
    }
  end
  k = k + 1 + 2 * n
  a = a + 3 + 5 * n
  if redis.call('SISMEMBER', paused, name) == 0 then
    local stored = redis.call('HGET', configured, name)
    if (stored or '') ~= believed then return {2, name, skipped} end
    if n == 0 then
      local job = take(queue)
      if job then return {1, queue, job, '', 0, skipped} end
      open[#open + 1] = queue
    elseif redis.call('LLEN', queue) == 0 then
      open[#open + 1] = queue
    else
      -- Checks: reads only. The time is read once, before the first write of the script.
      if not now then now = hl_now(ARGV[1]) end
      local all_ok, wait, busy, broken = true, 0, 0, false
      for _, l in ipairs(limits) do
        -- A state key of the wrong type (edited by hand) closes this queue alone.
        local fine, ok, w, extra = pcall(hl_check, l.kind, l.state, l.limit, l.period, 1, token, now)
        if not fine then
          broken = true
          break
        end
        l.ok, l.extra = ok, extra
        if not ok then
          all_ok = false
          if l.kind == 'c' then
            busy = 1
          elseif w > wait then
            wait = w
          end
        end
      end
      local counter = '@' .. name
      if broken then
        skipped[#skipped + 1] = name
        skipped[#skipped + 1] = 0
        skipped[#skipped + 1] = 2
      elseif all_ok then
        -- The job leaves its queue first (the first write: under OOM nothing happens), then
        -- every limit is charged.
        local job = take(queue)
        if job then
          local reclaimed = {}
          for i, l in ipairs(limits) do
            reclaimed[i] = hl_grant(l.kind, l.state, l.limit, l.period, 1, token, now, l.extra)
          end
          for i, l in ipairs(limits) do
            hl_touch(l.stats, index_key, l.member, l.sig, ttl, now, 'g')
            hl_count(l.stats, 'rc', reclaimed[i], ttl)
          end
          hl_minute(events_key, counter, 'g')
          -- Would the limits grant one more job now? If not, the process leaves the queue out
          -- until then instead of asking (and being refused).
          local more, next_wait, next_busy = 1, 0, 0
          for _, l in ipairs(limits) do
            local fine, ok, w = pcall(hl_check, l.kind, l.state, l.limit, l.period, 1, '', now)
            if fine and not ok then
              more = 0
              if l.kind == 'c' then
                next_busy = 1
              elseif w > next_wait then
                next_wait = w
              end
            end
          end
          -- Last: entries of identities whose stats expired.
          local ok, stale = pcall(redis.call, 'ZRANGEBYSCORE', index_key, '-inf', h_score(now), 'LIMIT', 0, 10)
          if ok and type(stale) == 'table' and #stale > 0 then
            pcall(redis.call, 'ZREM', index_key, unpack(stale))
          end
          return {1, queue, job, token, now, skipped, more, math.max(0, math.floor(next_wait)), next_busy}
        end
        open[#open + 1] = queue
      else
        for _, l in ipairs(limits) do
          if not l.ok then hl_touch(l.stats, index_key, l.member, l.sig, ttl, now, 'r') end
        end
        hl_minute(events_key, counter, 'r')
        skipped[#skipped + 1] = name
        skipped[#skipped + 1] = math.max(0, math.floor(wait))
        skipped[#skipped + 1] = busy
      end
    end
  end
end
return {0, open, skipped}
