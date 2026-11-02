-- LIMIT_ACQUIRE: checks every limit of a call, then grants all of them or none.
--
-- KEYS: per limit i: [2i-1] its state, [2i] its stats hash; then [2n+1] the `limits` index and
--   [2n+2] the minute's counters (l|YYYYMMDD|H:MM, the caller's clock).
-- ARGV: [1] '' or a fixed time (ms, tests only), [2] the token ('' without concurrency limits),
--   [3] n, [4] the ticket (the slot, in ms, of the place an earlier refusal gave this caller) or
--   '', [5] flags (L: keep a wait line; N: no bookkeeping), [6] ms waited so far ('' or a
--   number), [7] how far ahead (ms) a slot may be for the caller to take a place in the line
--   ('': any, the caller comes back whatever its slot); then 9 per limit: kind (c, w, b, g),
--   limit, period ms (the lease for c), cost, stats TTL ms, index member (<kind>:<id>), name
--   (for the minute counters), signature, default spacing ms (concurrency: the spacing used
--   until hold times are measured).
-- Granted: {1, reclaimed leases, 1 if the token already held a live lease, the signature stored
--   before for a limit whose signature differs ('' when none), now}.
-- Refused: {0, wait ms, index of the limit that decided the wait, 1 if the cost can never be
--   granted, the slot (ms; -1 when impossible), now, place, the signature stored before for a
--   refusing limit whose signature differs ('' when none)}: place 1 when the refusal gave the
--   caller a new place in the line (the slot is its ticket from now on), 2 when the caller kept
--   the place of its ticket, 0 when it holds none (no line, or a slot further than it waits).

local now = hl_now(ARGV[1])
local token, n, ticket, flags = ARGV[2], tonumber(ARGV[3]), ARGV[4], ARGV[5]
local waited = tonumber(ARGV[6]) or 0
local within = tonumber(ARGV[7])
local index_key, events_key = KEYS[2 * n + 1], KEYS[2 * n + 2]
local keep_stats = not hl_flag(flags, 'N')
local keep_line = keep_stats and hl_flag(flags, 'L')

local limits = {}
for i = 1, n do
  local b = 7 + (i - 1) * 9
  limits[i] = {
    kind = ARGV[b + 1], limit = tonumber(ARGV[b + 2]), period = tonumber(ARGV[b + 3]),
    cost = tonumber(ARGV[b + 4]), ttl = tonumber(ARGV[b + 5]), member = ARGV[b + 6],
    name = ARGV[b + 7], sig = ARGV[b + 8], spacing = tonumber(ARGV[b + 9]) or 25,
    state = KEYS[2 * i - 1], stats = KEYS[2 * i],
  }
end

-- Checks: reads only.
local all_ok, held = true, 0
for _, l in ipairs(limits) do
  local ok, wait, extra = hl_check(l.kind, l.state, l.limit, l.period, l.cost, token, now)
  l.ok, l.wait, l.extra = ok, wait, extra
  if not ok then all_ok = false end
  if extra == 'held' then held = 1 end
end

if all_ok then
  -- Grants: the first write is the first grant.
  local reclaimed = {}
  local total = 0
  for i, l in ipairs(limits) do
    reclaimed[i] = hl_grant(l.kind, l.state, l.limit, l.period, l.cost, token, now, l.extra)
    total = total + reclaimed[i]
  end
  local mismatch = ''
  if keep_stats then
    local counted = {}
    for i, l in ipairs(limits) do
      local previous = hl_touch(l.stats, index_key, l.member, l.sig, l.ttl, now, 'g')
      if previous and mismatch == '' then mismatch = previous end
      if waited > 0 then
        hl_count(l.stats, 'w', 1, l.ttl)
        hl_count(l.stats, 'wms', math.floor(waited), l.ttl)
      end
      hl_count(l.stats, 'rc', reclaimed[i], l.ttl)
      if not counted[l.name] then
        counted[l.name] = true
        hl_minute(events_key, l.name, 'g')
      end
    end
    -- Last: entries of identities whose stats expired (a removal; after it, writes would no
    -- longer be checked against the memory limit).
    local ok, stale = pcall(redis.call, 'ZRANGEBYSCORE', index_key, '-inf', h_score(now), 'LIMIT', 0, 10)
    if ok and type(stale) == 'table' and #stale > 0 then
      pcall(redis.call, 'ZREM', index_key, unpack(stale))
    end
  end
  return {1, total, held, mismatch, now}
end

-- Refused. Each failing limit's own wait, and the spacing one more waiter adds to its line:
-- period x cost / limit for rate limits; for concurrency the recent hold time / limit
-- (measured from releases: the decaying average `hold`, else the 24-hour average), else the
-- caller's default. A rate limit's wait is exact (`earliest`, the time before which no unit
-- frees); a concurrency limit's is an estimate (a slot may free at any time).
local own, limiting, earliest, widest = -1, 1, 0, 0
local rate_only = true
for i, l in ipairs(limits) do
  if not l.ok then
    if l.wait < 0 then return {0, -1, i, 1, -1, now, 0, ''} end
    if l.kind == 'c' then
      rate_only = false
      local spacing = l.spacing
      if keep_stats then
        local ok, h = pcall(redis.call, 'HMGET', l.stats, 'hold', 'h', 'hms')
        if ok and type(h) == 'table' then
          local recent, holds, held_ms = tonumber(h[1]), tonumber(h[2]), tonumber(h[3])
          if recent and recent > 0 then
            spacing = recent / l.limit
          elseif holds and holds > 0 and held_ms then
            spacing = held_ms / holds / l.limit
          end
        end
      end
      l.spacing = math.max(1, spacing)
      l.wait = l.spacing
    else
      l.spacing = l.period * l.cost / l.limit
      if l.wait > earliest then earliest = l.wait end
    end
    if l.spacing > widest then widest = l.spacing end
    if l.wait > own then own = l.wait; limiting = i end
  end
end

local start = now + own
local place = 0
if keep_line then
  local slot = tonumber(ticket)
  if slot and slot >= now + earliest then
    -- Still before the slot it was given (and no rate limit frees later): same place, the
    -- line untouched, nothing counted.
    return {0, math.max(0, math.floor(slot - now)), limiting, 0, math.floor(slot), now, 2, ''}
  end
  if slot and rate_only and slot >= now - math.max(100, 2 * widest) then
    -- Back for its place a little late (a timer, a scheduler's latency), and the units of
    -- its slot went to others: the next unit that frees is its, still without a new place
    -- (its place was counted when it was given; queueing again would put it behind everyone
    -- who came meanwhile). Rate limits only: their next unit's time is exact.
    return {0, math.max(0, math.floor(own)), limiting, 0, math.floor(now + own), now, 2, ''}
  end
  -- A place at the end of the line of every failing limit, for a caller that will come back
  -- for it (the others are told when the end of the line is, and the line stays as it is).
  for _, l in ipairs(limits) do
    if not l.ok then
      local ok, line = pcall(redis.call, 'HGET', l.stats, 'line')
      l.line = (ok and tonumber(line)) or 0
      if l.line > start then start = l.line end
    end
  end
  if not within or start <= now + within then
    place = 1
    for _, l in ipairs(limits) do
      if not l.ok then
        pcall(redis.call, 'HSET', l.stats, 'line', math.floor(math.max(l.line, start + l.spacing)))
      end
    end
  end
end
local mismatch = ''
if keep_stats then
  local counted = {}
  for _, l in ipairs(limits) do
    if not l.ok then
      local previous = hl_touch(l.stats, index_key, l.member, l.sig, l.ttl, now, 'r')
      if previous and mismatch == '' then mismatch = previous end
      if not counted[l.name] then
        counted[l.name] = true
        hl_minute(events_key, l.name, 'r')
      end
    end
  end
end
return {0, math.max(0, math.floor(start - now)), limiting, 0, math.floor(start), now, place, mismatch}
