-- BATCH_COMMIT: writes a batch definition in one step (docs/design/batches.md 2.5.3): new
-- batches (a root and its children) or an existing batch reopened with new children, their
-- members pushed to their queues or the schedule and registered, and the locks of unique
-- members, taken with the unique jobs' frozen library. Applied at most once per commit id (the
-- root's commits); every check runs before the first write, a batch hash or a job, so that
-- under OOM nothing at all is written.
--
-- KEYS: [1] queues, [2] schedule, [3] batches (the index), [4] batch-events (the outbox), [5]
--       the batch counters of the minute, then six keys per batch of the commit (the root first,
--       then the children in definition order: a parent always comes before its children):
--       hash, pending, failed, dead, kids, commits; then the destination queue lists; then, when
--       a member is unique: unique-locks, the unique counters of the minute and one lock key per
--       distinct digest.
-- ARGV: [1] commit id, [2] number of batches, then per batch: mode (new|open), bid, parent
--       (1-based index of an earlier batch of this commit, 0 for none), ttl (s), linger (s),
--       description, the complete, success and death callback templates (JSON arrays, or ''),
--       the number of extra fields, and those field/value pairs (written to a reopened batch
--       too: the crate's own fields, such as a workflow releasing its hold `open`); then the
--       number of jobs and 6 values per job: batch index, jid, KEYS index of its destination,
--       kind (q|s), queue name (q) or score (s), the JSON as stored; then the number of unique
--       members, and when there are some the KEYS index of unique-locks followed by 8 values
--       each: job index, KEYS index of its lock, digest, class, queue, until, for (ms),
--       on_conflict (drop|error).
-- Returns {'ok', {bids of the batches that fired (new ones, or a reopened one whose hold this
-- commit released), deepest first}, {jid, holder, ...} of the
-- unique duplicates left out, {jid, error, ...} of the members written without their lock},
-- {'already', duplicates} when the commit id was applied before (the duplicates that commit
-- left out, as a JSON array, or ''), or an error reply: NOBATCH <bid>, SUCCEEDED <bid>,
-- CANCELLED <bid>, EXISTS <bid>, DUPLICATE <job index> <holder>, WRONGTYPE <key>, ERR ....

local cid, nb = ARGV[1], tonumber(ARGV[2])
local now = h_now_ms()

-- The batches.
local B = {}
local a = 3
for i = 1, nb do
  local k = 5 + (i - 1) * 6
  local b = {
    mode = ARGV[a], bid = ARGV[a + 1], parent = tonumber(ARGV[a + 2]),
    ttl = tonumber(ARGV[a + 3]), linger = tonumber(ARGV[a + 4]), desc = ARGV[a + 5],
    cbc = ARGV[a + 6], cbs = ARGV[a + 7], cbd = ARGV[a + 8], extra = {},
    keys = {KEYS[k + 1], KEYS[k + 2], KEYS[k + 3], KEYS[k + 4], KEYS[k + 5], KEYS[k + 6]},
    added = 0, horizon = 0,
  }
  local nx = tonumber(ARGV[a + 9])
  for x = 1, 2 * nx do b.extra[x] = ARGV[a + 9 + x] end
  a = a + 10 + 2 * nx
  B[i] = b
end
local root = B[1]

-- The jobs.
local J = {}
local nj = tonumber(ARGV[a])
a = a + 1
for j = 1, nj do
  local o = a + (j - 1) * 6
  J[j] = {
    b = tonumber(ARGV[o]), jid = ARGV[o + 1], dest = KEYS[tonumber(ARGV[o + 2])],
    kind = ARGV[o + 3], target = ARGV[o + 4], json = ARGV[o + 5],
  }
end
a = a + nj * 6

-- The unique members.
local U = {}
local nu = tonumber(ARGV[a])
a = a + 1
local ulocks, uevents
if nu > 0 then
  local index = tonumber(ARGV[a])
  ulocks, uevents = KEYS[index], KEYS[index + 1]
  a = a + 1
  for u = 1, nu do
    local o = a + (u - 1) * 8
    local m = {
      job = tonumber(ARGV[o]), lock = KEYS[tonumber(ARGV[o + 1])], digest = ARGV[o + 2],
      class = ARGV[o + 3], queue = ARGV[o + 4], policy = ARGV[o + 5],
      for_ms = tonumber(ARGV[o + 6]), conflict = ARGV[o + 7],
    }
    U[u] = m
    J[m.job].unique = m
  end
end

-- 1. Idempotency: the root's commits record every commit id applied with it as the root.
local applied = redis.call('HGET', root.keys[6], cid)
if applied then return {'already', applied} end

-- 2. Checks, before any write.
if hb_wrong_type(KEYS[1], 'set') then return redis.error_reply('WRONGTYPE ' .. KEYS[1]) end
for i = 2, 4 do
  if hb_wrong_type(KEYS[i], 'zset') then return redis.error_reply('WRONGTYPE ' .. KEYS[i]) end
end
for i, b in ipairs(B) do
  if b.mode == 'open' then
    local h = redis.call('HMGET', b.keys[1], 'v', 'success_at', 'cancelled_at', 'ttl')
    if not h[1] then return redis.error_reply('NOBATCH ' .. b.bid) end
    if h[2] then return redis.error_reply('SUCCEEDED ' .. b.bid) end
    if h[3] then return redis.error_reply('CANCELLED ' .. b.bid) end
    b.ttl = tonumber(h[4] or b.ttl)
    if b.parent ~= 0 then return redis.error_reply('ERR a reopened batch has no parent index') end
    for _, x in ipairs({2, 5}) do
      if hb_wrong_type(b.keys[x], 'set') then return redis.error_reply('WRONGTYPE ' .. b.keys[x]) end
    end
  else
    if redis.call('EXISTS', unpack(b.keys)) > 0 then return redis.error_reply('EXISTS ' .. b.bid) end
    if b.parent < 0 or b.parent >= i then
      return redis.error_reply('ERR the parent of a batch must come before it')
    end
  end
end
if hb_wrong_type(root.keys[6], 'hash') then return redis.error_reply('WRONGTYPE ' .. root.keys[6]) end
local checked = {}
for _, job in ipairs(J) do
  if not checked[job.dest] then
    local want = (job.kind == 'q') and 'list' or 'zset'
    if hb_wrong_type(job.dest, want) then return redis.error_reply('WRONGTYPE ' .. job.dest) end
    checked[job.dest] = true
  end
end
-- Unique members: a member is a duplicate (left out) when another job holds its lock, when an
-- earlier member of this commit has its digest, or when its own jid holds the lock (that job is
-- already in Redis, outside this batch). With on_conflict = error, the first duplicate refuses
-- the whole commit.
local dups, dup_classes = {}, {}
local seen = {}
for _, m in ipairs(U) do
  local job = J[m.job]
  local ok, state = pcall(hu_check, m.lock, job.jid)
  if not ok then return redis.error_reply('WRONGTYPE ' .. m.lock) end
  local holder = nil
  if state == 'self' then
    holder = job.jid
  elseif state ~= 'free' then
    holder = state
  elseif seen[m.digest] then
    holder = seen[m.digest]
  end
  if holder then
    if m.conflict == 'error' then
      return redis.error_reply('DUPLICATE ' .. m.job .. ' ' .. holder)
    end
    job.dup = holder
    dups[#dups + 1] = job.jid
    dups[#dups + 1] = holder
    dup_classes[#dup_classes + 1] = m.class
  else
    seen[m.digest] = job.jid
  end
end

-- 3. Writes: the new batches first (under OOM the first write is refused and nothing changes).
local created = 0
for _, b in ipairs(B) do
  if b.mode == 'new' then
    local fields = {'v', 1, 'created_at', now, 'ttl', b.ttl, 'linger', b.linger, 'total', 0,
      'skipped', 0, 'kids', 0, 'kc', 0, 'ks', 0, 'touched_at', now}
    if b.desc ~= '' then fields[#fields + 1] = 'desc'; fields[#fields + 1] = b.desc end
    if b.cbc ~= '' then fields[#fields + 1] = 'cb_complete'; fields[#fields + 1] = b.cbc end
    if b.cbs ~= '' then fields[#fields + 1] = 'cb_success'; fields[#fields + 1] = b.cbs end
    if b.cbd ~= '' then fields[#fields + 1] = 'cb_death'; fields[#fields + 1] = b.cbd end
    if b.parent > 0 then fields[#fields + 1] = 'parent'; fields[#fields + 1] = B[b.parent].bid end
    for _, value in ipairs(b.extra) do fields[#fields + 1] = value end
    redis.call('HSET', b.keys[1], unpack(fields))
    created = created + 1
  else
    local fields = {'touched_at', now}
    for _, value in ipairs(b.extra) do fields[#fields + 1] = value end
    redis.call('HSET', b.keys[1], unpack(fields))
  end
end
for _, b in ipairs(B) do
  if b.mode == 'new' and b.parent > 0 then
    local p = B[b.parent]
    redis.call('SADD', p.keys[5], b.bid)
    redis.call('HINCRBY', p.keys[1], 'kids', 1)
  end
end
-- The members: each job is written, then its lock taken, then it is registered.
local queued = {}
local locked = false
local nolock = {}
for _, job in ipairs(J) do
  if not job.dup then
    local b = B[job.b]
    if job.kind == 'q' then
      if not queued[job.target] then
        redis.call('SADD', KEYS[1], job.target)
        queued[job.target] = true
      end
      redis.call('LPUSH', job.dest, job.json)
    else
      redis.call('ZADD', job.dest, job.target, job.json)
      local at = tonumber(job.target)
      if at > b.horizon then b.horizon = at end
    end
    local m = job.unique
    if m then
      -- The lock lives `for`, plus the delay of a scheduled member.
      local ttl, at = m.for_ms, ''
      if job.kind == 's' then
        at = job.target
        local delay = math.floor(tonumber(job.target) * 1000) - now
        if delay > 0 then ttl = ttl + delay end
      end
      local fields = {'class', m.class, 'queue', m.queue, 'until', m.policy,
        'for', tostring(math.floor(m.for_ms / 1000))}
      local took, err = pcall(hu_take, m.lock, ulocks, m.digest, job.jid, ttl, now, fields, at)
      if took then
        locked = true
      else
        nolock[#nolock + 1] = job.jid
        nolock[#nolock + 1] = h_err(err)
      end
    end
    b.added = b.added + redis.call('SADD', b.keys[2], job.jid)
  end
end
for _, b in ipairs(B) do
  if b.added > 0 then redis.call('HINCRBY', b.keys[1], 'total', b.added) end
end

-- 4. Expiry: every key of a batch lives max(ttl, the latest scheduled member + ttl, its current
-- lifetime); a parent lives at least as long as each of its children (deepest first).
local ttl = {}
for i = nb, 1, -1 do
  local b = B[i]
  local t = b.ttl * 1000
  if b.horizon > 0 then t = math.max(t, math.floor(b.horizon * 1000) - now + b.ttl * 1000) end
  t = math.max(t, ttl[i] or 0, redis.call('PTTL', b.keys[1]))
  ttl[i] = t
  if b.parent > 0 then ttl[b.parent] = math.max(ttl[b.parent] or 0, t) end
end
for i, b in ipairs(B) do
  for _, key in ipairs(b.keys) do redis.call('PEXPIRE', key, ttl[i]) end
  redis.call('ZADD', KEYS[3], hb_secs(now + ttl[i]), b.bid)
end
-- The commit id, with the duplicates it left out (what a resend after a lost reply reports).
local record = ''
if #dups > 0 then record = cjson.encode(dups) end
redis.call('HSET', root.keys[6], cid, record)
hb_inherit_ttl(root.keys[1], root.keys[6])

-- 5. New batches without member or child (duplicates excluded) fire at once, deepest first; a
-- reopened batch may fire when this commit released its hold.
local fired_bids, fired = {}, {}
for i = nb, 1, -1 do
  local b = B[i]
  local events = hb_evaluate(b.keys[1], b.keys[2], b.keys[3], KEYS[4], b.bid, now)
  if #events > 0 then
    fired_bids[#fired_bids + 1] = b.bid
    for _, event in ipairs(events) do fired[#fired + 1] = event end
  end
end

-- 6. Bookkeeping, then the index trim (a removal) last.
if created > 0 then pcall(hb_count, KEYS[5], 'created', created) end
hb_count_events(KEYS[5], fired)
for _, class in ipairs(dup_classes) do pcall(hu_count, uevents, class, 'd') end
if locked then pcall(hu_trim, ulocks, now, 10) end
return {'ok', fired_bids, dups, nolock}
