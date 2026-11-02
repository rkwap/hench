-- BATCH_DISPATCH: delivers the events a batch fired and did not dispatch yet, in the order
-- death, complete, success (docs/design/batches.md 2.5.5): pushes each event's callback jobs
-- (stored templates; `enqueued_at` is added here), registers them as members of the parent for
-- a child batch and updates the parent (kc, ks, death), then evaluates the parent. After
-- `success`, or the `complete` of a cancelled batch with no member left pending, the batch is
-- finished: its keys keep only their linger. Guarded by the outbox entry and the `*_dt` fields:
-- a repeat, or a second process dispatching at the same time, changes nothing.
--
-- KEYS: [1] batch-events, [2] batches, [3] queues, [4..9] the batch (hash, pending, failed,
--       dead, kids, commits), [10..15] its parent's six keys (the batch's own when it has no
--       parent), [16] the batch counters of the minute, [17..] the callbacks' queue lists.
-- ARGV: [1] bid, [2] the parent's bid or '', [3] number of callback queues, then their names
--       (ARGV[3 + i] is the queue of KEYS[16 + i]).
-- Returns {dispatched (1/0), parent refreshed (1/0), parent in the outbox (1/0), the number of
-- callbacks pushed, the events dispatched (comma-separated), the events fired in the
-- parent...}.

local bid, pbid = ARGV[1], ARGV[2]
local now = h_now_ms()
if not redis.call('ZSCORE', KEYS[1], bid) then return {0, 0, 0, 0, ''} end
local C = {KEYS[4], KEYS[5], KEYS[6], KEYS[7], KEYS[8], KEYS[9]}
local P = {KEYS[10], KEYS[11], KEYS[12], KEYS[13], KEYS[14], KEYS[15]}
local h = redis.call('HMGET', C[1], 'death_at', 'complete_at', 'success_at',
  'death_dt', 'complete_dt', 'success_dt', 'parent', 'cancelled_at')
if (h[7] or '') ~= pbid then return redis.error_reply('ERR parent mismatch for batch ' .. bid) end
local has_parent = pbid ~= '' and redis.call('EXISTS', P[1]) == 1

-- Checks, before any write.
if hb_wrong_type(KEYS[3], 'set') then return redis.error_reply('WRONGTYPE ' .. KEYS[3]) end
if has_parent and hb_wrong_type(P[2], 'set') then
  return redis.error_reply('WRONGTYPE ' .. P[2])
end
local qkey = {}
for i = 1, tonumber(ARGV[3]) do qkey[ARGV[3 + i]] = KEYS[16 + i] end
local due = {}
for i, name in ipairs({'death', 'complete', 'success'}) do
  if h[i] and not h[i + 3] then
    local raw = redis.call('HGET', C[1], 'cb_' .. name)
    local templates = {}
    if raw then
      local ok, decoded = pcall(cjson.decode, raw)
      if not ok or type(decoded) ~= 'table' then
        return redis.error_reply('ERR unreadable ' .. name .. ' callbacks of batch ' .. bid)
      end
      templates = decoded
    end
    for _, t in ipairs(templates) do
      local key = qkey[t.q]
      if not key then return redis.error_reply('ERR undeclared callback queue ' .. tostring(t.q)) end
      if hb_wrong_type(key, 'list') then return redis.error_reply('WRONGTYPE ' .. key) end
    end
    due[#due + 1] = {name, templates}
  end
end

-- Writes.
local registered, pushed = 0, 0
local parent_fired, names = {}, {}
for _, d in ipairs(due) do
  local name, templates = d[1], d[2]
  names[#names + 1] = name
  for _, t in ipairs(templates) do
    pushed = pushed + 1
    redis.call('SADD', KEYS[3], t.q)
    redis.call('LPUSH', qkey[t.q], string.sub(t.j, 1, -2) .. ',"enqueued_at":' .. now .. '}')
    if has_parent then registered = registered + redis.call('SADD', P[2], t.jid) end
  end
  if has_parent then
    if name == 'complete' then
      redis.call('HINCRBY', P[1], 'kc', 1)
    elseif name == 'success' then
      redis.call('HINCRBY', P[1], 'ks', 1)
    elseif redis.call('HSETNX', P[1], 'death_at', now) == 1 then
      redis.call('ZADD', KEYS[1], 'NX', hb_secs(now), pbid)
      parent_fired[#parent_fired + 1] = 'death'
    end
  end
  redis.call('HSET', C[1], name .. '_dt', now)
  if name == 'success' then
    hb_finish(C, KEYS[2], bid, now)
  elseif name == 'complete' and h[8] then
    hb_finish_cancelled(C, KEYS[2], bid, now)
  end
end
redis.call('ZREM', KEYS[1], bid)
local out = {1, 0, 0, pushed, table.concat(names, ',')}
if has_parent then
  if registered > 0 then
    redis.call('HINCRBY', P[1], 'total', registered)
    hb_inherit_ttl(P[1], P[2])
  end
  for _, event in ipairs(hb_evaluate(P[1], P[2], P[3], KEYS[1], pbid, now)) do
    parent_fired[#parent_fired + 1] = event
  end
  out[2] = hb_touch(P, KEYS[2], pbid, now)
  out[3] = redis.call('ZSCORE', KEYS[1], pbid) and 1 or 0
  hb_count_events(KEYS[16], parent_fired)
  for _, event in ipairs(parent_fired) do out[#out + 1] = event end
end
return out
