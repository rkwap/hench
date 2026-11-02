-- PERIODIC_FIRE: enqueues the job of one tick and advances the schedule, at most once per tick
-- (docs/design/periodic.md 2.5, 2.10). Assembled after the prelude, the unique library and
-- the periodic library.
--
-- KEYS: [1] periodic (due index), [2] periodic:<name>, [3] periodic-history:<name>, [4] queues,
--       [5] the job's destination (its queue's list, or the schedule; any key when nothing is
--       written), [6] the job's lock unique:<digest> (any key when not unique), [7] unique-locks,
--       [8] the unique counters of the minute.
-- ARGV: [1] name, [2] the `next` read ('' = none), [3] the digest read, [4] the new `next`
--       ('' = none: the schedule leaves the due index), [5] the tick handled (epoch seconds;
--       '' = none), [6] mode: enqueued | scheduled | halted | skipped, [7] the job's JSON,
--       [8] its queue's name (enqueued) or its score (scheduled), [9] ticks missed,
--       [10] the history entry ('' = none), [11] the history entry if the job is a duplicate,
--       [12] the history cap, [13] the job's jid, [14] the process identity, [15] now (ms, the
--       poller's clock), then the lock of a unique job: [16] digest ('' = not unique),
--       [17] until, [18] for (ms), [19] its score when scheduled (or ''), [20] class, [21] queue.
-- Returns {code, detail}:
--   {1, ''} done as asked ({1, error}: the job was written without its lock);
--   {2, ''} the tick was not after `last_tick`: advanced, nothing enqueued (a clock jump);
--   {3, holder} a duplicate: the tick is consumed without a job (the lock is held);
--   {0, ''} the tick was handled already (`next` changed); {-1, ''} the definition changed;
--   {-2, ''} paused; {-3, ''} gone (its index entry is removed);
--   {-4, error} the lock cannot be read (a key of the wrong type): nothing written.
--
-- Every check comes before the first write, and the first write is the job's (or, for a
-- duplicate, a counter under pcall then the hash): under OOM nothing is written at all. A
-- resend after a lost reply finds `next` changed and writes nothing, so it takes no second
-- lock either.

local due, hash, history = KEYS[1], KEYS[2], KEYS[3]
local name, digest = ARGV[1], ARGV[3]
local new_next, tick, mode = ARGV[4], ARGV[5], ARGV[6]
local missed, cap = tonumber(ARGV[9]), tonumber(ARGV[12])
local jid, by, now = ARGV[13], ARGV[14], ARGV[15]
local unique = ARGV[16]

if redis.call('EXISTS', hash) == 0 then
  redis.call('ZREM', due, name)
  return {-3, ''}
end
local s = redis.call('HMGET', hash, 'next', 'digest', 'paused', 'last_tick')
if s[3] then return {-2, ''} end
if s[2] ~= digest then return {-1, ''} end
if (s[1] or '') ~= ARGV[2] then return {0, ''} end

local code, detail, entry = 1, '', ARGV[10]
if tick ~= '' and s[4] and tonumber(tick) <= tonumber(s[4]) then
  -- Never enqueue an instant handled already, nor move `last_tick` backwards.
  mode, tick, code, entry = 'guard', '', 2, ''
end

local written = mode == 'enqueued' or mode == 'scheduled'
local locked_at = nil
if written then
  local function write() hp_write(KEYS[4], KEYS[5], mode, ARGV[7], ARGV[8]) end
  if unique ~= '' then
    local now_ms = h_now_ms()
    local for_ms = tonumber(ARGV[18])
    local fields = {'class', ARGV[20], 'queue', ARGV[21], 'until', ARGV[17],
      'for', tostring(math.floor(for_ms / 1000))}
    if ARGV[19] ~= '' then
      -- Waiting in the schedule: the lock records its member there (a debounce push finds it).
      fields[#fields + 1] = 'm'
      fields[#fields + 1] = ARGV[7]
    end
    local r = hu_push_one(KEYS[6], KEYS[7], KEYS[8], unique, jid, ARGV[20],
      hp_lock_ttl(for_ms, ARGV[19], now_ms), now_ms, fields, ARGV[19], write)
    if r[1] == -1 then return {-4, r[2]} end
    if r[1] == 0 then
      mode, detail, code, entry, written = 'duplicate', r[2], 3, ARGV[11], false
    else
      -- {1}: written with its lock; {4, error}: written without it; {2, jid}: written by an
      -- earlier attempt of this jid (impossible behind the check of `next`).
      if r[1] == 4 then detail = r[2] end
      locked_at = now_ms
    end
  else
    write()
  end
end

if written then
  redis.call('HSET', hash, 'last_tick', tick, 'last_at', now, 'last_jid', jid, 'last_by', by,
    'last_result', mode)
  redis.call('HINCRBY', hash, 'fired', 1)
elseif tick ~= '' then
  -- Halted by client middleware, or a duplicate: the tick is consumed without a job.
  redis.call('HSET', hash, 'last_tick', tick, 'last_at', now, 'last_by', by, 'last_result', mode)
  redis.call('HDEL', hash, 'last_jid')
  redis.call('HINCRBY', hash, 'skipped', 1)
elseif code == 2 then
  redis.call('HINCRBY', hash, 'skipped', 1)
end
if missed > 0 then
  redis.call('HINCRBY', hash, 'missed', missed)
end
if new_next ~= '' then
  redis.call('HSET', hash, 'next', new_next)
  redis.call('ZADD', due, new_next, name)
else
  redis.call('HDEL', hash, 'next')
  redis.call('ZREM', due, name)
end
redis.call('HDEL', hash, 'error', 'error_at')
hp_history(history, entry, cap)
if locked_at then
  pcall(hu_trim, KEYS[7], locked_at, 10)
end
return {code, detail}
