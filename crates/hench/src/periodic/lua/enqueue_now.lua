-- PERIODIC_ENQUEUE_NOW: pushes a schedule's job outside its timetable ("enqueue now"); the
-- timetable is not touched (docs/design/periodic.md 2.5). Assembled after the prelude, the
-- unique library and the periodic library.
--
-- KEYS: [1] periodic:<name>, [2] periodic-history:<name>, [3] queues, [4] the job's
--       destination (any key when nothing is written), [5] the job's lock (any key when not
--       unique), [6] unique-locks, [7] the unique counters of the minute.
-- ARGV: [1] mode: enqueued | scheduled | halted, [2] the job's JSON, [3] its queue's name
--       (enqueued) or its score (scheduled), [4] the history entry, [5] the history entry if
--       the job is a duplicate, [6] the history cap, [7] now (ms), [8] the job's jid, then the
--       lock of a unique job: [9] digest ('' = not unique), [10] until, [11] for (ms), [12] its
--       score when scheduled (or ''), [13] class, [14] queue.
-- Returns {1, ''} done ({1, error}: written without its lock), {2, jid} written already by an
-- earlier attempt (a resend), {3, holder} a duplicate (nothing written), {0, ''} no such
-- schedule, {-4, error} the lock cannot be read (nothing written).
--
-- Not idempotent without a lock: the caller sends it once, and on an error reads
-- `manual_jid` to learn whether it ran.

if redis.call('EXISTS', KEYS[1]) == 0 then return {0, ''} end
local mode, jid = ARGV[1], ARGV[8]
local code, detail, entry = 1, '', ARGV[4]
local written = mode == 'enqueued' or mode == 'scheduled'
local locked_at = nil
if written then
  local function write() hp_write(KEYS[3], KEYS[4], mode, ARGV[2], ARGV[3]) end
  if ARGV[9] ~= '' then
    local now_ms = h_now_ms()
    local for_ms = tonumber(ARGV[11])
    local fields = {'class', ARGV[13], 'queue', ARGV[14], 'until', ARGV[10],
      'for', tostring(math.floor(for_ms / 1000))}
    if ARGV[12] ~= '' then
      -- Waiting in the schedule: the lock records its member there (a debounce push finds it).
      fields[#fields + 1] = 'm'
      fields[#fields + 1] = ARGV[2]
    end
    local r = hu_push_one(KEYS[5], KEYS[6], KEYS[7], ARGV[9], jid, ARGV[13],
      hp_lock_ttl(for_ms, ARGV[12], now_ms), now_ms, fields, ARGV[12], write)
    if r[1] == -1 then return {-4, r[2]} end
    if r[1] == 2 then return {2, r[2]} end
    if r[1] == 0 then
      code, detail, entry, written = 3, r[2], ARGV[5], false
    else
      if r[1] == 4 then detail = r[2] end
      locked_at = now_ms
    end
  else
    write()
  end
end
if written then
  redis.call('HSET', KEYS[1], 'manual_at', ARGV[7], 'manual_jid', jid)
else
  redis.call('HSET', KEYS[1], 'manual_at', ARGV[7])
  redis.call('HDEL', KEYS[1], 'manual_jid')
end
hp_history(KEYS[2], entry, tonumber(ARGV[6]))
if locked_at then
  pcall(hu_trim, KEYS[6], locked_at, 10)
end
return {code, detail}
