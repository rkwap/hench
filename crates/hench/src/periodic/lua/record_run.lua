-- PERIODIC_RECORD_RUN: remembers how the latest execution of a schedule's job ended
-- (docs/design/periodic.md 2.10). Informational: sent after the job's outcome is written.
--
-- KEYS: [1] periodic:<name>.
-- ARGV: [1] the job's tick (ms), [2] status: ok | failed | interrupted | rescheduled, [3] the
--       error ('' = none), [4] the duration (ms), [5] the jid, [6] a token unique to this
--       record (a resend after a lost reply finds it and changes nothing).
-- Returns 1 recorded, 0 ignored (an older tick than the one recorded, a resend, or no such
-- schedule: a deleted schedule is never recreated).

if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
local s = redis.call('HMGET', KEYS[1], 'run_tick', 'run_token')
if s[2] == ARGV[6] then return 0 end
if s[1] and tonumber(s[1]) > tonumber(ARGV[1]) then return 0 end
redis.call('HSET', KEYS[1], 'run_tick', ARGV[1], 'run_status', ARGV[2], 'run_ms', ARGV[4],
  'run_at', h_now_ms(), 'run_jid', ARGV[5], 'run_token', ARGV[6])
if ARGV[3] ~= '' then
  redis.call('HSET', KEYS[1], 'run_error', ARGV[3])
else
  redis.call('HDEL', KEYS[1], 'run_error')
end
if ARGV[2] == 'failed' then
  redis.call('HINCRBY', KEYS[1], 'run_failures', 1)
end
return 1
