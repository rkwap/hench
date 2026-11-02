-- PUSH_BACK_UNIQUE (reliable fetch): replaces a job of the in-progress list by a new copy at
-- the back of a queue or in the schedule, like PUSH_BACK (an interrupted job; a job over a
-- limit or throttled, parked until its slot), and holds its lock in the same step: for `for`
-- plus the delay of a scheduled copy, whose score and member the lock records (a debounce push
-- may replace it). Assembled after the prelude, the unique library and the outcome helpers.
--
-- KEYS: [1] the in-progress list, [2] queues, [3] the destination (queue list or schedule),
--       [4] the lock, [5] unique-locks, [6] the events hash of the minute.
-- ARGV: [1] the job as fetched, [2] the new copy, [3] the queue's name (or the score), [4] queue
--       or schedule (as PUSH_BACK), then [5] op (hold|release), [6] jid, [7] digest, [8] class,
--       [9] queue, [10] until, [11] for (ms).
-- Returns {written (1, or 0 when the job was no longer in progress: nothing written), lock
-- status}.

local now_ms = h_now_ms()
if not redis.call('LPOS', KEYS[1], ARGV[1], 'RANK', -1) then
  return {0, 0}
end
local until_at, record, member = '', '', ''
if ARGV[4] == 'queue' then
  redis.call('SADD', KEYS[2], ARGV[3])
  redis.call('LPUSH', KEYS[3], ARGV[2])
else
  redis.call('ZADD', KEYS[3], ARGV[3], ARGV[2])
  until_at, record, member = ARGV[3], ARGV[3], ARGV[2]
end
local status = hu_lock_work(ARGV[5], KEYS[4], KEYS[5], KEYS[6], ARGV[6], ARGV[7], ARGV[8],
  ARGV[9], ARGV[10], tonumber(ARGV[11]), until_at, record, member, now_ms)
redis.call('LREM', KEYS[1], -1, ARGV[1])
return {1, status}
