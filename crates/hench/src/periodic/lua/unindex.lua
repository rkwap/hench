-- PERIODIC_UNINDEX: removes a schedule from the due index when it is deleted or paused, checked
-- in the same step (docs/design/periodic.md 2.6): a resume or a re-creation that lands after the
-- poller read the hash keeps its entry (a separate ZREM used to drop it, and the schedule then
-- never fired).
--
-- KEYS: [1] periodic (the due index), [2] periodic:<name>.
-- ARGV: [1] the schedule's name.
-- Returns 1 when the entry was removed, 0 when the schedule is active (or had no entry).

if redis.call('EXISTS', KEYS[2]) == 1 and redis.call('HEXISTS', KEYS[2], 'paused') == 0 then
  return 0
end
return redis.call('ZREM', KEYS[1], ARGV[1])
