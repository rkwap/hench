-- hench unique jobs: the lock work of the outcome scripts (ACK_UNIQUE, SETTLE_UNIQUE,
-- PUSH_BACK_UNIQUE, UNIQUE_LOCK_OP), assembled after the prelude and the unique library.

-- The time to live of a held lock: `for`, plus how far `until_at` (a score: when the job runs
-- next, or '') is in the future.
local function hu_hold_ttl(for_ms, until_at, now_ms)
  local ttl = for_ms
  if until_at ~= '' then
    local delay = math.floor(tonumber(until_at) * 1000) - now_ms
    if delay > 0 then ttl = ttl + delay end
  end
  return ttl
end

-- Holds ('hold': keeps it at least `hu_hold_ttl`, records `record` as its `at` and `member` as
-- the job's member in the schedule, or takes it if free) or releases ('release') the lock of
-- `jid`, under pcall (rule 9 of the design: an unusable lock key must never abort an outcome
-- script between its write and its removal). `record` and `member` are '' unless the job waits
-- in the schedule. Returns the status of hu_hold (1, 2, 0) or hu_release (1, 0, -1), or -2 when
-- the lock key could not be used. A `success` job that does not hold its lock here counts a
-- lapse (z).
local function hu_lock_work(op, lock, index, events, jid, digest, class, queue, policy, for_ms, until_at, record, member, now_ms)
  local ok, status
  if op == 'hold' then
    local fields = {'class', class, 'queue', queue, 'until', policy, 'for', tostring(math.floor(for_ms / 1000))}
    if record ~= '' and member ~= '' then
      fields[#fields + 1] = 'm'
      fields[#fields + 1] = member
    end
    ok, status = pcall(hu_hold, lock, index, digest, jid, hu_hold_ttl(for_ms, until_at, now_ms), now_ms, fields, record)
  else
    ok, status = pcall(hu_release, lock, index, digest, jid)
  end
  if not ok then status = -2 end
  if policy == 'success' and status ~= 1 then pcall(hu_count, events, class, 'z') end
  return status
end
