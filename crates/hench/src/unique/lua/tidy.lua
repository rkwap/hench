-- UNIQUE_TIDY: removes from the index the entries whose lock is gone (expired, or released by
-- a script that could not reach the index). Checks each lock in the script, so a lock taken
-- again meanwhile keeps its entry. Never touches a lock. Assembled after the prelude and the
-- unique library.
--
-- KEYS: [1] unique-locks, then one lock per entry.
-- ARGV: the digest of each entry.
-- Returns how many entries were removed.

local removed = 0
for i = 2, #KEYS do
  if redis.call('EXISTS', KEYS[i]) == 0 then
    removed = removed + redis.call('ZREM', KEYS[1], ARGV[i - 1])
  end
end
return removed
