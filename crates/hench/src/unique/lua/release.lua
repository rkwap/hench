-- UNIQUE_RELEASE: releases the lock of a jid (a `start` job when its perform starts; a job
-- releasing its own lock early; an operator releasing one lock). Compares the holder: another
-- job's lock is never released. Assembled after the prelude and the unique library.
--
-- KEYS: [1] the lock, [2] unique-locks, [3] the events hash of the minute.
-- ARGV: [1] jid, [2] digest, [3] class, [4] '1' to count a lapse (z) when the jid did not hold
--       the lock, else '0'.
-- Returns 1 released, 0 no lock, -1 another job holds it.

local status = hu_release(KEYS[1], KEYS[2], ARGV[2], ARGV[1])
if ARGV[4] == '1' and status ~= 1 then pcall(hu_count, KEYS[3], ARGV[3], 'z') end
return status
