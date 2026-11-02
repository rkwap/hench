-- UNIQUE_RELEASE_MANY: releases many locks at once (the jobs `kill_all` moved to the morgue;
-- `UniqueLocks::release_all`; a forced release). Each lock is released when its holder is the
-- jid given ('' = any holder) and, with a class filter, the lock's class is that class. An
-- index entry whose lock is gone is removed. A key that cannot be used is skipped. Assembled
-- after the prelude and the unique library.
--
-- KEYS: [1] unique-locks, then one lock per entry.
-- ARGV: three values per entry: jid ('' = any holder), digest, class ('' = any class).
-- Returns how many locks were released.

local function release(lock, jid, digest, class)
  local fields = redis.call('HMGET', lock, 'jid', 'class')
  local holder = fields[1]
  if not holder then
    redis.call('ZREM', KEYS[1], digest)
    return 0
  end
  if (jid == '' or holder == jid) and (class == '' or fields[2] == class) then
    redis.call('DEL', lock)
    redis.call('ZREM', KEYS[1], digest)
    return 1
  end
  return 0
end

local released = 0
for i = 2, #KEYS do
  local b = (i - 2) * 3
  local ok, count = pcall(release, KEYS[i], ARGV[b + 1], ARGV[b + 2], ARGV[b + 3])
  if ok then released = released + count end
end
return released
