-- hench: helpers shared by every script of the pro features (unique jobs, periodic jobs,
-- batches, rate limiting), prepended before the feature libraries and the script's body.
-- Feature helpers carry their own prefix (hu_, hp_, hb_, hl_); these carry h_.

-- The current time in milliseconds, from Redis's clock. Read it once per script.
local function h_now_ms()
  local t = redis.call('TIME')
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

-- Epoch seconds with millisecond precision, as a sorted-set score.
local function h_score(ms)
  return string.format('%.3f', ms / 1000)
end

-- The message of an error caught with pcall.
local function h_err(e)
  if type(e) == 'table' and e.err then return e.err end
  return tostring(e)
end

-- Gives `key` an expiry of at least `ttl_ms`, never a shorter one (Redis 6.2 has no
-- PEXPIRE GT). A key without an expiry gets one: every key of the pro features expires.
-- Returns 1 if the expiry changed, 0 otherwise (or when the key does not exist).
local function h_extend_at_least(key, ttl_ms)
  local current = redis.call('PTTL', key)
  if current == -2 then return 0 end
  if current == -1 or current < ttl_ms then
    redis.call('PEXPIRE', key, ttl_ms)
    return 1
  end
  return 0
end
