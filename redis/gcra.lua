-- GCRA (Generic Cell Rate Algorithm) rate limiter.
--
-- One atomic EVAL, one Redis round trip per check (constraint 1). The current
-- time is derived from Redis's own TIME so every replica shares one
-- authoritative time base and the caller's clock can never skew the bucket.
--
-- Bucket form of GCRA:
--   bucket capacity = burst
--   one token refills every `interval` ms, where interval = period / rate
--   stored state  = TAT (theoretical arrival time) of the next token, in
--                   epoch milliseconds
--   tokens(t)     = burst - (TAT - t) / interval
--
-- TAT is the only thing stored, and it is a single plain number, so the state
-- is small, orderable, and safe to reset (constraint 6) by simply deleting
-- the key: a missing key is indistinguishable from a fresh bucket. Losing it
-- to a failover likewise just resets to a fresh bucket.
--
-- KEYS[1]  gcra:{client_id}
-- ARGV[1]  burst           bucket capacity in tokens
-- ARGV[2]  rate            tokens per period
-- ARGV[3]  period_seconds  length of one rate period
-- ARGV[4]  cost            tokens consumed by this request (>= 1)
--
-- Returns {allowed, remaining, reset_at_ms, now_ms}:
--   allowed    1/0
--   remaining  floor of tokens in the bucket now, never negative
--   reset_at   on allow: when the bucket is full again (= the new TAT)
--              on deny:  when `cost` tokens are available again (Retry-After)
--   now        the script's own clock (Redis TIME), so callers can compute a
--              Retry-After window on the same clock the bucket was evaluated

local burst  = tonumber(ARGV[1])
local rate   = tonumber(ARGV[2])
local period = tonumber(ARGV[3])
local cost   = tonumber(ARGV[4])

-- Defensive only: the admin boundary (constraint 13) rejects these before they
-- reach Redis. Guarding here keeps the script total no matter the caller.
if burst < 1 or rate <= 0 or period <= 0 or cost < 1 then
  return { 0, 0, 0, 0 }
end

-- Fractional interval keeps precision for rates that don't divide the period
-- evenly (e.g. 1000 req/hour -> 3.6ms per token) instead of truncating.
local interval = (period * 1000) / rate

local t = redis.call('TIME')
local now = (t[1] * 1000) + math.floor(t[2] / 1000)

local tat = tonumber(redis.call('GET', KEYS[1]) or '0')
-- A missing key, or one whose refill finished long ago, has a TAT in the
-- past: the bucket is full, so pin the TAT to now rather than carrying a
-- stale timestamp forward.
if tat < now then
  tat = now
end

local available = burst - ((tat - now) / interval)

if available < cost then
  -- Deny without mutating state. The wait until `cost` tokens exist is
  -- (cost - available) intervals, measured from now.
  local remaining = available
  if remaining < 0 then
    remaining = 0
  end
  local reset_at = now + ((cost - available) * interval)
  if reset_at < now then
    reset_at = now
  end
  return { 0, math.floor(remaining), math.floor(reset_at), now }
end

-- Allow: push the TAT forward by the cost of this request, atomically.
local new_tat = tat + (cost * interval)
redis.call('SET', KEYS[1], tostring(new_tat))

local remaining = burst - ((new_tat - now) / interval)
if remaining < 0 then
  remaining = 0
end

return { 1, math.floor(remaining), math.floor(new_tat), now }
