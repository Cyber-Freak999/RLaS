-- Single-round-trip /v1/check script.
--
-- Replaces the previous four-RTT happy path (HGET api_keys -> GET
-- client_limits -> EVAL gcra -> XADD stream) with one atomic EVAL. That four
-- trip sequence is what kept p95 far above the <10ms target: every /check paid
-- four sequential Redis round trips, and no amount of client-side pipelining
-- could collapse it because the later keys depend on the client_id returned by
-- the first HGET. Doing auth, limit lookup/validation, GCRA, and the approved
-- event push inside one script is the only way to satisfy constraint 1's
-- literal "one Redis round trip per check" while keeping check-and-consume
-- atomic.
--
-- KEYS[1]  api_keys hash name        e.g. "api_keys"
-- KEYS[2]  client_limits key prefix  e.g. "client_limits:"
-- KEYS[3]  gcra state key prefix     e.g. "gcra:"
-- KEYS[4]  approved-request stream   e.g. "stream:approved_requests"
-- ARGV[1]  sha256(api_key)           lookup field in KEYS[1]
-- ARGV[2]  cost                      tokens consumed (>= 1, constraint 7)
-- ARGV[3]  stream MAXLEN             XADD trimming target (constraint 10)
--
-- Returns {status, remaining, reset_at_ms, now_ms, client_id, rate, period, burst, stream_ok}:
--   status  1 = allowed   (bucket consumed, stream event push attempted)
--           0 = denied    (bucket intact, no stream event)
--          -1 = unauthorized        (key not in api_keys)
--          -2 = limits not found    (client_limits:{id} missing)
--          -3 = limits invalid      (unparseable or out-of-range config)
--          -4 = bad parameters      (cost < 1; the HTTP boundary rejects this)
--   stream_ok 1 = push succeeded or not attempted; 0 = allowed but the XADD
--                failed (the limiter logs stream_xadd_failed, nothing else).
--                Only meaningful when status == 1; all other paths return 1
--                because no push was expected.
--
-- The client_limits / api_keys keys are still the only copy of who's allowed
-- to call (constraint 9) and the gcra state key keeps the exact
-- gcra:{client_id} format the control-plane resets by DEL on a limit change
-- (constraint 6) — the script builds it from KEYS[3] + the resolved client_id.

local apiKeys  = KEYS[1]
local limPref  = KEYS[2]
local gcraPref = KEYS[3]
local stream   = KEYS[4]

local keyHash  = ARGV[1]
local cost     = tonumber(ARGV[2])
local maxlen   = tonumber(ARGV[3])
if maxlen == nil or maxlen < 1 then
  maxlen = 100000
end

-- Defensive only: constraint 13 rejects cost < 1 at the HTTP boundary. The
-- guard keeps the script total no matter the caller.
if cost < 1 then
  return { -4, 0, 0, 0, '', 0, '', 0, 1 }
end

local t = redis.call('TIME')
local now = (t[1] * 1000) + math.floor(t[2] / 1000)

local client_id = redis.call('HGET', apiKeys, keyHash)
if not client_id then
  return { -1, 0, 0, now, '', 0, '', 0, 1 }
end

local raw = redis.call('GET', limPref .. client_id)
if not raw then
  return { -2, 0, 0, now, client_id, 0, '', 0, 1 }
end

local ok, limits = pcall(cjson.decode, raw)
if not ok or type(limits) ~= 'table' then
  return { -3, 0, 0, now, client_id, 0, '', 0, 1 }
end

-- Stored values are int64 JSON from the control-plane (constraint 13), but a
-- hand-edited or otherwise malformed entry must yield limits-invalid, not a
-- Lua runtime error that the wrapper would misread as a Redis outage.
local burst  = tonumber(limits.burst)
local rate   = tonumber(limits.rate)
local period = limits.period
if burst == nil or rate == nil or type(period) ~= 'string' then
  return { -3, 0, 0, now, client_id, 0, '', 0, 1 }
end

-- Mirror Limits.Valid() (constraint 13): rate > 0, burst >= 1, period enum.
if rate <= 0 or burst < 1 or
   (period ~= 'second' and period ~= 'minute' and period ~= 'hour') then
  return { -3, 0, 0, now, client_id, 0, '', 0, 1 }
end

local periodSec = 1
if period == 'minute' then
  periodSec = 60
elseif period == 'hour' then
  periodSec = 3600
end

-- GCRA core — identical math to gcra.lua (which stays as the pure-algorithm
-- reference; see the comment there for the bucket/TAT derivation).
local interval = (periodSec * 1000) / rate

local gcraKey = gcraPref .. client_id
local tat = tonumber(redis.call('GET', gcraKey) or '0')
if tat < now then
  tat = now
end

local available = burst - ((tat - now) / interval)

if available < cost then
  local remaining = available
  if remaining < 0 then
    remaining = 0
  end
  local reset_at = now + ((cost - available) * interval)
  if reset_at < now then
    reset_at = now
  end
  return { 0, math.floor(remaining), math.floor(reset_at), now, client_id, rate, period, burst, 1 }
end

local new_tat = tat + (cost * interval)
redis.call('SET', gcraKey, tostring(new_tat))

local remaining = burst - ((new_tat - now) / interval)
if remaining < 0 then
  remaining = 0
end

-- Commit the approved event in the same atomic script as the bucket
-- consumption (constraint 5). The old separate XADD left a crash window
-- between the EVAL and the XADD where a consumed token produced no analytics
-- event; here they can never diverge.
--
-- The push is deliberately NON-FATAL: it is pcall-guarded so a telemetry
-- failure can never turn an allowed request into a degraded /check or a
-- deny. stream_ok=0 tells the limiter to log stream_xadd_failed. The catch is
-- limited to runtime errors — script-aborting conditions (OOM, write to a
-- read-only replica) still abort the whole EVAL, which is identical to the
-- GCRA SET above, so the stream guard adds no new failure class.
local pushed, _ = pcall(redis.call, 'XADD', stream, 'MAXLEN', '~', maxlen, '*',
  'client_id', client_id, 'cost', tostring(cost), 'ts', tostring(now))
local stream_ok = 1
if not pushed then
  stream_ok = 0
end

return { 1, math.floor(remaining), math.floor(new_tat), now, client_id, rate, period, burst, stream_ok }
