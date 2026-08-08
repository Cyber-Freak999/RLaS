#!/usr/bin/env bash
set -euo pipefail

# M6 acceptance: fail-open under a Redis outage (AGENTS.md constraint 4).
#
# k6 cannot control Docker, so orchestration lives here: start load, stop
# redis-master mid-run, let Sentinel fail over while the per-replica circuit
# breaker trips and the fail-open fallback carries the failover window, bring
# redis-master back, and assert recovery — all without a rate-limiter restart.
# Passing means all of:
#   - k6 saw zero non-200/429 responses (no 503s while Redis was down),
#   - at least one X-RateLimit-Degraded: true response was served,
#   - circuit_breaker_tripped and circuit_breaker_recovered log events exist,
#   - a fresh /check through nginx returns non-degraded after recovery.
#
# Master-only, per AGENTS.md. The timeline: the master is killed hard (1s of
# SIGTERM grace, then SIGKILL — a graceful stop is the wrong fault here: redis
# flushes AOF + saves RDB on SIGTERM and *keeps serving* for ~9s, during which
# the FailoverClient reconnects and /check never fails, so the gate saw 0
# degraded responses and the breaker never tripped), the Sentinels mark it
# +sdown after 5s and fail over to the replica ~5s later. While the master is
# dead and no Sentinel yet answers with the promoted replica, every /check
# hits a hard Redis error within the 500ms check deadline (the fail-fast fix —
# without it go-redis absorbed the window in retries), so three consecutive
# failures trip the breaker and the cached token-bucket fallback carries the
# load degraded. Once the promoted replica answers, the breaker recovers and
# /check serves normally again; restarting redis-master re-joins it as a
# replica. A whole-tier stop would prove the same fail-open but wouldn't
# exercise the Sentinel promotion this scenario was built to test.

cd "$(dirname "$0")/.."

if ! docker compose ps --format '{{.Service}}' 2>/dev/null | grep -q '^rate-limiter$'; then
  echo "error: base stack not running; start it with: docker compose up -d --build" >&2
  exit 1
fi

# docker compose reads .env for the run overlay; source it here too for the
# admin-API calls this script makes after k6 exits.
if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

: "${ADMIN_BEARER_TOKEN:?ADMIN_BEARER_TOKEN must be set in .env}"

WARMUP_SECONDS="${WARMUP_SECONDS:-10}" # let every replica seed its fallback cache
OUTAGE_SECONDS="${OUTAGE_SECONDS:-15}" # the Redis tier is down for this long
K6_LOG="$(mktemp)"
K6_PID=""

REDIS_NODES="redis-master redis-replica redis-sentinel-1 redis-sentinel-2 redis-sentinel-3"
MASTER_IP="172.28.0.10" # redis-master's static address (docker-compose.yml)

# Sentinel promoted redis-replica during the outage, so restore the canonical
# topology (redis-master as primary) before the script ends. redis-master.conf
# sets replica-priority 1, making a forced failover deterministic. Idempotent:
# if .10 is already primary it's a no-op.
reset_topology() {
  local master
  master="$(docker compose exec -T redis-sentinel-1 redis-cli -p 26379 \
    sentinel get-master-addr-by-name mymaster 2>/dev/null | head -1)"
  if [ "$master" = "$MASTER_IP" ]; then
    return 0
  fi
  docker compose exec -T redis-sentinel-1 redis-cli -p 26379 sentinel failover mymaster >/dev/null
  for _ in $(seq 1 30); do
    master="$(docker compose exec -T redis-sentinel-1 redis-cli -p 26379 \
      sentinel get-master-addr-by-name mymaster 2>/dev/null | head -1)"
    [ "$master" = "$MASTER_IP" ] && return 0
    sleep 1
  done
  return 1
}

cleanup() {
  # Never leave the stack degraded, the topology flipped, or k6 orphaned, even
  # on a script error. Restart any Redis node we stopped, then put redis-master
  # back in charge so the next gate run stops the *real* primary.
  docker compose start $REDIS_NODES >/dev/null 2>&1 || true
  reset_topology >/dev/null 2>&1 || true
  if [ -n "$K6_PID" ]; then
    kill "$K6_PID" >/dev/null 2>&1 || true
    wait "$K6_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "==> chaos: load starts, redis-master stops mid-run, system must stay up"
# Baseline the breaker log counters: docker compose logs is cumulative across
# runs, so the gate must assert the delta, not an absolute count.
BASELINE_TRIPS="$(docker compose logs rate-limiter 2>&1 | grep -c 'circuit_breaker_tripped' || true)"
BASELINE_RECOVERS="$(docker compose logs rate-limiter 2>&1 | grep -c 'circuit_breaker_recovered' || true)"

# A previous run may have left redis-replica as primary; the scenario must stop
# the real primary, so restore the canonical topology first.
echo "==> ensuring redis-master is the primary"
if ! reset_topology; then
  echo "error: could not establish redis-master as primary before the run" >&2
  exit 1
fi

docker compose -f docker-compose.yml -f docker-compose.test.yml run --rm k6 run /scripts/chaos.js >"$K6_LOG" 2>&1 &
K6_PID=$!

# The k6 container takes ~10-20s to start (compose run), so a fixed sleep from
# process spawn would often kill the master before the load existed — every
# request then hit an already-promoted replica and the outage went unobserved.
# Wait for k6's own READY marker (setup() done, scenario about to start), then
# run the warmup so the fallback caches are seeded before the master dies.
for _ in $(seq 1 90); do
  if grep -q 'LOAD_STARTED' "$K6_LOG" 2>/dev/null; then
    break
  fi
  sleep 1
done
if ! grep -q 'LOAD_STARTED' "$K6_LOG" 2>/dev/null; then
  echo "error: k6 never reached the load phase within 90s (log: ${K6_LOG})" >&2
  exit 1
fi
echo "==> k6 load confirmed; warming up ${WARMUP_SECONDS}s before the kill"
sleep "$WARMUP_SECONDS"
echo "==> killing redis-master (hard: SIGKILL after 1s grace)"
docker compose stop --timeout 1 redis-master
sleep "$OUTAGE_SECONDS"
echo "==> restarting redis-master"
docker compose start redis-master

# k6 has a fixed 40s duration, so this returns ~15s after the restart above —
# enough for Sentinel to settle before the recovery assertion.
set +e
wait "$K6_PID"
K6_CODE=$?
set -e
K6_PID=""

echo "=== k6 exit code: ${K6_CODE} ==="
if [ "$K6_CODE" -ne 0 ]; then
  grep -E 'ERRO|level=error' "$K6_LOG" | tail -20 || true
  echo "error: k6 failed; full log: ${K6_LOG}" >&2
  exit 1
fi
grep -E 'degraded_responses|unexpected_status|http_req_failed' "$K6_LOG" || true

echo "==> asserting circuit-breaker log events"
LOGS="$(docker compose logs rate-limiter 2>&1)"
trip_count=$(printf '%s\n' "$LOGS" | grep -c 'circuit_breaker_tripped' || true)
recover_count=$(printf '%s\n' "$LOGS" | grep -c 'circuit_breaker_recovered' || true)
trip_delta=$((trip_count - BASELINE_TRIPS))
recover_delta=$((recover_count - BASELINE_RECOVERS))
echo "  breaker events this run: tripped x ${trip_delta}, recovered x ${recover_delta}"
if [ "$trip_delta" -lt 1 ] || [ "$recover_delta" -lt 1 ]; then
  echo "error: expected trip and recovery events this run, got trip=${trip_delta} recover=${recover_delta}" >&2
  exit 1
fi

echo "==> asserting degraded mode disengaged after recovery"
# The runner executes on the host; control-plane publishes 127.0.0.1:8081, so
# the admin API is reached via localhost, not the docker-network hostname.
CLIENT_JSON="$(curl -fsS -X POST http://localhost:8081/v1/admin/clients \
  -H "Authorization: Bearer ${ADMIN_BEARER_TOKEN}")"
CLIENT_ID="$(printf '%s' "$CLIENT_JSON" | grep -o '"client_id":"[^"]*"' | cut -d'"' -f4)"
API_KEY="$(printf '%s' "$CLIENT_JSON" | grep -o '"api_key":"[^"]*"' | cut -d'"' -f4)"
if [ -z "$CLIENT_ID" ] || [ -z "$API_KEY" ]; then
  echo "error: could not parse admin create response: ${CLIENT_JSON}" >&2
  exit 1
fi
curl -fsS -X PUT "http://localhost:8081/v1/admin/clients/${CLIENT_ID}/limits" \
  -H "Authorization: Bearer ${ADMIN_BEARER_TOKEN}" -H 'Content-Type: application/json' \
  -d '{"rate":1000000,"period":"hour","burst":100000000}' >/dev/null

recovered=0
for _ in $(seq 1 30); do
  code=$(curl -s -o /dev/null -D /tmp/chaos-headers -w '%{http_code}' \
    -X POST http://localhost:8080/v1/check \
    -H "X-Api-Key: ${API_KEY}" -H 'Content-Type: application/json' -d '{"cost":1}')
  if { [ "$code" = "200" ] || [ "$code" = "429" ]; } && ! grep -qi 'x-ratelimit-degraded' /tmp/chaos-headers; then
    recovered=1
    break
  fi
  sleep 1
done

curl -fsS -X DELETE "http://localhost:8081/v1/admin/clients/${CLIENT_ID}" \
  -H "Authorization: Bearer ${ADMIN_BEARER_TOKEN}" >/dev/null 2>&1 || true

if [ "$recovered" -ne 1 ]; then
  echo "error: /check stayed degraded for 30s after redis-master restarted" >&2
  exit 1
fi

echo "==> resetting topology: redis-master back to primary"
if ! reset_topology; then
  echo "error: could not restore redis-master as primary" >&2
  exit 1
fi
echo "  primary is ${MASTER_IP} (redis-master)"

echo "==> PASS: fail-open during Redis outage, clean recovery, no service restart"
