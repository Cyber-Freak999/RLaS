#!/usr/bin/env bash
set -euo pipefail

# F6 acceptance support: populate the dashboards with real-looking data so the
# business panels render usage immediately, instead of showing empty charts.
#
# The demo client is a real client (api_keys hash + client_limits in Redis),
# so live /check traffic flows through the normal path (nginx -> rate-limiter
# -> Streams -> control-plane consumer -> TimescaleDB). On top of that, 30 days
# of synthetic approved_requests are backfilled directly into TimescaleDB with
# INSERT ... ON CONFLICT DO NOTHING.
#
# Idempotent: the demo client's id + api key are persisted in .demo-seed so a
# re-run reuses them and re-asserts the demo limits. The backfill anchors its
# timestamps to the current hour (date_trunc, not now(), so re-runs within the
# hour conflict and insert nothing) and skips entirely when the 30-day window
# is already populated — repeat runs only add live traffic, never history.
# Use --reset to delete the demo client (Redis keys via the admin API) and its
# TimescaleDB rows first.

cd "$(dirname "$0")/.."

if ! docker compose ps --format '{{.Service}}' 2>/dev/null | grep -q '^rate-limiter$'; then
  echo "error: base stack not running; start it with: docker compose up -d --build" >&2
  exit 1
fi

# docker compose reads .env for the run overlay; source it here too for the
# admin-API and psql credentials this script uses.
if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

: "${ADMIN_BEARER_TOKEN:?ADMIN_BEARER_TOKEN must be set in .env}"
: "${POSTGRES_USER:-timescale}"
: "${POSTGRES_PASSWORD:-timescale}"
: "${POSTGRES_DB:-rlas}"

ADMIN="http://localhost:8081/v1/admin"   # control-plane (compose publishes 127.0.0.1:8081)
CHECK="http://localhost:8080/v1/check"   # nginx entry point
GRAFANA_URL="${GRAFANA_URL:-http://localhost:3000}"
STATE_FILE=".demo-seed"

RATE=1000
PERIOD=minute
BURST=500
BACKFILL_ROWS=720            # 30 days, one approved request per hour
VERIFY_MIN=700               # backfill (720) minus headroom for a partial insert
LIVE_REQUESTS=100

DEMO_CLIENT_ID=""
DEMO_API_KEY=""

# JSON field extraction without jq (keeps the toolchain to curl + grep, the
# same choice as k6/chaos-runner.sh).
json_field() {
  local json="$1" field="$2"
  printf '%s' "$json" | grep -o "\"${field}\":\"[^\"]*\"" | head -1 | cut -d'"' -f4
}

psql_query() {
  # Runs against the timescaledb container; the DB is reachable at 127.0.0.1
  # from inside the container itself (the compose healthcheck does the same).
  docker compose exec -T -e PGPASSWORD="${POSTGRES_PASSWORD}" timescaledb \
    psql -h 127.0.0.1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" \
    -v ON_ERROR_STOP=1 -Atc "$1"
}

create_demo_client() {
  echo "==> creating demo client via the admin API"
  local resp
  resp="$(curl -fsS -X POST "${ADMIN}/clients" -H "Authorization: Bearer ${ADMIN_BEARER_TOKEN}")"
  DEMO_CLIENT_ID="$(json_field "$resp" client_id)"
  DEMO_API_KEY="$(json_field "$resp" api_key)"
  if [ -z "$DEMO_CLIENT_ID" ] || [ -z "$DEMO_API_KEY" ]; then
    echo "error: could not parse admin create response: ${resp}" >&2
    exit 1
  fi
  printf 'DEMO_CLIENT_ID=%s\nDEMO_API_KEY=%s\n' "$DEMO_CLIENT_ID" "$DEMO_API_KEY" >"$STATE_FILE"
}

delete_demo_client() {
  if [ -n "$DEMO_CLIENT_ID" ]; then
    curl -fsS -X DELETE "${ADMIN}/clients/${DEMO_CLIENT_ID}" \
      -H "Authorization: Bearer ${ADMIN_BEARER_TOKEN}" >/dev/null 2>&1 || true
    psql_query "DELETE FROM approved_requests WHERE client_id = '${DEMO_CLIENT_ID}'" >/dev/null 2>&1 || true
  fi
  rm -f "$STATE_FILE"
}

set_demo_limits() {
  echo "==> asserting limits rate=${RATE}/${PERIOD} burst=${BURST} (resets GCRA state)"
  curl -fsS -X PUT "${ADMIN}/clients/${DEMO_CLIENT_ID}/limits" \
    -H "Authorization: Bearer ${ADMIN_BEARER_TOKEN}" -H 'Content-Type: application/json' \
    -d "{\"rate\":${RATE},\"period\":\"${PERIOD}\",\"burst\":${BURST}}" >/dev/null
}

backfill_history() {
  # Fast path: the window is already populated (from a previous run), so
  # re-inserting would only append a shifted duplicate window.
  local existing
  existing="$(psql_query "SELECT count(*) FROM approved_requests
                          WHERE client_id = '${DEMO_CLIENT_ID}'
                            AND ts >= date_trunc('hour', now()) - interval '30 days'")"
  if [ "${existing:-0}" -ge "$VERIFY_MIN" ]; then
    echo "==> skipping backfill (${existing} rows already in the 30-day window)"
    return 0
  fi
  echo "==> backfilling ${BACKFILL_ROWS} rows over the last 30 days (idempotent)"
  # date_trunc('hour', now()) anchors every row to the same hour boundary so a
  # re-run within the hour produces identical (event_id, ts) tuples and the
  # ON CONFLICT DO NOTHING is effective — relative now() would shift every
  # timestamp and silently append a second copy of the history.
  psql_query "INSERT INTO approved_requests (event_id, client_id, cost, ts)
              SELECT 'seed-' || n, '${DEMO_CLIENT_ID}', 1,
                     date_trunc('hour', now()) - ((720 - n) * interval '1 hour')
              FROM generate_series(1, ${BACKFILL_ROWS}) AS n
              ON CONFLICT (event_id, ts) DO NOTHING" >/dev/null
}

live_burst() {
  echo "==> firing ${LIVE_REQUESTS} live /check requests through nginx"
  local code ok=0
  for _ in $(seq 1 "$LIVE_REQUESTS"); do
    code="$(curl -s -o /dev/null -w '%{http_code}' -X POST "$CHECK" \
      -H "X-Api-Key: ${DEMO_API_KEY}" -H 'Content-Type: application/json' -d '{"cost":1}')"
    [ "$code" = "200" ] && ok=$((ok + 1))
  done
  echo "  allowed: ${ok}/${LIVE_REQUESTS}"
  if [ "$ok" -lt "$LIVE_REQUESTS" ]; then
    echo "error: demo burst did not hit the quota as expected" >&2
    exit 1
  fi
}

wait_for_rows() {
  # Live /check events reach TimescaleDB asynchronously via the Streams
  # consumer, so poll until the row count settles.
  echo "==> waiting for the Streams consumer to drain live events"
  local count=0 tries=0
  while [ "$tries" -lt 30 ]; do
    count="$(psql_query "SELECT count(*) FROM approved_requests
                         WHERE client_id = '${DEMO_CLIENT_ID}'
                           AND ts >= now() - interval '30 days'")"
    if [ "${count:-0}" -ge "$VERIFY_MIN" ]; then
      echo "  verified: ${count} approved_requests >= ${VERIFY_MIN}"
      return 0
    fi
    sleep 2
    tries=$((tries + 1))
  done
  echo "error: expected >= ${VERIFY_MIN} approved_requests for the demo client, saw ${count:-0}" >&2
  exit 1
}

if [ "${1:-}" = "--reset" ]; then
  if [ -f "$STATE_FILE" ]; then
    set -a
    # shellcheck disable=SC1091
    source "$STATE_FILE"
    set +a
  fi
  echo "==> resetting demo client"
  delete_demo_client
fi

# Reuse the demo client if it still exists (a re-run should not mint a new key).
if [ -f "$STATE_FILE" ]; then
  set -a
  # shellcheck disable=SC1091
  source "$STATE_FILE"
  set +a
  clients="$(curl -fsS "${ADMIN}/clients" -H "Authorization: Bearer ${ADMIN_BEARER_TOKEN}")"
  if printf '%s' "$clients" | grep -q "\"${DEMO_CLIENT_ID}\""; then
    echo "==> reusing demo client ${DEMO_CLIENT_ID} (from ${STATE_FILE})"
  else
    echo "==> demo client gone; creating a fresh one"
    create_demo_client
  fi
else
  create_demo_client
fi

set_demo_limits
backfill_history
live_burst
wait_for_rows

echo
echo "==> PASS: demo data seeded"
echo "  client_id: ${DEMO_CLIENT_ID}"
echo "  api_key:   ${DEMO_API_KEY}   (also in ${STATE_FILE})"
echo
echo "  Grafana:   ${GRAFANA_URL}   (admin/${GRAFANA_ADMIN_PASSWORD:-admin})"
echo "  Try it:    curl -X POST ${CHECK} -H 'X-Api-Key: ${DEMO_API_KEY}' -H 'Content-Type: application/json' -d '{\"cost\":1}'"
echo "  Reset it:  ${BASH_SOURCE[0]} --reset"
