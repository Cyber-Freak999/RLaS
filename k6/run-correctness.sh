#!/usr/bin/env bash
set -euo pipefail

# F2 acceptance: exact per-client quota enforcement across the 3 replicas
# behind nginx, plus a fleet-wide cross-check against each replica's /metrics.
#
# k6 alone cannot assert "all 3 replicas saw traffic" — custom metrics can't be
# created dynamically and per-VU state doesn't survive the run — so the spread
# check lives here, in bash, reading each replica's Prometheus counters.

cd "$(dirname "$0")/.."

if ! docker compose ps --format '{{.Service}}' 2>/dev/null | grep -q '^rate-limiter$'; then
  echo "error: base stack not running; start it with: docker compose up -d --build" >&2
  exit 1
fi

echo "==> correctness: exact per-client quota through nginx"
docker compose -f docker-compose.yml -f docker-compose.test.yml run --rm k6 run /scripts/correctness.js

echo "==> replica spread + fleet cross-check (per-replica /metrics)"
mapfile -t ids < <(docker compose ps -q rate-limiter)
if [ "${#ids[@]}" -ne 3 ]; then
  echo "error: expected 3 rate-limiter replicas, got ${#ids[@]}" >&2
  exit 1
fi

fleet_allowed=0
fleet_total=0
spread_ok=1
for id in "${ids[@]}"; do
  ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$id")
  metrics=$(curl -fsS --max-time 5 "http://${ip}:8080/metrics" || true)
  rep_total=$(printf '%s\n' "$metrics" | grep '^rlas_checks_total' | awk '{s += $NF} END {print s + 0}')
  rep_allowed=$(printf '%s\n' "$metrics" | grep '^rlas_checks_total' | grep 'allowed="true"' | awk '{s += $NF} END {print s + 0}')
  echo "  replica ${id}: total=${rep_total} allowed=${rep_allowed}"
  fleet_allowed=$((fleet_allowed + rep_allowed))
  fleet_total=$((fleet_total + rep_total))
  if [ "${rep_total}" -le 0 ]; then
    spread_ok=0
  fi
done

# correctness.js sends 2000 + 400 + 400 = 2800 requests; allowed are A1000 +
# B200 + C20 = 1220. rlas_checks_total carries no client label, so this is a
# fleet-level cross-check only — per-client exactness is k6's job.
if [ "$fleet_allowed" -ne 1220 ]; then
  echo "error: fleet allowed=${fleet_allowed}, want 1220 (A1000+B200+C20)" >&2
  exit 1
fi
if [ "$fleet_total" -ne 2800 ]; then
  echo "error: fleet total=${fleet_total}, want 2800" >&2
  exit 1
fi
if [ "$spread_ok" -eq 0 ]; then
  echo "error: one or more replicas served zero requests — round-robin broken" >&2
  exit 1
fi

echo "==> PASS: exact quota across 3 replicas, all replicas served traffic"
