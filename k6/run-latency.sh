#!/usr/bin/env bash
set -euo pipefail

# F3 acceptance: end-to-end latency through nginx (p95 < 10ms, p99 < 20ms)
# under sustained below-limit load. Fails the gate if a threshold is breached.

cd "$(dirname "$0")/.."

if ! docker compose ps --format '{{.Service}}' 2>/dev/null | grep -q '^rate-limiter$'; then
  echo "error: base stack not running; start it with: docker compose up -d --build" >&2
  exit 1
fi

echo "==> latency: p95 < 10ms / p99 < 20ms through nginx"
docker compose -f docker-compose.yml -f docker-compose.test.yml run --rm k6 run /scripts/latency.js
