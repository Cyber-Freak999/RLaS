# Global Rate Limiter as a Service

A High Availability rate-limiting service that gives every instance of every internal
microservice an accurate, shared view of remaining quota per third-party API client
(banking, logistics, AI-model providers, etc.), replacing per-instance local limits.

See `architecture-diagram.png` and `Global-Rate-Limiter-Architecture-Documentation.docx`
for the full design and the rationale behind every technology choice.

---

## 1. Architecture at a glance

- **rate-limiter** service (×3 replicas behind nginx) — exposes `POST /check`. Talks only to Redis. This is the latency-critical hot path.
- **control-plane** service — exposes `/admin/*` for client/limit management, and runs a background consumer that drains Redis Streams into TimescaleDB.
- **Redis** — Sentinel topology (1 master, 1 replica, 3 sentinels). Stores GCRA rate-limit state, per-client limits, API-key map, and the Streams transport for approved-request events.
- **TimescaleDB** — durable analytics/billing store, queried by Grafana.
- **Grafana** — business usage dashboard (from TimescaleDB) + operational dashboard (from Prometheus).
- **Prometheus** — scrapes latency and circuit-breaker metrics from both services.
- **nginx** — load balancer in front of the 3 rate-limiter replicas.
- **k6** — load, correctness-under-load, and automated chaos (Redis-outage) test scenarios, run via a separate test overlay — not part of normal startup.

---

## 2. Running the system

Requirements: Docker and Docker Compose only.

Copy `.env.example` to `.env` and fill in values (admin token, Redis/Timescale
connection info, Grafana admin password) before first run.

```bash
cp .env.example .env
docker compose up --build
```

This starts every core service (nginx, 3× rate-limiter, control-plane, Redis
Sentinel cluster, TimescaleDB, Prometheus, Grafana) and waits on `service_healthy`
health checks before dependent services start, so there is no crash-loop on a cold
start.

| Service            | URL                          |
|---------------------|-------------------------------|
| Rate limiter (via LB) | http://localhost:8080/v1/check |
| Control-plane admin API | http://localhost:8081/v1/admin |
| Grafana                 | http://localhost:3000        |
| Prometheus               | http://localhost:9090       |

Default admin bearer token and Grafana credentials are set via `.env` — see
`.env.example`.

Each service exposes `GET /healthz`, checking its actual dependencies (Redis,
and for control-plane, TimescaleDB too) rather than just process liveness —
this is what Compose's health-check-gated startup ordering relies on. Both
services also handle `SIGTERM` gracefully, draining in-flight requests before
exiting, so a `docker compose stop` or a chaos-test container kill doesn't
produce dropped-connection noise unrelated to the actual failure being tested.

### Provisioning a client

```bash
curl -X POST http://localhost:8081/v1/admin/clients \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -d '{"name": "client-a", "rate": 100, "period": "minute", "burst": 20}'
# => returns { "client_id": "...", "api_key": "..." } — the key is shown once
```

### Calling the rate limiter

```bash
curl -X POST http://localhost:8080/v1/check \
  -H "X-Api-Key: $CLIENT_API_KEY" \
  -d '{"cost": 1}'
```

Response on success (`200`):
```json
{ "allowed": true, "remaining": 19, "reset_at": "2026-07-18T00:01:00Z" }
```

Response on rejection (`429`, with a `Retry-After` header):
```json
{ "allowed": false, "remaining": 0, "reset_at": "2026-07-18T00:01:00Z" }
```

`remaining` is the client's current bucket level (burst capacity), so callers
can self-throttle against burst headroom rather than the sustained rate;
`reset_at` is when the bucket is expected to hold a full `cost` again.

An `X-RateLimit-Degraded: true` header is present whenever a response was served
from a replica's local circuit-breaker fallback rather than authoritative Redis
state (see §5, fail-safe behavior).

---

## 3. Running the tests

Tests are intentionally **not** part of `docker compose up` — starting the system
and testing it are kept as two separate, explicit steps.

```bash
docker compose -f docker-compose.yml -f docker-compose.test.yml run --rm k6
```

This runs three scenarios in sequence against the already-running system.
The chaos scenario is driven by a thin shell wrapper (`k6/chaos-runner.sh`) —
k6 has no Docker access, so the wrapper starts the k6 load in the background,
sleeps briefly, runs `docker compose stop redis-master`, sleeps through the
test window, runs `docker compose start redis-master`, then waits for k6 and
checks its exit code. k6 itself only ever does HTTP load and assertions;
container orchestration stays in bash.

1. **`correctness_under_load.js`** — multiple named virtual-client profiles (Client A
   at 100 req/min, Client B at 5000 req/min) hammer `/check` concurrently through
   nginx; asserts each client receives exactly their configured number of successes,
   no more, no fewer.
2. **`latency.js`** — sustained load (10,000 req/sec aggregate across all 3
   replicas) with threshold assertions on end-to-end latency through nginx:
   p95 < 10ms and p99 < 20ms. The test fails the build if a threshold is
   breached. True server-side latency (p99 < 5ms) is tracked separately via each
   service's Prometheus histogram on the ops dashboard — k6 can't observe that
   number through the load balancer, so the two thresholds are defined and
   measured independently.
3. **`chaos_redis_outage.js`** — starts load, then automatically stops the
   `redis-master` container mid-run, asserts requests continue succeeding via the
   local fallback (fail-open, `X-RateLimit-Degraded: true`) rather than erroring,
   restarts `redis-master`, and asserts the system recovers without a restart of the
   rate-limiter service.

Unit-level race safety (`go test -race ./...`) is run separately as part of each
service's own test suite:

```bash
docker compose run --rm rate-limiter go test -race ./...
```

---

## 4. Verifying edge cases manually

| Scenario | How to verify |
|---|---|
| Cluster-wide accuracy | Run `correctness_under_load.js` above, or manually fire concurrent requests at two different replica ports (`:8080` routes through nginx to all three) for the same client and confirm the combined success count matches their configured limit exactly. |
| Fail-safe on Redis outage | `docker compose stop redis-master`; watch `docker compose logs -f rate-limiter` for a `circuit_breaker_tripped` log line; confirm `/check` still returns `200`/`429` (not `503`) with `X-RateLimit-Degraded: true`; `docker compose start redis-master`; confirm the degraded header disappears once Sentinel reports the primary healthy again. |
| Limit change consistency | `PUT /admin/clients/{id}/limits` with a new rate; confirm the client's GCRA state resets (their next `/check` shows a fresh `remaining` under the new rate, not a stale value computed under the old one). |
| Variable request cost | `POST /check` with `{"cost": 5}` for a client and confirm `remaining` drops by 5, not 1. |
| Sentinel failover | `docker compose stop redis-master`; wait a few seconds; confirm (via `docker compose logs sentinel-1`) that 2 of 3 sentinels agree and promote the replica; confirm `/check` continues working against the new primary without any service restart. |
| Config durability | `docker compose restart redis-master`; confirm (via `GET /admin/clients`) that previously provisioned clients and their limits are still present — AOF persistence should mean no data loss. |
| Streams backpressure | `docker compose stop control-plane`; generate load against `/check` for a few minutes; confirm via `redis-cli XLEN stream:approved_requests` that the stream is trimmed near `MAXLEN` rather than growing unbounded; restart `control-plane` and confirm it resumes consuming. |

---

## 5. Design notes (see full documentation for rationale)

- **Fail-safe strategy:** fail-open with a local, in-memory circuit-breaker
  fallback per replica — an outage degrades accuracy briefly rather than
  disabling rate limiting (and therefore financial protection) entirely.
- **Cluster accuracy:** a single atomic GCRA Lua script per check, run against
  shared Redis state — correctness does not depend on which replica receives
  the request.
- **Analytics without hot-path cost:** approved requests are pushed onto Redis
  Streams (non-blocking) and drained asynchronously by control-plane into
  TimescaleDB — logging never adds latency to `/check`. Consumer-group delivery
  is at-least-once and TimescaleDB writes are idempotent
  (`ON CONFLICT (event_id) DO NOTHING`, keyed on the stream entry ID), so a
  crash between the write and the `XACK` can't create duplicate billing rows.
- **Degraded-mode drift:** the fail-open fallback is a per-replica bucket
  seeded from each replica's cached last-known per-client limits, so during an
  outage the fleet may admit up to ~3× a client's configured burst until Redis
  recovers. Bounded and accepted; every degraded response carries
  `X-RateLimit-Degraded: true`.
- **Failover window:** GCRA state is deliberately allowed to be lost on a
  Sentinel failover (it simply resets to a fresh bucket), so the cluster may
  briefly over-admit while a new primary is promoted and catches up.

---

## 6. Repository layout

```
.
├── .github/               # PR template; CI workflow (lands at M2)
├── rate-limiter/            # Go service: /check
├── control-plane/           # Go service: /admin + Streams consumer
├── redis/                   # Sentinel + Lua (GCRA) scripts
├── nginx/                   # LB config
├── grafana/                 # provisioned dashboards (business + ops)
├── prometheus/              # scrape config
├── k6/                      # load / correctness / chaos test scripts
├── docs/
│   ├── architecture-diagram.png
│   └── Global-Rate-Limiter-Architecture-Documentation.docx
├── docker-compose.yml
├── docker-compose.test.yml
├── .env.example
└── README.md
```
