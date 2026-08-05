# AGENTS.md

Instructions for any agent (or human) writing code in this repository.
Read `PRD.md` for *what* and *why*; this file is *how* — conventions and hard
constraints that must not be violated while implementing.

## Project shape

Two Go services, shared Redis, TimescaleDB for analytics, Grafana/Prometheus
for observability, k6 for tests. Full rationale lives in
`Global-Rate-Limiter-Architecture-Documentation.docx`; this file only restates
what's load-bearing for correctness.

```
rate-limiter/     Go service — POST /check only. Talks to Redis only. Nothing else.
control-plane/    Go service — /admin/*, Streams consumer. Talks to Redis + TimescaleDB.
redis/            GCRA Lua script, Sentinel config.
nginx/             LB config, round-robin, no sticky sessions.
grafana/           Provisioned dashboards (business + ops).
prometheus/        Scrape config.
k6/                 load / correctness / chaos scripts.
```

## Non-negotiable constraints

These are locked design decisions (see PRD + architecture doc). Do not silently
change them; if one seems wrong, flag it rather than deviating.

1. **Algorithm is GCRA, one atomic Lua script, one Redis round trip per check.**
   No fixed-window counters, no read-then-write-in-application-code patterns —
   the check-and-consume must happen atomically inside Redis via `EVAL`. The
   script derives the current time from Redis itself (`TIME`, allowed under
   effect replication, default since Redis 7.0), never from the caller's clock,
   so all replicas share one authoritative time base.
2. **`rate-limiter` never talks to TimescaleDB, and never talks to Redis for
   anything except: GCRA check, API-key lookup, limit-config read, Streams
   `XADD`.** Keeping this service's dependency surface minimal is what keeps
   `/check` fast and simple to reason about.
3. **Redis connections are Sentinel-aware** (e.g. `go-redis`
   `NewFailoverClient`), never a static `redis-master` hostname — a static
   hostname breaks the moment Sentinel fails over.
4. **Fail-open, not fail-closed**, when Redis is unreachable. A local
   in-memory circuit-breaker fallback serves the response; the response must
   carry `X-RateLimit-Degraded: true`. Never let a Redis outage return `503`
   for `/check`. The fallback is a per-replica token bucket seeded from each
   replica's cached last-known per-client limits (limits and the key→client
   map are refreshed on every successful Redis round trip); it trips after 3
   consecutive check-path Redis failures and recovers after 1 success.
5. **Every approved request is pushed onto Redis Streams (`XADD`), never
   written synchronously to TimescaleDB from the hot path.** The
   Streams-consumer in `control-plane` is the only thing that touches
   TimescaleDB for writes. Consumer-group delivery is at-least-once; the
   consumer writes idempotently (`ON CONFLICT (event_id) DO NOTHING`, where
   `event_id` is the stream entry ID) and `XACK`s after the write, so a crash
   between the write and the `XACK` can't create duplicate billing rows.
6. **A limit change (`PUT /admin/clients/{id}/limits`) must reset that
   client's GCRA state key.** Skipping this can produce mathematically
   undefined wait-time results — this is a correctness requirement, not a
   nice-to-have.
7. **`cost` defaults to 1** on `/check` if omitted; support arbitrary positive
   integers. Don't make it required — most callers won't set it.
8. **Auth is two separate mechanisms**: per-client API key (Redis-backed) for
   `/check`; a separate admin bearer token for `/admin/*`. Never let one
   satisfy the other.
9. **Redis runs with AOF persistence on every node** (`appendonly yes`,
   `appendfsync everysec`). `client_limits` and `api_keys` are the only copy
   of who's allowed to call the system — losing them on a restart
   deprovisions every client. GCRA state itself is fine to lose (it just
   resets to a fresh bucket). The promoted replica must run the same AOF
   settings as the original primary, or the durability guarantee silently
   evaporates on the first failover.
10. **The Streams key is trimmed** (`XADD ... MAXLEN ~ 100000`) and the
    consumer logs a warning if stream length exceeds ~50,000 on poll — this is
    the signal that the consumer is falling behind, before events actually
    start getting dropped.
11. **Both services expose a real `/healthz`** that checks actual
    dependencies (rate-limiter: Redis `PING`; control-plane: Redis `PING` +
    TimescaleDB connectivity), returning `503` if a dependency is down. This
    is what Compose's `service_healthy` condition depends on — a
    health check that always returns `200` makes that startup-ordering
    guarantee silently meaningless.
12. **Both services handle SIGTERM with graceful shutdown**
    (`http.Server.Shutdown(ctx)`, short timeout) so in-flight requests finish
    before a replica stops. Skipping this shows up as dropped-connection noise
    in the chaos test that looks like a fail-safe bug but isn't one — don't
    let that ambiguity into the test results.
13. **Admin API validates input before it reaches Redis or the Lua script**:
    reject `rate <= 0`, `burst < 1`, non-positive `cost`, and any `period`
    outside `second | minute | hour` with a `400`. GCRA's math assumes
    positive parameters; validate at the boundary, not inside the script. A
    `burst` of zero makes the bucket capacity zero and would deny every
    request — it's rejected, not special-cased.
14. **All routes are versioned under `/v1`** (`/v1/check`, `/v1/admin/*`) from
    the start. No version-negotiation logic is required for this deliverable
    — just the path prefix, so a future breaking change doesn't require
    retrofitting every existing caller.
15. **Structured log events use fixed names and fields** — the chaos test's
    log-based assertions and the manual README verification steps both depend
    on these exact event names. Do not rename or restructure without updating
    both:
    ```json
    {"event": "circuit_breaker_tripped", "reason": "redis_unreachable", "ts": "..."}
    {"event": "circuit_breaker_recovered", "ts": "..."}
    {"event": "sentinel_failover_detected", "new_primary": "...", "ts": "..."}
    ```

## API contract (authoritative — see architecture doc §4 for full rationale)

All routes are prefixed `/v1` (see constraint 14).

**`POST /v1/check`** — header `X-Api-Key`, body `{"cost"?: int}`.
Success `200`: `{"allowed": true, "remaining": int, "reset_at": timestamp}`.
Rejected `429` + `Retry-After` header: `{"allowed": false, "remaining": 0, "reset_at": timestamp}`.
Degraded responses (either code) additionally carry `X-RateLimit-Degraded: true`.

**`/v1/admin/*`** — header `Authorization: Bearer <admin token>`.
- `POST /v1/admin/clients` → `{client_id, api_key}` (key shown once).
- `GET /v1/admin/clients`
- `GET /v1/admin/clients/{id}/limits`
- `PUT /v1/admin/clients/{id}/limits` body `{rate, period, burst}` → resets GCRA state.
- `DELETE /v1/admin/clients/{id}`

## Redis key schema

| Key | Purpose | Written by | Read by |
|---|---|---|---|
| `gcra:{client_id}` | GCRA theoretical-arrival-time state | rate-limiter (on check), control-plane (reset on limit change) | rate-limiter |
| `client_limits:{client_id}` | `{rate, period, burst}` | control-plane | rate-limiter |
| `api_keys` (hash: `sha256(key)` → `client_id`) | key → client mapping (keys never stored in plaintext) | control-plane | rate-limiter |
| `stream:approved_requests` | Redis Stream of approved-request events | rate-limiter (`XADD`) | control-plane consumer (`XREADGROUP`) |

## Testing requirements before calling anything "done"

- `go test -race ./...` passes for both services.
- Multi-instance correctness test (3 replicas behind nginx) shows exact quota
  enforcement — not "approximately correct."
- k6 latency scenario meets the documented p95/p99 threshold.
- k6 chaos scenario: stop `redis-master` mid-load → traffic keeps succeeding
  in degraded mode → restart `redis-master` → system recovers without a
  service restart.
  - **Orchestration:** k6 does not control Docker directly. Use a thin shell
    wrapper: start k6 in the background, sleep, `docker compose stop
    redis-master`, sleep for the test window, `docker compose start
    redis-master`, `wait` for k6 and check its exit code. Keep container
    orchestration in bash, not inside the k6 script.
- `docker compose up --build` works with zero manual intervention.

## Conventions

- Structured logging (not `fmt.Println`) — every circuit-breaker trip,
  Sentinel failover observation, and auth failure should be a structured log
  line, since the README's manual-verification steps rely on grepping these.
- Comment *why*, not *what*, especially around the GCRA script and the
  fail-open fallback — these are the two places a reviewer will look first to
  check understanding, not just implementation.
- Keep `rate-limiter` and `control-plane` independently buildable/testable —
  no shared internal package that forces them to release together, beyond a
  small shared `types` or `redisclient` package if needed.

## Feature shipping standard

- **Workflow:** GitHub Flow. Short-lived branches off `main` named
  `feat/<prd-ref>-<slug>` (e.g. `feat/F3-gcra-lua`), merged via PR using
  rebase-merge to keep `main` linear. The PR is the review gate — every PR must
  complete the checklist in `.github/pull_request_template.md`.
- **Commits:** Conventional Commits (`feat:`, `fix:`, `test:`, `docs:`, `ci:`,
  `refactor:`, `chore:`). Keep commits atomic — one logical unit each that
  builds and passes on its own, except an intentional red test commit.
- **TDD is mandatory:** red-green-refactor for every feature and bugfix. Write
  the failing test first (`test:` commit), then implement (`feat:`/`fix:`),
  then refactor only if needed. Red commits stay local until the green one
  exists. Required test layers:
  1. Go unit tests — `go test -race ./...` per service.
  2. Direct GCRA Lua-script EVAL tests (PRD §8) — not only through HTTP.
  3. k6 acceptance scenarios (correctness / latency / chaos) for
     integration-level changes.
- **Sub-agents:** independent, parallelizable work streams are dispatched to
  sub-agents (per `dispatching-parallel-agents` / `subagent-driven-development`)
  with a precise spec and the verification commands they must run. A sub-agent
  only touches its assigned subsystem; integration checkpoints happen on
  `main`. *Why sub-agents:* (1) parallel wall-clock speedup — the
  rate-limiter, control-plane, k6, and dashboard work streams share no code,
  so sequencing them serially wastes time; (2) a fresh context per subsystem
  forces a precise written spec (scope + verification commands), which is what
  makes a sub-agent's output auditable without replaying the whole
  conversation; (3) the hard boundary "touch only your subsystem" contains any
  mistake to a reviewable unit instead of entangling unrelated files; (4) the
  shared contract (API shape, Redis key schema, stream names) stays coherent
  because integration happens on `main`, not inside a branch's private world.
- **CI policy:** a small PR workflow (`.github/workflows/ci.yml`: `go vet` +
  `go test -race ./...` for both modules) runs on PRs and `main` push. The
  heavy compose + k6 suite stays a local/release gate, not CI.
- **`main` is a protected branch** (GitHub branch protection rule — requires a
  PR to merge, linear history, conversation resolution; blocks force pushes and
  deletions, and not even the owner can bypass). Do not push to `main`
  directly, and do not reconfigure or remove the protection rule. A required CI
  status check (`build-and-test`) is wired into the rule once `ci.yml` exists
  at M2 — GitHub requires a check to exist before it can be required, so this
  is deliberately a two-step change, not an oversight.
- **CD:** none. `main` is the deliverable.

## Pre-build checklist (small decisions, easy to forget)

- [ ] `period` is a fixed enum: `second`, `minute`, `hour` — not a free-form
      duration string.
- [ ] `burst` is validated `>= 1` — a zero burst capacity would deny every
      request.
- [ ] API keys are stored hashed (SHA-256), never plaintext in Redis.
- [ ] Latency thresholds are two-tier: server-side p99 < 5ms (Go histogram)
      and k6 end-to-end p95 < 10ms / p99 < 20ms through nginx.
- [ ] `.env.example` exists and is kept current as new env vars are added
      (see below for the starting list).
- [ ] Redis container config includes `appendonly yes`, `appendfsync everysec`.
- [ ] Stream `XADD` calls include `MAXLEN ~ 100000`.
- [ ] Both services' `/healthz` actually check dependencies, not just liveness.
- [ ] Both services handle SIGTERM and drain in-flight requests before exit.

## Required environment variables (`.env.example`)

```
ADMIN_BEARER_TOKEN=
REDIS_SENTINEL_ADDRS=
REDIS_MASTER_NAME=
TIMESCALE_DSN=
GRAFANA_ADMIN_PASSWORD=
PROMETHEUS_SCRAPE_INTERVAL=
```

## Where things live

- Full rationale for every choice → `Global-Rate-Limiter-Architecture-Documentation.docx`
- What/why/acceptance criteria → `PRD.md`
- Diagram → `architecture-diagram.png`
- Run/test/verify instructions → `README.md`
