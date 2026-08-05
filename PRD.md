# PRD — Global Rate Limiter as a Service

**Status:** Locked for build
**Owner:** Cyberfreak

## 1. Problem

The company relies on hundreds of external APIs (banking, logistics, AI models)
with strict, priced quotas. Each microservice currently enforces its own limit
locally. When a service runs N instances, each instance behaves as if it owns
the full quota, so real usage across the fleet exceeds the actual limit. This
causes frequent `429` errors and direct financial penalties.

## 2. Goal

Ship a standalone, horizontally-scaled, high-availability service that every
internal microservice calls before hitting a third-party API, giving the whole
fleet one accurate, shared view of remaining quota per client — regardless of
which instance of which microservice, or which instance of the limiter itself,
handles the check.

## 3. Non-goals

- Not a public-facing API gateway — callers are internal, trusted microservices.
- Not a general-purpose API gateway (no routing, transformation, or proxying of
  the actual third-party calls) — this service only answers "allowed or not."
- Not building JWT/OAuth federation — no external identity provider to federate
  with (see architecture doc §3.4 for the reasoning).
- No multi-tenant admin RBAC — a single admin credential is sufficient for this
  scope.

## 4. Functional requirements

| # | Requirement | Acceptance criteria |
|---|---|---|
| F1 | Per-client configurable limits | Admin can set a distinct rate/period/burst per client; enforced independently per client. |
| F2 | Cluster-accurate checks | A client's total approved requests across all rate-limiter replicas never exceeds their configured limit, proven under concurrent multi-replica load. |
| F3 | Fast checks | `/check` p95/p99 latency stays under the documented threshold under sustained load. |
| F4 | Fail-safe on store outage | Redis becoming unreachable does not cause all `/check` calls to fail; degraded state is observable via a response header. |
| F5 | Billing/analytics logging | Every approved request is durably logged, without adding synchronous latency to `/check`. |
| F6 | Dashboard | Per-client usage, average response time, and trend graphs over 10/15/30-day windows, with filters. |
| F7 | Variable request cost | A single check can consume more than 1 unit of quota (for cost-metered upstream APIs). |

## 5. Non-functional requirements

- **HA:** no single point of failure in the request-check path (Redis Sentinel
  quorum failover, ≥3 rate-limiter replicas).
- **Observability:** operational state (latency, degraded-mode activation,
  Sentinel failover) must be visible without reading raw logs.
- **Testability:** every hard requirement (F2, F3, F4) must have an automated,
  reproducible test — not just manual verification.
- **Durability of configuration:** client limits and API keys must survive a
  Redis restart (AOF persistence) — losing this data deprovisions every
  client, which is a more severe failure than losing GCRA state.
- **Bounded log growth:** the Streams transport must not grow unbounded if the
  analytics consumer falls behind; trimming policy and a lag warning are
  required, not optional.
- **Operational readiness:** both services must expose real dependency-aware
  health checks and shut down gracefully (drain in-flight requests) so that
  the automated chaos test's results reflect actual fail-safe behavior, not
  artifacts of an ungraceful process kill.

## 6. Deliverables (per assignment brief)

- [ ] Complete solution, one ZIP file
- [ ] Architectural diagram, image format — `architecture-diagram.png`
- [ ] Detailed, commented code
- [ ] Unit tests: race-condition, load & performance
- [ ] Docker: `Dockerfile` + `docker-compose.yml`, single-command startup
- [ ] `README.md`: run instructions, how to trigger tests, how to verify edge cases

## 7. Success metrics (how "done" is judged)

- `docker compose up --build` starts the full system with no manual steps.
- The correctness-under-load test passes: exact quota enforcement across 3
  replicas behind the load balancer.
- The chaos test passes: killing the Redis primary mid-load does not drop
  traffic to errors; the system recovers automatically.
- The dashboard renders real usage data for a demo client with working
  10/15/30-day filters.
- Every row in the Requirements Traceability table (architecture doc §5) has a
  corresponding passing test or demonstrable behavior.

## 8. Key risks

| Risk | Mitigation |
|---|---|
| GCRA Lua script bug breaks atomicity | Unit tests directly against the script (not just through the HTTP layer) plus the multi-replica correctness test. |
| Chaos test flakiness (container orchestration timing) | Poll for container health/logs rather than fixed sleeps; document expected trip/recovery windows. |
| Scope creep beyond the two-week/take-home budget | PRD non-goals above are the explicit cut line — revisit before adding anything not listed in §4. |

## 9. References

- Original assignment brief (image, provided).
- `Global-Rate-Limiter-Architecture-Documentation.docx` — full design rationale.
- `AGENTS.md` — build conventions and constraints for anyone (human or agent)
  writing code against this PRD.
