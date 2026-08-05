## What

<!-- One paragraph: which PRD requirement(s) this addresses (F1–F7, NFRs, etc.)
and what problem it solves. -->

## Verification

<!-- Every item must be ticked by the author before requesting review. -->

- [ ] Failing test written first (red) before implementation — TDD
- [ ] `go test -race ./...` passes for both `rate-limiter` and `control-plane`
- [ ] Direct GCRA Lua-script EVAL tests pass (not only through HTTP)
- [ ] `docker compose up --build` starts the full stack with zero manual steps
- [ ] k6 scenario mapped to this change: correctness / latency / chaos / N/A
- [ ] Docs updated (`README.md`, `AGENTS.md`, architecture doc) if behavior changed

## Scope

- [ ] Stays within PRD §4/§5 — no unrequested features added

<!-- Commit convention: Conventional Commits. Keep commits atomic; on a feature
branch the TDD sequence is `test:` (red) → `feat:` (green) → optional
`refactor:`. Merged via rebase-merge to keep main linear. -->
