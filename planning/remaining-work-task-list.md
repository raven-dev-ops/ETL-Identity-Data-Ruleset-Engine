# Remaining Work Task List

Date prepared: 2026-03-15
Last updated: 2026-03-15

This checklist mirrors the active post-`v1.1.0` backlog in
`planning/active-github-issues-backlog.md`. GitHub remains the source
of truth for tracked work; this file is the short maintainer view.

## Current Status

- `v1.1.0` is published and closes the vendor-adapter, secure-operations,
  and customer deployment supportability cycle.
- The previous tracked backlog cycle is now archived in
  `planning/post-v1.5.0-github-issues-backlog.md`.
- A new backlog cycle is now opened in GitHub for the documented next gaps:
  multi-tenant security and HA deployment, live CAD/RMS integration
  targets, and production acceptance with CJIS operating controls.
- Active GitHub epics for this cycle are `#126`, `#127`, and `#128`.
- Open child issues for this cycle are `#129` through `#140`.
- Open milestones for this cycle are
  `v1.2.0-multi-tenant-foundation`,
  `v1.3.0-live-integrations`, and `v1.4.0-cjis-acceptance`.

## Open Task Groups

- [ ] `v1.2.0-multi-tenant-foundation` Multi-tenant security and high-availability foundation
- [ ] `v1.3.0-live-integrations` Live CAD/RMS integration targets
- [ ] `v1.4.0-cjis-acceptance` Production acceptance and CJIS operating controls

## Gaps Now Explicitly Tracked

- [ ] Tenant-aware persisted-state boundaries
- [ ] Tenant-scoped service authorization
- [ ] Field-level authorization hooks
- [ ] HA PostgreSQL deployment baseline
- [ ] First packaged live CAD integration target
- [ ] First packaged live RMS integration target
- [ ] Secure landed-file acquisition and chain-of-custody workflow
- [ ] Masked acceptance fixtures and drift-report workflow for live onboarding
- [ ] Environment promotion and sealing workflow
- [ ] CJIS operating-controls evidence review cadence
- [ ] Production acceptance suite and readiness report
- [ ] Incident response, audit review, and operator training package

## Next Step

- Next: start local catalog `#129` / GitHub issue `#129`.
