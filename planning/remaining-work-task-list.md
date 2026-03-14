# Remaining Work Task List

Date prepared: 2026-03-14
Last updated: 2026-03-14

This checklist mirrors the active post-`v0.6.0` backlog in
`planning/active-github-issues-backlog.md`. GitHub remains the source
of truth for tracked work; this file is the short maintainer view.

## Current Status

- The `v0.6.0` production-readiness baseline is published and the prior
  backlog cycle is complete.
- A new backlog cycle is now open for the documented post-`v0.6.0`
  gaps: durable replay, enterprise access control, and distributed
  deployment/runtime support.
- The active tracked work is organized into three milestone groups:
  `v0.7.0`, `v0.8.0`, and `v0.9.0`.
- GitHub sync is complete for the new backlog cycle with open epics
  `#81`, `#82`, and `#83`.
- GitHub issue `#84` is complete: persisted state now supports
  PostgreSQL DSNs alongside SQLite paths, and the state-store migration
  and runtime surface has PostgreSQL dialect coverage.
- GitHub issue `#85` is complete: manifest-backed persisted runs now
  archive immutable replay bundles with the original manifest, landed
  input snapshot, verification metadata, and operator bundle-validation
  support.
- GitHub issue `#86` is complete: `replay-run` now executes directly
  from verified archived replay bundles without restoring the original
  manifest and landing paths, and persisted run metadata records direct
  bundle replayability.
- GitHub issue `#87` is complete: persisted runs now write durable
  stage checkpoints, failed attempts record resumable summary state, and
  reruns resume from the latest checkpoint instead of redoing earlier
  completed stages.

## Open Task Groups

- [ ] `v0.7.0` Durable replay and database platform
  - Open GitHub issues: none
- [ ] `v0.8.0` Enterprise access and service control plane
  - Open GitHub issues: `#88`, `#89`, `#90`, `#91`
- [ ] `v0.9.0` Distributed deployment and event-driven runtime
  - Open GitHub issues: `#92`, `#93`, `#94`, `#95`

## Gaps Now Explicitly Tracked

- [x] PostgreSQL-backed persisted state
- [x] Immutable replay bundles for manifests and landed inputs
- [x] Replay independent of original landing paths
- [x] Checkpointed resume for failed persisted runs
- [ ] OIDC or JWT service authentication
- [ ] Fine-grained RBAC and actor identity propagation
- [ ] HTTP publish and export triggers
- [ ] Paginated service list and search endpoints
- [ ] Clustered deployment manifests
- [ ] Container signing and image-level supply-chain gates
- [ ] Event-driven ingestion and streaming entity refresh
- [ ] Clustered benchmarks and distributed SLO targets

## Next Step

- Start issue `#88`: Add OIDC or JWT service authentication for
  enterprise identity providers.
