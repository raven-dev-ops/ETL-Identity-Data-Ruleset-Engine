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
- GitHub issue `#88` is complete: the service now supports JWT bearer
  authentication backed by deployment-provided issuer, audience,
  signing, and claim-mapping metadata while retaining API-key auth as a
  documented compatibility mode.
- GitHub issue `#89` is complete: the service now enforces endpoint-
  level scopes beyond the stable `reader` / `operator` roles, and audit
  events persist authenticated actor identity plus granted/required
  scope context.
- GitHub issue `#90` is complete: the authenticated service API now
  exposes operator-only publish and named export-job triggers with
  scoped authorization, idempotent reuse, and persisted audit coverage.
- GitHub issue `#91` is complete: the service now exposes paginated run,
  golden-record, and review-case collection endpoints with documented
  filter, sort, and page-token semantics.
- GitHub issue `#92` is complete: the repo now ships a Kubernetes
  deployment baseline for the PostgreSQL-backed topology, including
  service, PostgreSQL, migration, and batch manifests plus a CI-backed
  smoke path.
- GitHub issue `#93` is complete: the release path now emits retained
  container attestation, SBOM-style inventory, provenance, and
  dependency-audit outputs, and CI enforces the container dependency
  gate before image publication.

## Open Task Groups

- [ ] `v0.7.0` Durable replay and database platform
  - Open GitHub issues: none
- [ ] `v0.8.0` Enterprise access and service control plane
  - Open GitHub issues: none
- [ ] `v0.9.0` Distributed deployment and event-driven runtime
  - Open GitHub issues: `#94`, `#95`

## Gaps Now Explicitly Tracked

- [x] PostgreSQL-backed persisted state
- [x] Immutable replay bundles for manifests and landed inputs
- [x] Replay independent of original landing paths
- [x] Checkpointed resume for failed persisted runs
- [x] OIDC or JWT service authentication
- [x] Fine-grained RBAC and actor identity propagation
- [x] HTTP publish and export triggers
- [x] Paginated service list and search endpoints
- [x] Clustered deployment manifests
- [x] Container signing and image-level supply-chain gates
- [ ] Event-driven ingestion and streaming entity refresh
- [ ] Clustered benchmarks and distributed SLO targets

## Next Step

- Start issue `#94`: Add event-driven ingestion and streaming
  entity-refresh mode.
