# Remaining Work Task List

Date prepared: 2026-03-13
Last updated: 2026-03-13

This checklist mirrors the active production-readiness backlog in
`planning/active-github-issues-backlog.md`. GitHub remains the source of
truth for tracked work; this file is the short maintainer view.

## Current Status

- A new backlog cycle is open to move the project beyond prototype scope
  toward a production MDM or enterprise data-product service.
- GitHub sync is complete with open epics `#58`, `#59`, and `#60`, plus
  open child issues `#77` through `#80`.
- The active tracked work is organized into three milestone groups:
  `v0.4.0`, `v0.5.0`, and `v0.6.0`.
- The current public `0.1.x` line remains a production-hardened
  prototype until those tracked issues are completed.
- Issue `#61` is complete: the runtime now validates a stable landed
  batch manifest contract before `normalize` or manifest-driven
  `run-all` reads real input files.
- Issue `#62` is complete: landed batches now resolve through local and
  object-storage-compatible adapters under the manifest contract.
- Issue `#63` is complete: completed runs and core artifacts now persist
  into SQLite and can be reloaded for reporting by `run_id`.
- Issue `#64` is complete: the run registry now captures lifecycle
  status and failure detail, reuses completed runs idempotently, and
  cleanly restarts failed attempts.
- Issue `#65` is complete: persisted state now upgrades through
  Alembic-backed migrations, and named runtime environments can provide
  config and secret-backed defaults.
- Issue `#66` is complete: manifest-driven persisted runs now support
  incremental refresh with predecessor reuse for unaffected entities and
  documented fallback-to-full behavior.
- Issue `#67` is complete: completed persisted runs can now be
  published under a versioned golden/crosswalk delivery contract with
  immutable snapshots and an atomic consumer pointer.
- Issue `#68` is complete: persisted review cases now support assignee,
  notes, timestamps, and the `pending` / `approved` / `rejected` /
  `deferred` lifecycle through SQLite-backed workflow commands.
- Issue `#69` is complete: approved and rejected persisted review
  decisions now override later candidate outcomes and apply
  deterministically to cluster and golden rebuilds on rerun.
- Issue `#70` is complete: a read-only operator service API now exposes
  persisted run status, golden-record lookup, crosswalk lookup, and
  review-case retrieval.
- Issue `#71` is complete: operator-facing CLI wrappers now support
  idempotent review decisions, manifest-backed replay, and downstream
  publication triggers with JSON output.
- Issue `#72` is complete: named warehouse and data-product export jobs
  now materialize persisted golden and crosswalk snapshots through
  audited SQLite export-run records and JSON operator commands for
  export execution history.
- Issue `#73` is complete: service, workflow, and delivery docs now
  share an explicit compatibility policy covering stable versus
  internal surfaces, versioning rules, and deprecation expectations.
- Issue `#74` is complete: the service now requires API-key
  authentication, distinguishes `reader` versus `operator` access, and
  loads service secrets through environment-backed runtime config.
- Issue `#75` is complete: batch and service paths now emit structured
  JSON logs, the service exposes authenticated `healthz`, `readyz`, and
  `/api/v1/metrics` endpoints, and privileged operator actions now
  persist audit events in SQLite.
- Issue `#76` is complete: the repo now ships a shared batch/service
  container image, a single-host compose deployment baseline, and a
  reusable container smoke test wired into CI.

## Open Task Groups

- [ ] `v0.4.0` Production data ingestion and persistence
  - Open issues: none. `v0.4.0` implementation work is complete in the
    active backlog catalog.
- [ ] `v0.5.0` Review workflow and service surface
  - Open issues: none. `v0.5.0` implementation work is complete in the
    active backlog catalog.
- [ ] `v0.6.0` Security, operations, and deployment hardening
  - Open issues: `#77` through `#80` in the active backlog catalog.

## Production-Readiness Gaps Now Explicitly Tracked

- [x] Real landed-batch ingestion adapters and object-storage-compatible
  support
- [x] Persistent relational state for runs, entities, goldens, and
  review cases
- [x] Idempotent orchestration and replay-safe run lifecycle handling
- [x] Migration tooling and environment-specific runtime configuration
- [x] Incremental processing across runs
- [x] Persisted manual-review workflow
- [x] Decision application into cluster and golden rebuilds
- [x] Service and operator interfaces for run status, review, and golden
  lookups
- [x] Downstream publication contracts for warehouse and data-product
  consumers
- [x] Authentication, authorization, and environment-backed service
  secrets
- [x] Observability baseline with structured logs, metrics, health
  checks, and audit events
- [x] Containerized batch and service runtime plus deployable
  single-host environment manifests
- [ ] Recovery runbooks and release hardening

## Next Step

- Execute the new milestone cycle in order: `v0.4.0`, then `v0.5.0`,
  then `v0.6.0`, starting with issue `#77`.
