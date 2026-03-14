# Remaining Work Task List

Date prepared: 2026-03-13
Last updated: 2026-03-14

This checklist mirrors the active production-readiness backlog in
`planning/active-github-issues-backlog.md`. GitHub remains the source of
truth for tracked work; this file is the short maintainer view.

## Current Status

- The production-readiness backlog cycle is complete in-repo and now
  defines a coherent single-host production operating target.
- GitHub sync is complete for the current production-readiness backlog
  cycle. No child issues, epics, or milestones remain open in GitHub
  for this catalog.
- The active tracked work is organized into three milestone groups:
  `v0.4.0`, `v0.5.0`, and `v0.6.0`.
- The current `main` branch now includes the completed
  production-readiness implementation set; the next maintainer action is
  release or new-scope planning.
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
- Issue `#77` is complete: named scale fixtures now live in
  `config/benchmark_fixtures.yml`, `run-all` now records phase timing
  and throughput metrics, and `benchmark-run` now validates those runs
  against the supported `single_host_container` capacity targets.
- Issue `#78` is complete: backup, restore, and replay runbooks now
  document the supported persisted-state recovery model, and local plus
  CI validation now execute a recovery smoke path that restores review
  state, rebuilds report artifacts, and replays a recovered run.
- Issue `#79` is complete: the release path now emits retained artifact
  hashes, dependency inventory, and dependency-audit outputs through a
  dedicated hardening script and CI artifact job.
- Issue `#80` is complete: production rollout, rollback, governance,
  PII-handling, audit, and consumer-responsibility guidance now live in
  one coherent production operating-model document.

## Open Task Groups

- [x] `v0.4.0` Production data ingestion and persistence
  - Open issues: none. `v0.4.0` implementation work is complete in the
    active backlog catalog.
- [x] `v0.5.0` Review workflow and service surface
  - Open issues: none. `v0.5.0` implementation work is complete in the
    active backlog catalog.
- [x] `v0.6.0` Security, operations, and deployment hardening
  - Open issues: none. `v0.6.0` implementation work is complete in the
    active backlog catalog.

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
- [x] Large-batch benchmark fixtures and SLO-style capacity targets for
  the supported deployment baseline
- [x] Production operating-model documentation

## Next Step

- The tracked production-readiness cycle is complete. The next step is
  to cut a release from the current green `main` state or open a new
  backlog cycle for net-new scope.
