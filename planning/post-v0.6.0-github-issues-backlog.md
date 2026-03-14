# Post-v0.6.0 GitHub Issues Backlog (Historical)

This file records the completed production-readiness GitHub cycle that
followed the bootstrap backlog, the completed `post-v0.1.0` follow-up
backlog, and the published `v0.1.x` hardening releases. It is a
historical record and should not be used as the active source for new
issue creation.

Historical backlog files remain available as read-only records:

- `planning/github-issues-backlog.md`
- `planning/post-v0.1.0-github-issues-backlog.md`

Date prepared: 2026-03-13
Last synced to GitHub: 2026-03-14

## Milestones

- `v0.4.0`: Production data ingestion and persistence
- `v0.5.0`: Review workflow and service surface
- `v0.6.0`: Security, operations, and deployment hardening

## Label Set To Create

- `type:epic`
- `type:feature`
- `type:docs`
- `type:chore`
- `type:bug`
- `area:repo`
- `area:ingest`
- `area:storage`
- `area:normalize`
- `area:matching`
- `area:survivorship`
- `area:workflow`
- `area:service`
- `area:quality`
- `area:security`
- `area:operations`
- `area:ci`
- `area:docs`
- `priority:p0`
- `priority:p1`
- `priority:p2`

## Issue Catalog

## v0.4.0: Production data ingestion and persistence

### 61) Define production batch input manifest and landing-zone contract

- Status: `closed`
- Milestone: `v0.4.0`
- Labels: `type:feature`, `area:ingest`, `priority:p0`
- Depends on: none
- Description:
  - The current runtime assumes internally generated synthetic inputs and fixed file names.
  - Production ETL needs an explicit manifest that describes source systems, landed files, schema versions, and run metadata for real inbound batches.
- Acceptance criteria:
  - A stable manifest format exists for real CSV and Parquet source batches.
  - The runtime validates required manifest fields, source identifiers, and expected input schemas before processing starts.
  - Invalid manifests fail fast with operator-readable errors and no partial run state.

### 62) Implement real-batch ingestion adapters for local and object-storage landing zones

- Status: `closed`
- Milestone: `v0.4.0`
- Labels: `type:feature`, `area:ingest`, `priority:p0`
- Depends on: #61
- Description:
  - The current runtime can read CSV and Parquet files once they are named correctly, but it does not support production-style landed-batch discovery.
  - ETL operations need adapters that resolve manifest entries into concrete input batches from supported landing zones.
- Acceptance criteria:
  - The runtime can ingest a manifest that points at real landed CSV and Parquet source files outside synthetic generation.
  - Local filesystem and object-storage-compatible landing-zone resolution are both supported.
  - Integration tests cover at least one non-synthetic batch path end to end.

### 63) Add persistent relational storage for runs, entities, golden records, and review cases

- Status: `closed`
- Milestone: `v0.4.0`
- Labels: `type:feature`, `area:storage`, `priority:p0`
- Depends on: #61
- Description:
  - Current pipeline state exists only as per-run files under one base directory.
  - Production operation needs durable storage for run metadata, normalized rows, candidate decisions, clusters, golden records, crosswalks, and review state.
- Acceptance criteria:
  - A relational schema exists for run registry, source records, candidate pairs, clusters, golden records, crosswalks, and review cases.
  - The pipeline can persist and reload state from that store.
  - Local integration tests cover schema bootstrap plus one end-to-end persisted run.

### 64) Add idempotent run registry and replay-safe orchestration

- Status: `closed`
- Milestone: `v0.4.0`
- Labels: `type:feature`, `area:operations`, `priority:p0`
- Depends on: #61, #63
- Description:
  - The current file pipeline has no persistent run identity, lifecycle state, or replay protection.
  - Production ETL needs durable run status, retry-safe execution, and explicit replay semantics.
- Acceptance criteria:
  - Every batch run has a persisted run ID, status, timestamps, and failure detail.
  - Re-running the same manifest does not duplicate persisted outputs.
  - Failed runs are resumable or cleanly restartable under a documented operator model.

### 65) Introduce migration tooling and environment-specific runtime configuration

- Status: `closed`
- Milestone: `v0.4.0`
- Labels: `type:chore`, `area:storage`, `priority:p1`
- Depends on: #63
- Description:
  - Production persistence needs schema evolution, environment isolation, and externalized configuration.
  - The current repo config model is repository-local and file-only.
- Acceptance criteria:
  - Database migrations are managed by a first-class migration tool with bootstrap commands.
  - Runtime configuration supports separate dev, test, and production environments without editing committed YAML files in place.
  - Secrets are loaded from environment or secret-store configuration rather than committed files.

### 66) Support incremental loads and cross-run entity refresh

- Status: `closed`
- Milestone: `v0.4.0`
- Labels: `type:feature`, `area:matching`, `priority:p1`
- Depends on: #63, #64
- Description:
  - The current runtime recalculates one isolated batch from scratch.
  - Production ETL needs incremental processing that can reuse prior entity state and update only affected clusters and goldens.
- Acceptance criteria:
  - A new batch can reuse persisted entity state from prior runs.
  - Only affected entities are recalculated when source records change.
  - Replay and backfill semantics are documented and covered by integration tests.

### 67) Publish stable downstream delivery contracts for golden and crosswalk outputs

- Status: `closed`
- Milestone: `v0.4.0`
- Labels: `type:feature`, `area:service`, `priority:p1`
- Depends on: #63, #64
- Description:
  - Current outputs are file artifacts optimized for prototype inspection rather than stable downstream publication.
  - Production consumers need versioned publish contracts for golden-record and crosswalk delivery.
- Acceptance criteria:
  - The runtime can publish stable golden and crosswalk outputs for downstream ETL consumers from persisted state.
  - Publication is atomic at the dataset or snapshot level.
  - Consumer-facing delivery contracts are documented and versioned.

## v0.5.0: Review workflow and service surface

### 68) Implement persisted manual-review case workflow

- Status: `closed`
- Milestone: `v0.5.0`
- Labels: `type:feature`, `area:workflow`, `priority:p0`
- Depends on: #63
- Description:
  - The supported public prototype currently emits a CSV handoff for manual review.
  - A production service needs persisted review cases, state transitions, and operator ownership data.
- Acceptance criteria:
  - Review cases are stored durably with status, assignee, timestamps, and operator notes.
  - The runtime exposes a defined lifecycle for pending, approved, rejected, and deferred review states.
  - Tests cover creation and transition of persisted review cases.

### 69) Apply manual-review decisions to cluster and golden rebuilds

- Status: `closed`
- Milestone: `v0.5.0`
- Labels: `type:feature`, `area:workflow`, `priority:p0`
- Depends on: #66, #68
- Description:
  - Persisted review state only matters if decisions affect entity resolution outcomes.
  - Production operation needs explicit merge and non-merge decisions to override heuristic classification in future runs.
- Acceptance criteria:
  - Approved review decisions can force merges and rejected review decisions can block merges on rerun.
  - Cluster and golden rebuilds apply prior review decisions deterministically.
  - Integration tests cover approved, rejected, and replayed review outcomes.

### 70) Add operator service API for run status, golden records, crosswalk lookups, and review cases

- Status: `closed`
- Milestone: `v0.5.0`
- Labels: `type:feature`, `area:service`, `priority:p0`
- Depends on: #63, #64, #68
- Description:
  - Current interaction is CLI and file-based only.
  - A production data product needs a service interface for operators and downstream systems to query current state.
- Acceptance criteria:
  - A service API exists for run status, golden record lookup, crosswalk lookup, and review-case retrieval.
  - Request and response validation are explicit and integration-tested.
  - The service can be exercised locally and in CI without synthetic-only shortcuts.

### 71) Add operator CLI commands for review decisions, replay, and downstream publication

- Status: `closed`
- Milestone: `v0.5.0`
- Labels: `type:feature`, `area:workflow`, `priority:p1`
- Depends on: #68, #70
- Description:
  - Production operators need non-UI control paths for review decisions, run replay, and publication tasks.
  - The current CLI is pipeline-stage oriented and does not yet expose those operational controls.
- Acceptance criteria:
  - Operator CLI commands exist for applying review decisions, replaying runs, and triggering publication.
  - Command behavior is idempotent where appropriate and surfaces actionable failure output.
  - CLI integration tests cover at least one review-decision and replay path.

### 72) Add downstream export jobs for warehouse and data-product consumers

- Status: `closed`
- Milestone: `v0.5.0`
- Labels: `type:feature`, `area:service`, `priority:p1`
- Depends on: #67, #70
- Description:
  - Production ETL consumers need scheduled or triggered exports, not just interactive queries.
  - The project needs explicit export jobs for warehouse and downstream data-product delivery.
- Acceptance criteria:
  - Export jobs can materialize golden and crosswalk snapshots for downstream consumers.
  - Export runs are tracked and auditable.
  - Warehouse-oriented delivery formats and locations are documented.

### 73) Define API and workflow compatibility contracts for external consumers

- Status: `closed`
- Milestone: `v0.5.0`
- Labels: `type:docs`, `area:docs`, `priority:p2`
- Depends on: #70, #72
- Description:
  - Once service and export interfaces exist, external consumers need explicit compatibility expectations.
  - The current docs define file contracts but not service compatibility policy.
- Acceptance criteria:
  - API and workflow contracts identify stable versus experimental surfaces.
  - Versioning and deprecation expectations are documented for service consumers.
  - Operator and consumer docs reference the same compatibility policy.

## v0.6.0: Security, operations, and deployment hardening

### 74) Add authentication, authorization, and secrets-management baseline

- Status: `closed`
- Milestone: `v0.6.0`
- Labels: `type:feature`, `area:security`, `priority:p0`
- Depends on: #70
- Description:
  - A production service cannot expose run and identity data without access controls and secret handling.
  - The current repo does not yet include a security baseline for service operations.
- Acceptance criteria:
  - Service access requires authenticated callers.
  - Authorization rules distinguish read-only versus privileged review and replay actions.
  - Secrets handling is environment-based and documented for deployment.

### 75) Add structured logging, metrics, health checks, and audit events

- Status: `closed`
- Milestone: `v0.6.0`
- Labels: `type:feature`, `area:operations`, `priority:p0`
- Depends on: #64, #70
- Description:
  - Production ETL and service operation need observability beyond local console output.
  - The current runtime does not yet emit structured operational telemetry or durable audit trails.
- Acceptance criteria:
  - Structured logs exist for run lifecycle, publication, and review-decision activity.
  - Metrics and health endpoints cover service and batch execution status.
  - Audit events are persisted for operator-sensitive actions.

### 76) Containerize the batch and service runtime with deployable environment manifests

- Status: `closed`
- Milestone: `v0.6.0`
- Labels: `type:feature`, `area:operations`, `priority:p1`
- Depends on: #70, #74, #75
- Description:
  - Production deployment requires reproducible runtime packaging and environment manifests.
  - The current repo ships scripts and releases, but not a deployable runtime image or service environment baseline.
- Acceptance criteria:
  - The runtime can be built as a container image for batch and service execution.
  - Deployment manifests exist for at least one supported environment topology.
  - CI validates that the containerized runtime starts and exposes the expected entrypoints.

### 77) Add scale benchmarks, large-batch fixtures, and SLO-oriented capacity targets

- Status: `closed`
- Milestone: `v0.6.0`
- Labels: `type:feature`, `area:quality`, `priority:p1`
- Depends on: #63, #66, #75
- Description:
  - The current test suite proves correctness on prototype-scale datasets, not operational scale.
  - Production readiness needs measurable throughput, latency, and failure-envelope targets.
- Acceptance criteria:
  - Benchmark fixtures exist for larger batch volumes than the current small-profile run.
  - Benchmark runs capture throughput and latency metrics for core pipeline phases.
  - Documented capacity targets or SLO-style expectations exist for supported deployments.

### 78) Add backup, restore, and replay runbooks for persisted state

- Status: `closed`
- Milestone: `v0.6.0`
- Labels: `type:docs`, `area:operations`, `priority:p1`
- Depends on: #63, #64, #76
- Description:
  - Persistent state introduces operational recovery requirements.
  - Production teams need backup, restore, and replay procedures before the system can be trusted operationally.
- Acceptance criteria:
  - Runbooks cover backup and restore of persisted entity and review state.
  - Replay procedures exist for rebuilding outputs from stored run inputs and review decisions.
  - Recovery steps are tested in at least one local or CI smoke path.

### 79) Add supply-chain and release-hardening gates for production artifacts

- Status: `closed`
- Milestone: `v0.6.0`
- Labels: `type:chore`, `area:ci`, `priority:p1`
- Depends on: #74, #76
- Description:
  - Production artifact distribution needs stronger release assurance than the current prototype flow.
  - The repo does not yet enforce supply-chain-oriented checks for deployable artifacts.
- Acceptance criteria:
  - CI produces SBOM or equivalent dependency inventory for released artifacts.
  - Dependency or image scanning is part of the release path.
  - Release docs and automation enforce the new hardening gates.

### 80) Document production rollout, governance boundaries, and support model

- Status: `closed`
- Milestone: `v0.6.0`
- Labels: `type:docs`, `area:docs`, `priority:p2`
- Depends on: #67, #73, #74, #78, #79
- Description:
  - Moving from prototype to enterprise data product requires explicit governance and support boundaries.
  - The current docs describe a safe public prototype, not an operational production rollout model.
- Acceptance criteria:
  - Production rollout guidance identifies supported environments, operator responsibilities, and rollback boundaries.
  - Governance docs identify PII handling, audit expectations, and consumer responsibilities.
  - The production operating model is documented as a coherent release target rather than scattered notes.

## Suggested Epic Issues

Create these 3 epics first, then link child issues:

1. Epic: Production Data Ingestion and Persistence (`v0.4.0`)
2. Epic: Review Workflow and Service Surface (`v0.5.0`)
3. Epic: Security, Operations, and Deployment Hardening (`v0.6.0`)

## Suggested Issue Creation Order

1. Create or confirm labels.
2. Create milestones `v0.4.0`, `v0.5.0`, and `v0.6.0`.
3. Create the 3 epics.
4. Create all child issues and assign them to epics.
5. Execute `v0.4.0` before `v0.5.0`, and `v0.5.0` before `v0.6.0`.

## Tracker Status Snapshot

Snapshot date: 2026-03-14

- Active epic issues in GitHub for this backlog: none
- Open child issues represented in this local active catalog: none
- Open milestones in GitHub for this backlog: none
- Closed milestones in GitHub for this backlog:
  - `v0.4.0`
  - `v0.5.0`
  - `v0.6.0`
- Closed issues and epics in historical local catalogs remain in:
  `planning/github-issues-backlog.md` and
  `planning/post-v0.1.0-github-issues-backlog.md`
- This backlog captures the completed production-readiness cycle that
  moved the project from a file-based identity-resolution prototype
  toward a production MDM or enterprise data-product service baseline.
- Issue `#61` is complete in-repo: the runtime now supports
  manifest-driven landed batch validation for `normalize` and `run-all`
  before partial outputs are written.
- Issue `#62` is complete in-repo: manifest-driven landed batches now
  resolve through local filesystem and object-storage-compatible
  adapters, with end-to-end coverage for `memory://` object storage.
- Issue `#63` is complete in-repo: completed runs and core pipeline
  artifacts now persist into SQLite, and the `report` stage can reload a
  persisted run by `run_id`.
- Issue `#64` is complete in-repo: the SQLite run registry now records
  `running`, `completed`, and `failed` attempts, reuses completed runs
  idempotently, and cleanly restarts failed attempts under the same
  `run_key`.
- Issue `#65` is complete in-repo: persisted state now upgrades through
  Alembic-backed migration commands, and named runtime environments can
  supply config overlays plus `${ENV_VAR}`-resolved secrets without
  editing committed YAML files in place.
- Issue `#66` is complete in-repo: manifest-driven persisted runs now
  support incremental refresh, reusing unaffected predecessor entities
  and recalculating only the affected candidate pairs, clusters, and
  goldens when the config fingerprint remains compatible.
- Issue `#67` is complete in-repo: completed persisted runs can now be
  published under the versioned `golden_crosswalk_snapshot/v1`
  downstream contract with immutable snapshot directories and an atomic
  `current.json` consumer pointer.
- Issue `#68` is complete in-repo: persisted review cases now track
  status, assignee, notes, timestamps, and lifecycle transitions, and
  the runtime exposes JSON-based inspection/update commands over the
  SQLite state store.
- Issue `#69` is complete in-repo: approved and rejected persisted
  review decisions now override later candidate decisions and apply
  deterministically to cluster and golden rebuilds on persisted reruns.
- Issue `#70` is complete in-repo: a read-only FastAPI service now
  exposes persisted run status, golden-record lookup, crosswalk lookup,
  and review-case retrieval with explicit request and response
  validation.
- Issue `#71` is complete in-repo: operator-facing CLI wrappers now
  support idempotent review decisions, manifest-backed replay, and
  downstream publication triggers with JSON output.
- Issue `#72` is complete in-repo: named warehouse and data-product
  export jobs now materialize persisted golden/crosswalk snapshots
  through audited SQLite export-run records and JSON operator commands
  for export execution history.
- Issue `#73` is complete in-repo: service, workflow, and delivery docs
  now share an explicit compatibility policy covering stable versus
  internal surfaces, versioning rules, and deprecation expectations for
  external consumers.
- Issue `#74` is complete in-repo: the service now requires API-key
  authentication, distinguishes `reader` versus `operator` access, and
  loads service secrets through environment-backed runtime config.
- Issue `#75` is complete in-repo: batch and service paths now emit
  structured JSON logs, the service exposes authenticated `healthz`,
  `readyz`, and `/api/v1/metrics` endpoints, and privileged operator
  actions now persist audit events in SQLite.
- Issue `#76` is complete in-repo: the repo now ships a shared
  batch/service container image, a single-host compose deployment
  baseline, and a reusable container smoke test wired into CI.
- Issue `#77` is complete in-repo: named benchmark fixtures now live in
  `config/benchmark_fixtures.yml`, `run-all` now emits phase timing and
  throughput metrics in `run_summary.json`, and `benchmark-run` now
  executes the real persisted pipeline against the supported
  `single_host_container` capacity targets.
- Issue `#78` is complete in-repo: backup, restore, and replay runbooks
  now document the supported manifest-era recovery model, and
  `scripts/run_checks.py` now runs a persisted-state recovery smoke path
  that validates state restore, report rebuild, and replay with an
  approved review override.
- Issue `#79` is complete in-repo: CI now runs a retained
  `release-hardening` job, `scripts/release_hardening_check.py` now
  builds release artifacts plus dependency inventory and audit outputs,
  and the release-process docs now require those hardening artifacts as
  part of the release gate.
- Issue `#80` is complete in-repo: rollout, rollback, governance, PII,
  audit, and consumer-responsibility guidance now live in one coherent
  production operating-model document linked from the architecture,
  safety, security, and README surfaces.
- Epic issues `#58`, `#59`, and `#60` are complete in GitHub and can
  now be treated as closed tracker history alongside their child issue
  sets.
