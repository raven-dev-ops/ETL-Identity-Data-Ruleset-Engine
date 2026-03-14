# Active GitHub Issues Backlog

This backlog is the current source of truth for new GitHub issues after
the completed bootstrap backlog, the completed `post-v0.1.0` follow-up
backlog, and the published `v0.1.x` hardening releases. Historical
backlog files remain available as read-only records:

- `planning/github-issues-backlog.md`
- `planning/post-v0.1.0-github-issues-backlog.md`

Date prepared: 2026-03-13
Last synced to GitHub: 2026-03-13

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

- Status: `open`
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

- Status: `open`
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

- Status: `open`
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

- Status: `open`
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

- Status: `open`
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

- Status: `open`
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

- Status: `open`
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

- Status: `open`
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

- Status: `open`
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

- Status: `open`
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

- Status: `open`
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

- Status: `open`
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

- Status: `open`
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

- Status: `open`
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

- Status: `open`
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

- Status: `open`
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

- Status: `open`
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

- Status: `open`
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

- Status: `open`
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

Snapshot date: 2026-03-13

- Active epic issues in GitHub for this backlog: `#58`, `#59`, `#60`
- Open child issues represented in this local active catalog: `#62`
  through `#80`
- Open milestones in GitHub for this backlog:
  - `v0.4.0` with epic `#58` plus child issues `#62` through `#67`
  - `v0.5.0` with epic `#59` plus child issues `#68` through `#73`
  - `v0.6.0` with epic `#60` plus child issues `#74` through `#80`
- Closed issues and epics in historical local catalogs remain in:
  `planning/github-issues-backlog.md` and
  `planning/post-v0.1.0-github-issues-backlog.md`
- This backlog opens a new production-readiness cycle focused on moving
  the project from a file-based identity-resolution prototype toward a
  production MDM or enterprise data-product service.
- Issue `#61` is complete in-repo: the runtime now supports
  manifest-driven landed batch validation for `normalize` and `run-all`
  before partial outputs are written.
