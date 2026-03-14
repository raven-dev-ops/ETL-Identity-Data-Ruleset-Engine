# Active GitHub Issues Backlog

This backlog is the current source of truth for new GitHub issues after
the completed bootstrap backlog, the completed `post-v0.1.0` follow-up
backlog, and the completed `v0.6.0` production-readiness cycle.
Historical backlog files remain available as read-only records:

- `planning/github-issues-backlog.md`
- `planning/post-v0.1.0-github-issues-backlog.md`
- `planning/post-v0.6.0-github-issues-backlog.md`

Date prepared: 2026-03-14
Last synced to GitHub: 2026-03-14

## Milestones

- `v0.7.0`: Durable replay and database platform
- `v0.8.0`: Enterprise access and service control plane
- `v0.9.0`: Distributed deployment and event-driven runtime

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

## v0.7.0: Durable replay and database platform

### 93) Add PostgreSQL state-store backend and dialect test coverage

- Status: `closed`
- Milestone: `v0.7.0`
- Labels: `type:feature`, `area:storage`, `priority:p0`
- Depends on: none
- Description:
  - The published `v0.6.0` line relies on SQLite as the supported
    persisted-state backend.
  - The next production step needs a networked relational backend that
    can support concurrent service and batch usage without the
    single-host SQLite constraint.
- Acceptance criteria:
  - The runtime supports PostgreSQL-backed persisted state alongside
    SQLite.
  - Alembic migrations run cleanly against PostgreSQL in CI.
  - Persistence, replay, publication, and service integration tests pass
    against the PostgreSQL backend.

### 94) Archive manifest and landed-input snapshots as immutable replay bundles

- Status: `closed`
- Milestone: `v0.7.0`
- Labels: `type:feature`, `area:ingest`, `priority:p0`
- Depends on: #93
- Description:
  - The current recovery model depends on operators restoring the
    original manifest path and landed input locations.
  - Production replay needs an immutable archived snapshot of those
    inputs so recovery does not depend on the original landing zone.
- Acceptance criteria:
  - Manifest-driven runs can persist an immutable replay bundle that
    includes the manifest plus referenced landed inputs.
  - Replay bundles are addressable from persisted run metadata.
  - Operators can verify bundle completeness before a run is marked
    recoverable.

### 95) Support replay from archived bundles without original landing paths

- Status: `closed`
- Milestone: `v0.7.0`
- Labels: `type:feature`, `area:storage`, `priority:p0`
- Depends on: #94
- Description:
  - Archived inputs only help if operators can replay directly from the
    archived bundle.
  - The runtime currently requires the original `manifest_path` and
    landing-zone layout to exist again.
- Acceptance criteria:
  - `replay-run` can execute from an archived replay bundle without the
    original landing-zone paths.
  - Recovery docs and smoke coverage prove replay from archived inputs.
  - Persisted run metadata records whether a run is replayable from an
    archived bundle alone.

### 96) Add checkpointed resume for failed persisted runs

- Status: `closed`
- Milestone: `v0.7.0`
- Labels: `type:feature`, `area:operations`, `priority:p1`
- Depends on: #93
- Description:
  - The current restart model is a clean rerun after failure.
  - Larger production batches need stage checkpoints so operators can
    resume work instead of rerunning the entire batch.
- Acceptance criteria:
  - Failed persisted runs can resume from documented stage checkpoints.
  - Checkpoint state is durable and auditable.
  - Integration tests cover resume after failure for at least one
    manifest-driven persisted batch.

## v0.8.0: Enterprise access and service control plane

### 97) Add OIDC or JWT service authentication for enterprise identity providers

- Status: `closed`
- Milestone: `v0.8.0`
- Labels: `type:feature`, `area:security`, `priority:p0`
- Depends on: none
- Description:
  - The current service baseline uses static API keys.
  - Enterprise deployments need service authentication that can
    integrate with external identity providers.
- Acceptance criteria:
  - The service supports OIDC or JWT bearer authentication backed by
    deployment-provided identity metadata.
  - Existing API-key auth remains documented as a simpler compatibility
    mode or is explicitly deprecated.
  - Integration tests cover authenticated service access with external
    identity claims.

### 98) Add fine-grained RBAC scopes and actor identity propagation

- Status: `closed`
- Milestone: `v0.8.0`
- Labels: `type:feature`, `area:security`, `priority:p0`
- Depends on: #97
- Description:
  - The current service and operator surface distinguishes only
    `reader` and `operator`.
  - Production control planes need narrower permissions and auditable
    end-user identity propagation.
- Acceptance criteria:
  - Service and operator actions enforce finer-grained scopes than the
    current two-role model.
  - Audit events persist the authenticated actor identity and scope
    context.
  - Docs define the supported RBAC model for operators and consumers.

### 99) Expose publish and export-job triggers over the service API

- Status: `closed`
- Milestone: `v0.8.0`
- Labels: `type:feature`, `area:service`, `priority:p1`
- Depends on: #98
- Description:
  - Publication and export orchestration remain CLI-driven.
  - Operators and downstream platforms need a service-level control path
    for those actions.
- Acceptance criteria:
  - The service exposes authenticated publish and export-job trigger
    endpoints.
  - Triggered actions remain auditable and idempotent where applicable.
  - Compatibility and security docs cover the new control-plane
    endpoints.

### 100) Add paginated list and search endpoints for runs, goldens, and review cases

- Status: `closed`
- Milestone: `v0.8.0`
- Labels: `type:feature`, `area:service`, `priority:p1`
- Depends on: #97
- Description:
  - The current service surface is record-oriented and limited to direct
    lookups plus a few list endpoints.
  - Production operators and consumers need stable list, filter, and
    pagination behavior for larger datasets.
- Acceptance criteria:
  - The service exposes paginated list endpoints for persisted runs,
    golden records, and review cases.
  - Filtering and sort semantics are documented and integration-tested.
  - The compatibility policy identifies the stable pagination contract.

## v0.9.0: Distributed deployment and event-driven runtime

### 101) Add Kubernetes or Helm deployment manifests for PostgreSQL-backed service topology

- Status: `closed`
- Milestone: `v0.9.0`
- Labels: `type:feature`, `area:operations`, `priority:p0`
- Depends on: #93, #97, #98
- Description:
  - The current deployment baseline is a single-host compose topology.
  - Production teams need a documented clustered deployment option for
    the service and batch runtime.
- Acceptance criteria:
  - The repo ships Kubernetes or Helm manifests for the supported
    PostgreSQL-backed deployment topology.
  - Deployment docs cover secrets, service exposure, and state-store
    wiring for the clustered path.
  - CI validates the deployment assets syntactically and through at
    least one smoke path.

### 102) Add image signing, SBOM attestation, and container scanning gates

- Status: `closed`
- Milestone: `v0.9.0`
- Labels: `type:chore`, `area:ci`, `priority:p1`
- Depends on: #101
- Description:
  - The release path now emits dependency inventory and audit outputs
    for built Python artifacts.
  - Containerized deployments need equivalent provenance and image-level
    security gates.
- Acceptance criteria:
  - Container images are signed or attested as part of the release path.
  - CI emits SBOM or provenance data for the image artifacts.
  - Image scanning is enforced before a production image is published.

### 103) Add event-driven ingestion and streaming entity-refresh mode

- Status: `closed`
- Milestone: `v0.9.0`
- Labels: `type:feature`, `area:ingest`, `priority:p1`
- Depends on: #93, #96
- Description:
  - The current runtime is batch-oriented.
  - Some production integrations need near-real-time ingestion and
    entity refresh without waiting for batch windows.
- Acceptance criteria:
  - The runtime supports an event-driven or streaming ingestion mode.
  - Entity refresh semantics remain deterministic and auditable.
  - Benchmarks and tests cover at least one continuous-ingest scenario.

### 104) Add clustered benchmark fixtures and SLO targets for distributed deployments

- Status: `closed`
- Milestone: `v0.9.0`
- Labels: `type:feature`, `area:quality`, `priority:p2`
- Depends on: #101, #103
- Description:
  - The current benchmark target is the single-host container path.
  - The next deployment line needs measurable throughput and latency
    expectations for the clustered topology.
- Acceptance criteria:
  - Benchmark fixtures exist for the distributed deployment path.
  - Benchmark outputs capture SLO-style latency and throughput metrics
    for the clustered runtime.
  - Capacity targets are documented for the supported distributed
    deployment baseline.

## Suggested Epic Issues

Create these 3 epics first, then link child issues:

1. Epic: Durable Replay and Database Platform (`v0.7.0`)
2. Epic: Enterprise Access and Service Control Plane (`v0.8.0`)
3. Epic: Distributed Deployment and Event-Driven Runtime (`v0.9.0`)

## Suggested Issue Creation Order

1. Create or confirm labels.
2. Create milestones `v0.7.0`, `v0.8.0`, and `v0.9.0`.
3. Create the 3 epics.
4. Create all child issues and assign them to epics.
5. Execute `v0.7.0` before `v0.8.0`, and `v0.8.0` before `v0.9.0`.

## Tracker Status Snapshot

Snapshot date: 2026-03-14

- Active epic issues in GitHub for this backlog: `#81`, `#82`, `#83`
- Open child issues represented in this local active catalog:
  none
- Open milestones in GitHub for this backlog:
  none after closeout of epics `#81`, `#82`, and `#83`
- Closed backlog history remains in:
  `planning/github-issues-backlog.md`,
  `planning/post-v0.1.0-github-issues-backlog.md`, and
  `planning/post-v0.6.0-github-issues-backlog.md`
- This backlog opens the post-`v0.6.0` cycle focused on durable replay,
  enterprise access control, and distributed deployment/runtime gaps
  that remain outside the current production target.
