# Active GitHub Issues Backlog

This backlog is the current source of truth for new GitHub issues after
the completed bootstrap backlog, the completed `post-v0.1.0` follow-up
backlog, the completed `v0.6.0` production-readiness cycle, the
completed `v0.7.0`-`v0.9.0` delivery cycle, the completed
`v1.0.0`-`v1.2.0` customer-pilot cycle, and the completed
`v1.3.0`-`v1.5.0` supportability cycle. Historical backlog files remain
available as read-only records:

- `planning/github-issues-backlog.md`
- `planning/post-v0.1.0-github-issues-backlog.md`
- `planning/post-v0.6.0-github-issues-backlog.md`
- `planning/post-v0.9.0-github-issues-backlog.md`
- `planning/post-v1.2.0-github-issues-backlog.md`
- `planning/post-v1.5.0-github-issues-backlog.md`

Date prepared: 2026-03-15
Last synced to GitHub: 2026-03-15

## Milestones

- `v1.2.0-multi-tenant-foundation`: Multi-tenant security and high-availability foundation
- `v1.3.0-live-integrations`: Live CAD/RMS integration targets
- `v1.4.0-cjis-acceptance`: Production acceptance and CJIS operating controls

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

## v1.2.0-multi-tenant-foundation: Multi-tenant security and high-availability foundation

### 129) Add tenant-aware persisted-state boundaries across runs and derived artifacts

- Status: `open`
- Milestone: `v1.2.0-multi-tenant-foundation`
- Labels: `type:feature`, `area:storage`, `priority:p0`
- Depends on: none
- Description:
  - The current persisted-state model assumes one deployment boundary
    rather than multiple tenant-specific identity domains.
  - Production use needs explicit tenant identifiers on runs, review
    cases, audit events, golden records, and public-safety activity so
    operators can prove state separation.
- Acceptance criteria:
  - The persisted-state schema adds a supported tenant boundary for core
    run, review, audit, and public-safety artifacts.
  - CLI and runtime flows can declare the tenant context for batch runs,
    replay, publish, export, and support operations.
  - Tests prove that persisted artifacts remain tenant-bounded.

### 130) Add tenant-scoped service authorization and query enforcement

- Status: `open`
- Milestone: `v1.2.0-multi-tenant-foundation`
- Labels: `type:feature`, `area:service`, `priority:p0`
- Depends on: #129
- Description:
  - The service currently enforces role and scope boundaries, but not
    tenant-specific read or write boundaries.
  - Production multi-tenant use needs every read-side and operator-side
    request to bind to an explicit tenant context.
- Acceptance criteria:
  - The service auth model supports tenant claims or equivalent
    deployment-supplied tenant bindings.
  - Read, review, replay, publish, export, and admin-console surfaces
    reject cross-tenant access.
  - Tests cover allowed and forbidden tenant access paths.

### 131) Add field-level authorization hooks for sensitive identity attributes

- Status: `open`
- Milestone: `v1.2.0-multi-tenant-foundation`
- Labels: `type:feature`, `area:security`, `priority:p1`
- Depends on: #130
- Description:
  - The current service line documents only endpoint-level role and
    scope enforcement.
  - Customer production rollouts may need policy-driven redaction or
    suppression for specific identity fields in service, export, or demo
    read models.
- Acceptance criteria:
  - The repo defines a supported policy hook for field-level masking or
    denial on documented read surfaces.
  - The service and export paths apply those policies consistently.
  - Docs define the supported policy boundary and fallback behavior.

### 132) Add a high-availability PostgreSQL deployment baseline and failover runbook

- Status: `open`
- Milestone: `v1.2.0-multi-tenant-foundation`
- Labels: `type:feature`, `area:operations`, `priority:p1`
- Depends on: #129
- Description:
  - The documented deployment baselines currently stop at single-node
    PostgreSQL.
  - Production acceptance needs a repo-supported HA reference topology
    and a documented failover, backup, and rollback story.
- Acceptance criteria:
  - The repo ships a documented HA PostgreSQL reference baseline.
  - Recovery and rollback runbooks cover failover and restore for that
    topology.
  - Smoke or rehearsal validation proves the supported commands and
    manifests are coherent.

## v1.3.0-live-integrations: Live CAD/RMS integration targets

### 133) Add the first packaged live CAD vendor integration target

- Status: `open`
- Milestone: `v1.3.0-live-integrations`
- Labels: `type:feature`, `area:ingest`, `priority:p0`
- Depends on: none
- Description:
  - The repo now ships synthetic vendor profiles, but not a maintained
    integration target for a real CAD export used in customer pilots.
  - The next step is a supported live CAD integration pack that keeps
    acquisition, mapping, and onboarding concrete.
- Acceptance criteria:
  - The repo ships a documented integration pack for one target CAD
    export shape.
  - The onboarding path can validate landed extracts for that target
    without ad hoc operator edits.
  - Docs define how customer-specific deployment variables plug into the
    target.

### 134) Add the first packaged live RMS vendor integration target

- Status: `open`
- Milestone: `v1.3.0-live-integrations`
- Labels: `type:feature`, `area:ingest`, `priority:p0`
- Depends on: none
- Description:
  - RMS onboarding has the same gap as CAD onboarding: the current repo
    validates synthetic vendor shapes but not a maintained live target.
  - A production pilot needs one documented RMS integration target with
    the same explicit contract and landing assumptions.
- Acceptance criteria:
  - The repo ships a documented integration pack for one target RMS
    export shape.
  - The onboarding path can validate landed extracts for that target
    without ad hoc operator edits.
  - Docs define how customer-specific deployment variables plug into the
    target.

### 135) Add secure landed-file acquisition and chain-of-custody workflow for vendor batches

- Status: `open`
- Milestone: `v1.3.0-live-integrations`
- Labels: `type:feature`, `area:operations`, `priority:p1`
- Depends on: #133, #134
- Description:
  - The runtime now expects landed files or object-storage paths, but it
    does not yet define a supported acquisition and custody workflow for
    live vendor exports.
  - Production pilots need a documented path from customer drop zone to
    validated manifest input.
- Acceptance criteria:
  - The repo defines a supported landing and custody workflow for live
    vendor batches.
  - The workflow records enough metadata for audit and replay.
  - Docs clearly separate live landed-input handling from synthetic repo
    fixtures.

### 136) Add masked acceptance fixtures and drift-report workflow for live customer onboarding

- Status: `open`
- Milestone: `v1.3.0-live-integrations`
- Labels: `type:feature`, `area:quality`, `priority:p1`
- Depends on: #133, #134, #135
- Description:
  - Customer onboarding conversations need concrete proof artifacts
    without pulling operational records into the repo.
  - The repo should support masked acceptance fixtures and repeatable
    drift-report packaging for live integration rehearsals.
- Acceptance criteria:
  - The repo ships a documented masked-fixture workflow for customer
    onboarding acceptance.
  - Drift-report output can be generated and reviewed without leaking
    operational identity data into the repo.
  - Tests or smoke coverage validate the masked-fixture packaging path.

## v1.4.0-cjis-acceptance: Production acceptance and CJIS operating controls

### 137) Add environment promotion and sealing workflow for protected pilot deployments

- Status: `open`
- Milestone: `v1.4.0-cjis-acceptance`
- Labels: `type:feature`, `area:operations`, `priority:p0`
- Depends on: #132, #135
- Description:
  - The repo now supports customer pilots and secure evidence packs, but
    it does not yet define how a validated lower environment becomes a
    protected pilot environment in a controlled way.
  - Production acceptance needs a documented promotion and environment
    sealing workflow.
- Acceptance criteria:
  - The repo documents a supported promotion path from validation to a
    protected pilot environment.
  - The workflow defines immutable config, artifact, and evidence inputs
    required for promotion.
  - Rollback and revalidation steps are documented.

### 138) Add CJIS operating-controls evidence capture and review cadence automation

- Status: `open`
- Milestone: `v1.4.0-cjis-acceptance`
- Labels: `type:feature`, `area:docs`, `priority:p1`
- Depends on: #137
- Description:
  - The current CJIS evidence pack supports point-in-time review, but it
    does not yet define a repeatable review cadence for ongoing
    operation.
  - Customer acceptance needs a documented operating-controls evidence
    loop rather than one-off artifact generation.
- Acceptance criteria:
  - The repo defines a repeatable evidence-capture and review cadence
    for the CJIS-sensitive deployment baseline.
  - The evidence pack and operational docs align with that cadence.
  - Docs clearly separate repo-side evidence support from agency-side
    compliance obligations.

### 139) Add a production acceptance suite and cutover readiness report

- Status: `open`
- Milestone: `v1.4.0-cjis-acceptance`
- Labels: `type:feature`, `area:quality`, `priority:p1`
- Depends on: #137, #138
- Description:
  - The repo has many individual checks, but not one coherent acceptance
    suite that says a protected pilot is ready for cutover.
  - Production deployment needs a single readiness artifact that pulls
    the critical checks together.
- Acceptance criteria:
  - The repo ships a documented production acceptance suite for the
    protected pilot baseline.
  - The suite produces a machine-readable readiness report.
  - Docs define which failures are blocking and which are advisory.

### 140) Add incident response, audit review, and operator training package for customer handoff

- Status: `open`
- Milestone: `v1.4.0-cjis-acceptance`
- Labels: `type:docs`, `area:operations`, `priority:p2`
- Depends on: #138, #139
- Description:
  - The product runtime is now strong enough that the remaining gap is
    operator execution quality in a protected environment.
  - Customer handoff needs a packaged training and response baseline
    rather than scattered operational notes.
- Acceptance criteria:
  - The repo ships a documented incident-response and audit-review
    package for customer operators.
  - Operator training material aligns with the supported runtime,
    service, support-bundle, and evidence surfaces.
  - The handoff package states which responsibilities remain outside the
    repo and inside agency or customer governance.

## Suggested Epic Issues

Create these 3 epics first, then link child issues:

1. Epic: Multi-tenant security and high-availability foundation (`v1.2.0-multi-tenant-foundation`)
2. Epic: Live CAD/RMS integration targets (`v1.3.0-live-integrations`)
3. Epic: Production acceptance and CJIS operating controls (`v1.4.0-cjis-acceptance`)

## Suggested Issue Creation Order

1. Create or confirm labels.
2. Create milestones `v1.2.0-multi-tenant-foundation`, `v1.3.0-live-integrations`, and `v1.4.0-cjis-acceptance`.
3. Create the 3 epics.
4. Create all child issues and assign them to epics.
5. Execute `v1.2.0-multi-tenant-foundation` before `v1.3.0-live-integrations`, and `v1.3.0-live-integrations` before `v1.4.0-cjis-acceptance`.

## Tracker Status Snapshot

Snapshot date: 2026-03-15

- This backlog opens the post-`v1.1.0` cycle focused on tenant
  boundaries, live CAD/RMS integration targets, and production
  acceptance with CJIS operating controls.
- Active epic issues in GitHub for this backlog:
  `#126`, `#127`, `#128`
- Open child issues represented in this local active catalog:
  `#129` through `#140`
- Open milestones in GitHub for this backlog:
  `v1.2.0-multi-tenant-foundation`, `v1.3.0-live-integrations`, and
  `v1.4.0-cjis-acceptance`
