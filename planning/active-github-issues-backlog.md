# Active GitHub Issues Backlog

This backlog is the current source of truth for new GitHub issues after
the completed bootstrap backlog, the completed `post-v0.1.0` follow-up
backlog, the completed `v0.6.0` production-readiness cycle, and the
completed `v0.7.0`-`v0.9.0` delivery cycle. Historical backlog files
remain available as read-only records:

- `planning/github-issues-backlog.md`
- `planning/post-v0.1.0-github-issues-backlog.md`
- `planning/post-v0.6.0-github-issues-backlog.md`
- `planning/post-v0.9.0-github-issues-backlog.md`

Date prepared: 2026-03-14
Last synced to GitHub: 2026-03-14

## Milestones

- `v1.0.0`: CAD/RMS source contracts and validation
- `v1.1.0`: Public safety onboarding and identity mapping
- `v1.2.0`: Customer deployment packaging and pilot handoff

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

## v1.0.0: CAD/RMS source contracts and validation

### 105) Add versioned CAD call-for-service source contracts and validators

- Status: `closed`
- Milestone: `v1.0.0`
- Labels: `type:feature`, `area:ingest`, `priority:p0`
- Depends on: none
- Description:
  - The public-safety demo currently relies on synthetic sidecar data
    and internal assumptions about CAD-shaped records.
  - Customer onboarding needs an explicit, versioned CAD contract for
    incident, involved-person, and source-person extracts.
- Acceptance criteria:
  - The repo ships a documented CAD batch contract with required files,
    columns, and version markers.
  - Validation rejects missing or malformed CAD contract inputs before
    runtime execution.
  - Tests cover valid and invalid CAD contract examples.

### 106) Add versioned RMS report/person source contracts and validators

- Status: `closed`
- Milestone: `v1.0.0`
- Labels: `type:feature`, `area:ingest`, `priority:p0`
- Depends on: none
- Description:
  - The current runtime does not expose a formal RMS onboarding
    contract, even though the demo narrative depends on RMS-style
    report and person data.
  - Customer pilots need a stable RMS contract that can be discussed
    with source-system owners and integration teams.
- Acceptance criteria:
  - The repo ships a documented RMS batch contract with required files,
    columns, and version markers.
  - Validation rejects missing or malformed RMS contract inputs before
    runtime execution.
  - Tests cover valid and invalid RMS contract examples.

### 107) Extend the manifest model with named CAD/RMS source bundles and source-class validation

- Status: `closed`
- Milestone: `v1.0.0`
- Labels: `type:feature`, `area:ingest`, `priority:p0`
- Depends on: #105, #106
- Description:
  - The current manifest model is file-oriented but not explicit about
    public-safety source bundles such as CAD and RMS extracts.
  - Production onboarding needs manifest-era validation that the full
    required bundle for each source class is present and internally
    consistent.
- Acceptance criteria:
  - Manifests can declare CAD and RMS source bundles with named source
    classes and contract versions.
  - Validation enforces required-file completeness for each declared
    public-safety source bundle.
  - Contract-aware manifest tests cover mixed CAD/RMS ingestion.

### 108) Add contract conformance tooling and onboarding fixtures for CAD/RMS batches

- Status: `closed`
- Milestone: `v1.0.0`
- Labels: `type:feature`, `area:quality`, `priority:p1`
- Depends on: #107
- Description:
  - Contract docs alone are not enough for customer onboarding and
    vendor conversations.
  - The repo needs repeatable conformance tooling plus realistic sample
    fixtures that show how CAD and RMS batches should be structured.
- Acceptance criteria:
  - The CLI exposes a contract-conformance check for CAD/RMS bundles.
  - The repo ships onboarding fixtures and example manifests for
    contract-valid CAD and RMS batches.
  - Docs explain how source owners can self-check their payloads before
    pipeline onboarding.

## v1.1.0: Public safety onboarding and identity mapping

### 109) Add CAD/RMS field-mapping overlays for vendor-specific source columns

- Status: `closed`
- Milestone: `v1.1.0`
- Labels: `type:feature`, `area:normalize`, `priority:p0`
- Depends on: #107
- Description:
  - Real CAD and RMS feeds will not arrive in the exact canonical field
    names used by the current synthetic runtime.
  - The onboarding path needs configurable mapping overlays that
    translate vendor-specific columns into the supported canonical
    person and incident shapes.
- Acceptance criteria:
  - The runtime can load source-specific mapping overlays for CAD and
    RMS inputs.
  - Mapping overlays support both person attributes and incident/link
    attributes.
  - Tests cover at least two distinct source-shape examples.

### 110) Add a public-safety activity ingestion path from contract inputs to persisted state

- Status: `closed`
- Milestone: `v1.1.0`
- Labels: `type:feature`, `area:workflow`, `priority:p0`
- Depends on: #109
- Description:
  - The current public-safety slice is demo-oriented and not yet a
    first-class persisted ingestion path from formal CAD/RMS contracts.
  - Customer demonstrations and pilots need the activity model to be
    built directly from validated contract inputs.
- Acceptance criteria:
  - Manifest-driven runs can ingest contract-valid CAD/RMS bundles into
    persisted state.
  - The runtime persists incident-to-identity activity data alongside
    the existing golden-person outputs.
  - End-to-end tests cover the contract-to-persisted-state path.

### 111) Add service and demo-shell read models for incident-to-identity activity views

- Status: `closed`
- Milestone: `v1.1.0`
- Labels: `type:feature`, `area:service`, `priority:p1`
- Depends on: #110
- Description:
  - The current service and demo shell expose identity outputs, but the
    public-safety activity view is still primarily generated as offline
    artifacts.
  - Customer-facing walkthroughs need stable read models that can serve
    CAD/RMS incident activity directly from persisted state.
- Acceptance criteria:
  - The service exposes documented read endpoints for persisted
    incident-to-identity activity.
  - The Django demo shell can render the same activity model from the
    persisted state or packaged pilot bundle.
  - Compatibility docs define the supported read-model contract.

### 112) Add regression fixtures for cross-system identity scenarios and false-merge guards

- Status: `closed`
- Milestone: `v1.1.0`
- Labels: `type:feature`, `area:quality`, `priority:p1`
- Depends on: #110
- Description:
  - A real public-safety onboarding story needs stronger proof around
    same-person merges, same-household separations, and cross-system
    false-positive avoidance.
  - The current synthetic suite does not yet package those cases as a
    focused regression set for CAD/RMS onboarding.
- Acceptance criteria:
  - The repo ships explicit regression fixtures for same-person, same-
    household, and false-merge public-safety scenarios.
  - Matching and survivorship tests guard the expected outcomes for
    those cases.
  - Demo documentation references the canonical regression scenarios.

## v1.2.0: Customer deployment packaging and pilot handoff

### 113) Package a standalone customer pilot bundle with seeded state and launch scripts

- Status: `closed`
- Milestone: `v1.2.0`
- Labels: `type:feature`, `area:operations`, `priority:p0`
- Depends on: #111
- Description:
  - The current demo bundle is useful for internal walkthroughs but is
    not yet shaped like a customer pilot handoff package.
  - Customer-facing pilots need a deterministic bundle that includes
    the seeded state, demo shell, launch helpers, and supporting docs.
- Acceptance criteria:
  - The repo can build a standalone customer pilot bundle from a seeded
    public-safety dataset.
  - The bundle includes the demo shell, persisted state, and startup
    helpers needed for a local pilot walkthrough.
  - Packaging tests verify bundle completeness and deterministic naming.

### 114) Add a Windows-first single-host pilot installer/bootstrap for the Django and PostgreSQL baseline

- Status: `closed`
- Milestone: `v1.2.0`
- Labels: `type:feature`, `area:operations`, `priority:p1`
- Depends on: #113
- Description:
  - The current deployment assets assume maintainer-level familiarity
    with Python, containers, and manual setup.
  - A customer pilot needs a simpler Windows-first bootstrap path that
    reflects how the demo will actually be evaluated.
- Acceptance criteria:
  - The repo ships a documented Windows-first bootstrap path for the
    supported single-host pilot topology.
  - The bootstrap prepares the Django shell, state store, and runtime
    configuration with minimal manual steps.
  - Smoke coverage validates the pilot bootstrap on the supported host
    path.

### 115) Add a customer environment readiness check and signed handoff manifest

- Status: `closed`
- Milestone: `v1.2.0`
- Labels: `type:feature`, `area:security`, `priority:p1`
- Depends on: #114
- Description:
  - Customer pilots need a concrete way to prove what artifact set was
    delivered and whether the target environment meets the documented
    prerequisites.
  - The current repo has release-hardening and CJIS preflight checks,
    but not a customer handoff manifest for pilot installs.
- Acceptance criteria:
  - The repo ships a pilot readiness check for the customer deployment
    baseline.
  - The customer pilot bundle includes a signed or hashed manifest of
    the delivered artifacts and versions.
  - Docs define the intended operator use of the readiness output and
    handoff manifest.

### 116) Add operator/admin runbooks and a pilot acceptance checklist for customer handoff

- Status: `closed`
- Milestone: `v1.2.0`
- Labels: `type:docs`, `area:docs`, `priority:p1`
- Depends on: #115
- Description:
  - A customer pilot is not complete when the artifacts exist but the
    delivery and acceptance steps still live only in maintainer context.
  - The repo needs explicit runbooks and acceptance criteria for the
    handoff itself.
- Acceptance criteria:
  - The repo ships operator/admin runbooks for install, startup,
    rollback, backup, and demo execution.
  - The repo ships a pilot acceptance checklist that can be used during
    customer handoff.
  - The README and planning docs point to the pilot handoff material.

## Suggested Epic Issues

Create these 3 epics first, then link child issues:

1. Epic: CAD/RMS Source Contracts and Validation (`v1.0.0`)
2. Epic: Public Safety Onboarding and Identity Mapping (`v1.1.0`)
3. Epic: Customer Deployment Packaging and Pilot Handoff (`v1.2.0`)

## Suggested Issue Creation Order

1. Create or confirm labels.
2. Create milestones `v1.0.0`, `v1.1.0`, and `v1.2.0`.
3. Create the 3 epics.
4. Create all child issues and assign them to epics.
5. Execute `v1.0.0` before `v1.1.0`, and `v1.1.0` before `v1.2.0`.

## Tracker Status Snapshot

Snapshot date: 2026-03-14

- Active epic issues in GitHub for this backlog:
  none
- Open child issues represented in this local active catalog:
  none
- Open milestones in GitHub for this backlog:
  none
- Closed backlog history remains in:
  `planning/github-issues-backlog.md`,
  `planning/post-v0.1.0-github-issues-backlog.md`,
  `planning/post-v0.6.0-github-issues-backlog.md`, and
  `planning/post-v0.9.0-github-issues-backlog.md`
- This backlog opens the post-`v0.9.2` cycle focused on formal CAD/RMS
  source contracts, public-safety onboarding, and customer-facing pilot
  packaging needed beyond the current production and demo baseline.
