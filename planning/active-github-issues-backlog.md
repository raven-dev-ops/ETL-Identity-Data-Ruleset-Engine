# Active GitHub Issues Backlog

This backlog is the current source of truth for new GitHub issues after
the completed bootstrap backlog, the completed `post-v0.1.0` follow-up
backlog, the completed `v0.6.0` production-readiness cycle, the
completed `v0.7.0`-`v0.9.0` delivery cycle, and the completed
`v1.0.0`-`v1.2.0` customer-pilot cycle. Historical backlog files remain
available as read-only records:

- `planning/github-issues-backlog.md`
- `planning/post-v0.1.0-github-issues-backlog.md`
- `planning/post-v0.6.0-github-issues-backlog.md`
- `planning/post-v0.9.0-github-issues-backlog.md`
- `planning/post-v1.2.0-github-issues-backlog.md`

Date prepared: 2026-03-14
Last synced to GitHub: 2026-03-14

## Milestones

- `v1.3.0`: Vendor adapter packs and onboarding acceleration
- `v1.4.0`: Secure operations and compliance evidence
- `v1.5.0`: Customer deployment automation and supportability

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

## v1.3.0: Vendor adapter packs and onboarding acceleration

### 117) Add packaged vendor profile overlays for common CAD exports

- Status: `open`
- Milestone: `v1.3.0`
- Labels: `type:feature`, `area:ingest`, `priority:p0`
- Depends on: none
- Description:
  - The current public-safety onboarding path supports contract-bound
    canonical bundles plus custom mapping overlays, but operators still
    have to author those overlays from scratch for each vendor extract.
  - Customer onboarding will move faster if the repo ships maintained
    profile overlays for representative CAD export shapes.
- Acceptance criteria:
  - The repo ships at least two documented CAD vendor-profile overlays.
  - Contract validation can apply those profiles without manual column
    rewrites by the operator.
  - Tests cover valid and invalid CAD payloads for each shipped profile.

### 118) Add packaged vendor profile overlays for common RMS exports

- Status: `open`
- Milestone: `v1.3.0`
- Labels: `type:feature`, `area:ingest`, `priority:p0`
- Depends on: none
- Description:
  - RMS onboarding has the same problem as CAD onboarding: the contract
    is stable, but the operator still needs vendor-specific mapping
    knowledge before validation can start.
  - The repo should ship maintained RMS profiles for representative
    report/person extract shapes used in public-safety pilots.
- Acceptance criteria:
  - The repo ships at least two documented RMS vendor-profile overlays.
  - Contract validation can apply those profiles without manual column
    rewrites by the operator.
  - Tests cover valid and invalid RMS payloads for each shipped profile.

### 119) Add an onboarding diff report for unmapped columns and contract drift

- Status: `open`
- Milestone: `v1.3.0`
- Labels: `type:feature`, `area:quality`, `priority:p1`
- Depends on: #117, #118
- Description:
  - Source owners need more than pass/fail validation during onboarding.
  - The repo should explain which source columns were mapped, ignored,
    or still unresolved so vendor conversations become concrete.
- Acceptance criteria:
  - The onboarding CLI emits a machine-readable diff report for mapped,
    unmapped, and unused source columns.
  - The report highlights required canonical fields that still have no
    source mapping.
  - Docs show how operators use the diff report during onboarding.

### 120) Add a syntheticized vendor-batch example generator for onboarding rehearsals

- Status: `open`
- Milestone: `v1.3.0`
- Labels: `type:feature`, `area:quality`, `priority:p1`
- Depends on: #117, #118
- Description:
  - Customer pilots and pre-sales work benefit from realistic vendor
    shapes, but the repo should continue to avoid real operational data.
  - The repo should be able to emit contract-valid vendor-shaped sample
    bundles directly from synthetic seed data for rehearsal and demo use.
- Acceptance criteria:
  - The repo ships a command that writes vendor-profile-shaped synthetic
    CAD and RMS onboarding bundles.
  - Generated bundles pass the public-safety onboarding checks.
  - Docs explain how to use the generator for rehearsals and demos.

## v1.4.0: Secure operations and compliance evidence

### 121) Add detached-signature support for customer handoff manifests

- Status: `open`
- Milestone: `v1.4.0`
- Labels: `type:feature`, `area:security`, `priority:p0`
- Depends on: none
- Description:
  - The current pilot bundle includes a hashed handoff manifest, but the
    integrity record is not yet signed.
  - Customer deliveries should support a detached signature workflow so
    the handoff can prove both integrity and signer identity.
- Acceptance criteria:
  - Pilot and release bundle packaging can emit a detached signature for
    the handoff manifest.
  - Verification tooling can validate both hash integrity and signature
    trust before bootstrap.
  - Docs define the supported signing and verification workflow.

### 122) Add secret-file and rotation health checks for runtime auth material

- Status: `open`
- Milestone: `v1.4.0`
- Labels: `type:feature`, `area:security`, `priority:p0`
- Depends on: none
- Description:
  - The runtime can already resolve environment-backed auth material,
    but secure customer environments often mount secrets as files and
    need periodic validation that the expected material is present.
  - The repo should validate secret-file inputs and expose rotation-read
    health checks for service bootstrap and ongoing operations.
- Acceptance criteria:
  - Runtime environments can resolve supported secret-file paths in
    addition to plain environment variables.
  - A readiness-style check validates required auth and signing inputs
    before service startup.
  - Docs define the supported secret-file pattern and rotation checks.

### 123) Add encrypted backup and export-bundle workflows for persisted state

- Status: `open`
- Milestone: `v1.4.0`
- Labels: `type:feature`, `area:operations`, `priority:p1`
- Depends on: #121, #122
- Description:
  - Persisted-state backups and pilot handoff artifacts currently rely
    on filesystem controls outside the repo runtime.
  - Secure customer operation needs encrypted backup and export bundles
    so copied artifacts are protected in transit and at rest.
- Acceptance criteria:
  - The repo ships an encrypted backup/export bundle workflow for
    persisted state and customer pilot handoff artifacts.
  - Recovery tooling can restore from the encrypted bundle with an
    operator-supplied key or passphrase.
  - Tests and docs cover the supported encrypt/restore workflow.

### 124) Generate a CJIS evidence pack from runtime config and audit artifacts

- Status: `open`
- Milestone: `v1.4.0`
- Labels: `type:feature`, `area:docs`, `priority:p1`
- Depends on: #122, #123
- Description:
  - The repo now has a CJIS-aligned baseline and preflight checks, but
    customer review still requires manual collection of config, audit,
    and deployment evidence.
  - The repo should generate an evidence pack that bundles the relevant
    machine-readable config and audit outputs for a review conversation.
- Acceptance criteria:
  - The repo ships a command that builds a CJIS evidence pack from the
    supported runtime configuration and audit artifacts.
  - The evidence pack includes a documented standards mapping index.
  - Docs clearly state that the pack supports review and does not by
    itself claim full operational compliance.

## v1.5.0: Customer deployment automation and supportability

### 125) Add a one-command Windows service wrapper for the pilot API and demo shell

- Status: `open`
- Milestone: `v1.5.0`
- Labels: `type:feature`, `area:operations`, `priority:p0`
- Depends on: #122
- Description:
  - The customer pilot bootstrap currently prepares local launch scripts,
    but the operator still starts the service and demo shell manually.
  - The single-host customer path needs an option to install or manage
    those processes as durable Windows services.
- Acceptance criteria:
  - The repo ships a supported Windows service wrapper for the demo
    shell and service API processes.
  - The bootstrap or admin tooling can install, start, stop, and remove
    those services.
  - Docs define the supported host assumptions and rollback path.

### 126) Add a support-bundle collector for customer pilot troubleshooting

- Status: `open`
- Milestone: `v1.5.0`
- Labels: `type:feature`, `area:operations`, `priority:p1`
- Depends on: #123
- Description:
  - When a customer pilot fails, support currently has to ask for logs,
    manifests, and runtime state piecemeal.
  - The repo should be able to collect a redacted support bundle that
    packages the relevant operational evidence in one artifact.
- Acceptance criteria:
  - The repo ships a support-bundle command for the pilot baseline.
  - The bundle includes documented logs, runtime config, and state
    metadata with the existing observability redaction rules applied.
  - Runbooks explain when and how to generate the support bundle.

### 127) Add a patch-upgrade and reseed workflow for existing pilot installs

- Status: `open`
- Milestone: `v1.5.0`
- Labels: `type:feature`, `area:operations`, `priority:p1`
- Depends on: #125
- Description:
  - The current customer-pilot path is deterministic, but upgrades are
    still effectively "re-extract and start over."
  - Customer pilots need a documented patch path that can preserve or
    intentionally reseed the supported install root in a controlled way.
- Acceptance criteria:
  - The repo ships a documented patch-upgrade workflow for the supported
    Windows single-host pilot baseline.
  - Operators can choose between preserving the current state or
    reseeding from the shipped manifest and state artifacts.
  - Smoke coverage validates the supported upgrade path.

### 128) Add an operator admin console for health, metrics, and recent audit events

- Status: `open`
- Milestone: `v1.5.0`
- Labels: `type:feature`, `area:service`, `priority:p1`
- Depends on: #122, #126
- Description:
  - The service already exposes health, metrics, and audit data, but the
    operator still has to inspect those surfaces manually or through raw
    API calls.
  - The customer-support path needs a minimal admin console for the
    supported pilot and single-host runtime.
- Acceptance criteria:
  - The repo ships a documented operator admin console surface for
    health, metrics, and recent audit events.
  - Access control follows the existing service auth and scope model.
  - Tests cover the supported read model and auth behavior.

## Suggested Epic Issues

Create these 3 epics first, then link child issues:

1. Epic: Vendor Adapter Packs and Onboarding Acceleration (`v1.3.0`)
2. Epic: Secure Operations and Compliance Evidence (`v1.4.0`)
3. Epic: Customer Deployment Automation and Supportability (`v1.5.0`)

## Suggested Issue Creation Order

1. Create or confirm labels.
2. Create milestones `v1.3.0`, `v1.4.0`, and `v1.5.0`.
3. Create the 3 epics.
4. Create all child issues and assign them to epics.
5. Execute `v1.3.0` before `v1.4.0`, and `v1.4.0` before `v1.5.0`.

## Tracker Status Snapshot

Snapshot date: 2026-03-14

- This backlog opens the post-`v1.0.0` cycle focused on vendor adapter
  packs, secure operational evidence, and customer supportability.
- Active epic issues in GitHub for this backlog:
  `#111`, `#112`, and `#113`
- Open child issues represented in this local active catalog:
  `#114` through `#125`
- Open milestones in GitHub for this backlog:
  `v1.3.0`, `v1.4.0`, and `v1.5.0`
