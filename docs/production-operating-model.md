# Production Operating Model

This document defines the supported production rollout model for the
current repository line. It turns the existing runtime, security,
recovery, delivery, and release-hardening docs into one operator-facing
target.

## Supported Release Target

The current supported production target is a batch-plus-service
deployment with these boundaries:

- persisted SQLite state is enabled
- authenticated service access is enabled with distinct `reader` and
  `operator` roles, typically via JWT bearer auth, plus endpoint-level
  scopes
- operators deploy either the documented single-host container topology
  or an equivalent single-host Python runtime with the same config and
  state model
- downstream consumers integrate through the documented service API,
  delivery contract, or export-job surfaces

## Supported Environments

The current supported deployment environments are:

- the documented single-host container topology under `deploy/`
- an equivalent single-host Python runtime that uses the same
  persisted-state, service-auth, and runtime-config surfaces
- Linux, Windows, and macOS for maintainer validation, with Linux and
  Windows as the primary batch and service operating targets validated
  in the main CI baseline

The current production target does not yet claim support for:

- clustered or multi-node database topologies
- real-time or streaming identity resolution

## Rollout Phases

### 1. Pre-Production Validation

Before first production cutover:

- validate the runtime with the documented local and CI checks
- run the retained release-hardening command from
  [release-process.md](release-process.md)
- validate backup, restore, and replay with
  [recovery-runbooks.md](recovery-runbooks.md)
- confirm runtime-environment secrets are supplied externally rather
  than committed into repo config
- confirm downstream consumers are using only documented stable
  contracts

### 2. Initial Production Cutover

For the first production rollout:

- deploy the release artifact and config snapshot as one versioned unit
- initialize or upgrade the SQLite state DB through the documented
  migration commands
- confirm health, readiness, metrics, and audit-event collection before
  exposing the operator API
- run one manifest-driven batch end to end before enabling consumer
  reads of published golden and crosswalk outputs

### 3. Steady-State Operation

In steady state:

- operators ingest manifest-driven landed batches
- review decisions are applied through the persisted workflow surfaces
- completed runs are published or exported only from persisted state
- backups, recovery checks, and release-hardening artifacts are retained
  according to the operator policy for the deployment

## Operator Responsibilities

### Platform Operators

Platform operators own:

- deployment of the batch runtime and authenticated service
- secret injection for service and runtime environments, including JWT
  issuer, audience, signing metadata, and the deployed role/scope
  contract
- SQLite state durability, backup scheduling, and restore execution
- health, metrics, structured-log, and audit-event collection
- applying release upgrades and state migrations

### Data Operators

Data operators own:

- manifest preparation and landed input validation
- review-case triage, assignment, approval, rejection, and replay
- downstream publication and export execution
- investigation of batch-level data-quality exceptions and run failures

### Consumer Teams

Consumer teams own:

- integrating only through documented stable surfaces
- handling snapshot changes through the documented delivery pointer
  semantics
- validating their own downstream joins and business use of golden or
  crosswalk data
- upgrading when a documented contract version changes

## Rollback Boundaries

### Application Rollback

If a release must be rolled back:

- redeploy the previous known-good image or Python package version
- keep the SQLite state DB and published snapshots intact unless a data
  recovery action is also required
- rerun health and readiness checks before reopening the service surface

### Data Rollback

If the rollback requires reversing persisted run state:

- restore the SQLite DB backup
- restore the verified replay bundle required for replay
- rebuild reports or republish outputs from the restored run state
- use `replay-run` only after the restored replay bundle is back at its
  recorded bundle path

### Consumer Rollback Boundary

Consumers should not read repo-local working files directly. Rollback
for consumers is bounded at:

- the versioned delivery snapshot pointer
- the documented service API
- the documented export-job outputs

Do not mutate published snapshots in place. To roll consumers back,
republish or repoint them to a prior immutable snapshot.

## Governance Boundaries

### PII And Data Handling

The public repository remains synthetic-only. For production deployments
outside the repository:

- treat identity records as sensitive operational data
- do not copy production data into the repository, issues, changelog,
  docs, or retained release artifacts
- keep manifests, logs, exports, and backups out of source control
- use environment-backed secrets for service credentials and deployment
  configuration

The repository-level safety and security rules in [SAFETY.md](../SAFETY.md)
and [SECURITY.md](../SECURITY.md) remain in force.

### Audit Expectations

Production operation should retain auditable evidence for:

- review decisions
- replay actions
- downstream publication
- export-job execution
- release-hardening outputs for the shipped artifact set

The current runtime already persists privileged action audit rows in
SQLite and emits structured JSON logs. Operators are responsible for
retaining and monitoring those signals in their deployment.

### Consumer Responsibilities

Consumers must:

- use only documented stable service, workflow, and delivery contracts
- avoid direct reads from internal SQLite tables or repo-local working
  directories
- treat golden and crosswalk outputs as identity-resolution products,
  not immutable source truth
- coordinate upgrades when a new documented version or compatibility
  rule is introduced

## Support Model

### Maintainer-Owned Surface

Repository maintainers support:

- the packaged runtime and documented CLI/API behavior
- the documented config contracts and output contracts
- the CI baseline, release process, recovery runbooks, and release
  hardening checks

### Deployment-Owned Surface

Deployment operators support:

- infrastructure, secrets, and runtime environment management
- data retention, backup cadence, and restore execution
- incident response for production failures or service exposure
- downstream consumer coordination inside their organization

### Escalation Model

Use these escalation boundaries:

- security incident or exposed credential: follow [SECURITY.md](../SECURITY.md)
- deployment, migration, backup, or replay failure: platform operator
- review-case or data-quality dispute: data operator
- contract misunderstanding or release-process drift in the repo:
  maintainer backlog and release process

## Coherent Release Target

The current production-readiness cycle is complete when all of these are
true for a release candidate:

- the single-host persisted-state deployment path is documented and
  validated
- recovery runbooks are documented and tested
- the service-auth, audit, logging, metrics, and health baselines are in
  place
- retained release-hardening outputs are produced for the built
  artifacts
- downstream consumers have stable documented integration boundaries

That is the supported production target for the current line. New
backlog work should extend this model intentionally rather than
introducing parallel undocumented operating assumptions.
