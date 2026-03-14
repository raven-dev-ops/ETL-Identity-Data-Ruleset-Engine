# Compatibility Policy

This document defines the current compatibility contract for external
consumers of the service, workflow, and downstream delivery surfaces.

It applies to:

- the documented authenticated HTTP endpoints in [service-api.md](service-api.md)
- the documented persisted operator workflow commands in
  [review-workflow.md](review-workflow.md)
- the documented downstream delivery contract in
  [delivery-contracts.md](delivery-contracts.md)
- the documented named export-job surface in [export-jobs.md](export-jobs.md)

## Stability Classes

### Stable

A surface is stable when it is both:

- explicitly documented as part of a named contract or command surface
- intended for external operator or consumer integration

The current stable surfaces are:

- documented operational health endpoints in
  [service-api.md](service-api.md)
- documented `/api/v1/...` endpoints plus the current `reader` and
  `operator` service-role split in [service-api.md](service-api.md)
- the `golden_crosswalk_snapshot/v1` delivery contract documented in
  [delivery-contracts.md](delivery-contracts.md)
- the documented export-job commands and audit states in
  [export-jobs.md](export-jobs.md)
- the documented review-workflow lifecycle states and operator commands
  in [review-workflow.md](review-workflow.md)

### Experimental Or Internal

A surface is experimental or internal when it is not presented as a
documented external contract.

The current experimental or internal surfaces include:

- the direct SQLite schema and table layout
- undocumented JSON fields or metadata blobs
- console log lines and human-readable markdown report wording
- repo-local working-directory paths under `data/` for downstream
  consumer integrations
- any future HTTP or CLI surface not yet documented in the files above

External consumers should integrate through the documented service,
workflow, and delivery surfaces rather than reading SQLite tables or
prototype working files directly.

## Versioning Rules

### HTTP API

The current authenticated API is versioned by path prefix under `/api/v1`.

The operational health endpoints `GET /healthz` and `GET /readyz` are
also part of the documented service surface for the current `0.x` line.
They are not path-versioned separately, so their paths and documented
response meaning remain stable within the current `0.x` line.

Within `v1`, the runtime may:

- add new endpoints
- add optional response fields
- add optional query parameters that do not change existing behavior

Within `v1`, the runtime does not change without a new API version:

- existing endpoint paths
- existing HTTP methods
- required request parameters
- documented response field meaning
- documented role requirements for an existing endpoint

Breaking API changes require a new versioned path such as `/api/v2`.

### Delivery Contracts

The current downstream contract is `golden_crosswalk_snapshot/v1`.

Breaking changes require a new contract version when they affect:

- published file names
- required manifest keys
- CSV headers or header order
- snapshot path layout
- pointer-manifest semantics

Additive metadata that does not break existing readers may be added
within the same contract version.

### Operator Workflow Commands

The documented persisted operator workflow commands are unversioned, so
their compatibility policy is narrower:

- documented command names remain stable within the current `0.x` line
- documented flags and lifecycle-state values remain stable within the
  current `0.x` line
- JSON responses may add new fields
- existing documented JSON keys are not removed or renamed without a
  documented deprecation cycle

This currently applies to:

- `review-case-list`
- `review-case-update`
- `apply-review-decision`
- `replay-run`
- `publish-run`
- `export-job-list`
- `export-job-run`
- `export-job-history`

## Deprecation Policy

Stable surfaces are not removed silently.

When a stable surface is deprecated, the project will:

- document the deprecation in the relevant contract doc
- record the deprecation in [CHANGELOG.md](../CHANGELOG.md)
- identify the replacement surface when one exists
- keep the deprecated surface available for at least one tagged release
  after the deprecation notice

Breaking removals are not made in the same release that first announces
the deprecation.

## Consumer Guidance

For the current line:

- use the documented authenticated `/api/v1/...` endpoints for service
  integrations
- use `golden_crosswalk_snapshot/v1` for published downstream data
- use the documented operator workflow commands for automation
- treat undocumented fields and direct SQLite access as internal

If a future release introduces a new API or delivery-contract version,
consumers should migrate to the new version rather than depending on
cross-version behavior.
