# Remaining Work Task List

Date prepared: 2026-03-14
Last updated: 2026-03-14

This checklist mirrors the active post-`v0.9.2` backlog in
`planning/active-github-issues-backlog.md`. GitHub remains the source
of truth for tracked work; this file is the short maintainer view.

## Current Status

- `v0.9.2` is published and closes the prior observability-hardening
  pass.
- The first `v1.0.0` onboarding slice is now in-repo: versioned CAD and
  RMS source-bundle contracts plus direct validation are implemented.
- The second `v1.0.0` onboarding slice is now in-repo: manifests can
  declare named CAD/RMS source bundles and validate them before
  normalize or `run-all` executes.
- The third `v1.0.0` onboarding slice is now in-repo: the CLI ships a
  public-safety onboarding conformance check plus checked-in example
  bundles and a sample manifest for source-owner self-checks.
- The `v1.0.0` milestone is now complete in-repo and ready for GitHub
  closeout.
- The previous tracked backlog cycle (`v0.7.0` through `v0.9.0`) is now
  complete and should be treated as historical.
- A new backlog cycle is now open for the documented next gaps: formal
  CAD/RMS source contracts, contract-driven public-safety onboarding,
  and customer-facing pilot packaging.
- The active tracked work is organized into three milestone groups:
  `v1.0.0`, `v1.1.0`, and `v1.2.0`.
- The `v1.1.0` milestone is now complete in-repo with the public-safety
  regression fixture set and expected merge/no-merge outcomes locked in
  tests.
- The first `v1.2.0` pilot-handoff slice is now in-repo: a standalone
  customer pilot bundle can be built from the seeded public-safety
  regression manifest and now includes persisted state, the prepared
  Django demo shell, a minimal runtime payload, and startup helpers.
- The second `v1.2.0` pilot-handoff slice is now in-repo: the shipped
  customer pilot bundle now includes a Windows-first bootstrap path
  that provisions local PostgreSQL, rebuilds the seeded run into that
  state store, and prepares the Django shell with generated launch and
  runtime config helpers.
- The third `v1.2.0` pilot-handoff slice is now in-repo: the customer
  pilot bundle includes a hashed handoff manifest, and the repo plus
  bundle both ship a readiness check for the documented Windows
  single-host baseline.
- The fourth `v1.2.0` pilot-handoff slice is now in-repo: the repo now
  ships customer pilot runbooks and an acceptance checklist for install,
  startup, rollback, backup, and demo execution.
- The full `v1.2.0` milestone is now complete in-repo and ready for
  GitHub closeout.

## Open Task Groups

- [x] `v1.2.0` Customer deployment packaging and pilot handoff

## Gaps Now Explicitly Tracked

- [x] Versioned CAD source contracts and validation tooling
- [x] Versioned RMS source contracts and validation tooling
- [x] Manifest-era source-bundle validation for public-safety inputs
- [x] Contract conformance tooling and onboarding fixtures for CAD/RMS batches
- [x] Vendor-specific CAD/RMS field mapping overlays
- [x] Persisted public-safety activity ingestion from formal contracts
- [x] Service and demo-shell read models over persisted public-safety activity
- [x] Regression fixtures for cross-system same-person and false-merge cases
- [x] Standalone customer pilot bundle with seeded state and launch helpers
- [x] Windows-first single-host pilot bootstrap
- [x] Customer readiness check and handoff manifest
- [x] Operator/admin runbooks and pilot acceptance checklist

## Next Step

- Next: release/admin work for the completed `v1.2.0` cycle, or open a
  new backlog cycle for net-new scope.
