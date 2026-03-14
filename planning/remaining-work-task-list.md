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
- The previous tracked backlog cycle (`v0.7.0` through `v0.9.0`) is now
  complete and should be treated as historical.
- A new backlog cycle is now open for the documented next gaps: formal
  CAD/RMS source contracts, contract-driven public-safety onboarding,
  and customer-facing pilot packaging.
- The active tracked work is organized into three milestone groups:
  `v1.0.0`, `v1.1.0`, and `v1.2.0`.
- GitHub sync is complete for the new backlog cycle with open epics
  `#96`, `#97`, and `#98`.

## Open Task Groups

- [ ] `v1.0.0` CAD/RMS source contracts and validation
  - Local catalog issues: `#108`
  - Open GitHub issues: `#102`
- [ ] `v1.1.0` Public safety onboarding and identity mapping
  - Local catalog issues: `#109` through `#112`
  - Open GitHub issues: `#103` through `#106`
- [ ] `v1.2.0` Customer deployment packaging and pilot handoff
  - Local catalog issues: `#113` through `#116`
  - Open GitHub issues: `#107` through `#110`

## Gaps Now Explicitly Tracked

- [x] Versioned CAD source contracts and validation tooling
- [x] Versioned RMS source contracts and validation tooling
- [x] Manifest-era source-bundle validation for public-safety inputs
- [ ] Vendor-specific CAD/RMS field mapping overlays
- [ ] Persisted public-safety activity ingestion from formal contracts
- [ ] Service and demo-shell read models over persisted public-safety activity
- [ ] Regression fixtures for cross-system same-person and false-merge cases
- [ ] Standalone customer pilot bundle with seeded state and launch helpers
- [ ] Windows-first single-host pilot bootstrap
- [ ] Customer readiness check and handoff manifest
- [ ] Operator/admin runbooks and pilot acceptance checklist

## Next Step

- Sync the new backlog cycle to GitHub, then begin with local catalog
  issue `#108`.
