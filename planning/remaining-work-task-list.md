# Remaining Work Task List

Date prepared: 2026-03-13
Last updated: 2026-03-13

This checklist mirrors the active production-readiness backlog in
`planning/active-github-issues-backlog.md`. GitHub remains the source of
truth for tracked work; this file is the short maintainer view.

## Current Status

- A new backlog cycle is open to move the project beyond prototype scope
  toward a production MDM or enterprise data-product service.
- GitHub sync is complete with open epics `#58`, `#59`, and `#60`, plus
  open child issues `#64` through `#80`.
- The active tracked work is organized into three milestone groups:
  `v0.4.0`, `v0.5.0`, and `v0.6.0`.
- The current public `0.1.x` line remains a production-hardened
  prototype until those tracked issues are completed.
- Issue `#61` is complete: the runtime now validates a stable landed
  batch manifest contract before `normalize` or manifest-driven
  `run-all` reads real input files.
- Issue `#62` is complete: landed batches now resolve through local and
  object-storage-compatible adapters under the manifest contract.
- Issue `#63` is complete: completed runs and core artifacts now persist
  into SQLite and can be reloaded for reporting by `run_id`.

## Open Task Groups

- [ ] `v0.4.0` Production data ingestion and persistence
  - Open issues: `#64` through `#67` in the active backlog catalog.
- [ ] `v0.5.0` Review workflow and service surface
  - Open issues: `#68` through `#73` in the active backlog catalog.
- [ ] `v0.6.0` Security, operations, and deployment hardening
  - Open issues: `#74` through `#80` in the active backlog catalog.

## Production-Readiness Gaps Now Explicitly Tracked

- [x] Real landed-batch ingestion adapters and object-storage-compatible
  support
- [x] Persistent relational state for runs, entities, goldens, and
  review cases
- [ ] Idempotent orchestration and replay-safe run lifecycle handling
- [ ] Incremental processing across runs
- [ ] Persisted manual-review workflow and decision application
- [ ] Service and operator interfaces for run status, review, and golden
  lookups
- [ ] Downstream publication contracts for warehouse and data-product
  consumers
- [ ] Authentication, authorization, observability, deployment, and
  recovery runbooks

## Next Step

- Execute the new milestone cycle in order: `v0.4.0`, then `v0.5.0`,
  then `v0.6.0`, starting with issue `#64`.
