# Remaining Work Task List

Date prepared: 2026-03-13
Last updated: 2026-03-13

This checklist captures the current maintainer task state after the
`v0.1.2` release closeout and the completed follow-on backlog cycle. The
active GitHub backlog source remains
`planning/active-github-issues-backlog.md`; this file is only a short
mirror of that tracked work.

## Completed Task List

- [x] `#54` Expand normalization fidelity for addresses and phone
  outputs.
- [x] `#55` Expand non-exact matching signals and threshold-tuning
  fixtures.
- [x] `#56` Decide the supported manual-review operating model.
- [x] `#57` Define how the remaining-work checklist and GitHub backlog
  should coexist.

## Current Status

- No open tracked tasks remain in the current backlog cycle.
- The additional 2026-03-13 hardening pass shipped in `v0.1.2`,
  including lightweight phonetic-name scoring, deterministic release
  packaging, Python-native validation entrypoints, and CI support-matrix
  expansion.
- The previously unsynced candidate next-scope items were resolved
  directly in the repository and published without opening a new GitHub
  backlog cycle.

## Resolved Scope Decisions

- [x] The public repository remains synthetic-only by design; external
  data import adapters are out of scope for the supported public runtime
  surface.
- [x] The supported matching strategy for the current public line is the
  deterministic explainable heuristic scorer; ML-assisted scoring is
  treated as out of scope rather than an implied pending feature.
- [x] The CSV manual-review handoff is documented as the resolved
  supported operating model for the current public line.
- [x] The maintained CI support matrix now covers Python `3.11` baseline
  validation on Linux and Windows plus Python `3.12` compatibility
  validation on Linux, Windows, and macOS.
- [x] Python-native `scripts/run_checks.py` and `scripts/run_pipeline.py`
  now provide shell-free local validation and pipeline execution, with
  shell wrappers delegating to the Python source of truth.
- [x] The accumulated hardening work is validated and shipped in
  `v0.1.2`; there is no pending follow-up patch-release candidate in the
  current repo state.
- [x] No new GitHub-backed backlog cycle is needed until net-new scope
  exists beyond the resolved public-boundary and support-matrix work.
