# Remaining Work Task List

Date prepared: 2026-03-13
Last updated: 2026-03-13

This checklist captures the remaining work after the `v0.1.1` release
closeout. The active GitHub backlog source is now
`planning/active-github-issues-backlog.md`; this checklist remains a
short maintainer view of the same follow-up themes.

## Tracked Follow-Up Tasks

- [ ] `#54` Expand normalization fidelity for addresses and phone
  outputs.
- [ ] `#55` Expand non-exact matching signals and threshold-tuning
  fixtures.
- [ ] `#56` Decide the supported manual-review operating model.
- [ ] `#57` Define how the remaining-work checklist and GitHub backlog
  should coexist.

## Pipeline Follow-Up Tasks

- [ ] Decide whether the manual review queue should remain a CSV handoff
  or graduate to a persisted review workflow with status updates and
  reassignment support.
- [ ] Expand matching beyond the current explainable heuristic set with
  additional non-exact signals such as address similarity, phone
  tolerance, and stronger evaluation fixtures for threshold tuning.
- [ ] Expand normalization beyond the current prototype baseline,
  especially richer address parsing and optional E.164-style phone
  output.

## Repo Maintenance Tasks

- [ ] Keep `planning/project-structure-outline.md` aligned with the live
  tree when new planning files, tests, scripts, or docs are added.
