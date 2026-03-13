# Remaining Work Task List

Date prepared: 2026-03-13
Last updated: 2026-03-13

This checklist captures the remaining work after the `v0.1.1` release
closeout. It is intentionally separate from the GitHub issue-sync
backlog files so maintainers can track immediate follow-up tasks
without changing the automation source of truth.

## Active Release Tasks

- [ ] Define the next post-`v0.1.1` release line and decide which
  remaining follow-up items below should be promoted into new GitHub
  milestones and issues.
- [ ] Decide whether the closed `planning/post-v0.1.0-github-issues-backlog.md`
  catalog should remain a historical record only or be replaced with a
  new active backlog file for future work.

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
- [ ] Decide whether the remaining-work checklist should be mirrored into
  GitHub as tracked issues or remain an in-repo maintainer artifact
  between release cuts.
