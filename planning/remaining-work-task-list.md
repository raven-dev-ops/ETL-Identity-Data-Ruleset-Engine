# Remaining Work Task List

Date prepared: 2026-03-13
Last updated: 2026-03-13

This checklist captures the remaining work after the current repository
hardening pass. It is intentionally separate from the GitHub
issue-sync backlog files so maintainers can track immediate follow-up
tasks without changing the automation source of truth.

## Active Release Tasks

- [ ] Generate the packaged `v0.1.1` sample bundle with
  `scripts/package_release_sample.py` and attach the resulting zip to
  the GitHub release.
- [ ] Prepare the `v0.1.1` patch release from a green commit:
  - confirm `pyproject.toml` and `src/etl_identity_engine/__init__.py`
    both carry `0.1.1`
  - keep `CHANGELOG.md` aligned with the published tag
  - tag and publish the patch release with links to the release docs and
    attached sample bundle
  - confirm the published release notes call out `v0.1.1` as the first
    post-`v0.1.0` stabilization release
- [ ] Close or resync epic `#44` after child issues `#46` and `#48` are
  complete so the live tracker and local snapshot stay aligned.

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
