# Post-v0.1.0 GitHub Issues Backlog

This backlog captures the next issue set after the completed bootstrap
and `v0.1.0` release. It is intentionally separate from
`planning/github-issues-backlog.md`, which remains the historical record
for the bootstrap delivery.

Date prepared: 2026-03-13
Last synced to GitHub: 2026-03-13

## Milestones

- `v0.1.1`: Patch release follow-up and distribution
- `v0.2.0`: Tracker operations hardening

## Label Set To Create

- `type:epic`
- `type:feature`
- `type:docs`
- `type:chore`
- `type:bug`
- `area:repo`
- `area:generate`
- `area:normalize`
- `area:matching`
- `area:survivorship`
- `area:quality`
- `area:ci`
- `area:docs`
- `priority:p0`
- `priority:p1`
- `priority:p2`

## Issue Catalog

## v0.1.1: Patch release follow-up and distribution

### 46) Package and attach reproducible sample output bundle to GitHub releases

- Status: `open`
- Milestone: `v0.1.1`
- Labels: `type:chore`, `area:repo`, `priority:p1`
- Depends on: none
- Description:
  - `v0.1.0` release notes describe the expected sample artifact set, but
    the release does not currently ship an attached sample bundle.
  - Add a reproducible way to package the documented small-profile
    `run-all` outputs and attach or publish that bundle with releases.
- Acceptance criteria:
  - Maintainers have a documented command or script that produces the
    release sample bundle from a clean checkout.
  - The bundle includes the documented normalized, matching, golden,
    review-queue, and exception outputs.
  - The release process references where that sample bundle is attached
    or stored.

### 47) Generalize release-process docs for patch and hotfix releases

- Status: `closed`
- Milestone: `v0.1.1`
- Labels: `type:docs`, `area:repo`, `priority:p1`
- Depends on: none
- Description:
  - `docs/release-process.md` is still framed around the first tagged
    release line.
  - Expand the guidance so maintainers can use the same process for
    patch and hotfix releases after `v0.1.0`.
- Acceptance criteria:
  - The release-process doc describes how to cut a patch release after an
    existing tag line.
  - The doc explains how to handle post-tag fixes that should land in the
    next patch release instead of silently changing an existing tag.
  - The changelog/update expectations for patch releases are explicit.

### 48) Prepare and publish v0.1.1 patch release for post-v0.1.0 automation fixes

- Status: `open`
- Milestone: `v0.1.1`
- Labels: `type:chore`, `area:repo`, `priority:p0`
- Depends on: #46, #47
- Description:
  - `main` now contains post-`v0.1.0` fixes for GitHub Actions Node 24
    compatibility plus issue/backlog synchronization hardening that are
    not part of the published `v0.1.0` tag.
  - Cut a patch release so the published release line matches the current
    maintainer and CI state.
- Acceptance criteria:
  - A `v0.1.1` tag is created from a green commit that includes the
    post-`v0.1.0` CI and tracker automation fixes.
  - The release notes summarize the patch scope and link the updated
    release/process docs.
  - The published patch release references the sample artifact bundle.

## v0.2.0: Tracker operations hardening

### 49) Add CI validation for backlog sync integrity and title-normalization collisions

- Status: `closed`
- Milestone: `v0.2.0`
- Labels: `type:chore`, `area:ci`, `priority:p1`
- Depends on: none
- Description:
  - The backlog automation now normalizes titles and syncs native
    sub-issue links, but that behavior is still validated only through
    local tests and manual execution.
  - Add a CI path that exercises the backlog sync logic in dry-run mode
    and catches duplicate-title or mapping regressions before merge.
- Acceptance criteria:
  - CI runs the backlog sync script in dry-run mode against the tracked
    planning files.
  - The CI path fails when normalized titles would collide or when the
    backlog parser drifts from the planning file structure.
  - The maintainer docs describe when this check should be used or
    required.

### 50) Close completed bootstrap milestones and establish active post-release milestones

- Status: `closed`
- Milestone: `v0.2.0`
- Labels: `type:chore`, `area:repo`, `priority:p1`
- Depends on: none
- Description:
  - GitHub milestones `M1` through `M6` remain open even though all child
    issues are closed and `v0.1.0` has shipped.
  - Clean up milestone state so the tracker reflects the completed
    bootstrap history and the current active release lines.
- Acceptance criteria:
  - Completed bootstrap milestones are closed in GitHub after verifying
    they contain no open issues.
  - Active post-release milestones are present for the supported next
    release lines.
  - Maintainer guidance notes when milestone closure and rollover should
    happen.

### 51) Document post-release backlog operating model and source of truth

- Status: `closed`
- Milestone: `v0.2.0`
- Labels: `type:docs`, `area:repo`, `priority:p2`
- Depends on: #49, #50
- Description:
  - The repository now has a completed bootstrap backlog and a
    post-release backlog, but the operating model for which file drives
    new GitHub work is not yet explicit.
  - Document how maintainers should open new issues after `v0.1.0`,
    including when to add work to the post-release backlog versus the
    historical bootstrap backlog.
- Acceptance criteria:
  - Repo docs identify the active backlog source for new GitHub issues.
  - The historical bootstrap backlog is clearly marked as completed and
    read-only for execution purposes.
  - Maintainers have a clear documented path for adding future epics and
    child issues.

## Suggested Epic Issues

Create these 2 epics first, then link child issues:

1. Epic: Patch Release and Distribution Follow-up (`v0.1.1`)
2. Epic: Tracker Operations Hardening (`v0.2.0`)

## Suggested Issue Creation Order

1. Create or confirm labels.
2. Create or confirm milestones (`v0.1.1`, `v0.2.0`).
3. Create the 2 epics.
4. Create all child issues and assign them to epics.
5. Execute `v0.1.1` before `v0.2.0`.

## Tracker Status Snapshot

Snapshot date: 2026-03-13

- Open issues represented in this local post-release catalog: none
- Closed issues and epics in this post-release set: `#44`, `#45`,
  `#46`, `#47`, `#48`, `#49`, `#50`, `#51`
- The `v0.1.1` patch release shipped and the release/distribution epic
  was closed after child issues `#46` and `#48` completed.
- The tracker-operations epic `#45` is also closed; future work should
  be added as new backlog items instead of reopening this historical
  post-release set.
- Bootstrap milestones `M1` through `M6` were closed in GitHub after the
  bootstrap backlog was fully executed.
- Historical post-release milestones remain `v0.1.1` and `v0.2.0`;
  define the next active milestone before syncing new work.
