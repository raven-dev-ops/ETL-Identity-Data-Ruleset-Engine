# Active GitHub Issues Backlog

This backlog is the current source of truth for new GitHub issues after
the completed bootstrap backlog and the completed `post-v0.1.0`
follow-up backlog. Historical backlog files remain available as
read-only records:

- `planning/github-issues-backlog.md`
- `planning/post-v0.1.0-github-issues-backlog.md`

Date prepared: 2026-03-13
Last synced to GitHub: 2026-03-13

## Milestones

- `v0.2.0`: Identity quality improvements
- `v0.3.0`: Workflow and backlog operations

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

## v0.2.0: Identity quality improvements

### 54) Expand normalization fidelity for addresses and phone outputs

- Status: `closed`
- Milestone: `v0.2.0`
- Labels: `type:feature`, `area:normalize`, `priority:p1`
- Depends on: none
- Description:
  - The current prototype normalizes addresses and phone numbers only to a lightweight baseline.
  - Improve canonical address and phone outputs so downstream matching and survivorship have stronger normalized inputs.
- Acceptance criteria:
  - Address normalization handles additional common punctuation, abbreviation, and ordering variants beyond the current baseline.
  - Phone normalization can optionally emit an E.164-style canonical output without changing the default contract unless configuration opts in.
  - Tests cover representative address and phone edge cases for the expanded normalization behavior.

### 55) Expand non-exact matching signals and threshold-tuning fixtures

- Status: `closed`
- Milestone: `v0.2.0`
- Labels: `type:feature`, `area:matching`, `priority:p1`
- Depends on: #54
- Description:
  - Matching is still intentionally lightweight and leans on exact or near-exact normalized field agreement.
  - Expand the explainable signal set and strengthen evaluation fixtures so threshold tuning remains evidence-driven.
- Acceptance criteria:
  - Matching uses at least one additional non-exact signal beyond the current baseline heuristic set.
  - Tests and fixtures cover regression cases for threshold tuning and manual-review boundaries.
  - The matching docs explain the added signals and their impact on score interpretation.

## v0.3.0: Workflow and backlog operations

### 56) Decide the supported manual-review operating model

- Status: `closed`
- Milestone: `v0.3.0`
- Labels: `type:feature`, `area:quality`, `priority:p1`
- Depends on: none
- Description:
  - `v0.1.1` intentionally keeps manual review as a CSV handoff instead of a persisted workflow.
  - The project needs an explicit supported operating model before expanding the review queue into a richer workflow surface.
- Acceptance criteria:
  - The supported manual-review model is chosen and documented as either CSV handoff only or a scoped persisted workflow expansion.
  - The docs identify what is in scope now versus deferred follow-up implementation work.
  - Any implementation work implied by the decision is captured as concrete follow-on issues or backlog items.

### 57) Define how the remaining-work checklist and GitHub backlog should coexist

- Status: `closed`
- Milestone: `v0.3.0`
- Labels: `type:docs`, `area:repo`, `priority:p2`
- Depends on: none
- Description:
  - The repository still uses both `planning/remaining-work-task-list.md` and the GitHub-backed backlog flow.
  - Clarify whether the checklist remains a short-term maintainer artifact, mirrors GitHub issues, or is eventually replaced.
- Acceptance criteria:
  - Maintainer docs define the role of `planning/remaining-work-task-list.md` relative to the active GitHub backlog.
  - The active backlog source and historical backlog files are clearly distinguished in repo docs.
  - Contributors have a documented path for promoting new local checklist items into tracked GitHub issues.

## Suggested Epic Issues

Create these 2 epics first, then link child issues:

1. Epic: Identity Quality Improvements (`v0.2.0`)
2. Epic: Workflow and Backlog Operations (`v0.3.0`)

## Suggested Issue Creation Order

1. Create or confirm labels.
2. Confirm milestone `v0.2.0` and create milestone `v0.3.0`.
3. Create the 2 epics.
4. Create all child issues and assign them to epics.
5. Execute `v0.2.0` before `v0.3.0`.

## Tracker Status Snapshot

Snapshot date: 2026-03-13

- Active epic issues in GitHub for this backlog: none
- Open issues represented in this local active catalog: none
- Closed issues and epics in this active catalog: `#52`, `#53`, `#54`,
  `#55`, `#56`, `#57`
- Historical backlog files remain read-only records for completed work.
- Milestone `v0.1.1` was closed on 2026-03-13 after the `v0.1.1` release closeout completed.
- Milestones `v0.2.0` and `v0.3.0` were both closed on 2026-03-13 after
  the tracked issue set completed.
- Additional hardening shipped in `v0.1.2` on 2026-03-13 without
  reopening the backlog: lightweight phonetic-name scoring,
  deterministic release packaging, Python-native local validation
  entrypoints, and Python `3.12` plus macOS compatibility validation.
- Refresh or replace this file before the next backlog sync when new
  work is ready to be tracked.
