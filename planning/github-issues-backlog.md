# GitHub Issues Backlog

This backlog is scoped for the full ETL Identity Data Ruleset Engine project and is organized for milestone-based delivery.

Date prepared: 2026-03-12
Last synced to GitHub: 2026-03-13

## Milestones

- `M1`: Project bootstrap and repo standards
- `M2`: Synthetic data generation
- `M3`: Normalization and data quality
- `M4`: Matching, scoring, and clustering
- `M5`: Survivorship and golden records
- `M6`: Reporting, hardening, and release

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

## M1: Project bootstrap and repo standards

### 7) Bootstrap repository skeleton

- Status: `closed`
- Milestone: `M1`
- Labels: `type:chore`, `area:repo`, `priority:p0`
- Depends on: none
- Description:
  - Create directories for `src`, `tests`, `docs`, `config`, `data`, and `.github`.
  - Add minimal placeholders so the structure is versioned.
- Acceptance criteria:
  - All target directories exist.
  - Repository tree matches `planning/project-structure-outline.md`.

### 8) Add Python package scaffolding and CLI entrypoint

- Status: `closed`
- Milestone: `M1`
- Labels: `type:feature`, `area:repo`, `priority:p0`
- Depends on: #7
- Description:
  - Add package namespace `src/etl_identity_engine`.
  - Add `cli.py` with subcommands: `generate`, `normalize`, `match`, `golden`, `report`, `run-all`.
- Acceptance criteria:
  - Running CLI help returns usage and commands.
  - Package imports successfully in tests.

### 9) Link governance and safety files from README

- Status: `closed`
- Milestone: `M1`
- Labels: `type:docs`, `area:repo`, `priority:p1`
- Depends on: #7
- Description:
  - Governance and safety files already exist in the repository.
  - Remaining work is to make them directly discoverable from the main documentation surface.
  - Update the README so governance, safety, and contribution paths are linked explicitly instead of only being mentioned in prose.
- Acceptance criteria:
  - `README.md` links `LICENSE`, `CONTRIBUTING.md`, `SECURITY.md`, `CODE_OF_CONDUCT.md`, and `SAFETY.md`.
  - The links appear in a dedicated documentation or governance section.
  - The README distinguishes governance docs from planning artifacts and stage docs.

### 10) Add GitHub issue forms and PR template

- Status: `closed`
- Milestone: `M1`
- Labels: `type:chore`, `area:ci`, `priority:p1`
- Depends on: #7
- Description:
  - Create `.github/ISSUE_TEMPLATE/{bug,feature,epic,docs}.yml`.
  - Add `.github/PULL_REQUEST_TEMPLATE.md`.
- Acceptance criteria:
  - GitHub detects issue templates in chooser.
  - PR template loads on new pull requests.

### 11) Document required checks and branch protection baseline

- Status: `closed`
- Milestone: `M1`
- Labels: `type:chore`, `area:ci`, `priority:p1`
- Depends on: #8
- Description:
  - The repository already has CI and issue-metadata workflows.
  - Remaining work is to define the expected merge gate for `main` and document which checks are required before release or merge.
  - This issue is about repository operating policy, not creating the initial workflow from scratch.
- Acceptance criteria:
  - The required checks for `main` are documented in repo docs or the release checklist.
  - The documentation names the workflows that must pass before merge.
  - Any branch-protection or ruleset settings that cannot be stored in git are explicitly documented for maintainers.

### 12) Add project-level task runner scripts

- Status: `closed`
- Milestone: `M1`
- Labels: `type:chore`, `area:repo`, `priority:p2`
- Depends on: #8
- Description:
  - Add `scripts/run_pipeline.ps1` and `scripts/run_pipeline.py`.
  - Support local end-to-end execution.
- Acceptance criteria:
  - Scripts execute without manual path edits.

## M2: Synthetic data generation

### 13) Define canonical data model

- Status: `closed`
- Milestone: `M2`
- Labels: `type:feature`, `area:generate`, `priority:p0`
- Depends on: #8
- Description:
  - Document person and incident entities, keys, and constraints in `docs/data-model.md`.
- Acceptance criteria:
  - Data model includes required and optional fields plus key relationships.

### 14) Implement synthetic source generator

- Status: `closed`
- Milestone: `M2`
- Labels: `type:feature`, `area:generate`, `priority:p0`
- Depends on: #13
- Description:
  - Build deterministic generator using a seed to create person and incident source records.
- Acceptance criteria:
  - Same seed reproduces identical outputs.
  - Supports small, medium, and large generation profiles.

### 15) Implement conflict recipe engine

- Status: `closed`
- Milestone: `M2`
- Labels: `type:feature`, `area:generate`, `priority:p0`
- Depends on: #14
- Description:
  - Add realistic duplicate and variation patterns: name token flips, nickname variants, DOB transpositions, and stale addresses.
- Acceptance criteria:
  - Each generated conflict type is measurable in the output summary.

### 16) Add output writer for CSV and Parquet

- Status: `closed`
- Milestone: `M2`
- Labels: `type:feature`, `area:generate`, `priority:p1`
- Depends on: #14
- Description:
  - Write generated datasets to both CSV and Parquet in `data/synthetic_sources/`.
- Acceptance criteria:
  - Both formats are generated from one command.

### 17) Add generator unit tests

- Status: `closed`
- Milestone: `M2`
- Labels: `type:feature`, `area:generate`, `priority:p1`
- Depends on: #14, #15
- Description:
  - Test schema validity, deterministic seed behavior, and duplicate-rate controls.
- Acceptance criteria:
  - Tests cover deterministic generation and conflict injection logic.

## M3: Normalization and data quality

### 18) Implement name normalization module

- Status: `closed`
- Milestone: `M3`
- Labels: `type:feature`, `area:normalize`, `priority:p0`
- Depends on: #14
- Description:
  - Normalize punctuation, casing, whitespace, token order, and common aliases.
- Acceptance criteria:
  - Canonical name fields are emitted for downstream matching.

### 19) Implement DOB and timestamp normalization module

- Status: `closed`
- Milestone: `M3`
- Labels: `type:feature`, `area:normalize`, `priority:p0`
- Depends on: #14
- Description:
  - Normalize incoming dates to one canonical format and flag invalid date values.
- Acceptance criteria:
  - Invalid and ambiguous dates are routed to exceptions.

### 20) Implement address normalization module

- Status: `closed`
- Milestone: `M3`
- Labels: `type:feature`, `area:normalize`, `priority:p1`
- Depends on: #14
- Description:
  - Normalize common street suffixes, directional terms, and unit patterns.
- Acceptance criteria:
  - Equivalent addresses map to standardized canonical output.

### 21) Implement phone normalization module

- Status: `closed`
- Milestone: `M3`
- Labels: `type:feature`, `area:normalize`, `priority:p2`
- Depends on: #14
- Description:
  - Normalize to a digits-only baseline and optional E.164-like output.
- Acceptance criteria:
  - Invalid phone patterns are flagged in the quality report.

### 22) Harden normalization orchestration for multi-input flows

- Status: `closed`
- Milestone: `M3`
- Labels: `type:feature`, `area:normalize`, `priority:p0`
- Depends on: #39
- Description:
  - Normalization modules already exist and the `run-all` path normalizes the synthetic source inputs.
  - Remaining work is to make normalization orchestration a first-class, standalone flow for multiple source inputs and a stable output layout.
  - This includes reducing stage-specific hardcoding and making the normalize command align with the documented pipeline behavior.
- Acceptance criteria:
  - A standalone normalization command can process the supported source inputs in one run.
  - Normalization behavior reads repo config consistently for the supported field modules.
  - Normalized artifact naming and output layout are documented and covered by tests.

### 23) Implement data-quality checks and exception output

- Status: `closed`
- Milestone: `M3`
- Labels: `type:feature`, `area:quality`, `priority:p1`
- Depends on: #22
- Description:
  - Add completeness, validity, and integrity checks with exception records.
- Acceptance criteria:
  - Data-quality summary and exception files are generated each run.

### 39) Validate runtime config semantics and cross-file consistency

- Status: `closed`
- Milestone: `M3`
- Labels: `type:feature`, `area:repo`, `priority:p0`
- Depends on: #22, #23
- Description:
  - A shared config loader already exists for normalization, blocking, matching, thresholds, and survivorship.
  - Remaining work is validation depth: required keys, allowed values, numeric bounds, and cross-file consistency are not yet enforced in a fail-fast way.
- Acceptance criteria:
  - Missing required config sections fail fast with actionable errors.
  - Threshold values are validated for logical consistency.
  - Blocking, matching, and survivorship config values are validated for allowed structure and types.
  - Automated tests cover invalid and inconsistent config cases.

## M4: Matching, scoring, and clustering

### 24) Emit blocking-pass metrics for candidate generation

- Status: `closed`
- Milestone: `M4`
- Labels: `type:feature`, `area:matching`, `priority:p0`
- Depends on: #22
- Description:
  - Multi-pass blocking and candidate generation already exist.
  - Remaining work is to emit per-pass observability so blocking effectiveness can be inspected and tuned.
  - The tracker should reflect reporting and hardening work rather than initial implementation.
- Acceptance criteria:
  - Candidate metrics are emitted for each blocking pass.
  - Output distinguishes raw per-pass counts from the de-duplicated overall candidate count.
  - Tests verify deterministic per-pass metrics for fixed input and config.

### 25) Improve weighted match scoring beyond exact-match field equality

- Status: `closed`
- Milestone: `M4`
- Labels: `type:feature`, `area:matching`, `priority:p0`
- Depends on: #24
- Description:
  - Base weighted scoring, threshold routing, and reason traces already exist.
  - Remaining work is to improve scoring quality with additional signals beyond strict exact equality while keeping the scoring path explainable.
  - Candidate examples include nickname, partial-address, or other near-match scenarios that are not well represented by the current exact-match scoring behavior.
- Acceptance criteria:
  - The scoring path supports at least one non-exact or derived match signal.
  - Scored outputs remain inspectable through weighted reason traces.
  - Tests demonstrate improved handling for representative false-positive and false-negative edge cases.

### 26) Add match decision thresholds

- Status: `closed`
- Milestone: `M4`
- Labels: `type:feature`, `area:matching`, `priority:p0`
- Depends on: #25
- Description:
  - Define `auto-merge`, `manual-review`, and `no-match` cutoffs in config.
- Acceptance criteria:
  - Thresholds are externalized in `config/thresholds.yml`.

### 27) Implement cluster construction from accepted links

- Status: `closed`
- Milestone: `M4`
- Labels: `type:feature`, `area:matching`, `priority:p1`
- Depends on: #26
- Description:
  - Group linked records into entity clusters with stable cluster IDs.
- Acceptance criteria:
  - Cluster IDs are deterministic for fixed input and config.

### 28) Expand matching tests for regression and edge-case coverage

- Status: `closed`
- Milestone: `M4`
- Labels: `type:feature`, `area:matching`, `priority:p1`
- Depends on: #24, #25
- Description:
  - Basic matching tests already cover blocking, scoring, thresholds, and deterministic clustering.
  - Remaining work is to add regression-oriented coverage for boundary conditions and known error modes.
- Acceptance criteria:
  - Tests include false-positive and false-negative scenarios.
  - Tests cover threshold-boundary behavior and multi-pass blocking de-duplication.
  - Tests verify deterministic cluster stability for fixed input and accepted links.

## M5: Survivorship and golden records

### 29) Implement survivorship rule engine

- Status: `closed`
- Milestone: `M5`
- Labels: `type:feature`, `area:survivorship`, `priority:p0`
- Depends on: #27
- Description:
  - Build rule-based field selection for golden record output.
- Acceptance criteria:
  - Rule precedence is deterministic and config-driven.

### 30) Add source priority and recency strategies

- Status: `closed`
- Milestone: `M5`
- Labels: `type:feature`, `area:survivorship`, `priority:p1`
- Depends on: #29
- Description:
  - Support source ranking and effective-date recency logic in survivorship.
- Acceptance criteria:
  - Tie-break behavior is documented and tested.

### 31) Add provenance tracking for every selected field

- Status: `closed`
- Milestone: `M5`
- Labels: `type:feature`, `area:survivorship`, `priority:p0`
- Depends on: #29
- Description:
  - Store which source record and rule produced each golden value.
- Acceptance criteria:
  - Golden output includes provenance metadata for every field.

### 32) Generate crosswalk output (source ID to golden ID)

- Status: `closed`
- Milestone: `M5`
- Labels: `type:feature`, `area:survivorship`, `priority:p0`
- Depends on: #27, #29
- Description:
  - Produce a mapping table for downstream joins and traceability.
- Acceptance criteria:
  - The crosswalk covers all source records with deterministic golden IDs.

### 33) Add survivorship and crosswalk tests

- Status: `closed`
- Milestone: `M5`
- Labels: `type:feature`, `area:survivorship`, `priority:p1`
- Depends on: #29, #30, #31, #32
- Description:
  - Test field-level precedence, provenance completeness, and crosswalk integrity.
- Acceptance criteria:
  - Tests verify all cluster members map to one golden record.

## M6: Reporting, hardening, and release

### 34) Add before/after quality metrics and duplicate-reduction deltas

- Status: `closed`
- Milestone: `M6`
- Labels: `type:feature`, `area:quality`, `priority:p0`
- Depends on: #22, #32
- Description:
  - The runtime already emits a run summary, exception counts, and review queue volume.
  - Remaining work is to publish comparative before and after quality metrics that show whether the pipeline improved the data.
- Acceptance criteria:
  - The run summary includes before and after completeness deltas for key fields.
  - The reporting output includes duplicate-reduction or cluster-consolidation metrics.
  - Metrics are emitted in both machine-readable output and markdown reporting form.
  - Tests cover the expected metric fields.

### 35) Implement manual review queue output

- Status: `closed`
- Milestone: `M6`
- Labels: `type:feature`, `area:quality`, `priority:p1`
- Depends on: #26, #32
- Description:
  - Emit an actionable queue for low-confidence clusters.
- Acceptance criteria:
  - Output includes reason codes and top contributing match signals.

### 36) Add end-to-end integration test on small synthetic dataset

- Status: `closed`
- Milestone: `M6`
- Labels: `type:feature`, `area:quality`, `priority:p0`
- Depends on: #14, #22, #27, #32
- Description:
  - Validate the entire pipeline from generation to golden outputs.
- Acceptance criteria:
  - The pipeline completes in CI and validates expected outputs.

### 37) Replace placeholder stage docs and link them from README

- Status: `closed`
- Milestone: `M6`
- Labels: `type:docs`, `area:docs`, `priority:p1`
- Depends on: #22, #32, #34
- Description:
  - Stage docs exist, but several are still placeholders or too thin to operate from.
  - Remaining work is to replace placeholder content with real runbooks and make those docs discoverable from the README.
- Acceptance criteria:
  - `README.md` links all stage documentation pages.
  - Placeholder-only docs are replaced with implementation-specific guidance.
  - Each major stage doc includes concrete runtime behavior, relevant config files, and example commands or outputs.

### 38) Prepare `v0.1.0` release checklist, changelog, and tag procedure

- Status: `closed`
- Milestone: `M6`
- Labels: `type:chore`, `area:repo`, `priority:p1`
- Depends on: #34, #37, #40, #41
- Description:
  - The package version is already declared, but the repository does not yet have a documented release procedure.
  - Remaining work is to define the release checklist, known limitations, sample outputs, and tagging steps for the first tagged release.
- Acceptance criteria:
  - A release checklist is committed to the repository.
  - Known limitations and release-readiness criteria are documented.
  - The tag and versioning procedure for `v0.1.0` is documented for maintainers.

### 40) Expand CI to Linux and Windows matrix with coverage reporting

- Status: `closed`
- Milestone: `M6`
- Labels: `type:chore`, `area:ci`, `priority:p1`
- Depends on: #36
- Description:
  - CI currently validates a single Ubuntu and Python path.
  - Remaining work is to validate both documented shell environments and publish test coverage in CI results.
- Acceptance criteria:
  - CI runs on Linux and Windows for the supported Python runtime.
  - The documented bash and PowerShell paths are exercised or validated in CI.
  - Coverage output is generated and visible in CI results.
  - The minimum release-readiness coverage expectation is documented.

### 41) Define output schemas and add contract tests for pipeline artifacts

- Status: `closed`
- Milestone: `M6`
- Labels: `type:feature`, `area:quality`, `priority:p1`
- Depends on: #34, #37
- Description:
  - End-to-end tests already assert the presence of several artifact fields.
  - Remaining work is to formalize those outputs as stable contracts rather than relying on incidental shape assertions.
- Acceptance criteria:
  - Stable schemas are documented for normalized, matches, clusters, golden, crosswalk, exceptions, review-queue, and summary artifacts.
  - Contract tests fail when required fields, field types, or file naming conventions drift.
  - CI fails on breaking output-shape changes before release.

### 42) Clean up README encoding and formatting artifacts

- Status: `closed`
- Milestone: `M6`
- Labels: `type:docs`, `area:docs`, `priority:p2`
- Depends on: none
- Description:
  - The README currently contains malformed encoded characters in the high-level pipeline section and inconsistent markdown formatting in a few rendered sections.
  - Clean up the markdown so GitHub renders the project overview cleanly and consistently.
- Acceptance criteria:
  - Malformed encoded characters are removed from the README.
  - The high-level pipeline section renders cleanly on GitHub.
  - Example tables and list sections render consistently and remain readable on both GitHub web and local markdown viewers.

## Suggested Epic Issues

Create these 6 epics first, then link child issues:

1. Epic: Project Bootstrap and Operating Baseline (`M1`)
2. Epic: Synthetic Data Foundation (`M2`)
3. Epic: Normalization and Data Quality Core (`M3`)
4. Epic: Probabilistic Matching Engine (`M4`)
5. Epic: Survivorship and Golden Record Layer (`M5`)
6. Epic: Reporting, Hardening, and Release (`M6`)

## Suggested Issue Creation Order

1. Create labels.
2. Create milestones (`M1` through `M6`).
3. Create 6 epics.
4. Create all child issues and assign them to epics.
5. Sort by dependency path and execute milestone work from `M1` to `M6`.

## Tracker Status Snapshot

Snapshot date: 2026-03-13

- Total issues tracked by GitHub: `42`
- Open issues: `0`
- Closed issues: `42`
- Open epics: none
- Closed epics: `#1`, `#2`, `#3`, `#4`, `#5`, `#6`

## Backlog Status

The GitHub issue backlog is fully executed as of 2026-03-13.

- All milestone child issues and epic issues are now closed.
- The remaining repository follow-up is release execution, not backlog implementation.

## Current Phase Gaps To Close Before `v0.1.0`

- Publish the first tagged release using the documented checklist and tag procedure.
- Attach or archive a fresh `run-all --profile small` sample artifact set with the release notes.

## Tracker Sync Notes

- Completed child issues were closed in GitHub on 2026-03-13 after verifying implementation against the codebase and test suite.
- Open issues were retitled or rewritten where the original backlog still described work that was already done.
- Issue `#39` was completed after adding fail-fast runtime config validation and invalid-config test coverage.
- Issue `#22` was completed after making the normalize CLI a documented multi-input stage path.
- Issue `#24` was completed after adding blocking-pass metrics and emitting a matching-stage metrics artifact.
- Issues `#25` and `#28` were completed after adding explainable partial-name scoring and broader matching regression coverage.
- Issue `#34` was completed after adding before/after completeness deltas and duplicate-reduction metrics to the reporting outputs.
- Issues `#37` and `#9` were completed after replacing placeholder stage docs and adding direct governance and safety links to the README.
- New issue `#42` was added to track README encoding and markdown rendering cleanup.
- Issue `#11` was completed after documenting the required `main` merge gates and branch-protection baseline for maintainers.
- Issue `#40` was completed after expanding CI to Linux and Windows and adding coverage publishing with an `85%` floor.
- Issue `#41` was completed after documenting pipeline artifact schemas and adding dedicated contract tests.
- Issue `#38` was completed after adding a release-process doc and `CHANGELOG.md` for the planned `v0.1.0` release.
- Issue `#42` was completed after rewriting the README into clean GitHub-flavored markdown and removing malformed encoding artifacts.
- The local backlog now mirrors the fully closed live tracker.
