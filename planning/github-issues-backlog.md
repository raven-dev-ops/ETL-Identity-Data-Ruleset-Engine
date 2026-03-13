# GitHub Issues Backlog

This backlog is scoped for the full ETL Identity Data Ruleset Engine project and is organized for milestone-based delivery.

Date prepared: 2026-03-12

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

### 1) Bootstrap repository skeleton

- Milestone: `M1`
- Labels: `type:chore`, `area:repo`, `priority:p0`
- Depends on: none
- Description:
  - Create directories for `src`, `tests`, `docs`, `config`, `data`, and `.github`.
  - Add minimal placeholders so the structure is versioned.
- Acceptance criteria:
  - All target directories exist.
  - Repository tree matches `planning/project-structure-outline.md`.

### 2) Add Python package scaffolding and CLI entrypoint

- Milestone: `M1`
- Labels: `type:feature`, `area:repo`, `priority:p0`
- Depends on: #1
- Description:
  - Add package namespace `src/etl_identity_engine`.
  - Add `cli.py` with subcommands: `generate`, `normalize`, `match`, `golden`, `report`, `run-all`.
- Acceptance criteria:
  - Running CLI help returns usage and commands.
  - Package imports successfully in tests.

### 3) Add project governance files

- Milestone: `M1`
- Labels: `type:docs`, `area:repo`, `priority:p1`
- Depends on: #1
- Description:
  - Add `LICENSE`, `CONTRIBUTING.md`, `SECURITY.md`, and `CODE_OF_CONDUCT.md`.
- Acceptance criteria:
  - Files exist and are linked from `README.md`.

### 4) Add GitHub issue forms and PR template

- Milestone: `M1`
- Labels: `type:chore`, `area:ci`, `priority:p1`
- Depends on: #1
- Description:
  - Create `.github/ISSUE_TEMPLATE/{bug,feature,epic,docs}.yml`.
  - Add `.github/PULL_REQUEST_TEMPLATE.md`.
- Acceptance criteria:
  - GitHub detects issue templates in chooser.
  - PR template loads on new pull requests.

### 5) Add CI workflow for tests and linting

- Milestone: `M1`
- Labels: `type:chore`, `area:ci`, `priority:p1`
- Depends on: #2
- Description:
  - Create GitHub Actions workflow for unit tests and lint checks.
- Acceptance criteria:
  - Workflow runs on push and pull request.
  - Failing tests block merge.

### 6) Add project-level task runner scripts

- Milestone: `M1`
- Labels: `type:chore`, `area:repo`, `priority:p2`
- Depends on: #2
- Description:
  - Add `scripts/run_pipeline.ps1` and `scripts/run_pipeline.py`.
  - Support local end-to-end execution.
- Acceptance criteria:
  - Scripts execute without manual path edits.

## M2: Synthetic data generation

### 7) Define canonical data model

- Milestone: `M2`
- Labels: `type:feature`, `area:generate`, `priority:p0`
- Depends on: #2
- Description:
  - Document person and incident entities, keys, and constraints in `docs/data-model.md`.
- Acceptance criteria:
  - Data model includes required/optional fields and key relationships.

### 8) Implement synthetic source generator

- Milestone: `M2`
- Labels: `type:feature`, `area:generate`, `priority:p0`
- Depends on: #7
- Description:
  - Build deterministic generator using a seed to create person and incident source records.
- Acceptance criteria:
  - Same seed reproduces identical outputs.
  - Supports small, medium, and large generation profiles.

### 9) Implement conflict recipe engine

- Milestone: `M2`
- Labels: `type:feature`, `area:generate`, `priority:p0`
- Depends on: #8
- Description:
  - Add realistic duplicate/variation patterns: name token flips, nickname variants, DOB transpositions, stale addresses.
- Acceptance criteria:
  - Each generated conflict type is measurable in output summary.

### 10) Add output writer for CSV and Parquet

- Milestone: `M2`
- Labels: `type:feature`, `area:generate`, `priority:p1`
- Depends on: #8
- Description:
  - Write generated datasets to both CSV and Parquet in `data/synthetic_sources/`.
- Acceptance criteria:
  - Both formats generated from one command.

### 11) Add generator unit tests

- Milestone: `M2`
- Labels: `type:feature`, `area:generate`, `priority:p1`
- Depends on: #8, #9
- Description:
  - Test schema validity, deterministic seed behavior, and duplicate-rate controls.
- Acceptance criteria:
  - Tests cover deterministic generation and conflict injection logic.

## M3: Normalization and data quality

### 12) Implement name normalization module

- Milestone: `M3`
- Labels: `type:feature`, `area:normalize`, `priority:p0`
- Depends on: #8
- Description:
  - Normalize punctuation, casing, whitespace, token order, and common aliases.
- Acceptance criteria:
  - Canonical name fields are emitted for downstream matching.

### 13) Implement DOB and timestamp normalization module

- Milestone: `M3`
- Labels: `type:feature`, `area:normalize`, `priority:p0`
- Depends on: #8
- Description:
  - Normalize incoming dates to one canonical format and flag invalid date values.
- Acceptance criteria:
  - Invalid and ambiguous dates are routed to exceptions.

### 14) Implement address normalization module

- Milestone: `M3`
- Labels: `type:feature`, `area:normalize`, `priority:p1`
- Depends on: #8
- Description:
  - Normalize common street suffixes, directional terms, and unit patterns.
- Acceptance criteria:
  - Equivalent addresses map to standardized canonical output.

### 15) Implement phone normalization module

- Milestone: `M3`
- Labels: `type:feature`, `area:normalize`, `priority:p2`
- Depends on: #8
- Description:
  - Normalize to digits-only baseline and optional E.164-like output.
- Acceptance criteria:
  - Invalid phone patterns are flagged in quality report.

### 16) Build normalization orchestration pipeline

- Milestone: `M3`
- Labels: `type:feature`, `area:normalize`, `priority:p0`
- Depends on: #12, #13, #14, #15
- Description:
  - Add orchestration that applies all normalization modules and writes `data/normalized/`.
- Acceptance criteria:
  - One command normalizes all sources.

### 17) Implement data-quality checks and exception output

- Milestone: `M3`
- Labels: `type:feature`, `area:quality`, `priority:p1`
- Depends on: #16
- Description:
  - Add completeness, validity, and integrity checks with exception records.
- Acceptance criteria:
  - DQ summary and exception files are generated each run.

## M4: Matching, scoring, and clustering

### 18) Implement candidate generation with blocking rules

- Milestone: `M4`
- Labels: `type:feature`, `area:matching`, `priority:p0`
- Depends on: #16
- Description:
  - Build multi-pass blocking strategy to reduce pairwise comparisons.
- Acceptance criteria:
  - Candidate pair counts are emitted by blocking pass.

### 19) Implement weighted match scoring engine

- Milestone: `M4`
- Labels: `type:feature`, `area:matching`, `priority:p0`
- Depends on: #18
- Description:
  - Compute confidence scores from name, DOB, address, phone, and identifier signals.
- Acceptance criteria:
  - Score and reason trace returned for every candidate pair.

### 20) Add match decision thresholds

- Milestone: `M4`
- Labels: `type:feature`, `area:matching`, `priority:p0`
- Depends on: #19
- Description:
  - Define `auto-merge`, `manual-review`, and `no-match` cutoffs in config.
- Acceptance criteria:
  - Thresholds are externalized in `config/thresholds.yml`.

### 21) Implement cluster construction from accepted links

- Milestone: `M4`
- Labels: `type:feature`, `area:matching`, `priority:p1`
- Depends on: #20
- Description:
  - Group linked records into entity clusters with stable cluster IDs.
- Acceptance criteria:
  - Cluster IDs are deterministic for fixed input + config.

### 22) Add matching module tests

- Milestone: `M4`
- Labels: `type:feature`, `area:matching`, `priority:p1`
- Depends on: #19, #20, #21
- Description:
  - Validate blocking behavior, score reproducibility, threshold routing, and clustering.
- Acceptance criteria:
  - Test coverage includes false positive and false negative edge cases.

## M5: Survivorship and golden records

### 23) Implement survivorship rule engine

- Milestone: `M5`
- Labels: `type:feature`, `area:survivorship`, `priority:p0`
- Depends on: #21
- Description:
  - Build rule-based field selection for golden record output.
- Acceptance criteria:
  - Rule precedence is deterministic and config-driven.

### 24) Add source priority and recency strategies

- Milestone: `M5`
- Labels: `type:feature`, `area:survivorship`, `priority:p1`
- Depends on: #23
- Description:
  - Support source ranking and effective-date recency logic in survivorship.
- Acceptance criteria:
  - Tie-break behavior documented and tested.

### 25) Add provenance tracking for every selected field

- Milestone: `M5`
- Labels: `type:feature`, `area:survivorship`, `priority:p0`
- Depends on: #23
- Description:
  - Store which source record and rule produced each golden value.
- Acceptance criteria:
  - Golden output includes provenance metadata for every field.

### 26) Generate crosswalk output (source ID to golden ID)

- Milestone: `M5`
- Labels: `type:feature`, `area:survivorship`, `priority:p0`
- Depends on: #21, #23
- Description:
  - Produce mapping table for downstream joins and traceability.
- Acceptance criteria:
  - Crosswalk covers all source records with deterministic golden IDs.

### 27) Add survivorship and crosswalk tests

- Milestone: `M5`
- Labels: `type:feature`, `area:survivorship`, `priority:p1`
- Depends on: #23, #24, #25, #26
- Description:
  - Test field-level precedence, provenance completeness, and crosswalk integrity.
- Acceptance criteria:
  - Tests verify all cluster members map to one golden record.

## M6: Reporting, hardening, and release

### 28) Implement run summary and before/after quality metrics

- Milestone: `M6`
- Labels: `type:feature`, `area:quality`, `priority:p0`
- Depends on: #17, #26
- Description:
  - Report duplicate reduction, completeness delta, and review queue volume.
- Acceptance criteria:
  - Metrics exported in both machine-readable and markdown formats.

### 29) Implement manual review queue output

- Milestone: `M6`
- Labels: `type:feature`, `area:quality`, `priority:p1`
- Depends on: #20, #26
- Description:
  - Emit actionable queue for low-confidence clusters.
- Acceptance criteria:
  - Output includes reason codes and top contributing match signals.

### 30) Add end-to-end integration test on small synthetic dataset

- Milestone: `M6`
- Labels: `type:feature`, `area:quality`, `priority:p0`
- Depends on: #8, #16, #21, #26
- Description:
  - Validate entire pipeline from generation to golden outputs.
- Acceptance criteria:
  - Pipeline completes in CI and validates expected outputs.

### 31) Expand documentation for each processing stage

- Milestone: `M6`
- Labels: `type:docs`, `area:docs`, `priority:p1`
- Depends on: #16, #21, #26
- Description:
  - Complete stage docs: architecture, normalization, matching, survivorship, evaluation.
- Acceptance criteria:
  - All stage docs link from README and include run examples.

### 32) Prepare `v0.1.0` release checklist and tag plan

- Milestone: `M6`
- Labels: `type:chore`, `area:repo`, `priority:p1`
- Depends on: #28, #30, #31
- Description:
  - Finalize release checklist, known limitations, and sample outputs.
- Acceptance criteria:
  - Release checklist complete and version plan documented.

### 33) Add runtime config loading and validation layer

- Milestone: `M3`
- Labels: `type:feature`, `area:repo`, `priority:p0`
- Depends on: #16, #17
- Description:
  - Add a shared loader for `config/*.yml` used by normalization,
    blocking, matching, thresholds, and survivorship flows.
  - Validate required keys, allowed values, and cross-file consistency at
    startup.
- Acceptance criteria:
  - Runtime behavior reads configuration from the repo config files
    instead of duplicating defaults in multiple modules.
  - Invalid or incomplete configuration fails fast with actionable error
    messages.

### 34) Expand CI to Linux and Windows matrix with coverage reporting

- Milestone: `M6`
- Labels: `type:chore`, `area:ci`, `priority:p1`
- Depends on: #30
- Description:
  - Extend CI to validate the documented bash and PowerShell execution
    paths.
  - Publish test coverage and define the minimum release-readiness gate.
- Acceptance criteria:
  - CI runs on Linux and Windows for the supported Python version.
  - Coverage output is generated and visible in CI results.

### 35) Formalize output schemas and contract tests for pipeline artifacts

- Milestone: `M6`
- Labels: `type:feature`, `area:quality`, `priority:p1`
- Depends on: #26, #28, #29
- Description:
  - Define stable schema contracts for normalized, matches, golden,
    crosswalk, exceptions, and review-queue outputs.
  - Add contract tests that fail when required fields, types, or file
    naming conventions drift.
- Acceptance criteria:
  - Output schemas are documented and exercised by automated tests.
  - Breaking output-shape changes fail CI before release.

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
5. Sort by dependency path and execute `M1` to `M6`.

## Recommended Near-Term Execution Order (Current State)

This sequence reflects the repository's current implementation state:
`M1` and `M2` are effectively in place, while `M3` through `M5` are
partially implemented and `M6` is mostly planning scaffolding.

1. Execute `#16` Build normalization orchestration pipeline
   - Expand the current CLI normalization path into the one supported
     multi-source orchestration entrypoint.
   - Wire `config/normalization_rules.yml` into runtime behavior instead
     of leaving normalization policy hardcoded.
2. Execute `#17` Implement data-quality checks and exception output
   - Emit concrete exception artifacts for invalid DOBs, malformed
     phones, and normalization failures under `data/exceptions/`.
3. Execute `#33` Add runtime config loading and validation layer
   - Centralize parsing and validation for `config/*.yml` so blocking,
     scoring, thresholds, and survivorship are not split between config
     files and hardcoded defaults.
4. Execute `#18` Implement candidate generation with blocking rules
   - Replace the single hardcoded blocking strategy with multi-pass,
     config-backed blocking and per-pass candidate metrics.
5. Execute `#19` Implement weighted match scoring engine
   - Return score reasons and contributing signals so scoring is
     inspectable and auditable.
6. Execute `#20` Add match decision thresholds
   - Use `config/thresholds.yml` at runtime and route candidate pairs to
     `auto-merge`, `manual-review`, and `no-match`.
7. Execute `#21` Implement cluster construction from accepted links
   - Generate deterministic cluster IDs and make downstream golden
     record construction consume clusters instead of implicit entity IDs.
8. Execute `#23` through `#26` as one vertical slice
   - Finish config-driven survivorship, add field-level provenance, and
     write the source-to-golden crosswalk.
9. Execute `#34` Expand CI to Linux and Windows matrix with coverage
   reporting
   - Add Linux and Windows CI coverage, publish test coverage, and
     define the branch protection expectation for release readiness.
10. Execute `#35` Formalize output schemas and contract tests for
    pipeline artifacts
    - Define stable contracts for normalized, matches, golden,
      crosswalk, exceptions, and review-queue outputs.
11. Execute `#28` through `#31`
    - Add before/after quality metrics, manual review outputs, and
      replace placeholder stage docs with actual runbooks and examples.

## Current Phase Gaps To Close Before `v0.1.0`

- Config files exist for normalization, blocking, matching, thresholds,
  and survivorship, but runtime behavior is still only partially wired
  to them.
- Threshold routing, deterministic clustering, provenance, and
  crosswalk outputs are not yet implemented end to end.
- Exception artifacts and manual review queues are not yet produced.
- Several stage documents are still placeholders rather than operating
  documentation.
- CI currently validates only one OS / Python runtime path and does not
  enforce a coverage target.

## Follow-On Issues Added To Catalog

- `#33` Runtime config loading and validation layer
  - Milestone: `M3`
  - Labels: `type:feature`, `area:repo`, `priority:p0`
  - Why: make `config/*.yml` authoritative and fail fast on invalid or
    incomplete rule configuration.
- `#34` Expand CI to Linux and Windows matrix with coverage reporting
  - Milestone: `M6`
  - Labels: `type:chore`, `area:ci`, `priority:p1`
  - Why: validate the documented PowerShell path and raise release
    confidence with coverage visibility.
- `#35` Formalize output schemas and contract tests for pipeline artifacts
  - Milestone: `M6`
  - Labels: `type:feature`, `area:quality`, `priority:p1`
  - Why: define stable contracts for normalized, matches, golden,
    crosswalk, exceptions, and review-queue outputs.

