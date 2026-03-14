# Project Structure Outline

Date prepared: 2026-03-12
Last updated: 2026-03-13

## Objective

Define and track the repository structure for implementing the ETL Identity Data Ruleset Engine from scaffold through release.

## Current Status

`M1` through `M6` are implemented in-repo, `v0.1.0` shipped, and the
`v0.1.2` follow-up patch release is now published. The next planning
cycle should start only when net-new scope exists beyond the completed
release and hardening work.

- package and CLI scaffold in `src/etl_identity_engine/`
- tests in `tests/`
- configs in `config/`
- implementation docs in `docs/`
- issue templates and CI workflows in `.github/`
- governance docs at repo root

## Implemented Structure

```text
ETL-Identity-Data-Ruleset-Engine/
  .github/
    ISSUE_TEMPLATE/
      bug.yml
      chore.yml
      config.yml
      docs.yml
      epic.yml
      feature.yml
    workflows/
      ci.yml
      issue-metadata.yml
      lint.yml
    PULL_REQUEST_TEMPLATE.md
  config/
    blocking_rules.yml
    matching_rules.yml
    normalization_rules.yml
    survivorship_rules.yml
    thresholds.yml
  data/
    candidate_pairs/
    exceptions/
    golden/
    matches/
    normalized/
    synthetic_sources/
  docs/
    architecture.md
    data-model.md
    evaluation-and-metrics.md
    matching-and-thresholds.md
    normalization.md
    output-contracts.md
    release-process.md
    standards-mapping.md
    survivorship.md
  planning/
    active-github-issues-backlog.md
    github-issues-backlog.md
    post-v0.1.0-github-issues-backlog.md
    project-structure-outline.md
    remaining-work-task-list.md
  scripts/
    bootstrap_venv.ps1
    bootstrap_venv.sh
    create_github_backlog.py
    create_github_backlog.ps1
    package_release_sample.py
    run_checks.py
    run_checks.ps1
    run_checks.sh
    run_pipeline.ps1
    run_pipeline.py
    run_pipeline.sh
    verify_github_issue_metadata.py
  src/
    etl_identity_engine/
      cli.py
      generate/
      io/
      matching/
      normalize/
      quality/
      survivorship/
  tests/
    conftest.py
    test_generate.py
    test_io_read.py
    test_matching.py
    test_normalize.py
    test_output_contracts.py
    test_package_release_sample.py
    test_pipeline_e2e.py
    test_quality.py
    test_repo_scripts.py
    test_runtime_config.py
    test_survivorship.py
    test_create_github_backlog.py
    test_github_issue_templates.py
    test_verify_github_issue_metadata.py
  CODE_OF_CONDUCT.md
  CONTRIBUTING.md
  LICENSE
  pyproject.toml
  README.md
  SAFETY.md
  SECURITY.md
```

## Current Direction

The current public line is now explicitly scoped around:

- synthetic-only public inputs
- deterministic explainable matching and survivorship
- CSV manual-review handoff
- Python-native plus shell-wrapper maintainer entrypoints
- a Python `3.11`/`3.12` CI support matrix with macOS compatibility
  validation

## Build Order

## Phase 1 (`M1`) Complete

- scaffold, governance, CI, templates, CLI baseline

## Post-Release Focus

- maintain tracker and backlog hygiene until new scope is ready to be
  opened
- keep release tooling and runbooks aligned with the published `v0.1.2`
  behavior
- treat any new product or workflow changes as a fresh backlog cycle
  rather than implicit continuation of the completed hardening work

