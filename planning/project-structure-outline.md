# Project Structure Outline

Date prepared: 2026-03-12
Last updated: 2026-03-13

## Objective

Define and track the repository structure for implementing the ETL Identity Data Ruleset Engine from scaffold through release.

## Current Status

`M1` through `M6` are implemented in-repo and `v0.1.0` has shipped.
Post-release hardening and release-follow-up planning are now tracked
separately.

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
      config.yml
      docs.yml
      epic.yml
      feature.yml
    workflows/
      ci.yml
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
    run_checks.ps1
    run_checks.sh
    run_pipeline.ps1
    run_pipeline.py
    run_pipeline.sh
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

## Next Structure Expansion

The following are planned to deepen implementation layers:

- richer normalization and matching strategies beyond the current
  prototype heuristics
- operational workflow support beyond file-based manual review handoff
- post-release planning and tracker synchronization maintenance

## Build Order

## Phase 1 (`M1`) Complete

- scaffold, governance, CI, templates, CLI baseline

## Post-Release Focus

- cut and publish the `v0.1.1` patch release with the packaged sample
  bundle
- tracker and backlog maintenance
- prototype-to-operations hardening for stage composability and runbooks

