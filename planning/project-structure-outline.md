# Project Structure Outline

Date prepared: 2026-03-12
Last updated: 2026-03-12

## Objective

Define and track the repository structure for implementing the ETL Identity Data Ruleset Engine from scaffold through release.

## Current Status

`M1` scaffold is implemented in-repo:

- package and CLI scaffold in `src/etl_identity_engine/`
- tests in `tests/`
- configs in `config/`
- docs placeholders in `docs/`
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
    standards-mapping.md
    survivorship.md
  planning/
    github-issues-backlog.md
    project-structure-outline.md
  scripts/
    create_github_backlog.ps1
    run_pipeline.ps1
    run_pipeline.py
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
    test_generate.py
    test_matching.py
    test_normalize.py
    test_pipeline_e2e.py
    test_survivorship.py
  CODE_OF_CONDUCT.md
  CONTRIBUTING.md
  LICENSE
  pyproject.toml
  README.md
  SAFETY.md
  SECURITY.md
```

## Next Structure Expansion (M2+)

The following are planned to deepen implementation layers:

- richer source schemas and synthetic conflict recipes
- normalization, matching, and survivorship rule files with versioning
- formal output schemas for `normalized`, `matches`, `golden`, and review queues
- additional docs for threshold tuning and runbook operations

## Build Order

## Phase 1 (`M1`) Complete

- scaffold, governance, CI, templates, CLI baseline

## Phase 2 (`M2`)

- synthetic generator enhancements and deterministic conflict injection

## Phase 3 (`M3`)

- normalization orchestration and DQ exception paths

## Phase 4 (`M4`)

- candidate generation, scoring, thresholding, clustering

## Phase 5 (`M5`)

- survivorship rules, provenance, crosswalk outputs

## Phase 6 (`M6`)

- metrics, docs hardening, release packaging

