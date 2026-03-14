# Project Structure Outline

Date prepared: 2026-03-12
Last updated: 2026-03-14

## Objective

Define and track the repository structure for implementing the ETL Identity Data Ruleset Engine from scaffold through release.

## Current Status

`M1` through `M6` are implemented in-repo, the `v0.1.x` prototype line
has now advanced to the published `v0.6.0` production-readiness
baseline, and a new post-`v0.6.0` backlog cycle is now tracked in the
active backlog.

- package and CLI scaffold in `src/etl_identity_engine/`
- tests in `tests/`
- configs in `config/`
- implementation docs in `docs/`
- issue templates and CI workflows in `.github/`
- governance docs at repo root

## Implemented Structure

```text
ETL-Identity-Data-Ruleset-Engine/
  deploy/
    compose.yaml
    container.env.example
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
    benchmark_fixtures.yml
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
    benchmarking-and-capacity.md
    compatibility-policy.md
    container-deployment.md
    data-model.md
    delivery-contracts.md
    evaluation-and-metrics.md
    export-jobs.md
    matching-and-thresholds.md
    normalization.md
    operations-observability.md
    output-contracts.md
    persistent-state.md
    production-batch-manifest.md
    production-operating-model.md
    recovery-runbooks.md
    release-process.md
    review-workflow.md
    runtime-environments.md
    service-api.md
    standards-mapping.md
    survivorship.md
  planning/
    active-github-issues-backlog.md
    github-issues-backlog.md
    post-v0.1.0-github-issues-backlog.md
    post-v0.6.0-github-issues-backlog.md
    project-structure-outline.md
    remaining-work-task-list.md
  scripts/
    bootstrap_venv.ps1
    bootstrap_venv.sh
    container_smoke_test.py
    create_github_backlog.py
    create_github_backlog.ps1
    package_release_sample.py
    persisted_state_recovery_smoke.py
    release_hardening_check.py
    run_checks.py
    run_checks.ps1
    run_checks.sh
    run_pipeline.ps1
    run_pipeline.py
    run_pipeline.sh
    verify_github_issue_metadata.py
  src/
    etl_identity_engine/
      benchmarking.py
      cli.py
      generate/
      io/
      matching/
      normalize/
      observability.py
      quality/
      storage/
      survivorship/
  tests/
    test_benchmarking.py
    conftest.py
    test_generate.py
    test_io_read.py
    test_matching.py
    test_normalize.py
    test_output_contracts.py
    test_package_release_sample.py
    test_pipeline_e2e.py
    test_quality.py
    test_release_hardening_check.py
    test_repo_scripts.py
    test_runtime_config.py
    test_survivorship.py
    test_create_github_backlog.py
    test_deployment_assets.py
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

- synthetic-only repository data with manifest-driven landed-batch
  runtime support outside the repo
- deterministic explainable matching and survivorship
- persisted review workflow plus a portable CSV queue artifact
- Python-native plus shell-wrapper maintainer entrypoints
- a Python `3.11`/`3.12` CI support matrix with macOS compatibility
  validation

## Build Order

## Phase 1 (`M1`) Complete

- scaffold, governance, CI, templates, CLI baseline

## Post-Release Focus

- keep the shipped container image and compose topology aligned with the
  documented single-host deployment path
- keep benchmark fixtures, capacity targets, and the persisted
  performance contract aligned with the supported deployment baseline
- execute the new post-`v0.6.0` backlog cycle tracked in
  `planning/active-github-issues-backlog.md`

