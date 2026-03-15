# Project Structure Outline

Date prepared: 2026-03-12
Last updated: 2026-03-14

## Objective

Define and track the repository structure for implementing the ETL Identity Data Ruleset Engine from scaffold through release.

## Current Status

`M1` through `M6` are implemented in-repo, the public line has now
advanced through the published `v1.0.0` release, the tracked
`v1.0.0`-`v1.2.0` customer-pilot backlog cycle is complete in-repo, and
the current backlog cycle has completed the `v1.3.0` vendor-adapter
slice and is now focused on secure operations evidence plus customer
deployment supportability, with encrypted backup/export workflows now
implemented in-repo.

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
    cjis.env.example
    compose.yaml
    container.env.example
    kubernetes/
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
    public_safety_demo/
    synthetic_sources/
  docs/
    architecture.md
    benchmarking-and-capacity.md
    cad-source-contract.md
    compatibility-policy.md
    container-deployment.md
    cjis-deployment-baseline.md
    customer-pilot-acceptance-checklist.md
    customer-pilot-bundle.md
    customer-pilot-readiness.md
    customer-pilot-runbooks.md
    event-stream-ingestion.md
    kubernetes-deployment.md
    data-model.md
    delivery-contracts.md
    encrypted-backup-bundles.md
    evaluation-and-metrics.md
    export-jobs.md
    matching-and-thresholds.md
    normalization.md
    operations-observability.md
    output-contracts.md
    persistent-state.md
    production-batch-manifest.md
    production-operating-model.md
    public-safety-onboarding.md
    public-safety-demo.md
    public-safety-vendor-profiles.md
    recovery-runbooks.md
    release-process.md
    review-workflow.md
    rms-source-contract.md
    runtime-environments.md
    service-api.md
    standards-mapping.md
    survivorship.md
    windows-pilot-bootstrap.md
  planning/
    active-github-issues-backlog.md
    github-issues-backlog.md
    post-v0.1.0-github-issues-backlog.md
    post-v0.6.0-github-issues-backlog.md
    post-v0.9.0-github-issues-backlog.md
    post-v1.2.0-github-issues-backlog.md
    project-structure-outline.md
    remaining-work-task-list.md
  fixtures/
    public_safety_onboarding/
    public_safety_regressions/
  scripts/
    bootstrap_venv.ps1
    bootstrap_venv.sh
    check_customer_pilot_readiness.ps1
    check_customer_pilot_readiness.py
    bootstrap_windows_customer_pilot.ps1
    bootstrap_windows_customer_pilot.py
    build_public_safety_demo_site.py
    container_smoke_test.py
    container_supply_chain_check.py
    create_github_backlog.py
    create_github_backlog.ps1
    kubernetes_manifest_smoke.py
    package_customer_pilot_bundle.py
    package_release_sample.py
    package_public_safety_demo.py
    cjis_preflight_check.py
    persisted_state_recovery_smoke.py
    release_hardening_check.py
    restore_encrypted_bundle.py
    run_checks.py
    run_checks.ps1
    run_checks.sh
    run_pipeline.ps1
    run_pipeline.py
    run_pipeline.sh
    run_public_safety_demo_shell.py
    verify_github_issue_metadata.py
  src/
    etl_identity_engine/
      benchmarking.py
      benchmark_runtime.py
      cli.py
      demo_shell/
      encrypted_bundle.py
      generate/
      ingest/
      io/
      matching/
      normalize/
      observability.py
      public_safety_demo.py
      quality/
      storage/
      survivorship/
      streaming.py
  tests/
    test_benchmarking.py
    test_bootstrap_windows_customer_pilot.py
    test_check_customer_pilot_readiness.py
    test_build_public_safety_demo_site.py
    conftest.py
    test_generate.py
    test_io_read.py
    test_matching.py
    test_normalize.py
    test_output_contracts.py
    test_package_release_sample.py
    test_package_customer_pilot_bundle.py
    test_package_public_safety_demo.py
    test_pipeline_e2e.py
    test_public_safety_conformance.py
    test_public_safety_demo_django.py
    test_public_safety_demo.py
    test_public_safety_contracts.py
    test_public_safety_rehearsal.py
    test_quality.py
    test_release_hardening_check.py
    test_repo_scripts.py
    test_runtime_config.py
    test_stream_refresh.py
    test_survivorship.py
    test_create_github_backlog.py
    test_deployment_assets.py
    test_encrypted_bundle.py
    test_github_issue_templates.py
    test_verify_github_issue_metadata.py
    test_state_backup_bundle.py
  CODE_OF_CONDUCT.md
  CONTRIBUTING.md
  LICENSE
  manage_public_safety_demo.py
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

- keep the shipped container image plus single-host and Kubernetes
  deployment assets aligned with the documented runtime paths
- keep release artifact and container attestation outputs aligned with
  the documented release path
- keep benchmark fixtures, capacity targets, and the persisted
  performance contract aligned with the supported single-host and
  clustered deployment baselines
- keep the event-stream contract and persisted stream-refresh runtime
  aligned with the documented micro-batch operator model
- execute the new post-`v1.0.0` backlog cycle tracked in
  `planning/active-github-issues-backlog.md`, now focused on secure
  operations evidence and customer deployment supportability after the
  packaged vendor-adapter slice completed

