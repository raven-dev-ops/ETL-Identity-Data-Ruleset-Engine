# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

- Added packaged CAD and RMS vendor profiles for common public-safety
  onboarding shapes, including bundle-local or manifest-declared
  `vendor_profile` support, direct CLI validation, and tests for valid
  and invalid profile-shaped inputs.
- Added machine-readable onboarding diff reports for CAD/RMS bundles
  and manifests, including mapped, unused, and unresolved column
  reporting plus JSON-first failure output from
  `check-public-safety-onboarding`.
- Added `generate-public-safety-vendor-batches`, which writes
  synthetic vendor-profile-shaped CAD and RMS onboarding bundles plus a
  matching manifest for rehearsal, demo, and pre-sales use.
- Added shared Ed25519 detached-signature support for release and
  customer-handoff manifests, including signed bundle packaging,
  standalone signature verification tooling, and customer-pilot
  readiness enforcement when signed handoff manifests are present.
- Added `_FILE` mounted-secret resolution for runtime environments,
  `check-runtime-auth-material` plus `serve-api` startup gates for auth
  and JWT signing material, and CJIS preflight support for optional
  secret-rotation age checks.
- Added encrypted persisted-state backup and restore workflows, the
  shared encrypted-bundle format plus generic extract tooling, and
  optional encrypted customer-pilot delivery packaging.

## [1.0.0] - 2026-03-14

- Added versioned `cad_call_for_service` and `rms_report_person`
  public-safety source-bundle contracts, including a contract marker
  file, required file-role validation, referential integrity checks, and
  a `validate-public-safety-contract` CLI command.
- Extended the production batch manifest model with named `source_bundles`
  for CAD and RMS onboarding so manifests can validate contract-bound
  public-safety bundles alongside landed person sources before runtime
  execution.
- Added `check-public-safety-onboarding`, a checked-in
  `fixtures/public_safety_onboarding/` example tree, and onboarding
  docs so source owners can self-check CAD/RMS bundles and example
  manifests before pipeline onboarding.
- Added vendor-column mapping overlays for CAD/RMS source bundles, with
  bundle-local and manifest-declared overlay support plus shipped
  vendor-shaped onboarding fixtures for both CAD and RMS examples.
- Added manifest-driven public-safety activity persistence from
  contract-valid CAD/RMS source bundles, including incident-to-identity
  and golden-activity SQL artifacts, persisted-run restoration, and
  end-to-end coverage for the contract-to-state path.
- Added persisted public-safety read models over that activity state,
  including authenticated service endpoints for incident-to-identity
  and golden-activity views plus standalone Django demo-shell loading
  directly from a persisted run or a packaged demo bundle.
- Added a checked-in `fixtures/public_safety_regressions/` proof set for
  same-person, same-household, and false-merge public-safety scenarios,
  plus matching, survivorship, and end-to-end tests that lock those
  outcomes.
- Added `package_customer_pilot_bundle.py`, which builds a standalone
  seeded public-safety customer pilot archive containing the seed
  dataset, persisted SQLite state, prepared Django demo shell, minimal
  runtime payload, and local startup helpers.
- Added a Windows-first customer-pilot bootstrap path that provisions a
  local PostgreSQL container, reruns the bundled seed manifest into
  PostgreSQL, rebuilds the Django demo shell from that state, and
  writes launch/config helpers for the single-host pilot baseline.
- Added a customer-pilot readiness check and hashed handoff manifest so
  the delivered bundle can verify artifact integrity and validate the
  documented Windows single-host baseline before bootstrap.
- Added operator/admin pilot runbooks plus a reusable customer-pilot
  acceptance checklist for install, startup, rollback, backup, and demo
  handoff execution.

## [0.9.2] - 2026-03-14

- Added shared observability redaction for structured logs and persisted
  audit-event details so free-text review notes, auth material, JWT-like
  tokens, PEM payloads, and DSN passwords are not retained verbatim in
  the operational trace surfaces.

## [0.9.1] - 2026-03-14

- Added a dedicated `cjis` runtime environment, a `deploy/cjis.env.example`
  deployment surface, a `cjis_preflight_check.py` script, and new CJIS
  deployment/standards docs so the repo now has a concrete repo-side
  baseline for CJIS-aligned production rollouts.

## [0.9.0] - 2026-03-14

- Added a concrete synthetic CAD/RMS public-safety demo stage and
  output slice, including `public-safety-demo`, a joined
  incident-to-identity view, a per-golden-person activity rollup, and
  dedicated demo dashboard, report, and summary artifacts.
- Added a deterministic `package_public_safety_demo.py` script for
  producing a single zipped mock CAD/RMS demo handoff bundle.
- Added a standalone Django + SQLite public-safety demo shell, plus a
  `run_public_safety_demo_shell.py` bootstrap path that loads the
  packaged mock CAD/RMS bundle into a self-contained read-only local
  walkthrough.
- Added a `build_public_safety_demo_site.py` script that turns the demo
  bundle into a hostable static shell with an overview page, scenario
  cards, embedded dashboard, and direct artifact links.
- Added PostgreSQL-backed persisted-state support alongside SQLite,
  normalized state-store URL handling, and PostgreSQL dialect coverage
  for the migration/runtime path.
- Added immutable replay-bundle archiving plus verification for
  manifest-driven persisted runs, including archived manifest/input
  snapshots recorded in persisted run summaries.
- Added direct replay-from-bundle support so `replay-run` no longer
  depends on the original manifest and landing paths once a verified
  archived bundle exists.
- Added durable run checkpoints plus failed-run resume support so
  persisted reruns can continue from the latest completed stage instead
  of restarting the entire pipeline.
- Added JWT bearer service authentication with issuer, audience,
  signing-key, and external-claim role mapping support, while retaining
  API-key auth as a documented compatibility mode for local/container
  deployments.
- Added endpoint-level service scopes, JWT scope-claim narrowing, and
  audit-context propagation for authenticated actor subject, role,
  granted scopes, and required scopes.
- Added authenticated service control-plane endpoints for persisted
  run publication and named export-job triggers, with idempotent reuse
  and persisted audit coverage.
- Added paginated run, golden-record, and review-case collection
  endpoints to the authenticated service API, with documented filter,
  sort, and page-token semantics.
- Added a Kubernetes deployment baseline for PostgreSQL-backed service
  and batch execution, including shipped manifests, example secrets, a
  dedicated `cluster` runtime environment, and a Docker-backed
  Kubernetes manifest smoke test wired into CI.
- Added retained container supply-chain outputs for the built image,
  including an attestation bundle, SBOM-style inventory, provenance,
  and a dependency-audit gate wired into CI.
- Added file-backed `stream-refresh` event-batch ingestion with
  deterministic persisted entity recomputation, copied event snapshots,
  stream lineage in `run_summary.json`, and continuous-ingest benchmark
  coverage.
- Added clustered benchmark fixtures and explicit SLO-metric output for
  the PostgreSQL-backed runtime baseline, including event-stream
  latency targets and temporary PostgreSQL provisioning for the
  distributed benchmark path.
- Changed runtime-environment loading to resolve only the selected
  environment's secret placeholders, so compatibility and cluster
  deployments do not require unrelated environment secrets.

## [0.6.0] - 2026-03-14

### Added

- Production batch-manifest support for `normalize` and `run-all`,
  including a documented local landing-zone contract for manifest-driven
  CSV and Parquet inputs.
- Landing-zone adapter support for object-storage-compatible manifest
  inputs through `fsspec` URIs, with end-to-end coverage using
  `memory://` batches.
- Optional SQLite-backed persistence for completed runs and core
  pipeline artifacts, plus a persisted-state reload path for `report`.
- Run-registry lifecycle support for persisted executions, including
  `running`/`completed`/`failed` status, failure detail capture,
  idempotent completed-run reuse, and clean restart attempts after
  failure.
- Alembic-backed state database migrations with CLI bootstrap and
  revision inspection commands.
- Runtime environment configuration with per-environment overlays and
  `${ENV_VAR}` resolution for secret-backed settings.
- Manifest-driven incremental refresh over persisted state, including
  predecessor reuse for unaffected entities and fallback-to-full
  behavior when no compatible predecessor is available.
- Versioned downstream delivery publication for persisted golden and
  crosswalk outputs, including immutable snapshot directories and an
  atomic `current.json` pointer for consumers.
- Persisted manual-review case workflow support with durable assignee,
  notes, timestamps, lifecycle status, and CLI inspection/update
  commands.
- Review-decision override support on persisted reruns, so approved
  cases can force merges and rejected cases can block later
  cluster/golden rebuilds.
- A read-only operator service API for persisted run status, golden
  record lookup, crosswalk lookup, and review-case retrieval.
- Operator CLI wrappers for idempotent review decisions, persisted
  manifest replay, and downstream publication triggers.
- Named downstream export jobs for warehouse and data-product consumers,
  including audited SQLite export-run tracking and JSON operator
  commands for export execution and history.
- A shared compatibility policy for external service, workflow, and
  downstream delivery consumers.
- API-key authentication and role-based service authorization for
  persisted operator APIs, including operator-only review decision and
  replay actions.
- A shared observability baseline with structured JSON logs, persisted
  SQLite audit events for privileged actions, authenticated `readyz`
  and `/api/v1/metrics` endpoints, and store-backed batch/service
  operational metrics.
- A containerized runtime baseline with a shared batch/service image, a
  single-host compose deployment manifest, and a reusable container
  smoke test wired into CI.
- Named scale benchmark fixtures, a `benchmark-run` CLI command, and a
  documented `single_host_container` capacity-target baseline for
  larger-batch operational validation.
- Backup, restore, and replay runbooks for persisted SQLite state,
  including a documented recovery smoke path in
  `scripts/persisted_state_recovery_smoke.py`.
- A retained release-hardening check that builds the distribution,
  writes dependency-inventory outputs, and records artifact hashes for
  release validation.
- A coherent production operating-model document covering rollout,
  rollback, governance boundaries, PII handling, audit expectations,
  and consumer responsibilities.

### Changed

- The runtime now validates batch manifests, source IDs, and required
  input columns before normalization starts, so invalid landed batches
  fail fast without writing partial normalized outputs.
- Manifest-driven runtime inputs now support both local filesystem and
  object-storage-compatible landing-zone resolution.
- Commands that consume matching or normalization rules can now resolve
  `config_dir` defaults from a named runtime environment without editing
  committed YAML files in place.
- Persisted run summaries and markdown reports now record batch context,
  refresh mode, predecessor reuse, and incremental fallback decisions.
- The runtime now exposes `publish-delivery` so downstream ETL consumers
  can read stable golden/crosswalk snapshots from persisted state
  instead of the prototype working directory.
- Persisted review-case rows now survive run reuse and expose the
  `pending`, `approved`, `rejected`, and `deferred` lifecycle states
  through SQLite-backed workflow commands.
- Later manifest-driven reruns now carry approved/rejected review
  decisions forward into candidate decisions, active-queue projection,
  cluster assignments, and golden-record rebuilds.
- The runtime now exposes `serve-api` for local and CI-accessible
  operator queries over persisted SQLite state with explicit request and
  response validation.
- `run-all` summaries and markdown reports now record phase timing and
  throughput metrics for the core pipeline stages, including persisted
  state writes when `--state-db` is enabled.
- The runtime now exposes `apply-review-decision`, `replay-run`, and
  `publish-run` as JSON-producing operator commands over the persisted
  workflow surface.
- The runtime now exposes `export-job-list`, `export-job-run`, and
  `export-job-history` so persisted runs can be materialized through
  configured downstream export locations with auditable reuse semantics.
- Service, workflow, and downstream delivery docs now identify stable
  versus internal surfaces and define shared versioning plus
  deprecation expectations for external consumers.
- `serve-api` now requires environment-backed service auth
  configuration, and the production runtime environment now defines
  `reader` and `operator` API-key slots through runtime config.
- The runtime environment catalog now includes a dedicated `container`
  profile so the single-host deployment topology can start without
  cloud object-storage credentials while still using the persisted-state
  and service-auth model.
- `scripts/run_checks.py` now executes a persisted-state recovery smoke
  path, so local validation and CI cover backup, restore, report
  rebuild, and replay behavior for manifest-driven persisted runs.
- CI now includes a `release-hardening` job that publishes a retained
  dependency inventory and audit artifact for the built release
  distribution.
- The architecture, safety, and security docs now link to a single
  production operating-model reference instead of scattering rollout and
  governance boundaries across separate notes.
- `scripts/release_hardening_check.py` now clears stale artifact files
  in its output directory before building, so the documented retained
  release path is rerun-safe on `dist/release-hardening`.

## [0.1.4] - 2026-03-13

### Changed

- `scripts/run_checks.py` now verifies that the installed
  `etl-identity-engine` distribution metadata matches
  `pyproject.toml`, so stale editable installs are caught before local
  validation reports a false-green result.
- `scripts/run_checks.py` now also verifies that the current tree
  produces exactly one wheel and one source distribution, so packaging
  failures are caught by the normal local and CI validation path, and it
  cleans up a repo-local `build/` directory when that check created one.
- `scripts/run_checks.py` now also smoke-tests the installed
  `etl-identity-engine` console entrypoint, so local validation catches
  broken or missing script installation alongside packaging failures.
- Package metadata now uses the non-deprecated SPDX-style `MIT` license
  declaration in `pyproject.toml`, so source and wheel builds no longer
  emit the setuptools `project.license` deprecation warning.
- The repo now ignores `build/`, so manual source and wheel builds do
  not leave accidental untracked packaging workspaces behind.
- The repo now also ignores local coverage artifacts such as
  `.coverage`, `coverage.xml`, and `htmlcov/`, so coverage runs do not
  dirty the worktree.

## [0.1.3] - 2026-03-13

### Changed

- Synced the package `__version__` metadata with `pyproject.toml` and
  added a regression test so future releases cannot drift between the
  installed package version and project metadata.
- Refreshed the active planning summaries so the repository now records
  the published `v0.1.2` state instead of a pending follow-up
  patch-release candidate.

## [0.1.2] - 2026-03-13

### Added

- Lightweight phonetic-name scoring for explainable candidate matching.
- Deterministic release-sample bundle packaging tests, including a
  byte-stability check for fixed metadata.
- A cross-platform Python-native `scripts/run_checks.py` entrypoint for
  shell-free local validation.

### Changed

- Release-sample packaging now writes deterministic zip entry metadata
  and derives the manifest timestamp from `SOURCE_DATE_EPOCH` or the
  HEAD commit timestamp so rebuilds are reproducible for a fixed commit.
- Active-backlog sync now skips entries marked `Status: closed` by
  default, with explicit `--include-closed` support for historical
  re-syncs.
- Local `run_checks` wrappers now validate the active backlog dry-run
  and release-sample packaging in addition to lint and tests, and CI now
  uses `--include-closed` for the historical bootstrap-backlog dry-run.
- Local `run_checks` release-sample validation now uses temporary output
  directories so routine pre-push checks do not leave retained bundles
  under `dist/`.
- `run_checks` and `run_pipeline` shell wrappers now delegate to the
  Python entrypoints so local maintenance behavior stays aligned across
  platforms.
- CI now validates Python `3.12` compatibility on Linux and Windows and
  adds a macOS `3.12` compatibility job alongside the existing Linux and
  Windows Python `3.11` baseline jobs.
- Public scope documentation now treats the synthetic-only boundary,
  deterministic heuristic matching strategy, and CSV manual-review model
  as explicit supported boundaries rather than open product ambiguity.
- The PowerShell backlog wrapper now delegates to the Python backlog
  script so both entrypoints stay behaviorally aligned.

## [0.1.1] - 2026-03-13

### Added

- Standalone `cluster` and `review-queue` CLI stages so the file-based
  pipeline can be rerun step-by-step outside `run-all`.
- Direct IO-reader tests, release-bundle packaging tests, and local test
  bootstrap for the `src/` package layout.
- Cross-platform `scripts/package_release_sample.py` for generating the
  documented release sample zip with a `manifest.json`.

### Changed

- Standalone `golden` now requires real cluster assignments instead of
  inferring groups from synthetic `person_entity_id` values.
- Standalone `report` now reads the downstream match, cluster, golden,
  and review-queue artifacts so its summary matches `run-all`.
- Stage input reads now fail fast on missing files, and `normalize`
  falls back to Parquet discovery only when CSV inputs are absent.
- `run-all` now accepts `--formats` and can normalize directly from
  generated Parquet inputs when CSV output is not requested.
- Pipeline wrapper scripts now forward arbitrary `run-all` CLI
  arguments instead of hard-coding a subset of options.
- Release-process documentation now uses the packaged sample-bundle
  workflow and the post-release tracker snapshot now records epic `#44`
  accurately.

### Removed

- Unused `generate.schemas` and `quality.reporting` modules from the
  package surface.

## [0.1.0] - 2026-03-13

### Added

- Config-driven CLI stages for synthetic generation, normalization,
  matching, golden-record generation, reporting, and `run-all`
  orchestration.
- Deterministic synthetic multi-table source generation with conflict
  injection and CSV or Parquet output.
- Multi-pass blocking metrics, explainable weighted scoring, clustering,
  survivorship provenance, and source-to-golden crosswalk outputs.
- Runtime config validation, data-quality exception artifacts,
  before/after completeness metrics, and duplicate-reduction reporting.
- Stage docs, output-contract docs, release-process docs, and GitHub
  backlog automation.

### Changed

- CI now validates Linux and Windows paths and publishes coverage
  artifacts.
- Pipeline artifact schemas are treated as stable contracts and covered
  by dedicated contract tests.

### Known Limitations

- Matching is intentionally rules-based and does not include advanced
  phonetic or ML-assisted scoring.
- The manual review queue remains a file output rather than a managed
  workflow.
