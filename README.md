# ETL Identity Data Ruleset Engine

ETL Identity Data Ruleset Engine is a prototype framework for improving
data quality and identity consistency in multi-source datasets. The
project demonstrates how an ETL pipeline can normalize inconsistent
records, detect likely duplicate identities through probabilistic
matching, and generate trusted "golden records" using deterministic
survivorship rules.
It also now exposes a concrete mock public-safety slice that joins CAD
and RMS incident activity back to the golden-person outputs.

Many operational systems ingest identity data from multiple sources that
store information with inconsistent formatting, partial identifiers, and
duplicate records. These inconsistencies propagate downstream into
reporting systems, analytics environments, and search interfaces,
reducing trust in the data.

This repository models those challenges using synthetic datasets and now
also supports manifest-driven local and object-storage-compatible batch
inputs for production-style evaluation. Teams can still explore
identity-resolution techniques safely with synthetic data, then validate
the same staged runtime against landed CSV or Parquet batches under an
explicit manifest contract. The public-safety slice now also ships
versioned CAD and RMS bundle contracts so source-system owners can
validate onboarding extracts before they are wired into the runtime, and
the production batch manifest can now declare those CAD/RMS bundles as
named contract-bound source classes alongside the landed person inputs.
Those public-safety bundles can now also carry vendor-column mapping
overlays so real CAD/RMS extracts do not have to rename themselves into
the synthetic canonical headers before onboarding validation starts.
For CAD and RMS onboarding, the repo now also ships maintained packaged
vendor profiles for common export shapes, so operators can select a
supported `vendor_profile` instead of authoring a custom overlay for
every pilot.
The onboarding path now also emits machine-readable per-file diff
reports for mapped canonical fields, unused source columns, and
required fields that still have no resolvable source mapping, so vendor
drift is visible without digging through raw CSV headers by hand.
Manifest-driven runs with `--state-db` now also persist the derived
incident-to-identity activity view from those validated CAD/RMS
bundles, so the demo slice can be restored directly from persisted
state instead of depending on synthetic sidecar files.
For explicit merge and no-merge proof cases, the repo also ships
`fixtures/public_safety_regressions/`, which packages canonical
same-person, same-household, and false-merge public-safety scenarios.

## Core Concepts

### Normalization

Standardizes inconsistent fields into canonical formats before
transformation and loading.

Examples:

- Convert `Smith, John` and `John A Smith` into a normalized name format.
- Standardize date formats.
- Normalize address structures.
- Remove punctuation and formatting inconsistencies.

### Probabilistic Identity Matching

Evaluates potential duplicate records using weighted signals rather than
exact matches.

Signals may include:

- name similarity
- date-of-birth match
- address overlap
- phone or identifier similarity
- phonetic matching

Each potential match receives a confidence score indicating the
likelihood that two records represent the same person.

### Golden Record Generation

When duplicates are detected, survivorship rules determine which
attributes become the authoritative record.

Examples of survivorship rules:

- Prefer full legal names over abbreviated names.
- Prefer verified identifiers over inferred values.
- Prefer the most recent address.
- Preserve source attribution for traceability.

The result is a consolidated trusted identity record.

## Goals

- Demonstrate identity resolution within an ETL pipeline.
- Improve data trust in downstream analytics and reporting.
- Provide a transparent rules-based approach to duplicate detection.
- Create a modular architecture adaptable to different data
  environments.
- Provide a synthetic environment for safe experimentation.

## High-Level Pipeline

`Source Data -> Normalization Layer -> Duplicate Candidate Generation -> Probabilistic Matching Engine -> Survivorship Rules Engine -> Golden Record Output -> Analytics / Reporting Data`

## Example Identity Conflict

Synthetic input records:

| ID | Name | DOB | Address |
| --- | --- | --- | --- |
| 1 | John A Smith | 1985-03-12 | 123 Main St |
| 2 | Smith, John | 1985-03-12 | 123 Main Street |
| 3 | Jon Smith | 1985-03-12 | 123 Main St |

Matching detects a high-confidence duplicate set and generates:

| Canonical Name | DOB | Address |
| --- | --- | --- |
| John A Smith | 1985-03-12 | 123 Main St |

## Repository Structure

```text
ETL-Identity-Data-Ruleset-Engine/
  .github/
  config/
  data/
  deploy/
  docs/
  planning/
  scripts/
  src/etl_identity_engine/
  tests/
```

Detailed structure and sequencing are tracked in
[planning/project-structure-outline.md](planning/project-structure-outline.md).

## Intended Use

This project is designed as a reference implementation for identity
resolution within ETL workflows. It is useful for teams dealing with
fragmented identity data across multiple operational systems and looking
to improve:

- data quality
- duplicate detection
- analytics reliability
- reporting accuracy

## Data Safety

All datasets used in this project are synthetic and generated for
demonstration purposes only. No operational, personal, or sensitive data
is included in the repository.

## Scope Boundaries

- The runtime now supports two input modes: synthetic generation for
  safe experimentation and manifest-driven landed batches for
  production-style evaluation. Local filesystem and object-storage-
  compatible landing zones are supported. Persisted SQL state via
  SQLite paths or PostgreSQL URLs,
  archived replay bundles for manifest-era inputs,
  manifest-driven incremental refresh, a container image, a single-host
  compose deployment baseline, a Kubernetes PostgreSQL-backed
  deployment baseline, named benchmark fixtures with single-host and
  clustered capacity targets, and an authenticated operator service API
  are now available.
  The current production runtime supports JWT bearer auth backed by
  deployment-supplied issuer, audience, and signing metadata, while the
  local container baseline retains API-key compatibility mode. The
  service surface now enforces documented endpoint scopes in addition to
  the stable `reader` and `operator` roles.
  The current service line supports paginated read-side lists and
  lookups plus operator-only review decision, replay, publish, and
  export-trigger actions.
- The supported matching track for the current release line is
  deterministic and explainable: exact matches plus heuristic partial
  and phonetic signals. ML-assisted scoring is intentionally out of
  scope for the supported public line.
- The supported manual-review operating model for the current release line
  now has two layers: the CSV queue artifact remains the portable file
  handoff, and persisted runs can also track review-case status,
  assignee, timestamps, and notes in the configured state store.
  Approved and rejected
  review decisions now carry forward into later persisted reruns and can
  override heuristic cluster and golden outcomes.

## Future Enhancements

- Configurable identity matching rules
- Additional explainable heuristic signals and richer offline evaluation
- Address standardization using geocoding services
- Long-lived broker integrations beyond the current deterministic
  file-backed event batch model
- Distributed real-time identity resolution pipelines

## Planning Artifacts

- [Project Structure Outline](planning/project-structure-outline.md)
- [Remaining Work Task List](planning/remaining-work-task-list.md)
- [Active GitHub Issues Backlog](planning/active-github-issues-backlog.md)
- [Post-v1.2.0 GitHub Issues Backlog (Historical)](planning/post-v1.2.0-github-issues-backlog.md)
- [Post-v0.9.0 GitHub Issues Backlog (Historical)](planning/post-v0.9.0-github-issues-backlog.md)
- [Post-v0.6.0 GitHub Issues Backlog (Historical)](planning/post-v0.6.0-github-issues-backlog.md)
- [Post-v0.1.0 GitHub Issues Backlog (Historical)](planning/post-v0.1.0-github-issues-backlog.md)
- [Bootstrap GitHub Issues Backlog (Historical)](planning/github-issues-backlog.md)

## Documentation

- [Architecture](docs/architecture.md)
- [CAD Source Contract](docs/cad-source-contract.md)
- [Data Model](docs/data-model.md)
- [Normalization](docs/normalization.md)
- [Production Batch Manifest](docs/production-batch-manifest.md)
- [Compatibility Policy](docs/compatibility-policy.md)
- [Benchmarking and Capacity](docs/benchmarking-and-capacity.md)
- [Container Deployment](docs/container-deployment.md)
- [CJIS Deployment Baseline](docs/cjis-deployment-baseline.md)
- [Customer Pilot Bundle](docs/customer-pilot-bundle.md)
- [Customer Pilot Runbooks](docs/customer-pilot-runbooks.md)
- [Customer Pilot Acceptance Checklist](docs/customer-pilot-acceptance-checklist.md)
- [Customer Pilot Readiness](docs/customer-pilot-readiness.md)
- [Event Stream Ingestion](docs/event-stream-ingestion.md)
- [Kubernetes Deployment](docs/kubernetes-deployment.md)
- [Delivery Contracts](docs/delivery-contracts.md)
- [Export Jobs](docs/export-jobs.md)
- [Persistent State](docs/persistent-state.md)
- [Recovery Runbooks](docs/recovery-runbooks.md)
- [Review Workflow](docs/review-workflow.md)
- [Service API](docs/service-api.md)
- [Runtime Environments](docs/runtime-environments.md)
- [Matching and Thresholds](docs/matching-and-thresholds.md)
- [Operations and Observability](docs/operations-observability.md)
- [Survivorship](docs/survivorship.md)
- [Evaluation and Metrics](docs/evaluation-and-metrics.md)
- [Output Contracts](docs/output-contracts.md)
- [Public Safety Onboarding](docs/public-safety-onboarding.md)
- [Public Safety Vendor Profiles](docs/public-safety-vendor-profiles.md)
- [Public Safety Demo](docs/public-safety-demo.md)
- [Production Operating Model](docs/production-operating-model.md)
- [Release Process](docs/release-process.md)
- [RMS Source Contract](docs/rms-source-contract.md)
- [Standards Mapping](docs/standards-mapping.md)
- [Windows Pilot Bootstrap](docs/windows-pilot-bootstrap.md)

## Maintainer Release Bundle

Package the documented release sample archive with:

```bash
python scripts/package_release_sample.py --output-dir dist/release-samples --profile small --seed 42 --formats csv,parquet
```

The release process treats that script as the authoritative bundle
entrypoint, and the resulting zip should be attached to the GitHub
release for the matching tag.

For a fixed clean commit, the bundle is byte-stable across reruns. The
packaging script derives `generated_at_utc` from the HEAD commit
timestamp by default and also honors `SOURCE_DATE_EPOCH` when you need a
reproducible rebuild timestamp override.

## Governance and Safety

- [License](LICENSE)
- [Contributing](CONTRIBUTING.md)
- [Security](SECURITY.md)
- [Code of Conduct](CODE_OF_CONDUCT.md)
- [Safety](SAFETY.md)

## Bootstrap Status (M1)

This repository now includes a working `M1` scaffold:

- Python package skeleton under `src/etl_identity_engine/`
- stage CLI commands: `generate`, `normalize`, `match`, `cluster`,
  `review-queue`, `golden`, `public-safety-demo`, `report`,
  `publish-delivery`, `publish-run`, `review-case-list`,
  `review-case-update`, `apply-review-decision`, `replay-run`,
  `stream-refresh`, `benchmark-run`, `export-job-list`,
  `export-job-run`, `export-job-history`, `serve-api`, `run-all`
- base test suite under `tests/`
- CI and issue templates under `.github/`
- governance files: `LICENSE`, `CONTRIBUTING.md`, `SECURITY.md`,
  `CODE_OF_CONDUCT.md`, `SAFETY.md`

## Synthetic Generator Status (M2)

`generate` now produces deterministic multi-table synthetic outputs with
configurable conflict injection:

- `person_source_a.*`
- `person_source_b.*`
- `conflict_annotations.*`
- `incident_records.*`
- `incident_person_links.*`
- `address_history.*`
- `generation_summary.json`

Formats are configurable (`csv`, `parquet`) and default to both.
The `normalize` stage auto-discovers CSV inputs first and falls back to
Parquet only when CSV inputs are absent, so one discovered format set is
used per run.

## Current Runtime Outputs

`run-all` now executes a full config-driven prototype slice and writes:

- normalized rows in `data/normalized/normalized_person_records.csv`
- scored candidate pairs with `decision`, `matched_fields`, and
  `reason_trace` in `data/matches/candidate_scores.csv`
- per-pass blocking metrics in `data/matches/blocking_metrics.csv`
- deterministic cluster assignments in `data/matches/entity_clusters.csv`
- golden records with field-level provenance in
  `data/golden/golden_person_records.csv`
- a source-to-golden crosswalk in
  `data/golden/source_to_golden_crosswalk.csv`
- a CAD/RMS incident-to-identity demo slice in `data/public_safety_demo/`
- a manual review queue in `data/review_queue/manual_review_queue.csv`
- exception artifacts and summary outputs under `data/exceptions/`
- phase timing and throughput metrics in `data/exceptions/run_summary.json`

The stable output shapes for those files are documented in
[docs/output-contracts.md](docs/output-contracts.md).
Persisted runs can also be published for downstream consumers through
the versioned snapshot contract documented in
[docs/delivery-contracts.md](docs/delivery-contracts.md).

The current matcher remains rules-based, but it now includes exact,
partial, and lightweight phonetic-name signals. Candidate outputs expose
those derived signals explicitly through `matched_fields` and
`reason_trace`.

When `run-all` is paired with `--state-db` and a manifest input, the
runtime also supports `--refresh-mode incremental`. That path reuses the
prior completed manifest run from persisted state, recalculates only the
affected entities and candidate pairs, and records the refresh outcome
in `data/exceptions/run_summary.json`.
Completed persisted runs can then be published into immutable golden and
crosswalk snapshots with `publish-delivery`.
When the manifest also declares CAD/RMS `source_bundles`, the persisted
run now keeps the joined incident-to-identity rows and golden-person
activity rollup as first-class SQL artifacts so the same demo slice can
be restored on reuse or replay.

The persisted runtime now also supports near-real-time micro-batch
refresh through `stream-refresh`. That command applies an ordered JSONL
event batch onto a completed persisted predecessor run, copies the
processed event file into `data/events/stream_events.jsonl`, and records
the stream batch digest, sequence range, and predecessor lineage in the
resulting `run_summary.json`.

The current manual-review operating model keeps the file handoff via
`data/review_queue/manual_review_queue.csv`, and persisted runs now also
support durable review-case state through `review-case-list` and
`review-case-update`. Approved and rejected review decisions now apply
to later persisted reruns, forcing merge or non-merge outcomes before
cluster and golden rebuilds.

Persisted SQL state can also now be served through an authenticated
operator API with `serve-api`. That surface exposes run status, golden
record lookups, source-to-golden crosswalk lookups, paginated run,
golden, and review-case collections, and persisted review-case
retrieval for downstream systems and operators, and it now supports
operator-only review decision, replay, publish, and export-trigger
actions behind separate API-key or JWT-backed roles. It also now
exposes persisted public-safety read models for golden-person activity
and incident-to-identity rows behind the dedicated
`public_safety:read` scope. It also now exposes authenticated
`healthz`, `readyz`, and `/api/v1/metrics` endpoints, while privileged
CLI and service actions emit structured JSON logs and persist audit
events in the configured state store. The service contract is documented in
[docs/service-api.md](docs/service-api.md), and the current operations
baseline is documented in
[docs/operations-observability.md](docs/operations-observability.md).

For operator workflows, the CLI now also exposes:

- `apply-review-decision` for idempotent review-case decisions
- `replay-run` for manifest-backed persisted reruns
- `stream-refresh` for ordered event-batch entity refresh over persisted
  state
- `publish-run` for JSON-based downstream publication triggers
- `export-job-list` for configured warehouse and data-product exports
- `export-job-run` for auditable downstream snapshot materialization
- `export-job-history` for export execution history and reuse tracking

Configured downstream export jobs now layer on top of the versioned
delivery contract and can target distinct warehouse or data-product
roots from `config/export_jobs.yml`. That operator surface is documented
in [docs/export-jobs.md](docs/export-jobs.md).
External service and workflow integrations should also follow the shared
compatibility rules in
[docs/compatibility-policy.md](docs/compatibility-policy.md).
Container build and compose deployment guidance is documented in
[docs/container-deployment.md](docs/container-deployment.md).
Benchmark fixture definitions, regression targets, and the
`benchmark-run` workflow are documented in
[docs/benchmarking-and-capacity.md](docs/benchmarking-and-capacity.md).
Container image attestation, provenance, and scan gates are documented
in [docs/release-process.md](docs/release-process.md).

For a concrete mock CAD/RMS demonstration, run the pipeline and inspect
the synthetic public-safety demo outputs:

```bash
python -m etl_identity_engine.cli run-all --base-dir demo-output --profile small --seed 42
python -m etl_identity_engine.cli public-safety-demo --base-dir demo-output
```

That produces:

- `data/public_safety_demo/incident_identity_view.csv`
- `data/public_safety_demo/golden_person_activity.csv`
- `data/public_safety_demo/public_safety_demo_dashboard.html`
- `data/public_safety_demo/public_safety_demo_report.md`
- `data/public_safety_demo/public_safety_demo_scenarios.json`
- `data/public_safety_demo/public_safety_demo_summary.json`
- `data/public_safety_demo/public_safety_demo_walkthrough.md`

To hand the demo to someone as one artifact:

```bash
python scripts/package_public_safety_demo.py --output-dir dist/public-safety-demo --profile small --seed 42 --formats csv,parquet
```

To prepare a standalone Django + SQLite demo shell around that bundle:

```bash
python scripts/run_public_safety_demo_shell.py --output-dir dist/public-safety-demo-django --profile small --seed 42 --formats csv,parquet --prepare-only
python scripts/run_public_safety_demo_shell.py --output-dir dist/public-safety-demo-django --profile small --seed 42 --formats csv,parquet --host 127.0.0.1 --port 8000
```

Or to load the same shell directly from persisted state:

```bash
python scripts/run_public_safety_demo_shell.py --output-dir dist/public-safety-demo-django --state-db data/state/pipeline_state.sqlite --run-id RUN-20260314T000000Z-EXAMPLE --prepare-only
python scripts/run_public_safety_demo_shell.py --output-dir dist/public-safety-demo-django --state-db data/state/pipeline_state.sqlite --run-id RUN-20260314T000000Z-EXAMPLE --host 127.0.0.1 --port 8000
```

That standalone shell uses Django's built-in SQLite backend and serves a
read-only local walkthrough over either the packaged bundle or a
materialized persisted run, so it stays self-contained for buyer demos.
The copy and scenario flow are tuned for an ID Network-style CAD/RMS
identity-resolution conversation.

To package that same seeded flow as a standalone customer pilot handoff
with persisted state, the prepared demo shell, and startup helpers:

```bash
python scripts/package_customer_pilot_bundle.py --output-dir dist/customer-pilot
```

That bundle is documented in
[docs/customer-pilot-bundle.md](docs/customer-pilot-bundle.md) and is
the current handoff path for a local public-safety pilot walkthrough.
The readiness and hashed handoff-manifest path is documented in
[docs/customer-pilot-readiness.md](docs/customer-pilot-readiness.md).
The operator/admin runbooks and the acceptance checklist are documented
in [docs/customer-pilot-runbooks.md](docs/customer-pilot-runbooks.md)
and
[docs/customer-pilot-acceptance-checklist.md](docs/customer-pilot-acceptance-checklist.md).

For the supported Windows-first single-host pilot path over local
PostgreSQL, use the documented bootstrap in
[docs/windows-pilot-bootstrap.md](docs/windows-pilot-bootstrap.md).

If you need a purely static handoff instead, the older static-site path
is still available:

```bash
python scripts/build_public_safety_demo_site.py --bundle dist/public-safety-demo/etl-identity-engine-v<version>-public-safety-demo-small.zip --output-dir dist/public-safety-demo-site --site-title "Hosted Public Safety Identity Demo"
```

For scale validation, `benchmark-run` executes the real persisted
pipeline against a named large-batch fixture from
`config/benchmark_fixtures.yml` and writes benchmark artifacts under
`dist/benchmarks/<fixture>/`.

The benchmark catalog now supports both standard batch fixtures and
continuous-ingest fixtures across two deployment targets:

- `single_host_container` for the compose and SQLite baseline
- `cluster_postgresql_baseline` for the clustered PostgreSQL runtime
  baseline

Event-stream fixtures seed one persisted run through `run-all`, then
drive repeated `stream-refresh` batches and emit a `continuous_ingest`
summary block plus explicit stream SLO metrics alongside the final run
artifacts.

The standalone `golden` stage uses normalized records plus
`data/matches/entity_clusters.csv` unless the input already includes
`cluster_id` values. The standalone `report` stage reads the normalized
artifact plus the current match, cluster, golden, and review-queue
artifacts so its counts match the pipeline state.
`run-all` also accepts `--formats` and will normalize from the generated
CSV outputs when available, or from generated Parquet outputs when CSV
is not part of the requested format set.

## Local Quickstart (Venv-First)

Prerequisite: Python 3.11+ installed and available on `PATH`.
The virtual environment installs Python dependencies and the local
`gh` CLI only. It does not provision shell runtimes such as `bash` or
PowerShell, but the repo provides Python-native `scripts/run_checks.py`
and `scripts/run_pipeline.py` entrypoints when you want shell-free local
validation and pipeline execution.

Maintained CI support currently covers:

- Python `3.11` baseline validation on Linux and Windows
- Python `3.12` compatibility validation on Linux, Windows, and macOS

### Windows (PowerShell)

Use the PowerShell path on Windows. Do not expect the venv to provide a
`bash` runtime.

```powershell
./scripts/bootstrap_venv.ps1
.\.venv\Scripts\Activate.ps1
./scripts/run_checks.ps1
python -m etl_identity_engine.cli generate --profile small --duplicate-rate 0.4 --formats csv,parquet
./scripts/run_pipeline.ps1
```

`run_checks.ps1` now covers the same local validation surface as the
documented CI baseline: package-build verification, `ruff`, `pytest`,
the installed `etl-identity-engine` console entrypoint smoke check, the
active-backlog dry-run, release-sample packaging, and the persisted-
state recovery smoke path. The build and packaging checks use temporary
output directories, so the wrapper does not leave artifacts under
`dist/`.
It also verifies that the installed editable package metadata matches
`pyproject.toml`, so rerun the bootstrap script or
`python -m pip install -e .[dev]` after pulling a version bump.

`run_pipeline.ps1` forwards any additional `run-all` CLI arguments, for
example `./scripts/run_pipeline.ps1 --base-dir tmp --config-dir config`.

### macOS / Linux (bash)

Use the bash path only on systems that already provide `bash`. That path
is validated in Linux CI rather than provisioned by the Python venv.

```bash
chmod +x ./scripts/bootstrap_venv.sh ./scripts/run_checks.sh ./scripts/run_pipeline.sh
./scripts/bootstrap_venv.sh
source .venv/bin/activate
./scripts/run_checks.sh
python -m etl_identity_engine.cli generate --profile small --duplicate-rate 0.4 --formats csv,parquet
./scripts/run_pipeline.sh
```

`run_checks.sh` covers the same local validation surface as the
documented CI baseline: package-build verification, `ruff`, `pytest`,
the installed `etl-identity-engine` console entrypoint smoke check, the
active-backlog dry-run, release-sample packaging, and the persisted-
state recovery smoke path. The build and packaging checks use temporary
output directories, so the wrapper does not leave artifacts under
`dist/`.
It also verifies that the installed editable package metadata matches
`pyproject.toml`, so rerun the bootstrap script or
`python -m pip install -e .[dev]` after pulling a version bump.

`run_pipeline.sh` forwards any additional `run-all` CLI arguments, for
example `./scripts/run_pipeline.sh --base-dir tmp --config-dir config`.

### Direct Commands (Any Platform)

```bash
python -m venv .venv
.venv/bin/python -m pip install --upgrade pip setuptools wheel
.venv/bin/python -m pip install -r requirements-dev.txt
.venv/bin/python scripts/run_checks.py
.venv/bin/python -m ruff check .
.venv/bin/python -m pytest
.venv/bin/python -m etl_identity_engine.cli run-all
```

On Windows, replace `.venv/bin/python` with
`.venv\Scripts\python.exe`.

### Windows Alias Troubleshooting

If `python` resolves to `...WindowsApps\python.exe`, install Python from
`python.org` and disable App execution aliases for `python.exe` and
`python3.exe` in Windows settings.

## GitHub Backlog Automation

Use the planning backlog to create labels, milestones, epics, and issues
via the cross-platform Python automation script:

```powershell
gh auth login
python scripts/create_github_backlog.py --repo "raven-dev-ops/ETL-Identity-Data-Ruleset-Engine" --dry-run
python scripts/create_github_backlog.py --repo "raven-dev-ops/ETL-Identity-Data-Ruleset-Engine"
```

The default backlog source is
`planning/active-github-issues-backlog.md`. The bootstrap backlog at
`planning/github-issues-backlog.md` and the completed
`planning/post-v1.2.0-github-issues-backlog.md`,
`planning/post-v0.1.0-github-issues-backlog.md`,
`planning/post-v0.6.0-github-issues-backlog.md`, and
`planning/post-v0.9.0-github-issues-backlog.md` files are historical
and should be used only when re-syncing closed tracker history with
`--include-closed`:

```powershell
python scripts/create_github_backlog.py --repo "raven-dev-ops/ETL-Identity-Data-Ruleset-Engine" --backlog-path planning/github-issues-backlog.md --include-closed --dry-run
```

By default, the backlog automation ignores catalog entries marked
`Status: closed` so the active backlog file can remain a historical
record after a cycle is complete without recreating closed GitHub work.

When filing new work manually, use the GitHub issue forms for `bug`,
`feature`, `docs`, `chore`, and `epic` so issues stay aligned with the
backlog label taxonomy and milestone structure.

On Windows, `scripts/create_github_backlog.ps1` remains available as a
PowerShell-specific wrapper if you prefer that entrypoint.

The `Issue Metadata` workflow runs after issue-template changes reach
`main` and checks GitHub's default-branch issue metadata plus the pushed
template files via the GitHub APIs. That provides a GitHub-side
verification path without relying on manual browser inspection.

The bootstrap scripts install both Python dependencies and a venv-scoped
GitHub CLI, so local checks do not require a global `gh` installation.
They do not install OS shell runtimes; Windows users should run the
PowerShell entrypoints locally, while Linux CI validates the documented
bash path. The Python-native `scripts/run_checks.py` and
`scripts/run_pipeline.py` entrypoints remain available on every
platform.
The local `run_checks` wrappers are the authoritative pre-push
validation path. They include the local `pytest` suite plus the
active-backlog dry-run, release-sample packaging, and persisted-state
recovery smoke checks, and they use temporary packaging output so
pre-push validation does not leave release artifacts behind. The remote
metadata check validates only what GitHub currently sees on the pushed
default branch.

## License

This project is provided as a technical demonstration and reference
implementation.
