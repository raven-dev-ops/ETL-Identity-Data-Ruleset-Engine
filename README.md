# ETL Identity Data Ruleset Engine

ETL Identity Data Ruleset Engine is a prototype framework for improving
data quality and identity consistency in multi-source datasets. The
project demonstrates how an ETL pipeline can normalize inconsistent
records, detect likely duplicate identities through probabilistic
matching, and generate trusted "golden records" using deterministic
survivorship rules.

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
explicit manifest contract.

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
  compatible landing zones are supported; persisted state and service
  workflows remain follow-on work rather than implicit capabilities of
  the current line.
- The supported matching track for the current `0.x` line is
  deterministic and explainable: exact matches plus heuristic partial
  and phonetic signals. ML-assisted scoring is intentionally out of
  scope for the supported public line.
- The supported manual-review operating model for the current `0.x` line
  is a CSV handoff via `data/review_queue/manual_review_queue.csv`.
  Persisted in-app review workflow state is out of scope.

## Future Enhancements

- Configurable identity matching rules
- Additional explainable heuristic signals and richer offline evaluation
- Address standardization using geocoding services
- Streaming ETL support
- Real-time identity resolution pipelines

## Planning Artifacts

- [Project Structure Outline](planning/project-structure-outline.md)
- [Remaining Work Task List](planning/remaining-work-task-list.md)
- [Active GitHub Issues Backlog](planning/active-github-issues-backlog.md)
- [Post-v0.1.0 GitHub Issues Backlog (Historical)](planning/post-v0.1.0-github-issues-backlog.md)
- [Bootstrap GitHub Issues Backlog (Historical)](planning/github-issues-backlog.md)

## Documentation

- [Architecture](docs/architecture.md)
- [Data Model](docs/data-model.md)
- [Normalization](docs/normalization.md)
- [Production Batch Manifest](docs/production-batch-manifest.md)
- [Persistent State](docs/persistent-state.md)
- [Runtime Environments](docs/runtime-environments.md)
- [Matching and Thresholds](docs/matching-and-thresholds.md)
- [Survivorship](docs/survivorship.md)
- [Evaluation and Metrics](docs/evaluation-and-metrics.md)
- [Output Contracts](docs/output-contracts.md)
- [Release Process](docs/release-process.md)
- [Standards Mapping](docs/standards-mapping.md)

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
  `review-queue`, `golden`, `report`, `run-all`
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
- a manual review queue in `data/review_queue/manual_review_queue.csv`
- exception artifacts and summary outputs under `data/exceptions/`

The stable output shapes for those files are documented in
[docs/output-contracts.md](docs/output-contracts.md).

The current matcher remains rules-based, but it now includes exact,
partial, and lightweight phonetic-name signals. Candidate outputs expose
those derived signals explicitly through `matched_fields` and
`reason_trace`.

The current manual-review operating model is a CSV handoff via
`data/review_queue/manual_review_queue.csv`; the project does not yet
implement a persisted review workflow.

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
active-backlog dry-run, and release-sample packaging. The build and
packaging checks use temporary output directories, so the wrapper does
not leave artifacts under `dist/`.
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
active-backlog dry-run, and release-sample packaging. The build and
packaging checks use temporary output directories, so the wrapper does
not leave artifacts under `dist/`.
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
`planning/post-v0.1.0-github-issues-backlog.md` file are historical and
should be used only when re-syncing closed tracker history with
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
active-backlog dry-run and release-sample packaging checks, and they use
temporary packaging output so pre-push validation does not leave release
artifacts behind. The remote metadata check validates only what GitHub
currently sees on the pushed default branch.

## License

This project is provided as a technical demonstration and reference
implementation.
