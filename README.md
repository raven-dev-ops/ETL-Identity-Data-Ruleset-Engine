# ETL Identity Data Ruleset Engine

ETL Identity Data Ruleset Engine is a prototype framework for improving
data quality and identity consistency in multi-source datasets. The
project demonstrates how an ETL pipeline can normalize inconsistent
records, detect likely duplicate identities through probabilistic
matching, and generate trusted **"golden records"** using deterministic
survivorship rules.

Many operational systems ingest identity data from multiple sources that
store information with inconsistent formatting, partial identifiers, and
duplicate records. These inconsistencies propagate downstream into
reporting systems, analytics environments, and search interfaces,
reducing trust in the data.

This repository models those challenges using **synthetic datasets** and
provides a reproducible ETL pipeline that addresses them.

Because the project uses only synthetic data, it allows teams to explore
identity-resolution techniques safely without exposing operational or
sensitive records.

------------------------------------------------------------------------

## Core Concepts

### Normalization

Standardizes inconsistent fields into canonical formats before
transformation and loading.

Examples: - Converting "Smith, John" and "John A Smith" into a
normalized name format - Standardizing date formats - Normalizing
address structures - Removing punctuation and formatting inconsistencies

### Probabilistic Identity Matching

Evaluates potential duplicate records using weighted signals rather than
exact matches.

Signals may include: - Name similarity - Date-of-birth match - Address
overlap - Phone or identifier similarity - Phonetic matching

Each potential match receives a **confidence score** indicating the
likelihood that two records represent the same person.

### Golden Record Generation

When duplicates are detected, **survivorship rules** determine which
attributes become the authoritative record.

Examples of survivorship rules: - Prefer full legal names over
abbreviated names - Prefer verified identifiers over inferred values -
Prefer the most recent address - Preserve source attribution for
traceability

The result is a consolidated **trusted identity record**.

------------------------------------------------------------------------

## Goals

-   Demonstrate identity resolution within an ETL pipeline
-   Improve data trust in downstream analytics and reporting
-   Provide a transparent rules-based approach to duplicate detection
-   Create a modular architecture adaptable to different data
    environments
-   Provide a synthetic environment for safe experimentation

------------------------------------------------------------------------

## High-Level Pipeline

Source Data â†’ Normalization Layer â†’ Duplicate Candidate Generation â†’
Probabilistic Matching Engine â†’ Survivorship Rules Engine â†’ Golden
Record Output â†’ Analytics / Reporting Data

------------------------------------------------------------------------

## Example Identity Conflict

Synthetic input records:

  ID   Name           DOB          Address
  ---- -------------- ------------ -----------------
  1    John A Smith   1985-03-12   123 Main St
  2    Smith, John    1985-03-12   123 Main Street
  3    Jon Smith      1985-03-12   123 Main St

Matching engine detects high confidence match and generates:

**Golden Record**

  Canonical Name   DOB          Address
  ---------------- ------------ -------------
  John A Smith     1985-03-12   123 Main St

------------------------------------------------------------------------

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

Detailed structure and sequencing are tracked in:

-   [planning/project-structure-outline.md](planning/project-structure-outline.md)

------------------------------------------------------------------------

## Intended Use

This project is designed as a **reference implementation for identity
resolution within ETL workflows**. It is useful for teams dealing with
fragmented identity data across multiple operational systems and looking
to improve:

-   data quality
-   duplicate detection
-   analytics reliability
-   reporting accuracy

------------------------------------------------------------------------

## Data Safety

All datasets used in this project are **synthetic and generated for
demonstration purposes only**. No operational, personal, or sensitive
data is included in the repository.

------------------------------------------------------------------------

## Future Enhancements

-   Configurable identity matching rules
-   Machine-learning assisted duplicate scoring
-   Address standardization using geocoding services
-   Streaming ETL support
-   Real-time identity resolution pipelines

------------------------------------------------------------------------

## Planning Artifacts

-   [Project Structure Outline](planning/project-structure-outline.md)
-   [GitHub Issues Backlog](planning/github-issues-backlog.md)

------------------------------------------------------------------------

## Bootstrap Status (M1)

This repository now includes a working `M1` scaffold:

-   Python package skeleton under `src/etl_identity_engine/`
-   stage CLI commands: `generate`, `normalize`, `match`, `golden`,
    `report`, `run-all`
-   base test suite under `tests/`
-   CI and issue templates under `.github/`
-   governance files: `LICENSE`, `CONTRIBUTING.md`, `SECURITY.md`,
    `CODE_OF_CONDUCT.md`, `SAFETY.md`

## Synthetic Generator Status (M2)

`generate` now produces deterministic multi-table synthetic outputs with
configurable conflict injection:

-   `person_source_a.*`
-   `person_source_b.*`
-   `conflict_annotations.*`
-   `incident_records.*`
-   `incident_person_links.*`
-   `address_history.*`
-   `generation_summary.json`

Formats are configurable (`csv`, `parquet`) and default to both.

### Local Quickstart (Venv-First)

Prerequisite: Python 3.11+ installed and available on PATH.

#### Windows (PowerShell)

```powershell
./scripts/bootstrap_venv.ps1
.\.venv\Scripts\Activate.ps1
./scripts/run_checks.ps1
python -m etl_identity_engine.cli generate --profile small --duplicate-rate 0.4 --formats csv,parquet
./scripts/run_pipeline.ps1
```

#### macOS / Linux (bash)

```bash
chmod +x ./scripts/bootstrap_venv.sh ./scripts/run_checks.sh ./scripts/run_pipeline.sh
./scripts/bootstrap_venv.sh
source .venv/bin/activate
./scripts/run_checks.sh
python -m etl_identity_engine.cli generate --profile small --duplicate-rate 0.4 --formats csv,parquet
./scripts/run_pipeline.sh
```

#### Direct Commands (Any Platform)

```bash
python -m venv .venv
```

```bash
.venv/bin/python -m pip install --upgrade pip setuptools wheel
.venv/bin/python -m pip install -r requirements-dev.txt
.venv/bin/python -m ruff check .
.venv/bin/python -m pytest
.venv/bin/python -m etl_identity_engine.cli run-all
```

On Windows, replace `.venv/bin/python` with `.venv\Scripts\python.exe`.

#### Windows Alias Troubleshooting

If `python` resolves to `...WindowsApps\python.exe`, install Python from
`python.org` and disable App execution aliases for `python.exe` and
`python3.exe` in Windows settings.

### GitHub Backlog Automation

Use the planning backlog to create labels, milestones, epics, and issues
via the cross-platform Python automation script:

```powershell
gh auth login
```

```powershell
python scripts/create_github_backlog.py --repo "raven-dev-ops/ETL-Identity-Data-Ruleset-Engine" --dry-run
python scripts/create_github_backlog.py --repo "raven-dev-ops/ETL-Identity-Data-Ruleset-Engine"
```

When filing new work manually, use the GitHub issue forms for `bug`,
`feature`, `docs`, `chore`, and `epic` so issues stay aligned with the
backlog label taxonomy and milestone structure.

On Windows, `scripts/create_github_backlog.ps1` remains available as a
PowerShell-specific wrapper if you prefer that entrypoint.

The `Issue Metadata` workflow runs after issue-template changes reach
`main` and checks GitHub's default-branch issue metadata and pushed
template files via the GitHub APIs. That provides a GitHub-side
verification path without relying on manual browser inspection.

The bootstrap scripts install both Python dependencies and a venv-scoped
GitHub CLI, so local checks do not require a global `gh` installation.
The local `pytest` suite is the authoritative pre-push validation path
for your unpushed issue-template files. The remote metadata check
validates only what GitHub currently sees on the pushed default branch.

------------------------------------------------------------------------

## License

This project is provided as a technical demonstration and reference
implementation.

