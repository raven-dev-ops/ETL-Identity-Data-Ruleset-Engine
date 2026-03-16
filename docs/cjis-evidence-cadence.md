# CJIS Evidence Cadence

This document defines the repo-side recurring evidence loop for the
CJIS-sensitive deployment baseline.

It packages the current evidence set, tracks whether the latest review
is pending or current or overdue, and records who completed the repo
review step.

It does not replace agency, CSA, or policy-owned review obligations.

## Default Cadence

The shipped cadence is:

- capture a fresh repo-side evidence pack at least every 30 days
- record the review completion for that capture in the cadence index
- treat the latest capture as `overdue` when the recorded due date has
  passed without a review update

The cadence length is configurable with `--cadence-days`, but the repo
default is 30 days.

## Capture

Create a fresh evidence pack and append it to the cadence index:

```powershell
.\.venv\Scripts\python.exe scripts/manage_cjis_evidence_cadence.py capture `
  --environment cjis `
  --runtime-config config\runtime_environments.yml `
  --env-file deploy\cjis.env `
  --output-dir dist\cjis-evidence-review
```

That command:

- runs `scripts/package_cjis_evidence_pack.py`
- writes the evidence zip under `dist\cjis-evidence-review\captures\...`
- updates `cjis_evidence_review_index.json`
- updates `cjis_evidence_review_index.md`

## Review

Record the repo-side review completion for the latest capture:

```powershell
.\.venv\Scripts\python.exe scripts/manage_cjis_evidence_cadence.py review `
  --output-dir dist\cjis-evidence-review `
  --reviewer security.analyst@example.gov
```

To review a specific capture or record a backdated review time, add
`--capture-id` and `--reviewed-at-utc`.

## Status

Render the current cadence state without creating a new pack:

```powershell
.\.venv\Scripts\python.exe scripts/manage_cjis_evidence_cadence.py status `
  --output-dir dist\cjis-evidence-review
```

The status payload reports:

- the latest capture id
- the next review due time
- whether the latest capture is `pending`, `current`, or `overdue`
- any overdue capture ids still recorded in the cadence index

## Artifact Contract

The cadence index stores:

- evidence-pack path and SHA-256
- evidence capture timestamp
- selected run id from the evidence manifest
- due date for the next repo review
- reviewer identity and reviewed-at timestamp when recorded
- repo-side scope boundary text

The latest cadence index is also one of the required inputs to
[production-acceptance-suite.md](production-acceptance-suite.md).

The cadence index is an operational tracking aid. It should not contain
raw agency data or policy-only evidence that belongs outside the repo
workflow.

## Scope Boundary

The repo-side cadence supports:

- repeatable evidence packaging
- review due-date tracking
- overdue visibility for the latest repo evidence capture

It does not, by itself, satisfy:

- agency retention policy
- formal compliance signoff
- incident-management obligations outside the shipped runtime
- non-product evidence collection performed by the operator organization
