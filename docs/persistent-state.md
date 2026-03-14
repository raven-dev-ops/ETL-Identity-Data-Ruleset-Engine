# Persistent State

The runtime now supports optional SQL-backed persistence for completed
pipeline runs.

Supported `--state-db` targets:

- local SQLite paths such as `data/state/pipeline_state.sqlite`
- PostgreSQL SQLAlchemy URLs such as
  `postgresql://etl_user:secret@db.internal:5432/identity_state`

## Current Scope

Persisted state is currently opt-in on `run-all`:

```bash
python -m etl_identity_engine.cli run-all \
  --base-dir . \
  --state-db data/state/pipeline_state.sqlite
```

When enabled, the runtime writes one completed run record plus the core
pipeline artifacts into the configured state store.

You can then reload a persisted run into the reporting stage:

```bash
python -m etl_identity_engine.cli report \
  --state-db data/state/pipeline_state.sqlite \
  --run-id RUN-20260314T000000Z-ABC12345 \
  --output data/exceptions/run_report.md
```

You can bootstrap or inspect the persisted schema through the runtime
CLI:

```bash
python -m etl_identity_engine.cli state-db-upgrade \
  --state-db data/state/pipeline_state.sqlite

python -m etl_identity_engine.cli state-db-current \
  --state-db data/state/pipeline_state.sqlite
```

You can also publish a stable downstream snapshot directly from the
persisted run:

```bash
python -m etl_identity_engine.cli publish-delivery \
  --state-db data/state/pipeline_state.sqlite \
  --run-id RUN-20260314T000000Z-ABC12345 \
  --output-dir published/delivery
```

Or execute a named downstream export job against that persisted run:

```bash
python -m etl_identity_engine.cli export-job-run \
  --state-db data/state/pipeline_state.sqlite \
  --job-name warehouse_identity_snapshot \
  --run-id RUN-20260314T000000Z-ABC12345
```

You can also serve persisted state through the authenticated operator
API:

```bash
python -m etl_identity_engine.cli serve-api \
  --state-db data/state/pipeline_state.sqlite \
  --host 127.0.0.1 \
  --port 8000
```

Operator CLI wrappers are also available for the persisted workflow:

```bash
python -m etl_identity_engine.cli apply-review-decision \
  --state-db data/state/pipeline_state.sqlite \
  --run-id RUN-20260314T000000Z-ABC12345 \
  --review-id REV-00001 \
  --decision approved \
  --notes "Approved after verification"

python -m etl_identity_engine.cli replay-run \
  --state-db data/state/pipeline_state.sqlite \
  --run-id RUN-20260314T000000Z-ABC12345 \
  --refresh-mode incremental

python -m etl_identity_engine.cli publish-run \
  --state-db data/state/pipeline_state.sqlite \
  --run-id RUN-20260314T000000Z-ABC12345 \
  --output-dir published/delivery
```

## Tables

The current schema includes:

- `pipeline_runs`
- `normalized_source_records`
- `candidate_pairs`
- `blocking_metrics`
- `entity_clusters`
- `golden_records`
- `source_to_golden_crosswalk`
- `review_cases`
- `export_job_runs`
- `audit_events`

## Run Registry

`pipeline_runs` stores:

- `run_id`
- `run_key`
- `attempt_number`
- `batch_id`
- `input_mode`
- `manifest_path`
- `base_dir`
- `config_dir`
- `profile`
- `seed`
- `formats`
- `status`
- `started_at_utc`
- `finished_at_utc`
- `total_records`
- `candidate_pair_count`
- `cluster_count`
- `golden_record_count`
- `review_queue_count`
- `failure_detail`
- `summary_json`

## Lifecycle Semantics

The run registry now uses these statuses:

- `running`
- `completed`
- `failed`

When `run-all --state-db ...` starts, it registers a `running` attempt
before the main pipeline work begins. On success, that attempt is
updated to `completed`. On failure, the attempt is updated to `failed`
with the operator-readable error text in `failure_detail`.

## Idempotent Replay Model

Completed runs are deduplicated by a stable `run_key` derived from the
input mode plus the batch-or-config identity for the invocation.

If `run-all` is called again with the same persisted run inputs and a
completed run already exists:

- the runtime does not create a second completed run row
- persisted artifact rows are not duplicated
- file artifacts are restored from persisted state into the requested
  `base_dir`

## Failed-Run Restart Model

The current restart model is a clean restart, not an in-place resume.

If the latest attempt for a given `run_key` failed:

- rerunning the same persisted invocation starts a new attempt
- the failed attempt remains in the registry for auditability
- the next successful attempt completes under the same `run_key` with a
  higher `attempt_number`

## Incremental Refresh Model

Manifest-driven persisted runs now support:

```bash
python -m etl_identity_engine.cli run-all \
  --base-dir . \
  --manifest manifest.yml \
  --state-db data/state/pipeline_state.sqlite \
  --refresh-mode incremental
```

The current incremental prototype uses this operator model:

- lineage is determined by the manifest path plus the runtime config path
- batch identity comes from the manifest `batch_id`
- the latest completed predecessor in that lineage is reused when the
  current config fingerprint matches the predecessor
- only affected candidate pairs, clusters, and golden records are
  recalculated
- unaffected persisted entities are carried forward from the prior
  completed run

If no compatible predecessor exists, the runtime falls back to a full
rebuild and records that fallback in `run_summary.json`.

## Artifact Storage

Each artifact table stores:

- `run_id`
- `row_index`
- the stable contract columns for that artifact

`row_index` preserves deterministic output order so persisted rows can be
reloaded in the same sequence as the file artifacts.

## Review Case Workflow

Persisted `review_cases` now track more than the initial queue CSV
fields. The current lifecycle fields are:

- `queue_status`
- `assigned_to`
- `operator_notes`
- `created_at_utc`
- `updated_at_utc`
- `resolved_at_utc`

The runtime currently exposes that workflow through:

```bash
python -m etl_identity_engine.cli review-case-list \
  --state-db data/state/pipeline_state.sqlite \
  --run-id RUN-20260314T000000Z-ABC12345

python -m etl_identity_engine.cli review-case-update \
  --state-db data/state/pipeline_state.sqlite \
  --run-id RUN-20260314T000000Z-ABC12345 \
  --review-id REV-00001 \
  --status approved \
  --assigned-to analyst.one \
  --notes "Approved after verification"
```

The lifecycle contract itself is documented in
[review-workflow.md](review-workflow.md).

Approved and rejected review decisions now affect later persisted reruns
for the same manifest lineage:

- `approved` forces the reviewed pair to merge before clustering
- `rejected` blocks the reviewed pair from merging even if the heuristic
  scorer would otherwise auto-merge it
- the resulting cluster and golden rebuilds are then persisted as the
  next completed run state

## Delivery Publication

Completed persisted runs can now be published under the versioned
`golden_crosswalk_snapshot/v1` contract.

The publish path:

- reads golden and crosswalk rows from the persisted state store rather
  than the working
  directory
- writes an immutable snapshot directory for the selected `run_id`
- updates `current.json` atomically so downstream consumers can follow a
  stable pointer

The consumer-facing contract is documented in
[delivery-contracts.md](delivery-contracts.md).

## Export Job Audit

Named downstream export jobs now record auditable execution state in the
`export_job_runs` table.

The export registry uses these statuses:

- `running`
- `completed`
- `failed`

Each export run stores:

- `export_run_id`
- `export_key`
- `attempt_number`
- `job_name`
- `source_run_id`
- `contract_name`
- `contract_version`
- `output_root`
- `status`
- `started_at_utc`
- `finished_at_utc`
- `snapshot_dir`
- `current_pointer_path`
- `row_counts_json`
- `metadata_json`
- `failure_detail`

Completed exports are deduplicated by a stable `export_key`, so
re-running the same named export against the same completed run reuses
the prior completed export record instead of writing duplicate audit
rows. The operator surface for this workflow is:

```bash
python -m etl_identity_engine.cli export-job-list

python -m etl_identity_engine.cli export-job-run \
  --state-db data/state/pipeline_state.sqlite \
  --job-name warehouse_identity_snapshot

python -m etl_identity_engine.cli export-job-history \
  --state-db data/state/pipeline_state.sqlite \
  --job-name warehouse_identity_snapshot
```

The configured job catalog and downstream locations are documented in
[export-jobs.md](export-jobs.md).

## Audit Events

Operator-sensitive actions now persist into the `audit_events` table.

The current audited actions include:

- review-decision application
- manifest-backed replay
- direct delivery publication
- JSON `publish-run` publication
- named export-job execution

Each audit row stores:

- `audit_event_id`
- `occurred_at_utc`
- `actor_type`
- `actor_id`
- `action`
- `resource_type`
- `resource_id`
- `run_id`
- `status`
- `details_json`

## Service Access

The persisted store now also supports an authenticated service surface
for:

- run status lookup
- golden-record lookup
- source-to-golden crosswalk lookup
- review-case retrieval

It also now exposes authenticated:

- `GET /healthz`
- `GET /readyz`
- `GET /api/v1/metrics`

That API contract is documented in [service-api.md](service-api.md).
The shared logging, metrics, health, and audit-event baseline is
documented in
[operations-observability.md](operations-observability.md).

## Recovery Model

The supported recovery model is now documented in
[recovery-runbooks.md](recovery-runbooks.md).

Operators should treat the minimum recoverable backup set for
manifest-driven persisted runs as:

- the persisted state store
- the manifest file referenced by the completed run
- the landed input snapshot referenced by that manifest
- any custom runtime config snapshot used for that run

That distinction is important because:

- `report`, `publish-run`, and downstream export jobs can rebuild from a
  restored persisted state store alone
- `replay-run` requires the stored `manifest_path` plus the landed input
  snapshot to exist again

## Current Boundary

This issue adds durable relational persistence, a basic run registry,
first-class schema migrations, a first incremental refresh path, a
persisted review-case lifecycle, a versioned downstream publication
contract, and auditable named export-job execution, not full
orchestration.
The current line does not yet provide:

- persisted failure-state resume from mid-pipeline checkpoints
- immutable source-data replay independent of the current manifest and
  landing-zone contents

Recovery procedures for the current supported model are now documented
in [recovery-runbooks.md](recovery-runbooks.md). The remaining boundary
above stays in effect until the runtime can replay independently of the
stored manifest path and landing snapshot.
