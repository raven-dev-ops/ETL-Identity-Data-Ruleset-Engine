# Persistent State

The runtime now supports optional SQLite-backed persistence for
completed pipeline runs.

## Current Scope

Persisted state is currently opt-in on `run-all`:

```bash
python -m etl_identity_engine.cli run-all \
  --base-dir . \
  --state-db data/state/pipeline_state.sqlite
```

When enabled, the runtime writes one completed run record plus the core
pipeline artifacts into SQLite.

You can then reload a persisted run into the reporting stage:

```bash
python -m etl_identity_engine.cli report \
  --state-db data/state/pipeline_state.sqlite \
  --run-id RUN-20260314T000000Z-ABC12345 \
  --output data/exceptions/run_report.md
```

You can bootstrap or inspect the SQLite schema through the runtime CLI:

```bash
python -m etl_identity_engine.cli state-db-upgrade \
  --state-db data/state/pipeline_state.sqlite

python -m etl_identity_engine.cli state-db-current \
  --state-db data/state/pipeline_state.sqlite
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

## Artifact Storage

Each artifact table stores:

- `run_id`
- `row_index`
- the stable contract columns for that artifact

`row_index` preserves deterministic output order so persisted rows can be
reloaded in the same sequence as the file artifacts.

## Current Boundary

This issue adds durable relational persistence, a basic run registry,
and first-class schema migrations, not full orchestration.
The current line does not yet provide:

- persisted failure-state resume from mid-pipeline checkpoints
- service APIs over the persisted store

Those remain tracked follow-on work in the active backlog.
