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
- `summary_json`

## Artifact Storage

Each artifact table stores:

- `run_id`
- `row_index`
- the stable contract columns for that artifact

`row_index` preserves deterministic output order so persisted rows can be
reloaded in the same sequence as the file artifacts.

## Current Boundary

This issue adds durable relational persistence, not full orchestration.
The current line does not yet provide:

- idempotent replay semantics
- persisted failure-state recovery
- migration tooling beyond schema bootstrap
- service APIs over the persisted store

Those remain tracked follow-on work in the active backlog.
