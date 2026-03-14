# Benchmarking and Capacity

The repo ships named benchmark fixtures in
`config/benchmark_fixtures.yml` and a first-class `benchmark-run`
command:

```bash
python -m etl_identity_engine.cli benchmark-run --fixture scale_medium
```

The benchmark runner executes the real persisted runtime, writes normal
pipeline artifacts, and evaluates the resulting `run_summary.json`
against the selected deployment target for that fixture.

For `mode: event_stream` fixtures, it seeds one persisted synthetic run,
then executes multiple `stream-refresh` batches and emits both a
`continuous_ingest` block and explicit `slo_metrics` in
`benchmark_summary.json`.

## Supported Deployment Targets

### `single_host_container`

This is the single-host container baseline documented in
[container-deployment.md](container-deployment.md):

- one shared batch/service image
- one SQLite state store
- one compose topology
- local or CI execution on one machine

### `cluster_postgresql_baseline`

This is the supported clustered runtime baseline documented in
[kubernetes-deployment.md](kubernetes-deployment.md):

- the dedicated `cluster` runtime environment
- PostgreSQL-backed persisted state
- the same batch and event-stream runtime semantics as the shipped
  clustered deployment line

When this target is selected and `--state-db` is not provided,
`benchmark-run` provisions a temporary local PostgreSQL container so the
benchmark reflects the clustered persistence path instead of a relabeled
SQLite run.

The benchmark still runs on one benchmark host. It does not include
Kubernetes scheduler latency, ingress latency, or multi-tenant service
traffic. These are repo-managed regression SLOs for the supported
runtime baseline, not external SLAs.

## Named Fixtures

### Single-host batch fixtures

- `scale_medium`
  - `profile`: `large`
  - `person_count`: `2400`
  - target: `single_host_container`
  - thresholds:
    - `max_total_duration_seconds <= 5.0`
    - `min_normalize_records_per_second >= 10000.0`
    - `min_match_candidate_pairs_per_second >= 20000.0`
- `scale_large`
  - `profile`: `large`
  - `person_count`: `9600`
  - target: `single_host_container`
  - thresholds:
    - `max_total_duration_seconds <= 30.0`
    - `min_normalize_records_per_second >= 10000.0`
    - `min_match_candidate_pairs_per_second >= 20000.0`

### Single-host continuous-ingest fixture

- `continuous_ingest_small`
  - `mode`: `event_stream`
  - `profile`: `small`
  - seed `person_count`: `96`
  - `stream_batch_count`: `3`
  - `stream_events_per_batch`: `12`
  - target: `single_host_container`
  - thresholds:
    - `max_total_duration_seconds <= 10.0`
    - `min_normalize_records_per_second >= 1000.0`
    - `min_match_candidate_pairs_per_second >= 1000.0`

### Clustered runtime fixtures

- `cluster_batch_medium`
  - `profile`: `large`
  - `person_count`: `2400`
  - target: `cluster_postgresql_baseline`
  - thresholds:
    - `max_total_duration_seconds <= 15.0`
    - `min_normalize_records_per_second >= 5000.0`
    - `min_match_candidate_pairs_per_second >= 10000.0`
- `cluster_continuous_ingest_small`
  - `mode`: `event_stream`
  - `profile`: `small`
  - seed `person_count`: `96`
  - `stream_batch_count`: `3`
  - `stream_events_per_batch`: `12`
  - target: `cluster_postgresql_baseline`
  - thresholds:
    - `max_total_duration_seconds <= 15.0`
    - `min_normalize_records_per_second >= 500.0`
    - `min_match_candidate_pairs_per_second >= 500.0`
    - `max_stream_batch_duration_seconds <= 6.0`
    - `max_p95_stream_batch_duration_seconds <= 6.0`
    - `min_stream_events_per_second >= 20.0`

## Benchmark Artifacts

By default, benchmark output is written under `dist/benchmarks/`:

- `dist/benchmarks/<fixture>/benchmark_summary.json`
- `dist/benchmarks/<fixture>/benchmark_report.md`
- `dist/benchmarks/<fixture>/run_artifacts/` for batch fixtures
- `dist/benchmarks/<fixture>/seed_run/` for event-stream fixtures
- `dist/benchmarks/<fixture>/stream_runs/` for event-stream fixtures
- `dist/benchmarks/<fixture>/state/` when a local SQLite state store is
  used

`benchmark_summary.json` includes:

- fixture definition
- deployment target name
- `deployment_profile` with runtime environment, state-store backend,
  display-safe state-store reference, and provisioning mode
- nested `run_summary` from the real pipeline run
- `slo_metrics` with explicit latency and throughput values
- capacity assertion results
- paths to the persisted benchmark artifacts

The `slo_metrics` block always includes:

- `latency.end_to_end_duration_seconds`
- `latency.normalize_duration_seconds`
- `latency.match_duration_seconds`
- `throughput.normalize_records_per_second`
- `throughput.match_candidate_pairs_per_second`

For event-stream fixtures, `slo_metrics.continuous_ingest` also
includes:

- `batch_count`
- `total_event_count`
- `events_per_second`
- `max_batch_duration_seconds`
- `p95_batch_duration_seconds`

The raw nested `run_summary.performance.phase_metrics` block still
captures per-phase timing and throughput for:

- `generate`
- `normalize`
- `match`
- `cluster`
- `review_queue`
- `golden`
- `crosswalk`
- `report`
- `persist_state`

## Common Commands

Run the medium single-host regression benchmark:

```bash
python -m etl_identity_engine.cli benchmark-run --fixture scale_medium
```

Run the shipped continuous-ingest single-host benchmark:

```bash
python -m etl_identity_engine.cli benchmark-run --fixture continuous_ingest_small
```

Run the clustered batch benchmark:

```bash
python -m etl_identity_engine.cli benchmark-run \
  --fixture cluster_batch_medium \
  --deployment-target cluster_postgresql_baseline
```

Run the clustered continuous-ingest benchmark:

```bash
python -m etl_identity_engine.cli benchmark-run \
  --fixture cluster_continuous_ingest_small \
  --deployment-target cluster_postgresql_baseline
```

If you want to benchmark against an already-managed PostgreSQL instance
instead of the temporary local container, pass an explicit DSN:

```bash
python -m etl_identity_engine.cli benchmark-run \
  --fixture cluster_batch_medium \
  --deployment-target cluster_postgresql_baseline \
  --state-db "postgresql://etl_identity:password@db.internal:5432/identity_state"
```
