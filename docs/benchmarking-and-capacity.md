# Benchmarking and Capacity

The repo now ships named large-batch benchmark fixtures in
`config/benchmark_fixtures.yml` and a first-class `benchmark-run`
command:

```bash
python -m etl_identity_engine.cli benchmark-run --fixture scale_medium
```

The benchmark runner executes the real persisted `run-all` path, then
evaluates the resulting `run_summary.json` performance block against the
configured capacity target for a supported deployment profile.
For `mode: event_stream` fixtures, it seeds one persisted synthetic run,
then executes multiple `stream-refresh` batches and emits a
`continuous_ingest` block in `benchmark_summary.json`.

## Supported Deployment Target

The current benchmark target name is `single_host_container`. It is the
same deployment baseline documented in
[container-deployment.md](container-deployment.md):

- one shared batch/service image
- one SQLite state store
- one single-host compose topology
- local or CI execution on one machine, not a distributed cluster

These targets are regression guards for the supported baseline. They are
not external latency guarantees for arbitrary hardware or multi-tenant
production traffic.

## Named Fixtures

- `scale_medium`
  - `profile`: `large`
  - `person_count`: `2400`
  - expected normalized source rows: about `4800`
  - target for `single_host_container`:
    - `max_total_duration_seconds <= 5.0`
    - `min_normalize_records_per_second >= 10000.0`
    - `min_match_candidate_pairs_per_second >= 20000.0`
- `scale_large`
  - `profile`: `large`
  - `person_count`: `9600`
  - expected normalized source rows: about `19200`
  - target for `single_host_container`:
    - `max_total_duration_seconds <= 30.0`
    - `min_normalize_records_per_second >= 10000.0`
    - `min_match_candidate_pairs_per_second >= 20000.0`
- `continuous_ingest_small`
  - `mode`: `event_stream`
  - `profile`: `small`
  - seed `person_count`: `96`
  - `stream_batch_count`: `3`
  - `stream_events_per_batch`: `12`
  - target for `single_host_container`:
    - `max_total_duration_seconds <= 10.0`
    - `min_normalize_records_per_second >= 1000.0`
    - `min_match_candidate_pairs_per_second >= 1000.0`

Reference maintainer measurements on 2026-03-14 were comfortably above
those thresholds:

- `scale_medium`
  - total duration: about `1.23s`
  - normalize throughput: about `66k records/s`
  - match throughput: about `80k candidate pairs/s`
- `scale_large`
  - total duration: about `17.77s`
  - normalize throughput: about `71k records/s`
  - match throughput: about `79k candidate pairs/s`

Those reference numbers are descriptive only. The committed targets are
lower on purpose so they catch major regressions without overfitting to
one workstation.

## Benchmark Artifacts

By default, benchmark output is written under `dist/benchmarks/`:

- `dist/benchmarks/<fixture>/benchmark_summary.json`
- `dist/benchmarks/<fixture>/benchmark_report.md`
- `dist/benchmarks/<fixture>/run_artifacts/` for batch fixtures
- `dist/benchmarks/<fixture>/seed_run/` for event-stream fixtures
- `dist/benchmarks/<fixture>/stream_runs/` for event-stream fixtures
- `dist/benchmarks/<fixture>/state/pipeline_state.sqlite`

`benchmark_summary.json` includes:

- fixture definition
- deployment target name
- nested `run_summary` from the real pipeline run
- capacity assertion results
- paths to the persisted benchmark artifacts

The nested `run_summary.performance` block captures phase timing and
throughput for:

- `generate`
- `normalize`
- `match`
- `cluster`
- `review_queue`
- `golden`
- `crosswalk`
- `report`
- `persist_state`

For event-stream fixtures, `benchmark_summary.json` also includes
`continuous_ingest` with:

- total stream batch count
- total event count
- aggregate stream duration
- aggregate events-per-second
- the last stream run summary path

## Common Commands

Run the medium regression benchmark and enforce its targets:

```bash
python -m etl_identity_engine.cli benchmark-run --fixture scale_medium
```

Run the shipped continuous-ingest benchmark:

```bash
python -m etl_identity_engine.cli benchmark-run --fixture continuous_ingest_small
```

Run the large benchmark but keep the artifacts even if you only want the
measurements and not a pass/fail gate:

```bash
python -m etl_identity_engine.cli benchmark-run \
  --fixture scale_large \
  --no-enforce-targets
```

Write artifacts to a custom location:

```bash
python -m etl_identity_engine.cli benchmark-run \
  --fixture scale_medium \
  --output-dir tmp/benchmarks
```
