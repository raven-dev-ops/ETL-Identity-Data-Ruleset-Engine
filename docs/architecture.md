# Architecture

The current prototype is a staged, file-based pipeline that turns
synthetic or manifest-defined landed multi-source person records into
normalized records, scored candidate pairs, deterministic clusters,
golden records, and reporting artifacts.

## Public Scope Boundaries

- The current runtime supports synthetic generation plus manifest-driven
  local and object-storage-compatible landed batches. Persisted SQL
  state is now supported, and an authenticated operator service API now
  exposes persisted run, golden, crosswalk, and review-case lookups
  plus operator-only review-decision and replay actions. A container
  image, a single-host compose deployment baseline, and named benchmark
  fixtures with capacity targets are now available. Publication and
  export orchestration remain CLI-driven follow-on service work.
- The supported matching engine remains deterministic and explainable:
  exact signals plus heuristic partial and phonetic-name scoring. The
  public `0.x` line does not introduce an ML-assisted scorer.
- The supported manual-review model now includes persisted review-case
  state when persisted-state support is enabled. The CSV queue remains the
  portable file artifact, and approved or rejected review decisions now
  apply deterministically to later cluster and golden rebuilds.

## Runtime Stages

1. `generate`
   - Writes synthetic source datasets under `data/synthetic_sources/`
   - Supports CSV and Parquet outputs
2. `normalize`
   - Reads discovered synthetic inputs, explicit source files, or a
     validated production batch manifest
   - Writes `data/normalized/normalized_person_records.csv`
3. `match`
   - Reads normalized records
   - Writes `data/matches/candidate_scores.csv`
   - Writes `data/matches/blocking_metrics.csv`
4. `cluster`
   - Reads normalized records plus candidate scores
   - Writes `data/matches/entity_clusters.csv`
5. `review-queue`
   - Reads candidate scores
   - Writes `data/review_queue/manual_review_queue.csv`
6. `golden`
   - Builds golden records from normalized records plus cluster
     assignments, or from already-clustered full rows
   - Writes `data/golden/golden_person_records.csv`
7. `report`
   - Builds markdown and JSON quality summaries from normalized records
     plus match, cluster, golden, and review-queue artifacts, or reloads
     a completed persisted run from the configured state store
   - Writes under `data/exceptions/`
8. `run-all`
   - Executes the end-to-end prototype path in one command
   - Either generates synthetic inputs or uses a validated production
     batch manifest
   - Can optionally persist completed run state into the configured
     state store
   - Supports manifest-driven incremental refresh when paired with
     `--state-db --refresh-mode incremental`
9. `publish-delivery`
   - Reads a completed persisted run from the configured state store
   - Publishes immutable downstream snapshots for golden records and the
     source-to-golden crosswalk
   - Updates an atomic `current.json` consumer pointer under the
     versioned delivery-contract root
10. `serve-api`
   - Reads persisted SQL state through a local HTTP service
   - Exposes authenticated run status, golden-record lookup,
     source-to-golden crosswalk lookup, and review-case retrieval
   - Supports operator-only review decision and replay actions
   - Exposes authenticated `healthz`, `readyz`, and `/api/v1/metrics`
     endpoints for service and batch observability
11. `export-job-run`
   - Reads a completed persisted run from the configured state store
   - Materializes a named warehouse or data-product export under the
     configured output root
   - Records auditable export execution and reuse in the state store
12. `benchmark-run`
   - Executes the real persisted `run-all` path against a named
     large-batch fixture
   - Captures phase timing and throughput metrics from the resulting run
     summary
   - Evaluates the run against a named deployment target such as
     `single_host_container`

## Config Surfaces

The current runtime reads these repo config files at startup:

- `config/normalization_rules.yml`
- `config/blocking_rules.yml`
- `config/matching_rules.yml`
- `config/thresholds.yml`
- `config/survivorship_rules.yml`
- `config/runtime_environments.yml`
- `config/export_jobs.yml`
- `config/benchmark_fixtures.yml`

Named runtime environments can now layer per-environment overrides from
`config/environments/<environment>/` and resolve secret-backed values
from `${ENV_VAR}` placeholders. The CLI can consume those defaults
through `--environment` and `--runtime-config` rather than requiring
operators to edit committed YAML in place.

Runtime config now fails fast when required sections, supported fields,
or threshold semantics are invalid.

The production batch manifest contract is documented separately in
[production-batch-manifest.md](production-batch-manifest.md).

## Persistent State

The runtime now supports optional SQL-backed persistence for:

- run registry metadata
- normalized source rows
- candidate pairs
- blocking metrics
- entity clusters
- golden records
- source-to-golden crosswalk rows
- manual-review queue rows
- audit events

`run-all --state-db ...` persists a completed run into the configured
state store, and
`report --state-db ... --run-id ...` can reload that state to reproduce
the reporting slice from the database instead of the filesystem. The
state schema is documented in [persistent-state.md](persistent-state.md).
The registry now records `running`, `completed`, and `failed` attempts,
reuses completed runs idempotently, and treats failed reruns as clean
restart attempts under the same `run_key`.
Schema bootstrap is now managed through Alembic-backed `state-db-upgrade`
and `state-db-current` commands instead of ad hoc table creation.
For manifest-driven runs, `run-all --state-db ... --refresh-mode incremental`
can reuse the latest completed predecessor from the same manifest
lineage, recalculate only the affected candidate pairs and clusters, and
carry forward unaffected persisted entities unchanged. If the current
configuration fingerprint differs from the predecessor, the runtime
falls back to a full rebuild and records that decision in the run
summary.
`publish-delivery --state-db ...` can then materialize a versioned
golden/crosswalk snapshot from any completed persisted run without
needing the original working-directory files.
`export-job-run --state-db ... --job-name ...` can then materialize the
same delivery contract through named warehouse or data-product export
jobs while recording auditable export-run metadata in the state store.
`serve-api --state-db ...` can expose the same persisted state through
an authenticated operator API for local and CI integration testing, and
the shared observability baseline now also persists privileged audit
events while exposing health and metrics endpoints over the service
surface.
`benchmark-run` now reuses that same persisted runtime path and the
`run_summary.json` performance block to capture concrete phase latency
and throughput metrics on named large-batch fixtures.

## Output Layout

The current end-to-end path writes:

- `data/synthetic_sources/`
- `data/normalized/normalized_person_records.csv`
- `data/matches/candidate_scores.csv`
- `data/matches/blocking_metrics.csv`
- `data/matches/entity_clusters.csv`
- `data/golden/golden_person_records.csv`
- `data/golden/source_to_golden_crosswalk.csv`
- `data/review_queue/manual_review_queue.csv`
- `data/exceptions/run_report.md`
- `data/exceptions/run_summary.json`

Named benchmark runs additionally write:

- `dist/benchmarks/<fixture>/benchmark_report.md`
- `dist/benchmarks/<fixture>/benchmark_summary.json`
- `dist/benchmarks/<fixture>/run_artifacts/`

## Manual Review Operating Model

The current manual-review model now has two surfaces:

- `review-queue` writes `data/review_queue/manual_review_queue.csv`
- fresh queue rows still initialize `queue_status` to `pending`
- when persisted state is enabled, the same cases are also stored in
  the state store with lifecycle status, assignee, timestamps, and
  notes
- operators can inspect and update persisted cases through
  `review-case-list` and `review-case-update`
- service consumers can retrieve persisted cases through `serve-api`
- authenticated operator clients can apply review decisions and replay
  manifest-backed runs through `serve-api`
- approved review decisions force future merge outcomes and rejected
  review decisions block future merges on persisted reruns of the same
  manifest lineage
- operators can apply those decisions through `apply-review-decision`,
  replay manifest-backed runs through `replay-run`, and trigger
  downstream publication through `publish-run`
- operators can list configured downstream export jobs, execute a named
  export, and inspect export history through `export-job-list`,
  `export-job-run`, and `export-job-history`

## Support Matrix

The current maintained support matrix is:

- Python `3.11` and `3.12`
- Linux and Windows validation in the main CI baseline
- additional macOS compatibility validation through a Python `3.12`
  smoke job

The repository still ships shell wrappers for PowerShell and bash, but
Python-native `scripts/run_checks.py` and `scripts/run_pipeline.py`
entrypoints are also supported so local validation and pipeline runs do
not depend on shell runtime provisioning.

The rollout, rollback, governance, and support boundaries for that
supported deployment line are documented in
[production-operating-model.md](production-operating-model.md).

The current deployment baseline also includes:

- one shared batch and service container image
- a single-host compose topology under `deploy/`
- CI smoke validation that the containerized CLI and service start
  successfully
- named benchmark fixtures and capacity targets for the supported
  single-host container baseline

## Command Example

Run the full prototype pipeline:

```bash
python -m etl_identity_engine.cli run-all
```

For the original design notes that predate the current implementation,
see [pipeline_architecture.md](../pipeline_architecture.md).

