# Architecture

The current prototype is a staged, file-based pipeline that turns
synthetic or manifest-defined landed multi-source person records into
normalized records, scored candidate pairs, deterministic clusters,
golden records, and reporting artifacts.

## Public Scope Boundaries

- The current runtime supports synthetic generation plus manifest-driven
  local and object-storage-compatible landed batches. Persisted SQLite
  state is now supported; service workflows remain tracked follow-on
  work rather than implicit capabilities of the current line.
- The supported matching engine remains deterministic and explainable:
  exact signals plus heuristic partial and phonetic-name scoring. The
  public `0.x` line does not introduce an ML-assisted scorer.
- The supported manual-review model remains the CSV handoff documented
  below rather than a persisted in-app workflow.

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
     a completed persisted run from SQLite
   - Writes under `data/exceptions/`
8. `run-all`
   - Executes the end-to-end prototype path in one command
   - Either generates synthetic inputs or uses a validated production
     batch manifest
   - Can optionally persist completed run state into SQLite

## Config Surfaces

The current runtime reads these repo config files at startup:

- `config/normalization_rules.yml`
- `config/blocking_rules.yml`
- `config/matching_rules.yml`
- `config/thresholds.yml`
- `config/survivorship_rules.yml`
- `config/runtime_environments.yml`

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

The runtime now supports optional SQLite-backed persistence for:

- run registry metadata
- normalized source rows
- candidate pairs
- blocking metrics
- entity clusters
- golden records
- source-to-golden crosswalk rows
- manual-review queue rows

`run-all --state-db ...` persists a completed run into SQLite, and
`report --state-db ... --run-id ...` can reload that state to reproduce
the reporting slice from the database instead of the filesystem. The
state schema is documented in [persistent-state.md](persistent-state.md).
The registry now records `running`, `completed`, and `failed` attempts,
reuses completed runs idempotently, and treats failed reruns as clean
restart attempts under the same `run_key`.
Schema bootstrap is now managed through Alembic-backed `state-db-upgrade`
and `state-db-current` commands instead of ad hoc table creation.

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

## Manual Review Operating Model

The supported manual-review model for the current `0.1.x` line remains a
file-based CSV handoff:

- `review-queue` writes `data/review_queue/manual_review_queue.csv`
- `queue_status` is initialized to `pending`
- reviewers are expected to consume that artifact outside the runtime
  rather than through a persisted in-app workflow

A persisted review workflow is intentionally out of scope for the
current supported runtime surface. Any future expansion beyond the CSV
handoff should be introduced as a new tracked backlog item instead of as
an implicit behavior change.

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

## Command Example

Run the full prototype pipeline:

```bash
python -m etl_identity_engine.cli run-all
```

For the original design notes that predate the current implementation,
see [pipeline_architecture.md](../pipeline_architecture.md).

