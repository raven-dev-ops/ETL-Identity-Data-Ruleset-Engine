# Architecture

The current prototype is a staged, file-based pipeline that turns
synthetic multi-source person records into normalized records, scored
candidate pairs, deterministic clusters, golden records, and reporting
artifacts.

## Runtime Stages

1. `generate`
   - Writes synthetic source datasets under `data/synthetic_sources/`
   - Supports CSV and Parquet outputs
2. `normalize`
   - Reads `person_source_*.csv` inputs, or `person_source_*.parquet`
     when CSV inputs are absent
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
     plus match, cluster, golden, and review-queue artifacts
   - Writes under `data/exceptions/`
8. `run-all`
   - Executes the end-to-end prototype path in one command
   - Accepts the same synthetic output formats as `generate` and chooses
     the generated normalization inputs accordingly

## Config Surfaces

The current runtime reads these repo config files at startup:

- `config/normalization_rules.yml`
- `config/blocking_rules.yml`
- `config/matching_rules.yml`
- `config/thresholds.yml`
- `config/survivorship_rules.yml`

Runtime config now fails fast when required sections, supported fields,
or threshold semantics are invalid.

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

## Command Example

Run the full prototype pipeline:

```bash
python -m etl_identity_engine.cli run-all
```

For the original design notes that predate the current implementation,
see [pipeline_architecture.md](../pipeline_architecture.md).

