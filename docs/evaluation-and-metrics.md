# Evaluation and Metrics

The current reporting path emits both markdown and machine-readable run
metrics under `data/exceptions/`.

The standalone `report` stage reads the normalized artifact plus the
current match, cluster, golden, and review-queue artifacts so the
summary matches the end-to-end pipeline state rather than only the
normalization step.

## Current Outputs

- `data/exceptions/run_report.md`
- `data/exceptions/run_summary.json`

Generate the current reporting outputs through the end-to-end path:

```bash
python -m etl_identity_engine.cli run-all
```

Or rerun reporting against existing artifacts:

```bash
python -m etl_identity_engine.cli report \
  --input data/normalized/normalized_person_records.csv \
  --output data/exceptions/run_report.md
```

To rebuild the manual review queue before rerunning reporting:

```bash
python -m etl_identity_engine.cli review-queue \
  --input data/matches/candidate_scores.csv \
  --output data/review_queue/manual_review_queue.csv
```

## Current Metrics

The summary currently includes:

- completeness counts for raw and canonical name, DOB, and phone fields
- before/after completeness deltas for those same fields
- candidate decision counts
- review queue volume
- cluster and golden-record counts
- duplicate-reduction metrics based on post-resolution record counts
- exception counts and missing-field counts
- phase timing and throughput metrics for the core `run-all` stages

## Performance Block

`run_summary.json` now also includes a `performance` block with:

- `total_duration_seconds`
- per-phase metrics for `generate`, `normalize`, `match`, `cluster`,
  `review_queue`, `golden`, `crosswalk`, `report`, and `persist_state`
- per-phase `duration_seconds`
- per-phase input and output record counts
- per-phase record throughput
- candidate-pair throughput for the match phase

The markdown report now renders the same performance section, so normal
pipeline runs and persisted report reloads keep the same timing view.

## Duplicate Reduction

The current duplicate-reduction block reports:

- `before_record_count`
- `after_record_count`
- `records_consolidated`
- `reduction_ratio`

## Benchmark Fixtures and Capacity Targets

For larger-batch operational validation, use the named fixtures and
capacity targets documented in
[benchmarking-and-capacity.md](benchmarking-and-capacity.md):

```bash
python -m etl_identity_engine.cli benchmark-run --fixture scale_medium
```

That command runs the real persisted pipeline path, captures the
performance block from the resulting `run_summary.json`, and evaluates
the measured run against the configured deployment target.

## Next Steps

Threshold tuning guidance, release-quality targets, and richer quality
interpretation notes are still tracked as follow-on documentation work.
The current test suite now includes explicit threshold-boundary
regression fixtures so scorer changes are checked against stable
`auto_merge`, `manual_review`, and `no_match` expectations before merge.

