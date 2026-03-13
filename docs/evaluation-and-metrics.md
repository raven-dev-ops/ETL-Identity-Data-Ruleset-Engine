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

## Duplicate Reduction

The current duplicate-reduction block reports:

- `before_record_count`
- `after_record_count`
- `records_consolidated`
- `reduction_ratio`

## Next Steps

Threshold tuning guidance, release-quality targets, and richer quality
interpretation notes are still tracked as follow-on documentation work.
The current test suite now includes explicit threshold-boundary
regression fixtures so scorer changes are checked against stable
`auto_merge`, `manual_review`, and `no_match` expectations before merge.

