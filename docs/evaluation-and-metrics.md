# Evaluation and Metrics

The current reporting path emits both markdown and machine-readable run
metrics under `data/exceptions/`.

## Current Outputs

- `data/exceptions/run_report.md`
- `data/exceptions/run_summary.json`

Generate the current reporting outputs through the end-to-end path:

```bash
python -m etl_identity_engine.cli run-all
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

