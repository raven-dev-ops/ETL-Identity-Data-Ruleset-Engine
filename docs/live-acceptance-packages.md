# Live Acceptance Packages

Use `package-live-target-acceptance` when you need a reviewable
customer-onboarding artifact without carrying raw landed inputs or
customer-specific custody metadata into the package.

This command is the supported follow-on step after
`capture-live-target-custody`.

Example:

```bash
etl-identity-engine package-live-target-acceptance --target-id cad_county_dispatch_v1 --source-root D:/etl/landed-batches/20260316T040300Z-cad_county_dispatch_v1 --output-dir D:/etl/acceptance-packages
```

## Output

Each acceptance package writes a timestamped directory containing:

- masked `landing/` source files
- masked bundle files under the target-pack bundle directory
- the copied `contract_manifest.yml`
- a sanitized `batch_manifest.yml` with `batch_id` rewritten to
  `acceptance-<target_id>`
- `drift_report.json`
- `drift_report.md`
- `acceptance_package_summary.json`
- a sanitized `README.md`

The package intentionally does not copy:

- `custody_manifest.json`
- `live_target_pack_summary.json`
- the rendered live-target README with customer contact or drop-zone
  values

## Drift Report

The drift report is derived from the live onboarding validation summary
and keeps only the reviewable contract details:

- bundle status
- contract and vendor-profile identity
- per-file row counts
- `overlay_mode`
- `missing_required_canonical_fields`
- `missing_source_columns`
- `unmapped_source_columns`

It omits raw landed values, original custody paths, and customer
operator metadata.

## Validation

The generated acceptance package is revalidated as a live target pack
after masking. That confirms the masked rows still preserve the bundle
contract and cross-file link integrity needed for smoke coverage and
customer-review rehearsals.
