# Public Safety Onboarding

The repo now ships a repeatable onboarding path for CAD and RMS source
owners:

1. Start from the checked-in example tree in
   `fixtures/public_safety_onboarding/`.
2. Replace the example rows with your own extracts while preserving the
   documented column names and contract marker files.
3. Run the conformance check before any pipeline onboarding work.

## Shipped Example Tree

The example fixture root is:

`fixtures/public_safety_onboarding/`

It includes:

- `cad_bundle/`
- `rms_bundle/`
- `landing/source_a.csv`
- `landing/source_b.csv`
- `example_manifest.yml`

## Self-Check Commands

Validate both public-safety bundles plus the example manifest:

```bash
etl-identity-engine check-public-safety-onboarding --manifest fixtures/public_safety_onboarding/example_manifest.yml --bundle-dir fixtures/public_safety_onboarding/cad_bundle --bundle-dir fixtures/public_safety_onboarding/rms_bundle
```

Validate a single bundle directly:

```bash
etl-identity-engine validate-public-safety-contract --bundle-dir fixtures/public_safety_onboarding/cad_bundle
```

## Expected Outcome

The conformance command prints JSON with:

- `status`
- bundle summaries
- manifest summary
- resolved source counts
- resolved source-bundle counts

If validation fails, fix the contract surface before attempting
pipeline onboarding. The current scope of this check is structural:

- required files exist
- required columns exist
- `source_system` values match the declared source class
- incident/link references are internally consistent
- manifest-declared source bundles match the resolved contract markers

This is the intended first pass for source owners and integration teams
before any field-mapping or persisted public-safety ingestion work.
