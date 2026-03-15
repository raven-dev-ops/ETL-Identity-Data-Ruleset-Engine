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
- `cad_vendor_bundle/`
- `rms_vendor_bundle/`
- `landing/source_a.csv`
- `landing/source_b.csv`
- `example_manifest.yml`
- `example_vendor_overlay_manifest.yml`

## Self-Check Commands

Validate both public-safety bundles plus the example manifest:

```bash
etl-identity-engine check-public-safety-onboarding --manifest fixtures/public_safety_onboarding/example_manifest.yml --bundle-dir fixtures/public_safety_onboarding/cad_bundle --bundle-dir fixtures/public_safety_onboarding/rms_bundle
```

Validate the shipped vendor-column overlay example:

```bash
etl-identity-engine check-public-safety-onboarding --manifest fixtures/public_safety_onboarding/example_vendor_overlay_manifest.yml --bundle-dir fixtures/public_safety_onboarding/cad_vendor_bundle --bundle-dir fixtures/public_safety_onboarding/rms_vendor_bundle
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
- vendor-column overlays resolve the shipped file shapes into the
  canonical CAD/RMS contract fields

The current onboarding model now supports two source-bundle shapes:

- canonical bundles that already use the documented contract columns
- vendor bundles that keep vendor-native columns and add a
  `mapping_overlay` YAML file

This remains the intended first pass for source owners and integration
teams before persisted public-safety activity ingestion work.
