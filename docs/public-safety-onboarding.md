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
- `../public_safety_regressions/`
- `landing/source_a.csv`
- `landing/source_b.csv`
- `example_manifest.yml`
- `example_vendor_overlay_manifest.yml`

The runtime also now ships maintained packaged CAD vendor profiles:

- `cad_county_dispatch_v1`
- `cad_records_management_v1`

And maintained packaged RMS vendor profiles:

- `rms_case_management_v1`
- `rms_records_bureau_v1`

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

Validate a vendor-native CAD bundle with a packaged profile:

```bash
etl-identity-engine validate-public-safety-contract --bundle-dir ./cad_bundle --vendor-profile cad_county_dispatch_v1
```

## Expected Outcome

The conformance command prints JSON with:

- `status`
- bundle summaries
- manifest summary
- resolved source counts
- resolved source-bundle counts
- per-file `diff_report` blocks for mapped, unmapped, and unused source
  columns

If validation fails, fix the contract surface before attempting
pipeline onboarding. The current scope of this check is structural:

- required files exist
- required columns exist
- `source_system` values match the declared source class
- incident/link references are internally consistent
- manifest-declared source bundles match the resolved contract markers
- vendor-column overlays resolve the shipped file shapes into the
  canonical CAD/RMS contract fields

The `diff_report` is the operator-facing drift surface during onboarding.
Use it to answer three concrete questions with the source owner:

- which canonical fields were mapped successfully
- which source columns are currently unused or unmapped
- which required canonical fields still have no resolvable source
  mapping

For broken vendor bundles, the command still prints JSON before exiting
nonzero. That lets operators save the report and work directly from
`missing_required_canonical_fields`, `missing_source_columns`, and
`unmapped_source_columns` instead of relying on a single error string.

The current onboarding model now supports two source-bundle shapes:

- canonical bundles that already use the documented contract columns
- vendor bundles that keep vendor-native columns and add a
  `mapping_overlay` YAML file

For the supported CAD profiles, there is now also a third option:

- vendor bundles that keep vendor-native columns and declare a shipped
  `vendor_profile`

The same pattern now applies to the shipped RMS profiles.

This remains the intended first pass for source owners and integration
teams before persisted public-safety activity ingestion work.

## Canonical Regression Scenarios

The repo also ships `fixtures/public_safety_regressions/` as the
canonical onboarding regression set for buyer and pilot discussions.

That fixture tree locks three scenarios:

- `same_person_cross_system`
  - one person appears in both CAD and RMS and must merge
- `same_household_separate_people`
  - two people share a household footprint and must remain distinct
- `cross_system_false_merge_guard`
  - a soundalike cross-system pair shares DOB but must not merge

Use it when you need to prove expected merge and no-merge outcomes, not
just contract validity:

```bash
etl-identity-engine check-public-safety-onboarding --manifest fixtures/public_safety_regressions/manifest.yml
python -m etl_identity_engine.cli run-all --base-dir dist/public-safety-regressions --manifest fixtures/public_safety_regressions/manifest.yml
```
