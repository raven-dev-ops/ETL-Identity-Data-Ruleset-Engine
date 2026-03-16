# Public Safety Onboarding

The repo now ships a repeatable onboarding path for CAD and RMS source
owners:

1. Start from the checked-in example tree in
   `fixtures/public_safety_onboarding/`.
2. Replace the example rows with your own extracts while preserving the
   documented column names and contract marker files.
3. Run the conformance check before any pipeline onboarding work.

For the first supported live targets, the runtime now also ships
renderable onboarding packs documented in `docs/live-target-packs.md`.
Use them when you need a maintained target-specific scaffold instead of
the generic fixture tree:

```bash
etl-identity-engine list-live-target-packs
etl-identity-engine prepare-live-target-pack --target-id cad_county_dispatch_v1 --output-dir dist/live-targets/cad_county_dispatch_v1
etl-identity-engine check-live-target-pack --target-id cad_county_dispatch_v1 --root-dir dist/live-targets/cad_county_dispatch_v1
```

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

Generate a full synthetic vendor-shaped rehearsal tree for all shipped
CAD and RMS profiles:

```bash
etl-identity-engine generate-public-safety-vendor-batches --output-dir dist/public-safety-vendor-rehearsal --profile small --seed 42
etl-identity-engine check-public-safety-onboarding --manifest dist/public-safety-vendor-rehearsal/synthetic_vendor_manifest.yml
```

Generate a narrower rehearsal tree for one CAD profile and one RMS
profile:

```bash
etl-identity-engine generate-public-safety-vendor-batches --output-dir dist/public-safety-vendor-rehearsal --profile small --seed 42 --cad-profile cad_county_dispatch_v1 --rms-profile rms_case_management_v1
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

For the first supported live CAD and RMS targets, that `vendor_profile`
path is now wrapped in a maintained target pack with:

- a concrete `batch_manifest.yml`
- a fixed bundle directory and filenames
- a bundle-local `contract_manifest.yml`
- sample landing files and bundle rows that pass the onboarding checks
- customer-variable hooks rendered into the pack README and summary

Those packs remove the need for ad hoc manifest edits during normal
onboarding for `cad_county_dispatch_v1` and `rms_records_bureau_v1`.

For rehearsal and pre-sales work, the repo now also ships
`generate-public-safety-vendor-batches`. That command derives canonical
synthetic seed data, materializes vendor-native CAD/RMS bundle files for
the selected shipped profiles, copies the matching landed person-source
files, and writes a manifest that passes the current onboarding checks.
Use it when you need vendor-shaped mock data without inventing bundle
rows by hand.

This remains the intended first pass for source owners and integration
teams before persisted public-safety activity ingestion work.

For live customer onboarding, stop after the staged-pack validation step
and move into the custody workflow in `docs/live-landed-input-custody.md`.
The synthetic fixtures in `fixtures/` stay rehearsal-only and should not
be treated as the chain-of-custody baseline for live exports.

When customer reviewers need proof artifacts, generate the masked
acceptance package in `docs/live-acceptance-packages.md` instead of
hand-copying raw landed files or custody manifests.

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
