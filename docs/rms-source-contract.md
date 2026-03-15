# RMS Source Contract

The `rms_report_person` contract defines the versioned onboarding shape
for an RMS report/person source bundle.

## Contract Identity

- `contract_name`: `rms_report_person`
- `contract_version`: `v1`
- Marker file: `contract_manifest.yml`

## Required Files

The bundle must contain three files referenced by
`contract_manifest.yml`. Each file may be `CSV` or `Parquet`.

- `person_records`
  - default filename: `rms_person_records.csv`
  - required columns:
    - `source_record_id`
    - `person_entity_id`
    - `source_system`
    - `first_name`
    - `last_name`
    - `dob`
    - `address`
    - `city`
    - `state`
    - `postal_code`
    - `phone`
    - `updated_at`
    - `is_conflict_variant`
    - `conflict_types`
- `incident_records`
  - default filename: `rms_incident_records.csv`
  - required columns:
    - `incident_id`
    - `source_system`
    - `occurred_at`
    - `location`
    - `city`
    - `state`
- `incident_person_links`
  - default filename: `rms_incident_person_links.csv`
  - required columns:
    - `incident_person_link_id`
    - `incident_id`
    - `person_entity_id`
    - `source_record_id`
    - `role`

## Example Marker

```yaml
contract_name: rms_report_person
contract_version: v1
files:
  person_records: rms_person_records.csv
  incident_records: rms_incident_records.csv
  incident_person_links: rms_incident_person_links.csv
```

Vendor-shaped bundles may also keep their vendor-native file headers
and rely on a manifest-declared or bundle-local mapping overlay:

```yaml
contract_name: rms_report_person
contract_version: v1
files:
  person_records: vendor_person_records.csv
  incident_records: vendor_incident_records.csv
  incident_person_links: vendor_incident_person_links.csv
```

When the manifest declares `mapping_overlay: overlays/vendor_columns.yml`
for the RMS source bundle, the runtime remaps vendor-specific person,
incident, and link columns into the canonical RMS contract fields before
bundle validation continues.

## Validation Rules

- `person_records.source_system` must be `rms`.
- `incident_records.source_system` must be `rms`.
- `source_record_id`, `incident_id`, and `incident_person_link_id`
  must be unique and non-empty in their respective files.
- Every `incident_person_links.incident_id` must exist in
  `incident_records`.
- Every `incident_person_links.source_record_id` must exist in
  `person_records`.
- Every `incident_person_links.person_entity_id` must match the
  referenced `person_records.source_record_id`.

## Validation Commands

```bash
etl-identity-engine validate-public-safety-contract --bundle-dir ./rms_bundle
```

The command prints a JSON summary when the bundle is valid and raises a
validation error before runtime execution when the bundle is malformed.

For full onboarding checks against a manifest plus one or more bundles:

```bash
etl-identity-engine check-public-safety-onboarding --manifest fixtures/public_safety_onboarding/example_manifest.yml --bundle-dir fixtures/public_safety_onboarding/rms_bundle
```
