# CAD Source Contract

The `cad_call_for_service` contract defines the versioned onboarding
shape for a CAD call-for-service source bundle.

## Contract Identity

- `contract_name`: `cad_call_for_service`
- `contract_version`: `v1`
- Marker file: `contract_manifest.yml`

## Required Files

The bundle must contain three files referenced by
`contract_manifest.yml`. Each file may be `CSV` or `Parquet`.

- `person_records`
  - default filename: `cad_person_records.csv`
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
  - default filename: `cad_incident_records.csv`
  - required columns:
    - `incident_id`
    - `source_system`
    - `occurred_at`
    - `location`
    - `city`
    - `state`
- `incident_person_links`
  - default filename: `cad_incident_person_links.csv`
  - required columns:
    - `incident_person_link_id`
    - `incident_id`
    - `person_entity_id`
    - `source_record_id`
    - `role`

## Example Marker

```yaml
contract_name: cad_call_for_service
contract_version: v1
files:
  person_records: cad_person_records.csv
  incident_records: cad_incident_records.csv
  incident_person_links: cad_incident_person_links.csv
```

Vendor-shaped bundles may also include an optional bundle-local
`mapping_overlay` entry:

```yaml
contract_name: cad_call_for_service
contract_version: v1
mapping_overlay: overlays/vendor_columns.yml
files:
  person_records: vendor_person_records.csv
  incident_records: vendor_incident_records.csv
  incident_person_links: vendor_incident_person_links.csv
```

The mapping overlay translates vendor-specific person, incident, and
link column names into the canonical CAD contract fields before bundle
validation continues.

## Validation Rules

- `person_records.source_system` must be `cad`.
- `incident_records.source_system` must be `cad`.
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
etl-identity-engine validate-public-safety-contract --bundle-dir ./cad_bundle
```

The command prints a JSON summary when the bundle is valid and raises a
validation error before runtime execution when the bundle is malformed.

For full onboarding checks against a manifest plus one or more bundles:

```bash
etl-identity-engine check-public-safety-onboarding --manifest fixtures/public_safety_onboarding/example_manifest.yml --bundle-dir fixtures/public_safety_onboarding/cad_bundle
```
