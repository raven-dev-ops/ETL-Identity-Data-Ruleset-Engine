# Production Batch Manifest

The runtime now supports a stable manifest contract for landed person
batches. This is the first production-readiness input surface: it
defines what a batch must declare before normalization or `run-all`
will read any real input files. The manifest can now also declare named
CAD and RMS source bundles so public-safety onboarding validates those
contract-bound payloads before the person pipeline starts.

## Supported Contract

- Manifest formats: `.json`, `.yaml`, `.yml`
- Supported `manifest_version`: `1.0`
- Supported `entity_type`: `person`
- Supported `landing_zone.kind`: `local_filesystem`, `object_storage`
- Supported source formats: `csv`, `parquet`
- Supported schema version: `person-v1`

## Required Top-Level Fields

- `manifest_version`
- `entity_type`
- `batch_id`
- `landing_zone`
- `sources`

Optional top-level fields:

- `source_bundles`

## Landing-Zone Contract

Supported landing-zone contracts:

- `local_filesystem`
  - required keys: `kind`, `base_path`
  - `base_path` may be absolute or relative to the manifest file
- `object_storage`
  - required keys: `kind`, `base_uri`
  - optional key: `storage_options`
  - `base_uri` must be an `fsspec`-compatible URI such as
    `memory://landing` or `s3://bucket/prefix`

For both kinds, each source entry resolves its `path` relative to the
landing-zone base unless the source path is already absolute for that
kind.

## Source Entry Contract

Each entry in `sources` must contain:

- `source_id`
- `path`
- `format`
- `schema_version`
- `required_columns`

`source_id` is the operator-facing identifier for the inbound source and
must match the `source_system` values found in the landed file when rows
are present.

## Source Bundle Contract

Each entry in `source_bundles` must contain:

- `bundle_id`
- `source_class`
- `path`
- `contract_name`
- `contract_version`

Optional source-bundle fields:

- `mapping_overlay`
- `vendor_profile`

Supported `source_class` values:

- `cad`
- `rms`

Supported contract identities:

- `cad_call_for_service` `v1`
- `rms_report_person` `v1`

`source_bundles` let a manifest declare full CAD or RMS onboarding
bundles in addition to the core landed person sources. Manifest-driven
`run-all --state-db ...` executions now validate those bundles during
manifest resolution and persist the derived incident-to-identity
activity rows alongside the golden-person outputs.

When a source bundle arrives with vendor-specific column names, add a
bundle-local or manifest-declared `mapping_overlay` YAML file. The
overlay translates vendor columns into the canonical contract fields for
`person_records`, `incident_records`, and `incident_person_links`
before contract validation continues.

For supported packaged onboarding profiles, a source bundle may instead
declare `vendor_profile`. That lets operators select a shipped mapping
profile without adding a bundle-local overlay file. `mapping_overlay`
and `vendor_profile` are mutually exclusive for the same source bundle.
The current shipped packaged profiles cover both CAD and RMS
onboarding.

## Supported Schema

`person-v1` currently requires these columns:

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

Files may contain additional columns, but they must include the full
required contract above.

## Example Manifest

```yaml
manifest_version: "1.0"
entity_type: person
batch_id: inbound-2026-03-13
landing_zone:
  kind: local_filesystem
  base_path: ./landing
sources:
  - source_id: source_a
    path: agency_a.csv
    format: csv
    schema_version: person-v1
    required_columns:
      - source_record_id
      - person_entity_id
      - source_system
      - first_name
      - last_name
      - dob
      - address
      - city
      - state
      - postal_code
      - phone
      - updated_at
      - is_conflict_variant
      - conflict_types
  - source_id: source_b
    path: agency_b.parquet
    format: parquet
    schema_version: person-v1
    required_columns:
      - source_record_id
      - person_entity_id
      - source_system
      - first_name
      - last_name
      - dob
      - address
      - city
      - state
      - postal_code
      - phone
      - updated_at
      - is_conflict_variant
      - conflict_types
```

Object-storage example:

```yaml
manifest_version: "1.0"
entity_type: person
batch_id: inbound-2026-03-13
landing_zone:
  kind: object_storage
  base_uri: s3://example-identity-landing/inbound/2026-03-13
  storage_options:
    anon: false
sources:
  - source_id: source_a
    path: agency_a.csv
    format: csv
    schema_version: person-v1
    required_columns:
      - source_record_id
      - person_entity_id
      - source_system
      - first_name
      - last_name
      - dob
      - address
      - city
      - state
      - postal_code
      - phone
      - updated_at
      - is_conflict_variant
      - conflict_types
```

Manifest example with public-safety source bundles:

```yaml
manifest_version: "1.0"
entity_type: person
batch_id: inbound-2026-03-13
landing_zone:
  kind: local_filesystem
  base_path: ./landing
sources:
  - source_id: source_a
    path: agency_a.csv
    format: csv
    schema_version: person-v1
    required_columns:
      - source_record_id
      - person_entity_id
      - source_system
      - first_name
      - last_name
      - dob
      - address
      - city
      - state
      - postal_code
      - phone
      - updated_at
      - is_conflict_variant
      - conflict_types
  - source_id: source_b
    path: agency_b.parquet
    format: parquet
    schema_version: person-v1
    required_columns:
      - source_record_id
      - person_entity_id
      - source_system
      - first_name
      - last_name
      - dob
      - address
      - city
      - state
      - postal_code
      - phone
      - updated_at
      - is_conflict_variant
      - conflict_types
source_bundles:
  - bundle_id: cad_primary
    source_class: cad
    path: cad_bundle
    contract_name: cad_call_for_service
    contract_version: v1
  - bundle_id: rms_primary
    source_class: rms
    path: rms_bundle
    contract_name: rms_report_person
    contract_version: v1
    mapping_overlay: overlays/vendor_columns.yml
  - bundle_id: cad_vendor_profile
    source_class: cad
    path: cad_vendor_bundle
    contract_name: cad_call_for_service
    contract_version: v1
    vendor_profile: cad_county_dispatch_v1
```

## Validation Behavior

Before normalization starts, the runtime validates:

- manifest shape and supported values
- source ID syntax and uniqueness
- source-bundle ID syntax and uniqueness
- source file presence
- file-format and extension alignment
- schema-version compatibility
- required-column presence
- `source_system` values against `source_id`
- declared CAD/RMS bundle contract identity and source class
- optional vendor-column mapping overlays for declared CAD/RMS bundles
- optional packaged vendor profiles for declared CAD bundles
- bundle required-file completeness and row-shape validation
- incident/link referential integrity inside each declared CAD/RMS bundle

Any failure aborts the run before partial normalized output is written.

## Object-Storage Notes

The object-storage adapter uses `fsspec`-compatible URIs. Protocols that
need an additional backend, such as `s3://`, also require the matching
plugin package. For S3-compatible URIs, install `s3fs` in addition to
the base project dependencies.
