# Production Batch Manifest

The runtime now supports a stable manifest contract for landed person
batches. This is the first production-readiness input surface: it
defines what a batch must declare before normalization or `run-all`
will read any real input files.

## Supported Contract

- Manifest formats: `.json`, `.yaml`, `.yml`
- Supported `manifest_version`: `1.0`
- Supported `entity_type`: `person`
- Supported `landing_zone.kind`: `local_filesystem`
- Supported source formats: `csv`, `parquet`
- Supported schema version: `person-v1`

## Required Top-Level Fields

- `manifest_version`
- `entity_type`
- `batch_id`
- `landing_zone`
- `sources`

## Landing-Zone Contract

`landing_zone` must contain:

- `kind`
- `base_path`

`base_path` may be absolute or relative to the manifest file. Each
source entry resolves its `path` relative to that landing-zone base
unless the source path is already absolute.

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

## Validation Behavior

Before normalization starts, the runtime validates:

- manifest shape and supported values
- source ID syntax and uniqueness
- source file presence
- file-format and extension alignment
- schema-version compatibility
- required-column presence
- `source_system` values against `source_id`

Any failure aborts the run before partial normalized output is written.
