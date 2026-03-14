# Data Model

This prototype uses synthetic records across five datasets generated in
`data/synthetic_sources/`.

## 1) `person_source_a` and `person_source_b`

One row per source-system representation of a person.

Required columns:

- `source_record_id` (PK within each source)
- `person_entity_id` (stable synthetic identity key across sources)
- `source_system` (`source_a` or `source_b`)
- `first_name`
- `last_name`
- `dob` (`YYYY-MM-DD` string)
- `address`
- `city`
- `state`
- `postal_code`
- `phone`
- `updated_at` (UTC timestamp string)
- `is_conflict_variant` (`true` or `false`)
- `conflict_types` (semicolon-delimited recipe names)

Constraints:

- exactly one `source_a` and one `source_b` record per `person_entity_id`
- `source_record_id` is unique per dataset

## 2) `conflict_annotations`

Tracks intentional synthetic conflicts injected into `source_b`.

Required columns:

- `source_record_id` (FK -> `person_source_b.source_record_id`)
- `person_entity_id`
- `source_system`
- `conflict_types`

Constraints:

- one row per conflict-variant source record
- `conflict_types` is non-empty

## 3) `incident_records`

Synthetic incident stubs.

Required columns:

- `incident_id` (PK)
- `source_system` (`cad` or `rms`)
- `occurred_at` (UTC timestamp string)
- `location`
- `city`
- `state`

## 4) `incident_person_links`

Many-to-many links between incidents and person source records.

Required columns:

- `incident_person_link_id` (PK)
- `incident_id` (FK -> `incident_records.incident_id`)
- `person_entity_id`
- `source_record_id` (FK -> person source tables)
- `role` (`VICTIM`, `SUSPECT`, `WITNESS`, `REPORTING_PARTY`)

Constraints:

- `incident_id` must exist in `incident_records`
- `source_record_id` must exist in source person datasets

## 5) `address_history`

Historical addresses per `person_entity_id`.

Required columns:

- `address_history_id` (PK)
- `person_entity_id`
- `address`
- `city`
- `state`
- `postal_code`
- `effective_start` (`YYYY-MM-DD`)
- `effective_end` (`YYYY-MM-DD` or empty for current)
- `is_current` (`true` or `false`)

Constraints:

- at least one address row per `person_entity_id`
- exactly one current row (`is_current=true`) per `person_entity_id`

## Exports

Generator supports both `CSV` and `Parquet` outputs for each dataset
from a single command invocation.

## Versioned Public-Safety Bundle Contracts

The repo now also ships versioned bundle contracts for public-safety
source onboarding:

- `cad_call_for_service` `v1`
- `rms_report_person` `v1`

Each bundle is anchored by `contract_manifest.yml` plus three declared
files:

- `person_records`
- `incident_records`
- `incident_person_links`

Those bundles reuse the row shapes documented above, but the contract
layer adds:

- bundle-level `contract_name` and `contract_version` markers
- explicit file-role mapping
- `source_system` alignment by source class
- file completeness and required-column validation
- link-to-incident and link-to-person integrity checks

See [CAD Source Contract](cad-source-contract.md) and
[RMS Source Contract](rms-source-contract.md).

