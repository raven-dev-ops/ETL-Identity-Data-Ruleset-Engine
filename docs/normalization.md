# Normalization

The normalization stage standardizes source person records into one
combined artifact for downstream matching.

## Current Runtime Behavior

- Input discovery defaults to `data/synthetic_sources/person_source_*.csv`
- Explicit source files can be supplied with repeated `--input` flags
- Production landed batches can be supplied through `--manifest`
- Output is written to `data/normalized/normalized_person_records.csv`
- Runtime rules are loaded from `config/normalization_rules.yml`
- Address normalization now standardizes common street suffixes,
  directional tokens, PO box forms, and unit markers into a single
  canonical token order.
- Phone normalization defaults to digit-only output and can opt into
  E.164-style output through config without changing the default
  contract for existing runs.

## Normalized Fields

The current stage writes the original source columns plus:

- `canonical_name`
- `canonical_dob`
- `canonical_address`
- `canonical_phone`

## Commands

Normalize all discovered source person files from the default synthetic
input directory:

```bash
python -m etl_identity_engine.cli normalize
```

Normalize explicit source files into the standard normalized artifact:

```bash
python -m etl_identity_engine.cli normalize \
  --input data/synthetic_sources/person_source_a.csv \
  --input data/synthetic_sources/person_source_b.csv \
  --output data/normalized/normalized_person_records.csv
```

Normalize a landed production-style batch through a manifest:

```bash
python -m etl_identity_engine.cli normalize \
  --manifest manifests/inbound-batch.yml \
  --output data/normalized/normalized_person_records.csv
```

The manifest contract requires:

- `manifest_version: "1.0"`
- `entity_type: person`
- a supported `landing_zone`
- one or more source entries with `source_id`, `path`, `format`,
  `schema_version`, and `required_columns`

The runtime validates the manifest, file presence, required columns, and
`source_system` identifiers before normalization starts. Invalid
manifests fail fast and do not write partial normalized output.

Supported landing-zone kinds are:

- `local_filesystem`
- `object_storage`

## Output Contract

`data/normalized/normalized_person_records.csv` is the current handoff
surface for the matching stage. The file contains one row per input
source record after normalization and preserves source-level identifiers
for traceability.

## Phone Output Config

`config/normalization_rules.yml` supports these phone options:

- `digits_only`
- `output_format`
- `default_country_code`

The current supported `output_format` values are:

- `digits_only`
- `e164`

`digits_only` remains the default for the `0.1.x` line. `e164` uses the
configured `default_country_code` for local 10-digit inputs and preserves
internationally prefixed digit strings when they are already present.

