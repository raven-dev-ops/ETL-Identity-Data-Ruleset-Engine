# Normalization

The normalization stage standardizes source person records into one
combined artifact for downstream matching.

## Current Runtime Behavior

- Input discovery defaults to `data/synthetic_sources/person_source_*.csv`
- Explicit source files can be supplied with repeated `--input` flags
- Output is written to `data/normalized/normalized_person_records.csv`
- Runtime rules are loaded from `config/normalization_rules.yml`

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

## Output Contract

`data/normalized/normalized_person_records.csv` is the current handoff
surface for the matching stage. The file contains one row per input
source record after normalization and preserves source-level identifiers
for traceability.

