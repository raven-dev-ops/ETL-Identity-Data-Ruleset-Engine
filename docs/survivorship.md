# Survivorship

The current survivorship path is deterministic and config-backed.

## Selection Strategy

Configured in `config/survivorship_rules.yml`.

Current behavior:

- prefer higher-ranked `source_priority`
- within the same source priority, prefer the most recent `updated_at`
- ignore empty candidate values

This strategy is applied independently to:

- `first_name`
- `last_name`
- `dob`
- `address`
- `phone`

## Golden Output

`run-all` writes `data/golden/golden_person_records.csv`.

The standalone `golden` stage expects normalized input plus
`data/matches/entity_clusters.csv` unless the input rows already contain
`cluster_id` values. It does not infer entity groups from the synthetic
`person_entity_id` field during normal CLI usage.

Each golden row now includes:

- selected field values
- `cluster_id`
- `golden_id`
- `source_record_count`
- per-field provenance columns

Per-field provenance columns follow this pattern:

- `<field>_source_record_id`
- `<field>_source_system`
- `<field>_rule_name`

Example:

- `first_name_source_record_id`
- `first_name_source_system`
- `first_name_rule_name`

## Crosswalk

`run-all` also writes `data/golden/source_to_golden_crosswalk.csv`.

The crosswalk maps every source record to:

- `cluster_id`
- `golden_id`
- original `source_record_id`
- original `source_system`

This output is the downstream join surface for traceability and
analytics.

## Command Example

Run the full clustered golden-record path:

```bash
python -m etl_identity_engine.cli run-all
```

To rerun only the golden stage against previously normalized and matched
artifacts:

```bash
python -m etl_identity_engine.cli golden \
  --input data/normalized/normalized_person_records.csv \
  --clusters data/matches/entity_clusters.csv \
  --output data/golden/golden_person_records.csv
```

This produces the golden-record output under `data/golden/`. `run-all`
also writes the source-to-golden crosswalk there.
