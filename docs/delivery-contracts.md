# Delivery Contracts

The runtime now supports a versioned downstream publication contract for
golden records and the source-to-golden crosswalk from persisted SQL
state.

The contract can be published directly with `publish-delivery` or
through configured named export jobs with `export-job-run`.

## Current Contract

- Contract name: `golden_crosswalk_snapshot`
- Contract version: `v1`
- Publication command:

```bash
python -m etl_identity_engine.cli publish-delivery \
  --state-db data/state/pipeline_state.sqlite \
  --run-id RUN-20260314T000000Z-ABC12345 \
  --output-dir published/delivery
```

If `--run-id` is omitted, the command publishes the latest completed run
from `--state-db`.

Configured export jobs layer on top of the same contract:

```bash
python -m etl_identity_engine.cli export-job-run \
  --state-db data/state/pipeline_state.sqlite \
  --job-name warehouse_identity_snapshot
```

The default catalog ships two named jobs:

- `warehouse_identity_snapshot` under `published/warehouse/person_identity/`
- `data_product_identity_snapshot` under
  `published/data_products/person_identity/`

Those paths come from `config/export_jobs.yml` and can be overridden per
environment.

## Published Layout

The current contract writes immutable snapshot directories under:

```text
published/delivery/golden_crosswalk_snapshot/v1/
  current.json
  snapshots/
    RUN-20260314T000000Z-ABC12345/
      golden_person_records.csv
      source_to_golden_crosswalk.csv
      delivery_manifest.json
```

## Atomicity Model

Publication is atomic at the snapshot level:

- the runtime writes the new snapshot into a temporary directory
- the temporary directory is renamed into the immutable final snapshot
  path only after all files are complete
- `current.json` is then replaced atomically to point consumers to the
  new snapshot

Downstream consumers should read `current.json` first and then resolve
the snapshot-relative paths from that pointer.

## `current.json` Contract

Required keys:

- `contract_name`
- `contract_version`
- `snapshot_id`
- `run_id`
- `published_at_utc`
- `relative_snapshot_path`
- `relative_manifest_path`

## `delivery_manifest.json` Contract

Required keys:

- `contract_name`
- `contract_version`
- `snapshot_id`
- `published_at_utc`
- `run_id`
- `state_db`
- `source_run`
- `row_counts`
- `artifacts`

Artifact entries include:

- `name`
- `relative_path`
- `row_count`
- `headers`
- `sha256`

`v1` publishes exactly two CSV artifacts:

- `golden_person_records.csv`
- `source_to_golden_crosswalk.csv`

Header compatibility for those files is inherited from the stable CSV
artifact contracts in [output-contracts.md](output-contracts.md).

If the selected runtime environment configures field authorization for
`delivery.golden_records` or `delivery.source_to_golden_crosswalk`,
publication still preserves the documented filenames, headers, and row
counts. `mask` replaces non-empty string values with `[MASKED]`, while
`deny` blocks the entire snapshot publication instead of emitting a
partial contract.

## Compatibility

`golden_crosswalk_snapshot/v1` is the current stable downstream consumer
contract.

Compatibility expectations for contract version bumps, additive changes,
and deprecation are defined in
[compatibility-policy.md](compatibility-policy.md).

## Versioning Rule

The contract version must be bumped when any consumer-visible behavior
changes, including:

- renamed files
- column additions, removals, or order changes
- pointer-manifest key changes
- changed path layout under the published contract root
