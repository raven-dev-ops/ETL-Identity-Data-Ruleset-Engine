# Export Jobs

Named export jobs provide a configured operator layer on top of the
versioned `golden_crosswalk_snapshot/v1` delivery contract.

They are intended for downstream warehouse and data-product consumers
that need stable output locations, auditable execution records, and
reusable export commands rather than ad hoc `publish-delivery`
invocations.

## Current Catalog

The default export-job catalog is defined in `config/export_jobs.yml`.

It currently ships two named jobs:

- `warehouse_identity_snapshot`
  - `consumer`: `warehouse`
  - `output_root`: `published/warehouse/person_identity/`
- `data_product_identity_snapshot`
  - `consumer`: `data_product`
  - `output_root`: `published/data_products/person_identity/`

Both jobs currently publish the `golden_crosswalk_snapshot` contract at
version `v1` using the `csv_snapshot` format.

Relative `output_root` values are resolved from the runtime config root,
so the default repo config publishes under the repo-local
`published/` directory unless an environment override supplies a
different path.

## Commands

List configured jobs:

```bash
python -m etl_identity_engine.cli export-job-list
```

Run a named export against the latest completed persisted run:

```bash
python -m etl_identity_engine.cli export-job-run \
  --state-db data/state/pipeline_state.sqlite \
  --job-name warehouse_identity_snapshot
```

Run a named export against a specific completed run:

```bash
python -m etl_identity_engine.cli export-job-run \
  --state-db data/state/pipeline_state.sqlite \
  --job-name data_product_identity_snapshot \
  --run-id RUN-20260314T000000Z-ABC12345
```

Inspect export history:

```bash
python -m etl_identity_engine.cli export-job-history \
  --state-db data/state/pipeline_state.sqlite \
  --job-name warehouse_identity_snapshot
```

The JSON payloads identify whether the command created a new export or
reused a prior completed export for the same `export_key`.

The documented command names, flags, audit states, and top-level JSON
workflow shape are the stable operator automation surface for the
current release line.

## Audit Model

Export execution is tracked in the persisted `export_job_runs` table.

Each record stores:

- `export_run_id`
- `export_key`
- `attempt_number`
- `job_name`
- `source_run_id`
- `contract_name`
- `contract_version`
- `output_root`
- `status`
- `started_at_utc`
- `finished_at_utc`
- `snapshot_dir`
- `current_pointer_path`
- `row_counts_json`
- `metadata_json`
- `failure_detail`

Current statuses are:

- `running`
- `completed`
- `failed`

Completed exports are deduplicated by `export_key`, so rerunning the
same named job for the same completed source run reuses the existing
completed export record instead of duplicating downstream snapshots.

## Delivery Format And Locations

Named export jobs currently publish the same immutable snapshot layout as
`publish-delivery`:

```text
<output_root>/golden_crosswalk_snapshot/v1/
  current.json
  snapshots/
    <RUN_ID>/
      golden_person_records.csv
      source_to_golden_crosswalk.csv
      delivery_manifest.json
```

That means warehouse and data-product consumers currently receive:

- `golden_person_records.csv`
- `source_to_golden_crosswalk.csv`
- `delivery_manifest.json`
- `current.json`

The file-level contract and pointer semantics are documented in
[delivery-contracts.md](delivery-contracts.md).

Shared versioning and deprecation expectations for those downstream
surfaces are defined in
[compatibility-policy.md](compatibility-policy.md).

## Current Boundary

The current export-job surface adds configured named jobs and persistent
audit tracking. It does not yet add:

- scheduler integration
- non-CSV warehouse delivery formats
- consumer-specific schema transforms beyond the shared delivery
  contract
- authenticated remote export orchestration

Those remain follow-on work in the active backlog.
