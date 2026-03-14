# Event Stream Ingestion

The runtime now supports a persisted event-driven refresh path through
`stream-refresh`.

This command applies an ordered JSONL event batch onto a completed
persisted run, recomputes only the affected entities through the
existing deterministic incremental-refresh engine, and records the
result as a new persisted run.

## Current Scope

The current event-driven runtime is intentionally deterministic and
file-backed:

- input is a JSONL or NDJSON event batch
- events are applied in ascending `sequence` order
- the predecessor run comes from persisted SQL state
- the refresh path reuses the existing incremental candidate, cluster,
  review, and golden rebuild logic
- the resulting run records event metadata, refresh lineage, checkpoint
  state, and audit rows in the persisted store

This is a near-real-time micro-batch operator path, not a long-lived
message-broker consumer.

## CLI Example

```bash
python -m etl_identity_engine.cli stream-refresh \
  --state-db data/state/pipeline_state.sqlite \
  --source-run-id RUN-20260314T000000Z-ABC12345 \
  --events data/events/batch_001.jsonl \
  --stream-id booking_updates \
  --base-dir work/stream_batch_001
```

When the command succeeds it writes the normal staged file artifacts
under `--base-dir`, persists the new run in `--state-db`, copies the
processed event batch to `data/events/stream_events.jsonl`, and records
the batch metadata in `data/exceptions/run_summary.json`.

## Event Contract

Each line must be one JSON object with these fields:

- `event_id`
- `sequence`
- `operation`
- `occurred_at_utc`
- `source_record_id`
- `source_system`
- optional `stream_id`
- `record` for `upsert` events only

Supported `operation` values:

- `upsert`
- `delete`

`record` for `upsert` events must match the raw person source-row
contract:

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

Example:

```json
{"event_id":"booking_updates-000001","stream_id":"booking_updates","sequence":1,"operation":"upsert","occurred_at_utc":"2026-03-14T10:00:00Z","source_record_id":"A-000001","source_system":"source_a","record":{"source_record_id":"A-000001","person_entity_id":"P-000001","source_system":"source_a","first_name":"JOHN","last_name":"SMITH","dob":"1985-03-12","address":"123 MAIN ST","city":"COLUMBUS","state":"OH","postal_code":"43004","phone":"555-111-2222","updated_at":"2026-03-14T10:00:00Z","is_conflict_variant":"false","conflict_types":""}}
{"event_id":"booking_updates-000002","stream_id":"booking_updates","sequence":2,"operation":"delete","occurred_at_utc":"2026-03-14T10:01:00Z","source_record_id":"B-000010","source_system":"source_b"}
```

## Determinism And Auditability

Each completed stream refresh records:

- stable `run_key` inputs derived from the stream batch identity plus
  predecessor run
- the predecessor `run_id`
- event count, sequence range, and SHA-256 digest
- per-batch insert, update, delete, and noop counts
- a copied event snapshot under `data/events/stream_events.jsonl`
- the standard persisted checkpoint and audit-event trail

That means a stream refresh remains inspectable through:

- `data/exceptions/run_summary.json`
- persisted `pipeline_runs`
- persisted `run_checkpoints`
- persisted `audit_events`

## Benchmarks

`benchmark-run` now supports `mode: event_stream` fixtures in
`config/benchmark_fixtures.yml`.

Those fixtures:

- seed one persisted synthetic run
- synthesize multiple deterministic event batches
- execute `stream-refresh` repeatedly
- emit a `continuous_ingest` block in `benchmark_summary.json`
- emit explicit stream SLO metrics including `events_per_second`,
  `max_batch_duration_seconds`, and `p95_batch_duration_seconds`

The shipped reference fixtures are:

- `continuous_ingest_small` for the single-host container baseline
- `cluster_continuous_ingest_small` for the PostgreSQL-backed clustered
  runtime baseline
