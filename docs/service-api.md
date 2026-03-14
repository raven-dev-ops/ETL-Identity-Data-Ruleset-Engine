# Service API

The runtime now ships a read-only operator API over persisted SQLite
state.

## Local Run

Start the API with:

```bash
python -m etl_identity_engine.cli serve-api \
  --state-db data/state/pipeline_state.sqlite \
  --host 127.0.0.1 \
  --port 8000
```

The service requires an existing `--state-db` created by persisted
`run-all` usage.

## Endpoint Surface

- `GET /healthz`
  - Returns process health plus the resolved SQLite path and API
    version.
- `GET /api/v1/runs/latest`
  - Returns the latest completed persisted run.
- `GET /api/v1/runs/{run_id}`
  - Returns one persisted run record with summary metadata.
- `GET /api/v1/runs/{run_id}/golden-records/{golden_id}`
  - Returns one persisted golden record.
- `GET /api/v1/runs/{run_id}/crosswalk/source-records/{source_record_id}`
  - Returns the source-to-golden crosswalk row for one source record.
- `GET /api/v1/runs/{run_id}/review-cases`
  - Returns persisted review cases for a run.
  - Supports `status` and `assigned_to` query filters.
- `GET /api/v1/runs/{run_id}/review-cases/{review_id}`
  - Returns one persisted review case.

## Validation Model

The service uses explicit request and response validation:

- path identifiers are validated before handler execution
- review-case status filtering is constrained to the supported lifecycle
  values
- response bodies are validated against explicit typed models for runs,
  golden records, crosswalk rows, and review cases

Missing rows return `404`. Invalid request parameters return `422`.

## Current Boundary

This API is read-only in the current line.

It does not yet:

- apply review decisions over HTTP
- trigger replay or publication actions
- expose authentication or authorization controls

Those remain tracked in the active backlog.
