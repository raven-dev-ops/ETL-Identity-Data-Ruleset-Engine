# Service API

The runtime now ships an authenticated operator API over persisted SQL
state.

## Local Run

Start the API with:

```bash
export ETL_IDENTITY_STATE_DB=data/state/pipeline_state.sqlite
export ETL_IDENTITY_SERVICE_READER_API_KEY=reader-secret
export ETL_IDENTITY_SERVICE_OPERATOR_API_KEY=operator-secret

python -m etl_identity_engine.cli serve-api \
  --environment prod \
  --host 127.0.0.1 \
  --port 8000
```

`serve-api` now requires a runtime environment with `service_auth`
configured through environment-backed secrets. The default production
runtime environment reads those values from
`config/runtime_environments.yml`.

The container deployment baseline uses the dedicated `container`
environment documented in
[container-deployment.md](container-deployment.md).

## Authentication And Roles

The current baseline uses API-key authentication.

- Default header: `X-API-Key`
- `reader` key:
  - may call `GET` lookup endpoints plus `GET /healthz`,
    `GET /readyz`, and `GET /api/v1/metrics`
- `operator` key:
  - may call all `reader` endpoints
  - may also execute privileged review-decision and replay actions

The default production environment expects:

- `ETL_IDENTITY_SERVICE_READER_API_KEY`
- `ETL_IDENTITY_SERVICE_OPERATOR_API_KEY`

Those values should be supplied by the deployment environment rather
than committed into repo config.

## Endpoint Surface

- `GET /healthz`
  - Returns liveness-style process health plus the resolved state-store
    reference and API version.
- `GET /readyz`
  - Returns readiness-style status, latest run summary, and audit-event
    totals from the persisted store.
- `GET /api/v1/metrics`
  - Returns authenticated JSON metrics for service uptime plus
    persisted batch, review, export, and audit counts.
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
- `POST /api/v1/runs/{run_id}/review-cases/{review_id}/decision`
  - Applies an operator review decision to a persisted review case.
- `POST /api/v1/runs/{run_id}/replay`
  - Replays a persisted manifest-backed run through `run-all`.

## Validation Model

The service uses explicit request and response validation:

- path identifiers are validated before handler execution
- review-case status filtering is constrained to the supported lifecycle
  values
- response bodies are validated against explicit typed models for runs,
  golden records, crosswalk rows, and review cases
- privileged action bodies are validated before review updates or replay
  execution begins

Missing rows return `404`. Invalid request parameters return `422`.
Missing or invalid API keys return `401`. Authenticated callers without
the required role return `403`. Unsupported replay operations such as
non-manifest source runs return `409`.

Privileged review-decision and replay actions also now persist audit
events in the configured state store, and the service emits structured
JSON request logs to `stderr` for operational collection.

## Compatibility

The documented `/api/v1/...` endpoints and the `reader` / `operator`
role split are the stable external consumer surface for the current
line.

Compatibility expectations for path versioning, additive changes, and
deprecation are defined in
[compatibility-policy.md](compatibility-policy.md).

## Current Boundary

It does not yet:

- expose publish or export-job triggers over HTTP
- integrate with an external identity provider
- support finer-grained authorization than the current `reader` and
  `operator` roles

Those remain tracked in the active backlog.
