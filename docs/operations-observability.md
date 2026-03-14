# Operations And Observability

The current production-hardening line now exposes a minimal shared
observability baseline for batch and service operation.

## Structured Logs

The CLI and service emit structured JSON log events to `stderr`.

Current event coverage includes:

- pipeline run request, start, reuse, completion, and failure
- review-decision application and failure
- replay completion and failure
- delivery publication
- export-job completion and failure
- service request completion and failure
- service startup

The current log payloads are intended for operators and deployment
pipelines, not as a stable external data contract.

Structured logs now apply shared redaction before emission:

- free-text operator-note fields are replaced with a redaction marker
- auth material such as bearer tokens, API keys, and JWT secrets is
  removed
- JWT-like token strings, PEM payloads, and DSN passwords are scrubbed
  from free-form error text

## Health Endpoints

The authenticated service exposes:

- `GET /healthz`
  - liveness-style process health
- `GET /readyz`
  - readiness-style SQLite connectivity plus latest batch-state summary

Both endpoints require authenticated `reader` or `operator` access. In
JWT mode, that means a bearer token whose external claims map to one of
those two service roles. In compatibility mode, it is the configured
API key.

## Metrics Endpoint

The authenticated service exposes:

- `GET /api/v1/metrics`

The current metrics payload includes:

- service start time and uptime
- persisted run counts by `running`, `completed`, and `failed`
- persisted export-run counts by `running`, `completed`, and `failed`
- persisted review-case counts by lifecycle state
- persisted audit-event count
- latest completed and latest failed run identifiers plus timestamps

This endpoint is the current operator-facing metrics surface for both
service health and batch execution status.

## Audit Events

Operator-sensitive actions now persist into the SQLite `audit_events`
table.

Current audited actions include:

- `apply_review_decision`
- `replay_run`
- `publish_delivery`
- `publish_run`
- `export_job_run`

Each audit event records:

- `audit_event_id`
- `occurred_at_utc`
- `actor_type`
- `actor_id`
- `action`
- `resource_type`
- `resource_id`
- `run_id`
- `status`
- `details_json`

For service-authenticated actions, `details_json` now carries the
authenticated actor context:

- `actor_role`
- `actor_subject`
- `granted_scopes`
- `required_scopes`
- `auth_mode`

This audit surface is intended for operational traceability of
privileged workflow and publication actions, not as a replacement for
the core persisted pipeline artifacts.

Persisted audit details now follow the same redaction baseline as the
structured logs. Operator workflow metadata remains auditable, but raw
review notes and raw auth material are not retained verbatim in
`details_json`.
