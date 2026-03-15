# Review Workflow

The runtime now supports persisted manual-review cases when `run-all` is
paired with `--state-db`.

## Current Scope

Fresh runs still emit the file artifact:

- `data/review_queue/manual_review_queue.csv`

When persisted state is enabled, the same review rows are also stored in
SQLite with workflow metadata:

- `queue_status`
- `assigned_to`
- `operator_notes`
- `created_at_utc`
- `updated_at_utc`
- `resolved_at_utc`

## Lifecycle States

The current lifecycle states are:

- `pending`
- `approved`
- `rejected`
- `deferred`

Allowed transitions:

- `pending` -> `approved`, `rejected`, `deferred`
- `deferred` -> `pending`, `approved`, `rejected`
- `approved` -> `pending`
- `rejected` -> `pending`

## Commands

List cases for a persisted run:

```bash
python -m etl_identity_engine.cli review-case-list \
  --state-db data/state/pipeline_state.sqlite \
  --run-id RUN-20260314T000000Z-ABC12345
```

Update a case:

```bash
python -m etl_identity_engine.cli review-case-update \
  --state-db data/state/pipeline_state.sqlite \
  --run-id RUN-20260314T000000Z-ABC12345 \
  --review-id REV-00001 \
  --assigned-to analyst.one \
  --status deferred \
  --notes "Need source verification"
```

Apply a decision through the operator wrapper:

```bash
python -m etl_identity_engine.cli apply-review-decision \
  --state-db data/state/pipeline_state.sqlite \
  --run-id RUN-20260314T000000Z-ABC12345 \
  --review-id REV-00001 \
  --decision approved \
  --notes "Approved after verification"
```

All three commands emit JSON so operators can script around them.

The authenticated service surface now also supports operator-only review
decisions. In JWT mode:

```bash
curl \
  -X POST \
  -H "Authorization: Bearer $ETL_IDENTITY_SERVICE_OPERATOR_JWT" \
  -H "Content-Type: application/json" \
  http://127.0.0.1:8000/api/v1/runs/RUN-20260314T000000Z-ABC12345/review-cases/REV-00001/decision \
  -d '{"decision":"approved","notes":"Approved after verification"}'
```

The `container` compatibility environment still supports the same
operation with `X-API-Key`.

In JWT mode, the caller must have:

- internal role: `operator`
- scope: `review_cases:write`

## Compatibility

The documented review lifecycle states and persisted operator commands
are the stable workflow automation surface for the current release line.

External operators should treat:

- command names
- documented flags
- lifecycle states such as `pending`, `approved`, `rejected`, and
  `deferred`

as stable, while treating direct SQLite reads as internal.

Shared versioning and deprecation expectations are defined in
[compatibility-policy.md](compatibility-policy.md).

## Current Boundary

This workflow now includes decision application on later persisted
manifest reruns:

- `approved` forces the reviewed pair into `auto_merge`
- `rejected` forces the reviewed pair into `no_match`
- those overrides are applied before cluster and golden rebuilds

It does not yet:

- expose field-level or tenant-level review authorization beyond the
  current `operator` role plus `review_cases:write` scope

Those remain tracked in the active backlog.
