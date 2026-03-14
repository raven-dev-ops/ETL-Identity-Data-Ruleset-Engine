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

Both commands emit JSON so operators can script around them.

## Current Boundary

This workflow now includes decision application on later persisted
manifest reruns:

- `approved` forces the reviewed pair into `auto_merge`
- `rejected` forces the reviewed pair into `no_match`
- those overrides are applied before cluster and golden rebuilds

It does not yet:

- expose write-side review workflow APIs
- add full operator replay tooling

Those remain tracked in the active backlog.
