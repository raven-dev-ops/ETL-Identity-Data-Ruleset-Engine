# Container Deployment

The runtime now ships a single container image that can execute both the
batch CLI and the authenticated service API.

## Image Build

Build the image from the repository root:

```bash
docker build -t etl-identity-engine:local .
```

The image exposes the `etl-identity-engine` console command as its
entrypoint and publishes service port `8000`.

## Single-Host Compose Topology

The repo now includes a deployable single-host topology in:

- `deploy/compose.yaml`
- `deploy/container.env.example`

Prepare a local env file:

```bash
cp deploy/container.env.example deploy/container.env
```

Then adjust at least:

- `ETL_IDENTITY_IMAGE`
- `ETL_IDENTITY_SERVICE_READER_API_KEY`
- `ETL_IDENTITY_SERVICE_OPERATOR_API_KEY`

The default compose layout expects:

- persisted state under `deploy/runtime/`
- repo config mounted read-only from `config/`
- the SQLite state DB at `/runtime/state/pipeline_state.sqlite` inside
  the container

## Batch Run

Run the one-off batch container:

```bash
docker compose -f deploy/compose.yaml --env-file deploy/container.env run --rm identity-batch
```

The default batch command executes:

- `run-all --environment container --base-dir /runtime/output --profile small --seed 42`

For real landed-batch execution, override the command and mount the
manifest plus landing-zone files under the runtime root.

## Service Run

Start the service after a persisted SQLite state DB exists:

```bash
docker compose -f deploy/compose.yaml --env-file deploy/container.env up -d identity-service
```

The service container runs:

- `serve-api --environment container --host 0.0.0.0 --port 8000`

The `container` runtime environment intentionally stays on API-key
compatibility mode. Health endpoints remain authenticated, so the
compose service health check uses the configured `reader` API key.

## Local Smoke Path

The repo now includes a reusable smoke path:

```bash
python scripts/container_smoke_test.py --image-tag etl-identity-engine:local
```

That script:

- builds the image
- validates the CLI entrypoint
- validates the compose manifest
- runs the batch container
- starts the service container
- waits for authenticated `GET /healthz`

## State Recovery

Backup, restore, and replay procedures for mounted persisted state are
now documented in [recovery-runbooks.md](recovery-runbooks.md).

For the single-host compose topology, the recoverable set is:

- the mounted SQLite DB under the runtime volume
- the verified replay bundle for a manifest-driven batch
- any custom config overlay mounted into the container runtime

Rebuilding reports or downstream publications only requires the
restored SQLite DB. Replaying a manifest-driven run also requires the
restored replay bundle to exist again at the stored container-visible
bundle path.

## Current Boundary

This deployment baseline now provides:

- one reproducible runtime image
- a deployable compose topology for single-host persisted-state usage
- CI smoke validation that the containerized batch and service surfaces
  start successfully
- documented backup, restore, and replay procedures for the mounted
  persisted-state model

It does not yet provide:

- orchestration manifests for a clustered environment
- image-signing or supply-chain hardening

Those remain tracked in the active backlog.
