# Kubernetes Deployment

The repo now ships a Kubernetes deployment baseline for the
PostgreSQL-backed persisted-state topology.

This baseline is intentionally concrete:

- PostgreSQL runs as a single StatefulSet-backed primary.
- The authenticated service runs as a Deployment.
- State migrations run through a one-off Job.
- Manifest-driven batch runs execute through a one-off Job.
- The service is exposed internally through a ClusterIP Service.
- An optional Ingress example is included for external routing.

The manifests live under `deploy/kubernetes/`.

## Shipped Assets

- `deploy/kubernetes/kustomization.yaml`
- `deploy/kubernetes/namespace.yaml`
- `deploy/kubernetes/postgres-service.yaml`
- `deploy/kubernetes/postgres-statefulset.yaml`
- `deploy/kubernetes/landing-pvc.yaml`
- `deploy/kubernetes/service-service.yaml`
- `deploy/kubernetes/service-deployment.yaml`
- `deploy/kubernetes/postgres-secret.example.yaml`
- `deploy/kubernetes/runtime-secret.example.yaml`
- `deploy/kubernetes/state-db-upgrade-job.yaml`
- `deploy/kubernetes/batch-job.yaml`
- `deploy/kubernetes/service-ingress.example.yaml`

## Runtime Environment

The Kubernetes baseline uses the dedicated `cluster` runtime
environment from `config/runtime_environments.yml`.

That environment is intentionally configured for:

- PostgreSQL via `ETL_IDENTITY_STATE_DB`
- API-key service auth compatibility mode
- the same stable `reader` and `operator` scope contract used by the
  rest of the runtime

The cluster baseline stays on API-key auth so the shipped probes and
smoke path remain self-contained. Teams that need JWT-backed ingress can
layer an environment override on top of this baseline and switch the
service command to `--environment prod`.

## Secrets

Create real Secret manifests from the shipped examples before applying
the topology.

### PostgreSQL bootstrap secret

Start from `deploy/kubernetes/postgres-secret.example.yaml` and provide:

- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`

### Runtime secret

Start from `deploy/kubernetes/runtime-secret.example.yaml` and provide:

- `ETL_IDENTITY_STATE_DB`
- `ETL_IDENTITY_OBJECT_STORAGE_ACCESS_KEY`
- `ETL_IDENTITY_OBJECT_STORAGE_SECRET_KEY`
- `ETL_IDENTITY_SERVICE_READER_API_KEY`
- `ETL_IDENTITY_SERVICE_OPERATOR_API_KEY`

For the shipped baseline, `ETL_IDENTITY_STATE_DB` should point at the
cluster-local PostgreSQL Service:

```text
postgresql://etl_identity:<password>@identity-postgres:5432/identity_state
```

Do not commit deployment-specific secret values into the repository.

## Apply Order

### 1. Prepare secrets and image reference

- copy the example Secret manifests
- replace placeholder credentials
- replace the image tag in:
  - `deploy/kubernetes/service-deployment.yaml`
  - `deploy/kubernetes/state-db-upgrade-job.yaml`
  - `deploy/kubernetes/batch-job.yaml`

### 2. Apply namespace, storage, PostgreSQL, and service

```bash
kubectl apply -f deploy/kubernetes/postgres-secret.yaml
kubectl apply -f deploy/kubernetes/runtime-secret.yaml
kubectl apply -k deploy/kubernetes
```

### 3. Run the state migration job

```bash
kubectl apply -f deploy/kubernetes/state-db-upgrade-job.yaml
kubectl wait --for=condition=complete job/identity-state-db-upgrade -n etl-identity --timeout=300s
```

### 4. Load a landed batch onto the PVC

The batch job expects:

- `/runtime/landing/batch-manifest.yaml`
- the landed files referenced by that manifest

The shipped `identity-landing-pvc` is the default attachment point for
those inputs.

For a `local_filesystem` manifest, copy both the manifest and its
referenced files onto the PVC.

For an `object_storage` manifest, the PVC only needs the manifest file;
the object-storage credentials must then be valid in the runtime Secret.

### 5. Run the batch job

```bash
kubectl apply -f deploy/kubernetes/batch-job.yaml
kubectl wait --for=condition=complete job/identity-batch -n etl-identity --timeout=1800s
```

### 6. Optionally expose the service through Ingress

The default Service is internal-only:

- `identity-service`
- port `8000`
- type `ClusterIP`

To route external traffic, start from
`deploy/kubernetes/service-ingress.example.yaml`, set the host and any
controller-specific annotations, then apply it.

## Service Exposure

The service container runs:

```text
serve-api --environment cluster --host 0.0.0.0 --port 8000
```

The shipped liveness and readiness probes authenticate with the
configured `reader` API key and call:

- `GET /healthz`
- `GET /readyz`

Downstream callers must also send the documented `X-API-Key` header
unless the deployment has been explicitly overlaid onto the JWT-backed
`prod` environment.

## State-Store Wiring

The cluster baseline is PostgreSQL-backed end to end:

- the StatefulSet owns the database data volume
- the runtime Secret carries the SQLAlchemy URL
- the migration Job bootstraps the schema
- the batch Job writes persisted runs into PostgreSQL
- the service reads persisted state from the same database

This is the supported clustered state model for the current line.

## Smoke Validation

The repo includes a reusable smoke path:

```bash
python scripts/kubernetes_manifest_smoke.py --image-tag etl-identity-engine:local --service-port 18081
```

That command:

- validates the Kubernetes manifest set and secret-key contract
- builds the runtime image
- starts an ephemeral PostgreSQL container
- runs the shipped `state-db-upgrade` job command
- runs the shipped manifest-driven batch job command
- starts the shipped service command
- checks authenticated `healthz`, `metrics`, and latest-run access

CI runs that smoke path on Linux.

## Current Boundary

This baseline now provides:

- a concrete Kubernetes manifest set for PostgreSQL-backed service and
  batch execution
- documented secret, storage, and service-exposure wiring
- a repo-native smoke path that exercises the shipped cluster commands

It does not yet provide:

- a high-availability PostgreSQL topology
- autoscaling guidance or distributed SLO targets
- container signing or image-provenance gates
